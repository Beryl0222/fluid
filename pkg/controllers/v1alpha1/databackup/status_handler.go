/*
  Copyright 2023 The Fluid Authors.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package databackup

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	"github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

type OnceHandler struct {
	dataBackup *v1alpha1.DataBackup
}

var _ dataoperation.StatusHandler = &OnceHandler{}

// CronHandler converges a cron DataBackup from its Kubernetes workloads (CronJob/Job/Pod).
// It never guesses the state from in-memory information: every decision is re-derived from
// cluster objects on every reconciliation, which makes controller restarts and intermediate
// states (CronJob created but Pod missing, Pod terminated with a conflicting status write, ...)
// converge idempotently.
type CronHandler struct {
	client.Client
	Log        logr.Logger
	Recorder   record.EventRecorder
	dataBackup *v1alpha1.DataBackup
}

var _ dataoperation.StatusHandler = &CronHandler{}

// UpdateStatusByHelmStatus update the operation status according to helm job status
func (o *OnceHandler) GetOperationStatus(ctx runtime.ReconcileRequestContext, opStatus *v1alpha1.OperationStatus) (result *v1alpha1.OperationStatus, err error) {
	result = opStatus.DeepCopy()
	object := o.dataBackup
	// 1. gdt pod name
	backupPodName := utils.GetDataBackupPodName(object.GetName())
	backupPod, err := kubeclient.GetPodByName(ctx.Client, backupPodName, object.GetNamespace())
	if err != nil {
		ctx.Log.Error(err, "Failed to get databackup-pod")
		return
	}

	// 2. only update status if finished
	if !kubeclient.IsFinishedPod(backupPod) {
		return
	}

	// TODO: inject nodeaffinity like other data operations when using job instead of pod

	var finishTime time.Time
	if len(backupPod.Status.Conditions) != 0 {
		finishTime = backupPod.Status.Conditions[0].LastTransitionTime.Time
	} else {
		// fail to get finishTime, use current time as default
		finishTime = time.Now()
	}
	result.Duration = utils.CalculateDuration(backupPod.CreationTimestamp.Time, finishTime)

	if kubeclient.IsSucceededPod(backupPod) {
		result.Phase = common.PhaseComplete
		result.Conditions = []v1alpha1.Condition{
			{
				Type:               common.Complete,
				Status:             v1.ConditionTrue,
				Reason:             "BackupSuccessful",
				Message:            "Backup Pod exec successfully and finish",
				LastProbeTime:      metav1.NewTime(time.Now()),
				LastTransitionTime: metav1.NewTime(finishTime),
			},
		}
	} else if kubeclient.IsFailedPod(backupPod) {
		result.Phase = common.PhaseFailed
		result.Conditions = []v1alpha1.Condition{
			{
				Type:               common.Failed,
				Status:             v1.ConditionTrue,
				Reason:             "BackupFailed",
				Message:            "Backup Pod exec failed and exit",
				LastProbeTime:      metav1.NewTime(time.Now()),
				LastTransitionTime: metav1.NewTime(finishTime),
			},
		}
	}
	return
}

// GetOperationStatus implements dataoperation.StatusHandler for cron DataBackups.
func (c *CronHandler) GetOperationStatus(ctx runtime.ReconcileRequestContext, opStatus *v1alpha1.OperationStatus) (result *v1alpha1.OperationStatus, err error) {
	result = opStatus.DeepCopy()
	object := c.dataBackup
	releaseName := utils.GetDataBackupReleaseName(object.GetName())
	cronjobName := utils.GetDataBackupCronJobName(releaseName)
	cronjobKey := types.NamespacedName{Namespace: object.GetNamespace(), Name: cronjobName}

	// 1. load the CronJob. When the helm release exists but the CronJob is missing (or its status
	// is not observable yet), delete the release so the Executing phase recreates it and retries.
	// GetCronJobStatus is also used as the existence probe; the scheduled time is tracked
	// authoritatively in the DataBackup status rather than copied from the CronJob status.
	if _, err = kubeclient.GetCronJobStatus(c.Client, cronjobKey); err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.Info("Related CronJob missing, will delete helm chart and retry", "cronjobName", cronjobName)
			if err = helm.DeleteReleaseIfExists(releaseName, object.GetNamespace()); err != nil {
				ctx.Log.Error(err, "can't delete DataBackup release", "releaseName", releaseName)
				return
			}
			return
		}
		ctx.Log.Error(err, "can't get DataBackup cronjob", "cronjobName", cronjobName)
		return
	}

	// A Failed phase caused by an invalid schedule is a configuration error rather than a round
	// failure: leave the status untouched so the object stays Failed with the clear reason.
	if opStatus.Phase == common.PhaseFailed && len(opStatus.Conditions) > 0 &&
		opStatus.Conditions[0].Reason == cdatabackup.ReasonInvalidCronSchedule {
		return result, nil
	}

	// 2. find the Job of the current round: jobs are labeled with the CronJob name, take the newest.
	jobs, err := utils.ListDataOperationJobByCronjob(c.Client, cronjobKey)
	if err != nil {
		ctx.Log.Error(err, "can't list DataBackup jobs by cronjob", "cronjobName", cronjobName)
		return
	}
	currentJob := newestJob(jobs)

	// 3. CronJob created but this round's Job/Pod has not shown up yet. Release the CronJob
	// (unsuspend) only while this round holds the target dataset occupation. If the occupation is
	// gone, bounce back to Pending where the schedule gate re-acquires it before re-entering the
	// round (CurrentExecutionID is kept so the same scheduled time is never started twice).
	if currentJob == nil {
		owned, ownErr := c.holdsDatasetOccupation()
		if ownErr != nil {
			ctx.Log.Error(ownErr, "failed to check dataset occupation before releasing backup job")
			err = ownErr
			return
		}
		if !owned {
			ctx.Log.Info("dataset occupation is not held by this backup, suspend the CronJob and return to Pending to acquire the occupation again")
			// close the gate first so that no backup job can be released while the occupation is missing
			if _, ownErr = kubeclient.SetCronJobSuspend(c.Client, cronjobKey, true); ownErr != nil {
				ctx.Log.Error(ownErr, "failed to suspend CronJob while the dataset occupation is missing")
				err = ownErr
				return
			}
			result.Phase = common.PhasePending
			return result, nil
		}
		if _, err = kubeclient.SetCronJobSuspend(c.Client, cronjobKey, false); err != nil {
			ctx.Log.Error(err, "failed to unsuspend CronJob to release the backup job")
			return
		}
		ctx.Log.V(1).Info("cron backup round released, waiting for the job to show up", "executionID", opStatus.CurrentExecutionID)
		return result, nil
	}

	// 4. the round Job exists: close the gate immediately so the next schedule cannot start an
	// overlapping Job while this round is being observed. concurrencyPolicy=Forbid in the chart
	// is the second line of defense.
	if _, err = kubeclient.SetCronJobSuspend(c.Client, cronjobKey, true); err != nil {
		ctx.Log.Error(err, "failed to suspend CronJob after the round job showed up")
		return
	}

	// stamp the recognizable generation identity onto the Job (idempotent), so after a controller
	// restart the round is identifiable from the Kubernetes object itself.
	c.stampExecutionID(currentJob, opStatus.CurrentExecutionID)

	roundPod := c.newestRoundPod(ctx, currentJob)
	if result.Infos == nil {
		result.Infos = map[string]string{}
	}
	result.Infos[cdatabackup.BackupExecutionJob] = currentJob.Name
	if roundPod != nil {
		result.Infos[cdatabackup.BackupExecutionPod] = roundPod.Name
	}

	finishedJobCondition := kubeclient.GetFinishedJobCondition(currentJob)

	// 5a. job still running: keep Executing. Defensively, if a newer job is running while the
	// previous round was already terminal, return to Pending (the occupation has already been
	// released by the Complete/Failed reconciliation) and let the gate start the next round.
	if finishedJobCondition == nil {
		if opStatus.Phase == common.PhaseComplete || opStatus.Phase == common.PhaseFailed {
			ctx.Log.Info("a newer round job is running, return to Pending for the next round")
			result.Phase = common.PhasePending
			result.Duration = "-"
			result.CurrentExecutionID = ""
		}
		return result, nil
	}

	// 5b. finished job observed while the previous terminal status is already recorded: close the
	// round and wait for the next schedule. The terminal condition is preserved on purpose so the
	// failure (or success) result stays diagnosable while the object is Pending.
	if opStatus.Phase == common.PhaseComplete || opStatus.Phase == common.PhaseFailed {
		result.Phase = common.PhasePending
		result.Duration = "-"
		result.CurrentExecutionID = ""
		return result, nil
	}

	// 5c. first observation of the finished round: record a diagnosable terminal result.
	finishTime := finishedJobCondition.LastTransitionTime.Time
	result.Duration = utils.CalculateDuration(currentJob.CreationTimestamp.Time, finishTime)

	podInfo := "no pod observed"
	if roundPod != nil {
		podInfo = fmt.Sprintf("job %s pod %s (phase %s)", currentJob.Name, roundPod.Name, roundPod.Status.Phase)
		if msg := podFailureMessage(roundPod); len(msg) > 0 {
			podInfo += ": " + msg
		}
	} else {
		podInfo = fmt.Sprintf("job %s (no pod observed): %s", currentJob.Name, finishedJobCondition.Message)
	}

	if finishedJobCondition.Type == batchv1.JobComplete {
		result.Phase = common.PhaseComplete
		if opStatus.LastScheduleTime != nil {
			result.LastSuccessfulTime = opStatus.LastScheduleTime
		}
		result.Conditions = []v1alpha1.Condition{
			{
				Type:               common.Complete,
				Status:             v1.ConditionTrue,
				Reason:             "BackupSuccessful",
				Message:            fmt.Sprintf("Backup round %s completed successfully, %s", opStatus.CurrentExecutionID, podInfo),
				LastProbeTime:      metav1.NewTime(time.Now()),
				LastTransitionTime: finishedJobCondition.LastTransitionTime,
			},
		}
		c.Recorder.Eventf(object, v1.EventTypeNormal, common.DataOperationSucceed,
			"DataBackup round %s succeeded", opStatus.CurrentExecutionID)
	} else {
		result.Phase = common.PhaseFailed
		result.Conditions = []v1alpha1.Condition{
			{
				Type:               common.Failed,
				Status:             v1.ConditionTrue,
				Reason:             "BackupFailed",
				Message:            fmt.Sprintf("Backup round %s failed, %s", opStatus.CurrentExecutionID, podInfo),
				LastProbeTime:      metav1.NewTime(time.Now()),
				LastTransitionTime: finishedJobCondition.LastTransitionTime,
			},
		}
		c.Recorder.Eventf(object, v1.EventTypeWarning, common.DataOperationFailed,
			"DataBackup round %s failed: %s", opStatus.CurrentExecutionID, finishedJobCondition.Message)
	}
	return result, nil
}

// holdsDatasetOccupation reports whether the target Dataset OperationRef currently points to this
// DataBackup. The occupation can only be held by one backup round at a time.
func (c *CronHandler) holdsDatasetOccupation() (bool, error) {
	dataset, err := utils.GetDataset(c.Client, c.dataBackup.Spec.Dataset, c.dataBackup.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			return false, nil
		}
		return false, err
	}
	refs := dataset.GetDataOperationInProgress(string(dataoperation.DataBackupType))
	for _, ref := range strings.Split(refs, ",") {
		if ref == c.dataBackup.GetName() {
			return true, nil
		}
	}
	return false, nil
}

// stampExecutionID annotates the round Job with the scheduled generation identity.
func (c *CronHandler) stampExecutionID(job *batchv1.Job, id string) {
	if len(id) == 0 {
		return
	}
	if job.Annotations != nil && job.Annotations[cdatabackup.ExecutionIDAnnotation] == id {
		return
	}
	jobToUpdate := job.DeepCopy()
	if jobToUpdate.Annotations == nil {
		jobToUpdate.Annotations = map[string]string{}
	}
	jobToUpdate.Annotations[cdatabackup.ExecutionIDAnnotation] = id
	if err := c.Update(context.TODO(), jobToUpdate); err != nil {
		// non-fatal: the identity is also held by the DataBackup status, the annotation is retried later
		c.Log.Info("failed to stamp execution id on round job, will retry", "job", job.Name, "executionID", id, "err", err.Error())
	}
}

// newestRoundPod returns the newest pod belonging to the round Job, or nil when the pod has not
// shown up yet (e.g. right after the CronJob created the Job).
func (c *CronHandler) newestRoundPod(ctx runtime.ReconcileRequestContext, job *batchv1.Job) *v1.Pod {
	pods, err := kubeclient.ListPodsForJob(c.Client, job)
	if err != nil {
		ctx.Log.Error(err, "failed to list pods for round job", "job", job.Name)
		return nil
	}
	if len(pods) == 0 {
		return nil
	}
	sort.Slice(pods, func(i, j int) bool {
		if !pods[i].CreationTimestamp.Equal(&pods[j].CreationTimestamp) {
			return pods[i].CreationTimestamp.After(pods[j].CreationTimestamp.Time)
		}
		return pods[i].Name > pods[j].Name
	})
	return &pods[0]
}

// newestJob returns the newest job in the list, which represents the current backup round.
func newestJob(jobs []batchv1.Job) *batchv1.Job {
	if len(jobs) == 0 {
		return nil
	}
	sorted := make([]batchv1.Job, len(jobs))
	copy(sorted, jobs)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].CreationTimestamp.Equal(&sorted[j].CreationTimestamp) {
			return sorted[i].CreationTimestamp.After(sorted[j].CreationTimestamp.Time)
		}
		return sorted[i].Name > sorted[j].Name
	})
	return &sorted[0]
}

// podFailureMessage extracts a human readable termination message from the backup pod so that the
// failure condition is directly diagnosable.
func podFailureMessage(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && len(cs.State.Terminated.Message) > 0 {
			return fmt.Sprintf("container %s terminated: %s", cs.Name, cs.State.Terminated.Message)
		}
		if cs.LastTerminationState.Terminated != nil && len(cs.LastTerminationState.Terminated.Message) > 0 {
			return fmt.Sprintf("container %s last terminated: %s", cs.Name, cs.LastTerminationState.Terminated.Message)
		}
	}
	if len(pod.Status.Message) > 0 {
		return pod.Status.Message
	}
	return ""
}
