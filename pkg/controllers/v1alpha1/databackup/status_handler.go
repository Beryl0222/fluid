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
	"fmt"
	"time"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type OnceHandler struct {
	dataBackup *v1alpha1.DataBackup
}

var _ dataoperation.StatusHandler = &OnceHandler{}

// backupPodMissingGracePeriod is how long the controller tolerates a missing backup pod
// when no terminal state has ever been observed. It absorbs pod creation and informer
// cache lag; once it elapses, the DataBackup is marked Failed with a diagnosable condition
// instead of being stuck in Executing or guessing an unconfirmed successful result.
var backupPodMissingGracePeriod = 2 * time.Minute

// terminalEvidenceKeys lists all infos keys that make up the persisted terminal-state
// evidence of a backup pod.
var terminalEvidenceKeys = []string{
	cdatabackup.BackupPodName,
	cdatabackup.BackupPodUID,
	cdatabackup.BackupPodNodeName,
	cdatabackup.BackupPodPhase,
	cdatabackup.BackupPodStartTime,
	cdatabackup.BackupPodFinishTime,
}

// GetOperationStatus resolves the DataBackup status from the backup pod.
//
// Once the backup pod is observed in a terminal phase (Succeeded/Failed), the execution
// identity (pod name/UID) and the result (phase, node, start/finish timestamps) are
// persisted into the DataBackup status together with the phase transition. The controller
// can therefore finish the remaining bookkeeping (phase, infos, dataset lock release, TTL
// and finalizer) even if the pod is garbage collected before the next reconcile or the
// controller is restarted. A pod that disappears without any observed terminal state is
// never treated as successful: after a bounded grace period the DataBackup is marked
// Failed with the BackupPodMissing reason.
func (o *OnceHandler) GetOperationStatus(ctx cruntime.ReconcileRequestContext, opStatus *v1alpha1.OperationStatus) (result *v1alpha1.OperationStatus, err error) {
	result = opStatus.DeepCopy()
	object := o.dataBackup
	backupPodName := utils.GetDataBackupPodName(object.GetName())

	backupPod, err := kubeclient.GetPodByName(ctx.Client, backupPodName, object.GetNamespace())
	if err != nil {
		// Real API errors (everything except NotFound, which is normalized to (nil, nil)
		// by GetPodByName) keep being propagated to the reconcile retry loop.
		ctx.Log.Error(err, "Failed to get databackup-pod")
		return
	}

	// Case 1: the backup pod still exists, observe its state directly.
	if backupPod != nil {
		// The pod came back, any previous missing-grace marker is obsolete.
		if _, ok := result.Infos[cdatabackup.BackupPodMissingSince]; ok {
			ctx.Log.Info("backup pod reappeared, clear the missing grace marker", "pod", backupPodName)
			delete(result.Infos, cdatabackup.BackupPodMissingSince)
		}

		// A persisted UID that differs from the live pod means the same-name executor was
		// recreated (e.g. the DataBackup was deleted and recreated). Discard the stale
		// terminal evidence and re-baseline on the new pod instead of misattributing results.
		if persistedUID, ok := result.Infos[cdatabackup.BackupPodUID]; ok && persistedUID != "" && persistedUID != string(backupPod.UID) {
			ctx.Log.Info("detected a recreated backup pod with a new UID, discard stale terminal evidence and re-observe",
				"pod", backupPodName, "persistedPodUID", persistedUID, "livePodUID", string(backupPod.UID),
				"dataBackupUID", string(object.GetUID()))
			clearTerminalEvidence(result.Infos)
		}

		// Running or pending pod: no phase transition and no status write, preserving the
		// behavior for normally running pods.
		if !kubeclient.IsFinishedPod(backupPod) {
			return result, nil
		}

		// Terminal state observed. Persist the execution identity and the result in the SAME
		// status update as the phase transition so that a later pod deletion or a controller
		// restart cannot strip the DataBackup of its terminal evidence.
		finishTime := getPodFinishTime(backupPod)
		result.Duration = utils.CalculateDuration(backupPod.CreationTimestamp.Time, finishTime)
		if result.Infos == nil {
			result.Infos = map[string]string{}
		}
		persistTerminalEvidence(result.Infos, backupPod, finishTime)

		if kubeclient.IsSucceededPod(backupPod) {
			result.Phase = common.PhaseComplete
			result.Conditions = []v1alpha1.Condition{
				buildBackupCondition(common.Complete, "BackupSuccessful", "Backup Pod exec successfully and finish", finishTime),
			}
		} else if kubeclient.IsFailedPod(backupPod) {
			result.Phase = common.PhaseFailed
			result.Conditions = []v1alpha1.Condition{
				buildBackupCondition(common.Failed, "BackupFailed", "Backup Pod exec failed and exit", finishTime),
			}
		}
		return result, nil
	}

	// Case 2: the backup pod is gone.
	// 2a: the terminal state was observed and persisted before the pod was deleted. Rebuild
	// any drifted phase/duration from the CR-borne evidence and let the reconcile continue
	// without accessing the pod.
	if hasTerminalEvidence(opStatus.Infos) {
		ctx.Log.Info("backup pod is deleted after its terminal state was observed, rely on the persisted terminal evidence",
			"pod", backupPodName, "podUID", opStatus.Infos[cdatabackup.BackupPodUID])
		reconstructTerminalStatusFromEvidence(ctx, result)
		return result, nil
	}

	// 2b: the DataBackup is already terminal without structured evidence (for example a
	// failure synthesized by an older controller version). Trust the persisted phase instead
	// of inferring anything from the missing pod.
	if result.Phase == common.PhaseComplete || result.Phase == common.PhaseFailed {
		ctx.Log.Info("backup pod is missing while DataBackup is already in a terminal phase, keep the persisted phase",
			"pod", backupPodName, "phase", result.Phase)
		return result, nil
	}

	// 2c: the pod is missing and a terminal state was NEVER observed. A successful result
	// must not be guessed. Apply a bounded grace period to tolerate pod creation / cache lag,
	// then mark the DataBackup Failed with a diagnosable condition.
	since, ok := result.Infos[cdatabackup.BackupPodMissingSince]
	if !ok || since == "" {
		if result.Infos == nil {
			result.Infos = map[string]string{}
		}
		result.Infos[cdatabackup.BackupPodMissingSince] = time.Now().UTC().Format(time.RFC3339)
		ctx.Log.Info("backup pod not found and no terminal state has been observed, start the missing grace period",
			"pod", backupPodName, "gracePeriod", backupPodMissingGracePeriod.String())
		return result, nil
	}

	missingAt, parseErr := time.Parse(time.RFC3339, since)
	if parseErr != nil {
		// A malformed marker must not cause an immediate failure: restart the grace period.
		ctx.Log.Error(parseErr, "failed to parse the backup pod missing timestamp, restart the grace period", "raw", since)
		result.Infos[cdatabackup.BackupPodMissingSince] = time.Now().UTC().Format(time.RFC3339)
		return result, nil
	}

	if time.Since(missingAt) < backupPodMissingGracePeriod {
		ctx.Log.V(1).Info("backup pod still missing within the grace period, keep waiting",
			"pod", backupPodName, "elapsed", time.Since(missingAt).String())
		// Return the untouched status so that no redundant status update is issued; the
		// executing-phase reconcile requeues periodically anyway.
		return opStatus.DeepCopy(), nil
	}

	// Grace period exhausted: surface a diagnosable failure and unblock lock release, TTL
	// and finalizer processing.
	reason := cdatabackup.BackupPodMissingReason
	message := fmt.Sprintf("Backup pod %s disappeared before its terminal state was observed, mark the DataBackup as Failed instead of guessing an unconfirmed result", backupPodName)
	ctx.Log.Error(fmt.Errorf("backup pod missing before its terminal state was observed"), message,
		"pod", backupPodName, "namespace", object.GetNamespace(), "name", object.GetName(),
		"dataBackupUID", string(object.GetUID()))
	if ctx.Recorder != nil {
		ctx.Recorder.Eventf(object, corev1.EventTypeWarning, cdatabackup.BackupPodMissingReason, "%s", message)
	}

	delete(result.Infos, cdatabackup.BackupPodMissingSince)
	result.Phase = common.PhaseFailed
	result.Duration = utils.CalculateDuration(object.GetCreationTimestamp().Time, time.Now())
	result.Conditions = []v1alpha1.Condition{
		buildBackupCondition(common.Failed, reason, message, time.Now()),
	}
	return result, nil
}

// getPodFinishTime returns the pod's terminal transition time, falling back to the current
// time when the pod does not report any condition.
func getPodFinishTime(pod *corev1.Pod) time.Time {
	if len(pod.Status.Conditions) != 0 {
		return pod.Status.Conditions[0].LastTransitionTime.Time
	}
	// fail to get finishTime, use current time as default
	return time.Now()
}

// persistTerminalEvidence writes the execution identity and result of the terminated backup
// pod into the DataBackup infos so that the evidence survives the pod's deletion.
func persistTerminalEvidence(infos map[string]string, pod *corev1.Pod, finishTime time.Time) {
	infos[cdatabackup.BackupPodName] = pod.Name
	infos[cdatabackup.BackupPodUID] = string(pod.UID)
	infos[cdatabackup.BackupPodNodeName] = pod.Spec.NodeName
	infos[cdatabackup.BackupPodPhase] = string(pod.Status.Phase)
	infos[cdatabackup.BackupPodStartTime] = pod.CreationTimestamp.UTC().Format(time.RFC3339)
	infos[cdatabackup.BackupPodFinishTime] = finishTime.UTC().Format(time.RFC3339)
}

// clearTerminalEvidence drops all backup-pod terminal evidence from the given infos.
func clearTerminalEvidence(infos map[string]string) {
	for _, key := range terminalEvidenceKeys {
		delete(infos, key)
	}
}

// hasTerminalEvidence reports whether the infos carry a complete, terminal observation of
// the backup pod.
func hasTerminalEvidence(infos map[string]string) bool {
	if infos == nil {
		return false
	}
	if _, ok := infos[cdatabackup.BackupPodFinishTime]; !ok {
		return false
	}
	phase := infos[cdatabackup.BackupPodPhase]
	return phase == string(corev1.PodSucceeded) || phase == string(corev1.PodFailed)
}

// reconstructTerminalStatusFromEvidence rebuilds the phase, conditions and duration from the
// persisted evidence when the backup pod is already gone. Fields that are already consistent
// are left untouched; only drifted fields are repaired and logged.
func reconstructTerminalStatusFromEvidence(ctx cruntime.ReconcileRequestContext, result *v1alpha1.OperationStatus) {
	infos := result.Infos

	finishTime, err := time.Parse(time.RFC3339, infos[cdatabackup.BackupPodFinishTime])
	if err != nil {
		// Do not guess anything from unparseable evidence: the already persisted phase and
		// conditions remain authoritative.
		ctx.Log.Error(err, "failed to parse the persisted backup pod finish time, keep the current phase",
			"raw", infos[cdatabackup.BackupPodFinishTime])
		return
	}

	var wantPhase common.Phase
	var condition v1alpha1.Condition
	switch infos[cdatabackup.BackupPodPhase] {
	case string(corev1.PodSucceeded):
		wantPhase = common.PhaseComplete
		condition = buildBackupCondition(common.Complete, "BackupSuccessful", "Backup Pod exec successfully and finish", finishTime)
	case string(corev1.PodFailed):
		wantPhase = common.PhaseFailed
		condition = buildBackupCondition(common.Failed, "BackupFailed", "Backup Pod exec failed and exit", finishTime)
	default:
		ctx.Log.Info("persisted terminal evidence carries an unknown pod phase, keep the current phase",
			"podPhase", infos[cdatabackup.BackupPodPhase])
		return
	}

	// Recompute the duration deterministically from the persisted executor timestamps only
	// when it is missing, covering cases where the status update carrying the duration was
	// lost, while avoiding redundant status writes during steady-state reconciliation.
	if result.Duration == "" || result.Duration == "Unfinished" {
		if startTime, err := time.Parse(time.RFC3339, infos[cdatabackup.BackupPodStartTime]); err == nil {
			if duration := utils.CalculateDuration(startTime, finishTime); duration != "" {
				result.Duration = duration
			}
		}
	}

	if result.Phase != wantPhase {
		ctx.Log.Info("reconstruct the DataBackup terminal phase from persisted evidence after the backup pod was deleted",
			"oldPhase", result.Phase, "newPhase", wantPhase, "podUID", infos[cdatabackup.BackupPodUID])
		result.Phase = wantPhase
		result.Conditions = []v1alpha1.Condition{condition}
	}
}

// buildBackupCondition constructs a terminal DataBackup condition with consistent timestamps.
func buildBackupCondition(conditionType common.ConditionType, reason, message string, transitionTime time.Time) v1alpha1.Condition {
	return v1alpha1.Condition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastProbeTime:      metav1.NewTime(time.Now()),
		LastTransitionTime: metav1.NewTime(transitionTime),
	}
}
