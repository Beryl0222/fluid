/*
Copyright 2026 The Fluid Authors.

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
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	cronparse "github.com/fluid-cloudnative/fluid/pkg/utils/cron"
)

const (
	// missedScheduleThreshold bounds how late a scheduled time is still made up.
	// If the controller was absent longer than this, the missed round is skipped instead
	// of running a stale backup, while newer rounds keep being scheduled normally.
	missedScheduleThreshold = 10 * time.Minute
	// maxScheduledRequeueInterval caps the wait when the next schedule is far away so that
	// clock drift and late spec changes are still picked up in bounded time.
	maxScheduledRequeueInterval = 10 * time.Minute
	// executionIDTimeFormat derives the recognizable identity of an execution round from
	// its scheduled time (UTC). The token only contains digits and the letter T, so it is
	// safe to use as a label or annotation value.
	executionIDTimeFormat = "20060102T150405"
)

// executionID returns the identity of the backup round scheduled at the given time.
func executionID(scheduledTime time.Time) string {
	return scheduledTime.UTC().Format(executionIDTimeFormat)
}

// ReconcileScheduledRound implements dataoperation.ScheduledOperation. It is the only place that
// decides which scheduled round the DataBackup currently belongs to. All decisions are derived
// from the DataBackup status (LastScheduleTime, CurrentExecutionID) and the parsed schedule, so
// controller restarts converge without ever starting the same scheduled time twice.
func (r *dataBackupOperation) ReconcileScheduledRound(
	ctx cruntime.ReconcileRequestContext,
	opStatus *datav1alpha1.OperationStatus,
	acquireOccupation func() error,
	releaseOccupation func() error,
) (roundOpen bool, requeueAfter time.Duration, err error) {
	log := ctx.Log.WithName("ReconcileScheduledRound")
	dataBackup := r.dataBackup
	now := time.Now()

	// 1. A round already recorded as open must be driven to completion before any new round is
	// considered. Re-enter Executing (idempotent) after making sure the dataset occupation is held.
	// This covers controller restarts at any in-flight moment (CronJob present but Pod not yet
	// observed, Pod running, Pod terminated before status was written, ...).
	if len(opStatus.CurrentExecutionID) != 0 {
		if err = acquireOccupation(); err != nil {
			log.Error(err, "failed to acquire dataset occupation for the open backup round",
				"executionID", opStatus.CurrentExecutionID)
			return false, 0, err
		}
		if opStatus.Phase != common.PhaseExecuting {
			opStatus.Phase = common.PhaseExecuting
			if err = r.UpdateOperationApiStatus(opStatus); err != nil {
				return false, 0, err
			}
		}
		return true, 0, nil
	}

	// 2. No open round: release any stale occupation. The operation is idempotent and tolerates a
	// dataset that has already been deleted, so no OperationRef is left behind after a restart.
	if err = releaseOccupation(); err != nil {
		log.Error(err, "failed to release stale dataset occupation")
		return false, 0, err
	}

	// 3. parse the schedule. The schedule was already validated in the None phase before any
	// workload existed; this additionally reports an expression edited into an invalid value
	// without ever creating or releasing a workload.
	schedule, parseErr := cronparse.Parse(dataBackup.Spec.Schedule)
	if parseErr != nil {
		log.Error(parseErr, "invalid cron schedule, no backup round will be started")
		r.Recorder.Eventf(dataBackup, v1.EventTypeWarning, cdatabackup.ReasonInvalidCronSchedule,
			"Invalid cron schedule %q: %v", dataBackup.Spec.Schedule, parseErr)
		// record the diagnosable condition once; keep the phase Pending and do not touch any workload
		if len(opStatus.Conditions) != 1 || opStatus.Conditions[0].Reason != cdatabackup.ReasonInvalidCronSchedule {
			opStatus.Conditions = []datav1alpha1.Condition{
				{
					Type:               common.Failed,
					Status:             v1.ConditionTrue,
					Reason:             cdatabackup.ReasonInvalidCronSchedule,
					Message:            parseErr.Error(),
					LastProbeTime:      metav1.NewTime(now),
					LastTransitionTime: metav1.NewTime(now),
				},
			}
			_ = r.UpdateOperationApiStatus(opStatus)
		}
		return false, 20 * time.Second, parseErr
	}

	// 4. determine the scheduled time window. Scheduled times are exclusive after the lower bound,
	// which is either the object creation time (first round) or the last scheduled time, so the
	// same scheduled time can never be started for a second time.
	after := dataBackup.CreationTimestamp.Time
	if opStatus.LastScheduleTime != nil {
		after = opStatus.LastScheduleTime.Time
	}
	windowStart := now.Add(-missedScheduleThreshold)
	if windowStart.Before(after) {
		windowStart = after
	}

	// enumerate scheduled times inside (windowStart, now]; the window is short and the minimum
	// cron granularity is one minute, so this loop stays small.
	var due time.Time
	cursor := windowStart
	for {
		var next time.Time
		next, err = schedule.Next(cursor)
		if err != nil {
			log.Error(err, "failed to compute next scheduled time")
			return false, 0, err
		}
		if next.After(now) {
			break
		}
		due = next
		cursor = next
	}

	// 5. no scheduled time due: wait until the next schedule without holding the dataset occupation.
	if due.IsZero() {
		var next time.Time
		next, err = schedule.Next(now)
		if err != nil {
			return false, 0, err
		}
		requeueAfter = next.Sub(now)
		if requeueAfter > maxScheduledRequeueInterval {
			requeueAfter = maxScheduledRequeueInterval
		}
		if requeueAfter < time.Second {
			requeueAfter = time.Second
		}
		log.V(1).Info("no backup schedule due, wait for the next round", "nextSchedule", next, "requeueAfter", requeueAfter)
		return false, requeueAfter, nil
	}

	// 6. a scheduled time is due: take the target dataset occupation first and only then record the
	// open round with its recognizable identity. The workload is released later in the Executing phase.
	if err = acquireOccupation(); err != nil {
		log.Error(err, "failed to acquire dataset occupation for the scheduled backup round", "scheduledTime", due)
		return false, 0, err
	}

	id := executionID(due)
	scheduledAt := metav1.NewTime(due)
	opStatus.Phase = common.PhaseExecuting
	opStatus.Duration = "Unfinished"
	opStatus.Conditions = []datav1alpha1.Condition{}
	opStatus.LastScheduleTime = &scheduledAt
	opStatus.CurrentExecutionID = id

	if err = r.UpdateOperationApiStatus(opStatus); err != nil {
		return false, 0, err
	}
	r.Recorder.Eventf(dataBackup, v1.EventTypeNormal, "BackupRoundScheduled",
		"Scheduled backup round %s (scheduled at %s) acquired dataset occupation", id, due.Format(time.RFC3339))
	log.Info("scheduled backup round opened", "executionID", id, "scheduledTime", due)
	return true, 0, nil
}
