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

package dataflow

import (
	"context"
	"reflect"
	"time"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/dataflow"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// mutateStatusFunc re-fetches the typed operation object and applies the desired waiting
// state and, when failCondition is non-nil, the stable dependency failure transition.
type mutateStatusFunc func(waitingFor datav1alpha1.WaitingStatus, failCondition *datav1alpha1.Condition) error

func reconcileDataLoad(ctx reconcileRequestContext) (needRequeue bool, err error) {
	dataLoad, err := utils.GetDataLoad(ctx.Client, ctx.Name, ctx.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("DataLoad not found, skip reconciling")
			return false, nil
		}
		return true, errors.Wrap(err, "failed to get dataload")
	}

	mutateStatusFn := func(waitingFor datav1alpha1.WaitingStatus, failCondition *datav1alpha1.Condition) error {
		tmp, err := utils.GetDataLoad(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		toUpdate.Status.WaitingFor = *waitingFor.DeepCopy()
		applyDependencyFailure(toUpdate, failCondition)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataLoad, dataLoad.Spec.RunAfter, dataLoad.Status, mutateStatusFn)
}

func reconcileDataBackup(ctx reconcileRequestContext) (needRequeue bool, err error) {
	dataBackup, err := utils.GetDataBackup(ctx.Client, ctx.Name, ctx.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("DataBackup not found, skip reconciling")
			return false, nil
		}
		return true, errors.Wrap(err, "failed to get databackup")
	}

	mutateStatusFn := func(waitingFor datav1alpha1.WaitingStatus, failCondition *datav1alpha1.Condition) error {
		tmp, err := utils.GetDataBackup(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		toUpdate.Status.WaitingFor = *waitingFor.DeepCopy()
		applyDependencyFailure(toUpdate, failCondition)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataBackup, dataBackup.Spec.RunAfter, dataBackup.Status, mutateStatusFn)
}

func reconcileDataMigrate(ctx reconcileRequestContext) (needRequeue bool, err error) {
	dataMigrate, err := utils.GetDataMigrate(ctx.Client, ctx.Name, ctx.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("DataMigrate not found, skip reconciling")
			return false, nil
		}
		return true, errors.Wrap(err, "failed to get datamigrate")
	}

	mutateStatusFn := func(waitingFor datav1alpha1.WaitingStatus, failCondition *datav1alpha1.Condition) error {
		tmp, err := utils.GetDataMigrate(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		toUpdate.Status.WaitingFor = *waitingFor.DeepCopy()
		applyDependencyFailure(toUpdate, failCondition)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataMigrate, dataMigrate.Spec.RunAfter, dataMigrate.Status, mutateStatusFn)
}

func reconcileDataProcess(ctx reconcileRequestContext) (needRequeue bool, err error) {
	dataProcess, err := utils.GetDataProcess(ctx.Client, ctx.Name, ctx.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("DataProcess not found, skip reconciling")
			return false, nil
		}
		return true, errors.Wrap(err, "failed to get dataprocess")
	}

	mutateStatusFn := func(waitingFor datav1alpha1.WaitingStatus, failCondition *datav1alpha1.Condition) error {
		tmp, err := utils.GetDataProcess(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		toUpdate.Status.WaitingFor = *waitingFor.DeepCopy()
		applyDependencyFailure(toUpdate, failCondition)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataProcess, dataProcess.Spec.RunAfter, dataProcess.Status, mutateStatusFn)
}

// applyDependencyFailure transitions the operation status to a stable Failed state with a
// diagnostic condition. It is a no-op when failCondition is nil or the object is already
// Failed, so repeated reconciles (e.g. after a controller restart) keep the result stable.
func applyDependencyFailure(toUpdate client.Object, failCondition *datav1alpha1.Condition) {
	if failCondition == nil {
		return
	}

	switch op := toUpdate.(type) {
	case *datav1alpha1.DataLoad:
		applyFailureToStatus(&op.Status, failCondition)
	case *datav1alpha1.DataBackup:
		applyFailureToStatus(&op.Status, failCondition)
	case *datav1alpha1.DataMigrate:
		applyFailureToStatus(&op.Status, failCondition)
	case *datav1alpha1.DataProcess:
		applyFailureToStatus(&op.Status, failCondition)
	}
}

// applyFailureToStatus stamps the Failed phase and a single diagnostic condition unless the
// status has already reached a stable Failed state.
func applyFailureToStatus(status *datav1alpha1.OperationStatus, failCondition *datav1alpha1.Condition) {
	if status.Phase == common.PhaseFailed {
		return
	}
	status.Phase = common.PhaseFailed
	status.Conditions = []datav1alpha1.Condition{*failCondition}
}

// reconcileOperationDataFlow makes the waiting decision for one operation using the
// identity-aware, restart-safe judgment shared with affinity injection. It persists the
// identity (UID) and observed terminal state of every referenced preceding operation:
//   - confirmed Complete unblocks the successor while retaining affinity snapshots;
//   - confirmed Failed or a UID identity conflict fails the successor with diagnostics;
//   - deletion after a confirmed terminal state is trusted from the persisted record and
//     never pushes the successor back to waiting;
//   - unreadable API only triggers a retry, and unknown states keep the successor waiting.
//
// Objects without a RunAfter dependency are never enqueued to this reconciler and stay
// completely untouched.
func reconcileOperationDataFlow(ctx reconcileRequestContext,
	object client.Object,
	runAfter *datav1alpha1.OperationRef,
	opStatus datav1alpha1.OperationStatus,
	mutateStatusFn mutateStatusFunc) (needRequeue bool, err error) {

	// Defensive guard: only objects declaring a RunAfter dependency carry waiting state.
	if runAfter == nil {
		return false, nil
	}

	refs := collectDependencyRefs(ctx.Namespace, runAfter)
	records := opStatus.WaitingFor.Dependencies

	var (
		updatedRecords = records
		firstFailure   *dataflow.DependencyCheckResult
	)

	for i := range refs {
		ref := refs[i]
		recorded := dataflow.FindDependencyRecord(records, &ref, ctx.Namespace)
		result := dataflow.CheckPrecedingOperation(ctx.Client, &ref, ctx.Namespace, recorded)

		if result.Verdict == dataflow.DependencyWaiting {
			if result.Err != nil {
				if apierrors.IsNotFound(result.Err) {
					// The dependency is still unknown (never observed, or deleted before any
					// terminal state was confirmed): keep waiting and retry later.
					ctx.Recorder.Eventf(object, corev1.EventTypeWarning, common.DataOperationNotFound,
						"Preceding operation %s \"%s/%s\" not found",
						ref.Kind, ref.Namespace, ref.Name)
					return true, nil
				}
				// The API is temporarily unreadable: retry without changing persisted state.
				return true, errors.Wrapf(result.Err,
					"failed to get preceding operation status (RunAfter: %v)", ref)
			}

			ctx.Recorder.Eventf(object, corev1.EventTypeNormal, common.DataOperationWaiting,
				"Waiting for operation %s \"%s/%s\" to complete",
				ref.Kind, ref.Namespace, ref.Name)
		}

		if result.Verdict == dataflow.DependencyFailed || result.Verdict == dataflow.DependencyConflict {
			if firstFailure == nil {
				failureCopy := result
				firstFailure = &failureCopy
			}
		}

		updatedRecords = dataflow.UpsertDependencyRecord(updatedRecords, result.Record)
	}

	waitingFor := opStatus.WaitingFor.DeepCopy()
	waitingFor.Dependencies = updatedRecords

	if firstFailure != nil {
		// Stable, diagnosable failure. Clear the waiting flag so the main operation
		// controller owns the Failed phase (e.g. TTL cleanup) from now on.
		waitingFor.OperationComplete = ptr.To(false)
		failCondition := buildDependencyFailedCondition(*firstFailure)
		if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return mutateStatusFn(*waitingFor, failCondition)
		}); updateErr != nil {
			return true, errors.Wrapf(updateErr, "failed to update operation status for dependency failure")
		}

		ctx.Recorder.Eventf(object, corev1.EventTypeWarning, failCondition.Reason, failCondition.Message)
		return false, nil
	}

	if firstFailure == nil && isAnyDependencyWaiting(updatedRecords, refs, ctx.Namespace) {
		// Persist identity pins (and other observations) but stay in the waiting state.
		if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return mutateStatusFn(*waitingFor, nil)
		}); updateErr != nil {
			return true, errors.Wrapf(updateErr, "failed to update operation status waitingFor")
		}
		return true, nil
	}

	// All referenced preceding operations are confirmed Complete. The terminal records
	// (including NodeAffinity snapshots) are persisted together with clearing the flag.
	waitingFor.OperationComplete = ptr.To(false)
	if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return mutateStatusFn(*waitingFor, nil)
	}); updateErr != nil {
		return true, errors.Wrapf(updateErr, "failed to update operation status waitingFor.OperationComplete=false")
	}

	return false, nil
}

// collectDependencyRefs resolves every distinct preceding object this operation depends
// on: the spec.runAfter referent and, when it points to a different object, the
// affinityStrategy.dependOn referent. Both follow the exact same identity judgment.
func collectDependencyRefs(opNamespace string, runAfter *datav1alpha1.OperationRef) []datav1alpha1.ObjectRef {
	runAfterRef := dataflow.ResolveDependencyRef(&runAfter.ObjectRef, opNamespace)
	refs := []datav1alpha1.ObjectRef{runAfterRef}

	if runAfter.AffinityStrategy.DependOn != nil {
		dependOnRef := dataflow.ResolveDependencyRef(runAfter.AffinityStrategy.DependOn, opNamespace)
		if !dataflow.SameObjectRef(dependOnRef, runAfterRef) {
			refs = append(refs, dependOnRef)
		}
	}
	return refs
}

// isAnyDependencyWaiting tells whether at least one referenced dependency lacks a
// confirmed terminal record in the freshly observed state.
func isAnyDependencyWaiting(updated []datav1alpha1.DependencyStatus,
	refs []datav1alpha1.ObjectRef, opNamespace string) bool {
	for i := range refs {
		record := dataflow.FindDependencyRecord(updated, &refs[i], opNamespace)
		if record == nil || (record.Phase != common.PhaseComplete && record.Phase != common.PhaseFailed) {
			return true
		}
	}
	return false
}

// buildDependencyFailedCondition converts the stable dependency verdict into a Failed
// condition carrying the diagnostic reason and message.
func buildDependencyFailedCondition(result dataflow.DependencyCheckResult) *datav1alpha1.Condition {
	reason := common.DataOperationDependencyFailed
	if result.Verdict == dataflow.DependencyConflict {
		reason = common.DataOperationDependencyConflict
	}

	now := metav1.NewTime(time.Now())
	return &datav1alpha1.Condition{
		Type:               common.Failed,
		Status:             corev1.ConditionTrue,
		Reason:             reason,
		Message:            result.Err.Error(),
		LastProbeTime:      now,
		LastTransitionTime: now,
	}
}
