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
	"fmt"
	"reflect"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/dataflow"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func reconcileDataLoad(ctx reconcileRequestContext) (needRequeue bool, err error) {
	dataLoad, err := utils.GetDataLoad(ctx.Client, ctx.Name, ctx.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("DataLoad not found, skip reconciling")
			return false, nil
		}
		return true, errors.Wrap(err, "failed to get dataload")
	}

	updateStatusFn := func(mutate func(status *datav1alpha1.OperationStatus)) error {
		tmp, err := utils.GetDataLoad(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		mutate(&toUpdate.Status)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataLoad, dataLoad.Spec.RunAfter, dataLoad.Status, updateStatusFn)
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

	updateStatusFn := func(mutate func(status *datav1alpha1.OperationStatus)) error {
		tmp, err := utils.GetDataBackup(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		mutate(&toUpdate.Status)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataBackup, dataBackup.Spec.RunAfter, dataBackup.Status, updateStatusFn)
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

	updateStatusFn := func(mutate func(status *datav1alpha1.OperationStatus)) error {
		tmp, err := utils.GetDataMigrate(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		mutate(&toUpdate.Status)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataMigrate, dataMigrate.Spec.RunAfter, dataMigrate.Status, updateStatusFn)
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

	updateStatusFn := func(mutate func(status *datav1alpha1.OperationStatus)) error {
		tmp, err := utils.GetDataProcess(ctx.Client, ctx.Name, ctx.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				return nil
			}
			return err
		}

		toUpdate := tmp.DeepCopy()
		mutate(&toUpdate.Status)
		if !reflect.DeepEqual(toUpdate.Status, tmp.Status) {
			return ctx.Client.Status().Update(context.TODO(), toUpdate)
		}

		return nil
	}

	return reconcileOperationDataFlow(ctx, dataProcess, dataProcess.Spec.RunAfter, dataProcess.Status, updateStatusFn)
}

func reconcileOperationDataFlow(ctx reconcileRequestContext,
	object client.Object,
	runAfter *datav1alpha1.OperationRef,
	opStatus datav1alpha1.OperationStatus,
	updateStatusFn func(mutate func(status *datav1alpha1.OperationStatus)) error) (needRequeue bool, err error) {

	// Objects without a RunAfter dependency are not governed by the dataflow waiting logic.
	if runAfter == nil {
		return false, nil
	}

	opRefNamespace := ctx.Namespace
	if len(runAfter.Namespace) != 0 {
		opRefNamespace = runAfter.Namespace
	}

	cached := opStatus.WaitingFor.PrecedingOperation
	decision, observed, err := dataflow.ResolvePrecedingOperation(ctx.Client, &runAfter.ObjectRef, opRefNamespace, cached)
	if err != nil {
		// API temporarily unreadable: only retry, no persisted state is touched.
		return true, errors.Wrapf(err, "failed to get preceding operation status (RunAfter: %v)", runAfter)
	}

	refKey := fmt.Sprintf("%s \"%s/%s\"", runAfter.Kind, opRefNamespace, runAfter.Name)

	switch decision {
	case dataflow.DependencyRetry:
		return true, errors.Errorf("temporarily failed to resolve preceding operation %s, retry later", refKey)

	case dataflow.DependencyWaiting, dataflow.DependencyNotFound:
		// Persist the pinned identity (if newly observed) so that a later same-name
		// recreation can be detected, while keeping the operation in the waiting state.
		if !reflect.DeepEqual(observed, cached) {
			mutate := func(status *datav1alpha1.OperationStatus) {
				status.WaitingFor.PrecedingOperation = observed
			}
			if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				return updateStatusFn(mutate)
			}); updateErr != nil {
				return true, errors.Wrapf(updateErr, "failed to persist preceding operation identity (RunAfter: %v)", runAfter)
			}
		}

		if decision == dataflow.DependencyNotFound {
			// The dependency outcome is unknown because the referenced object is absent and
			// no terminal state has ever been confirmed.
			ctx.Recorder.Eventf(object, corev1.EventTypeWarning, common.DataOperationNotFound, "Preceding operation %s not found", refKey)
		} else {
			ctx.Recorder.Eventf(object, corev1.EventTypeNormal, common.DataOperationWaiting, "Waiting for operation %s to complete", refKey)
		}
		return true, nil

	case dataflow.DependencyComplete:
		// Confirm success: unblock the successor and persist the terminal observation
		// (including the node affinity snapshot). A later deletion of the preceding
		// operation will not push the successor back to waiting.
		mutate := func(status *datav1alpha1.OperationStatus) {
			status.WaitingFor.OperationComplete = ptr.To(false)
			status.WaitingFor.PrecedingOperation = observed
		}
		if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return updateStatusFn(mutate)
		}); updateErr != nil {
			return true, errors.Wrapf(updateErr, "failed to update operation status after preceding operation %s completed", refKey)
		}
		return false, nil

	case dataflow.DependencyFailed, dataflow.DependencyIdentityConflict:
		// Persist the stable terminal failure and wake the operation controller, which
		// transitions the successor itself to Failed with a diagnosable condition.
		mutate := func(status *datav1alpha1.OperationStatus) {
			status.WaitingFor.OperationComplete = ptr.To(false)
			status.WaitingFor.PrecedingOperation = observed
		}
		if updateErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			return updateStatusFn(mutate)
		}); updateErr != nil {
			return true, errors.Wrapf(updateErr, "failed to update operation status with preceding operation terminal failure (RunAfter: %v)", runAfter)
		}

		reason := common.DataOperationDependencyFailed
		if decision == dataflow.DependencyIdentityConflict {
			reason = common.DataOperationIdentityConflict
		}
		ctx.Recorder.Eventf(object, corev1.EventTypeWarning, reason,
			"Preceding operation %s terminated with failure: %s", refKey, observed.Message)
		return false, nil
	}

	return true, nil
}
