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

package base

import (
	"context"
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

func getDataOperationKey(object client.Object) string {
	if object == nil {
		return ""
	}
	// Check for typed nil pointer
	rObj := reflect.ValueOf(object)
	if rObj.Kind() == reflect.Pointer && rObj.IsNil() {
		return ""
	}
	return object.GetName()
}

// dedupeNamespacedNames removes duplicated namespaced names while preserving the first
// occurrence order. A Dataset referenced by both ends of a DataMigrate (from == to) is
// therefore occupied only once.
func dedupeNamespacedNames(names []types.NamespacedName) []types.NamespacedName {
	seen := make(map[types.NamespacedName]struct{}, len(names))
	ret := make([]types.NamespacedName, 0, len(names))
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		ret = append(ret, name)
	}
	return ret
}

// resolveLockTargetDatasetNames returns the deduplicated namespaced names of all Datasets
// that the operation must occupy. Operations implementing dataoperation.MultiDatasetLockOperation
// (e.g. Dataset-to-Dataset DataMigrate) may occupy multiple Datasets whose names are derived
// from the operation spec, so that a Dataset which has been deleted is still discoverable
// during occupancy release. All other operations fall back to their single target Dataset.
func resolveLockTargetDatasetNames(operation dataoperation.OperationInterface) ([]types.NamespacedName, error) {
	if multi, ok := operation.(dataoperation.MultiDatasetLockOperation); ok {
		if names := dedupeNamespacedNames(multi.GetLockTargetDatasetNamespacedNames()); len(names) > 0 {
			return names, nil
		}
	}

	dataset, err := operation.GetTargetDataset()
	if err != nil {
		return nil, err
	}
	return []types.NamespacedName{{Name: dataset.Name, Namespace: dataset.Namespace}}, nil
}

// SetDataOperationInTargetDataset sets the status of every occupied Dataset to mark the data
// operation being performed. For operations occupying multiple Datasets (Dataset-to-Dataset
// DataMigrate), all endpoints are occupied with the same operation identity before the
// function returns successfully: if any endpoint cannot be occupied, every Dataset occupied
// in the current reconciliation pass is rolled back so that no half-lock is persisted and the
// workload is never started (the operation stays in Pending and is requeued).
func SetDataOperationInTargetDataset(ctx cruntime.ReconcileRequestContext, operation dataoperation.OperationInterface, engine OperationEngine) error {
	targetDataset := ctx.Dataset
	object := operation.GetOperationObject()

	// check if the bounded runtime is ready
	// TODO: can we remove this check? dataset binds runtime only when runtime is ready!
	ready := engine.CheckRuntimeReady()
	if !ready {
		ctx.Log.V(1).Info("Bounded accelerate runtime not ready", "targetDataset", targetDataset)
		ctx.Recorder.Eventf(object,
			v1.EventTypeNormal,
			common.RuntimeNotReady,
			"Bounded accelerate runtime not ready")
		return fmt.Errorf("bounded accelerate runtime not ready")
	}

	operationTypeName := string(operation.GetOperationType())
	dataOpKey := getDataOperationKey(object)

	targetNames, err := resolveLockTargetDatasetNames(operation)
	if err != nil {
		ctx.Log.Error(err, "can't resolve datasets to occupy")
		return err
	}

	// Occupy every target Dataset independently. Each Dataset is re-fetched and updated
	// inside a conflict retry, so concurrent status writers (other operations or the runtime
	// reconciler) can never make us overwrite their changes. Re-running this function is
	// idempotent: an already persisted occupancy of the same operation is observed as a no-op.
	occupied := make([]types.NamespacedName, 0, len(targetNames))
	for _, targetName := range targetNames {
		err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			dataset, err := utils.GetDataset(ctx.Client, targetName.Name, targetName.Namespace)
			if err != nil {
				return err
			}

			// set current data operation in the target dataset
			datasetToUpdate := dataset.DeepCopy()
			datasetToUpdate.SetDataOperationInProgress(operationTypeName, dataOpKey)
			// different operation may set other fields
			operation.SetTargetDatasetStatusInProgress(datasetToUpdate)

			if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
				if err := ctx.Client.Status().Update(context.TODO(), datasetToUpdate); err != nil {
					ctx.Log.Info("fail to update target dataset's lock, will requeue", "targetDatasetName", targetName)
					return err
				}
			}
			return nil
		})
		if err != nil {
			ctx.Log.Error(err, "failed to occupy target dataset, rolling back partial occupancy",
				"failedDataset", targetName, "occupiedDatasets", occupied)
			// The rollback only removes the current operation's own OperationRef entry and
			// never touches references owned by other operations. It is best effort: if it
			// fails as well, the operation remains in Pending and the next reconciliation
			// converges from the persisted Dataset status instead of starting any workload.
			if rollbackErr := releaseLocksOnDatasets(ctx, operation, occupied); rollbackErr != nil {
				ctx.Log.Error(rollbackErr, "failed to roll back partial occupancy, will keep converging on next reconciliation")
			}
			return err
		}
		occupied = append(occupied, targetName)
	}

	return nil
}

// releaseLockOnDataset releases the operation's occupancy on a single Dataset. A Dataset
// that no longer exists is treated as already released. Conflicts are retried with a fresh
// fetch so that references owned by other operations are never removed.
func releaseLockOnDataset(ctx cruntime.ReconcileRequestContext, operation dataoperation.OperationInterface, targetName types.NamespacedName) error {
	dataOpKey := getDataOperationKey(operation.GetOperationObject())
	operationTypeName := string(operation.GetOperationType())

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		dataset, err := utils.GetDataset(ctx.Client, targetName.Name, targetName.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				// The Dataset has been deleted (or recreated without the occupancy), there is
				// no persisted lock to release on this endpoint.
				ctx.Log.Info("can't find target dataset when releasing lock, skip", "dataset", targetName)
				return nil
			}
			// other error
			return err
		}

		datasetToUpdate := dataset.DeepCopy()
		dataOpRef := datasetToUpdate.RemoveDataOperationInProgress(operationTypeName, dataOpKey)

		if dataOpRef == "" {
			// different operation may set other fields
			operation.RemoveTargetDatasetStatusInProgress(datasetToUpdate)
		}
		if !reflect.DeepEqual(datasetToUpdate.Status, dataset.Status) {
			if err = ctx.Client.Status().Update(context.TODO(), datasetToUpdate); err != nil {
				return err
			}
		}
		return nil
	})
}

// releaseLocksOnDatasets releases the operation's occupancy on all given Datasets. Every
// endpoint is attempted even if one of them fails, so a transient failure on one endpoint
// does not block the cleanup of the others. The first encountered error is returned for
// requeue, while successful endpoint releases are persisted and become no-ops on retry.
func releaseLocksOnDatasets(ctx cruntime.ReconcileRequestContext, operation dataoperation.OperationInterface, targetNames []types.NamespacedName) error {
	var firstErr error
	for _, targetName := range targetNames {
		if err := releaseLockOnDataset(ctx, operation, targetName); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReleaseTargetDataset releases the OperationRef field which marks the data operation being
// performed on all Datasets occupied by the operation (both endpoints for a Dataset-to-Dataset
// DataMigrate).
func ReleaseTargetDataset(ctx cruntime.ReconcileRequestContext, operation dataoperation.OperationInterface) error {
	targetNames, err := resolveLockTargetDatasetNames(operation)
	if err != nil {
		ctx.Log.Error(err, "can't resolve datasets to release")
		return err
	}

	if err = releaseLocksOnDatasets(ctx, operation, targetNames); err != nil {
		ctx.Log.Error(err, "can't release lock on target dataset(s)")
	}
	return err
}
