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

package dataoperation

//go:generate mockgen -source=interface.go -destination=mock/mock_operation.go -package=mock

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/runtime"
)

type OperationInterfaceBuilder interface {
	Build(object client.Object) (OperationInterface, error)
}

// OperationInterface the interface of data operation crd
type OperationInterface interface {
	// HasPrecedingOperation check if current data operation depends on another data operation
	HasPrecedingOperation() bool

	// GetOperationObject get the data operation object
	GetOperationObject() client.Object

	// GetPossibleTargetDatasetNamespacedNames returns all possible target dataset's namespace and name, this should only be used for cleaning up data operations.
	GetPossibleTargetDatasetNamespacedNames() []types.NamespacedName

	// GetTargetDataset get the target dataset of the data operation, implementor should return the newest target dataset.
	GetTargetDataset() (*datav1alpha1.Dataset, error)

	// GetReleaseNameSpacedName get the installed helm chart name
	GetReleaseNameSpacedName() types.NamespacedName

	// GetChartsDirectory get the helm charts directory of data operation
	GetChartsDirectory() string

	// GetOperationType get the data operation type
	GetOperationType() OperationType

	// UpdateOperationApiStatus update the data operation status, object is the data operation crd instance.
	UpdateOperationApiStatus(opStatus *datav1alpha1.OperationStatus) error

	// Validate check the data operation spec is valid or not, if not valid return error with conditions
	Validate(ctx runtime.ReconcileRequestContext) ([]datav1alpha1.Condition, error)

	// UpdateStatusInfoForCompleted update the status infos field for phase completed, the parameter infos is not nil
	UpdateStatusInfoForCompleted(infos map[string]string) error

	// SetTargetDatasetStatusInProgress set the dataset status for certain field when data operation executing.
	SetTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset)

	// RemoveTargetDatasetStatusInProgress remove the dataset status for certain field when data operation finished.
	RemoveTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset)

	GetStatusHandler() StatusHandler

	// GetTTL gets timeToLive
	GetTTL() (ttl *int32, err error)

	// GetParallelTaskNumber get the parallel tasks for data operations.
	GetParallelTaskNumber() int32
}

type StatusHandler interface {
	// GetOperationStatus get operation status according to helm chart status
	GetOperationStatus(ctx runtime.ReconcileRequestContext, opStatus *datav1alpha1.OperationStatus) (result *datav1alpha1.OperationStatus, err error)
}

// ScheduledOperation is optionally implemented by operations whose executions are organized
// into schedule driven rounds (cron policy). It is consulted during the Pending phase before
// the target dataset's operation occupation is taken, so that a workload pod is released only
// after the occupation of the current round is acquired and rounds never overlap.
type ScheduledOperation interface {
	// IsScheduled reports whether the operation is currently driven by a schedule.
	IsScheduled() bool

	// ReconcileScheduledRound converges the Pending phase for a scheduled operation. It must be
	// idempotent and derive every decision from Kubernetes objects so that it keeps converging
	// after controller restarts or intermediate workload states:
	//   - when a scheduled round is already open in opStatus but not closed yet, it ensures the
	//     dataset occupation is held and returns roundOpen=true to re-enter the Executing phase;
	//   - when a new scheduled time is due, it invokes acquireOccupation exactly once, records the
	//     scheduled time and the round identity, advances the phase to Executing and returns
	//     roundOpen=true;
	//   - when no scheduled time is due, it releases any stale occupation, stays in Pending and
	//     returns roundOpen=false together with the duration after which to reconcile again.
	// The same scheduled time must never be started twice; releasing the occupation is delegated to
	// releaseOccupation so that a stale occupation can be healed without leaving an OperationRef behind.
	ReconcileScheduledRound(
		ctx runtime.ReconcileRequestContext,
		opStatus *datav1alpha1.OperationStatus,
		acquireOccupation func() error,
		releaseOccupation func() error,
	) (roundOpen bool, requeueAfter time.Duration, err error)
}
