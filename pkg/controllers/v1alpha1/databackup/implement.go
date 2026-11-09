/*
Copyright 2022 The Fluid Authors.

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
	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/record"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	"github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

type dataBackupOperation struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder

	// object for reconciler
	dataBackup *datav1alpha1.DataBackup
}

var _ dataoperation.OperationInterface = &dataBackupOperation{}

func (r *dataBackupOperation) GetOperationObject() client.Object {
	return r.dataBackup
}

func (r *dataBackupOperation) GetChartsDirectory() string {
	return utils.GetChartsDirectory() + "/" + cdatabackup.DatabackupChart
}

func (r *dataBackupOperation) HasPrecedingOperation() bool {
	return r.dataBackup.Spec.RunAfter != nil
}

func (r *dataBackupOperation) UpdateStatusInfoForCompleted(infos map[string]string) error {
	dataBackup := r.dataBackup

	infos[cdatabackup.BackupLocationPath] = dataBackup.Spec.BackupPath

	if !strings.HasPrefix(dataBackup.Spec.BackupPath, common.PathScheme.String()) {
		// pvc:// and other non-local targets are backed by networked storage: they do not
		// depend on any specific node, keep the node location node-independent.
		infos[cdatabackup.BackupLocationNodeName] = "NA"
		return nil
	}

	// local:// backups are written to the node the backup pod was scheduled to. Prefer the
	// node name persisted together with the observed terminal state so that pod garbage
	// collection after completion can no longer block the finalization.
	if nodeName, ok := infos[cdatabackup.BackupPodNodeName]; ok {
		infos[cdatabackup.BackupLocationNodeName] = nodeName
		return nil
	}

	// Fallback for statuses produced before the terminal evidence was introduced: read the
	// node from the live pod while it still exists.
	podName := utils.GetDataBackupPodName(dataBackup.GetName())
	backupPod, err := kubeclient.GetPodByName(r.Client, podName, dataBackup.GetNamespace())
	if err != nil {
		r.Log.Error(err, "Failed to get backup pod", "pod", podName)
		return fmt.Errorf("failed to get backup pod %s: %w", podName, err)
	}
	if backupPod == nil {
		// No live pod and no persisted evidence: never guess the node, surface a diagnosable
		// error and let the reconcile retry/backoff instead of recording a wrong location.
		return fmt.Errorf("backup pod %s not found and no persisted node evidence in DataBackup status, cannot determine the backup location node", podName)
	}
	infos[cdatabackup.BackupLocationNodeName] = backupPod.Spec.NodeName

	return nil
}

func (r *dataBackupOperation) Validate(ctx runtime.ReconcileRequestContext) ([]datav1alpha1.Condition, error) {
	dataBackup := r.dataBackup

	// 0. check the supported backup path format
	if !strings.HasPrefix(dataBackup.Spec.BackupPath, common.PathScheme.String()) && !strings.HasPrefix(dataBackup.Spec.BackupPath, common.VolumeScheme.String()) {
		err := fmt.Errorf("don't support path in this form, path: %s", dataBackup.Spec.BackupPath)
		return []datav1alpha1.Condition{
			{
				Type:               common.Failed,
				Status:             v1.ConditionTrue,
				Reason:             "PathNotSupported",
				Message:            "Only support pvc and local path now",
				LastProbeTime:      metav1.NewTime(time.Now()),
				LastTransitionTime: metav1.NewTime(time.Now()),
			},
		}, err
	}
	return nil, nil
}

func (r *dataBackupOperation) SetTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset) {
}

func (r *dataBackupOperation) RemoveTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset) {
}

func (r *dataBackupOperation) GetOperationType() dataoperation.OperationType {
	return dataoperation.DataBackupType
}

func (r *dataBackupOperation) GetPossibleTargetDatasetNamespacedNames() []types.NamespacedName {
	return []types.NamespacedName{
		{Namespace: r.dataBackup.Namespace, Name: r.dataBackup.Name},
	}
}

func (r *dataBackupOperation) GetTargetDataset() (*datav1alpha1.Dataset, error) {
	return utils.GetDataset(r.Client, r.dataBackup.Spec.Dataset, r.dataBackup.Namespace)
}

func (r *dataBackupOperation) GetReleaseNameSpacedName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: r.dataBackup.GetNamespace(),
		Name:      utils.GetDataBackupReleaseName(r.dataBackup.GetName()),
	}
}

// UpdateOperationApiStatus update the DataBackup Status
func (r *dataBackupOperation) UpdateOperationApiStatus(opStatus *datav1alpha1.OperationStatus) error {
	var dataBackupCpy = r.dataBackup.DeepCopy()
	dataBackupCpy.Status = *opStatus.DeepCopy()
	err := r.Status().Update(context.Background(), dataBackupCpy)
	if err != nil {
		if apierrors.IsConflict(err) {
			// The cached DataBackup is stale. Returning the conflict triggers a requeue and the
			// next reconcile re-gets the latest object, then either re-observes the backup pod or
			// relies on the persisted terminal evidence, so no terminal result is lost.
			r.Log.Info("conflict updating DataBackup status, will requeue and retry with the latest object",
				"namespace", r.dataBackup.GetNamespace(), "name", r.dataBackup.GetName(),
				"resourceVersion", r.dataBackup.GetResourceVersion())
		}
		return err
	}
	return nil
}

func (r *dataBackupOperation) GetStatusHandler() dataoperation.StatusHandler {
	return &OnceHandler{dataBackup: r.dataBackup}
}

// GetTTL implements dataoperation.OperationInterface.
func (r *dataBackupOperation) GetTTL() (ttl *int32, err error) {
	ttl = r.dataBackup.Spec.TTLSecondsAfterFinished
	return
}

func (r *dataBackupOperation) GetParallelTaskNumber() int32 {
	return 1
}
