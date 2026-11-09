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

package base_test

import (
	"context"
	stderrors "errors"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	enginemock "github.com/fluid-cloudnative/fluid/pkg/ddc/base/mock"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	dualOpName      = "migrate-op"
	sourceDataset   = "source-dataset"
	destDataset     = "dest-dataset"
	parallelOpName  = "another-migrate"
	testLockNS      = "default"
	dataMigrateKind = "DataMigrate"
)

// dualLockMockOperation implements both dataoperation.OperationInterface and
// dataoperation.MultiDatasetLockOperation for testing dual-endpoint occupancy.
type dualLockMockOperation struct {
	lockTargets []types.NamespacedName
}

var (
	_ dataoperation.OperationInterface        = &dualLockMockOperation{}
	_ dataoperation.MultiDatasetLockOperation = &dualLockMockOperation{}
)

func (m *dualLockMockOperation) HasPrecedingOperation() bool { return false }

func (m *dualLockMockOperation) GetOperationObject() ctrlclient.Object {
	return &datav1alpha1.DataMigrate{
		ObjectMeta: metav1.ObjectMeta{Name: dualOpName, Namespace: testLockNS},
	}
}

func (m *dualLockMockOperation) GetPossibleTargetDatasetNamespacedNames() []types.NamespacedName {
	return m.lockTargets
}

func (m *dualLockMockOperation) GetTargetDataset() (*datav1alpha1.Dataset, error) {
	if len(m.lockTargets) == 0 {
		return nil, stderrors.New("no target dataset")
	}
	return &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: m.lockTargets[0].Name, Namespace: m.lockTargets[0].Namespace},
	}, nil
}

func (m *dualLockMockOperation) GetReleaseNameSpacedName() types.NamespacedName {
	return types.NamespacedName{Name: "release", Namespace: testLockNS}
}

func (m *dualLockMockOperation) GetChartsDirectory() string { return "/charts/datamigrate" }

func (m *dualLockMockOperation) GetOperationType() dataoperation.OperationType {
	return dataoperation.DataMigrateType
}

func (m *dualLockMockOperation) UpdateOperationApiStatus(opStatus *datav1alpha1.OperationStatus) error {
	return nil
}

func (m *dualLockMockOperation) Validate(ctx cruntime.ReconcileRequestContext) ([]datav1alpha1.Condition, error) {
	return nil, nil
}

func (m *dualLockMockOperation) UpdateStatusInfoForCompleted(infos map[string]string) error {
	return nil
}

func (m *dualLockMockOperation) SetTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset) {
	dataset.Status.Phase = datav1alpha1.DataMigrating
}

func (m *dualLockMockOperation) RemoveTargetDatasetStatusInProgress(dataset *datav1alpha1.Dataset) {
	dataset.Status.Phase = datav1alpha1.BoundDatasetPhase
}

func (m *dualLockMockOperation) GetStatusHandler() dataoperation.StatusHandler { return nil }

func (m *dualLockMockOperation) GetTTL() (ttl *int32, err error) { return nil, nil }

func (m *dualLockMockOperation) GetParallelTaskNumber() int32 { return 1 }

func (m *dualLockMockOperation) GetLockTargetDatasetNamespacedNames() []types.NamespacedName {
	return m.lockTargets
}

// statusWriterClient allows replacing the status writer with an instrumented one.
type statusWriterClient struct {
	ctrlclient.Client
	statusWriter ctrlclient.StatusWriter
}

func (c *statusWriterClient) Status() ctrlclient.StatusWriter { return c.statusWriter }

type instrumentedStatusWriter struct {
	ctrlclient.StatusWriter
	failUpdateFor     *types.NamespacedName
	conflictOnceFor   *types.NamespacedName
	conflictTriggered bool
}

func (w *instrumentedStatusWriter) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.SubResourceUpdateOption) error {
	nn := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	if w.failUpdateFor != nil && *w.failUpdateFor == nn {
		return stderrors.New("injected persistent status update failure")
	}
	if w.conflictOnceFor != nil && *w.conflictOnceFor == nn && !w.conflictTriggered {
		w.conflictTriggered = true
		return apierrors.NewConflict(schema.GroupResource{Resource: "datasets"}, obj.GetName(), stderrors.New("injected resourceVersion conflict"))
	}
	return w.StatusWriter.Update(ctx, obj, opts...)
}

func newDualLockScheme() *apimachineryRuntime.Scheme {
	s := apimachineryRuntime.NewScheme()
	Expect(datav1alpha1.AddToScheme(s)).To(Succeed())
	return s
}

func newDatasetWithStatus(name string, operationRef map[string]string, phase datav1alpha1.DatasetPhase) *datav1alpha1.Dataset {
	return &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testLockNS},
		Status: datav1alpha1.DatasetStatus{
			Phase:        phase,
			OperationRef: operationRef,
		},
	}
}

func getDatasetStatus(c ctrlclient.Client, name string) datav1alpha1.DatasetStatus {
	var dataset datav1alpha1.Dataset
	Expect(c.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: testLockNS}, &dataset)).To(Succeed())
	return dataset.Status
}

var _ = Describe("Dual-endpoint dataset occupancy", func() {
	var (
		gomockCtrl *gomock.Controller
		engine     *enginemock.MockImplement
		fakeCtx    cruntime.ReconcileRequestContext
	)

	BeforeEach(func() {
		gomockCtrl = gomock.NewController(GinkgoT())
		engine = enginemock.NewMockImplement(gomockCtrl)
		fakeCtx = cruntime.ReconcileRequestContext{
			Log:      fake.NullLogger(),
			Recorder: record.NewFakeRecorder(20),
		}
	})

	AfterEach(func() {
		gomockCtrl.Finish()
	})

	Describe("SetDataOperationInTargetDataset", func() {
		It("occupies both source and destination datasets with the same operation identity", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset, nil, datav1alpha1.BoundDatasetPhase)
			dest := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, source, dest)
			fakeCtx.Client = client
			fakeCtx.Dataset = dest
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(Succeed())

			sourceStatus := getDatasetStatus(client, sourceDataset)
			destStatus := getDatasetStatus(client, destDataset)
			Expect(sourceStatus.OperationRef[dataMigrateKind]).To(Equal(dualOpName))
			Expect(destStatus.OperationRef[dataMigrateKind]).To(Equal(dualOpName))
			Expect(sourceStatus.Phase).To(Equal(datav1alpha1.DataMigrating))
			Expect(destStatus.Phase).To(Equal(datav1alpha1.DataMigrating))
		})

		It("occupies a dataset only once when from and to point to the same dataset", func() {
			s := newDualLockScheme()
			only := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, only)
			fakeCtx.Client = client
			fakeCtx.Dataset = only
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(Succeed())

			status := getDatasetStatus(client, destDataset)
			Expect(status.OperationRef[dataMigrateKind]).To(Equal(dualOpName))
			Expect(status.Phase).To(Equal(datav1alpha1.DataMigrating))
		})

		It("falls back to the single target dataset for operations without multi-dataset lock", func() {
			s := newDualLockScheme()
			fakeDataset := &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{Name: mockDatasetName, Namespace: mockNamespace},
			}
			client := fake.NewFakeClientWithScheme(s, fakeDataset)
			fakeCtx.Client = client
			fakeCtx.Dataset = fakeDataset
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := newMockOperation()

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(Succeed())

			status := getDatasetStatus(client, mockDatasetName)
			Expect(status.OperationRef[string(dataoperation.DataLoadType)]).To(Equal(mockDatasetName))
		})

		It("rolls back the first endpoint and returns error when the second endpoint write fails", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset, nil, datav1alpha1.BoundDatasetPhase)
			dest := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			rawClient := fake.NewFakeClientWithScheme(s, source, dest)
			failNN := types.NamespacedName{Name: destDataset, Namespace: testLockNS}
			client := &statusWriterClient{
				Client:       rawClient,
				statusWriter: &instrumentedStatusWriter{StatusWriter: rawClient.Status(), failUpdateFor: &failNN},
			}
			fakeCtx.Client = client
			fakeCtx.Dataset = dest
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			err := base.SetDataOperationInTargetDataset(fakeCtx, op, engine)
			Expect(err).To(HaveOccurred())

			// no half-lock: the occupancy acquired on the first endpoint is rolled back
			sourceStatus := getDatasetStatus(client, sourceDataset)
			destStatus := getDatasetStatus(client, destDataset)
			Expect(sourceStatus.OperationRef).To(BeEmpty())
			Expect(destStatus.OperationRef).To(BeEmpty())
			Expect(sourceStatus.Phase).To(Equal(datav1alpha1.BoundDatasetPhase))
		})

		It("converges from persisted occupancy when the controller restarted between the two writes", func() {
			s := newDualLockScheme()
			// source endpoint had already been occupied and persisted before the restart
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			dest := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, source, dest)
			fakeCtx.Client = client
			fakeCtx.Dataset = dest
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(Succeed())

			sourceStatus := getDatasetStatus(client, sourceDataset)
			destStatus := getDatasetStatus(client, destDataset)
			// re-acquiring the first endpoint must not duplicate the reference
			Expect(sourceStatus.OperationRef[dataMigrateKind]).To(Equal(dualOpName))
			Expect(destStatus.OperationRef[dataMigrateKind]).To(Equal(dualOpName))
		})

		It("preserves references of parallel operations when acquiring occupancy", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: parallelOpName}, datav1alpha1.DataMigrating)
			dest := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, source, dest)
			fakeCtx.Client = client
			fakeCtx.Dataset = dest
			engine.EXPECT().CheckRuntimeReady().Return(true)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(Succeed())

			sourceStatus := getDatasetStatus(client, sourceDataset)
			Expect(sourceStatus.OperationRef[dataMigrateKind]).To(ContainSubstring(parallelOpName))
			Expect(sourceStatus.OperationRef[dataMigrateKind]).To(ContainSubstring(dualOpName))
		})

		It("returns error without writing any lock when the bounded runtime is not ready", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset, nil, datav1alpha1.BoundDatasetPhase)
			dest := newDatasetWithStatus(destDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, source, dest)
			fakeCtx.Client = client
			fakeCtx.Dataset = dest
			engine.EXPECT().CheckRuntimeReady().Return(false)

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.SetDataOperationInTargetDataset(fakeCtx, op, engine)).To(HaveOccurred())

			Expect(getDatasetStatus(client, sourceDataset).OperationRef).To(BeEmpty())
			Expect(getDatasetStatus(client, destDataset).OperationRef).To(BeEmpty())
		})
	})

	Describe("ReleaseTargetDataset", func() {
		It("releases both endpoints symmetrically", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			dest := newDatasetWithStatus(destDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			client := fake.NewFakeClientWithScheme(s, source, dest)
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.ReleaseTargetDataset(fakeCtx, op)).To(Succeed())

			Expect(getDatasetStatus(client, sourceDataset).OperationRef).To(BeEmpty())
			Expect(getDatasetStatus(client, destDataset).OperationRef).To(BeEmpty())
			Expect(getDatasetStatus(client, sourceDataset).Phase).To(Equal(datav1alpha1.BoundDatasetPhase))
			Expect(getDatasetStatus(client, destDataset).Phase).To(Equal(datav1alpha1.BoundDatasetPhase))
		})

		It("only removes its own reference and keeps references owned by other operations", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: parallelOpName + "," + dualOpName},
				datav1alpha1.DataMigrating)
			client := fake.NewFakeClientWithScheme(s, source)
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
			}}

			Expect(base.ReleaseTargetDataset(fakeCtx, op)).To(Succeed())

			status := getDatasetStatus(client, sourceDataset)
			Expect(status.OperationRef[dataMigrateKind]).To(Equal(parallelOpName))
			// the dataset is still occupied by another operation, phase must not be reset
			Expect(status.Phase).To(Equal(datav1alpha1.DataMigrating))
		})

		It("tolerates a deleted endpoint dataset while releasing the other endpoint", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			// destination dataset has been deleted and does not exist in the client
			client := fake.NewFakeClientWithScheme(s, source)
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			Expect(base.ReleaseTargetDataset(fakeCtx, op)).To(Succeed())
			Expect(getDatasetStatus(client, sourceDataset).OperationRef).To(BeEmpty())
		})

		It("retries and succeeds when releasing hits a resourceVersion conflict", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: parallelOpName + "," + dualOpName},
				datav1alpha1.DataMigrating)
			rawClient := fake.NewFakeClientWithScheme(s, source)
			conflictNN := types.NamespacedName{Name: sourceDataset, Namespace: testLockNS}
			client := &statusWriterClient{
				Client:       rawClient,
				statusWriter: &instrumentedStatusWriter{StatusWriter: rawClient.Status(), conflictOnceFor: &conflictNN},
			}
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
			}}

			Expect(base.ReleaseTargetDataset(fakeCtx, op)).To(Succeed())

			status := getDatasetStatus(client, sourceDataset)
			Expect(status.OperationRef[dataMigrateKind]).To(Equal(parallelOpName))
		})

		It("is idempotent when the occupancy has already been released", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset, nil, datav1alpha1.BoundDatasetPhase)
			client := fake.NewFakeClientWithScheme(s, source)
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
			}}

			Expect(base.ReleaseTargetDataset(fakeCtx, op)).To(Succeed())
			Expect(getDatasetStatus(client, sourceDataset).OperationRef).To(BeEmpty())
		})

		It("still releases the remaining endpoint when one endpoint release fails persistently", func() {
			s := newDualLockScheme()
			source := newDatasetWithStatus(sourceDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			dest := newDatasetWithStatus(destDataset,
				map[string]string{dataMigrateKind: dualOpName}, datav1alpha1.DataMigrating)
			rawClient := fake.NewFakeClientWithScheme(s, source, dest)
			failNN := types.NamespacedName{Name: sourceDataset, Namespace: testLockNS}
			client := &statusWriterClient{
				Client:       rawClient,
				statusWriter: &instrumentedStatusWriter{StatusWriter: rawClient.Status(), failUpdateFor: &failNN},
			}
			fakeCtx.Client = client

			op := &dualLockMockOperation{lockTargets: []types.NamespacedName{
				{Name: sourceDataset, Namespace: testLockNS},
				{Name: destDataset, Namespace: testLockNS},
			}}

			err := base.ReleaseTargetDataset(fakeCtx, op)
			Expect(err).To(HaveOccurred())
			// the healthy endpoint must not be blocked by the failing endpoint
			Expect(getDatasetStatus(client, destDataset).OperationRef).To(BeEmpty())
			// the failing endpoint keeps its occupancy and will be converged on requeue
			Expect(getDatasetStatus(client, sourceDataset).OperationRef[dataMigrateKind]).To(Equal(dualOpName))
		})
	})
})
