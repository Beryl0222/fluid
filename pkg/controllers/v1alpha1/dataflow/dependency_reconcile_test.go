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

package dataflow

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	originalUID types.UID = "uid-original"
	recreatedID types.UID = "uid-recreated"
)

func waitingDataLoad(name, namespace string, runAfter *datav1alpha1.OperationRef, waiting datav1alpha1.WaitingStatus) *datav1alpha1.DataLoad {
	return &datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       datav1alpha1.DataLoadSpec{RunAfter: runAfter},
		Status:     datav1alpha1.OperationStatus{WaitingFor: waiting},
	}
}

func runAfterRef(kind, name, namespace string) *datav1alpha1.OperationRef {
	ref := &datav1alpha1.OperationRef{
		ObjectRef: datav1alpha1.ObjectRef{Kind: kind, Name: name},
	}
	if namespace != "" {
		ref.ObjectRef.Namespace = namespace
	}
	return ref
}

var _ = Describe("reconcileOperationDataFlow dependency identity and terminal state", func() {

	var (
		s         *runtime.Scheme
		namespace = "default"
	)

	BeforeEach(func() {
		s = runtime.NewScheme()
		_ = datav1alpha1.AddToScheme(s)
	})

	Context("when the preceding operation fails", func() {
		It("fails the successor stably and persists the diagnostic terminal observation", func() {
			preceding := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: originalUID},
				Status: datav1alpha1.OperationStatus{
					Phase: common.PhaseFailed,
					Conditions: []datav1alpha1.Condition{
						{Type: common.Failed, Message: "load job failed"},
					},
				},
			}
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", ""),
				datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)})

			recorder := record.NewFakeRecorder(32)
			ctx := reconcileRequestContext{
				Context:        context.TODO(),
				NamespacedName: types.NamespacedName{Name: "waiting", Namespace: namespace},
				Client:         fake.NewFakeClientWithScheme(s, preceding, waiting),
				Log:            logf.Log.WithName("test"),
				Recorder:       recorder,
			}

			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeFalse())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(updated.Status.WaitingFor.OperationComplete).NotTo(BeNil())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
			Expect(updated.Status.WaitingFor.PrecedingOperation).NotTo(BeNil())
			Expect(updated.Status.WaitingFor.PrecedingOperation.UID).To(Equal(originalUID))
			Expect(updated.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseFailed))
			Expect(updated.Status.WaitingFor.PrecedingOperation.Reason).To(Equal(common.DataOperationDependencyFailed))
			Expect(updated.Status.WaitingFor.PrecedingOperation.Message).To(ContainSubstring("load job failed"))

			Eventually(recorder.Events).Should(Receive(ContainSubstring(common.DataOperationDependencyFailed)))
		})
	})

	Context("when the preceding operation is deleted after a confirmed complete", func() {
		It("keeps the successor unblocked and does not push it back to waiting", func() {
			cached := &datav1alpha1.PrecedingOperationStatus{
				ObjectRef:     datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: namespace},
				UID:           originalUID,
				ObservedPhase: common.PhaseComplete,
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{MatchExpressions: []v1.NodeSelectorRequirement{
								{Key: common.K8sNodeNameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"node01"}},
							}},
						},
					},
				},
			}
			// The preceding object is absent (e.g. cleaned up by TTL); the cache survives a restart.
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", ""),
				datav1alpha1.WaitingStatus{
					OperationComplete:  ptr.To(false),
					PrecedingOperation: cached,
				})

			ctx := makeTestCtx(s, "waiting", namespace, waiting)
			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeFalse())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
			Expect(updated.Status.WaitingFor.PrecedingOperation).NotTo(BeNil())
			Expect(updated.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseComplete))
			Expect(updated.Status.WaitingFor.PrecedingOperation.NodeAffinity).NotTo(BeNil())
		})
	})

	Context("when a same-named object is recreated with a different UID", func() {
		It("refuses to treat the recreated object as the dependency and records an identity conflict", func() {
			cached := &datav1alpha1.PrecedingOperationStatus{
				ObjectRef:     datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: namespace},
				UID:           originalUID,
				ObservedPhase: common.PhaseComplete,
			}
			recreated := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: recreatedID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
			}
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", ""),
				datav1alpha1.WaitingStatus{
					OperationComplete:  ptr.To(true),
					PrecedingOperation: cached,
				})

			recorder := record.NewFakeRecorder(32)
			ctx := reconcileRequestContext{
				Context:        context.TODO(),
				NamespacedName: types.NamespacedName{Name: "waiting", Namespace: namespace},
				Client:         fake.NewFakeClientWithScheme(s, recreated, waiting),
				Log:            logf.Log.WithName("test"),
				Recorder:       recorder,
			}

			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeFalse())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
			Expect(updated.Status.WaitingFor.PrecedingOperation.Reason).To(Equal(common.DataOperationIdentityConflict))
			Expect(updated.Status.WaitingFor.PrecedingOperation.UID).To(Equal(originalUID))
			Expect(updated.Status.WaitingFor.PrecedingOperation.Message).To(ContainSubstring(string(originalUID)))
			Expect(updated.Status.WaitingFor.PrecedingOperation.Message).To(ContainSubstring(string(recreatedID)))

			Eventually(recorder.Events).Should(Receive(ContainSubstring(common.DataOperationIdentityConflict)))
		})
	})

	Context("when the dependency is first observed in a non-terminal phase", func() {
		It("pins the dependency identity while keeping the successor waiting", func() {
			preceding := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: originalUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
			}
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", ""),
				datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)})

			ctx := makeTestCtx(s, "waiting", namespace, preceding, waiting)
			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeTrue())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(updated.Status.WaitingFor.PrecedingOperation).NotTo(BeNil())
			Expect(updated.Status.WaitingFor.PrecedingOperation.UID).To(Equal(originalUID))
			Expect(updated.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(BeEmpty())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeTrue())
		})
	})

	Context("when the API server is temporarily unreadable", func() {
		It("only retries and does not mutate the persisted observation", func() {
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", ""),
				datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)})

			fakeBase := fakeclient.NewClientBuilder().WithScheme(s).WithRuntimeObjects(waiting).
				WithStatusSubresource(waiting).Build()
			apiErr := errors.New("api server unavailable")
			c := interceptor.NewClient(fakeBase, interceptor.Funcs{
				Get: func(ctx context.Context, cc client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if key.Name == "preceding" {
						return apiErr
					}
					return cc.Get(ctx, key, obj, opts...)
				},
			})

			rctx := reconcileRequestContext{
				Context:        context.TODO(),
				NamespacedName: types.NamespacedName{Name: "waiting", Namespace: namespace},
				Client:         c,
				Log:            logf.Log.WithName("test"),
				Recorder:       record.NewFakeRecorder(32),
			}

			needRequeue, err := reconcileDataLoad(rctx)
			Expect(err).To(HaveOccurred())
			Expect(needRequeue).To(BeTrue())

			updated := &datav1alpha1.DataLoad{}
			Expect(rctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(updated.Status.WaitingFor.PrecedingOperation).To(BeNil())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeTrue())
		})
	})

	Context("when the operation does not use RunAfter", func() {
		It("leaves the object untouched and does not requeue", func() {
			noDependency := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: namespace},
				Status: datav1alpha1.OperationStatus{
					WaitingFor: datav1alpha1.WaitingStatus{},
				},
			}
			ctx := makeTestCtx(s, "standalone", namespace, noDependency)
			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeFalse())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "standalone", Namespace: namespace}, updated)).To(Succeed())
			Expect(updated.Status.WaitingFor.PrecedingOperation).To(BeNil())
			Expect(updated.Status.WaitingFor.OperationComplete).To(BeNil())
		})
	})

	Context("when the dependency lives in another namespace", func() {
		It("resolves the cross-namespace reference and unblocks the successor on complete", func() {
			preceding := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "other-ns", UID: originalUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
			}
			waiting := waitingDataLoad("waiting", namespace,
				runAfterRef("DataLoad", "preceding", "other-ns"),
				datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)})

			ctx := makeTestCtx(s, "waiting", namespace, preceding, waiting)
			needRequeue, err := reconcileDataLoad(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(needRequeue).To(BeFalse())

			updated := &datav1alpha1.DataLoad{}
			Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
			Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
			Expect(updated.Status.WaitingFor.PrecedingOperation.Namespace).To(Equal("other-ns"))
			Expect(updated.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseComplete))
		})
	})

	Context("across all four data operation kinds", func() {
		It("fails DataBackup, DataMigrate and DataProcess with the same stable rules", func() {
			failedLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "load-1", Namespace: namespace, UID: originalUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseFailed},
			}

			waitingBackup := &datav1alpha1.DataBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: namespace},
				Spec: datav1alpha1.DataBackupSpec{
					RunAfter: runAfterRef("DataLoad", "load-1", ""),
				},
				Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
			}
			waitingMigrate := &datav1alpha1.DataMigrate{
				ObjectMeta: metav1.ObjectMeta{Name: "migrate-1", Namespace: namespace},
				Spec: datav1alpha1.DataMigrateSpec{
					RunAfter: runAfterRef("DataLoad", "load-1", ""),
				},
				Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
			}
			waitingProcess := &datav1alpha1.DataProcess{
				ObjectMeta: metav1.ObjectMeta{Name: "process-1", Namespace: namespace},
				Spec: datav1alpha1.DataProcessSpec{
					RunAfter: runAfterRef("DataLoad", "load-1", ""),
				},
				Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
			}

			sharedClient := fake.NewFakeClientWithScheme(s, failedLoad, waitingBackup, waitingMigrate, waitingProcess)
			newCtx := func(name string) reconcileRequestContext {
				return reconcileRequestContext{
					Context:        context.TODO(),
					NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
					Client:         sharedClient,
					Log:            logf.Log.WithName("test"),
					Recorder:       record.NewFakeRecorder(32),
				}
			}

			requeueBackup, err := reconcileDataBackup(newCtx("backup-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(requeueBackup).To(BeFalse())
			requeueMigrate, err := reconcileDataMigrate(newCtx("migrate-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(requeueMigrate).To(BeFalse())
			requeueProcess, err := reconcileDataProcess(newCtx("process-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(requeueProcess).To(BeFalse())

			backup := &datav1alpha1.DataBackup{}
			Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "backup-1", Namespace: namespace}, backup)).To(Succeed())
			Expect(backup.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseFailed))
			Expect(*backup.Status.WaitingFor.OperationComplete).To(BeFalse())

			migrate := &datav1alpha1.DataMigrate{}
			Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "migrate-1", Namespace: namespace}, migrate)).To(Succeed())
			Expect(migrate.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseFailed))

			process := &datav1alpha1.DataProcess{}
			Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "process-1", Namespace: namespace}, process)).To(Succeed())
			Expect(process.Status.WaitingFor.PrecedingOperation.ObservedPhase).To(Equal(common.PhaseFailed))
		})
	})
})
