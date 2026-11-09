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
	corev1 "k8s.io/api/core/v1"
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

const transientReadError = "transient api error"

func waitingDataLoad(name string, ref datav1alpha1.ObjectRef) *datav1alpha1.DataLoad {
	return &datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ref.Namespace},
		Spec: datav1alpha1.DataLoadSpec{
			RunAfter: &datav1alpha1.OperationRef{ObjectRef: ref},
		},
		Status: datav1alpha1.OperationStatus{
			WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)},
		},
	}
}

func dependencyRef(kind, name, namespace string) datav1alpha1.ObjectRef {
	return datav1alpha1.ObjectRef{Kind: kind, Name: name, Namespace: namespace}
}

var _ = Describe("RunAfter dependency identity and terminal state", func() {

	const namespace = "default"

	It("fails the successor with diagnostics when the preceding operation is Failed", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		preceding := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: types.UID("uid-a")},
			Status: datav1alpha1.OperationStatus{
				Phase: common.PhaseFailed,
				Conditions: []datav1alpha1.Condition{
					{Type: common.Failed, Status: corev1.ConditionTrue, Reason: "JobFailed", Message: "job blew up"},
				},
			},
		}
		waiting := waitingDataLoad("waiting", dependencyRef("DataLoad", "preceding", namespace))

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
		Expect(updated.Status.Phase).To(Equal(common.PhaseFailed))
		Expect(updated.Status.Conditions).To(HaveLen(1))
		Expect(updated.Status.Conditions[0].Reason).To(Equal(common.DataOperationDependencyFailed))
		Expect(updated.Status.Conditions[0].Message).To(ContainSubstring("job blew up"))
		Expect(updated.Status.WaitingFor.OperationComplete).NotTo(BeNil())
		Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
		Expect(updated.Status.WaitingFor.Dependencies).To(HaveLen(1))
		Expect(updated.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-a")))
		Expect(updated.Status.WaitingFor.Dependencies[0].Phase).To(Equal(common.PhaseFailed))

		Eventually(recorder.Events).Should(Receive(ContainSubstring(common.DataOperationDependencyFailed)))
	})

	It("keeps the successor unblocked when the completed preceding object is TTL-deleted", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		preceding := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: types.UID("uid-a")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		waiting := waitingDataLoad("waiting", dependencyRef("DataLoad", "preceding", namespace))
		ctx := makeTestCtx(s, "waiting", namespace, preceding, waiting)

		// First reconcile: observe Complete, persist the terminal record and unblock.
		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		updated := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
		Expect(*updated.Status.WaitingFor.OperationComplete).To(BeFalse())
		Expect(updated.Status.WaitingFor.Dependencies).To(HaveLen(1))
		Expect(updated.Status.WaitingFor.Dependencies[0].Phase).To(Equal(common.PhaseComplete))

		// TTL cleanup removes the preceding object; a later reconcile (e.g. after restart)
		// must trust the persisted terminal record instead of waiting again.
		Expect(ctx.Client.Delete(context.TODO(), preceding)).To(Succeed())

		needRequeue, err = reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		afterDelete := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, afterDelete)).To(Succeed())
		Expect(*afterDelete.Status.WaitingFor.OperationComplete).To(BeFalse())
		Expect(afterDelete.Status.Phase).NotTo(Equal(common.PhaseFailed))
		Expect(afterDelete.Status.WaitingFor.Dependencies).To(HaveLen(1))
		Expect(afterDelete.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-a")))
	})

	It("rejects a same-name recreation with a different UID as an identity conflict", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		original := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: types.UID("uid-a")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
		}
		waiting := waitingDataLoad("waiting", dependencyRef("DataLoad", "preceding", namespace))
		ctx := makeTestCtx(s, "waiting", namespace, original, waiting)

		// First observation pins the original UID while the operation is still running.
		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeTrue())

		pinned := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, pinned)).To(Succeed())
		Expect(pinned.Status.WaitingFor.Dependencies).To(HaveLen(1))
		Expect(pinned.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-a")))

		// The original object disappears and a same-name, different-UID object completes.
		Expect(ctx.Client.Delete(context.TODO(), original)).To(Succeed())
		impostor := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: namespace, UID: types.UID("uid-b")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		Expect(ctx.Client.Create(context.TODO(), impostor)).To(Succeed())

		recorder := record.NewFakeRecorder(32)
		ctx.Recorder = recorder
		needRequeue, err = reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		conflicted := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, conflicted)).To(Succeed())
		Expect(conflicted.Status.Phase).To(Equal(common.PhaseFailed))
		Expect(conflicted.Status.Conditions[0].Reason).To(Equal(common.DataOperationDependencyConflict))
		Expect(conflicted.Status.Conditions[0].Message).To(ContainSubstring("uid-a"))
		Expect(conflicted.Status.Conditions[0].Message).To(ContainSubstring("uid-b"))
		Expect(conflicted.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-a")))
		Eventually(recorder.Events).Should(Receive(ContainSubstring(common.DataOperationDependencyConflict)))
	})

	It("distinguishes confirmed failure, unknown state and conflict after a restart", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		// Successor already carries a persisted Failed terminal record while the object is gone.
		waiting := waitingDataLoad("waiting-failed", dependencyRef("DataLoad", "preceding", namespace))
		waiting.Status.Phase = common.PhaseFailed
		waiting.Status.Conditions = []datav1alpha1.Condition{
			{Type: common.Failed, Status: corev1.ConditionTrue, Reason: common.DataOperationDependencyFailed, Message: "stored"},
		}
		waiting.Status.WaitingFor.OperationComplete = ptr.To(false)
		waiting.Status.WaitingFor.Dependencies = []datav1alpha1.DependencyStatus{
			{
				Ref:     dependencyRef("DataLoad", "preceding", namespace),
				UID:     types.UID("uid-a"),
				Phase:   common.PhaseFailed,
				Reason:  "JobFailed",
				Message: "stored",
			},
		}

		ctx := makeTestCtx(s, "waiting-failed", namespace, waiting)
		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		stable := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting-failed", Namespace: namespace}, stable)).To(Succeed())
		Expect(stable.Status.Phase).To(Equal(common.PhaseFailed))
		Expect(stable.Status.Conditions[0].Message).To(Equal("stored"))

		// Unknown state: identity pinned before any terminal state, object now missing → keep waiting.
		unknown := waitingDataLoad("waiting-unknown", dependencyRef("DataLoad", "never-terminal", namespace))
		unknown.Status.WaitingFor.Dependencies = []datav1alpha1.DependencyStatus{
			{Ref: dependencyRef("DataLoad", "never-terminal", namespace), UID: types.UID("uid-c"), Phase: common.PhasePending},
		}
		ctxUnknown := makeTestCtx(s, "waiting-unknown", namespace, unknown)
		needRequeue, err = reconcileDataLoad(ctxUnknown)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeTrue())
	})

	It("only retries when the dependency API read temporarily fails", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		waiting := waitingDataLoad("waiting", dependencyRef("DataLoad", "preceding", namespace))
		fakeBase := fakeclient.NewClientBuilder().WithScheme(s).
			WithRuntimeObjects(waiting).
			WithStatusSubresource(waiting).
			Build()

		injectErr := errors.New(transientReadError)
		retryClient := interceptor.NewClient(fakeBase, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "preceding" {
					return injectErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		rctx := reconcileRequestContext{
			Context:        context.TODO(),
			NamespacedName: types.NamespacedName{Name: "waiting", Namespace: namespace},
			Client:         retryClient,
			Log:            logf.Log.WithName("test"),
			Recorder:       record.NewFakeRecorder(32),
		}

		needRequeue, err := reconcileDataLoad(rctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to get preceding operation status"))
		Expect(err.Error()).To(ContainSubstring(transientReadError))
		Expect(needRequeue).To(BeTrue())

		// Persisted state must remain untouched while the API is unreadable.
		updated := &datav1alpha1.DataLoad{}
		Expect(rctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
		Expect(*updated.Status.WaitingFor.OperationComplete).To(BeTrue())
		Expect(updated.Status.WaitingFor.Dependencies).To(BeEmpty())
	})

	It("applies the same judgment to cross-namespace dependencies", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		preceding := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "other-ns", UID: types.UID("uid-x")},
			Status: datav1alpha1.OperationStatus{
				Phase:      common.PhaseFailed,
				Conditions: []datav1alpha1.Condition{{Type: common.Failed, Status: corev1.ConditionTrue, Message: "cross-ns boom"}},
			},
		}
		waiting := waitingDataLoad("waiting", dependencyRef("DataLoad", "preceding", "other-ns"))
		waiting.Namespace = namespace
		ctx := makeTestCtx(s, "waiting", namespace, preceding, waiting)

		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		updated := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(common.PhaseFailed))
		Expect(updated.Status.WaitingFor.Dependencies[0].Ref.Namespace).To(Equal("other-ns"))
		Expect(updated.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-x")))
	})

	It("fails the successor when the affinity dependOn object fails even if runAfter completes", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		runAfterOp := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "run-after-op", Namespace: namespace, UID: types.UID("uid-a")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		dependOnOp := &datav1alpha1.DataProcess{
			ObjectMeta: metav1.ObjectMeta{Name: "affinity-op", Namespace: namespace, UID: types.UID("uid-b")},
			Status: datav1alpha1.OperationStatus{
				Phase:      common.PhaseFailed,
				Conditions: []datav1alpha1.Condition{{Type: common.Failed, Status: corev1.ConditionTrue, Message: "dependon boom"}},
			},
		}
		waiting := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "waiting", Namespace: namespace},
			Spec: datav1alpha1.DataLoadSpec{
				RunAfter: &datav1alpha1.OperationRef{
					ObjectRef: dependencyRef("DataLoad", "run-after-op", namespace),
					AffinityStrategy: datav1alpha1.AffinityStrategy{
						Policy:   datav1alpha1.RequireAffinityStrategy,
						DependOn: ptr.To(dependencyRef("DataProcess", "affinity-op", namespace)),
					},
				},
			},
			Status: datav1alpha1.OperationStatus{
				WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)},
			},
		}
		ctx := makeTestCtx(s, "waiting", namespace, runAfterOp, dependOnOp, waiting)

		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		updated := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "waiting", Namespace: namespace}, updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(common.PhaseFailed))
		Expect(updated.Status.WaitingFor.Dependencies).To(HaveLen(2))
		Expect(updated.Status.Conditions[0].Message).To(ContainSubstring("dependon boom"))
	})

	It("leaves objects without RunAfter completely untouched", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		plain := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: namespace},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhasePending},
		}
		ctx := makeTestCtx(s, "plain", namespace, plain)

		needRequeue, err := reconcileDataLoad(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(needRequeue).To(BeFalse())

		updated := &datav1alpha1.DataLoad{}
		Expect(ctx.Client.Get(context.TODO(), types.NamespacedName{Name: "plain", Namespace: namespace}, updated)).To(Succeed())
		Expect(updated.Status.WaitingFor.OperationComplete).To(BeNil())
		Expect(updated.Status.WaitingFor.Dependencies).To(BeEmpty())
		Expect(updated.Status.Phase).To(Equal(common.PhasePending))
	})

	It("persists terminal records for all four operation kinds", func() {
		s := runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())

		precedingLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "load-src", Namespace: namespace, UID: types.UID("uid-load")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		precedingBackup := &datav1alpha1.DataBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "backup-src", Namespace: namespace, UID: types.UID("uid-backup")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		precedingMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "migrate-src", Namespace: namespace, UID: types.UID("uid-migrate")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}
		precedingProcess := &datav1alpha1.DataProcess{
			ObjectMeta: metav1.ObjectMeta{Name: "process-src", Namespace: namespace, UID: types.UID("uid-process")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		}

		waitingBackup := &datav1alpha1.DataBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "backup-wait", Namespace: namespace},
			Spec: datav1alpha1.DataBackupSpec{
				RunAfter: &datav1alpha1.OperationRef{ObjectRef: dependencyRef("DataLoad", "load-src", namespace)},
			},
			Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
		}
		waitingMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "migrate-wait", Namespace: namespace},
			Spec: datav1alpha1.DataMigrateSpec{
				RunAfter: &datav1alpha1.OperationRef{ObjectRef: dependencyRef("DataBackup", "backup-src", namespace)},
			},
			Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
		}
		waitingProcess := &datav1alpha1.DataProcess{
			ObjectMeta: metav1.ObjectMeta{Name: "process-wait", Namespace: namespace},
			Spec: datav1alpha1.DataProcessSpec{
				RunAfter: &datav1alpha1.OperationRef{ObjectRef: dependencyRef("DataMigrate", "migrate-src", namespace)},
			},
			Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
		}
		waitingLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "load-wait", Namespace: namespace},
			Spec: datav1alpha1.DataLoadSpec{
				RunAfter: &datav1alpha1.OperationRef{ObjectRef: dependencyRef("DataProcess", "process-src", namespace)},
			},
			Status: datav1alpha1.OperationStatus{WaitingFor: datav1alpha1.WaitingStatus{OperationComplete: ptr.To(true)}},
		}

		sharedClient := fake.NewFakeClientWithScheme(s,
			precedingLoad, precedingBackup, precedingMigrate, precedingProcess,
			waitingBackup, waitingMigrate, waitingProcess, waitingLoad)

		ctxFor := func(name string) reconcileRequestContext {
			return reconcileRequestContext{
				Context:        context.TODO(),
				NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
				Client:         sharedClient,
				Log:            logf.Log.WithName("test"),
				Recorder:       record.NewFakeRecorder(32),
			}
		}

		By("DataBackup")
		requeue, err := reconcileDataBackup(ctxFor("backup-wait"))
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeFalse())
		By("DataMigrate")
		requeue, err = reconcileDataMigrate(ctxFor("migrate-wait"))
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeFalse())
		By("DataProcess")
		requeue, err = reconcileDataProcess(ctxFor("process-wait"))
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeFalse())
		By("DataLoad")
		requeue, err = reconcileDataLoad(ctxFor("load-wait"))
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeFalse())

		backupGot := &datav1alpha1.DataBackup{}
		Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "backup-wait", Namespace: namespace}, backupGot)).To(Succeed())
		Expect(*backupGot.Status.WaitingFor.OperationComplete).To(BeFalse())
		Expect(backupGot.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-load")))

		migrateGot := &datav1alpha1.DataMigrate{}
		Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "migrate-wait", Namespace: namespace}, migrateGot)).To(Succeed())
		Expect(migrateGot.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-backup")))

		processGot := &datav1alpha1.DataProcess{}
		Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "process-wait", Namespace: namespace}, processGot)).To(Succeed())
		Expect(processGot.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-migrate")))

		loadGot := &datav1alpha1.DataLoad{}
		Expect(sharedClient.Get(context.TODO(), types.NamespacedName{Name: "load-wait", Namespace: namespace}, loadGot)).To(Succeed())
		Expect(loadGot.Status.WaitingFor.Dependencies[0].UID).To(Equal(types.UID("uid-process")))
	})
})
