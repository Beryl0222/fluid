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
	"os"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
)

var _ = Describe("reconcilePending with a terminal RunAfter dependency failure", func() {

	var (
		fakeCtx   runtime.ReconcileRequestContext
		t         *base.TemplateEngine
		operation *mockOperation
		opStatus  *datav1alpha1.OperationStatus
		recorder  *record.FakeRecorder
	)

	BeforeEach(func() {
		fakeDataset := &datav1alpha1.Dataset{
			ObjectMeta: metav1.ObjectMeta{Name: mockDatasetName, Namespace: mockNamespace},
		}
		s := apimachineryRuntime.NewScheme()
		s.AddKnownTypes(datav1alpha1.GroupVersion, fakeDataset)
		recorder = record.NewFakeRecorder(10)
		fakeClient := fake.NewFakeClientWithScheme(s, fakeDataset)

		fakeCtx = runtime.ReconcileRequestContext{
			NamespacedName: types.NamespacedName{Namespace: mockNamespace, Name: mockDatasetName},
			Client:         fakeClient,
			Log:            fake.NullLogger(),
			RuntimeType:    "test-runtime",
			Recorder:       recorder,
		}

		_ = os.Setenv("FLUID_SYNC_RETRY_DURATION", "0s")
		t = base.NewTemplateEngine(nil, "test-engine", fakeCtx)
		operation = newMockOperation()
		opStatus = &datav1alpha1.OperationStatus{}
	})

	AfterEach(func() {
		_ = os.Unsetenv("FLUID_SYNC_RETRY_DURATION")
	})

	It("transitions the operation to Failed with a diagnosable condition when the dependency failed", func() {
		opStatus.Phase = common.PhasePending
		opStatus.WaitingFor = datav1alpha1.WaitingStatus{
			OperationComplete: ptr.To(false),
			PrecedingOperation: &datav1alpha1.PrecedingOperationStatus{
				ObjectRef:     datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: mockNamespace},
				UID:           "uid-1",
				ObservedPhase: common.PhaseFailed,
				Reason:        common.DataOperationDependencyFailed,
				Message:       "preceding operation DataLoad \"default/preceding\" reached phase Failed",
			},
		}

		result, err := t.Operate(fakeCtx, opStatus, operation)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())

		Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
		Expect(opStatus.Conditions).NotTo(BeEmpty())
		Expect(opStatus.Conditions[0].Type).To(Equal(common.Failed))
		Expect(opStatus.Conditions[0].Status).To(Equal(v1.ConditionTrue))
		Expect(opStatus.Conditions[0].Reason).To(Equal(common.DataOperationDependencyFailed))
		Expect(opStatus.Conditions[0].Message).To(ContainSubstring("reached phase Failed"))
		Expect(*opStatus.WaitingFor.OperationComplete).To(BeFalse())

		Eventually(recorder.Events).Should(Receive(ContainSubstring(common.DataOperationFailed)))
	})

	It("uses the identity conflict reason when a same-named object recreated the dependency", func() {
		opStatus.Phase = common.PhasePending
		opStatus.WaitingFor = datav1alpha1.WaitingStatus{
			OperationComplete: ptr.To(false),
			PrecedingOperation: &datav1alpha1.PrecedingOperationStatus{
				ObjectRef:     datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: mockNamespace},
				UID:           "uid-1",
				ObservedPhase: common.PhaseFailed,
				Reason:        common.DataOperationIdentityConflict,
				Message:       "observed UID \"uid-2\" does not match the pinned UID \"uid-1\"",
			},
		}

		result, err := t.Operate(fakeCtx, opStatus, operation)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())
		Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
		Expect(opStatus.Conditions[0].Reason).To(Equal(common.DataOperationIdentityConflict))
		Expect(opStatus.Conditions[0].Message).To(ContainSubstring("uid-1"))
	})

	It("does not fail when no terminal dependency failure has been observed", func() {
		opStatus.Phase = common.PhasePending
		opStatus.WaitingFor = datav1alpha1.WaitingStatus{
			OperationComplete: ptr.To(true),
			PrecedingOperation: &datav1alpha1.PrecedingOperationStatus{
				ObjectRef: datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: mockNamespace},
				UID:       "uid-1",
			},
		}

		result, err := t.Operate(fakeCtx, opStatus, operation)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())
		// Still waiting: the phase must not transition to Failed.
		Expect(opStatus.Phase).To(Equal(common.PhasePending))
		Expect(opStatus.Conditions).To(BeEmpty())
	})
})
