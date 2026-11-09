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

package dataload

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
)

var _ = Describe("DataLoadReconciler Build accepted-spec binding", func() {
	var (
		scheme     *runtime.Scheme
		recorder   *record.FakeRecorder
		reconciler *DataLoadReconciler
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(scheme)).To(Succeed())
		recorder = record.NewFakeRecorder(16)
		reconciler = NewDataLoadReconciler(
			fake.NewFakeClientWithScheme(scheme),
			fake.NullLogger(),
			scheme,
			recorder,
		)
	})

	It("keeps the live spec when no generation is bound", func() {
		dataLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "test-load", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset: datav1alpha1.TargetDataset{Name: "live-ds", Namespace: "default"},
				Policy:  datav1alpha1.Cron,
			},
		}
		dataLoad.Status.Phase = common.PhasePending

		op, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.GetOperationObject()).To(BeIdenticalTo(dataLoad))
		Expect(op.GetStatusHandler()).To(BeAssignableToTypeOf(&CronStatusHandler{}))
	})

	It("interprets an active generation with the accepted spec despite live drift", func() {
		dataLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "test-load", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset: datav1alpha1.TargetDataset{Name: "frozen-ds", Namespace: "default"},
				Policy:  datav1alpha1.Once,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataLoad)
		// Accept the generation-1 spec (the one that created the in-flight workloads).
		Expect(dataoperation.FreezeAcceptedSpec(client, dataLoad)).To(Succeed())

		// The operator then edits the live spec (generation 2) while the operation is Executing.
		dataLoad.Generation = 2
		dataLoad.Spec.Dataset.Name = "live-ds"
		dataLoad.Spec.Policy = datav1alpha1.Cron
		dataLoad.Status.Phase = common.PhaseExecuting

		op, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())

		bound := op.GetOperationObject().(*datav1alpha1.DataLoad)
		By("the effective spec is the accepted spec")
		Expect(bound.Spec.Dataset.Name).To(Equal("frozen-ds"))
		Expect(bound.Spec.Policy).To(Equal(datav1alpha1.Once))

		By("the status handler is selected from the accepted policy, not the drifted live policy")
		Expect(op.GetStatusHandler()).To(BeAssignableToTypeOf(&OnceStatusHandler{}))

		By("the input live object is not mutated and keeps the drifted spec")
		Expect(dataLoad.Spec.Dataset.Name).To(Equal("live-ds"))
		Expect(dataLoad.Spec.Policy).To(Equal(datav1alpha1.Cron))

		By("metadata is preserved from the live object")
		Expect(bound.GetName()).To(Equal(dataLoad.GetName()))
		Expect(bound.GetResourceVersion()).To(Equal(dataLoad.GetResourceVersion()))
	})
})

var _ = Describe("DataLoad generation-bound lock and next-generation adoption", func() {
	var scheme *runtime.Scheme

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(scheme)).To(Succeed())
	})

	It("keeps the dataset lock target and release name on the accepted generation under target drift", func() {
		frozenDataset := &datav1alpha1.Dataset{
			ObjectMeta: metav1.ObjectMeta{Name: "frozen-ds", Namespace: "default"},
		}
		liveDataset := &datav1alpha1.Dataset{
			ObjectMeta: metav1.ObjectMeta{Name: "live-ds", Namespace: "default"},
		}
		dataLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "load", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset: datav1alpha1.TargetDataset{Name: "frozen-ds", Namespace: "default"},
				Policy:  datav1alpha1.Once,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, frozenDataset, liveDataset, dataLoad)
		reconciler := NewDataLoadReconciler(client, fake.NullLogger(), scheme, record.NewFakeRecorder(16))

		Expect(dataoperation.FreezeAcceptedSpec(client, dataLoad)).To(Succeed())

		// Target dataset is changed after the generation was accepted.
		dataLoad.Generation = 2
		dataLoad.Spec.Dataset.Name = "live-ds"
		dataLoad.Status.Phase = common.PhaseExecuting
		op, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())

		By("the lock is acquired/released against the accepted dataset, never the drifted one")
		target, err := op.GetTargetDataset()
		Expect(err).NotTo(HaveOccurred())
		Expect(target.Name).To(Equal("frozen-ds"))
		Expect(op.GetPossibleTargetDatasetNamespacedNames()).To(Equal([]types.NamespacedName{
			{Namespace: "default", Name: "frozen-ds"},
		}))

		By("the helm release identity is derived from the object name and is immune to spec drift")
		Expect(op.GetReleaseNameSpacedName().Name).To(Equal(utils.GetDataLoadReleaseName(dataLoad.Name)))
	})

	It("adopts a newly selected policy only after the next Pending generation re-freezes the spec", func() {
		dataLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "load", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset: datav1alpha1.TargetDataset{Name: "ds", Namespace: "default"},
				Policy:  datav1alpha1.Once,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataLoad)
		reconciler := NewDataLoadReconciler(client, fake.NullLogger(), scheme, record.NewFakeRecorder(16))

		Expect(dataoperation.FreezeAcceptedSpec(client, dataLoad)).To(Succeed())

		dataLoad.Generation = 2
		dataLoad.Spec.Policy = datav1alpha1.Cron
		dataLoad.Status.Phase = common.PhaseExecuting

		By("the running generation keeps interpreting the accepted Once policy")
		op, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.GetStatusHandler()).To(BeAssignableToTypeOf(&OnceStatusHandler{}))

		By("a new run returns to Pending and re-freezes the live spec as the next generation")
		dataLoad.Status.Phase = common.PhasePending
		Expect(dataoperation.FreezeAcceptedSpec(client, dataLoad)).To(Succeed())
		dataLoad.Status.Phase = common.PhaseExecuting

		nextOp, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())
		Expect(nextOp.GetStatusHandler()).To(BeAssignableToTypeOf(&CronStatusHandler{}))
	})

	It("computes TTL cleanup from the accepted spec even after the live policy/TTL drift", func() {
		ttl := int32(100)
		dataLoad := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "load", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset:                  datav1alpha1.TargetDataset{Name: "ds", Namespace: "default"},
				Policy:                   datav1alpha1.Once,
				TTLSecondsAfterFinished: &ttl,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataLoad)
		reconciler := NewDataLoadReconciler(client, fake.NullLogger(), scheme, record.NewFakeRecorder(16))
		Expect(dataoperation.FreezeAcceptedSpec(client, dataLoad)).To(Succeed())

		// Live drift switches to Cron (which would report a nil TTL) and changes the TTL value.
		dataLoad.Generation = 2
		dataLoad.Spec.Policy = datav1alpha1.Cron
		driftedTTL := int32(1)
		dataLoad.Spec.TTLSecondsAfterFinished = &driftedTTL
		dataLoad.Status.Phase = common.PhaseComplete
		dataLoad.Status.Conditions = []datav1alpha1.Condition{{
			Type:               common.Complete,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}}

		op, err := reconciler.Build(dataLoad)
		Expect(err).NotTo(HaveOccurred())

		By("the accepted Once policy still reports the accepted TTL, not the drifted nil/1s value")
		acceptedTTL, err := op.GetTTL()
		Expect(err).NotTo(HaveOccurred())
		Expect(acceptedTTL).NotTo(BeNil())
		Expect(*acceptedTTL).To(Equal(int32(100)))

		By("cleanup eligibility and remaining time are derived from the accepted spec")
		Expect(utils.NeedCleanUp(&dataLoad.Status, op)).To(BeTrue())
		remaining, err := utils.Timeleft(&dataLoad.Status, op)
		Expect(err).NotTo(HaveOccurred())
		Expect(remaining).NotTo(BeNil())
		Expect(*remaining).To(BeNumerically(">", 50*time.Second))
	})
})
