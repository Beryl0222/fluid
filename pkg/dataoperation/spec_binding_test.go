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

package dataoperation

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
)

// updateCountingClient counts Update calls to assert idempotent writes.
type updateCountingClient struct {
	client.Client
	updateCount int
}

func (u *updateCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	u.updateCount++
	return u.Client.Update(ctx, obj, opts...)
}

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	Expect(v1.AddToScheme(s)).To(Succeed())
	Expect(datav1alpha1.AddToScheme(s)).To(Succeed())
	return s
}

func decodePayload(raw string) acceptedSpecPayload {
	var payload acceptedSpecPayload
	Expect(json.Unmarshal([]byte(raw), &payload)).To(Succeed())
	return payload
}

var _ = Describe("Accepted spec binding", func() {
	const namespace = "default"

	Context("FreezeAcceptedSpec", func() {
		It("persists the current generation and spec and is idempotent", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "load-freeze",
					Namespace:  namespace,
					Generation: 1,
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset: datav1alpha1.TargetDataset{Name: "dataset-a", Namespace: namespace},
					Policy:  datav1alpha1.Once,
				},
			}
			base := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			counting := &updateCountingClient{Client: base}

			By("first freeze writes the annotation once")
			Expect(FreezeAcceptedSpec(counting, dataLoad)).To(Succeed())
			Expect(counting.updateCount).To(Equal(1))

			persisted := &datav1alpha1.DataLoad{}
			Expect(base.Get(context.TODO(), client.ObjectKeyFromObject(dataLoad), persisted)).To(Succeed())
			raw, found := persisted.Annotations[AcceptedSpecAnnotation]
			Expect(found).To(BeTrue())
			payload := decodePayload(raw)
			Expect(payload.Generation).To(Equal(int64(1)))

			var frozenSpec datav1alpha1.DataLoadSpec
			Expect(json.Unmarshal(payload.Spec, &frozenSpec)).To(Succeed())
			Expect(frozenSpec.Dataset.Name).To(Equal("dataset-a"))
			Expect(frozenSpec.Policy).To(Equal(datav1alpha1.Once))

			By("second freeze with the same generation issues no update")
			Expect(FreezeAcceptedSpec(counting, dataLoad)).To(Succeed())
			Expect(counting.updateCount).To(Equal(1))
		})

		It("re-freezes when the generation changes", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "load-regen", Namespace: namespace, Generation: 1},
				Spec:       datav1alpha1.DataLoadSpec{Dataset: datav1alpha1.TargetDataset{Name: "dataset-a"}},
			}
			base := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			Expect(FreezeAcceptedSpec(base, dataLoad)).To(Succeed())

			dataLoad.Generation = 2
			dataLoad.Spec.Dataset.Name = "dataset-b"
			Expect(FreezeAcceptedSpec(base, dataLoad)).To(Succeed())

			persisted := &datav1alpha1.DataLoad{}
			Expect(base.Get(context.TODO(), client.ObjectKeyFromObject(dataLoad), persisted)).To(Succeed())
			payload := decodePayload(persisted.Annotations[AcceptedSpecAnnotation])
			Expect(payload.Generation).To(Equal(int64(2)))
			var frozenSpec datav1alpha1.DataLoadSpec
			Expect(json.Unmarshal(payload.Spec, &frozenSpec)).To(Succeed())
			Expect(frozenSpec.Dataset.Name).To(Equal("dataset-b"))
		})
	})

	Context("IsGenerationBound", func() {
		It("binds only the active execution phases", func() {
			Expect(IsGenerationBound(common.PhaseNone)).To(BeFalse())
			Expect(IsGenerationBound(common.PhasePending)).To(BeFalse())
			Expect(IsGenerationBound(common.PhaseExecuting)).To(BeTrue())
			Expect(IsGenerationBound(common.PhaseComplete)).To(BeTrue())
			Expect(IsGenerationBound(common.PhaseFailed)).To(BeTrue())
		})
	})

	DescribeTable("ApplyAcceptedSpec restores the frozen spec for every operation type",
		func(liveObj client.Object, frozenTargetName string, mutateLive func(client.Object), effectiveTarget func(client.Object) string) {
			scheme := newTestScheme()
			base := fake.NewFakeClientWithScheme(scheme, liveObj)

			// Freeze the original (generation 1) spec, then simulate a live drift to generation 2.
			Expect(FreezeAcceptedSpec(base, liveObj)).To(Succeed())
			frozenAnnotation := liveObj.GetAnnotations()[AcceptedSpecAnnotation]

			liveObj.SetGeneration(2)
			mutateLive(liveObj)

			By("non-bound phases keep using the live spec")
			pending, err := ApplyAcceptedSpec(liveObj, common.PhasePending)
			Expect(err).NotTo(HaveOccurred())
			Expect(pending).To(BeIdenticalTo(liveObj))

			By("bound phase without annotation is a no-op")
			liveObj.SetAnnotations(nil)
			noSnapshot, err := ApplyAcceptedSpec(liveObj, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(noSnapshot).To(BeIdenticalTo(liveObj))
			liveObj.SetAnnotations(map[string]string{AcceptedSpecAnnotation: frozenAnnotation})

			By("bound phase with snapshot returns the frozen spec but live metadata")
			effective, err := ApplyAcceptedSpec(liveObj, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective).NotTo(BeIdenticalTo(liveObj))
			Expect(effectiveTarget(effective)).To(Equal(frozenTargetName))
			Expect(effective.GetName()).To(Equal(liveObj.GetName()))
			Expect(effective.GetNamespace()).To(Equal(liveObj.GetNamespace()))
			Expect(effective.GetResourceVersion()).To(Equal(liveObj.GetResourceVersion()))
			Expect(effective.GetFinalizers()).To(Equal(liveObj.GetFinalizers()))

			By("the live object is not mutated by applying the snapshot")
			Expect(effectiveTarget(liveObj)).ToNot(Equal(frozenTargetName))

			By("Complete and Failed are bound as well")
			for _, phase := range []common.Phase{common.PhaseComplete, common.PhaseFailed} {
				terminal, err := ApplyAcceptedSpec(liveObj, phase)
				Expect(err).NotTo(HaveOccurred())
				Expect(effectiveTarget(terminal)).To(Equal(frozenTargetName))
			}
		},
		Entry("DataLoad",
			&datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "load", Namespace: namespace, Generation: 1, Finalizers: []string{"f"}},
				Spec:       datav1alpha1.DataLoadSpec{Dataset: datav1alpha1.TargetDataset{Name: "frozen-ds"}, Policy: datav1alpha1.Once},
			},
			"frozen-ds",
			func(o client.Object) { o.(*datav1alpha1.DataLoad).Spec.Dataset.Name = "live-ds" },
			func(o client.Object) string { return o.(*datav1alpha1.DataLoad).Spec.Dataset.Name },
		),
		Entry("DataMigrate",
			&datav1alpha1.DataMigrate{
				ObjectMeta: metav1.ObjectMeta{Name: "migrate", Namespace: namespace, Generation: 1},
				Spec: datav1alpha1.DataMigrateSpec{
					From:        datav1alpha1.DataToMigrate{DataSet: &datav1alpha1.DatasetToMigrate{Name: "frozen-ds", Namespace: namespace}},
					Parallelism: 3,
					Policy:      datav1alpha1.Cron,
				},
			},
			"frozen-ds",
			func(o client.Object) { o.(*datav1alpha1.DataMigrate).Spec.From.DataSet.Name = "live-ds" },
			func(o client.Object) string { return o.(*datav1alpha1.DataMigrate).Spec.From.DataSet.Name },
		),
		Entry("DataProcess",
			&datav1alpha1.DataProcess{
				ObjectMeta: metav1.ObjectMeta{Name: "process", Namespace: namespace, Generation: 1},
				Spec: datav1alpha1.DataProcessSpec{
					Dataset:   datav1alpha1.TargetDatasetWithMountPath{TargetDataset: datav1alpha1.TargetDataset{Name: "frozen-ds", Namespace: namespace}, MountPath: "/data"},
					Policy:    datav1alpha1.Once,
					Processor: datav1alpha1.Processor{Script: &datav1alpha1.ScriptProcessor{Source: "echo frozen"}},
				},
			},
			"frozen-ds",
			func(o client.Object) { o.(*datav1alpha1.DataProcess).Spec.Dataset.Name = "live-ds" },
			func(o client.Object) string { return o.(*datav1alpha1.DataProcess).Spec.Dataset.Name },
		),
	)

	Context("ApplyAcceptedSpec with a malformed annotation", func() {
		It("tolerates the corruption and falls back to the live object instead of erroring", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "load-bad",
					Namespace:   namespace,
					Annotations: map[string]string{AcceptedSpecAnnotation: "{not-json"},
				},
				Status: datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
			}
			effective, err := ApplyAcceptedSpec(dataLoad, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective).To(BeIdenticalTo(dataLoad))
		})
	})

	Context("ApplyAcceptedSpec with a syntactically valid but type-incompatible snapshot", func() {
		incompatibleAnnotation := func() map[string]string {
			// loadMetadata is a bool in DataLoadSpec; a string makes the apply round-trip fail.
			payload, err := json.Marshal(&acceptedSpecPayload{
				Generation: 1,
				Spec:       json.RawMessage(`{"loadMetadata":"not-a-bool"}`),
			})
			Expect(err).NotTo(HaveOccurred())
			return map[string]string{AcceptedSpecAnnotation: string(payload)}
		}

		It("falls back to the live object without returning a blocking error", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "load-type-mismatch",
					Namespace:   namespace,
					Annotations: incompatibleAnnotation(),
				},
				Status: datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
			}
			effective, err := ApplyAcceptedSpec(dataLoad, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective).To(BeIdenticalTo(dataLoad))
		})

		It("is repaired by ReconcileSpecBinding with a warning and a valid re-adopted snapshot", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "load-type-mismatch",
					Namespace:   namespace,
					Generation:  3,
					Annotations: incompatibleAnnotation(),
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset:      datav1alpha1.TargetDataset{Name: "ds"},
					LoadMetadata: true,
				},
			}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			recorder := record.NewFakeRecorder(8)
			Expect(ReconcileSpecBinding(c, recorder, fake.NullLogger(), dataLoad, common.PhaseExecuting)).To(Succeed())

			Expect(<-recorder.Events).To(ContainSubstring(common.DataOperationSpecCorrupted))

			persisted := &datav1alpha1.DataLoad{}
			Expect(c.Get(context.TODO(), client.ObjectKeyFromObject(dataLoad), persisted)).To(Succeed())
			payload := decodePayload(persisted.Annotations[AcceptedSpecAnnotation])
			Expect(payload.Generation).To(Equal(int64(3)))
			var repaired datav1alpha1.DataLoadSpec
			Expect(json.Unmarshal(payload.Spec, &repaired)).To(Succeed())
			Expect(repaired.LoadMetadata).To(BeTrue())

			By("the repaired snapshot now applies without falling back")
			effective, err := ApplyAcceptedSpec(persisted, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective).NotTo(BeIdenticalTo(persisted))
			Expect(effective.(*datav1alpha1.DataLoad).Spec.Dataset.Name).To(Equal("ds"))
		})
	})

	Context("DetectSpecDrift", func() {
		It("reports no drift without a snapshot", func() {
			dataLoad := &datav1alpha1.DataLoad{ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: namespace, Generation: 3}}
			drifted, accepted, current, err := DetectSpecDrift(dataLoad)
			Expect(err).NotTo(HaveOccurred())
			Expect(drifted).To(BeFalse())
			Expect(accepted).To(Equal(int64(0)))
			Expect(current).To(Equal(int64(3)))
		})

		It("compares accepted and current generations", func() {
			dataLoad := &datav1alpha1.DataLoad{ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: namespace, Generation: 1}}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			Expect(FreezeAcceptedSpec(c, dataLoad)).To(Succeed())

			By("same generation -> no drift")
			drifted, accepted, current, err := DetectSpecDrift(dataLoad)
			Expect(err).NotTo(HaveOccurred())
			Expect(drifted).To(BeFalse())
			Expect(accepted).To(Equal(int64(1)))
			Expect(current).To(Equal(int64(1)))

			By("advanced generation -> drift")
			dataLoad.Generation = 2
			drifted, accepted, current, err = DetectSpecDrift(dataLoad)
			Expect(err).NotTo(HaveOccurred())
			Expect(drifted).To(BeTrue())
			Expect(accepted).To(Equal(int64(1)))
			Expect(current).To(Equal(int64(2)))
		})

		It("returns an error for a malformed snapshot", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: namespace, Generation: 2,
					Annotations: map[string]string{AcceptedSpecAnnotation: "bad"}},
			}
			_, _, _, err := DetectSpecDrift(dataLoad)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("ReconcileSpecBinding", func() {
		It("freezes the live spec at Pending", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: namespace, Generation: 1},
				Spec:       datav1alpha1.DataLoadSpec{Dataset: datav1alpha1.TargetDataset{Name: "ds"}},
			}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			recorder := record.NewFakeRecorder(8)
			Expect(ReconcileSpecBinding(c, recorder, fake.NullLogger(), dataLoad, common.PhasePending)).To(Succeed())
			Expect(dataLoad.GetAnnotations()).To(HaveKey(AcceptedSpecAnnotation))
			Expect(recorder.Events).To(BeEmpty())
		})

		It("self-heals a missing snapshot in an active phase", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "heal", Namespace: namespace, Generation: 1},
			}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			Expect(ReconcileSpecBinding(c, record.NewFakeRecorder(8), fake.NullLogger(), dataLoad, common.PhaseExecuting)).To(Succeed())
			Expect(dataLoad.GetAnnotations()).To(HaveKey(AcceptedSpecAnnotation))
		})

		It("repairs a corrupt snapshot with a warning and re-adopts the current spec", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "corrupt",
					Namespace:   namespace,
					Generation:  2,
					Annotations: map[string]string{AcceptedSpecAnnotation: "{not-json"},
				},
				Spec: datav1alpha1.DataLoadSpec{Dataset: datav1alpha1.TargetDataset{Name: "ds"}},
			}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			recorder := record.NewFakeRecorder(8)
			Expect(ReconcileSpecBinding(c, recorder, fake.NullLogger(), dataLoad, common.PhaseExecuting)).To(Succeed())

			By("a corruption warning is emitted")
			Expect(<-recorder.Events).To(ContainSubstring(common.DataOperationSpecCorrupted))

			By("the annotation is repaired to a valid snapshot of the current spec/generation")
			persisted := &datav1alpha1.DataLoad{}
			Expect(c.Get(context.TODO(), client.ObjectKeyFromObject(dataLoad), persisted)).To(Succeed())
			payload := decodePayload(persisted.Annotations[AcceptedSpecAnnotation])
			Expect(payload.Generation).To(Equal(int64(2)))
			var frozenSpec datav1alpha1.DataLoadSpec
			Expect(json.Unmarshal(payload.Spec, &frozenSpec)).To(Succeed())
			Expect(frozenSpec.Dataset.Name).To(Equal("ds"))

			By("subsequent reads no longer report an error")
			effective, err := ApplyAcceptedSpec(persisted, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective.GetAnnotations()[AcceptedSpecAnnotation]).To(Equal(persisted.Annotations[AcceptedSpecAnnotation]))
		})

		It("emits a warning event on drift but changes nothing else", func() {
			dataLoad := &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "drift", Namespace: namespace, Generation: 1},
				Spec:       datav1alpha1.DataLoadSpec{Dataset: datav1alpha1.TargetDataset{Name: "ds-a"}},
			}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			Expect(FreezeAcceptedSpec(c, dataLoad)).To(Succeed())

			liveBefore := dataLoad.DeepCopy()
			dataLoad.Generation = 2
			dataLoad.Spec.Dataset.Name = "ds-b"
			// Persist the drift so the fake client reflects what the API server would store.
			Expect(c.Update(context.TODO(), dataLoad)).To(Succeed())
			recorder := record.NewFakeRecorder(8)
			Expect(ReconcileSpecBinding(c, recorder, fake.NullLogger(), dataLoad, common.PhaseExecuting)).To(Succeed())

			By("a single warning event carrying both generations is recorded")
			event := <-recorder.Events
			Expect(event).To(ContainSubstring(common.DataOperationSpecDrifted))
			Expect(event).To(ContainSubstring("1"))
			Expect(event).To(ContainSubstring("2"))
			Expect(recorder.Events).To(BeEmpty())

			By("the persisted object keeps the original snapshot and is otherwise untouched")
			persisted := &datav1alpha1.DataLoad{}
			Expect(c.Get(context.TODO(), client.ObjectKeyFromObject(dataLoad), persisted)).To(Succeed())
			payload := decodePayload(persisted.Annotations[AcceptedSpecAnnotation])
			Expect(payload.Generation).To(Equal(int64(1)))
			var frozenSpec datav1alpha1.DataLoadSpec
			Expect(json.Unmarshal(payload.Spec, &frozenSpec)).To(Succeed())
			Expect(frozenSpec.Dataset.Name).To(Equal("ds-a"))
			Expect(persisted.Generation).To(Equal(int64(2)))
			Expect(persisted.Spec.Dataset.Name).To(Equal("ds-b"))
			Expect(persisted.Annotations).To(Equal(liveBefore.Annotations))
		})

		It("does not emit events when the generation matches", func() {
			dataLoad := &datav1alpha1.DataLoad{ObjectMeta: metav1.ObjectMeta{Name: "stable", Namespace: namespace, Generation: 1}}
			c := fake.NewFakeClientWithScheme(newTestScheme(), dataLoad)
			Expect(FreezeAcceptedSpec(c, dataLoad)).To(Succeed())
			recorder := record.NewFakeRecorder(8)
			Expect(ReconcileSpecBinding(c, recorder, fake.NullLogger(), dataLoad, common.PhaseComplete)).To(Succeed())
			Expect(recorder.Events).To(BeEmpty())
		})

		It("is a no-op at None and unknown phases", func() {
			configMap := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: namespace}}
			c := fake.NewFakeClientWithScheme(newTestScheme(), configMap)
			Expect(ReconcileSpecBinding(c, record.NewFakeRecorder(8), fake.NullLogger(), configMap, common.PhaseNone)).To(Succeed())
			Expect(configMap.GetAnnotations()).To(BeEmpty())

			effective, err := ApplyAcceptedSpec(configMap, common.PhaseExecuting)
			Expect(err).NotTo(HaveOccurred())
			Expect(effective).To(BeIdenticalTo(configMap))
		})
	})
})
