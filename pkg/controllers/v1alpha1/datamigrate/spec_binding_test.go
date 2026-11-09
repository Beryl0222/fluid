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

package datamigrate

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
)

var _ = Describe("DataMigrateReconciler Build accepted-spec binding", func() {
	var scheme *runtime.Scheme

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(scheme)).To(Succeed())
	})

	newReconciler := func() *DataMigrateReconciler {
		return NewDataMigrateReconciler(
			fake.NewFakeClientWithScheme(scheme),
			fake.NullLogger(),
			scheme,
			record.NewFakeRecorder(16),
		)
	}

	It("keeps the live spec when no generation is bound", func() {
		dataMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataMigrateSpec{
				From:        datav1alpha1.DataToMigrate{DataSet: &datav1alpha1.DatasetToMigrate{Name: "live-ds", Namespace: "default"}},
				Parallelism: 5,
				Policy:      datav1alpha1.Once,
			},
		}
		dataMigrate.Status.Phase = common.PhasePending

		op, err := newReconciler().Build(dataMigrate)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.GetOperationObject()).To(BeIdenticalTo(dataMigrate))
		Expect(op.GetParallelTaskNumber()).To(Equal(int32(5)))
	})

	It("uses the accepted parallelism, policy and target in an active generation despite drift", func() {
		dataMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataMigrateSpec{
				From:        datav1alpha1.DataToMigrate{DataSet: &datav1alpha1.DatasetToMigrate{Name: "frozen-ds", Namespace: "default"}},
				Parallelism: 3,
				Policy:      datav1alpha1.Cron,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataMigrate)
		Expect(dataoperation.FreezeAcceptedSpec(client, dataMigrate)).To(Succeed())

		// Live drift: target, parallelism and policy are all changed after the generation started.
		dataMigrate.Generation = 2
		dataMigrate.Spec.From.DataSet.Name = "live-ds"
		dataMigrate.Spec.Parallelism = 8
		dataMigrate.Spec.Policy = datav1alpha1.Once
		dataMigrate.Status.Phase = common.PhaseExecuting

		op, err := newReconciler().Build(dataMigrate)
		Expect(err).NotTo(HaveOccurred())

		By("scaling basis stays on the accepted parallelism")
		Expect(op.GetParallelTaskNumber()).To(Equal(int32(3)))

		By("handler and target stay on the accepted spec")
		Expect(op.GetStatusHandler()).To(BeAssignableToTypeOf(&CronStatusHandler{}))
		bound := op.GetOperationObject().(*datav1alpha1.DataMigrate)
		Expect(bound.Spec.From.DataSet.Name).To(Equal("frozen-ds"))
		Expect(bound.Spec.Policy).To(Equal(datav1alpha1.Cron))
		Expect(bound.Spec.Parallelism).To(Equal(int32(3)))

		By("the live object remains untouched and carries the drifted values")
		Expect(dataMigrate.Spec.Parallelism).To(Equal(int32(8)))
		Expect(dataMigrate.Spec.From.DataSet.Name).To(Equal("live-ds"))
	})

	It("adopts the new spec only after a generation re-enters Pending and re-freezes", func() {
		dataMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataMigrateSpec{
				From:        datav1alpha1.DataToMigrate{DataSet: &datav1alpha1.DatasetToMigrate{Name: "frozen-ds", Namespace: "default"}},
				Parallelism: 3,
				Policy:      datav1alpha1.Cron,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataMigrate)
		Expect(dataoperation.FreezeAcceptedSpec(client, dataMigrate)).To(Succeed())

		// A new cron run flips the phase back to Pending and the operator changed the spec.
		dataMigrate.Generation = 2
		dataMigrate.Spec.Parallelism = 8
		dataMigrate.Status.Phase = common.PhasePending

		By("Pending reads the live spec and re-freezes it as the next generation")
		Expect(dataoperation.FreezeAcceptedSpec(client, dataMigrate)).To(Succeed())
		dataMigrate.Status.Phase = common.PhaseExecuting

		op, err := newReconciler().Build(dataMigrate)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.GetParallelTaskNumber()).To(Equal(int32(8)))
	})

	It("binds the Cron worker scale-out and terminal scale-to-zero basis to the accepted parallelism", func() {
		dataMigrate := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "test-migrate", Namespace: "default", Generation: 1},
			Spec: datav1alpha1.DataMigrateSpec{
				From:        datav1alpha1.DataToMigrate{DataSet: &datav1alpha1.DatasetToMigrate{Name: "frozen-ds", Namespace: "default"}},
				Parallelism: 3,
				Policy:      datav1alpha1.Cron,
			},
		}
		client := fake.NewFakeClientWithScheme(scheme, dataMigrate)
		Expect(dataoperation.FreezeAcceptedSpec(client, dataMigrate)).To(Succeed())

		dataMigrate.Generation = 2
		dataMigrate.Spec.Parallelism = 8
		dataMigrate.Status.Phase = common.PhaseExecuting

		op, err := newReconciler().Build(dataMigrate)
		Expect(err).NotTo(HaveOccurred())

		By("the Cron handler observes the accepted parallelism used for worker scale-out")
		cronHandler, ok := op.GetStatusHandler().(*CronStatusHandler)
		Expect(ok).To(BeTrue())
		Expect(cronHandler.dataMigrate.Spec.Parallelism).To(Equal(int32(3)))

		By("the terminal scale-to-zero gate also reads the accepted parallelism")
		Expect(op.GetParallelTaskNumber()).To(Equal(int32(3)))
	})
})
