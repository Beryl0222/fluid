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

package databackup

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
)

func gateScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func gateOperation(db *v1alpha1.DataBackup) *dataBackupOperation {
	return &dataBackupOperation{
		Client:     fake.NewFakeClientWithScheme(gateScheme(), db),
		Log:        ctrl.Log.WithName("test"),
		Recorder:   record.NewFakeRecorder(20),
		dataBackup: db,
	}
}

var _ = Describe("ReconcileScheduledRound", func() {
	ctx := cruntime.ReconcileRequestContext{Log: fake.NullLogger()}

	It("re-enters Executing for an already open round without advancing the scheduled time", func() {
		db := cronDataBackup()
		db.Status.Phase = common.PhasePending
		last := db.Status.LastScheduleTime
		op := gateOperation(db)

		acquired, released := 0, 0
		open, _, err := op.ReconcileScheduledRound(ctx, &db.Status, func() error {
			acquired++
			return nil
		}, func() error {
			released++
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(open).To(BeTrue())
		Expect(acquired).To(Equal(1))
		Expect(released).To(Equal(0))
		Expect(db.Status.Phase).To(Equal(common.PhaseExecuting))
		Expect(db.Status.LastScheduleTime).To(Equal(last))
		Expect(db.Status.CurrentExecutionID).To(Equal(testExecutionID))
	})

	It("starts exactly one new round when a scheduled time is due, recording the time and identity", func() {
		db := cronDataBackup()
		db.ObjectMeta.CreationTimestamp = v1.NewTime(time.Now().Add(-5 * time.Minute))
		db.Status = v1alpha1.OperationStatus{Phase: common.PhasePending}

		op := gateOperation(db)
		acquired := 0
		open, _, err := op.ReconcileScheduledRound(ctx, &db.Status, func() error {
			acquired++
			return nil
		}, func() error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(open).To(BeTrue())
		Expect(acquired).To(Equal(1))
		Expect(db.Status.Phase).To(Equal(common.PhaseExecuting))
		Expect(db.Status.LastScheduleTime).NotTo(BeNil())
		Expect(db.Status.CurrentExecutionID).NotTo(BeEmpty())

		// the same scheduled time must never be started again
		Expect(db.Status.CurrentExecutionID).To(Equal(executionID(db.Status.LastScheduleTime.Time)))
	})

	It("waits for the next schedule without acquiring the occupation when no scheduled time is due", func() {
		db := cronDataBackup()
		db.Spec.Schedule = "0 0 1 1 *"
		last := v1.NewTime(time.Now().Add(-15 * time.Second))
		db.Status = v1alpha1.OperationStatus{
			Phase:           common.PhasePending,
			LastScheduleTime: &last,
		}

		op := gateOperation(db)
		acquired, released := 0, 0
		open, requeueAfter, err := op.ReconcileScheduledRound(ctx, &db.Status, func() error {
			acquired++
			return nil
		}, func() error {
			released++
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(open).To(BeFalse())
		Expect(acquired).To(Equal(0))
		Expect(released).To(Equal(1))
		Expect(requeueAfter).To(BeNumerically(">", 0))
		Expect(requeueAfter).To(BeNumerically("<=", maxScheduledRequeueInterval))
		Expect(db.Status.Phase).To(Equal(common.PhasePending))
	})

	It("reports a clear error without opening a round when the schedule is invalid", func() {
		db := cronDataBackup()
		db.Spec.Schedule = "not-a-cron"
		db.Status = v1alpha1.OperationStatus{Phase: common.PhasePending}

		op := gateOperation(db)
		acquired := 0
		open, _, err := op.ReconcileScheduledRound(ctx, &db.Status, func() error {
			acquired++
			return nil
		}, func() error { return nil })
		Expect(err).To(HaveOccurred())
		Expect(open).To(BeFalse())
		Expect(acquired).To(Equal(0))
		Expect(db.Status.Conditions).To(HaveLen(1))
		Expect(db.Status.Conditions[0].Reason).To(Equal(cdatabackup.ReasonInvalidCronSchedule))
	})
})

var _ = Describe("cron policy selection", func() {
	It("validates the cron schedule in Validate", func() {
		db := cronDataBackup()
		db.Spec.Schedule = ""
		op := gateOperation(db)
		conditions, err := op.Validate(cruntime.ReconcileRequestContext{Log: fake.NullLogger()})
		Expect(err).To(HaveOccurred())
		Expect(conditions).To(HaveLen(1))
		Expect(conditions[0].Reason).To(Equal(cdatabackup.ReasonInvalidCronSchedule))

		db.Spec.Schedule = "invalid"
		conditions, err = op.Validate(cruntime.ReconcileRequestContext{Log: fake.NullLogger()})
		Expect(err).To(HaveOccurred())
		Expect(conditions[0].Reason).To(Equal(cdatabackup.ReasonInvalidCronSchedule))

		db.Spec.Schedule = "0 2 * * *"
		conditions, err = op.Validate(cruntime.ReconcileRequestContext{Log: fake.NullLogger()})
		Expect(err).NotTo(HaveOccurred())
		Expect(conditions).To(BeNil())
	})

	It("selects the cron handler and no TTL for Cron policy", func() {
		db := cronDataBackup()
		op := gateOperation(db)
		Expect(op.IsScheduled()).To(BeTrue())
		_, ok := op.GetStatusHandler().(*CronHandler)
		Expect(ok).To(BeTrue())
		ttl, err := op.GetTTL()
		Expect(err).NotTo(HaveOccurred())
		Expect(ttl).To(BeNil())
	})

	It("keeps the Once handler and TTL behavior for default Once policy", func() {
		db := cronDataBackup()
		db.Spec.Policy = v1alpha1.Once
		ttlVal := int32(300)
		db.Spec.TTLSecondsAfterFinished = &ttlVal
		op := gateOperation(db)
		Expect(op.IsScheduled()).To(BeFalse())
		_, ok := op.GetStatusHandler().(*OnceHandler)
		Expect(ok).To(BeTrue())
		ttl, err := op.GetTTL()
		Expect(err).NotTo(HaveOccurred())
		Expect(ttl).NotTo(BeNil())
		Expect(*ttl).To(Equal(int32(300)))
	})
})
