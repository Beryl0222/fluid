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
	"context"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdatabackup "github.com/fluid-cloudnative/fluid/pkg/databackup"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/compatibility"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

const (
	testBackupName  = "test"
	testDatasetName = "test-dataset"
	testCronJobName = "test-charts-backup"
	testExecutionID = "20261109T100000"
)

func cronTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = batchv1.AddToScheme(s)
	return s
}

func cronDataBackup() *v1alpha1.DataBackup {
	return &v1alpha1.DataBackup{
		ObjectMeta: v1.ObjectMeta{
			Name:      testBackupName,
			Namespace: "default",
		},
		Spec: v1alpha1.DataBackupSpec{
			Dataset:    testDatasetName,
			BackupPath: "local:///tmp/backup",
			Policy:     v1alpha1.Cron,
			Schedule:   "* * * * *",
		},
		Status: v1alpha1.OperationStatus{
			Phase:             common.PhaseExecuting,
			CurrentExecutionID: testExecutionID,
			LastScheduleTime:   ptrTime(2026, 11, 9, 10, 0),
		},
	}
}

func ptrTime(year int, month time.Month, day, hour, minute int) *v1.Time {
	t := v1.NewTime(time.Date(year, month, day, hour, minute, 0, 0, time.UTC))
	return &t
}

func lockedDataset() *v1alpha1.Dataset {
	return &v1alpha1.Dataset{
		ObjectMeta: v1.ObjectMeta{
			Name:      testDatasetName,
			Namespace: "default",
		},
		Status: v1alpha1.DatasetStatus{
			OperationRef: map[string]string{
				"DataBackup": testBackupName,
			},
		},
	}
}

func suspendedCronJob(suspended bool) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: v1.ObjectMeta{
			Name:      testCronJobName,
			Namespace: "default",
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          "* * * * *",
			Suspend:           &suspended,
			ConcurrencyPolicy: batchv1.ForbidConcurrent,
		},
	}
}

func roundJob(finished bool, succeed bool, finishTime *v1.Time) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: v1.ObjectMeta{
			Name:      testCronJobName + "-abcde",
			Namespace: "default",
			Labels: map[string]string{
				cdatabackup.CronJobLabel: testCronJobName,
			},
			CreationTimestamp: *ptrTime(2026, 11, 9, 10, 0),
		},
		Spec: batchv1.JobSpec{
			Selector: &v1.LabelSelector{
				MatchLabels: map[string]string{"job-name": testCronJobName + "-abcde"},
			},
		},
	}
	if finished {
		condType := batchv1.JobFailed
		if succeed {
			condType = batchv1.JobComplete
		}
		job.Status.Conditions = []batchv1.JobCondition{
			{
				Type:               condType,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: *finishTime,
			},
		}
	}
	return job
}

func roundPod(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name:      testCronJobName + "-abcde-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": testCronJobName + "-abcde"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Phase: phase},
	}
}

var _ = Describe("CronHandler", func() {
	var patch *gomonkey.Patches

	BeforeEach(func() {
		patch = gomonkey.ApplyFunc(compatibility.IsBatchV1CronJobSupported, func() bool {
			return true
		})
	})

	AfterEach(func() {
		if patch != nil {
			patch.Reset()
		}
	})

	newHandler := func(objects ...runtime.Object) (*CronHandler, *v1alpha1.DataBackup) {
		db := cronDataBackup()
		objs := append([]runtime.Object{db}, objects...)
		c := fake.NewFakeClientWithScheme(cronTestScheme(), objs...)
		return &CronHandler{
			Client:     c,
			Log:        fake.NullLogger(),
			Recorder:   record.NewFakeRecorder(20),
			dataBackup: db,
		}, db
	}

	ctx := func() cruntime.ReconcileRequestContext {
		return cruntime.ReconcileRequestContext{Log: fake.NullLogger()}
	}

	It("unsuspends the CronJob to release one job when the dataset occupation is held but no job exists yet", func() {
		cj := suspendedCronJob(true)
		handler, db := newHandler(lockedDataset(), cj)

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhaseExecuting))

		updated := &batchv1.CronJob{}
		Expect(handler.Client.Get(context.TODO(), types.NamespacedName{Namespace: "default", Name: testCronJobName}, updated)).To(Succeed())
		Expect(updated.Spec.Suspend).NotTo(BeNil())
		Expect(*updated.Spec.Suspend).To(BeFalse())
	})

	It("returns to Pending when the job is missing and the dataset occupation is not held", func() {
		cj := suspendedCronJob(true)
		handler, db := newHandler(cj)

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhasePending))
		// execution identity is kept so the same scheduled time is never started twice
		Expect(result.CurrentExecutionID).To(Equal(testExecutionID))
	})

	It("suspends the CronJob and keeps Executing while the round job is running", func() {
		cj := suspendedCronJob(false)
		job := roundJob(false, false, nil)
		pod := roundPod(corev1.PodRunning)
		handler, db := newHandler(lockedDataset(), cj, job, pod)

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhaseExecuting))
		Expect(result.Infos[cdatabackup.BackupExecutionJob]).To(Equal(job.Name))
		Expect(result.Infos[cdatabackup.BackupExecutionPod]).To(Equal(pod.Name))

		updated := &batchv1.CronJob{}
		Expect(handler.Client.Get(context.TODO(), types.NamespacedName{Namespace: "default", Name: testCronJobName}, updated)).To(Succeed())
		Expect(*updated.Spec.Suspend).To(BeTrue())

		// the round identity is stamped onto the job for recognition from Kubernetes objects
		updatedJob := &batchv1.Job{}
		Expect(handler.Client.Get(context.TODO(), types.NamespacedName{Namespace: "default", Name: job.Name}, updatedJob)).To(Succeed())
		Expect(updatedJob.Annotations[cdatabackup.ExecutionIDAnnotation]).To(Equal(testExecutionID))
	})

	It("records Complete with the successful time and diagnosable information when the round job succeeds", func() {
		finish := ptrTime(2026, 11, 9, 10, 2)
		cj := suspendedCronJob(false)
		job := roundJob(true, true, finish)
		pod := roundPod(corev1.PodSucceeded)
		handler, db := newHandler(lockedDataset(), cj, job, pod)

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhaseComplete))
		Expect(result.LastSuccessfulTime).To(Equal(db.Status.LastScheduleTime))
		Expect(result.Conditions).To(HaveLen(1))
		Expect(result.Conditions[0].Reason).To(Equal("BackupSuccessful"))
		Expect(result.Conditions[0].Message).To(ContainSubstring(testExecutionID))
		Expect(result.Infos[cdatabackup.BackupExecutionPod]).To(Equal(pod.Name))
	})

	It("records Failed with a diagnosable message when the round job fails", func() {
		finish := ptrTime(2026, 11, 9, 10, 2)
		cj := suspendedCronJob(false)
		job := roundJob(true, false, finish)
		pod := roundPod(corev1.PodFailed)
		handler, db := newHandler(lockedDataset(), cj, job, pod)

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhaseFailed))
		Expect(result.Conditions).To(HaveLen(1))
		Expect(result.Conditions[0].Reason).To(Equal("BackupFailed"))
		Expect(result.Conditions[0].Message).To(ContainSubstring(pod.Name))
	})

	It("closes the round to Pending on re-observation of the terminal job and keeps the failure condition", func() {
		finish := ptrTime(2026, 11, 9, 10, 2)
		cj := suspendedCronJob(false)
		job := roundJob(true, false, finish)
		pod := roundPod(corev1.PodFailed)
		handler, db := newHandler(lockedDataset(), cj, job, pod)
		db.Status.Phase = common.PhaseFailed
		db.Status.Conditions = []v1alpha1.Condition{
			{Type: common.Failed, Status: corev1.ConditionTrue, Reason: "BackupFailed"},
		}

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhasePending))
		Expect(result.CurrentExecutionID).To(BeEmpty())
		Expect(result.Duration).To(Equal("-"))
		// terminal condition is retained for diagnosis while waiting for the next round
		Expect(result.Conditions).To(HaveLen(1))
		Expect(result.Conditions[0].Reason).To(Equal("BackupFailed"))
	})

	It("keeps a Failed status untouched when the failure is caused by an invalid schedule", func() {
		cj := suspendedCronJob(false)
		handler, db := newHandler(lockedDataset(), cj)
		db.Status.Phase = common.PhaseFailed
		db.Status.CurrentExecutionID = ""
		db.Status.Conditions = []v1alpha1.Condition{
			{Type: common.Failed, Status: corev1.ConditionTrue, Reason: cdatabackup.ReasonInvalidCronSchedule},
		}

		result, err := handler.GetOperationStatus(ctx(), &db.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Phase).To(Equal(common.PhaseFailed))
	})
})
