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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

var _ = Describe("OnceHandler", func() {
	var (
		testScheme     *runtime.Scheme
		mockDataBackup *v1alpha1.DataBackup
	)

	BeforeEach(func() {
		testScheme = runtime.NewScheme()
		_ = v1alpha1.AddToScheme(testScheme)
		_ = corev1.AddToScheme(testScheme)

		mockDataBackup = &v1alpha1.DataBackup{
			ObjectMeta: v1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
			},
			Spec: v1alpha1.DataBackupSpec{},
		}
	})

	Describe("GetOperationStatus", func() {
		It("should return PhaseComplete when backup pod succeeded", func() {
			mockPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &mockPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "",
				},
				Client: client,
				Log:    fake.NullLogger(),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
		})

		It("should return PhaseFailed when backup pod failed", func() {
			mockFailedPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &mockFailedPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "",
				},
				Client: client,
				Log:    fake.NullLogger(),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
		})

		It("should return unchanged status when backup pod is still running", func() {
			runningPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &runningPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "",
				},
				Client: client,
				Log:    fake.NullLogger(),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			// Pod is still running, so status should not change phase
			Expect(opStatus.Phase).To(Equal(mockDataBackup.Status.Phase))
		})

		It("should start the missing grace period when backup pod does not exist and no terminal state was observed", func() {
			// No pod in the fake client: GetPodByName returns (nil, nil) for not-found.
			// The terminal state was never observed, so the phase must not flip to Complete
			// (never guess success). Instead a missing-grace marker is persisted and the phase
			// stays unchanged.
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "",
				},
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(mockDataBackup.Status.Phase))
			Expect(opStatus.Infos).To(HaveKey(cdatabackup.BackupPodMissingSince))
			Expect(opStatus.Infos[cdatabackup.BackupPodMissingSince]).NotTo(BeEmpty())
		})

		It("should persist terminal evidence when the backup pod succeeded", func() {
			mockPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:              "test-pod",
					Namespace:         "default",
					UID:               types.UID("pod-uid-1"),
					CreationTimestamp: v1.NewTime(time.Now().Add(-30 * time.Second)),
				},
				Spec: corev1.PodSpec{
					NodeName: "node-1",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
					Conditions: []corev1.PodCondition{
						{LastTransitionTime: v1.Now()},
					},
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &mockPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
			Expect(opStatus.Duration).NotTo(BeEmpty())
			Expect(opStatus.Infos[cdatabackup.BackupPodName]).To(Equal("test-pod"))
			Expect(opStatus.Infos[cdatabackup.BackupPodUID]).To(Equal("pod-uid-1"))
			Expect(opStatus.Infos[cdatabackup.BackupPodNodeName]).To(Equal("node-1"))
			Expect(opStatus.Infos[cdatabackup.BackupPodPhase]).To(Equal(string(corev1.PodSucceeded)))
			Expect(opStatus.Infos[cdatabackup.BackupPodStartTime]).NotTo(BeEmpty())
			Expect(opStatus.Infos[cdatabackup.BackupPodFinishTime]).NotTo(BeEmpty())
			// transient missing marker must not coexist with terminal evidence
			Expect(opStatus.Infos).NotTo(HaveKey(cdatabackup.BackupPodMissingSince))
		})

		It("should persist terminal evidence when the backup pod failed", func() {
			mockFailedPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					UID:       types.UID("pod-uid-failed"),
				},
				Spec: corev1.PodSpec{
					NodeName: "node-2",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &mockFailedPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
			Expect(opStatus.Infos[cdatabackup.BackupPodUID]).To(Equal("pod-uid-failed"))
			Expect(opStatus.Infos[cdatabackup.BackupPodNodeName]).To(Equal("node-2"))
			Expect(opStatus.Infos[cdatabackup.BackupPodPhase]).To(Equal(string(corev1.PodFailed)))
		})

		It("should keep the terminal phase from persisted evidence when the pod is deleted after terminal state was observed", func() {
			startTime := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
			finishTime := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
			statusWithEvidence := v1alpha1.OperationStatus{
				Phase:    common.PhaseComplete,
				Duration: "30s",
				Infos: map[string]string{
					cdatabackup.BackupPodName:       "test-pod",
					cdatabackup.BackupPodUID:        "pod-uid-1",
					cdatabackup.BackupPodNodeName:   "node-1",
					cdatabackup.BackupPodPhase:      string(corev1.PodSucceeded),
					cdatabackup.BackupPodStartTime:  startTime,
					cdatabackup.BackupPodFinishTime: finishTime,
				},
				Conditions: []v1alpha1.Condition{
					{Type: common.Complete, Status: corev1.ConditionTrue},
				},
			}
			// no pod exists in the client any more
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &statusWithEvidence)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
			Expect(opStatus.Conditions).To(HaveLen(1))
			Expect(opStatus.Conditions[0].Type).To(Equal(common.Complete))
			Expect(opStatus.Infos[cdatabackup.BackupPodNodeName]).To(Equal("node-1"))
		})

		It("should reconstruct a drifted terminal phase from persisted evidence when the pod is gone", func() {
			startTime := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
			finishTime := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
			driftedStatus := v1alpha1.OperationStatus{
				Phase:    common.PhaseExecuting,
				Duration: "Unfinished",
				Infos: map[string]string{
					cdatabackup.BackupPodName:       "test-pod",
					cdatabackup.BackupPodUID:        "pod-uid-1",
					cdatabackup.BackupPodNodeName:   "node-1",
					cdatabackup.BackupPodPhase:      string(corev1.PodSucceeded),
					cdatabackup.BackupPodStartTime:  startTime,
					cdatabackup.BackupPodFinishTime: finishTime,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &driftedStatus)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
			Expect(opStatus.Conditions).To(HaveLen(1))
			Expect(opStatus.Conditions[0].Type).To(Equal(common.Complete))
			Expect(opStatus.Conditions[0].Reason).To(Equal("BackupSuccessful"))
			Expect(opStatus.Duration).NotTo(Equal("Unfinished"))
		})

		It("should keep waiting without status writes while the pod is missing within the grace period", func() {
			statusWithMarker := v1alpha1.OperationStatus{
				Phase: common.PhaseExecuting,
				Infos: map[string]string{
					cdatabackup.BackupPodMissingSince: time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &statusWithMarker)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseExecuting))
			Expect(opStatus.Infos[cdatabackup.BackupPodMissingSince]).To(Equal(statusWithMarker.Infos[cdatabackup.BackupPodMissingSince]))
		})

		It("should mark Failed with BackupPodMissing after the grace period when the pod never reached an observed terminal state", func() {
			statusWithExpiredMarker := v1alpha1.OperationStatus{
				Phase: common.PhaseExecuting,
				Infos: map[string]string{
					cdatabackup.BackupPodMissingSince: time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339),
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			recorder := record.NewFakeRecorder(10)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: recorder,
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &statusWithExpiredMarker)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
			Expect(opStatus.Conditions).To(HaveLen(1))
			Expect(opStatus.Conditions[0].Type).To(Equal(common.Failed))
			Expect(opStatus.Conditions[0].Reason).To(Equal(cdatabackup.BackupPodMissingReason))
			Expect(opStatus.Conditions[0].Message).To(ContainSubstring("test-pod"))
			Expect(opStatus.Infos).NotTo(HaveKey(cdatabackup.BackupPodMissingSince))
			// a warning event must be emitted for diagnosability
			Eventually(recorder.Events).Should(Receive(ContainSubstring(cdatabackup.BackupPodMissingReason)))
		})

		It("should clear the missing marker and keep the phase when the pod reappears running", func() {
			runningPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					UID:       types.UID("pod-uid-1"),
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			}
			statusWithMarker := v1alpha1.OperationStatus{
				Phase: common.PhaseExecuting,
				Infos: map[string]string{
					cdatabackup.BackupPodMissingSince: time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &runningPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &statusWithMarker)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseExecuting))
			Expect(opStatus.Infos).NotTo(HaveKey(cdatabackup.BackupPodMissingSince))
		})

		It("should discard stale evidence and re-observe when a same-name pod is recreated with a new UID", func() {
			recreatedPod := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:              "test-pod",
					Namespace:         "default",
					UID:               types.UID("pod-uid-new"),
					CreationTimestamp: v1.NewTime(time.Now().Add(-10 * time.Second)),
				},
				Spec: corev1.PodSpec{
					NodeName: "node-3",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
					Conditions: []corev1.PodCondition{
						{LastTransitionTime: v1.Now()},
					},
				},
			}
			statusWithStaleEvidence := v1alpha1.OperationStatus{
				Phase: common.PhaseComplete,
				Infos: map[string]string{
					cdatabackup.BackupPodName:       "test-pod",
					cdatabackup.BackupPodUID:        "pod-uid-old",
					cdatabackup.BackupPodNodeName:   "node-old",
					cdatabackup.BackupPodPhase:      string(corev1.PodFailed),
					cdatabackup.BackupPodStartTime:  time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
					cdatabackup.BackupPodFinishTime: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &recreatedPod)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &statusWithStaleEvidence)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
			Expect(opStatus.Infos[cdatabackup.BackupPodUID]).To(Equal("pod-uid-new"))
			Expect(opStatus.Infos[cdatabackup.BackupPodNodeName]).To(Equal("node-3"))
			Expect(opStatus.Infos[cdatabackup.BackupPodPhase]).To(Equal(string(corev1.PodSucceeded)))
		})

		It("should keep an already terminal phase when the pod is missing without structured evidence", func() {
			terminalStatus := v1alpha1.OperationStatus{
				Phase: common.PhaseFailed,
				Conditions: []v1alpha1.Condition{
					{Type: common.Failed, Status: corev1.ConditionTrue, Reason: cdatabackup.BackupPodMissingReason},
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				Client:   client,
				Log:      fake.NullLogger(),
				Recorder: record.NewFakeRecorder(10),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &terminalStatus)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseFailed))
			Expect(opStatus.Conditions[0].Reason).To(Equal(cdatabackup.BackupPodMissingReason))
		})

		It("should use conditions LastTransitionTime when conditions are set on succeeded pod", func() {
			conditionTime := v1.Now()
			mockPodWithConditions := corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
					Conditions: []corev1.PodCondition{
						{
							LastTransitionTime: conditionTime,
						},
					},
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, mockDataBackup, &mockPodWithConditions)
			onceStatusHandler := &OnceHandler{dataBackup: mockDataBackup}
			ctx := cruntime.ReconcileRequestContext{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "",
				},
				Client: client,
				Log:    fake.NullLogger(),
			}

			opStatus, err := onceStatusHandler.GetOperationStatus(ctx, &mockDataBackup.Status)
			Expect(err).NotTo(HaveOccurred())
			Expect(opStatus.Phase).To(Equal(common.PhaseComplete))
			Expect(opStatus.Conditions).To(HaveLen(1))
			Expect(opStatus.Conditions[0].LastTransitionTime.Time).To(BeTemporally("~", conditionTime.Time, time.Second))
		})
	})
})
