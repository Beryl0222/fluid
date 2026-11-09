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

package engine

import (
	"errors"
	"fmt"
	"time"

	workloadv1alpha1 "github.com/fluid-cloudnative/advanced-statefulset/api/workload/v1alpha1"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/cache/component"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testArchRuntimeName = "my-runtime"
	testArchNamespace   = "default"
	testWorkerContainer = "worker-main"
)

func buildWorkersOnlyClass() *datav1alpha1.CacheRuntimeClass {
	return &datav1alpha1.CacheRuntimeClass{
		Topology: &datav1alpha1.RuntimeTopology{
			Worker: &datav1alpha1.RuntimeComponentDefinition{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: testWorkerContainer}},
					},
				},
			},
		},
	}
}

func buildWorkerASTS(runtimeName, namespace string, replicas int32) *workloadv1alpha1.AdvancedStatefulSet {
	workerName := common.GetCacheComponentName(runtimeName, common.ComponentTypeWorker)
	return &workloadv1alpha1.AdvancedStatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerName,
			Namespace: namespace,
		},
		Spec: workloadv1alpha1.AdvancedStatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: component.GetComponentMatchLabels(runtimeName, workerName),
			},
		},
	}
}

func buildWorkerPod(runtimeName, namespace, containerName string, ordinal int,
	ready bool, phase corev1.PodPhase, deleting bool) *corev1.Pod {
	workerName := common.GetCacheComponentName(runtimeName, common.ComponentTypeWorker)
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", workerName, ordinal),
			Namespace: namespace,
			Labels:    component.GetComponentMatchLabels(runtimeName, workerName),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: containerName}},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: readyStatus,
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: containerName, Ready: ready},
			},
		},
	}

	if deleting {
		// The fake client keeps terminating pods visible, mimicking the real API where a
		// deleting StatefulSet pod still shows up until graceful termination finishes.
		// A finalizer is required because the fake client rejects deleting objects without one.
		deletionTimestamp := metav1.NewTime(time.Now())
		pod.DeletionTimestamp = &deletionTimestamp
		pod.Finalizers = []string{"test.fluid.io/terminating"}
	}

	return pod
}

var _ = Describe("workersOnlyArchApi Tests", Label("pkg.ddc.cache.engine.arch_only_workers_test.go"), func() {
	workerName := common.GetCacheComponentName(testArchRuntimeName, common.ComponentTypeWorker)

	It("GetExecutionPodInfo should keep the worker-0 default path for a single ready replica", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 1)
		pod0 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, true, corev1.PodRunning, false)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts, pod0),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		podName, containerName, err := api.GetExecutionPodInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(podName).To(Equal(workerName + "-0"))
		Expect(containerName).To(Equal(testWorkerContainer))
	})

	It("GetExecutionPodInfo should deterministically pick the lowest ordinal ready worker", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 3)
		pod0 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, true, corev1.PodRunning, false)
		pod1 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 1, true, corev1.PodRunning, false)
		pod2 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 2, true, corev1.PodRunning, false)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts, pod0, pod1, pod2),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		first, _, err := api.GetExecutionPodInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(first).To(Equal(workerName + "-0"))

		// The selection is a pure function of the observed state: repeated resolutions
		// within the same state return the same stable identity.
		second, _, err := api.GetExecutionPodInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(Equal(first))
	})

	It("GetExecutionPodInfo should fail over to a healthy worker when worker-0 is not ready", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 2)
		// worker-0 is still starting up (not ready) while worker-1 is already healthy.
		pod0 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, false, corev1.PodPending, false)
		pod1 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 1, true, corev1.PodRunning, false)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts, pod0, pod1),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		podName, _, err := api.GetExecutionPodInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(podName).To(Equal(workerName + "-1"))
	})

	It("GetExecutionPodInfo should skip a terminating worker during rolling replacement or scale down", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 2)
		pod0 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, true, corev1.PodRunning, true)
		pod1 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 1, true, corev1.PodRunning, false)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts, pod0, pod1),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		podName, _, err := api.GetExecutionPodInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(podName).To(Equal(workerName + "-1"))
	})

	It("GetExecutionPodInfo should return the unavailable sentinel when all workers are not ready", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 2)
		pod0 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, false, corev1.PodPending, false)
		pod1 := buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 1, false, corev1.PodRunning, false)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts, pod0, pod1),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		podName, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
		Expect(podName).To(BeEmpty())
		Expect(errors.Is(err, ErrExecutionPodUnavailable)).To(BeTrue())
		var unavailableErr *ExecutionPodUnavailableError
		Expect(errors.As(err, &unavailableErr)).To(BeTrue())
		Expect(unavailableErr.Reason()).To(Equal(ExecutionPodUnavailableReasonNoReadyPod))
	})

	It("GetExecutionPodInfo should return the zero-replica sentinel when the statefulset is scaled to zero", func() {
		sts := buildWorkerASTS(testArchRuntimeName, testArchNamespace, 0)
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme, sts),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		_, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, ErrExecutionPodUnavailable)).To(BeTrue())
		var unavailableErr *ExecutionPodUnavailableError
		Expect(errors.As(err, &unavailableErr)).To(BeTrue())
		Expect(unavailableErr.Reason()).To(Equal(ExecutionPodUnavailableReasonZeroReplicas))
	})

	It("GetExecutionPodInfo should return the workload-not-found sentinel before the statefulset is created", func() {
		api := &workersOnlyArchApi{
			client:       fake.NewFakeClientWithScheme(CacheEngineTestScheme),
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		_, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, ErrExecutionPodUnavailable)).To(BeTrue())
		var unavailableErr *ExecutionPodUnavailableError
		Expect(errors.As(err, &unavailableErr)).To(BeTrue())
		Expect(unavailableErr.Reason()).To(Equal(ExecutionPodUnavailableReasonWorkloadNotFound))
	})

	It("GetExecutionPodInfo should return the unavailable sentinel when the client is not initialized", func() {
		api := &workersOnlyArchApi{
			client:       nil,
			name:         testArchRuntimeName,
			namespace:    testArchNamespace,
			runtimeClass: buildWorkersOnlyClass(),
		}

		_, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, ErrExecutionPodUnavailable)).To(BeTrue())
	})

	It("GetExecutionPodInfo should return error when worker template has no containers", func() {
		api := &workersOnlyArchApi{
			client:    fake.NewFakeClientWithScheme(CacheEngineTestScheme),
			name:      testArchRuntimeName,
			namespace: testArchNamespace,
			runtimeClass: &datav1alpha1.CacheRuntimeClass{
				Topology: &datav1alpha1.RuntimeTopology{
					Worker: &datav1alpha1.RuntimeComponentDefinition{
						Template: corev1.PodTemplateSpec{},
					},
				},
			},
		}

		_, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no container in worker pod template"))
	})

	It("GetExecutionPodInfo should return error when topology is nil", func() {
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: &datav1alpha1.CacheRuntimeClass{}}
		_, _, err := api.GetExecutionPodInfo()
		Expect(err).To(HaveOccurred())
	})

	It("GetExecutionEntries should return worker execution entries", func() {
		entries := &datav1alpha1.ExecutionEntries{
			MountUFS: &datav1alpha1.ExecutionCommonEntry{Command: []string{"/mount.sh"}},
		}
		runtimeClass := &datav1alpha1.CacheRuntimeClass{
			Topology: &datav1alpha1.RuntimeTopology{
				Worker: &datav1alpha1.RuntimeComponentDefinition{ExecutionEntries: entries},
			},
		}
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: runtimeClass}

		got := api.GetExecutionEntries()
		Expect(got).To(Equal(entries))
	})

	It("GetExecutionEntries should return nil when topology is nil", func() {
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: &datav1alpha1.CacheRuntimeClass{}}
		Expect(api.GetExecutionEntries()).To(BeNil())
	})

	It("GetExecutionEntries should return nil when worker topology is nil", func() {
		runtimeClass := &datav1alpha1.CacheRuntimeClass{
			Topology: &datav1alpha1.RuntimeTopology{},
		}
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: runtimeClass}
		Expect(api.GetExecutionEntries()).To(BeNil())
	})

	It("IsMountUFSSupported should always return false", func() {
		runtimeClass := &datav1alpha1.CacheRuntimeClass{
			Topology: &datav1alpha1.RuntimeTopology{
				Worker: &datav1alpha1.RuntimeComponentDefinition{
					ExecutionEntries: &datav1alpha1.ExecutionEntries{
						MountUFS: &datav1alpha1.ExecutionCommonEntry{Command: []string{"/mount.sh"}},
					},
				},
			},
		}
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: runtimeClass}
		Expect(api.IsMountUFSSupported()).To(BeFalse())
	})

	It("IsMountUFSSupported should return false when runtimeClass is nil", func() {
		api := &workersOnlyArchApi{name: testArchRuntimeName, namespace: testArchNamespace, runtimeClass: nil}
		Expect(api.IsMountUFSSupported()).To(BeFalse())
	})
})

var _ = Describe("workersOnlyArchApi selection helpers", Label("pkg.ddc.cache.engine.arch_only_workers_test.go"), func() {
	workerName := common.GetCacheComponentName(testArchRuntimeName, common.ComponentTypeWorker)

	DescribeTable("isReadyExecutionPod",
		func(pod *corev1.Pod, expected bool) {
			Expect(isReadyExecutionPod(pod, testWorkerContainer)).To(Equal(expected))
		},
		Entry("running and ready", buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, true, corev1.PodRunning, false), true),
		Entry("running but not ready", buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, false, corev1.PodRunning, false), false),
		Entry("pending pod", buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, false, corev1.PodPending, false), false),
		Entry("terminating pod", buildWorkerPod(testArchRuntimeName, testArchNamespace, testWorkerContainer, 0, true, corev1.PodRunning, true), false),
		Entry("nil pod", nil, false),
	)

	DescribeTable("workerPodOrdinal",
		func(podName string, expectedOrdinal int, expectedOK bool) {
			ordinal, ok := workerPodOrdinal(podName, workerName)
			Expect(ok).To(Equal(expectedOK))
			if expectedOK {
				Expect(ordinal).To(Equal(expectedOrdinal))
			}
		},
		Entry("worker-0", workerName+"-0", 0, true),
		Entry("worker-12", workerName+"-12", 12, true),
		Entry("foreign pod", "other-runtime-worker-1", 0, false),
		Entry("malformed name", workerName, 0, false),
	)
})
