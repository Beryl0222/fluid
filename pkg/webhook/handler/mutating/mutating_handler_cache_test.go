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

package mutating

import (
	"context"
	"os"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	"github.com/fluid-cloudnative/fluid/pkg/webhook/cache"
	"github.com/fluid-cloudnative/fluid/pkg/webhook/plugins"
	webhookutils "github.com/fluid-cloudnative/fluid/pkg/webhook/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const cachedPVCNamespacedName = "cached-ds-pvc"

func cacheConsistencyScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	Expect(corev1.AddToScheme(s)).To(Succeed())
	Expect(datav1alpha1.AddToScheme(s)).To(Succeed())
	return s
}

func cacheConsistencyObjects() []runtime.Object {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cachedPVCNamespacedName,
			Namespace:       "default",
			UID:             types.UID("pvc-uid-1"),
			ResourceVersion: "pvc-rv-2",
			Labels: map[string]string{
				common.LabelAnnotationStorageCapacityPrefix + "default-cached-ds-pvc": common.True,
			},
		},
	}
	dataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cachedPVCNamespacedName,
			Namespace:       "default",
			UID:             types.UID("dataset-uid-1"),
			ResourceVersion: "dataset-rv-1",
		},
		Status: datav1alpha1.DatasetStatus{
			Phase: datav1alpha1.BoundDatasetPhase,
			Runtimes: []datav1alpha1.Runtime{
				{
					Name:      cachedPVCNamespacedName,
					Namespace: "default",
					Type:      common.AlluxioRuntime,
				},
			},
		},
	}
	alluxioRuntime := &datav1alpha1.AlluxioRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cachedPVCNamespacedName,
			Namespace:       "default",
			UID:             types.UID("runtime-uid-1"),
			ResourceVersion: "runtime-rv-1",
		},
	}
	return []runtime.Object{pvc, dataset, alluxioRuntime}
}

func seedStaleRuntimeInfoCacheEntry() {
	runtimeInfo, err := base.BuildRuntimeInfo(cachedPVCNamespacedName, "default", common.AlluxioRuntime)
	Expect(err).NotTo(HaveOccurred())
	cache.SetRuntimeInfoByKey(
		types.NamespacedName{Name: cachedPVCNamespacedName, Namespace: "default"},
		cache.NewPersistentVolumeClaimInfoCache(runtimeInfo, true, cache.SourceObjectIdentity{
			PVCUID:                  types.UID("pvc-uid-1"),
			PVCResourceVersion:      "stale-pvc-rv",
			DatasetUID:              types.UID("dataset-uid-1"),
			DatasetResourceVersion:  "dataset-rv-1",
			RuntimeType:             common.AlluxioRuntime,
			RuntimeName:             cachedPVCNamespacedName,
			RuntimeNamespace:        "default",
			RuntimeUID:              types.UID("runtime-uid-1"),
			RuntimeResourceVersion:  "runtime-rv-1",
		}))
}

func podMountingCachedPVC() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "test",
					VolumeMounts: []corev1.VolumeMount{
						{Name: "data", MountPath: "/data"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: cachedPVCNamespacedName,
						},
					},
				},
			},
		},
	}
}

func cacheConsistencyRequest() admission.Request {
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: "default",
			Object: runtime.RawExtension{
				Raw: []byte(`{
					"apiVersion": "v1",
					"kind": "Pod",
					"metadata": {
						"name": "cache-test-pod",
						"namespace": "default"
					},
					"spec": {
						"containers": [
							{
								"name": "app",
								"image": "test",
								"volumeMounts": [
									{
										"name": "data",
										"mountPath": "/data"
									}
								]
							}
						],
						"volumes": [
							{
								"name": "data",
								"persistentVolumeClaim": {
									"claimName": "cached-ds-pvc"
								}
							}
						]
					}
				}`),
			},
		},
	}
}

var _ = Describe("Mutating Handler - RuntimeInfo Cache Consistency", func() {
	var (
		s     *runtime.Scheme

		patch *gomonkey.Patches
	)

	BeforeEach(func() {
		s = cacheConsistencyScheme()
		cache.EnableRuntimeInfoCacheForTest(64, time.Minute)

		mockReadFile := func(content string) ([]byte, error) {
			return []byte(pluginsProfile), nil
		}
		patch = gomonkey.ApplyFunc(os.ReadFile, mockReadFile)
	})

	AfterEach(func() {
		patch.Reset()
		cache.DisableRuntimeInfoCacheForTest()
	})

	It("returns a retryable stale error when the cached client hits a stale cache entry", func() {
		cachedClient := fake.NewFakeClientWithScheme(s, cacheConsistencyObjects()...)
		Expect(plugins.RegisterMutatingHandlers(cachedClient)).To(Succeed())
		seedStaleRuntimeInfoCacheEntry()

		handler := &FluidMutatingHandler{Client: cachedClient}
		err := handler.MutatePod(podMountingCachedPVC(), false)
		Expect(err).To(HaveOccurred())
		Expect(webhookutils.IsNeedRetryWithApiReaderError(err)).To(BeTrue())
		Expect(webhookutils.IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("PVC resourceVersion changed"))
	})

	It("redoes the whole mutation from the original pod with the API reader and heals the cache", func() {
		cachedClient := fake.NewFakeClientWithScheme(s, cacheConsistencyObjects()...)
		apiReader := fake.NewFakeClientWithScheme(s, cacheConsistencyObjects()...)
		Expect(plugins.RegisterMutatingHandlers(cachedClient)).To(Succeed())
		seedStaleRuntimeInfoCacheEntry()

		handler := &FluidMutatingHandler{}
		handler.Setup(cachedClient, apiReader, admission.NewDecoder(clientgoscheme.Scheme))

		resp := handler.Handle(context.TODO(), cacheConsistencyRequest())
		Expect(resp.AdmissionResponse.Allowed).To(BeTrue())

		// The retried mutation bypassed the stale entry and healed it with identities
		// observed from the authoritative API reader.
		cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{
			Name:      cachedPVCNamespacedName,
			Namespace: "default",
		})
		Expect(found).To(BeTrue())
		Expect(cached.IsBelongToDataset()).To(BeTrue())
		Expect(cached.GetIdentity().PVCResourceVersion).To(Equal("pvc-rv-2"))
		Expect(cached.GetIdentity().DatasetResourceVersion).To(Equal("dataset-rv-1"))
		Expect(cached.GetIdentity().RuntimeResourceVersion).To(Equal("runtime-rv-1"))
	})

	It("rejects the admission with a clear error when the API reader redo also fails", func() {
		cachedClient := fake.NewFakeClientWithScheme(s, cacheConsistencyObjects()...)
		// The direct API reader sees none of the objects, so the authoritative redo cannot
		// build any consistent snapshot.
		apiReader := fake.NewFakeClientWithScheme(s)
		Expect(plugins.RegisterMutatingHandlers(cachedClient)).To(Succeed())
		seedStaleRuntimeInfoCacheEntry()

		handler := &FluidMutatingHandler{}
		handler.Setup(cachedClient, apiReader, admission.NewDecoder(clientgoscheme.Scheme))

		resp := handler.Handle(context.TODO(), cacheConsistencyRequest())
		Expect(resp.AdmissionResponse.Allowed).To(BeFalse())
		Expect(resp.Result).NotTo(BeNil())
		Expect(resp.Result.Message).To(ContainSubstring("persistentvolumeclaims"))
	})

	It("allows the pod when the cached entry passes identity validation without source changes", func() {
		cachedClient := fake.NewFakeClientWithScheme(s, cacheConsistencyObjects()...)
		Expect(plugins.RegisterMutatingHandlers(cachedClient)).To(Succeed())

		// First admission populates the cache from current objects.
		firstHandler := &FluidMutatingHandler{}
		firstHandler.Setup(cachedClient, cachedClient, admission.NewDecoder(clientgoscheme.Scheme))
		firstResp := firstHandler.Handle(context.TODO(), cacheConsistencyRequest())
		Expect(firstResp.AdmissionResponse.Allowed).To(BeTrue())

		// Second admission reuses the validated cache entry successfully.
		secondHandler := &FluidMutatingHandler{}
		secondHandler.Setup(cachedClient, cachedClient, admission.NewDecoder(clientgoscheme.Scheme))
		secondResp := secondHandler.Handle(context.TODO(), cacheConsistencyRequest())
		Expect(secondResp.AdmissionResponse.Allowed).To(BeTrue())
	})
})
