package utils

import (
	"time"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/webhook/cache"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	testNamespace      = "default"
	testPVCNamespaced  = "ds-pvc"
	testPVCUID         = types.UID("pvc-uid-1")
	testPVCResourceVer = "pvc-rv-1"

	testDatasetUID         = types.UID("dataset-uid-1")
	testDatasetResourceVer = "dataset-rv-1"

	testRuntimeUID         = types.UID("runtime-uid-1")
	testRuntimeResourceVer = "runtime-rv-1"
)

func newDatasetPVC(uid types.UID, resourceVersion string, isDataset bool) *corev1.PersistentVolumeClaim {
	labels := map[string]string{}
	if isDataset {
		labels[common.LabelAnnotationStorageCapacityPrefix+"default-ds-pvc"] = common.True
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testPVCNamespaced,
			Namespace:       testNamespace,
			UID:             uid,
			ResourceVersion: resourceVersion,
			Labels:          labels,
		},
	}
}

func newBoundDataset(uid types.UID, resourceVersion string) *v1alpha1.Dataset {
	return &v1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testPVCNamespaced,
			Namespace:       testNamespace,
			UID:             uid,
			ResourceVersion: resourceVersion,
		},
		Status: v1alpha1.DatasetStatus{
			Phase: v1alpha1.BoundDatasetPhase,
			Runtimes: []v1alpha1.Runtime{
				{
					Name:      testPVCNamespaced,
					Namespace: testNamespace,
					Type:      common.AlluxioRuntime,
				},
			},
		},
	}
}

func newAlluxioRuntime(uid types.UID, resourceVersion string) *v1alpha1.AlluxioRuntime {
	return &v1alpha1.AlluxioRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testPVCNamespaced,
			Namespace:       testNamespace,
			UID:             uid,
			ResourceVersion: resourceVersion,
		},
	}
}

func newCacheEnabledFakeClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(setupScheme()).
		WithObjects(objs...).
		Build()
}

func matchingDatasetIdentity() cache.SourceObjectIdentity {
	return cache.SourceObjectIdentity{
		PVCUID:                 testPVCUID,
		PVCResourceVersion:     testPVCResourceVer,
		DatasetUID:             testDatasetUID,
		DatasetResourceVersion: testDatasetResourceVer,
		RuntimeType:            common.AlluxioRuntime,
		RuntimeName:            testPVCNamespaced,
		RuntimeNamespace:       testNamespace,
		RuntimeUID:             testRuntimeUID,
		RuntimeResourceVersion: testRuntimeResourceVer,
	}
}

func seedCachedEntry(pvcName string, isDataset bool, identity cache.SourceObjectIdentity) base.RuntimeInfoInterface {
	runtimeInfo, err := base.BuildRuntimeInfo(testPVCNamespaced, testNamespace, common.AlluxioRuntime)
	Expect(err).NotTo(HaveOccurred())
	cache.SetRuntimeInfoByKey(types.NamespacedName{Name: pvcName, Namespace: testNamespace},
		cache.NewPersistentVolumeClaimInfoCache(runtimeInfo, isDataset, identity))
	return runtimeInfo
}

var _ = Describe("CollectRuntimeInfosFromPVCs with runtime info cache enabled", func() {
	var (
		setupLog = zap.New(zap.UseDevMode(true))
		pvc      *corev1.PersistentVolumeClaim
		dataset  *v1alpha1.Dataset
		runtime  *v1alpha1.AlluxioRuntime
	)

	BeforeEach(func() {
		cache.EnableRuntimeInfoCacheForTest(64, time.Minute)
		pvc = newDatasetPVC(testPVCUID, testPVCResourceVer, true)
		dataset = newBoundDataset(testDatasetUID, testDatasetResourceVer)
		runtime = newAlluxioRuntime(testRuntimeUID, testRuntimeResourceVer)
	})

	AfterEach(func() {
		cache.DisableRuntimeInfoCacheForTest()
	})

	Context("cache miss and validated hit", func() {
		It("builds and caches an entry on miss and reuses the exact RuntimeInfo on a validated hit", func() {
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)

			first, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(first).To(HaveLen(1))
			Expect(first[testPVCNamespaced]).NotTo(BeNil())

			cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: testPVCNamespaced, Namespace: testNamespace})
			Expect(found).To(BeTrue())
			Expect(cached.IsBelongToDataset()).To(BeTrue())
			Expect(cached.GetIdentity().PVCUID).To(Equal(testPVCUID))
			Expect(cached.GetIdentity().DatasetUID).To(Equal(testDatasetUID))
			Expect(cached.GetIdentity().RuntimeUID).To(Equal(testRuntimeUID))

			// Second admission: identities still match, so the cached RuntimeInfo is reused.
			second, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(second[testPVCNamespaced]).To(BeIdenticalTo(first[testPVCNamespaced]))
		})

		It("reuses a pre-seeded entry whose identities all match the current objects", func() {
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			cachedInfo := seedCachedEntry(testPVCNamespaced, true, matchingDatasetIdentity())

			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(runtimeInfos[testPVCNamespaced]).To(BeIdenticalTo(cachedInfo))
		})

		It("caches a non-dataset PVC without RuntimeInfo and keeps returning an empty map", func() {
			regularPVC := newDatasetPVC(testPVCUID, testPVCResourceVer, false)
			fakeClient := newCacheEnabledFakeClient(regularPVC)

			first, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(first).To(HaveLen(0))

			cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: testPVCNamespaced, Namespace: testNamespace})
			Expect(found).To(BeTrue())
			Expect(cached.IsBelongToDataset()).To(BeFalse())

			second, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(second).To(HaveLen(0))
		})
	})

	Context("stale PVC identity", func() {
		It("detects PVC resourceVersion changes and invalidates the entry", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.PVCResourceVersion = "stale-pvc-rv"
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(runtimeInfos).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("PVC resourceVersion changed"))

			_, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: testPVCNamespaced, Namespace: testNamespace})
			Expect(found).To(BeFalse())
		})

		It("detects PVC UID changes after delete-and-recreate with the same name", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.PVCUID = types.UID("deleted-pvc-uid")
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("PVC UID changed"))
		})

		It("detects a dataset PVC turning into a regular PVC", func() {
			// Same PVC UID and resourceVersion, only the dataset labels disappear: the
			// dataset membership mismatch itself must invalidate the entry.
			regularPVC := newDatasetPVC(testPVCUID, testPVCResourceVer, false)
			fakeClient := newCacheEnabledFakeClient(regularPVC)
			seedCachedEntry(testPVCNamespaced, true, matchingDatasetIdentity())

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("dataset membership changed"))
		})

		It("detects a regular PVC turning into a dataset PVC", func() {
			fakeClient := newCacheEnabledFakeClient(pvc)
			regularIdentity := cache.SourceObjectIdentity{
				PVCUID:             testPVCUID,
				PVCResourceVersion: testPVCResourceVer,
			}
			seedCachedEntry(testPVCNamespaced, false, regularIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("dataset membership changed"))
		})
	})

	Context("stale Dataset identity", func() {
		It("detects Dataset resourceVersion changes (e.g. runtime rebinding)", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.DatasetResourceVersion = "stale-dataset-rv"
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("Dataset"))
			Expect(err.Error()).To(ContainSubstring("resourceVersion changed"))
		})

		It("detects Dataset UID changes after recreation", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.DatasetUID = types.UID("deleted-dataset-uid")
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("Dataset UID changed"))
		})
	})

	Context("stale Runtime identity", func() {
		It("detects Runtime resourceVersion changes (e.g. FUSE node selector updates)", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.RuntimeResourceVersion = "stale-runtime-rv"
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("Runtime"))
			Expect(err.Error()).To(ContainSubstring("resourceVersion changed"))
		})

		It("detects Runtime UID changes after recreation", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.RuntimeUID = types.UID("deleted-runtime-uid")
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("Runtime UID changed"))
		})

		It("returns a non-stale error when the Runtime cannot be read so the caller retries with the API reader", func() {
			fakeClient := newCacheEnabledFakeClient(pvc, dataset)
			seedCachedEntry(testPVCNamespaced, true, matchingDatasetIdentity())

			_, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeFalse())
			Expect(err.Error()).To(ContainSubstring("failed to get current Runtime"))
		})
	})

	Context("all-or-nothing snapshot across multiple PVCs", func() {
		It("aborts the whole collection without a partial map when one of multiple PVCs is stale", func() {
			regularPVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "regular-pvc",
					Namespace:       testNamespace,
					UID:             types.UID("regular-pvc-uid"),
					ResourceVersion: "regular-pvc-rv",
				},
			}
			fakeClient := newCacheEnabledFakeClient(regularPVC, pvc, dataset, runtime)

			// A valid entry for the regular PVC ...
			cache.SetRuntimeInfoByKey(types.NamespacedName{Name: "regular-pvc", Namespace: testNamespace},
				cache.NewPersistentVolumeClaimInfoCache(nil, false, cache.SourceObjectIdentity{
					PVCUID:             types.UID("regular-pvc-uid"),
					PVCResourceVersion: "regular-pvc-rv",
				}))
			// ... and a stale entry for the dataset PVC.
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.PVCResourceVersion = "stale-pvc-rv"
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient,
				[]string{"regular-pvc", testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
			// No partially refreshed snapshot may escape the function.
			Expect(runtimeInfos).To(BeNil())
		})

		It("returns an error when a PVC cannot be read instead of mixing readers", func() {
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			_, err := CollectRuntimeInfosFromPVCs(fakeClient,
				[]string{testPVCNamespaced, "missing-pvc"}, testNamespace, setupLog, false, false)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get current PVC"))
		})
	})

	Context("forced refresh with the API reader", func() {
		It("bypasses a stale cache entry, rebuilds from current objects and overwrites the cache", func() {
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.PVCResourceVersion = "stale-pvc-rv"
			fakeClient := newCacheEnabledFakeClient(pvc, dataset, runtime)
			staleInfo := seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(runtimeInfos).To(HaveLen(1))
			Expect(runtimeInfos[testPVCNamespaced]).NotTo(BeIdenticalTo(staleInfo))

			cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: testPVCNamespaced, Namespace: testNamespace})
			Expect(found).To(BeTrue())
			Expect(cached.GetIdentity().PVCResourceVersion).To(Equal(testPVCResourceVer))
			Expect(cached.GetIdentity().DatasetResourceVersion).To(Equal(testDatasetResourceVer))
			Expect(cached.GetIdentity().RuntimeResourceVersion).To(Equal(testRuntimeResourceVer))
		})
	})

	Context("cold cache and TTL expiry", func() {
		It("builds one consistent snapshot for multiple PVCs when the cache is empty (e.g. after a webhook restart)", func() {
			regularPVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "regular-pvc",
					Namespace:       testNamespace,
					UID:             types.UID("regular-pvc-uid"),
					ResourceVersion: "regular-pvc-rv",
				},
			}
			fakeClient := newCacheEnabledFakeClient(regularPVC, pvc, dataset, runtime)

			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient,
				[]string{testPVCNamespaced, "regular-pvc"}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			// Only the dataset PVC contributes RuntimeInfo; the regular PVC is recorded as
			// non-dataset and both entries come from the same reader view.
			Expect(runtimeInfos).To(HaveLen(1))
			Expect(runtimeInfos[testPVCNamespaced]).NotTo(BeNil())

			for _, name := range []string{testPVCNamespaced, "regular-pvc"} {
				cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: name, Namespace: testNamespace})
				Expect(found).To(BeTrue())
				if name == testPVCNamespaced {
					Expect(cached.IsBelongToDataset()).To(BeTrue())
				} else {
					Expect(cached.IsBelongToDataset()).To(BeFalse())
				}
			}
		})

		It("rebuilds from current objects instead of reusing stale data after the TTL expires", func() {
			// Enable the cache with a very short TTL for this spec.
			cache.EnableRuntimeInfoCacheForTest(64, 50*time.Millisecond)

			updatedPVC := newDatasetPVC(testPVCUID, "pvc-rv-2", true)
			fakeClient := newCacheEnabledFakeClient(updatedPVC, dataset, runtime)

			// Seed an expired entry whose identities no longer match the current PVC.
			staleIdentity := matchingDatasetIdentity()
			staleIdentity.PVCResourceVersion = "stale-pvc-rv"
			seedCachedEntry(testPVCNamespaced, true, staleIdentity)

			time.Sleep(80 * time.Millisecond)

			// The expired entry must be treated as a miss and rebuilt from current objects,
			// rather than failing validation against its stale identities.
			runtimeInfos, err := CollectRuntimeInfosFromPVCs(fakeClient, []string{testPVCNamespaced}, testNamespace, setupLog, false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(runtimeInfos).To(HaveLen(1))

			cached, found := cache.GetRuntimeInfoByKey(types.NamespacedName{Name: testPVCNamespaced, Namespace: testNamespace})
			Expect(found).To(BeTrue())
			Expect(cached.GetIdentity().PVCResourceVersion).To(Equal("pvc-rv-2"))
		})
	})

	Context("skipPrecheck semantics", func() {
		It("only skips the readiness precheck while identity validation stays active", func() {
			notBoundDataset := newBoundDataset(testDatasetUID, testDatasetResourceVer)
			notBoundDataset.Status.Phase = v1alpha1.NotBoundDatasetPhase
			firstClient := newCacheEnabledFakeClient(pvc, notBoundDataset, runtime)

			// skipPrecheck=true: the not-bound dataset does not block building the entry.
			runtimeInfos, err := CollectRuntimeInfosFromPVCs(firstClient, []string{testPVCNamespaced}, testNamespace, setupLog, true, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(runtimeInfos).To(HaveLen(1))

			// Another reader observes the updated PVC: identity validation must still run
			// regardless of skipPrecheck and detect the stale entry.
			updatedPVC := newDatasetPVC(testPVCUID, "pvc-rv-2", true)
			updatedPVC.Labels["extra-label"] = "changed"
			secondClient := newCacheEnabledFakeClient(updatedPVC, notBoundDataset, runtime)

			_, err = CollectRuntimeInfosFromPVCs(secondClient, []string{testPVCNamespaced}, testNamespace, setupLog, true, false)
			Expect(err).To(HaveOccurred())
			Expect(IsStaleRuntimeInfoCacheError(err)).To(BeTrue())
		})
	})
})
