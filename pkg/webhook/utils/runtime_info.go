package utils

import (
	"fmt"
	"time"

	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	"github.com/fluid-cloudnative/fluid/pkg/webhook/cache"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CollectRuntimeInfosFromPVCs resolves every given PVC to the RuntimeInfo that later
// webhook plugins consume and returns one all-or-nothing result for a single pod
// admission.
//
// When the runtime info cache is disabled (the default), the function keeps its original
// behavior and reads/builds everything from the passed reader.
//
// When the cache is enabled:
//   - A cached entry is reused only after its recorded PVC/Dataset/Runtime identities are
//     validated against the current objects. Any update, deletion, recreation or type
//     change is detected as a stale entry and aborts the whole collection with a
//     StaleRuntimeInfoCacheError, so a single admission can never mix stale and fresh
//     entries (this also covers multi-PVC pods, TTL expiry and webhook restarts, where
//     some entries may be missing while others are still cached).
//   - On cache miss, entries are built from the current objects and cached together with
//     their source object identities.
//   - When forceRefresh is true (i.e. the mutation is redone with the direct API reader),
//     the global cache is bypassed entirely, every entry is rebuilt from the authoritative
//     API state and the cache is healed with consistent entries.
//
// Read errors from the passed reader are returned as errors so that callers can trigger
// the retry-with-API-reader flow; if the retry also fails, the admission is rejected.
func CollectRuntimeInfosFromPVCs(reader client.Reader, pvcNames []string, namespace string,
	setupLog logr.Logger, skipPrecheck bool, forceRefresh bool) (runtimeInfos map[string]base.RuntimeInfoInterface, err error) {
	if utils.IsTimeTrackerDebugEnabled() {
		defer utils.TimeTrack(time.Now(), "CreateUpdatePodForSchedulingHandler.checkIfDatasetPVCs",
			"pvc.names", pvcNames, "pvc.namespace", namespace)
	}

	runtimeInfos = map[string]base.RuntimeInfoInterface{}
	if len(pvcNames) == 0 {
		return
	}

	// When the cache is disabled, preserve the original behavior exactly: no identity
	// validation and no extra PVC reads beyond the original per-PVC lookup.
	if !cache.IsRuntimeInfoCacheEnabled() {
		return collectRuntimeInfosWithoutCache(reader, pvcNames, namespace, setupLog, skipPrecheck)
	}

	// Cache enabled: resolve all PVCs into one consistent snapshot. The loop returns an
	// error on the first stale entry or read failure instead of returning a partially
	// refreshed map, so the caller can redo the whole mutation from the original pod.
	for _, pvcName := range pvcNames {
		key := types.NamespacedName{
			Name:      pvcName,
			Namespace: namespace,
		}

		// Always read the current PVC first so that a cached entry can only be reused
		// after being validated against the current PVC identity.
		pvc, pvcErr := kubeclient.GetPersistentVolumeClaim(reader, pvcName, namespace)
		if pvcErr != nil {
			return nil, errors.Wrapf(pvcErr,
				"failed to get current PVC %q to validate the runtime info cache", key.String())
		}

		if !forceRefresh {
			if cachedEntry, found := cache.GetRuntimeInfoByKey(key); found {
				stale, reason, validateErr := validateCachedRuntimeInfo(reader, cachedEntry, pvc)
				if validateErr != nil {
					return nil, errors.Wrapf(validateErr,
						"failed to validate the runtime info cache entry of PVC %q", key.String())
				}
				if !stale {
					if cachedEntry.IsBelongToDataset() {
						runtimeInfos[pvcName] = cachedEntry.GetRuntimeInfo()
					}
					continue
				}

				// The entry is stale: invalidate it and abort the whole collection so that
				// the admission is redone from the original pod with the API reader.
				setupLog.Info("stale runtime info cache entry detected, invalidating it",
					"pvc", key.String(), "reason", reason)
				cache.RemoveRuntimeInfoByKey(key)
				return nil, &StaleRuntimeInfoCacheError{Key: key, Reason: reason}
			}
		}

		// Cache miss or forced refresh: build the entry (and its source object identities)
		// from the current objects through the same reader, which keeps the whole snapshot
		// consistent with one reader view.
		entry, isDatasetPVC, buildErr := buildPVCInfoCacheEntry(reader, pvc, setupLog, skipPrecheck)
		if buildErr != nil {
			return nil, buildErr
		}
		cache.SetRuntimeInfoByKey(key, entry)
		if isDatasetPVC {
			runtimeInfos[pvcName] = entry.GetRuntimeInfo()
		}
	}

	return
}

// collectRuntimeInfosWithoutCache keeps the historical cache-disabled resolution path:
// per-PVC lookup, dataset check and runtime info build, aggregating unreadable PVCs.
func collectRuntimeInfosWithoutCache(reader client.Reader, pvcNames []string, namespace string,
	setupLog logr.Logger, skipPrecheck bool) (runtimeInfos map[string]base.RuntimeInfoInterface, err error) {
	errPVCs := []string{}
	runtimeInfos = map[string]base.RuntimeInfoInterface{}
	for _, pvcName := range pvcNames {
		var (
			isDatasetPVC bool
			runtimeInfo  base.RuntimeInfoInterface
		)
		pvc, pvcErr := kubeclient.GetPersistentVolumeClaim(reader, pvcName, namespace)
		if pvcErr != nil {
			setupLog.Error(pvcErr, "unable to check pvc, ignore and continue to check next pvc",
				"pvc",
				pvcName,
				"namespace",
				namespace)
			errPVCs = append(errPVCs, pvcName)
			continue
		}
		isDatasetPVC = kubeclient.CheckIfPVCIsDataset(pvc)
		if isDatasetPVC {
			runtimeInfo, err = buildRuntimeInfoInternalWithPrecheck(reader, pvc, setupLog, skipPrecheck)
			if err != nil {
				err = errors.Wrapf(err, "failed to build runtime info for PVC \"%v/%v\"", namespace, pvcName)
				return
			}
			runtimeInfos[pvcName] = runtimeInfo
		}
	}

	if len(errPVCs) > 0 {
		err = fmt.Errorf("failed to get the following PVCs %v", errPVCs)
		return
	}

	return
}

// validateCachedRuntimeInfo checks whether the cached entry still matches the current
// source objects visible through reader. It returns stale=true with a human-readable
// reason when an identity mismatch is detected. A non-nil error means the current state
// cannot be verified (e.g. a source object cannot be read), in which case the caller must
// retry with the API reader instead of trusting the entry.
func validateCachedRuntimeInfo(reader client.Reader, entry *cache.PersistentVolumeClaimInfoCache,
	pvc *corev1.PersistentVolumeClaim) (stale bool, reason string, err error) {
	identity := entry.GetIdentity()

	// 1. PVC identity: a UID change means delete-and-recreate with the same name, while a
	// ResourceVersion change means an update (including dataset label flips).
	if identity.PVCUID != pvc.UID {
		return true, fmt.Sprintf("PVC UID changed from %q to %q", identity.PVCUID, pvc.UID), nil
	}
	if identity.PVCResourceVersion != pvc.ResourceVersion {
		return true, fmt.Sprintf("PVC resourceVersion changed from %q to %q",
			identity.PVCResourceVersion, pvc.ResourceVersion), nil
	}

	// 2. The "belongs to Dataset" decision must stay consistent with the current PVC.
	currentIsDataset := kubeclient.CheckIfPVCIsDataset(pvc)
	if currentIsDataset != entry.IsBelongToDataset() {
		return true, fmt.Sprintf("PVC dataset membership changed from %v to %v",
			entry.IsBelongToDataset(), currentIsDataset), nil
	}

	// A non-dataset PVC has no further source objects to validate.
	if !currentIsDataset {
		return false, "", nil
	}

	// 3. Dataset identity: catches Dataset recreation (UID) as well as Dataset updates such
	// as rebinding another Runtime (ResourceVersion).
	datasetName := resolveDatasetName(pvc)
	dataset, err := utils.GetDataset(reader, datasetName, pvc.GetNamespace())
	if err != nil {
		return false, "", errors.Wrapf(err,
			"failed to get current Dataset %q/%q for runtime info cache validation",
			pvc.GetNamespace(), datasetName)
	}
	if identity.DatasetUID != dataset.UID {
		return true, fmt.Sprintf("Dataset UID changed from %q to %q", identity.DatasetUID, dataset.UID), nil
	}
	if identity.DatasetResourceVersion != dataset.ResourceVersion {
		return true, fmt.Sprintf("Dataset %q resourceVersion changed from %q to %q",
			datasetName, identity.DatasetResourceVersion, dataset.ResourceVersion), nil
	}

	// 4. Runtime identity: catches Runtime recreation (UID), Runtime spec updates such as
	// FUSE node selector changes (ResourceVersion) and type/name changes recorded in the
	// entry (a rebind is already covered by the Dataset ResourceVersion check above).
	runtimeObject, err := getRuntimeObject(reader, identity.RuntimeType, identity.RuntimeName, identity.RuntimeNamespace)
	if err != nil {
		return false, "", errors.Wrapf(err,
			"failed to get current Runtime %q/%q of type %q for runtime info cache validation",
			identity.RuntimeNamespace, identity.RuntimeName, identity.RuntimeType)
	}
	if identity.RuntimeUID != runtimeObject.GetUID() {
		return true, fmt.Sprintf("Runtime UID changed from %q to %q",
			identity.RuntimeUID, runtimeObject.GetUID()), nil
	}
	if identity.RuntimeResourceVersion != runtimeObject.GetResourceVersion() {
		return true, fmt.Sprintf("Runtime %q/%q resourceVersion changed from %q to %q",
			identity.RuntimeNamespace, identity.RuntimeName,
			identity.RuntimeResourceVersion, runtimeObject.GetResourceVersion()), nil
	}

	return false, "", nil
}

// buildPVCInfoCacheEntry resolves a single PVC into a cache entry that contains the
// "belongs to Dataset" decision, the RuntimeInfo used by later plugins and the identities
// of the PVC/Dataset/Runtime objects from which they are derived.
func buildPVCInfoCacheEntry(reader client.Reader, pvc *corev1.PersistentVolumeClaim,
	setupLog logr.Logger, skipPrecheck bool) (entry *cache.PersistentVolumeClaimInfoCache, isDatasetPVC bool, err error) {
	isDatasetPVC = kubeclient.CheckIfPVCIsDataset(pvc)
	identity := cache.SourceObjectIdentity{
		PVCUID:             pvc.UID,
		PVCResourceVersion: pvc.ResourceVersion,
	}

	var runtimeInfo base.RuntimeInfoInterface
	if isDatasetPVC {
		runtimeInfo, err = buildRuntimeInfoInternalWithPrecheck(reader, pvc, setupLog, skipPrecheck)
		if err != nil {
			return nil, false, err
		}

		datasetName := resolveDatasetName(pvc)
		dataset, datasetErr := utils.GetDataset(reader, datasetName, pvc.GetNamespace())
		if datasetErr != nil {
			return nil, false, errors.Wrapf(datasetErr,
				"failed to get Dataset %q/%q when building runtime info cache entry",
				pvc.GetNamespace(), datasetName)
		}
		identity.DatasetUID = dataset.UID
		identity.DatasetResourceVersion = dataset.ResourceVersion

		runtimeObject, runtimeErr := getRuntimeObject(reader,
			runtimeInfo.GetRuntimeType(), runtimeInfo.GetName(), runtimeInfo.GetNamespace())
		if runtimeErr != nil {
			return nil, false, errors.Wrapf(runtimeErr,
				"failed to get Runtime %q/%q of type %q when building runtime info cache entry",
				runtimeInfo.GetNamespace(), runtimeInfo.GetName(), runtimeInfo.GetRuntimeType())
		}
		identity.RuntimeType = runtimeInfo.GetRuntimeType()
		identity.RuntimeName = runtimeInfo.GetName()
		identity.RuntimeNamespace = runtimeInfo.GetNamespace()
		identity.RuntimeUID = runtimeObject.GetUID()
		identity.RuntimeResourceVersion = runtimeObject.GetResourceVersion()
	}

	entry = cache.NewPersistentVolumeClaimInfoCache(runtimeInfo, isDatasetPVC, identity)
	return entry, isDatasetPVC, nil
}

// resolveDatasetName returns the Dataset name associated with a dataset PVC. A PVC managed
// by Fluid may carry the Dataset name in its labels, otherwise the PVC name is used.
func resolveDatasetName(pvc *corev1.PersistentVolumeClaim) string {
	datasetName := pvc.GetName()
	if name, exists := common.GetManagerDatasetFromLabels(pvc.Labels); exists {
		datasetName = name
	}
	return datasetName
}

// getRuntimeObject fetches the typed Runtime object behind a Dataset and returns it as a
// metav1.Object so that its UID and ResourceVersion can be read generically.
func getRuntimeObject(reader client.Reader, runtimeType, name, namespace string) (metav1.Object, error) {
	switch runtimeType {
	case common.AlluxioRuntime:
		return utils.GetAlluxioRuntime(reader, name, namespace)
	case common.JindoRuntime:
		return utils.GetJindoRuntime(reader, name, namespace)
	case common.JuiceFSRuntime:
		return utils.GetJuiceFSRuntime(reader, name, namespace)
	case common.ThinRuntime:
		return utils.GetThinRuntime(reader, name, namespace)
	case common.EFCRuntime:
		return utils.GetEFCRuntime(reader, name, namespace)
	case common.VineyardRuntime:
		return utils.GetVineyardRuntime(reader, name, namespace)
	case common.CacheRuntime:
		return utils.GetCacheRuntime(reader, name, namespace)
	default:
		return nil, fmt.Errorf("unknown runtime type %q for runtime %q/%q", runtimeType, namespace, name)
	}
}

func buildRuntimeInfoInternalWithPrecheck(client client.Reader,
	pvc *corev1.PersistentVolumeClaim,
	log logr.Logger, skipPrecheck bool) (runtimeInfo base.RuntimeInfoInterface, err error) {
	if utils.IsTimeTrackerDebugEnabled() {
		defer utils.TimeTrack(time.Now(), "mutating.buildRuntimeInfoInternalByPVC",
			"pvc.name", pvc.GetName(), "pvc.namespace", pvc.GetNamespace())
	}
	namespace := pvc.GetNamespace()
	if len(namespace) == 0 {
		namespace = corev1.NamespaceDefault
	}
	pvcName := pvc.GetName()
	if datasetName, exists := common.GetManagerDatasetFromLabels(pvc.Labels); exists {
		pvcName = datasetName
	}

	if !skipPrecheck {
		if err = checkDatasetBound(client, pvcName, namespace); err != nil {
			return
		}
	}

	runtimeInfo, err = base.GetRuntimeInfo(client, pvcName, namespace)
	if err != nil {
		log.Error(err, "unable to get runtimeInfo, get failure", "runtime", pvc.GetName(), "namespace", namespace)
		return
	}
	return
}

func checkDatasetBound(client client.Reader, name, namespace string) (err error) {
	dataset, err := utils.GetDataset(client, name, namespace)
	if err != nil {
		return
	}

	if dataset.Status.Phase == v1alpha1.NotBoundDatasetPhase || dataset.Status.Phase == v1alpha1.NoneDatasetPhase {
		_, cond := utils.GetDatasetCondition(dataset.Status.Conditions, v1alpha1.DatasetNotReady)
		if cond != nil {
			err = fmt.Errorf("dataset \"%s/%s\" not ready because %s", dataset.Namespace, dataset.Name, cond.Message)
			return
		}
		err = fmt.Errorf("dataset \"%s/%s\" not bound", dataset.Namespace, dataset.Name)
		return
	}
	return nil
}
