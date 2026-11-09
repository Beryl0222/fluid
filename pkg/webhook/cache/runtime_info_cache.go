/*
Copyright 2022 The Fluid Authors.

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

package cache

import (
	"time"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils"

	"k8s.io/apimachinery/pkg/types"
	utilcache "k8s.io/apimachinery/pkg/util/cache"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	defaultCacheSize = 64

	defaultTimeToLive = 10 * time.Minute
)

var (
	runtimeInfoCache  *utilcache.LRUExpireCache
	fuseTemplateCache *utilcache.LRUExpireCache
	cacheSize         = 64
	log               = ctrl.Log.WithName("webhook.cache")
	timeToLive        = 5 * time.Minute
)

// SourceObjectIdentity records the identities of the source objects (the PVC, its
// Dataset and its Runtime) from which a PersistentVolumeClaimInfoCache entry is built.
//
// A cached entry may be reused only when every recorded identity still matches the
// current objects. Therefore any update (ResourceVersion change), deletion/recreation
// (UID change) or runtime type change of the source objects invalidates the entry,
// which prevents stale "is dataset" decisions and stale RuntimeInfo from being reused.
type SourceObjectIdentity struct {
	// PVC identity. UID changes when the PVC is deleted and recreated with the same name,
	// while ResourceVersion changes on every PVC update (e.g. label flips).
	PVCUID             types.UID
	PVCResourceVersion string

	// Dataset identity. Both fields are empty when the PVC is not backed by a Dataset.
	// A Dataset rebinding another Runtime bumps the Dataset ResourceVersion.
	DatasetUID             types.UID
	DatasetResourceVersion string

	// Runtime identity. All fields are empty when the PVC is not backed by a Dataset.
	// Runtime spec updates (e.g. FUSE node selector changes) bump the Runtime ResourceVersion.
	RuntimeType            string
	RuntimeName            string
	RuntimeNamespace       string
	RuntimeUID             types.UID
	RuntimeResourceVersion string
}

// PersistentVolumeClaimInfoCache represents runtime info and whether it belongs dataset,
// together with the identities of the PVC/Dataset/Runtime source objects at the time the
// entry is built.
type PersistentVolumeClaimInfoCache struct {
	info      base.RuntimeInfoInterface
	isDataset bool
	identity  SourceObjectIdentity
}

// NewPersistentVolumeClaimInfoCache builds a cache entry with the RuntimeInfo that later
// plugins consume, the "whether the PVC belongs to a Dataset" decision and the identities
// of the source objects from which both are derived.
func NewPersistentVolumeClaimInfoCache(runtimeInfo base.RuntimeInfoInterface, isDataset bool, identity SourceObjectIdentity) *PersistentVolumeClaimInfoCache {
	return &PersistentVolumeClaimInfoCache{
		info:      runtimeInfo,
		isDataset: isDataset,
		identity:  identity,
	}
}

func (c PersistentVolumeClaimInfoCache) GetRuntimeInfo() base.RuntimeInfoInterface {
	return c.info
}

func (c PersistentVolumeClaimInfoCache) IsBelongToDataset() bool {
	return c.isDataset
}

func (c PersistentVolumeClaimInfoCache) GetIdentity() SourceObjectIdentity {
	return c.identity
}

// IsRuntimeInfoCacheEnabled reports whether the runtime info cache is enabled.
// By default, the cache is disabled unless ENABLE_RUNTIMEINFO_CACHE=true is set.
func IsRuntimeInfoCacheEnabled() bool {
	return runtimeInfoCache != nil
}

// By default, cache is disabled
func init() {
	var (
		enabled bool = utils.GetBoolValueFromEnv(common.EnvEnableRuntimeInfoCache, false)
		found   bool
	)
	cacheSize, found = utils.GetIntValueFromEnv(common.EnvRuntimeInfoCacheSize)
	if !found {
		cacheSize = defaultCacheSize
	}

	if cacheSize > 0 && enabled {
		runtimeInfoCache = utilcache.NewLRUExpireCache(cacheSize)
		fuseTemplateCache = utilcache.NewLRUExpireCache(cacheSize)
		timeToLive = utils.GetDurationValueFromEnv(common.EnvRuntimeInfoCacheTTL, defaultTimeToLive)
	}
}

func GetRuntimeInfoByKey(key types.NamespacedName) (info *PersistentVolumeClaimInfoCache, found bool) {
	if utils.IsTimeTrackerDebugEnabled() {
		defer utils.TimeTrack(time.Now(), "runtimeInfoCache.GetRuntimeInfoByKey",
			"pvc.name", key.Name, "pvc.namespace", key.Namespace)
	}
	if runtimeInfoCache == nil {
		log.V(1).Info("Runtime Info Cache is disabled.")
		return
	}

	var entry interface{}
	entry, found = runtimeInfoCache.Get(key.String())
	if found {
		info = entry.(*PersistentVolumeClaimInfoCache)
	}

	return
}

// SetRuntimeInfoByKey unconditionally stores the entry under the key, overwriting and
// refreshing the TTL of any previously cached entry. Overwriting is required so that a
// refreshed entry built from current PVC/Dataset/Runtime objects can replace a stale one
// instead of being skipped forever until the TTL expires.
func SetRuntimeInfoByKey(key types.NamespacedName, info *PersistentVolumeClaimInfoCache) {
	if utils.IsTimeTrackerDebugEnabled() {
		defer utils.TimeTrack(time.Now(), "runtimeInfoCache.SetRuntimeInfoByKey",
			"pvc.name", key.Name, "pvc.namespace", key.Namespace)
	}
	if runtimeInfoCache == nil {
		log.V(1).Info("Runtime Info Cache is disabled.")
		return
	}

	runtimeInfoCache.Add(key.String(), info, timeToLive)
	log.V(1).Info("set runtimeInfoCache entry",
		"key", key, "isDataset", info.IsBelongToDataset(), "identity", info.GetIdentity())
}

// RemoveRuntimeInfoByKey invalidates the entry cached under the key when it is found to
// be stale against current source objects. It is a no-op when no entry is cached.
func RemoveRuntimeInfoByKey(key types.NamespacedName) {
	if runtimeInfoCache == nil {
		return
	}
	runtimeInfoCache.Remove(key.String())
	log.V(1).Info("removed runtimeInfoCache entry", "key", key)
}

// EnableRuntimeInfoCacheForTest resets and enables the runtime info caches with the given
// size and TTL. It is only meant for unit tests that need deterministic cache state.
func EnableRuntimeInfoCacheForTest(size int, ttl time.Duration) {
	if size <= 0 {
		size = defaultCacheSize
	}
	runtimeInfoCache = utilcache.NewLRUExpireCache(size)
	fuseTemplateCache = utilcache.NewLRUExpireCache(size)
	timeToLive = ttl
}

// DisableRuntimeInfoCacheForTest turns off the runtime info caches. It is only meant for
// unit tests that need to exercise the cache-disabled code path.
func DisableRuntimeInfoCacheForTest() {
	runtimeInfoCache = nil
	fuseTemplateCache = nil
}
