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
	"testing"
	"time"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"k8s.io/apimachinery/pkg/types"
)

func testKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: "default"}
}

func TestRuntimeInfoCacheDisabledByDefault(t *testing.T) {
	DisableRuntimeInfoCacheForTest()
	if IsRuntimeInfoCacheEnabled() {
		t.Fatalf("expected runtime info cache to be disabled")
	}
	if _, found := GetRuntimeInfoByKey(testKey("pvc")); found {
		t.Fatalf("expected no cache hit when cache is disabled")
	}
	// Writes must be dropped when the cache is disabled.
	SetRuntimeInfoByKey(testKey("pvc"), NewPersistentVolumeClaimInfoCache(nil, false, SourceObjectIdentity{}))
	if _, found := GetRuntimeInfoByKey(testKey("pvc")); found {
		t.Fatalf("expected disabled cache to drop writes")
	}
}

func TestRuntimeInfoCacheSetAndGet(t *testing.T) {
	EnableRuntimeInfoCacheForTest(defaultCacheSize, time.Minute)
	defer DisableRuntimeInfoCacheForTest()

	info, err := base.BuildRuntimeInfo("ds", "default", common.AlluxioRuntime)
	if err != nil {
		t.Fatalf("failed to build runtime info: %v", err)
	}
	identity := SourceObjectIdentity{
		PVCUID:                  "pvc-uid-1",
		PVCResourceVersion:      "100",
		DatasetUID:              "dataset-uid-1",
		DatasetResourceVersion:  "200",
		RuntimeType:             common.AlluxioRuntime,
		RuntimeName:             "ds",
		RuntimeNamespace:        "default",
		RuntimeUID:              "runtime-uid-1",
		RuntimeResourceVersion:  "300",
	}
	entry := NewPersistentVolumeClaimInfoCache(info, true, identity)
	SetRuntimeInfoByKey(testKey("pvc"), entry)

	got, found := GetRuntimeInfoByKey(testKey("pvc"))
	if !found {
		t.Fatalf("expected cache hit after Set")
	}
	if !got.IsBelongToDataset() {
		t.Fatalf("expected cached entry to belong to a dataset")
	}
	if got.GetRuntimeInfo() != info {
		t.Fatalf("expected the exact cached RuntimeInfo to be returned")
	}
	if got.GetIdentity() != identity {
		t.Fatalf("expected cached identity %v, got %v", identity, got.GetIdentity())
	}
}

func TestRuntimeInfoCacheSetOverwritesStaleEntry(t *testing.T) {
	EnableRuntimeInfoCacheForTest(defaultCacheSize, time.Minute)
	defer DisableRuntimeInfoCacheForTest()

	key := testKey("pvc")
	staleInfo, _ := base.BuildRuntimeInfo("ds", "default", common.AlluxioRuntime)
	staleEntry := NewPersistentVolumeClaimInfoCache(staleInfo, true, SourceObjectIdentity{
		PVCUID:             "pvc-uid-1",
		PVCResourceVersion: "100",
	})
	SetRuntimeInfoByKey(key, staleEntry)

	freshInfo, _ := base.BuildRuntimeInfo("ds", "default", common.JuiceFSRuntime)
	freshEntry := NewPersistentVolumeClaimInfoCache(freshInfo, false, SourceObjectIdentity{
		PVCUID:             "pvc-uid-2",
		PVCResourceVersion: "101",
	})
	// Unconditional overwrite must replace the stale entry instead of skipping it.
	SetRuntimeInfoByKey(key, freshEntry)

	got, found := GetRuntimeInfoByKey(key)
	if !found {
		t.Fatalf("expected cache hit after overwrite")
	}
	if got.IsBelongToDataset() {
		t.Fatalf("expected overwritten entry to reflect the new isDataset=false value")
	}
	if got.GetRuntimeInfo() != freshInfo {
		t.Fatalf("expected overwritten entry to hold the fresh RuntimeInfo")
	}
	if got.GetIdentity().PVCResourceVersion != "101" {
		t.Fatalf("expected overwritten identity, got %v", got.GetIdentity())
	}
}

func TestRuntimeInfoCacheRemove(t *testing.T) {
	EnableRuntimeInfoCacheForTest(defaultCacheSize, time.Minute)
	defer DisableRuntimeInfoCacheForTest()

	key := testKey("pvc")
	SetRuntimeInfoByKey(key, NewPersistentVolumeClaimInfoCache(nil, false, SourceObjectIdentity{
		PVCUID: "pvc-uid-1",
	}))
	if _, found := GetRuntimeInfoByKey(key); !found {
		t.Fatalf("expected cache hit before removal")
	}

	RemoveRuntimeInfoByKey(key)
	if _, found := GetRuntimeInfoByKey(key); found {
		t.Fatalf("expected cache miss after removal")
	}

	// Removing a missing key is a no-op and must not panic.
	RemoveRuntimeInfoByKey(testKey("never-cached"))
}

func TestRuntimeInfoCacheTTLExpiry(t *testing.T) {
	EnableRuntimeInfoCacheForTest(defaultCacheSize, 50*time.Millisecond)
	defer DisableRuntimeInfoCacheForTest()

	key := testKey("pvc")
	SetRuntimeInfoByKey(key, NewPersistentVolumeClaimInfoCache(nil, false, SourceObjectIdentity{
		PVCUID: "pvc-uid-1",
	}))
	if _, found := GetRuntimeInfoByKey(key); !found {
		t.Fatalf("expected cache hit before TTL expiry")
	}

	time.Sleep(80 * time.Millisecond)
	if _, found := GetRuntimeInfoByKey(key); found {
		t.Fatalf("expected cache entry to expire after its TTL")
	}
}
