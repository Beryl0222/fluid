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

package plugins

import (
	"strings"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
)

// fuseVolumeReference captures the fuse selector information a single staged volume requires on
// the node. The fuseLabelKey selects the runtime FUSE, while generation selects the exact FUSE
// template generation the volume was created against. An empty generation marks a legacy volume
// that predates FUSE generation tracking.
type fuseVolumeReference struct {
	volumeID     string
	fuseLabelKey string
	generation   string
}

// publishFuseGenerationReferences publishes the runtime fuse selector and the generation-scoped
// selector required by the given volume onto the node. Adding labels is idempotent, so the
// operation is safe to retry after a node patch conflict or a CSI plugin restart.
func (ns *nodeServer) publishFuseGenerationReferences(ref fuseVolumeReference) error {
	if len(ref.fuseLabelKey) == 0 {
		return status.Errorf(codes.InvalidArgument, "fuse label key is empty for volume %s", ref.volumeID)
	}

	release, err := ns.acquireRuntimeFuseLock(ref.fuseLabelKey)
	if err != nil {
		return err
	}
	defer release()

	buildLabels := func(_ *corev1.Node) (common.LabelsToModify, error) {
		var labelsToModify common.LabelsToModify
		// The runtime-level selector always stays while any generation of this runtime is in use.
		// It is also the selector existing FUSE DaemonSets match on, so it preserves backward
		// compatibility for volumes without a generation label.
		labelsToModify.Add(ref.fuseLabelKey, "true")

		if len(ref.generation) > 0 {
			generationLabelKey, err := utils.GetFuseGenerationLabelName(ref.fuseLabelKey, ref.generation)
			if err != nil {
				return labelsToModify, err
			}
			labelsToModify.Add(generationLabelKey, "true")
		}
		return labelsToModify, nil
	}

	if err := ns.patchNodeLabelsWithRetry(buildLabels); err != nil {
		return errors.Wrapf(err, "failed to publish fuse generation references for volume %s", ref.volumeID)
	}
	glog.Infof("Published fuse selector labels for volume %s (fuseLabelKey: %s, generation: %q)", ref.volumeID, ref.fuseLabelKey, ref.generation)
	return nil
}

// reconcileFuseGenerationReferences converges the node's fuse selector labels for the runtime of
// currentRef to the node's actual mount set. A generation-scoped selector is removed only when no
// staged volume on the node still references that generation, and the runtime-level selector is
// removed only when no volume of the runtime remains. Any undeterminable state (mount info read
// failure, transient API error, unresolvable volume ownership) aborts the whole reconciliation
// before the node is touched, so unknown occupancy can never be treated as safe to clean.
func (ns *nodeServer) reconcileFuseGenerationReferences(currentRef fuseVolumeReference) error {
	if len(currentRef.fuseLabelKey) == 0 {
		return status.Errorf(codes.InvalidArgument, "fuse label key is empty for volume %s", currentRef.volumeID)
	}

	release, err := ns.acquireRuntimeFuseLock(currentRef.fuseLabelKey)
	if err != nil {
		return err
	}
	defer release()

	// Reconstruct the reference set from the actual mount set instead of any in-memory counter,
	// so the result stays correct after concurrent interleavings and CSI plugin restarts.
	mountedVolumeIDs, err := listStagedCSIVolumeIDs()
	if err != nil {
		return status.Errorf(codes.Internal, "NodeUnstageVolume: failed to discover staged volumes on node %s: %v", ns.nodeId, err)
	}

	inUseGenerations := sets.New[string]()
	hasLegacyUser := false
	for _, volumeID := range sets.List(mountedVolumeIDs) {
		ref, err := ns.resolveFuseVolumeReference(volumeID)
		if err != nil {
			// Fail closed: an unresolvable mount might still need the selector, so do not remove
			// any label until the state can be determined (the RPC is retried later).
			return status.Errorf(codes.Internal, "NodeUnstageVolume: failed to resolve staged volume %s on node %s: %v", volumeID, ns.nodeId, err)
		}
		if ref == nil {
			continue
		}
		if ref.fuseLabelKey != currentRef.fuseLabelKey {
			// Volumes of another runtime do not participate in this runtime's reference counting.
			continue
		}

		if len(ref.generation) == 0 {
			hasLegacyUser = true
			continue
		}
		inUseGenerations.Insert(ref.generation)
	}

	runtimeInUse := hasLegacyUser || inUseGenerations.Len() > 0
	glog.Infof("NodeUnstageVolume: reconciled fuse references for runtime selector %s on node %s, in-use generations: %v, legacy user: %v",
		currentRef.fuseLabelKey, ns.nodeId, sets.List(inUseGenerations), hasLegacyUser)

	buildLabels := func(node *corev1.Node) (common.LabelsToModify, error) {
		return buildFuseSelectorCleanupLabels(node, currentRef.fuseLabelKey, inUseGenerations, runtimeInUse), nil
	}

	if err := ns.patchNodeLabelsWithRetry(buildLabels); err != nil {
		return errors.Wrapf(err, "failed to reconcile fuse generation references for volume %s", currentRef.volumeID)
	}
	return nil
}

// buildFuseSelectorCleanupLabels computes the label deletions that converge a node to the actual
// mount set. Every generation-scoped label without a remaining user is deleted, including stale
// labels left by older generations or interrupted reconciliations, and the runtime-level label is
// deleted only when the runtime has no user at all. Labels still referenced are never touched.
func buildFuseSelectorCleanupLabels(node *corev1.Node, fuseLabelKey string, inUseGenerations sets.Set[string], runtimeInUse bool) common.LabelsToModify {
	var labelsToModify common.LabelsToModify

	generationLabelPrefix := utils.GetFuseGenerationLabelPrefix(fuseLabelKey)
	for labelKey := range node.Labels {
		if !strings.HasPrefix(labelKey, generationLabelPrefix) {
			continue
		}
		generation := strings.TrimPrefix(labelKey, generationLabelPrefix)
		if inUseGenerations.Has(generation) {
			continue
		}
		glog.Infof("Removing unused fuse generation selector %s (generation %q has no remaining user on the node)", labelKey, generation)
		labelsToModify.Delete(labelKey)
	}

	if !runtimeInUse {
		glog.Infof("Removing runtime fuse selector %s because no volume of the runtime remains mounted on the node", fuseLabelKey)
		labelsToModify.Delete(fuseLabelKey)
	}

	return labelsToModify
}

// resolveFuseVolumeReference maps a staged CSI volume ID to the fuse selector reference it
// requires. A nil reference with nil error means the volume does not belong to Fluid (or its
// PV/PVC is definitively gone) and must be ignored. Every other lookup error is surfaced so the
// caller can fail closed instead of cleaning selectors based on unknown occupancy.
func (ns *nodeServer) resolveFuseVolumeReference(volumeID string) (*fuseVolumeReference, error) {
	pv, err := kubeclient.GetPersistentVolume(ns.apiReader, volumeID)
	if err != nil {
		if apierrs.IsNotFound(err) {
			glog.Warningf("Staged volume %s has no PersistentVolume anymore, skip it while reconciling fuse references", volumeID)
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to get PersistentVolume %s", volumeID)
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.CSIDriver {
		return nil, nil
	}

	if pv.Spec.ClaimRef == nil {
		return nil, errors.Errorf("PersistentVolume %s has an unexpected nil claimRef", volumeID)
	}

	pvc, err := kubeclient.GetPersistentVolumeClaim(ns.apiReader, pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
	if err != nil {
		if apierrs.IsNotFound(err) {
			glog.Warningf("Staged volume %s has no PersistentVolumeClaim anymore, skip it while reconciling fuse references", volumeID)
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to get PersistentVolumeClaim %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
	}

	fuseLabelKey := ""
	if pv.Spec.CSI.VolumeAttributes != nil {
		fuseLabelKey = pv.Spec.CSI.VolumeAttributes[common.VolumeAttrMountPodNodeSelectorKey]
	}
	if len(fuseLabelKey) == 0 {
		// Backward compatibility for PVs created before the node selector key was carried in the
		// PV's volume attributes: derive the key from the runtime info.
		runtimeInfo, runtimeErr := base.GetRuntimeInfo(ns.client, pvc.Name, pvc.Namespace)
		if runtimeErr != nil {
			if utils.IgnoreNotFound(runtimeErr) == nil {
				glog.Warningf("Runtime of staged volume %s (%s/%s) is not found, skip it while reconciling fuse references", volumeID, pvc.Namespace, pvc.Name)
				return nil, nil
			}
			return nil, errors.Wrapf(runtimeErr, "failed to resolve fuse label key for volume %s", volumeID)
		}
		fuseLabelKey = runtimeInfo.GetFuseLabelName()
	}

	generation := ""
	if pvc.Labels != nil {
		generation = pvc.Labels[common.LabelRuntimeFuseGeneration]
	}

	return &fuseVolumeReference{
		volumeID:     volumeID,
		fuseLabelKey: fuseLabelKey,
		generation:   generation,
	}, nil
}

// getVolumeFuseGeneration returns the FUSE generation the given runtime's volume is currently
// associated with. An empty generation means the volume predates FUSE generation tracking and
// must keep the legacy, generation-agnostic behavior.
func (ns *nodeServer) getVolumeFuseGeneration(namespace, name string) (string, error) {
	pvc, err := kubeclient.GetPersistentVolumeClaim(ns.apiReader, name, namespace)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get PersistentVolumeClaim %s/%s to resolve fuse generation", namespace, name)
	}
	if pvc.Labels == nil {
		return "", nil
	}
	return pvc.Labels[common.LabelRuntimeFuseGeneration], nil
}

// acquireRuntimeFuseLock serializes all Stage/Unstage reconciliations targeting the same runtime
// fuse selector on the node, so interleaved operations of multiple volumes of one runtime cannot
// compute label diffs against a concurrently changing node. The returned function releases the
// lock.
func (ns *nodeServer) acquireRuntimeFuseLock(fuseLabelKey string) (func(), error) {
	if ns.runtimeLocks == nil {
		// Defensive fallback for constructions outside the driver wiring; the per-volume lock is
		// still held by the callers.
		glog.Warningf("Runtime fuse lock set is not initialized, proceed without runtime-level locking for selector %s", fuseLabelKey)
		return func() {}, nil
	}
	if !ns.runtimeLocks.TryAcquire(fuseLabelKey) {
		return nil, status.Errorf(codes.Aborted, "another Stage/Unstage operation on runtime fuse selector %s is in progress", fuseLabelKey)
	}
	return func() { ns.runtimeLocks.Release(fuseLabelKey) }, nil
}

// patchNodeLabelsWithRetry applies a label mutation to the node with conflict retries. The node is
// re-fetched and the mutation is re-evaluated on every attempt, which makes both patch conflicts
// and retries after transient failures converge to the state implied by the latest node object.
func (ns *nodeServer) patchNodeLabelsWithRetry(buildLabels func(node *corev1.Node) (common.LabelsToModify, error)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Invalidate the cached node so every attempt observes the latest label set.
		ns.node = nil
		node, err := ns.getNode()
		if err != nil {
			return errors.Wrapf(err, "can't get node %s", ns.nodeId)
		}

		labelsToModify, err := buildLabels(node)
		if err != nil {
			return err
		}
		if len(labelsToModify.GetLabels()) == 0 {
			glog.V(3).Infof("No node label change is needed on node %s, skip patching", ns.nodeId)
			return nil
		}

		if err := ns.patchNodeWithLabel(node, labelsToModify); err != nil {
			return errors.Wrapf(err, "error when patching labels on node %s", ns.nodeId)
		}
		return nil
	})
}
