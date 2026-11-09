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

package dataflow

import (
	"fmt"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DependencyDecision is the verdict of evaluating a referenced preceding data operation
// against the live API object and the observation cached on the successor.
type DependencyDecision int

const (
	// DependencyWaiting means the referenced operation exists but has not reached a
	// confirmed terminal phase. The successor must keep waiting and retry later.
	DependencyWaiting DependencyDecision = iota
	// DependencyNotFound means the referenced operation is absent and no terminal state has
	// been confirmed yet, so the outcome is unknown. The successor must keep waiting and
	// retry later (the dependency may still be created or may already have been cleaned up).
	DependencyNotFound
	// DependencyComplete means the referenced operation's Complete terminal state has been
	// confirmed, either from the live object or from the persisted cache. The successor may
	// proceed and the cached node affinity snapshot must be used for affinity injection.
	DependencyComplete
	// DependencyFailed means the referenced operation's Failed terminal state has been
	// confirmed. The successor must fail with a stable, diagnosable result.
	DependencyFailed
	// DependencyIdentityConflict means a same-named object with a different UID was found for
	// a reference whose original UID was already pinned. The recreated object must never
	// impersonate the original dependency, so the successor fails with a stable, diagnosable
	// conflict result.
	DependencyIdentityConflict
	// DependencyRetry means the API server was temporarily unreadable. Callers must only
	// retry and must not mutate any persisted observation.
	DependencyRetry
)

// ResolvePrecedingOperation evaluates the referenced preceding data operation against the
// live API object and the observation cached on the successor. It returns the decision and
// the observation that the caller should persist (nil when there is nothing to persist).
//
// The same rules apply to all four data operation kinds, cross-namespace references and the
// affinityStrategy.dependOn reference:
//   - A confirmed terminal state cached on the successor is authoritative and never
//     regresses, so deleting the referenced object after completion (e.g. TTL cleanup) does
//     not push the successor back to waiting.
//   - The first observed UID pins the dependency identity. A same-named object with a
//     different UID is an identity conflict rather than the original dependency.
//   - A non-NotFound API error means the state is currently unknown: the decision is
//     DependencyRetry and the caller must keep every persisted state untouched.
func ResolvePrecedingOperation(c client.Client,
	ref *datav1alpha1.ObjectRef,
	defaultNamespace string,
	cached *datav1alpha1.PrecedingOperationStatus) (decision DependencyDecision,
	observed *datav1alpha1.PrecedingOperationStatus,
	err error) {

	if ref == nil {
		return DependencyComplete, nil, nil
	}

	namespace := defaultNamespace
	if len(ref.Namespace) != 0 {
		namespace = ref.Namespace
	}

	liveObject, getErr := utils.GetPrecedingOperationObject(c, ref, namespace)

	// A confirmed terminal state is authoritative and must never regress. It allows the
	// successor to make stable decisions even when the referenced object is unreadable.
	if cached != nil && cached.ObservedPhase != "" {
		if getErr != nil {
			if utils.IgnoreNotFound(getErr) == nil {
				// The referenced object is gone (e.g. TTL cleanup after a terminal phase).
				// The confirmed terminal state stays valid, so deletion never pushes the
				// successor back to waiting.
				return terminalDecision(cached), cached, nil
			}
			// API temporarily unreadable: only retry, keep every persisted state untouched.
			return DependencyRetry, nil, getErr
		}

		if cached.UID == liveObject.GetUID() {
			// Same object as the one whose terminal state was confirmed. The cached decision
			// wins even if the live phase flipped back to a non-terminal phase (e.g. a cron
			// operation starting a new run with the same UID).
			return terminalDecision(cached), cached, nil
		}

		// A same-named object with a different UID exists. It must never impersonate the
		// original dependency: surface a stable identity conflict while preserving the
		// originally pinned identity.
		conflict := cached.DeepCopy()
		conflict.ObservedPhase = common.PhaseFailed
		conflict.Reason = common.DataOperationIdentityConflict
		conflict.Message = buildIdentityConflictMessage(ref, namespace, cached.UID, liveObject.GetUID())
		return DependencyIdentityConflict, conflict, nil
	}

	// No confirmed terminal state has been observed yet.
	if getErr != nil {
		if utils.IgnoreNotFound(getErr) == nil {
			// Unknown state: the dependency has never been observed in a terminal phase and
			// is currently absent. Keep waiting; an identity-only cache (if any) is preserved.
			return DependencyNotFound, cached, nil
		}
		// API temporarily unreadable: only retry.
		return DependencyRetry, nil, getErr
	}

	liveStatus, err := utils.GetOperationStatus(liveObject)
	if err != nil {
		return DependencyRetry, nil, err
	}

	// Pin the identity on first sight so a later same-name recreation can be detected.
	observed = &datav1alpha1.PrecedingOperationStatus{
		ObjectRef: datav1alpha1.ObjectRef{
			APIVersion: ref.APIVersion,
			Kind:       ref.Kind,
			Name:       ref.Name,
			Namespace:  namespace,
		},
		UID: liveObject.GetUID(),
	}
	if cached != nil {
		if cached.UID != "" && cached.UID != liveObject.GetUID() {
			// The original dependency disappeared before reaching a terminal phase and a
			// same-named object with a different UID took its place.
			conflict := cached.DeepCopy()
			conflict.ObservedPhase = common.PhaseFailed
			conflict.Reason = common.DataOperationIdentityConflict
			conflict.Message = buildIdentityConflictMessage(ref, namespace, cached.UID, liveObject.GetUID())
			return DependencyIdentityConflict, conflict, nil
		}
		observed = cached.DeepCopy()
		// Re-resolve the namespace in case the successor's namespace resolution changed.
		observed.ObjectRef.Namespace = namespace
		observed.UID = liveObject.GetUID()
	}

	switch liveStatus.Phase {
	case common.PhaseComplete:
		observed.ObservedPhase = common.PhaseComplete
		observed.Reason = ""
		observed.Message = ""
		if liveStatus.NodeAffinity != nil {
			observed.NodeAffinity = liveStatus.NodeAffinity.DeepCopy()
		}
		return DependencyComplete, observed, nil
	case common.PhaseFailed:
		observed.ObservedPhase = common.PhaseFailed
		observed.Reason = common.DataOperationDependencyFailed
		observed.Message = buildDependencyFailedMessage(ref, namespace, liveStatus)
		return DependencyFailed, observed, nil
	default:
		// Non-terminal phase: remember the identity only and keep waiting.
		return DependencyWaiting, observed, nil
	}
}

// terminalDecision maps a cached confirmed terminal phase to its decision.
func terminalDecision(cached *datav1alpha1.PrecedingOperationStatus) DependencyDecision {
	if cached.ObservedPhase == common.PhaseFailed {
		if cached.Reason == common.DataOperationIdentityConflict {
			return DependencyIdentityConflict
		}
		return DependencyFailed
	}
	return DependencyComplete
}

// dependencyKey returns a human-readable reference of the preceding operation.
func dependencyKey(ref *datav1alpha1.ObjectRef, namespace string) string {
	return fmt.Sprintf("%s \"%s/%s\"", ref.Kind, namespace, ref.Name)
}

func buildDependencyFailedMessage(ref *datav1alpha1.ObjectRef, namespace string,
	liveStatus *datav1alpha1.OperationStatus) string {

	message := fmt.Sprintf("preceding operation %s reached phase Failed", dependencyKey(ref, namespace))
	for _, condition := range liveStatus.Conditions {
		if condition.Type == common.Failed && len(condition.Message) != 0 {
			return fmt.Sprintf("%s: %s", message, condition.Message)
		}
	}
	return message
}

func buildIdentityConflictMessage(ref *datav1alpha1.ObjectRef, namespace string,
	pinnedUID, observedUID types.UID) string {

	return fmt.Sprintf("a different object was found behind preceding operation %s: observed UID %q does not match the pinned UID %q of the original dependency, refusing to treat the recreated object as the dependency",
		dependencyKey(ref, namespace), observedUID, pinnedUID)
}
