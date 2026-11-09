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

package dataoperation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fluid-cloudnative/fluid/pkg/common"
)

// acceptedSpecPayload is the JSON payload persisted in the AcceptedSpecAnnotation.
// It captures the exact spec that was accepted for an execution generation and the
// object's metadata.generation at acceptance time.
type acceptedSpecPayload struct {
	// Generation is metadata.generation of the object when the spec was accepted.
	Generation int64 `json:"generation"`
	// Spec is the raw JSON serialization of the accepted spec.
	Spec json.RawMessage `json:"spec"`
}

// IsGenerationBound reports whether the given phase represents an execution generation
// that must be bound to its accepted spec. Only the phases after acceptance are bound;
// None and Pending always interpret the live spec because no generation has been accepted yet.
func IsGenerationBound(phase common.Phase) bool {
	switch phase {
	case common.PhaseExecuting, common.PhaseComplete, common.PhaseFailed:
		return true
	default:
		return false
	}
}

// extractSpecRaw returns the raw JSON of the object's spec field in a type-agnostic way.
func extractSpecRaw(obj client.Object) (json.RawMessage, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal object %s/%s to extract spec: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	var body map[string]json.RawMessage
	if err = json.Unmarshal(data, &body); err != nil {
		return nil, fmt.Errorf("failed to decode object %s/%s to extract spec: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	specRaw, ok := body["spec"]
	if !ok {
		return nil, fmt.Errorf("object %s/%s has no spec field to accept", obj.GetNamespace(), obj.GetName())
	}
	return specRaw, nil
}

// FreezeAcceptedSpec persists the object's current spec and metadata.generation into the
// accepted-spec annotation, establishing the immutable spec of the current execution generation.
//
// It is idempotent: when the annotation already reflects the current generation and spec, no
// update is issued. The write is retried on conflicts and the in-memory object is mirrored with
// the persisted annotation on success so that callers observe a coherent state within the same
// reconcile loop. The annotation update only touches metadata and therefore never increments
// metadata.generation.
func FreezeAcceptedSpec(c client.Client, obj client.Object) error {
	specRaw, err := extractSpecRaw(obj)
	if err != nil {
		return err
	}
	payloadRaw, err := json.Marshal(&acceptedSpecPayload{
		Generation: obj.GetGeneration(),
		Spec:       specRaw,
	})
	if err != nil {
		return err
	}
	payloadStr := string(payloadRaw)

	// Fast path: the in-memory object already carries the up-to-date snapshot and no write
	// happened, so its resourceVersion remains authoritative.
	if existing := obj.GetAnnotations()[AcceptedSpecAnnotation]; existing == payloadStr {
		return nil
	}

	var latest client.Object
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Re-fetch to avoid overwriting concurrent metadata changes.
		latest = obj.DeepCopyObject().(client.Object)
		if getErr := c.Get(context.TODO(), client.ObjectKeyFromObject(obj), latest); getErr != nil {
			return getErr
		}
		if current := latest.GetAnnotations()[AcceptedSpecAnnotation]; current == payloadStr {
			return nil
		}
		annotations := latest.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[AcceptedSpecAnnotation] = payloadStr
		latest.SetAnnotations(annotations)
		return c.Update(context.TODO(), latest)
	})
	if err != nil {
		return err
	}

	// Mirror the authoritative metadata onto the in-memory object. Syncing the resourceVersion
	// keeps later same-loop writes (status update, finalizer/owner update) on a current
	// optimistic-concurrency token, since the annotation update advanced the stored token.
	obj.SetAnnotations(latest.GetAnnotations())
	obj.SetResourceVersion(latest.GetResourceVersion())
	return nil
}

// loadAcceptedSpec reads and decodes the accepted-spec annotation. It returns ok=false (without
// error) when the annotation is absent, and an error when the present annotation is malformed.
func loadAcceptedSpec(obj client.Object) (payload acceptedSpecPayload, ok bool, err error) {
	raw, found := obj.GetAnnotations()[AcceptedSpecAnnotation]
	if !found || len(raw) == 0 {
		return payload, false, nil
	}
	if err = json.Unmarshal([]byte(raw), &payload); err != nil {
		return payload, false, fmt.Errorf("malformed accepted-spec annotation on %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	if len(payload.Spec) == 0 {
		return payload, false, fmt.Errorf("invalid accepted-spec annotation on %s/%s: empty spec snapshot", obj.GetNamespace(), obj.GetName())
	}
	return payload, true, nil
}

// DetectSpecDrift reports whether the object's live metadata.generation differs from the
// generation recorded in the accepted spec snapshot. It returns both generations for diagnostics.
// No drift is reported when no accepted snapshot exists.
func DetectSpecDrift(obj client.Object) (drifted bool, acceptedGeneration, currentGeneration int64, err error) {
	payload, ok, err := loadAcceptedSpec(obj)
	if err != nil {
		return false, 0, obj.GetGeneration(), err
	}
	currentGeneration = obj.GetGeneration()
	if !ok {
		return false, 0, currentGeneration, nil
	}
	return currentGeneration != payload.Generation, payload.Generation, currentGeneration, nil
}

// ApplyAcceptedSpec returns the object that spec-derived reconciliation must read from.
//
// When the phase belongs to a bound execution generation (Executing/Complete/Failed) and a valid,
// applicable accepted-spec snapshot exists, a deep copy is returned whose spec is replaced by the
// accepted snapshot while all metadata (name, namespace, resourceVersion, finalizers, owner
// references, deletion timestamp) is kept live. Otherwise the original object is returned unchanged.
//
// Any unreadable or inapplicable snapshot (malformed JSON, empty spec, or type-incompatible
// payload) is tolerated rather than treated as a hard error: the live object is returned so
// reconciliation (including finalizer removal during deletion) is never blocked.
// ReconcileSpecBinding is responsible for observing and repairing the corruption.
//
// The result is the single chokepoint that guarantees target dataset, policy handler, parallelism,
// TTL and helm value generation all interpret the workloads of the current generation with the
// exact spec that created them, regardless of later live spec edits.
func ApplyAcceptedSpec(obj client.Object, phase common.Phase) (client.Object, error) {
	if !IsGenerationBound(phase) {
		return obj, nil
	}
	effective, applicable := buildEffectiveObject(obj)
	if !applicable {
		// Corrupt/inapplicable annotation or no snapshot: degrade gracefully to the live object
		// instead of blocking. The corruption is surfaced and repaired by ReconcileSpecBinding.
		return obj, nil
	}
	return effective, nil
}

// buildEffectiveObject attempts to construct the spec-bound object. It returns ok=false (without
// panicking or returning a hard error) when no usable snapshot exists or the snapshot cannot be
// applied to the concrete object type, so callers can always fall back to the live object.
func buildEffectiveObject(obj client.Object) (effective client.Object, applicable bool) {
	payload, ok, err := loadAcceptedSpec(obj)
	if err != nil || !ok {
		return nil, false
	}

	effective = obj.DeepCopyObject().(client.Object)
	original, err := json.Marshal(effective)
	if err != nil {
		return nil, false
	}
	var body map[string]json.RawMessage
	if err = json.Unmarshal(original, &body); err != nil {
		return nil, false
	}
	body["spec"] = payload.Spec
	merged, err := json.Marshal(body)
	if err != nil {
		return nil, false
	}
	if err = json.Unmarshal(merged, effective); err != nil {
		// Covers type-incompatible (but syntactically valid) snapshots, e.g. a string where the
		// concrete spec struct expects an integer.
		return nil, false
	}
	return effective, true
}

// ReconcileSpecBinding reconciles the accepted-spec bookkeeping for one loop. It never mutates
// any workload, dataset lock or scaling state; its only side effects are the accepted-spec
// annotation and, on drift, an observable warning event plus a log line.
//
//   - Pending: accept (freeze) the current live spec as the generation's spec. The engine acquires
//     the dataset lock and transitions the phase to Executing in the same reconcile window.
//   - Executing/Complete/Failed: self-heal a missing snapshot by adopting the current spec,
//     otherwise compare generations and emit a diagnostic when the live spec has drifted.
//   - None or any other phase: no-op.
func ReconcileSpecBinding(c client.Client, recorder record.EventRecorder, log logr.Logger, obj client.Object, phase common.Phase) error {
	switch phase {
	case common.PhasePending:
		return FreezeAcceptedSpec(c, obj)
	case common.PhaseExecuting, common.PhaseComplete, common.PhaseFailed:
		payload, ok, err := loadAcceptedSpec(obj)
		// repairCorrupt re-adopts the current spec to fix an unreadable or inapplicable snapshot,
		// emitting a warning so the corruption is observable without blocking reconciliation.
		repairCorrupt := func(cause string) error {
			log.Info("Accepted spec snapshot is corrupt, re-adopting the current spec",
				"phase", phase, "namespace", obj.GetNamespace(), "name", obj.GetName(), "cause", cause)
			if recorder != nil {
				recorder.Eventf(obj, v1.EventTypeWarning, common.DataOperationSpecCorrupted,
					"Accepted spec snapshot is corrupt and will be repaired by re-adopting the current spec: %s", cause)
			}
			return FreezeAcceptedSpec(c, obj)
		}
		if err != nil {
			// Malformed JSON or empty spec.
			return repairCorrupt(err.Error())
		}
		if !ok {
			// Self-heal: restore the invariant for objects that entered an active phase without a
			// snapshot (partial write, controller restart, or upgraded pre-existing objects).
			log.Info("Accepted spec snapshot is missing in an active phase, adopting the current spec",
				"phase", phase, "namespace", obj.GetNamespace(), "name", obj.GetName())
			return FreezeAcceptedSpec(c, obj)
		}
		if _, applicable := buildEffectiveObject(obj); !applicable {
			// Syntactically valid but type-incompatible with the concrete object (e.g. a string
			// where the spec struct expects an integer): repair instead of getting stuck.
			return repairCorrupt("accepted spec cannot be applied to the current object type")
		}
		if obj.GetGeneration() != payload.Generation {
			log.Info("Spec drift detected during an active execution generation; the new spec will take effect in the next generation",
				"namespace", obj.GetNamespace(), "name", obj.GetName(),
				"acceptedGeneration", payload.Generation, "currentGeneration", obj.GetGeneration())
			if recorder != nil {
				recorder.Eventf(obj, v1.EventTypeWarning, common.DataOperationSpecDrifted,
					"Spec drift detected: accepted generation %d, current generation %d. "+
						"The running generation keeps using the accepted spec; the new spec takes effect in the next execution generation.",
					payload.Generation, obj.GetGeneration())
			}
		}
		return nil
	default:
		return nil
	}
}
