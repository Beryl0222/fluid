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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DependencyVerdict is the stable decision produced by CheckPrecedingOperation.
type DependencyVerdict string

const (
	// DependencyWaiting means the preceding operation has not reached a confirmed
	// terminal state: it is either still running, has never been observed, or the API
	// server was temporarily unreadable. Callers must keep waiting or retrying.
	DependencyWaiting DependencyVerdict = "Waiting"

	// DependencySucceeded means the preceding operation, observed live or reconstructed
	// from its persisted terminal record, is confirmed Complete.
	DependencySucceeded DependencyVerdict = "Succeeded"

	// DependencyFailed means the preceding operation, observed live or reconstructed
	// from its persisted terminal record, is confirmed Failed. The successor must fail
	// with a stable and diagnosable result instead of waiting forever.
	DependencyFailed DependencyVerdict = "Failed"

	// DependencyConflict means the live object located at the referenced name/namespace
	// carries a UID different from the UID the successor has pinned. The recreated
	// same-name object must not impersonate the original dependency.
	DependencyConflict DependencyVerdict = "Conflict"
)

// DependencyCheckResult is the judgment of a single preceding operation reference.
type DependencyCheckResult struct {
	// Verdict is the stable decision for the dependency.
	Verdict DependencyVerdict

	// Record is the dependency record that should be persisted. It may be nil when the
	// observation never produced a record (e.g. the object has never been found) or when
	// the recorded state did not change.
	Record *datav1alpha1.DependencyStatus

	// Err carries a stable diagnostic error for a Failed/Conflict verdict, the transient
	// API error for a retryable read, or the NotFound error while the dependency state is
	// still unknown.
	Err error
}

// CheckPrecedingOperation judges a single preceding operation reference against the live
// object in the API server and the terminal record persisted on the successor. It never
// mutates cluster state; the caller is responsible for persisting result.Record.
//
// Decision matrix:
//   - live read fails with a non-NotFound error: DependencyWaiting plus the error, the
//     caller retries only and keeps every persisted state untouched.
//   - live object not found while a terminal (Complete/Failed) record exists: the record
//     is trusted, so TTL cleanup or any other deletion cannot push the successor back to
//     waiting.
//   - live object not found without a terminal record: DependencyWaiting, the state is
//     unknown and the controller keeps retrying.
//   - live object UID differs from the recorded UID: DependencyConflict, a stable failure.
//   - live object observed: its UID is pinned; Complete/Failed produce terminal records
//     (Complete also snapshots NodeAffinity), any other phase yields DependencyWaiting.
//
// The same judgment is shared by the DataFlow waiting controller and affinity injection,
// covering all four operation kinds, cross-namespace references and affinityStrategy.dependOn.
func CheckPrecedingOperation(c client.Client, ref *datav1alpha1.ObjectRef, defaultNamespace string,
	recorded *datav1alpha1.DependencyStatus) DependencyCheckResult {

	resolvedRef := ResolveDependencyRef(ref, defaultNamespace)

	object, err := utils.GetPrecedingOperation(c, ref, resolvedRef.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if isTerminalRecord(recorded) {
				// The terminal state was confirmed before the object disappeared. Deletion
				// (e.g. TTL cleanup) must not move the successor back to waiting, and the
				// decision survives controller restarts.
				return terminalVerdictFromRecord(recorded)
			}
			// The object was never observed, or its identity was pinned before any terminal
			// state was seen. Both are unknown states: keep waiting and retry.
			return DependencyCheckResult{Verdict: DependencyWaiting, Record: recorded, Err: err}
		}
		// Temporary API failure: retry only, do not touch any persisted state.
		return DependencyCheckResult{Verdict: DependencyWaiting, Record: recorded, Err: err}
	}

	liveUID := object.GetUID()
	if recorded != nil && len(recorded.UID) != 0 && recorded.UID != liveUID {
		conflictErr := fmt.Errorf("identity conflict for preceding operation %s %s/%s: recorded UID %s does not match live UID %s",
			resolvedRef.Kind, resolvedRef.Namespace, resolvedRef.Name, recorded.UID, liveUID)
		return DependencyCheckResult{Verdict: DependencyConflict, Record: recorded.DeepCopy(), Err: conflictErr}
	}

	status, err := utils.GetOperationStatus(object)
	if err != nil {
		return DependencyCheckResult{Verdict: DependencyWaiting, Record: recorded, Err: err}
	}

	record := &datav1alpha1.DependencyStatus{
		Ref:   resolvedRef,
		UID:   liveUID,
		Phase: status.Phase,
	}

	switch status.Phase {
	case common.PhaseComplete:
		record.NodeAffinity = status.NodeAffinity.DeepCopy()
		fillTerminalDiagnostics(record, status.Conditions)
		return DependencyCheckResult{Verdict: DependencySucceeded, Record: record}
	case common.PhaseFailed:
		fillTerminalDiagnostics(record, status.Conditions)
		return DependencyCheckResult{
			Verdict: DependencyFailed,
			Record: record,
			Err: fmt.Errorf("preceding operation %s %s/%s (UID %s) failed: %s",
				resolvedRef.Kind, resolvedRef.Namespace, resolvedRef.Name, liveUID, terminalMessage(record)),
		}
	default:
		// Non-terminal phase: pin the identity so a later same-name recreation cannot
		// impersonate the original dependency.
		return DependencyCheckResult{Verdict: DependencyWaiting, Record: record}
	}
}

// ResolveDependencyRef returns a copy of ref with its namespace defaulted to
// defaultNamespace, so persisted records stay self-contained even when the user omitted
// the namespace in the reference.
func ResolveDependencyRef(ref *datav1alpha1.ObjectRef, defaultNamespace string) datav1alpha1.ObjectRef {
	resolved := *ref
	if len(resolved.Namespace) == 0 {
		resolved.Namespace = defaultNamespace
	}
	return resolved
}

// SameObjectRef tells whether two references point to the same kind/name/namespace object.
func SameObjectRef(a, b datav1alpha1.ObjectRef) bool {
	return a.Kind == b.Kind && a.Namespace == b.Namespace && a.Name == b.Name
}

// FindDependencyRecord returns the persisted record matching ref (kind/namespace/name)
// or nil when no record exists.
func FindDependencyRecord(records []datav1alpha1.DependencyStatus, ref *datav1alpha1.ObjectRef,
	defaultNamespace string) *datav1alpha1.DependencyStatus {
	resolved := ResolveDependencyRef(ref, defaultNamespace)
	for i := range records {
		if SameObjectRef(records[i].Ref, resolved) {
			return &records[i]
		}
	}
	return nil
}

// UpsertDependencyRecord returns records with record inserted or replaced at the position
// matching its ref. A nil record is ignored.
func UpsertDependencyRecord(records []datav1alpha1.DependencyStatus,
	record *datav1alpha1.DependencyStatus) []datav1alpha1.DependencyStatus {
	if record == nil {
		return records
	}
	for i := range records {
		if SameObjectRef(records[i].Ref, record.Ref) {
			records[i] = *record.DeepCopy()
			return records
		}
	}
	return append(records, *record.DeepCopy())
}

// isTerminalRecord tells whether recorded holds a confirmed terminal observation. A record
// without a pinned UID is never considered terminal, since it does not identify a concrete
// preceding object.
func isTerminalRecord(recorded *datav1alpha1.DependencyStatus) bool {
	if recorded == nil || len(recorded.UID) == 0 {
		return false
	}
	return recorded.Phase == common.PhaseComplete || recorded.Phase == common.PhaseFailed
}

// terminalVerdictFromRecord reconstructs the stable verdict from a persisted terminal
// record after the live object has become unreadable or absent.
func terminalVerdictFromRecord(recorded *datav1alpha1.DependencyStatus) DependencyCheckResult {
	record := recorded.DeepCopy()
	if record.Phase == common.PhaseComplete {
		return DependencyCheckResult{Verdict: DependencySucceeded, Record: record}
	}

	return DependencyCheckResult{
		Verdict: DependencyFailed,
		Record:  record,
		Err: fmt.Errorf("preceding operation %s %s/%s (UID %s) was confirmed failed and is no longer present: %s",
			record.Ref.Kind, record.Ref.Namespace, record.Ref.Name, record.UID, terminalMessage(record)),
	}
}

// fillTerminalDiagnostics copies the reason/message of the condition matching the recorded
// terminal phase, falling back to the first condition when no type matches.
func fillTerminalDiagnostics(record *datav1alpha1.DependencyStatus, conditions []datav1alpha1.Condition) {
	for _, cond := range conditions {
		if (record.Phase == common.PhaseComplete && cond.Type == common.Complete) ||
			(record.Phase == common.PhaseFailed && cond.Type == common.Failed) {
			record.Reason = cond.Reason
			record.Message = cond.Message
			return
		}
	}
	if len(conditions) > 0 {
		record.Reason = conditions[0].Reason
		record.Message = conditions[0].Message
	}
}

// terminalMessage returns the most informative available diagnostic for a terminal record.
func terminalMessage(record *datav1alpha1.DependencyStatus) string {
	if len(record.Message) != 0 {
		return record.Message
	}
	if len(record.Reason) != 0 {
		return record.Reason
	}
	return "no terminal message recorded"
}
