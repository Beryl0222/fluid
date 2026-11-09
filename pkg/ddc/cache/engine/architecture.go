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

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reason codes carried by ExecutionPodUnavailableError. They are stable strings so
// callers (and tests) can distinguish the transient causes without parsing messages.
const (
	// ExecutionPodUnavailableReasonZeroReplicas means the component workload is scaled
	// to zero replicas, so no execution pod can exist.
	ExecutionPodUnavailableReasonZeroReplicas = "ZeroReplicas"
	// ExecutionPodUnavailableReasonWorkloadNotFound means the component workload has not
	// been created yet.
	ExecutionPodUnavailableReasonWorkloadNotFound = "WorkloadNotFound"
	// ExecutionPodUnavailableReasonNoReadyPod means the workload exists but none of its
	// pods is Running and Ready (e.g. rolling replacement, node failure or slow startup).
	ExecutionPodUnavailableReasonNoReadyPod = "NoReadyPod"
)

// ErrExecutionPodUnavailable is the sentinel error returned when no stable, Ready pod
// can currently be selected to execute runtime operation commands (report summary,
// ufs mount, ...). It describes an expected transient condition rather than a
// configuration failure: callers must keep the last known good state (e.g. previous
// dataset cache states) and retry on the next reconcile instead of writing an error state.
var ErrExecutionPodUnavailable = fmt.Errorf("execution pod is temporarily unavailable")

// ExecutionPodUnavailableError wraps ErrExecutionPodUnavailable with the component and
// the concrete reason why selection failed. Use errors.Is(err, ErrExecutionPodUnavailable)
// to detect it, even when the error is wrapped.
type ExecutionPodUnavailableError struct {
	component string
	reason    string
	detail    string
}

func (e *ExecutionPodUnavailableError) Error() string {
	msg := fmt.Sprintf("%s: no available execution pod for component %s: %s",
		ErrExecutionPodUnavailable.Error(), e.component, e.reason)
	if e.detail != "" {
		msg = fmt.Sprintf("%s (%s)", msg, e.detail)
	}
	return msg
}

// Is makes the typed error match the ErrExecutionPodUnavailable sentinel through
// arbitrary wrapping (errors.Is / pkg/errors).
func (e *ExecutionPodUnavailableError) Is(target error) bool {
	return target == ErrExecutionPodUnavailable
}

// Reason returns the machine-readable reason code of the unavailability.
func (e *ExecutionPodUnavailableError) Reason() string {
	return e.reason
}

// newExecutionPodUnavailable builds an unavailable error for the given component.
func newExecutionPodUnavailable(component, reason, detail string) *ExecutionPodUnavailableError {
	return &ExecutionPodUnavailableError{
		component: component,
		reason:    reason,
		detail:    detail,
	}
}

// IsExecutionPodUnavailable reports whether err is (or wraps) the transient
// ErrExecutionPodUnavailable sentinel. Callers use it to distinguish an expected
// "no ready execution pod" condition from real operation errors.
func IsExecutionPodUnavailable(err error) bool {
	return errors.Is(err, ErrExecutionPodUnavailable)
}

// ArchitectureApi defines architecture-aware operations for a CacheRuntime.
// Different runtime topologies (Master-Worker / Workers-Only) are represented by
// different struct implementations, making it easy to extend with new architectures.
type ArchitectureApi interface {
	// GetExecutionPodInfo resolves the target pod name and container name used to execute
	// runtime operations (e.g. report summary, ufs mount).
	//
	// The resolved identity is pinned for the whole command invocation: the caller resolves
	// it once and reuses the same pod/container until the command finishes. When the selected
	// pod becomes unhealthy, the next command (i.e. the next reconcile retry) re-runs the
	// selection and may fail over to another healthy replica.
	//
	// When no Ready replica can be selected, an ExecutionPodUnavailableError wrapping
	// ErrExecutionPodUnavailable is returned; callers must not write an error state.
	GetExecutionPodInfo() (podName string, containerName string, err error)

	// GetExecutionEntries returns the execution entries for this architecture.
	GetExecutionEntries() *datav1alpha1.ExecutionEntries

	// IsMountUFSSupported returns whether UFS mounting is supported on this architecture.
	IsMountUFSSupported() bool
}

// resolveArchitectureApi inspects the runtime class topology and runtime spec
// to decide which architecture handler should be used.
//
// Resolution rules:
//   - If runtimeClass.Topology.Master is configured AND runtime.Spec.Master is not
//     disabled, MasterWorkerArchitecture is returned.
//   - Otherwise, WorkersOnlyArchitecture is returned.
//
// The k8s client is injected so the workers-only handler can discover the actual
// StatefulSet/Pod state instead of hardcoding the worker-0 pod. The master-worker
// handler keeps pinning the master pod and ignores the client.
func resolveArchitectureApi(c client.Client, name, namespace string, runtime *datav1alpha1.CacheRuntime, runtimeClass *datav1alpha1.CacheRuntimeClass) ArchitectureApi {
	noMasterTopology := runtimeClass == nil || runtimeClass.Topology == nil || runtimeClass.Topology.Master == nil
	masterDisabled := runtime == nil || runtime.Spec.Master.Disabled
	if noMasterTopology || masterDisabled {
		return &workersOnlyArchApi{
			client:       c,
			name:         name,
			namespace:    namespace,
			runtimeClass: runtimeClass,
		}
	}
	return &masterWorkerArchApi{
		client:       c,
		name:         name,
		namespace:    namespace,
		runtimeClass: runtimeClass,
	}
}
