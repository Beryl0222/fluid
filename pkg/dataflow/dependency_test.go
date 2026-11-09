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
	"context"
	"errors"
	"strings"
	"testing"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	testUID        types.UID = "uid-original"
	recreatedUID   types.UID = "uid-recreated"
	testNamespace            = "default"
	otherNamespace           = "other-ns"
)

func testScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	if err := datav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}
	return s
}

func newPrecedingLoad(uid types.UID, name, namespace string, phase common.Phase, nodeAffinity *v1.NodeAffinity) *datav1alpha1.DataLoad {
	return &datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       uid,
		},
		Status: datav1alpha1.OperationStatus{
			Phase:        phase,
			NodeAffinity: nodeAffinity,
		},
	}
}

func testRef(kind, name, namespace string) *datav1alpha1.ObjectRef {
	return &datav1alpha1.ObjectRef{
		Kind:      kind,
		Name:      name,
		Namespace: namespace,
	}
}

func cachedObservation(ref *datav1alpha1.ObjectRef, uid types.UID, phase common.Phase, affinity *v1.NodeAffinity) *datav1alpha1.PrecedingOperationStatus {
	return &datav1alpha1.PrecedingOperationStatus{
		ObjectRef: datav1alpha1.ObjectRef{
			Kind:      ref.Kind,
			Name:      ref.Name,
			Namespace: ref.Namespace,
		},
		UID:           uid,
		ObservedPhase: phase,
		NodeAffinity:  affinity,
	}
}

func TestResolvePrecedingOperation(t *testing.T) {
	nodeAffinity := &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      common.K8sNodeNameLabelKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"node01"},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name           string
		ref            *datav1alpha1.ObjectRef
		defaultNS      string
		cached         *datav1alpha1.PrecedingOperationStatus
		objects        []runtime.Object
		apiErr         bool
		wantDecision   DependencyDecision
		wantObserved   bool
		wantUID        types.UID
		wantPhase      common.Phase
		wantReason     string
		wantAffinity   bool
		wantErr        bool
		wantResolvedNS string
	}{
		{
			name:         "nil reference is treated as complete",
			ref:          nil,
			wantDecision: DependencyComplete,
			wantObserved: false,
		},
		{
			name:         "live object in non-terminal phase pins identity and waits",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			objects:      []runtime.Object{newPrecedingLoad(testUID, "preceding", testNamespace, common.PhaseExecuting, nil)},
			wantDecision: DependencyWaiting,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    "",
		},
		{
			name:         "live object complete confirms success and snapshots affinity",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			objects:      []runtime.Object{newPrecedingLoad(testUID, "preceding", testNamespace, common.PhaseComplete, nodeAffinity)},
			wantDecision: DependencyComplete,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseComplete,
			wantAffinity: true,
		},
		{
			name: "live object failed gives a stable failure with diagnostic reason",
			ref:  testRef("DataLoad", "preceding", ""),
			objects: []runtime.Object{&datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: testNamespace, UID: testUID},
				Status: datav1alpha1.OperationStatus{
					Phase: common.PhaseFailed,
					Conditions: []datav1alpha1.Condition{
						{Type: common.Failed, Message: "job pod failed"},
					},
				},
			}},
			defaultNS:    testNamespace,
			wantDecision: DependencyFailed,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseFailed,
			wantReason:   common.DataOperationDependencyFailed,
		},
		{
			name:           "referenced object absent with no cache is unknown and waits",
			ref:            testRef("DataLoad", "nonexistent", ""),
			defaultNS:      testNamespace,
			wantDecision:   DependencyNotFound,
			wantObserved:   false,
			wantResolvedNS: testNamespace,
		},
		{
			name:         "deletion after confirmed complete does not regress the successor",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, common.PhaseComplete, nodeAffinity),
			wantDecision: DependencyComplete,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseComplete,
			wantAffinity: true,
		},
		{
			name:         "deletion after confirmed failed keeps the stable failure",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, common.PhaseFailed, nil),
			wantDecision: DependencyFailed,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseFailed,
		},
		{
			name:         "same UID flipping back to a non-terminal phase does not override the cached complete state",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, common.PhaseComplete, nodeAffinity),
			objects:      []runtime.Object{newPrecedingLoad(testUID, "preceding", testNamespace, common.PhasePending, nil)},
			wantDecision: DependencyComplete,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseComplete,
			wantAffinity: true,
		},
		{
			name:         "same-named object with a different UID after a terminal state is an identity conflict",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, common.PhaseComplete, nodeAffinity),
			objects:      []runtime.Object{newPrecedingLoad(recreatedUID, "preceding", testNamespace, common.PhaseComplete, nil)},
			wantDecision: DependencyIdentityConflict,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseFailed,
			wantReason:   common.DataOperationIdentityConflict,
			// the originally confirmed affinity snapshot is preserved alongside the conflict
			wantAffinity: true,
		},
		{
			name:         "same-named object with a different UID before any terminal state is also an identity conflict",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, "", nil),
			objects:      []runtime.Object{newPrecedingLoad(recreatedUID, "preceding", testNamespace, common.PhasePending, nil)},
			wantDecision: DependencyIdentityConflict,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseFailed,
			wantReason:   common.DataOperationIdentityConflict,
		},
		{
			name:         "identity-only cache preserved when the referenced object is temporarily absent",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, "", nil),
			wantDecision: DependencyNotFound,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    "",
		},
		{
			name:           "cross-namespace reference is resolved in the referenced namespace",
			ref:            testRef("DataLoad", "preceding", otherNamespace),
			defaultNS:      testNamespace,
			objects:        []runtime.Object{newPrecedingLoad(testUID, "preceding", otherNamespace, common.PhaseComplete, nodeAffinity)},
			wantDecision:   DependencyComplete,
			wantObserved:   true,
			wantUID:        testUID,
			wantPhase:      common.PhaseComplete,
			wantResolvedNS: otherNamespace,
			wantAffinity:   true,
		},
		{
			name: "DataBackup kind follows the same resolution rules",
			ref:  testRef("DataBackup", "backup", ""),
			objects: []runtime.Object{&datav1alpha1.DataBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: testNamespace, UID: testUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
			}},
			defaultNS:    testNamespace,
			wantDecision: DependencyComplete,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseComplete,
		},
		{
			name: "DataMigrate kind follows the same resolution rules",
			ref:  testRef("DataMigrate", "migrate", ""),
			objects: []runtime.Object{&datav1alpha1.DataMigrate{
				ObjectMeta: metav1.ObjectMeta{Name: "migrate", Namespace: testNamespace, UID: testUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseFailed},
			}},
			defaultNS:    testNamespace,
			wantDecision: DependencyFailed,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    common.PhaseFailed,
			wantReason:   common.DataOperationDependencyFailed,
		},
		{
			name: "DataProcess kind follows the same resolution rules",
			ref:  testRef("DataProcess", "process", ""),
			objects: []runtime.Object{&datav1alpha1.DataProcess{
				ObjectMeta: metav1.ObjectMeta{Name: "process", Namespace: testNamespace, UID: testUID},
				Status:     datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
			}},
			defaultNS:    testNamespace,
			wantDecision: DependencyWaiting,
			wantObserved: true,
			wantUID:      testUID,
			wantPhase:    "",
		},
		{
			name:         "API temporarily unreadable only retries and returns no observation",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			apiErr:       true,
			wantDecision: DependencyRetry,
			wantObserved: false,
			wantErr:      true,
		},
		{
			name:         "API unreadable with a cached terminal state still only retries",
			ref:          testRef("DataLoad", "preceding", ""),
			defaultNS:    testNamespace,
			cached:       cachedObservation(testRef("DataLoad", "preceding", testNamespace), testUID, "", nil),
			apiErr:       true,
			wantDecision: DependencyRetry,
			wantObserved: false,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme(t)
			var c client.Client
			if tt.apiErr {
				fakeBase := fakeclient.NewClientBuilder().WithScheme(s).
					WithRuntimeObjects(tt.objects...).Build()
				c = interceptor.NewClient(fakeBase, interceptor.Funcs{
					Get: func(ctx context.Context, cc client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						return errors.New("api server unavailable")
					},
				})
			} else {
				c = fake.NewFakeClientWithScheme(s, tt.objects...)
			}

			decision, observed, err := ResolvePrecedingOperation(c, tt.ref, tt.defaultNS, tt.cached)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ResolvePrecedingOperation() err = %v, wantErr %v", err, tt.wantErr)
			}
			if decision != tt.wantDecision {
				t.Fatalf("ResolvePrecedingOperation() decision = %v, want %v", decision, tt.wantDecision)
			}
			if tt.wantObserved && observed == nil {
				t.Fatalf("expected a persisted observation, got nil")
			}
			if !tt.wantObserved && observed != nil {
				t.Fatalf("expected no observation, got %+v", observed)
			}
			if observed == nil {
				return
			}
			if observed.UID != tt.wantUID {
				t.Errorf("observed UID = %q, want %q", observed.UID, tt.wantUID)
			}
			if observed.ObservedPhase != tt.wantPhase {
				t.Errorf("observed phase = %q, want %q", observed.ObservedPhase, tt.wantPhase)
			}
			if tt.wantReason != "" && observed.Reason != tt.wantReason {
				t.Errorf("observed reason = %q, want %q", observed.Reason, tt.wantReason)
			}
			if (observed.NodeAffinity != nil) != tt.wantAffinity {
				t.Errorf("observed nodeAffinity presence = %v, want %v", observed.NodeAffinity != nil, tt.wantAffinity)
			}
			if tt.wantResolvedNS != "" && observed.Namespace != tt.wantResolvedNS {
				t.Errorf("observed namespace = %q, want %q", observed.Namespace, tt.wantResolvedNS)
			}
			if tt.wantReason == common.DataOperationIdentityConflict &&
				(observed.Message == "" ||
					!strings.Contains(observed.Message, string(testUID)) ||
					!strings.Contains(observed.Message, string(recreatedUID))) {
				t.Errorf("identity conflict message must mention both UIDs, got %q", observed.Message)
			}
		})
	}
}
