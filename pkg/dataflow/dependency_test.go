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
	"testing"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1.AddToScheme(s)
	_ = datav1alpha1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	return s
}

func sampleNodeAffinity() *v1.NodeAffinity {
	return &v1.NodeAffinity{
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
}

func completeRecord(ref datav1alpha1.ObjectRef) *datav1alpha1.DependencyStatus {
	return &datav1alpha1.DependencyStatus{
		Ref:          ref,
		UID:          types.UID("recorded-uid"),
		Phase:        common.PhaseComplete,
		NodeAffinity: sampleNodeAffinity(),
	}
}

func TestResolveDependencyRef(t *testing.T) {
	ref := &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op"}
	resolved := ResolveDependencyRef(ref, "default")
	if resolved.Namespace != "default" {
		t.Errorf("expected namespace defaulted to default, got %q", resolved.Namespace)
	}

	refWithNs := &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op", Namespace: "other"}
	resolved = ResolveDependencyRef(refWithNs, "default")
	if resolved.Namespace != "other" {
		t.Errorf("expected explicit namespace other to be kept, got %q", resolved.Namespace)
	}
}

func TestSameObjectRefAndRecordHelpers(t *testing.T) {
	refA := datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op", Namespace: "default"}
	refB := datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op", Namespace: "default"}
	refC := datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op", Namespace: "other"}
	if !SameObjectRef(refA, refB) {
		t.Errorf("refs %v and %v should point to the same object", refA, refB)
	}
	if SameObjectRef(refA, refC) {
		t.Errorf("refs in different namespaces must not be treated as the same object")
	}

	records := []datav1alpha1.DependencyStatus{{Ref: refA, UID: types.UID("uid-a"), Phase: common.PhaseComplete}}
	found := FindDependencyRecord(records, &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op"}, "default")
	if found == nil || found.UID != types.UID("uid-a") {
		t.Errorf("expected to find persisted record, got %v", found)
	}
	if missing := FindDependencyRecord(records, &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "op"}, "other"); missing != nil {
		t.Errorf("cross-namespace lookup must not match, got %v", missing)
	}

	upserted := UpsertDependencyRecord(nil, &datav1alpha1.DependencyStatus{Ref: refC, UID: types.UID("uid-c")})
	if len(upserted) != 1 {
		t.Fatalf("expected one record after insert, got %d", len(upserted))
	}
	upserted = UpsertDependencyRecord(upserted, &datav1alpha1.DependencyStatus{Ref: refA, UID: types.UID("uid-a2")})
	if len(upserted) != 2 {
		t.Fatalf("different ref must append, got %d records", len(upserted))
	}
	upserted = UpsertDependencyRecord(upserted, &datav1alpha1.DependencyStatus{Ref: refA, UID: types.UID("uid-a3")})
	if len(upserted) != 2 {
		t.Fatalf("same ref must replace in place, got %d records", len(upserted))
	}
	if got := FindDependencyRecord(upserted, &refA, "default"); got.UID != types.UID("uid-a3") {
		t.Errorf("expected replaced record uid-a3, got %q", got.UID)
	}
	if UpsertDependencyRecord(upserted, nil) != nil && len(UpsertDependencyRecord(upserted, nil)) != 2 {
		t.Errorf("nil record must be ignored")
	}
}

func TestCheckPrecedingOperation(t *testing.T) {
	ref := &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: "default"}

	tests := []struct {
		name           string
		recorded       *datav1alpha1.DependencyStatus
		objects        []runtime.Object
		client         func(s *runtime.Scheme) client.Client
		wantVerdict    DependencyVerdict
		wantErr        bool
		wantRecordUID  types.UID
		wantRecordPhase common.Phase
	}{
		{
			name: "live complete observed for the first time",
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("uid-a")},
					Status: datav1alpha1.OperationStatus{
						Phase:        common.PhaseComplete,
						NodeAffinity: sampleNodeAffinity(),
						Conditions: []datav1alpha1.Condition{
							{Type: common.Complete, Status: v1.ConditionTrue, Reason: "JobComplete", Message: "done"},
						},
					},
				},
			},
			wantVerdict:     DependencySucceeded,
			wantRecordUID:   types.UID("uid-a"),
			wantRecordPhase: common.PhaseComplete,
		},
		{
			name: "live failed produces a stable failed verdict with diagnostics",
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("uid-a")},
					Status: datav1alpha1.OperationStatus{
						Phase: common.PhaseFailed,
						Conditions: []datav1alpha1.Condition{
							{Type: common.Failed, Status: v1.ConditionTrue, Reason: "JobFailed", Message: "boom"},
						},
					},
				},
			},
			wantVerdict:     DependencyFailed,
			wantErr:         true,
			wantRecordUID:   types.UID("uid-a"),
			wantRecordPhase: common.PhaseFailed,
		},
		{
			name: "live non-terminal phase pins identity and keeps waiting",
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("uid-a")},
					Status:       datav1alpha1.OperationStatus{Phase: common.PhaseExecuting},
				},
			},
			wantVerdict:     DependencyWaiting,
			wantRecordUID:   types.UID("uid-a"),
			wantRecordPhase: common.PhaseExecuting,
		},
		{
			name:        "not found without any record is unknown and waits",
			wantVerdict: DependencyWaiting,
			wantErr:     true,
		},
		{
			name:        "not found after confirmed complete trusts the terminal record",
			recorded:    completeRecord(*ref),
			wantVerdict: DependencySucceeded,
			wantErr:     false,
			wantRecordUID:   types.UID("recorded-uid"),
			wantRecordPhase: common.PhaseComplete,
		},
		{
			name: "not found after confirmed failed keeps the stable failure",
			recorded: &datav1alpha1.DependencyStatus{
				Ref:     *ref,
				UID:     types.UID("recorded-uid"),
				Phase:   common.PhaseFailed,
				Reason:  "JobFailed",
				Message: "boom",
			},
			wantVerdict:     DependencyFailed,
			wantErr:         true,
			wantRecordUID:   types.UID("recorded-uid"),
			wantRecordPhase: common.PhaseFailed,
		},
		{
			name: "not found with identity pinned but no terminal state stays unknown",
			recorded: &datav1alpha1.DependencyStatus{
				Ref:   *ref,
				UID:   types.UID("recorded-uid"),
				Phase: common.PhasePending,
			},
			wantVerdict:     DependencyWaiting,
			wantErr:         true,
			wantRecordUID:   types.UID("recorded-uid"),
			wantRecordPhase: common.PhasePending,
		},
		{
			name: "same-name object with different UID is an identity conflict",
			recorded: &datav1alpha1.DependencyStatus{
				Ref:   *ref,
				UID:   types.UID("recorded-uid"),
				Phase: common.PhaseComplete,
			},
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("impostor-uid")},
					Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
				},
			},
			wantVerdict:     DependencyConflict,
			wantErr:         true,
			wantRecordUID:   types.UID("recorded-uid"),
			wantRecordPhase: common.PhaseComplete,
		},
		{
			name: "same UID as the record with complete phase succeeds",
			recorded: &datav1alpha1.DependencyStatus{
				Ref:   *ref,
				UID:   types.UID("uid-a"),
				Phase: common.PhaseComplete,
			},
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("uid-a")},
					Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete, NodeAffinity: sampleNodeAffinity()},
				},
			},
			wantVerdict:     DependencySucceeded,
			wantRecordUID:   types.UID("uid-a"),
			wantRecordPhase: common.PhaseComplete,
		},
		{
			name: "transient API error only retries and keeps the record untouched",
			client: func(s *runtime.Scheme) client.Client {
				injectErr := errors.New("server unavailable")
				fakeBase := fakeclient.NewClientBuilder().WithScheme(s).Build()
				return interceptor.NewClient(fakeBase, interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						return injectErr
					},
				})
			},
			recorded:        completeRecord(*ref),
			wantVerdict:     DependencyWaiting,
			wantErr:         true,
			wantRecordUID:   types.UID("recorded-uid"),
			wantRecordPhase: common.PhaseComplete,
		},
		{
			name: "unsupported kind is retryable rather than a stable failure",
			objects: []runtime.Object{
				&datav1alpha1.DataLoad{
					ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "default", UID: types.UID("uid-a")},
				},
			},
			wantVerdict: DependencyWaiting,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme()
			var c client.Client
			if tt.client != nil {
				c = tt.client(s)
			} else {
				c = fake.NewFakeClientWithScheme(s, tt.objects...)
			}

			queryRef := ref
			if tt.name == "unsupported kind is retryable rather than a stable failure" {
				queryRef = &datav1alpha1.ObjectRef{Kind: "UnknownKind", Name: "preceding", Namespace: "default"}
			}

			result := CheckPrecedingOperation(c, queryRef, "default", tt.recorded)
			if result.Verdict != tt.wantVerdict {
				t.Errorf("verdict = %q, want %q (err=%v)", result.Verdict, tt.wantVerdict, result.Err)
			}
			if (result.Err != nil) != tt.wantErr {
				t.Errorf("err presence = %v, want %v, err=%v", result.Err != nil, tt.wantErr, result.Err)
			}
			if tt.wantRecordUID != "" || tt.wantRecordPhase != "" {
				if result.Record == nil {
					t.Fatalf("expected a non-nil record for %s", tt.name)
				}
				if tt.wantRecordUID != "" && result.Record.UID != tt.wantRecordUID {
					t.Errorf("record UID = %q, want %q", result.Record.UID, tt.wantRecordUID)
				}
				if tt.wantRecordPhase != "" && result.Record.Phase != tt.wantRecordPhase {
					t.Errorf("record phase = %q, want %q", result.Record.Phase, tt.wantRecordPhase)
				}
			}
			if result.Verdict == DependencyConflict && (result.Record == nil || result.Record.UID != types.UID("recorded-uid")) {
				t.Errorf("conflict must preserve the original record, got %v", result.Record)
			}
		})
	}
}

func TestCheckPrecedingOperationCrossNamespace(t *testing.T) {
	s := testScheme()
	c := fake.NewFakeClientWithScheme(s,
		&datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{Name: "preceding", Namespace: "other-ns", UID: types.UID("uid-x")},
			Status:       datav1alpha1.OperationStatus{Phase: common.PhaseComplete},
		},
	)

	ref := &datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "preceding", Namespace: "other-ns"}
	result := CheckPrecedingOperation(c, ref, "default", nil)
	if result.Verdict != DependencySucceeded {
		t.Fatalf("expected Succeeded for cross-namespace dependency, got %q (err=%v)", result.Verdict, result.Err)
	}
	if result.Record.Ref.Namespace != "other-ns" {
		t.Errorf("record must persist the resolved namespace, got %q", result.Record.Ref.Namespace)
	}
}

func TestCheckPrecedingOperationNotFoundIsAPIError(t *testing.T) {
	s := testScheme()
	c := fake.NewFakeClientWithScheme(s)
	result := CheckPrecedingOperation(c,
		&datav1alpha1.ObjectRef{Kind: "DataLoad", Name: "ghost", Namespace: "default"},
		"default", nil)
	if result.Verdict != DependencyWaiting || !apierrors.IsNotFound(result.Err) {
		t.Errorf("expected Waiting with a NotFound API error, got verdict=%q err=%v", result.Verdict, result.Err)
	}
}
