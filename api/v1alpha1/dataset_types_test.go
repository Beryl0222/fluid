/*
  Copyright 2023 The Fluid Authors.

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

package v1alpha1

import (
	"testing"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDataset_RemoveDataOperationInProgress(t *testing.T) {
	type fields struct {
		TypeMeta   v1.TypeMeta
		ObjectMeta v1.ObjectMeta
		Spec       DatasetSpec
		Status     DatasetStatus
	}
	type args struct {
		operationType string
		name          string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "test1",
			fields: fields{
				Status: DatasetStatus{
					OperationRef: map[string]string{
						"DataLoad": "test1",
					},
				},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test1",
			},
			want: "",
		},
		{
			name: "test2",
			fields: fields{
				Status: DatasetStatus{
					OperationRef: map[string]string{
						"DataLoad": "test1,test2",
					},
				},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test1",
			},
			want: "test2",
		},
		{
			name: "test3",
			fields: fields{
				Status: DatasetStatus{},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test1",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset := &Dataset{
				TypeMeta:   tt.fields.TypeMeta,
				ObjectMeta: tt.fields.ObjectMeta,
				Spec:       tt.fields.Spec,
				Status:     tt.fields.Status,
			}
			if got := dataset.RemoveDataOperationInProgress(tt.args.operationType, tt.args.name); got != tt.want {
				t.Errorf("RemoveDataOperationInProgress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataset_SetDataOperationInProgress(t *testing.T) {
	type fields struct {
		TypeMeta   v1.TypeMeta
		ObjectMeta v1.ObjectMeta
		Spec       DatasetSpec
		Status     DatasetStatus
	}
	type args struct {
		operationType string
		name          string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "test1",
			fields: fields{
				Status: DatasetStatus{},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test1",
			},
			want: "test1",
		},
		{
			name: "test2",
			fields: fields{
				Status: DatasetStatus{
					OperationRef: map[string]string{
						"DataLoad": "test1",
					},
				},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test2",
			},
			want: "test1,test2",
		},
		{
			name: "test3",
			fields: fields{
				Status: DatasetStatus{
					OperationRef: map[string]string{
						"DataLoad": "test1",
					},
				},
			},
			args: args{
				operationType: "DataMigrate",
				name:          "test",
			},
			want: "test",
		},
		{
			name: "test4",
			fields: fields{
				Status: DatasetStatus{
					OperationRef: map[string]string{
						"DataLoad": "test",
					},
				},
			},
			args: args{
				operationType: "DataLoad",
				name:          "test",
			},
			want: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset := &Dataset{
				TypeMeta:   tt.fields.TypeMeta,
				ObjectMeta: tt.fields.ObjectMeta,
				Spec:       tt.fields.Spec,
				Status:     tt.fields.Status,
			}
			dataset.SetDataOperationInProgress(tt.args.operationType, tt.args.name)
			if got := dataset.GetDataOperationInProgress(tt.args.operationType); got != tt.want {
				t.Errorf("SetDataOperationInProgress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataset_DataMigrateRefLifecycle(t *testing.T) {
	blockingRef := DataMigrateRef{Name: "migrate-block", Namespace: "fluid", Block: true}
	nonBlockingRef := DataMigrateRef{Name: "migrate-pass", Namespace: "fluid", Block: false}

	t.Run("set and upsert keeps one entry per migration identity", func(t *testing.T) {
		dataset := &Dataset{}
		dataset.SetDataMigrateRef(blockingRef)
		dataset.SetDataMigrateRef(nonBlockingRef)
		if len(dataset.Status.DataMigrateRefs) != 2 {
			t.Fatalf("expected 2 refs, got %d", len(dataset.Status.DataMigrateRefs))
		}

		// upsert the same identity with a changed block flag, no new entry
		dataset.SetDataMigrateRef(DataMigrateRef{Name: "migrate-pass", Namespace: "fluid", Block: true})
		if len(dataset.Status.DataMigrateRefs) != 2 {
			t.Fatalf("expected still 2 refs after upsert, got %d", len(dataset.Status.DataMigrateRefs))
		}
		blocked, blockers := dataset.IsDataAccessBlocked()
		if !blocked || len(blockers) != 2 {
			t.Fatalf("expected 2 blockers, got blocked=%v blockers=%v", blocked, blockers)
		}
	})

	t.Run("remove only the targeted migration entry", func(t *testing.T) {
		dataset := &Dataset{}
		dataset.SetDataMigrateRef(blockingRef)
		dataset.SetDataMigrateRef(nonBlockingRef)

		dataset.RemoveDataMigrateRef("fluid", "migrate-block")
		blocked, blockers := dataset.IsDataAccessBlocked()
		if blocked {
			t.Fatalf("expected no blocker after removal, got %v", blockers)
		}
		if len(dataset.Status.DataMigrateRefs) != 1 || dataset.Status.DataMigrateRefs[0].Name != "migrate-pass" {
			t.Fatalf("unexpected refs after removal: %v", dataset.Status.DataMigrateRefs)
		}

		// removing again is a no-op
		dataset.RemoveDataMigrateRef("fluid", "migrate-block")
		if len(dataset.Status.DataMigrateRefs) != 1 {
			t.Fatalf("removal should be idempotent")
		}
	})

	t.Run("non-blocking entry never blocks access", func(t *testing.T) {
		dataset := &Dataset{}
		dataset.SetDataMigrateRef(nonBlockingRef)
		blocked, _ := dataset.IsDataAccessBlocked()
		if blocked {
			t.Fatalf("a block=false migration must not block dataset access")
		}
	})

	t.Run("empty status reports not blocked", func(t *testing.T) {
		dataset := &Dataset{}
		blocked, blockers := dataset.IsDataAccessBlocked()
		if blocked || blockers != nil {
			t.Fatalf("fresh dataset should not be blocked")
		}
	})
}
