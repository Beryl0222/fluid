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

package utils

import (
	"context"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
)

func TestGetDataMigrate(t *testing.T) {
	mockDataMigrateName := "fluid-test-data-migrate"
	mockDataMigrateNamespace := "default"
	initDataMigrate := &datav1alpha1.DataMigrate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mockDataMigrateName,
			Namespace: mockDataMigrateNamespace,
		},
	}

	s := runtime.NewScheme()
	s.AddKnownTypes(datav1alpha1.GroupVersion, initDataMigrate)

	fakeClient := fake.NewFakeClientWithScheme(s, initDataMigrate)

	testCases := map[string]struct {
		name      string
		namespace string
		wantName  string
		notFound  bool
	}{
		"test get DataMigrate case 1": {
			name:      mockDataMigrateName,
			namespace: mockDataMigrateNamespace,
			wantName:  mockDataMigrateName,
			notFound:  false,
		},
		"test get DataMigrate case 2": {
			name:      mockDataMigrateName + "not-exist",
			namespace: mockDataMigrateNamespace,
			wantName:  "",
			notFound:  true,
		},
	}

	for k, item := range testCases {
		gotDataMigrate, err := GetDataMigrate(fakeClient, item.name, item.namespace)
		if item.notFound {
			if err == nil && gotDataMigrate != nil {
				t.Errorf("%s check failure, want get err, but get nil", k)
			}
		} else {
			if gotDataMigrate.Name != item.wantName {
				t.Errorf("%s check failure, want DataMigrate name:%s, got DataMigrate name:%s", k, item.wantName, gotDataMigrate.Name)
			}
		}
	}
}

func TestGetDataMigrateJobName(t *testing.T) {
	type args struct {
		releaseName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test",
			args: args{
				releaseName: "test",
			},
			want: "test-migrate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDataMigrateJobName(tt.args.releaseName); got != tt.want {
				t.Errorf("GetDataMigrateJobName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDataMigrateReleaseName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test",
			args: args{
				name: "test",
			},
			want: "test-migrate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDataMigrateReleaseName(tt.args.name); got != tt.want {
				t.Errorf("GetDataMigrateReleaseName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTargetDatasetOfMigrate(t *testing.T) {
	dataSet := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset",
			Namespace: "fluid",
		},
		Status: datav1alpha1.DatasetStatus{
			Runtimes: []datav1alpha1.Runtime{
				{
					Name:      "juicefs-runtime",
					Namespace: "fluid",
					Type:      "juicefs",
					Category:  common.AccelerateCategory,
				},
			},
		},
	}
	juicefsRuntime := &datav1alpha1.JuiceFSRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset",
			Namespace: "fluid",
		},
	}
	runtimeObjs := []runtime.Object{}
	runtimeObjs = append(runtimeObjs, dataSet.DeepCopy(), juicefsRuntime.DeepCopy())
	fakeClient := fake.NewFakeClientWithScheme(testScheme, runtimeObjs...)

	type args struct {
		dataMigrate *datav1alpha1.DataMigrate
	}
	tests := []struct {
		name        string
		args        args
		wantDataset *datav1alpha1.Dataset
		wantErr     bool
	}{
		{
			name: "test-from",
			args: args{
				dataMigrate: &datav1alpha1.DataMigrate{
					Spec: datav1alpha1.DataMigrateSpec{
						From: datav1alpha1.DataToMigrate{
							DataSet: &datav1alpha1.DatasetToMigrate{
								Name:      "test-dataset",
								Namespace: "fluid",
							},
						},
					},
				},
			},
			wantDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
			},
			wantErr: false,
		},
		{
			name: "test-to",
			args: args{
				dataMigrate: &datav1alpha1.DataMigrate{
					Spec: datav1alpha1.DataMigrateSpec{
						To: datav1alpha1.DataToMigrate{
							DataSet: &datav1alpha1.DatasetToMigrate{
								Name:      "test-dataset",
								Namespace: "fluid",
							},
						},
					},
				},
			},
			wantDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
			},
			wantErr: false,
		},
		{
			name: "test-not-exist",
			args: args{
				dataMigrate: &datav1alpha1.DataMigrate{
					Spec: datav1alpha1.DataMigrateSpec{
						To: datav1alpha1.DataToMigrate{
							DataSet: &datav1alpha1.DatasetToMigrate{
								Name:      "not-exist-dataset",
								Namespace: "fluid",
							},
						},
					},
				},
			},
			wantDataset: nil,
			wantErr:     true,
		},
		{
			name: "test-to-type",
			args: args{
				dataMigrate: &datav1alpha1.DataMigrate{
					Spec: datav1alpha1.DataMigrateSpec{
						To: datav1alpha1.DataToMigrate{
							DataSet: &datav1alpha1.DatasetToMigrate{
								Name:      "test-dataset",
								Namespace: "fluid",
							},
						},
						RuntimeType: "juicefs",
					},
				},
			},
			wantDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
			},
			wantErr: false,
		},
		{
			name: "test-wrong-type",
			args: args{
				dataMigrate: &datav1alpha1.DataMigrate{
					Spec: datav1alpha1.DataMigrateSpec{
						To: datav1alpha1.DataToMigrate{
							DataSet: &datav1alpha1.DatasetToMigrate{
								Name:      "test-dataset",
								Namespace: "fluid",
							},
						},
						RuntimeType: "not-exist-runtime",
					},
				},
			},
			wantDataset: nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDataset, err := GetTargetDatasetOfMigrate(fakeClient, tt.args.dataMigrate)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTargetDatasetOfMigrate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDataset != nil || tt.wantDataset != nil {
				if gotDataset.Name != tt.wantDataset.Name || gotDataset.Namespace != tt.wantDataset.Namespace {
					t.Errorf("GetTargetDatasetOfMigrate() gotDataset = %v, want %v", gotDataset, tt.wantDataset)
				}
			}
		})
	}
}

// failingReader is a client.Reader that always returns the injected error, used to
// verify that transient API errors fail closed instead of opening dataset access.
type failingReader struct {
	err error
}

func (f *failingReader) Get(_ context.Context, _ types.NamespacedName, _ client.Object, _ ...client.GetOption) error {
	return f.err
}

func (f *failingReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return f.err
}

func TestCheckDatasetDataAccessBlocked(t *testing.T) {
	const namespace, name = "fluid", "test-dataset"

	blockedDataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status: datav1alpha1.DatasetStatus{
			DataMigrateRefs: []datav1alpha1.DataMigrateRef{
				{Name: "migration-job", Namespace: namespace, Block: true},
			},
		},
	}
	nonBlockedDataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status: datav1alpha1.DatasetStatus{
			DataMigrateRefs: []datav1alpha1.DataMigrateRef{
				{Name: "migration-job", Namespace: namespace, Block: false},
			},
		},
	}

	t.Run("explicit block returns a typed block error", func(t *testing.T) {
		c := fake.NewFakeClientWithScheme(testScheme, blockedDataset.DeepCopy())
		err := CheckDatasetDataAccessBlocked(c, namespace, name)
		if err == nil {
			t.Fatal("expected block error, got nil")
		}
		blockedErr, ok := err.(*DatasetAccessBlockedError)
		if !ok {
			t.Fatalf("expected *DatasetAccessBlockedError, got %T: %v", err, err)
		}
		if blockedErr.Name != name || blockedErr.Namespace != namespace {
			t.Fatalf("unexpected error target: %+v", blockedErr)
		}
		if len(blockedErr.Blockers) != 1 || blockedErr.Blockers[0].Name != "migration-job" {
			t.Fatalf("unexpected blockers: %v", blockedErr.Blockers)
		}
	})

	t.Run("block=false migration allows access", func(t *testing.T) {
		c := fake.NewFakeClientWithScheme(testScheme, nonBlockedDataset.DeepCopy())
		if err := CheckDatasetDataAccessBlocked(c, namespace, name); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("missing dataset opens nothing but does not block", func(t *testing.T) {
		c := fake.NewFakeClientWithScheme(testScheme)
		if err := CheckDatasetDataAccessBlocked(c, namespace, "gone-dataset"); err != nil {
			t.Fatalf("expected nil for a deleted dataset, got %v", err)
		}
	})

	t.Run("nil reader fails with an error", func(t *testing.T) {
		if err := CheckDatasetDataAccessBlocked(nil, namespace, name); err == nil {
			t.Fatal("expected error for nil reader")
		}
	})

	t.Run("transient API error fails closed", func(t *testing.T) {
		reader := &failingReader{err: apierrors.NewInternalError(fmt.Errorf("api server unavailable"))}
		err := CheckDatasetDataAccessBlocked(reader, namespace, name)
		if err == nil {
			t.Fatal("expected error when dataset cannot be read")
		}
		if _, ok := err.(*DatasetAccessBlockedError); ok {
			t.Fatalf("transient errors must not be misreported as block: %v", err)
		}
	})
}
