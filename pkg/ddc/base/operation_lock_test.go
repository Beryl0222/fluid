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

package base

import (
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// accessReleaserOperation is a minimal OperationInterface that also implements
// TargetDatasetAccessReleaser. Embedding the interface leaves unused methods panicking,
// which is fine because ReleaseTargetDataset only touches the overridden methods below.
type accessReleaserOperation struct {
	dataoperation.OperationInterface
	object   client.Object
	dataset  *datav1alpha1.Dataset
	released bool
}

func (a *accessReleaserOperation) GetOperationObject() client.Object { return a.object }

func (a *accessReleaserOperation) GetOperationType() dataoperation.OperationType {
	return dataoperation.DataMigrateType
}

func (a *accessReleaserOperation) GetTargetDataset() (*datav1alpha1.Dataset, error) {
	return a.dataset, nil
}

func (a *accessReleaserOperation) RemoveTargetDatasetStatusInProgress(*datav1alpha1.Dataset) {}

func (a *accessReleaserOperation) ReleaseTargetDatasetAccess(dataset *datav1alpha1.Dataset) {
	a.released = true
	dataset.RemoveDataMigrateRef(a.object.GetNamespace(), a.object.GetName())
}

var _ = Describe("ReleaseTargetDataset access releaser", func() {
	It("should invoke the access releaser even when other operations still hold the dataset", func() {
		dataset := &datav1alpha1.Dataset{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dataset", Namespace: "default"},
			Status: datav1alpha1.DatasetStatus{
				OperationRef: map[string]string{
					string(dataoperation.DataMigrateType): "m1,m2",
				},
				DataMigrateRefs: []datav1alpha1.DataMigrateRef{
					{Name: "m1", Namespace: "default", Block: true},
					{Name: "m2", Namespace: "default", Block: true},
				},
			},
		}

		s := runtime.NewScheme()
		s.AddKnownTypes(datav1alpha1.GroupVersion, &datav1alpha1.Dataset{})
		fakeClient := fake.NewFakeClientWithScheme(s, dataset.DeepCopy())

		migrateObject := &datav1alpha1.DataMigrate{
			ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "default"},
		}
		op := &accessReleaserOperation{
			object:  migrateObject,
			dataset: dataset,
		}

		ctx := cruntime.ReconcileRequestContext{
			Client: fakeClient,
			Log:    fake.NullLogger(),
		}

		Expect(ReleaseTargetDataset(ctx, op)).To(Succeed())
		Expect(op.released).To(BeTrue())

		updated := &datav1alpha1.Dataset{}
		Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(dataset), updated)).To(Succeed())

		// The generic in-progress phase hook is not run because m2 still holds the lock,
		// but the per-operation access state of m1 must have been released.
		Expect(updated.Status.OperationRef[string(dataoperation.DataMigrateType)]).To(Equal("m2"))
		Expect(updated.Status.DataMigrateRefs).To(HaveLen(1))
		Expect(updated.Status.DataMigrateRefs[0].Name).To(Equal("m2"))
	})
})

var _ = Describe("getDataOperationKey", func() {
	It("should return the object name", func() {
		obj := &datav1alpha1.DataBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "my-backup", Namespace: "default"},
		}
		Expect(getDataOperationKey(obj)).To(Equal("my-backup"))
	})

	It("should return empty string for object with no name", func() {
		obj := &datav1alpha1.DataBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "", Namespace: "default"},
		}
		Expect(getDataOperationKey(obj)).To(Equal(""))
	})

	It("should return empty string for nil object", func() {
		var obj *datav1alpha1.DataBackup = nil
		Expect(getDataOperationKey(obj)).To(Equal(""))
	})
})
