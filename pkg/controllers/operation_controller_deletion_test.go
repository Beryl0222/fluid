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

package controllers

import (
	"context"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/dataoperation"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
)

var _ = Describe("OperationReconciler ReconcileDeletion generation convergence", func() {
	const (
		finalizerName = "fluid.io/dataload-finalizer"
		releaseName   = "load-loader"
		namespace     = "default"
	)

	var (
		s        *runtime.Scheme
		recorder *record.FakeRecorder
	)

	BeforeEach(func() {
		s = runtime.NewScheme()
		Expect(datav1alpha1.AddToScheme(s)).To(Succeed())
		recorder = record.NewFakeRecorder(16)
	})

	newDeletingObject := func() *datav1alpha1.DataLoad {
		return &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "load",
				Namespace:         namespace,
				Finalizers:        []string{finalizerName},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
		}
	}

	It("deletes the stable release, releases the accepted dataset lock and removes the finalizer", func() {
		obj := newDeletingObject()
		dataset := &datav1alpha1.Dataset{
			ObjectMeta: metav1.ObjectMeta{Name: "frozen-ds", Namespace: namespace},
			Status: datav1alpha1.DatasetStatus{
				OperationRef: map[string]string{string(dataoperation.DataLoadType): "load"},
			},
		}
		c := fake.NewFakeClientWithScheme(s, obj, dataset)
		reconciler := NewDataOperationReconciler(
			&mockOperationInterfaceBuilder{
				buildFunc: func(object client.Object) (dataoperation.OperationInterface, error) {
					return &mockOperationInterface{
						operationObject:                      obj,
						releaseNamespacedName:                types.NamespacedName{Namespace: namespace, Name: releaseName},
						targetDataset:                        dataset,
						possibleTargetDatasetNamespacedNames: []types.NamespacedName{{Namespace: namespace, Name: "frozen-ds"}},
						operationType:                        dataoperation.DataLoadType,
					}, nil
				},
			}, c, fake.NullLogger(), recorder)

		var deletedReleaseName, deletedReleaseNamespace string
		patch := gomonkey.ApplyFunc(helm.DeleteReleaseIfExists, func(name, ns string) error {
			deletedReleaseName, deletedReleaseNamespace = name, ns
			return nil
		})
		defer patch.Reset()

		ctx := dataoperation.ReconcileRequestContext{
			ReconcileRequestContext: cruntime.ReconcileRequestContext{
				Context: context.Background(),
				Client:  c,
				Log:     fake.NullLogger(),
			},
			DataOpFinalizerName: finalizerName,
		}
		implement, err := reconciler.implementBuilder.Build(obj)
		Expect(err).NotTo(HaveOccurred())

		result, err := reconciler.ReconcileDeletion(ctx, implement)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())

		By("the helm release is deleted using the object-name-derived stable identity")
		Expect(deletedReleaseName).To(Equal(releaseName))
		Expect(deletedReleaseNamespace).To(Equal(namespace))

		By("the dataset operation lock is released")
		updatedDataset := &datav1alpha1.Dataset{}
		Expect(c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: "frozen-ds"}, updatedDataset)).To(Succeed())
		Expect(updatedDataset.Status.OperationRef).ToNot(HaveKey(string(dataoperation.DataLoadType)))

		By("the finalizer removal lets the deleting object be garbage collected")
		updatedObj := &datav1alpha1.DataLoad{}
		err = c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: "load"}, updatedObj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "object should be removed once its last finalizer is gone")
	})

	It("still converges when the accepted target dataset was cascade-deleted (NotFound)", func() {
		obj := newDeletingObject()
		c := fake.NewFakeClientWithScheme(s, obj)
		reconciler := NewDataOperationReconciler(
			&mockOperationInterfaceBuilder{
				buildFunc: func(object client.Object) (dataoperation.OperationInterface, error) {
					return &mockOperationInterface{
						operationObject:                      obj,
						releaseNamespacedName:                types.NamespacedName{Namespace: namespace, Name: releaseName},
						targetDatasetErr:                     apierrors.NewNotFound(schema.GroupResource{Group: "data.fluid.io", Resource: "datasets"}, "gone"),
						possibleTargetDatasetNamespacedNames: []types.NamespacedName{{Namespace: namespace, Name: "gone"}},
						operationType:                        dataoperation.DataLoadType,
					}, nil
				},
			}, c, fake.NullLogger(), recorder)

		helmCalled := false
		patch := gomonkey.ApplyFunc(helm.DeleteReleaseIfExists, func(name, ns string) error {
			helmCalled = true
			return nil
		})
		defer patch.Reset()

		ctx := dataoperation.ReconcileRequestContext{
			ReconcileRequestContext: cruntime.ReconcileRequestContext{
				Context: context.Background(),
				Client:  c,
				Log:     fake.NullLogger(),
			},
			DataOpFinalizerName: finalizerName,
		}
		implement, err := reconciler.implementBuilder.Build(obj)
		Expect(err).NotTo(HaveOccurred())

		result, err := reconciler.ReconcileDeletion(ctx, implement)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())
		Expect(helmCalled).To(BeTrue())

		updatedObj := &datav1alpha1.DataLoad{}
		err = c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: "load"}, updatedObj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "finalizer removal allows the deleting object to be collected")
	})

	It("does not block finalizer removal when the accepted-spec annotation is corrupt", func() {
		obj := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "load",
				Namespace:         namespace,
				Finalizers:        []string{finalizerName},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Annotations:       map[string]string{dataoperation.AcceptedSpecAnnotation: "{not-json"},
			},
		}
		c := fake.NewFakeClientWithScheme(s, obj)
		reconciler := NewDataOperationReconciler(
			&mockOperationInterfaceBuilder{
				buildFunc: func(object client.Object) (dataoperation.OperationInterface, error) {
					return &mockOperationInterface{
						operationObject:                      obj,
						releaseNamespacedName:                types.NamespacedName{Namespace: namespace, Name: releaseName},
						targetDatasetErr:                     apierrors.NewNotFound(schema.GroupResource{Group: "data.fluid.io", Resource: "datasets"}, "gone"),
						possibleTargetDatasetNamespacedNames: []types.NamespacedName{{Namespace: namespace, Name: "gone"}},
						operationType:                        dataoperation.DataLoadType,
					}, nil
				},
			}, c, fake.NullLogger(), recorder)

		helmCalled := false
		patch := gomonkey.ApplyFunc(helm.DeleteReleaseIfExists, func(name, ns string) error {
			helmCalled = true
			return nil
		})
		defer patch.Reset()

		By("Build tolerates the corrupt annotation and deletion still converges")
		implement, err := reconciler.implementBuilder.Build(obj)
		Expect(err).NotTo(HaveOccurred())

		ctx := dataoperation.ReconcileRequestContext{
			ReconcileRequestContext: cruntime.ReconcileRequestContext{
				Context: context.Background(),
				Client:  c,
				Log:     fake.NullLogger(),
			},
			DataOpFinalizerName: finalizerName,
		}
		result, err := reconciler.ReconcileDeletion(ctx, implement)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeFalse())
		Expect(helmCalled).To(BeTrue())

		updatedObj := &datav1alpha1.DataLoad{}
		err = c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: "load"}, updatedObj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the object is collected despite the corrupt annotation")
	})
})
