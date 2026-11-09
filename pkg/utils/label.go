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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ChangeNodeLabelWithUpdateMode updates the input labels in UPDATE mode.
func ChangeNodeLabelWithUpdateMode(client client.Client, node *v1.Node, labelsToModify common.LabelsToModify) (modifiedLabels []string, err error) {
	labels := labelsToModify.GetLabels()

	for _, labelToModify := range labels {
		oldLabels := node.Labels
		operationType := labelToModify.GetOperationType()
		labelToModifyKey := labelToModify.GetLabelKey()
		labelToModifyValue := labelToModify.GetLabelValue()

		switch operationType {
		case common.AddLabel:
			if _, exists := oldLabels[labelToModifyKey]; exists {
				err = fmt.Errorf("fail to add the label due to the label %s already exist", labelToModifyKey)
				return nil, err
			}
			node.Labels[labelToModifyKey] = labelToModifyValue
		case common.UpdateLabel:
			if _, exists := oldLabels[labelToModifyKey]; !exists {
				err = fmt.Errorf("fail to update the label due to the label %s does not exist", labelToModifyKey)
				return nil, err
			}
			node.Labels[labelToModifyKey] = labelToModifyValue
		case common.DeleteLabel:
			if _, exists := oldLabels[labelToModifyKey]; !exists {
				err = fmt.Errorf("fail to delete the label due to the label %s does not exist", labelToModifyKey)
				return nil, err
			}
			delete(node.Labels, labelToModifyKey)
		default:
			err = fmt.Errorf("fail to update the label due to the wrong operation: %s", operationType)
			return nil, err
		}
		modifiedLabels = append(modifiedLabels, labelToModifyKey)
	}
	err = client.Update(context.TODO(), node)
	if err != nil {
		return nil, errors.Wrapf(err, "update node labels failed, node name: %s, labels: %v", node.Name, node.Labels)
	}
	return modifiedLabels, nil
}

func PatchLabels(cli client.Client, obj client.Object, labelsToModify common.LabelsToModify) (modifiedLabels []string, err error) {
	labels := labelsToModify.GetLabels()
	labelValuePair := map[string]interface{}{}

	for _, labelToModify := range labels {
		operationType := labelToModify.GetOperationType()
		labelToModifyKey := labelToModify.GetLabelKey()
		labelToModifyValue := labelToModify.GetLabelValue()

		switch operationType {
		case common.AddLabel, common.UpdateLabel:
			labelValuePair[labelToModifyKey] = labelToModifyValue
		case common.DeleteLabel:
			labelValuePair[labelToModifyKey] = nil
		default:
			err = fmt.Errorf("fail to update the label due to the wrong operation: %s", operationType)
			return nil, err
		}
		modifiedLabels = append(modifiedLabels, labelToModifyKey)
	}

	metadata := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": labelValuePair,
		},
	}

	patchByteData, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	err = cli.Patch(context.TODO(), obj, client.RawPatch(types.StrategicMergePatchType, patchByteData))
	if err != nil {
		return nil, errors.Wrapf(err, "patch node labels failed, node name: %s, labels: %v", obj.GetName(), obj.GetLabels())
	}
	return modifiedLabels, nil
}

// ChangeNodeLabelWithPatchMode updates the input labels in PATCH mode.
func ChangeNodeLabelWithPatchMode(cli client.Client, node *v1.Node, labelsToModify common.LabelsToModify) (modifiedLabels []string, err error) {
	return PatchLabels(cli, node, labelsToModify)
}

func GetStorageLabelName(read common.ReadType, storage common.StorageType, runtimeType string, namespace, name, ownerDatasetUID string) string {
	prefix := common.LabelAnnotationStorageCapacityPrefix
	prefix = prefix + string(read) + runtimeType + "-" + string(storage)

	return GetNamespacedNameValueWithPrefix(prefix, namespace, name, ownerDatasetUID)
}

func GetLabelNameForMemory(runtimeType string, namespace, name, ownerDatasetUID string) string {
	read := common.HumanReadType
	storage := common.MemoryStorageType
	return GetStorageLabelName(read, storage, runtimeType, namespace, name, ownerDatasetUID)
}

func GetLabelNameForDisk(runtimeType string, namespace, name, ownerDatasetUID string) string {
	read := common.HumanReadType
	storage := common.DiskStorageType
	return GetStorageLabelName(read, storage, runtimeType, namespace, name, ownerDatasetUID)
}

func GetLabelNameForTotal(runtimeType string, namespace, name, ownerDatasetUID string) string {
	read := common.HumanReadType
	storage := common.TotalStorageType
	return GetStorageLabelName(read, storage, runtimeType, namespace, name, ownerDatasetUID)
}

func GetCommonLabelName(namespace, name, ownerDatasetUID string) string {
	prefix := common.LabelAnnotationStorageCapacityPrefix

	return GetNamespacedNameValueWithPrefix(prefix, namespace, name, ownerDatasetUID)
}

func GetRuntimeLabelName(runtimeType string, namespace, name, ownerDatasetUID string) string {
	prefix := common.LabelAnnotationStorageCapacityPrefix
	prefix = prefix + runtimeType + "-"

	return GetNamespacedNameValueWithPrefix(prefix, namespace, name, ownerDatasetUID)
}

func GetFuseLabelName(namespace, name, ownerDatasetUID string) string {
	return GetNamespacedNameValueWithPrefix(common.LabelAnnotationFusePrefix, namespace, name, ownerDatasetUID)
}

// FuseGenerationLabelSeparator is inserted between a runtime's fuse node selector label name and
// its fuse generation to build the generation-scoped selector label.
const FuseGenerationLabelSeparator = "-gen-"

// GetFuseGenerationLabelName builds the generation-scoped node selector label for a fuse runtime.
// For example, GetFuseGenerationLabelName("fluid.io/f-default-test", "2") results in
// "fluid.io/f-default-test-gen-2". The generation-scoped label allows the old and new generations
// of a runtime FUSE to be referenced independently on the same node during a rolling template
// switch. An error is returned when the resulting label key is not a qualified Kubernetes label
// key, so an unexpected generation value can never produce an invalid node patch.
func GetFuseGenerationLabelName(fuseLabelKey, generation string) (string, error) {
	if len(fuseLabelKey) == 0 || len(generation) == 0 {
		return "", errors.Errorf("fuseLabelKey and generation must not be empty, got fuseLabelKey=%q generation=%q", fuseLabelKey, generation)
	}

	prefix, name := "", fuseLabelKey
	if parts := strings.SplitN(fuseLabelKey, "/", 2); len(parts) == 2 {
		prefix, name = parts[0], parts[1]
	}

	generationLabelName := name + FuseGenerationLabelSeparator + generation
	if len(prefix) > 0 {
		if errs := validation.IsDNS1123Subdomain(prefix); len(errs) > 0 {
			return "", errors.Errorf("invalid label prefix %q derived from fuse label key %q: %v", prefix, fuseLabelKey, errs)
		}
	}
	if errs := validation.IsDNS1123Label(generationLabelName); len(errs) > 0 {
		return "", errors.Errorf("invalid generation label name %q derived from fuse label key %q: %v", generationLabelName, fuseLabelKey, errs)
	}

	if len(prefix) > 0 {
		return prefix + "/" + generationLabelName, nil
	}
	return generationLabelName, nil
}

// GetFuseGenerationLabelPrefix returns the key prefix shared by every generation-scoped selector
// label of the given runtime fuse label key. It is used to recognize the generation labels a node
// already carries, e.g. "fluid.io/f-default-test-gen-" for the fuse label key
// "fluid.io/f-default-test". The generation is the suffix of a matching label key.
func GetFuseGenerationLabelPrefix(fuseLabelKey string) string {
	prefix, name := "", fuseLabelKey
	if parts := strings.SplitN(fuseLabelKey, "/", 2); len(parts) == 2 {
		prefix, name = parts[0], parts[1]
	}

	generationLabelNamePrefix := name + FuseGenerationLabelSeparator
	if len(prefix) > 0 {
		return prefix + "/" + generationLabelNamePrefix
	}
	return generationLabelNamePrefix
}

func GetExclusiveKey() string {
	return common.FluidExclusiveKey
}

// GetNamespacedNameValueWithPrefix Transfer a fully namespaced name with a prefix to a legal value which under max length limit.
// If the full namespaced name exceeds 63 characters, it calculates the hash value of the name and truncates the name and namespace,
// then appends the hash value to ensure the name's uniqueness and length constraint.
func GetNamespacedNameValueWithPrefix(prefix, namespace, name, ownerDatasetUID string) (fullNamespacedNameWithPrefix string) {
	namespacedName := fmt.Sprintf("%s-%s", namespace, name)
	fullNamespacedNameWithPrefix = fmt.Sprintf("%s%s", prefix, namespacedName)
	// ensure forward compatibility
	if len(fullNamespacedNameWithPrefix) < validation.DNS1035LabelMaxLength {
		return
	}

	if ownerDatasetUID == "" {
		log.Info("The ownerDatasetUID is absent, fall back to original value which causes the resource creation failed by scheme validation", "key", fmt.Sprintf("%s-%s", namespace, name))
		return fullNamespacedNameWithPrefix
	}

	fullNamespacedNameWithPrefix = fmt.Sprintf("%s%s", prefix, ownerDatasetUID)

	return
}

func GetDatasetId(namespace, name, ownerDatasetUID string) (fullNamespacedNameWithPrefix string) {
	return GetNamespacedNameValueWithPrefix("", namespace, name, ownerDatasetUID)
}

func PatchLabelToObjects(cli client.Client, labelKey, labelValue string, objs ...client.Object) (errs []error) {
	for _, obj := range objs {
		if obj.GetLabels() == nil ||
			!ContainsLabel(obj.GetLabels(), common.LabelAnnotationDatasetId, labelValue) {
			labelValuePair := map[string]string{
				labelKey: labelValue,
			}

			metadata := map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": labelValuePair,
				},
			}

			patchByteData, err := json.Marshal(metadata)
			if err != nil {
				errs = append(errs, err)
				continue
			}

			err = cli.Patch(context.TODO(), obj, client.RawPatch(types.MergePatchType, patchByteData))
			if err != nil {
				log.Error(err, "failed to patch labels", "objKind", obj.GetObjectKind(), "name", obj.GetName(), "labels", obj.GetLabels())
				errs = append(errs, err)
				continue
			}
		}
	}
	return
}
