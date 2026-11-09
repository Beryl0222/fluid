/*
  Copyright 2024 The Fluid Authors.

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

import "github.com/fluid-cloudnative/fluid/pkg/common"

const (
	// OperationLabel is used for pod anti affinity.
	OperationLabel = "fluid.io/operation"

	// AcceptedSpecAnnotation stores the spec snapshot accepted by the current execution
	// generation together with the metadata.generation at which the spec was accepted.
	// Its value is a JSON-encoded acceptedSpecPayload (see spec_binding.go). The annotation
	// binds a data operation to one immutable spec from the moment it acquires the target
	// dataset lock until the generation reaches its terminal state and is cleaned up.
	AcceptedSpecAnnotation = "dataoperation." + common.LabelAnnotationPrefix + "accepted-spec"
)
