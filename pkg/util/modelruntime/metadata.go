// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package modelruntime

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/yalue/onnxruntime_go"
)

// TensorInfo captures ONNX input/output metadata relevant to TiDB.
type TensorInfo struct {
	Name        string
	ElementType onnxruntime_go.TensorElementDataType
	Shape       onnxruntime_go.Shape
}

var getInputOutputInfoFn = onnxruntime_go.GetInputOutputInfoWithONNXData
var getModelMetadataFn = func(onnxData []byte) (ModelMetadata, error) {
	return onnxruntime_go.GetModelMetadataWithONNXData(onnxData)
}

// ModelMetadata exposes ONNX custom metadata for inspection.
type ModelMetadata interface {
	Destroy() error
	LookupCustomMetadataMap(key string) (string, bool, error)
}

// InspectModelIOInfo returns ONNX model input/output metadata.
// It validates tensor-only IO and enforces FP32 element types.
func InspectModelIOInfo(onnxData []byte) (inputs []TensorInfo, outputs []TensorInfo, err error) {
	inputsInfo, outputsInfo, err := getInputOutputInfoFn(onnxData)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	parsedInputs, err := convertTensorInfo(inputsInfo)
	if err != nil {
		return nil, nil, err
	}
	parsedOutputs, err := convertTensorInfo(outputsInfo)
	if err != nil {
		return nil, nil, err
	}
	return parsedInputs, parsedOutputs, nil
}

// ModelDeclaresNondeterministic checks model metadata for nondeterminism hints.
// It returns false when metadata is unavailable to keep best-effort behavior.
func ModelDeclaresNondeterministic(onnxData []byte) (nondeterministic bool, err error) {
	meta, err := getModelMetadataFn(onnxData)
	if err != nil {
		return false, nil
	}
	defer func() {
		if destroyErr := meta.Destroy(); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx metadata")
		}
	}()
	val, ok, err := meta.LookupCustomMetadataMap("tidb_nondeterministic")
	if err != nil || !ok {
		return false, nil
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "true", "1", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func convertTensorInfo(items []onnxruntime_go.InputOutputInfo) ([]TensorInfo, error) {
	info := make([]TensorInfo, 0, len(items))
	for _, item := range items {
		if item.OrtValueType != onnxruntime_go.ONNXTypeTensor {
			return nil, errors.New("onnx input/output must be tensor")
		}
		if item.DataType != onnxruntime_go.TensorElementDataTypeFloat {
			return nil, errors.New("onnx input/output must use float32 tensors")
		}
		shape := item.Dimensions
		if err := validateScalarOrBatchShape(shape); err != nil {
			return nil, err
		}
		info = append(info, TensorInfo{
			Name:        item.Name,
			ElementType: item.DataType,
			Shape:       shape.Clone(),
		})
	}
	return info, nil
}

func validateScalarOrBatchShape(shape onnxruntime_go.Shape) error {
	switch len(shape) {
	case 1:
		if shape[0] != 1 && shape[0] != -1 {
			return errors.New("onnx input/output must have dimension 1")
		}
		return nil
	case 2:
		if shape[1] != 1 {
			return errors.New("onnx input/output must have inner dimension 1")
		}
		if shape[0] != 1 && shape[0] != -1 {
			return errors.New("onnx input/output must have batch dimension 1")
		}
		return nil
	default:
		return errors.New("onnx input/output must be scalar or batch tensor")
	}
}
