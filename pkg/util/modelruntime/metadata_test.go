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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
)

func TestInspectModelIOInfo(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "input",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "output",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restore()

	inputs, outputs, err := InspectModelIOInfo([]byte("dummy"))
	require.NoError(t, err)
	require.Equal(t, []TensorInfo{{Name: "input", ElementType: onnxruntime_go.TensorElementDataTypeFloat, Shape: onnxruntime_go.Shape{1}}}, inputs)
	require.Equal(t, []TensorInfo{{Name: "output", ElementType: onnxruntime_go.TensorElementDataTypeFloat, Shape: onnxruntime_go.Shape{1}}}, outputs)
}

func TestInspectModelIOInfoRejectsNonTensor(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
			{
				Name:         "input",
				OrtValueType: onnxruntime_go.ONNXTypeMap,
				DataType:     onnxruntime_go.TensorElementDataTypeFloat,
			},
		}, nil, nil
	})
	defer restore()

	_, _, err := InspectModelIOInfo([]byte("dummy"))
	require.Error(t, err)
}

func TestInspectModelIOInfoRejectsNonFloat(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
			{
				Name:         "input",
				OrtValueType: onnxruntime_go.ONNXTypeTensor,
				DataType:     onnxruntime_go.TensorElementDataTypeInt64,
			},
		}, nil, nil
	})
	defer restore()

	_, _, err := InspectModelIOInfo([]byte("dummy"))
	require.Error(t, err)
}

func TestInspectModelIOInfoRejectsNonScalarShape(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
			{
				Name:         "input",
				OrtValueType: onnxruntime_go.ONNXTypeTensor,
				Dimensions:   onnxruntime_go.Shape{2},
				DataType:     onnxruntime_go.TensorElementDataTypeFloat,
			},
		}, nil, nil
	})
	defer restore()

	_, _, err := InspectModelIOInfo([]byte("dummy"))
	require.Error(t, err)
}

func TestInspectModelIOInfoAcceptsBatchShape(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "input",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{-1, 1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "output",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{-1, 1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restore()

	inputs, outputs, err := InspectModelIOInfo([]byte("dummy"))
	require.NoError(t, err)
	require.Equal(t, []TensorInfo{{Name: "input", ElementType: onnxruntime_go.TensorElementDataTypeFloat, Shape: onnxruntime_go.Shape{-1, 1}}}, inputs)
	require.Equal(t, []TensorInfo{{Name: "output", ElementType: onnxruntime_go.TensorElementDataTypeFloat, Shape: onnxruntime_go.Shape{-1, 1}}}, outputs)
}

func TestInspectModelIOInfoPropagatesError(t *testing.T) {
	restore := swapGetInputOutputInfo(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return nil, nil, errors.New("boom")
	})
	defer restore()

	_, _, err := InspectModelIOInfo([]byte("dummy"))
	require.Error(t, err)
}

func swapGetInputOutputInfo(fn func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error)) func() {
	old := getInputOutputInfoFn
	getInputOutputInfoFn = fn
	return func() {
		getInputOutputInfoFn = old
	}
}
