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
	"github.com/pingcap/errors"
	"github.com/yalue/onnxruntime_go"
)

var newDynamicSessionFn = func(onnxData []byte, inputNames, outputNames []string) (dynamicSession, error) {
	return onnxruntime_go.NewDynamicAdvancedSessionWithONNXData(onnxData, inputNames, outputNames, nil)
}

// RunInference executes the ONNX model with scalar float inputs and returns scalar float outputs.
func RunInference(onnxData []byte, inputNames, outputNames []string, inputs []float32) ([]float32, error) {
	return RunInferenceWithCache(nil, "", onnxData, inputNames, outputNames, inputs)
}

// RunInferenceWithCache executes the ONNX model using a cached session when provided.
func RunInferenceWithCache(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs []float32) ([]float32, error) {
	if len(inputNames) != len(inputs) {
		return nil, errors.New("onnx input count does not match inputs")
	}
	session, cleanup, err := getSession(cache, key, onnxData, inputNames, outputNames)
	if err != nil {
		return nil, errors.Annotate(err, "create onnx session")
	}
	defer cleanup()

	inputValues := make([]onnxruntime_go.Value, len(inputs))
	for i, val := range inputs {
		tensor, err := onnxruntime_go.NewTensor(onnxruntime_go.Shape{1}, []float32{val})
		if err != nil {
			return nil, errors.Annotate(err, "create input tensor")
		}
		inputValues[i] = tensor
		defer tensor.Destroy()
	}
	outputValues := make([]onnxruntime_go.Value, len(outputNames))
	if err := session.run(inputValues, outputValues); err != nil {
		return nil, errors.Annotate(err, "run onnx session")
	}
	results := make([]float32, len(outputValues))
	for i, out := range outputValues {
		if out == nil {
			return nil, errors.New("onnx output is nil")
		}
		tensor, ok := out.(*onnxruntime_go.Tensor[float32])
		if !ok {
			return nil, errors.New("onnx output must be float32 tensor")
		}
		data := tensor.GetData()
		if len(data) != 1 {
			return nil, errors.New("onnx output must be scalar")
		}
		results[i] = data[0]
		if err := out.Destroy(); err != nil {
			return nil, errors.Annotate(err, "destroy onnx output")
		}
	}
	return results, nil
}

// RunInferenceBatch executes the ONNX model with batched scalar float inputs.
// inputs is a slice of rows, each row is a slice of float32 matching inputNames.
func RunInferenceBatch(onnxData []byte, inputNames, outputNames []string, inputs [][]float32) ([][]float32, error) {
	return RunInferenceBatchWithCache(nil, "", onnxData, inputNames, outputNames, inputs)
}

// RunInferenceBatchWithCache executes the ONNX model with cached sessions when provided.
func RunInferenceBatchWithCache(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs [][]float32) ([][]float32, error) {
	if len(inputs) == 0 {
		return nil, errors.New("onnx batch input is empty")
	}
	if len(inputNames) == 0 {
		return nil, errors.New("onnx input names are empty")
	}
	batchSize := len(inputs)
	for i, row := range inputs {
		if len(row) != len(inputNames) {
			return nil, errors.Errorf("onnx input count does not match inputs at row %d", i)
		}
	}
	session, cleanup, err := getSession(cache, key, onnxData, inputNames, outputNames)
	if err != nil {
		return nil, errors.Annotate(err, "create onnx session")
	}
	defer cleanup()

	inputValues := make([]onnxruntime_go.Value, len(inputNames))
	for i := range inputNames {
		data := make([]float32, batchSize)
		for rowIdx, row := range inputs {
			data[rowIdx] = row[i]
		}
		tensor, err := onnxruntime_go.NewTensor(onnxruntime_go.Shape{int64(batchSize), 1}, data)
		if err != nil {
			return nil, errors.Annotate(err, "create input tensor")
		}
		inputValues[i] = tensor
		defer tensor.Destroy()
	}

	outputValues := make([]onnxruntime_go.Value, len(outputNames))
	if err := session.run(inputValues, outputValues); err != nil {
		return nil, errors.Annotate(err, "run onnx session")
	}

	results := make([][]float32, batchSize)
	for i := 0; i < batchSize; i++ {
		results[i] = make([]float32, len(outputValues))
	}
	for outIdx, out := range outputValues {
		if out == nil {
			return nil, errors.New("onnx output is nil")
		}
		tensor, ok := out.(*onnxruntime_go.Tensor[float32])
		if !ok {
			return nil, errors.New("onnx output must be float32 tensor")
		}
		data := tensor.GetData()
		if len(data) != batchSize {
			return nil, errors.New("onnx output batch size mismatch")
		}
		for rowIdx := 0; rowIdx < batchSize; rowIdx++ {
			results[rowIdx][outIdx] = data[rowIdx]
		}
		if err := out.Destroy(); err != nil {
			return nil, errors.Annotate(err, "destroy onnx output")
		}
	}
	return results, nil
}

func getSession(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string) (*sessionEntry, func(), error) {
	if cache != nil {
		entry, err := cache.GetOrCreate(key, func() (dynamicSession, error) {
			return newDynamicSessionFn(onnxData, inputNames, outputNames)
		})
		if err != nil {
			return nil, func() {}, err
		}
		return entry, func() {}, nil
	}
	session, err := newDynamicSessionFn(onnxData, inputNames, outputNames)
	if err != nil {
		return nil, func() {}, err
	}
	entry := &sessionEntry{session: session}
	return entry, func() { _ = session.Destroy() }, nil
}
