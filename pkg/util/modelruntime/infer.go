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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/yalue/onnxruntime_go"
)

var (
	newDynamicSessionFn = func(onnxData []byte, inputNames, outputNames []string) (dynamicSession, error) {
		return onnxruntime_go.NewDynamicAdvancedSessionWithONNXData(onnxData, inputNames, outputNames, nil)
	}
	newRunOptionsFn      = onnxruntime_go.NewRunOptions
	runInferenceScalarFn = runInferenceScalar
	runInferenceBatchFn  = runInferenceBatch
)

// InferenceOptions controls runtime behavior for model inference.
type InferenceOptions struct {
	MaxBatchSize int
	Timeout      time.Duration
}

// RunInference executes the ONNX model with scalar float inputs and returns scalar float outputs.
func RunInference(onnxData []byte, inputNames, outputNames []string, inputs []float32) ([]float32, error) {
	return RunInferenceWithOptions(nil, "", onnxData, inputNames, outputNames, inputs, InferenceOptions{})
}

// RunInferenceWithCache executes the ONNX model using a cached session when provided.
func RunInferenceWithCache(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs []float32) ([]float32, error) {
	return RunInferenceWithOptions(cache, key, onnxData, inputNames, outputNames, inputs, InferenceOptions{})
}

// RunInferenceWithOptions executes the ONNX model using options such as timeouts.
func RunInferenceWithOptions(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs []float32, opts InferenceOptions) (outputs []float32, err error) {
	start := time.Now()
	defer func() {
		label := metrics.RetLabel(err)
		metrics.ModelInferenceCounter.WithLabelValues("scalar", label).Inc()
		metrics.ModelInferenceDuration.WithLabelValues("scalar", label).Observe(time.Since(start).Seconds())
	}()
	if len(inputNames) != len(inputs) {
		return nil, errors.New("onnx input count does not match inputs")
	}
	session, cleanup, err := getSession(cache, key, onnxData, inputNames, outputNames)
	if err != nil {
		return nil, errors.Annotate(err, "create onnx session")
	}
	defer cleanup()

	outputs, err = runInferenceScalarFn(session, inputNames, outputNames, inputs, opts.Timeout)
	return outputs, err
}

func runInferenceScalar(session *sessionEntry, _ []string, outputNames []string, inputs []float32, timeout time.Duration) (results []float32, err error) {
	inputValues := make([]onnxruntime_go.Value, len(inputs))
	defer func() {
		if destroyErr := destroyValues(inputValues); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx inputs")
			results = nil
		}
	}()
	outputValues := make([]onnxruntime_go.Value, len(outputNames))
	defer func() {
		if destroyErr := destroyValues(outputValues); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx outputs")
			results = nil
		}
	}()
	for i, val := range inputs {
		tensor, err := onnxruntime_go.NewTensor(onnxruntime_go.Shape{1}, []float32{val})
		if err != nil {
			return nil, errors.Annotate(err, "create input tensor")
		}
		inputValues[i] = tensor
	}
	if err := runWithTimeout(session, inputValues, outputValues, timeout); err != nil {
		return nil, errors.Annotate(err, "run onnx session")
	}
	results = make([]float32, len(outputValues))
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
	}
	return results, nil
}

// RunInferenceBatch executes the ONNX model with batched scalar float inputs.
// inputs is a slice of rows, each row is a slice of float32 matching inputNames.
func RunInferenceBatch(onnxData []byte, inputNames, outputNames []string, inputs [][]float32) ([][]float32, error) {
	return RunInferenceBatchWithOptions(nil, "", onnxData, inputNames, outputNames, inputs, InferenceOptions{})
}

// RunInferenceBatchWithCache executes the ONNX model with cached sessions when provided.
func RunInferenceBatchWithCache(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs [][]float32) ([][]float32, error) {
	return RunInferenceBatchWithOptions(cache, key, onnxData, inputNames, outputNames, inputs, InferenceOptions{})
}

// RunInferenceBatchWithOptions executes the ONNX model with cached sessions when provided.
func RunInferenceBatchWithOptions(cache *SessionCache, key SessionKey, onnxData []byte, inputNames, outputNames []string, inputs [][]float32, opts InferenceOptions) (outputs [][]float32, err error) {
	batchSize := len(inputs)
	start := time.Now()
	defer func() {
		label := metrics.RetLabel(err)
		metrics.ModelInferenceCounter.WithLabelValues("batch", label).Inc()
		metrics.ModelInferenceDuration.WithLabelValues("batch", label).Observe(time.Since(start).Seconds())
		if batchSize > 0 {
			metrics.ModelBatchSize.WithLabelValues("batch", label).Observe(float64(batchSize))
		}
	}()
	if batchSize == 0 {
		return nil, errors.New("onnx batch input is empty")
	}
	if len(inputNames) == 0 {
		return nil, errors.New("onnx input names are empty")
	}
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
	if opts.MaxBatchSize > 0 && batchSize > opts.MaxBatchSize {
		results := make([][]float32, batchSize)
		for start := 0; start < batchSize; start += opts.MaxBatchSize {
			end := start + opts.MaxBatchSize
			if end > batchSize {
				end = batchSize
			}
			subResults, err := runInferenceBatchFn(session, inputNames, outputNames, inputs[start:end], opts.Timeout)
			if err != nil {
				return nil, err
			}
			for i := range subResults {
				results[start+i] = subResults[i]
			}
		}
		outputs = results
		return outputs, nil
	}
	outputs, err = runInferenceBatchFn(session, inputNames, outputNames, inputs, opts.Timeout)
	return outputs, err
}

func runInferenceBatch(session *sessionEntry, inputNames, outputNames []string, inputs [][]float32, timeout time.Duration) (results [][]float32, err error) {
	batchSize := len(inputs)
	inputValues := make([]onnxruntime_go.Value, len(inputNames))
	defer func() {
		if destroyErr := destroyValues(inputValues); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx inputs")
			results = nil
		}
	}()
	outputValues := make([]onnxruntime_go.Value, len(outputNames))
	defer func() {
		if destroyErr := destroyValues(outputValues); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx outputs")
			results = nil
		}
	}()
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
	}

	if err := runWithTimeout(session, inputValues, outputValues, timeout); err != nil {
		return nil, errors.Annotate(err, "run onnx session")
	}

	results = make([][]float32, batchSize)
	for i := range batchSize {
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
		for rowIdx := range batchSize {
			results[rowIdx][outIdx] = data[rowIdx]
		}
	}
	return results, nil
}

func runWithTimeout(session *sessionEntry, inputs, outputs []onnxruntime_go.Value, timeout time.Duration) (err error) {
	if timeout <= 0 {
		return session.run(inputs, outputs, nil)
	}
	opts, err := newRunOptionsFn()
	if err != nil {
		return errors.Annotate(err, "create onnx run options")
	}
	defer func() {
		if destroyErr := opts.Destroy(); destroyErr != nil && err == nil {
			err = errors.Annotate(destroyErr, "destroy onnx run options")
		}
	}()
	var timedOut atomic.Bool
	timer := time.AfterFunc(timeout, func() {
		timedOut.Store(true)
		_ = opts.Terminate()
	})
	err = session.run(inputs, outputs, opts)
	timer.Stop()
	if timedOut.Load() {
		return errors.Errorf("onnx inference timeout after %s", timeout)
	}
	return err
}

func destroyValues(values []onnxruntime_go.Value) error {
	for _, value := range values {
		if value == nil {
			continue
		}
		if err := value.Destroy(); err != nil {
			return err
		}
	}
	return nil
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
