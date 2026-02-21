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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
)

type stubInferenceSession struct{}

func (s *stubInferenceSession) Run(inputs, outputs []onnxruntime_go.Value) error {
	return nil
}

func (s *stubInferenceSession) RunWithOptions(inputs, outputs []onnxruntime_go.Value, _ *onnxruntime_go.RunOptions) error {
	return nil
}

func (s *stubInferenceSession) Destroy() error {
	return nil
}

type sleepSession struct {
	sleep time.Duration
}

func (s *sleepSession) Run(inputs, outputs []onnxruntime_go.Value) error {
	return s.RunWithOptions(inputs, outputs, nil)
}

func (s *sleepSession) RunWithOptions(inputs, outputs []onnxruntime_go.Value, _ *onnxruntime_go.RunOptions) error {
	time.Sleep(s.sleep)
	return nil
}

func (s *sleepSession) Destroy() error {
	return nil
}

func TestRunInferenceBatchSplits(t *testing.T) {
	inputs := [][]float32{{1}, {2}, {3}, {4}, {5}}
	opts := InferenceOptions{MaxBatchSize: 2}
	runs := make([]int, 0, 3)

	restoreBatch := swapRunInferenceBatch(func(_ *sessionEntry, _ []string, _ []string, batch [][]float32, _ time.Duration) ([][]float32, error) {
		runs = append(runs, len(batch))
		out := make([][]float32, len(batch))
		for i, row := range batch {
			out[i] = []float32{row[0]}
		}
		return out, nil
	})
	defer restoreBatch()

	restoreSession := swapNewDynamicSession(func([]byte, []string, []string) (dynamicSession, error) {
		return &stubInferenceSession{}, nil
	})
	defer restoreSession()

	outputs, err := RunInferenceBatchWithOptions(nil, "", []byte("dummy"), []string{"a"}, []string{"out"}, inputs, opts)
	require.NoError(t, err)
	require.Equal(t, []int{2, 2, 1}, runs)
	for i := range inputs {
		require.Equal(t, inputs[i][0], outputs[i][0])
	}
}

func TestRunInferenceTimeout(t *testing.T) {
	restoreRunOptions := swapNewRunOptions(func() (*onnxruntime_go.RunOptions, error) {
		return &onnxruntime_go.RunOptions{}, nil
	})
	defer restoreRunOptions()

	entry := &sessionEntry{session: &sleepSession{sleep: 10 * time.Millisecond}}
	err := runWithTimeout(entry, nil, nil, time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestRunInferenceMetrics(t *testing.T) {
	metrics.ModelInferenceCounter.Reset()

	restoreScalar := swapRunInferenceScalar(func(_ *sessionEntry, _ []string, _ []string, _ []float32, _ time.Duration) ([]float32, error) {
		return []float32{1}, nil
	})
	defer restoreScalar()

	restoreSession := swapNewDynamicSession(func([]byte, []string, []string) (dynamicSession, error) {
		return &stubInferenceSession{}, nil
	})
	defer restoreSession()

	_, err := RunInferenceWithOptions(nil, "", []byte("dummy"), []string{"a"}, []string{"out"}, []float32{1}, InferenceOptions{})
	require.NoError(t, err)

	counter := metrics.ModelInferenceCounter.WithLabelValues("scalar", metrics.RetLabel(nil))
	require.Equal(t, 1.0, promtestutils.ToFloat64(counter))
}

func swapNewDynamicSession(fn func([]byte, []string, []string) (dynamicSession, error)) func() {
	old := newDynamicSessionFn
	newDynamicSessionFn = fn
	return func() {
		newDynamicSessionFn = old
	}
}

func swapRunInferenceScalar(fn func(*sessionEntry, []string, []string, []float32, time.Duration) ([]float32, error)) func() {
	old := runInferenceScalarFn
	runInferenceScalarFn = fn
	return func() {
		runInferenceScalarFn = old
	}
}

func swapRunInferenceBatch(fn func(*sessionEntry, []string, []string, [][]float32, time.Duration) ([][]float32, error)) func() {
	old := runInferenceBatchFn
	runInferenceBatchFn = fn
	return func() {
		runInferenceBatchFn = old
	}
}

func swapNewRunOptions(fn func() (*onnxruntime_go.RunOptions, error)) func() {
	old := newRunOptionsFn
	newRunOptionsFn = fn
	return func() {
		newRunOptionsFn = old
	}
}
