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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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
	restoreInit := swapInitRuntime(func() (RuntimeInfo, error) {
		return RuntimeInfo{}, nil
	})
	defer restoreInit()

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
	restoreInit := swapInitRuntime(func() (RuntimeInfo, error) {
		return RuntimeInfo{}, nil
	})
	defer restoreInit()

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

func TestRunInferenceBatchMetrics(t *testing.T) {
	restoreInit := swapInitRuntime(func() (RuntimeInfo, error) {
		return RuntimeInfo{}, nil
	})
	defer restoreInit()

	metrics.ModelBatchSize.Reset()
	metrics.ModelBatchSize.WithLabelValues("batch", metrics.RetLabel(nil))

	restoreBatch := swapRunInferenceBatch(func(_ *sessionEntry, _ []string, _ []string, batch [][]float32, _ time.Duration) ([][]float32, error) {
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

	inputs := [][]float32{{1}, {2}, {3}}
	_, err := RunInferenceBatchWithOptions(nil, "", []byte("dummy"), []string{"a"}, []string{"out"}, inputs, InferenceOptions{})
	require.NoError(t, err)

	count, sum := histogramCountAndSum(t, metrics.ModelBatchSize, map[string]string{
		metrics.LblType:   "batch",
		metrics.LblResult: metrics.RetLabel(nil),
	})
	require.Equal(t, uint64(1), count)
	require.InDelta(t, float64(len(inputs)), sum, 1e-9)
}

func TestRunInferenceInitFailure(t *testing.T) {
	restoreInit := swapInitRuntime(func() (RuntimeInfo, error) {
		return RuntimeInfo{}, errors.New("library missing")
	})
	defer restoreInit()

	_, err := RunInferenceWithOptions(nil, "", []byte("dummy"), []string{"a"}, []string{"out"}, []float32{1}, InferenceOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "initialize onnxruntime")
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

func swapInitRuntime(fn func() (RuntimeInfo, error)) func() {
	old := initRuntimeFn
	initRuntimeFn = fn
	return func() {
		initRuntimeFn = old
	}
}

func histogramCountAndSum(t *testing.T, vec *prometheus.HistogramVec, labels map[string]string) (uint64, float64) {
	t.Helper()
	ch := make(chan prometheus.Metric, 4)
	go func() {
		vec.Collect(ch)
		close(ch)
	}()
	for metric := range ch {
		dtoMetric := &dto.Metric{}
		require.NoError(t, metric.Write(dtoMetric))
		if labelsMatch(dtoMetric, labels) {
			hist := dtoMetric.GetHistogram()
			require.NotNil(t, hist)
			return hist.GetSampleCount(), hist.GetSampleSum()
		}
	}
	t.Fatalf("histogram metric not found for labels: %v", labels)
	return 0, 0
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return len(metric.GetLabel()) == 0
	}
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if labels[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}
