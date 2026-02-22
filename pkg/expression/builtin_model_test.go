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

package expression

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRecordModelLoadStatsMetrics(t *testing.T) {
	metrics.ModelLoadDuration.Reset()
	metrics.ModelLoadDuration.WithLabelValues("onnx", metrics.RetLabel(nil))

	vars := variable.NewSessionVars(nil)
	vars.StmtCtx = stmtctx.NewStmtCtx()
	ctx := WithModelInferenceStatsTarget(exprstatic.NewEvalContext(), stmtctx.ModelInferenceRoleProjection, 1)

	recordModelLoadStats(ctx, vars, 1, 1, "onnx", 150*time.Millisecond, nil)

	count, sum := histogramCountAndSum(t, metrics.ModelLoadDuration, map[string]string{
		metrics.LblType:   "onnx",
		metrics.LblResult: metrics.RetLabel(nil),
	})
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 0.15, sum, 0.001)
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
