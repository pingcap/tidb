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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/modelruntime"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
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

func TestParseModelSchemaAndValidateFloatColumns(t *testing.T) {
	cols, err := parseModelSchema(`["a double","b float"]`)
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Equal(t, "a", cols[0].name.L)
	require.Equal(t, "b", cols[1].name.L)
	require.NoError(t, validateFloatColumns(cols))

	_, err = parseModelSchema(`[]`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")

	cols, err = parseModelSchema(`["a int"]`)
	require.NoError(t, err)
	err = validateFloatColumns(cols)
	require.Error(t, err)
	require.Contains(t, err.Error(), "FLOAT or DOUBLE")
}

func TestResolveBatchableShape(t *testing.T) {
	inputs := []modelruntime.TensorInfo{
		{
			Name:        "a",
			ElementType: onnxruntime_go.TensorElementDataTypeFloat,
			Shape:       onnxruntime_go.Shape{1},
		},
	}
	outputs := []modelruntime.TensorInfo{
		{
			Name:        "b",
			ElementType: onnxruntime_go.TensorElementDataTypeFloat,
			Shape:       onnxruntime_go.Shape{1},
		},
	}
	batchable, err := resolveBatchableShape(inputs, outputs)
	require.NoError(t, err)
	require.False(t, batchable)

	inputs[0].Shape = onnxruntime_go.Shape{-1, 1}
	outputs[0].Shape = onnxruntime_go.Shape{-1, 1}
	batchable, err = resolveBatchableShape(inputs, outputs)
	require.NoError(t, err)
	require.True(t, batchable)

	outputs[0].Shape = onnxruntime_go.Shape{1, 2}
	_, err = resolveBatchableShape(inputs, outputs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scalar or batchable")
}

func TestClassifyTensorShapesRejectsUnsupported(t *testing.T) {
	infos := []modelruntime.TensorInfo{
		{
			Name:        "x",
			ElementType: onnxruntime_go.TensorElementDataTypeFloat,
			Shape:       onnxruntime_go.Shape{2},
		},
	}
	scalar, batch := classifyTensorShapes(infos)
	require.False(t, scalar)
	require.False(t, batch)

	infos[0].Shape = onnxruntime_go.Shape{1}
	scalar, batch = classifyTensorShapes(infos)
	require.True(t, scalar)
	require.False(t, batch)

	infos[0].Shape = onnxruntime_go.Shape{-1, 1}
	scalar, batch = classifyTensorShapes(infos)
	require.False(t, scalar)
	require.True(t, batch)
}

func TestModelPredictFunctionClassTypeInference(t *testing.T) {
	ctx := createContext(t)

	fn, err := funcs[ast.ModelPredict].getFunction(ctx, datumsToConstants(types.MakeDatums("m1", 1.0)))
	require.NoError(t, err)
	require.IsType(t, &builtinModelPredictSig{}, fn)
	require.Equal(t, mysql.TypeJSON, fn.getRetTp().GetType())

	outFn, err := funcs[ast.ModelPredictOutput].getFunction(ctx, datumsToConstants(types.MakeDatums("m1", "y", 1.0)))
	require.NoError(t, err)
	require.IsType(t, &builtinModelPredictOutputSig{}, outFn)
	require.Equal(t, mysql.TypeDouble, outFn.getRetTp().GetType())
}
