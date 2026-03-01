//go:build intest

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

package executor_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/modelruntime"
	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
)

func TestModelPredictFeatureGateAndOutput(t *testing.T) {
	store := testkit.CreateMockStore(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec("use test")
	rootTK.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	rootTK.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))

	rootTK.MustExec("set global tidb_enable_model_inference = off")
	rootTK.MustContainErrMsg("select model_predict(m1, 1.0).score", "tidb_enable_model_inference")

	rootTK.MustExec("set global tidb_enable_model_inference = on")
	rootTK.MustExec("create user u1")

	userTK := testkit.NewTestKit(t, store)
	require.NoError(t, userTK.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	userTK.MustContainErrMsg("select model_predict(test.m1, 1.0).score", "MODEL_EXECUTE")
	rootTK.MustExec("grant MODEL_EXECUTE on *.* to u1")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restorePredict := expression.SetModelPredictOutputHookForTest(func(modelName, outputName string, inputs []float32) (float64, error) {
		if modelName != "m1" || outputName != "score" {
			return 0, fmt.Errorf("unexpected request")
		}
		if len(inputs) != 1 || inputs[0] != 1.0 {
			return 0, fmt.Errorf("unexpected inputs")
		}
		return 0.42, nil
	})
	defer restorePredict()

	userTK.MustQuery("select model_predict(test.m1, 1.0).score").Check(testkit.Rows("0.42"))
}

func TestModelPredictBatchInference(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")

	tk.MustExec("create table t (x double)")
	tk.MustExec("insert into t values (1.0), (2.0), (3.0)")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{-1, 1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{-1, 1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restorePredict := expression.SetModelPredictOutputHookForTest(func(string, string, []float32) (float64, error) {
		return 0, fmt.Errorf("row inference should not be used in batch path")
	})
	defer restorePredict()

	callCount := 0
	restoreBatch := expression.SetModelPredictBatchHookForTest(func(modelName, outputName string, inputs [][]float32) ([]float64, error) {
		callCount++
		if modelName != "m1" || outputName != "score" {
			return nil, fmt.Errorf("unexpected request")
		}
		out := make([]float64, len(inputs))
		for i, row := range inputs {
			if len(row) != 1 {
				return nil, fmt.Errorf("unexpected inputs")
			}
			out[i] = float64(row[0]) * 10
		}
		return out, nil
	})
	defer restoreBatch()

	tk.MustExec("set @@tidb_enable_vectorized_expression = on")
	tk.MustExec("set @@tidb_init_chunk_size = 2")
	tk.MustExec("set @@tidb_max_chunk_size = 2")

	tk.MustQuery("select model_predict(test.m1, x).score from t order by x").
		Check(testkit.Rows("10", "20", "30"))
	require.Greater(t, callCount, 0)
}

func TestModelPredictRejectsNondeterministic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")
	tk.MustExec("set global tidb_model_allow_nondeterministic = off")
	require.False(t, vardef.ModelAllowNondeterministic.Load())

	ioCalls := 0
	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		ioCalls++
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restoreMeta := modelruntime.SetInspectModelMetadataHookForTest(func([]byte) (modelruntime.ModelMetadata, error) {
		return &stubModelMetadata{values: map[string]string{"tidb_nondeterministic": "true"}}, nil
	})
	defer restoreMeta()

	err := tk.QueryToErr("select model_predict(test.m1, 1.0).score")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tidb_model_allow_nondeterministic")
}

func TestModelPredictNullBehaviorReturnNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")
	tk.MustExec("set global tidb_model_null_behavior = 'RETURN_NULL'")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	tk.MustQuery("select model_predict(test.m1, null).score").Check(testkit.Rows("<nil>"))
}

func TestModelPredictInferenceStatsRecorded(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restorePredict := expression.SetModelPredictOutputHookForTest(func(modelName, outputName string, inputs []float32) (float64, error) {
		if modelName != "m1" || outputName != "score" {
			return 0, fmt.Errorf("unexpected request")
		}
		if len(inputs) != 1 || inputs[0] != 1.0 {
			return 0, fmt.Errorf("unexpected inputs")
		}
		return 0.5, nil
	})
	defer restorePredict()

	tk.MustQuery("select model_predict(test.m1, 1.0).score").Check(testkit.Rows("0.5"))

	stats := tk.Session().GetSessionVars().StmtCtx.ModelInferenceStats().SlowLogSummaries()
	require.Len(t, stats, 1)
	entry := stats[0]
	require.Equal(t, int64(1), entry.Calls)
	require.Equal(t, int64(0), entry.Errors)
	require.Equal(t, int64(1), entry.TotalBatchSize)
	require.Equal(t, int64(1), entry.MaxBatchSize)
	require.Equal(t, stmtctx.ModelInferenceRoleProjection, entry.Role)
}

func TestExplainAnalyzeIncludesModelInferenceStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restorePredict := expression.SetModelPredictOutputHookForTest(func(modelName, outputName string, inputs []float32) (float64, error) {
		if modelName != "m1" || outputName != "score" {
			return 0, fmt.Errorf("unexpected request")
		}
		if len(inputs) != 1 || inputs[0] != 1.0 {
			return 0, fmt.Errorf("unexpected inputs")
		}
		return 0.9, nil
	})
	defer restorePredict()

	rows := tk.MustQuery("explain analyze select model_predict(test.m1, 1.0).score").Rows()
	found := false
	for _, row := range rows {
		for _, col := range row {
			value, ok := col.(string)
			if !ok {
				continue
			}
			if strings.Contains(value, "model_inference:") && strings.Contains(value, "role=projection") {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	require.True(t, found)
}

func TestSlowLogModelInferencePayload(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", newCfg.Log.SlowQueryFile))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()

	tk.MustExec("set global tidb_enable_model_ddl = on")

	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.onnx")
	modelBytes := []byte("dummy-model")
	require.NoError(t, os.WriteFile(modelPath, modelBytes, 0o600))
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath

	tk.MustExec(fmt.Sprintf(
		"create model m1 (input (a double) output (score double)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustExec("set global tidb_enable_model_inference = on")

	restoreIO := modelruntime.SetInspectModelIOInfoHookForTest(func([]byte) ([]onnxruntime_go.InputOutputInfo, []onnxruntime_go.InputOutputInfo, error) {
		return []onnxruntime_go.InputOutputInfo{
				{
					Name:         "a",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, []onnxruntime_go.InputOutputInfo{
				{
					Name:         "score",
					OrtValueType: onnxruntime_go.ONNXTypeTensor,
					Dimensions:   onnxruntime_go.Shape{1},
					DataType:     onnxruntime_go.TensorElementDataTypeFloat,
				},
			}, nil
	})
	defer restoreIO()

	restorePredict := expression.SetModelPredictOutputHookForTest(func(modelName, outputName string, inputs []float32) (float64, error) {
		if modelName != "m1" || outputName != "score" {
			return 0, fmt.Errorf("unexpected request")
		}
		if len(inputs) != 1 || inputs[0] != 1.0 {
			return 0, fmt.Errorf("unexpected inputs")
		}
		return 1.2, nil
	})
	defer restorePredict()

	tk.MustQuery("select model_predict(test.m1, 1.0).score").Check(testkit.Rows("1.2"))

	rows := tk.MustQuery("select Model_inference from information_schema.slow_query where query like '%model_predict%' order by time desc limit 1").Rows()
	require.Len(t, rows, 1)
	payload := rows[0][0].(string)
	require.NotEmpty(t, payload)
	require.Contains(t, payload, "\"model_id\"")
	require.Contains(t, payload, "\"role\"")
}

func TestModelPredictE2EPublicModel(t *testing.T) {
	modelPath, err := filepath.Abs(filepath.Join("testdata", "onnx", "identity_scalar.onnx"))
	require.NoError(t, err)
	modelBytes, err := os.ReadFile(modelPath)
	require.NoError(t, err)

	if _, err := modelruntime.ResolveLibraryPath(); err != nil {
		t.Skipf("onnxruntime shared library not available: %v", err)
	}
	if _, err := modelruntime.Init(); err != nil {
		require.NoError(t, err)
	}

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")
	tk.MustExec("set global tidb_enable_model_inference = on")

	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(modelBytes))
	location := "file://" + modelPath
	tk.MustExec(fmt.Sprintf(
		"create model identity_model (input (x float) output (y float)) using onnx location '%s' checksum '%s'",
		location, checksum,
	))
	tk.MustQuery("select model_predict(identity_model, 0.25).y").Check(testkit.Rows("0.25"))
}

type stubModelMetadata struct {
	values map[string]string
}

func (s *stubModelMetadata) Destroy() error {
	return nil
}

func (s *stubModelMetadata) LookupCustomMetadataMap(key string) (string, bool, error) {
	val, ok := s.values[key]
	return val, ok, nil
}
