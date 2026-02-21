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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
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
