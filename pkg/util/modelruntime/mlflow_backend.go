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
	"context"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/modelruntime/mlflow"
	"github.com/yalue/onnxruntime_go"
)

type mlflowBackend struct {
	client *mlflow.Client
}

func (_ *mlflowBackend) InspectIO(_ context.Context, artifact Artifact) (ModelIOInfo, error) {
	if artifact.LocalPath == "" {
		return ModelIOInfo{}, errors.New("mlflow model directory is required")
	}
	mlmodelPath := filepath.Join(artifact.LocalPath, "MLmodel")
	data, err := os.ReadFile(mlmodelPath)
	if err != nil {
		return ModelIOInfo{}, errors.Annotate(err, "read MLmodel")
	}
	info, err := mlflow.ParseModel(data)
	if err != nil {
		return ModelIOInfo{}, err
	}
	if !info.HasPyFunc {
		return ModelIOInfo{}, errors.New("mlflow model missing python_function flavor")
	}
	inputs := make([]TensorInfo, len(info.InputNames))
	for i, name := range info.InputNames {
		inputs[i] = TensorInfo{
			Name:        name,
			ElementType: onnxruntime_go.TensorElementDataTypeFloat,
			Shape:       onnxruntime_go.Shape{-1, 1},
		}
	}
	outputs := make([]TensorInfo, len(info.OutputNames))
	for i, name := range info.OutputNames {
		outputs[i] = TensorInfo{
			Name:        name,
			ElementType: onnxruntime_go.TensorElementDataTypeFloat,
			Shape:       onnxruntime_go.Shape{-1, 1},
		}
	}
	return ModelIOInfo{Inputs: inputs, Outputs: outputs}, nil
}

func (b *mlflowBackend) Infer(ctx context.Context, artifact Artifact, inputNames, outputNames []string, inputs []float32, opts InferenceOptions) ([]float32, error) {
	outputs, err := b.InferBatch(ctx, artifact, inputNames, outputNames, [][]float32{inputs}, opts)
	if err != nil {
		return nil, err
	}
	if len(outputs) != 1 {
		return nil, errors.New("mlflow output row count mismatch")
	}
	return outputs[0], nil
}

func (b *mlflowBackend) InferBatch(ctx context.Context, artifact Artifact, _ []string, _ []string, inputs [][]float32, opts InferenceOptions) ([][]float32, error) {
	if artifact.LocalPath == "" {
		return nil, errors.New("mlflow model directory is required")
	}
	if b.client == nil {
		return nil, errors.New("mlflow sidecar client is not configured")
	}
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	return b.client.PredictBatch(ctx, mlflow.PredictRequest{ModelPath: artifact.LocalPath, Inputs: inputs})
}

func newMLflowBackend() *mlflowBackend {
	cfg := mlflow.DefaultConfig()
	if global := config.GetGlobalConfig(); global != nil {
		cfg = mlflow.Config{
			Python:       global.ModelMLflow.Python,
			Workers:      global.ModelMLflow.SidecarWorkers,
			Timeout:      global.ModelMLflow.RequestTimeout,
			CacheEntries: global.ModelMLflow.CacheEntries,
		}
	}
	pool := mlflow.NewSidecarPool(cfg)
	return &mlflowBackend{client: mlflow.NewClient(mlflow.ClientOptions{Dial: pool.Dial})}
}
