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
	"strings"
	"sync"
)

// Backend executes model inference for a specific engine.
type Backend interface {
	InspectIO(ctx context.Context, artifact Artifact) (ModelIOInfo, error)
	Infer(ctx context.Context, artifact Artifact, inputNames, outputNames []string, inputs []float32, opts InferenceOptions) ([]float32, error)
	InferBatch(ctx context.Context, artifact Artifact, inputNames, outputNames []string, inputs [][]float32, opts InferenceOptions) ([][]float32, error)
}

// ModelIOInfo captures parsed model input/output metadata.
type ModelIOInfo struct {
	Inputs  []TensorInfo
	Outputs []TensorInfo
}

// Artifact carries the runtime payload needed by inference backends.
type Artifact struct {
	ModelID   int64
	Version   int64
	Engine    string
	Bytes     []byte
	LocalPath string
}

var (
	backendRegistry = map[string]Backend{
		"ONNX": &onnxBackend{},
	}
	mlflowBackendOnce sync.Once
	mlflowBackendInst Backend
)

// BackendForEngine returns the backend for a model engine.
func BackendForEngine(engine string) (Backend, bool) {
	normalized := strings.ToUpper(engine)
	if normalized == "MLFLOW" {
		mlflowBackendOnce.Do(func() {
			mlflowBackendInst = newMLflowBackend()
		})
		return mlflowBackendInst, true
	}
	backend, ok := backendRegistry[normalized]
	return backend, ok
}
