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

import "context"

type onnxBackend struct{}

func (b *onnxBackend) InspectIO(ctx context.Context, artifact Artifact) (ModelIOInfo, error) {
	inputs, outputs, err := InspectModelIOInfo(artifact.Bytes)
	if err != nil {
		return ModelIOInfo{}, err
	}
	return ModelIOInfo{Inputs: inputs, Outputs: outputs}, nil
}

func (b *onnxBackend) Infer(ctx context.Context, artifact Artifact, inputNames, outputNames []string, inputs []float32, opts InferenceOptions) ([]float32, error) {
	key := SessionKeyFromParts(artifact.ModelID, artifact.Version, inputNames, outputNames)
	return RunInferenceWithOptions(GetProcessSessionCache(), key, artifact.Bytes, inputNames, outputNames, inputs, opts)
}

func (b *onnxBackend) InferBatch(ctx context.Context, artifact Artifact, inputNames, outputNames []string, inputs [][]float32, opts InferenceOptions) ([][]float32, error) {
	key := SessionKeyFromParts(artifact.ModelID, artifact.Version, inputNames, outputNames)
	return RunInferenceBatchWithOptions(GetProcessSessionCache(), key, artifact.Bytes, inputNames, outputNames, inputs, opts)
}
