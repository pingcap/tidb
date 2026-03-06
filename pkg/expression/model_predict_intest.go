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

package expression

// SetModelPredictOutputHookForTest overrides model inference for tests.
func SetModelPredictOutputHookForTest(fn func(modelName, outputName string, inputs []float32) (float64, error)) func() {
	modelPredictHookMu.Lock()
	old := modelPredictOutputHook
	modelPredictOutputHook = fn
	modelPredictHookMu.Unlock()
	return func() {
		modelPredictHookMu.Lock()
		modelPredictOutputHook = old
		modelPredictHookMu.Unlock()
	}
}

// SetModelPredictBatchHookForTest overrides batch model inference for tests.
func SetModelPredictBatchHookForTest(fn func(modelName, outputName string, inputs [][]float32) ([]float64, error)) func() {
	modelPredictHookMu.Lock()
	old := modelPredictBatchHook
	modelPredictBatchHook = fn
	modelPredictHookMu.Unlock()
	return func() {
		modelPredictHookMu.Lock()
		modelPredictBatchHook = old
		modelPredictHookMu.Unlock()
	}
}
