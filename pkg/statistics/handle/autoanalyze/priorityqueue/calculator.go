// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

// WeightCalculator is an interface for calculating weights of analysis jobs.
type WeightCalculator interface {
	CalculateWeight(job *TableAnalysisJob) float64
}

// PriorityCalculator implements the WeightCalculator interface.
type PriorityCalculator struct {
	threshold float64
}

// NewPriorityCalculator creates a new PriorityCalculator with the given threshold.
func NewPriorityCalculator(threshold float64) *PriorityCalculator {
	return &PriorityCalculator{threshold: threshold}
}

// CalculateWeight calculates the weight based on the given rules.
func (*PriorityCalculator) CalculateWeight(_ *TableAnalysisJob) float64 {
	// TODO: implement the weight calculation
	return 1
}
