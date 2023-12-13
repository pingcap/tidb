// Copyright 2023 PingCAP, Inc.
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

package analyzequeue

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
func (pc *PriorityCalculator) CalculateWeight(job *TableAnalysisJob) float64 {
	weight := calculateChangePercentageWeight(job.ChangePercentage, pc.threshold)
	// This means we have some indexes to analyze.
	if len(job.Indexes) > 0 || len(job.PartitionIndexes) > 0 {
		weight += 1.5
	}

	return weight
}

// calculateChangePercentageWeight calculates the weight based on the given rules and threshold.
func calculateChangePercentageWeight(changePercentage, threshold float64) float64 {
	switch {
	case changePercentage >= threshold && changePercentage < (threshold+0.2):
		return 1
	case changePercentage >= (threshold+0.2) && changePercentage < (threshold+0.3):
		return 1.3
	case changePercentage >= (threshold + 0.3):
		return 1.5
	default:
		return 0
	}
}
