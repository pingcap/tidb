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

import "math"

const (
	// EventNone represents no special event.
	EventNone = 0.0
	// EventNewIndex represents a special event for newly added indexes.
	EventNewIndex = 2.0
)

// TODO: make these configurable.
const (
	changeRatioWeight = 0.6
	sizeWeight        = 0.1
	analysisInterval  = 0.3
)

// PriorityCalculator implements the WeightCalculator interface.
type PriorityCalculator struct{}

// NewPriorityCalculator creates a new PriorityCalculator.
//
// For more information, please visit:
// https://github.com/pingcap/tidb/blob/master/docs/design/2023-11-29-priority-queue-for-auto-analyze.md
func NewPriorityCalculator() *PriorityCalculator {
	return &PriorityCalculator{}
}

// CalculateWeight calculates the weight based on the given rules.
// - Table Change Ratio (Change Ratio): Accounts for 60%
// - Table Size (Size): Accounts for 10%
// - Analysis Interval (Analysis Interval): Accounts for 30%
// priority_score calculates the priority score based on the following formula:
//
//	priority_score = (0.6 * math.Log10(1 + ChangeRatio) +
//	                  0.1 * (1 - math.Log10(1 + TableSize)) +
//	                  0.3 * math.Log10(1 + math.Sqrt(AnalysisInterval)) +
//	                  special_event[event])
func (pc *PriorityCalculator) CalculateWeight(job AnalysisJob) float64 {
	// We multiply the priority_score by 100 to increase its magnitude. This ensures that
	// when we apply the log10 function, the resulting value is more meaningful and reasonable.
	indicators := job.GetIndicators()
	changeRatio := 100 * indicators.ChangePercentage
	return changeRatioWeight*math.Log10(1+changeRatio) +
		sizeWeight*(1-math.Log10(1+indicators.TableSize)) +
		analysisInterval*math.Log10(1+math.Sqrt(indicators.LastAnalysisDuration.Seconds())) +
		pc.GetSpecialEvent(job)
}

// GetSpecialEvent returns the special event weight.
// Exported for testing purposes.
func (*PriorityCalculator) GetSpecialEvent(job AnalysisJob) float64 {
	if job.HasNewlyAddedIndex() {
		return EventNewIndex
	}

	return EventNone
}
