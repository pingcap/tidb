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

package priorityqueue_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/stretchr/testify/require"
)

type testData struct {
	ID                   int
	ChangePercentage     float64
	TableSize            float64
	LastAnalysisDuration time.Duration
}

func TestCalculateWeight(t *testing.T) {
	// Note: all groups are sorted by weight in ascending order.
	pc := priorityqueue.NewPriorityCalculator()
	// Only focus on change percentage. Bigger change percentage, higher weight.
	changePercentageGroup := []testData{
		{
			ChangePercentage:     0.6,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour,
		},
		{
			ChangePercentage:     1,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour,
		},
		{
			ChangePercentage:     10,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour,
		},
	}
	testWeightCalculation(t, pc, changePercentageGroup)
	// Only focus on table size. Bigger table size, lower weight.
	tableSizeGroup := []testData{
		{
			ChangePercentage:     0.6,
			TableSize:            100000,
			LastAnalysisDuration: time.Hour,
		},
		{
			ChangePercentage:     0.6,
			TableSize:            10000,
			LastAnalysisDuration: time.Hour,
		},
		{
			ChangePercentage:     0.6,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour,
		},
	}
	testWeightCalculation(t, pc, tableSizeGroup)
	// Only focus on last analysis duration. Longer duration, higher weight.
	lastAnalysisDurationGroup := []testData{
		{
			ChangePercentage:     0.6,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour,
		},
		{
			ChangePercentage:     0.6,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour * 12,
		},
		{
			ChangePercentage:     0.6,
			TableSize:            1000,
			LastAnalysisDuration: time.Hour * 24,
		},
	}
	testWeightCalculation(t, pc, lastAnalysisDurationGroup)
	// The system should not assign a higher weight to a recently analyzed table, even if it has undergone significant changes.
	justBeingAnalyzedGroup := []testData{
		{
			ChangePercentage:     0.5,
			TableSize:            1000,
			LastAnalysisDuration: 2 * time.Hour,
		},
		{
			ChangePercentage:     1,
			TableSize:            1000,
			LastAnalysisDuration: 10 * time.Minute,
		},
	}
	testWeightCalculation(t, pc, justBeingAnalyzedGroup)
}

// testWeightCalculation is a helper function to test the weight calculation.
// It will check if the weight is increasing for each test data group.
func testWeightCalculation(t *testing.T, pc *priorityqueue.PriorityCalculator, group []testData) {
	prevWeight := -1.0
	for _, tc := range group {
		job := &priorityqueue.NonPartitionedTableAnalysisJob{
			Indicators: priorityqueue.Indicators{
				ChangePercentage:     tc.ChangePercentage,
				TableSize:            tc.TableSize,
				LastAnalysisDuration: tc.LastAnalysisDuration,
			},
		}
		weight := pc.CalculateWeight(job)
		require.Greater(t, weight, 0.0)
		require.Greater(t, weight, prevWeight)
		prevWeight = weight
	}
}

func TestGetSpecialEvent(t *testing.T) {
	pc := priorityqueue.NewPriorityCalculator()

	jobWithIndex1 := &priorityqueue.DynamicPartitionedTableAnalysisJob{
		PartitionIndexes: map[string][]string{
			"index1": {"p1", "p2"},
		},
	}
	require.Equal(t, priorityqueue.EventNewIndex, pc.GetSpecialEvent(jobWithIndex1))

	jobWithIndex2 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Indexes: []string{"index1"},
	}
	require.Equal(t, priorityqueue.EventNewIndex, pc.GetSpecialEvent(jobWithIndex2))

	jobWithoutIndex := &priorityqueue.DynamicPartitionedTableAnalysisJob{
		PartitionIndexes: map[string][]string{},
	}
	require.Equal(t, priorityqueue.EventNone, pc.GetSpecialEvent(jobWithoutIndex))
}
