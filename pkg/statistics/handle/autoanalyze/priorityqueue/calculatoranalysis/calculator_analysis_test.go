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
	"bytes"
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/stretchr/testify/require"
)

const (
	baseChangeRate      = 0.001  // Base change rate: 0.1% per second
	changeRateDecayLog  = 3      // Controls how quickly the change rate decays for larger tables
	smallTableThreshold = 100000 // Tables smaller than this use the base change rate
	maxChangePercentage = 3.0    // Maximum change capped at 300% of table size
)

var update = flag.Bool("update", false, "update .golden files")

// Please read README.md for more details.
// To update golden file, run the test with -update flag.
func TestPriorityCalculatorWithGeneratedData(t *testing.T) {
	jobs := generateTestData(t)

	calculator := priorityqueue.NewPriorityCalculator()

	newPriorities := calculateNewPriorities(jobs, calculator)
	sortPrioritiesByWeight(newPriorities)

	// Convert newPriorities to CSV string
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.Write([]string{"ID", "CalculatedPriority", "TableSize", "Changes", "TimeSinceLastAnalyze", "ChangeRatio"})
	for _, p := range newPriorities {
		writer.Write([]string{
			fmt.Sprintf("%d", p.job.ID),
			fmt.Sprintf("%.4f", p.priority),
			fmt.Sprintf("%.0f", p.job.TableSize),
			fmt.Sprintf("%.0f", p.job.Changes),
			fmt.Sprintf("%.0f", p.job.TimeSinceLastAnalyze),
			fmt.Sprintf("%.4f", p.changeRatio),
		})
	}
	writer.Flush()
	got := buf.Bytes()

	// Compare with golden file
	golden := "testdata/calculated_priorities.golden.csv"
	if *update {
		t.Log("updating golden file")
		err := os.WriteFile(golden, got, 0644)
		require.NoError(t, err, "Failed to update golden file")
	}

	want, err := os.ReadFile(golden)
	require.NoError(t, err, "Failed to read golden file")

	require.Equal(t, want, got)
}

// calculateNewPriorities calculates priorities for all jobs
func calculateNewPriorities(jobs []TestJob, calculator *priorityqueue.PriorityCalculator) []jobWithPriority {
	newPriorities := make([]jobWithPriority, 0, len(jobs))
	for _, job := range jobs {
		priority := calculator.CalculateWeight(&job)
		changeRatio := job.Changes / job.TableSize // Calculate change ratio
		newPriorities = append(newPriorities, jobWithPriority{job, priority, changeRatio})
	}
	return newPriorities
}

// sortPrioritiesByWeight sorts jobs by priority in descending order
func sortPrioritiesByWeight(priorities []jobWithPriority) {
	slices.SortStableFunc(priorities, func(i, j jobWithPriority) int {
		return cmp.Compare(j.priority, i.priority)
	})
}

// generateCombinations generates test data combinations for analyzing table statistics.
//
// The function creates combinations based on different table sizes and time intervals
// since the last analysis. It calculates reasonable change amounts for each combination
// using the following principles:
//
//  1. Maximum Change Rate: Assumes a maximum change rate of 0.1% of the table size per second.
//     This rate can be adjusted based on real-world observations.
//
//  2. Change Calculation: For each table size and time interval, it calculates a maximum
//     reasonable change as: tableSize * maxChangeRate * timeSinceLastAnalyze.
//
//  3. Change Limit: The maximum change is capped at 300% of the table size to prevent
//     unrealistic scenarios for very long time intervals.
//
//  4. Change Variety: Generates six different change amounts for each combination:
//     1%, 10%, 50%, 100%, 200%, and 300% of the calculated maximum change.
//     This variety allows testing the priority calculator under different degrees of modification.
//
// This approach creates a dataset that simulates real-world scenarios by considering
// the relationship between table size, time since last analysis, and the magnitude of changes.
func generateCombinations(tableSizes, analyzeTimes []int64) [][]int64 {
	var combinations [][]int64
	id := 1
	for _, size := range tableSizes {
		for _, time := range analyzeTimes {
			// Calculate the maximum reasonable change based on table size and time since last analysis
			maxChange := calculateMaxChange(size, time)

			// Generate a range of changes from 10% to 300% of the maximum change
			// This allows us to test the priority calculator with various degrees of data modification
			changes := []int64{
				maxChange / 10, // 10% of max change
				maxChange / 5,  // 20% of max change
				maxChange / 2,  // 50% of max change
				maxChange,      // 100% of max change
				maxChange * 2,  // 200% of max change
				maxChange * 3,  // 300% of max change
			}

			for _, change := range changes {
				// Only add meaningful changes (> 0) and limit to 300% of table size
				// This ensures we don't generate unrealistic scenarios
				if change > 0 && change <= size*3 {
					combinations = append(combinations, []int64{int64(id), size, change, time})
					id++
				}
			}
		}
	}
	return combinations
}

// calculateMaxChange computes the maximum reasonable change for a given table size and time interval
func calculateMaxChange(tableSize, timeSinceLastAnalyze int64) int64 {
	// Determine the change rate based on table size
	var changeRate float64
	if tableSize < smallTableThreshold {
		// For small tables, use the base change rate
		changeRate = baseChangeRate
	} else {
		// For larger tables, apply a logarithmic decay to the change rate
		// This assumes that larger tables change proportionally slower
		changeRate = baseChangeRate * math.Pow(0.5, math.Log10(float64(tableSize))/changeRateDecayLog)
	}

	// Calculate the maximum change
	maxChange := float64(tableSize) * changeRate * float64(timeSinceLastAnalyze)

	// Cap the maximum change at a percentage of the table size to prevent unrealistic scenarios
	return int64(math.Min(maxChange, float64(tableSize)*maxChangePercentage))
}

// generateTestData generates test data for the priority calculator
func generateTestData(t *testing.T) []TestJob {
	tableSizes := []int64{
		1000,      // 1K
		5000,      // 5K
		10000,     // 10K
		50000,     // 50K
		100000,    // 100K
		500000,    // 500K
		1000000,   // 1M
		5000000,   // 5M
		10000000,  // 10M
		50000000,  // 50M
		100000000, // 100M
	}
	analyzeTimes := []int64{
		10,     // 10 seconds
		60,     // 1 minute
		300,    // 5 minutes
		900,    // 15 minutes
		1800,   // 30 minutes
		3600,   // 1 hour
		7200,   // 2 hours
		14400,  // 4 hours
		28800,  // 8 hours
		43200,  // 12 hours
		86400,  // 1 day
		172800, // 2 days
		259200, // 3 days
	}

	combinations := generateCombinations(tableSizes, analyzeTimes)

	jobs := make([]TestJob, len(combinations))
	for i, combo := range combinations {
		jobs[i] = TestJob{
			ID:                   int(combo[0]),
			TableSize:            float64(combo[1]),
			Changes:              float64(combo[2]),
			TimeSinceLastAnalyze: float64(combo[3]),
		}
	}

	t.Logf("Generated %d test jobs", len(jobs))
	return jobs
}

type jobWithPriority struct {
	job         TestJob
	priority    float64
	changeRatio float64
}

type TestJob struct {
	ID                   int
	TableSize            float64
	Changes              float64
	TimeSinceLastAnalyze float64
}

// Analyze implements AnalysisJob.
func (j *TestJob) Analyze(statsHandle types.StatsHandle, sysProcTracker sysproctrack.Tracker) error {
	panic("unimplemented")
}

// GetWeight implements AnalysisJob.
func (j *TestJob) GetWeight() float64 {
	panic("unimplemented")
}

// IsValidToAnalyze implements AnalysisJob.
func (j *TestJob) IsValidToAnalyze(sctx sessionctx.Context) (bool, string) {
	panic("unimplemented")
}

// SetWeight implements AnalysisJob.
func (j *TestJob) SetWeight(weight float64) {
	panic("unimplemented")
}

// String implements AnalysisJob.
func (j *TestJob) String() string {
	panic("unimplemented")
}

// GetTableID implements AnalysisJob.
func (j *TestJob) GetTableID() int64 {
	return int64(j.ID)
}

func (j *TestJob) GetIndicators() priorityqueue.Indicators {
	return priorityqueue.Indicators{
		ChangePercentage:     j.Changes / j.TableSize,
		TableSize:            j.TableSize,
		LastAnalysisDuration: time.Duration(j.TimeSinceLastAnalyze) * time.Second,
	}
}

func (j *TestJob) SetIndicators(indicators priorityqueue.Indicators) {
	panic("unimplemented")
}

func (j *TestJob) HasNewlyAddedIndex() bool {
	return false
}
