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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCalculateWeight(t *testing.T) {
	pc := NewPriorityCalculator(0.5)
	job := &TableAnalysisJob{
		ChangePercentage:     0.6,
		TableSize:            1000,
		LastAnalysisDuration: time.Duration(3600) * time.Second, // 1 hour
		PartitionIndexes:     map[string][]string{},
		Indexes:              []string{},
	}

	require.Equal(t, 1.4067534437617586, pc.CalculateWeight(job))
}

func TestGetSpecialEvent(t *testing.T) {
	pc := NewPriorityCalculator(0.5)

	jobWithIndex := &TableAnalysisJob{
		PartitionIndexes: map[string][]string{
			"index1": {"p1", "p2"},
		},
	}
	require.Equal(t, eventNewIndex, pc.getSpecialEvent(jobWithIndex))

	jobWithIndex = &TableAnalysisJob{
		Indexes: []string{"index1"},
	}
	require.Equal(t, eventNewIndex, pc.getSpecialEvent(jobWithIndex))

	jobWithoutIndex := &TableAnalysisJob{
		PartitionIndexes: map[string][]string{},
		Indexes:          []string{},
	}
	require.Equal(t, eventNone, pc.getSpecialEvent(jobWithoutIndex))
}
