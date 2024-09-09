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

	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/stretchr/testify/require"
)

func TestStringer(t *testing.T) {
	tests := []struct {
		name string
		job  priorityqueue.AnalysisJob
		want string
	}{
		{
			name: "analyze non-partitioned table",
			job: &priorityqueue.NonPartitionedTableAnalysisJob{
				TableID:       1,
				TableSchema:   "test_schema",
				TableName:     "test_table",
				TableStatsVer: 1,
				Weight:        1.999999,
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "NonPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeTable\n\tIndexes: \n\tSchema: test_schema\n\tTable: test_table\n\tTableID: 1\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
		{
			name: "analyze non-partitioned table index",
			job: &priorityqueue.NonPartitionedTableAnalysisJob{
				TableID:       2,
				TableSchema:   "test_schema",
				TableName:     "test_table",
				Indexes:       []string{"idx"},
				TableStatsVer: 1,
				Weight:        1.999999,
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "NonPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeIndex\n\tIndexes: idx\n\tSchema: test_schema\n\tTable: test_table\n\tTableID: 2\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
		{
			name: "analyze dynamic partition",
			job: &priorityqueue.DynamicPartitionedTableAnalysisJob{
				GlobalTableID:   3,
				TableSchema:     "test_schema",
				GlobalTableName: "test_table",
				Partitions:      []string{"p0", "p1"},
				TableStatsVer:   1,
				Weight:          1.999999,
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "DynamicPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeDynamicPartition\n\tPartitions: p0, p1\n\tPartitionIndexes: map[]\n\tSchema: test_schema\n\tGlobal Table: test_table\n\tGlobal TableID: 3\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
		{
			name: "analyze dynamic partition's indexes",
			job: &priorityqueue.DynamicPartitionedTableAnalysisJob{
				GlobalTableID:   4,
				TableSchema:     "test_schema",
				GlobalTableName: "test_table",
				PartitionIndexes: map[string][]string{
					"idx": {"p0", "p1"},
				},
				TableStatsVer: 1,
				Weight:        1.999999,
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "DynamicPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeDynamicPartitionIndex\n\tPartitions: \n\tPartitionIndexes: map[idx:[p0 p1]]\n\tSchema: test_schema\n\tGlobal Table: test_table\n\tGlobal TableID: 4\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
		{
			name: "analyze static partition",
			job: &priorityqueue.StaticPartitionedTableAnalysisJob{
				GlobalTableID:       5,
				TableSchema:         "test_schema",
				GlobalTableName:     "test_table",
				StaticPartitionName: "p0",
				StaticPartitionID:   6,
				TableStatsVer:       1,
				Weight:              1.999999,
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "StaticPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeStaticPartition\n\tIndexes: \n\tSchema: test_schema\n\tGlobalTable: test_table\n\tGlobalTableID: 5\n\tStaticPartition: p0\n\tStaticPartitionID: 6\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
		{
			name: "analyze static partition's index",
			job: &priorityqueue.StaticPartitionedTableAnalysisJob{
				GlobalTableID:       7,
				TableSchema:         "test_schema",
				GlobalTableName:     "test_table",
				StaticPartitionName: "p0",
				StaticPartitionID:   8,
				TableStatsVer:       1,
				Weight:              1.999999,
				Indexes:             []string{"idx"},
				Indicators: priorityqueue.Indicators{
					ChangePercentage: 0.5,
				},
			},
			want: "StaticPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeStaticPartitionIndex\n\tIndexes: idx\n\tSchema: test_schema\n\tGlobalTable: test_table\n\tGlobalTableID: 7\n\tStaticPartition: p0\n\tStaticPartitionID: 8\n\tTableStatsVer: 1\n\tChangePercentage: 0.500000\n\tTableSize: 0.00\n\tLastAnalysisDuration: 0s\n\tWeight: 1.999999\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.job.String())
		})
	}
}
