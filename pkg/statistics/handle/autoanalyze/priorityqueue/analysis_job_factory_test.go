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
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestCalculateChangePercentage(t *testing.T) {
	tests := []struct {
		name             string
		tblStats         *statistics.Table
		autoAnalyzeRatio float64
		want             float64
	}{
		{
			name: "Unanalyzed table",
			tblStats: &statistics.Table{
				HistColl:              *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, nil, nil),
				ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 0),
			},
			autoAnalyzeRatio: 0.5,
			want:             1,
		},
		{
			name: "Analyzed table with change percentage above threshold",
			tblStats: &statistics.Table{
				HistColl:              *statistics.NewHistCollWithColsAndIdxs(0, false, 100, 60, nil, nil),
				ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(1, 1),
				LastAnalyzeVersion:    1,
			},
			autoAnalyzeRatio: 0.5,
			want:             0.6,
		},
		{
			name: "Analyzed table with change percentage below threshold",
			tblStats: &statistics.Table{
				HistColl:              *statistics.NewHistCollWithColsAndIdxs(0, false, 100, 40, nil, nil),
				ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(1, 1),
				LastAnalyzeVersion:    1,
			},
			autoAnalyzeRatio: 0.5,
			want:             0,
		},
		{
			name: "Auto analyze ratio set to 0",
			tblStats: &statistics.Table{
				HistColl:              *statistics.NewHistCollWithColsAndIdxs(0, false, 100, 60, nil, nil),
				ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(1, 1),
				LastAnalyzeVersion:    1,
			},
			autoAnalyzeRatio: 0,
			want:             0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := priorityqueue.NewAnalysisJobFactory(nil, tt.autoAnalyzeRatio, 0)
			got := factory.CalculateChangePercentage(tt.tblStats)
			require.InDelta(t, tt.want, got, 0.001)
		})
	}
}

func TestGetTableLastAnalyzeDuration(t *testing.T) {
	tests := []struct {
		name         string
		tblStats     *statistics.Table
		currentTs    uint64
		wantDuration time.Duration
	}{
		{
			name: "Analyzed table",
			tblStats: &statistics.Table{
				LastAnalyzeVersion: oracle.GoTimeToTS(time.Now().Add(-24 * time.Hour)),
			},
			currentTs:    oracle.GoTimeToTS(time.Now()),
			wantDuration: 24 * time.Hour,
		},
		{
			name: "Unanalyzed table",
			tblStats: &statistics.Table{
				HistColl: statistics.HistColl{},
			},
			currentTs:    oracle.GoTimeToTS(time.Now()),
			wantDuration: 30 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := priorityqueue.NewAnalysisJobFactory(nil, 0, tt.currentTs)
			got := factory.GetTableLastAnalyzeDuration(tt.tblStats)
			require.InDelta(t, tt.wantDuration, got, float64(time.Second))
		})
	}
}

func TestCheckIndexesNeedAnalyze(t *testing.T) {
	analyzedMap := statistics.NewColAndIndexExistenceMap(1, 0)
	analyzedMap.InsertCol(1, true)
	analyzedMap.InsertIndex(1, false)
	tests := []struct {
		name     string
		tblInfo  *model.TableInfo
		tblStats *statistics.Table
		want     []string
	}{
		{
			name: "Test Table not analyzed",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  pmodel.NewCIStr("index1"),
						State: model.StatePublic,
					},
				},
			},
			tblStats: &statistics.Table{ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 0)},
			want:     nil,
		},
		{
			name: "Test Index not analyzed",
			tblInfo: &model.TableInfo{
				Indices: []*model.IndexInfo{
					{
						ID:    1,
						Name:  pmodel.NewCIStr("index1"),
						State: model.StatePublic,
					},
					{
						ID:         2,
						Name:       pmodel.NewCIStr("vec_index1"),
						State:      model.StatePublic,
						VectorInfo: &model.VectorIndexInfo{},
					},
				},
			},
			tblStats: &statistics.Table{
				HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, 0, 0, map[int64]*statistics.Column{
					1: {
						StatsVer: 2,
					},
				}, nil),
				ColAndIdxExistenceMap: analyzedMap,
				LastAnalyzeVersion:    1,
			},
			want: []string{"index1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := priorityqueue.NewAnalysisJobFactory(nil, 0, 0)
			got := factory.CheckIndexesNeedAnalyze(tt.tblInfo, tt.tblStats)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCalculateIndicatorsForPartitions(t *testing.T) {
	// 2024-01-01 10:00:00
	currentTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	currentTs := oracle.GoTimeToTS(currentTime)
	// 2023-12-31 10:00:00
	lastUpdateTime := time.Date(2023, 12, 31, 10, 0, 0, 0, time.UTC)
	lastUpdateTs := oracle.GoTimeToTS(lastUpdateTime)
	unanalyzedMap := statistics.NewColAndIndexExistenceMap(0, 0)
	analyzedMap := statistics.NewColAndIndexExistenceMap(2, 1)
	analyzedMap.InsertCol(1, true)
	analyzedMap.InsertCol(2, true)
	analyzedMap.InsertIndex(1, true)
	tests := []struct {
		name                       string
		globalStats                *statistics.Table
		partitionStats             map[priorityqueue.PartitionIDAndName]*statistics.Table
		defs                       []model.PartitionDefinition
		autoAnalyzeRatio           float64
		currentTs                  uint64
		wantAvgChangePercentage    float64
		wantAvgSize                float64
		wantAvgLastAnalyzeDuration time.Duration
		wantPartitions             []string
	}{
		{
			name: "Test Table not analyzed",
			globalStats: &statistics.Table{
				ColAndIdxExistenceMap: analyzedMap,
			},
			partitionStats: map[priorityqueue.PartitionIDAndName]*statistics.Table{
				priorityqueue.NewPartitionIDAndName("p0", 1): {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: statistics.AutoAnalyzeMinCnt + 1,
					},
					ColAndIdxExistenceMap: unanalyzedMap,
				},
				priorityqueue.NewPartitionIDAndName("p1", 2): {
					HistColl: statistics.HistColl{
						Pseudo:        false,
						RealtimeCount: statistics.AutoAnalyzeMinCnt + 1,
					},
					ColAndIdxExistenceMap: unanalyzedMap,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: pmodel.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: pmodel.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    1,
			wantAvgSize:                2002,
			wantAvgLastAnalyzeDuration: 1800 * time.Second,
			wantPartitions:             []string{"p0", "p1"},
		},
		{
			name: "Test Table analyzed and only one partition meets the threshold",
			globalStats: &statistics.Table{
				ColAndIdxExistenceMap: analyzedMap,
			},
			partitionStats: map[priorityqueue.PartitionIDAndName]*statistics.Table{
				priorityqueue.NewPartitionIDAndName("p0", 1): {
					HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, (statistics.AutoAnalyzeMinCnt+1)*2, map[int64]*statistics.Column{
						1: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
						2: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
					}, nil),
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
				priorityqueue.NewPartitionIDAndName("p1", 2): {
					HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, map[int64]*statistics.Column{
						1: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
						2: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
					}, nil),
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: pmodel.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: pmodel.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    2,
			wantAvgSize:                2002,
			wantAvgLastAnalyzeDuration: 24 * time.Hour,
			wantPartitions:             []string{"p0"},
		},
		{
			name: "No partition meets the threshold",
			globalStats: &statistics.Table{
				ColAndIdxExistenceMap: analyzedMap,
			},
			partitionStats: map[priorityqueue.PartitionIDAndName]*statistics.Table{
				priorityqueue.NewPartitionIDAndName("p0", 1): {
					HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, map[int64]*statistics.Column{
						1: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
						2: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
					}, nil),
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
				priorityqueue.NewPartitionIDAndName("p1", 2): {
					HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, map[int64]*statistics.Column{
						1: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
						2: {
							StatsVer: 2,
							Histogram: statistics.Histogram{
								LastUpdateVersion: lastUpdateTs,
							},
						},
					}, nil),
					Version:               currentTs,
					ColAndIdxExistenceMap: analyzedMap,
					LastAnalyzeVersion:    lastUpdateTs,
				},
			},
			defs: []model.PartitionDefinition{
				{
					ID:   1,
					Name: pmodel.NewCIStr("p0"),
				},
				{
					ID:   2,
					Name: pmodel.NewCIStr("p1"),
				},
			},
			autoAnalyzeRatio:           0.5,
			currentTs:                  currentTs,
			wantAvgChangePercentage:    0,
			wantAvgSize:                0,
			wantAvgLastAnalyzeDuration: 0,
			wantPartitions:             []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := priorityqueue.NewAnalysisJobFactory(nil, tt.autoAnalyzeRatio, tt.currentTs)
			gotAvgChangePercentage,
				gotAvgSize,
				gotAvgLastAnalyzeDuration,
				gotPartitions :=
				factory.CalculateIndicatorsForPartitions(
					tt.globalStats,
					tt.partitionStats,
				)
			require.Equal(t, tt.wantAvgChangePercentage, gotAvgChangePercentage)
			require.Equal(t, tt.wantAvgSize, gotAvgSize)
			require.Equal(t, tt.wantAvgLastAnalyzeDuration, gotAvgLastAnalyzeDuration)
			// Sort the partitions.
			sort.Strings(tt.wantPartitions)
			sort.Strings(gotPartitions)
			require.Equal(t, tt.wantPartitions, gotPartitions)
		})
	}
}

func TestCheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(t *testing.T) {
	tblInfo := model.TableInfo{
		Indices: []*model.IndexInfo{
			{
				ID:    1,
				Name:  pmodel.NewCIStr("index1"),
				State: model.StatePublic,
			},
			{
				ID:    2,
				Name:  pmodel.NewCIStr("index2"),
				State: model.StatePublic,
			},
			{
				ID:         3,
				Name:       pmodel.NewCIStr("index3"),
				State:      model.StatePublic,
				VectorInfo: &model.VectorIndexInfo{},
			},
		},
		Columns: []*model.ColumnInfo{
			{
				ID: 1,
			},
			{
				ID: 2,
			},
		},
	}
	partitionStats := map[priorityqueue.PartitionIDAndName]*statistics.Table{
		priorityqueue.NewPartitionIDAndName("p0", 1): {
			HistColl:              *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, nil, map[int64]*statistics.Index{}),
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 0),
		},
		priorityqueue.NewPartitionIDAndName("p1", 2): {
			HistColl: *statistics.NewHistCollWithColsAndIdxs(0, false, statistics.AutoAnalyzeMinCnt+1, 0, nil, map[int64]*statistics.Index{
				2: {
					StatsVer: 2,
				},
			}),
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(0, 1),
		},
	}

	factory := priorityqueue.NewAnalysisJobFactory(nil, 0, 0)
	partitionIndexes := factory.CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(&tblInfo, partitionStats)
	expected := map[string][]string{"index1": {"p0", "p1"}, "index2": {"p0"}}
	require.Equal(t, len(expected), len(partitionIndexes))

	for k, v := range expected {
		sort.Strings(v)
		if val, ok := partitionIndexes[k]; ok {
			sort.Strings(val)
			require.Equal(t, v, val)
		} else {
			require.Fail(t, "key not found in partitionIndexes: "+k)
		}
	}
}

func TestAutoAnalysisTimeWindow(t *testing.T) {
	tests := []struct {
		name       string
		start      time.Time
		end        time.Time
		current    time.Time
		wantWithin bool
	}{
		{
			name:       "Within time window",
			start:      time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			end:        time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC),
			current:    time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
			wantWithin: true,
		},
		{
			name:       "Outside time window",
			start:      time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			end:        time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC),
			current:    time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),
			wantWithin: false,
		},
		{
			name:       "Empty time window",
			start:      time.Time{},
			end:        time.Time{},
			current:    time.Now(),
			wantWithin: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := priorityqueue.NewAutoAnalysisTimeWindow(tt.start, tt.end)
			got := window.IsWithinTimeWindow(tt.current)
			require.Equal(t, tt.wantWithin, got)
		})
	}
}
