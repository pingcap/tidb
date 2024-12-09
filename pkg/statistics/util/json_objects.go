// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"cmp"
	"slices"

	"github.com/pingcap/tipb/go-tipb"
)

const (
	// TiDBGlobalStats represents the global-stats for a partitioned table.
	TiDBGlobalStats = "global"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	Columns           map[string]*JSONColumn `json:"columns"`
	Indices           map[string]*JSONColumn `json:"indices"`
	Partitions        map[string]*JSONTable  `json:"partitions"`
	DatabaseName      string                 `json:"database_name"`
	TableName         string                 `json:"table_name"`
	ExtStats          []*JSONExtendedStats   `json:"ext_stats"`
	PredicateColumns  []*JSONPredicateColumn `json:"predicate_columns"`
	Count             int64                  `json:"count"`
	ModifyCount       int64                  `json:"modify_count"`
	Version           uint64                 `json:"version"`
	IsHistoricalStats bool                   `json:"is_historical_stats"`
}

// Sort is used to sort the object in the JSONTable. it is used for testing to avoid flaky test.
func (j *JSONTable) Sort() {
	slices.SortFunc(j.PredicateColumns, func(a, b *JSONPredicateColumn) int {
		return cmp.Compare(a.ID, b.ID)
	})
}

// JSONExtendedStats is used for dumping extended statistics.
type JSONExtendedStats struct {
	StatsName  string  `json:"stats_name"`
	StringVals string  `json:"string_vals"`
	ColIDs     []int64 `json:"cols"`
	ScalarVals float64 `json:"scalar_vals"`
	Tp         uint8   `json:"type"`
}

// JSONColumn is used for dumping statistics.
type JSONColumn struct {
	Histogram *tipb.Histogram `json:"histogram"`
	CMSketch  *tipb.CMSketch  `json:"cm_sketch"`
	FMSketch  *tipb.FMSketch  `json:"fm_sketch"`
	// StatsVer is a pointer here since the old version json file would not contain version information.
	StatsVer          *int64  `json:"stats_ver"`
	NullCount         int64   `json:"null_count"`
	TotColSize        int64   `json:"tot_col_size"`
	LastUpdateVersion uint64  `json:"last_update_version"`
	Correlation       float64 `json:"correlation"`
}

// TotalMemoryUsage returns the total memory usage of this column.
func (col *JSONColumn) TotalMemoryUsage() (size int64) {
	if col.Histogram != nil {
		size += int64(col.Histogram.Size())
	}
	if col.CMSketch != nil {
		size += int64(col.CMSketch.Size())
	}
	if col.FMSketch != nil {
		size += int64(col.FMSketch.Size())
	}
	return size
}

// JSONPredicateColumn contains the information of the columns used in the predicate.
type JSONPredicateColumn struct {
	LastUsedAt     *string `json:"last_used_at"`
	LastAnalyzedAt *string `json:"last_analyzed_at"`
	ID             int64   `json:"id"`
}
