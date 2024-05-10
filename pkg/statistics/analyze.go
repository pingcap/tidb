// Copyright 2021 PingCAP, Inc.
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

package statistics

import (
	"fmt"
)

// NonPartitionTableID is the partition id for non-partition table.
const NonPartitionTableID = -1

// AnalyzeTableID is hybrid table id used to analyze table.
type AnalyzeTableID struct {
	TableID int64
	// PartitionID is used for the construction of partition table statistics. It indicate the ID of the partition.
	// If the table is not the partition table, the PartitionID will be equal to NonPartitionTableID.
	PartitionID int64
}

// GetStatisticsID is used to obtain the table ID to build statistics.
// If the 'PartitionID == NonPartitionTableID', we use the TableID to build the statistics for non-partition tables.
// Otherwise, we use the PartitionID to build the statistics of the partitions in the partition tables.
func (h *AnalyzeTableID) GetStatisticsID() int64 {
	statisticsID := h.TableID
	if h.PartitionID != NonPartitionTableID {
		statisticsID = h.PartitionID
	}
	return statisticsID
}

// IsPartitionTable indicates whether the table is partition table.
func (h *AnalyzeTableID) IsPartitionTable() bool {
	return h.PartitionID != NonPartitionTableID
}

func (h *AnalyzeTableID) String() string {
	return fmt.Sprintf("%d => %v", h.PartitionID, h.TableID)
}

// Equals indicates whether two table id is equal.
func (h *AnalyzeTableID) Equals(t *AnalyzeTableID) bool {
	if h == t {
		return true
	}
	if h == nil || t == nil {
		return false
	}
	return h.TableID == t.TableID && h.PartitionID == t.PartitionID
}

// AnalyzeResult is used to represent analyze result.
type AnalyzeResult struct {
	Hist    []*Histogram
	Cms     []*CMSketch
	TopNs   []*TopN
	Fms     []*FMSketch
	IsIndex int
}

// DestroyAndPutToPool destroys the result and put it to the pool.
func (a *AnalyzeResult) DestroyAndPutToPool() {
	for _, f := range a.Fms {
		f.DestroyAndPutToPool()
	}
	for _, h := range a.Hist {
		h.DestroyAndPutToPool()
	}
}

// AnalyzeResults represents the analyze results of a task.
type AnalyzeResults struct {
	Err      error
	ExtStats *ExtendedStatsColl
	Job      *AnalyzeJob
	Ars      []*AnalyzeResult
	TableID  AnalyzeTableID
	Count    int64
	StatsVer int
	Snapshot uint64
	// BaseCount is the original count in mysql.stats_meta at the beginning of analyze.
	BaseCount int64
	// BaseModifyCnt is the original modify_count in mysql.stats_meta at the beginning of analyze.
	BaseModifyCnt int64
	// For multi-valued index analyze, there are some very different behaviors, so we add this field to indicate it.
	//
	// Analyze result of multi-valued index come from an independent v2 analyze index task (AnalyzeIndexExec), and it's
	// done by a scan on the index data and building stats. According to the original design rational of v2 stats, we
	// should use the same samples to build stats for all columns/indexes. We created an exceptional case here to avoid
	// loading the samples of JSON columns to tidb, which may cost too much memory, and we can't handle such case very
	// well now.
	//
	// As the definition of multi-valued index, the row count and NDV of this index may be higher than the table row
	// count. So we can't use this result to update the table-level row count.
	// The snapshot field is used by v2 analyze to check if there are concurrent analyze, so we also can't update it.
	// The multi-valued index analyze task is always together with another normal v2 analyze table task, which will
	// take care of those table-level fields.
	// In conclusion, when saving the analyze result for mv index, we need to store the index stats, as for the
	// table-level fields, we only need to update the version.
	ForMVIndex bool
}

// DestroyAndPutToPool destroys the result and put it to the pool.
func (a *AnalyzeResults) DestroyAndPutToPool() {
	for _, f := range a.Ars {
		f.DestroyAndPutToPool()
	}
}
