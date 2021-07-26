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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"
)

// AnalyzeTableID is hybrid table id used to analyze table.
type AnalyzeTableID struct {
	TableID int64
	// PartitionID is used for the construction of partition table statistics. It indicate the ID of the partition.
	// If the table is not the partition table, the PartitionID will be equal to -1.
	PartitionID int64
}

// GetStatisticsID is used to obtain the table ID to build statistics.
// If the 'PartitionID == -1', we use the TableID to build the statistics for non-partition tables.
// Otherwise, we use the PartitionID to build the statistics of the partitions in the partition tables.
func (h *AnalyzeTableID) GetStatisticsID() int64 {
	statisticsID := h.TableID
	if h.PartitionID != -1 {
		statisticsID = h.PartitionID
	}
	return statisticsID
}

// IsPartitionTable indicates whether the table is partition table.
func (h *AnalyzeTableID) IsPartitionTable() bool {
	return h.PartitionID != -1
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

// AnalyzeResults represents the analyze results of a task.
type AnalyzeResults struct {
	TableID  AnalyzeTableID
	Ars      []*AnalyzeResult
	Count    int64
	ExtStats *ExtendedStatsColl
	Err      error
	Job      *AnalyzeJob
	StatsVer int
	Snapshot uint64
	// BaseCount is the original count in mysql.stats_meta at the beginning of analyze.
	BaseCount int64
	// BaseModifyCnt is the original modify_count in mysql.stats_meta at the beginning of analyze.
	BaseModifyCnt int64
}
