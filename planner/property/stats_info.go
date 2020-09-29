// Copyright 2018 PingCAP, Inc.
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

package property

import (
	"fmt"

	"github.com/pingcap/tidb/statistics"
)

// GroupNDV stores the cardinality of a group of columns.
type GroupNDV struct {
	// Cols are the UniqueIDs of columns.
	Cols []int64
	NDV  float64
}

// ToString prints GroupNDV slice. It is only used for test.
func ToString(ndvs []GroupNDV) string {
	return fmt.Sprintf("%v", ndvs)
}

// StatsInfo stores the basic information of statistics for the plan's output. It is used for cost estimation.
type StatsInfo struct {
	RowCount float64

	// Column.UniqueID -> Cardinality
	Cardinality map[int64]float64

	HistColl *statistics.HistColl
	// StatsVersion indicates the statistics version of a table.
	// If the StatsInfo is calculated using the pseudo statistics on a table, StatsVersion will be PseudoVersion.
	StatsVersion uint64

	// GroupNDVs stores the cardinality of column groups.
	GroupNDVs []GroupNDV
}

// String implements fmt.Stringer interface.
func (s *StatsInfo) String() string {
	return fmt.Sprintf("count %v, Cardinality %v", s.RowCount, s.Cardinality)
}

// Count gets the RowCount in the StatsInfo.
func (s *StatsInfo) Count() int64 {
	return int64(s.RowCount)
}

// Scale receives a selectivity and multiplies it with RowCount and Cardinality.
func (s *StatsInfo) Scale(factor float64) *StatsInfo {
	profile := &StatsInfo{
		RowCount:     s.RowCount * factor,
		Cardinality:  make(map[int64]float64, len(s.Cardinality)),
		HistColl:     s.HistColl,
		StatsVersion: s.StatsVersion,
		GroupNDVs:    make([]GroupNDV, len(s.GroupNDVs)),
	}
	for id, c := range s.Cardinality {
		profile.Cardinality[id] = c * factor
	}
	for i, g := range s.GroupNDVs {
		profile.GroupNDVs[i] = g
		profile.GroupNDVs[i].NDV = g.NDV * factor
	}
	return profile
}

// ScaleByExpectCnt tries to Scale StatsInfo to an expectCnt which must be
// smaller than the derived cnt.
// TODO: try to use a better way to do this.
func (s *StatsInfo) ScaleByExpectCnt(expectCnt float64) *StatsInfo {
	if expectCnt >= s.RowCount {
		return s
	}
	if s.RowCount > 1.0 { // if s.RowCount is too small, it will cause overflow
		return s.Scale(expectCnt / s.RowCount)
	}
	return s
}
