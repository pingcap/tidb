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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package property

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
)

// ScaleNDVFunc is used to avoid cycle import.
// `sctx` should be base.PlanContext, use any to avoid cycle import.
var ScaleNDVFunc func(vars *variable.SessionVars, originalNDV, originalRows, selectedRows float64) (newNDV float64)

// GroupNDV stores the NDV of a group of columns.
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

	// Column.UniqueID -> NDV
	ColNDVs map[int64]float64

	HistColl *statistics.HistColl
	// StatsVersion indicates the statistics version of a table.
	// If the StatsInfo is calculated using the pseudo statistics on a table, StatsVersion will be PseudoVersion.
	StatsVersion uint64

	// GroupNDVs stores the NDV of column groups.
	GroupNDVs []GroupNDV
}

// String implements fmt.Stringer interface.
func (s *StatsInfo) String() string {
	return fmt.Sprintf("count %v, ColNDVs %v", s.RowCount, s.ColNDVs)
}

// Count gets the RowCount in the StatsInfo.
func (s *StatsInfo) Count() int64 {
	return int64(s.RowCount)
}

// Scale receives a selectivity and multiplies it with RowCount and NDV.
func (s *StatsInfo) Scale(vars *variable.SessionVars, factor float64) *StatsInfo {
	originalRowCount := s.RowCount
	profile := &StatsInfo{
		RowCount:     s.RowCount * factor,
		ColNDVs:      make(map[int64]float64, len(s.ColNDVs)),
		HistColl:     s.HistColl,
		StatsVersion: s.StatsVersion,
		GroupNDVs:    make([]GroupNDV, len(s.GroupNDVs)),
	}
	for id, c := range s.ColNDVs {
		profile.ColNDVs[id] = ScaleNDVFunc(vars, c, originalRowCount, profile.RowCount)
	}
	for i, g := range s.GroupNDVs {
		profile.GroupNDVs[i] = g
		profile.GroupNDVs[i].NDV = ScaleNDVFunc(vars, g.NDV, originalRowCount, profile.RowCount)
	}
	return profile
}

// ScaleByExpectCnt tries to Scale StatsInfo to an expectCnt which must be
// smaller than the derived cnt.
// TODO: try to use a better way to do this.
func (s *StatsInfo) ScaleByExpectCnt(vars *variable.SessionVars, expectCnt float64) *StatsInfo {
	if expectCnt >= s.RowCount {
		return s
	}
	if s.RowCount > 1.0 { // if s.RowCount is too small, it will cause overflow
		return s.Scale(vars, expectCnt/s.RowCount)
	}
	return s
}

// GetGroupNDV4Cols gets the GroupNDV for the given columns.
func (s *StatsInfo) GetGroupNDV4Cols(cols []*expression.Column) *GroupNDV {
	if s == nil || len(cols) == 0 || len(s.GroupNDVs) == 0 {
		return nil
	}
	cols = expression.SortColumns(cols)
	for _, groupNDV := range s.GroupNDVs {
		if len(cols) != len(groupNDV.Cols) {
			continue
		}
		match := true
		for i, col := range groupNDV.Cols {
			if col != cols[i].UniqueID {
				match = false
				break
			}
		}
		if match {
			return &groupNDV
		}
	}
	return nil
}

// DeriveLimitStats derives the stats of the top-n plan.
func DeriveLimitStats(childProfile *StatsInfo, limitCount float64) *StatsInfo {
	stats := &StatsInfo{
		RowCount: math.Min(limitCount, childProfile.RowCount),
		ColNDVs:  make(map[int64]float64, len(childProfile.ColNDVs)),
		// limit operation does not change the histogram (kind of sample).
		HistColl: childProfile.HistColl,
	}
	for id, c := range childProfile.ColNDVs {
		stats.ColNDVs[id] = math.Min(c, stats.RowCount)
	}
	return stats
}
