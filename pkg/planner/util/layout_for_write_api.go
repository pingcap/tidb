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

package util

import (
	"cmp"
	"slices"

	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/size"
)

// TblColPosInfo represents a mapper from column index to handle index.
// This struct is used for our write APIs like INSERT/UPDATE/DELETE.
// Sometimes we need it because what planner output is a mixed row from multiple tables.
// We need to tell the executor which part of the mixed row belongs to which table.
// Sometimes we need it because we prune some columns, just leave the primary key and other indexed columns.
// At this time, we need to tell the write API how to get the one index's key value from the pruned and mixed row.
type TblColPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleCols HandleCols

	*table.ExtraPartialRowOption
}

// MemoryUsage return the memory usage of TblColPosInfo
func (t *TblColPosInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInt64 + size.SizeOfInt*2
	if t.HandleCols != nil {
		sum += t.HandleCols.MemoryUsage()
	}
	return
}

// TblColPosInfoSlice attaches the methods of sort.Interface to []TblColPosInfos sorting in increasing order.
type TblColPosInfoSlice []TblColPosInfo

// SortByStart sorts the slice by the start pos
func (c TblColPosInfoSlice) SortByStart() {
	slices.SortFunc(c, func(x, y TblColPosInfo) int {
		return cmp.Compare(x.Start, y.Start)
	})
}

// FindTblIdx finds the ordinal of the corresponding access column.
func (c TblColPosInfoSlice) FindTblIdx(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// Golang's official binary search returns either the place where the cmp func is 0,
	// or the i which makes cmp func(i) < 0 and cmp func(i+1) > 0.
	rangeMayBehindOrdinal, ok := slices.BinarySearchFunc(
		c,
		colOrdinal,
		func(colPosInfo TblColPosInfo, colOrdinal int) int {
			if colPosInfo.Start == colOrdinal {
				return 0
			}
			if colPosInfo.Start < colOrdinal {
				return -1
			}
			return 1
		})

	if rangeMayBehindOrdinal == 0 && !ok {
		return 0, false
	}
	if ok {
		return rangeMayBehindOrdinal, true
	}
	return rangeMayBehindOrdinal - 1, true
}
