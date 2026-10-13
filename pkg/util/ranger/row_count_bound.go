// Copyright 2026 PingCAP, Inc.
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

package ranger

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
)

// RowCountUpperBoundByUniqueKey returns a proven upper bound of the number of rows that an index scan
// over ranges can return, and whether such a bound exists.
//
// idxCols and idxColLens are the columns the ranges are built on, i.e. PhysicalIndexScan.IdxCols and
// IdxColLens, which may include the int handle appended for a non-unique index.
//
// The bound is len(ranges) when every range fixes all columns of some unique key of the table to non-NULL
// values, since such a range can match at most one row. Otherwise no bound is returned.
func RowCountUpperBoundByUniqueKey(tc types.Context, tblInfo *model.TableInfo, idxCols []*expression.Column,
	idxColLens []int, ranges []*Range) (uint64, bool) {
	if tblInfo == nil || len(ranges) == 0 || len(idxCols) == 0 || len(idxCols) != len(idxColLens) {
		return 0, false
	}
	uniqueKeys := uniqueKeyColumnIDs(tblInfo)
	if len(uniqueKeys) == 0 {
		return 0, false
	}
	fixed := make(map[int64]struct{}, len(idxCols))
	for _, ran := range ranges {
		clear(fixed)
		if !ran.collectFixedNotNullColumns(tc, idxCols, idxColLens, fixed) || !coversAnyKey(fixed, uniqueKeys) {
			return 0, false
		}
	}
	return uint64(len(ranges)), true
}

// uniqueKeyColumnIDs returns the column IDs of each unique key that is enforced on all rows of the table.
func uniqueKeyColumnIDs(tblInfo *model.TableInfo) [][]int64 {
	keys := make([][]int64, 0, len(tblInfo.Indices)+1)
	if tblInfo.PKIsHandle {
		if pkCol := tblInfo.GetPkColInfo(); pkCol != nil {
			keys = append(keys, []int64{pkCol.ID})
		}
	}
	for _, idx := range tblInfo.Indices {
		// A non-public index may not be unique on existing rows yet, a partial index is only unique on rows
		// matching its condition, and a prefix or multi-valued index is not unique on the full column values.
		if !(idx.Unique || idx.Primary) || idx.State != model.StatePublic ||
			idx.MVIndex || idx.HasCondition() || idx.HasPrefixIndex() {
			continue
		}
		ids := make([]int64, 0, len(idx.Columns))
		for _, idxCol := range idx.Columns {
			ids = append(ids, tblInfo.Columns[idxCol.Offset].ID)
		}
		keys = append(keys, ids)
	}
	return keys
}

// collectFixedNotNullColumns adds to fixed the IDs of the leading index columns that the range fixes to a single
// non-NULL value of the full column. It returns false if the range is malformed for this check.
func (ran *Range) collectFixedNotNullColumns(tc types.Context, idxCols []*expression.Column, idxColLens []int,
	fixed map[int64]struct{}) bool {
	n := min(len(ran.LowVal), len(ran.HighVal), len(idxCols), len(ran.Collators))
	allFixed := true
	for i := range n {
		low, high := ran.LowVal[i], ran.HighVal[i]
		if low.Kind() == types.KindMinNotNull || low.Kind() == types.KindMaxValue ||
			high.Kind() == types.KindMinNotNull || high.Kind() == types.KindMaxValue {
			allFixed = false
			break
		}
		cmp, err := low.Compare(tc, &high, ran.Collators[i])
		if err != nil || cmp != 0 {
			allFixed = false
			break
		}
		// The column is fixed, but only a full-length non-NULL value can be matched against a unique key:
		// a unique index allows duplicated NULLs, and a prefix only fixes part of the value.
		if !low.IsNull() && idxColLens[i] == types.UnspecifiedLength {
			fixed[idxCols[i].ID] = struct{}{}
		}
	}
	// If every column of the range is fixed, an exclusive end means the range is not a point.
	if allFixed && len(ran.LowVal) == len(ran.HighVal) && n == len(ran.LowVal) && (ran.LowExclude || ran.HighExclude) {
		return false
	}
	return true
}

func coversAnyKey(fixed map[int64]struct{}, keys [][]int64) bool {
	for _, key := range keys {
		covered := true
		for _, id := range key {
			if _, ok := fixed[id]; !ok {
				covered = false
				break
			}
		}
		if covered {
			return true
		}
	}
	return false
}
