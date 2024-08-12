// Copyright 2022 PingCAP, Inc.
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
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// CloneFieldNames uses types.FieldName.Clone to clone a slice of types.FieldName.
func CloneFieldNames(names []*types.FieldName) []*types.FieldName {
	if names == nil {
		return nil
	}
	cloned := make([]*types.FieldName, len(names))
	for i, name := range names {
		cloned[i] = new(types.FieldName)
		*cloned[i] = *name
	}
	return cloned
}

// CloneCIStrs uses model.CIStr.Clone to clone a slice of model.CIStr.
func CloneCIStrs(strs []model.CIStr) []model.CIStr {
	if strs == nil {
		return nil
	}
	cloned := make([]model.CIStr, 0, len(strs))
	cloned = append(cloned, strs...)
	return cloned
}

// CloneExprs uses Expression.Clone to clone a slice of Expression.
func CloneExprs(exprs []expression.Expression) []expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, e.Clone())
	}
	return cloned
}

// CloneExpressions uses CloneExprs to clone a slice of expression.Expression.
func CloneExpressions(exprs []expression.Expression) []expression.Expression {
	return CloneExprs(exprs)
}

// CloneExpression2D uses CloneExprs to clone a 2D slice of expression.Expression.
func CloneExpression2D(exprs [][]expression.Expression) [][]expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([][]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, CloneExprs(e))
	}
	return cloned
}

// CloneScalarFunctions uses (*ScalarFunction).Clone to clone a slice of *ScalarFunction.
func CloneScalarFunctions(scalarFuncs []*expression.ScalarFunction) []*expression.ScalarFunction {
	if scalarFuncs == nil {
		return nil
	}
	cloned := make([]*expression.ScalarFunction, 0, len(scalarFuncs))
	for _, f := range scalarFuncs {
		cloned = append(cloned, f.Clone().(*expression.ScalarFunction))
	}
	return cloned
}

// CloneAssignments uses (*Assignment).Clone to clone a slice of *Assignment.
func CloneAssignments(assignments []*expression.Assignment) []*expression.Assignment {
	if assignments == nil {
		return nil
	}
	cloned := make([]*expression.Assignment, 0, len(assignments))
	for _, a := range assignments {
		cloned = append(cloned, a.Clone())
	}
	return cloned
}

// CloneHandleCols uses HandleCols.Clone to clone a slice of HandleCols.
func CloneHandleCols(newCtx *stmtctx.StatementContext, handles []HandleCols) []HandleCols {
	if handles == nil {
		return nil
	}
	cloned := make([]HandleCols, 0, len(handles))
	for _, h := range handles {
		cloned = append(cloned, h.Clone(newCtx))
	}
	return cloned
}

// CloneCols uses (*Column).Clone to clone a slice of *Column.
func CloneCols(cols []*expression.Column) []*expression.Column {
	if cols == nil {
		return nil
	}
	cloned := make([]*expression.Column, 0, len(cols))
	for _, c := range cols {
		if c == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, c.Clone().(*expression.Column))
	}
	return cloned
}

// CloneColumns uses CloneCols to clone a slice of expression.Column.
func CloneColumns(cols []*expression.Column) []*expression.Column {
	return CloneCols(cols)
}

// CloneConstants uses (*Constant).Clone to clone a slice of *Constant.
func CloneConstants(constants []*expression.Constant) []*expression.Constant {
	if constants == nil {
		return nil
	}
	cloned := make([]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		cloned = append(cloned, c.Clone().(*expression.Constant))
	}
	return cloned
}

// CloneConstant2D uses CloneConstants to clone a 2D slice of *Constant.
func CloneConstant2D(constants [][]*expression.Constant) [][]*expression.Constant {
	if constants == nil {
		return nil
	}
	cloned := make([][]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		cloned = append(cloned, CloneConstants(c))
	}
	return cloned
}

// CloneDatums uses Datum.Clone to clone a slice of Datum.
func CloneDatums(datums []types.Datum) []types.Datum {
	if datums == nil {
		return nil
	}
	cloned := make([]types.Datum, 0, len(datums))
	for _, d := range datums {
		cloned = append(cloned, *d.Clone())
	}
	return cloned
}

// CloneDatum2D uses CloneDatums to clone a 2D slice of Datum.
func CloneDatum2D(datums [][]types.Datum) [][]types.Datum {
	if datums == nil {
		return nil
	}
	cloned := make([][]types.Datum, 0, len(datums))
	for _, d := range datums {
		cloned = append(cloned, CloneDatums(d))
	}
	return cloned
}

// CloneColInfos uses (*ColumnInfo).Clone to clone a slice of *ColumnInfo.
func CloneColInfos(cols []*model.ColumnInfo) []*model.ColumnInfo {
	if cols == nil {
		return nil
	}
	cloned := make([]*model.ColumnInfo, 0, len(cols))
	for _, c := range cols {
		cloned = append(cloned, c.Clone())
	}
	return cloned
}

// CloneRanges uses (*Range).Clone to clone a slice of *Range.
func CloneRanges(ranges []*ranger.Range) []*ranger.Range {
	if ranges == nil {
		return nil
	}
	cloned := make([]*ranger.Range, 0, len(ranges))
	for _, r := range ranges {
		cloned = append(cloned, r.Clone())
	}
	return cloned
}

// CloneByItemss uses (*ByItems).Clone to clone a slice of *ByItems.
func CloneByItemss(byItems []*ByItems) []*ByItems {
	if byItems == nil {
		return nil
	}
	cloned := make([]*ByItems, 0, len(byItems))
	for _, item := range byItems {
		cloned = append(cloned, item.Clone())
	}
	return cloned
}

// CloneSortItems uses SortItem.Clone to clone a slice of SortItem.
func CloneSortItems(items []property.SortItem) []property.SortItem {
	if items == nil {
		return nil
	}
	cloned := make([]property.SortItem, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, item.Clone())
	}
	return cloned
}

// CloneHandles uses Handle.Copy to clone a slice of Handle.
func CloneHandles(handles []kv.Handle) []kv.Handle {
	if handles == nil {
		return nil
	}
	cloned := make([]kv.Handle, 0, len(handles))
	for _, h := range handles {
		cloned = append(cloned, h.Copy())
	}
	return cloned
}

// QueryTimeRange represents a time range specified by TIME_RANGE hint
type QueryTimeRange struct {
	From time.Time
	To   time.Time
}

// Condition returns a WHERE clause base on it's value
func (tr *QueryTimeRange) Condition() string {
	return fmt.Sprintf("where time>='%s' and time<='%s'",
		tr.From.Format(MetricTableTimeFormat), tr.To.Format(MetricTableTimeFormat))
}

// MetricTableTimeFormat is the time format for metric table explain and format.
const MetricTableTimeFormat = "2006-01-02 15:04:05.999"

const emptyQueryTimeRangeSize = int64(unsafe.Sizeof(QueryTimeRange{}))

// MemoryUsage return the memory usage of QueryTimeRange
func (tr *QueryTimeRange) MemoryUsage() (sum int64) {
	if tr == nil {
		return
	}
	return emptyQueryTimeRangeSize
}

// EncodeIntAsUint32 is used for LogicalPlan Interface
func EncodeIntAsUint32(result []byte, value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(result, buf[:]...)
}

// GetMaxSortPrefix returns the prefix offset of sortCols in allCols.
func GetMaxSortPrefix(sortCols, allCols []*expression.Column) []int {
	tmpSchema := expression.NewSchema(allCols...)
	sortColOffsets := make([]int, 0, len(sortCols))
	for _, sortCol := range sortCols {
		offset := tmpSchema.ColumnIndex(sortCol)
		if offset == -1 {
			return sortColOffsets
		}
		sortColOffsets = append(sortColOffsets, offset)
	}
	return sortColOffsets
}

// DeriveLimitStats derives the stats of the top-n plan.
func DeriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount: math.Min(limitCount, childProfile.RowCount),
		ColNDVs:  make(map[int64]float64, len(childProfile.ColNDVs)),
	}
	for id, c := range childProfile.ColNDVs {
		stats.ColNDVs[id] = math.Min(c, stats.RowCount)
	}
	return stats
}
