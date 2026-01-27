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
	"iter"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// SliceRecursiveFlattenIter returns an iterator (iter.Seq2) that recursively iterates over all elements of an
// any-dimensional slice of any type.
// Performance note:
// For each slice, this function need to check the dynamic type before iterating over it. For each non-leaf slice, this
// function uses reflect to iterate over it. Be careful when trying to use this function in performance-critical code.
/*
Example:
	paths := [][][]*AccessPath{...}
	for idx, path := range SliceRecursiveFlattenIter[*AccessPath](paths) {
		// path is a *AccessPath here
	}
*/
func SliceRecursiveFlattenIter[E any, T any, Slice ~[]T](s Slice) iter.Seq2[int, E] {
	return func(yield func(int, E) bool) {
		sliceRecursiveFlattenIterHelper(s, yield, 0)
	}
}

func sliceRecursiveFlattenIterHelper[E any, Slice any](
	s Slice,
	yield func(int, E) bool,
	startIdx int,
) (nextIdx int, stop bool) {
	intest.AssertFunc(func() bool {
		return reflect.TypeOf(s).Kind() == reflect.Slice
	})
	// Case 1: Input slice is []E, which means it's already the lowest level.
	if leafSlice, isLeafSlice := any(s).([]E); isLeafSlice {
		idx := startIdx
		for _, v := range leafSlice {
			if !yield(idx, v) {
				return idx + 1, true
			}
			idx++
		}
		return idx, false
	}
	// Case 2: Otherwise, element of Slice is still a slice, we need to flatten it recursively.
	idx := startIdx
	// We have to use reflect to iterate over the slice here.
	v := reflect.ValueOf(s)
	for i := range v.Len() {
		val := v.Index(i).Interface()
		intest.AssertFunc(func() bool {
			return reflect.TypeOf(val).Kind() == reflect.Slice
		})
		idx, stop = sliceRecursiveFlattenIterHelper[E](val, yield, idx)
		if stop {
			return idx, true
		}
	}
	return idx, false
}

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
func CloneHandleCols(handles []HandleCols) []HandleCols {
	if handles == nil {
		return nil
	}
	cloned := make([]HandleCols, 0, len(handles))
	for _, h := range handles {
		cloned = append(cloned, h.Clone())
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

// ExtractTableAlias returns table alias of the base.LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the base.LogicalPlan Match some optimizer hints, and hints are not expected to take effect in this case.
// Columns with empty table names are ignored when determining the alias.
func ExtractTableAlias(p base.Plan, parentOffset int) *h.HintedTable {
	names := p.OutputNames()
	if len(names) == 0 {
		return nil
	}
	var firstName *types.FieldName
	for _, name := range names {
		if name.TblName.L != "" {
			firstName = name
			break
		}
	}
	if firstName == nil {
		return nil
	}
	for _, name := range names {
		if name.TblName.L == "" {
			continue
		}
		if name.TblName.L != firstName.TblName.L ||
			(name.DBName.L != "" && firstName.DBName.L != "" &&
				name.DBName.L != firstName.DBName.L) { // DBName can be nil, see #46160
			return nil
		}
	}
	qbOffset := p.QueryBlockOffset()
	var blockAsNames []ast.HintTable
	if p := p.SCtx().GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		blockAsNames = *p
	}
	// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
	if qbOffset != parentOffset && blockAsNames != nil && blockAsNames[qbOffset].TableName.L != "" {
		qbOffset = parentOffset
	}
	dbName := firstName.DBName
	if dbName.L == "" {
		dbName = ast.NewCIStr(p.SCtx().GetSessionVars().CurrentDB)
	}
	return &h.HintedTable{DBName: dbName, TblName: firstName.TblName, SelectOffset: qbOffset}
}

// GetPushDownCtx creates a PushDownContext from PlanContext
func GetPushDownCtx(pctx base.PlanContext) expression.PushDownContext {
	return GetPushDownCtxFromBuildPBContext(pctx.GetBuildPBCtx())
}

// GetPushDownCtxFromBuildPBContext creates a PushDownContext from BuildPBContext
func GetPushDownCtxFromBuildPBContext(bctx *base.BuildPBContext) expression.PushDownContext {
	return expression.NewPushDownContext(bctx.GetExprCtx().GetEvalCtx(), bctx.GetClient(),
		bctx.InExplainStmt, bctx.WarnHandler, bctx.ExtraWarnghandler, bctx.GroupConcatMaxLen)
}

// FilterPathByIsolationRead filter out AccessPath by isolation read engine requirement.
func FilterPathByIsolationRead(ctx base.PlanContext, paths []*AccessPath, tblName ast.CIStr,
	dbName ast.CIStr) ([]*AccessPath, error) {
	// TODO: filter paths with isolation read locations.
	if metadef.IsSystemRelatedDB(dbName.L) {
		return paths, nil
	}
	isolationReadEngines := ctx.GetSessionVars().GetIsolationReadEngines()
	availableEngine := map[kv.StoreType]struct{}{}
	var availableEngineStr string
	for i := len(paths) - 1; i >= 0; i-- {
		// availableEngineStr is for warning message.
		if _, ok := availableEngine[paths[i].StoreType]; !ok {
			availableEngine[paths[i].StoreType] = struct{}{}
			if availableEngineStr != "" {
				availableEngineStr += ", "
			}
			availableEngineStr += paths[i].StoreType.Name()
		}
		if _, ok := isolationReadEngines[paths[i].StoreType]; !ok && paths[i].StoreType != kv.TiDB {
			paths = slices.Delete(paths, i, i+1)
		}
	}
	var err error
	engineVals, _ := ctx.GetSessionVars().GetSystemVar(vardef.TiDBIsolationReadEngines)
	if len(paths) == 0 {
		helpMsg := ""
		if strings.Contains(engineVals, "tiflash") {
			helpMsg = ". Please check tiflash replica"
			if ctx.GetSessionVars().StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
				helpMsg += " or check if the query is not readonly and sql mode is strict"
			}
		}
		err = plannererrors.ErrInternal.GenWithStackByArgs(fmt.Sprintf("No access path for table '%s' is"+
			" found with '%v' = '%v', valid values can be '%s'%s.", tblName.String(),
			vardef.TiDBIsolationReadEngines, engineVals, availableEngineStr, helpMsg))
	}
	if _, ok := isolationReadEngines[kv.TiFlash]; !ok {
		if ctx.GetSessionVars().StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(
				"MPP mode may be blocked because the query is not readonly and sql mode is strict.")
		} else {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(
				fmt.Sprintf("MPP mode may be blocked because '%v'(value: '%v') not match, need 'tiflash'.",
					vardef.TiDBIsolationReadEngines, engineVals))
		}
	}
	return paths, err
}
