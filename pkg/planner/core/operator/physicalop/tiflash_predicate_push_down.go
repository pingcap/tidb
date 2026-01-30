// Copyright 2023 PingCAP, Inc.
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

package physicalop

import (
	"cmp"
	"math"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// selectivity = (row count after filter) / (row count before filter), smaller is better
// income = (1 - selectivity) * restColumnCount * tableRowCount, greater is better

const (
	selectivityThreshold = 0.6
	// The default now of number of rows in a pack of TiFlash
	tiflashDataPackSize  = 8192
	columnCountThreshold = 3
)

// expressionGroup is used to store
// 1. a group of expressions
// 2. the selectivity of the expressions
type expressionGroup struct {
	exprs       []expression.Expression
	selectivity float64
}

// transformColumnsToCode is used to transform the columns to a string of "0" and "1".
// @param: cols: the columns of a Expression
// @param: tableColumns: the total number of columns in the tablescan
// @example:
//
//			  the columns of tablescan are [a, b, c, d, e, f, g, h]
//			  the expression are ["a > 1", "c > 1", "e > 1 or g > 1"]
//	          so the columns of the expression are [a, c, e, g] and the tableColumns is 8
//	          the return value is "10101010"
//
// @return: the string of "0" and "1"
func transformColumnsToCode(cols []*expression.Column, tableColumns int) string {
	if len(cols) == 0 {
		return "0"
	}

	code := make([]byte, tableColumns)
	for _, col := range cols {
		if col.ID < int64(tableColumns) && col.ID >= 0 {
			code[col.ID] = '1'
		}
	}
	return string(code)
}

// groupByColumnsSortBySelectivity is used to group the conditions by the column they use
// and sort the groups by the selectivity of the conditions in the group.
// @param: conds: the conditions to be grouped
// @return: the groups of conditions sorted by the selectivity of the conditions in the group
// @example: conds = [a > 1, b > 1, a > 2, c > 1, a > 3, b > 2], return = [[a > 3, a > 2, a > 1], [b > 2, b > 1], [c > 1]]
// @note: when the selectivity of one group is larger than the threshold, we will remove it from the returned result.
// @note: when the number of columns of one group is larger than the threshold, we will remove it from the returned result.
func groupByColumnsSortBySelectivity(sctx base.PlanContext, conds []expression.Expression, ts *PhysicalTableScan) []expressionGroup {
	// Create a map to store the groupMap of conditions keyed by the columns
	groupMap := make(map[string][]expression.Expression)

	// Iterate through the conditions and group them by columns
	for _, cond := range conds {
		// If excuting light cost condition first,
		// the rows needed to be exucuted by the heavy cost condition will be reduced.
		// So if the cond contains heavy cost functions, skip it to reduce the cost of calculating the selectivity.
		if withHeavyCostFunctionForTiFlashPrefetch(cond) {
			continue
		}

		// If the number of columns is larger than columnCountThreshold,
		// the possibility of the selectivity of the condition is larger than the threshold is very small.
		// Skip.
		columns := expression.ExtractColumns(cond)
		if len(columns) <= columnCountThreshold {
			code := transformColumnsToCode(columns, len(ts.Table.Columns))
			groupMap[code] = append(groupMap[code], cond)
		}
	}

	// Estimate the selectivity of each group and check if it is larger than the selectivityThreshold
	var exprGroups []expressionGroup
	for _, group := range groupMap {
		selectivity, err := cardinality.Selectivity(sctx, ts.TblColHists, group, nil)
		if err != nil {
			logutil.BgLogger().Warn("calculate selectivity failed, do not push down the conditions group", zap.Error(err))
			continue
		}
		if selectivity <= selectivityThreshold {
			exprGroups = append(exprGroups, expressionGroup{exprs: group, selectivity: selectivity})
		}
	}

	// Sort exprGroups by selectivity in ascending order
	slices.SortStableFunc(exprGroups, func(x, y expressionGroup) int {
		if x.selectivity == y.selectivity && len(x.exprs) < len(y.exprs) {
			return -1
		}
		return cmp.Compare(x.selectivity, y.selectivity)
	})

	return exprGroups
}

// withHeavyCostFunctionForTiFlashPrefetch is used to check if the condition contain heavy cost functions.
// @param: cond: filter condition
// @note: heavy cost functions are functions that may cause a lot of memory allocation or disk IO.
func withHeavyCostFunctionForTiFlashPrefetch(cond expression.Expression) bool {
	if sf, ok := cond.(*expression.ScalarFunction); ok {
		switch sf.FuncName.L {
		case ast.LogicAnd, ast.LogicOr:
			return withHeavyCostFunctionForTiFlashPrefetch(sf.GetArgs()[0]) && withHeavyCostFunctionForTiFlashPrefetch(sf.GetArgs()[1])
		case ast.UnaryNot:
			return withHeavyCostFunctionForTiFlashPrefetch(sf.GetArgs()[0])
		// JSON functions
		case ast.JSONArray,
			ast.JSONArrayAppend,
			ast.JSONArrayInsert,
			ast.JSONContains,
			ast.JSONContainsPath,
			ast.JSONDepth,
			ast.JSONExtract,
			ast.JSONInsert,
			ast.JSONKeys,
			ast.JSONLength,
			ast.JSONMemberOf,
			ast.JSONMerge,
			ast.JSONMergePatch,
			ast.JSONMergePreserve,
			ast.JSONObject,
			ast.JSONOverlaps,
			ast.JSONPretty,
			ast.JSONQuote,
			ast.JSONRemove,
			ast.JSONReplace,
			ast.JSONSchemaValid,
			ast.JSONSearch,
			ast.JSONSet,
			ast.JSONStorageFree,
			ast.JSONStorageSize,
			ast.JSONType,
			ast.JSONUnquote,
			ast.JSONValid:
			return true
		// some time functions
		case ast.AddDate, ast.AddTime, ast.ConvertTz, ast.DateLiteral, ast.DateAdd, ast.DateFormat, ast.FromUnixTime, ast.GetFormat, ast.UTCTimestamp:
			return true
		case ast.DateSub, ast.DateDiff, ast.DayOfYear, ast.Extract, ast.FromDays, ast.TimestampLiteral, ast.TimestampAdd, ast.UnixTimestamp:
			return true
		case ast.LocalTimestamp, ast.MakeDate, ast.MakeTime, ast.MonthName, ast.PeriodAdd, ast.PeriodDiff, ast.Quarter, ast.SecToTime, ast.ToSeconds:
			return true
		case ast.StrToDate, ast.SubDate, ast.SubTime, ast.TimeLiteral, ast.TimeFormat, ast.TimeToSec, ast.TimeDiff, ast.TimestampDiff:
			return true
		// regexp functions
		case ast.Regexp, ast.RegexpLike, ast.RegexpReplace, ast.RegexpSubstr, ast.RegexpInStr:
			return true
			// TODO: add more heavy cost functions
		}
	}
	return false
}

// predicatePushDownToTableScan is used to push down the some filter conditions to the tablescan.
// @param: sctx: the session context
// @param: conds: the filter conditions
// @param: ts: the PhysicalTableScan to be pushed down to
func predicatePushDownToTableScan(sctx base.PlanContext, conds []expression.Expression, ts *PhysicalTableScan) {
	// group the conditions by columns and sort them by selectivity
	sortedConds := groupByColumnsSortBySelectivity(sctx, conds, ts)

	selectedConds := make([]expression.Expression, 0, len(conds))
	selectedIncome := 0.0
	selectedColumnCount := 0
	selectedSelectivity := 1.0
	totalColumnCount := len(ts.Columns)
	tableRowCount := ts.StatsInfo().RowCount

	for _, exprGroup := range sortedConds {
		mergedConds := append(selectedConds, exprGroup.exprs...)
		selectivity, err := cardinality.Selectivity(sctx, ts.TblColHists, mergedConds, nil)
		if err != nil {
			logutil.BgLogger().Warn("calculate selectivity failed, do not push down the conditions group", zap.Error(err))
			continue
		}
		colCnt := expression.ExtractColumnSet(mergedConds...).Len()
		income := (1 - selectivity) * tableRowCount
		// If selectedColumnCount does not change,
		// or the increase of the number of filtered rows is greater than tiflashDataPackSize and the income increases, push down the conditions.
		if colCnt == selectedColumnCount || (income > tiflashDataPackSize && income*(float64(totalColumnCount)-float64(colCnt)) > selectedIncome) {
			selectedConds = mergedConds
			selectedColumnCount = colCnt
			selectedIncome = income * (float64(totalColumnCount) - float64(colCnt))
			selectedSelectivity = selectivity
		} else if income < tiflashDataPackSize {
			// If the increase of the number of filtered rows is less than tiflashDataPackSize,
			// break the loop to reduce the cost of calculating selectivity.
			break
		}
	}

	if len(selectedConds) == 0 {
		return
	}
	// add the pushed down conditions to table scan
	ts.LateMaterializationFilterCondition = selectedConds
	ts.LateMaterializationSelectivity = selectedSelectivity
}

// isPredicateSimpleCompare is used to check if the condition is a simple comparison predicate
// which only contains >, >=, =, !=, <=, <, in.
func isPredicateSimpleCompare(cond expression.Expression) bool {
	if sf, ok := cond.(*expression.ScalarFunction); ok {
		switch sf.FuncName.L {
		case ast.EQ, ast.GE, ast.LE, ast.LT, ast.GT, ast.In:
			return true
		case ast.LogicAnd, ast.LogicOr:
			return isPredicateSimpleCompare(sf.GetArgs()[0]) && isPredicateSimpleCompare(sf.GetArgs()[1])
		case ast.UnaryNot:
			return isPredicateSimpleCompare(sf.GetArgs()[0])
		}
	}
	return false
}

// handleTiFlashPredicatePushDown is used to handle the TiFlash predicate push down.
// 1. Whether to use the inverted index.
// 2. Whether to push down the conditions to the table scan.
func handleTiFlashPredicatePushDown(pctx base.PlanContext, ts *PhysicalTableScan, indexHints []*ast.IndexHint) {
	// When the table is small, there is no need to push down the conditions.
	if ts.TblColHists.RealtimeCount <= tiflashDataPackSize || ts.KeepOrder || len(ts.FilterCondition) == 0 {
		return
	}

	// Currently, TiFlash does not support vector index with predicates.
	// So for now, one DataSource only contains one TiFlash !keepOrder path,
	// which means that the following code will only be executed once.
	// TODO: If there are multiple TiFlash !keepOrder paths in one DataSource in the future,
	// we need to move the following code to another place.
	// Since caculating selectivity is expensive, we need to avoid duplicate execution.
	for _, index := range ts.UsedColumnarIndexes {
		if index.IndexInfo.VectorInfo != nil {
			panic("TiFlash does not support vector index with pedicates")
		}
	}

	// Consider use index hints
	indexMap := make(map[string]int, len(ts.Table.Indices))
	for _, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		switch hint.HintType {
		case ast.HintUse:
			for _, name := range hint.IndexNames {
				if indexMap[name.L] != math.MaxInt {
					indexMap[name.L]++
				}
			}
		case ast.HintIgnore:
			for _, name := range hint.IndexNames {
				if indexMap[name.L] != math.MaxInt {
					indexMap[name.L]--
				}
			}
		case ast.HintForce:
			for _, name := range hint.IndexNames {
				indexMap[name.L] = math.MaxInt
			}
		default:
			continue
		}
	}

	indexedColumnNameToIndexInfoMap := make(map[string]*model.IndexInfo, len(ts.Table.Indices))
	for _, index := range ts.Table.Indices {
		if index.State == model.StatePublic && index.InvertedInfo != nil {
			if len(indexHints) > 0 && indexMap[index.Name.L] <= 0 {
				continue
			}
			// inverted index only support one column
			indexedColumnNameToIndexInfoMap[index.Columns[0].Name.L] = index
		}
	}

	selectedColumns := make(map[string]bool, len(indexedColumnNameToIndexInfoMap))
	selectedConditions := make([]expression.Expression, 0, len(ts.FilterCondition))
	columnNames := make([]string, 0, 4)
	for _, cond := range ts.FilterCondition {
		// 1. The predicate only contains >, >=, =, !=, <=, <, in.
		if !isPredicateSimpleCompare(cond) {
			continue
		}
		// 2. The columns of predicate all have inverted index.
		allHaveInvertedIndex := true
		columns := expression.ExtractColumns(cond)
		columnNames = slices.Grow(columnNames, len(columns))
		for _, col := range columns {
			parts := strings.Split(col.OrigName, ".")
			columnNames = append(columnNames, strings.ToLower(parts[len(parts)-1]))
		}
		for _, colName := range columnNames {
			if _, ok := indexedColumnNameToIndexInfoMap[colName]; !ok {
				allHaveInvertedIndex = false
				break
			}
		}
		if !allHaveInvertedIndex {
			continue
		}
		// 3. The selectivity of the predicate is less than 60%.
		selectivity, err := cardinality.Selectivity(pctx, ts.TblColHists, []expression.Expression{cond}, nil)
		if err != nil {
			logutil.BgLogger().Warn("calculate selectivity failed", zap.Error(err))
			continue
		}
		if selectivity > selectivityThreshold {
			continue
		}
		// all passed, add the columns to selected
		for _, colName := range columnNames {
			selectedColumns[colName] = true
		}
		selectedConditions = append(selectedConditions, cond)
		columnNames = columnNames[:0] // reset columnNames slice to avoid unnecessary memory allocation
	}

	for colName := range selectedColumns {
		if index, ok := indexedColumnNameToIndexInfoMap[colName]; ok {
			ts.UsedColumnarIndexes = append(ts.UsedColumnarIndexes, buildInvertedIndexExtra(index))
		}
	}

	for colName, index := range indexedColumnNameToIndexInfoMap {
		if _, ok := selectedColumns[colName]; !ok && indexMap[index.Name.L] == math.MaxInt {
			// if the index is not used, but the index hint is force use, add it to the used indexes
			ts.UsedColumnarIndexes = append(ts.UsedColumnarIndexes, buildInvertedIndexExtra(index))
		}
	}

	// if EnableLateMaterialization is set, try to push down some predicates to table scan
	if len(ts.FilterCondition) > len(selectedConditions) && pctx.GetSessionVars().EnableLateMaterialization {
		remaining := make([]expression.Expression, 0, len(ts.FilterCondition)-len(selectedConditions))
		for _, cond := range ts.FilterCondition {
			if !expression.Contains(pctx.GetExprCtx().GetEvalCtx(), selectedConditions, cond) {
				remaining = append(remaining, cond)
			}
		}
		predicatePushDownToTableScan(pctx, remaining, ts)
	}

	// Update the row count of table scan.
	if len(selectedConditions)+len(ts.LateMaterializationFilterCondition) != 0 {
		selectedConditions = append(selectedConditions, ts.LateMaterializationFilterCondition...)
		selectivity, err := cardinality.Selectivity(ts.SCtx(), ts.TblColHists, selectedConditions, nil)
		if err != nil {
			logutil.BgLogger().Warn("calculate selectivity failed", zap.Error(err))
			selectivity = selectivityThreshold
		}
		ts.SetStats(ts.StatsInfo().Scale(ts.SCtx().GetSessionVars(), selectivity))
		// just to make explain result stable
		slices.SortFunc(ts.UsedColumnarIndexes, func(lhs, rhs *ColumnarIndexExtra) int {
			return cmp.Compare(lhs.IndexInfo.ID, rhs.IndexInfo.ID)
		})
	}
}
