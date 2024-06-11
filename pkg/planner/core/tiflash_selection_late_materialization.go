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

package core

import (
	"cmp"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// selectivity = (row count after filter) / (row count before filter), smaller is better
// income = (1 - selectivity) * restColumnCount * tableRowCount, greater is better

const (
	selectivityThreshold = 0.7
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

// predicatePushDownToTableScan is used find the selection just above the table scan
// and try to push down the predicates to the table scan.
// Used for TiFlash late materialization.
func predicatePushDownToTableScan(sctx base.PlanContext, plan base.PhysicalPlan) base.PhysicalPlan {
	switch p := plan.(type) {
	case *PhysicalSelection:
		if physicalTableScan, ok := plan.Children()[0].(*PhysicalTableScan); ok && physicalTableScan.StoreType == kv.TiFlash {
			// Only when the the store type is TiFlash, we will try to push down predicates.
			predicatePushDownToTableScanImpl(sctx, p, physicalTableScan)
			if len(p.Conditions) == 0 {
				return p.Children()[0]
			}
		} else if !ok {
			newChildren := make([]base.PhysicalPlan, 0, len(plan.Children()))
			for _, child := range plan.Children() {
				newChildren = append(newChildren, predicatePushDownToTableScan(sctx, child))
			}
			plan.SetChildren(newChildren...)
		}
	case *PhysicalTableReader:
		p.tablePlan = predicatePushDownToTableScan(sctx, p.tablePlan)
	default:
		if len(plan.Children()) > 0 {
			newChildren := make([]base.PhysicalPlan, 0, len(plan.Children()))
			for _, child := range plan.Children() {
				newChildren = append(newChildren, predicatePushDownToTableScan(sctx, child))
			}
			plan.SetChildren(newChildren...)
		}
	}
	return plan
}

// transformColumnsToCode is used to transform the columns to a string of "0" and "1".
// @param: cols: the columns of a Expression
// @param: totalColumnCount: the total number of columns in the tablescan
// @example:
//
//			  the columns of tablescan are [a, b, c, d, e, f, g, h]
//			  the expression are ["a > 1", "c > 1", "e > 1 or g > 1"]
//	          so the columns of the expression are [a, c, e, g] and the totalColumnCount is 8
//	          the return value is "10101010"
//
// @return: the string of "0" and "1"
func transformColumnsToCode(cols []*expression.Column, totalColumnCount int) string {
	code := make([]byte, totalColumnCount)
	for _, col := range cols {
		code[col.Index] = '1'
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
func groupByColumnsSortBySelectivity(sctx base.PlanContext, conds []expression.Expression, physicalTableScan *PhysicalTableScan) []expressionGroup {
	// Create a map to store the groupMap of conditions keyed by the columns
	groupMap := make(map[string][]expression.Expression)

	// Iterate through the conditions and group them by columns
	for _, cond := range conds {
		// If excuting light cost condition first,
		// the rows needed to be exucuted by the heavy cost condition will be reduced.
		// So if the cond contains heavy cost functions, skip it.
		if withHeavyCostFunctionForTiFlashPrefetch(cond) {
			continue
		}

		columns := expression.ExtractColumns(cond)

		var code string
		if len(columns) == 0 {
			code = "0"
		} else if len(columns) <= columnCountThreshold {
			code = transformColumnsToCode(columns, len(physicalTableScan.Columns))
		} else {
			// If the number of columns is larger than columnCountThreshold,
			// the possibility of the selectivity of the condition is larger than the threshold is very small.
			// So we skip it to reduce the cost of calculating the selectivity.
			continue
		}
		groupMap[code] = append(groupMap[code], cond)
	}

	// Estimate the selectivity of each group and check if it is larger than the selectivityThreshold
	var exprGroups []expressionGroup
	for _, group := range groupMap {
		selectivity, _, err := cardinality.Selectivity(sctx, physicalTableScan.tblColHists, group, nil)
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
// @param: cond: condition of PhysicalSelection to be checked
// @note: heavy cost functions are functions that may cause a lot of memory allocation or disk IO.
func withHeavyCostFunctionForTiFlashPrefetch(cond expression.Expression) bool {
	if binop, ok := cond.(*expression.ScalarFunction); ok {
		switch binop.FuncName.L {
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

// removeSpecificExprsFromSelection is used to remove the conditions that needed to be pushed down.
// @param: physicalSelection: the PhysicalSelection to be modified
// @param: exprs: the conditions to be removed
func removeSpecificExprsFromSelection(physicalSelection *PhysicalSelection, exprs []expression.Expression) {
	conditions := physicalSelection.Conditions
	for i := len(conditions) - 1; i >= 0; i-- {
		if expression.Contains(exprs, conditions[i]) {
			conditions = append(conditions[:i], conditions[i+1:]...)
		}
	}
	physicalSelection.Conditions = conditions
}

// predicatePushDownToTableScanImpl is used to push down the some filter conditions of the selection to the tablescan.
// @param: sctx: the session context
// @param: physicalSelection: the PhysicalSelection containing the conditions to be pushed down
// @param: physicalTableScan: the PhysicalTableScan to be pushed down to
func predicatePushDownToTableScanImpl(sctx base.PlanContext, physicalSelection *PhysicalSelection, physicalTableScan *PhysicalTableScan) {
	// When the table is small, there is no need to push down the conditions.
	if physicalTableScan.tblColHists.RealtimeCount <= tiflashDataPackSize || physicalTableScan.KeepOrder {
		return
	}
	conds := physicalSelection.Conditions
	if len(conds) == 0 {
		return
	}

	// group the conditions by columns and sort them by selectivity
	sortedConds := groupByColumnsSortBySelectivity(sctx, conds, physicalTableScan)

	selectedConds := make([]expression.Expression, 0, len(conds))
	selectedIncome := 0.0
	selectedColumnCount := 0
	selectedSelectivity := 1.0
	totalColumnCount := len(physicalTableScan.Columns)
	tableRowCount := physicalTableScan.StatsInfo().RowCount

	for _, exprGroup := range sortedConds {
		mergedConds := append(selectedConds, exprGroup.exprs...)
		selectivity, _, err := cardinality.Selectivity(sctx, physicalTableScan.tblColHists, mergedConds, nil)
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
	logutil.BgLogger().Debug("planner: push down conditions to table scan", zap.String("table", physicalTableScan.Table.Name.L), zap.String("conditions", string(expression.SortedExplainExpressionList(sctx.GetExprCtx().GetEvalCtx(), selectedConds))))
	PushedDown(physicalSelection, physicalTableScan, selectedConds, selectedSelectivity)
}

// PushedDown is used to push down the selected conditions from PhysicalSelection to PhysicalTableScan.
// Used in unit test, so it is exported.
func PushedDown(sel *PhysicalSelection, ts *PhysicalTableScan, selectedConds []expression.Expression, selectedSelectivity float64) {
	// remove the pushed down conditions from selection
	removeSpecificExprsFromSelection(sel, selectedConds)
	// add the pushed down conditions to table scan
	ts.LateMaterializationFilterCondition = selectedConds
	// Update the row count of table scan after pushing down the conditions.
	ts.StatsInfo().RowCount *= selectedSelectivity
}
