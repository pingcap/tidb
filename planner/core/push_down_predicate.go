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
	"sort"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// selectivity = (row count after filter) / (row count before filter), smaller is better
	selectivityThreshold = 0.7
	// income = (1 - selectivity) * restColumnCount, greater is better
	incomeImproveThreshold = 0.3
)

// expressionGroup is used to store
// 1. a group of expressions
// 2. the selectivity of the expressions
type expressionGroup struct {
	exprs       []expression.Expression
	selectivity float64
}

func predicatePushDownToTableScan(sctx sessionctx.Context, plan PhysicalPlan) {
	switch p := plan.(type) {
	case *PhysicalSelection:
		physicalTableScan, ok := plan.Children()[0].(*PhysicalTableScan)
		if ok && physicalTableScan.isFullScan() && physicalTableScan.StoreType == kv.TiFlash {
			// Only when the table scan is a full scan and the store type is TiFlash,
			// we will try to push down predicates.
			predicatePushDownToTableScanImpl(sctx, p, physicalTableScan)
		}
	case *PhysicalTableReader:
		predicatePushDownToTableScan(sctx, p.tablePlan)
	default:
		for _, child := range plan.Children() {
			predicatePushDownToTableScan(sctx, child)
		}
	}
}

/**
 * @brief: This function is used to remove the empty selection in plan
 * 		   when late materialization is enabled and all conditions are pushed down.
 * @param: plan: the physical plan
 */
func removeEmptySelection(plan PhysicalPlan) {
	if tableReader, isTableReader := plan.(*PhysicalTableReader); isTableReader && tableReader.StoreType == kv.TiFlash {
		if selection, isSelection := tableReader.tablePlan.(*PhysicalSelection); isSelection && len(selection.Conditions) == 0 {
			if len(tableReader.tablePlan.Children()) == 1 {
				tableReader.tablePlan = tableReader.tablePlan.Children()[0]
			} else {
				logutil.BgLogger().Warn("invalid table plan", zap.String("plan", plan.ExplainID().String()))
			}
		}
		removeEmptySelection(tableReader.tablePlan)
		tableReader.TablePlans = flattenPushDownPlan(tableReader.tablePlan)
	} else {
		var newChildren []PhysicalPlan
		removed := false
		for _, child := range plan.Children() {
			switch x := child.(type) {
			case *PhysicalSelection:
				if len(x.Conditions) == 0 {
					newChildren = append(newChildren, x.children...)
					removed = true
				} else {
					newChildren = append(newChildren, x)
				}
			default:
				newChildren = append(newChildren, x)
			}
		}
		if removed && len(newChildren) > 0 {
			plan.SetChildren(newChildren...)
		}
		for _, child := range plan.Children() {
			removeEmptySelection(child)
		}
	}
}

/**
 * @brief: This function is used to transform the columns to a string of "0" and "1".
 * @param: cols: the columns of a Expression
 * @param: totalColumnCount: the total number of columns in the tablescan
 * @example: the columns of tablescan are [a, b, c, d, e, f, g, h]
 * 		 	 the expression are ["a > 1", "c > 1", "e > 1 or g > 1"]
 *           so the columns of the expression are [a, c, e, g] and the totalColumnCount is 8
 *           the return value is "10101010"
 * @return: the string of "0" and "1"
 */
func transformColumnsToCode(cols []*expression.Column, totalColumnCount int) string {
	code := make([]byte, totalColumnCount)
	for _, col := range cols {
		code[col.Index] = '1'
	}
	return string(code)
}

/**
 *
 * @brief: This function is used to group the conditions by the column they use
 *         and sort the groups by the selectivity of the conditions in the group.
 * @param: conds: the conditions to be grouped
 * @return: the groups of conditions sorted by the selectivity of the conditions in the group
 * @example: conds = [a > 1, b > 1, a > 2, c > 1, a > 3, b > 2]
 *           return = [[a > 3, a > 2, a > 1], [b > 2, b > 1], [c > 1]]
 * @note: when the selectivity of one group is larger than the threshold,
 *  	  we will remove it from the returned result.
 */
func groupByColumnsSortBySelectivity(sctx sessionctx.Context, conds []expression.Expression, physicalTableScan *PhysicalTableScan) []expressionGroup {
	// Create a map to store the groupMap of conditions keyed by the columns
	groupMap := make(map[string][]expression.Expression)

	// Iterate through the conditions and group them by columns
	for _, cond := range conds {
		// If excuting light cost condition first,
		// the rows needed to be exucuted by the heavy cost condition will be reduced.
		// So if the cond contains heavy cost functions, skip it.
		if withHeavyCostFunction(cond) {
			continue
		}

		columns := expression.ExtractColumns(cond)

		var code string
		if len(columns) == 0 {
			code = "0"
		} else {
			code = transformColumnsToCode(columns, len(physicalTableScan.Columns))
		}
		groupMap[code] = append(groupMap[code], cond)
	}

	// Estimate the selectivity of each group and check if it is larger than the selectivityThreshold
	var exprGroups []expressionGroup
	for _, group := range groupMap {
		selectivity, _, err := physicalTableScan.tblColHists.Selectivity(sctx, group, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, do not push down the conditions group", zap.Error(err))
			continue
		}
		if selectivity <= selectivityThreshold {
			exprGroups = append(exprGroups, expressionGroup{exprs: group, selectivity: selectivity})
		}
	}

	// Sort exprGroups by selectivity in ascending order
	sort.SliceStable(exprGroups, func(i, j int) bool {
		return exprGroups[i].selectivity < exprGroups[j].selectivity || (exprGroups[i].selectivity == exprGroups[j].selectivity && len(exprGroups[i].exprs) < len(exprGroups[j].exprs))
	})

	return exprGroups
}

/**
 * @brief: This function is used to check if the condition contain heavy cost functions.
 * @param: cond: condition of PhysicalSelection to be checked
 * @note: heavy cost functions are functions that may cause a lot of memory allocation or disk IO.
 * 		  For example, JSON functions.(To be supplemented)
 */
func withHeavyCostFunction(cond expression.Expression) bool {
	if binop, ok := cond.(*expression.ScalarFunction); ok {
		switch binop.FuncName.L {
		// JSON functions
		case ast.JSONType, ast.JSONExtract, ast.JSONUnquote, ast.JSONArray, ast.JSONObject, ast.JSONMerge, ast.JSONSet, ast.JSONInsert, ast.JSONReplace, ast.JSONRemove, ast.JSONOverlaps, ast.JSONContains:
			return true
		case ast.JSONMemberOf, ast.JSONContainsPath, ast.JSONValid, ast.JSONArrayAppend, ast.JSONArrayInsert, ast.JSONMergePatch, ast.JSONMergePreserve, ast.JSONPretty, ast.JSONQuote, ast.JSONSearch:
			return true
		case ast.JSONStorageFree, ast.JSONStorageSize, ast.JSONDepth, ast.JSONKeys, ast.JSONLength:
			return true
		// time functions
		case ast.AddDate, ast.AddTime, ast.ConvertTz, ast.Curdate, ast.CurrentDate, ast.CurrentTime, ast.CurrentTimestamp, ast.Curtime, ast.Date, ast.DateLiteral, ast.DateAdd, ast.DateFormat:
			return true
		case ast.DateSub, ast.DateDiff, ast.Day, ast.DayName, ast.DayOfMonth, ast.DayOfWeek, ast.DayOfYear, ast.Extract, ast.FromDays, ast.FromUnixTime, ast.GetFormat, ast.Hour, ast.LocalTime:
			return true
		case ast.LocalTimestamp, ast.MakeDate, ast.MakeTime, ast.MicroSecond, ast.Minute, ast.Month, ast.MonthName, ast.Now, ast.PeriodAdd, ast.PeriodDiff, ast.Quarter, ast.SecToTime, ast.Second:
			return true
		case ast.StrToDate, ast.SubDate, ast.SubTime, ast.Sysdate, ast.Time, ast.TimeLiteral, ast.TimeFormat, ast.TimeToSec, ast.TimeDiff, ast.Timestamp, ast.TimestampLiteral, ast.TimestampAdd:
			return true
		case ast.TimestampDiff, ast.ToDays, ast.ToSeconds, ast.UnixTimestamp, ast.UTCDate, ast.UTCTime, ast.UTCTimestamp, ast.Week, ast.Weekday, ast.WeekOfYear, ast.Year, ast.YearWeek, ast.LastDay:
			return true
			// TODO: add more heavy cost functions
		}
	}
	return false
}

/**
 * @brief: This function is used to remove the conditions that needed to be pushed down.
 * @param: physicalSelection: the PhysicalSelection to be modified
 * @param: exprs: the conditions to be removed
 */
func removeSpecificExprsFromSelection(physicalSelection *PhysicalSelection, exprs []expression.Expression) {
	newConds := make([]expression.Expression, 0, len(physicalSelection.Conditions))
	for _, cond := range physicalSelection.Conditions {
		if !expression.Contains(exprs, cond) {
			newConds = append(newConds, cond)
		}
	}
	physicalSelection.Conditions = newConds
}

/**
 * @brief: This function is used to push down the some filter conditions of the selection to the tablescan.
 * @param: sctx: the session context
 * @param: physicalSelection: the PhysicalSelection containing the conditions to be pushed down
 * @param: physicalTableScan: the PhysicalTableScan to be pushed down to
 */
func predicatePushDownToTableScanImpl(sctx sessionctx.Context, physicalSelection *PhysicalSelection, physicalTableScan *PhysicalTableScan) {
	conds := physicalSelection.Conditions
	if len(conds) == 0 {
		return
	}

	// group the conditions by columns and sort them by selectivity
	sortedConds := groupByColumnsSortBySelectivity(sctx, conds, physicalTableScan)

	selectedConds := make([]expression.Expression, 0, len(conds))
	selectedIncome := 0.0
	selectedColumnCount := 0
	totalColumnCount := len(physicalTableScan.Columns)

	for _, exprGroup := range sortedConds {
		mergedConds := append(selectedConds, exprGroup.exprs...)
		selectivity, _, err := physicalTableScan.tblColHists.Selectivity(sctx, mergedConds, nil)
		if err != nil {
			logutil.BgLogger().Info("calculate selectivity failed, do not push down the conditions group", zap.Error(err))
			continue
		}
		var cols []*expression.Column
		cols = expression.ExtractColumnsFromExpressions(cols, mergedConds, nil)
		// Remove the duplicated columns
		colSet := make(map[int64]struct{}, len(cols))
		for _, col := range cols {
			colSet[col.UniqueID] = struct{}{}
		}
		colCnt := len(colSet)
		income := (1 - selectivity) * (float64(totalColumnCount) - float64(colCnt))
		// If selectedColumnCount does not change,
		// or the income increase larger than the threshold after pushing down the group, push down it.
		if colCnt == selectedColumnCount || income-selectedIncome >= incomeImproveThreshold {
			selectedConds = mergedConds
			selectedColumnCount = colCnt
			selectedIncome = income
		}
	}

	if len(selectedConds) == 0 {
		return
	}
	logutil.BgLogger().Info("planner: push down conditions to table scan", zap.String("table", physicalTableScan.Table.Name.L), zap.String("conditions", expression.ExplainExpressionList(selectedConds, physicalSelection.Schema())))
	// remove the pushed down conditions from selection
	removeSpecificExprsFromSelection(physicalSelection, selectedConds)
	// add the pushed down conditions to table scan
	physicalTableScan.prewhereFilterCondition = append(physicalTableScan.prewhereFilterCondition, selectedConds...)
}
