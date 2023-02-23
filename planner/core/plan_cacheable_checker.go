// Copyright 2017 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Cacheable checks whether the input ast(query) is cacheable with empty session context, which is mainly for testing.
func Cacheable(node ast.Node, is infoschema.InfoSchema) bool {
	c, _ := CacheableWithCtx(nil, node, is)
	return c
}

// CacheableWithCtx checks whether the input ast(query) is cacheable.
// Handle "ignore_plan_cache()" hint
// If there are multiple hints, only one will take effect
func CacheableWithCtx(sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (bool, string) {
	_, isSelect := node.(*ast.SelectStmt)
	_, isUpdate := node.(*ast.UpdateStmt)
	_, isInsert := node.(*ast.InsertStmt)
	_, isDelete := node.(*ast.DeleteStmt)
	_, isSetOpr := node.(*ast.SetOprStmt)
	if !(isSelect || isUpdate || isInsert || isDelete || isSetOpr) {
		return false, "not a SELECT/UPDATE/INSERT/DELETE/SET statement"
	}
	checker := cacheableChecker{
		sctx:      sctx,
		cacheable: true,
		schema:    is,
	}
	node.Accept(&checker)
	return checker.cacheable, checker.reason
}

// cacheableChecker checks whether a query can be cached:
type cacheableChecker struct {
	sctx      sessionctx.Context
	cacheable bool
	schema    infoschema.InfoSchema
	reason    string // reason why cannot use plan-cache
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.DeleteStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.UpdateStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.InsertStmt:
		if node.Select == nil {
			// do not cache insert-values-stmt like 'insert into t values (...)' since
			// no performance benefit and to save memory.
			checker.cacheable = false
			checker.reason = "ignore insert-values-stmt"
			return in, true
		}
		for _, hints := range node.TableHints {
			if hints.HintName.L == HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}

	case *ast.VariableExpr:
		checker.cacheable = false
		checker.reason = "query has user-defined variables is un-cacheable"
		return in, true
	case *ast.ExistsSubqueryExpr, *ast.SubqueryExpr:
		if !checker.sctx.GetSessionVars().EnablePlanCacheForSubquery {
			checker.cacheable = false
			checker.reason = "query has sub-queries is un-cacheable"
			return in, true
		}
		return in, false
	case *ast.FuncCallExpr:
		if _, found := expression.UnCacheableFunctions[node.FnName.L]; found {
			checker.cacheable = false
			checker.reason = fmt.Sprintf("query has '%v' is un-cacheable", node.FnName.L)
			return in, true
		}
	case *ast.OrderByClause:
		for _, item := range node.Items {
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				checker.reason = "query has 'order by ?' is un-cacheable"
				return in, true
			}
		}
	case *ast.GroupByClause:
		for _, item := range node.Items {
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				checker.reason = "query has 'group by ?' is un-cacheable"
				return in, true
			}
		}
	case *ast.Limit:
		if node.Count != nil {
			if _, isParamMarker := node.Count.(*driver.ParamMarkerExpr); isParamMarker && !checker.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
				checker.cacheable = false
				checker.reason = "query has 'limit ?' is un-cacheable"
				return in, true
			}
		}
		if node.Offset != nil {
			if _, isParamMarker := node.Offset.(*driver.ParamMarkerExpr); isParamMarker && !checker.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
				checker.cacheable = false
				checker.reason = "query has 'limit ?, 10' is un-cacheable"
				return in, true
			}
		}
	case *ast.FrameBound:
		if _, ok := node.Expr.(*driver.ParamMarkerExpr); ok {
			checker.cacheable = false
			checker.reason = "query has ? in window function frames is un-cacheable"
			return in, true
		}
	case *ast.TableName:
		if checker.schema != nil {
			if isPartitionTable(checker.schema, node) {
				// Temporary disable prepared plan cache until https://github.com/pingcap/tidb/issues/33031
				// is fixed and additional tests with dynamic partition prune mode has been added.
				/*
					if checker.sctx != nil && checker.sctx.GetSessionVars().UseDynamicPartitionPrune() {
						return in, false // dynamic-mode for partition tables can use plan-cache
					}
				*/
				checker.cacheable = false
				checker.reason = "query accesses partitioned tables is un-cacheable"
				return in, true
			}
			if hasGeneratedCol(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "query accesses generated columns is un-cacheable"
				return in, true
			}
			if isTempTable(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "query accesses temporary tables is un-cacheable"
				return in, true
			}
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *cacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}

// NonPreparedPlanCacheable checks whether the input ast is cacheable for non-prepared plan cache with empty session context, which is mainly for testing.
func NonPreparedPlanCacheable(node ast.Node, is infoschema.InfoSchema) bool {
	ok, _ := NonPreparedPlanCacheableWithCtx(nil, node, is)
	return ok
}

// NonPreparedPlanCacheableWithCtx checks whether the input ast is cacheable for non-prepared plan cache.
// Only support: select {field} from {single-table} where {cond} and {cond} ...
// {cond}: {col} {op} {val}
// {op}: >, <, =
func NonPreparedPlanCacheableWithCtx(sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (ok bool, reason string) {
	selectStmt, isSelect := node.(*ast.SelectStmt)
	if !isSelect { // only support select statement now
		return false, "not a select statement"
	}
	if selectStmt.Kind != ast.SelectStmtKindSelect {
		return false, "not a select statement"
	}
	if len(selectStmt.TableHints) > 0 || // hints
		selectStmt.Distinct || selectStmt.GroupBy != nil || selectStmt.Having != nil || // agg
		selectStmt.WindowSpecs != nil || // window function
		selectStmt.OrderBy != nil || // order
		selectStmt.Limit != nil || // limit
		selectStmt.LockInfo != nil || selectStmt.SelectIntoOpt != nil { // lock info
		return false, "queries that have hints, aggregation, window-function, order-by, limit and lock are not supported"
	}
	from := selectStmt.From
	if from == nil || selectStmt.From.TableRefs == nil {
		return false, "queries that have sub-queries are not supported"
	}
	tableRefs := from.TableRefs
	if tableRefs.Right != nil {
		// We don't support the join for the non-prepared plan cache now.
		return false, "queries that access multiple tables are not supported"
	}
	switch x := tableRefs.Left.(type) {
	case *ast.TableSource:
		_, isTableName := x.Source.(*ast.TableName)
		if !isTableName {
			return false, "queries that have sub-queries are not supported"
		}
	}

	checker := nonPreparedPlanCacheableChecker{
		sctx:      sctx,
		cacheable: true,
		schema:    is,
	}
	node.Accept(&checker)
	return checker.cacheable, checker.reason
}

// nonPreparedPlanCacheableChecker checks whether a query's plan can be cached for non-prepared plan cache.
// NOTE: we can add more rules in the future.
type nonPreparedPlanCacheableChecker struct {
	sctx      sessionctx.Context
	cacheable bool
	reason    string // reason why this statement cannot hit the cache
	schema    infoschema.InfoSchema
}

// Enter implements Visitor interface.
func (checker *nonPreparedPlanCacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt, *ast.FieldList, *ast.SelectField, *ast.TableRefsClause, *ast.Join, *ast.BetweenExpr,
		*ast.TableSource, *ast.ColumnNameExpr, *ast.ColumnName, *driver.ValueExpr, *ast.PatternInExpr:
		return in, !checker.cacheable // skip child if un-cacheable
	case *ast.BinaryOperationExpr:
		if _, found := expression.NonPreparedPlanCacheableOp[node.Op.String()]; !found {
			checker.cacheable = false
			checker.reason = "query has some unsupported binary operation"
		}
		return in, !checker.cacheable
	case *ast.TableName:
		if checker.schema != nil {
			if isPartitionTable(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "queries that access partitioning table are not supported"
			}
			if hasGeneratedCol(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "queries that have generated columns are not supported"
			}
			if isTempTable(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "queries that access temporary tables are not supported"
			}
			if isView(checker.schema, node) {
				checker.cacheable = false
				checker.reason = "queries that access views are not supported"
			}
		}
		return in, !checker.cacheable
	}
	checker.cacheable = false // unexpected cases
	checker.reason = "query has some unsupported Node"
	return in, !checker.cacheable
}

// Leave implements Visitor interface.
func (checker *nonPreparedPlanCacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}

func hasGeneratedCol(schema infoschema.InfoSchema, tn *ast.TableName) bool {
	tb, err := schema.TableByName(tn.Schema, tn.Name)
	if err != nil {
		logutil.BgLogger().Error("Error occur in checking cacheable", zap.Error(err))
		return false
	}
	for _, col := range tb.Cols() {
		if col.IsGenerated() {
			return true
		}
	}
	return false
}

func isView(schema infoschema.InfoSchema, tn *ast.TableName) bool {
	return schema.TableIsView(tn.Schema, tn.Name)
}

func isTempTable(schema infoschema.InfoSchema, tn *ast.TableName) bool {
	tb, err := schema.TableByName(tn.Schema, tn.Name)
	if err != nil {
		logutil.BgLogger().Error("Error occur in checking cacheable", zap.Error(err))
		return false
	}
	if tb.Meta().TempTableType != model.TempTableNone {
		return true
	}
	return false
}

func isPartitionTable(schema infoschema.InfoSchema, tn *ast.TableName) bool {
	tb, err := schema.TableByName(tn.Schema, tn.Name)
	if err != nil {
		logutil.BgLogger().Error("Error occur in checking cacheable", zap.Error(err))
		return false
	}
	if tb.Meta().GetPartitionInfo() != nil {
		return true
	}
	return false
}

// isPlanCacheable returns whether this plan is cacheable and the reason if not.
func isPlanCacheable(sctx sessionctx.Context, p Plan, paramNum, limitParamNum int) (cacheable bool, reason string) {
	var pp PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		pp = x.SelectPlan
	case *Update:
		pp = x.SelectPlan
	case *Delete:
		pp = x.SelectPlan
	case PhysicalPlan:
		pp = x
	default:
		return false, fmt.Sprintf("skip plan-cache: unexpected un-cacheable plan %v", p.ExplainID().String())
	}
	if pp == nil { // simple DML statements
		return true, ""
	}
	if limitParamNum != 0 && !sctx.GetSessionVars().EnablePlanCacheForParamLimit {
		return false, "skip plan-cache: the switch 'tidb_enable_plan_cache_for_param_limit' is off"
	}
	return isPhysicalPlanCacheable(sctx, pp, paramNum, limitParamNum)
}

// isPhysicalPlanCacheable returns whether this physical plan is cacheable and return the reason if not.
func isPhysicalPlanCacheable(sctx sessionctx.Context, p PhysicalPlan, paramNum, limitParamNum int) (cacheable bool, reason string) {
	switch x := p.(type) {
	case *PhysicalTableDual:
		if paramNum > 0 {
			return false, "skip plan-cache: get a TableDual plan"
		}
	case *PhysicalTableReader:
		if x.StoreType == kv.TiFlash {
			return false, "skip plan-cache: TiFlash plan is un-cacheable"
		}
	case *PhysicalShuffle, *PhysicalShuffleReceiverStub:
		return false, "skip plan-cache: get a Shuffle plan"
	case *PhysicalIndexMergeReader:
		if x.AccessMVIndex {
			return false, "skip plan-cache: the plan with IndexMerge accessing Multi-Valued Index is un-cacheable"
		}
	case *PhysicalApply:
		return false, "skip plan-cache: PhysicalApply plan is un-cacheable"
	}

	for _, c := range p.Children() {
		if cacheable, reason = isPhysicalPlanCacheable(sctx, c, paramNum, limitParamNum); !cacheable {
			return cacheable, reason
		}
	}
	return true, ""
}
