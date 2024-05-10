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
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/filter"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Cacheable checks whether the input ast(query) is cacheable with empty session context, which is mainly for testing.
// TODO: only for test, remove this function later on.
func Cacheable(node ast.Node, is infoschema.InfoSchema) bool {
	c, _ := IsASTCacheable(nil, nil, node, is)
	return c
}

// CacheableWithCtx checks whether the input ast(query) is cacheable.
// TODO: only for test, remove this function later on.
func CacheableWithCtx(sctx base.PlanContext, node ast.Node, is infoschema.InfoSchema) (bool, string) {
	return IsASTCacheable(nil, sctx, node, is)
}

// IsASTCacheable checks whether the input ast(query) is cacheable.
// Handle "ignore_plan_cache()" hint
// If there are multiple hints, only one will take effect
func IsASTCacheable(ctx context.Context, sctx base.PlanContext, node ast.Node, is infoschema.InfoSchema) (bool, string) {
	switch node.(type) {
	case *ast.SelectStmt, *ast.UpdateStmt, *ast.InsertStmt, *ast.DeleteStmt, *ast.SetOprStmt:
	default:
		return false, "not a SELECT/UPDATE/INSERT/DELETE/SET statement"
	}
	checker := cacheableChecker{
		ctx:          ctx,
		sctx:         sctx,
		cacheable:    true,
		schema:       is,
		sumInListLen: 0,
		maxNumParam:  getMaxParamLimit(sctx),
	}
	node.Accept(&checker)
	return checker.cacheable, checker.reason
}

// cacheableChecker checks whether a query can be cached:
type cacheableChecker struct {
	ctx       context.Context
	sctx      base.PlanContext
	cacheable bool
	schema    infoschema.InfoSchema
	reason    string // reason why cannot use plan-cache

	sumInListLen int // the accumulated number of elements in all in-lists
	maxNumParam  int
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == h.HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.DeleteStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == h.HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.UpdateStmt:
		for _, hints := range node.TableHints {
			if hints.HintName.L == h.HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.InsertStmt:
		if node.Select == nil {
			nRows := len(node.Lists)
			nCols := 0
			if len(node.Lists) > 0 { // avoid index-out-of-range
				nCols = len(node.Lists[0])
			}
			if nRows*nCols > checker.maxNumParam { // to save memory
				checker.cacheable = false
				checker.reason = "too many values in the insert statement"
				return in, true
			}
		}
		for _, hints := range node.TableHints {
			if hints.HintName.L == h.HintIgnorePlanCache {
				checker.cacheable = false
				checker.reason = "ignore plan cache by hint"
				return in, true
			}
		}
	case *ast.PatternInExpr:
		checker.sumInListLen += len(node.List)
		if checker.sumInListLen > checker.maxNumParam { // to save memory
			checker.cacheable = false
			checker.reason = "too many values in in-list"
			return in, true
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
			checker.cacheable, checker.reason = checkTableCacheable(checker.ctx, checker.sctx, checker.schema, node, false)
			if !checker.cacheable {
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

var nonPrepCacheCheckerPool = &sync.Pool{New: func() any { return &nonPreparedPlanCacheableChecker{} }}

// NonPreparedPlanCacheableWithCtx checks whether this SQL is cacheable for non-prepared plan cache.
func NonPreparedPlanCacheableWithCtx(sctx base.PlanContext, node ast.Node, is infoschema.InfoSchema) (ok bool, reason string) {
	selStmt, isSelect := node.(*ast.SelectStmt)
	if !sctx.GetSessionVars().EnableNonPreparedPlanCacheForDML &&
		(!isSelect || selStmt.LockInfo != nil) {
		return false, "not a SELECT statement"
	}

	maxNumParam := getMaxParamLimit(sctx)
	var tableNames []*ast.TableName
	switch x := node.(type) {
	case *ast.SelectStmt:
		tableNames, ok, reason = isSelectStmtNonPrepCacheableFastCheck(sctx, x)
		if !ok {
			return ok, reason
		}
	case *ast.UpdateStmt:
		if len(x.TableHints) > 0 {
			return false, "not support update statement with table hints"
		}
		if x.MultipleTable {
			return false, "not support multiple tables update statements"
		}
		tableNames, ok, reason = extractTableNames(x.TableRefs.TableRefs, tableNames)
		if !ok {
			return ok, reason
		}
	case *ast.InsertStmt:
		if len(x.TableHints) > 0 {
			return false, "not support insert statement with table hints"
		}
		if x.Select == nil { // `insert into t values (...)`
			nRows := len(x.Lists)
			nCols := 0
			if len(x.Lists) > 0 { // avoid index-out-of-range
				nCols = len(x.Lists[0])
			}
			if nRows*nCols > maxNumParam { // to save memory
				return false, "too many values in the insert statement"
			}
			tableNames, ok, reason = extractTableNames(x.Table.TableRefs, tableNames)
			if !ok {
				return ok, reason
			}
		} else { // `insert into t select ...`
			selectStmt, ok := x.Select.(*ast.SelectStmt)
			if !ok {
				return false, "not a select statement"
			}
			tableNames, ok, reason = isSelectStmtNonPrepCacheableFastCheck(sctx, selectStmt)
			if !ok {
				return ok, reason
			}
			tableNames, ok, reason = extractTableNames(x.Table.TableRefs, tableNames)
			if !ok {
				return ok, reason
			}
		}
	case *ast.DeleteStmt:
		if len(x.TableHints) > 0 {
			return false, "not support insert statement with table hints"
		}
		if x.IsMultiTable {
			return false, "not support multiple tables delete statements"
		}
		tableNames, ok, reason = extractTableNames(x.TableRefs.TableRefs, tableNames)
		if !ok {
			return ok, reason
		}
	default:
		return false, "not a SELECT/UPDATE/INSERT/DELETE statement"
	}

	// allocate and init the checker
	checker := nonPrepCacheCheckerPool.Get().(*nonPreparedPlanCacheableChecker)
	checker.reset(sctx, is, tableNames, maxNumParam)

	node.Accept(checker)
	cacheable, reason := checker.cacheable, checker.reason

	if !cacheable {
		// this metrics can measure the extra overhead of non-prep plan cache.
		core_metrics.GetNonPrepPlanCacheUnsupportedCounter().Inc()
	}

	// put the checker back
	nonPrepCacheCheckerPool.Put(checker)
	return cacheable, reason
}

// isSelectStmtNonPrepCacheableFastCheck checks whether the input select statement is cacheable for non-prepared plan cache.
func isSelectStmtNonPrepCacheableFastCheck(sctx base.PlanContext, selectStmt *ast.SelectStmt) (names []*ast.TableName, ok bool, reason string) {
	if selectStmt.Kind != ast.SelectStmtKindSelect {
		return nil, false, "not a select statement"
	}
	if len(selectStmt.TableHints) > 0 || // hints
		selectStmt.Having != nil || // having
		selectStmt.WindowSpecs != nil || // window function
		(selectStmt.Limit != nil && !sctx.GetSessionVars().EnablePlanCacheForParamLimit) || // limit
		selectStmt.SelectIntoOpt != nil { // select-into statement
		return nil, false, "queries that have hints, having-clause, window-function are not supported"
	}
	from := selectStmt.From
	if from == nil || selectStmt.From.TableRefs == nil {
		return nil, false, "queries that have sub-queries are not supported"
	}
	tableRefs := from.TableRefs

	// match table names, currently only support 2 tables(2-way join) at most.
	tableNames, ok, reason := extractTableNames(tableRefs, nil)
	if !ok {
		return nil, false, reason
	}
	return tableNames, true, ""
}

// extractTableNames extracts table names from the input node.
// Currently support 2 tables(2-way join) at most.
func extractTableNames(node ast.ResultSetNode, names []*ast.TableName) ([]*ast.TableName, bool, string) {
	var ok bool
	var reason string
	switch x := node.(type) {
	case *ast.TableSource:
		name, isName := x.Source.(*ast.TableName)
		if isName {
			names = append(names, name)
		} else {
			if x.Source != nil {
				names, ok, reason = extractTableNames(x.Source, names)
				if !ok {
					return nil, ok, reason
				}
			}
		}
	case *ast.Join:
		if x.Left != nil {
			names, ok, reason = extractTableNames(x.Left, names)
			if !ok {
				return nil, ok, reason
			}
		}
		if x.Right != nil {
			names, ok, reason = extractTableNames(x.Right, names)
			if !ok {
				return nil, ok, reason
			}
		}
	default:
		return names, false, "queries that have sub-queries are not supported"
	}
	if len(names) > 2 {
		return names, false, "queries that have more than 2 tables are not supported"
	}
	return names, true, ""
}

// nonPreparedPlanCacheableChecker checks whether a query's plan can be cached for non-prepared plan cache.
// NOTE: we can add more rules in the future.
type nonPreparedPlanCacheableChecker struct {
	sctx      base.PlanContext
	cacheable bool
	reason    string // reason why this statement cannot hit the cache
	schema    infoschema.InfoSchema

	tableNodes []*ast.TableName // only support 2-way joins currently

	constCnt  int // the number of constants/parameters in this query
	filterCnt int // the number of filters in the current node

	maxNumberParam int // the maximum number of parameters for a query to be cached.
}

func (checker *nonPreparedPlanCacheableChecker) reset(sctx base.PlanContext, schema infoschema.InfoSchema, tableNodes []*ast.TableName, maxNumberParam int) {
	checker.sctx = sctx
	checker.cacheable = true
	checker.schema = schema
	checker.reason = ""
	checker.tableNodes = tableNodes
	checker.constCnt = 0
	checker.filterCnt = 0
	checker.maxNumberParam = maxNumberParam
}

// Enter implements Visitor interface.
func (checker *nonPreparedPlanCacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if checker.isFilterNode(in) {
		checker.filterCnt++
	}

	switch node := in.(type) {
	case *ast.SelectStmt, *ast.FieldList, *ast.SelectField, *ast.TableRefsClause, *ast.Join, *ast.BetweenExpr, *ast.OnCondition,
		*ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.Assignment, *ast.ParenthesesExpr, *ast.RowExpr,
		*ast.TableSource, *ast.ColumnNameExpr, *ast.PatternInExpr, *ast.BinaryOperationExpr, *ast.ByItem, *ast.AggregateFuncExpr:
		return in, !checker.cacheable // skip child if un-cacheable
	case *ast.Limit:
		if !checker.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
			checker.cacheable = false
			checker.reason = "query has 'limit ?' is un-cacheable"
		}
		return in, !checker.cacheable
	case *ast.ColumnName:
		if checker.filterCnt > 0 {
			// this column is appearing some filters, e.g. `col = 1`
			colFound := false
			for _, tableNode := range checker.tableNodes {
				if tableNode == nil {
					continue
				}
				if colType, found := getColType(checker.schema, tableNode, node); found {
					colFound = found
					if colType == mysql.TypeJSON || colType == mysql.TypeEnum || colType == mysql.TypeSet || colType == mysql.TypeBit {
						checker.cacheable = false
						checker.reason = "query has some filters with JSON, Enum, Set or Bit columns"
					}
				}
			}
			if !colFound {
				checker.cacheable = false
				checker.reason = "some column is not found in table schema"
			}
		}
		return in, !checker.cacheable
	case *ast.FuncCallExpr:
		if _, found := expression.UnCacheableFunctions[node.FnName.L]; found {
			checker.cacheable = false
			checker.reason = "query has un-cacheable functions"
		}
		return in, !checker.cacheable
	case *driver.ValueExpr:
		if node.GetType().GetFlag()&mysql.UnderScoreCharsetFlag > 0 {
			// for safety, not support values with under-score charsets, e.g. select _latin1'abc' from t.
			checker.cacheable = false
			checker.reason = "query has values with under-score charset"
		}
		if node.Kind() == types.KindBinaryLiteral {
			// for safety, BIT / HEX literals are not supported.
			checker.cacheable = false
			checker.reason = "query has BIT / HEX literals are not supported"
		}
		if node.IsNull() {
			// for a condition like `not-null-col = null`, the planner will optimize it to `False` and generate a
			// table-dual plan, but if it is converted to `not-null-col = ?` here, then the planner cannot do this
			// optimization and a table-full-scan will be generated.
			checker.cacheable = false
			checker.reason = "query has null constants"
		}
		checker.constCnt++
		if checker.maxNumberParam > 0 && checker.constCnt > checker.maxNumberParam { // just for safety and reduce memory cost
			checker.cacheable = false
			checker.reason = "query has too many constants"
		}
		return in, !checker.cacheable
	case *ast.GroupByClause:
		for _, item := range node.Items {
			if _, isCol := item.Expr.(*ast.ColumnNameExpr); !isCol {
				checker.cacheable = false
				checker.reason = "only support group by {columns}'"
				return in, !checker.cacheable
			}
		}
		return in, !checker.cacheable
	case *ast.OrderByClause:
		for _, item := range node.Items {
			if _, isCol := item.Expr.(*ast.ColumnNameExpr); !isCol {
				checker.cacheable = false
				checker.reason = "only support order by {columns}'"
				return in, !checker.cacheable
			}
		}
		return in, !checker.cacheable
	case *ast.TableName:
		if filter.IsSystemSchema(node.Schema.O) {
			checker.cacheable = false
			checker.reason = "access tables in system schema"
			return in, !checker.cacheable
		}
		if checker.schema != nil {
			checker.cacheable, checker.reason = checkTableCacheable(nil, checker.sctx, checker.schema, node, true)
		}
		return in, !checker.cacheable
	}

	checker.cacheable = false // unexpected cases
	checker.reason = "query has some unsupported Node"
	return in, !checker.cacheable
}

// Leave implements Visitor interface.
func (checker *nonPreparedPlanCacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	if checker.isFilterNode(in) {
		checker.filterCnt--
	}
	return in, checker.cacheable
}

func (*nonPreparedPlanCacheableChecker) isFilterNode(node ast.Node) bool {
	switch node.(type) {
	case *ast.BetweenExpr, *ast.PatternInExpr, *ast.BinaryOperationExpr:
		return true
	}
	return false
}

func getColType(schema infoschema.InfoSchema, tbl *ast.TableName, col *ast.ColumnName) (colType byte, found bool) {
	if tbl == nil {
		return 0, false
	}
	tb, err := schema.TableByName(tbl.Schema, tbl.Name)
	if err != nil {
		return 0, false
	}
	for _, c := range tb.Cols() {
		if c.Name.L == col.Name.L {
			return c.GetType(), true
		}
	}
	return 0, false
}

// isPlanCacheable returns whether this plan is cacheable and the reason if not.
func isPlanCacheable(sctx base.PlanContext, p base.Plan, paramNum, limitParamNum int, hasSubQuery bool) (cacheable bool, reason string) {
	var pp base.PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		pp = x.SelectPlan
	case *Update:
		pp = x.SelectPlan
	case *Delete:
		pp = x.SelectPlan
	case base.PhysicalPlan:
		pp = x
	default:
		return false, fmt.Sprintf("unexpected un-cacheable plan %v", p.ExplainID().String())
	}
	if pp == nil { // simple DML statements
		return true, ""
	}
	if limitParamNum != 0 && !sctx.GetSessionVars().EnablePlanCacheForParamLimit {
		return false, "the switch 'tidb_enable_plan_cache_for_param_limit' is off"
	}
	if hasSubQuery && !sctx.GetSessionVars().EnablePlanCacheForSubquery {
		return false, "the switch 'tidb_enable_plan_cache_for_subquery' is off"
	}
	if sctx.GetSessionVars().PlanCacheMaxPlanSize > 0 && uint64(pp.MemoryUsage()) > sctx.GetSessionVars().PlanCacheMaxPlanSize { // to save memory
		return false, "plan is too large(decided by the variable @@tidb_plan_cache_max_plan_size)"
	}
	return isPhysicalPlanCacheable(sctx, pp, paramNum, limitParamNum, false)
}

// isPhysicalPlanCacheable returns whether this physical plan is cacheable and return the reason if not.
func isPhysicalPlanCacheable(sctx base.PlanContext, p base.PhysicalPlan, paramNum, limitParamNum int, underIndexMerge bool) (cacheable bool, reason string) {
	var subPlans []base.PhysicalPlan
	switch x := p.(type) {
	case *PhysicalTableDual:
		if paramNum > 0 {
			return false, "get a TableDual plan"
		}
	case *PhysicalTableReader:
		if x.StoreType == kv.TiFlash {
			return false, "TiFlash plan is un-cacheable"
		}
	case *PhysicalShuffle, *PhysicalShuffleReceiverStub:
		return false, "get a Shuffle plan"
	case *PhysicalMemTable:
		return false, "PhysicalMemTable plan is un-cacheable"
	case *PhysicalIndexMergeReader:
		if x.AccessMVIndex && !enablePlanCacheForGeneratedCols(sctx) {
			return false, "the plan with IndexMerge accessing Multi-Valued Index is un-cacheable"
		}
		underIndexMerge = true
		subPlans = append(subPlans, x.partialPlans...)
	case *PhysicalIndexScan:
		if underIndexMerge && x.isFullScan() {
			return false, "IndexMerge plan with full-scan is un-cacheable"
		}
	case *PhysicalTableScan:
		if underIndexMerge && x.isFullScan() {
			return false, "IndexMerge plan with full-scan is un-cacheable"
		}
	case *PhysicalApply:
		return false, "PhysicalApply plan is un-cacheable"
	}

	subPlans = append(subPlans, p.Children()...)
	for _, c := range subPlans {
		if cacheable, reason = isPhysicalPlanCacheable(sctx, c, paramNum, limitParamNum, underIndexMerge); !cacheable {
			return cacheable, reason
		}
	}
	return true, ""
}

// getMaxParamLimit returns the maximum number of parameters for a query that can be cached in the Plan Cache.
func getMaxParamLimit(sctx base.PlanContext) int {
	v := 200
	if sctx == nil || sctx.GetSessionVars() == nil || sctx.GetSessionVars().OptimizerFixControl == nil {
		return v
	}
	n := fixcontrol.GetIntWithDefault(sctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44823, int64(v))
	if n == 0 {
		v = math.MaxInt32 // no limitation
	} else if n > 0 {
		v = int(n)
	}

	return v
}

func enablePlanCacheForGeneratedCols(sctx base.PlanContext) bool {
	// disable this by default since it's not well tested.
	defaultVal := true
	if sctx == nil || sctx.GetSessionVars() == nil || sctx.GetSessionVars().GetOptimizerFixControlMap() == nil {
		return defaultVal
	}
	return fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix45798, defaultVal)
}

// checkTableCacheable checks whether a query accessing this table is cacheable.
func checkTableCacheable(ctx context.Context, sctx base.PlanContext, schema infoschema.InfoSchema, node *ast.TableName, isNonPrep bool) (cacheable bool, reason string) {
	tableSchema := node.Schema
	if tableSchema.L == "" {
		tableSchema.O = sctx.GetSessionVars().CurrentDB
		tableSchema.L = strings.ToLower(tableSchema.O)
	}
	tb, err := schema.TableByName(tableSchema, node.Name)
	if intest.InTest && ctx != nil && ctx.Value(PlanCacheKeyTestIssue46760{}) != nil {
		err = errors.New("mock error")
	}
	if err != nil {
		sql := sctx.GetSessionVars().StmtCtx.OriginalSQL
		if len(sql) > 256 {
			sql = sql[:256]
		}
		logutil.BgLogger().Warn("find table failed", zap.Error(err), zap.String("sql", sql),
			zap.String("table_schema", tableSchema.O), zap.String("table_name", node.Name.O))
		return false, fmt.Sprintf("find table %s.%s failed: %s", tableSchema, node.Name, err.Error())
	}

	if tb.Meta().GetPartitionInfo() != nil {
		if sctx == nil || !sctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
			return false, "query accesses partitioned tables is un-cacheable if tidb_partition_pruning_mode = 'static'"
		}
		if fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix33031, false) {
			return false, "Fix33031 fix-control set and partitioned table"
		}
	}

	if !enablePlanCacheForGeneratedCols(sctx) {
		for _, col := range tb.Cols() {
			if col.IsGenerated() {
				return false, "query accesses generated columns is un-cacheable"
			}
		}
	}
	if tb.Meta().TempTableType != model.TempTableNone {
		return false, "query accesses temporary tables is un-cacheable"
	}

	if isNonPrep { // non-prep plan cache is stricter
		if tb.Meta().IsView() {
			return false, "queries that access views are not supported"
		}
		if !tb.Type().IsNormalTable() {
			return false, "queries that access in-memory tables"
		}
	}

	return true, ""
}
