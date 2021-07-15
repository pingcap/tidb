// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	util2 "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/set"
)

const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// HintSMJ is hint enforce merge join.
	HintSMJ = "merge_join"

	// TiDBBroadCastJoin indicates applying broadcast join by force.
	TiDBBroadCastJoin = "tidb_bcj"

	// HintBCJ indicates applying broadcast join by force.
	HintBCJ = "broadcast_join"
	// HintBCJPreferLocal specifies the preferred local read table
	HintBCJPreferLocal = "broadcast_join_local"

	// TiDBIndexNestedLoopJoin is hint enforce index nested loop join.
	TiDBIndexNestedLoopJoin = "tidb_inlj"
	// HintINLJ is hint enforce index nested loop join.
	HintINLJ = "inl_join"
	// HintINLHJ is hint enforce index nested loop hash join.
	HintINLHJ = "inl_hash_join"
	// HintINLMJ is hint enforce index nested loop merge join.
	HintINLMJ = "inl_merge_join"
	// TiDBHashJoin is hint enforce hash join.
	TiDBHashJoin = "tidb_hj"
	// HintHJ is hint enforce hash join.
	HintHJ = "hash_join"
	// HintHashAgg is hint enforce hash aggregation.
	HintHashAgg = "hash_agg"
	// HintStreamAgg is hint enforce stream aggregation.
	HintStreamAgg = "stream_agg"
	// HintUseIndex is hint enforce using some indexes.
	HintUseIndex = "use_index"
	// HintIgnoreIndex is hint enforce ignoring some indexes.
	HintIgnoreIndex = "ignore_index"
	// HintForceIndex make optimizer to use this index even if it thinks a table scan is more efficient.
	HintForceIndex = "force_index"
	// HintAggToCop is hint enforce pushing aggregation to coprocessor.
	HintAggToCop = "agg_to_cop"
	// HintReadFromStorage is hint enforce some tables read from specific type of storage.
	HintReadFromStorage = "read_from_storage"
	// HintTiFlash is a label represents the tiflash storage type.
	HintTiFlash = "tiflash"
	// HintTiKV is a label represents the tikv storage type.
	HintTiKV = "tikv"
	// HintIndexMerge is a hint to enforce using some indexes at the same time.
	HintIndexMerge = "use_index_merge"
	// HintTimeRange is a hint to specify the time range for metrics summary tables
	HintTimeRange = "time_range"
	// HintIgnorePlanCache is a hint to enforce ignoring plan cache
	HintIgnorePlanCache = "ignore_plan_cache"
	// HintLimitToCop is a hint enforce pushing limit or topn to coprocessor.
	HintLimitToCop = "limit_to_cop"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

// aggOrderByResolver is currently resolving expressions of order by clause
// in aggregate function GROUP_CONCAT.
type aggOrderByResolver struct {
	ctx       sessionctx.Context
	err       error
	args      []ast.ExprNode
	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (a *aggOrderByResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	a.exprDepth++
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		if a.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(a.ctx, n)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
	}
	return inNode, false
}

func (a *aggOrderByResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(a.ctx, v)
		if err != nil {
			a.err = err
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(a.args) {
			errPos := strconv.Itoa(pos)
			if v.P != nil {
				errPos = "?"
			}
			a.err = ErrUnknownColumn.FastGenByArgs(errPos, "order clause")
			return inNode, false
		}
		ret := a.args[pos-1]
		return ret, true
	}
	return inNode, true
}

func (b *PlanBuilder) buildAggregation(ctx context.Context, p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression,
	correlatedAggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, map[int]int, error) {
	b.optFlag |= flagBuildKeyInfo
	b.optFlag |= flagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag |= flagMaxMinEliminate
	b.optFlag |= flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag |= flagPredicatePushDown
	b.optFlag |= flagEliminateAgg
	b.optFlag |= flagEliminateProjection

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx, b.getSelectOffset())
	if hint := b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	allAggsFirstRow := true
	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, nil, err
		}
		if newFunc.Name != ast.AggFuncFirstRow {
			allAggsFirstRow = false
		}
		if aggFunc.Order != nil {
			trueArgs := aggFunc.Args[:len(aggFunc.Args)-1] // the last argument is SEPARATOR, remote it.
			resolver := &aggOrderByResolver{
				ctx:  b.ctx,
				args: trueArgs,
			}
			for _, byItem := range aggFunc.Order.Items {
				resolver.exprDepth = 0
				resolver.err = nil
				retExpr, _ := byItem.Expr.Accept(resolver)
				if resolver.err != nil {
					return nil, nil, errors.Trace(resolver.err)
				}
				newByItem, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
				if err != nil {
					return nil, nil, err
				}
				p = np
				newFunc.OrderByItems = append(newFunc.OrderByItems, &util.ByItems{Expr: newByItem, Desc: byItem.Desc})
			}
		}
		// combine identical aggregate functions
		combined := false
		for j := 0; j < i; j++ {
			oldFunc := plan4Agg.AggFuncs[aggIndexMap[j]]
			if oldFunc.Equal(b.ctx, newFunc) {
				aggIndexMap[i] = aggIndexMap[j]
				combined = true
				if _, ok := correlatedAggMap[aggFunc]; ok {
					if _, ok = b.correlatedAggMapper[aggFuncList[j]]; !ok {
						b.correlatedAggMapper[aggFuncList[j]] = &expression.CorrelatedColumn{
							Column: *schema4Agg.Columns[aggIndexMap[j]],
						}
					}
					b.correlatedAggMapper[aggFunc] = b.correlatedAggMapper[aggFuncList[j]]
				}
				break
			}
		}
		// create new columns for aggregate functions which show up first
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			column := expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  newFunc.RetTp,
			}
			schema4Agg.Append(&column)
			names = append(names, types.EmptyName)
			if _, ok := correlatedAggMap[aggFunc]; ok {
				b.correlatedAggMapper[aggFunc] = &expression.CorrelatedColumn{
					Column: column,
				}
			}
		}
	}
	for i, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
		names = append(names, p.OutputNames()[i])
	}
	if join, isJoin := p.(*LogicalJoin); isJoin && join.redundantSchema != nil {
		for i, col := range join.redundantSchema.Columns {
			if p.Schema().Contains(col) {
				continue
			}
			newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
			if err != nil {
				return nil, nil, err
			}
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			newCol, _ := col.Clone().(*expression.Column)
			newCol.RetType = newFunc.RetTp
			schema4Agg.Append(newCol)
			names = append(names, join.redundantNames[i])
		}
	}
	hasGroupBy := len(gbyItems) > 0
	for i, aggFunc := range plan4Agg.AggFuncs {
		err := aggFunc.UpdateNotNullFlag4RetType(hasGroupBy, allAggsFirstRow)
		if err != nil {
			return nil, nil, err
		}
		schema4Agg.Columns[i].RetType = aggFunc.RetTp
	}
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildTableRefs(ctx context.Context, from *ast.TableRefsClause) (p LogicalPlan, err error) {
	if from == nil {
		p = b.buildTableDual()
		return
	}
	defer func() {
		// After build the resultSetNode, need to reset it so that it can be referenced by outer level.
		for _, cte := range b.outerCTEs {
			cte.recursiveRef = false
		}
	}()
	return b.buildResultSetNode(ctx, from.TableRefs)
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		var isTableName bool
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			p, err = b.buildSelect(ctx, v)
		case *ast.SetOprStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			p, err = b.buildSetOpr(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
			isTableName = true
		default:
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}

		for _, name := range p.OutputNames() {
			if name.Hidden {
				continue
			}
			if x.AsName.L != "" {
				name.TblName = x.AsName
			}
		}
		// `TableName` is not a select block, so we do not need to handle it.
		if !isTableName && b.ctx.GetSessionVars().PlannerSelectBlockAsName != nil {
			b.ctx.GetSessionVars().PlannerSelectBlockAsName[p.SelectBlockOffset()] = ast.HintTable{DBName: p.OutputNames()[0].DBName, TableName: p.OutputNames()[0].TblName}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, name := range p.OutputNames() {
			colName := name.ColName.O
			if _, ok := dupNames[colName]; ok {
				return nil, ErrDupFieldName.GenWithStackByArgs(colName)
			}
			dupNames[colName] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case SemiJoin, InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	return p.ExtractOnCondition(conditions, p.children[0].Schema(), p.children[1].Schema(), deriveLeft, deriveRight)
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	for _, expr := range conditions {
		// For queries like `select a in (select a from s where s.b = t.b) from t`,
		// if subquery is empty caused by `s.b = t.b`, the result should always be
		// false even if t.a is null or s.a is null. To make this join "empty aware",
		// we should differentiate `t.a = s.a` from other column equal conditions, so
		// we put it into OtherConditions instead of EqualConditions of join.
		if expression.IsEQCondFromIn(expr) {
			otherCond = append(otherCond, expr)
			continue
		}
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			ctx := binop.GetCtx()
			arg0, lOK := binop.GetArgs()[0].(*expression.Column)
			arg1, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if isNullRejected(ctx, leftSchema, expr) && !mysql.HasNotNullFlag(leftCol.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if isNullRejected(ctx, rightSchema, expr) && !mysql.HasNotNullFlag(rightCol.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					if binop.FuncName.L == ast.EQ {
						cond := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// extractTableAlias returns table alias of the LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the logicalPlan match some optimizer hints, and hints are not expected to take effect in this case.
func extractTableAlias(p Plan, parentOffset int) *hintTableInfo {
	if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
		firstName := p.OutputNames()[0]
		for _, name := range p.OutputNames() {
			if name.TblName.L != firstName.TblName.L || name.DBName.L != firstName.DBName.L {
				return nil
			}
		}
		blockOffset := p.SelectBlockOffset()
		blockAsNames := p.SCtx().GetSessionVars().PlannerSelectBlockAsName
		// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
		if blockOffset != parentOffset && blockAsNames != nil && blockAsNames[blockOffset].TableName.L != "" {
			blockOffset = parentOffset
		}
		dbName := firstName.DBName
		if dbName.L == "" {
			dbName = model.NewCIStr(p.SCtx().GetSessionVars().CurrentDB)
		}
		return &hintTableInfo{dbName: dbName, tblName: firstName.TblName, selectOffset: blockOffset}
	}
	return nil
}

func (p *LogicalJoin) getPreferredBCJLocalIndex() (hasPrefer bool, prefer int) {
	if p.hintInfo == nil {
		return
	}
	if p.hintInfo.ifPreferAsLocalInBCJoin(p.children[0], p.blockOffset) {
		return true, 0
	}
	if p.hintInfo.ifPreferAsLocalInBCJoin(p.children[1], p.blockOffset) {
		return true, 1
	}
	return false, 0
}

func (p *LogicalJoin) setPreferredJoinType(hintInfo *tableHintInfo) {
	if hintInfo == nil {
		return
	}

	lhsAlias := extractTableAlias(p.children[0], p.blockOffset)
	rhsAlias := extractTableAlias(p.children[1], p.blockOffset)
	if hintInfo.ifPreferMergeJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferMergeJoin
	}
	if hintInfo.ifPreferBroadcastJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferBCJoin
	}
	if hintInfo.ifPreferHashJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferHashJoin
	}
	if hintInfo.ifPreferINLJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLJInner
	}
	if hintInfo.ifPreferINLJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLJInner
	}
	if hintInfo.ifPreferINLHJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLHJInner
	}
	if hintInfo.ifPreferINLHJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLHJInner
	}
	if hintInfo.ifPreferINLMJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLMJInner
	}
	if hintInfo.ifPreferINLMJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLMJInner
	}
	if containDifferentJoinTypes(p.preferJoinType) {
		errMsg := "Join hints are conflict, you can only specify one type of join"
		warning := ErrInternal.GenWithStack(errMsg)
		p.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		p.preferJoinType = 0
	}
	// set hintInfo for further usage if this hint info can be used.
	if p.preferJoinType != 0 {
		p.hintInfo = hintInfo
	}
}

func (ds *DataSource) setPreferredStoreType(hintInfo *tableHintInfo) {
	if hintInfo == nil {
		return
	}

	var alias *hintTableInfo
	if len(ds.TableAsName.L) != 0 {
		alias = &hintTableInfo{dbName: ds.DBName, tblName: *ds.TableAsName, selectOffset: ds.SelectBlockOffset()}
	} else {
		alias = &hintTableInfo{dbName: ds.DBName, tblName: ds.tableInfo.Name, selectOffset: ds.SelectBlockOffset()}
	}
	if hintTbl := hintInfo.ifPreferTiKV(alias); hintTbl != nil {
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == kv.TiKV {
				ds.preferStoreType |= preferTiKV
				ds.preferPartitions[preferTiKV] = hintTbl.partitions
				break
			}
		}
		if ds.preferStoreType&preferTiKV == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.table.Meta().Name.O, kv.TiKV.Name(), ds.ctx.GetSessionVars().GetIsolationReadEngines())
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		} else {
			ds.ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have set a hint to read table `" + hintTbl.tblName.O + "` from TiKV.")
		}
	}
	if hintTbl := hintInfo.ifPreferTiFlash(alias); hintTbl != nil {
		// `ds.preferStoreType != 0`, which means there's a hint hit the both TiKV value and TiFlash value for table.
		// We can't support read a table from two different storages, even partition table.
		if ds.preferStoreType != 0 {
			errMsg := fmt.Sprintf("Storage hints are conflict, you can only specify one storage type of table %s.%s",
				alias.dbName.L, alias.tblName.L)
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
			ds.preferStoreType = 0
			return
		}
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == kv.TiFlash {
				ds.preferStoreType |= preferTiFlash
				ds.preferPartitions[preferTiFlash] = hintTbl.partitions
				break
			}
		}
		if ds.preferStoreType&preferTiFlash == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.table.Meta().Name.O, kv.TiFlash.Name(), ds.ctx.GetSessionVars().GetIsolationReadEngines())
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		}
	}
}

func resetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.Flag &= ^mysql.NotNullFlag
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left)
	}

	b.optFlag = b.optFlag | flagPredicatePushDown
	// Add join reorder flag regardless of inner join or outer join.
	b.optFlag = b.optFlag | flagJoinReOrder

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right)
	if err != nil {
		return nil, err
	}

	// The recursive part in CTE must not be on the right side of a LEFT JOIN.
	if lc, ok := rightPlan.(*LogicalCTETable); ok && joinNode.Tp == ast.LeftJoin {
		return nil, ErrCTERecursiveForbiddenJoinOrder.GenWithStackByArgs(lc.name)
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = LeftOuterJoin
		resetNotNullFlag(joinPlan.schema, leftPlan.Schema().Len(), joinPlan.schema.Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = RightOuterJoin
		resetNotNullFlag(joinPlan.schema, 0, leftPlan.Schema().Len())
	default:
		joinPlan.JoinType = InnerJoin
	}

	// Merge sub join's redundantSchema into this join plan. When handle query like
	// select t2.a from (t1 join t2 using (a)) join t3 using (a);
	// we can simply search in the top level join plan to find redundant column.
	var (
		lRedundantSchema, rRedundantSchema *expression.Schema
		lRedundantNames, rRedundantNames   types.NameSlice
	)
	if left, ok := leftPlan.(*LogicalJoin); ok && left.redundantSchema != nil {
		lRedundantSchema = left.redundantSchema
		lRedundantNames = left.redundantNames
	}
	if right, ok := rightPlan.(*LogicalJoin); ok && right.redundantSchema != nil {
		rRedundantSchema = right.redundantSchema
		rRedundantNames = right.redundantNames
	}
	joinPlan.redundantSchema = expression.MergeSchema(lRedundantSchema, rRedundantSchema)
	joinPlan.redundantNames = make([]*types.FieldName, len(lRedundantNames)+len(rRedundantNames))
	copy(joinPlan.redundantNames, lRedundantNames)
	copy(joinPlan.redundantNames[len(lRedundantNames):], rRedundantNames)

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.setPreferredJoinType(b.TableHints())

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		// Keep these expressions as a LogicalSelection upon the inner join, in order to apply
		// possible decorrelate optimizations. The ON clause is actually treated as a WHERE clause now.
		if joinPlan.JoinType == InnerJoin {
			sel := LogicalSelection{Conditions: onCondition}.Init(b.ctx, b.getSelectOffset())
			sel.SetChildren(joinPlan)
			return sel, nil
		}
		joinPlan.AttachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		// If a inner join without "ON" or "USING" clause, it's a cartesian
		// product over the join tables.
		joinPlan.cartesianJoin = true
	}

	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
// 1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//    appears in "leftPlan".
// 2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
// 3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, filter)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.setSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
// 	All the common columns
// 	Every column in the first (left) table that is not a common column
// 	Every column in the second (right) table that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, nil)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.setSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonColumns(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, joinTp ast.JoinType, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	if joinTp == ast.LeftJoin {
		resetNotNullFlag(rsc, 0, rsc.Len())
	} else if joinTp == ast.RightJoin {
		resetNotNullFlag(lsc, 0, lsc.Len())
	}
	lColumns, rColumns := lsc.Columns, rsc.Columns
	lNames, rNames := leftPlan.OutputNames().Shallow(), rightPlan.OutputNames().Shallow()
	if joinTp == ast.RightJoin {
		lNames, rNames = rNames, lNames
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Check using clause with ambiguous columns.
	if filter != nil {
		checkAmbiguous := func(names types.NameSlice) error {
			columnNameInFilter := set.StringSet{}
			for _, name := range names {
				if _, ok := filter[name.ColName.L]; !ok {
					continue
				}
				if columnNameInFilter.Exist(name.ColName.L) {
					return ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				columnNameInFilter.Insert(name.ColName.L)
			}
			return nil
		}
		err := checkAmbiguous(lNames)
		if err != nil {
			return err
		}
		err = checkAmbiguous(rNames)
		if err != nil {
			return err
		}
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lName := range lNames {
		// Natural join should ignore _tidb_rowid
		if lName.ColName.L == "_tidb_rowid" {
			continue
		}
		for j := commonLen; j < len(rNames); j++ {
			if lName.ColName.L != rNames[j].ColName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lName.ColName.L] {
					break
				}
				// Mark this column exist.
				filter[lName.ColName.L] = false
			}

			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			name := lNames[i]
			copy(lNames[commonLen+1:i+1], lNames[commonLen:i])
			lNames[commonLen] = name

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			name = rNames[j]
			copy(rNames[commonLen+1:j+1], rNames[commonLen:j])
			rNames[commonLen] = name

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])
	names := make(types.NameSlice, len(schemaCols))
	copy(names, lNames)
	copy(names[len(lNames):], rNames[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := 0; i < commonLen; i++ {
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
	}

	p.SetSchema(expression.NewSchema(schemaCols...))
	p.names = names
	if joinTp == ast.RightJoin {
		leftPlan, rightPlan = rightPlan, leftPlan
	}
	// We record the full `rightPlan.Schema` as `redundantSchema` in order to
	// record the redundant column in `rightPlan` and the output columns order
	// of the `rightPlan`.
	// For SQL like `select t1.*, t2.* from t1 left join t2 using(a)`, we can
	// retrieve the column order of `t2.*` from the `redundantSchema`.
	p.redundantSchema = expression.MergeSchema(p.redundantSchema, expression.NewSchema(rightPlan.Schema().Clone().Columns...))
	p.redundantNames = append(p.redundantNames.Shallow(), rightPlan.OutputNames().Shallow()...)
	if joinTp == ast.RightJoin || joinTp == ast.LeftJoin {
		resetNotNullFlag(p.redundantSchema, 0, p.redundantSchema.Len())
	}
	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
	b.optFlag |= flagPredicatePushDown
	if b.curClause != havingClause {
		b.curClause = whereClause
	}

	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx, b.getSelectOffset())
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, AggMapper, false)
		if err != nil {
			return nil, err
		}
		p = np
		if expr == nil {
			continue
		}
		expressions = append(expressions, expr)
	}
	cnfExpres := make([]expression.Expression, 0)
	for _, expr := range expressions {
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
				ret, _, err := expression.EvalBool(b.ctx, expression.CNFExprs{con}, chunk.Row{})
				if err != nil || ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
				dual.names = p.OutputNames()
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			cnfExpres = append(cnfExpres, item)
		}
	}
	if len(cnfExpres) == 0 {
		return p, nil
	}
	// check expr field types.
	for i, expr := range cnfExpres {
		if expr.GetType().EvalType() == types.ETString {
			tp := &types.FieldType{
				Tp:      mysql.TypeDouble,
				Flag:    expr.GetType().Flag,
				Flen:    mysql.MaxRealWidth,
				Decimal: types.UnspecifiedLength,
			}
			types.SetBinChsClnFlag(tp)
			cnfExpres[i] = expression.TryPushCastIntoControlFunctionForHybridType(b.ctx, expr, tp)
		}
	}
	selection.Conditions = cnfExpres
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (b *PlanBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, name *types.FieldName) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Table
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(ctx context.Context, field *ast.SelectField) (model.CIStr, error) {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name, nil
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	funcCall, isFuncCall := innerExpr.(*ast.FuncCallExpr)
	// When used to produce a result set column, NAME_CONST() causes the column to have the given name.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
	if isFuncCall && funcCall.FnName.L == ast.NameConst {
		if v, err := evalAstExpr(b.ctx, funcCall.Args[0]); err == nil {
			if s, err := v.ToString(); err == nil {
				return model.NewCIStr(s), nil
			}
		}
		return model.NewCIStr(""), ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
	}
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return model.NewCIStr(fieldName), nil
	case types.KindNull:
		// See #4053, #3685
		return model.NewCIStr("NULL"), nil
	case types.KindBinaryLiteral:
		// Don't rewrite BIT literal or HEX literals
		return model.NewCIStr(field.Text()), nil
	case types.KindInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.Flag) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return model.NewCIStr("FALSE"), nil
			}
			return model.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return model.NewCIStr(fieldName), nil
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.Column, *types.FieldName, error) {
	var origTblName, tblName, origColName, colName, dbName model.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isCol := expr.(*expression.Column)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		idx := p.Schema().ColumnIndex(col)
		var name *types.FieldName
		// The column maybe the one from join's redundant part.
		if idx == -1 {
			name = findColFromNaturalUsingJoin(p, col)
		} else {
			name = p.OutputNames()[idx]
		}
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	if isCol {
		return col, name, nil
	}
	newCol := &expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
	}
	newCol.SetCoercibility(expr.Coercibility())
	return newCol, name, nil
}

type userVarTypeProcessor struct {
	ctx     context.Context
	plan    LogicalPlan
	builder *PlanBuilder
	mapper  map[*ast.AggregateFuncExpr]int
	err     error
}

func (p *userVarTypeProcessor) Enter(in ast.Node) (ast.Node, bool) {
	v, ok := in.(*ast.VariableExpr)
	if !ok {
		return in, false
	}
	if v.IsSystem || v.Value == nil {
		return in, true
	}
	_, p.plan, p.err = p.builder.rewrite(p.ctx, v, p.plan, p.mapper, true)
	return in, true
}

func (p *userVarTypeProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, p.err == nil
}

func (b *PlanBuilder) preprocessUserVarTypes(ctx context.Context, p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) error {
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	for agg, i := range mapper {
		aggMapper[agg] = i
	}
	processor := userVarTypeProcessor{
		ctx:     ctx,
		plan:    p,
		builder: b,
		mapper:  aggMapper,
	}
	for _, field := range fields {
		field.Expr.Accept(&processor)
		if processor.err != nil {
			return processor.err
		}
	}
	return nil
}

// findColFromNaturalUsingJoin is used to recursively find the column from the
// underlying natural-using-join.
// e.g. For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0`, the
// plan will be `join->selection->projection`. The schema of the `selection`
// will be `[t1.a]`, thus we need to recursively retrieve the `t2.a` from the
// underlying join.
func findColFromNaturalUsingJoin(p LogicalPlan, col *expression.Column) (name *types.FieldName) {
	switch x := p.(type) {
	case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort, *LogicalMaxOneRow:
		return findColFromNaturalUsingJoin(p.Children()[0], col)
	case *LogicalJoin:
		if x.redundantSchema != nil {
			idx := x.redundantSchema.ColumnIndex(col)
			return x.redundantNames[idx]
		}
	}
	return nil
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(ctx context.Context, p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int, considerWindow bool, expandGenerateColumn bool) (LogicalPlan, []expression.Expression, int, error) {
	err := b.preprocessUserVarTypes(ctx, p, fields, mapper)
	if err != nil {
		return nil, nil, 0, err
	}
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx, b.getSelectOffset())
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	newNames := make([]*types.FieldName, 0, len(fields))
	for i, field := range fields {
		if !field.Auxiliary {
			oldLen++
		}

		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schema columns.
		if considerWindow && !isWindowFuncField {
			col := p.Schema().Columns[i]
			proj.Exprs = append(proj.Exprs, col)
			schema.Append(col)
			newNames = append(newNames, p.OutputNames()[i])
			continue
		} else if !considerWindow && isWindowFuncField {
			expr := expression.NewZero()
			proj.Exprs = append(proj.Exprs, expr)
			col, name, err := b.buildProjectionField(ctx, p, field, expr)
			if err != nil {
				return nil, nil, 0, err
			}
			schema.Append(col)
			newNames = append(newNames, name)
			continue
		}
		newExpr, np, err := b.rewriteWithPreprocess(ctx, field.Expr, p, mapper, windowMapper, true, nil)
		if err != nil {
			return nil, nil, 0, err
		}

		// For window functions in the order by clause, we will append an field for it.
		// We need rewrite the window mapper here so order by clause could find the added field.
		if considerWindow && isWindowFuncField && field.Auxiliary {
			if windowExpr, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				windowMapper[windowExpr] = i
			}
		}

		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, nil, 0, err
		}
		schema.Append(col)
		newNames = append(newNames, name)
	}
	proj.SetSchema(schema)
	proj.names = newNames
	if expandGenerateColumn {
		// Sometimes we need to add some fields to the projection so that we can use generate column substitute
		// optimization. For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
		// an index on c. We need to add c into the projection so that we can replace a+1 with c.
		exprToColumn := make(ExprColumnMap)
		collectGenerateColumn(p, exprToColumn)
		for expr, col := range exprToColumn {
			idx := p.Schema().ColumnIndex(col)
			if idx == -1 {
				continue
			}
			if proj.schema.Contains(col) {
				continue
			}
			proj.schema.Columns = append(proj.schema.Columns, col)
			proj.Exprs = append(proj.Exprs, expr)
			proj.names = append(proj.names, p.OutputNames()[idx])
		}
	}
	proj.SetChildren(p)
	return proj, proj.Exprs, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child LogicalPlan, length int) (*LogicalAggregation, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.Init(b.ctx, child.SelectBlockOffset())
	if hint := b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	for _, col := range child.Schema().Columns {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	plan4Agg.names = child.OutputNames()
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schema.Columns {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
// Note that unionJoinFieldType doesn't handle charset and collation, caller need to handle it by itself.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	resultTp := types.NewFieldType(types.MergeFieldType(a.Tp, b.Tp))
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.Tp == mysql.TypeNewDecimal {
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.Flag &= b.Flag & mysql.UnsignedFlag
	} else {
		// Non-decimal results will be unsigned when the first SQL statement result in the union is unsigned.
		resultTp.Flag |= a.Flag & mysql.UnsignedFlag
	}
	resultTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
	// `Flen - Decimal` is the fraction before '.'
	resultTp.Flen = mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal) + resultTp.Decimal
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) && resultTp.Flen < mysql.MaxIntWidth {
		resultTp.Flen = mysql.MaxIntWidth
	}
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

func (b *PlanBuilder) buildProjection4Union(ctx context.Context, u *LogicalUnionAll) error {
	unionCols := make([]*expression.Column, 0, u.children[0].Schema().Len())
	names := make([]*types.FieldName, 0, u.children[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.children[0].Schema().Columns {
		tmpExprs := make([]expression.Expression, 0, len(u.Children()))
		tmpExprs = append(tmpExprs, col)
		resultTp := col.RetType
		for j := 1; j < len(u.children); j++ {
			tmpExprs = append(tmpExprs, u.children[j].Schema().Columns[i])
			childTp := u.children[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		if err := expression.CheckIllegalMixCollation("UNION", tmpExprs, types.ETInt); err != nil {
			return err
		}
		resultTp.Charset, resultTp.Collate = expression.DeriveCollationFromExprs(b.ctx, tmpExprs...)
		names = append(names, &types.FieldName{ColName: u.children[0].OutputNames()[i].ColName})
		unionCols = append(unionCols, &expression.Column{
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	u.schema = expression.NewSchema(unionCols...)
	u.names = names
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.children {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx, srcCol, dstType)
			} else {
				exprs[i] = srcCol
			}
		}
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.schema.Clone())
		// reset the schema type to make the "not null" flag right.
		for i, expr := range exprs {
			proj.schema.Columns[i].RetType = expr.GetType()
		}
		proj.SetChildren(child)
		u.children[childID] = proj
	}
	return nil
}

func (b *PlanBuilder) buildSetOpr(ctx context.Context, setOpr *ast.SetOprStmt) (LogicalPlan, error) {
	if setOpr.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		err := b.buildWith(ctx, setOpr.With)
		if err != nil {
			return nil, err
		}
	}

	// Because INTERSECT has higher precedence than UNION and EXCEPT. We build it first.
	selectPlans := make([]LogicalPlan, 0, len(setOpr.SelectList.Selects))
	afterSetOprs := make([]*ast.SetOprType, 0, len(setOpr.SelectList.Selects))
	selects := setOpr.SelectList.Selects
	for i := 0; i < len(selects); i++ {
		intersects := []ast.Node{selects[i]}
		for i+1 < len(selects) {
			breakIteration := false
			switch x := selects[i+1].(type) {
			case *ast.SelectStmt:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
			case *ast.SetOprSelectList:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
			}
			if breakIteration {
				break
			}
			intersects = append(intersects, selects[i+1])
			i++
		}
		selectPlan, afterSetOpr, err := b.buildIntersect(ctx, intersects)
		if err != nil {
			return nil, err
		}
		selectPlans = append(selectPlans, selectPlan)
		afterSetOprs = append(afterSetOprs, afterSetOpr)
	}
	setOprPlan, err := b.buildExcept(ctx, selectPlans, afterSetOprs)
	if err != nil {
		return nil, err
	}

	oldLen := setOprPlan.Schema().Len()

	for i := 0; i < len(setOpr.SelectList.Selects); i++ {
		b.handleHelper.popMap()
	}
	b.handleHelper.pushMap(nil)

	if setOpr.OrderBy != nil {
		setOprPlan, err = b.buildSort(ctx, setOprPlan, setOpr.OrderBy.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if setOpr.Limit != nil {
		setOprPlan, err = b.buildLimit(setOprPlan, setOpr.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Fix issue #8189 (https://github.com/pingcap/tidb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	if oldLen != setOprPlan.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(setOprPlan.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(setOprPlan)
		schema := expression.NewSchema(setOprPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.names = setOprPlan.OutputNames()[:oldLen]
		proj.SetSchema(schema)
		return proj, nil
	}
	return setOprPlan, nil
}

func (b *PlanBuilder) buildSemiJoinForSetOperator(
	leftOriginPlan LogicalPlan,
	rightPlan LogicalPlan,
	joinType JoinType) (leftPlan LogicalPlan, err error) {
	leftPlan, err = b.buildDistinct(leftOriginPlan, leftOriginPlan.Schema().Len())
	if err != nil {
		return nil, err
	}
	joinPlan := LogicalJoin{JoinType: joinType}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(leftPlan.Schema())
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	for j := 0; j < len(rightPlan.Schema().Columns); j++ {
		leftCol, rightCol := leftPlan.Schema().Columns[j], rightPlan.Schema().Columns[j]
		eqCond, err := expression.NewFunction(b.ctx, ast.NullEQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightCol)
		if err != nil {
			return nil, err
		}
		if leftCol.RetType.Tp != rightCol.RetType.Tp {
			joinPlan.OtherConditions = append(joinPlan.OtherConditions, eqCond)
		} else {
			joinPlan.EqualConditions = append(joinPlan.EqualConditions, eqCond.(*expression.ScalarFunction))
		}
	}
	return joinPlan, nil
}

// buildIntersect build the set operator for 'intersect'. It is called before buildExcept and buildUnion because of its
// higher precedence.
func (b *PlanBuilder) buildIntersect(ctx context.Context, selects []ast.Node) (LogicalPlan, *ast.SetOprType, error) {
	var leftPlan LogicalPlan
	var err error
	var afterSetOperator *ast.SetOprType
	switch x := selects[0].(type) {
	case *ast.SelectStmt:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSelect(ctx, x)
	case *ast.SetOprSelectList:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x})
	}
	if err != nil {
		return nil, nil, err
	}
	if len(selects) == 1 {
		return leftPlan, afterSetOperator, nil
	}

	columnNums := leftPlan.Schema().Len()
	for i := 1; i < len(selects); i++ {
		var rightPlan LogicalPlan
		switch x := selects[i].(type) {
		case *ast.SelectStmt:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSelect(ctx, x)
		case *ast.SetOprSelectList:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x})
		}
		if err != nil {
			return nil, nil, err
		}
		if rightPlan.Schema().Len() != columnNums {
			return nil, nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, SemiJoin)
		if err != nil {
			return nil, nil, err
		}
	}
	return leftPlan, afterSetOperator, nil
}

// buildExcept build the set operators for 'except', and in this function, it calls buildUnion at the same time. Because
// Union and except has the same precedence.
func (b *PlanBuilder) buildExcept(ctx context.Context, selects []LogicalPlan, afterSetOpts []*ast.SetOprType) (LogicalPlan, error) {
	unionPlans := []LogicalPlan{selects[0]}
	tmpAfterSetOpts := []*ast.SetOprType{nil}
	columnNums := selects[0].Schema().Len()
	for i := 1; i < len(selects); i++ {
		rightPlan := selects[i]
		if rightPlan.Schema().Len() != columnNums {
			return nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		if *afterSetOpts[i] == ast.Except {
			leftPlan, err := b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
			if err != nil {
				return nil, err
			}
			leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, AntiSemiJoin)
			if err != nil {
				return nil, err
			}
			unionPlans = []LogicalPlan{leftPlan}
			tmpAfterSetOpts = []*ast.SetOprType{nil}
		} else if *afterSetOpts[i] == ast.ExceptAll {
			// TODO: support except all.
			return nil, errors.Errorf("TiDB do not support except all")
		} else {
			unionPlans = append(unionPlans, rightPlan)
			tmpAfterSetOpts = append(tmpAfterSetOpts, afterSetOpts[i])
		}
	}
	return b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
}

func (b *PlanBuilder) buildUnion(ctx context.Context, selects []LogicalPlan, afterSetOpts []*ast.SetOprType) (LogicalPlan, error) {
	if len(selects) == 1 {
		return selects[0], nil
	}
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(ctx, selects, afterSetOpts)
	if err != nil {
		return nil, err
	}

	unionDistinctPlan, err := b.buildUnionAll(ctx, distinctSelectPlans)
	if err != nil {
		return nil, err
	}
	if unionDistinctPlan != nil {
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			return nil, err
		}
		if len(allSelectPlans) > 0 {
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	unionAllPlan, err := b.buildUnionAll(ctx, allSelectPlans)
	if err != nil {
		return nil, err
	}
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		unionPlan = unionAllPlan
	}

	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref:
//		https://dev.mysql.com/doc/refman/5.7/en/union.html
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (b *PlanBuilder) divideUnionSelectPlans(ctx context.Context, selects []LogicalPlan, setOprTypes []*ast.SetOprType) (distinctSelects []LogicalPlan, allSelects []LogicalPlan, err error) {
	firstUnionAllIdx := 0
	columnNums := selects[0].Schema().Len()
	for i := len(selects) - 1; i > 0; i-- {
		if firstUnionAllIdx == 0 && *setOprTypes[i] != ast.UnionAll {
			firstUnionAllIdx = i + 1
		}
		if selects[i].Schema().Len() != columnNums {
			return nil, nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
	}
	return selects[:firstUnionAllIdx], selects[firstUnionAllIdx:], nil
}

func (b *PlanBuilder) buildUnionAll(ctx context.Context, subPlan []LogicalPlan) (LogicalPlan, error) {
	if len(subPlan) == 0 {
		return nil, nil
	}
	u := LogicalUnionAll{}.Init(b.ctx, b.getSelectOffset())
	u.children = subPlan
	err := b.buildProjection4Union(ctx, u)
	return u, err
}

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct {
}

func (t *itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	return inNode, false
}

func (t *itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (b *PlanBuilder) buildSort(ctx context.Context, p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int) (*LogicalSort, error) {
	return b.buildSortWithCheck(ctx, p, byItems, aggMapper, windowMapper, nil, 0, false)
}

func (b *PlanBuilder) buildSortWithCheck(ctx context.Context, p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int,
	projExprs []expression.Expression, oldLen int, hasDistinct bool) (*LogicalSort, error) {
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		b.curClause = globalOrderByClause
	} else {
		b.curClause = orderByClause
	}
	sort := LogicalSort{}.Init(b.ctx, b.getSelectOffset())
	exprs := make([]*util.ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for i, item := range byItems {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewriteWithPreprocess(ctx, item.Expr, p, aggMapper, windowMapper, true, nil)
		if err != nil {
			return nil, err
		}

		// check whether ORDER BY items show up in SELECT DISTINCT fields, see #12442
		if hasDistinct && projExprs != nil {
			err = b.checkOrderByInDistinct(item, i, it, p, projExprs, oldLen)
			if err != nil {
				return nil, err
			}
		}

		p = np
		exprs = append(exprs, &util.ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// checkOrderByInDistinct checks whether ORDER BY has conflicts with DISTINCT, see #12442
func (b *PlanBuilder) checkOrderByInDistinct(byItem *ast.ByItem, idx int, expr expression.Expression, p LogicalPlan, originalExprs []expression.Expression, length int) error {
	// Check if expressions in ORDER BY whole match some fields in DISTINCT.
	// e.g.
	// select distinct count(a) from t group by b order by count(a);  ✔
	// select distinct a+1 from t order by a+1;                       ✔
	// select distinct a+1 from t order by a+2;                       ✗
	for j := 0; j < length; j++ {
		// both check original expression & as name
		if expr.Equal(b.ctx, originalExprs[j]) || expr.Equal(b.ctx, p.Schema().Columns[j]) {
			return nil
		}
	}

	// Check if referenced columns of expressions in ORDER BY whole match some fields in DISTINCT,
	// both original expression and alias can be referenced.
	// e.g.
	// select distinct a from t order by sin(a);                            ✔
	// select distinct a, b from t order by a+b;                            ✔
	// select distinct count(a), sum(a) from t group by b order by sum(a);  ✔
	cols := expression.ExtractColumns(expr)
CheckReferenced:
	for _, col := range cols {
		for j := 0; j < length; j++ {
			if col.Equal(b.ctx, originalExprs[j]) || col.Equal(b.ctx, p.Schema().Columns[j]) {
				continue CheckReferenced
			}
		}

		// Failed cases
		// e.g.
		// select distinct sin(a) from t order by a;                            ✗
		// select distinct a from t order by a+b;                               ✗
		if _, ok := byItem.Expr.(*ast.AggregateFuncExpr); ok {
			return ErrAggregateInOrderNotSelect.GenWithStackByArgs(idx+1, "DISTINCT")
		}
		// select distinct count(a) from t group by b order by sum(a);          ✗
		return ErrFieldInOrderNotSelect.GenWithStackByArgs(idx+1, col.OrigName, "DISTINCT")
	}
	return nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx sessionctx.Context, n ast.Node) (uVal uint64, isNull bool, isExpectedType bool) {
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		if !v.InExecute {
			return 0, false, true
		}
		param, err := expression.ParamMarkerExpression(ctx, v)
		if err != nil {
			return 0, false, false
		}
		str, isNull, err := expression.GetStringFromConstant(ctx, param)
		if err != nil {
			return 0, false, false
		}
		if isNull {
			return 0, true, true
		}
		val = str
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		sc := ctx.GetSessionVars().StmtCtx
		uVal, err := types.StrToUint(sc, v, false)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

func extractLimitCountOffset(ctx sessionctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		tableDual := LogicalTableDual{RowCount: 0}.Init(b.ctx, b.getSelectOffset())
		tableDual.schema = src.Schema()
		tableDual.names = src.OutputNames()
		return tableDual, nil
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx, b.getSelectOffset())
	if hint := b.TableHints(); hint != nil {
		li.limitHints = hint.limitHints
	}
	li.SetChildren(src)
	return li, nil
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.ColumnName, b *ast.ColumnName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Table.L == "" || a.Table.L == b.Table.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr, ignoreAsName bool) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if f.AsName.L == "" || ignoreAsName {
			if curCol, isCol := f.Expr.(*ast.ColumnNameExpr); isCol {
				return curCol.Name.Name.L == col.Name.Name.L
			} else if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// ColumnNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(f.Text()) == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return f.AsName.L == col.Name.Name.L
	}
	return false
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v, ignoreAsName) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !colMatch(matchedExpr.(*ast.ColumnNameExpr).Name, curCol.Name) &&
				!colMatch(curCol.Name, matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
	prevClause   []clauseCode
}

func (a *havingWindowAndOrderbyExprResolver) pushCurClause(newClause clauseCode) {
	a.prevClause = append(a.prevClause, a.curClause)
	a.curClause = newClause
}

func (a *havingWindowAndOrderbyExprResolver) popCurClause() {
	a.curClause = a.prevClause[len(a.prevClause)-1]
	a.prevClause = a.prevClause[:len(a.prevClause)-1]
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	case *ast.PartitionByClause:
		a.pushCurClause(partitionByClause)
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.pushCurClause(windowOrderByClause)
		}
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p LogicalPlan) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	schemaCols, outputNames := p.Schema().Columns, p.OutputNames()
	if idx < 0 {
		// For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0
		// order by t2.a`, the query plan will be `join->selection->sort`. The
		// schema of selection will be `[t1.a]`, thus we need to recursively
		// retrieve the `t2.a` from the underlying join.
		switch x := p.(type) {
		case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort, *LogicalMaxOneRow:
			return a.resolveFromPlan(v, p.Children()[0])
		case *LogicalJoin:
			if len(x.redundantNames) != 0 {
				idx, err = expression.FindFieldName(x.redundantNames, v.Name)
				schemaCols, outputNames = x.redundantSchema.Columns, x.redundantNames
			}
		}
		if err != nil || idx < 0 {
			return -1, err
		}
	}
	col := schemaCols[idx]
	if col.IsHidden {
		return -1, ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[a.curClause])
	}
	name := outputNames[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(c.Name, newColName) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    model.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.PartitionByClause:
		a.popCurClause()
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.popCurClause()
		}
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.curClause == orderByClause && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && a.curClause != orderByClause {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.curClause == orderByClause {
					index, a.err = a.resolveFromPlan(v, a.p)
				} else if a.curClause == havingClause && v.Name.Table.L != "" {
					// For SQLs like:
					//   select a from t b having b.a;
					index, a.err = a.resolveFromPlan(v, a.p)
					if a.err != nil {
						return node, false
					}
					if index != -1 {
						// For SQLs like:
						//   select a+1 from t having t.a;
						field := a.selectFields[index]
						if field.Auxiliary { // having can't use auxiliary field
							index = -1
						}
					}
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p)
			_ = err
			if index == -1 && a.curClause != fieldList &&
				a.curClause != windowOrderByClause && a.curClause != partitionByClause {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncsInSelectFields(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (b *PlanBuilder) extractAggFuncsInByItems(byItems []*ast.ByItem) []*ast.AggregateFuncExpr {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range byItems {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.AggFuncs
}

// extractCorrelatedAggFuncs extracts correlated aggregates which belong to outer query from aggregate function list.
func (b *PlanBuilder) extractCorrelatedAggFuncs(ctx context.Context, p LogicalPlan, aggFuncs []*ast.AggregateFuncExpr) (outer []*ast.AggregateFuncExpr, err error) {
	corCols := make([]*expression.CorrelatedColumn, 0, len(aggFuncs))
	cols := make([]*expression.Column, 0, len(aggFuncs))
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range aggFuncs {
		for _, arg := range agg.Args {
			expr, _, err := b.rewrite(ctx, arg, p, aggMapper, true)
			if err != nil {
				return nil, err
			}
			corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			cols = append(cols, expression.ExtractColumns(expr)...)
		}
		if len(corCols) > 0 && len(cols) == 0 {
			outer = append(outer, agg)
		}
		aggMapper[agg] = -1
		corCols, cols = corCols[:0], cols[:0]
	}
	return
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	extractor.curClause = fieldList
	for _, field := range sel.Fields.Fields {
		if !ast.HasWindowFlag(field.Expr) {
			continue
		}
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
		field.Expr = n.(ast.ExprNode)
	}
	for _, spec := range sel.WindowSpecs {
		_, ok := spec.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
	}
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if !ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, extractor.err
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

// correlatedAggregateResolver visits Expr tree.
// It finds and collects all correlated aggregates which should be evaluated in the outer query.
type correlatedAggregateResolver struct {
	ctx       context.Context
	err       error
	b         *PlanBuilder
	outerPlan LogicalPlan

	// correlatedAggFuncs stores aggregate functions which belong to outer query
	correlatedAggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (r *correlatedAggregateResolver) Enter(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.SelectStmt:
		if r.outerPlan != nil {
			outerSchema := r.outerPlan.Schema()
			r.b.outerSchemas = append(r.b.outerSchemas, outerSchema)
			r.b.outerNames = append(r.b.outerNames, r.outerPlan.OutputNames())
		}
		r.err = r.resolveSelect(v)
		return n, true
	}
	return n, false
}

// resolveSelect finds and collects correlated aggregates within the SELECT stmt.
// It resolves and builds FROM clause first to get a source plan, from which we can decide
// whether a column is correlated or not.
// Then it collects correlated aggregate from SELECT fields (including sub-queries), HAVING,
// ORDER BY, WHERE & GROUP BY.
// Finally it restore the original SELECT stmt.
func (r *correlatedAggregateResolver) resolveSelect(sel *ast.SelectStmt) (err error) {
	if sel.With != nil {
		l := len(r.b.outerCTEs)
		defer func() {
			r.b.outerCTEs = r.b.outerCTEs[:l]
		}()
		err := r.b.buildWith(r.ctx, sel.With)
		if err != nil {
			return err
		}
	}
	// collect correlated aggregate from sub-queries inside FROM clause.
	_, err = r.collectFromTableRefs(r.ctx, sel.From)
	if err != nil {
		return err
	}
	// we cannot use cache if there are correlated aggregates inside FROM clause,
	// since the plan we are building now is not correct and need to be rebuild later.
	p, err := r.b.buildTableRefs(r.ctx, sel.From)
	if err != nil {
		return err
	}

	// similar to process in PlanBuilder.buildSelect
	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = r.b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return err
	}
	if r.b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}

	hasWindowFuncField := r.b.detectSelectWindow(sel)
	if hasWindowFuncField {
		_, err = r.b.resolveWindowFunction(sel, p)
		if err != nil {
			return err
		}
	}

	_, _, err = r.b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return err
	}

	// find and collect correlated aggregates recursively in sub-queries
	_, err = r.b.resolveCorrelatedAggregates(r.ctx, sel, p)
	if err != nil {
		return err
	}

	// collect from SELECT fields, HAVING, ORDER BY and window functions
	if r.b.detectSelectAgg(sel) {
		err = r.collectFromSelectFields(p, sel.Fields.Fields)
		if err != nil {
			return err
		}
	}

	// collect from WHERE
	err = r.collectFromWhere(p, sel.Where)
	if err != nil {
		return err
	}

	// collect from GROUP BY
	err = r.collectFromGroupBy(p, sel.GroupBy)
	if err != nil {
		return err
	}

	// restore the sub-query
	sel.Fields.Fields = originalFields
	r.b.handleHelper.popMap()
	return nil
}

func (r *correlatedAggregateResolver) collectFromTableRefs(ctx context.Context, from *ast.TableRefsClause) (canCache bool, err error) {
	if from == nil {
		return true, nil
	}
	subResolver := &correlatedAggregateResolver{
		ctx: r.ctx,
		b:   r.b,
	}
	_, ok := from.TableRefs.Accept(subResolver)
	if !ok {
		return false, subResolver.err
	}
	if len(subResolver.correlatedAggFuncs) == 0 {
		return true, nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, subResolver.correlatedAggFuncs...)
	return false, nil
}

func (r *correlatedAggregateResolver) collectFromSelectFields(p LogicalPlan, fields []*ast.SelectField) error {
	aggList, _ := r.b.extractAggFuncsInSelectFields(fields)
	r.b.curClause = fieldList
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromGroupBy(p LogicalPlan, groupBy *ast.GroupByClause) error {
	if groupBy == nil {
		return nil
	}
	aggList := r.b.extractAggFuncsInByItems(groupBy.Items)
	r.b.curClause = groupByClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromWhere(p LogicalPlan, where ast.ExprNode) error {
	if where == nil {
		return nil
	}
	extractor := &AggregateFuncExtractor{skipAggMap: r.b.correlatedAggMapper}
	_, _ = where.Accept(extractor)
	r.b.curClause = whereClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, extractor.AggFuncs)
	if err != nil {
		return err
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

// Leave implements Visitor interface.
func (r *correlatedAggregateResolver) Leave(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt:
		if r.outerPlan != nil {
			r.b.outerSchemas = r.b.outerSchemas[0 : len(r.b.outerSchemas)-1]
			r.b.outerNames = r.b.outerNames[0 : len(r.b.outerNames)-1]
		}
	}
	return n, r.err == nil
}

// resolveCorrelatedAggregates finds and collects all correlated aggregates which should be evaluated
// in the outer query from all the sub-queries inside SELECT fields.
func (b *PlanBuilder) resolveCorrelatedAggregates(ctx context.Context, sel *ast.SelectStmt, p LogicalPlan) (map[*ast.AggregateFuncExpr]int, error) {
	resolver := &correlatedAggregateResolver{
		ctx:       ctx,
		b:         b,
		outerPlan: p,
	}
	correlatedAggList := make([]*ast.AggregateFuncExpr, 0)
	for _, field := range sel.Fields.Fields {
		_, ok := field.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.Having != nil {
		_, ok := sel.Having.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			_, ok := item.Expr.Accept(resolver)
			if !ok {
				return nil, resolver.err
			}
			correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
		}
	}
	correlatedAggMap := make(map[*ast.AggregateFuncExpr]int)
	for _, aggFunc := range correlatedAggList {
		correlatedAggMap[aggFunc] = len(sel.Fields.Fields)
		sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
			Auxiliary: true,
			Expr:      aggFunc,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_subq_agg_%d", len(sel.Fields.Fields))),
		})
	}
	return correlatedAggMap, nil
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx        sessionctx.Context
	fields     []*ast.SelectField
	schema     *expression.Schema
	names      []*types.FieldName
	err        error
	inExpr     bool
	isParam    bool
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn

	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	g.exprDepth++
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *driver.ParamMarkerExpr:
		g.isParam = true
		if g.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(g.ctx, n)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
		return n, true
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	extractor := &AggregateFuncExtractor{skipAggMap: g.skipAggMap}
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		idx, err := expression.FindFieldName(g.names, v.Name)
		if idx < 0 || !g.inExpr {
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				return inNode, false
			}
			if idx >= 0 {
				return inNode, true
			}
			if index != -1 {
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else if ast.HasWindowFlag(ret) {
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to window function")
				} else {
					return ret, true
				}
			}
			g.err = err
			return inNode, false
		}
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx, v)
		if err != nil {
			g.err = ErrUnknown.GenWithStackByArgs()
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 || ast.HasWindowFlag(ret) {
			fieldName := g.fields[pos-1].AsName.String()
			if fieldName == "" {
				fieldName = g.fields[pos-1].Text()
			}
			g.err = ErrWrongGroupField.GenWithStackByArgs(fieldName)
			return inNode, false
		}
		return ret, true
	case *ast.ValuesExpr:
		if v.Column == nil {
			g.err = ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func tblInfoFromCol(from ast.ResultSetNode, name *types.FieldName) *model.TableInfo {
	var tableList []*ast.TableName
	tableList = extractTableList(from, tableList, true)
	for _, field := range tableList {
		if field.Name.L == name.TblName.L {
			return field.TableInfo
		}
	}
	return nil
}

func buildFuncDependCol(p LogicalPlan, cond ast.ExprNode) (*types.FieldName, *types.FieldName, error) {
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, nil, nil
	}
	if binOpExpr.Op != opcode.EQ {
		return nil, nil, nil
	}
	lColExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	rColExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	lIdx, err := expression.FindFieldName(p.OutputNames(), lColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	rIdx, err := expression.FindFieldName(p.OutputNames(), rColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	if lIdx == -1 {
		return nil, nil, ErrUnknownColumn.GenWithStackByArgs(lColExpr.Name, "where clause")
	}
	if rIdx == -1 {
		return nil, nil, ErrUnknownColumn.GenWithStackByArgs(rColExpr.Name, "where clause")
	}
	return p.OutputNames()[lIdx], p.OutputNames()[rIdx], nil
}

func buildWhereFuncDepend(p LogicalPlan, where ast.ExprNode) (map[*types.FieldName]*types.FieldName, error) {
	whereConditions := splitWhere(where)
	colDependMap := make(map[*types.FieldName]*types.FieldName, 2*len(whereConditions))
	for _, cond := range whereConditions {
		lCol, rCol, err := buildFuncDependCol(p, cond)
		if err != nil {
			return nil, err
		}
		if lCol == nil || rCol == nil {
			continue
		}
		colDependMap[lCol] = rCol
		colDependMap[rCol] = lCol
	}
	return colDependMap, nil
}

func buildJoinFuncDepend(p LogicalPlan, from ast.ResultSetNode) (map[*types.FieldName]*types.FieldName, error) {
	switch x := from.(type) {
	case *ast.Join:
		if x.On == nil {
			return nil, nil
		}
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*types.FieldName]*types.FieldName, len(onConditions))
		for _, cond := range onConditions {
			lCol, rCol, err := buildFuncDependCol(p, cond)
			if err != nil {
				return nil, err
			}
			if lCol == nil || rCol == nil {
				continue
			}
			lTbl := tblInfoFromCol(x.Left, lCol)
			if lTbl == nil {
				lCol, rCol = rCol, lCol
			}
			switch x.Tp {
			case ast.CrossJoin:
				colDependMap[lCol] = rCol
				colDependMap[rCol] = lCol
			case ast.LeftJoin:
				colDependMap[rCol] = lCol
			case ast.RightJoin:
				colDependMap[lCol] = rCol
			}
		}
		return colDependMap, nil
	default:
		return nil, nil
	}
}

func checkColFuncDepend(
	p LogicalPlan,
	name *types.FieldName,
	tblInfo *model.TableInfo,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	whereDependNames, joinDependNames map[*types.FieldName]*types.FieldName,
) bool {
	for _, index := range tblInfo.Indices {
		if !index.Unique {
			continue
		}
		funcDepend := true
		for _, indexCol := range index.Columns {
			iColInfo := tblInfo.Columns[indexCol.Offset]
			if !mysql.HasNotNullFlag(iColInfo.Flag) {
				funcDepend = false
				break
			}
			cn := &ast.ColumnName{
				Schema: name.DBName,
				Table:  name.TblName,
				Name:   iColInfo.Name,
			}
			iIdx, err := expression.FindFieldName(p.OutputNames(), cn)
			if err != nil || iIdx < 0 {
				funcDepend = false
				break
			}
			iName := p.OutputNames()[iIdx]
			if _, ok := gbyOrSingleValueColNames[iName]; ok {
				continue
			}
			if wCol, ok := whereDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[wCol]; ok {
					continue
				}
			}
			if jCol, ok := joinDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[jCol]; ok {
					continue
				}
			}
			funcDepend = false
			break
		}
		if funcDepend {
			return true
		}
	}
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.Columns {
		if !mysql.HasPriKeyFlag(colInfo.Flag) {
			continue
		}
		hasPrimaryField = true
		pkName := &ast.ColumnName{
			Schema: name.DBName,
			Table:  name.TblName,
			Name:   colInfo.Name,
		}
		pIdx, err := expression.FindFieldName(p.OutputNames(), pkName)
		if err != nil || pIdx < 0 {
			primaryFuncDepend = false
			break
		}
		pCol := p.OutputNames()[pIdx]
		if _, ok := gbyOrSingleValueColNames[pCol]; ok {
			continue
		}
		if wCol, ok := whereDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[wCol]; ok {
				continue
			}
		}
		if jCol, ok := joinDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[jCol]; ok {
				continue
			}
		}
		primaryFuncDepend = false
		break
	}
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupByOrIsSingleValue(
	p LogicalPlan,
	expr ast.ExprNode,
	offset int,
	loc string,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	gbyExprs []ast.ExprNode,
	notInGbyOrSingleValueColNames map[*types.FieldName]ErrExprLoc,
) {
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		return
	}
	if _, ok := expr.(*ast.ColumnNameExpr); !ok {
		for _, gbyExpr := range gbyExprs {
			if ast.ExpressionDeepEqual(gbyExpr, expr) {
				return
			}
		}
	}
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.AnyValue {
			return
		}
	}
	colMap := make(map[*types.FieldName]struct{}, len(p.Schema().Columns))
	allColFromExprNode(p, expr, colMap)
	for col := range colMap {
		if _, ok := gbyOrSingleValueColNames[col]; !ok {
			notInGbyOrSingleValueColNames[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupBy(p LogicalPlan, sel *ast.SelectStmt) (err error) {
	if sel.GroupBy != nil {
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel)
	}
	return err
}

func addGbyOrSingleValueColName(p LogicalPlan, colName *ast.ColumnName, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	idx, err := expression.FindFieldName(p.OutputNames(), colName)
	if err != nil || idx < 0 {
		return
	}
	gbyOrSingleValueColNames[p.OutputNames()[idx]] = struct{}{}
}

func extractSingeValueColNamesFromWhere(p LogicalPlan, where ast.ExprNode, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	whereConditions := splitWhere(where)
	for _, cond := range whereConditions {
		binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
		if !ok || binOpExpr.Op != opcode.EQ {
			continue
		}
		if colExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr); ok {
			if _, ok := binOpExpr.R.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
		} else if colExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr); ok {
			if _, ok := binOpExpr.L.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupByWithGroupClause(p LogicalPlan, sel *ast.SelectStmt) error {
	gbyOrSingleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	for _, byItem := range sel.GroupBy.Items {
		expr := getInnerFromParenthesesAndUnaryPlus(byItem.Expr)
		if colExpr, ok := expr.(*ast.ColumnNameExpr); ok {
			addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
		} else {
			gbyExprs = append(gbyExprs, expr)
		}
	}
	// MySQL permits a nonaggregate column not named in a GROUP BY clause when ONLY_FULL_GROUP_BY SQL mode is enabled,
	// provided that this column is limited to a single value.
	// See https://dev.mysql.com/doc/refman/5.7/en/group-by-handling.html for details.
	extractSingeValueColNamesFromWhere(p, sel.Where, gbyOrSingleValueColNames)

	notInGbyOrSingleValueColNames := make(map[*types.FieldName]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(field.Expr), offset, ErrExprInSelect, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
	}

	if sel.OrderBy != nil {
		for offset, item := range sel.OrderBy.Items {
			if colName, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				index, err := resolveFromSelectFields(colName, sel.Fields.Fields, false)
				if err != nil {
					return err
				}
				// If the ByItem is in fields list, it has been checked already in above.
				if index >= 0 {
					continue
				}
			}
			checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(item.Expr), offset, ErrExprInOrderBy, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
		}
	}
	if len(notInGbyOrSingleValueColNames) == 0 {
		return nil
	}

	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}
	joinDepends, err := buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyOrSingleValueColNames))
	for name, errExprLoc := range notInGbyOrSingleValueColNames {
		tblInfo := tblInfoFromCol(sel.From.TableRefs, name)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, name, tblInfo, gbyOrSingleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, name.DBName.O+"."+name.TblName.O+"."+name.OrigColName.O)
		case ErrExprInOrderBy:
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return nil
	}
	return nil
}

func (b *PlanBuilder) checkOnlyFullGroupByWithOutGroupClause(p LogicalPlan, sel *ast.SelectStmt) error {
	resolver := colResolverForOnlyFullGroupBy{
		firstOrderByAggColIdx: -1,
	}
	resolver.curClause = fieldList
	for idx, field := range sel.Fields.Fields {
		resolver.exprIdx = idx
		field.Accept(&resolver)
	}
	if len(resolver.nonAggCols) > 0 {
		if sel.Having != nil {
			sel.Having.Expr.Accept(&resolver)
		}
		if sel.OrderBy != nil {
			resolver.curClause = orderByClause
			for idx, byItem := range sel.OrderBy.Items {
				resolver.exprIdx = idx
				byItem.Expr.Accept(&resolver)
			}
		}
	}
	if resolver.firstOrderByAggColIdx != -1 && len(resolver.nonAggCols) > 0 {
		// SQL like `select a from t where a = 1 order by count(b)` is illegal.
		return ErrAggregateOrderNonAggQuery.GenWithStackByArgs(resolver.firstOrderByAggColIdx + 1)
	}
	if !resolver.hasAggFuncOrAnyValue || len(resolver.nonAggCols) == 0 {
		return nil
	}
	singleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	extractSingeValueColNamesFromWhere(p, sel.Where, singleValueColNames)
	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}

	joinDepends, err := buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(resolver.nonAggCols))
	for i, colName := range resolver.nonAggCols {
		idx, err := expression.FindFieldName(p.OutputNames(), colName)
		if err != nil || idx < 0 {
			return ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
		}
		fieldName := p.OutputNames()[idx]
		if _, ok := singleValueColNames[fieldName]; ok {
			continue
		}
		tblInfo := tblInfoFromCol(sel.From.TableRefs, fieldName)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, fieldName, tblInfo, singleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		return ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
	}
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	nonAggCols            []*ast.ColumnName
	exprIdx               int
	nonAggColIdxs         []int
	hasAggFuncOrAnyValue  bool
	firstOrderByAggColIdx int
	curClause             clauseCode
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		c.hasAggFuncOrAnyValue = true
		if c.curClause == orderByClause {
			c.firstOrderByAggColIdx = c.exprIdx
		}
		return node, true
	case *ast.FuncCallExpr:
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.ColumnNameExpr:
		c.nonAggCols = append(c.nonAggCols, t.Name)
		c.nonAggColIdxs = append(c.nonAggColIdxs, c.exprIdx)
		return node, true
	case *ast.SubqueryExpr:
		return node, true
	}
	return node, false
}

func (c *colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

type colNameResolver struct {
	p     LogicalPlan
	names map[*types.FieldName]struct{}
}

func (c *colNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.ColumnNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		return inNode, true
	}
	return inNode, false
}

func (c *colNameResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		idx, err := expression.FindFieldName(c.p.OutputNames(), v.Name)
		if err == nil && idx >= 0 {
			c.names[c.p.OutputNames()[idx]] = struct{}{}
		}
	}
	return inNode, true
}

func allColFromExprNode(p LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &colNameResolver{
		p:     p,
		names: names,
	}
	n.Accept(extractor)
}

func (b *PlanBuilder) resolveGbyExprs(ctx context.Context, p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression, error) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:        b.ctx,
		fields:     fields,
		schema:     p.Schema(),
		names:      p.OutputNames(),
		skipAggMap: b.correlatedAggMapper,
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		resolver.exprDepth = 0
		resolver.isParam = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return nil, nil, errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(ctx, itemExpr, p, nil, true)
		if err != nil {
			return nil, nil, err
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, nil
}

func (b *PlanBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	join, isJoin := p.(*LogicalJoin)
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, ErrInvalidWildCard
		}
		list := unfoldWildStar(field, p.OutputNames(), p.Schema().Columns)
		// For sql like `select t1.*, t2.* from t1 join t2 using(a)`, we should
		// not coalesce the `t2.a` in the output result. Thus we need to unfold
		// the wildstar from the underlying join.redundantSchema.
		if isJoin && join.redundantSchema != nil && field.WildCard.Table.L != "" {
			redundantList := unfoldWildStar(field, join.redundantNames, join.redundantSchema.Columns)
			if len(redundantList) > len(list) {
				list = redundantList
			}
		}
		if len(list) == 0 {
			return nil, ErrBadTable.GenWithStackByArgs(field.WildCard.Table)
		}
		resultList = append(resultList, list...)
	}
	return resultList, nil
}

func unfoldWildStar(field *ast.SelectField, outputName types.NameSlice, column []*expression.Column) (resultList []*ast.SelectField) {
	dbName := field.WildCard.Schema
	tblName := field.WildCard.Table
	for i, name := range outputName {
		col := column[i]
		if col.IsHidden {
			continue
		}
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			col.ID != model.ExtraHandleID {
			colName := &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Schema: name.DBName,
					Table:  name.TblName,
					Name:   name.ColName,
				}}
			colName.SetType(col.GetType())
			field := &ast.SelectField{Expr: colName}
			field.SetText(name.ColName.O)
			resultList = append(resultList, field)
		}
	}
	return resultList
}

func (b *PlanBuilder) addAliasName(selectFields []*ast.SelectField, p LogicalPlan) (resultList []*ast.SelectField, err error) {
	if len(selectFields) != len(p.OutputNames()) {
		return nil, errors.Errorf("lengths of selectFields and OutputNames are not equal(%d, %d)",
			len(selectFields), len(p.OutputNames()))
	}
	for i, field := range selectFields {
		newField := *field
		if newField.AsName.L == "" {
			newField.AsName = p.OutputNames()[i].ColName
		}
		resultList = append(resultList, &newField)
	}
	return resultList, nil
}

func (b *PlanBuilder) pushHintWithoutTableWarning(hint *ast.TableOptimizerHint) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	if err := hint.Restore(ctx); err != nil {
		return
	}
	errMsg := fmt.Sprintf("Hint %s is inapplicable. Please specify the table names in the arguments.", sb.String())
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

func (b *PlanBuilder) pushTableHints(hints []*ast.TableOptimizerHint, currentLevel int) {
	hints = b.hintProcessor.GetCurrentStmtHints(hints, currentLevel)
	var (
		sortMergeTables, INLJTables, INLHJTables, INLMJTables, hashJoinTables, BCTables, BCJPreferLocalTables []hintTableInfo
		indexHintList, indexMergeHintList                                                                     []indexHintInfo
		tiflashTables, tikvTables                                                                             []hintTableInfo
		aggHints                                                                                              aggHintInfo
		timeRangeHint                                                                                         ast.HintTimeRange
		limitHints                                                                                            limitHintInfo
	)
	for _, hint := range hints {
		// Set warning for the hint that requires the table name.
		switch hint.HintName.L {
		case TiDBMergeJoin, HintSMJ, TiDBIndexNestedLoopJoin, HintINLJ, HintINLHJ, HintINLMJ,
			TiDBHashJoin, HintHJ, HintUseIndex, HintIgnoreIndex, HintForceIndex, HintIndexMerge:
			if len(hint.Tables) == 0 {
				b.pushHintWithoutTableWarning(hint)
				continue
			}
		}

		switch hint.HintName.L {
		case TiDBMergeJoin, HintSMJ:
			sortMergeTables = append(sortMergeTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case TiDBBroadCastJoin, HintBCJ:
			BCTables = append(BCTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case HintBCJPreferLocal:
			BCJPreferLocalTables = append(BCJPreferLocalTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case TiDBIndexNestedLoopJoin, HintINLJ:
			INLJTables = append(INLJTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case HintINLHJ:
			INLHJTables = append(INLHJTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case HintINLMJ:
			INLMJTables = append(INLMJTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case TiDBHashJoin, HintHJ:
			hashJoinTables = append(hashJoinTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
		case HintHashAgg:
			aggHints.preferAggType |= preferHashAgg
		case HintStreamAgg:
			aggHints.preferAggType |= preferStreamAgg
		case HintAggToCop:
			aggHints.preferAggToCop = true
		case HintUseIndex:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
			}
			indexHintList = append(indexHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Tables[0].TableName,
				partitions: hint.Tables[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   ast.HintUse,
					HintScope:  ast.HintForScan,
				},
			})
		case HintIgnoreIndex:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
			}
			indexHintList = append(indexHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Tables[0].TableName,
				partitions: hint.Tables[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   ast.HintIgnore,
					HintScope:  ast.HintForScan,
				},
			})
		case HintForceIndex:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
			}
			indexHintList = append(indexHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Tables[0].TableName,
				partitions: hint.Tables[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   ast.HintForce,
					HintScope:  ast.HintForScan,
				},
			})
		case HintReadFromStorage:
			switch hint.HintData.(model.CIStr).L {
			case HintTiFlash:
				tiflashTables = append(tiflashTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
			case HintTiKV:
				tikvTables = append(tikvTables, tableNames2HintTableInfo(b.ctx, hint.HintName.L, hint.Tables, b.hintProcessor, currentLevel)...)
			}
		case HintIndexMerge:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
			}
			indexMergeHintList = append(indexMergeHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Tables[0].TableName,
				partitions: hint.Tables[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   ast.HintUse,
					HintScope:  ast.HintForScan,
				},
			})
		case HintTimeRange:
			timeRangeHint = hint.HintData.(ast.HintTimeRange)
		case HintLimitToCop:
			limitHints.preferLimitToCop = true
		default:
			// ignore hints that not implemented
		}
	}
	b.tableHintInfo = append(b.tableHintInfo, tableHintInfo{
		sortMergeJoinTables:         sortMergeTables,
		broadcastJoinTables:         BCTables,
		broadcastJoinPreferredLocal: BCJPreferLocalTables,
		indexNestedLoopJoinTables:   indexNestedLoopJoinTables{INLJTables, INLHJTables, INLMJTables},
		hashJoinTables:              hashJoinTables,
		indexHintList:               indexHintList,
		tiflashTables:               tiflashTables,
		tikvTables:                  tikvTables,
		aggHints:                    aggHints,
		indexMergeHintList:          indexMergeHintList,
		timeRangeHint:               timeRangeHint,
		limitHints:                  limitHints,
	})
}

func (b *PlanBuilder) popVisitInfo() {
	if len(b.visitInfo) == 0 {
		return
	}
	b.visitInfo = b.visitInfo[:len(b.visitInfo)-1]
}

func (b *PlanBuilder) popTableHints() {
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	b.appendUnmatchedIndexHintWarning(hintInfo.indexHintList, false)
	b.appendUnmatchedIndexHintWarning(hintInfo.indexMergeHintList, true)
	b.appendUnmatchedJoinHintWarning(HintINLJ, TiDBIndexNestedLoopJoin, hintInfo.indexNestedLoopJoinTables.inljTables)
	b.appendUnmatchedJoinHintWarning(HintINLHJ, "", hintInfo.indexNestedLoopJoinTables.inlhjTables)
	b.appendUnmatchedJoinHintWarning(HintINLMJ, "", hintInfo.indexNestedLoopJoinTables.inlmjTables)
	b.appendUnmatchedJoinHintWarning(HintSMJ, TiDBMergeJoin, hintInfo.sortMergeJoinTables)
	b.appendUnmatchedJoinHintWarning(HintBCJ, TiDBBroadCastJoin, hintInfo.broadcastJoinTables)
	b.appendUnmatchedJoinHintWarning(HintBCJPreferLocal, "", hintInfo.broadcastJoinPreferredLocal)
	b.appendUnmatchedJoinHintWarning(HintHJ, TiDBHashJoin, hintInfo.hashJoinTables)
	b.appendUnmatchedStorageHintWarning(hintInfo.tiflashTables, hintInfo.tikvTables)
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

func (b *PlanBuilder) appendUnmatchedIndexHintWarning(indexHints []indexHintInfo, usedForIndexMerge bool) {
	for _, hint := range indexHints {
		if !hint.matched {
			var hintTypeString string
			if usedForIndexMerge {
				hintTypeString = "use_index_merge"
			} else {
				hintTypeString = hint.hintTypeString()
			}
			errMsg := fmt.Sprintf("%s(%s) is inapplicable, check whether the table(%s.%s) exists",
				hintTypeString,
				hint.indexString(),
				hint.dbName,
				hint.tblName,
			)
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
		}
	}
}

func (b *PlanBuilder) appendUnmatchedJoinHintWarning(joinType string, joinTypeAlias string, hintTables []hintTableInfo) {
	unMatchedTables := extractUnmatchedTables(hintTables)
	if len(unMatchedTables) == 0 {
		return
	}
	if len(joinTypeAlias) != 0 {
		joinTypeAlias = fmt.Sprintf(" or %s", restore2JoinHint(joinTypeAlias, hintTables))
	}

	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s%s. Maybe you can use the table alias name",
		strings.Join(unMatchedTables, ", "), restore2JoinHint(joinType, hintTables), joinTypeAlias)
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

func (b *PlanBuilder) appendUnmatchedStorageHintWarning(tiflashTables, tikvTables []hintTableInfo) {
	unMatchedTiFlashTables := extractUnmatchedTables(tiflashTables)
	unMatchedTiKVTables := extractUnmatchedTables(tikvTables)
	if len(unMatchedTiFlashTables)+len(unMatchedTiKVTables) == 0 {
		return
	}
	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s. Maybe you can use the table alias name",
		strings.Join(append(unMatchedTiFlashTables, unMatchedTiKVTables...), ", "),
		restore2StorageHint(tiflashTables, tikvTables))
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

// TableHints returns the *tableHintInfo of PlanBuilder.
func (b *PlanBuilder) TableHints() *tableHintInfo {
	if len(b.tableHintInfo) == 0 {
		return nil
	}
	return &(b.tableHintInfo[len(b.tableHintInfo)-1])
}

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p LogicalPlan, err error) {
	b.pushSelectOffset(sel.QueryBlockOffset)
	b.pushTableHints(sel.TableHints, sel.QueryBlockOffset)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current SELECT statement.
		b.popTableHints()
	}()
	if b.buildingRecursivePartForCTE {
		if sel.Distinct || sel.OrderBy != nil || sel.Limit != nil {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("ORDER BY / LIMIT / SELECT DISTINCT in recursive query block of Common Table Expression")
		}
		if sel.GroupBy != nil {
			return nil, ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}
	}
	enableNoopFuncs := b.ctx.GetSessionVars().EnableNoopFuncs
	if sel.SelectStmtOpts != nil {
		if sel.SelectStmtOpts.CalcFoundRows && !enableNoopFuncs {
			err = expression.ErrFunctionsNoopImpl.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
			return nil, err
		}
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		windowAggMap                  map[*ast.AggregateFuncExpr]int
		correlatedAggMap              map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
		projExprs                     []expression.Expression
	)

	// set for update read to true before building result set node
	if isForUpdateReadSelectLock(sel.LockInfo) {
		b.isForUpdateRead = true
	}

	if sel.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		err = b.buildWith(ctx, sel.With)
		if err != nil {
			return nil, err
		}
	}

	// For sub-queries, the FROM clause may have already been built in outer query when resolving correlated aggregates.
	// If the ResultSetNode inside FROM clause has nothing to do with correlated aggregates, we can simply get the
	// existing ResultSetNode from the cache.
	p, err = b.buildTableRefs(ctx, sel.From)
	if err != nil {
		return nil, err
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}
	if b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}

	if sel.GroupBy != nil {
		p, gbyCols, err = b.resolveGbyExprs(ctx, p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, err
		}
	}

	if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() && sel.From != nil {
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			return nil, err
		}
	}

	hasWindowFuncField := b.detectSelectWindow(sel)
	// Some SQL statements define WINDOW but do not use them. But we also need to check the window specification list.
	// For example: select id from t group by id WINDOW w AS (ORDER BY uids DESC) ORDER BY id;
	// We don't use the WINDOW w, but if the 'uids' column is not in the table t, we still need to report an error.
	if hasWindowFuncField || sel.WindowSpecs != nil {
		if b.buildingRecursivePartForCTE {
			return nil, ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}

		windowAggMap, err = b.resolveWindowFunction(sel, p)
		if err != nil {
			return nil, err
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return nil, err
	}

	// We have to resolve correlated aggregate inside sub-queries before building aggregation and building projection,
	// for instance, count(a) inside the sub-query of "select (select count(a)) from t" should be evaluated within
	// the context of the outer query. So we have to extract such aggregates from sub-queries and put them into
	// SELECT field list.
	correlatedAggMap, err = b.resolveCorrelatedAggregates(ctx, sel, p)
	if err != nil {
		return nil, err
	}

	// b.allNames will be used in evalDefaultExpr(). Default function is special because it needs to find the
	// corresponding column name, but does not need the value in the column.
	// For example, select a from t order by default(b), the column b will not be in select fields. Also because
	// buildSort is after buildProjection, so we need get OutputNames before BuildProjection and store in allNames.
	// Otherwise, we will get select fields instead of all OutputNames, so that we can't find the column b in the
	// above example.
	b.allNames = append(b.allNames, p.OutputNames())
	defer func() { b.allNames = b.allNames[:len(b.allNames)-1] }()

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone {
		if sel.LockInfo.LockType == ast.SelectLockForShare && !enableNoopFuncs {
			err = expression.ErrFunctionsNoopImpl.GenWithStackByArgs("LOCK IN SHARE MODE")
			return nil, err
		}
		p, err = b.buildSelectLock(p, sel.LockInfo)
		if err != nil {
			return nil, err
		}
	}
	b.handleHelper.popMap()
	b.handleHelper.pushMap(nil)

	hasAgg := b.detectSelectAgg(sel)
	needBuildAgg := hasAgg
	if hasAgg {
		if b.buildingRecursivePartForCTE {
			return nil, ErrCTERecursiveForbidsAggregation.GenWithStackByArgs(b.genCTETableNameForError())
		}

		aggFuncs, totalMap = b.extractAggFuncsInSelectFields(sel.Fields.Fields)
		// len(aggFuncs) == 0 and sel.GroupBy == nil indicates that all the aggregate functions inside the SELECT fields
		// are actually correlated aggregates from the outer query, which have already been built in the outer query.
		// The only thing we need to do is to find them from b.correlatedAggMap in buildProjection.
		if len(aggFuncs) == 0 && sel.GroupBy == nil {
			needBuildAgg = false
		}
	}
	if needBuildAgg {
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(ctx, p, aggFuncs, gbyCols, correlatedAggMap)
		if err != nil {
			return nil, err
		}
		for agg, idx := range totalMap {
			totalMap[agg] = aggIndexMap[idx]
		}
	}

	var oldLen int
	// According to https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html,
	// we can only process window functions after having clause, so `considerWindow` is false now.
	p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, totalMap, nil, false, sel.OrderBy != nil)
	if err != nil {
		return nil, err
	}

	if b.capFlag&canExpandAST != 0 {
		// To be compabitle with MySQL, we add alias name for each select field when creating view.
		// This function assumes one to one mapping between sel.Fields.Fields and p.OutputNames().
		// So we do this step right after Projection is built.
		sel.Fields.Fields, err = b.addAliasName(sel.Fields.Fields, p)
		if err != nil {
			return nil, err
		}
		originalFields = sel.Fields.Fields
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, err
		}
	}

	b.windowSpecs, err = buildWindowSpecs(sel.WindowSpecs)
	if err != nil {
		return nil, err
	}

	var windowMapper map[*ast.WindowFuncExpr]int
	if hasWindowFuncField || sel.WindowSpecs != nil {
		windowFuncs := extractWindowFuncs(sel.Fields.Fields)
		// we need to check the func args first before we check the window spec
		err := b.checkWindowFuncArgs(ctx, p, windowFuncs, windowAggMap)
		if err != nil {
			return nil, err
		}
		groupedFuncs, orderedSpec, err := b.groupWindowFuncs(windowFuncs)
		if err != nil {
			return nil, err
		}
		p, windowMapper, err = b.buildWindowFunctions(ctx, p, groupedFuncs, orderedSpec, windowAggMap)
		if err != nil {
			return nil, err
		}
		// `hasWindowFuncField == false` means there's only unused named window specs without window functions.
		// In such case plan `p` is not changed, so we don't have to build another projection.
		if hasWindowFuncField {
			// Now we build the window function fields.
			p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, windowAggMap, windowMapper, true, false)
			if err != nil {
				return nil, err
			}
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
			p, err = b.buildSortWithCheck(ctx, p, sel.OrderBy.Items, orderMap, windowMapper, projExprs, oldLen, sel.Distinct)
		} else {
			p, err = b.buildSort(ctx, p, sel.OrderBy.Items, orderMap, windowMapper)
		}
		if err != nil {
			return nil, err
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.names = p.OutputNames()[:oldLen]
		proj.SetSchema(schema)
		return proj, nil
	}

	return p, nil
}

func (b *PlanBuilder) buildTableDual() *LogicalTableDual {
	b.handleHelper.pushMap(nil)
	return LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag = mysql.NotNullFlag | mysql.PriKeyFlag
	return &expression.Column{
		RetType:  tp,
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraHandleName),
	}
}

// addExtraPIDColumn add an extra PID column for partition table.
// 'select ... for update' on a partition table need to know the partition ID
// to construct the lock key, so this column is added to the chunk row.
func (ds *DataSource) addExtraPIDColumn(info *extraPIDInfo) {
	pidCol := &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPidColID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraPartitionIdName),
	}

	ds.Columns = append(ds.Columns, model.NewExtraPartitionIDColInfo())
	schema := ds.Schema()
	schema.Append(pidCol)
	ds.names = append(ds.names, &types.FieldName{
		DBName:      ds.DBName,
		TblName:     ds.TableInfo().Name,
		ColName:     model.ExtraPartitionIdName,
		OrigColName: model.ExtraPartitionIdName,
	})
	ds.TblCols = append(ds.TblCols, pidCol)

	info.Columns = append(info.Columns, pidCol)
	info.TblIDs = append(info.TblIDs, ds.TableInfo().ID)
}

var (
	pseudoEstimationNotAvailable = metrics.PseudoEstimation.WithLabelValues("nodata")
	pseudoEstimationOutdate      = metrics.PseudoEstimation.WithLabelValues("outdate")
)

// getStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
func getStatsTable(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	statsHandle := domain.GetDomain(ctx).StatsHandle()

	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoTable(tblInfo)
	}

	var statsTbl *statistics.Table
	if pid == tblInfo.ID || ctx.GetSessionVars().UseDynamicPartitionPrune() {
		statsTbl = statsHandle.GetTableStats(tblInfo)
	} else {
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	}

	// 2. table row count from statistics is zero.
	if statsTbl.Count == 0 {
		pseudoEstimationNotAvailable.Inc()
		return statistics.PseudoTable(tblInfo)
	}

	// 3. statistics is outdated.
	if statsTbl.IsOutdated() {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		pseudoEstimationOutdate.Inc()
	}
	return statsTbl
}

func (b *PlanBuilder) tryBuildCTE(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (LogicalPlan, error) {
	for i := len(b.outerCTEs) - 1; i >= 0; i-- {
		cte := b.outerCTEs[i]
		if cte.def.Name.L == tn.Name.L {
			if cte.isBuilding {
				if cte.nonRecursive {
					// Can't see this CTE, try outer definition.
					continue
				}

				// Building the recursive part.
				cte.useRecursive = true
				if cte.seedLP == nil {
					return nil, ErrCTERecursiveRequiresNonRecursiveFirst.FastGenByArgs(tn.Name.String())
				}

				if cte.enterSubquery || cte.recursiveRef {
					return nil, ErrInvalidRequiresSingleReference.FastGenByArgs(tn.Name.String())
				}

				cte.recursiveRef = true
				p := LogicalCTETable{name: cte.def.Name.String(), idForStorage: cte.storageID}.Init(b.ctx, b.getSelectOffset())
				p.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))
				p.SetOutputNames(cte.seedLP.OutputNames())
				return p, nil
			}

			b.handleHelper.pushMap(nil)

			hasLimit := false
			limitBeg := uint64(0)
			limitEnd := uint64(0)
			if cte.limitLP != nil {
				hasLimit = true
				switch x := cte.limitLP.(type) {
				case *LogicalLimit:
					limitBeg = x.Offset
					limitEnd = x.Offset + x.Count
				case *LogicalTableDual:
					// Beg and End will both be 0.
				default:
					return nil, errors.Errorf("invalid type for limit plan: %v", cte.limitLP)
				}
			}

			var p LogicalPlan
			lp := LogicalCTE{cteAsName: tn.Name, cte: &CTEClass{IsDistinct: cte.isDistinct, seedPartLogicalPlan: cte.seedLP,
				recursivePartLogicalPlan: cte.recurLP, IDForStorage: cte.storageID,
				optFlag: cte.optFlag, HasLimit: hasLimit, LimitBeg: limitBeg,
				LimitEnd: limitEnd}}.Init(b.ctx, b.getSelectOffset())
			lp.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))
			p = lp
			p.SetOutputNames(cte.seedLP.OutputNames())
			if len(asName.String()) > 0 {
				lp.cteAsName = *asName
				var on types.NameSlice
				for _, name := range p.OutputNames() {
					cpOn := *name
					cpOn.TblName = *asName
					on = append(on, &cpOn)
				}
				p.SetOutputNames(on)
			}
			return p, nil
		}
	}

	return nil, nil
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (LogicalPlan, error) {
	dbName := tn.Schema
	sessionVars := b.ctx.GetSessionVars()

	if dbName.L == "" {
		// Try CTE.
		p, err := b.tryBuildCTE(ctx, tn, asName)
		if err != nil || p != nil {
			return p, err
		}
		dbName = model.NewCIStr(sessionVars.CurrentDB)
	}

	is := b.is
	if len(b.buildingViewStack) > 0 {
		if tempIs, ok := is.(*infoschema.TemporaryTableAttachedInfoSchema); ok {
			// For tables in view, always ignore local temporary table, considering the below case:
			// If a user created a normal table `t1` and a view `v1` referring `t1`, and then a local temporary table with a same name `t1` is created.
			// At this time, executing 'select * from v1' should still return all records from normal table `t1` instead of temporary table `t1`.
			is = tempIs.InfoSchema
		}
	}

	tbl, err := is.TableByName(dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	tableInfo := tbl.Meta()
	if b.isCreateView && tableInfo.TempTableType == model.TempTableLocal {
		return nil, ErrViewSelectTemporaryTable.GenWithStackByArgs(tn.Name)
	}

	var authErr error
	if sessionVars.User != nil {
		authErr = ErrTableaccessDenied.FastGenByArgs("SELECT", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tableInfo.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName.L, tableInfo.Name.L, "", authErr)

	if tbl.Type().IsVirtualTable() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in virtual tables")
		}
		return b.buildMemTable(ctx, dbName, tableInfo)
	}

	if tableInfo.IsView() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in views")
		}
		return b.BuildDataSourceFromView(ctx, dbName, tableInfo)
	}

	if tableInfo.IsSequence() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in sequences")
		}
		// When the source is a Sequence, we convert it to a TableDual, as what most databases do.
		return b.buildTableDual(), nil
	}

	if tableInfo.GetPartitionInfo() != nil {
		// Use the new partition implementation, clean up the code here when it's full implemented.
		if !b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
			b.optFlag = b.optFlag | flagPartitionProcessor
		}

		pt := tbl.(table.PartitionedTable)
		// check partition by name.
		if len(tn.PartitionNames) > 0 {
			pids := make(map[int64]struct{}, len(tn.PartitionNames))
			for _, name := range tn.PartitionNames {
				pid, err := tables.FindPartitionByName(tableInfo, name.L)
				if err != nil {
					return nil, err
				}
				pids[pid] = struct{}{}
			}
			pt = tables.NewPartitionTableWithGivenSets(pt, pids)
		}
		b.partitionedTable = append(b.partitionedTable, pt)
	} else if len(tn.PartitionNames) != 0 {
		return nil, ErrPartitionClauseOnNonpartitioned
	}

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}
	possiblePaths, err := getPossibleAccessPaths(b.ctx, b.TableHints(), tn.IndexHints, tbl, dbName, tblName, b.isForUpdateRead, b.is.SchemaMetaVersion())
	if err != nil {
		return nil, err
	}
	// Skip storage engine check for CreateView.
	if b.capFlag&canExpandAST == 0 {
		possiblePaths, err = filterPathByIsolationRead(b.ctx, possiblePaths, dbName)
		if err != nil {
			return nil, err
		}
	}

	// Try to substitute generate column only if there is an index on generate column.
	for _, index := range tableInfo.Indices {
		if index.State != model.StatePublic {
			continue
		}
		for _, indexCol := range index.Columns {
			colInfo := tbl.Cols()[indexCol.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				b.optFlag |= flagGcSubstitute
				break
			}
		}
	}

	var columns []*table.Column
	if b.inUpdateStmt {
		// create table t(a int, b int).
		// Imagine that, There are 2 TiDB instances in the cluster, name A, B. We add a column `c` to table t in the TiDB cluster.
		// One of the TiDB, A, the column type in its infoschema is changed to public. And in the other TiDB, the column type is
		// still StateWriteReorganization.
		// TiDB A: insert into t values(1, 2, 3);
		// TiDB B: update t set a = 2 where b = 2;
		// If we use tbl.Cols() here, the update statement, will ignore the col `c`, and the data `3` will lost.
		columns = tbl.WritableCols()
	} else if b.inDeleteStmt {
		// All hidden columns are needed because we need to delete the expression index that consists of hidden columns.
		columns = tbl.FullHiddenColsAndVisibleCols()
	} else {
		columns = tbl.Cols()
	}
	var statisticTable *statistics.Table
	if _, ok := tbl.(table.PartitionedTable); !ok || b.ctx.GetSessionVars().UseDynamicPartitionPrune() {
		statisticTable = getStatsTable(b.ctx, tbl.Meta(), tbl.Meta().ID)
	}

	// extract the IndexMergeHint
	var indexMergeHints []indexHintInfo
	if hints := b.TableHints(); hints != nil {
		for i, hint := range hints.indexMergeHintList {
			if hint.tblName.L == tblName.L && hint.dbName.L == dbName.L {
				hints.indexMergeHintList[i].matched = true
				// check whether the index names in IndexMergeHint are valid.
				invalidIdxNames := make([]string, 0, len(hint.indexHint.IndexNames))
				for _, idxName := range hint.indexHint.IndexNames {
					hasIdxName := false
					for _, path := range possiblePaths {
						if path.IsTablePath() {
							if idxName.L == "primary" {
								hasIdxName = true
								break
							}
							continue
						}
						if idxName.L == path.Index.Name.L {
							hasIdxName = true
							break
						}
					}
					if !hasIdxName {
						invalidIdxNames = append(invalidIdxNames, idxName.String())
					}
				}
				if len(invalidIdxNames) == 0 {
					indexMergeHints = append(indexMergeHints, hint)
				} else {
					// Append warning if there are invalid index names.
					errMsg := fmt.Sprintf("use_index_merge(%s) is inapplicable, check whether the indexes (%s) "+
						"exist, or the indexes are conflicted with use_index/ignore_index/force_index hints.",
						hint.indexString(), strings.Join(invalidIdxNames, ", "))
					b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
				}
			}
		}
	}
	ds := DataSource{
		DBName:              dbName,
		TableAsName:         asName,
		table:               tbl,
		tableInfo:           tableInfo,
		statisticTable:      statisticTable,
		astIndexHints:       tn.IndexHints,
		IndexHints:          b.TableHints().indexHintList,
		indexMergeHints:     indexMergeHints,
		possibleAccessPaths: possiblePaths,
		Columns:             make([]*model.ColumnInfo, 0, len(columns)),
		partitionNames:      tn.PartitionNames,
		TblCols:             make([]*expression.Column, 0, len(columns)),
		preferPartitions:    make(map[int][]model.CIStr),
		is:                  b.is,
		isForUpdateRead:     b.isForUpdateRead,
	}.Init(b.ctx, b.getSelectOffset())
	var handleCols HandleCols
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	names := make([]*types.FieldName, 0, len(columns))
	for i, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		names = append(names, &types.FieldName{
			DBName:            dbName,
			TblName:           tableInfo.Name,
			ColName:           col.Name,
			OrigTblName:       tableInfo.Name,
			OrigColName:       col.Name,
			Hidden:            col.Hidden,
			NotExplicitUsable: col.State != model.StatePublic,
		})
		newCol := &expression.Column{
			UniqueID: sessionVars.AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  col.FieldType.Clone(),
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		if col.IsPKHandleColumn(tableInfo) {
			handleCols = &IntHandleCols{col: newCol}
		}
		schema.Append(newCol)
		ds.TblCols = append(ds.TblCols, newCol)
	}
	// We append an extra handle column to the schema when the handle
	// column is not the primary key of "ds".
	if handleCols == nil {
		if tableInfo.IsCommonHandle {
			primaryIdx := tables.FindPrimaryIndex(tableInfo)
			handleCols = NewCommonHandleCols(b.ctx.GetSessionVars().StmtCtx, tableInfo, primaryIdx, ds.TblCols)
		} else {
			extraCol := ds.newExtraHandleSchemaCol()
			handleCols = &IntHandleCols{col: extraCol}
			ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
			schema.Append(extraCol)
			names = append(names, &types.FieldName{
				DBName:      dbName,
				TblName:     tableInfo.Name,
				ColName:     model.ExtraHandleName,
				OrigColName: model.ExtraHandleName,
			})
			ds.TblCols = append(ds.TblCols, extraCol)
		}
	}
	ds.handleCols = handleCols
	handleMap := make(map[int64][]HandleCols)
	handleMap[tableInfo.ID] = []HandleCols{handleCols}
	b.handleHelper.pushMap(handleMap)
	ds.SetSchema(schema)
	ds.names = names
	ds.setPreferredStoreType(b.TableHints())
	ds.SampleInfo = NewTableSampleInfo(tn.TableSample, schema.Clone(), b.partitionedTable)
	b.isSampling = ds.SampleInfo != nil

	// Init commonHandleCols and commonHandleLens for data source.
	if tableInfo.IsCommonHandle {
		ds.commonHandleCols, ds.commonHandleLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, tables.FindPrimaryIndex(tableInfo))
	}
	// Init FullIdxCols, FullIdxColLens for accessPaths.
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
		}
	}

	var result LogicalPlan = ds
	dirty := tableHasDirtyContent(b.ctx, tableInfo)
	if dirty {
		us := LogicalUnionScan{handleCols: handleCols}.Init(b.ctx, b.getSelectOffset())
		us.SetChildren(ds)
		result = us
	}
	if sessionVars.StmtCtx.TblInfo2UnionScan == nil {
		sessionVars.StmtCtx.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	}
	sessionVars.StmtCtx.TblInfo2UnionScan[tableInfo] = dirty

	for i, colExpr := range ds.Schema().Columns {
		var expr expression.Expression
		if i < len(columns) {
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				var err error
				expr, _, err = b.rewrite(ctx, columns[i].GeneratedExpr, ds, nil, true)
				if err != nil {
					return nil, err
				}
				colExpr.VirtualExpr = expr.Clone()
			}
		}
	}

	return result, nil
}

func (b *PlanBuilder) timeRangeForSummaryTable() QueryTimeRange {
	const defaultSummaryDuration = 30 * time.Minute
	hints := b.TableHints()
	// User doesn't use TIME_RANGE hint
	if hints == nil || (hints.timeRangeHint.From == "" && hints.timeRangeHint.To == "") {
		to := time.Now()
		from := to.Add(-defaultSummaryDuration)
		return QueryTimeRange{From: from, To: to}
	}

	// Parse time specified by user via TIM_RANGE hint
	parse := func(s string) (time.Time, bool) {
		t, err := time.ParseInLocation(MetricTableTimeFormat, s, time.Local)
		if err != nil {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return t, err == nil
	}
	from, fromValid := parse(hints.timeRangeHint.From)
	to, toValid := parse(hints.timeRangeHint.To)
	switch {
	case !fromValid && !toValid:
		to = time.Now()
		from = to.Add(-defaultSummaryDuration)
	case fromValid && !toValid:
		to = from.Add(defaultSummaryDuration)
	case !fromValid && toValid:
		from = to.Add(-defaultSummaryDuration)
	}

	return QueryTimeRange{From: from, To: to}
}

func (b *PlanBuilder) buildMemTable(_ context.Context, dbName model.CIStr, tableInfo *model.TableInfo) (LogicalPlan, error) {
	// We can use the `tableInfo.Columns` directly because the memory table has
	// a stable schema and there is no online DDL on the memory table.
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	names := make([]*types.FieldName, 0, len(tableInfo.Columns))
	var handleCols HandleCols
	for _, col := range tableInfo.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
		})
		// NOTE: Rewrite the expression if memory table supports generated columns in the future
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCols = &IntHandleCols{col: newCol}
		}
		schema.Append(newCol)
	}

	if handleCols != nil {
		handleMap := make(map[int64][]HandleCols)
		handleMap[tableInfo.ID] = []HandleCols{handleCols}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}

	// NOTE: Add a `LogicalUnionScan` if we support update memory table in the future
	p := LogicalMemTable{
		DBName:    dbName,
		TableInfo: tableInfo,
		Columns:   make([]*model.ColumnInfo, len(tableInfo.Columns)),
	}.Init(b.ctx, b.getSelectOffset())
	p.SetSchema(schema)
	p.names = names
	for i, col := range tableInfo.Columns {
		p.Columns[i] = col
	}

	// Some memory tables can receive some predicates
	switch dbName.L {
	case util2.MetricSchemaName.L:
		p.Extractor = newMetricTableExtractor()
	case util2.InformationSchemaName.L:
		switch strings.ToUpper(tableInfo.Name.O) {
		case infoschema.TableClusterConfig, infoschema.TableClusterLoad, infoschema.TableClusterHardware, infoschema.TableClusterSystemInfo:
			p.Extractor = &ClusterTableExtractor{}
		case infoschema.TableClusterLog:
			p.Extractor = &ClusterLogTableExtractor{}
		case infoschema.TableInspectionResult:
			p.Extractor = &InspectionResultTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionSummary:
			p.Extractor = &InspectionSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionRules:
			p.Extractor = &InspectionRuleTableExtractor{}
		case infoschema.TableMetricSummary, infoschema.TableMetricSummaryByLabel:
			p.Extractor = &MetricSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableSlowQuery:
			p.Extractor = &SlowQueryExtractor{}
		case infoschema.TableStorageStats:
			p.Extractor = &TableStorageStatsExtractor{}
		case infoschema.TableTiFlashTables, infoschema.TableTiFlashSegments:
			p.Extractor = &TiFlashSystemTableExtractor{}
		}
	}
	return p, nil
}

// checkRecursiveView checks whether this view is recursively defined.
func (b *PlanBuilder) checkRecursiveView(dbName model.CIStr, tableName model.CIStr) (func(), error) {
	viewFullName := dbName.L + "." + tableName.L
	if b.buildingViewStack == nil {
		b.buildingViewStack = set.NewStringSet()
	}
	// If this view has already been on the building stack, it means
	// this view contains a recursive definition.
	if b.buildingViewStack.Exist(viewFullName) {
		return nil, ErrViewRecursive.GenWithStackByArgs(dbName.O, tableName.O)
	}
	// If the view is being renamed, we return the mysql compatible error message.
	if b.capFlag&renameView != 0 && viewFullName == b.renamingViewName {
		return nil, ErrNoSuchTable.GenWithStackByArgs(dbName.O, tableName.O)
	}
	b.buildingViewStack.Insert(viewFullName)
	return func() { delete(b.buildingViewStack, viewFullName) }, nil
}

// BuildDataSourceFromView is used to build LogicalPlan from view
func (b *PlanBuilder) BuildDataSourceFromView(ctx context.Context, dbName model.CIStr, tableInfo *model.TableInfo) (LogicalPlan, error) {
	deferFunc, err := b.checkRecursiveView(dbName, tableInfo.Name)
	if err != nil {
		return nil, err
	}
	defer deferFunc()

	charset, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	viewParser := parser.New()
	viewParser.SetParserConfig(b.ctx.GetSessionVars().BuildParserConfig())
	selectNode, err := viewParser.ParseOneStmt(tableInfo.View.SelectStmt, charset, collation)
	if err != nil {
		return nil, err
	}
	originalVisitInfo := b.visitInfo
	b.visitInfo = make([]visitInfo, 0)
	selectLogicalPlan, err := b.Build(ctx, selectNode)
	if err != nil {
		if terror.ErrorNotEqual(err, ErrViewRecursive) &&
			terror.ErrorNotEqual(err, ErrNoSuchTable) &&
			terror.ErrorNotEqual(err, ErrInternal) {
			err = ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
		}
		return nil, err
	}

	if tableInfo.View.Security == model.SecurityDefiner {
		if pm := privilege.GetPrivilegeManager(b.ctx); pm != nil {
			for _, v := range b.visitInfo {
				if !pm.RequestVerificationWithUser(v.db, v.table, v.column, v.privilege, tableInfo.View.Definer) {
					return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
				}
			}
		}
		b.visitInfo = b.visitInfo[:0]
	}
	b.visitInfo = append(originalVisitInfo, b.visitInfo...)

	if b.ctx.GetSessionVars().StmtCtx.InExplainStmt {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, dbName.L, tableInfo.Name.L, "", ErrViewNoExplain)
	}

	if len(tableInfo.Columns) != selectLogicalPlan.Schema().Len() {
		return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
	}

	return b.buildProjUponView(ctx, dbName, tableInfo, selectLogicalPlan)
}

func (b *PlanBuilder) buildProjUponView(ctx context.Context, dbName model.CIStr, tableInfo *model.TableInfo, selectLogicalPlan Plan) (LogicalPlan, error) {
	columnInfo := tableInfo.Cols()
	cols := selectLogicalPlan.Schema().Clone().Columns
	outputNamesOfUnderlyingSelect := selectLogicalPlan.OutputNames().Shallow()
	// In the old version of VIEW implementation, tableInfo.View.Cols is used to
	// store the origin columns' names of the underlying SelectStmt used when
	// creating the view.
	if tableInfo.View.Cols != nil {
		cols = cols[:0]
		outputNamesOfUnderlyingSelect = outputNamesOfUnderlyingSelect[:0]
		for _, info := range columnInfo {
			idx := expression.FindFieldNameIdxByColName(selectLogicalPlan.OutputNames(), info.Name.L)
			if idx == -1 {
				return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
			}
			cols = append(cols, selectLogicalPlan.Schema().Columns[idx])
			outputNamesOfUnderlyingSelect = append(outputNamesOfUnderlyingSelect, selectLogicalPlan.OutputNames()[idx])
		}
	}

	projSchema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	projExprs := make([]expression.Expression, 0, len(tableInfo.Columns))
	projNames := make(types.NameSlice, 0, len(tableInfo.Columns))
	for i, name := range outputNamesOfUnderlyingSelect {
		origColName := name.ColName
		if tableInfo.View.Cols != nil {
			origColName = tableInfo.View.Cols[i]
		}
		projNames = append(projNames, &types.FieldName{
			// TblName is the of view instead of the name of the underlying table.
			TblName:     tableInfo.Name,
			OrigTblName: name.OrigTblName,
			ColName:     columnInfo[i].Name,
			OrigColName: origColName,
			DBName:      dbName,
		})
		projSchema.Append(&expression.Column{
			UniqueID: cols[i].UniqueID,
			RetType:  cols[i].GetType(),
		})
		projExprs = append(projExprs, cols[i])
	}
	projUponView := LogicalProjection{Exprs: projExprs}.Init(b.ctx, b.getSelectOffset())
	projUponView.names = projNames
	projUponView.SetChildren(selectLogicalPlan.(LogicalPlan))
	projUponView.SetSchema(projSchema)
	return projUponView, nil
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildApplyWithJoinType(outerPlan, innerPlan LogicalPlan, tp JoinType) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}}.Init(b.ctx, b.getSelectOffset())
	ap.SetChildren(outerPlan, innerPlan)
	ap.names = make([]*types.FieldName, outerPlan.Schema().Len()+innerPlan.Schema().Len())
	copy(ap.names, outerPlan.OutputNames())
	ap.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
	// Note that, tp can only be LeftOuterJoin or InnerJoin, so we don't consider other outer joins.
	if tp == LeftOuterJoin {
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		resetNotNullFlag(ap.schema, outerPlan.Schema().Len(), ap.schema.Len())
	}
	for i := outerPlan.Schema().Len(); i < ap.Schema().Len(); i++ {
		ap.names[i] = types.EmptyName
	}
	return ap
}

// buildSemiApply builds apply plan with outerPlan and innerPlan, which apply semi-join for every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildSemiApply(outerPlan, innerPlan LogicalPlan, condition []expression.Expression, asScalar, not bool) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate

	join, err := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not)
	if err != nil {
		return nil, err
	}

	ap := &LogicalApply{LogicalJoin: *join}
	ap.tp = plancodec.TypeApply
	ap.self = ap
	return ap, nil
}

func (b *PlanBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := LogicalMaxOneRow{}.Init(b.ctx, b.getSelectOffset())
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *PlanBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) (*LogicalJoin, error) {
	joinPlan := LogicalJoin{}.Init(b.ctx, b.getSelectOffset())
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.AttachOnConds(onCondition)
	joinPlan.names = make([]*types.FieldName, outerPlan.Schema().Len(), outerPlan.Schema().Len()+innerPlan.Schema().Len()+1)
	copy(joinPlan.names, outerPlan.OutputNames())
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			RetType:  types.NewFieldType(mysql.TypeTiny),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
		joinPlan.names = append(joinPlan.names, types.EmptyName)
		joinPlan.SetSchema(newSchema)
		if not {
			joinPlan.JoinType = AntiLeftOuterSemiJoin
		} else {
			joinPlan.JoinType = LeftOuterSemiJoin
		}
	} else {
		joinPlan.SetSchema(outerPlan.Schema().Clone())
		if not {
			joinPlan.JoinType = AntiSemiJoin
		} else {
			joinPlan.JoinType = SemiJoin
		}
	}
	// Apply forces to choose hash join currently, so don't worry the hints will take effect if the semi join is in one apply.
	if b.TableHints() != nil {
		outerAlias := extractTableAlias(outerPlan, joinPlan.blockOffset)
		innerAlias := extractTableAlias(innerPlan, joinPlan.blockOffset)
		if b.TableHints().ifPreferMergeJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferMergeJoin
		}
		if b.TableHints().ifPreferHashJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferHashJoin
		}
		if b.TableHints().ifPreferINLJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLJInner
		}
		if b.TableHints().ifPreferINLHJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLHJInner
		}
		if b.TableHints().ifPreferINLMJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLMJInner
		}
		// If there're multiple join hints, they're conflict.
		if bits.OnesCount(joinPlan.preferJoinType) > 1 {
			return nil, errors.New("Join hints are conflict, you can only specify one type of join")
		}
	}
	return joinPlan, nil
}

func getTableOffset(names []*types.FieldName, handleName *types.FieldName) (int, error) {
	for i, name := range names {
		if name.DBName.L == handleName.DBName.L && name.TblName.L == handleName.TblName.L {
			return i, nil
		}
	}
	return -1, errors.Errorf("Couldn't get column information when do update/delete")
}

// TblColPosInfo represents an mapper from column index to handle index.
type TblColPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleCols HandleCols
}

// TblColPosInfoSlice attaches the methods of sort.Interface to []TblColPosInfos sorting in increasing order.
type TblColPosInfoSlice []TblColPosInfo

// Len implements sort.Interface#Len.
func (c TblColPosInfoSlice) Len() int {
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c TblColPosInfoSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c TblColPosInfoSlice) Less(i, j int) bool {
	return c[i].Start < c[j].Start
}

// FindTblIdx finds the ordinal of the corresponding access column.
func (c TblColPosInfoSlice) FindTblIdx(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than colOrdinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].Start > colOrdinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	return rangeBehindOrdinal - 1, true
}

// buildColumns2Handle builds columns to handle mapping.
func buildColumns2Handle(
	names []*types.FieldName,
	tblID2Handle map[int64][]HandleCols,
	tblID2Table map[int64]table.Table,
	onlyWritableCol bool,
) (TblColPosInfoSlice, error) {
	var cols2Handles TblColPosInfoSlice
	for tblID, handleCols := range tblID2Handle {
		tbl := tblID2Table[tblID]
		var tblLen int
		if onlyWritableCol {
			tblLen = len(tbl.WritableCols())
		} else {
			tblLen = len(tbl.Cols())
		}
		for _, handleCol := range handleCols {
			offset, err := getTableOffset(names, names[handleCol.GetCol(0).Index])
			if err != nil {
				return nil, err
			}
			end := offset + tblLen
			cols2Handles = append(cols2Handles, TblColPosInfo{tblID, offset, end, handleCol})
		}
	}
	sort.Sort(cols2Handles)
	return cols2Handles, nil
}

func (b *PlanBuilder) buildUpdate(ctx context.Context, update *ast.UpdateStmt) (Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(update.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current UPDATE statement.
		b.popTableHints()
	}()

	b.inUpdateStmt = true
	b.isForUpdateRead = true

	if update.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		err := b.buildWith(ctx, update.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, update.TableRefs.TableRefs)
	if err != nil {
		return nil, err
	}

	var tableList []*ast.TableName
	tableList = extractTableList(update.TableRefs.TableRefs, tableList, false)
	for _, t := range tableList {
		dbName := t.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, t.Name.L, "", nil)
	}

	oldSchemaLen := p.Schema().Len()
	if update.Where != nil {
		p, err = b.buildSelection(ctx, p, update.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if update.TableRefs.TableRefs.Right == nil {
			// buildSelectLock is an optimization that can reduce RPC call.
			// We only need do this optimization for single table update which is the most common case.
			// When TableRefs.Right is nil, it is single table update.
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if update.Order != nil {
		p, err = b.buildSort(ctx, p, update.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	if update.Limit != nil {
		p, err = b.buildLimit(p, update.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Add project to freeze the order of output columns.
	proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldSchemaLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, oldSchemaLen)...))
	proj.names = make(types.NameSlice, len(p.OutputNames()))
	copy(proj.names, p.OutputNames())
	copy(proj.schema.Columns, p.Schema().Columns[:oldSchemaLen])
	proj.SetChildren(p)
	p = proj

	// update subquery table should be forbidden
	var notUpdatableTbl []string
	notUpdatableTbl = extractTableSourceAsNames(update.TableRefs.TableRefs, notUpdatableTbl, true)

	var updateTableList []*ast.TableName
	updateTableList = extractTableList(update.TableRefs.TableRefs, updateTableList, true)
	orderedList, np, allAssignmentsAreConstant, err := b.buildUpdateLists(ctx, updateTableList, update.List, p, notUpdatableTbl)
	if err != nil {
		return nil, err
	}
	p = np

	updt := Update{
		OrderedList:               orderedList,
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(update.List),
	}.Init(b.ctx)
	updt.names = p.OutputNames()
	// We cannot apply projection elimination when building the subplan, because
	// columns in orderedList cannot be resolved.
	updt.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag&^flagEliminateProjection, p)
	if err != nil {
		return nil, err
	}
	err = updt.ResolveIndices()
	if err != nil {
		return nil, err
	}
	tblID2Handle, err := resolveIndicesForTblID2Handle(b.handleHelper.tailMap(), updt.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	updt.TblColPosInfos, err = buildColumns2Handle(updt.OutputNames(), tblID2Handle, tblID2table, true)
	updt.PartitionedTable = b.partitionedTable
	updt.tblID2Table = tblID2table
	return updt, err
}

type tblUpdateInfo struct {
	name      string
	pkUpdated bool
}

// CheckUpdateList checks all related columns in updatable state.
func CheckUpdateList(assignFlags []int, updt *Update) error {
	updateFromOtherAlias := make(map[int64]tblUpdateInfo)
	for _, content := range updt.TblColPosInfos {
		tbl := updt.tblID2Table[content.TblID]
		flags := assignFlags[content.Start:content.End]
		var update, updatePK bool
		for i, col := range tbl.WritableCols() {
			if flags[i] >= 0 && col.State != model.StatePublic {
				return ErrUnknownColumn.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
			}
			if flags[i] >= 0 {
				update = true
				if mysql.HasPriKeyFlag(col.Flag) {
					updatePK = true
				}
			}
		}
		if update {
			// Check for multi-updates on primary key,
			// see https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html#error_er_multi_update_key_conflict
			if otherTable, ok := updateFromOtherAlias[tbl.Meta().ID]; ok {
				if otherTable.pkUpdated || updatePK {
					return ErrMultiUpdateKeyConflict.GenWithStackByArgs(otherTable.name, updt.names[content.Start].TblName.O)
				}
			} else {
				updateFromOtherAlias[tbl.Meta().ID] = tblUpdateInfo{
					name:      updt.names[content.Start].TblName.O,
					pkUpdated: updatePK,
				}
			}
		}
	}
	return nil
}

// If tl is CTE, its TableInfo will be nil.
// Only used in build plan from AST after preprocess.
func isCTE(tl *ast.TableName) bool {
	return tl.TableInfo == nil
}

func (b *PlanBuilder) buildUpdateLists(ctx context.Context, tableList []*ast.TableName, list []*ast.Assignment, p LogicalPlan,
	notUpdatableTbl []string) (newList []*expression.Assignment, po LogicalPlan, allAssignmentsAreConstant bool, e error) {
	b.curClause = fieldList
	// modifyColumns indicates which columns are in set list,
	// and if it is set to `DEFAULT`
	modifyColumns := make(map[string]bool, p.Schema().Len())
	var columnsIdx map[*ast.ColumnName]int
	cacheColumnsIdx := false
	if len(p.OutputNames()) > 16 {
		cacheColumnsIdx = true
		columnsIdx = make(map[*ast.ColumnName]int, len(list))
	}
	for _, assign := range list {
		idx, err := expression.FindFieldName(p.OutputNames(), assign.Column)
		if err != nil {
			return nil, nil, false, err
		}
		if idx < 0 {
			return nil, nil, false, ErrUnknownColumn.GenWithStackByArgs(assign.Column.Name, "field list")
		}
		if cacheColumnsIdx {
			columnsIdx[assign.Column] = idx
		}
		name := p.OutputNames()[idx]
		for _, tl := range tableList {
			if (tl.Schema.L == "" || tl.Schema.L == name.DBName.L) && (tl.Name.L == name.TblName.L) {
				if isCTE(tl) || tl.TableInfo.IsView() || tl.TableInfo.IsSequence() {
					return nil, nil, false, ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
				}
				// may be a subquery
				if tl.Schema.L == "" {
					for _, nTbl := range notUpdatableTbl {
						if nTbl == name.TblName.L {
							return nil, nil, false, ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
						}
					}
				}
			}
		}
		columnFullName := fmt.Sprintf("%s.%s.%s", name.DBName.L, name.TblName.L, name.ColName.L)
		// We save a flag for the column in map `modifyColumns`
		// This flag indicated if assign keyword `DEFAULT` to the column
		if extractDefaultExpr(assign.Expr) != nil {
			modifyColumns[columnFullName] = true
		} else {
			modifyColumns[columnFullName] = false
		}
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)
	for _, tn := range tableList {
		// Only generate virtual to updatable table, skip not updatable table(i.e. table in update's subQuery)
		updatable := true
		for _, nTbl := range notUpdatableTbl {
			if tn.Name.L == nTbl {
				updatable = false
				break
			}
		}
		if !updatable || isCTE(tn) || tn.TableInfo.IsView() || tn.TableInfo.IsSequence() {
			continue
		}

		tableInfo := tn.TableInfo
		tableVal, found := b.is.TableByID(tableInfo.ID)
		if !found {
			return nil, nil, false, infoschema.ErrTableNotExists.GenWithStackByArgs(tn.DBInfo.Name.O, tableInfo.Name.O)
		}
		for i, colInfo := range tableInfo.Columns {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.DBInfo.Name.L, tn.Name.L, colInfo.Name.L)
			isDefault, ok := modifyColumns[columnFullName]
			if ok && colInfo.Hidden {
				return nil, nil, false, ErrUnknownColumn.GenWithStackByArgs(colInfo.Name, clauseMsg[fieldList])
			}
			// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
			// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
			if ok && !isDefault {
				return nil, nil, false, ErrBadGeneratedColumn.GenWithStackByArgs(colInfo.Name.O, tableInfo.Name.O)
			}
			virtualAssignments = append(virtualAssignments, &ast.Assignment{
				Column: &ast.ColumnName{Schema: tn.Schema, Table: tn.Name, Name: colInfo.Name},
				Expr:   tableVal.Cols()[i].GeneratedExpr,
			})
		}
	}

	allAssignmentsAreConstant = true
	newList = make([]*expression.Assignment, 0, p.Schema().Len())
	tblDbMap := make(map[string]string, len(tableList))
	for _, tbl := range tableList {
		if isCTE(tbl) {
			continue
		}
		tblDbMap[tbl.Name.L] = tbl.DBInfo.Name.L
	}

	allAssignments := append(list, virtualAssignments...)
	dependentColumnsModified := make(map[int64]bool)
	for i, assign := range allAssignments {
		var idx int
		var err error
		if cacheColumnsIdx {
			if i, ok := columnsIdx[assign.Column]; ok {
				idx = i
			} else {
				idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
			}
		} else {
			idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
		}
		if err != nil {
			return nil, nil, false, err
		}
		col := p.Schema().Columns[idx]
		name := p.OutputNames()[idx]
		var newExpr expression.Expression
		var np LogicalPlan
		if i < len(list) {
			// If assign `DEFAULT` to column, fill the `defaultExpr.Name` before rewrite expression
			if expr := extractDefaultExpr(assign.Expr); expr != nil {
				expr.Name = assign.Column
			}
			newExpr, np, err = b.rewrite(ctx, assign.Expr, p, nil, false)
			if err != nil {
				return nil, nil, false, err
			}
			dependentColumnsModified[col.UniqueID] = true
		} else {
			// rewrite with generation expression
			rewritePreprocess := func(expr ast.Node) ast.Node {
				switch x := expr.(type) {
				case *ast.ColumnName:
					return &ast.ColumnName{
						Schema: assign.Column.Schema,
						Table:  assign.Column.Table,
						Name:   x.Name,
					}
				default:
					return expr
				}
			}
			newExpr, np, err = b.rewriteWithPreprocess(ctx, assign.Expr, p, nil, nil, false, rewritePreprocess)
			if err != nil {
				return nil, nil, false, err
			}
			// check if the column is modified
			dependentColumns := expression.ExtractDependentColumns(newExpr)
			var isModified bool
			for _, col := range dependentColumns {
				if dependentColumnsModified[col.UniqueID] {
					isModified = true
					break
				}
			}
			// skip unmodified generated columns
			if !isModified {
				continue
			}
		}
		if _, isConst := newExpr.(*expression.Constant); !isConst {
			allAssignmentsAreConstant = false
		}
		p = np
		newList = append(newList, &expression.Assignment{Col: col, ColName: name.ColName, Expr: newExpr})
		dbName := name.DBName.L
		// To solve issue#10028, we need to get database name by the table alias name.
		if dbNameTmp, ok := tblDbMap[name.TblName.L]; ok {
			dbName = dbNameTmp
		}
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, name.OrigTblName.L, "", nil)
	}
	return newList, p, allAssignmentsAreConstant, nil
}

// extractDefaultExpr extract a `DefaultExpr` from `ExprNode`,
// If it is a `DEFAULT` function like `DEFAULT(a)`, return nil.
// Only if it is `DEFAULT` keyword, it will return the `DefaultExpr`.
func extractDefaultExpr(node ast.ExprNode) *ast.DefaultExpr {
	if expr, ok := node.(*ast.DefaultExpr); ok && expr.Name == nil {
		return expr
	}
	return nil
}

func (b *PlanBuilder) buildDelete(ctx context.Context, delete *ast.DeleteStmt) (Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(delete.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current DELETE statement.
		b.popTableHints()
	}()

	b.inDeleteStmt = true
	b.isForUpdateRead = true

	if delete.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		err := b.buildWith(ctx, delete.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, delete.TableRefs.TableRefs)
	if err != nil {
		return nil, err
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	if delete.Where != nil {
		p, err = b.buildSelection(ctx, p, delete.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if !delete.IsMultiTable {
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if delete.Order != nil {
		p, err = b.buildSort(ctx, p, delete.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if delete.Limit != nil {
		p, err = b.buildLimit(p, delete.Limit)
		if err != nil {
			return nil, err
		}
	}

	// If the delete is non-qualified it does not require Select Priv
	if delete.Where == nil && delete.Order == nil {
		b.popVisitInfo()
	}
	var authErr error
	sessionVars := b.ctx.GetSessionVars()

	proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetChildren(p)
	proj.SetSchema(oldSchema.Clone())
	proj.names = p.OutputNames()[:oldLen]
	p = proj

	handleColsMap := b.handleHelper.tailMap()
	for _, cols := range handleColsMap {
		for _, col := range cols {
			for i := 0; i < col.NumCols(); i++ {
				exprCol := col.GetCol(i)
				if proj.Schema().Contains(exprCol) {
					continue
				}
				proj.Exprs = append(proj.Exprs, exprCol)
				proj.Schema().Columns = append(proj.Schema().Columns, exprCol)
				proj.names = append(proj.names, types.EmptyName)
			}
		}
	}

	del := Delete{
		IsMultiTable: delete.IsMultiTable,
	}.Init(b.ctx)

	del.names = p.OutputNames()
	del.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, p)
	if err != nil {
		return nil, err
	}

	tblID2Handle, err := resolveIndicesForTblID2Handle(handleColsMap, del.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}

	// Collect visitInfo.
	if delete.Tables != nil {
		// Delete a, b from a, b, c, d... add a and b.
		updatableList := make(map[string]bool)
		tbInfoList := make(map[string]*ast.TableName)
		collectTableName(delete.TableRefs.TableRefs, &updatableList, &tbInfoList)
		for _, tn := range delete.Tables.Tables {
			var canUpdate, foundMatch = false, false
			name := tn.Name.L
			if tn.Schema.L == "" {
				canUpdate, foundMatch = updatableList[name]
			}

			if !foundMatch {
				if tn.Schema.L == "" {
					name = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB).L + "." + tn.Name.L
				} else {
					name = tn.Schema.L + "." + tn.Name.L
				}
				canUpdate, foundMatch = updatableList[name]
			}
			// check sql like: `delete b from (select * from t) as a, t`
			if !foundMatch {
				return nil, ErrUnknownTable.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
			}
			// check sql like: `delete a from (select * from t) as a, t`
			if !canUpdate {
				return nil, ErrNonUpdatableTable.GenWithStackByArgs(tn.Name.O, "DELETE")
			}
			tb := tbInfoList[name]
			tn.DBInfo = tb.DBInfo
			tn.TableInfo = tb.TableInfo
			if tn.TableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now.", tn.Name.O)
			}
			if tn.TableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now.", tn.Name.O)
			}
			if sessionVars.User != nil {
				authErr = ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tb.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, tb.DBInfo.Name.L, tb.Name.L, "", authErr)
		}
	} else {
		// Delete from a, b, c, d.
		var tableList []*ast.TableName
		tableList = extractTableList(delete.TableRefs.TableRefs, tableList, false)
		for _, v := range tableList {
			if isCTE(v) {
				return nil, ErrNonUpdatableTable.GenWithStackByArgs(v.Name.O, "DELETE")
			}
			if v.TableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now.", v.Name.O)
			}
			if v.TableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now.", v.Name.O)
			}
			dbName := v.Schema.L
			if dbName == "" {
				dbName = b.ctx.GetSessionVars().CurrentDB
			}
			if sessionVars.User != nil {
				authErr = ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, v.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, dbName, v.Name.L, "", authErr)
		}
	}
	if del.IsMultiTable {
		// tblID2TableName is the table map value is an array which contains table aliases.
		// Table ID may not be unique for deleting multiple tables, for statements like
		// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
		// by its alias instead of ID.
		tblID2TableName := make(map[int64][]*ast.TableName, len(delete.Tables.Tables))
		for _, tn := range delete.Tables.Tables {
			tblID2TableName[tn.TableInfo.ID] = append(tblID2TableName[tn.TableInfo.ID], tn)
		}
		tblID2Handle = del.cleanTblID2HandleMap(tblID2TableName, tblID2Handle, del.names)
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	del.TblColPosInfos, err = buildColumns2Handle(del.names, tblID2Handle, tblID2table, false)
	return del, err
}

func resolveIndicesForTblID2Handle(tblID2Handle map[int64][]HandleCols, schema *expression.Schema) (map[int64][]HandleCols, error) {
	newMap := make(map[int64][]HandleCols, len(tblID2Handle))
	for i, cols := range tblID2Handle {
		for _, col := range cols {
			resolvedCol, err := col.ResolveIndices(schema)
			if err != nil {
				return nil, err
			}
			newMap[i] = append(newMap[i], resolvedCol)
		}
	}
	return newMap, nil
}

func (p *Delete) cleanTblID2HandleMap(
	tablesToDelete map[int64][]*ast.TableName,
	tblID2Handle map[int64][]HandleCols,
	outputNames []*types.FieldName,
) map[int64][]HandleCols {
	for id, cols := range tblID2Handle {
		names, ok := tablesToDelete[id]
		if !ok {
			delete(tblID2Handle, id)
			continue
		}
		for i := len(cols) - 1; i >= 0; i-- {
			hCols := cols[i]
			var hasMatch bool
			for j := 0; j < hCols.NumCols(); j++ {
				if p.matchingDeletingTable(names, outputNames[hCols.GetCol(j).Index]) {
					hasMatch = true
					break
				}
			}
			if !hasMatch {
				cols = append(cols[:i], cols[i+1:]...)
			}
		}
		if len(cols) == 0 {
			delete(tblID2Handle, id)
			continue
		}
		tblID2Handle[id] = cols
	}
	return tblID2Handle
}

// matchingDeletingTable checks whether this column is from the table which is in the deleting list.
func (p *Delete) matchingDeletingTable(names []*ast.TableName, name *types.FieldName) bool {
	for _, n := range names {
		if (name.DBName.L == "" || name.DBName.L == n.DBInfo.Name.L) && name.TblName.L == n.Name.L {
			return true
		}
	}
	return false
}

func getWindowName(name string) string {
	if name == "" {
		return "<unnamed window>"
	}
	return name
}

// buildProjectionForWindow builds the projection for expressions in the window specification that is not an column,
// so after the projection, window functions only needs to deal with columns.
func (b *PlanBuilder) buildProjectionForWindow(ctx context.Context, p LogicalPlan, spec *ast.WindowSpec, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, []property.SortItem, []property.SortItem, []expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	var partitionItems, orderItems []*ast.ByItem
	if spec.PartitionBy != nil {
		partitionItems = spec.PartitionBy.Items
	}
	if spec.OrderBy != nil {
		orderItems = spec.OrderBy.Items
	}

	projLen := len(p.Schema().Columns) + len(partitionItems) + len(orderItems) + len(args)
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, projLen)}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, 0, projLen)...))
	proj.names = make([]*types.FieldName, p.Schema().Len(), projLen)
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
		proj.schema.Append(col)
	}
	copy(proj.names, p.OutputNames())

	propertyItems := make([]property.SortItem, 0, len(partitionItems)+len(orderItems))
	var err error
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, partitionItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	lenPartition := len(propertyItems)
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, orderItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	newArgList := make([]expression.Expression, 0, len(args))
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg)
			continue
		}
		proj.Exprs = append(proj.Exprs, newArg)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(),
		}
		proj.schema.Append(col)
		newArgList = append(newArgList, col)
	}

	proj.SetChildren(p)
	return proj, propertyItems[:lenPartition], propertyItems[lenPartition:], newArgList, nil
}

func (b *PlanBuilder) buildArgs4WindowFunc(ctx context.Context, p LogicalPlan, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) ([]expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	newArgList := make([]expression.Expression, 0, len(args))
	// use below index for created a new col definition
	// it's okay here because we only want to return the args used in window function
	newColIndex := 0
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg)
			continue
		}
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(),
		}
		newColIndex += 1
		newArgList = append(newArgList, col)
	}
	return newArgList, nil
}

func (b *PlanBuilder) buildByItemsForWindow(
	ctx context.Context,
	p LogicalPlan,
	proj *LogicalProjection,
	items []*ast.ByItem,
	retItems []property.SortItem,
	aggMap map[*ast.AggregateFuncExpr]int,
) (LogicalPlan, []property.SortItem, error) {
	transformer := &itemTransformer{}
	for _, item := range items {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(ctx, item.Expr, p, aggMap, true)
		if err != nil {
			return nil, nil, err
		}
		p = np
		if it.GetType().Tp == mysql.TypeNull {
			continue
		}
		if col, ok := it.(*expression.Column); ok {
			retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
			continue
		}
		proj.Exprs = append(proj.Exprs, it)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  it.GetType(),
		}
		proj.schema.Append(col)
		retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return p, retItems, nil
}

// buildWindowFunctionFrameBound builds the bounds of window function frames.
// For type `Rows`, the bound expr must be an unsigned integer.
// For type `Range`, the bound expr must be temporal or numeric types.
func (b *PlanBuilder) buildWindowFunctionFrameBound(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem, boundClause *ast.FrameBound) (*FrameBound, error) {
	frameType := spec.Frame.Type
	bound := &FrameBound{Type: boundClause.Type, UnBounded: boundClause.UnBounded}
	if bound.UnBounded {
		return bound, nil
	}

	if frameType == ast.Rows {
		if bound.Type == ast.CurrentRow {
			return bound, nil
		}
		numRows, _, _ := getUintFromNode(b.ctx, boundClause.Expr)
		bound.Num = numRows
		return bound, nil
	}

	bound.CalcFuncs = make([]expression.Expression, len(orderByItems))
	bound.CmpFuncs = make([]expression.CompareFunc, len(orderByItems))
	if bound.Type == ast.CurrentRow {
		for i, item := range orderByItems {
			col := item.Col
			bound.CalcFuncs[i] = col
			bound.CmpFuncs[i] = expression.GetCmpFunction(b.ctx, col, col)
		}
		return bound, nil
	}

	col := orderByItems[0].Col
	// TODO: We also need to raise error for non-deterministic expressions, like rand().
	val, err := evalAstExpr(b.ctx, boundClause.Expr)
	if err != nil {
		return nil, ErrWindowRangeBoundNotConstant.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	expr := expression.Constant{Value: val, RetType: boundClause.Expr.GetType()}

	checker := &paramMarkerInPrepareChecker{}
	boundClause.Expr.Accept(checker)

	// If it has paramMarker and is in prepare stmt. We don't need to eval it since its value is not decided yet.
	if !checker.inPrepareStmt {
		// Do not raise warnings for truncate.
		oriIgnoreTruncate := b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate
		b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
		uVal, isNull, err := expr.EvalInt(b.ctx, chunk.Row{})
		b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate = oriIgnoreTruncate
		if uVal < 0 || isNull || err != nil {
			return nil, ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
	}

	desc := orderByItems[0].Desc
	if boundClause.Unit != ast.TimeUnitInvalid {
		// TODO: Perhaps we don't need to transcode this back to generic string
		unitVal := boundClause.Unit.String()
		unit := expression.Constant{
			Value:   types.NewStringDatum(unitVal),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}

		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName := ast.DateAdd
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.DateSub
		}
		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr, &unit)
		if err != nil {
			return nil, err
		}
		bound.CmpFuncs[0] = expression.GetCmpFunction(b.ctx, orderByItems[0].Col, bound.CalcFuncs[0])
		return bound, nil
	}
	// When the order is asc:
	//   `+` for following, and `-` for the preceding
	// When the order is desc, `+` becomes `-` and vice-versa.
	funcName := ast.Plus
	if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
		funcName = ast.Minus
	}
	bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr)
	if err != nil {
		return nil, err
	}
	bound.CmpFuncs[0] = expression.GetCmpFunction(b.ctx, orderByItems[0].Col, bound.CalcFuncs[0])
	return bound, nil
}

// paramMarkerInPrepareChecker checks whether the given ast tree has paramMarker and is in prepare statement.
type paramMarkerInPrepareChecker struct {
	inPrepareStmt bool
}

// Enter implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch v := in.(type) {
	case *driver.ParamMarkerExpr:
		pc.inPrepareStmt = !v.InExecute
		return v, true
	}
	return in, false
}

// Leave implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// buildWindowFunctionFrame builds the window function frames.
// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
func (b *PlanBuilder) buildWindowFunctionFrame(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem) (*WindowFrame, error) {
	frameClause := spec.Frame
	if frameClause == nil {
		return nil, nil
	}
	frame := &WindowFrame{Type: frameClause.Type}
	var err error
	frame.Start, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.Start)
	if err != nil {
		return nil, err
	}
	frame.End, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.End)
	return frame, err
}

func (b *PlanBuilder) checkWindowFuncArgs(ctx context.Context, p LogicalPlan, windowFuncExprs []*ast.WindowFuncExpr, windowAggMap map[*ast.AggregateFuncExpr]int) error {
	for _, windowFuncExpr := range windowFuncExprs {
		if strings.ToLower(windowFuncExpr.F) == ast.AggFuncGroupConcat {
			return ErrNotSupportedYet.GenWithStackByArgs("group_concat as window function")
		}
		args, err := b.buildArgs4WindowFunc(ctx, p, windowFuncExpr.Args, windowAggMap)
		if err != nil {
			return err
		}
		desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFuncExpr.F, args)
		if err != nil {
			return err
		}
		if desc == nil {
			return ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFuncExpr.F))
		}
	}
	return nil
}

func getAllByItems(itemsBuf []*ast.ByItem, spec *ast.WindowSpec) []*ast.ByItem {
	itemsBuf = itemsBuf[:0]
	if spec.PartitionBy != nil {
		itemsBuf = append(itemsBuf, spec.PartitionBy.Items...)
	}
	if spec.OrderBy != nil {
		itemsBuf = append(itemsBuf, spec.OrderBy.Items...)
	}
	return itemsBuf
}

func restoreByItemText(item *ast.ByItem) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	err := item.Expr.Restore(ctx)
	if err != nil {
		return ""
	}
	return sb.String()
}

func compareItems(lItems []*ast.ByItem, rItems []*ast.ByItem) bool {
	minLen := mathutil.Min(len(lItems), len(rItems))
	for i := 0; i < minLen; i++ {
		res := strings.Compare(restoreByItemText(lItems[i]), restoreByItemText(rItems[i]))
		if res != 0 {
			return res < 0
		}
		res = compareBool(lItems[i].Desc, rItems[i].Desc)
		if res != 0 {
			return res < 0
		}
	}
	return len(lItems) < len(rItems)
}

type windowFuncs struct {
	spec  *ast.WindowSpec
	funcs []*ast.WindowFuncExpr
}

// sortWindowSpecs sorts the window specifications by reversed alphabetical order, then we could add less `Sort` operator
// in physical plan because the window functions with the same partition by and order by clause will be at near places.
func sortWindowSpecs(groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec) []windowFuncs {
	windows := make([]windowFuncs, 0, len(groupedFuncs))
	for _, spec := range orderedSpec {
		windows = append(windows, windowFuncs{spec, groupedFuncs[spec]})
	}
	lItemsBuf := make([]*ast.ByItem, 0, 4)
	rItemsBuf := make([]*ast.ByItem, 0, 4)
	sort.SliceStable(windows, func(i, j int) bool {
		lItemsBuf = getAllByItems(lItemsBuf, windows[i].spec)
		rItemsBuf = getAllByItems(rItemsBuf, windows[j].spec)
		return !compareItems(lItemsBuf, rItemsBuf)
	})
	return windows
}

func (b *PlanBuilder) buildWindowFunctions(ctx context.Context, p LogicalPlan, groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, map[*ast.WindowFuncExpr]int, error) {
	args := make([]ast.ExprNode, 0, 4)
	windowMap := make(map[*ast.WindowFuncExpr]int)
	for _, window := range sortWindowSpecs(groupedFuncs, orderedSpec) {
		args = args[:0]
		spec, funcs := window.spec, window.funcs
		for _, windowFunc := range funcs {
			args = append(args, windowFunc.Args...)
		}
		np, partitionBy, orderBy, args, err := b.buildProjectionForWindow(ctx, p, spec, args, aggMap)
		if err != nil {
			return nil, nil, err
		}
		if len(funcs) == 0 {
			// len(funcs) == 0 indicates this an unused named window spec,
			// so we just check for its validity and don't have to build plan for it.
			err := b.checkOriginWindowSpec(spec, orderBy)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		err = b.checkOriginWindowFuncs(funcs, orderBy)
		if err != nil {
			return nil, nil, err
		}
		frame, err := b.buildWindowFunctionFrame(ctx, spec, orderBy)
		if err != nil {
			return nil, nil, err
		}

		window := LogicalWindow{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Frame:       frame,
		}.Init(b.ctx, b.getSelectOffset())
		window.names = make([]*types.FieldName, np.Schema().Len())
		copy(window.names, np.OutputNames())
		schema := np.Schema().Clone()
		descs := make([]*aggregation.WindowFuncDesc, 0, len(funcs))
		preArgs := 0
		for _, windowFunc := range funcs {
			desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFunc.F, args[preArgs:preArgs+len(windowFunc.Args)])
			if err != nil {
				return nil, nil, err
			}
			if desc == nil {
				return nil, nil, ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFunc.F))
			}
			preArgs += len(windowFunc.Args)
			desc.WrapCastForAggArgs(b.ctx)
			descs = append(descs, desc)
			windowMap[windowFunc] = schema.Len()
			schema.Append(&expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  desc.RetTp,
			})
			window.names = append(window.names, types.EmptyName)
		}
		window.WindowFuncDescs = descs
		window.SetChildren(np)
		window.SetSchema(schema)
		p = window
	}
	return p, windowMap, nil
}

// checkOriginWindowFuncs checks the validity for original window specifications for a group of functions.
// Because the grouped specification is different from them, we should especially check them before build window frame.
func (b *PlanBuilder) checkOriginWindowFuncs(funcs []*ast.WindowFuncExpr, orderByItems []property.SortItem) error {
	for _, f := range funcs {
		if f.IgnoreNull {
			return ErrNotSupportedYet.GenWithStackByArgs("IGNORE NULLS")
		}
		if f.Distinct {
			return ErrNotSupportedYet.GenWithStackByArgs("<window function>(DISTINCT ..)")
		}
		if f.FromLast {
			return ErrNotSupportedYet.GenWithStackByArgs("FROM LAST")
		}
		spec := &f.Spec
		if f.Spec.Name.L != "" {
			spec = b.windowSpecs[f.Spec.Name.L]
		}
		if err := b.checkOriginWindowSpec(spec, orderByItems); err != nil {
			return err
		}
	}
	return nil
}

// checkOriginWindowSpec checks the validity for given window specification.
func (b *PlanBuilder) checkOriginWindowSpec(spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if spec.Frame == nil {
		return nil
	}
	if spec.Frame.Type == ast.Groups {
		return ErrNotSupportedYet.GenWithStackByArgs("GROUPS")
	}
	start, end := spec.Frame.Extent.Start, spec.Frame.Extent.End
	if start.Type == ast.Following && start.UnBounded {
		return ErrWindowFrameStartIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if end.Type == ast.Preceding && end.UnBounded {
		return ErrWindowFrameEndIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if start.Type == ast.Following && (end.Type == ast.Preceding || end.Type == ast.CurrentRow) {
		return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if (start.Type == ast.Following || start.Type == ast.CurrentRow) && end.Type == ast.Preceding {
		return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}

	err := b.checkOriginWindowFrameBound(&start, spec, orderByItems)
	if err != nil {
		return err
	}
	err = b.checkOriginWindowFrameBound(&end, spec, orderByItems)
	if err != nil {
		return err
	}
	return nil
}

func (b *PlanBuilder) checkOriginWindowFrameBound(bound *ast.FrameBound, spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if bound.Type == ast.CurrentRow || bound.UnBounded {
		return nil
	}

	frameType := spec.Frame.Type
	if frameType == ast.Rows {
		if bound.Unit != ast.TimeUnitInvalid {
			return ErrWindowRowsIntervalUse.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		_, isNull, isExpectedType := getUintFromNode(b.ctx, bound.Expr)
		if isNull || !isExpectedType {
			return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		return nil
	}

	if len(orderByItems) != 1 {
		return ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	orderItemType := orderByItems[0].Col.RetType.Tp
	isNumeric, isTemporal := types.IsTypeNumeric(orderItemType), types.IsTypeTemporal(orderItemType)
	if !isNumeric && !isTemporal {
		return ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit != ast.TimeUnitInvalid && !isTemporal {
		return ErrWindowRangeFrameNumericType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit == ast.TimeUnitInvalid && !isNumeric {
		return ErrWindowRangeFrameTemporalType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	return nil
}

func extractWindowFuncs(fields []*ast.SelectField) []*ast.WindowFuncExpr {
	extractor := &WindowFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.windowFuncs
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MySQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is includes all partition rows, equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       or "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", which is the same as an empty frame.
	// https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// "RANGE/ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" is equivalent to empty frame.
	if needFrame && spec.Frame != nil &&
		spec.Frame.Extent.Start.UnBounded && spec.Frame.Extent.End.UnBounded {
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	if !needFrame {
		var updated bool
		newSpec := *spec

		// For functions that operate on the entire partition, the frame clause will be ignored.
		if spec.Frame != nil {
			specName := spec.Name.O
			b.ctx.GetSessionVars().StmtCtx.AppendNote(ErrWindowFunctionIgnoresFrame.GenWithStackByArgs(windowFuncName, getWindowName(specName)))
			newSpec.Frame = nil
			updated = true
		}
		if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
			useDefaultFrame, defaultFrame := aggregation.UseDefaultFrame(windowFuncName)
			if useDefaultFrame {
				newSpec.Frame = &defaultFrame
				updated = true
			}
		}
		if updated {
			return &newSpec, true
		}
	}
	return spec, false
}

// append ast.WindowSpec to []*ast.WindowSpec if absent
func appendIfAbsentWindowSpec(specs []*ast.WindowSpec, ns *ast.WindowSpec) []*ast.WindowSpec {
	for _, spec := range specs {
		if spec == ns {
			return specs
		}
	}
	return append(specs, ns)
}

func specEqual(s1, s2 *ast.WindowSpec) (equal bool, err error) {
	if (s1 == nil && s2 != nil) || (s1 != nil && s2 == nil) {
		return false, nil
	}
	var sb1, sb2 strings.Builder
	ctx1 := format.NewRestoreCtx(0, &sb1)
	ctx2 := format.NewRestoreCtx(0, &sb2)
	if err = s1.Restore(ctx1); err != nil {
		return
	}
	if err = s2.Restore(ctx2); err != nil {
		return
	}
	return sb1.String() == sb2.String(), nil
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, []*ast.WindowSpec, error) {
	// updatedSpecMap is used to handle the specifications that have frame clause changed.
	updatedSpecMap := make(map[string][]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	orderedSpec := make([]*ast.WindowSpec, 0, len(windowFuncs))
	for _, windowFunc := range windowFuncs {
		if windowFunc.Spec.Name.L == "" {
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					return nil, nil, ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					return nil, nil, err
				}
			}
			spec, _ = b.handleDefaultFrame(spec, windowFunc.F)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			continue
		}

		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			return nil, nil, ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		newSpec, updated := b.handleDefaultFrame(spec, windowFunc.F)
		if !updated {
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
		} else {
			var updatedSpec *ast.WindowSpec
			if _, ok := updatedSpecMap[name]; !ok {
				updatedSpecMap[name] = []*ast.WindowSpec{newSpec}
				updatedSpec = newSpec
			} else {
				for _, spec := range updatedSpecMap[name] {
					eq, err := specEqual(spec, newSpec)
					if err != nil {
						return nil, nil, err
					}
					if eq {
						updatedSpec = spec
						break
					}
				}
				if updatedSpec == nil {
					updatedSpec = newSpec
					updatedSpecMap[name] = append(updatedSpecMap[name], newSpec)
				}
			}
			groupedWindow[updatedSpec] = append(groupedWindow[updatedSpec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, updatedSpec)
		}
	}
	// Unused window specs should also be checked in b.buildWindowFunctions,
	// so we add them to `groupedWindow` with empty window functions.
	for _, spec := range b.windowSpecs {
		if _, ok := groupedWindow[spec]; !ok {
			if _, ok = updatedSpecMap[spec.Name.L]; !ok {
				groupedWindow[spec] = nil
				orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			}
		}
	}
	return groupedWindow, orderedSpec, nil
}

// resolveWindowSpec resolve window specifications for sql like `select ... from t window w1 as (w2), w2 as (partition by a)`.
// We need to resolve the referenced window to get the definition of current window spec.
func resolveWindowSpec(spec *ast.WindowSpec, specs map[string]*ast.WindowSpec, inStack map[string]bool) error {
	if inStack[spec.Name.L] {
		return errors.Trace(ErrWindowCircularityInWindowGraph)
	}
	if spec.Ref.L == "" {
		return nil
	}
	ref, ok := specs[spec.Ref.L]
	if !ok {
		return ErrWindowNoSuchWindow.GenWithStackByArgs(spec.Ref.O)
	}
	inStack[spec.Name.L] = true
	err := resolveWindowSpec(ref, specs, inStack)
	if err != nil {
		return err
	}
	inStack[spec.Name.L] = false
	return mergeWindowSpec(spec, ref)
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	if ref.Frame != nil {
		return ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	if spec.PartitionBy != nil {
		return errors.Trace(ErrWindowNoChildPartitioning)
	}
	if ref.OrderBy != nil {
		if spec.OrderBy != nil {
			return ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		spec.OrderBy = ref.OrderBy
	}
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = model.NewCIStr("")
	return nil
}

func buildWindowSpecs(specs []ast.WindowSpec) (map[string]*ast.WindowSpec, error) {
	specsMap := make(map[string]*ast.WindowSpec, len(specs))
	for _, spec := range specs {
		if _, ok := specsMap[spec.Name.L]; ok {
			return nil, ErrWindowDuplicateName.GenWithStackByArgs(spec.Name.O)
		}
		newSpec := spec
		specsMap[spec.Name.L] = &newSpec
	}
	inStack := make(map[string]bool, len(specs))
	for _, spec := range specsMap {
		err := resolveWindowSpec(spec, specsMap, inStack)
		if err != nil {
			return nil, err
		}
	}
	return specsMap, nil
}

// extractTableList extracts all the TableNames from node.
// If asName is true, extract AsName prior to OrigName.
// Privilege check should use OrigName, while expression may use AsName.
func extractTableList(node ast.ResultSetNode, input []*ast.TableName, asName bool) []*ast.TableName {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableList(x.Left, input, asName)
		input = extractTableList(x.Right, input, asName)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L != "" && asName {
				newTableName := *s
				newTableName.Name = x.AsName
				newTableName.Schema = model.NewCIStr("")
				input = append(input, &newTableName)
			} else {
				input = append(input, s)
			}
		} else if s, ok := x.Source.(*ast.SelectStmt); ok {
			if s.From != nil {
				var innerList []*ast.TableName
				innerList = extractTableList(s.From.TableRefs, innerList, asName)
				if len(innerList) > 0 {
					innerTableName := innerList[0]
					if x.AsName.L != "" && asName {
						newTableName := *innerList[0]
						newTableName.Name = x.AsName
						newTableName.Schema = model.NewCIStr("")
						innerTableName = &newTableName
					}
					input = append(input, innerTableName)
				}
			}
		}
	}
	return input
}

func collectTableName(node ast.ResultSetNode, updatableName *map[string]bool, info *map[string]*ast.TableName) {
	switch x := node.(type) {
	case *ast.Join:
		collectTableName(x.Left, updatableName, info)
		collectTableName(x.Right, updatableName, info)
	case *ast.TableSource:
		name := x.AsName.L
		var canUpdate bool
		var s *ast.TableName
		if s, canUpdate = x.Source.(*ast.TableName); canUpdate {
			if name == "" {
				name = s.Schema.L + "." + s.Name.L
				// it may be a CTE
				if s.Schema.L == "" {
					name = s.Name.L
				}
			}
			(*info)[name] = s
		}
		(*updatableName)[name] = canUpdate && s.Schema.L != ""
	}
}

// extractTableSourceAsNames extracts TableSource.AsNames from node.
// if onlySelectStmt is set to be true, only extracts AsNames when TableSource.Source.(type) == *ast.SelectStmt
func extractTableSourceAsNames(node ast.ResultSetNode, input []string, onlySelectStmt bool) []string {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableSourceAsNames(x.Left, input, onlySelectStmt)
		input = extractTableSourceAsNames(x.Right, input, onlySelectStmt)
	case *ast.TableSource:
		if _, ok := x.Source.(*ast.SelectStmt); !ok && onlySelectStmt {
			break
		}
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L == "" {
				input = append(input, s.Name.L)
				break
			}
		}
		input = append(input, x.AsName.L)
	}
	return input
}

func appendDynamicVisitInfo(vi []visitInfo, priv string, withGrant bool, err error) []visitInfo {
	return append(vi, visitInfo{
		privilege:        mysql.ExtendedPriv,
		dynamicPriv:      priv,
		dynamicWithGrant: withGrant,
		err:              err,
	})
}

func appendVisitInfo(vi []visitInfo, priv mysql.PrivilegeType, db, tbl, col string, err error) []visitInfo {
	return append(vi, visitInfo{
		privilege: priv,
		db:        db,
		table:     tbl,
		column:    col,
		err:       err,
	})
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}

// containDifferentJoinTypes checks whether `preferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	inlMask := preferRightAsINLJInner ^ preferLeftAsINLJInner
	inlhjMask := preferRightAsINLHJInner ^ preferLeftAsINLHJInner
	inlmjMask := preferRightAsINLMJInner ^ preferLeftAsINLMJInner

	mask := inlMask ^ inlhjMask ^ inlmjMask
	onesCount := bits.OnesCount(preferJoinType & ^mask)
	if onesCount > 1 || onesCount == 1 && preferJoinType&mask > 0 {
		return true
	}

	cnt := 0
	if preferJoinType&inlMask > 0 {
		cnt++
	}
	if preferJoinType&inlhjMask > 0 {
		cnt++
	}
	if preferJoinType&inlmjMask > 0 {
		cnt++
	}
	return cnt > 1
}

func (b *PlanBuilder) buildCte(ctx context.Context, cte *ast.CommonTableExpression, isRecursive bool) (p LogicalPlan, err error) {
	if isRecursive {
		// buildingRecursivePartForCTE likes a stack. We save it before building a recursive CTE and restore it after building.
		// We need a stack because we need to handle the nested recursive CTE. And buildingRecursivePartForCTE indicates the innermost CTE.
		saveCheck := b.buildingRecursivePartForCTE
		b.buildingRecursivePartForCTE = false
		err = b.buildRecursiveCTE(ctx, cte.Query.Query)
		if err != nil {
			return nil, err
		}
		b.buildingRecursivePartForCTE = saveCheck
	} else {
		p, err = b.buildResultSetNode(ctx, cte.Query.Query)
		if err != nil {
			return nil, err
		}

		p, err = b.adjustCTEPlanOutputName(p, cte)
		if err != nil {
			return nil, err
		}

		cInfo := b.outerCTEs[len(b.outerCTEs)-1]
		cInfo.seedLP = p
	}
	return nil, nil
}

// buildRecursiveCTE handles the with clause `with recursive xxx as xx`.
func (b *PlanBuilder) buildRecursiveCTE(ctx context.Context, cte ast.ResultSetNode) error {
	cInfo := b.outerCTEs[len(b.outerCTEs)-1]
	switch x := (cte).(type) {
	case *ast.SetOprStmt:
		// 1. Handle the WITH clause if exists.
		if x.With != nil {
			l := len(b.outerCTEs)
			defer func() {
				b.outerCTEs = b.outerCTEs[:l]
				sw := x.With
				x.With = sw
			}()
			err := b.buildWith(ctx, x.With)
			if err != nil {
				return err
			}
		}
		// Set it to nil, so that when builds the seed part, it won't build again. Reset it in defer so that the AST doesn't change after this function.
		x.With = nil

		// 2. Build plans for each part of SetOprStmt.
		recursive := make([]LogicalPlan, 0)
		tmpAfterSetOptsForRecur := []*ast.SetOprType{nil}

		expectSeed := true
		for i := 0; i < len(x.SelectList.Selects); i++ {
			var p LogicalPlan
			var err error

			var afterOpr *ast.SetOprType
			switch y := x.SelectList.Selects[i].(type) {
			case *ast.SelectStmt:
				p, err = b.buildSelect(ctx, y)
				afterOpr = y.AfterSetOperator
			case *ast.SetOprSelectList:
				p, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: y, With: y.With})
				afterOpr = y.AfterSetOperator
			}

			if expectSeed {
				if cInfo.useRecursive {
					// 3. If it fail to build a plan, it may be the recursive part. Then we build the seed part plan, and rebuild it.
					if i == 0 {
						return ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
					}

					// It's the recursive part. Build the seed part, and build this recursive part again.
					// Before we build the seed part, do some checks.
					if x.OrderBy != nil {
						return ErrNotSupportedYet.GenWithStackByArgs("ORDER BY over UNION in recursive Common Table Expression")
					}
					// Limit clause is for the whole CTE instead of only for the seed part.
					oriLimit := x.Limit
					x.Limit = nil

					// Check union type.
					if afterOpr != nil {
						if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
							return ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between seed part and recursive part, hint: The operator between seed part and recursive part must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
						}
						cInfo.isDistinct = *afterOpr == ast.Union
					}

					expectSeed = false
					cInfo.useRecursive = false

					// Build seed part plan.
					saveSelect := x.SelectList.Selects
					x.SelectList.Selects = x.SelectList.Selects[:i]
					p, err = b.buildSetOpr(ctx, x)
					if err != nil {
						return err
					}
					x.SelectList.Selects = saveSelect
					p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
					if err != nil {
						return err
					}
					cInfo.seedLP = p

					// Rebuild the plan.
					i--
					b.buildingRecursivePartForCTE = true
					x.Limit = oriLimit
					continue
				}
				if err != nil {
					return err
				}
			} else {
				if err != nil {
					return err
				}
				if afterOpr != nil {
					if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
						return ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between recursive part's selects, hint: The operator between recursive part's selects must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
					}
				}
				if !cInfo.useRecursive {
					return ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
				}
				cInfo.useRecursive = false
				recursive = append(recursive, p)
				tmpAfterSetOptsForRecur = append(tmpAfterSetOptsForRecur, afterOpr)
			}
		}

		if len(recursive) == 0 {
			// In this case, even if SQL specifies "WITH RECURSIVE", the CTE is non-recursive.
			p, err := b.buildSetOpr(ctx, x)
			if err != nil {
				return err
			}
			p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
			if err != nil {
				return err
			}
			cInfo.seedLP = p
			return nil
		}

		// Build the recursive part's logical plan.
		recurPart, err := b.buildUnion(ctx, recursive, tmpAfterSetOptsForRecur)
		if err != nil {
			return err
		}
		recurPart, err = b.buildProjection4CTEUnion(ctx, cInfo.seedLP, recurPart)
		if err != nil {
			return err
		}
		// 4. Finally, we get the seed part plan and recursive part plan.
		cInfo.recurLP = recurPart
		// Only need to handle limit if x is SetOprStmt.
		if x.Limit != nil {
			limit, err := b.buildLimit(cInfo.seedLP, x.Limit)
			if err != nil {
				return err
			}
			limit.SetChildren(limit.Children()[:0]...)
			cInfo.limitLP = limit
		}
		return nil
	default:
		p, err := b.buildResultSetNode(ctx, x)
		if err != nil {
			// Refine the error message.
			if errors.ErrorEqual(err, ErrCTERecursiveRequiresNonRecursiveFirst) {
				err = ErrCTERecursiveRequiresUnion.GenWithStackByArgs(cInfo.def.Name.String())
			}
			return err
		}
		p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
		if err != nil {
			return err
		}
		cInfo.seedLP = p
		return nil
	}
}

func (b *PlanBuilder) adjustCTEPlanOutputName(p LogicalPlan, def *ast.CommonTableExpression) (LogicalPlan, error) {
	outPutNames := p.OutputNames()
	for _, name := range outPutNames {
		name.TblName = def.Name
		name.DBName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if len(def.ColNameList) > 0 {
		if len(def.ColNameList) != len(p.OutputNames()) {
			return nil, ddl.ErrViewWrongList
		}
		for i, n := range def.ColNameList {
			outPutNames[i].ColName = n
		}
	}
	p.SetOutputNames(outPutNames)
	return p, nil
}

// prepareCTECheckForSubQuery prepares the check that the recursive CTE can't be referenced in subQuery. It's used before building a subQuery.
// For example: with recursive cte(n) as (select 1 union select * from (select * from cte) c1) select * from cte;
func (b *PlanBuilder) prepareCTECheckForSubQuery() []*cteInfo {
	modifiedCTE := make([]*cteInfo, 0)
	for _, cte := range b.outerCTEs {
		if cte.isBuilding && !cte.enterSubquery {
			cte.enterSubquery = true
			modifiedCTE = append(modifiedCTE, cte)
		}
	}
	return modifiedCTE
}

// resetCTECheckForSubQuery resets the related variable. It's used after leaving a subQuery.
func resetCTECheckForSubQuery(ci []*cteInfo) {
	for _, cte := range ci {
		cte.enterSubquery = false
	}
}

// genCTETableNameForError find the nearest CTE name.
func (b *PlanBuilder) genCTETableNameForError() string {
	name := ""
	for i := len(b.outerCTEs) - 1; i >= 0; i-- {
		if b.outerCTEs[i].isBuilding {
			name = b.outerCTEs[i].def.Name.String()
			break
		}
	}
	return name
}

func (b *PlanBuilder) buildWith(ctx context.Context, w *ast.WithClause) error {
	// Check CTE name must be unique.
	nameMap := make(map[string]struct{})
	for _, cte := range w.CTEs {
		if _, ok := nameMap[cte.Name.L]; ok {
			return ErrNonUniqTable
		}
		nameMap[cte.Name.L] = struct{}{}
	}
	for _, cte := range w.CTEs {
		b.outerCTEs = append(b.outerCTEs, &cteInfo{def: cte, nonRecursive: !w.IsRecursive, isBuilding: true, storageID: b.allocIDForCTEStorage})
		b.allocIDForCTEStorage++
		saveFlag := b.optFlag
		// Init the flag to flagPrunColumns, otherwise it's missing.
		b.optFlag = flagPrunColumns
		_, err := b.buildCte(ctx, cte, w.IsRecursive)
		if err != nil {
			return err
		}
		b.outerCTEs[len(b.outerCTEs)-1].optFlag = b.optFlag
		b.outerCTEs[len(b.outerCTEs)-1].isBuilding = false
		b.optFlag = saveFlag
	}
	return nil
}

func (b *PlanBuilder) buildProjection4CTEUnion(ctx context.Context, seed LogicalPlan, recur LogicalPlan) (LogicalPlan, error) {
	if seed.Schema().Len() != recur.Schema().Len() {
		return nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
	}
	exprs := make([]expression.Expression, len(seed.Schema().Columns))
	resSchema := getResultCTESchema(seed.Schema(), b.ctx.GetSessionVars())
	for i, col := range recur.Schema().Columns {
		if !resSchema.Columns[i].RetType.Equal(col.RetType) {
			exprs[i] = expression.BuildCastFunction4Union(b.ctx, col, resSchema.Columns[i].RetType)
		} else {
			exprs[i] = col
		}
	}
	b.optFlag |= flagEliminateProjection
	proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(resSchema)
	proj.SetChildren(recur)
	return proj, nil
}

// The recursive part/CTE's schema is nullable, and the UID should be unique.
func getResultCTESchema(seedSchema *expression.Schema, svar *variable.SessionVars) *expression.Schema {
	res := seedSchema.Clone()
	for _, col := range res.Columns {
		col.RetType = col.RetType.Clone()
		col.UniqueID = svar.AllocPlanColumnID()
		col.RetType.Flag &= ^mysql.NotNullFlag
	}
	return res
}
