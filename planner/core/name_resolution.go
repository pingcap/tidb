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

package core

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/util/mathutil"
	"runtime/debug"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	fd "github.com/pingcap/tidb/planner/funcdep"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
)

/*************************************** DOC *************************************************
New name resolution require building agg out as we see(rewrite) it in analyzing phase, so we do
it

We expand 2 member function for planBuilder and 1 member function for expression rewriter,
extracting them all out here since they are all for the same purpose of new name resolution.

PlanBuilder.findNearestScope        --- used to find the nearest scope of (correlated) agg
PlanBuilder.findAggInScopeBackward  --- used to find the (correlated) agg from the in to out scope.
PlanBuilder.buildReservedCols       --- used for projection build reserved columns.

expressionRewriter.buildAggregationDesc



********************************************************************************************/

// nestingMap is used to check whether extracting correlated aggregate to outside eval context is valid.
type nestingMap uint64

type IDIntervalCache [2]int64

func (ids *IDIntervalCache) Get() int64 {
	ids[0]++
	Assert(ids[0] <= ids[1])
	return ids[0]
}

func (ids *IDIntervalCache) Valid() bool {
	return ids[0] != ids[1] && ids[0] < ids[1]
}

func NewIDIntervalCache() IDIntervalCache {
	res := [2]int64{-1, -1}
	return res
}

type preBuiltSubQueryCacheItem struct {
	// cache the basic sub-query plan, avoid building it again.
	// NOTE: in analyzing phase, we build every sub-query the first we meet it, caching the basic information down. (especially those columns with allocated unique id).
	// But we won't change the plan-tree (the handling logic of rewriter in analyzing will always ignore the generated new np)
	//
	// For example, Given an old plan like:
	//
	//      apply
	//      /   \
	//    agg   t1 (sub-query)         // Old Tree
	//    /
	//   t
	//
	// Since by now, the analyzing phase is carried out after buildTableRef, after which the basic p is as datasource `t`. If we change the plan tree when we rewrite a
	// sub-query the first time we meet it at analyzing phase, the plan-tree will be changed like this:
	//
	//      agg
	//      /
	//    apply                        // Shouldn't change the tree
	//    /   \
	//   t    t1 (sub-query)
	//
	// Which will cause a lot of basic test cases failure, besides that's not reasonable either. So in analyzing phase we just built the sub-query & agg out, record them
	// down in corresponding scope which will be used in later in formal plan building phase, along with correlated column within them.
	//
	p LogicalPlan
	// see comment in handleInSubquery, indicating the allocated column id interval [min, max).
	InJoinColumnIDs IDIntervalCache
	// see comment in handleExistSubquery, indicating the allocated column id interval [min, max).
	existJoinColumnIDs IDIntervalCache
	// see comment in handleCompareSubquery, indicating the allocated column id interval [min, max).
	compareJoinColumnIDs IDIntervalCache
}

// ScopeSchema is used to resolve column and register basic agg in analyzing phase.
type ScopeSchema struct {
	scopeSchema  *expression.Schema
	scopeNames   []*types.FieldName
	selectFields []*ast.SelectField

	// agg group utility elements, aggFuncs are mapped to aggColumn and aggFuncExpr, and aggMapper is used to fast locate the offset in
	// aggFuncs/aggColumn when an address of *AggregateFuncExpr is given.
	aggFuncs     []*aggregation.AggFuncDesc
	aggColumn    []*expression.Column
	astAggFunc   []*ast.AggregateFuncExpr
	aggMapper    map[*ast.AggregateFuncExpr]int
	groupByItems []expression.Expression

	// win group utility elements, windowFuncs are mapped to windowColumn, and windowMapper is used to fast locate the
	// offset in windowFuncs/windowColumn when an address of *WindowFuncExpr is given.
	// todo：change window function resolution as the newer one. 2022/06/30.
	// windowFuncs  []*windowFuncs
	// windowColumn []*expression.Column
	// windowMapper map[*ast.WindowFuncExpr]int

	// we should build the projection expr out and map them to a specific column in analyzing phase.
	// otherwise, cases like: select (select 1) as a from dual order by a & select 1 as a, (select t.a) from t
	// they can't resolve themselves (mainly a) from the base from scope columns here.
	projExpr   []expression.Expression
	projColumn []*expression.Column
	projNames  []*types.FieldName

	mapScalarSubQueryByAddr map[*ast.SubqueryExpr]*preBuiltSubQueryCacheItem

	// since tidb's plan building is from bottom up，every operator only output what they care about.
	// generally speaking, current building process is from data source -> selection -> agg -> projection -> distinct -> order by...
	// actually, data source & selection & agg won't impose any elimination on the source schema, which means the predicate on
	// them can refer to any columns that derived from the base table. While after projection, the schema is projected, which causing
	// the predicate in order by can't refer to some base col if you don't notify projection operator to keep it for you in advance.
	//
	// that's why we analyze these clauses first before we build it, and using this fields to tell projection to reserve it for later use.
	reservedCols                []*expression.Column
	reservedColsNames           []*types.FieldName
	reservedCorrelatedCols      []*expression.CorrelatedColumn
	projectionCol4CorrelatedAgg map[int64]*expression.Column
}

// FullScopeSchema returns current scope's basic column plus register agg columns.
func (s *ScopeSchema) FullScopeSchema() *expression.Schema {
	ss := s.scopeSchema.Clone()
	ss.Append(s.aggColumn...)
	return ss
}

// GetCol is used to get column by unique id.
func (s *ScopeSchema) GetCol(id int64) (*expression.Column, int) {
	for i, one := range s.scopeSchema.Columns {
		if one.UniqueID == id {
			return one, i
		}
	}
	return nil, -1
}

// ColSet returns columns' int unique id set.
func (s *ScopeSchema) ColSet() *fd.FastIntSet {
	var colSet fd.FastIntSet
	if s.scopeSchema == nil {
		return &colSet
	}
	for _, c := range s.scopeSchema.Columns {
		colSet.Insert(int(c.UniqueID))
	}
	return &colSet
}

// ReservedColSet returns reserved columns' int unique id set.
func (s *ScopeSchema) ReservedColSet() *fd.FastIntSet {
	var colSet fd.FastIntSet
	for _, c := range s.reservedCols {
		colSet.Insert(int(c.UniqueID))
	}
	return &colSet
}

// Add adds scope columns with scope names.
func (s *ScopeSchema) Add(schema *expression.Schema, names []*types.FieldName) {
	if schema == nil || len(schema.Columns) == 0 {
		return
	}
	// assertion: len(schema) = len(names)
	if s.scopeSchema == nil {
		s.scopeSchema = expression.NewSchema(make([]*expression.Column, 0, schema.Len())...)
	}
	if s.scopeNames == nil {
		s.scopeNames = make([]*types.FieldName, 0, schema.Len())
	}
	colSet := s.ColSet()
	for i, col := range schema.Columns {
		if !colSet.Has(int(col.UniqueID)) {
			s.scopeSchema.Columns = append(s.scopeSchema.Columns, schema.Columns[i])
			s.scopeNames = append(s.scopeNames, names[i])
		}
	}
}

// AddColumn adds a scope column with a scope name.
func (s *ScopeSchema) AddColumn(column *expression.Column, name *types.FieldName) {
	if column == nil || name == nil {
		return
	}
	if s.scopeSchema == nil {
		s.scopeSchema = expression.NewSchema(make([]*expression.Column, 0, 1)...)
	}
	if s.scopeNames == nil {
		s.scopeNames = make([]*types.FieldName, 0, 1)
	}
	colSet := s.ColSet()
	if !colSet.Has(int(column.UniqueID)) {
		s.scopeSchema.Columns = append(s.scopeSchema.Columns, column)
		s.scopeNames = append(s.scopeNames, name)
	}
}

// AddReservedCols add the correlated column from sub-query to its nearest outer scope.
// eg: select (select (select sum(t.a+t1.a) from t t2) from t as t1) from t;
// since the sum will be evaluated in the second select block (the nearest scope), we
// only need to reserve t1.a on second scope, while t.a will be passed in outer apply operator.
//
// ps: the innermost sub-query only refer the projected column as sum(t.a+t1.a) from second
// select block rather than do the aggregation by itself.
func (s *ScopeSchema) AddReservedCols(corCols []*expression.CorrelatedColumn, cnames []*types.FieldName, cols []*expression.Column, names []*types.FieldName) {
	aggColumnCovered := func(id int64) bool {
		for _, aggCol := range s.aggColumn {
			if aggCol.UniqueID == id {
				return true
			}
		}
		return false
	}
	// reservation comes from sub-query's correlated column.
	for i, cc := range corCols {
		// background: in analyzing phase, the col set of scope contains all columns it can see.
		// make sure the correlated column is from this scope, and hasn't been added before.
		// reserved col may be the origin base col or be the currently seen appended agg col.
		if (s.ColSet().Has(int(cc.Column.UniqueID)) || aggColumnCovered(cc.Column.UniqueID)) && !s.ReservedColSet().Has(int(cc.Column.UniqueID)) {
			s.reservedCols = append(s.reservedCols, &cc.Column)
			s.reservedColsNames = append(s.reservedColsNames, cnames[i])
		}
	}
	// reservation comes from current scope's having or order-by clause.
	for i, c := range cols {
		// reserved col may be the origin base col or be the currently seen appended agg col.
		if (s.ColSet().Has(int(c.UniqueID)) || aggColumnCovered(c.UniqueID)) && !s.ReservedColSet().Has(int(c.UniqueID)) {
			s.reservedCols = append(s.reservedCols, c)
			s.reservedColsNames = append(s.reservedColsNames, names[i])
		}
	}
}

// AddReservedCorrelatedCols is used to adapt for old logic of correlated agg from having and order-by runtime.
// eg: select (select 1 from t order by count(n.a) limit 1) from t n;
// this case will keep correlated column of what count(n.a) is projected in the outer scope, for example named X here.
// it will project correlated(X) as Y in the inner query, then latter having or order by clause can refer it from p.schema.
// essentially, we can directly use correlated(X) in having and order by item, while that needs much detail detections.
func (s *ScopeSchema) AddReservedCorrelatedCols(col *expression.Column) {
	for _, cCol := range s.reservedCorrelatedCols {
		if cCol.UniqueID == col.UniqueID {
			// already added.
			return
		}
	}
	s.reservedCorrelatedCols = append(s.reservedCorrelatedCols, &expression.CorrelatedColumn{Column: *col, Data: new(types.Datum)})
}

// findNearestScope is called when the builder finishes building an aggregate
// function info.
//
// In addition, endAggFunc finds the correct scope level, given that the aggregate
// references the columns in cols. The reference scope is the one closest to the
// current scope which contains at least one of the variables referenced by the
// aggregate (or the current scope if the aggregate references no variables).
//
// return args: int indicates the index of outer scope. bool indicates whether it's in the current scope.
func (b *PlanBuilder) findNearestScope(corCols []*expression.CorrelatedColumn, cols []*expression.Column) (int, bool) {
	var colSet fd.FastIntSet
	for _, cc := range corCols {
		colSet.Insert(int(cc.Column.UniqueID))
	}
	for _, c := range cols {
		colSet.Insert(int(c.UniqueID))
	}
	// no specific columns or they are all covered by current scope.
	if len(corCols) == 0 || colSet.Len() == 0 || b.curScope.ColSet().Intersects(colSet) {
		return -1, true
	}
	// find the nearest outer scope that has something overlapped.
	for i := len(b.outerScopes) - 1; i >= 0; i-- {
		scope := b.outerScopes[i]
		if scope.ColSet().Intersects(colSet) {
			return i, false
		}
	}
	return -1, false
}

// findAggInScopeBackward search the agg place backward in the current scope and outer scope mainly used in building phase (
// distinguished with analyzing phase). Because correlated agg des will be built and appended to the nearest correspondent scope
// in analyzing phase.
func (b *PlanBuilder) findAggInScopeBackward(agg *ast.AggregateFuncExpr) (*aggregation.AggFuncDesc, expression.Expression) {
	// search the current scope.
	if offset, ok := b.curScope.aggMapper[agg]; ok {
		return b.curScope.aggFuncs[offset], b.curScope.aggColumn[offset]
	}

	// find the nearest outer scope if agg is built and appended in there.
	for _, scope := range b.outerScopes {
		if offset, ok := scope.aggMapper[agg]; ok {
			referredCol := scope.aggColumn[offset]
			// for clause like having and order by, we can not directly refer the correlated column from outer scope. In old runtime,
			// it will project the correlated agg in projection, and refer the projected new column in having and order by clause.
			// if b.curClause == havingClause || b.curClause == orderByClause {
			// 	// refer the projected column instead.
			//	return scope.aggFuncs[offset], b.curScope.projectionCol4CorrelatedAgg[referredCol.UniqueID]
			//}
			return scope.aggFuncs[offset], &expression.CorrelatedColumn{Column: *referredCol, Data: new(types.Datum)}
		}
	}
	return nil, nil
}

// initAggregateFunctionCheck is used to init basic element for latter aggregate function check on recursive ascent.
func (er *expressionRewriter) initAggregateFunctionCheck(agg *ast.AggregateFuncExpr) error {
	// we do the quick check of this in the recursive decent.
	if uint64(er.b.allowAggFunc) == 0 {
		// no way for local aggregate or correlated aggregate, error it.
		return ErrInvalidGroupFuncUse
	}
	// MaxAggLevel indicates the outermost select should in which this aggregate should be evaluated.
	// Specially MaxAggLevel = -1 means this aggregate didn't ref any outer column (we couldn't detect
	// its outermost eval select block), like count(*), which probably can be evaluated locally.
	//
	// BaseQueryBlock indicates the nest_level of current select clause, it's same as the length of
	// stack of schema/scope. Notes: It's different from selectBlockOffset.
	//
	// AggQueryBlock = -1 means this aggregate should be evaluated at local context.
	// AggQueryBlock = 0 means this aggregate should be evaluated at main select block.
	// AggQueryBlock = x means this aggregate should be evaluated at sub-x select block.
	if agg.Extra == nil {
		agg.Extra = &struct {
			InAggFunc       *ast.AggregateFuncExpr
			MaxAggLevel     int
			MaxAggFuncLevel int
			AggQueryBlock   int
			BaseQueryBlock  int
			InsideAggregate []*ast.AggregateFuncExpr
		}{MaxAggLevel: -1, MaxAggFuncLevel: -1, AggQueryBlock: -1, BaseQueryBlock: len(er.b.outerScopes)}
	}
	// agg.Extra.InAggFunc keep track of previous agg in current agg context.
	// agg   (    agg   (    agg))
	//  ^        |  ^         |
	//  +--------+  +---------+
	// er.curAggFunc only keep the newer agg when recursive descent and be restored when recursive ascent.
	agg.Extra.InAggFunc = er.b.inAggFunc
	er.b.inAggFunc = agg

	// adjustment to old logic of GROUP_CONCAT function.
	if agg.Order != nil {
		trueArgs := agg.Args[:len(agg.Args)-1] // the last argument is SEPARATOR, remote it.
		resolver := &aggOrderByResolver{
			ctx:  er.b.ctx,
			args: trueArgs,
		}
		for i, byItem := range agg.Order.Items {
			resolver.exprDepth = 0
			resolver.err = nil
			retExpr, _ := byItem.Expr.Accept(resolver)
			if resolver.err != nil {
				return errors.Trace(resolver.err)
			}
			agg.Order.Items[i].Expr = retExpr.(ast.ExprNode)
		}
	}
	return nil
}

// checkAggregateFunction is used to check whether an aggregate is allowed in given scope's clause context.
func (er *expressionRewriter) checkAggregateFunction(agg *ast.AggregateFuncExpr) error {
	// on the rewrite ascent here, we are sure that every column in the aggregate function are successfully resolved, and
	// underling which we recorded the max max_aggr_level for every aggregate in their resolution.
	allowAggFunc := er.b.allowAggFunc
	nestLevelMap := 1 << len(er.b.outerScopes)
	baseQueryBlock := len(er.b.outerScopes)
	if strings.HasPrefix(er.p.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "SELECT SUM( (SELECT COUNT(a) FROM t2) ) FROM t1") {
		fmt.Println(1)
	}

	/*
	   max_aggr_level is the level of the innermost qualifying query block of
	   the column references of this set function. If the set function contains
	   no column references, max_aggr_level is -1. eg: count(*)
	   max_aggr_level cannot be greater than nest level of the current query block.
	*/
	maxAggLevel := agg.Extra.MaxAggLevel
	Assert(maxAggLevel <= len(er.b.outerScopes))

	if maxAggLevel == baseQueryBlock {
		/*
		   The function must be aggregated in the current query block,
		   and it must be referred within a clause where it is valid
		   (ie. HAVING clause, ORDER BY clause or SELECT list)
		*/
		if (allowAggFunc & nestingMap(nestLevelMap)) != 0 {
			// local evaluate.
			agg.Extra.AggQueryBlock = baseQueryBlock
		}
	} else if maxAggLevel >= 0 || (allowAggFunc&nestingMap(nestLevelMap)) == 0 {
		// Look for an outer query block where the set function should be
		// aggregated. If it finds such a query block, then aggr_query_block is set
		// to this query block.
		//
		// In the process of traversing outward, we always guarantee that outer scope's
		// nest_level >= max_aggr_level, which is what the definition of max_aggr_level call for.
		//
		// if len(er.b.outerScopes)=1, here it means we are in the sub-query's context and current context is not valid,
		// and we should enumerate outer nest_levels left.
		for i := len(er.b.outerScopes) - 1; i >= 0 && i >= maxAggLevel; i-- {
			// here `i` is used as equivalence of outer select query's nest_level. (stack depth = select block's nest_level).
			// in ith bit of the allowAggFunc map, detecting whether it allows an agg in its clause context.
			if allowAggFunc&nestingMap(1<<i) != 0 {
				agg.Extra.AggQueryBlock = i
			}
		}
	} else {
		// maxAggLevel = -1 < 0
		//
		// Agg function without column reference is aggregated in innermost query,
		// without any validation.
		agg.Extra.AggQueryBlock = baseQueryBlock
	}

	// if we couldn't find an outer eval select block and current eval context is allowed, let's try to eval this agg locally if no ANSI mode is set.
	if agg.Extra.AggQueryBlock == -1 && (allowAggFunc&nestingMap(nestLevelMap)) != 0 && !er.b.ctx.GetSessionVars().SQLMode.HasANSIMode() {
		agg.Extra.AggQueryBlock = baseQueryBlock
	}

	// At this place a query block where the set function is to be aggregated
	// has been found and is assigned to AggQueryBlock, or AggQueryBlock is -1
	// to indicate an invalid set function.
	//
	// Additionally, check whether possible nested set functions are acceptable
	// here: their aggregation level must be greater than this set function's
	// MaxAggFuncLevel which is collected from bottom args-aggregate up.
	//
	// If there is no args-aggregate, the MaxAggFuncLevel should be equal to -1 by default.
	if agg.Extra.AggQueryBlock == -1 || agg.Extra.AggQueryBlock <= agg.Extra.MaxAggFuncLevel {
		return errors.Trace(ErrInvalidGroupFuncUse)
	}

	// 这个地方相当是在递归上升的过程中，在所有嵌套里层的 agg 都已经找好自己的层次之后，进行的判断。
	// 如果发现一个 agg 的参数有另一个 agg，并且这两个 agg eval 在同一个层次，那么就在这里直接报错。关键点是需要一个 AggQueryBlock 来记录 eval 的层次。
	// Aggregate nested case:
	// in the recursive ascent, the inside aggregate (if any) has formed their AggQueryBlock in the way up to here.
	// judge current aggregate's AggQueryBlock and inside aggregate's AggQueryBlock, if they are in the same level, error it.
	for i := 0; i < len(agg.Extra.InsideAggregate); i++ {
		if agg.Extra.AggQueryBlock == agg.Extra.InsideAggregate[i].Extra.AggQueryBlock {
			return errors.Trace(ErrInvalidGroupFuncUse)
		}
	}

	// 这个地方相当于是将当前这个 agg 加入的外层 select block 的待 build 的 agg list 当中，不过他们是用一个 list 来维护的。
	// 这个 reference_by 应该是他们内部查询所引用的指针。
	// Try to build aggregate desc out according to their AggQueryBlock computed from logic above when all the check has passed.
	if err := er.buildAggregationDesc(er.ctx, er.p, agg); err != nil {
		return errors.Trace(err)
	}

	if agg.Extra.AggQueryBlock != baseQueryBlock { // 如果当前 agg eval 到外围
		// referenced_by[0] = ref;
		//
		// Here means this aggregate should be evaluated in the outer scope, adding this aggregate to the corresponding outer scope.
		// todo: add this aggregate to outer scope.
		// er.err = er.buildAggregationDesc(er.ctx, er.p, agg)

		/*
			Mark subqueries as containing set function all the way up to the
			set function's aggregation query block.
			Note that we must not mark the Item of calculation context itself
			because with_sum_func on the aggregation query block is already set above.

			has_aggregation() being set for an Item means that this Item refers
			(somewhere in it, e.g. one of its arguments if it's a function) directly
			or indirectly to a set function that is calculated in a
			context "outside" of the Item (e.g. in the current or outer query block).

			with_sum_func being set for a query block means that this query block
			has set functions directly referenced (i.e. not through a subquery).

			If, going up, we meet a derived table, we do nothing special for it:
			it doesn't need this information.
		*/
		// 这个地方是标记 local select block 跟外围 block 之间有这样的 agg 关系
		// for (Query_block *sl = base_query_block; sl && sl != aggr_query_block; sl = sl->outer_query_block()) {
		// 	if (sl- > master_query_expression()- > item)
		// 		sl- > master_query_expression()- > item- > set_aggregation();
		// }
		// base_query_block->mark_as_dependent(aggr_query_block, true);
	}

	if agg.Extra.InAggFunc != nil {
		/*
			  		If the set function is nested，adjust the value of
				    max_sum_func_level for the containing set function.
			   		We take into account only set functions that are to be aggregated on
			   		the same level or outer compared to the nest level of the containing
			   		set function.
			   		But we must always pass up the max_sum_func_level because it is
			   		the maximum nest level of all directly and indirectly contained
			   		set functions. We must do that even for set functions that are
			   		aggregated inside of their containing set function's nest level
			   		because the containing function may contain another containing
			   		function that is to be aggregated outside or on the same level
			   		as its parent's nest level.
					nest_level from 0     1    2    3    4 ...   N
			                                   |    |
			                                   |    |
			                               count(y+count(x))
			   		由于 count(x) 直接绑定在内部 subq 了，所以他的 max_sum_func_level = 0；
		*/
		// 这个部分是解的一开始我们的这种 case ：select (select sum((select count(a)))) from t;
		// 在递归上升过程中，处理这个地方 count(a) 时候，count 的 eval 层次是 0， 因为他在 in_sum_func 之内，
		// 所以这里需要调整 sum 的 eval 层次要 >= 0. sum 本身没有 ref 列，所以其自身的 max_aggr_level =-1，
		// 默认 eval 在本此层次 = 1 之内。
		// eg: select (select sum((select count(a)))) from t
		// since count(a) is evaluated in nest level = 0 and sum itself is in select block = 1, here
		// we set sum agg's MaxAggFuncLevel = count agg's eval select block = 0 when handling count here,
		// which signals that we couldn't throw sum agg to a more outer scope (x < MaxAggFuncLevel) to
		// do the evaluation when handling sum agg.
		if agg.Extra.InAggFunc.Extra.BaseQueryBlock >= agg.Extra.AggQueryBlock {
			agg.Extra.InAggFunc.Extra.MaxAggFuncLevel = mathutil.Max(agg.Extra.InAggFunc.Extra.MaxAggFuncLevel, agg.Extra.AggQueryBlock)
		}
		// take current agg's MaxAggFuncLevel into consideration in case of tree or more nested aggregate.
		agg.Extra.InAggFunc.Extra.MaxAggFuncLevel = mathutil.Max(agg.Extra.InAggFunc.Extra.MaxAggFuncLevel, agg.Extra.MaxAggFuncLevel)

		// if we are inside another aggregate, record current aggregate
		er.b.inAggFunc.Extra.InsideAggregate = append(er.b.inAggFunc.Extra.InsideAggregate, agg)
	}
	er.b.inAggFunc = agg.Extra.InAggFunc
	return nil
}

// detachCorrelationInScope will change correlated column in specific scope as normal columns.
// for example: select (select count(a)) from t.
// after the rewrite in the sub-query with sub-p here, the args in agg will be rewritten as correlated columns.
// since we ganna append this agg des to outer scope, the args will have a new relative scope, so that's why
// we need to change the correlated column here.

// buildAggregationDesc will build aggInfo out and append it to corresponding scope when encountering it as you go.
// then all of this will be fetched and reused when building Aggregation in corresponding scope building phase.
func (er *expressionRewriter) buildAggregationDesc(ctx context.Context, p LogicalPlan, aggFunc *ast.AggregateFuncExpr) error {
	b := er.b
	corCols := make([]*expression.CorrelatedColumn, 0, 1)
	cols := make([]*expression.Column, 0, 1)
	newArgList := make([]expression.Expression, len(aggFunc.Args), len(aggFunc.Args))
	lenOrderItems := 0
	if aggFunc.Order != nil {
		lenOrderItems = len(aggFunc.Order.Items)
	}
	newOrderByItems := make([]*util.ByItems, lenOrderItems, lenOrderItems)

	if strings.HasPrefix(er.b.ctx.GetSessionVars().StmtCtx.OriginalSQL, "explain format = 'brief' select (select sum(count(a))) from t") {
		fmt.Println(1)
	}
	// for case like: select (select sum((select count(a)))) from t;
	// when analyzing sum(x) in second select block, it has no current scope columns by now. While when the
	// agg args evaluated from its sub-query, we need to find where it comes from.
	//
	// the sub-query will link correlated column for the outermost scope, and when the scalar sub-query is
	// returned, it will generate a new column on behalf of count(a) inside its projection. This column is
	// NOT from the current scope or outer scope, or we can say current scope is changed due to a new apply
	// is generated. Since a column from a sub-query has already been checked, we can just keep it.
	//
	// So we here just return current scope when we found len(corCols) == 0.

	// once the agg referred only outer columns, we should move this aggFunc to corresponding outer scope.

	nestLevel := aggFunc.Extra.AggQueryBlock
	// the full schema stack should be (er.b.outerScopes + er.b+currentScope).
	// when nestLevel=1, it means we are in second select block of two. Under which
	// len(er.b.outerScopes) = 1 + er.b+currentScope = 1 == 2.
	//
	// For simplicity, we use len(er.b.outerScopes) as the nestLevel in fixInAggFuncMaxLevel.
	// So nestLevel == len(er.b.outerScopes) means in current scope, otherwise, nestLevel means
	// the scopeIndex of er.b.outerScopes.
	inCurrentScope, scopeIndex := nestLevel == len(er.b.outerScopes), -1
	if !inCurrentScope {
		scopeIndex = nestLevel
	}

	if strings.HasPrefix(p.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "select (select 1 from t order by count(n.a) limit 1) from t n") {
		fmt.Println(1)
	}

	// Since the er.stack has rewritten and keep agg's args and order items before, reuse it.
	if aggFunc.Order != nil {
		// use the reversed the enter-stack order.
		for i := len(aggFunc.Order.Items) - 1; i >= 0; i-- {
			newByItem := er.ctxStack[len(er.ctxStack)-1]
			er.ctxStackPop(1)
			if inCurrentScope {
				newByItem = newByItem.Decorrelate(b.curScope.FullScopeSchema())
			} else {
				newByItem = newByItem.Decorrelate(b.outerScopes[scopeIndex].FullScopeSchema())
			}
			corCols = append(corCols, expression.ExtractCorColumns(newByItem)...)
			cols = append(cols, expression.ExtractColumns(newByItem)...)
			newOrderByItems[i] = &util.ByItems{Expr: newByItem, Desc: aggFunc.Order.Items[i].Desc}
		}
	}
	// rewrite the agg function's args according current scope.
	// use the reversed the enter-stack order.
	for i := len(aggFunc.Args) - 1; i >= 0; i-- {
		newArg := er.ctxStack[len(er.ctxStack)-1]
		er.ctxStackPop(1)
		if inCurrentScope {
			newArg = newArg.Decorrelate(b.curScope.FullScopeSchema())
		} else {
			newArg = newArg.Decorrelate(b.outerScopes[scopeIndex].FullScopeSchema())
		}
		corCols = append(corCols, expression.ExtractCorColumns(newArg)...)
		cols = append(cols, expression.ExtractColumns(newArg)...)
		newArgList[i] = newArg
	}
	// build agg desc.
	newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
	if err != nil {
		return err
	}
	newFunc.OrderByItems = newOrderByItems
	// check whether there is already an equivalence agg there. Refer it if any. (acting like aggMapper before)
	if inCurrentScope {
		for i, oldFunc := range b.curScope.aggFuncs {
			if oldFunc.Equal(b.ctx, newFunc) {
				// this below case's two agg have the different pointer address, while they are mapped to same col.
				// case like: select a, b, avg(c) from t group by a, b, c having (avg(c) > 3);
				//                          |                                        |
				//                          V                                        V
				//  b.curScope.aggMapper[address1]            b.curScope.aggMapper[address2]
				//                          +-----------+----------------------------+
				//                                      |
				//  b.curScope.aggFunc[offset]     <----+  (same offset)
				//  b.curScope.aggColumn[offset]
				b.curScope.aggMapper[aggFunc] = i
				er.ctxStackAppend(b.curScope.aggColumn[i], types.EmptyName)
				return nil
			}
		}
	} else {
		for i, oldFunc := range b.outerScopes[scopeIndex].aggFuncs {
			if oldFunc.Equal(b.ctx, newFunc) {
				// same as comments above, while the scope is outer scope.
				b.outerScopes[scopeIndex].aggMapper[aggFunc] = i
				er.ctxStackAppend(&expression.CorrelatedColumn{Column: *b.outerScopes[scopeIndex].aggColumn[i], Data: new(types.Datum)}, types.EmptyName)
				return nil
			}
		}
	}

	// When comes to here, it means this aggregate is a brand new one.
	column := expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  newFunc.RetTp,
	}
	if inCurrentScope {
		b.curScope.aggFuncs = append(b.curScope.aggFuncs, newFunc)
		b.curScope.aggColumn = append(b.curScope.aggColumn, &column)
		b.curScope.aggMapper[aggFunc] = len(b.curScope.aggFuncs) - 1
		b.curScope.astAggFunc = append(b.curScope.astAggFunc, aggFunc)
		// explain select (select count(a + n.a) from t) from t n;
		// if it's in the having or orderby of outer clause, select n.b from t n order by (select count(a + n.a) from t);
		// we should keep n.a in main select block even count(a + n.a) is evaluated in current scope.
		// b.curScope.AddReservedCols(corCols, cols)
	} else {
		b.outerScopes[scopeIndex].aggFuncs = append(b.outerScopes[scopeIndex].aggFuncs, newFunc)
		b.outerScopes[scopeIndex].aggColumn = append(b.outerScopes[scopeIndex].aggColumn, &column)
		b.outerScopes[scopeIndex].aggMapper[aggFunc] = len(b.outerScopes[scopeIndex].aggFuncs) - 1
		b.outerScopes[scopeIndex].astAggFunc = append(b.outerScopes[scopeIndex].astAggFunc, aggFunc)
		// b.outerScopes[scopeIndex].AddReservedCols(corCols, cols)
	}
	// As for adapt to old runtime, for those correlated agg from having and order by, we should keep a position in projection.
	if b.curClause == havingClause || b.curClause == orderByClause {
		if !inCurrentScope {
			// occupy a position in projection of current scope.
			// b.curScope.AddReservedCorrelatedCols(&column)
			b.outerScopes[scopeIndex].AddReservedCols(nil, nil, []*expression.Column{&column}, []*types.FieldName{types.EmptyName})
		} else {
			// occupy a position in projection eg: select s.a from t3 s having sum(s.a); reserve sum(s.a) in projection.
			b.curScope.AddReservedCols(nil, nil, []*expression.Column{&column}, []*types.FieldName{types.EmptyName})
		}
	}

	if !inCurrentScope {
		er.ctxStackAppend(&expression.CorrelatedColumn{Column: column, Data: new(types.Datum)}, types.EmptyName)
	} else {
		er.ctxStackAppend(&column, types.EmptyName)
	}
	return nil
}

// analyzeProjectionList will build expr for each of select fields but ignore the plan tree change.
// we should build the projection expr out and map them to a specific column in analyzing phase.
// otherwise, cases like: select (select 1) as a from dual order by a & select 1 as a, (select t.a) from t
// they can't resolve themselves (mainly a) from the base from scope columns here.
func (b *PlanBuilder) analyzeProjectionList(ctx context.Context, p LogicalPlan, fields []*ast.SelectField) error {
	originClause := b.curClause
	b.curClause = fieldList
	// set allow eval-context of agg func/correlated agg to true.
	// eval-context of agg func/correlated agg only be allowed for 3 kind of clause:
	// select-list clause & having clause & order by clause.
	nestLevel := len(b.outerScopes)
	b.allowAggFunc |= 1 << nestLevel
	defer func() {
		b.curClause = originClause
		b.allowAggFunc &= ^(1 << nestLevel)
	}()
	for _, field := range fields {
		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schema columns.

		// analyzing projection phase, we don't consider window function field here. (do it like old projection does)
		if isWindowFuncField {
			expr := expression.NewZero()
			b.curScope.projExpr = append(b.curScope.projExpr, expr)
			col, name, err := b.buildProjectionField(ctx, p, field, expr)
			if err != nil {
				return err
			}
			b.curScope.projColumn = append(b.curScope.projColumn, col)
			b.curScope.projNames = append(b.curScope.projNames, name)
			continue
		}
		// analyzing projection phase, trying to build every field to a schema column, so did agg as we see it when rewriting.
		// we won't change the plan tree here, instead we allocate new col for every expr here for later analyzing reference.
		if strings.HasPrefix(b.ctx.GetSessionVars().StmtCtx.OriginalSQL, "explain select n.a as x, (select  count(a + x) from t) from t n") {
			fmt.Println(1)
		}
		newExpr, _, err := b.rewriteWithPreprocess(ctx, field.Expr, p, nil, nil, true, nil)
		if err != nil {
			return err
		}
		b.curScope.projExpr = append(b.curScope.projExpr, newExpr)
		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return err
		}
		b.curScope.projColumn = append(b.curScope.projColumn, col)
		b.curScope.projNames = append(b.curScope.projNames, name)
	}
	return nil
}

// Essentially, there is no necessity for analyzing selection, DBs like Postgre won't allow correlated agg in
// where clause, while MySQL does. eg: select (select 1 from t where count(n.a) > 1 limit 1) from t n;
// After we adopt the new aggregation building approach, for this case, we still need to recognize correlated
// agg out and append them to outer scope as well before we build selection officially.
func (b *PlanBuilder) analyzeSelectionList(ctx context.Context, p LogicalPlan, where ast.ExprNode) error {
	originClause := b.curClause
	b.curClause = whereClause
	defer func() {
		b.curClause = originClause
	}()
	// we won't change the plan tree here, and we won't allocate new col for every expr here either.
	if where == nil {
		return nil
	}
	_, _, err := b.rewriteWithPreprocess(ctx, where, p, nil, nil, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (b *PlanBuilder) analyzeGroupByList(ctx context.Context, P LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) error {
	originClause := b.curClause
	b.curClause = groupByClause
	defer func() {
		b.curClause = originClause
	}()
	// we won't change the plan tree here, and we won't allocate new col for every expr here either.
	if gby == nil {
		return nil
	}
	resolver := &gbyResolver{
		ctx:        b.ctx,
		fields:     fields,
		schema:     P.Schema(),
		names:      P.OutputNames(),
		skipAggMap: b.correlatedAggMapper,
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		resolver.exprDepth = 0
		resolver.isParam = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		itemExpr := retExpr.(ast.ExprNode)
		// change the item.Expr for later build group-by usage.
		item.Expr = itemExpr
		// ignore the np here, we won't want change the plan tree here.
		_, _, err := b.rewrite(ctx, itemExpr, P, nil, true)
		if err != nil {
			return err
		}
		// we collect the group by expression in building phase rather here.
	}
	return nil
}

func (b *PlanBuilder) analyzeHavingList(ctx context.Context, p LogicalPlan, having *ast.HavingClause) error {
	originClause := b.curClause
	b.curClause = havingClause
	// set allow eval-context of agg func/correlated agg to true.
	// eval-context of agg func/correlated agg only be allowed for 3 kind of clause:
	// select-list clause & having clause & order by clause.
	nestLevel := len(b.outerScopes)
	b.allowAggFunc |= 1 << nestLevel
	// set a flag to notify whether correlated column rewriting should register the
	// column to corresponding outer scope.
	b.needRegister |= 1 << nestLevel
	defer func() {
		b.curClause = originClause
		b.allowAggFunc &= ^(1 << nestLevel)
		b.needRegister &= ^(1 << nestLevel)
	}()
	// we won't change the plan tree here, and we won't allocate new col for every expr here either.
	if having == nil {
		return nil
	}
	_, _, err := b.rewriteWithPreprocess(ctx, having.Expr, p, nil, nil, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (b *PlanBuilder) analyzeOrderByList(ctx context.Context, p LogicalPlan, orderBy *ast.OrderByClause) error {
	originClause := b.curClause
	b.curClause = orderByClause
	// set allow eval-context of agg func/correlated agg to true.
	// eval-context of agg func/correlated agg only be allowed for 3 kind of clause:
	// select-list clause & having clause & order by clause.
	nestLevel := len(b.outerScopes)
	b.allowAggFunc |= 1 << nestLevel
	// set a flag to notify whether correlated column rewriting should register the
	// column to corresponding outer scope.
	b.needRegister |= 1 << nestLevel
	defer func() {
		b.curClause = originClause
		b.allowAggFunc &= ^(1 << nestLevel)
		b.needRegister &= ^(1 << nestLevel)
	}()
	if strings.HasPrefix(b.ctx.GetSessionVars().StmtCtx.OriginalSQL, "SELECT a, b AS c FROM t1 ORDER BY c") {
		fmt.Println(1)
	}
	// we won't change the plan tree here, and we won't allocate new col for every expr here either.
	if orderBy == nil {
		return nil
	}
	transformer := &itemTransformer{}
	for _, byItem := range orderBy.Items {
		newExpr, _ := byItem.Expr.Accept(transformer)
		byItem.Expr = newExpr.(ast.ExprNode)
		_, _, err := b.rewriteWithPreprocess(ctx, byItem.Expr, p, nil, nil, true, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *PlanBuilder) pushNewScope() {
	if b.curScope != nil {
		b.outerScopes = append(b.outerScopes, b.curScope)
	}
	b.allocateCurScope()
}

func (b *PlanBuilder) popOldScope() {
	if len(b.outerScopes) == 0 {
		b.curScope = nil
		return
	}
	b.curScope = b.outerScopes[len(b.outerScopes)-1]
	b.outerScopes = b.outerScopes[0 : len(b.outerScopes)-1]
}

func (b *PlanBuilder) allocateCurScope() {
	b.curScope = &ScopeSchema{
		scopeSchema:             expression.NewSchema(),
		scopeNames:              []*types.FieldName{},
		aggMapper:               make(map[*ast.AggregateFuncExpr]int, 1),
		mapScalarSubQueryByAddr: make(map[*ast.SubqueryExpr]*preBuiltSubQueryCacheItem, 1),
	}
}

func (b *PlanBuilder) cleanCurScope() {
	b.curScope = &ScopeSchema{
		aggMapper:               make(map[*ast.AggregateFuncExpr]int, 1),
		mapScalarSubQueryByAddr: make(map[*ast.SubqueryExpr]*preBuiltSubQueryCacheItem, 1),
	}
}

func (b *PlanBuilder) buildReservedCols(p LogicalPlan, proj *LogicalProjection, schema *expression.Schema, newNames []*types.FieldName) (*LogicalProjection, *expression.Schema, []*types.FieldName) {
	// build the reserved cols.
	// case: select t.b from t order by (select t.a +1 from s limit 1);
	// the sub-query in order by clause should notify outer projection it's ganna using correlated column t.a in advance.
	// this is built in outer projection and used in sub-query's resolveIndices.
	// Sort
	//   +
	//     Apply
	//       |                                                           +-------------------------------+
	//       + -> outer scope: base table: t,  projected column: t.b and | t.a (notified from sub-query) |
	//       |                                                           +-------------------------------+
	//       + -> inner scope: base table: s,  correlated column t.a + 1
	//
	for i, col := range b.curScope.reservedCols {
		// check whether projection has already built the column from select.fields directly.
		if schema.Contains(col) && newNames[schema.ColumnIndex(col)].String() == b.curScope.reservedColsNames[i].String() {
			continue
		}
		index := p.Schema().ColumnIndex(col)
		if index < 0 {
			panic("shouldn't be here")
		}
		// the col is in it's agg's schema
		proj.Exprs = append(proj.Exprs, p.Schema().Columns[index])
		schema.Append(p.Schema().Columns[index])
		newNames = append(newNames, p.OutputNames()[index])
		continue

	}
	// link the reserved correlated agg from outer.
	// case: select (select 1 from t order by count(n.a) limit 1) from t n;
	// outer scope: base table: n, correlated agg: count(t.a) ->  col: X
	//                                                                 ^
	// inner scope:                                              +-----+
	//             projection: <constant 1>  <new col 4 correlated X>
	//                                             ^
	//               order by: refer --------------+
	// clause behind projection such as having and order by should notify projection what you ganna use and
	// let projection keep it for you. (for example: correlated column and correlated agg)
	// this is built in sub-query's projection and used in sub-query's later clause.
	//
	b.curScope.projectionCol4CorrelatedAgg = make(map[int64]*expression.Column, len(b.curScope.reservedCorrelatedCols))
	for _, cCol := range b.curScope.reservedCorrelatedCols {
		newCol := b.allocateNewCol(cCol)
		proj.Exprs = append(proj.Exprs, cCol)
		schema.Append(newCol)
		newNames = append(newNames, types.EmptyName)
		// map the position after projection.
		b.curScope.projectionCol4CorrelatedAgg[cCol.UniqueID] = newCol
	}
	return proj, schema, newNames
}

func (b *PlanBuilder) buildGroupBy(ctx context.Context, p LogicalPlan, gby *ast.GroupByClause) (LogicalPlan, error) {
	originClause := b.curClause
	b.curClause = groupByClause
	defer func() {
		b.curClause = originClause
	}()
	// we won't change the plan tree here, and we won't allocate new col for every expr here either.
	if gby == nil {
		return p, nil
	}
	// todo: consider the parameter.
	for _, item := range gby.Items {
		// ignore the np here, we won't want change the plan tree here.
		expr, np, err := b.rewrite(ctx, item.Expr, p, nil, true)
		if err != nil {
			return nil, err
		}
		// change the plan tree here.
		p = np
		b.curScope.groupByItems = append(b.curScope.groupByItems, expr)
	}
	return p, nil
}

// buildAggregation4NNR build the aggregation from the new name resolution scope.
func (b *PlanBuilder) buildAggregation4NNR(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
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

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(b.curScope.aggFuncs))}.Init(b.ctx, b.getSelectOffset())
	if hint := b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(b.curScope.aggFuncs)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(b.curScope.aggFuncs)+p.Schema().Len())

	allAggsFirstRow := true
	//for i, aggFunc := range aggFuncList {
	//newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
	//for _, arg := range aggFunc.Args {
	//	newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
	//	if err != nil {
	//		return nil, nil, err
	//	}
	//	p = np
	//	newArgList = append(newArgList, newArg)
	//}
	//newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
	//if err != nil {
	//	return nil, nil, err
	//}
	//if newFunc.Name != ast.AggFuncFirstRow {
	//	allAggsFirstRow = false
	//}
	//if aggFunc.Order != nil {
	//	trueArgs := aggFunc.Args[:len(aggFunc.Args)-1] // the last argument is SEPARATOR, remote it.
	//	resolver := &aggOrderByResolver{
	//		ctx:  b.ctx,
	//		args: trueArgs,
	//	}
	//	for _, byItem := range aggFunc.Order.Items {
	//		resolver.exprDepth = 0
	//		resolver.err = nil
	//		retExpr, _ := byItem.Expr.Accept(resolver)
	//		if resolver.err != nil {
	//			return nil, nil, errors.Trace(resolver.err)
	//		}
	//		newByItem, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
	//		if err != nil {
	//			return nil, nil, err
	//		}
	//		p = np
	//		newFunc.OrderByItems = append(newFunc.OrderByItems, &util.ByItems{Expr: newByItem, Desc: byItem.Desc})
	//	}
	//}
	// combine identical aggregate functions
	//combined := false
	//for j := 0; j < i; j++ {
	//	oldFunc := plan4Agg.AggFuncs[aggIndexMap[j]]
	//	if oldFunc.Equal(b.ctx, newFunc) {
	//		aggIndexMap[i] = aggIndexMap[j]
	//		combined = true
	//		if _, ok := correlatedAggMap[aggFunc]; ok {
	//			if _, ok = b.correlatedAggMapper[aggFuncList[j]]; !ok {
	//				b.correlatedAggMapper[aggFuncList[j]] = &expression.CorrelatedColumn{
	//					Column: *schema4Agg.Columns[aggIndexMap[j]],
	//				}
	//			}
	//			b.correlatedAggMapper[aggFunc] = b.correlatedAggMapper[aggFuncList[j]]
	//		}
	//		break
	//	}
	//}
	// create new columns for aggregate functions which show up first
	//if !combined {
	//	position := len(plan4Agg.AggFuncs)
	//	aggIndexMap[i] = position
	//	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
	//	column := expression.Column{
	//		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
	//		RetType:  newFunc.RetTp,
	//	}
	//	schema4Agg.Append(&column)
	//	names = append(names, types.EmptyName)
	//	if _, ok := correlatedAggMap[aggFunc]; ok {
	//		b.correlatedAggMapper[aggFunc] = &expression.CorrelatedColumn{
	//			Column: column,
	//		}
	//	}
	//}
	//}
	// there may be some sub-query in agg args or agg order args. so we need to change the plan tree here.
	for _, astAgg := range b.curScope.astAggFunc {
		// there may be some sub-query in agg args.
		for _, arg := range astAgg.Args {
			_, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, err
			}
			p = np
		}
		// there may be some sub-query in agg order args.
		if astAgg.Order != nil {
			trueArgs := astAgg.Args[:len(astAgg.Args)-1] // the last argument is SEPARATOR, remote it.
			resolver := &aggOrderByResolver{
				ctx:  b.ctx,
				args: trueArgs,
			}
			for _, byItem := range astAgg.Order.Items {
				resolver.exprDepth = 0
				resolver.err = nil
				retExpr, _ := byItem.Expr.Accept(resolver)
				if resolver.err != nil {
					return nil, errors.Trace(resolver.err)
				}
				_, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
				if err != nil {
					return nil, err
				}
				p = np
			}
		}
	}
	// collect those pre-built agg des and form agg schema.
	for i, aggFuncDes := range b.curScope.aggFuncs {
		if aggFuncDes.Name != ast.AggFuncFirstRow {
			allAggsFirstRow = false
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggFuncDes)
		schema4Agg.Append(b.curScope.aggColumn[i])
		names = append(names, types.EmptyName)
	}
	// build the remained column schema as first row.
	for i, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
		names = append(names, p.OutputNames()[i])
	}

	// this is applied because some collapsed column in natural join should also be built here for latter usage.
	diff := b.curScope.scopeSchema.ColSet().Difference(*p.Schema().ColSet())
	if !diff.IsEmpty() {
		for uniqueID, ok := diff.Next(0); ok; uniqueID, ok = diff.Next(uniqueID + 1) {
			col, offset := b.curScope.GetCol(int64(uniqueID))
			if col == nil {
				panic("should be here")
			}
			newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
			if err != nil {
				return nil, err
			}
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			newCol, _ := col.Clone().(*expression.Column)
			newCol.RetType = newFunc.RetTp
			schema4Agg.Append(newCol)
			names = append(names, b.curScope.scopeNames[offset])
		}
	}

	//var (
	//	join            *LogicalJoin
	//	isJoin          bool
	//	isSelectionJoin bool
	//)
	//join, isJoin = p.(*LogicalJoin)
	//selection, isSelection := p.(*LogicalSelection)
	//if isSelection {
	//	join, isSelectionJoin = selection.children[0].(*LogicalJoin)
	//}
	//if (isJoin && join.fullSchema != nil) || (isSelectionJoin && join.fullSchema != nil) {
	//	for i, col := range join.fullSchema.Columns {
	//		if p.Schema().Contains(col) {
	//			continue
	//		}
	//		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
	//		if err != nil {
	//			return nil, nil, err
	//		}
	//		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
	//		newCol, _ := col.Clone().(*expression.Column)
	//		newCol.RetType = newFunc.RetTp
	//		schema4Agg.Append(newCol)
	//		names = append(names, join.fullNames[i])
	//	}
	//}
	hasGroupBy := len(b.curScope.groupByItems) > 0
	for i, aggFunc := range plan4Agg.AggFuncs {
		err := aggFunc.UpdateNotNullFlag4RetType(hasGroupBy, allAggsFirstRow)
		if err != nil {
			return nil, err
		}
		schema4Agg.Columns[i].RetType = aggFunc.RetTp
	}
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = b.curScope.groupByItems
	plan4Agg.SetSchema(schema4Agg)
	return plan4Agg, nil
}

func Assert(b bool) {
	if !b {
		debug.PrintStack()
		panic(b)
	}
}
