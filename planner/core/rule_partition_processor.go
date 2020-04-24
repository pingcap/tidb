// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/ranger"
)

// partitionProcessor rewrites the ast for table partition.
//
// create table t (id int) partition by range (id)
//   (partition p1 values less than (10),
//    partition p2 values less than (20),
//    partition p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
//
// partitionProcessor is here because it's easier to prune partition after predicate push down.
type partitionProcessor struct{}

func (s *partitionProcessor) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	return s.rewriteDataSource(lp)
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalPlan) (LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch lp.(type) {
	case *DataSource:
		return s.prune(lp.(*DataSource))
	case *LogicalUnionScan:
		us := lp.(*LogicalUnionScan)
		ds := us.Children()[0]
		ds, err := s.prune(ds.(*DataSource))
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*LogicalUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				usChild := LogicalUnionScan{}.Init(ua.ctx)
				usChild.conditions = us.conditions
				usChild.SetChildren(child)
				children = append(children, usChild)
			}
			ua.SetChildren(children...)
			return ua, nil
		}
		// Only one partition, no union all.
		us.SetChildren(ds)
		return us, nil
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				return nil, err
			}
			children[i] = newChild
		}
	}

	return lp, nil
}

// partitionTable is for those tables which implement partition.
type partitionTable interface {
	PartitionExpr(ctx sessionctx.Context, columns []*expression.Column) (*tables.PartitionExpr, error)
}

func generateHashPartitionExpr(t table.Table, ctx sessionctx.Context, columns []*expression.Column) (*tables.PartitionExpr, error) {
	tblInfo := t.Meta()
	pi := tblInfo.Partition
	var column *expression.Column
	schema := expression.NewSchema(columns...)
	exprs, err := expression.ParseSimpleExprsWithSchema(ctx, pi.Expr, schema)
	if err != nil {
		return nil, err
	}
	exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
	if col, ok := exprs[0].(*expression.Column); ok {
		column = col
	}
	return &tables.PartitionExpr{
		Column: column,
		Expr:   exprs[0],
		Ranges: nil,
	}, nil
}

func (s *partitionProcessor) pruneHashPartition(ds *DataSource, pi *model.PartitionInfo) (LogicalPlan, error) {
	pExpr, err := generateHashPartitionExpr(ds.table, ds.ctx, ds.TblCols)
	if err != nil {
		return nil, err
	}
	pe := pExpr.Expr
	filterConds := ds.allConds
	val, ok, hasConflict := expression.FastLocateHashPartition(ds.context(), filterConds, pe)
	if hasConflict {
		// For condition like `a = 1 and a = 5`, return TableDual directly.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.context())
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if ok {
		idx := math.Abs(val) % int64(pi.Num)
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), plancodec.TypeTableScan, &newDataSource)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[idx].ID
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.context(), ds.table.Meta(), pi.Definitions[idx].ID)
		pl := &newDataSource
		return pl, nil
	}

	return s.makeUnionAllChildren(ds, pi, fullRange(len(pi.Definitions)))
}

func (s *partitionProcessor) prune(ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}

	// Try to locate partition directly for hash partition.
	if pi.Type == model.PartitionTypeHash && len(ds.allConds) > 0 {
		return s.pruneHashPartition(ds, pi)
	}
	if pi.Type == model.PartitionTypeRange {
		if len(pi.Columns) == 0 {
			return s.pruneRangePartition(ds, pi)
		}
	}

	// TODO: Clean up the old code.
	partitionDefs := ds.table.Meta().Partition.Definitions
	filterConds := ds.allConds

	var partitionExprs []expression.Expression
	var col *expression.Column
	if table, ok := ds.table.(partitionTable); ok {
		pExpr, err := table.PartitionExpr(ds.ctx, ds.TblCols)
		if err != nil {
			return nil, err
		}
		partitionExprs = pExpr.Ranges
		col = pExpr.Column
	}
	if len(partitionExprs) == 0 {
		return nil, errors.New("partition expression missing")
	}

	// do preSolve with filter exprs for situations like
	// where c1 = 1 and c2 > c1 + 10 and c2 < c3 + 1 and c3 = c1 - 10
	// no need to do partition pruning work for "alwaysFalse" filter results
	if len(partitionExprs) > 3 {
		sctx := ds.context()
		filterConds = expression.PropagateConstant(sctx, filterConds)
		filterConds = solver.Solve(sctx, filterConds)
		alwaysFalse := false
		if len(filterConds) == 1 {
			// Constant false.
			if con, ok := filterConds[0].(*expression.Constant); ok && con.DeferredExpr == nil {
				ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
				if err == nil && ret == false {
					alwaysFalse = true
				}
			}
		}
		if alwaysFalse {
			tableDual := LogicalTableDual{RowCount: 0}.Init(ds.context())
			tableDual.schema = ds.Schema()
			return tableDual, nil
		}
	}

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to the filter conditions pushed to the DataSource.
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	for i, expr := range partitionExprs {
		// If the select condition would never be satisified, prune that partition.
		pruned, err := s.canBePruned(ds.context(), col, expr.Clone(), ds.allConds)
		if err != nil {
			return nil, err
		}
		if pruned {
			continue
		}
		// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
		if len(ds.partitionNames) != 0 {
			if !s.findByName(ds.partitionNames, partitionDefs[i].Name.L) {
				continue
			}
		}

		// Not a deep copy.
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), plancodec.TypeTableScan, &newDataSource)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[i].ID
		newDataSource.possibleAccessPaths = make([]*accessPath, len(ds.possibleAccessPaths))
		for i := range ds.possibleAccessPaths {
			newPath := *ds.possibleAccessPaths[i]
			newDataSource.possibleAccessPaths[i] = &newPath
		}
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.context(), ds.table.Meta(), pi.Definitions[i].ID)
		children = append(children, &newDataSource)
	}
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.context())
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := LogicalUnionAll{}.Init(ds.context())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema)
	return unionAll, nil
}

var solver = expression.NewPartitionPruneSolver()

// canBePruned checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then canBePrune returns true.
func (s *partitionProcessor) canBePruned(sctx sessionctx.Context, partCol *expression.Column, partExpr expression.Expression, filterExprs []expression.Expression) (bool, error) {
	conds := make([]expression.Expression, 0, 1+len(filterExprs))
	conds = append(conds, partExpr)
	conds = append(conds, filterExprs...)
	conds = expression.PropagateConstant(sctx, conds)
	conds = solver.Solve(sctx, conds)

	if len(conds) == 1 {
		// Constant false.
		if con, ok := conds[0].(*expression.Constant); ok && con.DeferredExpr == nil {
			ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
			if err == nil && ret == false {
				return true, nil
			}
		}
		// Not a constant false, but this is the only condition, it can't be pruned.
		return false, nil
	}

	// Calculates the column range to prune.
	if partCol == nil {
		// If partition column is nil, we can't calculate range, so we can't prune
		// partition by range.
		return false, nil
	}

	// TODO: Remove prune by calculating range. Current constraint propagate doesn't
	// handle the null condition, while calculate range can prune something like:
	// "select * from t where t is null"
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, []*expression.Column{partCol}, []int{types.UnspecifiedLength})
	if err != nil {
		return false, err
	}
	return len(res.Ranges) == 0, nil
}

// findByName checks whether object name exists in list.
func (s *partitionProcessor) findByName(partitionNames []model.CIStr, partitionName string) bool {
	for _, s := range partitionNames {
		if s.L == partitionName {
			return true
		}
	}
	return false
}

func (*partitionProcessor) name() string {
	return "partition_processor"
}

type lessThanData struct {
	data     []int64
	maxvalue bool
}

func (lt *lessThanData) length() int {
	return len(lt.data)
}

func (lt *lessThanData) compare(ith int, v int64, unsigned bool) int {
	if ith == len(lt.data)-1 {
		if lt.maxvalue {
			return 1
		}
	}

	if unsigned {
		return compareUnsigned(lt.data[ith], v)
	}

	switch {
	case lt.data[ith] > v:
		return 1
	case lt.data[ith] == v:
		return 0
	}
	return -1
}

func compareUnsigned(v1, v2 int64) int {
	switch {
	case uint64(v1) > uint64(v2):
		return 1
	case uint64(v1) == uint64(v2):
		return 0
	}
	return -1
}

// partitionRange represents [start, range)
type partitionRange struct {
	start int
	end   int
}

// partitionRangeOR represents OR(range1, range2, ...)
type partitionRangeOR []partitionRange

func fullRange(end int) partitionRangeOR {
	var reduceAllocation [3]partitionRange
	reduceAllocation[0] = partitionRange{0, end}
	return reduceAllocation[:1]
}

func (or partitionRangeOR) intersectionRange(start, end int) partitionRangeOR {
	// Let M = intersection, U = union, then
	// a M (b U c) == (a M b) U (a M c)
	ret := or[:0]
	for _, r1 := range or {
		newStart, newEnd := intersectionRange(r1.start, r1.end, start, end)
		// Exclude the empty one.
		if newEnd > newStart {
			ret = append(ret, partitionRange{newStart, newEnd})
		}
	}
	return ret
}

func (or partitionRangeOR) Len() int {
	return len(or)
}

func (or partitionRangeOR) Less(i, j int) bool {
	return or[i].start < or[j].start
}

func (or partitionRangeOR) Swap(i, j int) {
	or[i], or[j] = or[j], or[i]
}

func (or partitionRangeOR) union(x partitionRangeOR) partitionRangeOR {
	or = append(or, x...)
	return or.simplify()
}

func (or partitionRangeOR) simplify() partitionRangeOR {
	// Make the ranges order by start.
	sort.Sort(or)
	sorted := or

	// Iterate the sorted ranges, merge the adjacent two when their range overlap.
	// For example, [0, 1), [2, 7), [3, 5), ... => [0, 1), [2, 7) ...
	res := sorted[:1]
	for _, curr := range sorted[1:] {
		last := &res[len(res)-1]
		if curr.start > last.end {
			res = append(res, curr)
		} else {
			// Merge two.
			if curr.end > last.end {
				last.end = curr.end
			}
		}
	}
	return res
}

func (or partitionRangeOR) intersection(x partitionRangeOR) partitionRangeOR {
	if or.Len() == 1 {
		return x.intersectionRange(or[0].start, or[0].end)
	}
	if x.Len() == 1 {
		return or.intersectionRange(x[0].start, x[0].end)
	}

	// Rename to x, y where len(x) > len(y)
	var y partitionRangeOR
	if or.Len() > x.Len() {
		x, y = or, x
	} else {
		y = or
	}

	// (a U b) M (c U d) => (x M c) U (x M d), x = (a U b)
	res := make(partitionRangeOR, 0, len(y))
	for _, r := range y {
		// As intersectionRange modify the raw data, we have to make a copy.
		tmp := make(partitionRangeOR, len(x))
		copy(tmp, x)
		tmp = tmp.intersectionRange(r.start, r.end)
		res = append(res, tmp...)
	}
	return res.simplify()
}

// intersectionRange calculate the intersection of [start, end) and [newStart, newEnd)
func intersectionRange(start, end, newStart, newEnd int) (int, int) {
	var s, e int
	if start > newStart {
		s = start
	} else {
		s = newStart
	}

	if end < newEnd {
		e = end
	} else {
		e = newEnd
	}
	return s, e
}

func (s *partitionProcessor) pruneRangePartition(ds *DataSource, pi *model.PartitionInfo) (LogicalPlan, error) {
	pExpr, err := ds.table.(partitionTable).PartitionExpr(ds.ctx, nil)
	if err != nil {
		return nil, err
	}

	lessThan := lessThanData{
		data:     pExpr.LessThan,
		maxvalue: pExpr.MaxValue,
	}

	col, fn, err := makePartitionByFnCol(ds, pi.Expr)
	if err != nil {
		return nil, err
	}

	result := fullRange(len(pi.Definitions))
	if col != nil {
		result = partitionRangeForCNFExpr(ds.ctx, ds.allConds, lessThan, col, fn, result)
	}

	return s.makeUnionAllChildren(ds, pi, result)
}

type parse interface {
	ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, []error, error)
}

// makePartitionByFnCol extracts the column and function information in 'partition by ... fn(col)'.
func makePartitionByFnCol(ds *DataSource, partitionExpr string) (*expression.Column, *expression.ScalarFunction, error) {
	var err error
	var stmts []ast.StmtNode
	if p, ok := ds.context().(parse); ok {
		stmts, _, err = p.ParseSQL(context.Background(), "select "+partitionExpr, "", "")
	} else {
		stmts, _, err = parser.New().Parse("select "+partitionExpr, "", "")
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	fnCol := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	schema := expression.NewSchema(ds.TblCols...)
	partExpr, err := expression.RewriteSimpleExprWithSchema(ds.context(), fnCol, schema)
	if err != nil {
		return nil, nil, err
	}
	var col *expression.Column
	var fn *expression.ScalarFunction
	switch raw := partExpr.(type) {
	case *expression.ScalarFunction:
		// Special handle for floor(unix_timestamp(ts)) as partition expression.
		// This pattern is so common for timestamp(3) column as partition expression that it deserve an optimization.
		if raw.FuncName.L == ast.Floor {
			if ut, ok := raw.GetArgs()[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
				args := ut.GetArgs()
				if len(args) == 1 {
					if c, ok1 := args[0].(*expression.Column); ok1 {
						return c, raw, nil
					}
				}
			}
		}

		if _, ok := monotoneIncFuncs[raw.FuncName.L]; ok {
			fn = raw
			col = fn.GetArgs()[0].(*expression.Column)
		}
	case *expression.Column:
		col = raw
	}
	return col, fn, nil
}

func partitionRangeForCNFExpr(sctx sessionctx.Context, exprs []expression.Expression, lessThan lessThanData,
	col *expression.Column, partFn *expression.ScalarFunction, result partitionRangeOR) partitionRangeOR {
	for i := 0; i < len(exprs); i++ {
		result = partitionRangeForExpr(sctx, exprs[i], lessThan, col, partFn, result)
	}
	return result
}

// partitionRangeForExpr calculate the partitions for the expression.
func partitionRangeForExpr(sctx sessionctx.Context, expr expression.Expression, lessThan lessThanData,
	col *expression.Column, partFn *expression.ScalarFunction, result partitionRangeOR) partitionRangeOR {
	// Handle AND, OR respectively.
	if op, ok := expr.(*expression.ScalarFunction); ok {
		if op.FuncName.L == ast.LogicAnd {
			return partitionRangeForCNFExpr(sctx, op.GetArgs(), lessThan, col, partFn, result)
		} else if op.FuncName.L == ast.LogicOr {
			args := op.GetArgs()
			newRange := partitionRangeForOrExpr(sctx, args[0], args[1], lessThan, col, partFn)
			return result.intersection(newRange)
		}
	}

	// Handle a single expression.
	dataForPrune, ok := extractDataForPrune(sctx, expr, col, partFn)
	if !ok {
		// Can't prune, return the whole range.
		return result
	}
	unsigned := mysql.HasUnsignedFlag(col.RetType.Flag)
	start, end := pruneUseBinarySearch(lessThan, dataForPrune, unsigned)
	return result.intersectionRange(start, end)
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx sessionctx.Context, expr1, expr2 expression.Expression, lessThan lessThanData,
	col *expression.Column, partFn *expression.ScalarFunction) partitionRangeOR {
	tmp1 := partitionRangeForExpr(sctx, expr1, lessThan, col, partFn, fullRange(lessThan.length()))
	tmp2 := partitionRangeForExpr(sctx, expr2, lessThan, col, partFn, fullRange(lessThan.length()))
	return tmp1.union(tmp2)
}

// monotoneIncFuncs are those functions that for any x y, if x > y => f(x) > f(y)
var monotoneIncFuncs = map[string]struct{}{
	ast.ToDays:        {},
	ast.UnixTimestamp: {},
}

// f(x) op const, op is > = <
type dataForPrune struct {
	op string
	c  int64
}

// extractDataForPrune extracts data from the expression for pruning.
// The expression should have this form:  'f(x) op const', otherwise it can't be pruned.
func extractDataForPrune(sctx sessionctx.Context, expr expression.Expression, partCol *expression.Column, partFn *expression.ScalarFunction) (dataForPrune, bool) {
	var ret dataForPrune
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return ret, false
	}
	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
		ret.op = op.FuncName.L
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == partCol.ID {
			ret.op = ast.IsNull
			return ret, true
		}
		return ret, false
	default:
		return ret, false
	}

	var col *expression.Column
	var con *expression.Constant
	if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == partCol.ID {
		if arg1, ok := op.GetArgs()[1].(*expression.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*expression.Column); ok && arg0.ID == partCol.ID {
		if arg1, ok := op.GetArgs()[0].(*expression.Constant); ok {
			ret.op = opposite(ret.op)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return ret, false
	}

	// Current expression is 'col op const'
	var constExpr expression.Expression
	if partFn != nil {
		// If the partition expression is fn(col), change constExpr to fn(constExpr).
		constExpr = replaceColumnWithConst(partFn, con)

		// Sometimes we need to relax the condition, < to <=, > to >=.
		// For example, the following case doesn't hold:
		// col < '2020-02-11 17:34:11' => to_days(col) < to_days(2020-02-11 17:34:11)
		// The correct transform should be:
		// col < '2020-02-11 17:34:11' => to_days(col) <= to_days(2020-02-11 17:34:11)
		ret.op = relaxOP(ret.op)
	} else {
		// If the partition expression is col, use constExpr.
		constExpr = con
	}
	c, isNull, err := constExpr.EvalInt(sctx, chunk.Row{})
	if err == nil && !isNull {
		ret.c = c
		return ret, true
	}
	return ret, false
}

// replaceColumnWithConst change fn(col) to fn(const)
func replaceColumnWithConst(partFn *expression.ScalarFunction, con *expression.Constant) *expression.ScalarFunction {
	args := partFn.GetArgs()
	// The partition function may be floor(unix_timestamp(ts)) instead of a simple fn(col).
	if partFn.FuncName.L == ast.Floor {
		ut := args[0].(*expression.ScalarFunction)
		if ut.FuncName.L == ast.UnixTimestamp {
			args = ut.GetArgs()
			args[0] = con
			return partFn
		}
	}

	// No 'copy on write' for the expression here, this is a dangerous operation.
	args[0] = con
	return partFn

}

// opposite turns > to <, >= to <= and so on.
func opposite(op string) string {
	switch op {
	case ast.EQ:
		return ast.EQ
	case ast.LT:
		return ast.GT
	case ast.GT:
		return ast.LT
	case ast.LE:
		return ast.GE
	case ast.GE:
		return ast.LE
	}
	panic("invalid input parameter" + op)
}

// relaxOP relax the op > to >= and < to <=
// Sometime we need to relax the condition, for example:
// col < const => f(col) <= const
// datetime < 2020-02-11 16:18:42 => to_days(datetime) <= to_days(2020-02-11)
// We can't say:
// datetime < 2020-02-11 16:18:42 => to_days(datetime) < to_days(2020-02-11)
func relaxOP(op string) string {
	switch op {
	case ast.LT:
		return ast.LE
	case ast.GT:
		return ast.GE
	}
	return op
}

func pruneUseBinarySearch(lessThan lessThanData, data dataForPrune, unsigned bool) (start int, end int) {
	length := lessThan.length()
	switch data.op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 6)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c+1, unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = 0, pos+1
	case ast.IsNull:
		start, end = 0, 1
	default:
		start, end = 0, length
	}

	if end > length {
		end = length
	}
	return start, end
}

func (s *partitionProcessor) makeUnionAllChildren(ds *DataSource, pi *model.PartitionInfo, or partitionRangeOR) (LogicalPlan, error) {
	var children []LogicalPlan
	for _, r := range or {
		for i := r.start; i < r.end; i++ {
			// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
			if len(ds.partitionNames) != 0 {
				if !s.findByName(ds.partitionNames, pi.Definitions[i].Name.L) {
					continue
				}
			}

			// Not a deep copy.
			newDataSource := *ds
			newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), plancodec.TypeTableScan, &newDataSource)
			newDataSource.isPartition = true
			newDataSource.physicalTableID = pi.Definitions[i].ID
			newDataSource.possibleAccessPaths = make([]*accessPath, len(ds.possibleAccessPaths))
			for i := range ds.possibleAccessPaths {
				newPath := *ds.possibleAccessPaths[i]
				newDataSource.possibleAccessPaths[i] = &newPath
			}
			// There are many expression nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to update the references.
			newDataSource.id = ds.id
			newDataSource.statisticTable = getStatsTable(ds.context(), ds.table.Meta(), pi.Definitions[i].ID)
			children = append(children, &newDataSource)
		}
	}

	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.context())
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := LogicalUnionAll{}.Init(ds.context())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema)
	return unionAll, nil
}
