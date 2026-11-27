// Copyright 2026 PingCAP, Inc.
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

package cardinality

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
)

// VirtualColumnBaseColumn returns the only stored column that the generation
// expression of the virtual generated column col reads, when conditions on col
// can be estimated from that column's statistics. It returns nil otherwise:
// hidden columns of expression indexes are estimated with the index statistics,
// and a base column whose statistics values can't be restored for evaluation
// (see hasRestorableStatsValues) is of no use.
func VirtualColumnBaseColumn(col *expression.Column) *expression.Column {
	if col == nil || col.VirtualExpr == nil || col.IsHidden {
		return nil
	}
	cols := expression.ExtractColumnsMapFromExpressions(nil, col.VirtualExpr)
	if len(cols) != 1 {
		return nil
	}
	for _, base := range cols {
		if base.VirtualExpr != nil || !hasRestorableStatsValues(base.RetType) {
			return nil
		}
		return base
	}
	return nil
}

// virtualColumnStatsNode builds the StatsNode of the conditions in exprs that
// reference only col, a virtual generated column without statistics of its own,
// from the statistics of the one stored column its generation expression reads.
// It returns nil when col isn't such a column or the conditions can't be
// estimated that way.
//
// ANALYZE never collects statistics for virtual generated columns, so without
// this a condition like `lower_c = 'x'` (lower_c AS (lower(c))) gets the pseudo
// estimate, however skewed the data is. Instead, col is replaced by its
// generation expression cast to col's type - the value the column holds - and
// the conditions are evaluated together over c's TopN, histogram bounds and NULL
// count, the way GetSelectivityByFilter handles LIKE and REGEXP.
//
// The node competes with the index nodes in GetUsableSetsByGreedy, so an index
// with statistics whose ranges cover the conditions together with other columns
// is still preferred: its statistics hold the stored values of col.
func virtualColumnStatsNode(
	ctx planctx.PlanContext,
	coll *statistics.HistColl,
	col *expression.Column,
	exprs []expression.Expression,
) *StatsNode {
	if !canEstimateFromBaseColumn(ctx, coll, col) {
		return nil
	}
	var mask int64
	conds := make([]expression.Expression, 0, len(exprs))
	for i, expr := range exprs {
		cols := expression.ExtractColumnsMapFromExpressions(nil, expr)
		if _, ok := cols[col.UniqueID]; !ok || len(cols) != 1 {
			continue
		}
		mask |= 1 << uint64(i)
		conds = append(conds, expr)
	}
	if len(conds) == 0 {
		return nil
	}
	filter := substituteVirtualColumn(ctx, col, expression.ComposeCNFCondition(ctx.GetExprCtx(), conds...))
	if filter == nil {
		return nil
	}
	histPoints := 0
	if isCaseMapping(col.VirtualExpr) && !shortens(col.RetType, col.VirtualExpr) {
		histPoints = pointCount(col, conds)
	}
	ok, sel, err := getSelectivityByFilter(ctx, coll, filter, histPoints)
	if err != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError(
			"Error when estimating a virtual generated column from its base column: " + err.Error()))
		return nil
	}
	if !ok {
		return nil
	}
	return &StatsNode{Tp: ColType, ID: col.UniqueID, mask: mask, numCols: 1, Selectivity: sel}
}

// isCaseMapping reports whether expr is LOWER or UPPER of a column, possibly
// under casts that don't shorten the value. One value of such an expression
// usually comes from only the few case variants of a base value, so a point
// condition on it selects about as many base values as it has points. That
// doesn't hold in general: mod(c, 2) = 0 selects half of all values, and so
// does CAST(LOWER(c) AS CHAR(1)) = 'a' when half of the values start with 'a'.
// getSelectivityByFilter still checks the histogram bounds against this.
func isCaseMapping(expr expression.Expression) bool {
	for {
		f, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch f.FuncName.L {
		case ast.Cast:
			arg := f.GetArgs()[0]
			if shortens(f.RetType, arg) {
				return false
			}
			expr = arg
			continue
		case ast.Lower, ast.Lcase, ast.Upper, ast.Ucase:
			_, isCol := f.GetArgs()[0].(*expression.Column)
			return isCol
		}
		return false
	}
}

// shortens reports whether a value of expr can be cut short when held as type tp.
func shortens(tp *types.FieldType, expr expression.Expression) bool {
	flen := tp.GetFlen()
	return flen != types.UnspecifiedLength && flen < expr.GetType(nil).GetFlen()
}

// pointCount returns how many distinct values of col the conditions can select
// at most when one of them is `col = constant` or `col IN (constants)`, and 0
// otherwise. One value of col can come from several values of the base column
// (lower('a') = lower('A')), so this caps base-column values only approximately.
func pointCount(col *expression.Column, conds []expression.Expression) int {
	points := 0
	for _, cond := range conds {
		f, ok := cond.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		args := f.GetArgs()
		n := 0
		switch f.FuncName.L {
		case ast.EQ:
			if len(args) == 2 && (isColumnAndConstant(col, args[0], args[1]) || isColumnAndConstant(col, args[1], args[0])) {
				n = 1
			}
		case ast.In:
			if len(args) < 2 {
				continue
			}
			n = len(args) - 1
			for _, arg := range args[1:] {
				if _, isConst := arg.(*expression.Constant); !isConst {
					n = 0
					break
				}
			}
			if c, isCol := args[0].(*expression.Column); !isCol || !c.EqualColumn(col) {
				n = 0
			}
		}
		if n > 0 && (points == 0 || n < points) {
			points = n
		}
	}
	return points
}

func isColumnAndConstant(col *expression.Column, a, b expression.Expression) bool {
	c, isCol := a.(*expression.Column)
	_, isConst := b.(*expression.Constant)
	return isCol && isConst && c.EqualColumn(col)
}

// canEstimateFromBaseColumn reports whether conditions on col should be
// estimated from the statistics of the column its generation expression reads:
// col is a virtual generated column over a single stored column, it has no
// usable statistics of its own (neither column statistics nor an index that it
// leads), and the base column has full column statistics whose values can be
// restored for evaluation.
func canEstimateFromBaseColumn(ctx planctx.PlanContext, coll *statistics.HistColl, col *expression.Column) bool {
	base := VirtualColumnBaseColumn(col)
	if base == nil {
		return false
	}
	if colStats := coll.GetCol(col.UniqueID); colStats != nil && colStats.IsFullLoad() &&
		!statistics.ColumnStatsIsInvalid(colStats, ctx, coll, col.ID) {
		return false
	}
	if virtualColumnLeadsIndexWithStats(ctx, coll, col.UniqueID) {
		return false
	}
	// Only the base column's own statistics: the histogram bounds of an index
	// are encoded keys, not values of the column's type.
	isIndex, id := findAvailableStatsForCol(ctx, coll, base.UniqueID)
	return id >= 0 && !isIndex
}

// hasRestorableStatsValues reports whether the TopN and histogram values of a
// column of type tp decode to values of that type, so expressions over the
// column can be evaluated on them. Temporal values decode as packed integers,
// and ENUM, SET and BIT as their internal representation. FLOAT values decode
// as float64, which a FLOAT chunk column can't hold.
func hasRestorableStatsValues(tp *types.FieldType) bool {
	switch tp.GetType() {
	case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit, mysql.TypeFloat:
		return false
	}
	switch tp.EvalType() {
	case types.ETString, types.ETInt, types.ETReal, types.ETDecimal:
		return true
	}
	return false
}

// virtualColumnLeadsIndexWithStats reports whether an index with usable
// statistics has the column as its first column. Such an index estimates the
// column directly (see https://github.com/pingcap/tidb/issues/69134).
func virtualColumnLeadsIndexWithStats(ctx planctx.PlanContext, coll *statistics.HistColl, uniqueID int64) bool {
	for idxID, cols := range coll.Idx2ColUniqueIDs {
		if len(cols) == 0 || cols[0] != uniqueID {
			continue
		}
		if idxStats := coll.GetIdx(idxID); idxStats != nil && !statistics.IndexStatsIsInvalid(ctx, idxStats, coll, idxID) {
			return true
		}
	}
	return false
}

// substituteVirtualColumn replaces col in expr with its generation expression
// cast to col's type, which is the value the column holds. It returns nil when
// col can't be replaced everywhere.
func substituteVirtualColumn(ctx planctx.PlanContext, col *expression.Column, expr expression.Expression) expression.Expression {
	exprCtx := ctx.GetExprCtx()
	value := expression.BuildCastFunction(exprCtx, col.VirtualExpr.Clone(), col.RetType)
	if value == nil {
		return nil
	}
	hasFail, res := expression.ColumnSubstituteAll(exprCtx, expr.Clone(), expression.NewSchema(col), []expression.Expression{value})
	if hasFail {
		return nil
	}
	return res
}
