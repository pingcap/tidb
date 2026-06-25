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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/spatial"
)

// spatialKeyExprPrefix identifies the hidden generated column that backs a
// spatial index (its generated expression is tidb_spatial_key(<col>)).
const spatialKeyExprPrefix = ast.TiDBSpatialKey + "("

// defaultPlanarCovererForPlanner must match the coverer used by the
// tidb_spatial_key builtin, so query ranges and stored keys share a curve.
var defaultPlanarCovererForPlanner = spatial.NewDefaultPlanarCoverer()

// SpatialIndexResolver recognizes spatial predicates (ST_Distance within a
// radius, ST_Contains / ST_Within point-in-polygon) over a column that has a
// spatial index, and conjoins a covering-cell range predicate on the index's
// hidden generated column. The original spatial predicate is retained as the
// refine filter, so the index returns a candidate superset and the Selection
// above the scan makes the result exact. It must run before predicate pushdown.
type SpatialIndexResolver struct{}

// Name returns the name of this optimization rule.
func (*SpatialIndexResolver) Name() string {
	return "spatial_resolve_index"
}

// Optimize applies spatial-index resolution for WHERE predicates.
func (s *SpatialIndexResolver) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.SpatialFunctionIsUsed {
		return plan, false, nil
	}
	changed := s.resolve(plan, nil)
	return plan, changed, nil
}

// resolve walks the plan; for a DataSource directly under a LogicalSelection it
// tries to inject covering-cell predicates.
func (s *SpatialIndexResolver) resolve(plan base.LogicalPlan, parent base.LogicalPlan) bool {
	changed := false
	if ds, ok := plan.(*logicalop.DataSource); ok {
		if sel, ok := parent.(*logicalop.LogicalSelection); ok {
			changed = s.injectForDataSource(ds, sel) || changed
		}
		return changed
	}
	for _, child := range plan.Children() {
		changed = s.resolve(child, plan) || changed
	}
	return changed
}

// injectForDataSource scans the selection's conditions for spatial predicates on
// indexed columns and appends covering-range conditions on the hidden columns.
func (s *SpatialIndexResolver) injectForDataSource(ds *logicalop.DataSource, sel *logicalop.LogicalSelection) bool {
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
	exprCtx := ds.SCtx().GetExprCtx()
	added := make([]expression.Expression, 0, 1)

	for _, cond := range sel.Conditions {
		rect, geomColID, ok := recognizeSpatialPredicate(cond, evalCtx)
		if !ok {
			continue
		}
		hiddenCol := s.findSpatialHiddenColumn(ds, geomColID)
		if hiddenCol == nil {
			continue
		}
		ranges, err := defaultPlanarCovererForPlanner.CoverRect(0, rect)
		if err != nil || len(ranges) == 0 {
			continue
		}
		rangeExpr := buildCellRangeExpr(exprCtx, hiddenCol, ranges)
		if rangeExpr != nil {
			added = append(added, rangeExpr)
		}
	}

	if len(added) == 0 {
		return false
	}
	// Conjoin the covering predicates; keep the original spatial predicate(s)
	// for refinement.
	sel.Conditions = append(sel.Conditions, added...)
	return true
}

// findSpatialHiddenColumn returns the expression.Column of the hidden generated
// column of a spatial index whose source column is geomColID, or nil.
func (*SpatialIndexResolver) findSpatialHiddenColumn(ds *logicalop.DataSource, geomColID int64) *expression.Column {
	tblInfo := ds.TableInfo
	// Resolve the geometry column's name from its ID.
	var geomColName string
	for _, c := range tblInfo.Columns {
		if c.ID == geomColID {
			geomColName = c.Name.L
			break
		}
	}
	if geomColName == "" {
		return nil
	}
	for _, idx := range tblInfo.Indices {
		if !idx.IsPublic() || len(idx.Columns) != 1 {
			continue
		}
		hiddenColInfo := tblInfo.Columns[idx.Columns[0].Offset]
		if !isSpatialHiddenColumn(hiddenColInfo) {
			continue
		}
		if _, dep := hiddenColInfo.Dependences[geomColName]; !dep {
			continue
		}
		// The hidden column may have been pruned from the DataSource schema (it
		// is not referenced by the original query). Reuse it if present, else add
		// it back so the index becomes usable and the injected predicate has a
		// column to reference.
		for _, sc := range ds.Schema().Columns {
			if sc.ID == hiddenColInfo.ID {
				return sc
			}
		}
		newCol := &expression.Column{
			UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
			ID:       hiddenColInfo.ID,
			RetType:  hiddenColInfo.FieldType.Clone(),
			OrigName: hiddenColInfo.Name.L,
		}
		ds.Columns = append(ds.Columns, hiddenColInfo)
		ds.Schema().Append(newCol)
		return newCol
	}
	return nil
}

// isSpatialHiddenColumn reports whether a column is the hidden generated column
// produced for a spatial index.
func isSpatialHiddenColumn(col *model.ColumnInfo) bool {
	return col.Hidden && strings.HasPrefix(col.GeneratedExprString, spatialKeyExprPrefix)
}

// recognizeSpatialPredicate matches the supported spatial predicates and returns
// the query rectangle to cover and the indexed geometry column's ID.
func recognizeSpatialPredicate(cond expression.Expression, evalCtx expression.EvalContext) (spatial.Rect, int64, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return spatial.Rect{}, 0, false
	}
	switch sf.FuncName.L {
	case ast.LE, ast.LT:
		// ST_Distance(col, const_point) <= r
		return recognizeDistancePredicate(sf, evalCtx)
	case ast.StContains:
		// ST_Contains(const_poly, col)
		args := sf.GetArgs()
		return recognizeContainmentPredicate(args[0], args[1], evalCtx)
	case ast.StWithin:
		// ST_Within(col, const_poly)
		args := sf.GetArgs()
		return recognizeContainmentPredicate(args[1], args[0], evalCtx)
	}
	return spatial.Rect{}, 0, false
}

// recognizeDistancePredicate handles ST_Distance(col, const_point) <= r.
func recognizeDistancePredicate(cmp *expression.ScalarFunction, evalCtx expression.EvalContext) (spatial.Rect, int64, bool) {
	args := cmp.GetArgs()
	distSF, ok := args[0].(*expression.ScalarFunction)
	if !ok || distSF.FuncName.L != ast.StDistance {
		return spatial.Rect{}, 0, false
	}
	radius, ok := evalConstFloat(args[1], evalCtx)
	if !ok || radius < 0 {
		return spatial.Rect{}, 0, false
	}
	dargs := distSF.GetArgs()
	geomCol, constGeom, ok := splitColAndConst(dargs[0], dargs[1])
	if !ok {
		return spatial.Rect{}, 0, false
	}
	ewkb, ok := evalConstString(constGeom, evalCtx)
	if !ok {
		return spatial.Rect{}, 0, false
	}
	srid, x, y, err := expression.DecodeEWKBPoint(ewkb)
	if err != nil || srid != 0 {
		return spatial.Rect{}, 0, false
	}
	return spatial.Rect{MinX: x - radius, MinY: y - radius, MaxX: x + radius, MaxY: y + radius}, geomCol.ID, true
}

// recognizeContainmentPredicate handles a column / constant-polygon pair; the
// query rectangle is the polygon's bounding box.
func recognizeContainmentPredicate(colArg, polyArg expression.Expression, evalCtx expression.EvalContext) (spatial.Rect, int64, bool) {
	geomCol, ok := colArg.(*expression.Column)
	if !ok {
		return spatial.Rect{}, 0, false
	}
	ewkb, ok := evalConstString(polyArg, evalCtx)
	if !ok {
		return spatial.Rect{}, 0, false
	}
	srid, minX, minY, maxX, maxY, err := expression.EWKBBounds(ewkb)
	if err != nil || srid != 0 {
		return spatial.Rect{}, 0, false
	}
	return spatial.Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}, geomCol.ID, true
}

// splitColAndConst returns (column, otherArg) if exactly one of a, b is an
// expression.Column.
func splitColAndConst(a, b expression.Expression) (*expression.Column, expression.Expression, bool) {
	if col, ok := a.(*expression.Column); ok {
		if _, ok := b.(*expression.Column); ok {
			return nil, nil, false
		}
		return col, b, true
	}
	if col, ok := b.(*expression.Column); ok {
		return col, a, true
	}
	return nil, nil, false
}

func evalConstFloat(e expression.Expression, ctx expression.EvalContext) (float64, bool) {
	if !isFoldableConst(e) {
		return 0, false
	}
	d, err := e.Eval(ctx, chunk.Row{})
	if err != nil || d.IsNull() {
		return 0, false
	}
	f, err := d.ToFloat64(ctx.TypeCtx())
	if err != nil {
		return 0, false
	}
	return f, true
}

func evalConstString(e expression.Expression, ctx expression.EvalContext) (string, bool) {
	if !isFoldableConst(e) {
		return "", false
	}
	d, err := e.Eval(ctx, chunk.Row{})
	if err != nil || d.IsNull() {
		return "", false
	}
	return d.GetString(), true
}

// isFoldableConst reports whether the expression carries no column reference, so
// it can be evaluated at plan time to define the query region.
func isFoldableConst(e expression.Expression) bool {
	return len(expression.ExtractColumns(e)) == 0
}

// buildCellRangeExpr builds OR_i (h >= lo_i AND h <= hi_i) over the covering
// ranges, where h is the hidden spatial-key column.
func buildCellRangeExpr(ctx expression.BuildContext, h *expression.Column, ranges []spatial.CellKeyRange) expression.Expression {
	boolType := types.NewFieldType(mysql.TypeLonglong)
	orTerms := make([]expression.Expression, 0, len(ranges))
	for _, r := range ranges {
		lo := &expression.Constant{Value: types.NewBytesDatum([]byte(r.Lo)), RetType: h.RetType.Clone()}
		hi := &expression.Constant{Value: types.NewBytesDatum([]byte(r.Hi)), RetType: h.RetType.Clone()}
		geCond, err := expression.NewFunction(ctx, ast.GE, boolType, h, lo)
		if err != nil {
			return nil
		}
		leCond, err := expression.NewFunction(ctx, ast.LE, boolType, h, hi)
		if err != nil {
			return nil
		}
		andCond, err := expression.NewFunction(ctx, ast.LogicAnd, boolType, geCond, leCond)
		if err != nil {
			return nil
		}
		orTerms = append(orTerms, andCond)
	}
	if len(orTerms) == 1 {
		return orTerms[0]
	}
	combined := orTerms[0]
	for _, term := range orTerms[1:] {
		var err error
		combined, err = expression.NewFunction(ctx, ast.LogicOr, boolType, combined, term)
		if err != nil {
			return nil
		}
	}
	return combined
}
