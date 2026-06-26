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

	"github.com/pingcap/errors"
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
		req, ok := recognizeSpatialPredicate(cond, evalCtx)
		if !ok {
			continue
		}
		hiddenCol, params, ok := s.findSpatialHiddenColumn(ds, req.geomColID)
		if !ok {
			continue
		}
		// Only inject when the indexed column's SRID matches the covering scheme;
		// otherwise the stored keys (planar Morton vs S2) are in a different space
		// and the ranges would never match. The retained exact predicate still
		// produces the correct (full-scan) result.
		if !req.sridMatchesColumn(ds.TableInfo, req.geomColID) {
			continue
		}
		ranges, err := req.ranges(params)
		if err != nil || len(ranges) == 0 {
			continue
		}
		rangeExpr := buildCellRangeExpr(exprCtx, hiddenCol, ranges)
		if rangeExpr != nil {
			added = append(added, rangeExpr)
		}
		// MBR pruning: if the point index carries ST_X/ST_Y bbox columns, conjoin a
		// bounding-box-intersection filter on them. These are plain numeric
		// comparisons on index columns, so the optimizer applies them during the
		// index scan, before the table lookup — pruning the covering's false
		// positives without a random read. The exact predicate still refines.
		if rect, hasRect := req.bboxRect(); hasRect {
			if xCol, yCol, ok := s.findSpatialBBoxColumns(ds, req.geomColID); ok {
				added = append(added, buildBBoxConds(exprCtx, xCol, yCol, rect)...)
			}
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
// column of a spatial index whose source column is geomColID, together with the
// coverer params parsed from that column's generated expression (so the planner
// covers the query with the exact same cell scheme the rows were indexed under).
func (*SpatialIndexResolver) findSpatialHiddenColumn(ds *logicalop.DataSource, geomColID int64) (*expression.Column, spatial.PlanarParams, bool) {
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
		return nil, spatial.PlanarParams{}, false
	}
	for _, idx := range tblInfo.Indices {
		if !idx.IsPublic() || len(idx.Columns) == 0 {
			continue
		}
		// Find the cell-key hidden column by its tidb_spatial_key marker. Any
		// leading columns are ordinary prefix columns of a composite spatial index
		// (e.g. (tenant_id, position)); any trailing columns are the ST_X/ST_Y
		// bbox columns. The optimizer combines the prefix equality from the query
		// with the injected cell ranges on the cell-key column.
		var hiddenColInfo *model.ColumnInfo
		for _, ic := range idx.Columns {
			if c := tblInfo.Columns[ic.Offset]; isSpatialHiddenColumn(c) {
				hiddenColInfo = c
				break
			}
		}
		if hiddenColInfo == nil {
			continue
		}
		if _, dep := hiddenColInfo.Dependences[geomColName]; !dep {
			continue
		}
		params, err := parseSpatialKeyParams(hiddenColInfo.GeneratedExprString)
		if err != nil {
			continue
		}
		// The hidden column may have been pruned from the DataSource schema (it
		// is not referenced by the original query). Reuse it if present, else add
		// it back so the index becomes usable and the injected predicate has a
		// column to reference.
		for _, sc := range ds.Schema().Columns {
			if sc.ID == hiddenColInfo.ID {
				return sc, params, true
			}
		}
		// Build the column's virtual expression tidb_spatial_key(geomCol, ...).
		// Without it the planner treats the re-added column as real and may push
		// the covering-range filter to the coprocessor, which cannot evaluate
		// tidb_spatial_key on the (unstored) virtual column. Carrying VirtualExpr
		// keeps such filters at the TiDB root (SplitSelCondsWithVirtualColumn),
		// where the geometry column is available to compute the key.
		virtualExpr := buildSpatialKeyVirtualExpr(ds, geomColID, params)
		newCol := &expression.Column{
			UniqueID:    ds.SCtx().GetSessionVars().AllocPlanColumnID(),
			ID:          hiddenColInfo.ID,
			RetType:     hiddenColInfo.FieldType.Clone(),
			OrigName:    hiddenColInfo.Name.L,
			VirtualExpr: virtualExpr,
		}
		ds.Columns = append(ds.Columns, hiddenColInfo)
		ds.Schema().Append(newCol)
		return newCol, params, true
	}
	return nil, spatial.PlanarParams{}, false
}

// buildSpatialKeyVirtualExpr reconstructs the tidb_spatial_key(geomCol[, params])
// expression that generates the hidden column, so the re-added column can carry
// it as its VirtualExpr. Returns nil if the geometry column is not in the schema
// (in which case the column is still usable for an index scan).
func buildSpatialKeyVirtualExpr(ds *logicalop.DataSource, geomColID int64, params spatial.PlanarParams) expression.Expression {
	var geomCol *expression.Column
	for _, c := range ds.Schema().Columns {
		if c.ID == geomColID {
			geomCol = c
			break
		}
	}
	if geomCol == nil {
		return nil
	}
	ctx := ds.SCtx().GetExprCtx()
	args := []expression.Expression{geomCol}
	// Only emit the tuning args when they differ from the defaults, matching how
	// the DDL builds the expression (1-arg for defaults, 6-arg when tuned).
	if params != spatial.DefaultPlanarParams() {
		realTp := types.NewFieldType(mysql.TypeDouble)
		mk := func(v float64) expression.Expression {
			return &expression.Constant{Value: types.NewFloat64Datum(v), RetType: realTp}
		}
		args = append(args, mk(float64(params.Level)), mk(params.MinX), mk(params.MinY), mk(params.MaxX), mk(params.MaxY))
	}
	keyType := types.NewFieldType(mysql.TypeVarString)
	keyType.SetCharset("binary")
	keyType.SetCollate("binary")
	keyType.SetFlen(8)
	vexpr, err := expression.NewFunction(ctx, ast.TiDBSpatialKey, keyType, args...)
	if err != nil {
		return nil
	}
	return vexpr
}

// parseSpatialKeyParams reconstructs the coverer params from a hidden column's
// generated expression string, e.g. "tidb_spatial_key(`p`, 18, -180, -90, 180,
// 90)" or "tidb_spatial_key(`p`)" (defaults). The arguments contain no nested
// commas, so a top-level split is sufficient.
func parseSpatialKeyParams(genExpr string) (spatial.PlanarParams, error) {
	inner := strings.TrimSpace(genExpr)
	if !strings.HasPrefix(inner, spatialKeyExprPrefix) || !strings.HasSuffix(inner, ")") {
		return spatial.PlanarParams{}, errors.Errorf("not a tidb_spatial_key expression: %q", genExpr)
	}
	inner = inner[len(spatialKeyExprPrefix) : len(inner)-1]
	parts := strings.Split(inner, ",")
	// parts[0] is the geometry column reference; the rest are the cell params.
	return spatial.ParsePlanarParams(parts[1:])
}

// isSpatialHiddenColumn reports whether a column is the hidden generated column
// produced for a spatial index.
func isSpatialHiddenColumn(col *model.ColumnInfo) bool {
	return col.Hidden && strings.HasPrefix(col.GeneratedExprString, spatialKeyExprPrefix)
}

// coverKind selects the covering scheme for a recognized predicate.
type coverKind int

const (
	coverPlanarRect coverKind = iota // SRID 0: cover a planar rectangle
	coverSphereCap                   // SRID 4326: cover a spherical cap (ST_Distance_Sphere)
	coverLatLngRect                  // SRID 4326: cover a lat/long rectangle (containment)
)

// coverRequest is a recognized spatial predicate, holding everything needed to
// produce CellKey ranges for whichever scheme the indexed column uses.
type coverRequest struct {
	geomColID int64
	kind      coverKind
	rect      spatial.Rect // planar/lat-lng rectangle
	cx, cy, r float64      // spherical cap centre (lng,lat) and radius (metres)
}

// ranges builds the covering CellKey ranges for this request, using the planar
// params (from the index's generated expression) for the SRID 0 scheme.
func (q coverRequest) ranges(params spatial.PlanarParams) ([]spatial.CellKeyRange, error) {
	switch q.kind {
	case coverSphereCap:
		return spatial.CoverCapDegrees(q.cx, q.cy, q.r)
	case coverLatLngRect:
		return spatial.CoverLatLngRectDegrees(q.rect.MinX, q.rect.MinY, q.rect.MaxX, q.rect.MaxY)
	default:
		return params.Coverer().CoverRect(0, q.rect)
	}
}

// bboxRect returns the query's minimum bounding rectangle for MBR pruning, when
// one is available. The planar and lat/long requests already carry the query
// MBR in rect; the spherical-cap request does not (its bbox would need a
// lat/long projection of the cap), so it opts out of bbox pruning for now.
func (q coverRequest) bboxRect() (spatial.Rect, bool) {
	switch q.kind {
	case coverPlanarRect, coverLatLngRect:
		return q.rect, true
	default:
		return spatial.Rect{}, false
	}
}

// findSpatialBBoxColumns returns the hidden ST_X/ST_Y index columns generated for
// the point spatial index on geomColID, re-adding them to the DataSource schema
// if they were pruned (they carry no VirtualExpr: their values are materialized
// in the index, so the bbox filter can be applied at the index scan). Returns
// ok=false when the index has no bbox columns (e.g. a general-geometry MVI).
func (*SpatialIndexResolver) findSpatialBBoxColumns(ds *logicalop.DataSource, geomColID int64) (xCol, yCol *expression.Column, ok bool) {
	tblInfo := ds.TableInfo
	var geomColName string
	for _, c := range tblInfo.Columns {
		if c.ID == geomColID {
			geomColName = c.Name.L
			break
		}
	}
	if geomColName == "" {
		return nil, nil, false
	}
	var xInfo, yInfo *model.ColumnInfo
	for _, c := range tblInfo.Columns {
		if !c.Hidden {
			continue
		}
		if _, dep := c.Dependences[geomColName]; !dep {
			continue
		}
		switch {
		case strings.HasPrefix(c.GeneratedExprString, ast.StX+"("):
			xInfo = c
		case strings.HasPrefix(c.GeneratedExprString, ast.StY+"("):
			yInfo = c
		}
	}
	if xInfo == nil || yInfo == nil {
		return nil, nil, false
	}
	reAdd := func(info *model.ColumnInfo, fn string) *expression.Column {
		for _, sc := range ds.Schema().Columns {
			if sc.ID == info.ID {
				return sc
			}
		}
		newCol := &expression.Column{
			UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
			ID:       info.ID,
			RetType:  info.FieldType.Clone(),
			OrigName: info.Name.L,
			// ST_X/ST_Y are virtual expression-index columns (materialized only in
			// the index). Carry the generating expression so the value is available
			// when not read straight from the index.
			VirtualExpr: buildBBoxVirtualExpr(ds, geomColID, fn),
		}
		ds.Columns = append(ds.Columns, info)
		ds.Schema().Append(newCol)
		return newCol
	}
	return reAdd(xInfo, ast.StX), reAdd(yInfo, ast.StY), true
}

// buildBBoxVirtualExpr reconstructs ST_X(geomCol) / ST_Y(geomCol) for a re-added
// bbox column's VirtualExpr. Returns nil if the geometry column is not in the
// schema (the column is still usable straight from the index).
func buildBBoxVirtualExpr(ds *logicalop.DataSource, geomColID int64, fn string) expression.Expression {
	var geomCol *expression.Column
	for _, c := range ds.Schema().Columns {
		if c.ID == geomColID {
			geomCol = c
			break
		}
	}
	if geomCol == nil {
		return nil
	}
	retType := types.NewFieldType(mysql.TypeDouble)
	vexpr, err := expression.NewFunction(ds.SCtx().GetExprCtx(), fn, retType, geomCol)
	if err != nil {
		return nil
	}
	return vexpr
}

// buildBBoxConds builds the four MBR-intersection comparisons for a point index:
// x >= minX, x <= maxX, y >= minY, y <= maxY. Returned as separate conditions so
// the optimizer can apply each as an independent index filter.
func buildBBoxConds(ctx expression.BuildContext, xCol, yCol *expression.Column, rect spatial.Rect) []expression.Expression {
	boolType := types.NewFieldType(mysql.TypeLonglong)
	realType := types.NewFieldType(mysql.TypeDouble)
	cmp := func(fn string, col *expression.Column, v float64) expression.Expression {
		c := &expression.Constant{Value: types.NewFloat64Datum(v), RetType: realType}
		e, err := expression.NewFunction(ctx, fn, boolType, col, c)
		if err != nil {
			return nil
		}
		return e
	}
	conds := []expression.Expression{
		cmp(ast.GE, xCol, rect.MinX), cmp(ast.LE, xCol, rect.MaxX),
		cmp(ast.GE, yCol, rect.MinY), cmp(ast.LE, yCol, rect.MaxY),
	}
	for _, c := range conds {
		if c == nil {
			return nil
		}
	}
	return conds
}

// sridMatchesColumn reports whether the indexed column's declared SRID is the
// one the covering scheme expects (0 for the planar rect, 4326 for the S2 cap /
// lat-lng rect).
func (q coverRequest) sridMatchesColumn(tblInfo *model.TableInfo, geomColID int64) bool {
	var colSRID uint32
	for _, c := range tblInfo.Columns {
		if c.ID == geomColID {
			colSRID = c.Srid
			break
		}
	}
	switch q.kind {
	case coverSphereCap, coverLatLngRect:
		return colSRID == spatial.SRID4326
	default:
		return colSRID == 0
	}
}

// recognizeSpatialPredicate matches the supported spatial predicates and returns
// a covering request plus the indexed geometry column's ID.
func recognizeSpatialPredicate(cond expression.Expression, evalCtx expression.EvalContext) (coverRequest, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return coverRequest{}, false
	}
	switch sf.FuncName.L {
	case ast.LE, ast.LT:
		// ST_Distance(col, const) <= r  or  ST_Distance_Sphere(col, const) <= r
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
	return coverRequest{}, false
}

// recognizeDistancePredicate handles ST_Distance / ST_Distance_Sphere(col,
// const_point) <= r. Planar distance covers the disc's bbox (SRID 0); spherical
// distance covers an S2 cap (SRID 4326).
func recognizeDistancePredicate(cmp *expression.ScalarFunction, evalCtx expression.EvalContext) (coverRequest, bool) {
	args := cmp.GetArgs()
	distSF, ok := args[0].(*expression.ScalarFunction)
	if !ok {
		return coverRequest{}, false
	}
	sphere := distSF.FuncName.L == ast.StDistanceSphere
	if distSF.FuncName.L != ast.StDistance && !sphere {
		return coverRequest{}, false
	}
	radius, ok := evalConstFloat(args[1], evalCtx)
	if !ok || radius < 0 {
		return coverRequest{}, false
	}
	dargs := distSF.GetArgs()
	geomCol, constGeom, ok := splitColAndConst(dargs[0], dargs[1])
	if !ok {
		return coverRequest{}, false
	}
	ewkb, ok := evalConstString(constGeom, evalCtx)
	if !ok {
		return coverRequest{}, false
	}
	srid, x, y, err := expression.DecodeEWKBPoint(ewkb)
	if err != nil {
		return coverRequest{}, false
	}
	if sphere {
		if srid != spatial.SRID4326 {
			return coverRequest{}, false
		}
		return coverRequest{geomColID: geomCol.ID, kind: coverSphereCap, cx: x, cy: y, r: radius}, true
	}
	if srid != 0 {
		return coverRequest{}, false
	}
	return coverRequest{
		geomColID: geomCol.ID,
		kind:      coverPlanarRect,
		rect:      spatial.Rect{MinX: x - radius, MinY: y - radius, MaxX: x + radius, MaxY: y + radius},
	}, true
}

// recognizeContainmentPredicate handles a column / constant-polygon pair; the
// query region is the polygon's bounding box (planar for SRID 0, lat/long for
// SRID 4326).
func recognizeContainmentPredicate(colArg, polyArg expression.Expression, evalCtx expression.EvalContext) (coverRequest, bool) {
	geomCol, ok := colArg.(*expression.Column)
	if !ok {
		return coverRequest{}, false
	}
	ewkb, ok := evalConstString(polyArg, evalCtx)
	if !ok {
		return coverRequest{}, false
	}
	srid, minX, minY, maxX, maxY, err := expression.EWKBBounds(ewkb)
	if err != nil {
		return coverRequest{}, false
	}
	rect := spatial.Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
	switch srid {
	case 0:
		return coverRequest{geomColID: geomCol.ID, kind: coverPlanarRect, rect: rect}, true
	case spatial.SRID4326:
		return coverRequest{geomColID: geomCol.ID, kind: coverLatLngRect, rect: rect}, true
	default:
		return coverRequest{}, false
	}
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
	if len(ranges) == 0 {
		return nil
	}
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
