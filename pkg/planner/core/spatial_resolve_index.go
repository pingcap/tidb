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
	"encoding/hex"
	"math"
	"strconv"
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
		// Only inject when the indexed column's SRID matches the covering scheme;
		// otherwise the stored keys (planar Morton vs S2) are in a different space
		// and the ranges would never match. The retained exact predicate still
		// produces the correct (full-scan) result.
		if !req.sridMatchesColumn(ds.TableInfo, req.geomColID) {
			continue
		}
		// Two index shapes: a POINT scalar index (covering-cell ranges on the hidden
		// cell-key column) or a general-geometry multi-valued index (a json_overlaps
		// over the query's covering cells, served by IndexMerge).
		matched := false
		if hiddenCol, params, ok := s.findSpatialHiddenColumn(ds, req.geomColID); ok {
			if ranges, err := req.ranges(params); err == nil && len(ranges) > 0 {
				if rangeExpr := buildCellRangeExpr(exprCtx, hiddenCol, ranges); rangeExpr != nil {
					added = append(added, rangeExpr)
					matched = true
				}
			}
		} else if params, ok := s.findSpatialMVIParams(ds, req.geomColID); ok {
			if overlaps := buildMVIOverlapsExpr(exprCtx, ds, req, params); overlaps != nil {
				added = append(added, overlaps)
				matched = true
			}
		}
		if !matched {
			continue
		}
		// MBR pruning: if the index carries bbox columns (ST_X/ST_Y for a point,
		// tidb_spatial_bbox for a general geometry), conjoin a bounding-box-
		// intersection filter on them. These are plain numeric comparisons on index
		// columns, so the optimizer applies them during the index scan, before the
		// table lookup — pruning the covering's false positives without a random
		// read. The exact predicate still refines.
		if rect, hasRect := req.bboxRect(); hasRect {
			if minX, minY, maxX, maxY, ok := s.findSpatialBBoxColumns(ds, req.geomColID); ok {
				added = append(added, buildBBoxConds(exprCtx, minX, minY, maxX, maxY, rect)...)
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

// findSpatialMVIParams reports whether geomColID is covered by a general-geometry
// multi-valued spatial index (a hidden column generated by tidb_spatial_keys),
// returning its coverer params.
func (*SpatialIndexResolver) findSpatialMVIParams(ds *logicalop.DataSource, geomColID int64) (spatial.PlanarParams, bool) {
	tblInfo := ds.TableInfo
	var geomColName string
	for _, c := range tblInfo.Columns {
		if c.ID == geomColID {
			geomColName = c.Name.L
			break
		}
	}
	if geomColName == "" {
		return spatial.PlanarParams{}, false
	}
	for _, idx := range tblInfo.Indices {
		if !idx.IsPublic() || len(idx.Columns) == 0 {
			continue
		}
		for _, ic := range idx.Columns {
			c := tblInfo.Columns[ic.Offset]
			if !isSpatialMVIColumn(c) {
				continue
			}
			if _, dep := c.Dependences[geomColName]; !dep {
				continue
			}
			params, err := parseSpatialKeysParams(c.GeneratedExprString)
			if err != nil {
				continue
			}
			return params, true
		}
	}
	return spatial.PlanarParams{}, false
}

// isSpatialMVIColumn reports whether a column is the hidden generated column of a
// general-geometry multi-valued spatial index (CAST(tidb_spatial_keys(...) AS ...
// ARRAY)).
func isSpatialMVIColumn(col *model.ColumnInfo) bool {
	return col.Hidden && strings.Contains(col.GeneratedExprString, ast.TiDBSpatialKeys+"(")
}

// buildMVIOverlapsExpr builds json_overlaps(tidb_spatial_keys(geom, params),
// <query cells>) so the multi-valued index is used via IndexMerge: the row matches
// when its covering cells overlap the cells covering the query region. The exact
// ST_ predicate is retained as the refine filter.
func buildMVIOverlapsExpr(ctx expression.BuildContext, ds *logicalop.DataSource, req coverRequest, params spatial.PlanarParams) expression.Expression {
	var geomCol *expression.Column
	for _, c := range ds.Schema().Columns {
		if c.ID == req.geomColID {
			geomCol = c
			break
		}
	}
	if geomCol == nil {
		return nil
	}
	rowCells := buildSpatialKeysExpr(ctx, geomCol, params)
	if rowCells == nil {
		return nil
	}
	cells, err := params.Coverer().CoverFixedLevelCells(req.rect)
	if err != nil || len(cells) == 0 {
		return nil
	}
	elems := make([]any, len(cells))
	for i, k := range cells {
		elems[i] = hex.EncodeToString(k)
	}
	var queryConst types.Datum
	queryConst.SetMysqlJSON(types.CreateBinaryJSON(elems))
	arg2 := &expression.Constant{Value: queryConst, RetType: types.NewFieldType(mysql.TypeJSON)}
	boolType := types.NewFieldType(mysql.TypeLonglong)
	overlaps, err := expression.NewFunction(ctx, ast.JSONOverlaps, boolType, rowCells, arg2)
	if err != nil {
		return nil
	}
	return overlaps
}

// buildSpatialKeysExpr reconstructs tidb_spatial_keys(geomCol[, params]) — the JSON
// array of covering cells — matching how the DDL builds the multi-valued index, so
// the planner can match the json_overlaps to that index.
func buildSpatialKeysExpr(ctx expression.BuildContext, geomCol *expression.Column, params spatial.PlanarParams) expression.Expression {
	args := []expression.Expression{geomCol}
	if params != spatial.DefaultPlanarParams() {
		realTp := types.NewFieldType(mysql.TypeDouble)
		mk := func(v float64) expression.Expression {
			return &expression.Constant{Value: types.NewFloat64Datum(v), RetType: realTp}
		}
		args = append(args, mk(float64(params.Level)), mk(params.MinX), mk(params.MinY), mk(params.MaxX), mk(params.MaxY))
	}
	expr, err := expression.NewFunction(ctx, ast.TiDBSpatialKeys, types.NewFieldType(mysql.TypeJSON), args...)
	if err != nil {
		return nil
	}
	return expr
}

// parseSpatialKeysParams reconstructs the coverer params from a multi-valued index
// hidden column's generated expression, e.g. "cast(tidb_spatial_keys(`g`, 6, 0, 0,
// 64, 64) as char(16) array)" or "cast(tidb_spatial_keys(`g`) as ...)" (defaults).
func parseSpatialKeysParams(genExpr string) (spatial.PlanarParams, error) {
	const marker = ast.TiDBSpatialKeys + "("
	i := strings.Index(genExpr, marker)
	if i < 0 {
		return spatial.PlanarParams{}, errors.Errorf("not a tidb_spatial_keys expression: %q", genExpr)
	}
	rest := genExpr[i+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return spatial.PlanarParams{}, errors.Errorf("malformed tidb_spatial_keys expression: %q", genExpr)
	}
	parts := strings.Split(rest[:end], ",")
	// parts[0] is the geometry column reference; the rest are the cell params.
	return spatial.ParsePlanarParams(parts[1:])
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
	case coverSphereCap:
		// Lat/long bounding box of the spherical cap (angular radius theta around
		// centre lng=cx, lat=cy), so the ST_X/ST_Y bbox columns prune SRID-4326
		// distance queries too. Slightly widened to stay conservative against
		// float error (the exact ST_Distance_Sphere refine removes the surplus).
		theta := q.r / spatial.EarthRadiusMeters // angular radius, radians
		const margin = 1 + 1e-9
		dLat := theta * margin * 180 / math.Pi
		minLat, maxLat := q.cy-dLat, q.cy+dLat
		// Cap reaches a pole: every longitude is within range.
		if minLat <= -90 || maxLat >= 90 {
			return spatial.Rect{MinX: -180, MinY: math.Max(minLat, -90), MaxX: 180, MaxY: math.Min(maxLat, 90)}, true
		}
		// Max longitude deviation of the cap at the centre latitude.
		ratio := math.Sin(theta) / math.Cos(q.cy*math.Pi/180)
		if ratio >= 1 {
			return spatial.Rect{MinX: -180, MinY: minLat, MaxX: 180, MaxY: maxLat}, true
		}
		dLng := math.Asin(ratio) * margin * 180 / math.Pi
		minLng, maxLng := q.cx-dLng, q.cx+dLng
		// A single BETWEEN cannot express an antimeridian wrap; skip the bbox there
		// (the cell covering still applies and the refine stays exact).
		if minLng < -180 || maxLng > 180 {
			return spatial.Rect{}, false
		}
		return spatial.Rect{MinX: minLng, MinY: minLat, MaxX: maxLng, MaxY: maxLat}, true
	default:
		return spatial.Rect{}, false
	}
}

// findSpatialBBoxColumns returns the hidden MBR index columns of the spatial index
// on geomColID as (minX, minY, maxX, maxY), re-adding them to the DataSource schema
// if they were pruned. A point index carries ST_X/ST_Y (the point is its own MBR,
// so minX=maxX and minY=maxY); a general-geometry index carries the four
// tidb_spatial_bbox(g, 0..3) columns. Returns ok=false when the index has no bbox
// columns.
func (*SpatialIndexResolver) findSpatialBBoxColumns(ds *logicalop.DataSource, geomColID int64) (minX, minY, maxX, maxY *expression.Column, ok bool) {
	tblInfo := ds.TableInfo
	var geomColName string
	for _, c := range tblInfo.Columns {
		if c.ID == geomColID {
			geomColName = c.Name.L
			break
		}
	}
	if geomColName == "" {
		return nil, nil, nil, nil, false
	}
	var xInfo, yInfo *model.ColumnInfo
	var comp [4]*model.ColumnInfo
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
		case strings.HasPrefix(c.GeneratedExprString, ast.TiDBSpatialBBox+"("):
			if i, ok := parseSpatialBBoxComponent(c.GeneratedExprString); ok {
				comp[i] = c
			}
		}
	}
	// reAdd returns the column's expression.Column from the schema, or re-adds it
	// carrying the generating expression as VirtualExpr (bbox columns are virtual
	// expression-index columns, materialized only in the index), so the value is
	// available when not read straight from the index.
	reAdd := func(info *model.ColumnInfo, virtual expression.Expression) *expression.Column {
		for _, sc := range ds.Schema().Columns {
			if sc.ID == info.ID {
				return sc
			}
		}
		newCol := &expression.Column{
			UniqueID:    ds.SCtx().GetSessionVars().AllocPlanColumnID(),
			ID:          info.ID,
			RetType:     info.FieldType.Clone(),
			OrigName:    info.Name.L,
			VirtualExpr: virtual,
		}
		ds.Columns = append(ds.Columns, info)
		ds.Schema().Append(newCol)
		return newCol
	}
	if xInfo != nil && yInfo != nil {
		x := reAdd(xInfo, buildBBoxVirtualExpr(ds, geomColID, ast.StX))
		y := reAdd(yInfo, buildBBoxVirtualExpr(ds, geomColID, ast.StY))
		return x, y, x, y, true // a point's MBR is the point
	}
	if comp[0] != nil && comp[1] != nil && comp[2] != nil && comp[3] != nil {
		col := func(i int) *expression.Column {
			return reAdd(comp[i], buildBBoxVirtualExpr(ds, geomColID, ast.TiDBSpatialBBox, intConst(i)))
		}
		return col(0), col(1), col(2), col(3), true
	}
	return nil, nil, nil, nil, false
}

// parseSpatialBBoxComponent extracts the component index (0..3) from a
// tidb_spatial_bbox column's generated expression, e.g. "tidb_spatial_bbox(`g`, 2)".
func parseSpatialBBoxComponent(genExpr string) (int, bool) {
	open := strings.Index(genExpr, "(")
	end := strings.LastIndex(genExpr, ")")
	if open < 0 || end <= open {
		return 0, false
	}
	parts := strings.Split(genExpr[open+1:end], ",")
	if len(parts) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || n < 0 || n > 3 {
		return 0, false
	}
	return n, true
}

// intConst builds an int64 constant for the tidb_spatial_bbox component argument.
func intConst(v int) expression.Expression {
	return &expression.Constant{Value: types.NewIntDatum(int64(v)), RetType: types.NewFieldType(mysql.TypeLonglong)}
}

// buildBBoxVirtualExpr reconstructs the generating expression of a bbox column —
// ST_X(g)/ST_Y(g) for a point, or tidb_spatial_bbox(g, i) for a general geometry —
// for the re-added column's VirtualExpr. Returns nil if the geometry column is not
// in the schema (the column is still usable straight from the index).
func buildBBoxVirtualExpr(ds *logicalop.DataSource, geomColID int64, fn string, extraArgs ...expression.Expression) expression.Expression {
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
	args := append([]expression.Expression{geomCol}, extraArgs...)
	vexpr, err := expression.NewFunction(ds.SCtx().GetExprCtx(), fn, retType, args...)
	if err != nil {
		return nil
	}
	return vexpr
}

// buildBBoxConds builds the MBR-intersection filter between the row's bounding box
// (minX,minY,maxX,maxY index columns) and the query rectangle: the boxes overlap
// iff minX <= qMaxX AND maxX >= qMinX AND minY <= qMaxY AND maxY >= qMinY. For a
// point index minX==maxX and minY==maxY, so this reduces to point-in-rectangle.
// Returned as separate conditions so the optimizer can apply each as an index filter.
func buildBBoxConds(ctx expression.BuildContext, minX, minY, maxX, maxY *expression.Column, rect spatial.Rect) []expression.Expression {
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
		cmp(ast.LE, minX, rect.MaxX), cmp(ast.GE, maxX, rect.MinX),
		cmp(ast.LE, minY, rect.MaxY), cmp(ast.GE, maxY, rect.MinY),
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
	case ast.StContains, ast.StWithin, ast.StIntersects:
		// ST_Contains / ST_Within / ST_Intersects of a column and a constant
		// geometry: one argument is the indexed column, the other the constant query
		// region. For all three, a matching row's geometry overlaps the region, so
		// its covering cells overlap the region's cells — the same covering serves
		// every predicate, and the argument order does not matter here.
		args := sf.GetArgs()
		return recognizeRegionPredicate(args[0], args[1], evalCtx)
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

// recognizeRegionPredicate handles a column / constant-geometry pair (the
// ST_Contains / ST_Within / ST_Intersects family); the query region is the
// constant geometry's bounding box (planar for SRID 0, lat/long for SRID 4326).
func recognizeRegionPredicate(arg0, arg1 expression.Expression, evalCtx expression.EvalContext) (coverRequest, bool) {
	geomCol, polyArg, ok := splitColAndConst(arg0, arg1)
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
