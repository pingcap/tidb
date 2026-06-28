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

package expression

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/geomrel"
	"github.com/pingcap/tidb/pkg/util/spatial"
	"github.com/pingcap/tipb/go-tipb"
)

// Geometry values are stored as EWKB: a 4-byte little-endian SRID prefix
// followed by standard OGC WKB, matching MySQL 8.0's internal geometry format.
// These builtins are a minimal prerequisite for the spatial-index POC; only the
// subset needed to insert, read back, and refine spatial queries is implemented.

var (
	_ functionClass = &stGeomFromTextFunctionClass{}
	_ functionClass = &stAsTextFunctionClass{}
	_ functionClass = &stDistanceFunctionClass{}
	_ functionClass = &stDistanceSphereFunctionClass{}
	_ functionClass = &geomRelFunctionClass{}
	_ functionClass = &stXFunctionClass{}
	_ functionClass = &stYFunctionClass{}
	_ functionClass = &stSRIDFunctionClass{}
	_ functionClass = &stGeometryTypeFunctionClass{}
	_ functionClass = &stAsBinaryFunctionClass{}
	_ functionClass = &stGeomFromWKBFunctionClass{}
	_ functionClass = &stEnvelopeFunctionClass{}
	_ functionClass = &stIsValidFunctionClass{}
	_ functionClass = &stIsEmptyFunctionClass{}
	_ functionClass = &stAsGeoJSONFunctionClass{}
	_ functionClass = &stGeomFromGeoJSONFunctionClass{}
	_ functionClass = &stAreaFunctionClass{}
	_ functionClass = &stLengthFunctionClass{}
	_ functionClass = &stDimensionFunctionClass{}
	_ functionClass = &stCentroidFunctionClass{}
	_ functionClass = &stStartPointFunctionClass{}
	_ functionClass = &stEndPointFunctionClass{}
	_ functionClass = &stExteriorRingFunctionClass{}
	_ functionClass = &stNumInteriorRingsFunctionClass{}
	_ functionClass = &stNumPointsFunctionClass{}
	_ functionClass = &stPointNFunctionClass{}
	_ functionClass = &tidbSpatialKeyFunctionClass{}
	_ functionClass = &tidbSpatialKeysFunctionClass{}
	_ functionClass = &tidbSpatialBBoxFunctionClass{}
	_ functionClass = &geomPointFunctionClass{}
	_ functionClass = &geomLineStringFunctionClass{}
	_ functionClass = &geomPolygonFunctionClass{}
	_ functionClass = &stLongitudeFunctionClass{}
	_ functionClass = &stLatitudeFunctionClass{}
)

var (
	_ builtinFunc = &builtinStGeomFromTextSig{}
	_ builtinFunc = &builtinStAsTextSig{}
	_ builtinFunc = &builtinStDistanceSig{}
	_ builtinFunc = &builtinStDistanceSphereSig{}
	_ builtinFunc = &builtinGeomRelSig{}
	_ builtinFunc = &builtinStXSig{}
	_ builtinFunc = &builtinStYSig{}
	_ builtinFunc = &builtinStSRIDSig{}
	_ builtinFunc = &builtinStGeometryTypeSig{}
	_ builtinFunc = &builtinStAsBinarySig{}
	_ builtinFunc = &builtinStGeomFromWKBSig{}
	_ builtinFunc = &builtinStEnvelopeSig{}
	_ builtinFunc = &builtinStSRIDSetSig{}
	_ builtinFunc = &builtinStIsValidSig{}
	_ builtinFunc = &builtinStIsEmptySig{}
	_ builtinFunc = &builtinStAsGeoJSONSig{}
	_ builtinFunc = &builtinStGeomFromGeoJSONSig{}
	_ builtinFunc = &builtinStAreaSig{}
	_ builtinFunc = &builtinStLengthSig{}
	_ builtinFunc = &builtinStDimensionSig{}
	_ builtinFunc = &builtinStCentroidSig{}
	_ builtinFunc = &builtinStEndpointSig{}
	_ builtinFunc = &builtinStExteriorRingSig{}
	_ builtinFunc = &builtinStNumInteriorRingsSig{}
	_ builtinFunc = &builtinStNumPointsSig{}
	_ builtinFunc = &builtinStPointNSig{}
	_ builtinFunc = &builtinTiDBSpatialKeySig{}
	_ builtinFunc = &builtinTiDBSpatialKeysSig{}
	_ builtinFunc = &builtinTiDBSpatialBBoxSig{}
	_ builtinFunc = &builtinGeomPointSig{}
	_ builtinFunc = &builtinGeomLineStringSig{}
	_ builtinFunc = &builtinGeomPolygonSig{}
	_ builtinFunc = &builtinStLongitudeSig{}
	_ builtinFunc = &builtinStLatitudeSig{}
)

// defaultPlanarCoverer is the SRID 0 coverer used by tidb_spatial_key and the
// planner hook. It is immutable, so a single shared instance is safe.
var defaultPlanarCoverer = spatial.NewDefaultPlanarCoverer()

// encodeEWKB serializes a simplefeatures geometry with the given SRID into
// TiDB's EWKB storage form (<srid_le_uint32><wkb>).
func encodeEWKB(g geom.Geometry, srid uint32) string {
	wkbVal := g.AsBinary()
	out := make([]byte, 4+len(wkbVal))
	binary.LittleEndian.PutUint32(out, srid)
	copy(out[4:], wkbVal)
	return string(out)
}

// DecodeEWKBPoint decodes an EWKB POINT value into its SRID and coordinates.
// Exported for the planner's spatial-index resolver.
func DecodeEWKBPoint(ewkb string) (srid uint32, x, y float64, err error) {
	s, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, 0, err
	}
	pt, ok := g.AsPoint()
	if !ok {
		return 0, 0, 0, errors.New("expected a POINT geometry")
	}
	xy, ok := pt.XY()
	if !ok {
		return 0, 0, 0, errors.New("empty POINT")
	}
	return s, xy.X, xy.Y, nil
}

// EWKBBounds returns the SRID and 2D bounding box of an EWKB geometry.
// Exported for the planner's spatial-index resolver.
func EWKBBounds(ewkb string) (srid uint32, minX, minY, maxX, maxY float64, err error) {
	s, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	mn, mx, ok := g.Envelope().MinMaxXYs()
	if !ok {
		return 0, 0, 0, 0, 0, errors.New("empty geometry has no bounding box")
	}
	return s, mn.X, mn.Y, mx.X, mx.Y, nil
}

// decodeEWKB parses TiDB's EWKB storage form back into an SRID and geometry.
//
// It skips simplefeatures' geometry validation (geom.NoValidate): stored values
// were already validated when constructed (ST_GeomFromText), and decodeEWKB's
// consumers only read coordinates, bounding boxes, or WKT — none of which need
// topological validity. Validation is the dominant cost of UnmarshalWKB for
// polygons (about 11x for a 50-vertex ring), so skipping it keeps the per-row
// read path cheap; malformed/truncated bytes still error during structural parsing.
func decodeEWKB(ewkb string) (uint32, geom.Geometry, error) {
	if len(ewkb) < 4 {
		return 0, geom.Geometry{}, errors.New("invalid geometry value: too short")
	}
	srid := binary.LittleEndian.Uint32([]byte(ewkb[:4]))
	g, err := geom.UnmarshalWKB([]byte(ewkb[4:]), geom.NoValidate{})
	if err != nil {
		return 0, geom.Geometry{}, errors.Trace(err)
	}
	return srid, g, nil
}

// pointXY decodes an EWKB POINT to its coordinates.
func pointXY(g geom.Geometry) (x, y float64, ok bool) {
	pt, ok := g.AsPoint()
	if !ok {
		return 0, 0, false
	}
	xy, ok := pt.XY()
	return xy.X, xy.Y, ok
}

type stGeomFromTextFunctionClass struct {
	baseFunctionClass
}

func (c *stGeomFromTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	if len(args) >= 2 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	// Type the result as GEOMETRY (still evaluated as a binary string, since
	// TypeGeometry maps to ETString). This lets the type system recognize the
	// value as geometry — e.g. so a plain B-tree functional index over it is
	// rejected like MySQL, rather than silently indexing the EWKB bytes.
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStGeomFromTextSig{bf}
	return sig, nil
}

type builtinStGeomFromTextSig struct {
	baseBuiltinFunc
}

func (b *builtinStGeomFromTextSig) Clone() builtinFunc {
	newSig := &builtinStGeomFromTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_GeomFromText(wkt[, srid]) -> EWKB geometry.
func (b *builtinStGeomFromTextSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	wktVal, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var srid int64
	if len(b.args) >= 2 {
		srid, isNull, err = b.args[1].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
	}
	g, err := geom.UnmarshalWKT(wktVal)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	if uint32(srid) == spatial.SRID4326 {
		if verr := validateGeographic4326(g); verr != nil {
			return "", false, verr
		}
	}
	return encodeEWKB(g, uint32(srid)), false, nil
}

// validateGeographic4326 rejects coordinates outside the valid WGS 84 ranges, matching
// MySQL's out-of-range error. The 4326 axis order is (latitude, longitude), so X is the
// latitude (∈ [-90,90]) and Y the longitude (∈ [-180,180]).
func validateGeographic4326(g geom.Geometry) error {
	env := g.Envelope()
	if env.IsEmpty() {
		return nil
	}
	mn, ok1 := env.Min().XY()
	mx, ok2 := env.Max().XY()
	if !ok1 || !ok2 {
		return nil
	}
	if mn.X < -90 || mx.X > 90 {
		bad := mn.X
		if mx.X > 90 {
			bad = mx.X
		}
		return errors.Errorf("latitude %g is out of range for SRID 4326; it must be within [-90, 90]", bad)
	}
	if mn.Y < -180 || mx.Y > 180 {
		bad := mn.Y
		if mx.Y > 180 {
			bad = mx.Y
		}
		return errors.Errorf("longitude %g is out of range for SRID 4326; it must be within [-180, 180]", bad)
	}
	return nil
}

// stTypedFromTextFunctionClass implements the typed WKT constructors
// (ST_PointFromText / ST_LineFromText / ST_LineStringFromText / ST_PolyFromText /
// ST_PolygonFromText): like ST_GeomFromText, but the parsed geometry must be of the
// expected type, else an error (MySQL parity).
type stTypedFromTextFunctionClass struct {
	baseFunctionClass
}

// typedWKTWant maps a typed-constructor function name to its required geometry type.
func typedWKTWant(funcName string) (geom.GeometryType, string) {
	switch funcName {
	case ast.StLineFromText, ast.StLineStringFromText:
		return geom.TypeLineString, "LINESTRING"
	case ast.StPolyFromText, ast.StPolygonFromText:
		return geom.TypePolygon, "POLYGON"
	default: // ast.StPointFromText
		return geom.TypePoint, "POINT"
	}
}

func (c *stTypedFromTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString}
	if len(args) >= 2 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	want, typeName := typedWKTWant(c.funcName)
	return &builtinStTypedFromTextSig{bf, want, typeName}, nil
}

type builtinStTypedFromTextSig struct {
	baseBuiltinFunc
	want     geom.GeometryType
	typeName string
}

func (b *builtinStTypedFromTextSig) Clone() builtinFunc {
	newSig := &builtinStTypedFromTextSig{want: b.want, typeName: b.typeName}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStTypedFromTextSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	wktVal, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var srid int64
	if len(b.args) >= 2 {
		srid, isNull, err = b.args[1].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
	}
	g, err := geom.UnmarshalWKT(wktVal)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	if g.Type() != b.want {
		return "", false, errors.Errorf("typed WKT constructor requires a %s geometry", b.typeName)
	}
	if uint32(srid) == spatial.SRID4326 {
		if verr := validateGeographic4326(g); verr != nil {
			return "", false, verr
		}
	}
	return encodeEWKB(g, uint32(srid)), false, nil
}

type stAsTextFunctionClass struct {
	baseFunctionClass
}

func (c *stAsTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStAsTextSig{bf}
	return sig, nil
}

type builtinStAsTextSig struct {
	baseBuiltinFunc
}

func (b *builtinStAsTextSig) Clone() builtinFunc {
	newSig := &builtinStAsTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_AsText(geom) -> WKT.
func (b *builtinStAsTextSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	// simplefeatures' WKT already uses MySQL's spacing (e.g. "POINT(3 4)").
	return g.AsText(), false, nil
}

type stDistanceFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
}

func (c *stDistanceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if sv, svErr := c.GetSessionVars(ctx.GetEvalCtx()); svErr == nil {
		sv.StmtCtx.SpatialFunctionIsUsed = true
	}
	sig := &builtinStDistanceSig{bf}
	return sig, nil
}

type builtinStDistanceSig struct {
	baseBuiltinFunc
}

func (b *builtinStDistanceSig) Clone() builtinFunc {
	newSig := &builtinStDistanceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal implements ST_Distance(g1, g2). The POC supports the planar
// (SRID 0) Cartesian distance between two points.
func (b *builtinStDistanceSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	g1r, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g2r, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	s1, g1, err := decodeEWKB(g1r)
	if err != nil {
		return 0, false, err
	}
	s2, g2, err := decodeEWKB(g2r)
	if err != nil {
		return 0, false, err
	}
	if s1 != s2 {
		return 0, false, errors.New("ST_Distance: binary geometry function passed two arguments with different SRIDs")
	}
	if s1 != 0 {
		// 4326 ST_Distance is geodesic (ellipsoidal) in MySQL; not yet implemented
		// here (ST_Distance_Sphere covers the spherical case).
		return 0, false, errors.New("ST_Distance: only SRID 0 is supported in the POC")
	}
	// Planar distance between any two SRID-0 geometries (point/line/polygon/…).
	d, ok := geom.Distance(g1, g2)
	if !ok {
		return 0, true, nil // an empty operand → NULL (MySQL)
	}
	return d, false, nil
}

type stDistanceSphereFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
}

func (c *stDistanceSphereFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if sv, svErr := c.GetSessionVars(ctx.GetEvalCtx()); svErr == nil {
		sv.StmtCtx.SpatialFunctionIsUsed = true
	}
	return &builtinStDistanceSphereSig{bf}, nil
}

type builtinStDistanceSphereSig struct {
	baseBuiltinFunc
}

func (b *builtinStDistanceSphereSig) Clone() builtinFunc {
	newSig := &builtinStDistanceSphereSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal implements ST_Distance_Sphere(p1, p2): the great-circle distance in
// metres between two WGS 84 points, using MySQL's default sphere radius.
func (b *builtinStDistanceSphereSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	g1r, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g2r, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	s1, g1, err := decodeEWKB(g1r)
	if err != nil {
		return 0, false, err
	}
	s2, g2, err := decodeEWKB(g2r)
	if err != nil {
		return 0, false, err
	}
	if s1 != s2 {
		return 0, false, errors.New("ST_Distance_Sphere: arguments have different SRIDs")
	}
	// Require the geographic SRID 4326, both so the haversine is meaningful and so
	// a mixed-SRID call cannot pair a spherical refine with a planar index scan
	// (which would silently drop rows).
	if s1 != spatial.SRID4326 {
		return 0, false, errors.New("ST_Distance_Sphere: only SRID 4326 is supported in the POC")
	}
	x1, y1, ok1 := pointXY(g1)
	x2, y2, ok2 := pointXY(g2)
	if !ok1 || !ok2 {
		return 0, false, errors.New("ST_Distance_Sphere: only POINT arguments are supported in the POC")
	}
	// 4326 axis order is (latitude, longitude): x is the latitude, y the longitude.
	return greatCircleMeters(y1, x1, y2, x2), false, nil
}

// greatCircleMeters is the haversine distance in metres between two points given
// as (x=longitude, y=latitude) degrees, using MySQL's sphere radius.
func greatCircleMeters(lng1, lat1, lng2, lat2 float64) float64 {
	const toRad = math.Pi / 180
	la1, la2 := lat1*toRad, lat2*toRad
	dLat := (lat2 - lat1) * toRad
	dLng := (lng2 - lng1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(la1)*math.Cos(la2)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return spatial.EarthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// geomRelFunctionClass implements an OGC binary relational predicate
// (ST_Within, ST_Contains, ST_Intersects, ...) via the pure-Go simplefeatures
// engine (pkg/util/geomrel), so the boundary/DE-9IM semantics match MySQL.
// setsSpatialFlag marks the predicates the planner can serve from a spatial
// index, so the resolver rule runs.
type geomRelFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
	pred            geomrel.Predicate
	setsSpatialFlag bool
}

func (c *geomRelFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if c.setsSpatialFlag {
		if sv, svErr := c.GetSessionVars(ctx.GetEvalCtx()); svErr == nil {
			sv.StmtCtx.SpatialFunctionIsUsed = true
		}
	}
	sig := &builtinGeomRelSig{baseBuiltinFunc: bf, pred: c.pred, funcName: c.funcName}
	// A pb code lets the predicate be pushed to the coprocessor (the spatial-index
	// refine filter), where it is evaluated over the stored EWKB.
	sig.setPbCode(geomRelPbCode(c.pred))
	return sig, nil
}

// geomRelPbCode maps a geometry relational predicate to its coprocessor
// ScalarFuncSig (see tipb branch spatial-pushdown / tikv-pushdown-handoff.md).
func geomRelPbCode(p geomrel.Predicate) tipb.ScalarFuncSig {
	switch p {
	case geomrel.Within:
		return tipb.ScalarFuncSig_StWithin
	case geomrel.Contains:
		return tipb.ScalarFuncSig_StContains
	case geomrel.Intersects:
		return tipb.ScalarFuncSig_StIntersects
	case geomrel.Equals:
		return tipb.ScalarFuncSig_StEquals
	case geomrel.Disjoint:
		return tipb.ScalarFuncSig_StDisjoint
	case geomrel.Touches:
		return tipb.ScalarFuncSig_StTouches
	case geomrel.Crosses:
		return tipb.ScalarFuncSig_StCrosses
	case geomrel.Overlaps:
		return tipb.ScalarFuncSig_StOverlaps
	case geomrel.Covers:
		return tipb.ScalarFuncSig_StCovers
	case geomrel.CoveredBy:
		return tipb.ScalarFuncSig_StCoveredBy
	default:
		return tipb.ScalarFuncSig_Unspecified
	}
}

// geomRelFromPbCode is the inverse of geomRelPbCode: it rebuilds the predicate and
// function name from a pushed-down ScalarFuncSig (the coprocessor / distsql path).
func geomRelFromPbCode(sig tipb.ScalarFuncSig) (geomrel.Predicate, string) {
	switch sig {
	case tipb.ScalarFuncSig_StContains:
		return geomrel.Contains, ast.StContains
	case tipb.ScalarFuncSig_StIntersects:
		return geomrel.Intersects, ast.StIntersects
	case tipb.ScalarFuncSig_StEquals:
		return geomrel.Equals, ast.StEquals
	case tipb.ScalarFuncSig_StDisjoint:
		return geomrel.Disjoint, ast.StDisjoint
	case tipb.ScalarFuncSig_StTouches:
		return geomrel.Touches, ast.StTouches
	case tipb.ScalarFuncSig_StCrosses:
		return geomrel.Crosses, ast.StCrosses
	case tipb.ScalarFuncSig_StOverlaps:
		return geomrel.Overlaps, ast.StOverlaps
	case tipb.ScalarFuncSig_StCovers:
		return geomrel.Covers, ast.StCovers
	case tipb.ScalarFuncSig_StCoveredBy:
		return geomrel.CoveredBy, ast.StCoveredBy
	default: // StWithin
		return geomrel.Within, ast.StWithin
	}
}

type builtinGeomRelSig struct {
	baseBuiltinFunc
	pred     geomrel.Predicate
	funcName string
}

func (b *builtinGeomRelSig) Clone() builtinFunc {
	newSig := &builtinGeomRelSig{pred: b.pred, funcName: b.funcName}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates the relational predicate via simplefeatures over the two EWKB
// geometries (after checking their SRIDs agree, as MySQL requires).
func (b *builtinGeomRelSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	g1, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g2, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	s1, geo1, err := decodeEWKB(g1)
	if err != nil {
		return 0, false, err
	}
	s2, geo2, err := decodeEWKB(g2)
	if err != nil {
		return 0, false, err
	}
	if s1 != s2 {
		return 0, false, errors.Errorf("%s: binary geometry function passed two arguments with different SRIDs", b.funcName)
	}
	// A spatial relation predicate with an empty-geometry operand is NULL in MySQL.
	if geo1.IsEmpty() || geo2.IsEmpty() {
		return 0, true, nil
	}
	// MySQL's ST_Crosses is only defined when dim(g1) < dim(g2), or both are lines;
	// in any other dimension combination (e.g. Crosses(polygon, line), poly/poly,
	// point/point) it returns NULL rather than a boolean.
	if b.pred == geomrel.Crosses {
		d1, d2 := geo1.Dimension(), geo2.Dimension()
		if !(d1 < d2 || (d1 == 1 && d2 == 1)) {
			return 0, true, nil
		}
	}
	res, err := geomrel.Relate(b.pred, g1, g2)
	if err != nil {
		return 0, false, err
	}
	if res {
		return 1, false, nil
	}
	return 0, false, nil
}

type stXFunctionClass struct {
	baseFunctionClass
}

func (c *stXFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStXSig{bf}
	return sig, nil
}

type builtinStXSig struct {
	baseBuiltinFunc
}

func (b *builtinStXSig) Clone() builtinFunc {
	newSig := &builtinStXSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStXSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, _, isNull, err := evalPointXY(ctx, b.args[0], row)
	return x, isNull, err
}

type stYFunctionClass struct {
	baseFunctionClass
}

func (c *stYFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStYSig{bf}
	return sig, nil
}

type builtinStYSig struct {
	baseBuiltinFunc
}

func (b *builtinStYSig) Clone() builtinFunc {
	newSig := &builtinStYSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStYSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	_, y, isNull, err := evalPointXY(ctx, b.args[0], row)
	return y, isNull, err
}

func evalPointXY(ctx EvalContext, arg Expression, row chunk.Row) (x, y float64, isNull bool, err error) {
	ewkb, isNull, err := arg.EvalString(ctx, row)
	if isNull || err != nil {
		return 0, 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, false, err
	}
	x, y, ok := pointXY(g)
	if !ok {
		return 0, 0, false, errors.New("ST_X/ST_Y: argument must be a POINT")
	}
	return x, y, false, nil
}

type stSRIDFunctionClass struct {
	baseFunctionClass
}

func (c *stSRIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args) == 2 {
		// ST_SRID(g, srid) is the setter form: return a copy of g with its SRID
		// replaced. The result is a GEOMETRY (evaluated as a binary string).
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		types.SetBinChsClnFlag(bf.tp)
		bf.tp.SetType(mysql.TypeGeometry)
		bf.tp.SetFlen(types.UnspecifiedLength)
		return &builtinStSRIDSetSig{bf}, nil
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStSRIDSig{bf}
	return sig, nil
}

type builtinStSRIDSetSig struct {
	baseBuiltinFunc
}

func (b *builtinStSRIDSetSig) Clone() builtinFunc {
	newSig := &builtinStSRIDSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_SRID(geom, srid) -> geom with its SRID set to srid.
func (b *builtinStSRIDSetSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	return encodeEWKB(g, uint32(srid)), false, nil
}

type builtinStSRIDSig struct {
	baseBuiltinFunc
}

func (b *builtinStSRIDSig) Clone() builtinFunc {
	newSig := &builtinStSRIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type stGeometryTypeFunctionClass struct {
	baseFunctionClass
}

func (c *stGeometryTypeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(64)
	sig := &builtinStGeometryTypeSig{bf}
	return sig, nil
}

type builtinStGeometryTypeSig struct {
	baseBuiltinFunc
}

func (b *builtinStGeometryTypeSig) Clone() builtinFunc {
	newSig := &builtinStGeometryTypeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_GeometryType(geom) -> the OGC type name (e.g. POINT).
func (b *builtinStGeometryTypeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	// MySQL reports the type in upper case (POINT, LINESTRING, ...).
	return strings.ToUpper(g.Type().String()), false, nil
}

type stAsBinaryFunctionClass struct {
	baseFunctionClass
}

func (c *stAsBinaryFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStAsBinarySig{bf}
	return sig, nil
}

type builtinStAsBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinStAsBinarySig) Clone() builtinFunc {
	newSig := &builtinStAsBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_AsBinary/ST_AsWKB(geom) -> standard WKB (the EWKB
// storage form with its 4-byte SRID prefix stripped).
func (b *builtinStAsBinarySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if len(ewkb) < 4 {
		return "", false, errors.New("invalid geometry value: too short")
	}
	return ewkb[4:], false, nil
}

type stGeomFromWKBFunctionClass struct {
	baseFunctionClass
}

func (c *stGeomFromWKBFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	if len(args) >= 2 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStGeomFromWKBSig{bf}
	return sig, nil
}

type builtinStGeomFromWKBSig struct {
	baseBuiltinFunc
}

func (b *builtinStGeomFromWKBSig) Clone() builtinFunc {
	newSig := &builtinStGeomFromWKBSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_GeomFromWKB(wkb[, srid]) -> EWKB geometry.
func (b *builtinStGeomFromWKBSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	wkbVal, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var srid int64
	if len(b.args) >= 2 {
		srid, isNull, err = b.args[1].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
	}
	g, err := geom.UnmarshalWKB([]byte(wkbVal))
	if err != nil {
		return "", false, errors.Trace(err)
	}
	if uint32(srid) == spatial.SRID4326 {
		if verr := validateGeographic4326(g); verr != nil {
			return "", false, verr
		}
	}
	return encodeEWKB(g, uint32(srid)), false, nil
}

type stEnvelopeFunctionClass struct {
	baseFunctionClass
}

func (c *stEnvelopeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStEnvelopeSig{bf}
	return sig, nil
}

type builtinStEnvelopeSig struct {
	baseBuiltinFunc
}

func (b *builtinStEnvelopeSig) Clone() builtinFunc {
	newSig := &builtinStEnvelopeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_Envelope(geom) -> the minimum bounding rectangle as a
// geometry, preserving the input SRID.
func (b *builtinStEnvelopeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	// Match MySQL's ST_Envelope: a degenerate MBR collapses to a POINT (zero
	// width+height) or a LINESTRING (zero width or height); otherwise a POLYGON
	// whose exterior ring is CCW starting at the min corner
	// (minX minY, maxX minY, maxX maxY, minX maxY, minX minY).
	mn, mx, ok := g.Envelope().MinMaxXYs()
	if !ok {
		return encodeEWKB(g.Envelope().AsGeometry(), srid), false, nil
	}
	var env geom.Geometry
	switch {
	case mn.X == mx.X && mn.Y == mx.Y:
		env = geom.NewPointXY(mn.X, mn.Y).AsGeometry()
	case mn.X == mx.X || mn.Y == mx.Y:
		env = geom.NewLineStringXY(mn.X, mn.Y, mx.X, mx.Y).AsGeometry()
	default:
		ring := geom.NewLineStringXY(mn.X, mn.Y, mx.X, mn.Y, mx.X, mx.Y, mn.X, mx.Y, mn.X, mn.Y)
		env = geom.NewPolygon([]geom.LineString{ring}).AsGeometry()
	}
	return encodeEWKB(env, srid), false, nil
}

type stIsValidFunctionClass struct {
	baseFunctionClass
}

func (c *stIsValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStIsValidSig{bf}
	return sig, nil
}

type builtinStIsValidSig struct {
	baseBuiltinFunc
}

func (b *builtinStIsValidSig) Clone() builtinFunc {
	newSig := &builtinStIsValidSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_IsValid(geom) -> 1 if the geometry is OGC-valid, else 0.
func (b *builtinStIsValidSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	if g.Validate() != nil {
		return 0, false, nil
	}
	return 1, false, nil
}

type stIsEmptyFunctionClass struct {
	baseFunctionClass
}

func (c *stIsEmptyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStIsEmptySig{bf}
	return sig, nil
}

type builtinStIsEmptySig struct {
	baseBuiltinFunc
}

func (b *builtinStIsEmptySig) Clone() builtinFunc {
	newSig := &builtinStIsEmptySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_IsEmpty(geom) -> 1 if the geometry is empty, else 0.
func (b *builtinStIsEmptySig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	if g.IsEmpty() {
		return 1, false, nil
	}
	return 0, false, nil
}

type stAsGeoJSONFunctionClass struct {
	baseFunctionClass
}

func (c *stAsGeoJSONFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStAsGeoJSONSig{bf}
	return sig, nil
}

type builtinStAsGeoJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinStAsGeoJSONSig) Clone() builtinFunc {
	newSig := &builtinStAsGeoJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_AsGeoJSON(geom) -> the GeoJSON representation.
func (b *builtinStAsGeoJSONSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	// GeoJSON (RFC 7946) is always [longitude, latitude]. The POC stores 4326 in
	// (latitude, longitude) axis order — matching MySQL's WKT — so swap for GeoJSON.
	if srid == spatial.SRID4326 {
		g = g.TransformXY(swapXY)
	}
	j, err := g.MarshalJSON()
	if err != nil {
		return "", false, errors.Trace(err)
	}
	return string(j), false, nil
}

// swapXY exchanges a geometry's X and Y coordinates — used to convert between the
// POC's stored 4326 axis order (latitude, longitude) and GeoJSON's [longitude,
// latitude] (RFC 7946).
func swapXY(xy geom.XY) geom.XY { return geom.XY{X: xy.Y, Y: xy.X} }

type stGeomFromGeoJSONFunctionClass struct {
	baseFunctionClass
}

func (c *stGeomFromGeoJSONFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStGeomFromGeoJSONSig{bf}
	return sig, nil
}

type builtinStGeomFromGeoJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinStGeomFromGeoJSONSig) Clone() builtinFunc {
	newSig := &builtinStGeomFromGeoJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_GeomFromGeoJSON(json) -> EWKB geometry. GeoJSON is
// WGS 84 by convention, so the result uses SRID 4326 (matching MySQL's default).
func (b *builtinStGeomFromGeoJSONSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	jsonVal, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	g, err := geom.UnmarshalGeoJSON([]byte(jsonVal))
	if err != nil {
		return "", false, errors.Trace(err)
	}
	// GeoJSON is [longitude, latitude]; the POC stores 4326 in (latitude, longitude)
	// axis order, so swap on the way in. (GeoJSON is always WGS 84 / SRID 4326.)
	g = g.TransformXY(swapXY)
	if verr := validateGeographic4326(g); verr != nil {
		return "", false, verr
	}
	return encodeEWKB(g, spatial.SRID4326), false, nil
}

type stAreaFunctionClass struct {
	baseFunctionClass
}

func (c *stAreaFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStAreaSig{bf}
	return sig, nil
}

type builtinStAreaSig struct {
	baseBuiltinFunc
}

func (b *builtinStAreaSig) Clone() builtinFunc {
	newSig := &builtinStAreaSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal implements ST_Area(geom) -> the area (0 for non-areal geometries).
func (b *builtinStAreaSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	return g.Area(), false, nil
}

type stLengthFunctionClass struct {
	baseFunctionClass
}

func (c *stLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStLengthSig{bf}
	return sig, nil
}

type builtinStLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinStLengthSig) Clone() builtinFunc {
	newSig := &builtinStLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal implements ST_Length(geom) -> the length (0 for non-linear geometries).
func (b *builtinStLengthSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	return g.Length(), false, nil
}

type stDimensionFunctionClass struct {
	baseFunctionClass
}

func (c *stDimensionFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStDimensionSig{bf}
	return sig, nil
}

type builtinStDimensionSig struct {
	baseBuiltinFunc
}

func (b *builtinStDimensionSig) Clone() builtinFunc {
	newSig := &builtinStDimensionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_Dimension(geom) -> 0 (point), 1 (line), 2 (area).
func (b *builtinStDimensionSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	return int64(g.Dimension()), false, nil
}

type stCentroidFunctionClass struct {
	baseFunctionClass
}

func (c *stCentroidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStCentroidSig{bf}
	return sig, nil
}

type builtinStCentroidSig struct {
	baseBuiltinFunc
}

func (b *builtinStCentroidSig) Clone() builtinFunc {
	newSig := &builtinStCentroidSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_Centroid(geom) -> the centroid point, keeping the SRID.
func (b *builtinStCentroidSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	return encodeEWKB(g.Centroid().AsGeometry(), srid), false, nil
}

type stStartPointFunctionClass struct {
	baseFunctionClass
}

func (c *stStartPointFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStEndpointSig{baseBuiltinFunc: bf, start: true}
	return sig, nil
}

type stEndPointFunctionClass struct {
	baseFunctionClass
}

func (c *stEndPointFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStEndpointSig{baseBuiltinFunc: bf, start: false}
	return sig, nil
}

type builtinStEndpointSig struct {
	baseBuiltinFunc
	start bool
}

func (b *builtinStEndpointSig) Clone() builtinFunc {
	newSig := &builtinStEndpointSig{start: b.start}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_StartPoint/ST_EndPoint(line) -> the first/last point,
// or NULL when the argument is not a non-empty LINESTRING (MySQL semantics).
func (b *builtinStEndpointSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	ls, ok := g.AsLineString()
	if !ok || ls.IsEmpty() {
		return "", true, nil
	}
	pt := ls.StartPoint()
	if !b.start {
		pt = ls.EndPoint()
	}
	return encodeEWKB(pt.AsGeometry(), srid), false, nil
}

type stExteriorRingFunctionClass struct {
	baseFunctionClass
}

func (c *stExteriorRingFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStExteriorRingSig{bf}
	return sig, nil
}

type builtinStExteriorRingSig struct {
	baseBuiltinFunc
}

func (b *builtinStExteriorRingSig) Clone() builtinFunc {
	newSig := &builtinStExteriorRingSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_ExteriorRing(polygon) -> its outer ring as a
// LINESTRING, or NULL when the argument is not a POLYGON (MySQL semantics).
func (b *builtinStExteriorRingSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	p, ok := g.AsPolygon()
	if !ok {
		return "", true, nil
	}
	return encodeEWKB(p.ExteriorRing().AsGeometry(), srid), false, nil
}

type stNumInteriorRingsFunctionClass struct {
	baseFunctionClass
}

func (c *stNumInteriorRingsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStNumInteriorRingsSig{bf}
	return sig, nil
}

type builtinStNumInteriorRingsSig struct {
	baseBuiltinFunc
}

func (b *builtinStNumInteriorRingsSig) Clone() builtinFunc {
	newSig := &builtinStNumInteriorRingsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_NumInteriorRings(polygon) -> the interior-ring count, or
// NULL when the argument is not a POLYGON (MySQL semantics).
func (b *builtinStNumInteriorRingsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	p, ok := g.AsPolygon()
	if !ok {
		return 0, true, nil
	}
	return int64(p.NumInteriorRings()), false, nil
}

type stNumPointsFunctionClass struct {
	baseFunctionClass
}

func (c *stNumPointsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStNumPointsSig{bf}
	return sig, nil
}

type builtinStNumPointsSig struct {
	baseBuiltinFunc
}

func (b *builtinStNumPointsSig) Clone() builtinFunc {
	newSig := &builtinStNumPointsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_NumPoints(line) -> the point count, or NULL when the
// argument is not a LINESTRING (MySQL semantics).
func (b *builtinStNumPointsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	ls, ok := g.AsLineString()
	if !ok {
		return 0, true, nil
	}
	return int64(ls.Coordinates().Length()), false, nil
}

type stPointNFunctionClass struct {
	baseFunctionClass
}

func (c *stPointNFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
	sig := &builtinStPointNSig{bf}
	return sig, nil
}

type builtinStPointNSig struct {
	baseBuiltinFunc
}

func (b *builtinStPointNSig) Clone() builtinFunc {
	newSig := &builtinStPointNSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements ST_PointN(line, n) -> the n-th point (1-indexed), or
// NULL when the argument is not a LINESTRING or n is out of range.
func (b *builtinStPointNSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	n, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	ls, ok := g.AsLineString()
	if !ok {
		return "", true, nil
	}
	seq := ls.Coordinates()
	if n < 1 || n > int64(seq.Length()) {
		return "", true, nil
	}
	xy := seq.GetXY(int(n - 1))
	return encodeEWKB(geom.NewPointXY(xy.X, xy.Y).AsGeometry(), srid), false, nil
}

type tidbSpatialKeysFunctionClass struct {
	baseFunctionClass
}

func (c *tidbSpatialKeysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args) != 1 && len(args) != 6 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for range args[1:] {
		argTps = append(argTps, types.ETReal)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	return &builtinTiDBSpatialKeysSig{bf}, nil
}

type builtinTiDBSpatialKeysSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBSpatialKeysSig) Clone() builtinFunc {
	newSig := &builtinTiDBSpatialKeysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// SpatialKeyHexLen is the width of a hex-encoded cell key (8 bytes -> 16 chars),
// used as the element type of the multi-valued spatial index.
const SpatialKeyHexLen = 16

// evalJSON implements tidb_spatial_keys(geom): the JSON array of fixed-level cell
// keys (hex-encoded) covering the geometry's bounding box. Backs the MVI for
// general (non-point) geometries. POC scope: SRID 0.
func (b *builtinTiDBSpatialKeysSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.BinaryJSON{}, isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	if srid != 0 {
		return types.BinaryJSON{}, false, errors.New("tidb_spatial_keys: only SRID 0 is supported in the POC")
	}
	params := spatial.DefaultPlanarParams()
	if len(b.args) == 6 {
		vals := make([]float64, 5)
		for i := range vals {
			v, isNull, err := b.args[i+1].EvalReal(ctx, row)
			if isNull || err != nil {
				return types.BinaryJSON{}, isNull, err
			}
			vals[i] = v
		}
		params = spatial.PlanarParams{Level: uint(vals[0]), MinX: vals[1], MinY: vals[2], MaxX: vals[3], MaxY: vals[4]}
	}
	mn, mx, ok := g.Envelope().MinMaxXYs()
	if !ok {
		// An empty geometry covers no cells.
		return types.CreateBinaryJSON([]any{}), false, nil
	}
	cells, err := params.Coverer().CoverFixedLevelCells(spatial.Rect{
		MinX: mn.X, MinY: mn.Y, MaxX: mx.X, MaxY: mx.Y,
	})
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	elems := make([]any, len(cells))
	for i, k := range cells {
		elems[i] = hex.EncodeToString(k)
	}
	return types.CreateBinaryJSON(elems), false, nil
}

type tidbSpatialBBoxFunctionClass struct {
	baseFunctionClass
}

func (c *tidbSpatialBBoxFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// tidb_spatial_bbox(geom, component) -> one MBR component as a double.
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	return &builtinTiDBSpatialBBoxSig{bf}, nil
}

type builtinTiDBSpatialBBoxSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBSpatialBBoxSig) Clone() builtinFunc {
	newSig := &builtinTiDBSpatialBBoxSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal returns one component of the geometry's minimum bounding rectangle:
// 0=minX, 1=minY, 2=maxX, 3=maxY. An empty geometry (no bbox) yields NULL, which
// the MBR-intersection filter treats as non-matching; the retained refine stays
// exact.
func (b *builtinTiDBSpatialBBoxSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	comp, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	_, minX, minY, maxX, maxY, err := EWKBBounds(ewkb)
	if err != nil {
		// Empty geometry (or otherwise no bounding box).
		return 0, true, nil
	}
	switch comp {
	case 0:
		return minX, false, nil
	case 1:
		return minY, false, nil
	case 2:
		return maxX, false, nil
	case 3:
		return maxY, false, nil
	default:
		return 0, false, errors.Errorf("tidb_spatial_bbox: invalid component %d", comp)
	}
}

func (b *builtinStSRIDSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	srid, _, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, false, err
	}
	return int64(srid), false, nil
}

type tidbSpatialKeyFunctionClass struct {
	baseFunctionClass
}

func (c *tidbSpatialKeyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// tidb_spatial_key(geom) or tidb_spatial_key(geom, level, minX, minY, maxX, maxY).
	if len(args) != 1 && len(args) != 6 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for range args[1:] {
		argTps = append(argTps, types.ETReal)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	// The CellKey is a fixed 8-byte big-endian Morton code.
	bf.tp.SetFlen(8)
	sig := &builtinTiDBSpatialKeySig{bf}
	return sig, nil
}

type builtinTiDBSpatialKeySig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBSpatialKeySig) Clone() builtinFunc {
	newSig := &builtinTiDBSpatialKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString implements tidb_spatial_key(geom): the order-preserving CellKey a
// POINT geometry is indexed under. POC scope: SRID 0 POINTs.
func (b *builtinTiDBSpatialKeySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	ewkb, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return "", false, err
	}
	x, y, ok := pointXY(g)
	if !ok {
		return "", false, errors.New("tidb_spatial_key: only POINT geometries are supported in the POC")
	}
	// WGS 84 points use the S2 cell scheme; SRID 0 uses the planar quadtree.
	// MySQL's EPSG:4326 axis order is (latitude, longitude), so the stored first
	// coordinate (x) is the latitude and the second (y) the longitude.
	if srid == spatial.SRID4326 {
		return string(spatial.EncodePointS2(y, x)), false, nil
	}
	coverer := defaultPlanarCoverer
	if len(b.args) == 6 {
		p, perr := b.planarParams(ctx, row)
		if perr != nil {
			return "", false, perr
		}
		coverer = p.Coverer()
	}
	key, err := coverer.EncodePoint(srid, x, y)
	if err != nil {
		return "", false, err
	}
	return string(key), false, nil
}

// planarParams reads the (level, minX, minY, maxX, maxY) tuning args.
func (b *builtinTiDBSpatialKeySig) planarParams(ctx EvalContext, row chunk.Row) (spatial.PlanarParams, error) {
	vals := make([]float64, 5)
	for i := range vals {
		v, isNull, err := b.args[i+1].EvalReal(ctx, row)
		if err != nil {
			return spatial.PlanarParams{}, err
		}
		if isNull {
			return spatial.PlanarParams{}, errors.New("tidb_spatial_key: cell params must not be NULL")
		}
		vals[i] = v
	}
	p := spatial.PlanarParams{Level: uint(vals[0]), MinX: vals[1], MinY: vals[2], MaxX: vals[3], MaxY: vals[4]}
	if p.Level == 0 || p.Level > 31 || p.MinX >= p.MaxX || p.MinY >= p.MaxY {
		return spatial.PlanarParams{}, errors.Errorf("tidb_spatial_key: invalid cell params level=%d bounds=[%g,%g]x[%g,%g]", p.Level, p.MinX, p.MaxX, p.MinY, p.MaxY)
	}
	return p, nil
}

// setGeometryResultType marks a builtin's result as GEOMETRY (evaluated as a
// binary EWKB string), matching how ST_GeomFromText types its result.
func setGeometryResultType(bf baseBuiltinFunc) {
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetType(mysql.TypeGeometry)
	bf.tp.SetFlen(types.UnspecifiedLength)
}

// geomPointFunctionClass implements the MySQL POINT(x, y) constructor.
type geomPointFunctionClass struct {
	baseFunctionClass
}

func (c *geomPointFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	setGeometryResultType(bf)
	return &builtinGeomPointSig{bf}, nil
}

type builtinGeomPointSig struct {
	baseBuiltinFunc
}

func (b *builtinGeomPointSig) Clone() builtinFunc {
	newSig := &builtinGeomPointSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGeomPointSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	y, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return encodeEWKB(geom.NewPointXY(x, y).AsGeometry(), 0), false, nil
}

// geomLineStringFunctionClass implements MySQL LineString(pt, pt, ...).
type geomLineStringFunctionClass struct {
	baseFunctionClass
}

func (c *geomLineStringFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = types.ETString
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	setGeometryResultType(bf)
	return &builtinGeomLineStringSig{bf}, nil
}

type builtinGeomLineStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGeomLineStringSig) Clone() builtinFunc {
	newSig := &builtinGeomLineStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGeomLineStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	xys := make([]float64, 0, len(b.args)*2)
	for _, a := range b.args {
		ewkb, isNull, err := a.EvalString(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
		_, g, err := decodeEWKB(ewkb)
		if err != nil {
			return "", false, err
		}
		pt, ok := g.AsPoint()
		if !ok {
			return "", false, errors.New("LineString: each argument must be a POINT")
		}
		xy, ok := pt.XY()
		if !ok {
			return "", false, errors.New("LineString: empty POINT argument")
		}
		xys = append(xys, xy.X, xy.Y)
	}
	return encodeEWKB(geom.NewLineStringXY(xys...).AsGeometry(), 0), false, nil
}

// geomPolygonFunctionClass implements MySQL Polygon(ring, ...) where each ring is
// a LINESTRING (the first is the exterior, the rest are holes).
type geomPolygonFunctionClass struct {
	baseFunctionClass
}

func (c *geomPolygonFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = types.ETString
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	setGeometryResultType(bf)
	return &builtinGeomPolygonSig{bf}, nil
}

type builtinGeomPolygonSig struct {
	baseBuiltinFunc
}

func (b *builtinGeomPolygonSig) Clone() builtinFunc {
	newSig := &builtinGeomPolygonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGeomPolygonSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	rings := make([]geom.LineString, 0, len(b.args))
	for _, a := range b.args {
		ewkb, isNull, err := a.EvalString(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
		_, g, err := decodeEWKB(ewkb)
		if err != nil {
			return "", false, err
		}
		ls, ok := g.AsLineString()
		if !ok {
			return "", false, errors.New("Polygon: each argument must be a LINESTRING ring")
		}
		rings = append(rings, ls)
	}
	return encodeEWKB(geom.NewPolygon(rings).AsGeometry(), 0), false, nil
}

// evalGeographicPoint decodes a geographic (SRID 4326) POINT argument and returns
// its stored coordinates (x = coord 0, y = coord 1). ST_Latitude is x, ST_Longitude
// is y. Mirrors MySQL's domain checks: the SRS must be geographic and the value a
// POINT.
func evalGeographicPoint(ctx EvalContext, fn string, arg Expression, row chunk.Row) (x, y float64, isNull bool, err error) {
	ewkb, isNull, err := arg.EvalString(ctx, row)
	if isNull || err != nil {
		return 0, 0, isNull, err
	}
	srid, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, false, err
	}
	if srid != spatial.SRID4326 {
		return 0, 0, false, errors.Errorf("function %s is only defined for geographic spatial reference systems, but one of its arguments is in SRID %d, which is not geographic", fn, srid)
	}
	px, py, ok := pointXY(g)
	if !ok {
		return 0, 0, false, errors.Errorf("%s requires a POINT argument", fn)
	}
	return px, py, false, nil
}

// stLongitudeFunctionClass implements MySQL ST_Longitude(point) for a 4326 point.
type stLongitudeFunctionClass struct {
	baseFunctionClass
}

func (c *stLongitudeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	return &builtinStLongitudeSig{bf}, nil
}

type builtinStLongitudeSig struct {
	baseBuiltinFunc
}

func (b *builtinStLongitudeSig) Clone() builtinFunc {
	newSig := &builtinStLongitudeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStLongitudeSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	_, lng, isNull, err := evalGeographicPoint(ctx, "st_longitude", b.args[0], row)
	return lng, isNull, err
}

// stLatitudeFunctionClass implements MySQL ST_Latitude(point) for a 4326 point.
type stLatitudeFunctionClass struct {
	baseFunctionClass
}

func (c *stLatitudeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	return &builtinStLatitudeSig{bf}, nil
}

type builtinStLatitudeSig struct {
	baseBuiltinFunc
}

func (b *builtinStLatitudeSig) Clone() builtinFunc {
	newSig := &builtinStLatitudeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStLatitudeSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	lat, _, isNull, err := evalGeographicPoint(ctx, "st_latitude", b.args[0], row)
	return lat, isNull, err
}
