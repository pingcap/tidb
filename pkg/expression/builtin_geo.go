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
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/geos"
	"github.com/pingcap/tidb/pkg/util/spatial"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
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
	_ functionClass = &geosRelFunctionClass{}
	_ functionClass = &stXFunctionClass{}
	_ functionClass = &stYFunctionClass{}
	_ functionClass = &stSRIDFunctionClass{}
	_ functionClass = &tidbSpatialKeyFunctionClass{}
	_ functionClass = &tidbSpatialKeysFunctionClass{}
)

var (
	_ builtinFunc = &builtinStGeomFromTextSig{}
	_ builtinFunc = &builtinStAsTextSig{}
	_ builtinFunc = &builtinStDistanceSig{}
	_ builtinFunc = &builtinStDistanceSphereSig{}
	_ builtinFunc = &builtinGeosRelSig{}
	_ builtinFunc = &builtinStXSig{}
	_ builtinFunc = &builtinStYSig{}
	_ builtinFunc = &builtinStSRIDSig{}
	_ builtinFunc = &builtinTiDBSpatialKeySig{}
	_ builtinFunc = &builtinTiDBSpatialKeysSig{}
)

// defaultPlanarCoverer is the SRID 0 coverer used by tidb_spatial_key and the
// planner hook. It is immutable, so a single shared instance is safe.
var defaultPlanarCoverer = spatial.NewDefaultPlanarCoverer()

// encodeEWKB serializes a go-geom value with the given SRID into TiDB's
// EWKB storage form (<srid_le_uint32><wkb_le>).
func encodeEWKB(g geom.T, srid uint32) (string, error) {
	wkbVal, err := wkb.Marshal(g, binary.LittleEndian)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, srid); err != nil {
		return "", err
	}
	return string(append(buf.Bytes(), wkbVal...)), nil
}

// DecodeEWKBPoint decodes an EWKB POINT value into its SRID and coordinates.
// Exported for the planner's spatial-index resolver.
func DecodeEWKBPoint(ewkb string) (srid uint32, x, y float64, err error) {
	s, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, 0, err
	}
	pt, ok := g.(*geom.Point)
	if !ok {
		return 0, 0, 0, errors.New("expected a POINT geometry")
	}
	return s, pt.X(), pt.Y(), nil
}

// EWKBBounds returns the SRID and 2D bounding box of an EWKB geometry.
// Exported for the planner's spatial-index resolver.
func EWKBBounds(ewkb string) (srid uint32, minX, minY, maxX, maxY float64, err error) {
	s, g, err := decodeEWKB(ewkb)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	b := g.Bounds()
	return s, b.Min(0), b.Min(1), b.Max(0), b.Max(1), nil
}

// decodeEWKB parses TiDB's EWKB storage form back into an SRID and geometry.
func decodeEWKB(ewkb string) (uint32, geom.T, error) {
	if len(ewkb) < 4 {
		return 0, nil, errors.New("invalid geometry value: too short")
	}
	srid := binary.LittleEndian.Uint32([]byte(ewkb[:4]))
	g, err := wkb.Unmarshal([]byte(ewkb[4:]))
	if err != nil {
		return 0, nil, err
	}
	return srid, g, nil
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
	g, err := wkt.Unmarshal(wktVal)
	if err != nil {
		return "", false, err
	}
	out, err := encodeEWKB(g, uint32(srid))
	if err != nil {
		return "", false, err
	}
	return out, false, nil
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
	wktVal, err := wkt.Marshal(g)
	if err != nil {
		return "", false, err
	}
	return mysqlWKT(wktVal), false, nil
}

// mysqlWKT reshapes go-geom's WKT into MySQL's spacing: no space between the
// geometry type and its opening paren, and no space after coordinate-separating
// commas (e.g. "POINT (3 4)" -> "POINT(3 4)", "POLYGON ((0 0, 1 1))" ->
// "POLYGON((0 0,1 1))"). The space between a coordinate's X and Y is preserved.
func mysqlWKT(w string) string {
	w = strings.ReplaceAll(w, " (", "(")
	w = strings.ReplaceAll(w, ", ", ",")
	return w
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
		return 0, false, errors.New("ST_Distance: only SRID 0 is supported in the POC")
	}
	p1, ok1 := g1.(*geom.Point)
	p2, ok2 := g2.(*geom.Point)
	if !ok1 || !ok2 {
		return 0, false, errors.New("ST_Distance: only POINT arguments are supported in the POC")
	}
	dx := p1.X() - p2.X()
	dy := p1.Y() - p2.Y()
	return math.Sqrt(dx*dx + dy*dy), false, nil
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
	p1, ok1 := g1.(*geom.Point)
	p2, ok2 := g2.(*geom.Point)
	if !ok1 || !ok2 {
		return 0, false, errors.New("ST_Distance_Sphere: only POINT arguments are supported in the POC")
	}
	return greatCircleMeters(p1.X(), p1.Y(), p2.X(), p2.Y()), false, nil
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

// geosRelFunctionClass implements an OGC binary relational predicate
// (ST_Within, ST_Contains, ST_Intersects, ...) by delegating to GEOS, so the
// boundary/DE-9IM semantics match MySQL. setsSpatialFlag marks the predicates
// the planner can serve from a spatial index, so the resolver rule runs.
type geosRelFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
	pred            geos.Predicate
	setsSpatialFlag bool
}

func (c *geosRelFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	return &builtinGeosRelSig{baseBuiltinFunc: bf, pred: c.pred, funcName: c.funcName}, nil
}

type builtinGeosRelSig struct {
	baseBuiltinFunc
	pred     geos.Predicate
	funcName string
}

func (b *builtinGeosRelSig) Clone() builtinFunc {
	newSig := &builtinGeosRelSig{pred: b.pred, funcName: b.funcName}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates the relational predicate via GEOS over the two EWKB
// geometries (after checking their SRIDs agree, as MySQL requires).
func (b *builtinGeosRelSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	g1, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g2, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	s1, _, err := decodeEWKB(g1)
	if err != nil {
		return 0, false, err
	}
	s2, _, err := decodeEWKB(g2)
	if err != nil {
		return 0, false, err
	}
	if s1 != s2 {
		return 0, false, errors.Errorf("%s: binary geometry function passed two arguments with different SRIDs", b.funcName)
	}
	res, err := geos.Relate(b.pred, g1, g2)
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
	pt, ok := g.(*geom.Point)
	if !ok {
		return 0, 0, false, errors.New("ST_X/ST_Y: argument must be a POINT")
	}
	return pt.X(), pt.Y(), false, nil
}

type stSRIDFunctionClass struct {
	baseFunctionClass
}

func (c *stSRIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStSRIDSig{bf}
	return sig, nil
}

type builtinStSRIDSig struct {
	baseBuiltinFunc
}

func (b *builtinStSRIDSig) Clone() builtinFunc {
	newSig := &builtinStSRIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
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
	bnd := g.Bounds()
	cells, err := params.Coverer().CoverFixedLevelCells(spatial.Rect{
		MinX: bnd.Min(0), MinY: bnd.Min(1), MaxX: bnd.Max(0), MaxY: bnd.Max(1),
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
	pt, ok := g.(*geom.Point)
	if !ok {
		return "", false, errors.New("tidb_spatial_key: only POINT geometries are supported in the POC")
	}
	// WGS 84 points use the S2 cell scheme; SRID 0 uses the planar quadtree.
	if srid == spatial.SRID4326 {
		return string(spatial.EncodePointS2(pt.X(), pt.Y())), false, nil
	}
	coverer := defaultPlanarCoverer
	if len(b.args) == 6 {
		p, perr := b.planarParams(ctx, row)
		if perr != nil {
			return "", false, perr
		}
		coverer = p.Coverer()
	}
	key, err := coverer.EncodePoint(srid, pt.X(), pt.Y())
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
