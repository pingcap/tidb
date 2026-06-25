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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	_ functionClass = &stContainsFunctionClass{}
	_ functionClass = &stWithinFunctionClass{}
	_ functionClass = &stXFunctionClass{}
	_ functionClass = &stYFunctionClass{}
	_ functionClass = &stSRIDFunctionClass{}
	_ functionClass = &tidbSpatialKeyFunctionClass{}
)

var (
	_ builtinFunc = &builtinStGeomFromTextSig{}
	_ builtinFunc = &builtinStAsTextSig{}
	_ builtinFunc = &builtinStDistanceSig{}
	_ builtinFunc = &builtinStContainsSig{}
	_ builtinFunc = &builtinStWithinSig{}
	_ builtinFunc = &builtinStXSig{}
	_ builtinFunc = &builtinStYSig{}
	_ builtinFunc = &builtinStSRIDSig{}
	_ builtinFunc = &builtinTiDBSpatialKeySig{}
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
	return wktVal, false, nil
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

type stContainsFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
}

func (c *stContainsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if sv, svErr := c.GetSessionVars(ctx.GetEvalCtx()); svErr == nil {
		sv.StmtCtx.SpatialFunctionIsUsed = true
	}
	sig := &builtinStContainsSig{bf}
	return sig, nil
}

type builtinStContainsSig struct {
	baseBuiltinFunc
}

func (b *builtinStContainsSig) Clone() builtinFunc {
	newSig := &builtinStContainsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_Contains(g1, g2): does polygon g1 contain point g2.
func (b *builtinStContainsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return evalContains(ctx, b.args[0], b.args[1], row)
}

type stWithinFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
}

func (c *stWithinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if sv, svErr := c.GetSessionVars(ctx.GetEvalCtx()); svErr == nil {
		sv.StmtCtx.SpatialFunctionIsUsed = true
	}
	sig := &builtinStWithinSig{bf}
	return sig, nil
}

type builtinStWithinSig struct {
	baseBuiltinFunc
}

func (b *builtinStWithinSig) Clone() builtinFunc {
	newSig := &builtinStWithinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt implements ST_Within(g1, g2): is point g1 within polygon g2. It is the
// inverse argument order of ST_Contains.
func (b *builtinStWithinSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return evalContains(ctx, b.args[1], b.args[0], row)
}

// evalContains evaluates whether polyArg (a polygon) contains pointArg (a point),
// using planar ray-casting on the polygon's exterior ring. POC scope: SRID 0,
// POINT in POLYGON only.
func evalContains(ctx EvalContext, polyArg, pointArg Expression, row chunk.Row) (int64, bool, error) {
	polyR, isNull, err := polyArg.EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pointR, isNull, err := pointArg.EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	sPoly, gPoly, err := decodeEWKB(polyR)
	if err != nil {
		return 0, false, err
	}
	sPoint, gPoint, err := decodeEWKB(pointR)
	if err != nil {
		return 0, false, err
	}
	if sPoly != sPoint {
		return 0, false, errors.New("ST_Contains/ST_Within: arguments have different SRIDs")
	}
	poly, ok := gPoly.(*geom.Polygon)
	if !ok {
		return 0, false, errors.New("ST_Contains/ST_Within: the container argument must be a POLYGON in the POC")
	}
	pt, ok := gPoint.(*geom.Point)
	if !ok {
		return 0, false, errors.New("ST_Contains/ST_Within: the contained argument must be a POINT in the POC")
	}
	if poly.NumLinearRings() == 0 {
		return 0, false, nil
	}
	if pointInRing(pt.X(), pt.Y(), poly.LinearRing(0).Coords()) {
		return 1, false, nil
	}
	return 0, false, nil
}

// pointInRing reports whether (x, y) lies inside the ring using the
// even-odd ray-casting rule.
func pointInRing(x, y float64, ring []geom.Coord) bool {
	inside := false
	n := len(ring)
	if n < 3 {
		return false
	}
	j := n - 1
	for i := range n {
		xi, yi := ring[i].X(), ring[i].Y()
		xj, yj := ring[j].X(), ring[j].Y()
		if (yi > y) != (yj > y) {
			xCross := (xj-xi)*(y-yi)/(yj-yi) + xi
			if x < xCross {
				inside = !inside
			}
		}
		j = i
	}
	return inside
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
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
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
	key, err := defaultPlanarCoverer.EncodePoint(srid, pt.X(), pt.Y())
	if err != nil {
		return "", false, err
	}
	return string(key), false, nil
}
