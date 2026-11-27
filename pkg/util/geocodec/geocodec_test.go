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

package geocodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// line is a little-endian LINESTRING with coordinates d of the positions in fs.
func line(d dims, fs ...float64) []byte {
	return le(TypeLineString, d, count(len(fs)*8/d.size()), coords(fs...))
}

// polygon is a little-endian POLYGON with coordinates d of the rings in rings.
func polygon(d dims, rings ...[]float64) []byte {
	body := make([][]byte, 0, 1+2*len(rings))
	body = append(body, count(len(rings)))
	for _, r := range rings {
		body = append(body, count(len(r)*8/d.size()), coords(r...))
	}
	return le(TypePolygon, d, body...)
}

var (
	point12  = le(TypePoint, dimsXY, coords(1, 2))
	point12M = le(TypePoint, dimsXYM, coords(1, 2, 3))
	line0011 = line(dimsXY, 0, 0, 1, 1)
	triangle = polygon(dimsXY, []float64{0, 0, 0, 1, 1, 1, 0, 0})
)

// checkAccepted requires a bare value b that CheckBare accepts to agree with Header and to
// read back unchanged through the stored format.
func checkAccepted(t testing.TB, b []byte) {
	srid, typ, err := Header(b)
	require.NoError(t, err, "%X", b)
	require.Equal(t, binary.LittleEndian.Uint32(b), srid, "%X", b)
	require.Equal(t, GeomType(binary.LittleEndian.Uint32(b[5:])&^ewkbFlags), typ, "%X", b)
	s := BareToStored(b)
	require.Equal(t, Version1, s[0], "%X", b)
	back, err := StoredToBare(s)
	require.NoError(t, err, "%X", b)
	require.Equal(t, b, back, "%X", b)
}

// checkAny requires CheckBare and ParseColumnValue to agree on b: CheckBare accepts what
// ParseColumnValue accepts, a value either accepts is consistent with Header and the
// stored format, and a refusal is ErrInvalid or ErrTooDeep. It reports whether
// ParseColumnValue accepted b.
func checkAny(t testing.TB, b []byte) bool {
	checkErr := CheckBare(b)
	if checkErr != nil {
		require.True(t, errors.Is(checkErr, ErrInvalid) || errors.Is(checkErr, ErrTooDeep), "%X: %v", b, checkErr)
	} else {
		checkAccepted(t, b)
	}
	got, srid, sum, err := ParseColumnValue(b)
	if err != nil {
		return false
	}
	require.NoError(t, checkErr, "%X", b)
	require.Len(t, got, len(b), "%X", b)
	again, againSRID, againSum, err := ParseColumnValue(got)
	require.NoError(t, err, "%X", b)
	require.Equal(t, got, again, "%X", b)
	require.Equal(t, srid, againSRID, "%X", b)
	require.Equal(t, sum, againSum, "%X", b)
	_, typ, err := Header(got)
	require.NoError(t, err, "%X", b)
	require.Equal(t, sum.Type, typ, "%X", b)
	return true
}

func TestMySQLFormat(t *testing.T) {
	// Bytes from MySQL 9.7.2: HEX(ST_GeomFromText('POINT(1 2)', 4326)), latitude 1 and
	// longitude 2, which MySQL holds longitude first.
	mysql4326 := mustHex(t, "E6100000 01 01000000 0000000000000040 000000000000F03F")
	require.NoError(t, CheckBare(mysql4326))
	srid, typ, err := Header(mysql4326)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, TypePoint, typ)
	require.Equal(t, mustHex(t, "01 E6100000 01 01000000 0000000000000040 000000000000F03F"), BareToStored(mysql4326))
	checkAccepted(t, mysql4326)

	// Any SRID passes, since no SRS axis order is applied.
	checkAccepted(t, mustHex(t, "39300000 01 01000000 000000000000F03F 0000000000000040"))
	// The smallest valid value: GEOMETRYCOLLECTION EMPTY, 13 bytes.
	smallest := mustHex(t, "00000000 01 07000000 00000000")
	require.NoError(t, CheckBare(smallest))
	checkAccepted(t, smallest)

	rejects := map[string]string{
		"a byte short": "00000000 01 01000000 000000000000F03F 00000000000000",
		"a byte long":  "00000000 01 01000000 000000000000F03F 0000000000000040 00",
		// MySQL's format is little-endian at the root and in every member. MySQL 9.7.2
		// rejects any other byte-order byte in a bare literal, at the root or in a
		// member, with ERROR 1416.
		"big-endian WKB":        "00000000 00 00000001 3FF0000000000000 4000000000000000",
		"big-endian member":     "00000000 01 04000000 01000000 00 00000001 3FF0000000000000 4000000000000000",
		"big-endian collection": "00000000 00 00000004 00000001 01 01000000 000000000000F03F 0000000000000040",
		"big-endian at depth 3": "00000000 01 07000000 01000000 01 07000000 01000000" +
			" 00 00000001 3FF0000000000000 4000000000000000",
		"bad byte order":         "00000000 02 01000000 000000000000F03F 0000000000000040",
		"bad byte order, member": "00000000 01 04000000 01000000 02 01000000 000000000000F03F 0000000000000040",
		"ISO Z type code":        "00000000 01 E9030000 000000000000F03F 0000000000000040 0000000000000840",
		"unknown type code":      "00000000 01 08000000 00000000",
		// The SRID is the prefix, so the type word never has the SRID flag.
		"SRID flag":             "00000000 01 01000020 E6100000 000000000000F03F 0000000000000040",
		"SRID flag in a member": "00000000 01 04000000 01000000 01 01000020 E6100000 000000000000F03F 0000000000000040",
		"partly NaN point":      "00000000 01 01000000 000000000000F87F 0000000000000040",
		"infinite point":        "00000000 01 01000000 000000000000F07F 0000000000000040",
		"NaN in a linestring":   "00000000 01 02000000 01000000 000000000000F87F 0000000000000040",
		"ring without points":   "00000000 01 03000000 01000000 00000000",
		"member of the wrong type": "00000000 01 04000000 01000000 01 02000000 01000000" +
			" 000000000000F03F 0000000000000040",
		"member dimension differs": "00000000 01 04000080 01000000 01 01000000 000000000000F03F 0000000000000040",
		"hostile count":            "00000000 01 07000000 FFFFFFFF",
		"hostile point count":      "00000000 01 02000000 FFFFFF0F",
		"too short for an SRID":    "000000",
		"no geometry":              "E6100000",
		// A stored value is not a bare value.
		"stored value": "01 E6100000 01 01000000 000000000000F03F 0000000000000040",
	}
	for name, h := range rejects {
		b := mustHex(t, h)
		require.ErrorIs(t, CheckBare(b), ErrInvalid, name)
		_, _, _, err := ParseColumnValue(b)
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

func TestStoredFormat(t *testing.T) {
	// The stored format is the version byte, then the bare value.
	b := bareOf(4326, point12)
	s := BareToStored(b)
	require.Equal(t, mustHex(t, "01 E6100000 01 01000000 000000000000F03F 0000000000000040"), s)
	bare, err := StoredToBare(s)
	require.NoError(t, err)
	require.Equal(t, b, bare)
	// StoredToBare returns the bytes after the version byte, without copying them.
	require.Same(t, &s[1], &bare[0])

	for _, version := range []byte{0, 2, 0x7f, 0xff} {
		_, err := StoredToBare(append([]byte{version}, b...))
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
	}
	_, err = StoredToBare(nil)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestHeader(t *testing.T) {
	values := map[GeomType][][]byte{
		TypePoint:              {point12, le(TypePoint, dimsXYZM, coords(1, 2, 3, 4)), emptyPoint(dimsXY)},
		TypeLineString:         {line0011, line(dimsXYM, 0, 0, 1, 1, 1, 2)},
		TypePolygon:            {triangle, polygon(dimsXY)},
		TypeMultiPoint:         {collectionOf(TypeMultiPoint, dimsXY, point12), collectionOf(TypeMultiPoint, dimsXYZ, le(TypePoint, dimsXYZ, coords(1, 2, 3)))},
		TypeMultiLineString:    {collectionOf(TypeMultiLineString, dimsXY, line0011), collectionOf(TypeMultiLineString, dimsXY)},
		TypeMultiPolygon:       {collectionOf(TypeMultiPolygon, dimsXY, triangle)},
		TypeGeometryCollection: {collectionOf(TypeGeometryCollection, dimsXY, point12), collectionOf(TypeGeometryCollection, dimsXY)},
	}
	for want, vs := range values {
		for _, v := range vs {
			for _, srid := range []uint32{0, 4326, 3857, math.MaxUint32} {
				b := bareOf(srid, v)
				// Header reads the SRID, byte order and type word, and no more.
				for _, in := range [][]byte{b, b[:9]} {
					gotSRID, typ, err := Header(in)
					require.NoError(t, err, "%X", in)
					require.Equal(t, srid, gotSRID)
					require.Equal(t, want, typ)
				}
				for n := range 9 {
					_, _, err := Header(b[:n])
					require.ErrorIs(t, err, ErrInvalid, "%X cut to %d", b, n)
				}
			}
		}
	}
	for name, h := range map[string]string{
		"big-endian":        "00000000 00 00000001",
		"bad byte order":    "00000000 02 01000000",
		"SRID flag":         "00000000 01 01000020",
		"unknown type code": "00000000 01 08000000",
		"ISO type code":     "00000000 01 E9030000",
	} {
		_, _, err := Header(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

// TestZMAndEmpty checks values MySQL's binary format cannot hold, which the bare value
// holds with EWKB's Z and M flags and as empty geometries.
func TestZMAndEmpty(t *testing.T) {
	gc := TypeGeometryCollection
	for _, v := range [][]byte{
		// Z and M, at the root and below it.
		le(TypePoint, dimsXYZ, coords(1, 2, 3)),
		point12M,
		le(TypePoint, dimsXYZM, coords(1, 2, 3, 4)),
		line(dimsXYZ, 0, 0, 1, 1, 1, 1),
		polygon(dimsXYM, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1}),
		collectionOf(TypeMultiPoint, dimsXYZM, le(TypePoint, dimsXYZM, coords(1, 2, 3, 4))),
		collectionOf(TypeMultiLineString, dimsXYM, line(dimsXYM, 0, 0, 1, 1, 1, 1)),
		collectionOf(TypeMultiPolygon, dimsXYZ, polygon(dimsXYZ, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1})),
		collectionOf(gc, dimsXYZ),
		collectionOf(gc, dimsXYM, collectionOf(gc, dimsXYM, point12M)),
		// An empty geometry other than a GEOMETRYCOLLECTION, at any depth.
		emptyPoint(dimsXY),
		line(dimsXY),
		polygon(dimsXY),
		collectionOf(TypeMultiPoint, dimsXY),
		collectionOf(TypeMultiLineString, dimsXY),
		collectionOf(TypeMultiPolygon, dimsXY),
		collectionOf(TypeMultiPoint, dimsXY, point12, emptyPoint(dimsXY)),
		collectionOf(TypeMultiLineString, dimsXY, line0011, line(dimsXY)),
		collectionOf(TypeMultiPolygon, dimsXY, triangle, polygon(dimsXY)),
		collectionOf(gc, dimsXY, point12, collectionOf(gc, dimsXY, line(dimsXY))),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY), collectionOf(TypeMultiPoint, dimsXY, emptyPoint(dimsXY))),
	} {
		b := bareOf(4326, v)
		require.NoError(t, CheckBare(b), "%X", b)
		require.True(t, checkAny(t, b), "%X", b)
	}
	// At the depth bound, as the deepest member.
	for _, b := range [][]byte{
		bareChain(MaxDepth, emptyPoint(dimsXY)),
		chainOf(MaxDepth, ewkbZ, le(TypePoint, dimsXYZ, coords(1, 2, 3))),
	} {
		require.NoError(t, CheckBare(b))
		checkAccepted(t, b)
	}
}

// TestNaNRewrite checks that ParseColumnValue returns a POINT EMPTY of another NaN as the
// quiet NaN, in a copy, and the input left as it is, while CheckBare accepts any NaN.
func TestNaNRewrite(t *testing.T) {
	want := bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, emptyPoint(dimsXYM)))
	for _, nan := range nanBits {
		f := math.Float64frombits(nan)
		in := bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, le(TypePoint, dimsXYM, coords(f, f, f))))
		require.NoError(t, CheckBare(in))
		orig := bytes.Clone(in)
		got, srid, sum, err := ParseColumnValue(in)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, uint32(4326), srid)
		require.Equal(t, Summary{Type: TypeMultiPoint, Envelope: Envelope{1, 2, 1, 2, true}}, sum)
		require.Equal(t, orig, in)
		require.Equal(t, nan == quietNaN, &got[0] == &in[0])
	}
}

func TestParseColumnValue(t *testing.T) {
	const xy = dimsXY
	gc := func(members ...[]byte) []byte { return collectionOf(TypeGeometryCollection, xy, members...) }
	square := []float64{0, 0, 1, 0, 1, 1, 0, 1}
	closed := []float64{0, 0, 1, 0, 1, 1, 0, 0}
	hole := []float64{0.2, 0.1, 0.8, 0.1, 0.8, 0.7, 0.2, 0.1}
	line1 := line(xy, 0, 0)

	for _, c := range []struct {
		g   []byte
		typ GeomType
		env Envelope
	}{
		{point12, TypePoint, Envelope{1, 2, 1, 2, true}},
		{line0011, TypeLineString, Envelope{0, 0, 1, 1, true}},
		{polygon(xy, closed, hole), TypePolygon, Envelope{0, 0, 1, 1, true}},
		{collectionOf(TypeMultiPoint, xy, point12, le(TypePoint, xy, coords(3, -4))), TypeMultiPoint, Envelope{1, -4, 3, 2, true}},
		{collectionOf(TypeMultiPolygon, xy, polygon(xy, closed)), TypeMultiPolygon, Envelope{0, 0, 1, 1, true}},
		{gc(), TypeGeometryCollection, Envelope{}},
		{gc(point12, gc()), TypeGeometryCollection, Envelope{1, 2, 1, 2, true}},
		// An interior ring outside the exterior ring widens the envelope.
		{polygon(xy, closed, []float64{5, 5, 5, 6, 6, 6, 5, 5}), TypePolygon, Envelope{0, 0, 6, 6, true}},
		// The envelope takes X and Y only, and an empty point has no position.
		{collectionOf(TypeMultiPoint, dimsXYZM, le(TypePoint, dimsXYZM, coords(1, 2, 300, 400)), emptyPoint(dimsXYZM),
			le(TypePoint, dimsXYZM, coords(-5, 6, -700, 800))), TypeMultiPoint, Envelope{-5, 2, 1, 6, true}},
	} {
		for _, srid := range []uint32{0, 4326, 3857} {
			in := bareOf(srid, c.g)
			got, gotSRID, sum, err := ParseColumnValue(in)
			require.NoError(t, err, "%X", in)
			// A value already in canonical form is returned as it is.
			require.Same(t, &in[0], &got[0], "%X", in)
			require.Equal(t, srid, gotSRID, "%X", in)
			require.Equal(t, Summary{Type: c.typ, Envelope: c.env}, sum, "%X", in)
		}
	}

	// The structure rules of MySQL's format, as measured on MySQL 9.7.2, except that the
	// bare value also holds empty geometries and Z and M. CheckBare accepts them all.
	for _, c := range []struct {
		name string
		g    []byte
		ok   bool
	}{
		{"LINESTRING(0 0,0 0)", line(xy, 0, 0, 0, 0), true},
		{"unclosed ring of 4", polygon(xy, square), true},
		{"self-crossing ring", polygon(xy, []float64{0, 0, 2, 2, 2, 0, 0, 2, 0, 0}), true},
		{"ring of 4 equal points", polygon(xy, make([]float64, 8)), true},
		{"unclosed interior ring of 4", polygon(xy, append(square, 0, 0), []float64{0.2, 0.2, 0.2, 0.4, 0.4, 0.4, 0.3, 0.3}), true},
		{"overlapping polygons", collectionOf(TypeMultiPolygon, xy, polygon(xy, square), polygon(xy, square)), true},
		{"collection of a polygon", gc(polygon(xy, square)), true},
		{"collection of an empty collection", gc(gc()), true},
		{"LINESTRING(0 0)", line1, false},
		{"ring of 1", polygon(xy, []float64{0, 0}), false},
		{"ring of 2", polygon(xy, []float64{0, 0, 0, 0}), false},
		{"ring of 3", polygon(xy, []float64{0, 0, 1, 0, 0, 0}), false},
		{"interior ring of 3", polygon(xy, closed, []float64{0.2, 0.2, 0.4, 0.4, 0.2, 0.2}), false},
		{"MULTILINESTRING((0 0))", collectionOf(TypeMultiLineString, xy, line1), false},
		{"multipolygon with a ring of 3", collectionOf(TypeMultiPolygon, xy, polygon(xy, []float64{0, 0, 1, 0, 0, 0})), false},
		{"collection of LINESTRING(0 0)", gc(line1), false},
		{"nested collection of LINESTRING(0 0)", gc(gc(line1)), false},
		{"collection of MULTILINESTRING((0 0))", gc(collectionOf(TypeMultiLineString, xy, line1)), false},
		{"POINT EMPTY", emptyPoint(xy), true},
		{"LINESTRING EMPTY", line(xy), true},
		{"POLYGON EMPTY", polygon(xy), true},
		{"MULTIPOINT((1 2),EMPTY)", collectionOf(TypeMultiPoint, xy, point12, emptyPoint(xy)), true},
		{"collection of LINESTRING EMPTY", gc(line(xy)), true},
		{"LINESTRING M(0 0 1,1 1 2)", line(dimsXYM, 0, 0, 1, 1, 1, 2), true},
		{"LINESTRING Z(0 0 1)", line(dimsXYZ, 0, 0, 1), false},
		{"multipolygon Z with a ring of 3", collectionOf(TypeMultiPolygon, dimsXYZ, polygon(dimsXYZ, []float64{0, 0, 1, 1, 0, 1, 0, 0, 1})), false},
	} {
		in := bareOf(0, c.g)
		require.NoError(t, CheckBare(in), c.name)
		got, _, _, err := ParseColumnValue(in)
		if c.ok {
			require.NoError(t, err, "%s: %X", c.name, in)
			require.Equal(t, in, got, c.name)
		} else {
			require.ErrorIs(t, err, ErrInvalid, "%s: %X", c.name, in)
		}
	}

	// A one-point linestring is reported once the whole value is read, so a member nested
	// too deep behind it is reported first.
	_, _, _, err := ParseColumnValue(bareOf(0, gc(line1, nestedCollections(MaxDepth))))
	require.ErrorIs(t, err, ErrTooDeep)
}

func TestDepthBound(t *testing.T) {
	require.NoError(t, CheckBare(append(make([]byte, 4), nestedCollections(MaxDepth)...)))
	require.NoError(t, CheckBare(bareChain(MaxDepth, point12)))
	garbage := mustHex(t, "FF FFFFFFFF FFFFFFFF")
	for _, b := range [][]byte{
		append(make([]byte, 4), nestedCollections(MaxDepth+1)...),
		bareChain(MaxDepth+1, point12),
		// Depth is checked before anything else in a geometry. Its parent's count still
		// needs room for the smallest geometry.
		bareChain(MaxDepth+1, garbage),
	} {
		require.ErrorIs(t, CheckBare(b), ErrTooDeep)
		_, _, _, err := ParseColumnValue(b)
		require.ErrorIs(t, err, ErrTooDeep)
		// Header reads only the root, so depth does not matter to it.
		_, typ, err := Header(b)
		require.NoError(t, err)
		require.Equal(t, TypeGeometryCollection, typ)
	}

	// Errors are reported in reading order: a malformed member ahead of a too-deep one is
	// ErrInvalid, and behind it ErrTooDeep.
	root := mustHex(t, "00000000 01 07000000 02000000")
	bad := mustHex(t, "01 09000000 00000000")
	deep := nestedCollections(MaxDepth)
	require.ErrorIs(t, CheckBare(append(append(append([]byte{}, root...), bad...), deep...)), ErrInvalid)
	require.ErrorIs(t, CheckBare(append(append(append([]byte{}, root...), deep...), bad...)), ErrTooDeep)
}

func TestTruncatedAndOverLong(t *testing.T) {
	gc := TypeGeometryCollection
	for _, v := range [][]byte{
		point12,
		line(dimsXY, 0, 0, 1, 1, 2, 0),
		polygon(dimsXY, []float64{0, 0, 0, 1, 1, 1, 1, 0, 0, 0}, []float64{0.2, 0.2, 0.2, 0.4, 0.4, 0.4, 0.2, 0.2}),
		collectionOf(TypeMultiPoint, dimsXY, point12, le(TypePoint, dimsXY, coords(3, 4))),
		collectionOf(TypeMultiLineString, dimsXY, line0011, line(dimsXY, 2, 2, 3, 3)),
		collectionOf(TypeMultiPolygon, dimsXY, triangle, polygon(dimsXY, []float64{5, 5, 5, 6, 6, 6, 5, 5})),
		collectionOf(gc, dimsXY, point12, collectionOf(gc, dimsXY, line0011), collectionOf(gc, dimsXY)),
		collectionOf(gc, dimsXY),
		le(TypePoint, dimsXYZM, coords(1, 2, 3, 4)),
		collectionOf(gc, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12, emptyPoint(dimsXY))),
	} {
		b := bareOf(4326, v)
		require.NoError(t, CheckBare(b))
		for n := range b {
			require.ErrorIs(t, CheckBare(b[:n]), ErrInvalid, "%X cut to %d", b, n)
		}
		for _, extra := range [][]byte{{0}, {1}, b[4:]} {
			require.ErrorIs(t, CheckBare(append(append([]byte{}, b...), extra...)), ErrInvalid, "%X", b)
		}
	}
}

// TestGenerated checks CheckBare and ParseColumnValue on generated values of every type and
// coordinate type, with and without empty and big-endian members, against what the
// generator knows of each: its canonical form, its envelope, whether a line or ring is too
// short for MySQL's rules, and whether a member is big-endian.
func TestGenerated(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261119, 1))
	accepted, short, big, rewritten := 0, 0, 0, 0
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			for _, bigChance := range []float64{0, 0.3} {
				gen := wkbGen{rnd: rnd, d: d, empty: empty, big: bigChance}
				for i := range 500 {
					typ := allTypes[i%len(allTypes)]
					wkb, hasBig := gen.geometry(typ, 1)
					srid := gen.srid()
					in := bareOf(srid, wkb)
					// Header reads only the root, which the generator writes little-endian.
					gotSRID, gotType, err := Header(in)
					require.NoError(t, err, "%X", in)
					require.Equal(t, srid, gotSRID)
					require.Equal(t, typ, gotType)
					got, gotSRID, sum, err := ParseColumnValue(in)
					switch {
					case hasBig:
						require.ErrorIs(t, CheckBare(in), ErrInvalid, "%X", in)
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						big++
					case gen.short:
						checkAccepted(t, in)
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						short++
					default:
						checkAccepted(t, in)
						require.NoError(t, err, "%X", in)
						require.Equal(t, bareOf(srid, gen.canon), got, "%X", in)
						require.Equal(t, srid, gotSRID, "%X", in)
						require.Equal(t, Summary{Type: typ, Envelope: gen.env}, sum, "%X", in)
						accepted++
						if !bytes.Equal(in, got) {
							rewritten++
						}
					}
				}
			}
		}
	}
	require.Greater(t, accepted, 3000)
	require.Greater(t, short, 1000)
	require.Greater(t, big, 1000)
	require.Greater(t, rewritten, 100)
}

// TestGeneratedMutations checks CheckBare and ParseColumnValue on generated values with one
// byte changed, which reach the checks a well-formed value does not.
func TestGeneratedMutations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261119, 2))
	changes := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf0, 0xf8, 0x7f, 0xff}
	accepted := 0
	for _, d := range []dims{dimsXY, dimsXYZM} {
		gen := wkbGen{rnd: rnd, d: d, empty: 0.15}
		for i := range 300 {
			gen.geometry(allTypes[i%len(allTypes)], 1)
			form := bareOf(4326, gen.canon)
			for range 40 {
				b := bytes.Clone(form)
				b[rnd.IntN(len(b))] = changes[rnd.IntN(len(changes))]
				if checkAny(t, b) {
					accepted++
				}
			}
		}
	}
	require.Greater(t, accepted, 1000)
}

// TestAllocs checks that CheckBare, ParseColumnValue and StoredToBare allocate nothing,
// whatever the input's counts claim, except ParseColumnValue's rewritten value and
// BareToStored's result.
func TestAllocs(t *testing.T) {
	deep := bareChain(MaxDepth-1, multiPointWKB(10000))
	nan := bareOf(0, collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(math.NaN(), math.NaN()))))
	for _, c := range []struct {
		b                 []byte
		check, parseAlloc float64
	}{
		{deep, 0, 0},
		{deep[:len(deep)-1], 0, 0},
		{nan, 0, 1},
	} {
		require.Equal(t, c.check, testing.AllocsPerRun(3, func() { _ = CheckBare(c.b) }), "%X", c.b[:min(len(c.b), 32)])
		require.Equal(t, c.parseAlloc, testing.AllocsPerRun(3, func() { _, _, _, _ = ParseColumnValue(c.b) }), "%X", c.b[:min(len(c.b), 32)])
	}
	s := BareToStored(deep)
	require.Zero(t, testing.AllocsPerRun(3, func() { _, _ = StoredToBare(s) }))
	require.Equal(t, 1.0, testing.AllocsPerRun(3, func() { _ = BareToStored(deep) }))
}

func FuzzBare(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(bareOf(0, polygon(dimsXY, []float64{0, 0, 1, 0, 1, 1, 0, 1})))
	f.Add(bareOf(0, collectionOf(TypeGeometryCollection, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12), line(dimsXY))))
	f.Add(bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, le(TypePoint, dimsXYM, coords(math.NaN(), math.NaN(), math.NaN())))))
	f.Add(bareChain(MaxDepth, point12))
	f.Fuzz(func(t *testing.T, b []byte) {
		require.Zero(t, testing.AllocsPerRun(1, func() { _ = CheckBare(b) }), "%X", b)
		checkAny(t, b)
		_, _, _ = Header(b)
	})
}
