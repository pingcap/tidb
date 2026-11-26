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
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseColumnValue(t *testing.T) {
	const xy = dimsXY
	gc := func(members ...[]byte) []byte { return collectionOf(TypeGeometryCollection, xy, members...) }
	square := []float64{0, 0, 1, 0, 1, 1, 0, 1}
	closed := []float64{0, 0, 1, 0, 1, 1, 0, 0}
	hole := []float64{0.2, 0.1, 0.8, 0.1, 0.8, 0.7, 0.2, 0.1}
	line1 := line(xy, 0, 0)

	for _, c := range []struct {
		g   []byte
		typ Type
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
	// bare value also holds empty geometries and Z and M.
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
		{"interior ring outside", polygon(xy, closed, []float64{5, 5, 5, 6, 6, 6, 5, 5}), true},
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
		{"MULTIPOINT EMPTY", collectionOf(TypeMultiPoint, xy), true},
		{"MULTIPOINT((1 2),EMPTY)", collectionOf(TypeMultiPoint, xy, point12, emptyPoint(xy)), true},
		{"MULTILINESTRING EMPTY", collectionOf(TypeMultiLineString, xy), true},
		{"MULTIPOLYGON EMPTY", collectionOf(TypeMultiPolygon, xy), true},
		{"collection of MULTIPOINT EMPTY", gc(collectionOf(TypeMultiPoint, xy)), true},
		{"collection of LINESTRING EMPTY", gc(line(xy)), true},
		{"POINT Z(1 2 3)", le(TypePoint, dimsXYZ, coords(1, 2, 3)), true},
		{"LINESTRING M(0 0 1,1 1 2)", line(dimsXYM, 0, 0, 1, 1, 1, 2), true},
		{"collection ZM of a point", collectionOf(TypeGeometryCollection, dimsXYZM, le(TypePoint, dimsXYZM, coords(1, 2, 3, 4))), true},
		{"LINESTRING Z(0 0 1)", line(dimsXYZ, 0, 0, 1), false},
		{"multipolygon Z with a ring of 3", collectionOf(TypeMultiPolygon, dimsXYZ, polygon(dimsXYZ, []float64{0, 0, 1, 1, 0, 1, 0, 0, 1})), false},
	} {
		in := bareOf(0, c.g)
		got, _, _, err := ParseColumnValue(in)
		if c.ok {
			require.NoError(t, err, "%s: %X", c.name, in)
			require.Equal(t, in, got, c.name)
		} else {
			require.ErrorIs(t, err, ErrInvalid, "%s: %X", c.name, in)
		}
	}

	// The envelope takes X and Y only, and an empty point has no position.
	_, _, sum, err := ParseColumnValue(bareOf(0, collectionOf(TypeMultiPoint, dimsXYZM,
		le(TypePoint, dimsXYZM, coords(1, 2, 300, 400)), emptyPoint(dimsXYZM), le(TypePoint, dimsXYZM, coords(-5, 6, -700, 800)))))
	require.NoError(t, err)
	require.Equal(t, Summary{Type: TypeMultiPoint, Envelope: Envelope{-5, 2, 1, 6, true}}, sum)

	for _, h := range []string{
		// CheckBare's rules hold too.
		"00000000 01 01000000 000000000000F03F 00000000000000",
		"00000000 00 00000001 3FF0000000000000 4000000000000000",
		"00000000 01 04000000 01000000 00 00000001 3FF0000000000000 4000000000000000",
		"00000000 01 01000020 E6100000 000000000000F03F 0000000000000040",
		"00000000 01 01000000 000000000000F87F 0000000000000040",
		"00000000 01 01000000 000000000000F07F 0000000000000040",
		"",
		"00000000",
		// A stored value is not a bare value.
		"01 E6100000 01 01000000 000000000000F03F 0000000000000040",
	} {
		_, _, _, err := ParseColumnValue(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, h)
	}
	_, _, _, err = ParseColumnValue(bareChain(MaxDepth+1, point12))
	require.ErrorIs(t, err, ErrTooDeep)
	// A one-point linestring is reported once the whole value is read, so a member nested
	// too deep behind it is reported first.
	_, _, _, err = ParseColumnValue(bareOf(0, gc(line1, nestedCollections(MaxDepth))))
	require.ErrorIs(t, err, ErrTooDeep)
}

// point12M is POINT M(1 2 3).
var point12M = le(TypePoint, dimsXYM, coords(1, 2, 3))

// TestBareRewrite checks that a POINT EMPTY of another NaN is returned as the quiet NaN, in
// a copy, and the input left as it is.
func TestBareRewrite(t *testing.T) {
	want := bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, emptyPoint(dimsXYM)))
	for _, nan := range nanBits {
		f := math.Float64frombits(nan)
		in := bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, le(TypePoint, dimsXYM, coords(f, f, f))))
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

// TestGeneratedBare checks ParseColumnValue on generated values against what the generator
// knows of each: its canonical form, its envelope, whether a line or ring is too short, and
// whether a member is big-endian.
func TestGeneratedBare(t *testing.T) {
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
					got, gotSRID, sum, err := ParseColumnValue(in)
					switch {
					case hasBig:
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						big++
					case gen.short:
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						short++
					default:
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

// TestGeneratedBareMutations checks that ParseColumnValue refuses or canonicalizes
// generated values with one byte changed.
func TestGeneratedBareMutations(t *testing.T) {
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
				if checkBareCanonical(t, b) {
					accepted++
				}
			}
		}
	}
	require.Greater(t, accepted, 1000)
}

// checkBareCanonical requires what ParseColumnValue makes of b, if it accepts b, to be a
// bare value of b's length that CheckBare accepts and that converts to itself with the
// same summary, and reports whether it accepted b.
func checkBareCanonical(t testing.TB, b []byte) bool {
	got, srid, sum, err := ParseColumnValue(b)
	if err != nil {
		return false
	}
	require.Len(t, got, len(b), "%X", b)
	require.NoError(t, CheckBare(got), "%X", b)
	again, againSRID, againSum, err := ParseColumnValue(got)
	require.NoError(t, err, "%X", b)
	require.Equal(t, got, again, "%X", b)
	require.Equal(t, srid, againSRID, "%X", b)
	require.Equal(t, sum, againSum, "%X", b)
	gotSRID, typ, err := Header(got)
	require.NoError(t, err, "%X", b)
	require.Equal(t, srid, gotSRID, "%X", b)
	require.Equal(t, sum.Type, typ, "%X", b)
	return true
}

// TestBareAllocs checks that ParseColumnValue allocates only a rewritten value, and nothing
// for a value it refuses, whatever its counts claim.
func TestBareAllocs(t *testing.T) {
	deep := bareChain(MaxDepth-1, multiPointWKB(10000))
	for _, c := range []struct {
		b      []byte
		allocs float64
	}{
		{deep, 0},
		{bareOf(0, collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(math.NaN(), math.NaN())))), 1},
		{hostileCounts(), 0},
		{deep[:len(deep)-1], 0},
	} {
		allocs := testing.AllocsPerRun(3, func() { _, _, _, _ = ParseColumnValue(c.b) })
		require.Equal(t, c.allocs, allocs, "%X", c.b[:min(len(c.b), 32)])
	}
}

func FuzzParseColumnValue(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(bareOf(0, polygon(dimsXY, []float64{0, 0, 1, 0, 1, 1, 0, 1})))
	f.Add(bareOf(0, collectionOf(TypeGeometryCollection, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12), line(dimsXY))))
	f.Add(bareOf(4326, polygon(dimsXYZ, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1})))
	f.Add(bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, le(TypePoint, dimsXYM, coords(math.NaN(), math.NaN(), math.NaN())))))
	f.Fuzz(func(t *testing.T, b []byte) {
		if !checkBareCanonical(t, b) {
			return
		}
		// What ParseColumnValue accepts, CheckBare accepts.
		require.NoError(t, CheckBare(b), "%X", b)
	})
}
