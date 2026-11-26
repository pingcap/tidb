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
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsStored checks that IsStored never confuses the two formats.
func TestIsStored(t *testing.T) {
	// Every root header; some SRIDs mimic the stored version and byte order, or set 0x20.
	for code := uint32(1); code <= 7; code++ {
		for _, srid := range []uint32{0, 4326, 0x0101, 0x20000101, 0x20202020, math.MaxUint32} {
			little := binary.LittleEndian.AppendUint32(nil, srid)
			little = append(little, byteOrderLittle)
			little = binary.LittleEndian.AppendUint32(little, code)
			big := binary.LittleEndian.AppendUint32(nil, srid)
			big = append(big, byteOrderBig)
			big = binary.BigEndian.AppendUint32(big, code)
			require.False(t, IsStored(little), "%X", little)
			require.False(t, IsStored(big), "%X", big)
		}
		for _, flags := range []uint32{0, ewkbZ, ewkbM, ewkbZ | ewkbM} {
			s := []byte{Version1, byteOrderLittle}
			s = binary.LittleEndian.AppendUint32(s, code|flags|ewkbSRID)
			require.True(t, IsStored(s), "%X", s)
		}
	}
	for n := range 6 {
		require.False(t, IsStored(bytes.Repeat([]byte{0xff}, n)))
	}

	// Generated geometries, stored and as bare values.
	rnd := rand.New(rand.NewPCG(20261019, 1))
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			gen := wkbGen{rnd: rnd, d: d, empty: empty}
			for i := range 300 {
				wkb, _ := gen.geometry(allTypes[i%len(allTypes)], 1)
				srid := gen.srid()
				require.True(t, IsStored(stored(srid, wkb)), "%X", wkb)
				require.False(t, IsStored(mysqlOf(srid, wkb)), "%X", wkb)
			}
		}
	}
}

// headerToBare rewrites only the root header of a stored value into a bare value.
func headerToBare(s []byte) []byte {
	word := binary.LittleEndian.Uint32(s[2:])
	b := binary.LittleEndian.AppendUint32(nil, binary.LittleEndian.Uint32(s[6:]))
	b = append(b, byteOrderLittle)
	b = binary.LittleEndian.AppendUint32(b, word&^ewkbSRID)
	return append(b, s[10:]...)
}

func TestBareToStored(t *testing.T) {
	const xy = dimsXY
	gc := func(members ...[]byte) []byte { return collectionOf(TypeGeometryCollection, xy, members...) }
	square := []float64{0, 0, 1, 0, 1, 1, 0, 1}
	closed := []float64{0, 0, 1, 0, 1, 1, 0, 0}
	hole := []float64{0.2, 0.1, 0.8, 0.1, 0.8, 0.7, 0.2, 0.1}
	line1 := line(xy, 0, 0)

	// Both formats give the same stored value and summary.
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
			want := stored(srid, c.g)
			for _, in := range [][]byte{mysqlOf(srid, c.g), want} {
				got, gotSRID, sum, err := BareToStored(in)
				require.NoError(t, err, "%X", in)
				require.Equal(t, want, got, "%X", in)
				require.Equal(t, srid, gotSRID, "%X", in)
				require.Equal(t, Summary{Type: c.typ, Envelope: c.env}, sum, "%X", in)
			}
		}
	}

	// The structure rules of MySQL's format, as measured on MySQL 9.7.2. A bare value and a
	// stored value follow them alike, except that both hold empty geometries and Z and M,
	// which MySQL's format cannot.
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
		want := stored(0, c.g)
		for _, in := range [][]byte{headerToBare(want), want} {
			got, _, _, err := BareToStored(in)
			if c.ok {
				require.NoError(t, err, "%s: %X", c.name, in)
				require.Equal(t, want, got, c.name)
			} else {
				require.ErrorIs(t, err, ErrInvalid, "%s: %X", c.name, in)
			}
		}
	}

	// The envelope takes X and Y only, and an empty point has no position.
	_, _, sum, err := BareToStored(stored(0, collectionOf(TypeMultiPoint, dimsXYZM,
		le(TypePoint, dimsXYZM, coords(1, 2, 300, 400)), emptyPoint(dimsXYZM), le(TypePoint, dimsXYZM, coords(-5, 6, -700, 800)))))
	require.NoError(t, err)
	require.Equal(t, Summary{Type: TypeMultiPoint, Envelope: Envelope{-5, 2, 1, 6, true}}, sum)

	for b, want := range map[string]error{
		// Another version byte.
		"02 01 01000020 00000000 000000000000F03F 0000000000000040": ErrUnsupportedVersion,
		// A byte short, and a byte long.
		"01 01 01000020 00000000 000000000000F03F 00000000000000":      ErrInvalid,
		"01 01 01000020 00000000 000000000000F03F 0000000000000040 00": ErrInvalid,
		// EWKB without the SRID flag: neither format.
		"01 01 01000000 000000000000F03F 0000000000000040": ErrInvalid,
		// A big-endian stored root: neither format.
		"01 00 20000001 00000000 3FF0000000000000 4000000000000000": ErrInvalid,
		// Big-endian WKB is not a bare value, nor is the SRID flag, since the SRID is the
		// prefix.
		"00000000 00 00000001 3FF0000000000000 4000000000000000":                               ErrInvalid,
		"00000000 01 01000020 E6100000 000000000000F03F 0000000000000040":                      ErrInvalid,
		"00000000 01 04000000 01000000 01 01000020 E6100000 000000000000F03F 0000000000000040": ErrInvalid,
		// NaN and infinite coordinates.
		"00000000 01 01000000 000000000000F87F 0000000000000040":    ErrInvalid,
		"01 01 01000020 00000000 000000000000F07F 0000000000000040": ErrInvalid,
		"":         ErrInvalid,
		"00000000": ErrInvalid,
	} {
		_, _, _, err := BareToStored(mustHex(t, b))
		require.ErrorIs(t, err, want, b)
	}
	_, _, _, err = BareToStored(storedChain(MaxDepth+1, 0, point12))
	require.ErrorIs(t, err, ErrTooDeep)
	_, _, _, err = BareToStored(mysqlChain(MaxDepth+1, point12))
	require.ErrorIs(t, err, ErrTooDeep)
	// A one-point linestring is reported once the whole value is read, so a member nested
	// too deep behind it is reported first.
	deep := nestedCollections(MaxDepth)
	_, _, _, err = BareToStored(stored(0, gc(line1, deep)))
	require.ErrorIs(t, err, ErrTooDeep)
	_, _, _, err = BareToStored(mysqlOf(0, gc(line1, deep)))
	require.ErrorIs(t, err, ErrTooDeep)
}

// TestBareRewrite checks the canonical form of stored values that are not in it: a
// big-endian member becomes little-endian and POINT EMPTY the quiet NaN.
// point12M is POINT M(1 2 3).
var point12M = le(TypePoint, dimsXYM, coords(1, 2, 3))

func TestBareRewrite(t *testing.T) {
	// A bare value's POINT EMPTY of another NaN becomes the quiet NaN too.
	for _, nan := range nanBits {
		f := math.Float64frombits(nan)
		got, _, _, err := BareToStored(mysqlOf(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, le(TypePoint, dimsXYM, coords(f, f, f)))))
		require.NoError(t, err)
		require.Equal(t, stored(4326, collectionOf(TypeMultiPoint, dimsXYM, point12M, emptyPoint(dimsXYM))), got)
	}

	got, _, _, err := BareToStored(mustHex(t, "01 01 04000020 00000000 01000000 00 00000001 3FF0000000000000 4000000000000000"))
	require.NoError(t, err)
	require.Equal(t, stored(0, collectionOf(TypeMultiPoint, dimsXY, point12)), got)

	const d = dimsXYZ
	ring := []float64{0, 0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1}
	want := stored(4326, collectionOf(TypeGeometryCollection, d,
		emptyPoint(d),
		collectionOf(TypeMultiPoint, d, emptyPoint(d), le(TypePoint, d, coords(1, 2, 3))),
		polygon(d, ring)))
	wantSum := Summary{Type: TypeGeometryCollection, Envelope: Envelope{0, 0, 1, 2, true}}
	for _, nan := range nanBits {
		for _, big := range []bool{false, true} {
			var order binary.AppendByteOrder = binary.LittleEndian
			orderByte := byteOrderLittle
			if big {
				order, orderByte = binary.BigEndian, byteOrderBig
			}
			// The same value with every member in order and every empty point as nan.
			header := func(b []byte, typ Type) []byte {
				return order.AppendUint32(append(b, orderByte), uint32(typ)|ewkbZ)
			}
			floats := func(b []byte, fs ...float64) []byte {
				for _, f := range fs {
					b = order.AppendUint64(b, math.Float64bits(f))
				}
				return b
			}
			empty := func(b []byte) []byte {
				b = header(b, TypePoint)
				for range 3 {
					b = order.AppendUint64(b, nan)
				}
				return b
			}
			b := []byte{Version1, byteOrderLittle}
			b = binary.LittleEndian.AppendUint32(b, 7|ewkbZ|ewkbSRID)
			b = binary.LittleEndian.AppendUint32(b, 4326)
			b = binary.LittleEndian.AppendUint32(b, 3)
			b = empty(b)
			b = order.AppendUint32(header(b, TypeMultiPoint), 2)
			b = empty(b)
			b = floats(header(b, TypePoint), 1, 2, 3)
			b = order.AppendUint32(header(b, TypePolygon), 1)
			b = floats(order.AppendUint32(b, 4), ring...)
			in := bytes.Clone(b)

			got, srid, sum, err := BareToStored(b)
			require.NoError(t, err, "%X", b)
			require.Equal(t, want, got, "%X", b)
			require.Equal(t, uint32(4326), srid)
			require.Equal(t, wantSum, sum)
			// The input is left as it is.
			require.Equal(t, in, b)
			require.Equal(t, nan == quietNaN && !big, bytes.Equal(b, want))
		}
	}
}

// TestGeneratedBare checks BareToStored on generated values against what the generator
// knows of each: its canonical form, its envelope, and whether a line or ring is too short.
func TestGeneratedBare(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261119, 1))
	accepted, short, rewritten, fromMySQL := 0, 0, 0, 0
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			for _, big := range []float64{0, 0.3} {
				gen := wkbGen{rnd: rnd, d: d, empty: empty, big: big}
				for i := range 500 {
					typ := allTypes[i%len(allTypes)]
					wkb, _ := gen.geometry(typ, 1)
					srid := gen.srid()
					in := stored(srid, wkb)
					want := stored(srid, gen.canon)

					got, gotSRID, sum, err := BareToStored(in)
					if gen.short {
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						short++
					} else {
						require.NoError(t, err, "%X", in)
						require.Equal(t, want, got, "%X", in)
						require.Equal(t, srid, gotSRID, "%X", in)
						require.Equal(t, Summary{Type: typ, Envelope: gen.env}, sum, "%X", in)
						accepted++
						if !bytes.Equal(in, want) {
							rewritten++
						}
					}
					// The canonical form is little-endian, as a bare value must be.
					got, gotSRID, sum, err = BareToStored(mysqlOf(srid, gen.canon))
					if gen.short {
						require.ErrorIs(t, err, ErrInvalid, "%X", in)
						continue
					}
					require.NoError(t, err, "%X", in)
					require.Equal(t, want, got, "%X", in)
					require.Equal(t, srid, gotSRID, "%X", in)
					require.Equal(t, Summary{Type: typ, Envelope: gen.env}, sum, "%X", in)
					fromMySQL++
				}
			}
		}
	}
	require.Greater(t, accepted, 3000)
	require.Greater(t, short, 1000)
	require.Greater(t, rewritten, 500)
	require.Greater(t, fromMySQL, 300)
}

// TestGeneratedBareMutations checks that BareToStored refuses or canonicalizes generated
// values with one byte changed: what it accepts converts to itself.
func TestGeneratedBareMutations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261119, 2))
	changes := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf0, 0xf8, 0x7f, 0xff}
	accepted := 0
	for _, d := range []dims{dimsXY, dimsXYZM} {
		gen := wkbGen{rnd: rnd, d: d, empty: 0.15, big: 0.3}
		for i := range 300 {
			wkb, _ := gen.geometry(allTypes[i%len(allTypes)], 1)
			forms := [][]byte{stored(4326, wkb), mysqlOf(4326, gen.canon)}
			for _, form := range forms {
				for range 20 {
					b := bytes.Clone(form)
					b[rnd.IntN(len(b))] = changes[rnd.IntN(len(changes))]
					if checkBareCanonical(t, b) {
						accepted++
					}
				}
			}
		}
	}
	require.Greater(t, accepted, 1000)
}

// checkBareCanonical requires what BareToStored makes of b, if it accepts b, to be a
// stored value of b's length that converts to itself with the same summary, and reports
// whether it accepted b.
func checkBareCanonical(t testing.TB, b []byte) bool {
	s, srid, sum, err := BareToStored(b)
	if err != nil {
		return false
	}
	wantLen := len(b) + 1
	if IsStored(b) {
		wantLen = len(b)
	}
	require.Len(t, s, wantLen, "%X", b)
	require.True(t, IsStored(s), "%X", b)
	again, againSRID, againSum, err := BareToStored(s)
	require.NoError(t, err, "%X", b)
	require.Equal(t, s, again, "%X", b)
	require.Equal(t, srid, againSRID, "%X", b)
	require.Equal(t, sum, againSum, "%X", b)
	gotSRID, typ, err := Header(s)
	require.NoError(t, err, "%X", b)
	require.Equal(t, srid, gotSRID, "%X", b)
	require.Equal(t, sum.Type, typ, "%X", b)
	return true
}

// TestBareAllocs checks that BareToStored allocates only its result, and nothing for a
// value it refuses, whatever its counts claim.
func TestBareAllocs(t *testing.T) {
	deep := mysqlChain(MaxDepth-1, multiPointWKB(10000))
	bigMember := mustHex(t, "01 01 04000020 00000000 01000000 00 00000001 3FF0000000000000 4000000000000000")
	for _, c := range []struct {
		b      []byte
		allocs float64
	}{
		{deep, 1},
		{storedFromMySQL(deep), 1},
		{bigMember, 1},
		{stored(0, collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(math.NaN(), math.NaN())))), 1},
		{mysqlOf(0, collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(math.NaN(), math.NaN())))), 1},
		{hostileCounts(), 0},
		{storedFromMySQL(hostileCounts()), 0},
		{deep[:len(deep)-1], 0},
	} {
		allocs := testing.AllocsPerRun(3, func() { _, _, _, _ = BareToStored(c.b) })
		require.Equal(t, c.allocs, allocs, "%X", c.b[:min(len(c.b), 32)])
	}
}

func FuzzBareToStored(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(mysqlOf(0, polygon(dimsXY, []float64{0, 0, 1, 0, 1, 1, 0, 1})))
	f.Add(stored(4326, point12))
	f.Add(stored(0, collectionOf(TypeGeometryCollection, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12), line(dimsXY))))
	f.Add(stored(4326, polygon(dimsXYZ, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1})))
	f.Add(mustHex(f, "01 01 04000020 00000000 01000000 00 00000001 3FF0000000000000 4000000000000000"))
	f.Fuzz(func(t *testing.T, b []byte) {
		// The formats never overlap.
		if _, _, err := MySQLToStored(b); err == nil {
			require.False(t, IsStored(b), "%X", b)
		}
		if !checkBareCanonical(t, b) {
			return
		}
		s, _, _, err := BareToStored(b)
		require.NoError(t, err, "%X", b)
		// Round trip through the bare value.
		out, err := StoredToBare(s)
		require.NoError(t, err, "%X", b)
		back, _, _, err := BareToStored(out)
		require.NoError(t, err, "%X", b)
		require.Equal(t, s, back, "%X", b)
	})
}
