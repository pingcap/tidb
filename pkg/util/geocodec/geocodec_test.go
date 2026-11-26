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
	line0011 = line(dimsXY, 0, 0, 1, 1)
	triangle = polygon(dimsXY, []float64{0, 0, 0, 1, 1, 1, 0, 0})
)

// checkAccepted requires a bare value b that CheckBare accepts to agree with Header and to
// read back unchanged through the stored format.
func checkAccepted(t testing.TB, b []byte) {
	srid, typ, err := Header(b)
	require.NoError(t, err, "%X", b)
	require.Equal(t, binary.LittleEndian.Uint32(b), srid, "%X", b)
	require.Equal(t, Type(binary.LittleEndian.Uint32(b[5:])&^ewkbFlags), typ, "%X", b)
	s := BareToStored(b)
	require.Equal(t, Version1, s[0], "%X", b)
	back, err := StoredToBare(s)
	require.NoError(t, err, "%X", b)
	require.Equal(t, b, back, "%X", b)
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
	}
	for name, h := range rejects {
		require.ErrorIs(t, CheckBare(mustHex(t, h)), ErrInvalid, name)
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
	values := map[Type][][]byte{
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
		le(TypePoint, dimsXYM, coords(1, 2, 4)),
		le(TypePoint, dimsXYZM, coords(1, 2, 3, 4)),
		line(dimsXYZ, 0, 0, 1, 1, 1, 1),
		polygon(dimsXYM, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1}),
		collectionOf(TypeMultiPoint, dimsXYZM, le(TypePoint, dimsXYZM, coords(1, 2, 3, 4))),
		collectionOf(TypeMultiLineString, dimsXYM, line(dimsXYM, 0, 0, 1, 1, 1, 1)),
		collectionOf(TypeMultiPolygon, dimsXYZ, polygon(dimsXYZ, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1})),
		collectionOf(gc, dimsXYZ),
		collectionOf(gc, dimsXYM, collectionOf(gc, dimsXYM, le(TypePoint, dimsXYM, coords(1, 2, 3)))),
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
		checkAccepted(t, b)
	}
	// POINT EMPTY of any NaN.
	for _, nan := range nanBits {
		f := math.Float64frombits(nan)
		require.NoError(t, CheckBare(bareOf(0, le(TypePoint, dimsXYM, coords(f, f, f)))))
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

// randomBytes returns random bytes drawn mostly from values meaningful in a header, so
// that parsing gets past the first few bytes.
func randomBytes(rnd *rand.Rand, n int) []byte {
	alphabet := []byte{0, 1, 2, 3, 4, 5, 6, 7, 0x20, 0x40, 0x80, 0xf0, 0x3f, 0x7f, 0xf8, 0xff}
	b := make([]byte, n)
	for i := range b {
		if rnd.IntN(4) == 0 {
			b[i] = byte(rnd.Uint32())
		} else {
			b[i] = alphabet[rnd.IntN(len(alphabet))]
		}
	}
	return b
}

// checkAny requires CheckBare to refuse b with ErrInvalid or ErrTooDeep, or to accept it
// consistently with Header and the stored format.
func checkAny(t testing.TB, b []byte) {
	err := CheckBare(b)
	if err != nil {
		require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep), "%X: %v", b, err)
		return
	}
	checkAccepted(t, b)
}

func TestGarbage(t *testing.T) {
	for _, s := range []string{"", "x", "POINT(1 2)", "\x00\x00\x00\x00", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"} {
		require.Error(t, CheckBare([]byte(s)))
		_, _, _ = Header([]byte(s))
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		b := randomBytes(rnd, rnd.IntN(80))
		checkAny(t, b)
		_, _, _ = Header(b)
	}
}

// TestGenerated checks CheckBare on generated values of every type and coordinate type,
// with and without empty and big-endian members, against what the generator knows of each.
func TestGenerated(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261111, 1))
	accepted, bigEndian := 0, 0
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			for _, big := range []float64{0, 0.3} {
				gen := wkbGen{rnd: rnd, d: d, empty: empty, big: big}
				for i := range 500 {
					typ := allTypes[i%len(allTypes)]
					wkb, hasBig := gen.geometry(typ, 1)
					srid := gen.srid()
					b := bareOf(srid, wkb)
					// Header reads only the root, which the generator writes little-endian.
					gotSRID, gotType, err := Header(b)
					require.NoError(t, err, "%X", b)
					require.Equal(t, srid, gotSRID)
					require.Equal(t, typ, gotType)
					if hasBig {
						require.ErrorIs(t, CheckBare(b), ErrInvalid, "%X", b)
						bigEndian++
						continue
					}
					require.NoError(t, CheckBare(b), "%X", b)
					checkAccepted(t, b)
					accepted++
				}
			}
		}
	}
	require.Greater(t, accepted, 4000)
	require.Greater(t, bigEndian, 1000)
}

// TestGeneratedMutations checks CheckBare on generated values with one byte changed, which
// reach the checks a well-formed value does not.
func TestGeneratedMutations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261111, 2))
	values := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf0, 0xf8, 0x7f, 0xff}
	for _, d := range []dims{dimsXY, dimsXYZM} {
		gen := wkbGen{rnd: rnd, d: d, empty: 0.1}
		for i := range 100 {
			wkb, _ := gen.geometry(allTypes[i%len(allTypes)], 1)
			b := bareOf(gen.srid(), wkb)
			for _, pos := range rnd.Perm(len(b))[:min(len(b), 100)] {
				m := append([]byte{}, b...)
				for _, v := range append(values, b[pos]^byte(1+rnd.IntN(255))) {
					m[pos] = v
					checkAny(t, m)
				}
			}
		}
	}
}

// TestAllocs checks that CheckBare and StoredToBare allocate nothing, whatever the input's
// counts claim, and that BareToStored allocates only its result.
func TestAllocs(t *testing.T) {
	deep := bareChain(MaxDepth-1, multiPointWKB(10000))
	for _, b := range [][]byte{deep, hostileCounts(), deep[:len(deep)-1]} {
		require.Zero(t, testing.AllocsPerRun(3, func() { _ = CheckBare(b) }), "%X", b[:min(len(b), 32)])
	}
	s := BareToStored(deep)
	require.Zero(t, testing.AllocsPerRun(3, func() { _, _ = StoredToBare(s) }))
	require.Equal(t, 1.0, testing.AllocsPerRun(3, func() { _ = BareToStored(deep) }))
}

func FuzzCheckBare(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(bareOf(0, collectionOf(TypeGeometryCollection, dimsXY, triangle)))
	f.Add(bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, le(TypePoint, dimsXYM, coords(1, 2, 3)), emptyPoint(dimsXYM))))
	f.Add(bareChain(MaxDepth, point12))
	f.Fuzz(func(t *testing.T, b []byte) {
		require.Zero(t, testing.AllocsPerRun(1, func() { _ = CheckBare(b) }), "%X", b)
		checkAny(t, b)
		_, _, _ = Header(b)
	})
}
