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
	line0011 = line(dimsXY, 0, 0, 1, 1)
	triangle = polygon(dimsXY, []float64{0, 0, 0, 1, 1, 1, 0, 0})
)

// checkStoredToBare requires StoredToBare to agree with Header on a stored value b: it
// refuses what Header refuses, and otherwise rewrites only the root header, so the bare
// value reads back as b whenever BareToStored accepts it. It never panics, whatever b
// holds after its root header, which no writer stores broken.
func checkStoredToBare(t testing.TB, b []byte) {
	got, err := StoredToBare(b)
	srid, _, headerErr := Header(b)
	if headerErr != nil {
		require.Equal(t, headerErr, err, "%X", b)
		return
	}
	require.NoError(t, err, "%X", b)
	require.Len(t, got, len(b)-1, "%X", b)
	require.Equal(t, srid, binary.LittleEndian.Uint32(got), "%X", b)
	require.Equal(t, byteOrderLittle, got[4], "%X", b)
	require.Equal(t, binary.LittleEndian.Uint32(b[2:])&^ewkbSRID, binary.LittleEndian.Uint32(got[5:]), "%X", b)
	require.Equal(t, b[10:], got[9:], "%X", b)
	again, againSRID, err := BareToStored(got)
	if err != nil {
		// A body the stored format allows and the bare value does not, such as a
		// big-endian member, or a broken one.
		require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep), "%X: %v", b, err)
		return
	}
	require.Equal(t, b, again, "%X", b)
	require.Equal(t, srid, againSRID, "%X", b)
}

func TestMySQLFormat(t *testing.T) {
	// Bytes from MySQL 9.7.2: HEX(ST_GeomFromText('POINT(1 2)', 4326)), latitude 1 and
	// longitude 2, which MySQL holds longitude first, as the stored format does.
	mysql4326 := mustHex(t, "E6100000 01 01000000 0000000000000040 000000000000F03F")
	s, srid, err := BareToStored(mysql4326)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)

	// The conversion rewrites the header only: the WKB body is the same bytes.
	require.Equal(t, mustHex(t, "01 01 01000020 E6100000 0000000000000040 000000000000F03F"), s)
	back, err := StoredToBare(s)
	require.NoError(t, err)
	require.Equal(t, mysql4326, back)

	// Any SRID passes through, since no SRS axis order is applied.
	mysql12345 := mustHex(t, "39300000 01 01000000 000000000000F03F 0000000000000040")
	s, srid, err = BareToStored(mysql12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), srid)
	back, err = StoredToBare(s)
	require.NoError(t, err)
	require.Equal(t, mysql12345, back)

	// The smallest valid value: GEOMETRYCOLLECTION EMPTY, 13 bytes.
	smallest := mustHex(t, "00000000 01 07000000 00000000")
	s, _, err = BareToStored(smallest)
	require.NoError(t, err)
	back, err = StoredToBare(s)
	require.NoError(t, err)
	require.Equal(t, smallest, back)

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
		"too short for an SRID":    "000000",
		"no geometry":              "E6100000",
	}
	for name, h := range rejects {
		_, _, err := BareToStored(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

func TestStoredLayout(t *testing.T) {
	// Every stored value carries the EWKB SRID flag and its SRID, 0 included.
	for srid, h := range map[uint32]string{
		0:    "01 01 01000020 00000000 000000000000F03F 0000000000000040",
		4326: "01 01 01000020 E6100000 000000000000F03F 0000000000000040",
	} {
		s, _, err := BareToStored(bareOf(srid, point12))
		require.NoError(t, err)
		require.Equal(t, mustHex(t, h), s)
	}
}

// TestStoredWithoutSRID checks that every reader of the stored format refuses a root
// without the SRID flag, as ErrInvalid.
func TestStoredWithoutSRID(t *testing.T) {
	for _, h := range []string{
		"01 01 01000000 000000000000F03F 0000000000000040",
		"01 01 07000000 00000000",
		// A root without the flag is refused before its other faults.
		"01 01 08000000",
		"01 01 07000000 FFFFFFFF",
	} {
		b := mustHex(t, h)
		_, err := StoredToBare(b)
		require.ErrorIs(t, err, errStoredWithoutSRID, h)
		require.ErrorIs(t, err, ErrInvalid, h)
		_, _, err = Header(b)
		require.ErrorIs(t, err, errStoredWithoutSRID, h)
		checkStoredToBare(t, b)
	}
	// With the flag, the same value is read.
	_, _, err := Header(mustHex(t, "01 01 01000020 00000000 000000000000F03F 0000000000000040"))
	require.NoError(t, err)
}

// storedRejects are stored values whose structure is broken, which validate refuses.
// StoredToBare refuses those whose root header is broken, and converts the others from
// their root header alone.
func storedRejects(t testing.TB) map[string][]byte {
	good := stored(4326, point12)
	cases := map[string][]byte{
		"empty":        nil,
		"version only": {Version1},
		"a byte short": good[:len(good)-1],
		"a byte long":  append(append([]byte{}, good...), 0),
	}
	for name, h := range map[string]string{
		"bad byte order":        "01 02 01000020 00000000 000000000000F03F 0000000000000040",
		"unknown type code":     "01 01 08000020 00000000",
		"ISO type code in EWKB": "01 01 E9030020 00000000 000000000000F03F 0000000000000040 0000000000000840",
		"hostile count":         "01 01 07000020 00000000 FFFFFFFF",
		"hostile point count":   "01 01 02000020 00000000 FFFFFF0F",
		"Z point a byte short":  "01 01 010000A0 00000000 000000000000F03F 0000000000000040 00000000000008",
		"empty point then junk": "01 01 07000020 00000000 02000000 01 01000000 000000000000F87F 000000000000F87F" +
			" 01 09000000",
	} {
		cases[name] = mustHex(t, h)
	}
	return cases
}

func TestStoredRejects(t *testing.T) {
	for name, b := range storedRejects(t) {
		if len(b) > 0 {
			require.ErrorIs(t, validate(b[1:], dialectEWKB), ErrInvalid, name)
		}
		checkStoredToBare(t, b)
	}
	// The root header alone decides.
	for _, name := range []string{"empty", "version only", "bad byte order", "unknown type code", "ISO type code in EWKB"} {
		_, err := StoredToBare(storedRejects(t)[name])
		require.Error(t, err, name)
	}
	for _, name := range []string{"a byte short", "a byte long", "hostile count", "Z point a byte short"} {
		_, err := StoredToBare(storedRejects(t)[name])
		require.NoError(t, err, name)
	}
}

// storedTrusted are stored values that break a rule of the format but not its structure,
// which no writer stores.
var storedTrusted = []struct{ name, hex string }{
	{"SRID flag below the root", "01 01 04000020 00000000 01000000 01 01000020 E6100000 000000000000F03F 0000000000000040"},
	{"member of the wrong type", "01 01 04000020 00000000 01000000 01 02000000 00000000"},
	{"member dimension differs", "01 01 04000020 00000000 01000000 01 01000080 000000000000F03F 0000000000000040 0000000000000840"},
	{"ring without points", "01 01 03000020 00000000 01000000 00000000"},
	{"partly NaN point", "01 01 01000020 00000000 000000000000F87F 0000000000000040"},
	{"infinite coordinate", "01 01 01000020 00000000 000000000000F07F 0000000000000040"},
	{"NaN in a linestring", "01 01 02000020 00000000 01000000 000000000000F87F 000000000000F87F"},
}

// TestStoredTrusted records what StoredToBare does with a value that breaks a rule of the
// format but not its structure: validate refuses each, and StoredToBare copies the body,
// which the bare value's reader refuses in turn.
func TestStoredTrusted(t *testing.T) {
	for _, c := range storedTrusted {
		b := mustHex(t, c.hex)
		require.ErrorIs(t, validate(b[1:], dialectEWKB), ErrInvalid, c.name)
		bare, err := StoredToBare(b)
		require.NoError(t, err, c.name)
		_, _, err = BareToStored(bare)
		require.ErrorIs(t, err, ErrInvalid, c.name)
		checkStoredToBare(t, b)
	}
}

func TestVersion(t *testing.T) {
	good := stored(4326, point12)
	for _, version := range []byte{0, 2, 0x7f, 0xff} {
		b := append([]byte{version}, good[1:]...)
		_, err := StoredToBare(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
		_, _, err = Header(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
	}
	// The version is checked before anything else, so it is reported for a payload that
	// is missing or malformed too.
	for _, b := range [][]byte{{0}, {2}, {2, 0xff}} {
		_, err := StoredToBare(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
		_, _, err = Header(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
	}
}

func TestStoredBigEndian(t *testing.T) {
	// POINT(1 2) with SRID 4326, big-endian EWKB, which a client may send.
	s := mustHex(t, "01 00 20000001 000010E6 3FF0000000000000 4000000000000000")
	require.NoError(t, validate(s[1:], dialectEWKB))

	// A stored value is little-endian, which is all TiDB writes: a big-endian root is
	// refused, with or without the SRID flag, whole or cut short.
	for _, b := range [][]byte{
		s,
		mustHex(t, "01 00 A0000001 000010E6 3FF0000000000000 4000000000000000 4008000000000000"),
		mustHex(t, "01 00 00000001 3FF0000000000000 4000000000000000"),
		mustHex(t, "01 00 20000001"),
		mustHex(t, "01 00"),
	} {
		_, err := StoredToBare(b)
		require.ErrorIs(t, err, errBigEndianStored, "%X", b)
		require.ErrorIs(t, err, ErrInvalid, "%X", b)
		_, _, err = Header(b)
		require.ErrorIs(t, err, errBigEndianStored, "%X", b)
		checkStoredToBare(t, b)
	}

	// A big-endian member is below the root header, which is all StoredToBare and Header
	// read; the bare value's reader refuses it.
	member := mustHex(t, "01 01 04000020 E6100000 01000000 00 00000001 3FF0000000000000 4000000000000000")
	bare, err := StoredToBare(member)
	require.NoError(t, err)
	_, _, err = BareToStored(bare)
	require.ErrorIs(t, err, ErrInvalid)
	srid, typ, err := Header(member)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, TypeMultiPoint, typ)
	checkStoredToBare(t, member)
}

// TestBareFrame checks values MySQL's binary format cannot hold: the bare value keeps
// their Z and M flags and their empty geometries, and reads back as the same stored value.
func TestBareFrame(t *testing.T) {
	gc := TypeGeometryCollection
	values := [][]byte{
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
		collectionOf(gc, dimsXYZM, collectionOf(gc, dimsXYZM, collectionOf(gc, dimsXYZM))),
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
		collectionOf(gc, dimsXY, emptyPoint(dimsXY)),
		collectionOf(gc, dimsXY, point12, collectionOf(gc, dimsXY, line(dimsXY))),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY, collectionOf(gc, dimsXY, collectionOf(TypeMultiPolygon, dimsXY)))),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY, collectionOf(TypeMultiLineString, dimsXY, line0011, line(dimsXY)))),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY), collectionOf(TypeMultiPoint, dimsXY, emptyPoint(dimsXY))),
		// Empty collections, which MySQL's format holds too.
		collectionOf(gc, dimsXY),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY), point12),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY, collectionOf(gc, dimsXY))),
	}
	for _, v := range values {
		s := stored(4326, v)
		bare, err := StoredToBare(s)
		require.NoError(t, err, "%X", s)
		require.Equal(t, bareOf(4326, v), bare)
		back, srid, err := BareToStored(bare)
		require.NoError(t, err, "%X", bare)
		require.Equal(t, s, back)
		require.Equal(t, uint32(4326), srid)
		checkStoredToBare(t, s)
	}

	// A POINT EMPTY of another NaN reads back as it was written.
	for _, nan := range nanBits {
		p := le(TypePoint, dimsXYM, coords(math.Float64frombits(nan), math.Float64frombits(nan), math.Float64frombits(nan)))
		s, _, err := BareToStored(bareOf(0, p))
		require.NoError(t, err, "%X", p)
		require.Equal(t, stored(0, p), s)
	}

	// At the depth bound, as the deepest member.
	for _, c := range []struct {
		bare, stored []byte
	}{
		{bareChain(MaxDepth, emptyPoint(dimsXY)), storedChain(MaxDepth, 0, emptyPoint(dimsXY))},
		{bareChain(MaxDepth, line(dimsXY)), storedChain(MaxDepth, 0, line(dimsXY))},
	} {
		s, _, err := BareToStored(c.bare)
		require.NoError(t, err)
		require.Equal(t, c.stored, s)
		bare, err := StoredToBare(s)
		require.NoError(t, err)
		require.Equal(t, c.bare, bare)
	}
}

func TestDepthBound(t *testing.T) {
	// One bound applies to both directions, so a value that is stored converts back.
	garbage := bytes.Repeat([]byte{0xff}, minGeometrySize)
	for _, c := range []struct {
		bare   []byte
		stored []byte
	}{
		{append(make([]byte, 4), nestedCollections(MaxDepth)...), storedChain(MaxDepth, 0, nestedCollections(1))},
		{bareChain(MaxDepth, point12), storedChain(MaxDepth, 0, point12)},
	} {
		s, _, err := BareToStored(c.bare)
		require.NoError(t, err)
		require.Equal(t, c.stored, s)
		back, err := StoredToBare(s)
		require.NoError(t, err)
		require.Equal(t, c.bare, back)
		checkStoredToBare(t, s)
	}

	for _, c := range []struct {
		bare   []byte
		stored []byte
	}{
		{append(make([]byte, 4), nestedCollections(MaxDepth+1)...), storedChain(MaxDepth+1, 0, nestedCollections(1))},
		{bareChain(MaxDepth+1, point12), storedChain(MaxDepth+1, 0, point12)},
		// Depth is checked before anything else in a geometry. Its parent's count still
		// needs room for the smallest geometry.
		{bareChain(MaxDepth+1, garbage), storedChain(MaxDepth+1, 0, garbage)},
		{nil, storedChain(MaxDepth+1, ewkbZ, mustHex(t, "01 01000080 000000000000F87F 000000000000F87F 000000000000F87F"))},
	} {
		if c.bare != nil {
			_, _, err := BareToStored(c.bare)
			require.ErrorIs(t, err, ErrTooDeep)
		}
		require.ErrorIs(t, validate(c.stored[1:], dialectEWKB), ErrTooDeep)
		checkStoredToBare(t, c.stored)
		// Header reads only the root, so depth does not matter to it.
		srid, typ, err := Header(c.stored)
		require.NoError(t, err)
		require.Equal(t, uint32(0), srid)
		require.Equal(t, TypeGeometryCollection, typ)
	}

	// Errors are reported in reading order: a malformed member ahead of a too-deep one
	// is ErrInvalid, and behind it ErrTooDeep.
	root := mustHex(t, "01 01 07000020 00000000 02000000")
	bad := mustHex(t, "01 09000000 00000000")
	deep := nestedCollections(MaxDepth)
	badFirst := append(append(append([]byte{}, root...), bad...), deep...)
	deepFirst := append(append(append([]byte{}, root...), deep...), bad...)
	require.ErrorIs(t, validate(badFirst[1:], dialectEWKB), ErrInvalid)
	require.ErrorIs(t, validate(deepFirst[1:], dialectEWKB), ErrTooDeep)
	_, _, err := BareToStored(bareOf(0, badFirst[2:]))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestTruncatedAndOverLong(t *testing.T) {
	gc := TypeGeometryCollection
	values := [][]byte{
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
	}
	for _, v := range values {
		s := stored(4326, v)
		for n := 1; n < len(s); n++ {
			require.ErrorIs(t, validate(s[1:n], dialectEWKB), ErrInvalid, "%X cut to %d", s, n)
			checkStoredToBare(t, s[:n])
		}
		for _, extra := range [][]byte{{0}, {1}, s[1:]} {
			long := append(append([]byte{}, s...), extra...)
			require.ErrorIs(t, validate(long[1:], dialectEWKB), ErrInvalid, "%X", long)
			checkStoredToBare(t, long)
		}

		bare, err := StoredToBare(s)
		require.NoError(t, err, "%X", s)
		for n := range bare {
			_, _, err := BareToStored(bare[:n])
			require.ErrorIs(t, err, ErrInvalid, "%X cut to %d", bare, n)
		}
		for _, extra := range [][]byte{{0}, {1}, bare[4:]} {
			_, _, err := BareToStored(append(append([]byte{}, bare...), extra...))
			require.ErrorIs(t, err, ErrInvalid, "%X", bare)
		}
	}
}

// randomBytes returns random bytes drawn mostly from values that are meaningful in a header,
// so that parsing gets past the first few bytes.
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

func TestGarbage(t *testing.T) {
	for _, s := range []string{"", "x", "POINT(1 2)", "\x00\x00\x00\x00", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"} {
		_, _, err := BareToStored([]byte(s))
		require.Error(t, err)
		_, err = StoredToBare([]byte(s))
		require.Error(t, err)
		_, _, _ = Header([]byte(s))
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		b := randomBytes(rnd, rnd.IntN(80))
		if _, _, err := BareToStored(b); err != nil {
			require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep), "%X: %v", b, err)
		}
		s := append([]byte{Version1}, b...)
		checkStoredToBare(t, s)
		_, _, _ = Header(s)
	}
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
				s := stored(srid, v)
				gotSRID, typ, err := Header(s)
				require.NoError(t, err, "%X", s)
				require.Equal(t, srid, gotSRID)
				require.Equal(t, want, typ)

				// Header reads the version, byte order, type word and SRID, and no more.
				gotSRID, typ, err = Header(s[:10])
				require.NoError(t, err, "%X", s)
				require.Equal(t, srid, gotSRID)
				require.Equal(t, want, typ)
				for n := range 10 {
					_, _, err = Header(s[:n])
					require.ErrorIs(t, err, ErrInvalid, "%X cut to %d", s, n)
				}
			}
		}
	}

	// Without the SRID flag the value is refused.
	_, _, err := Header(mustHex(t, "01 01 05000000"))
	require.ErrorIs(t, err, errStoredWithoutSRID)
	require.ErrorIs(t, err, ErrInvalid)

	for name, h := range map[string]string{
		"bad byte order":    "01 02 01000020 00000000",
		"unknown type code": "01 01 08000020 00000000",
		"ISO type code":     "01 01 E9030020 00000000",
	} {
		_, _, err := Header(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

// TestGenerated checks the conversions on generated values of every type and coordinate
// type, with and without empty members and big-endian members, against what the generator
// knows about each.
func TestGenerated(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261111, 1))
	roundTrip, bigEndian := 0, 0
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			for _, big := range []float64{0, 0.3} {
				gen := wkbGen{rnd: rnd, d: d, empty: empty, big: big}
				for i := range 500 {
					typ := allTypes[i%len(allTypes)]
					wkb, hasBig := gen.geometry(typ, 1)
					srid := gen.srid()
					s := stored(srid, wkb)

					require.NoError(t, validate(s[1:], dialectEWKB), "%X", s)
					gotSRID, gotType, err := Header(s)
					require.NoError(t, err, "%X", s)
					require.Equal(t, srid, gotSRID)
					require.Equal(t, typ, gotType)
					checkStoredToBare(t, s)

					bare, err := StoredToBare(s)
					require.NoError(t, err, "%X", s)
					require.Equal(t, bareOf(srid, wkb), bare)
					again, gotSRID, err := BareToStored(bare)
					if hasBig {
						require.ErrorIs(t, err, ErrInvalid, "%X", bare)
						bigEndian++
						continue
					}
					require.NoError(t, err, "%X", bare)
					require.Equal(t, s, again)
					require.Equal(t, srid, gotSRID)
					roundTrip++
				}
			}
		}
	}
	require.Greater(t, roundTrip, 4000)
	require.Greater(t, bigEndian, 1000)
}

// TestGeneratedMutations checks StoredToBare and BareToStored on generated values with one
// byte changed, which reach the checks a well-formed value does not.
func TestGeneratedMutations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261111, 2))
	values := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf0, 0xf8, 0x7f, 0xff}
	for _, d := range []dims{dimsXY, dimsXYZM} {
		gen := wkbGen{rnd: rnd, d: d, empty: 0.1, big: 0.1}
		for i := range 100 {
			wkb, _ := gen.geometry(allTypes[i%len(allTypes)], 1)
			s := stored(gen.srid(), wkb)
			for _, pos := range rnd.Perm(len(s))[:min(len(s), 100)] {
				b := append([]byte{}, s...)
				for _, v := range append(values, s[pos]^byte(1+rnd.IntN(255))) {
					b[pos] = v
					checkStoredToBare(t, b)
					m := bareOf(0, b[1:])
					if got, _, err := BareToStored(m); err == nil {
						back, err := StoredToBare(got)
						require.NoError(t, err, "%X", m)
						require.Equal(t, m, back)
					}
				}
			}
		}
	}
}

func TestNestedCountsAllocs(t *testing.T) {
	// Collections nested to the depth bound, each claiming as many members as the
	// remaining input could hold, then zeros. Each count alone fits, but the counts
	// claim the same bytes again at every level.
	const pad = 1 << 16
	b := make([]byte, 4, 4+MaxDepth*minGeometrySize+pad)
	for range MaxDepth {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7)
		b = binary.LittleEndian.AppendUint32(b, uint32((cap(b)-len(b)-4)/minGeometrySize))
	}
	b = append(b, make([]byte, pad)...)
	s := storedFromBare(b)
	_, _, err := BareToStored(b)
	require.ErrorIs(t, err, ErrInvalid)
	require.ErrorIs(t, validate(s[1:], dialectEWKB), ErrInvalid)
	checkStoredToBare(t, s)
	for _, f := range []func(){
		func() { _, _, _ = BareToStored(b) },
		func() { _ = validate(s[1:], dialectEWKB) },
	} {
		require.Equal(t, 0.0, testing.AllocsPerRun(1, f))
	}
}

func TestStoredToBareAllocs(t *testing.T) {
	gc := TypeGeometryCollection
	s := stored(4326, collectionOf(gc, dimsXY, point12, collectionOf(TypeMultiPolygon, dimsXY, triangle),
		collectionOf(gc, dimsXY, line(dimsXY, 0, 0, 1, 1, 2, 2))))
	// The one allocation is the result, whatever the root holds; a broken root header
	// allocates nothing.
	for _, b := range [][]byte{s, stored(0, le(TypePoint, dimsXYZ, coords(1, 2, 3))), s[:len(s)-1]} {
		require.Equal(t, 1.0, testing.AllocsPerRun(100, func() { _, _ = StoredToBare(b) }), "%X", b)
	}
	require.Equal(t, 0.0, testing.AllocsPerRun(100, func() { _, _ = StoredToBare(s[:9]) }))
}

// TestIngestAllocs checks that BareToStored's validate allocates nothing: a value that
// converts allocates its result, and a value that does not convert allocates nothing,
// whatever its counts claim.
func TestIngestAllocs(t *testing.T) {
	deep := bareChain(MaxDepth-1, multiPointWKB(10000))
	for _, c := range []struct {
		b      []byte
		allocs float64
	}{
		{deep, 1},
		{hostileCounts(), 0},
		{deep[:len(deep)-1], 0},
	} {
		allocs := testing.AllocsPerRun(3, func() { _, _, _ = BareToStored(c.b) })
		require.Equal(t, c.allocs, allocs, "%X", c.b[:min(len(c.b), 32)])
	}
}

func FuzzBareToStored(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(bareOf(0, collectionOf(TypeGeometryCollection, dimsXY, triangle)))
	f.Add(bareOf(0, collectionOf(TypeGeometryCollection, dimsXY, point12,
		collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(3, 4))))))
	f.Add(bareOf(4326, collectionOf(TypeMultiPoint, dimsXYM, le(TypePoint, dimsXYM, coords(1, 2, 3)), emptyPoint(dimsXYM))))
	f.Fuzz(func(t *testing.T, b []byte) {
		s, _, err := BareToStored(b)
		if err != nil {
			return
		}
		require.Len(t, s, len(b)+1)
		// The bare value is canonical: whatever is stored converts back to the same bytes.
		out, err := StoredToBare(s)
		require.NoError(t, err)
		require.Equal(t, b, out)
		checkStoredToBare(t, s)
	})
}

func FuzzStoredToBare(f *testing.F) {
	gc := TypeGeometryCollection
	for _, v := range [][]byte{
		point12,
		polygon(dimsXYZ, []float64{0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1}),
		collectionOf(gc, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12), line(dimsXY)),
		collectionOf(gc, dimsXY, collectionOf(TypeMultiPolygon, dimsXY, triangle), collectionOf(gc, dimsXY)),
	} {
		f.Add(stored(4326, v))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkStoredToBare(t, b)
		_, _, _ = Header(b)
	})
}
