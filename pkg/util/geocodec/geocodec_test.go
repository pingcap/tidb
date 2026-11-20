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

// checkStoredToMySQL requires StoredToMySQL to agree with validate on a stored value b. A
// value validate accepts it converts, refuses as one MySQL's format cannot hold when
// validate says so, or refuses as big-endian. A value validate refuses, which no writer
// stores, it may convert, rewriting only the header, but it never panics.
func checkStoredToMySQL(t testing.TB, b []byte) {
	got, err := StoredToMySQL(b)
	payload, payloadErr := payloadOf(b)
	if payloadErr != nil {
		require.Equal(t, payloadErr, err, "%X", b)
		return
	}
	notMySQL, validErr := validate(payload, dialectEWKB)
	if validErr != nil {
		if err != nil {
			require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep) ||
				errors.Is(err, ErrNotMySQLExpressible), "%X: %v", b, err)
			return
		}
		require.Len(t, got, len(payload), "%X", b)
		require.Equal(t, payload[9:], got[9:], "%X", b)
		return
	}
	if errors.Is(err, errBigEndianStored) {
		return
	}
	if notMySQL {
		require.ErrorIs(t, err, ErrNotMySQLExpressible, "%X", b)
		return
	}
	require.NoError(t, err, "%X", b)
	// MySQL's format reads back as the same stored value.
	again, srid, err := MySQLToStored(got)
	require.NoError(t, err, "%X", b)
	require.Equal(t, b, again, "%X", b)
	require.Equal(t, binary.LittleEndian.Uint32(payload[5:]), srid, "%X", b)
}

func TestMySQLFormat(t *testing.T) {
	// Bytes from MySQL 9.7.2: HEX(ST_GeomFromText('POINT(1 2)', 4326)), latitude 1 and
	// longitude 2, which MySQL holds longitude first, as the stored format does.
	mysql4326 := mustHex(t, "E6100000 01 01000000 0000000000000040 000000000000F03F")
	s, srid, err := MySQLToStored(mysql4326)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)

	// The conversion rewrites the header only: the WKB body is the same bytes.
	require.Equal(t, mustHex(t, "01 01 01000020 E6100000 0000000000000040 000000000000F03F"), s)
	back, err := StoredToMySQL(s)
	require.NoError(t, err)
	require.Equal(t, mysql4326, back)

	// Any SRID passes through, since no SRS axis order is applied.
	mysql12345 := mustHex(t, "39300000 01 01000000 000000000000F03F 0000000000000040")
	s, srid, err = MySQLToStored(mysql12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), srid)
	back, err = StoredToMySQL(s)
	require.NoError(t, err)
	require.Equal(t, mysql12345, back)

	// The smallest valid value: GEOMETRYCOLLECTION EMPTY, 13 bytes.
	smallest := mustHex(t, "00000000 01 07000000 00000000")
	s, _, err = MySQLToStored(smallest)
	require.NoError(t, err)
	back, err = StoredToMySQL(s)
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
		"EWKB flags":             "00000000 01 01000020 E6100000 000000000000F03F 0000000000000040",
		"empty linestring":       "00000000 01 02000000 00000000",
		"polygon without rings":  "00000000 01 03000000 00000000",
		"empty multipoint":       "00000000 01 04000000 00000000",
		"NaN point":              "00000000 01 01000000 000000000000F87F 000000000000F87F",
		"infinite point":         "00000000 01 01000000 000000000000F07F 0000000000000040",
		"NaN in a linestring":    "00000000 01 02000000 01000000 000000000000F87F 0000000000000040",
		"ring without points":    "00000000 01 03000000 01000000 00000000",
		"empty member":           "00000000 01 07000000 01000000 01 02000000 00000000",
		"member of the wrong type": "00000000 01 04000000 01000000 01 02000000 01000000" +
			" 000000000000F03F 0000000000000040",
		"too short for an SRID": "000000",
		"no geometry":           "E6100000",
	}
	for name, h := range rejects {
		_, _, err := MySQLToStored(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

func TestStoredLayout(t *testing.T) {
	// Every stored value carries the EWKB SRID flag and its SRID, 0 included.
	for srid, h := range map[uint32]string{
		0:    "01 01 01000020 00000000 000000000000F03F 0000000000000040",
		4326: "01 01 01000020 E6100000 000000000000F03F 0000000000000040",
	} {
		s, _, err := MySQLToStored(mysqlOf(srid, point12))
		require.NoError(t, err)
		require.Equal(t, mustHex(t, h), s)
	}
}

// TestStoredWithoutSRID checks that every reader of the stored format refuses a root
// without the SRID flag, in either byte order, as ErrInvalid.
func TestStoredWithoutSRID(t *testing.T) {
	for _, h := range []string{
		"01 01 01000000 000000000000F03F 0000000000000040",
		"01 00 00000001 3FF0000000000000 4000000000000000",
		"01 01 07000000 00000000",
		"01 00 00000007 00000000",
		// A root without the flag is refused before its other faults.
		"01 01 08000000",
		"01 01 07000000 FFFFFFFF",
	} {
		b := mustHex(t, h)
		_, err := StoredToMySQL(b)
		require.ErrorIs(t, err, errStoredWithoutSRID, h)
		require.ErrorIs(t, err, ErrInvalid, h)
		_, _, err = Header(b)
		require.ErrorIs(t, err, errStoredWithoutSRID, h)
		checkStoredToMySQL(t, b)
	}
	// With the flag, the same values are read.
	for _, h := range []string{
		"01 01 01000020 00000000 000000000000F03F 0000000000000040",
		"01 00 20000001 00000000 3FF0000000000000 4000000000000000",
	} {
		_, _, err := Header(mustHex(t, h))
		require.NoError(t, err, h)
	}
}

// storedRejects are stored values whose structure is broken, which every reader refuses.
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
		// A malformed value is ErrInvalid even when it also has no MySQL form.
		"Z point a byte short": "01 01 010000A0 00000000 000000000000F03F 0000000000000040 00000000000008",
		"empty point then junk": "01 01 07000020 00000000 02000000 01 01000000 000000000000F87F 000000000000F87F" +
			" 01 09000000",
	} {
		cases[name] = mustHex(t, h)
	}
	return cases
}

func TestStoredRejects(t *testing.T) {
	for name, b := range storedRejects(t) {
		_, err := StoredToMySQL(b)
		require.ErrorIs(t, err, ErrInvalid, name)
		require.NotErrorIs(t, err, errStoredWithoutSRID, name)
		if len(b) > 0 {
			_, err = validate(b[1:], dialectEWKB)
			require.ErrorIs(t, err, ErrInvalid, name)
		}
		checkStoredToMySQL(t, b)
	}
}

// storedTrusted are stored values that break a rule of the format but not its structure,
// with what StoredToMySQL makes of each: no writer stores one, so it does not check for it.
var storedTrusted = []struct {
	name, hex string
	toMySQL   error
}{
	{"SRID flag below the root", "01 01 04000020 00000000 01000000 01 01000020 E6100000 000000000000F03F 0000000000000040", ErrNotMySQLExpressible},
	{"member of the wrong type", "01 01 04000020 00000000 01000000 01 02000000 00000000", ErrNotMySQLExpressible},
	{"member dimension differs", "01 01 04000020 00000000 01000000 01 01000080 000000000000F03F 0000000000000040 0000000000000840", ErrNotMySQLExpressible},
	{"ring without points", "01 01 03000020 00000000 01000000 00000000", nil},
	// A NaN first coordinate marks POINT EMPTY.
	{"partly NaN point", "01 01 01000020 00000000 000000000000F87F 0000000000000040", ErrNotMySQLExpressible},
	{"infinite coordinate", "01 01 01000020 00000000 000000000000F07F 0000000000000040", nil},
	{"NaN in a linestring", "01 01 02000020 00000000 01000000 000000000000F87F 000000000000F87F", nil},
}

// TestStoredTrusted records what StoredToMySQL does with a value that breaks a rule of the
// format but not its structure: validate refuses each, and StoredToMySQL copies the bytes
// unless MySQL's format cannot hold them.
func TestStoredTrusted(t *testing.T) {
	for _, c := range storedTrusted {
		b := mustHex(t, c.hex)
		_, err := validate(b[1:], dialectEWKB)
		require.ErrorIs(t, err, ErrInvalid, c.name)
		_, err = StoredToMySQL(b)
		require.ErrorIs(t, err, c.toMySQL, c.name)
		checkStoredToMySQL(t, b)
	}
}

func TestVersion(t *testing.T) {
	good := stored(4326, point12)
	for _, version := range []byte{0, 2, 0x7f, 0xff} {
		b := append([]byte{version}, good[1:]...)
		_, err := StoredToMySQL(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
		_, _, err = Header(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
	}
	// The version is checked before anything else, so it is reported for a payload that
	// is missing or malformed too.
	for _, b := range [][]byte{{0}, {2}, {2, 0xff}} {
		_, err := StoredToMySQL(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
		_, _, err = Header(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion)
	}
}

func TestStoredBigEndian(t *testing.T) {
	// POINT(1 2) with SRID 4326, big-endian EWKB.
	s := mustHex(t, "01 00 20000001 000010E6 3FF0000000000000 4000000000000000")
	srid, typ, err := Header(s)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, TypePoint, typ)
	_, err = validate(s[1:], dialectEWKB)
	require.NoError(t, err)

	// StoredToMySQL copies the body unchanged, so it takes only little-endian values,
	// which is all TiDB writes. A big-endian member is rejected too.
	for _, b := range [][]byte{
		s,
		mustHex(t, "01 01 04000020 E6100000 01000000 00 00000001 3FF0000000000000 4000000000000000"),
		mustHex(t, "01 00 A0000001 000010E6 3FF0000000000000 4000000000000000 4008000000000000"),
	} {
		_, err = StoredToMySQL(b)
		require.ErrorIs(t, err, errBigEndianStored)
		checkStoredToMySQL(t, b)
	}
}

func TestNotMySQLExpressible(t *testing.T) {
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
	}
	for _, v := range values {
		s := stored(4326, v)
		_, err := StoredToMySQL(s)
		require.ErrorIs(t, err, ErrNotMySQLExpressible, "%X", s)
		checkStoredToMySQL(t, s)
	}

	// Empty collections have a MySQL form.
	for _, v := range [][]byte{
		collectionOf(gc, dimsXY),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY), point12),
		collectionOf(gc, dimsXY, collectionOf(gc, dimsXY, collectionOf(gc, dimsXY))),
	} {
		s := stored(4326, v)
		mysql, err := StoredToMySQL(s)
		require.NoError(t, err, "%X", s)
		require.Equal(t, mysqlOf(4326, v), mysql)
		checkStoredToMySQL(t, s)
	}

	// At the depth bound, as the deepest member.
	for _, s := range [][]byte{
		storedChain(MaxDepth, 0, emptyPoint(dimsXY)),
		storedChain(MaxDepth, ewkbZ, le(TypePoint, dimsXYZ, coords(1, 2, 3))),
	} {
		_, err := StoredToMySQL(s)
		require.ErrorIs(t, err, ErrNotMySQLExpressible)
		checkStoredToMySQL(t, s)
	}
}

func TestDepthBound(t *testing.T) {
	// One bound applies to both directions, so a value that is stored converts back.
	garbage := bytes.Repeat([]byte{0xff}, minGeometrySize)
	for _, c := range []struct {
		mysql  []byte
		stored []byte
	}{
		{append(make([]byte, 4), nestedCollections(MaxDepth)...), storedChain(MaxDepth, 0, nestedCollections(1))},
		{mysqlChain(MaxDepth, point12), storedChain(MaxDepth, 0, point12)},
	} {
		s, _, err := MySQLToStored(c.mysql)
		require.NoError(t, err)
		require.Equal(t, c.stored, s)
		back, err := StoredToMySQL(s)
		require.NoError(t, err)
		require.Equal(t, c.mysql, back)
		checkStoredToMySQL(t, s)
	}

	for _, c := range []struct {
		mysql  []byte
		stored []byte
	}{
		{append(make([]byte, 4), nestedCollections(MaxDepth+1)...), storedChain(MaxDepth+1, 0, nestedCollections(1))},
		{mysqlChain(MaxDepth+1, point12), storedChain(MaxDepth+1, 0, point12)},
		// Depth is checked before anything else in a geometry. Its parent's count still
		// needs room for the smallest geometry.
		{mysqlChain(MaxDepth+1, garbage), storedChain(MaxDepth+1, 0, garbage)},
		{nil, storedChain(MaxDepth+1, ewkbZ, mustHex(t, "01 01000080 000000000000F87F 000000000000F87F 000000000000F87F"))},
	} {
		if c.mysql != nil {
			_, _, err := MySQLToStored(c.mysql)
			require.ErrorIs(t, err, ErrTooDeep)
		}
		_, err := StoredToMySQL(c.stored)
		require.ErrorIs(t, err, ErrTooDeep)
		_, err = validate(c.stored[1:], dialectEWKB)
		require.ErrorIs(t, err, ErrTooDeep)
		checkStoredToMySQL(t, c.stored)
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
	_, err := StoredToMySQL(badFirst)
	require.ErrorIs(t, err, ErrInvalid)
	checkStoredToMySQL(t, badFirst)
	_, err = StoredToMySQL(deepFirst)
	require.ErrorIs(t, err, ErrTooDeep)
	checkStoredToMySQL(t, deepFirst)
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
		// Values with no MySQL form are ErrInvalid too when malformed.
		le(TypePoint, dimsXYZM, coords(1, 2, 3, 4)),
		collectionOf(gc, dimsXY, collectionOf(TypeMultiPoint, dimsXY, point12, emptyPoint(dimsXY))),
	}
	for _, v := range values {
		s := stored(4326, v)
		for n := range s {
			_, err := StoredToMySQL(s[:n])
			require.ErrorIs(t, err, ErrInvalid, "%X cut to %d", s, n)
			checkStoredToMySQL(t, s[:n])
		}
		for _, extra := range [][]byte{{0}, {1}, s[1:]} {
			long := append(append([]byte{}, s...), extra...)
			_, err := StoredToMySQL(long)
			require.ErrorIs(t, err, ErrInvalid, "%X", long)
			checkStoredToMySQL(t, long)
		}

		mysql, err := StoredToMySQL(s)
		if err != nil {
			continue
		}
		for n := range mysql {
			_, _, err := MySQLToStored(mysql[:n])
			require.ErrorIs(t, err, ErrInvalid, "%X cut to %d", mysql, n)
		}
		for _, extra := range [][]byte{{0}, {1}, mysql[4:]} {
			_, _, err := MySQLToStored(append(append([]byte{}, mysql...), extra...))
			require.ErrorIs(t, err, ErrInvalid, "%X", mysql)
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
		_, _, err := MySQLToStored([]byte(s))
		require.Error(t, err)
		_, err = StoredToMySQL([]byte(s))
		require.Error(t, err)
		_, _, _ = Header([]byte(s))
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		b := randomBytes(rnd, rnd.IntN(80))
		if _, _, err := MySQLToStored(b); err != nil {
			require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep), "%X: %v", b, err)
		}
		s := append([]byte{Version1}, b...)
		checkStoredToMySQL(t, s)
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
	expressible, notExpressible, bigEndian := 0, 0, 0
	for _, d := range []dims{dimsXY, dimsXYZ, dimsXYM, dimsXYZM} {
		for _, empty := range []float64{0, 0.15} {
			for _, big := range []float64{0, 0.3} {
				gen := wkbGen{rnd: rnd, d: d, empty: empty, big: big}
				for i := range 500 {
					typ := allTypes[i%len(allTypes)]
					wkb, notMySQL := gen.geometry(typ, 1)
					srid := gen.srid()
					s := stored(srid, wkb)

					gotNotMySQL, err := validate(s[1:], dialectEWKB)
					require.NoError(t, err, "%X", s)
					require.Equal(t, notMySQL, gotNotMySQL, "%X", s)
					gotSRID, gotType, err := Header(s)
					require.NoError(t, err, "%X", s)
					require.Equal(t, srid, gotSRID)
					require.Equal(t, typ, gotType)
					checkStoredToMySQL(t, s)

					mysql, err := StoredToMySQL(s)
					if errors.Is(err, errBigEndianStored) {
						require.Positive(t, big)
						bigEndian++
						continue
					}
					if notMySQL {
						require.ErrorIs(t, err, ErrNotMySQLExpressible, "%X", s)
						notExpressible++
						continue
					}
					require.NoError(t, err, "%X", s)
					require.Equal(t, mysqlOf(srid, wkb), mysql)
					again, gotSRID, err := MySQLToStored(mysql)
					require.NoError(t, err, "%X", mysql)
					require.Equal(t, s, again)
					require.Equal(t, srid, gotSRID)
					expressible++
				}
			}
		}
	}
	require.Greater(t, expressible, 1000)
	require.Greater(t, notExpressible, 3000)
	require.Greater(t, bigEndian, 1000)
}

// TestGeneratedMutations checks StoredToMySQL and MySQLToStored on generated values with one
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
					checkStoredToMySQL(t, b)
					m := mysqlOf(0, b[1:])
					if got, _, err := MySQLToStored(m); err == nil {
						back, err := StoredToMySQL(got)
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
	s := storedFromMySQL(b)
	_, _, err := MySQLToStored(b)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = StoredToMySQL(s)
	require.ErrorIs(t, err, ErrInvalid)
	require.NotErrorIs(t, err, errStoredWithoutSRID)
	checkStoredToMySQL(t, s)
	for _, f := range []func(){
		func() { _, _, _ = MySQLToStored(b) },
		func() { _, _ = StoredToMySQL(s) },
	} {
		require.Equal(t, 0.0, testing.AllocsPerRun(1, f))
	}
}

func TestStoredToMySQLAllocs(t *testing.T) {
	gc := TypeGeometryCollection
	s := stored(4326, collectionOf(gc, dimsXY, point12, collectionOf(TypeMultiPolygon, dimsXY, triangle),
		collectionOf(gc, dimsXY, line(dimsXY, 0, 0, 1, 1, 2, 2))))
	// skim allocates nothing: the one allocation is the result.
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = StoredToMySQL(s)
	})
	require.Equal(t, 1.0, allocs)
	for _, b := range [][]byte{stored(0, le(TypePoint, dimsXYZ, coords(1, 2, 3))), s[:len(s)-1]} {
		allocs = testing.AllocsPerRun(100, func() {
			_, _ = StoredToMySQL(b)
		})
		require.Equal(t, 0.0, allocs)
	}
}

// TestIngestAllocs checks that MySQLToStored's validate allocates nothing: a value that
// converts allocates its result, and a value that does not convert allocates nothing,
// whatever its counts claim.
func TestIngestAllocs(t *testing.T) {
	deep := mysqlChain(MaxDepth-1, multiPointWKB(10000))
	for _, c := range []struct {
		b      []byte
		allocs float64
	}{
		{deep, 1},
		{hostileCounts(), 0},
		{deep[:len(deep)-1], 0},
	} {
		allocs := testing.AllocsPerRun(3, func() { _, _, _ = MySQLToStored(c.b) })
		require.Equal(t, c.allocs, allocs, "%X", c.b[:min(len(c.b), 32)])
	}
}

func FuzzMySQLToStored(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(mysqlOf(0, collectionOf(TypeGeometryCollection, dimsXY, triangle)))
	f.Add(mysqlOf(0, collectionOf(TypeGeometryCollection, dimsXY, point12,
		collectionOf(TypeMultiPoint, dimsXY, le(TypePoint, dimsXY, coords(3, 4))))))
	f.Fuzz(func(t *testing.T, b []byte) {
		s, _, err := MySQLToStored(b)
		if err != nil {
			return
		}
		require.Len(t, s, len(b)+1)
		// MySQL's format is canonical: whatever is stored converts back to the same bytes.
		out, err := StoredToMySQL(s)
		require.NoError(t, err)
		require.Equal(t, b, out)
		checkStoredToMySQL(t, s)
	})
}

func FuzzStoredToMySQL(f *testing.F) {
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
		checkStoredToMySQL(t, b)
		_, _, _ = Header(b)
	})
}

// FuzzSkim checks that skim, which trusts its input, never panics or allocates on
// arbitrary bytes, and agrees with validate on what validate accepts.
func FuzzSkim(f *testing.F) {
	f.Add(stored(4326, point12)[1:])
	f.Add(stored(0, collectionOf(TypeMultiPoint, dimsXYM, emptyPoint(dimsXYM)))[1:])
	f.Add(nestedCollections(MaxDepth))
	f.Fuzz(func(t *testing.T, b []byte) {
		require.Zero(t, testing.AllocsPerRun(1, func() { _, _ = skim(b) }), "%X", b)
		notMySQL, err := validate(b, dialectEWKB)
		if err != nil {
			return
		}
		skimNotMySQL, err := skim(b)
		if errors.Is(err, errBigEndianStored) {
			return
		}
		require.NoError(t, err, "%X", b)
		require.Equal(t, notMySQL, skimNotMySQL, "%X", b)
	})
}
