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
	"encoding/hex"
	"errors"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
)

func mustWKT(t testing.TB, wkt string) geom.Geometry {
	g, err := geom.UnmarshalWKT(wkt)
	require.NoError(t, err, wkt)
	return g
}

func mustHex(t testing.TB, s string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}

func mustEncode(t testing.TB, wkt string, srid uint32) []byte {
	stored, err := encode(mustWKT(t, wkt), srid)
	require.NoError(t, err, wkt)
	return stored
}

// decodeStored parses a stored value into its SRID and geometry.
func decodeStored(stored []byte) (uint32, geom.Geometry, error) {
	payload, err := payloadOf(stored)
	if err != nil {
		return 0, geom.Geometry{}, err
	}
	return build(payload, dialectEWKB)
}

// referenceStoredToMySQL converts a stored value by decoding it into a geometry and
// writing that with mysqlWriter. StoredToMySQL must give the same bytes or the same
// error for every stored value that is little-endian throughout.
func referenceStoredToMySQL(stored []byte) ([]byte, error) {
	srid, g, err := decodeStored(stored)
	if err != nil {
		return nil, err
	}
	m := mysqlWriter{enc: writer{buf: make([]byte, 4, 32)}}
	binary.LittleEndian.PutUint32(m.enc.buf, srid)
	if err := m.geometry(g, 1); err != nil {
		return nil, err
	}
	return m.enc.buf, nil
}

// mysqlWriter writes the WKB inside MySQL's binary format from a geometry: little-endian,
// 2D, and no empty geometry except a GEOMETRYCOLLECTION.
type mysqlWriter struct {
	// enc writes counts and coordinates, which are encoded alike in both formats.
	enc writer
}

func (m *mysqlWriter) header(typ geom.GeometryType, ctype geom.CoordinatesType) error {
	code, ok := typeCodeOf(typ)
	if !ok {
		return ErrInvalid
	}
	if ctype != geom.DimXY {
		return ErrNotMySQLExpressible
	}
	m.enc.buf = append(m.enc.buf, byteOrderLittle)
	m.enc.uint32(code)
	return nil
}

func (m *mysqlWriter) geometry(g geom.Geometry, depth int) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	ctype := g.CoordinatesType()
	typ := g.Type()
	// WKB cannot express an empty point, and MySQL's format admits no empty geometry
	// other than a GEOMETRYCOLLECTION.
	if g.IsEmpty() && typ != geom.TypeGeometryCollection {
		return ErrNotMySQLExpressible
	}
	if err := m.header(typ, ctype); err != nil {
		return err
	}
	var n int
	var member func(i int) geom.Geometry
	switch typ {
	case geom.TypePoint:
		c, _ := g.MustAsPoint().Coordinates()
		m.enc.coordinates(c, ctype)
		return nil
	case geom.TypeLineString:
		m.enc.sequence(g.MustAsLineString().Coordinates(), ctype)
		return nil
	case geom.TypePolygon:
		rings := g.MustAsPolygon().Coordinates()
		m.enc.uint32(uint32(len(rings)))
		for _, ring := range rings {
			m.enc.sequence(ring, ctype)
		}
		return nil
	case geom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		n, member = mp.NumPoints(), func(i int) geom.Geometry { return mp.PointN(i).AsGeometry() }
	case geom.TypeMultiLineString:
		mls := g.MustAsMultiLineString()
		n, member = mls.NumLineStrings(), func(i int) geom.Geometry { return mls.LineStringN(i).AsGeometry() }
	case geom.TypeMultiPolygon:
		mp := g.MustAsMultiPolygon()
		n, member = mp.NumPolygons(), func(i int) geom.Geometry { return mp.PolygonN(i).AsGeometry() }
	default:
		gc := g.MustAsGeometryCollection()
		n, member = gc.NumGeometries(), gc.GeometryN
	}
	m.enc.uint32(uint32(n))
	for i := range n {
		if err := m.geometry(member(i), depth+1); err != nil {
			return err
		}
	}
	return nil
}

// checkAgainstReference requires StoredToMySQL to agree with referenceStoredToMySQL on
// b. The one difference allowed is big-endian geometry, which the reference reads and
// StoredToMySQL rejects with ErrInvalid; b is then not in the canonical form.
func checkAgainstReference(t testing.TB, b []byte) {
	got, err := StoredToMySQL(b)
	want, wantErr := referenceStoredToMySQL(b)
	if errors.Is(err, ErrInvalid) && !errors.Is(wantErr, ErrInvalid) {
		if errors.Is(wantErr, ErrTooDeep) {
			// A big-endian geometry ahead of the depth overflow.
			require.GreaterOrEqual(t, bytes.IndexByte(b, byteOrderBig), 0, "%X", b)
			return
		}
		srid, g, decodeErr := decodeStored(b)
		require.NoError(t, decodeErr, "%X", b)
		canonical, encodeErr := encode(g, srid)
		require.NoError(t, encodeErr, "%X", b)
		require.NotEqual(t, canonical, b, "%X", b)
		return
	}
	require.Equal(t, wantErr, err, "%X", b)
	require.Equal(t, want, got, "%X", b)
}

// nestedCollections returns WKB (little-endian, no SRID) for depth GEOMETRYCOLLECTIONs,
// each holding the next, the innermost empty.
func nestedCollections(depth int) []byte {
	b := make([]byte, 0, depth*minGeometrySize)
	for i := range depth {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7)
		n := uint32(1)
		if i == depth-1 {
			n = 0
		}
		b = binary.LittleEndian.AppendUint32(b, n)
	}
	return b
}

// storedChain returns a stored value at SRID 0 of depth-1 GEOMETRYCOLLECTIONs, each
// holding the next and the innermost holding leaf, a little-endian geometry without an
// SRID, which so sits at depth. flags are set on every collection's type word.
func storedChain(depth int, flags uint32, leaf []byte) []byte {
	b := []byte{Version1, byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 7|flags|ewkbSRID)
	b = binary.LittleEndian.AppendUint32(b, 0)
	b = binary.LittleEndian.AppendUint32(b, 1)
	for range depth - 2 {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7|flags)
		b = binary.LittleEndian.AppendUint32(b, 1)
	}
	return append(b, leaf...)
}

// mysqlChain is storedChain in MySQL's binary format, without flags.
func mysqlChain(depth int, leaf []byte) []byte {
	b := make([]byte, 4)
	for range depth - 1 {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7)
		b = binary.LittleEndian.AppendUint32(b, 1)
	}
	return append(b, leaf...)
}

func TestStoredRoundTrip(t *testing.T) {
	wkts := []string{
		"POINT(1 2)",
		"POINT Z(1 2 3)",
		"POINT M(1 2 4)",
		"POINT ZM(1 2 3 4)",
		"POINT EMPTY",
		"POINT Z EMPTY",
		"LINESTRING(0 0,1 1,2 0)",
		"LINESTRING ZM(0 0 1 2,1 1 3 4)",
		"LINESTRING EMPTY",
		"POLYGON((0 0,0 1,1 1,1 0,0 0),(0.2 0.2,0.2 0.4,0.4 0.4,0.2 0.2))",
		"POLYGON Z((0 0 1,0 1 1,1 1 1,0 0 1))",
		"POLYGON EMPTY",
		"MULTIPOINT((1 2),(3 4))",
		"MULTIPOINT M((1 2 3),EMPTY)",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,0 1,1 1,0 0)),((5 5,5 6,6 6,5 5)))",
		"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1),GEOMETRYCOLLECTION(POINT(3 4)))",
		"GEOMETRYCOLLECTION Z(POINT Z(1 2 3))",
		"GEOMETRYCOLLECTION EMPTY",
	}
	for _, wkt := range wkts {
		g := mustWKT(t, wkt)
		for _, srid := range []uint32{0, 4326, 3857} {
			stored, err := encode(g, srid)
			require.NoError(t, err, wkt)
			require.Equal(t, Version1, stored[0])
			gotSRID, got, err := decodeStored(stored)
			require.NoError(t, err, wkt)
			require.Equal(t, srid, gotSRID, wkt)
			require.True(t, geom.ExactEquals(g, got), "%s: got %s", wkt, got.AsText())
			require.Equal(t, g.CoordinatesType(), got.CoordinatesType(), wkt)

			hSRID, hType, err := Header(stored)
			require.NoError(t, err)
			require.Equal(t, srid, hSRID)
			require.Equal(t, g.Type(), hType)
		}
	}
}

func TestStoredLayout(t *testing.T) {
	// Every stored value carries the EWKB SRID flag and its SRID, 0 included.
	require.Equal(t, mustHex(t, "01 01 01000020 00000000 000000000000F03F 0000000000000040"),
		mustEncode(t, "POINT(1 2)", 0))
	require.Equal(t, mustHex(t, "01 01 01000020 E6100000 000000000000F03F 0000000000000040"),
		mustEncode(t, "POINT(1 2)", 4326))

	// Z and M are type-word flags on every level, not ISO type codes; the SRID flag is
	// on the root only.
	require.Equal(t,
		mustHex(t, "01 01 040000A0 00000000 01000000 01 01000080 000000000000F03F 0000000000000040 0000000000000840"),
		mustEncode(t, "MULTIPOINT Z((1 2 3))", 0))

	// POINT EMPTY is NaN coordinates, the quiet NaN PostGIS and GEOS write.
	require.Equal(t, mustHex(t, "01 01 01000020 00000000 000000000000F87F 000000000000F87F"),
		mustEncode(t, "POINT EMPTY", 0))

	// A value without the SRID flag, which this package never writes, reads as SRID 0.
	flagless := mustHex(t, "01 01 01000000 000000000000F03F 0000000000000040")
	srid, g, err := decodeStored(flagless)
	require.NoError(t, err)
	require.Equal(t, uint32(0), srid)
	require.Equal(t, "POINT(1 2)", g.AsText())
	mysql, err := StoredToMySQL(flagless)
	require.NoError(t, err)
	require.Equal(t, mustHex(t, "00000000 01 01000000 000000000000F03F 0000000000000040"), mysql)
}

func TestStoredRejects(t *testing.T) {
	good := mustEncode(t, "POINT(1 2)", 4326)
	cases := map[string][]byte{
		"empty":        nil,
		"version only": {Version1},
		"a byte short": good[:len(good)-1],
		"a byte long":  append(append([]byte{}, good...), 0),
	}
	hexCases := map[string]string{
		"bad byte order":           "01 02 01000000 000000000000F03F 0000000000000040",
		"unknown type code":        "01 01 08000000",
		"ISO type code in EWKB":    "01 01 E9030000 000000000000F03F 0000000000000040 0000000000000840",
		"SRID flag below the root": "01 01 04000000 01000000 01 01000020 E6100000 000000000000F03F 0000000000000040",
		"member of the wrong type": "01 01 04000000 01000000 01 02000000 00000000",
		"member dimension differs": "01 01 04000000 01000000 01 01000080 000000000000F03F 0000000000000040 0000000000000840",
		"hostile count":            "01 01 07000000 FFFFFFFF",
		"hostile point count":      "01 01 02000000 FFFFFF0F",
		"ring without points":      "01 01 03000000 01000000 00000000",
		"partly NaN point":         "01 01 01000000 000000000000F87F 0000000000000040",
		"infinite coordinate":      "01 01 01000000 000000000000F07F 0000000000000040",
		"NaN in a linestring":      "01 01 02000000 01000000 000000000000F87F 000000000000F87F",
		// A malformed value is ErrInvalid even when it also has no MySQL form.
		"Z point a byte short": "01 01 01000080 000000000000F03F 0000000000000040 00000000000008",
		"empty point then junk": "01 01 07000000 02000000 01 01000000 000000000000F87F 000000000000F87F" +
			" 01 09000000",
	}
	for name, h := range hexCases {
		cases[name] = mustHex(t, h)
	}
	for name, b := range cases {
		_, _, err := decodeStored(b)
		require.ErrorIs(t, err, ErrInvalid, name)
		_, err = StoredToMySQL(b)
		require.ErrorIs(t, err, ErrInvalid, name)
		checkAgainstReference(t, b)
	}
}

func TestVersion(t *testing.T) {
	good := mustEncode(t, "POINT(1 2)", 4326)
	for _, version := range []byte{0, 2, 0x7f, 0xff} {
		b := append([]byte{version}, good[1:]...)
		_, err := StoredToMySQL(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
		_, _, err = Header(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
		_, _, err = decodeStored(b)
		require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", version)
		checkAgainstReference(t, b)
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
	// POINT(1 2) with SRID 4326, big-endian EWKB, which the reader takes.
	stored := mustHex(t, "01 00 20000001 000010E6 3FF0000000000000 4000000000000000")
	srid, g, err := decodeStored(stored)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, "POINT(1 2)", g.AsText())
	srid, typ, err := Header(stored)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, geom.TypePoint, typ)

	// StoredToMySQL copies the body unchanged, so it takes only little-endian values,
	// which is all this package writes. A big-endian member is rejected too.
	for _, b := range [][]byte{
		stored,
		mustHex(t, "01 01 04000020 E6100000 01000000 00 00000001 3FF0000000000000 4000000000000000"),
		mustHex(t, "01 00 A0000001 000010E6 3FF0000000000000 4000000000000000 4008000000000000"),
	} {
		_, err = StoredToMySQL(b)
		require.ErrorIs(t, err, ErrInvalid)
		checkAgainstReference(t, b)
	}
}

func TestMySQLFormat(t *testing.T) {
	// Bytes from MySQL 9.7.2: HEX(ST_GeomFromText('POINT(1 2)', 4326)), latitude 1 and
	// longitude 2, which MySQL holds longitude first, as the stored format does.
	mysql4326 := mustHex(t, "E6100000 01 01000000 0000000000000040 000000000000F03F")
	stored, srid, sum, err := MySQLToStored(mysql4326)
	require.NoError(t, err)
	require.Equal(t, uint32(4326), srid)
	require.Equal(t, Summary{Type: geom.TypePoint}, sum)

	// The conversion rewrites the header only: the WKB body is the same bytes.
	require.Equal(t, mustHex(t, "01 01 01000020 E6100000 0000000000000040 000000000000F03F"), stored)
	back, err := StoredToMySQL(stored)
	require.NoError(t, err)
	require.Equal(t, mysql4326, back)

	// Any SRID passes through, since no SRS axis order is applied.
	mysql12345 := mustHex(t, "39300000 01 01000000 000000000000F03F 0000000000000040")
	stored, srid, _, err = MySQLToStored(mysql12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), srid)
	back, err = StoredToMySQL(stored)
	require.NoError(t, err)
	require.Equal(t, mysql12345, back)

	// The smallest valid value: GEOMETRYCOLLECTION EMPTY, 13 bytes.
	smallest := mustHex(t, "00000000 01 07000000 00000000")
	stored, _, sum, err = MySQLToStored(smallest)
	require.NoError(t, err)
	require.Equal(t, Summary{Type: geom.TypeGeometryCollection}, sum)
	back, err = StoredToMySQL(stored)
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
		"empty member":           "00000000 01 07000000 01000000 01 02000000 00000000",
		"too short for an SRID":  "000000",
		"no geometry":            "E6100000",
	}
	for name, h := range rejects {
		_, _, _, err := MySQLToStored(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

func TestNotMySQLExpressible(t *testing.T) {
	wkts := []string{
		// Z and M, at the root and below it.
		"POINT Z(1 2 3)",
		"POINT M(1 2 4)",
		"POINT ZM(1 2 3 4)",
		"LINESTRING Z(0 0 1,1 1 1)",
		"POLYGON M((0 0 1,0 1 1,1 1 1,0 0 1))",
		"MULTIPOINT ZM((1 2 3 4))",
		"MULTILINESTRING M((0 0 1,1 1 1))",
		"MULTIPOLYGON Z(((0 0 1,0 1 1,1 1 1,0 0 1)))",
		"GEOMETRYCOLLECTION Z EMPTY",
		"GEOMETRYCOLLECTION M(GEOMETRYCOLLECTION M(POINT M(1 2 3)))",
		"GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM EMPTY))",
		// An empty geometry other than a GEOMETRYCOLLECTION, at any depth.
		"POINT EMPTY",
		"LINESTRING EMPTY",
		"POLYGON EMPTY",
		"MULTIPOINT EMPTY",
		"MULTILINESTRING EMPTY",
		"MULTIPOLYGON EMPTY",
		"MULTIPOINT((1 2),EMPTY)",
		"MULTILINESTRING((0 0,1 1),EMPTY)",
		"MULTIPOLYGON(((0 0,0 1,1 1,0 0)),EMPTY)",
		"GEOMETRYCOLLECTION(POINT EMPTY)",
		"GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION(LINESTRING EMPTY))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOLYGON EMPTY)))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTILINESTRING((0 0,1 1),EMPTY)))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY,MULTIPOINT(EMPTY))",
	}
	for _, wkt := range wkts {
		stored := mustEncode(t, wkt, 4326)
		_, err := StoredToMySQL(stored)
		require.ErrorIs(t, err, ErrNotMySQLExpressible, wkt)
		checkAgainstReference(t, stored)
	}

	// Empty collections have a MySQL form.
	for _, wkt := range []string{
		"GEOMETRYCOLLECTION EMPTY",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY,POINT(1 2))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY))",
	} {
		stored := mustEncode(t, wkt, 4326)
		mysql, err := StoredToMySQL(stored)
		require.NoError(t, err, wkt)
		checkAgainstReference(t, stored)
		again, _, _, err := MySQLToStored(mysql)
		require.NoError(t, err, wkt)
		require.Equal(t, stored, again, wkt)
	}

	// At the depth bound, as the deepest member.
	emptyPoint := mustHex(t, "01 01000000 000000000000F87F 000000000000F87F")
	zPoint := mustHex(t, "01 01000080 000000000000F03F 0000000000000040 0000000000000840")
	for _, stored := range [][]byte{
		storedChain(MaxDepth, 0, emptyPoint),
		storedChain(MaxDepth, ewkbZ, zPoint),
	} {
		_, err := StoredToMySQL(stored)
		require.ErrorIs(t, err, ErrNotMySQLExpressible)
		checkAgainstReference(t, stored)
	}
}

func TestDepthBound(t *testing.T) {
	// One bound applies to both directions, so a value that is stored reads back.
	point := mustHex(t, "01 01000000 000000000000F03F 0000000000000040")
	garbage := bytes.Repeat([]byte{0xff}, minGeometrySize)
	for _, c := range []struct {
		mysql  []byte
		stored []byte
	}{
		{
			append(make([]byte, 4), nestedCollections(MaxDepth)...),
			storedChain(MaxDepth, 0, nestedCollections(1)),
		},
		{mysqlChain(MaxDepth, point), storedChain(MaxDepth, 0, point)},
	} {
		stored, _, _, err := MySQLToStored(c.mysql)
		require.NoError(t, err)
		require.Equal(t, c.stored, stored)
		back, err := StoredToMySQL(stored)
		require.NoError(t, err)
		require.Equal(t, c.mysql, back)
		_, g, err := decodeStored(stored)
		require.NoError(t, err)
		again, err := encode(g, 0)
		require.NoError(t, err)
		require.Equal(t, stored, again)
		checkAgainstReference(t, stored)
	}

	for _, c := range []struct {
		mysql  []byte
		stored []byte
	}{
		{
			append(make([]byte, 4), nestedCollections(MaxDepth+1)...),
			storedChain(MaxDepth+1, 0, nestedCollections(1)),
		},
		{mysqlChain(MaxDepth+1, point), storedChain(MaxDepth+1, 0, point)},
		// Depth is checked before anything else in a geometry. Its parent's count still
		// needs room for the smallest geometry.
		{mysqlChain(MaxDepth+1, garbage), storedChain(MaxDepth+1, 0, garbage)},
		{nil, storedChain(MaxDepth+1, ewkbZ, mustHex(t, "01 01000080 000000000000F87F 000000000000F87F 000000000000F87F"))},
	} {
		if c.mysql != nil {
			_, _, _, err := MySQLToStored(c.mysql)
			require.ErrorIs(t, err, ErrTooDeep)
		}
		_, err := StoredToMySQL(c.stored)
		require.ErrorIs(t, err, ErrTooDeep)
		checkAgainstReference(t, c.stored)
		// Header reads only the root, so depth does not matter to it.
		srid, typ, err := Header(c.stored)
		require.NoError(t, err)
		require.Equal(t, uint32(0), srid)
		require.Equal(t, geom.TypeGeometryCollection, typ)
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
	checkAgainstReference(t, badFirst)
	_, err = StoredToMySQL(deepFirst)
	require.ErrorIs(t, err, ErrTooDeep)
	checkAgainstReference(t, deepFirst)

	// A geometry built deeper than the bound is not encoded.
	tooDeep := geom.NewGeometryCollection(nil).AsGeometry()
	for range MaxDepth {
		tooDeep = geom.NewGeometryCollection([]geom.Geometry{tooDeep}).AsGeometry()
	}
	_, err = encode(tooDeep, 0)
	require.ErrorIs(t, err, ErrTooDeep)
}

func TestTruncatedAndOverLong(t *testing.T) {
	wkts := []string{
		"POINT(1 2)",
		"LINESTRING(0 0,1 1,2 0)",
		"POLYGON((0 0,0 1,1 1,1 0,0 0),(0.2 0.2,0.2 0.4,0.4 0.4,0.2 0.2))",
		"MULTIPOINT((1 2),(3 4))",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,0 1,1 1,0 0)),((5 5,5 6,6 6,5 5)))",
		"GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION(LINESTRING(0 0,1 1)),GEOMETRYCOLLECTION EMPTY)",
		"GEOMETRYCOLLECTION EMPTY",
		// Values with no MySQL form are ErrInvalid too when malformed.
		"POINT ZM(1 2 3 4)",
		"GEOMETRYCOLLECTION(MULTIPOINT((1 2),EMPTY))",
	}
	for _, wkt := range wkts {
		stored := mustEncode(t, wkt, 4326)
		for n := range stored {
			_, err := StoredToMySQL(stored[:n])
			require.ErrorIs(t, err, ErrInvalid, "%s cut to %d", wkt, n)
			checkAgainstReference(t, stored[:n])
		}
		for _, extra := range [][]byte{{0}, {1}, stored[1:]} {
			long := append(append([]byte{}, stored...), extra...)
			_, err := StoredToMySQL(long)
			require.ErrorIs(t, err, ErrInvalid, wkt)
			checkAgainstReference(t, long)
		}

		mysql, err := referenceStoredToMySQL(stored)
		if err != nil {
			continue
		}
		for n := range mysql {
			_, _, _, err := MySQLToStored(mysql[:n])
			require.ErrorIs(t, err, ErrInvalid, "%s cut to %d", wkt, n)
		}
		for _, extra := range [][]byte{{0}, {1}, mysql[4:]} {
			_, _, _, err := MySQLToStored(append(append([]byte{}, mysql...), extra...))
			require.ErrorIs(t, err, ErrInvalid, wkt)
		}
	}
}

func TestGarbage(t *testing.T) {
	for _, s := range []string{"", "x", "POINT(1 2)", "\x00\x00\x00\x00", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"} {
		_, _, _, err := MySQLToStored([]byte(s))
		require.Error(t, err)
		_, err = StoredToMySQL([]byte(s))
		require.Error(t, err)
		_, _, _ = Header([]byte(s))
	}

	// Random bytes drawn mostly from values that are meaningful in a header, so that
	// parsing gets past the first few bytes.
	alphabet := []byte{0, 1, 2, 3, 4, 5, 6, 7, 0x20, 0x40, 0x80, 0xf0, 0x3f, 0x7f, 0xf8, 0xff}
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		b := make([]byte, rnd.IntN(80))
		for i := range b {
			if rnd.IntN(4) == 0 {
				b[i] = byte(rnd.Uint32())
			} else {
				b[i] = alphabet[rnd.IntN(len(alphabet))]
			}
		}
		if _, _, _, err := MySQLToStored(b); err != nil {
			require.True(t, errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooDeep), "%X: %v", b, err)
		}
		stored := append([]byte{Version1}, b...)
		checkAgainstReference(t, stored)
		_, _, _ = Header(stored)
	}
}

func TestHeader(t *testing.T) {
	wkts := []string{
		"POINT(1 2)",
		"POINT ZM(1 2 3 4)",
		"POINT EMPTY",
		"LINESTRING(0 0,1 1)",
		"LINESTRING M(0 0 1,1 1 2)",
		"POLYGON((0 0,0 1,1 1,0 0))",
		"POLYGON EMPTY",
		"MULTIPOINT((1 2))",
		"MULTIPOINT Z((1 2 3))",
		"MULTILINESTRING((0 0,1 1))",
		"MULTILINESTRING EMPTY",
		"MULTIPOLYGON(((0 0,0 1,1 1,0 0)))",
		"MULTIPOLYGON M(((0 0 1,0 1 1,1 1 1,0 0 1)))",
		"GEOMETRYCOLLECTION(POINT(1 2))",
		"GEOMETRYCOLLECTION EMPTY",
	}
	for _, wkt := range wkts {
		g := mustWKT(t, wkt)
		for _, srid := range []uint32{0, 4326, 3857, math.MaxUint32} {
			stored, err := encode(g, srid)
			require.NoError(t, err)
			gotSRID, typ, err := Header(stored)
			require.NoError(t, err, wkt)
			require.Equal(t, srid, gotSRID, wkt)
			require.Equal(t, g.Type(), typ, wkt)

			// Header reads the version, byte order, type word and SRID, and no more.
			gotSRID, typ, err = Header(stored[:10])
			require.NoError(t, err, wkt)
			require.Equal(t, srid, gotSRID, wkt)
			require.Equal(t, g.Type(), typ, wkt)
			for n := range 10 {
				_, _, err = Header(stored[:n])
				require.ErrorIs(t, err, ErrInvalid, "%s cut to %d", wkt, n)
			}
		}
	}

	// Without the SRID flag the SRID is 0.
	srid, typ, err := Header(mustHex(t, "01 01 05000000"))
	require.NoError(t, err)
	require.Equal(t, uint32(0), srid)
	require.Equal(t, geom.TypeMultiLineString, typ)

	for name, h := range map[string]string{
		"bad byte order":    "01 02 01000020 00000000",
		"unknown type code": "01 01 08000020 00000000",
		"ISO type code":     "01 01 E9030000",
	} {
		_, _, err := Header(mustHex(t, h))
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

// geomGen generates random geometries of one coordinate type.
type geomGen struct {
	rnd   *rand.Rand
	ctype geom.CoordinatesType
	// empty is the chance that a geometry other than a GEOMETRYCOLLECTION is empty.
	empty float64
}

var allTypes = []geom.GeometryType{
	geom.TypePoint,
	geom.TypeLineString,
	geom.TypePolygon,
	geom.TypeMultiPoint,
	geom.TypeMultiLineString,
	geom.TypeMultiPolygon,
	geom.TypeGeometryCollection,
}

// maxGenDepth bounds how deep generated collections nest.
const maxGenDepth = 4

func (gen *geomGen) isEmpty() bool { return gen.rnd.Float64() < gen.empty }

func (gen *geomGen) float() float64 {
	switch gen.rnd.IntN(6) {
	case 0:
		return 0
	case 1:
		return math.Copysign(0, -1)
	case 2:
		return math.SmallestNonzeroFloat64 * float64(1+gen.rnd.IntN(1000))
	case 3:
		return math.Copysign(math.MaxFloat64, gen.rnd.NormFloat64())
	}
	return gen.rnd.NormFloat64() * 1000
}

func (gen *geomGen) sequence(n int) geom.Sequence {
	floats := make([]float64, n*gen.ctype.Dimension())
	for i := range floats {
		floats[i] = gen.float()
	}
	return geom.NewSequence(floats, gen.ctype)
}

func (gen *geomGen) point() geom.Point {
	if gen.isEmpty() {
		return geom.NewEmptyPoint(gen.ctype)
	}
	return geom.NewPoint(gen.sequence(1).Get(0))
}

func (gen *geomGen) lineString() geom.LineString {
	n := 0
	if !gen.isEmpty() {
		n = 1 + gen.rnd.IntN(5)
	}
	return geom.NewLineString(gen.sequence(n))
}

func (gen *geomGen) polygon() geom.Polygon {
	if gen.isEmpty() {
		return geom.Polygon{}.ForceCoordinatesType(gen.ctype)
	}
	rings := make([]geom.LineString, 1+gen.rnd.IntN(3))
	for i := range rings {
		rings[i] = geom.NewLineString(gen.sequence(1 + gen.rnd.IntN(5)))
	}
	return geom.NewPolygon(rings)
}

// members returns how many members a MULTIPOINT, MULTILINESTRING or MULTIPOLYGON has.
func (gen *geomGen) members() int {
	if gen.isEmpty() {
		return 0
	}
	return 1 + gen.rnd.IntN(3)
}

func (gen *geomGen) geometry(typ geom.GeometryType, depth int) geom.Geometry {
	switch typ {
	case geom.TypePoint:
		return gen.point().AsGeometry()
	case geom.TypeLineString:
		return gen.lineString().AsGeometry()
	case geom.TypePolygon:
		return gen.polygon().AsGeometry()
	case geom.TypeMultiPoint:
		pts := make([]geom.Point, gen.members())
		for i := range pts {
			pts[i] = gen.point()
		}
		return geom.NewMultiPoint(pts).ForceCoordinatesType(gen.ctype).AsGeometry()
	case geom.TypeMultiLineString:
		lss := make([]geom.LineString, gen.members())
		for i := range lss {
			lss[i] = gen.lineString()
		}
		return geom.NewMultiLineString(lss).ForceCoordinatesType(gen.ctype).AsGeometry()
	case geom.TypeMultiPolygon:
		polys := make([]geom.Polygon, gen.members())
		for i := range polys {
			polys[i] = gen.polygon()
		}
		return geom.NewMultiPolygon(polys).ForceCoordinatesType(gen.ctype).AsGeometry()
	}
	// A GEOMETRYCOLLECTION is expressible empty, so it is empty a quarter of the time.
	members := make([]geom.Geometry, gen.rnd.IntN(4))
	for i := range members {
		types := allTypes
		if depth >= maxGenDepth {
			types = allTypes[:len(allTypes)-1]
		}
		members[i] = gen.geometry(types[gen.rnd.IntN(len(types))], depth+1)
	}
	return geom.NewGeometryCollection(members).ForceCoordinatesType(gen.ctype).AsGeometry()
}

func (gen *geomGen) srid() uint32 {
	switch gen.rnd.IntN(4) {
	case 0:
		return 0
	case 1:
		return 4326
	case 2:
		return 3857
	}
	return gen.rnd.Uint32()
}

// TestStoredToMySQLMatchesReference compares StoredToMySQL, which rewrites the header,
// with decoding the value and writing it with the MySQL-dialect writer, over generated
// geometries of every type and coordinate type, with and without empty members.
func TestStoredToMySQLMatchesReference(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261002, 1))
	expressible, notExpressible := 0, 0
	for _, ctype := range []geom.CoordinatesType{geom.DimXY, geom.DimXYZ, geom.DimXYM, geom.DimXYZM} {
		for _, empty := range []float64{0, 0.15} {
			gen := geomGen{rnd: rnd, ctype: ctype, empty: empty}
			for i := range 700 {
				g := gen.geometry(allTypes[i%len(allTypes)], 1)
				require.Equal(t, ctype, g.CoordinatesType())
				srid := gen.srid()
				stored, err := encode(g, srid)
				require.NoError(t, err)

				got, err := StoredToMySQL(stored)
				want, wantErr := referenceStoredToMySQL(stored)
				require.Equal(t, wantErr, err, g.AsText())
				require.Equal(t, want, got, g.AsText())
				if err != nil {
					require.ErrorIs(t, err, ErrNotMySQLExpressible, g.AsText())
					require.False(t, ctype == geom.DimXY && empty == 0, g.AsText())
					notExpressible++
					continue
				}
				require.Equal(t, geom.DimXY, ctype, g.AsText())
				expressible++

				// MySQL's format reads back as the same stored value.
				again, gotSRID, sum, err := MySQLToStored(got)
				require.NoError(t, err, g.AsText())
				require.Equal(t, stored, again, g.AsText())
				require.Equal(t, srid, gotSRID)
				requireSummary(t, g, sum)
			}
		}
	}
	require.Greater(t, expressible, 1000)
	require.Greater(t, notExpressible, 3000)
}

// TestStoredToMySQLMutations compares StoredToMySQL with the reference on generated
// stored values with one byte changed, which reach the checks a well-formed value does
// not.
func TestStoredToMySQLMutations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(20261002, 2))
	values := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf8, 0xff}
	for _, ctype := range []geom.CoordinatesType{geom.DimXY, geom.DimXYZM} {
		gen := geomGen{rnd: rnd, ctype: ctype, empty: 0.1}
		for i := range 70 {
			stored, err := encode(gen.geometry(allTypes[i%len(allTypes)], 1), gen.srid())
			require.NoError(t, err)
			// Every position of a small value, a sample of a large one.
			positions := rnd.Perm(len(stored))[:min(len(stored), 200)]
			for _, pos := range positions {
				b := append([]byte{}, stored...)
				for _, v := range values {
					b[pos] = v
					checkAgainstReference(t, b)
				}
				b[pos] = stored[pos] ^ byte(1+rnd.IntN(255))
				checkAgainstReference(t, b)
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
	stored := append([]byte{Version1, byteOrderLittle}, b[5:]...)
	_, _, _, err := MySQLToStored(b)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = StoredToMySQL(stored)
	require.ErrorIs(t, err, ErrInvalid)
	checkAgainstReference(t, stored)
	// The reader allocates only the root's members before the second count fails, and
	// the scan nothing, also ahead of building.
	allocs := testing.AllocsPerRun(1, func() {
		_, _, _ = build(b[4:], dialectMySQL)
	})
	require.Equal(t, 1.0, allocs)
	allocs = testing.AllocsPerRun(1, func() {
		_, _, _ = read(b[4:], dialectMySQL)
	})
	require.Equal(t, 0.0, allocs)
	allocs = testing.AllocsPerRun(1, func() {
		_, _, _, _ = MySQLToStored(b)
	})
	require.Equal(t, 0.0, allocs)
}

func TestStoredToMySQLAllocs(t *testing.T) {
	stored := mustEncode(t, "GEOMETRYCOLLECTION(POINT(1 2),MULTIPOLYGON(((0 0,0 1,1 1,0 0))),"+
		"GEOMETRYCOLLECTION(LINESTRING(0 0,1 1,2 2)))", 4326)
	// The scan allocates nothing: the one allocation is the result.
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = StoredToMySQL(stored)
	})
	require.Equal(t, 1.0, allocs)
	for _, b := range [][]byte{mustEncode(t, "POINT Z(1 2 3)", 0), stored[:len(stored)-1]} {
		allocs = testing.AllocsPerRun(100, func() {
			_, _ = StoredToMySQL(b)
		})
		require.Equal(t, 0.0, allocs)
	}
}

func FuzzMySQLToStored(f *testing.F) {
	f.Add(mustHex(f, "E6100000 01 01000000 0000000000000040 000000000000F03F"))
	f.Add(mustHex(f, "00000000 01 07000000 01000000 01 03000000 01000000 04000000"+
		" 0000000000000000 0000000000000000 000000000000F03F 0000000000000000"+
		" 000000000000F03F 000000000000F03F 0000000000000000 0000000000000000"))
	f.Add(mustHex(f, "00000000 01 07000000 02000000 01 01000000 000000000000F03F 0000000000000040"+
		" 01 04000000 01000000 01 01000000 0000000000000840 0000000000001040"))
	f.Fuzz(func(t *testing.T, b []byte) {
		stored, _, _, err := MySQLToStored(b)
		if err != nil {
			return
		}
		require.Len(t, stored, len(b)+1)
		// MySQL's format is canonical: whatever is stored converts back to the same bytes.
		out, err := StoredToMySQL(stored)
		require.NoError(t, err)
		require.Equal(t, b, out)
		checkAgainstReference(t, stored)
	})
}

func FuzzStoredToMySQL(f *testing.F) {
	for _, wkt := range []string{
		"POINT(1 2)",
		"POLYGON Z((0 0 1,0 1 1,1 1 1,0 0 1))",
		"GEOMETRYCOLLECTION(MULTIPOINT((1 2)),LINESTRING EMPTY)",
		"GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,0 1,1 1,0 0))),GEOMETRYCOLLECTION EMPTY)",
	} {
		f.Add(mustEncode(f, wkt, 4326))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkAgainstReference(t, b)
		out, err := StoredToMySQL(b)
		if err != nil {
			return
		}
		// What converts reads back, and converts again to the same bytes.
		again, _, _, err := MySQLToStored(out)
		require.NoError(t, err)
		out2, err := StoredToMySQL(again)
		require.NoError(t, err)
		require.Equal(t, out, out2)
	})
}
