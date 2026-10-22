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

	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
)

// referenceMySQLToStored is MySQLToStored as it was before the scan: it reads the value
// into a geometry and encodes that.
func referenceMySQLToStored(b []byte) (stored []byte, srid uint32, g geom.Geometry, err error) {
	if len(b) < 4 {
		return nil, 0, geom.Geometry{}, ErrInvalid
	}
	srid = binary.LittleEndian.Uint32(b)
	if _, g, err = build(b[4:], dialectMySQL); err != nil {
		return nil, 0, geom.Geometry{}, err
	}
	if stored, err = encode(g, srid); err != nil {
		return nil, 0, geom.Geometry{}, err
	}
	return stored, srid, g, nil
}

// requireSummary requires sum to summarize g.
func requireSummary(t testing.TB, g geom.Geometry, sum Summary) {
	require.Equal(t, Summary{Type: g.Type()}, sum, g.AsText())
}

// ingestStats counts what checkIngest has compared.
var ingestStats struct {
	accepted, rejected int
	// tooComplex counts the values read refused for the budget, which build built.
	tooComplex int
}

// checkRead requires read, which scans before it builds, to agree with build on b in
// dialect d, except that it refuses with ErrTooComplex a value over the budget, which build
// builds. A value read accepts is the one build builds, by construction.
func checkRead(t testing.TB, b []byte, d dialect) {
	srid, _, err := read(b, d)
	wantSRID, _, wantErr := build(b, d)
	if errors.Is(err, ErrTooComplex) {
		require.NoError(t, wantErr, "%X", b)
		ingestStats.tooComplex++
		return
	}
	require.Equal(t, wantErr, err, "%X", b)
	require.Equal(t, wantSRID, srid, "%X", b)
}

// checkIngest requires MySQLToStored to agree with the reference conversion on b: the
// same error, or the same stored bytes, SRID and geometry type. It also checks read on b
// and on b's payload in both dialects, and StoredToMySQL on b.
func checkIngest(t testing.TB, b []byte) {
	for _, d := range []dialect{dialectEWKB, dialectMySQL} {
		checkRead(t, b, d)
	}
	if len(b) >= 1 {
		checkRead(t, b[1:], dialectEWKB)
	}
	if len(b) >= 4 {
		checkRead(t, b[4:], dialectMySQL)
	}
	checkAgainstReference(t, b)
	got, gotSRID, sum, err := MySQLToStored(b)
	want, wantSRID, g, wantErr := referenceMySQLToStored(b)
	require.Equal(t, wantErr, err, "%X", b)
	require.Equal(t, want, got, "%X", b)
	if err != nil {
		ingestStats.rejected++
		return
	}
	ingestStats.accepted++
	// The stored format adds only the version byte: the SRID moves behind the type word.
	require.Len(t, got, len(b)+1, "%X", b)
	require.Equal(t, wantSRID, gotSRID, "%X", b)
	requireSummary(t, g, sum)
}

// mixedWriter writes a stored value as writer does, except that each geometry below the
// root takes the byte order big picks, and a POINT EMPTY the NaN bits nan.
type mixedWriter struct {
	buf  []byte
	srid uint32
	big  func() bool
	nan  uint64
}

func mixedEncode(g geom.Geometry, srid uint32, big func() bool, nan uint64) []byte {
	w := mixedWriter{buf: []byte{Version1}, srid: srid, big: big, nan: nan}
	w.geometry(g, true)
	return w.buf
}

func (w *mixedWriter) geometry(g geom.Geometry, root bool) {
	var order binary.AppendByteOrder = binary.LittleEndian
	orderByte := byteOrderLittle
	if !root && w.big() {
		order, orderByte = binary.BigEndian, byteOrderBig
	}
	ctype := g.CoordinatesType()
	code, _ := typeCodeOf(g.Type())
	if ctype.Is3D() {
		code |= ewkbZ
	}
	if ctype.IsMeasured() {
		code |= ewkbM
	}
	if root {
		code |= ewkbSRID
	}
	w.buf = append(w.buf, orderByte)
	w.buf = order.AppendUint32(w.buf, code)
	if root {
		w.buf = order.AppendUint32(w.buf, w.srid)
	}
	float := func(v float64) { w.buf = order.AppendUint64(w.buf, math.Float64bits(v)) }
	coordinates := func(c geom.Coordinates) {
		float(c.X)
		float(c.Y)
		if ctype.Is3D() {
			float(c.Z)
		}
		if ctype.IsMeasured() {
			float(c.M)
		}
	}
	sequence := func(seq geom.Sequence) {
		w.buf = order.AppendUint32(w.buf, uint32(seq.Length()))
		for i := range seq.Length() {
			coordinates(seq.Get(i))
		}
	}
	members := func(n int, member func(int) geom.Geometry) {
		w.buf = order.AppendUint32(w.buf, uint32(n))
		for i := range n {
			w.geometry(member(i), false)
		}
	}
	switch g.Type() {
	case geom.TypePoint:
		if c, ok := g.MustAsPoint().Coordinates(); ok {
			coordinates(c)
		} else {
			for range ctype.Dimension() {
				w.buf = order.AppendUint64(w.buf, w.nan)
			}
		}
	case geom.TypeLineString:
		sequence(g.MustAsLineString().Coordinates())
	case geom.TypePolygon:
		rings := g.MustAsPolygon().Coordinates()
		w.buf = order.AppendUint32(w.buf, uint32(len(rings)))
		for _, ring := range rings {
			sequence(ring)
		}
	case geom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		members(mp.NumPoints(), func(i int) geom.Geometry { return mp.PointN(i).AsGeometry() })
	case geom.TypeMultiLineString:
		mls := g.MustAsMultiLineString()
		members(mls.NumLineStrings(), func(i int) geom.Geometry { return mls.LineStringN(i).AsGeometry() })
	case geom.TypeMultiPolygon:
		mp := g.MustAsMultiPolygon()
		members(mp.NumPolygons(), func(i int) geom.Geometry { return mp.PolygonN(i).AsGeometry() })
	case geom.TypeGeometryCollection:
		gc := g.MustAsGeometryCollection()
		members(gc.NumGeometries(), gc.GeometryN)
	}
}

// nanBits are NaNs a POINT EMPTY may be written with: the quiet NaN TiDB writes, Go's
// math.NaN(), a negative quiet NaN, a signaling NaN and the largest NaN.
var nanBits = []uint64{emptyPointNaN, 0x7FF8000000000001, 0xFFF8000000000000, 0x7FF0000000000001, 0x7FFFFFFFFFFFFFFF}

// ingestForms returns the forms g is given in: the stored format, MySQL's format if g has
// one, and the stored format with big-endian members and other NaNs, which the reader
// takes.
func ingestForms(t testing.TB, rnd *rand.Rand, g geom.Geometry, srid uint32) [][]byte {
	stored, err := encode(g, srid)
	require.NoError(t, err)
	forms := [][]byte{stored}
	if mysql, err := StoredToMySQL(stored); err == nil {
		forms = append(forms, mysql)
	}
	allBig := func() bool { return true }
	someBig := func() bool { return rnd.IntN(2) == 0 }
	nan := nanBits[rnd.IntN(len(nanBits))]
	return append(forms, mixedEncode(g, srid, allBig, nan), mixedEncode(g, srid, someBig, nan))
}

// TestIngestMatchesReference compares the scan with the reference conversions over
// generated geometries of every type and coordinate type, with and without empty members,
// in every form, and over those forms with one byte changed, which reach the checks a
// well-formed value does not.
func TestIngestMatchesReference(t *testing.T) {
	ingestStats.accepted, ingestStats.rejected, ingestStats.tooComplex = 0, 0, 0
	rnd := rand.New(rand.NewPCG(20261020, 1))
	changes := []byte{0, 1, 2, 3, 4, 7, 0x20, 0x80, 0xf0, 0xf8, 0x7f, 0xff}
	for _, ctype := range []geom.CoordinatesType{geom.DimXY, geom.DimXYZ, geom.DimXYM, geom.DimXYZM} {
		for _, empty := range []float64{0, 0.15} {
			gen := geomGen{rnd: rnd, ctype: ctype, empty: empty}
			for i := range 300 {
				g := gen.geometry(allTypes[i%len(allTypes)], 1)
				for _, b := range ingestForms(t, rnd, g, gen.srid()) {
					checkIngest(t, b)
					if i%10 != 0 {
						continue
					}
					for _, pos := range rnd.Perm(len(b))[:min(len(b), 40)] {
						m := append([]byte{}, b...)
						for _, v := range changes {
							m[pos] = v
							checkIngest(t, m)
						}
						m[pos] = b[pos] ^ byte(1+rnd.IntN(255))
						checkIngest(t, m)
					}
					for n := range min(len(b), 64) {
						checkIngest(t, b[:n])
					}
					checkIngest(t, append(append([]byte{}, b...), 0))
				}
			}
		}
	}
	t.Logf("MySQLToStored accepted %d, rejected %d; read over the decode budget %d",
		ingestStats.accepted, ingestStats.rejected, ingestStats.tooComplex)
	// Generated values nest at most a few levels, as real data does, and are always built.
	require.Zero(t, ingestStats.tooComplex)
	require.Greater(t, ingestStats.accepted, 10000)
	require.Greater(t, ingestStats.rejected, 100000)
}

// TestIngestEdges compares the scan with the reference on values at the depth bound, with
// hostile counts, with big-endian members, with NaN and infinite points, with a malformed
// member ahead of or behind one nested too deep, and on large and deep values.
func TestIngestEdges(t *testing.T) {
	point := mustHex(t, "01 01000000 000000000000F03F 0000000000000040")
	line0 := mustHex(t, "01 02000000 00000000")
	bePoint := mustHex(t, "00 00000001 3FF0000000000000 4000000000000000")
	emptyPoint := mustHex(t, "01 01000000 010000000000F87F 000000000000F87F")
	deep := nestedCollections(MaxDepth)
	cases := [][]byte{
		mysqlChain(MaxDepth, point),
		mysqlChain(MaxDepth+1, point),
		storedChain(MaxDepth, 0, point),
		storedChain(MaxDepth+1, 0, point),
		storedChain(MaxDepth, 0, bePoint),
		storedChain(MaxDepth, 0, emptyPoint),
		storedChain(MaxDepth+1, 0, emptyPoint),
		mysqlChain(MaxDepth-1, multiPointWKB(1000)),
		storedFromMySQL(mysqlChain(MaxDepth-1, multiPointWKB(1000))),
		bigEndianChain(MaxDepth-1, bigEndianMultiPointWKB(1000)),
		hostileCounts(),
		storedFromMySQL(hostileCounts()),
		append(binary.LittleEndian.AppendUint32(nil, 4326), appendPointWKB(nil, binary.LittleEndian, 2, 1)...),
		append(make([]byte, 4), polygonWKB(1000)...),
		append(make([]byte, 4), multiPointWKB(10000)...),
		bigEndianChain(2, bigEndianMultiPointWKB(10000)),
	}
	// NaN and infinite coordinates, which only a stored POINT EMPTY may have: all NaN.
	for _, h := range []string{
		"00000000 01 01000000 000000000000F87F 000000000000F87F",
		"00000000 01 01000000 010000000000F87F 000000000000F8FF",
		"00000000 01 04000000 01000000 01 01000000 000000000000F87F 000000000000F87F",
		"00000000 01 01000000 000000000000F87F 0000000000000040",
		"00000000 01 01000000 000000000000F07F 000000000000F07F",
		"01 01 01000020 00000000 000000000000F07F 000000000000F07F",
		"01 01 01000020 00000000 000000000000F87F 000000000000F07F",
		"01 01 01000020 00000000 010000000000F07F 000000000000F87F",
		"01 01 010000A0 00000000 000000000000F87F 000000000000F87F 0000000000000000",
		"01 01 04000020 00000000 01000000 00 00000001 7FF8000000000001 FFF8000000000000",
	} {
		cases = append(cases, mustHex(t, h))
	}
	// An empty linestring, which MySQL's format rejects, ahead of a member nested too deep
	// and behind it: the first error in reading order is reported.
	for _, members := range [][][]byte{{line0, deep}, {deep, line0}} {
		m := binary.LittleEndian.AppendUint32(nil, 0)
		m = append(m, byteOrderLittle)
		m = binary.LittleEndian.AppendUint32(m, 7)
		m = binary.LittleEndian.AppendUint32(m, 2)
		m = append(append(m, members[0]...), members[1]...)
		cases = append(cases, m, storedFromMySQL(m))
	}
	ingestStats.tooComplex = 0
	for _, b := range cases {
		checkIngest(t, b)
	}
	// Only the chains around 1,000 points are over the decode budget.
	t.Logf("over the decode budget: %d", ingestStats.tooComplex)
	require.Positive(t, ingestStats.tooComplex)
}

// TestIngestAllocs checks that MySQLToStored's scan allocates nothing: a value that
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
		allocs := testing.AllocsPerRun(3, func() { _, _, _, _ = MySQLToStored(c.b) })
		require.Equal(t, c.allocs, allocs, "%X", c.b[:min(len(c.b), 32)])
	}
}

// FuzzIngestScan compares the scan with the reference conversions on b, and on b read as
// WKB in MySQL's format and as a stored payload.
func FuzzIngestScan(f *testing.F) {
	for _, h := range []string{
		"E6100000 01 01000000 0000000000000040 000000000000F03F",
		"00000000 01 03000000 01000000 04000000 0000000000000000 0000000000000000" +
			" 000000000000F03F 0000000000000000 000000000000F03F 000000000000F03F 0000000000000000 000000000000F03F",
		"00000000 01 07000000 02000000 01 01000000 000000000000F03F 0000000000000040" +
			" 01 04000000 01000000 01 01000000 0000000000000840 0000000000001040",
		"00000000 01 07000000 01000000 01 02000000 01000000 000000000000F03F 0000000000000040",
		"01 01 04000020 00000000 01000000 00 00000001 3FF0000000000000 4000000000000000",
		"01 01 01000020 00000000 010000000000F87F 000000000000F87F",
		"00000000 01 01000000 000000000000F87F 000000000000F87F",
		"00000000 01 04000000 01000000 01 01000000 000000000000F87F 000000000000F87F",
	} {
		f.Add(mustHex(f, h))
	}
	for _, wkt := range []string{
		"POINT(1 2)",
		"POLYGON Z((0 0 1,0 1 1,1 1 1,0 0 1))",
		"GEOMETRYCOLLECTION(MULTIPOINT((1 2)),LINESTRING EMPTY)",
		"GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,0 1,1 1,0 0))),GEOMETRYCOLLECTION EMPTY)",
		"POLYGON((0 0,1 0,1 1,0 0),(0 0,0 100,1 1,0 0))",
		"MULTIPOINT M(EMPTY,(1 2 3))",
	} {
		g, err := geom.UnmarshalWKT(wkt, geom.NoValidate{})
		require.NoError(f, err)
		stored, err := encode(g, 4326)
		require.NoError(f, err)
		f.Add(stored)
		f.Add(mixedEncode(g, 0, func() bool { return true }, 0x7FF8000000000001))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkIngest(t, b)
		checkIngest(t, append(make([]byte, 4), b...))
		checkIngest(t, append([]byte{Version1}, b...))
	})
}

// TestDecodeBudget checks which shapes the decode budget refuses: real-shaped data is
// built, also nested; a short chain to the depth bound is built; much data, or many
// geometries however small, nested deep are refused before anything is allocated.
func TestDecodeBudget(t *testing.T) {
	for _, c := range budgetCases() {
		payload := storedFromMySQL(c.mysql)[1:]
		s := scanner{r: reader{buf: payload, dialect: dialectEWKB}, ingest: true}
		require.NoError(t, s.scan(), c.name)
		_, _, err := read(payload, dialectEWKB)
		t.Logf("%s: %d bytes, work %d (%.1f times the size), %v", c.name, len(payload), s.work,
			float64(s.work)/float64(len(payload)), err)
		if !c.refused {
			require.NoError(t, err, c.name)
			continue
		}
		require.ErrorIs(t, err, ErrTooComplex, c.name)
		_, _, err = read(c.mysql[4:], dialectMySQL)
		require.ErrorIs(t, err, ErrTooComplex, c.name)
		allocs := testing.AllocsPerRun(2, func() { _, _, _ = read(payload, dialectEWKB) })
		require.Equal(t, 0.0, allocs, c.name)
	}
	// Storing and converting are not limited.
	for _, c := range budgetCases() {
		stored, _, _, err := MySQLToStored(c.mysql)
		require.NoError(t, err, c.name)
		back, err := StoredToMySQL(stored)
		require.NoError(t, err, c.name)
		require.Equal(t, c.mysql, back, c.name)
	}
}

// budgetCases are values in MySQL's format of the shapes the decode budget is measured on,
// and whether it refuses to build them.
func budgetCases() []struct {
	name    string
	mysql   []byte
	refused bool
} {
	multiPolygon := multiPolygonWKB(1000, 21)
	points := collectionOfPointsWKB(10000)
	empties := make([][]byte, 100000)
	for i := range empties {
		empties[i] = nestedCollections(1)
	}
	return []struct {
		name    string
		mysql   []byte
		refused bool
	}{
		{"MultiPolygon1000Islands", append(make([]byte, 4), multiPolygon...), false},
		{"CollectionOf10kPoints", append(make([]byte, 4), points...), false},
		{"Nested3Deep", append(make([]byte, 4), collectionWKB(collectionWKB(collectionWKB(multiPolygon, points), points), multiPolygon)...), false},
		{"Deep1024EmptyCollections", append(make([]byte, 4), nestedCollections(MaxDepth)...), false},
		{"Deep100MultiPoint10k", mysqlChain(100, multiPointWKB(10000)), true},
		{"Deep1023MultiPoint100k", mysqlChain(1023, multiPointWKB(100000)), true},
		// Each copy of a geometry counts, however small: 100,000 empty collections below
		// 16 collections, 900 kB, take 32 MB of work, where their bytes alone take 14 MB.
		// As many empty polygons take the same work and allocate 159 MB to build.
		{"Deep16CollectionOf100kEmpty", mysqlChain(16, collectionWKB(empties...)), true},
	}
}

// storedFromMySQL rewrites the root header of MySQL-format bytes into the stored format.
func storedFromMySQL(m []byte) []byte {
	b := []byte{Version1, byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, binary.LittleEndian.Uint32(m[5:])|ewkbSRID)
	b = binary.LittleEndian.AppendUint32(b, binary.LittleEndian.Uint32(m))
	return append(b, m[9:]...)
}

func appendPointWKB(b []byte, order binary.AppendByteOrder, x, y float64) []byte {
	if order == binary.BigEndian {
		b = append(b, byteOrderBig)
	} else {
		b = append(b, byteOrderLittle)
	}
	b = order.AppendUint32(b, 1)
	b = order.AppendUint64(b, math.Float64bits(x))
	return order.AppendUint64(b, math.Float64bits(y))
}

// multiPointWKB returns little-endian WKB, without an SRID, for a MULTIPOINT of n points.
func multiPointWKB(n int) []byte {
	b := []byte{byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 4)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	for i := range n {
		b = appendPointWKB(b, binary.LittleEndian, float64(i%360-180), float64(i%180-90))
	}
	return b
}

// bigEndianMultiPointWKB returns big-endian WKB for a MULTIPOINT of n big-endian points.
func bigEndianMultiPointWKB(n int) []byte {
	b := []byte{byteOrderBig}
	b = binary.BigEndian.AppendUint32(b, 4)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	for i := range n {
		b = appendPointWKB(b, binary.BigEndian, float64(i%360-180), float64(i%180-90))
	}
	return b
}

// polygonWKB returns little-endian WKB for a POLYGON of one closed ring of n points.
func polygonWKB(n int) []byte {
	b := []byte{byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 3)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	for i := range n {
		a := 2 * math.Pi * float64(i%(n-1)) / float64(n-1)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(10*math.Cos(a)))
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(10*math.Sin(a)))
	}
	return b
}

// multiPolygonWKB returns little-endian WKB for a MULTIPOLYGON of n islands, each a closed
// ring of points points.
func multiPolygonWKB(n, points int) []byte {
	b := []byte{byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 6)
	b = binary.LittleEndian.AppendUint32(b, uint32(n))
	for i := range n {
		ring := polygonWKB(points)
		// Move the island: 10 degrees of radius, 25 apart.
		for j := 13; j+16 <= len(ring); j += 16 {
			x := math.Float64frombits(binary.LittleEndian.Uint64(ring[j:])) + float64(i%14*25-170)
			y := math.Float64frombits(binary.LittleEndian.Uint64(ring[j+8:])) + float64(i/14%7*25-75)
			binary.LittleEndian.PutUint64(ring[j:], math.Float64bits(x))
			binary.LittleEndian.PutUint64(ring[j+8:], math.Float64bits(y))
		}
		b = append(b, ring...)
	}
	return b
}

// collectionWKB returns little-endian WKB for a GEOMETRYCOLLECTION of members.
func collectionWKB(members ...[]byte) []byte {
	b := []byte{byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 7)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, m := range members {
		b = append(b, m...)
	}
	return b
}

// collectionOfPointsWKB returns little-endian WKB for a GEOMETRYCOLLECTION of n points.
func collectionOfPointsWKB(n int) []byte {
	points := make([][]byte, n)
	for i := range points {
		points[i] = appendPointWKB(nil, binary.LittleEndian, float64(i%360-180), float64(i%180-90))
	}
	return collectionWKB(points...)
}

// bigEndianChain returns a stored value at SRID 4326 of depth-1 collections around leaf,
// every one below the root big-endian.
func bigEndianChain(depth int, leaf []byte) []byte {
	b := []byte{Version1, byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, 7|ewkbSRID)
	b = binary.LittleEndian.AppendUint32(b, 4326)
	b = binary.LittleEndian.AppendUint32(b, 1)
	for range depth - 2 {
		b = append(b, byteOrderBig)
		b = binary.BigEndian.AppendUint32(b, 7)
		b = binary.BigEndian.AppendUint32(b, 1)
	}
	return append(b, leaf...)
}

// hostileCounts returns a 1 MiB value in MySQL's format of collections nested to the
// depth bound, each claiming as many members as the rest of the value could hold.
func hostileCounts() []byte {
	const size = 1 << 20
	b := make([]byte, 4, size)
	for range MaxDepth {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7)
		b = binary.LittleEndian.AppendUint32(b, uint32((size-len(b)-4)/minGeometrySize))
	}
	return append(b, make([]byte, size-len(b))...)
}
