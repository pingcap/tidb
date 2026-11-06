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

// Package geocodec converts between the two byte forms of a geometry value.
//
//   - The stored format, which is what a geometry column holds and what every geometry
//     expression evaluates to: a format-version byte followed by little-endian EWKB, the
//     PostGIS/GEOS extension of WKB whose type word carries Z, M and SRID flags. The SRID
//     flag is always set, so a stored value is self-describing.
//   - MySQL's binary format, which a bare SELECT returns and a bare literal carries: a
//     4-byte little-endian SRID followed by 2D WKB.
//
// The stored format and MySQL's format hold coordinates in the same order, longitude
// first on a geographic SRS, and below the root header their bodies are the same bytes,
// so converting a stored value to MySQL's format rewrites only the header.
//
// Converting between the formats, in either direction, builds no geometry: a scan checks
// the value, and then only the header is rewritten. A geometry is built only when one is
// read, so a conversion costs what the value's size does, whatever its nesting.
//
// Every reader and every scan bounds nesting at MaxDepth, requires the input to be
// consumed exactly, and checks counts against the input before allocating, each against
// what remains and all of them together against the whole, because the bytes can come
// straight from a client.
package geocodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/peterstace/simplefeatures/geom"
)

const (
	// Version1 is the stored-format version whose payload is EWKB.
	Version1 byte = 1
	// MaxDepth bounds geometry nesting, counted in geometry headers from the root: a
	// POINT is 1, GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2))) is 3. Real data
	// stays in single digits; the bound exists so hostile input fails cleanly instead
	// of overflowing the goroutine stack, which Go cannot recover from.
	MaxDepth = 1024
)

var (
	// ErrInvalid means the bytes are not a well-formed geometry in the expected format.
	ErrInvalid = errors.New("invalid geometry data")
	// ErrTooDeep means the geometry nests deeper than MaxDepth.
	ErrTooDeep = fmt.Errorf("geometry nested deeper than %d levels", MaxDepth)
	// ErrUnsupportedVersion means a stored value starts with an unknown version byte.
	ErrUnsupportedVersion = errors.New("unsupported geometry storage format version")
	// ErrNotMySQLExpressible means the geometry has no form in MySQL's binary format: it
	// has Z or M coordinates, or an empty geometry other than a GEOMETRYCOLLECTION at any
	// depth, which WKB cannot represent or MySQL does not accept.
	ErrNotMySQLExpressible = errors.New("geometry cannot be expressed in MySQL's binary format")
	// ErrTooComplex means building the geometry would take too much work for its size:
	// it nests much of its data in many collections. The value is well-formed and can be
	// stored and converted; only building it as a geometry is refused.
	ErrTooComplex = errors.New("geometry nests too much of its data too deep to be built")
)

// The budget for building a geometry. Building a collection copies everything below it,
// at every level, so the work is the sum, over the collections, of the bytes of their
// members and of decodeWorkPerGeometry for each geometry below them, which the scan
// measures before anything is built. Data that nests in single digits needs at most a few
// times its size; a collection chain around most of the value needs its depth times its
// size or more: 1.5 times that around points, 2.2 times around empty geometries.
const (
	// decodeWorkFactor is the work allowed per byte of the value.
	decodeWorkFactor = 16
	// decodeWorkFloor is the work always allowed, so short values nested deep, such as
	// 1,024 empty collections (about 10.5 MB of work), are still built.
	decodeWorkFloor = 16 << 20
	// decodeWorkPerGeometry is the work each geometry below a collection adds to building
	// it, besides its bytes. A copy of a geometry allocates about 48 bytes whatever its
	// size, a collection member's 16-byte geom.Geometry and its 32-byte struct, so its
	// bytes alone undercount a small one: an empty polygon is 9 bytes. Building allocates
	// about 4.6 bytes per byte of work, measured on collection chains around points, so
	// the 48 bytes count as 48 / 4.6 = 10.4 bytes of work, rounded up.
	decodeWorkPerGeometry = 11
)

// decodeWork is the work of building a geometry, which the scan of its WKB counts
// collection by collection. A reader that builds a geometry from another format counts
// it through the same methods, so that a value is refused or built whatever its format.
type decodeWork uint64

// addCollection adds the work of building a collection whose members take memberBytes
// bytes of WKB and hold geometries geometries, the members and every geometry below
// them: building it copies each of them.
func (w *decodeWork) addCollection(memberBytes, geometries uint64) {
	*w += decodeWork(memberBytes + decodeWorkPerGeometry*geometries)
}

// exceeds reports whether the work passes the budget for a geometry of size bytes of WKB:
// max(decodeWorkFactor x size, decodeWorkFloor).
func (w decodeWork) exceeds(size uint64) bool {
	return uint64(w) > max(decodeWorkFactor*size, decodeWorkFloor)
}

const (
	ewkbZ     uint32 = 0x80000000
	ewkbM     uint32 = 0x40000000
	ewkbSRID  uint32 = 0x20000000
	ewkbFlags        = ewkbZ | ewkbM | ewkbSRID

	byteOrderBig    byte = 0
	byteOrderLittle byte = 1

	// minGeometrySize is the smallest possible WKB geometry: a byte-order byte, a type
	// word and a zero count.
	minGeometrySize = 9

	// anyType lets a GEOMETRYCOLLECTION member be of any type.
	anyType geom.GeometryType = -1

	// emptyPointNaN is the quiet NaN PostGIS and GEOS write for POINT EMPTY. Go's
	// math.NaN() has another bit pattern, and the stored bytes are compared.
	emptyPointNaN uint64 = 0x7FF8000000000000
)

// dialect selects which WKB variant a reader accepts.
type dialect int

const (
	// dialectEWKB is the stored-format payload: either byte order, EWKB flags for Z, M
	// and a root SRID.
	dialectEWKB dialect = iota
	// dialectMySQL is the WKB inside MySQL's binary format: little-endian, 2D, and no
	// empty geometry except a GEOMETRYCOLLECTION.
	dialectMySQL
)

// Summary is what converting a value into the stored format learns about it, besides its
// SRID, without building it.
type Summary struct {
	// Type is the geometry type of the root.
	Type geom.GeometryType
}

// MySQLToStored converts MySQL's binary format into the stored format, and also returns
// the SRID and a summary of the geometry. The WKB must be little-endian at the root and
// in every member, as MySQL's format is. Any SRID is accepted, since the coordinates are
// taken in the order they are stored and no SRS axis order is applied. A scan that builds
// nothing makes the reader's checks, and then only the header is rewritten: below it the
// stored format holds the same bytes.
func MySQLToStored(b []byte) (stored []byte, srid uint32, sum Summary, err error) {
	if len(b) < 4 {
		return nil, 0, Summary{}, ErrInvalid
	}
	s := scanner{r: reader{buf: b[4:], dialect: dialectMySQL}, ingest: true}
	if err := s.scan(); err != nil {
		return nil, 0, Summary{}, err
	}
	// The scan has checked the root header: the little-endian byte order and a type code
	// without flags.
	srid = binary.LittleEndian.Uint32(b)
	stored = make([]byte, 0, len(b)+1)
	stored = append(stored, Version1, byteOrderLittle)
	stored = binary.LittleEndian.AppendUint32(stored, binary.LittleEndian.Uint32(b[5:])|ewkbSRID)
	stored = binary.LittleEndian.AppendUint32(stored, srid)
	return append(stored, b[9:]...), srid, s.sum, nil
}

// StoredToMySQL converts a stored value into MySQL's binary format. A scan that builds
// nothing first checks that the value is well-formed, little-endian throughout and
// expressible in MySQL's format, which rules out Z or M coordinates and an empty
// geometry other than a GEOMETRYCOLLECTION at any depth. Then only the header is
// rewritten: the SRID moves in front and the root type word loses its flags, while the
// body is copied unchanged.
func StoredToMySQL(stored []byte) ([]byte, error) {
	payload, err := payloadOf(stored)
	if err != nil {
		return nil, err
	}
	s := scanner{r: reader{buf: payload, dialect: dialectEWKB}}
	if err := s.scan(); err != nil {
		return nil, err
	}
	if s.notMySQL {
		return nil, ErrNotMySQLExpressible
	}
	// The scan has checked the root header: a little-endian byte order, a type word and,
	// if the SRID flag is set, an SRID.
	word := binary.LittleEndian.Uint32(payload[1:])
	body := payload[5:]
	var srid uint32
	if word&ewkbSRID != 0 {
		srid = binary.LittleEndian.Uint32(body)
		body = body[4:]
	}
	out := make([]byte, 0, 9+len(body))
	out = binary.LittleEndian.AppendUint32(out, srid)
	out = append(out, byteOrderLittle)
	out = binary.LittleEndian.AppendUint32(out, word&^ewkbFlags)
	return append(out, body...), nil
}

// Header returns a stored value's SRID and geometry type without parsing its body.
func Header(stored []byte) (srid uint32, typ geom.GeometryType, err error) {
	payload, err := payloadOf(stored)
	if err != nil {
		return 0, 0, err
	}
	r := reader{buf: payload, dialect: dialectEWKB}
	order, err := r.byteOrder()
	if err != nil {
		return 0, 0, err
	}
	typ, _, hasSRID, err := r.typeWord(order, true)
	if err != nil {
		return 0, 0, err
	}
	if hasSRID {
		if srid, err = r.uint32(order); err != nil {
			return 0, 0, err
		}
	}
	return srid, typ, nil
}

// encode serializes g with srid into the stored format: little-endian EWKB with the SRID
// flag set, whatever the SRID.
func encode(g geom.Geometry, srid uint32) ([]byte, error) {
	w := writer{srid: srid, buf: make([]byte, 0, 32)}
	w.buf = append(w.buf, Version1)
	if err := w.geometry(g, 1); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func payloadOf(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrInvalid
	}
	if stored[0] != Version1 {
		return nil, ErrUnsupportedVersion
	}
	return stored[1:], nil
}

// read builds the geometry b holds in dialect d, after a scan that builds nothing has
// checked it and measured the work of building it, which must be within the budget.
func read(b []byte, d dialect) (uint32, geom.Geometry, error) {
	s := scanner{r: reader{buf: b, dialect: d}, ingest: true}
	if err := s.scan(); err != nil {
		return 0, geom.Geometry{}, err
	}
	if s.work.exceeds(uint64(len(b))) {
		return 0, geom.Geometry{}, ErrTooComplex
	}
	return build(b, d)
}

// build reads and builds the geometry b holds in dialect d.
func build(b []byte, d dialect) (uint32, geom.Geometry, error) {
	r := reader{buf: b, dialect: d}
	g, err := r.geometry(1, anyType, geom.DimXY)
	if err != nil {
		return 0, geom.Geometry{}, err
	}
	if r.pos != len(r.buf) {
		return 0, geom.Geometry{}, ErrInvalid
	}
	return r.srid, g, nil
}

type reader struct {
	buf     []byte
	pos     int
	dialect dialect
	srid    uint32
	// claimed is the sum of the least bytes each count read so far needs. The elements of
	// different counts never share bytes, so a well-formed value claims at most its own
	// length. Bounding the sum, not only each count, keeps nested counts from claiming the
	// same bytes again at every level, which would let a small value allocate many times
	// its size before it fails.
	claimed uint64
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) byteOrder() (byte, error) {
	if r.remaining() < 1 {
		return 0, ErrInvalid
	}
	order := r.buf[r.pos]
	r.pos++
	switch order {
	case byteOrderLittle:
	case byteOrderBig:
		if r.dialect == dialectMySQL {
			return 0, ErrInvalid
		}
	default:
		return 0, ErrInvalid
	}
	return order, nil
}

func (r *reader) uint32(order byte) (uint32, error) {
	if r.remaining() < 4 {
		return 0, ErrInvalid
	}
	var v uint32
	if order == byteOrderLittle {
		v = binary.LittleEndian.Uint32(r.buf[r.pos:])
	} else {
		v = binary.BigEndian.Uint32(r.buf[r.pos:])
	}
	r.pos += 4
	return v, nil
}

// count reads an element count and checks that that many elements of at least
// minSize bytes each fit in the remaining input, together with the elements of every
// count read before.
func (r *reader) count(order byte, minSize int) (int, error) {
	n, err := r.uint32(order)
	if err != nil {
		return 0, err
	}
	need := uint64(n) * uint64(minSize)
	if need > uint64(r.remaining()) || r.claimed+need > uint64(len(r.buf)) {
		return 0, ErrInvalid
	}
	r.claimed += need
	return int(n), nil
}

// typeWord reads a geometry type word and returns the geometry type, its coordinate
// type and whether an EWKB SRID follows.
func (r *reader) typeWord(order byte, root bool) (geom.GeometryType, geom.CoordinatesType, bool, error) {
	word, err := r.uint32(order)
	if err != nil {
		return 0, 0, false, err
	}
	if r.dialect == dialectEWKB {
		return ewkbTypeWord(word, root)
	}
	typ, ok := geometryTypeOf(word)
	if !ok {
		return 0, 0, false, ErrInvalid
	}
	return typ, geom.DimXY, false, nil
}

// ewkbTypeWord splits an EWKB type word into the geometry type, the coordinate type and
// whether an SRID follows. Only the root may carry an SRID.
func ewkbTypeWord(word uint32, root bool) (geom.GeometryType, geom.CoordinatesType, bool, error) {
	ctype := geom.DimXY
	if word&ewkbZ != 0 {
		ctype |= geom.DimXYZ
	}
	if word&ewkbM != 0 {
		ctype |= geom.DimXYM
	}
	hasSRID := word&ewkbSRID != 0
	if hasSRID && !root {
		return 0, 0, false, ErrInvalid
	}
	typ, ok := geometryTypeOf(word &^ ewkbFlags)
	if !ok {
		return 0, 0, false, ErrInvalid
	}
	return typ, ctype, hasSRID, nil
}

func geometryTypeOf(code uint32) (geom.GeometryType, bool) {
	switch code {
	case 1:
		return geom.TypePoint, true
	case 2:
		return geom.TypeLineString, true
	case 3:
		return geom.TypePolygon, true
	case 4:
		return geom.TypeMultiPoint, true
	case 5:
		return geom.TypeMultiLineString, true
	case 6:
		return geom.TypeMultiPolygon, true
	case 7:
		return geom.TypeGeometryCollection, true
	}
	return 0, false
}

// memberTypeOf returns the type a collection's members must have.
func memberTypeOf(typ geom.GeometryType) geom.GeometryType {
	switch typ {
	case geom.TypeMultiPoint:
		return geom.TypePoint
	case geom.TypeMultiLineString:
		return geom.TypeLineString
	case geom.TypeMultiPolygon:
		return geom.TypePolygon
	}
	return anyType
}

// geometry reads one geometry. A member of a collection passes the element type its
// parent requires and the parent's coordinate type, which it must match.
func (r *reader) geometry(depth int, want geom.GeometryType, parentCType geom.CoordinatesType) (geom.Geometry, error) {
	if depth > MaxDepth {
		return geom.Geometry{}, ErrTooDeep
	}
	root := depth == 1
	order, err := r.byteOrder()
	if err != nil {
		return geom.Geometry{}, err
	}
	typ, ctype, hasSRID, err := r.typeWord(order, root)
	if err != nil {
		return geom.Geometry{}, err
	}
	if !root && (ctype != parentCType || (want != anyType && typ != want)) {
		return geom.Geometry{}, ErrInvalid
	}
	if hasSRID {
		if r.srid, err = r.uint32(order); err != nil {
			return geom.Geometry{}, err
		}
	}
	switch typ {
	case geom.TypePoint:
		p, err := r.point(order, ctype)
		return p.AsGeometry(), err
	case geom.TypeLineString:
		ls, err := r.lineString(order, ctype)
		return ls.AsGeometry(), err
	case geom.TypePolygon:
		p, err := r.polygon(order, ctype)
		return p.AsGeometry(), err
	}
	n, err := r.count(order, minGeometrySize)
	if err != nil {
		return geom.Geometry{}, err
	}
	if n == 0 && r.dialect == dialectMySQL && typ != geom.TypeGeometryCollection {
		return geom.Geometry{}, ErrInvalid
	}
	members := make([]geom.Geometry, n)
	memberType := memberTypeOf(typ)
	for i := range members {
		if members[i], err = r.geometry(depth+1, memberType, ctype); err != nil {
			return geom.Geometry{}, err
		}
	}
	return collection(typ, members, ctype), nil
}

func collection(typ geom.GeometryType, members []geom.Geometry, ctype geom.CoordinatesType) geom.Geometry {
	var g geom.Geometry
	switch typ {
	case geom.TypeMultiPoint:
		pts := make([]geom.Point, len(members))
		for i, m := range members {
			pts[i] = m.MustAsPoint()
		}
		g = geom.NewMultiPoint(pts).AsGeometry()
	case geom.TypeMultiLineString:
		lss := make([]geom.LineString, len(members))
		for i, m := range members {
			lss[i] = m.MustAsLineString()
		}
		g = geom.NewMultiLineString(lss).AsGeometry()
	case geom.TypeMultiPolygon:
		polys := make([]geom.Polygon, len(members))
		for i, m := range members {
			polys[i] = m.MustAsPolygon()
		}
		g = geom.NewMultiPolygon(polys).AsGeometry()
	default:
		g = geom.NewGeometryCollection(members).AsGeometry()
	}
	// The constructors take the coordinate type from the members, which all have ctype.
	// Only an empty collection has none to take it from; forcing it on any other would
	// copy every member again.
	if len(members) == 0 {
		g = g.ForceCoordinatesType(ctype)
	}
	return g
}

// coordinates reads n positions of ctype into a flat slice. Every coordinate must be
// finite.
func (r *reader) coordinates(order byte, ctype geom.CoordinatesType, n int) ([]float64, error) {
	floats := make([]float64, n*ctype.Dimension())
	finite, err := r.floats(order, floats)
	if err != nil {
		return nil, err
	}
	if !finite {
		return nil, ErrInvalid
	}
	return floats, nil
}

// floats reads len(dst) coordinate values into dst and reports whether all of them are
// finite. The check is made while decoding, since a second pass over the values just
// written costs more than the decoding itself on some CPUs.
func (r *reader) floats(order byte, dst []float64) (bool, error) {
	if r.remaining() < 8*len(dst) {
		return false, ErrInvalid
	}
	b := r.buf[r.pos : r.pos+8*len(dst)]
	finite := true
	if order == byteOrderLittle {
		for i := range dst {
			bits := binary.LittleEndian.Uint64(b[8*i:])
			if bits&nonFinite == nonFinite {
				finite = false
			}
			dst[i] = math.Float64frombits(bits)
		}
	} else {
		for i := range dst {
			bits := binary.BigEndian.Uint64(b[8*i:])
			if bits&nonFinite == nonFinite {
				finite = false
			}
			dst[i] = math.Float64frombits(bits)
		}
	}
	r.pos += len(b)
	return finite, nil
}

func (r *reader) point(order byte, ctype geom.CoordinatesType) (geom.Point, error) {
	var buf [4]float64
	floats := buf[:ctype.Dimension()]
	if _, err := r.floats(order, floats); err != nil {
		return geom.Point{}, err
	}
	nan := 0
	for _, f := range floats {
		if math.IsNaN(f) {
			nan++
		} else if math.IsInf(f, 0) {
			return geom.Point{}, ErrInvalid
		}
	}
	switch {
	case nan == 0:
		return geom.NewPoint(coordinatesOf(floats, ctype)), nil
	case nan == len(floats) && r.dialect == dialectEWKB:
		// EWKB writes POINT EMPTY as all-NaN coordinates.
		return geom.NewEmptyPoint(ctype), nil
	}
	return geom.Point{}, ErrInvalid
}

func coordinatesOf(floats []float64, ctype geom.CoordinatesType) geom.Coordinates {
	c := geom.Coordinates{XY: geom.XY{X: floats[0], Y: floats[1]}, Type: ctype}
	switch ctype {
	case geom.DimXYZ:
		c.Z = floats[2]
	case geom.DimXYM:
		c.M = floats[2]
	case geom.DimXYZM:
		c.Z, c.M = floats[2], floats[3]
	}
	return c
}

func (r *reader) sequence(order byte, ctype geom.CoordinatesType) (geom.Sequence, int, error) {
	n, err := r.count(order, 8*ctype.Dimension())
	if err != nil {
		return geom.Sequence{}, 0, err
	}
	floats, err := r.coordinates(order, ctype, n)
	if err != nil {
		return geom.Sequence{}, 0, err
	}
	return geom.NewSequence(floats, ctype), n, nil
}

func (r *reader) lineString(order byte, ctype geom.CoordinatesType) (geom.LineString, error) {
	seq, n, err := r.sequence(order, ctype)
	if err != nil {
		return geom.LineString{}, err
	}
	if n == 0 && r.dialect == dialectMySQL {
		return geom.LineString{}, ErrInvalid
	}
	return geom.NewLineString(seq), nil
}

func (r *reader) polygon(order byte, ctype geom.CoordinatesType) (geom.Polygon, error) {
	n, err := r.count(order, 4)
	if err != nil {
		return geom.Polygon{}, err
	}
	if n == 0 {
		if r.dialect == dialectMySQL {
			return geom.Polygon{}, ErrInvalid
		}
		return geom.Polygon{}.ForceCoordinatesType(ctype), nil
	}
	rings := make([]geom.LineString, n)
	for i := range rings {
		seq, points, err := r.sequence(order, ctype)
		if err != nil {
			return geom.Polygon{}, err
		}
		if points == 0 {
			return geom.Polygon{}, ErrInvalid
		}
		rings[i] = geom.NewLineString(seq)
	}
	return geom.NewPolygon(rings), nil
}

// scanner walks a payload with the reader's primitives and makes the reader's checks in
// the reader's order, but builds no geometry, so it allocates nothing. It accepts exactly
// the payloads the reader accepts in the same dialect, except that StoredToMySQL's scan
// rejects a big-endian geometry, and it records what its caller needs to know about the
// value.
type scanner struct {
	r reader
	// ingest is set for a value given for storage or to be built: a big-endian geometry
	// is accepted. Without it, as for StoredToMySQL, which copies the body unchanged into
	// MySQL's little-endian format, a big-endian geometry is ErrInvalid: stored values
	// are written little-endian, so it was not written here.
	ingest bool

	// notMySQL is set by Z or M coordinates, or by an empty geometry other than a
	// GEOMETRYCOLLECTION. It is reported only once the whole value is known to be
	// well-formed, so a malformed value reports its structural error whatever its
	// coordinates.
	notMySQL bool
	// geometries counts the geometry headers scanned so far.
	geometries uint64
	// work is the work of building the value.
	work decodeWork
	// sum is the summary.
	sum Summary
}

// scan scans the whole payload: one geometry, and nothing after it.
func (s *scanner) scan() error {
	if err := s.geometry(1, anyType, geom.DimXY); err != nil {
		return err
	}
	if s.r.pos != len(s.r.buf) {
		return ErrInvalid
	}
	return nil
}

// geometry scans one geometry, as reader.geometry reads one.
func (s *scanner) geometry(depth int, want geom.GeometryType, parentCType geom.CoordinatesType) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	root := depth == 1
	s.geometries++
	order, err := s.r.byteOrder()
	if err != nil {
		return err
	}
	if order != byteOrderLittle && !s.ingest {
		return ErrInvalid
	}
	typ, ctype, hasSRID, err := s.r.typeWord(order, root)
	if err != nil {
		return err
	}
	if !root && (ctype != parentCType || (want != anyType && typ != want)) {
		return ErrInvalid
	}
	if root {
		s.sum.Type = typ
	}
	if ctype != geom.DimXY {
		s.notMySQL = true
	}
	if hasSRID {
		if _, err = s.r.uint32(order); err != nil {
			return err
		}
	}
	switch typ {
	case geom.TypePoint:
		return s.point(order, ctype)
	case geom.TypeLineString:
		return s.lineString(order, ctype)
	case geom.TypePolygon:
		return s.polygon(order, ctype)
	}
	n, err := s.r.count(order, minGeometrySize)
	if err != nil {
		return err
	}
	if n == 0 && typ != geom.TypeGeometryCollection {
		if s.r.dialect == dialectMySQL {
			return ErrInvalid
		}
		s.notMySQL = true
	}
	memberType := memberTypeOf(typ)
	start, geometries := s.r.pos, s.geometries
	for range n {
		if err := s.geometry(depth+1, memberType, ctype); err != nil {
			return err
		}
	}
	s.work.addCollection(uint64(s.r.pos-start), s.geometries-geometries)
	return nil
}

// point checks a point's coordinates as reader.point does: all finite, or, in EWKB, all
// NaN, which is how EWKB writes POINT EMPTY.
func (s *scanner) point(order byte, ctype geom.CoordinatesType) error {
	dim := ctype.Dimension()
	if s.r.remaining() < 8*dim {
		return ErrInvalid
	}
	coords := s.coordinates(8 * dim)
	if finite(coords, order) {
		return nil
	}
	if s.r.dialect != dialectEWKB {
		return ErrInvalid
	}
	for i := 0; i+8 <= len(coords); i += 8 {
		if !math.IsNaN(math.Float64frombits(bitsAt(coords[i:], order))) {
			return ErrInvalid
		}
	}
	s.notMySQL = true
	return nil
}

// sequence checks a coordinate sequence as reader.sequence does and returns its length.
func (s *scanner) sequence(order byte, ctype geom.CoordinatesType) (int, error) {
	dim := ctype.Dimension()
	n, err := s.r.count(order, 8*dim)
	if err != nil {
		return 0, err
	}
	// count has checked that the coordinates are there.
	coords := s.coordinates(8 * dim * n)
	if !finite(coords, order) {
		return 0, ErrInvalid
	}
	return n, nil
}

// coordinates returns the next n bytes, which the caller has checked are there, and moves
// past them.
func (s *scanner) coordinates(n int) []byte {
	coords := s.r.buf[s.r.pos : s.r.pos+n]
	s.r.pos += n
	return coords
}

// nonFinite is the exponent of a NaN or an infinity, which has every bit set.
const nonFinite uint64 = 0x7FF0000000000000

// finite reports whether every coordinate in coords, in byte order order, is finite.
func finite(coords []byte, order byte) bool {
	if order == byteOrderLittle {
		for i := 0; i+8 <= len(coords); i += 8 {
			if binary.LittleEndian.Uint64(coords[i:])&nonFinite == nonFinite {
				return false
			}
		}
		return true
	}
	for i := 0; i+8 <= len(coords); i += 8 {
		if binary.BigEndian.Uint64(coords[i:])&nonFinite == nonFinite {
			return false
		}
	}
	return true
}

func bitsAt(b []byte, order byte) uint64 {
	if order == byteOrderLittle {
		return binary.LittleEndian.Uint64(b)
	}
	return binary.BigEndian.Uint64(b)
}

// lineString checks a linestring as reader.lineString does.
func (s *scanner) lineString(order byte, ctype geom.CoordinatesType) error {
	n, err := s.sequence(order, ctype)
	if err != nil {
		return err
	}
	if n == 0 {
		if s.r.dialect == dialectMySQL {
			return ErrInvalid
		}
		s.notMySQL = true
	}
	return nil
}

// polygon checks a polygon as reader.polygon does.
func (s *scanner) polygon(order byte, ctype geom.CoordinatesType) error {
	n, err := s.r.count(order, 4)
	if err != nil {
		return err
	}
	if n == 0 {
		if s.r.dialect == dialectMySQL {
			return ErrInvalid
		}
		s.notMySQL = true
		return nil
	}
	for range n {
		points, err := s.sequence(order, ctype)
		if err != nil {
			return err
		}
		if points == 0 {
			return ErrInvalid
		}
	}
	return nil
}

// writer writes the stored-format payload: little-endian EWKB with the SRID flag on the
// root.
type writer struct {
	buf []byte
	// srid is written into the root's EWKB header.
	srid uint32
}

func (w *writer) uint32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

func (w *writer) float64(v float64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(v))
}

func (w *writer) header(typ geom.GeometryType, ctype geom.CoordinatesType, root bool) error {
	code, ok := typeCodeOf(typ)
	if !ok {
		return ErrInvalid
	}
	if ctype.Is3D() {
		code |= ewkbZ
	}
	if ctype.IsMeasured() {
		code |= ewkbM
	}
	if root {
		code |= ewkbSRID
	}
	w.buf = append(w.buf, byteOrderLittle)
	w.uint32(code)
	if root {
		w.uint32(w.srid)
	}
	return nil
}

func typeCodeOf(typ geom.GeometryType) (uint32, bool) {
	switch typ {
	case geom.TypePoint:
		return 1, true
	case geom.TypeLineString:
		return 2, true
	case geom.TypePolygon:
		return 3, true
	case geom.TypeMultiPoint:
		return 4, true
	case geom.TypeMultiLineString:
		return 5, true
	case geom.TypeMultiPolygon:
		return 6, true
	case geom.TypeGeometryCollection:
		return 7, true
	}
	return 0, false
}

func (w *writer) coordinates(c geom.Coordinates, ctype geom.CoordinatesType) {
	w.float64(c.X)
	w.float64(c.Y)
	if ctype.Is3D() {
		w.float64(c.Z)
	}
	if ctype.IsMeasured() {
		w.float64(c.M)
	}
}

func (w *writer) sequence(seq geom.Sequence, ctype geom.CoordinatesType) {
	n := seq.Length()
	w.uint32(uint32(n))
	for i := range n {
		w.coordinates(seq.Get(i), ctype)
	}
}

func (w *writer) geometry(g geom.Geometry, depth int) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	ctype := g.CoordinatesType()
	typ := g.Type()
	if err := w.header(typ, ctype, depth == 1); err != nil {
		return err
	}
	switch typ {
	case geom.TypePoint:
		c, ok := g.MustAsPoint().Coordinates()
		if !ok {
			nan := math.Float64frombits(emptyPointNaN)
			c = geom.Coordinates{XY: geom.XY{X: nan, Y: nan}, Z: nan, M: nan, Type: ctype}
		}
		w.coordinates(c, ctype)
	case geom.TypeLineString:
		w.sequence(g.MustAsLineString().Coordinates(), ctype)
	case geom.TypePolygon:
		rings := g.MustAsPolygon().Coordinates()
		w.uint32(uint32(len(rings)))
		for _, ring := range rings {
			w.sequence(ring, ctype)
		}
	case geom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		n := mp.NumPoints()
		w.uint32(uint32(n))
		for i := range n {
			if err := w.geometry(mp.PointN(i).AsGeometry(), depth+1); err != nil {
				return err
			}
		}
	case geom.TypeMultiLineString:
		mls := g.MustAsMultiLineString()
		n := mls.NumLineStrings()
		w.uint32(uint32(n))
		for i := range n {
			if err := w.geometry(mls.LineStringN(i).AsGeometry(), depth+1); err != nil {
				return err
			}
		}
	case geom.TypeMultiPolygon:
		mp := g.MustAsMultiPolygon()
		n := mp.NumPolygons()
		w.uint32(uint32(n))
		for i := range n {
			if err := w.geometry(mp.PolygonN(i).AsGeometry(), depth+1); err != nil {
				return err
			}
		}
	case geom.TypeGeometryCollection:
		gc := g.MustAsGeometryCollection()
		n := gc.NumGeometries()
		w.uint32(uint32(n))
		for i := range n {
			if err := w.geometry(gc.GeometryN(i), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
