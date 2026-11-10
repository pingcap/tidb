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
//   - The stored format, which a geometry column holds: a version byte, then little-endian
//     EWKB (WKB with Z, M and SRID flags) with the SRID flag always set.
//   - MySQL's binary format, which a bare SELECT returns: a 4-byte little-endian SRID, then
//     2D WKB.
//
// Both hold the same coordinates in the same order below the root header, so a conversion
// checks the value with a scan that builds nothing and rewrites only the header. Every
// read and scan bounds nesting at MaxDepth, consumes the input exactly and checks counts
// before allocating.
package geocodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/pingcap/tidb/pkg/util/size"
)

const (
	// Version1 is the stored-format version whose payload is EWKB.
	Version1 byte = 1
	// MaxDepth bounds nesting, counted in geometry headers from the root (a POINT is 1), so
	// hostile input fails instead of overflowing the goroutine stack.
	MaxDepth = 1024
)

var (
	// ErrInvalid means the bytes are not a well-formed geometry in the expected format.
	ErrInvalid = errors.New("invalid geometry data")
	// ErrTooDeep means the geometry nests deeper than MaxDepth.
	ErrTooDeep = fmt.Errorf("geometry nested deeper than %d levels", MaxDepth)
	// ErrUnsupportedVersion means a stored value starts with an unknown version byte.
	ErrUnsupportedVersion = errors.New("unsupported geometry storage format version")
	// ErrNotMySQLExpressible means the geometry has Z or M coordinates, or an empty geometry
	// other than a GEOMETRYCOLLECTION, which MySQL's binary format cannot hold.
	ErrNotMySQLExpressible = errors.New("geometry cannot be expressed in MySQL's binary format")
	// ErrTooComplex means building the geometry would exceed the decode budget; it can still
	// be stored and converted.
	ErrTooComplex = errors.New("geometry nests too much of its data too deep to be built")

	// errBigEndianStored is the ErrInvalid of a big-endian geometry in a stored value, which
	// StoredToMySQL cannot copy into MySQL's little-endian format.
	errBigEndianStored = fmt.Errorf("%w: big-endian geometry in a stored value", ErrInvalid)
)

// The decode budget. Building a collection copies everything below it, so the work is the
// sum over collections of their members' bytes and decodeWorkPerGeometry per geometry.
const (
	// decodeWorkFactor is the work allowed per byte of the value.
	decodeWorkFactor = 16
	// decodeWorkFloor is the work always allowed, so short deep values such as 1,024 empty
	// collections (about 10.5 MB of work) are still built.
	decodeWorkFloor = 16 * size.MB
	// decodeWorkPerGeometry is the fixed cost of copying one geometry: about 48 bytes
	// allocated, at about 4.6 bytes per unit of work (measured), rounded up.
	decodeWorkPerGeometry = 11
)

// decodeWork is the work of building a geometry, counted by the scan collection by
// collection.
type decodeWork uint64

// addCollection adds the work of building a collection: its members' bytes, and the fixed
// cost of every geometry below it.
func (w *decodeWork) addCollection(memberBytes, geometries uint64) {
	*w += decodeWork(memberBytes + decodeWorkPerGeometry*geometries)
}

// exceeds reports whether the work passes max(decodeWorkFactor x n, decodeWorkFloor).
func (w decodeWork) exceeds(n uint64) bool {
	return uint64(w) > max(decodeWorkFactor*n, decodeWorkFloor)
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

// MySQLToStored converts MySQL's binary format into the stored format. It checks the WKB
// framing, not MySQL's structure rules such as a linestring's minimum of two points.
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

// StoredToMySQL converts a stored value into MySQL's binary format, rewriting only the
// header; a value MySQL's format cannot hold gives ErrNotMySQLExpressible.
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
	if s.work.exceeds(s.budgetSize()) {
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
	// claimed sums the least bytes each count read so far needs, so nested counts cannot
	// claim the same bytes again and allocate many times the value's size.
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

// count reads an element count and checks that its elements, at least minSize bytes each,
// fit in the input together with every earlier count's.
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
	// The constructors take the coordinate type from the members; only an empty collection
	// needs it set.
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

// floats reads len(dst) values into dst and reports whether all are finite, checked while
// decoding since a second pass costs more.
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

// scanner checks a payload as the reader does, in the same order, but builds nothing and
// allocates nothing.
type scanner struct {
	r reader
	// ingest accepts big-endian geometries; StoredToMySQL, which copies the body into
	// MySQL's little-endian format, scans without it.
	ingest bool

	// notMySQL records Z or M coordinates or an empty non-collection, reported only once the
	// value is known to be well-formed.
	notMySQL bool
	// sridBytes is 4 when the root carries an EWKB SRID.
	sridBytes int
	// geometries counts the geometry headers scanned so far.
	geometries uint64
	// work is the work of building the value.
	work decodeWork
	// sum is the summary.
	sum Summary
}

// budgetSize is the length the decode budget scales with: the WKB without an EWKB SRID, so
// a value gets the same budget in either format.
func (s *scanner) budgetSize() uint64 { return uint64(len(s.r.buf) - s.sridBytes) }

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
		return errBigEndianStored
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
		s.sridBytes = 4
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

// coordinates writes one position; a non-finite value is ErrInvalid, as the reader would
// refuse it.
func (w *writer) coordinates(c geom.Coordinates, ctype geom.CoordinatesType) error {
	if !isFinite(c.X) || !isFinite(c.Y) || ctype.Is3D() && !isFinite(c.Z) ||
		ctype.IsMeasured() && !isFinite(c.M) {
		return ErrInvalid
	}
	w.float64(c.X)
	w.float64(c.Y)
	if ctype.Is3D() {
		w.float64(c.Z)
	}
	if ctype.IsMeasured() {
		w.float64(c.M)
	}
	return nil
}

func isFinite(f float64) bool { return math.Float64bits(f)&nonFinite != nonFinite }

// emptyPoint writes POINT EMPTY as EWKB does, every coordinate the quiet NaN.
func (w *writer) emptyPoint(ctype geom.CoordinatesType) {
	for range ctype.Dimension() {
		w.buf = binary.LittleEndian.AppendUint64(w.buf, emptyPointNaN)
	}
}

func (w *writer) sequence(seq geom.Sequence, ctype geom.CoordinatesType) error {
	n := seq.Length()
	w.uint32(uint32(n))
	for i := range n {
		if err := w.coordinates(seq.Get(i), ctype); err != nil {
			return err
		}
	}
	return nil
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
			w.emptyPoint(ctype)
			return nil
		}
		return w.coordinates(c, ctype)
	case geom.TypeLineString:
		return w.sequence(g.MustAsLineString().Coordinates(), ctype)
	case geom.TypePolygon:
		rings := g.MustAsPolygon().Coordinates()
		w.uint32(uint32(len(rings)))
		for _, ring := range rings {
			// The reader refuses a ring without points.
			if ring.Length() == 0 {
				return ErrInvalid
			}
			if err := w.sequence(ring, ctype); err != nil {
				return err
			}
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
