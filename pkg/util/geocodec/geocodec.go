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
// rewrites only the header. Bytes from a client are checked against every rule of the
// format once, by validate; a stored value, checked when it was written, is only walked.
// Every walk bounds nesting at MaxDepth, consumes the input exactly and checks counts
// against the input before allocating.
package geocodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
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

	// errBigEndianStored is the ErrInvalid of a big-endian geometry in a stored value, which
	// every writer writes little-endian.
	errBigEndianStored = fmt.Errorf("%w: big-endian geometry in a stored value", ErrInvalid)
	// errStoredWithoutSRID is the ErrInvalid of a stored value whose root has no SRID flag.
	errStoredWithoutSRID = fmt.Errorf("%w: stored geometry without the SRID flag", ErrInvalid)
)

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
	anyType Type = 0

	// nonFinite is the exponent of a NaN or an infinity, which has every bit set.
	nonFinite uint64 = 0x7FF0000000000000
	// emptyPointNaN is the quiet NaN PostGIS and GEOS write for each coordinate of POINT
	// EMPTY, the canonical form; Go's math.NaN() has another bit pattern.
	emptyPointNaN uint64 = 0x7FF8000000000000
)

// Type is a geometry type, numbered by its WKB type code, as the parser numbers a geometry
// column's subtype, where 0 is any type.
type Type uint32

// The geometry types.
const (
	TypePoint              Type = 1
	TypeLineString         Type = 2
	TypePolygon            Type = 3
	TypeMultiPoint         Type = 4
	TypeMultiLineString    Type = 5
	TypeMultiPolygon       Type = 6
	TypeGeometryCollection Type = 7
)

// dims says which coordinates a position has: Z adds 1 and M adds 2.
type dims byte

const (
	dimsXY   dims = 0b00
	dimsXYZ  dims = 0b01
	dimsXYM  dims = 0b10
	dimsXYZM dims = 0b11
)

// size is the number of bytes in a position.
func (d dims) size() int {
	switch d {
	case dimsXY:
		return 16
	case dimsXYZM:
		return 32
	}
	return 24
}

// dialect selects which WKB variant validate accepts.
type dialect int

const (
	// dialectEWKB is the stored-format payload: either byte order, EWKB flags for Z, M
	// and a root SRID.
	dialectEWKB dialect = iota
	// dialectMySQL is the WKB inside MySQL's binary format: little-endian, 2D, and no
	// empty geometry except a GEOMETRYCOLLECTION.
	dialectMySQL
)

// Envelope bounds the X and Y of every position of a geometry, interior rings included.
type Envelope struct {
	MinX, MinY, MaxX, MaxY float64
	// NotEmpty is false for a geometry without positions, which has no bounds.
	NotEmpty bool
}

// Summary is what BareToStored learns about a value while checking it.
type Summary struct {
	// Type is the geometry type of the root.
	Type Type
	// Envelope bounds the value's positions.
	Envelope Envelope
}

// MySQLToStored converts MySQL's binary format into the stored format. It checks the WKB
// framing, not MySQL's structure rules such as a linestring's minimum of two points.
func MySQLToStored(b []byte) (stored []byte, srid uint32, err error) {
	if len(b) < 4 {
		return nil, 0, ErrInvalid
	}
	if _, err := validate(b[4:], dialectMySQL); err != nil {
		return nil, 0, err
	}
	stored, srid = storedFromValidMySQL(b)
	return stored, srid, nil
}

// storedFromValidMySQL rewrites the header of b, MySQL's binary format that validate has
// accepted, so with a little-endian root and a type code without flags.
func storedFromValidMySQL(b []byte) (stored []byte, srid uint32) {
	srid = binary.LittleEndian.Uint32(b)
	stored = make([]byte, 0, len(b)+1)
	stored = append(stored, Version1, byteOrderLittle)
	stored = binary.LittleEndian.AppendUint32(stored, binary.LittleEndian.Uint32(b[5:])|ewkbSRID)
	stored = binary.LittleEndian.AppendUint32(stored, srid)
	return append(stored, b[9:]...), srid
}

// IsStored reports whether b is in the stored format rather than MySQL's: its sixth byte
// has the EWKB SRID flag 0x20, which a MySQL WKB type byte (0 to 7) never has.
func IsStored(b []byte) bool {
	return len(b) > 5 && b[5]&byte(ewkbSRID>>24) != 0
}

// BareToStored converts a value given for a geometry column, in MySQL's format or the
// stored format, into the canonical stored format, with MySQL's structure checks.
func BareToStored(b []byte) (stored []byte, srid uint32, sum Summary, err error) {
	if !IsStored(b) {
		if len(b) < 4 {
			return nil, 0, Summary{}, ErrInvalid
		}
		v := validator{c: cursor{buf: b[4:]}, dialect: dialectMySQL, bare: true}
		if err := v.run(); err != nil {
			return nil, 0, Summary{}, err
		}
		stored, srid = storedFromValidMySQL(b)
		return stored, srid, v.sum, nil
	}
	payload, err := payloadOf(b)
	if err != nil {
		return nil, 0, Summary{}, err
	}
	v := validator{c: cursor{buf: payload}, dialect: dialectEWKB, bare: true}
	if err := v.run(); err != nil {
		return nil, 0, Summary{}, err
	}
	stored = append(make([]byte, 0, len(b)), b...)
	if v.rewrite {
		fix := validator{c: cursor{buf: payload}, dialect: dialectEWKB, fix: stored[1:]}
		if err := fix.run(); err != nil {
			return nil, 0, Summary{}, err
		}
	}
	return stored, v.srid, v.sum, nil
}

// StoredToMySQL converts a stored value into MySQL's binary format, rewriting only the
// header; a value MySQL's format cannot hold gives ErrNotMySQLExpressible.
func StoredToMySQL(stored []byte) ([]byte, error) {
	payload, err := payloadOf(stored)
	if err != nil {
		return nil, err
	}
	notMySQL, err := skim(payload)
	if err != nil {
		return nil, err
	}
	if notMySQL {
		return nil, ErrNotMySQLExpressible
	}
	// payloadOf and skim have checked the root header: a little-endian byte order, a type
	// word with the SRID flag, and the SRID.
	word := binary.LittleEndian.Uint32(payload[1:])
	srid := binary.LittleEndian.Uint32(payload[5:])
	body := payload[9:]
	out := make([]byte, 0, 9+len(body))
	out = binary.LittleEndian.AppendUint32(out, srid)
	out = append(out, byteOrderLittle)
	out = binary.LittleEndian.AppendUint32(out, word&^ewkbFlags)
	return append(out, body...), nil
}

// Header returns a stored value's SRID and geometry type without parsing its body.
func Header(stored []byte) (srid uint32, typ Type, err error) {
	payload, err := payloadOf(stored)
	if err != nil {
		return 0, 0, err
	}
	c := cursor{buf: payload}
	// payloadOf has checked that the root is not big-endian and has the SRID flag.
	if _, err := c.byteOrder(); err != nil {
		return 0, 0, err
	}
	word, err := c.uint32(byteOrderLittle)
	if err != nil {
		return 0, 0, err
	}
	typ, _, _, ok := splitType(word)
	if !ok {
		return 0, 0, ErrInvalid
	}
	if srid, err = c.uint32(byteOrderLittle); err != nil {
		return 0, 0, err
	}
	return srid, typ, nil
}

// payloadOf returns a stored value's EWKB, whose root must be little-endian and carry the
// SRID flag, as every writer writes it. A root too short or of an unknown byte order is
// left to the walk to report.
func payloadOf(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrInvalid
	}
	if stored[0] != Version1 {
		return nil, ErrUnsupportedVersion
	}
	payload := stored[1:]
	if len(payload) > 0 && payload[0] == byteOrderBig {
		return nil, errBigEndianStored
	}
	if len(payload) >= 5 && payload[0] == byteOrderLittle && binary.LittleEndian.Uint32(payload[1:])&ewkbSRID == 0 {
		return nil, errStoredWithoutSRID
	}
	return payload, nil
}

// cursor reads WKB from buf at pos. claimed sums the bytes every count read so far needs,
// so nested counts cannot claim the same bytes again and allocate many times the input.
type cursor struct {
	buf     []byte
	pos     int
	claimed uint64
}

// next returns the next n bytes and moves past them.
func (c *cursor) next(n int) ([]byte, error) {
	if n > len(c.buf)-c.pos {
		return nil, ErrInvalid
	}
	b := c.buf[c.pos : c.pos+n]
	c.pos += n
	return b, nil
}

func (c *cursor) byteOrder() (byte, error) {
	b, err := c.next(1)
	if err != nil {
		return 0, err
	}
	if b[0] != byteOrderLittle && b[0] != byteOrderBig {
		return 0, ErrInvalid
	}
	return b[0], nil
}

func (c *cursor) uint32(order byte) (uint32, error) {
	b, err := c.next(4)
	if err != nil {
		return 0, err
	}
	if order == byteOrderLittle {
		return binary.LittleEndian.Uint32(b), nil
	}
	return binary.BigEndian.Uint32(b), nil
}

// count reads an element count and checks that its elements, at least minSize bytes each,
// fit in the input together with every earlier count's.
func (c *cursor) count(order byte, minSize int) (int, error) {
	n, err := c.uint32(order)
	if err != nil {
		return 0, err
	}
	need := uint64(n) * uint64(minSize)
	if need > uint64(len(c.buf)-c.pos) || c.claimed+need > uint64(len(c.buf)) {
		return 0, ErrInvalid
	}
	c.claimed += need
	return int(n), nil
}

func bitsAt(b []byte, order byte) uint64 {
	if order == byteOrderLittle {
		return binary.LittleEndian.Uint64(b)
	}
	return binary.BigEndian.Uint64(b)
}

// splitType splits an EWKB type word into the geometry type, the coordinates of its
// positions, and whether an SRID follows; ok is false for an unknown type code.
func splitType(word uint32) (typ Type, d dims, hasSRID, ok bool) {
	typ = Type(word &^ ewkbFlags)
	if word&ewkbZ != 0 {
		d |= dimsXYZ
	}
	if word&ewkbM != 0 {
		d |= dimsXYM
	}
	return typ, d, word&ewkbSRID != 0, typ >= TypePoint && typ <= TypeGeometryCollection
}

// memberTypeOf returns the type a collection's members must have.
func memberTypeOf(typ Type) Type {
	switch typ {
	case TypeMultiPoint:
		return TypePoint
	case TypeMultiLineString:
		return TypeLineString
	case TypeMultiPolygon:
		return TypePolygon
	}
	return anyType
}

// validate checks b, WKB in dialect d from an untrusted source, against every rule of the
// format, building nothing and allocating nothing. It reports whether MySQL's format can
// hold the value.
func validate(b []byte, d dialect) (notMySQL bool, err error) {
	v := validator{c: cursor{buf: b}, dialect: d}
	if err := v.run(); err != nil {
		return false, err
	}
	return v.notMySQL, nil
}

type validator struct {
	c       cursor
	dialect dialect
	// bare adds the bare path's rules, at least 2 points per linestring and 4 per ring, and
	// fills sum.
	bare bool
	// fix, if set, is a copy of c.buf to make canonical in place: little-endian, and POINT
	// EMPTY as emptyPointNaN.
	fix []byte

	// notMySQL records Z or M coordinates or an empty non-collection, reported only once the
	// value is known to be well-formed.
	notMySQL bool
	// broken marks a structure bare rejects, reported once the whole value is read.
	broken bool
	// rewrite marks a value not in canonical form.
	rewrite bool
	// srid is the root's SRID, if it has one.
	srid uint32
	sum  Summary
}

// run checks the whole input: one geometry, and nothing after it.
func (v *validator) run() error {
	if err := v.geometry(1, anyType, dimsXY); err != nil {
		return err
	}
	if v.c.pos != len(v.c.buf) || v.broken {
		return ErrInvalid
	}
	return nil
}

// little makes the n-byte field just read little-endian in fix.
func (v *validator) little(order byte, n int) {
	if v.fix != nil && order == byteOrderBig {
		slices.Reverse(v.fix[v.c.pos-n : v.c.pos])
	}
}

// geometry checks one geometry. A member of a collection passes the type its parent
// requires and the parent's coordinates, which it must match.
func (v *validator) geometry(depth int, want Type, parentDims dims) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	root := depth == 1
	order, err := v.c.byteOrder()
	if err != nil {
		return err
	}
	if order == byteOrderBig {
		if v.dialect == dialectMySQL {
			return ErrInvalid
		}
		v.rewrite = true
		if v.fix != nil {
			v.fix[v.c.pos-1] = byteOrderLittle
		}
	}
	word, err := v.c.uint32(order)
	if err != nil {
		return err
	}
	v.little(order, 4)
	typ, d, hasSRID, ok := splitType(word)
	if !ok || hasSRID && !root || v.dialect == dialectMySQL && word&ewkbFlags != 0 {
		return ErrInvalid
	}
	if !root && (d != parentDims || want != anyType && typ != want) {
		return ErrInvalid
	}
	if d != dimsXY {
		v.notMySQL = true
	}
	if root {
		v.sum.Type = typ
	}
	if hasSRID {
		if v.srid, err = v.c.uint32(order); err != nil {
			return err
		}
		v.little(order, 4)
	}
	size := d.size()
	switch typ {
	case TypePoint:
		return v.point(order, size)
	case TypeLineString:
		n, err := v.sequence(order, size)
		if err != nil {
			return err
		}
		if n == 0 {
			return v.empty()
		}
		if n == 1 && v.bare {
			v.broken = true
		}
		return nil
	case TypePolygon:
		return v.polygon(order, size)
	}
	n, err := v.c.count(order, minGeometrySize)
	if err != nil {
		return err
	}
	v.little(order, 4)
	if n == 0 && typ != TypeGeometryCollection {
		if err := v.empty(); err != nil {
			return err
		}
	}
	for range n {
		if err := v.geometry(depth+1, memberTypeOf(typ), d); err != nil {
			return err
		}
	}
	return nil
}

// empty records an empty geometry other than a GEOMETRYCOLLECTION, which MySQL's format
// cannot hold and so refuses.
func (v *validator) empty() error {
	if v.dialect == dialectMySQL {
		return ErrInvalid
	}
	v.notMySQL = true
	return nil
}

// point checks a point's coordinates: all finite, or all NaN. A point has no count to
// write as 0, as the other empty geometries do, so EWKB writes POINT EMPTY as NaN
// coordinates, as PostGIS and GEOS do. MySQL's format has no POINT EMPTY.
func (v *validator) point(order byte, size int) error {
	coords, err := v.c.next(size)
	if err != nil {
		return err
	}
	if finite(coords, order) {
		v.positions(coords, order, size)
		return nil
	}
	for i := 0; i < len(coords); i += 8 {
		bits := bitsAt(coords[i:], order)
		if !math.IsNaN(math.Float64frombits(bits)) {
			return ErrInvalid
		}
		v.rewrite = v.rewrite || bits != emptyPointNaN
	}
	if v.fix != nil {
		for i := v.c.pos - size; i < v.c.pos; i += 8 {
			binary.LittleEndian.PutUint64(v.fix[i:], emptyPointNaN)
		}
	}
	return v.empty()
}

// positions takes in finite positions of size bytes just read: bare widens the envelope
// to them, and fix gets them little-endian.
func (v *validator) positions(coords []byte, order byte, size int) {
	if v.bare {
		env := v.sum.Envelope
		for i := 0; i < len(coords); i += size {
			x := math.Float64frombits(bitsAt(coords[i:], order))
			y := math.Float64frombits(bitsAt(coords[i+8:], order))
			if !env.NotEmpty {
				env = Envelope{MinX: x, MinY: y, MaxX: x, MaxY: y, NotEmpty: true}
				continue
			}
			env.MinX, env.MinY = min(env.MinX, x), min(env.MinY, y)
			env.MaxX, env.MaxY = max(env.MaxX, x), max(env.MaxY, y)
		}
		v.sum.Envelope = env
	}
	if v.fix != nil && order == byteOrderBig {
		for i := v.c.pos - len(coords); i < v.c.pos; i += 8 {
			slices.Reverse(v.fix[i : i+8])
		}
	}
}

// sequence checks a count of positions of size bytes and their coordinates, which must be
// finite, and returns the count.
func (v *validator) sequence(order byte, size int) (int, error) {
	n, err := v.c.count(order, size)
	if err != nil {
		return 0, err
	}
	v.little(order, 4)
	coords, err := v.c.next(n * size)
	if err != nil {
		return 0, err
	}
	if !finite(coords, order) {
		return 0, ErrInvalid
	}
	v.positions(coords, order, size)
	return n, nil
}

// polygon checks a polygon: rings of at least one point each.
func (v *validator) polygon(order byte, size int) error {
	n, err := v.c.count(order, 4)
	if err != nil {
		return err
	}
	v.little(order, 4)
	if n == 0 {
		return v.empty()
	}
	for range n {
		points, err := v.sequence(order, size)
		if err != nil {
			return err
		}
		if points == 0 {
			return ErrInvalid
		}
		if points < 4 && v.bare {
			v.broken = true
		}
	}
	return nil
}

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

// skim walks a stored payload, validated when it was written, and reports whether MySQL's
// format can hold it. It reads only headers and counts, and a point's first coordinate,
// whose NaN marks POINT EMPTY; it checks only what walking and copying need.
func skim(payload []byte) (notMySQL bool, err error) {
	s := skimmer{c: cursor{buf: payload}}
	if err := s.geometry(1); err != nil {
		return false, err
	}
	if s.c.pos != len(payload) {
		return false, ErrInvalid
	}
	return s.notMySQL, nil
}

type skimmer struct {
	c        cursor
	notMySQL bool
}

func (s *skimmer) geometry(depth int) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	order, err := s.c.byteOrder()
	if err != nil {
		return err
	}
	if order != byteOrderLittle {
		return errBigEndianStored
	}
	word, err := s.c.uint32(byteOrderLittle)
	if err != nil {
		return err
	}
	typ, d, hasSRID, ok := splitType(word)
	if !ok {
		return ErrInvalid
	}
	// MySQL's format holds no Z, M or SRID below the root, so the body is copied only
	// without them.
	if d != dimsXY || hasSRID && depth > 1 {
		s.notMySQL = true
	}
	if hasSRID {
		if _, err := s.c.next(4); err != nil {
			return err
		}
	}
	size := d.size()
	switch typ {
	case TypePoint:
		coords, err := s.c.next(size)
		if err != nil {
			return err
		}
		if math.IsNaN(math.Float64frombits(binary.LittleEndian.Uint64(coords))) {
			s.notMySQL = true
		}
		return nil
	case TypeLineString:
		n, err := s.sequence(size)
		if n == 0 {
			s.notMySQL = true
		}
		return err
	case TypePolygon:
		n, err := s.c.count(byteOrderLittle, 4)
		if err != nil {
			return err
		}
		if n == 0 {
			s.notMySQL = true
		}
		for range n {
			if _, err := s.sequence(size); err != nil {
				return err
			}
		}
		return nil
	}
	n, err := s.c.count(byteOrderLittle, minGeometrySize)
	if err != nil {
		return err
	}
	if n == 0 && typ != TypeGeometryCollection {
		s.notMySQL = true
	}
	for range n {
		if err := s.geometry(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

// sequence skips a count of positions of size bytes each and returns the count.
func (s *skimmer) sequence(size int) (int, error) {
	n, err := s.c.count(byteOrderLittle, size)
	if err != nil {
		return 0, err
	}
	_, err = s.c.next(n * size)
	return n, err
}
