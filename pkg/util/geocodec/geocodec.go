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

// Package geocodec defines the byte forms of a geometry value.
//
//   - The bare value: a 4-byte little-endian SRID, then little-endian EWKB without the SRID
//     flag. A 2D value whose only empty geometry is a GEOMETRYCOLLECTION is thus MySQL's
//     binary format; other values extend it with EWKB's Z and M flags.
//   - The stored format: a version byte, then the bare value.
//
// Both hold the seven OGC geometry types, type codes 1 to 7.
package geocodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// Version1 is the first stored-format version.
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
)

const (
	ewkbZ     uint32 = 0x80000000
	ewkbM     uint32 = 0x40000000
	ewkbSRID  uint32 = 0x20000000
	ewkbFlags        = ewkbZ | ewkbM | ewkbSRID

	byteOrderLittle byte = 1

	sridSize = 4
	// minGeometrySize is the smallest possible WKB geometry: a byte-order byte, a type
	// word and a zero count.
	minGeometrySize = 9

	// anyType lets a GEOMETRYCOLLECTION member be of any type.
	anyType Type = 0

	// nonFinite is the exponent of a NaN or an infinity, which has every bit set.
	nonFinite uint64 = 0x7FF0000000000000
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

func (d dims) size() int {
	switch d {
	case dimsXY:
		return 16
	case dimsXYZM:
		return 32
	}
	return 24
}

// CheckBare checks a bare value from a client against every rule of the format, without
// allocating. MySQL's structure rules, such as a linestring's 2+ points, are not checked.
func CheckBare(b []byte) error {
	if len(b) < sridSize {
		return ErrInvalid
	}
	return validate(b[sridSize:])
}

// BareToStored prepends the version byte; bare must have passed CheckBare.
func BareToStored(bare []byte) []byte {
	stored := make([]byte, 0, 1+len(bare))
	return append(append(stored, Version1), bare...)
}

// StoredToBare checks and strips the version byte, without copying.
func StoredToBare(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrInvalid
	}
	if stored[0] != Version1 {
		return nil, ErrUnsupportedVersion
	}
	return stored[1:], nil
}

// Header reads only the root header of a bare value.
func Header(bare []byte) (srid uint32, typ Type, err error) {
	c := cursor{buf: bare}
	if srid, err = c.uint32(); err != nil {
		return 0, 0, err
	}
	if err = c.littleEndian(); err != nil {
		return 0, 0, err
	}
	word, err := c.uint32()
	if err != nil {
		return 0, 0, err
	}
	typ, _, hasSRID, ok := splitType(word)
	if hasSRID || !ok {
		return 0, 0, ErrInvalid
	}
	return srid, typ, nil
}

type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) next(n int) ([]byte, error) {
	if n > len(c.buf)-c.pos {
		return nil, ErrInvalid
	}
	b := c.buf[c.pos : c.pos+n]
	c.pos += n
	return b, nil
}

func (c *cursor) littleEndian() error {
	b, err := c.next(1)
	if err != nil {
		return err
	}
	if b[0] != byteOrderLittle {
		return ErrInvalid
	}
	return nil
}

func (c *cursor) uint32() (uint32, error) {
	b, err := c.next(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

// count reads an element count whose elements, minSize bytes or more each, must fit in the
// rest of the input.
func (c *cursor) count(minSize int) (int, error) {
	n, err := c.uint32()
	if err != nil {
		return 0, err
	}
	if uint64(n)*uint64(minSize) > uint64(len(c.buf)-c.pos) {
		return 0, ErrInvalid
	}
	return int(n), nil
}

// splitType splits an EWKB type word; ok is false for a type code other than 1 to 7.
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

func validate(b []byte) error {
	v := validator{c: cursor{buf: b}}
	if err := v.geometry(1, anyType, dimsXY); err != nil {
		return err
	}
	if v.c.pos != len(b) {
		return ErrInvalid
	}
	return nil
}

type validator struct {
	c cursor
}

// geometry checks one geometry; want and parentDims are what its parent requires.
func (v *validator) geometry(depth int, want Type, parentDims dims) error {
	if depth > MaxDepth {
		return ErrTooDeep
	}
	if err := v.c.littleEndian(); err != nil {
		return err
	}
	word, err := v.c.uint32()
	if err != nil {
		return err
	}
	typ, d, hasSRID, ok := splitType(word)
	if !ok || hasSRID {
		return ErrInvalid
	}
	if depth > 1 && (d != parentDims || want != anyType && typ != want) {
		return ErrInvalid
	}
	size := d.size()
	switch typ {
	case TypePoint:
		return v.point(size)
	case TypeLineString:
		_, err := v.sequence(size)
		return err
	case TypePolygon:
		return v.polygon(size)
	}
	n, err := v.c.count(minGeometrySize)
	if err != nil {
		return err
	}
	for range n {
		if err := v.geometry(depth+1, memberTypeOf(typ), d); err != nil {
			return err
		}
	}
	return nil
}

// point accepts all-NaN coordinates: EWKB writes POINT EMPTY that way, as PostGIS and GEOS
// do.
func (v *validator) point(size int) error {
	coords, err := v.c.next(size)
	if err != nil {
		return err
	}
	if finite(coords) {
		return nil
	}
	for i := 0; i < len(coords); i += 8 {
		if !math.IsNaN(math.Float64frombits(binary.LittleEndian.Uint64(coords[i:]))) {
			return ErrInvalid
		}
	}
	return nil
}

func (v *validator) sequence(size int) (int, error) {
	n, err := v.c.count(size)
	if err != nil {
		return 0, err
	}
	coords, err := v.c.next(n * size)
	if err != nil {
		return 0, err
	}
	if !finite(coords) {
		return 0, ErrInvalid
	}
	return n, nil
}

// polygon requires a point in each ring; no rings is POLYGON EMPTY.
func (v *validator) polygon(size int) error {
	n, err := v.c.count(4)
	if err != nil {
		return err
	}
	for range n {
		points, err := v.sequence(size)
		if err != nil {
			return err
		}
		if points == 0 {
			return ErrInvalid
		}
	}
	return nil
}

func finite(coords []byte) bool {
	for i := 0; i+8 <= len(coords); i += 8 {
		if binary.LittleEndian.Uint64(coords[i:])&nonFinite == nonFinite {
			return false
		}
	}
	return true
}
