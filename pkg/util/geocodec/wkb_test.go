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
	"encoding/hex"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This file writes WKB for the tests byte by byte, without a geometry model.

func mustHex(t testing.TB, s string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}

// flagsOf returns the EWKB type-word flags for d.
func flagsOf(d dims) uint32 {
	var flags uint32
	if d&dimsXYZ != 0 {
		flags |= ewkbZ
	}
	if d&dimsXYM != 0 {
		flags |= ewkbM
	}
	return flags
}

// le is a little-endian geometry of type typ with coordinates d, its body body.
func le(typ GeomType, d dims, body ...[]byte) []byte {
	b := []byte{byteOrderLittle}
	b = binary.LittleEndian.AppendUint32(b, uint32(typ)|flagsOf(d))
	for _, part := range body {
		b = append(b, part...)
	}
	return b
}

// coords is the little-endian bytes of fs.
func coords(fs ...float64) []byte {
	var b []byte
	for _, f := range fs {
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
	}
	return b
}

// count is a little-endian count.
func count(n int) []byte { return binary.LittleEndian.AppendUint32(nil, uint32(n)) }

// emptyPoint is POINT EMPTY with coordinates d, as EWKB writes it.
func emptyPoint(d dims) []byte {
	fs := make([]float64, d.size()/8)
	for i := range fs {
		fs[i] = math.Float64frombits(quietNaN)
	}
	return le(TypePoint, d, coords(fs...))
}

// collectionOf is a collection of type typ with coordinates d holding members.
func collectionOf(typ GeomType, d dims, members ...[]byte) []byte {
	return le(typ, d, append([][]byte{count(len(members))}, members...)...)
}

// byteOrderBig is the byte-order byte of big-endian WKB, which no bare value holds.
const byteOrderBig byte = 0

// bareOf makes a little-endian root geometry g without an SRID the bare value at srid:
// MySQL's binary format for a 2D value, extended with the Z and M flags otherwise.
func bareOf(srid uint32, g []byte) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, srid), g...)
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

// chainOf returns a bare value at SRID 0 of depth-1 GEOMETRYCOLLECTIONs, each holding the
// next and the innermost holding leaf, a little-endian geometry, which so sits at depth.
// flags are set on every collection's type word.
func chainOf(depth int, flags uint32, leaf []byte) []byte {
	b := make([]byte, 4)
	for range depth - 1 {
		b = append(b, byteOrderLittle)
		b = binary.LittleEndian.AppendUint32(b, 7|flags)
		b = binary.LittleEndian.AppendUint32(b, 1)
	}
	return append(b, leaf...)
}

// bareChain is chainOf without flags.
func bareChain(depth int, leaf []byte) []byte { return chainOf(depth, 0, leaf) }

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

// wkbGen generates random WKB of every type, byte by byte, and knows whether what it
// generates has a big-endian member, which the bare value cannot hold.
type wkbGen struct {
	rnd *rand.Rand
	d   dims
	// empty is the chance that a geometry other than a GEOMETRYCOLLECTION is empty.
	empty float64
	// big is the chance that a geometry below the root is big-endian.
	big float64

	// What the generator knows about the last root geometry, for ParseColumnValue:
	// canon is its canonical form (little-endian, POINT EMPTY as quietNaN), env bounds
	// its positions, and short says it has a linestring of 1 point or a ring of 1 to 3.
	canon []byte
	env   Envelope
	short bool
}

// position widens env to the position (x, y).
func (g *wkbGen) position(x, y float64) {
	if !g.env.NotEmpty {
		g.env = Envelope{MinX: x, MinY: y, MaxX: x, MaxY: y, NotEmpty: true}
		return
	}
	g.env.MinX, g.env.MinY = min(g.env.MinX, x), min(g.env.MinY, y)
	g.env.MaxX, g.env.MaxY = max(g.env.MaxX, x), max(g.env.MaxY, y)
}

// float64s appends n random positions to b in order and to canon.
func (g *wkbGen) float64s(b []byte, order binary.AppendByteOrder, n int) []byte {
	dim := g.d.size() / 8
	var x float64
	for i := range n * dim {
		f := g.float()
		switch i % dim {
		case 0:
			x = f
		case 1:
			g.position(x, f)
		}
		b = order.AppendUint64(b, math.Float64bits(f))
		g.canon = binary.LittleEndian.AppendUint64(g.canon, math.Float64bits(f))
	}
	return b
}

// genMaxDepth bounds how deep generated collections nest.
const genMaxDepth = 4

var allTypes = []GeomType{
	TypePoint, TypeLineString, TypePolygon, TypeMultiPoint, TypeMultiLineString,
	TypeMultiPolygon, TypeGeometryCollection,
}

// quietNaN is the NaN PostGIS, GEOS and TiDB write for each coordinate of POINT EMPTY.
const quietNaN uint64 = 0x7FF8000000000000

// nanBits are NaNs a POINT EMPTY may be written with: the quiet NaN TiDB writes, Go's
// math.NaN(), a negative quiet NaN, a signaling NaN and the largest NaN.
var nanBits = []uint64{quietNaN, 0x7FF8000000000001, 0xFFF8000000000000, 0x7FF0000000000001, 0x7FFFFFFFFFFFFFFF}

func (g *wkbGen) float() float64 {
	switch g.rnd.IntN(6) {
	case 0:
		return 0
	case 1:
		return math.Copysign(0, -1)
	case 2:
		return math.SmallestNonzeroFloat64 * float64(1+g.rnd.IntN(1000))
	case 3:
		return math.Copysign(math.MaxFloat64, g.rnd.NormFloat64())
	}
	return g.rnd.NormFloat64() * 1000
}

// geometry returns random WKB of type typ, little-endian at the root, and whether a member
// is big-endian.
func (g *wkbGen) geometry(typ GeomType, depth int) (b []byte, bigEndian bool) {
	if depth == 1 {
		g.canon, g.env, g.short = nil, Envelope{}, false
	}
	var order binary.AppendByteOrder = binary.LittleEndian
	orderByte := byteOrderLittle
	if depth > 1 && g.rnd.Float64() < g.big {
		order, orderByte = binary.BigEndian, byteOrderBig
		bigEndian = true
	}
	uint32s := func(v uint32) {
		b = order.AppendUint32(b, v)
		g.canon = binary.LittleEndian.AppendUint32(g.canon, v)
	}
	b = append(b, orderByte)
	g.canon = append(g.canon, byteOrderLittle)
	uint32s(uint32(typ) | flagsOf(g.d))
	isEmpty := typ != TypeGeometryCollection && g.rnd.Float64() < g.empty
	// positions writes n positions, of which a linestring or ring needs least.
	positions := func(n, least int) {
		uint32s(uint32(n))
		b = g.float64s(b, order, n)
		g.short = g.short || n > 0 && n < least
	}
	switch typ {
	case TypePoint:
		if isEmpty {
			for range g.d.size() / 8 {
				b = order.AppendUint64(b, nanBits[g.rnd.IntN(len(nanBits))])
				g.canon = binary.LittleEndian.AppendUint64(g.canon, quietNaN)
			}
		} else {
			b = g.float64s(b, order, 1)
		}
	case TypeLineString:
		if isEmpty {
			positions(0, 2)
		} else {
			positions(1+g.rnd.IntN(5), 2)
		}
	case TypePolygon:
		if isEmpty {
			uint32s(0)
		} else {
			rings := 1 + g.rnd.IntN(3)
			uint32s(uint32(rings))
			for range rings {
				positions(1+g.rnd.IntN(5), 4)
			}
		}
	default:
		n := 0
		if typ == TypeGeometryCollection {
			// A GEOMETRYCOLLECTION is empty a quarter of the time.
			n = g.rnd.IntN(4)
		} else if !isEmpty {
			n = 1 + g.rnd.IntN(3)
		}
		uint32s(uint32(n))
		for range n {
			member := memberTypeOf(typ)
			if member == anyType {
				types := allTypes
				if depth >= genMaxDepth {
					types = allTypes[:len(allTypes)-1]
				}
				member = types[g.rnd.IntN(len(types))]
			}
			m, memberBigEndian := g.geometry(member, depth+1)
			b = append(b, m...)
			bigEndian = bigEndian || memberBigEndian
		}
	}
	return b, bigEndian
}

func (g *wkbGen) srid() uint32 {
	switch g.rnd.IntN(4) {
	case 0:
		return 0
	case 1:
		return 4326
	case 2:
		return 3857
	}
	return g.rnd.Uint32()
}
