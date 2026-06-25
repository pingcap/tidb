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

// Package spatial implements the engine-neutral cell covering used by the
// spatial index. A point maps to a single order-preserving CellKey; a query
// region maps to a set of CellKey ranges that cover every cell a contained
// point could be indexed under (no false negatives). The covering is coarse,
// so the caller refines candidates with the exact geometry predicate.
package spatial

import (
	"encoding/binary"

	"github.com/pingcap/errors"
)

// CellKey is an order-preserving encoded cell identifier. Comparing two
// CellKeys as byte slices (bytes.Compare) matches their order along the
// space-filling curve, so a contiguous curve segment is a single byte range.
type CellKey []byte

// CellKeyRange is an inclusive [Lo, Hi] range of CellKeys to scan.
type CellKeyRange struct {
	Lo CellKey
	Hi CellKey
}

// Rect is an axis-aligned query rectangle in the coverer's coordinate space.
type Rect struct {
	MinX, MinY, MaxX, MaxY float64
}

// Coverer is the engine- and SRID-neutral seam between geometry and the index.
type Coverer interface {
	// EncodePoint returns the single CellKey a point is indexed under.
	EncodePoint(srid uint32, x, y float64) (CellKey, error)
	// CoverRect returns CellKey ranges covering every cell that a point inside
	// the rectangle could map to. The result is a superset (over-covering is
	// allowed); the caller refines with the exact predicate.
	CoverRect(srid uint32, r Rect) ([]CellKeyRange, error)
}

// PlanarCoverer is the SRID 0 coverer: a quadtree over a bounded Cartesian
// domain whose leaf cells are linearized by a Z-order (Morton) curve.
type PlanarCoverer struct {
	minX, minY float64
	maxX, maxY float64
	// level is the quadtree depth: each axis is divided into 2^level columns,
	// so a leaf cell is (domainWidth / 2^level) wide. level <= 31 keeps the
	// interleaved Morton code within 62 bits of a uint64.
	level uint
}

// DefaultDomainBound matches the design's default SRID 0 domain of
// [-(1<<31), (1<<31)-1] per axis (also CockroachDB's default).
const DefaultDomainBound = float64(int64(1) << 31)

// DefaultLevel is the default quadtree depth. It controls cell granularity
// (and therefore the false-positive rate), not correctness.
const DefaultLevel = 16

// NewDefaultPlanarCoverer returns the SRID 0 coverer with the design defaults.
func NewDefaultPlanarCoverer() *PlanarCoverer {
	return NewPlanarCoverer(-DefaultDomainBound, -DefaultDomainBound, DefaultDomainBound, DefaultDomainBound, DefaultLevel)
}

// NewPlanarCoverer builds a planar coverer over the given domain and depth.
func NewPlanarCoverer(minX, minY, maxX, maxY float64, level uint) *PlanarCoverer {
	if level == 0 || level > 31 {
		level = DefaultLevel
	}
	return &PlanarCoverer{minX: minX, minY: minY, maxX: maxX, maxY: maxY, level: level}
}

// gridSide is the number of cells along one axis (2^level).
func (c *PlanarCoverer) gridSide() uint32 {
	return uint32(1) << c.level
}

// quantize maps a coordinate on one axis to an integer cell column in
// [0, gridSide), clamping out-of-domain values to the boundary cell so they
// stay indexed (over-covered near the edge) rather than rejected.
func (c *PlanarCoverer) quantize(v, lo, hi float64) uint32 {
	side := c.gridSide()
	if v <= lo {
		return 0
	}
	if v >= hi {
		return side - 1
	}
	frac := (v - lo) / (hi - lo)
	idx := uint32(frac * float64(side))
	if idx >= side {
		idx = side - 1
	}
	return idx
}

// EncodePoint implements Coverer.
func (c *PlanarCoverer) EncodePoint(srid uint32, x, y float64) (CellKey, error) {
	if srid != 0 {
		return nil, errors.Errorf("PlanarCoverer only supports SRID 0, got %d", srid)
	}
	col := c.quantize(x, c.minX, c.maxX)
	rowCell := c.quantize(y, c.minY, c.maxY)
	return mortonKey(interleave(col, rowCell)), nil
}

// CoverRect implements Coverer by recursively descending the quadtree and
// emitting one CellKey range per node that is fully inside the rectangle or is
// a leaf, skipping nodes disjoint from the rectangle.
func (c *PlanarCoverer) CoverRect(srid uint32, r Rect) ([]CellKeyRange, error) {
	if srid != 0 {
		return nil, errors.Errorf("PlanarCoverer only supports SRID 0, got %d", srid)
	}
	// Normalize the rectangle.
	if r.MinX > r.MaxX {
		r.MinX, r.MaxX = r.MaxX, r.MinX
	}
	if r.MinY > r.MaxY {
		r.MinY, r.MaxY = r.MaxY, r.MinY
	}
	var ranges []CellKeyRange
	c.cover(0, 0, 0, r, &ranges)
	return mergeRanges(ranges), nil
}

// cover walks the quadtree node at (level, col, row) given in that node's grid
// resolution. It appends covering ranges (in Morton/leaf resolution) to out.
func (c *PlanarCoverer) cover(level uint, col, row uint32, r Rect, out *[]CellKeyRange) {
	// The node's domain extent at this level.
	side := uint32(1) << level
	cellW := (c.maxX - c.minX) / float64(side)
	cellH := (c.maxY - c.minY) / float64(side)
	nodeMinX := c.minX + float64(col)*cellW
	nodeMaxX := nodeMinX + cellW
	nodeMinY := c.minY + float64(row)*cellH
	nodeMaxY := nodeMinY + cellH

	// Disjoint: nothing under this node can match.
	if nodeMaxX < r.MinX || nodeMinX > r.MaxX || nodeMaxY < r.MinY || nodeMinY > r.MaxY {
		return
	}

	// Fully contained, or a leaf: emit the contiguous Morton range of all leaf
	// cells under this node.
	fullyInside := nodeMinX >= r.MinX && nodeMaxX <= r.MaxX && nodeMinY >= r.MinY && nodeMaxY <= r.MaxY
	if fullyInside || level == c.level {
		shift := 2 * (c.level - level) // 2 Morton bits per finer level
		base := interleave(col, row) << shift
		hi := base + (uint64(1) << shift) - 1
		*out = append(*out, CellKeyRange{Lo: mortonKey(base), Hi: mortonKey(hi)})
		return
	}

	// Otherwise descend into the four child quadrants.
	for dx := uint32(0); dx < 2; dx++ {
		for dy := uint32(0); dy < 2; dy++ {
			c.cover(level+1, col*2+dx, row*2+dy, r, out)
		}
	}
}

// mortonKey encodes a Morton code as an 8-byte big-endian key so byte order
// equals numeric order.
func mortonKey(m uint64) CellKey {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, m)
	return b
}

// interleave produces the Morton code of cell (col, row) by interleaving their
// bits (row in even bit positions, col in odd), so adjacent Z-order cells get
// adjacent codes.
func interleave(col, row uint32) uint64 {
	return spread(col)<<1 | spread(row)
}

// spread inserts a zero bit between each of the low 32 bits of v.
func spread(v uint32) uint64 {
	x := uint64(v)
	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x << 2)) & 0x3333333333333333
	x = (x | (x << 1)) & 0x5555555555555555
	return x
}

// mergeRanges sorts and coalesces adjacent/overlapping CellKey ranges so the
// index scan sees a minimal range set.
func mergeRanges(ranges []CellKeyRange) []CellKeyRange {
	if len(ranges) <= 1 {
		return ranges
	}
	// Ranges come from Morton codes; sort by Lo.
	sortRangesByLo(ranges)
	merged := ranges[:1]
	for _, cur := range ranges[1:] {
		last := &merged[len(merged)-1]
		// Adjacent if cur.Lo == last.Hi + 1.
		if leUint64(loVal(cur), addOne(hiVal(*last))) {
			if geUint64(hiVal(cur), hiVal(*last)) {
				last.Hi = cur.Hi
			}
			continue
		}
		merged = append(merged, cur)
	}
	return merged
}

func loVal(r CellKeyRange) uint64 { return binary.BigEndian.Uint64(r.Lo) }
func hiVal(r CellKeyRange) uint64 { return binary.BigEndian.Uint64(r.Hi) }

func addOne(v uint64) uint64 {
	if v == ^uint64(0) {
		return v
	}
	return v + 1
}
func leUint64(a, b uint64) bool { return a <= b }
func geUint64(a, b uint64) bool { return a >= b }

func sortRangesByLo(ranges []CellKeyRange) {
	// Simple insertion sort; range counts are small for typical queries.
	for i := 1; i < len(ranges); i++ {
		for j := i; j > 0 && loVal(ranges[j-1]) > loVal(ranges[j]); j-- {
			ranges[j-1], ranges[j] = ranges[j], ranges[j-1]
		}
	}
}
