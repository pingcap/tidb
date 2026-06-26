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
	"math"
	"strconv"
	"strings"

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

// PlanarParams is the per-index tuning of the planar coverer (subdivision depth
// and domain bounds). It is the single source of truth shared by the index
// write path (the tidb_spatial_key builtin) and the query planner: both derive
// the coverer from identical params, so stored keys and query ranges agree.
//
// Selectivity depends on these: a leaf cell is domainWidth/2^Level wide, so
// matching the bounds to the data extent and choosing a Level that makes a leaf
// small relative to typical queries is what turns the index from "scan the whole
// cell then refine" into a genuinely selective scan.
type PlanarParams struct {
	Level                  uint
	MinX, MinY, MaxX, MaxY float64
}

// DefaultPlanarParams returns the design-default planar tuning.
func DefaultPlanarParams() PlanarParams {
	return PlanarParams{
		Level: DefaultLevel,
		MinX:  -DefaultDomainBound, MinY: -DefaultDomainBound,
		MaxX: DefaultDomainBound, MaxY: DefaultDomainBound,
	}
}

// Coverer builds the planar coverer described by these params.
func (p PlanarParams) Coverer() *PlanarCoverer {
	return NewPlanarCoverer(p.MinX, p.MinY, p.MaxX, p.MaxY, p.Level)
}

// ParsePlanarParams parses the argument list (excluding the geometry, i.e. the
// text after the first comma of tidb_spatial_key(geom, level, minX, minY, maxX,
// maxY)) into PlanarParams. An empty list yields the defaults. The expected form
// is exactly five numbers: level minX minY maxX maxY.
func ParsePlanarParams(extraArgs []string) (PlanarParams, error) {
	if len(extraArgs) == 0 {
		return DefaultPlanarParams(), nil
	}
	if len(extraArgs) != 5 {
		return PlanarParams{}, errors.Errorf("spatial: expected 0 or 5 cell params, got %d", len(extraArgs))
	}
	nums := make([]float64, 5)
	for i, s := range extraArgs {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return PlanarParams{}, errors.Errorf("spatial: invalid cell param %q: %v", s, err)
		}
		nums[i] = f
	}
	p := PlanarParams{Level: uint(nums[0]), MinX: nums[1], MinY: nums[2], MaxX: nums[3], MaxY: nums[4]}
	if p.Level == 0 || p.Level > 31 {
		return PlanarParams{}, errors.Errorf("spatial: cell level must be in [1,31], got %d", p.Level)
	}
	if p.MinX >= p.MaxX || p.MinY >= p.MaxY {
		return PlanarParams{}, errors.Errorf("spatial: invalid bounds [%g,%g]x[%g,%g]", p.MinX, p.MaxX, p.MinY, p.MaxY)
	}
	return p, nil
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
	c.cover(0, 0, 0, r, c.coverLevelFor(r), &ranges)
	return mergeRanges(ranges), nil
}

// targetCellsAcross is roughly how many covering cells span the query rectangle
// per axis. It caps the covering's cell (and therefore range) count so the
// planner builds a small OR of index ranges instead of thousands. Larger =
// tighter covering (fewer false positives) but more ranges.
const targetCellsAcross = 8

// coverLevelFor picks the quadtree depth to cover a rectangle at: deep enough
// that a leaf is small relative to the rectangle (good selectivity), shallow
// enough that the covering stays a bounded number of cells. It never exceeds the
// coverer's leaf level, so emitted ranges always contain whole leaf cells.
func (c *PlanarCoverer) coverLevelFor(r Rect) uint {
	w := math.Max(r.MaxX-r.MinX, r.MaxY-r.MinY)
	domainW := math.Max(c.maxX-c.minX, c.maxY-c.minY)
	if w <= 0 || domainW <= 0 {
		return c.level
	}
	// cellWidth(L) = domainW / 2^L; choose L so cellWidth ~= w/targetCellsAcross.
	l := int(math.Floor(math.Log2(targetCellsAcross * domainW / w)))
	if l < 1 {
		l = 1
	}
	if uint(l) > c.level {
		return c.level
	}
	return uint(l)
}

// CoverFixedLevelCells returns the canonical cell keys of every cell at the
// coverer's level that overlaps the rectangle. Unlike CoverRect (which returns
// leaf-resolution ranges for an ordered range scan), this returns one discrete
// key per touched cell, suitable for a set-overlap (multi-valued index) match: a
// stored geometry and a query geometry that intersect both touch a common cell,
// so their cell sets overlap. Over-covering (bbox) is fine; the caller refines.
func (c *PlanarCoverer) CoverFixedLevelCells(r Rect) ([]CellKey, error) {
	if r.MinX > r.MaxX {
		r.MinX, r.MaxX = r.MaxX, r.MinX
	}
	if r.MinY > r.MaxY {
		r.MinY, r.MaxY = r.MaxY, r.MinY
	}
	col0 := c.quantize(r.MinX, c.minX, c.maxX)
	col1 := c.quantize(r.MaxX, c.minX, c.maxX)
	row0 := c.quantize(r.MinY, c.minY, c.maxY)
	row1 := c.quantize(r.MaxY, c.minY, c.maxY)
	keys := make([]CellKey, 0, (col1-col0+1)*(row1-row0+1))
	for col := col0; col <= col1; col++ {
		for row := row0; row <= row1; row++ {
			keys = append(keys, mortonKey(interleave(col, row)))
		}
	}
	return keys, nil
}

// cover walks the quadtree node at (level, col, row) given in that node's grid
// resolution, descending no deeper than coverLevel. It appends covering ranges
// (in Morton/leaf resolution) to out.
func (c *PlanarCoverer) cover(level uint, col, row uint32, r Rect, coverLevel uint, out *[]CellKeyRange) {
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

	// Fully contained, or as deep as we cover: emit the contiguous Morton range
	// of all leaf cells under this node (over-covering at coverLevel < leaf).
	fullyInside := nodeMinX >= r.MinX && nodeMaxX <= r.MaxX && nodeMinY >= r.MinY && nodeMaxY <= r.MaxY
	if fullyInside || level >= coverLevel {
		shift := 2 * (c.level - level) // 2 Morton bits per finer level
		base := interleave(col, row) << shift
		hi := base + (uint64(1) << shift) - 1
		*out = append(*out, CellKeyRange{Lo: mortonKey(base), Hi: mortonKey(hi)})
		return
	}

	// Otherwise descend into the four child quadrants.
	for dx := range uint32(2) {
		for dy := range uint32(2) {
			c.cover(level+1, col*2+dx, row*2+dy, r, coverLevel, out)
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
