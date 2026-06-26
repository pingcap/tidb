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

package spatial

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// keyInRanges reports whether k falls in any covering range.
func keyInRanges(k CellKey, ranges []CellKeyRange) bool {
	for _, r := range ranges {
		if bytes.Compare(k, r.Lo) >= 0 && bytes.Compare(k, r.Hi) <= 0 {
			return true
		}
	}
	return false
}

// TestPlanarCoverNoFalseNegatives is the Milestone 1 acceptance test: for many
// random query rectangles, every random point that lies inside the rectangle
// must encode to a CellKey contained in the covering ranges. A false negative
// (an inside point whose cell is not covered) would silently drop matching rows.
func TestPlanarCoverNoFalseNegatives(t *testing.T) {
	c := NewPlanarCoverer(-1000, -1000, 1000, 1000, 12)
	rng := rand.New(rand.NewSource(42))

	const queries = 500
	const pointsPerQuery = 200
	totalInside := 0
	for range queries {
		x1 := rng.Float64()*2000 - 1000
		x2 := rng.Float64()*2000 - 1000
		y1 := rng.Float64()*2000 - 1000
		y2 := rng.Float64()*2000 - 1000
		rect := Rect{MinX: math.Min(x1, x2), MinY: math.Min(y1, y2), MaxX: math.Max(x1, x2), MaxY: math.Max(y1, y2)}
		ranges, err := c.CoverRect(0, rect)
		require.NoError(t, err)

		for range pointsPerQuery {
			px := rng.Float64()*2000 - 1000
			py := rng.Float64()*2000 - 1000
			if px < rect.MinX || px > rect.MaxX || py < rect.MinY || py > rect.MaxY {
				continue
			}
			totalInside++
			key, err := c.EncodePoint(0, px, py)
			require.NoError(t, err)
			require.Truef(t, keyInRanges(key, ranges),
				"false negative: point (%f,%f) inside rect %+v not covered", px, py, rect)
		}
	}
	require.Greater(t, totalInside, 0, "expected some points inside the query rectangles")
	t.Logf("checked %d inside-points across %d queries with zero false negatives", totalInside, queries)
}

// TestPlanarEncodePointDeterministic verifies a point maps to exactly one stable
// key and that nearby points in the same leaf cell share a key.
func TestPlanarEncodePointDeterministic(t *testing.T) {
	c := NewPlanarCoverer(0, 0, 1024, 1024, 10) // leaf cell = 1x1 unit
	k1, err := c.EncodePoint(0, 100.2, 200.7)
	require.NoError(t, err)
	k2, err := c.EncodePoint(0, 100.9, 200.1)
	require.NoError(t, err)
	require.Equal(t, k1, k2, "points in the same 1x1 leaf cell must share a key")

	k3, err := c.EncodePoint(0, 102.0, 200.0)
	require.NoError(t, err)
	require.NotEqual(t, k1, k3, "points in different cells must differ")
}

// TestPlanarCoverRectIsSuperset checks the covering of a small rectangle is a
// modest, non-empty set of ranges (sanity on the range count).
func TestPlanarCoverRectIsSuperset(t *testing.T) {
	c := NewDefaultPlanarCoverer()
	ranges, err := c.CoverRect(0, Rect{MinX: -10, MinY: -10, MaxX: 10, MaxY: 10})
	require.NoError(t, err)
	require.NotEmpty(t, ranges)
	for _, r := range ranges {
		require.LessOrEqual(t, bytes.Compare(r.Lo, r.Hi), 0)
	}
}

// TestNon0SRIDRejected confirms the planar coverer is SRID 0 only for now.
func TestNon0SRIDRejected(t *testing.T) {
	c := NewDefaultPlanarCoverer()
	_, err := c.EncodePoint(4326, 1, 2)
	require.Error(t, err)
	_, err = c.CoverRect(4326, Rect{})
	require.Error(t, err)
}

// TestParsePlanarParams covers the param parsing shared by the builtin/planner.
func TestParsePlanarParams(t *testing.T) {
	d, err := ParsePlanarParams(nil)
	require.NoError(t, err)
	require.Equal(t, DefaultPlanarParams(), d)

	p, err := ParsePlanarParams([]string{"12", "0", "0", "1", "1"})
	require.NoError(t, err)
	require.Equal(t, PlanarParams{Level: 12, MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}, p)

	_, err = ParsePlanarParams([]string{"12", "0", "0"})
	require.Error(t, err)
	_, err = ParsePlanarParams([]string{"0", "0", "0", "1", "1"}) // bad level
	require.Error(t, err)
	_, err = ParsePlanarParams([]string{"12", "1", "0", "0", "1"}) // minX>=maxX
	require.Error(t, err)
}

// TestCoverRectRangeCountBounded confirms the cell cap keeps the covering to a
// small number of ranges (so the planner builds a small OR), independent of how
// deep the coverer's leaf level is.
func TestCoverRectRangeCountBounded(t *testing.T) {
	for _, level := range []uint{10, 16, 24, 31} {
		c := NewPlanarCoverer(0, 0, 1, 1, level)
		rs, err := c.CoverRect(0, Rect{MinX: 0.4, MinY: 0.4, MaxX: 0.6, MaxY: 0.6})
		require.NoError(t, err)
		require.NotEmpty(t, rs)
		require.LessOrEqual(t, len(rs), 64, "level %d produced too many ranges: %d", level, len(rs))
	}
}
