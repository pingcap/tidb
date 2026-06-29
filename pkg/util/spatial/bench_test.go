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
	"math/rand"
	"sort"
	"testing"
)

// TestSelectivityVsLevel quantifies, for a uniform point cloud over the indexed
// domain, how the covering's candidate count and false-positive ratio change
// with the cell level for a fixed-size proximity query. Higher level => tighter
// covering (fewer candidates) until the range count overhead grows.
func TestSelectivityVsLevel(t *testing.T) {
	const n = 100000
	rng := rand.New(rand.NewSource(1))
	type pt struct{ x, y float64 }
	pts := make([]pt, n)
	for i := range pts {
		pts[i] = pt{rng.Float64(), rng.Float64()} // [0,1)x[0,1)
	}
	// Query: a 0.05x0.05 window around (0.5,0.5); true matches are points inside.
	q := Rect{MinX: 0.475, MinY: 0.475, MaxX: 0.525, MaxY: 0.525}
	trueMatches := 0
	for _, p := range pts {
		if p.x >= q.MinX && p.x <= q.MaxX && p.y >= q.MinY && p.y <= q.MaxY {
			trueMatches++
		}
	}
	t.Logf("uniform points=%d, true matches in window=%d (%.2f%%)", n, trueMatches, 100*float64(trueMatches)/n)
	for _, level := range []uint{8, 10, 12, 14, 16} {
		c := NewPlanarCoverer(0, 0, 1, 1, level)
		ranges, _ := c.CoverRect(0, q)
		// Build sorted point keys to count candidates landing in the ranges.
		keys := make([][]byte, n)
		for i, p := range pts {
			k, _ := c.EncodePoint(0, p.x, p.y)
			keys[i] = k
		}
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		cand := 0
		for _, r := range ranges {
			lo := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], r.Lo) >= 0 })
			hi := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], r.Hi) > 0 })
			cand += hi - lo
		}
		t.Logf("level=%2d: ranges=%3d candidates=%5d  false-positive-ratio=%.2fx  scanned=%.2f%% of table",
			level, len(ranges), cand, float64(cand)/float64(trueMatches), 100*float64(cand)/n)
	}
}

func BenchmarkCoverRect(b *testing.B) {
	c := NewPlanarCoverer(0, 0, 1, 1, 16)
	q := Rect{MinX: 0.475, MinY: 0.475, MaxX: 0.525, MaxY: 0.525}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.CoverRect(0, q)
	}
}

func BenchmarkEncodePoint(b *testing.B) {
	c := NewDefaultPlanarCoverer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.EncodePoint(0, 123.456, 789.012)
	}
}
