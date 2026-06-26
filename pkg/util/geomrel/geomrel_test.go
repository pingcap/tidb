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

package geomrel

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// ewkb builds the POC EWKB form (<srid_le><wkb>) from a WKT literal.
func ewkb(t *testing.T, wktStr string) string {
	g, err := wkt.Unmarshal(wktStr)
	require.NoError(t, err)
	b, err := wkb.Marshal(g, binary.LittleEndian)
	require.NoError(t, err)
	return string(append([]byte{0, 0, 0, 0}, b...))
}

func TestRelateOGCSemantics(t *testing.T) {
	box := ewkb(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")

	cases := []struct {
		pred Predicate
		a, b string
		want bool
	}{
		// Interior point: within / contained.
		{Within, ewkb(t, "POINT(5 5)"), box, true},
		{Contains, box, ewkb(t, "POINT(5 5)"), true},
		// Boundary corners: NOT within / contained (OGC), and consistent.
		{Within, ewkb(t, "POINT(0 0)"), box, false},
		{Within, ewkb(t, "POINT(10 10)"), box, false},
		{Contains, box, ewkb(t, "POINT(0 0)"), false},
		// Exterior point.
		{Within, ewkb(t, "POINT(15 5)"), box, false},
		// Intersects: boundary touch is true, exterior is false.
		{Intersects, box, ewkb(t, "POINT(0 0)"), true},
		{Intersects, box, ewkb(t, "POINT(15 5)"), false},
		{Disjoint, box, ewkb(t, "POINT(15 5)"), true},
		{Equals, ewkb(t, "POINT(1 1)"), ewkb(t, "POINT(1 1)"), true},
		{Equals, ewkb(t, "POINT(1 1)"), ewkb(t, "POINT(2 2)"), false},
	}
	for _, c := range cases {
		got, err := Relate(c.pred, c.a, c.b)
		require.NoError(t, err)
		require.Equalf(t, c.want, got, "pred=%d", c.pred)
	}
}

// TestRelateConcurrent exercises the context pool from many goroutines.
func TestRelateConcurrent(t *testing.T) {
	box := ewkb(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")
	pt := ewkb(t, "POINT(5 5)")
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				got, err := Relate(Within, pt, box)
				require.NoError(t, err)
				require.True(t, got)
			}
		}()
	}
	wg.Wait()
}

// TestRelateInvalidGeometryNoPanic confirms a GEOS topology exception on an
// invalid (self-intersecting) polygon is returned as an error, not a panic.
func TestRelateInvalidGeometryNoPanic(t *testing.T) {
	bowtie := ewkb(t, "POLYGON((0 0,2 2,2 0,0 2,0 0))") // self-intersecting
	pt := ewkb(t, "POINT(1 1)")
	require.NotPanics(t, func() {
		_, _ = Relate(Within, pt, bowtie)
		_, _ = Relate(Overlaps, bowtie, bowtie)
	})
}
