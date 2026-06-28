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

package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPOCSpatialS2 exercises the SRID 4326 (S2) path: a lat/long points table
// with a spatial index, queried by ST_Distance_Sphere within a radius. Results
// must match a full scan (with and without forcing the index), including a
// region near the antimeridian and one near a pole.
func TestPOCSpatialS2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE places (id int primary key, p POINT NOT NULL SRID 4326)")

	// A grid of lon/lat points covering tricky regions: around the prime
	// meridian, the antimeridian (±180), and near the north pole.
	var b strings.Builder
	b.WriteString("INSERT INTO places VALUES ")
	id := 0
	add := func(lng, lat float64) {
		if id > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d, ST_GeomFromText('POINT(%g %g)',4326))", id, lat, lng) // 4326 WKT axis: (lat, lng)
		id++
	}
	for lng := -180.0; lng <= 180.0; lng += 5 {
		for lat := -80.0; lat <= 85.0; lat += 5 {
			add(lng, lat)
		}
	}
	tk.MustExec(b.String())

	tk.MustExec("CREATE SPATIAL INDEX sidx ON places (p)")

	type q struct {
		name             string
		lng, lat, radius float64
	}
	queries := []q{
		{"london", -0.13, 51.5, 600000},     // ~600 km around London
		{"antimeridian", 179.5, 10, 400000}, // straddles ±180
		{"near-pole", 20, 84, 800000},       // near the north pole
	}
	for _, qq := range queries {
		where := fmt.Sprintf("ST_Distance_Sphere(p, ST_GeomFromText('POINT(%g %g)',4326)) <= %g", qq.lat, qq.lng, qq.radius)
		base := tk.MustQuery("SELECT id FROM places WHERE " + where + " ORDER BY id").Rows()
		require.NotEmptyf(t, base, "query %s returned no rows", qq.name)
		// Forcing the index must return identical rows (proves S2 covering +
		// refine equivalence, incl. antimeridian/pole).
		tk.MustQuery("SELECT id FROM places FORCE INDEX (sidx) WHERE " + where + " ORDER BY id").Check(base)
		t.Logf("query %s: %d rows", qq.name, len(base))
	}

	// Containment on 4326 via ST_Within against a lon/lat box.
	const within = "SELECT id FROM places %s WHERE " +
		"ST_Within(p, ST_GeomFromText('POLYGON((50 0,50 2,52 2,52 0,50 0))',4326)) ORDER BY id"
	wantWithin := tk.MustQuery(fmt.Sprintf(within, "")).Rows()
	tk.MustQuery(fmt.Sprintf(within, "FORCE INDEX (sidx)")).Check(wantWithin)

	// The ST_X/ST_Y bbox columns prune SRID-4326 distance queries too (away from
	// the antimeridian/poles): the resolver injects the spherical cap's lat/long
	// MBR as a Selection on the bbox columns. (TestPOCSpatialS2's distance loop
	// already proved the bbox is conservative — no false negatives.)
	const london = "ST_Distance_Sphere(p, ST_GeomFromText('POINT(51.5 -0.13)',4326)) <= 600000"
	var sel strings.Builder
	for _, r := range tk.MustQuery("EXPLAIN format='brief' SELECT id FROM places FORCE INDEX (sidx) WHERE " + london).Rows() {
		if strings.Contains(fmt.Sprintf("%v", r[0]), "Selection") {
			sel.WriteString(fmt.Sprintf("%v|", r[4]))
		}
	}
	require.Contains(t, sel.String(), "_v$_sidx", "expected the bbox columns in a cop Selection (spherical-cap bbox pruning)")
}

// TestPOCSpatial4326Axis pins MySQL's EPSG:4326 axis order (latitude, longitude):
// the first WKT coordinate is the latitude. Values verified against MySQL 9.7
// (ST_Latitude=30, ST_Longitude=50; ST_Distance_Sphere=146775.88330371436 m).
func TestPOCSpatial4326Axis(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("SELECT ST_Latitude(ST_GeomFromText('POINT(30 50)',4326))").Check(testkit.Rows("30"))
	tk.MustQuery("SELECT ST_Longitude(ST_GeomFromText('POINT(30 50)',4326))").Check(testkit.Rows("50"))
	tk.MustQuery("SELECT ABS(ST_Distance_Sphere(ST_GeomFromText('POINT(30 50)',4326), " +
		"ST_GeomFromText('POINT(31 51)',4326)) - 146775.88330371436) < 1").Check(testkit.Rows("1"))
}
