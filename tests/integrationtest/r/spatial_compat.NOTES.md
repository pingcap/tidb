# spatial_compat: PoC vs MySQL 8.0.46 findings

Recorded `r/spatial_compat.result` (PoC TiDB) and `r/spatial_compat.result.mysql`
(stock MySQL 8.0.46) with the same `mysql_tester`, then diffed. Two divergences,
both in the geometry-function layer. Distances, accessors, SRID, and **both
spatial-index result sets match MySQL exactly**.

## 1. ST_AsText spacing (compat gap, cosmetic)

PoC emits `POINT (0 0)` (note the space); MySQL emits `POINT(0 0)`. Source is
go-geom's `wkt.Marshal` default. Apps that string-compare or re-parse WKT will
trip on this. Fix in the `ST_AsText`/`ST_AsWKT` formatter.

## 2. Point-on-boundary containment (correctness bug)

`ST_Within(p, box)` / `ST_Contains(box, p)` for box
`POLYGON((0 0,10 0,10 10,0 10,0 0))`:

| point        | PoC | MySQL (OGC) |
|--------------|-----|-------------|
| (0,0) corner | 1   | 0           |
| (10,10) corner | 0 | 0           |

MySQL excludes boundary points (OGC: boundary is not interior). The PoC's
ray-casting returns 1 for the (0,0) corner but 0 for the opposite (10,10)
corner — so it is both wrong vs MySQL **and internally inconsistent** on
corners (a classic ray-cast edge-inclusion artifact). Interior points all
match, so this only bites on-boundary cases.

Repro: `cd tests/integrationtest && diff r/spatial_compat.result r/spatial_compat.result.mysql`

## RESOLVED (2026-06, overnight Item 2)

Both divergences fixed; `diff r/spatial_compat.result r/spatial_compat.result.mysql`
is now empty (PoC output identical to MySQL 8.0.46):

1. ST_AsText now emits MySQL spacing (`POINT(0 0)`), via `mysqlWKT` post-processing
   in `pkg/expression/builtin_geo.go`.
2. ST_Within/ST_Contains (and the new ST_Intersects/ST_Equals/ST_Disjoint/
   ST_Touches/ST_Crosses/ST_Overlaps) now delegate to GEOS (libgeos, via
   `pkg/util/geos`), giving OGC boundary semantics that match MySQL. The
   hand-rolled ray-casting was removed.

## UPDATE: GEOS replaced by pure-Go simplefeatures

The relational predicates were migrated from go-geos (cgo/libgeos) to
github.com/peterstace/simplefeatures (pure Go, same OGC Simple Feature Access
spec as GEOS/JTS/PostGIS). The recorded result remains byte-identical to MySQL
8.0.46 (`diff r/spatial_compat.result r/spatial_compat.result.mysql` is empty),
so OGC boundary semantics still match — now with no cgo dependency.

## NEW (2026-06, broader corpus vs MySQL 9.7.1)

Found by the standalone `spatial-compat` harness in `tidb-dev-hacks`
(`spatial-compat/`), which replays a wider mysql-tester corpus against a MySQL
oracle and a PoC `tidb-server`. Golden recorded from MySQL 9.7.1; PoC at
`v9.0.0-beta.2.pre` (`49ddda26fb`). Accessors, planar `ST_Distance`, the
predicate set, `ST_Area`/`ST_Length`/`ST_Centroid`/`ST_IsValid`, the WKB
round-trip, and **spatial-index result-equivalence** all still match MySQL.
Three new divergences:

### 3. ST_Envelope ring orientation (compat)

`ST_AsText(ST_Envelope(POLYGON((0 0,10 0,10 10,0 10,0 0))))`:

| | ring |
|---|---|
| MySQL | `POLYGON((0 0,10 0,10 10,0 10,0 0))` (CCW from lower-left) |
| PoC   | `POLYGON((0 0,0 10,10 10,10 0,0 0))` (CW from lower-left) |

Same bbox (`ST_Equals` would agree), but the WKT text differs, so anything that
string-compares the envelope diverges. Emit the envelope ring CCW to match MySQL.

### 4. ST_AsGeoJSON formatting (cosmetic compat gap)

| | `ST_AsGeoJSON(POINT(5 5))` |
|---|---|
| MySQL | `{"type": "Point", "coordinates": [5.0, 5.0]}` |
| PoC   | `{"type":"Point","coordinates":[5,5]}` |

MySQL adds a space after `:`/`,` and renders integer coords as floats (`5.0`).
Same class as the ST_AsText spacing gap fixed above — the GeoJSON formatter needs
the same MySQL-matching pass.

### 5. SRID 4326 axis order (correctness/compat)

`ST_Distance_Sphere` over identical SRID-4326 literals diverges because MySQL
follows the EPSG:4326 authority order **(lat, long)** while the PoC reads
**(long, lat)**: London→Paris 343493.6 m (MySQL) vs 403518.2 m (PoC);
London→NYC 5570841.3 m vs 8246307.2 m. A real portability hazard for SRID-4326
data moved from MySQL; reconcile to the authority axis order (or document and
provide axis-order handling). Downstream: with MySQL lat,long literals near the
antimeridian/poles the PoC's long,lat reading puts coordinates out of range and
returns NULL distances where MySQL returns finite ones.

## NEW (2026-06, extended corpus — more findings)

Extending the corpus (relationship matrix, multi-geometries, collections) turned
up three more divergences and a set of missing functions.

### 6. ST_GeometryType collection name

MySQL returns `GEOMCOLLECTION`; the PoC returns `GEOMETRYCOLLECTION`. (All other
type names match.)

### 7. ST_Crosses / ST_Overlaps NULL semantics (compat)

MySQL returns NULL when the operand dimensions make the predicate undefined (OGC):
`ST_Overlaps` for different-dimension operands, `ST_Crosses` when the first
operand's dimension is not lower than the second's. The PoC returns a concrete 0/1
instead. E.g. `ST_Crosses(polygon, polygon)` and `ST_Crosses(polygon, line)` and
`ST_Overlaps(polygon, line)` are NULL in MySQL but 0/1 in the PoC.

### 8. Missing functions (next compat slice)

- `ST_Distance` rejects non-POINT arguments (errno 1105, "only POINT arguments are
  supported in the POC"); MySQL supports point-line, point-polygon, polygon-polygon.
- Not implemented (errno 1305): `ST_NumPoints`, `ST_StartPoint`, `ST_EndPoint`,
  `ST_PointN`, `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_NumGeometries`,
  `ST_IsSimple`.

Source: the standalone `spatial-compat` harness in `tidb-dev-hacks`; MySQL 8.0.46 /
8.4 / 9.7.1 are byte-identical on the corpus, so a single MySQL-9 golden is the
oracle. Spatial-index result-equivalence still holds.
