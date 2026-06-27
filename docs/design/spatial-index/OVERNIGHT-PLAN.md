# Spatial Index POC — Overnight Execution Plan

Living document. Updated as work proceeds. Companion to `PLAN.md` /
`PLAN-points-mvp.md` / `research.md` (on the `spatial-index-design` branch).

Branch: `spatial-index-poc`. PR: https://github.com/pingcap/tidb/pull/69475.

Starting point (already landed, 9 commits): points-only MVP, SRID 0 —
geometry types + `SRID`, EWKB + `ST_` builtins, `pkg/util/spatial` planar
coverer + `tidb_spatial_key`, `CREATE SPATIAL INDEX` (standalone + inline) with
a `SpatialIndexResolver` planner rule (covering-cell ranges + retained refine
filter), MySQL-compat integration test, Bazel/nogo clean.

## Roadmap / status

### Implemented (working today)

- **Types & storage**: geometry column types parse (`POINT`, `LINESTRING`,
  `POLYGON`, `MULTI*`, `GEOMETRYCOLLECTION`) + per-column `SRID`; EWKB storage
  (`<srid_le_u32><wkb>`), MySQL-compatible. Pure-Go geometry stack
  (`simplefeatures` + `golang/geo` S2) — no cgo/libgeos.
- **ST_ functions** (comprehensive, via pure-Go simplefeatures):
  - I/O: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`(`ST_AsWKB`),
    `ST_GeomFromGeoJSON`/`ST_AsGeoJSON`.
  - Accessors: `ST_X`/`ST_Y`, `ST_SRID` (getter + `ST_SRID(g, srid)` setter),
    `ST_GeometryType`, `ST_Envelope`, `ST_IsValid`, `ST_IsEmpty`, `ST_Dimension`,
    `ST_StartPoint`/`ST_EndPoint`, `ST_ExteriorRing`, `ST_NumInteriorRings`,
    `ST_NumPoints`, `ST_PointN`.
  - Measurement: `ST_Area`, `ST_Length`, `ST_Centroid`, `ST_Distance` (planar),
    `ST_Distance_Sphere` (4326).
  - DE-9IM predicates: `ST_Within/Contains/Intersects/Equals/Disjoint/Touches/
    Crosses/Overlaps/Covers/CoveredBy` (OGC-correct).
  - Geometry-returning builtins are typed `GEOMETRY`, so a plain B-tree
    functional index over them is correctly rejected.
- **Index**: `CREATE SPATIAL INDEX` (standalone + inline), `SHOW CREATE` renders
  `SPATIAL KEY`. Point index → scalar plain index on hidden `tidb_spatial_key`;
  general-geometry index → multi-valued index on `tidb_spatial_keys` (one row →
  many covering cells, IndexMerge); composite `(prefix…, geom)`. SRID dispatch:
  planar Morton quadtree (SRID 0) vs S2 (SRID 4326). `SpatialIndexResolver`
  auto-injects the covering predicate (cell ranges for a point; a `json_overlaps`
  over the query's covering cells for a general-geometry MVI — so a plain
  `ST_Within`/`ST_Contains` uses the MVI via IndexMerge with no manual
  `json_overlaps`) and keeps the ST_ predicate as a refine filter. Both index
  shapes carry the geometry's MBR as bbox index columns (`ST_X/ST_Y` for a point,
  `tidb_spatial_bbox(g,0..3)` for a general geometry) that prune covering false
  positives before the table lookup. Per-index cell-level tuning via index `COMMENT`.
- **Proven**: selectivity benchmark, MySQL-byte-identical compat test, pure-Go +
  Bazel/nogo build.

### Left for a complete MVP

- DONE: DML maintenance verified for point and general-geometry MVI (UPDATE/DELETE
  re-cover the index; ADMIN CHECK consistent) — `TestPOCSpatialDMLMaintenance`.
- The ST_ function surface is now broad (see Implemented); remaining accessors
  are niche (`ST_NumGeometries`/`ST_GeometryN`, `ST_InteriorRingN`,
  `ST_Perimeter`, GeoJSON options).
- MySQL error parity for the POC divergences (`ST_SRID`, constructors).

Done since the initial plan: the geometry-functional-index correctness bug is
fixed (geometry builtins typed `GEOMETRY`; the DDL guard rejects them).

### First release (MySQL surface + planner integration)

- **Geometry constructor functions** — `POINT()`/`LineString()`/`Polygon()`
  implemented (compose; build EWKB SRID 0), so `ST_Within(g, POLYGON(...))` works
  inline instead of only via `ST_GeomFromText('POLYGON((...))')`. They are a MySQL
  query-surface ergonomics item, not required for the index (the MVI operates on
  the stored EWKB however produced). Remaining: the Multi*/GeometryCollection
  constructors and the `ST_*FromText`/`*FromWKB` typed aliases.
- **Spatial cost/statistics + ANALYZE (top priority; ANALYZE fix landed).**
  Root cause found: `ANALYZE` produced a 0-bucket histogram for the point spatial
  index, pinning the cell-range estimate at the `estRows=1` floor. The point index
  is an expression index over virtual columns generated from the geometry column;
  v2 ANALYZE samples table rows and *recomputes* those columns, but the row sampler
  can't decode geometry (the `pkg/util/codec` hash/encode switches have no
  `TypeGeometry` case → sampled as NULL → every derived column NULL). A
  multi-valued index escapes this by being analyzed via an independent index-scan
  (it reads the materialized entries). **Fix:** route geometry-derived expression
  indexes to that same independent index-scan analyze (`getModifiedIndexesInfoForAnalyze`
  + `isGeometryDerivedIndex` in `planbuilder.go`). Result: the index now gets a real
  histogram (156 buckets) and `estRows` moves `1.00 → 159.9` vs actual 169
  (`TestPOCSpatialIndexAnalyzeStats`). Auto-selection now works hint-free: with
  real stats the optimizer picks the index for selective queries and falls back to
  a full scan when most of the table matches (radius sweep over the 10k-point grid:
  index at ~13/81/317 results, full scan at ~2.8k/10k). The POC tests now drop the
  `FORCE INDEX` hint where they ANALYZE (`TestPOCSpatialAutoSelect`,
  `TestPOCSpatialIndexEquivalence`). VERIFY (needs a real
  TiKV cluster): that the index↔full-scan **cut-off point is accurate** — the
  unistore cost model uses in-memory costs, so the true cross-over (random-read
  index lookups vs sequential full scan) can only be measured on real storage;
  confirm the optimizer switches at the selectivity where actual latency crosses
  over, and tune the cost factors if not. FUTURE OPTIMIZATION
  (low priority): independent `x`/`y` histograms so the bbox selectivity
  (`sel(x)·sel(y)`) is available — must be benchmarked across data distributions
  first (uniform vs clustered/skewed; the independence assumption breaks under
  correlation), and keep the scan-cost estimate (entries scanned) distinct from the
  output-cardinality estimate (entries matched) — the former drives single-table
  cost, the latter drives join planning.
- **Performance: bounding-box-in-index → coprocessor pushdown.** See
  `bbox-pushdown-design.md`. Layer A (bbox-in-index) is **implemented for the
  POINT index**: ST_X/ST_Y are carried as hidden index columns and the resolver
  injects an MBR-intersection filter that the optimizer applies during the index
  scan, before the table lookup. Verified pruning: on the radius benchmark it
  cuts **169 candidates → 121 table lookups** (48 pruned), results exact, invariant
  `lookups <= candidates`. CAVEATS learned: (1) the **latency** win is invisible
  in unistore — an in-memory "table lookup" is ~free, so the filter's overhead
  outweighs it; the real win is fewer **random-read I/Os**, only measurable on
  real storage (needs a real-TiKV/disk benchmark). (2) auto-selection regressed
  (see the stats item above). General-geometry (MVI) bbox columns are now
  implemented too (`tidb_spatial_bbox`, pruned on the cop Build side of the
  IndexMerge — e.g. 81 covering candidates → 25 lookups → 9 results in
  `TestPOCSpatialMVIAutoInjectAndBBox`). TODO: the
  point covering-index rewrite (`ST_Point(x,y)` refine on index data); Layer B
  exact-refine pushdown to TiKV. The **tipb protocol part is DONE** (`~/repos/tipb`
  branch `spatial-pushdown`: ScalarFuncSig 7100-7109). The **TiKV contract +
  correctness corpus** is in `tikv-pushdown-handoff.md` (ready for parallel TiKV
  work). Remaining: TiDB pb wiring (`setPbCode`/`PBToExpr`/allow-list/dep bump)
  + unistore round-trip validation (no perf, plumbing only).
- Stand up a **real-storage (TiKV/disk) benchmark** so the Layer A and pushdown
  latency wins can actually be measured (the mock store only shows lookup-count).
- `ST_Intersects`/`ST_Within`/`ST_Contains` → covering auto-rewrite (cell ranges
  for a point, `json_overlaps` for the MVI): DONE for all three.
- Dumpling/Lightning round-trips; KNN (`ORDER BY ST_Distance LIMIT k`).
- Docs, system variables, compatibility matrix.

### Future improvements & optimizations

- GEOS-gated advanced ops (`ST_Buffer`/`ST_Union`/`ST_Intersection`/
  `ST_ConvexHull`/`ST_Area`/`ST_Length`/`ST_Centroid`).
- (bbox-in-index + pushdown moved up to the primary performance item above; see
  `bbox-pushdown-design.md`.)
- Cost-based / adaptive cell-level selection; multi-resolution covering; true
  Hilbert ordering.
- Global spatial index for partitioned tables.
- Geography type + full SRID catalog/validation; 3D/measured (Z/M) coords.
- TiFlash/columnar spatial; coverer tuning for skewed data.

## Execution order (this run)

1. **Selectivity** — make the index actually prune, and measure it.
2. **ST_ functions, tiers P0–P1** (incl. `ST_Intersects`, `ST_Envelope`,
   `ST_GeometryType`, `ST_GeomFromWKB`/`ST_AsBinary`).
3. **SRID 4326 / S2 coverer** — `github.com/golang/geo/s2`, `ST_Distance_Sphere`,
   planner SRID dispatch, antimeridian/pole correctness.
4. **Full GEOMETRY support via MVI** — non-point columns (POLYGON/LINESTRING/…),
   one row → many covering cells through the multi-valued-index write path;
   generalized `ST_Intersects`/`ST_Contains`/`ST_Within`. Includes bbox-in-index
   (the MBR carried as hidden index columns; the value stays empty).
5. **Composite spatial index `(tenant_id, position)`** — prefix ordinary
   column(s) before the hidden cell-key column; planner matches
   `tenant_id = X AND <spatial predicate>`.

Then: **self-review → enumerate tests → benchmark → review again.**

## Decisions / constraints (from the user)

- Run autonomously; land each item as a tested commit; keep this file updated.
  Default to conservative choices.
- **go-geos / GEOS**: allowed in principle (TiDB already uses CGO; `CGO_ENABLED=1`
  for tidb-server). BUT `libgeos` is not installed locally and the Bazel/CI build
  won't have it, so adding go-geos now breaks the build. Therefore: implement
  P0–P2 predicates in **pure Go**; **defer** overlay/buffer (`ST_Union`/
  `ST_Intersection`/`ST_Difference`/`ST_Buffer`/`ST_ConvexHull`) which truly need
  GEOS, with a note that enabling them needs `libgeos-dev` + go-geos wired into
  WORKSPACE/CI.
- **Coprocessor pushdown of refine**: LOW priority. If reached, implement in
  **unistore** (the local test/verification engine) first.
- **Spatial cost/stats**: MEDIUM priority. Feasible via the cell-key (Hilbert/
  Morton) linearization; tie to ANALYZE round-trips.
- **Global spatial index for partitioned tables**: SKIP (edge case for now).
- **Dumpling/Lightning round-trips**: nice-to-have, only if time/tokens remain.

## ST_ function priority list

- **P0 (index-critical):** `ST_Distance` ✅(0)→sphere(#3); `ST_Contains`/
  `ST_Within` ✅(pt-in-poly)→general; **`ST_Intersects`**;
  `ST_GeomFromText`/`ST_AsText` ✅.
- **P1 (I/O + accessors):** `ST_GeomFromWKB`, `ST_AsBinary`/`ST_AsWKB`,
  `ST_GeometryType`, `ST_IsValid`, `ST_IsEmpty`, `ST_Envelope`,
  `ST_SRID(g,srid)` setter, `ST_AsGeoJSON`/`ST_GeomFromGeoJSON`.
- **P2 (DE-9IM family):** `ST_Equals`, `ST_Disjoint`, `ST_Touches`,
  `ST_Crosses`, `ST_Overlaps`, `ST_Covers`/`ST_CoveredBy`.
- **P3 (measurement/derived):** `ST_Length`/`ST_Area`/`ST_Centroid` DONE natively
  via simplefeatures (not GEOS-gated, contrary to the original assumption);
  `ST_StartPoint`/`ST_EndPoint`/`ST_ExteriorRing`/`ST_NumInteriorRings`/
  `ST_NumPoints`/`ST_PointN` DONE. Remaining pure-Go-able: `ST_Perimeter`,
  `ST_NumGeometries`/`ST_GeometryN`. Still GEOS-gated:
  `ST_Buffer`/`ST_ConvexHull`/overlay.

## Known limitations (POC)

- `POLYGON()` / `LINESTRING()` / etc. parse as function calls but are not
  implemented (resolve to "function does not exist"); `ST_SRID` on a
  non-geometry argument returns a generic "invalid geometry value" error rather
  than MySQL's `ER_CANNOT_CONVERT_STRING`. The internal mysql-test expectations
  are aligned to this POC behavior on a separate tidb-test branch.
- FIXED (TypeGeometry type-switch audit): ~28 operations exercised (GROUP BY,
  hash/merge join, DISTINCT, ORDER BY, UPDATE/DELETE/REPLACE, window, …) — all work
  except two gaps, now fixed: `INSERT ... SELECT` nulled the geometry
  (`chunk.Row.GetDatum` had no `TypeGeometry` case) and `UNION` asserted in the
  cast-string flen setup (`builtin_cast.go`). Both now read geometry as a binary
  string. Regressions in `TestPOCGeoFunctions`. (The `pkg/util/codec` hash-encode
  default for type 255 is *not* reached — hash agg/join on geometry work.)
- FIXED (CI): `CREATE SPATIAL INDEX` was rejected under the default (strict) config
  (`allow-expression-index` off) because the Layer A bbox columns use `ST_X`/`ST_Y`
  (point) and `tidb_spatial_bbox` (general geometry), which were not in
  `GAFunction4ExpressionIndex` — only `tidb_spatial_key`/`keys` were. Added the
  three; the index now creates regardless of config. This had been failing 9 of the
  spatial unit tests on CI since Layer A (`varsutil.go`).

## Deferred / backlog (not this run unless time remains)

- KNN operator (`ORDER BY ST_Distance LIMIT k` without a radius) via
  expanding-ring search.
- Coprocessor pushdown (unistore first).
- Spatial stats/cost + ANALYZE.
- Dumpling/Lightning round-trips; MySQL GIS test-suite subset.
- Global partitioned index (skipped).
- GEOS-backed overlay/buffer.

## Update (user, mid-run)

- **libgeos-dev is now installed** → use GEOS (github.com/twpayne/go-geos) for
  OGC-correct geometry instead of hand-rolled go-geom ray-casting. This becomes
  the engine for the Item-2 predicates AND fixes the boundary bug below. Watch
  the Bazel/CI build (cgo + system libgeos in the sandbox) — may need WORKSPACE/CI
  wiring; if it can't link there, gate or document.
- **Two MySQL-8.0.46 compat divergences found** (see
  `tests/integrationtest/r/spatial_compat.NOTES.md`), both in the geometry-function
  layer — distances/accessors/SRID and BOTH spatial-index result sets match MySQL:
  1. `ST_AsText` spacing: PoC `POINT (0 0)` vs MySQL `POINT(0 0)` (go-geom
     wkt.Marshal default) — fix the formatter.
  2. Boundary containment: PoC `ST_Within` is inconsistent on polygon corners
     ((0,0)→1 but (10,10)→0; MySQL→0 for both, OGC boundary-not-within). Ray-cast
     edge artifact. GEOS fixes this for free.
  → Folded into Item 2: do it on GEOS, then re-record compat and diff vs MySQL.

## Build & CI status (end of run)

- `go build ./pkg/...` and all POC `go test` pass with CGO on (libgeos present).
  Integration `spatial_compat` is byte-identical to MySQL 8.0.46.
- **cgo/libgeos removed (resolved):** the GEOS predicates were migrated from
  go-geos (cgo/libgeos) to github.com/peterstace/simplefeatures (pure Go, same
  OGC spec). The whole spatial stack now builds with `CGO_ENABLED=0`, and the
  compat suite is still byte-identical to MySQL 8.0.46. The earlier Bazel
  libgeos cc-toolchain blocker is gone; Bazel now only needs the pure-Go deps
  (simplefeatures, golang/geo, go-geom) added to DEPS.bzl as proxy-fetch entries
  like the go-geom family already are (commit c3a546775f) — no cc wiring. That
  DEPS.bzl/bazel_prepare step is the only remaining CI follow-up.
- Pre-existing race (not ours): tidb-server startup races in grpc `SetLoggerV2`
  vs the channelz goroutine (logutil.InitLogger / metrics) appear under the race
  detector on master too.

## Self-review outcome (Item 6)

Two independent review passes on the spatial diff.
- Pass 1 found 1 HIGH (unbounded general-geometry MVI covering -> INSERT-time
  blowup + uint32 capacity overflow), 2 MEDIUM (GEOS panic on invalid geometry;
  ST_Distance_Sphere missing SRID validation -> silent mixed-SRID wrong result),
  4 LOW. It confirmed the core planar/S2 covering math, mergeRanges/addOne, and
  mysqlWKT are correct (no false negatives).
- Fixed in commit 662ea363be: the HIGH (cell cap + uint64), both MEDIUMs (geos
  recover; sphere SRID check + planner SRID-scheme gate), and the empty-range
  guard. Added regression tests.
- Pass 2 verified the fixes are correct and complete with no new high-severity
  bugs; remaining notes are non-correctness (per-row coverer alloc; ST_Distance_
  Sphere intentionally SRID-4326-only in the POC).

## Progress log

(Newest first. Each item: status, commits, surprises, what was verified.)

- [DONE] Item 1 — Selectivity.
  - Per-index cell tuning via index COMMENT `spatial:level,minX,minY,maxX,maxY`,
    baked into `tidb_spatial_key(geom, level, minX, minY, maxX, maxY)` args as the
    single source of truth; planner re-derives the same coverer from the hidden
    column's generated expression.
  - Added a max-cells cap (`coverLevelFor`) so CoverRect emits a bounded number
    of ranges (was 1228 for a fine-level query → now ≤ ~a few dozen) — the huge
    OR was breaking index range building.
  - Bug found+fixed: the re-added hidden column lacked `VirtualExpr`, so the
    optimizer pushed the covering filter to the coprocessor, which can't evaluate
    `tidb_spatial_key` on the unstored virtual column → 0 rows when a full scan
    was chosen. Setting `VirtualExpr` keeps the filter at the TiDB root. Surfaced
    only when a prior query loaded stats and flipped index→full-scan.
  - SHOW CREATE now renders `SPATIAL KEY name (col) COMMENT 'spatial:...'`
    (params in the comment, round-trips) instead of leaking the args.
  - Verified: `TestPOCSpatialSelectivity` — index scans 169/10000 rows (1.7%) for
    75 matches (2.25x false-positive ratio), identical rows vs full scan. Note:
    automatic index *selection* over a full scan needs spatial stats/cost (Item:
    spatial costs/stats); test forces the index to prove pruning.
