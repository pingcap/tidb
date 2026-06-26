# Spatial Index POC — Overnight Execution Plan

Living document. Updated as work proceeds. Companion to `PLAN.md` /
`PLAN-points-mvp.md` / `research.md` (on the `spatial-index-design` branch).

Branch: `spatial-index-poc`. PR: https://github.com/pingcap/tidb/pull/69475.

Starting point (already landed, 9 commits): points-only MVP, SRID 0 —
geometry types + `SRID`, EWKB + `ST_` builtins, `pkg/util/spatial` planar
coverer + `tidb_spatial_key`, `CREATE SPATIAL INDEX` (standalone + inline) with
a `SpatialIndexResolver` planner rule (covering-cell ranges + retained refine
filter), MySQL-compat integration test, Bazel/nogo clean.

## Execution order (this run)

1. **Selectivity** — make the index actually prune, and measure it.
2. **ST_ functions, tiers P0–P1** (incl. `ST_Intersects`, `ST_Envelope`,
   `ST_GeometryType`, `ST_GeomFromWKB`/`ST_AsBinary`).
3. **SRID 4326 / S2 coverer** — `github.com/golang/geo/s2`, `ST_Distance_Sphere`,
   planner SRID dispatch, antimeridian/pole correctness.
4. **Full GEOMETRY support via MVI** — non-point columns (POLYGON/LINESTRING/…),
   one row → many covering cells through the multi-valued-index write path;
   generalized `ST_Intersects`/`ST_Contains`/`ST_Within`. Includes bbox-in-value.
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
- **P3 (measurement/derived — GEOS-gated, deferred):** `ST_Length`, `ST_Area`,
  `ST_Centroid`, `ST_Perimeter`, `ST_PointN`/`ST_NumPoints`/ring accessors
  (pure-Go-able), `ST_Buffer`/`ST_ConvexHull`/overlay (GEOS).

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
