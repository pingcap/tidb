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

## Progress log

(Newest first. Each item: status, commits, surprises, what was verified.)

- [in progress] Item 1 — Selectivity. Started.
