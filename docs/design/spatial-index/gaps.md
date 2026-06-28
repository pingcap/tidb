# Spatial POC — Compatibility & Index-Utilization Gaps

What's missing for (1) MySQL compatibility on the two implemented SRIDs (0 planar,
4326 geographic), and (2) fully exploiting the spatial index. Companion to
`review-plan.md` (milestones), `mysql-function-catalog.md` (function surface),
`storage-format.md` (refine-CPU levers), `pushdown-contract.md` (cop pushdown), and
`OVERNIGHT-PLAN.md` round-2 items #1/#2/#7/#8.

`✓code` = verified in source this audit; otherwise from design notes.

## Part 1 — MySQL compatibility gaps (SRID 0 & 4326)

### SRID 4326 (geographic) — the larger divergences
- **Axis order** — ✅ **FIXED**. 4326 now uses MySQL's EPSG axis order
  **(latitude, longitude)**: the S2 covering, `ST_Distance_Sphere`, and the cap/rect
  cover all treat the first coordinate as latitude (the accessors already did).
  Verified vs MySQL 9.7 (`ST_Latitude`/`ST_Longitude`/`ST_Distance_Sphere`).
  `TestPOCSpatial4326Axis`. Full convention (incl. GeoJSON/WKB, vs PostGIS): see
  [`axis-order.md`](axis-order.md).
- **Planar refine** — ✅ **point-in-polygon now geodesic** (S2) for the 4326 region
  predicates, matching MySQL where the planar refine diverged (`geomrel/geodesic.go`;
  `TestGeodesic4326PointInPolygon` proves a geodesic-inside / planar-outside point).
  Follow-ups: polygon/polygon + Touches/Crosses/Overlaps stay planar; the index
  *covering* still uses the vertex lat/lng bbox, so a pathological large polygon whose
  geodesic edge bulges past its vertex bbox could miss index candidates (the full scan
  stays correct); and TiKV's geo-crate refine is still planar, so real-TiKV 4326-region
  pushdown should be gated off until it's geodesic too.
- **`ST_Distance` on 4326** `✓code` — hard-restricted to SRID 0
  (`builtin_geo.go:341`); MySQL returns the **geodesic distance in meters**
  (ellipsoidal). `ST_Distance_Sphere` uses a sphere, not MySQL's ellipsoid, so the
  meters differ slightly too.
- **Geodesic `ST_Length` / `ST_Area`** for 4326 (MySQL → meters / m² on the
  ellipsoid) — not done.
- **Coordinate-range validation** — ✅ **FIXED** in `ST_GeomFromText` (4326 lat ∉
  [−90,90] / lng ∉ [−180,180] errors, matching MySQL); other ingest paths
  (`ST_GeomFromWKB`/GeoJSON/constructors) are a follow-up.

### SRID 0 (planar) — mostly compatible; remaining gaps
- **`ST_Distance` non-point** — ✅ **FIXED** for SRID 0 (any geometry types, via
  `geom.Distance`). 4326 `ST_Distance` (ellipsoidal meters) still a follow-up.
- **Empty geometry** — ✅ **FIXED**: a spatial predicate with an empty operand is now
  NULL (matching MySQL).
- Boundary/DE-9IM semantics should now be OGC-correct (simplefeatures); worth a fresh
  diff vs MySQL.

### Both SRIDs
- **Function tail** — ~65 MySQL functions remain (see `mysql-function-catalog.md` M3).
  ✅ **Started**: the typed WKT constructors `ST_PointFromText` / `ST_LineFromText` /
  `ST_LineStringFromText` / `ST_PolyFromText` / `ST_PolygonFromText`
  (`TestPOCSpatialTypedConstructors`). Remaining: the `*FromWKB` variants, the 9 MBR
  predicates, processing (`ST_Buffer`/`Union`/`Intersection`/`Difference`/
  `SymDifference`/`ConvexHull`/`Simplify`/`Validate`/`MakeEnvelope`/`SwapXY`/
  `LineInterpolate*`/`PointAtDistance`/`Collect`), geohash (4), niche accessors
  (`IsSimple`/`IsClosed`/`InteriorRingN`/`GeometryN`/`NumGeometries`),
  Multi*/GeometryCollection constructors.
- **`ST_Transform`** (cross-SRID reprojection) — absent.
- **Error parity** — MySQL-specific codes/messages (the PoC uses "…in the POC").
- **SRS catalog** — `information_schema.ST_SPATIAL_REFERENCE_SYSTEMS`,
  `CREATE/DROP SPATIAL REFERENCE SYSTEM` (minor for just 0/4326).

## Part 2 — Index-utilization gaps

### A. Query shapes the index can't serve (fall back to scan/sort)
- **KNN** `ORDER BY ST_Distance(g, const) LIMIT k` — ✅ **path B implemented**: a
  Sort/TopN whose ByItems is `ST_Distance(col, const)` over a SRID-0 point index is
  rewritten to `Point(ST_X, ST_Y)` (from the in-index bbox columns), so the top-k is
  computed **index-only** — `IndexFullScan`, no table read, no EWKB decode — and matches
  the full-scan baseline exactly (`TestPOCSpatialKNN`). It is still an O(n) index scan +
  TopN, **not** spatial pruning. Path A (a best-first / expanding-cell physical operator,
  O(k log n), like the vector-ANN feature) remains future work for the real latency win.
- **Spatial joins** `ST_Intersects(t1.g, t2.g)` `✓code` — the resolver needs a
  **constant** query geometry to compute covering cells, so column↔column joins can't
  be index-accelerated → cross-join + filter. An index-nested-loop spatial join (cell
  lookup per outer row) would be a large win.
- **Point covering-index** — point queries still do an `IndexLookUp` (random table
  probe + EWKB decode) although `ST_X`/`ST_Y` are *in the index*. Index-only refine
  removes the dominant read cost (random I/O) + the decode. (Round-2 item #8.)
  **Implemented** for SRID-0 points (`TestPOCSpatialPointCoveringIndex`); 4326 is a follow-up.

### B. Predicates not index-eligible
- **MBR predicates** — not implemented, so the natural bbox-index users get no index
  path.
- **`Disjoint` / negated predicates** — inherently the complement of a region → no
  covering range (full scan; largely unavoidable).

### C. Index used, but doing more work than needed
- **Morton vs Hilbert (SRID 0)** — Morton's poorer locality → more disjoint key
  ranges + more covering false positives → more rows refined. Hilbert tightens it
  (`storage-format.md`) → lower latency/CPU.
- **Refine CPU** — the simplefeatures relate-internal WKB re-parse (~14%), the per-row
  constant re-decode, and decode-once on the write path (the pprof findings in
  `storage-format.md`).
- **Statistics quality** — cost-based selection works at the extremes, but
  mid-selectivity plan choice depends on spatial cardinality estimation accuracy; poor
  estimates → mis-plans.
- **4326 pushdown trade-off** — 4326 region predicates push to the cop (planar both
  sides) today. Adding geodesic refine *forces 4326 pushdown off* (cop stays planar)
  → more rows shipped to root → higher latency. Making **TiKV's refine geodesic**
  gives both correctness and the pushdown latency win.

### D. Coverage gaps
- **Partitioned tables** — unverified that the hidden-index + resolver compose with
  partitioning; if not, partitioned spatial tables lose the index. (Round-2 item #5,
  not in the implement set.)
- **TiFlash / OLAP** — no columnar spatial index, so analytic spatial scans
  (aggregations over large regions) can't use one.
- **Other projected SRIDs** — only 0/4326 are indexable (the 4-site SRID gating,
  round-2 item #7).

## Priority (highest value first)
- **Correctness:** 4326 **axis order**, then **geodesic refine** (#2).
- **Utilization:** **KNN** (#1), **point covering-index** (#8), **spatial joins**.
