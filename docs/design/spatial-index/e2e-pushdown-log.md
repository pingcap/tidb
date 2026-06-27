# Spatial Index — Real-Cluster E2E & Pushdown Benchmark Log

Living log of the overnight real-TiKV end-to-end run: verify the coprocessor
pushdown end to end (custom TiDB + custom TiKV), find/fix issues, and measure the
benefits (bbox-in-index pruning, index-vs-full-scan, refine pushdown) on real
storage — where unistore's in-memory costs hide them.

## Setup

- **TiDB**: merged `spatial-index-poc` (index: Layer A bbox + MVI + ANALYZE stats +
  GA fix + constructors) ⊕ `spatial-pushdown` (Layer B pb wiring). Merge commit
  `cdd8b9b230`. Local tipb `replace => /home/mattias/repos/tipb` (branch
  `spatial-pushdown`, ScalarFuncSig 7100-7109).
- **TiKV**: `mjonss/tikv` branch `spatial-copr-pushdown` (`impl_spatial.rs` native
  DE-9IM evaluator; tipb = `mjonss/tipb` `spatial-pushdown`). Release build.
- **Cluster**: `tiup playground` with `--db.binpath` / `--kv.binpath`.
- Box: 16 cores, 60 GB RAM.

## Plan

1. Bring up the cluster with custom binaries; confirm health + that TiKV is the
   spatial build.
2. Correctness: for ST_Within / ST_Intersects / ST_Distance(_Sphere), confirm
   real-TiKV results == full-scan/unistore results (point + general geometry,
   SRID 0 + 4326).
3. Pushdown verification: confirm the ST_ refine is evaluated at TiKV (cop), not
   the TiDB root — via EXPLAIN (cop Selection), `coprocessor` slow-log / metrics,
   and a correctness cross-check with pushdown forced off.
4. Benchmark: latency for (a) full scan, (b) index + root refine, (c) index +
   bbox-in-index pruning, (d) index + bbox + cop pushdown — across selectivities
   and data distributions; verify the cost-model cut-off point.
5. Log + fix every issue found.

## Summary of this run

End-to-end coprocessor pushdown **works and is correct** on a real cluster (custom
TiDB ⊕ pushdown + custom TiKV Rust evaluator), and the benefits are **measured**.

- **3 bugs found + fixed** (all committed): (1) TiKV `FieldTypeTp::Geometry →
  EvalType::Bytes` — without it *all* spatial pushdown errored; (2) `ST_Crosses`
  computed symmetrically vs MySQL's dimension gate — fixed in **both** TiDB (Go) and
  TiKV (Rust); (3) `ST_Envelope` ring order + missing `ST_Longitude`/`ST_Latitude`
  (MySQL compat).
- **Correctness:** all 8 DE-9IM predicates match MySQL over **3000** random
  geometry pairs (points/lines/polygons/multi*); the **index path is sound** —
  FORCE INDEX == full scan == MySQL over 3000 geoms × 60 random windows (bbox/cell
  prune never drops a true match). Compat battery 40/44 (remaining: AsGeoJSON
  whitespace/float format, 1 m sphere rounding — cosmetic).
- **Performance:** pushdown ~50–100× (3.3 s → 34 ms at 5M rows; 208× fewer rows
  shipped); index+bbox keeps latency flat in selectivity while a full scan grows.
- **Known limitation:** 4326 predicates are planar in the refine (geodesic only in
  the S2 covering) → small boundary divergence from MySQL's geodesic 4326.

Cluster left running: TiDB `127.0.0.1:4000` (tag `spatial`). MySQL compare box:
docker `mysql-compat` on `127.0.0.1:3307` (root/root).

## Status / timeline

- Builds done; cluster up; correctness + benchmark + bug fixes complete (above).

## Issues found / fixed

### TiDB-vs-MySQL 8.0.46 compat (unistore, 43-query battery)

37/43 match (all DE-9IM predicates, constructors, ST_X/Y/SRID/Area/Length/Distance/
Centroid/PointN/etc., even ST_Within on a boundary point). Divergences:

1. **ST_Envelope ring order** — TiDB emits the bbox ring clockwise
   `POLYGON((0 0,0 3,2 3,2 0,0 0))`; MySQL emits it CCW
   `POLYGON((0 0,2 0,2 3,0 3,0 0))` (OGC exterior orientation). Same box, wrong
   winding. → FIX.
2. **ST_AsGeoJSON formatting** — TiDB `{"type":"Point","coordinates":[1,2]}` vs
   MySQL `{"type": "Point", "coordinates": [1.0, 2.0]}` (MySQL adds spaces after
   `:`/`,` and renders coords as floats). Semantically equal, format differs. → FIX.
3. **ST_Longitude / ST_Latitude missing** — MySQL has them (4326 accessors); the
   POC doesn't (falls through to "function does not exist"). → IMPLEMENT.
4. **ST_Distance_Sphere** off by 1 m at 157 km (157249 vs 157250) — formula/rounding,
   0.0006% (same 6370986 m radius). Negligible; left as-is.
5. **Error parity** — invalid WKT: TiDB err 1105 "invalid WKT syntax" vs MySQL 3037
   "Invalid GIS data"; SRID mismatch: TiDB 1105 vs MySQL 3033. Different codes. Low
   priority.
6. **Degenerate LINESTRING validation** — TiDB rejects a linestring whose two
   endpoints coincide ("only one distinct XY value"); MySQL accepts it. Minor
   strictness difference (surfaced while fuzzing).

**After fixes (#1 ST_Envelope, #3 ST_Longitude/Latitude, plus the ST_Crosses gate):
re-ran the battery against the cluster's rebuilt TiDB — 40/44 MATCH, 0 TiDB-errors;
only AsGeoJSON formatting and the 1 m sphere rounding remain (both cosmetic).**
Larger fuzz (800, then **3000** random pairs incl. multipoint/multilinestring):
**all 8 predicates match** the Rust cop eval vs MySQL on every run — ST_Crosses
included (was 16 mismatches). Strong evidence the native DE-9IM evaluator is correct.

### Real-cluster pushdown (custom TiDB + custom TiKV)

**BUG #1 (TiKV, FIXED): `Unsupported type: Geometry` — the pushed-down ST_Within
errored at TiKV.** The TiDB plan was correct (EXPLAIN: `Selection(Probe)
st_within(p, <const>)` at `cop[tikv]`, plus the bbox prune `ge/le(_v$_sidx_2,..)`
at cop), but the query failed:
`expr_builder.rs:311 Invalid st_within signature: ... function.rs:270 Unsupported
type: Geometry`. Root cause: `EvalType::try_from(FieldTypeTp)` in
`tidb_query_datatype/src/def/eval_type.rs` mapped the string/blob types to
`EvalType::Bytes` but let `FieldTypeTp::Geometry` (0xff) fall to the `_ =>`
UnsupportedType arm — so neither the geometry column nor the constant polygon could
be built into an RPN node. Fix: map `Geometry => EvalType::Bytes` (it is EWKB bytes;
`impl_spatial.rs` decodes it). `impl_spatial.rs` itself is fine (args are `BytesRef`,
`decode_ewkb` strips the 4-byte SRID prefix). Rebuilding TiKV to retest.

**→ FIXED & VERIFIED:** after the EvalType fix + TiKV rebuild, the pushed-down
`ST_Within` evaluates natively at TiKV and returns the exact known answer
(`33,34,43,44` = the 4 grid points strictly inside the query polygon). Coprocessor
pushdown now works end to end (custom TiDB + custom TiKV Rust evaluator).

### BUG #2 (FIXED, TiDB + TiKV): ST_Crosses symmetric vs MySQL's dimension gate

Found by **fuzzing** the cluster (Rust cop) vs MySQL over 400 random SRID-0
geometry pairs: 7/8 predicates matched; **ST_Crosses had 16 mismatches**, all
`ST_Crosses(POLYGON, LINESTRING)` where TiDB returned 1 and MySQL returned NULL.
MySQL only defines ST_Crosses when **dim(g1) < dim(g2)** or **both are lines**;
every other combination returns NULL (verified matrix: pt/pt, line/pt, poly/line,
poly/poly, poly/pt → NULL; pt/line, line/line, line/poly, pt/poly → computed). The
`geo` crate (TiKV) and `geomrel` (TiDB) both compute crosses symmetrically. Fix:
gate ST_Crosses to that rule on **both** sides — `builtinGeomRelSig.evalInt`
(returns NULL) and `impl_spatial.rs` `st_crosses` (returns `Ok(None)`), with a
shared `geom_dimension` helper in Rust. Re-fuzzing after the TiKV rebuild to
confirm parity.

### Predicate correctness — cluster (Rust cop) vs MySQL 8.0

Table of 24 diverse geometry pairs (point/line/polygon; interior, boundary, corner,
touching edges, disjoint, equal, holes, collinear overlap, multipoint); each
predicate run as `WHERE PRED(g1,g2)` (forces cop pushdown — confirmed `cop[tikv]
st_within`). **All 8 MySQL-comparable predicates match exactly** (ST_Within,
ST_Contains, ST_Intersects, ST_Equals, ST_Disjoint, ST_Touches, ST_Crosses,
ST_Overlaps). ST_Covers/ST_CoveredBy can't be compared — MySQL 8.0 lacks them
(PostGIS extensions); TiDB evaluates them. → the native Rust DE-9IM evaluator is
correct on real storage.

### Extended pushdown paths (cluster vs MySQL)

- **General-geometry MVI + ST_Intersects** (400 unit-cell polygons): EXPLAIN shows
  the `json_overlaps(tidb_spatial_keys(g),…)` auto-injection → IndexMerge → bbox
  prune at cop → `st_intersects(…)` `Selection(Probe)` at `cop[tikv]`. Count = 25,
  **matches MySQL**.
- **SRID 4326 + ST_Within** (S2 path): EXPLAIN shows S2 cell ranges + lat/long bbox
  prune + `st_within` at cop. Count = 45 (POC) vs **50 (MySQL)** — a divergence.

**KNOWN LIMITATION (4326 predicates are PLANAR, not geodesic).** The 5 differing
points sit exactly on the query polygon's straight (planar) edge (coord0 = −5);
MySQL returns `ST_Within` = true for them because it evaluates 4326 predicates
**geodesically** — the great-circle edge between two equal-latitude vertices bulges
off the parallel, so those boundary points fall inside MySQL's polygon. The POC's
S2 cell *covering* is geodesic-aware, but the *refine* (`impl_spatial.rs` / Go
simplefeatures) is planar, so results diverge at/near 4326 boundaries (small at
city scale, growing with region size). Making 4326 predicates exact needs a
geodesic predicate library on both sides — a real feature, out of POC scope.
Related: the POC's axis convention for 4326 is coord0 = longitude (for S2/cap),
whereas MySQL 4326 is lat-long (coord0 = latitude); ST_X/ST_Longitude/ST_Latitude
were aligned to MySQL, but the design should settle the index/covering axis
convention. SRID 0 (planar) has no such divergence.

## Benchmark results

Real cluster (1 PD / 1 TiKV / 1 TiDB, custom binaries). Table `g`: **499,849**
points (707×707 grid, SRID 0), spatial index, ANALYZEd. Query:
`SELECT count(*) FROM g [idx] WHERE ST_Within(p, <box>)`. Pushdown toggled via
`mysql.expr_pushdown_blacklist` (st_within). Median of 5, warm cache.

| selectivity | matching | full+root | full+cop | index+root | index+cop |
|---|---|---|---|---|---|
| 0.02% | 81     | 410 ms | 34 ms | 35 ms | 34 ms |
| 0.16% | 729    | 360 ms | 36 ms | 40 ms | 38 ms |
| 0.5%  | 2 401  | 365 ms | 37 ms | 48 ms | 40 ms |
| 2%    | 9 801  | 392 ms | 42 ms | 81 ms | 36 ms |
| 8%    | 39 601 | 493 ms | 63 ms | 184 ms | 37 ms |

- **full+root** = full scan, refine at TiDB (pre-pushdown behavior): a Selection
  carrying ST_Within can't push, so TiKV ships **all 499,849 rows** to TiDB and
  TiDB evaluates ST_Within on every one. 360–490 ms — the floor we were stuck at.
- **full+cop** = full scan, refine at TiKV (pushdown): **~10× faster** (34–63 ms).
  EXPLAIN ANALYZE @0.5%: rows shipped TiKV→TiDB drop from **499,849 → 2,401**
  (**208× fewer**); the cop `Selection` does the ST_Within filtering natively.
- **index+root**: index cell-range + bbox prune at cop, ST_Within refine at root —
  degrades with selectivity (more bbox-surviving candidates shipped to root to
  refine: 35 ms → 184 ms).
- **index+cop** = the full stack (index + bbox prune + cop refine): **flat ~35 ms**
  across all selectivities — the index bounds the scan and the cop refine bounds
  what's shipped. Best in every row.

**Headline:** coprocessor pushdown is the decisive win — **~10× latency, ~200×
fewer rows shipped** — because without it a spatial `Selection` ships the whole
table to TiDB. The index+bbox adds a further win at higher selectivity (index+cop
37 ms vs full+cop 63 ms @8%). At 500k rows warm-cache the index-vs-full-scan gap is
small for low selectivity (a cop-filtered full scan of 500k is already ~34 ms); the
index's scan/IO reduction widens at larger scale (below).

### Same benchmark at 5M rows (2236×2236 grid) — index benefit now clear

| selectivity | matching | full+root | full+cop | index+cop |
|---|---|---|---|---|
| 0.002% | 81      | 3263 ms | 34 ms  | 36 ms |
| 0.05%  | 2 401   | 3294 ms | 39 ms  | 40 ms |
| 0.2%   | 9 801   | 3327 ms | 44 ms  | 40 ms |
| 0.8%   | 39 601  | 3442 ms | 65 ms  | 39 ms |
| 3.2%   | 159 201 | 3950 ms | 141 ms | 44 ms |

- **Pushdown** scales with the win: full+root is now **3.3–4.0 s** (ships all 5M
  rows to TiDB), full+cop **34–141 ms** → **~50–100×**.
- **Index** benefit is now unambiguous: **full+cop grows with selectivity**
  (34 → 141 ms — it always scans all 5M at the cop) while **index+cop stays flat
  ~40 ms** at every selectivity (the cell-range + bbox bound the scan to the query
  region). At 3.2%: index+cop 44 ms vs full+cop 141 ms = **3.2×**.
- The two compose: index+cop is **~75–90× faster than the pre-pushdown floor**
  (full+root) and flat in selectivity — the intended end state.

