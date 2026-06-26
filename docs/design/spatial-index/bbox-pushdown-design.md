# Spatial index: bounding-box-in-index + coprocessor pushdown

Status: design / planning. Companion to `OVERNIGHT-PLAN.md`. Goal: cut the
dominant per-query cost of the spatial index (table lookups + root-side refine)
and push the refine to the coprocessor (TiKV), with a TiDB-only first layer that
already delivers most of the win.

## 1. Problem: where the time goes today

A spatial query (`WHERE ST_Within(g, <const>)`, etc.) plans as an `IndexLookUp`:

```
IndexRangeScan(cell_key ∈ covering ranges)     -- cheap, at the index
  → TableRowIDScan (fetch the geometry column)  -- ONE random read PER CANDIDATE  ← dominant cost
    → root Selection: ST_Within(g, <const>)     -- exact refine, needs the full geometry
```

The covering-cell predicate is coarse: it returns a *candidate superset*. Every
false positive is **table-looked-up and decoded** before the root refine
discards it. The current POC measures ~1.6x candidates/result for a tuned point
index — i.e. ~38% of the random reads are wasted — and the ratio grows as the
covering gets coarser or geometries get larger.

Two distinct, complementary optimizations:

| Layer | Cuts | Helps without TiKV? | Needs tipb/tikv? |
| --- | --- | --- | --- |
| **A. bbox-in-index** | **table lookups** (dominant) | **yes** (single-node, unistore, TiKV) | **no** — numeric comparisons already push down |
| **B. exact-refine pushdown** | network + root CPU | no (in-process = no network) | yes |

Layer A is the 80/20 and is the foundation; layer B is the remaining, TiKV-only
increment. We aim for B but land A first because A is where the measurable
single-node speedup comes from.

## 2. Layer A — bounding box (and point coords) in the index

### Mechanism

Extend the hidden generated columns the spatial index covers so the index entry
carries the geometry's MBR (and, for points, its exact coordinates):

- **Point index** (scalar): `INDEX (tidb_spatial_key(p), x, y)` where `x`,`y` are
  hidden generated columns `ST_X(p)`, `ST_Y(p)`. The MBR of a point is the point.
- **General-geometry index** (MVI): `INDEX ((cast(tidb_spatial_keys(g,…) as char
  array)), minx, miny, maxx, maxy)` with the four bbox columns
  (`tidb_spatial_bbox(g, 0..3)`) generated from `g` (one MBR per row, replicated
  across the row's cell-key array entries).

The MBR therefore lives in the index **key** (TiDB encodes every secondary-index
column into the key; the value holds only the row handle). It is *not* an
"include columns in the value" feature — TiDB has no such mechanism. The bbox
columns trail the cell key, so they cannot narrow the range scan (range building
stops at the first non-equality column, the cell key) but are decoded from the
key and applied as index filters before the table lookup.

The `SpatialIndexResolver` then injects, alongside the existing cell-range
predicate, a **bbox-intersection filter on the index columns**:

```
minx <= q_maxx  AND  maxx >= q_minx  AND  miny <= q_maxy  AND  maxy >= q_miny
```

where `(q_minx..q_maxy)` is the MBR of the query constant (already computed by
`EWKBBounds`). For a point index the same filter degenerates to
`x BETWEEN q_minx AND q_maxx AND y BETWEEN q_miny AND q_maxy`.

### Why this is a coprocessor pushdown for free

The bbox filter is a conjunction of **plain numeric comparisons on index
columns**. TiDB already pushes such index filters to the coprocessor and applies
them *during the index scan, before the table lookup*. So with no tipb/tikv
change, layer A:

- prunes false positives whose MBR cannot intersect the query, and
- does it **at the storage node, before the random read** — eliminating the
  wasted table lookups, which is the dominant cost.

This is the classic R-tree "filter on MBR, then fetch" pattern, expressed in
TiDB's index-filter machinery.

### Point covering-index refinement (optional, bigger win for points)

For a point index that carries `(x, y)`, the resolver can rewrite the exact
refine to run on **index data only**, reconstructing the point without a table
lookup: `ST_Within(p, q)` → `ST_Within(ST_Point(x, y), q)`. Combined with the
bbox filter this makes the spatial predicate a *covering* index access — zero
table lookups for filtering; the handle is fetched only for surviving rows to
return their other columns. (Requires `ST_Point`/constructor support; tracked
separately.)

## 3. Which functions / lookup types / queries this helps

### Predicates (the refine the bbox prunes for)

bbox pruning works for any predicate whose **true set is contained in "MBR
intersects the query MBR"** (monotone w.r.t. bbox):

| Predicate | bbox-prunable? | Query MBR |
| --- | --- | --- |
| `ST_Within(g, q)` / `ST_Contains(q, g)` | yes | MBR(q) |
| `ST_Intersects(g, q)` | yes | MBR(q) |
| `ST_Crosses/Overlaps/Touches(g, q)` | yes | MBR(q) |
| `ST_Covers/ST_CoveredBy(g, q)` | yes | MBR(q) |
| `ST_Equals(g, q)` | yes (MBR must equal) | MBR(q) |
| `ST_Distance(g, p) <= r` / `ST_Distance_Sphere …` | yes | MBR(p) expanded by r |
| `ST_Disjoint(g, q)` | **no** (anti-monotone: answer is the complement) | — |

So everything except `ST_Disjoint` benefits. KNN (`ORDER BY ST_Distance LIMIT
k`) benefits via expanding-bbox ring search.

### Index lookup types

- **`IndexLookUp` (point scalar index)** — primary beneficiary: bbox filter cuts
  table lookups; covering rewrite can remove them entirely.
- **`IndexMerge` (general-geometry MVI)** — bbox filter applied to each partial
  index scan; cuts lookups on the merged candidate set.
- **Composite index** `(tenant_id, …, spatial)` — bbox filter stacks after the
  prefix equality.

### Query shapes (largest wins first)

1. **Region / window** (`ST_Within(g, <rectangle/polygon>)`) — bbox prunes most
   non-matches; near-covering for points.
2. **Radius / distance** (`ST_Distance(g, p) <= r`) — query MBR is the r-box.
3. **Spatial join** (`… JOIN … ON ST_Intersects(a.g, b.g)`) — each probe prunes
   via bbox; big aggregate effect.
4. **Low-selectivity / coarse-covering** queries — the higher the
   candidate/result ratio, the more lookups bbox removes.

## 4. Layer B — exact-refine pushdown (tipb + TiKV)

After bbox pruning, the residual false positives are geometries whose MBR
intersects the query but which don't actually satisfy the exact predicate. To
filter those at the storage node too, push the exact `ST_*` predicate down.

### Data contract

Geometry values are stored as **EWKB**: `<srid u32 little-endian><standard
WKB>`. The pushed-down predicate receives two EWKB byte strings (the stored
column value and the query constant) and returns a boolean.

### tipb (protocol) — `~/repos/tipb`, branch `spatial-pushdown`

Add `ScalarFuncSig` enum values for the geometry predicates in
`proto/expression.proto`, e.g.:

```
StWithinSig = <n>;        StContainsSig = <n+1>;
StIntersectsSig = …;      StEqualsSig = …;
StCoversSig = …;          StCoveredBySig = …;
StTouchesSig = …;         StCrossesSig = …;   StOverlapsSig = …;
// (ST_Disjoint intentionally omitted — not bbox-prunable, low value to push.)
```

Each takes two `string` (binary) args (the EWKB operands) and returns `int`
(0/1). No new `FieldType`/`EvalType` is needed — geometry rides as a binary
string, matching how TiDB types it (`TypeGeometry` → `ETString`). Regenerate the
Go/Rust bindings. This mirrors the existing `github.com/mjonss/tipb` fork pattern
used for the analyze work.

### TiDB wiring (the `spatial-index-poc` branch)

1. `setPbCode` on the `geomRel` sigs (map each predicate → its new
   `ScalarFuncSig`).
2. Reverse mapping in `expression.PBToExpr` so a cop can rebuild the
   `ScalarFunction` (unistore reuses this directly — see below).
3. Add the predicates to the pushdown allow-list (`scalarExprSupportedByTiKV` /
   the storage-specific pushable set) so the planner emits a **cop-level
   Selection** for the refine instead of a root Selection. Drop the `VirtualExpr`
   trick for the pushable case.
4. Bump the `tipb` dependency to the fork branch.

### unistore (validation, not performance)

unistore's coprocessor evaluates pushed-down expressions with **TiDB's own Go
`expression` package**. Once `PBToExpr` maps the new pb codes back to the
`geomRel` builtins, unistore evaluates them with the existing `geomrel`/
simplefeatures code — **no new evaluator**. This validates the end-to-end pb
round-trip + plan shape (cop Selection lands at the storage layer), but gives no
speedup in-process. It is the de-risking step before TiKV.

### TiKV (the real performance win) — separate session

Implement the predicates in the Rust coprocessor:

- Add the new `ScalarFuncSig` arms to the cop expression evaluator
  (`components/tidb_query_expr`), dispatching to geometry predicate functions.
- Geometry engine: decode EWKB (strip the 4-byte SRID prefix, parse WKB) and
  evaluate DE-9IM predicates. Options, in order of effort/quality:
  - the `geo`/`geo-types` + `geos`-rs crates (GEOS bindings) for OGC-correct
    predicates (matches simplefeatures/MySQL semantics), or
  - a pure-Rust port of the bbox + point-in-polygon fast paths first, GEOS for
    the rest.
- Correctness contract: results must match the TiDB-side `geomrel`
  (simplefeatures) evaluation byte-for-byte on the shared test corpus, so a
  pushed-down query returns exactly the same rows as the root-refine plan.
- Start with `ST_Within`/`ST_Contains`/`ST_Intersects` (cover the common
  region/point queries); add the rest incrementally.

Parallelizable: tipb (protocol) and TiKV (Rust eval) can proceed against this
contract independently of the TiDB wiring; integration is the pb-code mapping.

## 5. Benchmark (build first, measure the deltas)

`BenchmarkSpatialIndexLookups` (pkg/executor) establishes the baseline so the
layer-A/B deltas are visible. For a representative dataset (e.g. 100k points and
polygons over a unit box) and a set of region/radius queries it reports, via
`EXPLAIN ANALYZE` runtime stats and `b.ReportMetric`:

- **candidates** — `IndexRangeScan` actRows (rows passing the cell filter).
- **lookups** — `TableRowIDScan`/`TableLookUp` actRows (the random reads).
- **results** — final row count.
- **fp_ratio** = candidates / results (wasted-lookup multiplier).
- **ns/op** — end-to-end latency.

Expected effects:

- **Layer A**: `lookups` drops toward `results` (fp_ratio → ~1); `ns/op` falls
  with the random reads removed. (Point covering rewrite: `lookups` → 0 for the
  filter.)
- **Layer B (TiKV)**: fewer rows shipped + refine CPU moved to storage; visible
  on real TiKV, ~flat on unistore.

The same harness, run before/after each layer, is the acceptance evidence.

## 6. Sequencing

1. **Benchmark** (baseline) — DONE: `BenchmarkSpatialIndexLookups`
   (169 lookups / 75 results).
2. **Layer A — bbox-in-index** — DONE for the POINT index (ST_X/ST_Y hidden index
   columns + resolver MBR filter; **169 candidates → 121 lookups**, results exact,
   `TestPOCSpatialSelectivity`) and for the **general-geometry MVI**
   (`tidb_spatial_bbox(g,0..3)` columns + the MVI json_overlaps auto-injection;
   the MBR-intersection filter is applied on the cop Build side of the IndexMerge —
   **81 covering candidates → 25 lookups → 9 results**, exact,
   `TestPOCSpatialMVIAutoInjectAndBBox`). The MBR lives in the index **key** (as
   ordinary index columns); the value stays empty — TiDB has no index-value/INCLUDE
   mechanism, and the columns trail the cell key so they act as index filters, not
   range narrowers. Empirical caveats:
   - **Latency is not measurable in unistore.** An in-memory "table lookup" is
     ~free, so the bbox filter's per-candidate overhead (computing ST_X/ST_Y +
     four comparisons) makes ns/op *rise*. The win is fewer random-read I/Os,
     which only a real-storage benchmark can show. This is the *same* caveat as
     Layer B / cop pushdown — both need real TiKV to show a time win.
   - **Auto-selection regressed**: the extra bbox conditions tip the
     statistics-free optimizer toward a full scan, so the index now needs
     `FORCE INDEX`. → bumps "spatial cost/statistics" to a top priority.
   - TODO: spherical-cap bbox for ST_Distance_Sphere; `ST_Intersects` recognition
     for the MVI auto-injection (`ST_Within`/`ST_Contains` done).
3. **Real-storage benchmark** (next): so Layer A's and Layer B's latency wins are
   measurable at all.
4. **Layer A+ — point covering rewrite** (needs `ST_Point`): refine on index
   `(x,y)` with zero table lookups.
5. **tipb fork** (DONE: `~/repos/tipb` `spatial-pushdown`) + **TiDB pb wiring** +
   **unistore** validation (plumbing PoC).
6. **TiKV** Rust predicate eval (separate session) — the network/CPU win.
