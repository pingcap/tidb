# Spatial Support for TiDB — Project Completion Plan

Status: **draft** (2026-07-29). Owner: Mattias Jonsson (@mjonss).
Umbrella tracking issue: [#6347](https://github.com/pingcap/tidb/issues/6347).

This is the top-level plan for landing full geospatial support in TiDB, from the
current design/PoC state through to GA. It is written to be convertible into a GitHub
tracking issue (or a checklist under #6347). It sits above the two design documents and
the code-review stack; it does not restate their detail, it sequences them.

Companion documents (all under `docs/design/spatial-index/`):

- `../2026-06-25-spatial-index.md` — the **spatial index** design (RFC, PR #69473). Design PR B below.
- `research.md`, `PLAN.md`, `PLAN-points-mvp.md`, `CONTEXT.md` — index design background.
- `storage-format.md` — pre-GA encoding lock-ins.
- On the `spatial-index-poc` branch: `review-plan.md` (the PR stack), `gaps.md`,
  `mysql-function-catalog.md`, `pushdown-contract.md`, `OVERNIGHT-PLAN.md`,
  `e2e-pushdown-log.md`, `srid-support-reference.md`.

---

## 1. Where we are today

| Artifact | State | Role |
| --- | --- | --- |
| [#6347](https://github.com/pingcap/tidb/issues/6347) | open (2018), `feature/accepted` | Umbrella tracking issue — link everything here |
| [#38916](https://github.com/pingcap/tidb/pull/38916) | **closed** | Original geospatial design (types/functions). To be **revived** as Design PR A |
| [#69473](https://github.com/pingcap/tidb/pull/69473) | **open, in review** | Spatial **index** design RFC — Design PR B (this branch) |
| [#69475](https://github.com/pingcap/tidb/pull/69475) | open, `[DNM]` | The **PoC** branch — end-to-end working prototype, not for merge as-is |

The PoC (`spatial-index-poc`) has already validated the **entire** stack end to end:
geometry types + EWKB storage, a broad `ST_*` function surface (pure-Go
`simplefeatures` + `golang/geo` S2, no cgo), `CREATE SPATIAL INDEX` for both points
(scalar hidden column) and general geometry (MVI), the `SpatialIndexResolver` planner
rule, bbox pre-filter (Layer A), exact-refine coprocessor pushdown (Layer B, incl. a
TiKV Rust evaluator and tipb sigs), KNN path B, cost-based selection, and an ANALYZE
fix. It is a **proof**, not a merge candidate: it is 70+ squashed-worthy commits across
tidb + tipb + tikv, carries a local `replace` for the tipb fork, and mixes the two
scope layers (basic data + index) that must land separately.

**The core of this plan is therefore not "build it" — most of it is built and proven —
but "restructure the proven work into reviewable, correctly-scoped, mergeable PRs
behind feature flags, split cleanly into basic-data vs index."**

---

## 2. Scope split: two design PRs, three implementation milestones

Per the user's framing and `review-plan.md`:

- **Design PR A — Basic geospatial support (no index).** A revived, focused version of
  #38916: the `GEOMETRY`/`POINT`/… types, per-column `SRID`, EWKB storage, and a
  *minimal but useful* `ST_*` function set. Supports **SRID 0 and 4326** only, extensible
  later to more SRIDs, more functions, and coprocessor pushdown **without a new design**.
  This is the MVP that makes geometry storable and queryable by full scan.

- **Design PR B — Spatial index (#69473, this branch).** Adds the index on top of the
  basic type, with the optional pushdown optimizations. Already written and in review;
  needs a few addenda (§5).

These map to three **functional implementation milestones** (hard rule: **M1 must be in
master, ideally with its flag removed, before M2 index PRs start merging**):

1. **M1 — Basic spatial data (no index).** Design PR A's implementation.
2. **M2 — Spatial index.** Design PR B's implementation (points → general geometry →
   pushdown), gated by its own flag.
3. **M3 — Full function tail.** The geometry *processing* functions (`ST_Buffer`,
   `ST_Union`, …) and long-tail accessors/aliases. Orthogonal to the index (normal
   expression path, not index-eligible, not pushed down) → can run **any time after M1,
   in parallel, off the index critical path**.

---

## 3. Design PR A — basic geospatial (to be written)

#38916 is closed and its text is not in-tree, so Design PR A needs a fresh design doc
(`docs/design/YYYY-MM-DD-geospatial-basic.md`, from `docs/design/TEMPLATE.md`). It should
cover — most content can be lifted from the PoC docs, which already settled these:

- **Types & storage**: `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTI*`,
  `GEOMETRYCOLLECTION`; per-column `SRID` attribute; EWKB value format
  (`<srid_le_u32><wkb>`, MySQL-compatible). **Flag the pre-GA value-format lock-in**
  (version byte / lean layout vs raw EWKB) from `storage-format.md` — this is decided in
  Design A, not Design B, because A owns the type.
- **SRID scope**: SRID 0 (planar) and 4326 (geographic) for v1; the SRS-catalog model
  (`information_schema.st_spatial_reference_systems`, PROJECTED-vs-GEOGRAPHIC dispatch,
  axis order) as the documented extension path. Axis-order convention (MySQL EPSG
  lat/long for 4326) per `axis-order.md`.
- **Function set (M1 minimal)**: I/O (`ST_GeomFromText/WKB/GeoJSON`, `ST_AsText/Binary/GeoJSON`),
  constructors (`Point`, `LineString`, `Polygon`, `Multi*`, `GeometryCollection`),
  accessors (`ST_X/Y/Latitude/Longitude/SRID/GeometryType/Dimension/IsEmpty/IsValid/Envelope/…`),
  measurement (`ST_Area/Length/Centroid/Distance/Distance_Sphere`), DE-9IM predicates
  (`ST_Within/Contains/Intersects/Equals/Disjoint/Touches/Crosses/Overlaps` + `Covers/CoveredBy`).
  Authoritative list: `mysql-function-catalog.md`.
- **Refine/geometry engine**: pure-Go `simplefeatures` (+ S2 for geodesic); no
  cgo/libgeos. Document accepted v1 limitations (e.g. geodesic vs planar refine scope,
  `ST_Area` geodesic gap).
- **Type plumbing**: `TypeGeometry` handling across chunk/codec/cast (INSERT…SELECT,
  UNION, joins, GROUP BY — the audited surface from the PoC).
- **Feature flag** `tidb_enable_spatial` and its planned **removal** after stabilization.
- Explicitly **defer**: the index (Design B), the processing-function tail (M3), SRS
  catalog + projected/other SRIDs, `ST_Transform`.

Deliverable: the design doc + its own discussion PR, referencing #6347 and #69473.

---

## 4. Design PR B — spatial index (#69473, in review)

Already written (this branch). To close design review, add/confirm coverage of the
items that are *new vs #38916* and were not obvious at first draft — several already
have companion notes to fold in:

- **Coprocessor pushdown contract** (Layer B): tipb `ScalarFuncSig` 7100–7109, the
  bbox-prefilter→exact-refine split, EWKB/Bytes encoding, DE-9IM semantics == MySQL.
  Source: `pushdown-contract.md`. Decide: fold into #69473 or ship as a short addendum.
- **bbox-in-index** pruning + the **expression-index/MVI** representation (point → scalar
  `tidb_spatial_key`; general geometry → MVI `tidb_spatial_keys` + `json_overlaps`).
- **Index-eligible predicate set**: region predicates (`Within/Contains/Intersects/Covers/CoveredBy`)
  and cap predicates (`ST_Distance[_Sphere] ≤ r`); everything else runs as a plain filter.
- **Pre-GA cell-key curve lock-in**: Morton vs Hilbert for SRID 0 (benchmark-gated;
  recorded in index metadata so it can change). Source: `storage-format.md`.
- **ANALYZE** for geometry-derived virtual indexes (independent index-scan path).
- **Accepted limitations** and the SRID/SRS tier (see `review-plan.md` open questions).

---

## 5. Implementation PR stack

The PR-by-PR stack is fully worked out in `review-plan.md` (OWNERS-routed, one primary
SIG per PR, flag-gated, ~100–800 reviewable lines each). Summary below; that file is the
source of truth for files/owners/dependencies. Each tidb PR branches off the previous
("Depends on #N"); squash the PoC's WIP commits into these units.

### M1 — Basic data (Design PR A) · gate `tidb_enable_spatial`
| PR | Scope | SIG |
| --- | --- | --- |
| setup | Experimental flag(s) + OWNERS for new spatial dirs | critical-tidb-server + new OWNERS |
| types | Geometry types + `SRID` grammar (regenerates `parser.go`) | parser |
| geom-plumbing | `TypeGeometry` value plumbing (chunk `GetDatum`, cast flen) | community + expression |
| fn-io-accessors | ST_ I/O + accessors + measurement | expression |
| fn-predicates | DE-9IM predicates + constructors + compat fixes | expression (+ geomrel OWNERS) |

→ stabilize, then **remove the M1 flag** (no prior impl to preserve).

### M2 — Spatial index (Design PR B) · gate `tidb_enable_spatial_index`
| PR | Scope | SIG |
| --- | --- | --- |
| index-coverer | Planar/S2 coverer + `tidb_spatial_key/keys` builtins | spatial(new) + expression |
| index-ddl | CREATE SPATIAL INDEX (point) + bbox cols + SHOW CREATE | ddl |
| index-planner | `SpatialIndexResolver`: cell-range + bbox prune + refine | planner |
| index-ga-funcs | bbox funcs in `GAFunction4ExpressionIndex` | critical-tidb-server ⚠️ |
| index-analyze | ANALYZE for geometry-derived indexes | planner (+ stats) |
| index-mvi | General-geometry MVI (`json_overlaps` + MVI bbox) | ddl + planner |
| pushdown-tipb | DE-9IM `ScalarFuncSig` 7100–7109 | **tipb** (cross-repo) |
| pushdown-tikv | Rust evaluator + `Geometry→Bytes` | **tikv** (cross-repo) |
| pushdown-tidb | Pushdown wiring (`setPbCode`/allow-list/`columnToPBExpr`) | expression |

### M3 — Function tail · parallel, off critical path
Processing ops (`ST_Buffer/Union/Intersection/Difference/SymDifference/ConvexHull/Simplify`,
possibly GEOS-gated), remaining niche accessors, MBR predicates, geohash, typed I/O
aliases. Pure expression-layer builtins, single-team (`sig-approvers-expression`).

---

## 6. Cross-repo work (tipb + TiKV)

Layer-B pushdown spans three repos and has a strict landing order:

1. **tipb** (`pingcap/tipb`) — add the `ScalarFuncSig` 7100–7109. **Must merge first.**
   The PoC uses fork `mjonss/tipb` via a local `go.mod` `replace`; this must move to a
   pinned upstream ref before the tidb branch is CI-clean (`bazel_prepare` reverts the
   fork `DEPS.bzl` entries otherwise).
2. **TiKV** (`tikv/tikv`) — the Rust evaluator (`impl_spatial.rs`), `Geometry→Bytes`,
   EvalType/Crosses fixes. PoC branch `mjonss/tikv:spatial-copr-pushdown`.
3. **tidb** — pushdown-tidb wiring (depends on tipb merged).

Layer A (bbox-in-index) needs **none** of this — it uses only existing index-filter
machinery and works on unistore and TiKV alike. It delivers most of the win. Layer B is
an **optional, later** increment; keep it off the index critical path.

---

## 7. Pre-GA decisions to settle (hard-to-change lock-ins)

These need explicit sign-off before GA because they require a migration to change later:

1. **GEOMETRY value format** (Design A owns): raw EWKB vs a version-tagged / leaner
   layout. See `storage-format.md`. Recommend at least a format-version byte.
2. **SRID-0 index cell-key curve** (Design B owns): Morton (today) vs Hilbert. Benchmark
   pruning-vs-encode; record the curve in index metadata / cell-key version so a
   `DROP`/`CREATE INDEX` rebuild can switch it.
3. **4326 semantics for v1**: accept planar refine + documented axis convention as a
   limitation, or block on geodesic refine everywhere. (PoC now has geodesic
   point-in-polygon; polygon/polygon + TiKV-side geodesic are follow-ups.)
4. **SRID/SRS tier**: recommended next target after the points-index milestone is "SRID 0
   + all PROJECTED SRSs + the SRS catalog" (cheap, high compat value); geographic-beyond-4326
   and PostGIS `ST_Transform` are a separate, geodesic-tied effort.
5. **Feature-flag names/defaults** and whether general-geometry/pushdown ship gated
   separately from the points milestone.

---

## 8. PoC → mergeable: what's proven vs what needs hardening

Proven end-to-end in the PoC (reuse, don't rebuild). Key hand-written surface
(confirmed in source, `spatial-index-poc` branch):

- `pkg/util/spatial/` — `coverer.go` (planar Morton quadtree, SRID 0) + `s2.go` (S2, 4326).
- `pkg/util/geomrel/` — `geomrel.go` (DE-9IM refine) + `geodesic.go` (Andoyer/geodesic).
- `pkg/expression/builtin_geo.go` — ~2,200 lines, the full `ST_*` surface.
- `pkg/planner/core/spatial_resolve_index.go` — the `SpatialIndexResolver` rule.
- deps: `github.com/peterstace/simplefeatures`, `github.com/golang/geo` (S2); tipb via a
  local `replace … => github.com/mjonss/tipb` fork.

What these prove: the covering math (no false negatives, property-tested), point + MVI
index shapes, resolver auto-injection, Layer A pruning (measured 169→121 lookups), Layer
B round-trip vs unistore + a TiKV evaluator, KNN path B (index-only top-k), ANALYZE
histogram fix, concurrent-DML `ADMIN CHECK` consistency, MySQL byte-identical compat
suite, pure-Go `CGO_ENABLED=0` build.

**Mechanical restructuring** (per `gaps.md` / `OVERNIGHT-PLAN.md`):
- Split the monolithic PoC branch into the §5 flag-gated PR stack (the largest task).
- tipb fork → pinned upstream ref; `DEPS.bzl` proxy-fetch entries for the new pure-Go
  deps (simplefeatures, golang/geo, go-geom); `make bazel_prepare` clean.
- MySQL **error-code/message parity** (PoC uses "…in the POC" placeholders).
- `ST_GeomFromWKB` 4326 range validation — *already done per gaps.md; confirm.*

**Architectural rework** (PoC shortcuts that work but should not merge as-is):
- **Centralize the EWKB codec.** Encode/decode is duplicated (`builtin_geo.go`
  `encode/decodeEWKB` and `geomrel.go` `decodeEWKB`); geometry currently rides as a
  type-tagged binary string (`ETString` + `TypeGeometry`) with no first-class value type.
  A mergeable version wants one codec and a proper value/codec path.
- **Rework the planner integration.** `spatial_resolve_index.go` is one ~1,050-line
  logical rewrite rule that injects predicates, re-adds pruned hidden columns, and
  recovers coverer params by `strings.Split` of the generated-expression text and the
  index `COMMENT`. This should become a proper spatial **access path + range builder**
  with real cost integration, not string-parsing + predicate injection.
- **Coverer quality**: Morton → Hilbert for SRID 0 (§7); generic-geometry covering is
  **bbox-only** today (flag the pathological-4326-polygon false-negative note); trim
  per-row coverer allocations.

**Missing / external (genuine build work, not restructuring):**
- TiKV Rust cop evaluator (`impl_spatial.rs`) + un-forking tipb — both off-repo today.
- **True-pruning KNN** (path A, expanding-ring operator); only index-only-scan path B exists.
- **Spatial joins** (column↔column) — not index-accelerated (resolver needs a constant
  geometry); an index-nested-loop spatial join is a large future win.
- Lift PoC scope caps: composite index is **point-only**; general-geometry index is
  **SRID-0-only**; geodesic `ST_Area` and non-point/polygon 4326 geometry error out.
- Partitioned-table composition (global index is a later phase; confirm or gate).
- Real-storage (TiKV/disk) benchmark for Layer A / pushdown latency; cost-model
  cross-over accuracy (unistore uses in-memory costs). TiFlash columnar path.

---

## 9. Critical path & parallelization

```
Design A (write) ─┐
                  ├─► M1 impl (types→plumbing→fns) ─► remove M1 flag ─► M2 impl ─► GA
Design B (#69473) ┘        │                                    (index-coverer→ddl→planner
                           │                                     →analyze→mvi; +pushdown)
                           └─► M3 function tail (parallel, anytime after M1) ───────────┘
tipb sigs (merge first) ─────────────────────────────► pushdown-tikv ─► pushdown-tidb
```

- **Serial spine**: Design A → M1 in master → M2 index. This is the one hard ordering.
- **Parallel**: Design B review runs now (independent of M1 coding). M3 tail runs
  parallel to M2. tipb PR can land anytime; TiKV evaluator follows it; both feed the
  optional pushdown-tidb PR near the end of M2.
- **Fastest useful GA**: M1 (geometry usable) → M2 points index + Layer A (the headline
  index win, no cross-repo work). Layer B, general-geometry MVI, KNN path A, projected
  SRIDs, and M3 tail follow incrementally.

---

## 10. Open questions

Carried from `review-plan.md` / the design doc's Unresolved Questions:

1. Does #69473 cover the pushdown contract, or does it need an addendum?
2. 4326 semantics for v1 (planar-refine limitation vs block on geodesic).
3. Feature-flag naming/defaults; separate gating for general-geometry/pushdown.
4. SRID/SRS coverage tier and where projected-SRID expansion sits.
5. Value-format and cell-key-curve lock-ins (§7).
6. Global vs local policy for partitioned spatial indexes (Phase 3).

---

## 11. Suggested tracking-issue structure

When promoting this to a GitHub issue under #6347, use three checklist sections
mirroring the milestones, each listing its §5 PRs with `Depends on #N`, plus a
"Design" section (Design A doc + PR, #69473 sign-off + addenda) and a "Cross-repo"
section (tipb, TiKV). Link every code PR back to #6347. Plan a flag-removal cleanup PR
per wave and track it in #6347 so it isn't forgotten.
