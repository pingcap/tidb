# Spatial Support — Design & Code Review Plan

How to get the full spatial support (index + tipb + TiKV) reviewed without handing
any human a 20k-line branch. The strategy is to (1) keep **design** review on the
existing RFCs, and (2) split **code** review into a stack of small, single-team PRs
behind experimental flags, in three functional milestones — basic data (no index)
→ spatial index → full function set.

## Existing design artifacts (the design layer already exists)

| Ref | Title | State | Role |
| --- | --- | --- | --- |
| [#6347](https://github.com/pingcap/tidb/issues/6347) | Support for SPATIAL functions, data types and indexes | open (2018) | Umbrella tracking issue — link every PR here |
| [#38916](https://github.com/pingcap/tidb/pull/38916) | *: Geospatial design | closed | Original functions/types/overall design (reference) |
| [#69473](https://github.com/pingcap/tidb/pull/69473) | *: Spatial index design | open | **Active** index design RFC — the design review |

**Design-review action:** land #69473 (design sign-off) before/independently of the
code PRs. Confirm it (or a short addendum) covers the parts that are *new* vs
#38916 and weren't obvious at design time:
- the **coprocessor pushdown contract** — drafted as a companion design note,
  [`pushdown-contract.md`](pushdown-contract.md) (tipb `ScalarFuncSig` 7100–7109,
  the bbox-prefilter → exact-refine split, EWKB/Bytes encoding, DE-9IM semantics
  verified == MySQL/simplefeatures). Cross-repo (tipb + TiKV); the design ref for
  pushdown-tipb/pushdown-tikv/pushdown-tidb below.
- **bbox-in-index** pruning and the **expression-index / MVI** representation
  (point → scalar `tidb_spatial_key`; general geometry → MVI `tidb_spatial_keys` +
  `json_overlaps`).
- the **index-eligible predicate set**: the resolver injects a covering-cell range
  for the *region* predicates `ST_Within` / `ST_Contains` / `ST_Intersects` /
  `ST_Covers` / `ST_CoveredBy` (Covers ⊇ Contains and CoveredBy ⊇ Within, so the
  prefilter has no false negatives) and for the *cap* predicates `ST_Distance` /
  `ST_Distance_Sphere ≤ r`. Other predicates run as plain filters (still
  cop-pushable).
- **ANALYZE** for geometry-derived virtual indexes (independent index-scan path).
- accepted **limitations**: planar 4326 refine (only the S2 covering is geodesic);
  4326 axis convention (coord0 = lng here vs lat in MySQL).

## Actual scope (it looks bigger than it is)

73 commits on top of current master (`d13cf4e625`). The 20k-line diff is dominated
by **generated `pkg/parser/parser.go`** (goyacc, ~25k of the churn — reviewers
review the `.y` grammar, not the generated file). Hand-written surface:

- **~4,000 lines of new Go** (≈ `builtin_geo.go` 1982, `spatial_resolve_index.go`
  808, `pkg/util/spatial` 458, `pkg/util/geomrel` 93, ddl ~285, parser types/ast
  ~130, stats glue).
- **~500 lines Rust** (TiKV `impl_spatial.rs`) + the EvalType/Crosses fixes.
- **tens of lines** of tipb.
- ~1.5k tests, ~900 docs.

So each PR below is ~100–800 reviewable lines.

## OWNERS routing (so each PR has one review team)

From the directory `OWNERS` (each `no_parent_owners: true`):

| Path | Approver sig |
| --- | --- |
| `pkg/parser/**` (grammar, `types/field_type`, `ast/functions`) | `sig-approvers-parser` (+ `sig-critical-approvers-parser`) |
| `pkg/expression/**` (`builtin_geo`, `builtin_cast`, `expr_to_pb`, `infer_pushdown`, `distsql_builtin`) | `sig-approvers-expression` |
| `pkg/ddl/**` (`create_table`, `executor`, `show`) | `sig-approvers-ddl` |
| `pkg/planner/**` (`spatial_resolve_index`, `planbuilder`) | `sig-approvers-planner` |
| `pkg/statistics/**` | `sig-approvers-stats` |
| `pkg/sessionctx/variable/**` (feature flag, GA-list) | `sig-critical-approvers-tidb-server` ⚠️ critical |
| `pkg/util/chunk`, `pkg/util/codec`, `pkg/types` | root → community/dep |
| **`pkg/util/spatial`, `pkg/util/geomrel` (NEW)** | none yet → **add an OWNERS naming the spatial team** so they don't fall to community-wide review |

Two consequences: (1) the boundary to split on is **parser \| expression \| ddl \|
planner \| stats \| sessionctx(critical) \| tikv \| tipb**; (2) `sessionctx/variable`
is a **critical** approver, so isolate the flag and the GA-list into their own
1-file PRs and don't block index work on that queue.

## Code-review stack (one primary sig per PR, flag-gated)

Each row is a compilable unit; "no user-visible behavior yet" is fine while gated.

| PR | Scope | Primary sig | Key files | Depends | M |
| --- | --- | --- | --- | --- | --- |
<!-- M = milestone: 1 = basic data (no index), 2 = spatial index. Milestone 3 (full function set) is future expression PRs, not listed. -->

| **setup** | Experimental flag(s) + OWNERS for the new spatial dirs | `critical-tidb-server` + new OWNERS | `sessionctx/variable`, `pkg/util/{spatial,geomrel}/OWNERS` | — | 1 |
| **types** | Geometry types + `SRID` grammar (regenerates `parser.go`) | `parser` | `parser/*.y`, `types/field_type` | — | 1 |
| **geom-plumbing** | `TypeGeometry` value plumbing: chunk `GetDatum` + cast flen (INSERT…SELECT / UNION) | community + `expression` | `util/chunk/row.go`, `builtin_cast.go` | types | 1 |
| **fn-io-accessors** | ST_ I/O + accessors + measurement | `expression` | `builtin_geo.go`, `builtin.go`, `ast/functions.go` | types | 1 |
| **fn-predicates** | DE-9IM predicates + constructors + compat (Envelope/Long/Lat/Crosses gate) | `expression` (+ geomrel OWNERS) | `geomrel.go`, `builtin_geo.go` | fn-io-accessors | 1 |
| **index-coverer** | Planar/S2 coverer + `tidb_spatial_key/keys` builtins | `spatial`(new) + `expression` | `pkg/util/spatial/*`, key builtins | types | 2 |
| **index-ddl** | CREATE SPATIAL INDEX (point) + bbox cols + SHOW CREATE | `ddl` | `create_table`, `executor`, `show` | index-coverer | 2 |
| **index-planner** | `SpatialIndexResolver`: cell-range + bbox prune + refine | `planner` | `spatial_resolve_index.go`, `planbuilder` | index-ddl, fn-predicates | 2 |
| **index-ga-funcs** | bbox functions GA in `GAFunction4ExpressionIndex` | `critical-tidb-server` ⚠️ | `varsutil.go` | index-ddl | 2 |
| **index-analyze** | ANALYZE for geometry-derived indexes | `planner` (+ `stats`) | `planbuilder.go` | index-planner | 2 |
| **index-mvi** | General-geometry MVI (`json_overlaps` + MVI bbox) | `ddl` + `planner` | ddl MVI, `spatial_resolve_index.go` | index-planner | 2 |
| **pushdown-tipb** | DE-9IM `ScalarFuncSig` 7100–7109 | **tipb** | `expression.proto` | — | 2 |
| **pushdown-tikv** | Rust evaluator + `Geometry→Bytes` + Crosses gate | **tikv** | `impl_spatial.rs`, `eval_type.rs` | pushdown-tipb | 2 |
| **pushdown-tidb** | Pushdown wiring (`setPbCode`/allow-list/`columnToPBExpr`) | `expression` | `builtin_geo.go`, `expr_to_pb.go`, `infer_pushdown.go` | pushdown-tipb, fn-predicates | 2 |

index-ddl/index-planner were one "index" PR; splitting on the ddl↔planner boundary makes each
single-team. index-mvi still spans ddl+planner — split the same way if a single team is
required. index-coverer and the key builtins span the new `pkg/util/spatial` and
`pkg/expression`; the coverer can land first (spatial OWNERS) and the builtin in TF*.

## Functional landing order (three milestones)

The PR rows above slot into three functional milestones. **The only hard ordering
constraint: basic data support must be in master — and ideally its flag removed —
before index PRs start merging.** Within each milestone, rows can be reordered or
subdivided further.

1. **Basic spatial data — no index** · setup, types, geom-plumbing, fn-io-accessors, fn-predicates.
   Geometry types + EWKB storage + full type *syntax* + a *minimal but useful*
   function set: I/O (`ST_GeomFromText/AsText/AsBinary/GeomFromWKB`), the DE-9IM
   predicates, accessors (`ST_X/Y/SRID/GeometryType`), measurement
   (`ST_Distance(_Sphere)`), constructors. Geometry becomes storable and queryable
   by full scan. Stabilize, then **remove the basic flag** (no prior impl to keep).
2. **Spatial index** · index-coverer, index-ddl, index-planner, index-ga-funcs, index-analyze (points + stats) → index-mvi (general
   geometry) → pushdown-tipb → pushdown-tikv → pushdown-tidb (pushdown). Gated by its own flag(s).
3. **Full function set** · the geometry-*processing* tail (`ST_Buffer`, `ST_Union`,
   `ST_Intersection`, `ST_Difference`, `ST_ConvexHull`, `ST_Simplify`, …), remaining
   accessors, GeoJSON options, and important PostGIS extras. Pure expression-layer
   builtins.

### Function catalog: milestone-1 *minimal* vs milestone-3 *tail*

The authoritative full list (every MySQL spatial function, POC-implemented marked,
PostGIS extras noted) is [`mysql-function-catalog.md`](mysql-function-catalog.md);
this section is the milestone-split summary.

Boundary rule: **milestone 1 = everything needed to store, read, and query/index a
geometry; milestone 3 = geometry *processing/analysis* and the long-tail
variants/aliases.** Milestone 1 is ≈ what the POC already implements, so its `fn-*`
PRs are mostly review/cleanup of existing code rather than new work.

Membership below was **verified empirically against MySQL 9.7.1 and 8.0.46** (call
each with no args: `1582` = present, `1305`/`1046` = absent). **The two versions are
identical** — nothing in this catalog is 9.7-only or 8.0-only — so "MySQL" = both.

**Milestone 1 — minimal (store / read / query / index)** · all present in MySQL.
- *I/O:* `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`; `ST_AsText`
  (`ST_AsWKT`), `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- *Constructors:* `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`,
  `MultiPolygon`, `GeometryCollection`.
- *Accessors:* `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (get + set),
  `ST_GeometryType`, `ST_Dimension`, `ST_IsEmpty`, `ST_IsValid`, `ST_Envelope`,
  `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`, `ST_ExteriorRing`,
  `ST_NumInteriorRings`.
- *Measurement:* `ST_Area`, `ST_Length`, `ST_Centroid`, `ST_Distance`,
  `ST_Distance_Sphere`.
- *Predicates (DE-9IM):* `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`.

**Milestone 3 — tail.** *In MySQL* (the long tail to reach full parity):
- *Set ops / processing:* `ST_Buffer` (+ `ST_Buffer_Strategy`), `ST_ConvexHull`,
  `ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_SymDifference`, `ST_Simplify`.
- *Editing / sampling / utility:* `ST_Validate`, `ST_MakeEnvelope`, `ST_SwapXY`,
  `ST_Collect`, `ST_LineInterpolatePoint(s)`, `ST_PointAtDistance`.
- *Distance variants:* `ST_FrechetDistance`, `ST_HausdorffDistance`.
- *Niche accessors:* `ST_GeometryN`, `ST_NumGeometries`, `ST_InteriorRingN`,
  `ST_IsSimple`, `ST_IsClosed`.
- *MBR predicates:* `MBRContains`, `MBRWithin`, `MBRIntersects`, `MBRDisjoint`,
  `MBREquals`, `MBROverlaps`, `MBRTouches`, `MBRCoveredBy`.
- *Geohash:* `ST_GeoHash`, `ST_PointFromGeoHash`, `ST_LatFromGeoHash`,
  `ST_LongFromGeoHash`.
- *Typed I/O aliases:* `ST_PointFromText`/`ST_LineFromText`/`ST_PolyFromText`/
  `ST_M*FromText` (+ `…FromTxt`/`…FromWKB` spellings) — thin wrappers over the
  generic form.
- *Cross-SRID:* `ST_Transform` — travels with the projected/PostGIS SRID work (needs
  the SRS catalog + reprojection), not here.

*NOT in MySQL — PostGIS-only* (verified absent in 9.7 & 8.0). **Policy: keep a
PostGIS extra only when it is index-supported.**
- `ST_Covers` / `ST_CoveredBy` — **kept**: now index-eligible region predicates
  (`Covers ⊇ Contains`, `CoveredBy ⊇ Within`, so the covering-cell prefilter is valid
  with no false negatives — implemented in the resolver).
- Possible extensions (only if index-supported / by demand, none implemented):
  `ST_Relate` (DE-9IM matrix), `ST_PointOnSurface`, `ST_Boundary`, `ST_IsRing`,
  `ST_Perimeter`, `ST_Subdivide`, `ST_ClosestPoint`, `ST_Azimuth`. (Also absent: the
  bare non-`ST_` aliases such as `GeometryType`.)

**Library note:** the processing ops (`ST_Buffer`/`ST_Union`/`ST_Intersection`/…)
need GEOS-equivalent algorithms; Go `simplefeatures` and the TiKV `geo` crate cover
some but not all, so the tail may pull in additional geometry-library capability —
another reason to keep it off the index critical path.

### Assessment — does the full function set (3) come after the index (2)? **Yes.**

The function tail is **orthogonal** to the index, and that — more than review
ergonomics — is why it goes last:
- It is evaluated by the normal expression path (**no executor surface**), is **not
  index-eligible**, and is **not pushed down** (only the DE-9IM predicates are — see
  [`pushdown-contract.md`](pushdown-contract.md)). So it **gates nothing** and must
  stay off the index's critical path.
- Deferring it *does* keep the **planner/optimizer** review focused — the reviewer
  confirms index/pushdown eligibility against a handful of predicates, not ~70
  functions — but it does **not** materially change the **executor** or **statistics**
  review (neither touches the tail). So "more complete review of executor/planner/
  stats" is a modest, planner-only benefit; the decisive reason is orthogonality.
- Because it is independent and single-team (`sig-approvers-expression`), it can run
  **in parallel** with the index to spread review load. So milestone 3 really means
  "any time after milestone 1, off the index critical path."

Note: in TiDB, functions are **generic calls** (not in the yacc grammar), so the
tail adds **no parser churn** — "full syntax" is just the one-time geometry-*type*
grammar in milestone 1 (which regenerates `parser.go` once).

## SRID / SRS coverage (scope beyond v1)

v1 supports **SRID 0 (planar)** and **SRID 4326 (S2/geographic)** — enough to prove
the model. Full MySQL parity (and optionally PostGIS) is a follow-up, layered by
cost so the cheap, high-value part can land without the expensive part:

- **SRS catalog** — populate `information_schema.st_spatial_reference_systems` from
  the EPSG dataset (MySQL ships ~5,100 entries) plus the metadata the engine needs
  per SRS: kind (PROJECTED vs GEOGRAPHIC), axis order, coordinate bounds, unit,
  ellipsoid. Prerequisite for everything below. *Moderate* (a system table + data).
- **All PROJECTED SRSs** (Cartesian, e.g. 3857 Web Mercator) — *low extra cost*: the
  planar Morton coverer is already Cartesian; it just needs the SRS's coordinate
  bounds to size the quadtree domain, and the ST_ functions are already planar. So
  "every projected SRID MySQL supports" ≈ the catalog + dispatch-by-bounds. **This
  is the recommended next SRID target after the points-index milestone** — big compatibility win,
  small change.
- **GEOGRAPHIC SRSs beyond 4326** (other datums/ellipsoids) — *moderate*: S2 covering
  generalizes to the sphere, but exact geodesic *refine* per ellipsoid is the same
  bigger lift as the planar-4326 limitation (a geodesic predicate/distance library
  on both TiDB and TiKV).
- **PostGIS-level** (user `CREATE SPATIAL REFERENCE SYSTEM`, `ST_Transform` between
  SRSs via a projection/`proj` library) — *bigger; out of scope for now*. Needs
  on-the-fly reprojection; revisit only if required. (This is the "bigger change"
  case — flagged, not committed.)

**Recommendation:** target "SRID 0 + all PROJECTED SRSs + the SRS catalog" as the
SRID scope right after the points-index milestone; treat full geographic/geodesic and PostGIS
transforms as a separate, explicitly-scoped effort tied to the geodesic-refine work.

## Feature flags (and their removal)

- Gate milestone 1 (basic data) on a flag (e.g. `tidb_enable_spatial`), and
  milestone 2 (index) on its own (e.g. `tidb_enable_spatial_index`); optionally a
  third for the pushdown so it can ramp independently. Multiple flags are fine.
- **These are launch gates, not compatibility switches.** This is brand-new
  functionality — there is no prior implementation to fall back to — so once the
  basic feature is stable in master, **remove the flag(s)** (and the dead gated-off
  branches) rather than keeping them indefinitely. Plan a cleanup PR per flag right
  after each wave GA's. Track the removal in #6347 so it isn't forgotten.

## Mechanics & gotchas for reviewers

- **Stacked PRs:** each tidb PR branches off the previous; note "Depends on #N" and
  keep them rebasing forward. Squash the 73 WIP commits into these logical units.
- **Generated files:** types (the grammar PR) regenerates `parser.go` (~12k lines) and
  the threadsafe builtin table — mark them generated so nobody reads them; run
  `make bazel_prepare` for BUILD metadata.
- **tipb dependency for pushdown-tidb/pushdown-tikv:** pushdown-tipb must merge first; until it does, the branch
  pins the fork. For go: `replace github.com/pingcap/tipb => github.com/mjonss/tipb
  v0.0.0-20260626151721-eacc7e94342e`. For bazel: the `go_repository` `replace`
  attribute form (no mirror needed) — see `e2e-pushdown-log.md`; `make
  bazel_prepare` reverts it, so re-apply.
- **Pushdown correctness evidence** for pushdown-tikv/pushdown-tidb review: real-cluster results vs
  MySQL (8/8 DE-9IM predicates over 3000 fuzzed pairs; index path sound) and the
  benchmark are in `docs/design/spatial-index/e2e-pushdown-log.md`.

## Open questions to settle in design review

1. Does #69473 cover the **pushdown contract** (tipb + TiKV), or does it need an
   addendum / a short companion design note?
2. **4326 semantics:** accept planar-refine + the coord0=lng axis convention as a
   documented limitation for v1, or block on geodesic predicates?
3. Feature-flag name + default, and whether general-geometry/pushdown ship gated
   separately from the points-index milestone.
4. **SRID/SRS coverage tier:** settle the SRS model in #69473 (catalog source =
   EPSG via `information_schema.st_spatial_reference_systems`; PROJECTED-vs-GEOGRAPHIC
   dispatch; axis order). Decide whether the first SRID-expansion target is "all
   PROJECTED SRSs + catalog" (cheap, recommended) and where geographic-beyond-4326
   and PostGIS `ST_Transform` sit (tied to geodesic-refine; PostGIS likely out of
   scope).
