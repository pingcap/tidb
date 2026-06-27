# Spatial Support — Design & Code Review Plan

How to get the full spatial support (index + tipb + TiKV) reviewed without handing
any human a 20k-line branch. The strategy is to (1) keep **design** review on the
existing RFCs, and (2) split **code** review into a stack of small, single-team PRs
behind an experimental flag, landing a points-only MVP first.

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
  P1/K1/T9 below.
- **bbox-in-index** pruning and the **expression-index / MVI** representation
  (point → scalar `tidb_spatial_key`; general geometry → MVI `tidb_spatial_keys` +
  `json_overlaps`).
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

| PR | Scope | Primary sig | Key files | Depends | Wave |
| --- | --- | --- | --- | --- | --- |
| **S0** | Experimental flag(s) + OWNERS for the new spatial dirs | `critical-tidb-server` + new OWNERS | `sessionctx/variable`, `pkg/util/{spatial,geomrel}/OWNERS` | — | 1 |
| **TP** | Geometry types + `SRID` grammar (regenerates `parser.go`) | `parser` | `parser/*.y`, `types/field_type` | — | 1 |
| **TC** | `TypeGeometry` value plumbing: chunk `GetDatum` + cast flen (INSERT…SELECT / UNION) | community + `expression` | `util/chunk/row.go`, `builtin_cast.go` | TP | 1 |
| **TF1** | ST_ I/O + accessors + measurement | `expression` | `builtin_geo.go`, `builtin.go`, `ast/functions.go` | TP | 1 |
| **TF2** | DE-9IM predicates + constructors + compat (Envelope/Long/Lat/Crosses gate) | `expression` (+ geomrel OWNERS) | `geomrel.go`, `builtin_geo.go` | TF1 | 1 |
| **TCov** | Planar/S2 coverer + `tidb_spatial_key/keys` builtins | `spatial`(new) + `expression` | `pkg/util/spatial/*`, key builtins | TP | 1 |
| **TD** | CREATE SPATIAL INDEX (point) + bbox cols + SHOW CREATE | `ddl` | `create_table`, `executor`, `show` | TCov | 1 |
| **TPl** | `SpatialIndexResolver`: cell-range + bbox prune + refine | `planner` | `spatial_resolve_index.go`, `planbuilder` | TD, TF2 | 1 |
| **TGA** | bbox functions GA in `GAFunction4ExpressionIndex` | `critical-tidb-server` ⚠️ | `varsutil.go` | TD | 1 |
| **TStat** | ANALYZE for geometry-derived indexes | `planner` (+ `stats`) | `planbuilder.go` | TPl | 1 |
| **TMVI** | General-geometry MVI (`json_overlaps` + MVI bbox) | `ddl` + `planner` | ddl MVI, `spatial_resolve_index.go` | TPl | 2 |
| **P1** | DE-9IM `ScalarFuncSig` 7100–7109 | **tipb** | `expression.proto` | — | 2 |
| **K1** | Rust evaluator + `Geometry→Bytes` + Crosses gate | **tikv** | `impl_spatial.rs`, `eval_type.rs` | P1 | 2 |
| **T9** | Pushdown wiring (`setPbCode`/allow-list/`columnToPBExpr`) | `expression` | `builtin_geo.go`, `expr_to_pb.go`, `infer_pushdown.go` | P1, TF2 | 2 |

TD/TPl were one "index" PR; splitting on the ddl↔planner boundary makes each
single-team. TMVI still spans ddl+planner — split the same way if a single team is
required. TCov and the key builtins span the new `pkg/util/spatial` and
`pkg/expression`; the coverer can land first (spatial OWNERS) and the builtin in TF*.

## Land in two waves

- **Wave 1 — points MVP (S0, TP, TC, TF1, TF2, TCov, TD, TPl, TGA, TStat):** a
  complete, usable points-only spatial index that ANALYZEs and auto-selects.
- **Wave 2 — enhancements:** general geometry (TMVI), then the pushdown chain
  (P1 → K1 → T9). Pushdown is independent of TMVI; either order.

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
  is the recommended next SRID target after the points MVP** — big compatibility win,
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
SRID scope right after the points MVP; treat full geographic/geodesic and PostGIS
transforms as a separate, explicitly-scoped effort tied to the geodesic-refine work.

## Feature flags (and their removal)

- Gate Wave 1 on `tidb_enable_spatial_index` (off by default); optionally a second
  flag for the pushdown so it can ramp independently. Multiple flags are fine.
- **These are launch gates, not compatibility switches.** This is brand-new
  functionality — there is no prior implementation to fall back to — so once the
  basic feature is stable in master, **remove the flag(s)** (and the dead gated-off
  branches) rather than keeping them indefinitely. Plan a cleanup PR per flag right
  after each wave GA's. Track the removal in #6347 so it isn't forgotten.

## Mechanics & gotchas for reviewers

- **Stacked PRs:** each tidb PR branches off the previous; note "Depends on #N" and
  keep them rebasing forward. Squash the 73 WIP commits into these logical units.
- **Generated files:** TP (the grammar PR) regenerates `parser.go` (~12k lines) and
  the threadsafe builtin table — mark them generated so nobody reads them; run
  `make bazel_prepare` for BUILD metadata.
- **tipb dependency for T9/K1:** P1 must merge first; until it does, the branch
  pins the fork. For go: `replace github.com/pingcap/tipb => github.com/mjonss/tipb
  v0.0.0-20260626151721-eacc7e94342e`. For bazel: the `go_repository` `replace`
  attribute form (no mirror needed) — see `e2e-pushdown-log.md`; `make
  bazel_prepare` reverts it, so re-apply.
- **Pushdown correctness evidence** for K1/T9 review: real-cluster results vs
  MySQL (8/8 DE-9IM predicates over 3000 fuzzed pairs; index path sound) and the
  benchmark are in `docs/design/spatial-index/e2e-pushdown-log.md`.

## Open questions to settle in design review

1. Does #69473 cover the **pushdown contract** (tipb + TiKV), or does it need an
   addendum / a short companion design note?
2. **4326 semantics:** accept planar-refine + the coord0=lng axis convention as a
   documented limitation for v1, or block on geodesic predicates?
3. Feature-flag name + default, and whether general-geometry/pushdown ship gated
   separately from the points MVP.
4. **SRID/SRS coverage tier:** settle the SRS model in #69473 (catalog source =
   EPSG via `information_schema.st_spatial_reference_systems`; PROJECTED-vs-GEOGRAPHIC
   dispatch; axis order). Decide whether the first SRID-expansion target is "all
   PROJECTED SRSs + catalog" (cheap, recommended) and where geographic-beyond-4326
   and PostGIS `ST_Transform` sit (tied to geodesic-refine; PostGIS likely out of
   scope).
