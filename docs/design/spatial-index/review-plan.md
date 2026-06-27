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
- the **coprocessor pushdown contract** — tipb `ScalarFuncSig` 7100–7109, the
  bbox-prefilter → exact-refine split, and the TiKV evaluator's OGC/DE-9IM
  semantics (verified == MySQL/simplefeatures). This is cross-repo (tipb + TiKV)
  and is the most likely design gap.
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

## Code-review stack (dependency-ordered, single-team, flag-gated)

Gate everything behind an experimental flag (e.g. `tidb_enable_spatial_index`, off
by default) so each PR is a no-op until enabled and safe to merge incrementally.

| PR | Scope | Repo | Key files | ~LOC | Depends on | Owner | MVP? |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **T1** | Geometry types + EWKB storage + SRID; `TypeGeometry` fixes (chunk `GetDatum`, cast flen) | tidb | parser `.y`+types, `create_table`, `chunk/row.go`, `builtin_cast.go` | ~400 (+gen parser.go) | — | parser/types/ddl | ✅ |
| **T2** | ST_ I/O + accessors + measurement | tidb | `builtin_geo.go`, `builtin.go`, `ast/functions.go` | ~800 | T1 | expression | ✅ |
| **T3** | DE-9IM predicates + constructors + compat (Envelope, Longitude/Latitude, Crosses gate) | tidb | `geomrel.go`, `builtin_geo.go` | ~700 | T2 | expression | ✅ |
| **T4** | Coverer + `tidb_spatial_key/keys` | tidb | `pkg/util/spatial/*`, key builtins | ~600 | T1 | spatial team | ✅ |
| **T5** | Point spatial index: DDL + `SpatialIndexResolver` (cell-range + refine) | tidb | ddl `create_table`/`executor`/`show`, `spatial_resolve_index.go`, `planbuilder` | ~600 | T3, T4 | ddl + planner | ✅ |
| **T6** | bbox-in-index (Layer A) + GA-list fix | tidb | `spatial_resolve_index.go`, `create_table.go`, `varsutil.go` | ~300 | T5 | planner + ddl | ✅ |
| **T7** | ANALYZE stats for geometry-derived indexes | tidb | `planbuilder.go`, analyze routing | ~150 | T5 | statistics | ✅ |
| **T8** | General-geometry MVI (`json_overlaps` auto-injection + MVI bbox) | tidb | ddl MVI, `spatial_resolve_index.go` | ~400 | T6 | ddl + planner | — |
| **P1** | DE-9IM `ScalarFuncSig` 7100–7109 | tipb | `expression.proto` | ~tens | — | tipb owners | — |
| **K1** | Rust DE-9IM evaluator + `Geometry→Bytes` EvalType + Crosses gate | tikv | `impl_spatial.rs`, `eval_type.rs`, geo crate | ~500 | P1 | TiKV coprocessor | — |
| **T9** | Coprocessor pushdown wiring (`setPbCode`/allow-list/`columnToPBExpr`) | tidb | `builtin_geo.go`, `expr_to_pb.go`, `infer_pushdown.go` | ~150 | P1, T3 | expression + planner | — |

## Land in two waves

- **Wave 1 — points MVP (T1–T7):** a complete, usable points-only spatial index
  that ANALYZEs and auto-selects. Coherent, bounded, mergeable on its own.
- **Wave 2 — enhancements:** general geometry (T8), then the pushdown chain
  (P1 → K1 → T9). The pushdown is independent of T8 and can go in either order.

## Mechanics & gotchas for reviewers

- **Stacked PRs:** each tidb PR branches off the previous; note "Depends on #N" and
  keep them rebasing forward. Squash the 73 WIP commits into these logical units.
- **Generated files:** T1/any grammar PR regenerates `parser.go` (~12k lines) and
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
