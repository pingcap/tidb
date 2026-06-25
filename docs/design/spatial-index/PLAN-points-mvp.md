# Points-only Spatial Index MVP (TiKV secondary index)

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it. Design background is in `docs/design/spatial-index/research.md`; the broader roadmap is in `docs/design/spatial-index/PLAN.md`. This file is the concrete first deliverable and is self-contained.

## Purpose / Big Picture

After this change a user can create a spatial index on a `POINT` column and have proximity and point-in-region queries served by a selective index scan instead of a full table scan. Concretely, for a table of locations, "find rows within radius r of a point" and "find rows whose point lies in a polygon/window" use the index.

Observable outcome: on a seeded table, `EXPLAIN` shows a spatial index range scan plus a refine filter (not `TableFullScan`), and the query returns identical rows with and without the index.

Scope is deliberately the smallest useful slice: the indexed column must be a `POINT` (one index entry per row, no covering fan-out), for SRID 0 and SRID 4326. Non-point geometries, the multi-valued covering index, native nearest-neighbour, partitioned-table (global) indexes, and TiFlash are out of scope and are later deliverables tracked in `PLAN.md`. The MVP targets non-partitioned tables; the global-index-for-partitioned-tables design from the Sunny Bains review (see `research.md` -> "Index value contents and table partitioning") is a tracked follow-on.

## Progress

- [ ] Milestone 0: Confirm the prerequisite geometry-type contract and wrap it behind a thin accessor so churn stays localized.
- [ ] Milestone 1 (prototype, riskiest): SRID 0 planar coverer (`EncodePoint`, `CoverQuery`) validated in isolation, plus a throwaway proof that the planner can inject coverer-produced ranges into an index access path.
- [ ] Milestone 2: `CREATE SPATIAL INDEX` on a `POINT` column (SRID 0) as a hidden-generated-column expression index; one entry per row.
- [ ] Milestone 3: Planner hook + refine: recognize spatial predicates on the indexed column, build cell ranges from the coverer, keep the exact predicate as a filter (SRID 0).
- [ ] Milestone 4: SRID 4326 via an S2 coverer behind the same interface (antimeridian/pole correct, spherical-cap distance covering).
- [ ] Milestone 5: Correctness and compatibility pass; Bazel metadata; lint.

## Surprises & Discoveries

- Observation: the points index needs almost no new storage/write code.
  Evidence: `pkg/table/tables/index.go:663-671` selects `PlainIndexKVGenerator` when `IndexInfo.MVIndex` is false (one row, one entry). A hidden-generated-column expression index reuses this path unchanged.
- Observation: the geometry type is essentially absent in master today; only the type constant exists.
  Evidence: `pkg/parser/mysql/type.go:47` defines `TypeGeometry = 0xff`, but no `ST_*` builtins are registered in `pkg/expression/builtin.go` and there is no EWKB value type. So the type plus basic functions are a real prerequisite (landing independently).
- Observation: normal expression-index matching will not fire for spatial predicates.
  Evidence: ranger matches conditions to the indexed expression literally (`pkg/util/ranger/detacher.go:1033`). Our predicate is `ST_Distance(position, c) <= r`, not `tidb_spatial_key(position) = ...`, so a custom planner hook must translate it into ranges on the hidden column. This is the riskiest piece and gets a prototype milestone.

## Decision Log

- Decision: Model the points index as an expression index on a hidden, virtual generated column `tidb_spatial_key(position)`, not as a brand-new index kind.
  Rationale: reuses the existing expression-index DDL, the non-MVI `PlainIndexKVGenerator` write path, and the standard index scan, concentrating new work in the coverer and the planner hook. For points there is exactly one entry per row, so MVI machinery is unnecessary.
  Date/Author: 2026-06-24, Mattias Jonsson.

- Decision: Restrict the MVP index to a `POINT`-typed, `NOT NULL`, SRID-constrained (0 or 4326) column.
  Rationale: one-entry-per-row holds only for points; `NOT NULL` matches MySQL's spatial-index requirement; a fixed per-column SRID fixes the cell scheme. Generic geometries are a later deliverable.
  Date/Author: 2026-06-24, Mattias Jonsson.

- Decision: Deliver SRID 0 first (planar quadtree, no external dependency), then SRID 4326 (S2) behind the same coverer interface, both within this MVP.
  Rationale: SRID 0 validates the whole DDL -> write -> planner -> scan -> refine pipeline cheaply; 4326 carries the real user value (lat/long proximity) and adds the `github.com/golang/geo/s2` dependency only once the pipeline is proven.
  Date/Author: 2026-06-24, Mattias Jonsson.

- Decision: Refine reuses the existing exact predicate as a retained filter; no new executor evaluation code.
  Rationale: the index returns a candidate superset; leaving `ST_Distance`/`ST_Contains` in `TableFilters` (the Selection above the scan) makes the result exact using functions the prerequisite already provides.
  Date/Author: 2026-06-24, Mattias Jonsson.

- Decision: MVP stores the point's bbox/coordinates in the index value (so distance refine can skip the lookback for points), and targets non-partitioned tables only; the global index for partitioned tables is a follow-on.
  Rationale: aligns with the Sunny Bains review's bbox-in-value refinement at the cheapest point (a point's bbox is just its coordinates), while deferring the partitioned-table global-index work. Open wrinkle to resolve in Milestone 2: the hidden-generated-column expression index normally has an empty value, so carrying the bbox/coordinates needs a small index-value-generation extension (tracked in `research.md` open questions, item 6).
  Date/Author: 2026-06-25, Mattias Jonsson.

- Open choice (not yet decided): user-facing DDL surface, MySQL-compatible `SPATIAL INDEX (col)` (needs parser grammar work) vs a lower-effort `CREATE INDEX ... USING HILBERT` or explicit expression-index syntax. Recommendation: `SPATIAL INDEX` for compatibility, but the internal representation is identical either way, so the grammar can be chosen late.

## Outcomes & Retrospective

To be filled in at Milestone 3 (pipeline working for SRID 0) and at MVP completion.

## Context and Orientation

Definitions for a novice reader.

A **POINT geometry** is stored as EWKB: a 4-byte little-endian SRID prefix then OGC WKB (byte order, type, X, Y). **SRID 0** is an abstract Cartesian plane; **SRID 4326** is WGS 84 lat/long. A **space-filling curve (SFC)** linearizes 2D into a 1D ordered value; a point maps to one cell whose id is its position on the curve. **Covering** a query region means producing the set of cell-id ranges that can contain matching points. **Refine** means re-checking candidates with the exact predicate, since cells are coarse.

**Prerequisite contract** (lands independently; this MVP codes against it behind one thin accessor):

1. A stored `POINT` value the index can read as `(srid, x, y)` (EWKB decode).
2. The predicate functions used for refine, registered as builtins: `ST_Distance` and `ST_Distance_Sphere` (distance, SRID 0 and 4326), and `ST_Contains`/`ST_Within` (point in polygon).
3. `ST_GeomFromText`/`ST_AsText` to write and read values in tests.

Today only `TypeGeometry` (`pkg/parser/mysql/type.go:47`) exists; the rest is absent, so Milestone 0 is to verify the landed contract and wrap point access behind `spatial.PointAccessor` so signature churn does not spread.

**Key integration points** (verified, with current line numbers; reconfirm before editing):

- Expression index / hidden column: `pkg/parser/ast/ddl.go:293-301` (`IndexPartSpecification.Expr`); `pkg/meta/model/column.go:96-105` (`ColumnInfo.GeneratedExprString`, `Hidden`); creation flow near `pkg/ddl/index.go:916`.
- Index metadata: `pkg/meta/model/index.go:260-287` (`IndexInfo`, `MVIndex`).
- Predicate to range: `pkg/util/ranger/detacher.go:1033` (`DetachCondAndBuildRangeForIndex`).
- Access path build and selection: `pkg/planner/core/planbuilder.go:1320-1414` (`getPossibleAccessPaths`); `pkg/planner/core/stats.go:397-449` (range building per path); `pkg/planner/core/find_best_task.go:1770-1861` (`skylinePruning`); `pkg/planner/util/path.go:46-150` (`AccessPath.Ranges`, `AccessConds`, `TableFilters`).
- Index KV write: `pkg/table/tables/index.go:663-671` (`GenIndexKVIter` selecting `PlainIndexKVGenerator`); `pkg/tablecodec/tablecodec.go:1228-1283` (`GenIndexKey`).
- Builtin registration: `pkg/expression/builtin.go:659+` (`var funcs`).

## Plan of Work

The work goes bottom-up so the riskiest piece is proven before the rest is built on it.

First, add an engine-neutral coverer package (proposed `pkg/util/spatial`). Define a `Coverer` interface with `EncodePoint(srid, x, y) (int64, error)` returning a point's cell id, and `CoverQuery(srid, region) ([]CellRange, error)` returning the cell-id ranges covering a query region (a rectangle, or a distance-bounded disc on a plane). Implement `planarCoverer` for SRID 0 over a configurable bounded domain (default `[-(1<<31), (1<<31)-1]` per axis, per `research.md`). Validate in isolation: a point encodes to one cell; `CoverQuery` of a region returns ranges that include the cell of every point inside the region (no false negatives), checked against brute force on random points. In the same milestone, write a throwaway test that constructs an `AccessPath` and confirms coverer-produced ranges can be injected as `path.Ranges` and drive an index scan, this de-risks the planner hook before committing to its final location.

Second, wire DDL. Add an internal builtin `tidb_spatial_key(geom) -> bigint` in a new `pkg/expression/builtin_spatial.go`, registered in the `funcs` map, that calls `spatial.EncodePoint` on the decoded point. Creating a spatial index on a `POINT` column generates a hidden virtual generated column whose `GeneratedExprString` is `tidb_spatial_key(<col>)` and builds a plain (non-MVI) secondary index on it, reusing the expression-index path. Record minimal metadata on `IndexInfo` marking it spatial and recording the source column and SRID so the planner can recognize it. Enforce the `POINT`, `NOT NULL`, SRID-constrained column restriction at DDL validation.

Third, the planner hook and refine. In access-path construction for a `DataSource`, for each spatial index whose point column appears in a recognized spatial predicate (`ST_Distance(col, const) <= r`, `ST_Distance_Sphere(...) <= r`, `ST_Contains(const_poly, col)`, `ST_Within(col, const_poly)`, or a rectangular MBR test), evaluate the constant query region at plan time, call `CoverQuery`, set the resulting cell ranges as the path's `Ranges`, and leave the original exact predicate in `TableFilters` so the Selection above the scan refines. Confirm via `EXPLAIN` and result-equivalence. The bounded form `WHERE within radius ORDER BY distance LIMIT k` is supported automatically: the index serves the `WHERE`, an ordinary `TopN` sorts the small candidate set.

Fourth, add `s2Coverer` for SRID 4326 using `github.com/golang/geo/s2`, behind the same `Coverer` interface, with spherical-cap covering for distance queries and correct antimeridian/pole handling. No code above the coverer interface changes.

Fifth, the correctness and compatibility pass.

## Concrete Steps

Run from the repository root.

Prototype the SRID 0 coverer and the range-injection proof (Milestone 1). Because new files and a new top-level test are added, regenerate Bazel metadata, and enable failpoints around package tests per repository policy:

    make bazel_prepare
    make failpoint-enable
    go test ./pkg/util/spatial/... -run TestPlanarCoverNoFalseNegatives -v
    make failpoint-disable

Expected: the test fails before `planarCoverer` is implemented and passes after; the brute-force comparison reports zero false negatives over the random sample.

For DDL and planner milestones, use targeted package tests plus a small integration test under `tests/integrationtest` (record per `docs/agents/testing-flow.md`). Example end-to-end check after Milestone 3:

    make failpoint-enable
    go test ./pkg/executor/... -run TestSpatialPointIndexEquivalence -v
    make failpoint-disable

Before any final-status claim, run the `Ready` profile from `.agents/skills/tidb-verify-profile` (includes `make lint` because code changes).

## Validation and Acceptance

Acceptance is behavior, not compilation.

- Milestone 1: coverer unit test fails before, passes after; zero false negatives vs brute force; the range-injection prototype drives an index scan.
- Milestone 2: after `CREATE SPATIAL INDEX` on a seeded `POINT` column, the index entry count equals the row count (one entry per row), and later inserts add one entry each.
- Milestone 3: for a seeded table, `ST_Distance`-within and `ST_Contains` queries return identical rows with the index dropped and created, and `EXPLAIN` shows a spatial index range scan plus a refine filter rather than a full scan; `WHERE within radius ORDER BY distance LIMIT k` returns the same ordered rows as the no-index plan.
- Milestone 4: the same equivalence holds for SRID 4326 data, including a query region crossing the antimeridian and one near a pole.
- Milestone 5: targeted tests pass; `make bazel_prepare` changes are included; `make lint` is clean.

End-to-end scenario proving the feature: seed a points table, run "within radius of (x,y) order by distance limit 20" with the index dropped and then created, confirm identical ordered results while `EXPLAIN` shows index use only in the second case.

## Idempotence and Recovery

The coverer package and its tests are pure and rerunnable. `CREATE/DROP SPATIAL INDEX` use the existing DDL job framework, so a failed backfill rolls back cleanly. Each milestone is additive; the index kind is not exposed to users until Milestone 3, so reverting an early milestone is cheap.

## Artifacts and Notes

Capture as work proceeds: the `EXPLAIN` output showing the spatial access path; the index-entry-count check; and the measured false-positive ratio of the covering on representative point data (e.g. the Capital Bikeshare dataset), since that drives cell-depth tuning noted in `research.md`.

## Interfaces and Dependencies

In `pkg/util/spatial` (final path to be confirmed):

    type CellRange struct{ Lo, Hi int64 }

    type Coverer interface {
        EncodePoint(srid uint32, x, y float64) (int64, error)
        CoverQuery(srid uint32, region QueryRegion) ([]CellRange, error)
    }

Implementations: `planarCoverer` (SRID 0, bounded domain), `s2Coverer` (SRID 4326, via `github.com/golang/geo/s2`, Apache 2.0). A thin `PointAccessor` wraps the prerequisite type's EWKB decode so type churn is localized. The internal builtin `tidb_spatial_key` lives in `pkg/expression/builtin_spatial.go` and is registered in `pkg/expression/builtin.go`. DDL reuses the expression-index path (`pkg/ddl/index.go`); the write path reuses `PlainIndexKVGenerator` (`pkg/table/tables/index.go`); the planner hook lives in access-path construction (`pkg/planner/core`, possibly with a small `pkg/util/ranger` addition).

## Revision note

2026-06-24: Initial points-only MVP plan created, grounded in verified integration points. Concrete decisions: hidden-generated-column expression index, POINT/NOT NULL/SRID-constrained restriction, SRID 0 first then 4326, refine via retained filter. Open choice: user-facing DDL grammar.
