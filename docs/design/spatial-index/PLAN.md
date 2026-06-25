# Spatial Index for TiDB (TiKV MVI-based, SRID 0 and 4326)

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it. Background and the full survey behind every decision here live in the companion `docs/design/spatial-index/research.md`.

## Purpose / Big Picture

After this change, a TiDB user can create a spatial index on a `GEOMETRY` column and have proximity and containment queries served by selective index lookups instead of full table scans. Concretely, given a table of store locations, a query for "all stores within 10 km of a point" (a distance-bounded predicate) and "which region contains this point" (`ST_Contains` / `ST_Within`) will use the index.

You can observe it working by running `EXPLAIN` on such a query and seeing a spatial index access path (a range scan over covering-cell keys) plus a refine filter, instead of a `TableFullScan`, and by confirming the result rows are identical with and without the index.

The index targets **TiKV** (the OLTP path) and reuses TiDB's existing multi-valued-index (MVI) machinery: one geometry is decomposed into the set of grid cells covering it, and each cell becomes an ordered index entry (one row, many cell keys). Spatial-overlap queries become cheap cell-key range scans followed by an exact-geometry refine. The covering logic is kept in an engine-neutral package so a future TiFlash columnar spatial index can reuse it, but no TiFlash plumbing is built here.

Scope boundary: this project is the **index only**. The `GEOMETRY` data type, EWKB storage, and spatial functions are a prerequisite (see `Context and Orientation`), not deliverables of this plan.

Concrete first deliverable: the points-only MVP is planned in detail in `docs/design/spatial-index/PLAN-points-mvp.md` (a `POINT`-column secondary index for SRID 0 then 4326, modeled as a hidden-generated-column expression index). This file remains the broader roadmap; the MVP plan is where implementation starts.

## Progress

- [ ] Milestone 0: Track the independently-landing geometry type; code the covering package against its contract behind a `Geometry` abstraction so pre-merge churn stays localized.
- [ ] Milestone 1: SRID 0 planar quadtree coverer + cell-key encoding, validated in isolation (prototype).
- [ ] Milestone 2: `CREATE SPATIAL INDEX` DDL on a `GEOMETRY` column, writing MVI-style covering entries (insert path + backfill).
- [ ] Milestone 3: Planner access path + executor filter-and-refine for `ST_Intersects`/`ST_Contains`/`ST_Within` and distance-bound predicates (SRID 0).
- [ ] Milestone 4: S2 coverer for SRID 4326 behind the same `CellCoverer` interface.
- [ ] Milestone 5: Correctness and compatibility test pass (result equivalence with/without index; MySQL GIS-suite subset).
- [ ] (Deferred, separate effort) Coprocessor pushdown of the refine predicate; expanding-ring KNN operator; TiFlash path.

## Surprises & Discoveries

- Observation: TiDB already has the exact one-row-to-many-keys primitive needed (the multi-valued index for JSON arrays).
  Evidence: `pkg/meta/model/index.go` (`MVIndex`), `pkg/ddl/index.go` sets it when an indexed column has `IsArray()`; `pkg/statistics/analyze.go` notes MVI row/NDV can exceed table row count due to fan-out.
- Observation: the literal "inverted index" in TiDB is unrelated to this project.
  Evidence: `ColumnarIndexTypeInverted` in `pkg/meta/model/index.go` is a TiFlash columnar index for integer columns; the TiKV fan-out primitive we reuse is the MVI.
- Observation: prerequisite scaffolding already has substantial prior art that was closed for lack of progress, not for being wrong.
  Evidence: PRs #66602 (parser types + SRID), #38611 (GEOMETRY type), tikv/tikv#13652, and design doc PR #38916. The design doc explicitly defers the index as needing more research.

## Decision Log

- Decision: Target the TiKV OLTP path via MVI machinery first; keep the covering logic engine-neutral so a future TiFlash (TiCI columnar) spatial index can reuse it, but build no TiFlash plumbing now.
  Rationale: the primary user need is OLTP "find near me / which region" point queries; MVI already provides the distributed one-row-many-keys primitive. Over-generalizing to TiFlash up front would block the first implementation.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Treat the geometry type, EWKB storage, and spatial functions as a prerequisite that lands independently in master; code the index against it behind a `Geometry` abstraction, expecting minor churn before merge.
  Rationale: keeps scope to the index; the type work (prior art #66602/#38611) is owned separately. The only coupling is a small contract (read EWKB+SRID, compute MBR, recognize the optimized predicates), so localizing it limits churn.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: SRID 0 planar coverer defaults its domain to `[-(1<<31), (1<<31)-1]` per axis (a generous bound covering common coordinate systems; CockroachDB's docs use the same value), exposed as an overridable per-index option; out-of-domain coordinates are clamped to the boundary cell (still indexed and correct, over-covered near the edge), not rejected.
  Rationale: a safe catch-all that contains common CRSes (Web Mercator ±20,037,508 m, lat/long ±180/±90), while the override lets meter- or degree-scale data get tight leaf cells (leaf size = domain-width / 2^maxLevel).
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Adopt `github.com/golang/geo/s2` (Apache 2.0, Google's official Go S2 port) for the SRID 4326 coverer rather than an in-house spherical coverer.
  Rationale: correct antimeridian/pole handling and spherical-cap covering are subtle; the canonical, license-compatible port avoids reimplementing them.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Distinguish two cell knobs: max level (precision) and max cells per geometry (fan-out cap). A point always writes exactly one MVI entry regardless of depth; only extended geometries fan out, bounded by the cap (default 4; CockroachDB's published default is also 4).
  Rationale: point-heavy "store locations" workloads then have one index entry per row independent of precision, keeping write amplification predictable.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: 2D index is required; design the cell-key encoding to be dimension-tagged so 3D can be added later, but do not build 3D.
  Rationale: MySQL and MariaDB are 2D-only, so 2D meets the compatibility bar; 3D is gated on the type carrying a Z coordinate, which the 2D EWKB scaffolding does not provide. PostGIS supports 3D/4D with an ND-GiST index, so 3D remains a sensible future extension.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Use two coverers behind one `CellCoverer` interface: a planar quadtree over a bounded domain for SRID 0, and a spherical S2 cell system for SRID 4326. Deliver SRID 0 first.
  Rationale: SRID 0 is an unbounded abstract plane needing a fixed domain (a cell grid inherently requires a bounded extent; CockroachDB likewise requires bounds); SRID 4326 needs true spherical handling for correctness at the antimeridian, poles, and for distance-cap covering. SRID 0 first proves the distributed pipeline before spherical complexity lands. Full discussion in `research.md` -> "SRID cell scheme".
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Use cell-covering over MVI as a secondary spatial index; record the ER-tree (He & Chen 2022) as the alternative for a clustered spatial organization, not rejected on implementation-layer grounds.
  Rationale: corrected understanding after reading the paper. The ER-tree is not a pure R-tree but a hybrid on a Hilbert space-filling-curve foundation; its "no dual query" advantage comes from storing the data clustered by Hilbert(geometry) with the R-tree embedded in the same SSTable pointing at data blocks (not a separate geometry->PK index). That advantage is contingent on clustering the table by the geometry, which competes with the table's normal primary-key clustering and needs TiKV/RocksDB engine work. This project targets a secondary index (table keeps its normal PK), so cell-covering over MVI fits and needs no engine work; the trade is a refine plus handle lookback. The ER-tree clustered organization stays a legitimate option if spatial-clustered tables become a goal. Since we control both TiDB and TiKV, the choice is driven by clustered-vs-secondary semantics, not by which binary owns the code. CockroachDB (closest analog) also chose cell covering as a secondary index. Full analysis in `research.md`.
  Date/Author: 2026-06-23, Mattias Jonsson.

- Decision: Adopt the Sunny Bains review (2026-06-25) bbox-in-value refinement (store the geometry bounding box, optionally full EWKB for a covering index, for a cheap pre-lookback filter that recovers MBR pruning inside the flat MVI without a tree). For partitioned tables, default to a global spatial index but treat local-vs-global as a workload choice, not a spatial rule.
  Rationale: the local-vs-global tradeoff is the general secondary-index one, not spatial-specific. What is spatial-specific is that pure spatial predicates do not constrain the partition key, so they cannot prune partitions and a local spatial index fans out to all of them; hence global is the stronger default. The exception is partition-key-co-constrained workloads (e.g. multi-tenant partitioned by `tenant_id` with queries carrying `tenant_id`), where a local index prunes and avoids global-index overhead. `partition_id` in a global entry is the `PARTITION BY` physical partition id, not the PK (already in the index key); reuses TiDB's existing global-index machinery. Open questions tracked in `research.md` -> "Index value contents and table partitioning".
  Date/Author: 2026-06-25, Mattias Jonsson.

- Decision: Filter-and-refine with the exact predicate evaluated in TiDB initially; coprocessor pushdown deferred.
  Rationale: the index returns a candidate superset; correctness lives in the refine step. TiDB-side refine is simplest to get correct first; pushdown is a performance follow-up.
  Date/Author: 2026-06-23, Mattias Jonsson.

## Outcomes & Retrospective

To be filled in at major milestones. Initial entry pending Milestone 1 prototype.

## Context and Orientation

A novice reader needs these definitions and locations.

**Geometry / EWKB**: a geometry value is stored as Extended Well-Known Binary: a 4-byte little-endian SRID prefix followed by standard OGC WKB (byte order, geometry type, coordinates), i.e. `<srid><wkb>`. This matches MySQL. All geometry types share `mysql.TypeGeometry` (`0xFF`) with a geometric-subtype constraint.

**SRID**: Spatial Reference System Identifier. SRID 0 is an abstract unitless Cartesian plane (Euclidean distance). SRID 4326 is WGS 84 latitude/longitude on the globe. These are the two this project supports.

**MBR**: minimum bounding rectangle, the smallest axis-aligned box containing a geometry. The 2D MBR is what gets covered by cells.

**Cell covering**: decomposing a geometry (or a query region) into a set of hierarchical grid cells that contain it. Indexing stores one entry per covering cell; querying scans the cells covering the query region. Coarse cells yield a candidate superset, refined by exact geometry tests.

**Multi-valued index (MVI)**: TiDB's existing index kind where one row produces many index entries. Defined by `MVIndex` in `pkg/meta/model/index.go`; set in `pkg/ddl/index.go` for array columns; used today for JSON `MEMBER OF`/`JSON_CONTAINS`/`JSON_OVERLAPS`. This project reuses the same fan-out to write one index entry per covering cell.

**Prerequisite scaffolding (must exist before Milestone 1 finishes)**. The index needs, and only needs, from the type layer:

1. A readable stored geometry value (EWKB bytes + SRID). Prior art: PR #38611, tikv/tikv#13652.
2. Computation of a geometry's 2D MBR and enumeration of its shape for covering. The `github.com/twpayne/go-geom` library (adopted by the prior design) provides bounds and parsing.
3. The predicates the index optimizes, recognizable by the planner: an intersects/contains relation and a distance-bound predicate, with exact evaluation available in TiDB. Parser/type acceptance prior art: PR #66602 (enable by removing the `TypeGeometry` rejection in `pkg/planner/core/preprocess.go`).

The geometry type and basic functions land independently in master. Milestone 0 is therefore not implementation work but coupling management: code the covering package against the three points above behind a `Geometry` abstraction, so the expected pre-merge churn (signatures, package paths) stays confined to that boundary rather than spreading through the index, DDL, planner, and executor code.

**Where the index work lands** (repository-relative entry points, to be confirmed during implementation):

- New engine-neutral covering package, e.g. `pkg/types/spatial/` or `pkg/util/spatial/`: `CellCoverer`, the two coverers, and cell-key encoding.
- Index model and DDL: `pkg/meta/model/index.go`, `pkg/ddl/index.go` (a spatial index kind that reuses the MVI fan-out write path), and the parser's `SPATIAL INDEX` grammar (`pkg/parser`).
- Planner access-path selection: `pkg/planner/core` (recognize spatial predicates, emit a covering-cell range scan plus a refine filter).
- Executor refine: `pkg/executor` (exact geometry predicate over candidate rows).

## Plan of Work

The work proceeds bottom-up: prove the covering math in isolation, then the storage/DDL fan-out, then the query path, then the spherical coverer, then compatibility.

First, in the new covering package, define the `CellCoverer` interface and the order-preserving `CellKey` encoding (dimension-tagged for a future 3D extension). Implement `planarQuadtreeCoverer` for SRID 0 over a bounded, configurable coordinate domain with a fixed maximum subdivision depth and a maximum cell count per geometry. Provide `Cover(geom)` for stored geometries and `CoverQuery(region)` returning candidate cell-key ranges for a query polygon and for a distance-bounded disc. This is the riskiest core and is validated as a prototype before any storage wiring.

Second, add the spatial index kind to the index model and DDL. On row insert/update, compute the covering and write one index entry per cell using the existing MVI fan-out write path. Implement `CREATE SPATIAL INDEX` (parser grammar already has the `Rtree` token and a `SPATIAL INDEX` surface to wire) and backfill of existing rows through the index backfill framework, covering each existing geometry.

Third, wire the query path. In the planner, recognize `ST_Intersects`/`ST_Contains`/`ST_Within`/MBR predicates and distance-bound predicates over an indexed geometry column, and produce an access path that scans the candidate cell-key ranges from `CoverQuery` and applies an exact-geometry refine filter. Initially evaluate the refine in TiDB. Confirm via `EXPLAIN`.

Fourth, implement `s2Coverer` for SRID 4326 behind the same interface, including correct antimeridian/pole handling and spherical-cap covering for distance queries. No code above the `CellCoverer` seam changes.

Fifth, the compatibility and correctness pass: result-equivalence tests (same rows with and without the index, including across the antimeridian and near poles for 4326) and a subset of the MySQL GIS test suite via `mysql-tester`.

## Concrete Steps

Run from the repository root unless stated.

Prototype the SRID 0 coverer in isolation (Milestone 1). After adding the package and a unit test:

    make failpoint-enable
    go test ./pkg/util/spatial/... -run TestPlanarQuadtreeCover -v
    make failpoint-disable

Expected: the test asserts that a point covers to exactly one leaf cell, a polygon covers to a bounded set of cells whose union contains the polygon's MBR, and that `CoverQuery` of a region returns ranges that include every cell a contained point would be indexed under (no false negatives).

Because new Go files and a new top-level test function are added, regenerate Bazel metadata before relying on Bazel builds:

    make bazel_prepare

For the DDL and query milestones, prefer targeted package tests and a small integration test under `tests/integrationtest` (record per `docs/agents/testing-flow.md`). Before any final-status claim, run the `Ready` verification profile from `.agents/skills/tidb-verify-profile`, which includes `make lint` when code changed.

## Validation and Acceptance

Acceptance is human-verifiable behavior, not just compilation:

- Milestone 1: the prototype unit test fails before the coverer is implemented and passes after; covering of a sample geometry has no false negatives against a brute-force point-membership check.
- Milestone 2: after `CREATE SPATIAL INDEX`, the index entry count for a known table matches the sum of per-row covering-cell counts (fan-out is observable), and backfill plus subsequent inserts both populate entries.
- Milestone 3: for a seeded table, a `ST_Contains`/distance-bound query returns identical rows with and without the index, and `EXPLAIN` shows a covering-cell range scan plus a refine filter rather than a full scan.
- Milestone 4: the same equivalence holds for SRID 4326 data, including query regions that cross the antimeridian and that sit near a pole.
- Milestone 5: the chosen MySQL GIS-suite subset passes via `mysql-tester`, and Dumpling/Lightning round-trips of a table with a spatial index are unaffected.

The single end-to-end scenario that proves the feature: seed a "stores" table with points, run "within 10 km of (x,y) order by distance limit 20" both with the index dropped and created, and confirm identical ordered results while `EXPLAIN` shows index use only in the second case.

## Idempotence and Recovery

The covering package and tests are pure and safe to rerun. DDL steps are guarded by `IF NOT EXISTS`/`IF EXISTS` where applicable; a failed `CREATE SPATIAL INDEX` backfill rolls back via the existing DDL job framework, leaving no partial index. The prototype and integration tests can be re-recorded idempotently. If a milestone's design proves wrong, revert is cheap because each milestone is additive and gated behind the index kind not yet being exposed to users until Milestone 3.

## Artifacts and Notes

Capture, as work proceeds: the `EXPLAIN` output showing the spatial access path; index-entry counts demonstrating fan-out; and measured candidate-set precision (false-positive ratio) for representative data, since that drives the cell-depth tuning called out as an open question in `research.md`.

## Interfaces and Dependencies

The stable seam, in the new engine-neutral package (final path to be confirmed, e.g. `pkg/util/spatial`):

    type CellKey []byte // order-preserving, dimension-tagged cell identifier

    type CellCoverer interface {
        // Cover returns the cell keys a stored geometry is indexed under.
        Cover(geom Geometry) ([]CellKey, error)
        // CoverQuery returns the candidate cell-key ranges to scan for a query region.
        CoverQuery(region QueryRegion) ([]CellKeyRange, error)
    }

Implementations: `planarQuadtreeCoverer` (SRID 0, domain default `[-(1<<31), (1<<31)-1]` per axis, overridable), `s2Coverer` (SRID 4326). Dependencies: `github.com/twpayne/go-geom` for geometry parsing and bounds (already proposed by the prior design); `github.com/golang/geo/s2` (Apache 2.0, Google's Go S2 port) for the 4326 coverer. The index model/DDL reuse the existing MVI write path (`pkg/ddl/index.go`); the planner access-path and executor refine reuse standard scan-plus-filter plumbing in `pkg/planner/core` and `pkg/executor`.

Everything above the `CellCoverer` seam is SRID- and engine-agnostic, which is the boundary that keeps a future TiFlash columnar spatial index reusing the covering logic without re-architecture.

## Revision note

2026-06-23: Initial plan created. Decisions captured from design discussion: TiKV/MVI target with engine-neutral seam, scaffolding as prerequisite, 2D required with dimension-tagged encoding for future 3D, two coverers behind one interface with SRID 0 delivered first. No code yet.
