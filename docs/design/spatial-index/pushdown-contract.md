# Design Note: Spatial Coprocessor Pushdown Contract

Companion to the spatial-index design ([#69473]) and the original geospatial design
([#38916]); tracking issue [#6347]. This note specifies the **cross-repo contract**
that lets TiDB push a spatial *refine* predicate (e.g. `ST_Within`) down to TiKV so
it is evaluated next to the data instead of at the TiDB root. It is the design
reference for three PRs: **P1** (tipb sigs), **K1** (TiKV evaluator), **T9** (TiDB
wiring).

[#6347]: https://github.com/pingcap/tidb/issues/6347
[#38916]: https://github.com/pingcap/tidb/pull/38916
[#69473]: https://github.com/pingcap/tidb/pull/69473

## Motivation

The spatial index produces *candidate* rows (cell-range scan + bbox prefilter); the
exact OGC predicate must still be evaluated to drop false positives. Without
pushdown that `Selection` runs at the TiDB root, so TiKV ships **every candidate**
to TiDB. Measured on a 5M-row cluster: a full-scan `ST_Within` at the root ships all
~5M rows and takes ~3.3 s; pushed to the coprocessor it ships only the matching rows
(~2.4k at 0.5% selectivity — **~200× fewer**) and takes ~34 ms (**~50–100×**). The
contract below is what makes that safe and correct.

## Scope of the contract

The DE-9IM predicates, as binary functions over two geometries:

`ST_Within, ST_Contains, ST_Intersects, ST_Equals, ST_Disjoint, ST_Touches,
ST_Crosses, ST_Overlaps, ST_Covers, ST_CoveredBy`

Constructors, accessors, and measurement functions (`ST_X`, `ST_Area`,
`ST_Distance`, …) are **not** in this contract — they stay at the root for now.

## 1. Protocol (tipb) — PR P1

Add ten `ScalarFuncSig` enum values, **7100–7109**, one per predicate
(`StWithin = 7100 … StCoveredBy = 7109`). This is the entire tipb change; it is
shared verbatim by TiDB and TiKV so the two sides agree on the wire identity of
each predicate.

- A pushed predicate is an `Expr` of type `ScalarFunc` with `Sig` = one of the
  above and exactly **two children**, each an EWKB-valued expression (a `ColumnRef`
  for the stored geometry and a `Constant` for the query geometry — or two columns).
- Return type is `BIGINT` (`1`/`0`), or NULL (see §4).

## 2. Value & type encoding

- A geometry value on the wire is **EWKB**: a 4-byte little-endian SRID prefix
  followed by standard OGC WKB — `<srid: u32 LE><WKB>`.
- Its `FieldType.Tp` is `TypeGeometry` (0xFF); its **EvalType is `Bytes`**. Both
  sides must map `TypeGeometry → Bytes` (TiKV: `eval_type.rs`; the K1 fix). Without
  this the RPN builder rejects the column/constant as "Unsupported type: Geometry".
- TiKV strips the 4-byte SRID prefix and parses the remainder as WKB. TiDB has
  already gated the query by SRID (predicate over a single-SRID index), so TiKV does
  **not** re-check SRID equality.

## 3. Plan-shape contract (TiDB) — PR T9

- The pushed predicate is a `cop[tikv]` **`Selection`** placed **after** the index
  cell-range scan and the bbox prefilter (the `Selection(Probe)` on the table side
  of an `IndexLookUp`, or directly on a `TableScan`). So TiKV evaluates the exact
  predicate only on rows that already survived bbox pruning.
- Pushdown is gated by `scalarExprSupportedByTiKV` (the allow-list) + `columnToPBExpr`
  permitting `TypeGeometry`. A TiKV that predates these sigs must never be sent them
  — handled by the standard storage-engine/function allow-list, plus the
  `mysql.expr_pushdown_blacklist` escape hatch (used to A/B the benefit).
- **Pushdown is an optimization, never a correctness dependency.** With the predicate
  forced to the root (blacklist) the result must be identical — TiDB's root evaluator
  (`pkg/util/geomrel`, simplefeatures) and TiKV's evaluator (`geo` crate) implement
  the same OGC spec.

## 4. Semantics contract (must match on both sides)

Both evaluators MUST agree, bit for bit, with each other and with TiDB's root
evaluator. Reference: OGC Simple Features / DE-9IM (= GEOS/JTS/PostGIS/MySQL).

- **Boundary:** a point on a polygon edge is `Within = false`, `Touches = true`,
  `CoveredBy = true`, `Intersects = true`.
- **`ST_Crosses` dimension gate (MySQL-compatible):** returns **NULL** unless
  `dim(a) < dim(b)` or both operands are lines; the symmetric form most libraries
  compute is wrong vs MySQL. Applied on both sides (TiDB `builtinGeomRelSig`, TiKV
  `st_crosses`).
- **NULL:** if either argument is NULL the result is NULL.
- **WKB types:** points, lines, polygons, and the multi-/collection types (OGC
  codes 1–7), 2D.

**Validation (already done, see `e2e-pushdown-log.md`):** on a real cluster, all 8
MySQL-comparable predicates match MySQL 8.0 over **3000 random geometry pairs**, and
the index path is sound (FORCE INDEX == full scan == MySQL over 3000 geoms × 60
windows).

## 5. TiKV evaluator shape — PR K1

`components/tidb_query_expr/src/impl_spatial.rs` (~500 lines):
`decode_ewkb` (strip SRID → WKB reader, length-capped against corrupt counts) →
`relate` (geo crate `Relate` → DE-9IM `IntersectionMatrix`) → one `#[rpn_fn]` per
predicate. Plus the `geo` crate dependency and the `FieldTypeTp::Geometry →
EvalType::Bytes` fix. `st_crosses` is a hand-written rpn_fn (the dimension gate).

## 6. Known limitations (accepted for v1)

- **Planar 4326 refine.** The S2 *covering* is geodesic, but the refine (both root
  and cop) is planar, so results near 4326 boundaries differ from MySQL's geodesic
  4326. Because **both** TiDB-root and TiKV use planar libraries, pushdown does not
  *change* the answer — it diverges from MySQL identically either way. Geodesic
  predicates are future work.
- **Empty geometry.** A predicate with an empty-geometry operand returns 0 here vs
  NULL in MySQL (rare; a candidate follow-up — would be an empty-operand → NULL
  guard on both sides).

## Open questions for design review

1. Capability/version negotiation: rely solely on the function allow-list, or add an
   explicit TiKV capability gate so a mixed-version cluster never receives an unknown
   sig (it would error the cop request rather than fall back)?
2. Should the SRID prefix be dropped on the wire (TiDB already gated SRID) to save
   bytes, or kept for self-describing values / future cross-SRID support?
3. Axis convention for 4326 (coord0 = lng here vs lat in MySQL) — settle in the
   index design (#69473) since it also affects the S2 covering, not just the refine.
