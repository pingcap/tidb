# TiKV coprocessor: spatial predicate pushdown — handoff

Contract for implementing the geometry DE-9IM predicates in the TiKV Rust
coprocessor, so TiDB's spatial-index refine filter can be evaluated at the
storage node. Companion to `bbox-pushdown-design.md` (Layer B). The **tipb
protocol part is done** (`~/repos/tipb`, branch `spatial-pushdown`, commit
`eacc7e9`); regenerate the TiKV protobuf bindings from that branch.

## 1. What to implement

Evaluate the new `ScalarFuncSig` values in `components/tidb_query_expr` (the
RPN expression evaluator). Each is a binary predicate over two **EWKB** byte
strings returning an `Int` (0/1, or NULL if either arg is NULL).

| ScalarFuncSig | value | predicate | OGC meaning (a, b) |
| --- | --- | --- | --- |
| `StWithin` | 7100 | ST_Within | a is within b |
| `StContains` | 7101 | ST_Contains | a contains b |
| `StIntersects` | 7102 | ST_Intersects | a intersects b |
| `StEquals` | 7103 | ST_Equals | a spatially equals b |
| `StDisjoint` | 7104 | ST_Disjoint | a and b are disjoint |
| `StTouches` | 7105 | ST_Touches | a touches b |
| `StCrosses` | 7106 | ST_Crosses | a crosses b |
| `StOverlaps` | 7107 | ST_Overlaps | a overlaps b |
| `StCovers` | 7108 | ST_Covers | a covers b |
| `StCoveredBy` | 7109 | ST_CoveredBy | a is covered by b |

Both args evaluate as `bytes` (the column value and the query constant). The
result column is `Int` with `TypeTiny`/`TypeLonglong`.

## 2. EWKB value layout

A geometry value is **EWKB**: a 4-byte little-endian SRID prefix followed by
standard OGC WKB. Strip the first 4 bytes, parse the remainder as WKB.

```
<srid: u32 little-endian> <standard WKB>
```

Examples (hex, SRID 0):

- `POINT(2 2)` →
  `00000000` `01` `01000000` `0000000000000040` `0000000000000040`
  (srid=0 · byteorder=LE · type=1(Point) · x=2.0 · y=2.0)
- `POLYGON((0 0,4 0,4 4,0 4,0 0))` →
  `00000000` `0103000000` `01000000` `05000000` `0000000000000000…`
  (srid=0 · LE · type=3(Polygon) · 1 ring · 5 points · coords…)

The SRID byte order is little-endian; the WKB itself carries its own byte-order
byte per the OGC spec. For the POC, SRID 0 (planar) and 4326 (WGS84) appear;
predicates are computed on the raw coordinates (TiDB has already gated by SRID).

## 3. Correctness contract

Results MUST match TiDB's reference evaluator `pkg/util/geomrel` (which uses the
pure-Go `github.com/peterstace/simplefeatures`, the same OGC spec as
GEOS/JTS/PostGIS). Recommended Rust engine: the `geo`/`geos` crates. The
boundary semantics matter (e.g. a point on a polygon edge is `Within=false`,
`Touches=true`).

### Test corpus (a, b → expected)

Generated from `geomrel.Relate` over SRID-0 geometries. Use as Rust unit-test
vectors (encode the WKT to EWKB, evaluate, compare). `1`=true, `0`=false.

```
a = POINT(2 2),  b = POLYGON((0 0,4 0,4 4,0 4,0 0))   # interior point
  Within=1 Contains=0 Intersects=1 Equals=0 Disjoint=0 Touches=0 Crosses=0 Overlaps=0 Covers=0 CoveredBy=1
a = POINT(5 5),  b = POLYGON((0 0,4 0,4 4,0 4,0 0))   # outside
  Within=0 Contains=0 Intersects=0 Equals=0 Disjoint=1 Touches=0 Crosses=0 Overlaps=0 Covers=0 CoveredBy=0
a = POINT(0 0),  b = POLYGON((0 0,4 0,4 4,0 4,0 0))   # on the boundary (corner)
  Within=0 Contains=0 Intersects=1 Equals=0 Disjoint=0 Touches=1 Crosses=0 Overlaps=0 Covers=0 CoveredBy=1
a = LINESTRING(0 0,4 4),  b = POLYGON((0 0,4 0,4 4,0 4,0 0))   # diagonal across
  Within=1 Contains=0 Intersects=1 Equals=0 Disjoint=0 Touches=0 Crosses=0 Overlaps=0 Covers=0 CoveredBy=1
a = POLYGON((0 0,2 0,2 2,0 2,0 0)),  b = POLYGON((1 1,3 1,3 3,1 3,1 1))   # partial overlap
  Within=0 Contains=0 Intersects=1 Equals=0 Disjoint=0 Touches=0 Crosses=0 Overlaps=1 Covers=0 CoveredBy=0
a = POLYGON((0 0,4 0,4 4,0 4,0 0)),  b = POLYGON((0 0,4 0,4 4,0 4,0 0))   # equal
  Within=1 Contains=1 Intersects=1 Equals=1 Disjoint=0 Touches=0 Crosses=0 Overlaps=0 Covers=1 CoveredBy=1
a = POLYGON((0 0,1 0,1 1,0 1,0 0)),  b = POLYGON((5 5,6 5,6 6,5 6,5 5))   # disjoint
  Within=0 Contains=0 Intersects=0 Equals=0 Disjoint=1 Touches=0 Crosses=0 Overlaps=0 Covers=0 CoveredBy=0
```

(`pkg/util/geomrel` can regenerate / extend this corpus — see the throwaway
`TestGenCorpus` pattern in the PR history.)

## 4. How TiDB sends it (Expr shape)

The refine predicate is serialized as a standard `tipb.Expr`:

```
Expr{
  tp:         ScalarFunc,
  sig:        StWithin (7100) | … ,
  field_type: { tp: Tiny/Longlong },        // int result
  children: [
    <geometry column ref or constant bytes>, // arg a (EWKB)
    <geometry constant bytes>,               // arg b (EWKB, the query literal)
  ],
}
```

TiDB only pushes these down **after** the bbox pre-filter (Layer A) has pruned
the obvious non-matches, so the predicate runs on the bbox-surviving candidates.
No special framing beyond the standard scalar-function encoding.

## 5. Status / sequencing

- tipb sigs + go-tipb bindings: DONE (`spatial-pushdown` branch).
- TiDB-side wiring: DONE (TiDB branch `spatial-pushdown`). `geomRelPbCode` sets the
  sig in `getFunction`; `getSignatureByPB`/`geomRelFromPbCode` reverse-map it;
  `scalarExprSupportedByTiKV` allow-lists the predicates; `columnToPBExpr` now
  pushes `TypeGeometry` columns; go.mod has a local `replace` to the tipb fork.
  **Validated in unistore** (`TestPOCSpatialRefinePushdown`): the `ST_Within`
  refine is evaluated at `cop[tikv]` (table scan *and* index-lookup probe side)
  and returns the same rows as a root/full-scan evaluation.
- TiKV Rust evaluator (this doc): to do — can proceed in parallel against the
  corpus above. TiDB already emits the `Expr` shape in §4 for it to decode.

Note: the unistore validation reuses TiDB's Go evaluator via the pb round-trip,
so it proves the wiring/encoding but not a native Rust implementation — that is
exactly what the TiKV side adds.
