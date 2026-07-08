# Spatial Index POC — Architecture & Correctness Review

Review date: 2026-07-05. Scope: the full POC branch `spatial-index-poc`
(`098a20e0d7..a4fc776953`, "Phase A" through batch B4). Companion to
`review-plan.md` (milestones), `gaps.md` (known gaps), `storage-format.md`
(pre-GA encodings), and the design docs on the `spatial-index-design` branch.

Method: manual read of the algorithmic core (`pkg/util/spatial/coverer.go`,
`s2.go`, `pkg/util/geomrel/geomrel.go`, `geodesic.go`,
`pkg/planner/core/spatial_resolve_index.go`, the DDL rewrite in
`pkg/ddl/create_table.go` / `executor.go`, the `tidb_spatial_key*` builtins,
pushdown wiring), plus three breadth sweeps (all of `builtin_geo.go`;
parser/type/DDL plumbing; a test-suite audit). Line numbers are as of
`a4fc776953`.

## Verdict

The architecture is right and the POC proves it. Cell-covering over a hidden
expression index is the correct design for TiKV's ordered keyspace, and
modeling it as `[prefix..., tidb_spatial_key(g), ST_X(g), ST_Y(g)]` (or MVI +
4 bbox columns) reuses battle-tested machinery end to end: online backfill,
MVI/IndexMerge, ADMIN CHECK, and DROP INDEX hidden-column cleanup all just
work. The axis-order discipline, where spatial implementations usually rot,
is impeccable: every lat/lng-sensitive site (storage, `EncodePointS2`,
cap/rect covers, geodesic refine, GeoJSON, accessors) was traced and no flip
bugs were found.

The problems cluster in exactly two places: (1) the index contract rests on
invariants nothing enforces (value SRID == declared SRID; stored bytes are
valid EWKB; the `GeneratedExprString` text format), and (2) the 4326 covering
and the geodesic refine disagree about what a polygon is. Both are fixable
within the architecture.

Finding classes: **A** can make the index return wrong rows; **B** are
user-visible bugs/compat; **C** are structural debts to pay before
graduation; **D** are algorithmic tuning; **E** is test coverage.

## A. Index-correctness findings (wrong results possible)

### A1. SRID is never enforced on write; key scheme dispatches on value SRID, planner on declared SRID

`ColumnInfo.Srid` (`pkg/meta/model/column.go:113`) is read only by DDL
validation and the resolver's `sridMatchesColumn`
(`spatial_resolve_index.go:846`); no write path checks it (`Datum.ConvertTo`
routes geometry through `convertToString`, `pkg/types/datum.go:1041`).
Meanwhile `tidb_spatial_key` keys each row by the **value's** EWKB SRID
(`builtin_geo.go:1940`). Insert a SRID-0 point into a declared-4326 indexed
column (or vice versa): the insert succeeds, the row gets a key in the wrong
key space, S2/planar query ranges silently miss it, and ADMIN CHECK passes
because the index is self-consistent with the generated column. MySQL rejects
the insert (error 3643).

Related: `Srid == 0` conflates "no SRID attribute" with "declared SRID 0" in
`validateSpatialColumn` (`create_table.go:1401`), even though
`mysql.SridFlag` exists and could distinguish them; and the raw column path
accepts arbitrary non-EWKB bytes with no validation at all.

**Fix:** one `TypeGeometry` case in the cast layer validating WKB + geometry
subtype + SRID against the target column closes every write path
(INSERT/UPDATE/LOAD DATA/IMPORT INTO) at once. First thing to fix.

### A2. 4326 region covering uses the planar vertex bbox while the refine is geodesic

`recognizeRegionPredicate` covers `EWKBBounds` (the vertex envelope,
`spatial_resolve_index.go:947`) via `CoverLatLngRectDegrees`, but the refine
builds an S2 polygon whose edges are great-circle arcs
(`geomrel/geodesic.go`). A geodesic edge between (lat 45, lng −80) and
(lat 45, lng 80) bulges to about latitude 80; a point at lat 75, lng 0 is
inside the polygon geodesically but far outside the vertex bbox, so its cell
is outside the covering: the index path drops it while the full scan returns
it. Any wide east-west polygon at mid latitude hits this; gaps.md's
"pathological large polygon" framing understates it. Worse, the injected bbox
filter (`buildBBoxConds` on the same vertex rect) independently excludes such
rows even if the cells matched.

**Fix:** cover the same object you refine with: build the `s2.Polygon` (the
code already exists in `geodesic.go`) and use
`RegionCoverer.Covering(poly)`, plus its `RectBound()` for the bbox
conditions. This also tightens the covering for free. The cap path is already
done right (`bboxRect`'s `asin(sinθ/cosφ)` longitude formula, pole handling,
and conservative margin all check out).

### A3. Plan-time constants are baked into the plan with no plan-cache guard

The resolver evaluates the query geometry and radius at optimize time and
hardcodes cell ranges, bbox constants, and the MVI `json_overlaps` cell array
into the plan (`spatial_resolve_index.go:909-960, 1011`). There is no
`SetSkipPlanCache` or cacheability check for spatial predicates anywhere. A
prepared `ST_Within(g, ?)` under prepared-statement or instance plan cache
would reuse stale ranges for new parameters: silent wrong results. Until the
resolver is parameter-aware, it should mark the statement uncacheable
whenever it fires (and a test should pin that).

### A4. The planned "gate 4326 region pushdown off" was never implemented

`OVERNIGHT-PLAN.md` item 2 states it as a MUST ("a geodesic root + planar cop
would break the pushdown-never-changes-the-answer contract"), and gaps.md
repeats it, but `scalarExprSupportedByTiKV` lists all ten `St*` predicates
unconditionally (`infer_pushdown.go:231`). The tests don't catch it because
unistore executes the same Go `geomrel.Relate` (geodesic dispatch included)
via `distsql_builtin.go:1168`; on real TiKV the geo-crate refine is planar,
so the answer would depend on where the Selection lands. The gate (block
pushdown when an argument's SRID can be 4326) is small; land it before any
real-TiKV run. Relatedly, pushdown is enabled against stock TiKV that has no
`ScalarFuncSig_St*` at all (custom tipb branch); that needs a
capability/feature gate before leaving POC.

### A5. Write path and read path quantize cell boundaries with different arithmetic

`EncodePoint` uses `quantize` (floor of `(v-lo)/(hi-lo)*side`,
`coverer.go:154`), but `CoverRect`'s descent tests node extents recomputed as
`minX + col*cellW` in floating point (`coverer.go:269-287`). Near a cell
boundary these can disagree by an ulp: a point that quantizes into cell k+1
while `cover` computes cell k+1's `nodeMinX` as strictly greater than the
query rect's max gets pruned, a false negative. It needs a point/rect landing
within ~1 ulp of a boundary, so it will not show up in randomized tests, but
it is structurally avoidable: quantize the rect corners with the same
function and do the descent/disjointness in integer cell coordinates, exactly
as `CoverFixedLevelCells` already does (which is why the MVI path doesn't
have this problem). This also removes the only float in the covering
contract.

### A6. `tidb_spatial_bbox` maps every decode error to NULL

`builtin_geo.go:1853-1857` treats any `EWKBBounds` error as "empty geometry",
so a malformed stored value (reachable per A1) yields NULL MBR columns, the
bbox filter rejects the row, and the query result shrinks with no error.
Distinguish "empty" (NULL is right) from "parse failure" (propagate the
error).

### A7. NaN handling is implementation-defined in the key path

NaN survives every params validation (`p.MinX >= p.MaxX` is false for NaN;
`ParsePlanarParams` happily parses `"NaN"`/`"Inf"` from an index COMMENT),
and `uint32(NaN * side)` in `quantize` is platform-dependent Go behavior:
nondeterministic index keys across architectures. The function ingest paths
reject NaN via simplefeatures validation, but the raw write path (A1) and
COMMENT params don't. Reject NaN/Inf explicitly in `ParsePlanarParams`,
`planarParams`, and `EncodePoint`. Also `recognizeDistancePredicate` accepts
a NaN radius (`radius < 0` is false for NaN).

### A8. `loop.Normalize()` forces the smaller-area interpretation of every 4326 ring

`geodesic.go:113`. With the odd-even `PolygonFromLoops` rule this handles
holes correctly (verified), but it silently flips any polygon meant to
enclose more than a hemisphere, and it makes ring orientation meaningless,
which is a semantic decision MySQL makes differently (MySQL 8 errors on
polygons larger than half the globe rather than reinterpreting). Fine for the
POC; record it as a deliberate semantic choice and validate against MySQL's
error behavior when doing error parity.

## B. User-visible bugs / compat

- **B1. `ALTER TABLE t ADD SPATIAL INDEX` is a silent no-op.** The grammar
  produces `ast.ConstraintSpatial`, but `AlterTableAddConstraint` has no case
  for it and falls through to `default: // Nothing to do now` returning
  success (`pkg/ddl/executor.go:1806-1831`). Only `CREATE SPATIAL INDEX` and
  inline CREATE TABLE work. Silent success is the worst failure mode; wire it
  to `createSpatialIndex` or error.
- **B2. Binary protocol can't return geometry.** `DumpBinaryRow` has no
  `TypeGeometry` case, so any prepared-statement SELECT of a geometry column
  fails with "invalid type 255"
  (`pkg/server/internal/column/column.go:245-284`; only the text path was
  extended). Most real drivers use the binary protocol, so this blocks
  basically all application access.
- **B3. `SridFlag` squats on MySQL protocol bit 15 (`NUM_FLAG`) and leaks to
  the wire** (`pkg/parser/mysql/type.go:66`, unmasked in `DumpFlag`). Clients
  will see geometry columns flagged numeric. Persisted + wire-visible bits
  are the wrong home for a schema attribute; `Srid` + `omitempty` in
  ColumnInfo suffices, or mask the bit.
- **B4. SRID argument wraps through `uint32` unchecked** in every constructor
  and `ST_SRID` (`builtin_geo.go:245, 353, 1007, 862`):
  `ST_GeomFromText('POINT(1 1)', 4294971622)` wraps to 4326 and silently
  becomes geographic. MySQL errors. The wrap into 4326 is the dangerous case.
- **B5. `ST_SRID(g, 4326)` mints unvalidated geographic values** (no
  coordinate-range check, `builtin_geo.go:849-863`), so gaps.md's "only
  ST_GeomFromWKB remains" claim misses two ingest paths: the setter and raw
  column writes (A1). `ST_SRID(POINT(300, 500), 4326)` flows straight into
  the S2 key path.
- **B6. Silent planar results on 4326 where MySQL errors:** ST_Envelope and
  ST_Centroid compute in degree space (`builtin_geo.go:1046, 1458`) while
  ST_Area properly errors. Align on the ST_Area (loud-error) policy.
- **B7. POINT/LineString/Polygon constructors discard argument SRIDs and
  stamp 0** (`builtin_geo.go:2063-2129`); MySQL propagates. At minimum error
  on non-zero input SRID.
- **B8. `ST_AsBinary` returns `ewkb[4:]` without parsing**
  (`builtin_geo.go:945`), so garbage round-trips as WKB (compounds A1).
- Minor: 4326 `ST_Distance` errors on empty points where the planar branch
  returns NULL; composite spatial indexes leak the internal expression-index
  form in SHOW CREATE TABLE (acknowledged in a comment) and SHOW INDEX shows
  internals for all forms; MODIFY COLUMN silently drops the SRID attribute.

## C. Architectural debts to pay before graduation

### C1. The whole index contract is stringly typed

Coverer params travel as a display string: DDL bakes them into
`GeneratedExprString`, the planner recovers them by `strings.Split` on that
text (`spatial_resolve_index.go:582-611`), SHOW CREATE re-extracts the column
name textually, and spatial-index *recognition* itself is a
`Hidden && strings.HasPrefix(GeneratedExprString, "tidb_spatial_key(")`
convention with no marker on `IndexInfo`. Any future change to AST restore
formatting silently un-recognizes existing indexes (writes keep going, plans
and SHOW CREATE regress); a column name containing a comma breaks the split.
For the real implementation: an explicit spatial marker + params on
`IndexInfo`, with `GeneratedExprString` demoted to an artifact. This one
change eliminates findings across three files and is the single
highest-leverage structural fix.

### C2. Predicate injection into the Selection: great POC seam, wrong production integration

What it buys is real: no new physical operators, ranger/skyline/IndexMerge do
the work, cost-based fallback for free, and the bbox-as-key-columns trick
makes pruning an index-side filter. What it costs:

1. The resolver only fires when a `LogicalSelection` sits directly above the
   `DataSource`, and the rule runs **before** predicate pushdown, so in any
   multi-table query the WHERE-Selection still sits above the Join and
   spatial predicates in joins never get the index. This is a distinct gap
   from the column-column spatial-join item in gaps.md and worth adding
   there.
2. It mutates `ds.Schema()`/`ds.Columns` mid-rule and depends on the second
   ColumnPruner pass to clean up.
3. If the index path loses costing, the injected DNF and `json_overlaps`
   remain as redundant root-level filters recomputing `tidb_spatial_key` per
   surviving row.
4. EXPLAIN exposes internal predicates.
5. Selectivity of the injected DNF on a hidden column is what actually drives
   plan choice at mid selectivity (gaps.md already notes this).

The design-PR direction (AccessPath-level integration in
`getPossibleAccessPaths`/ranger, per `PLAN-points-mvp.md`) fixes all five;
treat the current rule as the throwaway de-risking artifact the design doc
said it would be. Moving the hook to `DataSource.PushedDownConds` after PPD
would be an intermediate step that at least fixes (1).

### C3. The refine hot path parses each EWKB up to four times per row

`builtinGeomRelSig.evalInt` decodes both operands with `NoValidate` for
SRID/empty checks, then hands the raw *strings* to `geomrel.Relate`, which
re-parses both **with** full topological validation (`geomrel.go:50-70` has
its own copy-pasted decoder, contradicting the `builtin_geo.go:160` rationale
comment), and simplefeatures' relate does a fourth internal WKB round-trip
(`storage-format.md` quantifies that one at ~14%). `Relate` should take
decoded `geom.Geometry` (also fixing the per-row query-constant re-decode
already identified in `storage-format.md`), and geomrel should share the one
codec. The `string` ↔ `[]byte` copies in the signature are part of the same
cleanup.

### C4. `SpatialFunctionIsUsed` is set from `getFunction`

`builtin_geo.go:415, 490, 609`: a side channel from expression construction
into optimizer flags. It works today; derive it from an AST/plan walk like
other rule flags when this graduates.

### C5. COMMENT-based cell tuning validation is inconsistent

`'spatial:...'` is a fine POC vehicle (real syntax would be index options,
per the design). But it collides with genuine comments, isn't validated for
NaN (A7), and `tidb_spatial_keys`' 6-arg form doesn't validate params at all
while `NewPlanarCoverer` silently substitutes defaults for a bad level
(`coverer.go:139-144`), which could de-synchronize write keys from planner
ranges that `ParsePlanarParams` rejects. Make the validators identical.

## D. Algorithmic choices

### D1. Coverer defaults make the SRID-0 point index useless without tuning

Level 16 over the ±2³¹ default domain gives 65,536-unit leaf cells; any real
dataset clustered in a small extent lands in one or two cells, and the index
degenerates to scan-and-refine. Every meaningful test in the suite tunes via
COMMENT. But note the asymmetry: for the **point** index, level only sets
stored-key resolution; `EncodePoint` emits one key regardless and `CoverRect`
adapts its depth (`coverLevelFor`). Storing leaf keys at level 31 costs
nothing on writes and strictly improves selectivity at every domain, exactly
like the 4326 path already stores S2 level-30 leaves. Only the MVI genuinely
needs a shared, tunable level (set-overlap must match, and level drives the
8192-cell write bound). **Recommendation:** decouple them; point index always
at max depth, level/domain params become MVI-only tuning. That deletes most
of the tuning burden for the common case.

### D2. Morton vs Hilbert (SRID 0)

The `storage-format.md` analysis is correct and the benchmark-before-GA plan
is right. Two additions: the adaptive `CoverRect` + `mergeRanges` already
blunts Morton's worst jumps, so measure ranges-per-query on realistic query
sizes before paying Hilbert's encode cost; and D1's level-31 change increases
key density, which slightly favors Hilbert's locality. S2 for 4326 is the
right call and needs no revisiting.

### D3. Fixed-level MVI set-overlap is sound

Both sides cover bboxes of a shared partition of the plane, so intersecting
geometries share a cell; and both sides quantize with the same function, so
it dodges A5. The costs are inherent: bbox over-covering for diagonal
geometries, and the 8192-cell hard failure on INSERT for geometry large
relative to the level. Fail-loud is defensible for a POC; the standard
production answer is a multi-level covering (store a few adaptive cells per
geometry, query expands to ancestors + descendants), which is what
S2/CockroachDB do and what removes both the tuning cliff and the write
bound. Worth recording in the design doc as the expected evolution.

### D4. Geodesic pieces are right

Andoyer matches MySQL/boost to sub-metre (verified against MySQL 9.7 per the
tests); haversine with MySQL's 6370986 m radius for `ST_Distance_Sphere`
keeps covering and refine consistent by construction. One latent trap to
write down: if 4326 `ST_Distance <= r` (ellipsoidal) ever becomes
index-eligible, a spherical cap sized `r/R` under-covers by up to ~0.5%; the
cap radius must be inflated. Today it's correctly not recognized
(`spatial_resolve_index.go:925`).

### D5. KNN path B and the covering rewrite are sound within their guards

The `Point(ST_X, ST_Y)` reconstruction equals the stored point exactly, the
ST_SRID re-stamp for 4326 keeps geodesic dispatch (and matches the lat-first
convention), and `parentNeedsGeomCol` is conservatively correct. Two notes:
the KNN rewrite only fires with Sort/TopN directly above the DataSource, so
WHERE + ORDER BY misses it; and the honest framing that path B is O(n) is
appreciated. Path A (best-first expanding-cell operator) is where the real
win is, as gaps.md says.

## E. Test coverage

The point-index pipeline is genuinely well proven: property-based
no-false-negatives for planar CoverRect and S2 caps, sound IGNORE-INDEX
differentials, EXPLAIN plan assertions, a real concurrent-DML test with
UPDATE/DELETE plus ADMIN CHECK, and an ANALYZE/estimation regression guard.
The gaps that matter:

1. **`TestPOCSpatialS2`'s baselines are unsound**: taken after index creation
   without IGNORE INDEX, and the FORCE INDEX queries have no EXPLAIN
   assertions, so the pole/antimeridian "equivalence" checks can compare the
   index plan with itself (or two full scans). The 4326 headline claims rest
   on this test; fix the baselines first, since A2's fix needs a trustworthy
   4326 harness. Same pattern in `TestPOCSpatialComposite`'s baseline.
2. **The MVI overlap invariant is never property-tested** (only hand-picked
   polygons), and there is no E2E test for `GEOMETRYCOLLECTION EMPTY` (row
   becomes invisible to the index; semantically safe today only because
   empty-operand predicates return NULL) or for the >8192-cell INSERT
   failure mode.
3. **Domain-boundary clamping is untested at every level**: no test writes
   out-of-domain points or queries rects crossing the domain edge (the clamp
   design reads as correct, but nothing pins it).
4. Untested: rollback, partitioned tables (gaps.md part D already flags),
   prepared statements / plan cache (A3), NULLs in unindexed geometry
   columns. Also `TestPOCSpatialMVIAutoInjectAndBBox`'s docstring claims
   hint-free auto-selection but every checked query uses FORCE INDEX.

## Suggested order of attack

1. **A1 + A6 + B4/B5/B8** as one unit: a single validating geometry cast in
   the write path plus SRID/range checks on the remaining ingest paths. This
   turns the index contract from "hope" into an invariant.
2. **A2**: cover the S2 polygon (and use its RectBound for bbox conds)
   instead of the vertex bbox; fix the S2 test baselines (E1) in the same
   change so the fix is actually provable.
3. **A3** (skip plan cache when the resolver fires) and **A4** (pushdown
   gates): both small, both silent-wrongness.
4. **B1/B2** (ALTER no-op, binary protocol): straightforward bugs, high
   user-visibility.
5. **C1** (IndexInfo marker + structural params) before building anything
   more on the resolver, then the C2 direction per the design PR.
6. **D1** (decouple point-index resolution from MVI level) when returning to
   the coverer; fold A5 and A7's integer-arithmetic/NaN fixes into the same
   pass.

Everything else (C3 perf, D2 benchmark, B6/B7 compat edges, E gaps) can ride
along in the ordinary flow of the milestone plan. Net: this POC did its job,
the design holds up under implementation, and the findings above are the
difference between a POC that demonstrates the architecture and a base to
start landing PRs from.
