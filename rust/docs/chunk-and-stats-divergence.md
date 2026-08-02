# Chunk and statistics divergence inventory

Go source of truth: `pkg/util/chunk/{column,chunk,codec,row,chunk_util}.go`,
`pkg/statistics/{histogram,cmsketch,scalar}.go`,
`pkg/planner/cardinality/{pseudo,row_count_column,row_count_index,selectivity,exponential}.go`.

Rust audited: `rust/crates/tidb-chunk`, `rust/crates/tidb-stats`,
`rust/crates/tidb-planner/src/cardinality`, plus (read-only, owned by other
units) the chunk-format wire decoder in `rust/crates/tidb-codec/src/column.rs`
and `rust/crates/tidb-distsql/src/chunk_decode.rs`.

Method: line-by-line source comparison. **Nothing in this document was
executed** — this machine cannot run freshly built binaries. `cargo check` and
`cargo clippy` are the only gates that ran.

---

## Worst three overall

1. **B-1** — uniform-distribution equality estimate on an empty histogram is
   wrong by ~19 orders of magnitude before clamping, and by the full table
   width after it. Flips full-scan vs point-lookup. **Fixed in this pass.**
2. **B-2** — the top-level `Selectivity()` combination rules and
   `pseudoSelectivity` are not ported at all, so multi-condition selectivity
   has no TiDB-shaped answer. Gap, not a divergence in ported code.
   **Closed:** the combination tail, both string-match defaults,
   `crossValidationSelectivity` and `pseudoSelectivity` are ported and pinned
   to captured TiDB `estRows`.
3. **A-1** — `Chunk::append_datum` routes a `Datum::Decimal` through decimal
   text and *panics* on a value `MyDecimal` cannot hold; Go stores the
   `MyDecimal` it already has and cannot reach that failure.

---

## Part A — `tidb-chunk` vs `pkg/util/chunk`

### Verified equal

Each of these was compared statement by statement and matches, including
overflow/wrap behaviour where Go relies on it.

| Area | Go | Rust |
| --- | --- | --- |
| Fixed-vs-variable type dispatch (`getFixedLen`) | `codec.go:165-179` | `tidb-chunk/src/column.rs:77-92` |
| Same dispatch in the wire decoder | `codec.go:165` | `tidb-codec/src/column.rs:74-105` |
| `sizeTime` = 8 (`types.Time` is one `uint64`), `MyDecimalStructSize` = 40 | `pkg/types/time.go:224`, `pkg/types/mydecimal.go:233` | `column.rs:68,72`; `tidb-codec/src/column.rs:87-88` |
| Null bitmap bit order: bit `i&7` of byte `i>>3`, **0 = null, 1 = not-null** | `column.go:225-228, 254-263` | `column.rs:210-226` |
| Bitmap padding: byte appended only when `length>>3 >= len(bitmap)`; new bits default 0 | `column.go:255-258` | `column.rs:218-221` |
| `AppendNull`: bit left 0, fixed column still appends the zeroed `elemBuf`, var column repeats the last offset | `column.go:355-363` | `column.rs:396-404` |
| `finishAppendFixed` / `finishAppendVar` ordering (bitmap before `length++`) | `column.go:365-369, 395-399` | `column.rs:242-246, 408-412` |
| Var-column leading `0` offset seeded at construction and kept by `reset` (including the `else if !IsFixed()` re-seed arm) | `column.go:134-140, 212-220` | `column.rs:173-184, 230-239` |
| Empty-but-not-null var cell vs null cell: both zero width, distinguished only by the bitmap | `column.go:355-363, 736-739` | `column.rs:396-404, 433-437`; test `var_len_append_get_string_bytes_null` |
| Enum/Set cell = 8-byte native value ‖ name, and `getNameValue`'s zero-width short circuit | `column.go:46-53, 763-772` | `column.rs:336-377` |
| `appendMultiSameNullBitmap`, including `byte(1<<(8-numRedundantBits)) - 1` wrapping to `0xff` when `numRedundantBits == 0` | `column.go:316-336` | `column.rs:578-596` |
| `reconstruct` (fixed + var), overlapping in-place `copy`, and the trailing bitmap clean | `column.go:802-850` | `column.rs:530-574` |
| `nullCount` (popcount fast path + tail) | `column.go:547-560` | `column.rs:509-524` |
| `CopyExpectedRowsWithRowIDFunc` / `CopyRows` / `CopySelectedJoinRowsDirect` / `copySelectedInnerRows` / `copySameOuterRows` | `chunk_util.go:33-220` | `chunk_util.rs:34-170`, `column.rs:491-505, 601-648` |
| Chunk-format wire framing: `u32 length ‖ u32 nullCount ‖ [bitmap] ‖ [offsets] ‖ data`, little-endian, bitmap present only when `nullCount > 0`, all-not-null fast path | `codec.go:48-73, 103-141` | `tidb-codec/src/column.rs:735-830`, `RawColumn::is_null` at `:135-145` |
| `sel` is **not** applied by the chunk encoder — `Codec.Encode` walks `chk.columns` and encodes `col.length` rows regardless of `chk.sel` | `codec.go:40-46` | Same: nothing in `tidb-codec`'s framing consults a selection |
| `sel` semantics in the container: `NumRows`, `GetRow` remap, `appendSel` keyed on column 0, `Reset` clears it | `chunk.go:385-405, 647-650, 301-310` | `chunk.rs:107-136, 158-179` |
| `Row.DatumWithBuffer` unsigned dispatch, and `TypeYear` deliberately ignoring the unsigned flag | `row.go:147-197` | `row.rs:164-203` |

Answering the decisive question for Part A: **for every type with a ported
getter I found no value where the two disagree about null-ness, length, or
byte content.** Fixed cells are `to_ne_bytes` of the same Go scalar, var cells
are the same byte run between the same offsets, and the bitmap is bit-identical.

### Ranked divergences

**A-1 (rank 2 — panic reachable from ordinary data).**
Go `pkg/util/chunk/chunk.go:670` (`case types.KindMysqlDecimal:
c.AppendMyDecimal(colIdx, d.GetMysqlDecimal())`) appends the `*types.MyDecimal`
the datum already holds — there is no conversion and no failure mode.
Rust `rust/crates/tidb-chunk/src/chunk.rs:290-298` instead formats
`Datum::Decimal` to text, re-parses it with `MyDecimal::from_string`, and
`assert!(err.is_none(), ...)`.
Distinguishing case: any `Datum::Decimal` whose canonical text `MyDecimal`
cannot represent exactly — e.g. a literal with more than 30 fractional digits,
`0.` followed by 40 digits. Go stores the (truncated) `MyDecimal` cell and the
query proceeds; Rust aborts the statement with a panic.
Caveat: reachability depends on whether `tidb_datatype::Decimal` admits more
digits than `MyDecimal` does. I did not verify that bound, so this is
"panic exists on a path Go cannot fail on", not "panic confirmed reachable".
Not fixed — the correct repair is to append the `MyDecimal` without a text
round trip, which needs the datum representation decision that
`tidb-datatype` owns.

**A-2 (rank 3 — wire/decode strictness).**
`rust/crates/tidb-codec/src/column.rs:807-821` rejects an offset table whose
first entry is non-zero, and rejects a non-monotonic table. Go's
`Codec.decodeColumn` (`codec.go:126-133`) validates neither; in fact
`Decoder.ReuseIntermChk` (`codec.go:275-289`) exists precisely to rebase a
column whose `offsets[0] != 0`.
Distinguishing case: a producer that re-encodes a partially consumed
intermediate chunk emits `offsets = [40, 45, 51]`; Go decodes it and rebases,
Rust returns `InvalidOffset`. TiKV's coprocessor and Go's own `Codec.Encode`
always start at 0, so this is a latent difference, not an observed one.
Rust is the stricter side. Not fixed — `tidb-codec` belongs to another unit.

**A-3 (rank 3 — panic where Go returns NULL).**
Go's `Row.DatumWithBuffer` (`row.go:152-197`) is a `switch` with **no default
arm**: an unlisted `tp.GetType()` leaves the caller's buffer at its zero value,
i.e. a NULL datum. Rust `row.rs:247-249` panics.
Distinguishing case: a non-null cell in a column typed `TypeGeometry` or
`TypeNull`. Go yields NULL; Rust aborts. TiDB does not produce non-null cells
of those types, so this is defensive-only.

**A-4 (rank 3 — documented deferral, affects printed/encoded decimals).**
`row.rs:233-246` notes that Go additionally calls `d.SetLength(tp.GetFlen())`
and `d.SetFrac(tp.GetDecimal())` (`row.go:176-185`) and that the explicit
`SetFrac` override is deferred. Distinguishing case: a `DECIMAL(10,2)` column
whose cell carries `digitsFrac = 4`; Go's datum reports frac 2, Rust's reports
the stored 4, changing the re-encoded/printed text. Already tracked in-code.

**A-5 (informational — not representable).**
Go's `Chunk.Reset` (`chunk.go:301-310`) returns early when `c.columns == nil`,
preserving `numVirtualRows`; `chunk.rs:158-164` always zeroes it. Only
`renewEmpty`/`renewWithCapacity` produce a truly nil `columns` in Go —
`New(nil, …)` and `NewEmptyChunk(nil)` produce a non-nil empty slice and behave
like Rust. A Rust `Vec` cannot distinguish the two, and neither `Renew` nor
`renewEmpty` is ported, so nothing reaches the difference today.

---

## Part B — statistics

How far I got: the histogram estimators, the scalar/interpolation layer, the
CM sketch + TopN query path, the pseudo constants and formulas, the
out-of-range machinery, and the column row-count entry point were all compared
line by line. **Not** compared: `builder.rs` (histogram construction / ANALYZE),
TopN merge, global/partition stats merging, index row counting beyond the
equality path, and the ~1100-line `row_count_estimator.rs` was read only around
its estimator entry points, not exhaustively.

### Verified equal

| Area | Go | Rust |
| --- | --- | --- |
| Bucket layout: `Count` is **cumulative**, `Repeat` is the upper bound's frequency; `BucketCount(idx)` differences adjacent counts | `histogram.go:897-902` | `tidb-stats/src/histogram.rs:250-258` |
| `NotNullCount` = last bucket's cumulative count; `TotalRowCount` adds `NullCount` | `histogram.go:716-735` | `histogram.rs:260-273` |
| `LocateBucket`: `LowerBound` over the flattened `(lower,upper)` sequence, the `index%2==0 && !match` "before this bucket" case, and the extra upper-bound equality test for degenerate buckets | `histogram.go:545-571` | `histogram.rs:280-357` |
| `EqualRowCount`: repeat → bucket-NDV `(BucketCount-Repeat)/(NDV-1)` → `NotNullCount/NDV`, and `(0,false)` when not in a bucket | `histogram.go:505-518` | `histogram.rs:371-393` |
| `LessRowCountWithBktIdx`, including `curCount-curRepeat` on an exact upper-bound match and the `preCount + frac*(curCount-curRepeat-preCount)` interpolation | `histogram.go:573-596` | `histogram.rs:398-425` |
| `GreaterRowCount` (`max(0, notNull - less - equal)`) | `histogram.go:521-526` | `histogram.rs:437-442` |
| `BetweenRowCount`, including the same-bucket underestimate rescue `min(min(lessB, notNull-lessA), lowEqual+ndvAvg)` and the whole skew block (`skewEstimate = BucketCount(i)`, minus `Repeat` when the range misses the last value, `min(2*est, skew)` cap, `MaxEst` widening) | `histogram.go:605-645` | `histogram.rs:450-488` |
| `CalculateSkewRatioCounts` | `histogram.go:648-655` | `tidb-stats/src/row_estimate.rs:44-57` |
| `calcFraction` guards (`upper<=lower→0.5`, clamp, NaN/Inf→0.5) | `scalar.go:29-44` | `histogram.rs:77-92` |
| `convertBytesToScalar` (first ≤8 bytes, big-endian, right-zero-padded) — the length-1..7 cases produce byte-identical results to Go's hand-unrolled shifts | `scalar.go:170-196` | `histogram.rs:109-114` |
| `convertDatumToScalar` incl. `MinNotNull → -MaxFloat64`, `MaxValue → +MaxFloat64`, common-prefix trimming for bytes | `scalar.go:53-92` | `histogram.rs:154-176` |
| `commonPrefixLength` | `scalar.go:148-168` | `histogram.rs:96-104` |
| `OutOfRange` (compare against first lower / last upper) | `histogram.go:1029-1035` | `histogram.rs:492-505` |
| `OutOfRangeRowCount` — all 12 steps: `oneValue`, the determinate-objective early return *before* the low-NDV adjustment, the `<100` NDV smoothing, common prefix over 4 datums, unsigned negative clamp + impossible-range 0, `histWidth<0`/`+Inf`→0, `predWidth==0`→`histWidth=0`, the squared triangular overlaps, `0.5/0.5` vs `1.0` percentages, the `modifyCount==0||addedRows==0` max inflation, and the fact that a positive skew ratio **discards** the earlier `MinEst` assignment | `histogram.go:1107-1266` | `histogram.rs:528-653`, `overlap_geometry.rs` |
| `calculateLeftOverlapPercent` / `calculateRightOverlapPercent` | `histogram.go:1138-1163` | `overlap_geometry.rs` |
| `AbsRowCountDifference`, `GetIncreaseFactor` | `histogram.go:722-727, 759` | `histogram.rs:509-521` |
| CM sketch `queryHashValue`: `(count - cell)/(width-1)` noise with Go's uint64 wrap, the `temp=1` sentinel, the median-of-sorted + `min(minValue+1)` cap, `-temp`, then `considerDefVal` | `cmsketch.go:260-292` | `cmsketch.rs:355-388` |
| `considerDefVal` — `(cnt==0 \|\| (cnt>def && cnt < 2*(count/width))) && def>0`, same divide-then-double order | `cmsketch.go:215-217` | `cmsketch.rs:410-415` |
| `TopN.QueryTopN` / `LowerBound` / `BetweenCount` (half-open `[l,r)` sum over the sorted encoded list) | `cmsketch.go:619-684` | `cmsketch.rs:668-692` |
| `pseudoEqualRate=1000`, `pseudoLessRate=3`, `pseudoBetweenRate=40` | `pseudo.go:31-33` | `cardinality/pseudo.rs:27-31` |
| `getPseudoRowCountBySignedIntRanges` / `…ByUnsignedIntRanges`, including the wrapping `high-low` width cap and the `low==high → 1` handle rule | `pseudo.go:99-166` | `pseudo.rs:196-272` |
| `getPseudoRowCountByColumnRanges`: `[NULL,+inf]→all`, `MinNotNull` minus `tableRows/1000` nulls, `+inf→/3`, equal→`/1000`, else `/40`, final clamp | `pseudo.go:200-231` | `pseudo.rs:277-303` |
| `getPseudoRowCountByIndexRanges`, incl. the `/100` per equal-prefix-column decay and the `totalCount > tableRowCount → tableRowCount/3` clamp | `pseudo.go:168-198` | `pseudo.rs:308-345` |
| `outOfRangeEQSelectivity` and `outOfRangeBetweenRate = 100` | `selectivity.go:1119-1133`, `histogram.go:53` | `cardinality/out_of_range.rs:24-49` |
| `outOfRangeFullNDV` (deletion fallback, `sqrt` NDV derivation, increase-factor scaling, `max(ndv, 100)`, `max(1, …)`) | `selectivity.go:1136-1169` | `out_of_range.rs:58+` |
| `ApplyExponentialBackoff` and `MaxExponentialBackoffCols = 4` | `exponential.go:23-56` | `cardinality.rs:29-80` |
| `getColumnRowCount` arithmetic: point/PK-at-most-one, the low-exclude subtract + `Clamp(0, NotNullCount)`, the `!LowExclude && IsNull → +NullCount` NULL rule, the high-inclusive add, `Clamp(0, realtime)`, `MultiplyAll(increaseFactor)`, the `ToleranceFactor` full-range test, and the final `Clamp(1, realtime)` | `row_count_column.go:120-236` | `cardinality/row_count_column.rs:263-326` |
| `RowEstimate` `Add/AddAll/Subtract/MultiplyAll/DivideAll/Clamp` incl. `Clamp`'s min≤est≤max re-ordering | `histogram.go:672-714` | `row_count_column.rs:55-100` |
| `equalRowCountOnColumn`: NULL → `NullCount`; ver-1 empty-bounds → 0; ver-1 out-of-range → `outOfRangeEQSelectivity * TotalRowCount`; ver-1 CM sketch; ver-2 TopN → histogram repeat → uniform | `row_count_column.go:160-206` | `row_count_estimator.rs:312+` |
| `IsLastBucketEndValueUnderrepresented` | `selectivity.go:582-611` | `row_count_estimator.rs:~255-280` |
| `GetUsableSetsByGreedy` mask traversal and `compareType` tie-break ordering | `selectivity.go:552, 633-751` | `selectivity_greedy.rs` |

### Ranked divergences

**B-1 (rank 1 — order-of-magnitude, plan-flipping). FIXED.**

Go `pkg/planner/cardinality/row_count_index.go:374`:

```go
return statistics.DefaultRowEst(max(float64(topN.MinCount()-1), 1))
```

`TopN.MinCount()` returns **`uint64`** (`pkg/statistics/cmsketch.go:573`) and
returns **0** for a nil or empty TopN. `MinCount()-1` is therefore unsigned and
*wraps*: Go evaluates `float64(math.MaxUint64)` ≈ `1.8446744073709552e19`.

Rust `rust/crates/tidb-planner/src/cardinality/uniform.rs:67-70` computed the
same expression in `f64`:

```rust
let min_topn = stats.topn_min_count.unwrap_or(0.0);
return RowEstimate::default_est(go_max(min_topn - 1.0, 1.0));   // 0.0 - 1.0 → max(_,1) → 1.0
```

Distinguishing case. Table `t(c int)`, `realtime_row_count = 1_000_000`,
`modify_count = 0`; `c`'s stats row records `NDV = 5` but no histogram buckets
are loaded and TopN is empty (the shape that reaches this branch at all — it
requires `not_null_count == 0` and `hist_ndv > 0`). Predicate `WHERE c = 42`:

* Go: `estimateRowCountWithUniformDistribution` → `1.8446744073709552e19`,
  which `getColumnRowCount`'s closing `totalCount.Clamp(1, realtimeRowCount)`
  turns into **1,000,000** — the whole table.
* Rust (before this fix): **1**.

That is the difference between a full table scan and a point index lookup.

Fixed by reproducing the source's unsigned wrap explicitly, with the Go line
cited in a comment. The fix deliberately propagates what is almost certainly an
upstream overflow bug, because matching TiDB's `estRows` is the contract.

**B-2 (rank 1 gap — CLOSED).**
The top-level `Selectivity()` combination is now ported. What each piece
became:

* the product-of-selectivities loop, the leftover-condition block, and the
  one-row floor (`selectivity.go:217-429`) — `combine_selectivity` in
  `rust/crates/tidb-planner/src/selectivity_greedy.rs`. The leftover block
  charges its minimum **once for the whole remaining mask**, which is what
  makes two non-prefix `LIKE`s estimate 1000 rather than 100.
* `GetStrMatchDefaultSelectivity` / `GetNegateStrMatchDefaultSelectivity`
  (`session.go:3675-3692`) — `SelectivityDefaults::from_session`. The port had
  been carrying 0.8/0.8 as the shipped default; the shipped default is
  **0.1/0.9**, and only an explicitly-set 0.8 makes both sides 0.8. Real TiDB
  prints 1000.00 for `LIKE '%a%'` and 9000.00 for `NOT LIKE '%a%'` on a
  10000-pseudo-row table, which is the arithmetic that settled it.
* `crossValidationSelectivity` (`selectivity.go:1173`) —
  `cross_validation_selectivity`, keeping the `math.MaxFloat64` seed that the
  caller relies on to fall back, and the unclamped `rowCount / totalRowCount`.
* `pseudoSelectivity` (`pseudo.go:40-97`) in its entirety —
  `cardinality::pseudo::pseudo_selectivity`, including the unique-key shortcut
  that returns `1.0 / RealtimeCount` and abandons every other condition.

Evidence: `rust/crates/tidb-planner/tests/selectivity_pseudo_source.rs` pins
nineteen `EXPLAIN` `estRows` values captured from a real TiDB server on an
unanalyzed table. A mutation that truncates the product loop to its first node
fails every multi-condition pin and leaves the single-condition controls
passing.

`SELECTION_FACTOR` at `cost_factors.rs:24` still has no caller; the
combination tail takes its factor from `SelectivityDefaults`, which is the
session owner's value rather than the cost model's constant.

**B-3 (rank 3 — naming, latent).**
`pseudo_row_count_by_index_ranges`'s parameter is named `unique_columns:
Option<usize>` and documented as "`None` means the index is not known to be
unique" (`pseudo.rs:308-320`). Go's parameter is `colsLen`
(`pseudo.go:168`), passed as the index's **column count**
(`row_count_index.go:66`) with no uniqueness meaning; the `+= 1.0` it guards is
the "full-length inclusive point range" rule, which applies to non-unique
indexes too. The arithmetic is identical, but a caller that reads the Rust doc
and passes `None` for a non-unique index loses the `+= 1.0` arm and instead
takes the `/100`-decay path. The one live caller
(`tidb-executor/src/access_cost.rs:1042`) should be re-read against
`row_count_index.go:66` before this is relied on. Not changed — renaming a
public parameter is outside "small and certain".

---

## What is unverified

* **Everything runtime.** No test, no binary, no query. `cargo check` and
  `cargo clippy` on `tidb-planner` are the only executed gates.
* The B-1 fix has no regression test: the existing parity fixtures in
  `crates/tidb-planner/tests/row_count_estimator_source.rs` all carry populated
  histograms and therefore take the *other* branch of
  `estimate_uniform_equality`, so they neither cover nor contradict the change.
  A fixture with `buckets: vec![]`, `ndv > 0`, `topn: None`, `modify_count: 0`
  is the missing case.
* A-1's reachability (whether `tidb_datatype::Decimal` can hold a value
  `MyDecimal` rejects) was not checked.
* `tidb-stats/src/builder.rs`, TopN/global-stats merging, and index row
  counting beyond the equality path were not compared.
* Collation handling inside `Histogram::locate_bucket` was compared
  structurally against `chunk.Compare`, not against TiDB's collator behaviour
  for a non-binary collation.
