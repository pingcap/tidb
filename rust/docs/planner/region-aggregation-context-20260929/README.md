# Distributed aggregate descriptors and typed state

This is integration repair and seed evidence for the complete Go packages in
`source-inventory.json`, not a package parity/completion receipt. Go master is
`12b639a1161cd5a60126a47277f5ad14c320fd4a`; the execution archive is
`8936d7bdcb`. Their aggregation, cophandler, types and collate sources are
identical. Rust baseline is `b415a65bd8`.

## Root cause and implementation

The region loop implemented its own aggregate kind, numeric accumulator and
comparison switch. It discarded aggregate modes, evaluated direct columns
without their declared types, and recovered collation only from scan columns.
Consequently final COUNT counted partial rows, computed extrema lost collation,
and real/temporal/JSON extrema failed on their second non-NULL input.

`tidb-expr::aggregation::DistAggregate` now owns a canonical `AggFuncDesc`, typed
arguments, mode and per-group state, following `NewDistAggFunc`. The region loop
owns group lookup and reuses one typed MutRow buffer. It delegates COUNT, SUM,
SUM_INT, AVG, FIRST_ROW, MIN/MAX and bit aggregate updates and partial results.
AVG evaluates partial sum before count; COUNT and FIRST_ROW retain their NULL
and first-row short circuits. Go's distributed factory ignores HasDistinct;
this factory does likewise. Descriptor construction does not insert SQL casts.

The protobuf source retains Expr field 9 and AggFunctionMode, and outgoing
partial aggregate requests explicitly send Partial1Mode. Rust bindings regenerate
through the existing build script. All scalar initializers preserve an absent
mode. The existing schema checker validates field/enum declarations against the
Go module's pinned TiPB; wire tests verify field 9 for all five values.

SUM/AVG use the shared source-only ToFloat64 conversion, separate from destination
float production. StrToInt now parses the bounded integer prefix even when
float-prefix expansion failed, preserving Go's final error precedence. These
are shared conversion fixes, not special cases in the region loop.

## Evidence

- Initial capture: 1,932 cases, 513 value/warning-count differences.
- Excluding 168 malformed final/partial2 COUNT cases with non-integer partial
  inputs leaves 1,764 valid cases. The permanent test failed before the fix with
  387 differences.
- Expanded regression: 3,129 valid cases, exact results, error code/message,
  warning count/order/code/message; all pass. The only normalization removes
  Go's error-class prefix (`[types:1292]` versus `[1292]`).
- Coverage includes signed/unsigned integers, real/float, decimal, strings,
  temporal values, JSON, NULL mixtures, complete/final/partial modes, computed
  collation, count arity, partial AVG, overflow, non-finite floats, and lazy
  error-producing arguments under strict/ignore/warn flags.
- Factory panics are test failures, never excluded successes. Non-integer
  partial counts are excluded because Go reads their raw carrier as GetInt64;
  reproducing those malformed values would add an unsafe Rust contract.

`go-baseline.tsv.gz` and `rust-baseline.tsv.gz` preserve the original capture.
`go-expanded.tsv.gz` includes all 3,297 expanded Go rows before the 168 exclusions.
The executable regression fixture is
`../../../crates/tidb-unistore/testdata/region-aggregate-go.tsv.gz`.
`go-oracle.txt` is the exact standalone overlay source used for the expanded
capture. Columns are name, flags, Aggregation hex, ColumnInfo hex, comma-separated
literal Expr hex, partial results, error hex, warning count, warning hex list.
Reals preserve IEEE bits; other non-NULL results preserve SQL-rendered bytes.

To repeat the Go capture, copy `go-oracle.txt` to
`/private/tmp/region-aggregate_test.go`, map a nonexistent `rust_region_audit_test.go`
in the reference checkout's `pkg/expression/aggregation` directory to that file
using a Go overlay JSON, and run from the reference checkout:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/region-aggregate-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression/aggregation -run '^TestRustRegionAggregateAudit$' -count=1 -v

The oracle writes `/private/tmp/region-aggregate-go.tsv`. The wrapper must finish
with failpoint refcount zero. `verify.py` checks the exclusion rule, capture
cardinality, original difference counts, fixture identity and content hashes.
See `validation.txt` for exact Rust commands and outcomes.

## Remaining package work and limits

GROUP_CONCAT remains rejected, matching the existing capability limit; the
Go factory also exposes it, so the complete factory/package is not claimed.
Local GetAggFunc/GetResult/ResetContext and DISTINCT lifecycle, every original
Go test/benchmark, and the remaining descriptor methods need complete package
validation. The inventory includes all tracked package source, tests, fixtures,
generated/platform inputs and build/support artifacts; it does not mark them
implemented merely because they are listed.

Computed group keys remain unsupported. Existing group-key ordering and
collation-aware grouping are unchanged. Empty-input region behavior is unchanged
and was not newly certified. Conversion domains not represented by the captures
(including binary literal/bit/vector inputs and malformed byte strings), arbitrary
expression error transport, and real TiKV integration are not certified here.
This change does not resolve the separate projection/regexp audits. No
sysbench/TPC-C/TPC-H/YCSB throughput claim is made; row-buffer reuse removes
per-aggregate row construction for computed arguments, but benchmark runs are
still required.

The full Unistore run has one unrelated existing failure:
`closure_executor_selection_over_point_range_answers_zero_rows` errors on `abc`.
The same command fails on untouched `b415a65bd8`. With that test excluded, 189
pass and 13 existing parity-gap tests are ignored. Expression aggregation tests
have 18 existing ignored tests. These are explicit gaps, not successful parity.
