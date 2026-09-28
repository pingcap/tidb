# Protobuf expression structural audit, 2026-09-28

This is an audit of the shared protobuf scalar-expression boundary, not a package
transcreation receipt or a claim that the whole Rust implementation matches Go.
No production code was changed. All findings below remain open.

## Baseline and coverage

- Rust branch: `hparser-integration`, commit
  `e2bf492e203ddfbc54ec8c8b218d3847efab95cc`.
- Go master fetched for this audit:
  `4b2221cddfd81aae904573ab6a5c15e6c8351e6e`.
- Go execution archive: `8936d7bdcb`. Its `pkg/expression`, `pkg/types`, and
  `pkg/store/mockstore/unistore/cophandler/cop_handler.go` have no source diff
  against that master. This is a source-equivalent expression reference, not a
  claim that the entire archive equals current master.
- TiPB projection: `v0.0.0-20260623093813-5f9928e91afe`.
- Complete enum inventory: **640** values; Go decoder registers **565**;
  Rust accepts **234**; **331 Go decoder registrations are missing in Rust**;
  there are no Rust-only registrations. Static Rust inventory was checked against
  executing its constructor for every enum discriminant in `0..10000`.
- **2,154 differential rows**: 718 inputs under strict, warning, and ignore
  truncation modes. Every one of the 234 accepted Rust signatures was sampled.
  Go serialized each expression; Rust consumed the identical protobuf bytes.
- **292 differing rows**: 21 are Go panics on malformed typed inputs, excluded
  from behavioral defect counts. Of the remaining **271 rows across 58
  signatures**, three are a one-ULP ASIN difference. Thus **268 rows across 57
  signatures** differ in behavior or warning count, beyond that math difference.
  These are observations, not 268 independent root causes.

The schema projection check passes (44 messages, 9 enums). Missing registrations
are decoder capability gaps, not missing protobuf enum definitions. Go decoder
registration does not prove a function is eligible for every planner pushdown
route; some entries serve TiFlash. Do not interpret 331 as 331 SQL regressions.

## Structural findings

| Boundary / root cause | Reproduction in the matrix | Required design direction |
| --- | --- | --- |
| Incomplete decoder dispatch | Missing Abs, Coalesce, NullEQ and many numeric, string, temporal and JSON signatures; all 331 names are in `pb-audit-inventory.json` | Maintain a complete upstream package inventory and explicit capability mapping, with generated-schema and decoder gates kept separate. |
| Signedness lost between typed evaluation and generic casts | `CastIntAsReal/uint64max`: Go returns approximately 1.8446744073709552e19; Rust returns -1 | Preserve Go's unsigned integer carrier interpretation through typed source evaluation and target conversion. |
| Numeric conversion error policy bypassed | `CastStringAsDecimal/base/1`: strict Go errors on `invalid`; Rust returns 0.00 with a warning. Overflow casts also clamp where Go errors; ignore mode can still warn in Rust | Conversion producers must return their original typed diagnostics to the statement error policy, as Go does. Audit all numeric casts together. |
| Unsigned decimal target metadata lost | `CastRealAsDecimal/unsigned/0`: Go -0.00; Rust -1.26; decimal/string/duration sources have related differences | Preserve the complete target FieldType instead of reducing it to precision and scale. |
| JSON source type erased during conversion | JSON string `"1.26"` and boolean `true` convert to real in Go; Rust treats serialized JSON text as a numeric string. JSON null/object integer/decimal error behavior also differs | Follow Go's JSON-kind-specific conversion dispatch. |
| Temporal conversion policy and fraction handling differ | Invalid string/numeric time casts return NULL plus warning in Rust where strict Go errors; real/decimal 0.5 loses the fractional part of a zero date | Preserve temporal type, fractional precision and conversion errors through the typed cast boundary. |
| Request context omits the statement clock | `CastDurationAsTime` and duration-based ADDDATE/SUBDATE return `no statement clock for a TIME cast` in Rust; Go produces a date | Supply a statement-stable clock through RequestEvalContext, matching Go's context lifetime and timezone rules. |
| Interval conversion does not follow statement policy | `AddDateDatetimeReal/normal`: interval 1.5 DAY errors in strict Go; Rust silently adds two days. Warning mode loses Go's warning; string intervals have related differences | Reuse Go-equivalent typed interval conversion and error handling across the ADDDATE/SUBDATE signature family. |
| Generic argument evaluation changes NULL/error order | `PlusReal/null_bad`, `Pow/bad_then_null`, `Atan2Args/bad_then_null`, `Substring3ArgsUTF8/bad_pos_then_null`: Go conversion error/warning is suppressed by Rust's NULL handling. Integer MOD also rejects a conversion Go accepts in warning/ignore modes | Match each typed builtin's argument evaluation order and conversion contract before invoking value kernels. |
| JSON modification eagerly evaluates unused children | `JsonReplaceSig/null_before_bad_path` and `JsonArrayAppendSig/null_before_bad_path`: Go returns NULL without evaluating a failing value cast; Rust evaluates it | Follow each Go builtin's early-return points instead of a generic eager child loop. |
| Result decimal scale ignored | `UnixTimestampDec/normal`: Go 1704164645.12, Rust 1704164645.123 for return scale 2 | Apply protobuf result metadata at the same production boundary as Go. |
| Math implementation differs at the bit level | `Asin/0.5`: Go 0.5235987755982989; Rust 0.5235987755982988 | Investigate platform math implementation compatibility separately from structural evaluation defects. |

An additional **source-confirmed, not dynamically exercised** transport gap:
`rust/crates/tidb-unistore/src/cophandler.rs::eval_shared` converts typed evaluation
errors to debug strings. Its callers use `other_error`, putting them in the outer
coprocessor response. Go's `genRespWithMPPExec` and `toPBError` in
`pkg/store/mockstore/unistore/cophandler/cop_handler.go` preserve SQL error codes
and messages in `tipb.SelectResponse.Error`. A root fix must preserve the typed
error through the response boundary, with an end-to-end response regression test.

Useful implementation entrypoints are `tidb-expr/src/scalar_function/pb_builtin.rs`,
`tidb-expr/src/cast.rs` (especially `report_decimal_production`),
`tidb-expr/src/context.rs::Columns::now`, `tidb-expr/src/time_fn/add_sub.rs`, and
`tidb-unistore/src/cophandler/eval_context.rs`, all under `rust/crates/`.

## Evidence and reproduction

`pb-audit-inventory.json` contains all enum values and exact Go/Rust/missing sets.
`pb-systematic-diffs.json` contains every observed difference with decoded error
messages. `pb-systematic-go.tsv` stores name, flags, protobuf hex, result, error
hex, warning count. `pb-systematic-rust.tsv` stores the same columns except the
protobuf input. There are no headers. Results encode NULL, error, decoder error,
panic, hex string values, or exact IEEE real bits. Flags 0/2/1 mean strict/warn/ignore.

The `.go.txt`, `.rs.txt`, and `.py.txt` files preserve the exact temporary audit
harnesses as evidence, not production tests. The Rust runner temporarily inserts
the probes and restores both source files in `finally`; run it only in an isolated
checkout without concurrent edits. The collector tests passing means that all
rows were collected, **not that Go and Rust agreed**. SHA256SUMS covers the captured
evidence, excluding this README.

For replay, copy each evidence file to `/private/tmp`, dropping only the final
`.txt` suffix on harness filenames. Use a reference checkout at
`/private/tmp/tidb-go-structural-20260927` at the Go archive revision above. The Go
overlay adds the test without modifying reference source. Run from that archive:

```sh
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-systematic-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustSystematicPB$' -count=1 -v
```

The failpoint wrapper enabled and disabled failpoints; its final refcount was zero.
From the audited Rust repository root, the exact commands executed were:

```sh
python3 /private/tmp/build-pb-audit.py
python3 /private/tmp/run-pb-systematic-rust.py
python3 /private/tmp/compare-pb-audit.py
python3 rust/scripts/check-tipb-proto-projection.py
```

The Rust runner executes these targeted commands:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib audit_pb_registry_inventory -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib audit_systematic_pb_matrix -- --test-threads=1
```

Replaying captured Go bytes alone is sufficient to reproduce Rust observations.
Rerunning Go time-dependent cases changes the statement date; the missing Rust
clock still fails independently of that date. The archived Go checkout emits
harmless missing-git-metadata messages; the captured test exit status is successful.

## Limits and next implementation boundary

This exhausts the decoder inventory and samples every accepted signature. It does
not exhaust the input space or find every issue in TiDB's Rust implementation.
The comparison checks normalized values, success/error state and warning count;
it does not compare SQL error codes/messages when both sides error, warning
codes/messages, or every internal datum type. Coverage does not establish full
collation, metadata, timezone/DST, row/vector execution, planner reachability,
request transport, or complete SQL semantics. Performance and sysbench/TPCC/TPCH/
YCSB were not measured. No performance claim follows from this audit.

Seven malformed typed cases (six time comparisons and DateDiff) panic in Go under
all three modes. They remain in raw evidence for transparency, but making Rust
panic is not a correctness goal. There were no Rust panics in this matrix.

The primary cross-crate implementation boundary is the Go `pkg/expression`
package, with explicit dependencies on types, statement context and coprocessor
transport. Findings should feed its complete source/test/generated/build inventory
and package validation receipt. Fixing selected rows cannot be reported as whole
package parity. No lint/build readiness claim is made: this commit adds only audit
documentation and captured evidence; `make lint` was not run for it.
