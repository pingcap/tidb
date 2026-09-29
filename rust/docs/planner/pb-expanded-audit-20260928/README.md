# Expanded protobuf expression parity audit, 2026-09-28

This audit finds additional structural differences after the numeric-cast repair
in `0d5b56d232`. It changes no production code. The findings below are open.
This is seed evidence, not a complete Go-package transcreation receipt. Finite
runtime samples cannot establish that every mismatch has been found.

## Reference and scope

Rust: `06b95edc50520943eb8bb2effab09b2ccf94cc6d`, branch
`hparser-integration`. Go master: `4b2221cddfd81aae904573ab6a5c15e6c8351e6e`.
The Go execution archive at `/private/tmp/tidb-go-structural-20260927` is from
`8936d7bdcb`; `pkg/expression`, `pkg/types`, and
`pkg/store/mockstore/unistore/cophandler/cop_handler.go` have no source differences
from that master. This does not claim the entire archive equals master.

The Go oracle and master use TiPB `v0.0.0-20260908093239-fed7bc47c39d`.
The Rust integration branch's go.mod still pins
`v0.0.0-20260623093813-5f9928e91afe`. Their ScalarFuncSig enums agree; their
ExecType enums do not.

The complete scalar enum/decoder inventory remains: 640 enum entries, 565 Go
registrations, 234 accepted Rust signatures, **331 missing Rust registrations**,
no Rust-only registrations. The runtime constructor scan was rerun. See the
[original inventory](../pb-systematic-audit-20260928/README.md); registration does
not imply eligibility on every planner pushdown route.

`package-inventory.json` records all 208 tracked artifacts in master's
`pkg/expression` subtree with Git blob identities: 75 direct production Go files,
60 direct test Go files, 11 Bazel files, and 62 support/subpackage artifacts.
Subpackages are separate completion units. This is a file inventory, not proof
that their implementations, generated inputs, platform variants, fixtures, and
validation gates have all been ported or reviewed.

## Differential results

The matrix has **8,298 unique rows**: 2,154 previous cases rerun, 3,768 additional
literal cases, and 2,376 new chunk-column cases. All 234 accepted signatures are
sampled. Numeric casts cover all 21 numeric source/target pairs and six result
field shapes; additional cases exercise invalid bytes, integer bounds, tiny and
huge real values, JSON values, string charsets/widths, and child error order.

**392 rows differ**: 243 in the previous input set, 79 new literal rows, and
70 column rows. These include 21 Go panics on malformed original typed inputs
and three ASIN one-ULP differences. Excluding those leaves **368 behavior or
diagnostic differences across 51 signatures**. There are no Rust panics.
These counts are observations across modes and shapes, not independent bugs.

Overlapping difference categories: 210 result/state, 151 warning count,
196 warning content/order, and 52 typed errors. Unlike the previous audit,
this comparator checks diagnostic contents. Thus the previous-input difference
count is not directly comparable to the old count; additional diagnostics were
previously invisible. Prior fixes passed their 441-row numeric-cast fixture;
they did not prove untested casts and columns equivalent.

## Confirmed additional findings and structural causes

| Boundary | Reproducer in differences.json | Go vs Rust | Root and required design direction |
| --- | --- | --- | --- |
| Byte string to integer | `CastStringAsInt/extended/7/0`, strict | Invalid byte `ff`: Go 1292 error; Rust zero without warning. `12ff` in warning mode: Go 12 plus warning; Rust zero without warning. Also reproduced through columns. | `cast.rs::report_int_truncation` and value conversion require valid UTF-8 and discard conversion failure. Follow Go's byte-string numeric prefix parsing and preserve its diagnostic. |
| Integer overflow identity | `CastStringAsInt/extended/11/0`, strict | String 18446744073709551616: Go 1690 unsigned overflow; Rust 1292 truncation. | Generic truncation reporting replaces the numeric parser's original error. Preserve typed conversion errors through context handling. |
| Unsigned warning lifecycle | `CastStringAsInt/extended/2/4`, warning | Negative fractional string: Go emits 1292 then 8031; Rust emits only 1292. | `report_negative_string_unsigned` requires fully consumed integer text. Go's `builtinCastStringAsIntSig.evalInt` decides the second warning after the first error has been downgraded/ignored by context. Follow that lifecycle. |
| JSON to fixed binary string | `CastJsonAsString/extended/1/binary/3` | JSON array `[]`: Go two bytes; Rust adds a zero byte. | `eval_string_cast_with_type` applies binary padding to every source family. Go's `builtinCastJSONAsStringSig.evalString` returns after string production without the padding used by other signatures. Preserve source-specific production boundaries. |
| Diagnostic source identity | `CastRealAsDecimal/column/5/0`, warning | Same decimal result, but Go reports `Column#0`; Rust reports `1e+100`. JSON-real-to-integer overflow also differs in exponent formatting. | Value-only helpers lose expression identity and source-specific formatting. Carry the diagnostic source through conversion as Go does. |
| Duration warning identity | `CastIntAsDuration/base/3`, warning | Go 1690 Duration overflow; Rust 1292 time truncation. JSON-to-duration also reports `TIME` vs `time`. | Shared result/warning-count checks hid differing error families and type names. Follow typed temporal conversion error production. |
| JSON child evaluation order | `JsonReplaceSig/extended/path_before_value` and `second_path_before_first_value` | Go invalid-path 3143; Rust evaluates the bad JSON value first and returns 3140. NULL paths in replace/array-append return NULL in Go but error in Rust. | Eager child evaluation bypasses each Go signature's ordered validation and NULL exits. Preserve per-signature order, including array-append's distinct ordering. |
| Numeric child evaluation order | `PlusReal/extended/null_bad`, `Pow/extended/bad_null`, `Atan2Args/extended/bad_null` | Go conversion error; Rust NULL. | Universal NULL short-circuiting replaces typed child evaluation order. Same structural family as the previous audit, now demonstrated with additional typed failing children. |

JSON error-order cases use a JSON-typed failing child,
`CastStringAsJson("invalid")`, with ParseToJSONFlag set. They do not depend on an
invalid protobuf argument domain. Column cases use an actual typed chunk row;
they are not constants renamed as columns.

The [previous audit](../pb-systematic-audit-20260928/README.md) also identifies
remaining statement-clock, temporal conversion/interval policy, typed NULL
ordering, and UnixTimestampDec scale differences. This audit does not close
those issues. A repair must inventory and account for the whole owning Go
package; these boundary examples alone are not a package completion unit.

## Protocol completeness and version drift

The existing projection checker verifies fields that Rust declares, but accepts
omitted fields and messages. It also follows the integration branch's go.mod,
not necessarily the requested master reference. Passing it proves neither
complete schema coverage nor master parity.

Descriptor inventories in `proto-inventory.json` show:

| TiPB reference | Omitted fields in present messages | Missing messages / enums in selected import closure | Existing projection check |
| --- | --- | --- | --- |
| June integration pin | 50 fields in 11 messages | 35 / 18 | Pass |
| September master pin | 52 fields in 12 messages | 37 / 19 | Fails: ExecType has 21 local vs 22 upstream entries |

Master adds `TypeExplainForConnection = 21`, Executor's
`explain_for_connection = 26`, and ExecutorExecutionSummary's
`tiflash_hash_table_stats`. No ScalarFuncSig enum values are missing.

Other omissions include Expr's `aggFuncMode` and `order_by`, executor bodies
such as Projection/Join/ExchangeReceiver/IndexLookUp, nested child links,
partitioning fields, and DAG request options. Go's
`pkg/expression/aggregation/agg_to_pb.go` transports aggregation mode and ordering;
Rust's local Expr schema cannot carry them. Go Unistore's MPP executor builder
has branches whose bodies Rust cannot decode. Rust's cophandler supports only
a subset of these executor shapes.

**An omitted protocol field is not automatically a SQL bug.** The inventory
includes TiFlash-specific, deprecated, and potentially unused capabilities.
Each needs an explicit owning-package integration decision and validation.
Do not infer, for example, that every omitted DAG option is consumed by Go's
Unistore context. Long-term schema validation needs a pinned master reference,
complete descriptor inventory, and explicit reviewed omissions, in addition to
checking the types/numbers of included fields. Regenerate outputs from inputs;
do not hand-edit generated Rust.

## Evidence, reproduction, and validation

`go.tsv` contains name, flags, expression protobuf hex, result, error hex,
warning count, comma-separated warning hex, and optional original column-literal
protobuf hex. `rust.tsv` contains name, flags, result, error hex, warning count,
and comma-separated warning hex. `differences.json` is the readable comparison.
Real results compare exact IEEE bits; other values compare SQL bytes.
Diagnostic comparison strips only Go's error-class prefix, retaining SQL code,
message and ordering. Typed Rust error text is compared only when the replay
can expose its SQL identity; other error variants are not fully compared.

Harnesses are text artifacts and are not permanent production/test additions.
To reproduce on the audited source, copy `go-oracle.txt` to
`/private/tmp/pb-expanded_test.go`, `go-overlay.json` to
`/private/tmp/pb-expanded-overlay.json`, `rust-replay.txt` to
`/private/tmp/pb-expanded-rust-probe.rs`, and `registry-probe.txt` to
`/private/tmp/pb-systematic-inventory-probe.rs`. The overlay expects the Go
archive path above. For Rust-only replay, copy `go.tsv` to
`/private/tmp/pb-expanded-go.tsv`. The runner temporarily inserts probes and
restores both source files in its finally block; use a clean checkout with no
concurrent writers. Paths in the schema inventory script reflect this host and
must be adjusted on another host. Both pinned TiPB modules and protoc are needed.

Exact successful collection/comparison commands:

```sh
# Working directory: /private/tmp/tidb-go-structural-20260927
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-expanded-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustSystematicPB$' -count=1 -v

# Working directory: TiDB repository root; runner inserts/restores both probes.
python3 /private/tmp/run-pb-expanded-rust.py
# The runner invokes:
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib audit_pb_registry_inventory -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib audit_expanded_pb_matrix -- --test-threads=1
python3 /private/tmp/compare-pb-expanded.py
python3 /private/tmp/audit-tipb-coverage.py
```

The saved `run-rust.txt`, `compare.txt`, and `audit-proto.txt` are those scripts.
Collection tests passed; this means observations were captured, **not parity
passed**. Go failpoint cleanup returned refcount zero. `validation.txt` retains
final log excerpts, including harmless archive-not-a-Git-repository messages;
the full verbose logs are not committed. Evidence keys were checked for 8,298
unique and matching rows, with the 392/368/51 totals asserted.

No production code changed, so no new lint or SQL integration run is claimed.
The repository's locked server build gates still apply to this rust/docs change:
`cd rust && cargo build --locked -p tidb-server` in the pre-commit hook and again
immediately before push. Build success does not resolve the reported differences.

Not verified: exhaustive input space, all protocol capability semantics,
full-package parity, every Rust error variant's SQL diagnostics, end-to-end
SQL/cluster execution, or sysbench/TPCC/TPCH/YCSB performance. This documentation
change has no runtime effect; the observed correctness and compatibility risks
remain in the implementation.
