# Build and test record

The WIP focused profile passed during construction:

```text
cargo test --locked -p tidb-expr --lib builtin_cast_semantics::tests::union_unsigned_integer_cast_clamps_negative_values
cargo test --locked -p tidb-planner --lib plan_builder::set_opr_tests::union_unsigned_widening_uses_the_in_union_cast_signature
```

The full Ready profile is recorded in the parity receipt after the final
rebase against `origin/hparser-integration`.

Observed before the final rebase: focused expression/planner tests, the
planner set-operation owner suite (35/35), all-target compilation, formatting,
and `git diff --check` passed. The broad owner test retained one unrelated
loopback-PD label-delivery failure; the expression nextest run retained one
unrelated loopback HTTP JSON-schema fixture failure. Strict clippy was blocked
by pre-existing diagnostics in unrelated `tidb-mysql`, generated protobuf, and
other workspace code.

## Chunk A-1 decimal datum batch

Focused and owner commands were run from `rust/`:

```text
cargo test --offline --locked -p tidb-chunk --lib chunk::tests::decimal_datum_overflow_uses_go_truncation_without_panicking -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-chunk --lib -- --test-threads=1
cargo check --offline --locked -p tidb-datatype -p tidb-chunk --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype -p tidb-chunk --all-targets -- -D warnings
```

The focused regression, serialized datatype owner (381 unit + 63 generated/
integration tests), owner compilation, formatting, and diff checks passed. The
serialized chunk owner retained 241 passes, 35 spill/temp-file or dependent
row-container failures, and 4 ignored tests; these are the documented macOS
temporary-storage/concurrency failures. Strict clippy stopped on unrelated
`tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics. Full details
are in `rust/testport/receipts/chunk_a1_datum.md`.

## JSON_MERGE_PRESERVE grouping batch

The focused merge regression, serialized datatype owner profile (384 unit + 63
generated/integration tests), owner compilation, formatting, and diff checks
passed. Strict clippy remains blocked by the unrelated
`tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics. Full command
and risk details are in `rust/testport/receipts/json_merge_preserve.md`.

## `pkg/kv` write-conflict retry-marker batch

The focused `tidb-executor` regression passed and pins Go's exact 9007
`Write conflict, please retry the transaction [try again later]` wire message.
The serialized executor owner profile retained 1,030 passes and 136 existing
planner/remote/spill/fixture failures; owner compilation, formatting, and
whitespace checks passed. Strict clippy remains blocked by unrelated
`tidb-mysql` and generated `tidb-proto` diagnostics. The complete inventory,
commands, and boundaries are in
`rust/testport/receipts/kv_write_conflict_retry_marker.md`.
