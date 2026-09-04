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

## `pkg/util/dbterror` catalogue precedence batch

The focused `registered_std` regression and serialized `tidb-error` owner
profile passed (8 unit tests plus 31 generated/source integration tests).
Owner compilation, formatting, whitespace, and strict clippy all passed for
the crate. The TiDB/`errno` catalogue is now preferred for overlapping codes,
matching Go's `ErrClass.NewStd`; full details are in
`rust/testport/receipts/dbterror_registered_std_precedence.md`.

## `pkg/types` DATETIME maximum-precision batch

The focused maximum-bound regression and serialized `tidb-datatype` owner
profile passed. `Time::validate` now rejects only the exact Go
`MaxDatetime`-ceiling precision escape; owner compilation, formatting, and
whitespace checks passed. Strict clippy remains blocked by the unrelated
`tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics. Full
commands and remaining temporal boundaries are recorded in
`rust/testport/receipts/types_time_validate_max_datetime.md`.

## `pkg/expression` decimal `DIV` unsigned-width batch

The focused decimal quotient and Go-arithmetic regressions passed. The
serialized `tidb-datatype` owner profile passed with 391 unit and 63
generated/source integration tests. The serialized `tidb-expr` owner profile
had 1,121 passes, one pre-existing external HTTP JSON-schema fixture failure,
and 130 documented gap tests ignored; the new arithmetic regression passed.
Owner compilation, formatting, and whitespace checks passed. Strict clippy
remains blocked by the unrelated `tidb-mysql/src/consts.rs:117-120`
`map-or-identity` diagnostics. Full commands and boundaries are recorded in
`rust/testport/receipts/expression_intdiv_unsigned_width.md`.

## `pkg/types` raw `ToPackedUint` batch

The focused raw-pack and existing codec regressions passed. The serialized
`tidb-datatype` owner profile passed with 392 unit and 63 generated/source
integration tests; benchmark targets compiled. Owner
compilation, formatting, and whitespace checks pass. Strict clippy remains
blocked by the unrelated `tidb-mysql/src/consts.rs:117-120`
`map-or-identity` diagnostics and generated workspace diagnostics. Full
commands and remaining temporal boundaries are recorded in
`rust/testport/receipts/types_time_packed_raw.md`.

## `pkg/types` duration `RoundFrac` tie-direction batch

The focused exact-negative-tie and past-tie regressions passed, as did the
existing duration-method and temporal codec regressions. The serialized
`tidb-datatype` owner profile passed with 392 unit tests and 63 generated/source
integration tests; benchmark targets compiled. Owner compilation, formatting,
and whitespace checks passed. Strict clippy remains blocked by the unrelated
`tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics. Full commands,
inventory, and remaining temporal boundaries are recorded in
`rust/testport/receipts/types_duration_round_ties.md`.

## `pkg/types`/`pkg/expression` `STR_TO_DATE` exhaustion batch

The focused datatype and live-expression regressions passed, as did all
existing `STR_TO_DATE` source vectors. The serialized `tidb-datatype` owner
profile passed with 393 unit tests and 63 generated/source integration tests;
the serialized `tidb-expr` owner profile had 1,130 passes, one pre-existing
external HTTP JSON-schema fixture failure, and 125 documented gap tests
ignored. Owner compilation, formatting, and whitespace checks passed. Strict
clippy remains blocked by unrelated `tidb-mysql` `map-or-identity` and
generated `tidb-proto` diagnostics. Full commands and boundaries are recorded
in `rust/testport/receipts/types_str_to_date_exhaustion.md`.

## `pkg/types` float-string invalid-date batch

The focused numeric float-string regression and existing source vectors passed.
The serialized `tidb-datatype` owner profile passed with 394 unit tests and 63
generated/source integration tests; benchmark targets compiled. Owner
compilation, formatting, and whitespace checks passed. Strict clippy remains
blocked by the unrelated `tidb-mysql/src/consts.rs:117-120`
`map-or-identity` diagnostics. Full commands and remaining temporal boundaries
are recorded in `rust/testport/receipts/types_float_string_invalid_date.md`.

## `pkg/types` TIMESTAMP DST-gap batch

The focused parser, expression-cast, and executor write-cast regressions
passed. A Los Angeles `2018-03-11 02:00:16` TIMESTAMP is adjusted to
`03:00:00`; read casts and lenient writes report Go's 8179 warning, while
strict writes return 8179. The serialized `tidb-datatype` owner profile passed
with 395 unit and 63 generated/source integration tests; the serialized
`tidb-expr` profile had 1,132 passes, one known external HTTP JSON-schema
fixture failure, and 124 ignored gap tests. Owner compilation, formatting, and
whitespace checks passed. Strict datatype clippy remains blocked by the
unrelated `tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics.
Full inventory, commands, and boundaries are recorded in
`rust/testport/receipts/types_timestamp_dst_gap.md`.

## `pkg/types` numeric zero-date flag batch

The focused parser and datum-conversion regressions passed. Rust
`parse_time_from_num(0)` now returns a `ZeroDate` error beside the zero
fallback when `FlagIgnoreZeroDateErr` is clear, while default statement and
expression callers preserve Go's accepted zero. The serialized owner profiles,
owner compilation, formatting, and whitespace checks passed. The expression
profile retains one known external HTTP JSON-schema fixture failure and the
broad executor profile retains existing planner/storage fixture failures.
Strict datatype clippy remains blocked by the unrelated
`tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics. Full commands,
inventory, and remaining boundaries are recorded in
`rust/testport/receipts/types_parse_time_from_num_zero.md`.

## `pkg/types` `StrToDate` zero-in-date batch

The focused datatype regression passed. `Time::str_to_date` now forwards an
explicit zero-in-date allowance to `Time::validate`, so partial formats reject
zero month/day values when the Go flag is clear and preserve them when it is
set. Owner compilation, formatting, and whitespace checks passed; the standard
expression and executor profile blockers remain unchanged. Full commands,
inventory, and boundary notes are recorded in
`rust/testport/receipts/types_str_to_date_zero_in_date.md`.

## `pkg/expression` `STR_TO_DATE` punctuation closure

The existing focused regression and owner Ready profile cover the T11 fix:
`%.` consumes Go's Unicode punctuation set (including U+00BF) and rejects
ASCII symbols such as `+`. The shared classifier is used by both Rust parser
owners. Full source inventory and validation evidence remain in
`rust/testport/receipts/expression_collation_audit.md`.

## `pkg/types` decimal `ModeCeiling` batch

The focused decimal round regression passed. Rust now mirrors Go's
non-word-aligned `ModeCeiling` first-digit inspection and its word-aligned
full-suffix control. Owner profiles, compilation, formatting, and whitespace
checks are recorded in `rust/testport/receipts/types_decimal_round_ceiling.md`;
the standard expression and executor profile blockers remain unrelated.

## `pkg/types` decimal `FromBin` failure-state batch

The focused corrupt-word regression passed. Rust now exposes Go's zero
receiver and fixed payload consumption beside `BadNumber` for callers that
need cursor progress, while the strict decoder API remains unchanged. Full
owner profiles and the baseline blockers are recorded in
`rust/testport/receipts/types_decimal_from_bin_failure.md`.

## `pkg/types` float-prefix NUL warning batch

The datatype and live `CAST(... AS DOUBLE)` focused regressions pass. All
DOUBLE warning builders now use the source-compatible trimmed subject and
stop at NUL. Full owner profiles and baseline blockers are recorded in
`rust/testport/receipts/types_float_warning_nul.md`.
