# AI-DLC state

- Workspace: brownfield TiDB Go/Rust parity worktree
- Stage: CONSTRUCTION / Build and Test (complete for the current bounded work unit)
- Work unit: `pkg/types` `StrToDate` zero-in-date parity batch (finding T9)
- Go oracle: fetched `origin/master` (`fc7788ff517c3407dc7e000be989ab23e6648211`)
- Rust target: dedicated worktree branch `codex/hparser-parity-latest`
- User approval: execution requested directly; no interactive approval pause
- Validation: the focused parser and datum-conversion regressions, serialized
  datatype/expression/executor owner profiles, owner compilation, formatting,
  and diff checks pass. The expression profile keeps one known external HTTP
  JSON-schema fixture failure; the broad executor profile keeps its existing
  planner/storage fixture failures. Strict datatype clippy remains blocked
  only by the unrelated `tidb-mysql/src/consts.rs:117-120`
  `map-or-identity` diagnostics.
- Prior commit/push: JSON separator batch `242d294f2c` is pushed to
  `hparser-integration`.
- Commit/push: JSON merge batch `71ffce262e` is pushed to
  `hparser-integration`.
- Commit/push: `pkg/kv` retry-marker batch is validated and the receipt is
  included in the final pushed change.
- Commit/push: `pkg/util/dbterror` precedence batch is validated and pushed as
  `3c1119e3b6` to `hparser-integration`; the later state-only oracle update is
  `8552e1a508`.
- Current batch: Rust `tidb-datatype::Time::validate` now matches Go's
  `MaxDatetime` precision ceiling for DATETIME. Focused and serialized owner
  tests, compilation, formatting, and diff checks pass; strict clippy remains
  blocked only by the unrelated `tidb-mysql/src/consts.rs:117-120`
  `map-or-identity` diagnostics. Receipt:
  `rust/testport/receipts/types_time_validate_max_datetime.md`.
- Current batch: Rust decimal `DIV` now retains a complete quotient until
  Go-compatible `ToInt`/`ToUint` conversion, preserving upper-half unsigned
  BIGINT results and source overflow boundaries. Focused and serialized owner
  results are recorded in
  `rust/testport/receipts/expression_intdiv_unsigned_width.md`.
- Current batch: Rust `Time::to_packed_uint` now mirrors Go's direct raw
  bit-pack and accepts synthetic fields without revalidation. Focused and
  serialized owner results are recorded in
  `rust/testport/receipts/types_time_packed_raw.md`.
- Current batch: Rust `round_duration_fsp` now mirrors Go's
  `Duration.RoundFrac`/`time.Time.Round` tie direction. Exact negative
  half-way values round toward zero, while values past the midpoint round away
  from zero. Focused regressions and the serialized `tidb-datatype` owner
  profile pass; strict clippy remains blocked by the unrelated
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics. Receipt:
  `rust/testport/receipts/types_duration_round_ties.md`.
- Current batch: Rust `tidb-datatype` and live `tidb-expr` `STR_TO_DATE`
  parsers now preserve Go's exhausted-token state for `%p`/`%H` meridiem
  fixing. Focused source suites pass; datatype all-targets passes with 393
  unit and 63 integration/source tests; expr all-targets has 1,130 passes,
  one pre-existing external JSON-schema fixture failure, and 125 ignored gap
  tests. Strict clippy remains blocked by unrelated `tidb-mysql` and generated
  `tidb-proto` diagnostics. Receipt:
  `rust/testport/receipts/types_str_to_date_exhaustion.md`.
- Current batch: Rust's float-string numeric datetime path now forwards Go's
  `allow_invalid_date` context into `parse_time_from_num`. The focused
  regression and serialized datatype owner profile pass with 394 unit and 63
  integration/source tests; strict clippy remains blocked by unrelated
  `tidb-mysql` diagnostics. Receipt:
  `rust/testport/receipts/types_float_string_invalid_date.md`.
- Current batch: Rust TIMESTAMP string and packed numeric parsing now preserve
  Go's DST-gap adjustment and 8179 diagnostic. The marker flows through
  expression casts and write conversion, where lenient rows store the adjusted
  value with a warning and strict rows return 8179. Focused regressions and
  owner Ready results are recorded in
  `rust/testport/receipts/types_timestamp_dst_gap.md`.
- Current batch: Rust numeric temporal parsing now honors Go's
  `FlagIgnoreZeroDateErr` for `ParseTimeFromNum(0)`. Strict datum conversion
  returns the zero fallback beside a temporal error; default expression paths
  retain zero. Receipt:
  `rust/testport/receipts/types_parse_time_from_num_zero.md`.
- Current batch: Rust `Time::str_to_date` now forwards an explicit
  `allow_zero_in_date` flag to `Time::validate`, preserving Go's strict
  `FlagIgnoreZeroInDate` behavior while source-vector callers remain
  permissive. Receipt:
  `rust/testport/receipts/types_str_to_date_zero_in_date.md`.
- Next action: continue with the next executable package boundary after the
  T9 batch is committed and pushed.
