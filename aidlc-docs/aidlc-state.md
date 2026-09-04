# AI-DLC state

- Workspace: brownfield TiDB Go/Rust parity worktree
- Stage: CONSTRUCTION / Build and Test (complete for the current bounded work unit)
- Work unit: `tidb-expr` BINARY-source string-cast parity batch
- Go oracle: fetched `origin/master` (`fc7788ff517c3407dc7e000be989ab23e6648211`)
- Rust target: dedicated worktree branch `codex/hparser-parity-latest`
- User approval: execution requested directly; no interactive approval pause
- Validation: focused valid/invalid BINARY-source CAST AS CHAR regressions pass.
  Ready reports 409 datatype unit and 64 source/integration passes; expression
  reports 1,160 passes, one known loopback HTTP JSON-schema fixture failure,
  and 116 ignored; executor reports 1,058 passes and 121 existing
  planner/storage/fixture failures. All three owner checks plus formatting and diff checks
  pass. Strict datatype clippy remains blocked only by the unrelated
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics.
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
- Current batch: the previously implemented `STR_TO_DATE` punctuation fix is
  now recorded as T11 closed in the main audit. Datatype and expression use
  the shared Go Unicode punctuation classifier; focused regressions and Ready
  evidence remain in `rust/testport/receipts/expression_collation_audit.md`.
- Current batch: Rust `Decimal::round_ceiling_to_scale` now matches Go's
  non-word-aligned first-discarded-digit behavior while retaining the
  word-aligned full-suffix scan. Receipt:
  `rust/testport/receipts/types_decimal_round_ceiling.md`.
- Current batch: Rust `Decimal::from_bin_with_failure` now preserves Go's
  zero receiver and fixed payload size on corrupt legal-shape input. Receipt:
  `rust/testport/receipts/types_decimal_from_bin_failure.md`.
- Current batch: Rust DOUBLE warning sites now use Go's trimmed, NUL-terminated
  diagnostic subject. Receipt:
  `rust/testport/receipts/types_float_warning_nul.md`.
- Current batch: Rust bounded decimal multiplication now preserves Go's
  operand sign on overflow, so opposite-signed overflow products render `-0`.
  Receipt: `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust fixed-word decimal parsing now preserves Go's
  `ErrTruncatedWrongVal("DECIMAL", ...)` identity for empty or digit-less
  input, distinct from exponent `BadNumber`. Receipt:
  `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust `FieldType::source_string` now uses the strict integer
  display-width default, so BIGINT metadata omits deprecated `(M)` widths like
  Go. Receipt: `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust field-type binary classification remains authoritative to
  the stored collation spelling, so empty legacy `Collate` values do not inherit
  the cached `Binary` enum. Receipt:
  `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust `Datum::compare_with_error` now preserves Go's ordering
  beside temporal/duration parse errors and numeric/decimal string truncation
  diagnostics, while the strict `compare` wrapper remains source-compatible.
  Receipt: `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust `Decimal::add_mysql` and opposite-sign `sub_mysql` now
  preserve Go's leading-word carry heuristic and nine-word overflow boundary.
  The focused 81-digit regression and Ready counts are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust's internal decimal cast dispatch now carries Go's
  unspecified scale sentinel, and `WrapWithCastAsDecimal` restores strict
  constant precision/scale refinement. The REAL `123.555` regression is active
  and its Ready evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust UNION decimal casts now select source-specific Go
  signatures, clamp negative REAL/integer/DECIMAL sources and negative unsigned
  text before parsing, preserve positive DECIMAL values, and apply the merged
  target shape afterward. Focused regressions and the Ready profile are in
  `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: the complete Go-derived cast-wrapper metadata tables are
  active in Rust: 51 decimal constant rows and 40 CHAR-width rows cover
  source families, caps, FSP, JSON, and blob/string sizing. Focused evidence
  is in `rust/testport/receipts/types_explain_format_audit.md`.
- Current batch: Rust BINARY-source `CAST AS CHAR` now follows Go's
  `HandleBinaryLiteral`/`from_binary` path, preserving the valid decoded prefix
  and publishing warning 3854 for invalid octets. Focused evidence is in
  `rust/testport/receipts/types_explain_format_audit.md`.
- Next action: run the Ready profile, commit and push this BINARY-source cast
  batch, then continue with the next executable Rust package boundary. Direct
  datatype comparison warning publication remains a bounded API follow-up.
