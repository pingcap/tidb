# zcode 会话独立进度（避免与并发实例竞写 PROGRESS.md）

> inUnion 实现规格（已完成；精确落点：求值名→CastType 映射在 scalar_function.rs；eval_cast 的 UnsignedInUnion 分支；build_cast_function 的 name match 在 simple_expr.rs）：①tidb-ast CastType 加 UnsignedInUnion 变体（镜像 Unsigned）；②tidb-expr cast.rs eval_cast 加臂：负输入钳 0（Go builtin_cast.go:998）；③simple_expr.rs build_cast_function 加 in_union 参数（unsigned-int 目标+in_union → 内部名 cast_unsigned_in_union）；④build_cast_to 保持 false 包装；新增 build_cast_to_in_union 传 true；⑤set_opr.rs 与递归 CTE 的 cast 站点改调 in-union 变体；⑥回归：ordinary unsigned 保持低位转换，in-union 负输入为 0。证据：`rust/testport/receipts/expression_planner_in_union.md`。

## 已完成（已推送 hparser-integration）

- **37b3c17f2b（已推送）**：decimal 源 in-union 钳 0 臂（values 路由 cast_decimal_in_union → 负数钳 Real(0)，Go builtin_cast.go:1650-1661）+ 2 断言（负钳 0 pre-fix 失败、正透传）。expr 唯一失败仍为已知网络 flaky。

- temporal 复合单元 pins（632d55f3f2）、两分组形状 pins（608dda6d29）
- 审计对账：expr-builtin item 1/2/3/4/6/7 全闭环（466d4e6120/bdf90f7245）；chunk A-3 核实过期（0b8f2de438）
- 2026-09-04: `BuildCastFunction4Union` unsigned-integer `inUnion` carrier across `tidb-ast` → `tidb-expr` → `tidb-planner`, including recursive CTE projections; focused regressions and receipt are complete, and the batch is pushed to `hparser-integration`.

## 队列
1. parser #11（结构性）
2. 分区裁剪验证（等用户对照查询）

- 2026-09-04: chunk A-1 datum storage parity is implemented in Rust and
  validated in the isolated worktree. `Datum::Decimal` now follows Go's
  fixed-cell prefix/truncation at `Chunk::append_datum` and every `MutRow`
  datum/value entry point without introducing an overflow panic. Receipt:
  `rust/testport/receipts/chunk_a1_datum.md`; pushed as commit
  `c59b2bd60e` to `hparser-integration`.

- 2026-09-04: aligned Rust `tidb-datatype` JSON text rendering with Go's
  `jsonMarshalStringTo`: scalar values and object keys now escape U+2028/U+2029
  as `\\u2028`/`\\u2029` while preserving all other `serde_json` behavior.
  Focused and full owner validation are recorded in
  `rust/testport/receipts/json_u2028_escape.md`; pushed as commit
  `242d294f2c` to `hparser-integration`.

- 2026-09-04: aligned Rust `tidb-datatype` `JSON_MERGE_PRESERVE` with Go's
  adjacent-object grouping and one-level array flattening. The interrupted
  object-run regression is recorded in
  `rust/testport/receipts/json_merge_preserve.md`; pushed as commit
  `71ffce262e` to `hparser-integration`.

- 2026-09-04: aligned the Rust `pkg/kv` write-conflict marker with Go's
  `TxnRetryableMark`. The generic 9007 driver error now appends
  `[try again later]`, with a focused code/SQLSTATE/message regression and
  complete `pkg/kv`/`tidb-executor` inventories in
  `rust/testport/receipts/kv_write_conflict_retry_marker.md`; the batch is
  included in the final pushed change.

- 2026-09-04: aligned Rust `tidb-error::registered_std` with Go's
  `pkg/util/dbterror` lookup precedence. Overlapping codes now prefer the
  TiDB/`errno` catalogue, with focused 3143/1243/1820 message and placeholder
  regressions; the complete owner inventory and Ready profile are recorded in
  `rust/testport/receipts/dbterror_registered_std_precedence.md`; pushed as
  commit `3c1119e3b6` to `hparser-integration`.

- 2026-09-04: aligned the Rust `tidb-datatype` DATETIME validation ceiling
  with Go's complete `checkDateRange` comparison. The exact
  `9999-12-31 23:59:59.999999` maximum remains valid, while a packed
  microsecond above `999999` at that exact second is rejected and earlier dates
  retain Go's ordering. The complete owner inventory, focused regression, and
  Ready profile are recorded in
  `rust/testport/receipts/types_time_validate_max_datetime.md`.

- 2026-09-04: aligned Rust decimal `DIV` with Go's unsigned result-width
  conversion. `Decimal::div_rem_unbounded` preserves quotients above
  `i64::MAX`; `tidb-expr` now returns the full-range `Datum::UInt` when either
  operand is unsigned, while retaining Go's negative overflow and truncated
  zero rules. Focused regressions, complete owner profiles, and the known
  external JSON-schema fixture failure are recorded in
  `rust/testport/receipts/expression_intdiv_unsigned_width.md`.

- 2026-09-04: aligned `tidb-datatype::Time::to_packed_uint` with Go's raw
  bit-pack. Synthetic invalid clock/fraction fields now produce the source
  packed bits instead of a Rust-only range error; strict validation remains
  on parse/conversion paths. Focused codec regressions and the complete owner
  profile are recorded in `rust/testport/receipts/types_time_packed_raw.md`.

- 2026-09-04: aligned `tidb-datatype::round_duration_fsp` with Go's
  `Duration.RoundFrac`/`time.Time.Round` behavior. Exact negative half-way
  values now round toward zero (positive infinity), while values past the
  midpoint still round away from zero. Focused tie regressions and the complete
  owner profile are recorded in
  `rust/testport/receipts/types_duration_round_ties.md`.

- 2026-09-04: aligned datatype and live expression `STR_TO_DATE` exhaustion
  handling with Go's `ctx[token] = 0` state. `%p`/`%H` meridiem fixes now see
  exhausted token presence, while empty fractions and skip tokens retain their
  source behavior. Focused owner regressions and Ready profiles are recorded in
  `rust/testport/receipts/types_str_to_date_exhaustion.md`.

- 2026-09-04: aligned the numeric float-string datetime path with Go's
  `ParseTimeFromFloatString` context flags. `ALLOW_INVALID_DATES` now preserves
  `2020-02-31`, while strict mode rejects it; focused source regressions and
  the complete datatype owner profile are recorded in
  `rust/testport/receipts/types_float_string_invalid_date.md`.

- 2026-09-04: aligned TIMESTAMP DST-gap parsing and write diagnostics with
  Go's `parseTime`/`adjustTimestampErrForDST`. A Los Angeles
  `2018-03-11 02:00:16` value becomes `03:00:00`; expression casts and
  lenient writes emit 8179 while strict writes return it, preserving the
  adjusted value. Focused parser/cast/write regressions and owner Ready
  results are recorded in
  `rust/testport/receipts/types_timestamp_dst_gap.md`.

- 2026-09-04: aligned numeric zero-date parsing with Go's
  `FlagIgnoreZeroDateErr`. Strict `ParseTimeFromNum(0)` returns the zero
  fallback beside a temporal error, while default expression conversions keep
  the accepted zero. Focused parser/conversion regressions and owner Ready
  results are recorded in
  `rust/testport/receipts/types_parse_time_from_num_zero.md`.

- 2026-09-04: aligned `Time.StrToDate` zero-in-date validation with Go's
  `FlagIgnoreZeroInDate`. Partial formats now reject zero month/day values
  when the flag is clear and preserve them when enabled; source-vector and
  benchmark callers retain the default permissive path. Focused regression and
  owner Ready results are recorded in
  `rust/testport/receipts/types_str_to_date_zero_in_date.md`.

- 2026-09-04: closed the T11 audit entry for `STR_TO_DATE` `%.'` punctuation.
  The already-pushed datatype and expression changes share Go's Unicode
  punctuation classifier; focused regression and Ready evidence remain in
  `rust/testport/receipts/expression_collation_audit.md`.

- 2026-09-04: aligned decimal `ModeCeiling` with Go's split rounding logic.
  Non-word-aligned cuts inspect only the first discarded digit, while aligned
  cuts scan the full discarded word suffix. Focused decimal regression and
  owner Ready results are recorded in
  `rust/testport/receipts/types_decimal_round_ceiling.md`.

- 2026-09-04: aligned decimal `FromBin` corruption state with Go by exposing
  the zero receiver and fixed payload size alongside `BadNumber`; the strict
  wrapper remains compatible. Focused regression and Ready evidence are in
  `rust/testport/receipts/types_decimal_from_bin_failure.md`.

- 2026-09-04: aligned Go's NUL-truncated DOUBLE warning subjects across every
  Rust warning site, with datatype and live cast regressions. Evidence is in
  `rust/testport/receipts/types_float_warning_nul.md`.

- 2026-09-04: aligned bounded decimal multiplication overflow with Go's
  sign-preserving receiver state. Opposite-signed overflow products now render
  `-0`; the focused regression and Ready profile are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned fixed-word decimal parser error identity with Go.
  Empty/digit-less input now returns `TruncatedWrongValue`, while exponent
  overflow remains `BadNumber`; the focused regression and Ready profile are
  recorded in `rust/testport/receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned `FieldType::source_string` with Go's strict integer
  display-width default. BIGINT metadata with a deprecated `(M)` width now
  renders `bigint BINARY`; focused regression and Ready evidence are recorded
  in `rust/testport/receipts/types_explain_format_audit.md`.

- 2026-09-04: reconciled empty-collation field classification with Go's
  spelling-authoritative `IsBinaryStr`. A legacy JSON field with `Collate:""`
  remains a character string and needs restored data; the focused regression is
  recorded in `rust/testport/receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned decimal add/sub fixed-word overflow with Go's
  leading base-1e9 word heuristic. A full nine-word `999999999…` operand plus
  one now returns the Go overflow/max-value pair, while smaller carries remain
  valid. Focused regression and Ready evidence are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
## chunk A-1 范围确认（下批实现）
## chunk A-1 范围确认（下批实现；树当前全绿 1128/0 expr lib，A-1 实现需新上下文完整验证）

> 实现备注：两型结构已初读——MyDecimal 为 base-1e9 word_buf + digits 计数；Decimal 为 DecimalDigits + scale/storage_scale/declared-shape。直接转换需语义级 digit 翻译（非文本），并处理 result_frac/storage_scale 的对应字段。工作量 ≈ 100-150 行 + 向量回归。

现场：column.rs:430-450 解码路径 `MyDecimal::from_raw_bytes(raw)` → `to_string_bytes()` 文本 → `Decimal::parse_mysql(&text)`。忠实修复 = tidb-datatype 提供 MyDecimal→Decimal 直接转换（绕过文本往返），替换二次解析。前置：读两型结构（mydecimal.rs 的 words/digits 表示 vs decimal/mod.rs 的表示）决定转换实现面。回归：预置 Go FromBin 字节向量往返钉住。


## 下批规格（real 源 in-union 臂，Go builtin_cast.go 精确行号）

1. **real→INT 无符号目标**（:1370-1380，castAsRealToIntSig）：`else if b.inUnion && val < 0 { res = 0 }` —— 负实数钳 0（非 in-union 非 negative 走 ConvertFloatToUint 溢出转换）。Rust 侧：real 源 + unsigned int 目标的 in-union 名臂加此钳。
2. **real→DECIMAL**（:1405-1420，castAsRealToDecimalSig）：`if !b.inUnion || val >= 0 { FromFloat64 } else { 置零 decimal }` —— in-union + 负值 → 零 decimal。
3. 回归：负实数钳 0（pre-fix 失败）、正实数透传、非 in-union 负实数走普通转换。

- 已推送 430bb835594：real→int in-union 钳 0 臂（Go castAsRealToIntSig :1370-1380 语义：负实数钳 0 而非 unsigned wrap）+ 2 回归（pre-fix 失败已验证）。rebase 到远端最新（含另一会话 mview build-sql 提交）后推送。

- 已推送 66122ec76eb（rebase 到远端最新 2a1f4b900fa DST-gap 提交之上）：real→decimal in-union 置零臂 + 2 回归（负数钳 0、正数 FromFloat64 透传）。expr 套件全绿（1127+2/0 在 lib、18+0 集成）。

- 2026-09-04: Rust `Datum::compare_with_error` now retains Go's ordering
  beside temporal/duration parse errors and numeric/decimal string truncation
  diagnostics. The strict `Datum::compare` wrapper remains unchanged for
  error-only callers; focused bidirectional temporal and numeric regressions
  plus the datatype Ready profile are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: Rust `Datum::compare_with_context` now carries statement
  zero-in-date/invalid-date flags and the explicit session timezone through
  temporal string ordering. Focused `ALLOW_INVALID_DATES` and timezone-offset
  regressions are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`; live expression
  warning/context publication remains the open D5 caller follow-up.
- 2026-09-04: Rust live temporal comparisons now read `Columns` date modes and
  session timezone, rejecting invalid dates in strict mode and publishing 1292
  through the warning sink. Focused evaluator regressions and the Ready
  profile are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`; direct datatype
  callers still own publication of returned comparison diagnostics.
- 2026-09-04: Rust aggregate decimal wrappers now preserve Go's unspecified
  source scale through internal `cast_decimal` dispatch and restore strict
  constant precision/scale refinement. The formerly ignored REAL
  `123.555` regression is active; focused and Ready evidence are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: aligned Rust UNION decimal casts with Go's source-specific
  `BuildCastFunction4Union` signatures. REAL/integer/DECIMAL negative sources
  clamp to zero where Go does, negative unsigned text is discarded before
  parsing without a warning, and positive DECIMAL values retain their type
  before the merged precision/scale is applied. Focused regressions and Ready
  evidence are recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: activated the full Go-derived cast-wrapper metadata tables in
  Rust: 51 `WrapWithCastAsDecimal` constant rows and 40 `CAST AS CHAR` width
  rows now execute against the normal wrapper/builder paths, covering source
  widths, decimal caps, temporal FSP, JSON widening, and blob families.
  Focused evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: aligned Rust BINARY-source `CAST AS CHAR` with Go's
  `HandleBinaryLiteral`/`from_binary` boundary. Invalid octets now return the
  successfully decoded prefix and publish warning 3854 in non-strict mode;
  valid bytes remain unchanged. Focused and Ready evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: activated Go's TO_BASE64 GBK session-charset rows using a
  connection-aware Rust resolver. String literals now enter the ordinary
  `to_binary` boundary with GBK metadata before base64 encoding, matching
  `0ru2/sj9` and related rows. Focused evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- parser #5 (RESERVED_KEYWORDS 缺 DATABASE/DATABASES/DISTINCT) 核实已被并发会话修复 — RESERVED_KEYWORDS 当前含全部 236 条含这三个关键词。审计项 5 关闭。

- 与远端同步确认：planner crate 内 .first()?/.get(1)? 模式已清零（系统性扫描确认）。四包聚合 1591+/0。
- 下批候选：chunk A-1 直接转换（需读两型结构后实现 ~150 行）、parser #11 charset-aware scanner（结构性）、Time::round_frac TZ（签名变更跨两 crate）。
