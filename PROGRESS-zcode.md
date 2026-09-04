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
- 2026-09-04: activated Go's TestDate zero-date SQL-mode rows through explicit
  Rust statement contexts. `DATE()` now has executable regressions for
  preserving zero components with the modes disabled and returning NULL plus
  warning 1292 under `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`. Focused and Ready
  evidence is recorded in `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: activated Go's MD5/PASSWORD GBK connection-charset rows through
  a resolver-backed Rust rewrite. Valid values now use the ordinary
  `to_binary` boundary before hashing, and unrepresentable `ㅂ123` values
  surface the same conversion error. Focused and Ready evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: aligned CRC32's evaluator with Go's raw-byte `EvalString`
  contract and activated the GBK connection-charset rows through the ordinary
  `to_binary` rewrite. Focused and Ready evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- 2026-09-04: implemented `CURRENT_RESOURCE_GROUP()` in the Rust expression
  evaluator. The new `Columns::current_resource_group()` accessor carries the
  effective statement group, with Go-derived value and NULL regressions.
  Focused and Ready evidence is recorded in
  `rust/testport/receipts/types_explain_format_audit.md`.
- parser #5 (RESERVED_KEYWORDS 缺 DATABASE/DATABASES/DISTINCT) 核实已被并发会话修复 — RESERVED_KEYWORDS 当前含全部 236 条含这三个关键词。审计项 5 关闭。

- 与远端同步确认：planner crate 内 .first()?/.get(1)? 模式已清零（系统性扫描确认）。四包聚合 1591+/0。
- 下批候选：chunk A-1 直接转换（需读两型结构后实现 ~150 行）、parser #11 charset-aware scanner（结构性）、Time::round_frac TZ（签名变更跨两 crate）。

- 本会话累计交付 31 个提交到 hparser-integration（全部四包全绿验证），覆盖 planner/parser/datatype/codec/expr 五个 crate 的 Go 对照修复。**快赢批次已全部消化**，剩余均为结构性/设计门槛项：chunk A-1（datum 决策）、parser #11 charset-aware scanner（结构性）、Time::round_frac TZ（签名变更）、CHAR/VARCHAR padding（storage 面）、~175 站点 error-code 重构（跨 crate）。

## 环境变更(2026-09-05 会话恢复)
- 本机 homebrew 已被卸载(/opt/homebrew 不存在, brew 命令缺失)。旧行话里 `OPENSSL_DIR=$(brew --prefix openssl)` 的导出已失效——设置 OPENSSL_DIR 反而会让 openssl-sys 走系统路径分支而失败。
- 正确做法: 不要设置 OPENSSL_DIR/DYLD_FALLBACK_LIBRARY_PATH。tikv-client-rs 的 `openssl = {version="0.10", features=["vendored"]}`(主 worktree 有未提交的同款补丁; parity worktree 我也加了同样的**未提交**本地补丁, 不要 commit 该文件)会源码编译 OpenSSL 到 target, 一次约几分钟, 之后正常。
- 验证: `cargo test -p tidb-planner --lib` = 908 passed / 0 failed(当前 FETCH_HEAD 同步点)。早前 /tmp/wt_lib13.txt 的 5 个失败为过期树状态, 非当前远端。
- Go 工具链同样从 PATH 消失; 但 ~/.cache/codex-go1.25.10/go(1.25.12, 与 go.mod 匹配)和 codex-gopath-1.25.10 仍在。make lint 用:
  `PATH=~/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=~/.cache/codex-gopath-1.25.10 make lint`
- chunk lib 35 失败/executor lib 122 失败为**预存环境失败**(macOS 临时目录 spill NotFound 等), stash 对照证明与本批 codec 修复无关(有/无修复均同数失败); codec/datatype/planner/distsql 全绿。
- 批次完成并推送 `a8c15bd8f8f`(codec: 解码 decimal cell 时以 cell 的 resultFrac 为可见 scale, 弃文本往返):
  Go 证据 = chunk cell 原样拷贝 40 字节 MyDecimal(DecimalDiv 使 resultFrac 与 digitsFrac 独立), String() 按 resultFrac 渲染。
  Rust column.rs 旧路径 to_string_bytes→parse_mysql 把可见 scale 钉在 digitsFrac, 多显小数位。
  修复 = Decimal::from_my_decimal 直转(该 API 已在 tidb-datatype 存在, chunk/row/spill 路径早已使用)。
  回归 = patched-cell 测试(resultFrac=2/digitsFrac=6 → Display "1.23", 系数 "1234567"); pre-fix 基线失败(scale 6≠2)已证。
  门禁: codec 46+166 全绿; datatype 410/0; planner 908/0; distsql 253+28/0; fmt/clippy/diff-check/make lint PASS。
  chunk(35)/executor(122)为预存环境失败, stash A/B 证明与本批无关。
- 本会话累计 32 个提交。下批: parser #11 charset-aware scanner。
- parser #11(client-charset scanner)关闭为 parity-by-API 并推送 `2d97d650ba8`:
  核实链条完整——GBK/big5/sjis 危险字节对(lead≥0x81 + trail 0x5C/0x27/0x60)永非法 UTF-8; mysql_connection.rs 查询解码门先行转码/拒绝非 UTF-8; Lexer 全链 &str 无法表达该输入。加 charset 字段 = 无可达行为的规格化声明(违反 No speculative behavior), 故记录关闭而非实现。
- 下批: Time::round_frac 时区语义(跨 tidb-datatype/tidb-expr)。
- Time::round_frac 时区项关闭并推送 `fb6e70a35e0`(types: 记录 zone-free to_i64 的调用方审计):
  结论——所有生产调用方 zone 安全: 表达式整数 getter 均在 WrapWithCastAsInt(session zone cast, to_i64_signed_in)之后; ranger YEAR 块的 pre_value 仅喂 out-of-range 算符翻转(2e13 量级 vs ≤2155 年界, 时区不可翻转结论); 直转路径均带 session zone。无需签名变更。
- 下批候选: error-code ~175 站点(跨 crate)、CHAR/VARCHAR padding(storage 面)、Cast flen/flag 族、chunk A-2 offset-table strictness(docs/chunk-and-stats-divergence.md)。
- error-code 批完成并推送 `3f45cc0b89f`(executor: derive every raised SQLSTATE from the error code):
  MysqlError::new 删除 state 参数, 经 mysql_state(NewErr 等价)推导; 246 个字面量站点机械重写; 脚本验证所有 pre-rewrite 字面量与推导值**全部一致**=零行为变化(含 HY000 fallback 语义); 3 个外部 state 重建站点改用 with_state; ParseCoded 运行时 errno 改为 Go 式推导。
  门禁: cargo check --all-targets 清洁; executor 套件失败数与基线一致(122 预存环境失败); fmt/clippy/diff-check/make lint PASS; 推送后 rebase 合并态 error 测试 22/0、codec 46/0。
  本会话累计 33 提交(实际独立批次 4 个推送提交)。
- 下批候选: CHAR/VARCHAR padding、Cast flen/flag 族、chunk A-2。
- 本轮核实(只读): 算术 flen/decimal 规则(builtin_arithmetic.rs)已逐行实现 Go setFlenDecimal4*/setType4Div*; inUnion 已建模(simple_expr.rs:677 + func.rs 25 处); 时序 cast 目标(wrap_with_cast_as_time)存在。expr-builtin inventory 第 5 条(Cast)已改写为"mostly absorbed", 残差=逐行 BINARY(n)/DECIMAL(p,s) 宽度核对。推送 `fa26adb05b1`+inventory 改写批。
- 下轮恢复点: (1) 逐行核对 BINARY(n)/DECIMAL(p,s) cast 目标宽度 vs builtin_cast.go(残差子批); (2) chunk A-2(docs/chunk-and-stats-divergence.md); (3) 若并发会话新增 MysqlError::new 站点跟随 error-code 新约定。
- Cast 家族闭环: Go WrapWithCastAs{Int,Real,Decimal,String,Time,Duration,JSON,VectorFloat32} 与 Rust wrap_with_cast_as_* 8:8 一一对应; wrap_with_cast_as_string 本轮逐行核对一致(decimal+3/MaxIntWidth/bit(flen+7)/float清 flen/coercibility 三分支); 11 个 wrapper 测试全绿。inventory 残差改指 BINARY(n)/DECIMAL(p,s) 解析器 FieldInfo 宽度(归语句重写面)。
- 下轮恢复点: (1) chunk A-2(docs/chunk-and-stats-divergence.md); (2) 语句重写面 BINARY(n)/DECIMAL(p,s) FieldInfo 宽度; (3) 并发会话新增站点跟随。
