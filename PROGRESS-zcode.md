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
- chunk A-3/A-4 核实为**已修复**(吸收树中 row.rs `_ => return` 零触碰契约 = Go 无 default 臂; NewDecimal 臂已 with_declared_shape 按Go SetFrac 规则), divergence 文档已标记 FIXED(verified 2026-09-05)。
- 下轮恢复点: (1) BINARY(n)/DECIMAL(p,s) 解析器 FieldInfo 宽度面; (2) B-3 命名项(改公开参数名, 属"小而确定"之外, 暂缓); (3) 并发会话新增站点跟随。
- cast 目标元数据批(executor: result field resolver 对齐 Go parseCastType)进行中: 19 向量 Go 派生回归(pre-fix 失败已证: Signed flags 0≠128), 14 臂修复=Signed/Unsigned flen 22+BinaryFlag、BINARY(n) 指定长度翻 TypeString、DATETIME/TIME fsp 的 +1+fsp flen、YEAR 双 unspecified、DOUBLE 22/unspec、FLOAT 独立 Float(12)、JSON (4194304,0)+utf8mb4 全臂落地、Char/Binary/Decimal/Date/Vector 补 BinaryFlag(ParseToJSONFlag 1<<18 wire 截断不可见已注释)。门禁: resolver 6/6、exec lib 335/0、fmt/clippy/diff-check/make lint PASS。
- cast 目标元数据批完成并推送 `01056120c85`(rebase 于 bcb8414aa6f 之上)。下轮恢复点: (1) 重读三份 divergence/audit 文档找新开放项; (2) B-3 命名项(暂缓); (3) 并发会话新增站点跟随。
- F1 关闭: 审计描述的 8005 字面量站点已不存在; 现树 Undetermined 臂 code 1105 + "execution result undetermined" 正是 Go ClassGlobal terror 的 defaultMySQLErrorCode=ErrUnknown 回退(terror.go:266-274), 8005 是 local-latch 写冲突另一错误。文档已标记 CLOSED。
- 下轮恢复点: (1) error-code audit F2/F3/F4 核实(可能同为过期或已由 error-code 批覆盖); (2) distsql-coprocessor-parity.md Rank1/Rank2 两开放项; (3) expr-builtin inventory A/B(DIV decimal 分歧)两项; (4) B-3 命名(暂缓)。
- distsql Rank 1.1/2.1 核实为**已被并发会话修复**: 1.1 两条路径已发计算后的 flags(real_tikv_read 字段+select_push_down_flags 默认; cop_scan 经 StmtContext.push_down_flags, 有测试); 2.1 open_scan 已构造 DistSqlContext + RequestBuilder::from_context(resource group/replica read/paging), 残余字段(priority/task id/max_execution_time 等)已在代码中列为显式队列。audit 文档两节已标记 FIXED(verified)。
- 下轮恢复点: (1) error-code audit F2/F3/F4 核实; (2) StmtContext 补 priority/task_id/max_execution_time 穿线(2.1 显式残余); (3) B-3 命名(暂缓)。
- F4 批(executor: 删除 DdlAdmissionError::new 隐藏默认)推送中: ~40 站点改 with_code(GENERIC_ERROR_CODE,...) 显式命名(零行为变化), const 升 pub(crate), From<ColumnTypeError> 同改。exec 套件 8 个失败经 stash A/B 证实为预存(失败名单完全一致)。fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) F3 read_only_scan errors 1235/42000; (2) F2 ~59 unknown 站点(需 live 证据, 记录); (3) F4 后续=逐站点 Go errno 对比(~40 个显式 1105)。
- 本轮共 3 推: c4f20f9c7ef(distsql 1.1/2.1 关闭)、ca8b39ec1f0(F4 隐藏默认删除)、及上轮 f88422da30a(F1)。
- F3 起点: rust/crates/tidb-planner/src/read_only_scan/errors.rs — ReadOnlyScanError/UnsupportedReadOnlyFeature(23 变体)无 MySQL code 字段, 经 SqlQueryError::unknown 成 1105; Go 等价拒绝=ErrNotSupportedYet 1235/42000。修法=加 code 字段+variant→errno 映射+SqlQueryError 构造点接线。
- F3 核实为确认开放(拓扑已全程追踪): ReadOnlyScanError/PreparedPlanError 无 code → RealTiKvReadError::Plan Display 展平 → server 侧 SqlQueryError::unknown(1105/HY000); SqlQueryError 本身(code/state/message)完全可承载。修法已写入 audit doc: planner 加 mysql_code() 访问器(Parse→1064/42000, Unsupported→1235/42000, UnknownTable→1146/42S02, UnknownColumn→1054/42S22, 不变量→1105/HY000) + seam 改用。下轮直接按此执行。
- 下轮恢复点: (1) F3 按上述设计执行; (2) F2 ~59 unknown 站点(需 live 证据, 记录); (3) F4 后续逐站点 Go errno 对比。
- F3 第一步完成: ReadOnlyScanError/PreparedPlanError 的 mysql_code() 访问器落地(逐变体 Go errno: Parse 1064/42000, Unsupported/UnsupportedPredicate 1235/42000, UnknownTable 1146/42S02, UnknownColumn 1054/42S22, 内部不变量 1105/HY000, prepared 语法拒绝 1235/42000), 2 个逐变体回归全绿。planner 911/0, fmt/clippy/diff-check/make lint PASS。
- 残余: server seam(~25 处 unknown 展平点与 F2 共享)采纳 accessor 需先定位 read 管道实际可达站点(live 证据)。
- F3 第一步推送 `67958fd5b7d`(rebase 于远端新提交之上)。本会话累计推送: c4f20f9c7ef, ca8b39ec1f0, 24649548b64(journal), 67958fd5b7d。
- 下轮恢复点: (1) F3 残余=server seam 采纳(需定位可达站点); (2) F2 ~59 unknown(需 live 证据); (3) F4 后续逐站点 Go errno 对比(~40 显式 1105); (4) 重读三份 divergence/audit 文档找新开放项。
- F4 后续评估(记录): with_code(GENERIC) 站点多属"本节点不提供而 Go 不拒绝"形状, Go 无对应 errno, 发明 code 违反 correctness-first; 其中 Go 确有对应错误的站点(ATTRIBUTES 校验/auto_random 基数等)逐站点对照需 Go 侧 grep 佐证, 排低优先级。
- F3 残余评估(记录): ReadOnlyScanError/PreparedPlanError 在 server 语句路径尚无生产消费者(prepare_configured_point_read 仅测试调用), 即 read-only 第二管道未接线=无可达 seam; accessor 已就位待管道接线时一行采纳。
- 文档清扫结论: parser-lexer 12 项全闭环; expr-builtin A-G 全 FIXED; types-datatype D1/D2/时区全闭环; chunk A-1..A-5/B-1/B-2 全闭环; error-code F1/F4/F5/F6/F7/F9 闭环, F2/F3 残余被 live 证据阻塞; distsql 1.1/2.1 闭环, Rank3 response_channel 已正确, 唯一 DEFERRED=read-only tier 警告汇(无 SHOW WARNINGS 面)。队列实质清空。
- 树健康基线(当前 HEAD): planner 911/0, codec 46/0, distsql 28/0, datatype 410/0。队列实质清空, 剩余项均被 live-cluster 证据阻塞(F2/F3-seam/分区裁剪对照)或低价值(F4 逐站点发明 code)。
- 下轮恢复点: (1) 若有 live cluster, 优先 F2/F3-seam 定位可达站点; (2) 否则按用户 goal 遍历下一 Go package 做 parity walk(无既有 audit 文档的面, 如 tidb-session/tidb-statistics 表面)。
- vardef 机械重审计批: 脚本 diff Go Def* 常量(395) vs Rust defaults.rs(400 引用值) — 值级 400 项全部一致; 缺 4 个默认(QUERY_COP_STORE_LIMIT=15/COLUMNAR_STORAGE_ENABLED=true/MERGE_PARTITION_STATS_CONCURRENCY=1/SERVER_MEMORY_LIMIT="80%")已补齐+late_added_defaults_match_go 回归。9 个 Rust-only 扩展(MView/TxnFile/OpenAI/FullOuterJoin/SharedLockUpgrade)属 fork 自有/并发会话活跃区, 不碰已记录。vardef 44+3+3 全绿, fmt/clippy/diff-check/make lint PASS。
- vardef 名字表补齐(第二小批): TIDB_QUERY_COP_STORE_LIMIT / TIDB_COLUMNAR_STORAGE_ENABLED 两个名字常量补入 tidb_vars.rs(脚本提取表原有缺口); vardef 全套 44+3+3 全绿 fmt/diff-check PASS。
- vardef 审计收据写入 rust/docs/vardef-defaults-parity-audit.md; 两个推送: 12577ef915d(4 默认值)+c07080636c8(2 名字常量)。注册表一致性核实: Go registry 亦无该两 SysVar 条目, Rust catalog 缺席=正确。
- 收据推送 `fb20b01ab78`。本会话 vardef 面批次: 12577ef915d/c07080636c8/fb20b01ab78。
- 下轮恢复点: (1) vardef 深层=Go SysVar 注册表 484 条 vs Rust catalog 971 条的结构差异解释与逐条 scope/type/默认 diff(脚本化, 大面); (2) 或选择下一个无 audit 文档的 Go package 继续遍历; (3) F2 仍需 live 证据。
- sysvar 注册表面批: 脚本 name-set diff(Go defaultSysVars 521+noop 423=944 vs Rust catalog 963, 含 GlobalConfigName/并发 helper 注册的解释) → 真实缺失=2 条: tidb_columnar_storage_enabled(Go sysvar.go:982 Global Bool ON)与 tidb_query_cop_store_limit(Go sysvar.go:2294 Global|Session Unsigned 0..256 def 15), 已补入 distsql_storage.rs(ENTRIES 计数 49→51)+registry 有序不变量回归测试。31 个 Rust-only=fork 扩展。A/B: 280 失败预存一致, 净 +1 通过。fmt/clippy/diff-check PASS。
- 下轮恢复点: (1) sysvar scope/type/default 的逐条值 diff(名字集已闭环); (2) F2/F3-seam 被 live 证据阻塞。
- sysvar 注册表批推送 `4d1b311ab7e`。本轮累计 4 推: 12577ef915d/c07080636c8/fb20b01ab78(vardef)/4d1b311ab7e(registry)+journal 提交。
- 下轮恢复点: (1) sysvar scope/type/default 逐条值 diff(名字集已闭环, 脚本已有, 扩展 scope/value/min/max 三元组即可); (2) F2/F3-seam 被 live 证据阻塞; (3) F4 逐站点低优先级。
- sysvar 属性级第二遍完成: 427 条单行条目 scope/value/type/min/max 全对照, 0 真实分歧(4 条 TypeTime 标记为脚本缺陷, Go sysvar.go:865 确有 TypeTime, Rust VarType::Time 正确)。sysvar 表面(名字+属性)闭环, 已写入 vardef 收据文档。
- 下轮恢复点: (1) 多行 Get/Set hook 条目的逐变量行为审计(不同类); (2) F2 live 阻塞; (3) F4 逐站点低优先级。
- F3 seam 完成: 在保留远端 planner `mysql_code()` tuple 合约的基础上，`tidb-server` single/multi-node prepared read 与 `RealTiKvReadError::Plan` flattening 均传递 Go-compatible code/state；`PreparedBindError` 补充 8112/HY000。逐变体 planner 与 server 回归、fmt/diff-check/make lint 已验证；详见 `rust/testport/receipts/planner_read_only_error_codes.md`。
- 下轮恢复点: (1) F2 ~59 generic unknown 站点仍需 live evidence; (2) F4 后续逐站点 Go errno 对比; (3) 重读 divergence/audit 文档寻找下一 Rust-only parity gap。
- 下轮恢复点: (1) 多行 Get/Set hook 条目的逐变量行为审计(不同类); (2) F2/F3-seam live 阻塞; (3) F4 逐站点低优先级。
- validate_password 耦合校验批(session): SET GLOBAL 的耦合钩子落地—length 低于 number+special+2*mixed 时上调为下限; 任一 count 设置后 length 不足则提升(updatePasswordValidationLength 语义)。回归: 5 步耦合场景(pre-fix 失败已证: "8"≠"12")。session lib 1251 通过/281 预存环境失败(A/B 一致+1 flaky 单独复跑两次通过), fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) hook 审计后续 72-4=约 68 个 Validation 条目逐个对照; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- validate_password 耦合批推送 `56c55cafc84`。
- hook 审计第二片: tidb_read_consistency 白名单(strict/weak 大小写不敏感, 其余 ErrWrongTypeForVar 1232, Go session.go:702)落地 run_validation + read_consistency_whitelist 回归。session lib 1260 通过/279 预存失败(噪声内), fmt/clippy/diff-check/make lint PASS。
- hook 覆盖状态: 75 个 Validation 条目中 validate_password 簇(5)+read_consistency 已移植; 32 个名字在 Rust 校验代码已有分派; 余 ~43 个名字全树有出现但多为常量/读侧引用, SET 校验臂的逐个对照仍开放(已提取 mpp_version/mpp_dml_type 等部分钩子体: dml_type 非 next-gen 下无白名单=无需移植)。
- 下轮恢复点: (1) 余下白名单臂逐个落地(mpp_version 动态版本集/mpp_exchange_compression_mode/runtime_filter_type|mode/tiflash_hashagg_preaggregation_mode/collation_database/character_set_database/init_connect SQL 解析校验); (2) F2/F3-seam live 阻塞。
