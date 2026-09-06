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
- database charset/collation 批: collation_database 并入 checkCollation 臂; character_set_database 新臂(空值 1231/未知 1115/存规范名, Go varsutil.go:76)。session lib 1265 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- mpp_exchange_compression_mode 白名单批: run_validation 臂(ToExchangeCompressionMode 复用 vardef modes 模块, 拒绝信息列选项 NONE/FAST/HIGH_COMPRESSION/UNSPECIFIED, 裸 errors.Errorf→1105=Refused 变体) + 大小写回归。session lib 1267 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) hook 白名单后续: mpp_version(需先移植 kv MppVersion 表)/runtime_filter_type|mode/tiflash_hashagg_preaggregation_mode/init_connect(SQL 解析); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- runtime_filter type/mode 白名单批: type=逗号分隔 IN/MIN_MAX 大小写不敏感(拒绝消息照 Go 原文含 "sepreated" 拼写错误); mode=精确 OFF/LOCAL(大小写敏感)。两条 run_validation 臂 + 回归。session lib 1280 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) hook 白名单后续: mpp_version(需 kv MppVersion 表)/init_connect(SQL 解析)/tiflash_hashagg_preaggregation_mode; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- init_connect 批: run_validation 臂落地—值必须可解析为 SQL(Go sysvar.go:704, 会话模式在本边界以缺省模式代替并注释), 失败=ErrWrongTypeForVar 1232, 空值=零语句通过(同 Go ParseSQL(""))。回归 3 断言。session lib 1283 通过/281 预存(失败集与既有一致), fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) hook 白名单后续: mpp_version(需 kv MppVersion 表)/tiflash_hashagg_preaggregation_mode(核实 Go master 无此变量, 已剔除); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- mpp_version 批: kv MppVersion 表镜像移植到 vardef modes.rs(UNSPECIFIED=-1/V0..V3/newest=3 + to_mpp_version 解析, -1..3 闭区间)+mpp_version run_validation 臂(拒绝消息 "-1 (unspecified), 0, 1, 2, 3")+双侧回归。session lib 1288 通过/281 预存(失败集与既有零新增), vardef 45+3+3, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) hook 余项逐个; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- hook 审计状态收敛: 30 个待分类余项(tidb_replica_read 已核实由 Enum+possible_values 通用校验完整覆盖=部分余项或同此形态)。分类脚本对多形态 Name: 条目(常量引用/字面量/跨行)的适配未完成, 下一轮先修脚本再批量分类, 产出精确工单后逐变量落地。
- 下轮恢复点: (1) 修分类脚本 → 30 项三分桶(Enum 通用已覆盖/警告废弃型/需真移植)→ 逐个落地; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- hook 分类器修复并产出精确工单: 10 需真移植/2 Enum 已覆盖/1 废弃警告/17 格式未解析(多为废弃警告型)。写入 vardef 收据。
- 下轮恢复点: (1) 10 条 NEEDS WORK 逐个落地(mem_arbitrator 4 连+gogc_tuner_threshold+tiflash_pipeline_model+schema_cache_size+opt_index_join_build_v2+pessimistic_txn_fair_locking+tx_read_ts); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- mem_arbitrator 簇批(4 变量): mode 小写白名单 disable/standard/priority; wait_averse 精确 0/1/nolimit; query_reserved 0 或 >1 的整数; soft_limit 0/auto 规范化+其余值透传(字节表未移植, 代码中记录)。拒绝均 1105 Refused。session lib 1292 通过/281 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) 工单余项: gogc_tuner_threshold/max/min、tiflash_pipeline_model、schema_cache_size、opt_index_join_build_v2、pessimistic_txn_fair_locking、tx_read_ts; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- gogc_tuner_threshold 批: 钩子在 Go 于类型归一化前消费原始值(tidbOptFloat64 非法值静默回退默认 0.6; 最短浮点文本存储; 范围守卫为 && 矛盾条件死代码+tuner 运行态比较未启动时为 0 不拒绝)—Rust 在 validate_in_scope 归一化前拦截实现, 3 断言回归(bogus→"0.6"/-5 透传)。tx_read_ts 钩子为空操作已核实关闭。session lib 1297 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) 工单余项: tiflash_pipeline_model/schema_cache_size(opt_index_join_build_v2、pessimistic_txn_fair_locking 待读钩子体); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- index_join_v2 + schema_cache_size 批: 前者 falsy 拒绝(always-enabled 消息 1105)+truthy 规范化为 ON; 后者字节解析+64MB 下限钳/MaxInt64 上限钳(Go 的 1365 警告本边界无 sink, 值钳位保留并注释)+不可解析 1292。回归双场景。session lib 1299 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) 工单余项: tiflash_pipeline_model/pessimistic_txn_fair_locking(条目位置待查); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- 工单最终处置并推送: tiflash_pipeline_model=废弃警告型(值透传, 警告 sink 缺口同 schema_cache 钳位注释); fair_locking 拒绝臂仅 next-gen 生效(惰性)。30 项全部处置: 16 已落地+回归/2 Enum 通用覆盖/1 废弃/1 空操作/1 next-gen 惰性/若干 fork 或不存在。hook 审计关闭。
- 下轮恢复点: (1) 选下一个无 audit 文档的 Go package 遍历; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- F2 静态分类完成并写入 audit: 47 站点四桶(startup 13/事务写 8/点读 prepare 8/完成杂项 18); 事务写簇修复设计=复用 txn.rs TxnErrorKind 映射, 仍需一次冲突捕获钉错误文本签名。
- 下轮恢复点: (1) 若有 live cluster 捕获冲突错误文本→落地事务写簇 9007 路由; (2) 选下一无文档 Go package(候选 tidb-hint 已查结构在位); (3) F4 低优先级。
- hint 面核实(只读): parse_stmt_hints/重复警告/RemoveDuplicatedHints 去重均已在位, 无需新批。
- 树健康基线(六 crate): session 1302 通过/281 预存(环境 spill 类, A/B 已证与批次无关); vardef 45/0; planner 911/0; codec 46/0; distsql 28/0; datatype 410/0。
- 下轮恢复点: (1) hook 余项 30 分类中"needs work"已清 5(mem_arbitrator 4+gogc), 余 5(tiflash_pipeline_model/fair_locking=惰性已记录, schema_cache_size/index_join_v2 已落地, tx_read_ts 空操作)→工单实际清空; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) 选新面: tidb-privilege 或 tidb-domain。
- privilege privs 面首审: 四张 scope 清单(32/19/13/4)元素与顺序全匹配(命名差异为枚举别名); GrantOption 不入 ALL_* 双侧一致。收据 rust/docs/privilege-privs-parity-audit.md。
- 下轮恢复点: (1) privilege 动态权限/password expiry/SET-ROLE 行为面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- privilege 第二遍: DYNAMIC_PRIVS 21=21 精确匹配(含注释剥离后脚本核验), 大小写语义一致; RegisterDynamicPrivilege 插件扩展有意不移植(const 决策已在模块文档)。收据已追加。
- 下轮恢复点: (1) privilege 行为面(SET-ROLE/角色图、password expiry); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- privilege 第三遍: check_password_expired 忠实移植核实(1862/沙箱/lifetime 阶梯全对齐); 记录一个微差异=Go AddDate 日历日 vs Rust 秒算术(DST 边界 ±1h, 修复需 registry 携带时区)。收据已追加。
- 下轮恢复点: (1) privilege 余项: SET-ROLE/角色图; (2) DST 微差异修复(需 registry 时区, 排队); (3) F2/F3-seam live 阻塞; (4) F4 低优先级。
- privilege 第四遍: 角色图核实(BFS 传递闭包/激活直接性/身份顺序/dynamic 授予覆盖与 REVOKE ALL 全删语义均对齐)。SET-ROLE 语句面为唯一未审切片。
- 下轮恢复点: (1) SET-ROLE 语句语义面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- privilege 审计正式关闭: SET ROLE 五种 selection/3530 门/拒绝保留旧集/bypass 与 SET DEFAULT ROLE 授权门均在位(无需代码改动), 推后续提交。
- 下轮恢复点: (1) 下一无文档面候选: tidb-domain/tidb-config 深层; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 微差异(需 registry 时区, 排队)。
- config 默认值面首审: 69 顶层字段零真实分歧(38 值级匹配+21 拼写/嵌套归属核实), 收据 rust/docs/config-defaults-parity-audit.md。
- 下轮恢复点: (1) config 嵌套段默认值(TiKVClient/PDClient/内存限制)表 diff; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- config 嵌套段第二遍: PessimisticTxn/TrxSummary/Performance 逐字段全一致(txn 尺寸限制 6MB/100MB 经 config.go:64-66 核实); TiKVClient/PDClient 属 client-go 外部谱系(由 third_party 重同步流程负责), 不在本审计声明内。收据已追加。
- 下轮恢复点: (1) 新面候选: tidb-domain infoschema 面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- charset 注册表面首审: 7=7 字符集、maxlen 与默认 collation(含 gbk/gb18030 的 new-collation 条件)全匹配; Go CharsetIDs 约 260 项 legacy 表属 fork 范围边界, 记录不修。收据 rust/docs/charset-registry-parity-audit.md。
- 下轮恢复点: (1) collation id 表与 CharsetIDs legacy 范围决策复核; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- collation id 表第二遍: Go 223 项 id→name 在 Rust 表中零错名; 50 个 Rust-only id 全部来自 Go charset.go 描述符超集(76/250/0900 族/256+ 动态段), 回退 46 同 Go DefaultCollationID。收据已追加。
- 下轮恢复点: (1) 新面候选继续(tidb-domain); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- charset 数据新鲜度校验: generate-parser-charset.py 重跑零 diff(生成层与 Go master 字节同步); 收据措辞修正=宽 MySQL 列表由 known_charsets.rs 生成镜像承载, 非"范围边界"。
- 下轮恢复点: (1) tidb-domain 面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain 面开篇: 13 个 Go 文件全部有 Rust 镜像模块; sysvar_cache 切片核实(6 函数语义+SetGlobal 前写全局视图的顺序细节均在位)。收据 rust/docs/domain-sysvar-cache-parity-audit.md。
- 下轮恢复点: (1) domain 余模块逐个行为审计(schema_checker/ru_stats/plan_replayer 优先); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain schema_checker 切片核实(含 8028/8027 错误对与 ResultFail 带变更的 Go quirk 文档化; 10 测试全绿), 推后续提交。
- 下轮恢复点: (1) domain 余模块: ru_stats/plan_replayer/historical_stats/topn_slow_query/serverinfo_syncer; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain ru_stats 切片核实(函数级全覆盖+时间桶数学含 DST UTC 往返/除零 panic 对齐), 推后续提交。
- 下轮恢复点: (1) domain 余模块: plan_replayer/historical_stats/topn_slow_query/serverinfo_syncer; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain plan_replayer 切片核实(31 函数全映射: GC/状态记录/handle SendTask/collector/占用键纪律, trait 注入 FS 与 SQL 效应), 推后续提交。
- 下轮恢复点: (1) domain 余模块: historical_stats/topn_slow_query/serverinfo_syncer; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain historical_stats + topn_slow_query 切片核实(堆算术/partition 决定 is_partition 的查找序/通道满丢弃语义均在位), 推后续提交。
- 下轮恢复点: (1) domain 末模块 serverinfo_syncer; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- domain serverinfo_syncer 切片核实(73 函数: info 三型+clone/marshal/topology/syncer 会话与存储/endpoint claim 认领全映射), 推后续提交。domain 面全部模块处置完毕。
- 下轮恢复点: (1) 新面候选: tidb-domain 已闭环→选 tidb-privilege 已闭环→候选 tidb-config 深层已做→下一候选 tidb-kvcache/tidb-hash 小工具面或 infoschema 面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- kvcache+hash 双面收据(rust/docs/kvcache-hash-parity-audit.md): Put 淘汰循环逐分支对齐(内存重采样规则/探测失败 DeleteAll/quota-0 单次淘汰); IHasher 契约镜像。
- 下轮恢复点: (1) 新面候选继续; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- fallback+skip_column_types 白名单批: allow_fallback_to_tikv 只接受 tiflash token(trim/去重按 store type/首现顺序, 任意非 tiflash token=1231); analyze_skip_column_types 小写白名单七类型(json/text/mediumtext/longtext/blob/mediumblob/longblob, 规范化存储, 1231 拒绝)。session lib 1326 通过/280 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) super_read_only 耦合(vars.rs 级, 需兄弟 global 读+StmtType 判定); (2) hook 余项核实(17 未解析条目); (3) F2/F3-seam live 阻塞; (4) F4 低优先级。
- 事故记录与恢复: 裸 `git stash pop` 弹出了共享 stash 列表中并发会话的 "codex-planner-read-only-seam"(stash 跨 worktree 共享!)→ 6 文件 UU 冲突。已用 checkout HEAD 回滚该误应用(其 stash@{0} 条目完整保留, 并发会话工作无损失), 本分支回到干净状态。教训: 永远不裸 pop, pop 必须显式 ref 且核对描述; env 补丁改为手动单行重放。
- 本轮批次: super_read_only 耦合+overflow 过期钉子刷新(2cb24c6f957 谱系, 终点 f0ec16f6145)。session lib 1331 通过/280 预存。
- 下轮恢复点: (1) 17 未解析 hook 条目逐个核实; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- max_dist_task_nodes + evolve_plan_baselines 白名单批: 0 节点数拒绝(消息 "-1 or [1, 128]"); evolve ON 拒绝(Cannot enable baseline evolution, 测试旋钮默认 false)。gogc max/min 定性为运行态耦合(gctuner 原子量)归 deferred。session lib 1337 通过/279 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) tx_isolation_one_shot(checkIsolationLevel); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- tx_isolation_one_shot 定性 deferred(checkIsolationLevel 需读会话 skip-check 姊妹值, 验证分派无会话上下文参数——签名穿线改动); exchange_partition/tiflash_read_for_write_stmt 关闭为警告无 sink 型(值透传一致)。
- 下轮恢复点: (1) 验证分派会话上下文穿线设计(解锁 tx_isolation_one_shot + gogc max/min); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- tx_isolation_one_shot 批: 验证分派会话上下文穿线落地(validate_in_scope_with_lookup + run_validation_with_lookup, Option<lookup>), vars.rs write 传 self.get 闭包; SERIALIZABLE/READ-UNCOMMITTED 拒绝 8048 除非 skip-check ON(警告无 sink 已注释)。回归 3 断言。session lib 1340 通过/279 预存, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) gogc max/min 仍待 gctuner 运行面(不随本穿线解锁); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- charset 收据精化: Go 自身双表(CharsetNameToID wire id vs CharacterSetInfos 描述符默认 collation)对 6 字符集不一致(latin1 47 vs 8 等); Rust 生成表镜像描述符侧, wire 路径从列 collation 推 id(更细粒度)。非待修分歧。
- 下轮恢复点: (1) infoschema/privilege 行为面; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 微差异排队。
- allow_fallback_to_tikv 臂收敛核实: 现行树单臂(并发会话版本: trim/去重/非 tiflash 拒绝, 与我批语义一致且更严格拒绝空 token); 我的回归测试对着统一臂全绿。无重复臂。
- 下轮恢复点: (1) 新面候选 infoschema(大, 需立项); (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 微差异排队。
- infoschema 面立项: 架构映射收据 rust/docs/infoschema-parity-audit.md(16 文件/15.4k 行 → 按职责分布映射表+三项范围决策+切片顺序 a/b/c)。
- 下轮恢复点: (1) 切片 a: DDL reload 版本语义(catalog_reload/catalog_watch vs builder.go); (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- infoschema 切片 a 核实: DDL reload 版本语义(单快照/7 动作靶向补丁/其余全量回退=Go applyDefaultAction 等价/版本间隙阈值)全对齐, 收据已追加。
- 下轮恢复点: (1) 切片 b cluster-table plumbing; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- infoschema 切片 b 定性: cluster.go 属 CLUSTER 内存表面(节点不暴露, by-design); Rust cluster_catalog.rs=meta 持久化加载(切片 a 已核)。无代码改动。
- 下轮恢复点: (1) 切片 c bundle builder; (2) F2/F3-seam live 阻塞; (3) F4 低优先级。
- infoschema 切片 c 定性: bundleInfoBuilder 的增量 delta 机制被架构吸收(reload 全量回退重派生 + placement_delivery 直发 PD)。切片 a/b/c 全部闭环, infoschema 面收据完整。
- 下轮恢复点: (1) 新面候选继续扫描; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 微差异排队。
- privilege 第四遍: password policy 簇(MEDIUM 分类链/读取顺序/消息措辞 + LOW/STRONG + ValidatePassword 分派)忠实移植核实, 收据已追加。
- 下轮恢复点: (1) 新面候选或文档重扫; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 排队。
- privilege 第五遍: SHOW GRANTS 导出面核实(export.rs 行形状+打印名走查+GrantOption 后缀; grants 测试簇 100/0), 收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 排队。
- sql_mode 位表核验: 33 个 mode 位位置逐一一致(1<<iota 同构), 组合模式展开在位。收据追加 charset-registry 文档。
- 下轮恢复点: (1) 新面候选或 privs 深层; (2) F2/F3-seam live 阻塞; (3) F4 低优先级; (4) DST 排队。
- F4 后续定案关闭: generic-1105 站点均无 Go 可对应 errno(AUTO_RANDOM_BASE 溢出 Go 静默回绕/AUTO_INCREMENT 非整数属语法层/前缀键等已带 1089/1170), 1105 是边界拒绝的诚实代码。收据已追加。
- 下轮恢复点: (1) 新面候选; (2) F2/F3-seam live 阻塞; (3) DST 排队。
- 合流核实: 并发会话已在同一工单落地他们的版本(gogc tuner bounds/tiflash preaggregation/analyze column options/partition prune hooks/super_read_only 耦合+restricted 联动), 与我的批次语义一致或更完整; 注册表计数测试过期(971→973)已修; fmt 漂移归一化。session lib 1348 通过/281 预存。
- 下轮恢复点: (1) 继续跟随并发会话在同一工单的增量(冲突最小化); (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- parser-lexer 文档 Unverified 段退役: 工具链恢复后原 worked examples 已全部成为树内测试钉子(parser_root_source.rs 的链式拒绝/pipes_as_concat ring 等), 批次推送。
- 下轮恢复点: (1) infoschema 切片 b 已闭→下一行为面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- funcdep 邻接面核实: FDSet API 全映射(closure 族/条件 FD/等价/常量/null 化/笛卡尔/AddFrom/唯一 id 注册), 18 模块回归+planner join 规则测试全绿; 边蕴含算法的行级深读列为条件性后续。收据已追加。
- 下轮恢复点: (1) infoschema 切片 b/c 已闭; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- init_connect 执行缺口记录: 变量定义/SET 校验/回归齐备, 但连接建立时无执行路径(Go 对每个非 root 新连接执行, 失败拒连)——feature 级工单, 已写入 vardef 收据。
- 下轮恢复点: (1) init_connect 执行面(连接后置钩子)立项或继续他面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- init_connect 立项推进: Go 语义(conn.go:1114-1157)与 Rust 三缝点(pipeline execute/has_dynamic_priv_with_roles/get_global)全部提取并写入收据, 实现面已完备可执行。
- 下轮恢复点: (1) 按 DESIGN 在握手完成点接线 initConnect 执行; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- init_connect 执行面定性更新: 连接层仅有 auth 状态机+执行管线脚手架, **逐连接 run loop 尚未建成**→钩子点属未来基础设施, 该缺口与连接循环一起落地(收据已更新)。
- 下轮恢复点: (1) 选与连接循环无关的新面(如 perfschema/metrics 表面或 planner 行为面); (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- funcdep 算法深读完成: implies/add_functional_dependency/reduce_cols/add_constants/add_equivalence_closure 五个核心与 Go fd_graph.go 行级等价(替换-vs-丢弃纪律/lax-lax 特例/跳过新边迭代界/等价常量传播/not-null 继承)。条件性后续关闭, 收据已追加。
- 下轮恢复点: (1) 新面选型或文档重扫; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- parser 差分环核验: 51,598 集成 fixture 全量回放——51,499 接受匹配+99 拒绝一致+0 恢复不匹配+0 不对称=解析器与 Go golden 全语料一致。写入 parser-lexer 文档。
- 下轮恢复点: (1) 新面候选继续; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- metrics schema readers 切片核实(gen_prom_ql/gen_label_condition/gen_label_condition_values 与 Go metrics_schema.go 逐行一致), 收据已追加。
- 下轮恢复点: (1) 新面选型或既有 slice 深读; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- D11 残余边界评估: INSERT/REPLACE/LOAD DATA info 字段生产者为 rank-4(无主流客户端读 terminal EOF 的这些字段), packet 编码器部分已由并发会话修复; 列为连接循环落地时的伴随工单。
- 开放项总台账(全部阻塞于外部输入或大面决策): F2 事务写簇 9007 路由(需冲突文本捕获)/F3-seam(需连接循环)/DST 微差异(需 registry 时区)/D11 info 生产者(需 engine-trait 扩展)/partition 裁剪(需用户对照查询)/tpcds(需 dsdgen)/#202、CHAR coercibility(需架构决策)。可自主执行的机械对照与行为核验均已闭环。
- 竞写事故收敛: 我与兄弟会话在同一 exec.rs 修复上并行提交, rebase 冲突后按"远端已在"原则 reset 取其树(faa39df4ee4, 语义相同); 冗余本地提交丢弃, env 单行补丁重放, executor 编译恢复。教训补充: push 前 rebase 遇"Could not apply"时先核对是否与远端内容重复, 重复则 reset 而非手工解决。
- 下轮恢复点: (1) 新面选型或跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 收敛核查(mererge 后): 兄弟会话 random_bytes 修复形态=.to_string()(与我丢弃的 .to_owned() 等价); executor lib 在合并树 1077 通过/123 预存环境失败(基线 ±1 flake), 编译恢复确认。
- 下轮恢复点: (1) 静默期→新面候选(infoschema 深层/其他 crate)或收敛复查; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- funcdep 面收尾: funcdep_misc.go 三助手定位为 functional_dependencies.rs 的 add_not_null/constant/equivalence_facts(not-null 逐列 null-reject 测试与 Go 一致); 16 FD 提取回归+914 planner 全绿。funcdep 面(fd_graph+misc)全覆盖, 收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- memory 工具面核验: mem_total(60s 缓存)/mem_used(500ms 缓存)与 Go meminfo.go 一致; 模块结构(action/arbitrator/pool/tracker/membuf/systimemon)镜像 Go 包布局。收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- memory tracker 切片核实: Tracker 全 API 镜像(限制/动作栈/挂接分离/消费/标签 + arbitrator 集成扩展), 20 模块回归全绿。收据已追加。
- 下轮恢复点: (1) arbitrator/pool 深读或新面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- pool+action 切片核实: ResourcePool 41 函数(超集含 arbitrator 集成) 25 测试全绿; ActionOnExceed 契约镜像 12 测试全绿。收据已追加。
- 下轮恢复点: (1) arbitrator 深读或新面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- memory arbitrator 面核实: 端到端 full_flow 测试(Go TestMemArbitrator 移植)驱动整条仲裁管线, 4 测试全绿; memory 面(meminfo/tracker/pool/action/arbitrator)全部闭环, 收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- gcutil 面核实: 六函数逐一对齐(validate_snapshot 拒旧快照+ErrSnapshotTooOld 渲染/get_gc_safe_point 读 mysql.tidb); crate 无测试=记录为首个可补项。收据已追加。
- 下轮恢复点: (1) gcutil snapshot 校验回归测试(可自足); (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- gcutil 快照校验回归批: 2 测试落地(mock Context 驱动 restricted-SQL 与全局读取)——过期快照拒绝+渲染时间断言/新快照通过/读失败透传/CheckGCEnable ON-OFF 表值驱动。gcutil 2/0, fmt/clippy/diff-check/make lint PASS。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- errmsg 面核实: extend 与 Go Extend 逐行一致(nil 安全/首个匹配/后缀拼接去尾点), 5 集成测试全绿, 收据已追加。
- 下轮恢复点: (1) placement/infoschema 深层或新面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- placement rules 面处置: 由 DDL 会话活跃持有(17 模块测试+bundle 投递回归在位), 避免重复审计; 其收据归属 DDL 会话。
- 下轮恢复点: (1) 新面选型(privilege 余项/其他 crate); (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- plan inventory 环核验: explain=4674/explain_analyze=49/total=4723, inventory current 测试通过(--check 模式确认源清单同步)。
- 下轮恢复点: (1) privilege 余项或其他 crate 面; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- redact 面核实: MARKER/OFF/ON 模式(含内部 marker 双写)、NeedRedact、Value("?")、DeRedact/File 日志后处理全镜像; 4 单测+5 planner 回归全绿。收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- SEM 面核实: v1 门+sem_v2(config/sql_rule/restricted_hint+六个不可见/受限谓词)全镜像, 18 测试全绿——noop-gated sysvar 臂所依赖的门面。收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- collate charset.go 补记: switchDefaultCollation 的行为由 Rust 条件式 default_collation 架构性吸收(变异 vs 计算), 可观察默认 collation 在两种状态下均一致。收据已追加。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- planner 4 失败诊断(cached_plan_rebuilds_*): rebase 并入的 ranger/cache-key 增量使重载后 range_is_safe 拒绝 → UnsafeRange{plan_id}; 候选引入点=34d7549bb06(LIKE escape 入 plan scope)或后续 ranger 改动。归属 DDL/planner 会话(活跃区), 已记录待其修复; 深挖二分属其领域。
- 下轮恢复点: (1) 兄弟会话修复 4 测试; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队; (5) session lib +36 失败同批溯源。
- tests_analyze 5 失败 A/B 定案: stash sysvar.rs 后失败依旧→与我的 sysvar.rs 无关, 属兄弟会话 stats/analyze 在途区(estimates 1.00 vs 7.00 = 伪统计回退)。已通知性记录, 修复归其 owner。
- 下轮恢复点: (1) 跟随兄弟会话 analyze/stats 修复; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- tests_analyze 根因定位(推 173e6fbc57b 后): a9c77f181fd(statistics: reconcile analyze metadata after sampling)将 analyze 版本标记改为采样快照 TSO 且把替换推迟到"真实集群边界"——有界会话测试路径无该边界, 存储的统计未被会话识别→伪统计回退→estimates 1.00。修复归其 owner: 在会话测试路径补齐版本替换或调整快照语义。
- 下轮恢复点: (1) 跟随兄弟会话修复; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- UnsafeRange 引入点收窄: d5d32d8a49c 为纯重构无害; 与 34d7549bb06(LIKE escape 入 cache keys)的交互最可疑——escape 字节进入 cache key 后, 重载路径对 [Datum::Int(42)] 参数的 range 重算结果与原树不再 range_is_safe 一致。归兄弟会话。
- 下轮恢复点: (1) 跟随兄弟会话修复; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- UnsafeRange 探针结果: 重载路径 rebuilt_ranges=0(detacher 对 eq(col,Int(42)) + common-handle 产生空集) — 其余条件(used=1/access=1/remained=0)正常。空集→range_is_safe false→UnsafeRange。归兄弟会话 ranger 重构。
- 下轮恢复点: (1) 跟随兄弟会话修复; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 当前树状态: executor 1070/138(基线漂移: 预存集从 122 漂至 138, 因兄弟会话 DDL 增量持续合入); exec.rs random_bytes 编译修复在位; 树编译干净。
- 下轮恢复点: (1) 新面或跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至 f2d23b20e1f(ddl: validate sequence identifiers + types: preserve numeric set truncation events); 关键套件全绿(datatype 412/0, super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0)。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- executor 138 失败细分: 驱动簇需运行存储(环境); column_default 的 DST 测试(2011-03-13 02:30 = 美东弹簧前进)仅在观察美 DST 的时区通过——本机时区决定性, 属测试环境依赖而非代码分歧。
- 下轮恢复点: (1) 跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量同步验证: 兄弟会话最新批次(ddl: add column if-not-exists / sequence cache bounds)合入后, 关键套件全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, datatype 412/0, executor 1069 通过/139 预存)。所有已落地面在新增量下稳定。
- 下轮恢复点: (1) 新面选型或跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至 4272cdfa565(pkg/ddl: enforce sequence create privilege); 全关键面稳定(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0); planner 4 UnsafeRange 仍为兄弟会话在途区。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 22-crate 最终聚合核验: session 1321+/281 预存环境; vardef 45/0; executor 1069+/139 预存环境; codec 46/0; datatype 412/0; planner 920+/4 UnsafeRange 预存; distsql 29/0; chunk 242+/35 预存环境; expr 1195+/1 已知网络 flaky; funcdep 18/0; domain 143/0; gcutil 2/0; hint 0/0; kvcache 0/0; hash 0/0; errmsg 0/0; config 81/0; placement 24/0; br 31/0; allocator-stats 0/0; hack 4/0; util 539/0。全部失败均为预存环境/兄弟在途, 零新增。
- 下轮恢复点: (1) 跟随兄弟会话增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量同步验证: ddl foreign-key reference errors 合入后全关键面稳定(datatype 重跑 412/0 确认前次 5 失败为并行 flake)。
- 下轮恢复点: (1) 新面选型或跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量同步: 兄弟会话继续 DDL 对齐(foreign key compatibility/auto random alter/JSON selectivity skip/hidden expression-index column skip), 均不在我的审计范围。树稳定。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- F8 更新: overflow message 现在包含限定表达式(Go 形式), 测试钉住; 审计文档标记 FIXED。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量同步验证: 兄弟会话 DDL foreign key on partitioned tables 拒绝臂合入后全关键面稳定(super_read_only 1/0, datatype 412/0, executor 1071 通过/139 预存)。
- 下轮恢复点: (1) 新面选型或跟随增量; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量同步: 兄弟会话持续推入 DDL 对齐批次(sequence create privilege/generated column checks/if-not-exists 等), 引入 333 失败(session lib), 全部归其活跃区。我的批次全绿(所有回归测试通过)。
- 下轮恢复点: (1) 跟随兄弟会话修复; (2) 新面选型; (3) F2/F3-seam live 阻塞; (4) F4 已闭; (5) DST 排队。
- 增量收敛: session lib 334 失败(环境+兄弟在途), 其中 1 个 flake 轮换。全关键面稳定。
- 下轮恢复点: (1) 跟随兄弟会话修复; (2) 新面选型; (3) F2/F3-seam live 阻塞; (4) F4 已闭; (5) DST 排队。
- 增量收敛核查: 树同步至最新, 关键面稳定(session 1321 通过/332 环境集, planner 920 通过/4 UnsafeRange 预存, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0); planner 4 UnsafeRange 维持。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0, util 539/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 增量收敛核查: 同步至最新, 关键面全绿(super_read_only 1/0, tx_isolation_one_shot 1/0, gcutil 2/0, datatype 412/0)。无新增分歧。
- 下轮恢复点: (1) 新面选型; (2) F2/F3-seam live 阻塞; (3) F4 已闭; (4) DST 排队。
- 新面批次: tidb-timer (Go pkg/timer @ a85e0fd5df) 完整对照审计 (api/tablestore/runtime/metrics 三路并行)。修复 5 项 behavior-breaking:
  (1) worker.rs VersionNotMatch 后 GetByID err 重赋值 (Go worker.go:349-359, 删除计时器不再无限重试);
  (2) mod.rs start() 锁跨 check+init 恢复 Go Start 原子性;
  (3) cron.rs 四项 robfig 对齐: 阶梯星号清 STAR_BIT (dom/dow OR 规则恢复)、descriptor 区分大小写、@every 亚秒截断、空逗号段跳过 + getRange 校验顺序/文案逐字对齐;
  (4) mem_store.rs overflow sender 观察 closed 标志, close() 不再挂死;
  (5) TimerExt::unmarshal 严格化 (json.UnmarshalTypeError 语义, 损坏 TIMER_EXT 使 List 报错而非静默半记录)。
  验收: cargo test -p tidb-timer 16 lib + 49 int 全绿 (8 cron 单测含 5 个新回归 + test_timer_ext_unmarshal_strict_like_go); fmt; git diff --check; make lint 过。
  收据: rust/docs/timer-package-parity-audit.md。commit 701ec0194cd 已推 origin/hparser-integration (55b0e217e68..701ec0194cd)。
  接受的 narrowing (已记录收据): TZ=/CRON_TZ= 前缀拒绝 (TiDB 不写)、prometheus metrics 面以 atomics 替代、日志字段收窄、json U+2028 转义/数字宽松度、notifier uuid 格式、panic 路径健壮性。
  注意: dbsid/hparser-integration 现含兄弟会话 2 个独立 commit (e62a9972080/42e53f1e40c, tpch+session 接线, 基于旧 merge base 7306a715bd2), 与 origin 谱系分叉 3000 commit —— 未强推 dbsid 以免破坏兄弟工作, dbsid 同步暂挂, 需协调。
- 下轮恢复点: (1) tidb-naming/tidb-schemaver 小面核查; (2) 兄弟增量收敛核查; (3) F2/F3-seam live 阻塞; (4) DST 排队; (5) dbsid 分叉待协调。
- 新面批次 2: tidb-schemaver (Go pkg/ddl/schemaver @ a85e0fd5df) 全文件审计 + 传输面核查。修复 2 项:
  (1) Session keepalive 弹性: 单次 lease_keep_alive_once 失败不再立即关 done (clientv3 内部重试直到租约真亡), 连续失败满一个 SESSION_TTL (90s) 才关; ctx 结束仍即时关闭。回归测试 session_survives_transient_keepalive_failures。
  (2) EtcdWatchOps::watch 增 require_leader 旗标 (Go syncer.go:519 WithRequireLeader 仅用于 job 镜像 watch): job watch 传 true, 全局版本 watch 传 false; 生产 adapter 记录 etcd-client crate 暂无 gRPC metadata 钩子可落实。
  超时面核实结论 (审计分歧 2): pd-client KV worker 所有命令经 across_endpoints(..., timeout, ...) 有界, 无挂死风险; 常量选择留给 server 接线时传 KEY_OP_DEFAULT_TIMEOUT (2s)。
  验收: cargo test -p tidb-schemaver --all-targets 9/9; cargo build -p tidb-server 干净; fmt; git diff --check; make lint 本会话已过 (Go 面未动)。收据 rust/docs/schemaver-parity-audit.md。
  推送: 0ea2f081fb4..2fbeaf07820 origin/hparser-integration。附带核验 tidb-naming (与 Go naming.go 逐字一致含 regexp 边界, 无需改动)。
  dbsid 分叉维持暂挂 (见上条)。
- 下轮恢复点: (1) 新面候选: tidb-ttl/pkg-disttask/dxf 大面, 或 infoschema 深层; (2) 兄弟增量收敛核查; (3) F2/F3-seam live 阻塞; (4) DST 排队; (5) dbsid 分叉待协调。
- 新面批次 3: tidb-stmtsummary (Go pkg/util/stmtsummary + v2 @ a85e0fd5df) 全包审计。修复 1 项 behavior-breaking:
  当前 STATEMENTS_SUMMARY_EVICTED 汇总行在区间轮转后仍暴露 (Go reader.go:214-220 懒过期语义) —— get_stmt_evicted_other_row 增加 begin_time 过滤。pre-fix 失败基线: 移植 Go 回归 TestCurrentRowsExcludePreviousIntervalEvictedOther 先失败 (暴露上轮汇总行) 后通过。52 测试全绿; fmt; diff-check; make lint 过。
  收据 rust/docs/stmtsummary-parity-audit.md。推送 40e4683e9a6..946f43d3fbb origin/hparser-integration。
  开放项 (feature 级, 未认领): (1) v2/reader.go 951 行未移植 (MemReader/HistoryReader/持久日志扫描, persistent 模式无读路径, 见 src/lib.rs 与 src/v2/mod.rs 头注); (2) v2/logger.go 轮转未移植 (FileStmtLogWriter append-only, file_max_size/days/backups 空挂, persistent 模式应保持关闭)。其余 narrowing (UTC 时区渲染/UTF-8 截断边界/proxy 饱和转换) 已记录收据。
- 下轮恢复点: (1) stmtsummary 开放项 v2 reader/logger 移植 (大批次); (2) 新面候选 tidb-ttl/dxf; (3) 兄弟增量收敛核查; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 大批次: v2/reader.go (951 行) 全文件移植完成 (rust/crates/tidb-stmtsummary/src/v2/reader.rs, Go @ a85e0fd5df)。
  内容: MemReader (窗口内存读 + evicted 汇总行)、HistoryReader + scan/parse 流水线 (无缓冲文件派发/concurrent-2 扫描转解析/monitor 错误通道)、stmtChecker (digest/priv/time-range)、stmtFile/stmtFiles 钉住活动 inode 的轮转去重、持久化记录 JSON 解析 (与 record 序列化字段名一致 + encoding/json 宽松度)、全部 9 个 Go v2/reader_test.go 回归移植。
  移植中被测试逼出的两个关键差异: (1) Go close(channel) 显式关闭 vs Rust 丢 sender 关闭 —— scan worker 转 parse 前必须 drop lines sender, 否则死锁; (2) os.DirEntry.Info 惰性解析 —— walk 时才查 metadata, 注入失败可模拟。parseEndTs 保留 Go 的 base-name 前缀 quirk (仅相对路径 config 可解析轮转文件时间戳)。
  验收: cargo test -p tidb-stmtsummary --all-targets 61 lib 全绿; fmt; diff-check; make lint 过 (Cargo.lock 随 tempfile dev-dep 入库)。收据更新 stmtsummary-parity-audit.md。推送 401c5035cf1..0685f495eb9。
  v2 包剩余缺口: logger.go 轮转 (FileStmtLogWriter append-only), persistent 模式保持关闭。
- 下轮恢复点: (1) v2/logger.go 轮转移植 (中批次); (2) 新面 tidb-ttl/dxf; (3) 兄弟增量收敛核查; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 中批次: v2/logger.go 文件 sink 移植完成 (RotatingFileLogWriter, Go @ a85e0fd5df)。
  new_stmt_summary 接线点替换 append-only FileStmtLogWriter, 镜像 pingcap/log 的 lumberjack sink: file_max_size (MB) 超限轮转、备份名 <base>-<本地时间戳><ext> 与 v2 reader parseEndTs 完全互操作 (跨模块测试验证)、file_max_backups 计数 + file_max_days 年龄双维清理 (0 关闭)。
  调试要点: (1) chrono 的 %.3f 仅格式化, 解析需 %.f (NaiveDateTime::from_str 解析不了 11-05-56 这种 dash 时间, 默认值 1970 导致新备份被误剪); (2) prune 前缀取 file_stem 而非 rsplit('-') (时间戳内含 dash); (3) keep_count 用 usize::MAX 会在 index+keep 溢出; (4) 同毫秒轮转同名备份 rename 覆盖 (lumberjack 同样行为), 测试隔 2ms 写入。
  验收: cargo test -p tidb-stmtsummary --lib 64 稳定全绿 (3 次连跑); fmt; diff-check; make lint 过。收据更新。推送 f176dfecaa2..828880055f1。
  v2 包状态: 5 个 Go 生产文件中 4 个全移植 + logger.go 缩减为 StmtLogWriter trait 边界 (zap core/ecnodeer 为生态机器)。
- 下轮恢复点: (1) 新面 tidb-ttl/dxf; (2) 兄弟增量收敛核查; (3) F2/F3-seam live 阻塞; (4) DST 排队; (5) dbsid 分叉待协调。
- 增量收敛核查: 同步至 55bf4a80d41 (兄弟 serverstate 构造器契约 + init-stats returns), 相关面全绿: ddl-serverstate 7/0, stats-handle-initstats 1/0, schemaver 9/0, timer 16/0。无新增分歧。
- 新面批次: tidb-ttl (Go pkg/ttl 的 cache/sqlbuilder/session 三子包 @ a85e0fd5df) 全文件审计。移植质量极高, 无 behavior 修复需求:
  (1) 字符串键列非 UTF-8 数据: Go 写原始字节进 SQL, 本 crate SQL 面全 &str, lossy 转换会构造错误 DELETE —— 定为文档化边界, build 报错 (站点注释);
  (2) write_value_expr 非数值臂经 writeDatum 路由不可达 (bit/blob/binary-string 先走 hex), 站点注释;
  (3) unsigned_edge fallback 替代 Go GetInt64 panic 路径, 注释。cache/mod.rs 陈旧边界声明更正 (Update 方法已按 trait 移植)。
  匹配面: table.go 全分裂族/解码器/EvalExpireTime, task/ttlstatus 全 SQL 逐字节, sqlbuilder 状态机/ScanQueryGenerator, session 事务序 —— 详见收据。
  验收: cargo test -p tidb-ttl 32 全绿; fmt; diff-check; make lint 过。收据 rust/docs/ttl-parity-audit.md。
  ttl 剩余未认领子包: ttlworker/client/metrics (worker 运行时需 executor seam, 与 F3-seam 同源阻塞)。
- 下轮恢复点: (1) 新面候选或跟随兄弟增量; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-resourcemanager (Go pkg/resourcemanager 全包 @ a85e0fd5df) 全文件审计。结论: 零 behavior 分歧。
  匹配面: 单例生命周期/调度链 (DistTask 跳过/Hold/Downclock guard/200ms 门限/±1 tune+超频上限)、spool Tune 顺序与阻塞准入 (5ms sleep + LIFO 等待计数)、workerpool panic fallback 文本/首错 CAS/Tune WaitGroup/Release 顺序、8-shard pool manager 与 shard 序迭代、CPU scheduler 阈值 (<0.5/0.7)、prometheus 指标名 (tidb_rm_pool_concurrency{type}, tidb_rm_ema_cpu_usage), pkg/util/cpu 依赖面由 tidb_util::cpu 全覆盖 (无静默丢弃)。
  落地 4 条站点注释: worker 通道 clone-vs-live-read 契约、iterator 零时种子、构造器 option 可见性顺序、lib.rs 头注 CPU 面已全移植。
  验收: cargo test -p tidb-resourcemanager 14 全绿; fmt; diff-check; make lint 过。收据 rust/docs/resourcemanager-parity-audit.md。
- 下轮恢复点: (1) 新面候选 (tidb-syssession/tidb-hint/infoschema 深层) 或跟随兄弟增量; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-syssession (Go pkg/session/syssession @ a85e0fd5df) 全文件审计。无生产 behavior 分歧, 3 项精修落地:
  (1) WithForceBlockGCSession 注册循环在 tidb_util::intest::IN_TEST 下早退, 镜像 Go pool.go:304-312 的 intest.InTest break (ForceBlockGCInTest failpoint 无对应钩子, 恒 false; 生产行为不变);
  (2) TransferOwner/EnterOperation 的 owner-check 错误补 Go objectStr 身份后缀 (caller: Owner(id), owner: Owner(id)/<nil>);
  (3) txn_valid 源错误 stringify 注释为 crate 错误边界。
  匹配面: 池容量归一化/Get-Put 全序/CloseUnlessReturned≡returned defer/WithSession/WithForceBlockGCSession/TransferOwner 全卫语句/EnterOperation 线程不安全竞态拒绝/inUse 记账/panic→avoidReuse/测试钩子含 Go 的 "ResetSctxForTestcaller" 拼接 quirk。Rust-only: Session::clone 复制 owner id (Go 不可复制指针禁止第二代理, Go 形调用模式不可达)。
  验收: cargo test -p tidb-syssession --lib 14 稳定全绿 (3 连跑); fmt; diff-check; make lint 过。收据 rust/docs/syssession-parity-audit.md。推送 4879b0a2a4a..a1c425a50df。
- 下轮恢复点: (1) 新面候选 (tidb-hint/infoschema 深层/剩余小 crate); (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-hint (Go pkg/util/hint @ a85e0fd5df) 全文件审计。修复 3 项 behavior 分歧:
  (1) NTH_PLAN 先赋值后钳 -1 (hint.go:521-525), NTH_PLAN(0) 不再留下 enabled 的 0 (task_map_need_backup 不再误判);
  (2) HYPO_INDEX db key 小写化 (DBName.L, hint.go:364), checker 输入与 hypo map key 一致;
  (3) 空 qb_name() 不占槽 (Go len(qbName)==0), 后续命名 hint 无伪告警。
  有意分歧 (站点注释): contains_table_hint 大小写不敏感 (Go 原始大小写比较漏掉大写 USE_PLAN_CACHE, 本 crate 解析即规范化大写, 保持宽松使各写法生效); NO_INDEX_LOOKUP_PUSHDOWN 空参跳过替代 Go panic; fill_default_database 规范化 LEADING 树 (Go 只填扁平副本)。开放 modeling 项: READ_FROM_STORAGE 单 hint 双引擎组 vs Go 每引擎组一个 hint (restore 文本与去重 key 不同, 需 parser/ast 改动)。
  新增 2 回归 (nth_plan_zero_clamps_to_disabled / hypo_index_database_key_is_lowercased); tidb-session 257 失败为共享分支预存集 (与本批无关, 双向对比一致); fmt; diff-check; make lint 过。收据 rust/docs/hint-parity-audit.md。
  事故记录: (a) wip 变更经共享 stash 竞态丢失一次, 全量重打 (stash push/pop 必须显式 ref 且立即验证); (b) 兄弟会话开始在 /tmp/tidb-zcode-parity worktree 内活跃编辑 (planner/executor/stmt_ctx 未提交变更 + WALK_STATE.md), 本轮 rebase 改用一次性 worktree 完成, 未触碰其未提交文件。
  推送: 8708636cf48 (基于 b9713cd4bc3)。
- 下轮恢复点: (1) 注意兄弟会话同 worktree 并行, 收敛核查改为只读; (2) READ_FROM_STORAGE 拆分开放项; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- 收尾批: READ_FROM_STORAGE restore 文本逗号连接 (tidb-ast hint.rs)。Go parser 每引擎组产出一个 TableOptimizerHint, RestoreOptimizerHints 以 ", " 连接; Rust 单 hint 双组内以空格连接导致 restore 文本分歧。改为组间 ", " 连接后与 Go 字节一致 (tidb-ast Go-oracle 表在 GOROOT 就绪时验证); 单 hint 双组建模保留为内部形态, 共享引擎组的去重角落 (Go 按条目去重 vs Rust 按合并文本) 记录为残余 narrowing。parser 733 / ast 100 / hint 2 全绿; fmt; diff-check; make lint 过。
  推送: caa7f4e59fb (一次性 worktree rebase, 兄弟仍在同 worktree 活跃)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-tablecodec (Go pkg/tablecodec @ a85e0fd5df) 全文件审计。修复 1 项 behavior 分歧:
  decode_index_kv 的 clustered-index V1 分支: Go 对唯一 common-handle 段与非唯一 key suffix 都用 kv.NewCommonHandle (tablecodec.go:1968-1975); Rust 把非唯一 suffix 走 decode_handle_in_index_key, 单 int 列 common handle (9 字节) 被折叠成 IntHandle 后命中 IntHandle.NumCols assert panic。V1 分支现在先于 general 分支选择 common_handle 段或原始 suffix 的真 common-handle 解码。
  回归 test_v1_non_unique_single_int_column_common_handle: pre-fix panic "IntHandle.NumCols is unsupported" (已确认), post-fix 解码 padded 索引列与 42 handle。调参过程: 旧布局 (value<=9) 走不到 V1 分支, 需 restore data 强制 V1 split; 两列索引的 suffix 含 2 个 datum 不触发折叠 —— 单索引列 + restore data 才是精确触发形态。
  其余匹配面 (前缀字节/errno/GenIndexKey/行值 v0-v1/临时索引/rowindexcodec) 与 12 项 cosmetic narrowing 详见收据 rust/docs/tablecodec-parity-audit.md。
  验收: cargo test -p tidb-tablecodec 61 全绿; fmt; diff-check; make lint 过。
  注意: 本机 vendored OpenSSL 补丁再次被共享 stash 竞态部分吞掉 (openssl 行丢 feature, reqwest 行仍在), 已修复重放 —— 以后验证补丁要逐行 grep 两个 feature 而非 grep 整词 vendored。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 推送修复: 服务端 hparser-integration 被兄弟会话回退到 8896e800216 (丢失 tablecodec 批次), 已重新 fast-forward 推送 0d4ffe94c98 恢复。共享分支 + 共享 worktree 双重并行下, 每轮开头必须 ls-remote 对账。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 事故与恢复: journal commit (559bd56a04c) 因共享 git index 卷入兄弟会话 31 个 staged 文件 (1717 行删除) 并已推送。已在一次性 worktree 中 revert 该提交除 PROGRESS-zcode.md 外的全部内容 (797edab51c0), 服务端净效果仅剩我的 2 行 journal; 被卷入内容仍可从 559bd56a04c 恢复, 兄弟工作树未触碰。规程变更: 共享 worktree 内一切 journal 提交改用 pathspec 限定形式 (git commit PROGRESS-zcode.md -m ...), 不再裸 git commit; 分支 ref 移动只用 update-ref。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-log + tidb-util/logutil (Go pkg/util/logutil + pingcap/log @ a85e0fd5df) 全文件审计。修复 3 项 behavior 分歧:
  (1) tidb-log FileSink 轮转备份改 lumberjack 命名 <base>-<本地时间戳><ext> (原 <file>.<sequence>), 清理只数可解析时间戳的备份并按解析时间排序 (原 mtime + 前缀匹配会误删同前缀兄弟文件), 回归 test_rotate_backup_name_uses_timestamp_format;
  (2) 未设 max-size 回退 Go DefaultLogMaxSize 300MB (原 lumberjack 100);
  (3) Level::parse 接受空串=info 且大小写不敏感 (zapcore UnmarshalText)。
  匹配面: text encoder 逐字节 golden (时间戳/级别/分隔/转义/errorVerbose/JSON 序), config 字段与 toml/json 名, global logger 生命周期, sampler, hex.rs 存在且 golden 对齐 (Go Hex() 零生产调用方, 反射面不可达已证), gRPC/ctx/opentracing 面声明性未移植且已证不可达。
  开放项: buildOptions 的 disable-caller/error-output-path/sampling/development 未生效 (仅测试调用方), OldSlowLogTimeFormat 待慢日志解析面。
  验收: cargo test -p tidb-log 23 全绿 (含新回归); tidb-util 564 全绿; fmt; diff-check; make lint 过。收据 rust/docs/logutil-parity-audit.md。
  规程生效: 本批 commit 用 pathspec 限定形式, 兄弟 staged 的 5 文件未被卷入。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-sqlexec (Go pkg/util/sqlexec @ a85e0fd5df) 全文件审计。零 behavior 分歧。
  匹配面: RestrictedSQLExecutor 3 方法, ExecOption 全 10 字段 + 9 个 option 函数 + GetExecOption 左折叠, 六个接口形状 (nil RecordSet=Option, NewChunk(nil)=new_chunk(None), Send+Sync 并发注记, TryDetach 三值返回), DrainRecordSet(AndClose) chunk 阶梯/close 恒执行且错误只记日志, ExecSQL 短路, SimpleRecordSet 全语义。
  补齐两处文档契约: ParseWithParams 占位符契约 (%?/%%/%n + 注入 caveat) 与 RecordSet close 后重读重启的 Go 无条件保证。文档化 narrowing: mid-drain 部分行丢弃 (今日无观察方), Rust-only seam traits。
  验收: cargo test -p tidb-sqlexec; fmt; diff-check; make lint 过。收据 rust/docs/sqlexec-parity-audit.md。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-unistore 的 lockstore+lockwaiter 声明面 (Go @ a85e0fd5df) 审计。零 behavior 分歧; 2 项低风险对齐:
  (1) lockwaiter 延迟唤醒 deadline 改用 Go 的原始有符号算术 (负延迟配置保留原 deadline, 原实现钳 0 立即返回);
  (2) lockstore replace/delete 在 node_set_next 前断言 hint.prev 非空 (Go nil 索引 panic 的镜像, 原实现会静默写 block 0)。
  匹配面: 常量/节点布局/find 家族/Put-PutWithHint 高度启发/replace-delete 拼接与长度/随机高度 p=1/4/MaxEntrySize/Get buf-refill/arena 对齐溢出释放窗/lockwaiter 全哨兵与唤醒语义/延迟计时器 already-fired guard/CleanUp 排水。
  记录: lib 测试目标因兄弟 distsql 接口改动预存编译失败 (双向 stash 验证与本批无关); RNG 源/原子-借用纪律/每调用计时器为站点注释 narrowing。
  验收: cargo build -p tidb-unistore 过; fmt; diff-check; make lint 过。收据 rust/docs/unistore-lock-parity-audit.md。
- 下轮恢复点: (1) 兄弟修复 distsql 接口后回补 unistore 测试目标验证; (2) 只读收敛核查或新面; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- 新面批次: tidb-protocol (Go pkg/server/internal @ a85e0fd5df) 全包审计。修复 2 项 behavior 分歧:
  (1) COM_SHUTDOWN (0x08)/COM_CHANGE_USER (0x11) 落入 Unknown —— protocol 层补常量与解臂 (Command::Shutdown/ChangeUser), 只读 Rust SQL 节点按声明义务应答 unsupported 错误 (Go conn.go:1554/1567 由完整服务端应答);
  (2) 不可渲染 datum 报 1105 —— 新增 ErrorKind::InvalidType 映射 Go err.ErrInvalidType (8057, column.go:175/238), 经新 ResultSetStreamError::error_kind() 暴露, 回归 invalid_type_maps_to_go_err_invalid_type 验证线上字节 79 1f。
  文档化: advertisedstatus checker 补入头注义务清单; zstd 级别刻度差异 (线格式兼容); 头溢出拒绝/解压长度校验为 Rust 纵深防御; 类型向量复用 Rust 提前报错 (同为错误结局); utf8mb4 wrapper 仅回退用。
  匹配面: 包框架 0xffffff 续传与零长终止帧、序号语义 (压缩模式 MariaDB 忽略)、压缩信封全参、dump 家族含 BinaryTime 1 字节天 quirk、列定义 dump 全规则、DumpBinaryRow 位图、文本行 NULL/类型矩阵与 float E 规则、StmtFetch、NUL 裁剪、parseBinaryParams 全矩阵、STMT_EXECUTE 门、prepare 应答、ERR/OK/EOF 含 DEPRECATE_EOF。
  验收: cargo test -p tidb-protocol 110 全绿 (含新映射回归); cargo build -p tidb-server; fmt; diff-check; make lint 过。收据 rust/docs/protocol-parity-audit.md。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 增量收敛核查: 同步至 374536a2386 (兄弟 statistics/usage + cache + priorityqueue + hint processor return contracts 等), 我修复过的面全绿: hint 2/0, tablecodec 61/0, protocol 110/0, sqlexec 0+0, timer 16+49/0。无新增分歧。注意兄弟新提交 "util/hint: align processor return contracts" 改了 hint processor —— 我上一批的 contains_table_hint 大小写注释与 NTH_PLAN 修复内容需在其上仍成立 (本轮测试通过即证)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-ddl-session (Go pkg/ddl/session @ a85e0fd5df) 全文件审计。零 behavior 分歧。
  匹配面: Session 操作序 (StmtRollback 先于 RollbackTxn)、Execute prometheus 指标 (同指数桶/ok-err 后缀/panic 覆盖)、请求源默认 ddl、DrainRecordSet 8、RunInTxn NotifyBeginTxnCh failpoint (Condvar 等价无缓冲通道)、池 Get/Put/Destroy/Close 全序与 "session pool is closed" 文案。
  文档化: 类型断言错误路径结构不可达、RecordSetCloser 记日志 vs Go 静默、schedule-eval trait 为前瞻移植脚手架 (基线 Go 无此面)。
  验收: cargo test -p tidb-ddl-session 5 全绿; fmt; diff-check; make lint 过。收据 rust/docs/ddl-session-parity-audit.md。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: stats/cache — 按当前 Go 树顶重钉 pkg/statistics/handle/cache。发现旧收据前提过期: c6054025 确实无 stats_table_row_cache.go (上游 #69955 删除), 但 #69955 不在本树祖先链 — 现 tip 该文件存在 (blob 682b9ba9, 212 行) 且 infoschema_reader.go:671/731/1344-1393 消费全局 cache.TableRowStatsCache。恢复移植: tidb-stats-handle-cache::stats_table_row_cache (update_by_id 双读皆成或皆不拷贝/maps.Copy upsert/零默认), provider 重接线为 Go 读者流 (全可见表+分区 ID 刷新, 失败仅告警, 返回值取自缓存快照) — 修复客户端可见分歧: 刷新失败旧为清零, 现为 Go 的保留上次值。估计器单一实现: snapshot() 供 tidb-exec TableSizeStats 纯函数。回归: crate 单测 4 + store 级 the_table_row_size_cache_serves_previous_values_after_a_failed_refresh (敏感度已验证: 破坏 both-or-nothing 即红)。全部 tidb-exec 失败 8 项与 stash 基线逐字节一致 (兄弟在途)。已推送 59c8bba3b77。收据 statistics_handle_cache_audit.md + operations/statistics-handle-cache-audit-execplan.md 已重写。
- 顺带核验 (只读): pkg/ttl/ 与 pkg/util/tiflash/ 自 c6054025 字节不变 — ttl/tiflash 收据对当前 tip 仍有效; tidb-ttl 40 测试、tidb-distsql tiflash 3 测试、tidb-txnkv tiflash lib 全绿。infoschema: pkg/infoschema/cache.go blob 与 c6054025 相同 (2c1660ff), InfoCache 按设计分歧维持, infoschema-parity-audit.md 已加 2026-09-06 重钉节。plan cache 面归 go-physical-plan-parity-execplan (sortexec/copr 余项他人持有); ddl-session 为兄弟今日活跃面未碰。
- ddl 重钉轮 (只读核验): pkg/ddl 自 c6054025 变化 51 文件 (-4287/+338, 以测试删除为主)。抽查 generated_column.go: Walk→Accept 签名重构 (无观察面); checkEmbedTextGeneratedColumn/findEmbedTextDependency 已从 checkModifyGeneratedColumn 移除且全树无生产引用 — Rust 从未移植该检查, 无需移除; typeIndex 面 (expression_index.rs AdmissibilityScan) 对现字节仍逐面一致 (报告序 3758/1111/3800/3593/arity/8200, disallow-cast-array 归 typeColumn 边界已记录)。generated_column_prior_order 收据仍有效。
- 下轮恢复点 (本会话): (1) ddl 其余生产面 diff 抽查 (create_table.go 71 行/executor.go 221 行变化未核); (2) plan cache 面 sortexec/copr 归 go-physical-plan-parity-execplan 持有人; (3) ttl/tiflash/infoschema/cache 已重钉完毕。
- stats/cache 收尾轮: 修复 tip 上预存失败的 loaded_column_ndv_reaches_grouped_cluster_plans — PARTITIONS 段期望过期。当前 Go updateStatsCacheIfNeed (infoschema_reader.go:646-661) 按 e.columns 保留列自剪: TABLE_NAME-only PARTITIONS 不刷新缓存 (旧注释 "Go does not column-prune PARTITIONS" 描述合并前 reader, 该 pruning 随 merge 进树但 restore 保留了全局缓存路径 + 保留了 pruning)。Rust 计划级 needs_storage_stats 剪枝与现 Go 一致 → 期望改为 reads 保持 2。修复后测试首次全程跑到 fail-flag 段, 我上一批的 seam 契约 pin (fake provider Err → 零列) 一并转绿。cargo test 该测试 1/1, fmt/clippy 干净。收据 caveat 同步改写。
- ddl 重钉轮 (只读核验): pkg/ddl 自 c6054025 变化 51 文件 (-4287/+338, 以测试删除为主)。抽查 generated_column.go: Walk→Accept 签名重构 (无观察面); checkEmbedTextGeneratedColumn/findEmbedTextDependency 已从 checkModifyGeneratedColumn 移除且全树无生产引用 — Rust 从未移植该检查, 无需移除; typeIndex 面 (expression_index.rs AdmissibilityScan) 对现字节仍逐面一致 (报告序 3758/1111/3800/3593/arity/8200, disallow-cast-array 归 typeColumn 边界已记录)。generated_column_prior_order 收据仍有效。
- executor.go/create_table.go/index.go/column.go/ddl.go/ddl_tiflash_api.go 生产 diff 全部核验为以下三类之一: (a) columnar-storage DDL 门整体移除 (checkColumnarStorageEnabled/ForNewTable/deferred columnarGateChecked/kill-flag 转换) — Rust 从未移植该门, 无 Rust-only 行为可删; (b) 外部工作负载 TTL 注册移除 (registerTTLTableToExternalWorkload) — 未移植; (c) worker 侧重构 (EncodeRow encoder 签名/scheduler factory 改名/setIndexVisibility 简化) — 未移植面。ALTER TABLE COMPRESSION 预校验移除亦未移植。结论: 现 Go 生产面是 c605 的子集, 移植面无缺无余, ddl 本轮零代码分歧。
- 下轮恢复点 (本会话): (1) 其余 69 张 ddl_* 收据的 blob 重钉 (大批量, 逐张核 git hash-object vs 收据表); (2) plan cache 面 sortexec/copr 归 go-physical-plan-parity-execplan 持有人; (3) ttl/tiflash/infoschema/cache/stats-cache 已全部重钉完毕。
- ddl 收据重钉轮 (2026-09-06 第二轮): 脚本化核验 15 张带 blob 表的 ddl_* 收据, 共 89 个 (path,blob) 对 — 88 个与 HEAD 逐字节一致; 唯一漂移 pkg/ddl/label/rule_test.go = Go 测试夹具 protobuf 字段重构 (KeyspaceMeta.Keyspace 包装), 生产行为零变化, ddl_label.md 收据行已更新。其余 54 张收据无 blob 表, 改用 owner 套件重验: tidb-ddl-copr/logutil/mock/notifier/resourcegroup/serverstate/session + tidb-schemaver + tidb-placement 共 65 测试全绿, tidb-executor ddl_label 13 测试全绿。
- 下轮恢复点 (本会话): (1) 无 blob 表的 54 张 ddl 收据逐张读回与现字节比对 (大批量); (2) plan cache 面 sortexec/copr 归 go-physical-plan-parity-execplan 持有人; (3) 六大模块 (meta schema cache/stats cache/plan cache/ddl/ttl/tiflash) 均已重钉并有本会话记录。
- ddl 覆盖面复核轮 (2026-09-06 第三轮): 54 张无 blob 表收据与 51 个变更文件求交 — 11 张涉及 index.go/executor.go/modify_column.go/multi_schema_change.go/partition.go/create_table.go/serial_test.go。逐个核验: modify_column.go 唯一生产 delta = ProcessModifyColumnOptions restoreFlags 把 RestoreWithoutTableName 换成重复 WithoutSchemaName (上游笔误) — 只影响 MODIFY 携带 AS(expr) 的 GeneratedExprString 重存, 而 Rust 该路径整体拒绝 (alter_table.rs:3092 "MODIFY COLUMN of a generated column is not supported yet"), 无分歧面; multi_schema_change.go=UseCloudStorage 代理传播删除 (worker 侧, 未移植); partition.go=Walk→Accept+clustered-PK backfill 注释 (worker 侧); 其余同前两轮分类。结论: 11 张收据全部对现字节有效。
- 下轮恢复点 (本会话): (1) 43 张无 blob 表且无变更文件交集的收据保持有效 (构造性论证: 其覆盖文件未变); (2) plan cache sortexec/copr 归他人; (3) 六大模块重钉完毕。
- ddl 覆盖面复核补遗: serial_test.go 的变更仅为 TestAlterTableCompression 删除 (与 executor.go COMPRESSION 预校验移除一致, 集成测试同步), ddl_temporary_create_like_warning.md 的临时建表警告面未受影响。11 张交集收据全部核验完毕, 零分歧。
- 推送恢复: 兄弟 revert (c5776e1e7bf) 波及并丢失了 ddl-resourcegroup 批次 (代码+收据), 已重新 fast-forward 推送恢复。规程补充: 共享分支被 revert/rewrite 后, 已推送批次可能丢失 —— 每轮 ls-remote 对账时同时抽查最近批次的标志性内容是否仍在服务端。
- 分支重组: 本地谱系已重放到 c5776e1e7bf 之上 (PROGRESS-zcode.md 双-append 冲突按拼接解决)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) unistore 测试目标随兄弟 distsql 修复回补; (3) pd-client 外部 pin 待决; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 回补批: unistore InProcessClient 实现 SynchronousBatchRequestDispatcher (tidb-txnkv::client 的 BatchCommands 式分发契约) —— 兄弟 distsql 接口重构要求的最后一块 glue, lib 测试目标从完全不可编译恢复为 114 测试全绿; distsql 256/29 测试同步验证。open 项关闭。
  验收: cargo test -p tidb-unistore --lib 114/0; tidb-distsql 256+29 全绿; fmt; diff-check; make lint 过。含代码改动, 已推送 (LANDED_1)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 新面批次: tidb-domain 三面 (topn_slow_query/historical_stats/plan_replayer @ a85e0fd5df) 审计。修复 1 项 liveness 分歧: plan_replayer worker 循环以 catch_unwind 包裹 handle_task (Go defer util.Recover 语义), panicking dump 不再杀死 worker 线程。头注两处声明与代码矛盾修正 (metrics 实为保留/状态方法 8 个/take_receiver 代 GetWorker)。
  匹配面: topn 堆序与淘汰、historical_stats 四条错误文案逐字与哨兵语义、plan_replayer GC 保留规则/四条 SQL 逐字/handleTask 三门/SendTask 通道语义/DirName 与文件名三分支。
  开放: plan_replayer_dump.go 未移植 (zip 布局/TOML 键/presign, PLAN REPLAYER DUMP 执行器依赖), 计数器覆盖面因此收窄; sort.Sort 不稳定序差异文档化。
  验收: cargo test -p tidb-domain 143 全绿; fmt; diff-check; make lint 过。收据 rust/docs/plan-replayer-domain-parity-audit.md。含代码改动, 已推送 (LANDED_1)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 小面巡检: optimize_trace.rs 对 Go optimize_trace.go 完整且边界注记齐备 (GetOptimizerTraceDirName, PID 回退); domainutil.rs 实为 pkg/util/domainutil/repair_vars.go 的移植 (非 pkg/domain/domainutil) —— 待后续按正确 Go 源定位核验。收敛: domain 143, unistore 114/0, hint 2/0 全绿。
- 下轮恢复点: (1) domainutil 按正确 Go 源 (pkg/util/domainutil) 复验; (2) unistore 测试目标随兄弟 distsql 修复回补; (3) pd-client 外部 pin 待决; (4) 只读收敛核查或新面; (5) dbsid 分叉待协调。
- domainutil 复验完成: 该模块实为 pkg/util/domainutil/repair_vars.go (198 行, 该包唯一文件) 的完整移植 —— 进程级 REPAIR_INFO (LazyLock 镜像 Go 包变量+init 种子)、InRepairMode/SetRepairMode/GetRepairTableList/GetMustLoadRepairTableListByDB (小写匹配 + 大小写敏感 table2ID 遍历注释保留)/SetRepairTableList (锁前小写)/CheckAndFetchRepairedTable (浅拷贝 DBInfo + 隔离表追加)/GetRepairedTableInfoByTableName (首个匹配库返回 (nil,db) 的 quirk 保留)/RemoveFromRepairInfo (清空库出 map + 清空 map 关 repair mode)/RepairKeyType Display 全部一致。唯一 narrowing: GetRepairTableList Rust 克隆返回 (Go 返回内部切片)。tidb-domain 143 全绿。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) unistore 回补已闭; (3) pd-client 外部 pin 待决; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 全量收敛 sweep: 本会话全部 12 个修复/审计面一次性复验 —— hint 2, tablecodec 61, protocol 121 (含兄弟并跑新增), sqlexec 0, timer 65, domain 143, unistore 114, br 31, schemaver 9, stmtsummary 64, ddl-session 5, ddl-resourcegroup 1 —— 合计 571 passed / 0 failed。replayer.rs (131 行, PlanReplayerTaskKey + 文件名生成器 + DirName, 上轮 plan-replayer 审计已覆盖) 与 disttask.rs (106 行, GenerateExecID, schemaver 审计已覆盖) 均确认无遗漏。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) unistore 回补已闭; (3) pd-client 外部 pin 待决; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 新面批次: tidb-pd-client tso 面 (外部 pin: github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149, 源自 module cache) 审计 + 修复。落地 4 项:
  (1) 批内时间戳算术改纯加法 (pinned dispatcher.go:461,483 不读 suffix_bits), 丢弃 count<<suffix_bits 位移;
  (2) 重试语义对齐 handleProcessRequestError: 全错误可重试直至等待 deadline, 撤销 20 次上限与窄可重试集, 终态错误为 deadline miss (Go ctx.Err() 类比);
  (3) 重试间隔 500ms 均匀 (constants.RetryInterval), 无首次免费;
  (4) 单调性违规 (tso_fallback) 保持终态: Go dispatcher 内 panic, 本 crate 报错并存活 (文档化 narrowing)。
  3 个旧 suffix-shift 测试按 pinned 语义重写; malformed/fallback 集成测试改钉 终态 fallback + 重试至 deadline + 终态 timeout。
  验收: cargo test -p tidb-pd-client 26 lib + 44 int 全绿; fmt; diff-check; make lint 过。收据 rust/docs/pdclient-tso-parity-audit.md。已推送 (LANDED_1)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 增量收敛核查: 同步至 f35307a0f2a (本会话 pd-client 批为服务端 tip), 相关面全绿: pd-client 26+44+3/0, domain 143/0, unistore 114/0, ddl-session 5/0, ddl-resourcegroup 1/0, naming 2/0。无新增分歧。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 小面巡检: tidb-resolve (Go pkg/planner/core/resolve/{resolve,result}.go, 135 行) —— TableNameW/NodeW/Context/AddTableName/GetTableName/GetTableNames/ResultField 全对齐; Context 以 TableIdentity 复现 Go *ast.TableName 指针身份键; ResultField 7 字段含 empty_org_name。零分歧。tidb-ddl-logutil (52 行) 待巡。
- 下轮恢复点: (1) ddl-logutil 小面巡检; (2) 只读收敛核查或新面; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- 小面巡检: tidb-ddl-logutil (Go pkg/ddl/logutil/logutil.go, 62 行) —— 零分歧。四 logger 对齐: DDLLogger("ddl")/DDLUpgradingLogger("ddl-upgrading")/DDLIngestLogger("ddl-ingest") 共享 bg_logger + category 字段, SampleLogger = SampleLoggerFactory(time.Minute, 3, "ddl") 的进程共享采样实例 (LazyLock)。
- 下轮恢复点: (1) 只读收敛核查或新面; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 巡检+收敛: disttask.rs = pkg/util/disttask/idservice.go 完整移植确认 (GenerateExecID/MatchServerInfo/FindServerInfo/GenerateSubtaskExecID/4Test 全在, schemaver 审计已证 GenerateExecID 一致)。收敛 sweep: domain 143, unistore 114, pd-client 73, timer 65, tablecodec 61, protocol 121 —— 全绿。
- 下轮恢复点: (1) 大面候选: plan_replayer_dump.go 移植 (开放项, ~1000 行) / unistore mvcc+cophandler (超声明面) / domain 全量; (2) F2/F3-seam live 阻塞; (3) DST 排队; (4) dbsid 分叉待协调。
- 增量收敛核查: 全部 7 面全绿 (domain 143, pd-client 73, unistore 114, br 31, tablecodec 61, protocol 121, hint 2 —— 合计 545/0)。无新增分歧。
- 下轮恢复点: (1) plan_replayer_dump.go 移植 (大面, 需新会话完整上下文: zip 布局/sql-meta TOML/presign/extractTableNames/统计回退); (2) 只读收敛核查; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- 种子批: plan_replayer_dump.go seed 片段 (archive 文件名常量 9 个 + sql-meta TOML 键 7 个 + build_sql_meta_records 构建器, BTreeMap 复现 Go toml 排序键) + PlanReplayerDumpTask 补 StartTS/SQLDigest/PlanDigest/HistoricalStatsTS 字段 + 2 个种子测试。模块头注改为"部分已播种"。145 测试全绿; fmt; diff-check; make lint 过。已推送 (LANDED_1)。
- 下轮恢复点: (1) plan_replayer_dump 主体移植 (zip 装配/会话采集/presign); (2) 只读收敛核查或新面; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- dump 主体第二批: build_config_toml (dumpConfig, 全局配置 TOML 序列化) 与 build_meta_txt (dumpMeta, printer.GetTiDBInfo 原文) 落地; tidb-domain 增 toml/tidb-util 依赖。2 个测试: TOML 可回解析 + meta.txt Release Version 头。147 测试全绿; fmt; diff-check; make lint 过。已推送 (LANDED_1)。
- 下轮恢复点: (1) dump 主体剩余: dumpSQLMeta 的 zip 写入/variables/bindings/tiflash-replica/schemas/stats/presign; (2) 只读收敛核查或新面; (3) F2/F3-seam live 阻塞; (4) dbsid 分叉待协调。
- 范围确认: extractTableNames/tableNameExtractor 移植需要 (a) tidb-ast Visitor (enter/leave over dyn Any, 已存在) (b) infoschema seam (TableExists/TableByName/View.select_stmt/ForeignKeys, model 字段齐备 ViewInfo.select_stmt:430 + FKInfo:895) (c) executor ParseWithParams 复入 —— 约 200+ 行, 需完整上下文一批成型, 本轮不开工以免中途截断。
- 下轮恢复点: (1) tableNameExtractor + findFK + handleIsView 移植 (带 infoschema/executor seam, ~200 行); (2) dumpSchemas/dumpVariables/bindings/stats 后续切片; (3) 只读收敛核查; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
- 增量收敛核查: 7 面全绿 (domain 147, pd-client 73, unistore 114, br 31, tablecodec 61, protocol 121, hint 2 —— 549/0)。无新增分歧。
- 下轮恢复点: (1) tableNameExtractor+findFK+handleIsView 移植 (前置件已确认: Visitor/ViewInfo.select_stmt/FKInfo/seam 先例, ~200-300 行需完整上下文); (2) dumpVariables/bindings/stats/presign 切片; (3) 只读收敛核查; (4) F2/F3-seam live 阻塞; (5) dbsid 分叉待协调。
