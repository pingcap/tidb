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
