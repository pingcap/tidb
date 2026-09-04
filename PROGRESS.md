# tidb-parity 滚动进度（本地 summary，不入库）

> 已知 flaky：tidb-expr `json_schema_valid_resolves_file_and_http_references`（网络依赖，单跑通过/全量偶发，与本会话改动无关）。

> 当前焦点 / 下一步（union shape-pin 测试草稿未通过 harness 断言，已回退；inUnion 评估标志的 ignored gap pin 待下轮用 set_opr_tests 真实 harness 重写）：①expr-builtin temporal arithmetic（DATE_ADD/DATE_SUB/DATEDIFF/TIMESTAMPDIFF/EXTRACT，审计建议顺序 2——完全未扫）②chunk A-3 已核实过期（row.rs 重构为 row_decoder.rs，Missing/Null 已建模；datum 级 per-type 审计仍开放）③parser #11（结构性）④CAST 面：套件全绿，大缺口=inUnion flag 与 CAST flen/flag 产出族（audit item 5），需要多批次。⑤等用户的分区对照查询后验证 Rust 裁剪。注意：另一会话 ca9bc95d09 修了 parser 测试注册——重放我批次时需重跑 parser/lexer 套件（已验证全绿）。

## 已推送（origin/hparser-integration，截至 8e0f80e381 之后还有 f8ddb7c72a/06bccf90e2/6fba82d378/50a0a29c13/5465936985/3369859aa2）

- planner 10 提交：边界扫描 6 批 + 聚合消除 + 一元链/序列 + 代价/skew + narrowings 审计 + 5 个确定性失败修复 —— planner 库全绿 903/0。
- parser `f8ddb7c72a`：谓词/IS 链式 latches（divergence #3 关闭）+ #4 门验证钉住（`3369859aa2`）—— parser 全绿 730+97/0。
- datatype `06bccf90e2`/`6fba82d378`：json_path $[N to] 降级 + lone surrogate U+FFFD 三表面 —— datatype 全绿 370+63/0。
- codec `5465936985`：chunk wire decoder 去 first-zero/单调校验 —— codec 全绿 45+166/0。
- datatype `50a0a29c13`：FieldTypeBuilder F2 零值（executor 136 失败为既有基线，与本改动无关）。

## 进行中

- tidb-lexer #9（ANSI_QUOTES 标识符文本用 scanString 解码缓冲）已实现+6 断言回归已加，卡在关键词计数测试失败（690 vs 钉 689；Go 钉 684/233）—— 比对表找多出的 6 条。
- 工作树未提交：lexer lib.rs + lexer_source.rs 新测试 + types audit F3 已标记 verified。

## 纪律

- push 前 `git fetch origin hparser-integration` + `git rebase FETCH_HEAD`（tracking ref 不可靠）。
- commit message 禁用反引号（shell 替换）。
- 不碰主 worktree / tidb-expr collation / 其他会话活跃文件。

- lexer #9（进行中→完成）：ANSI_QUOTES 标识符文本改用 scanString 解码缓冲（decode_quoted_string + NO_BACKSLASH_ESCAPES 分支）+ 6 断言回归；关键词计数测试更新为生成目录真值 690（generate_keyword --check 对 go-master parser.y 字节级通过；手工钉的 689 过期）。lexer 全绿 86+3+6/0。
- 已推送 dc6330cbbc：lexer #9 ANSI_QUOTES 解码 + 关键词计数 690（generate_keyword --check 对 go-master parser.y 通过；手工钉 689 过期）。lexer 全绿。
- 已推送 4f3651cb9a：审计文档更新——算术 flen/decimal 三规则与 Go 逐行一致（item 1 闭环）；控制流推断核心已逐 case 实现（item 3 核心闭环，CASE/IF laziness + NULLIF 规则仍开放）；types F3 验证已修复。
- 发现：expr-builtin 审计的多个"Not done"条目在当前树上已被吸收实现（控制流推断、F3）——审计文档条目逐个过期中，每批先验证再动手。

- 已推送 0e908e13d7：进度日志入库（PROGRESS.md 成为共享日志，其他会话也在写）。远端在其 mview/ddl 批次上叠加了我的三个提交（lexer #9、审计进度、日志）。四包套件在最终树全绿。

- 控制流审计项 3 全闭环：IF/IFNULL/CASE 惰性求值与 NULLIF 规则均与 Go 一致（lib.rs:149/:160/:998、func.rs:539）；唯一残差是 CASE 的全分支静态类型提升（lib.rs:1009-1016 已记录）。

- 字符串族第一批：LPAD/RPAD 内容/截断/字符计数/负长度的 Go 钉住回归（string_packet.rs，验证已实现行为）。审计 item 4 的 REPLACE/STRCMP 等已在前续实现，剩余 ELT/MAKE_SET/EXPORT_SET/packet-limited 族待逐个核对。

- 已推送 115f5aa5ce：EXPORT_SET 求值实现（Go 四签名：bit0 先行、3 参默认 ,/64、5 参 clamp 0..=64、NULL 传播、arg_eval_type 掩码声明 int 位）+ 6 断言回归（位序、两种 arity、clamp、NULL 传播、dispatch 可达）。expr 全绿 1113+18/0。

- CAST 面勘察：builtin_cast_semantics 套件全绿（3 通过 + 1 文档化 ignored gap=向量化层）。F2 修复后 decimal=-1 → 0 的涟漪已消（expr 全绿 1113+18/0）。inUnion flag 与 CAST flen/flag 产出族为多批次项目（audit item 5）。

- 已推送 632d55f3f2：temporal 复合单元提取（DAY_MICROSECOND..SECOND_MICROSECOND）的 Go 钉住回归 6 断言（日期串、时长串、六位微秒保留、负号整体应用）；诊断出新分歧：MINUTE_MICROSECOND 提取 mm:ss.ffffff 时 parse_signed_duration_hms 要求 HH: 前缀返回 NULL（Go ParseDurationValue 按 2 组分配 = 203456700）——已排队单独批次。time_fn 全绿 46/0。expr 全绿 1114+18/0。

- 已推送 608dda6d29：MINUTE_MICROSECOND mm:ss.ffffff 分歧确认已被分组解析重写解决（2 组→mi/sec），补 3 断言（两分组 MINUTE_MICROSECOND/HOUR_MICROSECOND、单分组 SECOND_MICROSECOND 已有）。expr 全绿 1114+18/0。

- 审计 item 4（字符串族）关闭：INSERT packet 溢出已被 TestInsertBinarySig 移植钉住、STRCMP collation 已实现、ELT/FIELD/MAKE_SET/EXPORT_SET 覆盖齐；CHAR/VARCHAR padding 归 storage 面仍开放。已推送。

- chunk 审计 A-3 核实过期：引用的 panic 站点已不存在（row.rs 重构为 row_decoder.rs + ColumnLookup::Missing/Null 建模，row_decoder_source.rs:61-62 钉住）；datum 级 per-type 审计仍开放。

- expr-builtin 审计 item 6（math）与 item 7（temporal）核实为已实现（RAND 种子、复合单位提取、微秒进位均已在树上），文档已更新为 RESOLVED。审计"Resume here"清单全部对账完毕。

- 已推送（本轮）：union 形状 pin —— 探针发现 Rust UNION 对类型不匹配子树就地重指向 joined type（无 cast 节点），与 Go 的 BuildCastFunction4Union cast-ScalarFunction 形状不同；这正是模块文档记载的 narrowing。已转正为回归钉住（union_mismatched_children_are_re_typed_in_place_like_the_documented_narrowing）。inUnion 评估标志实现批仍待 ScalarFunction 状态扩展决策。
- 已完成 chunk A-1 读回补齐并推送（跟进上游 c59b2bd60e 的 datum 决策）：上游 lossy 桥的 resultFrac= FromString 尾部 digitsFrac，会把隐藏字暴露到可见刻度之外；现钉为 min(可见刻度, 保留位数)（与精确路径和 Go producer 一致，协议可见小数位=resultFrac），set_result_frac 从 test-only 放宽为 pub(crate)。fail-before=隐藏字乘积 72 vs 71；另加整数溢出字节级 oracle（对照 Go FromString）、全零符号归一 pin（master mydecimal.go:531-543）、chunk 读回集成 pin。两 crate 777 测试，40 个失败与基线对照集完全一致。回执 chunk_a1_readback_parity.md。
- 已完成 parser #12（@@instance. 扫描器前缀）并推送：tidb-lexer scan_at 去掉 Rust-only 的 "instance." 前缀——Go startWithAt（lexer.go:671）只认 global./session./local.，@@instance. 由语法层（parser.y SystemVariable/VariableAssignment）从字面量拆分；旧前缀泄漏导致 set @@instance."x"=1 被误接受（Go 语法错误）、select @@instance. 被误拒（Go 解析为 Instance 空名变量）。两个 fail-before 回归 + lexer/token 形状 pin；普通 @@instance.x 的 token 跨度前后字节一致。lexer+parser 925/925 全绿。回执 parser_instance_scope_prefix.md，parser-lexer-divergence.md #12 已标 FIXED。
- 已完成 CAST 目标类型产出族（audit item 5）并推送：rewriter cast_target 对齐 parser.y CastType 规则——BINARY(N) 有长度时切 TypeString；AS YEAR 产出 TypeYear（eval 仍 ETInt 不变）；FLOAT 保 TypeFloat {12,-1} 不再折叠到 Double；DOUBLE 补 {22,-1} 默认；SIGNED/UNSIGNED/DECIMAL/DOUBLE/FLOAT/JSON 补 binary charset+BinaryFlag（JSON 另补 utf8mb4）；VECTOR 只设 charset/collation 无 BinaryFlag（既有 pin 正确）。1 个 fail-before 回归钉全表；tidb-expr 全量除已知网络 flake 全绿，exec/planner 的 cast/sysvar/restore 消费者测试通过。回执 cast_target_type_family.md。同批核销 expr-builtin 审计的过期条目：C/D/E/G 已全部实现（NOT FIXED 标记清零）。
- 已完成 CAST AS CHAR 宽度产出（audit item 5 续）并推送：移植 Go adjustRetFtForCastString 全表——int 按型宽 3/4、5/6、8/9、10/11、Longlong 恒 20（issue 44786）、Year 4、Bit 源 flen；Real 87/370（TiDB 用 f 格式）；decimalPrecisionToLength；时间族 10/19 +1+小数位；JSON 涨码 LongBlob + 4294967295；字符串族继承/blob 常量。build_cast_function 仅对 VarString 未定宽目标生效（Go TypeString 早退，BINARY(N) 不受影响）。CHAR CHARSET 的 coercibility/repertoire 仍是记录边界。1 个 fail-before（修复前未定宽目标 flen=-1 vs Go 20）；tidb-expr 全量除已知网络 flake 全绿。回执 cast_char_width_estimation.md。
- 已完成 #196 标识符大小写映射（parser 面）并推送：digest.rs:251、user.rs:192/421、role_grant.rs:95、ddl_job_alter.rs:49 五处 Rust 全量 to_lowercase（尾随 Σ→ς 规则）换成 tidb_mysql::to_lowercase（Go strings.ToLower 简单映射移植，Go 锚点：digester.go:227、parser.y:12219/12223/12257/12261/12577）。fail-before 回归：`SELECT * FROM ΟΔΟΣ` 的 digest 必须是 οδοσ（σ）而非 οδος（ς）。CiString 本就用 simple-case 移植无需改；util_parser.rs 的 to_lowercase 是临时查找键（两侧同变换）不属可观察面。parser+lexer 925/925 全绿。
- 已完成 #196 planner 键面 + NUL 填充门并推送：schema_table_key/from.rs 十三处标识符键改用 Go 简单映射（CIStr.L=strings.ToLower）；CAST 宽度表落地后暴露 eval 的 NUL 填充过宽——Go padZeroForBinaryType 只对固定 TypeString+binary 填充，cast_type_of 现按 FieldTypeCode::String 重建填充长度（CAST(1 AS BINARY) Go=ret flen 20 值 1 字节）。两个 fail-before 回归，planner 1175/1175 全绿（含回绿的既有 pin）。
- 已完成 CAST AS CHAR CHARSET 边界闭合并推送：parse 时按 Go GetDefaultCollation 拒绝未知字符集（parser.y:9971 诊断）；cast_target 落 charset 名+默认 collation（BINARY 后缀加 BinaryFlag）；eval 按 ret charset 分流——binary 走字节截断不填充（padZeroForBinaryType 的 TypeString 门），utf8mb4 保持字符截断。三个 fail-before 回归。收窄记录：CHAR(3) BINARY 与 CHAR(3) CHARSET binary 在 Go 仅差 BinaryFlag，AST 载荷按 Go 自身 restore 一样折叠，flag 残差已记录。回执 cast_target_type_family.md（CHAR charset follow-up 节）。
- 已完成 parser 编码诊断对齐并推送：CAST(1 AS FLOAT(54)) → [expression:1426]Too-big precision 54 specified for 'CAST'. Maximum is 53.（Go FLOAT FloatOpt 规则）；ALTER TABLE ALGORITHM=FOO → [parser:1800]Unknown ALGORITHM 'FOO'（terror.ClassParser.NewStd）。均为 err_coded 编码诊断，fail-before 行钉在 test_error_msg 兼容表。回执 cast_target_type_family.md（FLOAT 诊断 follow-up 节）。
- #197 现状核实：IndexOption.tp 已是 Go 形状的原始 i64（ddl.rs:85 "Go IndexType numeric value"），index/partition 塌缩实例过期；ALTER ALGORITHM 的硬失败与 Go ErrUnknownAlterAlgorithm(1800) 语义一致（均已编码对齐）。#196 的五枚举清单未在文档中找到原始列表，暂缓。
- 混合类型控制函数下推（TryPushCastIntoControlFunctionForHybridType, builtin_cast.go:2898）本轮尝试后回退并记录：实现要点=build_cast_function 对 IF/"case_when"/ELT 的数值目标在分支含 hybrid（Enum/Set，Bit 排除 issue 24725）时下推 cast 分支并重推控制节点（infer_type4_control_funcs/builtin_return_type 重建 ret）；wrap 用 build_cast_function(LongLong{source flen,dec 0,bin}|Double{22,unspec}) 替代 ENUM_SET_AS_INT 戳（cast_arg_as_int 的 hybrid 短路已按 ordinal 求值）。已写结构+数值回归（IF(1,e,'a') SIGNED over enum('x','y','z')='y' → 2），但 eval 在 chunk get_bytes(0) 崩溃（EnumColumns ctx 供 Enum datum，某节点仍按行读 var 列，未定位）——需独立批次带插桩调试；价值=形状/元数据 parity（显式 CAST 场景 Rust 未下推也能答对 ordinal，因 Enum datum 存活到外层 cast），非错误值类。下轮可做：先定位 get_bytes 调用者（RUST_BACKTRACE=full 已见 eval 链），再决定是否需要 ENUM_SET_AS_INT 戳等价物。
- 已完成混合类型控制函数下推（builtin_cast.go:2898）并推送：build_cast_function 对 if/case_when/elt 数值目标在分支含 hybrid（Enum/Set，Bit 排除）时下推 cast 分支（wrap = Go WrapWithCastAsInt/Real 形状；ENUM_SET_AS_INT 戳不需要——cast_arg_as_int 的 hybrid 短路直接按 ordinal 求值）并以推断 ret 重建控制节点。fail-before 形状+数值回归（IF(1,e,'a') SIGNED over enum='y' → 2）。上轮 get_bytes 崩溃根因=测试 chunk 需在 offset 0 放枚举 datum（列节点按 offset 读行），已修复。tidb-expr 1181 测试除已知网络 flake 全绿。回执 cast_hybrid_push.md。
- 已完成 T14 并推送：convert_kind DST 分支按 Go Time{FromGoTime(tAdj)}（time.go:467 复合字面量清零 type/fsp）把调整后的值回退为 DATETIME/fsp 0；fail-before 回归（kind Timestamp→DateTime、fsp→0）+ 调整渲染 2018-03-11 03:00:00；tidb-datatype 474/474 全绿。回执 types_time_dst_metadata.md，types 审计 T14 标 FIXED。
- 已完成 ROW_COUNT/LAST_INSERT_ID 求值（builtin_info 面板）并推送：两个函数此前注册+定形但无求值臂（回答 Unsupported）。新增 row_count（PrevAffectedRows，None→NULL 沿 found_rows 惯例）、last_insert_id 0 参（PrevLastInsertID，UInt——vectorized 源 pin）与 1 参（cast_arg_as_int 求整 → set_last_insert_id 记录 → 返回同值）三臂；捕获式上下文回归验证记录副作用。fail-before 已验证；tidb-expr 全量除已知网络 flake 全绿。回执 info_row_count_last_insert_id.md。
- 已完成 FORMAT_BYTES/FORMAT_NANO_TIME 求值（builtin_info 面板）并推送：两函数注册+定形但无求值臂。新增臂经 numeric_arg 强制 ETReal 后按 Go GetFormatBytes/GetFormatNanoTime（util.go:1804-1879）渲染——IEC 字节阶梯与 ns→d 时间阶梯、divisor 1 时 0 位小数、其余 2 位、商 ≥100000 时科学计数（Go FormatFloat('e',2) 的两位指数+符号格式由 format_float_e_go 复刻）。fail-before（臂不存在）+ Go 自家 TestFormatBytes/TestFormatNanoTime 全向量。tidb-expr 全量除已知网络 flake 全绿。
- 完成注册表 arity 系统性核验并推送：283 个 Go baseFunctionClass（AST 常量经 functions.go 解析为 SQL 名）对 276 个 Rust registry 条目逐一比对——零分歧。7 个朴素名称差异均为专用机制（CAST 构建路径、typed getvar 签名、INSERT VALUES、builtin_ext JSON 分发）。核验回执 registry_arity_parity.md。
