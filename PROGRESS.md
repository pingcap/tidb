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
