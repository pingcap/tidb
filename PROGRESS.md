# tidb-parity 滚动进度（本地 summary，不入库）

> 当前焦点 / 下一步：①expr-builtin 字符串族扫尾 ②chunk A-1（需 datum 决策）③parser #11（结构性）④等用户的分区对照查询后验证 Rust 裁剪。控制流审计项 3 已全闭环（惰性求值 + NULLIF 规则均与 Go 一致；CASE 静态类型提升为已记录残差）。注意：另一会话 ca9bc95d09 修了 parser 测试注册——重放我批次时需重跑 parser/lexer 套件（已验证全绿）。

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
