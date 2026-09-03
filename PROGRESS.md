# tidb-parity 滚动进度（本地 summary，不入库）

> 当前焦点 / 下一步：①分区裁剪 Rust 验证（等用户对照查询）②expr-builtin：CASE/IF laziness + NULLIF NULL rule 审阅 ③expr-builtin 字符串族扫尾 ④chunk A-1（需 datum 决策）⑤parser #11 charset-aware scanner（结构性深改）。

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
