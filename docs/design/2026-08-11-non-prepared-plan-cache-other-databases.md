# 其他数据库的 Non-Prepared Plan Cache 调研

本文是 [Non-Prepared Plan Cache 统一 cacheability 检查](2026-08-11-non-prepared-plan-cache-cacheability-check.md) 的配套调研。具体代码改动见 [实现说明](2026-08-11-non-prepared-plan-cache-cacheability-check-implementation.md)，OceanBase 的源码调用链见 [专项调研](2026-08-11-non-prepared-plan-cache-oceanbase.md)。

## 调研范围

`Non-Prepared Plan Cache` 是 TiDB 使用的名字。其他数据库中，与它最接近的能力通常叫：

- automatic/simple/forced parameterization；
- cursor sharing；
- statement concentrator；
- fast parameterization。

它们要解决的问题基本相同：应用发送的是带字面量的普通 SQL，而不是带参数的 prepared statement；数据库希望把其中可以安全替换的字面量提取成参数，让多条结构相同的 SQL 复用解析结果或执行计划。

本文关注的是“普通 SQL 如何归一化并复用计划”，不讨论客户端 prepared statement 的协议细节，也不讨论 result cache。

## 结论先行

几种实现虽然名字不同，但大体都可以拆成下面几层：

1. 识别并提取可以参数化的字面量，得到归一化 SQL 和参数值。
2. 根据字面量所在的语法位置、类型和语义，决定参数化、保留原值，或者整条 SQL 不缓存。
3. 用归一化 SQL 加数据库、会话语义和优化器环境组成缓存键。
4. 在同一个归一化 SQL 下，根据参数类型或参数分布选择一个或多个计划。
5. 在 schema、统计信息或相关会话状态变化后，使已有结果失效或重新优化。
6. 控制只执行一次的 SQL 对缓存空间的污染，并提供原始 SQL、归一化 SQL、命中情况和不缓存原因等诊断信息。

对 TiDB 当前方案最直接的启发是：

- “先参数化，再调用公共 `IsASTCacheable`”的方向是合理的，但参数化之前仍然需要保留 Non-Prepared 特有的语法位置和类型检查。
- 不能把现有 parameterizer 原样当成完整的 cacheability checker。其他数据库也都存在“这个位置保留原值”或“这个语句不共享”的规则。
- statement LRU 的 key 不能只有归一化 SQL。数据库、SQL mode、字符集、时区等可能改变解析或语义的状态也要进入 key，或者能够使缓存失效。
- 一个归一化 SQL 永远只对应一个计划并不是成熟系统的最终形态。SQL Server、Oracle、PostgreSQL 和 Db2 都以不同方式处理参数敏感问题。
- 参数的类型信息很重要。长期看只保存 `Datum`，依靠值反推类型，不足以覆盖所有兼容性要求。

## 整体对比

| 数据库 | 普通字面量 SQL 自动归一化 | 参数化策略 | 同一 SQL 的计划选择 | 主要特点 |
| --- | --- | --- | --- | --- |
| SQL Server | 支持 | Simple 或 Forced Parameterization | 普通缓存；新版本支持 Parameter Sensitive Plan | 语法位置规则详细，生成参数有明确类型，缓存键包含大量会话环境 |
| Oracle | 支持 | `CURSOR_SHARING=FORCE` | 一个 parent cursor 下可以有多个 child cursor；Adaptive Cursor Sharing 按值域选计划 | 把归一化 SQL 和具体可共享计划分开管理 |
| Db2 | 支持 | Statement Concentrator | 可结合 `REOPT` 在首次或每次执行时重新优化 | 很强调参数类型、长度和使用上下文对结果及计划的影响 |
| OceanBase | 支持 | Fast Parser 做快速参数化 | plan cache 中查找可用计划 | 与 TiDB 场景最接近；归一化结果显式保存参数和不参数化信息 |
| PostgreSQL | 普通 Simple Query 不自动做这一层 | Prepared Statement 内使用参数 | 在 generic plan 和 custom plan 之间选择 | 重点是 prepared statement 内的计划复用和参数敏感性 |
| MySQL | 官方能力主要围绕 prepared statement | 客户端或 SQL PREPARE 显式提供参数 | prepared statement 的内部结构复用 | 可作为兼容性对照，不是 TiDB Non-Prepared 方案的直接参考实现 |

## SQL Server

### 基本设计

SQL Server 把普通 SQL 的自动参数化分成两档：

- Simple Parameterization：只参数化一小部分形态简单、风险较低的语句。
- Forced Parameterization：在数据库级别尽量参数化普通的 `SELECT`、`INSERT`、`UPDATE`、`DELETE`，但仍然有明确的排除条件。

参数化后的语句进入 plan cache。SQL Server 的缓存中会区分 ad hoc、自动参数化、动态 SQL 和 prepared SQL 等对象，但它们会共享一套与编译环境相关的缓存管理机制。

[SQL Server Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

### 不是所有字面量都直接替换

Forced Parameterization 也不是遍历 AST 后把所有常量替换成参数。例如下面这些位置有专门规则，可能保留原值，或者让语句不能使用 forced parameterization：

- `TOP`、`TABLESAMPLE`、`HAVING`、`GROUP BY`、`ORDER BY` 等子句；
- `LIKE` pattern、`CONVERT` style、部分内建函数参数；
- 可以在编译期折叠的算术表达式；
- query hint 参数；
- 已经 prepared 的语句、游标、特定 `SET` 环境、`RECOMPILE`；
- 参数数量过多的语句。

这里最值得 TiDB 参考的不是具体规则，而是规则的分层：某个字面量不能替换，不一定意味着整条语句不能缓存；只有当前机制无法保留语义时，整条语句才需要 bypass Plan Cache。

### 参数带有明确类型

SQL Server 会根据原始字面量生成有具体类型的参数，例如整数、带 precision/scale 的 decimal、不同长度范围的 varchar/nvarchar 和 varbinary。官方文档也明确提醒，常量表达式在参数化前后可能因为类型推导、算术和转换规则产生不同结果。

这说明参数化结果不应该只表达“这里有一个值”，还需要可靠地保留“这个值在原始 SQL 中是什么类型”。

### 缓存键不只是归一化 SQL

SQL Server 暴露的 plan attributes 中，可能参与缓存键的内容包括：

- database id；
- 影响语义的 `SET` options；
- language、date format、date first；
- compatibility level；
- name resolution 依赖的 user；
- cursor options；
- 临时表相关的 session 信息。

[SQL Server `sys.dm_exec_plan_attributes`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-exec-plan-attributes-transact-sql?view=sql-server-ver17)

这和 TiDB statement LRU 面临的问题相同：即使归一化 SQL 文本一样，只要解析、名字解析或表达式语义可能不同，就不能直接复用同一个 statement carrier。

### 参数敏感和缓存污染

自动参数化会引入参数敏感问题：第一次编译时适合某个值的计划，可能不适合其他值。SQL Server 2022 以后提供 Parameter Sensitive Plan Optimization，可以为同一条参数化语句保留多个 plan variant，并根据参数落入的基数区间选择计划。

[SQL Server Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)

SQL Server 还支持 `optimize for ad hoc workloads`：某条 ad hoc SQL 第一次出现时只保存较小的 plan stub，第二次出现才保存完整计划，用来减少大量 single-use SQL 污染 plan cache。

[SQL Server optimize for ad hoc workloads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option?view=sql-server-ver17)

## Oracle

### Cursor Sharing

Oracle 默认使用 `CURSOR_SHARING=EXACT`，只有 SQL 文本一致时才共享 parent cursor。配置为 `FORCE` 后，Oracle 会把普通 SQL 中的字面量替换成系统生成的 bind variable，再按转换后的 SQL 在 shared pool 中查找 cursor。

一次执行大致是：

1. 保存原始字面量并生成系统 bind；
2. 用归一化后的 SQL 查找 parent cursor；
3. 找不到时 hard parse，找到可共享的 child cursor 时 soft parse；
4. 使用本次的 bind 值执行。

`FORCE` 减少的是 hard parse，不会让每次提交 SQL 的 parse 动作本身消失。

[Oracle Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/23/tgsql/improving-rwp-cursor-sharing.html)

### Parent cursor 和 child cursor

Oracle 的设计比较清楚地分开了两类对象：

- parent cursor：代表归一化 SQL；
- child cursor：保存可以在特定环境下执行的计划、bind 和依赖对象等信息。

即使 parent cursor 相同，下面这些差异也可能要求新的 child cursor：

- SQL 解析到的 schema object 不同；
- optimizer mode 或 optimizer environment 不同；
- NLS 等会话语义不同；
- bind metadata 不兼容；
- 原有 child cursor 因 schema 或统计信息变化而失效。

这与 TiDB 中 `PlanCacheStmt` 和 `PlanCacheValue` 的职责划分很接近：前者是参数化语句的载体，后者才是具体计划。

### 仍然需要语法位置规则

Oracle 的文档举了 `ORDER BY` 数字常量的例子。数字在这里可能表示 select list 的序号，值变化会改变语义；即使系统做了 bind 替换，也不能简单认为两条语句一定可以共享 cursor。

这说明“字面量节点看起来相同”不等于“处在任何位置都可以参数化”。参数化逻辑必须知道父节点和使用上下文。

### Adaptive Cursor Sharing

Oracle 会观察不同 bind 值对应的 selectivity 和执行统计：

- 先把 cursor 标记为 bind-sensitive；
- 如果发现不同值的执行特征差异明显，再变成 bind-aware；
- 为不同 selectivity 范围生成或选择不同 child cursor；
- 等价计划可以合并，最终通常稳定为少量计划，而不是每个参数值一个计划。

这个方案说明，扩大自动参数化范围后，参数敏感性应该作为独立问题处理，而不应通过永久拒绝所有可能敏感的语句来回避。

### 产品定位上的提醒

Oracle 官方把 `CURSOR_SHARING=FORCE` 更多看作无法及时修改应用时的缓解方案，仍然更推荐应用使用 bind variables。这是因为自动替换发生在 SQL 已经送到数据库并完成解析之后，也无法替代应用层的 SQL 注入防护。

## Db2

### Statement Concentrator

Db2 可以通过 statement concentrator 把动态 SQL 中的字面量替换成类似 `:L0` 的参数，使结构相同的语句共享 package cache entry。该能力默认关闭，也可以选择在归一化时移除不影响语义的 comment。

Db2 的诊断和 explain 信息能够同时保留原始语句和集中后的语句，便于判断是否发生了归一化以及实际复用了哪个 cache entry。

[Db2 Statement Concentrator](https://www.ibm.com/docs/en/db2/11.5.x?topic=plans-statement-concentrator)

### 类型和长度会影响结果

Db2 文档特别强调了参数 marker 的类型和长度：它们由使用上下文决定，必要时需要通过 `CAST` 明确类型。Statement Concentrator 也可能因为归一化后的参数长度推导方式不同，改变字符串表达式的结果类型或长度。

[Db2 Parameter Markers](https://www.ibm.com/docs/en/db2/11.5.x?topic=ess-providing-variable-input-dynamically-executed-sql-statements-by-using-parameter-markers)

因此 Db2 要求常量复用时考虑：

- 使用位置是否相同；
- data type 是否一致；
- 字符串等类型的长度是否兼容；
- 某些要求 immediate value 的上下文中，常量值是否完全一致。

这个案例对 TiDB 很重要：如果参数化只返回 `[]Datum`，后续再从运行时值推断类型，有可能丢掉原始 literal 的类型、长度或写法信息。第一阶段可以对有风险的 literal 采用 preserve；如果当前机制仍无法忠实构造参数化 SQL，再让整条语句 bypass Non-Prepared Plan Cache。长期更合理的是参数化结果携带原始 `FieldType` 或等价元数据。

### REOPT

Db2 允许使用 `REOPT` 调整参数化语句的优化时机：

- `REOPT ONCE`：第一次执行时使用实际参数值优化，之后复用；
- `REOPT ALWAYS`：每次执行时都使用当前参数重新优化，并且不再使用 statement concentrator 的普通复用方式。

[Db2 REOPT](https://www.ibm.com/docs/en/db2/11.5.x?topic=bespdbc-performance-improvements-when-using-reopt-option-bind-command)

它和 Oracle/SQL Server 的实现不同，但解决的是同一个问题：归一化 SQL 可以共享，并不代表所有参数值都适合共享同一个计划。

## OceanBase

本节只概括产品设计；Prepared/Text 两条源码调用链、共用和不共用的检查见 [OceanBase 专项调研](2026-08-11-non-prepared-plan-cache-oceanbase.md)。

### Fast Parameterization

OceanBase 与 TiDB Non-Prepared Plan Cache 的场景最接近。普通 SQL 到达后，Fast Parser 先通过词法分析快速得到：

- 参数化 SQL；
- 原始参数值；
- 用于 plan cache 查找的 key。

命中后直接取得可用计划；未命中时再经过完整解析和优化，并将生成的计划写入缓存。

[OceanBase Fast Parameterization](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001717237)

[OceanBase Plan Cache](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001123504)

### 参数化上下文不只有 SQL 和值

OceanBase 的开源实现中，`ObFastParserResult` 保存 parameterized SQL、原始参数、question mark 上下文、`INSERT ... VALUES` token 位置和 array binding 参数等信息。完整 Parser 再把下面这些信息补充到 `ObPlanCacheCtx`：

- 哪些 token 不应参数化及其原始文本；
- select item 中参数的位置及表达形式；
- 参数的 charset、正负号和必须为正等约束；
- Prepared 模式下不能自由变化的参数值。

这些信息说明，快速参数化也需要显式表达“替换了什么”和“为什么这个位置不能按普通参数处理”，而不是只输出一段带 `?` 的文本。

[OceanBase `ObFastParserResult` 等数据结构](https://github.com/oceanbase/oceanbase/blob/master/src/sql/plan_cache/ob_plan_cache_struct.h)

### Plan cache key

OceanBase 开源代码中的 plan cache key 还包含 database id、session id、plan cache mode、系统变量、配置、namespace 和 connection collation 等信息。具体字段会随版本变化，但总体原则稳定：参数化 SQL 只是 key 的主要部分，不是完整 key。

查找流程的源码注释也明确分成：fast parser 生成参数化 SQL 和参数、按参数化 SQL 找 plan cache value、检查权限和可用性等步骤。

[OceanBase `ObPlanCache::get_plan`](https://github.com/oceanbase/oceanbase/blob/master/src/sql/plan_cache/ob_plan_cache.cpp)

### 控制和可观测性

OceanBase 支持通过 session/global `cursor_sharing` 控制是否自动替换字面量，也支持 statement hint：

- `CURSOR_SHARING_EXACT`：本条语句不做 literal replacement；
- `USE_PLAN_CACHE(NONE)`：本条语句不使用 plan cache。

系统视图中可以查看归一化 SQL、SQL ID 和 cursor sharing 状态；outline/plan binding 也使用参数化 SQL 或其 SQL ID 关联。这让参数化不仅服务于 plan cache，也成为诊断和计划管理时的稳定语句身份。

[OceanBase Cursor Sharing](https://en.oceanbase.com/docs/common-oceanbase-database-10000000000931135)

[OceanBase Query Hints](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001108045)

## PostgreSQL 和 MySQL：作为对照

### PostgreSQL

PostgreSQL 的内建计划复用主要围绕 prepared statement。Simple Query Protocol 接收完整 SQL 文本；Extended Query Protocol 则明确分成 Parse、Bind、Execute，参数值在 Bind 阶段提供。

[PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol-flow.html)

在 prepared statement 内，PostgreSQL 不会始终固定使用一个计划：

- custom plan 使用本次参数值优化；
- generic plan 不依赖具体参数值，编译成本较低；
- 默认先执行若干次 custom plan，再比较平均代价和 generic plan 代价，决定后续使用哪一种；
- 也可以通过 `plan_cache_mode` 强制使用 custom 或 generic plan。

[PostgreSQL PREPARE](https://www.postgresql.org/docs/current/sql-prepare.html)

[PostgreSQL `plan_cache_mode`](https://www.postgresql.org/docs/current/runtime-config-query.html)

PostgreSQL 还会在 schema、`search_path`、role/RLS 等相关环境变化后重新分析或使 cached plan 失效。它的源码把 cached query source 和 generic/custom plan 分开管理，这也说明“参数化语句载体”和“具体执行计划”适合成为两个生命周期不同的对象。

[PostgreSQL `plancache.c`](https://doxygen.postgresql.org/plancache_8c_source.html)

从官方协议和 PREPARE 文档可以推断，PostgreSQL 没有把普通 Simple Query 中的不同字面量自动集中成同一条 prepared statement；它的主要借鉴价值是 generic/custom plan 选择和严格的失效管理。

### MySQL

MySQL 官方文档描述的 statement cache 主要用于 prepared statement 和 stored program。prepared statement 的解析结果及内部结构可以复用，并在依赖对象 metadata 变化后自动 reprepare。

[MySQL Caching of Prepared Statements and Stored Programs](https://dev.mysql.com/doc/refman/8.4/en/statement-caching.html)

[MySQL Prepared Statements](https://dev.mysql.com/doc/refman/8.4/en/sql-prepared-statements.html)

从官方文档覆盖的能力看，MySQL 没有提供和 SQL Server Forced Parameterization、Db2 Statement Concentrator 对等的普通文本 SQL 自动归一化接口。因此 TiDB 的 Non-Prepared Plan Cache 不是简单复刻 MySQL 行为，而是在 MySQL 协议兼容之外提供的服务端优化。

## 对 TiDB 当前设计的具体启发

### 1. 当前总体方向是合理的

当前设计准备把流程调整为：

```text
Non-Prepared 特有 precheck
    -> 参数化，并保留参数值/上下文
    -> 对参数化 AST 调用公共 IsASTCacheable
    -> 生成或复用 PlanCacheStmt
    -> 后端选择 PlanCacheValue
```

这与其他数据库的总体分层一致。公共 checker 负责回答“这个参数化后的语句能否进入 plan cache”，Non-Prepared precheck 负责回答“能否安全地把这条普通 SQL 转成这种参数化形式”。两者不能合并成一次不区分阶段的 AST 扫描。

### 2. 不建议直接复用现有 parameterizer 而不加约束

其他数据库都证明了参数化规则与语法上下文相关：

- SQL Server 会按 clause 或函数参数位置保留常量；
- Oracle 的 `ORDER BY` 数字可能是 ordinal，值本身参与语义；
- Db2 要求比较使用上下文、类型和长度；
- OceanBase 的参数化结果专门记录 not-param token 和位置元数据。

因此 TiDB 当前设计中的三种处理结果是必要的：

1. parameterize：替换成参数；
2. preserve：保留原值，但语句仍可继续检查；
3. reject：整条语句不走 Non-Prepared Plan Cache。

`LIMIT`、`ORDER BY` ordinal、内建函数特殊参数、字符集 introducer、bit/hex literal、`NULL` 等，都应先明确属于哪一种，而不是由通用 `ValueExpr` visitor 统一替换。

### 3. 第一阶段应保守保留原始类型语义

SQL Server 和 Db2 都把参数类型作为参数化协议的一部分。TiDB 第一阶段如果还没有完整的 typed parameter result，应继续拒绝或保留那些参数化后可能改变类型推导、结果 metadata 或表达式语义的 literal。

后续可以把参数化结果从：

```text
parameterized AST + []Datum
```

扩展为：

```text
parameterized AST
+ parameter value
+ original FieldType / charset / collation
+ source location and parameterization kind
```

这样才能逐步、安全地扩大支持范围，而不需要把所有特殊情况永久放在 deny list 中。

### 4. statement LRU 必须使用复合 key

当前设计准备给 statement LRU 补充 database、SQL mode、charset/collation 等信息，这一点有充分的外部实现依据：

- SQL Server 把 database、`SET` options、language/date 等作为 cache-key attributes；
- Oracle child cursor 受 schema resolution、optimizer 和 NLS 环境影响；
- OceanBase key 包含 database、session、sys vars、config 和 collation；
- PostgreSQL 会因 `search_path`、role/RLS 等变化重新分析。

原则上，所有可能改变 parser 输出、name resolution、返回类型或优化语义的状态都需要满足以下二选一：进入 key，或者在变化时可靠地使对应 carrier 失效。

### 5. 公共 checker 每次执行仍有必要

当前 TiDB statement LRU 还没有完整覆盖所有动态状态和失效条件。在这个前提下，即使命中 `PlanCacheStmt`，仍然每次对本次参数化 AST 调用 `IsASTCacheable`，是合理的保守设计。

未来只有在下面两个条件都满足后，才适合把完整 cacheability 检查移到 LRU miss：

- key 已包含所有影响检查结果的语义环境；
- schema、table 属性、系统变量等变化能够准确使 carrier 失效。

### 6. 参数敏感计划应作为后续独立议题

TiDB 后端已经会按参数类型等维度区分部分 plan cache entry，但这不等价于按 selectivity 选择计划。扩大 Non-Prepared 支持范围后，可能更容易遇到同一归一化 SQL 在不同参数值上最优计划差异很大的情况。

其他数据库提供了几种不同思路：

- SQL Server：一个 dispatcher 加多个 parameter-sensitive variants；
- Oracle：根据运行时统计逐步形成 bind-aware child cursors；
- PostgreSQL：比较 generic 和 custom plan 的成本；
- Db2：通过 `REOPT` 选择首次或每次重新优化。

本次 cacheability 统一不需要同时解决这个问题，但设计上不要假设“一个归一化 SQL 必须永久对应一个计划”。

### 7. 需要补齐可观测性

至少需要能够区分下面几类状态：

- 原始 SQL 和参数化 SQL；
- 参数化 precheck 被拒绝及其原因；
- 公共 `IsASTCacheable` 被拒绝及其原因；
- statement LRU hit/miss；
- backend plan cache hit/miss；
- 因 schema 或会话环境变化导致的失效。

这既方便验证新实现，也能避免用户看到 `last_plan_from_cache=0` 时无法判断是参数化失败、cacheability 失败，还是后端没有合适计划。

## 对本次实现范围的建议

### 本次应做

- 增加 Non-Prepared 参数化 precheck，明确 parameterize/preserve/reject。
- 参数化后统一调用 `IsASTCacheable`。
- 保守处理可能改变类型、metadata 或语义的 literal。
- statement LRU 使用复合 key，并且只缓存已经通过完整检查的 carrier。
- 保持 statement 级 bypass 行为，不让 cacheability 优化改变 SQL 本身能否正常执行。
- 增加 A 值后执行 B 值、B 值后执行 A 值的双向测试，避免首个 literal 决定后续行为。
- 测试结果值以外，再校验 result field type、charset/collation 和 warning/error 行为。

### 本次不建议一起做

- 把 AST parameterizer 重写成 OceanBase 式 lexical fast parser。
- 实现 parameter-sensitive 多计划选择。
- 把 session statement LRU 扩成全局 statement cache。
- 引入“第二次出现才缓存”的 admission policy。
- 一次性支持所有 bit/hex、introducer、特殊函数参数等 typed literal。

这些方向有价值，但与统一 cacheability checker 的正确性边界不同，放到同一个改动中会显著增加 review 和回归范围。

## 建议的后续演进顺序

1. 当前：完成 precheck、参数化、公共 checker 和复合 LRU key，优先保证行为兼容。
2. Typed parameter：让参数化结果携带原始类型、charset/collation 和来源位置，逐步缩小保守 reject 范围。
3. 可观测性：统一记录 raw SQL、normalized SQL、bypass stage/reason 和两层 cache hit。
4. 性能：在 key 和失效机制完备后，评估 checker 只在 LRU miss 执行，并评估 second-hit admission。
5. 计划质量：根据实际 workload 评估 generic/custom 或 parameter-sensitive 多计划机制。

## 调研限制

- SQL Server、Oracle 和 Db2 的实现细节主要依据官方行为文档、系统视图和公开概念模型，无法像开源项目一样检查完整实现。
- OceanBase 和 PostgreSQL 同时参考了官方文档与公开源码；源码链接指向当前主分支，只用于理解结构，不应依赖具体字段名作为稳定接口。
- PostgreSQL 和 MySQL 关于“普通文本 SQL 不自动集中”的描述是根据官方公开的协议、PREPARE 和 statement caching 能力作出的判断，不代表数据库内部完全不存在任何解析或元数据缓存。
- 本文调研时间为 2026-08-14。数据库新版本可能继续调整参数化范围、缓存键或参数敏感计划机制。
