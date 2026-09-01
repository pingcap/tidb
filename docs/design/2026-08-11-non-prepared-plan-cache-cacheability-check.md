# Non-Prepared Plan Cache 支持范围与 cacheability 检查改造

配套材料：

- [代码实现说明](2026-08-11-non-prepared-plan-cache-cacheability-check-implementation.md)
- [其他数据库实现调研](2026-08-11-non-prepared-plan-cache-other-databases.md)
- [OceanBase 实现专项调研](2026-08-11-non-prepared-plan-cache-oceanbase.md)

## 背景

Plan Cache 不能只根据参数化 SQL 判断计划是否可以复用。同一种 SQL 结构可能因为参数位置、表类型、schema 或 session 状态不同而不适合缓存。TiDB 会在不同阶段检查：

- AST 结构和表达式是否适合复用；
- 参数类型和取值是否与缓存计划兼容；
- schema、MDL、权限和会话环境是否变化；
- 优化后的物理计划是否安全。

本设计主要调整优化前的前端职责：Non-Prepared 如何处理普通 SQL literal，以及如何判断参数化后的 statement 能否进入公共 Plan Cache 流程。后端已有的 schema、权限、参数类型、range rebuild 和物理计划检查继续保留。

## 当前两条路径

### Prepared Plan Cache

Prepared SQL 在进入 Plan Cache 前已经有参数标记：

```sql
PREPARE stmt FROM 'SELECT * FROM t WHERE a = ?';
```

TiDB 可以直接对带参数标记的 AST 调用 `IsASTCacheable`，检查 statement 类型、hint、函数、参数位置、相关开关、表属性和结构大小。通过后构造已经 parse/preprocess 的 statement carrier，再进入共用后端。

### Non-Prepared Plan Cache

普通 SQL 中没有客户端参数：

```sql
SELECT * FROM t WHERE a = 1;
SELECT * FROM t WHERE a = 2;
```

Non-Prepared 需要先判断 literal 是否能安全转换，再得到统一的参数化 SQL：

```sql
SELECT * FROM t WHERE a = ?;
```

当前流程先调用 `NonPreparedPlanCacheableWithCtx`。这个 checker 同时承担：

1. DML/locking read 等入口策略；
2. 特殊 literal、literal 数量和 `LIMIT` 等参数化策略；
3. statement 结构、表类型和表达式等 AST cacheability。

第 3 类职责与 `IsASTCacheable` 重复，但两边长期演进不一致。当前 Non-Prepared checker 采用较窄的 AST allowlist，因此 HAVING、window、CTE、三表以上 join 等 Prepared 已支持的结构仍可能被提前拒绝。

## 问题和目标

本次首先要解决的是支持范围和规则漂移问题：Non-Prepared 不应仅因为独立 AST allowlist 没有覆盖某个结构，就拒绝 Prepared 已经能够安全处理的 SQL。改造后，Non-Prepared 只保留普通 SQL 自动参数化特有的判断；与参数来源无关的 AST、schema 和物理计划安全规则复用公共逻辑。

基于 TiDB 当前代码结构，本次选择删除重复 AST allowlist，并在 Non-Prepared 参数化后调用现有 `IsASTCacheable`。这是实现规则复用的方式，不是为了共用某个函数而设定的目标，也不表示可以简单改成：

```text
参数化 → IsASTCacheable(parameterized AST)
```

参数化后，公共 checker 已经看不到原 literal 的写法、类型和所在上下文。旧 checker 中属于 Non-Prepared 参数化安全的规则必须在原始信息仍然可见时完成；实现上可以与 literal 处理合并为一次遍历，但职责上仍要与公共 AST cacheability 检查分开。新进入 parameterizer 的 AST 位置也必须逐类确认。

本次改造目标：

- 扩大并稳定 Non-Prepared Plan Cache 的 SQL 结构支持范围；
- 删除容易与 Prepared 规则漂移的 Non-Prepared AST allowlist；
- 让 Non-Prepared 只保留职责明确的参数化安全检查；
- 复用与参数来源无关的公共 AST 和后端安全规则；
- 在 TiDB 当前实现中，通过参数化后调用 `IsASTCacheable` 落地公共 AST 规则复用；
- 修复参数化过程中 original AST 恢复的正确性问题；
- 保持后端缓存和物理计划保护不变。

第一阶段继续保留：

- 特殊 literal 的保守处理：能够忠实保留原 token 时进入 statement identity，当前实现无法安全保留时整条语句 bypass；
- 原始 literal 默认 200 的上限；
- DML/locking read 和 `LIMIT` 的产品开关；
- `SELECT ... INTO` 的提前回退；
- 每次执行公共 checker。

本次不修改 statement LRU key 的组成。现有 statement LRU 仍用于保存和查找已经 parse/preprocess 的 carrier；key 完整性和解析上下文隔离问题另行处理。

特殊 literal 的进一步参数化、OceanBase 风格的 not-param constraint matcher，以及只在 statement LRU miss 时运行公共 checker，不属于第一阶段。

## 目标流程

```text
普通 SQL / original AST
    ↓
检查入口 gate
    - 总开关和通用排除项
    - DML / locking read 开关
    ↓
参数化前检查
    - 原始 literal 总数
    - 参数化流程的基本前提
    - LIMIT 产品策略
    - SELECT ... INTO
    ↓
按上下文处理 literal，并选择性参数化
    - 普通数据值 parameterize
    - 结构或解释敏感的值 preserve
    - 所有路径恢复 original AST
    ↓
检查 ParamSQL 构造结果
    - success：得到可靠的 ParamSQL 和 ParamValues
    - bypass：无法保证 restore 或参数映射正确，走普通优化
    - error：original AST 无法安全恢复，返回错误
    ↓
按参数化 SQL 查现有 statement LRU
    - hit：取得缓存的 parameterized AST / carrier
    - miss：重新 parse 参数化 SQL；失败时整条语句 bypass
    ↓
每次执行 IsASTCacheable
    - false：记录 bypass reason，走普通优化
    - true：继续构造或复用 carrier
    ↓
共用 Plan Cache 后端和物理计划检查
```

## 参数化规则

### Literal 的两种处理方式

每个 literal 按所在 AST 上下文选择两种处理方式之一：

- **parameterize**：可以自由变化，例如 `WHERE`、`ON`、`IN`、`BETWEEN`、HAVING 和 DML values 中的普通运行时数据；
- **preserve**：会影响语法结构、输出列名、metadata 或表达式解释，需要保留原值。

同一条 SQL 可以同时包含 parameterize 和 preserve。例如：

```sql
SELECT * FROM t WHERE a = 10 ORDER BY 1 LIMIT 100;
```

第一阶段得到 `SELECT * FROM t WHERE a = ? ORDER BY 1 LIMIT 100`：`10` 被参数化，`ORDER BY 1` 和 `LIMIT 100` 保留。

第一阶段的 preserve 采用简单实现：literal 继续留在 parameterized SQL 中，因此自然进入 statement identity。`ORDER BY 1` 和 `ORDER BY 2` 会形成不同 key，不需要额外实现 OceanBase 风格的 not-param matcher。

第一阶段按下面的原则处理常见 literal：

| literal 场景 | 处理 |
| --- | --- |
| 普通过滤值、join 条件、HAVING、DML values/assignment | parameterize |
| `ORDER BY`/`GROUP BY` 位置序号、window frame、影响输出 metadata 的值 | preserve |
| `NULL`、BIT/HEX literal | preserve 原 AST 节点和原 token |
| charset introducer literal | introducer 与字符串整体 preserve |
| `WEIGHT_STRING(... AS CHAR/BINARY(n))` 的语法参数 | 只参数化第一个表达式，保留 `CHAR`/`BINARY` 和长度 |
| 构建期类型依赖参数（`CHAR ... USING` charset、`LPAD/RPAD` length、`CONVERT_TZ`/`UNIX_TIMESTAMP` datetime、`FROM_UNIXTIME` timestamp） | preserve，避免函数签名或返回类型按首次执行固化 |
| 时间精度或构建期类型依赖参数（`TIME`、`TIMEDIFF`、`TIMESTAMP`、显式 FSP 的当前时间函数） | preserve，避免 FSP 或首参数类型按首次执行固化 |
| `ROUND`/`TRUNCATE` 的值和 scale 参数 | preserve，避免返回 decimal scale 按首次执行固化 |
| `RAND(seed)` 的 seed 参数 | preserve，避免不同 seed 复用首次构建的 RNG 状态 |
| `ADDTIME`/`SUBTIME` 的参数 | preserve 整个函数参数列表，避免返回 FSP/Flen 按首次执行固化 |
| `BENCHMARK` 的 loop count 参数 | preserve loop count，第二个表达式按常规上下文处理，避免循环次数按首次执行固化 |
| 未识别的 literal 上下文 | 默认 preserve |

`fallback` 不是第三种 literal 决策，而是整条语句的控制流。所有 literal 处理完成后，如果 ParamSQL restore 或参数数量/顺序无法保证正确，本次参数化结果为 bypass，整条 SQL 回到普通优化；statement LRU miss 后在同一上下文中重新解析 ParamSQL 失败，也按 statement 级 bypass 处理。如果 original AST 本身无法安全恢复，则返回错误，不能继续使用可能已经被修改的 AST。

### 默认必须保守

删除旧 allowlist 后，CTE、set operation、window、subquery 和 multi-table DML 等结构会首次进入 parameterizer。参数化器必须按上下文显式决定 literal 的处理方式：

> 未确认安全的位置不能默认参数化；第一阶段优先 preserve。如果 preserve 机制本身无法忠实生成 ParamSQL，则整条语句 bypass，而不是给该 literal 增加第三种决策。

context matrix 必须落实为代码中的上下文规则，而不仅是一张测试表。公共 checker 运行在参数化之后，不能替代这层判断。

### 参数化前检查

参数化前需要完整遍历 original AST，包括最终会被 preserve 的子树。这一步只判断参数化流程能否安全启动，不承担特殊 literal 的通用拒绝，也不重新引入 statement 结构 allowlist。

- 所有 original literal 都计入默认 200 上限；
- `LIMIT` 产品开关关闭时，含 literal `LIMIT/OFFSET` 的 SQL 按现有策略 bypass；开关打开时，第一阶段可以 preserve，后续再单独评估参数化；
- `SELECT ... INTO` 在 carrier 构造前 bypass。

`NULL`、BIT/HEX 和 charset introducer 交给 literal 处理逻辑：第一阶段优先保留原 AST 节点及完整 token。charset introducer 必须与后面的字符串整体保留。仅仅跳过参数替换并不一定等于忠实 preserve；例如当前参数化 restore 使用的配置会省略字符串 charset，实施时必须修正 restore 方式。某类 token 仍无法忠实进入 ParamSQL 时，参数化流程将整条语句标记为 bypass。

这些拒绝都表示“不使用 Non-Prepared Plan Cache”，不是 SQL 执行错误。

parameterizer 会临时替换 original AST 中的 literal，再 restore 出参数化 SQL。无论 SQL restore 成功还是失败，都必须在退出前恢复 original AST，避免普通优化路径看到残留的参数标记或已经归池的对象。

## 公共 checker 的调用方式

参数化完成后，对 parameterized AST 调用 `IsASTCacheable`。第一阶段在 statement LRU hit 和 miss 时都调用，因为结果仍依赖 subquery/limit 开关、partition prune mode、fix-control、当前表属性等 session 和 InfoSchema 状态。

只有在这些动态输入已经被现有的缓存隔离、metadata 或可靠失效机制覆盖后，才可以考虑只在 LRU miss 时检查。

公共 checker 由 Non-Prepared 入口显式调用，不修改 Prepared carrier 构造函数的检查和 warning 行为，避免 Prepared 专用 fix-control 意外覆盖 Non-Prepared 的拒绝结果。

## Statement LRU admission

statement LRU 仍然保存已经 parse/preprocess 的 carrier。只有同时满足下面两个条件的 carrier 才能写入现有 statement LRU：

- parameterized AST 通过 `IsASTCacheable`；
- carrier 构造完成后仍标记为可缓存。

如果某次执行因为动态开关或当前 InfoSchema 被公共 checker 拒绝，已有 LRU entry 可以保留，但本次不能进入 Plan Cache 后端。下一次仍需根据当时状态重新检查。

## Bypass reason、warning 和 metric

### Reason 优先级

本次不保证 `EXPLAIN FORMAT='plan_cache'` 的具体 reason 文本与旧 checker 完全一致。支持范围和检查顺序都会变化，强行保留旧文本会继续暴露旧 checker 的实现结构。

需要保证同一状态下结果确定，并按下面的优先级选择第一个 reason：

1. 入口策略；
2. 参数化安全；
3. 公共 AST cacheability；
4. statement carrier；
5. 物理计划。

普通执行不新增 warning；Explain Plan Cache 继续使用 Non-Prepared 的 warning 前缀。

### Metric 语义

旧 checker 只有进入完整 AST visitor 后被拒绝才增加 unsupported counter，fast-check early return 不计数。新流程统一计数后，指标范围会扩大。

本设计接受这个可观测变化，并定义新的计数边界：

| 场景 | unsupported counter |
| --- | --- |
| 总开关、trace、restricted SQL 等入口排除 | 不计 |
| DML/locking read 功能开关关闭 | 不计 |
| 参数化 precheck 或上下文安全规则拒绝 | 计一次 |
| `IsASTCacheable` 拒绝 | 计一次 |
| carrier 构造后不可缓存 | 计一次 |
| 物理计划 checker 拒绝 | 沿用后端 metric，不重复计 |

升级后 unsupported counter 不能与升级前的绝对值直接比较，dashboard 可能出现台阶变化。metric 不增加 reason label，避免高基数和指标兼容问题。

## 行为变化矩阵

删除通用 AST allowlist 不等于下面所有 SQL 都保证命中。每一类需要分别经过 parameterizer、公共 AST 和物理计划检查。

| SQL 类别 | literal 处理 | 公共 AST | 最终预期 | 第一阶段 |
| --- | --- | --- | --- | --- |
| HAVING | 普通过滤值 parameterize | 按公共规则检查 | 普通计划预期可命中 | 放开 |
| window | frame 等结构值 preserve | 按公共规则检查 | 视物理计划而定 | 放开已确认位置 |
| subquery | 内部 literal 按上下文分类 | 受 subquery 开关控制 | 视 subquery 计划而定 | 开关打开时验证 |
| CTE、set operation | 各分支按相同规则分类 | 按公共规则检查 | 视物理计划而定 | 分类放开 |
| 三表以上 join | filter literal parameterize | 按公共规则检查 | 普通 join 预期可命中 | 放开 |
| multi-table DML | filter/assignment parameterize | 可能通过 | 写入正确性风险较高 | 单独阶段 |
| view | 按表达式位置分类 | 可能通过 | 依赖 schema 保护验证 | 验证后决定 |
| system table | 按表达式位置分类 | 可能通过 | `PhysicalMemTable` 通常使整条语句 bypass | 不承诺命中 |
| JSON/ENUM/SET/BIT 类型列的 filter | literal 按形式和上下文分类 | 可能通过 | 依赖类型和 range 保护 | 单独验证 |

对于尚未确认的类别，允许增加范围具体、命名明确的临时策略；不能重新引入“未知 AST 默认拒绝”的通用 statement allowlist。

## 正确性和兼容性决策

- 原始 literal 默认 200 上限第一阶段保留，后续再讨论改成实际参数数量；
- view、system table 和特殊列类型不一次性承诺命中，按行为矩阵分阶段验证；
- 每次执行 `IsASTCacheable` 是第一阶段的正确性要求，性能成本通过 benchmark 量化后再优化；
- 被参数化的普通 literal 第一阶段继续使用 `Datum` 和现有类型推导；特殊 literal 先 preserve，不要求为了本次改造立即把它们转换成 typed parameter；
- schema、MDL、权限、参数类型和物理计划等后端检查保持不变；
- Prepared 和 Non-Prepared 不要求共享 statement carrier、cache entry 或物理计划。

## 风险和验证重点

| 风险 | 处理方式 |
| --- | --- |
| 未知 AST 位置被自动参数化 | context-aware 规则，未知位置默认 preserve；无法忠实生成 ParamSQL 时整条语句 bypass |
| 特殊 literal 信息丢失 | literal 处理逻辑保留原 AST 节点和完整 token，restore/重新解析不可靠时整条语句 bypass |
| literal 上限被 preserve 绕过 | precheck 遍历全部 original literal |
| 动态开关变化后沿用旧结论 | 每次执行公共 checker |
| 参数化失败污染 original AST | 所有退出路径原子恢复 |
| reason 和 metric 无意变化 | 固定 reason 优先级，明确 metric 新语义 |
| AST 通过但物理计划仍拒绝 | 行为矩阵区分各阶段结果 |

端到端测试需要比较 cache disabled、Prepared、Non-Prepared miss 和 Non-Prepared hit 的结果，并对不同参数值执行 `A → B`、`B → A` 两种顺序。除查询结果外，还要比较 output metadata、warning、affected rows 和最终数据。

## 实施阶段

### PR 1：参数化正确性修复

- 修复所有错误路径上的 original AST 恢复；
- 增加对应单元测试。

这个 PR 不改变 checker、支持范围、reason 和 metric。

### PR 2：完成新流程切换

- 增加 parameterization precheck，以及区分参数化成功、整条语句 bypass 和内部错误的返回接口；
- 实现 context-aware literal 处理，literal 层只区分 parameterize 和 preserve，未知位置默认 preserve；
- 让普通 literal parameterize，特殊 literal preserve-first；
- 把 DML/locking read 开关移到入口 gate；
- 参数化后在 statement LRU hit/miss 都调用 `IsASTCacheable`；
- 统一 statement LRU admission、bypass reason 和 metric；
- 删除旧 checker 及其 AST allowlist；
- 先放开行为矩阵中风险较低且验证完成的类别；
- 对高风险类别保留命名明确的临时策略；
- 增加参数化矩阵、端到端差分测试和性能 benchmark。

这个 PR 可以按内部 commit 分层实现：先落 precheck、literal 处理规则和单元测试，再切换调用链并删除旧 checker，最后补齐 reason、metric、端到端测试和 benchmark。生产代码不引入“旧 checker 和新 precheck 长期串联”的临时状态。

详细的函数、文件改动、伪代码和测试命令见[代码实现说明](2026-08-11-non-prepared-plan-cache-cacheability-check-implementation.md)。

## 最终状态

改造后，HAVING、window、CTE、多表 join 等结构不再仅因 Non-Prepared 独立 allowlist 未覆盖而回退。Non-Prepared parameterizer 判断普通 SQL literal 如何安全转换或保留，并在无法构造可靠 ParamSQL 时让整条语句 bypass；公共 AST 规则判断参数化后的 statement 是否适合复用，Non-Prepared 入口负责 LRU 和 bypass 编排，公共后端继续负责 schema、权限、参数类型和物理计划安全。

TiDB 当前通过调用 `IsASTCacheable` 复用公共 AST 规则；真正需要长期保持一致的是规则职责和支持范围，而不是某个具体函数的调用形式。





# future work

特殊 literal 的进一步参数化、OceanBase 风格的 not-param constraint matcher，以及只在 statement LRU miss 时运行公共 checker
