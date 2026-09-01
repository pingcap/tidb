# OceanBase Non-Prepared Plan Cache 实现调研

本文是 [其他数据库的 Non-Prepared Plan Cache 调研](2026-08-11-non-prepared-plan-cache-other-databases.md) 的补充，重点回答下面几个问题：

- OceanBase 的 Prepared 和 Non-Prepared 是否共用 cacheability checker；
- 普通文本 SQL 是怎样参数化、查找和写入 Plan Cache 的；
- Prepared 和 Non-Prepared 的参数化有哪些差异；
- 这些设计对 TiDB 本次改造有什么参考价值。

调研基于 OceanBase `master` 的提交 `f05b2af69729379eae4612d1d5c2ce310ae093f0`，提交日期为 2026-08-13。文中的代码结构可能随版本变化。

## 结论

OceanBase 没有一个类似 TiDB `IsASTCacheable` 的单体 AST checker，由 Prepared 和 Non-Prepared 以完全相同的方式调用。

它的设计更接近：

```text
不同协议分别准备 SQL 和参数
        ↓
共用一套带 mode 的参数化和约束记录规则
        ↓
完整解析和优化后判断计划能否写入缓存
        ↓
命中时共用 schema、参数类型、权限和计划约束匹配
```

其中：

- 普通文本 SQL 和 Prepared 的入口不同，参数来源也不同；
- 两者共用大部分“某个 literal 能否自由变化”的语义规则；
- 不适合参数化的 literal 通常被保留下来，后续命中时要求其保持一致，而不是直接拒绝整条 SQL；
- schema、参数类型、权限、会话环境和物理计划等检查主要由后端共用；
- Text 和 Prepared 的 cache key 包含不同 mode，因此当前不会共享同一个 cache entry 或物理计划。

因此，如果把 checker 理解为一个返回 `true/false` 的函数，OceanBase 没有 Prepared/Text 共用的统一 checker；如果把它理解为完整的可缓存性判断，那么 OceanBase 采用的是“入口分开、参数化规则核心共用、后端匹配共用”。

## 先区分两类 mode

OceanBase 源码中存在两组容易混淆的 mode。

第一组是参数化阶段使用的执行模式：

| 参数化 mode | 含义 |
| --- | --- |
| `TEXT_MODE` | 普通文本 SQL，也就是本文所说的 Non-Prepared |
| `PS_PREPARE_MODE` | 处理 `COM_STMT_PREPARE`，分析参数位置并保存 Prepared metadata |
| `PS_EXECUTE_MODE` | 处理 `COM_STMT_EXECUTE`，将真实参数与 Prepare metadata 组合起来 |

此外还有 PL 对应的 Prepare/Execute mode，与本文关系不大，不再展开。

第二组是 Plan Cache key 中的模式：

| Plan Cache mode | 含义 |
| --- | --- |
| `PC_TEXT_MODE` | Text SQL 的缓存项 |
| `PC_PS_MODE` | Prepared Execute 的缓存项 |

即使两条路径最终都得到下面的参数化 SQL：

```sql
SELECT * FROM t WHERE a = ?;
```

它们也会因为 cache mode 不同而进入不同的缓存项。目前 OceanBase 共用代码框架和后端匹配逻辑，但不共用 Text/Prepared 的实际计划。

## Fast Parser 是什么

Fast Parser 是普通文本 SQL 查 Plan Cache 前使用的轻量级扫描器。它不构造完整 AST，主要完成下面几件事：

1. 找出 SQL 文本中的 literal；
2. 生成参数化 SQL；
3. 收集 literal 的值、位置和原始 token；
4. 尽早使用参数化 SQL 查 Plan Cache。

例如：

```sql
SELECT * FROM t WHERE a = 10 AND b = 'abc';
```

Fast Parser 大致得到：

```text
参数化 SQL：SELECT * FROM t WHERE a = ? AND b = ?
参数值：[10, 'abc']
原始 token：["10", "'abc'"]
```

它的目标是减少 cache hit 路径的开销。如果每次都先构造完整 AST、做名字解析，再查 Plan Cache，即使命中也已经付出了较高的前端成本。

Fast Parser 不负责完整理解 SQL 语义，例如：

- 不解析表和列对应的 schema 对象；
- 不负责完整的类型推导和权限检查；
- 不知道所有函数参数和特殊语法位置是否可以自由参数化；
- 不判断最终物理计划是否适合缓存。

所以第一次 cache miss 时仍然需要完整 Parser。完整 Parser 和参数化规则会确认 Fast Parser 的结果是否可靠，并把后续命中需要的约束保存下来。

Prepared Execute 已经从协议和 Prepare metadata 中知道参数的位置、值和类型，因此不需要 Fast Parser 再从 SQL 文本中寻找参数。

## Non-Prepared SQL 的完整执行流程

下面以普通文本 SQL 为例，说明一次执行从收到 SQL 到执行计划的完整过程。

```text
普通 Text SQL
    ↓
Fast Parser 快速提取 literal，生成参数化 SQL
    ↓
使用 Text mode、参数化 SQL 和会话环境查 Plan Cache
    ├─ 命中：匹配参数和依赖后执行缓存计划
    └─ 未命中：完整解析、参数化、优化并生成新计划
                    ↓
                 判断是否写入缓存
                    ↓
                 执行新计划
```

### 1. 快速提取参数

收到普通 SQL 后，OceanBase 首先进入 Text 模式。Fast Parser 扫描 SQL，生成参数化 SQL 和本次 literal 列表。

这一步只为快速查缓存准备输入，不代表已经确认每个 literal 都可以自由变化。某些特殊位置需要等完整解析后才能判断。

### 2. 定位候选缓存项

OceanBase 使用下面的信息定位候选缓存项：

- 参数化 SQL；
- Text cache mode；
- 当前 database；
- 会话和配置环境；
- connection collation 等影响 SQL 语义的信息。

找到相同的参数化 SQL 仍然不等于可以直接复用物理计划。一个 key 下可以有多组不同的 schema 依赖、参数约束和计划形态，后面还要继续匹配。

### 3. Cache hit 时验证能否复用

命中候选项后，OceanBase 依次确认：

- Fast Parser 提取的 literal 数量是否与首次完整解析一致；
- 不能自由参数化的位置是否仍然保持相同 token；
- schema、临时表和相关依赖是否变化；
- 参数类型、signed/unsigned、charset/collation 是否兼容；
- decimal precision/scale 和其他参数约束是否满足；
- 用户变量、权限和当前执行环境是否匹配；
- 当前应当选择哪一种物理计划。

全部匹配后，将本次参数填入缓存计划并执行。这个路径通常不需要再次构造完整 AST，也不需要重新优化。

任意一层不匹配时，不会错误复用当前计划，而是继续寻找其他 plan variant；仍然找不到时按 cache miss 处理。

### 4. Cache miss 时完整解析

cache miss 后进入正常的 hard parse 流程：

1. 使用完整 Parser 构造 AST，并确认 statement type；
2. 根据 AST 上下文判断每个 literal 是可以参数化，还是必须保持不变；
3. 对比 Fast Parser 与完整 Parser 识别的 literal 数量和顺序；
4. 完成名字解析、类型处理和权限相关信息收集；
5. 进入优化器生成物理计划。

Fast Parser 与完整 Parser 必须保持一致。如果两边识别出的 literal 数量、顺序或位置无法对齐，本次 SQL 仍然可以使用新生成的计划正常执行，但不会写入 Text Plan Cache。

### 5. 记录不能自由变化的 literal

某个 literal 不适合自由参数化时，OceanBase 通常不会直接拒绝整条 SQL，而是把这个位置记录为 not-param。

例如：

```sql
SELECT * FROM t ORDER BY 1;
```

`ORDER BY 1` 中的 `1` 可能表示输出列的位置，不能像 `WHERE a = 1` 中的过滤值一样自由替换。Text 模式会记录它的原始 token，后续命中时要求这个位置保持一致。

类似的特殊位置还包括：

- `GROUP BY`、`ORDER BY` 中的位置引用；
- hint、collation、cast 的部分参数；
- named window 和部分 window 定义；
- 日期格式字符串；
- `SUBSTR`、JSON 等函数中影响解释方式的参数；
- 影响返回类型、precision 或执行语义的位置。

这样可以让 SQL 的其他普通过滤值继续共享计划，同时避免特殊 literal 被错误复用。

### 6. 判断是否写入缓存

生成物理计划后，还要判断当前计划是否适合加入 Plan Cache，主要包括：

- Plan Cache 和当前 statement 的缓存开关是否允许；
- SQL 是否显式要求不使用 Plan Cache；
- 是否涉及当前不支持缓存的 dblink、hybrid search 等能力；
- 生成的计划是否可以安全复制和保存；
- schema、资源和计划形态是否满足缓存要求。

通过后，参数化 SQL、not-param 信息、参数约束、schema/权限依赖和物理计划一起写入 Text Plan Cache。没有通过也只是不写缓存，本次新计划仍然正常执行。

## Prepared SQL 的执行流程

Prepared 路径分为 Prepare 和 Execute 两个阶段。

### Prepare 阶段

Prepare 阶段主要做下面几件事：

1. 完整解析和名字解析；
2. 检查 statement 是否支持 Prepared 协议；
3. 检查 `?` 的数量是否超过限制；
4. 记录参数数量、位置、字段和 statement type 等 metadata；
5. 在开启 Prepared 自动参数化时，继续分析 SQL 中的固定 literal；
6. 保存 Execute 阶段组合参数和查找计划需要的信息。

Prepare metadata 和物理 Plan Cache 是两层不同的缓存。Prepare 成功不表示已经生成物理计划；物理计划通常在第一次 Execute 时生成。

### Execute 阶段

Execute 阶段主要做下面几件事：

1. 根据 statement ID 取得 Prepare metadata；
2. 检查客户端传入的参数数量并解码真实值和类型；
3. 合并客户端 `?` 参数和 Prepare 阶段提取的固定 literal；
4. 使用 Prepared cache mode 查找物理计划；
5. 匹配参数类型、collation、precision/scale、schema 和权限等约束；
6. 命中时执行缓存计划，未命中时重新生成并按条件写入 Prepared Plan Cache。

## 三种参数化模式分别做什么

### `TEXT_MODE`

处理完整的普通 SQL 文本。

主要职责是：

- 从 SQL 文本中提取 literal；
- 将完整 Parser 的结果和 Fast Parser 结果对齐；
- 判断哪些 literal 可以自由变化；
- 对不能参数化的位置保存原始 token；
- 为 Text Plan Cache 生成参数和匹配信息。

### `PS_PREPARE_MODE`

处理 Prepared SQL 的结构，此时客户端 `?` 还没有真实值。

主要职责是：

- 识别已有 `?` 的数量和位置；
- 判断 Prepared SQL 中的固定 literal 是否可以继续参数化；
- 记录两类参数的顺序和对应关系；
- 保存 Execute 时需要的参数化模板和特殊 literal 信息。

### `PS_EXECUTE_MODE`

处理本次 Execute 的真实参数。

主要职责是：

- 取得客户端传入的值和类型；
- 与 Prepare 阶段提取的固定 literal 组成完整参数列表；
- 根据真实值生成类型、collation 和参数约束；
- 为本次执行匹配或生成合适的 Prepared 计划。

三种模式共用参数化和 AST 遍历框架，但输入、生成的 metadata 和失败后的处理不同，并不是无差别地执行同一组逻辑。

## Prepared 也会参数化固定 literal

OceanBase 的 Prepared SQL 除了客户端 `?`，还可以包含固定 literal：

```sql
SELECT * FROM t WHERE c1 = 1 AND c2 = ?;
```

在 Prepared 自动参数化开启且语义安全时，Prepare 阶段可以继续提取 `c1 = 1` 中的 `1`。这样下面几条 Prepared SQL 有机会使用相同的参数化模板：

```sql
SELECT * FROM t WHERE c1 = 1 AND c2 = ?;
SELECT * FROM t WHERE c1 = 2 AND c2 = ?;
SELECT * FROM t WHERE c1 = ? AND c2 = ?;
```

这时需要保存：

- 哪些参数来自客户端 `?`；
- 哪些参数来自 Prepared SQL 的固定 literal；
- 两类参数在 Execute 时如何组成完整列表；
- 哪些固定 literal 不能继续参数化。

如果固定 literal 不适合继续参数化，通常只是缩小 Prepared SQL 之间的共享范围，并不表示 Prepared Plan Cache 整体不可用。原有客户端 `?` 仍然可以正常参与 PS 计划缓存。

## Prepared 和 Non-Prepared 参数化的主要区别

两条路径的语法安全规则大体共用，主要区别在参数来源、处理时机和匹配信息。

| 维度 | Non-Prepared / Text | Prepared |
| --- | --- | --- |
| 参数来源 | SQL 文本中的 literal | 客户端 `?`，以及可选的固定 literal |
| 参数化时机 | 每次执行 Text SQL 时 | Prepare 时分析结构，Execute 时填入真实参数 |
| Fast Parser | 需要，并与完整 Parser 对齐 | 不需要 |
| `?` 的含义 | 没有 Prepared 协议提供的参数值 | 客户端参数，值在 Execute 时提供 |
| 固定 literal | 主要参数来源 | 开启自动参数化时才继续提取 |
| 类型信息 | SQL literal 在解析时已有类型 | 客户端 `?` 的真实类型通常到 Execute 才知道 |
| not-param 表达 | 保存原 SQL token | 保存参数值、类型或 collation 等约束 |
| 无法继续参数化 | 通常缩小共享范围；无法表达时不写 Text 缓存 | 通常保留固定 literal，PS Plan Cache 仍可工作 |

例如：

```sql
-- Non-Prepared
SELECT * FROM t WHERE a = 1 AND b = 'x';

-- Prepared
SELECT * FROM t WHERE a = 1 AND b = ?;
```

Non-Prepared 的参数都来自当前 SQL 文本。Prepared 则同时存在固定 literal `1` 和客户端参数 `?`，需要先在 Prepare 阶段记录位置，再在 Execute 阶段与真实参数合并。

因此可以概括为：

> Non-Prepared 是从每条 SQL 文本中发现参数；Prepared 是在已有 `?` 的基础上，可选地继续抽取固定 literal，并在 Execute 时组合真实参数。

## 各入口自己的检查

### Text 入口

Text 路径特有的检查主要包括：

- 当前 statement type 是否进入 Text Plan Cache 主流程；
- Fast Parser 能否稳定生成参数化 SQL 和 literal 列表；
- Fast Parser 与完整 Parser 识别的常量数量和顺序是否一致；
- not-param 位置能否与本次原始 token 对应；
- Text literal 是否能用当前参数化和约束机制安全表达。

Text 路径主要覆盖核心 DML，另外对 `SHOW VARIABLES` 有单独支持。解析或执行本身合法，但不满足 Text Plan Cache 条件的 SQL，仍然正常执行，只是不写缓存。

### Prepare 入口

Prepare 阶段特有的检查主要包括：

- statement 是否支持 Prepared 协议；
- `?` 的数量是否超过上限；
- 参数字段和 returning 参数等协议 metadata 是否能正确构造；
- Prepared SQL 中的固定 literal 是否可以继续自动参数化；
- 原始 Prepared SQL 与参数化模板如何关联。

协议不合法或参数数量错误可能直接导致 Prepare 报错；固定 literal 不能继续参数化通常只会缩小共享范围。

### Execute 入口

Execute 阶段特有的检查主要包括：

- statement ID 和 Prepare metadata 是否存在；
- 客户端参数数量是否匹配；
- 参数值和类型能否正确解码；
- 客户端参数与固定 literal 能否组成完整参数列表；
- 当前 statement 是否进入 Prepared Plan Cache 主流程。

Prepared 的 statement type 范围略宽，特别是部分 `EXPLAIN` 和 SHOW family；核心 SELECT/DML 与 Text 路径重合。

### 两条路径共用的后端检查

进入计划生成或缓存匹配后，两条路径共用大量检查：

- Plan Cache 开关和 SQL hint；
- schema、临时表和依赖对象；
- 参数类型、signed/unsigned、charset/collation；
- decimal precision/scale 和其他参数约束；
- 用户变量、权限和会话环境；
- 当前物理计划是否适合缓存和复用。

这些检查不是集中在一个 AST checker 中，而是分布在计划生成、缓存写入和命中匹配阶段。

## Prepared 和 Non-Prepared 的支持范围

### 先区分协议支持和 Plan Cache 支持

Prepared 协议支持某条 SQL，不表示它的物理计划一定进入 Plan Cache。类似地，Text SQL 能正常执行，也不表示一定能写入 Text Plan Cache。

从 statement type 看：

| 路径 | 进入 Plan Cache 主流程的 statement |
| --- | --- |
| Text/Non-Prepared | 核心 DML，另外单独支持 `SHOW VARIABLES` |
| Prepared Execute | 核心 DML，以及部分 `EXPLAIN` 和 SHOW family |

Prepared 的入口范围略宽，但核心 SELECT/DML 是重合的。

### 查询结构上没有明显的两套 allowlist 断层

OceanBase 没有看到类似 TiDB 当前这样的情况：Prepared 使用较完整的 AST checker，而 Non-Prepared 再维护一份很窄的 AST allowlist，导致 HAVING、window、CTE 或多表 join 仅因为 checker 未覆盖而整体回退。

对核心 DML 来说，两条路径共用完整解析、参数化规则以及 not-param/参数约束的记录机制。某个结构能否最终复用计划，主要取决于：

- 特殊 literal 是否能通过 not-param 约束表达；
- schema、参数类型和权限能否匹配；
- 最终生成的物理计划是否适合缓存。

这不表示 HAVING、window、CTE、subquery 或多表 join 无条件都能命中，只表示它们不会因为 Text 路径额外维护的一份总 allowlist 而统一失败。

### 差异主要体现在共享粒度

当一个 literal 不能自由变化时：

- Text 模式通常保存原 token，要求下次仍然一致；
- Prepared 模式通常保存对应的值、类型或 collation 约束；
- Prepared 固定 literal 无法继续自动参数化时，可以继续使用原始 Prepared 模板。

所以很多差异表现为“哪些值可以共享同一计划”，而不是“整条 SQL 能不能使用 Plan Cache”。

## OceanBase 为什么不需要每次运行完整 AST checker

Text SQL 第一次 cache miss 时会把后续命中需要的信息保存下来，包括：

- 完整 Parser 识别出的 literal 数量；
- 不能参数化的位置和对应 token；
- 参数类型、charset/collation；
- 参数约束；
- schema、用户变量和权限依赖。

后续执行只需要用 Fast Parser 得到本次参数，然后逐项匹配这些信息。如果任意条件不满足，就查找其他 plan variant 或重新 hard parse。

关键不是“第一次检查通过后永远相信”，而是把检查结论转换成可缓存、可在 hit 路径重新验证的 metadata。

相比之下，TiDB 当前 Non-Prepared statement LRU 没有保存这么完整的约束和依赖。因此本次设计在 statement LRU hit 后继续运行 `IsASTCacheable`，是合理的保守选择。

## 与 TiDB 当前实现的主要差异

| 维度 | OceanBase | TiDB 当前实现 |
| --- | --- | --- |
| Text 参数化入口 | Fast Parser 提前生成 key，miss 后用完整 Parser 校验 | 基于已经构造的完整 AST 参数化 |
| 参数化规则 | 共用 mode-aware 的参数化框架，按上下文记录 not-param 和其他约束 | Prepared 和 Non-Prepared 前端逻辑分开 |
| 特殊 literal | 优先记录 not-param，缩小共享范围 | 很多情况直接让整条 Non-Prepared SQL bypass Plan Cache |
| AST 支持范围 | 没有明显的 Text 独立窄 allowlist | Non-Prepared checker 比 Prepared 明显更窄 |
| hit 时验证 | 匹配结构化 metadata 和依赖 | statement LRU 层保存的信息较少 |
| Text/Prepared 计划 | mode 隔离，不直接共享 | 本次改造也不要求共享实际计划 |

## 对 TiDB 方案的启发

### 1. 共用公共规则，但保留 Non-Prepared 前置逻辑

TiDB 可以让 Prepared 和 Non-Prepared 共用 `IsASTCacheable`，但不能把 Non-Prepared 原始 literal 的处理伪装成 Prepared 协议参数。

合理的职责边界是：

```text
Non-Prepared 前置逻辑
  → 判断普通 SQL literal 能否安全转换
  → 生成参数化 SQL 和参数
        ↓
公共 AST cacheability 检查
        ↓
公共 schema、参数类型和物理计划后端
```

### 2. Literal 处理和整条语句退出是两个层级

OceanBase 没有为每个 literal 定义 `parameterize`、`preserve`、`fallback` 三态枚举。更准确的理解是：

- Fast Parser 先抽取 literal 并生成参数化 key；
- 完整 Parser 根据 AST 上下文记录哪些位置属于 not-param，以及 `must_be_positive` 等额外约束；
- 后续无法建立安全参数映射或约束不匹配时，整条 Text SQL 查找其他 plan variant 或重新 hard parse。

因此，not-param 是 literal/参数位置上的属性，而退出当前缓存流程是 statement 级控制流，两者不应合并成 literal 的三个并列决策。

TiDB 第一阶段可以采用类似的职责划分，但实现方式更简单：literal 层只选择 `parameterize` 或 `preserve`；完成整棵 AST 的处理后，如果 restore、重新解析或参数映射仍无法保证正确，参数化接口再让整条语句 bypass Non-Prepared Plan Cache。不能仅因为公共 checker 没有拒绝，就默认某个 literal 可以参数化。

### 3. 当前不需要照搬三个 mode

TiDB Prepared 当前没有继续参数化 Prepared SQL 固定 literal 的需求，因此本次改造不必为了形式统一引入完整的 `TEXT/PS_PREPARE/PS_EXECUTE` mode 系统。

当前更重要的是：

- 把 Non-Prepared literal 的 parameterize/preserve 规则做完整；
- 保留 Prepared 和 Non-Prepared 不同的参数来源；
- 共用真正等价的 AST 和后端检查；
- 不要求两种协议立即共享 statement carrier 或物理计划。

如果未来 TiDB 也支持 Prepared SQL 固定 literal 的自动参数化，再引入类似 mode 会更有价值。

### 4. 如果未来引入 Fast Parser，必须设计一致性保护

TiDB 当前基于完整 AST 参数化，没有 OceanBase 的双 Parser 对齐问题。如果未来为了性能改成词法级快速参数化，需要同时设计：

- Fast Parser 和完整 Parser 的 literal 数量、顺序校验；
- 特殊 token 的原文保存；
- 失败后无副作用地回到完整解析；
- Parser 或参数化规则变化后的缓存失效机制。

### 5. 长期可以把检查结论结构化

TiDB 后续如果希望 statement LRU hit 时不再遍历完整 AST，需要先让 carrier 保存足够的信息，并在 hit 时重新验证：

- parser 和 session 语义上下文；
- 参数类型和特殊 literal 约束；
- schema、系统变量和权限依赖；
- 会影响 cacheability 的动态开关。

不能仅因为第一次检查通过，就永久复用这个结论。

## 源码索引

正文只描述职责和流程。下面集中列出关键源码入口，方便需要进一步核对实现的读者查阅：

- [普通 Text SQL 入口](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/ob_sql.cpp#L3010-L3120)
- [Text Plan Cache 查找和 Fast Parser](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_plan_cache.cpp#L575-L795)
- [Prepared Prepare 阶段](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/ob_sql.cpp#L1238-L1510)
- [Prepared Execute 阶段](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/ob_sql.cpp#L2583-L2710)
- [三种参数化 mode 和完整参数化流程](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_sql_parameterization.cpp#L387-L415)
- [参数化主流程](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_sql_parameterization.cpp#L1130-L1365)
- [特殊 literal 和 not-param 规则](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_sql_parameterization.cpp#L2251-L2505)
- [hard parse 后的缓存准入流程](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/ob_sql.cpp#L4796-L5148)
- [参数类型、collation 和约束匹配](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_plan_set.cpp#L69-L205)
- [not-param 和 schema 匹配](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_plan_cache_value.cpp#L1958-L2055)
- [Text/Prepared cache mode 隔离](https://github.com/oceanbase/oceanbase/blob/f05b2af69729379eae4612d1d5c2ce310ae093f0/src/sql/plan_cache/ob_plan_cache_struct.h#L39-L180)

## 最终判断

OceanBase 的设计不能概括为“Prepared checker 直接复用于 Non-Prepared”。更准确的描述是：

```text
Text：Fast Parser + Text 特有一致性检查 --------+
                                             |
Prepared：协议参数和 Prepare metadata --------+--> 共用参数化和约束规则
                                                   → 共用缓存准入骨架
                                                   → mode 隔离的 cache entry
                                                   → 共用参数、schema、权限和计划匹配
```

对 TiDB 最有价值的参考不是某个具体函数，而是这套职责边界：

- 共用语义规则和后端匹配；
- 保留 Non-Prepared 参数化前置逻辑；
- literal 层明确区分 parameterize 和 preserve，无法安全构造参数化语句时在 statement 层退出；
- 不要求两种协议立即共享 cache entry 或 physical plan；
- 长期把检查结论转换成 hit 路径可以重新验证的 metadata。
