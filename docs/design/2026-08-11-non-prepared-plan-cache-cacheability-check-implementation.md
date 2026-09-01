# Non-Prepared Plan Cache 支持范围改造实现说明

相关设计和调研：

- [组内设计文档](2026-08-11-non-prepared-plan-cache-cacheability-check.md)
- [其他数据库实现调研](2026-08-11-non-prepared-plan-cache-other-databases.md)
- [OceanBase 实现专项调研](2026-08-11-non-prepared-plan-cache-oceanbase.md)

## 文档目的

本文是《Non-Prepared Plan Cache 支持范围与 cacheability 检查改造》的配套实现文档，面向实际写代码和 review 的同学，重点回答：

- 当前调用链具体在哪里；
- 要新增哪些类型、函数和检查；
- 每个文件具体改什么、哪些旧代码可以删除；
- LRU hit/miss、错误、warning 和 metric 分别怎么处理；
- 测试应该落在哪些文件、覆盖哪些分支。

本文描述两个实现 PR：先独立修复 AST 恢复，再在灰度开关保护下接入参数化安全规则并复用公共 AST 规则。TiDB 当前通过参数化后调用 `IsASTCacheable` 落地规则复用，这是一项实现选择，不是改造目标本身。原始 literal 总数、`LIMIT` 和 `SELECT ... INTO` 等阶段性策略继续保留；特殊 literal 第一阶段采用 preserve-first，不要求立即把它们转换成 typed parameter，也不实现 OceanBase 风格的 not-param constraint matcher。

新流程由 `tidb_enable_non_prepared_plan_cache_unified_cacheability_check` 控制，默认关闭。关闭时继续使用原有 `NonPreparedPlanCacheableWithCtx`、原有参数化规则、原有支持范围以及原有 reason/metric 行为；打开后才使用本文描述的 parameterization precheck、context-aware parameterize/preserve 和公共 `IsASTCacheable` 流程。独立的 original AST 恢复修复不受开关控制。两条路径继续使用现有 statement LRU，本文不修改其 key 组成。

## 当前代码路径

入口在 `pkg/planner/optimize.go` 的 `getPlanFromNonPreparedPlanCache`。当前执行顺序是：

```text
getPlanFromNonPreparedPlanCache
    |
    |-- 检查总开关、trace、restricted SQL 等入口条件
    |
    |-- core.NonPreparedPlanCacheableWithCtx(original AST, InfoSchema)
    |       - DML / locking read 开关
    |       - Non-Prepared AST allowlist
    |       - 特殊 literal 和 literal 总数
    |       - 表、列类型和 LIMIT 等限制
    |
    |-- core.GetParamSQLFromAST(original AST)
    |       - ParameterizeAST 临时替换 ValueExpr
    |       - restore 参数化 SQL
    |       - RestoreASTWithParams 恢复 original AST
    |
    |-- SessionVars.GetNonPreparedPlanCacheStmt(paramSQL)
    |       |
    |       |-- miss: ParseParameterizedSQL
    |       |         SetParameterValuesIntoSCtx
    |       |         GeneratePlanCacheStmtWithAST(..., false, ...)
    |       |         AddNonPreparedPlanCacheStmt
    |       |
    |       `-- hit: 复用 PlanCacheStmt
    |
    `-- core.GetPlanFromPlanCache(..., isNonPrepared=true, ...)
```

实现时需要注意当前路径的几个细节：

1. `GeneratePlanCacheStmtWithAST` 的 `isPrepStmt=false` 分支直接设置 `cacheable=true`，注释假设 Non-Prepared 已经在调用前完成检查；
2. statement LRU miss 后不检查 `cachedStmt.StmtCacheable`，当前会无条件写入 LRU；
3. `GetParamSQLFromAST` 只有在 `ParameterizeAST` 成功后才恢复 AST，SQL restore 报错时 original AST 可能残留参数标记；
4. `NonPreparedPlanCacheableWithCtx` 的 unsupported counter 只在 visitor 执行结束后计数，fast check 的早退没有计数；
5. 后端 `GetPlanFromPlanCache` 已经负责参数写入、schema/MDL、权限、物理 plan key、类型匹配、range rebuild 和物理计划检查，本次不重复实现这些能力。

## 灰度开关

### 定义

新增 Boolean 系统变量：

```text
tidb_enable_non_prepared_plan_cache_unified_cacheability_check
```

定义如下：

| 属性 | 取值 |
| --- | --- |
| Scope | GLOBAL、SESSION |
| 默认值 | `OFF` |
| 动态修改 | 支持 |
| `OFF` | 使用改造前的 Non-Prepared checker、参数化规则和支持范围 |
| `ON` | 使用本文描述的新参数化和统一 cacheability 检查流程 |

这个变量只在 `tidb_enable_non_prepared_plan_cache=ON` 时有实际效果。它不替代 Non-Prepared Plan Cache 总开关，也不改变 `tidb_enable_non_prepared_plan_cache_for_dml`、`tidb_enable_plan_cache_for_param_limit` 等已有开关的产品语义。

变量命名使用 `unified_cacheability_check` 而不是 `new` 或版本号，表达它控制的是“Non-Prepared 参数化安全规则与公共 cacheability 规则收敛”这一稳定语义。

灰度和回滚示例：

```sql
-- 只为当前 session 打开新流程。
SET SESSION tidb_enable_non_prepared_plan_cache_unified_cacheability_check = ON;

-- 为之后新建的 session 设置默认值；不改变已经存在的 session。
SET GLOBAL tidb_enable_non_prepared_plan_cache_unified_cacheability_check = ON;

-- 发现问题时立即让当前 session 回到旧流程。
SET SESSION tidb_enable_non_prepared_plan_cache_unified_cacheability_check = OFF;
```

切换开关本身不产生 warning，也不增加 unsupported counter。是否 bypass、使用哪个 reason 和如何计数，由所选中的完整 legacy/unified 路径决定。

### 分流原则

`getPlanFromNonPreparedPlanCache` 先执行现有总开关、trace、restricted SQL、retry 和 multi-statement 等通用入口排除，再读取一次 session 开关值并选择整条调用链：

```text
getPlanFromNonPreparedPlanCache
    |
    |-- 通用入口排除项
    |
    |-- unified cacheability check = OFF
    |       `-- legacyNonPreparedPlanCachePath
    |               - NonPreparedPlanCacheableWithCtx
    |               - 原有参数化和 statement LRU 流程
    |               - 原有 reason、warning 和 unsupported metric 边界
    |
    `-- unified cacheability check = ON
            `-- unifiedNonPreparedPlanCachePath
                    - DML / locking read 入口 gate
                    - parameterization precheck
                    - context-aware parameterize/preserve
                    - LRU hit/miss 均执行 IsASTCacheable
                    - StmtCacheable admission check
```

不能只在某个 checker 调用点局部判断开关，否则关闭开关时仍可能使用新 parameterizer、新 reason 或新 metric，形成无法可靠回滚的混合路径。建议把旧路径和新路径拆成两个私有 helper，入口只负责共同 gate 和一次性分流。

### 与正确性修复的边界

下面一项属于独立正确性修复，对 `OFF` 和 `ON` 都生效：

- 所有退出路径都安全恢复 original AST；

因此“关闭时与原本一样”指 SQL 支持范围、cacheability 决策、bypass reason、warning 和 unsupported metric 保持旧行为，而不是重新启用已知的 AST 污染问题。

### 动态切换和现有 LRU

开关切换不修改 statement LRU key，也不要求为了切换开关清空整个 LRU。统一流程取得 carrier 后仍会重新运行 `IsASTCacheable`；如果新参数化规则生成了不同的 ParamSQL，则沿用现有 SQL 文本自然形成不同的 LRU entry。本文不承诺为 legacy/unified 两种模式分别保留 carrier。

后端物理 Plan Cache key 和现有失效机制保持不变。开关只控制前端调用流程；后端继续使用现有 backend key、参数类型、schema/MDL 和物理 checker 保护计划复用。

## 目标调用链

开关打开时，`getPlanFromNonPreparedPlanCache` 的新分支按下面的顺序执行：

```text
1. Non-Prepared 通用入口排除项
2. DML / locking read 开关
3. Non-Prepared parameterization precheck
4. 参数化，并恢复 original AST
5. 按 paramSQL 查现有 statement LRU
6. 获取 parameterized AST
   - hit: cachedStmt.PreparedAst.Stmt
   - miss: ParseParameterizedSQL
7. 每次调用 IsASTCacheable(parameterized AST, current InfoSchema)
8. miss 时构造 PlanCacheStmt
9. 只有 StmtCacheable=true 才写 statement LRU
10. 调用共用的 GetPlanFromPlanCache
```

关键顺序不能调换：

- precheck 必须在参数化之前，否则看不到原始 literal 的类型和数量；
- `IsASTCacheable` 必须在参数化之后，否则无法按参数标记的位置做公共检查；
- `IsASTCacheable` 必须在 LRU hit 和 miss 两条路径都执行，因为结果依赖当前 session 和 InfoSchema；
- miss 时必须先通过公共 checker，再构造和写入 carrier。

## 新增的参数化接口

### 返回结构

在 `pkg/planner/core/plan_cache_param.go` 增加：

```go
// NonPreparedPlanCacheParamResult is the result of parameterizing a normal SQL
// statement for the non-prepared plan cache.
type NonPreparedPlanCacheParamResult struct {
	ParamSQL    string
	ParamValues []types.Datum
}
```

字段只保留调用方真正需要的内容：

- `ParamSQL` 用于 parse、statement LRU 和后端 plan cache；
- `ParamValues` 用于本次执行的参数表达式；
- literal 数量、是否包含 `LIMIT` 等只属于 precheck 内部状态，不暴露给 planner。

### 主入口

同一文件增加：

```go
// ParameterizeForNonPreparedPlanCache validates and parameterizes stmt for the
// non-prepared plan cache. supported=false means the whole statement should
// bypass the non-prepared plan cache and use normal planning.
func ParameterizeForNonPreparedPlanCache(
	sctx base.PlanContext,
	stmt ast.StmtNode,
) (result NonPreparedPlanCacheParamResult, supported bool, reason string, err error)
```

返回值约定：

| 返回值 | 含义 | `getPlanFromNonPreparedPlanCache` 的处理 |
| --- | --- | --- |
| `supported=true, err=nil` | 整条语句参数化成功 | 继续 statement LRU 和公共 checker |
| `supported=false, err=nil` | 不满足 Non-Prepared 策略，或无法构造语义可靠的 ParamSQL | 记录 bypass reason，整条语句走普通优化 |
| `err!=nil` | original AST 恢复失败或其他不能安全继续的内部错误 | 返回错误，不使用可能已被修改的 AST |

这里的 `supported` 是整条参数化流程的结果，不是 literal 的第三种决策。不要用 `err` 表示“不支持缓存”；只要 original AST 已经安全恢复，这类情况就应当让整条语句正常 bypass。只有无法保证 original AST 可继续使用时才返回错误。

原 `GetParamSQLFromAST` 在灰度期必须保留给开关关闭的旧路径使用。新入口可以复用它的 AST 安全恢复基础设施，但不能改变旧入口选择 literal 的规则。只有开关默认打开并经过完整灰度、确认不再需要回滚后，才能另行决定是否删除或降为内部函数。

## 参数化 precheck 的实现

### 新 visitor

在 `plan_cache_param.go` 增加私有 visitor，例如：

```go
type nonPreparedPlanCachePrechecker struct {
	sctx         base.PlanContext
	supported    bool
	reason       string
	literalCount int
	maxLiteral   int
}
```

初始化时：

```go
checker := nonPreparedPlanCachePrechecker{
	sctx:       sctx,
	supported:  true,
	maxLiteral: getMaxParamLimit(sctx),
}
```

visitor 必须遍历 original AST 的全部子树，不能复用 `paramReplacer` 的 `skipChildren` 规则。

### `ValueExpr` 检查

遇到每个 `*driver.ValueExpr` 时都执行 `literalCount++`，超过 `getMaxParamLimit` 时返回 `query has too many constants`。计数针对 original AST 中的全部 literal，不是最终生成的参数数量。

precheck 不再因为 `UnderScoreCharsetFlag`、`KindBinaryLiteral` 或 `IsNull()` 统一拒绝整条 SQL。这些信息由后续 literal 处理逻辑使用：能够忠实保留原 AST 节点和完整 token 时采用 preserve；如果完成整棵 AST 的处理后仍无法保证 restore、重新解析或参数映射正确，则整条语句 bypass。

original AST 中已经存在的 `ParamMarkerExpr` 不属于本次 parameterization 生成的 marker。为避免恢复校验把原始 `?` 与新生成的 marker 混淆，precheck 发现任意原始 marker 时直接按 statement-level bypass 处理，reason 固定为 `query has parameter markers`，并保留原 AST 交给普通优化路径。

因此旧的三个特殊 literal reason 不再作为第一阶段的固定结果。只有整条语句实际需要 bypass 时才返回与具体限制对应的 reason，避免把“这个 literal 不应参数化”和“整条语句不能进入 Non-Prepared Plan Cache”混为一谈。

即使 literal 位于下面这些最终不会参数化的位置，precheck 也必须计数，后续处理逻辑也必须访问并明确选择 parameterize 或 preserve：

- `SELECT` field；
- `GROUP BY`、`ORDER BY`；
- `LIMIT/OFFSET`；
- 日期/时间函数的 format 参数；
- window frame。

### `LIMIT` 检查

遇到任意 `*ast.Limit`，如果 `EnablePlanCacheForParamLimit=false`，返回当前 reason：

```text
query has 'limit ?' is un-cacheable
```

这里保留的是产品开关语义，不是因为 literal 本身无法 preserve。开关关闭时继续让整条语句 bypass；开关打开时，第一阶段保留 literal `LIMIT/OFFSET`，后续再单独评估是否参数化。原因是 `IsASTCacheable` 只在看到 `ParamMarkerExpr` 时检查该开关，而 `NewPlanCacheKey` 又会对所有记录到的 `Limit` 检查开关；不显式处理会改变当前执行路径和 warning。

### `SELECT ... INTO`

遇到 `*ast.SelectStmt` 且 `SelectIntoOpt != nil` 时，将整条语句标记为 bypass。

旧 checker 会提前拒绝这类 statement。公共 `IsASTCacheable` 只看到 `SelectStmt`，不会检查 Prepared 协议限制；继续执行可能在 `GeneratePlanCacheStmtWithAST` 中返回 `ErrUnsupportedPs`，把原来的普通优化变成 SQL 错误。

这个规则属于 Non-Prepared carrier 构造前的兼容策略，先放在 precheck 中。后续如果要支持，应单独验证每种 `SELECT INTO` 类型，而不是随 checker 统一一起放开。

### DML 和 locking read

DML 开关不放进 visitor。在 `optimize.go` 入口保留一个私有检查：

```go
func nonPreparedPlanCacheDMLAllowed(
	vars *variable.SessionVars,
	stmt ast.StmtNode,
) (bool, string) {
	if vars.EnableNonPreparedPlanCacheForDML {
		return true, ""
	}
	switch stmt := stmt.(type) {
	case *ast.SelectStmt:
		if !containsLockingRead(stmt) {
			return true, ""
		}
	case *ast.SetOprStmt:
		if !containsLockingRead(stmt) {
			return true, ""
		}
	}
	return false, "not a SELECT statement"
}

type lockingReadFinder struct {
	found bool
}

func (finder *lockingReadFinder) Enter(in ast.Node) (ast.Node, bool) {
	if stmt, ok := in.(*ast.SelectStmt); ok && stmt.LockInfo != nil {
		finder.found = true
		return in, true
	}
	return in, false
}

func (*lockingReadFinder) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func containsLockingRead(node ast.Node) bool {
	if node == nil {
		return false
	}
	finder := lockingReadFinder{}
	node.Accept(&finder)
	return finder.found
}
```

开关关闭时 bypass 并走普通优化，不计 unsupported metric，保持它作为功能入口条件的语义；但 Explain Plan Cache 仍然要显示 `not a SELECT statement`，与当前行为一致。

## parameterize / preserve 规则

### 默认规则

开关打开、不再使用旧 allowlist 后，不能继续使用“没有特殊处理就参数化”的默认行为。实现必须保证：

```text
明确标记为 parameterize → 替换成参数标记
明确标记为 preserve     → literal 留在参数化 SQL 中
```

未知 AST 上下文第一阶段默认 preserve。实现上由表达式 selector 对已确认安全的表达式容器显式递归，未列入白名单的表达式节点直接跳过子树；因此新 AST 节点不会因为包含 `ValueExpr` 就被自动参数化。`fallback` 不是第三种 literal 决策：只有在所有 literal 处理完成后，发现某个 preserve 结构无法通过 restore 稳定进入 statement identity，或者完整 token 无法在相同解析上下文中重建，整条参数化流程才返回 `supported=false`。公共 `IsASTCacheable` 在参数化之后运行，不能代替这层判断。

建议在替换前增加一次 context-aware 遍历。它只用私有 set/map 记录需要 parameterize 的 `ValueExpr`；未被记录的 literal 默认 preserve。新流程使用单独的私有 replacer，或让底层 replacer 显式接收选择结果，只替换 set/map 中明确标记的节点。旧 `GetParamSQLFromAST` 仍使用 legacy selection，不能因为共用 `paramReplacer` 而被间接切换到新规则。遍历过程中如果发现 ParamSQL 无法忠实表达某个 preserve 结构，则在 statement 级记录 `supported=false` 和 reason，不需要定义 literal 级的 `fallback` action 或三态枚举。

第一阶段 preserve 的 literal 直接保留在 `ParamSQL` 中，因此 `ORDER BY 1` 和 `ORDER BY 2` 会形成不同的 LRU entry 和后端 `StmtText`。本次不实现额外的 not-param constraint matcher。

preserve 不能只理解为“visitor 跳过替换”。必须验证 restore 后完整 token 仍然存在。例如当前参数化 restore 使用 `RestoreStringWithoutCharset`，会省略字符串 charset；charset introducer 要么使用能够保留 introducer 的 restore 方式，要么让整条语句 bypass。

### Context matrix

第一阶段需要得到下面的矩阵：

| AST 上下文 | 处理 | 原因/实现 |
| --- | --- | --- |
| `WHERE`、join `ON` | parameterize | 典型运行时 filter value |
| `IN`、`BETWEEN` | parameterize | 后端已有 range rebuild 和参数类型保护 |
| HAVING | parameterize | 与普通 filter 相同；新增端到端测试 |
| `INSERT ... VALUES`、UPDATE assignment | parameterize | 本次参数值写入 `PlanCacheParams` |
| `SELECT` field 子树 | preserve | 保持输出列名和 metadata；沿用现有 skip |
| `GROUP BY`、`ORDER BY` 子树 | preserve | 避免位置引用、alias 和 `ONLY_FULL_GROUP_BY` 语义变化 |
| `LIMIT/OFFSET` | preserve | 不同 literal 保留在 `ParamSQL` 和 `StmtText`；沿用现有 skip |
| `NULL` | preserve | 避免丢失无类型 `NULL` 的原始语义 |
| BIT/HEX literal | preserve | 保留原 AST 节点和原始 token，不转换成普通整数或字符串参数 |
| charset introducer literal | preserve | introducer 与后面的字符串作为整体保留；不能拆开参数化 |
| `DATE_FORMAT` 等 format 参数 | 只参数化第一个参数 | 沿用现有特殊处理 |
| `WEIGHT_STRING(... AS CHAR/BINARY(n))` 的语法参数 | 只参数化第一个表达式，保留 `CHAR`/`BINARY` 和长度 | 后两个 AST 参数参与语法恢复，不能替换为 `?` |
| 构建期类型依赖参数：`CHAR ... USING` charset、`LPAD/RPAD` length、`CONVERT_TZ`/`UNIX_TIMESTAMP` datetime、`FROM_UNIXTIME` timestamp | preserve 影响签名或返回类型推导的参数 | 保证不同 charset、长度或时间精度不会复用首次构建的函数类型 |
| 时间精度和构建期类型参数：`TIME`、`TIMEDIFF`、`TIMESTAMP`、`NOW`/`CURRENT_TIMESTAMP`/`CURRENT_TIME`/`LOCALTIME`/`LOCALTIMESTAMP`/`UTC_TIMESTAMP`/`UTC_TIME`/`SYSDATE` | preserve 函数参数 | FSP 和首参数类型参与函数签名构建，不能在后续执行中只替换运行时参数 |
| `ROUND`/`TRUNCATE` 的值和 scale 参数 | preserve 函数参数 | 返回类型的 decimal scale 在构建期确定 |
| `RAND(seed)` 的 seed 参数 | preserve | 常量 seed 在构建期初始化 RNG，避免不同 seed 复用同一随机状态 |
| `ADDTIME`/`SUBTIME` 的参数 | preserve 整个函数参数列表 | 返回 FSP/Flen 在构建期由两个参数共同确定，不能复用不同精度的函数签名 |
| `BENCHMARK` 的 loop count 参数 | preserve 第一个参数，第二个表达式按常规规则递归处理 | 正数 loop count 会缓存到函数签名，避免不同循环次数复用首次构建状态 |
| window frame bound | preserve | 在新流程的 context selector/replacer 中处理；不改变 legacy replacer 行为 |
| CTE、derived table、set operation 内的 filter | parameterize | 不在 preserve 子树下时递归使用相同规则 |
| multi-table DML 的 filter/assignment | parameterize | 参数化规则先实现，端到端放开单独评审 |
| 未识别的 literal 上下文 | preserve | 不能自动参数化；如果最终无法稳定生成 ParamSQL，则整条语句 bypass |

需要为新进入的顶层 AST 结构增加 table-driven parameterizer 测试，至少覆盖：

- CTE 内外各有 literal；
- `UNION/INTERSECT/EXCEPT` 各分支有 literal；
- named/inline window spec 和 frame bound；
- derived table 和 correlated/uncorrelated subquery；
- multi-table UPDATE/DELETE；
- HAVING；
- 含 hint 的 statement。

测试要同时断言：

- 参数化 SQL 的形状；
- `ParamValues` 的数量和顺序；
- preserve 位置仍保留 literal；
- original AST restore 后与参数化前 restore 的 SQL 一致。

如果某个新上下文中的 literal 会影响语法结构或 metadata，应明确采用 preserve；如果现有 restore 无法忠实保留，则参数化接口返回 `supported=false`，让整条语句 bypass。切换到公共 checker 前必须完成这张矩阵，不能依赖“公共 checker 可能会拒绝”。

## original AST 的恢复

### 当前问题

当前 `GetParamSQLFromAST` 的逻辑是：

```go
paramSQL, params, err = ParameterizeAST(stmt)
if err != nil {
	return "", nil, err
}
// copy values
err = RestoreASTWithParams(stmt, params)
```

`ParameterizeAST` 先修改 AST，再调用 `stmt.Restore`。如果 `stmt.Restore` 失败，它返回的 `params` 为空，调用方无法恢复已经替换的节点。

### 建议改法

把“执行替换并持有原 ValueExpr”的主体抽成私有函数：

```go
func parameterizeAST(
	stmt ast.StmtNode,
) (paramSQL string, params []*driver.ValueExpr, err error)
```

它在 `stmt.Accept(paramReplacer)` 后立刻保存 `params`，然后再 restore SQL。即使 SQL restore 失败，也把已经替换的原节点列表返回给内部调用方。

`GetParamSQLFromAST` 使用该私有函数，并保证：

1. 只要发生过替换，就在退出前调用 `RestoreASTWithParams`；
2. 在恢复前复制每个参数的 `Datum`；
3. ParamSQL restore 失败时仍先恢复 AST；高层 `ParameterizeForNonPreparedPlanCache` 在恢复成功后返回 `supported=false`，让整条语句 bypass；
4. AST restore 失败时返回错误；如果 ParamSQL restore 也失败，在错误中同时保留两个原因；
5. 只有 marker 已经从 AST 中替换掉后，才能放回 `paramMakerPool`。

为了避免 `RestoreASTWithParams` 修改到一半才发现非法 offset，可以先做一次只读校验：所有待恢复 marker 的 offset 必须在 `[0, len(params))` 内。校验通过后第二次 visitor 才执行替换和对象归池。

`ParameterizeAST` 是低层 API，注释已经说明它可能修改输入。可以继续保留当前对外语义；Non-Prepared 生产路径必须只调用能够保证恢复的高层接口。

### 必须增加的失败路径测试

在 `plan_cache_param_test.go` 增加可注入 restore 错误的 AST 节点或测试 hook，验证：

- ParamSQL restore 失败但 AST 恢复成功时，返回 `supported=false, err=nil`；
- 上述 bypass 路径的 original AST 中不存在新增的 `ParamMarkerExpr`，仍可以正常 restore 和进入普通 optimizer；
- AST restore 失败时返回错误，不进入普通 optimizer；
- 连续再次参数化不会复用仍挂在 AST 上的池化 marker。

## `getPlanFromNonPreparedPlanCache` 的具体改造

### PR 2 的最终控制流

`pkg/planner/optimize.go` 中先保留共同入口 gate，然后一次性选择 legacy 或 unified 分支：

```go
func getPlanFromNonPreparedPlanCache(...) (...) {
	vars := sctx.GetSessionVars()

	// Keep the existing common switch, trace, restricted SQL, retry and
	// multi-statement entry exclusions unchanged.
	if !vars.EnableNonPreparedPlanCacheUnifiedCacheabilityCheck {
		return getPlanFromNonPreparedPlanCacheLegacy(ctx, sctx, stmt, is)
	}
	return getPlanFromNonPreparedPlanCacheUnified(ctx, sctx, stmt, is)
}
```

`getPlanFromNonPreparedPlanCacheLegacy` 从当前实现机械抽取，除 PR 1 的 AST 恢复修复外不改变控制流。新分支按下面的伪代码实现。伪代码省略了当前已有的 test hook，但实现时必须保留 hook 的相对位置。

```go
func getPlanFromNonPreparedPlanCacheUnified(...) (...) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	vars := sctx.GetSessionVars()

	if allowed, reason := nonPreparedPlanCacheDMLAllowed(vars, stmt); !allowed {
		recordNonPreparedPlanCacheBypass(stmtCtx, reason, false)
		return nil, nil, false, nil
	}

	result, supported, reason, err := core.ParameterizeForNonPreparedPlanCache(
		sctx.GetPlanCtx(), stmt,
	)
	if err != nil {
		return nil, nil, false, err
	}
	if !supported {
		recordNonPreparedPlanCacheBypass(stmtCtx, reason, true)
		return nil, nil, false, nil
	}

	paramExprs := core.Params2Expressions(result.ParamValues)
	value := vars.GetNonPreparedPlanCacheStmt(result.ParamSQL)

	var cachedStmt *core.PlanCacheStmt
	var paramStmt ast.StmtNode
	if value == nil {
		paramStmt, err = core.ParseParameterizedSQL(sctx, result.ParamSQL)
		if err != nil {
			// Treat a malformed parameterized SQL as a statement-level bypass.
			// Ordinary execution must keep the original AST path warning-free;
			// EXPLAIN FORMAT='plan_cache' reports this stable reason.
			recordNonPreparedPlanCacheBypass(stmtCtx, "failed to parse parameterized SQL", true)
			return nil, nil, false, nil
		}
	} else {
		cachedStmt = value.(*core.PlanCacheStmt)
		paramStmt = cachedStmt.PreparedAst.Stmt
	}

	cacheable, reason := core.IsASTCacheable(ctx, sctx.GetPlanCtx(), paramStmt, is)
	if !cacheable {
		recordNonPreparedPlanCacheBypass(stmtCtx, reason, true)
		return nil, nil, false, nil
	}

	if cachedStmt == nil {
		if err := core.SetParameterValuesIntoSCtx(
			sctx.GetPlanCtx(), true, nil, paramExprs,
		); err != nil {
			return nil, nil, false, err
		}

		cachedStmt, _, _, err = core.GeneratePlanCacheStmtWithAST(
			ctx, sctx, false, result.ParamSQL, paramStmt, is,
		)
		if err != nil {
			return nil, nil, false, err
		}
		if !cachedStmt.StmtCacheable {
			recordNonPreparedPlanCacheBypass(
				stmtCtx, cachedStmt.UncacheableReason, true,
			)
			return nil, nil, false, nil
		}
		vars.AddNonPreparedPlanCacheStmt(result.ParamSQL, cachedStmt)
	}

	plan, names, err := core.GetPlanFromPlanCache(
		ctx, sctx, true, is, cachedStmt, paramExprs,
	)
	if err != nil {
		return nil, nil, false, err
	}
	return plan, names, true, nil
}
```

### LRU hit 路径

hit 后必须从 `cachedStmt.PreparedAst.Stmt` 取得 parameterized AST，并对它重新运行 `IsASTCacheable`。

如果本次因为动态开关或当前 InfoSchema 被拒绝：

- 不调用 `GetPlanFromPlanCache`；
- 不删除 statement LRU entry，因为开关恢复后它可能再次可用；
- 本次返回 `ok=false`，让外层使用 original AST 普通优化。

### LRU miss 路径

miss 时顺序必须是：

1. parse parameterized SQL；
2. `IsASTCacheable`；
3. 把本次参数写入 session context；
4. `GeneratePlanCacheStmtWithAST(..., false, ...)`；
5. 检查 `StmtCacheable`；
6. 写入 statement LRU。

不能在第 2 步之前构造 carrier，否则公共 checker 拒绝的 statement 可能产生 Prepared warning、受到 `Fix49736` 强制放开的影响，或者进入 LRU。

如果第 5 步发现不可缓存，第一阶段让整条语句 bypass，回到 original AST 普通优化，不缓存 carrier。这个分支可能多做一次 PlanBuilder 工作，但只发生在 AST checker 通过、构造阶段又发现不可缓存的情况，优先保证语义清楚。后续如需优化，可以研究安全复用 `GeneratePlanCacheStmtWithAST` 返回的 plan。

### 保留现有 test hook

以下 hook 的位置不能在重构时丢失：

- `PlanCacheKeyTestIssue43667`：保持在参数化完成、statement LRU 查找前；
- `PlanCacheKeyTestIssue47133`：保持在 `GetPlanFromPlanCache` 返回 names 后。

先把现有函数机械抽取成 legacy helper 并用旧路径回归测试锁定行为，再实现 unified helper，比较容易确认 hook 和错误路径没有遗漏。两个 helper 都必须保留相应 hook；不要为了共享 hook 而重新合并两条流程。

## bypass reason、warning 和 metric

### Reason 选择

开关打开的新流程不保证具体 reason 文本与旧 checker 完全一致。实现必须保证同一个 session/InfoSchema 状态下选择结果确定，并按调用链采用第一个失败原因：

1. 入口策略，例如 DML/locking read 功能开关；
2. 参数化流程，例如 literal 上限或无法忠实构造 ParamSQL；
3. 公共 `IsASTCacheable`；
4. carrier 构造结果；
5. 物理计划检查。

普通执行不增加 warning；`EXPLAIN FORMAT='plan_cache'` 继续使用 `skip non-prepared plan-cache: <reason>` 的前缀。新路径测试应验证 reason 类别和优先级，不要求复制旧 checker 的检查顺序。开关关闭时继续使用旧路径原有 reason 和 warning，相关 golden 测试保持不变。

### 新 helper

在 `optimize.go` 增加私有 helper，例如：

```go
func recordNonPreparedPlanCacheBypass(
	stmtCtx *stmtctx.StatementContext,
	reason string,
	countUnsupported bool,
) {
	if countUnsupported {
		core_metrics.GetNonPrepPlanCacheUnsupportedCounter().Inc()
	}
	if stmtCtx.InExplainStmt &&
		stmtCtx.ExplainFormat == types.ExplainFormatPlanCache {
		stmtCtx.AppendWarning(errors.NewNoStackErrorf(
			"skip non-prepared plan-cache: %s", reason,
		))
	}
}
```

需要在 `optimize.go` 增加 `pkg/planner/core/metrics` 的别名 import。`pkg/planner/core/metrics/metrics.go` 中已有 counter 和 accessor，不需要修改指标定义。

### 计数边界

开关打开时建议统一成：

| 场景 | unsupported counter | warning |
| --- | --- | --- |
| Non-Prepared 总开关、trace、restricted SQL 等入口排除 | 不计 | 无 |
| DML/locking read 功能开关关闭 | 不计 | 仅 Explain Plan Cache |
| parameterization precheck 拒绝或 ParamSQL 无法忠实构造 | 计一次 | 仅 Explain Plan Cache |
| `IsASTCacheable` 拒绝 | 计一次 | 仅 Explain Plan Cache |
| carrier 构造后 `StmtCacheable=false` | 计一次 | 仅 Explain Plan Cache |
| 物理计划 checker 拒绝 | 沿用后端 metric | 沿用后端行为 |

这会让 unified 路径的 unsupported counter 覆盖范围比旧路径更完整：当前旧 checker 的 fast-check early return 没有计数。本设计接受打开开关后的可观测行为变化；开关关闭时继续保留旧 fast-check 的历史计数边界。灰度期间 dashboard 必须按开关启用范围解释数据，开启前后的 counter 绝对值不能直接比较。

`ParseParameterizedSQL` 在 statement-LRU miss 上失败时也属于参数化阶段的 statement-level bypass：普通执行不追加 parser 原始 warning，unsupported counter 增加一次；`EXPLAIN FORMAT='plan_cache'` 使用稳定 reason `failed to parse parameterized SQL`。这样解析失败不会把原始 AST fallback 与缓存前端的内部 parser 错误混在一起。

不要给 metric 增加 reason label。

## Statement LRU admission

本次沿用现有 statement LRU 的 key、容量和淘汰行为，不在 `SessionVars` 中新增 key builder，也不把 unified/legacy 模式编码进 key。参数化 SQL 仍通过现有 `AddNonPreparedPlanCacheStmt` / `GetNonPreparedPlanCacheStmt` 查找 statement carrier。

unified 路径需要遵守以下 admission 规则：

- hit 后重新执行 `IsASTCacheable`，不能直接复用 carrier 创建时的检查结论；
- miss 时只有通过公共 checker 且 `StmtCacheable=true` 的 carrier 才能写入现有 LRU；
- checker 拒绝或 carrier 不可缓存时直接走普通优化，不写入 LRU；
- 动态开关切换不依赖 LRU key 隔离，实际使用哪条前端流程由当前 session 开关决定。

现有 LRU 的参数化 SQL identity 和后端计划 key 不属于本次改造范围。若后续发现需要扩大 key 覆盖范围，应作为独立 correctness 变更评审，不能与本次 cacheability 职责调整混在一起。

## `GeneratePlanCacheStmtWithAST` 的处理

第一阶段不修改函数签名，也不增加新的公共 wrapper，减少对 Prepared 和 stored procedure 调用点的影响。

修改 `pkg/planner/core/plan_cache_utils.go` 中 Non-Prepared 分支的注释：

```go
if isPrepStmt {
	cacheable, reason = IsASTCacheable(...)
} else {
	// The non-prepared caller checks cacheability before building the
	// PlanCacheStmt.
	cacheable = true
}
```

不要改成两条路径都在这里调用 `IsASTCacheable`，原因有两个：

1. `Fix49736` 位于这个函数内部，会把 checker 的 false 强制改成 true；当前它不会覆盖 Non-Prepared 入口拒绝；
2. 不可缓存时这里使用 `skip prepared plan-cache` warning，文案不适合 Non-Prepared。

开关打开的 Non-Prepared 调用方在构造完成后仍需检查 `StmtCacheable`，因为 PlanBuilder 可能根据 partition processor 等信息把 statement 标记为不可缓存。开关关闭时保留旧调用方行为。

当 unified Non-Prepared 调用触发上述 partition processor 拒绝时，不在 `GeneratePlanCacheStmtWithAST` 内追加 `skip prepared plan-cache` warning；由调用方统一通过 `recordNonPreparedPlanCacheBypass` 处理，只在 `EXPLAIN FORMAT='plan_cache'` 下输出 Non-Prepared 前缀的 warning。Prepared 调用和开关关闭的 legacy Non-Prepared warning 行为保持不变。

## 灰度期保留旧 checker

为了保证开关关闭时回到原有行为，PR 2 不能删除 `pkg/planner/core/plan_cacheable_checker.go` 中的 legacy 实现，包括：

- `NonPreparedPlanCacheableWithCtx`；
- `nonPreparedPlanCacheableChecker`；
- `nonPrepCacheCheckerPool`；
- `isSelectStmtNonPrepCacheableFastCheck`；
- `nonPreparedPlanCacheableTableHints`；
- `extractTableNames`；
- `getColType`；
- 只服务这些符号的字段、方法和 import。

现有 `TestNonPreparedPlanCacheable` 和 `BenchmarkNonPreparedPlanCacheableChecker` 继续覆盖开关关闭的路径；同时新增：

- AST 公共规则由 `IsASTCacheable` 的单元测试承接；
- Non-Prepared 特有规则迁到 `plan_cache_param_test.go`；
- 新放开的 SQL 迁到 `plan_cache_test.go` 做端到端测试；
- benchmark 改成覆盖“precheck + parameterize + IsASTCacheable”的新热路径。

### `checkTableCacheable`

当前签名：

```go
func checkTableCacheable(
	ctx context.Context,
	sctx base.PlanContext,
	schema infoschema.InfoSchema,
	node *ast.TableName,
	isNonPrep bool,
) (bool, string)
```

灰度期保留 `isNonPrep` 参数和 legacy 专属的 view、非 normal table 拒绝分支。开关关闭的路径继续传 `true`；开关打开的路径通过公共 `IsASTCacheable` 使用 `false` 对应的公共 table 规则。最终仍有物理计划检查，例如 `PhysicalMemTable` 会被拒绝；但 view、system table 等必须用端到端测试确认实际落点，不能只依赖 checker 单测。

如果测试发现某类表只能在 Non-Prepared 拒绝，应在 parameterization precheck 中增加名字明确的策略函数，不要重新给公共 table checker 加 `isNonPrep` 布尔分支。

只有在后续版本将 unified 开关默认打开、完成灰度并正式移除 legacy 回滚能力后，才能在独立清理 PR 中删除上述 checker、测试和 `checkTableCacheable` 的 `isNonPrep` 参数。该清理不属于本文两个实现 PR。

保留以下公共/后端逻辑不变：

- `IsASTCacheable` 和 `cacheableChecker`；
- `getMaxParamLimit`；
- `isPlanCacheable`；
- `isPhysicalPlanCacheable`；
- generated column、partition、temporary table 等公共 table 检查。

## 各文件最终改动清单

### `pkg/planner/optimize.go`

新增：

- `getPlanFromNonPreparedPlanCacheLegacy`；
- `getPlanFromNonPreparedPlanCacheUnified`；
- `nonPreparedPlanCacheDMLAllowed`；
- `recordNonPreparedPlanCacheBypass`；
- `core_metrics` import。

修改：

- `getPlanFromNonPreparedPlanCache` 在共同入口 gate 后读取新开关并选择完整的 legacy/unified 分支；
- legacy 分支继续调用 `NonPreparedPlanCacheableWithCtx` 和 `GetParamSQLFromAST`；
- unified 分支调用 `ParameterizeForNonPreparedPlanCache`；
- unified 分支在 LRU hit/miss 都调用 `IsASTCacheable`；
- unified miss 时检查 `StmtCacheable` 后再写 LRU；
- 两个分支都保留两个现有 test hook。

### `pkg/planner/core/plan_cache_param.go`

新增：

- `NonPreparedPlanCacheParamResult`；
- `ParameterizeForNonPreparedPlanCache`；
- `nonPreparedPlanCachePrechecker`；
- context-aware literal 遍历和待参数化节点 set/map；
- 参数化内部 helper；
- restore 前的 marker 校验；
- `FrameBound` preserve 规则。

修改：

- `GetParamSQLFromAST` 所有路径恢复 original AST；
- 新流程的 replacer 只替换 set/map 中明确选择 parameterize 的 literal，legacy replacer 的选择规则不变；
- pool 对象只在 AST 不再引用后归还。

暂不修改：

- `Params2Expressions` 继续从 `Datum` 推导参数类型；
- `ParseParameterizedSQL` 的 parser 配置来源。

### `pkg/planner/core/plan_cacheable_checker.go`

灰度期保留旧 Non-Prepared checker、allowlist、辅助代码和 `checkTableCacheable` 的 `isNonPrep` 分支，作为开关关闭时的完整回滚路径。公共 `IsASTCacheable` 需要按 AST 作用域识别 CTE 名称，避免把 CTE 引用当作物理表查询 InfoSchema；CTE 定义内部访问的真实表仍必须执行原有 table cacheability 检查。

### `pkg/planner/core/plan_cache_utils.go`

更新 Non-Prepared 已由调用方检查的注释；unified Non-Prepared carrier 拒绝时抑制 prepared 前缀的 partition warning，由 unified 调用方输出正确 warning。Prepared checker、Prepared warning 和 `Fix49736` 流程不变。

### `pkg/sessionctx/variable/session.go`

新增 `EnableNonPreparedPlanCacheUnifiedCacheabilityCheck` session 字段。`AddNonPreparedPlanCacheStmt` / `GetNonPreparedPlanCacheStmt` 继续使用现有实现和 key，不新增 key builder，不把 unified/legacy 模式写入 key；只需确认 unified 路径遵守新的 LRU admission 规则。

### `pkg/sessionctx/variable/tidb_vars.go`

新增 `TiDBEnableNonPreparedPlanCacheUnifiedCacheabilityCheck` 常量和默认值 `DefTiDBEnableNonPreparedPlanCacheUnifiedCacheabilityCheck=false`。

### `pkg/sessionctx/variable/sysvar.go`

注册 `tidb_enable_non_prepared_plan_cache_unified_cacheability_check`，类型为 Boolean，scope 为 GLOBAL 和 SESSION，默认 `OFF`；`SetSession` 写入对应 SessionVars 字段。不把它声明为 `SET_VAR` hint 可修改，避免单条 statement 在 planner 流程中途改变前端模式。

### 测试文件

- `pkg/planner/core/plan_cache_param_test.go`：precheck、参数化矩阵、AST restore 和 benchmark；
- `pkg/planner/core/plan_cacheable_checker_test.go`：保留旧 checker 专属测试，补公共 checker 必要用例；
- `pkg/planner/core/plan_cache_test.go`：分别覆盖开关关闭的兼容行为，以及开关打开后的支持范围、warning/reason 和不同参数顺序；
- `pkg/sessionctx/variable/session_test.go`：保留现有 statement LRU 行为测试；新增或扩展 unified 路径的 hit/miss、`StmtCacheable=false` 不写入 LRU 和容量/淘汰行为测试；
- 相应 sysvar 测试：默认 `OFF`、GLOBAL/SESSION 设置和新 session 继承；
- 必要时扩展 `pkg/planner/core/plan_cache_partition_table_test.go`：partition mode 动态切换。

### `pkg/planner/MAINTAINER_GUIDE.md`

更新：

- 改造目标是 Non-Prepared 支持范围和公共 cacheability 规则收敛；
- unified cacheability 开关默认关闭，关闭时走 legacy 前端路径，正确性修复由两条路径共享；
- TiDB 当前通过 Prepared/Non-Prepared 调用 `IsASTCacheable` 复用公共 AST 规则；
- Non-Prepared parameterization precheck 的职责；
- statement LRU 沿用现有 key，本次只调整 carrier admission；
- statement LRU 只接收通过公共 checker 且 `StmtCacheable=true` 的 carrier；
- 为什么第一阶段每次执行公共 checker。

## 端到端测试设计

### 开关兼容和切换用例

所有已有 Non-Prepared Plan Cache 回归用例在不设置新变量时继续以默认 `OFF` 运行，锁定旧 checker 的支持范围、reason、warning 和 metric。对本文新增或改变支持范围的用例显式执行：

```sql
SET tidb_enable_non_prepared_plan_cache = ON;
SET tidb_enable_non_prepared_plan_cache_unified_cacheability_check = ON;
```

至少增加以下对照：

- 默认值以及新建 session 继承的值为 `OFF`；
- 同一条 HAVING、window、CTE 或三表 join SQL 在 `OFF` 时保持旧路径 bypass，在 `ON` 时进入新流程并按行为矩阵验证最终落点；
- 同一条包含多个拒绝条件的 SQL 在 `OFF` 时保持旧 reason/metric，在 `ON` 时使用新优先级；
- 在 statement LRU entry 已存在时执行 `OFF -> ON -> OFF` 和 `ON -> OFF -> ON`，验证每次执行都按当前开关选择对应前端流程；不要求通过 LRU key 隔离 legacy/unified carrier；
- 在两种模式下分别验证 original AST 恢复和统一 admission 规则；
- 总开关为 `OFF` 时，新开关无效果，仍完全跳过 Non-Prepared Plan Cache。

测试不能只断言 `@@last_plan_from_cache`。还要验证 statement LRU hit/miss（通过现有 hook 或可观察测试点）、Explain reason、unsupported counter 增量和 SQL 结果，证明切换的是完整前端流程。

### 兼容用例

开关打开时，以下场景执行两次后 `@@last_plan_from_cache` 仍应为 0：

- DML 开关关闭时的 INSERT/UPDATE/DELETE；
- DML 开关关闭时的 `SELECT ... FOR UPDATE`；
- literal 总数超过 Fix44823 对应上限；
- param-limit 开关关闭时的 `LIMIT/OFFSET`；
- `SELECT ... INTO`；
- `ignore_plan_cache()`、用户变量、不可缓存函数；
- 公共 checker 或物理 checker 明确拒绝的 statement。

`NULL`、BIT、HEX 和 charset introducer 不再因为“不能参数化”而统一让整条语句 bypass。参数化单测需要断言能够 preserve 的类别完整保留在 `ParamSQL` 中、不进入 `ParamValues`，并验证 ordinary literal 与这些 preserve literal 混合时的参数顺序。charset introducer 还要覆盖当前 `RestoreStringWithoutCharset` 的问题：修正 restore 后验证完整 token；如果第一阶段不能修正，则明确断言整条语句 bypass。端到端测试再根据公共 AST 和物理计划结果断言最终落点；至少选择一组普通 SELECT 用例验证相同 SQL 的第二次执行可以命中。

同时验证普通执行没有新增 warning；`EXPLAIN FORMAT='plan_cache'` 包含 `skip non-prepared plan-cache: <reason>`。

增加包含多个拒绝条件的用例，确认 reason 优先级：入口策略 → 参数化安全 → 公共 AST → carrier → 物理计划。unsupported counter 用测试确认入口 gate 不计数、precheck/公共 AST/carrier 各计一次，物理计划拒绝不重复计数。

### 行为矩阵和分阶段用例

开关打开、不再使用旧 allowlist，只表示 SQL 可以进入后续检查，不表示每一类都必须命中物理 Plan Cache。测试按下面的矩阵分别断言最终落点：

| SQL 类别 | parameterizer 预期 | AST checker 预期 | 最终预期 | 第一阶段 |
| --- | --- | --- | --- | --- |
| HAVING | filter value parameterize | 按公共规则 | 普通计划可命中 | 放开 |
| window | frame 等结构值 preserve | 按公共规则 | 视物理计划 | 放开已确认位置 |
| subquery | 内外 literal 分别分类 | 受开关控制 | 视 subquery plan | 开关打开时验证 |
| CTE、set operation | 各分支按相同规则分类 | 按公共规则 | 视物理计划 | 分类放开 |
| 三表以上 join | filter value parameterize | 按公共规则 | 普通 join 可命中 | 放开 |
| multi-table UPDATE/DELETE | filter/assignment parameterize | 可能通过 | 写入正确性需要单独验证 | 后续阶段 |
| view | 按表达式位置分类 | 可能通过 | 验证 schema 依赖 | 验证后决定 |
| system table | 按表达式位置分类 | 可能通过 | `PhysicalMemTable` 通常拒绝 | 不要求命中 |
| JSON/ENUM/SET/BIT 类型列的 filter | literal 按形式和上下文分类 | 可能通过 | 验证类型和 range 保护 | 单独验证 |

“按表达式位置分类”表示表类型本身不是 parameterize/preserve 决策；决策针对 SQL 中的 literal。表和最终计划由 AST/physical checker 处理。

预期可命中的类别每类至少准备两个参数值，按 `A -> B` 和 `B -> A` 两种顺序执行。预期物理阶段拒绝的类别需要断言：

- 参数化和公共 checker 的落点符合矩阵；
- `@@last_plan_from_cache` 保持为 0；
- Explain Plan Cache 显示物理阶段 reason；
- SQL 结果与关闭 Plan Cache 时一致。

每类都比较：

1. 关闭 Plan Cache 的结果；
2. Prepared Plan Cache 的结果；
3. Non-Prepared miss 的结果；
4. Non-Prepared hit 或预期 bypass 的结果；
5. output field metadata、warning、affected rows 和表中最终数据。

### 动态状态用例

在 statement LRU entry 已存在时切换：

- `EnableNonPreparedPlanCacheUnifiedCacheabilityCheck`，并验证当前执行按开关选择对应前端流程，不依赖 key 隔离；
- `EnablePlanCacheForSubquery`；
- `EnablePlanCacheForParamLimit`；
- partition prune mode；
- Fix44823、Fix33031、Fix45798 等相关 fix-control；
- schema：普通表和 view/temporary table 等属性变化。

确认当前执行重新运行公共 checker，而不是沿用 carrier 创建时的结论。

## 建议实现顺序

### PR 1：独立正确性修复

1. 修复 `GetParamSQLFromAST` 的 original AST 恢复；
2. 增加失败路径和 AST 恢复单元测试。

这个 PR 不引入新 precheck，不改变 checker、支持范围、reason 或 metric，便于独立 backport 和排查。

### PR 2：完成新流程切换

1. 注册默认 `OFF` 的 unified cacheability 系统变量并加入 SessionVars；
2. 把当前实现机械抽取为 legacy helper，用现有测试锁定关闭时的行为；
3. 增加 parameterization precheck 和新返回接口；
4. 增加 context-aware literal 遍历、新流程专用的节点选择/replacer，以及特殊 literal、window frame 等 preserve 规则；
5. 增加 unified helper，把 DML/locking read 开关放到该分支入口；
6. unified 分支参数化后在 LRU hit/miss 都调用 `IsASTCacheable`；
7. unified 分支增加 bypass helper、固定 reason 优先级和 `StmtCacheable` LRU admission 检查；
8. 接受并记录 unified 分支 unsupported metric 的新计数语义；
9. 保留 `NonPreparedPlanCacheableWithCtx`、旧 allowlist 及其测试作为回滚路径；
10. 补齐开关切换、参数化矩阵、端到端差分测试和 benchmark；
11. 更新 `MAINTAINER_GUIDE.md`，运行 targeted tests、benchmark、`make fmt` 和 `make check`。

为了便于 review，这个 PR 内部按 commit 分层：先提交 sysvar 和 legacy helper 抽取，再提交 precheck、literal 处理规则和单元测试，然后接入 unified 分支，最后补 reason、metric、端到端测试、benchmark 和文档。生产代码不串联旧 checker 与新 precheck，而是在入口选择完整路径。

### 后续清理 PR：移除 legacy 路径

这不是本文实现 PR 的一部分。只有在 unified 路径经过灰度、默认值已经切换为 `ON`、兼容性和性能观测达到要求，并且产品决定不再保留即时回滚能力后，才能：

1. 删除 legacy helper 和 `NonPreparedPlanCacheableWithCtx`；
2. 删除旧 allowlist、辅助代码、测试和 benchmark；
3. 简化 `checkTableCacheable`；
4. 决定删除系统变量，或保留为仅兼容读取且不再改变行为的 deprecated 变量。

## 实现阶段待确认事项

下面这些事项是 PR 2 完成前必须关闭的实现和验证项，不能仅以 parameterizer 或 AST checker 返回通过作为完成标准。

### charset introducer 的完整恢复

当前参数化 restore 使用 `RestoreStringWithoutCharset`，会丢失 charset introducer。第一阶段采用 statement-level bypass：precheck 发现 introducer 后返回 `supported=false, err=nil`，保证不会生成语义不完整的 `ParamSQL`，并且 original AST 保持不变。

测试至少覆盖 introducer 与普通 filter literal 混合的语句，断言 `ParameterizeForNonPreparedPlanCache` 返回 `supported=false, err=nil`，普通执行不新增 warning，`EXPLAIN FORMAT='plan_cache'` 显示稳定的 bypass reason。后续只有在 restore 能完整保留 token 后，才能把该策略改成 literal-level preserve。

### 未识别上下文默认 preserve 的缓存碎片

未知 literal 上下文默认 preserve 是第一阶段的正确性边界，但不同 literal 会形成不同的参数化 SQL identity 和后端 `StmtText`，从而降低归一化比例并增加缓存碎片。现有 statement LRU 会按这些 SQL identity 查找 entry，本次不调整其 key 组成。

新增 benchmark 时需要包含 literal 值持续变化的未识别上下文，除参数化本身的 `ns/op` 和 allocation 外，还要记录生成的 ParamSQL identity 数量以及 statement/plan cache 的命中情况，并与已明确 parameterize 的 filter 场景对照。第一阶段不以命中率下降为由放宽默认规则；后续只在补齐语义测试后，逐类把上下文从 preserve 改为 parameterize。

### 分阶段验证最终缓存落点

CTE、set operation、window、subquery、view 和 system table 等结构通过 parameterization 和 `IsASTCacheable`，只表示可以进入下一阶段，不表示最终一定能够写入或命中物理 Plan Cache。测试必须分别断言 parameterizer、公共 AST checker、carrier 和物理 checker 的落点，避免把物理阶段 bypass 误判为支持范围改造失败，也避免把 AST checker 通过误判为已经支持缓存。

对于预期物理阶段拒绝的类别，仍需验证 SQL 结果、field metadata、warning 和动态 schema/session 状态与关闭 Plan Cache 时一致，并确认 statement LRU 中不存在 `StmtCacheable=false` 的 carrier。

### multi-table DML 的放开边界

第一阶段可以实现 multi-table UPDATE/DELETE 中 filter 和 assignment 的参数化规则，但在写入正确性、affected rows、warning、trigger/constraint 等行为完成独立评审和端到端差分测试前，不正式允许它们进入 Non-Prepared Plan Cache。

如果这些验证不属于当前 PR，入口或 parameterization precheck 必须保留明确的 statement-level bypass gate，并提供稳定 reason；不能依赖公共 AST checker 或物理 checker 偶然拒绝。正式移除 gate 时，应单独提交行为变更和测试，至少比较关闭 Plan Cache、Prepared miss/hit、Non-Prepared miss/hit，以及不同参数执行顺序下的最终数据。

### LRU hit 重跑公共 checker 的性能

每次 statement LRU hit 都运行 `IsASTCacheable` 是动态 session 和 InfoSchema 正确性的要求，第一阶段不能通过缓存 checker 结论来规避。实现完成后需要增加或扩展 Non-Prepared hit-path benchmark，分别覆盖公共 checker 通过和因动态状态拒绝的路径，记录 `ns/op`、allocation 和相对改造前的变化。

如果开销明显，需要先用 profile 确认热点，再评估 checker 内部无状态、无语义变化的优化；不能把依赖 session、InfoSchema 或 fix-control 的检查结果存入 `PlanCacheStmt` 长期复用。

## 本地验证命令

按仓库测试规范启用 failpoint 后执行 targeted tests：

```bash
make failpoint-enable

pushd pkg/planner/core
go test -run 'Test(NonPreparedPlanCache|.*UnifiedCacheability.*|.*ParamSQL.*|.*Parameteriz.*)' -tags=intest,deadlock
go test -run '^$' -bench '^(BenchmarkNonPrepared|BenchmarkUnifiedNonPrepared|BenchmarkParameterize|BenchmarkGetParamSQL)' -tags=intest,deadlock
popd

pushd pkg/sessionctx/variable
go test -run 'Test(NonPreparedPlanCacheStmt|.*NonPreparedPlanCacheUnifiedCacheability.*)' -tags=intest,deadlock
popd

make failpoint-disable
make fmt
make check
```

如果修改了 partition 相关用例，再单独运行对应测试名，不运行整个 workspace。

## Review 时重点检查

- 新开关是否为 GLOBAL/SESSION Boolean 且默认 `OFF`，总开关关闭时是否无效果；
- 开关关闭时是否完整走 legacy checker、参数化、reason、warning 和 metric 路径；
- 开关打开时是否完整走 unified 路径，而不是与 legacy checker 串联；
- 动态切换时是否按当前开关选择 legacy 或 unified 前端，而不是依赖 LRU key 隔离；
- PR 1 的 AST 恢复正确性修复是否在两个模式下都生效；
- original AST 在所有 return path 上是否保持不变；
- precheck 是否遍历了 preserve 位置的 literal；
- LRU hit 是否也运行 `IsASTCacheable`；
- checker false 是否在构造 carrier 和进入后端之前返回；
- `StmtCacheable=false` 的 carrier 是否没有写入 LRU；
- statement LRU 是否只接收通过公共 checker 且 `StmtCacheable=true` 的 carrier；
- 未识别的 literal 上下文是否默认 preserve；无法忠实生成 ParamSQL 时是否在 statement 级 bypass；
- 普通执行是否没有新增 Prepared warning；
- `Fix49736` 是否仍不能覆盖 Non-Prepared 入口拒绝；
- unified 路径绕过旧 allowlist 后，每类新增行为是否有端到端测试；
- bypass reason 是否遵守入口策略、参数化流程、公共 AST、carrier、物理计划的优先级；
- unsupported metric 是否按文档中的新计数边界实现。

完成以上改动后，代码中的职责应当是清楚的：新开关决定选择 legacy 还是 unified 前端；legacy helper 保留旧支持范围和可观测行为；unified 路径中的 `plan_cache_param.go` 决定 literal 应当 parameterize 还是 preserve，并给出整条语句能否安全生成 ParamSQL 的结果；`IsASTCacheable` 决定参数化 AST 是否满足公共 plan cache 规则；unified helper 负责两者的编排和 statement 级 bypass；`GetPlanFromPlanCache` 继续负责两条路径共用的后端。
