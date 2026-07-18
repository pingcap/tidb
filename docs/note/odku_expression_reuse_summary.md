# ODKU Expression Reuse 优化完整总结

## Diff 范围

本文档基于当前工作区相对 `origin/feature/release-8.5-materialized-view` 的完整 diff 总结。

当前代码改动覆盖 12 个文件，另有本文档记录设计、验证和风险：

| 文件 | 作用 |
| --- | --- |
| `pkg/sessionctx/variable/tidb_vars.go` | 新增系统变量名和默认值 |
| `pkg/sessionctx/variable/session.go` | 在 `SessionVars` 中保存开关状态 |
| `pkg/sessionctx/variable/sysvar.go` | 注册 session/global 变量 |
| `pkg/sessionctx/variable/setvar_affect.go` | 允许该变量通过 hint 更新 |
| `pkg/planner/core/planbuilder.go` | 在构建 INSERT ODKU 时按复杂度创建并传递 memo |
| `pkg/planner/core/expression_rewriter.go` | 在 AST rewrite 阶段复用公共表达式 |
| `pkg/planner/core/resolve_indices.go` | 在 ResolveIndices 阶段复用 expression 子树 |
| `pkg/planner/core/common_plans.go` | 在 `Insert` 计划上记录当前语句是否实际启用复用 |
| `pkg/expression/scalar_function.go` | 新增按已解析参数重建 `ScalarFunction` 的 helper |
| `pkg/expression/builtin.go` | 新增 builtin signature 按参数重建的内部 helper |
| `pkg/expression/scalar_function_test.go` | 覆盖 `CloneWithArgs` 的语义 |
| `pkg/planner/core/planbuilder_test.go` | 覆盖 ODKU 表达式复用行为并新增 benchmark |

整体 diff 规模会随 benchmark 和文档调整略有浮动。

## 问题背景

真实业务里的 `INSERT ... ON DUPLICATE KEY UPDATE` 语句可能包含大量结构高度重复的 assignment 表达式。典型形态类似：

```sql
col_a = IF(common_condition, values(col_a), col_a),
col_b = IF(common_condition, values(col_b), col_b),
col_c = IF(common_condition, values(col_c), col_c)
```

其中 `common_condition` 可能非常长，例如多次出现：

```sql
values(c1) is not null
and (c1 <= values(c1) or c1 = '2100-01-01 00:00:00')
and values(c2) != 1
```

在原实现中，每个 assignment 都会独立经历两类成本：

1. AST rewrite 阶段重复把相同 SQL 子表达式改写成 expression tree。
2. 物理计划 `ResolveIndices` 阶段重复 clone 并解析相同 expression 子树。

这些重复工作在普通 SQL 里通常不明显，但 ODKU 这种“几十个 assignment 共用同一个判断条件”的场景会放大编译期开销，主要表现为：

- planner 编译耗时高。
- `expressionRewriter.Leave`、`newFunctionWithInit`、`binaryOpToExpression` 等路径 CPU 占比高。
- `ResolveIndices` 和 `ScalarFunction.Clone` 带来大量额外分配。

## 总体解决思路

这次优化按两个阶段处理重复工作：

1. 在 AST rewrite 阶段复用相同 SQL 子表达式。
2. 在 `ResolveIndices` 阶段继续复用已经构造出的 expression 子树。

同时新增一个 session/global 开关控制该优化，默认开启：

```sql
tidb_enable_odku_expression_reuse = ON
```

如果关闭该开关，或者当前 ODKU 表达式足够简单，ODKU 路径会回到原先逐个 assignment 独立 rewrite / resolve 的行为，尽量避免额外性能成本和语义风险。

## AST Rewrite 阶段复用

### 入口

在 `PlanBuilder.buildInsert` 里，如果 `tidb_enable_odku_expression_reuse` 开启，并且当前 ODKU assignment 中的 function-like AST 节点数量达到阈值，会为当前 INSERT 创建一个 `odkuExprMemo`，并传给 `rewriteInsertOnDuplicateUpdate`。

这个 memo 只在当前 INSERT 的 ODKU rewrite 生命周期内使用，rewrite 完成后会清空 frame 状态，不跨语句复用。

当前有一个简单的复杂度门控：

- 只统计会被 rewrite 成 scalar function 的表达式节点，例如 `IF`、`CAST`、二元比较、逻辑运算、`IS NULL`、`LIKE` 等。
- 不把单独的 `VALUES(col)` 计入复杂度，因为这类表达式很便宜，通常没有必要为了它启用 memo。
- 如果所有 RHS 都只是简单的 `VALUES(col)`、列引用、常量或 `DEFAULT`，会直接跳过 AST visitor 预扫描，尽量贴近原始路径成本。
- 当统计数量低于阈值时，即使 session 开关为 `ON`，当前语句也不会启用 ODKU expression reuse。

### Memo Key

AST rewrite 阶段的 key 来自原始 AST 子树的 restore 文本。实现上，`odkuExprMemo` 内部复用了一个 `bytes.Buffer` 和一个 `RestoreCtx`，每次构造 key 时只重置 buffer，不重复分配临时写入缓冲。

使用 restore text 的原因是：

- 不依赖 AST 指针是否相同。
- 能识别不同 assignment 中语义相同、文本结构相同的子表达式。
- 对 ODKU 里的重复 `IF(common_condition, values(col), col)` 场景有效，尤其能复用 `common_condition` 这一类公共部分。

### 安全过滤

不是所有表达式都允许 memo。当前实现跳过以下类型或标记：

- 参数 marker。
- 子查询表达式。
- 聚合函数。
- 窗口函数。
- 变量表达式。
- 带 subquery / aggregate / window / variable / param marker flag 的表达式。
- `IN (subquery)`。
- `SetCollationExpr` 及其子树。

另外，在 `Leave` 阶段写入 memo 之前，还会通过 `expression.IsMutableEffectsExpr` 过滤带 mutable side effects 的 expression。

这样做的目的是避免把有上下文依赖、执行副作用、参数依赖或特殊 collation 处理的表达式错误复用。

### 栈帧机制

`expressionRewriter` 是 visitor 模型，rewrite 过程中 expression 会通过 `ctxStack` 传递。为了安全复用，新增了 `odkuMemoFrame`：

- `stackLen` 用来校验当前 AST 子树 rewrite 后是否正好向 stack push 了一个 expression。
- `hit` 表示命中 memo，命中后直接把缓存 expression push 到 `ctxStack`，并跳过子节点遍历。
- `valid` 表示当前节点可以 memo。
- `disableMemo` 用于处理需要禁用 memo 的嵌套区域。

只有在 `Leave` 时确认 stack 状态符合预期，才会把当前 expression 写入 memo。

## ResolveIndices 阶段复用

AST rewrite 阶段复用后，多个 assignment 可以共享同一个 expression 子树。但是后续 `Insert.ResolveIndices` 仍然会遍历 expression 并把 column 的 `Index` 解析到对应 schema 上。

因此 `ResolveIndices` 阶段也需要配套处理，否则会重新打散共享结构或重复解析同一棵树。

### 独立 memo

如果当前 INSERT 在 rewrite 阶段实际启用了 ODKU expression reuse，`Insert.ResolveIndices` 中会使用两个 memo：

```go
tableResolveMemo
onDupResolveMemo
```

它们分别用于：

- `tableResolveMemo`：解析目标表 schema 上的 column，例如 assignment 左侧 column 和 generated column 表达式。
- `onDupResolveMemo`：解析 `Schema4OnDuplicate` 上的 ODKU 表达式。

这里不能共用一个 memo，因为两个 schema 的 column index 语义不同。

### 指针级复用

`resolveExprIndicesWithMemo` 使用 expression 指针作为 key。也就是说：

- AST rewrite 阶段已经共享出来的 expression 子树，在 resolve 阶段会命中同一个 memo。
- 没有共享出来的 expression 子树，不会额外通过文本或 hash 做跨树匹配。

这个边界比较保守，能避免 resolve 阶段引入新的语义匹配逻辑。

## 避免 ScalarFunction 深拷贝开销

最初的 resolve memo 仍然有一个明显问题：遇到 `ScalarFunction` 时，如果先调用 `ScalarFunction.Clone()`，会通过 builtin 的 `Clone()` 深拷贝旧的 `args` 子树，然后再把子节点替换成 resolve 后的结果。

也就是说，即使子节点已经通过 memo 复用了，外层函数 clone 仍然会先复制一遍旧参数树。

这次改成：

1. 先递归 resolve 子节点。
2. 再调用 `ScalarFunction.CloneWithArgs`，用已经 resolve 好的 args 重建函数节点。

### `ScalarFunction.CloneWithArgs`

新增 helper 的目标是：

- 保留原函数名。
- 保留返回类型。
- 保留 hash code / canonical hash code。
- 保留 charset / collation / coercibility / repertoire。
- 不复制旧参数树。
- 使用传入的新 args 作为函数参数。

### builtin signature 重建

builtin signature 不能简单复用原对象，因为原对象包含 `args` 和一些依赖参数的缓存状态。当前实现通过内部 helper：

- 浅拷贝 concrete builtin signature。
- 替换 args。
- 重置 `bufAllocator`。
- 重置 `childrenVectorized` 和 `childrenVectorizedOnce`。
- clone collator，避免共享内部可变状态。

这样能避免重新走 `NewFunction` 路径，因为 `NewFunction` 会重新做类型推导、collation 推导、NULL flag 调整、constant folding 和特殊函数初始化，既更贵，也可能改变语义。

## 开关与兼容性

新增变量：

```sql
tidb_enable_odku_expression_reuse
```

属性：

- Scope: `GLOBAL | SESSION`
- 类型: bool
- 默认值: `ON`
- 支持 session 级关闭，用于规避潜在兼容风险。
- 已加入 hint updatable verified map，可以通过相关 hint 更新。

当开关为 `OFF`，或当前 ODKU 表达式没有达到复杂度阈值时：

- `PlanBuilder.buildInsert` 不创建 `odkuExprMemo`。
- `Insert.ResolveIndices` 走原始逐表达式 `ResolveIndices` 逻辑。
- 尽量不引入额外 map / memo / key 构造成本。

## 测试与 Benchmark

### 单元测试

新增或扩展的测试包括：

1. `TestInsertOnDuplicateUpdateReusesCommonSubExpressions`

覆盖：

- 开关开启时，两个 assignment 中相同条件表达式在 rewrite 后共享同一个 expression 子树。
- `ResolveIndices` 后，共享关系仍然保留。
- 开关关闭时，不发生共享，行为回到原路径。

2. `TestScalarFunctionCloneWithArgs`

覆盖：

- `CloneWithArgs` 保持函数语义。
- clone 后的 args slice 不受调用方后续修改影响。
- 原始 expression 不被污染。
- hash code 和 canonical hash code 保持一致。

3. `TestInsertOnDuplicateUpdateExpressionReuseComplexityGate`

覆盖：

- 纯 `VALUES(col)` 赋值不会启用复用。
- 带重复复杂条件的 ODKU assignment 会达到复杂度阈值并启用复用。

4. `TestInsertOnDuplicateUpdateExpressionReuseSetVarHint`

覆盖：

- `INSERT /*+ SET_VAR(tidb_enable_odku_expression_reuse=off) */ ... ON DUPLICATE KEY UPDATE` 能对当前语句关闭复用。
- `INSERT /*+ SET_VAR(tidb_enable_odku_expression_reuse=on) */ ... ON DUPLICATE KEY UPDATE` 能对当前语句开启复用。
- `SET_VAR` hint 生效后会恢复原 session 变量值。

### Benchmark

新增 benchmark：

```go
BenchmarkInsertOnDuplicateUpdateCompile
```

benchmark 使用匿名化的表结构和 ODKU 语句，包含大量重复 `IF(common_condition, values(col), col)` assignment，用来衡量 planner 编译阶段的耗时和分配。

本地最新结果：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `1,138,725 ns/op` | `653,013 B/op` | `7,373 allocs/op` |
| `reuse_on` | `458,384 ns/op` | `267,122 B/op` | `2,175 allocs/op` |

对比收益：

- 时间减少 `680,341 ns/op`，约 `59.7%`。
- 内存分配减少 `385,891 B/op`，约 `59.1%`。
- 分配次数减少 `5,198 allocs/op`，约 `70.5%`。
- 整体编译耗时约提升 `2.5x`。

针对纯 `VALUES(col)` assignment 的简单 ODKU case，新增了独立 benchmark：

```go
BenchmarkInsertOnDuplicateUpdateCompileValuesOnly
```

在复杂度门控前，本地曾观察到这类 case 开启复用会略有回退：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `16,400 ns/op` | `19,088 B/op` | `164 allocs/op` |
| `reuse_on` | `20,667 ns/op` | `19,848 B/op` | `177 allocs/op` |

这个结果说明，简单 `VALUES(col)` ODKU 没有足够的公共子表达式收益，应该通过复杂度门控回到原始路径。

复杂度门控后，同一个 benchmark 的最新结果为：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `15,332 ns/op` | `19,112 B/op` | `165 allocs/op` |
| `reuse_on` | `15,461 ns/op` | `19,112 B/op` | `165 allocs/op` |

## 已执行验证

本地执行过：

```bash
go test ./pkg/expression -run TestScalarFunctionCloneWithArgs -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build go test ./pkg/planner/core -run 'TestInsertOnDuplicateUpdate(ReusesCommonSubExpressions|ExpressionReuseComplexityGate)' -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build go test ./pkg/planner/core -run '^$' -bench 'BenchmarkInsertOnDuplicateUpdateCompile($|ValuesOnly)' -benchmem -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build make bazel_lint_changed
```

另外，为确认 diff 基准，执行过：

```bash
git fetch origin feature/release-8.5-materialized-view
```

## 风险与注意点

### 语义风险

AST rewrite 阶段通过 restore text 判断相同子树，因此必须确保不会复用带上下文依赖或副作用的表达式。当前已经过滤参数、变量、子查询、聚合、窗口函数、mutable effects 和特殊 collation 子树，但后续如果新增特殊表达式类型，需要考虑是否也应该加入过滤。

### 内部结构依赖

`CloneWithArgs` 依赖当前 builtin signature 的结构约定，即 concrete builtin 内部包含 `baseBuiltinFunc`，并且参数和 vectorized 缓存状态可以通过统一 helper 重置。如果后续 builtin 增加更多与 args 强相关的内部缓存，需要同步检查该 helper。

### HashCode 风险

`CloneWithArgs` 目前会复制原函数已有的 hash code / canonical hash code。这个在当前使用场景下是合理的，因为 resolve 只改变 column 的 `Index`，不改变表达式语义和 `UniqueID`，hash code 本身也主要基于语义身份。但如果未来某个调用点用 `CloneWithArgs` 替换成语义不同的 args，就不能沿用这一假设。

### 默认开启风险

变量默认开启可以让真实 ODKU 场景直接受益，但也意味着新逻辑会进入线上默认路径。为降低风险，保留了 session/global 开关，发现异常时可以快速关闭。

### 未完成验证

当前没有跑更大范围的 planner package 测试或 integration test。

## 总结

这套改动的核心目标是降低 ODKU 大量重复 assignment 表达式带来的 planner 编译成本。实现上分为两层：

1. AST rewrite 阶段用 restore text memo 复用重复 SQL 子表达式。
2. ResolveIndices 阶段用 expression pointer memo 保留共享子树，并通过 `CloneWithArgs` 避免不必要的深拷贝。

从当前 benchmark 看，优化后 ODKU 编译时间、内存分配和分配次数都有明显下降，收益主要来自重复表达式 rewrite 减少，以及 resolve 阶段避免重复 clone / resolve 公共子树。
