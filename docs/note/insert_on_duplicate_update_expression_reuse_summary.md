# INSERT ... ON DUPLICATE KEY UPDATE Expression Reuse 优化完整总结

## Diff 范围

本文档基于当前工作区相对 `origin/feature/release-8.5-materialized-view` 的完整 diff 总结。

当前 diff 覆盖 planner、expression、session variable、测试和文档几个部分：

| 文件 | 作用 |
| --- | --- |
| `pkg/sessionctx/variable/tidb_vars.go` | 新增系统变量名和默认值 |
| `pkg/sessionctx/variable/session.go` | 在 `SessionVars` 中保存开关状态 |
| `pkg/sessionctx/variable/sysvar.go` | 注册 session/global 变量 |
| `pkg/sessionctx/variable/setvar_affect.go` | 允许该变量通过 hint 更新 |
| `pkg/planner/core/planbuilder.go` | 在构建 `INSERT ... ON DUPLICATE KEY UPDATE` 时按复杂度创建并传递 memo |
| `pkg/planner/core/expression_rewriter.go` | 在 AST rewrite 阶段复用公共表达式 |
| `pkg/planner/core/resolve_indices.go` | 在 ResolveIndices 阶段复用 expression 子树 |
| `pkg/planner/core/common_plans.go` | 在 `Insert` 计划上记录当前语句是否实际启用复用 |
| `pkg/planner/core/plan_cache_utils.go` | 将 `ON DUPLICATE KEY UPDATE` expression reuse 开关纳入 plan cache key |
| `pkg/expression/builtin.go` | 为 builtin signature clone 提供 `cloneFromWithArgs` 基础能力 |
| `pkg/expression/builtin_clone_with_args.go` | 为每个 concrete builtin signature 提供显式 `CloneWithArgs` |
| `pkg/expression/scalar_function.go` | 提供 `ScalarFunction.CloneWithArgs`，用于按 resolved args 重建函数 shell |
| `pkg/planner/core/planbuilder_test.go` | 覆盖 `ON DUPLICATE KEY UPDATE` 表达式复用行为并新增 benchmark |
| `pkg/planner/core/plan_cache_test.go` | 覆盖 plan cache key 随 `ON DUPLICATE KEY UPDATE` expression reuse 开关变化 |

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

这些重复工作在普通 SQL 里通常不明显，但 `ON DUPLICATE KEY UPDATE` 这种“几十个 assignment 共用同一个判断条件”的场景会放大编译期开销，主要表现为：

- planner 编译耗时高。
- `expressionRewriter.Leave`、`newFunctionWithInit`、`binaryOpToExpression` 等路径 CPU 占比高。
- `ResolveIndices` 和 `ScalarFunction.Clone` 带来大量额外分配。

## 总体解决思路

这次优化按两个阶段处理重复工作：

1. 在 AST rewrite 阶段复用相同 SQL 子表达式。
2. 在 `ResolveIndices` 阶段继续复用已经构造出的 expression 子树。

同时新增一个 session/global 开关控制该优化，默认开启：

```sql
tidb_enable_on_duplicate_expression_reuse = ON
```

如果关闭该开关，或者当前 `ON DUPLICATE KEY UPDATE` 表达式足够简单，该路径会回到原先逐个 assignment 独立 rewrite / resolve 的行为，尽量避免额外性能成本和语义风险。

## AST Rewrite 阶段复用

### 入口

在 `PlanBuilder.buildInsert` 里，如果 `tidb_enable_on_duplicate_expression_reuse` 开启，并且当前 `ON DUPLICATE KEY UPDATE` assignment 中的 function-like AST 节点数量达到阈值，会为当前 INSERT 创建一个 `onDuplicateExprMemo`，并传给 `rewriteInsertOnDuplicateUpdate`。

这个 memo 只在当前 INSERT 的 `ON DUPLICATE KEY UPDATE` rewrite 生命周期内使用，rewrite 完成后会清空 frame 状态，不跨语句复用。

当前有一个简单的复杂度门控：

- 只统计会被 rewrite 成 scalar function 的表达式节点，例如 `IF`、`CAST`、二元比较、逻辑运算、`IS NULL`、`LIKE` 等。
- 不把单独的 `VALUES(col)` 计入复杂度，因为这类表达式很便宜，通常没有必要为了它启用 memo。
- 如果所有 RHS 都只是简单的 `VALUES(col)`、列引用、常量或 `DEFAULT`，会直接跳过 AST visitor 预扫描，尽量贴近原始路径成本。
- 当统计数量低于阈值时，即使 session 开关为 `ON`，当前语句也不会启用 `ON DUPLICATE KEY UPDATE` expression reuse。

### Memo Key

AST rewrite 阶段的 key 来自原始 AST 子树的 restore 文本。实现上，`onDuplicateExprMemo` 内部复用了一个 `bytes.Buffer` 和一个 `RestoreCtx`，每次构造 key 时只重置 buffer，不重复分配临时写入缓冲。

为了避免对收益很低或代价不稳定的 AST 子树生成 restore key，每个 assignment rewrite 前会先做一次轻量预扫描：

- 只允许 function-like AST 节点生成 key，例如 `IF`、二元比较、逻辑运算、`IS NULL`、`CAST`、`LIKE` 等。
- 单独的 `VALUES(col)`、列引用、常量、`DEFAULT` 不生成 key。它们自身 rewrite 成本很低，但生成 restore key 和做 map lookup 仍然有额外开销。
- 预扫描会计算以每个 AST 节点为根的表达式子树深度，只有 depth 不超过 `16` 的节点才允许生成 key。
- depth 超过阈值时，只跳过当前子树的 key generation，子节点仍然正常 rewrite；如果子节点本身足够浅，仍然可以参与 memo 复用。

这个 depth guard 只影响优化是否启用，不影响 SQL 语义。它的目的只是限制深层表达式反复 `Restore` 可能带来的编译期成本放大。

使用 restore text 的原因是：

- 不依赖 AST 指针是否相同。
- 能识别不同 assignment 中语义相同、文本结构相同的子表达式。
- 对 `ON DUPLICATE KEY UPDATE` 里的重复 `IF(common_condition, values(col), col)` 场景有效，尤其能复用 `common_condition` 这一类公共部分。

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

`expressionRewriter` 是 visitor 模型，rewrite 过程中 expression 会通过 `ctxStack` 传递。为了安全复用，新增了 `onDuplicateMemoFrame`：

- `stackLen` 用来校验当前 AST 子树 rewrite 后是否正好向 stack push 了一个 expression。
- `hit` 表示命中 memo，命中后直接把缓存 expression push 到 `ctxStack`，并跳过子节点遍历。
- `valid` 表示当前节点可以 memo。
- `disableMemo` 用于处理需要禁用 memo 的嵌套区域。

只有在 `Leave` 时确认 stack 状态符合预期，才会把当前 expression 写入 memo。

## ResolveIndices 阶段复用

AST rewrite 阶段复用后，多个 assignment 可以共享同一个 expression 子树。但是后续 `Insert.ResolveIndices` 仍然会遍历 expression 并把 column 的 `Index` 解析到对应 schema 上。

因此 `ResolveIndices` 阶段也需要配套处理，否则会重新打散共享结构或重复解析同一棵树。

### 独立 memo

如果当前 INSERT 在 rewrite 阶段实际启用了 `ON DUPLICATE KEY UPDATE` expression reuse，`Insert.ResolveIndices` 中会使用两个 memo：

```go
tableResolveMemo
onDupResolveMemo
```

它们分别用于：

- `tableResolveMemo`：解析目标表 schema 上的 column，例如 assignment 左侧 column 和 generated column 表达式。
- `onDupResolveMemo`：解析 `Schema4OnDuplicate` 上的 `ON DUPLICATE KEY UPDATE` 表达式。

这里不能共用一个 memo，因为两个 schema 的 column index 语义不同。

### 指针级复用

`resolveExprIndicesWithMemo` 使用 expression 指针作为 key。也就是说：

- AST rewrite 阶段已经共享出来、并作为 `ResolveIndices` 入口出现的 expression，会在 resolve 阶段命中同一个 memo。
- 没有共享出来的 expression 子树，不会额外通过文本或 hash 做跨树匹配。

这个边界比较保守，能避免 resolve 阶段引入新的语义匹配逻辑。

### 为什么 ON DUPLICATE KEY UPDATE 可以安全共享

这个优化强依赖 `ON DUPLICATE KEY UPDATE` assignment 的特殊性。对同一个 `INSERT ... ON DUPLICATE KEY UPDATE` 来说，所有 assignment expression 都在同一个语义环境下 rewrite，并且后续都使用同一个 `Schema4OnDuplicate` 做 ResolveIndices。也就是说：

- 表达式能看到的 column 集合是固定的。
- 相同 column name / `VALUES(col)` 在不同 assignment 里的解析目标一致。
- ResolveIndices 使用的 schema 不会随 assignment 改变。

因此，如果两个 assignment 中出现完全相同且无副作用的表达式，把它们 rewrite 成同一个 expression subtree，并在 resolve 阶段复用同一个 resolved expression，是安全的。

这个前提不应直接泛化到跨 schema 的场景。例如普通 predicate pushdown、join condition rewrite 或其他可能在不同 input schema 下 resolve 的表达式，即使 SQL 文本相同，也可能解析到不同 column index 或不同 side 的 column，不能简单按 restore text 或 expression pointer 共享。

## 避免 ScalarFunction 深拷贝开销

普通 `Expression.ResolveIndices` 的语义是先 clone expression，再在 clone 出来的树上解析 column index。这个语义对通用调用方更安全，但在 `ON DUPLICATE KEY UPDATE` expression reuse 的场景里会抵消一部分收益：

- AST rewrite 阶段已经让多个 assignment 共享相同 expression。
- 如果 resolve 阶段继续 clone，`ScalarFunction.Clone()` 会通过 builtin 的 `Clone()` 深拷贝旧的 `args` 子树。
- 对大量重复条件来说，这会重新产生一批本来可以避免的临时 expression 分配。

当前实现为每个 concrete builtin signature 提供显式 `CloneWithArgs` 能力：

```go
func (sf *ScalarFunction) CloneWithArgs(args []Expression) Expression
```

这个入口会 clone 当前 `ScalarFunction` 和 builtin signature 的 metadata，但不会 deep clone 原来的旧 `args` 子树；它会 shallow-copy 调用方传入的 resolved args slice。`resolveExprIndicesWithMemo` 因此可以按以下方式工作：

- memo key 使用原始 expression 指针。
- memo value 是 clone-and-resolved 后的新 expression。
- 遇到 `ScalarFunction` 时，先递归 resolve children，并在递归过程中查询 memo。
- 当前 scalar function 只 clone 自己的 shell/metadata，children 使用递归返回的 resolved args。

这样可以保留普通 `Expression.ResolveIndices` 不修改输入 expression tree 的语义，同时避免 `ScalarFunction.Clone()` 对已经共享的旧 child subtree 做重复深拷贝。

`CloneWithArgs` 的 concrete builtin signature 方法集中放在 `builtin_clone_with_args.go`，每个方法复用对应 signature 的现有 clone metadata 逻辑，并把 `cloneFrom` 替换成 `cloneFromWithArgs`。这样能避免反射式浅拷贝，同时保留各 builtin signature 对特殊字段的显式处理。

## 开关与兼容性

新增变量：

```sql
tidb_enable_on_duplicate_expression_reuse
```

属性：

- Scope: `GLOBAL | SESSION`
- 类型: bool
- 默认值: `ON`
- 支持 session 级关闭，用于规避潜在兼容风险。
- 已加入 hint updatable verified map，可以通过相关 hint 更新。

当开关为 `OFF`，或当前 `ON DUPLICATE KEY UPDATE` 表达式没有达到复杂度阈值时：

- `PlanBuilder.buildInsert` 不创建 `onDuplicateExprMemo`。
- `Insert.ResolveIndices` 走原始逐表达式 `ResolveIndices` 逻辑。
- 尽量不引入额外 map / memo / key 构造成本。

该开关也会进入 plan cache key。这样当用户切换 `tidb_enable_on_duplicate_expression_reuse` 时，prepared plan cache / non-prepared plan cache 不会继续命中另一种开关状态下生成的 cached plan。

## Plan Cache 下的收益边界

这次优化主要作用在 plan cache miss / compile path。也就是说，它降低的是以下阶段的成本：

- `PlanBuilder.buildInsert` 中 `ON DUPLICATE KEY UPDATE` assignment 的 expression rewrite。
- `Insert.ResolveIndices` 中 `ON DUPLICATE KEY UPDATE` expression 的 index resolve。
- rewrite / resolve 过程中重复 `NewFunction`、类型推导、collation 推导、deep clone 旧 expression subtree 的成本。

对于普通 session plan cache，如果 cached plan 命中，TiDB 会直接复用当前 session plan cache 中的 `PlanCacheValue`，不会重新执行 `ON DUPLICATE KEY UPDATE` rewrite / ResolveIndices。因此：

- plan cache miss 时，这次优化可以降低生成 cached plan 的成本。
- plan cache hit 时，本来就不会重新编译 `ON DUPLICATE KEY UPDATE` expression，所以 `resolveExprIndicesWithMemo` 不会再次运行，也不需要再次运行。
- 如果 workload 主要命中 session plan cache，这次优化的收益主要体现在首次 prepare / 首次 cache miss / cache invalidation 后重新 build plan 的场景。

对于 instance plan cache，情况有所不同。instance plan cache 的 cached plan 会跨 session 共享，因此命中后会调用 `CloneForInstancePlanCache` 克隆一份 plan，避免多个 session 并发读写同一个 cached plan。当前 `Insert.CloneForPlanCache` 会通过 `CloneAssignments` 克隆 `OnDuplicate`，而 `Assignment.Clone` 会调用 `Expr.Clone()`。这意味着：

- instance plan cache hit 不会重新执行 `ResolveIndices`，所以 `resolveExprIndicesWithMemo` 对 hit path 没有直接收益。
- `Expr.Clone()` 是普通 deep clone，不会使用 `ON DUPLICATE KEY UPDATE` resolve memo，也不会保留 build 阶段在多个 assignment 间形成的 expression sharing。
- 这次优化仍然能降低 instance plan cache miss 时生成 cached plan 的成本，但不能消除 instance plan cache hit 时 clone cached plan 的成本。

如果后续要继续优化 instance plan cache hit path，可以考虑给 plan cache clone 增加 expression clone memo。设计上可以在一次 plan clone 过程中维护：

```go
map[expression.Expression]expression.Expression
```

clone expression 时先查 memo，命中则复用本次 clone 已经生成的新 expression；未命中才真正 clone，并把 old expression 到 new expression 的映射写入 memo。这样可以达到两个目标：

- cloned plan 不和 cached plan 共享 expression，仍然满足跨 session 并发安全要求。
- cloned plan 内部可以保留 cached plan 中已有的公共子表达式共享关系，减少 instance plan cache hit 时的 clone CPU 和 alloc。

这个 follow-up 的适用范围不应只限 `ON DUPLICATE KEY UPDATE`，但需要谨慎评估所有 physical plan clone 路径中 expression 是否允许在同一个 cloned plan 内共享，尤其是执行期可能被修改的 expression、plan cache parameter、runtime cache、hash code / canonical hash code 等内部状态。

## 测试与 Benchmark

### 单元测试

新增或扩展的测试包括：

1. `TestInsertOnDuplicateUpdateReusesCommonSubExpressions`

覆盖：

- 开关开启时，两个 assignment 中相同条件表达式在 rewrite 后共享同一个 expression 子树。
- `ResolveIndices` 后，共享关系仍然保留。
- 开关关闭时，不发生共享，行为回到原路径。

2. `TestInsertOnDuplicateUpdateExpressionReuseComplexityGate`

覆盖：

- 纯 `VALUES(col)` 赋值不会启用复用。
- 带重复复杂条件的 `ON DUPLICATE KEY UPDATE` assignment 会达到复杂度阈值并启用复用。

3. `TestInsertOnDuplicateUpdateExpressionReuseSetVarHint`

覆盖：

- `INSERT /*+ SET_VAR(tidb_enable_on_duplicate_expression_reuse=off) */ ... ON DUPLICATE KEY UPDATE` 能对当前语句关闭复用。
- `INSERT /*+ SET_VAR(tidb_enable_on_duplicate_expression_reuse=on) */ ... ON DUPLICATE KEY UPDATE` 能对当前语句开启复用。
- `SET_VAR` hint 生效后会恢复原 session 变量值。

4. `TestPlanCacheKeyIncludesOnDuplicateUpdateExpressionReuse`

覆盖：

- `tidb_enable_on_duplicate_expression_reuse=ON` 和 `OFF` 会生成不同 plan cache key。
- 切回相同开关值时 key 保持稳定。
- `NewPlanCacheKey` 在 `intest.InTest` 下不会因为新增 key 字段导致 hash buffer 重新分配。

### Benchmark

新增 benchmark：

```go
BenchmarkInsertOnDuplicateUpdateCompile
```

benchmark 使用匿名化的表结构和 `ON DUPLICATE KEY UPDATE` 语句，包含大量重复 `IF(common_condition, values(col), col)` assignment，用来衡量 planner 编译阶段的耗时和分配。

以下是一组本地 `-count=5` benchmark 结果均值。耗时会受机器负载影响，`B/op` 和 `allocs/op` 更稳定：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `920,193 ns/op` | `654,088 B/op` | `7,373 allocs/op` |
| `reuse_on` | `458,509 ns/op` | `264,064 B/op` | `2,183 allocs/op` |

对比收益：

- 时间减少约 `461,684 ns/op`，约 `50.2%`。
- 内存分配减少 `390,024 B/op`，约 `59.6%`。
- 分配次数减少 `5,190 allocs/op`，约 `70.4%`。
- 整体编译耗时约提升 `2.0x`。

针对纯 `VALUES(col)` assignment 的简单 `ON DUPLICATE KEY UPDATE` case，新增了独立 benchmark：

```go
BenchmarkInsertOnDuplicateUpdateCompileValuesOnly
```

在复杂度门控前，本地曾观察到这类 case 如果强行开启复用会略有回退：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `16,400 ns/op` | `19,088 B/op` | `164 allocs/op` |
| `reuse_on` | `20,667 ns/op` | `19,848 B/op` | `177 allocs/op` |

这个结果只用于说明为什么需要复杂度门控，不代表当前实现下的最新 on/off 对比。简单 `VALUES(col)` assignment 没有足够的公共子表达式收益，应该通过复杂度门控回到原始路径。

复杂度门控后，同一个 benchmark 的一组本地 `-count=5` 结果均值为：

| 模式 | 时间 | 内存 | allocs |
| --- | ---: | ---: | ---: |
| `reuse_off` | `15,079 ns/op` | `19,304 B/op` | `165 allocs/op` |
| `reuse_on` | `14,800 ns/op` | `19,304 B/op` | `165 allocs/op` |

`reuse_on` 对比 `reuse_off` 时间差异在噪声范围内，内存和 alloc 次数完全一致。
这组才是当前实现的有效对比；`reuse_on` 因复杂度门控没有进入 memoized rewrite / resolve 路径，所以 alloc 次数没有增加。

## 已执行验证

本地执行过：

```bash
GOCACHE=/tmp/go-build go test ./pkg/expression -run TestScalarFunctionCloneWithArgs -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build go test ./pkg/planner/core -run 'TestInsertOnDuplicateUpdateExpressionReuse(SetVarHint|ComplexityGate)|TestInsertOnDuplicateUpdateReusesCommonSubExpressions' -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build go test ./pkg/planner/core -run 'TestInsertOnDuplicateUpdateExpressionReuse(SetVarHint|ComplexityGate)|TestInsertOnDuplicateUpdateReusesCommonSubExpressions|TestPlanCacheKeyIncludesOnDuplicateUpdateExpressionReuse' -tags=intest,deadlock -count=1
```

```bash
GOCACHE=/tmp/go-build go test ./pkg/planner/core -run '^$' -bench 'BenchmarkInsertOnDuplicateUpdateCompile' -benchmem -count=5 -tags=intest,deadlock
```

```bash
GOCACHE=/tmp/go-build make bazel_prepare
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

这个共享逻辑只针对 `ON DUPLICATE KEY UPDATE` assignment 开启。关键前提是所有 assignment expression 使用同一个 `Schema4OnDuplicate`，相同表达式的 column 解析目标不会因为 assignment 位置不同而变化。如果未来想把同类 memo 复用扩展到其他 planner rewrite 场景，必须重新证明对应场景下 expression 的 schema 环境也是稳定且唯一的。

### CloneWithArgs 覆盖风险

`builtinFunc` 新增了 `CloneWithArgs` 方法。如果后续新增 concrete builtin signature，只实现 `Clone()` 但没有实现 `CloneWithArgs()`，编译会失败。这是有意设计，用于避免新增 builtin signature 时漏掉 `ON DUPLICATE KEY UPDATE` memoized resolve 所需的低分配 clone path。

`ScalarFunction.CloneWithArgs` 不复制原来的 `hashcode` / `canonicalhashcode` 缓存，因为传入 args 从 API 角度不一定与旧 args 完全等价。新 expression 会在需要 hash 时基于新 args 重新计算。

### 默认开启风险

变量默认开启可以让真实 `ON DUPLICATE KEY UPDATE` 场景直接受益，但也意味着新逻辑会进入线上默认路径。为降低风险，保留了 session/global 开关，发现异常时可以快速关闭。

### 未完成验证

当前没有跑更大范围的 planner package 测试或 integration test。

### Plan Cache 收益边界

当前实现没有优化 instance plan cache hit 时的 plan clone 成本。instance plan cache 命中后会 clone cached plan，其中 `OnDuplicate` expression 会通过普通 `Expression.Clone()` deep clone，`resolveExprIndicesWithMemo` 不会参与这条路径。

## 总结

这套改动的核心目标是降低 `ON DUPLICATE KEY UPDATE` 大量重复 assignment 表达式带来的 planner 编译成本。实现上分为两层：

1. AST rewrite 阶段用 restore text memo 复用重复 SQL 子表达式。
2. ResolveIndices 阶段用 expression pointer memo 保留共享表达式，并通过显式 `CloneWithArgs` 避免对旧 child subtree 做不必要的深拷贝。

从当前 benchmark 看，优化后 `ON DUPLICATE KEY UPDATE` 编译时间、内存分配和分配次数都有明显下降，收益主要来自重复表达式 rewrite 减少，以及 resolve 阶段避免重复 deep clone 已经共享出来的 expression subtree。

需要注意的是，这个收益主要覆盖 plan cache miss / compile path。对于 session plan cache hit，编译阶段本身会被跳过；对于 instance plan cache hit，当前仍然会为跨 session 安全 deep clone cached plan。后续如果要降低 instance plan cache hit 的 clone 成本，需要在 plan cache clone 路径引入 expression clone memo，避免同一次 cloned plan 内重复克隆已经共享的 expression subtree。
