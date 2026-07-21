# FTS / BM25 / Vector Score 特殊列映射设计

## 1. 背景

在全文检索和向量检索场景中，常见查询形态如下：

```sql
select fts_match(title, 'database') as score
from t
where fts_match(title, 'database')
order by fts_match(title, 'database') desc;
```

或者：

```sql
select vec_l2_distance(vec, '[1,2,3]')
from t
where ...
order by vec_l2_distance(vec, '[1,2,3]');
```

这些函数有一个共同点：

- 真正的计算必须发生在索引侧，而不是 TiDB 本地。
- 上层 `SELECT`、`ORDER BY`、后续可能的 `TopN`，都需要复用这个计算结果。
- 同一个函数可能在同一棵计划树里出现多次，所有出现点都必须映射到同一个返回值，而不是重复计算，也不能按“出现位置”猜测绑定关系。

当前 TiDB 在 vector distance 路径上已经有一套“索引返回特殊列，planner 重写父节点表达式”的成熟模式；FTS/BM25 也应当沿用同样的范式。

本设计文档讨论：

- 如何在当前代码结构下引入 `id = -2050` 的 score 特殊列。
- 如何确保“同一个函数出现多次时，全部正确映射到同一个值”。
- 如何分阶段落地，先做单读，再考虑回表和索引侧 TopK。

## 2. 当前实现概况

### 2.1 FTS builtin 的语义

当前 FTS builtin 的返回类型就是 `ETReal`：

- `pkg/expression/builtin_fts.go:113`
- `pkg/expression/builtin_fts.go:187`

这意味着：

- FTS 表达式本来就不是纯布尔函数。
- `WHERE fts_match(...)` 只是上层通过 `IS TRUE` 语义把 `real` 转成布尔条件。
- 这也正是后续 score / BM25 的预留口。

`expr_to_pb.go` 中已经有对应注释：

- `pkg/expression/expr_to_pb.go:276`

注释明确写了：TiDB 的 FTS functions 返回 float，是为了潜在的 BM25 score 场景。

### 2.2 TiCI FTS path 当前只支持 filter，不支持 score

`DataSource.analyzeTiCIIndex()` 在 `PredicatePushDown` 阶段尝试构造 TiCI FTS path：

- `pkg/planner/core/operator/logicalop/logical_datasource.go:173`
- `pkg/planner/core/operator/logicalop/logical_datasource.go:656`

最终在 `buildTiCIFTSPathAndCleanUp()` 里构建 `FtsQueryInfo`：

- `pkg/planner/core/operator/logicalop/logical_datasource.go:902`
- `pkg/planner/core/operator/logicalop/logical_datasource.go:951`

当前固定写死：

```go
QueryType: tipb.FTSQueryType_FTSQueryTypeNoScore
```

因此当前 path 本质上是：

- TiCI 负责做 FTS filter
- TiDB 不接收 score 列

### 2.3 当前 validation 直接禁止 SELECT / ORDER BY 中使用 FTS

当前逻辑规则 `ftsFuncValidation` 直接禁止：

- `SELECT fts_match(...)`
- `ORDER BY fts_match(...)`

对应位置：

- `pkg/planner/core/rule_ftsfunc_validation.go:44`
- `pkg/planner/core/rule_ftsfunc_validation.go:52`

这也是为什么现在用户写：

```sql
select fts_match() from ...
where fts_match()
order by fts_match()
```

根本进不到后续物理计划阶段。

### 2.4 当前物理路径选择也直接拒绝 “FTS path + sort property”

即便放开 validation，当前物理路径选择仍会把这类计划判 invalid：

- `pkg/planner/core/find_best_task.go:2275`

代码含义是：

- 只要 `candidate.path.FtsQueryInfo != nil`
- 且当前父节点请求了 sort property
- 直接放弃该 path

这意味着 V1 方案至少要允许：

- 无序 TiCI scan
- TiDB root sort / root TopN

不然 `ORDER BY score` 仍然无法落地。

### 2.5 TiCI 当前 row layout 对尾部列顺序有强假设

TiCI index scan 当前的特殊 row layout 在：

- `pkg/planner/core/operator/physicalop/physical_index_scan.go:453`

其尾部逻辑为：

- 可选 `ExtraPhysTblID`
- 必定追加 `ExtraVersion`

executor 侧又依赖两个重要假设：

- `pkg/executor/builder.go:4574`
- `pkg/executor/distsql.go:796`

具体假设是：

- 最后一列一定是 `_tidb_mvcc_version`
- 如果有 `ExtraPhysTblID`，它被认为在 version 前一个位置

因此 `-2050` score 列如果要插入 TiCI 返回 schema，必须避开这两个假设。

### 2.6 vector distance 已经有成熟的“特殊列映射”范式

vector distance 优化的核心逻辑位于：

- `pkg/planner/core/task.go:883`
- `pkg/planner/core/task.go:1011`

尤其是 `tryReturnDistanceFromIndex()`：

- 发现 index 侧已有 distance
- 注入一个特殊列 `VirtualColVecSearchDistanceID`
- 把上层 `TopN/Projection` 中的函数重写成这个列

这正是 FTS/BM25 score 需要复用的基本范式。

## 3. 问题本质

这里真正要解决的问题不是“传一个 `-2050` 列”本身，而是：

1. planner 必须识别哪些表达式需要复用 score。
2. planner 必须判断哪些表达式在语义上是同一个函数。
3. planner 必须保证这些语义等价表达式全部绑定到同一个特殊列。
4. executor 必须稳定接收到该列，并且列顺序不能破坏现有 TiCI 假设。

如果只做“索引返回一个 `-2050` 列”但不做语义绑定，会出现以下问题：

- `SELECT` 和 `ORDER BY` 中各自都有一个 `fts_match()`，TiDB 不知道它们应不应该共用同一个返回值。
- 如果存在 rewrite，文本不同但语义等价的表达式可能无法匹配。
- 如果靠出现顺序绑定，一旦 projection / sort / topN 形态变化，绑定就会漂移。

因此，绑定必须基于表达式语义，而不是文本或位置。

## 4. 设计目标

### 4.1 功能目标

- 支持 `SELECT score_expr`
- 支持 `ORDER BY score_expr`
- 支持同一个 score expr 在同一棵计划树里重复出现多次
- 所有重复出现点都映射到同一个 `-2050` 返回列
- 避免 TiDB 本地重新执行 FTS/BM25/vector score

### 4.2 非目标

V1 不追求：

- 同一个 `DataSource` 上多个不同 score expr 同时返回
- `JOIN ON`、`GROUP BY`、`HAVING`、window 中使用 score
- 回表路径 `IndexLookUpReader` 上直接输出 score
- TiCI index side 的 score TopK pushdown

### 4.3 兼容性目标

- 不污染 `DataSource` 的逻辑 schema
- 不破坏 `ExtraVersion` 在最后一列的约定
- 不破坏现有 `ExtraPhysTblID` 假设
- 不影响普通 TiCI FTS filter-only 路径

## 5. 总体方案

总体采用两层结构：

- 逻辑层：放松限制，但不改逻辑 schema，只保留 shape 校验。
- 物理层：在 `Attach2Task` 阶段完成 score 绑定、特殊列注入、父节点表达式重写。

更具体地说：

1. 新增特殊列 `ExtraFTSScoreID = -2050`
2. 当物理父节点发现自己消费了 FTS score expr 时：
   - 向下定位单读 TiCI path
   - 判断这些 expr 是否都语义等价
   - 判断它们是否与 index access cond 中的 FTS expr 语义一致
   - 成功则把 `FtsQueryInfo.QueryType` 切成 `WithScore`
   - 向 `PhysicalIndexScan` / `PhysicalIndexReader` 注入 score 列
   - 把父节点表达式中的所有等价 `fts_match()` 重写成 `Column{-2050}`

这样：

- 同一个函数出现多次，不再重复求值
- 也不需要在 TiDB 本地 eval FTS builtin
- 所有映射都绑定到同一个 score 列

## 6. 为什么绑定放在物理阶段，而不是逻辑阶段

这是本方案的核心取舍。

### 6.1 逻辑阶段可见信息不足

`analyzeTiCIIndex()` 运行于 `PredicatePushDown`：

- `pkg/planner/core/operator/logicalop/logical_datasource.go:173`

当时 `DataSource` 只知道：

- `PushedDownConds`
- 以及能否构造 TiCI FTS path

它并不知道：

- 上层 `Projection` 是否真的要输出 score
- 上层 `Sort/TopN` 是否要消费 score
- 最终是否走单读还是双读

### 6.2 逻辑 schema 改写侵入面过大

如果在逻辑阶段把 score 列塞进 `DataSource.Schema()`，会牵涉：

- `PruneColumns`
- index covering
- single/double read 判定
- 各类 rule 对 schema 的假设

这会显著放大改动范围。

### 6.3 物理阶段已经有现成范式

vector distance 在物理阶段完成：

- 特殊列注入
- 父节点表达式替换

而且已经证明这样做能稳定工作：

- `pkg/planner/core/task.go:1011`

因此 FTS/BM25 score 沿用相同范式是更自然的。

## 7. V1 范围与边界

V1 明确只支持：

- 单个 `DataSource`
- 单个 distinct score expr
- `PhysicalIndexReader` / 单读 TiCI path
- `Projection`、`Sort`、`TopN` 消费 score

V1 不支持：

- `IndexLookUpReader`
- 多个不同 score expr
- 没有对应 access expr 的纯 score 查询
- JOIN / AGG / HAVING / WINDOW / CTE

### 7.1 为什么 V1 只做单读

`PhysicalIndexReader` 的数据流更简单：

- index side 返回什么
- reader 就可以直接输出什么

而 `PhysicalIndexLookUpReader` 的 schema 默认是 table plan schema：

- `pkg/planner/core/operator/physicalop/physical_indexlookup_reader.go:221`

它不会自动把 index side 额外列带到最终输出，需要显式把 score 从 index rows 穿过 lookup task 再拼回最终 row。复杂度高很多，适合第二阶段处理。

## 8. 特殊列建模

### 8.1 model 常量

建议在 `pkg/meta/model/table.go` 中新增：

```go
const ExtraFTSScoreID int64 = -2050
var ExtraFTSScoreName = ast.NewCIStr("_tidb_fts_score")
```

位置建议靠近：

- `ExtraVersionID`
- `VirtualColVecSearchDistanceID`

理由：

- 它和 `ExtraVersionID` 一样，会进入 PB 和 executor 特殊列路径
- 它不像 vector distance 那样纯粹局限于 table scan 场景

### 8.2 ColumnInfo helper

建议在 `pkg/meta/model/column.go` 中新增：

```go
func NewExtraFTSScoreColInfo() *ColumnInfo
```

字段建议：

- `ID: ExtraFTSScoreID`
- `Name: ExtraFTSScoreName`
- `Type: mysql.TypeDouble`
- `Flag: 0` 或按需要设置 `NotNullFlag`
- binary charset / collation

### 8.3 为什么类型建议用 DOUBLE

当前 FTS builtin 的返回类型是 `ETReal`，更接近 `DOUBLE`，而不是强行缩成 `FLOAT`：

- `pkg/expression/builtin_fts.go:113`
- `pkg/expression/builtin_fts.go:187`

如果 score 列类型与 builtin 返回类型不一致，会产生潜在问题：

- projection type 不一致
- sort 比较类型不一致
- PB 侧列类型和父节点表达式预期不一致

因此 V1 直接使用 `mysql.TypeDouble` 更稳。

## 9. 语义等价判定

### 9.1 判定工具

必须复用现有表达式语义等价工具：

- `pkg/expression/scalar_function.go:591`
- `pkg/expression/scalar_function.go:600`

也就是：

- `CanonicalHashCode()`
- `ExpressionsSemanticEqual()`

不能做的事情：

- 不能用 `expr.String()`
- 不能用文本匹配
- 不能按出现位置绑定

### 9.2 归一化步骤

对所有候选 score expr，需要做统一归一化，伪代码如下：

```go
func normalizeFTSScoreExpr(expr expression.Expression) expression.Expression {
    // 1. 如果是 MATCH ... AGAINST，先 rewrite 到内部 helper
    // 2. 如果外层包了 IsTruthWithNull / IsTruthWithoutNull，把壳去掉
    // 3. 返回规范化后的 expr
}
```

原因：

- `WHERE fts_match(...)` 往往会包 `IS TRUE`
- `SELECT fts_match(...)` 则通常不会
- 如果不剥壳，这两个位置虽然语义同源，但 hash 可能不一致

归一化逻辑应尽量和：

- `pkg/planner/core/operator/logicalop/logical_datasource.go:875`
- `pkg/expression/expr_to_pb.go:276`

保持同一语义。

### 9.3 V1 的 distinct expr 限制

V1 要求：

- 同一条 path 上所有 score consumer 归一化后，只能得到 1 个 distinct hash

支持：

```sql
select f(), f()
from t
where f()
order by f();
```

不支持：

```sql
select f1(), f2()
from t
where f1() and f2();
```

后一类在 V1 直接报错：

- `multiple distinct FTS score expressions are not supported yet`

## 10. 物理阶段的绑定与重写

### 10.1 关键思想

score 的绑定不在逻辑层做，而在以下父节点的 `Attach2Task` 中做：

- `PhysicalProjection`
- `PhysicalSort`
- `PhysicalTopN`

对应位置：

- `pkg/planner/core/task.go:1472`
- `pkg/planner/core/task.go:864`
- `pkg/planner/core/task.go:1294`

做法是：

1. 父节点扫描自己的表达式，看是否包含 FTS score consumer。
2. 如果没有，走原逻辑。
3. 如果有，强制把 child task 转成 root task。
4. 在 root task 中向下找到 `PhysicalIndexReader + PhysicalIndexScan`。
5. 检查是否为单读 TiCI FTS path。
6. 成功则注入 `-2050`，并把父节点中所有等价函数都改成同一个 score 列。

### 10.2 为什么强制先转 root task

V1 中，如果父节点消费了 score，我建议不要尝试把这个节点继续 pushdown 到 cop 侧，而是：

- 直接转 root
- 在 root 上消费 score 列

理由：

- 改动更小
- 避免和现有 projection/topN pushdown 逻辑交叉
- 避免出现 “cop 侧想 push 一个 FTS function，但我们其实只想要 score 列”的混合状态

因此 V1 的策略很简单：

- 发现 score consumer
- 先 root 化
- 再 rewrite 为列

## 11. 单读路径上的 plan shape

`CopTask.ConvertToRootTask()` 在单读 index path 下，会构造：

- `PhysicalIndexReader`

对应位置：

- `pkg/planner/core/operator/physicalop/task_base.go:485`
- `pkg/planner/core/operator/physicalop/task_base.go:544`

也就是说，单读 TiCI path 的根形态通常是：

```text
Projection / Sort / TopN
  └─ IndexReader
       └─ IndexScan(TiCI)
```

因此 V1 的核心 helper 可以明确只处理这条链：

- 向下找到 `PhysicalIndexReader`
- 再找到 `PhysicalIndexScan`

如果不是这条链，V1 直接返回“不支持”。

## 12. 建议新增的 helper

### 12.1 找路径 helper

建议新增：

```go
func findSingleReadTiCIFTSReader(plan base.PhysicalPlan) (*physicalop.PhysicalIndexReader, *physicalop.PhysicalIndexScan, bool)
```

约束：

- 只接受单读 `PhysicalIndexReader`
- 允许中间存在一层轻量 wrapper，但最终必须能定位到唯一 `PhysicalIndexScan`
- `scan.StoreType == kv.TiCI`
- `scan.FtsQueryInfo != nil`

### 12.2 score consumer 收集

```go
func collectFTSScoreExprs(exprs []expression.Expression) []expression.Expression
```

用于：

- `Projection.Exprs`
- `TopN.ByItems`
- `Sort.ByItems`

### 12.3 归一化与 distinct 检查

```go
func buildFTSScoreBinding(
    scoreExprs []expression.Expression,
    accessConds []expression.Expression,
    parserType model.FullTextParserType,
) (*FTSScoreBinding, error)
```

该函数应完成：

- consumer expr 归一化
- access cond expr 归一化
- consumer distinct hash 统计
- consumer 是否出现在 access cond hash 集合中的校验

### 12.4 score 列注入

```go
func ensureFTSScoreInjected(
    reader *physicalop.PhysicalIndexReader,
    scan *physicalop.PhysicalIndexScan,
    binding *FTSScoreBinding,
) (*expression.Column, error)
```

职责：

- 若 scan 已有 `-2050`，复用
- 否则向 scan schema 注入
- 同步向 reader schema / outputColumns 注入
- 返回“供父节点 rewrite 使用的 reader schema 中的 score 列”

### 12.5 父节点表达式重写

```go
func rewriteFTSScoreExpr(
    expr expression.Expression,
    binding *FTSScoreBinding,
    scoreCol *expression.Column,
) expression.Expression
```

规则：

- 若 `normalize(expr)` 与 `binding.ExprHash` 相同，替换为 `scoreCol.Clone()`
- 若是 scalar function，递归处理参数
- 否则原样返回

## 13. `ensureFTSScoreInjected()` 的精确行为

这是整套方案最关键的实现点之一。

### 13.1 先检查 scan schema 中是否已存在 score 列

如果已存在：

- 直接复用
- 保证多个父节点共用同一个 injected score 列

这正是“同一个函数可能出现多次，他们要都能正确映射到那个值”的核心保证。

### 13.2 注入到 `PhysicalIndexScan.Schema()`

当前 TiCI row layout 在：

- `pkg/planner/core/operator/physicalop/physical_index_scan.go:459`

V1 要求插入规则为：

- 没有 `ExtraPhysTblID`：`[..., score, ExtraVersion]`
- 有 `ExtraPhysTblID`：`[..., score, ExtraPhysTblID, ExtraVersion]`

不能做成：

- `[..., ExtraPhysTblID, score, ExtraVersion]`
- `[..., ExtraPhysTblID, ExtraVersion, score]`

原因：

- [builder.go:4578](/DATA/disk4/yiding/gocode/tidb/pkg/executor/builder.go#L4578) 假定 `ExtraPhysTblID` 在 version 前一列
- [distsql.go:800](/DATA/disk4/yiding/gocode/tidb/pkg/executor/distsql.go#L800) 假定最后一列是 version

### 13.3 注入到 `PhysicalIndexReader.Schema()` 与 `OutputColumns`

`PhysicalIndexReader` 默认 schema 不是 index scan schema，而是 `DataSourceSchema`：

- `pkg/planner/core/operator/physicalop/physical_index_reader.go:77`

因此仅仅修改 `scan.Schema()` 不够，还必须：

- 在 `reader.Schema()` 中 append 一个 score 列
- 在 `reader.OutputColumns` 中 append 对应列

不然 executor builder 仍然不会请求这个列：

- `pkg/executor/builder.go:4379`

### 13.4 `scoreCol.Index` 的处理

`IndexReader` 在执行前会对 `OutputColumns` 做 `ResolveIndices()`：

- `pkg/planner/core/operator/physicalop/physical_index_reader.go:185`

因此建议：

- 注入时先构造列对象并挂入 schema
- `ResolveIndices()` 自然会把它解析到 scan schema 中的实际位置
- 避免手工写死 index

## 14. `ToPB()` 与 PB schema

在 `PhysicalIndexScan.ToPB()` 中，目前只识别：

- `ExtraHandleID`
- `ExtraPhysTblID`
- `ExtraVersionID`

位置：

- `pkg/planner/core/operator/physicalop/physical_index_scan.go:628`

V1 需要新增：

```go
else if col.ID == model.ExtraFTSScoreID {
    columns = append(columns, model.NewExtraFTSScoreColInfo())
}
```

否则：

- `-2050` 会被误当成普通 table column 去查 `FindColumnInfoByID`
- 直接失败

## 15. `Projection` 的处理细节

### 15.1 触发条件

`PhysicalProjection` 中只要 `Exprs` 里存在 FTS score consumer，就走特殊路径。

### 15.2 处理流程

1. `t := tasks[0].Copy()`
2. `t = t.ConvertToRootTask(...)`
3. 在 root plan 中定位 `IndexReader + IndexScan`
4. 构建 `FTSScoreBinding`
5. `scan.FtsQueryInfo.QueryType = WithScore`
6. 注入 score 列
7. 将 `p.Exprs` 中所有等价 score expr 替换为 `scoreCol`
8. 再正常 `attachPlan2Task(p, t)`

### 15.3 替换后的收益

替换完成后：

- `Projection` 不再包含 FTS builtin
- 只包含一个普通列

因此：

- executor 不会本地 eval FTS function
- 多个 `SELECT f()` 会直接复用同一个 score 列

## 16. `Sort` / `TopN` 的处理细节

### 16.1 `Sort`

`PhysicalSort` 的逻辑和 `Projection` 基本一致：

- 发现 `ByItems` 中存在 score expr
- root 化
- 绑定、注入、rewrite
- 最终 root sort 按 score 列排序

### 16.2 `TopN`

`TopN` 比 `Sort` 更复杂，因为它当前有一套 pushdown 和 heavy-function 优化逻辑：

- `pkg/planner/core/task.go:883`
- `pkg/planner/core/task.go:1294`

V1 建议不要尝试复用 `getPushedDownTopN()` 去做 FTS score pushdown，而是：

- 只要 `TopN.ByItems` 包含 score expr
- 直接转 root
- rewrite 成 score 列
- root 上执行 TopN

这样可以规避两个复杂问题：

- TiCI score TopK pushdown 语义
- 与现有 vector heavy-function 优化逻辑交织

### 16.3 为什么 V1 不做 TiCI score TopK pushdown

当前 `TryToPassTiCITopN()`：

- `pkg/planner/core/operator/physicalop/physical_index_scan.go:726`

只支持 hybrid index sort columns 与普通列排序匹配，不支持函数排序。把 score TopK pushdown 一起做，会引入更多协议和执行语义约束。V1 不需要承担这部分复杂度。

## 17. `find_best_task` 的调整

当前：

- `pkg/planner/core/find_best_task.go:2275`

会把任意 `FtsQueryInfo != nil` 且带 sort property 的 path 直接判 invalid。

V1 建议改为：

- 不再直接 invalid
- 但也不宣称该 path 满足 sort property
- 让 planner 正常走“无序 scan + root sort/root topN”

这样：

- `ORDER BY score` 至少可以执行
- 同时不会错误地把 score 排序当成 index-order 能力

## 18. validation 的调整

当前 validation 过于保守，全部禁止了 `SELECT` / `ORDER BY`。

V1 建议调整成：

- `Projection/Sort/TopN`：不再一刀切报错
- `Join/Aggregation/Window/GroupBy/Having`：继续禁止
- `Selection`：仍然要求 FTS expr 必须是可下推 access cond，不允许当普通 TiDB 本地表达式

也就是说：

- shape 约束继续保留
- score 绑定是否成功，不在 validation 中决定
- 绑定失败在物理计划阶段报更具体的错误

## 19. 错误模型

建议在物理绑定阶段报下列明确错误：

- `FTS score expression must also appear in a pushed-down FTS predicate`
- `multiple distinct FTS score expressions are not supported yet`
- `FTS score is only supported on single-read TiCI index path`
- `FTS score in JOIN/GROUP BY/HAVING/WINDOW is not supported`

这样比当前统一报：

- `cannot be used in SELECT fields`
- `ORDER BY is not supported`

要精确得多。

## 20. 为什么这套设计能保证“同一个函数多次出现都映射到那个值”

这里把关键逻辑单独说明。

### 20.1 重复出现点不会各自分配新列

score 列的注入由 `ensureFTSScoreInjected()` 统一负责：

- 若 scan schema 已经有 `-2050`
- 直接复用

因此同一条 root plan 上：

- 第一个消费者会触发注入
- 后续消费者只会复用同一个 injected 列

### 20.2 绑定不是按位置，而是按 canonical hash

所有 consumer expr 会先归一化，然后比对：

- `CanonicalHashCode()`

因此：

- `SELECT` 中的 `fts_match()`
- `ORDER BY` 中的 `fts_match()`
- 同一个 projection 中多次出现的 `fts_match()`

只要语义等价，都会命中同一个 hash。

### 20.3 rewrite 发生在每个父节点自己的表达式树里

`Projection`、`Sort`、`TopN` 分别对自己的表达式树做 rewrite：

- 只要某个节点语义等价于绑定 expr
- 直接替换成 `scoreCol`

因此“同一个函数多次出现”本质上只是一棵表达式树里命中了多次同一个规则，而不是需要维护“第几个出现点对第几个返回列”的 fragile mapping。

## 21. 为什么 V1 不直接支持 `IndexLookUpReader`

`IndexLookUpReader` 的 schema 默认是 table plan schema：

- `pkg/planner/core/operator/physicalop/physical_indexlookup_reader.go:221`

而 index side 额外返回列会先落进：

- `lookupTableTask.idxRows`
- `pkg/executor/distsql.go:82`

若要让 score 穿过 lookup，必须再做以下事情：

1. index request 把 score 一起返回
2. score 保存在 `idxRows`
3. table worker 回表后，按 handle 把 score 拼回最终 row
4. `IndexLookUpReader.Schema()` 扩出 score 列

这会同时涉及：

- planner
- index worker
- table worker
- row merge

因此明显应分期处理，而不是和单读路径一起做。

## 22. V2 方向：双读与回表

后续如果要支持：

```sql
select non_index_col, fts_match(...)
from t
where fts_match(...);
```

则需要进入 V2。

V2 的最小实现路径：

- 让 score 作为 index-side 辅助列进入 `lookupTableTask.idxRows`
- 建立 `handle -> score` 关联
- table side 回表后，把 score 和 table row 合并
- 最终 `IndexLookUpReader` 输出扩展 schema

这部分与当前 partition by-items / keep-order 的 index 辅助列处理模式相近，但不是本设计文档的 V1 范围。

## 23. 备选方案与取舍

### 23.1 备选方案 A：在逻辑层直接扩 `DataSource.Schema()`

优点：

- 上层所有节点天然能看到 score 列

缺点：

- 侵入逻辑 rule 太多
- 会影响 covering / prune columns / single scan 判定
- 风险大

结论：

- 不采用

### 23.2 备选方案 B：每个出现点都单独分配一个特殊列

优点：

- 实现直观

缺点：

- 同一个函数出现两次会返回两列，浪费
- executor / schema 管理复杂
- 很难保证等价表达式共享一个值

结论：

- 不采用

### 23.3 备选方案 C：先做统一的 “index-returned computed column” 抽象

优点：

- 抽象最干净

缺点：

- 牵涉 vector / FTS / hybrid index 多条线
- 一次性改动过大

结论：

- 长期可考虑
- 当前以 FTS score 特化实现为主

## 24. 测试建议

### 24.1 planner / explain 用例

建议增加：

- `select f(), f() from t where f()`
- `select f() from t where f() order by f()`
- `select f()+1 from t where f()`
- `select f1(), f2() from t where f1() and f2()` 报错
- `select f() from t where other_cond` 报错

其中 `f()` 代表 FTS score expr。

### 24.2 explain 验证

验证点：

- 计划中出现 `_tidb_fts_score`
- 不再出现 TiDB 侧 eval 的 FTS builtin
- TiCI path 的 `FtsQueryInfo.QueryType` 为 `WithScore`

### 24.3 执行验证

验证：

- `SELECT/ORDER BY` 同时使用同一个 score expr 时结果正确
- 重复出现的 score expr 数值一致
- `ORDER BY score DESC LIMIT N` 结果稳定

## 25. 建议的实施顺序

### Phase 1：准备工作

1. 新增 `ExtraFTSScoreID = -2050`
2. 新增 `NewExtraFTSScoreColInfo()`
3. `PhysicalIndexScan.ToPB()` 识别 `-2050`

### Phase 2：放开限制

1. 调整 `ftsFuncValidation`
2. 放开 `find_best_task.go` 中 FTS + sort property 的 blanket ban

### Phase 3：单读路径绑定与重写

1. 在 `task.go` 增加 normalize / binding / inject / rewrite helpers
2. 在 `Projection/Sort/TopN` 的 `Attach2Task` 中接入
3. 仅支持 `PhysicalIndexReader`

### Phase 4：测试与收尾

1. explain / planner case
2. execution case
3. 错误模型 case

### Phase 5：V2

1. `IndexLookUpReader`
2. score 透传 `lookupTableTask`
3. 可选的 TiCI TopK pushdown

## 26. 最终结论

对于当前代码库，最合适的实现不是“简单传一个 `-2050` 列”，而是：

- 在单读 TiCI FTS path 上注入 `-2050` score 特殊列
- 用 canonical hash 做语义绑定
- 把 `Projection/Sort/TopN` 中所有等价的 score expr 全部重写到同一个列

这样可以最小改动地解决两个核心问题：

- score 必须在索引侧执行
- 同一个函数可能出现多次，所有出现点都要正确映射到同一个值

同时，这套设计与现有 vector distance 的“特殊列回填”模式一致，易于复用和扩展，也为后续 BM25、多 score expr、回表路径和 TiCI TopK pushdown 留出了清晰演进路径。
