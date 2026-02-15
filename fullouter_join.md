# TiDB 支持 `FULL OUTER JOIN`（阶段 1：仅 HashJoin v1）工作量评估

> 目标：在 TiDB 里新增语法 + 执行层面的 `FULL OUTER JOIN` 支持。
>
> 本评估基于你提出的约束：**先只做 Hash Join**，并且 **先只在 HashJoin v1 上实现**。

---

## 1. 现状快速结论（先回答“看起来只要加一个 joiner 吗？”）

仅新增一个 `fullOuterJoiner` **不够**。

原因是 TiDB 现有 HashJoin v1 的“outer join 支持”是以 **“一侧被保留（outer side）”** 为前提设计的：

- `LeftOuterJoin / RightOuterJoin`：
  - probe 阶段输出 **unmatched outer 行**
  - build 阶段（当 `UseOuterToBuild=true`）会在末尾扫描 hash table 输出 **unmatched outer 行**
  - 但**另一侧（inner side）不需要输出 unmatched**
- `FULL OUTER JOIN` 的语义是：**两侧都要保留**，既要输出 unmatched left，又要输出 unmatched right。

因此 full join 需要额外的：

1) **两侧各自的 “outer side filter” 处理**（ON 条件中仅引用单侧的条件，不能简单下推成 selection，不然会丢保留侧行）。  
2) **build side 的 match tracking**（需要知道哪些 build 行被匹配过，才能在 tail 输出未匹配行）。  
3) **probe side 的 unmatched 输出**（在 `UseOuterToBuild` 的代码路径里目前是不会输出 probe side unmatched 的）。  

joiner 只是其中一环，真正的工作量主要在 **parser / planner / executor 的“join type + 条件下推/执行语义”闭环**。

---

## 2. 建议的阶段 1 范围（减少工作量、降低风险）

为了让第一版能落地，建议明确一些限制（后续再逐步放开）：

1) **只支持 `FULL OUTER JOIN ... ON ...`**  
   - 先不做 `NATURAL FULL OUTER JOIN` / `FULL OUTER JOIN ... USING(...)`（因为 TiDB 目前的 natural/using 会依赖 FullSchema/列消除逻辑，full join 下两侧都可能生成 NULL，需要额外审计）。
   - 额外说明：`FULL` 在 TiDB parser 中是 unreserved keyword，`FULL JOIN` 可能与 table alias/identifier 产生二义性（例如 `t1 full join t2` 在 MySQL 语义下可能被解释为 `t1 AS full JOIN t2`）。
     为降低语法歧义与兼容性风险，阶段 1 建议 **仅识别 `FULL OUTER JOIN`（`OUTER` 不可省略）**，并保持 `FULL JOIN` 的 MySQL 兼容解释：`t1 AS full JOIN t2`。
2) **只支持 Root 执行（TiDB 层）**  
   - **不 push down 到 TiFlash MPP**，也不 push down 到 TiKV coprocessor（`tipb.JoinType` 没有 full join）。
3) **只出 HashJoin（v1）**  
   - 禁止 MergeJoin / IndexJoin / HashJoin v2（planner 枚举时直接跳过）。
4) **执行语义对齐标准 SQL**  
   - 同时支持 `=` 与 `<=>`（`nulleq`）作为 join 的等值条件：
     - `=`：任一侧 join key 为 `NULL` 不匹配（沿用现有 HashJoin 行为：含 `NULL` join key 直接当作 unmatched）。
     - `<=>`：NULL-safe 等值比较，`NULL <=> NULL = TRUE`，因此 `NULL` join key 行会参与 hash，并可与另一侧 `NULL` join key 行匹配。
   - 仅当 `<=>` 被抽取为 join 的 `EqualConditions`（两侧都是 column）时作为 join key；否则会落入 `OtherConditions`，仅作为结果过滤条件处理。
5) **输出列的 NotNull flag**  
   - full join 下：**左右两侧所有输出列都需要清掉 NotNull**（因为 unmatched 行会产生 NULL）。

这些限制能把“可控的正确性面”压到最小，并且更容易加单测覆盖。

---

## 3. 代码现状：HashJoin v1 / Joiner 关键点

### 3.1 HashJoin v1 的 outer join 机制

HashJoin v1（`pkg/executor/join/hash_join_v1.go`）已经有一套很关键的基础设施：

- `HashJoinCtxV1.outerMatchedStatus []*bitmap.ConcurrentBitmap`
  - **按 build side chunk 维护每行是否 matched 的 bitmap**
- tail 阶段输出 unmatched build 行：
  - `ProbeWorkerV1.handleUnmatchedRowsFromHashTable()` 会遍历 `rowContainerForProbe.GetChunk(i)`，
    对 bitmap 未置位的行调用 `Joiner.OnMissMatch(...)` 输出。

这套机制目前只在 `UseOuterToBuild=true` 时启用（见 `waitJoinWorkersAndCloseResultChan()`）。

对 full join 来说，这个能力非常有用：full join 必须要输出 build side 的 unmatched 行。

### 3.2 Joiner 的能力边界

Joiner 接口（`pkg/executor/join/joiner.go`）目前有：

- `TryToMatchInners(outerRow, innerIter, ...)`
- `TryToMatchOuters(outerIter, innerRow, ...)`（用于 outer side build 的场景，返回每个 outer row 的 matched/unmatched/hasNull 状态）
- `OnMissMatch(outerRow, ...)`（只定义了“outer row unmatched 怎么输出”）

它天然只表达“outer side unmatched”的输出，因此 full join 要表达“两边 unmatched”时：

- 需要 **在 executor 里显式处理另一侧 unmatched**
- 或者扩展 Joiner 接口（工作量更大，影响面更广）

---

## 4. 工作量拆分（按模块/文件）

下面按“为了让 full join 能跑通（阶段 1 限制下）”列出必须改动的模块，并给出粗略复杂度。

### 4.1 Parser / AST（语法层）

涉及文件：

- `pkg/parser/parser.y`
  - `JoinType:` 目前只有 `"LEFT"` / `"RIGHT"`，需要加 `"FULL"` 分支
  - 仅支持 `FULL OUTER JOIN`（不把 `FULL JOIN` 当作缩写，以避免语法二义性）
- `pkg/parser/ast/dml.go`
  - `type JoinType int` 注释写着包含 full，但常量没定义 full
  - `(*ast.Join).Restore()` 目前只输出 LEFT/RIGHT，需要补 FULL
- `pkg/parser/parser_test.go`
  - 加 parse + restore 的 case（覆盖 `FULL OUTER JOIN`；并包含至少一个 “`FULL JOIN` 不作为缩写” 的 case）

复杂度：**低（~0.5 天）**

### 4.2 Planner：Logical JoinType + schema/nullability + predicate pushdown

这是 full join 最容易踩坑、也是影响面最大的部分。

#### 4.2.1 新增 JoinType

涉及文件：

- `pkg/planner/core/operator/logicalop/logical_join.go`
  - `type JoinType int` 增加 `FullOuterJoin`
  - 更新：
    - `JoinType.IsOuterJoin()`：full join 必须返回 true
    - `JoinType.String()`：用于 explain/info
  - **最关键**：`LogicalJoin.PredicatePushDown(...)` 需要新增 `case FullOuterJoin`

复杂度：**中（~1 天）**

#### 4.2.2 LogicalPlan builder：AST join -> LogicalJoin join type + NotNull 处理

涉及文件：

- `pkg/planner/core/logical_plan_builder.go`
  - `switch joinNode.Tp` 里新增 `ast.FullJoin` 映射到 `logicalop.FullOuterJoin`
  - 对 schema 的 NotNull flag：
    - left/right outer join 只清一侧
    - full outer join 需要 **清两侧所有列**
  - `FullSchema`/`FullNames` 合并逻辑：full join 没有 outer/inner 概念，建议按 **left + right** 顺序固定

复杂度：**中（~0.5 天）**

#### 4.2.3 PhysicalJoin schema：NotNull flag 必须两侧清空

涉及文件：

- `pkg/planner/core/util.go`：`BuildPhysicalJoinSchema(...)`
  - 目前只对 left/right outer join 清一侧 not null
  - full join 要对 merge 后的 schema 清两侧（等价于 `ResetNotNullFlag(newSchema, 0, newSchema.Len())`）

复杂度：**低（~0.2 天）**

#### 4.2.4 Predicate PushDown / 条件处理（重点风险）

现有 outer join 的处理逻辑（见 `LogicalJoin.PredicatePushDown`）默认：

- 只有一侧是 preserved side（outer），另一侧可以正常下推 selection

full join 的正确处理应该是：

- **两侧都是 preserved**
- `ON` 中仅引用单侧的条件（对应 `LeftConditions` / `RightConditions`）**不能当成 selection pushdown**
  - 应该作为“outer filter”保留在 join operator 内部，用于：
    - probe side：不满足 filter 的行直接作为 unmatched 输出
    - build side：不满足 filter 的行不进 hash table，但在 tail 作为 unmatched 输出

因此 full join 需要：

- 在 `PredicatePushDown` 里添加 full join 分支：
  - 禁止把 `LeftConditions` / `RightConditions` 下推到 child selection
  - 也要避免 simplifyOuterJoin / outer join elimination 等规则把 full join 错当成可简化的 outer join
- 审计/屏蔽可能依赖“单侧 outer”的优化规则：
  - join reorder（`pkg/planner/core/rule_join_reorder.go`）目前只允许 inner/left/right，full join 会自然被跳过，但要确认不会走到错误分支
  - aggregation push down / join elimination 等规则（搜索 `LeftOuterJoin/RightOuterJoin` 的 switch）可能需要显式跳过 full join

复杂度：**高（~1–2 天，主要是 correctness audit）**

### 4.3 Physical plan 枚举：只允许 HashJoin（Root）+ 禁止 MPP / Merge / Index

涉及文件：

- `pkg/planner/core/exhaust_physical_plans.go`
  - `exhaustPhysicalPlans4LogicalJoin` 当前会枚举：
    - MPP hash join（TiFlash）
    - merge join
    - index join
    - root hash join
  - full join 阶段 1 需要：
    - 禁止 MPP join（否则 `plan_to_pb.go` 会把未知 join type 当 inner join 映射，语义错误）
    - 禁止 merge/index join
    - 只保留 `getHashJoins(...)` 的 root hash join

复杂度：**中（~0.5–1 天）**

### 4.4 Executor：HashJoin v1 让 full join “两边 unmatched 都输出”

这是阶段 1 的核心实现。

涉及文件/位置：

- `pkg/executor/builder.go`：`buildHashJoin(...)`
  - 当前 outer join 有个假设：
    - inner side 的 `LeftConditions/RightConditions` 必须为空（否则 build executor 直接报错）
  - full join 下两侧 `LeftConditions/RightConditions` 都需要保留在 join 内部，因此这里需要：
    - 为 full join 取消/改写 “inner condition should be empty” 检查
    - 同时把 `LeftConditions` / `RightConditions` 分别放到 join exec 的两个 filter 字段（build/probe 各一个）
  - joiner 构造：
    - `join.NewJoiner` 目前只认识 Inner/Left/Right/Semi...，必须避免传 `FullOuterJoin` 导致 panic
    - full join 很可能需要 **两个 joiner**：
      - `joinerForBuildUnmatched`: 输出 `(buildRow, NULLs_of_probe)`
      - `joinerForProbeUnmatched`: 输出 `(NULLs_of_build, probeRow)`
    - matched row 输出可复用现有 left/right outer joiner 的 `TryToMatchOuters` 逻辑（因为它能返回 outer row status，便于标记 build side matched bitmap）

- `pkg/executor/join/hash_join_v1.go`
  - build 阶段：
    - 需要为 full join 建立 build side matched bitmap（可复用 `UseOuterToBuild=true` 的逻辑：`outerMatchedStatus`）
    - build side filter：不满足 filter 的 build 行不入 hash table，但保留在 rowContainer，让 tail 输出 unmatched
  - probe 阶段：
    - 需要输出 **probe side unmatched 行**（当前 `join2ChunkForOuterHashJoin` 不会输出）
    - 需要在“有 key bucket，但 other condition 全过滤掉”的情况下把 probe 行当 unmatched 输出
  - tail 阶段：
    - 输出 build side unmatched（现有 `handleUnmatchedRowsFromHashTable` 已具备，只需要确保 full join 会走到这条路径，并且 `OnMissMatch` 输出的是 “buildRow + NULL probe”）

复杂度：**高（~2–4 天）**

> 备注：如果选择“最小侵入”的实现策略，是在 HashJoin v1 内部对 `FullOuterJoin` 单独开分支，
> 避免改 Joiner interface（否则影响面会更大，估计至少再 +2 天）。

### 4.5 Tests：覆盖语义 + 防回归

建议至少补齐：

1) parser/restore UT（`pkg/parser/parser_test.go`）
2) executor UT（`pkg/executor/join/...`）：
   - 基本：
     - match + left only unmatched + right only unmatched
     - duplicate key（多对多）确认 matched 行数正确
     - join key 含 NULL：
       - `=`：两边都应输出 unmatched（NULL 不匹配）
       - `<=>`：`NULL`-`NULL` 应命中匹配；且当两侧 `NULL` key 有重复时会产生 `N×M` 行（需要显式覆盖）
   - other condition：
     - key bucket 有行，但 other condition 过滤完 -> 双方各自输出 unmatched
3) planner explain UT：
   - `EXPLAIN` 输出 JoinType string 正确
   - 确认不会选到 MPP / Merge / Index join（只出现 HashJoin）

复杂度：**中（~1–2 天）**

---

## 5. 粗略工期评估（阶段 1）

按“只做 hash join v1 + 限制范围”的前提，保守估计：

- Parser/AST：0.5 天
- Planner JoinType + schema/nullability：1.5–2.5 天
- Physical plan 枚举限制（只 hash join / no mpp）：0.5–1 天
- Executor HashJoin v1 full join 语义：2–4 天
- UT/Explain 覆盖：1–2 天

**合计：~6–10 个工作日**（主要浮动在 planner predicate pushdown/correctness audit 和 executor 实现细节）。

如果第一版为了赶进度愿意接受更多限制（例如：只支持最简单的 `FULL OUTER JOIN ... ON <equality keys only>`，拒绝 other condition / 复杂表达式），可以把 executor 的工作量砍掉一部分，但后续补齐会很痛。

---

## 6. 我认为最关键的技术风险点（提前标出来）

1) **Predicate pushdown 语义**  
   full join 下 `LeftConditions`/`RightConditions` 的处理如果和 left/right outer join 共用逻辑，很容易把 preserved side 的行“过滤掉”，造成 silent wrong result。

2) **NotNull flag / 类型属性**  
   full join 的输出列 nullable 规则会影响很多上层算子（尤其是聚合、谓词、表达式折叠）。

3) **MPP / ToPB 映射**  
   `pkg/planner/core/plan_to_pb.go` 里 join type 映射没有 full join，如果不显式阻断，可能会被错误映射成 inner join，属于高风险 silent corruption。

4) **性能与内存**  
   build side matched bitmap 是按 build side 行数 scale 的（1 bit/row + bitmap 管理开销），在 full join 下属于刚需；但要注意：
   - spill 场景下 rowContainer 在磁盘，bitmap 在内存，整体内存仍可控但需要关注峰值
   - 若 join key 使用 `<=>`，`NULL` 会参与等值匹配：当两侧存在大量 `NULL`（且 key 去重度低）时，匹配结果可能出现明显的 `N×M` 级增长（这不是 full join 特有，但在 full join 场景下更容易“看起来像爆炸”）。

---

## 7. 下一步建议（如果你要我继续细化）

如果你愿意，我可以继续把这个评估变成一个更“可执行”的 checklist（带具体改动点和伪代码），例如：

- 新增 `ast.FullJoin` / `logicalop.FullOuterJoin` 的最小 patch 路径
- full join 下 planner 如何避免 pushdown 错误（具体 case）
- HashJoin v1 full join 的实现草图（build/probe/tail 三段各加哪些逻辑）
- UT case 列表（按语义覆盖矩阵）
