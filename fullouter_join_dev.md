# TiDB `FULL OUTER JOIN`（阶段 1：HashJoin v1）开发划分与落地清单

> 本文是对 `fullouter_join.md` 的“可执行化拆分”：把需要做的工作拆成**可并行/可分 PR**的任务列表，并给出每块的验收标准与建议测试。
>
> 阶段 1 目标与约束：
> - 仅支持 `FULL OUTER JOIN ... ON ...`（不做 `NATURAL` / `USING`）。
> - 语法层面仅识别 `FULL OUTER JOIN`（`OUTER` 不可省略）；`FULL JOIN` 保持 MySQL 兼容解释（`t1 AS full JOIN t2`）。
> - 仅 Root 执行（TiDB 层），禁止 TiFlash MPP / TiKV coprocessor pushdown。
> - 仅 HashJoin v1（禁止 MergeJoin / IndexJoin / HashJoin v2）。
> - ON 等值条件同时支持 `=` 与 `<=>`（`nulleq`）：
>   - `=`：任一侧 join key 为 `NULL` 不匹配。
>   - `<=>`：`NULL <=> NULL = TRUE`，`NULL` join key 参与 hash 并可匹配。
> - 在 planner/executor 完整支持前，`FULL OUTER JOIN` 必须在 planner 阶段显式报错（fail-fast），禁止 silent fallback 到 inner join。

补充说明（语法层面的取舍）：

- `FULL` 在 TiDB parser 中是 **unreserved keyword**，可能与 table alias/identifier 产生二义性。例如 MySQL 语义下：
  - `t1 full join t2` 可以被解释为 `t1 AS full JOIN t2`（即普通 inner/cross join），而不是 FULL OUTER JOIN。
- 为避免这类二义性与兼容性风险，阶段 1 **不把 `FULL JOIN` 当作 `FULL OUTER JOIN` 的缩写**；
  语法层面仅识别 `FULL OUTER JOIN`（`OUTER` 不可省略），并保持 `FULL JOIN` 的 MySQL 兼容解释：`t1 AS full JOIN t2`。

---

## 0. 语义口径（先统一“正确性”）

在开始拆任务之前，需要把这三类语义口径写死，避免后续在 planner/executor 两边各自“猜”：

1) **Join 输出的 NULL 规则**
   - full join 任何一侧 unmatched 行都会把另一侧列补成 NULL。
   - 因此 full join 输出 schema：左右两侧所有列都必须清掉 NotNull flag。

2) **ON 条件的分解与执行位置**
   - Join key（`EqualConditions`）：只包含两侧都是 column 的 `=` 或 `<=>`。
   - 单侧条件（`LeftConditions` / `RightConditions`）：仅引用单侧的 ON 条件；full join 下它们是“preserved-side filter”，**不能下推成 selection**。
   - 其余条件（`OtherConditions`）：引用两侧或复杂表达式；在 join 结果上过滤。若过滤导致一个 outer row 没有任何输出，则该 outer row 仍视为 unmatched（full join 两侧都要输出 unmatched）。

3) **`<=>` 作为 join key 的语义**
   - 仅当 `<=>` 被抽为 `EqualConditions`（两侧都是 column）时，才进入 hash key 逻辑。
   - 对 hash join 来说其实现抓手是每个 join key 的 `IsNullEQ`（按 key 维度），决定 build/probe hashing 时是否“忽略 NULL”。

---

## 0.1 关键设计决策（阶段 1）

本小节记录阶段 1 的关键实现取舍，目的是：后续重启/多人协作时不需要再“口头对齐”。

### 决策 1：物理计划枚举 build 方向，但 FULL OUTER JOIN 强制 `UseOuterToBuild=false`

FULL OUTER JOIN 语义对称，但 HashJoin 的资源开销强依赖 build side（rowContainer/hash table/bitmap 均随 build 行数增长）。因此：

- 物理计划层面：FULL OUTER JOIN 像 InnerJoin 一样**枚举两种 build 方向**（build-left / build-right），由 cost/hint 选择更小的一侧作为 build。
- 执行层面：FULL OUTER JOIN **强制 `UseOuterToBuild=false`**，原因：
  - HashJoin v1 的 `UseOuterToBuild=true` 路径（`join2ChunkForOuterHashJoin`）是为“单侧 preserved”的 left/right outer join 设计的，probe-side unmatched 不会自然输出。
  - FULL OUTER JOIN 需要同时输出 probe-side unmatched + build-side unmatched，若同时维护 `UseOuterToBuild=true/false` 两条 full join 路径，复杂度与测试矩阵会翻倍，不适合 phase 1。

实现含义：即使枚举 build-left/build-right，也只走 HashJoin v1 的“常规 join2Chunk 风格”路径，并在该路径补齐 FULL OUTER JOIN 的 match tracking + tail unmatched 输出。

### 决策 2：Executor 不新增 `fullOuterJoiner`，采用“两把 joiner + `TryToMatchOuters` 追踪 build match”

FULL OUTER JOIN 必须同时输出：
- probe-side unmatched：输出 `(probeRow, NULL-build)`
- build-side unmatched：tail 阶段输出 `(NULL-probe, buildRow)`

而 Joiner 接口天然以“单侧 outer row”为中心（`OnMissMatch` 只处理 outerRow unmatched），因此阶段 1 采用最小侵入方案：

- 每个 probe worker 构造两把 joiner（均为现有 joiner 类型，避免修改 Joiner 接口影响面扩散）：
  - `buildJoiner`：用于输出 matched 行、并用 `TryToMatchOuters(...)` 返回的 per-row 状态来标记 build matched bitmap；tail 阶段也用它输出 build-side unmatched。
  - `probeJoiner`：仅用于 probe-side unmatched 输出（`OnMissMatch`）。
- 精确 match tracking：必须以 “是否真正产出过至少一条通过 `OtherConditions` 的结果行” 为准，不能以 “key bucket 非空” 代替。`TryToMatchOuters` 的返回状态可满足这个需求。

不同 build 方向下 joiner 选择表（输出 schema 始终为 left,right）：

- build=left / probe=right：
  - `buildJoiner`: `LeftOuterJoiner`（outer=left，unmatched -> left + NULL-right）
  - `probeJoiner`: `RightOuterJoiner`（outer=right，unmatched -> NULL-left + right）
- build=right / probe=left：
  - `buildJoiner`: `RightOuterJoiner`（outer=right，unmatched -> NULL-left + right）
  - `probeJoiner`: `LeftOuterJoiner`（outer=left，unmatched -> left + NULL-right）

### 决策 3：FULL OUTER JOIN 引入 `BuildFilter` / `ProbeFilter`，避免复用单个 `OuterFilter`

FULL OUTER JOIN 两侧都是 preserved side，ON 中仅引用单侧的条件（`LeftConditions`/`RightConditions`）不能下推成 Selection；在执行时应表现为：

- `BuildFilter`：决定 build 行是否进入 hash table（不满足者不能参与匹配），但行仍需保留在 rowContainer 供 tail unmatched 输出。
- `ProbeFilter`：决定 probe 行是否参与匹配；不满足者直接作为 probe-side unmatched 输出。

因此阶段 1 不复用单个 `OuterFilter`（其落点会随 `UseOuterToBuild` 翻转），而是显式引入 `BuildFilter`/`ProbeFilter` 来固定语义。

### 决策 4：`IsNullEQ` 必须按 key 维度参与“是否可匹配”的判断

HashJoin v1 已支持 `IsNullEQ` 用于 `<=>` join key，但 FULL OUTER JOIN 的 probe-side unmatched 逻辑不能简单写成：

- “probe key 含 NULL => unmatched”

而必须变成：

- 若 probe row 在任一 `=` join key 上为 NULL（对应 `IsNullEQ[key]=false`），则该 row 不可匹配，应输出 probe-side unmatched。
- 若 NULL 只出现在 `<=>` join key 上（对应 `IsNullEQ[key]=true`），则该 row 仍可参与 hash 匹配。

这条规则同样适用于 build side（是否入 hash table、是否可被匹配）。

---

## 0.2 过渡期安全门（parser/ast 先行合入时必须满足）

parser/ast 先行后，系统会开始接受 `ast.FullJoin`。此时如果 planner 仍按默认分支把它当 inner join 处理，会产生 silent wrong result。为避免该风险，阶段 1 明确要求：

1) **强制 fail-fast**
   - 在 `logical_plan_builder` 将 `ast.FullJoin` 映射到 `logicalop.FullOuterJoin` 之前，遇到 `ast.FullJoin` 必须直接返回 `ErrNotSupportedYet`（或等价错误），错误信息应明确包含 `FULL OUTER JOIN`。
   - 目标是“宁可报错，不可错算”。

2) **禁止语义降级**
   - 明确禁止把 `ast.FullJoin` 走默认 inner join 分支。
   - 该安全门应在 PR1 合入时一并落地，不可后置。

3) **安全门验收**
   - `SELECT ... FULL OUTER JOIN ... ON ...` 在 PR1 阶段应得到明确的 not supported 错误。
   - `FULL JOIN` 仍按 alias 语义解析（`t1 AS full JOIN t2`）。
   - `FULL OUTER JOIN ... USING(...)` 与 `NATURAL FULL OUTER JOIN ...` 在阶段 1 也应明确报 not supported（直到后续显式支持）。

---

## 1. PR/任务拆分建议（推荐顺序）

建议至少拆成 4 个小 PR（也可以合并，但拆开更利于 review 与回滚）：

### PR1：Parser / AST + Planner fail-fast gate（语法闭环 + 安全门）

- **目标**
  - 支持解析 `FULL OUTER JOIN`。
  - 不把 `FULL JOIN` 当作 `FULL OUTER JOIN` 的缩写（避免 `full` 作为 alias/identifier 的二义性）。
  - restore 输出能完整 round-trip（parse -> restore -> parse）。
  - parser/ast 合入后，planner 对 `FULL OUTER JOIN` 显式报错，避免 silent fallback。
- **涉及文件**
  - `pkg/parser/parser.y`
  - `pkg/parser/lexer.go`（`FULL OUTER JOIN` 组合 token 识别）
  - `pkg/parser/ast/dml.go`
  - `pkg/parser/parser_test.go`
  - `pkg/planner/core/logical_plan_builder.go`（临时 fail-fast gate）
- **验收标准**
  - 新增 parse + restore UT 覆盖至少：
    - `t1 FULL OUTER JOIN t2 ON t1.a = t2.a`
    - `t1 FULL OUTER JOIN t2 ON t1.a <=> t2.a`
  - `t1 FULL JOIN t2 ...` 维持 alias 语义（不当作 full outer join 缩写）。
  - 在 PR1 阶段，`FULL OUTER JOIN` 查询返回明确 not supported 错误（非 inner join 结果）。
  - 在阶段 1，`FULL OUTER JOIN ... USING(...)` / `NATURAL FULL OUTER JOIN ...` 返回明确 not supported 错误。

### PR2：Planner（LogicalJoin/PhysicalJoin 支持 Full Join）

- **目标**
  - planner 端能构造出 `logicalop.FullOuterJoin`（或等价 JoinType）并贯穿到物理计划。
  - full join 下 schema nullable 正确（NotNull flag 清空）。
  - full join 下 predicate pushdown 不丢 preserved 行。
  - 移除 PR1 的 fail-fast gate，并由真实 full join 语义接管。
- **涉及文件（典型）**
  - `pkg/planner/core/operator/logicalop/logical_join.go`（JoinType + `PredicatePushDown` full join 分支）
  - `pkg/planner/core/logical_plan_builder.go`（AST join -> LogicalJoin join type 映射；nullable 处理）
  - `pkg/planner/core/util.go`（`BuildPhysicalJoinSchema`，full join 清两侧 NotNull）
  - 可能还需要审计若干规则（join elimination / outer join simplify 等）对 full join 的处理：`rg "LeftOuterJoin|RightOuterJoin"`。
- **验收标准**
  - `EXPLAIN` 能显示 full join join type 字符串正确。
  - `PredicatePushDown` 不把 full join 的 `LeftConditions` / `RightConditions` 下推成子树 Selection（避免 silent wrong result）。

### PR3：Physical plan 枚举限制（只允许 Root HashJoin v1）

- **目标**
  - full join 只生成 Root HashJoin（v1）。
  - 明确阻断：
    - MPP hash join / shuffle join（因为 `plan_to_pb.go` 无 full join 映射，风险是 silent corruption）。
    - MergeJoin / IndexJoin / HashJoin v2。
- **涉及文件**
  - `pkg/planner/core/exhaust_physical_plans.go`
  - （必要时）`pkg/planner/core/plan_to_pb.go`：加 defensive check，遇到 full join 直接报错/禁止下推（即使理论上已经不会走到）。
- **验收标准**
  - explain 计划中 full join 只出现 root HashJoin（并且版本为 v1 路径）。

### PR4：Executor（HashJoin v1 FULL OUTER JOIN 语义）

- **目标**
  - HashJoin v1 支持 full join：matched 输出 + 双侧 unmatched 输出 + match tracking。
  - 同时正确支持 join key `=` 与 `<=>`（通过 `IsNullEQ`）。
  - 不引入 Joiner interface 大改（优先 executor 内部分支实现）。
- **涉及文件（典型）**
  - `pkg/executor/builder.go`：buildHashJoin() 能构造 full join 所需的 joiner/filters，并避免现有 “inner condition should be empty” 的假设冲突。
  - `pkg/executor/join/hash_join_v1.go`：核心实现（build/probe/tail 三段）。
  - 必要时 `pkg/executor/join/joiner.go` 仅做最小增强（优先不动）。
- **验收标准**
  - 基本语义覆盖（见第 4 节测试矩阵）。
  - 并发与 spill 场景不 crash，不死锁（至少跑到 join 结束并输出一致结果）。

### PR5（可选）：系统测试 / explain 测试数据补齐

如果 UT 不好覆盖某些语义组合，建议补 integrationtest 的 `.test`：
- `tests/integrationtest/t/executor/jointest/join.test`（偏 join 语义）
- `tests/integrationtest/t/planner/core/plan.test` / `integration.test`（偏 plan 选择与 explain）

---

## 2. 关键实现任务清单（按模块）

### 2.1 Parser / AST

- [ ] `parser.y` + `lexer.go`：仅识别 `FULL OUTER JOIN`，不把 `FULL JOIN` 视作缩写。
- [ ] AST JoinType 常量补齐（需要能区分 full）。
- [ ] `Restore()` 输出 FULL / FULL OUTER（选一种统一输出风格即可，但要稳定）。
- [ ] parser UT：覆盖 `FULL OUTER JOIN`（并包含至少一个 “`FULL JOIN` 不作为缩写” 的 case）。
- [ ] planner fail-fast gate：`ast.FullJoin` 在 PR1 阶段返回 `ErrNotSupportedYet`（或等价错误），禁止静默走 inner join。
- [ ] 阶段 1 范围门禁：`FULL OUTER JOIN ... USING(...)` / `NATURAL FULL OUTER JOIN ...` 明确报 not supported。

### 2.2 Planner：JoinType + Schema(nullable) + Pushdown

- [ ] LogicalJoin JoinType 增加 FullOuterJoin（并确保 `IsOuterJoin()` 返回 true）。
- [ ] 移除 PR1 中的 fail-fast gate，改为真实 FullOuterJoin 逻辑闭环。
- [ ] `LogicalJoin.PredicatePushDown` 增加 full join 分支：
  - full join 下两侧都是 preserved：`LeftConditions` / `RightConditions` 不得变成 child selection。
  - 需要审计：是否有规则在 full join 下错误使用 “inner/outer side” 概念。
- [ ] build schema nullable：
  - Logical plan builder / BuildPhysicalJoinSchema：full join 清两侧 NotNull flag。

### 2.3 Planner：Plan 枚举限制（只 HashJoin v1 / Root）

- [ ] `exhaust_physical_plans.go`：
  - full join 禁用 MergeJoin、IndexJoin、HashJoin v2（如果现有入口会枚举到）。
  - full join 禁用 MPP hash join。
- [ ] `plan_to_pb.go`（建议加断言型保护）：
  - 若 join type 为 full join 且尝试 ToPB/MPP，下发前直接返回 error（避免未来 refactor 误入）。

### 2.4 Executor：HashJoin v1 FULL OUTER JOIN

这里建议按“数据流三段”切任务，便于按 commit/PR review：

#### 2.4.1 Build 阶段：build side 过滤 + match tracking 基础设施

- [ ] 为 full join 保留 build side 的 rowContainer（含被 build filter 过滤掉的行，用于 tail 输出 unmatched）。
- [ ] 为 build side 维护 matched bitmap（可复用 `UseOuterToBuild=true` 的 `outerMatchedStatus` 机制，或复制一套更直观的 full-join 专用字段）。
- [ ] **注意 `IsNullEQ`**：build side hashing 对每个 key 的 NULL 行是否入表，必须由 `IsNullEQ[key]` 决定。

#### 2.4.2 Probe 阶段：matched 输出 + probe-side unmatched 输出

- [ ] `probe` 过程中，对每个 probe row：
  - 有 key bucket 且 other condition 产出至少一行：标记 build rows matched；输出 matched rows。
  - 否则：输出 probe-side unmatched（full join 需要保留 probe 侧）。
- [ ] “key bucket 有行但 other condition 全过滤掉”时，probe row 仍视为 unmatched（full join 必须输出）。
- [ ] **注意 `IsNullEQ`**：probe side hashing 对含 NULL 的 row，是否视为“可参与匹配”，取决于 `IsNullEQ`，不能简单沿用 `hCtx.HasNull[i] => unmatched`。

#### 2.4.3 Tail 阶段：输出 build-side unmatched（含被 build filter 过滤掉的行）

- [ ] 遍历 build side rowContainer，对未 matched 的行输出 build-side unmatched。
- [ ] build-side unmatched 输出时，另一侧列补 NULL。
- [ ] 对 build filter 过滤掉的 build 行：
  - 不能进 hash table（避免错误匹配），但仍应按 unmatched 输出。

#### 2.4.4 joiner 复用策略（建议最小侵入）

full join 需要 “左 unmatched 形如 (leftRow, NULL-right)” 和 “右 unmatched 形如 (NULL-left, rightRow)” 两种输出形态：

- [ ] 最小侵入方案：在 executor 内构造/复用两个 outer joiner：
  - “LeftOuterJoiner” 用于输出 left-side unmatched
  - “RightOuterJoiner” 用于输出 right-side unmatched
- [ ] matched rows 输出可复用 inner joiner 或 outer joiner 的匹配逻辑，但要确保 build-side match tracking 被正确设置。

---

## 3. 风险点清单（做实现时必须逐条对照）

- [ ] **Predicate pushdown**：full join 下 preserved 行不能因为下推 selection 被过滤掉（silent wrong result）。
- [ ] **`<=>` join key 的 NULL 处理**：不能把 “含 NULL => unmatched” 写死。
- [ ] **ToPB / MPP**：必须阻断 full join 下推（哪怕未来有人打开 MPP 枚举也不能 silently 变 inner join）。
- [ ] **结果集增长**：`<=>` 下 `NULL` 会参与等值匹配，重复 NULL 会产生 `N×M`；需要至少一个显式测试覆盖（避免未来有人“优化掉”）。
- [ ] **Spill**：rowContainer spill 后，tail 遍历与 bitmap 仍要正确（不要依赖 chunk 常驻内存）。

---

## 4. 建议测试矩阵（阶段 1 必备）

为了避免 UT 数量爆炸，建议挑少量 case 但覆盖关键维度。下面是一个最小覆盖矩阵（每个 case 都建议同时跑 `EXPLAIN` 校验 plan 只选 HashJoin root）：

### 4.0 PR1 fail-fast 回归（parser/ast 先行阶段）

在 PR1（尚未接入真实 full join 语义）阶段，优先保证“不会错算”：

1) `FULL OUTER JOIN ... ON ...`
   - 预期：planner 返回 `ErrNotSupportedYet`，错误信息包含 `FULL OUTER JOIN`。

2) `FULL OUTER JOIN ... USING(...)`
   - 预期：planner 返回 `ErrNotSupportedYet`（阶段 1 范围门禁）。

3) `NATURAL FULL OUTER JOIN ...`
   - 预期：planner 返回 `ErrNotSupportedYet`（阶段 1 范围门禁）。

4) `FULL JOIN`（无 `OUTER`）
   - 预期：保持 alias 语义，不被识别为 full outer join（parser/restore case 锁定）。

### 4.1 基本匹配 + 双侧 unmatched

1) `=` key
   - 左有一行 key=1，右有一行 key=1：输出 1 行 matched
   - 左多一行 key=2，右无 key=2：输出 left unmatched（右侧补 NULL）
   - 右多一行 key=3，左无 key=3：输出 right unmatched（左侧补 NULL）

2) `<=>` key
   - 左 key=NULL，右 key=NULL：输出 matched（不是 unmatched）

### 4.2 重复 key（多对多）

1) `=` key 重复：key=1 左 2 行，右 3 行 -> 输出 6 行（确认 join 行数正确）

2) `<=>` NULL 重复：key=NULL 左 2 行，右 3 行 -> 输出 6 行（显式覆盖 `N×M`）

### 4.3 other condition 把同 key bucket 全过滤掉

例如 `ON t1.k = t2.k AND t1.v > t2.v`：
- key bucket 存在，但过滤后 0 行：
  - probe row 必须输出 unmatched
  - build row（如果对方没有其它 probe 命中）最终也必须在 tail 输出 unmatched

### 4.4 build/probe filter（ON 中仅引用单侧）

例如 `ON t1.k = t2.k AND t1.f = 1 AND t2.g = 2`：
- full join 下 `t1.f=1` / `t2.g=2` 不能下推 selection 丢 preserved 行：
  - 不满足 filter 的行不能参与匹配，但要作为 unmatched 被保留输出。

---

## 5. 本文档的“完成定义”

当下面条件同时满足时，可以认为阶段 1 交付完成：

- [ ] 语法：`FULL OUTER JOIN` parse/restore 全通过
- [ ] planner：explain 中 join type 正确，nullable 正确，两侧 NotNull 清空
- [ ] plan 枚举：full join 只选 root HashJoin v1；禁止 MPP/merge/index/v2
- [ ] executor：full join 双侧 unmatched + `<=>` join key 正常工作
- [ ] tests：最小矩阵覆盖并稳定通过（包含 `<=>` NULL-NULL match 与 NULL 重复 `N×M`）

补充里程碑（防止 parser/ast 先行期间出现错误语义）：

- [ ] parser/ast 合入后到 PR2 合入前，`FULL OUTER JOIN` 必须稳定报 not supported（不可产生 inner join 结果）。
