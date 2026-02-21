# Materialized View Refresh（实现与设计笔记）

本文记录 TiDB `REFRESH MATERIALIZED VIEW`（`COMPLETE` / `FAST`）的当前实现与后续演进方向：当前 `COMPLETE` 已落地事务性刷新；`FAST` 已搭好 internal statement 的框架路径（可走 compile/optimize/executor），但暂未实现真正的增量刷新逻辑（planner 仍会返回 not supported 占位错误）。

> 备注：当前代码里用于记录 refresh 状态的系统表名是 `mysql.tidb_mview_refresh`（而不是 `mv_refresh_info`），核心字段是 `LAST_SUCCESSFUL_REFRESH_READ_TSO`。见 `pkg/session/bootstrap.go` 里系统表定义。

## 目标（当前实现的范围）

1. **事务性（all-or-nothing）**：一次 refresh 的数据替换与 refresh 元信息更新必须在同一个事务里提交/回滚。
2. **并发互斥**：对同一个 MV，多个 session 同时 refresh 时，只允许一个成功进入执行路径，其他直接报“正在 refresh/无法立即获得锁”的错误。
3. **更新 refresh 元信息**：成功提交前，更新 `mysql.tidb_mview_refresh` 中该 MV 行的状态，尤其是：
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <本次 COMPLETE refresh 事务 for_update_ts>`
4. **失败也要落元信息**：如果 refresh 执行失败，也要把 `LAST_REFRESH_RESULT='failed'` 以及失败原因写回 `mysql.tidb_mview_refresh`（并保证 MV 表数据不被部分写入）。
5. **COMPLETE refresh 可用**：事务内 `DELETE + INSERT` 完整替换 MV 数据。
6. **FAST refresh 先搭框架路径**：通过构造 internal-only AST 走 `ExecuteInternalStmt`，以便后续接入真正的增量刷新执行计划（当前仍会报 not supported）。
7. **权限语义先收敛到 MVP**：外层语句只做 `ALTER` on MV 的权限检查；refresh 执行时改用 internal session，避免把 `mysql.tidb_mview_refresh` 的系统表权限暴露给业务用户。

## 非目标（先不做）

- `FAST` refresh 的增量消费逻辑（包括但不限于 MLOG 消费、merge/upsert 等）。
- `WITH SYNC MODE` 的独立语义（当前 refresh 本身就是同步执行，因此 `WITH SYNC MODE` 已支持解析/执行，但行为与不带该选项一致；后续如果引入异步 refresh，再重新定义该语义）。
- 大 MV 的性能优化（例如避免超大事务、减少 delete 开销、swap table 等）。
- refresh 历史表（`mysql.tidb_mview_refresh_hist`）的写入与清理策略（MVP 先只更新 current 表）。

## 数据与元信息来源

- MV 的物理表是普通 table，标记在 `TableInfo.MaterializedView != nil`。
- MV 的定义 SQL 存在 `TableInfo.MaterializedView.SQLContent`，是 canonical SQL 的 `SELECT ...`（参见 `pkg/meta/model/table.go` 与 `pkg/ddl/materialized_view.go`）。
- refresh 状态系统表：
  - `mysql.tidb_mview_refresh`（主键 `MVIEW_ID`，字段包括 `LAST_REFRESH_RESULT / LAST_REFRESH_TYPE / LAST_REFRESH_TIME / LAST_SUCCESSFUL_REFRESH_READ_TSO / LAST_REFRESH_FAILED_REASON`）

其中：
- `MVIEW_ID` 可直接使用 MV 物理表的 `TableInfo.ID`。

## 语句行为（用户视角）

支持的语法（当前实现均会走统一的事务框架；其中 FAST 暂不支持真正执行逻辑）：

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- 使用 current DB
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE; -- 当前等同普通 refresh（refresh 本身同步）

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE FAST; -- 当前等同普通 refresh（refresh 本身同步）
```

当前限制：`FAST` 会在 planner/build 阶段返回不支持错误（占位），但 internal statement + ExecuteInternalStmt 的执行链路已经铺通。

当前权限语义（MVP）：

- 执行 `REFRESH MATERIALIZED VIEW` 需要目标 MV 的 `ALTER` 权限（外层语义权限）。
- refresh 内部会切到 internal session 执行 `DELETE/INSERT` 与 `mysql.tidb_mview_refresh` 更新，因此不要求调用用户对 `mysql.tidb_mview_refresh` 具有直接 DML 权限。
- 未来如果引入更细粒度权限模型（例如同时校验 base table 的 `SELECT`），再在此基础上扩展。

## 核心执行流程（事务性 refresh 框架）

最直观的实现就是“事务 + mutex 行锁 + savepoint + 数据刷新 + 元信息更新”。当前 `COMPLETE` 与 `FAST` 共享同一套外层框架，差异只在“刷新实现”步骤：

1. 从 DDL 的 session pool 获取一个 internal session，并在该 internal session 上开启事务（建议 **pessimistic**，以保证 `FOR UPDATE NOWAIT` 立即生效）。
2. 在事务内，用 `SELECT ... FOR UPDATE NOWAIT` 对 refresh 信息行加锁（作为 refresh mutex）。
3. 再用普通 `SELECT` 把 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 读一遍，与第 2 步的值对比：
   - 如果不一致，将本次 refresh 视为失败：更新 `LAST_REFRESH_RESULT='failed'` / `LAST_REFRESH_FAILED_REASON` 等并 `COMMIT`，然后返回错误终止 refresh（避免在同一事务里同时观察到 `start_ts` / `for_update_ts` 两个快照下不一致的 refresh 元信息）。
4. 设置一个 savepoint（用于失败时回滚 MV 数据变更，但仍可提交失败元信息）。
5. 根据 refresh type 执行刷新实现：
   - `COMPLETE`：`DELETE FROM <mv_table>` + `INSERT INTO <mv_table> <mv_select_sql>`。
   - `FAST`：构造 internal statement（见下文），走 `ExecuteInternalStmt`（当前 planner/build 会直接报 not supported 占位）。
6. 在提交前更新 `mysql.tidb_mview_refresh` 该行（成功场景）：
   - `LAST_REFRESH_RESULT='success'`
   - `LAST_REFRESH_TYPE = <'complete' | 'fast'>`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <COMPLETE refresh: txn for_update_ts>`
   - `LAST_REFRESH_FAILED_REASON = NULL`
7. 提交事务。

失败场景（例如 `INSERT INTO ... SELECT ...` 报错）：

1. `ROLLBACK TO SAVEPOINT` 回滚 MV 表的数据变更（保证 MV 数据不被部分更新）。
2. 更新 `mysql.tidb_mview_refresh` 该行（失败场景）：
   - `LAST_REFRESH_RESULT='failed'`
   - `LAST_REFRESH_TYPE = <'complete' | 'fast'>`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_REFRESH_FAILED_REASON = <error string>`
   - 注意：`LAST_SUCCESSFUL_REFRESH_READ_TSO` **不更新**，保持上一次成功值。
3. `COMMIT` 提交失败元信息，然后把原始错误返回给用户。

伪 SQL（仅展示关键点）：

```sql
-- 下面 SQL 在 internal session 内执行
BEGIN PESSIMISTIC;

-- (A) mutex：锁住这一行，NOWAIT 失败则直接返回
SELECT MVIEW_ID, LAST_SUCCESSFUL_REFRESH_READ_TSO
  FROM mysql.tidb_mview_refresh
 WHERE MVIEW_ID = <mview_id>
 FOR UPDATE NOWAIT;

-- (A2) consistency check：普通 SELECT 读出来的值必须与 (A) 一致，否则直接报错终止 refresh
SELECT LAST_SUCCESSFUL_REFRESH_READ_TSO
  FROM mysql.tidb_mview_refresh
 WHERE MVIEW_ID = <mview_id>;

SAVEPOINT tidb_mview_refresh_sp;

-- (B) 全量替换
DELETE FROM <db>.<mv>;
-- 说明：TiDB 在 sql_mode 严格模式下会阻止“写入语句的 SELECT 部分”走 TiFlash/MPP。
-- 对 refresh 这种内部维护语句，在 internal session 上设置一个 flag（例如 `SessionVars.InMaterializedViewMaintenance`），
-- 让优化器跳过这段 strict-mode guard，从而允许 INSERT ... SELECT 的 SELECT 部分走 TiFlash/MPP。
INSERT INTO <db>.<mv> <SQLContent>;
  -- SQLContent 是 MV 定义的 SELECT（失败则回滚到 savepoint）
  -- 以便 refresh 的全量重算更容易利用 TiFlash 做大扫描。

-- (C) 更新 refresh 元信息（在同一事务里）
UPDATE mysql.tidb_mview_refresh
   SET LAST_REFRESH_RESULT = 'success',
       LAST_REFRESH_TYPE = <refresh_type>,
       LAST_REFRESH_TIME = NOW(6),
       LAST_SUCCESSFUL_REFRESH_READ_TSO = <for_update_ts>,
       LAST_REFRESH_FAILED_REASON = NULL
 WHERE MVIEW_ID = <mview_id>;

COMMIT;
```

### 锁行为与错误语义

对 `mysql.tidb_mview_refresh` 做 `SELECT ... FOR UPDATE NOWAIT` 后有 3 种结果：

1. **成功返回 1 行**：说明拿到了锁，可以继续 refresh。
2. **返回锁冲突错误**：说明其他 session 已经在 refresh（或至少持有该行锁）。
   - TiDB 里 `FOR UPDATE NOWAIT` 的典型错误是 MySQL errno `3572`（`ErrLockAcquireFailAndNoWaitSet`）。
   - MVP 可以直接把该错误透传；体验更好的是把它包装成更语义化的错误（例如 “another session is refreshing this materialized view”）。
3. **没有报错但结果集为空（0 行）**：说明系统表里缺失该 `MVIEW_ID` 对应的行。
   - 这代表元信息不一致（不符合预期），应当报错并回滚事务。

### 为什么要用 pessimistic txn

`FOR UPDATE NOWAIT` 的目的就是“立刻失败而不是等待”，并且必须在事务里才有意义。
显式 `BEGIN PESSIMISTIC` 能保证锁在第一时间被获取并且冲突能立刻暴露出来，符合“mutex”的直觉语义。

### COMPLETE refresh 的 read tso（for_update_ts）获取方式

需求是：COMPLETE refresh 成功时，`LAST_SUCCESSFUL_REFRESH_READ_TSO` 写入“本次 refresh 事务的 for_update_ts（read tso）”。

原因是：在 `BEGIN PESSIMISTIC` 里，`INSERT INTO ... SELECT ...` 这类 DML 语句的读会使用 `for_update_ts`，
因此 MV 表里写入的数据快照也对应 `for_update_ts`。如果只写入 `start_ts`，就可能出现 “MV 数据包含了 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 之后的数据” 的表象，
并且会误导后续基于该 tso 的增量刷新/校验逻辑。

FAST refresh 目前尚未实现真正的增量刷新执行逻辑（planner 仍会报 not supported），因此这里先只定义 COMPLETE refresh 的 read tso 语义。

实现上有两种简单办法：

1. **从 Session 的 TxnCtx 拿**：在完成 refresh 的 DML（`DELETE` / `INSERT ... SELECT ...`）之后，
   通过 `sctx.GetSessionVars().TxnCtx.GetForUpdateTS()` 取 `for_update_ts`。
2. **复用 last_query_info**：在事务内执行完关键 DML（例如 `INSERT ... SELECT ...`）后，
   通过 `@@tidb_last_query_info` 的 JSON 字段取 `for_update_ts`。

建议优先选 (1)（更直接、少一次 SQL），并且能保证取到的是事务上下文的 `for_update_ts`（而不是某条语句的临时值）。

当前实现采用 (1)：在 COMPLETE refresh 的 DML 执行完成后，从 `TxnCtx` 读取 `for_update_ts` 并写入 `LAST_SUCCESSFUL_REFRESH_READ_TSO`。

## 代码落点建议（实现方式）

`REFRESH MATERIALIZED VIEW` 当前仍走 DDL statement 的分发路径，但执行方式是“同步执行 + 内部 SQL / internal statement”，不会进入 DDL job 队列：

- `pkg/executor/ddl.go` 会把 `*ast.RefreshMaterializedViewStmt` 转发到 `ddl.Executor.RefreshMaterializedView`。
- 实际执行在 `pkg/ddl/materialized_view.go` 的 `(*executor).RefreshMaterializedView(...)`：
  - 先从 DDL `sessPool` 获取 internal session，再在该 session 内跑整段 refresh 事务。
  - `COMPLETE`：事务内执行 `DELETE FROM` + `INSERT INTO ... SQLContent`（`ExecuteInternal`）。
  - `FAST`：读取 `LAST_SUCCESSFUL_REFRESH_READ_TSO`，构造 `*ast.RefreshMaterializedViewImplementStmt` 并走 `ExecuteInternalStmt`（当前 planner/build 返回 not supported，占位）。

### 为什么会被当成 DDL statement

从“语义”上讲，`REFRESH MATERIALIZED VIEW` 更像一个 **utility / maintenance statement**：它不改 schema（不是 create/drop/alter），但会改 MV 的物理数据以及刷新相关的系统表元信息。

当前 TiDB 代码里它被放进 DDL statement 路径，主要是**工程组织与复用上的选择**：

- Parser/AST 层面：`RefreshMaterializedViewStmt` 定义在 `pkg/parser/ast/ddl.go`，`pkg/parser/parser.y` 里也把它放在 DDL statement 的 grammar 分支里，所以自然会被 executor 归类到 DDL statement。
- Executor 路径：`pkg/executor/ddl.go` 的 switch case 会把该语句转发给 DDL executor（`pkg/ddl/materialized_view.go`）。

需要强调的是：**走 DDL statement 路径并不等同于一定要走 DDL job（异步/owner/job queue/reorg）**。
当前 refresh 的实现是在 DDL executor 里“同步执行一段内部逻辑”（本质上就是一个事务内的数据变更 + 系统表元信息更新），不触发 schema version 变更，也不需要 DDL job。

如果后续希望它在 TiDB 的语句分类上更贴近 DML（比如想支持在用户显式事务里执行、或在 metrics/audit 上归类为 DML），那就需要把该语句从 parser 的 DDL 分支迁出（AST 位置与 executor 分发都要改），这是更大范围的重构，不建议作为 MVP 的阻塞项。

当前实现位于 `pkg/ddl/materialized_view.go` 的 `RefreshMaterializedView`，核心做法是“同步执行 + 显式事务封装”：

- 不新引入 DDL job（因为不改 schema、也不需要 reorg/checkpoint）。
- 刷新逻辑统一在 internal session 上执行，避免复用当前用户 session 的事务/变量/权限环境。
- 通过 `BEGIN PESSIMISTIC` + `SELECT ... FOR UPDATE NOWAIT` 对 `mysql.tidb_mview_refresh` 做 mutex。
- 用 `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` 保证失败时 MV 数据不部分写入，但失败元信息可 `COMMIT` 落盘。
- 运行期间会临时将 `tidb_constraint_check_in_place_pessimistic=ON`（避免 pessimistic txn 下 savepoint 受限）。
- 执行刷新实现时会开启 `SessionVars.InMaterializedViewMaintenance`（允许 refresh 的扫描部分更容易走 TiFlash/MPP）。
- `COMPLETE` 使用 `ExecuteInternal(...)` 执行 `DELETE` + `INSERT`。
- `FAST` 使用 `ExecuteInternalStmt(...)` 执行 internal statement，并在返回 `RecordSet` 非空时 drain `Next()`，确保 executor tree 真正跑完后再 `Close()` 回收资源。
- 用 `sqlescape.MustEscapeSQL`（或等价安全拼接工具）来拼出 `%n.%n` 形式的表名，避免手写 quote。
- 用 `kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)` 作为内部 SQL 的 context，保持内部事务标记一致。

## 测试建议（后续落地时）

建议新增 executor UT（`pkg/executor/test/ddl/`）覆盖：

1. **基础正确性**：
   - 建 base table + mlog + mv
   - 插入 base 数据
   - 执行 `REFRESH MATERIALIZED VIEW mv COMPLETE`
   - 验证 mv 内容与 `SELECT ... GROUP BY ...` 一致
   - 验证 `mysql.tidb_mview_refresh.LAST_SUCCESSFUL_REFRESH_READ_TSO > 0` 且 `LAST_REFRESH_RESULT='success'`
2. **并发互斥**：
   - session A：开启 refresh，在拿到 `FOR UPDATE` 锁后 pause（可以用 failpoint 或者人为事务持锁）
   - session B：执行 refresh，期望报 NOWAIT 锁冲突相关错误
3. **元信息缺失**：
   - 手动删掉 `mysql.tidb_mview_refresh` 中该行
   - 执行 refresh，期望报“refresh info row missing”错误

## 已知限制与后续演进方向

- FAST refresh 目前只搭了“框架路径”，还没有实现真正的增量刷新逻辑：
  - 引入一个 **internal-only 的 statement AST**：`RefreshMaterializedViewImplementStmt`，只会在 executor 内部直接构造，不会由 parser 从 SQL text 解析得到。
  - 该 AST 携带两类信息：
    1. 原始的 `RefreshMaterializedViewStmt`（要求 `Type=FAST`）
    2. `LAST_SUCCESSFUL_REFRESH_READ_TSO` 的值（在 FAST refresh 路径里假设它**必须是非 NULL 的 int64**，否则直接报错）
  - 执行入口使用 `ExecuteInternalStmt(ctx, stmtNode)`，目的是让它未来可以走 TiDB 常规的 **compile / optimize / executor** pipeline，而不是走 `buildSimple` 的简单路径。
  - planner 侧已经把 `RefreshMaterializedViewImplementStmt` 从 `buildSimple` 的分支里拆出来，并单独加了 build 分支；但目前 build 会直接返回“不支持”的错误（placeholder），以便先把 code path 铺通。
  - `RefreshMaterializedViewImplementStmt.Restore()` 会输出形如 `IMPLEMENT FOR <RefreshStmt> USING TIMESTAMP <LAST_SUCCESSFUL_REFRESH_READ_TSO>` 的字符串，用于日志/toString；因为 parser grammar 不暴露这种语法形式，所以 Restore 不追求“可反向 parse”的语义。
  - `ExecuteInternalStmt` 返回的 `RecordSet` 如果非空，必须通过 `Next()` drain 才能保证整棵 executor tree 真正执行完成；因此 FAST refresh 的框架代码会在 `rs != nil` 且 `err == nil` 时主动 drain，再 `Close()` 回收资源（对未来可能生成 `INSERT INTO ... SELECT ...` 之类 plan 的实现更稳）。
- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` 在一个事务里，对大 MV 可能产生超大事务，容易触发 txn size limit/写入放大/GC 压力。
  后续可考虑“新表构建 + 原子切换”的策略，但那通常涉及 DDL（rename/交换分区等），需要重新设计原子性边界。
- 为了支持未来更高级的 refresh（例如 FAST/增量、基于 MLOG 的 merge/upsert、或其它无法用 SQL text 表达的执行算子），可能需要逐步减少对“拼 SQL + ExecuteInternal”的依赖：
  - 当前引入的 `RefreshMaterializedViewImplementStmt` 可以作为第一步：先用内部 AST 串起 compile/optimize/executor 的链路，并把 executor-only 参数（比如 `LAST_SUCCESSFUL_REFRESH_READ_TSO`）以结构化方式传下去。
  - 后续再让 planner 为该 statement 生成专用 plan tree / executor（例如最终形成 `INSERT INTO mv SELECT ...` 或更复杂的 merge/upsert 计划），从而彻底替换 “拼 SQL + 执行 SQL text” 的接口。
- 失败记录（`LAST_REFRESH_FAILED_REASON`）要在“不污染 MV 数据”的同时持久化，依赖 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 这类能力；
  如果未来出现不支持 savepoint 的执行场景（或需要更强的一致性语义），可能需要把“失败元信息落盘”与“数据更新”拆成两段事务，并重新定义并发互斥与状态可见性。
