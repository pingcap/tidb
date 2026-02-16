# Transactional COMPLETE refresh for materialized view (design notes)

本文是对 `REFRESH MATERIALIZED VIEW ... COMPLETE` 最直观（native）的实现方案的设计草案，目标是先落地一版**事务性的 complete refresh**，并满足并发互斥与元信息更新的要求。

> 备注：当前代码里用于记录 refresh 状态的系统表名是 `mysql.tidb_mview_refresh`（而不是 `mv_refresh_info`），核心字段是 `LAST_SUCCESSFUL_REFRESH_READ_TSO`。见 `pkg/session/bootstrap.go` 里系统表定义。

## 目标（MVP）

1. **只实现 COMPLETE refresh**（`FAST` 先不做）。
2. **事务性（all-or-nothing）**：一次 refresh 的数据替换与 refresh 元信息更新必须在同一个事务里提交/回滚。
3. **并发互斥**：对同一个 MV，多个 session 同时 refresh 时，只允许一个成功进入执行路径，其他直接报“正在 refresh/无法立即获得锁”的错误。
4. **更新 refresh 元信息**：成功提交前，更新 `mysql.tidb_mview_refresh` 中该 MV 行的状态，尤其是：
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <本次 refresh 事务 start_ts>`
5. **失败也要落元信息**：如果 refresh 执行失败，也要把 `LAST_REFRESH_RESULT='failed'` 以及失败原因写回 `mysql.tidb_mview_refresh`（并保证 MV 表数据不被部分写入）。

## 非目标（先不做）

- `FAST` refresh 以及与 MLOG 的增量消费逻辑。
- `WITH SYNC MODE` 的独立语义（MVP 里 refresh 本身就是同步执行，因此 `WITH SYNC MODE` 先暂时等同普通 COMPLETE；后续如果引入异步 refresh，再重新定义该语义）。
- 大 MV 的性能优化（例如避免超大事务、减少 delete 开销、swap table 等）。
- refresh 历史表（`mysql.tidb_mview_refresh_hist`）的写入与清理策略（MVP 先只更新 current 表）。

## 数据与元信息来源

- MV 的物理表是普通 table，标记在 `TableInfo.MaterializedView != nil`。
- MV 的定义 SQL 存在 `TableInfo.MaterializedView.SQLContent`，是 canonical SQL 的 `SELECT ...`（参见 `pkg/meta/model/table.go` 与 `pkg/ddl/materialized_view.go`）。
- refresh 状态系统表：
  - `mysql.tidb_mview_refresh`（主键 `MVIEW_ID`，字段包括 `LAST_REFRESH_RESULT / LAST_REFRESH_TYPE / LAST_REFRESH_TIME / LAST_SUCCESSFUL_REFRESH_READ_TSO / LAST_REFRESH_FAILED_REASON`）

其中：
- `MVIEW_ID` 可直接使用 MV 物理表的 `TableInfo.ID`。

## 建议的语句行为（用户视角）

支持：

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- 使用 current DB
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE; -- MVP 等同普通 COMPLETE
```

暂不支持或后续再定义：

```sql
REFRESH MATERIALIZED VIEW mv FAST;
```

## 核心执行流程（事务性 complete refresh）

最直观的实现就是你描述的那条主干流程，并补上锁与元信息更新：

1. 开启一个事务（建议 **pessimistic**，以保证 `FOR UPDATE NOWAIT` 立即生效）。
2. 在事务内，用 `SELECT ... FOR UPDATE NOWAIT` 对 refresh 信息行加锁（作为 refresh mutex）。
3. 设置一个 savepoint（用于失败时回滚 MV 数据变更，但仍可提交失败元信息）。
4. 删除 MV 旧数据：`DELETE FROM <mv_table>`。
5. 全量重算并写入：`INSERT INTO <mv_table> <mv_select_sql>`。
6. 在提交前更新 `mysql.tidb_mview_refresh` 该行（成功场景）：
   - `LAST_REFRESH_RESULT='success'`
   - `LAST_REFRESH_TYPE='complete'`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <txn start_ts>`
   - `LAST_REFRESH_FAILED_REASON = NULL`
7. 提交事务。

失败场景（例如 `INSERT INTO ... SELECT ...` 报错）：

1. `ROLLBACK TO SAVEPOINT` 回滚 MV 表的数据变更（保证 MV 数据不被部分更新）。
2. 更新 `mysql.tidb_mview_refresh` 该行（失败场景）：
   - `LAST_REFRESH_RESULT='failed'`
   - `LAST_REFRESH_TYPE='complete'`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_REFRESH_FAILED_REASON = <error string>`
   - 注意：`LAST_SUCCESSFUL_REFRESH_READ_TSO` **不更新**，保持上一次成功值。
3. `COMMIT` 提交失败元信息，然后把原始错误返回给用户。

伪 SQL（仅展示关键点）：

```sql
BEGIN PESSIMISTIC;

-- (A) mutex：锁住这一行，NOWAIT 失败则直接返回
SELECT MVIEW_ID, LAST_SUCCESSFUL_REFRESH_READ_TSO
  FROM mysql.tidb_mview_refresh
 WHERE MVIEW_ID = <mview_id>
 FOR UPDATE NOWAIT;

SAVEPOINT tidb_mview_refresh_sp;

-- (B) 全量替换
DELETE FROM <db>.<mv>;
-- 说明：TiDB 在 sql_mode 严格模式下会阻止“写入语句的 SELECT 部分”走 TiFlash/MPP。
-- 对 refresh 这种内部维护语句，更推荐在 internal session 上设置一个 flag（例如 `SessionVars.InMaterializedViewMaintenance`），
-- 让优化器跳过这段 strict-mode guard，从而允许 INSERT ... SELECT 的 SELECT 部分走 TiFlash/MPP。
INSERT INTO <db>.<mv> <SQLContent>;
  -- SQLContent 是 MV 定义的 SELECT（失败则回滚到 savepoint）
  -- 以便 refresh 的全量重算更容易利用 TiFlash 做大扫描。

-- (C) 更新 refresh 元信息（在同一事务里）
UPDATE mysql.tidb_mview_refresh
   SET LAST_REFRESH_RESULT = 'success',
       LAST_REFRESH_TYPE = 'complete',
       LAST_REFRESH_TIME = NOW(6),
       LAST_SUCCESSFUL_REFRESH_READ_TSO = <start_ts>,
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

### start_ts 的获取方式

需求是 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 写入“本次 refresh 事务的 start_ts（read tso）”。

实现上有两种简单办法：

1. **从事务对象拿**：`txn, _ := sctx.Txn(true)` 然后读 `txn.StartTS()`。
2. **复用现有模式**：在事务内执行完第一条语句（建议就是那条 `SELECT ... FOR UPDATE`）后，
   通过 `@@tidb_last_query_info` 的 JSON 字段取 `start_ts`（CREATE MV build 已经在用同样的方式）。

建议优先选 (1)（更直接、少一次 SQL），但 (2) 与现有 create-mview build 逻辑更一致。

## 代码落点建议（实现方式）

`REFRESH MATERIALIZED VIEW` 当前走 DDL statement 路径，执行入口已经存在但返回 unsupported：

- `pkg/executor/ddl.go` 会把 `*ast.RefreshMaterializedViewStmt` 转发到 `ddl.Executor.RefreshMaterializedView`
- 目前 `pkg/ddl/materialized_view.go` 里的 `(*executor).RefreshMaterializedView(...)` 直接 `return unsupported`

### 为什么会被当成 DDL statement

从“语义”上讲，`REFRESH MATERIALIZED VIEW` 更像一个 **utility / maintenance statement**：它不改 schema（不是 create/drop/alter），但会改 MV 的物理数据以及刷新相关的系统表元信息。

当前 TiDB 代码里它被放进 DDL statement 路径，主要是**工程组织与复用上的选择**：

- Parser/AST 层面：`RefreshMaterializedViewStmt` 定义在 `pkg/parser/ast/ddl.go`，`pkg/parser/parser.y` 里也把它放在 DDL statement 的 grammar 分支里，所以自然会被 executor 归类到 DDL statement。
- Executor 路径：`pkg/executor/ddl.go` 的 switch case 会把该语句转发给 DDL executor（`pkg/ddl/materialized_view.go`）。

需要强调的是：**走 DDL statement 路径并不等同于一定要走 DDL job（异步/owner/job queue/reorg）**。
MVP 的 complete refresh 完全可以在 DDL executor 里“同步执行一段内部 SQL”，本质上就是一个事务内的 `DELETE + INSERT + UPDATE`，不触发 schema version 变更，也不需要 DDL job。

如果后续希望它在 TiDB 的语句分类上更贴近 DML（比如想支持在用户显式事务里执行、或在 metrics/audit 上归类为 DML），那就需要把该语句从 parser 的 DDL 分支迁出（AST 位置与 executor 分发都要改），这是更大范围的重构，不建议作为 MVP 的阻塞项。

MVP 实现建议直接在 `pkg/ddl/materialized_view.go` 的 `RefreshMaterializedView` 里做“同步执行 + 内部 SQL”：

- 不新引入 DDL job（因为不改 schema、也不需要 reorg/checkpoint）。
- 用 `ctx.GetSQLExecutor().ExecuteInternal(...)` 执行上述 SQL 片段。
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

- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` 在一个事务里，对大 MV 可能产生超大事务，容易触发 txn size limit/写入放大/GC 压力。
  后续可考虑“新表构建 + 原子切换”的策略，但那通常涉及 DDL（rename/交换分区等），需要重新设计原子性边界。
- 为了支持未来更高级的 refresh（例如 FAST/增量、基于 MLOG 的 merge/upsert、或其它无法用 SQL text 表达的执行算子），可能需要逐步减少对“拼 SQL + ExecuteInternal”的依赖。
  一个可行的演进路径是新增一条 **internal-only 的 refresh 语句/AST**（例如 `REFRESH MV INTERNAL <mview_id>` 或 `REFRESH MATERIALIZED VIEW <name> INTERNAL ...`）：
  - 让它仍然走 TiDB 现有的 parse/optimize/build plan 流程（从而复用优化器），但由 planner 为该语句生成专用 plan node（而不是把复杂逻辑硬编码成 SQL 字符串）。
  - 外层 `RefreshMaterializedView` 框架保持不变：继续负责事务封装（BEGIN/COMMIT/rollback-to-savepoint）、对 `mysql.tidb_mview_refresh` 的 mutex 行锁、以及更新 refresh 元信息。
- 失败记录（`LAST_REFRESH_FAILED_REASON`）要在“不污染 MV 数据”的同时持久化，依赖 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 这类能力；
  如果未来出现不支持 savepoint 的执行场景（或需要更强的一致性语义），可能需要把“失败元信息落盘”与“数据更新”拆成两段事务，并重新定义并发互斥与状态可见性。
