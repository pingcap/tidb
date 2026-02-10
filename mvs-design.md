# MVS 设计说明（基于最新代码）

## 1. 范围

本文档对应当前工作区 `pkg/mvs` 的实现（调度 + 执行 + 元数据访问）。

主要文件：

- `pkg/mvs/service.go`
- `pkg/mvs/impl.go`
- `pkg/mvs/server_maintainer.go`
- `pkg/mvs/task_executor.go`
- `pkg/mvs/consistenthash.go`
- `pkg/mvs/priority_queue.go`
- `pkg/mvs/utils.go`

MVS 负责两类后台任务：

- 物化视图刷新（`mv`）
- MVLog 清理（`mvLog`）

## 2. 启动与接线

### 2.1 注册

`Domain.Init()` 中调用：

- `do.mvService = mvs.RegisterMVS(do.ctx, do.ddlNotifier.RegisterHandler, do.sysSessionPool, do.notifyMVSMetadataChange)`
- 位置：`pkg/domain/domain.go`

`RegisterMVS(...)`（`pkg/mvs/impl.go`）做三件事：

1. `NewMVJobsManager(ctx, se, &serverHelper{})` 构造服务。
2. 立即 `NotifyDDLChange()`，保证启动后第一次循环强制拉元数据。
3. 注册 `notifier.MVJobsHandlerID`，当前仅在 `meta.ActionCreateMaterializedViewLog` 时触发 `onDDLHandled()`。

### 2.2 启动

`dom.StartMVService()` 启动两个 goroutine：

- `mvService.Run`
- `watchMVSMetaChange`

`watchMVSMetaChange` 监听 etcd key `/tidb/mvs/ddl`，收到事件后调用 `mvService.NotifyDDLChange()`。
当 `etcdClient == nil` 时，watcher 不启动，服务只靠周期拉取。

## 3. 核心结构

### 3.1 `MVService`

职责：

- 周期/事件触发拉取元数据；
- 基于一致性哈希做 owner 过滤；
- 维护内存 pending + 小根堆；
- 将到期任务提交到 `TaskExecutor`；
- 处理成功、删除、失败重试。

关键字段：

- `lastRefresh atomic.Int64`：最近一次拉取时间（ms）
- `ddlDirty atomic.Bool` + `notifier Notifier`：DDL/任务完成唤醒
- `mvRefreshMu`：`pending map[string]mvItem` + `PriorityQueue[*mv]`
- `mvLogPurgeMu`：`pending map[string]mvLogItem` + `PriorityQueue[*mvLog]`
- `sch *ServerConsistentHash`
- `executor *TaskExecutor`

### 3.2 任务对象

- `mv`: `ID / refreshInterval / nextRefresh / orderTs / retryCount`
- `mvLog`: `ID / purgeInterval / nextPurge / orderTs / retryCount`
- `orderTs` 为调度排序键（ms）。
- `maxNextScheduleTs = 9e18` 作为“已投递未回收”的运行中哨兵，避免重复出队。

### 3.3 `ServerConsistentHash`

- `init()`：循环重试拿本节点 server ID（带 backoff，直到成功或 ctx 取消）。
- `refresh()`：拉全量 server 列表并重建 hash ring。
- `Available(taskID)`：判断当前节点是否 owner。

### 3.4 `TaskExecutor`

- FIFO 队列 + worker 池；
- 支持动态调整并发和超时；
- `timeout > 0` 时，超时仅释放 worker，不会取消真实任务；
- `Close()` 会清空未执行队列并等待运行中任务结束。

## 4. `Run()` 主循环

`MVService.Run()` 流程：

1. `sch.init()`；失败直接退出。
2. `executor.Run()` 启动 worker。
3. 进入循环，等待：
   - `timer.C`
   - `notifier.C`
   - `ctx.Done()`
4. 每轮逻辑：
   - 若来自 notifier：`forceFetch = ddlDirty.Swap(false)`。
   - 若 `forceFetch` 或 `shouldFetch(now)` 为真：
     - `sch.refresh()`
     - `FetchAllMVMeta()`
     - 若 `FetchAllMVMeta()` 失败，仍将 `lastRefresh=now`，避免紧循环重试。
   - `fetchExecTasks(now)` 从两类堆中取到期任务（并将 `orderTs` 设为 `maxNextScheduleTs`）。
   - 调用 `purgeMVLog(...)` 和 `refreshMV(...)` 提交执行。
   - 计算下次唤醒：`min(nextFetchTime, nextDueTime)`。

## 5. 元数据拉取与任务重建

### 5.1 `FetchAllMVMeta()`

顺序执行：

1. `fetchAllTiDBMLogPurge()`
2. `fetchAllTiDBMViews()`
3. 两者成功后更新 `lastRefresh = now`

任一步失败即返回 error。

### 5.2 `fetchAllTiDBMLogPurge()`

SQL：

- `SELECT t.MLOG_ID, UNIX_TIMESTAMP(l.PURGE_START), l.PURGE_INTERVAL, UNIX_TIMESTAMP(t.LAST_PURGE_TIME) ...`

行处理：

- 忽略空 `MLOG_ID`、空 `PURGE_START/PURGE_INTERVAL`；
- `lastPurgeTime` 为空时按 `time.Time{}`；
- 用 `calcNextExecTime(purgeStart, intervalSec, lastPurgeTime)` 算 `nextPurge`；
- 初始化 `orderTs = nextPurge.UnixMilli()`；
- 过滤非 owner（`!sch.Available(id)`）后进入 `buildMLogPurgeTasks`。

### 5.3 `fetchAllTiDBMViews()`

SQL：

- `SELECT t.MVIEW_ID, UNIX_TIMESTAMP(v.REFRESH_START), v.REFRESH_INTERVAL, UNIX_TIMESTAMP(t.LAST_REFRESH_TIME) ...`

行处理：

- 忽略空 `MVIEW_ID`、空 `REFRESH_START/REFRESH_INTERVAL`；
- `intervalSec = max(rowInterval, 1)`；
- `lastRefreshTime` 为空时按 `time.Time{}`；
- 计算 `nextRefresh`，并置 `orderTs`；
- 过滤非 owner 后进入 `buildMVRefreshTasks`。

### 5.4 `buildMLogPurgeTasks()`（按最新伪代码）

对 `newPending` 全量重建当前节点任务视图：

- 已存在任务：
  - 更新 `purgeInterval`；
  - 比较 `nextPurge` 是否变化；
  - 始终更新 `nextPurge`；
  - 若任务不在运行中（`orderTs != maxNextScheduleTs`）且发生变化，则同步更新 `orderTs = nextPurge.UnixMilli()` 并 `prio.Update(...)`。
- 新任务：插入 `pending` 与堆。
- 消失任务：从 `pending` 与堆删除。

### 5.5 `buildMVRefreshTasks()`

逻辑与 `buildMLogPurgeTasks()` 对称：

- 更新 `refreshInterval` 与 `nextRefresh`；
- 非运行中且变化时，更新 `orderTs = nextRefresh.UnixMilli()`；
- 处理新增与删除。

## 6. 执行路径（refresh / purge）

### 6.1 出队与投递

`fetchExecTasks(now)` 从小根堆连续取出到期任务：

- 一旦遇到 `orderTs == maxNextScheduleTs` 或未到期，即停止该队列扫描；
- 每个被选中任务先写回 `orderTs = maxNextScheduleTs`，再提交给执行器。

### 6.2 `refreshMV(...)` 成功/失败处理

每个任务调用 `mh.RefreshMV(...)`：

- 失败：`retryCount++`，按 `now + retryDelay(retryCount)` 重排；
- 成功且 `deleted=true`：从内存队列移除；
- 成功且 `deleted=false`：
  - `retryCount = 0`
  - 调用 `rescheduleMVSuccess`，同时更新：
    - `m.nextRefresh = nextRefresh`
    - `m.orderTs = nextRefresh.UnixMilli()`
  - 再 `prio.Update(...)`

### 6.3 `purgeMVLog(...)` 成功/失败处理

每个任务调用 `mh.PurgeMVLog(...)`：

- 失败：`retryCount++`，按退避重排；
- 成功且 `deleted=true`：移除任务；
- 成功且 `deleted=false`：
  - `retryCount = 0`
  - 调用 `rescheduleMVLogSuccess`，同时更新：
    - `l.nextPurge = nextPurge`
    - `l.orderTs = nextPurge.UnixMilli()`
  - 再 `prio.Update(...)`

### 6.4 重试策略

- 基础延迟：5s
- 指数翻倍
- 上限：5min

任务收尾都会 `notifier.Wake()`，让主循环尽快重算调度点。

## 7. `serverHelper` SQL 语义

### 7.1 `RefreshMV`

在 `withRCRestrictedTxn(..., BEGIN PESSIMISTIC)` 事务里：

1. 查 `mysql.tidb_mviews` 元数据；
2. 执行 `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`；
3. 查 `mysql.tidb_mview_refresh_hist` 最新记录；
4. 若 `REFRESH_FAILED_REASON` 非空，返回错误；
5. 计算并返回 `nextRefresh`。

无元数据时返回 `deleted=true`。

### 7.2 `PurgeMVLog`

同一事务内：

1. 查 `mysql.tidb_mlogs`（schema、mlog、related mv、purge 配置）；
2. 解析关联 MV，求 `MIN(LAST_SUCCESSFUL_REFRESH_READ_TSO)`；
3. `SELECT ... FOR UPDATE` 锁 `mysql.tidb_mlog_purge` 行；
4. 删 mlog 表中 `COMMIT_TSO = 0` 或 `<= minTSO` 的数据；
5. 更新 `mysql.tidb_mlog_purge`（时间、行数、耗时）；
6. 写 `mysql.tidb_mlog_purge_hist`（SUCCESS/FAILED）。

无元数据时返回 `deleted=true`。

实现细节：步骤失败时会尽量写 FAILED 历史，并通过返回 `failedErr` 让调度层进入重试分支。

## 8. 基础组件

- `Notifier`：多生产者单消费者唤醒器，`awake` 位实现信号合并。
- `PriorityQueue`：泛型小根堆，支持 `Push/Front/Update/Remove`。
- `ConsistentHash`：CRC32 环 + 虚拟节点（副本数限制 `1..200`）。

## 9. 当前约束与 TODO

1. DDL 触发面仍有限。
当前仅 `ActionCreateMaterializedViewLog` 触发跨节点通知，代码中已有 TODO 扩展到更多 MV 元数据变更事件。

2. 任务超时为“统计超时”而非“取消执行”。
超时后真实 SQL 仍会继续跑到结束。

3. `fetchAllTiDBMLogPurge()` 对异常 `PURGE_INTERVAL` 未做下限钳制。
`calcNextExecTime(interval<=0)` 会直接返回 `last`，会影响调度语义；如需强约束建议与 `fetchAllTiDBMViews()` 一样做最小值保护。
