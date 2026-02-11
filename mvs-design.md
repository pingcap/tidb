# MVS 设计说明（基于最新代码）

本文档基于当前工作区代码（`pkg/mvs/*` + `pkg/domain/domain.go`）生成，描述 MVS（Materialized View Service）的运行机制、调度模型和执行语义。

## 1. 职责与范围

MVS 是 TiDB 内部后台调度器，负责两类周期任务：

- 物化视图刷新（MV Refresh）
- 物化视图日志清理（MVLog Purge）

MVS 的目标：

- 从系统表加载 MV/MVLog 元数据
- 基于一致性哈希把任务稳定分配到单节点执行
- 根据执行结果重排、重试或删除任务
- 通过 DDL/etcd 通知降低元数据更新生效延迟
- 持续上报执行与队列指标

## 2. 关键代码文件

- `pkg/mvs/service.go`：`MVService` 主循环、任务队列、重排逻辑
- `pkg/mvs/impl.go`：`serverHelper`、SQL 执行事务、`RegisterMVS`
- `pkg/mvs/task_executor.go`：执行器（队列、worker、超时、背压）
- `pkg/mvs/task_backpressure.go`：背压控制器接口与 CPU/Mem 实现
- `pkg/mvs/server_maintainer.go`：节点发现与一致性哈希封装
- `pkg/mvs/consistenthash.go`：一致性哈希环
- `pkg/mvs/priority_queue.go`：优先队列
- `pkg/mvs/utils.go`：`Notifier` 通知器
- `pkg/mvs/metrics_reporter.go`：指标上报
- `pkg/mvs/task_handler.go`：任务处理接口定义

## 3. 启动与接线

### 3.1 Domain 初始化接线

`Domain.Init()` 中调用：

- `pkg/domain/domain.go`: `mvs.RegisterMVS(do.ctx, do.ddlNotifier.RegisterHandler, do.sysSessionPool, do.notifyMVSMetadataChange)`

`RegisterMVS(...)`（`pkg/mvs/impl.go`）完成：

1. 创建服务：`NewMVJobsManager(...)`
2. 启动前强制拉取标记：`NotifyDDLChange()`
3. 设置默认背压控制器：`NewCPUMemBackpressureController(0.8, 0.8, 200ms)`
4. 注册 DDL handler（当前只处理 `meta.ActionCreateMaterializedViewLog`）

DDL handler 执行 `onDDLHandled()`，由 Domain 层把事件 fanout 给其他节点。

### 3.2 运行时 goroutine

`StartMVService()` 启动两个 goroutine：

- `mvService.Run`
- `watchMVSMetaChange`

`watchMVSMetaChange` 监听 etcd key `/tidb/mvs/ddl`，收到事件后调用 `mvService.NotifyDDLChange()`。

## 4. MVService 结构与状态

`MVService` 中与调度相关的核心字段：

- `lastRefresh`：最近一次全量拉取元数据时间（毫秒）
- `sch *ServerConsistentHash`：任务归属判断
- `executor *TaskExecutor`：任务执行器
- `notifier + ddlDirty`：事件触发快速唤醒
- `mvRefreshMu`：MV refresh 的 `pending map + PriorityQueue`
- `mvLogPurgeMu`：MVLog purge 的 `pending map + PriorityQueue`

任务排序键：

- `orderTs`（毫秒时间戳）越小优先级越高
- `maxNextScheduleTs = 9e18` 表示“已投递/运行中”，防止重复出队

## 5. 主循环（Run）流程

`MVService.Run()` 执行顺序：

1. `sch.init()` 获取本节点 server ID（失败带 backoff 重试）
2. `executor.Run()` 启动 worker 池
3. 启动两个 timer：

- 调度 timer（动态下一次调度点）
- 指标 timer（`defaultMVMetricInterval = 1s`）

4. 循环 select：

- 调度 timer 到期
- 指标 timer 到期（执行 `reportMetrics`）
- notifier 事件（可触发强制拉元数据）
- `ctx.Done()`

5. 每轮执行：

- 若 `forceFetch || shouldFetch(now)`：刷新节点列表并拉取元数据
- 取出到期任务（并把任务 `orderTs` 置为 `maxNextScheduleTs`）
- 提交 refresh/purge 任务给执行器
- 计算下次唤醒点 `nextScheduleTime(now)`

失败保护：

- 元数据拉取失败时也会更新 `lastRefresh=now`，避免 tight loop。

## 6. 元数据拉取与队列重建

`fetchAllMVMeta()` 顺序执行：

1. `fetchAllTiDBMLogPurge()`
2. `fetchAllTiDBMViews()`

两者都成功后更新 `lastRefresh`。

owner 过滤逻辑：

- 新拉取任务会经过 `sch.Available(id)` 判断
- 非本节点 owner 的任务会被丢弃

`buildMLogPurgeTasks` / `buildMVRefreshTasks` 的重建原则：

- 已存在任务：更新 interval 和 next time
- 若 next time 变化且任务不在运行中：更新 `orderTs` 并 `prio.Update`
- 新任务：插入 pending + prio
- 已删除任务：从 pending + prio 移除

## 7. 任务出队与执行结果处理

### 7.1 出队规则

`fetchExecTasks(now)` 对两个优先队列分别扫描：

- `orderTs == maxNextScheduleTs`：停止（队首运行中）
- `orderTs > now`：停止（未到期）
- 到期任务：加入待执行列表，并把 `orderTs` 置为 `maxNextScheduleTs`

### 7.2 Refresh 任务处理

`refreshMV()` 提交 `mv-refresh/<mvID>`：

- 成功且 deleted：移除任务
- 成功且未删除：`retryCount=0`，按返回的 `nextRefresh` 重排
- 失败：`retryCount++`，按指数退避重排

### 7.3 Purge 任务处理

`purgeMVLog()` 提交 `mvlog-purge/<mvLogID>`：

- 成功且 deleted：移除任务
- 成功且未删除：`retryCount=0`，按返回的 `nextPurge` 重排
- 失败：`retryCount++`，按指数退避重排

重试退避参数：

- base：`5s`
- max：`5min`
- 指数增长并封顶

每次任务收尾都会 `notifier.Wake()`，加快下一轮调度。

## 8. TaskExecutor 设计

执行器特性（`pkg/mvs/task_executor.go`）：

- FIFO 环形队列
- 动态并发调整（`UpdateConfig`）
- 任务超时统计
- 背压（取任务前检查）
- 关闭时清空未执行队列并等待运行中任务收敛

配置接口（由 `MVService` 暴露）：

- `SetTaskExecConfig(maxConcurrency int, timeout time.Duration)`
- `SetTaskBackpressureController(controller TaskBackpressureController)`

默认配置：

- `defaultMVTaskMaxConcurrency = 10`
- `defaultMVTaskTimeout = 60s`

超时语义：

- 超时后只释放 worker 槽位并计数，不会中断真实任务 goroutine。

## 9. 背压控制

接口：

- `TaskBackpressureController.ShouldBackpressure() (bool, time.Duration)`

默认实现：`CPUMemBackpressureController`

- CPU 阈值：`0.8`
- 内存阈值：`0.8`
- 默认 delay：`200ms`

触发时机：

- worker 在从队列取任务前检查背压
- 若背压命中则 sleep 后重试取任务

## 10. serverHelper SQL 事务语义

### 10.1 RefreshMV

事务内执行：

1. 查询 `mysql.tidb_mviews`
2. 执行 `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE FAST`
3. 查询 `mysql.tidb_mview_refresh_hist` 最新记录
4. 若失败原因非空返回错误
5. 计算并返回 `nextRefresh`

若元数据不存在，返回 `deleted=true`。

### 10.2 PurgeMVLog

事务内执行：

1. 查询 `mysql.tidb_mlogs` 获取 purge 配置与关联 MV
2. 计算关联 MV 的最小 `LAST_SUCCESSFUL_REFRESH_READ_TSO`
3. `SELECT ... FOR UPDATE` 锁 `mysql.tidb_mlog_purge`
4. 删除目标 mlog 表中过期 `COMMIT_TSO`
5. 更新 `mysql.tidb_mlog_purge`
6. 写入 `mysql.tidb_mlog_purge_hist`
7. 计算并返回 `nextPurge`

异常时尽力写 FAILED 历史，返回错误让调度层进入重试路径。

## 11. 指标

`reportMetrics()` 每秒执行一次，更新：

- executor counter：submitted/completed/failed/timeout/rejected（增量）
- executor gauge：running/waiting
- service gauge：MV refresh pending/running、MVLog purge pending/running

## 12. 当前限制与注意事项

- DDL 触发面当前仅覆盖 `ActionCreateMaterializedViewLog`
- 任务超时是“执行槽位释放”，不是 SQL cancel
- `fetchAllTiDBMLogPurge()` 未对 `PURGE_INTERVAL` 做最小值钳制（`<=0` 时 `calcNextExecTime` 返回 `last`）
