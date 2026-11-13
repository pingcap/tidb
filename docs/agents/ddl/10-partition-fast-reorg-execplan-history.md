# 历史 ExecPlan 与审查过程


本文保留旧方案及实验过程，不作为实施计划。当前计划见 [ExecPlan](10-partition-fast-reorg-execplan.md)。

# 完整分区 fast reorg：行增量协议与实现计划


> 2026-11-02 最终范围与状态合同见 [正式设计](../../design/2026-11-02-partition-global-sort.md)。2026-11-03 第 15 节收敛保留 handle 的纯 split 协议（每个新分区唯一旧父，可以同时拆多个旧分区）；整数 clustered 路径与 MDL on 前提保留。下文 nonclustered 映射属于后续扩展，不是首版前置任务。完整数据基线仍全部 ingest，未改变为 index-only 交付。



本 ExecPlan 按仓库根目录 `PLANS.md` 维护，是尚未实现的设计。当前授权范围是设计、独立模型与隔离 SQL 前提实验；产品执行路径和真实 TiKV/DXF 实现步骤留待后续。已执行项按 Progress/Artifacts 记录，不把计划中的 Go 测试当作已运行。初始设计基线为 `a49049a271fca7a1b4c2234fdafd6a1abd2d0645`；2026-11-02 在 `44facb0f374a16d39d5a7267714a9c5d0d7f17a4` 工作区重新核对关键路径并修订。当前不修改产品运行时代码；隔离 SQL probe 只启动自己的本地 unistore 进程。

## Purpose / Big Picture


使普通 `REORGANIZE PARTITION` 的行基线、新分区全部 local index、替代 global index 都通过排序和 SST ingest 构建。SST ingest 指把批量排序后的 KV 导入存储，替代逐批提交全量数据事务。正常用户 DML、DDL 元数据和并发增量合并仍使用事务。

用户提供的运行经验：几百 GB 数据的 reorg 行搬迁耗时数小时，同量级 ADD INDEX 可在一小时内完成。这是用户的观测，不是本次控制变量 benchmark；用于说明优化动机，不推导加速倍数。正确性优先于扫描次数，接受先完成行布局，再分别构建 local 和 global index。

使用者在大范围搬迁以及小范围重组但带全表 global index 的情况下，都能使用完整 fast 路径。验收同时检查 record/local/global 三类基线的执行证据、SQL/索引正确性、并发更新删除和故障恢复；不以仅索引加速作为交付。

当前主设计选择独立 DirtyRows 脏键空间，将基线构建和新布局并发写入隔离；repair 点读当前旧父分区行，不重放历史行值。现有 local index 编码、排序、ingest 和临时索引 merge 继续复用。首版统一纯拆分、无映射行身份：整数 clustered handle 及 nonclustered；每个新分区唯一旧父，可同时拆多个相邻旧分区。通用 nonclustered 合并映射移到后续扩展。源码已有的 MDL old-running-transaction 检查作为首版屏障，需补充 admission 与 MDL off 的持久互斥。

## Progress


- [x] (2026-11-03) 旧 binary 复现 RR 在无关表固定快照、拆分完成后首次访问目标表读到 0/4 行，报告 ps-8f2ngd1f；不是当前源码回归结论。
- [ ] 阻断项：确定 readTS/布局匹配合同并验证当前源码、计划缓存、只读/历史读/GC 路径；未关闭不视为设计正确性完成。
- [x] (2026-11-03) 用户认可纯拆分限制；实现 admission、两写者/生命周期原型，发现初始化 stale temp 值反例，当前协议改为仅 dirty key 加当前源修复。
- [x] (2026-11-03) 审查通用映射的过度泛化，补充 split 静态单射证明、反向父 record guard 和 BackfillState 复用；有限模型通过并有负向反例。
- [x] (2026-11-03) 在现有旧二进制/隔离 unistore 验证 SQL 父 guard/marker/fence 冲突、跨父移动、用户回滚、悲观 RR 空键锁、MDL 阻塞、多父 EXCHANGE 重复 row ID 的纯拆分。
- [ ] 当前源码的新 hook/allocator、悲观 RC、真实 TiKV/SST/DXF 故障与退出窗口仍未验证；未通过则该类型继续 txn。
- [x] 核实当前 record copy、local/global index、DML 双写与 ingest 时间戳入口。
- [x] 找到非 clustered 删除跳过不存在目标行的分支，明确旧 TS 导入路线的必要改动。
- [x] 定义候选行增量协议、状态、任务身份、恢复边界及验证入口。
- [x] 核对现有 rollback 截止点、DXF 取消入口和 GC 登记路径，补充阶段化异常协议。
- [x] (2026-11-02) 确定串行 Row → LocalIndex → GlobalIndex 主路线，补充不变量、状态屏障及索引复用前提。
- [x] (2026-11-02) 运行有限行协议模型：344 个状态、23 个终态满足行一致；去掉 Capture drain 后找到漏行反例。
- [ ] 在 mock 与真实 TiKV 原型上验证并发删除、行增量 merge 和导入恢复。
- [ ] 后续扩展：验证非 clustered 映射的完整生命周期和事务/存储成本；不作为受限首版的完成条件。
- [ ] 实现完整 record/local/global ingest、恢复和清理。
- [ ] 完成真实环境的 SQL、索引、GC、兼容性与性能验收。

## Surprises & Discoveries


`pkg/table/tables/partition.go:partitionedTable.RemoveRecord` 对非 clustered 新分区副本先读目标行；不存在时直接返回，不产生目标行删除。假设基线扫描后删除发生、目标还未导入，则仅把基线 commit TS 调早不能覆盖缺失的删除标记。这是代码路径推导的反例，还没有运行故障实验。

`pkg/ingestor/ingestctrl/local.go:SetTSBeforeImportEngine` 接受显式非零 TS，证明底层有指定导入版本的接口；但不能由此推导任意历史 TS 在线导入正确。`region_job.go` 和 `job_worker.go` 有导入 TS 在当前时间前一天至后一小时内的 `intest.AssertFunc`。这是测试断言，不是本次确认的生产时长上限，但与长期固定旧 TS 的设计有冲突。

`pkg/ddl/ingest/backend.go:BackendCtx.Register` 按 index ID 管理一组 engine。`pkg/ddl/backfilling_dist_executor.go:BackfillTaskMeta` 当前注明仅支持 index element。record KV 应有独立 output kind 与 engine/task 身份，不能伪装成某个普通 index ID。

`pkg/ddl/backfilling_operators.go:tableScanWorker.scanRecords` 使用任务读事务的 startTS；现有 add-index 并不保证全部任务共用 job.SnapshotVer。新行协议需要自行规定可重试的源快照，不能直接假定已有固定快照。

`pkg/meta/model/reorg.go` 已有 `DDLReorgMeta.Stage`，但已分配的 stage 值用于 modify-column。扩展分区协议时保留既有数值语义；同时 DML 只能依赖发布的 TableInfo 元数据，不能只把行状态写在后台 job 中。

`pkg/meta/model/job.go:Job.IsRollbackable` 对分区重组在 StatePublic 返回 false；`pkg/ddl/rollingback.go:onRollbackReorganizePartition` 同样拒绝这个状态，而转换函数显式支持从 DeleteReorganization 恢复旧分区。因此已切读不等于已越过 rollback 截止点，不能仅依据局部代码注释缩小已有窗口。

`pkg/dxf/framework/handle/handle.go:CancelTask` 只调用 task manager 的取消入口，没有在该函数内等待全部存储 RPC 结束；查不到活跃任务也会返回 nil。`pkg/ddl/job_worker.go:JobNeedGC` 对 Cancelled job 不进入普通 delete-range 登记。新协议不能把任一取消请求的成功返回当成“所有数据已清理”，尤其在已创建临时产物的情况下。

## Decision Log


- Decision: 不把最新行集收敛或模型通过等同于 SQL/DDL 正确性闭环；补充 readTS 与物理布局匹配的必需合同。
  Rationale: 旧 binary late-first-access RR 探针出现 fresh=4/old=0 的静默不一致。新布局可能没有旧 readTS 的历史；MDL 不能排空尚未访问目标表的旧事务。
  Date/Author: 2026-11-03 / Codex；端到端设计仍未完成。

- Decision: 当前首版 DirtyRows 不存行值/Put/Delete/applied，只存脏目标键及写入 nonce；repair 当前事务点读唯一旧父源行并检查目标路由。
  Rationale: 初始化旧/新 schema 写入交错可令旧 temp 行值早于 S，完整值 replay 已有明确反例；当前源修复使历史 marker 成为安全冗余。
  Date/Author: 2026-11-03 / Codex；下列 full-value replay 旧决策已被本条替代。

- Decision: RowBackfillState 复用 BackfillState；temp row 仅保存最终 Put/Delete，merge 统一重读、应用、删除，不存 applied 标记。
  Rationale: 已双写值重放一次幂等，读写冲突保持并发顺序；可省模式字段与跳过分支，代价是少量额外 record 写入。索引 merge 不作此更改。
  Date/Author: 2026-11-03 / Codex，模型 --replay-all 通过，真实存储待验证。

- Decision: nonclustered 首版优先单旧分区拆分，保留同一 handle，不建立通用 RowToken/SourceOwner/F/R/Live。
  Rationale: target 的唯一旧父关系提供正向单射证明；反向通过旧父 record-key 的原子冲突检查与自动 hidden handle 重试维持，不需要逐行映射。
  Date/Author: 2026-11-03 / Codex，待真实并发验证。

- Decision: 完整 record/local/global 基线 ingest 是统一交付目标。
  Rationale: 用户要求完整解决分区重组，不交付保留全量事务搬行的部分加速。
  Date/Author: 2026-09-15 / user and Codex。

- Decision: 行增量采用独立临时空间，先证明两阶段行、索引 ingest；融合扫描作为同一完整功能的可选性能优化。
  Rationale: 临时行空间使基线导入期间没有普通事务同时覆盖目标 record KV，减少对历史导入 TS 的依赖。两阶段便于证明，且三类基线都走 ingest。
  Date/Author: 2026-09-15 / Codex，候选设计，未实验验证。

- Decision: 先完成行 merge，再串行启动 local、global 的准备和基线；每组索引在自身扫描前通过 schema 边界启用增量。
  Rationale: 索引基线从已完整且持续更新的行布局生成；行 merge 只写 record，不重复制造索引事件。
  Date/Author: 2026-09-15 / Codex。

- Decision: 非 clustered prototype 使用稳定的正向映射和目标归属预留，切读后的旧布局维护使用反向映射。
  Rationale: 历史 EXCHANGE 可产生相同 row ID，删除时不能用不存在的目标行判断归属。该方案提供明确正确性基线，但每行映射成本是必须测量的设计风险。
  Date/Author: 2026-09-15 / Codex，未证明其性能达到交付要求。

- Decision: 固定旧导入 TS 保留为对照实验，不作为未经修改 DML 即可启用的捷径。
  Rationale: 缺失删除标记、GC 保护和重试写同一 MVCC 版本都需要独立处理。
  Date/Author: 2026-09-15 / Codex。

- Decision: rollback 使用持久化 abort 子阶段，保留 DeleteReorganization 的回退能力，Public 后前向恢复。
  Rationale: 需要隔离迟到 importer、旧 schema DML 和 GC，并在 owner 故障后继续回滚；已停止完整维护的旧索引不能作为恢复目标。
  Date/Author: 2026-09-15 / Codex。

- Decision: 第一版串行构建 Row → LocalIndex → GlobalIndex，record/local 融合延期，各索引组只在自身扫描前开始捕获。
  Rationale: 用户明确正确性优先且接受额外扫描；行收敛后索引可满足更接近既有 ADD INDEX 的输入前提，减少跨输出组协议。
  Date/Author: 2026-11-02 / Codex，结合用户约束。

## Outcomes & Retrospective


2026-11-03 本轮：初始 full-value merge 模型漏了 schema arming，补充 startup 后找到真实设计反例。当前选择 dirty-key/source-point-repair，并保留原模型为负向证据。现有二进制的 SQL primitive 和 MDL 实验通过，但 dirty binary 非当前源码，且不是新 hook 或 SST 验收。协议范围已收敛，产品实现与发布门槛未完成。

2026-11-03：纠正所有 nonclustered 必须逐行映射的过度泛化。正式设计第 14 节是当前首版选择；通用映射留作合并后续方案。该 split 协议尚未实现，有限模型不证明 allocator/事务重试/退出窗口。

本次产物是可审查协议及开发计划，没有运行时代码、实验结果或性能结论。比可行性文档进一步确定了行捕获空间与 merge 的主路线，并给出可以否定此路线的实验：若映射成本使基线仍产生不可接受的逐行事务开销，或无法保持旧、新布局互相维护，就修改协议并更新本计划，不能缩减为 index-only 交付。

## Context and Orientation


`pkg/ddl/partition.go:onReorganizePartition` 驱动分区 schema 状态。`doPartitionReorgWork` 当前强制事务回填；`reorgPartitionDataAndIndex` 先复制旧分区行，再回填新分区索引，最后补未变更分区的 global index。

`pkg/table/tables/partition.go` 是普通 DML 的新旧分区路由入口，处理 `AddRecord`、`RemoveRecord`、`partitionedTableUpdateRecord`。`newPartitionedTable` 为旧分区、新分区及未变更分区构造不同索引集合。修改这里必须同时验证相邻 schema 版本的事务。

global index 用逻辑 table ID 与 index ID 定位索引空间；local index 用 physical partition ID 与 index ID。普通 REORGANIZE 创建新 physical IDs，global index 再创建新 index IDs。本文不改变 SQL 的分区表达式含义；改变分区方式、LOCAL/GLOBAL 转换、NewTableID 的额外 SQL 变体另列能力矩阵。

“基线”是源快照中的整批行；当前“行增量”是受影响目标键的 dirty marker，不是历史行值或删除动作日志；“fence”是阻止旧阶段 worker 和旧 DML 协议继续写入的边界。发布元数据、等待 schema 同步、排除仍可提交的旧协议事务和停止旧 worker 都属于该边界，不能只检查所有节点已加载新 schema。

## Protocol: namespaces and row identity


将临时数据放在 job 专用且能持久化清理的 KV 范围。建议为临时行和映射各分配内部 physical ID，用正常分配器防止与用户表空间冲突。它们不是新的用户可查询表，不为它们触发用户约束、统计或通知；编码由 `tablecodec` 提供专用 helper。这里描述的是拟新增布局，不是现有 API。

当前首版 DirtyRows 以目标 `(newPID,h)` 为 key，仅保存非空 marker 和事务写入 nonce。不保存历史行值、Put/Delete、Capture/DualWritten 或 applied 标记。删除也写 marker，不能直接删除临时 key。repair 从同一当前 KV 事务中点读 `parent(newPID),h`，重新计算新路由：源不存在或已离开目标则删目标，否则编码当前源值写目标；再删 marker 并写逻辑 range fence nonce。marker nonce 不作提交时间排序，不与 S 比较过滤。


用户修改权威布局时，在同一事务内写源行/源索引及 old/new image 对应的脏目标键。无逐行映射；未变更 physical key 不重复写，跨范围进入/退出分别标记对应一端。目标索引尚未启动时不写其临时变化；对应 local/global 索引阶段启动后，才在同一用户事务内维护该组索引的临时变化。事务回滚时全部撤销。Capture 状态不写新布局的普通 record KV，且禁止从“新 record 尚不存在”推导出无需记录删除。

clustered handle 在合法普通 REORGANIZE 中保留原 handle；分区约束、键变更和允许的组合仍需测试。以下为首版排除的 nonclustered 后续扩展：映射建议以 `(sourcePID, sourceHandle, targetPID)` 为正向 key，记录目标 handle；目标预留以 `(targetPID, targetHandle)` 为 key，记录所属来源。扫描和 DML 原子地获取/创建同一个映射；优先预留原 handle，已有其他来源占用则从真实 allocator 分配替代 handle，重试必须返回已保存映射。

映射不能依据 record 是否已导入决定归属。源行删除后映射和预留保留到 job 退出，保证快照重试仍找到同一目标；来源 key 复用的最终状态由事务序列和 RowDelta 覆盖决定。跨分区移动同时为旧目标写 Delete、为新目标写 Put，包含旧、新 handle 的变化。

切读后仍需向旧布局维护 DML，不能简单将新 handle 当作旧 handle。为已经映射的行保存反向关系；切读后新插入行需要原子建立可双向使用的 identity，并处理旧目标可能存在的 row ID 冲突。`_tidb_rowid` 的可观察变化与现有重组语义一并验证。映射不因切读完成立即 GC，要等旧布局不再读写且恢复/回滚不再需要它。

这项映射可能为非 clustered 基线带来 O(N) 元数据写入。原型需要计量事务次数与字节，尝试批量预留和批量持久化；不得承诺它天然廉价。映射写入可以使用事务，因为它不是全量 record/local/global KV 的事务回填，但若成本抵消 ingest 收益，设计仍不满足性能目标。避免为无冲突行保留逐行映射的优化，需要另外证明映射的可重建性和并发预留安全。

## Protocol: stages and DML behavior


2026-11-02 主路线确定为串行的 RowBuild → LocalIndexBuild → GlobalIndexBuild。第一版不融合 record 与 local index 的基线。两者融合会迫使 local 增量从搬行开始捕获，并要求 record/local 多输出组共同完成、fence、恢复；这增加证明面，先不采用。本文状态名都是拟议名称，不是已有 API。

job.Phase 只决定后台工作；发布到 TableInfo 的 RowState 决定前台行为；每个索引组的 IndexInfo.State/BackfillState 独立决定索引写入。不能复用一个 Partition.DDLState 同时表达这三个维度。现有 newPartitionedTable 会把 local 索引克隆状态设成 pi.DDLState，接入时需用持久化的目标布局索引状态覆盖，避免 WriteReorganization 下过早维护普通目标索引。

| 后台 Phase | RowState | 受影响范围的用户事务 | 新 local / 新 global index | 完成条件 |
| --- | --- | --- | --- | --- |
| Prepare | 尚未启用 | 旧布局读写 | 均未启动 | 资源和协议归属持久化 |
| CapturePrepare | Capture 的兼容过渡 | 旧布局，逐步启用行捕获 | 均未启动 | 未捕获事务退出；捕获覆盖所有后续变化 |
| RowBuild | Capture | 旧布局 + RowDelta；不写目标 record | 均未启动 | 固定 S 的全部 record 导入成功且 importer drain/fence |
| RowReady | DualWriteReady | 旧布局 + 目标 record + 完整 temp row | 均未启动 | Capture-only 事务全部退出 |
| RowMerge | DualWriteMerging | 同 RowReady | 均未启动 | 不再存在未应用的 Capture delta |
| RowDirect | Direct | 旧布局 + 目标 record；不再生成 Capture delta | 均未启动 | 发布完整行布局，索引可安全开始准备 |
| LocalPrepare / LocalBuild / LocalMerge | Direct | 两套行 + 旧索引 + 按 BackfillState 维护新 local | local 按已有 fast add-index 协议；global 未启动 | 新分区全部 local 正确且转入持续维护 |
| GlobalPrepare / GlobalBuild / GlobalMerge | Direct | 两套行 + 旧索引 + 新 local + 按 BackfillState 维护新 global | global 按已有 fast add-index 协议 | 全表替代 global 正确且转入持续维护 |
| Switch | Direct | 按既有 DeleteReorganization 协议双向维护 | 新索引可读，旧索引保留回退能力 | 旧读写协议退出 |
| Retire / Finish | 新布局权威 | 旧布局逐步退出，沿用 Public 边界 | 新索引正常维护 | 清理描述持久化，映射使用者退出 |

CapturePrepare 沿 DeleteOnly/WriteOnly 的兼容过渡传播删除、插入和更新捕获；只有仍可提交的未捕获事务全部退出后才取得源快照 S。过渡期只写脏目标键；DeleteOnly 的部分捕获或其他 S 前 marker 可保留，因为 repair 点读当前源而不重放其历史动作。S 仍必须在未完整标记的事务退出后取得。

RowBuild 扫描 DroppingDefinitions，用新表达式路由及稳定 identity 生成 record KV，再排序和导入。此阶段 importer 是目标普通 record 的唯一写入者。用户事务只修改旧布局与 RowDelta；目标 local/global 索引尚不存在也不被查询。旧布局的索引和约束完整维护，内部目标 record 写入不触发新布局的提前约束检查。

所有 record 组成功并确认存储侧导入终止后，发布 DualWriteReady。此时前台对目标 record 使用内部 raw Put/Delete，不能调用会更新未构建索引或检查不完整目标索引的普通 AddRecord/UpdateRecord。跨目标分区移动在同一事务写旧目标 Delete、新目标 Put 及两份 delta。源布局仍负责 SQL 约束。

DualWriteReady 与 Capture 可以短暂共存，但禁止此时启动 RowMerge。等待 Capture-only 事务退出后，再发布/同步 DualWriteMerging。其间 Capture 的延迟提交只进入 delta，不会永久遗漏，因为 merge 尚未开始。

Row repair 在当前同一 KV 事务读取 Open range fence、marker 和旧父分区源行；按源是否存在及新路由是否属于该目标选择 Put/Delete，删除 marker 并写 fence nonce，再提交。missing marker 跳过。source read 不能用 S 或缓存；发生冲突重新取当前源。只改 record，不发新 handle、不生成索引事件。

merge 可以顺序扫描专用 delta 空间并持久化已完成 cursor。屏障后不再有 Running-only 提交：若扫描前旧 temp 被双写 DML 覆盖，目标已同步；若扫描后产生新 temp，目标也已同步。因此不要求临时空间物理清空。cursor 只在该范围 merge 事务成功后推进；提交结果未知时保守重扫，不能推测成功。

完成行 merge 后发布 Direct，等待必要的兼容边界。此后始终维护完整目标行，并开始 LocalPrepare。残留 temp 值等所有使用者退出后异步 GC；旧双写 temp 事务与 Direct 共存时两者都写目标 record，仍通过该 key 的事务冲突序列化，不再引入 Capture-only 写入。

LocalPrepare 只对 AddingDefinitions 的 local 索引实例发布 DeleteOnly → WriteOnly → WriteReorganization 与 BackfillStateRunning。未变更分区及旧分区的 local 索引状态不变。等未捕获 local 增量的事务退出后才扫描新分区。LocalBuild 使用已有 index KV 编码、global sort 和 SST ingest；完成后按 ReadyToMerge → Merging 合并临时索引，再保持新 local 的正常维护。clustered primary 已是 record，不另建 primary KV；nonclustered primary 等实际 secondary KV 仍属于 local 组。

GlobalPrepare 在 local 完整之后启用替代 global index 的临时写入，覆盖全表 DML，包括未变更分区。GlobalBuild 扫描 AddingDefinitions ∪ (Definitions − DroppingDefinitions)，每个物理分区恰好一次，只输出替代 global indexes。完成后沿现有 fast add-global-index 的 merge 协议收敛。无某类索引时跳过对应组，但所有行屏障不省略。

只有 row/local/global 都完整且持续维护，才沿原有 WriteReorganization → DeleteReorganization 切读；旧 schema 与旧布局的退出及 Public 不可回退边界不改变。这里“完整”是逻辑完成，不是取消了所有前台写入，或所有 temp key 物理清空。

## Correctness argument and proof obligations


这是条件化正确性论证，不是已经验证的存储协议。证明目标是：每个成功提交的用户事务保留 SQL 语义；切读时新布局包含且仅包含同一逻辑行集；所有新索引指向当前正确的目标 partition/handle。重启、回滚和清理也属于正确性，不只比较行数。

I1：切读前旧布局始终权威且完整，用户约束由它维护。捕获失败使原用户事务失败，不能只提交旧布局。切读后仍需反向维护的窗口遵守同样原子性。

I2：每个新分区唯一旧父、搬迁保留 handle，保证同一源快照的目标 key 单射。静态 parent 计划在扫描/DML/repair/索引/恢复中一致；源槽位复用按当前状态修复，不为历史逻辑行建立 token，也不按 raw value 相同去重。

I3：S 之后每个影响目标布局的提交都有同事务的目标脏键 marker，或已在 Direct 阶段原子维护目标 record。跨分区移动是旧目标删除加新目标写入，二者同事务；退出受影响范围也必须删除旧目标，不能只判断更新后的路由。

I4：基线导入与前台写目标 record 在时间上不重叠。发布 Ready 的前置条件是所有组已持久完成、没有在途且可能迟到的 ingest。generation 检查是必要但非充分条件。

I5：RowMerge 开始后不再有 Capture-only 提交。每个未应用 temp 最终被 merge，或被已同时写目标的后续 DML 覆盖；merge/DML 通过同一 delta key 写冲突序列化。Direct 之后不再生成未应用增量。

I6：每类索引开始扫描之前，目标行完整且持续更新，该组索引增量协议已经覆盖所有可提交事务。扫描源集合、索引集合、handle 和编码语义满足原 ADD INDEX 协议前提，才允许复用其正确性结论。

I7：切读时 row/local/global 的完成标记持久化且都可持续维护；旧可见性退出之前保持必要双写。GC 只删除已无使用者、无迟到后台写入且有持久化 ownership 的范围。

在 I1–I5 成立时，S 后未变目标由基线提供；变化目标有 marker，由当前源点读修复或 Direct 双写提供正确值。source 消失/离开 q 则删目标，仍属于 q 则复制当前值。若并发 DML 改变该目标的正确值，它也写同一 marker，与 repair 删除冲突；不影响该目标则其正确值不变。由 I2 将逐 key 结论提升到逻辑行集一致。I6 允许分别对 local/global 应用已有 index-build 协议；I7 才允许将新布局发布给 SQL。

### Adjacent states and transaction barriers


| 相邻协议 | 为什么可短暂共存 | 禁止推进的条件 |
| --- | --- | --- |
| 未捕获 / CapturePrepare | 尚未选 S，旧布局权威 | 未捕获事务仍可在 S 之后提交 |
| Capture / Ready | 基线已结束，未 merge；Capture 最终仍在 delta | 仍有 Capture-only 提交可能时不能扫描 merge |
| Ready / Merging | 两者 DML 完全相同，区别仅后台许可 | Capture-only 事务尚未被排除 |
| Merging / Direct | 两者都原子更新目标 record | 仍有未应用的 Running 阶段 temp 或 merge 提交未知 |
| 各索引 Running / ReadyToMerge / Merging | 沿已存在的索引协议，需逐组确认 | 未捕获索引写入者未退出、迟到 importer |
| 切读前 / DeleteReorganization | 两套行与索引完整，双向映射存在 | 任一布局停止维护或身份无法反向解析 |
| DeleteReorganization / Public | 沿现有兼容退出，不扩展回滚窗口 | 旧协议事务和 map 使用者未退出，不能 GC |

每个需要排除旧写入者的转换都发布 schema diff，覆盖逻辑表与受影响 physical IDs；global 捕获边界还要覆盖未变更分区。仅修改 job.Phase 不会让远端事务感知。pkg/sessiontxn/isolation/base.go 的 SchemaChecker 使用 TableDeltaMap 中的 physical IDs，且 MDL 开关改变校验方式；因此需分别证明 MDL on/off 下长事务如何被等待或拒绝。不能只说“schema sync 已完成”。若现有 schema/MDL 机制无法证明该屏障，需新增明确协议或限制启用范围，不能默许通过。

### Counterexamples and evidence


漏删反例：目标尚无基线时删除，若不保存 Delete，随后 ingest 会复活行。漏行反例：Capture-only 事务在 merge cursor 扫过后才提交，行只留在 delta；不能进入 Direct。覆盖反例：merge 使用旧读值且不冲突检查，覆盖已经提交的新值。身份反例：两个旧分区相同 row ID 被合到同一目标，raw KV 覆盖造成少行。索引反例：先导入 local 基线 u→A，源端 A 删除并由 B 复用 u；直接对尚未收敛的新 local 做普通唯一检查，会误报重复。迟到导入反例：已发布 Direct，失联 importer 恢复后再次覆盖用户新值。

可执行的有限模型见 [partition_row_delta.py](models/partition_row_delta.py)。它固定两个目标 identity，覆盖 update、跨目标 move、delete、reinsert、延迟 Capture 提交、merge 读写交错和冲突重试，穷举模型内全部可达状态。344 个状态、23 个终态满足 source == target；--unsafe 移除 Capture drain 后产生具体漏行反例，退出码为 1 是预期。这个数量只描述该有限输入，不代表无限状态或所有数据值已证明。

模型假设用户事务及 merge 原子提交、同 delta key 写冲突有效、identity 稳定、固定源快照、importer 已被 fence。它不模拟真实 TiKV MVCC、索引、DDL/MDL、映射分配、故障、GC、CDC 或外键。模型通过只能排除其抽象内的反例，不能替代真实实验。后续应以真实 failpoint 固定模型中的失败时序，并在代码层确认事务选项建立相同的冲突关系。

## Index reuse contract


保留已有 global sort 的 index encoding、外部文件、MergeSort、WriteAndIngest、duplicate detection、temporary index value 格式及 merge 规则。特别保留 unique delete 的 handle/行存在性检查；不要自创“最后一个 temp value 覆盖即可”的索引算法。

适配集中在调用边界：持久化显式扫描集合、分区级索引状态、阶段任务身份及分区 DDL 错误处理。generateReadIndexPlan 当前遍历旧 Definitions，不能原样调用；executor 会重新读 TableInfo，仅改 owner 的 table clone 无效。Local 组只扫描 AddingDefinitions；Global 组一次覆盖完整最终物理集合，不能按新分区/未变更分区拆成两轮独立 global 构建而丢失统一 duplicate/checkpoint 语义。

现有 readIndexStepExecutor 每个任务使用一个 indexes 列表。串行 Local/Global 组避免一个 subtask 对不同分区输出不同索引集合：local 任务固定 local IDs，global 任务固定 global IDs。允许组内多个索引，但要审查 local 元数据的 PID/engine 分组和 unique 重复检测域，不能让不同 PID 的合法相同 local key 误判冲突。

复用验收先跑现有 ADD INDEX/ADD GLOBAL INDEX 并发、唯一键复用、NULL、common handle、GlobalIndexVersion、恢复等回归用例，再增加“旧布局仍读写、新分区尚未公开”的适配用例。跨分区扫描可能看到同一唯一键的不同时间版本：是否沿现有 global sort 重复检测正确处理，需要确定性实验；如果不能证明，不改变成熟 duplicate-check 算法来掩盖，而是审查扫描快照、路由和 temp 协议前提，必要时回到设计。

## Protocol: snapshots, GC, and large jobs


行基线固定 S，保证 SST 任务可重建。DirtyRows 只记录键，不过滤 S 前 marker；repair 用自己的当前事务源视图而非 S。初始化先捕获 v1、后旧 schema 未捕获提交 v2 的情形已否定 full-value replay：S 为 v2，旧 temp=v1 不能覆盖回来。源槽位复用按当前源状态修复；nonce 不是 commitTS。

S 需要保护到所有 record 基线任务完成、旧 importer 被 fence 且无需再读源快照。参考 `br/pkg/gc/manager_global.go` 的 service safe point 与 `manager_keyspace.go` 的 keyspace barrier 行为，在适合 DDL 的公共层实现生命周期管理；不因 BR 有 helper 就直接引入其 package 依赖。

保护建立后检查它是否真的覆盖 S，owner 切换和暂停期间续约。若 GC 已越过 S，不能静默刷新 S 后继续旧 manifest；应停止写入并回滚，或者执行另行证明的整轮重建协议。释放保护时旧任务不能再恢复读 S。长期 job 的 GC 保留成本要暴露并计量。

当前 ingest 的 import TS 会由 checkpoint/engine 路径管理，可以与 S 不同。记录组的 TS、已导入 watermark 和输出 manifest 一起保持稳定；对超长暂停与部分导入任务，不得只为绕过 TS 断言而重分配时间戳。测试断言的时间范围与真实存储约束分别验证。

## Interfaces and Dependencies


拟在 `pkg/meta/model/reorg.go` 增加版本化 partition reorg job metadata，在分区 TableInfo 模型中保存 DML 可见的行状态与临时空间/映射身份。字段逻辑上包括 ProtocolVersion、Phase、RowBackfillState、SourceSnapshotTS、SourcePartitionIDs、TargetPartitionIDs、ParentByNewPID、IndexGroups、DirtyRowsPhysicalID、RangeFenceIDs、TaskGeneration、ManifestReference、CleanupDescriptor。名称为拟议字段，不保证都应直接放进同一 struct；大的 range/manifest 存到任务元数据，避免膨胀每个 schema 版本。

异常协议还需 AbortPhase、OriginalFailure、CleanupFailure、RollbackFromPhase 和已知在途导入身份。先持久化资源分配意图、静态 parent plan 与确定性 job/generation 归属，再创建外部产物；恢复可按归属发现“已创建但成功响应丢失”的对象，避免只有内存知道资源存在。

拟增加 `pkg/ddl/partition_ingest.go` 作为分区 handler 与行 pipeline 的衔接，新增 `pkg/table/tables/partition_reorg_delta.go` 管理普通 DML 的临时行和身份逻辑。对应索引组启动之前，DML 不生成该组 temp index；行阶段不维护不完整的目标索引。索引继续使用 `pkg/ddl/backfilling_read_index.go` 与 `backfilling_merge_temp.go` 的能力。数据编码的 shared helper 放在 `pkg/tablecodec`，确保 DDL worker 和 DML 生成相同 key/value。这些是待创建的文件，不是当前存在的路径。

行任务元数据有明确 `record` kind；索引任务保留 `index` kind。Task key 同时包含 job、multi-schema 序号、protocol generation、phase 和 group。每个 group 对应一套稳定的源范围、输出目标集合、数据 kind 和 manifest。暂停、取消、历史任务查询、统计和 cleanup 全部使用同一身份构造方法。

公共 ingest adapter 接受显式 kind 与 engine identity，不借用一个假 index ID 触发 duplicate-check 或错误转换。首版 record 身份由纯拆分的唯一旧父和原 handle 保证；异常来源即使 raw value 相同也不去重。通用独立影子 handle 预留不纳入首版。索引 duplicate detection 仍交给索引协议。

索引 planner 接收每个 physical partition 的输出 index IDs；不能将全局 index 列表无差别用于未变更分区。远端 executor 重建同一静态 parent plan，校验 generation/phase。纯 split 基线保留 handle，不调用 allocator 或 AddRecord；源 DML 的候选重试使用真实会话 allocator，不可借 fake allocator。record repair 的旧 attempt 通过事务 range fence 阻止，不能只靠读取阶段后再写入。

## Idempotence and Recovery


每个基线组有源快照、源范围、目标映射版本、排序产物标识、import TS 与已导入状态。某组成功但 job 阶段未更新时，恢复读取同一成功 manifest；某组只部分成功时，使用原任务的可恢复进度，不能混入另一个快照或映射。

从 RowIngest 转入 DualWriteReady 前，先确认所有组成功并阻止旧 importer 后续写入，再发布 DML 状态。不能用“owner context 已取消”代替远端任务终止确认。IndexIngest 同理；直接写已开始后，迟到的基线可能覆盖新值，所以这个边界是正确性条件。

行 delta 与用户 DML 在同一事务持久化，owner 故障不丢捕获。RowMerge 通过当前 delta value 和事务冲突恢复，任务 cursor 可以重复处理已完成范围，但不能跳过尚未处理的 Capture value。切换后新生成的 temp value 已与目标同事务更新，不要求重新全量扫描。

取消时先停止并 fence 所有 row/index importer 和 merge worker，再沿原可回滚 schema 窗口恢复旧布局、停止新协议 DML，最后持久化 GC 描述。首版清理包括新分区、新 global indexes、local/global temp index、DirtyRows、排序产物和 GC barrier；没有逐行正反映射。成功清理旧分区/旧 global 等临时数据；Closed repair fence 属于保留的任务安全元数据，不能随 marker 物理范围一起删除。

首版切读后维护旧布局使用 parent 关系和相同 handle，只有未来通用独立影子 handle 方案才需要反向映射。现有取消窗口不扩大；每个窗口允许的 rollback 行为以 handler 和取消检查为准。旧版本元数据缺失字段时仍走完整旧事务路径，新协议不能被不理解其含义的旧节点接管；启用时增加实际版本能力门槛。

## Rollback protocol and failure matrix


### Existing boundary and rollback target


回滚目标是保留用户已经提交的 DML，并恢复旧分区布局；不是将表恢复到 DDL 开始时的快照。切读前旧布局一直是权威数据，RowDelta 和新基线都可以在停止使用后丢弃，不需先完成 RowMerge 或把 delta 回放到旧表。用户并发 INSERT/UPDATE/DELETE 不能被回滚过程撤销。

切读后的 DeleteReorganization 仍有回退能力，前提是新布局的每笔已提交 DML 都已在同一事务中正确维护旧布局及旧索引。正反 handle 映射是这项不变量的一部分。进入 Public 后旧索引降为 DeleteOnly，已有完整性不再保证，因此取消应遵循现有拒绝规则；发生错误时继续完成新布局及收尾，不将损坏或落后的旧布局重新发布。

下面按普通 REORGANIZE 的实际 job schema 状态区分。前置资源分配即使发生在 None，也可能产生需要清理的内部对象，不能机械走“无副作用 Cancelled”。

| 阶段 | SQL 权威布局 | 回滚处理 | 暂不能删除的资源 |
| --- | --- | --- | --- |
| 准备资源、尚未发布新协议 | 旧 | 撤销或登记已分配临时对象的清理；无副作用时可直接取消 | 未持久化归属的外部对象先确认 ownership |
| Capture / RowIngest | 旧 | 停止导入，关闭捕获，丢弃新基线及 delta，无需完成行 merge | 仍被 DML 使用的 delta/map、在途 ingest 目标 |
| DualWriteReady / RowMerge / Direct | 旧 | 停止导入/merge，保持旧表正确，按 schema 边界退出新布局写入 | 正向映射、delta 和新布局直到最后旧协议事务退出 |
| IndexIngest / IndexMerge | 旧 | 不需要补完新索引；停 worker 后清理新分区和替代索引 | 被 DML 维护的 temp index 和身份映射 |
| DeleteReorganization，已切读 | 新，旧仍完整维护 | 保持反向双写，恢复旧可见性并同步，再退出新布局写入 | 正反映射、新旧索引直到新布局读写协议退出 |
| Public | 新，旧不再保证完整 | 不回滚；幂等重试前向收尾 | 新数据全部保留；旧资源等安全 GC 边界 |
| Done / RollbackDone，GC 排队或进行中 | 最终布局 | 只恢复清理，不重启构建或重新选择布局 | 未完成清理描述与对应外部对象 ownership |

### Durable abort subphases


在 job 的 partition reorg metadata 中记录 abort 子阶段和原因，沿现有 JobStateRollingback 驱动。拟议子阶段是 AbortRequested、StopBackgroundWrites、RestoreOldVisibility、DisableNewWrites、PersistCleanup、RollbackComplete。它们不是已有枚举；新增编码需版本化，且与正向 Phase 分开，防止回滚把 SchemaState 退到 WriteReorganization 后误启动 RowIngest。

AbortRequested 持久化原始错误、取消请求或快照失效原因、原正向阶段、task generation 和所有资源 ownership。之后不提交任何新正向任务。首次错误与后续 cleanup 错误分别保留，避免“清理 PD 规则失败”覆盖真正导致 DDL 失败的原因。

StopBackgroundWrites 取消并枚举当前 generation 的 record、index、merge 和参数更新任务，包括历史任务记录和已提交但未登记到 job 的任务窗口。只停止后台构建，不立刻删掉 DML 需要的 RowDelta/映射。等待后台不再写入，然后才能推进元数据和清理；失败后重试本子阶段，不跳到 RollbackComplete。

切读前 RestoreOldVisibility 主要验证旧布局仍公开且完整；切读后恢复旧 Definitions、索引可见性和原分区顺序，保持必要的新旧双写及反向映射，发布 schema 并等待所有可提交事务满足回退协议。对于 LIST 不能按排序猜原分区顺序，沿用 OriginalPartitionIDsOrder 的含义。数据身份完整性无法确认时保持带原因的待恢复状态，不盲目选一个布局公开。

DisableNewWrites 分阶段发布停止新布局、temp index、RowDelta 和 map 更新的状态。不能认为恢复旧 Definitions 的同一次内存修改就结束了所有新布局读写；相邻 schema 事务需要兼容的过渡，并复用/扩展现有 schema/MDL 边界。这个阶段结束后，任何还能成功提交的用户事务只依赖保留布局，才允许清理临时身份映射。

PersistCleanup 写入完整且幂等的清理描述，再进入 RollbackDone/历史队列。描述涵盖新分区、替代 global index、temp local/global index、RowDelta、正反映射、local-sort 产物、对象存储前缀及 GC 保护服务。新分区内的 local index 可以随整个物理范围删除，global index 和专用临时物理空间需要独立范围。清理描述不可仅从已被移除的 TableInfo 推导。

RollbackComplete 表示旧布局已恢复、新协议使用者已退出、后台不会再写、清理工作已可靠登记；不要求所有异步 GC 立即物理完成。物理清理失败由持久化清理任务重试，不恢复正向构建。若清理登记本身失败，job 保留可重试状态，不能先删掉唯一的 ownership 记录。

### In-flight ingest is a separate gate


task generation 只能阻止旧 worker 提交后续请求，不能自动撤销已到 TiKV 的 ingest RPC。CancelTask 返回、executor 退出、或者 owner 丢失，都不足以单独证明存储侧写入已经结束。特别是客户端超时但服务端已接收的请求，必须按“结果未知”处理。

原型需要记录每次导入的稳定 engine/request 身份，并证明：取消后所有已发送请求已完成或不能再产生写入。若现有传输/存储协议不能证明这一点，需要补充可验证的存储侧 fence 或 drain 机制；本计划不假设已有这样的 API。单纯在发送前读一次 job generation 仍有检查后发送的竞争窗口。

结果未知时停留在 StopBackgroundWrites，禁止删除目标范围或声称 rollback 完成。正常阶段从 RowIngest 切到 Direct 前也使用同一标准，不能让晚到基线覆盖新的正常写入。对每次 RPC outcome unknown，恢复应重查/重放同一幂等身份，不能新建一个无法关联的导入任务。

### Error classification and recovery action


事务冲突、Region split、可重试网络错误按当前 phase 重试，保持 source snapshot、mapping、任务身份和已导入 checkpoint 的含义不变。元数据提交超时先读回持久化 job/TableInfo/任务状态，再选择继续或重试，避免把已经提交的切读再做一次错误转换。

owner 或 executor 故障视为接管事件，不自动改成 rollback。新 owner 先读取正向/abort 阶段；已经开始 rollback 就继续 rollback。局部 ingest 成功依靠 manifest 和原身份恢复；本地文件丢失只在源快照仍有效且重建规则已证明时重扫。对象存储文件缺失也不能简单将该组当作完成。

用户 Pause 保持当时的 DML 协议、delta/map 和 GC 保护；不是暂停用户写入或丢弃捕获。长暂停期间临时空间会继续增长。磁盘满、对象存储暂不可达、权限失效等资源错误优先进入可诊断的暂停/重试处理，具体映射到现有 job 状态；不能切回整批事务回填。若 RowDelta 写入失败，用户原事务也失败，不能提交旧行变化却遗漏增量。

唯一性错误先区分真实约束冲突与跨扫描版本误报。确认是不可恢复的编码、校验或重复错误，且仍在 rollback 窗口时，启动 abort。无法证明旧布局或身份映射完整性的内部错误，不因“可 rollback”标志就删除新布局；保留证据并停止推进，按已验证恢复程序处理。Public 后错误使用前向恢复。

GC 保护续约失败时先停止新增依赖旧快照的工作并检查保护是否仍有效；仅一次网络错误不等于快照已失效。确认 safepoint 越过 S 后，不能刷新 S 继续同一 manifest；切读前旧布局完整时转入 abort，切读后按实际仍需的恢复数据与不可回退边界处理。停止依赖并 fence 旧任务后才释放保护，避免回滚自身还需读取的元数据失效。

PD placement、label rules、TiFlash 配置、统计通知和对象清理各自可能失败。按确定的物理 ID 和持久化预期状态幂等重试，不因分区同名误删保留分区规则。恢复正确 schema/数据与异步外部收尾分开记录。沿用 `finishDDLJob` 先登记必要 delete-range 再删除 job 的原则，并审查 `rollbackLikeDropPartition` 直接标记 Cancelled 的错误分支：新协议已有资源时不能因此绕开 `JobNeedGC`，本次观察不等于确认现有路径存在泄漏。

### Failure injection acceptance


新增 `TestPartitionFastReorgRollback`、`TestPartitionFastReorgFailureRecovery` 与真实 TiKV 对应测试。每个正向阶段和每个 abort 子阶段都覆盖“副作用之前、之后但元数据未确认、元数据已提交但响应丢失”三个位置；两个 owner 连续故障、重复 cancel、pause 后 cancel、rollback 中重启也要覆盖。

必测场景包括 row/index 部分导入、RowDelta merge 事务冲突、切读后新插入行再 rollback、Public 后 cancel 被拒绝、映射创建一半事务回滚、GC 续约丢失、PD 更新失败、清理队列登记失败、对象删除失败，以及旧 executor 暂停后在 cleanup 尝试期间恢复发送。存储请求响应丢失的实验与普通 task cancel 分开验证。

每例检查保留布局的数据与索引、已提交用户 DML 不丢失、schema/partition 顺序及映射使用者退出。另查 gc_delete_range/完成记录和实际 KV/对象空间，确认所有产物有清理归属且没有保留空间被误删。JobStateRollbackDone 和 SQL 返回取消错误只是观察项，不能单独作为清理正确性的证明。

## Plan of Work


当前实施以正式设计第 15 节为准：admission 纯细分判定与不可变 parent plan；DirtyRows 标记与当前源点读 repair；逻辑 range 的事务 fence；权威/shadow 约束隔离；按窗口推进 MDL 和 GC。源代码原型前先把本地 unistore SQL 前提移植为当前包的 failpoint/真实 TiKV 用例，不把旧 binary 探针记为当前产品测试通过。以下早期 milestone 的 full-value/map 描述仅对照，不是当前首版方案。

优先执行正式设计第 14 节的无映射 split 原型：验证 parent/h 关系与 raw row 行为，新增同一旧父 record-key 的候选检查、原子写入与 hidden handle 重试，多 domain 验证反向双写/取消/退出。RowBackfillState 复用既有 model.BackfillState；旧的 RowToken milestone 仅属于通用合并扩展。通过真实原型后再接已有索引 pipeline。

### Milestone 1: protocol prototypes


本轮已完成不改产品路径的 admission/并发模型及本地旧 binary SQL prerequisite probe。后续从当前源码实现 `TestPartitionPureSplitPlan`、`TestPartitionPureSplitDirtyStartup`、`TestPartitionPureSplitDMLGuard`、`TestPartitionPureSplitRepairFence` 原型；这些名称尚未存在，不能用无匹配退出码作验收。真实 TiKV 用例检查当前源点读、RR/RC LockCtx、allocator/cache、阶段 fence 与故障。以下早期 RowDelta/HandleMap 原型是历史对照或通用合并后续工作。

首版先创建 `TestPartitionFastReorgRowDelta` 原型测试；`TestPartitionFastReorgHandleMap` 属于后续 nonclustered 扩展，验证 absent-target delete、update、跨分区移动、row ID 冲突、事务回滚、merge 两种提交顺序和重复执行。不先修改默认 DDL 路径。构建 record ingest 原型时记录真实导入的 KV kind、目标 PID 与 TS。

随后在真实 TiKV 上运行同样的竞争时序。成功条件是每种时序的新布局与旧布局最终逻辑一致，删除不复活、更新不回退、没有身份混淆，且原型基线使用 SST 导入。mock 后端通过不代表完成此 milestone。首版不建立逐行映射；后续扩展若映射使每行事务成本接近旧搬行，需记录实际成本并修正协议，不以部分索引加速替代。

### Milestone 2: state and pipeline integration


在 `onReorganizePartition` 加完整新协议分支及能力选择，修改 DML 为 Capture/Ready/Merging/Direct。注册 record-kind pipeline、对应 planner 和 checkpoint；串行加入 LocalPrepare/Build/Merge 与 GlobalPrepare/Build/Merge，为各组加准确输入集合，复用索引 pipeline。新旧路径按 job 创建时协议选择固定，已有 job 不迁移。

成功条件是包含两个 local index 和一个 global index 的 split/merge 完整运行，三个基线 kind 都有 ingest 证据；无 secondary index 表也可运行。针对多域事务验证状态同步边界，每个转换都有可重复 failpoint。

### Milestone 3: recovery and GC


补齐 phase/group task identity、manifest、GC 保护续约、远端 worker fence、RowDelta 与映射清理。增加在 record ingest、RowMerge、index ingest、index merge 和切读边界的 owner/executor 故障测试，验证旧任务无法越阶段写入。

实现并验证本计划的 abort 子阶段及异常矩阵，保持 DeleteReorganization 可回退、Public 不可回退。结果未知的 ingest 请求是 gate：没有可验证的 drain/fence 就不能结束该 milestone，也不能通过延迟 GC 或扩大测试等待时间掩盖迟到写入。

成功条件是恢复相同结果、暂停不丢 marker、取消后无后台数据写入，数据 GC 无遗漏；Closed repair fence 的有意保留单列为安全元数据，不能误清理。GC 保护失效要可预期地拒绝恢复旧快照，不能静默成功。

### Milestone 4: performance and release scope


使用相同数据比较传统路径与完整 fast 路径，覆盖受影响比例 1%、10%、100%，无 global/多个 global，多 local index，以及不同前台写入速率。分别统计行基线、映射、行增量、索引基线、索引 merge 时间和字节，记录前台延迟与临时空间峰值。

不预设加速倍数，但要解释映射/增量成本是否抵消基线收益。target 环境的 DXF、local/global sort、GC/keyspace 和 TiFlash 组合要有明确支持矩阵；未测试组合不能宣称已支持。全部必要验证完成后才决定默认开关与发布条件。

## Concrete Steps


以下命令从仓库根目录执行，是后续开发命令，本次只做文档检查。代码实施前先读相关包的 doc.go、DDL README 以及 go-coding-style。新增 Go 文件、imports、顶层测试或 Bazel 元数据触发 `make bazel_prepare`；生成文件由工具更新。新测试名是计划新增的名称，不能在尚未添加测试时把无测试匹配的退出码当作成功。

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh pkg/ddl -list '^TestPartitionFastReorg'
    ./tools/check/failpoint-go-test.sh pkg/ddl -run '^TestPartitionFastReorg' -count=1
    ./tools/check/failpoint-go-test.sh pkg/ddl/tests/partition -run '^TestReorgPartitionGlobalIndexNonTouchedAfterDropped$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/ddl/ingest -run '^TestAddGlobalIndexInIngest(WithUpdate)?$' -count=1

`-list` 应显示已新增的 RowDelta/HandleMap 等测试，执行输出没有 SKIP。新增表层/编码测试后按所属包加对应 targeted 命令，不全包扫测。任何新发现的 bug 修复增加 fail-before/pass-after 证据。

真实存储测试拟放在现有 `tests/realtikvtest/ddltest` 包，测试名 `TestPartitionFastReorgRealTiKV`。使用 `docs/agents/testing-flow.md` 的后台 playground 生命周期、独立 tag、就绪检查与 cleanup，保持数据目录和 PD endpoint 的归属明确。该包的开关定义在 `tests/realtikvtest/testkit.go`，需要显式开启真实 TiKV；在 playground 就绪后执行：

    ./tools/check/failpoint-go-test.sh tests/realtikvtest/ddltest -run '^TestPartitionFastReorgRealTiKV$' -count=1 -args -with-real-tikv=true -tikv-path 'tikv://127.0.0.1:2379?disableGC=true'

此命令的 disableGC=true 仅用于基本导入/事务场景。GC 保护实验应另用真实 GC 开启的环境/配置，验证屏障、TTL 丢失和续约；不能用这条命令证明 GC 正确。多节点/故障测试由新增 fixture 实际启动所需 TiDB/domain 和 executor，不以普通单会话 SQL 替代。

代码准备完成时按 Ready 范围执行 targeted tests 与 `make lint`，保留实际日志。文档阶段不运行这些昂贵命令，也不声称实现完成。

## Validation and Acceptance


首版必须按纯拆分验收：保留所有旧边界和整体范围；反例跨旧边界/末端变更应在启用前选 txn。数据用例含历史 EXCHANGE 造成父域间重复 handle、多父移动、删除/插入复用、初始化旧 schema 交错、分区路由离开原 q、空源点读、不同事务模式及后端 unknown result。完整值 replay 的 startup 反例作为新协议失败用例，不能删除或绕开。

完整端到端 fixture 建立 RANGE 分区表，包含普通 local indexes `idx_b(b)`、`idx_ab(a,b)` 和合法的 global unique index，插入被重组和未变更分区数据，再做纯 split；merge/跨边界/范围扩缩验证启用前选 txn，不作为首版 fast 成功用例。通过 failpoint 在 row scan、row ingest、RowMerge 和 index ingest 阶段分别插入/删除/更新。检查实际索引访问计划、全表结果、`ADMIN CHECK TABLE`、重复唯一键拒绝和 KV 中的 partition/handle。

后续 nonclustered 扩展 fixture 使用合法 EXCHANGE 形成重复 row ID，不依赖向用户表任意写内部 KV 来构造不可能的数据。检验映射、切读前后 DML、反向维护和取消后的旧表可用性。独立 clustered fixture 证明不需要逐行映射时的路径。

基线执行计数要求 record、local index、global index 均走 ingest，且旧全量事务 worker 没有承担这三类基线。delta/map/metadata 事务单独计数，既不误判为部分实现，也不隐藏其性能成本。完成条件包含真实 TiKV、故障/GC 实验和范围内兼容性，不仅是单测通过。

## Artifacts and Notes


相关现状与较广范围风险见 [可行性分析](09-partition-fast-reorg-feasibility.md)。本计划是其后续细化，当前主路线为独立行增量，Row → Local → Global 串行构建，按组延迟索引捕获；较早导入 TS 是对照路线。未来改变主路线时同时更新两份文档。

本次新增证据来自精确命令：

    sed -n '1867,1922p' pkg/table/tables/partition.go
    sed -n '1930,2085p' pkg/table/tables/partition.go
    sed -n '1782,1825p' pkg/ingestor/ingestctrl/local.go
    sed -n '414,452p' pkg/ingestor/ingestctrl/region_job.go
    sed -n '350,396p' pkg/ingestor/ingestctrl/job_worker.go
    sed -n '255,302p' pkg/ddl/ingest/backend.go
    sed -n '40,102p' pkg/ddl/ingest/engine_mgr.go
    sed -n '1,135p' pkg/meta/model/reorg.go
    sed -n '850,913p' pkg/meta/model/job.go
    sed -n '374,540p' pkg/ddl/rollingback.go
    sed -n '2195,2275p' pkg/ddl/partition.go
    sed -n '238,280p' pkg/dxf/framework/handle/handle.go
    sed -n '410,455p' pkg/ddl/job_worker.go

修订记录：2026-09-15，建立完整协议计划；将 absent-target delete、阶段 fence、非 clustered 正反映射和 GC 快照保护具体化，记录尚未验证的映射成本与存储行为。未新增运行时代码。

修订记录：2026-09-15，增加 rollback/异常矩阵、持久化 abort 子阶段、切读后的回退窗口、未知 ingest 结果处理与失败注入验收；异常恢复是完整功能的必要交付范围。

本次采用 Ready 文档验证范围：对 README 运行 `git diff --check -- docs/agents/ddl/README.md`，对新增两份文档分别运行 `git diff --no-index --check /dev/null docs/agents/ddl/09-partition-fast-reorg-feasibility.md` 和 `git diff --no-index --check /dev/null docs/agents/ddl/10-partition-fast-reorg-execplan.md`。本地引用与本计划结构的检查脚本记录在可行性文档末尾。没有运行上述实现测试或性能实验。

修订记录：2026-11-02，根据正确性优先、允许额外扫描的用户要求，确定 Row → Local → Global 串行路线，推迟各索引组的捕获启用；补充状态表、不变量、条件化证明、相邻状态及 MDL/physical-ID 屏障、索引复用边界和有限模型结果。record/local 融合暂不采用。运行时代码与真实 TiKV 尚未验证，handle 映射性能、存储侧 importer fence 和长事务屏障仍是实现前 gate。

2026-11-02 本次实际验证命令（从仓库根目录）：

    python3 docs/agents/ddl/models/partition_row_delta.py
    python3 docs/agents/ddl/models/partition_row_delta.py --unsafe
    git diff --no-index --check /dev/null docs/agents/ddl/09-partition-fast-reorg-feasibility.md
    git diff --no-index --check /dev/null docs/agents/ddl/10-partition-fast-reorg-execplan.md
    git diff --no-index --check /dev/null docs/agents/ddl/models/partition_row_delta.py

模型正常运行退出 0；--unsafe 找到反例退出 1。no-index 对非空新文件可退出 1，是否存在格式错误以诊断输出为准。另用 Python 检查两份文档的本地 Markdown 引用、行末空白、ExecPlan 必需章节、无嵌套 fence 及标题空行。采用 Ready 文档范围：只变更设计及其独立模型，不改产品代码，不运行 make lint、Go tests 或真实存储实验。未新增 agent 政策，按 agents-review-guide 检查文档边界与引用。

修订记录：2026-11-02，正式设计确定受限首版与后续扩展边界，用户允许 nonclustered 暂不支持。已核实 MDL 实际检查旧事务；发现 SwitchMDL 的并发预检存在注释注明的窗口，正式设计增加事务化 dependency。客户集群历史 schema 为 nonclustered，首版不覆盖该案例，证据与范围影响记录在正式设计。模型扩大为 513 个 histories、101125 次状态访问、6297 个终态，增加 missing-delete/late-import 两个负向变体，均找到预期反例。

修订记录：2026-11-02，正式设计 10.2 补充 nonclustered 候选协议：F/R 原子 reservation、真实 hidden handle allocator、源槽位保护、历史 identity 与当前 live-link 分离、切读后反向维护及新增验证面。发现 rowcodec.RawChecksum 包含 handle 编码，因此改 handle 不能直接复用原 record bytes。该扩展尚未模型/真实存储验证，客户场景需要它才能覆盖。

本轮补充验证（2026-11-02）：`make lint` 实际退出 0，但输出 local-notes/pr-datasets/parquet-logical-types-gen/repro 中 importer/main 混合包和 macOS find illegal option -- n，故 Go lint 文件集合可能不完整，不将其记为完整 lint 通过；dashboard linter 命令已执行。独立 Python 模型以 `--suite` 运行通过，`--unsafe`、`--mutation missing-delete`、`--mutation late-import` 均得到预期反例并退出 1。四份文档引用、空白及 ExecPlan 格式检查通过。未运行产品 Go 测试或真实 TiKV，nonclustered RowToken/SourceOwner/F/R/Live 扩展还未进入模型验证；正式设计记录该边界。

补充：nonclustered 协议从物理源槽位映射提升为 RowToken + SourceOwner/F/R + Live，避免跨分区移动时不可变历史身份与当前反向位置混淆。`python3 docs/agents/ddl/models/partition_handle_identity.py` 通过 96 个合法 schedules、768 个 transitions；这仅验证原子 identity claim 抽象，真实事务、allocator/缓存、checksum、GC 未验证。

修订记录：2026-11-03，重新审查后新增 split 无逐行映射路线及模型。正常模型 81 组 split 单射、1296 组反向历史/5184 次原子转换通过；--unguarded 找到单步反例。row/global sort 的存储合同仍保留，真实事务与故障未验证；不把模型当作源码正确性证明。

2026-11-03 新发现：checkReorgPartitionDefs 对末尾 RANGE 分区允许改变 end range，因此 map-free split admission 不能只依赖原有 SQL 校验；需独立确认旧、新覆盖范围严格相等，恢复也使用持久化原始边界。

2026-11-03 第二项简化：取消 temp row 的 applied/Capture/DualWrite 标记，统一事务重读→Put/Delete record→删除 temp。`python3 docs/agents/ddl/models/partition_row_delta.py --suite --replay-all` 通过 513 组历史、94911 次状态访问、6297 个终态；三个协议破坏变体仍找到预期反例。这个简化不改变 temp index 编码或 merge。

2026-11-03 本轮 Ready 文档/模型检查：正式设计第 14 节、可行性说明、本计划和索引入口同步；新增 partition_split_identity.py，扩展 partition_row_delta.py 的 --replay-all。正常与四个负向变体的退出码已逐项检查。make lint 退出 0 但既存 local-notes/find 诊断导致范围不完整；不宣称完整 lint 或真实存储验证通过。

2026-11-03 澄清多分区拆分：正式设计第 14.8 节把 len(DroppingDefinitions)==1 的过严限制改为每个新分区唯一旧父、保留全部旧边界及外范围。允许一个作业分别细分多个相邻旧分区，不允许 nonclustered 跨旧边界的来源混合。计划持久化 parent map，统一行完成/MDL 屏障，global index 只重建一轮；跨父域 UPDATE 与并发重试列为新增未验证项。

2026-11-03 本轮新增模型/探针：partition_split_plan.py、partition_split_concurrency.py、partition_split_sql_probe.py；partition_row_delta.py 增加 --reconcile/--startup。精确命令与结果边界见正式设计 15.6。所有本地服务均使用独立目录/socket，finally 停止 own process；报告含二进制版本和 owned_process_stopped。没有修改产品 Go/Bazel 文件。

2026-11-03 当前实验报告：`.codex-tmp/ps-sxmhpbt5/result.json`，对应旧 dirty binary hash 10d7421b80；不是工作区 44facb0。状态 PASS 且 own process 已停止。probe 完整输出含父/marker/fence 的 9007、RR 空键 1205、EXCHANGE 后重复 handles 1/2、同 handle 多父拆分、MDL delete-only 阻塞和 TO_DAYS/NULL/global-index 读取。当前源码与真实 TiKV/DXF 尚待后续实现验证。

本轮 prototype 的关键边界：SourcePointRead 必须是 repair 当前事务视图；用户 txn.StartTS 可能早于基线导入，不可假定所有 startTS>B 或按 nonce 过滤 S 前 marker；悲观 guard 必须锁不存在键并使用当前锁返回值，不能锁后读取旧 snapshot；事务层冲突不能在表层擅自换 snapshot。

新增 DeleteOnly 初始化负向：`partition_row_delta.py --startup delete --replay-all` 错删 S 中仍存在的行；`--startup delete --reconcile` 正确点读并保留当前源。当前协议已覆盖两个初始化反例，历史完整值 replay 决策保留为对照，不能再按其指导实施。

本轮最终 Ready 文档/原型检查：15 个模型调用（含预期退出 1 的反例）、Python 语法、文档链接/格式检查通过；make lint 返回 0 但既有 local-notes/find 诊断导致范围不完整。源产品路径未变；所有自建 unistore server 已停止，最新报告 ps-sxmhpbt5 保留。第一项产品实施 gate 仍为当前源码的 DirtyRows hook、pessimistic RC/allocator、真实 SST/DXF 完成及故障恢复验证。

正确性复核见正式设计第 16 节。`python3 docs/agents/ddl/models/partition_split_sql_probe.py --late-snapshot` 在旧 dirty binary 上退出 1 并记录 SILENT_MISMATCH，所有自建进程已停止。当前源码尚未确认；新路径需要显式读版本合同，不允许把旧 RR 配新 PID 的静默缺行当合法结果。
