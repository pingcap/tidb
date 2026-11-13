# 历史审查记录：分区 global sort 设计

本文保留被否定的候选与实验过程，不作为实施规格。当前规范见 [最终设计](2026-11-02-partition-global-sort.md)。

# REORGANIZE PARTITION 通过 DXF/global sort 构建新布局

日期：2026-11-02；2026-11-03 简化审查修订。状态：设计与独立原型验证阶段，产品路径尚未实现，尚无真实 TiKV/DXF 验收。当前源码基线为 `44facb0f374a16d39d5a7267714a9c5d0d7f17a4`，工作区有其他修改，本次不改动产品代码。正确性状态见第 16 节：尚未闭环，已发现旧 RR 首次访问的读取版本缺口；不得把第 15 节作为已证明正确的完整实施规格。当前协议草案入口为第 15 节：用户认可的纯拆分范围、静态规划、脏键源端修复和退出合同；第 14 节保留推导历史，其中完整值重放已被初始化反例否定，不再作为首版协议。本文确定协议方向；此前的[可行性分析](../agents/ddl/09-partition-fast-reorg-feasibility.md)和[ExecPlan](../agents/ddl/10-partition-fast-reorg-execplan.md)提供取证与后续实施步骤，若范围冲突，以本文为准。

## 1. 结论与目标

当前为待完成正确性审查的草案，第 16 节有阻断性读版本问题。行数据主路线采用独立脏键集合 DirtyRows 加串行构建：**行 record 基线 ingest → 点读当前旧源行修复脏键 → local index ingest/merge → global index ingest/merge → 切读**。三类基线都走 DXF/global sort/SST ingest；元数据、前台用户事务和增量 merge 继续使用事务。第一版不做 record/local 融合，也不重新发明临时索引格式或唯一性算法。nonclustered 优先支持纯拆分（每个新分区只有一个旧父分区，可同时拆多个相邻旧分区），保留 handle、使用旧父分区 record 检查反向写入，不建立 RowToken/SourceOwner/F/R/Live；通用合并及复杂重划分继续旧 txn 路径。

正确性优先，允许额外扫描。用户观察到几百 GB 的行重组耗时数小时，同量级 ADD INDEX 可在一小时内完成；这说明值得加速搬行，但不是本次控制变量性能实验，不承诺倍数。

第一版先覆盖下述明确子集，其他输入在协议启用前继续走完整旧 txn 路径，并记录选择原因。启用之后不允许半途静默切回事务搬行。省略某类索引组是因为表没有该类索引，不是保留该组全量事务回填。

## 2. 第一版能力边界

| 项目 | 第一版支持 | 暂不支持及原因 |
| --- | --- | --- |
| SQL | 普通 RANGE REORGANIZE 的纯拆分，可多个相邻旧分区分别细分 | 首版统一不做合并、跨旧边界重划、范围扩缩、REMOVE PARTITIONING、ALTER PARTITION BY、multi-schema |
| 行身份 | 整数 clustered PK；另支持纯拆分的 implicit row ID，每个新分区唯一旧父，handle 在两套布局中一致 | nonclustered 通用合并/边界重划、common handle 暂不纳入首版；不是所有 nonclustered 都需要映射 |
| 分区 | clustered 保留原 RANGE(pk) 范围；nonclustered 为同一 RANGE 表达式下一个或多个相邻旧分区分别细分，保留全部旧边界，含 TO_DAYS(datetime)，整体范围不变 | nonclustered 多旧分区合并或跨旧边界重划不启用；LIST/HASH 暂不纳入 |
| 索引 | 现有 fast add-index 支持的普通列 BTREE local/global；global unique 与 nonunique 均保留 | 表达式、前缀、multi-valued、vector/columnar 等不纳入首版 |
| 表属性 | 普通持久化 TiKV 表，无生成列、外键、TTL、TiFlash/table cache | 这些组合需独立证明副作用与复制路径；不是宣称其原理上不可支持 |
| 事务屏障 | MDL 全程开启，所有 TiDB 理解新协议 | MDL off、旧节点接管、混合版本不启用 |
| 执行环境 | 同一 keyspace 的 classic 部署，DXF + 对象存储 global sort 可用 | local sort、cross-keyspace/nextgen 不纳入首版 |
| 运维并发 | 允许普通并发 DML、pause/resume、cancel、owner/executor 故障 | 不允许该表并发 EXCHANGE/其他改布局 DDL；nonclustered 首轮不支持用户显式写 row ID；禁止不理解协议的降级 |

这些限制是首版能力表，不更改 REORGANIZE 的 SQL 结果语义。限制判断使用 job 创建/首次执行时的持久元数据与集群能力，恢复不依据当时全局开关重新选择。当前表满足上述限制，也不自动证明历史 DDL 当时满足，历史案例需另取证。

CDC/备份/PITR 兼容是发布前条件，不能仅以 SQL 正确通过。首版不宣称支持订阅本表或覆盖本表的并发增量备份/恢复任务；能力预检和产品层冲突互斥要有可执行入口，若部署无法可靠识别/协调这类任务，则该组合不开放新路径。不能只在文档中要求用户“保证没有”。

## 3. 为什么 clustered 整数主键使身份问题闭合

定义旧物理行身份为 `(oldPID, h)`，`h` 是整数 clustered PK 的原始 handle 编码。新 identity 为 `(newRoute(h), h)`。分区路由是完整 PK 的确定函数；合法旧表中同一个 h 不可能属于两个不同逻辑行。旧新布局保留相同 h，不需要 handle map、目标预留或反向 map；切读后的反向双写只需旧分区路由函数。

更新 PK 被视为同一事务中的 Delete(old h) + Put(new h)，跨受影响/未变更范围移动同样显式处理两端。普通非 PK 更新保留 h。索引 entry 用目标 PID 和同一 h。unsigned 不能先转有符号整数再比较路由，沿原 tablecodec/Datum 语义。

考虑历史 EXCHANGE WITHOUT VALIDATION 或其他不满足路由的数据：扫描时检查每行在旧分区的合法归属和新目标路由。record 排序发现同一目标 key 来自不同源 identity 时，即使 value 相同也报构建错误，不允许去重或 last-write-wins。失败保留源布局并回滚新布局。这是数据异常检测，不将不合法源布局“修好”作为本功能目标。

此前为 nonclustered 通用合并提出的稳定映射方案可能产生 O(N) 事务元数据；它不是支持所有 nonclustered 场景的必要条件。单父分区拆分按第 14 节保留 handle，只有 O(分区数) 的静态父分区关系，不需要逐行身份层，也不以 raw value 相同判断两行相同。common handle 可沿相同理论扩展，但需复合键/排序规则/编码专项验收，不与首版混交。

## 4. 数据空间与原子操作

旧行与旧索引在切读前始终是 SQL 权威数据。新行位于 AddingDefinitions 的新 physical IDs，尚不可读。新 global index 使用新的 index IDs；新 local index 可复用原 ID，由新 PID 隔离。未变更分区 local index 不重建。

为 job/generation 分配一个内部 physical ID 存 DirtyRows，key 为 `(tempPhysicalID, targetPID, encodedHandle)`。当前方案只保存非空 marker，建议带协议版本与用户 txn.StartTS 作为写入 nonce，不保存行值、Put/Delete 或 applied 标记；nonce 不作 commit 顺序或快照过滤依据。每次相关行变更都在同一事务写旧/新受影响目标键的 marker，删除也必须写 marker，不能删除 temp key。第 15 节说明为何不再重放历史行值。

DML 内部先按权威布局执行 SQL 约束与行/索引变更，再执行影子布局捕获/写入，所有操作使用同一事务、同一语句 staging 范围。失败时语句与原用户事务按现有语义回滚，不能留下仅旧数据提交的窗口。REPLACE、INSERT ON DUPLICATE KEY UPDATE、LOAD DATA 等复用相同底层 mutation hook，不能只在某个 SQL executor 捕获。

下表描述切读前；切读后权威布局及退出规则以第 15.5 节为准。

| RowBackfillState（行为别名） | 对旧权威布局 | 目标 record | DirtyRows | 目标新索引 |
| --- | --- | --- | --- | --- |
| Capture/Running | 正常维护 | 不写 | 标记旧/新受影响目标键 | 均关闭 |
| Ready/Merging | 正常维护 | raw Put/Delete | 同格式脏键 marker，与两套行同事务 | 均关闭 |
| Direct | 正常维护 | raw Put/Delete | 不再生成 | 按各索引组状态维护 |

raw Put/Delete 与当前源点读修复是拟新增内部写入入口，含义是写 record KV 而不调用普通 AddRecord 的不完整索引检查。Delete 即使目标 key 不存在也记录事务删除，不采用当前 nonclustered RemoveRecord 的跳过逻辑。源表依然执行正常约束；对影子布局不能套用“用户插入必须不存在”的 assertion。row codec、checksum、默认值、资源组和内部来源标签沿现有机制。

索引阶段仍通过显式索引 mutation 编码旧值和新值。当前普通索引 update 可能依据 touched 列跳过不变键；分区/PK 改变即使索引列未变，也要更新 PID/handle，复用当前分区 DML 的 remove/add 语义。不要让 raw row helper 偶然绕过索引事件。

## 5. 状态机、发布和 MDL 屏障

分开持久化 job.Phase（后台工作）、TableInfo 中的 RowState（前台行写入）、每组目标索引的 State/BackfillState。现有 Partition.DDLState 只作为外层可见性协议。当前 newPartitionedTable 会根据 pi.DDLState 克隆 local index 状态，需要增加目标布局状态投影，不能只修改 owner 内存对象。

| 阶段 | 前台行为 | 后台动作 | 推进条件 |
| --- | --- | --- | --- |
| Prepare | 旧布局 | 持久化协议、IDs、清理归属、MDL 依赖 | 元数据与能力检查完成 |
| CaptureDeleteOnly | 删除方向捕获，插入尚未捕获 | 沿既有 DeleteOnly 兼容过渡 | 全集群及旧事务屏障完成 |
| CaptureWriteOnly | 完整行增量捕获 | 沿既有 WriteOnly 过渡 | 未捕获事务全部退出 |
| RowBuild / Capture | 完整捕获 | 取得固定 S，扫描旧分区，sort/ingest record | 全部 import 组成功且完成导入屏障 |
| RowReady | 目标 raw 写 + 完整 temp row | 发布 Ready 并等待 MDL | Capture-only 事务全部退出 |
| RowMerge | 同 RowReady | 事务合并 Capture delta | 全范围已处理且提交已确认 |
| RowDirect | 双写两套完整行 | 发布 Direct | 兼容过渡完成 |
| LocalPrepare/Build/Merge | 两套行，local 按既有 temp 协议 | 仅扫新分区，sort/ingest/merge local | local 完整且持续维护 |
| GlobalPrepare/Build/Merge | 两套行和新 local，global 按 temp 协议 | 扫最终全表布局，sort/ingest/merge global | global 完整且持续维护 |
| Switch | 新布局权威，仍维护旧布局 | 沿 DeleteReorganization 切读 | 切读 schema/MDL 屏障完成 |
| Retire/Finish | 新布局正常服务 | 沿 Public 退出旧写入并登记 GC | 无旧协议使用者，清理已持久登记 |

CaptureDeleteOnly 期间如何处理 update 的删除方向沿既有分区 DDL 的兼容要求，不能记录 Put 而遗漏 Delete；完整快照 S 只在 CaptureWriteOnly 的 MDL 屏障之后取得，因此该早期阶段不承担完整捕获证明。

每次改变前台写入行为都更新 TableInfo 与 schema version，走 registerMDLInfo/updateGlobalVersionAndWaitSynced。job 的 table_ids 至少包括逻辑表 ID；它覆盖同一逻辑表全部分区的 DML，包括仅访问未变更分区的事务。若使用物理 ID 的校验路径也参与，schema diff 仍保留完整相关 physical IDs。

代码依据：planner/core/preprocess.go 把 `tableInfo.ID` / `tbl.Meta().ID` 及所用版本注册到 MDL related table map；SessionVars.RemoveLockDDLJobs 移除尚有旧版本事务的 job；issyncer.MDLCheckLoop 仅对未被移除的 job 上报版本；schemaver.waitVersionSyncedWithMDL 等待相关服务器确认。因此此机制可作为事务屏障，不需要新建每行/每事务全局锁。

MDL 是持久协议前提，不是一次预检。新增一个低频的 durable MDL-dependency 注册：新协议 admission 与 MDL off 操作在同一个元数据 policy key 上事务串行化，admission 只有 enabled=true 才增加依赖，off 只有依赖数为 0 才允许提交。计数/成员有 job ID，接管能重建且只能幂等释放，存在资源或回滚未完成时不释放。SwitchMDL 的本地 vardef 修改必须在受保护的持久化事务成功后生效，失败不能先在本机关闭。这个 helper 是拟新增设计，不是现有 SwitchMDL 已保证的属性。

集群 capability 检查确保所有节点理解此依赖；新节点加入也先加载 MDL 模式与能力。隔离节点失去 schema lease 后不能继续成功提交，沿 TiDB 现有集群有效性前提，不把任意网络超时当成它已经退出。首版不另证明 MDL off 的 lease/amend 组合。

## 6. 行基线、时间戳与 importer 边界

在捕获屏障后取固定源快照 S，保存到 job；扫描范围覆盖每个 DroppingDefinition 的完整 record prefix，不能仅以某时刻“有数据”的 min/max 推断所有后续数据。S 后的新插入不需要基线读到，因为必有 delta。建立 GC service/keyspace 保护并确认 S 尚未失效，owner 切换和暂停时持续续约。

扫描直接读取 S 的旧 record，保留 h，按新规则输出新 record key。只输出行，不生成任何新索引。固定 S、schema 编码配置和分区映射使每个 range 输出不可变；外部文件保存行数、字节、checksum、目标 key 范围和来源 identity 的校验信息。零行 range 也有完成记录。不能把部分成功的任务换一个快照或重新分配一组不相关的 TS。

每个 ingest group 在导入前固定 importTS 和 manifest。扫描 TS 与 importTS 分开。失败重试使用同一 KV 内容与同一 importTS；禁止恢复时替换成当前 TS。所有 record 组的 importTS 小于发布 RowReady 前的持久化 TSO 边界 B，新的 repair 事务 startTS 大于 B，后续目标 record 的用户提交 commitTS 大于 B；不假定用户 startTS 都大于 B。长事务可能早已开始、到新 schema 才首次访问本表，其快照不能被表层偷偷替换，新增 shadow 写冲突沿原有策略处理。发布 Ready 后不允许新增 import group 或输出版本。

迟到导入有两个层面的保护：固定旧 importTS 重放不会覆盖更高 MVCC 版本，是额外防线；主屏障仍要求导入副作用完成，防止 GC 后复活或范围清理之后再写入。不能用“版本较老”取消 drain，也不能把客户端 cancel 当成存储侧完成。

第一版 importer adapter 的确定合同：正常完成记录只在全部 range 有确认成功的 ImportEngine 结果、没有可再次分派的合法 task、全部已知执行尝试终止后写入；每次重派登记稳定 attempt 身份。超时/失联/结果未知进入 Recovering，禁止 RowReady、释放 GC 保护或 GC 目标范围。只允许重查/重放相同 SST/importTS 并收集完成证据；不能靠等待固定分钟数或忽略故障进入下一阶段。

当前 ingestctrl.ImportEngine 的 worker/job wait group 和 cloudImportExecutor 在 ImportEngine 返回后才完成 subtask 提供正常路径基础；DXF canceled/succeeded 状态本身不是跨失联 attempt 的存储 fence。实现需扩展 attempt/manifest 台账，并验证 TiKV 已接收请求与重试的确认语义。若现有 TiKV ingest 幂等/完成语义不足以确认所有可能写入已终止，则需要存储侧可验证的 fence/drain 扩展；没有该合同就不允许首版上线。这里没有虚构现成的 TiKV API。

因此故障期间允许阻塞或保留待恢复 job，保证安全；不承诺任何失联都能在有界时间内自动完成。实现阶段要把正常重试和 owner/executor 接管做成可自动完成；只有真实结果不明时才 fail closed。S 与 importTS 的 GC 保留成本需要监控，不能无限静默增长。

## 7. 当前脏键修复的逐 key 证明

对于目标 K=(q,h)，唯一旧父为 P=parent(q)。定义目标正确值 D(K)：当前旧源 `(P,h)` 不存在，或其当前行按新规则不属于 q 时，D(K) 为不存在；否则是该行在 q 下的编码。这个点读来自 repair 当前事务的 startTS，不能使用基线快照 S 或旧扫描缓存。

每次能改变 D(K) 的用户提交都在同一事务写 DirtyRows[K]。Running 期间仅标记；Ready/Merging 期间还同事务维护目标 record。repair 事务读取 marker 和源行、算出 D(K)、Put/Delete 目标、删除 marker，再提交；它还写该逻辑范围的事务 fence，防止阶段退出后旧 worker 再次修复。

若并发用户事务改变了 D(K)，它也写同一 marker，与 repair 删除发生写冲突，两个事务按提交顺序收敛；若没有改变 D(K)，repair 读到的旧源内容即使发生无关变化，对 K 的结果仍正确。作用于同一源 handle 但不同目标 q 的移动，在 old/new image 对应的目标键都标记；源槽位复用按当前行状态修复，不追踪历史逻辑行实例。

初始化的旧 marker 只产生多余源点读，不携带可能早于 S 的行值，因此不会把较新的基线覆盖成旧值。S 仍须在未完整标记的事务退出之后选取，merge 仍须在 Running-only 事务退出之后扫描；脏键修复不取消这两个屏障。cursor 之后出现的 marker 来自已双写协议，目标已同步，不要求 temp 物理清空。

证明依赖同事务源/marker 写入、同一 KV 快照读取、真实写冲突、唯一父关系、固定路由语义、导入完成和阶段 fence。模型和 SQL prerequisite probe 分别提供有限范围证据，不能据此声称新 DXF 实现已验证。完整前提、初始化反例与收尾条件见第 15 节。

## 8. 索引复用的精确合同

每组索引开始前，其扫描表已经是完整、持续更新的行表；组自身的 DeleteOnly/WriteOnly/Running 捕获屏障已经完成。索引状态按目标布局投影：旧 local、未变更 local 与旧 global 始终正常维护；仅新 local 或替代 global 使用 backfill 状态。复制 IndexInfo 时要复制对应 BackfillState，不能同 ID 全局修改。

Local 组输入只含 AddingDefinitions，每个任务索引集合固定为相关 local index IDs。Global 组输入为 `AddingDefinitions ∪ (Definitions − DroppingDefinitions)`，每 PID 恰好一次，每任务索引集合固定为替代 global index IDs。保持一个 global 构建域：不拆成“新分区一轮、未变更分区一轮”独立完成，否则可能破坏 duplicate detection 与完成身份。

复用 readIndexStepExecutor/buildExternalStorePipeline、mergeSortExecutor、cloudImportExecutor 及 MergeTempIndexOperator 的编码与执行组件。需要扩展的是 planner 的显式 PID 集合、表状态投影、任务 kind/phase/group 身份、分区错误处理和完成清理。generateReadIndexPlan 当前读旧 Definitions；远端 executor 又重新读元数据，因此 owner 的局部 clone 无法完成适配。record 组有独立编码器/engine identity，不伪装成 index ID。

保留成熟的 unique/nonunique/NULL/temp key 版本与 delete handle 校验算法。普通 DML 的索引变化从当前权威行 old/new image 编码，但目标 key 使用新 PID 和不变 h；行布局完整后才进入索引协议，避免 merge 的行存在性检查面对未搬完的行。

索引扫描不强行改成行阶段的 S；沿现有 ADD INDEX 的扫描/merge 版本协议及 GC 机制。全表 global scan 的跨时间唯一键复用、跨分区移动可能形成暂时重复输入，需重跑现有 global-sort 回归并增加影子布局场景；不以旧 unique index 正确就推导新输入绝无重复。若输入适配不满足成熟协议前提，修正适配或限制首版组合，不删 duplicate-check 掩盖误报。

local 完成后持续维护，再启动 global；无对应索引时跳过该组。所有新索引仍不对 SQL 可见，直到统一切读。影子布局 local index 即使构建完成，也不能提前改变查询优化器的可见索引集合。

## 9. 切读、恢复、回滚及清理

Row/Local/Global 三个持久化完成标记都存在且持续维护时，沿现有 partition Definitions 替换与 DeleteReorganization 切读。新 primary handle 与旧相同，按旧/新分区规则反向维护即可。维持旧索引完整性直到现有 Public 不可回退边界；不扩大取消窗口。

阶段单调，先持久化 intent，再提交确定性 task，成功后先确认 task/manifest 再更新阶段。task key 包含 job ID、protocol version、generation、kind、phase、group；恢复必须查询同一历史成功任务，不新建另一轮。任务 output 可重复读，副作用只允许相同内容/版本的幂等重放。row/index/merge 的身份区分也应用于 pause、cancel、统计和清理。

| 故障窗口 | 保留不变量 | 恢复或取消动作 |
| --- | --- | --- |
| 任务提交成功、job 未记住响应 | 源权威，task 身份确定 | 按 key 找回同一任务 |
| SST 部分导入或结果未知 | 保持 Capture 与 GC 保护 | 固定 manifest/TS 恢复，未确认不切 Ready |
| Capture/Ready 转换后 owner 故障 | TableInfo 已发布行为是真实协议 | 重做 MDL 屏障，再决定 merge，不能仅看内存 |
| merge 已提交、cursor 未保存 | 目标和 delta 原子一致 | 从旧 cursor 重扫 |
| row 已完成、local/global 未完成 | 目标行持续完整，旧布局可读 | 同组索引恢复；取消不需要把新索引补完 |
| 切读前取消 | 源布局完整 | 停后台，发布退出新写入，等 MDL，登记新布局与 temp GC |
| DeleteReorganization 取消 | 两套布局仍完整 | 恢复旧可见性并同步，再退出新布局 |
| Public 后错误 | 新布局权威，旧索引已不保证完整 | 拒绝回退，幂等前向收尾 |
| cleanup 登记/执行失败 | ownership 仍持久存在 | 重试清理，不重启已结束构建 |

暂停只停后台，前台协议不变，Capture 期间 delta 继续增长。暂停期间仍需 GC 续约，暴露容量与续约状态。资源错误按当前阶段恢复；确认 S 已失效且还需重扫时，切读前 abort，不能换 S 混入旧 manifest。用户 DML 已提交的数据始终保留，rollback 不是回到 DDL 开始快照。

取消前先结束所有可能写入的 importer/merge，再等待前台协议退出；结果未知时停在 Recovering/AbortDrain，不能先报 RollbackDone。成功清理旧分区、旧 global、临时空间和对象产物；取消清理新分区、新 global 和临时产物。新 PID 范围内 local index 随物理分区清理。清理描述在移除元数据前持久化，不能只从已删除的 TableInfo 重建。

## 10. 所比较方法与正确性结论

| 方法 | 正确性分析 | 结论 |
| --- | --- | --- |
| 只开启 DXF，保留 txn worker | 现有事务语义可保留，主要改变分发；无法消除全量 record 事务写入成本 | 可作对照，不是本次 ingest 方案 |
| 直接扫描旧分区并 ingest，沿用双写 | 导入可能覆盖新值；absent-target delete 不留标记会复活行 | 拒绝 |
| 仅给基线较早 TS | 有版本顺序潜力，但缺失 Delete、GC、旧版本重放、源身份问题仍未解决 | 不选；不是只改 TS 可正确 |
| record/local 同次生成 + temp row/temp local | 原理可行；固定 S/identity 下生成一致 KV，所有组共同 fence，先 row merge 再 local temp merge | 后续优化；首版增加输出组/恢复证明面，不选 |
| record/local 同次生成，直接普通 local 双写 | 基线索引可能仍占用已删除/改键的 unique key，新 DML 会误报或留下旧 entry | 拒绝；必须有 temp local 或等价协议 |
| RowDelta 后行/索引串行构建 | 行新协议先单独收敛；索引得到完整行源，可复用成熟协议 | 首版主路线 |
| global 原地改旧 entry 的 partition ID | 同一 unique key 当前 value 无法同时供旧/新 schema 定位，改变读写兼容合同 | 不选，保留新 index ID |
| 全程阻塞前台写入再 ingest | 在真正的全程写锁、排空事务与 worker 条件下可简化增量，但改变在线 DDL 使用体验 | 正确但不符合本次在线目标，不作为隐式 fallback |
| nonclustered 稳定映射 | 适用于希望独立分配影子 handle 的一般方案，但不是唯一解；元数据成本和反向维护复杂 | 不作首版；单父分区拆分优先采用第 14 节保留 handle 方案 |

没有把所有可能算法列举为已验证；表中覆盖本次讨论的主要替代方案。不同算法可以在各自前提下正确，选择依据是在线语义、证明面、成熟组件复用和性能潜力。

### 10.1 Nonclustered 的限制不能只写“没发生过 EXCHANGE”

保留隐式 handle 的快路径只有在整个作业期间都能保证 `(sourcePID,h) → (targetPID,h)` 单射时才正确。只在快照 S 检查一次没有重复不够：未变更分区到新分区的移动、历史 EXCHANGE 后的 allocator/cached range、显式 row ID 操作，都可能在检查之后引入身份冲突。SHOW CREATE 没有 EXCHANGE 记录不等于表从未 EXCHANGE，保留不完整的 DDL history 也不能作证明。

SHARD_ROW_ID_BITS 的 shard 不是分区 ID；不同分区的 row ID 不能据此视为不相交。AUTO_INCREMENT 列也不一定是 handle：客户表 UNIQUE(id, create_time) 允许不同 create_time 下相同 id，不能偷偷把 id 当 clustered handle。否则既改变行身份，也没有证明全局唯一。

可研究的受限 nonclustered 扩展是：对全部相关历史 handle 与运行期分配建立可验证的互斥/ownership，或在捕获期间分来源保存 delta 并持久标记碰撞，碰撞时退出 DDL。它还需要覆盖最终切读后旧 schema 的双写窗口，不能只在切读前再扫一次。若依赖逐行 ownership，成本已接近稳定映射；但第 14 节单父分区拆分可由来源拓扑证明，不需要 ownership 或无 EXCHANGE 历史假设。首版不采用未经证明的 no-EXCHANGE 快路径。

对确需独立影子 handle 的一般稳定映射方案，创建映射、目标预留及其反向关系必须同事务；扫描与 DML 原子获取同一映射，基线恢复不能换 handle；每次新插入和跨分区移动都使用同一分配规则，切读后新行还要映射回旧布局。它有可行的正确性结构，但需要独立模型、故障验证和 O(N) 元数据成本测量。限制首版不会让这项工作自动消失。

### 10.2 支持 nonclustered 的候选完整协议

本节是此前通用映射候选，2026-11-03 审查后不作为首版推荐；保留供后续需要一般合并时比较。不能从 clustered 模型通过推导它已经正确。它需要行身份原型单独通过，再复用本文行状态机和索引流水线。适用于客户案例的范围还需加入 RANGE(TO_DAYS(datetime)) 的确定性路由、原部署能力与复制兼容，不只打开 HasClusteredIndex 判断。

仅用 source PID/handle 作永久身份不足以覆盖跨分区 update，因为源槽位会移动。拟新增内部 RowToken 表示作业期间的一条逻辑行，目标分区 q 由完整行按新表达式确定。已有行的 token 由首次 source-slot claim 建立，编码包含 generation、旧布局标签及初始 `(oldPID,oldHandle)`；切读后新行用新布局标签及首次 target identity 建立 token，两个空间不混淆。

持久化源归属 `SourceOwner(oldPID,oldHandle)=token`，正向映射 `F(token,q)=newHandle`，目标归属 `R(q,newHandle)=token`，及当前 `Live(token)={oldPID,oldHandle,newPID,newHandle,present}`。SourceOwner、F、R 是不可变 reservation；只有 Live 随事务更新。反向查找是 target identity → R 的 token → Live 的当前 old identity，不能把 R 直接固化为第一次 old PID/handle。这样源跨分区移动也不改 token，不覆盖历史恢复身份。所有空间按 job/generation 隔离。

GetOrCreateTarget 的原子合同是：先解析/建立 source slot 的 RowToken，再在事务读取 F(token,q)；已存在就返回同一个 handle。不存在时，先尝试原 handle 对应的 R；若未保留，原子写 F 与 R。R 已属于其他 token 时，从真实 logical-table hidden RowID allocator 分配候选，按照 SHARD_ROW_ID_BITS/MaxShardRowIDBits 处理并检查溢出，再检查 R，直到能原子保留。不能依据 record 是否已导入、raw row 是否相同判断归属。两个执行者同时创建同一个 F 或抢同一个 R 时，提交写冲突保证只有一个结果，失败方重读；allocator 空洞可接受，F/R 半提交不可接受。

基线 scanner 的映射事务不写源 record、不改变源行/旧索引；它只建立不可变 reservation，不能把快照 S 的行位置覆盖到已经由 DML 更新的 Live。Live 初始化也采用事务 create-if-absent，已有记录以用户事务的更新为准；S 的行值只进入基线 record。可批量为一批源行创建映射，避免每行一个往返。扫描、用户 DML 与恢复全都调用同一 helper。若 scanner 读到 S 中旧槽位、用户已把行移动到新槽位，旧 SourceOwner 仍返回同一 token；scanner 不生成第二个 token。DML 的 F/R 与源行、RowDelta 在同一用户事务中创建，失败全部回滚；allocator 发号不回收。scanner 已完成的映射可以早于 record ingest，删除行的映射也不释放。因源删除而查不到当前行时，scanner 仍按 S 中 source identity 建立同一映射，不能重分配。

捕获 Delete 即使目标 record 不存在，也先解析/创建 F 并写显式 Delete。更新 old/new image 分别计算旧/新 q；路由或源 handle 改变时，同事务对旧目标写 Delete，对新目标写 Put。源跨分区 update 在现有代码中可能生成新 hidden handle，新 SourceOwner 原子绑定同一 token，Live 指向新 old identity；新目标 F 仍按 token/q 查询。删除把 Live 标记 absent，保留全部 reservation。原 row slot 不能被另一 token 重用：普通 hidden handle 分配也要走 claim adapter，遇到历史 reservation 就重新分配；显式 rowid 写入首轮排除。这样 scanner 的 S 身份不会被后来的新行继承。

例子：O1:h=7 与 O2:h=7 都进入 N。假设 O1 先保留 `(N,7)`，O2 就保留 `(N,allocatedHandle)`。O2 的 scanner、delete、update、index encoder 和重试都经同一 RowToken/F 得到同一个 allocatedHandle。行内容相同仍保留两行。这里谁保留 7 可以随首次成功 claim 不同而不同，但一旦持久化就不再改变；不要求预先决定全表排序中的赢家。

不应默认“仅冲突行有映射、其他恒等”。在 importer 尚未生成数据时，无法从目标行存在性区分“未扫描”与“没有碰撞”；新 DML 也需要稳定确认归属。因此正确性基线是对所有受影响的源槽位保留 F/R，即使映射为恒等。避免 O(N) 映射是后续优化，需要单独证明区间编码、可重建性或完整 allocator 历史，不是删除 map 写入即可。

切读前目标 DML 的 local/global entry 使用 F 返回的 target handle。目标 handle 改变时不能只替换 record key 再搬原始 value：当前 rowcodec.RawChecksum 明确把 handle.Encoded() 纳入 checksum。必须用正确 target handle 重编码/重算 row checksum，保留列、默认值与时区语义；反向旧副本按旧 handle 编码。AUTO_INCREMENT 用户列不变，hidden handle allocation 不能改变 LAST_INSERT_ID 的用户列语义。

切读后，现有行从 R 找 token，再从 Live 解析当前旧 PID/handle，按当前行内容计算旧布局路由。更新分区列时旧 source identity 也可能改变：同事务保留 token、绑定新源槽位、删除旧布局行、写新布局行，并更新 Live。目标 q 改变也按 F(token,q) 找稳定目标 handle。历史 reservation 永不重新分配；Live 用版本化完整值推进，删除保存 absent，不使用覆盖历史 R 的方式改变归属。所有涉及同一 token 的变更都写 Live，作为额外事务冲突点。

切读后新插入行先在新权威布局分配并保留 target identity 及新 token，再选择可用旧 source identity，原子建立 SourceOwner/F/R/Live 并写两套行/索引。旧 source 候选必须同时检查 record、历史源 reservation 和并发旧 schema 分配者。所有仍可能分配 hidden handle 的前台路径使用同一 allocation/claim adapter，采用固定 key 锁顺序或冲突重试，不让旧、新 schema 分别用未经检查的缓存候选写同一个槽位。

为减少兼容面，扩展首轮也排除用户显式写 `_tidb_rowid` 的语句；不是禁止普通 INSERT/UPDATE。内部保留 handle 的 update 路径仍受 map/claim 协议约束。更完整的显式 rowid 支持需要定义删除后复用 slot 的 incarnation/live-link 规则，不能仅因为物理 key 不存在就复用一个仍被旧 schema 或 snapshot 引用的 reservation。

取消语义增加两个必要条件：切读前旧行一直完整，停止新布局协议后可丢弃 map；切读后的可回退窗口中，每笔新布局 DML 已通过 live link 原子维护旧行和旧索引，才允许恢复旧可见性。map 不随 row merge 或切读完成立即删除；直到旧 schema、旧布局维护、importer 和 rollback 使用者全部退出才能 GC。所有 scanner、DML、merge、index、rollback 从同一份持久 map/live-link 解析身份。

该候选协议的条件化证明分为三个引理：原子 claim 保证不同 token 不会拥有同一个源/目标 reservation；持久 SourceOwner 与 F 保证同一逻辑行和 q 在基线/DML/重试中不换 handle；事务化 Live 保证位置移动后 token 不变，且切读后的当前行能正确反向维护而历史恢复身份不丢。在三个引理与完整行捕获成立时，本文逐目标 key 的行收敛证明才可应用。Live 的创建/移动/删除、源槽位复用和相邻 schema 双向分配是新增证明面。独立的有限 identity 模型覆盖其中一部分原子时序，不能声称真实事务或所有复用情况已有正式验证。

需要新增原型：同 h 不同源（包括相同 value）、scanner 与 DML 抢 F/R、事务回滚后重试、源跨分区移动、目标跨分区移动、删除再插入、切读后 insert/update 再 rollback、两套 schema 分配同一候选，以及 checksum 开启且 handle 改变。每个场景检查映射、reservation、live-link、record 和索引一起一致。真实 TiKV 确认 claim 冲突、幂等与 GC；先别把成本较高的 map 做成无证据的“轻量结构”。

性能增量为 O(N) 小 KV 元数据 + O(DML) 映射查找及捕获，基线 record 和索引仍 ingest。应分别测 map 事务数/字节/缓存命中、影子 handle 分配与 shard overflow、记录重编码、row merge 与前台延迟。扫描批量 map、只对受影响分区建 map、按 shard 分散 map key 是可用优化方向；任何缓存只能缓存已持久结果，不能成为唯一来源。

修正结论：通用的独立影子 handle 方案需要完整行身份层；支持单父分区 nonclustered 拆分并不需要该层。二者都无需重写 DXF/global sort。保守映射可以作为正确性起点，不能保证性能收益与 clustered 相同。若目标是解决此客户，应把它作为必要里程碑，不能仅以 clustered 路径验证通过交付。

### 10.3 覆盖面边界

隐式 row ID、sharded handle、时间表达式 RANGE 与多个 global index 的组合仍需要专门验收。形状适配不等于部署、复制或生产正确性已经认证。本文保留协议审查过程，对外版本不包含客户调查标识或原始调查日志。

## 11. 证明、模型与实现验收的分层证据

协议层的安全性论证见第 3、5、6、7、8、9 节。有限模型见 [partition_row_delta.py](../agents/ddl/models/partition_row_delta.py)，不依赖第三方包。正常 suite 穷举 513 个有限 mutation history，共 101125 个可达状态访问、6297 个完成终态，在每个 Direct 状态满足 source == target。状态数按 history 累加，跨 history 不去重。它假设两个 identity 已固定且单射，用户/merge 事务原子且写冲突真实；不模拟索引、MDL 实现、RPC、GC、存储异常或所有 SQL 语句。

执行命令：

```bash
python3 docs/agents/ddl/models/partition_row_delta.py --suite
python3 docs/agents/ddl/models/partition_row_delta.py --unsafe
python3 docs/agents/ddl/models/partition_row_delta.py --mutation missing-delete
python3 docs/agents/ddl/models/partition_row_delta.py --mutation late-import
```

第一条退出 0；其余故意破坏协议的命令应退出 1 并输出反例。反例分别证明 Capture drain、显式 Delete 与迟到基线控制不能省略。不是把模型证明冒充 TiKV 实现证明。

后续实现验收采用确定性 failpoint，不只随机压测：

| 需验证的前提 | 必测时序与观察 |
| --- | --- |
| MDL 捕获与 merge 屏障 | 两个 domain：长事务停在捕获前/Ready 前，DDL 被挡住；释放后才扫描/merge；仅访问未变更分区也阻挡 global 边界 |
| 事务语句覆盖 | insert/delete/update、PK 变更、跨新旧/未变更范围、REPLACE/UPSERT、语句失败及事务回滚；相同 key 删除再插入 |
| merge 串行化 | merge 读 delta 后暂停，DML 提交，再恢复 merge；反向顺序；检查真实重试及最终 raw record |
| 首版 identity | signed/unsigned 边界、同 h 重试、异常源路由/重复 record key；不得按相同 value 去重 |
| importer 合同 | server 已接收后响应丢失、旧 executor 失联再恢复、Region split/leader 切换、重复导入；验证没有跨阶段新 TS 和未知副作用被当完成 |
| 索引输入适配 | 新分区全部 local、全表 global、不重复扫描旧副本；唯一键复用、NULL、跨分区移动及支持编码版本 |
| 生命周期 | 每阶段 owner/executor 故障、pause/resume/cancel、切读后 DML 再 rollback、Public 后取消拒绝、cleanup 失败后恢复 |
| GC/容量 | S 保护续约、真实 GC 开启时长暂停与失效、对象文件缺失、临时空间满；不以 disableGC 证明此项 |
| 发布互斥 | MDL off 与 job admission 竞争；旧节点加入；升级暂停；不支持组合预先选 txn，无半途 fallback |

SQL 对比要与 raw KV/PID/handle 检查、ADMIN CHECK TABLE、真实执行计划的索引扫描结果、唯一键拒绝、delete-range 与对象清理记录结合；仅 COUNT(*) 不够。mock 通过后仍需真实 TiKV，尤其 MVCC、SST 重放、GC 和 ingest 响应丢失。

## 12. 代码落点与交付顺序

模型字段放在 pkg/meta/model 的版本化 partition reorg metadata；持久化协议版本、phase/row state、各组状态、源/目标 PID、固定 S、delta PID、manifest/importTS、task/attempt 身份、barrier version、abort/cleanup 描述和 MDL dependency。大 manifest 存 task 元数据或对象文件，job 只引用且校验 hash，不膨胀每次 schema diff。

pkg/ddl/partition.go 负责新旧路径选择、外层可见性及串行 phase；拟新增 partition_ingest.go 衔接 record 任务、barrier 和恢复。pkg/table/tables/partition.go 及拟新增 partition_reorg_delta.go 负责 DML 路由、raw row 与 delta；tablecodec 放共享 key/value helper。索引复用既有 backfilling_* 和 index_merge_tmp.go 组件。pkg/ddl/ddl.go:SwitchMDL 与 admission 共用持久依赖保护。delete_range/job finishing 登记新资源清理，不能只修改 happy path。

先交付 clustered 整数 PK 的行协议原型、真实 MDL 屏障与 importer 契约实验，再接 local、global，最后补全部故障与 GC 验收。每个里程碑更新 ExecPlan；新增测试需确认实际匹配，遵循仓库 failpoint/Bazel/Ready 规则。现有 ADD INDEX 的回归继续原样跑，适配用例单独增加。性能测试放在正确性门槛之后。

最终设计结论：该受限首版有明确的协议、组件边界和条件化正确性证明，值得实施。单父分区 nonclustered 拆分可以避免逐行映射；通用合并不纳入首版。MDL 有现成屏障可复用，索引有成熟 pipeline 可复用；行捕获/merge 和跨 attempt importer 完成合同是主要新增工作。设计完成不等于功能可上线，未通过上述存储及故障验收前不开默认路径。

## 13. 本轮验证记录与结论边界

2026-11-02：已执行源码路径核对、有限行模型正常 suite 和三个负向变体，文档本地引用、空白、code fence 与 ExecPlan 必需章节检查。`make lint` 退出 0，但 local-notes 混合包与 macOS find 参数诊断导致 Go lint 范围不能视为完整；dashboard linter 已执行。未修改产品 Go/Bazel 文件，不运行 Go 单测、真实 TiKV 或 benchmark。

首版设计选择已确定；实现安全性依赖正文逐项规定的合同，尤其 MDL、ingest attempt 完成和事务冲突。nonclustered 扩展已有候选协议与有限身份模型，但真实事务竞争、allocator/缓存、checksum、存储故障实验尚未执行。不能把 clustered 行模型的 10 万状态用于声称 nonclustered 已获验证。支持客户案例还需证明分区表达式、部署和复制路径，不能只增加一个 handle-map helper 即宣称覆盖。

nonclustered 有限 identity 模型见 [partition_handle_identity.py](../agents/ddl/models/partition_handle_identity.py)，执行 `python3 docs/agents/ddl/models/partition_handle_identity.py` 得到 `PASS: 96 schedules; 768 transitions; identity and rollback invariants`。它枚举固定 8 个事件的全部合法顺序，包括同 handle 抢目标、先移动后扫描、删除 reservation 保留、切读后 insert/move 和旧身份回退重建；原子 claim 和真实 allocator 合同是模型假设。它不模拟 claim 事务读写交错/重试、源 record KV、无限 slot 复用、索引、checksum、GC、MDL 或 TiKV，不能替代这些实现验证。

## 14. 2026-11-03 简化审查：先证明不需要映射的范围

### 14.1 审查结论与撤回的过度泛化

之前把 nonclustered 通用合并的身份层扩展到所有重组，导致 RowToken、SourceOwner、F/R、Live 和双向 allocator 成为默认负担。这个推广没有必要。首版支持每个新分区唯一旧父的纯拆分，可以同时拆多个相邻旧分区，handle 保留不变；通用合并、跨旧边界重划继续走旧 txn 路径。第 10.2 节映射方案降为备选，不作为该拆分路径依赖。

新的主流程仍是 record 基线 ingest、行 temp merge、local 构建、global 构建、切读。新增加的数据结构只包括独立 temp row 空间、作业阶段/任务身份和静态父分区关系；父关系大小随分区数增长，不随行数增长。索引临时结构继续复用现有实现。

### 14.2 正向单射由分区关系保证

此前 len(DroppingDefinitions)==1 是充分条件，不是必要条件。当前首版条件是全部 AddingDefinitions 使用同一分区规则，每个新分区范围完全包含于唯一一个旧分区，旧范围被无重叠完整细分；可同时涉及多个相邻旧分区。需要额外核对原始下界与新旧末端界相等：当前 checkReorgPartitionDefs 对最后一个旧 RANGE 分区允许改变末端界，SQL 校验通过并不自动满足这里的条件。末端变更先走 txn；MAXVALUE 保留为 MAXVALUE 才满足常见 pmax split 条件。对任意新分区 q，持久计划确定唯一的 parent(q)=P；不同 q 的 P 可以不同。扫描行须验证真实旧路由确为 P，且新路由落入其子分区；不能默许 EXCHANGE WITHOUT VALIDATION 造成的非法归属。

搬行函数为 `(P,h,row) → (newRoute(row),h,row)`。如果两个源行映射到同一个 `(q,h)`，它们原本都必须在同一个 `(P,h)`；物理 record key 唯一，因此不能是两条同时存在的不同源行。即使表的其他旧分区也有相同 h，也不参与这个目标范围。源槽位删除后再使用可以按完整 DML old/new image 的 Delete/Put 收敛，不需要给每个历史行实例保留 RowToken。

这个证明不依赖“表从未 EXCHANGE”、AUTO_INCREMENT 普通列全局唯一、或 shard bits 对应分区。它依赖合法路由、单一父分区和每个旧 record key 的物理唯一性。对多旧分区合并，同一个 q 可以接收不同旧 PID 的相同 h，上述证明不成立，首版拒绝选择该 fast 路径。

RANGE(TO_DAYS(create_time)) 可以满足同样拓扑，无需改成 RANGE(pk)。old/new 共用 job 捕获的 SQLMode、Location、列类型与表达式上下文，覆盖 datetime 边界/NULL/非法日期测试。表达式支持与部署能力仍需实际验证，不能由拓扑证明替代。

### 14.3 保留 handle 的反向双写合同

不能只说“split 所以全部无冲突”。切读后两个子分区 q1/q2 都要维护同一个旧父分区 P，若向 q2 插入 h=7，而 q1 中已有 h=7，反向写 P:7 会覆盖另一行。隐式 allocator 通常能避免很多重复，但正确性不以“通常”作依据。

在两套布局都必须完整的阶段，保持不变量：对于父 P，子分区的当前 live handle 集合彼此不相交，且它们的并集与 P 的 record 集合一一对应。源快照保证这个起点；后续通过正常事务维持。

对自动生成 handle 的 INSERT，及实际产生新 handle 的跨分区 UPDATE，先选择候选 h，再检查权威目标 record key 和其旧布局对应 record key。两者任一被另一行占用就重新分配 hidden handle；用户列及 AUTO_INCREMENT 列值保持不变。最终两套布局使用同一个 h，检查和两套 record/index 写入处于同一事务。两个并发事务抢占同一个父 P:h 必须在该 record key 上冲突并重试，不能做独立于写事务的预检，不能无条件 raw Set 覆盖它。

这些 record key 本来就要为双写维护，无需再建逐行 reservation map。隐式 handle 重试只针对影子/权威 record-key 冲突，不吞掉真实业务 unique index 冲突。候选耗尽/溢出依真实 allocator 错误处理。实现应在语句 staging 层撤销本次 handle 尝试的 record/index/temp 修改，不能重做或更改用户列的 AUTO_INCREMENT 分配。

更新已有行且保留 h 时，使用实际 old/new image 路由到两套布局，删除旧位置、写入新位置，同事务完成。不要沿用当前 nonclustered “目标 raw value 与 oldData 不同就换 handle”的判断：行 merge 之前目标可能只是旧基线，值不相等不代表是另一行。拆分路径以 parent/h 关系判断槽位，按 RowState 使用 raw row/temp 协议。

RowMerge 开始之前 importer 必须已经结束、Capture-only 事务已经退出；该路径沿第 7 节证明收敛。切读前旧父分区权威，切读后在可回退阶段新子分区权威，但完整旧父分区仍是反向目标。反向 helper 不直接照搬现有 partitionedTableAddRecord：当前代码第二次 addRecord 出错只返回错误，并没有候选重分配循环；需要补齐记录冲突的事务重试行为。

用户显式写 `_tidb_rowid` 暂不纳入该首版；识别用户语句与内部保留 handle 的调用，不能把所有 hasRecordID 都误判为显式写入。它可以有意在不同新子分区使用相同 h，无法同时保留显式指定值和旧父分区唯一性；不能偷偷给它换 h。普通自动 INSERT/UPDATE 不被排除。

在旧布局仍能被 SQL 读取或回退之前，禁止停止对应旧 record/index 的维护。正常 old/new row codec 和 checksum 版本仍需验证；handle 不变仅消除因重分配 handle 导致的 checksum 变更，不代表任何旧格式 raw value 都可无条件搬到新 physical key。不可回退之后才沿现有退出机制停止旧写入，GC 等待旧协议使用者退出；不要提前移除旧父分区 guard，却仍允许要求旧布局完整的事务提交。该生命周期同样需要多 domain 的真实测试，有限模型不证明源码当前已经具备这些边界。

### 14.4 行状态复用 BackfillState，减少新枚举

前文 Capture/Ready/Direct 是行为解释，不是要再实现一套枚举。在分区 reorg 元数据中使用独立的 RowBackfillState，值直接复用 model.BackfillState，而非新增另一套 Capture/Ready/Merging/Direct 枚举。字段独立于 IndexInfo.BackfillState，不表示已存在的字段。

| RowBackfillState | 行 DML 行为 | 后台许可 |
| --- | --- | --- |
| Running | 权威布局 + 旧/新受影响目标键 marker | record 基线 sort/ingest |
| ReadyToMerge | 两套 record + 同格式 marker | 等待 Running 事务退出，禁止开始 merge |
| Merging | 同 ReadyToMerge | 事务合并未应用 temp row |
| Inapplicable（完成后） | 两套完整 record，不再生成 temp row | 启动索引组，随后切读 |

Inapplicable 本身不表示“尚未启动”还是“已完成”，必须结合持久 job phase 判断；旧 job 缺新协议版本仍走 txn。Running/Ready 的 schema 准备边界仍必要。ReadyToMerge 与 Merging 前台行为相同，作用是让原协议写入者彻底退出，与 ADD INDEX 的现有解释一致。当前 DirtyRows 只保留脏目标键及 nonce，不保留行值/Delete/applied；不能把 temp index value 格式直接当作行格式。LocalPrepare/Build 等是后台工作名，不为每个名字新增一个前台 schema 状态；只有改变 DML 行为时才发布 schema 和等待 MDL，普通扫描/排序进度用任务 checkpoint。

### 14.5 历史候选：完整值重放已被初始化反例否定

本节记录曾经的完整值重放候选，不再是当前推荐。第 15.2 节发现捕获启用前后旧/新 schema 写入交错可使 temp 行值早于基线，因此首版改为脏键加当前源点读。以下无 applied 标记的推导只说明捕获完整生效后的窗口，不能覆盖初始化。

Running 时用户事务维护旧权威布局及 temp `(operation,row)`；ReadyToMerge/Merging 时同一事务维护两套 record 及完全相同格式的 temp。merge 统一执行：事务重读 temp，raw Put/Delete 目标，再删除 temp，提交。已由前台应用的相同完整值再写一次仍正确。行阶段尚未构建新索引，重放不会重复执行 AddRecord 或产生新的索引事件。

如果 merge 先提交，后来的 DML 写入更新的 record 和 temp；如果 DML 先提交，merge 读取其完整最新值再应用，或因同一 temp key 的删除/写入冲突重试。不存在 read-check 后脱离事务回放的窗口。cursor 扫过后新出现的 temp 来自 Ready/Merging 协议，已原子应用 record，因此无需回头追求 temp 清空。Running-only 写入者仍必须在扫描 merge 前退出；无 flag 不意味着可省屏障。

代价是对部分已双写 temp 多写一次 record，换取更小格式和单一 merge 规则。这是符合“正确性优先、接受额外工作量”的取舍；未来性能确有需要再引入 applied 优化。该简化只用于完整 record value 的 Put/Delete，不能推广到具有 unique/delete/version 语义的 temp index merge。

### 14.6 未采用的其他简化

仅以当前快照没有重复 row ID 或历史没有 EXCHANGE 来免映射，无法约束之后 DML，仍不采用。仅在发现碰撞时修改源 row ID 的方向可以复用当前 txn worker 思路，但固定 S 的 ingest 不能逐行照搬：旧 S 仍可能包含重键前的 record，旧 handle 的删除与另一来源行可能共享目标 key，基线排序和 delta 必须一起证明。它不自动比限制 split 更简单。

通过短暂阻塞全部写入实现原子切换，可以省一些反向协议，但长事务可能把短暂停顿变成长时间停写，也改变现有在线 DDL 行为，不作为默认路径。把旧/新 handle 合成更宽 record key 会改变 nonclustered int handle 编码及索引/执行器合同，远超本次范围。全量 RowToken 映射在首版 split 不再有必要。

旧 TS 导入、记录与 local index 融合仍是后续优化或对照方案；不能因为简化行身份就移除 MDL 屏障、显式 Delete、固定任务身份、GC 保护和失败关闭策略。

### 14.7 本轮证据、模型与范围限制

新增有限模型 [partition_split_identity.py](../agents/ddl/models/partition_split_identity.py)。执行：

```bash
python3 docs/agents/ddl/models/partition_split_identity.py
python3 docs/agents/ddl/models/partition_split_identity.py --unguarded
```

正常模式枚举 81 组合法源布局，split 全部保持单射，其中 56 组在合并时出现碰撞；另枚举 1296 组反向 mutation history、5184 次原子转换，旧/新行集始终一致。去掉父分区记录占用检查后，一个向其他子分区插入相同 h 的操作就产生反例，预期退出 1。

这个模型把事务当作原子动作，且 allocator 可重新发候选作为假设；没有枚举真实事务读写交错/锁、缓存分配器、row codec、索引、MDL、ingest 或 GC。源 snapshot 到行 merge 的版本竞争由此前 row delta 模型抽象覆盖，两个模型通过不等于完整存储实现已证明。后续第一项真实原型就是两个 session 同时抢父 P:h、同一事务多次 INSERT、跨子分区 UPDATE、新 schema INSERT 后 rollback，以及 shadow unique 冲突不得误重试。

归档 submitted_names.json 中取消后的 job 31746/31752/31758 明确是单个 pmax 拆为日期分区和新的 pmax。这是当前客户表的实际重试形状，支持优先研究该窄路径；它不是 job 31610/31613 原始 SQL 的证据。部署仍是另一个能力维度，不能直接宣称覆盖 Essential/实际线上版本。

最终审查结论：先做有静态单射证明的非 clustered split，加小范围的双写 record 冲突重分配，再复用行 temp 状态与成熟索引构建，比通用逐行身份映射更清晰、基线事务成本更低。它是一项明确缩小范围的候选设计，不是已经实现的优化；如果双写 helper 或退出窗口无法通过真实并发验收，就保留该类型 txn 路径，不能靠省略检查上线。

2026-11-03 统一重放模型验证命令：

```bash
python3 docs/agents/ddl/models/partition_row_delta.py --suite --replay-all
python3 docs/agents/ddl/models/partition_row_delta.py --replay-all --unsafe
python3 docs/agents/ddl/models/partition_row_delta.py --replay-all --mutation missing-delete
python3 docs/agents/ddl/models/partition_row_delta.py --replay-all --mutation late-import
```

正常版通过 513 组历史、94911 次状态访问、6297 个终态；三个负向变体仍分别找到 Capture drain、缺 Delete、迟到 ingest 反例并退出 1。旧默认模型保留用于对比，不表示首版还存 applied 标记。RowDelta 的 temp 原子读写仍属于模型假设，非真实存储验证。

本轮采用 Ready 文档/模型验证范围。已实际执行本节全部正常及负向命令，并检查负向退出码为 1；引用/空白/ExecPlan 结构检查通过。`make lint` 再次退出 0，但仍有既存 local-notes 混合包及 macOS find 参数诊断，Go lint 文件范围不完整，不记为完整 lint 通过；dashboard 检查运行完成。未修改产品 Go/Bazel 文件，未执行真实 TiKV、Go 测试或 benchmark。反向 helper、实际事务候选重试、MDL 退出、codec、复制与存储完成合同仍需实现验证，模型数量不代表这些条件已经证明。

### 14.8 多旧分区分别拆分：把限制放在来源关系上

现有 SQL 入口 ReorganizePartitions 接收多个 PartNames；getReplacedPartitionIDs 对 RANGE 要求所选旧分区相邻。这里讨论的 DXF/ingest 路径尚未实现，不把 SQL 已支持等同于新的 fast 路径已可用。

无映射方案无需限制一次 DDL 只能选一个旧分区。准确 admission 是新布局为旧布局的纯细分：分区表达式/类型不变，选中范围外边界不变，全部旧内部边界仍出现在新边界集合中，每个新分区 q 有唯一的 parent(q)。例如：

```text
旧：[0,100)                 | [100,200)
新：[0,50) | [50,100)       | [100,150) | [150,200)
```

P0 和 P1 中即使都有 h=7，它们也会去属于不同父分区的不同子分区。若两个来源输出同一个 (q,h)，唯一 parent(q) 强制它们原本同在一个 (P,h)，因此不能是两条同时存在的不同源行。第 14.2 节的单射证明逐 q 原样成立，不需要增加逐行映射。

以下虽然新分区更多，仍不是纯拆分：

```text
旧：[0,100)                 | [100,200)
新：[0,50) |          [50,150)          | [150,200)
```

[50,150) 跨过旧边界 100，可能同时接收 P0/P1 的 h=7，属于重划并混合来源；nonclustered 首版继续旧 txn。不能用新分区数大于旧分区数来判断是否满足无映射条件。

反向记录占用检查按每个 q 的 parent(q) 执行；在不同父域中使用相同 h 无需互斥。跨父分区 UPDATE 在同一用户事务删除旧域行、选择并检查目的父域的候选 handle、写入两套新位置及 temp，使用实际 old/new handle，不要求整个表的 h 全局唯一。实际事务锁顺序、死锁重试和同事务多行移动仍需新增专项测试，不能把单父域原子模型当作跨父并发验证。

规划上把静态 parent map 固定到同一 job/generation，扫描 DroppingDefinitions 可并行；所有 record 组完成后再统一进入 Ready/行 merge，保持同一个逻辑表上的 MDL 屏障，不引入父分区各自独立切读。Local 组覆盖全部新分区，Global 组在最终全表布局统一构建一轮，不按父分区重复重建。

admission 用现有类型/排序规则的边界比较建立包含关系，而不是比较 LessThan 字符串；恢复使用持久的旧边界与 parent 计划。已有 partition_split_identity.py 的正向模型本来就包含两个旧父分区和四个新子分区，81 组布局保持单射；反向 1296 组历史仅覆盖单父域局部操作，未覆盖跨父域并发，真实验证范围仍按上文明确保留。

## 15. 用户认可限制后的收敛协议（当前规范）

本节优先于历史候选。用户已认可“纯拆分、保留全部旧边界和整体范围，每个新分区唯一旧父”的限制；首版对 clustered 和 nonclustered 统一选取此形状，不把通用 clustered merge 顺带纳入。当前授权仍是设计和原型验证，不修改产品执行路径。

### 15.1 Admission 与不可变 SplitPlan

先沿现有 SQL 校验处理错误语句；fast classifier 不改变合法 REORGANIZE 的语法与结果。形状或能力不适用时，在启用新协议之前选择完整旧 txn 路径，记录原因。已启用后只按所选协议恢复或回滚，不中途混回旧搬行器。

逻辑判定输入为原始完整分区定义、所选旧 PID、拟议新边界、原分区表达式/列类型和冻结的求值语义。先核对源存在且对 RANGE 相邻，分区规则未变、外范围相等、旧内部边界全部保留；再按区间顺序建立每个新分区唯一的 parent。不能把“新分区更多”当纯拆分，也不能只依赖 checkReorgPartitionDefs：最后一个 RANGE 分区的 SQL 校验允许末端改变。

边界使用 TiDB 类型语义比较。RANGE 标量沿 getRangeValue 的 signed/unsigned 求值，MAXVALUE 独立处理；RANGE COLUMNS 在支持范围内沿已有列类型比较，不做字符串排序。首版表达式限已存储类型上的确定性规则，明确覆盖整数列与 TO_DAYS(DATETIME)；上下文敏感且无法证明一致的规则不启用。冻结求值上下文不等于可以忽略源 DML 实际路由语义。

逻辑判定可以在分配新 PID 前完成，先用新分区 ordinal 表示 child。第一次元数据执行再绑定真实新 PID、temp physical ID 和替代 global index IDs，与协议版本/模式原子持久化；此后固定不变。计划包含原始/目标边界与 IDs、ParentByNewPID、完整源范围、index group 输入/输出集合、schema 编码/路由摘要和协议版本。摘要排除动态 BackfillState、schema revision、并发度与进度字段，避免合法阶段转换造成假 mismatch。

64 位边界与 IDs 使用强类型或带类型的十进制字符串编码，不能经 float64 中转。planner 本身只需按旧/新区间做线性归并；多父只增加 O(分区数) 元数据。source/target PID 不能相同或复用。source 元数据和数据的旧路由异常分别报错，不用 last-write-wins 掩盖。

可执行 admission 原型 [partition_split_plan.py](../agents/ddl/models/partition_split_plan.py) 对已求值整数边界工作，覆盖 18 个边界/ID 用例、256 对细分关系，并用独立“旧切点为新切点子集”有限 oracle 验证其中 81 对可接受。它不是 TiDB parser/生产 planner，不验证真实表达式、collation、MDL 或部署能力。

### 15.2 初始化反例与 DirtyRows

完整值 temp replay 的反例是：新 schema 用户事务把源改为 v1 并捕获 temp=v1；尚存的旧 schema RC/长事务稍后把源改为 v2 而未捕获；等待旧事务退出后，基线 S 正确读到 v2。若最终把 temp=v1 覆盖回目标，结果错误。DeleteOnly 阶段只捕获旧位置删除、未捕获更新后的 Put，也可能留下这种不代表 S 后变化的历史动作。

MDL 保证旧事务最终退出，但不意味着它必定先于所有已捕获新事务提交。旧的正向模型从“捕获已生效”开始，遗漏了此初始化窗口，因此此前的 full-value replay 通过数字不能作为完整协议证明。

当前选择是只记录 DirtyRows[(q,h)]。建议 value 为版本和 txn.StartTS nonce，目的只是让每个相关事务确实写 marker；不保存历史行值、Put/Delete 或 applied 标记，不根据 nonce 与 S 的大小过滤，也不把 startTS 当 commitTS。相同用户事务多次触及一个 marker 可合并为最终一笔 KV 写入；语句失败按 staging 撤销，事务回滚随源数据一起撤销。

取 S 之前仍等未完整标记的旧事务退出，使 S 覆盖其全部提交。S 前已有 marker 可以保留，它们只触发当前源修复。S 后任何影响目标布局的提交都标记 old/new image 对应的目标键；删除即使目标不存在也必须标记，不能删除 marker 或只标记新位置。

### 15.3 Repair 事务与可证明的并发边界

对 marker K=(q,h)，在同一个新 KV 事务中执行：读取当前开放的逻辑 range fence；读取 marker；按 ParentByNewPID 得 P；点读旧源 `(P,h)`；若源存在且新路由仍为 q，按当前源值写目标，否则删除目标；删除 marker；写本 range fence nonce；提交。

marker、源行、fence 都使用 repair 事务的同一 startTS，不从 S 或旧扫描缓存取修复值。源 lookup 使用真实 row handle；PID 在 key 中单独表达，不让携带其他 PID 的 PartitionHandle 覆盖 parent(q)。一笔批事务可复用同一 source point read，但不能跨事务缓存修复值。

完整值重放不再用于行 merge。真实 KV 层每笔相关 DML 都写 marker；若用 SQL 实验模拟，不能用可能被优化成 no-op 的“值不变 UPDATE”冒充这次写入。marker 与源/目标同事务，读后删除必然与并发相关 DML 发生写冲突。Row repair 不调用 AddRecord，不生成索引事件，也不发新 handle。

后台 repair 的提交失败可整笔重新开始；用户事务的 9007/死锁/锁超时沿 TiDB 原有事务或语句重试规则处理，不能在表层吞掉失败、偷偷更换用户事务快照。候选 handle 的 occupied 重试只在同一语句 staging 中重新选择 hidden ID，用户列和 AUTO_INCREMENT 已生成值不变。

Running → Ready 的导入屏障和 Ready → Merging 的旧 Running 事务退出仍必要。新 schema DML 已原子双写目标/marker 后，cursor 之后出现的 marker 不再代表未应用数据，所以完成不要求 temp 空。删除/插入重用同一个物理源槽位按当前状态修复即可，不保留 RowToken。

### 15.4 双写、锁及跨父域 DML

在两套布局必须完整时，以权威布局校验 SQL 约束，shadow 只维护 KV/索引投影，不额外执行基于未完成或已退休 shadow 的业务约束检查。特别是切读后不能让旧 shadow 断言或旧业务 unique 检查变成用户可见的错误；成熟 index backfill 的版本/重复检测算法不改。

nonclustered 新候选检查权威 record 和对应父/子 shadow record；两者有别的 live 行就重新分配 hidden handle，随后同事务写两套相同 handle。clustered handle 来自主键值，不允许通过此循环擅自修改用户主键；按普通 SQL 主键约束处理。更新产生新 handle 时，标记实际 old/new handle；跨父移动删除旧域并检查目的父域，不需要全表 handle 全局唯一。

未变更分区的 old/new physical ID 相同，只执行一次普通 mutation；行脏键空间只涉及 AddingDefinitions。进入拆分范围时标记新目标，离开时标记旧目标，不能遗漏一端。global index 捕获在自己的阶段覆盖全部表，不能据此重复更新未变更 local index 或重复统计。

optimistic 路径的 occupied 检查与 record 写入必须同事务，写集合冲突使失败方回滚。pessimistic 路径要使用会话事务管理器的 ForUpdateTS、锁等待/取消/死锁语义，包含不存在的候选键。当前 PointGet 的 RC 路径可能 LockOnlyIfExists，不能把 RC 的 SELECT FOR UPDATE 当作空槽位 reservation；guard 必须按当前 locking-read/返回值判断，不能锁后再用旧 snapshot 误判为空。

表层不能直接 import executor 形成依赖环。拟在 tblctx.MutateContext 增加可选 reorg mutation support capability，由 session 适配层提供锁上下文和真实 allocator；对应细节参考现有 tblsession 与 ExchangePartitionDMLSupport 组织方式。这里是拟新增合同，不是现有接口。普通 session 必须具备该能力；不支持的写入路径须在修改源之前拒绝/等待。

LOAD DATA 等只有确实经过同一事务 mutation hook 才在支持范围。绕过 hook 的 IMPORT/外部 SST 恢复/非事务批写，以及尚未验证的 pipelined 模式，不能在同表作业期间悄悄写源；第一版需要可执行互斥或写入前能力检查。源有业务变更却没有 marker 的路径会直接破坏证明，不靠文档要求用户保证。

### 15.5 切读、回滚、退休及 repair fence

| 窗口 | SQL 权威布局 | 必须保证 | 可否回退 |
| --- | --- | --- | --- |
| Capture/RowBuild | 旧 | 旧完整；S 后变化有 marker | 可，停后台后丢弃新数据和 marker |
| RowReady/Repair/IndexBuild | 旧 | repair 收敛后两套行完整，索引按组构建 | 可，无需先补完索引 |
| 发布 DeleteReorganization 至其 MDL 完成 | 按各事务 schema 读旧/新 | 两套行和必需索引都完整、双方正确双写 | 按现有窗口可回退 |
| DeleteReorganization 已同步 | 新 | 新权威，旧仍可用于回退 | 可，先恢复旧可见性并同步 |
| Public 已发布 | 新 | 新布局正确；旧数据可不完整，不再参与 SQL 约束 | 不可回退，沿当前代码前向收尾 |
| Public 同步且旧写入者全部退出 | 新 | 无旧布局 Put/在途构建，清理归属持久 | 仅清理，不重启构建 |

切读还必须满足第 16 节的 readTS/布局版本合同；仅有 MDL、双写及最新行集等价不足以保证所有读正确。Public 前保持父域 handle 单射及 old/new 等价；Public 后不再把 old/new 等价作为不变量。所有仍可提交的客户端都必须读新布局，旧 shadow 可沿 delete-only/退出协议变得不完整。缓存旧双写协议的事务可以暂时留下 shadow 写入，但不能因为 shadow 不完整而修改新权威数据的语义；禁止 GC 早于这些 writer 退出。Rollback 只在旧仍完整的窗口恢复原 Definitions；恢复过渡继续保持必要双写，直到新读写协议退出，再关闭并清理新布局。

Dirty repair 还需要单独防止旧后台 attempt 在 Public 后点读过时旧源，覆盖新数据。第一版使用每个稳定逻辑 merge range 的事务 fence：worker 每笔 repair 同事务读取 Open 并写该 fence nonce；owner 结束/取消该阶段时，把所有相关 fence 写为 Closed。旧事务若晚于 Closed 提交就写冲突；新事务读到 Closed 或缺失则停止，worker 不得创建/重新打开 fence。

range fence 是 O(逻辑任务范围) 元数据，不是逐行映射，不放在会被 DXF 活跃/历史搬迁自动删除的行中，也不能与将被整段删除的 DirtyRows 共用 GC 范围。Open/Closed 值关联 job/phase/generation、range 边界及计划摘要；成功 Closed 还关联已持久化的范围终点/完成摘要，Abort Closed 不能被当成成功完成。Closed 作为普通 live value 保留；普通数据 GC 不删它。后续若要清理 Closed 记录，需要先证明全部旧 attempt 和在途事务已终止；不能只按时间删除，否则缓存 Open 的旧事务可能在 tombstone 被 GC 后重新写入。

成功关闭 fence 不是完成证明：还需每个完整范围 cursor/manifest 在修复事务确认提交后达终点。提交结果未知时不推进 cursor；只有所有范围完成、fence 关闭，才进入 RowDirect/索引阶段。范围覆盖全部相关 temp 前缀，不能在 Running writer 尚存时用暂时 min/max 缩小后遗漏标记。每个新分区可分多个固定逻辑 range，避免所有批事务争同一个 gate。

此事务 fence 不解决 SST ingest RPC：import 仍按已规定的稳定 manifest/TS、完成确认和 unknown-result fail-closed 合同处理。不能把 SQL range gate 想象成已有的 TiKV ingest fencing API。

### 15.6 可重复验证及仍未验证的部分

执行以下不修改产品路径的命令：

```bash
python3 docs/agents/ddl/models/partition_split_plan.py
python3 docs/agents/ddl/models/partition_split_concurrency.py
python3 docs/agents/ddl/models/partition_split_concurrency.py --lifecycle
python3 docs/agents/ddl/models/partition_split_concurrency.py --unguarded
python3 docs/agents/ddl/models/partition_split_concurrency.py --ignore-conflicts
python3 docs/agents/ddl/models/partition_split_concurrency.py --lifecycle --early-gc
python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile
python3 docs/agents/ddl/models/partition_row_delta.py --startup --reconcile
python3 docs/agents/ddl/models/partition_row_delta.py --startup --replay-all
python3 docs/agents/ddl/models/partition_split_sql_probe.py
```

正常 admission 18 个专项用例/256 对关系通过；并发模型 7 个两写者场景，363 个节点、184 个终态，包含 16 次冲突重试边及 142 次回滚边；生命周期模型在已排空旧读者前提下，7 场景、2079 个节点、604 个终态通过。缺父 guard、忽略 write conflict、提前 GC 都找到预期反例，退出 1。

脏键修复模型通过 513 组历史、94784 次状态访问、6170 个终态；新增 startup 场景为 9 个状态、1 个终态通过。相同 startup 下旧 full-value replay 输出 source=v2/target=v1 并退出 1，明确否定旧候选。此前模型把一组 marker/源视为规范目标投影；不把它当真实 source scanner/路由实现证明。

SQL probe 只连接自己启动的本地 socket，使用独立 unistore 目录，退出时停止自己的进程并保留 result.json、版本和日志。使用的现有二进制为 2026-08-07 构建的 dirty master，基准提交 10d7421b803edb5bc34c5bb879375572bf22e4aa，不是当前源码 44facb0。fixture old_rows/new_rows 是 KV 命名空间的关系表模型，不是新 DXF hook；真实 sql_shape/date_shape REORGANIZE 仍走现有 txn 路径。

它验证 optimistic 父槽位和 marker/fence 的 9007 写冲突、候选重选、跨父移动、用户回滚、RR 悲观空槽位锁的 1205 超时后重选、旧事务阻塞实际多分区 REORGANIZE 的 delete-only 状态。EXCHANGE 构造两个旧父分区都有 row ID 1/2，纯拆分后四行的隐式 handle 全部保留，ADMIN CHECK TABLE 通过；TO_DAYS(DATETIME)、NULL、两个时区、global index 强制读取也通过（结果以最近一次 probe 的完整记录为准）。

当前探针不证明新 record SST ingest、实际 mutation hook、真实 hidden allocator/cache、悲观 RC/KV adapter、同事务复杂批写、checksum、复制、存储完成/GC 合同，也不是多节点 TiKV 故障实验。上线前这些仍是必要门槛；不能以旧二进制/unistore SQL probe 代替当前源码的 failpoint、真实 TiKV 和 DXF 验收。

本轮完整 SQL 报告位于本机 `.codex-tmp/ps-sxmhpbt5/result.json`，二进制身份在同目录 `version.txt`；`status=PASS`、`owned_process_stopped=true`。启动/停止逻辑与复现命令保存在 probe 源码，数据/日志仅来自自建 fixture，不接触客户集群。该本地路径是证据位置，不是仓库可移植依赖。

当前设计完成度：已明确形状、持久计划、写入协议、初始化修复、事务 fence、状态边界和失败关闭条件；已对部分前提运行模型和 SQL primitive 实验。尚不能把其称为当前源码的新功能已经正确或可发布。下一步是把这些用例移植为当前 TiDB 的 hook/真实 TiKV 原型，优先验证悲观 RC、批量 staging/allocator、SST 完成与故障恢复，再接全量 record/local/global ingest。

代价模型也随当前选择更新：marker 只含键与小 nonce，降低宽行 DML 的临时写入体积，但 repair 增加当前源 point read。可在同一批事务按 `(parent,h)` 做 BatchGet/去重；不能跨事务缓存源结果。若长时间构建导致几乎全部行都变脏，repair 会接近全量事务工作量，不能承诺固定加速倍数。需测 distinct dirty keys、源点读字节、repair batch/gate 冲突、候选重试和前台延迟。

最终复跑补充：最近 SQL 报告为 `.codex-tmp/ps-sxmhpbt5/result.json`；旧事务阻塞 delete-only 连续观察 2.5 秒（测试 lease=1s），再提交旧事务并观察 DDL 完成。marker/gate 冲突均记录 9007，悲观 RR 锁等待记录 1205；closed gate 字段只表示已观察 Closed 值，不把它描述为已实现新 worker 的自动拒绝。全部自建 server 由 finally 终止。

负向 marker 用例使用 `python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile --mutation missing-delete`：默认历史中的删除可能被先前已有 marker 覆盖，不能强行把该历史要求为失败；完整有限 suite 能找到删除基线行却未写任何 marker 的反例。`--reconcile --unsafe`、`--reconcile --mutation late-import` 仍找到其他必要屏障的反例。

初始化还单独验证了 DeleteOnly 仅留下历史 Delete 的情况：源在 S 仍有 v2，但 replay Delete 会删掉新基线。`python3 docs/agents/ddl/models/partition_row_delta.py --startup delete --replay-all` 得到 source=v2/target=不存在的反例；`--startup delete --reconcile` 通过。具体 arming 实现可能留下旧 Put 或部分 Delete；当前脏键协议对两者都不回放历史内容。这个反例针对候选设计，不是对现有 ADD INDEX 或产品 reorg 宣称发现 bug。

本轮最终检查记录：按 Ready 文档/原型范围执行，15 组模型正向/负向调用的退出码符合预期，4 份文档的本地引用、空白、fence 和 ExecPlan 结构检查通过。`make lint` 退出 0，但既存 local-notes 混合包与 macOS find 参数诊断仍使 Go lint 范围不完整，不能记为完整 lint 通过。未修改产品 Go/Bazel 文件，未运行当前源码 Go 测试或真实 TiKV/DXF。全部具体实验命令见本节和 probe 源文件。

## 16. 正确性审查：条件化行证明与尚未闭合的端到端设计

### 16.1 目前结论

不能宣称整个 DDL 设计已经证明正确。现阶段成立的是条件化的行身份/修复论证；“最新行集收敛”不是“所有 SQL 读快照都正确”。模型/旧 binary SQL probe 是发现反例和核实前提的工具，不是完整证明。当前还缺读版本合同、SST 完成的可执行判定和适配层状态组合的证明。

### 16.2 可证明的行协议部分

假设：原数据合法路由、形状为纯细分且元数据不漂移；基线 S 保真完整，S 在未完整捕获事务退出之后取得；此后每个影响目标键的事务与源数据原子写 marker；repair 同事务读 marker/当前父源并删除 marker/写目标及 fence；merge 前 Running-only 写入者已退出，之后 DML 原子双写；扫描覆盖完整 temp 范围且 cursor 在已确认提交后前进；没有迟到 baseline 或阶段外 repair。

引理一，身份单射：如果两个源输出同一 `(q,h)`，parent(q) 唯一使它们同为 `(P,h)`，因此在同一源快照不能是两个不同物理行。此证明适用于多个旧父分别拆分，不能用于混合来源的新分区。

引理二，单键修复：定义 D(q,h,t) 为 t 的源读视图中 `(parent(q),h)` 的行值（且新路由为 q），否则不存在。repair 在快照 t 计算 D，并同事务写目标、删 marker。若并发 DML 改变 D，它必写同一个 marker，因此不能在 repair 读到旧值后，两者仍无冲突地提交一个错误覆盖。DML 先提交则 repair 读到新值或冲突重试；repair 先提交则后续 DML 双写新目标。对 D 无影响的源变化不妨碍此次修复。

引理三，遍历完成：S 后未变键由基线正确提供；变化键都被标记。merge 开始后没有 Running-only 提交，cursor 扫过后新 marker 都伴随已应用目标写。因此遍历完成时目标的最新行集等于旧布局按新规则的投影。源删除、移动、槽位复用按当前状态处理，S 前残留标记只增加冗余修复。

这些引理的结论只到 RowDirect 的行集。继续到切读，还需证明 nonclustered 反向 guard 在所有事务模式维持父域唯一性；各索引适配产生的输入和 DML 流等价于成熟 ADD INDEX 的合同；每个 reader 的 MVCC 时间与物理布局匹配。后二者不能由行集引理推导出来。

### 16.3 已复现的旧快照首次访问风险

增加探针：

```bash
python3 docs/agents/ddl/models/partition_split_sql_probe.py --late-snapshot
```

时序为：T1 BEGIN RR 并查询无关表，固定旧 readTS，但尚未访问待拆分表；T2 完成该表纯拆分；T1 才首次访问拆分表。旧二进制实测 fresh session 读到四行 `(1,25),(2,75),(3,125),(4,175)`，T1 静默读到空集。报告位于本机 `.codex-tmp/ps-8f2ngd1f/result.json`，状态 SILENT_MISMATCH、退出 1，own process 已停止。

这只是旧 dirty binary（10d7421b80）的观察，不是当前 44facb0 源码已复现的产品 bug；但该时序必须纳入新设计。把行复制到新 PID 的 MVCC 版本通常晚于旧 readTS，若读路径使用新分区定义配旧 readTS，就可能查不到搬迁前已经存在的行。MDL 只跟踪已访问相关表的事务，不能据此宣称所有较老事务都已退出。

已读当前代码：preprocess.tryLockMDLAndUpdateSchemaIfNecessary 对首次访问尝试更新到 latest TableInfo，针对新增 index 有专门隐藏逻辑；sessiontxn optimistic 普通 RR 读使用原 txn startTS。尚未证明分区布局存在等价的旧快照保护。自动提交只读、SnapshotInfoschema、staleness 等还会走不同的 MDL 分支，不能只补一个普通事务场景就结束审查。

### 16.4 必须补齐的读版本合同

明确禁止“没有对应 MVCC 历史的新布局 + 旧 readTS”静默执行。首版保守语义可以是：能够绑定到有效布局则正确读取，否则显式返回 schema-change/retry 错误；不能偷偷推进 RR 的 readTS，也不能把空结果当正常结果。

候选实现是维护布局 epoch 和新布局的可读下界：该下界须在 record 与全部必需索引完成、且持续维护已生效之后取得；遇到有效 readTS 早于新布局可读下界时，不使用该布局。它必须检查真正的 statement readTS/ForUpdateTS，而不是一律使用 txn.StartTS；要覆盖首次访问、已有 MDL、只读自动提交、plan cache、locking read、显式历史读/AS OF 及 GC。

这不是已完成实现规格：字段生命周期、元数据/计划绑定时点、旧计划正在执行时的有效区间、与正常 schema lease/GC 的关系仍需定下来。不能把“增加一个比较字段”直接当作已解决。透明保留旧布局读需要旧元数据与旧数据保留及读写分流；强制所有旧事务退出则会影响无关长事务。两者代价不同，当前优先论证显式拒绝不安全组合的保守方案，未证明覆盖面前不启用新路径。

### 16.5 端到端尚未关闭的证明义务

| 义务 | 现有证据 | 仍缺什么 |
| --- | --- | --- |
| readTS 与布局匹配 | 旧 binary 已有静默缺行反例；当前读源码已核对 | 当前源码确认、明确读取合同及完整路径覆盖；这是设计缺口 |
| 所有源写入与 marker 原子 | KV 模型及 SQL fixture 的原子/冲突行为 | 实际 mutation hook、RR/RC、批写、allocator/staging、绕过 hook 路径的可执行排除 |
| 新索引构建等价性 | 成熟 ADD INDEX 组件、旧 txn-mode 分区 SQL probe | 显式输入集合、handle/编码、每布局状态、全部 DML 流的等价论证与回归 |
| baseline 永不越阶段写入 | TiDB 导入重试/取消调用链已读 | context cancel 或 DXF canceled 不等于存储已无副作用；RPC 结果未知/旧 attempt/GC 的完成协议需落实 |
| 阶段退出与故障组合 | repair fence 的 SQL 冲突、有限生命周期反例 | job/TableInfo/cursor/manifest/fence 跨持久化边界及真正多节点故障证明 |

对 baseline 结果未知选择停住，可以保住安全性，但没有证明作业能前进或取消能结束；那是仍未闭合的可用性/恢复合同。还需核实真实 TiKV 的 UUID/重试/完成语义，不能发明一个现成 fence API，也不能以测试未复现来代替。

结论：可以继续做当前源码的定向原型，但现在不应把草案当作端到端正确性已闭合的设计去全面接线。先关闭读版本问题和导入完成协议，再讨论整体实现就绪。已证明的条件化引理、有限测试范围和未决合同应始终分开表述。
