# 历史可行性与候选取证

本文保留推演历史，不作为实施规范。当前结论见 [最终设计](../../design/2026-11-02-partition-global-sort.md)。

> 历史取证与候选分析，不按本文旧章节实施。唯一当前规格见 [最终设计](../../design/2026-11-02-partition-global-sort.md)，旧“读绑定/SST 完成未决”已改为历史读投影/独立写令牌和 unknown generation 不发布的确定约束；产品实施验收仍待进行。

# REORGANIZE PARTITION 接入 fast reorg：可行性与实施前分析

> 2026-11-02 最终范围与状态合同见 [正式设计](../../design/2026-11-02-partition-global-sort.md)。2026-11-03 第 15 节收敛保留 handle 的纯 split 协议（每个新分区唯一旧父，可以同时拆多个旧分区）；整数 clustered 路径与 MDL on 前提保留。下文 nonclustered 映射属于后续扩展，不是首版前置任务。完整数据基线仍全部 ingest，未改变为 index-only 交付。


- 日期：2026-09-15；2026-11-02 修订执行顺序与证明边界。
- 代码基线：`a49049a271fca7a1b4c2234fdafd6a1abd2d0645`。
- 状态：设计草案；未实现，未做正确性实验或性能测试。
- 范围：普通 `REORGANIZE PARTITION` 的完整 fast reorg，包括行数据、新分区 local index、全表替代 global index 的基线 ingest，以及在线增量协调、恢复和清理。
- 本文不是当前功能说明。2026-11-02 主方案为 Row → LocalIndex → GlobalIndex 串行构建，各索引组在行完整之后、自己的扫描之前才启用临时增量。详细状态表和正确性论证以 [ExecPlan](10-partition-fast-reorg-execplan.md) 为准。当前实现见 [分区 DDL](08-partition-ddl.md)，普通加索引机制见 [Add index](06-add-index.md)。后续实现前需重新核对代码基线。
- 协议细化与后续开发步骤见 [完整 fast reorg ExecPlan](10-partition-fast-reorg-execplan.md)。目前选择“行基线 ingest + 独立行增量 merge”作为候选主路线，尚未实验验证。

## 1. 结论

完整接入有可研究的基础：分区重组已经把旧/新行数据以及旧/新 global index 隔离到不同 key 空间，并且有在线切换状态机。索引部分可复用现有 ingest + temporary index merge，行数据部分需要先证明基线 ingest 与 DML 双写的 MVCC 协议。

进一步检查表明：非 clustered 删除会跳过尚不存在的新分区行，单靠较早的导入 TS 无法补足删除语义。候选主方案在基线导入期间将新布局行变化保存为独立 RowDelta，导入完成后通过事务 merge 建立完整行布局，再构建全部索引。固定旧 TS 路线作为对照研究，需要修改删除行为并证明 GC、重试及版本顺序。

Local index KV 的生成、排序和 ingest 已有 add-index fast reorg 能力可复用，不是需要重新设计的构建算法。存在多个普通 local index 不阻止当前分区重组，也不构成本方案的独立原理障碍；接入重点是目标 partition ID、索引列表和新旧分区的 DML 状态适配。首要待证明问题仍是 record KV 与并发 DML 的一致性、handle 冲突及恢复。

交付目标是三部分基线全部走 ingest：行搬迁、新分区 local index、替代 global index。仅接 global index、保留整表事务搬行或 local index 回填，不满足这个目标。实现可以按模块拆分提交，但完整路径才是验收单位。

“全部走 fast reorg”针对全量基线构建；正常用户 DML、元数据更新及必要的增量 merge 仍可以使用事务。这与现有 fast reorg 的含义一致，不要求消除整个 DDL 生命周期中的全部事务。

主要待解决问题是行版本顺序、handle 冲突、扫描源与目标映射、增量启用时点、作业阶段持久化、DXF 任务身份、唯一性检查和回滚/GC。目标同时覆盖大范围行搬迁与局部分区变更引发的全表 global index 重建成本；本次没有性能结论。

## 2. 已核实的当前实现

以下路径相对仓库根目录；Markdown 链接可直接打开源文件。行号可能随代码变化，优先按函数定位。

| 事实 | 代码依据 |
| --- | --- |
| 分区 DDL 未初始化 fast/dist 开关，只初始化普通 reorg 参数 | [`initJobReorgMetaFromVariables`](../../../pkg/ddl/reorg_util.go) |
| 分区重组强制事务模式 | [`doPartitionReorgWork`](../../../pkg/ddl/partition.go) 设置 `ReorgTypeTxn` |
| 首先搬行，再构建新分区索引，最后补未变更分区的 global index | [`reorgPartitionDataAndIndex`](../../../pkg/ddl/partition.go) |
| global index 分配新 index ID，通过 `DDLChangedIndex` 区分旧、新 | [`onReorganizePartition`](../../../pkg/ddl/partition.go) |
| 新旧分区使用不同索引集合，未变更分区保留两套索引 | [`newPartitionedTable`](../../../pkg/table/tables/partition.go) |
| 新 global index 目前的回填也走事务 | [`addTableIndex`](../../../pkg/ddl/index.go) 与 job 的 `ReorgTp` 决定执行路径 |
| fast reorg 的 DML 重定向由 `IndexInfo.BackfillState` 控制 | [`GenTempIdxKeyByState`](../../../pkg/table/tables/index.go) |
| fast reorg 已有 local index 的编码、排序与 ingest pipeline；local/global 可在加索引任务中共存 | [`buildLocalStorePipeline`、`buildExternalStorePipeline`](../../../pkg/ddl/backfilling_read_index.go)，以及下列 ingest 测试中的 local/global 组合 |
| ingest 流程已经支持普通 add-global-index | [`TestAddGlobalIndexInIngest`、`TestAddGlobalIndexInIngestWithUpdate`](../../../pkg/ddl/ingest/integration_test.go) |
| 非 clustered 删除新副本时，目标不存在则不写删除 | [`partitionedTable.RemoveRecord`](../../../pkg/table/tables/partition.go) |

### 2.1 新 table ID 的准确含义

普通分区重组为新分区分配新的 physical ID，逻辑 table ID 通常不变。

- 行数据大致位于 `t{partitionID}_r{handle}`。
- local index 大致位于 `t{partitionID}_i{indexID}...`。
- global index 使用逻辑 table ID 下的 index key 空间；通过新 index ID 与旧索引隔离。
- 分区方式变更涉及 `NewTableID` 的情况另有映射，不能直接套普通重组的假设。

这些只是便于解释的 key 结构。真实编码及不同 global index version 的 key/value 布局以 `tablecodec` 为准。

### 2.2 为什么已有 global index 也重建

当前代码要求旧、新 schema 同时存在期间，分别能访问指向旧、新分区的索引映射。一个 unique index key 的单个当前 value 无法直接供两套布局共同使用，因此实现选择新 index ID。

由此产生扫描放大：受影响分区只占全表一小部分时，未受影响分区的行也要加入替代 global index。`TestReorgPartitionGlobalIndexNonTouchedAfterDropped` 明确覆盖未变更分区位于被重组分区之后的情况。

保留此设计可把范围控制在回填加速。global index 原地更新属于另一个多版本读写协议设计，不适合同时做。

## 3. 完整方案与交付边界

| 方案 | 潜在收益 | 主要代价 | 判断 |
| --- | --- | --- | --- |
| 行与两类索引的基线全部 ingest | 同时降低行搬迁及索引构建的事务成本 | 行 MVCC、增量、handle 冲突、阶段与恢复 | 本文的交付目标 |
| 仅索引或仅 global index 接 ingest | 降低部分索引构建成本 | 仍保留大量事务搬行 | 不作为交付方案 |
| global index 原地改 partition 映射 | 可能避免未变更分区的索引重建 | 改变旧、新 schema 共存协议 | 不与本方案捆绑 |

“global index”和“global sort”是不同维度：前者是索引跨分区的逻辑性质，后者是 ingest 使用对象存储等完成排序的执行方式。global index 并不等于必须使用 global sort。

## 4. 建议执行流程

### 4.1 元数据准备与增量启用

在新分区与替代索引开始接受 DML 之前，确定完整回填协议并持久化。索引 fast 路径初始化 `BackfillStateRunning`，行数据则需要相应的基线版本规则或增量捕获协议。通过 `DeleteOnly → WriteOnly → WriteReorganization` 的 schema 发布流程建立一致的 DML 行为。

候选主方案中，正常 DML 继续维护旧数据和旧索引，同时在同一事务内维护 RowDelta；新索引 temporary index 延迟到各自索引阶段准备时启用。Capture 阶段不写新布局普通 record，避免与基线 importer 竞争；Delete 保存显式删除意图，即使目标尚不存在也不能跳过。旧布局承担原有查询与约束；新布局仍不可读。行协议尚未验证，不能仅设置索引状态就视为完成。

此处是方案建议，尚未证明可以只增加一次字段赋值完成。需要验证每种物理分区构造出来的索引实例保留正确的 `BackfillState`，以及插入、删除、更新路径都采用相同路由规则。

### 4.2 行基线 ingest

扫描 `DroppingDefinitions`，按新的分区表达式路由，生成 `AddingDefinitions` 下的 record KV，排序并 ingest。全量行搬迁不再调用旧事务 worker 逐批提交。

现有 worker 的旧行冲突保护、目标行存在性判断和重复 `_tidb_rowid` 处理是新方案需要保持的语义，不是可以直接删除的冗余步骤。它们应由版本协议、稳定的 handle 映射和必要的冲突处理替代。第 10 节列出待证明的行协议。

此阶段完成条件包括：基线已导入，行级并发变更已按所选协议正确反映在新布局中，handle 映射稳定，并持久化可恢复的完成标记。不能仅以 SST 写出或 owner 的内存状态判断完成。

具体顺序为 record 基线全部导入并 fence 旧 importer，发布行 DualWriteReady，再推进并同步 DualWriteMerging，通过事务合并 RowDelta，最后进入 Direct。merge 事务读取 delta、写目标 record、清理 delta，并与并发 DML 对同一 delta key 的写入冲突重试。细节与正确性前提见 ExecPlan。

### 4.3 全部索引基线 ingest

先完成 RowMerge 并发布 Direct，使新分区行完整且持续维护，再单独启动 local index 构建，最后单独启动替代 global index 构建。每组在扫描之前发布自己的 DeleteOnly/WriteOnly/BackfillState 协议并排除未捕获的旧事务，沿用已有 global sort 编码、排序、导入、重复检测与临时索引 merge。第一版接受额外扫描，record/local 同次生成只保留为后续优化候选。

行阶段写目标 record 的内部路径不调用普通 AddRecord 的不完整索引检查；旧布局负责正常 SQL 约束。Local 完成前不能将新分区发布给 SQL。新旧布局上的同 ID local index 实例分别管理状态，不能统一设置 Running。

### 4.4 扫描与输出集合

| 阶段 | 扫描源 | 输出 |
| --- | --- | --- |
| RowBuild | DroppingDefinitions 的固定快照 S | AddingDefinitions 的 record KV |
| LocalBuild | 已完整的 AddingDefinitions | 全部相关 local index KV，排除 record 形式的 clustered primary |
| GlobalBuild | AddingDefinitions ∪ (Definitions − DroppingDefinitions)，每个 PID 一次 | 替代 global index KV |

Local 与 Global 分组使用稳定且不同的 DXF task identity，各任务有固定的 index IDs。Global 必须统一覆盖最终布局，不能同时扫描旧副本和新副本，也不能遗漏未变更分区。新分区 global entry 使用新 PID，未变更分区使用原 PID。

### 4.5 增量 merge 与切读

ingest 完成后推进：

```text
Running → ReadyToMerge → Merging → Inapplicable
```

每个需要改变 DML 行为的转换沿用 schema 版本发布与同步机制。merge 期间正常 DML 双写目标索引和临时索引，后台用事务合并需要补齐的旧增量。

只有行数据、local index、新 global index 的 ingest 和所有必要增量处理都完成，才执行现有 `WriteReorganization → DeleteReorganization` 切换。原有新旧索引可见性、分区双写退出和 GC 边界作为兼容性约束保留，内部协议的改变需逐项验证。

“merge 完成”不要求临时 key 空间物理上完全为空：当前 worker 会跳过已由双写覆盖的临时记录，剩余垃圾还需要 GC。不要新增一个“临时索引必须完全空”的完成条件。

## 5. 快照与时间戳：不能直接照搬抽象图

“取得一个全表快照，然后 ingest”只能作为概念描述，不能视为当前代码事实。

[`tableScanWorker.scanRecords`](../../../pkg/ddl/backfilling_operators.go) 使用 `wrapInBeginRollback` 给扫描任务获取读事务 `startTS`，然后传入 `buildTableScan`。不能默认所有分区、所有 task 共用 `job.SnapshotVer`。

另外：

- `getReorgInfo` 使用 `job.SnapshotVer` 初始化/恢复 reorg 信息，并确定扫描范围。
- `BackfillSubTaskMeta.TS` 与导入幂等性有关，不能当成上述扫描事务时间戳。
- local ingest checkpoint 保存导入 TS，并在 import 后推进 watermark 和下一次 TS。

索引阶段应尽量继承现有 add-index 协议；行阶段不能直接复用这些扫描/导入 TS 规则，因为目标 record KV 已有 DML 双写。两个阶段各自的版本协议均需要证明。完整流程要求的逻辑先后关系是：

```text
行版本/增量协议生效且未捕获事务退出
    → 扫描旧分区并 ingest 新 record KV
    → 行布局正确且阶段完成已持久化
    → 启用 local 临时增量，扫描新分区并 ingest/merge local index
    → 启用 global 临时增量，扫描最终布局并 ingest/merge global index
    → 所有基线 ingest 完成
    → 发布并同步 merge 写入协议
    → merge 完成
    → 切读
```

如果行协议或跨分区唯一性检测需要统一快照，再评估固定快照方案及其 GC safepoint、长任务、重试成本。扫描时间戳、行导入版本、索引导入版本和增量截止边界分别明确，不能简单共用一个 TS 字段。

## 6. Global index 正确性重点

### 6.1 两份行副本不等于两条逻辑行

设旧分区 O 中有行 h，搬迁到新分区 N。重组期间 O:h 与 N:h 同时存在。新 global index 只能以 N:h 为这一行的目标。

在串行方案的索引阶段，如果直接使用旧 `Definitions` 扫描而不做路由转换，会生成 O:h 的映射；如果同时扫描 O 和 N，又可能生成重复索引输入。如果采用融合扫描，则从 O 读取是合法的，但编码前需要显式转成 N 和对应目标 handle。目标集合或映射错误不能依赖增量 merge 自动修复。

### 6.2 未变更分区仍是在线数据源

不能只为 AddingDefinitions 的 DML 记录增量。未变更分区也会更新新 global index；insert、delete、更新唯一列、与被重组范围之间移动都需要覆盖。

当前 `newPartitionedTable` 为未变更分区保留原始索引集合，提供了新旧索引同时维护的基础。接入时应验证过滤索引列表的修改没有破坏它。

### 6.3 跨分区移动与唯一键复用是首要原型实验

建议用 failpoint 构造以下确定性时序：

1. 扫描源分区，读到 `(uniqueKey=u, partition=N1, handle=h1)`。
2. DML 将该行移动至 N2，或者删除后由另一行复用 u。
3. 扫描目标分区，读到 u 对应的另一个 partition/handle。
4. 进入 duplicate detection 和临时索引 merge。

判断依据是 SQL 层没有出现非法的同时重复，DDL 不应因中间扫描版本误报，也不能在成功后指向旧位置。

现有普通 add-global-index 已有并发 update 测试，说明基础机制值得复用；但上述精确交错是否被完整覆盖，本次未验证。特别需要分别检查 local sort 的重复检测和 global sort 的各阶段检测。不要从“旧 unique index 会阻止重复”推导出“新索引输入绝不会出现跨时间版本重复”。

### 6.4 PartitionHandle 已有支持，仍需端到端验证

已检查的链路：

- `TempIndexValueElem` 的 global unique delete 可保存 partition ID。
- `DecodeHandleInIndexValue` 可返回 `kv.PartitionHandle`。
- `PartitionHandle.Equal` 在双方都是 partition handle 时同时比较 partition ID 和内部 handle。
- `checkTempIndexKey` 删除保护会检查被引用行是否存在。
- `EncodeRecordKey` 接到 partition handle 时使用其中的 partition ID，而非机械使用逻辑 table 的 record prefix。

因此不能仅看到 merge 使用父表对象，就认定它无法定位分区行。真正要验证的是各编码版本和读写分支都保留了 partition handle，特别是相同 `_tidb_rowid` 出现在不同分区时。

### 6.5 覆盖 index version、NULL 与 handle 形式

替代索引创建时调用 `setGlobalIndexVersion`，因此不能假设新旧索引编码版本相同。测试按当前代码允许的组合覆盖 legacy/V1/V2、unique/non-unique、clustered/non-clustered、common handle，以及 unique index 的 NULL 非 distinct 分支。

旧 global index 约束只有在索引语义保持一致时才能直接沿用。LOCAL/GLOBAL 转换、分区方式变更应单独审查；本文以普通 REORGANIZE 为主，完整行/索引 ingest 不代表已经证明所有共享 handler 的 SQL 变体。

## 7. DDL 与 ingest 之间需要的接口边界

### 7.1 完整协议与阶段选择

删除 `job.ReorgMeta.ReorgTp = ReorgTypeTxn` 并改为 Ingest 不足以完成接入。需要行基线 ingest、索引基线 ingest 和增量协调的完整执行分支，既有 index-only pipeline 不会自动处理 record KV。

建议显式持久化分区重组的执行阶段，同时固定行与索引协议。实际实现可以复用或扩展现有元数据，但需避免多个阶段反复修改同一个全局 `ReorgTp`，使恢复后的分支含义不明确。

新增字段缺失时解释为原有事务路径。运行中的旧 job 不因当前全局开关改变而迁移执行协议。

完整 fast 路径的能力检查在发布新协议前完成。已经进入新协议或写入部分 SST 后，不能直接改回传统回填。此时应按所选协议恢复、暂停或回滚。现有 add-index 的 `TxnMerge` fallback 不足以代表完整分区 fast reorg；原有关闭 fast 的事务路径可以作为兼容行为保留，但不能把部分基线仍走事务的 job 计为完整 fast 路径成功。

能力判断还要明确各类索引、multi-schema 子作业和目标部署模式的支持范围。任何未覆盖组合都应显式报告其能力边界，不能静默跳过行 ingest 或 local index ingest，仍声称完整加速。

### 7.2 扫描计划与元数据重建

[`generateReadIndexPlan`](../../../pkg/ddl/backfilling_dist_scheduler.go) 直接遍历当前 `Partition.Definitions`；非 DXF 的 reorg 初始化和分区遍历也需要逐项核对。

[`getUserTableFromTaskStore`](../../../pkg/ddl/backfilling_dist_scheduler.go) 会重新从元数据取表，executor 又按 subtask physical ID 获取物理分区。只改变 owner 手中的 table clone 不足以改变远端扫描计划。

建议由分区 DDL 生成明确的目标 partition IDs 和 index IDs，持久化到任务计划或可稳定重建的版本化元数据中。调度、executor、重试、merge 都校验目标集合，防止从不同阶段的 `Definitions` 推导出不同结果。

`GetReorganizedPartitionedTable` 只使用 AddingDefinitions，不包含未变更分区，不能直接充当完整 global index 的扫描视图。

### 7.3 DXF task key

当前 `TaskKeyBuilder` 主要以 job ID、multi-schema 序号、是否 merge 组成 task key，没有按本方案的扫描阶段或索引组区分。

如果照搬现有两轮 global 回填，第一次 DXF task 成功后，第二次使用同一 task key，可能命中历史成功任务并直接返回。这是从调用路径推导出的接入风险，尚未实际构造失败。

完整方案至少有行构建与索引构建的身份区别；分多个 ingest 组时也需要持久化且稳定的阶段/组标识，并同步适配查找、恢复、暂停、取消、行数统计和清理，不能只修改提交时的字符串。

### 7.4 错误处理属于分区 DDL

`runReorgJobAndHandleErr` 和 `runIngestReorgJob` 内部含有 `convertAddIdxJob2RollbackJob`。直接复用整个函数会把分区 job 带入不匹配的回滚逻辑。

建议抽取扫描、ingest、merge 的公共执行能力；分区 handler 负责解释重试、暂停、取消及转换到自己的 rollback。不要伪造一个独立 add-index job 来绕过参数类型和状态机。

### 7.5 Local index：复用已有能力，适配分区上下文

本节列的是接入适配项，不是 local index ingest 的新算法或独立可行性障碍。复用现有 local index KV 编码、排序、ingest 和临时索引 merge；新分区上的所有相关 local index 都包含在完整交付中。

同一个 local index ID 在旧分区、新分区和未变更分区有不同有效状态。新分区依赖克隆的 `IndexInfo` 跟随 `Partition.DDLState`。

Local index 的新旧 key 空间由 partition ID 隔离，可以复用 index ID。例如 `t{oldPID}_i{idxID}` 与 `t{newPID}_i{idxID}` 是不同的索引空间；未变更分区的 local index 不重建。

不能为全部 local index 实例统一启用 Running。需要确保旧分区/未变更分区仍保持完整索引与既有 DML 语义，只对新分区使用临时索引协议。

如果把 local 与 global 一次扫描并行编码，还需要“每个 physical partition 应生成哪些 index IDs”的映射：未变更分区只构建新 global index，新分区才构建两类索引。现有执行器的索引集合传递需要检查是否支持这种差异。

## 8. Checkpoint、恢复与兼容性

建议持久化的逻辑信息如下，名称是设计概念，不是已存在的 API：

| 信息 | 用途 |
| --- | --- |
| 协议版本与选定模式 | 新旧 job 兼容，防止中途改模式 |
| 当前阶段 | 区分行 ingest、行增量协调、索引 ingest、索引 merge、切读 |
| 目标 partition/index 集合或稳定重建依据 | 恢复后使用相同计划 |
| 行导入版本与 handle 映射 | 防止覆盖 DML、恢复时改变目标行身份 |
| 构建任务身份 | 区分任务组，正确找到运行中及历史 DXF task |
| 每阶段 checkpoint / import TS | 知道哪些数据已持久化，安全重复执行 |
| 清理所需 index IDs、temp IDs、目标 table ID | 完成和回滚后清理完整 |

当前 ingest checkpoint 区分本地 flush 与已 import 的 watermark。本地文件失效时不能把 flushed 当作已写入 TiKV。DXF 与非 DXF 使用不同存储策略，不能把一条 reorg handle 当成全部 ingest 进度。

`NormalCheckpointStorage` 的存取按 job ID 和 index element type 组织，另校验 physical ID；混合阶段/多个索引组复用时还要检查是否会读到前一阶段的 checkpoint。

每个跨存储的阶段边界都需要幂等恢复顺序，而不是假定 DDL job metadata、reorg table 和 DXF task 能在一笔事务中一起提交。例如 DXF 已成功、DDL 阶段尚未推进时，应识别同一已完成任务再推进，不能重新分配一套不相关的 ingest 身份。

混合版本方面，新增 JSON 字段可解码并不意味着旧 owner 能执行新协议。启用条件需要结合实际发布版本、集群升级流程和旧节点接管能力审查。本次未验证这些兼容条件。

## 9. 回滚与清理

完整阶段矩阵、abort 子阶段和异常分类见 [ExecPlan](10-partition-fast-reorg-execplan.md) 的 Rollback protocol and failure matrix。回滚保留用户已提交的 DML，恢复旧布局，不回到 DDL 开始时的数据快照。

已核对实际边界：`Job.IsRollbackable` 和 `onRollbackReorganizePartition` 在 StatePublic 拒绝回退；DeleteReorganization 虽已切读，转换逻辑仍可恢复旧分区。这要求 fast 路径在该窗口继续正确反向维护旧行和旧索引，保留 handle 映射。Public 后发生异常则前向恢复。

成功后的清理目标：旧分区、旧 global index、替代 global index 的临时空间，以及 ingest 本地/对象存储中可删除的作业产物。

回滚后的清理目标：新分区、新 global index 及其临时空间；旧数据和旧索引继续保持可用。清理空间的 table ID 要根据实际目标编码确定。

当前 `delete_range.go` 的分区 DDL 分支没有普通 add-index 分支那样的 `TempIndexPrefix | indexID` 清理项，需要扩展 finished args/清理逻辑或采用等价的持久化清理描述。

取消还涉及停止 DXF executor 和 ingest 写入。停止之后经过必要的 schema 同步再清理，避免 DeleteRange 完成后仍有后台写入。

`CancelTask` 只发出任务取消操作，并未在该函数内等待所有存储请求完成。已经发送但结果未知的 ingest RPC 单独处理；发送前校验 generation 不足以撤回它。没有明确 drain/fence 证据时不清理目标、不宣告 rollback 完成。回滚本身通过持久化子阶段恢复，不能因为清理遇到错误就丢失 job 的资源 ownership。

异常矩阵涵盖事务/网络重试、owner/executor 故障、pause/cancel、磁盘满、快照/GC 保护丢失、对象文件丢失、PD/label/TiFlash 配置失败、切读元数据提交结果未知和 GC 登记失败。已进入 rollback 的 job 由新 owner 继续 rollback，不重启正向构建；不可回退点后保留新布局并完成收尾。

`convertReorgPartitionJob2RollbackJob` 当前有按 `Unique` 筛选的索引处理。支持 non-unique global index 时需要端到端核对这一分支与其后续清理，不能直接认为所有替代索引均已覆盖；本次不将此观察认定为已复现缺陷。

不扩大现有的可取消/可回滚窗口。切读后的阶段限制以实际 handler 和取消校验为准。

`JobNeedGC` 会排除 Cancelled job，因此新协议分配过资源后不能简单沿用直接标记 Cancelled 的错误出口。需要保证清理描述先可靠登记，逻辑回滚完成与物理 GC 完成分别观察；本次不据此推定旧路径存在已复现缺陷。

## 10. 行数据 ingest：完整方案的首要设计问题

新 physical ID 解决了新旧数据空间隔离，但新分区在回填期间已接受 DML 双写，不是无并发写入的空目标。

候选主路线改为显式行增量，较早 MVCC 基线版本保留为对照路线。原因是现有非 clustered 删除在新目标不存在时不写 tombstone：扫描读到行、随后删除、最后才导入基线时，仅调低导入 TS 仍会留下已删除的行。以下两种协议本次均未实测：

1. 主路线：新行空间采用固定源快照基线加独立 RowDelta，在基线导完且 importer 已 fence 后发布双写并合并。RowDelta 保存目标行完整值或显式 Delete，和用户 DML 同事务；merge 通过同一 delta key 的事务冲突处理并发覆盖。现有临时索引不能直接充当这种行记录。
2. 对照路线：使用严格受控的 MVCC 导入时间戳，使基线与新分区正常事务写入有明确版本顺序。需要补齐不存在目标的删除标记，同时证明快照选择、GC、分区迁移、重复 ingest 和新旧 schema 的读写可见性。底层接受显式 import TS 不等于协议自动成立。

非 clustered 表还可能因历史 EXCHANGE 产生相同 handle。当前事务 worker 可以读冲突目标并重新分配 row ID；SST 生成要维持相同语义，需要额外协议。

具体需要证明的时序：扫描到 v1 后，DML 已把新分区更新成 v2 或删除该行，随后才导入 v1，最终仍只能看到 v2 或删除状态。导入版本与所有相关 DML 提交版本的顺序、初始化窗口中的旧 schema 事务、重试换 TS、GC safepoint 都在证明范围内。

如果使用独立行增量，还要保证捕获、持久化、回放和停止捕获的边界，不漏行也不重复制造行身份。行增量表达路由、旧/新 handle 和删除语义，不能仅复用 temporary index value。

handle 冲突解决不能依赖无序 SST 的 last-write-wins。需要稳定的源 `(partitionID, handle)` 到目标 `(partitionID, handle)` 映射，或等价的可恢复冲突解决机制；DML、record KV、local/global index KV 和回滚都使用一致身份。不能为了规避这个问题保留整批数据的事务搬迁。

ExecPlan 给出非 clustered 的正向映射、目标归属预留与切读后的反向映射原型。它可能产生 O(N) 映射元数据写入，应测量并优化批量持久化；不能因这些不是 record KV 就忽略其事务成本。若映射成本抵消 ingest 收益，继续修正完整协议，不改为部分索引交付。

## 11. 实验与测试计划

这是后续验证计划，本次未执行。

### 11.1 先做完整路径的确定性原型实验

1. 行基线扫描后分别执行 update/delete，再延迟 ingest，验证目标行版本不会回退、已删除行不会复活；覆盖初始化 schema 窗口与重试。
2. 合并含重复 row ID 的旧分区，验证 record/local/global 输出与并发 DML 使用一致且可恢复的目标 handle。
3. 验证新分区 local/global 与未变更分区 global 的临时索引路由，旧索引查询与唯一性保持。
4. 固定“先扫描源、再跨分区移动、后扫描目标”的时序，检查 local sort/global sort 的重复检测与最终索引。
5. 分别在 record SST 已导入、index SST 部分导入、任务成功但 DDL 未推进时切换 owner，验证统一完成条件和恢复身份。

这些实验比先跑大表 benchmark 更适合决定完整设计是否成立。只有 index-only 原型通过，不能据此宣布完整方案可用。

### 11.2 功能测试矩阵

| 维度 | 关键用例 |
| --- | --- |
| 分区布局 | split、merge、RANGE/LIST 合法重组、全分区重组、空受影响分区但非空未变更分区 |
| 集合顺序 | 未变更分区在重组范围之前/之后；多个新分区；不遗漏或重复 physical ID |
| 索引 | 多个 global index、unique/non-unique、NULL、支持的编码版本、与 local index 共存 |
| Handle | int/common clustered handle、隐式 row ID、历史 EXCHANGE 产生的重复 row ID |
| DML | 各阶段 insert/delete/update、唯一列变化、唯一键复用、跨新分区移动、受影响与未变更范围之间移动 |
| 多节点 | 相邻 schema 版本交错、旧 schema 长事务、切读前后查询与约束 |
| 恢复 | 行部分导入、行增量协调、索引部分导入、任务成功未推进、ReadyToMerge、merge 中途的 owner/executor 故障 |
| 生命周期 | pause/resume、cancel、重试耗尽、本地文件丢失、磁盘满、成功/回滚 GC |
| 开关兼容 | fast 关闭、ingest 不可用的预先选择、旧 job 恢复、multi-schema 支持范围 |

SQL 结果检查结合 `ADMIN CHECK TABLE`、确认实际执行计划的索引扫描对比、重复键拒绝，以及目标 index KV 的 partition ID 检查。仅比较行数无法发现错误指向和孤儿索引。

可复用的测试入口：

- [`pkg/ddl/tests/partition/reorg_partition_test.go`](../../../pkg/ddl/tests/partition/reorg_partition_test.go)：重组失败与未变更分区 global 回填。
- [`pkg/ddl/tests/partition/multi_domain_test.go`](../../../pkg/ddl/tests/partition/multi_domain_test.go)：多 schema/DML 交错。
- [`pkg/ddl/ingest/integration_test.go`](../../../pkg/ddl/ingest/integration_test.go)：global index ingest、并发 update、checkpoint、cancel。
- [`pkg/ddl/tests/partition/global_index_version_test.go`](../../../pkg/ddl/tests/partition/global_index_version_test.go)：global index 版本与 handle 边界。
- [`pkg/ddl/backfilling_dist_scheduler_test.go`](../../../pkg/ddl/backfilling_dist_scheduler_test.go)：任务计划。
- [`pkg/ddl/ingest/checkpoint_test.go`](../../../pkg/ddl/ingest/checkpoint_test.go)：checkpoint。

mock ingest 可以验证状态和编码，真实 SST 导入、MVCC、故障恢复与性能需要 RealTiKV 环境。具体命令遵循 [testing-flow](../testing-flow.md) 和仓库验证规则，不能把 mock 通过当成存储协议证明。

## 12. 性能评估

粗略模型：

```text
旧耗时 ≈ Trow_txn + Tlocal_txn + Tglobal_txn + Tschema
新耗时 ≈ Trow_scan_sort_ingest + Tidentity_map + Trow_delta
        + Tindex_scan_sort_ingest + Tindex_merge + Tschema_extra
```

若采用融合扫描，行与索引的部分扫描/编码成本可以共享，以上按串行阶段相加的模型需要调整。完整方案不存在“行搬迁仍固定走事务”带来的那项收益上限；实际收益取决于读取、排序、网络、TiKV 导入/compaction 及增量成本。

优先比较同一张表上无 global、一个 global、多个 global，并固定受影响比例。再变化受影响比例（如 1%、10%、100%）、行宽、索引宽度、前台写入速率、local/global sort。

除总时间外，至少记录：行搬迁时间、各类索引扫描/排序/导入时间、merge 时间、扫描/写入字节、事务重试、TiKV 写入与 compaction 压力、临时空间峰值、前台延迟。

主方案把 local/global 的临时索引捕获推迟到各自扫描前，避免搬行期间积累 temp index。RowDelta 与 handle 映射仍有写入和空间成本；整个构建过程很长时，这些成本仍可能抵消部分收益。用户观察到几百 GB 的行重组耗时数小时、同量级 ADD INDEX 可在一小时内完成，支持优化动机，但尚非本次同配置 benchmark。

当前分区 reorg 的进度估计偏向受影响分区的数据量；新 global index 扫全表后，需要按阶段报告工作量，避免进度百分比和吞吐指标误导使用者。

## 13. 建议实施拆分与进入条件

1. 先完成第 11.1 节的确定性实验，优先证明行版本与 handle 协议，同时验证索引增量、唯一性和恢复。
2. 固化完整 job 协议：源/目标集合、行/索引阶段、时间戳、任务身份、checkpoint、统一切读条件。
3. 实现 record KV ingest，并为新分区 local index、全表替代 global index 接入已有索引 pipeline 和增量协调能力。Local index 以分区上下文适配为主。可分提交，但不以其中一部分作为完整功能交付。
4. 补齐跨阶段恢复、pause/cancel、回滚、行/索引/temp 数据与对象存储清理。
5. 在目标部署配置下验证真实 TiKV、DXF、相应排序路径与性能；通过完整行/索引验收后才能宣称该 DDL 接入 fast reorg。

验收需要执行证据确认 record/local/global 三类全量基线均通过 ingest，没有静默退回整批事务回填。元数据事务和必要增量事务不属于此限制。

共享 handler 的其他 SQL 变体（分区方式变更、LOCAL/GLOBAL 转换、NewTableID）另列兼容矩阵；不据普通 REORGANIZE 的成功自动宣称支持。行 ingest 本身已属于本方案，不再作为后续可选扩展。

后续实施按已建立的 [ExecPlan](10-partition-fast-reorg-execplan.md) 继续维护，先完成原型 gate，再落地完整路径。本文不承诺开发工期，也不代表上述方案已获正确性验证。

## 14. 本次分析验证记录

- 文档产物包括本文、完整协议 ExecPlan 及 DDL 索引入口；无 Go/Bazel/运行时修改。
- 验证范围采用 `tidb-verify-profile` 的 Ready 文档范围，检查引用、格式和政策边界；没有代码变更，不运行 lint、编译或 package tests。
- 已阅读 `docs/agents/agents-review-guide.md`：本文描述设计建议，不新增 agent 政策；现状文档与提议明确分开。
- 精确的关键取证命令（均从仓库根目录运行）：

```bash
git rev-parse HEAD
sed -n '514,656p' pkg/ddl/ingest/integration_test.go
sed -n '1261,1335p' pkg/ddl/tests/partition/reorg_partition_test.go
sed -n '530,610p' pkg/ddl/backfilling_operators.go
sed -n '1078,1125p' pkg/tablecodec/tablecodec.go
sed -n '120,143p' pkg/tablecodec/tablecodec.go
sed -n '710,750p' pkg/kv/key.go
sed -n '900,1018p' pkg/ddl/reorg.go
sed -n '332,408p' pkg/ddl/backfilling_dist_scheduler.go
sed -n '400,452p' pkg/ddl/ingest/checkpoint.go
sed -n '3384,3422p' pkg/ddl/index.go
```

- 尚未验证：实现可编译性、精确并发时序、真实 ingest、混合版本接管、取消/恢复/GC 实验、性能收益。

文档格式与引用检查命令：

```bash
git diff --check -- docs/agents/ddl/README.md
git diff --no-index --check /dev/null docs/agents/ddl/09-partition-fast-reorg-feasibility.md
git diff --no-index --check /dev/null docs/agents/ddl/10-partition-fast-reorg-execplan.md
python3 - <<'PY'
from pathlib import Path
import re
files = [Path('docs/agents/ddl/README.md'), Path('docs/agents/ddl/09-partition-fast-reorg-feasibility.md'), Path('docs/agents/ddl/10-partition-fast-reorg-execplan.md')]
count = 0
for p in files:
    data = p.read_text()
    assert len(re.findall(r'^```', data, re.M)) % 2 == 0, f'unbalanced code fences: {p}'
    for target in re.findall(r'\]\(([^)]+)\)', data):
        if '://' in target or target.startswith('#'):
            continue
        resolved = p.parent / target.split('#')[0]
        assert resolved.exists(), f'broken link: {p}: {target}'
        count += 1
plan = files[-1].read_text()
for heading in ['Progress', 'Surprises & Discoveries', 'Decision Log', 'Outcomes & Retrospective', 'Concrete Steps', 'Validation and Acceptance', 'Idempotence and Recovery', 'Interfaces and Dependencies']:
    assert f'## {heading}\n' in plan, f'missing section: {heading}'
assert '```' not in plan, 'ExecPlan must use indented code blocks'
lines = plan.splitlines()
for i, line in enumerate(lines):
    if line.startswith('#'):
        assert lines[i+1:i+3] == ['', ''], f'heading spacing at line {i+1}'
print(f'PASS: {count} local Markdown links across {len(files)} files; ExecPlan structure and heading spacing')
PY
```

2026-11-02 补充验证：运行 `python3 docs/agents/ddl/models/partition_row_delta.py`，344 个状态、23 个终态行一致；运行同命令加 `--unsafe`，移除 Capture drain 后得到漏行反例，预期退出码 1。模型范围与假设见 ExecPlan，不代表真实 TiKV、索引或恢复已验证。

2026-11-03 审查修正：并非所有 nonclustered reorg 都需要映射。单父分区拆分可由 parent/h 证明正向单射，反向窗口需在同一事务检查旧父 record 占用并为自动 hidden handle 重试。正式设计第 14 节优先于此前通用映射讨论；一般合并与跨旧边界重划仍走 txn。

2026-11-03 temp row 再简化：完整 Put/Delete 统一事务重放即可，不需要已应用标记。RowBackfillState 复用现有 BackfillState，且与索引状态分开。模型 --suite --replay-all 通过；只针对 record，不改成熟 temp index 协议。

2026-11-03 多分区拆分澄清：多个旧分区可以分别细分，只要每个新分区范围只属于一个旧父。保留全部旧边界与整体范围，比“只选一个旧分区”更准确；跨旧边界重划仍不能套用此无映射证明。见正式设计第 14.8 节。

2026-11-03 已确认的新初始化反例：旧 schema 的未捕获提交 v2 可晚于新 schema 捕获 v1，而 S 读到 v2；重放 temp=v1 会回退。第 15 节把当前行协议改为 DirtyRows 仅存目标键，repair 点读 parent(q),h 的当前源并检查新路由；S、Running drain、marker 同事务写入及 range fence 仍必要。原完整值 replay 的正向数字不覆盖这个窗口，startup 负向模型和本地 SQL 探针已复现；不是当前 TiDB 产品 bug。

2026-11-03 正确性状态修正：条件化行集收敛不足以证明 SQL 读正确。旧 binary 上 RR 在拆分后首次访问目标表出现 0/4 行观察；当前源码未确认。正式设计第 16 节列出 readTS/布局匹配、完整 mutation 覆盖、index 适配等价性和 SST 完成协议的未闭合义务，不能宣称端到端设计已证明。
