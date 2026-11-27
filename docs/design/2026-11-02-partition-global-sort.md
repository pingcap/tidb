# REORGANIZE PARTITION：DXF/global sort 最终设计与首版约束

版本：v1，2026-11-03。状态：最终实施规格；产品执行路径尚未实现，不能据此宣称已上线或当前源码已通过集成测试。TiDB 取证基线为 `44facb0f374a16d39d5a7267714a9c5d0d7f17a4`。本设计取代[历史审查记录](2026-11-03-partition-global-sort-review-notes.md)中的中间候选；[ExecPlan](../agents/ddl/10-partition-fast-reorg-execplan.md)按本规格推进。

## 1. 最终选择

首版只加速 RANGE 的纯拆分：可以同时拆多个相邻旧分区，但保留全部旧边界和整体覆盖范围，每个新分区恰好有一个旧父分区。搬迁保留 handle，不建立逐行正反映射。

执行顺序固定为：

```text
判定纯拆分、持久计划
  → 启用同事务脏键捕获并等待 MDL
  → 固定 S 扫描旧分区，DXF/global sort/ingest record
  → 同事务点读当前旧源，修复 DirtyRows
  → global sort 构建/merge 新分区 local index
  → global sort 构建/merge 全表替代 global index
  → 封存全部后台写入，切换可见布局
  → 排空旧写令牌，退休旧布局，按 GC 规则清理
```

三个全量基线均 ingest；正常 DML、脏键修复、元数据和临时索引 merge 使用事务。首版不融合 record/local 基线。完整值 temp replay 已因初始化反例否定，只保留脏目标键并点读当前源。

两个关键缺口采用确定方案：**读取按有效 readTS 绑定对应历史布局，写入使用独立的当前写布局；结果不确定的导入使整个未发布 generation 永久失去发布资格，改用全新命名空间重建。** 不依赖一个尚不存在的 TiKV range fence API。

## 2. 首版支持范围与约束

| 维度 | 首版合同 |
| --- | --- |
| SQL 形状 | 普通 RANGE REORGANIZE 的纯细分；多个旧分区必须相邻，旧内边界和外范围不变 |
| Handle | 整数 clustered PK 或隐式 `_tidb_rowid`，包括按已有规则分配的 sharded row ID；common handle 暂不启用 |
| 分区规则 | 同一确定性规则，明确覆盖整数 RANGE、单列整数 RANGE COLUMNS、TO_DAYS(DATETIME)；上下文敏感且不能证明一致的表达式不启用 |
| 索引 | 已有 fast add-index 支持的普通列 BTREE local/global，unique/nonunique/NULL；前缀、表达式、multi-valued、vector/columnar 暂不启用 |
| 表与 DDL | 不做合并、跨旧边界重划、范围扩缩、REMOVE/ALTER PARTITIONING、multi-schema；生成列、外键、TTL、TiFlash/table cache 组合暂不开放 |
| 事务 | MDL 全程开启；RR/RC 按第 4 节绑定读写视图；旧 RR 快照与首次写入布局不匹配时显式要求整笔重试 |
| DML | 普通事务 mutation hook 必须覆盖所有变更；显式写 `_tidb_rowid`、绕过 hook 的 IMPORT/restore/外部 SST 与未验证 pipelined 写入在同表作业期间拒绝/等待 |
| 版本 | 所有 TiDB 先部署读绑定和新协议，再允许创建 fast 作业；混合旧版本/降级不启用；同一 keyspace 的 Classic 路径，其他内核/引擎未认证不启用 |
| 存储 | 只支持已认证“成功应答在 apply 之后”的 ingest；不得隐藏重试不确定的 mutating RPC |
| 暂停与故障 | 可软暂停并让在途 RPC 完成；未知结果/失联导入 attempt 可能导致整代重建，不承诺保留该代导入进度 |
| 空间 | 有显式 active/quarantine 容量预算；不确定旧代可长期 CleanupPending，达到预算停止 fast admission，旧 txn 路径仍可用 |
| 外部组件 | CDC/备份/PITR 及不同部署组合未经适配验收不宣称支持；需要可执行互斥/能力控制，不能只靠操作手册 |

不适用时在启用前选择完整旧 txn 路径并记录原因；不把合法 REORGANIZE 语法变成不合法。启用后不静默混回旧搬行器。表/读绑定/存储能力未部署齐全时也走旧路径。

用户观察几百 GB 的行重组需数小时、同量级 ADD INDEX 可在一小时内完成，是优化动机，不是本次同配置 benchmark。首版允许额外扫描、源点读和故障重做，不承诺倍数。

## 3. 静态计划与数据命名空间

Admission 先沿现有 SQL 校验，再用类型正确的边界比较确认纯细分。不能比较 LessThan 字符串，也不能只看新分区数更多；末尾 RANGE 的现有 SQL 校验允许端点变化，fast 路径须另外拒绝。

逻辑计划先用 child ordinal，元数据阶段绑定真实新 PID。每代计划持久化：协议版本、logical table ID、原始 source PIDs/边界、fresh target PIDs、ParentByNewPID、列/路由语义摘要、源快照 S、索引组、DirtyRows namespace、repair fence namespace、任务/attempt/RPC journal、manifest/importTS、GC pin 和清理归属。

DXF task key、backend cache key、engine UUID、对象前缀、remote SST/attempt/request 身份都包含 job/generation/kind/group；不得只用 jobID 或 tableName/indexID 复用。每笔发送的 payload/manifest 深拷贝或不可变封存后与 Intent 绑定，避免旧 actor 共享 engine/slice 后实际发送另一代数据。每代的 record/local 使用 fresh target PID；替代 global 使用 fresh index ID（logical table ID 不变）。所有可能导入的 key 均验证属于该代的完整允许集合，不能只验最小/最大 key：

- record：target PID + record tag。
- local：target PID + 允许的 local index ID。
- global：logical table ID + 该代 replacement global index ID。

禁止 SST 写原权威行、原 global index、marker 或 fence。编码器逐 KV 检查、manifest 校验内容摘要；排序/导入不得改变 bytes。数据或 schema 旧路由异常不去重、不用 last-write-wins，终止本次 fast 构建并保留原数据。

source 与 target、不同 generation 的目标集合永久不相交，ID 永不复用。尤其 replacement global index ID 是 logical table 下的索引 ID：跨换代/取消/回退必须保留 MaxIndexID 的已分配高水位，不能把原始 TableInfo.Clone 的旧计数恢复回来；否则晚 RPC 可能污染后续新索引。清理和发号不能从已变更的 Definitions 猜测，始终使用持久计划。64 位边界/IDs 用强类型或带类型十进制编码，不能经 float64。

## 4. 读取：先确定 readTS，再绑定物理布局

### 4.1 一致性读取

对 RR 使用事务管理器规定的一致性快照 TS；RC 使用该语句的有效 readTS；历史读使用其显式 TS。相关分区表的首版读路径禁用 MaxUint64 点查快路径，先物化有限 TS，并按现有 snapshot/GC 规则保护该读取。新协议作业启用前，所有节点都必须已经采用这条规则。它可能增加 TSO/历史元数据查找及缓存成本；首版要求明确 benchmark，不把 DDL 加速当作普通查询零开销的证明。

同一语句的所有一致性数据源共享同一有效 readTS，不能逐表分配不同 TS。物理 read projection 取自 **同一 readTS 的元数据**：Definitions、公开 index IDs/编码版本、路由/列类型。列/权限的逻辑解析保持 TiDB 原规则；如果逻辑列 schema 与历史物理投影无法安全配合，显式 ErrInfoSchemaChanged，不误解旧 bytes。历史读元数据/数据已失效则 SnapshotTooOld/明确错误，绝不回退最新布局。

可复用 Domain.GetSnapshotMeta/GetSnapshotInfoSchema 的能力，但不能盲信 latest cache 对任意新 TS 都精确。首版必须核对 readTS 快照中的 schema version；只命中能证明覆盖该 TS 的版本/区间，否则回源 snapshot meta。不可让 SessionExtendedInfoSchema 的最新 MDL overlay 覆盖这个只读投影。

读投影全程用于 partition pruning、index scan、global index handle 到 row 的回查；plan cache 校验该投影的 PID/public-index/schema 指纹。不能只改 table scan 而让 index lookup 用当前分区表对象。用户读令牌只允许该快照 Definitions 的公开 PID/索引；不能显式查询 AddingDefinitions 或 Quarantine 私有空间。读投影不能授权 mutation；这是独立于写上下文的接口约束。

这样，旧 RR 在拆分后首次访问仍使用旧布局及旧 readTS；新读使用新布局。旧物理数据按 GC safepoint 与删除范围时间保留，不要求对旧快照 backdate 新行。

### 4.2 当前读与写入令牌

写入通过独立的 MDL mutation token，绑定本事务认可的当前权威布局及当前 DML 协议。locking/current read 使用该 token 的布局和 ForUpdateTS；有效旧 token 能跨切换继续，是因为 DDL 在其退出前维持完整双写。

首个写 token 的 RR admission 还须检查：本事务的一致性读布局是否与该写布局一致（包含列语义、公开索引 IDs/版本及分区 PID，不只比较行 PID）。若 RR 老快照的布局/公开索引集合与首次写 token 不一致，则 ErrInfoSchemaChanged，要求重启事务；即使 current read TS 较新也不能绕过此检查，否则后续一致性 SELECT 看不到只写在新 PID 的 own writes。RC 按语句视图与写 token 配合。

已在切换前取得有效旧写 token 的事务可完成：它的 own writes 同事务存在于 old/new 两份，RR 在旧布局读取能看到；DDL 的 MDL 屏障等它退出。更早只捕获或索引尚不完整的 token 已由前面的状态屏障排空，不能跨到切读后。

同语句/事务的缓存写计划必须校验完整 write signature：代号、generation、影子命名空间、RowBackfillState、各索引状态及 schema。不能把读投影或旧代缓存对象当当前 mutation context。不得偷偷推进 RR TS，也不得在表层吞掉 commit 冲突换快照。

### 4.3 内部 backfill 读取

index builder 需要在尚未发布时读新分区，不能走普通 SQL 的 public-layout 选择。它使用显式、只读的 BackfillReadPermit：job/generation/phase、允许输入 PIDs、列语义、exactTS 或 minReadTS。

record baseline 输入只含原 source PIDs，强制读 S；local 输入只含本代 target PIDs；global 输入为 target 加未变更 PIDs。二者均在行完整、该组完整捕获的 MDL 屏障后取得 minReadTS，逐 range 新读 TS 必须不小于它，不能沿用旧 session snapshot。不是泛化的 InRestrictedSQL 免检：内部写仍须具备 marker/mutation 能力，或与作业互斥。

## 5. DML 与 DirtyRows

DirtyRows key 为 `(namespace,q,h)`，q 为新分区，h 为真实 row handle；value 仅为协议/事务写 nonce，不存历史行值、动作或 applied 标记。nonce 可取 txn.StartTS，但不作 commit 顺序或 S 过滤依据。

| RowBackfillState | 受影响范围的前台操作 |
| --- | --- |
| Running | 权威行/索引 + old/new 目标键 marker，同事务 |
| ReadyToMerge / Merging | 权威行/索引 + 目标 record + marker，同事务 |
| Inapplicable（已完成） | 两份完整 record；各索引按自己的状态维护，不再写 marker |

准备的 DeleteOnly/WriteOnly 兼容过渡允许冗余 marker；S 仅在未完整标记的写令牌退出后取得。删除也写 marker，不能“目标还不存在就跳过”。更新分区列/handle 标记两个端点；进入/离开受影响范围分别处理；未变更 old/new physical ID 相同只写一次。

权威布局校验业务约束，shadow 仅维护所需 KV/索引，不能因半成品或退休 shadow 报业务 unique/assertion 错误。行 repair 不调用 AddRecord、不生成索引事件。

nonclustered 新 handle 候选在同事务检查权威及对应父/子 record；占用则重新分配 hidden handle，两份使用同一 h。只重选 hidden ID，不重算用户 AUTO_INCREMENT 列/LastInsertID。clustered 用户主键不允许由该循环修改。

optimistic 写集合冲突沿原有事务重试规则；pessimistic 使用会话 ForUpdateTS/current locking read，锁不存在候选键，并正确处理锁返回值、deadlock/超时/staging。RC 的 LockOnlyIfExists 路径不能充当空槽 reservation。表层通过拟新增 tblctx capability 请求这些服务，不能 import executor 形成环；未支持的上下文在修改源前拒绝/等待。

同表 bulk ingest/restore 与 reorg admission 使用共享 table-operation guard，在同一元数据键上事务 CAS 竞争，不能双方先读“空闲”再各自启动。未知未排空的 canonical-source importer 不能只因 lease 过期就释放 guard。互斥冲突须等待/拒绝，不能靠选择旧 txn 路径绕过。平台无法控制绕过这些入口的外部写入时，不开放 fast 功能。MDL 关闭与 active reorg 也用持久依赖/同键串行化，不能仅一次预检。

## 6. 基线与脏键修复

捕获 MDL 屏障后取 S，建立并验证 GC 保护，固定原始 schema/parent 计划和完整 source record 前缀范围。Reader 重试读同一 S；输出 manifest、checksum、range 与 importTS 固定。record scanner 无索引/业务过滤条件，完整枚举源 record 前缀，保留 h，不调用 allocator；codec/checksum 与新 physical key 的兼容性必须验证，不能无条件复制旧格式 value。

DXF 使用专门 record kind，不伪装 index ID。生成 record KV 后复用外部排序/merge/import 组件，按严格导入协议完成。只有 record phase 所有输入、导入意图和成功回执封存，才发布 Ready；之后不允许本代 record import。S 不再用于后续 repair；读输出与 record import 封存后可释放该 S pin。

Ready 的 MDL 屏障排空所有 Running-only 写令牌，之后分配并持久化 repair minReadTS，再开始 repair；每个 repair 事务 startTS 必须不小于它，不能用旧快照扫描 marker 而把后来的旧 Running marker 漏掉。对于脏键 K=(q,h)，在同一个新 KV 事务中：

```text
读 Open range fence 和 marker
  → 读当前 parent(q),h
  → 源存在且新路由为 q：按当前源值 Put 目标
    否则：Delete 目标
  → 删除 marker，写 range fence nonce
  → 提交；失败整笔重试，不缓存源结果
```

源、marker、fence 用同一符合 minReadTS 的当前读视图，不能用 S；若拿到 PartitionHandle，必须使用独立真实 row handle，不让其内含 PID 覆盖 parent(q)。范围覆盖全部 temp 前缀，cursor 只在确认提交后前进。

每个逻辑 range 的 fence 为 O(任务范围) 元数据。结束/取消该阶段时原子改 Closed，旧 repair 若晚提交则写冲突，新 attempt 读 Closed/缺失即停，worker 无权创建/打开 fence。Closed 作为 live 安全元数据保留，不与 DirtyRows 共 GC；要清理它须另证旧事务/attempt 终止。关闭不是完成证明，成功还需匹配完整 range 的提交后终点/摘要；Abort Closed 不作成功。

全部 range 完成且 fence 关闭后进 RowDirect；等待旧 marker 写令牌退出，才清理 marker。游标之后新 marker 已由前台同事务应用目标，无须追求 temp 为空。

## 7. 索引阶段

先 local，后 global，每组用已有 Running/ReadyToMerge/Merging/Inapplicable 协议。只改适配层，不修改成熟 temp index value、unique/NULL/delete/version 检查。

| 组 | 输入 | 输出 |
| --- | --- | --- |
| local | 本代全部新分区 | 其相关 local indices；clustered primary 已是 record |
| global | 本代新分区 + 未变更分区，各一次 | 本代 replacement global IDs |

行已经完整并持续镜像，再启用该组 temp capture，等待未捕获写令牌退出才扫描。未变更 local 和原权威 global 正常维护，不重建/不改状态。global 统一构建一轮，不按父分区重复构建。

每布局索引状态投影独立；完成组仅使影子索引正常维护，**不提前对 SQL Public**。定义切换时才原子公开新 PID/新 global，旧 global 降为影子。否则历史元数据可能出现公开 global 指向未公开分区。

适配正确性的归约条件：输入是最终布局的一份完整行集，DML 流对 target PID/handle/列值/索引版本的变化与普通 ADD INDEX 等价。必须验证跨分区移动、唯一键复用、NULL、global PartitionHandle、旧/新 GID 编码及所有状态过渡；“组件成熟”不能替代这个条件。

## 8. 严格导入协议与 generation 隔离

### 8.1 请求账本

每个可能修改 TiKV 数据的 RPC 先原子持久 Intent，含 job/generation/phase/attempt/seq、不可变 manifest/TS、目标允许集合，再发送。每次发送都检查当前许可，缺失或非 Active 即拒绝；不缓存许可无限重发。每个 Intent 只对应一次 wire 尝试；重启见未决 Intent 不重发，已成功回执不是再次发送许可，确定拒绝后的重试用新 seq。后台库/transport 不得把不确定重试藏在最终 nil 后面。

| 返回 | 处理 |
| --- | --- |
| 认证存储的成功应答 | 视为已 apply，持久回执；回执丢失则控制层仍按未知处理 |
| 能按该存储版本证明未执行的拒绝 | 可重规划/重试，同一不可变内容/TS；默认不认识的错误不归此类 |
| 超时、after-send cancel、断连、Undetermined、未分类错误、失联运行 attempt | Sticky Uncertain，整代失去发布资格 |

回执和 range coverage 必须匹配封存 manifest 的全部数据及生成的 CF/SST，不能只看行数 metric 或其中一个 CF。空 probe/缺失 engine 的 nil 不是非空数据回执；真实空输入也有显式零数据完成记录。匹配后封存该 phase，禁止新 Intent。未封存、遗漏回执或有 Unknown 时不能进 Ready。Import 运行 subtask 不按普通幂等任务迁移重发；DDL owner 失效可继续等待原合法 attempt，无法确认就换代。Read/sort 可按完整 winning-manifest 规则重试。

本地 TiKV 源码已核到 classic ingest 等待 async_write 的 Finished/apply 回调后才发成功响应；这提供正常成功合同，不把 client cancel 解释为 server cancel。部署版本必须认证同一语义，不认识的内核走旧路径。

### 8.2 不确定代永不发布

g 只允许 `BUILDING → READY → PUBLISHED`；未发布时可单向变成 QUARANTINED，不能再回 READY/PUBLISHED。后台所有 key 都属于 g 的私有集合。任何不确定结果即持久化 Quarantine/Abort，再撤销发送许可和 repair fence。

重试分配全新 target PIDs、replacement global IDs、marker/fence、manifest/TS。原始 source PIDs 和逻辑 SQL 形状不变。发布新捕获目标并等旧代专用写令牌退出，取新 S，从原权威源全量重建；首版不搬用旧输出、旧 marker 或部分已导入进度。

代号切换后，关闭旧读输出登记/导入许可，再释放旧 S 的 pin；旧只读 scanner 可失败，但其输出永不能再用于发布。不因未知私有 SST 而无限钉住全局源 GC。旧 RPC 即使极晚 apply，也只写 g 私有集合，不会触及下一代、原权威源或已发布代。迟到“成功”不能取消 Quarantine。只有当前代、无 Unknown、全部后台 phase 封存、row/index 完成才能在原子元数据事务发布。Publish 与 Quarantine 共享存储态 CAS：结果未知先读实际状态和 Definitions，不凭内存判断。若已 PUBLISHED，不能再走 never-published 换代清理。

明确的代价是故障可能重做整代，不提供未知 import 的原地透明恢复。这避免需要新 TiKV range-fence RPC 或靠旧 importTS/GC 推断安全。

### 8.3 物理清理与逻辑回退分开

从未发布的 Quarantine 可在撤销协议、排空相关前台写令牌且旧权威完整后，让 SQL 作业回退/换代；不能谎称其物理数据已清理。

确认无未来写入的代正常登记 delete-range/对象清理。不确定代持久化 CleanupPending，自动清理完成必须有 quiescence 证据；不能把它的 IDs 未加筛选地交给普通 finishDDLJob/delete-range 调度，延期 journal 必须先落盘再让 SQL job 进历史；首版无证据则保留隔离，不按 TTL 猜测，也不复用 IDs。容量计入 quarantine 预算，越限停 fast admission。未知 RPC 的永久故障可能需要运维处理，这是明示约束，不阻塞正常源表读写或把错误数据发布。

## 9. 切换、回退和 GC

切换之前目标 record/local/global 已完整并持续镜像，所有 import sealed、repair fenced、无 Unknown。原子更新 Definitions、公开新 global、降低旧 global、job 状态；该元数据事务的实际 commitTS C 是元数据 MVCC 的布局切点，无须猜一个客户端时钟时间。

- readTS<C 的读取投影取旧布局；readTS≥C 取新布局。新数据/索引的完成提交均在 C 之前。
- 切换后有效旧写令牌仍完整双写；MDL 等其退出，之后才能停止旧完整性。
- DeleteReorganization 窗口可回退：旧仍完整，先原子恢复原布局并同步，再退新写入。回退后的新代曾被历史读取，属于 Retired-visible，**不是 Never-published Quarantine**。
- Public 后新布局权威、旧影子可不完整，不再检查其业务约束，拒绝回退，前向恢复。
- 曾公开的 old/new 物理空间只在相应删除范围时间早于 GC safepoint、有效旧读/写及后台写入都安全退出后清理。readTS 的元数据和数据受同一 snapshot/GC 规则保护；旧快照过期显式报错。

暂停采用停止新工作并排空在途请求的软暂停；source marker/镜像按当前协议继续，必要 pin 续约。硬中断造成未知则换代；S 失效或 importTS 不再可接受也换代，不偷偷改同代 TS。切换之后不再有合法 ingest，故障只恢复正常事务/元数据/清理，不启动新 baseline。

## 10. 正确性论证

证明以 TiDB 原子 MVCC/普通 SQL 约束、有效 snapshot/GC 租约、元数据事务与认证 ingest 成功语义为基础，不宣称重证整个存储系统。

1. **身份单射**：两个源若输出同一(q,h)，唯一 parent(q) 强迫它们同为(P,h)，不能是同一快照的不同物理行。范围扩缩、混合来源拒绝，所以不用逐行映射。
2. **变更覆盖**：S 在不完整捕获写者退出后取得；S 后每个改变目标正确值的事务原子标记 old/new 键。未变键由 S 提供；初始化旧 marker 不带历史数据，点读不会覆盖成陈旧值。
3. **单键修复**：D(q,h,t) 是修复快照中父源行的当前投影。能改变 D 的并发 DML 也写该 marker，与 repair 删除冲突；DML 先则重读/重试，repair 先则后续已双写事务更新目标。不影响 D 的变化不妨碍此键。
4. **遍历完成**：merge 前 Running-only 令牌退出，之后新 marker 均伴随目标写入。全范围提交后 cursor 加 Closed fence，保证所有未应用变化修复、晚 worker 不能越阶段。故 RowDirect 的最新行集等于源投影，并由镜像保持。
5. **索引归约**：第 7 节输入/流等价条件把 local/global 的构建问题归约为已有 ADD INDEX 合同；镜像保证从完成到切换不漂移。不能省略适配验证。
6. **读历史**：元数据在 C 原子改变，τ<C 读旧 PID/旧 public index，它们在 τ 时仍权威；τ≥C 读已完整的新布局。使用同一 τ 的 KV 历史，避免旧快照配新 PID。有效读阻止物理旧范围过早 GC；过期/绑定不明显式错误。
7. **事务自洽**：首写 RR 只绑定与其一致性布局相同的写令牌；有效旧令牌跨切换时双份 own writes。读投影不能变更数据，当前读跟随写令牌，防止写已退休 PID 或读不到 own writes。
8. **不确定导入隔离**：可发布代每个发送都有已确认回执，封存后没有新发送。未知代永不发布且命名空间不相交，晚 apply 不影响当前数据，即使当前代删除版本已被 GC。取消和物理清理的完成状态分开，因此不靠等待假装已安全。
9. **恢复单调**：manifest/计划/Intent/回执/Quarantine/Closed 单调持久化；提交结果未知不推进 cursor。重入只接受同身份的事实，或以新代从原源重建，不重新解释旧成功/旧失败。

安全性成立不等于任何永久故障下都能有界成功；第 2、8 节明确给出重做、预算停用与隔离运维约束。实现若不满足任一合同，不允许启用该路径。

### 10.1 切换时必须成立的精确断言

设 `P(q)` 为新分区 q 的唯一旧父，`R_t(p,h)` 为提交视图 t 下的权威行，`F_t(q,h)` 为该父行存在且按新规则路由到 q 时的编码结果，否则为空。行阶段完成后的断言是：对每个新 q、每个 h，`Target_t(q,h)=F_t(q,h)`，包括不存在的键；不只是“扫描到的行都已导入”。每个后续已提交前台事务必须维持这个等式。

完整索引阶段再要求：每个新 local/global 的逻辑条目集合恰好等于最终行集经既定 index encoder 的投影，含 unique/NULL 语义及 global PID。切换事务只能在这两个全称断言及后台封存条件都成立时提交。断言的维持依靠状态屏障、marker 写冲突、已完成组持续维护，而不是上线时做一次 count/checksum 比对；count 相等不能证明不存在错行或错索引。

查询正确性另外要求：每个合法有效 readTS τ 的数据和物理布局同属 τ；RR own writes 满足第 4 节的写令牌合同。它不能从“切换时最新行集相等”推出。恢复正确性另外要求：任何未确认后台写入都不能再触及可发布命名空间；它不能从“任务标记结束”推出。

因此这里得到的是有明确前提的协议安全论证：第 10 节各引理成立，可推出布局切换不会遗漏/复活已提交行、产生索引不一致或把有效历史读取配到错误布局。尚不能宣称对未来 Go 实现做了机械证明；特别是第 7 节的 ADD INDEX 事件流等价、所有写入口覆盖、存储版本成功语义必须逐项验收。发现任何一项不满足，处理是禁止 fast admission/修正实现，不能把它当作可接受的小概率 corner case。

## 11. 验证证据与上线门槛

[规划原型](../agents/ddl/models/partition_split_plan.py)覆盖 18 个边界/ID 用例、256 对细分关系；[脏键模型](../agents/ddl/models/partition_row_delta.py)覆盖普通与初始化 stale Put/部分 Delete；[并发模型](../agents/ddl/models/partition_split_concurrency.py)覆盖读/准备/提交/回滚及退休；[安全模型](../agents/ddl/models/partition_split_safety.py)覆盖元数据/数据同 TS、缓存、错误历史写、未知代晚写和 tombstone GC。

```bash
python3 docs/agents/ddl/models/partition_split_plan.py
python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile
python3 docs/agents/ddl/models/partition_row_delta.py --startup delete --reconcile
python3 docs/agents/ddl/models/partition_split_concurrency.py --lifecycle
python3 docs/agents/ddl/models/partition_split_safety.py
python3 docs/agents/ddl/models/partition_split_safety.py --latest-layout
python3 docs/agents/ddl/models/partition_split_safety.py --historical-write
python3 docs/agents/ddl/models/partition_split_safety.py --mixed-rr-write
python3 docs/agents/ddl/models/partition_split_safety.py --reuse-uncertain
python3 docs/agents/ddl/models/partition_split_sql_probe.py --late-snapshot --bind-read-layout
```

四个 safety 破坏变体预期退出 1；正常安全模型还覆盖回退后历史布局区间读取。SQL probe 使用自己启动的旧 dirty binary/unistore；`tidb_snapshot` 仅用于验证已有历史 schema/data 绑定原语，不是建议在用户事务里 SET 该变量。fixture safe point 沿 testkit 模拟，**不验证真实 GC**。新 read resolver 仍须检查版本精确性，不能因 probe 通过就直接复用可能滞后的 latest cache。

本机证据：`ps-8f2ngd1f/result.json` 记录旧 RR 0/4 行；`ps-398mypcm/result.json` 记录 snapshot 绑定后普通/强制 global index 读取均恢复 4 行；路径前缀为 `.codex-tmp/`。这些非仓库依赖。所有 own server 已停止。此前事务/MDL/EXCHANGE/日期实验报告 `ps-sxmhpbt5/result.json`。

存储取证：本地 TiKV clean commit `11b128e16d58d838c5949d7f86153105eda3dadb`，`src/import/ingest.rs` 等待 `raft_writer::wait_write` 的 Finished，`components/raftstore/src/store/fsm/apply.rs` 在 pending SST ingest/write_to_db 后回调。该结论只认证读到的路径，不自动认证任意部署版本。

产品发布仍须在当前源码与真实 TiKV 完成：全部 DML hooks/staging/allocator/shard/overflow、悲观 RR/RC 空键锁、RR own writes 与首写拒绝、metadata cache lag/plan cache/历史读/实际 GC、wide-row/checksum、index stream 等价及 unique/NULL/key reuse、每个 RPC/manifest/Meta 边界的故障与换代、旧写/回退历史读取、quarantine 预算/清理和 CDC/部署矩阵。源包新增测试/Bazel/lint遵循仓库流程。

本次交付是最终设计与约束及独立证据，不是这些产品测试已执行。原始全量行/索引 ingest 目标未缩减为 index-only 或全量 txn 回填。

## 12. 已比较方案与取舍

| 方案 | 正确性与选择 |
| --- | --- |
| 只改 ReorgType、沿用普通双写再 ingest | 缺失目标的删除和版本顺序未处理，拒绝 |
| temp 保存完整历史行值并 replay | arming stale Put/部分 Delete 反例已证，拒绝 |
| 只把 importTS 调早 | 不能补出缺失删除，不能解决 metadata/readTS 配错或 GC 后晚写，拒绝 |
| DirtyRows + 当前源点读 | 唯一父关系使当前值可 O(1) 定位，作为首版 |
| 全量 RowToken/正反映射 | 可研究一般合并/独立 handle，但复杂且有 O(N) 事务元数据；纯拆分不选 |
| record/local 融合 | 在另建 temp local 和共同完成边界时可能成立，首版分开以缩小证明面 |
| 全局模糊/分任务基线快照 | dirty 覆盖与唯一 winning range 输出下可研究，但首版固定 S，先不扩展这一证明面 |
| global index 原地更新 | 并非理论不可能；需处理旧写令牌的旧 PID 值与新读者及最终重定向，首版保留新 GID/成熟构建流程 |
| 只加新布局 readTS 下界并报错 | 可作保守退路，但当前方案用历史读投影恢复正确读；无法确定绑定才显式错误 |
| 等所有旧事务退出再切读 | 会被未访问本表的无关长事务拖住，仍需处理历史读，首版不选 |
| unknown import 原地重试或只等 ctx cancel | 不可证明迟到写/GC 安全，拒绝；未知代隔离并换新命名空间 |

RR 首写布局匹配也包括公开索引集合。即使 PID 未变，若读侧历史索引已在当前写布局被删除，直接允许写可能使后续历史索引读取看不到 own writes；首版不做这种细粒度豁免。有效旧写令牌持有其 MDL 兼容合同，不能等同于写已退休布局。

## 13. 可执行存储原型（2026-11-10）

已添加 [Go prototype](../../tools/partition-reorg-prototype/README.md)，使用当前源码的 row/index codec、simplesst 多 writer 排序/merge、真实 TiKV MVCC 和 Write/MultiIngest，验证行修复、索引字节及解码回查、历史布局、丢失导入回执后隔离重建。运行方式、结果与验收范围见 [prototype ExecPlan](../plans/2026-11-10-partition-reorg-prototype.md)。

原型以自己的协调器和 MVCC layout key 代替 SQL 元数据/MDL/DXF 调度；在线索引续作已使用生产 temp 编码/过滤与独立 merge adapter，在构建期间执行前台写入、唯一键复用、NULL/分区移动与回滚验证；阶段屏障使用原型的粗粒度 MVCC 状态键，2026-11-12 进一步增加冻结快照三方对比，并在独立集群调用实际 DXF merge step/生产 worker 重放；连续前台写入仍由独立原型验证，且无 GC worker。它验证底层存储链路，未实现本设计的线上 SQL 执行路径；第 11 节的产品发布门槛继续有效。
