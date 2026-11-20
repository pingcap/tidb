# 纯拆分 REORGANIZE：最终设计实施计划


本 ExecPlan 按仓库根目录 `PLANS.md` 维护。当前用户目标是“给我最终设计和约束”；本轮交付设计和独立证据，不实施产品执行路径。后续产品里程碑明确列为未完成，不能当作本次已验收。唯一规格为 [最终设计](../../design/2026-11-02-partition-global-sort.md)；[历史计划](10-partition-fast-reorg-execplan-history.md)和[审查记录](../../design/2026-11-03-partition-global-sort-review-notes.md)保留被否定候选。

## Purpose / Big Picture


让一个或多个相邻旧 RANGE 分区分别细分，record/local/global 全量基线用 DXF/global sort ingest，减少事务搬行成本。保留 handle、原权威源和线上事务；用脏键修复处理并发，用 readTS 对应的历史布局解决旧快照错读，用 fresh generation 隔离不确定导入。首版不包含合并、独立影子 handle 映射或物理不确定数据的自动强制清理。

设计交付的验收是：限制、接口与状态选择确定；每条安全结论有前提/证明；已有反例被具体协议处理；证据范围诚实且可复现。产品发布的验收另含当前源码/真实 TiKV/DXF 全链路测试和性能评估，不能由独立模型替代。

## Progress


- [x] 用户认可纯拆分限制，允许多个相邻旧父分别细分，保留全部旧边界和整体范围。
- [x] 完成 admission 原型及 metadata 边界测试。
- [x] 从历史反例淘汰 full-value replay 和通用 RowToken 默认方案，选择 DirtyRows 当前源点读修复。
- [x] 旧 dirty binary 复现 old RR 首次访问 0/4 行；通过 snapshot schema/data 绑定原语恢复普通及 global index 读取 4 行。
- [x] 确定一致性读布局与 mutation token 分离、RR 首写检查和 own-write 合同。
- [x] 核对本地 clean TiKV classic ingest 的成功应答位于 apply 完成之后。
- [x] 确定 Intent/回执封存、不可确定代永不发布、fresh namespaces 重建与 CleanupPending 预算约束。
- [x] 独立模型覆盖 MVCC 绑定、错误历史写、own writes、未知晚写/tombstone GC；保留负向反例。
- [ ] 后续实现：当前源码的读/写投影、metadata 精确性和计划缓存/GC/当前读适配。
- [ ] 后续实现：DirtyRows hook、staging/allocator、RR/RC 锁及内部写能力控制。
- [ ] 后续实现：record pipeline、strict RPC journal、代际重建、shadow index 适配和角色切换。
- [ ] 后续发布：当前源码 failpoint、真实 TiKV/DXF 故障/复制矩阵及性能验收。

## Surprises & Discoveries


nonclustered 并不总需要映射。新分区的唯一旧父使 `(q,h)` 可逆到 `(parent(q),h)`，不同旧父重复 h 在纯拆分不碰撞；跨边界重划不成立。

完整值 replay 的初始化假设不成立。旧 schema 未捕获或 DeleteOnly 部分动作可以晚于新捕获提交；S 读到较新源，旧 temp 值/删除会回退结果。dirty key 不携历史内容，修复当前源避免这类错误。

MDL 不排空从未访问目标表的旧 RR 事务。旧 binary 实验 fresh=4、old RR=0；snapshot metadata/data at same TS 读到 4。最新行集一致与 MVCC 查询正确是不同义务。

本地 TiKV `11b128e16d58d838c5949d7f86153105eda3dadb` 的 ingest 等待 Finished/apply 后响应成功；网络错误/cancel 不提供相同知识。最终规格不靠 unknown 原地重试或不存在的存储 fence，而用不可发布代和不相交新命名空间。

## Decision Log


- Decision: 首版统一纯拆分、多父可分别拆分，整数 clustered 与 implicit handle，禁止显式写 row ID。
  Rationale: 静态单射证明，去掉 O(N) 身份元数据；保留原目标的完整 ingest。
  Date/Author: 2026-11-03 / user and Codex。

- Decision: DirtyRows 仅标记目标键；repair 点读当前父源，不 replay 历史值。
  Rationale: 关闭 stale Put/部分 Delete 初始化反例，源槽位复用按当前状态处理。
  Date/Author: 2026-11-03 / Codex。

- Decision: 一致性读取 readTS 的元数据布局，mutation token 保持独立且按 MDL 有效；RR 首次写布局不匹配显式重试。
  Rationale: 解决旧 RR 首次访问缺行与错误历史写，同时防止 read-own-write 混布局。
  Date/Author: 2026-11-03 / Codex。

- Decision: 不确定导入使未发布代永久失去资格；全新 PID/GID/marker 重建，旧代 CleanupPending 明示保留。
  Rationale: 不需要臆测 RPC drain，旧晚写不可能污染被发布代；用重做/空间预算换安全。
  Date/Author: 2026-11-03 / Codex。

## Outcomes & Retrospective


最终设计把先前未决的读绑定和 unknown import 转成了具体合同与约束，不再保留“未来应找到某个 fence API”的空缺。独立证据支持这些前提，产品尚未实现；最终规格不等于当前 TiDB 已获得这些保证。保留历史反例以防未来为性能又引入相同错误。

## Context and Orientation


`pkg/ddl/partition.go` 的现有普通重组强制 txn，先行、后新分区索引、再未变更 global。新分支只在 admission 能力满足时启用。`pkg/table/tables/partition.go` 是 mutation 路由入口，`tblctx/tblsession` 提供会话能力；新 hook 不能让 read-only projection 授权写。

`pkg/planner/core/preprocess.go` 的 MDL 最新 overlay 不能覆盖物理读视图。`Domain.GetSnapshotMeta/GetSnapshotInfoSchema` 提供历史信息基础，但 cache latest 对任意新 TS 的精确性需核验。读 token 的指纹含公开 PID/index/编码/列语义；写 token 的签名另外含 generation/影子空间/状态。

`backfilling_read_index.go`、merge sort、cloud import 和 `index_merge_tmp.go` 保留成熟索引算法，只适配输入/状态。record 为独立 kind，不伪造 index ID。global 统一扫描最终集合一次。

## Plan of Work


Milestone 1 先在当前源码实现只读投影和 token 检查，重放已记录的 old RR 反例。覆盖普通扫描、global lookup、RR/RC/current read、缓存、own writes 和 GC 失效，确保旧读正确或明确错误。

Milestone 2 接 admission/不可变计划、DirtyRows 和 KV repair range fence。完整 DML hook 原子性、allocator 候选重试、未变更数据去重和 unsupported writer 入口互斥必须先验收，再加 baseline。

Milestone 3 接 record external pipeline 和严格 Import Intent/回执。任何 unknown 都不能被内层重试变成无记忆 nil；import running task 不原地跨 executor 重发。注入 owner/worker 失败、丢响应，验证整个目标代从不发布且新代全部 fresh。

Milestone 4 顺序接 local/global，证明输入和事件等价于 mature ADD INDEX。全部完成才切 visible Definitions；rollback-visible 代按历史 GC 处理，never-published 代按 quarantine 合同处理。

Milestone 5 做真实 TiKV/DXF、组件矩阵、空间和性能。未验证组合不开放；不预设加速倍数。每个里程碑写实际证据，生成元数据遵循 repo Bazel/failpoint/Ready 规则。

## Concrete Steps


本轮已执行的独立验证从 repo 根运行：

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

四个 safety 破坏变体预期退出 1；normal model/binding primitive 退出 0。SQL probe 需要现有 binary/PyMySQL，使用自己启动的 unistore/socket，自动停止并保留版本和日志；它不设置任何客户变量，也不是当前源码 regression test。

后续代码实施前先读取目标包 doc.go、go-coding-style、Bazel/failpoint/真实 TiKV 测试技能及 docs/agents/testing-flow.md。新增当前包测试名称由实现确定，先用 -list 确认存在后 targeted 跑；不把未来测试名无匹配当成功。产品代码 Ready 包含 make lint，当前独立 Python/文档检查不替代产品验证。

## Validation and Acceptance


设计交付验收映射为：形状与允许/拒绝例子；字段/角色/状态与命名空间边界；旧 RR 修复策略；源码已有 post-apply 能力；unknown 不可发布/重建/清理约束；单射/捕获/修复/读取/事务/代际安全证明；明确测试范围和未来发布门槛。逐项均在最终设计有确定规则，未把未知项留给开发猜。

产品验收另外要求成功 SQL 结果/索引/约束/历史读/own writes 等价、每种故障仍保留已提交用户数据、无源与 published namespace 越权写入，所有私有产物有清理归属。CleanupPending 不当作物理清理完成；预算停 fast 不阻断旧模式能力。没有 secondary index 时也须完成行修复与封存。

## Idempotence and Recovery


原始逻辑形状固定，每代物理输出 fresh。read/sort 的 winning manifest 封存后不可换内容；repair 重入点读当前源、提交后推进；import 通过 Intent/回执和不可确定代淘汰处理。Owner 切换只接受同一身份的持久事实，不能把 unknown 当未发送。

Quarantine 从不复活。reset generation 不复用旧 marker/文件进度，重新捕获屏障和 S；未发布代可以逻辑回退后留 CleanupPending。曾发布后回退的代仍可能被历史读引用，按 readTS 元数据与正常 delete-range safepoint 清理。Closed repair fence 为保留安全元数据，不与 marker 范围一起删。

## Artifacts and Notes


TiDB baseline 为 44facb0，SQL binary 是旧 dirty 10d7421b80。旧 RR counter 为 `.codex-tmp/ps-8f2ngd1f/result.json`；历史绑定成功为 `.codex-tmp/ps-398mypcm/result.json`（普通和 global 均 4 行）；前置事务/MDL/日期 smoke 为 `.codex-tmp/ps-sxmhpbt5/result.json`。本机路径只作证据，不是 portable dependency；own processes 均停止。

本地 TiKV 取证 commit 11b128e 来源目录 `/Users/joechenrh/code/pingcap/tikv`，没有修改或运行 Rust 测试。接口成功语义只认证读到的 classic 路径，发布仍需认证对应版本。

最终审查采用 Ready 范围（文档与独立原型，不含产品 Go 实现）。2026-11-03 重跑 15 个正/负模型命令：7 个正常变体退出 0，8 个破坏变体按预期退出 1。主行模型为 513 histories / 94784 states / 6170 terminals；安全模型为 580 read bindings、4 own-write、9 rollback bindings、49 quarantine events。命令为上文 Concrete Steps 中的模型命令，另补：

    python3 docs/agents/ddl/models/partition_row_delta.py --startup put --reconcile
    python3 docs/agents/ddl/models/partition_split_concurrency.py
    python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile --mutation missing-delete
    python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile --mutation late-import
    python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile --mutation stale-scan
    python3 docs/agents/ddl/models/partition_row_delta.py --suite --reconcile --mutation stale-source
    make lint

另用 Python ast.parse 检查 models 全部 Python 语法、校验七份设计/历史/索引文档的 56 个本地链接，并对本次文档/原型执行 git diff --no-index --check；均通过。核对 SQL 证据 ps-398mypcm、ps-9xjo991z、ps-t88cplw9 均 owned_process_stopped=true。

make lint 退出 0，但输出既有 local-notes/repro 混包和 macOS find illegal option -- n，因此不能宣称 lint 覆盖完整。没有改动产品 Go/Bazel；没有运行当前源码产品单测或真实 TiKV/DXF，尚未验证项按最终设计第 11 节保留。依 docs/agents/agents-review-guide.md 自审：未引入代理政策、没有覆盖根规则、链接有效；方案规格与已实现行为明确分离。

## Interfaces and Dependencies


拟新增 PureSplitPlan/generation journal/DirtyRows/range fence/strict import policy；字段合同以最终设计为准，均非已存在 API。ReadLayout 为不可 mutation 的历史元数据投影，CurrentMutationToken 绑定 MDL/写协议。首写与 plan-cache 检查不能丢掉 RR own-write 条件。

所用基础为已有 snapshot meta/Infoschema、MVCC atomic txn、真实 RowID allocator、external sort/backend、DDL job/schema sync 与 GC。tblctx capability 的会话适配层提供 locking/current read，禁止表层引入 executor 环。可能写用户 KV 的 RPC 必须接受目标允许集合检查和 Intent/permit 封存，不能只看 DXF task succeed/cancel 状态。

修订记录：2026-11-03，将历史多方案收敛为最终规格，保存旧文档供审计；本次目标为设计交付，产品实现里程碑明确未执行。

## Prototype implementation follow-up (2026-11-10)


The user subsequently requested an executable prototype. Its implementation and current evidence are tracked separately in [the prototype ExecPlan](../../plans/2026-11-10-partition-reorg-prototype.md) and [tool README](../../../tools/partition-reorg-prototype/README.md). The tool exercises real TiKV transactions, simplesst sorting, ImportSST and MVCC layout records. It does not complete the SQL/DXF/MDL integration milestones above; those remain unchecked. The previous statement about a design-only deliverable describes the 2026-11-03 delivery.
