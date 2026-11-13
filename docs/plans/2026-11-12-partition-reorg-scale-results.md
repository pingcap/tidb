# REORGANIZE PARTITION 原型规模验证

2026-11-12。范围是独立原型及生产 DXF merge step 的重放验证；正式 SQL REORGANIZE 路径尚未启用。所有 Go 构建/测试使用 `-p 2`、`GOMAXPROCS=2`。

## 规模与结果

| 档位 | 初始行数 | 排序 worker | 成功后台变更 | 实际 ingest Region | 最终行数 | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 中档 | 40,000 | 8 | 2,048 | 10 | 40,006 | 16 项检查通过 |
| 大档 | 200,000 | 16 | 8,192 | 10 | 200,006 | 16 项检查通过 |
| 大档重复 | 200,000 | 1 | 8,192 | 10 | 200,006 | 16 项检查通过 |

每行含 512 字节 payload。增加的 6 行来自显式的在线索引场景。两轮大档的完整行集 SHA-256 相同：`a5f5a3a7e6da65b87fa87fb6be3f3c06b19e4afd18512af444a6b65f17a4a438`。每轮还分别执行全量字节比较、索引回查和独立逻辑行集合校验，摘要不替代这些校验。

两个大档分别生成完整冻结输入，交给实际生产 DXF merge step 重放。每轮 15 个用例、124 次 subtask 调用通过，合计 248 次；覆盖 batch 1/2/16、CPU 1/2、重复执行、提交失败回滚、冲突重试和非法分区拒绝。生产结果、冻结适配层结果、直接解码物理行得到的期望索引三方一致。

附加验证通过：147 项生产 merge 矩阵用例／412 次 subtask 调用，plain/gzip 解码与损坏/越界拒绝测试，原型单测，以及 6 项已有 ADD INDEX SQL corner-case 回归。所有自建进程和 PD endpoint 均已停止，failpoint refcount 为 0。

## 全量正确性检查

没有抽样或降低原有断言：源行投影与目标 record 完整相等；local/global 索引集合完整相等；每个索引条目解码 PID/handle 后批量回查实际行；预期逻辑 ID 集合及后台每个 handle 的最后一次写入独立核对。NULL、跨分区移动、唯一键复用、回滚、旧快照读、迟到 worker、丢失 ingest 回执与新代重建均保留。

多 Region 故障代只落盘 9,994／49,994／49,994 行便失去回执。逐字节验证这些非空子集后，该代仍永久禁止发布；fresh generation 必须重建完整数据。

## 发现与修正

1. 旧故障测试错误要求“丢失第一 Region 回执后，其余 Region 也已经导入”。先复现失败，再改成验证合法已应用子集、禁止发布，并要求新代全量重建。正常路径的全量一致性断言未放宽。
2. 中档并发触发原型粗粒度阶段键竞争，立即重试 256 次的循环耗尽。改为一处共享、受总超时限制的短退避重试，只接受确认回滚的 write conflict。未知提交/网络结果不重试，SST RPC 单次发送规则不变。原失败参数重跑通过。

失败报告保留在 `.codex-tmp/partition-reorg-prototype-75z1jyfk/result.json` 和 `.codex-tmp/partition-reorg-prototype-15vnc8ua/result.json`，不计入成功结果。

## 保持实现简洁

没有新增另一套回填协议，也没有修改生产 DDL 算法。规模相关改动限于：全量索引回查使用 128-key BatchGet；标准 gzip JSON 快照与 1 GiB 解压后上限；明确的规模/写入/超时参数；后台任务完成或取消后 join；三处重复重试循环收敛到一个函数。快照关闭后原子 rename，已导出的大对象及时释放。

原型仍在内存中保存测试数据和校验结果，runner 默认 Go 软堆目标为 3 GiB。单个旧父最多 100,000 行，以免与保留的场景 handle/ID 重叠。冻结包仅供测试对比，不是正式执行需要额外 dump 的源表快照。

## 复现与证据

```bash
GOMAXPROCS=2 go test -p 2 -tags=intest,deadlock ./tools/partition-reorg-prototype -run '^TestPrototype' -count=1
GOMAXPROCS=2 go build -p 2 -o .codex-tmp/partition-scale-final ./tools/partition-reorg-prototype
python3 tools/partition-reorg-prototype/run.py --binary .codex-tmp/partition-scale-final \
  --rows-per-parent 100000 --workers 16 --dml-iterations 4096 --split-regions --timeout-seconds 1200
```

将最后一条命令的 worker 改为 1 可重复大档；中档使用每父 20,000 行、8 worker、1,024 次写入。生产重放命令及全部实际参数保存于下列 JSON，构建流程见[实施计划](2026-11-10-partition-reorg-prototype.md)：

- `.codex-tmp/partition-scale-runs.json`：三档命令与结果。
- `.codex-tmp/partition-scale-production-replays.json`：两轮生产重放命令与结果。
- `.codex-tmp/partition-scale-production-unit.log`：生产矩阵及压缩边界测试。
- `.codex-tmp/partition-scale-sql-regressions.log`：现有 SQL 回归。

Ready 验证：完整 `make bazel_prepare` 本轮成功；生成结果复查无漂移，原型单测、工具包 vet、定向 revive、格式和文档检查通过。`make lint` 退出 0，但仍输出既有 local-notes 混包及 macOS find 错误，不能宣称仓库 lint 覆盖完整。

这些结果支持上述原型范围内未发现行/索引不一致，不能证明绝无错误，也不构成正式功能完成或性能加速承诺。未覆盖正式 SQL/MDL/完整 DXF 调度、真实 GC、Region 故障及副本切换；当前仍是单副本本地 TiKV，不能据此推算数百 GB 生产表现。
