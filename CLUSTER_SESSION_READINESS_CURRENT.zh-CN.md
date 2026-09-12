# Rust cluster-session readiness 当前结论

## 结论与版本

2026-09-12 在 `/tmp/tidb-hparser-current` 重新运行原始真实 access-path 流程，退出 0，日志结尾为 `the access-path differential passed`。当前 readiness 阻塞已解除，不应继续将历史启动失败描述为仍在复现的服务端死锁。

生产代码基线为 `a5da79cfd8`；运行期间独立提交的 `28eff34dc7` 只调整 GROUP BY 单元测试契约及报告，不改变节点实现。工作区已有的 shared-statistics 测试和报告 WIP 保留，未混入本次提交。

Go source of truth 固定为 master commit `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，二进制 `/tmp/tidb-go-master-oracle/bin/tidb-server`。真实回放日志开头记录了二进制自报的完整 SHA。PD/TiKV 使用 `v9.0.0-beta.2.pre-nightly`，避免旧 PD 不支持该 Go master 所需 QueryRegion RPC 的版本不兼容。

## 根因与已有修复

旧 `run-realtikv-access-path.sh` 在 `wait_for_port` 成功后仅执行一次 ready 日志 grep。TCP 监听已开放并不代表应用初始化完成；若 grep 发生得早，脚本立即退出，EXIT trap 随即终止仍在初始化的节点。日志停在统计加载或 TLS 初始化不能证明节点永久阻塞。

源码证据：`rust/crates/tidb-server/src/cluster_session_node/boot.rs` 先调用 `ConcurrentSqlNode::bind`，再注册信号处理器，最后输出 ready。Go master 的 `pkg/server/server.go::Run` 也先初始化并启动监听，再执行 `s.health.Store(true)`。因此应等待应用就绪状态，不能把端口可连接直接等同于应用已就绪。

独立修复已推送至 `origin/hparser-integration`：

- `1f89c30b65`：TCP 开放后有界等待 ready，检查进程退出。
- `9839a744e0`：四个集成入口共用 `rust/scripts/cluster-session-readiness.sh`。

共享等待保留 180 秒上限；提前退出和永久没有 ready 的节点仍失败。没有提前输出 ready、删除 ready 断言或跳过 SQL 对照。

历史红测 `/tmp/readiness-9f9a-red.log` 用同一回归驱动旧启动检查，退出 1，输出 `the Rust node never reported ready`，当时日志只有 `mysql_tls`。本轮直接核对旧提交代码，确认一次性 grep 与清理路径。

## 本轮验证

在仓库根目录运行四入口回归和 shell 语法检查，全部退出 0：

```bash
for runner in access-path analyze convergence repeatable-read; do
  bash rust/scripts/test-access-path-readiness.sh "run-realtikv-${runner}.sh" || exit
done
bash -n rust/scripts/cluster-session-readiness.sh \
  rust/scripts/test-access-path-readiness.sh \
  rust/scripts/run-realtikv-{access-path,analyze,convergence,repeatable-read}.sh
```

每个入口均覆盖延迟 ready、进程提前退出、永久无 ready。真实 access-path 命令：

```bash
RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 \
  ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
  ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
  ACCESS_PATH_KEEP_LOGS=/tmp/readiness-current-head-evidence \
  bash rust/scripts/run-realtikv-access-path.sh \
  > /tmp/readiness-current-head-replay.log 2>&1
```

结果为退出 0；`/tmp/readiness-current-head-replay.log:2093` 为 `the access-path differential passed`。流程覆盖 ANALYZE 前后 SQL 对照、统计加载和 50000 行表的读取验证。`/tmp/readiness-current-head-evidence/rust-node.log:8`：

```json
{"event":"cluster_session_node_ready","address":"127.0.0.1:47600","schema_version":68,"max_connections":0,"account_count":1,"skipped_tables":[],"stats_loaded":4,"stats_pseudo":67}
```

原脚本进程已回收并确认退出 0，本轮 TiUP tag `accesspath-17159-1789205999` 的进程已清理。未终止其他任务的服务。

本轮另收尾并推送 GROUP BY 契约提交 `28eff34dc7`：定向回归 1 passed、session 集成 310 passed、make lint 退出 0，采用仓库 Ready 验证范围。完整命令和 Go 行序、错误码证据见 `GROUP_BY_CONTRACT_FIX.zh-CN.md`。

## 剩余工作的边界

本轮没有改动 readiness 生产代码，已有修复经原始流程再次验证。其他三个 runner 仅执行启动回归，未执行其完整集成套件。原 chunk panic 的完整根因证据、分区 BatchPointGet fast path、shared-statistics 以及全量 Rust/Go/Bazel 门禁仍未全部完成；不能以本轮 access-path 通过宣称全部质量目标完成。

分区失败已有真实 Go 输出 `/tmp/partition-point-go.out`：分区表 IN 查询使用单个 Batch_Point_Get，并按分区定义顺序输出访问分区。Rust fast planner 当前直接拒绝 partitioned table，这属于实际实现缺口，后续必须实现分区路由和元数据传播，不能仅修改期望文本。
