# Rust cluster-session readiness 复核

## 根因和修复

启动脚本在 TCP 监听成功后仅检查一次 `cluster_session_node_ready`。监听早于应用初始化完成，因此检查存在竞争；检查失败后的清理又会终止尚在启动的节点。端口已监听、统计已加载、没有 ready 的日志不能单独证明服务端死锁。

Go source of truth 为 `/tmp/tidb-go-master-oracle`，固定 master commit `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。`pkg/server/server.go:387` 创建监听，`:542` 设置 health=true，同样区分监听和应用可用。Rust `rust/crates/tidb-server/src/cluster_session_node/boot.rs` 在 bind、shutdown handler 初始化后输出真实 ready 事件。

修复已独立推送到 hparser-integration：

- `1f89c30b65`：access-path 等待真实 ready。
- `9839a744e0`：access-path、analyze、convergence、repeatable-read 共用 `rust/scripts/cluster-session-readiness.sh`。

共用 helper 每秒检查事件，最多等待 180 秒；进程提前退出立即失败，持续无 ready 仍失败。未提前输出事件，未降低 SQL 对照断言。

## 本轮验证

基线为 `61a737a77b`，2026-09-12 已重跑四入口回归，全部退出 0：

```bash
for runner in access-path analyze convergence repeatable-read; do
  bash rust/scripts/test-access-path-readiness.sh "run-realtikv-${runner}.sh" || exit
done
```

每个入口均覆盖延迟 ready、提前退出和永久无 ready。`make lint` 退出 0，日志 `/tmp/fromless-lint.log`。

真实集群本轮复验命令：

```bash
RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 \
  ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
  ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
  ACCESS_PATH_KEEP_LOGS=/tmp/readiness-current-final-evidence \
  bash rust/scripts/run-realtikv-access-path.sh > /tmp/readiness-current-final.log 2>&1
```

本轮退出 0，日志结尾为 `the access-path differential passed`，覆盖 ANALYZE 前后对照。节点日志 `/tmp/readiness-current-final-evidence/rust-node.log:8` 包含真实 ready，schema_version=68、stats_loaded=4。脚本已执行清理。此次运行同时包含后续 TableDual 空 schema 修复，见 `FROMLESS_SUBQUERY_FIX.zh-CN.md`。

## 脚本来源与剩余范围

这些 `rust/scripts/run-realtikv-*.sh` 是 hparser-integration 的 Rust 对 Go 集成 runner，不能当作 Go master 原生测试入口。`git log --diff-filter=A -- rust/scripts/run-realtikv-access-path.sh` 显示 access-path 初次引入提交为 `0e229647f5`。

readiness 已有修复和可重复验证，不应继续列为所有 failed cases 的共同外部 blocker。其余 SQL 失败和全量 Rust/Go/Bazel 门禁仍需各自验证；另外三个入口的完整 RealTiKV 套件本轮未运行。
