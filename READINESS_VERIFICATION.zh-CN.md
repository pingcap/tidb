# Rust cluster-session readiness 根因与复验

## 当前结论

2026-09-12 核验代码为 `7903130ef784eaaa8f73bbb2ffbcae3a7c72bee6`，与当时远端 `pingcap/tidb:hparser-integration` 一致。readiness 竞争已由独立提交 `1f89c30b65` 修复，`9839a744e0` 将等待逻辑应用到四个集成入口。本轮没有重复修改服务端或 SQL 断言。

“端口已经监听、统计已经加载，但日志没有 ready”本身不能证明服务端死锁。旧脚本在端口开放后只 grep 一次，失败立即 exit，EXIT trap 随即杀掉仍在初始化的 Rust 节点。这解释了为何反复得到相同的截断日志。

## 源码依据

- Rust `rust/crates/tidb-server/src/sql_node.rs:1836`：`ConcurrentSqlNode::bind` 先执行 `TcpListener::bind`，随后启动内存限制和内存告警 runner。
- Rust `rust/crates/tidb-server/src/cluster_session_node/boot.rs:441`：等待 bind 返回，读取本地地址，安装 signal handler，之后才在第 455 行输出 `cluster_session_node_ready`。
- Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的 `pkg/server/server.go:518`：先 `initTiDBListener`；启动网络 listener 后，第 542 行才 `s.health.Store(true)`。因此 TCP 连通与应用完成初始化是两个不同的观察点。

修复后的 `rust/scripts/cluster-session-readiness.sh` 持续检查原始 ready 事件，最长等待 180 秒；节点退出立即失败；持续无 ready 仍失败并打印日志。四个使用入口为 access-path、analyze、convergence、repeatable-read。没有提前伪造 ready，也没有绕过初始化。

## 本轮验证

以下命令在 `/tmp/tidb-hparser-current` 执行：

```bash
for runner in access-path analyze convergence repeatable-read; do
  bash rust/scripts/test-access-path-readiness.sh "run-realtikv-${runner}.sh" || exit
done
make lint > /tmp/readiness-sept12-lint.log 2>&1
bash -n rust/scripts/cluster-session-readiness.sh \
  rust/scripts/test-access-path-readiness.sh \
  rust/scripts/run-realtikv-{access-path,analyze,convergence,repeatable-read}.sh
git diff --check
```

四入口回归全部退出 0，每个入口均验证：TCP 已通但 ready 延迟一秒可成功、进程提前退出被拒绝、存活但始终无 ready 被超时拒绝。`make lint`、shell 语法检查与 diff 检查均退出 0。修复前的红测证据已记录在 `BLOCKER_RESOLUTION_REPORT.zh-CN.md` 的“2026-09-10 readiness 竞争修复与证据更正”章节。

真实 Go/Rust access-path 对照命令：

```bash
RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 \
  ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
  ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
  ACCESS_PATH_KEEP_LOGS=/tmp/readiness-sept12-current-evidence \
  bash rust/scripts/run-realtikv-access-path.sh \
  > /tmp/readiness-sept12-current.log 2>&1
```

本轮真实对照退出 0，日志最后一行是 `the access-path differential passed`。`/tmp/readiness-sept12-current-evidence/rust-node.log:8` 记录：

```json
{"event":"cluster_session_node_ready","address":"127.0.0.1:47600","schema_version":68,"max_connections":0,"account_count":1,"skipped_tables":[],"stats_loaded":4,"stats_pseudo":67}
```

Go binary 自报的 Git Commit Hash 与上述固定 master SHA 一致。显式选择兼容的 PD/TiKV nightly，是因为该 Go master 使用的 `QueryRegion` RPC 不受默认 PD v8.5.6 支持；这是版本兼容要求，不能误报为 Rust readiness 故障。测试脚本已退出并清理本轮节点、playground 和临时数据；保留上述诊断日志。

## 本地目录与脚本来源

用户环境目录 `/Users/chenhuansheng/Documents/GitHub/db9-ai/tidb` 当前在 `release-8.5`，没有这些 Rust 脚本。实际修复与验证使用 `/tmp/tidb-hparser-current`。在其他旧 worktree 重跑前应先核对 HEAD 是否包含上述修复提交；本轮没有切换或覆盖用户目录。

这些脚本是 hparser-integration 分支中跟随 Rust 实现加入的验证工具，不是 Go master 的原始测试套件。`git log --diff-filter=A` 查得首次加入记录：

| 脚本 | 首次提交 |
| --- | --- |
| `run-realtikv-access-path.sh` | `0e229647f5` |
| `run-realtikv-analyze.sh` | `610c123952` |
| `run-realtikv-repeatable-read.sh` | `c2820d39b9` |
| `run-realtikv-convergence.sh` | `f7481cbd67` |

## 验证边界

本轮针对 readiness 及其直接依赖的 access-path。其余三个入口仅运行启动回归，不代表其完整 SQL 集成套件通过。整个 Rust failed cases 目标仍未完成；既有串行 session 基线为 1652 passed / 63 failed / 209 ignored，本轮未重跑该全量套件。剩余失败应按实际 SQL、执行计划或执行器差异处理，不能再统一归因于 readiness。
