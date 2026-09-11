# Rust 集成测试 Blocker Resolution

## 2026-09-11 convergence 全局授权观察收紧后通过

旧 `wait_for_rust_grant "UPDATE"` 会匹配既有 conv.orders 表级 UPDATE，不能
证明刚由 Go 授予的全局 UPDATE 已同步。现在匹配完整
`GRANT SELECT,INSERT,UPDATE ON *.* TO 'rustmade'@'%'`。回归从生产脚本提取
函数与调用，先仅返回旧表级 UPDATE，再返回新全局授权。旧实现第一轮就
误报成功（`/tmp/convergence-global-grant-red.log`）；修复后必须读取第二轮
才成功（`/tmp/convergence-global-grant-green.log`）。未弱化任何授权断言。

```bash
bash rust/scripts/test-convergence-global-grant.sh
bash -n rust/scripts/test-convergence-global-grant.sh rust/scripts/run-realtikv-convergence.sh
make lint
CONVERGENCE_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
CONVERGENCE_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
bash rust/scripts/run-realtikv-convergence.sh
```

上述均退出 0，lint `/tmp/convergence-global-grant-lint.log`，真实门禁
`/tmp/convergence-strict-master.log`：固定 master fdfadb96b2cf 对照下，wide SQL、
事务跨节点读回、CREATE/ALTER/DROP TABLE、账户密码两端登录、双向全局与
scoped grant、watch 事件和 DROP USER 均通过，脚本完成清理。
这是 convergence 门禁的完成证据；transport-retry 等剩余原始门禁仍未完成。

## 2026-09-11 DROP USER 避免重复账户持久化

真实 red `/tmp/convergence-column-grant-master.log` 的 DROP USER 返回
`no statement snapshot is bound`。cluster account writer 已在自己的事务中
读写账户，但 Session 修改 scratch registry 时又执行普通连接 mysql.user
mirror DELETE，形成第二条持久化路径。Go master
`pkg/executor/simple.go:2535-2660` 使用 system session 的内部事务删除账户和
授权，不通过用户查询连接重复提交。现在 cluster 入口显式委托账户存储，
仅在该次执行中禁止内部 mysql.user mirror；权限检查、scratch 变更、原有
account writer 原子提交与 live registry 发布顺序保留。委托状态无论返回
成功或错误都会恢复，普通 Session 的 mirror 默认行为保留。

嵌入式真实存储回归修复前失败：残留 mirror INSERT 在后续 SELECT 触发
NotExist 唯一键断言，`/tmp/account-drop-red.log`，1.24 秒；这是相同重复
持久化在嵌入式栈上的表现，不冒充完全相同的缺 snapshot 消息。修复后
`/tmp/account-drop-green.log` 通过，1.26 秒，检查账户及 mysql.db 授权行
删除、重复 DROP 返回 1396、随后重新 CREATE 成功。Session tests_grants
102/102，包含委托后 mirror 恢复；server 全量 435/435，48.89 秒。

Ready 命令：

```bash
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_grants -- --nocapture
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all
make lint
git diff --check
```

日志 `/tmp/account-drop-server-full.log`、`/tmp/account-delegation-session.log`、
`/tmp/account-drop-lint.log`。固定 master/nightly convergence
`/tmp/convergence-drop-master.log` 全程退出 0，Go 读回已删除账户 COUNT=0，
脚本完成清理。反向全局授权旧断言的覆盖缺口另行收紧，不据此宣称全部原始
门禁完成，也不宣称完整 Go 账户执行器 package 已转译。

## 2026-09-11 列级授权输出断言与后续失败

固定 master `pkg/privilege/privileges/cache.go:1973-2012` 将 ColumnName
直接放入 `SELECT(...)`，不加反引号。将 convergence 的列级授权期望修正为
`GRANT SELECT(customer) ON ...`。真实 red `/tmp/convergence-allocator-master.log`
等满 60 秒且最后输出正是无反引号形式；修改后
`/tmp/convergence-column-grant-master.log` 该断言通过，也观察到了 Go 修改后的
全局 SELECT,INSERT,UPDATE 及 scoped INSERT 授权。bash -n、git diff --check、
make lint 通过（`/tmp/convergence-column-grant-lint.log`）。完整命令沿用固定
master + nightly 的 convergence 命令，未改生产权限行为。

完整 convergence 仍失败：DROP USER 返回 1105，内部 unique index lookup
报告 `no statement snapshot is bound to this session's cluster storage`。
此外反向全局授权观察目前只匹配 UPDATE，可能提前匹配已有表级 UPDATE，
应在后续收紧为完整全局 grant 字符串，不能仅据该 marker 声称 watch 验证充分。

原 transport-retry 本轮直接运行，独立 target 确实仍存在，无需改为 all。
`RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-transport-retry.sh`
失败于 `realtikv_transport_retry.rs:399`：同一 lazy response 在 leader 停止后
恢复时报 `Source("query deadline exceeded")`，运行 7.28 秒，日志
`/tmp/transport-retry-current.log`。这不是入口或 readiness 错误。两脚本均已
退出并执行清理；剩余工作继续按具体失败修复，整体目标未完成。

## 2026-09-11 账户 row-ID 分配使用独立事务

真实 red `/tmp/convergence-account-master.log` 的 CREATE USER 在 TID:4
allocator 元数据键上与 Go 事务发生 9007。Go master
`pkg/meta/autoid/autoid.go:925` 在 `kv.RunInNewTxn(..., true, ...)` 中预留 ID，
与账户行事务独立。Rust 原 account writer 将 allocator watermark 放进账户
mutation 集合，扩大了账户事务的冲突范围，并使取消账户事务回收已发出的 ID。

账户 planner 现在要求独立 reservation 回调；两个生产入口（真实账户写入、
启动账户 seed）复用现有 ClusterAutoIdStore 的独立 rebase/reserve 和重试。
按本次缺失行数分配连续句柄，保留旧 bootstrap 无 watermark 时的最低句柄
修复。账户行和索引仍在原事务中原子提交；ID 预留即使账户提交失败也保留。
本改动不声明 Go autoid 整包转译完成，也未增加测试侧写冲突重试。

回归 `account_rows_do_not_commit_allocator_metadata` 修复前失败，0.13 秒，
`/tmp/account-allocator-red.log`。修复后账户写入 13/13，包含放弃计划后 ID
不复用、peer 更新 allocator 后提交账户行不会覆盖 watermark 的测试。
服务端全量 434/434，51.68 秒；日志 `/tmp/account-allocator-server-full.log`。
Ready 验证命令：

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-exec --test all cluster_account_write_source -- --nocapture
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all
make lint
git diff --check
```

真实固定 master/nightly convergence `/tmp/convergence-allocator-master.log`
通过 CREATE USER、Rust/Go 两端密码登录、表级和列级授权写入。随后旧断言
等待带反引号的 SELECT(`customer`)，Go 实际输出 SELECT(customer)，60 秒
后失败并清理。Go `privileges/cache.go:1973-2012` 直接写 ColumnName，证明
这是下一类字符串断言问题，完整 convergence 尚未通过。lint 退出 0，日志
`/tmp/account-allocator-lint.log`。

## 2026-09-11 convergence 账户操作身份与新暴露的冲突

固定 master 对照 `/tmp/convergence-master-oracle/create-user.out`、
`drop-user.out`、`show-grants.out` 分别证明仅持有 conv.* 权限的 appuser
不能 CREATE/DROP USER（1227）或查看他人权限（1044）。原 runner 却要求
这些操作成功。账户管理和权限 watch 观察改用已有 root 连接，保留普通
appuser CREATE/DROP USER 必须返回 1227 的断言，其他跨节点登录和 scoped
grant 断言不变。原 red `/tmp/convergence-alter-master-nightly.log` 的 CREATE
USER 返回 1227；修正后 `/tmp/convergence-account-master.log` 已通过普通用户
拒绝检查并进入 root 的账户持久化，随后出现新的 9007 冲突。因此该 runner
仍未整体通过，不能声称账户写入缺陷已解决。

新冲突键为 mysql 表 4 的 auto-table-ID 元数据键（字节中包含 mDB:1/TID:4），
需继续对照 Go autoid 分配与内部事务重试。当前 Rust
cluster_account_write::first_free_row_id/publish_row_id_watermark 将 allocator
更新与账户行放在同一事务；本轮不通过测试重试隐藏该冲突。已完成 bash -n、
git diff --check 和 make lint（`/tmp/convergence-account-lint.log`）。
运行固定 master/nightly 的完整命令同上一节，账户写入回归尚未完成。

## 2026-09-11 convergence ALTER 按固定 master 验证

旧断言要求 Rust 拒绝 ADD COLUMN，真实 red 见
`/tmp/convergence-shared-readiness.log`。固定 master fdfadb96b2cf 的
unistore 对照 `/tmp/convergence-master-oracle/{setup,alter}.sql` 及 `alter.out`
确认 conv.* 权限允许 ADD COLUMN，旧行新列 NULL，更新后为 42。
runner 改为严格验证这些值并由 Go peer 读回，未通过拒绝合法 SQL 迁就旧断言。
增加 CONVERGENCE_TIDB_SERVER / CONVERGENCE_CLUSTER_VERSION，与 access-path
现有入口一致，打印真实基准版本。master 与 PD 8.5.6 不兼容 QueryRegion，
故使用已安装 nightly PD/TiKV；不能将旧 PD 超时算作 Rust readiness 失败。

Ready 验证：bash -n、git diff --check、make lint 均通过，lint 日志
`/tmp/convergence-alter-lint.log`。真实命令：

```bash
CONVERGENCE_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
CONVERGENCE_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
bash rust/scripts/run-realtikv-convergence.sh
```

`/tmp/convergence-alter-master-nightly.log` 证明 ALTER 两个断言通过，随后账户
阶段返回 1227，因为 appuser 没有 CREATE USER。这与固定 master 的
`create-user.out` 一致；属于下一项 fixture 身份问题，未宣称整套通过。

## 2026-09-11 Python 3.9 认证错误导致的 packet EOF

repeatable-read 在 ready 后失败的根因是共享 raw MySQL 客户端调用
`zip(stage_one, challenge, strict=True)`，本机 Python 3.9.6 不支持该关键字。
客户端在发送认证包前退出，服务端随后记录 packet EOF；不是事务或 ready 死锁。
对两个 SHA-1 固定长度摘要按字节索引异或，与固定 Go master
`pkg/parser/auth/mysql_native_password.go::CheckScrambledPassword` 的协议说明和
实现一致。回归直接采用该 master `mysql_native_password_test.go::TestCheckScramble`
的密码、salt 和完整 20 字节 token，并覆盖空密码。

`python3 rust/scripts/test-mysql-native-password.py` 修复前 1 error，报相同
TypeError（`/tmp/native-password-python39-red.log`），修复后 2 passed
（`/tmp/native-password-python39-green.log`）。Ready 验证另包括：

```bash
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-repeatable-read.sh
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-multi-statement-txn.sh
make lint
git diff --check
```

`/tmp/repeatable-read-python39.log`：事务保持 BEGIN 时 balance=100，并发事务
提交 999 后重复读取仍为 100，自身提交返回 9007，Go 读回 999。
`/tmp/multi-statement-python39.log`：悲观事务 read-your-writes、另一连接读取旧值、
NOWAIT 3572、乐观竞争 9007、文本 UPDATE/INSERT/DELETE、Go 读回均通过；
cluster 7683989829543214242。两门禁不再因认证阶段 EOF 失败；集群沿用原脚本
v8.5.6，协议向量源自固定 master，不称为 master 全套差分完成。
lint `/tmp/native-password-lint.log` 退出 0。整体目标仍有其他门禁未完成。

## 2026-09-11 三个遗漏 runner 的 readiness 竞争

发现 convergence、analyze、repeatable-read 仍在 TCP 端口开放后只 grep 一次
ready。与此前 access-path 相同，不能据此推断服务端死锁。将四个 runner
统一接到 `cluster-session-readiness.sh`：等待原始 ready 事件，180 秒上限，
进程提前退出立即失败。Go master `pkg/server/server.go::Run` 先建 listener、
后发布 health 的依据保持不变，没有更改服务端启动语义。

回归读取每个生产脚本的实际启动调用段。三个遗漏脚本修复前均报
`the Rust node never reported ready`，日志仅有 mysql_tls；日志分别为
`/tmp/{convergence,analyze,repeatable-read}-readiness-red.log`。
修复后四个入口均通过延迟 ready、提前退出、持续无 ready 三个场景：
`/tmp/shared-readiness-green.log`。Ready 验证：

```bash
for name in access-path convergence analyze repeatable-read; do
  bash rust/scripts/test-access-path-readiness.sh run-realtikv-${name}.sh || exit
done
bash -n rust/scripts/cluster-session-readiness.sh rust/scripts/test-access-path-readiness.sh rust/scripts/run-realtikv-{access-path,convergence,analyze,repeatable-read}.sh
make lint
git diff --check
```

真实运行 `/tmp/convergence-shared-readiness.log` 已通过 ENUM、join、聚合、
子查询、窗口、Rust 提交由 Go 读回、双向 CREATE/DROP 后读写，停在旧断言
`ALTER was accepted, but this mode must refuse it`。
`/tmp/repeatable-read-shared-readiness.log` 明确输出 ready（46700，schema 57），
随后 Python 3.9 在认证 token 的 `zip(strict=True)` 抛 TypeError，服务端 EOF
是客户端退出的结果。两个脚本都结束并清理，不能将后续失败再归为 readiness。
真实集群为脚本原有 v8.5.6，未冒充固定 master 差分验证；analyze 真实全程未在
本次重跑。整体目标继续推进。lint 日志 `/tmp/shared-readiness-lint.log`。

## 2026-09-11 optimistic 2PC 异步 secondary 完成证据

原真实测试在 `secondary_publications.len() == 1` 失败（实际 0），
`/tmp/optimistic-2pc-restored.log`。固定 Go master 的 client-go
`v2.0.8-0.20260903102657-08cbf831121a/txnkv/transaction/2pc.go:1075-1118`
先提交 primary，再通过 `spawnWithStorePool` 提交 secondaries；因此同步
返回的 receipt 不能证明后台批次已经完成。

增加按事务显式订阅的 detached completion 通道。后台只有实际解码响应后
才通知，保留 region/key error；admission、transport、deadline 失败报告错误。
默认没有订阅，不等待观察者，不改变 primary 返回时机和 secondary 失败指标。
真实测试在 commit 返回之后等待全部订阅结束，严格要求一个成功的 secondary
publication，再执行原有 marker、读回、重试和回滚检查。

延迟响应回归先红：`/tmp/detached-observer-red.log`，完成通知超时，2.03 秒。
修复后同模块 10 passed，`/tmp/detached-observer-green.log`。分别订阅两笔事务，
验证第一笔响应暂停期间第二笔独立完成、暂停的第一笔无提前完成事件、释放后
收到真实响应，同时保持原有 client authority 不被网络等待锁住的断言。

Ready 验证命令：

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv --test all async_commit_one_pc_source -- --nocapture
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-optimistic-2pc.sh
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all
make lint
git diff --check
```

真实场景退出 0，日志 `/tmp/optimistic-2pc-observed.log`：cluster
7683988181398460413，primary region 28、secondary region 10；分裂、leader
转移、提交读回、冲突回滚、新旧锁检查全部通过，脚本完成严格进程/端口/数据
清理。lint 日志 `/tmp/detached-observer-lint.log`。该修复解决这一门禁的
完成证据缺口，不是全部原始集成门禁或整个 client-go package 的完成声明。

## 2026-09-11 PD batch topology 使用同一物理连接检验 stream 隔离

先修复旧 realtikv_replica_read target 为 all 加完整模块测试名，原 target
失败证据 `/tmp/replica-read-target-red.log`。随后真实运行到 target freeze /
request published 阶段，在 realtikv_replica_read.rs:814 报 3 != 2，
`/tmp/pd-batch-topology-restored.log`。此前的地址级 channel version 来自前
一个请求，默认四连接池轮询后失败请求实际用了另一物理连接。

固定 Go master 对应 client-go
`v2.0.8-0.20260903102657-08cbf831121a/config/client.go:210` 默认连接数 4，
`internal/client/conn_pool.go:198` 轮询连接；地址级最近版本并不等于某次
请求的版本。本测试目标是同一物理连接上的 direct/forwarded stream 隔离，
故在该测试显式使用 with_connection_count(1)，生产默认保持 4。保留全部
版本相等、单次失败、无 transport resend、direct sibling 生存、精确 peer
就绪及同地址重启读回断言，未将不等改为宽松比较。

Ready 验证：

```bash
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-pd-batch-topology.sh
# /tmp/pd-batch-topology-single-channel.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all
bash -n rust/scripts/run-realtikv-pd-batch-topology.sh
make lint
git diff --check
```

业务断言通过：GetPrevRegion adjacency；physical proxy 127.0.0.1:58160
转发 follower 127.0.0.1:58161，leader store 1 不变；channel 1 的 route
generation 1 恰好失败一次，调用方 retry generation 2 成功。lint 退出 0，
`/tmp/pd-batch-single-channel-lint.log`。脚本终态退出 0，tag
realtikv-pd-batch-87799-1789067451 的进程、data 和 phase 均已清理。

## 2026-09-11 replica-read 原始 RealTiKV 门禁恢复

旧 realtikv_replica_read target 已合并，原调用退出 101，
`/tmp/replica-read-target-red.log`。改为 all target 中完整测试名
`realtikv_replica_read::follower_policy_reaches_a_live_nonleader_voter`，
保留 --ignored --exact、peer 身份与有效响应的原断言。

Ready 验证命令为
`RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-replica-read.sh`；
`bash -n rust/scripts/run-realtikv-replica-read.sh`、`make lint`、
`git diff --check` 均通过。真实结果 `/tmp/replica-read-restored.log`：
region_id=14，leader_peer_id=15，post_leader_peer_id=15，selected_peer_id=25，
selected_store_id=4，selected_address=127.0.0.1:48160，replica_read=true，
stale_read=false，usable_response=true。所属 tag realtikv-replica-read-85513
进程和数据已清理；lint 为 `/tmp/replica-read-harness-lint.log`。
只修复测试入口，不改变 follower 选择或读取协议。

## 2026-09-11 region-retry 原始 RealTiKV 门禁恢复

旧 `cargo test -p difftest-transaction-tests --test realtikv_region_retry -- --list`
退出 101，目标已合并为 all，`/tmp/region-retry-target-red.log`。
脚本改为 all target 加完整模块测试名，保留 ignored/exact、原阶段握手和
所有成员发现/PD 移除/region leader 切换断言。

Ready 验证：

```bash
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-region-retry.sh
bash -n rust/scripts/run-realtikv-region-retry.sh
make lint
git diff --check
```

均退出 0；`/tmp/region-retry-restored.log` 证明同一进程的 PD 路由
http://127.0.0.1:26379 → http://127.0.0.1:26382，TiKV leader
127.0.0.1:44160 → 127.0.0.1:44162。tag realtikv-region-retry-83784
的所属进程、data 和 phase 已清理。lint 为
`/tmp/region-retry-harness-lint.log`。此修改只修复测试入口，未变更协议行为。

## 2026-09-11 optimistic-2pc 清理恢复，继续追踪异步 secondary 证据

路径回归从生产 TAG/validate_owned_paths 提取代码，旧 campaign28 前缀拒绝
本脚本 realtikv-optimistic-2pc-<pid>，`/tmp/optimistic-paths-red.log`。
现仅允许数字 PID 标签，同时要求 data 和 phase 路径完全匹配。
`bash rust/scripts/test-optimistic-2pc-paths.sh`、原 `--self-test-cleanup`、
`bash -n`、`make lint`、`git diff --check` 均退出 0。
lint 日志 `/tmp/optimistic-paths-lint.log`。

实际执行：

```bash
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-optimistic-2pc.sh
# 退出 1，/tmp/optimistic-2pc-restored.log
```

清理已成功，tag realtikv-optimistic-2pc-81835 的进程/data/phase 已移除。
真实测试提交成功后在 optimistic_2pc_realtikv_source.rs:263 失败：同步
receipt.secondary_publications 为 0，旧断言要求 1。未修改或删除该断言。
当前源代码已将 classic 2PC secondary 交给 detached task，同步 receipt
不能代表其完成状态。固定 Go master 所依赖 client-go
`v2.0.8-0.20260903102657-08cbf831121a/txnkv/transaction/2pc.go:1099`
同样 spawnWithStorePool 后立即返回，不能为满足断言强制同步 secondary。

下一步需为实际异步完成提供可验证的 publication 证据，同时保持原提交、
split/leader retry、回滚和所有 readback 断言。此提交只解决清理失败，
该 RealTiKV 门禁仍为红色，不记为通过。

## 2026-09-11 prepared-write 原始 RealTiKV 门禁恢复

脚本创建 realtikv-prepared-write-<pid>，清理却只允许旧 campaign28 前缀；
Cargo 集成测试已合并为 all target，脚本仍指定旧独立 target。
修复路径检查为精确数字 PID 标签，保留目录完全相等的约束；入口改为
`--test all prepared_write_persists_realtikv_source::prepared_insert_and_update_persist_through_one_shared_authority -- --ignored --exact --nocapture`。
未修改 SQL、持久化断言、authority 重建或 receipt 检查。

新增 test-prepared-write-paths.sh 从生产脚本提取真实 TAG 和路径检查函数：
修复前 production tag rejected（`/tmp/prepared-write-paths-red.log`）；
修复后接受自身标签、拒绝无关目录和路径穿越。原清理自测保留且通过，
验证清掉本次进程而无关进程仍存活。

Ready 验证：

```bash
bash rust/scripts/test-prepared-write-paths.sh
bash rust/scripts/run-realtikv-prepared-write.sh --self-test-cleanup
bash -n rust/scripts/run-realtikv-prepared-write.sh rust/scripts/test-prepared-write-paths.sh
RUSTFLAGS='' RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-prepared-write.sh
make lint
git diff --check
```

均退出 0。真实 PD/TiKV v8.5.6、3 TiKV；结果日志
`/tmp/prepared-write-restored.log`：cluster_id=7683983192718641636、
table_id=528491、handle=10、final_balance=107、write_authority_id=1、
restart_authority_id=2。tag realtikv-prepared-write-80814 的 PD/TiKV 进程
和目录已清理。`/tmp/prepared-write-harness-lint.log` 为 lint 证据。
此为原命令的启动/清理 harness 修复，不涉及 Go SQL 语义变更，也不等同于
完整 Go package 移植完成；其余 RealTiKV 和 Go integration 门禁仍待推进。

## 2026-09-11 INSERT 快照测试显式选择立即检查模式

最后的 `an_insert_reads_for_its_uniqueness_check_and_publishes_at_that_read`
独立失败，`/tmp/insert-inplace-red.log`：期待一个执行阶段快照，实际为零。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/sessionctx/vardef/tidb_vars.go:1603` 默认 ConstraintCheckInPlace=false；
`pkg/executor/insert.go:330` 在关闭立即检查或悲观事务时选择 DupKeyCheckLazy。
原测试把“INSERT 一定先读取唯一性”当作默认行为，前提不正确。

保留原快照数和返回值断言，只在测试中显式设置
`tidb_txn_mode='optimistic', tidb_constraint_check_in_place=ON`，覆盖其本意的
执行阶段立即检查。仅打开约束检查仍因悲观模式失败，日志
`/tmp/insert-inplace-green.log` 实际为 red；两个前提均设置后 1 passed，
0.05 秒，`/tmp/insert-optimistic-inplace-green.log`。未修改生产默认值。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib an_insert_reads_for_its_uniqueness_check_and_publishes_at_that_read
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 完整 434 passed / 0 failed，45.10 秒，退出 0；/tmp/server-inplace-final.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# fmt、lint、diff check 退出 0，/tmp/insert-inplace-{fmt,lint-final}.log
```

## 2026-09-11 fix52592 不再一律禁止完整主键 MaxTS

原回归独立失败，`/tmp/fix52592-max-ts-red.log`，0.04 秒：ON hint 下
完整主键查询被强制归类 Unknown。Go master
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/core/point_get_plan.go:83` 的 fix52592 只关闭 TryFastPlan；
`tests/pointget/point_get_plan_test.go:383` 验证随后得到单点 TableReader；
`pkg/planner/core/common_plans.go:1704` 明确允许完整主键单点 TableReader
使用 MaxTS。不能将“关闭快速规划”等同于“禁止点读时间戳优化”。

现在保留已有完整主键、非分区、无残余条件等 AST 证明，不再因 fix 开启
一律拒绝。LIMIT 仍保守：快速规划关闭后可能保留 Limit 算子，当前 AST
判定器无法证明实际物理根，不扩大这部分准入。
原测试所有会话设置、hint ON/OFF、非法 hint first-wins、prepared 断言均
未改，修复后 1 passed，0.06 秒，`/tmp/fix52592-max-ts-green.log`。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib fix_52592_preserves_max_ts_for_a_complete_clustered_key
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 完整 433 passed / 1 failed，45.07 秒，/tmp/server-fix52592-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# scoped test、fmt、lint、diff check 退出 0；完整 server 退出 101
```

最后一条是 INSERT 立即唯一性检查测试；其他原始外部集成门禁仍未全部完成。

## 2026-09-11 SLI 测试启用所依赖 crate 的 failpoints

默认 server 测试中的 txn_write_throughput_sli_matches_source 失败并非已证实
的统计错误：tidb-util 的 SLI failpoint 受该 crate 的 failpoints feature 控制。
测试直接设置 fail::cfg，但 server dev-dependencies 只启用了 fail 库本身，
没有启用 tidb-util 的 cfg，因此 FinishExecuteStmt 正常重置状态。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/executor/executor_failpoint_test.go:595` 显式启用 CheckTxnWriteThroughput。
先用 `cargo test --manifest-path rust/Cargo.toml -p tidb-server --features failpoints
--lib txn_write_throughput_sli_matches_source` 验证全部原断言通过（0.05 秒，
`/tmp/write-sli-failpoints.log`）。现仅在 server dev-dependencies 为 tidb-util
启用 failpoints，使默认测试命令具有该测试的必要环境，生产依赖默认不变。
未跳过用例、删除断言或让生产 SLI 永远不重置。

Ready 验证：默认同名测试命令日志 `/tmp/write-sli-default-green.log`；
`make lint` 退出 0，`/tmp/write-sli-lint.log`。完整 server 基准待本轮汇总。

## 2026-09-11 非唯一索引缓存测试检查普通 SELECT 描述

上一完整 server 回归为 430 passed / 4 failed，45.02 秒，
`/tmp/server-prepared-bound-shape-baseline.log`。其中
`cached_prepared_index_lookup_uses_one_timestamp` 在执行前错误要求
非唯一索引 ia(a) 对应 point_get_plan。Go master
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/core/point_get_plan.go:617` 明确跳过 !idxInfo.Unique；
Rust 现在保留普通 select_plan 描述，首次 EXECUTE 生成缓存物理树。

仅将描述存在性断言改为 select_plan，保留三组参数 7/8/9 的多行、单行、
空集结果，保留每次只有一个普通快照、连接读资源释放，以及最终
@@last_plan_from_cache=1 的真实缓存命中断言。未修改生产计划选择。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib cached_prepared_index_lookup_uses_one_timestamp
# 1 passed，0.05 秒，/tmp/cached-index-descriptor-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/cached-index-descriptor-{fmt,lint}.log
```

尚未宣称其他失败或整个原始门禁通过。

## 2026-09-11 缓存 SELECT 用本次绑定值判定读取形状

`a_bounded_single_row_cluster_scan_keeps_its_statement_timestamp` 独立失败：
`SELECT v FROM t WHERE id = ? LIMIT 1` 申请了普通时间戳，而完整主键点读应
使用 MaxTS，`/tmp/prepared-bound-shape-red.log`，0.05 秒。
缓存 SELECT 分支把未绑定的 PREPARE 模板交给读取形状判定，无法证明完整键。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/sessiontxn/isolation/optimistic.go:144-148` 检查 Execute.Plan，
`pkg/planner/core/common_plans.go:1687` 检查本次物理计划是否为完整点范围。
Rust 本次修复仍使用现有读取形状判定器，但输入改成本次参数绑定后的 AST；
不重解析 SQL，不改缓存物理计划执行。该绑定副本也供已有预加锁键判定复用。

原回归不变：`id >= ? LIMIT 1` 的范围查询保持一个普通时间戳，
`id = ? LIMIT 1` 完整点查询使用 MaxTS；每条 PREPARE 以 1、2 两组参数执行，
验证返回值、读取结束和连接关闭。修复后 1 passed，0.05 秒，
`/tmp/prepared-bound-shape-green.log`。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib a_bounded_single_row_cluster_scan_keeps_its_statement_timestamp
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/prepared-bound-shape-{green,fmt,lint}.log
```

完整 server 回归见 `/tmp/server-prepared-bound-shape-baseline.log`，
其他失败仍单独跟踪。
边界：增加普通缓存 SELECT 的 AST 绑定副本成本；尚未把现有 AST 读取形状
判定器整体替换为 Go 的物理计划判定器，fix52592 的独立失败仍需处理。

## 2026-09-11 mock transaction 接入点写入返回值锁接口

四个事务测试在点写入阶段报 1105：only a pessimistic transaction locks
statement keys。独立红测 `an_explicit_transaction_holds_one_transaction_for_every_statement`
为 0 passed / 1 failed，0.04 秒，`/tmp/mock-prelock-red.log`。
MockSessionTransaction 已实现带断言的普通锁入口，却未实现新增的
lock_staged_keys_with_values，落入 trait 的拒绝默认实现。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/executor/point_get.go:602,615,621` 在 e.lock 时 InitReturnValues 并缓存
返回行。Rust 真实 SessionTransaction 已接入该接口，故本次只补 mock 接口，
复用其现有锁定/重试结果；mock 数据仍来自内存快照。未给生产接口增加无锁
成功回退，未修改默认事务模式，未弱化失败断言。

排除了一项假设：临时 BEGIN OPTIMISTIC 点更新探针在生产修改前即通过
（`/tmp/optimistic-prelock-red.log` 名称虽带 red，实际为 green）。上游不会为
该模式产生预加锁请求，不能据此宣称发现乐观事务缺陷；探针已删除。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 429 passed / 5 failed，45.49 秒，退出 101；/tmp/server-mock-prelock-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# fmt、lint、diff check 均退出 0，/tmp/mock-prelock-{fmt,lint}.log
```

四个原失败用例的参数绑定、事务生命周期、schema move 及 mock 冲突断言
均通过。`an_explicit_transaction_does_not_lock_what_go_would_lock` 是既有
mock 能力限制的刻画，不是 Go 悲观锁行为等价证据；本次不将其作为真实
存储的锁排他性验证，也不声称整个事务 package 已完成。

## 2026-09-11 unistore LIKE escape 按协议整数求值

`auto_analyze_fills_missing_partition_statistics_like_go` 的两条自动任务已经
正确写入并 finished，但带 LIKE 过滤的 COUNT 为 0。探针显示 SELECT 投影
中的 LIKE 对两行均返回 1；WHERE 下推到 cop Selection 后却不匹配。
证据 `/tmp/missing-partitions-{jobs-red,filter-probe}.log`。

根因是 Rust unistore `SimpleSig::Like` 用 eval_bytes 读取 escape，而 protobuf
实际携带整数 92，导致表达式返回 NULL。Go master
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/expression/builtin_like.go:77,85,94` 使用 EvalInt 后取 byte(escape)。
Rust protobuf 生产端 `tidb-expr/src/pb_predicate.rs::string_like_to_pb`
已经正确编码整数，无需修改。

修复 unistore 第三个参数走整数求值，保留 NULL 并传播错误，以低八位作为
escape。旧单测错误地传入字符串反斜杠，现改为协议真实的 Int(92)，修复前
None != Some(1)，`/tmp/cop-like-escape-red.log`。额外检查 Int(348) 的 byte
截断和 NULL；保留大小写 collation、转义百分号正反匹配。原始 server SQL
断言不变，完整通过；临时探针已全部删除。

Ready 验证：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-unistore --lib
# 160 passed / 13 既有 ignored，2.01 秒，/tmp/cop-like-unistore-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib auto_analyze_fills_missing_partition_statistics_like_go
# 1 passed，3.94 秒，/tmp/missing-partitions-jobs-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/cop-like-{fmt,lint}.log
```

完整 server 回归 **425 passed / 9 failed**，44.76 秒，
`/tmp/server-cop-like-baseline.log`；原始自动分析分区用例通过。
本修改只修复 escape 类型契约，未声称补齐现有 LIKE 的所有 Unicode/collation
语义或完成整个 Go expression package；其他失败和外部集成门禁继续推进。

## 2026-09-11 历史统计 GC 使用与写入相同的本地 DATETIME

`clear_outdated_history_stats_uses_the_go_retention_duration` 独立复现：
过期记录仍保留，`/tmp/history-gc-red.log`，3.26 秒。根因是历史统计的
create_time 写入本地 DATETIME(6)，清理却用 UTC TIMESTAMP 计算 cutoff。
上海时区延迟清理，洛杉矶时区提前清理；均与索引扫描和保留时间解析无关。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 证据：
`pkg/meta/metadef/system_tables_def.go:381,392` 两表均为 DATETIME(6)；
`pkg/statistics/handle/history/history_stats.go:165,201` 分别用内部会话
NOW(6) 和本地 time.Now() 写入；`storage/gc.go:192` 用 NOW() 减保留期；
`pkg/expression/builtin_time.go:2672,2752` 明确 NOW() 截断到整秒并转换
到会话时区。

修复仅将历史 GC cutoff 改用内部 SYSTEM 会话的本地 DATETIME 整秒时间。
UTC TIMESTAMP 辅助函数及其他清理调用保持原语义。回归在独立子进程中
分别设置 UTC、Asia/Shanghai、America/Los_Angeles，避免修改并发测试进程
环境。保留原过期删除断言，新增一小时保留期下新记录不得删除的断言。
修复前上海过期保留（`/tmp/history-gc-zones-red.log`），洛杉矶新记录误删
（`/tmp/history-gc-west-red.log`，1.23 秒）；修复后三个时区全部通过。

Ready 验证命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib clear_outdated_history_stats_uses_the_go_retention_duration
# 1 passed，内部执行三个时区，/tmp/history-gc-zones-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/history-gc-{fmt,lint}.log
```

完整 server 回归 **424 passed / 10 failed**，44.78 秒，
`/tmp/server-history-gc-baseline.log`；历史 GC 用例通过。
这是历史统计 GC 时区修复，不是整个 storage Go package 的完成声明。
整体目标仍包含其他 Rust 失败和原始外部集成门禁。

## 2026-09-11 GLOBAL 临时表回归从 catalog 取得表 ID

global_temporary_analyze_uses_session_rows_and_statistics 独立失败
（1.24 秒，`/tmp/global-temp-analyze-red.log`）。定向探针发现生产
ANALYZE 已正确发布 table_id=2、pseudo=false、row_count=0、columns=1；
测试却选择了系统表 ID 281474976710588（`/tmp/global-temp-probe.log`）。
原因是测试用“统计快照中新出现的第一个 key”识别临时表，后台首次
统计加载同时补入多个系统表 key，这个差集不是表身份来源。

现从 catalog 按 test.global_stats 获取真实表 ID。新增该 ID 在显式
ANALYZE 前没有真实统计的断言，保留 ON COMMIT DELETE ROWS、分析前
pseudo、分析后真实统计一列及空表查询仍用 pseudo 的所有断言。
临时探针已删除；未修改生产统计或临时表实现。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/handletest/handle_test.go:1100` 的
TestStatsCacheShouldNotCacheTemporaryTable 明确区分普通访问不缓存和
显式 ANALYZE 后缓存增加；没有依赖异步缓存 key 差集推断表 ID。
Rust 原临时表分析与发布分支已满足这里的行为，失败源于错误测试对象。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib global_temporary_analyze_uses_session_rows_and_statistics
# 1 passed，1.35 秒，/tmp/global-temp-analyze-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/global-temp-analyze-{fmt,lint}.log
```

完整 server 回归为 **423 passed / 11 failed**，44.26 秒，日志
`/tmp/server-global-temp-baseline.log`。本用例通过，剩余失败继续处理。
完整目标仍包括其他 Rust 失败及原始外部集成门禁，尚未完成。

## 2026-09-11 写入索引精确范围的过时 Selection 断言

a_write_reaches_the_index_path_like_a_select 独立失败（1.24 秒，
`/tmp/write-index-red.log`）：实际已有 ka(a) IndexRangeScan [10,10]，
但测试仍将历史“superset range 必须有 Selection”当作必需行为。

已用 `bin/tidb-server -V` 确认 Go oracle commit 为
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，启动独立 unistore 并执行
完全相同的 CREATE/INSERT/EXPLAIN/UPDATE/DELETE。原始 SQL 和结果位于
`/tmp/write-index-oracle/query.sql`、`/tmp/write-index-oracle/go.out`。
Go UPDATE/DELETE 都是 IndexLookUp → ka(a) IndexRangeScan [10,10] +
TableRowIDScan，没有 Selection；额外条件 a=10 AND b>100 则在 Probe
端有 Selection gt(test.wi.b,100)。unique b=100 仍是 Point_Get。
实际更新结果为 (1,101),(2,201),(3,300)，删除后仅 id=3，均与原断言一致。
oracle 进程已正常终止，未留后台节点。

测试现要求精确范围 [10,10] 且不含 Selection，并新增非访问列条件
必须保留 Selection 的正向检查。原索引选择、唯一键 Point_Get、写后
所有行值和删除结果断言保留。未修改生产实现、SQL golden 或估算值。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib a_write_reaches_the_index_path_like_a_select
# 1 passed，1.23 秒，/tmp/write-index-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 422 passed / 12 failed，44.06 秒，/tmp/server-write-index-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/write-index-{fmt,lint}.log
```

其他 Rust failures 与完整外部集成门禁继续保持未完成。

## 2026-09-11 TLS 进程级变量测试隔离

重跑 reloader_tests 再次暴露共享进程状态竞争，19 passed / 3 failed
（`/tmp/tls-process-red.log`）。失败包括 noop 刷新、旧提交发布和
ON 预发布；各测试都创建自己的 GlobalSysvars，但实际 getter/setter
共享 REQUIRE_SECURE_TRANSPORT 原子值，其他模拟节点可在断言前改写它。

保留真实运行时钩子，未改成检查局部快照：新增仅测试可用的
isolate_process_globals，用当前测试可执行文件的 --exact 参数在独立
进程执行原测试体；父进程要求成功退出且恰好一项通过，否则带 stdout/
stderr 报错，不能因错误过滤到零测试而假绿。用例内部线程、channel
时序、提交顺序及全部登录断言保持不变，没有 #[ignore] 或串行化整个
测试套件。21 个变量发布/重载用例及 3 个启动登录用例采用该入口；
不使用全局变量的 zero_interval 用例仍直接运行。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/server/tests/tls/tls_test.go:187` 的 TestTLSVerify 没有 t.Parallel，
在同一受控开关状态下验证 ON 拒绝明文、允许 TLS（:320-335）。
Rust 默认并行 harness 中的独立进程隔离保留这一前提；不改变 Go 的
进程级变量语义。这里的 Rust 登录用例仍是其原有 admission 覆盖，
不宣称替代 Go 的真实网络 TLS 集成测试。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_sysvar_seam::reloader_tests
# 22 passed / 0 failed，/tmp/tls-process-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 421 passed / 13 failed，43.86 秒，/tmp/server-tls-process-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/tls-process-{fmt,lint}.log
```

生产代码和安全检查未改变；未解决的其他失败及外部集成门禁继续保留。

## 2026-09-11 未知全局变量回归使用真实 scratch 快照

refresh_failure_skips_a_future_unknown_global_without_panicking 单独运行
通过，但 reloader_tests 并行运行时失败：live.overrides() 意外包含
secure-transport 覆盖项（`/tmp/future-sysvar-module-red.log`，21/1）。
该测试使用 GlobalSysvars::new() 构造 scratch，get 会读取进程级 TLS
开关；其他测试的 ON 会被当作当前事务快照中的持久 ON 预发布。
生产 RealClusterSysvars::begin（cluster_sysvar_seam.rs:383）实际使用
GlobalSysvars::from_cluster_rows，读取的是隔离的存储快照。

现将该回归的 scratch 改为生产构造方式，并断言空存储快照的
require_secure_transport 为 OFF。保留未知变量不能 panic、失败警告
内容和 live.overrides() 为空的所有断言，未修改生产变量发布逻辑。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/domain/sysvar_cache.go:138` 从已知变量注册表与表中值构造缓存，
随后单独运行全局 getter/setter 对应的运行时钩子；未知未来变量不会
凭空成为已知变量的持久更改。

验证命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib refresh_failure_skips_a_future_unknown_global_without_panicking
# 1 passed，/tmp/future-sysvar-target-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_sysvar_seam::reloader_tests
# 目标用例通过；模块仍 21 passed / 1 failed
# /tmp/future-sysvar-module-green.log 名称不代表模块全绿
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 退出 0，/tmp/future-sysvar-{fmt,lint}.log
```

模块剩余失败为 an_older_local_commit_publishing_last_rereads_the_newer_durable_value：
全局 TLS getter 期望 OFF 却读到并发测试的 ON。尚需修复其测试隔离；
全局变量加载、登录验证及完整目标均未宣布完成。本项使用 Ready 的
定向验证与 lint，明确保留扩大验证中发现的未解决失败。

## 2026-09-11 分区 ANALYZE 回归中的 DDL 事件与缓存同步

partition_scoped_analyze_refreshes_global_count_and_modify_count 在 DROP p2
后立即读取 SHOW STATS_META，独立得到 9 而不是 7（1.27 秒，
`/tmp/partition-scoped-red.log`）。固定 Go master
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/statistics/handle/globalstats/global_stats_test.go:492` 在 CREATE 后
处理统计 DDL 事件，并在统计读取前调用 h.Update；DROP 的统计扣减
由 `pkg/statistics/handle/ddl/ddl_test.go:1080` 明确在处理事件后验证。
SHOW 读取缓存，因此只等 schema DDL 返回不够。

fixture 现按顺序消费 CREATE、ADD PARTITION 和 DROP PARTITION 的统计
事件；DROP 后通过已有 FLUSH 入口触发缓存刷新并等待完成（五秒上限），
再读取计数。保留原 2/9、0/9 断言，以及重新 ANALYZE 前后两处计数 7
断言；没有修改生产统计实现或预期值。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib partition_scoped_analyze_refreshes_global_count_and_modify_count
# 1 passed，7.37 秒，/tmp/partition-scoped-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 418 passed / 16 failed，44.11 秒，/tmp/server-partition-scoped-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/partition-scoped-{fmt,lint}.log
```

目标用例在全套通过，但 ordinary_cluster_sysvars_are_also_installed_before_login_and_reloaded
本轮再次失败，故总失败数仍为 16，不能从上一轮总数直接减一。
本项不代表剩余 Rust failures 和外部集成门禁已完成。

## 2026-09-11 优先队列 DROP 回归等待事件事务

auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path
独立复现 DROP 后队列长度仍为 2，预期 1（3.38 秒，
`/tmp/queue-drop-red.log`）。固定 Go master
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go:309-317`
在 DROP 后先处理统计事件，再调用 pq.HandleDDLEvent，随后才断言队列。
Rust fixture 已启动异步 notifier，但原测试没有等待它处理 DROP。

现在在读取队列快照前等待 PRIORITY_QUEUE_HANDLER_ID 对应的事务完成位，
有五秒上限。保留队列成员/长度断言及后续完整自动分析验证，未改生产
队列逻辑。等待条件是事件处理完成，而非“直到队列长度等于期望值”。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path
# 1 passed，6.59 秒，/tmp/queue-drop-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 418 passed / 16 failed，43.36 秒，/tmp/server-queue-drop-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/queue-drop-{fmt,lint}.log
```

此项不代表剩余所有 Rust failed cases 或外部集成门禁完成。

## 2026-09-11 统计 mock owner 按存储隔离

两个无 etcd 启动入口创建统计 MockManager 时传入 None，所有独立
embedded 集群因此共享 mock_store_id 和统计 owner key。双真实 unistore
stack 回归 independent_unistore_stores_can_both_own_statistics 在修复前
得到 (true,false)，第二个集群无法获得 owner（4.27 秒，
`/tmp/stats-owner-isolation-red.log`）。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/domain/domain.go:2092` 将 do.store 传入 NewMockManager；
`pkg/owner/mock.go:52-57` 使用 store.UUID() 而非 nil-store fallback。
Rust 两个入口现使用与 mock DDL owner 相同的 embedded-authority 身份；
同一 opener 的 clone 保留身份，独立 store authority 互不争抢。
etcd owner 路径不变，未延长就绪等待上限。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib independent_unistore_stores_can_both_own_statistics
# 1 passed，3.26 秒，/tmp/stats-owner-isolation-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 417 passed / 17 failed，43.88 秒，/tmp/server-stats-owner-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/stats-owner-isolation-{fmt,lint}.log
```

原 auto_analyze_skips_configured_column_types_like_go 在全套通过。
priority_queue 用例已越过 owner 就绪及队列初始化，继续暴露后续错误：
DROP 后立即读取队列仍为 2，预期 1，需继续核对 DDL 订阅处理时序。
不将这个后续失败或剩余外部集成门禁记为完成。

## 2026-09-11 统计 DDL 测试等待正确订阅者的事务

已确认此前四个并发超时不是统计写入未完成。辅助函数等待整个 notifier
表清空，而生产 notifier 还注册了自动分析队列订阅者；未初始化的队列
在自动分析启用时返回 NotReadyRetryLater，使事件继续留存。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/testutil/util.go:29-39` 的
HandleNextDDLEventWithTxn 只调用统计 handler 并等待其事务完成。

辅助函数现只等待 STATS_META_HANDLER_ID 对应的 processed_by_flag 位。
生产 process_event_for_handler 在同一事务内更新统计和该位，然后提交，
所以查询到完成位才代表统计提交完成。保留原五秒上限与全部统计结果
断言，不禁用自动分析、不删除其他订阅者仍需处理的事件，也不修改生产
notifier 的重试逻辑。

新增 stats_ddl_wait_does_not_require_an_unready_other_subscriber：用始终
返回 NotReadyRetryLater 的额外 handler 确定性复现等待路径，避免改动
进程级自动分析开关干扰并行测试。修复前 6.27 秒后仍有一条事件而失败
（`/tmp/stats-subscriber-wait-red.log`）；修复后 2.25 秒通过，且明确
断言 stats_meta 存在 0/0 记录、未完成订阅者的事件仍在 notifier 表中。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib stats_ddl_wait_does_not_require_an_unready_other_subscriber
# 1 passed，/tmp/stats-subscriber-wait-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
# 415 passed / 18 failed，43.30 秒，/tmp/server-stats-subscriber-baseline.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/stats-subscriber-wait-{fmt,lint}.log
```

table_lifecycle、truncate_hash_partition、truncate_partitioned_table、
truncate_partitions_refreshes_global_stats_meta 四个用例在默认并发全套中
全部通过。新增回归也在全套通过。剩余 18 项及外部集成门禁尚未完成。

## 2026-09-11 分区 EXPLAIN 修复后的全套结果

在 22b9dcf9cf 上执行
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib`：
410 passed / 22 failed，42.47 秒，日志 `/tmp/server-partition-explain-baseline.log`。
三个分区 EXPLAIN 用例在全套中也全部通过。

下一组 DDL fixture 超时的源码线索：drain_stats_ddl_events 等待整个
mysql.tidb_ddl_notifier 表为空，build_notifier 同时注册 StatsMetaHandler
和 PriorityQueueHandler；未初始化的自动分析队列在自动分析启用时
返回 NotReadyRetryLater，因而事件可以在统计处理完成后仍留存。
固定 Go master `pkg/statistics/handle/ddl/testutil/util.go:29-39` 的
HandleNextDDLEventWithTxn 只等待统计 handler 的事务完成，不等待
priority queue。需要通过明确启用自动分析的回归验证这一判断，再修正
辅助函数的完成条件；此处尚未修改测试辅助函数，也未将超时记为解决。

## 2026-09-11 动态分区 EXPLAIN access object

global_index_statistics_match_go 原始回归独立失败：IndexReader access
object 为空，预期 partition:all（`/tmp/partition-explain-red.log`）。
Rust physical_access 无条件清空所有 reader，同时把动态分区名称追加
到 TableScan，和固定 Go master 的 reader/scan 分工相反。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`：
`pkg/planner/core/operator/physicalop/physical_table_reader.go:168`、
`physical_index_reader.go:141`、`physical_indexlookup_reader.go:172`、
`physical_indexmerge_reader.go:185` 通过 GetDynamicAccessPartition 返回
reader 的动态分区信息；`physical_table_scan.go:259` 仅对静态物理
分区扫描追加 partition 名称。

修复四种 reader 的 access object，从其实际 cop 子树读取已有动态裁剪
信息，移除 TableScan 上错误的动态信息合并；保留按物理 ID 识别静态
分区的逻辑。新增 dynamic_partitions_belong_to_readers_not_scans 回归
覆盖四种 reader、全部分区和 p0/p2 子集，检查 scan 不携带动态分区。
修复前 unwrap(None) 失败（`/tmp/partition-explain-unit-red.log`）。
未修改任何已有 SQL、估算值或预期 EXPLAIN。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1298 passed，/tmp/partition-explain-executor-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib global_index_statistics_match_go
# 1 passed，/tmp/partition-explain-server-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib global_stats_drive_partition_plans_like_go
# 1 passed，含静态/动态切换，/tmp/partition-explain-pruning-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib merged_global_cmsketch_drives_equality_estimate
# 1 passed，/tmp/partition-explain-cms-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
make lint
git diff --check
# 均退出 0，/tmp/partition-explain-{fmt,lint}.log
```

本项验证现有单表 cop reader 表示；不声称完整移植 Go 所有多表
TableScanAndPartitionInfos 结构。其余失败及完整外部门禁仍需推进。

## 2026-09-11 binding 修复后的全套结果

在 cbb0565e7c 上执行
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib`：
407 passed / 25 failed，42.14 秒，日志 `/tmp/server-binding-baseline.log`。
三个原有时间戳 panic 用例与新增时区回归均通过；其余失败仍须推进。
下一组已核实的源码差异是动态分区 EXPLAIN：Rust explain.rs 的 reader
分支直接返回 None，而固定 Go master 的 physical_table_reader.go:168、
physical_index_reader.go:141 明确返回动态分区 AccessObject；Rust 已有
dynamic_partition_access helper，但该 reader 分支没有调用。这里只记录
定位证据，尚未修改或验证该组修复。

## 2026-09-11 binding 缓存 TIMESTAMP 解码 panic

`global_binding_commands_commit_outside_the_user_transaction` 独立复现
`rowcodec.rs:759 Go map decoder timezone` panic，完整栈在
`/tmp/binding-timezone-red.log`。调用链为提交 binding → 刷新缓存 →
SystemRow::parse → decode_table_row_to_map；parse 的 None 时区入口
要求投影不含 TIMESTAMP，但 bind_info 的 create_time/update_time
均为 TIMESTAMP(6)。这也影响另一个 binding 可见性用例和系统表
hidden ID 用例，不能把 panic 当作存储环境缺口。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 依据：
`pkg/meta/metadef/system_tables_def.go:245` 的表定义；
`pkg/bindinfo/utils.go:145` 使用内部会话执行 SELECT；
`pkg/sessionctx/variable/session.go:2907` 解析实际会话时区；
`pkg/util/rowcodec/decoder.go:144` 将 UTC TIMESTAMP 转入解码器时区。
Rust binding 刷新现在从节点 global time_zone 构造内部读取会话的时区，
通过已有 parse_in_timezone 入口传入，不改变底层 codec 的契约。

新增完整集群回归 global_binding_reload_decodes_timestamps_in_its_session_timezone：
用户会话 UTC 写入，内部读取时区 +08:00；断言缓存的创建/更新时间
分别为 08:00:00.123456 和 09:00:00.654321。修复前同一 panic
（`/tmp/binding-timezone-unit-red.log`），修复后通过，保留微秒精度。

Ready 验证命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib global_binding
# 3 passed，/tmp/binding-timezone-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib system_table_hidden_ids_use_the_full_counter_key
# 1 passed，/tmp/binding-timezone-counter-green.log
RUSTUP_TOOLCHAIN=1.97 cargo fmt --manifest-path rust/Cargo.toml --all -- --check
# 退出 0，/tmp/binding-timezone-fmt.log
make lint
# 退出 0，/tmp/binding-timezone-lint.log
git diff --check
```

本修复前、空表健康度修复后的 server 全套为 403 passed / 28 failed
（`/tmp/server-health-baseline.log`，42.38 秒）。其中四个 DDL fixture
在全套并发下仍触发五秒事件等待上限；独立通过不足以宣称它们在全套
稳定。还存在事务模式、时间戳优化、分区 EXPLAIN、自动分析、全局变量
隔离等失败；完整目标与外部集成门禁仍未完成。

## 2026-09-11 空表健康度使用原始缓存的 pseudo 状态

partition_global_stats_health_matches_go 在空分区表 ANALYZE 后独立失败：
SHOW STATS_META 返回三条记录，SHOW STATS_HEALTHY 却为空
（`/tmp/partition-health-red.log`）。消费 CREATE 事件仍失败
（`/tmp/partition-health-events.log`），因此移除了该实验改动。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/executor/show_stats.go:522` 读取原始缓存 Table.GetStatsHealthy；
`pkg/statistics/table.go:806` 仅跳过原始 pseudo 对象。Rust 转换层已分别
保存 cache_pseudo 和优化器 pseudo，但健康度误用了后者：零行的真实
统计也会被优化器标为 pseudo。现在健康度只检查 cache_pseudo，保留
优化器原有估算策略。回归覆盖合成 pseudo 不显示、未分析空表显示 0、
已分析空表显示 100，原分区用例的全部健康度和计数断言保持不变。

验证（Ready 范围）：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib initialized_empty_stats_are_not_a_synthetic_cache_pseudo
# 修复前失败：预期 (0,true)，实际 (0,false)，/tmp/empty-health-unit-red.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1297 passed，/tmp/empty-health-executor-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib partition_global_stats_health_matches_go
# 1 passed，10.29 秒，/tmp/empty-health-server-green.log
make lint
# 退出 0，/tmp/empty-health-lint.log
git diff --check
```

本项是 SHOW 健康度语义修复，不代表统计包完整移植或全部门禁完成。

## 2026-09-11 多分区 TRUNCATE 后刷新全局统计缓存

truncate_partitions_refreshes_global_stats_meta_like_go 独立失败：截断 p2/p4
并 FLUSH STATS_DELTA 后 SHOW STATS_META 仍为 15，预期 11
（`/tmp/truncate-multi-red.log`）。仅消费 DDL 事件仍失败
（`/tmp/truncate-multi-green.log`，该日志名称不代表通过）。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/globalstats/global_stats_test.go:550` 的
TestDDLPartition4GlobalStats 在 CREATE 后消费事件；TRUNCATE 后则先
flush delta，再消费事件，再 h.Update，才检查内存全局统计。
Rust fixture 现补齐事件消费，并通过现有 FLUSH 入口触发和等待统计缓存
reload，等待有五秒上限。保留 15→11、再次 ANALYZE 后 7 条统计及
全局 11 的断言；没有改生产统计或预期值。

验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib truncate_partitions_refreshes_global_stats_meta_like_go`
通过，1 passed，4.27 秒（`/tmp/truncate-multi-refresh.log`）。
Ready gate `make lint` 通过（`/tmp/truncate-multi-final-lint.log`）；
`git diff --check` 通过。

## 2026-09-11 DROP PARTITION 统计事件同步

drop_partitions_statistics_match_go 独立失败：删除 p0/p1 后全局统计仍为
count=5、modify_count=0，预期 2/3（`/tmp/drop-partitions-red.log`）。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/ddl_test.go:1031` TestDropPartitions 在检查前
显式消费 ActionDropTablePartition，随后验证 2/3 及两个旧分区版本变化。

Rust fixture 现于 CREATE 和 DROP PARTITION 后驱动已有真实 notifier。
保留原插入、ANALYZE、全局统计及两个旧分区版本递增断言；生产统计
逻辑未修改，预期值未调整。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib drop_partitions_statistics_match_go`
通过，1 passed，3.25 秒（`/tmp/drop-partitions-green.log`）；`make lint`
通过（`/tmp/drop-partitions-lint.log`），`git diff --check` 通过。

## 2026-09-11 ADD PARTITION 两种 prune mode 的事件同步

add_partition_statistics_follow_global_prune_mode_like_go 独立失败于
CREATE 后分区 stats_meta 行数 0、预期 1（`/tmp/add-partition-red.log`）。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/ddl_test.go:413` TestDDLPartition 在 static 和
dynamic 两轮中，都显式消费 CREATE 与 ADD PARTITION 的 DDL 事件。

Rust fixture 现补齐两个消费点，使用已有真实 notifier。保留原分区
统计存在、static 无全局行、dynamic 有全局行、新分区统计和三个
histogram 的全部断言，未改生产统计逻辑或期望值。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib add_partition_statistics_follow_global_prune_mode_like_go`
通过，1 passed，5.24 秒（`/tmp/add-partition-green.log`）；`make lint`
通过（`/tmp/add-partition-lint.log`），`git diff --check` 通过。

## 2026-09-11 HASH 分区截断统计事件同步

truncate_hash_partition_statistics_match_go 独立失败：截断 p0 后全局
count/modify_count 仍为 5/0，预期 4/1（`/tmp/hash-partition-red.log`）。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/ddl_test.go:830` TestTruncateAHashPartition 在
断言前显式处理 ActionTruncateTablePartition 事件。

Rust 测试现于 CREATE 和 TRUNCATE PARTITION 后驱动已有真实 notifier。
原来的 4/1 统计值、新旧 p0 ID 不同及旧统计版本递增断言均保持不变。
未更改生产统计更新，也未直接写入预期值。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib truncate_hash_partition_statistics_match_go`
通过，1 passed，3.32 秒（`/tmp/hash-partition-green.log`）；`make lint`
通过（`/tmp/hash-partition-lint.log`），`git diff --check` 通过。

## 2026-09-11 TRUNCATE 分区表统计事件同步

truncate_partitioned_table_statistics_match_go 独立失败：新分区 ID 已发布，
但对应 stats_meta 行数为 0，预期 1（`/tmp/truncate-partitioned-red.log`）。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/ddl_test.go:250` 在 TRUNCATE 后显式处理
ActionTruncateTable，然后检查两个新 ID 的统计行与旧 ID 的版本变化。

Rust fixture 现于 CREATE 后和 TRUNCATE 后驱动已有真实 notifier。
保留原始插入/ANALYZE、新旧分区 ID 不重叠、新统计行存在和旧统计版本
递增断言，未调整生产 DDL 或统计写入行为。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib truncate_partitioned_table_statistics_match_go`
从失败转为 1 passed（`/tmp/truncate-partitioned-green.log`）；`make lint`
通过（`/tmp/truncate-partitioned-lint.log`），`git diff --check` 通过。

## 2026-09-11 DROP SCHEMA 统计测试消费对应事件

drop_schema_ddl_retires_all_statistics_like_go 独立运行在读取初始统计版本时
越界：CREATE 的统计事件未消费，stats_meta 查询为空
（`/tmp/stats-drop-schema-red.log`）。
Go master 固定版本 `pkg/statistics/handle/ddl/ddl_test.go:1472` 的
TestDropSchema 先建立统计，再于 DROP DATABASE 后显式调用
HandleDDLEventWithTxn 处理 ActionDropSchema，才检查版本变化。

Rust fixture 现于两个表建好后、DROP DATABASE 后分别驱动已有真实 notifier。
普通表、分区表全局 ID 和两个分区 ID 共四项版本递增断言全部保留。
没有直接补写统计、跳过 ID 或忽略空结果。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib drop_schema_ddl_retires_all_statistics_like_go`
通过，1 passed，3.25 秒（`/tmp/stats-drop-schema-green.log`）；
`make lint` 通过（`/tmp/stats-drop-schema-lint.log`），`git diff --check` 通过。

## 2026-09-11 表生命周期统计测试消费 DDL 事件

最新修改前 server 基线正常结束，397 passed / 34 failed，40.79 秒
（`/tmp/server-current-baseline.log`）。其中 table_lifecycle_ddl_updates_statistics_like_go
独立复现 CREATE 后 stats_meta 为空，预期 [0,0]
（`/tmp/stats-lifecycle-red.log`）。fixture 仅创建零 lease stack，未启动
stats owner；DDL 返回不代表持久化事件已由统计订阅者处理。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/statistics/handle/ddl/ddl_test.go:60` TestDDLTable 显式调用
HandleNextDDLEventWithTxn；同文件 250 行附近的 TRUNCATE 用例同样显式
消费事件。Rust 用例现在于 CREATE、CREATE LIKE、TRUNCATE、DROP 后调用
已有 drain_stats_ddl_events，驱动真实 notifier 到持久化队列清空。
未直接写统计表或更改任何统计计数、histogram 数量、ID/版本递增断言。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib table_lifecycle_ddl_updates_statistics_like_go`
通过，1 passed，5.28 秒（`/tmp/stats-lifecycle-green.log`）；
`make lint` 通过（`/tmp/stats-lifecycle-lint.log`），`git diff --check` 通过。
本轮基线 34 个失败为修改前观测，不能当作当前剩余失败的精确计数。

## 2026-09-11 binary prepared SELECT 使用保留的缓存计划

pipeline `prepared_execution_retains_ast_and_reuses_current_handles` 独立
失败：第二次参数执行行值正确，但 last_plan_from_cache 为 0，预期 1
（`/tmp/pipeline-cache-red.log`）。run_prepared_with_result_authority 只绑定
AST 并走普通规划，漏掉 PreparedAst 已保存的 select_plan；SQL EXECUTE
已有完整的缓存准入、binding 和物理计划执行路径。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/optimize.go:950` 在 Execute 规划中调用 GetPlanFromPlanCache。
Rust binary 入口现在复用同一 select-plan 绑定与执行路径，保留 statement
hint、binding 标记和缓存关闭策略。共享执行 helper 可在原 statement
生命周期内捕获 ResultMaterializationAuthority，不从恢复后的变量重建策略。
未直接设置虚假的命中状态；仍以物理计划确实被执行和 cache_hit 为准。

新增 session API 回归，检查换参返回行（包括重复投影列）、首次未命中、
再次命中、结果 authority 非空及关闭缓存后未命中。旧实现第二次执行
失败（`/tmp/binary-cache-unit-red.log`），修复后通过。
Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-session --lib prepared` 为
**103 passed / 0 failed / 2 ignored**（`/tmp/binary-cache-prepared-green.log`）；
同参数 `-p tidb-server --lib pipeline_session::tests` 为 **14 passed / 0 failed**
（`/tmp/binary-cache-pipeline-green.log`）。`make lint` 通过
（`/tmp/binary-cache-lint.log`），`git diff --check` 通过。
其余 server、session 与外部集成门禁仍需按原目标继续验证和修复。

## 2026-09-11 pipeline 测试身份使用明确的安全传输

生命周期测试独立运行通过（`/tmp/pipeline-auth-single.log`），同进程运行
时却在 session_context 的明文认证处报 SecureTransportRequired。
其他测试会设置进程级 require_secure_transport，普通 SQL fixture 的
身份创建因此依赖测试调度。Go master 固定版本的 `pkg/server/conn.go:669`
仅拒绝不满足安全传输条件的连接；不应为修测试关闭此生产检查。

测试 helper 改用 DirectTls 认证，并一致地设置 secure_transport=true。
这只是已完成握手的 session fixture；没有增加或声称网络 TLS 集成覆盖。
安全策略测试仍以 authenticate_native 验证明文连接被拒绝。
该测试新增在策略 ON 后打开普通 fixture 的回归，旧 helper 独立失败
（`/tmp/pipeline-transport-unit-red.log`），修复后通过。

Ready 检查：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib pipeline_session::tests`
为 **13 passed / 1 failed**（`/tmp/pipeline-transport-green.log`）。
剩余 prepared_execution_retains_ast_and_reuses_current_handles 已越过认证，
失败为缓存命中状态实际 0、预期 1，尚未解决，不能称为 pipeline 全绿。
`make lint` 通过（`/tmp/pipeline-transport-lint.log`）。

## 2026-09-11 prepared GROUP BY 表头测试比较无序行

prepared 扩展验证发现 `a_group_by_field_keeps_its_written_alias_in_the_header`
实际表头 x、行 20/10，预期同一表头及 10/20。SQL 无 ORDER BY。
Go master 固定版本的 `pkg/executor/test/aggregate/aggregate_test.go:487`
对 GROUP BY 结果先 Sort() 再比较。此处同样只规范化 EXECUTE 返回行的
比较顺序，保留原 SQL、表头、每个值及重复数断言。

修复前证据 `/tmp/prepare-sysvar-prepared-regression.log`：101 passed /
1 failed / 2 ignored。修复后 `RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib prepared`
为 **102 passed / 0 failed / 2 ignored**（`/tmp/prepared-header-green.log`）。
原有 ignored 未新增或修改。Ready gate `make lint` 通过
（`/tmp/prepared-header-lint.log`），`git diff --check` 通过。

## 2026-09-11 PREPARE 元数据路径校验系统变量

原 server `prepared_system_variable_scope_errors_survive_cluster_metadata_probe`
独立失败（`/tmp/prepare-sysvar-red.log`）：非法 @@session.ddl_slow_threshold
在 PREPARE 被接受。plan_bound_prepared_columns 漏掉普通执行路径已有的
bind_variables，planner 收到未绑定变量后报泛化 1105，协议的元数据
fallback 又将该错误吞掉。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/core/expression_rewriter.go:1984` 的 rewriteSystemVariable
在规划阶段检查显式作用域与 InternalSessionVariable；对应测试在
`pkg/planner/core/tests/rewriter/rewriter_test.go:24`。
Rust 元数据路径现在复用 session.bind_variables，返回既有的 Var 错误，
不改协议错误码映射、不执行查询、不修改保留的 prepared AST。

新增 session API 回归，旧实现实际 1105、预期 1238
（`/tmp/prepare-sysvar-unit-red.log`）；修复后覆盖顶层、嵌套、隐藏内部
变量及合法变量的用例通过，原变量执行测试也通过
（`/tmp/prepare-sysvar-unit-green.log`）。原 server 用例核对错误码、
SQLSTATE、文本和错误包，全部通过（`/tmp/prepare-sysvar-server-green.log`）。
Ready gate `make lint` 通过（`/tmp/prepare-sysvar-lint.log`）。
扩展 `cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib prepared`
为 101 passed / 1 failed / 2 ignored；剩余是无 ORDER BY 的 GROUP BY
行序断言（`/tmp/prepare-sysvar-prepared-regression.log`），另行处理。

## 2026-09-11 提交时冲突测试固定乐观事务前提

`lost_the_race_fails_at_commit` 三个用例（普通 BEGIN、prepared BEGIN、
autocommit=0）独立重跑全部失败，UPDATE 阶段误入悲观锁路径，尚未到达
它们要求检查的 COMMIT（`/tmp/optimistic-conflict-red.log`）。
测试要求的是乐观事务写冲突语义，却继承当前默认的悲观模式。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/session/tidb_test.go:449,473` 显式设置 session.tidb_txn_mode 为
optimistic，另有 begin optimistic 场景。Rust 三个对应测试现在同样
在开始前设置 session 模式；未改变生产默认、原 SQL 事务入口、
UPDATE 成功、COMMIT 9007、回滚后最终行值等断言。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib lost_the_race_fails_at_commit`
从 0 passed / 3 failed 转为 **3 passed / 0 failed**
（`/tmp/optimistic-conflict-green.log`）；`make lint` 退出 0
（`/tmp/optimistic-conflict-lint.log`），`git diff --check` 通过。
这仅纠正提交冲突用例的测试前提；mock 的悲观锁返回值接口与其他失败
仍需继续处理，不能据此宣称全部事务模式已验证。

## 2026-09-11 autocommit 预取遵守实际事务模式

准备 UPDATE 独立失败，错误为 `only a pessimistic transaction locks
statement keys`（`/tmp/prepared-lock-red.log`）。点写在绑定阶段产生
prelock_keys，但 begin_autocommit_write 返回的事务可能是乐观模式。
DeferredSnapshot 之前仅检查 key 是否为空，无条件请求悲观锁。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/core/point_get_plan.go:1208` 和 1215 仅在
TxnCtx.IsPessimistic 时设置 point/batch-point UPDATE 的 Lock。
Rust 现在同样检查实际预取事务的 is_pessimistic()；乐观模式从原事务
读取 snapshot，悲观模式保留锁返回值及 for_update_ts 路径。
未改变事务后端模式、提交时间戳、锁接口错误检查或原断言。

新增 `optimistic_autocommit_point_write_uses_its_snapshot_without_prelocking`
覆盖同一准备语句对存在与不存在 key 的更新，并验证提交值。
旧实现失败（`/tmp/optimistic-prelock-unit-red.log`），修复后新用例、
原准备 UPDATE/DELETE 用例及真实悲观点更新锁用例均通过
（`/tmp/server-prelock-green.log`）。Ready gate `make lint` 通过
（`/tmp/optimistic-prelock-lint.log`）。
全量 `RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib` 正常结束，
**386 passed / 45 failed**，41.30 秒；这是本轮观测，不能将所有减少项
都归因于本修复，跨测试全局状态相关失败仍需独立复现和治理。

## 2026-09-11 reader 内 TopN 的标量排序键

独立复现 `cluster_views_are_registered_from_go_table_info`：包含
SUM 视图和 MAX 标量子查询的 EXPLAIN 返回 `Get unexpected expression`
（`/tmp/cluster-view-red.log`）。探针证明报错节点是 reader 内的
coprocessor TopN，排序表达式是聚合消除后保留的 cast_decimal(v)。
root 的投影注入未遗漏：Go 的对应 pass 本来不进入 TiKV reader。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/store/mockstore/unistore/cophandler/closure_exec.go:1037` 对
orderByExprs 逐项 Eval 并保存独立的排序 key；普通 Go Sort/TopN 则要求列键。
Rust 的本地 reader 此前直接将 coprocessor 表达式交给普通 TopN。
现在仅在 reader 执行适配中以私有投影物化排序键，并从输出裁去额外列。
原物理计划、EXPLAIN、普通 TopN 限制及断言均保持不变；诊断日志已移除。

新增 `max_over_a_derived_sum_materializes_coprocessor_topn_keys`：
以主键聚合的派生 SUM 再取 MAX，包含非主键顺序的最大值及 NULL。
关闭修复后独立失败（`/tmp/cop-topn-unit-red.log`），开启修复后
executor 全量 **1297 passed / 0 failed**（`/tmp/cop-topn-executor-green.log`）。
原 server 视图用例通过（`/tmp/cluster-view-green.log`），`make lint`
通过（`/tmp/cop-topn-lint.log`）。命令使用
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib`，按 Ready 范围验证。
其他 server 和外部集成失败仍不据此宣称完成。
本轮 server 全量正常结束，**374 passed / 56 failed**，耗时 40.93 秒
（`/tmp/server-after-cop-topn.log`），无 readiness 或 DDL owner 永久等待。

## 2026-09-11 HashJoin 保留规划表达式的比较规则

server 原始用例独立失败：`a_case_insensitive_cluster_column_orders_groups_and_dedups_by_its_collation`
自连接 COUNT 实际 4、预期 6（`/tmp/server-collation-red.log`）。
两侧列保留 utf8mb4_general_ci，但 executor 的 physical_builder
丢弃 EqualConditions，按列重建 Tiny 类型 eq，比较规则成为 Binary。
因此 B/b 两行未产生应有的交叉匹配。诊断探针已全部移除。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/executor/builder.go:1950` 明确从 EqualConditions[i].CharsetAndCollation()
设置左右 HashJoin key 的比较规则。Rust HashJoin 现在 resolve 原有
等值表达式，保留规划阶段的规则及表达式元数据；没有修改预期结果。

新增 `hash_join_preserves_the_planned_comparison_collation` 回归，用
B/b 两行同时核对默认 CI 自连接 4 行和显式 utf8mb4_bin 自连接 2 行。
旧实现前者返回 2，red 日志 `/tmp/hash-collation-unit-red.log`；
修复后新旧两个 server 用例均通过（`/tmp/hash-collation-server-green.log`）。
`make lint` 通过（`/tmp/hash-collation-lint.log`）。
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib` 全量 1296 passed / 0 failed
（`/tmp/hash-collation-executor-green.log`），使用 Ready 验证范围。
本次修复范围为物理 HashJoin，其他 join 类型和整体剩余门禁不据此声明完成。

## 2026-09-11 HAVING 子查询区分结果集合与排序契约

executor 剩余 subqueries 失败独立复现于 `/tmp/subquery-order-red.log`：
无 ORDER BY 的 GROUP BY/HAVING 返回 3,2，旧断言要求 2,3。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
的 `pkg/executor/test/aggregate/aggregate_test.go:487` 对无排序聚合
先 Sort() 再比较。Rust 测试现在保留原 SQL，按多重集合精确比较结果，
并额外执行 ORDER BY a 的同一查询，直接断言有序结果。
没有修改执行器、过滤条件、预期行值或行数。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib` 全部 1296 用例通过
（`/tmp/subquery-order-green.log`）；`make lint` 退出 0
（`/tmp/subquery-order-lint.log`）。server 的其余失败与外部集成门禁
仍需逐项处理，不能将 executor 全绿等同于整体目标完成。

## 2026-09-11 point-get 显式输出保留隐藏列

独立复现 loaded_hidden_columns_preserve_native_layout_and_index_values：
UPDATE t SET v=30 WHERE id=1 返回 point-get output column is outside the row，
`/tmp/hidden-point-red.log`。HandleSourceExec 的输出 Stored(offset) 指向
完整表列，但 next 先通过 visible_of 去掉隐藏列，导致表达式索引维护需要
的隐藏列偏移落在截断行之外。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
pkg/executor/point_get.go 按 e.Schema() DecodeRowValToChunk，并依照 schema
填充虚拟列。Rust 显式物理输出映射现使用完整 decoded row；默认可见输出
继续使用原前缀。没有改变隐藏列可见性、表存储布局或索引断言。

新增 mapped_point_read_retains_hidden_columns_required_by_the_plan 回归，
按隐藏列在前、普通列在后的映射检查输出；旧实现同样越界失败
（`/tmp/hidden-point-unit-red.log`），修复后通过。原始 server 用例
包括 UPDATE 后 FORCE INDEX(vi) 读取也全部通过，0.03 秒
（`/tmp/hidden-point-server-green.log`）。

验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib` 为 **1295 passed /
1 failed**（`/tmp/hidden-point-executor-green.log`）；剩余 subqueries 在
2183 行实际顺序 3,2、预期 2,3，尚未修复，不能视为全绿。
`cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
loaded_hidden_columns_preserve_native_layout_and_index_values` 通过；
两条 cargo 命令均使用上述工具链与栈设置。`make lint` 退出 0
（`/tmp/hidden-point-lint.log`），`git diff --check` 通过。

## 2026-09-11 嵌入式 DDL owner 按存储隔离

check_constraint_runs_through_the_owner_job_queue 单独执行 1.25 秒通过
（`/tmp/check-owner-red.log`，文件名不代表失败），并行全量却反复等不到
历史记录。`/tmp/server-parallel-diagnosis.log` 显示已关闭存储的 scheduler
持续扫描失败；该诊断运行取证后终止，不能计为完成。

根因：RealClusterDdl 的无 etcd 分支调用 MockManager 时传入 store_id=None，
所有独立嵌入式库都使用同一个 mock_store_id/DDL_OWNER_KEY。一个库的
owner 会阻止其他库取得 ownership，因而无法处理后者的持久化 job。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/ddl/ddl.go` 传入 opt.Store，`pkg/owner/mock.go::NewMockManager`
以 store.UUID() 为选举命名空间，只有 nil store 才使用 mock_store_id。

Rust 现使用嵌入式存储的稳定 read authority ID 作为本地 store 身份，
opener 克隆保持同一 ID。新增回归创建两个独立 authority 和一个同库竞争者，
确认独立库都可成为 owner、同库仍互斥。旧实现 2.02 秒失败
（`/tmp/ddl-owner-isolation-red.log`），修复后通过
（`/tmp/ddl-owner-isolation-green.log`）。未修改队列、job 完成断言或超时。

Ready 验证：`make lint` 退出 0（`/tmp/ddl-owner-isolation-lint.log`），
`git diff --check` 通过。完整命令
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-server --lib -- --nocapture` 正常结束，42.83 秒，
**371 passed / 58 failed**（`/tmp/server-owner-isolation-full.log`）；
原 check-constraint 用例通过，没有跳过任何测试。全量仍为失败，58 项
保持待修复，不能把解除调度等待当作整体目标完成。

## 2026-09-11 MODIFY/RENAME COLUMN 统计事件同步

独立复现 `modify_column_ddl_recreates_missing_default_statistics_like_go`：
MODIFY 后 histogram 返回 `[]`，期望 `[["0", "3", "0", "0"]]`，
1.25 秒失败（`/tmp/modify-column-stats-red.log`）。固定 Go master
`pkg/statistics/handle/ddl/subscriber.go` 的 ActionModifyColumn 在
非 analyzed 事件中调用 insertStats4Col；统计初始化发生在事件消费阶段。
此 fixture 与前述 ADD COLUMN 相同，没有启动统计 owner。

在 CREATE、MODIFY、RENAME 后使用已验证的真实 notifier 消费 helper，
保留删除原 histogram 后再检查重建及无 bucket 的全部断言。未修改生产
统计实现、期望值或 golden。完整用例 1 passed，4.26 秒，日志
`/tmp/modify-column-stats-green.log`。

验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib
modify_column_ddl_recreates_missing_default_statistics_like_go -- --nocapture`；
`make lint` 退出 0（`/tmp/modify-column-stats-lint.log`）；
`git diff --check` 通过。此项独立关闭，其他失败和 scheduler 等待仍未完成。

## 2026-09-11 ADD COLUMN 统计测试的事件同步

独立失败 `add_column_ddl_initializes_statistics_like_go` 的根因是测试没有
消费持久化 DDL 事件。临时探针证明 ALTER 后 job 3/4 已在
mysql.tidb_ddl_notifier 中，等待 1.5 秒后 processed_by_flag 仍均为 0；
统计行依然为空（`/tmp/stats-ddl-delivery-probe.log`）。fixture 仅创建
Unistore stack，没有 production boot 的 campaign_stats_owner 步骤。

固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/statistics/handle/ddl/ddl_test.go::TestDDLHistogram` 在 ALTER 后显式
`statstestutil.HandleNextDDLEventWithTxn(h)`，再断言统计结果。Rust 回归
现在对应地运行实际 notifier，确认持久化队列清空后停止 worker，再检查
原有 histogram/bucket/version 断言。5 秒内队列未清空仍失败，不增加
固定成功延迟，不改统计值，不直接伪造事件或写入统计行。

所有 ADD COLUMN 分支均通过：nullable/default/NOT NULL/virtual、多列
sub-job，以及 IF NOT EXISTS 跳过列不重建已删除 histogram 的约束。
原始红色证据 `/tmp/server-stats-single-red.log`；最终 1 passed，8.29 秒，
`/tmp/stats-ddl-drain-final.log`。临时探针全部删除。

Ready 验证命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib add_column_ddl_initializes_statistics_like_go
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib ddl_notifier
# 3 passed; /tmp/stats-ddl-notifier-regression.log
make lint
# exit 0; /tmp/stats-ddl-drain-lint.log
git diff --check
# exit 0
```

仅关闭此失败类别；不能把其他统计失败或 DDL scheduler 等待归为相同原因，
完整 server 和原始集成清单仍未全部通过。

## 2026-09-11 catalog-load 单表分发修复

原始脚本先失败于过时断言：它要求拒绝 VARCHAR(64)，但节点已支持并加载。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/util/rowcodec/decoder.go` 明确将 TypeVarchar 解码为字符串。
脚本现在精确验证两张表的 ID、列形状、无拒绝表，以及 Go/Rust 实际返回行；
VARCHAR fixture 覆盖普通值、空串、前后空格和 64 字符上限。
新增 Go binary 和集群版本参数，以固定 master 与兼容 nightly 实测。

更新元数据断言后，原始单表 SELECT 显露真正生产错误：
`configured ORDER BY/LIMIT planning failed: RelationBinding(ExactlyTwoBaseRelationsRequired)`，
日志 `/tmp/quality-catalog-load-master.log`。多表 adapter 根据 catalog 中
配置了两张表便将全部文本查询送入双表 Join planner。
Go `logical_plan_builder.go::buildJoin` 对 `Right == nil` 明确只规划 Left。
Rust 现从 AST 的 FROM 关系选择单表路径，通过既有 catalog resolver 和
ReadOnlyScanPlan 降低计划，再复用匹配 table_id 的 reader、取消与结果处理。
无事务状态的锁定读取仍拒绝；Join/TopN 路由保持原有验证。

新增无网络回归 `single_table_queries_are_admitted_with_two_configured_tables`：
旧实现失败（`/tmp/catalog-single-route-red.log`），修复后精确验证两个 table_id、
限定名/别名、未知表与锁定读取；原 Join/TopN 回归也通过
（`/tmp/catalog-single-route-green.log`）。最终增加锁定断言后的测试在完整
server 运行中亦为通过。

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 \
CATALOG_LOAD_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
CATALOG_LOAD_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
bash rust/scripts/run-realtikv-catalog-load.sh
# exit 0; /tmp/quality-catalog-load-master-green.log
make lint
# exit 0; /tmp/catalog-single-route-lint.log
git diff --check
# exit 0
```

完整 `cargo test -p tidb-server --lib` **未通过**：在
`/tmp/catalog-single-route-server.log` 记录 61 项失败，check-constraint
测试持续等待 DDL 历史记录，超过两分钟后取栈并终止本次进程（退出 101），
没有跳过或改写测试。栈 `/tmp/catalog-server-hang.sample` 定位到
`cluster_session_node/ddl.rs::wait_persisted_job` 与 scheduler 等待。
独立执行 `add_column_ddl_initializes_statistics_like_go` 也稳定失败：
实际统计行 `[]`，Go 预期 `[["0", "3", "0"]]`，日志
`/tmp/server-stats-single-red.log`，耗时 1.24 秒。这些是下一组明确失败入口，
并非 readiness 或外部取证缺口。整体质量目标仍未完成。

## 2026-09-11 PD-route 入口与 Bazel 复验

PD-route 旧脚本调用不存在的 `--test realtikv_pd_route`，原始真实运行
失败见 `/tmp/quality-pd-route-red.log`。改为聚合 `all` 与完整名称
`realtikv_pd_route::pd_only_input_discovers_route_and_reaches_tikv`，新增
恰好 1 passed、0 failed 检查，防止 `--exact` 失配导致零测试假通过。
使用 `RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 bash
rust/scripts/run-realtikv-pd-route.sh` 完整运行退出 0，日志
`/tmp/quality-pd-route-green.log`。`bash -n`、`git diff --check`、
`make lint` 通过（`/tmp/quality-pd-route-lint.log`）。未改动 RPC 实现或预期。

Bazel parser 的历史 replacement 阻塞已不再复现。`make bazel_prepare`
第一次在 tazel filepath.Walk 回调中空指针退出，当时 Cargo 正在构建；
临时文件变化只是待证假设。Cargo 完成后相同命令退出 0，最终没有任何
生成文件差异，日志 `/tmp/quality-bazel-prepare-retry.log`。
`bazel query //pkg/parser/...` 退出 0；`bazel test //pkg/parser/...`
实际执行 12 个 target，全部通过，parser 分片 test.log 亦为 PASS。
日志分别为 `/tmp/quality-bazel-parser-query.log`、
`/tmp/quality-bazel-parser-test.log`。不修改 go.mod 的本地 parser replacement。

catalog-load 使用固定 Go master 的新证据是：两张表加载成功后，普通
单表 SELECT 返回 `RelationBinding(ExactlyTwoBaseRelationsRequired)`。
此失败在更新过时 VARCHAR 拒绝断言后显露，尚需修复；不能计为通过。

## 2026-09-10 adaptive-forwarding 测试入口修复

原命令实测退出 1（`/tmp/quality-adaptive-forwarding.log`）：
`no test target named realtikv_replica_read`。当前 Cargo manifest 使用
`autotests=false` 和共享 aggregate-tests.rs，普通集成源已归入 `all`。
脚本改用 `--test all` 与完整模块测试名
`realtikv_replica_read::adaptive_forwarding_reuses_proxy_then_recovers_direct`。
新增执行数量断言，必须恰好 1 passed、0 failed，保留全部行为 marker。
此次仅修复测试接线，没有修改 Rust RPC 行为或 Go 对照预期。

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 \
bash rust/scripts/run-realtikv-adaptive-forwarding.sh
# exit 0; /tmp/quality-adaptive-forwarding-green.log
bash -n rust/scripts/run-realtikv-adaptive-forwarding.sh
make lint
# exit 0; /tmp/quality-adaptive-lint.log
```

真实三节点 TiKV 证明 forwarded_header=tikv-forwarded-host、首次与复用
响应可用、busy_sequence=500,800,150、恢复直连且 preference_cleared=true。
清理检查亦通过。原始失败已由同一端到端脚本复验关闭。

## 2026-09-10 最新 readiness 实测与格式门禁

在 `2337e56a36` 修复上重跑真实集群，完整 access-path 对照退出 0，
末行 `the access-path differential passed`。Rust 日志实际输出
`cluster_session_node_ready`，地址 127.0.0.1:47600，schema_version 68，
stats_loaded 4；不能再将 readiness 描述为尚未解除的 blocker。

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-pruned-probe-evidence \
bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-pruned-probe.log 2>&1
bash rust/scripts/test-access-path-readiness.sh
# delayed ready / exited / stuck 全部符合预期
```

继续复验历史质量门禁，`cargo fmt --all -- --check` 检出 78 个格式差异块，
涉及 40 个 Rust 文件（`/tmp/pruned-probe-fmt-check.log`）。仅用 cargo fmt
自动格式化；同一检查随后退出 0（`/tmp/pruned-probe-fmt-green.log`），
作为独立格式类别提交，不改变 SQL 断言或 golden。

`RUSTUP_TOOLCHAIN=1.97 cargo check --manifest-path rust/Cargo.toml --offline
--locked -j12 --workspace` 从仓库根目录执行并退出 0，日志
`/tmp/pruned-probe-workspace-check.log`。从 rust 子目录直接使用稳定版
会读取 `.cargo/config.toml` 的 nightly 专用 `-Zthreads=8` 而失败；这是
工具链与入口不匹配，采用既有根目录入口即可，不修改 nightly 配置。
格式修正后 `make lint` 退出 0（`/tmp/quality-format-lint.log`），
`git diff --check` 通过。整体目标仍保留外部脚本、Go suites、Bazel 等
完整验收项；此处不宣称全部集成测试完成。

历史 parser 命令亦重跑：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml --offline --locked -j12 -p tidb-parser --lib --test all`
退出 0，integration target 为 100 passed、0 failed、1 ignored，日志
`/tmp/quality-parser-tests.log`；没有新增 ignore。

## 2026-09-10 IndexJoin 保留裁剪后的可用索引前缀

condition eleven 剩余 customer 路径差异并非成本偏低：候选日志证明只有
Table 路径到达成本比较。`path_matches_index_join_runtime` 遇到被裁剪的
尾列直接返回 false，错误淘汰已匹配连接键的 idx_customer。
固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/planner/util/column.go::indexInfo2ColsImpl` 遇到缺失列仅截断
prefixCols，保留前面的可用键；`stats.go::fillIndexPath` 将该前缀传入路径。
Rust 现按相同边界终止匹配，保留此前结果；不跨越前缀缺口。

新增 `index_join_keeps_a_usable_prefix_when_trailing_columns_are_pruned`，
旧实现断言失败（`/tmp/pruned-probe-red.log`），修复后通过，同时验证缺失
首列不能用于探测后面的连接键。原始 condition eleven 完整计划断言通过，
包含有序 idx_customer；没有修改 SQL、Go golden 或既有计划断言。

Ready 验证：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 928 passed, 0 failed; /tmp/pruned-probe-planner.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1295 passed, 0 failed; /tmp/pruned-probe-full-executor.log
make lint
# exit 0; /tmp/pruned-probe-lint.log
git diff --check
# exit 0
```

临时成本探针已删除。此结果关闭 condition eleven 的剩余路径失败，不能
代替 BLOCKER_RESOLUTION.md 要求的所有外部集成、workspace 和 Bazel 门禁。

## 2026-09-10 aggregate repair 输出 ID 修复

Go master `logical_aggregation.go::PruneColumns` 在补充 COUNT/FIRST_ROW 时调用
AllocPlanColumnID；Rust local helper 使用 i64::MIN 占位，整树调用者未完成分配，
物理 EXPLAIN 泄漏 ScalarQueryCol#-9223372036854775808。本次在整树裁剪点使用
既有 RuleContext.column_allocator 分配，保持 local helper 接口。新增完整
condition eleven 计划断言禁止 repair 输出出现 ScalarQueryCol，占位旧实现
失败于该断言（`/tmp/aggregate-repair-id-red.log`），修复后通过。

按同 JSON 统计 Go 证据同步更正历史测试：顶层 IndexHashJoin、下层 IndexJoin，
键顺序 district/warehouse，orders 使用 TableRangeScan；保持 customer 的
idx_customer 和全部扫描行数断言。当前测试因此停在 customer 索引路径断言，
而不再被错误 MergeJoin 预期遮挡。未修改 Go golden。

Ready 验证：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib` 927 passed（`/tmp/aggregate-repair-id-planner.log`）；
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib` 1294 passed / 1 failed
（`/tmp/aggregate-repair-id-executor.log`）；`make lint` 退出 0
（`/tmp/aggregate-repair-id-lint.log`）；`git diff --check` 通过。
condition eleven 尚未完成，下一项为 customer index/table 候选成本差异。

## 2026-09-10 propagated predicate 访问路径估算修复

condition eleven 的 new_order 扫描新断言要求 Go 实测 9000 行，旧实现为
11250，红色证据 `/tmp/eleven-static-range-red.log`。临时记录揭示原始访问
估算只有 90 行、DataSource 9000 行、table_path_count_after_access=None，
随后触发 9000/0.8 调整。AST WHERE 只含 customer 条件，优化器传播给
new_order/orders 的条件没有经过真实 histogram 访问估算。

统计加载 bridge 现在使用 source.pushed_down_conds 和现有 native ranger
重算索引访问范围，调用现有 access_cost::index_row_count，保留 RowEstimate
边界，并把 common-handle primary 估算同步到 table path。范围使用语句求值器、
范围配额与 fallback handler。列裁剪后保留连续可解析的索引前缀，缺少尾列不
意味着缺少仓库等值范围。第一次要求完整索引列的实验未修复（native-path-estimates.log），
采用前缀后 9000 行断言通过（native-path-prefix.log）。临时诊断代码已删除。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib`：1294 passed / 1 failed，
日志 `/tmp/native-path-executor-final.log`；condition eleven 越过新的 9000 行
及先前 10 行断言，仍失败于旧 MergeJoin 断言。`make lint` 退出 0，日志
`/tmp/native-path-lint.log`；`git diff --check` 通过。此提交修复访问估算缺口，
不代表 condition eleven 的所有物理计划差异已经解决，整体目标继续 active。

## 2026-09-10 table probe 范围选择率与扫描反算

本轮将 `lt/le/gt/ge` 单列条件经现有 ranger 构建 ColumnRange，调用
`get_row_count_by_column_ranges`（包括空 histogram 的 pseudo 回退），采用列
自身 collation。列间比较继续走原默认分支，不将范围条件硬编码成 0.8。
回归 `range_with_metadata_only_statistics_uses_range_estimator` 精确校验
`(rows/3-rows/1000)/rows`，符合 Go 对非 NULL 下界的 pseudo 估算；关闭新增
范围分支后失败，日志 `/tmp/metadata-range-red.log`。

table probe 保留被运行时 join key 替换的静态条件为 residual，按 Go
`constructDS2TableScanTask` 将输出行数除 residual selectivity 后应用 access
floor 与唯一键上限；扫描和 Selection 共享同一选择率。pseudo source 使用
原 pseudo range 路径，analyzed source 使用含范围处理的现有 helper。
orders 10.00 行断言修复前失败（`/tmp/table-probe-rows-red.log`），修复后通过。
此前退化的 subqueries 两层 IndexHashJoin 也恢复通过，与同 JSON 的 Go 对照一致。

验证命令与证据：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-planner --lib`，`/tmp/probe-range-planner-final.log`；
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib`，`/tmp/probe-range-executor-final.log`；
`make lint`，`/tmp/probe-range-lint.log`。实际结果为 planner 927 passed、
executor 1294 passed / 1 failed，lint 退出 0，使用 Ready 验证范围；
condition eleven 其余旧 MergeJoin/路径/行数断言仍待 Go 校正
与实现修复，不以新断言通过声明整个 case 或整体目标完成。

## 2026-09-10 subqueries 回归的 Go 证据

已导出 wait_orders/wait_lineitem 的原始 Rust fixture 统计到
`/tmp/wait-probe-oracle/`，Go master 加载同一 JSON 并执行 setup.sql 中的查询。
`go.out` 确认两层 IndexHashJoin（semi、anti semi），旧 subqueries 类型断言正确。
未修改生产实现时原测试通过，日志 `rust-export.log`。临时统计导出代码已移除。

Go l2 probe Selection 400.08、TableRangeScan 1203.85，过滤为
`lt(l_orderkey,100000)`；l3 Selection 320.06、TableRangeScan 400.08，过滤为
列间 `gt(l_receiptdate,l_commitdate)`。因此两个过滤选择率分别约 0.3323 和 0.8。
上一轮实验的 analyzed_filter_selectivity 对未识别的 lt 也返回 0.8，不能直接
用于 table probe 的范围反算。JSON 只有 NDV 而无 histogram buckets；现有
cardinality/row_count_estimator.rs::get_row_count_by_column_ranges 在 histogram
total_row_count 为零时回退 pseudo_row_count，提供了正确的后续实现入口。
应通过 ranger 构建 ColumnRange 并复用该估算器，而不是硬编码 1/3 或把所有
条件送入 analyzed helper。该证据解释了实验引入的计划排序回归，整体目标未完成。

## 2026-09-10 table probe 行数实验与回归约束

新增 condition eleven orders 扫描 10.00 行断言，当前实现失败（1.00），日志
`/tmp/table-probe-rows-red.log`。实验将被 runtime keys 替换的静态 access 条件
加入 residual 估算，先按选择率反算扫描，再应用 access floor 与唯一键上限。
orders 10 行断言通过，但 executor 全量出现新增
`driver::tests::subqueries::subqueries` 失败（subqueries.rs:2500，要求两个
decorrelated joins，实际一个）；全量 1293 passed / 2 failed，日志
`/tmp/table-probe-rows-experiment.log`。这尚不证明新的 plan 错误，也不证明旧
断言正确，需要用 Go master 对照该具体 SQL，并区分 pseudo 与 analyzed
选择率调用。为避免集成未经验证的行为，本轮实验生产改动已撤回；保留 orders
扫描行数红色回归。下一步须按 Go chosenRemained 构建及 Selectivity 路径完整
核实，而不是仅匹配列 ID 或在全路径调用 analyzed 估算器。整体目标未完成。

## 2026-09-10 table probe 统计版本保留

Go `constructDS2TableScanTask` 为 probe scan 设置
`StatsVersion: ds.StatsInfo().StatsVersion`。Rust 无 access floor 时新建的
StatsInfo 默认版本为零，导致 analyzed orders probe 错误显示 `stats:pseudo`。
现保留 source 统计版本，仍不附加 Go 此处刻意省略的 NDV。

在 condition eleven 的原始 analyzed 计划上新增断言：orders access 不得显示
`stats:pseudo`。旧实现运行退出 101，直接失败于此断言，日志
`/tmp/probe-version-red.log`。修复后该断言通过，随后仍失败于原有两层 MergeJoin
断言，日志 `/tmp/probe-version-green.log`；这个文件名不表示整个测试通过。
执行命令为 `RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib
tpcc_condition_eleven_pushes_filters_through_nested_derived_joins`。
`make lint` 退出 0（`/tmp/probe-version-lint.log`），`git diff --check` 通过。
这是可独立验证的版本丢失修复，condition eleven 的扫描行数、路径和 synthetic
COUNT 问题仍未解决，未声明完整 case 通过。

## 2026-09-10 condition eleven 精确 Go master 对照

在 `882cc27174` 上复现剩余 executor 测试，并使用现有 load_stats 导出 API
导出 customer/orders/new_order 的实际 fixture 统计。Go oracle 仍固定
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，以 unistore 启动并加载相同 JSON。
证据位于 `/tmp/tpcc-eleven-oracle/`：三份表名 JSON、setup.sql、setup.out、
query.sql、go-plan.out、rust.log。导出及可读计划临时日志代码已经移除。

重要更正：Go master 本例没有两层 MergeJoin；实际为顶层 IndexHashJoin，
new_order build 与 orders probe 的下层 IndexJoin，与 Rust join 类型一致。
旧断言不能作为 Go 行为依据。但不能仅修改类型让测试通过，因为以下真实差异
已由相同统计证实：

| 节点 | Go master | Rust |
| --- | --- | --- |
| new_order TableRangeScan | 9000 行 | 11250 行 |
| orders probe TableRangeScan | 10 行 | 1 行，错误显示 pseudo |
| orders probe Selection | 1 行 | 1 行 |
| customer probe | idx_customer IndexReader，扫描 10 行 | TableReader，扫描 1 行 |
| customer synthetic COUNT | Column#41 | ScalarQueryCol#-9223372036854775808 |

Go `constructDS2TableScanTask` 在过滤前计算 rowCount/selectivity，随后应用
accessRowsFloor 和唯一键上限，并保留统计版本；Rust 当前 runtime table path
直接把过滤后 rowCount 作为 scan 统计。该入口是下一修复点。new_order 静态
扫描另经过 CountAfterAccess < dsStats 时的 0.8 调整，需要独立检查加载统计
后的 native common-handle range 估算。未经证明，不把这些差异视为格式噪音。
本轮为 WIP 取证，没有新增生产修复，也未修改现有失败断言；整体目标 active。

## 2026-09-10 condition nine 修复并通过

已定位成本约十倍的原因：Rust 把 accessRowsFloor 直接作为 index filter 后的
runtime 行数，再除 index 选择率生成扫描行数。Go master
`exhaust_physical_plans.go::constructDS2IndexScanTask` 将下限用于
CountAfterAccess，并保持 CountAfterIndex/CountAfterAccess 比率。
本次沿现有 index-filter 选择率路径将下限换算到过滤后行数，并保留原始 runtime
行数作为另一项下限。无 index filter 时选择率为 1，唯一键仍最多一行。

原始候选逐节点证据 `/tmp/nine-inner-tree.log`：每次 probe 扫描
299990.000083 行、过滤后 29999.5 行；修复后 `/tmp/nine-floor-stage-green.log`
为扫描 29999.5、过滤后 3000。outer 8 行对应 EXPLAIN 扫描 239996、过滤后
24000，与此前相同 JSON 统计导入 Go master 的实测结果一致，计划自然选择
district outer / history inner IndexHashJoin。

根据固定 master 的 `load-analyzed.out`，将历史测试的 IndexJoin 类型改为
IndexHashJoin；根据 Go `base_physical_agg.go` 为 partial state 分配新 UniqueID
的逻辑，将错误的 `Column#0 -> Column#0` 改为精确验证 partial 输出等于 final
输入、final 输出与 partial 输出不同。其余树结构、24000/239996 行数和 access
keys 断言保留。不是删掉失败断言或重录 Rust golden。

更新后的同一回归，暂时恢复错误的 floor 阶段后退出 101，日志
`/tmp/nine-floor-regression-red.log`；恢复修复后退出 0，日志
`/tmp/nine-floor-stage-final.log`。所有临时树日志已移除。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1294 passed / 1 failed，剩 condition eleven；/tmp/nine-floor-executor.log
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 926 passed / 0 failed；/tmp/nine-floor-planner.log
make lint
# exit 0；/tmp/nine-floor-lint.log
git diff --check
# exit 0
```

本次修复现有 secondary-index residual-filter 路径的下限阶段；table-filter
补偿、独立 index selectivity 求值及其他 IndexJoin 路径的完整 Go 覆盖仍需审计，
不将此单例通过视为整个 Go planner 包完成。condition eleven 与其余完整集成
gates 仍待执行和修复，整体目标继续保持 active。

## 2026-09-10 IndexJoin fractional outer 平均 probe 修复

Go master `exhaust_physical_plans.go::enumerateIndexJoinByOuterIdx` 对正数
buildRows 直接计算 `EqualCondOutCnt / buildRows`，零或缺失统计时为零。
Rust dispatcher 错误地先将 outer 行数钳到至少 1，导致 outer 0.8、join 0.8
时平均 probe 为 0.8 而非 1。现移除此下限并保留 Go 的零值分支。
新增回归直接检查真实 `exhaust_physical_plans` 生成的 IndexJoin runtime
property；同时覆盖 outer 0.8 和 0，不以孤立公式测试代替接线验证。

修复前 `index_join_probe_average_preserves_fractional_outer_rows` 失败
（left 0.8 / right 1.0），日志 `/tmp/fractional-outer-red.log`。
修复后 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib` 全量 **926 passed**，日志 `/tmp/fractional-outer-planner.log`；
`make lint` 退出 0，日志 `/tmp/fractional-outer-lint.log`，采用 Ready profile。
TPCC 定向组仍 **5 passed / 2 failed**，日志 `/tmp/fractional-outer-tpcc.log`。

已记录 condition nine 的候选成本 `/tmp/nine-candidate-costs.log`：analyzed
district outer 的 IndexHashJoin 18564874.10，history outer 的 IndexJoin
6981768.48，双方输出估算均 0.8000267。此前相同 JSON 统计的 Go master
district outer IndexHashJoin 成本 1869948.23。因此后续应检查 inner 子计划
成本及 probe 缩放；不能为通过测试直接固定 join 方向。临时成本日志代码已移除。
两个 TPCC 的完整修复及其余 BLOCKER_RESOLUTION.md gates 仍未完成。

## 2026-09-10 physical 深链 ResolveIndices 栈溢出修复

原有 `physical::tests::deep_chain_walks_and_tears_down_without_recursion`
构造 40000 层 Selection，在 `resolve_indices()` 递归访问普通子节点时栈溢出，
32 MiB 栈仍失败，单独复现日志 `/tmp/resolve-depth-red.log`。
`schema()` 的继承路径也使用递归。本次将两处改为显式迭代，保留节点 schema
优先、Sequence 使用末子节点以及 Go 的子节点先于父节点绑定顺序。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`operator/physicalop/base_physical_plan.go::ResolveIndices` 遇首个错误返回；
Rust 在首个错误后停止绑定并重新组装全部普通子节点，新增回归验证树及后续
兄弟表达式未丢失或被继续绑定。`core/resolve_indices.go::resolveIndices4PhysicalSelection`
只在 Conditions 循环中查询子节点 schema，因此 Rust 空 Selection 同样直接返回，
避免空条件深链反复查找 schema 的二次开销。reader/CTE 等特殊字段的既有绑定
顺序未修改，本次不声称所有特殊嵌套字段均已去递归。

验证使用 Ready profile：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 925 passed / 0 failed，默认栈；/tmp/resolve-depth-planner-suite.log
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib physical::tests::deep_chain_walks_and_tears_down_without_recursion -- --exact
# 最终增加深层 schema 断言后 1 passed，0.13 秒；/tmp/resolve-depth-final.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1293 passed / 2 failed；/tmp/resolve-depth-executor-suite.log
make lint
# exit 0；/tmp/resolve-depth-lint.log
git diff --check
# exit 0
```

executor 两项失败仍为 condition nine / eleven 的物理计划差异，未修改其计划
期望。前一轮 Projection 修复另已通过 381 项逻辑规划器测试，独立提交为
`12ea885d12`。原始 readiness blocker 不再存在；整体目标仍未完成。

## 2026-09-10 Projection 组合 NDV 传播修复

固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，使用 Rust
`statistics_table_from_planner_statistics` 和 `gen_json_table_from_stats` 导出
condition nine 的原始 fixture，随后以 Go `LOAD STATS` 导入同一份 JSON。
证据目录 `/tmp/tpcc-master-oracle.0t3pI6/`：`district-rust-stats.json`、
`history-rust-stats.json`、`load-analyzed.sql`、`load-analyzed.out`。
Go 估算 join 0.80 行，district 8 行，history probe 24000 行；实际选择
IndexHashJoin。历史测试中的 IndexJoin 类型及 `Column#0` 不是该 master 的输出。

Rust 根因之一已定位到 `logical/projection.rs::derive_stats`：新建 StatsInfo
丢弃子节点 GroupNDVs，缓存路径也没有按 Go 刷新组合统计。Go
`logical_projection.go::getGroupNDVs` 仅映射直接列引用，丢弃无法完整映射的组合，
并按输出 UniqueID 排序。修复严格沿用此规则，包括缓存刷新和重复投影最后映射语义。
修复前 district 单列 NDV 都为 0.8，join 以分母 1 得到 6.400213；修复后
保留主键组合 NDV 8，join 得到 0.8000267，显示为 0.80，与 Go 一致。

回归 `projection_preserves_and_refreshes_renamed_group_ndvs` 修复前失败：
实际 `[]`，期望 `[GroupNdv { columns: [11, 12], ndv: 8.0 }]`；修复后通过。
日志 `/tmp/projection-group-red.log`、`/tmp/projection-group-green.log`。
定向命令 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib projection_preserves_and_refreshes_renamed_group_ndvs`。
`make lint` 退出 0，日志 `/tmp/projection-group-lint.log`。

TPCC 定向组仍为 5 passed / 2 failed，命令使用 `RUST_MIN_STACK=33554432`
及 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-executor --lib tpcc_condition_`。condition nine 仍存在物理路径选择差异，
condition eleven 仍缺少预期 MergeJoin，未放宽断言。
额外 planner 全量分别在默认栈和 32 MiB 栈触发
`physical::tests::deep_chain_walks_and_tears_down_without_recursion` 栈溢出；
日志 `/tmp/projection-group-planner-suite.log` 和
`/tmp/projection-group-planner-suite-stack.log`，不计为通过。
原始 readiness 竞态已由 `1f89c30b65` 修复，真实 access-path 已有通过证据；
当前失败不得再次归为 readiness 外部阻塞。整体目标保持未完成。

## 2026-09-10 condition nine analyzed 统计链路取证

当前提交 a075207030，聚合列绑定断言仍为本地 WIP。开启 Go master 正确的
Fix44855 默认值后，计划选择 history 为 Build、district 为 Probe，而非测试要求的
district 为 Build。日志 `/tmp/tpcc-nine-floor-probe.log`：history 全表 300000，
Selection 30000.50，HashAgg 1.00，过滤后 0.80；顶层 IndexJoin 6.40。

进一步 probe `/tmp/tpcc-nine-ndv-probe.log` 显示分组列 ID 正确为 17/18，输入行
30000.50000833347，两列 NDV 都为 1.000033334166685，group NDV 为空。
这来自 fixture 的原 NDV 10 按默认 skew=1 的选择率约 0.1 缩放，并非列 ID 丢失。
Go master `cardinality/ndv.go::estimateSkewedNDV` 和 Rust 对应公式均为
originalNDV * selectedRows / originalRows。此前称“一行分组异常”只是待验证假设，
不能直接认定该数字为实现 bug。

下步需要将同一 fixture 的 histogram/NDV/realtime 数据导入 Go master 并录制 analyzed
计划，核对历史断言的 0.80 join 行和 24000 probe 行。当前实际生产修复没有新增，
没有放宽这些断言；临时 DEBUG 探针已移除。完成了候选方向与统计来源的取证，
并不表示两个 TPCC failures 或整体 gates 已通过。

## 2026-09-10 Fix44855 的 probe 下限默认开启

固定 Go master `exhaust_physical_plans.go:868` 的 `indexJoinProbeAccessRowsFloor`
读取 `GetBoolWithDefault(..., true)`，而同文件约 1182 行的 NDV 上限读取 false。
Rust bridge 把下限也传为默认 false，混淆了两个不同默认值。现在下限默认 true，
显式 OFF 仍禁用；未改变上限算法。

原测试错误声称下限默认 OFF。保留其 OFF 行为断言，改为显式设置 OFF，再验证
默认计划与 ON 计划完全一致，且 ON 使用完整 join-key 二级索引。恢复旧默认 false
后新增默认一致性断言稳定失败（`/tmp/fix44855-default-red.log`，退出 101）；
恢复 true 后测试通过（`/tmp/fix44855-default-green.log`）。命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib index_join_probe_rows_use_only_the_access_paths_join_keys
make lint
```

该修复不代表 TPCC 已通过。condition nine 仍需核对 analyzed 计划：开启正确默认值后
inner 候选形状变化；condition eleven 仍无预期 MergeJoin。另有本地 WIP 将错误的
Column#0 常量断言改为 partial/final 实际绑定关系，依据 Go `BuildFinalModeAggregation`
分配新 UniqueID、保留 original final schema 的源码；该 WIP 尚未作为完成修复提交。

## 2026-09-10 按当前谓词验证 DataSource 统计缓存

修复 `InitStats`：把原 AST 谓词绑定到 DataSource 当前 schema，比对全部
pushed_down_conds。仅等价时复用原有 histogram/range 估算；新增或改变条件时交给
planner 当前表达式推导。AND/OR 允许结合顺序和排列变化，叶子保持现有类型/列身份
相等检查，条件一对一匹配。原 handle/index 路径估算保留。

新增 `derived_aggregate_null_filter_refreshes_source_statistics`，固定 master 在
全新数据库用完全相同 DDL/三行数据验证 Selection=8、scan=10、结果 d_id=1；
证据 `/tmp/tpcc-master-oracle.0t3pI6/derived-null-exact.out`。复用旧数据库的另一份
结果为 1/1.25，统计状态不同，明确不采用为此 fixture 的 oracle。
恢复旧无条件缓存写入后新增测试稳定失败 10 != 8
（`/tmp/predicate-cache-derived-red.log`）；修复后通过。

完整 executor `/tmp/predicate-cache-final-full.log`：1293 passed / 2 failed。
两个剩余失败是 TPCC condition eleven，以及 condition nine 已推进到 analyzed SUM
合成列编号断言（Column#27 -> Column#24 与旧 Column#0 -> Column#0）。
没有更改这些断言。global-count OR-of-BETWEEN 的 5.75 估算也通过，避免了直接清空
stats 实验的额外回归。`make lint` 退出 0（`/tmp/predicate-cache-lint.log`）。
命令：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib`。
Ready 仅覆盖本项，仍不代表整体目标或统计 package 完整移植完成。

## 2026-09-10 验证并否决直接重新推导统计

可撤销实验：`InitStats::descend` 在已有 pushed_down_conds 时清除预填 DataSource stats，
使后续 planner 从当前条件推导。condition-nine 的 district 8.00 和 IndexHashJoin 断言
均通过，测试推进到 analyzed 阶段 SUM 的合成列编号断言：实际 Column#27 -> Column#24，
旧测试要求 Column#0 -> Column#0。日志 `/tmp/tpcc-native-stats-experiment.log`。
这证明遗漏优化后条件影响实际成本选择，但不证明直接清空统计是可用修复。

完整 executor 实验 `/tmp/native-stats-full-experiment.log` 为 1286 passed / 8 failed，
较此前新增 global-count/index-range、TPCH Q14、common-handle ordered-limit、join-filter、
TPCC 两点查、TPCH Q2 六项失败。命令如下：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib
```

实验代码已撤销，原有 AST histogram/range 估算能力保留，红色回归断言保留。
正式接入应让优化后表达式复用完整估算入口；不能用只支持部分算子的
`logical/rewrite.rs::analyzed_filter_selectivity` 替代完整统计路径。
`ColumnResolver::resolve_expression` 可返回完整绑定表达式，值得用于接入设计，
但仅把整棵表达式包装成 opaque Column 会绕过 AST 条件分类，也不能作为修复。

## 2026-09-10 TPCC NULL 过滤估算的精确红色入口

在当前 `9197fccf9b` 上确认工作树干净后继续诊断。`InitStats::descend` 的临时探针
证明：第二次统计初始化时 district 的 `pushed_down_conds` 已包含 `d_w_id=1` 和
`NOT ISNULL(CAST(d_ytd AS DECIMAL(34,2)))`，但传给 access_cost 的原始 AST 仍只有
`d_w_id=1`，得到 selectivity=0.001。日志 `/tmp/tpcc-source-stats.log`。
`PlannerStatisticsLoad::initialize` 复用 `InitStats`，随后 `recursive_derive_stats`
发现 DataSource 已有 stats 就直接返回，遗漏新增过滤。

在原 condition-nine 回归中新增针对 district NULL Selection 的 estRows 断言，保持
原计划、结果行和其他断言。固定 master 的 `/tmp/tpcc-master-oracle.0t3pI6/condition-nine.out`
证明期望为 8.00。以下命令已执行，退出 101，耗时 0.03 秒，实际 10.00 != 8.00：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup
```

日志 `/tmp/tpcc-nine-filter-red.log`。临时 `[DEBUG-tpcc-source]` 探针已移除。
新增测试断言仍为本地 WIP，尚无生产修复，不将此项当作 green 或完成提交。
下一步应让统计初始化消费当前优化后的条件，并保留已有 column/index histogram、
参数上下文和路径估算；直接清空 stats 或只修 pseudo 分支不能证明完整修复。

## 2026-09-10 TPCC condition nine 的 master 实测取证

以同一固定 master binary 启动独立 unistore，完整 DDL、数据和 SQL 位于
`/tmp/tpcc-master-oracle.0t3pI6/condition-nine.sql`；结果和版本位于同目录
`condition-nine.out`。Go 返回 COUNT=1，选择 IndexHashJoin；Rust 也返回 1，但选择
IndexJoin。Go 的 district Selection 从 10 行降为 8，Rust 保留 10 行；Go 此次计划
没有 cop partial HashAgg，Rust 有。不能只把 Rust 测试中 IndexHashJoin 改成 IndexJoin。

临时成本和计划探针日志 `/tmp/tpcc9-cost-probe.log`、`/tmp/tpcc9-plan-probe.log`：
Rust 候选 build_rows=10、probe_rows=0.8、build_size=56、probe_size=80。
EXPLAIN 的 inner 8 行是乘以 outer 次数后的显示，不能与单次 probe 成本输入直接比较。
下一步核对 `logical/rewrite.rs::pseudo_range_filter_selectivity`、source stats bridge、
runtime avg_inner_row_count 和聚合候选成本。当前仍属未完成诊断，未修改 TPCC golden 或断言，
临时 DEBUG 探针已移除。三个已完成修复提交为 `9f18bd4f96`、`0c5d738da8`、`cd4514ae09`，
已分别推送 origin/hparser-integration。

## 2026-09-10 异步统计加载队列的并行测试隔离

完整并行 executor 曾在 `an_unloaded_column_is_queued_for_async_load` 随机失败，串行通过。
两个 fixture 共用 table_id=11 / column_id=1，加载完成测试在清理阶段删除另一个测试刚入队
的项。固定 master `pkg/statistics/column.go::ColumnStatsIsInvalid` 同样以物理表 ID 和列 ID
构造 `TableItemID`；生产队列的键语义正确，测试身份不应碰撞。

新增固定交错回归 `cleaning_loaded_fixture_preserves_other_fixture_pending_load`，先入队
unloaded fixture，再执行 loaded fixture 的清理和加载状态检查。旧固定 ID 下稳定失败
（`/tmp/stats-queue-red.log`）；改用模块内 AtomicI64 唯一表 ID 后通过。原有全部断言保留，
没有用串行化或生产锁隐藏竞争。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib index_async_load_queue_tests
# 8 passed；/tmp/stats-queue-green.log
RUST_MIN_STACK=33554432 bash -c 'for iteration in {1..30}; do rust/target/debug/deps/tidb_executor-980779d52cc3a360 index_async_load_queue_tests --test-threads=8 || exit; done'
# 30 轮全部通过；/tmp/stats-queue-stress.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib
# 1292 passed / 2 TPCC failed；/tmp/executor-queue-full.log
make lint
# 退出 0；/tmp/stats-queue-lint.log
```

本项 Ready 验证完成。整个目标仍未完成，剩余 TPCC 和外部完整 gates 继续处理。

## 2026-09-10 远端读取保留隐藏 record handle

`TableScanExec::open` 的旧逻辑认为远端不能提供 `_tidb_rowid`，无条件回退本地 cursor，
导致两个 write-range 回归的 cop 请求数为 0。实际上 `RemoteRowCursor::next_keyed_row`
已经在 staged merge 中保留真实 record key。现在从 codec 解码 Int handle，在虚拟列
materialization 后插入输出；需要额外 handle 的扫描不走直接 chunk 交接，以保证输出 schema。
这遵循 Go 以 record handle 定位 UPDATE/DELETE 行、保留 ExtraHandle 列的契约。

新增 `remote_extra_handle_survives_pruning_and_staged_merge` 覆盖只选 `_tidb_rowid`、
用户列值与物理 handle 不同、staged UPDATE 和 DELETE。临时恢复旧回退后测试稳定失败
（cop_scans 0 != 1，`/tmp/remote-handle-red.log`）；恢复修复后完整 remote-scan 组
28 passed（`/tmp/reader-final-green.log`），包括两个原始 write-range 失败和虚拟列回归。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib remote_scan::tests
make lint
git diff --check
```

以上通过，lint 日志 `/tmp/reader-final-lint.log`。同步远端 catalog 改动后重跑了该组。
虚拟依赖独立提交 `9f18bd4f96`；本项独立提交。Ready 仅覆盖本项修复，整个目标仍有
TPCC 计划选择、并行统计队列测试隔离和其他完整验收待完成；不能宣称所有 Rust cases 通过。

## 2026-09-10 Reader 虚拟列依赖补齐

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/planner/core/operator/physicalop/task_base.go` 在构造 root reader 前调用
`ExpandVirtualColumn`；`physical_utils.go` 递归收集依赖、去重并保留尾部 synthetic
handle，reader 外层投影恢复 SQL 输出。Rust 原先缺少这个步骤，导致强制二级索引读取
`b AS (a+10)` 时 schema 只有 b,c，绑定 a 报 `Can't find column with UniqueID 1 in schema`。

修复在 CopTask 转 root 时补齐依赖，透传 Selection/Limit/Sort/TopN 的输出 schema，
保留 Aggregate/Projection 的输出契约，最后隐藏新增列。已解析的 protobuf 元数据必须
包含依赖元数据，否则明确报错，不猜测列默认值。新增测试覆盖嵌套虚拟列、重复输出、
有/无主键、强制/忽略索引、SUM，以及 synthetic handle 排序和重复调用。

红色证据 `/tmp/virtual-write-red.log`、`/tmp/virtual-reader-disabled-red.log`；
恢复 expansion 后新增 SQL 测试通过，planner task 测试 59 passed。
仅此修复使原 write-range 测试推进到 UPDATE cop 请求计数断言，不能称该原用例已完全通过；
后者由独立的远端 record handle 修复处理。`make lint` 退出 0
（`/tmp/virtual-handle-lint.log`），`git diff --check` 通过。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib virtual_dependency_expansion_preserves_reader_output
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib task::
```

本项按 Ready 范围验证；整个目标仍进行中。串行 executor 在两个 reader 修复叠加时
1291 passed / 2 TPCC failed，并行另有共享统计队列竞争；这些剩余失败没有跳过或放宽。

## 2026-09-10 Fix52592 接入普通物理点查转换

固定 Go master 的实际 SQL 对照保存在 `/tmp/go-null-oracle.8cduRk/fix52592.txt`：`SELECT b FROM t WHERE a>=5 AND a<=5 AND b>1` 默认包含 Point_Get；`SET tidb_opt_fix_control='52592:ON'` 后为 TableReader/Selection/TableRangeScan，范围仍是 `[5,5]`。Go `pkg/planner/core/find_best_task.go` 在计算 `canConvertPointGet` 时读取此 fix，同时控制 table 和 index 路径。临时 Go unistore 已收到 SIGTERM 并正常退出，日志和 SQL 输出保留。

Rust 原来只在 AST fast-plan 入口读取 52592，普通 DispatchContext 没有该状态，导致相同查询开关 ON 后仍发点查。`/tmp/fix52592-red.log` 为精确失败证据（退出 101）。现在 bridge 读取执行上下文的 fix control，并传递 point conversion permission；dispatch 对普通 table/index 的 PointGet 和 BatchPointGet 转换统一应用。默认值保持允许转换，开关解析继续使用现有 Go-compatible bool getter。

原远端回归验证真实 get/scan 操作及 residual、ORDER BY、LIMIT，修复后通过。新增 `fix52592_disables_unique_index_point_conversion` 覆盖 unique index 单点和批量、缺失值、residual，并连续切换 OFF/ON/OFF 检查行结果及读操作，结果通过。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request
# 1 passed；/tmp/fix52592-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
fix52592_disables_unique_index_point_conversion
# 1 passed；/tmp/fix52592-unique-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1287 passed / 4 failed；/tmp/executor-fix52592.log
make lint
# 退出 0；/tmp/fix52592-lint.log
```

当前稳定失败为 TPCC condition nine、condition eleven、`write_range_reader_preserves_record_identity_and_staged_rows`、`write_range_reader_reconstructs_virtual_columns`。并行统计队列干扰和 BLOCKER_RESOLUTION.md 其余全量验收仍未完成；不能将上述串行结果称为整个 Rust 测试体系通过。

## 2026-09-10 NULL-bound 测试按实际 Go master 更正

旧 `an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request` 要求 `a BETWEEN NULL AND NULL` 读取并返回 100 行再过滤。该预期与实际 Go master 不符，不应通过增加 Rust 无效扫描满足它。

使用固定 master binary `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 启动独立 unistore（127.0.0.1:47891，路径 `/tmp/go-null-oracle.8cduRk`），执行真实 MySQL 协议 SQL：创建 clustered BIGINT 主键表、插入两行、EXPLAIN 和执行 NULL-bound 查询。`/tmp/go-null-oracle.8cduRk/results.txt` 保存版本和输出：`TableDual_6 0.00 root rows:0`，SELECT 返回空；正常 `a>97` 对照仍为 TableRangeScan。完整启动日志同目录。Go `logical_datasource.go::Conds2TableDual` 所在优化链路不能用旧注释中单独 `IsConstNull` 的行为替代。

测试现在断言 NULL-bound 查询结果为空、`StorageOps::default()`（无 get/scan/cop 请求）、wire rows=0；正常 BETWEEN 98 AND 100 的三行远端读取对照保留。这是用 Go 实测更正并加强请求断言，不是放宽结果断言、改 golden 或跳过 case；Rust 生产逻辑未改。

修复前失败见 `/tmp/executor-common-catalog.log`；`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request` 现在 1 passed（`/tmp/null-bound-green.log`）。`make lint` 退出 0（`/tmp/null-bound-lint.log`）。整体目标仍未完成。

## 2026-09-10 无显式 PRIMARY 索引的 common-handle catalog 修复

稳定红色复现：`dirty_common_handle_reads_share_the_remote_staged_merge` 在 unsigned common-handle `a=18446744073709551615` 下返回空（`/tmp/common-remote-red.log`，退出 101）。表已保存 common-handle offsets，但 catalog 没有独立 PRIMARY KvIndex。Go 的 `TableInfo.Indices` 始终保留 clustered PRIMARY 元数据，是否维护独立索引记录是另一个问题；Rust `handle_range::clustered_primary_metadata` 已实现该重建，planner catalog 却漏用它。

`Catalog::planner_catalog` 现在复用此函数，将缺失的 clustered PRIMARY 元数据加入 SourceTable 索引视图；已有真实 PRIMARY 保留其 ID、列和前缀。未新增物理索引记录，也未修改 SQL 断言。原 dirty common-handle 失败与 `composite_cnf_writes_fetch_only_the_matching_keys` 均通过。前者扩展了单点、重复 IN、缺失键断言，并验证这些查询不打开普通/远端扫描；覆盖 signed、unsigned、大小写不敏感字符串三种键，保留原 staged UPDATE/DELETE、LIMIT、聚合和投影断言。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
dirty_common_handle_reads_share_the_remote_staged_merge
# 1 passed；/tmp/common-catalog-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1284 passed / 6 failed；/tmp/executor-common-catalog.log
make lint
# 退出 0；/tmp/common-catalog-lint.log
```

remote_scan 模块为 22 passed / 4 failed（`/tmp/common-metadata-tests.log`），另有两项 TPCC 聚合失败。并行队列隔离及其他完整验收项仍未完成，整体目标保持未完成。

## 2026-09-10 unsigned staged ORDER BY 元数据修复

`unsigned_staged_rows_merge_in_the_readers_value_order` 修复前返回 `u64::MAX, 0, 1`，期望 `0, 1, u64::MAX`。已用单测独立复现，`/tmp/unsigned-merge-red.log` 退出 101。

定点探针排除了 ORDER BY 未传入和合并器 unsigned 比较缺失：reader 实际收到 keep_order=true、unsigned=true，但只有一段扫描；planner 传入的是 `Int(i64::MIN)..Int(i64::MAX)`。证据 `/tmp/unsigned-ranges-probe.log`。根因在 `Catalog::planner_catalog`：`SourceTable.pk_is_handle` 来自 `KvTable.pk_handle_offset`，`SourceColumn.is_primary_key` 只取 PRI_KEY flag。通过 `register_kv` 安装的表已设置 handle offset，但未复制该 flag，导致 planner 找不到 primary handle 列并以 signed LongLong 生成范围。

Go master `fdfadb96b2c` 的 `pkg/planner/core/operator/logicalop/logical_datasource.go::getPKIsHandleColFromSchema` 要求 PKIsHandle 和 primary-column flag 一致；`pkg/distsql/request_builder.go::SplitRangesAcrossInt64Boundary` 随后按 unsigned 值顺序拆成两段。Rust catalog 转换现在将 integer/common handle offsets 同时用于 primary-column 标识，保留已有 PRI_KEY 标识，保证传给 planner 的元数据与实际 storage handle 一致。未修改 signed/unsigned 编码或合并器比较，也没有给结果额外排序来遮盖问题。

原回归覆盖 staged UPDATE/DELETE、升降序、LIMIT、无序结果集合以及两段远端请求数。新增投影裁掉主键的 `SELECT b FROM t WHERE a>=1 ORDER BY a` 断言，结果仍按 unsigned 主键正确排序。所有 `[DEBUG-unsigned-merge]` 探针已删除。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
unsigned_staged_rows_merge_in_the_readers_value_order
# 1 passed；/tmp/unsigned-merge-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1282 passed / 8 failed；/tmp/executor-unsigned-metadata.log
make lint
# 退出 0；/tmp/unsigned-metadata-lint.log
```

remote_scan 模块运行 20 passed / 6 failed（`/tmp/remote-metadata-green.log`）；另外两项稳定失败仍为 TPCC 聚合。并行统计队列干扰仍未修复，串行结果不能代替并行门禁。其他 BLOCKER_RESOLUTION.md 完整验收项继续保留，整体目标未完成。

## 2026-09-10 clustered PRIMARY 点查返回空修复

`primary_batch_reads_use_written_common_handle_encoding` 在修复前稳定失败：DECIMAL(8,2) 主键插入 `(5.00,10),(6.00,20)` 后，IN 查询返回空，期望为 10、20。日志 `/tmp/common-handle-red.log`，退出 101。

根因不是 readiness，也不是仅缺少 short-handle padding。物理计划允许 PointGet/BatchPointGet 保留 PRIMARY 的 index ID；`UniqueIndexPointSourceExec::open` 无条件走普通唯一索引的 `_i` 键查询，而 `KvIndex.clustered_primary` 写入路径不维护独立索引键。Go master `fdfadb96b2c` 的 `pkg/executor/builder.go::isCommonHandleRead` 明确返回 `tbl.IsCommonHandle && idx.Primary`；`point_get.go::Next` 和 `batch_point_get.go::initialize` 都对该情形跳过普通索引读取并构造记录 handle。

Rust 现在使用已有 `clustered_primary` 元数据识别该路径，调用与写入相同的 `KvTable::common_handle_from_values`，保留表级 collation 和 CommonHandle padding；普通 unique index 继续走原有 lookup。扩展 SQL 回归，在已有 DECIMAL、VARCHAR、BIGINT UNSIGNED 的重复 IN/不存在值用例后分别检查单点查询。原有回归由红转绿，新增单点断言也通过。临时 `[DEBUG-common-key]` 探针已删除。

验证命令（仓库根目录）：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
primary_batch_reads_use_written_common_handle_encoding
# 1 passed；/tmp/common-handle-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1281 passed / 9 failed；/tmp/executor-common-primary-serial.log
make lint
# 退出 0；/tmp/common-primary-lint.log
```

并行全量为 1280 passed / 10 failed（`/tmp/executor-common-primary.log`）：PRIMARY case 已通过，另外出现 `access_cost::index_async_load_queue_tests::a_fully_loaded_column_is_not_queued`。串行不出现该额外失败；该模块多个测试共用全局异步队列的 table=11/column=1，测试隔离问题仍待独立修复，不能把串行通过等同于并行门禁通过。稳定的 9 项为下节 10 项去掉 PRIMARY case；其他完整门禁仍待验收。

## 2026-09-10 prepared filter 参数上下文修复

最终合并状态完整 access-path 再次退出 0：`/tmp/access-merged-readiness.log`；节点日志 `/tmp/access-merged-readiness-evidence/rust-node.log` 第 8 行包含 `cluster_session_node_ready`。沿用下节固定 Go master 和 nightly 命令，仅替换 `ACCESS_PATH_KEEP_LOGS` 目录。索引候选修复独立提交为 `891ed3c537`；prepared filter 修复单独提交。

根因：`tidb-expr/src/evaluator.rs::eval_vectorized_expression` 的非 deferred 常量分支调用 `constant.eval()`，没有传递执行上下文。SelectionExec 的向量过滤因此无法读取已经绑定的 prepared parameter，抛出 `unbound prepared parameter`。StmtContext 绑定和 clone 均保留参数；问题在过滤求值调用点。

Go master `fdfadb96b2c` 的 `pkg/expression/constant.go::Constant.VecEvalInt` 等方法调用 `genVecFromConstExpr`，通过当前 EvalContext 求值；`getLazyDatum` 从该上下文的 ParamValues 取参数。Rust 现与已有 projection batch 路径一致，对每个非空 batch 调用 `constant.eval_in(ctx)` 一次并广播结果；空 batch 不求值，deferred 表达式仍按行执行。

新增回归 `vector_filter_reads_current_parameter_once_per_nonempty_chunk`：复用过滤表达式，切换 NULL、0、1、-1 参数，验证 0、1、8 行的过滤 mask、NULL mask 和参数读取次数。新增回归和已有 `prepared_filter_and_projection_use_fresh_execution_state` 修复前均失败，日志 `/tmp/vector-filter-red.log`、`/tmp/prepared-filter-red.log`。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib evaluator::tests
# 11 passed，/tmp/vector-filter-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1280 passed / 10 failed，/tmp/executor-after-vector-filter.log
```

已 fast-forward 同步远端 `cb4043ed3a`，保留其他作者的 statement context 和字符映射提交。合并后再次运行 executor 全量，仍为 **1280 passed / 10 failed**（`/tmp/executor-merged-readiness.log`）；`make lint` 退出 0（`/tmp/readiness-merged-lint.log`）。新增 skyline 回归和已有 prepared filter 回归均通过。

仍失败的 10 项如下，未跳过、未改断言，且不属于 readiness：

- `driver::tests::aggregates::tpcc_condition_eleven_pushes_filters_through_nested_derived_joins`
- `driver::tests::aggregates::tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup`
- `driver::tests::point_get::primary_batch_reads_use_written_common_handle_encoding`
- `remote_scan::tests::a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request`
- `remote_scan::tests::an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request`
- `remote_scan::tests::composite_cnf_writes_fetch_only_the_matching_keys`
- `remote_scan::tests::dirty_common_handle_reads_share_the_remote_staged_merge`
- `remote_scan::tests::unsigned_staged_rows_merge_in_the_readers_value_order`
- `remote_scan::tests::write_range_reader_preserves_record_identity_and_staged_rows`
- `remote_scan::tests::write_range_reader_reconstructs_virtual_columns`

从仓库根目录执行 Cargo 不会自动读取 `rust/.cargo/config.toml`，全量测试需要显式设置 `RUST_MIN_STACK=33554432`。未设置时出现的 grouped subquery 栈溢出不是本轮参数修复的测试结论。

## 2026-09-10 最新结论：readiness 已解除，access-path 对照通过

以下为最新状态，后文保留早期失败和 WIP 记录作为时间线，不能用早期描述覆盖本节结果。

- 启动竞争修复已推送：`1f89c30b65`。TCP listener 先于应用 ready 是正常启动窗口；原脚本在窗口内只 grep 一次就退出并杀掉节点。现有界轮询 ready，同时检测进程退出。确定性回归验证延迟 ready、提前退出、永久无 ready 三种情况。
- Go master 基准固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，binary 为 `/tmp/tidb-go-master-oracle/bin/tidb-server`；配套 nightly PD/TiKV 版本和 hash 见下文。不能用 v8.5.6 的估算代替 master。
- 最新 skyline 代码完整运行 access-path，退出 0：`/tmp/access-final-skyline.log`。节点 `/tmp/access-final-skyline-evidence/rust-node.log` 第 8 行输出 `cluster_session_node_ready`，schema_version=68、stats_loaded=4。所有该脚本断言通过，没有修改 SQL golden 或放宽断言。
- 新增 ANALYZE 后复合索引支配回归，包含局部驱逐统计 payload 的情况；旧 cost-only 路径选 idx_rare，当前选择 Go master 的 idx_cover。analyzed 状态来自 existence metadata，完整 RowEstimate 保留估算上下界，skyline 使用 LIMIT 调整前的行数。
- 此修复的接入范围仅为有 analyzed 元数据且无残余 index filter 的普通索引候选。表路径、pseudo、残余 index filter 等未知指标候选继续保留供 cost 选择；不声称 Go planner package 或全部 skyline 行为完成。比较器提供的 fix-control relevance 回执尚未接入 statement tracing。

复现命令（仓库根目录 `/tmp/tidb-hparser-current`）：

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-final-skyline-evidence \
bash rust/scripts/run-realtikv-access-path.sh
bash rust/scripts/test-access-path-readiness.sh
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib skyline_tests
make lint
```

readiness 回归通过；skyline_tests 实际运行 3 项并全部通过（`/tmp/skyline-final-tests.log`）；`make lint` 退出 0（`/tmp/readiness-final-lint.log`）。全量 executor 基线为 1279 passed / 11 failed；禁用新增 skyline 后为 1278 passed / 12 failed，差异只有新增复合索引回归，证明这 11 项不是本次裁剪引入。日志分别为 `/tmp/skyline-executor-all-stack.log`、`/tmp/skyline-executor-disabled.log`。

整体质量目标仍未完成。readiness 不能再作为剩余 SQL/执行器失败的 blocker；后续应按实际失败继续修复。原 chunk panic 的完整 stack/reproduction 仍未建立，已有 guard 不能视为根因闭环。

## 2026-09-10 scan-pushdown

提交 `53fc7659a2` 在 Rust 1.97 下通过完整 Real TiKV scan-pushdown：

```text
RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-scan-pushdown.sh
EXIT:0
```

- `PI()` 与 Go 返回完全相同的 `id` 行集合。
- residual predicate 不再触发 `Chunk::column` 越界、1105 或断连接。
- COT predicate/projection 保持 Go errno `1690`。
- PD shutdown request-handle 警告仅影响 receipt 观察，不影响 SQL 结果比较。

## PD bootstrap 继续验证（2026-09-10）

新增的确定性 gRPC 回归先返回启动错误、下次返回合法 membership；无需 sleep 改写 fixture。
原始错误类型 `1` 是 UNKNOWN，`NOT_BOOTSTRAPPED` 是 `2`，之前将两者混同的分析不成立。
回归在仅重试 NOT_BOOTSTRAPPED 的实现下失败，错误精确为 `[PD:server:ErrServerNotStarted]server not started`。
现在同时重试该明确启动错误，保留其他错误及跨集群检查。退避上限参照 Go `pkg/store/store.go` / `pkg/util/misc.go`，启动重试依据 go.mod 固定 PD client 的 `servicediscovery.serviceDiscovery.initRetry`。

验证：`RUSTUP_TOOLCHAIN=nightly-2026-08-22 cargo test --manifest-path rust/Cargo.toml -p tidb-pd-client --test all`：45 passed。
日志：`/tmp/pd-bootstrap-red.log`、`/tmp/pd-bootstrap-green.log`。这是 WIP 范围验证，未宣称全部门禁通过。

lock-recovery 最近保留日志显示 `the cluster catalog has no table mysql.tidb`，不是测试成功。
进一步阅读脚本发现它使用 Go `beforeCommitSecondaries` failpoint 构造已提交 primary / 未提交 secondary，再由 Rust 集成测试验证锁恢复。
此前强行添加 Rust read-table wrapper 是 harness 方向错误；下一步恢复 Go fixture server，仍由 Rust 测试验证核心行为。不能把缺失系统表当成这条测试必须移植的前置条件。

## lock-recovery 端到端通过（2026-09-10）

修正脚本后使用 Go failpoint-enabled fixture：

```text
LOCK_RECOVERY_TIDB_SERVER=/Users/chenhuansheng/Documents/GitHub/db9-ai/hparser-integration-tidb/bin/tidb-server
RUSTUP_TOOLCHAIN=nightly-2026-08-22 bash rust/scripts/run-realtikv-lock-recovery.sh
lock-recovery lock recovery passed: campaign13_lock_recovery status=committed ... cop_attempts=2 publications=1
```

这确认 PD readiness 重试、Go failpoint 的 primary/secondary 构造、Rust lock resolver 和第二次 cop response 全部跨进程工作。此前 `mysql.tidb` 缺失来自错误地用 Rust server 承担 Go fixture；现已移除该 wrapper。测试过滤器也改为 aggregate target 的全限定 case 名，避免 0 tests 假失败。

## access-path 当前证据（2026-09-10）

完整脚本运行完成，但统计阶段仍有 2 个硬失败、7 个路径差异：

- `bucket=1 AND rare=7`：Go `idx_cover` 估算 1（伪统计阶段 0.10），Rust 伪统计 1.25。
- `SELECT bucket,rare ... bucket=1`：Go 覆盖索引估算 500，Rust 2000。
- ANALYZE 后 Rust 对 `rare=7`、`rare>0`、`u(a=1,b=2)` 等选择退化为全表或错误索引，说明问题不止成本常数，而是 Go `GetRowCountByIndexRanges` 的复合索引等值前缀、直方图 total-row-count 和列统计回退语义未完整接入。

权威运行：`RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-access-path.sh`。该结果未宣称通过；下一修复类别继续对照 `pkg/planner/cardinality/row_count_index.go` 与 Rust `row_count_estimator.rs`。

## 2026-09-10 access-path reload 修复验证

提交 `c058347ad5` 修复了 Go 语义中的统计装载条件：当缓存项 histogram 版本相同但处于 evicted（仅元数据）状态时，必须重新读取完整 histogram，而不能因版本比较直接复用。该修复已独立推送到 `origin/hparser-integration`。

重跑 `run-realtikv-access-path.sh` 后仍观察到相同的 2 个 hard failure 与 7 个路径 divergence，说明当前剩余问题位于统计 payload 本身或 planner 的索引估算调用链，而非该缓存复用条件。证据已保留在 `/tmp/access-path-rerun.log`，下一步继续检查 index histogram payload 与 range 编码的一致性。

追加提交 `5de8ec9007`：修正前一提交中的分支方向，evicted payload（`!is_full_load`）现在进入 `load_item(..., full_load=true)`，避免仅保留 metadata。已推送到 `origin/hparser-integration`。

## 2026-09-10 剩余 skyline 差异的最小回归（WIP）

本轮进一步修正 candidate 的 analyzed 判断：Go `isCandidatesPseudo` 使用 `ColAndIdxExistenceMap.HasAnalyzed`，与 NDV payload 是否在内存中无关。WIP 原先通过 `hist.index_ndvs().contains_key` 判断会在局部 eviction 时漏掉复合索引。现在 bridge 传递 `index_stats_existence` 中为 true 的索引 ID 到 `DataSource.analyzed_index_ids`，每次统计初始化重建，并在 clone 时保留；dispatch 使用这个集合。

扩展已有 ANALYZE 回归为两种状态：完整 payload；仅驱逐多列索引 payload、保留单列索引 payload 和全部 analyzed 元数据。曾尝试全部驱逐，但旧逻辑也通过，不能作为修复证据，已改成局部驱逐。临时恢复旧 NDV 判断后运行同一测试，`evicted=true` 明确失败并输出 idx_rare，日志 `/tmp/skyline-eviction-red.log`（退出 101）。恢复 existence map 判断后执行 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::tests::primary_keys`，**15 passed**，日志 `/tmp/skyline-existence-green.log`（退出 0）。`git diff --check` 通过。本轮验证为 WIP；最新 existence map 修改尚未完整集成重跑，未提交推送；表路径/pseudo/residual-filter 全面接入仍待完成。

本轮普通 dispatch 已接入第一组可完整构造的索引候选：有已分析索引统计、无残余 index filter 时，构造 access/index 列长度集合、covering、property、global/MV、eq/IN 和 min/max 估算，使用共享逆序循环保留 skyline，然后才对保留任务比较 cost。其他候选暂以未知 metrics 保留，不凭空补风险或 CountAfterIndex。该范围仍未达到 Go 全部 skyline 行为，尤其表路径、pseudo 分类及 residual index filter 的 Selectivity 尚待补齐，不以本次 case 通过代替完整目标。

验证发生实质变化：`analyzed_composite_index_dominates_single_equality_index` 从失败变为通过，实际选择 idx_cover；`driver::tests::primary_keys` 实际 **15 passed**；固定 master/nightly 的 `run-realtikv-access-path.sh` 输出 **the access-path differential passed**，完整日志 `/tmp/access-skyline-wip.log`，节点日志 `/tmp/access-skyline-wip-evidence/`。此集成构建之后又将 skyline 使用的访问行数改为 LIMIT 调整前的值（Go 在物理 scan 调整前做 skyline）；最新代码重新运行主键测试 15 passed、`make lint` 退出 0，但该最后修改未重新进行完整集成。集成命令与上节固定 master 命令相同，仅日志目录改为上述路径。所有这些实现仍为未提交 WIP，下一步补齐剩余候选信息后再独立提交推送。

后续核验模块树发现重要更正：`find_best_task/candidate.rs` 和 `index_join/candidate.rs` 中存在比较器及其引用，但 `find_best_task.rs` 原本没有声明 candidate 模块；不能据文件搜索就声称其在运行时已由 IndexJoin 使用。本轮已声明 `pub mod candidate`，将 Go `skylinePruning` 的逆序淘汰循环实现为 `insert_skyline_candidate`，保留互不支配候选，返回 pseudo winner / fix45132 使用信息，允许跳过 TiFlash。第一次测试过滤实际运行 0 项，未计入通过；接线后处理测试闭包生命周期错误并重跑，`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib skyline_tests` 实际 **3 passed**，涵盖双向插入顺序下的复合索引支配、互不支配/属性冲突保留和 TiFlash 保留，日志 `/tmp/skyline-frontier-tests.log`。此为可测试的裁剪原语，普通 dispatch 的候选构造与调用仍未接入，完整 SQL 红色回归尚未解决，改动仍未提交推送。

后续 WIP 已发现并补足一个前置数据缺口：`driver/planner_bridge.rs` 原先调用只返回 `.est` 的 `index_range_row_count`，丢弃估算器已有的 `min_est/max_est`。现保存完整 `RowEstimate` 到 `DataSource.index_path_row_estimates`，重新配置统计时清空旧数据，并在 DataSource clone 中保留。既有 `index_path_count_after_access` 继续接收相同 est，尚未改变路径选择。`RUSTUP_TOOLCHAIN=1.97 cargo check --manifest-path rust/Cargo.toml -p tidb-executor` 已退出 0（日志 `/tmp/skyline-bounds-check.log`）。这些更改尚未提交推送；仍需接入候选属性及 Go skyline 淘汰循环，再验证下方红色回归。不能把数据传递完成等同于 planner bug 已解决。

新增本地测试 `driver::tests::primary_keys::analyzed_composite_index_dominates_single_equality_index`：创建三个索引，插入 2000 行，调用真实 `analyze_kv_table` 并安装统计，最后 EXPLAIN `SELECT * FROM t WHERE bucket=1 AND rare=7`。命令：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib analyzed_composite_index_dominates_single_equality_index
```

已运行，退出 101；测试本身耗时 0.06 秒。实际为 `IndexLookUp -> IndexRangeScan(idx_rare) + Selection(eq(bucket,1)) -> TableRowIDScan`，估算 1 行。期望 Go master 的 `idx_cover`，与上一完整集成对照相同。原始输出 `/tmp/analyzed-skyline-red.log`。该测试目前有意保持红色作为后续修复入口，不代表修复完成。

静态调用链证据：共享 `find_best_task/candidate.rs::compare_candidates` 只有 `find_best_task/index_join/candidate.rs` 调用；普通 `dispatch.rs` 在 `for path in ds.enumerated_paths` 中直接比较 task cost，只对 IndexJoin 另做 skyline count 比较。`logical/rule_prune_indexes.rs` 的提前过滤是相关列评分/数量限制，不是 Go `compareCandidates` 的支配关系裁剪。因此下一步应补普通 DataSource 的 skyline candidate 构建和比较，并保留 Go 的 property、covering、pseudo、risk、eq/IN 与 fix45132 约束；不能只按索引列数强制选择复合索引。本轮未改变生产计划选择逻辑，使用 WIP 验证范围。

## 2026-09-10 固定 Go master 的完整 access-path 结果

已建立干净 worktree `/tmp/tidb-go-master-oracle`，固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。构建命令 `env -u LDFLAGS make server` 退出 0，`bin/tidb-server -V` 确认该 hash、Go 1.26.2、无 dirty 后缀；`git status --short` 为空。首次直接 `make server` 因本机 LDFLAGS 中的 ICU `-L...` 参数被传给 Go linker 而失败，仅清除该命令的环境变量即解决，未修改 Go 源码。

master Go 与 PD v8.5.6 组合不能 bootstrap：PD 返回 `Unimplemented: unknown method QueryRegion for service pdpb.PD`。该运行日志为 `/tmp/access-master-diff.log`。新增 `ACCESS_PATH_CLUSTER_VERSION` 参数，允许选择兼容的 PD/TiKV，默认值仍为 v8.5.6，输出中明确记录版本。随后使用本机已有 nightly：PD `d71c0396ac26eb96a969c28b0efdafef9dd5aac3`，TiKV `1167092fea81ff8cb16ac49779f5702f7e225e79`。

完整运行命令：

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-master-nightly-evidence \
bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-master-nightly-diff.log 2>&1
```

结果：**0 failure(s), 1 divergent choice(s)，退出 1**。Go/Rust 的 pseudo 复合索引均为 1.25；ANALYZE 后 covering 查询均为 500；大表查询返回行对照通过。唯一差异为 ANALYZE 后 `SELECT * FROM t WHERE bucket=1 AND rare=7`：Go 使用 `idx_cover(bucket,rare)`，Rust 使用 `idx_rare(rare)`，双方 estRows 都为 1。该差异仍保留为失败，尚需对照 master skyline pruning 和 cost 调用链修复。不能把旧运行的 2 failures / 7 divergences 继续描述为此次 master 基准的结果，也不能仅凭一次运行将所有历史统计加载差异都归因于版本。

节点日志位于 `/tmp/access-master-nightly-evidence/`；此次没有改动 Rust 生产实现或 SQL 断言。验证 profile 为 Ready 范围：`make lint`、`bash -n rust/scripts/run-realtikv-access-path.sh`、`bash rust/scripts/test-access-path-readiness.sh` 均退出 0。整体目标仍未完成，其余脚本和 Go integration suites 也不能据此视为通过。

## 2026-09-10 access-path 的 Go 基准版本缺口

脚本的 Go 节点固定为 TiUP **v8.5.6**，不是用户要求的 Go master。对照 `origin/master` 的 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，`pkg/planner/cardinality/selectivity.go` 在返回前明确执行 `ret = max(ret, 1.0/float64(coll.RealtimeCount))`。该下限来自 `11b8149926`（2026-05-28，#67841）。随后 `pkg/planner/core/stats.go::adjustCountAfterAccess` 将较低的路径估算调整为 `ds.StatsInfo().RowCount / cost.SelectionFactor`。

因此 `bucket=1 AND rare=7` 的原始 pseudo index 估算虽为 `0.10`，经 master 一行下限和 `0.8` selection factor 调整后为 `1.25`。Rust 现有结果与这段 master 源码一致，不能为追平 v8.5.6 的 `0.10` 删除下限。本轮保留生产估算逻辑，并增加 EXPLAIN characterization 测试 `pseudo_composite_index_applies_master_selectivity_floor`，核对实际 scan 节点（不是 reader 的 operator info 引用）及 idx_cover。

脚本新增 `ACCESS_PATH_TIDB_SERVER`：指定可执行的 Go binary 后使用 TiUP 的 `--db.binpath`，并打印该 binary 的 `-V`；未指定时明确打印旧版本基准提示。PD/TiKV 仍为原版本，SQL 断言与 golden 均未修改。当前可见本地 Go binary 为 `7a8404bd17-dirty`，不能称为固定 master 的验证。尚需构建指定 master revision 并用此入口重跑，剩余 ANALYZE 后统计估算错误也未解决。

验证：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib pseudo_composite_index_applies_master_selectivity_floor`；`bash -n rust/scripts/run-realtikv-access-path.sh`；`bash rust/scripts/test-access-path-readiness.sh`；`git diff --check`；`make lint`。这是基准版本取证和测试入口改进，不是全部 access-path failures 的完成声明。

## 2026-09-10 readiness 竞争修复与证据更正

此前对话将一次 `never reported ready` 输出反复描述为已复现的服务端死锁，证据不足，应撤回。旧脚本只等待 TCP 端口开放，然后立即执行一次 ready 日志 grep；grep 失败就触发 EXIT trap 杀掉节点。因此日志停在 `mysql_tls` 不足以证明节点持续阻塞。

Rust `sql_node.rs::ConcurrentSqlNode::bind` 在创建 memory runners 之前调用 `TcpListener::bind`，`cluster_session_node/boot.rs` 则在 bind 返回、安装 signal handler 后才输出 ready。端口可连接与 ready 日志之间存在正常时序窗口。Go source of truth `origin/master`（`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`）的 `pkg/server/server.go::Run` 同样先 `initTiDBListener`，随后启动网络 listener，最后才设置 `s.health.Store(true)`；TCP 可连接不是应用 ready 的充分条件。

修改 `run-realtikv-access-path.sh`：端口开放后轮询原 ready 事件，最多等待 180 秒；进程退出立即失败，持续无 ready 仍超时失败并打印日志。未删除 ready 断言，未改动 Rust 服务启动或任何 SQL golden。

回归 `test-access-path-readiness.sh` 从生产脚本提取实际启动检查代码，模拟端口已开放但 ready 延迟一秒。修复前退出 1，打印 `the Rust node never reported ready` 和 `mysql_tls`；修复后通过。另验证提前退出和活进程永久无 ready 都被拒绝，超时测试通过推进 Bash SECONDS 避免等待三分钟。

验证命令与结果：

```bash
bash rust/scripts/test-access-path-readiness.sh
# PASS: delayed ready; exited node rejected; stuck node rejected
bash -n rust/scripts/run-realtikv-access-path.sh rust/scripts/test-access-path-readiness.sh
git diff --check
make lint
# 均退出 0，使用 Ready 验证范围
RUSTUP_TOOLCHAIN=1.97 ACCESS_PATH_KEEP_LOGS=/tmp/access-readiness-evidence \
  bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-readiness-fixed.log 2>&1
```

真实运行已输出 `cluster_session_node_ready`，地址 `127.0.0.1:47600`，schema_version 60；完成所有 access-path SQL 对照，最后因原有 **2 failures / 7 divergent choices** 退出 1。节点日志保存在 `/tmp/access-readiness-evidence/rust-node.log`。启动 blocker 已解除，整体目标仍未完成；剩余失败为 strict-superset pseudo estRows 和 ANALYZE 后 covering index estRows，须继续对照 Go cardinality 实现修复，不能将本次运行记为全套通过。

## 2026-09-10 chunk panic 修复

定位到 StreamAgg DECIMAL SUM 快速路径使用原始列 offset；child chunk prune 后列数不足时会在 `chunk.rs:212` 越界。现已在两个快速路径入口验证 `index < chunk.num_cols()`，布局不匹配时回退通用表达式求值，避免 panic 并保持 Go 语义。提交：`rust: guard decimal stream aggregation column access`。`tidb-executor` 聚合相关测试编译完成；已有 prepared plan receipt 测试失败与本改动无关，需继续按 Go planner source of truth 处理。
