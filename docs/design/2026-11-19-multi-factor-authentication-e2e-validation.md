# 原生 MFA 实现后端到端验证报告

日期：2026-11-19。目标：在当前 `mfa-native-support` 分支、基于 `release-7.1.9-0` 的源码上，按用户文档验证 MySQL 客户端直连 TiDB 的原生 MFA 链路。该报告只记录本次实现后的结果；IP/网段访问控制和 LDAP Simple 单插件的既有验证分别见用户文档引用的报告。

## 环境与边界

| 项目 | 实际值 |
| --- | --- |
| TiDB | 当前工作树源码；`mfa-native-support`；基线提交 `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26` |
| 构建 | `go1.27.3`；`go build -tags codes`；使用 `/tmp/tidb-e2e.mod` 临时覆盖 etcd/otelgrpc 兼容性，未修改仓库 `go.mod` |
| 存储 | 隔离单节点 `unistore`，临时数据目录 |
| 客户端 | Homebrew MySQL CLI 8.4.9 |
| LDAP | Docker 镜像 `tidb-ldap-probe:alpine3.22`，OpenLDAP 2.6.8；独立服务账户和两个目录用户 |
| TiDB TLS | 临时 CA/服务端证书；MySQL CLI 使用 `--ssl-mode=VERIFY_IDENTITY --ssl-ca=...` |
| TiDB 到 LDAP TLS | 本轮 MFA 链路使用明文 LDAP 连接，避免把既有 LDAP TLS 验证误算为本轮新增能力；StartTLS/LDAPS 的既有能力见 `2026-11-18-authentication-runtime-validation.md` |

LDAP 用户条目使用显式 DN。目录用户密码在 LDAP 中维护，TiDB 账号只保存 `authentication_string` 中的 DN，不保存 LDAP 密码。

## 结果

以下成功连接均执行了普通 SQL，并返回了 `CURRENT_USER()`；拒绝用例返回 1045，未出现因子降级。

| 场景 | 结果 |
| --- | --- |
| `CREATE USER`：`mysql_native_password`＋LDAP Simple | 通过；SHOW CREATE USER 顺序与 JSON 配置一致 |
| `CREATE USER`：`caching_sha2_password`＋LDAP Simple | 通过；在 TiDB TLS 上完成 SHA-2 全量交换后继续 LDAP 因子 |
| 两因子正确第一、第二密码 | 通过；MySQL CLI `--password1/--password2` 直连成功 |
| 两因子第二密码错误或缺失 | 通过拒绝；不会退回单因子 |
| `ADD 3 FACTOR` 后三因子正确登录 | 通过；`--password3` 被正确请求和验证 |
| 三因子第二或第三密码错误、第三密码缺失 | 通过拒绝；不会退回两因子 |
| `SET PASSWORD` 修改第一因子后登录 | 通过；新第一密码成功，旧第一密码失败，后续 LDAP 因子仍保留 |
| `ALTER ... ADD/MODIFY/DROP FACTOR`、`SHOW CREATE USER` | 通过；逆序 MODIFY 按明确因子编号更新，删除后配置和登录顺序正确 |
| `REQUIRE SSL` 与 MFA | 通过；非 TLS 连接拒绝，VERIFY_IDENTITY TLS 连接完成全部因子 |
| `authentication_policy='*,*'` | 通过；无 `AUTHENTICATION_POLICY_ADMIN` 的账号创建单因子被拒绝，创建两因子成功；旧单因子账号在该 policy 下自助 `SET PASSWORD` 成功；最后恢复 `*,,'` |
| 客户端不发送 MFA capability | 通过拒绝；原始 MySQL 协议客户端只提交第一因子时收到 1045 |
| LDAP 服务账户密码日志 | 通过脱敏检查；`SET GLOBAL authentication_ldap_simple_bind_root_pwd` 的 info 日志为 `val=******`，日志中未出现明文 |

## 发现并修复的实现问题

首次使用 MySQL 8.4.9 连接时，客户端报 2027 `Malformed packet`。抓取协议包发现 TiDB 将 AuthNextFactor 缓冲区直接交给 `PacketIO.WritePacket`，但该接口要求调用方预留 4 字节包头，导致插件名 `mysql_clear_password` 被截断为 `ql_clear_password`。修复为复用现有 `arena.AllocWithLen(4, ...)` 写包约定，并增加 `TestRequestNextAuthFactorPacket` 防止回归。修复后同一条 MySQL CLI 链路完成了所有上述成功/失败场景。

## 自动化测试

通过：

```text
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/executor -run TestMFAUserFactorLifecycle -count=1
go test -mod=mod ./pkg/parser -run TestMFAUserStatements -count=1  # 在 pkg/parser 子模块目录执行
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/privilege/privileges -run TestDecodeMultiFactorAuth -count=1
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/server -run TestRequestNextAuthFactorPacket -count=1
go build -mod=mod -modfile=/tmp/tidb-e2e.mod -tags codes ./cmd/tidb-server
```

`pkg/server` 中依赖 `failpoint.Inject` 的既有测试在直接运行 `go test` 时会因仓库未先执行 `failpoint-enable` 而走到真实认证读包路径；这属于测试启动方式问题，不能作为 MFA 失败结论。MFA 包头回归测试不依赖该 failpoint，已单独通过。正式 CI 仍应按仓库 Makefile 先启用 failpoint 再运行完整 server 测试。

## 收尾重跑

在完成自动化测试后又恢复隔离 OpenLDAP、当前源码构建的 TiDB 和 MySQL 8.4.9，重新执行直连验收。使用 `VERIFY_IDENTITY` 和临时 CA，结果如下：

| 场景 | 结果 |
| --- | --- |
| 两因子 native＋LDAP 正确凭据 | 成功，返回 `CURRENT_USER()` |
| 两因子第二凭据错误或缺失 | 均以 1045 拒绝 |
| 三因子 native＋LDAP＋LDAP 正确凭据 | 成功，返回 `CURRENT_USER()` |
| 三因子第三凭据错误或缺失 | 均以 1045 拒绝 |
| `caching_sha2_password`＋LDAP | 成功完成 SHA-2 交换和 LDAP Bind |
| `REQUIRE SSL` | 明文连接 1045，TLS 连接成功 |
| `SET PASSWORD` 后续因子保留 | 旧第一密码 1045，新第一密码与 LDAP 因子成功 |
| ADD/MODIFY/DROP FACTOR 与 SHOW CREATE USER | 成功，因子编号和顺序符合预期 |
| 不带 MFA capability 的原始协议客户端 | 1045 拒绝 |
| LDAP 服务账户密码日志 | SET GLOBAL 后仅记录 `val=******`，未出现明文 |

本次重跑使用临时目录和容器，测试结束后已停止并删除临时进程与容器；不会修改仓库 `go.mod` 或正式部署环境。

## 尚未覆盖

本轮未覆盖 COM_CHANGE_USER 的真实 MySQL CLI 调用、多节点传播、代理终止 TLS、会话迁移、其他驱动、并发压力，以及 LDAP StartTLS/LDAPS 与 MFA 的组合。LDAP TLS 和现有单插件行为已有独立实测；这些组合仍应在正式集成环境继续验收。用户文档中的 SQL 语法、账号管理、策略和直连登录示例已分别通过 parser、TiDB 实例和 MySQL CLI 验证；未覆盖项不应被表述为已通过。
