# TiDB 原生多因素认证测试验收

## 1. 测试目标和范围

本报告记录 TiDB 原生 MFA 的实现验收证据，覆盖账号管理、MySQL 协议、第一因子与 LDAP Simple 后续因子、策略、TLS、密码修改和日志脱敏。验收范围是 MySQL 命令行客户端直连 TiDB；LDAP Simple 单插件功能沿用既有能力，不计入本报告的新增实现范围。

## 2. 测试环境

| 项目 | 环境 |
| --- | --- |
| TiDB | `release-7.1.9-0` 基线上的当前 MFA 实现 |
| Go | `go1.27.3` |
| 存储 | 隔离单节点 `unistore`，临时数据目录 |
| 客户端 | MySQL CLI 8.4.9 |
| LDAP | OpenLDAP 2.6.8，独立服务账户和目录用户 |
| TiDB TLS | 临时 CA 和服务端证书；客户端使用 `VERIFY_IDENTITY` |
| LDAP 连接 | MFA 直连验收使用 LDAP Simple；LDAP TLS 与单插件 TLS 行为属于已有能力 |

LDAP 用户使用显式 DN。LDAP 密码只保存在目录服务，TiDB 的 MFA JSON 保存插件名和身份字符串。

## 3. 自动化测试

以下测试和构建通过：

```text
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/executor -run TestMFAUserFactorLifecycle -count=1
(cd pkg/parser && go test -mod=mod . -run TestMFAUserStatements -count=1)
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/privilege/privileges -run TestDecodeMultiFactorAuth -count=1
go test -mod=mod -modfile=/tmp/tidb-e2e.mod -tags=intest ./pkg/server -run TestRequestNextAuthFactorPacket -count=1
go build -mod=mod -modfile=/tmp/tidb-e2e.mod -tags codes ./cmd/tidb-server
```

覆盖点：

- parser 解析 `CREATE USER ... AND IDENTIFIED` 及 `ADD/MODIFY/DROP N FACTOR`；
- MFA JSON 的合法、缺失和损坏数据；
- CREATE、ADD、逆序 MODIFY、DROP 的账号因子生命周期；
- `AuthNextFactor` 包头、插件名和 `mysql_clear_password` 映射；
- MFA 账号的认证和失败分类。

`TestRequestNextAuthFactorPacket` 验证包包含正确的四字节包头、`0x02` 类型字节和以 NUL 结尾的插件名。完整 `pkg/server` 测试仍应通过仓库规定的 failpoint 启用流程运行；该前置流程不影响上述 MFA 回归测试结论。

## 4. 账号管理和 SQL 结果

| 场景 | 结果 |
| --- | --- |
| 创建 native＋LDAP 两因子账号 | 成功；`SHOW CREATE USER` 顺序与 JSON 配置一致 |
| 创建 caching SHA-2＋LDAP 两因子账号 | 成功 |
| 增加第三因子 | 成功；第三因子按位置 3 保存 |
| 修改第二、第三因子 | 成功；按明确编号更新 |
| 删除第三因子 | 成功；保留第二因子 |
| 删除第二因子 | 成功；原第三因子前移为第二因子 |
| `SET PASSWORD` 修改第一因子 | 成功；后续因子保留，旧第一密码失败、新第一密码成功 |
| `authentication_policy='*,*'` | 无策略管理员的单因子创建被拒绝；两因子创建成功；既有单因子账号的 `SET PASSWORD` 保持可用；测试结束恢复默认策略 |
| `REQUIRE SSL` 与 MFA | TLS 连接完成全部因子；非 TLS 连接被拒绝 |
| 缺少第二或第三因子定义 | 账号管理语句拒绝，不生成空缺序号 |
| 两个 native 或 native/caching SHA-2 作为后续因子 | 按 MySQL 兼容规则拒绝 |

本文档和[用户文档](2026-11-18-multi-factor-authentication-user-guide.md)中的 SQL 示例均通过 parser 解析；账号管理示例在隔离 TiDB 实例中执行。

## 5. 端到端认证矩阵

每个成功场景都执行普通 SQL 并返回 `CURRENT_USER()`；拒绝场景均未建立 SQL 会话。

| 场景 | 结果 |
| --- | --- |
| native＋LDAP，两个凭据正确 | 成功 |
| caching SHA-2＋LDAP，两个凭据正确 | 成功；完成 SHA-2 交换后继续 LDAP Bind |
| 第二因子密码错误 | 1045 拒绝，不退回单因子 |
| 第二因子密码缺失 | 1045 拒绝，不退回单因子 |
| 三因子 native＋LDAP＋LDAP，三个凭据正确 | 成功；客户端正确请求 `password3` |
| 三因子第二因子错误 | 1045 拒绝 |
| 三因子第三因子错误或缺失 | 1045 拒绝，不退回两因子 |
| LDAP 服务账户密码错误或为空 | 1045 拒绝；恢复服务账户后重新登录成功 |
| LDAP 服务不可用 | 1045 拒绝，不跳过 LDAP 因子 |
| 客户端不发送 MFA capability | 1045 拒绝，不建立会话 |
| `REQUIRE SSL` 账号使用 VERIFY_IDENTITY | 成功完成全部因子 |
| `REQUIRE SSL` 账号关闭客户端 TLS | 1045 拒绝 |

## 6. MySQL 行为对照

使用 MySQL 8.4.9 验证与 MFA 相关的兼容边界：

| 行为 | MySQL 8.4.9 结果 | TiDB 结果 |
| --- | --- | --- |
| native 单因子 | 成功（显式启用 native） | 成功 |
| `CREATE USER` 使用 native＋native | 拒绝第二因子插件 | 拒绝 |
| native＋SHA-2 或 SHA-2＋native | 拒绝后续本地密码插件 | 拒绝 |
| `REQUIRE SSL`、TLS、无客户端证书 | 成功 | 成功 |
| `REQUIRE SSL`、关闭 TLS | 拒绝 | 拒绝 |

本机 MySQL 未安装 Enterprise LDAP 服务端插件，因此没有用 MySQL 完成 LDAP MFA 的正向登录；TiDB 的 LDAP 后续因子使用已有 `authentication_ldap_simple` 实现，并通过真实 OpenLDAP 完成了正向和失败场景。

## 7. 日志脱敏

执行以下配置操作并检查 TiDB 常规 info 日志：

```sql
SET GLOBAL authentication_ldap_simple_bind_root_pwd = '<随机服务账户密码>';
```

结果：日志只出现 `val=******`，未出现随机明文密码。LDAP 登录在脱敏后仍成功，说明该修复只作用于日志副本，不改变配置值或认证行为。

## 8. 已覆盖的兼容边界

- 默认策略下既有单因子账号保持原认证路径；
- 第一因子密码修改保留后续因子；
- 因子配置保存在既有 `User_attributes` JSON，系统表结构不变；
- MFA 认证失败不会降级为单因子；
- 因子编号、`SHOW CREATE USER` 和删除后的顺序一致；
- `REQUIRE SSL` 与 MFA 独立；
- native、caching SHA-2、LDAP Simple 的已支持组合和错误边界；
- MFA 协议 capability、`AuthNextFactor` 包格式和客户端能力检查；
- LDAP 服务账户密码日志脱敏。

## 9. 未覆盖范围

以下场景未在本次直连验收中完成，不纳入已通过结论：

- `COM_CHANGE_USER` 的真实 MySQL CLI 调用；协议和服务端回归测试已覆盖对应实现入口；
- 多节点传播和跨节点一致性；
- 代理终止 TLS、TiProxy 会话迁移和其他代理认证语义；
- 其他驱动和客户端实现；
- 并发、压力和连接池耗尽测试；
- LDAP StartTLS/LDAPS 与 MFA 同时启用的组合；
- WebAuthn、TOTP、LDAP SASL 等未支持因子。

这些边界需要相应集成环境、客户端或外部服务具备后再验收；它们不改变本报告已覆盖的直连 MFA 结论。

## 10. 相关文档

- [MFA 用户文档](2026-11-18-multi-factor-authentication-user-guide.md)
- [MFA 技术设计](2026-11-18-native-mfa-spec.md)
