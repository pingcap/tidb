# TiDB 当前认证能力简报

基线：`release-7.1.9-0`，提交 `5c980fb645c`；核查日期：2026-11-18。

## 支持范围

| 方式 | 当前能力与条件 |
| --- | --- |
| `mysql_native_password` | 默认密码认证；存储无盐双重 SHA-1 哈希，登录采用挑战响应，兼容旧客户端。 |
| `caching_sha2_password` | 加盐、默认 5000 轮 SHA-256 密码派生；非空密码每次完整认证，未实现快速缓存和 RSA 交换，TCP 部署应配置 TLS。 |
| `tidb_sm3_password` | SM3 密码认证，需要支持该插件的客户端。 |
| `authentication_ldap_simple` | LDAP Simple Bind，需要 LDAP 服务配置及安全传输。 |
| `authentication_ldap_sasl` | LDAP SASL，机制选项包括 SCRAM-SHA-1、SCRAM-SHA-256、GSSAPI。 |
| `auth_socket` | 通过 Unix socket 对端操作系统用户身份认证。 |
| `tidb_auth_token` | JWT 认证，需要配置 JWKS 和账号声明。 |
| TLS 客户端证书 | 支持 `REQUIRE X509 / ISSUER / SUBJECT`，可与密码联合校验。 |

另有会话迁移专用的 `tidb_session_token` 和自定义认证 Go extension 接口。

## 关键限制

- 每个账号配置一个认证插件，未实现 MySQL 多插件多因素认证（MFA）。
- `authentication_policy` 读写报 **1193**；`AND IDENTIFIED` 多因素 SQL 报 **1064**；单独指定 `authentication_fido` 建用户报 **1524**。
- `default_authentication_plugin` 控制初始握手，未指定插件的建用户路径仍选择 native，建账号时应显式指定插件。
- 切换 native 与 SHA-2 需要重新提供密码或目标插件的认证串。
- `mysql_clear_password` 用于客户端传输密码或 Token；TiDB 内置账号插件未包含 `sha256_password`、PAM、独立 Kerberos、Windows、FIDO、WebAuthn。

## 引入记录

| 能力 | PR / 作者 | 合入日期 | 首次发布 |
| --- | --- | --- | --- |
| SM3 | [#36193](https://github.com/pingcap/tidb/pull/36193) / CbcWestwolf | 2022-09-08 | v6.3.0-DMR，2022-09-30 |
| LDAP Simple、SASL | [#43582](https://github.com/pingcap/tidb/pull/43582) / YangKeao | 2023-05-10 | v7.1.0 LTS，2023-05-31 |

## 与 MySQL 8.4.9 的主要差异

MySQL 默认使用 SHA-2，支持快速认证缓存、RSA 交换和最多三因子的 MFA，native 默认禁用；LDAP、PAM、Windows、Kerberos、WebAuthn 服务端插件属于 Enterprise，旧 FIDO 插件已移除。

依据见[详细核对记录](2026-11-18-authentication-comparison.md)：已核对源码、PR 和发布记录，前期隔离实例的 10 项建账号及 SQL 支持检查通过。

后续[真实认证实测](2026-11-18-authentication-runtime-validation.md)补充验证了 native、SHA-2、auth_socket 和真实 OpenLDAP Simple 登录。TiDB 两轮分别 41、42 项断言符合预期，包括显式/搜索/多值 DN、改密、连接池复用、StartTLS、LDAPS、服务故障；其中同时确认了 SET GLOBAL 会将 LDAP 服务账户密码写入常规日志的已有缺陷。LDAP Simple 直接复用，MFA 框架尚未实现。LDAP SASL、SM3、JWT 外部认证全链路、其他驱动和性能仍未实测。
