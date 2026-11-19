# TiDB 原生多因素认证用户文档

本文说明 TiDB 原生多因素认证（MFA）的用户行为、配置方法和运维边界。MFA 使用 MySQL 的有序认证插件语法和协议：账号先验证第一因子，再按顺序验证后续因子；全部因子成功后才建立 SQL 会话。

## 支持范围

- 一个账号最多配置三个有序因子。
- 第一因子支持 `mysql_native_password` 和 `caching_sha2_password`。
- 当前后续因子支持 `authentication_ldap_simple`。第二、第三因子可以使用同一插件，但应绑定各自的 LDAP 身份和密码。
- 使用 MySQL 的 `CREATE USER ... AND IDENTIFIED ...`、`ALTER USER ... ADD/MODIFY/DROP N FACTOR` 和 `SHOW CREATE USER` 管理因子。
- 使用 MySQL 兼容的 `authentication_policy` 约束因子数量和插件位置。
- 复用 TiDB 已有的 LDAP Simple、密码策略、TLS 和账号权限能力。
- 已验证的直连接入方式是 MySQL 命令行客户端 8.4.9 连接 TiDB；其他驱动、代理和会话迁移需要单独验收。

MFA 因子是认证插件。数据库密码和 LDAP 密码都属于密码凭据；配置两个插件不等于已经具备密码、持有物或生物特征等不同类别的安全因素。

已有单因子账号不会因为升级或保持默认策略而自动增加因子。没有 MFA 的账号继续沿原认证路径工作。

当前不支持 WebAuthn、硬件密钥、TOTP、LDAP SASL 作为 MFA 因子，也不支持使用 `mysql_native_password` 或 `caching_sha2_password` 作为第二、第三因子。未支持的组合会在账号管理或登录时失败，不会降级为单因子登录。

## 前置条件

启用 MFA 前准备以下内容：

1. 所有可能接收目标账号登录或账号管理语句的 TiDB 节点运行支持 MFA 的版本。混合旧版本期间不能保证旧节点识别并强制执行后续因子。
2. 已部署并可用的 LDAP Simple 服务。LDAP 用户密码由目录系统维护，TiDB 账号只保存 LDAP 身份字符串，不保存 LDAP 用户密码。
3. TiDB 到 LDAP 的服务账户、CA 和连接参数。LDAP 的全局变量由集群统一管理，所有使用 LDAP Simple 的账号和因子共用这些设置。
4. 启用 MFA 前可用的管理员账号和恢复通道。账号因子变更对后续认证生效，不会自动断开已有 SQL 会话。

推荐同时保护两段连接：客户端到 TiDB 使用 TiDB TLS，TiDB 到 LDAP 使用 LDAP TLS。两段 TLS 是传输保护，不计入 MFA 因子，也不改变 MFA 语法。

## 配置 LDAP Simple

下面的设置示例使用 StartTLS。变量名和取值沿用 TiDB 已有的 LDAP Simple 能力：

```sql
SET GLOBAL authentication_ldap_simple_tls = ON;
SET GLOBAL authentication_ldap_simple_ca_path = '/etc/tidb/certs/ldap-ca.pem';
SET GLOBAL authentication_ldap_simple_server_host = 'ldap.example.com';
SET GLOBAL authentication_ldap_simple_server_port = 389;
SET GLOBAL authentication_ldap_simple_bind_root_dn = 'cn=tidb-bind,ou=service,dc=example,dc=com';
SET GLOBAL authentication_ldap_simple_bind_root_pwd = '<服务账户密码>';
```

主机名不包含 `ldap://` 前缀，并应与 LDAP 服务端证书匹配。显式用户 DN 可以跳过用户搜索，但 LDAP 连接池仍需要有效的服务账户预绑定。更换全局 LDAP 设置会影响已有 LDAP Simple 账号；设置成功后应在每个 TiDB 节点使用真实账号完成登录验证。

服务账户密码属于敏感配置。TiDB 的常规 info 日志只记录脱敏后的值（`******`），不记录明文密码。日志脱敏只改变日志输出，不改变 LDAP 认证和配置语义。

## 创建 MFA 账号

由具备账号管理权限的管理员创建第一因子和 LDAP 第二因子：

```sql
CREATE USER 'mfa_user'@'<账号 Host>'
  IDENTIFIED WITH caching_sha2_password BY '<数据库密码>'
  AND IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user,ou=People,dc=example,dc=com';

SHOW CREATE USER 'mfa_user'@'<账号 Host>';
```

第一因子也可以使用 `mysql_native_password`。LDAP `AS` 值是 LDAP 用户的身份字符串，LDAP 密码在登录时由客户端提供并由 LDAP 服务验证。

`REQUIRE SSL` 是独立的账号连接条件，可以追加在账号定义末尾：

```sql
CREATE USER 'mfa_user'@'<账号 Host>'
  IDENTIFIED WITH caching_sha2_password BY '<数据库密码>'
  AND IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user,ou=People,dc=example,dc=com'
  REQUIRE SSL;
```

`REQUIRE SSL` 只要求客户端与 TiDB 使用加密连接，不要求客户端证书，也不计为 MFA 因子。TiDB 到 LDAP 的 TLS 仍由 LDAP 全局变量控制。

## 登录

MySQL 命令行客户端按因子顺序请求密码：

```sh
mysql --protocol=TCP --host=tidb.example.com --port=4000 \
  --user=mfa_user --password1 --password2 \
  --enable-cleartext-plugin \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/tidb-ca.pem
```

客户端应在提示符中输入第一因子数据库密码和第二因子 LDAP 密码，不要将密码写入命令行参数。LDAP Simple 使用 `mysql_clear_password` 客户端插件，因此需要显式启用 `--enable-cleartext-plugin`。客户端 TLS 用于在发送密码前验证 TiDB 身份；LDAP TLS 保护另一段连接。

第一因子、第二因子和所有后续因子都成功后，客户端才可以执行普通 SQL。任一密码错误、凭据缺失、LDAP 不可用、客户端不支持 MFA 协议，或账号配置损坏，登录都会被拒绝；TiDB 不会退回到较少因子的认证。

## 增加、修改和删除因子

管理员可以为已有单因子账号增加第二因子：

```sql
ALTER USER 'mfa_user'@'<账号 Host>'
  ADD 2 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user,ou=People,dc=example,dc=com';
```

修改已有因子的身份绑定：

```sql
ALTER USER 'mfa_user'@'<账号 Host>'
  MODIFY 2 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user_v2,ou=People,dc=example,dc=com';
```

增加第三因子时必须先存在第二因子：

```sql
ALTER USER 'mfa_user'@'<账号 Host>'
  ADD 3 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user_factor3,ou=People,dc=example,dc=com';

SHOW CREATE USER 'mfa_user'@'<账号 Host>';
```

三因子登录需要第三个密码：

```sh
mysql --protocol=TCP --host=tidb.example.com --port=4000 \
  --user=mfa_user --password1 --password2 --password3 \
  --enable-cleartext-plugin \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/tidb-ca.pem
```

删除因子：

```sql
ALTER USER 'mfa_user'@'<账号 Host>' DROP 3 FACTOR;
ALTER USER 'mfa_user'@'<账号 Host>' DROP 2 FACTOR;
```

删除第二因子后，原第三因子会前移为第二因子，客户端应将其密码作为 `password2` 提供。因子编号必须连续，不能留下空缺。因子数量、插件位置和策略要求不满足时，语句会失败并保持原账号配置。

## 修改密码和恢复访问

`SET PASSWORD` 和只修改第一因子密码的 `ALTER USER ... IDENTIFIED BY ...` 保留已有第二、第三因子：

```sql
SET PASSWORD = '<新的数据库密码>';
```

第一因子密码仍受 TiDB 当前密码策略和密码历史规则约束。LDAP 用户密码在目录系统中修改；目录密码变更后应使用新连接验证新密码，旧密码应被拒绝。

LDAP 服务不可用时，管理员应恢复目录服务或修复 LDAP 配置，不能跳过后续因子。遗忘密码时由相应系统的管理员重置密码；LDAP 身份永久变更时，数据库管理员使用 `MODIFY N FACTOR` 替换绑定。删除因子会降低账号的认证要求，只有具备相应权限并符合策略时才应执行。

账号配置和 LDAP 密码变更只影响后续认证。需要立即收回已有会话时，管理员应按现有连接管理流程终止连接。

## authentication_policy

`authentication_policy` 采用 MySQL 的逗号分隔策略。星号表示该位置必需但插件不限，空项表示可选，未出现的位置不允许使用：

| 策略 | 含义 |
| --- | --- |
| `'*,,'` | 默认策略，允许一至三个因子 |
| `'*'` | 只允许第一因子 |
| `'*,*'` | 必须配置两个因子 |
| `'*,*,'` | 至少两个因子，允许第三因子 |
| `'*,*,*'` | 必须配置三个因子 |
| `'*,authentication_ldap_simple'` | 第二因子必须使用 LDAP Simple |

查看或修改策略：

```sql
SHOW GLOBAL VARIABLES LIKE 'authentication_policy';
SET GLOBAL authentication_policy = '*,*,';
```

默认策略不会改造既有单因子账号。非默认策略会约束 CREATE USER、ALTER USER 的因子操作和适用的 SET PASSWORD；旧账号不会因为策略变化自动补齐因子。`AUTHENTICATION_POLICY_ADMIN` 需要显式授予，并且只允许越过策略约束，不授予 CREATE USER 或 ALTER USER 权限。修改全局变量仍需要相应的系统变量权限。

TiDB 首版不支持 MySQL 8.4 的 `*:plugin` 默认插件和自动补齐写法。使用该写法会被拒绝。现有密码策略、LDAP 目录密码策略和 MFA policy 分别生效。

## 部署、升级和回退

1. 在所有目标节点部署支持 MFA 的 TiDB，并准备 LDAP、TLS、管理员权限和恢复通道。
2. 先创建测试账号，验证正确和错误凭据、缺失凭据、LDAP 故障、账号管理以及每个节点的登录结果。
3. 逐步为目标账号增加因子，使用 `SHOW CREATE USER` 检查顺序和插件。
4. 回退到不支持 MFA 的版本前，停止目标账号的新访问并按需终止已有连接。旧版本不能强制执行额外因子；恢复服务前应重新升级并逐节点检查账号配置。

## 相关文档

- [原生 MFA 技术设计](2026-11-18-native-mfa-spec.md)
- [原生 MFA 端到端测试验收](2026-11-19-multi-factor-authentication-e2e-validation.md)
