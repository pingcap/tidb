# TiDB 原生多因素认证技术设计

## 1. 目标和边界

本设计在 `release-7.1.9-0` 的账号、权限、协议和 LDAP Simple 实现上增加 MySQL 兼容的有序多因素认证。目标是让 TiDB 能够创建、修改、持久化和验证最多三个认证因子，同时保持没有 MFA 的账号沿原路径工作。

支持范围：

- 第一因子为 `mysql_native_password` 或 `caching_sha2_password`。
- 后续因子为 `authentication_ldap_simple`，最多两个。
- MySQL MFA 账号语法、`authentication_policy`、协议 capability 和 `AuthNextFactor` 交换。
- 直连客户端认证；现有 LDAP Simple、TLS、密码策略、锁定和账号权限能力复用。

不在范围：新增 LDAP 实现、WebAuthn/TOTP/硬件密钥、LDAP SASL MFA、代理终止后的因子转发和会话迁移的 MFA 适配。

## 2. 不变量和兼容合同

1. MFA 因子按账号定义的顺序逐个验证，全部成功后才提交认证身份。
2. 因子失败、客户端能力不足、LDAP 故障或 MFA 配置损坏均拒绝登录，不能降级为第一因子。
3. 默认 `authentication_policy='*,,'` 时，旧单因子账号的认证、账号管理和默认插件行为保持不变。
4. 系统表结构不变。MFA 使用现有 `mysql.user.User_attributes` JSON，不新增列、不改列类型。
5. `SET PASSWORD` 只更新第一因子密码并保留后续因子；`ALTER USER` 的其他既有属性更新逻辑保持。
6. `REQUIRE SSL` 仍是独立的账号连接条件，不参与因子计数，也不要求客户端证书。
7. 使用 `mysql_native_password` 或 `caching_sha2_password` 作为第二、第三因子按 MySQL 规则拒绝；未支持插件组合不生成可绕过的账号配置。

## 3. MySQL 对齐合同

TiDB 沿用 MySQL 的用户可见语法和持久化名称：

```sql
CREATE USER user IDENTIFIED ...
  AND IDENTIFIED ...;
ALTER USER user ADD 2 FACTOR IDENTIFIED ...;
ALTER USER user MODIFY 2 FACTOR IDENTIFIED ...;
ALTER USER user DROP 2 FACTOR;
```

后续因子保存在 `User_attributes` 的 MySQL MFA 成员中，结构为：

```json
{
  "multi_factor_authentication": [
    {
      "plugin": "authentication_ldap_simple",
      "authentication_string": "uid=mfa_user,ou=People,dc=example,dc=com",
      "requires_registration": 0,
      "passwordless": 0
    }
  ]
}
```

数组下标对应因子 2、3。TiDB 不修改 `mysql.user` 表结构，也不另建 TiDB 专用 MFA 表。因子操作按明确编号定位；逆序的多个 `MODIFY` 或 `DROP` 不依赖 MySQL 运行时重载顺序，序列化后仍保持编号 2、3 的稳定含义。该稳定行为与 MySQL 8.4.9 在特定逆序 MODIFY/重载组合下的差异属于已知兼容边界。

## 4. 账号管理执行流

### CREATE USER

Parser 将第一因子放入既有 `AuthOption`，将后续因子放入 `UserSpec.AuthFactors`。执行器按当前策略和首因子插件校验：

1. 读取 `authentication_policy`。
2. 确定第一因子的有效插件；MFA 账号必须使用 native 或 caching SHA-2。
3. 校验后续因子数量、插件和策略位置。
4. 为每个后续因子编码 MySQL MFA JSON。
5. 与既有账号字段在同一内部事务中写入 `mysql.user`。
6. 提交后通知权限缓存刷新。

没有后续因子时，CREATE 继续使用原有账号写入路径和 JSON 表示。

### ALTER USER

Parser 产生 `AuthFactorOp` 列表，执行器对目标账号行加锁并读取现有 MFA JSON。因子操作只允许位置 2、3，并且同一条语句的多个操作使用相同动作：

- `ADD` 只追加到下一个连续位置；
- `MODIFY` 按明确位置替换已有因子；
- `DROP` 删除指定位置，后续数组元素前移；
- 所有操作完成后重新校验最终因子链和策略。

MFA JSON 更新与密码锁定、metadata、resource group 等既有 `User_attributes` 更新合并为一个 JSON 表达式，避免后写覆盖先写。事务失败回滚，提交后按现有机制通知权限缓存。

### SET PASSWORD

`SET PASSWORD` 读取并校验目标账号的 MFA 配置，更新第一因子密码，保留 `User_attributes.multi_factor_authentication`。默认无 MFA 路径继续使用既有轻量流程；配置了 MFA 或启用非默认 policy 时读取相应账号数据以执行约束。

## 5. authentication_policy

`authentication_policy` 由系统变量校验器解析为一至三个位置。每个位置是 `*`、具体插件或空值；空位置只能出现在尾部。TiDB 实现 MySQL 8.0.27 与 8.4 共有的数量和指定插件约束，默认值为 `*,,'`。

账号执行器在 CREATE、ALTER 的因子操作和适用的 SET PASSWORD 路径调用同一策略校验。策略变化不会遍历并改造旧账号；默认策略下旧账号不增加新的读取、验证或回调。`AUTHENTICATION_POLICY_ADMIN` 只有在已具备账号管理权限且动态权限检查明确通过时，才可将策略不匹配降级为警告。旧 GRANT 隐式建用户沿原路径处理。

MySQL 8.4 的 `*:plugin` 默认插件和自动补齐语义未纳入实现，解析时明确拒绝。

## 6. 持久化和权限缓存

`pkg/privilege/privileges/mfa.go` 解码 `User_attributes.multi_factor_authentication`，验证数组、因子数量、插件名和 `requires_registration/passwordless` 标志。当前支持的后续插件只有 `authentication_ldap_simple`。

权限缓存加载 `mysql.user` 时：

- 没有 MFA 成员表示普通账号；
- 合法数组保存为账号的有序因子列表；
- JSON 损坏、数量超限或插件不支持时保留账号但标记 `MFAConfigurationInvalid`。

认证遇到无效标记时立即拒绝，不能因解析失败而采用第一因子放行。账号精确 Host 匹配和现有缓存优先级保持不变。系统表写入、BR 恢复和缓存刷新沿现有事务与传播机制工作；MFA 不新增独立迁移状态。

## 7. 首次认证流程

首次连接的调用链保持第一因子验证入口不变，MFA 只在账号快照确认存在后追加后续步骤：

1. `ConnectionVerification` 验证 native 或 caching SHA-2 第一因子。
2. 账号快照标记 MFA 作用域；首因子失败仍沿原错误路径返回。
3. 服务端检查客户端的 `mysql.MultiFactorAuthentication` capability。
4. 对每个后续因子调用 `NextFactorAuthConn.RequestNextAuthFactor`。
5. `clientConn` 发送 MySQL `AuthNextFactor`（`0x02`）包；LDAP Simple 映射为 `mysql_clear_password` 插件，读取客户端响应。
6. `ConnectionVerification` 将响应交给现有 LDAP Simple 实现，复用连接池、搜索、Bind、超时和归还逻辑。
7. 所有因子成功后才设置已认证用户并建立 SQL 会话。

服务端在默认 capability 中声明 MFA 位。客户端缺少该位、后续插件请求失败或 LDAP Bind 失败时，认证失败且不返回成功包。首因子和后续因子使用同一个账号配置快照，避免一次认证中途读取不同版本的账号状态。

## 8. CHANGE_USER、失败计数和会话边界

`COM_CHANGE_USER` 先按目标账号重新确定 MFA 范围，再执行第一因子和后续因子。MFA 目标的认证失败返回 MySQL 规定的 CHANGE_USER 失败结果并断开连接；无 MFA 账号和首次握手沿原错误与会话恢复路径处理。MFA 的 native/SHA-2 CHANGE_USER 包含空 `pluginName` 情形，使用与首次认证相同的 MFA 预查和 SHA-2 交换。

MFA 失败使用现有密码锁定统计中的独立 `CountFailedMFA` 标记：

- 第一因子失败仍计入原密码失败路径；
- 后续因子失败、LDAP 故障或客户端 MFA 能力不足计为一次 MFA 失败；
- 只有全部因子和账号条件成功时才清除对应失败状态；
- 无 MFA 账号不增加 MFA 计数。

账号配置变更只影响新认证；已有 SQL 会话不自动断开。迁移、代理和其他未适配接入路径不能以单因子凭据绕过 MFA，当前不把这些路径列入支持承诺。

## 9. 错误和安全边界

- 非法因子编号、重复操作、因子缺失、超过三个因子和策略不匹配在账号管理阶段拒绝。
- 不支持的后续插件在账号管理或认证阶段拒绝。
- 损坏的 MFA JSON 在缓存和认证阶段 fail closed；需要解析账号配置的 SHOW/管理操作返回配置错误。
- 认证失败沿现有 1045/CHANGE_USER 错误语义返回，MFA 不暴露 LDAP 认证串或密码。
- LDAP 服务账户密码的日志输出使用固定脱敏文本；安全 SQL 文本和普通变量日志的既有脱敏规则保持。

## 10. 模块职责和源码入口

| 模块 | 状态 | 职责 |
| --- | --- | --- |
| `pkg/parser/parser.y`、`pkg/parser/ast/misc.go` | 修改 | 解析 `AND IDENTIFIED` 和 `ADD/MODIFY/DROP N FACTOR`，生成 `AuthFactorOp`；不改变旧账号语法树含义 |
| `pkg/executor/simple.go` | 修改 | 账号因子校验、policy 校验、JSON 更新、事务提交和 `SET PASSWORD` 保留因子；不实现 LDAP 网络认证 |
| `pkg/privilege/privileges/mfa.go` | 新增 | 解码和校验 MySQL MFA JSON；不访问 SQL、LDAP 或权限缓存 |
| `pkg/privilege/privileges/cache.go` | 修改 | 将账号 JSON 解码为有序因子或无效配置标记；保持 Host 匹配和缓存刷新机制 |
| `pkg/privilege/privileges/privileges.go` | 修改 | 先验证第一因子，再按顺序调用 `NextFactorAuthConn` 和已有 LDAP Simple；维护 MFA 失败分类 |
| `pkg/privilege/conn/conn.go`、`pkg/server/conn.go` | 新增/修改 | 定义可选的后续因子传输接口并实现 `AuthNextFactor` 包交换；无 MFA 路径不调用该接口 |
| `pkg/server/server.go` | 修改 | 在默认握手 capability 中声明 `MultiFactorAuthentication` |
| `pkg/session/session.go` | 修改 | 将 MFA 失败标记接入既有自动锁定和认证错误处理 |
| `pkg/sessionctx/variable/sysvar.go`、`pkg/privilege/privileges/privileges.go` | 修改 | 解析 `authentication_policy`，注册 `AUTHENTICATION_POLICY_ADMIN` 并执行显式动态权限检查 |

实现不新增通用认证注册中心、跨请求 MFA 状态机、专用系统表或首因子适配层。新增 helper 只承担 JSON、策略和后续因子传输的真实边界。

## 11. 测试和验收

测试覆盖 parser/AST、JSON 编解码、权限缓存、账号生命周期、policy、协议包、认证失败计数、CHANGE_USER 和直连端到端场景。MySQL CLI 8.4.9 与 OpenLDAP 2.6.8 的两因子、三因子、错误/缺失凭据、密码修改、因子管理、`REQUIRE SSL`、策略约束和日志脱敏结果记录在[原生 MFA 端到端测试验收](2026-11-19-multi-factor-authentication-e2e-validation.md)。

不影响既有行为的回归门槛包括：无 MFA 账号登录、原有单因子插件、密码策略、TLS、账号权限、旧 JSON/metadata、普通 `CHANGE_USER`、BR 账号数据和账号 Host 匹配。未覆盖的代理、会话迁移、多节点传播、其他驱动、并发压力和 LDAP TLS 与 MFA 组合需要在相应集成环境继续验证。
