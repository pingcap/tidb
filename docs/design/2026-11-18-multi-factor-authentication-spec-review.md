# 证书双因素账号维护技术 spec 审阅记录

日期：2026-11-18。对象：[技术 spec v0.1](2026-11-18-multi-factor-authentication-spec.md)。这是基于源码与基线实测的设计检查，修复代码尚未编写，不能视作实现 review 或测试通过记录。最新状态：用户重新质疑证书方案是否满足客户 MFA 需求，故本稿只保留证书方案内部的设计检查，不代表产品范围或客户验收已确认。

## 基线与证据

用户指定的 TiDB `release-7.1.9-0` / `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26` 是行为基线；MySQL `mysql-8.4.9` / `f9c88132a87a7d9e740e50bce2621999695bd3fe` 是自助权限和 TLS 属性更新的语义参照。当前 TiDB 的缺口不能作为正确语义继续保留，也不能把其他 MySQL 功能自动扩大为交付要求。

两次重新生成凭据、重新初始化存储的真实 CLI 测试得到一致结果：[验证报告](2026-11-18-multi-factor-authentication-validation.md)。目前没有产品差异供审查；工作区新增内容为词汇表和讨论文档，诊断脚本位于临时目录。

| 检查项 | 证据与结论 |
| --- | --- |
| 属性保持性 | [ALTER 转换调用](/Users/cs/code/tidb/pkg/executor/simple.go:1794)、[空选项编码](/Users/cs/code/tidb/pkg/executor/grant.go:522)、[覆盖写入](/Users/cs/code/tidb/pkg/executor/simple.go:2112)与实测 `{}` 一致 |
| 自身权限例外 | [自身分支](/Users/cs/code/tidb/pkg/executor/simple.go:1838)与实际 REQUIRE NONE、AS 空串成功一致 |
| MySQL 参照 | [纯自身改密分类](/Users/cs/code/mysql-server/sql/sql_parse.cc:4781)、[管理权限检查](/Users/cs/code/mysql-server/sql/sql_parse.cc:4841)、[显式 SSL 更新](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1308)、[整组属性替换](/Users/cs/code/mysql-server/sql/auth/acl_table_user.cc:931) |
| 原始输入保真 | [AuthOption](/Users/cs/code/tidb/pkg/parser/ast/misc.go:98)、[AlterUserStmt](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1884)字段足以判断安全相关操作；必须在填充默认插件之前分类 |
| 权限入口 | [RequestVerification](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:175)继续处理生效角色、SEM、三权分立及 extension；不能替换为静态用户表位判断 |
| 系统会话清理 | [ReleaseSysSession](/Users/cs/code/tidb/pkg/executor/internal/exec/executor.go:398)先 rollback，失败关闭会话，成功再归还池 |

## 执行流与数据闭合

已追踪 SQL → Parser → SimpleExec.Next → executeAlterUser → 内部系统表事务 → 提交 → NotifyUpdatePrivilege → 后续 ConnectionVerification/checkSSL。修复位于账号管理入口，不在握手中引入第二套认证状态。

TLS 修复仅控制 ALTER 的写入意图。CREATE 仍可初始化 `{}`，GRANT 的 [tlsOption 写入路径](/Users/cs/code/tidb/pkg/executor/grant.go:510)原本已有空选项跳过逻辑，两者均无需改变。TOKEN_ISSUER 与 TLS 共用 AST slice，因此用“选项非空则调用原 helper”并保留其 token-only 空返回，比把每种 TLS enum 再枚举一次更直接。

自身权限检查使用原始 AuthOpt；执行器会给空 `AuthPlugin` 补入账号当前插件，补入后再判定会误拒绝合法 BY 改密。密码强度继续由现有校验负责，权限分类不能把空明文值当成另一个授权类别。真实 parser 不会同时生成 ByAuthString 和 ByHashString，无需新增不可能状态处理。

现有 AST 不区分 `WITH '' BY ...` 与未指定有效插件名。这两者都会解析到当前插件并走明文策略，保留该行为不会开放 AS 空串或插件切换；为追求字面语法一致而增加 AST 标志不属于本次必要修改。

## 接口与模块审计

全部接口的唯一契约表位于 spec。唯一修改行为的接口是 `SimpleExec.executeAlterUser`，调用者为 `SimpleExec.Next`，返回原有 error。拟新增的判断为局部逻辑，不增加对外 API、DTO 或状态字段。

Parser 负责表达语句，执行器负责权限及持久化更新，privileges 模块负责已有权限与认证检查，Domain 负责既有缓存通知。执行器不承担 TLS 握手、不读取私钥、不自行广播集群状态；认证模块不推测账号管理语句的修改意图。依赖方向和既有职责保持。

`mysql.user` 和 `mysql.global_priv` 的结构、字段编码不变。省略 REQUIRE 时跳过整个 TLS JSON 写入，也保持原无记录状态和未知字段；显式 REQUIRE 沿用整组替换语义。不存在新增 schema 迁移或回滚转换。

## 权限与旧路径审计

| 路径 | 审阅结论与后续回归 |
| --- | --- |
| 普通自身 BY 改密 | 无管理权限仍允许，密码策略/历史/沙箱继续按原规则处理 |
| 自身附加属性或 WITH/AS | 改为要求现有管理权限；这是明确兼容变化，适用所有账号，已在 spec 提请确认 |
| 同名不同 Host | 继续以 AuthHostname 区分，不能获得自身豁免 |
| 有效管理权限 | 继续接受 CREATE USER 或现有 mysql.user UPDATE 检查结果，包括生效角色；不能放宽 SEM/三权分立限制 |
| 修改他人的特殊账号 | 保持 SYSTEM_USER/RESTRICTED_USER_ADMIN 既有检查和动态权限规则 |
| MAX_USER_CONNECTIONS | 保留其单独要求 CREATE USER 的既有限制 |
| 管理员 AS 导入 | 保持，包括现有密码测试中 root 的 AS 空串用例；管理员负责双因素启用前提 |
| 单因子登录与 SET PASSWORD | 产品代码不修改这些路径，仍需跑实质回归确认 |
| LDAP/token/extension | 不改变认证实现；只有采用 ALTER 自身非纯改密的管理语句受新权限边界影响 |

已查看的测试包括 [TestAlterUserStmt](/Users/cs/code/tidb/pkg/privilege/privileges/privileges_test.go:257)、[TestUser](/Users/cs/code/tidb/pkg/executor/test/simpletest/simple_test.go:307)、[TestValidatePassword](/Users/cs/code/tidb/pkg/executor/test/passwordtest/password_management_test.go:39)及 [TestTLSVerify](/Users/cs/code/tidb/pkg/server/tests/tls/tls_test.go:183)。这些测试是开发时补充与回归的落点，本轮未执行其 Go 测试包。

## 错误与生命周期审计

内部系统会话在循环前建立 READ-COMMITTED 悲观事务，执行器 defer 释放会话。因此多目标中途拒绝会回滚此前未提交数据库写入，不需要新增逐目标补偿。`IF EXISTS` 的不存在账号 note 与实际写入错误、权限错误应分别保留。测试要同时检查持久化凭据、TLS JSON 和实际认证结果。

调用者旧事务在 ALTER 开始前已隐式提交，不能纳入本次回滚承诺。既有 unlock extension 事件可能在 commit 前发生，不能把数据库原子性扩展为外部事件事务性。上述边界均明确写入 spec，避免新增修复扩大成事务框架改造。

commit 前失败不通知缓存；commit 后通知错误表示数据可能已提交，不能假定账号恢复原状态。实现不新增隐式重试；运维按原机制核对配置和逐节点验证。已有连接不重新认证，多节点缓存不是瞬时一致，旧节点也仍能执行存在缺口的管理操作。

临时实例测试不能代替共享存储多节点测试。spec 明确后者使用同一存储的两个节点，并将 TLS reload 作为单实例操作验证。

## 复杂度、决定与剩余验证

共享 helper 改语义会波及 CREATE/GRANT；逐字段合并 TLS 会偏离 MySQL 的显式替换规则；新增 MFA 表、策略开关或握手阶段无法直接解决当前账号管理缺口。这些方案均没有本次需要承担的约束，采用执行器两处局部判断。

源码事实、实测结果和拟实现行为已分开标记。拟实现的兼容变化有明确输入、权限判定、错误结果及测试落点，接口没有悬空调用者。术语与用户文档一致，IP 限制独立复用。

在证书方案内部，技术判定已有实现依据；目前需要先重新确认产品方案能否满足客户需求，技术 spec 暂不进入确认或开发。各插件实际客户端、COM_CHANGE_USER、证书生命周期及多节点等运行证据仍未完成。最终选择不同 MFA 方案时，应重新设计和审阅，不能沿用本稿充当原生 MFA 设计结论。
