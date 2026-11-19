# 现有身份验证、证书双因素与 IP 限制验证

日期：2026-11-18。结论：基础登录与 IPv4 来源限制已经具备；当前基线的账号维护行为不能完整满足[历史证书候选方案](2026-11-18-multi-factor-authentication-certificate-candidate.md)定义的保障。账号维护问题已复现，但本报告不证明客户接受该候选方案，也不证明已实现 MySQL 原生 MFA。Q10 已选择按原生 MFA 重新梳理；本报告只保留前期验证证据。

按用户 Q8 的要求，本次先检查现有功能，再判断开发必要性。验证未切换分支，未修改产品代码，未操作现有数据库。

## 基线与方法

| 项目 | 实际使用值 |
| --- | --- |
| TiDB 分支 | `release-7.1.9-0` |
| TiDB 提交 | `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26` |
| 构建 | 当前源码，`go1.27.3`，`-mod=readonly -tags codes` |
| 存储与部署 | 临时目录中的独立单节点 `unistore`；SQL 仅监听 `127.0.0.1` 随机端口 |
| 客户端 | Homebrew MySQL CLI `8.4.9`，TLS `VERIFY_IDENTITY` |
| 账号 | 每个场景新建账号；普通账号只有 `USAGE`，管理员使用隔离实例的 root |
| 密码策略 | 隔离实例 `validate_password.enable=ON`、`validate_password.length=8` |
| 证书 | 临时 CA、服务端证书、匹配身份的客户端证书、其他主体证书及不受信任证书 |
| 重复性 | 两次独立启动实例、重新生成凭据；两轮均为 27 项断言，20 项通过、7 项失败，失败项完全一致 |

构建时仅写入 Git 提交、分支和 Edition 元数据；版本输出中的 `v8.4.0-this-is-a-placeholder` 和 Core Version 来自当前源码，报告以 Git 提交识别基线。仓库原有 `bin/tidb-server` 对应更旧提交，没有将它的行为作为本次结论依据。

执行命令：

```sh
go build -mod=readonly -p 8 -tags codes \
  -ldflags '-X github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitHash=5c980fb645c8f3ae76f006967a4bc8b28ce8ba26 -X github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitBranch=release-7.1.9-0 -X github.com/pingcap/tidb/pkg/util/versioninfo.TiDBEdition=Enterprise' \
  -o /tmp/tidb-mfa-check.q9HXlG/tidb-server ./cmd/tidb-server

python3 /tmp/tidb-mfa-check.q9HXlG/validate.py \
  /tmp/tidb-mfa-check.q9HXlG/tidb-server \
  --output /tmp/tidb-mfa-check.q9HXlG/results.json
```

验证脚本退出码为 1，表示发现与验收要求不符的断言；两轮均完成全部场景，没有 setup/harness error。输出末行为 `{"passed": 20, "failed": 7}`。初次构建的依赖下载失败已通过重试解决；脚本启动参数错误已在正式两轮测试前纠正。

本机诊断证据：[脚本](/tmp/tidb-mfa-check.q9HXlG/validate.py)、[第一轮结果](/tmp/tidb-mfa-check.q9HXlG/results.json)、[第二轮结果](/tmp/tidb-mfa-check.q9HXlG/results-repeat.json)。这些是临时诊断文件；复现时可重新构建二进制并指定新的输出路径。每轮完成后脚本终止实例，删除数据、私钥和客户端凭据文件，报告保留脱敏结果。

## 已有且通过的能力

| 能力 | 实测结果 |
| --- | --- |
| 非空密码＋匹配 issuer/subject 的证书 | 登录成功 |
| 正确密码但缺少证书 | 拒绝，1045 |
| 正确证书但密码错误 | 拒绝，1045 |
| 客户端证书主体不匹配 | 拒绝，1045 |
| 客户端证书不受信任 | 连接被拒绝，CLI 报 2013；不依赖特定 TLS 错误文案验收 |
| 配置双因素账号使用明文连接 | 拒绝，1045 |
| 普通密码账号、未设置证书要求 | TLS 无客户端证书和原有明文连接均成功 |
| 精确 IPv4 地址限制 | 包含当前来源的账号成功，不包含当前来源的账号失败 |
| IPv4 点分掩码网段限制 | 包含当前来源的账号成功，不包含当前来源的账号失败 |
| 锁定账号 | 后续登录失败，3118 |
| 自身 `SET PASSWORD` | 合法新密码成功，证书要求保持，缺少证书仍拒绝 |
| 自身 `SET PASSWORD = ''` | 全局策略拒绝，1819 |
| 普通用户修改其他账号 | 拒绝，1227 |
| 管理员导入空认证串 | 保留已有允许行为；管理员负责维护目标双因素账号的非空密码前提 |

IP 用例从真实 TCP 来源 `127.0.0.1` 连接，分别使用允许该地址的 `127.0.0.1`、`127.0.0.0/255.255.255.0` 和不允许该地址的 `192.0.2.42`、`192.0.2.0/255.255.255.0` 账号定义；各用户名独立，无同名宽范围账号。这验证了账号 Host 的地址及网段匹配，没有模拟跨主机网络或 NAT。

## 复现的缺口

以下 SQL 中密码为占位符。实际运行由脚本生成符合策略的随机密码，使用私有客户端配置文件传入；普通用户通过正确密码与证书连接后执行自身修改。

### 1. 未写 REQUIRE 的 ALTER USER 清除证书约束

管理员先创建具有密码和证书要求的账号，再只修改密码：

```sql
CREATE USER 'alter_password'@'127.0.0.1'
  IDENTIFIED BY '<旧密码>'
  REQUIRE ISSUER '/CN=MFA Validation CA'
  AND SUBJECT '/CN=mfa-validation-user';

ALTER USER 'alter_password'@'127.0.0.1' IDENTIFIED BY '<新密码>';

SELECT Priv FROM mysql.global_priv
WHERE User='alter_password' AND Host='127.0.0.1';
```

实际结果：`Priv` 从包含 `ssl_type=3`、issuer、subject 的 JSON 变为 `{}`；新密码不带证书可以登录。旧密码仍然失败，说明密码修改已生效，而证书约束被丢弃。另一个独立账号执行 `ACCOUNT LOCK` 再 `ACCOUNT UNLOCK` 后，不带证书也可以登录。

这对应 3 项失败断言。源码原因是 [executeAlterUser](/Users/cs/code/tidb/pkg/executor/simple.go:1794) 即使没有 REQUIRE 也调用转换函数；[tlsOption2GlobalPriv](/Users/cs/code/tidb/pkg/executor/grant.go:522) 对空选项返回 `{}`，后续[写入全局认证属性](/Users/cs/code/tidb/pkg/executor/simple.go:2112)覆盖原值。MySQL [只在显式指定 SSL 属性时更新](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1308)。

### 2. 普通用户可以取消自己的证书要求

仅有 `USAGE` 的双因素账号在认证成功后执行：

```sql
ALTER USER 'self_tls'@'127.0.0.1' REQUIRE NONE;
```

实际操作成功，后续仅用密码即可登录。预期应为权限不足、配置保持不变。这对应 2 项失败断言；普通用户修改别人的账号会被拒绝，证明是自身修改的权限豁免过宽。

### 3. 普通用户可以绕过全局策略清空密码

为单独观察密码因素，使用仅有 `USAGE` 的普通密码账号执行：

```sql
ALTER USER 'self_hash'@'127.0.0.1'
  IDENTIFIED WITH mysql_native_password AS '';
```

实际操作成功，随后不提交密码即可登录，即使全局密码策略已启用且最小长度为 8。这对应 2 项失败断言。这里实测的是普通密码账号的自助权限；证书账号走相同的自身修改权限分支，不能依赖全局密码策略阻止这种认证串导入。

第 2、3 项共享[自身账号无条件绕过管理权限检查](/Users/cs/code/tidb/pkg/executor/simple.go:1838)这一原因。MySQL [将普通自身修改豁免收敛为纯明文改密](/Users/cs/code/mysql-server/sql/sql_parse.cc:4781)，不包括更换插件、导入认证串或修改 REQUIRE。TiDB 对管理员导入认证串的既有能力应保留。

## 开发必要性与验证边界

若继续采用证书候选方案，需要两项聚焦修复：省略 REQUIRE 时保留账号证书约束；普通用户的自身修改豁免仅覆盖合法改密，其他管理操作执行相应权限检查。现有密码和 TLS 校验可作为该方案基础。只要求运维避免 ALTER USER 无法解决普通用户主动移除要求的问题。若交付 MySQL 原生 MFA，则需另外设计插件链，不能用这两项修复替代。

本次结果足以判断当前版本未完整满足要求，也足以确认无需重新实现证书登录或 IPv4 来源控制。它不是完整发布验收：尚未实测全部内置密码插件、证书过期/尚未生效、缺失私钥、证书轮换、COM_CHANGE_USER、密码过期沙箱、多节点缓存传播和 TLS reload。后续[技术 spec](2026-11-18-multi-factor-authentication-spec.md)将列明需要执行的回归和集成验证；MySQL 行为对照来自源码及其现有测试，未运行 MySQL 服务端对照实例。
