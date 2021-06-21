# Proposal:

- Author(s):     [morgo](https://github.com/morgo)
- Last updated:  June 16, 2021
- Discussion at: N/A

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document was created to discuss the design of Dynamic Privileges. It is intended to be implemented in combination with Security Enhanced Mode, but there no interdependencies between the two features.

## Motivation or Background

MySQL 8.0 introduced the concept of “dynamic privileges” (see [WL#8131](https://dev.mysql.com/worklog/task/?id=8131)). The intention behind this functionality is that plugins can create new named privileges to suit their purposes, such as “Firewall Admin” or “Audit Admin” instead of requiring the `SUPER` privilege, which becomes overloaded and too coarse.

Dynamic privileges are **not to be confused with** SQL Roles (RBAC). They work together just fine. Consider the following testcase which demonstrates the features working together:

```sql
  mustExec(c, rootSe, "CREATE USER notsuper")
	mustExec(c, rootSe, "CREATE USER otheruser")
	mustExec(c, rootSe, "CREATE ROLE anyrolename")
	mustExec(c, rootSe, "SET tidb_enable_dynamic_privileges=1")

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "notsuper", Hostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "SET tidb_enable_dynamic_privileges=1")

	// test SYSTEM_VARIABLES_ADMIN
	_, err := se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_admin ON *.* TO notsuper")
	mustExec(c, se, "SET GLOBAL wait_timeout = 86400")

	// test ROLE_ADMIN
	_, err = se.ExecuteInternal(context.Background(), "GRANT anyrolename TO otheruser")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or ROLE_ADMIN privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT ROLE_ADMIN ON *.* TO notsuper")
	mustExec(c, se, "GRANT anyrolename TO otheruser")

	// revoke SYSTEM_VARIABLES_ADMIN, confirm it is dropped
	mustExec(c, rootSe, "REVOKE SYSTEM_VARIABLES_AdmIn ON *.* FROM notsuper")
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86000")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")

	// grant super, confirm that it is also a substitute for SYSTEM_VARIABLES_ADMIN
	mustExec(c, rootSe, "GRANT SUPER ON *.* TO notsuper")
	mustExec(c, se, "SET GLOBAL wait_timeout = 86400")

	// revoke SUPER, assign SYSTEM_VARIABLES_ADMIN to anyrolename.
	// confirm that a dynamic privilege can be inherited from a role.
	mustExec(c, rootSe, "REVOKE SUPER ON *.* FROM notsuper")
	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_AdmIn ON *.* TO anyrolename")
	mustExec(c, rootSe, "GRANT anyrolename TO notsuper")

	// It's not a default role, this should initially fail:
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(c, se, "SET ROLE anyrolename")
	mustExec(c, se, "SET GLOBAL wait_timeout = 87000")
```

Dynamic privileges are different from static privileges in the following ways:

* The name of the privilege is “DYNAMIC”. A plugin registers it, and the server has no prior knowledge of its existence.
* Privileges can only be global scoped.
* Privileges are stored in the table `mysql.global_grants` and not `mysql.user`.
* The `GRANT OPTION` is stored for each dynamic privilege (versus for the user as a whole).

We have the same requirement for fine grained privileges in TiDB, so it makes sense to adopt a similar implementation of dynamic privileges. This document describes both the implementation of the framework for dynamic privileges and an initial set of dynamic privileges that are required to be implemented.

## Detailed Design

Implementing Dynamic Privileges requires the following work to be completed.

### Persistence

For TiDB, we can use the same table structure as MySQL:

```sql
CREATE TABLE `global_grants` (
  `USER` char(32) NOT NULL DEFAULT '',
  `HOST` char(255) NOT NULL DEFAULT '',
  `PRIV` char(32) NOT NULL DEFAULT '',
  `WITH_GRANT_OPTION` enum('N','Y') NOT NULL DEFAULT 'N',
  PRIMARY KEY (`USER`,`HOST`,`PRIV`)
);
```

There is an existing table called “global_priv” which initially looked like it provided similar functionality, except:
* The priv is expected to be a JSON encoded string
* There is no column named `WITH_GRANT_OPTION`.

I looked at repurposing this table (which stores TLS options), but because the `PRIV` value is the data and not the key, it gets messy. I instead plan to use the same schema as MySQL.

This table will persist dynamic privileges. Similar to MySQL, the cache is read into memory and cached (privilege/privileges/cache.go). Dynamic privileges will be cached in the same way as existing privileges.

### Privilege Checking API

Checking for existence of a Dynamic privilege needs a different function from normal privilege checks. I.e.

```
// RequestVerification(activeRole []*auth.RoleIdentity, db, table,
// column string, priv mysql.PrivilegeType) bool
if pm.RequestVerification(activeRoles, "", "", "", mysql.ProcessPriv) {
	// has processPrivilege
}
```

This is not suitable because:
* The privilege `mysql.ProcessPriv` must be predefined (i.e. it's not dynamic).
* The 3 empty string values (schema, table, column) are never applicable to dynamic privileges.
* Dynamic privileges are grantable individually. There may be scenarios where code wants to check if a user has both a dynamic privilege and the ability to grant it (such as in the output of `SHOW GRANTS`).

I propose the following:

```
// RequestDynamicVerification(activeRole []*auth.RoleIdentity, priv string, grantable bool) bool
if pm.RequestDynamicVerification(activeRoles, "BACKUP_ADMIN", false) {
	// has backup admin privilege
}
```

### Plugin API

There will need to be a way for plugins to register new dynamic privileges via their OnInit method. I propose the following:

```
import (
	"github.com/pingcap/tidb/privilege/privileges"
)

err = privileges.RegisterDynamicPrivilege("AUDIT_ADMIN")
if err != nil {
	return err
}
```

### Metadata Commands

#### SHOW GRANTS

The output of `SHOW GRANTS` needs to be modified to show each of the dynamic privileges applicable to a user, following static privileges. I.e.
```
mysql [localhost:8023] {root} (test) > show grants for 'u1';
+---------------------------------------------------------+
| Grants for u1@%                                         |
+---------------------------------------------------------+
| GRANT USAGE ON *.* TO `u1`@`%`                          |
| GRANT BINLOG_ADMIN ON *.* TO `u1`@`%`                   |
| GRANT BACKUP_ADMIN ON *.* TO `u1`@`%` WITH GRANT OPTION |
+---------------------------------------------------------+
3 rows in set (0.00 sec)
```
#### Information_schema

The table `user_privileges` should show a hybrid of both static and dynamic privileges:

```
mysql [localhost:8023] {root} (information_schema) > mysql [localhost:8023] {root} where grantee = "'u1'@'%'"
    -> ;
+----------+---------------+----------------+--------------+
| GRANTEE  | TABLE_CATALOG | PRIVILEGE_TYPE | IS_GRANTABLE |
+----------+---------------+----------------+--------------+
| 'u1'@'%' | def           | USAGE          | NO           |
| 'u1'@'%' | def           | BINLOG_ADMIN   | NO           |
| 'u1'@'%' | def           | BACKUP_ADMIN   | YES          |
+----------+---------------+----------------+--------------+
3 rows in set (0.00 sec)
```

#### SHOW CREATE USER

No change

#### CREATE USER

No change

#### GRANT / REVOKE

Needs to support the syntax of a privilege being either a static privilege, or a dynamic privilege. Dynamic privileges only support `*.*`

```
mysql [localhost:8023] {root} (information_schema) > grant select on *.* to 'u1';
Query OK, 0 rows affected (0.00 sec)

mysql [localhost:8023] {root} (information_schema) > grant binlog_admin on test.* to 'u1';
ERROR 3619 (HY000): Illegal privilege level specified for BINLOG_ADMIN
mysql [localhost:8023] {root} (information_schema) > grant binlog_admin on *.* to 'u1';
Query OK, 0 rows affected (0.01 sec)
````

`GRANT ALL` will also GRANT each of the `DYNAMIC` privileges that are registered with the server at the time the command is executed:

```
mysql [localhost:8023] {root} (test) > show grants for u1;
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Grants for u1@%                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `u1`@`%`                                                                                                                                                                                                       |
| GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN,BINLOG_ADMIN,BINLOG_ENCRYPTION_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,ENCRYPTION_KEY_ADMIN,FLUSH_OPTIMIZER_COSTS,FLUSH_STATUS,FLUSH_TABLES,FLUSH_USER_RESOURCES,GROUP_REPLICATION_ADMIN,INNODB_REDO_LOG_ARCHIVE,INNODB_REDO_LOG_ENABLE,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_APPLIER,REPLICATION_SLAVE_ADMIN,RESOURCE_GROUP_ADMIN,RESOURCE_GROUP_USER,ROLE_ADMIN,SERVICE_CONNECTION_ADMIN,SESSION_VARIABLES_ADMIN,SET_USER_ID,SHOW_ROUTINE,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN,TABLE_ENCRYPTION_ADMIN,XA_RECOVER_ADMIN ON *.* TO `u1`@`%` |
| GRANT BACKUP_ADMIN ON *.* TO `u1`@`%` WITH GRANT OPTION                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
3 rows in set (0.00 sec)
```

Currently TiDB does not expand `GRANT ALL` when the value is read back from `SHOW GRANTS`. It is possible to maintain this current behavior difference.

#### Alter User

No change

### Initial Set of Dynamic Privileges

#### Borrowed from MySQL

| Privilege Name | Description | Notes |
| --------------- | --------------- | --------------- |
| `BACKUP_ADMIN` | Enables BR backups and restores, as well as lightning restores. | Currently this required `SUPER`. It will now require `BACKUP_ADMIN` or `SUPER`. |
| `SYSTEM_VARIABLES_ADMIN` | Allows changing any GLOBAL system variable. | Currently this required `SUPER`. It will now require `SYSTEM_VARIABLES_ADMIN` or `SUPER`. |
| `ROLE_ADMIN` | Allows granting and revoking roles. | Won’t allow revoking on restricted_users (see below). |
| `CONNECTION_ADMIN` | Allows killing connections. | Like `PROCESS` static privilege, but slightly more restrictive (no show processlist). |
| `SYSTEM_USER` | The user can't be altered or dropped by only the `CREATE USER` privilege. | Helps prevent security escalations. |

#### TiDB Extensions

| Privilege Name | Description | Notes |
| --------------- | --------------- | --------------- |
| `RESTORE_ADMIN` | Restoring should require more permissions than backup because it is a higher risk operation. | It is inspired by MySQL's BACKUP_ADMIN/CLONE_ADMIN but not applicable to MySQL due to no online restore. |
| `RESTRICTED_VARIABLES_ADMIN` | Allows changing a restricted `GLOBAL` system variable. | Currently in SEM all high risk variables are unloaded. TBD, it might be required in future that they are only visible/settable to those with this privilege and not SUPER. |
| `RESTRICTED_STATUS_ADMIN` | Allows observing restricted status variables. | i.e. `SHOW GLOBAL STATUS` by default hides some statistics when `SEM` is enabled. |
| `RESTRICTED_CONNECTION_ADMIN` | Killing connections that belong to your own user is always permitted. Killing connections that belong to other users requires `CONNECTION_ADMIN` or `SUPER`; but there is an exception for `RESTRICTED_USER_ADMIN` users. To kill these connections also requires `RESTRICTED_CONNECTION_ADMIN`. This affects `KILL`, `KILL TIDB` commands. | It is intended for the CloudAdmin user in DBaaS. |
| `RESTRICTED_USER_ADMIN` | A special privilege to say that their access can’t be changed by `SUPER` users. Statements `DROP USER`, `SET PASSWORD`, `ALTER USER`, `REVOKE` are all limited. | It is intended for the CloudAdmin user in DbaaS. |
| `RESTRICTED_TABLES_ADMIN` | A special privilege which means that the SEM hidden table semantic doesn’t apply. | It is intended for the CloudAdmin user in DbaaS. | 

### Parser Changes

The parser already supports `DYNAMIC` privileges. This can be confirmed by the following patch to TiDB, where they are sent as the static type of `ExtendedPriv`:

```
diff --git a/planner/core/planbuilder.go b/planner/core/planbuilder.go
index 90d5b9e82..d1644ee83 100644
--- a/planner/core/planbuilder.go
+++ b/planner/core/planbuilder.go
@@ -2298,6 +2298,11 @@ func collectVisitInfoFromGrantStmt(sctx sessionctx.Context, vi []visitInfo, stmt
 
        var allPrivs []mysql.PrivilegeType
        for _, item := range stmt.Privs {
+
+               if item.Priv == mysql.ExtendedPriv {
+                       fmt.Printf("### Attempting to set DYNAMIC privilege: %s\n", item.Name)
+               }
+
                if item.Priv == mysql.AllPriv {
                        switch stmt.Level.Level {
                        case ast.GrantLevelGlobal:
```

This results in the following written to the log file:
```
mysql> grant acdc on *.* to u1;
ERROR 8121 (HY000): privilege check fail

### Attempting to set DYNAMIC privilege: acdc
```

It might be possible that changes are still required if `ExtendedPriv` is not supported in all contexts (REVOKE, etc).

Note that creating a role with the same name as a DYNAMIC privilege is supported. A `GRANT` statement can be attributed to ROLES when it omits the ON *.* syntax. Thus:

```
GRANT BINLOG_ADMIN TO u1; // grants the role binlog_admin
GRANT BINLOG_ADMIN ON *.* TO u1; // grants the dynamic privilege binlog_admin
```

This same nuance applies to MySQL.

### Documentation Plan

The statement reference pages for each of the affected metadata commands will need to be updated to describe dynamic privileges.

There will also need to be documentation specific to `DYNAMIC` privileges to describe how it works, and the purpose of fine-grained access control.

## Test Design

Testing dynamic privileges is a little bit complex because of the various ways privileges can be inherited by users:

* Direct `GRANT` to the user
* Granting to a role that the user inherits.

### Functional Tests

Unit tests will be added to cover the semantics around role/dynamic privilege precedence, including logical restoring in a different order.

Unit tests will also cover each of the "initial set of Dynamic privileges". Tests will include both directly assigning the privileges and assigning via a ROLE (RBAC).

Integration testing needs to test with global kill enabled/disabled.

### Scenario Tests

The use-cases required by the DBaaS team should be validated when combined with `security-enhanced-mode`. They are:

| Account Name | root | cloudAdmin |
| --------------- | --------------- | --------------- |
| Backup & Restore to cloud | Y | Y |
| File privilege | N | N |
| Read or set variables | Y | Y | 
| set restricted variables(some of them even can not be read) | N | Y |
| Read or set restricted system tables | N | Y | 
| DROP USER cloudAdmin | N | Hard to N(unless some hardcoded) |
| REVOKE cloudAdmin | N | Hard to N(unless some hardcoded) |
| Show processlist, Access to threads belong to other users | Y | Y |
| Change password if the password of SUPER user is forgotten | N | Y |
| Kill connections belong to cloudAdmin | N | Y |
| SHUTDOWN / RESTART | N | Y (graceful shutdown on k8s for tidb-server) |

Scenario testing will be required for:
* all the dynamic privileges
* several user-defined dynamic privileges

### Compatibility Tests

The introduction of `DYNAMIC` privileges is not expected to introduce any compatibility issues, because backwards compatibility is ensured. However, plugins should migrate to registering their own dynamic privileges and not rely on the use of `SUPER`. This is considered an enhancement, and not included in-scope for the initial introduction of dynamic privileges (which introduces the framework for plugins to use).

## Impact & Risks

In its initial release, dynamic privilege usage will be controlled by an experimental feature flag (`tidb_enable_dynamic_privileges`), which is modifyable on a GLOBAL or SESSION basis. The implementation will be via restricting `GRANT` and `REVOKE` statements from creating dynamic privileges (it is too invasive to conditionally modify the ast visitor functionaliy).

For backwards compatibility, the MySQL-compatible dynamic privileges will also permit `SUPER`. This helps prevent upgrade issues, such a when TiDB was bootstrapped `GRANT ALL ON *.*` would not have granted all the dynamic privileges. There might be some impact on Upgrade/Downgrade story if eventually the `BACKUP_ADMIN` privilege is used instead of `SUPER`, but for the initial release I am planning to allow either.

## Investigation & Alternatives

An alternative could be to support fine-grained access in a TiDB specific way. Because the MySQL functionality overlaps nicely, it doesn’t really make sense not to follow.
The initial implementation of dynamic privileges only implements a subset of MySQL’s [dynamic privileges](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html) (see table 6.3). Given that these are supposed to be “dynamic”, I don’t think this is a problem.

## Unresolved Questions

### Adding new dynamic privileges to a lower privileged user

In the case that `cloudAdmin` does not have `SUPER`, but requires additional fine grained privileges granted at a later date, there are several potential solutions:

1. Write a `session/bootstrap.go` task to "split" an existing `DYNAMIC` privilege into two. i.e. users with privilege `XYZ` now have `XYZ` and `ZYX`.
2. Allow the privilege `SELECT, INSERT, UPDATE ON mysql.*` to `cloudAdmin` + `RELOAD` on `*.*`. This will allow `cloudAdmin` to insert `ZYX` into the `global_grant` table, and then run `FLUSH PRIVILEGES` to reload the privilege cache.
3. Add an API for plugins that register new dynamic privileges, such that on first installation they can say `ZYX` is also satisfied by `XYZ` (triggering an internal copy of privileges).
4. Support a feature similar to MySQL's [`--init-file`](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_init_file) which executes with unrestricted privileges.
5. Make the privilegemanager completely pluggable (it is currently an interface, and extending it to plugins is not a difficult stretch). Make cloudAdmin privileges embedded into the cloud-specific privilege manager, and not dependent on the internal system tables.

The current recommended method is (1), since the method (2) does not effectively restrict the credentials of `cloudAdmin`. (3) is a workaround for the fact that `visitInfo` does not support OR conditions for privileges. (4) and (5) have merit, but require development outside the scope of this current proposal.
