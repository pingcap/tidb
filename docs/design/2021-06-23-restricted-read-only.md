# Technical Design Document for Restrcted Read Only

> Author: [huzhifeng@pingcap.com](mailto:huzhifeng@pingcap.com)
This document is open to the web, and it will appear as a design doc PR to the TiDB repository.

# Introduction

In this document, we introduced the `TIDB_RESTRICTED_READ_ONLY` global variable. Turning it on will make the cluster read-only eventually for all users, including users with SUPER or CONNECTION_ADMIN privilege.

# Motivation or Background

See the [original specification](https://docs.google.com/document/d/1LtmW4fIohTjPcupD9smQGQpN2B2tRSFNyH8PFbg-L2c/edit).

# **Detailed Design**

## read-only handling

Currently, TiDB supports `read_only` and `super_read_only` global variables with no actual operation. Since TIDB_RESTRICTED_READ_ONLY does not comply with the behavior of MySQL read-only and super-read-only (which will block if there is table locked or ongoing committing of a transaction), we will not build TIDB_RESTRICTED_READ_ONLY upon read-only and super-read-only. These two variables will be remained untouched.

We will create a new global variable TIDB_RESTRICTED_READ_ONLY, and turning it to on and off will only be allowed in SEM mode, and only users with RESTRICTED_VARIABLES_ADMIN will be allowed to modify this variable.

To allow replication service to write, we introduced a new dynamic privilege level called `RESTRICTED_REPLICA_WRITER_ADMIN`, user with this privilege will be ignored for read-only checks.

Upon the change of the variable on a TiDB sever, the change will be broadcasted to all other TiDB servers through PD. Normally, the other TiDB servers will receive the update immediately, however under certain circumstances (such as TiDB server lose connection to PD), the lag can be up to 30 seconds.

### How to restrict SQLs

If read-only is turned on, we will reject SQLs that might change data during planning. The rules are:

- we won't restrict internal SQLs
- we rely on an allow list to determine whether SQL is allowed to execute
    - we would allow set variables (otherwise can't unset them)
    - we would allow `analyze table`
    - we allow `show`
    - we allow create and drop SQL bindings
    - we allow prepare SQLs
    - we allow `begin` and `rollback`
    - we only allow `commit` if there is no change in the transaction, otherwise the transaction will abort
- finally, we resort to `planner/optimizer.go:IsReadOnly` for testing if the query is read-only for cases like explain, prepare and execute, etc.

## Privilege management

- add `tidb_restricted_read_only` to hidden variables, i.e., only users with `RESTRICTED_VARIABLES_ADMIN` privilege can modify it.
  - alternative: add `RESTRICTED_READ_ONLY_ADMIN` privilege, only user with this privilege can change the setting, and user with this privilege can write to the cluster without being affected by the read-only setting. However it might be redundant for introducing a new privilege level.
- add `RESTRICTED_REPLICA_WRITER_ADMIN` to allow replication writer to surpass read-only checks. Note this privilege check is required even if SEM is not enabled, i.e., SUPER user still needs to be explicitly granted this privilege to be able to write.

## Difference with MySQL read-only

MySQL has support for read-only and super-read-only, which is currently not supported in TiDB. Restricted read only shares similar functionality as them, but differs in:

* When turning on read-only or super-read-only, MySQL might [fail or block when enabling read-only under some circumstances](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_read_only). However turning on TIDB_RESTRICTED_READ_ONLY will return success immediately. Some other TiDB servers might not yet received the update of this variable, and will remain in updatable status until eventually the change on the global variable is broadcasted to all TiDB servers in the cluster.

## Alternative

- we will implement read-only and super-read-only that complies with MySQL's, and build TIDB_RESTRICED_READ_ONLY upon them, but we only need eventual read-only for TIDB_RESTRICTED_READ_ONLY. It is more relaxed, and it should always return success.
- we support read-only and super-read-only in a similar manner as TIDB_RESTRICED_READ_ONLY, this however will make its behavior incompatible with MySQL.
