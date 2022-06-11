# TiDB Design Documents

- Author(s): [morgo](http://github.com/morgo)
- Discussion PR: https://github.com/pingcap/tidb/pull/28482
- Tracking Issue: https://github.com/pingcap/tidb/issues/28481

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Currently, TiDB will create a root user with no password and bind to `0.0.0.0/::`. This is a potential security issue if users do not have a firewall configured correctly, or if a rogue actor has local access.

This document introduces the `--intialize-secure` and `--initialize-insecure` bootstrap options. The current behavior is equivalent to `--initialize-insecure`, but the intention is to change the default to secure **once** it is determined stable.

## Motivation or Background

The motivation for this change is to improve the default security of TiDB. The current default is insecure, and requires additional steps to be taken by users to secure TiDB.

The design attempts to make the change with the least breakage possible. However, there will be some impact to usability.

## Detailed Design

The current bootstrap (to be referred to as `--initialize-insecure`) creates a root user as follows. This will remain unchanged:

```sql
CREATE USER 'root'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
```

The `--initialize-secure` bootstrap will instead create a user based on the OS user. For example, with an OS user of `ubuntu`:

```sql
CREATE USER 'ubuntu'@'localhost' IDENTIFIED WITH 'auth_socket' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK;
GRANT ALL PRIVILEGES ON *.* TO 'ubuntu'@'localhost' WITH GRANT OPTION;
```

After TiDB has bootstrapped, the user will be able to login and create additional users:

```bash
mysql -S /tmp/tidb.sock
..
mysql> CREATE USER 'root'@'%' IDENTIFIED BY 'securePassword';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
```

### Required Supporting Features

In order to add the `--initialize-secure` bootstrap option, the following supported features are required:

1. Support for Socket Authentication (`auth_socket`) [PR in Review](https://github.com/pingcap/tidb/pull/27561)
2. Stale socket files are automatically cleaned up on server start [PR Merged](https://github.com/pingcap/tidb/pull/27886)
3. TiDB listens on both TCP **and** unix socket by default [PR in Review](https://github.com/pingcap/tidb/pull/28486)
4. Auth plugin can be changed with ALTER/CREATE [PR Draft](https://github.com/pingcap/tidb/pull/28468)

## Test Design

Integration tests are required to verify the behavior of the `--initialize-secure` bootstrap option works with other components (such as TiDB Dashboard, Operator, DBaaS, TiUP).

Some components (such as DBaaS) may choose to explicitly use `--initialize-insecure` to bootstrap, and then alter the root password to a secure password. This is similar to how bootstrapping works in MySQL with various installation methods.

There are no risks on upgrade/downgrade testing, because the change will be implemented in the bootstrap process only. This change is not intended to be cherry picked to existing GA versions, as this will cause compatibility issues with TiDB Dashboard (confirmed by TiDB Dashboard team).

## Impacts & Risks

The design choice of `auth_socket` is based on our current requirement of only supporting unix-like operating systems, and not needing to support Windows. This is a reasonable choice, as we do not expect to support Windows in the future. There might be some minor added complexity if we currently do not handle cases well such as the socket file already existing.

Adding the option `--initialize-secure` itself does not add risk, but enabling it by default will add risk as it is a difference in behavior. This design tries to minimize the risk, but it is a large change. There is a risk if we don't update all the documentation and communicate the change effectively, it will become a support issue for new users.

## Investigation & Alternatives

The alternative implementation is to generate a random password with `--initialize-secure`. This is the approach that is used by MySQL. This has been discussed, and rejected.

## Unresolved Questions

- Should the `auth_socket` user on the MySQL-side be the OS-user, or `root`. It is possible to do either. We need to discuss what is the less of 2 evils:
  - Having 2 "roots" (localhost, and %).
  - Having a default user that is different for everyone, with instructions to create a root account.

