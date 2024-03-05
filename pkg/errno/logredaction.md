# Log Redaction

Background:

Issue: https://github.com/pingcap/tidb/issues/18566

Our database may store some sensitive information, such as customer ID number, credit card number, etc. This sensitive information sometimes exists in the error message in some manners like a form of key-value, and this information will be printed in the log along with the error.

Some of our users do not want this sensitive information to spread along with logs. Therefore, our requirement is to provide a switch which can hide these possible sensitive information when printing the log.


For example,
```sql
mysql> create table t (a int, unique key (a));
Query OK, 0 rows affected (0.00 sec)

mysql> insert into t values (1),(1);
ERROR 1062 (23000): Duplicate entry '1' for key 'a'
mysql> set @@session.tidb_redact_log=1;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into t values (1),(1);
ERROR 1062 (23000): Duplicate entry '?' for key '?'
```

And its corresponding log is：
```
[2020/10/20 11:45:37.796 +08:00] [INFO] [conn.go:800] ["command dispatched failed"] [conn=5] [connInfo="id:5, addr:127.0.0.1:57222 status:10, collation:utf8_general_ci, user:root"] [command=Query] [status="inTxn:0, autocommit:1"] [sql="insert into t values (1),(1)"] [txn_mode=OPTIMISTIC] [err="[kv:1062]Duplicate entry '1' for key 'a'"]

[2020/10/20 11:45:49.539 +08:00] [INFO] [conn.go:800] ["command dispatched failed"] [conn=5] [connInfo="id:5, addr:127.0.0.1:57222 status:10, collation:utf8_general_ci, user:root"] [command=Query] [status="inTxn:0, autocommit:1"] [sql="insert into t values ( ? ) , ( ? )"] [txn_mode=OPTIMISTIC] [err="[kv:1062]Duplicate entry '?' for key '?'"]
```

As you can see, after enabling `tidb_redact_log`, sensitive content is hidden both in the error message and in the log.
