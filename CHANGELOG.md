# TiDB Changelog
All notable changes to this project will be documented in this file. See also [Release Notes](https://github.com/pingcap/docs/blob/master/releases/rn.md), [TiKV Changelog](https://github.com/tikv/tikv/blob/master/CHANGELOG.md) and [PD Changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [2.1.5] 2019-02-28

### SQL Optimizer/Executor
* Make `SHOW CREATE TABLE` do not print the column charset information when the charset information of a column is the same with that of a table, to improve the compatibility of `SHOW CREATE TABLE` with MySQL [#9306](https://github.com/pingcap/tidb/pull/9306)
* Fix the panic or the wrong result of the `Sort` operator in some cases by extracting `ScalarFunc` from `Sort` to a `Projection` operator for computing to simplify the computing logic of `Sort` [#9319](https://github.com/pingcap/tidb/pull/9319)
* Remove the sorting field with constant values in the `Sort` operator [#9335](https://github.com/pingcap/tidb/pull/9335), [#9440](https://github.com/pingcap/tidb/pull/9440)
* Fix the data overflow issue when inserting data into an unsigned integer column [#9339](https://github.com/pingcap/tidb/pull/9339)
* Set `cast_as_binary` to `NULL` when the length of the target binary exceeds `max_allowed_packet` [#9349](https://github.com/pingcap/tidb/pull/9349)
* Optimize the constant folding process of `IF` and `IFNULL` [#9351](https://github.com/pingcap/tidb/pull/9351)
* Optimize the index selection of TiDB using skyline pruning to improve the stability of simple queries [#9356](https://github.com/pingcap/tidb/pull/9356)
* Support computing the selectivity of the `DNF` expression [#9405](https://github.com/pingcap/tidb/pull/9405)
* Fix the wrong SQL query result of `!=ANY()` and `=ALL()` in some cases [#9403](https://github.com/pingcap/tidb/pull/9403)
* Fix the panic or the wrong result when the Join Key types of two tables on which  the `Merge Join` operation is performed are different [#9438](https://github.com/pingcap/tidb/pull/9438)
* Fix the issue that the result of the `RAND()` function is not compatible with MySQL [#9446](https://github.com/pingcap/tidb/pull/9446)
* Refactor the logic of `Semi Join` processing `NULL` and the empty result set to get the correct result and improve the compatibility with MySQL [#9449](https://github.com/pingcap/tidb/pull/9449)

### Server
* Add the `tidb_constraint_check_in_place` system variable to check the data uniqueness constraint when executing the `INSERT` statement [#9401](https://github.com/pingcap/tidb/pull/9401)
* Fix the issue that the value of the `tidb_force_priority` system variable is different from that set in the configuration file [#9347](https://github.com/pingcap/tidb/pull/9347)
* Add the `current_db` field in general logs to print the name of the currently used database [#9346](https://github.com/pingcap/tidb/pull/9346)
* Add an HTTP API of obtaining the table information with the table ID [#9408](https://github.com/pingcap/tidb/pull/9408)
* Fix the issue that `LOAD DATA` loads incorrect data in some cases [#9414](https://github.com/pingcap/tidb/pull/9414)
* Fix the issue that it takes a long time to build a connection between the MySQL client and TiDB in some cases [#9451](https://github.com/pingcap/tidb/pull/9451)

### DDL
* Fix some issues when canceling the `DROP COLUMN` operation [#9352](https://github.com/pingcap/tidb/pull/9352)
* Fix some issues when canceling the `DROP` or `ADD` partitioned table operation [#9376](https://github.com/pingcap/tidb/pull/9376)
* Fix the issue that `ADMIN CHECK TABLE` mistakenly reports the data index inconsistency in some cases [#9399](https://github.com/pingcap/tidb/pull/9399)
* Fix the time zone issue of the `TIMESTAMP` default value [#9108](https://github.com/pingcap/tidb/pull/9108)

## [2.1.4] 2019-02-15

### SQL Optimizer/Executor

* Fix the issue that the `VALUES` function does not handle the FLOAT type correctly [#9223](https://github.com/pingcap/tidb/pull/9223)
* Fix the wrong result issue when casting Float to String in some cases [#9227](https://github.com/pingcap/tidb/pull/9227)
* Fix the wrong result issue of the `FORMAT` function in some cases [#9235](https://github.com/pingcap/tidb/pull/9235)
* Fix the panic issue when handling the Join query in some cases [#9264](https://github.com/pingcap/tidb/pull/9264)
* Fix the issue that the `VALUES` function does not handle the ENUM type correctly [#9280](https://github.com/pingcap/tidb/pull/9280)
* Fix the wrong result issue of `DATE_ADD` / `DATE_SUB` in some cases [#9284](https://github.com/pingcap/tidb/pull/9284)

### Server

* Optimize the "reload privilege success" log and change it to the DEBUG level [#9274](https://github.com/pingcap/tidb/pull/9274)

### DDL

* Change `tidb_ddl_reorg_worker_cnt` and `tidb_ddl_reorg_batch_size` to global variables [#9134](https://github.com/pingcap/tidb/pull/9134)
* Fix the bug caused by adding an index to a generated column in some abnormal conditions [#9289](https://github.com/pingcap/tidb/pull/9289)

## [2.1.3] 2019-01-25

### SQL Optimizer/Executor
* Fix the panic issue of Prepared Plan Cache in some cases [#8826](https://github.com/pingcap/tidb/pull/8826)
* Fix the issue that Range computing is wrong when the index is a prefix index  [#8851](https://github.com/pingcap/tidb/pull/8851)
* Make `CAST(str AS TIME(N))` return null if the string is in the illegal `TIME` format when `SQL_MODE` is not strict [#8966](https://github.com/pingcap/tidb/pull/8966)
* Fix the panic issue of Generated Column during the process of `UPDATE` in some cases [#8980](https://github.com/pingcap/tidb/pull/8980)
* Fix the upper bound overflow issue of the statistics histogram in some cases [#8989](https://github.com/pingcap/tidb/pull/8989)
* Support Range for `_tidb_rowid` construction queries, to avoid full table scan and reduce cluster stress [#9059](https://github.com/pingcap/tidb/pull/9059)
* Return an error when the `CAST(AS TIME)` precision is too big [#9058](https://github.com/pingcap/tidb/pull/9058)
* Allow using `Sort Merge Join` in the Cartesian product [#9037](https://github.com/pingcap/tidb/pull/9037)
* Fix the issue that the statistics worker cannot resume after the panic in some cases [#9085](https://github.com/pingcap/tidb/pull/9085)
* Fix the issue that `Sort Merge Join` returns the wrong result in some cases [#9046](https://github.com/pingcap/tidb/pull/9046)
* Support returning the JSON type in the `CASE` clause [#8355](https://github.com/pingcap/tidb/pull/8355)
### Server
* Return a warning instead of an error when the non-TiDB hint exists in the comment [#8766](https://github.com/pingcap/tidb/pull/8766)
* Verify the validity of the configured TIMEZONE value [#8879](https://github.com/pingcap/tidb/pull/8879)
* Optimize the `QueryDurationHistogram` metrics item to display more statement types [#8875](https://github.com/pingcap/tidb/pull/8875)
* Fix the lower bound overflow issue of bigint in some cases [#8544](https://github.com/pingcap/tidb/pull/8544)
* Support the `ALLOW_INVALID_DATES` SQL mode [#9110](https://github.com/pingcap/tidb/pull/9110)
### DDL
* Fix a `RENAME TABLE` compatibility issue to keep the behavior consistent with that of MySQL [#8808](https://github.com/pingcap/tidb/pull/8808)
* Support making concurrent changes of `ADD INDEX` take effect immediately [#8786](https://github.com/pingcap/tidb/pull/8786)
* Fix the `UPDATE` panic issue during the process of `ADD COLUMN` in some cases [#8906](https://github.com/pingcap/tidb/pull/8906)
* Fix the issue of concurrently creating Table Partition in some cases [#8902](https://github.com/pingcap/tidb/pull/8902)
* Support converting the `utf8` character set to `utf8mb4` [#8951](https://github.com/pingcap/tidb/pull/8951) [#9152](https://github.com/pingcap/tidb/pull/9152)
* Fix the issue of Shard Bits overflow [#8976](https://github.com/pingcap/tidb/pull/8976)
* Support outputting the column character sets in `SHOW CREATE TABLE` [#9053](https://github.com/pingcap/tidb/pull/9053)
* Fix the issue of the maximum length limit of the varchar type column in `utf8mb4` [#8818](https://github.com/pingcap/tidb/pull/8818)
* Support `ALTER TABLE TRUNCATE TABLE PARTITION` [#9093](https://github.com/pingcap/tidb/pull/9093)
* Resolve the charset when the charset is not provided [#9147](https://github.com/pingcap/tidb/pull/9147)


## [2.1.2] 2018-12-21
* Make TiDB compatible with TiDB-Binlog of the Kafka version [#8747](https://github.com/pingcap/tidb/pull/8747)
* Improve the exit mechanism of TiDB in a rolling update [#8707](https://github.com/pingcap/tidb/pull/8707)
* Fix the panic issue caused by adding the index for the generated column in some cases [#8676](https://github.com/pingcap/tidb/pull/8676)
* Fix the issue that the optimizer cannot find the optimal query plan when `TIDB_SMJ Hint` exists in the SQL statement in some cases [#8729](https://github.com/pingcap/tidb/pull/8729)
* Fix the issue that `AntiSemiJoin` returns an incorrect result in some cases [#8730](https://github.com/pingcap/tidb/pull/8730)
* Improve the valid character check of the `utf8` character set [#8754](https://github.com/pingcap/tidb/pull/8754)
* Fix the issue that the field of the time type might return an incorrect result when the write operation is performed before the read operation in a transaction [#8746](https://github.com/pingcap/tidb/pull/8746)


## [2.1.1] 2018-12-12

### SQL Optimizer/Executor

* Fix the round error of the negative date [#8574](https://github.com/pingcap/tidb/pull/8574)
* Fix the issue that the `uncompress` function does not check the data length [#8606](https://github.com/pingcap/tidb/pull/8606)
* Reset bind arguments of the `prepare` statement after the `execute` command is executed [#8652](https://github.com/pingcap/tidb/pull/8652)
* Support automatically collecting the statistics information of a partition table [#8649](https://github.com/pingcap/tidb/pull/8649)
* Fix the wrongly configured integer type when pushing down the `abs` function [#8628](https://github.com/pingcap/tidb/pull/8628)
* Fix the data race on the JSON column [#8660](https://github.com/pingcap/tidb/pull/8660)

### Server

* Fix the issue that the transaction obtained TSO is incorrect when PD breaks down [#8567](https://github.com/pingcap/tidb/pull/8567)
* Fix the bootstrap failure caused by the statement that does not conform to ANSI standards [#8576](https://github.com/pingcap/tidb/pull/8576)
* Fix the issue that incorrect parameters are used in transaction retries [#8638](https://github.com/pingcap/tidb/pull/8638)

### DDL

* Change the default character set and collation of tables into `utf8mb4` [#8590](https://github.com/pingcap/tidb/pull/8590)
* Add the `ddl_reorg_batch_size` variable to control the speed of adding indexes [#8614](https://github.com/pingcap/tidb/pull/8614)
* Make the character set and collation options content in DDL case-insensitive [#8611](https://github.com/pingcap/tidb/pull/8611)
* Fix the issue of adding indexes for generated columns [#8655](https://github.com/pingcap/tidb/pull/8655)

## [2.1.0-GA] 2018-11-30

### Upgrade caveat
* TiDB 2.1 does not support downgrading to v2.0.x or earlier due to the adoption of the new storage engine
* Parallel DDL is enabled in TiDB 2.1, so the clusters with TiDB version earlier than 2.0.1 cannot upgrade to 2.1 using rolling update. You can choose either of the following two options:
    - Stop the cluster and upgrade to 2.1 directly
    - Roll update to 2.0.1 or later 2.0.x versions, and then roll update to the 2.1 version
* `raft learner` is enabled in TiDB 2.1 by default. If you upgrade the TiDB platform from 1.x to 2.1, stop the cluster before the upgrade. You can also roll update TiKV to 2.1 and then roll update PD to 2.1.
* If you upgrade from TiDB 2.0.6 or earlier to TiDB 2.1, make sure if there is any ongoing DDL operation, especially the time consuming `Add Index` operation, because the DDL operations slow down the upgrading process.

### SQL Optimizer
* Optimize the selection range of `Index Join` to improve the execution performance
* Optimize the selection of outer table for `Index Join` and use the table with smaller estimated value of Row Count as the outer table
* Optimize Join Hint `TIDB_SMJ` so that Merge Join can be used even without proper index available
* Optimize Join Hint `TIDB_INLJ` to specify the Inner table to Join
* Optimize correlated subquery, push down Filter, and extend the index selection range, to improve the efficiency of some queries by orders of magnitude
* Support using Index Hint and Join Hint in the `UPDATE` and `DELETE` statement
* Support pushing down more functions:`ABS` / `CEIL` / `FLOOR` / `IS TRUE` / `IS FALSE`
* Optimize the constant folding algorithm for the `IF` and `IFNULL` built-in functions
* Optimize the output of the `EXPLAIN` statement and use hierarchy structure to show the relationship between operators

### SQL executor
* Refactor all the aggregation functions and improve execution efficiency of the `Stream` and `Hash` aggregation operators
* Implement the parallel `Hash Aggregate` operators and improve the computing performance by 350% in some scenarios
* Implement the parallel `Project` operators and improve the performance by 74% in some scenarios
* Read the data of the Inner table and Outer table of `Hash Join` concurrently to improve the execution performance
* Optimize the execution speed of the `REPLACE INTO` statement and increase the performance nearly by 10 times
* Optimize the memory usage of the time data type and decrease the memory usage of the time data type by fifty percent
* Optimize the point select performance and improve the point select efficiency result of Sysbench by 60%
* Improve the performance of TiDB on inserting or updating wide tables by 20 times
* Support configuring the memory upper limit of a single statement in the configuration file
* Optimize the execution of Hash Join, if the Join type is Inner Join or Semi Join and the inner table is empty, return the result without reading data from the outer table
* Support using the [`EXPLAIN ANALYZE` statement](https://www.pingcap.com/docs/sql/understanding-the-query-execution-plan/#explain-analyze-output-format) to check the runtime statistics including the execution time and the number of returned rows of each operator

### Statistics
* Support enabling auto ANALYZE statistics only during certain period of the day
* Support updating the table statistics automatically according to the feedback of the queries
* Support configuring the number of buckets in the histogram using the `ANALYZE TABLE WITH BUCKETS` statement
* Optimize the Row Count estimation algorithm using histogram for mixed queries of equality query and range queries

### Expressions
* Support following built-in function:
    - `json_contains`
    - `json_contains_path`
    - `encode/decode`

### Server
* Support queuing the locally conflicted transactions within tidb-server instance to optimize the performance of conflicted transactions
* Support Server Side Cursor
* Add the [HTTP API](https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md)
    - Scatter the distribution of table Regions in the TiKV cluster
    - Control whether to open the `general log`
    - Support modifying the log level online
    - Check the TiDB cluster information
* [Add the `auto_analyze_ratio` system variables to contorl the ratio of Analyze](https://pingcap.com/docs/FAQ/#whats-the-trigger-strategy-for-auto-analyze-in-tidb)
* [Add the `tidb_retry_limit` system variable to control the automatic retry times of transactions](https://pingcap.com/docs/sql/tidb-specific/#tidb-retry-limit)
* [Add the `tidb_disable_txn_auto_retry` system variable to control whether the transaction retries automatically](https://pingcap.com/docs/sql/tidb-specific/#tidb-disable-txn-auto-retry)
* [Support using `admin show slow` statement to obtain the slow queries ](https://pingcap.com/docs/sql/slow-query/#admin-show-slow-command)
* [Add the `tidb_slow_log_threshold` environment variable to set the threshold of slow log automatically](https://pingcap.com/docs/sql/tidb-specific/#tidb_slow_log_threshold)
* [Add the `tidb_query_log_max_len` environment variable to set the length of the SQL statement to be truncated in the log dynamically](https://pingcap.com/docs/sql/tidb-specific/#tidb_query_log_max_len)

### DDL
* Support the parallel execution of the add index statement and other statements to avoid the time consuming add index operation blocking other operations
* Optimize the execution speed of `ADD INDEX` and improve it greatly in some scenarios
* Support the `select tidb_is_ddl_owner()` statement to facilitate deciding whether TiDB is `DDL Owner`
* Support the `ALTER TABLE FORCE` syntax
* Support the `ALTER TABLE RENAME KEY TO` syntax
* Add the table name and database name in the output information of `admin show ddl jobs`
* [Support using the `ddl/owner/resign` HTTP interface to release the DDL owner and start electing a new DDL owner](https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md)

### Compatibility
* Support more MySQL syntaxes
* Make the `BIT` aggregate function support the `ALL` parameter
* Support the `SHOW PRIVILEGES` statement
* Support the `CHARACTER SET` syntax in the `LOAD DATA` statement
* Support the `IDENTIFIED WITH` syntax in the `CREATE USER` statement
* Support the `LOAD DATA IGNORE LINES` statement
* The `Show ProcessList` statement returns more accurate information


## [2.1.0-rc.5] - 2018-11-12
### SQL Optimizer
* Fix the issue that `IndexReader` reads the wrong handle in some cases [#8132](https://github.com/pingcap/tidb/pull/8132)
* Fix the issue occurred while the `IndexScan Prepared` statement uses `Plan Cache` [#8055](https://github.com/pingcap/tidb/pull/8055)
* Fix the issue that the result of the `Union` statement is unstable [#8165](https://github.com/pingcap/tidb/pull/8165)
### SQL Execution Engine
* Improve the performance of TiDB on inserting or updating wide tables [#8024](https://github.com/pingcap/tidb/pull/8024)
* Support the unsigned `int` flag in the `Truncate` built-in function [#8068](https://github.com/pingcap/tidb/pull/8068)
* Fix the error occurred while converting JSON data to the decimal type [#8109](https://github.com/pingcap/tidb/pull/8109)
* Fix the error occurred when you `Update` the float type [#8170](https://github.com/pingcap/tidb/pull/8170)
### Statistics
* Fix the incorrect statistics issue during point queries in some cases [#8035](https://github.com/pingcap/tidb/pull/8035)
* Fix the selectivity estimation of statistics for primary key in some cases [#8149](https://github.com/pingcap/tidb/pull/8149)
* Fix the issue that the statistics of deleted tables are not cleared up for a long period of time [#8182](https://github.com/pingcap/tidb/pull/8182)
### Server
* Improve the readability of logs and make logs better
    - [#8063](https://github.com/pingcap/tidb/pull/8063)
    - [#8053](https://github.com/pingcap/tidb/pull/8053)
    - [#8224](https://github.com/pingcap/tidb/pull/8224)
* Fix the error occurred when obtaining the table data of `infoschema.profiling` [#8096](https://github.com/pingcap/tidb/pull/8096)
* Replace the unix socket with the pumps client to write binlogs [#8098](https://github.com/pingcap/tidb/pull/8098)
* Add the threshold value for the `tidb_slow_log_threshold` environment variable, which dynamically sets the slow log [#8094](https://github.com/pingcap/tidb/pull/8094)
* Add the original length of a SQL statement truncated while the `tidb_query_log_max_len` environment variable dynamically sets logs [8200](https://github.com/pingcap/tidb/pull/8200)
* Add the `tidb_opt_write_row_id` environment variable to control whether to allow writing `_tidb_rowid` [#8218](https://github.com/pingcap/tidb/pull/8218)
* Add an upper bound to the `Scan` command of ticlient, to avoid overbound scan [#8081](https://github.com/pingcap/tidb/pull/8081), [#8247](https://github.com/pingcap/tidb/pull/8247)
### DDL
* Fix the issue that executing DDL statements in transactions encounters an error in some cases [#8056](https://github.com/pingcap/tidb/pull/8056)
* Fix the issue that executing `truncate table` in partition tables does not take effect [#8103](https://github.com/pingcap/tidb/pull/8103)
* Fix the issue that the DDL operation does not roll back correctly after being cancelled in some cases [#8057](https://github.com/pingcap/tidb/pull/8057)
* Add the `admin show next_row_id` command to return the next available row ID [#8268](https://github.com/pingcap/tidb/pull/8268)


## [2.1.0-rc.4] - 2018-10-23
### SQL Optimizer
* Fix the issue that column pruning of `UnionAll` is incorrect in some cases [#7941](https://github.com/pingcap/tidb/pull/7941)
* Fix the issue that the result of the `UnionAll` operator is incorrect in some cases [#8007](https://github.com/pingcap/tidb/pull/8007)
### SQL Execution Engine
* Fix the precision issue of the `AVG` function [#7874](https://github.com/pingcap/tidb/pull/7874)
* Support using the `EXPLAIN ANALYZE` statement to check the runtime statistics including the execution time and the number of returned rows of each operator during the query execution process [#7925](https://github.com/pingcap/tidb/pull/7925)
* Fix the panic issue of the `PointGet` operator when a column of a table appears multiple times in the result set [#7943](https://github.com/pingcap/tidb/pull/7943)
* Fix the panic issue caused by too large values in the `Limit` subclause [#8002](https://github.com/pingcap/tidb/pull/8002)
* Fix the panic issue during the execution process of the `AddDate`/`SubDate` statement in some cases [#8009](https://github.com/pingcap/tidb/pull/8009)
### Statistics
* Fix the issue of judging the prefix of the histogram low-bound of the combined index as out of range [#7856](https://github.com/pingcap/tidb/pull/7856)
* Fix the memory leak issue caused by statistics collecting [#7873](https://github.com/pingcap/tidb/pull/7873)
* Fix the panic issue when the histogram is empty [#7928](https://github.com/pingcap/tidb/pull/7928)
* Fix the issue that the histogram bound is out of range when the statistics is being uploaded [#7944](https://github.com/pingcap/tidb/pull/7944)
* Limit the maximum length of values in the statistics sampling process [#7982](https://github.com/pingcap/tidb/pull/7982)
### Server
* Refactor Latch to avoid misjudgment of transaction conflicts and improve the execution performance of concurrent transactions [#7711](https://github.com/pingcap/tidb/pull/7711)
* Fix the panic issue caused by collecting slow queries in some cases [#7874](https://github.com/pingcap/tidb/pull/7847)
* Fix the panic issue when `ESCAPED BY` is an empty string in the `LOAD DATA` statement [#8005](https://github.com/pingcap/tidb/pull/8005)
* Complete the “coprocessor error” log information [#8006](https://github.com/pingcap/tidb/pull/8006)
### Compatibility
* Set the `Command` field of the `SHOW PROCESSLIST` result to `Sleep` when the query is empty [#7839](https://github.com/pingcap/tidb/pull/7839)
### Expressions
* Fix the constant folding issue of the `SYSDATE` function [#7895](https://github.com/pingcap/tidb/pull/7895)
* Fix the issue that `SUBSTRING_INDEX` panics in some cases [#7897](https://github.com/pingcap/tidb/pull/7897)
### DDL
* Fix the stack overflow issue caused by throwing the `invalid ddl job type` error [#7958](https://github.com/pingcap/tidb/pull/7958)
* Fix the issue that the result of `ADMIN CHECK TABLE` is incorrect in some cases [#7975](https://github.com/pingcap/tidb/pull/7975)


## [2.1.0-rc.2] - 2018-09-14
### SQL Optimizer
* Put forward a proposal of the next generation Planner [#7543](https://github.com/pingcap/tidb/pull/7543)
* Improve the optimization rules of constant propagation [#7276](https://github.com/pingcap/tidb/pull/7276)
* Enhance the computing logic of `Range` to enable it to handle multiple `IN` or `EQUAL` conditions simultaneously [#7577](https://github.com/pingcap/tidb/pull/7577)
* Fix the issue that the estimation result of `TableScan` is incorrect when `Range` is empty [#7583](https://github.com/pingcap/tidb/pull/7583)
* Support the `PointGet` operator for the `UPDATE` statement [#7586](https://github.com/pingcap/tidb/pull/7586)
* Fix the panic issue during the process of executing the `FirstRow` aggregate function in some conditions [#7624](https://github.com/pingcap/tidb/pull/7624)
### SQL Execution Engine
* Fix the potential `DataRace` issue when the `HashJoin` operator encounters an error [#7554](https://github.com/pingcap/tidb/pull/7554)
* Make the `HashJoin` operator read the inner table and build the hash table simultaneously [#7544](https://github.com/pingcap/tidb/pull/7544)
* Optimize the performance of Hash aggregate operators [#7541](https://github.com/pingcap/tidb/pull/7541)
* Optimize the performance of Join operators [#7493](https://github.com/pingcap/tidb/pull/7493), [#7433](https://github.com/pingcap/tidb/pull/7433)
* Fix the issue that the result of `UPDATE JOIN` is incorrect when the Join order is changed [#7571](https://github.com/pingcap/tidb/pull/7571)
* Improve the performance of Chunk’s iterator [#7585](https://github.com/pingcap/tidb/pull/7585)
### Statistics
* Fix the issue that the auto Analyze work repeatedly analyzes the statistics [#7550](https://github.com/pingcap/tidb/pull/7550)
* Fix the statistics update error that occurs when there is no statistics change [#7530](https://github.com/pingcap/tidb/pull/7530)
* Use the RC isolation level and low priority when building `Analyze` requests [#7496](https://github.com/pingcap/tidb/pull/7496)
* Support enabling statistics auto-analyze on certain period of a day [#7570](https://github.com/pingcap/tidb/pull/7570)
* Fix the panic issue when logging the statistics information [#7588](https://github.com/pingcap/tidb/pull/7588)
* Support configuring the number of buckets in the histogram using the `ANALYZE TABLE WITH BUCKETS` statement [#7619](https://github.com/pingcap/tidb/pull/7619)
* Fix the panic issue when updating an empty histogram [#7640](https://github.com/pingcap/tidb/pull/7640)
* Update `information_schema.tables.data_length` using the statistics information [#7657](https://github.com/pingcap/tidb/pull/7657)
### Server
* Add Trace related dependencies [#7532](https://github.com/pingcap/tidb/pull/7532)
* Enable the `mutex profile` feature of Golang  [#7512](https://github.com/pingcap/tidb/pull/7512)
* The `Admin` statement requires the `Super_priv` privilege [#7486](https://github.com/pingcap/tidb/pull/7486)
* Forbid users to `Drop` crucial system tables [#7471](https://github.com/pingcap/tidb/pull/7471)
* Switch from `juju/errors` to `pkg/errors` [#7151](https://github.com/pingcap/tidb/pull/7151)
* Complete the functional prototype of SQL Tracing [#7016](https://github.com/pingcap/tidb/pull/7016)
* Remove the goroutine pool [#7564](https://github.com/pingcap/tidb/pull/7564)
* Support viewing the goroutine information using the `USER1` signal [#7587](https://github.com/pingcap/tidb/pull/7587)
* Set the internal SQL to high priority while TiDB is started [#7616](https://github.com/pingcap/tidb/pull/7616)
* Use different labels to filter internal SQL and user SQL in monitoring metrics [#7631](https://github.com/pingcap/tidb/pull/7631)
* Store the top 30 slow queries in the last week to the TiDB server [#7646](https://github.com/pingcap/tidb/pull/7646)
* Put forward a proposal of setting the global system time zone for the TiDB cluster [#7656](https://github.com/pingcap/tidb/pull/7656)
* Enrich the error message of “GC life time is shorter than transaction duration” [#7658](https://github.com/pingcap/tidb/pull/7658)
* Set the global system time zone when starting the TiDB cluster [#7638](https://github.com/pingcap/tidb/pull/7638)
### Compatibility
* Add the unsigned flag for the `Year` type [#7542](https://github.com/pingcap/tidb/pull/7542)
* Fix the issue of configuring the result length of the `Year` type in the `Prepare`/`Execute` mode [#7525](https://github.com/pingcap/tidb/pull/7525)
* Fix the issue of inserting zero timestamp in the `Prepare`/`Execute` mode [#7506](https://github.com/pingcap/tidb/pull/7506)
* Fix the error handling issue of the integer division [#7492](https://github.com/pingcap/tidb/pull/7492)
* Fix the compatibility issue when processing `ComStmtSendLongData` [#7485](https://github.com/pingcap/tidb/pull/7485)
* Fix the error handling issue during the process of converting string to integer [#7483](https://github.com/pingcap/tidb/pull/7483)
* Optimize the accuracy of values in the `information_schema.columns_in_table` table [#7463](https://github.com/pingcap/tidb/pull/7463)
* Fix the compatibility issue when writing or updating the string type of data using the MariaDB client [#7573](https://github.com/pingcap/tidb/pull/7573)
* Fix the compatibility issue of aliases of the returned value [#7600](https://github.com/pingcap/tidb/pull/7600)
* Fix the issue that the `NUMERIC_SCALE` value of the float type is incorrect in the `information_schema.COLUMNS` table [#7602](https://github.com/pingcap/tidb/pull/7602)
* Fix the issue that Parser reports an error when the single line comment is empty [#7612](https://github.com/pingcap/tidb/pull/7612)
### Expressions
* Check the value of `max_allowed_packet` in the `insert` function [#7528](https://github.com/pingcap/tidb/pull/7528)
* Support the built-in function `json_contains`  [#7443](https://github.com/pingcap/tidb/pull/7443)
* Support the built-in function `json_contains_path` [#7596](https://github.com/pingcap/tidb/pull/7596)
* Support the built-in function `encode/decode` [#7622](https://github.com/pingcap/tidb/pull/7622)
* Fix the issue that some time related functions are not compatible with the MySQL behaviors in some cases [#7636](https://github.com/pingcap/tidb/pull/7636)
* Fix the compatibility issue of parsing the time type of data in string [#7654](https://github.com/pingcap/tidb/pull/7654)
* Fix the issue that the time zone is not considered when computing the default value of the `DateTime` data [#7655](https://github.com/pingcap/tidb/pull/7655)
### DML
* Set correct `last_insert_id` in the `InsertOnDuplicateUpdate` statement [#7534](https://github.com/pingcap/tidb/pull/7534)
* Reduce the cases of updating the `auto_increment_id` counter [#7515](https://github.com/pingcap/tidb/pull/7515)
* Optimize the error message of `Duplicate Key` [#7495](https://github.com/pingcap/tidb/pull/7495)
* Fix the `insert...select...on duplicate key update` issue [#7406](https://github.com/pingcap/tidb/pull/7406)
* Support the `LOAD DATA IGNORE LINES` statement [#7576](https://github.com/pingcap/tidb/pull/7576)
### DDL
* Add the DDL job type and the current schema version information in the monitor [#7472](https://github.com/pingcap/tidb/pull/7472)
* Complete the design of the `Admin Restore Table` feature [#7383](https://github.com/pingcap/tidb/pull/7383)
* Fix the issue that the default value of the `Bit` type exceeds 128 [#7249](https://github.com/pingcap/tidb/pull/7249)
* Fix the issue that the default value of the `Bit` type cannot be `NULL` [#7604](https://github.com/pingcap/tidb/pull/7604)
* Reduce the interval of checking `CREATE TABLE/DATABASE` in the DDL queue [#7608](https://github.com/pingcap/tidb/pull/7608)
* Use the `ddl/owner/resign` HTTP interface ro release the DDL owner and start electing a new owner [#7649](https://github.com/pingcap/tidb/pull/7649)
### TiKV Go Client
* Support the issue that the `Seek` operation only obtains `Key`  [#7419](https://github.com/pingcap/tidb/pull/7419)
### [Table Partition](https://github.com/pingcap/tidb/projects/6) (Experimental)
* Fix the issue that the `Bigint` type cannot be used as the partition key  [#7520](https://github.com/pingcap/tidb/pull/7520)
* Support the rollback operation when an issue occurs during adding an index in the partitioned table [#7437](https://github.com/pingcap/tidb/pull/7437)


## [2.0.0-rc.5] - 2018-04-17
### New Features
* Support showing memory usage of the executing statements in the `SHOW PROCESSLIST` statement
* Support setting the table comment using the `Alter` statement
### Improvements
* Clean up the written data while rolling back the `Add Index` operation, to reduce consumed space
* Optimize the insert on duplicate key update statement to improve the performance by 10 times
### Bug Fixes
* Fix the issue about applying the Top-N pushdown rule
* Fix the issue that `Alter Table Modify Column` reports an error in extreme conditions
* Fix the issue about the type of the results returned by the `UNIX_TIMESTAMP` function
* Fix the issue that the NULL value is inserted while adding NOT NULL columns
* Fix the estimation of the number of rows for the columns that contain NULL values
* Fix the zero value of the Binary type
* Fix the BatchGet issue within a transaction

## [2.0.0-rc.4] - 2018-04-01
### New Features
* Support `SHOW GRANTS FOR CURRENT_USER();`
* Support the `SET TRANSACTION` syntax
* Support displaying floating point numbers using scientific notation
### Improvements
* Improve the execution performance of `DecodeBytes`
* Optimize `LIMIT 0` to `TableDual`, to avoid building useless execution plans
### Bug Fixes
* Fix the issue that the `Expression` in `UnionScan` is not cloned
* Fix the potential goroutine leak issue in `copIterator`
* Fix the issue that admin check table misjudges the unique index including null
* Fix the type inference issue during binary literal computing
* Fix the issue in parsing the `CREATE VIEW` statement
* Fix the panic issue when one statement contains both `ORDER BY` and `LIMIT 0`

## [2.0.0-rc.3] - 2018-03-23
### New Features
* Support closing the `Join Reorder` optimization in the optimizer using `STRAIGHT_JOIN`
* Output more detailed status information of DDL jobs in `ADMIN SHOW DDL JOBS`
* Support querying the original statements of currently running DDL jobs using `ADMIN SHOW DDL JOB QUERIES`
* Support recovering the index data using `ADMIN RECOVER INDEX` for disaster recovery
* Attach a lower priority to the `ADD INDEX` operation to reduce the impact on online business
* Support aggregation functions with JSON type parameters, such as `SUM/AVG`
* Support modifying the `lower_case_table_names` system variable in the configuration file, to support the OGG data synchronization tool
* Support using implicit `RowID` in CRUD operations
### Improvements
* Improve compatibility with the Navicat management tool
* Use the Stream Aggregation operator when the `GROUP BY` clause is empty, to increase the speed
* Optimize the execution speed of `ADD INDEX` to greatly increase the speed in some scenarios
* Optimize checks on length and precision of the floating point type, to improve compatibility with MySQL
* Improve the parsing error log of time type and add more error information
* Improve memory control and add statistics about `IndexLookupExecutor` memory
### Bug Fixes
* Fix the wrong result issue of `MAX/MIN` in some scenarios
* Fix the issue that the result of `Sort Merge Join` does not show in order of `Join Key` in some scenarios
* Fix the error of comparison between uint and int in boundary conditions

## [2.0.0-rc.2] - 2018-03-15
Only TiKV has this release

## [2.0.0-rc.1] - 2018-03-09
### New Features
* Support limiting the memory usage by a single SQL statement, to reduce the risk of OOM
* Support pushing the Stream Aggregate operator down to TiKV
* Support validating the configuration file
* Support obtaining the information of TiDB configuration through HTTP API
### Improvements
* Improve the compatibility with Navicat
* Improve the optimizer and extract common expressions with multiple OR conditions, to choose better query plan
* Improve the optimizer and convert subqueries to Join operators in more scenarios, to choose better query plan
* Compatible with more MySQL syntax in Parser
* Resolve Lock in the Batch mode to increase the garbage collection speed
* Optimize the `Add Index` operation and give lower priority to all write and read operations, to reduce the impact on online business
### Bug Fixes
* Fix the length of Boolean field to improve compatibility

## [1.1.0-beta] - 2018-02-24
### New Features
* Add more monitoring metrics and refine the log
* Add the `tidb_config` session variable to output the current TiDB configuration
* Support displaying the table creating time in `information_schema`
### Improvements
* Compatible with more MySQL syntax
* Optimize queries containing the `MaxOneRow` operator
* Configure the size of intermediate result sets generated by Join, to further reduce the memory used by Join
* Optimize the query performance of the SQL engine to improve the test result of the Sysbench Select/OLTP by 10%
* Improve the computing speed of subqueries in the optimizer using the new execution engine; compared with TiDB 1.0, TiDB 1.1 Beta has great improvement in tests like TPC-H and TPC-DS
### Bug Fixes
* Fix the panic issue in the `Union` and `Index Join` operators
* Fix the wrong result issue of the `Sort Merge Join` operator in some scenarios
* Fix the issue that the `Show Index` statement shows indexes that are in the process of adding
* Fix the failure of the `Drop Stats` statement

## [1.0.8] - 2018-02-11
### New Features
* Add limitation (Configurable, the default value is 5000) to the DML statements number within a transaction
### Improvements
* Improve the stability of the GC process by ignoring the regions with GC errors
* Run GC concurrently to accelerate the GC process
* Provide syntax support for the `CREATE INDEX` statement
* Optimize the performance of the `InsertIntoIgnore` statement
### Bug Fixes
* Fix issues in the `Outer Join` result in some scenarios
* Fix the issue in the `ShardRowID` option
* Fix an issue in the `Table/Column` aliases returned by the Prepare statement
* Fix an issue in updating statistics delta
* Fix a panic error in the `Drop Column` statement
* Fix an DML issue when running the `Add Column After` statement

## [1.0.7] - 2018-01-22
### Improvements
* Optimize the `FIELD_LIST` command
* Fix data race of the information schema
* Avoid adding read-only statements to history
* Add the session variable to control the log query
* Add schema info API for the http status server
* Update the behavior when `RunWorker` is false in DDL
* Improve the stability of test results in statistics
* Support `PACK_KEYS` syntax for the `CREATE TABLE` statement
* Add `row_id` column for the null pushdown schema to optimize performance
### Bug Fixes
* Fix the resource leak issue in statistics
* Fix the goroutine leak issue
* Fix an issue about `IndexJoin`

## [1.1.0-alpha] - 2018-01-19
### New Features
* Support the PROXY protocol
### Improvements
* Support more syntax
* Reduce memory usage of statistics info using more compact structure
* Speed up loading statistics info when starting tidb-server
* Provide more accurate query cost evaluation
* Use `Count-Min Sketch` to estimate the cost of queries using unique index more accurately
* Support more complex conditions to make full use of index
* Refactor all executor operators using Chunk architecture, improve the execution performance of analytical statements and reduce memory usage
* Optimize performance of the `INSERT IGNORE` statement
* Push down more types and functions to TiKV
* Support more `SQL_MODE`
* Optimize the `Load Data` performance to increase the speed by 10 times
* Optimize the `Use Database` performance
* Support statistics on the memory usage of physical operators
