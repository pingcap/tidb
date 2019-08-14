# TiDB Changelog
All notable changes to this project will be documented in this file. See also [Release Notes](https://github.com/pingcap/docs/blob/master/releases/rn.md), [TiKV Changelog](https://github.com/tikv/tikv/blob/master/CHANGELOG.md) and [PD Changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [2.1.14] 2019-07-04
* Fix wrong query results caused by column pruning in some cases [#11019](https://github.com/pingcap/tidb/pull/11019)
* Fix the wrongly displayed information in `db` and `info` columns of `show processlist` [#11000](https://github.com/pingcap/tidb/pull/11000)
* Fix the issue that `MAX_EXECUTION_TIME` as a SQL hint and global variable does not work in some cases [#10999](https://github.com/pingcap/tidb/pull/10999)
* Support automatically adjust the incremental step allocated by auto-increment ID based on the load [#10997](https://github.com/pingcap/tidb/pull/10997)
* Fix the issue that the `Distsql` memory information of `MemTracker` is not correctly cleaned when a query ends [#10971](https://github.com/pingcap/tidb/pull/10971)
* Add the `MEM` column in the `information_schema.processlist` table to describe the memory usage of a query [#10896](https://github.com/pingcap/tidb/pull/10896)
* Add the `max_execution_time` global system variable to control the maximum execution time of a query [#10940](https://github.com/pingcap/tidb/pull/10940)
* Fix the panic caused by using unsupported aggregate functions [#10911](https://github.com/pingcap/tidb/pull/10911)
* Add an automatic rollback feature for the last transaction when the `load data` statement fails [#10862](https://github.com/pingcap/tidb/pull/10862)
* Fix the issue that TiDB returns a wrong result in some cases when the `OOMAction` configuration item is set to `Cancel` [#11016](https://github.com/pingcap/tidb/pull/11016)
* Disable the `TRACE` statement to avoid the TiDB panic issue [#11039](https://github.com/pingcap/tidb/pull/11039)
* Add the `mysql.expr_pushdown_blacklist` system table that dynamically enables/disables pushing down specific functions to Coprocessor [#10998](https://github.com/pingcap/tidb/pull/10998)
* Fix the issue that the `ANY_VALUE` function does not work in the `ONLY_FULL_GROUP_BY` mode [#10994](https://github.com/pingcap/tidb/pull/10994)
* Fix the incorrect evaluation caused by not doing a deep copy when evaluating the user variable of the string type [#11043](https://github.com/pingcap/tidb/pull/11043)

## [3.0.0] 2019-06-28
## New Features
* Support Window Functions; compatible with all window functions in MySQL 8.0, including `NTILE`, `LEAD`, `LAG`, `PERCENT_RANK`, `NTH_VALUE`, `CUME_DIST`, `FIRST_VALUE` , `LAST_VALUE`, `RANK`, `DENSE_RANK`, and `ROW_NUMBER`
* Support Views (Experimental)
* Improve Table Partition
    - Support Range Partition
    - Support Hash Partition
* Add the plug-in framework, supporting plugins such as IP Whitelist (Enterprise feature) and Audit Log (Enterprise feature).
* Support the SQL Plan Management function to create SQL execution plan binding to ensure query stability (Experimental)

## SQL Optimizer
* Optimize the `NOT EXISTS` subquery and convert it to `Anti Semi Join` to improve performance
* Optimize the constant propagation on the `Outer Join`, and add the optimization rule of `Outer Join` elimination to reduce non-effective computations and improve performance
* Optimize the `IN` subquery to execute `Inner Join` after aggregation to improve performance
* Optimize `Index Join` to adapt to more scenarios
* Improve the Partition Pruning optimization rule of Range Partition 
* Optimize the query logic for `_tidb_rowid`to avoid full table scan and improve performance
* Match more prefix columns of the indexes when extracting access conditions of composite indexes if there are relevant columns in the filter to improve performance
* Improve the accuracy of cost estimates by using order correlation between columns
* Optimize `Join Reorder` based on the Greedy algorithm and the dynamic planning algorithm to improve accuracy for index selection using `Join` 
* Support Skyline Pruning, with some rules to prevent the execution plan from relying too heavily on statistics to improve query stability
* Improve the accuracy of row count estimation for single-column indexes with NULL values 
* Support `FAST ANALYZE` that randomly samples in each Region to avoid full table scan and improve performance with statistics collection 
* Support the incremental Analyze operation on monotonically increasing index columns to improve performance with statistics collection 
* Support using subqueries in the `DO` statement
* Support using `Index Join` in transactions
* Optimize `prepare`/`execute` to support DDL statements with no parameters
* Modify the system behaviour to auto load statistics when the `stats-lease` variable value is 0
* Support exporting historical statistics  
* Support the `dump`/`load` correlation of histograms  

## SQL Execution Engine
* Optimize log output: `EXECUTE` outputs user variables and `COMMIT` outputs slow query logs to facilitate troubleshooting
* Support the `EXPLAIN ANALYZE` function to improve SQL tuning usability
* Support the `admin show next_row_id` command to get the ID of the next row
* Add six built-in functions: `JSON_QUOTE`, `JSON_ARRAY_APPEND`, `JSON_MERGE_PRESERVE`, `BENCHMARK` ,`COALESCE`, and `NAME_CONST`
* Optimize control logics on the chunk size to dynamically adjust based on the query context, to reduce the SQL execution time and resource consumption 
*  Support tracking and controlling memory usage in three operators - `TableReader`, `IndexReader` and `IndexLookupReader` 
* Optimize the Merge Join operator to support an empty `ON` condition 
* Optimize write performance for single tables that contains too many columns 
* Improve the performance of `admin show ddl jobs` by supporting scanning data in reverse order
* Add the `split table region` statement to manually split the table Region to alleviate the hotspot issue
* Add the `split index region` statement to manually split the index Region to alleviate the hotspot issue 
* Add a blacklist to prohibit pushing down expressions to Coprocessor
* Optimize the `Expensive Query` log to print the SQL query in the log when it exceeds the configured limit of execution time or memory

## DDL
* Support migrating from character set `utf8` to `utf8mb4`
* Change the default character set from`utf8` to `utf8mb4` 
* Add the `alter schema` statement to modify the character set and the collation of the database 
* Support ALTER algorithm `INPLACE`/`INSTANT`
* Support `SHOW CREATE VIEW`
* Support `SHOW CREATE USER` 
* Support fast recovery of mistakenly deleted tables
* Support adjusting the number of concurrencies of ADD INDEX dynamically  
* Add the `pre_split_regions` option that pre-allocates Regions when creating the table using the `CREATE TABLE` statement, to relieve write hot Regions caused by lots of writes after the table creation 
* Support splitting Regions by the index and range of the table specified using SQL statements to relieve hotspot issues 
* Add the `ddl_error_count_limit` global variable to limit the number of DDL task retries 
* Add a feature to use `SHARD_ROW_ID_BITS` to scatter row IDs when the column contains an AUTO_INCREMENT attribute to relieve the hotspot issue
* Optimize the lifetime of invalid DDL metadata to speed up recovering the normal execution of DDL operations after upgrading the TiDB cluster 

## Transactions
* Support the pessimistic transaction model (Experimental)  
* Optimize transaction processing logics to adapt to more scenarios: 
    - Change the default value `tidb_disable_txn_auto_retry` to `on`, which means non-auto committed transactions will not be retried   
    - Add the `tidb_batch_commit` system variable to split a transaction into multiple ones to be executed concurrently  
    - Add the `tidb_low_resolution_tso` system variable to control the number of TSOs to obtain in batches and reduce the number of times that transactions request for TSOs,  to improve performance in scenarios with relatively low requirement of consistency 
    - Add the `tidb_skip_isolation_level_check` variable to control whether to report errors when the isolation level is set to SERIALIZABLE 
    - Modify the `tidb_disable_txn_auto_retry` system variable to make it work on all retryable errors  

## Permission Management
* Perform permission check on the `ANALYZE`, `USE`, `SET GLOBAL`, and  `SHOW PROCESSLIST` statements
* Support Role Based Access Control (RBAC) (Experimental)

## Server
* Optimize slow query logs
    - Restructure the log format
    - Optimize the log content 
    - Optimize the log query method to support using the `INFORMATION_SCHEMA.SLOW_QUERY` and `ADMIN SHOW SLOW` statements of the memory table to query slow query logs
* Develop a unified log format specification with restructured log system to facilitate collection and analysis by tools  
* Support using SQL statements to manage Binlog services, including querying status, enabling Binlog, maintaining and sending Binlog strategies.   
* Support using `unix_socket` to connect to the database
* Support `Trace` for SQL statements
* Support getting information for a TiDB instance via the `/debug/zip` HTTP interface to facilitate troubleshooting.
* Optimize monitoring items to facilitate troubleshooting: 
    - Add the `high_error_rate_feedback_total` monitoring item to monitor the difference between the actual data volume and the estimated data volume based on statistics 
    - Add a QPS monitoring item in the database dimension
* Optimize the system initialization process to only allow the DDL owner to perform the initialization. This reduces the startup time for initialization or upgrading. 
* Optimize the execution logic of `kill query` to improve performance and ensure resource is release properly 
* Add a startup option `config-check` to check the validity of the configuration file
* Add the `tidb_back_off_weight` system variable to control the backoff time of internal error retries
* Add the `wait_timeout`and `interactive_timeout` system variables to control the maximum idle connections allowed
* Add the connection pool for TiKV to shorten the connection establishing time

## Compatibility
* Support the `ALLOW_INVALID_DATES` SQL mode
* Support the MySQL 320 Handshake protocol
* Support manifesting unsigned BIGINT columns as auto-increment columns  
* Support the `SHOW CREATE DATABASE IF NOT EXISTS` syntax
* Optimize the fault tolerance of `load data` for CSV files
* Abandon the predicate pushdown operation when the filtering condition contains a user variable to improve the compatibility with MySQL’s behavior of using user variables to simulate Window Functions 


## [3.0.0-rc.3] 2019-06-21
## SQL Optimizer
* Remove the feature of collecting virtual generated column statistics[#10629](https://github.com/pingcap/tidb/pull/10629)
* Fix the issue that the primary key constant overflows during point queries [#10699](https://github.com/pingcap/tidb/pull/10699)
* Fix the issue that using uninitialized information in `fast analyze` causes panic [#10691](https://github.com/pingcap/tidb/pull/10691)
* Fix the issue that executing the `create view` statement using `prepare` causes panic because of wrong column information [#10713](https://github.com/pingcap/tidb/pull/10713)
* Fix the issue that the column information is not cloned when handling window functions [#10720](https://github.com/pingcap/tidb/pull/10720)
* Fix the wrong estimation for the selectivity rate of the inner table selection in index join [#10854](https://github.com/pingcap/tidb/pull/10854)
* Support automatic loading statistics when the `stats-lease` variable value is 0 [#10811](https://github.com/pingcap/tidb/pull/10811)

## Execution Engine
* Fix the issue that resources are not correctly released when calling the `Close` function in `StreamAggExec` [#10636](https://github.com/pingcap/tidb/pull/10636)
* Fix the issue that the order of `table_option` and `partition_options` is incorrect in the result of executing the `show create table` statement for partitioned tables [#10689](https://github.com/pingcap/tidb/pull/10689)
* Improve the performance of `admin show ddl jobs` by supporting scanning data in reverse order [#10687](https://github.com/pingcap/tidb/pull/10687)
* Fix the issue that the result of the `show grants` statement in RBAC is incompatible with that of MySQL when this statement has the `current_user` field [#10684](https://github.com/pingcap/tidb/pull/10684)
* Fix the issue that UUIDs might generate duplicate values ​​on multiple nodes [#10712](https://github.com/pingcap/tidb/pull/10712)
* Fix the issue that the `show view` privilege is not considered in `explain` [#10635](https://github.com/pingcap/tidb/pull/10635) 
* Add the `split table region` statement to manually split the table Region to alleviate the hotspot issue [#10765](https://github.com/pingcap/tidb/pull/10765) 
* Add the `split index region` statement to manually split the index Region to alleviate the hotspot issue [#10764](https://github.com/pingcap/tidb/pull/10764) 
* Fix the incorrect execution issue when you execute multiple statements such as `create user`, `grant`, or `revoke` consecutively [#10737] (https://github.com/pingcap/tidb/pull/10737) 
* Add a blacklist to prohibit pushing down expressions to Coprocessor [#10791](https://github.com/pingcap/tidb/pull/10791) 
* Add the feature of printing the `expensive query` log when a query exceeds the memory configuration limit [#10849](https://github.com/pingcap/tidb/pull/10849) 
* Add the `bind-info-lease` configuration item to control the update time of the modified binding execution plan [#10727](https://github.com/pingcap/tidb/pull/10727) 
* Fix the OOM issue in high concurrent scenarios caused by the failure to quickly release Coprocessor resources, resulted from the `execdetails.ExecDetails` pointer [#10832] (https://github.com/pingcap/tidb/pull/10832) 
* Fix the panic issue caused by the `kill` statement in some cases [#10876](https://github.com/pingcap/tidb/pull/10876) 
## Server
* Fix the issue that goroutine might leak when repairing GC [#10683](https://github.com/pingcap/tidb/pull/10683) 
* Support displaying the `host` information in slow queries [#10693](https://github.com/pingcap/tidb/pull/10693) 
* Support reusing idle links that interact with TiKV [#10632](https://github.com/pingcap/tidb/pull/10632) 
* Fix the support for enabling the `skip-grant-table` option in RBAC [#10738](https://github.com/pingcap/tidb/pull/10738) 
* Fix the issue that `pessimistic-txn` configuration goes invalid [#10825](https://github.com/pingcap/tidb/pull/10825) 
* Fix the issue that the actively cancelled ticlient requests are still retried [#10850](https://github.com/pingcap/tidb/pull/10850) 
* Improve performance in the case where pessimistic transactions conflict with optimistic transactions [#10881](https://github.com/pingcap/tidb/pull/10881) 
## DDL
* Fix the issue that modifying charset using `alter table` causes the `blob` type change [#10698](https://github.com/pingcap/tidb/pull/10698) 
* Add a feature to use `SHARD_ROW_ID_BITS` to scatter row IDs when the column contains an `AUTO_INCREMENT` attribute to alleviate the hotspot issue [#10794](https://github.com/pingcap/tidb/pull/10794) 
* Prohibit adding stored generated columns by using the `alter table` statement [#10808](https://github.com/pingcap/tidb/pull/10808) 
* Optimize the invalid survival time of DDL metadata to shorten the period during which the DDL operation is slower after cluster upgrade [#10795](https://github.com/pingcap/tidb/pull/10795) 

## [2.1.13] 2019-06-21
* Add a feature to use `SHARD_ROW_ID_BITS` to scatter row IDs when the column contains an `AUTO_INCREMENT` attribute to relieve the hotspot issue [#10788](https://github.com/pingcap/tidb/pull/10788)
* Optimize the lifetime of invalid DDL metadata to speed up recovering the normal execution of DDL operations after upgrading the TiDB cluster [#10789](https://github.com/pingcap/tidb/pull/10789)
* Fix the OOM issue in high concurrent scenarios caused by the failure to quickly release Coprocessor resources, resulted from the `execdetails.ExecDetails` pointer [#10833](https://github.com/pingcap/tidb/pull/10833)
* Add the `update-stats` configuration item to control whether to update statistics [#10772](https://github.com/pingcap/tidb/pull/10772)
* Add the following TiDB-specific syntax to support Region presplit to solve the hotspot issue:
  - Add the `PRE_SPLIT_REGIONS` table option  [#10863](https://github.com/pingcap/tidb/pull/10863)
  - Add the `SPLIT TABLE table_name INDEX index_name` syntax [#10865](https://github.com/pingcap/tidb/pull/10865)
  - Add the `SPLIT TABLE [table_name] BETWEEN (min_value...) AND (max_value...) REGIONS [region_num]` syntax [#10882](https://github.com/pingcap/tidb/pull/10882)
* Fix the panic issue caused by the `KILL` syntax in some cases [#10879](https://github.com/pingcap/tidb/pull/10879)
* Improve the compatibility with MySQL for `ADD_DATE` in some cases [#10718](https://github.com/pingcap/tidb/pull/10718)
* Fix the wrong estimation for the selectivity rate of the inner table selection in index join [#10856](https://github.com/pingcap/tidb/pull/10856)

## [2.1.12] 2019-06-13
* Fix the issue caused by unmatched data types when using the index query feedback [#10755](https://github.com/pingcap/tidb/pull/10755)
* Fix the issue that the blob column is changed to the text column caused by charset altering in some cases [#10745](https://github.com/pingcap/tidb/pull/10745)
* Fix the issue that the `GRANT` operation in the transaction mistakenly reports “Duplicate Entry” in some cases [#10739](https://github.com/pingcap/tidb/pull/10739)
* Improve the compatibility with MySQL of the following features
    - The `DAYNAME` function [#10732](https://github.com/pingcap/tidb/pull/10732)
    - The `MONTHNAME` function [#10733](https://github.com/pingcap/tidb/pull/10733)
    - Support the 0 value for the `EXTRACT` function when processing `MONTH` [#10702](https://github.com/pingcap/tidb/pull/10702)
    - The `DECIMAL` type can be converted to `TIMESTAMP` or  `DATETIME` [#10734](https://github.com/pingcap/tidb/pull/10734)
* Change the column charset while changing the table charset [#10714](https://github.com/pingcap/tidb/pull/10714)
* Fix the overflow issue when converting a decimal to a float in some cases [#10730](https://github.com/pingcap/tidb/pull/10730)
* Fix the issue that some extremely large messages report the “grpc: received message larger than max” error caused by inconsistent maximum sizes of messages sent/received by gRPC of TiDB and TiKV [#10710](https://github.com/pingcap/tidb/pull/10710)
* Fix the panic issue caused by `ORDER BY` not filtering NULL in some cases [#10488](https://github.com/pingcap/tidb/pull/10488)
* Fix the issue that values returned by the `UUID` function might be duplicate when multiple nodes exist [#10711](https://github.com/pingcap/tidb/pull/10711) 
* Change the value returned by `CAST(-num as datetime)` from `error` to NULL [#10703](https://github.com/pingcap/tidb/pull/10703) 
* Fix the issue that an unsigned histogram meets signed ranges in some cases [#10695](https://github.com/pingcap/tidb/pull/10695)
* Fix the issue that an error is reported mistakenly for reading data when the statistics feedback meets the bigint unsigned primary key [#10307](https://github.com/pingcap/tidb/pull/10307)
* Fix the issue that the result of `Show Create Table` for partitioned tables is not correctly displayed in some cases [#10690](https://github.com/pingcap/tidb/pull/10690)
* Fix the issue that the calculation result of the `GROUP_CONCAT` aggregate function is not correct for some correlated subqueries [#10670](https://github.com/pingcap/tidb/pull/10670)
* Fix the issue that the result of `Show Create Table` for partitioned tables is not correctly displayed in some cases [#10690](https://github.com/pingcap/tidb/pull/10690)

## [2.1.11] 2019-05-31

* Fix the issue that incorrect schema is used for `delete from join` [#10595](https://github.com/pingcap/tidb/pull/10595)
* Fix the issue that the built-in `CONVERT()` may return incorrect field type [#10263](https://github.com/pingcap/tidb/pull/10263)
* Merge non-overlapped feedback when updating bucket count [#10569](https://github.com/pingcap/tidb/pull/10569)
* Fix calculation errors of `unix_timestamp()-unix_timestamp(now())` [#10491](https://github.com/pingcap/tidb/pull/10491)
* Fix the incompatibility issue of `period_diff` with MySQL 8.0 [#10501](https://github.com/pingcap/tidb/pull/10501)
* Skip `Virtual Column` when collecting statistics to avoid exceptions [#10628](https://github.com/pingcap/tidb/pull/10628)
* Support the `SHOW OPEN TABLES` statement [#10374](https://github.com/pingcap/tidb/pull/10374)
* Fix the issue that goroutine leak may happen in some cases [#10656](https://github.com/pingcap/tidb/pull/10656)
* Fix the issue that setting the `tidb_snapshot` variable in some cases may cause incorrect parsing of time format [#10637](https://github.com/pingcap/tidb/pull/10637)

## [3.0.0-rc.2] 2019-05-28
### SQL Optimizer
* Support Index Join in more scenarios
[#10540](https://github.com/pingcap/tidb/pull/10540)
* Support exporting historical statistics [#10291](https://github.com/pingcap/tidb/pull/10291)
* Support the incremental `Analyze` operation on monotonically increasing index columns 
[#10355](https://github.com/pingcap/tidb/pull/10355)
* Neglect the NULL value in the `Order By` clause [#10488](https://github.com/pingcap/tidb/pull/10488)
* Fix the wrong schema information calculation of the `UnionAll` logical operator when simplifying the column information [#10384](https://github.com/pingcap/tidb/pull/10384)
* Avoid modifying the original expression when pushing down the `Not` operator [#10363](https://github.com/pingcap/tidb/pull/10363/files)
* Support the `dump`/`load` correlation of histograms [#10573](https://github.com/pingcap/tidb/pull/10573)
### Execution Engine
* Handle virtual columns with a unique index properly when fetching duplicate rows in `batchChecker` [#10370](https://github.com/pingcap/tidb/pull/10370)
* Fix the scanning range calculation issue for the `CHAR` column [#10124](https://github.com/pingcap/tidb/pull/10124)
* Fix the issue of `PointGet` incorrectly processing negative numbers [#10113](https://github.com/pingcap/tidb/pull/10113)
* Merge `Window` functions with the same name to improve execution efficiency [#9866](https://github.com/pingcap/tidb/pull/9866)
* Allow the `RANGE` frame in a `Window` function to contain no `OrderBy` clause [#10496](https://github.com/pingcap/tidb/pull/10496)

### Server
Fix the issue that TiDB continuously creates a new connection to TiKV when a fault occurs in TiKV [#10301](https://github.com/pingcap/tidb/pull/10301)
Make `tidb_disable_txn_auto_retry` affect all retryable errors instead of only write conflict errors [#10339](https://github.com/pingcap/tidb/pull/10339)
Allow DDL statements without parameters to be executed using `prepare`/`execute` [#10144](https://github.com/pingcap/tidb/pull/10144)
Add the `tidb_back_off_weight` variable to control the backoff time [#10266](https://github.com/pingcap/tidb/pull/10266)
Prohibit TiDB retrying non-automatically committed transactions in default conditions by setting the default value of `tidb_disable_txn_auto_retry` to `on` [#10266](https://github.com/pingcap/tidb/pull/10266)
Fix the database privilege judgment of `role` in `RBAC` [#10261](https://github.com/pingcap/tidb/pull/10261)
Support the pessimistic transaction model (experimental) [#10297](https://github.com/pingcap/tidb/pull/10297)
Reduce the wait time for handling lock conflicts in some cases [#10006](https://github.com/pingcap/tidb/pull/10006)
Make the Region cache able to visit follower nodes when a fault occurs in the leader node [#10256](https://github.com/pingcap/tidb/pull/10256)
Add the `tidb_low_resolution_tso` variable to control the number of TSOs obtained in batches and reduce the times of transactions obtaining TSO to adapt for scenarios where data consistency is not so strictly required [#10428](https://github.com/pingcap/tidb/pull/10428)

### DDL
Fix the uppercase issue of the charset name in the storage of the old version of TiDB
[#10272](https://github.com/pingcap/tidb/pull/10272)
Support `preSplit` of table partition, which pre-allocates table Regions when creating a table to avoid write hotspots after the table is created
[#10221](https://github.com/pingcap/tidb/pull/10221)
Fix the issue that TiDB incorrectly updates the version information in PD in some cases [#10324](https://github.com/pingcap/tidb/pull/10324)
Support modifying the charset and collation using the `ALTER DATABASE` statement
[#10393](https://github.com/pingcap/tidb/pull/10393)
Support splitting Regions based on the index and range of the specified table  to relieve hotspot issues
[#10203](https://github.com/pingcap/tidb/pull/10203)
Prohibit modifying the precision of the decimal column using the `alter table` statement
[#10433](https://github.com/pingcap/tidb/pull/10433)
Fix the restriction for expressions and functions in hash partition
[#10273](https://github.com/pingcap/tidb/pull/10273)
Fix the issue that adding indexes in a table that contains partitions will in some cases cause TiDB panic
[#10475](https://github.com/pingcap/tidb/pull/10475)
Validate table information before executing the DDL to avoid invalid table schemas
[#10464](https://github.com/pingcap/tidb/pull/10464)
Enable hash partition by default; and enable range columns partition when there is only one column in the partition definition
[#9936](https://github.com/pingcap/tidb/pull/9936)


## [2.1.10] 2019-05-21
* Fix the issue that some abnormalities cause incorrect table schema when using `tidb_snapshot` to read the history data [#10359](https://github.com/pingcap/tidb/pull/10359)
* Fix the issue that the `NOT` function causes wrong read results in some cases [#10363](https://github.com/pingcap/tidb/pull/10363)
* Fix the wrong behavior of `Generated Column` in the `Replace` or `Insert on duplicate update` statement [#10385](https://github.com/pingcap/tidb/pull/10385)
* Fix a bug of the `BETWEEN` function in the `DATE`/`DATETIME` comparison [#10407](https://github.com/pingcap/tidb/pull/10407)
* Fix the issue that a single line of a slow log that is too long causes an error report when using the `SLOW_QUERY` table to query a slow log [#10412](https://github.com/pingcap/tidb/pull/10412)
* Fix the issue that the result of `DATETIME ` plus `INTERVAL` is not the same with that of MySQL in some cases [#10416](https://github.com/pingcap/tidb/pull/10416), [#10418](https://github.com/pingcap/tidb/pull/10418)
* Add the check for the invalid time of February in a leap year [#10417](https://github.com/pingcap/tidb/pull/10417)
* Execute the internal initialization operation limitation only in the DDL owner to avoid a large number of conflict error reports when initializing the cluster [#10426](https://github.com/pingcap/tidb/pull/10426)
* Fix the issue that `DESC` is incompatible with MySQL when the default value of the output timestamp column is `default current_timestamp on update current_timestamp` [#10337](https://github.com/pingcap/tidb/issues/10337) 
* Fix the issue that an error occurs during the privilege check in the `Update` statement [#10439](https://github.com/pingcap/tidb/pull/10439)
* Fix the issue that wrong calculation of `RANGE` causes a wrong result in the `CHAR` column in some cases [#10455](https://github.com/pingcap/tidb/pull/10455)
* Fix the issue that the data might be overwritten after decreasing `SHARD_ROW_ID_BITS` [#9868](https://github.com/pingcap/tidb/pull/9868)
* Fix the issue that `ORDER BY RAND()` does not return random numbers [#10064](https://github.com/pingcap/tidb/pull/10064)
* Prohibit the `ALTER` statement modifying the precision of decimals [#10458](https://github.com/pingcap/tidb/pull/10458) 
* Fix the compatibility issue of the `TIME_FORMAT` function with MySQL [#10474](https://github.com/pingcap/tidb/pull/10474)
* Check the parameter validity of `PERIOD_ADD` [#10430](https://github.com/pingcap/tidb/pull/10430)
* Fix the issue that the behavior of the invalid `YEAR` string in TiDB is incompatible with that in MySQL [#10493](https://github.com/pingcap/tidb/pull/10493)
* Support the `ALTER DATABASE` syntax [#10503](https://github.com/pingcap/tidb/pull/10503)
* Fix the issue that the `SLOW_QUERY` memory engine reports an error when no  `;` exists in the slow query statement [#10536](https://github.com/pingcap/tidb/pull/10536)
* Fix the issue that the `Add index` operation in partitioned tables cannot be canceled in some cases [#10533](https://github.com/pingcap/tidb/pull/10533)
* Fix the issue that the OOM panic cannot be recovered in some cases [#10545](https://github.com/pingcap/tidb/pull/10545)
* Improve the security of the DDL operation rewriting the table metadata [#10547](https://github.com/pingcap/tidb/pull/10547)

## [3.0.0-rc.1] 2019-05-10

### SQL Optimizer
* Improve the accuracy of cost estimates by using order correlation between columns; introduce a heuristic parameter `tidb_opt_correlation_exp_factor` to control the preference for index scans for scenarios when correlation cannot be directly used for estimation. [#9839](https://github.com/pingcap/tidb/pull/9839)
* Match more prefix columns of the indexes when extracting access conditions of composite indexes if there are relevant columns in the filter [#10053](https://github.com/pingcap/tidb/pull/10053)
* Use the dynamic programming algorithm to specify the execution order of join operations when the number of tables participating in the join is less than the value of `tidb_opt_join_reorder_threshold`. [#8816](https://github.com/pingcap/tidb/pull/8816)
* Match more prefix columns of the indexes in the inner tables that build the index join when using composite indexes as the access conditions [#8471](https://github.com/pingcap/tidb/pull/8471)
* Improve the accuracy of row count estimation for single-column indexes with NULL values [#9474](https://github.com/pingcap/tidb/pull/9474)
* Specially handle `GROUP_CONCAT` when eliminating aggregate functions during the logical optimization phase to prevent incorrect executions [#9967](https://github.com/pingcap/tidb/pull/9967)
* Properly push the filter down to child nodes of the join operator if the filter is a constant [#9848](https://github.com/pingcap/tidb/pull/9848)
* Specially handle some functions such as `RAND()` when pruning columns during the logical optimization phase to prevent incompatibilities with MySQL [#10064](https://github.com/pingcap/tidb/pull/10064)
* Support `FAST ANALYZE`, which speeds up statistics collection by sampling the region instead of scanning the entire region. This feature is controlled by the variable `tidb_enable_fast_analyze`. [#10258](https://github.com/pingcap/tidb/pull/10258)
* Support SQL Plan Management, which ensures execution stability by performing execution plan binding for SQL statements. This feature is currently in beta and only supports bound execution plans for SELECT statements. It is not recommended to use it in the production environment. [#10284](https://github.com/pingcap/tidb/pull/10284)
### Execution Engine
* Support tracking and controlling memory usage in three operators - `TableReader`, `IndexReader` and `IndexLookupReader`
[#10003](https://github.com/pingcap/tidb/pull/10003)
* Support showing more information about coprocessor tasks in the slow log such as the number of tasks in coprocessor, the average/longest/90% of execution/waiting time and the addresses of the TiKVs which take the longest execution time or waiting time [#10165](https://github.com/pingcap/tidb/pull/10165)
* Support the prepared DDL statements with no placeholders [#10144](https://github.com/pingcap/tidb/pull/10144)
### Server
* Only allow the DDL owner to execute bootstrap when TiDB is started [#10029](https://github.com/pingcap/tidb/pull/10029)
* Add the variable `tidb_skip_isolation_level_check` to prevent TiDB from reporting errors when setting the transaction isolation level to SERIALIZABLE [#10065](https://github.com/pingcap/tidb/pull/10065)
* Merge the implicit commit time and the SQL execution time in the slow log [#10294](https://github.com/pingcap/tidb/pull/10294)
* Support for SQL Roles (RBAC Privilege Management)
    - Support `SHOW GRANT` [#10016](https://github.com/pingcap/tidb/pull/10016)
    - Support `SET DEFAULT ROLE` [#9949](https://github.com/pingcap/tidb/pull/9949)
    - Support `GRANT ROLE` [#9721](https://github.com/pingcap/tidb/pull/9721)
* Fix the `ConnectionEvent` error from the `whitelist` plugin that makes TiDB exit [#9889](https://github.com/pingcap/tidb/pull/9889)
* Fix the issue of mistakenly adding read-only statements to the transaction history [#9723](https://github.com/pingcap/tidb/pull/9723)
* Improve `kill` statements to stop SQL execution  and release resources more quickly [#9844](https://github.com/pingcap/tidb/pull/9844)
* Add a startup option `config-check` to check the validity of the configuration file [#9855](https://github.com/pingcap/tidb/pull/9855)
* Fix the validity check of inserting NULL fields when the strict SQL mode is disabled [#10161](https://github.com/pingcap/tidb/pull/10161)
### DDL
* Add the `pre_split_regions` option for `CREATE TABLE` statements; this option supports pre-splitting the Table Region when creating a table to avoid write hot spots caused by lots of writes after the table creation [#10138](https://github.com/pingcap/tidb/pull/10138)
* Optimize the execution performance of some DDL statements [#10170](https://github.com/pingcap/tidb/pull/10170)
* Add the warning that full-text indexes are not supported for `FULLTEXT KEY` [#9821](https://github.com/pingcap/tidb/pull/9821)
* Fix the compatibility issue for the UTF8 and UTF8MB4 charsets in the old versions of TiDB [#9820](https://github.com/pingcap/tidb/pull/9820)
* Fix the potential bug in `shard_row_id_bits` of a table [#9868](https://github.com/pingcap/tidb/pull/9868)
* Fix the bug that the column charset is not changed after the table charset is changed [#9790](https://github.com/pingcap/tidb/pull/9790)
* Fix a potential bug in `SHOW COLUMN` when using `BINARY`/`BIT` as the column default value [#9897](https://github.com/pingcap/tidb/pull/9897)
* Fix the compatibility issue in displaying `CHARSET`/`COLLATION` descriptions in the `SHOW FULL COLUMNS` statement [#10007](https://github.com/pingcap/tidb/pull/10007)
* Fix the issue that the `SHOW COLLATIONS` statement only lists collations supported by TiDB [#10186](https://github.com/pingcap/tidb/pull/10186)


## [2.1.9] 2019-05-06
* Fix compatibility of the `MAKETIME` function when unsigned type overflows [#10089](https://github.com/pingcap/tidb/pull/10089)
* Fix the stack overflow caused by constant folding in some cases [#10189](https://github.com/pingcap/tidb/pull/10189)
* Fix the privilege check issue for `Update` when an alias exists in some cases [#10157](https://github.com/pingcap/tidb/pull/10157), [#10326](https://github.com/pingcap/tidb/pull/10326)
* Track and control memory usage in DistSQL [#10197](https://github.com/pingcap/tidb/pull/10197)
* Support specifying collation as `utf8mb4_0900_ai_ci` [#10201](https://github.com/pingcap/tidb/pull/10201)
* Fix the wrong result issue of the `MAX` function when the primary key is of the Unsigned type [#10209](https://github.com/pingcap/tidb/pull/10209)
* Fix the issue that NULL values can be inserted into NOT NULL columns in the non-strict SQL mode [#10254](https://github.com/pingcap/tidb/pull/10254)
* Fix the wrong result issue of the `COUNT` function when multiple columns exist in `DISTINCT` [#10270](https://github.com/pingcap/tidb/pull/10270)
* Fix the panic issue occurred when `LOAD DATA` parses irregular CSV files [#10269](https://github.com/pingcap/tidb/pull/10269)
* Ignore the overflow error when the outer and inner join key types are inconsistent in `Index Lookup Join` [#10244](https://github.com/pingcap/tidb/pull/10244)
* Fix the issue that a statement is wrongly judged as point-get in some cases [#10299](https://github.com/pingcap/tidb/pull/10299)
* Fix the wrong result issue when the time type does not convert the time zone in some cases [#10345](https://github.com/pingcap/tidb/pull/10345)
* Fix the issue that TiDB character set cases are inconsistent in some cases [#10354](https://github.com/pingcap/tidb/pull/10354)
* Support controlling the number of rows returned by operator [#9166](https://github.com/pingcap/tidb/issues/9166)
    - `Selection` & `Projection` [#10110](https://github.com/pingcap/tidb/pull/10110)
    - `StreamAgg` & `HashAgg` [#10133](https://github.com/pingcap/tidb/pull/10133)
    - `TableReader` & `IndexReader` & `IndexLookup` [#10169](https://github.com/pingcap/tidb/pull/10169)
* Improve the slow query log:
    - Add `SQL Digest` to distinguish similar SQL [#10093](https://github.com/pingcap/tidb/pull/10093)
    - Add version information of statistics used by slow query statements [#10220](https://github.com/pingcap/tidb/pull/10220)
    - Show memory consumption of a statement in slow query log [#10246](https://github.com/pingcap/tidb/pull/10246)
    - Adjust the output format of Coprocessor related information so it can be parsed by pt-query-digest [#10300](https://github.com/pingcap/tidb/pull/10300)
    - Fix the `#` character issue in slow query statements [#10275](https://github.com/pingcap/tidb/pull/10275)
    - Add some information columns to the memory table of slow query statements  [#10317](https://github.com/pingcap/tidb/pull/10317)
    - Add the transaction commit time to slow query log [#10310](https://github.com/pingcap/tidb/pull/10310)
    - Fix the issue some time formats cannot be parsed by pt-query-digest [#10323](https://github.com/pingcap/tidb/pull/10323) 

## [2.1.8] 2019-04-12

* Fix the issue that the processing logic of `GROUP_CONCAT` function is incompatible with MySQL when there is a NULL-valued parameter [#9930](https://github.com/pingcap/tidb/pull/9930)
* Fix the equality check issue of decimal values in the `Distinct` mode [#9931](https://github.com/pingcap/tidb/pull/9931)
* Fix the collation compatibility issue of the date, datetime, and timestamp types for the `SHOW FULL COLUMNS` statement 
    - [#9938](https://github.com/pingcap/tidb/pull/9938)
    - [#10114](https://github.com/pingcap/tidb/pull/10114)
* Fix the issue that the row count estimation is inaccurate when the filtering condition contains correlated columns [#9937](https://github.com/pingcap/tidb/pull/9937)
* Fix the compatibility issue between the `DATE_ADD` and `DATE_SUB` functions
    - [#9963](https://github.com/pingcap/tidb/pull/9963)
    - [#9966](https://github.com/pingcap/tidb/pull/9966)
* Support the `%H` format for the `STR_TO_DATE` function to improve compatibility [#9964](https://github.com/pingcap/tidb/pull/9964)
* Fix the issue that the result is wrong when the `GROUP_CONCAT` function groups by a unique index [#9969](https://github.com/pingcap/tidb/pull/9969)
* Return a warning when the Optimizer Hints contains an unmatched table name [#9970](https://github.com/pingcap/tidb/pull/9970)
* Unify the log format to facilitate collecting logs using tools for analysis Unified Log Format
* Fix the issue that a lot of NULL values cause inaccurate statistics estimation [#9979](https://github.com/pingcap/tidb/pull/9979)
* Fix the issue that an error is reported when the default value of the TIMESTAMP type is the boundary value [#9987](https://github.com/pingcap/tidb/pull/9987)
* Validate the value of `time_zone` [#10000](https://github.com/pingcap/tidb/pull/10000)  
* Support the `2019.01.01` time format [#10001](https://github.com/pingcap/tidb/pull/10001)
* Fix the issue that the row count estimation is displayed incorrectly in the result returned by the `EXPLAIN` statement in some cases [#10044](https://github.com/pingcap/tidb/pull/10044)
* Fix the issue that `KILL TIDB [session id]` cannot instantly stop the execution of a statement in some cases [#9976](https://github.com/pingcap/tidb/pull/9976)
* Fix the predicate pushdown issue of constant filtering conditions in some cases [#10049](https://github.com/pingcap/tidb/pull/10049)
* Fix the issue that a read-only statement is not processed correctly in some cases [#10048](https://github.com/pingcap/tidb/pull/10048)

## [2.1.7] 2019-03-27

* Fix the issue of longer startup time when upgrading the program caused by canceling DDL operations [#9768](https://github.com/pingcap/tidb/pull/9768)
* Fix the issue that the `check-mb4-value-in-utf8` configuration item is in the wrong position in the `config.example.toml` file [#9852](https://github.com/pingcap/tidb/pull/9852)
* Improve the compatibility of the `str_to_date` built-in function with MySQL [#9817](https://github.com/pingcap/tidb/pull/9817)
* Fix the compatibility issue of the `last_day` built-in function [#9750](https://github.com/pingcap/tidb/pull/9750)
* Add the `tidb_table_id` column for `infoschema.tables` to facilitate getting `table_id` by using SQL statements and add the `tidb_indexes` system table to manage the relationship between Table and Index [#9862](https://github.com/pingcap/tidb/pull/9862)
* Add a check about the null definition of Table Partition [#9663](https://github.com/pingcap/tidb/pull/9663)
* Change the privileges required by `Truncate Table` from `Delete` to `Drop` to make it consistent with MySQL [#9876](https://github.com/pingcap/tidb/pull/9876)
* Support using subqueries in the `DO` statement [#9877](https://github.com/pingcap/tidb/pull/9877) 
* Fix the issue that the `default_week_format` variable does not take effect in the `week` function [#9753](https://github.com/pingcap/tidb/pull/9753) 
* Support the plugin framework [#9880](https://github.com/pingcap/tidb/pull/9880), [#9888](https://github.com/pingcap/tidb/pull/9888) 
* Support checking the enabling state of binlog by using the `log_bin` system variable [#9634](https://github.com/pingcap/tidb/pull/9634) 
* Support checking the Pump/Drainer status by using SQL statements [#9896](https://github.com/pingcap/tidb/pull/9896) 
* Fix the compatibility issue about checking mb4 character on utf8 when upgrading TiDB [#9887](https://github.com/pingcap/tidb/pull/9887) 

## [3.0.0-beta.1] - 2019-03-26

### SQL Optimizer
* Support calculating the Cartesian product by using `Sort Merge Join` [#9032](https://github.com/pingcap/tidb/pull/9037)
* Support Skyline Pruning, with some rules to prevent the execution plan from relying too heavily on statistics [#9337](https://github.com/pingcap/tidb/pull/9337)
* Support Window Functions 
    - `NTILE` [#9682](https://github.com/pingcap/tidb/pull/9682)
    - `LEAD` and `LAG` [#9672](https://github.com/pingcap/tidb/pull/9672)
    - `PERCENT_RANK` [#9671](https://github.com/pingcap/tidb/pull/9671)
    - `NTH_VALUE` [#9596](https://github.com/pingcap/tidb/pull/9596)
    - `CUME_DIST` [#9619](https://github.com/pingcap/tidb/pull/9619)
    - `FIRST_VALUE` and `LAST_VALUE` [#9560](https://github.com/pingcap/tidb/pull/9560)
    - `RANK` and `DENSE_RANK` [#9500](https://github.com/pingcap/tidb/pull/9500)
    - `RANGE FRAMED` [#9450](https://github.com/pingcap/tidb/pull/9450)
    - `ROW FRAMED` [#9358](https://github.com/pingcap/tidb/pull/9358)
    - `ROW NUMBER` [#9098](https://github.com/pingcap/tidb/pull/9098)
* Add a type of statistic that indicates the order correlation between columns and the handle column [#9315](https://github.com/pingcap/tidb/pull/9315) 
### SQL Execution Engine
* Add built-in functions:
    - `JSON_QUOTE` [#7832](https://github.com/pingcap/tidb/pull/7832)
    - `JSON_ARRAY_APPEND` [#9609](https://github.com/pingcap/tidb/pull/9609)
    - `JSON_MERGE_PRESERVE` [#8931](https://github.com/pingcap/tidb/pull/8931)
    - `BENCHMARK` [#9252](https://github.com/pingcap/tidb/pull/9252)
    - `COALESCE` [#9087](https://github.com/pingcap/tidb/pull/9087)
    - `NAME_CONST` [#9261](https://github.com/pingcap/tidb/pull/9261)
* Optimize the Chunk size based on the query context, to reduce the execution time of SQL statements and resources consumption of the cluster [#6489](https://github.com/pingcap/tidb/issues/6489)
### Privilege management
* Support `SET ROLE` and `CURRENT_ROLE` [#9581](https://github.com/pingcap/tidb/pull/9581)
* Support `DROP ROLE` [#9616](https://github.com/pingcap/tidb/pull/9616)
* Support `CREATE ROLE` [#9461](https://github.com/pingcap/tidb/pull/9461)
### Server
* Add the `/debug/zip` HTTP interface to get information of the current TiDB instance [#9651](https://github.com/pingcap/tidb/pull/9651)
* Support the `show pump status` and `show drainer status` SQL statements to check the Pump or Drainer status [9456](https://github.com/pingcap/tidb/pull/9456)
* Support modifying the Pump or Drainer status by using SQL statements [#9789](https://github.com/pingcap/tidb/pull/9789)
* Support adding HASH fingerprints to SQL text for easy tracking of slow SQL statements [#9662](https://github.com/pingcap/tidb/pull/9662)
* Add the `log_bin` system variable (“0” by default) to control the enabling state of binlog; only support checking the state currently [#9343](https://github.com/pingcap/tidb/pull/9343)
* Support managing the sending binlog strategy by using the configuration file [#9864](https://github.com/pingcap/tidb/pull/9864)
* Support querying the slow log by using the `INFORMATION_SCHEMA.SLOW_QUERY` memory table [#9290](https://github.com/pingcap/tidb/pull/9290)
* Change the MySQL version displayed in TiDB from 5.7.10 to 5.7.25 [#9553](https://github.com/pingcap/tidb/pull/9553) 
* Unify the log format for easy collection and analysis by tools
* Add the `high_error_rate_feedback_total` monitoring item to record the difference between the actual data volume and the estimated data volume based on statistics [#9209](https://github.com/pingcap/tidb/pull/9209)
* Add the QPS monitoring item in the database dimension, which can be enabled by using a configuration item [#9151](https://github.com/pingcap/tidb/pull/9151)
### DDL
* Add the `ddl_error_count_limit` global variable (“512” by default) to limit the number of DDL task retries (If this number exceeds the limit, the DDL task is canceled) [#9295](https://github.com/pingcap/tidb/pull/9295)
* Support ALTER ALGORITHM `INPLACE`/`INSTANT` [#8811](https://github.com/pingcap/tidb/pull/8811)
* Support the `SHOW CREATE VIEW` statement [#9309](https://github.com/pingcap/tidb/pull/9309)
* Support the `SHOW CREATE USER` statement [#9240](https://github.com/pingcap/tidb/pull/9240)

## [2.1.6] 2019-03-15

### SQL Optimizer/Executor
* Optimize planner to select the outer table based on cost when both tables are specified in Hint of `TIDB_INLJ` [#9615](https://github.com/pingcap/tidb/pull/9615)
* Fix the issue that `IndexScan` cannot be selected correctly in some cases
[#9587](https://github.com/pingcap/tidb/pull/9587)
* Fix incompatibility with MySQL of check in the `agg` function in subqueries [#9551](https://github.com/pingcap/tidb/pull/9551)
* Make `show stats_histograms` only output valid columns to avoid panics
 [#9502](https://github.com/pingcap/tidb/pull/9502)
### Server
* Support the `log_bin` variable to enable/disable Binlog [#9634](https://github.com/pingcap/tidb/pull/9634)
* Add a sanity check for transactions to avoid false transaction commit [#9559](https://github.com/pingcap/tidb/pull/9559)
* Fix the issue that setting variables may lead to panic [#9539](https://github.com/pingcap/tidb/pull/9539)

### DDL
* Fix the issue that the `Create Table Like` statement causes panic in some cases [#9652](https://github.com/pingcap/tidb/pull/9652)
* Enable the `AutoSync` feature of etcd clients to avoid connection issues between TiDB and etcd in some cases [#9600](https://github.com/pingcap/tidb/pull/9600)


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

## [3.0.0-beta] - 2019-01-18

### New Features

* Support View
* Support Window Function
* Support Range Partition
* Support Hash Partition

### SQL Optimizer

* Re-support the optimization rule of `AggregationElimination` [#7676](https://github.com/pingcap/tidb/pull/7676)
* Optimize the `NOT EXISTS` subquery and convert it to Anti Semi Join [#7842](https://github.com/pingcap/tidb/pull/7842)
* Add the `tidb_enable_cascades_planner` variable to support the new Cascades optimizer. Currently, the Cascades optimizer is not yet fully implemented and is turned off by default [#7879](https://github.com/pingcap/tidb/pull/7879)
* Support using Index Join in transactions [#7877](https://github.com/pingcap/tidb/pull/7877)
* Optimize the constant propagation on the Outer Join, so that the filtering conditions related to the Outer table in the Join result can be pushed down through the Outer Join to the Outer table, reducing the useless calculation of the Outer Join and improving the execution performance [#7794](https://github.com/pingcap/tidb/pull/7794)
* Adjust the optimization rule of Projection Elimination to the position after the Aggregation Elimination, to avoid redundant `Project` operators [#7909](https://github.com/pingcap/tidb/pull/7909)
* Optimize the `IFNULL` function and eliminate this function when the input parameter has a non-NULL attribute [#7924](https://github.com/pingcap/tidb/pull/7924)
* Support building range for `_tidb_rowid`, to avoid full table scan and reduce cluster stress [#8047](https://github.com/pingcap/tidb/pull/8047)
* Optimize the `IN` subquery to do the Inner Join after the aggregation, and add the `tidb_opt_insubq_to_join_and_agg` variable to control whether to enable this optimization rule and open it by default [#7531](https://github.com/pingcap/tidb/pull/7531)
* Support using subqueries in the `DO` statement [#8343](https://github.com/pingcap/tidb/pull/8343)
* Add the optimization rule of Outer Join elimination to reduce unnecessary table scan and Join operations and improve execution performance [#8021](https://github.com/pingcap/tidb/pull/8021)
* Modify the Hint behavior of the `TIDB_INLJ` optimizer, and the optimizer will use the table specified in Hint as the Inner table of Index Join [#8243](https://github.com/pingcap/tidb/pull/8243)
* Use `PointGet` in a wide range so that it can be used when the execution plan cache of the `Prepare` statement takes effect [#8108](https://github.com/pingcap/tidb/pull/8108)
* Introduce the greedy `Join Reorder` algorithm to optimize the join order selection when joining multiple tables [#8394](https://github.com/pingcap/tidb/pull/8394)
* Support View [#8757](https://github.com/pingcap/tidb/pull/8757)
* Support Window Function [#8630](https://github.com/pingcap/tidb/pull/8630)
* Return warning to the client when `TIDB_INLJ` is not in effect, to enhance usability [#9037](https://github.com/pingcap/tidb/pull/9037)
* Support deducing the statistics for filtered data based on filtering conditions and table statistics [#7921](https://github.com/pingcap/tidb/pull/7921)
* Improve the Partition Pruning optimization rule of Range Partition [#8885](https://github.com/pingcap/tidb/pull/8885)

### SQL Executor

* Optimize the `Merge Join` operator to support the empty `ON` condition [#9037](https://github.com/pingcap/tidb/pull/9037) 
* Optimize the log and print the user variables used when executing the `EXECUTE` statement [#7684](https://github.com/pingcap/tidb/pull/7684) 
* Optimize the log to print slow query information for the `COMMIT` statement [#7951](https://github.com/pingcap/tidb/pull/7951) 
* Support the `EXPLAIN ANALYZE` feature to make the SQL tuning process easier [#7827](https://github.com/pingcap/tidb/pull/7827) 
* Optimize the write performance of wide tables with many columns [#7935](https://github.com/pingcap/tidb/pull/7935) 
* Support `admin show next_row_id` [#8242](https://github.com/pingcap/tidb/pull/8242) 
* Add the `tidb_init_chunk_size` variable to control the size of the initial Chunk used by the execution engine [#8480](https://github.com/pingcap/tidb/pull/8480) 
* Improve `shard_row_id_bits` and cross-check the auto-increment ID [#8936](https://github.com/pingcap/tidb/pull/8936) 

### `Prepare` Statement

* Prohibit adding the `Prepare` statement containing subqueries to the query plan cache to guarantee the query plan is correct when different user variables are input [#8064](https://github.com/pingcap/tidb/pull/8064)
* Optimize the query plan cache to guarantee the plan can be cached when the statement contains non-deterministic functions [#8105](https://github.com/pingcap/tidb/pull/8105)
* Optimize the query plan cache to guarantee the query plan of `DELETE`/`UPDATE`/`INSERT` can be cached [#8107](https://github.com/pingcap/tidb/pull/8107)
* Optimize the query plan cache to remove the corresponding plan when executing the `DEALLOCATE` statement [#8332](https://github.com/pingcap/tidb/pull/8332)
* Optimize the query plan cache to avoid the TiDB OOM issue caused by caching too many plans by limiting the memory usage [#8339](https://github.com/pingcap/tidb/pull/8339)
* Optimize the `Prepare` statement to support using the `?` placeholder in the `ORDER BY`/`GROUP BY`/`LIMIT` clause [#8206](https://github.com/pingcap/tidb/pull/8206)

### Privilege Management

* Add the privilege check for the `ANALYZE` statement [#8486](https://github.com/pingcap/tidb/pull/8486)
* Add the privilege check for the `USE` statement [#8414](https://github.com/pingcap/tidb/pull/8418) 
* Add the privilege check for the `SET GLOBAL` statement [#8837](https://github.com/pingcap/tidb/pull/8837)
* Add the privilege check for the `SHOW PROCESSLIST` statement [#7858](https://github.com/pingcap/tidb/pull/7858)

### Server

* Support the `Trace` feature [#9029](https://github.com/pingcap/tidb/pull/9029)
* Support the plugin framework [#8788](https://github.com/pingcap/tidb/pull/8788)
* Support using `unix_socket` and TCP simultaneously to connect to the database [#8836](https://github.com/pingcap/tidb/pull/8836)
* Support the `interactive_timeout` system variable  [#8573](https://github.com/pingcap/tidb/pull/8573)
* Support the `wait_timeout` system variable [#8346](https://github.com/pingcap/tidb/pull/8346)
* Support splitting a transaction into multiple transactions based on the number of statements using the `tidb_batch_commit` variable [#8293](https://github.com/pingcap/tidb/pull/8293)
* Support using the `ADMIN SHOW SLOW` statement to check slow logs [#7785](https://github.com/pingcap/tidb/pull/7785)

### Compatibility

* Support the `ALLOW_INVALID_DATES` SQL mode [#9027](https://github.com/pingcap/tidb/pull/9027)
* Improve `LoadData` fault-tolerance for the CSV file [#9005](https://github.com/pingcap/tidb/pull/9005)
* Support the MySQL 320 handshake protocol [#8812](https://github.com/pingcap/tidb/pull/8812)
* Support using the unsigned bigint column as the auto-increment column [#8181](https://github.com/pingcap/tidb/pull/8181)
* Support the `SHOW CREATE DATABASE IF NOT EXISTS` syntax [#8926](https://github.com/pingcap/tidb/pull/8926)
* Abandon the predicate pushdown operation when the filtering condition contains a user variable to improve the compatibility with MySQL’s behavior of using user variables to mock the Window Function behavior [#8412](https://github.com/pingcap/tidb/pull/8412)

### DDL

* Support fast recovery of mistakenly deleted tables [#7937](https://github.com/pingcap/tidb/pull/7937)
* Support adjusting the number of concurrencies of `ADD INDEX` dynamically [#8295](https://github.com/pingcap/tidb/pull/8295)
* Support changing the character set of tables or columns to `utf8`/`utf8mb4` [#8037](https://github.com/pingcap/tidb/pull/8037)
* Change the default character set from `utf8` to `utf8mb4` [#7965](https://github.com/pingcap/tidb/pull/7965)
* Support Range Partition [#8011](https://github.com/pingcap/tidb/pull/8011)


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
