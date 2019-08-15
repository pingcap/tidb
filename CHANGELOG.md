# TiDB Changelog
All notable changes to this project will be documented in this file. See also [Release Notes](https://github.com/pingcap/docs/blob/master/releases/rn.md), [TiKV Changelog](https://github.com/tikv/tikv/blob/master/CHANGELOG.md) and [PD Changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [2.1.16] 2019-08-15
## SQL Optimizer
* Fix the issue that row count is estimated inaccurately for the equal condition on the time column [#11526](https://github.com/pingcap/tidb/pull/11526)
* Fix the issue that `TIDB_INLJ` Hint does not take effect or take effect on the specified table [#11361](https://github.com/pingcap/tidb/pull/11361)
* Change the implementation of `NOT EXISTS` in a query from OUTER JOIN to ANTI JOIN to find a more optimized execution plan [#11291](https://github.com/pingcap/tidb/pull/11291)
* Support subqueries within `SHOW` statements, allowing syntaxes such as `SHOW COLUMNS FROM tbl WHERE FIELDS IN (SELECT 'a')` [#11461](https://github.com/pingcap/tidb/pull/11461)
* Fix the issue that the `SELECT … CASE WHEN … ELSE NULL ...` query gets an incorrect result caused by the constant folding optimization [#11441](https://github.com/pingcap/tidb/pull/11441)
## SQL Execution Engine
* Fix the issue that the `DATE_ADD` function gets a wrong result when `INTERVAL` is negative [#11616](https://github.com/pingcap/tidb/pull/11616)
* Fix the issue that the `DATE_ADD` function might get an incorrect result because it performs type conversion wrongly when it accepts an argument of the `FLOAT`, `DOUBLE`, or `DECIMAL` type [#11628](https://github.com/pingcap/tidb/pull/11628)
* Fix the issue that the error message is inaccurate when CAST(JSON AS SIGNED) overflows [#11562](https://github.com/pingcap/tidb/pull/11562)
* Fix the issue that other child nodes are not closed when one child node fails to be closed and returns an error during the process of closing Executor [#11598](https://github.com/pingcap/tidb/pull/11598)
* Support `SPLIT TABLE` statements that return the number of Regions that are successfully split and a finished percentage rather than an error when the scheduling is not finished for Region scatter before the timeout [#11487](https://github.com/pingcap/tidb/pull/11487)
* Make `REGEXP BINARY` function case sensitive to be compatible with MySQL [#11505](https://github.com/pingcap/tidb/pull/11505)
* Fix the issue that `NULL` is not returned correctly because the value of `YEAR` in the `DATE_ADD`/`DATE_SUB` result overflows when it is smaller than 0 or larger than 65535 [#11477](https://github.com/pingcap/tidb/pull/11477)
* Add in the slow query table a `Succ` field that indicates whether the execution succeeds [#11412](https://github.com/pingcap/tidb/pull/11421)
* Fix the MySQL incompatibility issue caused by fetching the current timestamp multiple times when a SQL statement involves calculations of the current time (such as `CURRENT_TIMESTAMP` or `NOW`) [#11392](https://github.com/pingcap/tidb/pull/11392)
* Fix the issue that the auto_increment columns do not handle the FLOAT or DOUBLE type [#11389](https://github.com/pingcap/tidb/pull/11389)
* Fix the issue that `NULL` is not returned correctly when the `CONVERT_TZ` function accepts an invalid argument [#11357](https://github.com/pingcap/tidb/pull/11357)
* Fix the issue that an error is reported by the `PARTITION BY LIST` statement. (Currently only the syntax is supported; when TiDB executes the statement, a regular table is created and a prompting message is provided) [#11236](https://github.com/pingcap/tidb/pull/11236)
* Fix the issue that `Mod(%)`, `Multiple(*)`, and `Minus(-)` operations return an inconsistent `0` result with that in MySQL when there are many decimal digits (such as `select 0.000 % 0.11234500000000000000`) [11353](https://github.com/pingcap/tidb/pull/11353)
## Server
* Fix the issue that the plugin gets a `NULL` domain when `OnInit` is called back [#11426](https://github.com/pingcap/tidb/pull/11426)
* Fix the issue that the table information in a schema can still be obtained through the HTTP interface after the schema has been deleted [#11586](https://github.com/pingcap/tidb/pull/11586)
## DDL
* Disallow dropping indexes on auto-increment columns to avoid incorrect results of the auto-increment columns caused by this operation [#11402](https://github.com/pingcap/tidb/pull/11402)
* Fix the issue that the character set of the column is not correct when creating and modifying the table with different character sets and collations [#11423](https://github.com/pingcap/tidb/pull/11423)
* Fix the issue that the column schema might get wrong when `alter table ... set default...` and another DDL statement that modifies this column are executed in parallel [#11374](https://github.com/pingcap/tidb/pull/11374)
* Fix the issue that data fails to be backfilled when Generated Column A depends on Generated Column B and A is used to create an index [#11538](https://github.com/pingcap/tidb/pull/11538)
* Speed up `ADMIN CHECK TABLE` operations [#11538](https://github.com/pingcap/tidb/pull/11676)

## [2.1.15] 2019-07-18
* Fix the issue that the `DATE_ADD` function returns wrong results due to incorrect alignment when dealing with microseconds
[#11289](https://github.com/pingcap/tidb/pull/11289)
* Fix the issue that an error is reported when the empty value in the string column is compared with `FLOAT` or `INT` [#11279](https://github.com/pingcap/tidb/pull/11279)
* Fix the issue that the `INSERT` function fails to return the `NULL` value when a parameter is `NULL` [#11249](https://github.com/pingcap/tidb/pull/11249)
* Fix the issue that an error occurs when indexing the column of the non-string type and `0` length [#11215](https://github.com/pingcap/tidb/pull/11215)
* Add the `SHOW TABLE REGIONS` statement to query the Region distribution of a table through SQL statements [#11238](https://github.com/pingcap/tidb/pull/11238)
* Fix the issue that columns are wrongly pruned because the `SELECT` subquery fails to correctly parse the column in the `UPDATE` expression [#11254](https://github.com/pingcap/tidb/pull/11254)
* Add the `ADMIN PLUGINS ENABLE``/`ADMIN PLUGINS DISABLE` SQL statement to dynamically enable or disable plugins [#11189](https://github.com/pingcap/tidb/pull/11189)
* Add the session connection information in the audit plugins [#11189](https://github.com/pingcap/tidb/pull/11189)
* Fix the panic issue that happens when a column is queried on multiple times and the returned result is `NULL` during point queries [#11227](https://github.com/pingcap/tidb/pull/11227)
* Add the `tidb_scatter_region` configuration item to scatter table Regions when creating a table [#11213](https://github.com/pingcap/tidb/pull/11213)
* Fix the data race issue caused by non-thread safe `rand.Rand` when using the `RAND` function [#11170](https://github.com/pingcap/tidb/pull/11170)
* Fix the issue that the comparison result of integers and non-integers is incorrect in some cases [#11191](https://github.com/pingcap/tidb/pull/11191)
* Support modifying the collation of a database or a table, but the character set of the database/table has to be UTF-8 or UTF8MB4 [#11085](https://github.com/pingcap/tidb/pull/11085)
* Fix the issue that the precision shown by the `SHOW CREATE TABLE` statement is incomplete when `CURRENT_TIMESTAMP` is used as the default value of the column and the decimal precision is specified [#11087](https://github.com/pingcap/tidb/pull/11087)

## [2.1.14] 2019-07-04
* Fix wrong query results caused by column pruning in some cases [#11019](https://github.com/pingcap/tidb/pull/11019)
* Fix the wrongly displayed information in `db` and `info` columns of `show processlist` [#11000](https://github.com/pingcap/tidb/pull/11000)
* Fix the issue that `MAX_EXECUTION_TIME` as a SQL hint and global variable does not work under some cases [#10999](https://github.com/pingcap/tidb/pull/10999)
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
