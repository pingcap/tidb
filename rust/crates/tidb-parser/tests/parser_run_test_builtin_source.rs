// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-order transcreation of the Go `RunTest` builtin-function table in
//! `pkg/parser/parser_test.go` (`TestBuiltin`). Split out of
//! `parser_run_test_source` for file size; every case row is
//! character-identical to the original.

use crate::parser_run_test_helper::run_cases;

fn test_builtin_cases_0() {
    run_cases(&[
        ("SELECT POW(1, 2)", true, "SELECT POW(1, 2)"),
        ("SELECT POW(1, 2, 1)", true, "SELECT POW(1, 2, 1)"),
        ("SELECT POW(1, 0.5)", true, "SELECT POW(1, 0.5)"),
        ("SELECT POW(1, -1)", true, "SELECT POW(1, -1)"),
        ("SELECT POW(-1, 1)", true, "SELECT POW(-1, 1)"),
        ("SELECT RAND();", true, "SELECT RAND()"),
        ("SELECT RAND(1);", true, "SELECT RAND(1)"),
        ("SELECT MOD(10, 2);", true, "SELECT 10%2"),
        ("SELECT ROUND(-1.23);", true, "SELECT ROUND(-1.23)"),
        ("SELECT ROUND(1.23, 1);", true, "SELECT ROUND(1.23, 1)"),
        (
            "SELECT ROUND(1.23, 1, 1);",
            true,
            "SELECT ROUND(1.23, 1, 1)",
        ),
        ("SELECT CEIL(-1.23);", true, "SELECT CEIL(-1.23)"),
        ("SELECT CEILING(1.23);", true, "SELECT CEILING(1.23)"),
        ("SELECT FLOOR(-1.23);", true, "SELECT FLOOR(-1.23)"),
        ("SELECT LN(1);", true, "SELECT LN(1)"),
        ("SELECT LN(1, 2);", true, "SELECT LN(1, 2)"),
        ("SELECT LOG(-2);", true, "SELECT LOG(-2)"),
        ("SELECT LOG(2, 65536);", true, "SELECT LOG(2, 65536)"),
        ("SELECT LOG(2, 65536, 1);", true, "SELECT LOG(2, 65536, 1)"),
        ("SELECT LOG2(2);", true, "SELECT LOG2(2)"),
        ("SELECT LOG2(2, 2);", true, "SELECT LOG2(2, 2)"),
        ("SELECT LOG10(10);", true, "SELECT LOG10(10)"),
        ("SELECT LOG10(10, 1);", true, "SELECT LOG10(10, 1)"),
        ("SELECT ABS(10, 1);", true, "SELECT ABS(10, 1)"),
        ("SELECT ABS(10);", true, "SELECT ABS(10)"),
        ("SELECT ABS();", true, "SELECT ABS()"),
        (
            "SELECT CONV(10+'10'+'10'+X'0a',10,10);",
            true,
            "SELECT CONV(10+_UTF8MB4'10'+_UTF8MB4'10'+x'0a', 10, 10)",
        ),
        ("SELECT CONV();", true, "SELECT CONV()"),
        (
            "SELECT CRC32('MySQL');",
            true,
            "SELECT CRC32(_UTF8MB4'MySQL')",
        ),
        ("SELECT CRC32();", true, "SELECT CRC32()"),
        ("SELECT SIGN();", true, "SELECT SIGN()"),
        ("SELECT SIGN(0);", true, "SELECT SIGN(0)"),
        ("SELECT SQRT(0);", true, "SELECT SQRT(0)"),
        ("SELECT SQRT();", true, "SELECT SQRT()"),
        ("SELECT ACOS();", true, "SELECT ACOS()"),
        ("SELECT ACOS(1);", true, "SELECT ACOS(1)"),
        ("SELECT ACOS(1, 2);", true, "SELECT ACOS(1, 2)"),
        ("SELECT ASIN();", true, "SELECT ASIN()"),
        ("SELECT ASIN(1);", true, "SELECT ASIN(1)"),
        ("SELECT ASIN(1, 2);", true, "SELECT ASIN(1, 2)"),
    ]);
}

fn test_builtin_cases_1() {
    run_cases(&[
        (
            "SELECT ATAN(0), ATAN(1), ATAN(1, 2);",
            true,
            "SELECT ATAN(0),ATAN(1),ATAN(1, 2)",
        ),
        (
            "SELECT ATAN2(), ATAN2(1,2);",
            true,
            "SELECT ATAN2(),ATAN2(1, 2)",
        ),
        ("SELECT COS(0);", true, "SELECT COS(0)"),
        ("SELECT COS(1);", true, "SELECT COS(1)"),
        ("SELECT COS(1, 2);", true, "SELECT COS(1, 2)"),
        ("SELECT COT();", true, "SELECT COT()"),
        ("SELECT COT(1);", true, "SELECT COT(1)"),
        ("SELECT COT(1, 2);", true, "SELECT COT(1, 2)"),
        ("SELECT DEGREES();", true, "SELECT DEGREES()"),
        ("SELECT DEGREES(0);", true, "SELECT DEGREES(0)"),
        ("SELECT EXP();", true, "SELECT EXP()"),
        ("SELECT EXP(1);", true, "SELECT EXP(1)"),
        ("SELECT PI();", true, "SELECT PI()"),
        ("SELECT PI(1);", true, "SELECT PI(1)"),
        ("SELECT RADIANS();", true, "SELECT RADIANS()"),
        ("SELECT RADIANS(1);", true, "SELECT RADIANS(1)"),
        ("SELECT SIN();", true, "SELECT SIN()"),
        ("SELECT SIN(1);", true, "SELECT SIN(1)"),
        ("SELECT TAN(1);", true, "SELECT TAN(1)"),
        ("SELECT TAN();", true, "SELECT TAN()"),
        (
            "SELECT TRUNCATE(1.223,1);",
            true,
            "SELECT TRUNCATE(1.223, 1)",
        ),
        ("SELECT TRUNCATE();", true, "SELECT TRUNCATE()"),
        (
            "SELECT SUBSTR('Quadratically',5);",
            true,
            "SELECT SUBSTR(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SELECT SUBSTR('Quadratically',5, 3);",
            true,
            "SELECT SUBSTR(_UTF8MB4'Quadratically', 5, 3)",
        ),
        (
            "SELECT SUBSTR('Quadratically' FROM 5);",
            true,
            "SELECT SUBSTR(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SELECT SUBSTR('Quadratically' FROM 5 FOR 3);",
            true,
            "SELECT SUBSTR(_UTF8MB4'Quadratically', 5, 3)",
        ),
        (
            "SELECT SUBSTRING('Quadratically',5);",
            true,
            "SELECT SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SELECT SUBSTRING('Quadratically',5, 3);",
            true,
            "SELECT SUBSTRING(_UTF8MB4'Quadratically', 5, 3)",
        ),
        (
            "SELECT SUBSTRING('Quadratically' FROM 5);",
            true,
            "SELECT SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);",
            true,
            "SELECT SUBSTRING(_UTF8MB4'Quadratically', 5, 3)",
        ),
        (
            "SELECT CONVERT('111', SIGNED);",
            true,
            "SELECT CONVERT(_UTF8MB4'111', SIGNED)",
        ),
        (
            "SELECT LEAST(), LEAST(1, 2, 3);",
            true,
            "SELECT LEAST(),LEAST(1, 2, 3)",
        ),
        (
            "SELECT INTERVAL(1, 0, 1, 2)",
            true,
            "SELECT INTERVAL(1, 0, 1, 2)",
        ),
        (
            "SELECT (INTERVAL(1, 0, 1, 2)+5)*7+INTERVAL(1, 0, 1, 2)/2",
            true,
            "SELECT (INTERVAL(1, 0, 1, 2)+5)*7+INTERVAL(1, 0, 1, 2)/2",
        ),
        (
            "SELECT INTERVAL(0, (1*5)/2)+INTERVAL(5, 4, 3)",
            true,
            "SELECT INTERVAL(0, (1*5)/2)+INTERVAL(5, 4, 3)",
        ),
        (
            "SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY);",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)",
        ),
        ("SELECT DATABASE();", true, "SELECT DATABASE()"),
        ("SELECT SCHEMA();", true, "SELECT SCHEMA()"),
        ("SELECT USER();", true, "SELECT USER()"),
        ("SELECT USER(1);", true, "SELECT USER(1)"),
    ]);
}

fn test_builtin_cases_2() {
    run_cases(&[
        ("SELECT CURRENT_USER();", true, "SELECT CURRENT_USER()"),
        ("SELECT CURRENT_ROLE();", true, "SELECT CURRENT_ROLE()"),
        ("SELECT CURRENT_USER;", true, "SELECT CURRENT_USER()"),
        ("SELECT CONNECTION_ID();", true, "SELECT CONNECTION_ID()"),
        ("SELECT VERSION();", true, "SELECT VERSION()"),
        ("SELECT CURRENT_RESOURCE_GROUP();", true, "SELECT CURRENT_RESOURCE_GROUP()"),
        ("SELECT BENCHMARK(1000000, AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(1000000, AES_ENCRYPT(_UTF8MB4'text', UNHEX(_UTF8MB4'F3229A0B371ED2D9441B830D21A390C3')))"),
        ("SELECT BENCHMARK(AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(AES_ENCRYPT(_UTF8MB4'text', UNHEX(_UTF8MB4'F3229A0B371ED2D9441B830D21A390C3')))"),
        ("SELECT CHARSET('abc');", true, "SELECT CHARSET(_UTF8MB4'abc')"),
        ("SELECT COERCIBILITY('abc');", true, "SELECT COERCIBILITY(_UTF8MB4'abc')"),
        ("SELECT COERCIBILITY('abc', 'a');", true, "SELECT COERCIBILITY(_UTF8MB4'abc', _UTF8MB4'a')"),
        ("SELECT COLLATION('abc');", true, "SELECT COLLATION(_UTF8MB4'abc')"),
        ("SELECT ROW_COUNT();", true, "SELECT ROW_COUNT()"),
        ("SELECT SESSION_USER();", true, "SELECT SESSION_USER()"),
        ("SELECT SYSTEM_USER();", true, "SELECT SYSTEM_USER()"),
        ("SELECT FORMAT_BYTES(512);", true, "SELECT FORMAT_BYTES(512)"),
        ("SELECT FORMAT_NANO_TIME(3501);", true, "SELECT FORMAT_NANO_TIME(3501)"),
        ("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true, "SELECT SUBSTRING_INDEX(_UTF8MB4'www.mysql.com', _UTF8MB4'.', 2)"),
        ("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true, "SELECT SUBSTRING_INDEX(_UTF8MB4'www.mysql.com', _UTF8MB4'.', -2)"),
        ("SELECT ASCII(), ASCII(\"\"), ASCII(\"A\"), ASCII(1);", true, "SELECT ASCII(),ASCII(_UTF8MB4''),ASCII(_UTF8MB4'A'),ASCII(1)"),
        ("SELECT LOWER(\"A\"), UPPER(\"a\")", true, "SELECT LOWER(_UTF8MB4'A'),UPPER(_UTF8MB4'a')"),
        ("SELECT LCASE(\"A\"), UCASE(\"a\")", true, "SELECT LCASE(_UTF8MB4'A'),UCASE(_UTF8MB4'a')"),
        ("SELECT REPLACE('www.mysql.com', 'w', 'Ww')", true, "SELECT REPLACE(_UTF8MB4'www.mysql.com', _UTF8MB4'w', _UTF8MB4'Ww')"),
        ("SELECT LOCATE('bar', 'foobarbar');", true, "SELECT LOCATE(_UTF8MB4'bar', _UTF8MB4'foobarbar')"),
        ("SELECT LOCATE('bar', 'foobarbar', 5);", true, "SELECT LOCATE(_UTF8MB4'bar', _UTF8MB4'foobarbar', 5)"),
        ("SELECT tidb_version();", true, "SELECT TIDB_VERSION()"),
        ("SELECT tidb_is_ddl_owner();", true, "SELECT TIDB_IS_DDL_OWNER()"),
        ("SELECT tidb_decode_plan();", true, "SELECT TIDB_DECODE_PLAN()"),
        ("SELECT tidb_decode_key('abc');", true, "SELECT TIDB_DECODE_KEY(_UTF8MB4'abc')"),
        ("SELECT tidb_decode_base64_key('abc');", true, "SELECT TIDB_DECODE_BASE64_KEY(_UTF8MB4'abc')"),
        ("SELECT tidb_decode_sql_digests('[]');", true, "SELECT TIDB_DECODE_SQL_DIGESTS(_UTF8MB4'[]')"),
        ("CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"),
        ("select row(1)", false, ""),
        ("select row(1, 1,)", false, ""),
        ("select (1, 1,)", false, ""),
        ("select row(1, 1) > row(1, 1), row(1, 1, 1) > row(1, 1, 1)", true, "SELECT ROW(1,1)>ROW(1,1),ROW(1,1,1)>ROW(1,1,1)"),
        ("Select (1, 1) > (1, 1)", true, "SELECT ROW(1,1)>ROW(1,1)"),
        ("create table t (`row` int)", true, "CREATE TABLE `t` (`row` INT)"),
        ("create table t (row int)", false, ""),
        ("SELECT *, CAST(data AS CHAR CHARACTER SET utf8) FROM t;", true, "SELECT *,CAST(`data` AS CHAR CHARSET UTF8) FROM `t`"),
    ]);
}

fn test_builtin_cases_3() {
    run_cases(&[
        (
            "SELECT CAST(data AS CHARACTER);",
            true,
            "SELECT CAST(`data` AS CHAR)",
        ),
        (
            "SELECT CAST(data AS CHARACTER(10) CHARACTER SET utf8);",
            true,
            "SELECT CAST(`data` AS CHAR(10) CHARSET UTF8)",
        ),
        (
            "SELECT CAST(data AS BINARY)",
            true,
            "SELECT CAST(`data` AS BINARY)",
        ),
        (
            "SELECT *, CAST(data AS JSON) FROM t;",
            true,
            "SELECT *,CAST(`data` AS JSON) FROM `t`",
        ),
        (
            "SELECT *, JSON_SUM_CRC32(data AS UNSIGNED ARRAY) FROM t;",
            true,
            "SELECT *,JSON_SUM_CRC32(`data` AS UNSIGNED ARRAY) FROM `t`",
        ),
        (
            "SELECT *, JSON_SUM_CRC32(data AS DOUBLE ARRAY) FROM t;",
            true,
            "SELECT *,JSON_SUM_CRC32(`data` AS DOUBLE ARRAY) FROM `t`",
        ),
        (
            "SELECT *, JSON_SUM_CRC32(data AS DOUBLE) FROM t;",
            false,
            "",
        ),
        ("SELECT *, JSON_SUM_CRC32(data) FROM t;", false, ""),
        (
            "select cast(1 as signed int);",
            true,
            "SELECT CAST(1 AS SIGNED)",
        ),
        (
            "select cast(1 as double);",
            true,
            "SELECT CAST(1 AS DOUBLE)",
        ),
        ("select cast(1 as float);", true, "SELECT CAST(1 AS FLOAT)"),
        (
            "select cast(1 as float(0));",
            true,
            "SELECT CAST(1 AS FLOAT)",
        ),
        (
            "select cast(1 as float(24));",
            true,
            "SELECT CAST(1 AS FLOAT)",
        ),
        (
            "select cast(1 as float(25));",
            true,
            "SELECT CAST(1 AS DOUBLE)",
        ),
        (
            "select cast(1 as float(53));",
            true,
            "SELECT CAST(1 AS DOUBLE)",
        ),
        ("select cast(1 as float(54));", false, ""),
        ("select cast(1 as real);", true, "SELECT CAST(1 AS DOUBLE)"),
        (
            "select cast('2000' as year);",
            true,
            "SELECT CAST(_UTF8MB4'2000' AS YEAR)",
        ),
        (
            "select cast(time '2000' as year);",
            true,
            "SELECT CAST(TIME '2000' AS YEAR)",
        ),
        (
            "select cast(b as signed array);",
            true,
            "SELECT CAST(`b` AS SIGNED ARRAY)",
        ),
        (
            "select cast(b as char(10) array);",
            true,
            "SELECT CAST(`b` AS CHAR(10) ARRAY)",
        ),
        ("SELECT last_insert_id();", true, "SELECT LAST_INSERT_ID()"),
        (
            "SELECT last_insert_id(1);",
            true,
            "SELECT LAST_INSERT_ID(1)",
        ),
        ("SELECT binary 'a';", true, "SELECT BINARY _UTF8MB4'a'"),
        ("SELECT BIT_COUNT(1);", true, "SELECT BIT_COUNT(1)"),
        (
            "select current_timestamp",
            true,
            "SELECT CURRENT_TIMESTAMP()",
        ),
        (
            "select current_timestamp()",
            true,
            "SELECT CURRENT_TIMESTAMP()",
        ),
        (
            "select current_timestamp(6)",
            true,
            "SELECT CURRENT_TIMESTAMP(6)",
        ),
        ("select current_timestamp(null)", false, ""),
        ("select current_timestamp(-1)", false, ""),
        ("select current_timestamp(1.0)", false, ""),
        ("select current_timestamp('2')", false, ""),
        ("select now()", true, "SELECT NOW()"),
        ("select now(6)", true, "SELECT NOW(6)"),
        (
            "select sysdate(), sysdate(6)",
            true,
            "SELECT SYSDATE(),SYSDATE(6)",
        ),
        (
            "SELECT time('01:02:03');",
            true,
            "SELECT TIME(_UTF8MB4'01:02:03')",
        ),
        (
            "SELECT time('01:02:03.1')",
            true,
            "SELECT TIME(_UTF8MB4'01:02:03.1')",
        ),
        ("SELECT time('20.1')", true, "SELECT TIME(_UTF8MB4'20.1')"),
        (
            "SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');",
            true,
            "SELECT TIMEDIFF(_UTF8MB4'2000:01:01 00:00:00', _UTF8MB4'2000:01:01 00:00:00.000001')",
        ),
        (
            "SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');",
            true,
            "SELECT TIMESTAMPDIFF(MONTH, _UTF8MB4'2003-02-01', _UTF8MB4'2003-05-01')",
        ),
    ]);
}

fn test_builtin_cases_4() {
    run_cases(&[
        (
            "SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');",
            true,
            "SELECT TIMESTAMPDIFF(YEAR, _UTF8MB4'2002-05-01', _UTF8MB4'2001-01-01')",
        ),
        (
            "SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');",
            true,
            "SELECT TIMESTAMPDIFF(MINUTE, _UTF8MB4'2003-02-01', _UTF8MB4'2003-05-01 12:05:55')",
        ),
        ("select current_time", true, "SELECT CURRENT_TIME()"),
        ("select current_time()", true, "SELECT CURRENT_TIME()"),
        ("select current_time(6)", true, "SELECT CURRENT_TIME(6)"),
        ("select current_time(-1)", false, ""),
        ("select current_time(1.0)", false, ""),
        ("select current_time('1')", false, ""),
        ("select current_time(null)", false, ""),
        ("select curtime()", true, "SELECT CURTIME()"),
        ("select curtime(6)", true, "SELECT CURTIME(6)"),
        ("select curtime(-1)", false, ""),
        ("select curtime(1.0)", false, ""),
        ("select curtime('1')", false, ""),
        ("select curtime(null)", false, ""),
        ("select utc_timestamp", true, "SELECT UTC_TIMESTAMP()"),
        ("select utc_timestamp()", true, "SELECT UTC_TIMESTAMP()"),
        ("select utc_timestamp(6)", true, "SELECT UTC_TIMESTAMP(6)"),
        ("select utc_timestamp(-1)", false, ""),
        ("select utc_timestamp(1.0)", false, ""),
        ("select utc_timestamp('1')", false, ""),
        ("select utc_timestamp(null)", false, ""),
        ("select utc_time", true, "SELECT UTC_TIME()"),
        ("select utc_time()", true, "SELECT UTC_TIME()"),
        ("select utc_time(6)", true, "SELECT UTC_TIME(6)"),
        ("select utc_time(-1)", false, ""),
        ("select utc_time(1.0)", false, ""),
        ("select utc_time('1')", false, ""),
        ("select utc_time(null)", false, ""),
        (
            "SELECT MICROSECOND('2009-12-31 23:59:59.000010');",
            true,
            "SELECT MICROSECOND(_UTF8MB4'2009-12-31 23:59:59.000010')",
        ),
        (
            "SELECT SECOND('10:05:03');",
            true,
            "SELECT SECOND(_UTF8MB4'10:05:03')",
        ),
        (
            "SELECT MINUTE('2008-02-03 10:05:03');",
            true,
            "SELECT MINUTE(_UTF8MB4'2008-02-03 10:05:03')",
        ),
        (
            "SELECT HOUR(), HOUR('10:05:03');",
            true,
            "SELECT HOUR(),HOUR(_UTF8MB4'10:05:03')",
        ),
        (
            "SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE()",
            true,
            "SELECT CURRENT_DATE(),CURRENT_DATE(),CURDATE()",
        ),
        ("SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE(1)", false, ""),
        (
            "SELECT DATEDIFF('2003-12-31', '2003-12-30');",
            true,
            "SELECT DATEDIFF(_UTF8MB4'2003-12-31', _UTF8MB4'2003-12-30')",
        ),
        (
            "SELECT DATE('2003-12-31 01:02:03');",
            true,
            "SELECT DATE(_UTF8MB4'2003-12-31 01:02:03')",
        ),
        ("SELECT DATE();", true, "SELECT DATE()"),
        (
            "SELECT DATE('2003-12-31 01:02:03', '');",
            true,
            "SELECT DATE(_UTF8MB4'2003-12-31 01:02:03', _UTF8MB4'')",
        ),
        (
            "SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y');",
            true,
            "SELECT DATE_FORMAT(_UTF8MB4'2003-12-31 01:02:03', _UTF8MB4'%W %M %Y')",
        ),
    ]);
}

fn test_builtin_cases_5() {
    run_cases(&[
        ("SELECT DAY('2007-02-03');", true, "SELECT DAY(_UTF8MB4'2007-02-03')"),
        ("SELECT DAYOFMONTH('2007-02-03');", true, "SELECT DAYOFMONTH(_UTF8MB4'2007-02-03')"),
        ("SELECT DAYOFWEEK('2007-02-03');", true, "SELECT DAYOFWEEK(_UTF8MB4'2007-02-03')"),
        ("SELECT DAYOFYEAR('2007-02-03');", true, "SELECT DAYOFYEAR(_UTF8MB4'2007-02-03')"),
        ("SELECT DAYNAME('2007-02-03');", true, "SELECT DAYNAME(_UTF8MB4'2007-02-03')"),
        ("SELECT FROM_DAYS(1423);", true, "SELECT FROM_DAYS(1423)"),
        ("SELECT WEEKDAY('2007-02-03');", true, "SELECT WEEKDAY(_UTF8MB4'2007-02-03')"),
        ("SELECT UTC_DATE, UTC_DATE();", true, "SELECT UTC_DATE(),UTC_DATE()"),
        ("SELECT UTC_DATE(), UTC_DATE()+0", true, "SELECT UTC_DATE(),UTC_DATE()+0"),
        ("SELECT WEEK();", true, "SELECT WEEK()"),
        ("SELECT WEEK('2007-02-03');", true, "SELECT WEEK(_UTF8MB4'2007-02-03')"),
        ("SELECT WEEK('2007-02-03', 0);", true, "SELECT WEEK(_UTF8MB4'2007-02-03', 0)"),
        ("SELECT WEEKOFYEAR('2007-02-03');", true, "SELECT WEEKOFYEAR(_UTF8MB4'2007-02-03')"),
        ("SELECT MONTH('2007-02-03');", true, "SELECT MONTH(_UTF8MB4'2007-02-03')"),
        ("SELECT MONTHNAME('2007-02-03');", true, "SELECT MONTHNAME(_UTF8MB4'2007-02-03')"),
        ("SELECT YEAR('2007-02-03');", true, "SELECT YEAR(_UTF8MB4'2007-02-03')"),
        ("SELECT YEARWEEK('2007-02-03');", true, "SELECT YEARWEEK(_UTF8MB4'2007-02-03')"),
        ("SELECT YEARWEEK('2007-02-03', 0);", true, "SELECT YEARWEEK(_UTF8MB4'2007-02-03', 0)"),
        ("SELECT ADDTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT ADDTIME(_UTF8MB4'01:00:00.999999', _UTF8MB4'02:00:00.999998')"),
        ("SELECT ADDTIME('02:00:00.999998');", true, "SELECT ADDTIME(_UTF8MB4'02:00:00.999998')"),
        ("SELECT ADDTIME();", true, "SELECT ADDTIME()"),
        ("SELECT SUBTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT SUBTIME(_UTF8MB4'01:00:00.999999', _UTF8MB4'02:00:00.999998')"),
        ("SELECT CONVERT_TZ();", true, "SELECT CONVERT_TZ()"),
        ("SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00');", true, "SELECT CONVERT_TZ(_UTF8MB4'2004-01-01 12:00:00', _UTF8MB4'+00:00', _UTF8MB4'+10:00')"),
        ("SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00', '+10:00');", true, "SELECT CONVERT_TZ(_UTF8MB4'2004-01-01 12:00:00', _UTF8MB4'+00:00', _UTF8MB4'+10:00', _UTF8MB4'+10:00')"),
        ("SELECT GET_FORMAT(DATE, 'USA');", true, "SELECT GET_FORMAT(DATE, _UTF8MB4'USA')"),
        ("SELECT GET_FORMAT(DATETIME, 'USA');", true, "SELECT GET_FORMAT(DATETIME, _UTF8MB4'USA')"),
        ("SELECT GET_FORMAT(TIME, 'USA');", true, "SELECT GET_FORMAT(TIME, _UTF8MB4'USA')"),
        ("SELECT GET_FORMAT(TIMESTAMP, 'USA');", true, "SELECT GET_FORMAT(DATETIME, _UTF8MB4'USA')"),
        ("SELECT LOCALTIME(), LOCALTIME(1)", true, "SELECT LOCALTIME(),LOCALTIME(1)"),
        ("SELECT LOCALTIMESTAMP(), LOCALTIMESTAMP(2)", true, "SELECT LOCALTIMESTAMP(),LOCALTIMESTAMP(2)"),
        ("SELECT MAKEDATE(2011,31);", true, "SELECT MAKEDATE(2011, 31)"),
        ("SELECT MAKETIME(12,15,30);", true, "SELECT MAKETIME(12, 15, 30)"),
        ("SELECT MAKEDATE();", true, "SELECT MAKEDATE()"),
        ("SELECT MAKETIME();", true, "SELECT MAKETIME()"),
        ("SELECT PERIOD_ADD(200801,2)", true, "SELECT PERIOD_ADD(200801, 2)"),
        ("SELECT PERIOD_DIFF(200802,200703)", true, "SELECT PERIOD_DIFF(200802, 200703)"),
        ("SELECT QUARTER('2008-04-01');", true, "SELECT QUARTER(_UTF8MB4'2008-04-01')"),
        ("SELECT SEC_TO_TIME(2378)", true, "SELECT SEC_TO_TIME(2378)"),
        ("SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')", true, "SELECT TIME_FORMAT(_UTF8MB4'100:00:00', _UTF8MB4'%H %k %h %I %l')"),
    ]);
}

fn test_builtin_cases_6() {
    run_cases(&[
        (
            "SELECT TIME_TO_SEC('22:23:00')",
            true,
            "SELECT TIME_TO_SEC(_UTF8MB4'22:23:00')",
        ),
        (
            "SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(WEEK, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_SECOND,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(SECOND, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_MINUTE,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(MINUTE, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_HOUR,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(HOUR, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_DAY,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(DAY, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_WEEK,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(WEEK, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_MONTH,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(MONTH, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_QUARTER,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(QUARTER, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_YEAR,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(YEAR, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TIMESTAMPADD(SQL_TSI_MICROSECOND,1,'2003-01-02');",
            false,
            "",
        ),
        (
            "SELECT TIMESTAMPADD(MICROSECOND,1,'2003-01-02');",
            true,
            "SELECT TIMESTAMPADD(MICROSECOND, 1, _UTF8MB4'2003-01-02')",
        ),
        (
            "SELECT TO_DAYS('2007-10-07')",
            true,
            "SELECT TO_DAYS(_UTF8MB4'2007-10-07')",
        ),
        (
            "SELECT TO_SECONDS('2009-11-29')",
            true,
            "SELECT TO_SECONDS(_UTF8MB4'2009-11-29')",
        ),
        (
            "SELECT LAST_DAY('2003-02-05');",
            true,
            "SELECT LAST_DAY(_UTF8MB4'2003-02-05')",
        ),
        (
            "SELECT UTC_TIME(), UTC_TIME(1)",
            true,
            "SELECT UTC_TIME(),UTC_TIME(1)",
        ),
        (
            "select extract(microsecond from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(MICROSECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(second from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(SECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(minute from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(MINUTE FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(hour from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(HOUR FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(day from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(DAY FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(week from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(WEEK FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(month from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(MONTH FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(quarter from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(QUARTER FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(year from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(YEAR FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(second_microsecond from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(SECOND_MICROSECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(minute_microsecond from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(MINUTE_MICROSECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(minute_second from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(MINUTE_SECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(hour_microsecond from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(HOUR_MICROSECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(hour_second from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(HOUR_SECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(hour_minute from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(HOUR_MINUTE FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(day_microsecond from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(DAY_MICROSECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(day_second from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(DAY_SECOND FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(day_minute from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(DAY_MINUTE FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(day_hour from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(DAY_HOUR FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select extract(year_month from \"2011-11-11 10:10:10.123456\")",
            true,
            "SELECT EXTRACT(YEAR_MONTH FROM _UTF8MB4'2011-11-11 10:10:10.123456')",
        ),
        (
            "select from_unixtime(1447430881)",
            true,
            "SELECT FROM_UNIXTIME(1447430881)",
        ),
        (
            "select from_unixtime(1447430881.123456)",
            true,
            "SELECT FROM_UNIXTIME(1447430881.123456)",
        ),
        (
            "select from_unixtime(1447430881.1234567)",
            true,
            "SELECT FROM_UNIXTIME(1447430881.1234567)",
        ),
        (
            "select from_unixtime(1447430881.9999999)",
            true,
            "SELECT FROM_UNIXTIME(1447430881.9999999)",
        ),
    ]);
}

fn test_builtin_cases_7() {
    run_cases(&[
        ("select from_unixtime(1447430881, \"%Y %D %M %h:%i:%s %x\")", true, "SELECT FROM_UNIXTIME(1447430881, _UTF8MB4'%Y %D %M %h:%i:%s %x')"),
        ("select from_unixtime(1447430881.123456, \"%Y %D %M %h:%i:%s %x\")", true, "SELECT FROM_UNIXTIME(1447430881.123456, _UTF8MB4'%Y %D %M %h:%i:%s %x')"),
        ("select from_unixtime(1447430881.1234567, \"%Y %D %M %h:%i:%s %x\")", true, "SELECT FROM_UNIXTIME(1447430881.1234567, _UTF8MB4'%Y %D %M %h:%i:%s %x')"),
        ("SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin;", true, "SELECT CAST(_UTF8MB4'test collated returns' AS CHAR CHARSET UTF8) COLLATE utf8_bin"),
        ("SELECT TRIM('  bar   ');", true, "SELECT TRIM(_UTF8MB4'  bar   ')"),
        ("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');", true, "SELECT TRIM(LEADING _UTF8MB4'x' FROM _UTF8MB4'xxxbarxxx')"),
        ("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');", true, "SELECT TRIM(BOTH _UTF8MB4'x' FROM _UTF8MB4'xxxbarxxx')"),
        ("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');", true, "SELECT TRIM(TRAILING _UTF8MB4'xyz' FROM _UTF8MB4'barxxyz')"),
        ("SELECT LTRIM(' foo ');", true, "SELECT LTRIM(_UTF8MB4' foo ')"),
        ("SELECT RTRIM(' bar ');", true, "SELECT RTRIM(_UTF8MB4' bar ')"),
        ("SELECT RPAD('hi', 6, 'c');", true, "SELECT RPAD(_UTF8MB4'hi', 6, _UTF8MB4'c')"),
        ("SELECT BIT_LENGTH('hi');", true, "SELECT BIT_LENGTH(_UTF8MB4'hi')"),
        ("SELECT CHAR(65);", true, "SELECT CHAR_FUNC(65, NULL)"),
        ("SELECT CHAR_LENGTH('abc');", true, "SELECT CHAR_LENGTH(_UTF8MB4'abc')"),
        ("SELECT CHARACTER_LENGTH('abc');", true, "SELECT CHARACTER_LENGTH(_UTF8MB4'abc')"),
        ("SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');", true, "SELECT FIELD(_UTF8MB4'ej', _UTF8MB4'Hej', _UTF8MB4'ej', _UTF8MB4'Heja', _UTF8MB4'hej', _UTF8MB4'foo')"),
        ("SELECT FIND_IN_SET('foo', 'foo,bar')", true, "SELECT FIND_IN_SET(_UTF8MB4'foo', _UTF8MB4'foo,bar')"),
        ("SELECT FIND_IN_SET('foo')", true, "SELECT FIND_IN_SET(_UTF8MB4'foo')"),
        ("SELECT MAKE_SET(1,'a'), MAKE_SET(1,'a','b','c')", true, "SELECT MAKE_SET(1, _UTF8MB4'a'),MAKE_SET(1, _UTF8MB4'a', _UTF8MB4'b', _UTF8MB4'c')"),
        ("SELECT MID('Sakila', -5, 3)", true, "SELECT MID(_UTF8MB4'Sakila', -5, 3)"),
        ("SELECT OCT(12)", true, "SELECT OCT(12)"),
        ("SELECT OCTET_LENGTH('text')", true, "SELECT OCTET_LENGTH(_UTF8MB4'text')"),
        ("SELECT ORD('2')", true, "SELECT ORD(_UTF8MB4'2')"),
        ("SELECT POSITION('bar' IN 'foobarbar')", true, "SELECT POSITION(_UTF8MB4'bar' IN _UTF8MB4'foobarbar')"),
        ("SELECT QUOTE('Don\\'t!')", true, "SELECT QUOTE(_UTF8MB4'Don''t!')"),
        ("SELECT BIN(12)", true, "SELECT BIN(12)"),
        ("SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')", true, "SELECT ELT(1, _UTF8MB4'ej', _UTF8MB4'Heja', _UTF8MB4'hej', _UTF8MB4'foo')"),
        ("SELECT EXPORT_SET(5,'Y','N'), EXPORT_SET(5,'Y','N',','), EXPORT_SET(5,'Y','N',',',4)", true, "SELECT EXPORT_SET(5, _UTF8MB4'Y', _UTF8MB4'N'),EXPORT_SET(5, _UTF8MB4'Y', _UTF8MB4'N', _UTF8MB4','),EXPORT_SET(5, _UTF8MB4'Y', _UTF8MB4'N', _UTF8MB4',', 4)"),
        ("SELECT FORMAT(), FORMAT(12332.2,2,'de_DE'), FORMAT(12332.123456, 4)", true, "SELECT FORMAT(),FORMAT(12332.2, 2, _UTF8MB4'de_DE'),FORMAT(12332.123456, 4)"),
        ("SELECT FROM_BASE64('abc')", true, "SELECT FROM_BASE64(_UTF8MB4'abc')"),
        ("SELECT TO_BASE64('abc')", true, "SELECT TO_BASE64(_UTF8MB4'abc')"),
        ("SELECT INSERT(), INSERT('Quadratic', 3, 4, 'What'), INSTR('foobarbar', 'bar')", true, "SELECT INSERT_FUNC(),INSERT_FUNC(_UTF8MB4'Quadratic', 3, 4, _UTF8MB4'What'),INSTR(_UTF8MB4'foobarbar', _UTF8MB4'bar')"),
        ("SELECT LOAD_FILE('/tmp/picture')", true, "SELECT LOAD_FILE(_UTF8MB4'/tmp/picture')"),
        ("SELECT LPAD('hi',4,'??')", true, "SELECT LPAD(_UTF8MB4'hi', 4, _UTF8MB4'??')"),
        ("SELECT LEFT(\"foobar\", 3)", true, "SELECT LEFT(_UTF8MB4'foobar', 3)"),
        ("SELECT RIGHT(\"foobar\", 3)", true, "SELECT RIGHT(_UTF8MB4'foobar', 3)"),
        ("SELECT REPEAT(\"a\", 10);", true, "SELECT REPEAT(_UTF8MB4'a', 10)"),
        ("SELECT SLEEP(10);", true, "SELECT SLEEP(10)"),
        ("SELECT ANY_VALUE(@arg);", true, "SELECT ANY_VALUE(@`arg`)"),
        ("SELECT INET_ATON('10.0.5.9');", true, "SELECT INET_ATON(_UTF8MB4'10.0.5.9')"),
    ]);
}

fn test_builtin_cases_8() {
    run_cases(&[
        (
            "SELECT INET_NTOA(167773449);",
            true,
            "SELECT INET_NTOA(167773449)",
        ),
        (
            "SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089');",
            true,
            "SELECT INET6_ATON(_UTF8MB4'fdfe::5a55:caff:fefa:9089')",
        ),
        (
            "SELECT INET6_NTOA(INET_NTOA(167773449));",
            true,
            "SELECT INET6_NTOA(INET_NTOA(167773449))",
        ),
        (
            "SELECT IS_FREE_LOCK(@str);",
            true,
            "SELECT IS_FREE_LOCK(@`str`)",
        ),
        (
            "SELECT IS_IPV4('10.0.5.9');",
            true,
            "SELECT IS_IPV4(_UTF8MB4'10.0.5.9')",
        ),
        (
            "SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));",
            true,
            "SELECT IS_IPV4_COMPAT(INET6_ATON(_UTF8MB4'::10.0.5.9'))",
        ),
        (
            "SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));",
            true,
            "SELECT IS_IPV4_MAPPED(INET6_ATON(_UTF8MB4'::10.0.5.9'))",
        ),
        (
            "SELECT IS_IPV6('10.0.5.9');",
            true,
            "SELECT IS_IPV6(_UTF8MB4'10.0.5.9')",
        ),
        (
            "SELECT IS_USED_LOCK(@str);",
            true,
            "SELECT IS_USED_LOCK(@`str`)",
        ),
        (
            "SELECT NAME_CONST('myname', 14);",
            true,
            "SELECT NAME_CONST(_UTF8MB4'myname', 14)",
        ),
        (
            "SELECT RELEASE_ALL_LOCKS();",
            true,
            "SELECT RELEASE_ALL_LOCKS()",
        ),
        ("SELECT UUID();", true, "SELECT UUID()"),
        ("SELECT UUID_SHORT()", true, "SELECT UUID_SHORT()"),
        (
            "SELECT UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db')",
            true,
            "SELECT UUID_TO_BIN(_UTF8MB4'6ccd780c-baba-1026-9564-5b8c656024db')",
        ),
        (
            "SELECT UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db', 1)",
            true,
            "SELECT UUID_TO_BIN(_UTF8MB4'6ccd780c-baba-1026-9564-5b8c656024db', 1)",
        ),
        (
            "SELECT BIN_TO_UUID(0x6ccd780cbaba102695645b8c656024db)",
            true,
            "SELECT BIN_TO_UUID(x'6ccd780cbaba102695645b8c656024db')",
        ),
        (
            "SELECT BIN_TO_UUID(0x6ccd780cbaba102695645b8c656024db, 1)",
            true,
            "SELECT BIN_TO_UUID(x'6ccd780cbaba102695645b8c656024db', 1)",
        ),
        ("SELECT SLEEP();", true, "SELECT SLEEP()"),
        ("SELECT ANY_VALUE();", true, "SELECT ANY_VALUE()"),
        ("SELECT INET_ATON();", true, "SELECT INET_ATON()"),
        ("SELECT INET_NTOA();", true, "SELECT INET_NTOA()"),
        ("SELECT INET6_ATON();", true, "SELECT INET6_ATON()"),
        (
            "SELECT INET6_NTOA(INET_NTOA());",
            true,
            "SELECT INET6_NTOA(INET_NTOA())",
        ),
        ("SELECT IS_FREE_LOCK();", true, "SELECT IS_FREE_LOCK()"),
        ("SELECT IS_IPV4();", true, "SELECT IS_IPV4()"),
        (
            "SELECT IS_IPV4_COMPAT(INET6_ATON());",
            true,
            "SELECT IS_IPV4_COMPAT(INET6_ATON())",
        ),
        (
            "SELECT IS_IPV4_MAPPED(INET6_ATON());",
            true,
            "SELECT IS_IPV4_MAPPED(INET6_ATON())",
        ),
        ("SELECT IS_IPV6()", true, "SELECT IS_IPV6()"),
        ("SELECT IS_USED_LOCK();", true, "SELECT IS_USED_LOCK()"),
        ("SELECT NAME_CONST();", true, "SELECT NAME_CONST()"),
        (
            "SELECT RELEASE_ALL_LOCKS(1);",
            true,
            "SELECT RELEASE_ALL_LOCKS(1)",
        ),
        ("SELECT UUID(1);", true, "SELECT UUID(1)"),
        ("SELECT UUID_SHORT(1)", true, "SELECT UUID_SHORT(1)"),
        (
            "select \"2011-11-11 10:10:10.123456\" + interval 10 second",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)",
        ),
        (
            "select \"2011-11-11 10:10:10.123456\" - interval 10 second",
            true,
            "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)",
        ),
        (
            "select  interval 10 second + \"2011-11-11 10:10:10.123456\"",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)",
        ),
        (
            "select date_add(\"2011-11-11 10:10:10.123456\", interval 10 microsecond)",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)",
        ),
        (
            "select date_add(\"2011-11-11 10:10:10.123456\", interval 10 second)",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)",
        ),
        (
            "select date_add(\"2011-11-11 10:10:10.123456\", interval 10 minute)",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)",
        ),
        (
            "select date_add(\"2011-11-11 10:10:10.123456\", interval 10 hour)",
            true,
            "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)",
        ),
    ]);
}

fn test_builtin_cases_9() {
    run_cases(&[
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 day)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 week)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 month)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 quarter)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 year)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10.10\" second_microsecond)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10.10' SECOND_MICROSECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10:10.10\" minute_microsecond)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10.10' MINUTE_MICROSECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10:10\" minute_second)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' MINUTE_SECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10:10:10.10\" hour_microsecond)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10.10' HOUR_MICROSECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10:10:10\" hour_second)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10' HOUR_SECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"10:10\" hour_minute)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' HOUR_MINUTE)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10.10 hour_minute)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10.10\" day_microsecond)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10.10' DAY_MICROSECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10\" day_second)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10' DAY_SECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"11 10:10\" day_minute)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10' DAY_MINUTE)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"11 10\" day_hour)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10' DAY_HOUR)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval \"11-11\" year_month)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11-11' YEAR_MONTH)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", 10)", false, ""),
        ("select date_add(\"2011-11-11 10:10:10.123456\", 0.10)", false, ""),
        ("select date_add(\"2011-11-11 10:10:10.123456\", \"11,11\")", false, ""),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 sql_tsi_microsecond)", false, ""),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 sql_tsi_second)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 sql_tsi_minute)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 sql_tsi_hour)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 10 sql_tsi_day)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 sql_tsi_week)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 sql_tsi_month)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 sql_tsi_quarter)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"),
        ("select date_add(\"2011-11-11 10:10:10.123456\", interval 1 sql_tsi_year)", true, "SELECT DATE_ADD(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"),
        ("select strcmp('abc', 'def')", true, "SELECT STRCMP(_UTF8MB4'abc', _UTF8MB4'def')"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10 microsecond)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10 second)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10 minute)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10 hour)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10 day)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 1 week)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 1 month)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 1 quarter)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 1 year)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10.10\" second_microsecond)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10.10' SECOND_MICROSECOND)"),
    ]);
}

fn test_builtin_cases_10() {
    run_cases(&[
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10:10.10\" minute_microsecond)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10.10' MINUTE_MICROSECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10:10\" minute_second)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' MINUTE_SECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10:10:10.10\" hour_microsecond)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10.10' HOUR_MICROSECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10:10:10\" hour_second)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10' HOUR_SECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"10:10\" hour_minute)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' HOUR_MINUTE)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval 10.10 hour_minute)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10.10\" day_microsecond)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10.10' DAY_MICROSECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10\" day_second)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10' DAY_SECOND)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10\" day_minute)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10' DAY_MINUTE)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"11 10\" day_hour)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10' DAY_HOUR)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", interval \"11-11\" year_month)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11-11' YEAR_MONTH)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", 10)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", 0.10)", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"),
        ("select adddate(\"2011-11-11 10:10:10.123456\", \"11,11\")", true, "SELECT ADDDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11,11' DAY)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10 microsecond)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10 second)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10 minute)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10 hour)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10 day)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 1 week)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 1 month)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 1 quarter)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 1 year)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10.10\" second_microsecond)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10.10' SECOND_MICROSECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10:10.10\" minute_microsecond)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10.10' MINUTE_MICROSECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10:10\" minute_second)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' MINUTE_SECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10:10:10.10\" hour_microsecond)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10.10' HOUR_MICROSECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10:10:10\" hour_second)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10' HOUR_SECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"10:10\" hour_minute)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' HOUR_MINUTE)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval 10.10 hour_minute)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10.10\" day_microsecond)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10.10' DAY_MICROSECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10\" day_second)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10' DAY_SECOND)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"11 10:10\" day_minute)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10' DAY_MINUTE)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"11 10\" day_hour)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10' DAY_HOUR)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", interval \"11-11\" year_month)", true, "SELECT DATE_SUB(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11-11' YEAR_MONTH)"),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", 10)", false, ""),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", 0.10)", false, ""),
        ("select date_sub(\"2011-11-11 10:10:10.123456\", \"11,11\")", false, ""),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10 microsecond)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10 second)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"),
    ]);
}

fn test_builtin_cases_11() {
    run_cases(&[
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10 minute)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10 hour)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10 day)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 1 week)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 1 month)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 1 quarter)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 1 year)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10.10\" second_microsecond)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10.10' SECOND_MICROSECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10:10.10\" minute_microsecond)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10.10' MINUTE_MICROSECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10:10\" minute_second)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' MINUTE_SECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10:10:10.10\" hour_microsecond)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10.10' HOUR_MICROSECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10:10:10\" hour_second)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10:10' HOUR_SECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"10:10\" hour_minute)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'10:10' HOUR_MINUTE)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval 10.10 hour_minute)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10.10\" day_microsecond)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10.10' DAY_MICROSECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10:10\" day_second)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10:10' DAY_SECOND)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"11 10:10\" day_minute)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10:10' DAY_MINUTE)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"11 10\" day_hour)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11 10' DAY_HOUR)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", interval \"11-11\" year_month)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11-11' YEAR_MONTH)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", 10)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", 0.10)", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"),
        ("select subdate(\"2011-11-11 10:10:10.123456\", \"11,11\")", true, "SELECT SUBDATE(_UTF8MB4'2011-11-11 10:10:10.123456', INTERVAL _UTF8MB4'11,11' DAY)"),
        ("select unix_timestamp()", true, "SELECT UNIX_TIMESTAMP()"),
        ("select unix_timestamp('2015-11-13 10:20:19.012')", true, "SELECT UNIX_TIMESTAMP(_UTF8MB4'2015-11-13 10:20:19.012')"),
        ("SELECT GET_LOCK('lock1',10);", true, "SELECT GET_LOCK(_UTF8MB4'lock1', 10)"),
        ("SELECT RELEASE_LOCK('lock1');", true, "SELECT RELEASE_LOCK(_UTF8MB4'lock1')"),
        ("select avg(), avg(c1,c2) from t;", false, "SELECT AVG(),AVG(`c1`, `c2`) FROM `t`"),
        ("select avg(distinct c1) from t;", true, "SELECT AVG(DISTINCT `c1`) FROM `t`"),
        ("select avg(distinctrow c1) from t;", true, "SELECT AVG(DISTINCT `c1`) FROM `t`"),
        ("select avg(distinct all c1) from t;", true, "SELECT AVG(DISTINCT `c1`) FROM `t`"),
        ("select avg(distinctrow all c1) from t;", true, "SELECT AVG(DISTINCT `c1`) FROM `t`"),
        ("select avg(c2) from t;", true, "SELECT AVG(`c2`) FROM `t`"),
        ("select bit_and(c1) from t;", true, "SELECT BIT_AND(`c1`) FROM `t`"),
        ("select bit_and(all c1) from t;", true, "SELECT BIT_AND(`c1`) FROM `t`"),
        ("select bit_and(distinct c1) from t;", false, ""),
        ("select bit_and(distinctrow c1) from t;", false, ""),
        ("select bit_and(distinctrow all c1) from t;", false, ""),
        ("select bit_and(distinct all c1) from t;", false, ""),
        ("select bit_and(), bit_and(distinct c1) from t;", false, ""),
        ("select bit_and(), bit_and(distinctrow c1) from t;", false, ""),
    ]);
}

fn test_builtin_cases_12() {
    run_cases(&[
        ("select bit_and(), bit_and(all c1) from t;", false, ""),
        (
            "select bit_or(c1) from t;",
            true,
            "SELECT BIT_OR(`c1`) FROM `t`",
        ),
        (
            "select bit_or(all c1) from t;",
            true,
            "SELECT BIT_OR(`c1`) FROM `t`",
        ),
        ("select bit_or(distinct c1) from t;", false, ""),
        ("select bit_or(distinctrow c1) from t;", false, ""),
        ("select bit_or(distinctrow all c1) from t;", false, ""),
        ("select bit_or(distinct all c1) from t;", false, ""),
        ("select bit_or(), bit_or(distinct c1) from t;", false, ""),
        ("select bit_or(), bit_or(distinctrow c1) from t;", false, ""),
        ("select bit_or(), bit_or(all c1) from t;", false, ""),
        (
            "select bit_xor(c1) from t;",
            true,
            "SELECT BIT_XOR(`c1`) FROM `t`",
        ),
        (
            "select bit_xor(all c1) from t;",
            true,
            "SELECT BIT_XOR(`c1`) FROM `t`",
        ),
        ("select bit_xor(distinct c1) from t;", false, ""),
        ("select bit_xor(distinctrow c1) from t;", false, ""),
        ("select bit_xor(distinctrow all c1) from t;", false, ""),
        ("select bit_xor(), bit_xor(distinct c1) from t;", false, ""),
        (
            "select bit_xor(), bit_xor(distinctrow c1) from t;",
            false,
            "",
        ),
        ("select bit_xor(), bit_xor(all c1) from t;", false, ""),
        ("select max(c1,c2) from t;", false, ""),
        (
            "select max(distinct c1) from t;",
            true,
            "SELECT MAX(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select max(distinctrow c1) from t;",
            true,
            "SELECT MAX(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select max(distinct all c1) from t;",
            true,
            "SELECT MAX(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select max(distinctrow all c1) from t;",
            true,
            "SELECT MAX(DISTINCT `c1`) FROM `t`",
        ),
        ("select max(c2) from t;", true, "SELECT MAX(`c2`) FROM `t`"),
        ("select min(c1,c2) from t;", false, ""),
        (
            "select min(distinct c1) from t;",
            true,
            "SELECT MIN(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select min(distinctrow c1) from t;",
            true,
            "SELECT MIN(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select min(distinct all c1) from t;",
            true,
            "SELECT MIN(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select min(distinctrow all c1) from t;",
            true,
            "SELECT MIN(DISTINCT `c1`) FROM `t`",
        ),
        ("select min(c2) from t;", true, "SELECT MIN(`c2`) FROM `t`"),
        ("select sum(c1,c2) from t;", false, ""),
        (
            "select sum(distinct c1) from t;",
            true,
            "SELECT SUM(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select sum(distinctrow c1) from t;",
            true,
            "SELECT SUM(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select sum(distinct all c1) from t;",
            true,
            "SELECT SUM(DISTINCT `c1`) FROM `t`",
        ),
        (
            "select sum(distinctrow all c1) from t;",
            true,
            "SELECT SUM(DISTINCT `c1`) FROM `t`",
        ),
        ("select sum(c2) from t;", true, "SELECT SUM(`c2`) FROM `t`"),
        (
            "select count(c1) from t;",
            true,
            "SELECT COUNT(`c1`) FROM `t`",
        ),
        ("select count(distinct *) from t;", false, ""),
        ("select count(distinctrow *) from t;", false, ""),
        ("select count(*) from t;", true, "SELECT COUNT(1) FROM `t`"),
    ]);
}

fn test_builtin_cases_13() {
    run_cases(&[
        ("select count(distinct c1, c2) from t;", true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"),
        ("select count(distinctrow c1, c2) from t;", true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"),
        ("select count(c1, c2) from t;", false, ""),
        ("select count(all c1) from t;", true, "SELECT COUNT(`c1`) FROM `t`"),
        ("select count(distinct all c1) from t;", false, ""),
        ("select count(distinctrow all c1) from t;", false, ""),
        ("select approx_count_distinct(c1) from t;", true, "SELECT APPROX_COUNT_DISTINCT(`c1`) FROM `t`"),
        ("select approx_count_distinct(c1, c2) from t;", true, "SELECT APPROX_COUNT_DISTINCT(`c1`, `c2`) FROM `t`"),
        ("select approx_count_distinct(c1, 123) from t;", true, "SELECT APPROX_COUNT_DISTINCT(`c1`, 123) FROM `t`"),
        ("select approx_percentile(c1) from t;", true, "SELECT APPROX_PERCENTILE(`c1`) FROM `t`"),
        ("select approx_percentile(c1, c2) from t;", true, "SELECT APPROX_PERCENTILE(`c1`, `c2`) FROM `t`"),
        ("select approx_percentile(c1, 123) from t;", true, "SELECT APPROX_PERCENTILE(`c1`, 123) FROM `t`"),
        ("select group_concat(c2,c1) from t group by c1;", true, "SELECT GROUP_CONCAT(`c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"),
        ("select group_concat(c2,c1 SEPARATOR ';') from t group by c1;", true, "SELECT GROUP_CONCAT(`c2`, `c1` SEPARATOR ';') FROM `t` GROUP BY `c1`"),
        ("select group_concat(distinct c2,c1) from t group by c1;", true, "SELECT GROUP_CONCAT(DISTINCT `c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"),
        ("select group_concat(distinctrow c2,c1) from t group by c1;", true, "SELECT GROUP_CONCAT(DISTINCT `c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"),
        ("SELECT student_name, GROUP_CONCAT(DISTINCT test_score ORDER BY test_score DESC SEPARATOR ' ') FROM student GROUP BY student_name;", true, "SELECT `student_name`,GROUP_CONCAT(DISTINCT `test_score` ORDER BY `test_score` DESC SEPARATOR ' ') FROM `student` GROUP BY `student_name`"),
        ("select std(c1), std(all c1), std(distinct c1) from t", true, "SELECT STDDEV_POP(`c1`),STDDEV_POP(`c1`),STDDEV_POP(DISTINCT `c1`) FROM `t`"),
        ("select std(c1, c2) from t", false, ""),
        ("select stddev(c1), stddev(all c1), stddev(distinct c1) from t", true, "SELECT STDDEV_POP(`c1`),STDDEV_POP(`c1`),STDDEV_POP(DISTINCT `c1`) FROM `t`"),
        ("select stddev(c1, c2) from t", false, ""),
        ("select stddev_pop(c1), stddev_pop(all c1), stddev_pop(distinct c1) from t", true, "SELECT STDDEV_POP(`c1`),STDDEV_POP(`c1`),STDDEV_POP(DISTINCT `c1`) FROM `t`"),
        ("select stddev_pop(c1, c2) from t", false, ""),
        ("select stddev_samp(c1), stddev_samp(all c1), stddev_samp(distinct c1) from t", true, "SELECT STDDEV_SAMP(`c1`),STDDEV_SAMP(`c1`),STDDEV_SAMP(DISTINCT `c1`) FROM `t`"),
        ("select stddev_samp(c1, c2) from t", false, ""),
        ("select variance(c1), variance(all c1), variance(distinct c1) from t", true, "SELECT VAR_POP(`c1`),VAR_POP(`c1`),VAR_POP(DISTINCT `c1`) FROM `t`"),
        ("select variance(c1, c2) from t", false, ""),
        ("select var_pop(c1), var_pop(all c1), var_pop(distinct c1) from t", true, "SELECT VAR_POP(`c1`),VAR_POP(`c1`),VAR_POP(DISTINCT `c1`) FROM `t`"),
        ("select var_pop(c1, c2) from t", false, ""),
        ("select var_samp(c1), var_samp(all c1), var_samp(distinct c1) from t", true, "SELECT VAR_SAMP(`c1`),VAR_SAMP(`c1`),VAR_SAMP(DISTINCT `c1`) FROM `t`"),
        ("select var_samp(c1, c2) from t", false, ""),
        ("select json_arrayagg(c2) from t group by c1", true, "SELECT JSON_ARRAYAGG(`c2`) FROM `t` GROUP BY `c1`"),
        ("select json_arrayagg(c1, c2) from t group by c1", false, ""),
        ("select json_arrayagg(distinct c2) from t group by c1", false, "SELECT JSON_ARRAYAGG(DISTINCT `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_arrayagg(all c2) from t group by c1", true, "SELECT JSON_ARRAYAGG(`c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(c1, c2) from t group by c1", true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(c1, c2, c3) from t group by c1", false, ""),
        ("select json_objectagg(distinct c1, c2) from t group by c1", false, "SELECT JSON_OBJECTAGG(DISTINCT `c1`, `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(c1, distinct c2) from t group by c1", false, "SELECT JSON_OBJECTAGG(`c1`, DISTINCT `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(distinct c1, distinct c2) from t group by c1", false, "SELECT JSON_OBJECTAGG(DISTINCT `c1`, DISTINCT `c2`) FROM `t` GROUP BY `c1`"),
    ]);
}

fn test_builtin_cases_14() {
    run_cases(&[
        ("select json_objectagg(all c1, c2) from t group by c1", true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(c1, all c2) from t group by c1", true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"),
        ("select json_objectagg(all c1, all c2) from t group by c1", true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"),
        ("select AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3'))", true, "SELECT AES_ENCRYPT(_UTF8MB4'text', UNHEX(_UTF8MB4'F3229A0B371ED2D9441B830D21A390C3'))"),
        ("select AES_DECRYPT(@crypt_str,@key_str)", true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`)"),
        ("select AES_DECRYPT(@crypt_str,@key_str,@init_vector);", true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`, @`init_vector`)"),
        ("SELECT COMPRESS('');", true, "SELECT COMPRESS(_UTF8MB4'')"),
        ("SELECT DECODE(@crypt_str, @pass_str);", true, "SELECT DECODE(@`crypt_str`, @`pass_str`)"),
        ("SELECT DES_DECRYPT(@crypt_str), DES_DECRYPT(@crypt_str, @key_str);", true, "SELECT DES_DECRYPT(@`crypt_str`),DES_DECRYPT(@`crypt_str`, @`key_str`)"),
        ("SELECT DES_ENCRYPT(@str), DES_ENCRYPT(@key_num);", true, "SELECT DES_ENCRYPT(@`str`),DES_ENCRYPT(@`key_num`)"),
        ("SELECT ENCODE('cleartext', CONCAT('my_random_salt','my_secret_password'));", true, "SELECT ENCODE(_UTF8MB4'cleartext', CONCAT(_UTF8MB4'my_random_salt', _UTF8MB4'my_secret_password'))"),
        ("SELECT ENCRYPT('hello'), ENCRYPT('hello', @salt);", true, "SELECT ENCRYPT(_UTF8MB4'hello'),ENCRYPT(_UTF8MB4'hello', @`salt`)"),
        ("SELECT MD5('testing');", true, "SELECT MD5(_UTF8MB4'testing')"),
        ("SELECT OLD_PASSWORD(@str);", true, "SELECT OLD_PASSWORD(@`str`)"),
        ("SELECT PASSWORD(@str);", true, "SELECT PASSWORD(@`str`)"),
        ("SELECT RANDOM_BYTES(@len);", true, "SELECT RANDOM_BYTES(@`len`)"),
        ("SELECT SHA1('abc');", true, "SELECT SHA1(_UTF8MB4'abc')"),
        ("SELECT SHA('abc');", true, "SELECT SHA(_UTF8MB4'abc')"),
        ("SELECT SHA2('abc', 224);", true, "SELECT SHA2(_UTF8MB4'abc', 224)"),
        ("SELECT SM3('abc');", true, "SELECT SM3(_UTF8MB4'abc')"),
        ("SELECT UNCOMPRESS('any string');", true, "SELECT UNCOMPRESS(_UTF8MB4'any string')"),
        ("SELECT UNCOMPRESSED_LENGTH(@compressed_string);", true, "SELECT UNCOMPRESSED_LENGTH(@`compressed_string`)"),
        ("SELECT VALIDATE_PASSWORD_STRENGTH(@str);", true, "SELECT VALIDATE_PASSWORD_STRENGTH(@`str`)"),
        ("SELECT JSON_EXTRACT();", true, "SELECT JSON_EXTRACT()"),
        ("SELECT JSON_UNQUOTE();", true, "SELECT JSON_UNQUOTE()"),
        ("SELECT JSON_TYPE('[123]');", true, "SELECT JSON_TYPE(_UTF8MB4'[123]')"),
        ("SELECT JSON_TYPE();", true, "SELECT JSON_TYPE()"),
        ("SELECT a->'$.a' FROM t", true, "SELECT JSON_EXTRACT(`a`, _UTF8MB4'$.a') FROM `t`"),
        ("SELECT a->>'$.a' FROM t", true, "SELECT JSON_UNQUOTE(JSON_EXTRACT(`a`, _UTF8MB4'$.a')) FROM `t`"),
        ("SELECT '{}'->'$.a' FROM t", false, ""),
        ("SELECT '{}'->>'$.a' FROM t", false, ""),
        ("SELECT a->3 FROM t", false, ""),
        ("SELECT a->>3 FROM t", false, ""),
        ("SELECT 1 member of (a)", true, "SELECT 1 MEMBER OF (`a`)"),
        ("SELECT 1 member of a", false, ""),
        ("SELECT 1 member a", false, ""),
        ("SELECT 1 not member of a", false, ""),
        ("SELECT 1 member of (1+1)", false, ""),
        ("SELECT concat('a') member of (cast(1 as char(1)))", true, "SELECT CONCAT(_UTF8MB4'a') MEMBER OF (CAST(1 AS CHAR(1)))"),
        ("SELECT `uuid`()", true, "SELECT UUID()"),
    ]);
}

fn test_builtin_cases_15() {
    run_cases(&[
        ("select nextval(seq)", true, "SELECT NEXTVAL(`seq`)"),
        ("select lastval(seq)", true, "SELECT LASTVAL(`seq`)"),
        ("select setval(seq, 100)", true, "SELECT SETVAL(`seq`, 100)"),
        ("select next value for seq", true, "SELECT NEXTVAL(`seq`)"),
        (
            "select next value for sequence",
            true,
            "SELECT NEXTVAL(`sequence`)",
        ),
        (
            "select NeXt vAluE for seQuEncE2",
            true,
            "SELECT NEXTVAL(`seQuEncE2`)",
        ),
        (
            "select regexp_like('aBc', 'abc', 'im');",
            true,
            "SELECT REGEXP_LIKE(_UTF8MB4'aBc', _UTF8MB4'abc', _UTF8MB4'im')",
        ),
        (
            "select regexp_substr('aBc', 'abc', 1, 1, 'im');",
            true,
            "SELECT REGEXP_SUBSTR(_UTF8MB4'aBc', _UTF8MB4'abc', 1, 1, _UTF8MB4'im')",
        ),
        (
            "select regexp_instr('aBc', 'abc', 1, 1, 0, 'im');",
            true,
            "SELECT REGEXP_INSTR(_UTF8MB4'aBc', _UTF8MB4'abc', 1, 1, 0, _UTF8MB4'im')",
        ),
        (
            "select regexp_replace('aBc', 'abc', 'def', 1, 1, 'i');",
            true,
            "SELECT REGEXP_REPLACE(_UTF8MB4'aBc', _UTF8MB4'abc', _UTF8MB4'def', 1, 1, _UTF8MB4'i')",
        ),
        (
            "select 'aBc' ilike 'abc';",
            true,
            "SELECT _UTF8MB4'aBc' ILIKE _UTF8MB4'abc'",
        ),
    ]);
}

#[test]
fn test_builtin() {
    test_builtin_cases_0();
    test_builtin_cases_1();
    test_builtin_cases_2();
    test_builtin_cases_3();
    test_builtin_cases_4();
    test_builtin_cases_5();
    test_builtin_cases_6();
    test_builtin_cases_7();
    test_builtin_cases_8();
    test_builtin_cases_9();
    test_builtin_cases_10();
    test_builtin_cases_11();
    test_builtin_cases_12();
    test_builtin_cases_13();
    test_builtin_cases_14();
    test_builtin_cases_15();
}
