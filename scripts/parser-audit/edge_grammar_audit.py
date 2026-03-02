#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Deep Grammar Parity Audit - Phase 2: Edge Cases & Grammar Alternative Analysis

Focus on:
1. Subtle syntax alternatives within each grammar rule
2. Error cases that SHOULD fail but may pass incorrectly
3. Complex combinations of clauses
4. Obscure but valid SQL that may be missing
"""

import re
import subprocess
import os
import json
import sys
from collections import defaultdict

PARSER_DIR = "/Users/qiliu/projects/tidb/pkg/parser"
ORIGIN_Y = "/Users/qiliu/.gemini/antigravity/brain/cb2a854e-5e7a-4815-9dee-cda5c836a2d1/parser_origin.y"

GO_TEST_TEMPLATE = '''package parser

import (
	"strings"
	"testing"
)

func TestAuditEdge_{label}(t *testing.T) {{
	type tc struct {{
		sql string
		tag string
		shouldFail bool
	}}
	cases := []tc{{
{cases}
	}}
	for _, c := range cases {{
		t.Run(c.tag, func(t *testing.T) {{
			hp := NewHandParser()
			scanner := NewScanner(c.sql)
			hp.Init(scanner, c.sql)
			_, _, err := hp.ParseSQL()
			if c.shouldFail {{
				if err == nil {{
					t.Errorf("SHOULD_FAIL [%s]: expected error for: %s", c.tag, c.sql)
				}}
			}} else {{
				if err != nil {{
					// Only log if not empty
					msg := err.Error()
					_ = strings.TrimSpace(msg)
					t.Errorf("FAIL [%s]: %v\\n  SQL: %s", c.tag, err, c.sql)
				}}
			}}
		}})
	}}
}}
'''

def fmt_case(sql, tag, should_fail=False):
    fail_str = "true" if should_fail else "false"
    return f'\t\t{{{json.dumps(sql)}, {json.dumps(tag)}, {fail_str}}},'


TEST_GROUPS = {

"SelectHints": [
    # Optimizer hints - these have complex syntax
    (False, "SELECT /*+ USE_INDEX(t, idx) */ * FROM t", "use_index hint"),
    (False, "SELECT /*+ IGNORE_INDEX(t, idx) */ * FROM t", "ignore_index hint"),
    (False, "SELECT /*+ FORCE_INDEX(t, idx) */ * FROM t", "force_index hint"),
    (False, "SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1, t2", "hash_join hint"),
    (False, "SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t1, t2", "merge_join hint"),
    (False, "SELECT /*+ INL_JOIN(t2) */ * FROM t1 JOIN t2 ON t1.id=t2.id", "inl_join hint"),
    (False, "SELECT /*+ INL_HASH_JOIN(t2) */ * FROM t1 JOIN t2 ON t1.id=t2.id", "inl_hash_join hint"),
    (False, "SELECT /*+ HASH_AGG() */ COUNT(*) FROM t GROUP BY a", "hash_agg hint"),
    (False, "SELECT /*+ STREAM_AGG() */ COUNT(*) FROM t GROUP BY a", "stream_agg hint"),
    (False, "SELECT /*+ AGG_TO_COP() */ COUNT(*) FROM t GROUP BY a", "agg_to_cop hint"),
    (False, "SELECT /*+ SET_VAR(max_execution_time=5000) */ * FROM t", "set_var hint"),
    (False, "SELECT /*+ QB_NAME(q1) */ * FROM t /*+ LEADING(t) */", "qb_name+leading hint"),
    (False, "SELECT /*+ NO_REORDER() */ * FROM t1, t2", "no_reorder hint"),
    (False, "SELECT /*+ MAX_EXECUTION_TIME(5000) */ * FROM t", "max_execution_time hint"),
    (False, "SELECT * FROM t /*+ RESOURCE_GROUP(rg1) */ WHERE 1", "resource_group hint"),
],

"SelectLockParts": [
    # Locking granularity
    (False, "SELECT * FROM t FOR UPDATE NOWAIT", "for update nowait"),
    (False, "SELECT * FROM t FOR UPDATE SKIP LOCKED", "for update skip locked"),
    (False, "SELECT * FROM t FOR SHARE NOWAIT", "for share nowait"),
    (False, "SELECT * FROM t FOR SHARE SKIP LOCKED", "for share skip locked"),
    (False, "SELECT * FROM t FOR UPDATE OF t1", "for update of t1"),
    (False, "SELECT * FROM t LOCK IN SHARE MODE", "lock in share mode"),
    # INTO OUTFILE options
    (False, "SELECT * FROM t INTO OUTFILE '/tmp/out.csv' FIELDS TERMINATED BY ','", "outfile fields terminated"),
    (False, "SELECT * FROM t INTO OUTFILE '/tmp/out.csv' LINES TERMINATED BY '\\n'", "outfile lines terminated"),
    (False, "SELECT * FROM t INTO OUTFILE '/tmp/out.csv' CHARACTER SET utf8mb4", "outfile charset"),
    (False, "SELECT * FROM t INTO DUMPFILE '/tmp/dump.dat'", "into dumpfile"),
    (False, "SELECT @x := 1 INTO @y FROM t", "into user var"),
],

"SelectFrom": [
    (False, "SELECT * FROM t AS t1 FORCE INDEX (PRIMARY)", "force index primary"),
    (False, "SELECT * FROM t USE INDEX FOR JOIN (idx)", "use index for join"),
    (False, "SELECT * FROM t USE INDEX FOR ORDER BY (idx)", "use index for order by"),
    (False, "SELECT * FROM t USE INDEX FOR GROUP BY (idx)", "use index for group by"),
    # Sampling clause
    (False, "SELECT * FROM t TABLESAMPLE BERNOULLI(10 PERCENT)", "tablesample bernoulli"),
    (False, "SELECT * FROM t TABLESAMPLE SYSTEM(10 PERCENT)", "tablesample system"),
    (False, "SELECT * FROM t TABLESAMPLE BERNOULLI(100 ROWS)", "tablesample rows"),
    # AS OF
    (False, "SELECT * FROM t AS OF TIMESTAMP '2023-01-01'", "as of timestamp"),
    # LATERAL DERIVED
    (False, "SELECT t1.a, t2.b FROM t1, LATERAL (SELECT b FROM t2 WHERE t2.a = t1.a LIMIT 1) t2", "lateral derived"),
    # JSON_TABLE  
    (False, "SELECT * FROM JSON_TABLE('[{\"a\":1}]', '$[*]' COLUMNS(a INT PATH '$.a')) jt", "json_table"),
],

"GroupByRollup": [
    (False, "SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP", "group by rollup"),
    (False, "SELECT GROUPING(a) FROM t GROUP BY a WITH ROLLUP", "grouping function"),
],

"WindowFunctions": [
    (False, "SELECT NTILE(4) OVER (PARTITION BY a ORDER BY b) FROM t", "ntile"),
    (False, "SELECT LEAD(a, 1) OVER (ORDER BY b) FROM t", "lead"),
    (False, "SELECT LAG(a, 1, 0) OVER (ORDER BY b) FROM t", "lag with default"),
    (False, "SELECT FIRST_VALUE(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t", "first_value"),
    (False, "SELECT LAST_VALUE(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t", "last_value"),
    (False, "SELECT NTH_VALUE(a, 2) OVER (ORDER BY b) FROM t", "nth_value"),
    (False, "SELECT CUME_DIST() OVER (PARTITION BY a ORDER BY b) FROM t", "cume_dist"),
    (False, "SELECT PERCENT_RANK() OVER (PARTITION BY a ORDER BY b) FROM t", "percent_rank"),
    (False, "SELECT SUM(a) OVER (PARTITION BY b ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t", "sum window rows"),
    (False, "SELECT SUM(a) OVER (PARTITION BY b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t", "sum window range"),
    (False, "SELECT SUM(a) OVER (PARTITION BY b GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t", "sum window groups"),
    (False, "SELECT SUM(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)", "named window def"),
    (False, "SELECT SUM(a) OVER w FROM t WINDOW w AS (b ORDER BY c)", "window inherit"),
],

"Expressions": [
    # Unary operators
    (False, "SELECT -a FROM t", "unary minus"),
    (False, "SELECT +a FROM t", "unary plus"),  
    (False, "SELECT ~a FROM t", "bitwise not"),
    (False, "SELECT !a FROM t", "logical not !"),
    (False, "SELECT NOT a FROM t", "logical not NOT"),
    # Operators
    (False, "SELECT a DIV b FROM t", "div"),
    (False, "SELECT a MOD b FROM t", "mod op"),
    (False, "SELECT a & b FROM t", "bitand"),
    (False, "SELECT a | b FROM t", "bitor"),
    (False, "SELECT a ^ b FROM t", "bitxor"),
    (False, "SELECT a << 2 FROM t", "lshift"),
    (False, "SELECT a >> 2 FROM t", "rshift"),
    # Collation
    (False, "SELECT a = b COLLATE utf8mb4_bin FROM t", "collate in expr"),
    (False, "SELECT a COLLATE utf8mb4_bin FROM t", "collate expr"),
    # ILIKE
    (False, "SELECT a ILIKE 'b%' FROM t", "ilike"),
    # Regex
    (False, "SELECT a REGEXP BINARY 'b.*' FROM t", "regexp binary"),
    # Assignment
    (False, "SELECT @a := 1", "user var assign"),
    (False, "SELECT @a = 1 FROM t", "user var compare"),
    # Row expression
    (False, "SELECT ROW(1, 2) = ROW(1, 2) FROM t", "row compare"),
    (False, "SELECT (1, 2) IN (SELECT a, b FROM t)", "row in subquery"),
    # MATCH AGAINST
    (False, "SELECT MATCH(a, b) AGAINST ('text') FROM t", "match against"),
    (False, "SELECT MATCH(a) AGAINST ('text' IN BOOLEAN MODE) FROM t", "match against boolean"),
    (False, "SELECT MATCH(a) AGAINST ('text' WITH QUERY EXPANSION) FROM t", "match with query expansion"),
    # Interval expression  
    (False, "SELECT INTERVAL 1 DAY + '2023-01-01'", "interval expr"),
    # VARCHAR/CHAR function
    (False, "SELECT CHAR(65, 66) FROM t", "char()"),
    # Next value for
    (False, "SELECT NEXT VALUE FOR s", "next value for"),
    (False, "SELECT NEXTVAL(s)", "nextval func"),
    # System time functions without ()
    (False, "SELECT CURRENT_DATE FROM t", "current_date no parens"),
    (False, "SELECT CURRENT_TIME FROM t", "current_time no parens"),
    (False, "SELECT CURRENT_TIMESTAMP FROM t", "current_timestamp no parens"),
    (False, "SELECT LOCALTIME FROM t", "localtime no parens"),
    (False, "SELECT LOCALTIMESTAMP FROM t", "localtimestamp no parens"),
    # TRANSLATE
    (False, "SELECT TRANSLATE('hello', 'hel', 'bye') FROM t", "translate"),
    # COMPRESS
    (False, "SELECT COMPRESS('data') FROM t", "compress"),
],

"InsertEdge": [
    # SELECT with union in insert
    (False, "INSERT INTO t SELECT 1 UNION SELECT 2", "insert union select"),
    (False, "INSERT INTO t SELECT 1 UNION ALL SELECT 2", "insert union all select"),
    # ROW() in values
    (False, "INSERT INTO t VALUES ROW(1, 2), ROW(3, 4)", "insert values row()"),
    # Partition selection
    (False, "INSERT INTO t PARTITION (p1) VALUES (1)", "insert partition"),
    # AS alias for on duplicate key update  
    (False, "INSERT INTO t (a, b) VALUES (1, 2) AS new ON DUPLICATE KEY UPDATE a = new.a", "insert values alias"),
],

"UpdateEdge": [
    # Join UPDATE
    (False, "UPDATE t1 JOIN t2 ON t1.id=t2.id SET t1.a=t2.a WHERE t1.b=1", "join update"),
    (False, "UPDATE t1 LEFT JOIN t2 ON t1.id=t2.id SET t1.a=COALESCE(t2.a, 0)", "left join update"),
    # Partition  
    (False, "UPDATE t PARTITION (p1) SET a=1 WHERE b=2", "update partition"),
    # Multi-table no WHERE
    (False, "UPDATE t1 INNER JOIN t2 USING (id) SET t1.a=1", "multi-table update using"),
],

"DeleteEdge": [
    # Multi-table delete with alias
    (False, "DELETE t1 FROM t1 AS t INNER JOIN t2 AS s ON t.id=s.id WHERE t.a=1", "delete aliased tables"),
    # Partition delete
    (False, "DELETE FROM t PARTITION (p1) WHERE a=1", "delete partition"),
    # Multi-table using multiple tables
    (False, "DELETE t1, t2 FROM t1 INNER JOIN t2 ON t1.id=t2.id WHERE t1.a=1", "delete multi-table list"),
],

"CreateTableEdge": [
    # Column options
    (False, "CREATE TABLE t (a INT NOT NULL DEFAULT 0 AUTO_INCREMENT PRIMARY KEY COMMENT 'id')", "create col all options"),
    (False, "CREATE TABLE t (a VARCHAR(255) BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)", "create col binary charset"),
    (False, "CREATE TABLE t (a INT GENERATED ALWAYS AS (b + 1) VIRTUAL)", "generated virtual"),
    (False, "CREATE TABLE t (a INT GENERATED ALWAYS AS (b + 1) STORED)", "generated stored"),
    (False, "CREATE TABLE t (a INT INVISIBLE)", "invisible column"),
    (False, "CREATE TABLE t (a VECTOR(3))", "vector column"),
    (False, "CREATE TABLE t (a INT CHECK (a > 0))", "column check constraint"),
    (False, "CREATE TABLE t (a INT, CHECK (a > 0))", "table check constraint"),
    (False, "CREATE TABLE t (a INT, b INT, UNIQUE KEY uq (a, b))", "unique key"),
    (False, "CREATE TABLE t (a INT, b INT, INDEX idx (a) USING BTREE COMMENT 'cidx')", "index with options"),
    (False, "CREATE TABLE t (a INT, FULLTEXT INDEX ft (a) WITH PARSER ngram)", "fulltext with parser"),
    (False, "CREATE TABLE t (a INT, PRIMARY KEY (a) USING BTREE)", "pk using btree"),
    (False, "CREATE TABLE t (a INT) AUTO_INCREMENT=1000 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='my table'", "table options"),
    (False, "CREATE TABLE t (a INT) PLACEMENT POLICY=p1", "placement policy"),
    (False, "CREATE TABLE t (a INT) TTL = `a` + INTERVAL 1 YEAR TTL_ENABLE = 'OFF'", "ttl option"),
    (False, "CREATE TABLE t (a INT) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=2", "shard+pre_split"),
    # Column types
    (False, "CREATE TABLE t (a TINYINT, b SMALLINT, c MEDIUMINT, d INT, e BIGINT)", "int types"),
    (False, "CREATE TABLE t (a FLOAT, b DOUBLE, c REAL, d DECIMAL(10,2))", "float types"),
    (False, "CREATE TABLE t (a CHAR(10), b VARCHAR(255), c TEXT, d TINYTEXT, e MEDIUMTEXT, f LONGTEXT)", "string types"),
    (False, "CREATE TABLE t (a BINARY(10), b VARBINARY(255), c BLOB, d TINYBLOB, e MEDIUMBLOB, f LONGBLOB)", "binary types"),
    (False, "CREATE TABLE t (a DATE, b TIME, c DATETIME, d TIMESTAMP, e YEAR)", "date types"),
    (False, "CREATE TABLE t (a ENUM('x', 'y', 'z'))", "enum type"),
    (False, "CREATE TABLE t (a SET('x', 'y', 'z'))", "set type"),
    (False, "CREATE TABLE t (a JSON)", "json type"),
    (False, "CREATE TABLE t (a BIT(8))", "bit type"),
    (False, "CREATE TABLE t (a VECTOR(3))", "vector type"),
    # Partitoning subtleties
    (False, "CREATE TABLE t (a INT, b INT) PARTITION BY RANGE COLUMNS(a, b) (PARTITION p0 VALUES LESS THAN (10, 10))", "range columns partition"),
    (False, "CREATE TABLE t (a INT) PARTITION BY LIST COLUMNS(a) (PARTITION p0 VALUES IN (1, 2, NULL))", "list columns partition"),
    (False, "CREATE TABLE t (a INT) PARTITION BY KEY() PARTITIONS 4", "key() no cols partition"),
    (False, "CREATE TABLE t (a INT) PARTITION BY LINEAR HASH(a) PARTITIONS 4", "linear hash partition"),
    (False, "CREATE TABLE t (a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10) COMMENT 'c' ENGINE=InnoDB)", "partition with options"),
    # FOREIGN KEY
    (False, "CREATE TABLE t (a INT, FOREIGN KEY (a) REFERENCES r(id) ON DELETE CASCADE ON UPDATE SET NULL)", "fk on delete cascade"),
    (False, "CREATE TABLE t (a INT, FOREIGN KEY (a) REFERENCES r(id) ON DELETE RESTRICT ON UPDATE RESTRICT)", "fk restrict"),
],

"AlterTableEdge": [
    (False, "ALTER TABLE t ADD COLUMN a INT FIRST", "alter add col first"),
    (False, "ALTER TABLE t ADD COLUMN a INT AFTER b", "alter add col after"),
    (False, "ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b VARCHAR(10)", "alter multi add"),
    (False, "ALTER TABLE t MODIFY COLUMN a BIGINT NOT NULL DEFAULT 0", "alter modify with options"),
    (False, "ALTER TABLE t CHANGE COLUMN a b BIGINT FIRST", "alter change first"),
    (False, "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5", "alter column set default"),
    (False, "ALTER TABLE t ALTER COLUMN a DROP DEFAULT", "alter column drop default"),
    (False, "ALTER TABLE t RENAME COLUMN a TO b", "alter rename column"),
    (False, "ALTER TABLE t ADD FULLTEXT INDEX ft (a) WITH PARSER ngram", "alter add fulltext"),
    (False, "ALTER TABLE t ADD SPATIAL INDEX si (a)", "alter add spatial"),
    (False, "ALTER TABLE t ADD CHECK (a > 0)", "alter add check"),
    (False, "ALTER TABLE t DROP CHECK ck", "alter drop check"),
    (False, "ALTER TABLE t ALTER CHECK ck NOT ENFORCED", "alter check not enforced"),
    (False, "ALTER TABLE t DROP FOREIGN KEY fk", "alter drop fk"),
    (False, "ALTER TABLE t DISABLE KEYS", "alter disable keys"),
    (False, "ALTER TABLE t ENABLE KEYS", "alter enable keys"),
    (False, "ALTER TABLE t ORDER BY a ASC, b DESC", "alter order by"),
    (False, "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4", "alter convert charset"),
    (False, "ALTER TABLE t DEFAULT CHARACTER SET = utf8mb4", "alter default charset"),
    (False, "ALTER TABLE t COLLATE = utf8mb4_general_ci", "alter collate"),
    (False, "ALTER TABLE t AUTO_ID_CACHE 1", "alter auto_id_cache"),
    (False, "ALTER TABLE t AUTO_RANDOM_BASE = 100", "alter auto_random_base"),
    (False, "ALTER TABLE t COMMENT = 'new comment'", "alter comment"),
    (False, "ALTER TABLE t PACK_KEYS = 1", "alter pack_keys"),
    (False, "ALTER TABLE t ROW_FORMAT = COMPRESSED", "alter row_format"),
    (False, "ALTER TABLE t COALESCE PARTITION 2", "alter coalesce partition"),
    (False, "ALTER TABLE t ANALYZE PARTITION p1", "alter analyze partition"),
    (False, "ALTER TABLE t CHECK PARTITION p1", "alter check partition"),
    (False, "ALTER TABLE t OPTIMIZE PARTITION p1", "alter optimize partition"),
    (False, "ALTER TABLE t REBUILD PARTITION p1", "alter rebuild partition"),
    (False, "ALTER TABLE t REPAIR PARTITION p1", "alter repair partition"),
    (False, "ALTER TABLE t IMPORT PARTITION p1 TABLESPACE", "alter import partition tablespace"),
    (False, "ALTER TABLE t DISCARD PARTITION p1 TABLESPACE", "alter discard partition tablespace"),
    (False, "ALTER TABLE t IMPORT TABLESPACE", "alter import tablespace"),
    (False, "ALTER TABLE t DISCARD TABLESPACE", "alter discard tablespace"),
    (False, "ALTER TABLE t ADD PARTITION PARTITIONS 5", "alter add partition count"),
    (False, "ALTER TABLE t PARTITION BY HASH(a % 8) PARTITIONS 8", "alter repartition"),
    (False, "ALTER TABLE t PLACEMENT POLICY = p1", "alter placement policy"),
    (False, "ALTER TABLE t TTL = `ts` + INTERVAL 1 DAY TTL_ENABLE = 'ON'", "alter ttl"),
    (False, "ALTER TABLE t REMOVE TTL", "alter remove ttl"),
    (False, "ALTER TABLE t WITH VALIDATION", "alter with validation"),
    (False, "ALTER TABLE t WITHOUT VALIDATION", "alter without validation"),
    (False, "ALTER TABLE t EXCHANGE PARTITION p1 WITH TABLE t2 WITHOUT VALIDATION", "alter exchange partition no validation"),
    (False, "ALTER TABLE t ATTRIBUTES='merge_option=allow'", "alter attributes"),
    (False, "ALTER TABLE t PARTITION p1 ATTRIBUTES='merge_option=allow'", "alter partition attributes"),
],

"CreateIndexEdge": [
    (False, "CREATE INDEX idx ON t (a DESC, b ASC)", "create index col ordering"),
    (False, "CREATE INDEX idx ON t (a(10))", "create index prefix length"),
    (False, "CREATE INDEX idx ON t (a) KEY_BLOCK_SIZE=4 COMMENT 'c'", "create index options"),
    (False, "CREATE INDEX idx ON t (a) INVISIBLE", "create invisible index"),
    (False, "CREATE INDEX idx ON t (a) VISIBLE", "create visible index"),
    (False, "CREATE SPATIAL INDEX si ON t (a)", "create spatial index"),
    (False, "CREATE INDEX idx ON t ((CAST(JSON_EXTRACT(doc, '$.a') AS CHAR(100))))", "create expr index"),
    (False, "CREATE INDEX idx ON t (a) ALGORITHM=INPLACE LOCK=SHARED", "create index with alg+lock"),
    (False, "CREATE INDEX idx ON t (a) GLOBAL", "create global index"),
    (False, "CREATE UNIQUE INDEX idx ON t (a) CLUSTERED", "create clustered unique index"),
],

"SetStmtEdge": [
    # SET CONFIG variations
    (False, "SET CONFIG tidb max_proc_insert_concurrency = 4", "set config tidb identifier"),
    (False, "SET CONFIG 'tidb-server1' max_proc_insert_concurrency = 4", "set config instance str"),
    # Multiple assignments
    (False, "SET a = 1, b = 2, @c = 3", "set multiple vars"),
    (False, "SET @@SESSION.a = 1, @@GLOBAL.b = 2", "set session+global vars"),
    # Expressions in SET
    (False, "SET a = a + 1", "set expr"),
    (False, "SET a = DEFAULT", "set default"),
    (False, "SET a = ON", "set ON value"),
    (False, "SET a = OFF", "set OFF value"),
],

"AdminEdge": [
    (False, "ADMIN SHOW DDL JOBS WHERE state='running'", "admin show ddl jobs where"),
    (False, "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 5", "admin show ddl job queries limit"),
    (False, "ADMIN SHOW DDL JOB QUERIES LIMIT 10", "admin show ddl job queries limit only"),
    (False, "ADMIN CHECK TABLE t, t2", "admin check multi table"),
    (False, "ADMIN SPLIT TABLE t BY (1), (2), (3)", "admin split table by"),
    (False, "ADMIN SPLIT TABLE t INDEX idx BY (1), (2)", "admin split index by"),
    (False, "ADMIN ALTER DDL JOBS 1 THREAD=4", "admin alter ddl thread"),
    (False, "ADMIN ALTER DDJ JOBS 1, 2 THREAD=4 BATCH_SIZE=100", "admin alter ddl multi"),
    (False, "ADMIN CLEANUP TABLE LOCK t", "admin cleanup table lock"),
    (False, "ADMIN SET BDR ROLE PRIMARY", "admin set bdr role"),
    (False, "ADMIN SHOW BDR ROLE", "admin show bdr role"),
],

"ExplainEdge": [
    (False, "EXPLAIN FORMAT='VERBOSE' SELECT * FROM t", "explain format verbose"),
    (False, "EXPLAIN FORMAT='TIDB_JSON' SELECT * FROM t", "explain format tidb_json"),
    (False, "EXPLAIN FORMAT=TRADITIONAL SELECT * FROM t", "explain format traditional"),
    (False, "EXPLAIN FORMAT=VERBOSE ANALYZE SELECT * FROM t", "explain format verbose analyze"),
    (False, "EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM t", "explain analyze format json"),
],

"GrantRevokeEdge": [
    (False, "GRANT SELECT (a, b) ON t TO 'u'@'h'", "grant column priv"),
    (False, "GRANT EXECUTE ON PROCEDURE p TO 'u'@'h'", "grant execute on procedure"),
    (False, "GRANT EXECUTE ON FUNCTION f TO 'u'@'h'", "grant execute on function"),
    (False, "GRANT 'r1', 'r2' TO 'u1'@'h1', 'u2'@'h2'", "grant multi roles to multi users"),
    (False, "REVOKE GRANT OPTION FOR SELECT ON t FROM 'u'@'h'", "revoke grant option"),
    (False, "REVOKE 'r1' FROM 'u'@'h'", "revoke role"),
    (False, "REVOKE 'r1', 'r2' FROM 'u1'@'h1', 'u2'@'h2'", "revoke multi roles"),
    (False, "GRANT ALL ON *.* TO 'u'@'h' REQUIRE SSL", "grant require ssl"),
    (False, "GRANT ALL ON *.* TO 'u'@'h' REQUIRE X509", "grant require x509"),
    (False, "GRANT ALL ON *.* TO 'u'@'h' REQUIRE CIPHER 'AES256-SHA'", "grant require cipher"),
    (False, "GRANT ALL ON *.* TO 'u'@'h' WITH MAX_USER_CONNECTIONS 5", "grant with max conn"),
],

"AnalyzeEdge": [
    (False, "ANALYZE TABLE t UPDATE HISTOGRAM ON a WITH 256 BUCKETS", "analyze update histogram"),
    (False, "ANALYZE TABLE t DROP HISTOGRAM ON a", "analyze drop histogram"),
    (False, "ANALYZE TABLE t WITH 1 CMSKETCH DEPTH 8 WIDTH 512", "analyze with cmsketch"),
    (False, "ANALYZE TABLE t COLUMNS a, b INDEX idx1", "analyze cols + index"),
    # Multiple tables
    (False, "ANALYZE TABLE t1, t2, t3", "analyze multiple tables"),
],

"AlterDatabaseEdge": [
    (False, "ALTER DATABASE mydb CHARACTER SET utf8mb4", "alter db charset"),
    (False, "ALTER DATABASE mydb COLLATE utf8mb4_unicode_ci", "alter db collate"),
    (False, "ALTER SCHEMA mydb DEFAULT CHARACTER SET = utf8mb4", "alter schema charset"),
    (False, "ALTER DATABASE mydb PLACEMENT POLICY = p1", "alter db placement"),
    (False, "ALTER DATABASE mydb ENCRYPTION = 'y'", "alter db encryption"),
],

"AlterUserEdge": [
    (False, "ALTER USER 'u'@'h' PASSWORD HISTORY 3", "alter user pw history"),
    (False, "ALTER USER 'u'@'h' PASSWORD REUSE INTERVAL 90 DAY", "alter user pw reuse interval"),
    (False, "ALTER USER 'u'@'h' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 2", "alter user login attempts"),
    (False, "ALTER USER 'u'@'h' IDENTIFIED WITH 'caching_sha2_password' BY 'pwd' ACCOUNT UNLOCK", "alter user identified unlock"),
    (False, "ALTER USER 'u'@'h' REQUIRE NONE", "alter user require none"),
    (False, "ALTER USER 'u'@'h' WITH MAX_USER_CONNECTIONS 10", "alter user max conn"),
    (False, "ALTER USER 'u'@'h' ATTRIBUTE '{\"name\": \"John\"}'", "alter user attribute"),
    (False, "ALTER USER 'u'@'h' COMMENT 'my user'", "alter user comment"),
    (False, "ALTER USER 'u'@'h' RESOURCE GROUP rg1", "alter user resource group"),
],

"DropEdge": [
    (False, "DROP TABLE t1, t2, t3", "drop multiple tables"),
    (False, "DROP VIEW v1, v2", "drop multiple views"),
    (False, "DROP DATABASE IF EXISTS mydb", "drop database if exists"),
    (False, "DROP INDEX idx ON t ALGORITHM=INSTANT LOCK=NONE", "drop index with options"),
    (False, "DROP USER IF EXISTS 'u'@'h'", "drop user if exists"),
    (False, "DROP ROLE IF EXISTS 'r'", "drop role if exists"),
    (False, "DROP STATISTICS s ON t", "drop statistics on table"),
    (False, "DROP STATS t", "drop stats table"),
    (False, "DROP STATS t PARTITION p1", "drop stats partition"),
    (False, "DROP STATS t GLOBAL", "drop stats global"),
],

"BRIEEdge": [
    (False, "BACKUP TABLE t TO 's3://bucket'", "backup table"),
    (False, "BACKUP DATABASE mydb TO 's3://bucket'", "backup database"),
    (False, "BACKUP DATABASE * TO 's3://bucket' LAST_BACKUP = 12345", "backup incremental"),
    (False, "RESTORE TABLE t FROM 's3://bucket'", "restore table"),
    (False, "RESTORE DATABASE * FROM 's3://bucket' CONCURRENCY=16", "restore with options"),
    (False, "IMPORT INTO t FROM 's3://bucket/*.csv' FORMAT 'CSV'", "import into s3"),
    (False, "IMPORT INTO t FROM '/local/*.csv' FORMAT 'CSV' FIELDS_TERMINATED_BY ','", "import into local"),
    (False, "CANCEL IMPORT JOB 1234", "cancel import job"),
],

"LoadDataEdge": [
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t CHARACTER SET utf8mb4", "load data charset"),
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t (a, b) SET c = a + b", "load data set"),
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' ESCAPED BY '\\\\'", "load data full fields"),
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t LINES STARTING BY 'x' TERMINATED BY '\\n'", "load data lines starting"),
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t PARTITION (p1)", "load data partition"),
    (False, "LOAD DATA INFILE '/t.csv' INTO TABLE t (a, b) SET c = NOW()", "load data set expr"),
],

"PreparedEdge": [
    (False, "PREPARE s FROM @query", "prepare from var"),
    (False, "EXECUTE s", "execute no params"),
    (False, "EXECUTE s USING @a, @b", "execute multi params"),
    (False, "DEALLOCATE PREPARE s", "deallocate prepare"),
],

"TransactionEdge": [
    (False, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY", "start tx consistent + ro"),
    (False, "START TRANSACTION WITH CAUSAL CONSISTENCY ONLY", "start tx causal consistency"),
    (False, "COMMIT WORK", "commit work"),
    (False, "COMMIT AND CHAIN", "commit and chain"),
    (False, "COMMIT AND CHAIN NO RELEASE", "commit chain no release"),
    (False, "COMMIT RELEASE", "commit release"),
    (False, "ROLLBACK WORK", "rollback work"),
    (False, "ROLLBACK AND NO CHAIN RELEASE", "rollback chain release"),
    (False, "BEGIN WORK", "begin work"),
],

"SplitEdge": [
    (False, "SPLIT TABLE t INDEX idx BETWEEN (1, 'a') AND (10, 'z') REGIONS 5", "split index composite"),
    (False, "SPLIT TABLE t BY (1), (5), (10)", "split by values"),
    (False, "SPLIT TABLE t INDEX idx BY (1, 'a'), (5, 'z')", "split index by values"),
    (False, "SPLIT REGION FOR TABLE t INDEX idx BETWEEN (1) AND (100) REGIONS 10", "split region for table index"),
    (False, "SPLIT REGION FOR TABLE t USE (1, 2), (3, 4)", "split region by"),
    (False, "SPLIT REGION FOR PARTITION TABLE t INDEX idx BETWEEN (1) AND (10) REGIONS 2", "split partition table index"),
],

"FlashbackEdge": [
    (False, "FLASHBACK TABLE db.t TO BEFORE DROP", "flashback db.table"),
    (False, "FLASHBACK DATABASE db TO BEFORE DROP RENAME TO db2", "flashback db rename"),
    (False, "FLASHBACK CLUSTER TO TIMESTAMP NOW() - INTERVAL 1 HOUR", "flashback to expr ts"),
],

"AbnormalInput": [
    # Things that SHOULD fail
    (True, "SELECT", "select no field - should fail"),
    (True, "INSERT INTO", "insert no table - should fail"),
    (True, "UPDATE SET", "update no table - should fail"),
    (True, "DELETE", "delete no table - should fail"),
    (True, "CREATE TABLE", "create table no name - should fail"),
    (True, "SELECT * FROM t FOR", "incomplete lock - should fail"),
    (True, "SELECT * FROM t ORDER BY", "incomplete order by - should fail"),
    (True, "SELECT * WHERE a=1", "select no from WHERE - should fail"),  # parser.y actually allows this? Check
],

"PlacementPolicy": [
    (False, "CREATE PLACEMENT POLICY p1 FOLLOWERS=3", "create placement policy"),
    (False, "CREATE PLACEMENT POLICY IF NOT EXISTS p1 FOLLOWER_CONSTRAINTS='[\"+region=us-east\"]'", "create placement policy if not exists"),
    (False, "ALTER PLACEMENT POLICY p1 VOTERS=5", "alter placement policy"),
    (False, "DROP PLACEMENT POLICY p1", "drop placement policy"),
    (False, "DROP PLACEMENT POLICY IF EXISTS p1", "drop placement policy if exists"),
    (False, "CREATE TABLE t (a INT) PLACEMENT POLICY=p1", "table with placement policy"),
    (False, "ALTER TABLE t PARTITION p1 PLACEMENT POLICY=p1", "partition with placement policy"),
],

"ResourceGroup": [
    (False, "CREATE RESOURCE GROUP rg1 RU_PER_SEC=1000", "create resource group"),
    (False, "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC=1000 PRIORITY=HIGH QUERY_LIMIT=(EXEC_ELAPSED='5s' ACTION=DRYRUN)", "create resource group full"),
    (False, "ALTER RESOURCE GROUP rg1 RU_PER_SEC=2000", "alter resource group"),
    (False, "DROP RESOURCE GROUP rg1", "drop resource group"),
    (False, "DROP RESOURCE GROUP IF EXISTS rg1", "drop resource group if exists"),
    (False, "SET RESOURCE GROUP rg1", "set resource group"),
    (False, "CALIBRATE RESOURCE", "calibrate resource"),
    (False, "CALIBRATE RESOURCE WORKLOAD TPCC", "calibrate resource workload"),
    (False, "CALIBRATE RESOURCE START_TIME = '2023-01-01' DURATION = '1h'", "calibrate resource start time"),
    (False, "QUERY WATCH ADD RESOURCE GROUP rg1 SQL TEXT EXACT TO 'SELECT 1'", "query watch add"),
    (False, "QUERY WATCH REMOVE 1", "query watch remove"),
],

"Procedures": [
    (False, "CREATE PROCEDURE p () BEGIN SELECT 1; END", "create procedure"),
    (False, "CREATE PROCEDURE p (IN a INT, OUT b VARCHAR(10)) BEGIN SELECT a INTO b; END", "create procedure with params"),
    (False, "DROP PROCEDURE p", "drop procedure"),
    (False, "DROP PROCEDURE IF EXISTS p", "drop procedure if exists"),
],

"TrafficControl": [
    (False, "TRAFFIC REPLAY FROM S3 'bucket' DURATION '1h'", "traffic replay"),
    (False, "TRAFFIC CAPTURE TO S3 'bucket' DURATION '1h'", "traffic capture"),
    (False, "CANCEL TRAFFIC JOBS ALL", "cancel traffic jobs"),
    (False, "SHOW TRAFFIC JOBS", "show traffic jobs"),
],

"Sequences": [
    (False, "CREATE SEQUENCE s CACHE 100", "create sequence with cache"),
    (False, "CREATE SEQUENCE s NO CACHE", "create sequence no cache"),
    (False, "CREATE SEQUENCE s CYCLE", "create sequence cycle"),
    (False, "CREATE SEQUENCE s NO CYCLE", "create sequence no cycle"),
    (False, "ALTER SEQUENCE s RESTART WITH 1", "alter sequence restart"),
    (False, "ALTER SEQUENCE s NO MAXVALUE", "alter sequence no maxvalue"),
    (False, "ALTER SEQUENCE s NO MINVALUE", "alter sequence no minvalue"),
    (False, "DROP SEQUENCE IF EXISTS s", "drop sequence if exists"),
],

}


def run_batch(label: str, cases: list) -> list:
    """Run a batch of test cases, returns list of (tag, sql, expected_fail, actual_err)."""
    lines = []
    for should_fail, sql, tag in cases:
        lines.append(fmt_case(sql, tag, should_fail))

    test_file = os.path.join(PARSER_DIR, "_audit_edge_test.go")
    code = GO_TEST_TEMPLATE.format(label=label, cases="\n".join(lines))
    with open(test_file, "w") as f:
        f.write(code)

    try:
        result = subprocess.run(
            ["go", "test", "-v", "-run", f"TestAuditEdge_{label}", "./"],
            cwd=PARSER_DIR,
            capture_output=True,
            text=True,
            timeout=120,
        )
        combined = result.stdout + result.stderr

        failures = []
        # Parse the go test -v output for failures
        # Lines like: --- FAIL: TestAuditEdge_X/tag (0.00s)
        # And: audit_edge_test.go:NN: FAIL [tag]: <err>
        for _, sql, tag in cases:
            # Check if this sub-test failed
            safe_tag = re.sub(r'[\s/\\]', '_', tag)
            if f"--- FAIL: TestAuditEdge_{label}/{safe_tag}" in combined:
                # Extract the error message
                idx = combined.find(f"FAIL [{tag}]:")
                if idx == -1:
                    idx = combined.find(f"SHOULD_FAIL [{tag}]:")
                err_msg = ""
                if idx != -1:
                    end = combined.find("\n", idx)
                    err_msg = combined[idx:end].strip()
                failures.append((tag, sql, err_msg))

        return failures
    except subprocess.TimeoutExpired:
        return [(tag, sql, "TIMEOUT") for _, sql, tag in cases]
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


def main():
    total_cases = sum(len(v) for v in TEST_GROUPS.values())
    print(f"Running {total_cases} edge-case SQL audit checks across {len(TEST_GROUPS)} categories...\n")

    all_failures = []
    for group, cases in TEST_GROUPS.items():
        label = group
        failures = run_batch(label, cases)
        if failures:
            all_failures.extend([(group, tag, sql, err) for tag, sql, err in failures])
            print(f"  ❌ {group}: {len(failures)} failure(s)")
        else:
            print(f"  ✅ {group}: all pass")

    if not all_failures:
        print("\n🎉 ALL EDGE-CASE CHECKS PASSED!")
        return 0

    print(f"\n\n{'='*60}")
    print(f"FAILURES ({len(all_failures)} total):")
    print(f"{'='*60}")
    current_group = None
    for group, tag, sql, err in sorted(all_failures, key=lambda x: x[0]):
        if group != current_group:
            print(f"\n── {group} ──")
            current_group = group
        print(f"  [{tag}]")
        print(f"    SQL: {sql}")
        print(f"    ERR: {err}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
