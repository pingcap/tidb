#!/usr/bin/env python3
"""
Deep Grammar Parity Audit

This script parses parser.y and for every named rule, extracts the raw
grammar alternatives (the |-separated productions). Then for a curated set of
"test SQL" entries per rule, it attempts to parse each SQL with the HandParser
binary and reports failures.

Strategy:
1. Extract all top-level grammar rules from parser_origin.y
2. For key rules, define representative SQL snippets that MUST parse
3. Run each through the Go test harness and report failures

This gives a ground-truth gap list: entries that parser.y supports but
HandParser currently does NOT.
"""

import re
import subprocess
import sys
import os
import json
from typing import Optional

PARSER_ORIGIN = "/Users/qiliu/.gemini/antigravity/brain/cb2a854e-5e7a-4815-9dee-cda5c836a2d1/parser_origin.y"
PARSER_DIR = "/Users/qiliu/projects/tidb/pkg/parser"

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Parse the grammar and extract rules
# ─────────────────────────────────────────────────────────────────────────────

def strip_actions(text: str) -> str:
    """Remove Go action blocks { ... } from yacc/bison grammar text."""
    result = []
    depth = 0
    i = 0
    while i < len(text):
        c = text[i]
        if c == '{':
            depth += 1
            i += 1
        elif c == '}':
            depth -= 1
            i += 1
        elif depth == 0:
            result.append(c)
            i += 1
        else:
            i += 1
    return ''.join(result)


def extract_rules(grammar_text: str) -> dict[str, str]:
    """
    Returns a dict of {rule_name: raw_alternatives_text} where each
    alternatives_text is the yacc rule body (stripped of Go actions).
    """
    # Split at the %% markers
    parts = grammar_text.split('%%')
    if len(parts) < 2:
        return {}
    rules_section = parts[1]

    # Tokenize top-level rules. Rules start at column 0 with Name:
    # Alternatives start with \t| or \t (for first alt).
    rule_pattern = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*):', re.MULTILINE)
    positions = [(m.start(), m.group(1)) for m in rule_pattern.finditer(rules_section)]

    rules = {}
    for idx, (pos, name) in enumerate(positions):
        end = positions[idx + 1][0] if idx + 1 < len(positions) else len(rules_section)
        body = rules_section[pos:end]
        # Find everything after the colon
        colon_pos = body.index(':')
        body = body[colon_pos + 1:]
        # Strip Go actions before storing
        clean = strip_actions(body)
        rules[name] = clean.strip()

    return rules


def get_alternatives(rule_body: str) -> list[str]:
    """Split a rule body into individual alternatives (split on leading |)."""
    # Split on lines that start with | (tab-| pattern from yacc)
    parts = re.split(r'\n\s*\|', rule_body)
    return [p.strip() for p in parts if p.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Build a mini test harness using 'go test -run' on a single parser
# ─────────────────────────────────────────────────────────────────────────────

# We'll use the differential test infrastructure: parse via HandParser,
# and if it produces an error, it's a miss.
# We write a temporary Go test file, then run it.

GO_TEST_SKELETON = """package parser

import (
\t"testing"
)

func TestAuditSQL_{idx}(t *testing.T) {{
\tfor _, sql := range []string{{
{sqls}
\t}} {{
\t\thp := NewHandParser()
\t\tscanner := NewScanner(sql)
\t\thp.Init(scanner, sql)
\t\t_, _, err := hp.ParseSQL()
\t\tif err != nil {{
\t\t\tt.Logf("FAIL: %s\\nERR: %v", sql, err)
\t\t\tt.Fail()
\t\t}}
\t}}
}}
"""


def run_sql_test(sqls: list[str], label: str) -> list[tuple[str, str]]:
    """Run a batch of SQL strings through HandParser, return list of (sql, error)."""
    # Write temp test file
    test_file = os.path.join(PARSER_DIR, "_audit_test_temp.go")
    sqls_str = "\n".join(f'\t\t{json.dumps(s)},' for s in sqls)
    code = GO_TEST_SKELETON.format(idx=label, sqls=sqls_str)
    with open(test_file, "w") as f:
        f.write(code)

    try:
        result = subprocess.run(
            ["go", "test", "-v", "-run", f"TestAuditSQL_{label}", "./"],
            cwd=PARSER_DIR,
            capture_output=True,
            text=True,
            timeout=60,
        )
        output = result.stdout + result.stderr

        failures = []
        # Parse -v output to identify which SQL failed
        for sql in sqls:
            escaped = sql.replace('"', '\\"')
            if f"FAIL: {sql}" in output or f'FAIL: {escaped}' in output:
                # Extract the error
                idx = output.find(f"FAIL: {sql}")
                err_section = output[idx:idx+400]
                err_match = re.search(r'ERR: (.+)', err_section)
                err_msg = err_match.group(1) if err_match else "unknown error"
                failures.append((sql, err_msg))
        return failures
    except subprocess.TimeoutExpired:
        return [(s, "TIMEOUT") for s in sqls]
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: The curated test SQL set (organized by grammar rule category)
# ─────────────────────────────────────────────────────────────────────────────

# Each entry: (rule_name, sql_string, description)
# Focus on KNOWN edge cases that may be missed based on grammar inspection.

TEST_CASES = [
    # ── SELECT ──────────────────────────────────────────────────────────────
    ("SelectStmt", "SELECT 1", "bare select"),
    ("SelectStmt", "SELECT * FROM t", "simple select"),
    ("SelectStmt", "SELECT * FROM t WHERE a = 1", "select with where"),
    ("SelectStmt", "SELECT * FROM t GROUP BY a HAVING count(*) > 1", "group by + having"),
    ("SelectStmt", "SELECT * FROM t ORDER BY a DESC LIMIT 10 OFFSET 5", "order+limit+offset"),
    ("SelectStmt", "SELECT * FROM t FOR UPDATE", "select for update"),
    ("SelectStmt", "SELECT * FROM t LOCK IN SHARE MODE", "lock in share mode"),
    ("SelectStmt", "SELECT * FROM t FOR SHARE", "select for share"),
    ("SelectStmt", "SELECT * FROM t FOR UPDATE NOWAIT", "for update nowait"),
    ("SelectStmt", "SELECT * FROM t FOR UPDATE SKIP LOCKED", "for update skip locked"),
    ("SelectStmt", "SELECT * FROM t INTO OUTFILE '/tmp/out.txt'", "into outfile"),
    ("SelectStmt", "SELECT SQL_CALC_FOUND_ROWS * FROM t", "sql_calc_found_rows"),
    ("SelectStmt", "SELECT HIGH_PRIORITY * FROM t", "high_priority"),
    ("SelectStmt", "SELECT DISTINCT a, b FROM t", "distinct"),
    ("SelectStmt", "SELECT ALL a FROM t", "all"),
    ("SelectStmt", "SELECT STRAIGHT_JOIN * FROM t", "straight_join"),
    ("SelectStmt", "SELECT SQL_SMALL_RESULT a FROM t", "sql_small_result"),
    ("SelectStmt", "SELECT SQL_BIG_RESULT a FROM t", "sql_big_result"),
    ("SelectStmt", "SELECT SQL_BUFFER_RESULT a FROM t", "sql_buffer_result"),
    ("SelectStmt", "SELECT SQL_NO_CACHE a FROM t", "sql_no_cache"),
    ("SelectStmt", "SELECT SQL_CACHE a FROM t", "sql_cache"),
    ("SelectStmt", "SELECT * FROM t AS t1", "table alias"),
    ("SelectStmt", "TABLE t ORDER BY a LIMIT 10", "TABLE statement"),
    ("SelectStmt", "VALUES ROW(1,2), ROW(3,4)", "VALUES statement"),
    # Window functions
    ("SelectStmt", "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t", "window row_number"),
    ("SelectStmt", "SELECT a, SUM(b) OVER w FROM t WINDOW w AS (PARTITION BY a)", "named window"),
    ("SelectStmt", "SELECT RANK() OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t", "window rows frame"),
    ("SelectStmt", "SELECT RANK() OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t", "window range frame"),
    # CTEs
    ("SelectStmtWithClause", "WITH cte AS (SELECT 1) SELECT * FROM cte", "simple CTE"),
    ("SelectStmtWithClause", "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte", "recursive CTE"),
    # UNION / set operations
    ("SetOprStmt", "SELECT 1 UNION SELECT 2", "union"),
    ("SetOprStmt", "SELECT 1 UNION ALL SELECT 2", "union all"),
    ("SetOprStmt", "SELECT 1 INTERSECT SELECT 2", "intersect"),
    ("SetOprStmt", "SELECT 1 EXCEPT SELECT 2", "except"),
    ("SetOprStmt", "(SELECT 1) UNION (SELECT 2) ORDER BY 1 LIMIT 1", "parenthesized union"),
    # Subqueries in FROM
    ("SelectStmt", "SELECT * FROM (SELECT 1) AS t", "subquery in FROM"),
    ("SelectStmt", "SELECT * FROM (SELECT 1 UNION SELECT 2) t", "union subquery in FROM"),
    # ── INSERT ──────────────────────────────────────────────────────────────
    ("InsertIntoStmt", "INSERT INTO t VALUES (1, 2)", "insert values"),
    ("InsertIntoStmt", "INSERT INTO t (a, b) VALUES (1, 2)", "insert with col list"),
    ("InsertIntoStmt", "INSERT INTO t VALUES (1), (2), (3)", "insert multi-row"),
    ("InsertIntoStmt", "INSERT INTO t SET a=1, b=2", "insert set"),
    ("InsertIntoStmt", "INSERT INTO t SELECT * FROM s", "insert select"),
    ("InsertIntoStmt", "INSERT IGNORE INTO t VALUES (1)", "insert ignore"),
    ("InsertIntoStmt", "INSERT LOW_PRIORITY INTO t VALUES (1)", "insert low_priority"),
    ("InsertIntoStmt", "INSERT DELAYED INTO t VALUES (1)", "insert delayed"),
    ("InsertIntoStmt", "INSERT HIGH_PRIORITY INTO t VALUES (1)", "insert high_priority"),
    ("InsertIntoStmt", "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a=VALUES(a)", "on duplicate key update"),
    ("InsertIntoStmt", "INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a=a+1", "on dup key update col"),
    ("InsertIntoStmt", "INSERT INTO t SELECT * FROM s ON DUPLICATE KEY UPDATE a=1", "insert select on dup"),
    ("InsertIntoStmt", "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte", "insert with CTE"),
    ("InsertIntoStmt", "INSERT INTO t TABLE s", "insert table"),
    # REPLACE
    ("ReplaceIntoStmt", "REPLACE INTO t VALUES (1, 2)", "replace values"),
    ("ReplaceIntoStmt", "REPLACE INTO t SET a=1", "replace set"),
    ("ReplaceIntoStmt", "REPLACE LOW_PRIORITY INTO t VALUES (1)", "replace low priority"),
    # ── UPDATE ──────────────────────────────────────────────────────────────
    ("UpdateStmt", "UPDATE t SET a=1", "simple update"),
    ("UpdateStmt", "UPDATE t SET a=1 WHERE b=2", "update where"),
    ("UpdateStmt", "UPDATE t SET a=1 ORDER BY b LIMIT 5", "update order+limit"),
    ("UpdateStmt", "UPDATE t1, t2 SET t1.a=t2.a WHERE t1.id=t2.id", "multi-table update"),
    ("UpdateStmt", "UPDATE IGNORE t SET a=1", "update ignore"),
    ("UpdateStmt", "UPDATE LOW_PRIORITY t SET a=1", "update low priority"),
    ("UpdateStmt", "WITH cte AS (SELECT 1 AS a) UPDATE t SET t.x = (SELECT a FROM cte)", "update with CTE"),
    # ── DELETE ──────────────────────────────────────────────────────────────
    ("DeleteFromStmt", "DELETE FROM t", "simple delete"),
    ("DeleteFromStmt", "DELETE FROM t WHERE a=1", "delete where"),
    ("DeleteFromStmt", "DELETE FROM t ORDER BY a LIMIT 10", "delete order+limit"),
    ("DeleteFromStmt", "DELETE IGNORE FROM t WHERE 1", "delete ignore"),
    ("DeleteFromStmt", "DELETE LOW_PRIORITY FROM t", "delete low priority"),
    ("DeleteFromStmt", "DELETE QUICK FROM t", "delete quick"),
    ("DeleteFromStmt", "DELETE t1 FROM t1, t2 WHERE t1.id=t2.id", "delete multi-table from list"),
    ("DeleteFromStmt", "DELETE FROM t1 USING t1, t2 WHERE t1.id=t2.id", "delete using"),
    ("DeleteFromStmt", "WITH cte AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM cte)", "delete with CTE"),
    # ── SHOW ────────────────────────────────────────────────────────────────
    ("ShowStmt", "SHOW DATABASES", "show databases"),
    ("ShowStmt", "SHOW DATABASES LIKE 'test%'", "show databases like"),
    ("ShowStmt", "SHOW DATABASES WHERE schema_name='test'", "show databases where"),
    ("ShowStmt", "SHOW TABLES", "show tables"),
    ("ShowStmt", "SHOW FULL TABLES", "show full tables"),
    ("ShowStmt", "SHOW TABLES IN mydb", "show tables in db"),
    ("ShowStmt", "SHOW OPEN TABLES", "show open tables"),
    ("ShowStmt", "SHOW TABLE STATUS", "show table status"),
    ("ShowStmt", "SHOW COLUMNS FROM t", "show columns from"),
    ("ShowStmt", "SHOW FULL COLUMNS FROM t", "show full columns"),
    ("ShowStmt", "SHOW FIELDS FROM t", "show fields"),
    ("ShowStmt", "SHOW EXTENDED COLUMNS FROM t", "show extended columns"),
    ("ShowStmt", "SHOW INDEX FROM t", "show index"),
    ("ShowStmt", "SHOW INDEXES FROM t", "show indexes"),
    ("ShowStmt", "SHOW KEYS FROM t", "show keys"),
    ("ShowStmt", "SHOW INDEX FROM t IN mydb", "show index in db"),
    ("ShowStmt", "SHOW GLOBAL VARIABLES", "show global variables"),
    ("ShowStmt", "SHOW SESSION VARIABLES", "show session variables"),
    ("ShowStmt", "SHOW VARIABLES LIKE 'max%'", "show variables like"),
    ("ShowStmt", "SHOW GLOBAL STATUS", "show global status"),
    ("ShowStmt", "SHOW GLOBAL BINDINGS", "show global bindings"),
    ("ShowStmt", "SHOW SESSION BINDINGS", "show session bindings"),
    ("ShowStmt", "SHOW CREATE TABLE t", "show create table"),
    ("ShowStmt", "SHOW CREATE DATABASE mydb", "show create database"),
    ("ShowStmt", "SHOW CREATE VIEW v", "show create view"),
    ("ShowStmt", "SHOW CREATE USER 'root'@'localhost'", "show create user"),
    ("ShowStmt", "SHOW CREATE SEQUENCE s", "show create sequence"),
    ("ShowStmt", "SHOW PROCESSLIST", "show processlist"),
    ("ShowStmt", "SHOW FULL PROCESSLIST", "show full processlist"),
    ("ShowStmt", "SHOW MASTER STATUS", "show master status"),
    ("ShowStmt", "SHOW BINARY LOG STATUS", "show binary log status"),
    ("ShowStmt", "SHOW REPLICA STATUS", "show replica status"),
    ("ShowStmt", "SHOW SLAVE STATUS", "show slave status"),
    ("ShowStmt", "SHOW WARNINGS", "show warnings"),
    ("ShowStmt", "SHOW ERRORS", "show errors"),
    ("ShowStmt", "SHOW COUNT(*) WARNINGS", "show count warnings"),
    ("ShowStmt", "SHOW COUNT(*) ERRORS", "show count errors"),
    ("ShowStmt", "SHOW PRIVILEGES", "show privileges"),
    ("ShowStmt", "SHOW BUILTINS", "show builtins"),
    ("ShowStmt", "SHOW ENGINES", "show engines"),
    ("ShowStmt", "SHOW CHARSET", "show charset"),
    ("ShowStmt", "SHOW COLLATION", "show collation"),
    ("ShowStmt", "SHOW TRIGGERS", "show triggers"),
    ("ShowStmt", "SHOW EVENTS", "show events"),
    ("ShowStmt", "SHOW PLUGINS", "show plugins"),
    ("ShowStmt", "SHOW PROCEDURE STATUS", "show procedure status"),
    ("ShowStmt", "SHOW FUNCTION STATUS", "show function status"),
    ("ShowStmt", "SHOW PROFILES", "show profiles"),
    ("ShowStmt", "SHOW PROFILE ALL FOR QUERY 1 LIMIT 10", "show profile"),
    ("ShowStmt", "SHOW GRANTS FOR 'root'@'localhost'", "show grants"),
    ("ShowStmt", "SHOW GRANTS FOR 'root'@'localhost' USING 'r1'", "show grants using"),
    ("ShowStmt", "SHOW STATS_META", "show stats_meta"),
    ("ShowStmt", "SHOW STATS_HISTOGRAMS", "show stats_histograms"),
    ("ShowStmt", "SHOW STATS_BUCKETS", "show stats_buckets"),
    ("ShowStmt", "SHOW STATS_HEALTHY", "show stats_healthy"),
    ("ShowStmt", "SHOW STATS_TOPN", "show stats_topn"),
    ("ShowStmt", "SHOW STATS_LOCKED", "show stats_locked"),
    ("ShowStmt", "SHOW STATS_EXTENDED", "show stats_extended"),
    ("ShowStmt", "SHOW HISTOGRAMS_IN_FLIGHT", "show histograms"),
    ("ShowStmt", "SHOW COLUMN_STATS_USAGE", "show column stats usage"),
    ("ShowStmt", "SHOW ANALYZE STATUS", "show analyze status"),
    ("ShowStmt", "SHOW PLACEMENT", "show placement"),
    ("ShowStmt", "SHOW PLACEMENT LABELS", "show placement labels"),
    ("ShowStmt", "SHOW PLACEMENT FOR TABLE t", "show placement for table"),
    ("ShowStmt", "SHOW PLACEMENT FOR DATABASE mydb", "show placement for db"),
    ("ShowStmt", "SHOW BACKUPS", "show backups"),
    ("ShowStmt", "SHOW RESTORES", "show restores"),
    ("ShowStmt", "SHOW BINDING_CACHE STATUS", "show binding cache status"),
    ("ShowStmt", "SHOW IMPORT JOBS", "show import jobs"),
    ("ShowStmt", "SHOW DISTRIBUTION JOBS", "show distribution jobs"),
    ("ShowStmt", "SHOW SESSION_STATES", "show session states"),
    ("ShowStmt", "SHOW IMPORT JOB 1", "show import job"),
    ("ShowStmt", "SHOW DISTRIBUTION JOB 1", "show distribution job"),
    ("ShowStmt", "SHOW CREATE PROCEDURE p", "show create procedure"),
    ("ShowStmt", "SHOW AFFINITY", "show affinity"),
    ("ShowStmt", "SHOW TABLE t PARTITION (p1) DISTRIBUTIONS WHERE 1", "show table distributions"),
    # ── CREATE ──────────────────────────────────────────────────────────────
    ("CreateTableStmt", "CREATE TABLE t (a INT, b VARCHAR(100))", "create table"),
    ("CreateTableStmt", "CREATE TABLE IF NOT EXISTS t (a INT)", "create table if not exists"),
    ("CreateTableStmt", "CREATE TABLE t LIKE s", "create table like"),
    ("CreateTableStmt", "CREATE TABLE t AS SELECT 1", "create table as select"),
    ("CreateTableStmt", "CREATE TEMPORARY TABLE t (a INT)", "create temp table"),
    ("CreateTableStmt", "CREATE TABLE t (a INT) PARTITION BY HASH(a) PARTITIONS 4", "create partitioned table"),
    ("CreateTableStmt", "CREATE TABLE t (a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)", "range partition"),
    ("CreateTableStmt", "CREATE TABLE t (a INT) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1,2), PARTITION p1 VALUES IN (3,4))", "list partition"),
    ("CreateTableStmt", "CREATE TABLE t (a INT) PARTITION BY KEY(a) PARTITIONS 4", "key partition"),
    ("CreateIndexStmt", "CREATE INDEX idx ON t (a)", "create index"),
    ("CreateIndexStmt", "CREATE UNIQUE INDEX idx ON t (a, b)", "create unique index"),
    ("CreateIndexStmt", "CREATE FULLTEXT INDEX idx ON t (a)", "create fulltext index"),
    ("CreateIndexStmt", "CREATE INDEX idx ON t (a) USING BTREE", "create index using btree"),
    ("CreateViewStmt", "CREATE VIEW v AS SELECT * FROM t", "create view"),
    ("CreateViewStmt", "CREATE OR REPLACE VIEW v AS SELECT * FROM t", "create or replace view"),
    ("CreateDatabaseStmt", "CREATE DATABASE mydb", "create database"),
    ("CreateDatabaseStmt", "CREATE DATABASE IF NOT EXISTS mydb", "create database if not exists"),
    ("CreateDatabaseStmt", "CREATE DATABASE mydb CHARACTER SET utf8mb4", "create db with charset"),
    # ── ALTER ────────────────────────────────────────────────────────────────
    ("AlterTableStmt", "ALTER TABLE t ADD COLUMN a INT", "alter add column"),
    ("AlterTableStmt", "ALTER TABLE t ADD COLUMN (a INT, b VARCHAR(10))", "alter add multi columns"),
    ("AlterTableStmt", "ALTER TABLE t DROP COLUMN a", "alter drop column"),
    ("AlterTableStmt", "ALTER TABLE t MODIFY COLUMN a BIGINT", "alter modify column"),
    ("AlterTableStmt", "ALTER TABLE t CHANGE COLUMN a b BIGINT", "alter change column"),
    ("AlterTableStmt", "ALTER TABLE t ADD INDEX idx (a)", "alter add index"),
    ("AlterTableStmt", "ALTER TABLE t DROP INDEX idx", "alter drop index"),
    ("AlterTableStmt", "ALTER TABLE t RENAME TO t2", "alter rename table"),
    ("AlterTableStmt", "ALTER TABLE t ENGINE=InnoDB", "alter engine"),
    ("AlterTableStmt", "ALTER TABLE t AUTO_INCREMENT=100", "alter auto_increment"),
    ("AlterTableStmt", "ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (100))", "alter add partition"),
    ("AlterTableStmt", "ALTER TABLE t DROP PARTITION p1", "alter drop partition"),
    ("AlterTableStmt", "ALTER TABLE t PARTITION BY HASH(a) PARTITIONS 8", "alter re-partition"),
    ("AlterTableStmt", "ALTER TABLE t REMOVE PARTITIONING", "alter remove partitioning"),
    ("AlterTableStmt", "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)", "alter add pk"),
    ("AlterTableStmt", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES r(id)", "alter add fk"),
    ("AlterTableStmt", "ALTER TABLE t DROP PRIMARY KEY", "alter drop pk"),
    ("AlterTableStmt", "ALTER TABLE t TRUNCATE PARTITION p1", "alter truncate partition"),
    ("AlterTableStmt", "ALTER TABLE t REORGANIZE PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (5), PARTITION p1b VALUES LESS THAN (10))", "alter reorganize partition"),
    ("AlterTableStmt", "ALTER TABLE t EXCHANGE PARTITION p1 WITH TABLE t2", "alter exchange partition"),
    ("AlterTableStmt", "ALTER TABLE t LOCK = DEFAULT", "alter lock"),
    ("AlterTableStmt", "ALTER TABLE t ALGORITHM = INSTANT", "alter algorithm"),
    ("AlterTableStmt", "ALTER TABLE t FORCE", "alter force"),
    # ── DROP ────────────────────────────────────────────────────────────────
    ("DropTableStmt", "DROP TABLE t", "drop table"),
    ("DropTableStmt", "DROP TABLE IF EXISTS t", "drop table if exists"),
    ("DropTableStmt", "DROP TEMPORARY TABLE t", "drop temp table"),
    ("DropIndexStmt", "DROP INDEX idx ON t", "drop index"),
    ("DropDatabaseStmt", "DROP DATABASE mydb", "drop database"),
    ("DropViewStmt", "DROP VIEW v", "drop view"),
    ("DropViewStmt", "DROP VIEW IF EXISTS v", "drop view if exists"),
    # ── DML special ─────────────────────────────────────────────────────────
    ("LoadDataStmt", "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE t", "load data infile"),
    ("LoadDataStmt", "LOAD DATA LOCAL INFILE '/tmp/x.csv' INTO TABLE t", "load data local infile"),
    ("LoadDataStmt", "LOAD DATA INFILE '/tmp/x.csv' IGNORE INTO TABLE t (a, b)", "load data ignore"),
    ("LoadDataStmt", "LOAD DATA INFILE '/tmp/x.csv' REPLACE INTO TABLE t FIELDS TERMINATED BY ','", "load data replace fields"),
    ("LoadDataStmt", "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE t LINES TERMINATED BY '\\n'", "load data lines terminated"),
    ("LoadDataStmt", "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE t IGNORE 1 LINES", "load data ignore lines"),
    # ── SET ─────────────────────────────────────────────────────────────────
    ("SetStmt", "SET a = 1", "set simple"),
    ("SetStmt", "SET GLOBAL a = 1", "set global"),
    ("SetStmt", "SET @@GLOBAL.a = 1", "set @@global"),
    ("SetStmt", "SET @a = 1", "set user var"),
    ("SetStmt", "SET NAMES utf8mb4", "set names"),
    ("SetStmt", "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci", "set names collate"),
    ("SetStmt", "SET CHARACTER SET utf8mb4", "set charset"),
    ("SetStmt", "SET CHARSET utf8mb4", "set charset short"),
    ("SetStmt", "SET PASSWORD = 'newpwd'", "set password"),
    ("SetStmt", "SET PASSWORD FOR 'user'@'host' = 'newpwd'", "set password for user"),
    ("SetStmt", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", "set tx isolation"),
    ("SetStmt", "SET GLOBAL TRANSACTION READ ONLY", "set global tx readonly"),
    ("SetStmt", "SET SESSION TRANSACTION READ WRITE", "set session tx rw"),
    ("SetStmt", "SET TRANSACTION READ ONLY AS OF TIMESTAMP '2023-01-01'", "set tx read only as of"),
    ("SetStmt", "SET SESSION_STATES 'state_blob'", "set session states"),
    ("SetStmt", "SET RESOURCE GROUP rg1", "set resource group"),
    ("SetRoleStmt", "SET ROLE ALL", "set role all"),
    ("SetRoleStmt", "SET ROLE DEFAULT", "set role default"),
    ("SetRoleStmt", "SET ROLE NONE", "set role none"),
    ("SetRoleStmt", "SET ROLE 'r1', 'r2'", "set role list"),
    ("SetRoleStmt", "SET ROLE ALL EXCEPT 'r1'", "set role all except"),
    ("SetDefaultRoleStmt", "SET DEFAULT ROLE ALL TO 'user'@'host'", "set default role all"),
    ("SetDefaultRoleStmt", "SET DEFAULT ROLE NONE TO 'user'@'host'", "set default role none"),
    ("SetDefaultRoleStmt", "SET DEFAULT ROLE 'r1' TO 'user'@'host'", "set default role list"),
    # ── GRANT / REVOKE ──────────────────────────────────────────────────────
    ("GrantStmt", "GRANT SELECT ON t TO 'user'@'host'", "grant select on table"),
    ("GrantStmt", "GRANT ALL PRIVILEGES ON *.* TO 'user'@'host'", "grant all on *.*"),
    ("GrantStmt", "GRANT SELECT, INSERT ON mydb.* TO 'user'@'host'", "grant multi priv"),
    ("GrantStmt", "GRANT SELECT ON t TO 'user'@'host' WITH GRANT OPTION", "grant with grant option"),
    ("GrantStmt", "GRANT PROXY ON 'localuser'@'%' TO 'extuser'@'host'", "grant proxy"),
    ("GrantRoleStmt", "GRANT 'r1' TO 'user'@'host'", "grant role"),
    ("RevokeStmt", "REVOKE SELECT ON t FROM 'user'@'host'", "revoke select"),
    ("RevokeStmt", "REVOKE ALL PRIVILEGES ON *.* FROM 'user'@'host'", "revoke all"),
    # ── CREATE/DROP USER ────────────────────────────────────────────────────
    ("CreateUserStmt", "CREATE USER 'u'@'h' IDENTIFIED BY 'pwd'", "create user"),
    ("CreateUserStmt", "CREATE USER IF NOT EXISTS 'u'@'h'", "create user if not exists"),
    ("CreateUserStmt", "CREATE USER 'u'@'h' IDENTIFIED WITH 'mysql_native_password' BY 'pwd'", "create user with auth plugin"),
    ("AlterUserStmt", "ALTER USER 'u'@'h' IDENTIFIED BY 'newpwd'", "alter user password"),
    ("AlterUserStmt", "ALTER USER 'u'@'h' PASSWORD EXPIRE", "alter user pw expire"),
    ("AlterUserStmt", "ALTER USER 'u'@'h' ACCOUNT LOCK", "alter user lock"),
    ("AlterUserStmt", "ALTER USER USER() IDENTIFIED BY 'newpwd'", "alter user current user"),
    ("DropUserStmt", "DROP USER 'u'@'h'", "drop user"),
    ("RenameUserStmt", "RENAME USER 'u'@'h' TO 'u2'@'h2'", "rename user"),
    # ── TRANSACTIONS ────────────────────────────────────────────────────────
    ("BeginTransactionStmt", "BEGIN", "begin"),
    ("BeginTransactionStmt", "BEGIN PESSIMISTIC", "begin pessimistic"),
    ("BeginTransactionStmt", "BEGIN OPTIMISTIC", "begin optimistic"),
    ("BeginTransactionStmt", "START TRANSACTION", "start transaction"),
    ("BeginTransactionStmt", "START TRANSACTION WITH CONSISTENT SNAPSHOT", "start tx consistent snapshot"),
    ("BeginTransactionStmt", "START TRANSACTION READ ONLY", "start tx read only"),
    ("BeginTransactionStmt", "START TRANSACTION READ WRITE", "start tx read write"),
    ("BeginTransactionStmt", "START TRANSACTION READ ONLY AS OF TIMESTAMP '2023-01-01'", "start tx as of timestamp"),
    ("CommitStmt", "COMMIT", "commit"),
    ("RollbackStmt", "ROLLBACK", "rollback"),
    ("RollbackStmt", "ROLLBACK TO SAVEPOINT sp1", "rollback to savepoint"),
    ("SavepointStmt", "SAVEPOINT sp1", "savepoint"),
    ("ReleaseSavepointStmt", "RELEASE SAVEPOINT sp1", "release savepoint"),
    # ── ADMIN ────────────────────────────────────────────────────────────────
    ("AdminStmt", "ADMIN SHOW DDL", "admin show ddl"),
    ("AdminStmt", "ADMIN SHOW DDL JOBS 5", "admin show ddl jobs"),
    ("AdminStmt", "ADMIN SHOW DDL JOB QUERIES 1, 2, 3", "admin show ddl job queries"),
    ("AdminStmt", "ADMIN CANCEL DDL JOBS 1, 2", "admin cancel ddl jobs"),
    ("AdminStmt", "ADMIN PAUSE DDL JOBS 1, 2", "admin pause ddl jobs"),
    ("AdminStmt", "ADMIN RESUME DDL JOBS 1, 2", "admin resume ddl jobs"),
    ("AdminStmt", "ADMIN CHECK TABLE t", "admin check table"),
    ("AdminStmt", "ADMIN CHECK INDEX t idx", "admin check index"),
    ("AdminStmt", "ADMIN RECOVER INDEX t idx", "admin recover index"),
    ("AdminStmt", "ADMIN CLEANUP INDEX t idx", "admin cleanup index"),
    ("AdminStmt", "ADMIN CHECKSUM TABLE t", "admin checksum table"),
    ("AdminStmt", "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST", "admin reload expr blacklist"),
    ("AdminStmt", "ADMIN RELOAD OPT_RULE_BLACKLIST", "admin reload opt rule blacklist"),
    ("AdminStmt", "ADMIN PLUGINS ENABLE plugin1", "admin plugins enable"),
    ("AdminStmt", "ADMIN PLUGINS DISABLE plugin1", "admin plugins disable"),
    ("AdminStmt", "ADMIN FLUSH BINDINGS", "admin flush bindings"),
    ("AdminStmt", "ADMIN EVOLVE BINDINGS", "admin evolve bindings"),
    ("AdminStmt", "ADMIN RELOAD BINDINGS", "admin reload bindings"),
    ("AdminStmt", "ADMIN RELOAD STATISTICS", "admin reload statistics"),
    ("AdminStmt", "ADMIN SHOW TELEMETRY", "admin show telemetry"),
    ("AdminStmt", "ADMIN RESET TELEMETRY_ID", "admin reset telemetry"),
    ("AdminStmt", "ADMIN FLUSH PLAN_CACHE", "admin flush plan cache"),
    ("AdminStmt", "ADMIN SHOW SLOW RECENT 10", "admin show slow recent"),
    ("AdminStmt", "ADMIN SHOW SLOW TOP ALL 10", "admin show slow top all"),
    ("AdminStmt", "ADMIN SHOW SLOW TOP INTERNAL 10", "admin show slow top internal"),
    # ── EXPLAIN ──────────────────────────────────────────────────────────────
    ("ExplainStmt", "EXPLAIN SELECT * FROM t", "explain select"),
    ("ExplainStmt", "EXPLAIN FORMAT=JSON SELECT * FROM t", "explain format json"),
    ("ExplainStmt", "EXPLAIN FORMAT=BRIEF SELECT * FROM t", "explain format brief"),
    ("ExplainStmt", "EXPLAIN FORMAT=ROW SELECT * FROM t", "explain format row"),
    ("ExplainStmt", "EXPLAIN ANALYZE SELECT * FROM t", "explain analyze"),
    ("ExplainStmt", "EXPLAIN FOR CONNECTION 1", "explain for connection"),
    ("ExplainStmt", "DESC t", "desc table"),
    ("ExplainStmt", "DESCRIBE t", "describe table"),
    ("ExplainStmt", "EXPLAIN INSERT INTO t VALUES (1)", "explain insert"),
    ("ExplainStmt", "EXPLAIN UPDATE t SET a=1", "explain update"),
    ("ExplainStmt", "EXPLAIN DELETE FROM t", "explain delete"),
    # ── PREPARE / EXECUTE ────────────────────────────────────────────────────
    ("PreparedStmt", "PREPARE s FROM 'SELECT * FROM t WHERE a = ?'", "prepare"),
    ("PreparedStmt", "EXECUTE s USING @a", "execute"),
    ("PreparedStmt", "DEALLOCATE PREPARE s", "deallocate"),
    ("PreparedStmt", "DROP PREPARE s", "drop prepare"),
    # ── USE ─────────────────────────────────────────────────────────────────
    ("UseStmt", "USE mydb", "use db"),
    # ── DO ──────────────────────────────────────────────────────────────────
    ("DoStmt", "DO 1+1", "do expr"),
    ("DoStmt", "DO SLEEP(1)", "do sleep"),
    # ── TRUNCATE ─────────────────────────────────────────────────────────────
    ("TruncateTableStmt", "TRUNCATE TABLE t", "truncate table"),
    ("TruncateTableStmt", "TRUNCATE t", "truncate bare"),
    # ── BINLOG ────────────────────────────────────────────────────────────────
    ("BinlogStmt", "BINLOG 'encodedblob'", "binlog"),
    # ── FLUSH ────────────────────────────────────────────────────────────────
    ("FlushStmt", "FLUSH TABLES", "flush tables"),
    ("FlushStmt", "FLUSH TABLES t1, t2", "flush tables list"),
    ("FlushStmt", "FLUSH TABLES WITH READ LOCK", "flush tables with read lock"),
    ("FlushStmt", "FLUSH NO_WRITE_TO_BINLOG TABLES", "flush no write to binlog"),
    ("FlushStmt", "FLUSH LOCAL TABLES", "flush local tables"),
    ("FlushStmt", "FLUSH PRIVILEGES", "flush privileges"),
    ("FlushStmt", "FLUSH STATUS", "flush status"),
    ("FlushStmt", "FLUSH LOGS", "flush logs"),
    ("FlushStmt", "FLUSH BINARY LOGS", "flush binary logs"),
    ("FlushStmt", "FLUSH SLOW LOGS", "flush slow logs"),
    ("FlushStmt", "FLUSH HOSTS", "flush hosts"),
    ("FlushStmt", "FLUSH TIDB PLUGINS p1", "flush tidb plugins"),
    ("FlushStmt", "FLUSH CLIENT_ERRORS_SUMMARY", "flush client error summary"),
    ("FlushStmt", "FLUSH STATS_DELTA", "flush stats delta"),
    ("FlushStmt", "FLUSH STATS_DELTA CLUSTER", "flush stats delta cluster"),
    # ── KILL ─────────────────────────────────────────────────────────────────
    ("KillStmt", "KILL 1234", "kill conn"),
    ("KillStmt", "KILL CONNECTION 1234", "kill connection"),
    ("KillStmt", "KILL QUERY 1234", "kill query"),
    ("KillStmt", "KILL TIDB 1234", "kill tidb"),
    # ── LOCK / UNLOCK ────────────────────────────────────────────────────────
    ("LockTablesStmt", "LOCK TABLES t READ", "lock tables read"),
    ("LockTablesStmt", "LOCK TABLES t WRITE", "lock tables write"),
    ("LockTablesStmt", "LOCK TABLES t READ LOCAL", "lock tables read local"),
    ("LockTablesStmt", "LOCK TABLES t1 READ, t2 WRITE", "lock multiple tables"),
    ("UnlockTablesStmt", "UNLOCK TABLES", "unlock tables"),
    # ── ANALYZE ──────────────────────────────────────────────────────────────
    ("AnalyzeTableStmt", "ANALYZE TABLE t", "analyze table"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t INDEX idx1", "analyze table index"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t PARTITION p1", "analyze partition"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t ALL INDEXES", "analyze all indexes"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t PREDICATE COLUMNS", "analyze predicate columns"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t COLUMNS a, b", "analyze named columns"),
    ("AnalyzeTableStmt", "ANALYZE INCREMENTAL TABLE t INDEX idx", "analyze incremental"),
    # ── Expressions ─────────────────────────────────────────────────────────
    ("SelectStmt", "SELECT 1 + 2 * 3 - 4 / 2 % 3", "arithmetic ops"),
    ("SelectStmt", "SELECT a & b | c ^ d << 1 >> 2 FROM t", "bitwise ops"),
    ("SelectStmt", "SELECT a AND b OR c XOR d FROM t", "logical ops"),
    ("SelectStmt", "SELECT NOT a, !b FROM t", "not"),
    ("SelectStmt", "SELECT a = b, a != b, a <> b, a < b, a > b, a <= b, a >= b FROM t", "comparison ops"),
    ("SelectStmt", "SELECT a <=> b FROM t", "null safe eq"),
    ("SelectStmt", "SELECT a BETWEEN 1 AND 10 FROM t", "between"),
    ("SelectStmt", "SELECT a NOT BETWEEN 1 AND 10 FROM t", "not between"),
    ("SelectStmt", "SELECT a IN (1, 2, 3) FROM t", "in list"),
    ("SelectStmt", "SELECT a NOT IN (1, 2, 3) FROM t", "not in list"),
    ("SelectStmt", "SELECT a IS NULL, a IS NOT NULL FROM t", "is null"),
    ("SelectStmt", "SELECT a IS TRUE, a IS NOT TRUE, a IS FALSE, a IS NOT FALSE FROM t", "is true/false"),
    ("SelectStmt", "SELECT a LIKE 'b%' FROM t", "like"),
    ("SelectStmt", "SELECT a NOT LIKE 'b%' FROM t", "not like"),
    ("SelectStmt", "SELECT a REGEXP 'b.*' FROM t", "regexp"),
    ("SelectStmt", "SELECT a NOT REGEXP 'b.*' FROM t", "not regexp"),
    ("SelectStmt", "SELECT a RLIKE 'b.*' FROM t", "rlike"),
    ("SelectStmt", "SELECT IF(a>1, 'yes', 'no') FROM t", "if"),
    ("SelectStmt", "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t", "case when"),
    ("SelectStmt", "SELECT CASE WHEN a=1 THEN 'one' ELSE 'other' END FROM t", "case when bare"),
    ("SelectStmt", "SELECT COALESCE(a, b, c) FROM t", "coalesce"),
    ("SelectStmt", "SELECT NULLIF(a, b) FROM t", "nullif"),
    ("SelectStmt", "SELECT IFNULL(a, b) FROM t", "ifnull"),
    ("SelectStmt", "SELECT CAST(a AS SIGNED) FROM t", "cast signed"),
    ("SelectStmt", "SELECT CAST(a AS DECIMAL(10, 2)) FROM t", "cast decimal"),
    ("SelectStmt", "SELECT CAST(a AS CHAR(10)) FROM t", "cast char"),
    ("SelectStmt", "SELECT CAST(a AS BINARY) FROM t", "cast binary"),
    ("SelectStmt", "SELECT CAST(a AS JSON) FROM t", "cast json"),
    ("SelectStmt", "SELECT CONVERT(a, SIGNED) FROM t", "convert"),
    ("SelectStmt", "SELECT CONVERT(a USING utf8) FROM t", "convert using"),
    ("SelectStmt", "SELECT EXISTS (SELECT 1) FROM t", "exists"),
    ("SelectStmt", "SELECT a IN (SELECT b FROM t2) FROM t", "in subquery"),
    ("SelectStmt", "SELECT a = ANY (SELECT b FROM t2) FROM t", "any subquery"),
    ("SelectStmt", "SELECT a > ALL (SELECT b FROM t2) FROM t", "all subquery"),
    # JSON operators
    ("SelectStmt", "SELECT a->'$.key' FROM t", "json extract"),
    ("SelectStmt", "SELECT a->>'$.key' FROM t", "json extract text"),
    # String concat
    ("SelectStmt", "SELECT 'a' || 'b' FROM t", "string concat ||"),
    # Timstamp literals
    ("SelectStmt", "SELECT TIMESTAMP '2023-01-01 00:00:00'", "timestamp literal"),
    ("SelectStmt", "SELECT DATE '2023-01-01'", "date literal"),
    ("SelectStmt", "SELECT TIME '12:00:00'", "time literal"),
    # Sequence functions  
    ("SelectStmt", "SELECT NEXTVAL(s)", "nextval"),
    ("SelectStmt", "SELECT LASTVAL(s)", "lastval"),
    ("SelectStmt", "SELECT SETVAL(s, 100)", "setval"),
    # System functions
    ("SelectStmt", "SELECT DATABASE()", "database()"),
    ("SelectStmt", "SELECT USER()", "user()"),
    ("SelectStmt", "SELECT CURRENT_USER()", "current_user()"),
    ("SelectStmt", "SELECT VERSION()", "version()"),
    ("SelectStmt", "SELECT CONNECTION_ID()", "connection_id()"),
    ("SelectStmt", "SELECT CURRENT_USER", "current_user no parens"),
    ("SelectStmt", "SELECT CURDATE()", "curdate()"),
    ("SelectStmt", "SELECT NOW()", "now()"),
    ("SelectStmt", "SELECT NOW(3)", "now(fsp)"),
    ("SelectStmt", "SELECT SYSDATE()", "sysdate()"),
    ("SelectStmt", "SELECT UTC_DATE()", "utc_date()"),
    ("SelectStmt", "SELECT UTC_TIME()", "utc_time()"),
    ("SelectStmt", "SELECT UTC_TIMESTAMP()", "utc_timestamp()"),
    ("SelectStmt", "SELECT LOCALTIME()", "localtime()"),
    ("SelectStmt", "SELECT LOCALTIMESTAMP()", "localtimestamp()"),
    # Aggregate functions
    ("SelectStmt", "SELECT COUNT(*) FROM t", "count(*)"),
    ("SelectStmt", "SELECT COUNT(DISTINCT a) FROM t", "count distinct"),
    ("SelectStmt", "SELECT SUM(a) FROM t", "sum"),
    ("SelectStmt", "SELECT AVG(DISTINCT a) FROM t", "avg distinct"),
    ("SelectStmt", "SELECT MAX(a) FROM t", "max"),
    ("SelectStmt", "SELECT MIN(a) FROM t", "min"),
    ("SelectStmt", "SELECT GROUP_CONCAT(a ORDER BY b SEPARATOR ',') FROM t", "group_concat"),
    ("SelectStmt", "SELECT BIT_AND(a) FROM t", "bit_and"),
    ("SelectStmt", "SELECT BIT_OR(a) FROM t", "bit_or"),
    ("SelectStmt", "SELECT BIT_XOR(a) FROM t", "bit_xor"),
    ("SelectStmt", "SELECT STDDEV_POP(a) FROM t", "stddev_pop"),
    ("SelectStmt", "SELECT STDDEV_SAMP(a) FROM t", "stddev_samp"),
    ("SelectStmt", "SELECT VAR_POP(a) FROM t", "var_pop"),
    ("SelectStmt", "SELECT VAR_SAMP(a) FROM t", "var_samp"),
    ("SelectStmt", "SELECT JSON_ARRAYAGG(a) FROM t", "json_arrayagg"),
    ("SelectStmt", "SELECT JSON_OBJECTAGG(a, b) FROM t", "json_objectagg"),
    ("SelectStmt", "SELECT APPROX_COUNT_DISTINCT(a) FROM t", "approx_count_distinct"),
    # TRIM variants
    ("SelectStmt", "SELECT TRIM(a) FROM t", "trim simple"),
    ("SelectStmt", "SELECT TRIM('x' FROM a) FROM t", "trim str from"),
    ("SelectStmt", "SELECT TRIM(LEADING 'x' FROM a) FROM t", "trim leading"),
    ("SelectStmt", "SELECT TRIM(TRAILING 'x' FROM a) FROM t", "trim trailing"),
    ("SelectStmt", "SELECT TRIM(BOTH 'x' FROM a) FROM t", "trim both"),
    # SUBSTRING variants
    ("SelectStmt", "SELECT SUBSTRING(a, 1, 5) FROM t", "substring"),
    ("SelectStmt", "SELECT SUBSTRING(a FROM 1) FROM t", "substring from"),
    ("SelectStmt", "SELECT SUBSTRING(a FROM 1 FOR 5) FROM t", "substring from for"),
    # Date functions
    ("SelectStmt", "SELECT DATE_ADD(a, INTERVAL 1 DAY) FROM t", "date_add"),
    ("SelectStmt", "SELECT DATE_SUB(a, INTERVAL 1 MONTH) FROM t", "date_sub"),
    ("SelectStmt", "SELECT ADDDATE(a, INTERVAL 1 DAY) FROM t", "adddate"),
    ("SelectStmt", "SELECT ADDDATE(a, 1) FROM t", "adddate int"),
    ("SelectStmt", "SELECT SUBDATE(a, INTERVAL 1 HOUR) FROM t", "subdate"),
    ("SelectStmt", "SELECT TIMESTAMPADD(DAY, 1, a) FROM t", "timestampadd"),
    ("SelectStmt", "SELECT TIMESTAMPDIFF(MONTH, a, b) FROM t", "timestampdiff"),
    ("SelectStmt", "SELECT EXTRACT(YEAR FROM a) FROM t", "extract"),
    ("SelectStmt", "SELECT GET_FORMAT(DATE, 'ISO') FROM t", "get_format"),
    ("SelectStmt", "SELECT WEIGHT_STRING(a) FROM t", "weight_string"),
    ("SelectStmt", "SELECT WEIGHT_STRING(a AS CHAR(10)) FROM t", "weight_string as char"),
    ("SelectStmt", "SELECT WEIGHT_STRING(a AS BINARY(10)) FROM t", "weight_string as binary"),
    # Char function
    ("SelectStmt", "SELECT CHAR(65, 66, 67) FROM t", "char()"),
    ("SelectStmt", "SELECT CHAR(65 USING utf8) FROM t", "char() using"),
    # MOD
    ("SelectStmt", "SELECT MOD(a, 3) FROM t", "mod()"),
    # PASSWORD
    ("SelectStmt", "SELECT PASSWORD('pwd') FROM t", "password()"),
    # INSERT function
    ("SelectStmt", "SELECT INSERT('hello', 1, 3, 'bye') FROM t", "insert function"),
    # POSITION
    ("SelectStmt", "SELECT POSITION('he' IN a) FROM t", "position"),
    # JOIN types
    ("SelectStmt", "SELECT * FROM t1 JOIN t2 ON t1.id=t2.id", "inner join"),
    ("SelectStmt", "SELECT * FROM t1 INNER JOIN t2 ON t1.id=t2.id", "inner join explicit"),
    ("SelectStmt", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id=t2.id", "left join"),
    ("SelectStmt", "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id=t2.id", "left outer join"),
    ("SelectStmt", "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id=t2.id", "right join"),
    ("SelectStmt", "SELECT * FROM t1 CROSS JOIN t2", "cross join"),
    ("SelectStmt", "SELECT * FROM t1 NATURAL JOIN t2", "natural join"),
    ("SelectStmt", "SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.id=t2.id", "straight_join"),
    ("SelectStmt", "SELECT * FROM t1 JOIN t2 USING (id)", "join using"),
    ("SelectStmt", "SELECT * FROM t1, t2 WHERE t1.id=t2.id", "cross join comma"),
    # PARTITION selection
    ("SelectStmt", "SELECT * FROM t PARTITION (p1, p2)", "partition selection"),
    # USE INDEX hints
    ("SelectStmt", "SELECT * FROM t USE INDEX (idx)", "use index"),
    ("SelectStmt", "SELECT * FROM t IGNORE INDEX (idx)", "ignore index"),
    ("SelectStmt", "SELECT * FROM t FORCE INDEX (idx)", "force index"),
    # Lateral
    ("SelectStmt", "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.a = t1.a) AS t3", "lateral subquery"),
    # CALL
    ("CallStmt", "CALL proc()", "call proc no args"),
    ("CallStmt", "CALL proc(1, 'str', @var)", "call proc with args"),
    ("CallStmt", "CALL db.proc(1)", "call schema.proc"),
    # BRIE
    ("BRIEStmt", "BACKUP DATABASE * TO 's3://bucket/path'", "backup database"),
    ("BRIEStmt", "RESTORE DATABASE * FROM 's3://bucket/path'", "restore database"),
    # CREATE SEQUENCE
    ("CreateSequenceStmt", "CREATE SEQUENCE s", "create sequence"),
    ("CreateSequenceStmt", "CREATE SEQUENCE IF NOT EXISTS s MINVALUE 1 MAXVALUE 100 INCREMENT 1", "create sequence with options"),
    # DROP SEQUENCE
    ("DropSequenceStmt", "DROP SEQUENCE s", "drop sequence"),
    # ALTER SEQUENCE
    ("AlterSequenceStmt", "ALTER SEQUENCE s INCREMENT 2", "alter sequence"),
    # SPLIT REGION
    ("SplitRegionStmt", "SPLIT TABLE t BETWEEN (1) AND (100) REGIONS 10", "split table between"),
    ("SplitRegionStmt", "SPLIT TABLE t INDEX idx BETWEEN (1) AND (100) REGIONS 5", "split table index"),
    # NON TRANSACTIONAL
    ("NonTransactionalDMLStmt", "BATCH ON id LIMIT 1000 DELETE FROM t WHERE ts < NOW()", "non-transactional delete"),
    # FLASHBACK
    ("FlashbackTableStmt", "FLASHBACK TABLE t TO BEFORE DROP", "flashback table"),
    ("FlashbackTableStmt", "FLASHBACK TABLE t TO BEFORE DROP RENAME TO t2", "flashback table rename"),
    ("FlashbackToTimestampStmt", "FLASHBACK CLUSTER TO TIMESTAMP '2023-01-01'", "flashback cluster to ts"),
    ("FlashbackDatabaseStmt", "FLASHBACK DATABASE mydb TO BEFORE DROP", "flashback database"),
    # RENAME TABLE
    ("RenameTableStmt", "RENAME TABLE t1 TO t2", "rename table"),
    ("RenameTableStmt", "RENAME TABLE t1 TO t2, t3 TO t4", "rename multiple tables"),
    # RECOVER TABLE
    ("RecoverTableStmt", "RECOVER TABLE t", "recover table"),
    # IMPORT INTO
    ("ImportIntoStmt", "IMPORT INTO t FROM '/path/*.csv'", "import into"),
    # SHUTDOWN
    ("ShutdownStmt", "SHUTDOWN", "shutdown"),
    # RESTART  
    ("RestartStmt", "RESTART", "restart"),
    # CREATE BINDING
    ("CreateBindingStmt", "CREATE GLOBAL BINDING FOR SELECT * FROM t USING SELECT /*+ USE_INDEX(t, idx) */ * FROM t", "create binding"),
    ("CreateBindingStmt", "CREATE SESSION BINDING FOR SELECT * FROM t USING SELECT * FROM t USE INDEX (idx)", "create session binding"),
    # DROP BINDING
    ("DropBindingStmt", "DROP GLOBAL BINDING FOR SELECT * FROM t", "drop binding"),
    # SET BINDING
    ("SetBindingStmt", "SET BINDING ENABLED FOR SELECT * FROM t", "set binding enabled"),
    ("SetBindingStmt", "SET BINDING DISABLED FOR SELECT * FROM t", "set binding disabled"),
    # ANALYZE CONFIG
    ("AnalyzeTableStmt", "ANALYZE TABLE t WITH 1024 BUCKETS", "analyze with buckets"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t WITH 4 TOPN", "analyze with topn"),
    ("AnalyzeTableStmt", "ANALYZE TABLE t WITH 0.001 SAMPLERATE", "analyze with samplerate"),
    # LOCK/UNLOCK STATS
    ("LockStatsStmt", "LOCK STATS t", "lock stats table"),
    ("UnlockStatsStmt", "UNLOCK STATS t", "unlock stats table"),
    # OptimizeTable
    ("OptimizeTableStmt", "OPTIMIZE TABLE t1, t2", "optimize table"),
    # CREATE STATISTICS
    ("CreateStatisticsStmt", "CREATE STATISTICS s ON a, b (t)", "create statistics"),
    # DROP STATISTICS
    ("DropStatisticsStmt", "DROP STATISTICS s", "drop statistics"),
    # HELP
    ("HelpStmt", "HELP 'SELECT'", "help"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"Running {len(TEST_CASES)} SQL audit checks against HandParser...\n")

    # Group by rule  
    from collections import defaultdict
    by_rule: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for rule, sql, desc in TEST_CASES:
        by_rule[rule].append((sql, desc))

    all_failures: list[tuple[str, str, str, str]] = []

    for rule, cases in by_rule.items():
        sqls = [sql for sql, desc in cases]
        descs = {sql: desc for sql, desc in cases}
        failures = run_sql_test(sqls, rule.replace("-", "_"))
        for sql, err in failures:
            all_failures.append((rule, descs.get(sql, "?"), sql, err))

    # Print report
    if not all_failures:
        print("✅ ALL CHECKS PASSED - HandParser supports every tested grammar rule")
        return 0

    print(f"❌ FOUND {len(all_failures)} FAILURES:\n")
    current_rule = None
    for rule, desc, sql, err in sorted(all_failures, key=lambda x: x[0]):
        if rule != current_rule:
            print(f"\n── {rule} ──")
            current_rule = rule
        print(f"  FAIL [{desc}]")
        print(f"       SQL: {sql}")
        print(f"       ERR: {err}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
