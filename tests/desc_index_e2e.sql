-- End-to-end smoke test for descending-order indexes (pingcap/tidb#2519).
--
-- Run via the wrapper at tests/run_desc_index_e2e.sh, which boots a tiup
-- playground cluster against caller-supplied TiDB and TiKV binaries:
--
--   TIDB_BIN=/path/to/bin/tidb-server \
--   TIKV_BIN=/path/to/target/release/tikv-server \
--     ./tests/run_desc_index_e2e.sh
--
-- Or directly against any cluster that exposes 127.0.0.1:4000:
--
--   mysql -h 127.0.0.1 -P 4000 -u root --force < tests/desc_index_e2e.sql
--
-- Each section runs a small SQL block with the expected behaviour annotated
-- above it. A failure (wrong row order, missing rows, unexpected error)
-- indicates the wire format between TiDB and TiKV is inconsistent for that
-- scenario. Section 3 deliberately exercises a DDL that must be rejected,
-- so use --force when running interactively.

-- ============================================================================
-- 0. Setup
-- ============================================================================
CREATE DATABASE IF NOT EXISTS test;
USE test;

-- Drop any leftover state from a previous run.
DROP TABLE IF EXISTS desc_e2e_single;
DROP TABLE IF EXISTS desc_e2e_mixed;
DROP TABLE IF EXISTS desc_e2e_pk_clustered_should_fail;
DROP TABLE IF EXISTS desc_e2e_pk_nonclustered;
DROP TABLE IF EXISTS desc_e2e_strings;
DROP TABLE IF EXISTS desc_e2e_expression;

-- The feature is opt-in.
SELECT @@global.tidb_enable_descending_index AS before_set;
SET @@global.tidb_enable_descending_index = ON;
SELECT @@global.tidb_enable_descending_index AS after_set;

-- ============================================================================
-- 1. Single descending column: INDEX(b DESC)
-- ============================================================================
CREATE TABLE desc_e2e_single (a INT, b INT, INDEX idx_b (b DESC));
INSERT INTO desc_e2e_single VALUES (1, 10), (2, 20), (3, 5), (4, 30), (5, 15);

-- Metadata round-trip
SHOW CREATE TABLE desc_e2e_single;
SELECT collation FROM information_schema.statistics WHERE table_name='desc_e2e_single' AND index_name='idx_b';
-- Expected: D

-- Forward scan satisfies ORDER BY b DESC: no Sort, no 'desc' flag.
EXPLAIN FORMAT='brief' SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b DESC;
SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b DESC;
-- Expected order: 30, 20, 15, 10, 5

-- Reverse scan satisfies ORDER BY b ASC: 'keep order:true, desc' in plan.
EXPLAIN FORMAT='brief' SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b ASC;
SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b ASC;
-- Expected order: 5, 10, 15, 20, 30

-- Point lookup on the DESC column.
EXPLAIN FORMAT='brief' SELECT a FROM desc_e2e_single USE INDEX(idx_b) WHERE b = 20;
SELECT a FROM desc_e2e_single USE INDEX(idx_b) WHERE b = 20;
-- Expected: 2

SELECT a FROM desc_e2e_single USE INDEX(idx_b) WHERE b = 999;
-- Expected: empty result

-- Range scan
EXPLAIN FORMAT='brief' SELECT b FROM desc_e2e_single USE INDEX(idx_b) WHERE b BETWEEN 10 AND 25 ORDER BY b DESC;
SELECT b FROM desc_e2e_single USE INDEX(idx_b) WHERE b BETWEEN 10 AND 25 ORDER BY b DESC;
-- Expected: 20, 15, 10

-- ============================================================================
-- 2. Mixed-direction composite: INDEX(a, b DESC)  — MySQL 8.0 flagship case
-- ============================================================================
CREATE TABLE desc_e2e_mixed (a INT, b INT, INDEX idx_ab (a, b DESC));
INSERT INTO desc_e2e_mixed VALUES (1, 10), (1, 5), (1, 15), (2, 20), (2, 8), (2, 12);

-- Forward scan satisfies ORDER BY a ASC, b DESC (column-by-column match).
EXPLAIN FORMAT='brief' SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a ASC, b DESC;
SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a ASC, b DESC;
-- Expected: (1,15)(1,10)(1,5)(2,20)(2,12)(2,8)

-- Reverse scan satisfies ORDER BY a DESC, b ASC (bitwise complement).
EXPLAIN FORMAT='brief' SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a DESC, b ASC;
SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a DESC, b ASC;
-- Expected: (2,8)(2,12)(2,20)(1,5)(1,10)(1,15)

-- Unsatisfiable by either direction: planner inserts Sort.
EXPLAIN FORMAT='brief' SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a, b;
SELECT a, b FROM desc_e2e_mixed USE INDEX(idx_ab) ORDER BY a, b;
-- Expected: (1,5)(1,10)(1,15)(2,8)(2,12)(2,20)

-- ============================================================================
-- 3. PRIMARY KEY direction guards
-- ============================================================================

-- Clustered PK with DESC must be rejected at DDL time.
-- Expected: error "DESC is not supported on the columns of a clustered PRIMARY KEY"
CREATE TABLE desc_e2e_pk_clustered_should_fail (a INT, b INT, PRIMARY KEY (a, b DESC) CLUSTERED);

-- NONCLUSTERED PK with DESC is allowed (encoded as a unique secondary index).
CREATE TABLE desc_e2e_pk_nonclustered (a INT, b INT, PRIMARY KEY (a DESC) NONCLUSTERED);
INSERT INTO desc_e2e_pk_nonclustered VALUES (10, 1), (5, 2), (20, 3), (1, 4);
SELECT a FROM desc_e2e_pk_nonclustered USE INDEX(`PRIMARY`) ORDER BY a DESC;
-- Expected: 20, 10, 5, 1

-- ============================================================================
-- 4. String column DESC
-- ============================================================================
CREATE TABLE desc_e2e_strings (id INT, name VARCHAR(32), INDEX idx_name (name DESC));
INSERT INTO desc_e2e_strings VALUES (1,'apple'),(2,'banana'),(3,'cherry'),(4,'date'),(5,'elderberry');

EXPLAIN FORMAT='brief' SELECT name FROM desc_e2e_strings USE INDEX(idx_name) ORDER BY name DESC;
SELECT name FROM desc_e2e_strings USE INDEX(idx_name) ORDER BY name DESC;
-- Expected: elderberry, date, cherry, banana, apple

SELECT id FROM desc_e2e_strings USE INDEX(idx_name) WHERE name = 'cherry';
-- Expected: 3

-- ============================================================================
-- 5. Expression index with DESC
-- ============================================================================
CREATE TABLE desc_e2e_expression (a INT, b INT, INDEX idx_expr ((a + b) DESC));
INSERT INTO desc_e2e_expression VALUES (1,10),(2,20),(3,5),(4,30),(5,15);

SHOW CREATE TABLE desc_e2e_expression;
SELECT collation FROM information_schema.statistics WHERE table_name='desc_e2e_expression';
-- Expected: D

EXPLAIN FORMAT='brief' SELECT a, b FROM desc_e2e_expression USE INDEX(idx_expr) ORDER BY (a + b) DESC;
SELECT a, b FROM desc_e2e_expression USE INDEX(idx_expr) ORDER BY (a + b) DESC;
-- Expected: rows with a+b in descending order: (4,30)=34, (2,20)=22, (5,15)=20, (1,10)=11, (3,5)=8

-- ============================================================================
-- 6. UPDATE / DELETE against DESC indexes
-- ============================================================================
UPDATE desc_e2e_single SET a = a + 100 WHERE b = 20;
SELECT * FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b DESC;
-- Expected: row (102, 20) shows updated a

DELETE FROM desc_e2e_single WHERE b = 5;
SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b DESC;
-- Expected: 30, 20, 15, 10 (5 is gone)

-- ============================================================================
-- 7. Sysvar create-time-only semantics
-- ============================================================================
SET @@global.tidb_enable_descending_index = OFF;
-- Existing DESC tables continue to work after the flip.
SELECT b FROM desc_e2e_single USE INDEX(idx_b) ORDER BY b DESC;
-- Expected: 30, 20, 15, 10

-- New CREATE INDEX silently drops DESC when the gate is off.
CREATE TABLE desc_e2e_off_new (x INT, INDEX idx_x (x DESC));
SELECT collation FROM information_schema.statistics WHERE table_name='desc_e2e_off_new';
-- Expected: A (DESC was dropped)
DROP TABLE desc_e2e_off_new;

-- ============================================================================
-- 8. Cleanup
-- ============================================================================
SET @@global.tidb_enable_descending_index = DEFAULT;
DROP TABLE IF EXISTS desc_e2e_single;
DROP TABLE IF EXISTS desc_e2e_mixed;
DROP TABLE IF EXISTS desc_e2e_pk_nonclustered;
DROP TABLE IF EXISTS desc_e2e_strings;
DROP TABLE IF EXISTS desc_e2e_expression;

SELECT 'e2e test complete' AS status;
