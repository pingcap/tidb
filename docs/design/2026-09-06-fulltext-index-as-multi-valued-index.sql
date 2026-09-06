-- ---------------------------------------------------------------------------
-- Full-text search accelerated by a multi-valued index
--
-- A runnable companion to
-- 2026-09-06-fulltext-index-as-multi-valued-index.md, which explains what each
-- section is demonstrating and what to look for in the plans.
--
-- How to run:
--   make                         # builds bin/tidb-server
--   ./bin/tidb-server            # listens on 4000 by default
--   mysql -h 127.0.0.1 -P 4000 -u root \
--     < docs/design/2026-09-06-fulltext-index-as-multi-valued-index.sql
--
-- Every statement here has been executed; the sections marked as refused are
-- the only ones that error.
--
-- What is in place on this branch:
--   * CREATE FULLTEXT INDEX on a single column builds a real multi-valued index
--     over CAST(FTS_TOKENIZE(col, ...) AS CHAR(n) ARRAY). One declaration is
--     all you write; SHOW CREATE TABLE reports it back as FULLTEXT INDEX.
--   * MATCH ... AGAINST is evaluated locally, and the planner derives the
--     `member of` predicates it implies so that index can answer it.
--   * A multi-column FULLTEXT index stays metadata-only - a multi-valued index
--     covers exactly one expression - so MATCH over it still scans.
--
-- The analyzer settings are frozen into the index when it is created, and a
-- query against that index compiles with them rather than with whatever the
-- session holds later. Section 7 shows this.
-- ---------------------------------------------------------------------------

DROP DATABASE IF EXISTS ftsdemo;
CREATE DATABASE ftsdemo;
USE ftsdemo;

-- MATCH ... AGAINST is evaluated in TiDB and is off by default.
SET @@tidb_enable_local_match_against = ON;

-- ---------------------------------------------------------------------------
-- 1. Table with a STANDARD-parser full-text index
--
-- Nothing but the FULLTEXT declaration. DDL reads the session analyzer settings
-- once, here, and writes them into the index as literals:
--     innodb_ft_min_token_size   = 3
--     innodb_ft_max_token_size   = 84    (also the CHAR(n) element width)
--     innodb_ft_enable_stopword  = ON
-- ---------------------------------------------------------------------------
CREATE TABLE articles (
  id    INT PRIMARY KEY,
  title VARCHAR(200),
  body  VARCHAR(500),
  FULLTEXT INDEX idx_body (body)
);

-- What DDL actually built. The declared form is what is reported back; the
-- expression and its hidden generated column are an implementation detail.
SHOW CREATE TABLE articles;
SHOW INDEX FROM articles;

-- ---------------------------------------------------------------------------
-- 2. Data
--
-- Bulk filler plus a few rows holding rare tokens. The optimizer is cost-based,
-- so on a tiny table it will pick a full scan no matter what is derived; give
-- it enough rows and selective enough terms for the index to be the cheap plan.
-- ---------------------------------------------------------------------------
INSERT INTO articles VALUES
  (1, 'MySQL Tutorial',        'This tutorial provides a basic distributed sql walkthrough'),
  (2, 'How To Use MySQL Well', 'After you went through a mysql tutorial on replication'),
  (3, 'Optimizing MySQL',      'In this tutorial we show how to optimize a distributed database'),
  (4, 'MySQL vs PostgreSQL',   'This article compares mysql and postgresql storage engines'),
  (5, 'MySQL Security',        'How to secure your mysql database with proper privileges');

-- Filler, so the table is large enough for an index path to win on cost.
INSERT INTO articles
SELECT n + 100, CONCAT('Filler ', n), CONCAT('common filler text number ', n)
FROM (
  SELECT a.n + b.n * 10 + c.n * 100 AS n
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) nums;

ANALYZE TABLE articles;

-- Worth a look: the tokens actually indexed for a row.
SELECT FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) FROM articles WHERE id = 1;

-- ---------------------------------------------------------------------------
-- 3. MATCH queries that SHOULD use idx_body
--
-- Look for `IndexRangeScan ... index:idx_body` under an IndexMerge in each
-- EXPLAIN. The MATCH stays above as a Selection: the index picks candidates,
-- the MATCH decides.
-- ---------------------------------------------------------------------------

-- A required term becomes one index lookup.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- Required terms intersect.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed +database' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed +database' IN BOOLEAN MODE);

-- Optional terms union (one IndexRangeScan per token).
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('distributed postgresql' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('distributed postgresql' IN BOOLEAN MODE);

-- A phrase contributes its tokens; adjacency is checked by the residual MATCH,
-- so 'distributed sql' matches row 1 but 'sql distributed' matches nothing.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('"distributed sql"' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"distributed sql"' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"sql distributed"' IN BOOLEAN MODE);

-- A prohibited term narrows nothing itself, but must not stop the required one.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed -database' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed -database' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 4. Equivalent hand-written queries
--
-- These are the predicates the planner derives internally. Useful for checking
-- the index in isolation.
--
-- Note the shape: no CAST. CAST(... AS CHAR(n) ARRAY) is legal only inside an
-- index definition, so a query names the bare FTS_TOKENIZE(...) expression,
-- with the same literals the index was built with.
-- ---------------------------------------------------------------------------
EXPLAIN SELECT id FROM articles
  WHERE 'distributed' MEMBER OF (FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1));

EXPLAIN SELECT id FROM articles
  WHERE JSON_OVERLAPS(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1), '["distributed", "postgresql"]');

EXPLAIN SELECT id FROM articles
  WHERE JSON_CONTAINS(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1), '["distributed", "database"]');

-- ---------------------------------------------------------------------------
-- 5. Cases that deliberately do NOT use the index
--
-- Each of these should show a TableFullScan. The first two are correctness
-- requirements, not missed optimizations: the derived tokens select the rows a
-- MATCH keeps, so under a negation or in one branch of an OR they would range
-- over exactly the rows that must be returned and lose them.
-- ---------------------------------------------------------------------------

-- Negated MATCH.
EXPLAIN SELECT COUNT(*) FROM articles WHERE NOT MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
SELECT COUNT(*) FROM articles WHERE NOT MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- MATCH in one branch of an OR.
EXPLAIN SELECT COUNT(*) FROM articles
  WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE) OR id < 5;

-- A prefix term cannot be expressed as a token lookup, so nothing is derived.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distrib*' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distrib*' IN BOOLEAN MODE);

-- A hand-written index whose analyzer differs from the FULLTEXT index the
-- MATCH compiles against: idx_hand bounds tokens at 5 characters, so its
-- entries were produced by a different token stream and cannot answer the
-- query. Expect idx_body, never idx_hand.
CREATE TABLE mismatched (
  id   INT PRIMARY KEY,
  body VARCHAR(500),
  FULLTEXT INDEX idx_body (body),
  KEY idx_hand ((CAST(FTS_TOKENIZE(body, 'STANDARD', 5, 84, 1) AS CHAR(84) ARRAY)))
);
INSERT INTO mismatched VALUES (1, 'distributed sql database');
INSERT INTO mismatched
SELECT n + 100, CONCAT('common filler text number ', n) FROM (
  SELECT a.n + b.n * 10 + c.n * 100 AS n
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) nums;
ANALYZE TABLE mismatched;
EXPLAIN SELECT id FROM mismatched WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- A multi-column FULLTEXT index is metadata-only, so MATCH over it scans.
CREATE TABLE multi (
  id    INT PRIMARY KEY,
  title VARCHAR(200),
  body  VARCHAR(500),
  FULLTEXT INDEX idx_tb (title, body)
);
INSERT INTO multi VALUES (1, 'Distributed SQL', 'a database');
ANALYZE TABLE multi;
EXPLAIN SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+distributed' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 6. NGRAM parser
--
-- The ngram parser sizes its grams from the min_token_size argument, and that
-- same size is the CHAR(n) element width. ngram_token_size is a GLOBAL-only
-- variable and defaults to 2, which is what the index below is built for.
-- ---------------------------------------------------------------------------
CREATE TABLE ngram_articles (
  id   INT PRIMARY KEY,
  body VARCHAR(500),
  FULLTEXT INDEX idx_body (body) WITH PARSER NGRAM
);
INSERT INTO ngram_articles
SELECT n, CONCAT('common filler text number ', n) FROM (
  SELECT a.n + b.n * 10 + c.n * 100 AS n
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) nums;
INSERT INTO ngram_articles VALUES (1001, 'zq alone here');
ANALYZE TABLE ngram_articles;

EXPLAIN SELECT id FROM ngram_articles WHERE MATCH(body) AGAINST('+zq' IN BOOLEAN MODE);
SELECT id FROM ngram_articles WHERE MATCH(body) AGAINST('+zq' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 6b. Multi-tenant: bounding the token lookup to one tenant
--
-- A single-column FULLTEXT index answers "which rows contain this term" across
-- the whole table, so a tenant filter can only be applied after reading every
-- tenant's matching rows. To bound the lookup, put the tenant column in front
-- of the tokenized one. There is no FULLTEXT syntax for that yet - MySQL's
-- FULLTEXT(a, b) means multi-column search, not a prefix - so write the index
-- out, using the same analyzer literals a FULLTEXT index would freeze in.
--
-- This index also authorises the MATCH it answers; a separate FULLTEXT index is
-- not needed, and would only store a second copy of the same tokens.
-- ---------------------------------------------------------------------------
CREATE TABLE docs (
  id        INT PRIMARY KEY,
  tenant_id INT,
  body      VARCHAR(255),
  KEY idx_tenant_body (tenant_id, (CAST(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY)))
);
INSERT INTO docs
SELECT n, n % 10, CONCAT('common filler text number ', n) FROM (
  SELECT a.n + b.n * 10 + c.n * 100 AS n
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) nums;
-- The same rare token in two different tenants.
INSERT INTO docs VALUES (1001, 3, 'rareone alone here'), (1002, 7, 'rareone elsewhere here');
ANALYZE TABLE docs;

-- Both columns in the range: `range:[3 "rareone",3 "rareone"]`. That is the
-- thing to check - an index chosen on the token alone looks the same in the
-- plan tree while reading every tenant's rows.
EXPLAIN SELECT id FROM docs
  WHERE tenant_id = 3 AND MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);
SELECT id FROM docs
  WHERE tenant_id = 3 AND MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);

-- An IN list becomes one range per tenant.
EXPLAIN SELECT id FROM docs
  WHERE tenant_id IN (3, 7) AND MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);

-- No tenant predicate: the leading column is unbounded, so there is no range to
-- build and this scans. That is the ordinary index-range rule, not a full-text
-- limitation.
EXPLAIN SELECT id FROM docs WHERE MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 6c. NULL documents
--
-- A FULLTEXT-indexed column takes NULL like any other. The row stays in the
-- index, and a NULL document matches nothing.
-- ---------------------------------------------------------------------------
CREATE TABLE nullable (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  FULLTEXT INDEX idx_body (body)
);
INSERT INTO nullable VALUES (1, NULL), (2, 'distributed sql');
UPDATE nullable SET body = NULL WHERE id = 2;
UPDATE nullable SET body = 'relational storage' WHERE id = 1;
ADMIN CHECK TABLE nullable;

SELECT id, body FROM nullable ORDER BY id;
-- Matches row 1 only; the NULL document matches nothing.
SELECT id FROM nullable WHERE MATCH(body) AGAINST('+relational' IN BOOLEAN MODE);
SELECT id FROM nullable WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- Building the index over existing NULLs works too.
CREATE TABLE nullable_backfill (id INT PRIMARY KEY, body VARCHAR(255));
INSERT INTO nullable_backfill VALUES (1, NULL), (2, 'distributed sql');
ALTER TABLE nullable_backfill ADD FULLTEXT INDEX idx_body (body);
ADMIN CHECK TABLE nullable_backfill;
SELECT id FROM nullable_backfill WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 6d. Definitions that are refused
--
-- Two ways to write an index that would look fine and answer nothing. Both are
-- rejected at definition time rather than at query time. Expect an error from
-- each statement in this section.
-- ---------------------------------------------------------------------------

-- Token-size bounds that cross admit no token at all, so the index would be
-- built, hold nothing, and match nothing.
CREATE TABLE bad_bounds (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  KEY idx ((CAST(FTS_TOKENIZE(body, 'STANDARD', 84, 3, 1) AS CHAR(84) ARRAY)))
);

-- Two hand-written token indexes over one column that disagree on the
-- analyzer: which one MATCH compiled against would depend on the order they
-- sit in the table, so the query is refused instead.
CREATE TABLE ambiguous (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  KEY idx_a ((CAST(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY))),
  KEY idx_b ((CAST(FTS_TOKENIZE(body, 'STANDARD', 5, 84, 1) AS CHAR(84) ARRAY)))
);
INSERT INTO ambiguous VALUES (1, 'distributed sql');
SELECT id FROM ambiguous WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- Declaring one of them FULLTEXT settles it: the declared index is the one
-- named for the purpose, so this succeeds.
CREATE TABLE settled (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  FULLTEXT INDEX idx_ft (body),
  KEY idx_hand ((CAST(FTS_TOKENIZE(body, 'STANDARD', 5, 84, 1) AS CHAR(84) ARRAY)))
);
INSERT INTO settled VALUES (1, 'distributed sql');
SELECT id FROM settled WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- ---------------------------------------------------------------------------
-- 7. Where the analyzer comes from
--
-- The index froze its settings at creation, so a later variable change does not
-- reinterpret rows already indexed. A metadata-only index has no such snapshot
-- and does follow the variables. innodb_ft_min_token_size is GLOBAL-only.
-- ---------------------------------------------------------------------------
-- Baseline: 'sql' is 3 characters and matches in both tables.
SELECT id FROM articles WHERE MATCH(body) AGAINST('+sql' IN BOOLEAN MODE);
SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+sql' IN BOOLEAN MODE);

SET GLOBAL innodb_ft_min_token_size = 8;

-- Unchanged: the MV-backed index answers with the analyzer it was built with,
-- which still admits a 3-character token.
SELECT id FROM articles WHERE MATCH(body) AGAINST('+sql' IN BOOLEAN MODE);

-- Now empty: the metadata-only index has no frozen analyzer, so it follows the
-- variable, and 'sql' is below the new minimum and analyzes away.
SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+sql' IN BOOLEAN MODE);

SET GLOBAL innodb_ft_min_token_size = 3;

-- ---------------------------------------------------------------------------
-- 8. Other things worth poking at
--
--   * ADMIN CHECK TABLE articles;   -- the index holds real, checkable data
--   * ALTER TABLE articles DROP INDEX idx_body;   -- drops the hidden column too
--   * ALTER TABLE articles ADD FULLTEXT INDEX idx_body (body);  -- same result
--     as declaring it inline, via a backfill
--   * ALTER TABLE articles DROP COLUMN body;   -- refused: the index depends on it
-- ---------------------------------------------------------------------------
