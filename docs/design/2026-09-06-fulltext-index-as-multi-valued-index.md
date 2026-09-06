# Full-text search backed by a multi-valued index

- Author(s): [terry1purcell](https://github.com/terry1purcell)
- Discussion PR: https://github.com/pingcap/tidb/pull/70893
- Tracking Issue: https://github.com/pingcap/tidb/issues/70491

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Trying it by hand](#trying-it-by-hand)
    * [Functional Tests](#functional-tests)
    * [Compatibility Tests](#compatibility-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

`CREATE FULLTEXT INDEX` on a single column is materialised as an ordinary
multi-valued index over the tokenized column, and `MATCH ... AGAINST` is given
an access path that reads it. The `MATCH` remains in the plan as a residual
filter: the index proposes candidate rows, the `MATCH` decides.

## Motivation or Background

There is no columnar engine on this kernel to hold a full-text index, so a
`MATCH ... AGAINST` filter is evaluated over every row: TiDB analyzes each
document at read time and matches it against the compiled query. That is correct
and hopeless for a large corpus.

But there is already a structure that answers "which rows contain this token" —
a multi-valued index. Building the `FULLTEXT` index as one turns full-text
search from a table scan into an index lookup, without a new index type, a new
executor, or a storage change.

## Detailed Design

### DDL

A single-column `FULLTEXT` index is rewritten, before hidden columns are built,
into an expression index over the tokenized column:

```text
FULLTEXT INDEX idx (body)
  =>  INDEX idx ((CAST(FTS_TOKENIZE(`body`, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY)))
```

`FTS_TOKENIZE(text, parser, min_token_size, max_token_size, enable_stopword)`
analyzes text with the pure-Go analyzer and returns its distinct tokens as a
JSON array. Its configuration comes from constant arguments rather than session
variables, which is what makes it deterministic enough for a generated column:
the literals recorded in the schema are the index's analyzer snapshot, so a
later `SET` of the `innodb_ft_*` variables cannot make new rows tokenize
differently from rows already indexed.

Because the rewrite happens before hidden-column construction, the whole
lifecycle — backfill, `ADMIN CHECK`, partitions, `CREATE TABLE LIKE`, rename,
drop — comes from the expression-index machinery that already exists.

`IndexInfo.Tp` keeps `IndexTypeFulltext` as the marker that the index was
declared `FULLTEXT`. `IndexInfo.FullTextInfo` must stay nil: `IsNonKVIndex`
reports true when it is set, which would suppress the KV index this exists to
build. `SHOW CREATE TABLE` reports the declared form, gated on the marker rather
than on the shape of the generated expression, so an expression index a user
wrote over `FTS_TOKENIZE` keeps its own identity.

A **multi-column** `FULLTEXT` index stays metadata-only, since a multi-valued
index covers exactly one expression. `MATCH` over it is evaluated exactly as
before, without an index to narrow the rows.

### Planner

`deriveFTSIndexFilters` turns each `MATCH` filter into the `member of`
predicates it entails and offers them to the index-merge machinery, which needs
to learn nothing about full-text search: it sees the same shape a user could
have written by hand. Required terms become separate conjuncts so they can
intersect; optional terms become one `json_overlaps` so they union.

The predicates are added only to the local condition set that generates paths,
never to `ds.AllConds` — they exist to unlock an access path, and re-evaluating
one per row would re-tokenize the document for a verdict the `MATCH` already
gives.

Nothing is derived unless an index over the matched column exists whose
expression records the analyzer the query compiled with.

### Soundness

The derived predicates become **access** conditions, so rows outside the ranges
they build are never read. Every rule below exists because breaking it returns
fewer rows with the index than without it:

- Only a `MATCH` in **positive position** is used. Under a negation, or in one
  branch of an `OR`, the implication runs the other way, and the terms would
  range over precisely the rows that must be kept.
- A **required clause that contributes no indexable token** — a prefix, or a
  nested group with only optional branches — stops derivation entirely. A
  document satisfying the required clauses matches whether or not any optional
  clause does, so narrowing by optional tokens would drop it.
- **Prohibited terms** are never usable: they exclude documents rather than
  requiring them. **Prefix terms** are dropped, which stays sound because the
  remaining terms still over-approximate.
- A **score comparison** such as `(MATCH(...)) >= 0` is not used. Evaluation is
  no-score, so that predicate keeps every row.

## Test Design

### Trying it by hand

Everything below is copy-pasteable in order against a build of this branch, and
every plan and result shown was captured from an actual run. Start a server with
`make && ./bin/tidb-server` and connect with
`mysql -h 127.0.0.1 -P 4000 -u root`.

#### 1. Create the table

```sql
DROP DATABASE IF EXISTS ftsdemo;
CREATE DATABASE ftsdemo;
USE ftsdemo;

-- MATCH is evaluated in TiDB, and is off by default.
SET @@tidb_enable_local_match_against = ON;

CREATE TABLE articles (
  id    INT PRIMARY KEY,
  title VARCHAR(200),
  body  VARCHAR(500),
  FULLTEXT INDEX idx_body (body)
);
```

`SHOW CREATE TABLE articles` reports the declared form:

```
  FULLTEXT INDEX `idx_body`(`body`) WITH PARSER STANDARD
```

`SHOW INDEX FROM articles` reports what it actually is:

```
articles  1  idx_body  1  NULL  ...  FULLTEXT  ...
          cast(fts_tokenize(`body`, _utf8mb4'STANDARD', 3, 84, 1) as char(84) array)
```

The `3, 84, 1` are the minimum token size, the maximum, and stopwords-enabled,
read from the session once and frozen into the index. See
[§6](#6-where-the-analyzer-comes-from).

#### 2. Load data

The optimizer is cost-based, so a small table is scanned no matter what is
derivable. Give it enough rows and selective enough terms:

```sql
INSERT INTO articles VALUES
  (1, 'MySQL Tutorial',        'This tutorial provides a basic distributed sql walkthrough'),
  (2, 'How To Use MySQL Well', 'After you went through a mysql tutorial on replication'),
  (3, 'Optimizing MySQL',      'In this tutorial we show how to optimize a distributed database'),
  (4, 'MySQL vs PostgreSQL',   'This article compares mysql and postgresql storage engines'),
  (5, 'MySQL Security',        'How to secure your mysql database with proper privileges');

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
```

The tokenizer is callable directly, which is the quickest way to see what a row
contributes to the index:

```sql
SELECT FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) FROM articles WHERE id = 1;
-- ["tutorial", "provides", "basic", "distributed", "sql", "walkthrough"]
```

#### 3. Queries that use the index

Look for `IndexRangeScan ... index:idx_body` under an `IndexMerge`, with the
`MATCH` still above as a `Selection`: the index proposes, the `MATCH` disposes.

```sql
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
```

```
└─Selection_5            match_against("+distributed", ftsdemo.articles.body)
  └─IndexMerge_10
    ├─IndexRangeScan_8   index:idx_body(...)   range:["distributed","distributed"]
    └─TableRowIDScan_9   table:articles
```
→ rows `1, 3`

Optional terms become one range each, unioned:

```sql
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('distributed postgresql' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('distributed postgresql' IN BOOLEAN MODE);
```

```
  └─IndexMerge_11
    ├─IndexRangeScan_8   range:["distributed","distributed"]
    ├─IndexRangeScan_9   range:["postgresql","postgresql"]
    └─TableRowIDScan_10
```
→ rows `1, 3, 4`

Required terms intersect, and a phrase contributes its tokens while adjacency is
left to the residual `MATCH`:

```sql
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed +database' IN BOOLEAN MODE);  -- row 3
SELECT id FROM articles WHERE MATCH(body) AGAINST('"distributed sql"' IN BOOLEAN MODE);       -- row 1
SELECT id FROM articles WHERE MATCH(body) AGAINST('"sql distributed"' IN BOOLEAN MODE);       -- no rows
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed -database' IN BOOLEAN MODE);  -- row 1
```

The last two read the same index range as their counterparts; the `MATCH`
rejects the candidates.

These hand-written forms are what the planner derives internally, and are useful
for checking the index in isolation. Note there is no `CAST` — that is legal only
inside an index definition:

```sql
EXPLAIN SELECT id FROM articles
  WHERE 'distributed' MEMBER OF (FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1));
EXPLAIN SELECT id FROM articles
  WHERE JSON_OVERLAPS(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1), '["distributed", "postgresql"]');
```

#### 4. Queries that deliberately scan

Each of these should show a `TableFullScan`, and each returns the same rows it
would with an index. The first two are correctness requirements, not missed
optimizations — see [Soundness](#soundness).

```sql
-- Negated MATCH: 1003 rows.
EXPLAIN SELECT COUNT(*) FROM articles WHERE NOT MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
SELECT COUNT(*) FROM articles WHERE NOT MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);

-- MATCH in one branch of an OR.
EXPLAIN SELECT COUNT(*) FROM articles
  WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE) OR id < 5;

-- A prefix cannot be a token lookup, so nothing is derivable. Still rows 1, 3.
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distrib*' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('+distrib*' IN BOOLEAN MODE);
```

An index built with a different analyzer cannot answer the query either. Here
`idx_hand` bounds tokens at 5 characters while the FULLTEXT index uses the
session default of 3, so the plan uses `idx_body` and never `idx_hand`:

```sql
CREATE TABLE mismatched (
  id   INT PRIMARY KEY,
  body VARCHAR(500),
  FULLTEXT INDEX idx_body (body),
  KEY idx_hand ((CAST(FTS_TOKENIZE(body, 'STANDARD', 5, 84, 1) AS CHAR(84) ARRAY)))
);
```

And a multi-column FULLTEXT index is metadata-only, so `MATCH` over it scans:

```sql
CREATE TABLE multi (
  id    INT PRIMARY KEY,
  title VARCHAR(200),
  body  VARCHAR(500),
  FULLTEXT INDEX idx_tb (title, body)
);
INSERT INTO multi VALUES (1, 'Distributed SQL', 'a database');
ANALYZE TABLE multi;
EXPLAIN SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+distributed' IN BOOLEAN MODE);
```

#### 5. Multi-tenant: bounding the search to one tenant

A single-column index answers "which rows contain this token" across the whole
table. For "which of *this tenant's* rows", put the tenant column in front.
There is no `FULLTEXT` syntax for that, so write the index out with the same
analyzer literals. It also authorises the `MATCH` it answers, so no second index
is needed:

```sql
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

EXPLAIN SELECT id FROM docs
  WHERE tenant_id = 3 AND MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);
```

```
    ├─IndexRangeScan_9   index:idx_tenant_body(tenant_id, cast(fts_tokenize(...) as char(84) array))
                         range:[3 "rareone",3 "rareone"]
```
→ row `1001` only

**Check the range, not the index name.** `[3 "rareone",3 "rareone"]` is two
columns: one tenant, one token. An index chosen on the token alone looks
identical in the plan tree while reading every tenant's rows.

```sql
-- One range per tenant.
EXPLAIN SELECT id FROM docs
  WHERE tenant_id IN (3, 7) AND MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);

-- No tenant predicate: the leading column is unbounded, so this scans. That is
-- the ordinary index-range rule, not a full-text limitation.
EXPLAIN SELECT id FROM docs WHERE MATCH(body) AGAINST('+rareone' IN BOOLEAN MODE);
```

The other route is to partition the *table* by tenant: the index follows
per-partition automatically, and pruning bounds the search before the index is
consulted. TiDB has no way to partition an index on its own.

#### 6. NULL documents

A `FULLTEXT`-indexed column takes `NULL` like any other. The row stays in the
index, and a `NULL` document matches nothing:

```sql
CREATE TABLE nullable (id INT PRIMARY KEY, body VARCHAR(255), FULLTEXT INDEX idx_body (body));
INSERT INTO nullable VALUES (1, NULL), (2, 'distributed sql');
UPDATE nullable SET body = NULL WHERE id = 2;
UPDATE nullable SET body = 'relational storage' WHERE id = 1;
ADMIN CHECK TABLE nullable;

SELECT id FROM nullable WHERE MATCH(body) AGAINST('+relational' IN BOOLEAN MODE);   -- row 1
SELECT id FROM nullable WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);  -- no rows
```

Building the index over existing `NULL`s works too:

```sql
CREATE TABLE nullable_backfill (id INT PRIMARY KEY, body VARCHAR(255));
INSERT INTO nullable_backfill VALUES (1, NULL), (2, 'distributed sql');
ALTER TABLE nullable_backfill ADD FULLTEXT INDEX idx_body (body);
ADMIN CHECK TABLE nullable_backfill;
```

#### 7. Where the analyzer comes from

An index materialised as a multi-valued index froze its settings into the
expression it is built over, and a query against it compiles with that snapshot
rather than with whatever the session holds later. A metadata-only index has no
snapshot, so it follows the variables. `innodb_ft_min_token_size` is global-only:

```sql
-- Baseline: 'sql' is 3 characters and matches in both tables.
SELECT id FROM articles WHERE MATCH(body) AGAINST('+sql' IN BOOLEAN MODE);        -- row 1
SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+sql' IN BOOLEAN MODE);    -- row 1

SET GLOBAL innodb_ft_min_token_size = 8;

-- Unchanged: the MV-backed index answers with the analyzer it was built with.
SELECT id FROM articles WHERE MATCH(body) AGAINST('+sql' IN BOOLEAN MODE);        -- row 1

-- Now empty: the metadata-only index follows the variable, and 'sql' is below
-- the new minimum.
SELECT id FROM multi WHERE MATCH(title, body) AGAINST('+sql' IN BOOLEAN MODE);    -- no rows

SET GLOBAL innodb_ft_min_token_size = 3;
```

Changing `innodb_ft_*` does not reinterpret an existing index; it changes what a
**new** one would be built with.

#### 8. Definitions that are refused

Two ways to write something that would look fine and answer nothing. Both fail
at definition time rather than silently at query time — these are the only
statements in this walkthrough that error.

```sql
-- Token-size bounds that cross admit no token at all.
CREATE TABLE bad_bounds (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  KEY idx ((CAST(FTS_TOKENIZE(body, 'STANDARD', 84, 3, 1) AS CHAR(84) ARRAY)))
);
```
```
ERROR 1235: ... 'FTS_TOKENIZE() with minimum token size 84 above maximum 3,
                 which admits no token'
```

```sql
-- Two hand-written token indexes over one column that disagree on the analyzer:
-- which one MATCH compiled against would depend on the order they sit in the
-- table.
CREATE TABLE ambiguous (
  id   INT PRIMARY KEY,
  body VARCHAR(255),
  KEY idx_a ((CAST(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY))),
  KEY idx_b ((CAST(FTS_TOKENIZE(body, 'STANDARD', 5, 84, 1) AS CHAR(84) ARRAY)))
);
INSERT INTO ambiguous VALUES (1, 'distributed sql');
SELECT id FROM ambiguous WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
```
```
ERROR 1235: ... 'MATCH ... AGAINST over a column with several FTS_TOKENIZE
                 indexes that disagree on the analyzer'
```

Declaring one of them `FULLTEXT` settles it — the declared index is the one named
for the purpose — and the query succeeds.

#### 9. Other things worth trying

```sql
ADMIN CHECK TABLE articles;                      -- the index holds real, checkable data
ALTER TABLE articles DROP COLUMN body;           -- refused: the index depends on it
ALTER TABLE articles DROP INDEX idx_body;        -- drops the hidden column too
CREATE TABLE ngram_articles (                    -- the other parser
  id   INT PRIMARY KEY,
  body VARCHAR(500),
  FULLTEXT INDEX idx_body (body) WITH PARSER NGRAM
);
```

The gram size comes from `ngram_token_size` (global-only, default 2) and travels
in `FTS_TOKENIZE`'s `min_token_size` slot.

### Functional Tests

| Area | Test |
| --- | --- |
| DDL, both index shapes | `TestCreateTableWithFullTextIndexBuildsMVIndex`, `TestCreateTableWithFullTextIndexMetadata` |
| Backfill over existing rows | included in the above |
| `NULL` documents | `TestFullTextIndexAllowsNullColumn` |
| Access path, six MATCH forms | `TestFTSMatchAgainstUsesMVIndex` |
| Soundness: negation and `OR` | `TestFTSMatchAgainstNegatedKeepsScan` |
| Soundness: unindexable required clause | `TestFTSMatchAgainstUnindexableRequiredTermScans`, `TestQueryIndexTermsRequiredClauseWithoutTokens` |
| Soundness: score comparisons | `TestFTSMatchAgainstComparisonIsRejected` |
| Index terms are an over-approximation | `TestQueryIndexTermsAreSound` |
| Multi-tenant composite index | `TestFTSMatchAgainstUsesCompositeMVIndex` |
| Analyzer source, frozen vs session | `TestFTSMatchAgainstUsesIndexAnalyzerNotSession` |
| Analyzer mismatch and ambiguity | `TestFTSMatchAgainstMismatchedAnalyzerIsNotUsed`, `TestFTSMatchAgainstAmbiguousAnalyzerRejected` |
| NGRAM parser | `TestFTSMatchAgainstNgramUsesMVIndex` |
| Configurations that analyze to nothing | `TestValidateAnalyzerConfigRejectsEmptyTokenStreams` |
| Argument determinism | `TestFTSTokenizeRejectsParameterConfig` |
| Index-merge dependency | `TestFTSMatchAgainstWithoutIndexMergeScans` |

`tests/integrationtest/t/planner/core/fulltext_search.test` passes unchanged,
with the single-column indexes it declares now backed by real index data.

### Compatibility Tests

- **Partitioned tables**: covered by the DDL tests, including `ADMIN CHECK`.
- **Schema tracker (DM)**: `TestFullTextIndexMirrorsExecutor` asserts the
  tracker produces the same metadata the executor does, for `CREATE TABLE`,
  `CREATE INDEX` and `ALTER TABLE ADD`, plus the multi-column shape.
- **Lightning / importer**: build table metadata with no session to read the
  analyzer from, and take the settings a default-configured server would use.
  `TestDefaultAnalyzerConfigMatchesSessionDefaults` pins those against the
  system-variable defaults so the two cannot drift.

## Impacts & Risks

- **A `FULLTEXT` index now occupies KV space and costs writes**, where before it
  held no data. That is the point of the change, but the same DDL statement now
  produces something materially different.
- **Downgrade is not graceful.** An index created by this build is an expression
  index over a hidden generated column. A build without this change reads that
  schema as an ordinary multi-valued index: `SHOW CREATE TABLE` renders the
  expression rather than `FULLTEXT INDEX`, and `MATCH` no longer resolves
  against it.
- **Two shapes coexist.** Indexes created before this change stay metadata-only
  and keep following session analyzer settings, while new ones use their frozen
  snapshot. The same query can therefore answer differently depending on when
  the index was created, if the `innodb_ft_*` variables changed in between.
- **Error messages changed.** `DROP COLUMN` and `MODIFY COLUMN` on an indexed
  column now fail with the generated-column and expression-index errors rather
  than the FULLTEXT ones.
- **The access path requires index merge.** A multi-valued index is reachable
  only through index-merge path generation, so `tidb_enable_index_merge=OFF` or
  a `NO_INDEX_MERGE()` hint makes a `MATCH` scan. Results do not change, only
  the plan. This is true of every multi-valued index, not only full-text ones.

## Investigation & Alternatives

- **Un-gating the columnar full-text index.** `MATCH` on the columnar engine is
  gated by deployment mode. If the classic TiFlash build implements the reader,
  that would be a better answer than this and should be preferred.
- **Pushing a `LIKE` pre-filter down** to TiKV narrows the scan without an index
  but cannot use one; it is complementary rather than an alternative.
- **Rewriting `MATCH` over a JSON column into `MEMBER OF`** was investigated and
  abandoned: it addresses a different shape of data.

## Unresolved Questions

- **Syntax for a leading key column.** The multi-tenant shape has to be written
  out by hand today. It should not be called `PARTITION BY`: it is a leading key
  column, not partitioning, and TiDB has no index-only partitioning — local
  versus global is defined by the table's partitions.
- **Whether a hand-written `FTS_TOKENIZE` index should authorise a `MATCH`.**
  It does today, so that a multi-tenant table needs one index rather than two.
  The alternative is requiring a declared `FULLTEXT` index, at the cost of a
  redundant second copy of the same tokens.
- **Relevance scoring and top-k**, which remain out of scope: the intersection
  worker materialises the full candidate handle map before emitting a table
  task, so there is no early termination to exploit even with a score available.
