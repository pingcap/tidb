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

```sql
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

`2026-09-06-fulltext-index-as-multi-valued-index.sql`, beside this document, is
a runnable script covering every case below. Every statement in it has been
executed, and the two in its "definitions that are refused" section are the only
ones that error:

```
make && ./bin/tidb-server
mysql -h 127.0.0.1 -P 4000 -u root \
  < docs/design/2026-09-06-fulltext-index-as-multi-valued-index.sql
```

Setup:

```sql
SET @@tidb_enable_local_match_against = ON;   -- MATCH is evaluated in TiDB; off by default

CREATE TABLE articles (
  id   INT PRIMARY KEY,
  body VARCHAR(500),
  FULLTEXT INDEX idx_body (body)
);
```

Insert enough rows that an index path beats a scan — the optimizer is
cost-based, and on a small table it will scan no matter what is derivable. Then:

```sql
EXPLAIN SELECT id FROM articles WHERE MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
```

```
└─Selection_5            match_against("+distributed", test.articles.body)
  └─IndexMerge_10
    ├─IndexRangeScan_8   index:idx_body(cast(fts_tokenize(`body`, _utf8mb4'STANDARD', 3, 84, 1) as char(84) array))
                         range:["distributed","distributed"]
    └─TableRowIDScan_9   table:articles
```

`SHOW INDEX` reveals what was built; `FTS_TOKENIZE` shows what a row contributes:

```sql
SELECT FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) FROM articles WHERE id = 1;
-- ["tutorial", "provides", "basic", "distributed", "sql", "walkthrough"]
```

**Multi-tenant.** There is no `FULLTEXT` syntax for a leading key column, so
write the index out with the same analyzer literals. It also authorises the
`MATCH` it answers, so no second index is needed:

```sql
KEY idx_tenant_body (tenant_id, (CAST(FTS_TOKENIZE(body, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY)))
```

```
range:[3 "rareone",3 "rareone"]
```

Check the **range**, not the index name: an index chosen on the token alone
looks identical in the plan tree while reading every tenant's rows.

**Queries that deliberately scan**, and should be confirmed to return the same
rows as with the index: a negated `MATCH`, a `MATCH` in one branch of an `OR`, a
prefix-only search such as `+distrib*`, a multi-column `MATCH`, and an index
whose analyzer differs from the query's.

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
