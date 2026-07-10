# Local No-Score Execution for MATCH ... AGAINST

- Author(s): TBD
- Discussion PR: TBD
- Tracking issue: TBD

## Summary

TiDB can evaluate a restricted `MATCH ... AGAINST` predicate locally when a
TiCI full-text access path cannot see the latest row or when the optimizer
chooses a cheaper normal index/table path with a local residual filter.

The local result is a match flag, not a relevance score:

- `1.0` means the row matches;
- `0.0` means it does not match;
- `NULL` is returned for `AGAINST(NULL)`.

TiCI remains the indexed full-text execution path. Local execution requires a
matching regular FULLTEXT index and uses the exact analyzer configuration
captured when that index was created.

## Scope

The first implementation supports:

- direct boolean filter contexts such as `WHERE MATCH(...) AGAINST(...)`;
- `IN BOOLEAN MODE` without query expansion;
- `STANDARD_V1` and `NGRAM_V1` regular FULLTEXT indexes;
- optional, required, prohibited, prefix, and quoted phrase terms;
- dirty transactions, where TiCI cannot observe uncommitted rows;
- cost competition between a TiCI path and a normal path with local residual
  evaluation.

It does not support:

- relevance scoring;
- natural-language mode or query expansion;
- projection, ordering, arithmetic, `CASE`, or threshold comparisons that use
  the numeric value as a score;
- TiCI helper functions such as `fts_match_word` outside TiCI;
- `MULTILINGUAL_V1` local analysis;
- nested boolean groups, score modifiers, or phrase proximity;
- local execution without a matching FULLTEXT index.

The feature is guarded by `tidb_enable_local_match_against`. Local-vs-TiCI CBO
also requires `tidb_opt_enable_alternative_logical_plans`.

## SQL and planner contract

Eligible examples:

```sql
SELECT * FROM t
WHERE MATCH(title) AGAINST('+tidb -mysql' IN BOOLEAN MODE);

SELECT * FROM t
WHERE status = 'open'
  AND MATCH(title, body) AGAINST('"distributed sql"' IN BOOLEAN MODE);
```

Score-dependent examples remain unsupported locally:

```sql
SELECT MATCH(title) AGAINST('tidb' IN BOOLEAN MODE) FROM t;
SELECT * FROM t ORDER BY MATCH(title) AGAINST('tidb' IN BOOLEAN MODE);
SELECT * FROM t WHERE MATCH(title) AGAINST('tidb' IN BOOLEAN MODE) > 0.5;
```

The expression rewriter records whether MATCH occurs in a direct boolean
context. The normal optimization round builds the native TiCI candidate. When
both feature gates are enabled, a separate alternative round rebuilds the
query with eligible MATCH predicates retained as a TiDB-side `Selection`.
Normal access-path enumeration and cost comparison then choose between the
TiCI plan and the local residual plan.

Local binding verifies all of the following before a plan is executable:

1. the expression is a direct boolean filter in boolean mode;
2. every MATCH argument is a column of one table;
3. a public regular FULLTEXT index has the same column set;
4. the index has a persisted analyzer snapshot;
5. all matching indexes agree on the analyzer configuration;
6. the current literal or prepared parameter parses and normalizes
   successfully.

Validation happens during plan binding, before the executor reads its first
row. This makes syntax errors deterministic even when an earlier predicate
produces no input rows. Prepared statements are excluded from plan cache
because the search parameter must be compiled for every execution.

## Analyzer snapshot

Parser type alone does not define a full-text token stream. Token-size limits,
NGRAM size, stopword enablement, and custom stopword contents can all change
after an index is built. Switching between local and TiCI must not change the
result set, so local execution never reconstructs these values from query-time
session variables.

`model.FullTextIndexInfo.ParserConfig` stores the immutable parser parameters
and stopword list sent to TiCI. DDL and local planning consume the same
snapshot:

```go
type FullTextIndexParserConfig struct {
    ParserParams map[string]string
    StopWords    []string
}
```

Maps and slices are deep-cloned with `IndexInfo`. Snapshot JSON is limited to
1 MiB for one index and for the aggregate of all FULLTEXT indexes on a table.
The aggregate check applies to both CREATE TABLE and sequential ALTER ADD
FULLTEXT INDEX operations.

### DDL durability

CREATE TABLE may call TiCI before the TiDB schema transaction commits. The
executor therefore resolves stopwords and stores the snapshot in the initial
DDL job arguments before enqueueing the job. A retry cannot send one analyzer
to TiCI and later persist another after a stopword table changes.

ADD FULLTEXT INDEX persists its snapshot in the first schema-state transition,
before TiCI creation/backfill side effects. ADD PARTITION reuses the existing
index snapshots when grouping TiCI requests.

Metadata written before analyzer snapshots existed cannot prove equivalence.
Local execution and new ADD PARTITION jobs reject such indexes and ask the
user to rebuild them. A rolling-upgrade job that already performed TiCI
partition side effects is still allowed to roll back. That cleanup does not
re-read a custom stopword table; if an old persisted group hash contains the
former stopword contents, rollback maps the unmatched marker to the one
mutable legacy STANDARD group and drops the recorded partition for its index
IDs.

## Local query and document model

`pkg/expression/fulltext` compiles boolean syntax into a no-score query tree:

```text
group
  must:    term | prefix | phrase
  should:  term | prefix | phrase
  mustNot: term | prefix | phrase
```

Filtering rules match the existing TiCI no-score rewrite:

- all required clauses must match;
- all prohibited clauses must not match;
- optional clauses form an OR only when there is no required clause;
- optional clauses do not affect filtering when a required clause exists;
- a query with only prohibited clauses matches no rows;
- an empty or analyzer-filtered required query matches no rows.

Each MATCH column is analyzed independently. A row document stores its token
stream, unique-token set, token frequencies, and per-token positions. Phrase
matching never crosses a column boundary. NULL and empty columns contribute no
tokens.

### STANDARD_V1

STANDARD uses the TiCI-compatible PreserveUnderscore tokenizer, token-length
filter, lowercase filter, and captured stopword set.

A normal term must analyze to exactly one token. A prefix term intentionally
bypasses the minimum-size and stopword filters, matching boolean wildcard
semantics, but still applies tokenization, lowercase conversion, and the
maximum-size limit.

Analyzer-filtered phrase tokens retain their original positions. For example,
if `a` is filtered from `"foo a bar"`, the remaining tokens keep a gap and
match `foo x bar`, not adjacent `foo bar`.

### NGRAM_V1

NGRAM uses the captured `ngram_token_size`. A normal term becomes an ordered
phrase of its generated grams. Prefix normalization follows the existing TiCI
prefix helper contract and requires exactly one generated prefix token.

## Matching complexity

Term lookup is O(1) in the row token set. Prefix matching scans the unique
token set.

Dense phrases use KMP over each column token stream. The failure table is
compiled once with the query, so repeated-token misses are O(query tokens +
document tokens), including when query and document sizes grow together.

Sparse phrases contain analyzer position gaps. They anchor on the rarest
query token in the document and intersect sorted position lists with two
pointers. This avoids repeatedly testing every candidate start against every
later token. Query metadata records the additional document passes so the cost
model can price this path conservatively.

Queries proven unable to match return before building a row document. This is
both an execution optimization and a cost-model invariant.

## Selectivity and cost

The local residual `Selection` derives statistics from its child:

- a query proven unable to match has zero selectivity;
- a single ordinary positive STANDARD term may use an analyzed ILIKE proxy;
- NGRAM, prefix, phrase, multiple-clause, and multi-column searches retain a
  conservative default rather than claiming false precision;
- `AGAINST(NULL)` substitutes a NULL constant before the single-column proxy
  restriction, preserving three-valued logic for any column count.

The local CPU estimate is shared by cost model v1 and v2. It grows with:

- average bytes of all matched columns;
- fixed per-document query work;
- the number of document-sized scans required by prefix or phrase matching.

Document bytes and scan count are multiplicative. A query proven false has no
document-sized cost because execution returns before analysis.

TiCI path statistics are applied only when TiCI is the selected full-text
access path. Residual filters remain available for the alternative normal
path, so their selectivity and CPU cost can participate in the final CBO
comparison.

## EXPLAIN EXPLORE

EXPLAIN EXPLORE treats both feature gates as relevant variables for an
eligible MATCH predicate. Recording the dependency even when both defaults
are OFF lets breadth-first exploration reach the joint ON state.

Every generated state restores optimizer variables and fix-control settings.
The full generation call also restores the pooled session's current database
and cost-model version.

The emitted EXPLAIN ANALYZE and CREATE BINDING SQL includes:

```sql
SET_VAR(tidb_enable_local_match_against=ON)
SET_VAR(tidb_opt_enable_alternative_logical_plans=ON)
```

when those gates are required by the displayed local residual plan. Therefore
the output commands reproduce the candidate even when the caller keeps both
session variables OFF.

## Compatibility and failure behavior

- With `tidb_enable_local_match_against=OFF`, existing TiCI and ILIKE fallback
  behavior is unchanged.
- A dirty transaction uses local evaluation only when the local gate is ON;
  otherwise it retains the existing TiCI dirty-write error.
- Missing, unsupported, or conflicting analyzer metadata rejects local
  binding instead of silently using query-time defaults.
- TiCI helper functions remain non-local and non-pushdown local MATCH builtins
  remain TiDB-side expressions.
- Unsupported syntax fails during binding and never falls back to a broader
  substring interpretation.

## Test plan

Unit tests cover:

- STANDARD and NGRAM tokenization;
- required, prohibited, optional, prefix, dense phrase, and sparse phrase
  semantics;
- NULL, empty, stopword-filtered, and pure-negative queries;
- repeated-token phrase misses and coupled query/document growth;
- query work and document-scan cost metadata;
- parser snapshot cloning and size limits.

Planner and DDL tests cover:

- dirty-write local fallback and the OFF guard;
- plan-time validation for empty input and prepared parameters;
- analyzer behavior after token sysvars or stopword tables change;
- local-vs-TiCI CBO selecting a selective normal index;
- CREATE TABLE snapshot durability before the first worker step;
- sequential ADD FULLTEXT aggregate metadata limits;
- rejection of new ADD PARTITION on legacy metadata;
- cleanup of partial legacy ADD PARTITION side effects without rereading
  stopwords;
- deterministic EXPLAIN EXPLORE output and executable emitted commands.

## Alternatives

Expanding the ILIKE fallback is not a substitute for local full-text
evaluation: substring matching cannot reproduce token boundaries, positions,
stopwords, phrases, or NGRAM behavior.

A persistent local inverted index could improve large scans, but requires a
separate storage, DDL, consistency, recovery, and statistics design. Relevance
scoring similarly needs an explicit ranking contract before local MATCH can be
used outside direct boolean filters.

## Open questions

- When should the two experimental feature gates become defaults?
- Should a future local path be allowed without an existing FULLTEXT index,
  and if so, what defines its analyzer contract?
- Which ranking model should a later score-producing implementation expose?
