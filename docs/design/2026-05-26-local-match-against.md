# Local No-Score Execution for MATCH ... AGAINST

- Author(s): TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Current Behavior](#current-behavior)
- [Detailed Design](#detailed-design)
  - [Execution Contract](#execution-contract)
  - [Planner Selection](#planner-selection)
  - [Modifier Handling Before Local Binding](#modifier-handling-before-local-binding)
  - [Alternative Plan and ILIKE Fallback Interaction](#alternative-plan-and-ilike-fallback-interaction)
  - [Analyzer Selection](#analyzer-selection)
  - [Analyzer Configuration](#analyzer-configuration)
  - [Local Evaluator Metadata](#local-evaluator-metadata)
  - [Document Representation](#document-representation)
  - [Query IR](#query-ir)
  - [Boolean Mode](#boolean-mode)
  - [Term Normalization](#term-normalization)
  - [NGRAM Semantics](#ngram-semantics)
  - [Match Algorithms](#match-algorithms)
  - [Expression Evaluation](#expression-evaluation)
  - [Predicate Pushdown and TiCI Separation](#predicate-pushdown-and-tici-separation)
  - [DataSource Plumbing and Column Pruning](#datasource-plumbing-and-column-pruning)
  - [Costing and Selectivity](#costing-and-selectivity)
  - [CBO Between TiCI and Local MATCH](#cbo-between-tici-and-local-match)
    - [CBO Implementation Plan](#cbo-implementation-plan)
  - [Plan Cache and Prepared Statements](#plan-cache-and-prepared-statements)
  - [Compatibility and Error Handling](#compatibility-and-error-handling)
  - [Implementation Plan](#implementation-plan)
- [Test Design](#test-design)
- [Impacts and Risks](#impacts-and-risks)
- [Investigation and Alternatives](#investigation-and-alternatives)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes a local TiDB execution path for
`MATCH (col1, col2, ...) AGAINST (expr IN BOOLEAN MODE)` when the expression
is used as a boolean filter and no TiCI full-text access path is selected. The
phase-1 scope is intentionally aligned with the current TiCI indexed
`MATCH ... AGAINST` path: boolean-mode no-score filtering only.

The first local implementation is explicitly no-score. It evaluates
match/no-match semantics with the full-text analyzers in
`pkg/expression/fulltext/analyzer.go` and returns a numeric match flag from the
existing `DOUBLE` builtin contract:

- `1.0` means the row matches.
- `0.0` means the row does not match.
- `NULL` is returned for `AGAINST(NULL)`.

The returned value is not a relevance score and must not be documented or
costed as one. Projection, ordering, and threshold comparisons that need a real
relevance value remain unsupported until a separate scoring design exists.

TiCI remains the preferred indexed path. Local execution is a correctness
fallback for direct filter contexts, dirty-write transactions, and test or
small-deployment cases where a TiCI full-text index cannot be used.

## Motivation or Background

TiDB already has most of the pieces needed for local full-text matching:

- `pkg/parser/parser.y` parses `MATCH ... AGAINST` into `ast.MatchAgainst`,
  including natural-language, boolean, and query-expansion modifiers.
- `pkg/expression/fulltext/analyzer.go` implements `STANDARD_V1` and
  `NGRAM_V1` analyzers.
- `pkg/expression/matchagainst/` implements boolean-mode parser IR for the
  TiCI rewrite path.
- `pkg/expression/fts_helper.go` rewrites boolean-mode `MATCH ... AGAINST`
  into `fts_match_word`, `fts_match_prefix`, and `fts_match_phrase` for the
  current TiCI no-score filter path.
- `pkg/expression/builtin_fts.go` defines `FTSMysqlMatchAgainst`, but its
  local `evalReal` currently returns the existing unsupported error.
- `pkg/planner/core/expression_rewriter.go` already distinguishes direct
  boolean predicate contexts from scalar contexts for the current ILIKE
  fallback.

The existing ILIKE fallback is useful as a temporary compatibility path, but it
is not full-text search. It has no analyzer semantics, no token positions, no
phrase semantics, and no NGRAM-specific term behavior. The local implementation
should replace that semantic approximation with analyzer-backed matching for
the supported subset.

MySQL documents the SQL surface and search modes in:

- https://dev.mysql.com/doc/refman/8.4/en/fulltext-search.html
- https://dev.mysql.com/doc/refman/8.4/en/fulltext-boolean.html
- https://dev.mysql.com/doc/refman/8.4/en/fulltext-search-ngram.html

## Goals and Non-Goals

Goals:

- Execute `MATCH ... AGAINST` locally in TiDB for direct boolean filter
  contexts when no TiCI full-text path is selected.
- Return only a match flag: `1.0`, `0.0`, or `NULL` for `AGAINST(NULL)`.
- Use the parser type from a matching regular FULLTEXT index definition.
- Support `STANDARD_V1` and `NGRAM_V1` in the first implementation.
- Support analyzer-backed boolean-mode matching for terms, required terms,
  prohibited terms, prefixes, and phrases.
- Preserve current TiCI behavior whenever a TiCI full-text access path can serve
  the predicate.
- Keep TiCI helper functions (`fts_match_word`, `fts_match_prefix`,
  `fts_match_phrase`) TiCI-only.
- Keep local full-text evaluation inside TiDB and avoid generic coprocessor or
  TiFlash scalar pushdown.

Non-goals for the first implementation:

- Relevance scoring or MySQL/InnoDB ranking compatibility.
- Natural-language mode, either implicit or `IN NATURAL LANGUAGE MODE`.
- `SELECT MATCH(...)` as a meaningful scalar value.
- `ORDER BY MATCH(...)`, score aliases, or threshold comparisons such as
  `MATCH(...) > 0.5`.
- Score modifiers `>`, `<`, and `~` in boolean mode.
- `WITH QUERY EXPANSION`.
- Persistent local inverted indexes in TiKV or TiDB.
- `MULTILINGUAL_V1` before a matching local analyzer exists.
- Phrase proximity syntax such as `"w1 w2" @N`.

## Current Behavior

The current local and TiCI paths are split:

- Parser:
  - `pkg/parser/parser.y` accepts `MATCH (...) AGAINST (...)`.
  - `pkg/parser/ast/expressions.go` stores the expression as
    `ast.MatchAgainst`.
  - `pkg/parser/ast/dml.go` stores the modifier in
    `ast.FulltextSearchModifier`.
- Expression:
  - `pkg/expression/builtin_fts.go` validates that the search argument is a
    constant string or `NULL` and that matched arguments are string columns.
  - `builtinFtsMysqlMatchAgainstSig.evalReal` returns `NULL` for
    `AGAINST(NULL)` and otherwise returns:
    `cannot use 'MATCH ... AGAINST' outside of fulltext index`.
- TiCI path:
  - `pkg/planner/core/operator/logicalop/logical_datasource.go` chooses a TiCI
    index path, rewrites boolean-mode `MATCH ... AGAINST` through
    `expression.RewriteMySQLMatchAgainstRecursively`, and builds
    `tipb.FTSQueryInfo`.
  - The current TiCI path is no-score. When a boolean query has required terms,
    optional terms are not represented as filters.
  - The current indexed `MATCH ... AGAINST` rewrite only supports
    `IN BOOLEAN MODE`; implicit natural-language mode, explicit
    `IN NATURAL LANGUAGE MODE`, and `WITH QUERY EXPANSION` are rejected before
    TiCI helper-function execution.
- ILIKE fallback:
  - `pkg/expression/fts_to_like.go` and
    `pkg/planner/core/fulltext_to_like.go` translate a strict subset of direct
    predicate-context `MATCH ... AGAINST` expressions into ILIKE predicates.
    The existing legacy fallback accepts simple natural-language and
    boolean-mode search strings, but that fallback surface does not define the
    phase-1 local evaluator scope.
  - It rejects phrases, prefix `*`, score modifiers, grouping, query
    expansion, and punctuation that cannot be represented safely by substring
    matching.

This proposal changes the TiDB-side builtin from "reject local execution" to
"evaluate a no-score full-text match flag" only after the planner has attached
local evaluation metadata.

## Detailed Design

### Execution Contract

The local path is row-local and analyzer-backed.

For each evaluated row:

1. Resolve a local full-text plan for the `MATCH ... AGAINST` expression.
2. Analyze each matched column value with the selected full-text analyzer.
3. Parse and normalize the search string according to the modifier and parser
   type.
4. Evaluate the query IR against the row document.
5. Return `1.0` if the row matches, `0.0` if it does not match, or `NULL` for a
   `NULL` search string.

The local path must not claim stable ordering between matching rows. Multiple
matches, term frequency, and phrase frequency can be collected internally for
debugging or future work, but they must not affect the phase-1 SQL result.

### Planner Selection

Local no-score execution is selected only for `IN BOOLEAN MODE` search strings
in direct boolean filter contexts. The first phase should reuse the same
context boundary already used by the ILIKE fallback: every ancestor from
`MATCH ... AGAINST` to the filter root is a boolean wrapper such as `AND`,
`OR`, `NOT`, parentheses, or an internal truth test.

Supported examples:

```sql
SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE);
SELECT * FROM t WHERE MATCH(title, body) AGAINST ('+tidb -mysql' IN BOOLEAN MODE);
SELECT * FROM t WHERE a = 1 AND NOT MATCH(title) AGAINST ('mysql' IN BOOLEAN MODE);
```

Unsupported in phase 1:

```sql
SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb');
SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb' IN NATURAL LANGUAGE MODE);
SELECT MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) FROM t;
SELECT * FROM t ORDER BY MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) DESC;
SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) > 0.5;
SELECT * FROM t WHERE CASE WHEN MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) THEN 1 ELSE 0 END = 1;
```

Planner decision order:

```text
if TiCI full-text path is available and the modifier is IN BOOLEAN MODE:
    use TiCI
else if modifier is IN BOOLEAN MODE and direct boolean filter context and local execution is enabled and metadata is valid:
    keep FTSMysqlMatchAgainst for TiDB-side local eval
else if old ILIKE fallback is enabled for a strict supported subset:
    rewrite to ILIKE
else:
    return the existing unsupported error
```

This order describes the first local fallback implementation. A later CBO mode
should not hard-code "TiCI wins whenever available"; it should build separate
TiCI-indexed and local-residual alternatives and let physical costing choose
between them. The CBO shape is described in
[CBO Between TiCI and Local MATCH](#cbo-between-tici-and-local-match).

The direct-filter decision must be recorded before `MATCH ... AGAINST` becomes
an ordinary `FTSMysqlMatchAgainst` scalar function. The expression rewriter has
the AST context through `inDirectMatchBooleanContext`; `DataSource` only sees
expression trees after predicate pushdown. A later local-binding pass must not
infer local eligibility from `expression.ContainsFullTextSearchFn` alone,
because scalar contexts such as `MATCH(...) > 0`, `CASE WHEN MATCH(...)`, a
projection, or an `ORDER BY` item all contain the same scalar function shape.

Add explicit usage metadata to the builtin signature, or an equivalent side map
that survives expression cloning:

```go
type FTSMatchUsage int

const (
    FTSMatchUsageScalar FTSMatchUsage = iota
    FTSMatchUsageDirectFilter
)
```

The rewriter should mark `FTSMatchUsageDirectFilter` only when the AST context
is a direct boolean filter context. Local metadata binding may then proceed
only for top-level pushed conditions whose contained `FTSMysqlMatchAgainst`
calls are all marked direct-filter eligible. Unmarked calls must keep the
current unsupported behavior unless TiCI can serve them.

The first implementation should bind local execution only after the predicate
has reached a single `DataSource` as a top-level condition that can be returned
as a residual `Selection`. WHERE predicates and single-table ON/HAVING
predicates that predicate pushdown can place on a `DataSource` may use the
local path. FTS predicates that remain in `LogicalJoin.OtherConditions`,
aggregation/HAVING nodes, projections, sorting, window functions, or other
scalar plan nodes remain unsupported until those executor placements are
designed explicitly.

Dirty writes need special handling. Today `DataSource.checkTiCIDirtyWrite`
returns an error because TiCI cannot see uncommitted data. With local no-score
execution enabled:

```text
if a table has dirty writes and the query contains an FTS predicate:
    mark TiCI FTS path unusable
    if the predicate is direct-filter local-eligible:
        keep the local FTSMysqlMatchAgainst filter
    else:
        return the current dirty-write error
```

### Modifier Handling Before Local Binding

The expression rewriter currently has a modifier guard in
`matchAgainstToBuiltin`. That guard exists because the TiFlash scalar pushdown
protocol does not serialize `ast.FulltextSearchModifier`, so modifiers that
TiFlash would execute incorrectly must be blocked before a pushed-down native
plan can be built.

Local execution needs a different boundary. The guard must not conflate:

- "can this `MATCH ... AGAINST` occurrence be emitted as an
  `FTSMysqlMatchAgainst` scalar function so later planner stages can bind local
  metadata?"; and
- "can this scalar function be pushed to TiFlash/TiCI without losing modifier
  semantics?".

Phase-1 local no-score execution should allow the rewriter to emit
`FTSMysqlMatchAgainst` only for direct-filter `IN BOOLEAN MODE` predicates.
Implicit natural-language mode, explicit `IN NATURAL LANGUAGE MODE`, and
`WITH QUERY EXPANSION` remain unsupported before local binding. This keeps the
local target surface aligned with the current TiCI indexed `MATCH ... AGAINST`
rewrite, which only produces `fts_match_word`, `fts_match_prefix`, and
`fts_match_phrase` for boolean-mode queries.

Define local modifier support separately from TiFlash pushdown support:

```go
func modifierSupportedByLocalNoScore(modifier ast.FulltextSearchModifier) bool {
    return modifier.IsBooleanMode() && !modifier.WithQueryExpansion()
}
```

Rewriter behavior with the local feature guard enabled:

```text
if direct-filter context and modifierSupportedByLocalNoScore(modifier):
    emit FTSMysqlMatchAgainst
    set the AST modifier on the builtin signature
    mark direct-filter usage metadata for later local binding
else:
    keep the existing scalar/pushdown guard and unsupported errors
```

This split is still required for phase-1 boolean-mode predicates:

```sql
SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE);
```

Without the split, the query can fail in the expression rewriter before
`DataSource` has a chance to bind `FTSLocalEvalInfo`. The split must not relax
natural-language modifiers or scalar contexts: projection, ordering,
comparisons, CASE, natural-language mode, and query expansion remain
unsupported unless a future design proves a compatible execution contract.

### Alternative Plan and ILIKE Fallback Interaction

The existing ILIKE fallback is driven by the alternative logical-plan rounds,
not by `DataSource` binding:

1. `expression_rewriter.go` emits the native `FTSMysqlMatchAgainst` in round 1.
2. Direct boolean contexts call `PlanBuilder.MarkPredicateMatch`.
3. If `ftsNativeViable` says the native form cannot run, the builder also calls
   `MarkNonViableFTSMatch`.
4. `planner/optimize.go` can then discard round 1 and rebuild a later round
   with `StmtCtx.AlternativeLogicalPlanFTSLikeFallback = true`, causing the
   rewriter to emit ILIKE predicates instead of `FTSMysqlMatchAgainst`.

Local execution is bound later, after predicate pushdown reaches
`DataSource`. If the current non-viable signal is left unchanged, a query that
is valid for local full-text execution can be rewritten to ILIKE before the
local binder ever sees the original `FTSMysqlMatchAgainst`. That would bypass
the analyzer-backed path and reintroduce the substring fallback semantics this
design is meant to replace.

The implementation should split the signals:

- Direct-filter eligibility is expression metadata on `FTSMysqlMatchAgainst`.
  It is required by local binding and must not depend on whether the ILIKE
  alternative round is enabled.
- ILIKE fallback eligibility remains a separate compatibility signal. It is
  used only when local matching is disabled, or when TiCI and local binding both
  fail with an error that the existing strict ILIKE fallback may rescue.
- `MarkNonViableFTSMatch` must not be set solely because TiFlash/TiCI native
  execution is unavailable when the local feature guard is enabled and the
  occurrence is a direct-filter local candidate. The native round must survive
  long enough for `DataSource.analyzeTiCIIndex` or its replacement binding code
  to either select TiCI, bind a local residual predicate, or return a
  fallbackable error.
- `AlternativeLogicalPlanHasPredicateContextMatch` should not automatically
  schedule a competing ILIKE round for a successfully local-bound predicate.
  Local analyzer-backed matching is the preferred semantic fallback. ILIKE
  remains a last-resort compatibility path, not a cost competitor for local
  full-text execution.

One possible control flow is:

```text
expression rewriter:
    build FTSMysqlMatchAgainst
    mark direct-filter usage metadata on the scalar function
    if local MATCH is disabled:
        keep the current MarkPredicateMatch / MarkNonViableFTSMatch behavior
    else:
        do not mark the round non-viable only because TiFlash/TiCI is unavailable

DataSource binding:
    if TiCI index path matches:
        rewrite to TiCI helper functions and build FtsQueryInfo
    else if local binding succeeds:
        attach FTSLocalEvalInfo and return the top-level condition as residual
    else if old ILIKE fallback is enabled and the query is in its strict subset:
        return an FTSLikeFallbackError so optimize.go can rebuild the ILIKE round
    else:
        return the local/native unsupported error
```

This also means the existing `PlanBuilder` booleans are too coarse once local
execution exists. At minimum, the implementation needs distinct state for:

- "this MATCH occurrence is a direct-filter local candidate";
- "the legacy ILIKE fallback should be explored for this statement";
- "the current native/local round is definitely not executable and should be
  discarded".

Those states may live on the scalar function metadata, the plan builder, or
both, but the final rule is that a successfully local-bound predicate must keep
the original `FTSMysqlMatchAgainst` with `FTSLocalEvalInfo`; it must not be
converted to ILIKE by an alternative build round.

### Analyzer Selection

Local execution must use the parser type that would be used by a matching
regular FULLTEXT index.

Rules:

1. Resolve all columns in `MATCH (col1, col2, ...)`.
2. All columns must come from the same table.
3. Find a public regular FULLTEXT index whose column set exactly matches the
   `MATCH` column set.
4. If multiple candidates still remain, choose the first public index in table
   metadata order.
5. Use `IndexInfo.FullTextInfo.ParserType` from the selected index.
6. If no matching regular FULLTEXT index exists, reject local execution in
   phase 1:

   ```text
   MATCH ... AGAINST local execution requires a matching FULLTEXT index
   ```

This is intentionally a set comparison rather than an ordered-list comparison
because the current TiCI indexed path uses `intset.FastIntSet` coverage checks
for regular FULLTEXT indexes and accepts `MATCH(body, title)` against
`FULLTEXT(title, body)`. Local binding should follow that TiCI scope instead of
inventing a stricter column-order rule. After the index is selected, build the
multi-column document from the selected index/fulltext field metadata rather
than from the user-written argument order. This matches TiCI's multi-value
field construction and preserves the phase-1 rule that phrases do not cross
column boundaries.

Hybrid full-text components are not used for multi-column local
`MATCH ... AGAINST` in the first phase. Their current TiCI behavior is
single-column helper-function oriented, and local execution should not infer a
multi-column analyzer contract from hybrid metadata.

Parser type mapping:

| Parser type | Local status | Analyzer function |
| --- | --- | --- |
| `STANDARD_V1` | Supported | `fulltext.AnalyzeStandardV1` |
| `NGRAM_V1` | Supported | `fulltext.AnalyzeNgramV1` |
| `MULTILINGUAL_V1` | Unsupported in phase 1 | unsupported error |
| `INVALID` or unknown | Unsupported | unsupported error |

Add a small dispatcher in `pkg/expression/fulltext`:

```go
type Analyzer interface {
    Analyze(text string) ([]Token, error)
}

func GetAnalyzer(config AnalyzerConfig) (Analyzer, error)
```

### Analyzer Configuration

Parser type alone is not enough to define local full-text behavior. The row
document and query string must be analyzed with the same configuration:

```go
type AnalyzerConfig struct {
    ParserType model.FullTextParserType

    InnodbFtMinTokenSize int
    InnodbFtMaxTokenSize int
    InnodbFtEnableStopword bool
    Stopwords []string

    NgramTokenSize int
}
```

`pkg/expression/fulltext/analyzer.go` currently reads these values from the
session/global context:

- `innodb_ft_min_token_size`
- `innodb_ft_max_token_size`
- `innodb_ft_enable_stopword`
- `ngram_token_size`

That context-reading API can remain as a wrapper for tests and standalone
callers, but the local `MATCH ... AGAINST` path should not bind index-backed
execution to the query-time sysvar values alone. DDL currently captures
full-text parser parameters in `buildTiCIFulltextParserInfo` and sends them to
TiCI at FULLTEXT index creation time, while `model.FullTextIndexInfo` stores
only `ParserType`. If a user changes token-size or stopword sysvars after the
index is created, analyzing local rows with the current sysvars can diverge
from the indexed TiCI behavior.

Preferred implementation:

- Extend `model.FullTextIndexInfo` to persist the parser parameters needed by
  local evaluation, including the stopword list or an equivalent stored
  stopword payload.
- Populate that metadata from the same values used by
  `buildTiCIFulltextParserInfo`.
- Build `AnalyzerConfig` from the selected FULLTEXT index metadata during
  planner binding.
- Use this config for both query normalization and row document analysis.

If persisting analyzer configuration is deferred, local execution must stay
behind an explicit experimental guard and its behavior must be documented as
query-time-sysvar based. It should not claim compatibility with TiCI or MySQL
for indexes created under different full-text sysvar values.

### Local Evaluator Metadata

Planner validation attaches local metadata to `FTSMysqlMatchAgainst`.

```go
type FTSLocalEvalInfo struct {
    TableID int64
    IndexID int64
    ParserType model.FullTextParserType
    AnalyzerConfig fulltext.AnalyzerConfig

    ColumnIDs []int64
    ColumnUniqueIDs []int64
    ColumnOffsets []int

    NoScore bool
}
```

`ColumnIDs` and `IndexID` describe the metadata contract. `ColumnUniqueIDs`
and `ColumnOffsets` describe the expression schema used during evaluation.
`AnalyzerConfig` describes the executable analyzer contract and must be cloned
with the expression.

`FTSLocalEvalInfo` is attached only after the earlier direct-filter usage check
has succeeded. It is not the mechanism that discovers whether scalar use is
safe; it is the proof that planner validation already accepted this occurrence
for local no-score evaluation.

The offsets are only valid relative to the input schema at the operator where
the scalar function is evaluated. Optimizer rules that move a local FTS
expression across schema boundaries must rebind the offsets or keep the
predicate above the boundary. The first implementation should keep local FTS
filters in a TiDB-side `Selection` above the reader rather than pushing them
into coprocessor tasks.

`builtinFtsMysqlMatchAgainstSig.Clone` must clone `FTSLocalEvalInfo`, so plan
alternatives and expression cloning do not drop local validation.

### Document Representation

Build a document from the current row and the matched columns:

```go
type Document struct {
    Columns []ColumnDocument
    TokenSet map[string]struct{}
    TokenFreq map[string]int
}

type ColumnDocument struct {
    ColumnOrdinal int
    Tokens []fulltext.Token
    Positions map[string][]int
}
```

Rules:

- A `NULL` column contributes no tokens.
- An empty string contributes no tokens.
- Token positions are kept per column.
- Term and prefix matches can aggregate across all matched columns.
- Phrase matches are evaluated within a single column.
- Phrase matches must not cross a column boundary in phase 1.
- The matcher uses token positions exactly as emitted by the analyzer. Filters
  such as length filtering may leave position gaps, and phrase matching should
  respect those gaps.

The "no phrase across columns" rule is conservative and easy to explain. A
future enhancement can add a field-boundary gap if TiCI or MySQL compatibility
testing shows that cross-column phrase matches are required.

### Query IR

Introduce an executable no-score query IR under `pkg/expression/fulltext` or
`pkg/expression/matchagainst`:

```go
type Query struct {
    ParserType model.FullTextParserType
    Root QueryNode
}

type MatchResult struct {
    Matched bool
}

type QueryNode interface {
    Match(doc *Document) MatchResult
}

type TermKind int

const (
    TermWord TermKind = iota
    TermPrefix
    TermNgramPhrase
)

type TermNode struct {
    RawText string
    Tokens []fulltext.Token
    Kind TermKind
}

type PhraseNode struct {
    RawText string
    Tokens []fulltext.Token
}

type GroupNode struct {
    Must []QueryNode
    Should []QueryNode
    MustNot []QueryNode
}
```

The existing `pkg/expression/matchagainst.BooleanGroup` should remain the
syntax-level AST for boolean mode. Local execution adds a normalization step
that converts syntax terms into executable nodes after the analyzer and parser
type are known.

This split is important:

- syntax parsing handles MySQL boolean operators and syntax errors;
- normalization handles token size, stopword filtering, prefix special cases,
  and NGRAM behavior;
- matching operates over normalized query tokens and row tokens.

### Boolean Mode

Boolean mode applies with `IN BOOLEAN MODE`.

Supported phase-1 syntax:

| Syntax | Behavior |
| --- | --- |
| `word` | optional term |
| `+word` | required term |
| `-word` | prohibited term |
| `word*` | prefix term |
| `"word1 word2"` | exact phrase |

Unsupported phase-1 syntax:

| Syntax | Reason |
| --- | --- |
| `>word`, `<word`, `~word` | score modifiers are out of scope |
| `"w1 w2" @N` | proximity phrase is out of scope |
| `( ... )` | only after both parser paths support grouping consistently |
| `+(...)`, `-(...)` | same as grouping |

Filtering rules:

- Every `Must` node must match.
- Every `MustNot` node must not match.
- `Should` nodes behave as an OR filter only when the group has no `Must`
  nodes.
- If the group has at least one `Must` node, `Should` nodes do not affect the
  phase-1 result.
- A query with only prohibited terms returns `0.0`, not "all rows except
  prohibited".
- Empty groups return `0.0`.

These rules match the filter-only behavior already used by the TiCI rewrite in
`pkg/expression/fts_helper.go`: optional boolean clauses are filter-relevant
only when there is no required clause.

Current parser coverage must be treated as part of the implementation contract:

| Syntax | `STANDARD_V1` current parser | `NGRAM_V1` current parser | Phase-1 local target |
| --- | --- | --- | --- |
| `word` | accepted | accepted | supported |
| `+word`, `-word` | accepted | accepted | supported |
| `word*` | accepted | accepted | supported |
| leading `*word` | accepted and ignored | accepted and ignored | preserve current parser behavior |
| `"word1 word2"` | accepted | accepted | supported |
| `(word1 word2)` | rejected | rejected | unsupported |
| `>word`, `<word`, `~word` | rejected | rejected | unsupported |
| `"w1 w2" @N` | rejected | rejected | unsupported |

The implementation must not infer support from unused IR fields such as
`BooleanModifierBoost`, `BooleanModifierDeBoost`, `BooleanModifierNegate`, or
`BooleanPhrase.Distance`. Either parser support, normalization, and tests are
added together, or the syntax returns a deterministic unsupported error before
local evaluation.

### Term Normalization

Normalization converts parser terms to executable query nodes.

General rules:

- Optional terms that normalize to zero tokens are dropped.
- Prohibited terms that normalize to zero tokens are dropped.
- Required terms that normalize to zero tokens make the group unsatisfiable.
- Terms that normalize to an explicit no-match node follow the same role-based
  rule: optional and prohibited no-match nodes are ignored, while a required
  no-match node makes the group unsatisfiable.
- A phrase with zero normalized tokens follows the same rule based on its
  boolean role.
- A phrase with one normalized token is evaluated as a term unless the parser
  type requires phrase semantics.
- Unsupported syntax returns an error before evaluation.

`STANDARD_V1` term rules:

- Plain terms follow the current TiCI `fts_match_word` contract.
- Analyze the raw term with the selected `STANDARD_V1` analyzer.
- A plain term that emits exactly one token becomes `TermWord`.
- A plain term that emits zero or multiple tokens becomes a no-match node.
  Do not widen a multi-token term into OR or phrase matching; TiCI's
  `fts_match_word` returns an empty query when the analyzed query text is not
  exactly one token.
- Phrases are analyzed with `AnalyzeStandardV1` and matched as adjacent tokens
  within one column.
- Prefix terms follow the current TiCI `fts_match_prefix` contract:
  - first run tokenizer-only `PreserveUnderscoreTokenize` and require exactly
    one source token;
  - then run a prefix-query analyzer that lowercases the token, keeps the max
    token-size limit, and bypasses the min token-size and stopword filters;
  - if this does not produce exactly one prefix token, normalize the prefix to
    no-match rather than an unsupported SQL error;
  - do not drop the prefix only because it is shorter than
    `innodb_ft_min_token_size` or a stopword.

The prefix-specific rule is necessary because boolean wildcard syntax can
search indexed words by prefix even when the prefix text itself would not be an
indexed full token.

### NGRAM Semantics

`NGRAM_V1` must not be treated as simple word matching.

Rules:

- Boolean term search without `*`:
  - Analyze the term into NGRAM tokens.
  - Convert the term into phrase semantics over those NGRAM tokens.
  - This follows the existing TiCI rewrite, which maps NGRAM boolean terms to
    `fts_match_phrase`.
- Phrase search:
  - Analyze the phrase into NGRAM tokens.
  - Match those tokens as an adjacent phrase.
- Wildcard search:
  - Preserve the current TiCI rewrite shape: wildcard terms become
    `fts_match_prefix`, even for `NGRAM_V1`.
  - Run the TiCI prefix-query contract: tokenizer-only must produce exactly one
    source token, then the NGRAM prefix analyzer must produce exactly one query
    token.
  - If the prefix text emits exactly one NGRAM token, match document tokens by
    prefix. With fixed-size NGRAM tokens, this behaves like an exact token match
    unless the analyzer configuration changes.
  - If the prefix text emits zero or multiple NGRAM tokens, normalize it to
    no-match according to its boolean role. Do not reinterpret longer wildcard
    terms as phrases unless the TiCI indexed rewrite changes first.

Example with `ngram_token_size = 2`:

| Query | Normalized local behavior |
| --- | --- |
| boolean `abc` | phrase `"ab bc"` |
| phrase `"abc"` | phrase `"ab bc"` |
| wildcard `a*` | no-match |
| wildcard `ab*` | token prefix `ab` |
| wildcard `abc*` | no-match |

### Match Algorithms

#### Word Match

`TermWord` matches if any normalized query token text exists in
`Document.TokenSet`.

`TermWord` is single-token by construction. Multi-token syntax is represented
as a `PhraseNode` only for quoted phrases and NGRAM term normalization; a
`STANDARD_V1` plain word that analyzes to multiple tokens is no-match to mirror
TiCI `fts_match_word`.

#### Prefix Match

`TermPrefix` matches if any document token has the query prefix.

The initial implementation can scan `Document.TokenSet` because local
execution is already a row-local fallback. If benchmarks show prefix scanning
is material, add a per-document sorted token list or prefix trie later.

#### Phrase Match

`PhraseNode` matches when all query tokens appear in one `ColumnDocument` with
adjacent positions.

Algorithm:

1. If the phrase has zero tokens, it cannot match.
2. For each column, find every position for the first token.
3. For each candidate start position, verify that token `i` has position
   `start + i` in the same column.
4. Return true on the first full match.

This uses analyzer-emitted positions. If the analyzer leaves gaps after length
filtering or stopword filtering, the phrase must not bridge the gap unless a
future compatibility change explicitly renumbers positions.

#### Group Match

`GroupNode` matching:

```text
if any Must node does not match:
    return false
if any MustNot node matches:
    return false
if Must is non-empty:
    return true
if Should is empty:
    return false
return any Should node matches
```

Nested groups can use the same algorithm once both parser paths support group
syntax consistently.

### Expression Evaluation

Add local matching to `pkg/expression/builtin_fts.go`:

```go
type builtinFtsMysqlMatchAgainstSig struct {
    baseBuiltinFunc
    modifier ast.FulltextSearchModifier
    localEvalInfo *FTSLocalEvalInfo
}

func SetFTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction, info *FTSLocalEvalInfo) error
```

`evalReal` shape:

```go
func (b *builtinFtsMysqlMatchAgainstSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
    search, isNull, err := b.evalSearchArg(ctx, row)
    if err != nil || isNull {
        return 0, true, err
    }
    if b.localEvalInfo == nil {
        return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
    }

    plan, err := b.getOrBuildLocalNoScorePlan(ctx, search)
    if err != nil {
        return 0, false, err
    }
    doc, err := buildDocument(ctx, row, plan.Columns, plan.Analyzer)
    if err != nil {
        return 0, false, err
    }
    if plan.Query.Match(doc).Matched {
        return 1, false, nil
    }
    return 0, false, nil
}
```

The implementation should avoid rebuilding query IR for every row:

- literal search strings can be parsed once per expression instance;
- mutable constants from prepared statements should be parsed once per
  execution value;
- row column analyzer output is evaluated per row;
- vectorized evaluation can be added after row evaluation is correct.

If `localEvalInfo` is nil, the builtin must keep the current unsupported error.
This prevents accidental local execution without planner validation.

`builtinFtsMatchWordSig`, `builtinFtsMatchPrefixSig`, and
`builtinFtsMatchPhraseSig` remain TiCI helper functions in phase 1. They are
lower-level helper functions and do not carry enough metadata to choose a local
analyzer safely.

### Predicate Pushdown and TiCI Separation

Local `FTSMysqlMatchAgainst` must not be pushed to TiKV or TiFlash through the
generic scalar pushdown path.

Reasons:

- Local metadata is not serialized through protobuf.
- `pkg/expression/distsql_builtin.go` does not carry the
  `ast.FulltextSearchModifier` for `FTSMysqlMatchAgainst`.
- TiCI helper functions and local full-text matching have different execution
  contracts.

Planner rules:

- Keep TiCI access-path selection unchanged when a TiCI full-text index can
  serve the predicate.
- For local fallback, keep `FTSMysqlMatchAgainst` as a TiDB-side filter above
  the reader.
- Update `ftsFuncValidation` to distinguish:
  - helper functions, which are still restricted to TiCI access conditions;
  - local-bound `FTSMysqlMatchAgainst`, which is allowed only in direct boolean
    filter contexts.
- Update expression pushdown checks so local-bound `FTSMysqlMatchAgainst` is
  not treated as TiFlash-supported.
- Treat the expression rewriter's direct-filter eligibility as a required input
  for local binding; `DataSource` must not classify scalar `MATCH` uses by
  recursively searching for FTS function names.

The planner must keep TiCI and local semantics separate. TiCI still rewrites
boolean-mode `MATCH ... AGAINST` into helper FTS functions and builds
`FtsQueryInfo`. Local execution should keep the original
`FTSMysqlMatchAgainst` scalar function with local metadata.

### DataSource Plumbing and Column Pruning

Local execution needs a different physical placement from TiCI. The existing
DataSource path first allows `FTSMysqlMatchAgainst` to enter
`DataSource.PushedDownConds` so `analyzeTiCIIndex` can test TiCI access paths.
For a local fallback, the implementation must remove every pushed-down
top-level condition that contains a local-bound `FTSMysqlMatchAgainst` from
`DataSource.PushedDownConds` and append that whole condition back to the
residual predicates returned by `DataSource.PredicatePushDown`. That produces
an ordinary TiDB-side `Selection` above the reader:

```text
predicates -> PushDownExprs
    pushedDown: may include a top-level condition containing MATCH ... AGAINST
    residual: other non-pushable predicates

analyzeTiCIIndex:
    if TiCI path selected:
        rewrite MATCH to TiCI helper functions and build FtsQueryInfo
    else if local fallback selected:
        bind FTSLocalEvalInfo to eligible MATCH calls inside the condition
        remove the containing top-level condition from DataSource.PushedDownConds
        return the containing condition as a residual predicate
```

Do not surgically extract only the inner `MATCH ... AGAINST` from a boolean
wrapper. For example, `MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) OR a > 10`
must remain one residual condition. Returning only the `MATCH` child and leaving
`a > 10` pushed down would change SQL semantics. Top-level CNF siblings that do
not contain local FTS can still remain pushed down.

This requires an API change in the current planner code. Today
`DataSource.PredicatePushDown` owns the local `predicates` return slice, while
`DataSource.analyzeTiCIIndex()` only mutates `ds.PushedDownConds` and returns
an error. The local implementation should either:

- change `analyzeTiCIIndex` to return extra residual predicates, then append
  them to `predicates` in `DataSource.PredicatePushDown`; or
- perform local fallback binding directly in `PredicatePushDown`, where both
  `ds.PushedDownConds` and the residual `predicates` slice are available.

Only mutating `ds.PushedDownConds` is insufficient because it would remove the
local condition from the reader side without returning it to the parent
`Selection`. `ds.AllConds` should still keep the original predicate set for
partition pruning and later bookkeeping.

Keeping the predicate above the reader is also the preferred column-pruning
contract. A residual `LogicalSelection` already calls
`expression.ExtractColumnsFromExpressions(..., ignoreFTSFunc=false)` for its
conditions, so the matched text columns are passed to the child `DataSource` as
ordinary parent-used columns. That makes `ColsRequiringFullLen` include those
columns because local evaluation needs the full column value.

Do not rely on `DataSource.PruneColumns` seeing local FTS only through
`ds.AllConds`. Today it calls
`expression.ExtractColumnsFromExpressions(ds.AllConds, nil, true)`, and the
`ignoreFTSFunc=true` mode skips every function listed in `FTSFuncMap`. That is
correct for TiCI helper execution because TiCI owns the text index lookup, but
it is wrong if a local-bound FTS predicate is accidentally kept only in the
DataSource predicate bookkeeping. Therefore the implementation must either keep
the local-bound top-level condition as a residual `Selection`, or add a
finer-grained classifier that extracts columns from local-bound
`FTSMysqlMatchAgainst` while still ignoring TiCI helper functions.

If the classifier path is needed, it should distinguish:

| Function state | Column extraction in `PruneColumns` |
| --- | --- |
| TiCI helper functions (`fts_match_word`, `fts_match_prefix`, `fts_match_phrase`) | ignore matched columns when the chosen TiCI path owns the predicate |
| unbound `FTSMysqlMatchAgainst` | keep current behavior; it is not executable locally |
| local-bound `FTSMysqlMatchAgainst` with `FTSLocalEvalInfo` | extract matched columns and keep them in the DataSource schema |

`ColsRequiringFullLen` must also include the matched columns for local-bound
FTS predicates. The analyzer needs the full column value; prefix-index truncation
or covering-index-only reads are not a valid input for local full-text
matching.

`ftsFuncValidation` needs the same distinction. The current rule uses
`ContainsFullTextSearchFn` and rejects FTS functions in `LogicalSelection`
without knowing whether a predicate has been bound for local no-score
execution. After local metadata binding:

- TiCI helper functions remain restricted to TiCI access conditions.
- Unbound `FTSMysqlMatchAgainst` remains rejected outside a valid TiCI path.
- Local-bound `FTSMysqlMatchAgainst` is allowed only in the residual
  `LogicalSelection` shape validated by the planner and marked as
  direct-filter eligible by the expression rewriter.

This prevents accidental scalar use while allowing ordinary predicates such as:

```sql
SELECT 1 FROM t WHERE MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE);
SELECT * FROM t WHERE a = 1 AND NOT MATCH(title) AGAINST ('mysql' IN BOOLEAN MODE);
```

### Costing and Selectivity

Local no-score execution does not create a full-text access path. It is a
TiDB-side CPU filter over rows produced by the ordinary table or index access
path.

Costing rules:

- For the initial non-CBO fallback, preserve existing behavior by preferring
  TiCI whenever a TiCI full-text path is available.
- Do not add a local full-text index access path.
- Add CPU cost proportional to the matched column byte length and normalized
  query token count when the optimizer compares plans that keep the local
  filter.
- Keep selectivity conservative. Reuse the existing ILIKE selectivity
  substitution only as a temporary estimate for simple single-column terms, and
  fall back to a default selection factor for phrases, prefixes, NGRAM terms,
  and multi-column MATCH until full-text-specific statistics exist.
- Do not use the no-score match flag as a ranking signal in costing.

CBO implementation should make the estimate boundary explicit:

- TiCI FTS estimates belong to the TiCI access path first. The current code
  refines `AccessPath.CountAfterAccess` in `deriveSearchPathStats`, but that is
  path-local. If a TiCI-only alternative is expected to influence join order,
  the chosen TiCI estimate must also be reflected in the alternative's
  `DataSource.StatsInfo`.
- Local `MATCH` estimates belong to the residual `LogicalSelection`, not the
  child `DataSource`. The child should still be costed as an ordinary table or
  index access path; the parent selection applies local full-text selectivity
  and CPU cost.
- The current generic selection cost treats one scalar function as one unit of
  CPU. Local full-text filters should get an expression-specific cost weight
  because their work scales with matched text bytes and normalized query terms.
- The current `ast.FTSMysqlMatchAgainst` selectivity substitution to ILIKE is a
  temporary bridge. Once local binding exists, selectivity should distinguish
  local-bound `FTSMysqlMatchAgainst` from TiCI helper functions and from
  unbound scalar uses.

### CBO Between TiCI and Local MATCH

The column-pruning split between TiCI FTS and local residual `MATCH ... AGAINST`
is the main CBO difficulty, but it is not impossible to bridge. The problem is
that the current planner stores predicate placement, column requirements, and
access paths on one mutable `DataSource`, while the two execution modes need
opposite contracts:

- TiCI consumes `MATCH ... AGAINST` inside `FtsQueryInfo` and can avoid reading
  the matched text columns back into TiDB when the index covers the query.
- Local execution keeps the original `FTSMysqlMatchAgainst` as a TiDB-side
  residual filter and must read the matched text columns at full length.

The current TiCI path is selected before normal physical CBO sees all
competitors. `DataSource.PredicatePushDown` pushes predicates into
`DataSource.PushedDownConds`, then `analyzeTiCIIndex` can choose one TiCI
FULLTEXT index, rewrite `FTSMysqlMatchAgainst` into TiCI helper functions,
build `tipb.FTSQueryInfo`, and call `keepOnlyTiCIPath`. At that point normal
table and row-index access paths have been removed from `PossibleAccessPaths`.
Later, `deriveSearchPathStats` can update the TiCI path's
`CountAfterAccess`, but the non-TiCI competitors are already gone.

This means a query such as:

```sql
SELECT *
FROM t
WHERE MATCH(title) AGAINST ('+tidb' IN BOOLEAN MODE)
  AND status = 'open';
```

cannot reliably compare "TiCI FTS first" with "use a selective `status` index,
then apply local `MATCH`". Join queries have the same issue when a selective
outer table could drive an index join into a normal row index before applying
local `MATCH`.

Do not solve this by placing both TiCI and local modes inside one shared
`DataSource` with path-local flags only. Too much state is shared today:

- `Schema` and `Columns`;
- `ColsRequiringFullLen`;
- `AllConds` and `PushedDownConds`;
- `StatsInfo`;
- the single-scan checks used by table/index path construction.

If the shared column set includes local-only text columns, the TiCI candidate
can look non-covering or require an unnecessary table lookup. If the shared
column set omits those columns, the local candidate is physically invalid
because it cannot evaluate the analyzer-backed predicate. A path-level solution
would need a broader planner refactor with path-specific required columns,
path-specific residual predicates, and path-specific final projections.

The lower-risk CBO design is to use separate logical alternatives:

1. TiCI FTS indexed alternative:
   - bind the predicate to a TiCI FULLTEXT index;
   - rewrite `FTSMysqlMatchAgainst` into `fts_match_word`,
     `fts_match_prefix`, or `fts_match_phrase`;
   - build `FtsQueryInfo`;
   - allow pruning of text columns that are needed only by TiCI evaluation;
   - cost the chosen TiCI path through `deriveSearchPathStats`.
2. Local residual `MATCH` alternative:
   - disable TiCI consumption for that eligible `MATCH` occurrence;
   - keep the original `FTSMysqlMatchAgainst` with `FTSLocalEvalInfo`;
   - return the whole top-level predicate containing local `MATCH` as a
     residual `LogicalSelection`;
   - retain matched columns in the `DataSource` schema and
     `ColsRequiringFullLen`;
   - let ordinary table indexes, row indexes, and index joins compete normally.
3. Legacy ILIKE fallback:
   - keep it as a compatibility fallback when local matching is disabled or
     local binding is rejected;
   - do not use it as the main local CBO competitor because it does not have
     full-text token, position, phrase, or NGRAM semantics.

TiDB already has a rebuild-and-cost framework for alternative logical plans
behind `tidb_opt_enable_alternative_logical_plans`, and the existing
FTS-to-ILIKE fallback is a useful shape reference. The local-vs-TiCI CBO mode
should use a separate planner signal rather than reusing the ILIKE mode flag:

```text
round 1: native TiCI-capable FTS planning
round 2: local MATCH residual planning
round 3: legacy ILIKE fallback only if local is disabled or unsupported
```

Each round must run pruning and predicate pushdown independently. The TiCI
round may ignore columns used only by `FtsQueryInfo`; the local round must keep
those columns through the residual `LogicalSelection`.

Stats timing matters for joins. Join reorder derives logical statistics before
physical access-path costing, while TiCI search estimates currently land on
the path-local `CountAfterAccess`. That helps physical path costing, but it
does not by itself update `DataSource.StatsInfo` for table order or upper
logical operators. If a TiCI alternative should influence join order, the TiCI
round must make the selected alternative's `DataSource.StatsInfo` row count
consistent with the TiCI FTS estimate instead of stopping at path-local stats.

The local alternative has a different stats boundary. When local `MATCH` is a
real residual `LogicalSelection`, `LogicalSelection.DeriveStats` can apply the
normal selection factor to the child stats. This is approximate, but it gives
join reorder and upper logical operators a visible filter boundary. Keeping
local `MATCH` only inside `DataSource.AllConds` loses both that stats boundary
and the column-pruning protection described above.

Index join is another reason to keep the alternatives separate. TiCI FULLTEXT
indexes are not valid index-join inner probes because they are not ordinary
equality or range probes on the join key. A local residual alternative can
still make this shape valid:

```text
selective outer table
    -> normal row index probe on the inner table join key
    -> fetch matched text columns
    -> TiDB-side local MATCH residual filter
```

Therefore, local-vs-TiCI CBO should compare complete physical plans, including
join order and index-join enumeration. Choosing only at single-table access
path selection time is too early for the motivating join cases.

#### CBO Implementation Plan

The first implementable CBO version should reuse the alternative logical-plan
round driver. It should not store TiCI and local state on one shared
`DataSource` path list.

1. Add a statement-local planning mode for the local residual round.
   - Add a new signal such as
     `StatementContext.AlternativeLogicalPlanFTSLocalResidual`. This is a
     planning mode, not a user-visible semantic guard.
   - Reset it in `ResetAlternativeLogicalPlanSignals`.
   - Restore it around each alternative round in `planner/optimize.go`, the
     same way the current `AlternativeLogicalPlanFTSLikeFallback` setup is
     restored after a round.
   - Keep it separate from `AlternativeLogicalPlanFTSLikeFallback`. The local
     round must preserve `FTSMysqlMatchAgainst` and attach
     `FTSLocalEvalInfo`; the ILIKE round rewrites the AST expression to
     substring predicates.

2. Record a local CBO candidate during expression rewriting.
   - In `pkg/planner/core/expression_rewriter.go`, when
     `inDirectMatchBooleanContext()` is true and
     `tidb_enable_local_match_against` is enabled for an
     `IN BOOLEAN MODE` predicate, record the local guard as a relevant
     optimizer variable. Also record
     `tidb_opt_enable_alternative_logical_plans`, because the local-vs-TiCI
     CBO round is gated by that optimizer switch. Then mark that a local
     residual alternative is worth exploring.
   - The signal can be a new builder method, for example
     `MarkLocalMatchCandidate`, or a narrower variant of the existing
     predicate-MATCH signal. It should only require direct-filter boolean
     eligibility; table/index metadata is resolved later in `DataSource`.
   - Continue marking the legacy ILIKE fallback only when local matching is
     disabled or a local/native planning error is explicitly fallbackable.
     A successfully local-bound predicate must not automatically enable the
     ILIKE cost competitor.

3. Add a local residual alternative round in `planner/optimize.go`.
   - Enable the round only when:
     - `tidb_opt_enable_alternative_logical_plans` is ON;
     - `tidb_enable_local_match_against` is ON;
     - round 1 saw at least one direct-filter local MATCH candidate.
   - Place it before the `fts-like-fallback` round.
   - Its setup sets `AlternativeLogicalPlanFTSLocalResidual = true` and leaves
     `AlternativeLogicalPlanFTSLikeFallback = false`.
   - The normal `bestCost` comparison chooses between the default TiCI-capable
     round and the local-residual round. If the local round fails but the TiCI
     round is valid, keep the TiCI plan. If both native/TiCI and local are
     invalid and the error is fallbackable, the existing ILIKE fallback round
     may still be tried.

4. Teach `DataSource.analyzeTiCIIndex` to honor the local residual mode.
   - Default mode keeps the current behavior: choose a TiCI FULLTEXT path when
     one covers the predicate, rewrite to TiCI helper functions, build
     `FtsQueryInfo`, and call `keepOnlyTiCIPath`.
   - Local residual mode must skip TiCI consumption for eligible
     `FTSMysqlMatchAgainst` predicates, call the local binder, remove the
     containing top-level condition from `DataSource.PushedDownConds`, and
     return that condition as a residual predicate from
     `PredicatePushDown`.
   - Reuse the existing residual shape from `bindLocalFTSConds`: delete TiCI
     access paths for this local alternative and let table paths, row indexes,
     index merge, and index join enumeration proceed normally.
   - Keep the whole top-level condition residual. Do not extract only the inner
     `MATCH` expression from an `OR`, `NOT`, or other boolean wrapper.

5. Make TiCI and local statistics visible at the right layer.
   - For the TiCI round, keep using `deriveSearchPathStats` for
     `path.CountAfterAccess`. When the TiCI path is the only path left after
     `keepOnlyTiCIPath`, propagate that estimated row count back to
     `DataSource.StatsInfo` before upper logical operators and join reorder use
     the alternative's stats.
   - For the local round, leave the child `DataSource` stats based on the
     ordinary access predicates. Apply the local MATCH selectivity in the
     residual `LogicalSelection`.
   - `LogicalSelection.DeriveStats` currently applies the fixed
     `cost.SelectionFactor`. The CBO patch should add a targeted path for
     local-bound `FTSMysqlMatchAgainst` conditions, or a shared helper that can
     call `cardinality.Selectivity` for these residual filters and fall back to
     `cost.SelectionFactor` on error.
   - In `pkg/planner/cardinality/selectivity.go`, split the current
     `ast.FTSMysqlMatchAgainst` handling into:
     - local-bound MATCH: use simple ILIKE substitution only for simple
       single-column `STANDARD_V1` word cases where it is known to be an
       approximation, otherwise use a conservative full-text default;
     - TiCI helper functions: keep them path-owned and do not estimate them as
       residual scalar filters;
     - unbound MATCH: keep the existing conservative fallback or unsupported
       behavior.

6. Add local full-text CPU cost.
   - In cost model v2, extend `filterCostVer2` or add a helper called from
     `getPlanCostVer24PhysicalSelection` that detects local-bound
     `FTSMysqlMatchAgainst`.
   - Estimate per-row work as:

     ```text
     local_match_cost =
         rows *
         (base_match_cost
          + matched_text_bytes_per_row * analyzer_cost_per_byte
          + normalized_query_terms * query_match_cost)
     ```

   - Estimate `matched_text_bytes_per_row` from the child stats and matched
     columns recorded in `FTSLocalEvalInfo` (`ColumnUniqueIDs` / column
     metadata). Use the existing row-size helpers as a model, and fall back to
     a small default when stats are unavailable.
   - For mutable prepared-statement search strings, use a conservative default
     query-term count at plan time. Do not parse and bake parameter-specific
     query terms into a reusable plan.
   - Either mirror a coarse multiplier in cost model v1 or gate local-vs-TiCI
     CBO to cost model v2 until v1 has an equivalent estimate. Do not let v1
     treat a row-local analyzer as a normal one-function predicate while v2
     applies the full CPU cost.

7. Preserve winner state and plan-cache boundaries.
   - The winner's logical-plan build state is restored through
     `saveLogicalPlanBuildCtx` / `restoreLogicalPlanBuildCtx`. Ensure any new
     local-round planning mode is not accidentally restored as a winner-visible
     semantic flag.
   - A local winner must keep the existing plan-cache skip reason until the
     analyzer configuration is persisted in FULLTEXT index metadata.
   - A TiCI winner keeps the current TiCI plan-cache boundary. An ILIKE winner
     keeps the existing mutable-search-string cache guard.

8. Add CBO-focused tests before changing default behavior.
   - Default/TiCI round still wins when the TiCI FTS estimate is cheaper than a
     full scan plus local MATCH.
   - Local residual round wins when a normal selective index plus local MATCH is
     cheaper than the TiCI FTS path.
   - A selective outer table can choose index join into the matched table and
     apply local MATCH after fetching the full text column.
   - Local residual predicates keep matched text columns for
     `SELECT 1 ... WHERE MATCH(...)`, while the TiCI round can still prune text
     columns that are needed only by `FtsQueryInfo`.
   - The ILIKE fallback round is not enabled for a successfully local-bound
     predicate, but still works when local binding is rejected and the strict
     ILIKE subset applies.

### Plan Cache and Prepared Statements

The search string must be constant during one evaluation, but prepared
statement parameters can change between executions.

Rules:

- Literal search strings may build and cache query IR during first evaluation.
- Parameter markers and other mutable constants must not bake query tokens into
  a reusable global plan without checking the current value.
- If query IR is stored in the expression instance, the cache key must include:
  - search datum;
  - parser type;
  - selected index ID;
  - analyzer configuration digest, including token-size settings and stopwords.
- Planning must record the local execution guard, for example
  `tidb_enable_local_match_against`, as a relevant optimizer variable. A plan
  built with local-bound `FTSMysqlMatchAgainst` must not be reused after the
  guard is turned off, and a plan built while the guard is off must not prevent
  a later execution from binding local metadata after the guard is turned on.
- If the temporary query-time-sysvar fallback is used instead of persisted index
  configuration, the cache key must include the sysvar values read by the
  analyzer. That fallback should be removed once `FullTextIndexInfo` persists
  the configuration.
- `builtinFtsMysqlMatchAgainstSig` is already not safe to share across
  sessions. Local execution should keep that property unless all cached state
  is proven session-independent.

Prepared statement correctness example:

```sql
PREPARE s FROM 'SELECT * FROM t WHERE MATCH(title) AGAINST (? IN BOOLEAN MODE)';
SET @q = '+tidb';
EXECUTE s USING @q;
SET @q = '-mysql';
EXECUTE s USING @q;
```

The second execution must not reuse tokens parsed for `+tidb`.

Repeated identical predicates in one query can share parsed query IR later, but
the first implementation should prioritize correctness over common-subexpression
reuse.

### Compatibility and Error Handling

Return behavior:

| Case | Local result |
| --- | --- |
| `AGAINST(NULL)` | `NULL` |
| NULL matched column | ignored as an empty column |
| all matched columns NULL or empty | `0.0` |
| empty search string | `0.0` |
| all query terms filtered by analyzer | `0.0` |
| `STANDARD_V1` plain term or prefix that does not normalize to exactly one token | no-match according to boolean role |
| boolean query with only prohibited terms | `0.0` |
| non-constant search argument | unsupported error |
| non-string search argument | unsupported error |
| non-string matched column | unsupported error |
| no matching FULLTEXT index | unsupported error in phase 1 |
| unsupported parser type | unsupported error |
| unsupported boolean operator | syntax or unsupported error |
| implicit natural-language mode or `IN NATURAL LANGUAGE MODE` | unsupported error in phase 1 |
| scalar score context | unsupported error in phase 1 |
| `WITH QUERY EXPANSION` | unsupported error in phase 1 |

Collation and case sensitivity:

- The current analyzers lowercase tokens.
- Local matching is therefore case-insensitive in phase 1.
- Collation-specific token equality is not implemented in phase 1.

Stopwords:

- `STANDARD_V1` calls `stopwordFilter`.
- `stopwordSetFromSysVar` currently returns an empty set when stopwords are
  enabled because TiDB does not resolve InnoDB stopword table contents on this
  path yet. The index-bound local path should instead use the stopword payload
  from `AnalyzerConfig` when that metadata is available.
- The local matcher should use analyzer output exactly as provided and should
  not add a second stopword layer after analysis.

### Implementation Plan

1. Analyzer dispatcher and document model
   - Add `AnalyzerConfig` and `GetAnalyzer(config)` in
     `pkg/expression/fulltext`.
   - Preserve the current session-context analyzer wrappers for existing tests,
     but make local `MATCH ... AGAINST` use an explicit analyzer config.
   - Persist or otherwise bind the analyzer configuration used when the
     matching FULLTEXT index was created; do not silently use current sysvars
     as the compatibility contract.
   - Add document construction and token-position helpers.
   - Add tests for NULL columns, empty strings, multi-column documents, prefix
     lookup, phrase positions, and NGRAM positions.

2. Query parser and normalization
   - Reuse `pkg/expression/matchagainst.BooleanGroup` for boolean syntax.
   - Normalize terms, prefixes, phrases, and NGRAM boolean terms to executable
     no-score query nodes.
   - Add deterministic unsupported errors for natural-language mode, score
     modifiers, proximity, query expansion, and parser-type gaps.

3. Local evaluator
   - Add local no-score evaluation behind `builtinFtsMysqlMatchAgainstSig`.
   - Attach and clone `FTSLocalEvalInfo`.
   - Cache parsed query IR safely for literal constants and prepared
     statements.
   - Keep helper FTS functions TiCI-only.

4. Planner binding and validation
   - Resolve matching regular FULLTEXT index metadata.
   - Build `FTSLocalEvalInfo` from table/index metadata and analyzer config.
   - Add a session variable guard, for example:

     ```sql
     SET tidb_enable_local_match_against = ON;
     ```

   - Record the guard as a relevant optimizer variable and include it in plan
     cache reuse boundaries for any statement that contains a local-eligible
     `MATCH ... AGAINST`.
   - Preserve the expression rewriter's direct-filter eligibility on
     `FTSMysqlMatchAgainst` across clone paths, and bind local metadata only
     for eligible occurrences.
   - Split local no-score modifier support from TiFlash pushdown modifier
     support. Only direct-filter boolean mode may reach local binding;
     natural-language mode, `WITH QUERY EXPANSION`, and scalar score contexts
     still fail before local execution.
   - Split local direct-filter metadata from the existing ILIKE alternative-plan
     signals. When local execution is enabled, do not discard the native round
     before `DataSource` has a chance to bind a TiCI path or local residual
     predicate.
   - When TiCI is not selected, move top-level conditions containing
     local-bound FTS predicates out of `DataSource.PushedDownConds` and back
     into the residual predicate list so they become TiDB-side `Selection`
     filters.
   - Change the `analyzeTiCIIndex` / `PredicatePushDown` interface as needed
     so local fallback predicates are actually returned as residual filters.
   - Update column extraction/pruning so local-bound FTS predicates retain their
     matched columns and require full-length column values.
   - Update `ftsFuncValidation` so local-bound `FTSMysqlMatchAgainst` is not
     rejected as a helper-function misuse.
   - Keep ILIKE fallback behind a compatibility path until local matching is
     enabled by default.

5. Local-vs-TiCI CBO
   - Implement the local residual alternative round described in
     [CBO Implementation Plan](#cbo-implementation-plan).
   - Keep the default non-CBO behavior unchanged when
     `tidb_opt_enable_alternative_logical_plans` is OFF.
   - Add a planner signal separate from the ILIKE fallback mode so the local
     round preserves `FTSMysqlMatchAgainst` and the ILIKE round remains a
     last-resort compatibility rewrite.
   - Propagate TiCI search estimates into `DataSource.StatsInfo` only inside
     the TiCI alternative where `keepOnlyTiCIPath` has selected the TiCI path.
   - Apply local MATCH selectivity and CPU cost at the residual
     `LogicalSelection` layer.
   - Add CBO tests for single-table index choice, join order, index join,
     column pruning, and ILIKE fallback isolation.

6. Follow-up work
   - Add consistent group parsing for both parser paths.
   - Add `MULTILINGUAL_V1` once its analyzer exists.
   - Design natural-language local matching only after the TiCI indexed path has
     a matching no-score contract for it.
   - Design relevance scoring separately.
   - Add query expansion after scoring and executor interfaces are designed.

## Test Design

Unit tests:

- `pkg/expression/fulltext`
  - analyzer dispatcher;
  - document construction;
  - token positions after analyzer filters;
  - phrase and prefix helpers;
  - NGRAM wildcard prefix normalization, including shorter/longer no-match
    cases.
- `pkg/expression/matchagainst`
  - boolean operator parsing;
  - syntax errors for unsupported operators;
  - current parser compatibility for leading `*`, trailing `*`, and phrases.
- `pkg/expression`
  - `FTSMysqlMatchAgainst` local `evalReal`;
  - `AGAINST(NULL)`;
  - NULL and empty matched columns;
  - boolean required/prohibited/optional behavior;
  - phrase and prefix matching;
  - `STANDARD_V1` single-token behavior for word and prefix terms;
  - prepared statement value changes.

Integration tests:

- `SELECT * FROM t WHERE MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE)`.
- `SELECT * FROM t WHERE MATCH(title) AGAINST ('+tidb -mysql' IN BOOLEAN MODE)`.
- Multi-column `MATCH(title, body)` with a matching regular FULLTEXT index.
- Multi-column `MATCH(body, title)` with only `FULLTEXT(title, body)` remains
  supported because the current TiCI index path matches regular FULLTEXT column
  sets without requiring the same argument order.
- `STANDARD_V1` and `NGRAM_V1` parser-specific behavior.
- `STANDARD_V1` terms that analyze to multiple tokens match TiCI no-match
  behavior instead of phrase matching.
- `STANDARD_V1` prefix terms that produce multiple source tokens or exceed the
  max token-size limit match TiCI no-match behavior.
- `NGRAM_V1` wildcard terms match TiCI prefix-helper behavior: exactly one
  NGRAM query token is required, while shorter or longer wildcard text is
  no-match.
- TiCI available: TiCI path remains selected.
- TiCI unavailable: local path executes when enabled and the old unsupported
  error remains when disabled.
- Boolean-mode direct filter with local execution enabled and alternative
  logical plans disabled: `MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE)`
  reaches local binding instead of failing in `matchAgainstToBuiltin`'s TiFlash
  pushdown guard.
- Plan-cache guard toggle: a cached local-bound plan is not reused after
  `tidb_enable_local_match_against` is turned off, and a plan cached while the
  guard is off does not block local binding after it is turned on.
- Dirty-write transaction: TiCI path is skipped and local evaluation can see
  modified rows when enabled.
- Local predicate stays above reader and is not pushed to TiKV/TiFlash.
- Local predicates under boolean wrappers preserve the whole residual
  condition, for example `MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) OR
  a > 10`.
- Alternative logical plans enabled: a local-eligible `MATCH` predicate remains
  analyzer-backed and is not rewritten to ILIKE before `DataSource` local
  binding. Include a phrase or prefix case that ILIKE cannot represent to catch
  accidental fallback.
- Alternative logical plans enabled with local execution disabled or local
  binding rejected: the old strict ILIKE fallback still works for its supported
  subset and still rejects unsupported syntax deterministically.
- Scalar-context guard: `MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) > 0`,
  `CASE WHEN MATCH(...)`, projection, and `ORDER BY` are not marked
  local-eligible and still return unsupported errors without TiCI support.
- Modifier guard: implicit natural-language mode,
  `IN NATURAL LANGUAGE MODE`, and `WITH QUERY EXPANSION` stay unsupported even
  in direct filter contexts.
- Placement guard: FTS predicates that remain in `LogicalJoin.OtherConditions`
  or aggregation/HAVING nodes are rejected until those local execution
  placements are explicitly supported.
- Column-pruning regression: `SELECT 1 FROM t WHERE MATCH(title) AGAINST
  ('tidb' IN BOOLEAN MODE)` still reads `title` for local evaluation even
  though it is not projected.
- Full-length-column regression: local evaluation does not use truncated
  prefix-index values as analyzer input.
- Analyzer-config regression: local evaluation uses the selected FULLTEXT
  index's parser configuration instead of silently changing behavior when
  full-text sysvars are modified after index creation.
- CBO alternatives: with alternative logical plans enabled, compare a TiCI FTS
  plan against a normal selective index plus local residual `MATCH`.
- CBO join alternative: a selective outer table can drive a normal index join
  into the matched table and apply local residual `MATCH` after fetching the
  text columns.
- Stats timing regression: a TiCI estimate used for a TiCI-only alternative is
  visible to the logical row count when it is expected to affect join order,
  not only to path-local `CountAfterAccess`.

Negative tests:

- `SELECT MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) FROM t`.
- `ORDER BY MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE)`.
- `MATCH(title) AGAINST ('tidb' IN BOOLEAN MODE) > 0.5`.
- Implicit natural-language mode and `IN NATURAL LANGUAGE MODE`.
- `WITH QUERY EXPANSION`.
- `>word`, `<word`, `~word`, and `@N`.
- Missing matching FULLTEXT index.
- Unsupported parser type.

Compatibility tests:

- Compare supported no-score match/no-match behavior with MySQL for:
  - boolean optional terms;
  - `+` and `-`;
  - prefix `*`;
  - quoted phrases;
  - only prohibited terms;
  - empty and analyzer-filtered search strings;
  - NULL behavior.
- Compare local behavior with TiCI no-score behavior for boolean filters.
- Confirm unsupported syntax returns deterministic errors instead of falling
  back to substring matching.

Benchmarks:

- analyzer cost per KB for `STANDARD_V1` and `NGRAM_V1`;
- local matching cost per row for one-column and multi-column MATCH;
- phrase matching overhead;
- prefix scanning overhead;
- prepared statement query-cache hit and miss cost.

## Impacts and Risks

Impacts:

- Users can execute supported `MATCH ... AGAINST` filters locally when TiCI is
  unavailable.
- Tests and small deployments can validate analyzer-backed full-text filters
  without requiring TiCI execution.
- The ILIKE fallback can stop carrying semantic burden for full-text syntax.

Risks:

- Local execution is CPU expensive and can make full table scans slower.
- Returning a numeric match flag from a `DOUBLE` builtin can be confused with
  relevance if planner validation lets it escape into scalar contexts.
- Analyzer sysvar changes can affect results if query IR is cached too broadly.
- If analyzer configuration is not persisted with FULLTEXT index metadata,
  local fallback can diverge from TiCI after token-size or stopword settings
  change.
- NGRAM wildcard and phrase semantics are easy to get subtly wrong.
- Relaxing `ftsFuncValidation` too broadly could allow TiCI helper functions in
  unsupported contexts.

Mitigations:

- Gate local execution with `tidb_enable_local_match_against` initially.
- Bind local metadata only after planner validation.
- Bind and cache local query IR with an explicit analyzer configuration digest.
- Keep local FTS as a TiDB-side filter.
- Prefer TiCI whenever a TiCI index path is selected.
- Keep unsupported syntax explicit.
- Add unit tests for parser-specific normalization before enabling by default.

## Investigation and Alternatives

### Keep TiCI-only execution

This is the current preferred production path, but it leaves TiDB unable to
evaluate `MATCH ... AGAINST` filters locally. It also cannot handle dirty-write
transactions where TiCI cannot see uncommitted rows.

### Expand the ILIKE fallback

This is not recommended. ILIKE cannot represent analyzer tokenization,
positions, phrase matching, stopword filtering, or NGRAM phrase conversion. The
current fallback is intentionally strict because broad substring matching would
return wrong rows for full-text syntax.

### Support scalar score contexts first

This is explicitly out of scope for the current phase. It requires a ranking
contract, optimizer costing, and executor behavior for result reuse in
projection and ordering. The no-score filter path should land first because it
has a smaller semantic surface and aligns with the current TiCI filter path.

### Build a local persistent inverted index

This would improve performance but is much larger than the current goal. It
would require DDL, write path, storage, consistency, recovery, and statistics
work.

### Store TiCI and local CBO state on one DataSource

This is not recommended for the first CBO implementation. TiCI and local
residual execution need different required columns, residual predicates, and
full-length-column requirements. Keeping those differences on shared
`DataSource` fields would make covering checks, pruning, stats, and physical
path construction path-dependent. A single-`DataSource` solution should be
treated as a broader planner refactor with path-specific required columns and
path-specific residual filters.

## Unresolved Questions

- Should local no-score execution eventually be enabled by default when a
  matching FULLTEXT index exists?
- Should a future phase allow an explicit no-index local scan using
  `STANDARD_V1`, or should local execution always require matching FULLTEXT
  index metadata?
- Should `MATCH(...) > 0` and `MATCH(...) != 0` be normalized to direct boolean
  filter contexts before relevance scoring exists?
- Should local execution support `JOIN ON` and HAVING predicates that cannot be
  placed as residual `Selection` filters directly above a `DataSource`?
- What is the cleanest shared API for `STANDARD_V1` prefix-query normalization
  so local evaluation reuses the TiCI-compatible two-stage contract without
  duplicating analyzer internals?
- Should phrase matching ever bridge analyzer position gaps caused by filtered
  tokens?
- Should phrase matching across multiple `MATCH` columns ever be allowed?
- How should local execution expose warnings for large full scans?
- Should local-vs-TiCI CBO be enabled only under
  `tidb_opt_enable_alternative_logical_plans`, or should it eventually become
  the default planning mode after local selectivity and TiCI stats propagation
  are stable?
