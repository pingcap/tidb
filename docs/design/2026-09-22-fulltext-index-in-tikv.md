# Full-text search on a FULLTEXT index built in TiKV

- Author(s): [terry1purcell](https://github.com/terry1purcell)
- Discussion PR: https://github.com/pingcap/tidb/pull/71531
- Tracking Issue: https://github.com/pingcap/tidb/issues/70491

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

On the classic kernel a `FULLTEXT` index is materialised in TiKV as a
positional inverted index: one KV entry per distinct term per row, keyed by
the term and the row handle, whose value carries the term's positions in the
document. `MATCH ... AGAINST` in BOOLEAN MODE is answered by a posting-list
engine in TiDB that evaluates the whole boolean query, positions included,
against those entries and feeds the matching handles to the ordinary
IndexMerge table lookup.

## Motivation or Background

There is no columnar engine on the classic kernel to hold a full-text index,
so until now a `FULLTEXT` index was refused there and a `MATCH ... AGAINST`
filter was evaluated over every row: TiDB analyzed each document at read time
and matched it against the compiled query. That is correct and hopeless for a
large corpus.

An earlier attempt stored tokens in a multi-valued index without positions.
An NGRAM search for `abc` could then only ask the index for rows containing
`ab` and `bc` somewhere; on long text nearly every row contains every common
bigram, so the index proposed almost the whole table and every candidate was
tokenized again. Positions are what make phrase and NGRAM searches answerable
from the index, and are the reason for a dedicated index shape.

## Detailed Design

### What the user sees

```sql
CREATE TABLE articles (id INT PRIMARY KEY, body TEXT,
                       FULLTEXT INDEX idx_body (body) WITH PARSER NGRAM);
SELECT id FROM articles WHERE MATCH(body) AGAINST('分布式数据库' IN BOOLEAN MODE);
```

- On the classic kernel (`kerneltype.IsClassic()`, the classic TiKV storage
  layer) a `FULLTEXT` index is materialised in TiKV. The next-gen kernel is
  untouched in every deploy mode: starter keeps today's TiFlash columnar index
  and the other modes keep refusing `FULLTEXT`. No cloud storage engine or
  TiFlash code is on the path.
- `MATCH ... AGAINST` in BOOLEAN MODE (no query expansion) over an indexed
  column uses the index. The index authorises the `MATCH` on its own;
  `tidb_enable_local_match_against` remains the switch for evaluating a
  `MATCH` on a column that has no index.
- The analyzer settings in force at `CREATE` time are frozen in the index
  metadata. Later `SET GLOBAL innodb_ft_*` changes what a new index is built
  with, never how an existing one is read.
- Out of scope for v1: relevance scores and `ORDER BY MATCH(...)`, natural
  language mode, query expansion, multi-column `FULLTEXT` indexes, global
  indexes on partitioned tables.

### What is shared with the starter deployment

Only the front half. The parser and AST for `FULLTEXT ... WITH PARSER`, the
preprocessor's grammar checks and the parser-type enum are reused. Starter's
`FullTextInfo` metadata, its columnar `ADD INDEX` job that waits on TiFlash,
`FTS_MATCH_WORD` and the TiFlash pushdown planning are all bypassed. On the
classic kernel the index is an ordinary KV `ADD INDEX` job with a marker, the
tokenizer and `MATCH` evaluation are the pure-Go code already on the branch,
and the posting-list read path is new.

### Storage layout

One KV entry per distinct term per row, written through the ordinary index
machinery.

```
key   = t{tableID}_i{indexID} + memcomparable(term bytes) + handle
value = 0x00                      TailLen = 0 (extensible v0 index-value layout)
        0x01                      payload kind: positions, version 1
        uvarint(n) + n × uvarint(delta position)
```

Why this shape:

- The key is exactly a non-unique KV index key over one binary string column,
  so region split, delete-range on `DROP INDEX`, the temp index used during
  `ADD INDEX`, backfill, ingest and admin tooling see nothing new. Within a
  term, entries are ordered by handle, which is what makes the read side a
  streaming merge instead of a hash intersection.
- The value uses the extensible layout so that every existing TiDB decoder
  treats the payload as ignorable trailing bytes. Verified against
  `tablecodec` (`splitIndexValueForIndexValueVersion0`, `DecodeIndexHandle`,
  `IndexKVIsUnique`, `IsUntouchedIndexKValue`): with `TailLen = 0` and a first
  payload byte outside `{125,126,127,128}`, the handle still comes from the
  key, the entry is never classified as untouched or unique, and the temp-index
  merge copies the value byte for byte. Nothing in TiKV ever decodes these
  values, because no coprocessor request is issued against this index
  (see the write path and planner sections).
- Positions are token-stream ordinals from the analyzer, delta-encoded. A term's
  frequency is `n`, available for scoring later without a layout change.
- A NULL or empty document writes no entries.

Sizing: a 1 MB NGRAM document writes about one entry per distinct bigram with a
positions list proportional to occurrences, roughly 1.5 bytes per bigram
occurrence in total. That is well inside the 6 MB entry and 100 MB transaction
limits, and is charged to the statement's memory quota like any other index.

### Metadata and DDL

This is an implementation for the classic TiKV storage layer only, selected
at build time by `kerneltype.IsClassic()`. The TiFlash columnar full-text
index, the next-gen kernel's cloud storage engine, and everything that reads
`FullTextInfo` / `IsColumnarIndex()` are left untouched.

`IndexInfo` keeps `Tp = IndexTypeFulltext` and gains a separate marker:

```go
// TiKVFullText is set on a FULLTEXT index materialised in TiKV as a positional
// inverted index. It is never set together with FullTextInfo.
TiKVFullText *TiKVFullTextIndexInfo `json:"tikv_fulltext,omitempty"`

type TiKVFullTextIndexInfo struct {
    ParserType     FullTextParserType `json:"parser_type"`
    // Analyzer snapshot, frozen at CREATE time.
    MinTokenSize   int  `json:"min_token_size"`
    MaxTokenSize   int  `json:"max_token_size"`
    EnableStopword bool `json:"enable_stopword"`
    NgramTokenSize int  `json:"ngram_token_size,omitempty"`
}
```

To everything that does not check the marker this is an ordinary non-unique KV
index over one column, which is what makes writes, backfill, delete-range,
`DROP COLUMN`, `CREATE TABLE LIKE`, partitioned tables and `ALTER INDEX
VISIBLE` behave without special cases. The sites that must check it:

- analyze and auto-analyze skip it (its keys are terms, not column values);
- `getPossibleAccessPaths` does not offer it as a range path;
- `checkIndexColumn` skips the TEXT/BLOB prefix-length rule;
- the mutation checker and `ADMIN CHECK` route as described under the write path;
- `SHOW CREATE TABLE` and BR's index-repair SQL render
  `FULLTEXT INDEX ... WITH PARSER`.

DDL flow on the classic kernel:

- The preprocessor rewrites `FULLTEXT` into `COLUMNAR ... USING FULLTEXT` only
  on the next-gen kernel, as today. On the classic kernel the statement reaches
  the DDL executor as a FULLTEXT index and takes the ordinary KV `ADD INDEX`
  path (`ActionAddIndex`, ordinary reorg backfill) with the marker set.
- The analyzer snapshot is captured from the session in the executor and
  carried in `model.IndexArg` (new `json:"tikv_fulltext,omitempty"` field),
  because the owner builds the `IndexInfo` without the user's session.
- Refused: partial `WHERE`, `GLOBAL`, prefix length, `DESC`, binary and
  non-string columns, `MULTILINGUAL` parser, more than one column.
- Schema tracker (DM) and `BuildTableInfoFromAST` callers (Lightning, importer)
  go through the same builder with a default analyzer, mirroring what the
  executor produces.

Mixed versions: a TiDB without this change ignores the unknown field and
maintains the index as an ordinary index over the text column, writing
column-value keys under the same index ID. Creation should therefore be gated
on the cluster bootstrap version so the index cannot be created mid-upgrade,
and downgrade needs a release note saying the index must be dropped first.

### Write path

`tables.index` learns one new fan-out, next to the multi-valued one in
`GenIndexKVIter` / `getIndexedValue`:

- The index object builds its analyzer once from the metadata snapshot (the
  partial-index condition is the precedent for an index evaluating something
  from the row itself; no hidden generated column is involved).
- For a row it tokenizes the text datum, groups positions by term, and yields
  tuples `[term, positions]`. The key generator encodes only the term, the
  value generator emits the payload described under the storage layout.
- `Delete` re-tokenizes the old value with the same frozen analyzer and removes
  each `(term, handle)`. `UpdateRecord` already skips indexes whose columns did
  not change, so an update that leaves the text alone does no tokenizing.
- `ADD INDEX` backfill, both txn and ingest, and the temp-index merge call the
  same generator, so existing rows are covered without new code. Ingest drops
  duplicate keys within an engine, which is harmless because terms are distinct
  per row.

Consistency checks:

- The mutation checker decodes every index key and compares it to the row
  datum; for this shape it instead checks membership: the key's term must be
  one the row's document analyzes to.
- `ADMIN CHECK TABLE` and `ADMIN CHECK INDEX` use the record-to-index path
  already used for MV indexes (`admin.CheckRecordAndIndex` → `index.Exist`
  per term) and never issue an index lookup against it. `ADMIN RECOVER` and
  `CLEANUP INDEX` are not adapted.
- Untouched entries are not rewritten on updates of other columns: doing so
  would re-tokenize the document for nothing, and the read path merges the
  memory buffer over the snapshot so the unchanged entries are still found.

### Read path

The reader is the existing IndexMerge executor with a new kind of partial
plan. A `PhysicalIndexScan` carrying a `FullText` descriptor is the partial
plan; instead of a coprocessor request, a partial worker in TiDB runs the
posting-list engine and feeds the handles it yields to the ordinary IndexMerge
table lookup, which handles partitions, batching, memory tracking and limits.

1. **Query plan.** The search string is compiled at execution time with the
   analyzer frozen in the index (`fulltext.CompileBooleanQuery`), and
   `Query.OpenPostings` builds a tree of streams mirroring the query tree:
   terms read posting lists, phrases align positions, groups intersect,
   union and subtract, prefixes collect every term under them.
2. **Posting cursors.** `tikvPostingSource` opens `snapshot.Iter` over
   `[prefix+term, next)` at the statement's read TS, decoding the handle from
   the key and positions from the value. A prefix term scans from the prefix
   and stops at the first term outside it; terms are stored in byte order, so
   the range is contiguous. The snapshot comes from the executor builder like
   point get's, so isolation, stale read and replica read behave normally.
3. **Transactions.** The posting source reads the snapshot only, like every
   coprocessor reader. Rows the transaction changed are merged in by the
   UnionScan the planner places above the reader: its memory-buffer reader
   for the full-text partial is given the table's record range, so every
   changed row is re-checked against the MATCH. This is also what makes it
   safe not to rewrite untouched entries on updates of other columns.
4. **Residual.** The original `MATCH` stays above as a `Selection`, so
   correctness never depends on the engine being exact.

Why TiDB-side scans rather than the coprocessor: TiKV's index scan cannot
return the positions payload, and putting positions in the key would bloat
keys to the size of the positions list. The cost is that a posting scan is
paced by `kv_scan` RPCs from one TiDB instead of running inside TiKV. For a
query whose terms are common the scan is proportional to the number of rows
containing them, which is inherent to an inverted index without further
structure and is what MySQL's InnoDB FTS does too.

### Planner

- `generateFullTextIndexPaths` runs after index-merge path generation. For
  each `FULLTEXT` index built in TiKV and each `MATCH` over its column that is
  a top-level conjunct, locally evaluated, in BOOLEAN MODE without query
  expansion, with a constant search string the plan can bake in and the
  index's own analyzer, it offers an IndexMerge path whose single partial path
  is the full-text scan. Negated `MATCH`, `MATCH` under `OR`, natural language
  mode and prepared parameters keep scanning, for the soundness reasons above.
- **When such a path exists it replaces every other path**, as the columnar
  full-text path does. The alternative is tokenizing every document to
  evaluate the MATCH, which the cost model does not see (a filter costs one
  constant per row), and the index has no statistics to tell a rare term from
  a common one; with the default string-match selectivity of 0.8 the scan
  would otherwise always win. Several MATCH conjuncts still compete on cost.
  `USE INDEX`, `FORCE INDEX` and `IGNORE INDEX`, in both syntaxes, are
  honoured, so a scan can be forced. Invisible indexes follow
  `tidb_opt_use_invisible_indexes`.
- The path cannot keep an order, even one on the indexed column. Plans using
  it are not cached, since the search string is baked in.
- Explain shows `FullTextIndexScan(Build)` as a root operator with
  `index:idx(col) fulltext:"<search>"` under `IndexMerge`, the table lookup as
  the coprocessor probe, and the residual `match_against` Selection above.
- Row estimate: the `MATCH`'s own selectivity (the ILIKE proxy on the
  selectivity term). Posting-length statistics are a follow-up.

## Test Design

| Area | Test |
| --- | --- |
| Metadata and DDL shapes, refusals, cluster gate | `TestFullTextIndexBuiltInTiKV`, `TestFullTextIndexBuiltInTiKVRefusals` (`pkg/ddl`) |
| Entries under DML, backfill, partitions, admin check both directions, recover | `TestFullTextIndexBuiltInTiKVEntries` (`pkg/ddl`) |
| Value layout inert to existing decoders | `TestTiKVFullTextIndexValueIsInertToOtherDecoders` (`pkg/tablecodec`) |
| Generator and mutation-checker membership | `TestFullTextIndexKVGeneration`, `TestFullTextIndexMutationCheck` (`pkg/table/tables`) |
| Engine equals per-document matcher over random corpora and queries | `TestOpenPostingsAgreesWithScan` and siblings (`pkg/expression/fulltext`) |
| Index plan results equal scan results: boolean forms, NGRAM, partitions, transactions | `TestFullTextIndexMatchAgainst*` (`pkg/executor/test/indexmergereadtest`) |
| Planning rules, hints, invisible index, ordering, plan cache | `TestFullTextIndexPathPlanning` (`pkg/planner/core`) |
| Schema tracker mirrors the executor | `TestFullTextIndexMirrorsExecutor` (`pkg/ddl/schematracker`) |
| Cluster version detection | `TestDetectAndUpdateJobVersion` (`pkg/ddl`) |
| Plan shapes and results | `tests/integrationtest/t/planner/core/fulltext_index_tikv.test` |

## Impacts & Risks

- A `FULLTEXT` index on the classic kernel now occupies KV space and costs
  writes, where before the statement was refused.
- A TiDB without this change reads the index as an ordinary index over the
  text column. Creation is refused until every node in the cluster is at
  least the first release carrying the feature, and a downgrade must drop the
  index first.
- The index path replaces every other path when it applies; `IGNORE INDEX`
  restores the scan.
- Plans using the index are never cached, since the search string is baked
  in. A prepared statement with a parameter search string scans.

## Investigation & Alternatives

- **Multi-valued index over tokens**: no positions, so NGRAM and phrase
  searches over long text degenerate into a scan of the table. Superseded.
- **Positions in the key**, which would let the coprocessor return them:
  keys the size of the positions list, and an intersection that needs hash
  sets instead of a merge.
- **Posting lists as blobs per term**, as InnoDB does: every insert touching
  a common term rewrites the same blob, serialising writers.

## Unresolved Questions

- Relevance scoring and `ORDER BY MATCH(...) LIMIT n`: the term frequency is
  in the value, but ranking and early termination are not designed.
- Multi-column `FULLTEXT` indexes: the entry layout covers one column.
- Posting-length statistics for cardinality estimation; today the path uses
  the `MATCH` selectivity proxy and is preferred outright.
- `ADMIN CLEANUP INDEX`, which reads indexes through a coprocessor scan.
