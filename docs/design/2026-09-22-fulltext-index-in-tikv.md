# Full-text search on a positional inverted FULLTEXT index (classic kernel)

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

The entries live in TiKV, but no storage-layer code changes. The index is
ordinary non-unique KV index entries, written through the ordinary index
machinery and read back with plain snapshot range scans from TiDB. Nothing in
TiKV, client-go, kvproto, tipb or the coprocessor changes, and no coprocessor
request is ever issued against the index. The whole implementation is TiDB
code.

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
- Key columns ahead of the tokenized column confine a search to one value of
  them, the multi-tenant shape:

  ```sql
  CREATE TABLE docs (id INT PRIMARY KEY, tenant_id BIGINT, body TEXT,
                     FULLTEXT INDEX idx_body (tenant_id, body));
  SELECT id FROM docs WHERE tenant_id = 42
    AND MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
  ```

  The last column of a `FULLTEXT` index is the one tokenized; every column
  before it is an ordinary key column, encoded ahead of the term in each
  entry as one whole value, whatever its type: a `VARCHAR` tenant holds its
  collation sort key, exactly as in an ordinary index, and is never analyzed. The index answers a search only when the query pins every key
  column to one value (an equality, or `IS NULL`), and then serves those
  equalities itself. This differs from MySQL, where every column of a
  multi-column `FULLTEXT` index is text searched by `MATCH(a, b)`; that
  shape is out of scope here.
- Out of scope for v1: relevance scores and `ORDER BY MATCH(...)`, natural
  language mode, query expansion, `MATCH` over several columns, global
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
key   = t{tableID}_i{indexID} + memcomparable(key column values...) + memcomparable(term bytes) + handle
value = 0x00                      TailLen = 0 (extensible v0 index-value layout)
        0x01                      payload kind: positions, version 1
        uvarint(n) + n × uvarint(delta position)
```

Why this shape:

- The key is exactly a non-unique KV index key over the key columns followed
  by one binary string column, so region split, delete-range on `DROP
  INDEX`, the temp index used during `ADD INDEX`, backfill, ingest and admin
  tooling see nothing new. The key columns are encoded as an ordinary index
  encodes them (a sort key under a collation), so an equality on them is
  answered by the key alone. Within one set of key values and a term,
  entries are ordered by handle, which is what makes the read side a
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
index whose last column is tokenized, which is what makes writes, backfill, delete-range,
`DROP COLUMN`, `CREATE TABLE LIKE`, partitioned tables and `ALTER INDEX
VISIBLE` behave without special cases. The sites that must check it:

- analyze and auto-analyze skip it (its keys are terms, not column values);
- `getPossibleAccessPaths` does not offer it as a range path;
- `checkIndexColumn` and the `MODIFY COLUMN` key-length re-check skip the
  TEXT/BLOB prefix-length rule for the tokenized column; key columns follow
  the ordinary rules;
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
- Refused: partial `WHERE`, `GLOBAL`, prefix length, `DESC`, a binary or
  non-string tokenized column, an expression key part, `MULTILINGUAL` parser.
  Every `FULLTEXT` index over one column must be built with the same parser
  and analyzer settings, so a `MATCH` compiles its search string the same
  way whichever index serves it.
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
  tuples `[key values..., term, positions]`. The key generator encodes the
  key values and the term, the value generator emits the payload described
  under the storage layout.
- `Delete` re-tokenizes the old value with the same frozen analyzer and removes
  each `(term, handle)`. `UpdateRecord` already skips indexes whose columns did
  not change, so an update that leaves the text alone does no tokenizing.
- `ADD INDEX` backfill, both txn and ingest, and the temp-index merge call the
  same generator, so existing rows are covered without new code. Ingest drops
  duplicate keys within an engine, which is harmless because terms are distinct
  per row.

Consistency checks:

- The mutation checker decodes every index key and compares it to the row
  datum; for this shape it compares the key columns in their encoded form
  and checks the term by membership: it must be one the row's document
  analyzes to.
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
   `[prefix+keyvalues+term, next)` at the statement's read TS, decoding the
   handle from the key and positions from the value; the key-column values
   the plan pinned are encoded once and prefix every term. A prefix term
   scans from the prefix and stops at the first term outside it; terms are
   stored in byte order under the key values, so the range is contiguous. The snapshot comes from the executor builder like
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
- For an index with key columns, ranger is run over the conditions with the
  key columns; the path exists only when the result is a single point range
  covering every key column. The point's access conditions are served by the
  index and leave the table filters; `IN` lists, ranges and `OR`s over a key
  column keep the scan.
- Every entry ends with the row's clustered handle, as in any non-unique
  index, so a term's postings are laid out in handle order and conditions on
  a leading prefix of the handle columns select a contiguous slice of each
  term's postings. The columns and the cases in which the key carries them
  are those `HandleColsToAppend` gives an ordinary index (#69745): a
  clustered primary key that shares no column with the index, or a signed
  integer handle. Ranger is run over the remaining conditions with those
  columns, and its ranges become the trailing dimensions of the path's
  ranges, after the key-column point; the term sits between them in the key
  and is implied. The executor reads each exact term's postings within each
  range in turn, which keeps them in handle order since the ranges are
  sorted and disjoint. For a search of exact terms (every standard term and
  phrase, and every NGRAM fragment at least a gram long) the conditions are
  served by the index and leave the table filters; a prefix search reads
  every term with the prefix, which a handle range cannot narrow, so its
  conditions stay as table filters too. This lets `FULLTEXT(body)` on a
  table with `PRIMARY KEY (tenant_id, id) CLUSTERED` serve both global and
  tenant-scoped exact searches without storing the tenant twice, while
  `FULLTEXT(tenant_id, body)` remains the choice for prefix searches within
  a tenant, for search dimensions outside the primary key, and for
  tenant-first locality.
- **The path competes on cost with every other path.** Reading the index
  costs the posting entries the search reads, not the rows it matches:
  every posting list the search opens is read in full. For each term (each
  NGRAM gram, each phrase word, each prefix) the entries read are the rows
  containing the term within the rows the key-column point selects, and,
  for an exact term, the handle ranges too. The rows containing a term are
  estimated with the term's `ILIKE '%term%'` form against the column's
  statistics, the same estimate #65626 uses for a `MATCH`; when the
  statistics cannot evaluate it (no statistics, or a non-binary collation)
  the string-match default selectivity is used. Each term and handle range
  is also charged a seek, since each opens a scan of its own. The posting
  scans run one after another in one TiDB worker, so unlike a coprocessor
  scan they are not divided by the scan concurrency. Every other path
  evaluates the `MATCH` in TiDB on the rows it returns, and the Selection
  doing so is charged for analyzing each document, in proportion to the
  column's average size (about 100ns a byte measured for NGRAM, twice a
  simple function per byte). A poorly filtering search therefore loses to a
  scan, and a selective condition on another index wins over the search.
  `USE INDEX` and `FORCE INDEX` naming the index force it, in both syntaxes;
  `IGNORE INDEX` forbids it. Invisible indexes follow
  `tidb_opt_use_invisible_indexes`. Under `tidb_opt_prefer_range_scan`
  without statistics the path is kept like an MV index path: it never
  scans the table.
- The path cannot keep an order, even one on the indexed column. Plans using
  it are not cached, since the search string is baked in.
- Explain shows `FullTextIndexScan(Build)` as a root operator with
  `index:idx(col) fulltext:"<search>"`, followed by `range:` for the pinned
  key columns and the handle ranges, `[7 1,7 1]` for key column 7 and handle
  1, under `IndexMerge`, with the table lookup as the coprocessor probe.
- Row estimate: the `MATCH`'s own selectivity. A STANDARD query with a
  single term is estimated through its ILIKE form; any other query, NGRAM
  included, is evaluated directly against the column's TopN and histogram
  bounds, which gives the same estimate as the equivalent `LIKE` for a
  binary collation and the string-match default otherwise. Posting-length
  statistics are a follow-up.

## Test Design

| Area | Test |
| --- | --- |
| Metadata and DDL shapes, key columns, refusals, analyzer agreement, cluster gate | `TestFullTextIndexBuiltInTiKV`, `TestFullTextIndexBuiltInTiKVRefusals` (`pkg/ddl`) |
| Entries under DML, backfill, partitions, admin check both directions, recover | `TestFullTextIndexBuiltInTiKVEntries` (`pkg/ddl`) |
| Value layout inert to existing decoders | `TestTiKVFullTextIndexValueIsInertToOtherDecoders` (`pkg/tablecodec`) |
| Generator and mutation-checker membership | `TestFullTextIndexKVGeneration`, `TestFullTextIndexMutationCheck` (`pkg/table/tables`) |
| Engine equals per-document matcher over random corpora and queries | `TestOpenPostingsAgreesWithScan` and siblings (`pkg/expression/fulltext`) |
| Index plan results equal scan results: boolean forms, NGRAM, partitions, transactions, key columns, clustered-handle ranges | `TestFullTextIndexMatchAgainst*` (`pkg/executor/test/indexmergereadtest`) |
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
- The index path is chosen on cost. Its cost rests on per-term estimates
  from the column's statistics, which are rough for short high-NDV text and
  fall back to a default under non-binary collations; `USE INDEX` and
  `IGNORE INDEX` override the choice either way.
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
- `MATCH` over several columns: the entry layout tokenizes one column.
- Several values of a key column in one search (`tenant_id IN (...)`), which
  needs a merge of one posting scan per value.
- Posting-length statistics for cardinality and cost estimation; today the
  per-term cost and the `MATCH` row estimate come from the column's
  statistics through the term's `ILIKE` form.
- `ADMIN CLEANUP INDEX`, which reads indexes through a coprocessor scan.
