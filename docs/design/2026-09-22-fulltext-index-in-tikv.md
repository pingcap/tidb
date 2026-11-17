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
  with, never how an existing one is read. A `MATCH` that TiDB evaluates
  itself (the residual above an index read, or a `MATCH` on a path that does
  not use the index) analyzes with the same frozen settings. The index is
  found by tracing the column to the table that produces it and matching the
  column ID, so an alias, derived table or CTE never borrows another table's
  analyzer. A `MATCH` on a column without such an index analyzes with the
  session's `innodb_ft_*` variables, and its plan is not cached, since the
  plan cache key does not hold them.
- The column list means what it means in MySQL: the columns that are
  tokenized and searched. One column is supported; a list of several, which
  MySQL searches together as one document, is refused rather than read as
  anything else (see "Several tokenized columns" below).
- Key columns confine a search to one value of each, the multi-tenant shape.
  They are named apart from the column list, by `KEY_COLUMNS`:

  ```sql
  CREATE TABLE docs (id INT PRIMARY KEY, tenant_id BIGINT, body TEXT,
                     FULLTEXT INDEX idx_body (body) KEY_COLUMNS (tenant_id));
  SELECT id FROM docs WHERE tenant_id = 42
    AND MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
  ```

  `SHOW CREATE TABLE` prints the clause inside a TiDB comment,
  `FULLTEXT INDEX idx_body(body) WITH PARSER STANDARD /*T![fts_key]
  KEY_COLUMNS(tenant_id) */`, so MySQL, and a TiDB too old to know the
  clause, read the definition as a plain `FULLTEXT(body)`. A key column is
  never analyzed: it is encoded ahead of the term in each entry as one
  whole value, whatever its type, and a `VARCHAR` tenant holds its
  collation sort key, exactly as in an ordinary index. The index answers a
  search only when the query pins every key column to one value (an
  equality, or `IS NULL`), and then serves those equalities itself. An
  application that ports MySQL's multi-tenant idiom, putting the tenant
  column inside the `MATCH` and its value in the search string, rewrites it
  to the equality the index serves:

  ```sql
  -- MySQL idiom: the tenant value is searched as text.
  SELECT id FROM docs
   WHERE MATCH(tenant_id, body) AGAINST('+distributed +"t-42"' IN BOOLEAN MODE);
  -- Here: the tenant is a key column, pinned by an equality.
  SELECT id FROM docs
   WHERE tenant_id = 't-42' AND MATCH(body) AGAINST('+distributed' IN BOOLEAN MODE);
  ```

  The index never serves the first form, since it tokenizes one column: it
  fails unless `tidb_enable_local_match_against` is on, and then every row
  is analyzed. It gives the same rows as the second only when the tenant
  value appears in no document text.
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
        0x02                      payload kind: document length and positions
        uvarint(dl)               document length in tokens, the same in every entry of the row
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
  frequency is `n`.
- The document length `dl` is the number of tokens the document analyzes to.
  With the term frequency it is what a relevance score such as BM25 needs from
  each posting, so scoring can be added without a layout change and without
  reading or analyzing the documents a search finds. It costs 1-3 bytes per
  entry. It cannot go stale: the write path deletes and rewrites all of a
  row's entries whenever its document changes. Kind `0x01`, positions without
  the length, was a pre-release layout; its values are refused rather than
  misread.
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
    // Version of the analyzer; metadata written without one reads as 1.
    Version        int  `json:"version,omitempty"`
}
```

The analyzer is versioned because the terms an index holds are only as
stable as the code that produced them. A new index records the current
version. An index whose version this binary does not know, or whose settings
fail validation, does not stop the schema from loading. Instead the index is
unusable: the planner never reads it (`IndexInfo.QueryableTiKVFullTextColumn`
returns nil), and every write or check of it fails rather than adding or
removing entries that another release would analyze differently. A later
analyzer change bumps the version instead of silently orphaning entries.

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
  `FULLTEXT INDEX ... WITH PARSER`, with the tokenized column in the column
  list and the key columns in `KEY_COLUMNS`.

DDL flow on the classic kernel:

- The preprocessor rewrites `FULLTEXT` into `COLUMNAR ... USING FULLTEXT` only
  on the next-gen kernel, as today. On the classic kernel the statement reaches
  the DDL executor as a FULLTEXT index and takes the ordinary KV `ADD INDEX`
  path (`ActionAddIndex`, ordinary reorg backfill) with the marker set.
- `KEY_COLUMNS` is turned into key parts ahead of the tokenized column where
  every caller enters the DDL layer (`NormalizeTiKVFullTextIndex`, called
  for `CREATE TABLE`, `CREATE INDEX` / `ALTER TABLE ... ADD`, and the DM
  schema tracker), so the stored `IndexInfo` keeps the encoding order, key
  columns then the tokenized column, and DDL job arguments carry only that
  form. The clause is refused on any other index.
- The analyzer snapshot is captured from the session in the executor and
  carried in `model.IndexArg` (new `json:"tikv_fulltext,omitempty"` field),
  because the owner builds the `IndexInfo` without the user's session.
- Refused: more than one column in the column list, partial `WHERE`,
  `GLOBAL`, prefix length, `DESC`, a binary or non-string tokenized column,
  an expression key part, a column named twice, `MULTILINGUAL` parser,
  and a local temporary table, whose rows never reach TiKV, where the index
  is read (error 1796, as MySQL refuses one on an InnoDB temporary table).
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
  per term) and never issue an index lookup against it. A column the row
  does not store, because it was added by an instant `ADD COLUMN` after the
  row was written, is taken as its original default. `ADMIN RECOVER` and
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
   Intersections leapfrog: in an AND, a phrase or a `-term`, the stream with
   the fewest postings sets the pace and the others seek to the handles it
   proposes, so `+rare +common` reads about as many entries of the common
   list as the rare one has rows, not the whole common list.
2. **Posting cursors.** `tikvPostingSource` opens `snapshot.Iter` over
   `[prefix+keyvalues+term, next)` at the statement's read TS, decoding the
   handle from the key and positions from the value; the key-column values
   the plan pinned are encoded once and prefix every term. A prefix term
   scans from the prefix and stops at the first term outside it; terms are
   stored in byte order under the key values, so the range is contiguous. The snapshot comes from the executor builder like
   point get's, so isolation, stale read and replica read behave normally.
   A seek reads forward only within the batch of `PostingScanBatch` (256)
   entries the client-go scanner has already fetched. Once that batch is used
   up the cursor reopens the scan at `(term, handle)`, so a seek costs one
   scan request and no batch is fetched only to be discarded. Seeks also
   work within handle ranges, skipping the ranges below the target. Prefix
   cursors don't seek, because their entries are grouped by term.
3. **Interruption, memory and limits.** The cursor checks the SQL killer
   every 256 entries and before each new scan, so `KILL`,
   `MAX_EXECUTION_TIME`, the server memory-limit kill and runaway rules act
   while the index is read. A scan also stops within a batch when its
   reader is closed or its context cancelled. A prefix search charges the
   IndexMerge memory tracker for the handles it collects, so
   `tidb_mem_quota_query` can stop it. A union reader stops at a `LIMIT`
   pushed into the IndexMerge.
4. **Accounting.** client-go sends snapshot scans on a background context,
   so the posting snapshot carries the accounting the coprocessor path gets
   for free:
   - an RPC interceptor (`fullTextReadAccount`) adds each scan's RU to the
     statement's RU details and reports it to the runaway checker;
   - the snapshot is tagged for Top SQL and the KV exec counters;
   - `EXPLAIN ANALYZE` shows
     `fulltext:{posting_entries, posting_scans, scan_rpc:{num, time}}` for
     the partial worker.
5. **Transactions.** The posting source reads the snapshot only, like every
   coprocessor reader. Rows the transaction changed are merged in by the
   UnionScan the planner places above the reader: its memory-buffer reader
   for the full-text partial is given the table's record range, so every
   changed row is re-checked against the MATCH. This is also what makes it
   safe not to rewrite untouched entries on updates of other columns.
   - Cost: each statement in a transaction that changed the table analyzes
     every row the transaction changed in it (in each pruned partition),
     whatever the search. A statement outside such a transaction pays
     nothing.
   - This is stricter than MySQL: InnoDB applies FULLTEXT changes at commit,
     so its searches see committed data only, even in the writing
     transaction. Here a search sees the transaction's own changes, as every
     other predicate does.
   - Alternatives considered (see Investigation & Alternatives): not using
     the index in such a transaction, or refusing the search there.
6. **Residual.** The original `MATCH` stays above as a `Selection`, so
   correctness never depends on the engine being exact. Any other `MATCH`
   that the data source took on but no storage engine can evaluate, such as
   a second `MATCH` on the table or one under an IndexMerge over ordinary
   indexes, is kept as a root condition of the IndexMerge too.

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
  expansion, with a constant search string and the index's own analyzer, it
  offers an IndexMerge path whose single partial path is the full-text scan.
  A `?` search string counts as constant: the path is built with the
  current execution's value. Negated `MATCH`, `MATCH` under `OR` and natural
  language mode keep scanning, for the soundness reasons above. The path is
  offered only when `tidb_isolation_read_engines` includes TiKV.
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
  `FULLTEXT(body) KEY_COLUMNS(tenant_id)` remains the choice for prefix searches within
  a tenant, for search dimensions outside the primary key, and for
  tenant-first locality.
- **The path competes on cost with every other path.** Reading the index
  costs the posting entries the search reads, not the rows it matches.
  `fulltext.Query.EstimatePostingCost` follows how the engine executes the
  search:
  - The driving list of an AND or a phrase, the one with the fewest
    postings, is read in full.
  - Every other required list, and every `-term` list, is charged per seek
    to the driver's rows: a batch of `PostingScanBatch` entries and a scan
    request for each seek, capped at reading the list in full.
  - OR branches and prefixes are read in full.
  - Each term and handle range is charged a scan request, since each opens
    a scan of its own.
- **How many postings a term has.** A posting list's length is the number
  of rows containing the term, within the rows the key-column point selects
  and, for an exact term, within the handle ranges.
  - A FULLTEXT index has no statistics, and `LONGTEXT`/`MEDIUMTEXT` columns
    skip ANALYZE by default. So the planner counts the postings of each
    exact term in the statement's snapshot (`fulltext_probe.go`), since
    they are one contiguous key range under the key-column point. Handle
    ranges confine a probed count to their share of the rows, by the
    selectivity of their conditions. A first count stops at 64 entries per
    term, within one `kv_scan` batch. When every term of a search reaches
    that, the term with the most postings is counted again up to 1024
    entries, for at most 4 terms per statement. At most 32 terms are probed
    per statement. A partitioned table probes at most 4 partitions, spread
    over its partitions, and scales the count to all of them. Counts are
    kept in the statement context, so each term is probed once per
    statement, and a failed probe never fails planning.
  - Nothing is probed when another access path (not an IndexMerge, not
    TiFlash) already reads at most 64 rows: no posting scan could beat it,
    and the rows the `MATCH` then filters are too few for its estimate to
    matter.
  - A term under the cap is counted exactly. For a term at the cap, the
    estimate from the column's statistics is kept, raised to at least the
    count read.
  - When a `MATCH` could be served both by an index with key columns pinned
    to a point and by an index on the text column alone, only the keyed
    index is offered, unless a hint names the other: it reads the postings
    under the point, which are never more.
  - The statistics estimate uses the term's `ILIKE '%term%'` form, the same
    estimate #65626 uses for a `MATCH`. When the statistics cannot evaluate
    it (no statistics, or a non-binary collation), the string-match default
    selectivity is used. Prefixes are never probed, since their postings
    span every term under them; they always use the statistics estimate.
- **Execution costs.** The posting scans run one after another in one TiDB worker, so unlike a coprocessor
  scan they are not divided by the scan concurrency. The handles the search
  yields are looked up one by one, anywhere in the table, so the path is
  also charged an IndexLookUp's double-read cost for them (a request per
  task and CPU per row, divided by the lookup concurrency). Every other path
  evaluates the `MATCH` in TiDB on the rows it returns, and the Selection
  doing so is charged for analyzing each document, in proportion to the
  column's average size: 0.125 times the CPU factor per byte, calibrated so
  that a root `MATCH` over a tenant weighs against the TiKV scan feeding it
  as it did when measured on 10M rows (about 4.6 times the scan's time). A
  poorly filtering search therefore loses to a scan, and a selective
  condition on another index wins over the search. The rows the table
  lookup sends back are sized as an IndexLookUp's table side sizes them
  (`cardinality.GetAvgRowSize`, where a column without statistics counts a
  small fixed width), not by the declared type width of every column. On a
  wide table whose `TEXT` columns lack statistics the type width is about
  30 times larger, and made the IndexMerge lose to an ordered IndexLookUp
  that reads the same rows. Statistics-based
  string-match estimates, which planning requests once per access path and
  task, are evaluated once per statement and reused.
- **Hints.** `USE INDEX` and `FORCE INDEX` naming the index force it, in both syntaxes;
  `IGNORE INDEX` forbids it. Invisible indexes follow
  `tidb_opt_use_invisible_indexes`. Under `tidb_opt_prefer_range_scan`
  without statistics the path is kept like an MV index path: it never
  scans the table.
- The path cannot keep an order, even one on the indexed column. An
  IndexMerge applies advisory sort items for `ORDER BY ... LIMIT` only when
  every partial path can report match results for them.
- **Beside a `LIMIT` or `TopN`.** When a `LIMIT` or `TopN` prefers plans
  that push it into a coprocessor task, a coprocessor task normally shuts
  out every root task from the comparison. The IndexMerge runs only as a
  root task, and a coprocessor task whose `MATCH` stays a root condition
  cannot take the `LIMIT` either. So while every coprocessor task leaves
  conditions for the root, an IndexMerge root task still competes with
  them on cost; once one coprocessor task can take the `LIMIT`, the
  existing rule applies unchanged.
- **When the ordered plan wins.** For `... AND MATCH(...) ORDER BY k LIMIT
  n` the alternative to the search is an ordered IndexLookUp on `k` with
  the `MATCH` as a root `Selection` above it. With a selective search that
  plan reads many rows for each one the `Selection` keeps, and the
  IndexLookUp's unbounded read-ahead can hold far more rows than the
  `LIMIT` needs; on a wide table that exceeded the default memory quota.
  The adaptive `LIMIT` scan (`tidb_enable_adaptive_limit_scan`, #70252)
  originally rejected a root `Selection` in this shape; #73183 extends it
  to learn the `Selection`'s pass ratio and bound the read-ahead by it.
- **Plan cache.** A plan whose search string, key-column point and handle
  ranges are all literals is cached. The `FullText` descriptor is shared by
  clones of the scan, and range rebuild leaves a FULLTEXT scan's ranges
  alone, since they hold the encoded key-column point and handle ranges.
  A plan is not cached when its search string, or a condition behind its
  key-column point or handle ranges, is a parameter or another mutable
  constant: the scan bakes them in, so a cached plan would search a later
  execution with an earlier one's values.
- Explain shows `FullTextIndexScan(Build)` as a root operator with
  `index:idx(col) fulltext:"<search>"`, followed by `range:` for the pinned
  key columns and the handle ranges, `[7 1,7 1]` for key column 7 and handle
  1, under `IndexMerge`, with the table lookup as the coprocessor probe.
- **Row estimate.** For a FULLTEXT index without key columns, the `MATCH`'s
  selectivity comes from the posting counts above
  (`estimateFullTextMatchSelectivities`). It is recorded before the data
  source's statistics are derived, and used by every path built over it
  (`cardinality.SetFullTextMatchSelectivity`). The terms are combined as
  follows:
  - Required terms combine with the exponential backoff the planner uses
    for correlated conditions, rather than independence: the rarest share
    counts in full, and the next three count with weights 1/2, 1/4 and 1/8.
  - An NGRAM word is bounded by its rarest gram, since every row containing
    the word holds each of its grams.
  - OR branches combine as independent, and `-term` scales by the share of
    rows without the term.
  - A STANDARD phrase of several words keeps the statistics estimate below,
    since postings say nothing about adjacency.

  For an index with key columns, whose postings for a term are one range
  only under each key value, the postings under the key-column point are
  counted when the other conditions pin one, and the selectivity is the
  `MATCH`'s share of the rows the statistics give that point.

  Otherwise (an index with key columns not pinned to a point, or no
  FULLTEXT index on the column), the estimate comes from the statistics. A STANDARD query with a single term is
  estimated through its ILIKE form. Any other query, NGRAM included, is
  evaluated directly against the column's TopN and histogram bounds, which
  gives the same estimate as the equivalent `LIKE` for a binary collation
  and the string-match default otherwise.

## Test Design

| Area | Test |
| --- | --- |
| Metadata and DDL shapes, key columns, refusals (temporary tables included), analyzer agreement, cluster gate | `TestFullTextIndexBuiltInTiKV`, `TestFullTextIndexBuiltInTiKVRefusals` (`pkg/ddl`) |
| Analyzer versions; an unusable index fails writes instead of the schema load | `TestTiKVFullTextIndexVersion` (`pkg/meta/model`), `TestFullTextIndexUnusable` (`pkg/table/tables`) |
| Entries under DML, backfill, partitions, admin check both directions, recover | `TestFullTextIndexBuiltInTiKVEntries` (`pkg/ddl`) |
| Admin check of rows written before an instant `ADD COLUMN` | `TestFullTextIndexAdminCheckAddedColumn` (`pkg/ddl`) |
| Value layout inert to existing decoders | `TestTiKVFullTextIndexValueIsInertToOtherDecoders` (`pkg/tablecodec`) |
| Generator and mutation-checker membership | `TestFullTextIndexKVGeneration`, `TestFullTextIndexMutationCheck` (`pkg/table/tables`) |
| Engine equals per-document matcher over random corpora and queries, with and without seekable cursors | `TestOpenPostingsAgreesWithScan` and siblings (`pkg/expression/fulltext`) |
| Seeks bound the entries read; prefix memory charged and released | `TestOpenPostingsSeeksPastCommonTerms`, `TestOpenPostingsChargesPrefixMemory` (`pkg/expression/fulltext`) |
| Posting cost and row estimates | `TestEstimatePostingCost`, `TestEstimatePostingCostBoundedByFullReads`, `TestEstimatePostingRows` (`pkg/expression/fulltext`) |
| TiKV cursor: seek within a batch or reopen, handle ranges, kill/runaway/close interrupts, prefix memory quota, RU and runaway accounting, one scan request per batch | `TestTiKVPostingSeek`, `TestTiKVPostingInterrupted`, `TestTiKVPostingPrefixMemory`, `TestFullTextReadAccount` (`pkg/executor`) |
| Index plan results equal scan results: boolean forms, NGRAM, partitions, transactions, key columns, clustered-handle ranges, seeks, several `MATCH`es, `LIMIT` | `TestFullTextIndexMatchAgainst*` (`pkg/executor/test/indexmergereadtest`) |
| Planning rules, hints, invisible index, ordering, isolation read engines, plan cache with literal and parameter search strings and ranges | `TestFullTextIndexPathPlanning` (`pkg/planner/core`) |
| Posting counts choose between index and scan without column statistics | `TestFullTextIndexPostingsProbedWithoutStatistics` (`pkg/planner/core`) |
| Table-side row size on a wide table without column statistics | `TestFullTextIndexMergeRowSizeOnWideTable` (`pkg/planner/core`) |
| A local `MATCH` takes its analyzer from its base column's index, or the session without one | `TestLocalMatchAgainstAnalyzerFollowsBaseColumn`, `TestLocalMatchAgainstFollowsSessionAnalyzer` (`pkg/planner/core`) |
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
- The index path is chosen on cost. For exact terms the cost and the row
  estimate rest on posting counts, which are exact for a term with up to
  64 postings, or 1024 when it is counted further. A common term, a
  prefix, a STANDARD phrase of several words, and a `MATCH` on an index
  whose key columns are not pinned to a point still use estimates from the
  column's statistics. Those are rough for short high-NDV text and
  fall back to a default under non-binary collations or without statistics.
  `USE INDEX` and `IGNORE INDEX` override the choice either way.
- Planning reads TiKV, unless another path already reads at most 64 rows:
  up to 32 bounded range scans per statement, one per
  term not yet counted (a partitioned table probes up to 4 partitions for
  each). The cost is small next to the read it decides on, but it is paid
  each time the statement is planned.
- A plan with a literal search string and literal ranges is cached. A
  prepared statement with a parameter search string, or parameters behind
  the key-column point or handle ranges, uses the index but is planned on
  every execution.

## Investigation & Alternatives

- **Multi-valued index over tokens**: no positions, so NGRAM and phrase
  searches over long text degenerate into a scan of the table. Superseded.
- **Positions in the key**, which would let the coprocessor return them:
  keys the size of the positions list, and an intersection that needs hash
  sets instead of a merge.
- **Posting lists as blobs per term**, as InnoDB does: every insert touching
  a common term rewrites the same blob, serialising writers.
- **IndexMerge over one partial path per term, without the excluded (`-`)
  operator.** Not preferred: it limits customers who want the full
  three-valued logic of boolean mode, required (`+`), optional and excluded
  (`-`), which MySQL applications already write and expect the index to
  serve.
  - Required terms map to an IndexMerge intersection and optional terms, in
    a group without required ones, to a union. The excluded operator is the
    only one needing a set difference, which IndexMerge does not have. It
    never yields rows on its own (a search of excluded terms only matches
    nothing), it only removes rows from the positive set.
  - Without it in the index, a `-` search either reads the positive rows and
    re-evaluates the MATCH on each document, fetching every row the excluded
    terms would have removed and tokenizing it again, which is the cost the
    path exists to avoid; or it does not use the index and scans the table.
  - The posting engine applies `-` cheaply: an excluded list is read in
    handle order and stepped alongside the result as it streams, costing the
    size of that list (counted by `PostingReads`), never adding rows.
  - Independently of `-`, per-term IndexMerge cannot serve phrases, which
    need positions; NGRAM searches, where every word is a phrase of grams;
    or nested groups such as `+(a b) +(c d)`. Its intersection also reads
    every list in full, where the engine reads the smallest list and seeks
    in the others, and a search of up to 32 terms would plan as many
    partial paths.
  - A per-term IndexMerge remains a possible later optimisation for the
    simplest searches: exact single words, `+` and optional only, one
    level, STANDARD parser. It would reuse IndexMerge's executor, cost and
    EXPLAIN, at the price of two plan shapes for one feature.
- **Not using the index in a transaction that changed the table**, to avoid
  merging the transaction's changes through UnionScan. Not preferred.
  - Not offering the path there falls back to a scan that analyzes every
    row of the table: one earlier write in the transaction turns an indexed
    search into a full scan, a performance cliff that depends on the
    transaction's history rather than the query.
  - Refusing the search there instead breaks applications that write and
    then search in one transaction, which works on MySQL.
  - Searching committed data only, as InnoDB does, would make MATCH the one
    predicate in a statement that does not see the transaction's changes,
    and still needs UnionScan to drop the rows the transaction deleted.
  - The merge as designed reuses the ordinary UnionScan with one change, the
    record range it is given, and is tested against the scan for inserts,
    deletes, updates in and out of a match, updates of other columns, and
    rows moving between partitions.
- **Several tokenized columns**, `FULLTEXT(title, body)` searched by
  `MATCH(title, body)`. Designed, not built; refused until it is.
  - Meaning, as in MySQL: every listed column is tokenized, and `MATCH` names
    exactly the index's columns. The columns combine into one document: a
    row's tokens are those of all its columns, and the boolean query is
    evaluated once over them, so `+mysql +guide` matches a row with `mysql`
    in `title` and `guide` in `body`. Per term, the postings are the union
    of the term's occurrences in every column. Evaluating the query per
    column and combining the results instead would need every column to
    match the whole query (AND) or miss rows like the one above (OR), and
    is expressible in SQL anyway, with one `MATCH` per column.
  - Positions continue across columns with a gap, so a phrase never spans
    the end of one column and the start of the next (InnoDB's behaviour to
    be confirmed before release). A NULL column contributes no tokens. The
    document length is the total over the columns. Changing any of them
    rewrites all of the row's entries, as today.
  - Storage stays one posting list per term, under the same key. Putting a
    column ID in the key would let a subset of the columns be searched,
    which MySQL does not allow, at the price of merging one list per column
    for every term read.
- **Key columns by position**, the last column tokenized and the ones before
  it key columns, as earlier drafts had it. Rejected: it gives
  `FULLTEXT(title, body)` a meaning other than MySQL's, silently turning a
  ported index into one keyed by `title`, and leaves no way to name several
  tokenized columns. **Key columns by type** (string columns tokenized, the
  rest key columns) was rejected too: a string tenant identifier would be
  tokenized.
- **Document length for scoring kept elsewhere than in every entry.**
  - One extra posting list per index under a reserved term, holding one
    entry per row with its length, read in handle order alongside the term
    lists: one entry per row instead of a few bytes per entry, but an extra
    list read for every scored search, a reserved term the analyzer must
    never produce, and one more write per row.
  - Recomputing the length from the row at scoring time: analyzes every
    document a search finds, the cost the index exists to avoid. Rejected.
  - Not storing it until scoring is designed: adding it later would need a
    second value layout and an index rebuild before existing indexes could
    score. Rejected; the length is stored from the first release.

## Unresolved Questions

- Relevance scoring and `ORDER BY MATCH(...) LIMIT n`: the term frequency and
  the document length are in the value, which is what BM25 needs per posting,
  but ranking and early termination are not designed. The table-wide inputs
  are open: the row count `N` and average length `avgdl` would come from
  statistics, and a term's document frequency `df` is exact for the lists a
  search reads (optional terms beside a required one are not read today) and
  otherwise estimated. Skipping low-scoring rows early needs a per-term
  upper bound on the score, a per-term statistic rather than a per-entry one.
  Scores will not equal InnoDB's, which uses a TF-IDF variant.
- `MATCH` over several columns, designed but not built: see "Several
  tokenized columns" under Investigation & Alternatives.
- Several values of a key column in one search (`tenant_id IN (...)`), which
  needs a merge of one posting scan per value.
- Posting-length statistics for terms the planning probe cannot count
  exactly: common terms, prefixes, and the terms of an index with key
  columns across the table. These still fall back to the column's
  statistics through the term's `ILIKE` form, and a STANDARD phrase without
  column statistics still chooses a scan.
- Plan-cache reuse for a parameter search string, which would need the
  string re-bound at execution and the cost to stop depending on the value.
- `ADMIN CLEANUP INDEX`, which reads indexes through a coprocessor scan.
