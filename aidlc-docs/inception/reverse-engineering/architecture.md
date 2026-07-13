# Relevant Baseline Architecture

- `LimitExec` knows exact `offset + count` demand and already bounds each child
  chunk request.
- A normal `IndexLookUpJoin` only uses parent required rows for outer joins.
- Its single outer worker doubles batch size up to
  `tidb_index_join_batch_size` and fills channels sized by
  `tidb_index_lookup_join_concurrency`.
- An outer `IndexLookUpExecutor` independently prefetches index ranges and table
  lookup tasks with fixed concurrency and batch settings.
- Closing LIMIT eventually cancels workers, but work already admitted to both
  pipelines can greatly exceed LIMIT demand.

The change therefore needs one demand owner at LIMIT and admission points at
both speculative producer layers.
