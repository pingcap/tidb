# executor: per key lookup for semi join

**Branch:** `innerhash`
**PR:** [#66785](https://github.com/pingcap/tidb/pull/66785)
**Status:** WIP
**Labels:** `ok-to-test`, `release-note-none`, `size/L`

## Summary

This PR adds an early termination optimization for semi joins in the `IndexNestedLoopHashJoin` executor. When executing a semi join (e.g., `WHERE EXISTS (SELECT ...)`), once every outer row has found at least one matching inner row, there is no need to continue reading more inner rows. This can significantly reduce I/O and CPU for queries where the inner side has many duplicate matches per key.

## Problem

Without this optimization, `IndexNestedLoopHashJoin` for semi joins fetches **all** inner rows even though only existence is needed. For tables with high duplication on the join key, this means reading far more data than necessary.

## Approach

Enable incremental (batched) inner fetching for semi joins and check after each batch whether all outer rows have already been matched. If so, stop fetching early.

Two code paths are handled:

- **Unordered path** (`KeepOuterOrder=false`): After each batch of inner rows, `allOuterRowsMatched()` is checked. If all outer rows in the hash table are marked matched, inner fetching stops.
- **Ordered path** (`KeepOuterOrder=true`): Uses `probeAndMarkOuterRows()` to mark matches incrementally across batches, then `emitSemiJoinResultInOrder()` outputs matched rows in their original chunk order.

Both paths handle semi joins with and without additional cross-table conditions.

## Changed Files

| File | Change |
|------|--------|
| `pkg/executor/join/index_lookup_hash_join.go` | +191 lines: core optimization logic |
| `pkg/executor/join/test/indexjoin/index_lookup_join_test.go` | +68 lines: test coverage |
| `pkg/executor/join/test/indexjoin/BUILD.bazel` | shard count update |

## Key Functions Added/Modified

- **`supportIncrementalLookUp()`** — Semi join is not included here; instead, incremental fetch is enabled at runtime in `handleTask()`.
- **`handleTask()`** — New semi join paths for both ordered and unordered execution. Enables `maxFetchSize` for semi joins to allow batched inner reads.
- **`probeAndMarkOuterRows()`** — Probes the hash table for each inner row and marks matching outer rows. Handles joins with other conditions via `TryToMatchOuters`, saving matched inner rows for later emit.
- **`allOuterRowsMatched()`** — Checks whether all hashable outer rows (those in the hash table) have been marked matched.
- **`emitSemiJoinResultInOrder()`** — Emits matched outer rows in original chunk order for the ordered path. Uses saved inner rows for condition evaluation when needed.

## Test Coverage

`TestIndexHashJoinSemiJoinEarlyTermination` covers:

1. Semi join with `ORDER BY + LIMIT` (ordered path)
2. Semi join without `LIMIT` (full result correctness)
3. Semi join with additional cross-table conditions (`t2.c > t1.c`)
4. Both ordered and unordered paths exercised

**Patch coverage:** 79.71% (28 lines uncovered)

## Review Findings

### CodeRabbit

1. Memory tracker not attached to `matchedInners` chunk list
2. `TryToMatchOuters` called once but may need loop for complete matching
3. Test assertions could be stronger (only `require.Greater` for some checks)
4. Missing coverage for unordered early-termination path

### Pantheon AI

1. **P0** — Silent row drops when fanout exceeds `MaxChunkSize`
2. **P1** — Untracked memory allocation for `matchedInners`
3. **P2** — Missing plan verification in tests (should confirm `IndexHashJoin` is actually used)
4. **P2** — Double evaluation of join conditions
5. O(N) scan inefficiency in `allOuterRowsMatched`

## Open Items

- Address memory tracking for `matchedInners` and `matchedInnerPtrs`
- Verify `TryToMatchOuters` loop completeness for large outer row sets
- Add plan verification (`EXPLAIN`) in tests to confirm the optimizer chooses `IndexHashJoin`
- Consider maintaining a running match counter instead of O(N) scan in `allOuterRowsMatched`
- Add test coverage for the unordered semi join path
