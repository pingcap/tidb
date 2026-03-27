# Ranger Notes

## 2026-03-27 Range Builder Performance Notes

Benchmarks:

- `BenchmarkDetachCondAndBuildRangeForIndex`: long `IN` list on the leading columns of a composite index.
- `BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout`: `64 x 64` cartesian fanout on `(a, b, c)`.
- `BenchmarkBuildColumnRangeForLongInList`: long single-column `IN` list.

This round implemented:

1. `appendPoints2IndexRange` now allocates shared backing arrays for `LowVal` / `HighVal` / `Collators` per batch instead of per emitted range.
2. `buildFromIn` now collects one datum per candidate value, sorts and deduplicates that list first, and only then materializes range points.

Observed before/after benchmark deltas on the local mock-store setup.
The "After" numbers below capture the `-count=3` median that closed the previous correctness-fix round:

- `BenchmarkDetachCondAndBuildRangeForIndex`
  - Before: `412784 ns/op`, `1001413 B/op`, `10464 allocs/op`
  - After: `303160 ns/op`, `1058223 B/op`, `7392 allocs/op`
- `BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout`
  - Before: `851692 ns/op`, `4339060 B/op`, `34778 allocs/op`
  - After: `701504 ns/op`, `4402117 B/op`, `22680 allocs/op`
- `BenchmarkBuildColumnRangeForLongInList`
  - Before: `378209 ns/op`, `670626 B/op`, `10260 allocs/op`
  - After: `328208 ns/op`, `744214 B/op`, `10259 allocs/op`

Takeaways for possible follow-up issue(s):

- `appendPoints2IndexRange` was strongly allocation-bound, and batch allocation produced a clear CPU drop with a large `allocs/op` reduction.
- The `buildFromIn` change reduced CPU, but `B/op` did not improve. One plausible remaining cost is the extra temporary datum slice plus later point materialization.
- `convertPoint` / `(*point).Clone` remains a hotspot candidate worth profiling again after the current allocation reduction.
- `validInterval` still pays repeated `codec.EncodeKey` cost; a typed fast path may be worth exploring if future profiles still show it high.
- Cartesian fanout remains expensive even after the current change. `rangeMaxSize` fallback behavior should stay benchmarked because it is the main safety valve against range explosion.

## 2026-03-28 Follow-up Allocation Round

This round implemented:

1. `points2Ranges` now batch-allocates `Range` objects and the single-datum `LowVal` / `HighVal` / `Collators` slices instead of allocating them per emitted range.
2. `appendPoints2IndexRange` now batch-allocates `Range` objects in addition to the backing arrays that were already shared in the previous round.

The local `-count=3` median moved from a fresh rerun of the correctness-fixed code immediately before this allocation round to:

- `BenchmarkDetachCondAndBuildRangeForIndex`
  - Before: `319656 ns/op`, `1058343 B/op`, `7392 allocs/op`
  - After: `296492 ns/op`, `1058215 B/op`, `6369 allocs/op`
- `BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout`
  - Before: `699837 ns/op`, `4402237 B/op`, `22680 allocs/op`
  - After: `649902 ns/op`, `4417926 B/op`, `18141 allocs/op`
- `BenchmarkBuildColumnRangeForLongInList`
  - Before: `345745 ns/op`, `744374 B/op`, `10261 allocs/op`
  - After: `287953 ns/op`, `729710 B/op`, `6166 allocs/op`

These "Before" numbers are a fresh pre-change rerun for the allocation round above, not a restatement of the 2026-03-27 section's recorded "After" values, so small drift between the two sections is expected.

Takeaways for the next round:

- The profile that motivated this change showed `points2Ranges`, `datumsToPoints`, and `(*point).Clone` dominating allocation traffic, so reducing per-range object churn was worth it even before touching the conversion path.
- `allocs/op` dropped materially again on both the index and single-column long-`IN` paths. This suggests range materialization was still a large contributor, but a post-change pprof is still needed to separate it cleanly from datum sorting.
- `B/op` only improved meaningfully on the single-column path. The cartesian-fanout index benchmark still grows slightly in bytes, so the next likely hotspots remain `convertPoint` / `(*point).Clone` and `validInterval`'s repeated key encoding.
