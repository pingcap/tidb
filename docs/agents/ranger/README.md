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
The "After" numbers below use the median of `-count=3` reruns on the correctness-fixed code:

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
