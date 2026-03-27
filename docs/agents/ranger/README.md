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

## 2026-03-28 Cartesian Fanout Allocation Round

This round implemented:

1. `AppendRanges2PointRanges` now batch-allocates `Range` objects and the appended `LowVal` / `HighVal` / `Collators` backing arrays instead of letting `appendRange2PointRange` allocate three slices per emitted range.
2. `ranger_test` now has an append-safety regression for the point-plus-tail fanout path, mirroring the earlier coverage we added for `points2Ranges` and `appendPoints2IndexRange`.

The local `-count=3` median moved from the previous range-materialization round to:

- `BenchmarkDetachCondAndBuildRangeForIndex`
  - Before: `296492 ns/op`, `1058215 B/op`, `6369 allocs/op`
  - After: `306992 ns/op`, `1058243 B/op`, `6369 allocs/op`
- `BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout`
  - Before: `649902 ns/op`, `4417926 B/op`, `18141 allocs/op`
  - After: `386039 ns/op`, `4351272 B/op`, `1752 allocs/op`
- `BenchmarkBuildColumnRangeForLongInList`
  - Before: `287953 ns/op`, `729710 B/op`, `6166 allocs/op`
  - After: `289309 ns/op`, `729698 B/op`, `6166 allocs/op`

Takeaways for the next round:

- The cartesian-fanout path still had one more obvious object-allocation hotspot after the previous rounds, and it was worth fixing: CPU dropped by about 41% and `allocs/op` dropped by about 90% on the fanout benchmark.
- The two non-fanout benchmarks stayed effectively flat, so this change looks narrowly targeted at the intended path rather than a broad behavioral shift.
- A post-change fanout pprof still attributes most `alloc_space` to `AppendRanges2PointRanges` and `appendPoints2IndexRange`, but now that object counts collapsed, that remaining cost is mostly the unavoidable datum/collator backing storage rather than tiny per-range headers.
- With the fanout object churn removed, the README's older follow-up items now matter more on the single-column path: `sortAndDedupDatums`, `datumsToPoints`, `(*point).Clone`, and `validInterval` remain the next places worth investigating.

## 2026-03-28 Point Conversion Follow-up Round

This round implemented:

1. `convertPoint` now returns the original point when `ConvertTo` does not actually change the datum, avoiding the `(*point).Clone` allocation on the common long-`IN` path.
2. `validInterval` now has an equality fast path for scalar datum kinds where `Datum.Equals` is strong enough to prove both endpoints encode to the same boundary, so it can skip repeated `codec.EncodeKey` work for point intervals.
3. `datumsToPoints` now batch-allocates the emitted `point` structs instead of allocating one heap object per endpoint.

The local `-count=3` median moved from the previous cartesian-fanout round to:

- `BenchmarkDetachCondAndBuildRangeForIndex`
  - Before: `306992 ns/op`, `1058243 B/op`, `6369 allocs/op`
  - After: `231204 ns/op`, `859931 B/op`, `199 allocs/op`
- `BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout`
  - Before: `386039 ns/op`, `4351272 B/op`, `1752 allocs/op`
  - After: `374999 ns/op`, `4315189 B/op`, `590 allocs/op`
- `BenchmarkBuildColumnRangeForLongInList`
  - Before: `289309 ns/op`, `729698 B/op`, `6166 allocs/op`
  - After: `206423 ns/op`, `532829 B/op`, `20 allocs/op`

Takeaways for the next round:

- This was the largest reduction so far in both `allocs/op` and `B/op` on the single-column long-`IN` workload. The `(*point).Clone` hotspot is gone from the post-change single-column profile, and ranger-specific alloc object churn is now close to zero there.
- The fanout benchmark only moved slightly in CPU and bytes, but it still benefited from the shared point-object allocation because each point range no longer carries per-endpoint heap objects.
- Post-change single-column pprof shows the remaining CPU is now dominated by datum sorting and comparison inside `sortAndDedupDatums`, while `validInterval` is down to a minor cost. The large remaining `alloc_space` entries are `points2Ranges` and `datumsToPoints`, which now mostly represent backing storage bytes rather than millions of tiny objects.
- The next optimization candidates are therefore narrower and more semantic-risky than the earlier allocation rounds: datum sorting in `sortAndDedupDatums`, or reducing backing-array bytes in `points2Ranges` / `datumsToPoints`.
