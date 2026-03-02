# FMSketch Benchmark Report

## Summary

The PR (`simplify-fmsketch`) replaces `dolthub/swiss.Map[uint64, bool]` + `sync.Pool` with native Go `map[uint64]struct{}` and simple nil assignment. On Go 1.25.6 (which has Swiss-table-backed native maps), the PR is **the fastest configuration overall at ~10.3s vs 14-20s for alternatives**. The pool+Clear pattern that was intended as an optimization is actually a **40-90% performance regression** regardless of map implementation.

Pre-allocating the map to `maxSize` capacity in `NewFMSketch` avoids incremental grow-and-rehash cycles during merge, giving an additional 19% speedup over the initial PR version (12.0s -> 10.3s).

| Configuration | Phase 1 | Phase 2 | Phase 3 | Total | Verdict |
|---|---|---|---|---|---|
| **PR (native map, no pool)** | **7.3s** | **2.5s** | 0.49s | **10.3s** | **Winner** |
| Master (swiss, no destroy) | 9.8s | 3.8s | 0.35s | 13.9s | +35% slower |
| Stash (native map + pool, no destroy) | 12.5s | 4.0s | 0.47s | 16.9s | +64% slower |
| Master + destroy (swiss + pool+Clear) | 16.6s | 2.5s | 0.48s | 19.5s | +89% slower |
| Stash + destroy (native map + pool+clear) | 13.9s | 2.5s | 0.56s | 17.0s | +65% slower |

## Test configuration

```
partitions=8000 columns=1 regions=50 entries=500 maxsize=10000 overlap=0.30 GOGC=100
Go: go1.25.6 darwin/arm64
5 iterations per configuration, medians reported
```

## Detailed results (median of 5 iterations)

### Phase 1: Region -> Partition merge (8000 partitions x 50 regions)

This is the hot path during ANALYZE — each partition's region sketches are deserialized and merged.

| Configuration | Wall | Allocs | Alloc bytes | GC runs | GC pause |
|---|---|---|---|---|---|
| **PR** | **7.3s** | 2.3M | 10.9 GB | 29 | 1.4ms |
| Master | 9.8s | 4.2M | 9.2 GB | 33 | 1.9ms |
| Master + destroy | 16.6s | 829K | 1.7 GB | 1 | 0.1ms |
| Stash | 12.5s | 5.0M | 18.6 GB | 48 | 2.6ms |
| Stash + destroy | 13.9s | 824K | 1.6 GB | 0 | 0ms |

Master+destroy and Stash+destroy show very few GC runs and allocations because the pool recycles objects. But the `Clear()` / `clear(map)` call on each return-to-pool is so expensive that the wall time is **far worse** despite avoiding GC. The pool suppresses GC but replaces it with something slower.

The stash without destroy is slow (12.5s) because `NewFMSketch` hits the pool, gets a pre-allocated map, but never returns it — worst of both worlds.

The PR's pre-allocation to `maxSize` capacity eliminates map growth during merge (2.3M allocs vs 2.6M before, 10.9 GB vs 13.1 GB), saving ~1.7s.

### Phase 2: Save/Load round-trip (8000 sketches)

Simulates persistence to `mysql.stats_fm_sketch`: encode each partition sketch, release it, then decode.

| Configuration | Wall | Allocs | Alloc bytes | GC runs |
|---|---|---|---|---|
| **PR** | **2.5s** | 576K | 7.0 GB | 4 |
| Master | 3.8s | 448K | 7.8 GB | 5 |
| Master + destroy | 2.5s | 304K | 4.7 GB | 3 |
| Stash | 4.0s | 880K | 9.2 GB | 4 |
| Stash + destroy | 2.5s | 296K | 4.7 GB | 2 |

PR matches the destroy configurations here — the encode/decode path doesn't involve the pool, so the difference is purely map performance. Native maps are ~35% faster than swiss for deserialization.

### Phase 3: Partition -> Global merge (1 col x 8000 partitions)

Simulates `blockingMergePartitionStats2GlobalStats`: merge all partition sketches into one global sketch per column.

| Configuration | Wall | Allocs |
|---|---|---|
| **PR** | 0.49s | 0 |
| Master | 0.35s | 4 |
| Master + destroy | 0.48s | 22 |
| Stash | 0.47s | 0 |
| Stash + destroy | 0.56s | 20 |

Master is slightly faster here (0.35s vs 0.49s) because swiss map iteration may be faster for the merge-into-global pattern. But Phase 3 is <5% of total time, so this doesn't matter.

## Key takeaways

1. **Pool+Clear is a net negative.** On both map implementations, enabling destroy (pool+Clear) makes Phase 1 dramatically slower. The cost of clearing a 10K-entry map on every return-to-pool far exceeds any GC savings.

2. **Native maps are faster than swiss on Go 1.25.6.** Even without the pool removal, switching from swiss to native maps (master vs stash, no destroy) saves ~0.8s in Phase 1 — but the pool interaction in the stash adds overhead that negates this.

3. **The PR's simplicity wins.** By removing both swiss maps AND the pool, the PR gets the cleanest allocation pattern and the best total performance. Fewer moving parts = fewer pathological interactions.

4. **Pre-allocating to maxSize pays off.** Allocating `make(map[uint64]struct{}, maxSize)` up front avoids incremental rehashing during merge, cutting Phase 1 from 9.0s to 7.3s (-19%) at the cost of ~600 MB higher peak heap (3.7 GB vs 3.1 GB).

5. **GC is not a problem at this scale.** 29 GC runs with 1.4ms total pause across 7.3s of work is negligible. The pool was solving a problem that didn't exist.

## Raw data

All raw output files are in this directory:
- `master_no_destroy.txt` — swiss map, pool unused
- `master_destroy.txt` — swiss map, pool+Clear active
- `stash_native_map_pool_no_destroy.txt` — native map, pool unused
- `stash_native_map_pool_destroy.txt` — native map, pool+clear active
- `pr_no_destroy.txt` — native map, no pool (with maxSize pre-allocation)
- `pr_destroy.txt` — native map, no pool (destroy = nil, same as no destroy; earlier run with cap-128 pre-allocation)
