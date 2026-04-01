# Improving NDV Estimation in TiDB

## Background

### The Paper

[Sampling-based Estimation of the Number of Distinct Values in Distributed Environment](https://arxiv.org/abs/2206.05476) (Li et al., KDD 2022) proposes a sketch-based method to estimate the frequency-of-frequency vector in a distributed streaming model, enabling classical sampling-based NDV estimators (Chao-Lee, GEE, Smoothed Jackknife, etc.) to run with sub-linear communication cost across distributed nodes.

### Patent Constraint

A patent was filed in China in 2022 covering the paper's technique. The specific algorithm — **sketch-based frequency-of-frequency estimation in a distributed streaming model** — must be avoided in any implementation.

### What's Safely in the Public Domain

| Technique | Origin | Status |
|-----------|--------|--------|
| FM Sketch | Flajolet & Martin, 1985 | Public domain |
| Count-Min Sketch | Cormode & Muthukrishnan, 2005 | Public domain |
| HyperLogLog | Flajolet et al., 2007 | Public domain |
| GEE / Good-Turing estimator | Good, 1953; Charikar et al., 2000 | Public domain |
| Chao-Lee estimator | Chao & Lee, 1992 | Public domain |
| Reservoir sampling | Vitter, 1985 | Public domain |
| Local f-vector computation from samples | Standard statistics | Public domain |

## TiDB's Current Approach

TiDB uses two parallel paths for NDV estimation during `ANALYZE`:

### Path 1: FM Sketch (scanning-based)

Every row is hashed and inserted into an FM Sketch. The sketch is mergeable across TiKV regions and partitions.

- Implementation: `pkg/statistics/fmsketch.go`
- NDV formula: `(mask + 1) * len(hashset)` — estimates jump by powers of 2
- Max sketch size: 10,000 entries
- Single hash function, no stochastic averaging
- Serialized via protobuf (`tipb.FMSketch`) for TiKV ↔ TiDB transfer
- Merged for partitioned tables in `pkg/statistics/handle/globalstats/`

### Path 2: GEE Estimator (sampling-based)

A reservoir sample is collected, and the GEE estimator uses `f₁` (count of values appearing exactly once) to scale up NDV.

- Implementation: `pkg/statistics/estimate.go`
- Formula: `NDV = sqrt(rowCount / sampleSize) * f₁ + (sampleNDV - f₁)`
- Only uses `f₁`, not the full frequency-of-frequency vector

### Key Files

| File | Role |
|------|------|
| `pkg/statistics/fmsketch.go` | FM Sketch data structure and merge logic |
| `pkg/statistics/estimate.go` | GEE estimator (NDV from samples) |
| `pkg/statistics/cmsketch.go` | Count-Min Sketch + TopN |
| `pkg/statistics/sample.go` | Sample collection with FM Sketch integration |
| `pkg/executor/analyze_col.go` | Column ANALYZE execution |
| `pkg/executor/analyze_col_sampling.go` | Sampling-based stats building |
| `pkg/statistics/handle/globalstats/` | Partition stats merging (FM Sketch merge lives here) |
| `pkg/statistics/handle/storage/` | Stats persistence and loading |

## Proposed Approach: Upgrade FM Sketch to HyperLogLog

### Why This Approach

HyperLogLog is the natural evolution of FM Sketch. It addresses the key accuracy limitations while:

- **Avoiding the patent entirely** — HLL is a well-established public-domain algorithm (2007), and this stays on the scanning-based path rather than the patented sketch-based frequency-of-frequency streaming approach
- **Minimizing the diff** — the `FMSketch` interface (`InsertValue`, `MergeFMSketch`, `NDV()`, proto serialization) can be preserved; only the internals change
- **Reusing existing plumbing** — TiKV push-down, protobuf transport, partition merging, and storage all work with the same interface

### What Changes

**Current FM Sketch internals:**
- `hashset map[uint64]struct{}` — stores up to 10,000 unique hash values
- `mask uint64` — tracks power-of-2 level
- NDV = `(mask + 1) * len(hashset)` — coarse, jumps by 2x at level transitions

**HLL internals (replacement):**
- Fixed array of registers (e.g., 2^14 = 16,384 registers = ~12KB)
- Each register stores the max number of leading zeroes seen for its bucket
- NDV estimated via harmonic mean across all registers
- Standard error ~1.04 / sqrt(m) ≈ 0.8% with 16K registers

**Merge operation:**
- Current: set union with mask reconciliation
- HLL: register-wise max — equally simple, O(m) where m = register count

### What Stays the Same

- The `FMSketch` type name and public API (or rename to reflect HLL if preferred)
- Protobuf serialization interface (update the wire format to carry registers instead of hashset)
- All callers: `InsertValue`, `NDV()`, `MergeFMSketch`, `Copy`, `MemoryUsage`
- The GEE estimator in `estimate.go` (independent path, can remain as-is)
- TopN, histogram, and CM Sketch — all unaffected

### Considerations

- **Wire format migration**: the protobuf representation (`tipb.FMSketch`) will need a new format for HLL registers. Consider supporting both formats during a transition period for rolling upgrades (TiKV and TiDB may be at different versions).
- **TiKV side**: FM Sketch is also built on the TiKV side during coprocessor ANALYZE requests. The TiKV implementation would need a matching upgrade.
- **Storage migration**: existing FM Sketch data in `mysql.stats_fm_sketch` would need migration or dual-format decoding.
- **Memory**: HLL with 16K registers uses ~12KB per column vs. FM Sketch's up to ~80KB (10K × 8 bytes). This is an improvement.

## Alternative Approaches Considered

### Option A: Local f-vector, transmit only the summary

Each TiKV region computes a sample locally, builds the exact frequency-of-frequency vector `(f₁, f₂, ...)` from that local sample, and sends only the compact f-vector to TiDB for aggregation into a classical estimator. This is standard sampling + local aggregation, not the patented streaming sketch approach.

**Status**: viable as a complementary improvement if sampling-based estimators are needed beyond what HLL provides. Could layer on top of the HLL upgrade.

### Option B: Direct HyperLogLog (separate from FM Sketch)

Add HLL as a new, separate sketch alongside FM Sketch rather than replacing it.

**Status**: more disruptive than an in-place upgrade. The in-place approach (Option C) is preferred to avoid maintaining two parallel NDV sketch implementations.

## References

- [Paper: Sampling-based NDV Estimation (arxiv)](https://arxiv.org/abs/2206.05476)
- [Paper: KDD 2022 Proceedings](https://dl.acm.org/doi/10.1145/3534678.3539390)
- [HyperLogLog: Flajolet et al., 2007](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [GEE Estimator: Charikar et al., 2000](https://www.vldb.org/conf/2001/P541.pdf)
- [NDV Estimation Survey (2024)](https://link.springer.com/article/10.1007/s11704-024-40952-3)
