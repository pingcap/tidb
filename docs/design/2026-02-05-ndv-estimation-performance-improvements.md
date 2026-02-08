# Proposal: NDV Estimation Performance Improvements for Very Large Tables

- Author:     Terry Purcell
- Last updated:  2026-02-05

## Abstract

This proposal presents ideas for improving the performance of NDV (Number of Distinct Values) estimation during `ANALYZE TABLE` operations, especially for very large tables with billions of rows. The current FMSketch-based approach processes every row, which becomes prohibitively expensive at scale.

## Background

### Current Implementation

NDV estimation in TiDB/TiKV uses the Flajolet-Martin sketch (FMSketch) algorithm:

**TiDB Side (`pkg/statistics/fmsketch.go`):**
- Uses MurmurHash3 to hash each value
- Maintains a `hashset` (limited by `maxSize`) of unique hashed values
- Tracks a `mask` representing the maximum trailing zeros observed
- NDV estimate: `(mask + 1) * hashset.Count()`
- Default `MaxSketchSize` is 10,000

**TiKV Side:**
- Similar FMSketch implementation in Rust
- Processes rows during `AnalyzeColumnsReq` or `AnalyzeMixedReq`
- Results sent to TiDB via protobuf (`tipb::AnalyzeColumnsResp`)

**Histogram NDV:**
- Each histogram `Bucket` has an `NDV` field tracking distinct values in that range
- The overall `Histogram.NDV` aggregates across all buckets

### Problem Statement

For very large tables (100M+ rows), the current approach has several issues:

1. **Full Scan Required**: Every row must be processed, even though NDV estimation is probabilistic
2. **Linear Time Complexity**: Processing time scales linearly with table size
3. **Memory Pressure**: Large hash sets consume significant memory
4. **Analyze Timeout**: Large tables may exceed analyze timeout limits

## Proposed Solutions

### Phase 1: Progressive Sampling (Already Implemented)

The `ProgressiveFMSketch` in `pkg/statistics/progressive_fmsketch.go` implements progressive sampling:

```go
var DefaultProgressiveSchedule = []SamplingPhase{
    {Threshold: 0, Rate: 1.0},            // 0-500K: 100%
    {Threshold: 500_000, Rate: 0.5},      // 500K-1M: 50%
    {Threshold: 1_000_000, Rate: 0.25},   // 1M-5M: 25%
    {Threshold: 5_000_000, Rate: 0.10},   // 5M-10M: 10%
    {Threshold: 10_000_000, Rate: 0.05},  // 10M-50M: 5%
    {Threshold: 50_000_000, Rate: 0.02},  // 50M-100M: 2%
    {Threshold: 100_000_000, Rate: 0.01}, // 100M+: 1%
}
```

**Key Features:**
- Bernoulli sampling with decreasing rates
- Goodman/Chao1-style extrapolation for unsampled data
- Confidence interval estimation
- ~95% reduction in processed rows for 100M row tables

**Remaining Work:**
- [ ] Integrate with TiKV's analyze path
- [ ] Add system variable to control schedule
- [ ] Benchmark accuracy vs performance trade-offs

### Phase 2: HyperLogLog Alternative

**Proposal:** Implement HyperLogLog (HLL) as an alternative to FMSketch.

**Advantages:**
- Fixed memory usage (typically 12KB for 2^14 registers)
- Proven accuracy (~1.04/√m standard error)
- Widely used in production systems (Redis, PostgreSQL, etc.)
- Better mergability for distributed computation

**Implementation Ideas:**

```go
type HyperLogLog struct {
    registers []uint8   // 2^p registers (typically p=14)
    precision uint8     // Precision parameter (p)
}

func (hll *HyperLogLog) Add(hash uint64) {
    // Use first p bits to select register
    registerIdx := hash >> (64 - hll.precision)
    // Count leading zeros in remaining bits
    w := hash << hll.precision
    leadingZeros := bits.LeadingZeros64(w) + 1
    // Update register if new value is larger
    if uint8(leadingZeros) > hll.registers[registerIdx] {
        hll.registers[registerIdx] = uint8(leadingZeros)
    }
}

func (hll *HyperLogLog) Estimate() uint64 {
    // Harmonic mean with bias correction
    // ... (standard HLL formula)
}
```

**Trade-offs:**
- FMSketch: Variable memory, exact for small sets
- HLL: Fixed memory, ~1% error for all sizes

### Phase 3: Stratified Sampling by Region

**Proposal:** Leverage TiKV's region-based data distribution for smarter sampling.

**Approach:**
1. Sample regions proportionally rather than rows
2. Use region statistics (approximate row count, key range) to guide sampling
3. Merge per-region sketches at TiDB level

**Benefits:**
- Better parallelism (sample different regions concurrently)
- More uniform coverage of key space
- Can leverage existing region metadata

**Implementation Considerations:**
- Requires coordination between TiDB and TiKV
- Need to handle region splits/merges during analyze
- May require changes to `AnalyzeRequest` protobuf

### Phase 4: Incremental NDV Updates

**Proposal:** Support incremental NDV updates without full re-analyze.

**Approach:**
1. Track "delta" FMSketch for changes since last analyze
2. Merge delta with existing statistics periodically
3. Use transaction timestamps to identify changed rows

**Key Challenges:**
- DELETE operations: Can't easily "remove" from FMSketch
- Need approximate "staleness" metric
- Complexity in multi-version concurrency

**Potential Implementation:**
```go
type IncrementalNDVTracker struct {
    baseSketch     *FMSketch  // From last full analyze
    deltaSketch    *FMSketch  // Changes since then
    baseRowCount   uint64
    deltaRowCount  uint64
    lastAnalyzeTS  uint64
}

func (t *IncrementalNDVTracker) EstimateCurrentNDV() int64 {
    // Heuristic: merge sketches with adjustment for deletions
    merged := t.baseSketch.Copy()
    merged.MergeFMSketch(t.deltaSketch)

    // Apply decay factor based on estimated churn
    decayFactor := 1.0 - (float64(t.deltaRowCount) / float64(t.baseRowCount)) * 0.1
    return int64(float64(merged.NDV()) * decayFactor)
}
```

### Phase 5: Column-Specific Optimization

**Proposal:** Use column type and metadata for smarter NDV estimation.

**Optimizations by Column Type:**

| Column Type | Optimization |
|-------------|-------------|
| AUTO_INCREMENT | NDV = MAX(col) - MIN(col) + 1 (exact) |
| ENUM | NDV = len(enum_values) (exact) |
| BOOLEAN/BIT(1) | NDV ≤ 2 (exact) |
| DATE/DATETIME | Use range-based estimation |
| Foreign Key | Bound by referenced table's NDV |
| Unique Index | NDV = row_count (exact) |

**Implementation:**
```go
func estimateNDVByType(col *Column, rowCount uint64) (ndv int64, exact bool) {
    switch {
    case col.IsAutoIncrement():
        // Query MIN/MAX and compute exact
        return maxVal - minVal + 1, true
    case col.HasUniqueConstraint():
        return int64(rowCount), true
    case col.Type == mysql.TypeEnum:
        return int64(len(col.EnumValues)), true
    default:
        return 0, false // Use FMSketch
    }
}
```

### Phase 6: Adaptive Sketch Size

**Proposal:** Dynamically adjust sketch size based on observed NDV growth.

**Approach:**
1. Start with small sketch size (e.g., 1,000)
2. Monitor NDV growth rate during sampling
3. If growth rate is high (many unique values), increase sketch size
4. If growth rate plateaus, stop increasing

**Benefits:**
- Memory efficient for low-NDV columns
- Accurate for high-NDV columns
- Self-adapting without user configuration

### Phase 7: Bloom Filter Pre-screening

**Proposal:** Use Bloom filter to quickly identify likely-unique values.

**Approach:**
1. Maintain small Bloom filter during scan
2. Only add to FMSketch if Bloom filter returns "maybe new"
3. Reduces hash insertions for repeated values

**Trade-off Analysis:**
- Extra memory for Bloom filter
- Faster for skewed distributions (many repeats)
- Marginal benefit for uniform distributions

## Performance Comparison (Estimates)

| Method | 100M Rows Time | Memory | Accuracy |
|--------|---------------|--------|----------|
| Current FMSketch | 100% (baseline) | Medium | ~5% error |
| Progressive Sampling | 5-10% | Medium | ~10% error |
| HyperLogLog | 100% | Fixed (12KB) | ~1% error |
| Progressive + HLL | 5-10% | Fixed | ~5% error |
| Region Stratified | 20-30% | Medium | ~5% error |

## Implementation Roadmap

### Phase 1 (Current)
- [x] Implement ProgressiveFMSketch in TiDB
- [ ] Add tests and benchmarks
- [ ] System variable for schedule configuration
- [ ] Integration with analyze workers

### Phase 2 (Short-term)
- [ ] Port progressive sampling to TiKV
- [ ] Implement HyperLogLog option
- [ ] Benchmark on real workloads

### Phase 3 (Medium-term)
- [ ] Region-based stratified sampling
- [ ] Column-type optimizations
- [ ] Adaptive sketch sizing

### Phase 4 (Long-term)
- [ ] Incremental NDV updates
- [ ] ML-based cardinality prediction
- [ ] Cross-column correlation tracking

## Configuration Options

New system variables to consider:

```sql
-- Enable progressive sampling
SET GLOBAL tidb_analyze_progressive_sampling = ON;

-- Sampling schedule aggressiveness (0.0 = always full, 1.0 = most aggressive)
SET GLOBAL tidb_analyze_sample_aggressiveness = 0.5;

-- Minimum sample rate (never sample less than this %)
SET GLOBAL tidb_analyze_min_sample_rate = 0.01;

-- Use HyperLogLog instead of FMSketch
SET GLOBAL tidb_analyze_use_hyperloglog = OFF;

-- Adaptive sketch size
SET GLOBAL tidb_analyze_adaptive_sketch = ON;
```

## Compatibility

- **Backward Compatible**: All optimizations should be opt-in or transparent
- **Statistics Format**: No changes to stored statistics format
- **Query Planning**: No changes to how statistics are used in planning

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Accuracy degradation | Provide confidence intervals, allow fallback to full scan |
| Skewed data distribution | Use stratified sampling, detect skew |
| Memory overhead | Set hard limits, use streaming algorithms |
| Configuration complexity | Provide sensible defaults, auto-tuning |

## References

1. Flajolet-Martin original paper: https://algo.inria.fr/flajolet/Publications/FlMa85.pdf
2. HyperLogLog paper: http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
3. Chao1 estimator: https://en.wikipedia.org/wiki/Mark_and_recapture#Chao1_estimator
4. TiKV FMSketch implementation: `components/tidb_query_datatype/src/codec/data_type/fmsketch.rs`
5. TiDB existing design: `docs/design/2018-09-04-histograms-in-plan.md`

## Open Questions

1. What's the acceptable accuracy trade-off for 10x performance improvement?
2. Should we provide per-table configuration for analyze settings?
3. How to handle partitioned tables with different partition sizes?
4. Should incremental updates be automatic or user-triggered?
5. What monitoring/observability do we need for analyze quality?
