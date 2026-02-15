# Progressive Sampling for NDV Estimation in TiDB ANALYZE

## Summary

This document proposes a **Progressive Sampling** approach for NDV (Number of Distinct Values) calculation during `ANALYZE TABLE` operations. The goal is to significantly reduce the computational cost of NDV estimation for large tables (100M+ rows) while maintaining acceptable accuracy.

## Problem Statement

### Current Behavior

TiDB currently uses **FMSketch (Flajolet-Martin Sketch)** for NDV estimation. The FMSketch:
- Processes **every row** in the table (or sampled rows when sampling is enabled for histograms)
- Maintains a hashset of size up to `MaxSketchSize = 10,000`
- Uses level-based pruning when the hashset exceeds capacity

### Performance Issues

For tables with 100M+ rows:
1. **CPU Cost**: Hashing and inserting every value is expensive
2. **Time Cost**: Linear scan of all rows is slow
3. **Resource Contention**: Long-running ANALYZE blocks other operations
4. **Diminishing Returns**: After observing millions of distinct values, additional samples provide minimal accuracy improvement

### Desired Behavior

- **First 500,000 rows**: Process 100% of data for high accuracy on smaller tables and accurate initial NDV estimation
- **Beyond 500,000 rows**: Progressively reduce sampling rate as row count increases
- **Final Result**: Extrapolate NDV to approximate what a full scan would produce

## Design

### Core Concept: Progressive Sampling Schedule

```
Rows Processed    Sample Rate    Effective Rows Sampled
─────────────────────────────────────────────────────────
0 - 500K          100%           500,000
500K - 1M         50%            250,000
1M - 5M           25%            1,000,000
5M - 10M          10%            500,000
10M - 50M         5%             2,000,000
50M - 100M        2%             1,000,000
100M+             1%             variable
─────────────────────────────────────────────────────────
Total for 100M row table:        ~5.25M samples
```

This represents a **95% reduction** in processed rows for a 100M row table.

### Algorithm: Adaptive FMSketch with Sampling

```go
// ProgressiveFMSketch extends FMSketch with progressive sampling
type ProgressiveFMSketch struct {
    *FMSketch

    // Sampling state
    rowsProcessed     uint64    // Total rows seen
    rowsSampled       uint64    // Rows actually processed
    currentSampleRate float64   // Current sampling rate (0.0-1.0)

    // Phase tracking
    phaseThresholds   []uint64  // Row counts where rate changes
    phaseRates        []float64 // Sampling rates for each phase
    currentPhase      int

    // For extrapolation
    observedNDV       int64     // NDV from sampled rows
    sampleFraction    float64   // Total fraction of data sampled
}
```

### Sampling Decision Function

```go
func (s *ProgressiveFMSketch) ShouldSample() bool {
    s.rowsProcessed++

    // Phase 1: Full sampling for first 500K rows
    if s.rowsProcessed <= 500_000 {
        s.rowsSampled++
        return true
    }

    // Update phase if threshold crossed
    s.updatePhase()

    // Bernoulli sampling with current rate
    if s.fastRand() < s.currentSampleRate {
        s.rowsSampled++
        return true
    }
    return false
}

func (s *ProgressiveFMSketch) updatePhase() {
    for s.currentPhase < len(s.phaseThresholds)-1 {
        if s.rowsProcessed >= s.phaseThresholds[s.currentPhase+1] {
            s.currentPhase++
            s.currentSampleRate = s.phaseRates[s.currentPhase]
        } else {
            break
        }
    }
}
```

### NDV Extrapolation

The key challenge is extrapolating the observed NDV from samples to estimate the true NDV. We use the **Goodman estimator** for unbiased NDV estimation:

```go
func (s *ProgressiveFMSketch) EstimateNDV() int64 {
    if s.rowsSampled == 0 {
        return 0
    }

    // Get observed NDV from FMSketch
    observedNDV := s.FMSketch.NDV()

    // If we sampled everything, return exact NDV
    if s.rowsSampled == s.rowsProcessed {
        return observedNDV
    }

    // Goodman estimator for NDV extrapolation
    // Based on: "On the bias of the Horvitz-Thompson estimator"
    sampleFraction := float64(s.rowsSampled) / float64(s.rowsProcessed)

    // f1 = number of values appearing exactly once in sample
    f1 := s.countSingletons()

    // Extrapolated NDV using Chao1-style estimator
    // NDV_est = observedNDV + f1 * (1 - sampleFraction) / sampleFraction
    adjustment := float64(f1) * (1.0 - sampleFraction) / sampleFraction
    estimatedNDV := float64(observedNDV) + adjustment

    // Bound by total rows
    if estimatedNDV > float64(s.rowsProcessed) {
        estimatedNDV = float64(s.rowsProcessed)
    }

    return int64(math.Round(estimatedNDV))
}
```

### Confidence Interval Calculation

Provide users with accuracy bounds:

```go
func (s *ProgressiveFMSketch) ConfidenceInterval(confidence float64) (lower, upper int64) {
    ndv := s.EstimateNDV()

    // Standard error based on sample size and observed NDV
    sampleFraction := float64(s.rowsSampled) / float64(s.rowsProcessed)
    variance := float64(ndv) * (1.0 - sampleFraction) / sampleFraction
    stdErr := math.Sqrt(variance)

    // z-score for confidence level (e.g., 1.96 for 95%)
    z := normalQuantile(confidence)

    lower = int64(math.Max(0, float64(ndv) - z*stdErr))
    upper = int64(float64(ndv) + z*stdErr)

    return lower, upper
}
```

## Implementation Plan

### Phase 1: Core ProgressiveFMSketch (TiDB)

**Files to modify:**

1. `pkg/statistics/fmsketch.go` - Add ProgressiveFMSketch type
2. `pkg/statistics/sample.go` - Integrate with SampleCollector
3. `pkg/executor/analyze_col.go` - Use progressive sampling in analyze

**New files:**

1. `pkg/statistics/progressive_fmsketch.go` - Core implementation
2. `pkg/statistics/ndv_estimator.go` - Extrapolation algorithms

### Phase 2: Configuration & Control

**New system variables:**

```sql
-- Enable/disable progressive sampling (default: ON for tables > 1M rows)
SET tidb_analyze_progressive_sampling = ON;

-- Minimum rows before sampling starts (default: 500000)
SET tidb_analyze_progressive_sampling_threshold = 500000;

-- Minimum sample rate for very large tables (default: 0.01)
SET tidb_analyze_min_sample_rate = 0.01;
```

**Files to modify:**

1. `pkg/sessionctx/variable/sysvar.go` - Add new variables
2. `pkg/sessionctx/variable/tidb_vars.go` - Variable definitions

### Phase 3: TiKV Integration (Optional Enhancement)

For even better performance, push progressive sampling to TiKV:

**Files in TiKV:**

1. `components/tidb_query_datatype/src/codec/datum.rs`
2. `components/tidb_query_executors/src/analyze.rs`

**Protocol changes:**

```protobuf
message AnalyzeColumnsReq {
    // Existing fields...

    // Progressive sampling configuration
    bool enable_progressive_sampling = 20;
    uint64 progressive_threshold = 21;
    double min_sample_rate = 22;
}
```

## Sampling Rate Schedule Configuration

### Default Schedule

```go
var DefaultProgressiveSchedule = ProgressiveSchedule{
    Phases: []SamplingPhase{
        {Threshold: 0,          Rate: 1.0},   // 0-500K: 100%
        {Threshold: 500_000,    Rate: 0.5},   // 500K-1M: 50%
        {Threshold: 1_000_000,  Rate: 0.25},  // 1M-5M: 25%
        {Threshold: 5_000_000,  Rate: 0.10},  // 5M-10M: 10%
        {Threshold: 10_000_000, Rate: 0.05},  // 10M-50M: 5%
        {Threshold: 50_000_000, Rate: 0.02},  // 50M-100M: 2%
        {Threshold: 100_000_000, Rate: 0.01}, // 100M+: 1%
    },
}
```

### Adaptive Schedule Based on Observed NDV Growth

```go
func (s *ProgressiveFMSketch) AdaptiveRateAdjustment() {
    // If NDV is growing slowly, reduce sample rate faster
    ndvGrowthRate := s.calculateNDVGrowthRate()

    if ndvGrowthRate < 0.001 { // Very few new distinct values
        s.currentSampleRate *= 0.5 // Halve the rate
    } else if ndvGrowthRate > 0.1 { // Many new distinct values
        s.currentSampleRate = math.Min(1.0, s.currentSampleRate * 1.5) // Increase rate
    }
}
```

## Accuracy Analysis

### Theoretical Bounds

For a table with:
- N = total rows
- D = true NDV
- n = sampled rows

The expected error of the Goodman estimator is:

```
E[error] ≈ D * sqrt((1-f)/f) / sqrt(d)

where:
  f = n/N (sample fraction)
  d = observed distinct values in sample
```

### Empirical Testing Plan

| Table Size | True NDV | Sample Rate | Expected Error |
|------------|----------|-------------|----------------|
| 1M rows    | 100K     | 75%         | < 1%           |
| 10M rows   | 500K     | 15%         | < 3%           |
| 100M rows  | 1M       | 5%          | < 5%           |
| 1B rows    | 10M      | 1%          | < 10%          |

### Handling Edge Cases

1. **Very low NDV** (< 1000): Always use 100% sampling
2. **Very high NDV** (NDV ≈ row count): Increase minimum sample rate
3. **Skewed distributions**: Use stratified sampling hints

## Performance Projections

### CPU Time Reduction

| Table Size | Current Time | Progressive Time | Speedup |
|------------|--------------|------------------|---------|
| 1M rows    | 5s           | 4s               | 1.25x   |
| 10M rows   | 50s          | 15s              | 3.3x    |
| 100M rows  | 8min         | 45s              | 10x     |
| 1B rows    | 80min        | 5min             | 16x     |

### Memory Usage

No significant change - FMSketch already bounds memory at `MaxSketchSize = 10,000`.

## API Changes

### ANALYZE Statement Extensions

```sql
-- Use progressive sampling (default for large tables)
ANALYZE TABLE t WITH PROGRESSIVE SAMPLING;

-- Force full scan (disable progressive sampling)
ANALYZE TABLE t WITH FULL SCAN;

-- Custom sampling parameters
ANALYZE TABLE t WITH PROGRESSIVE SAMPLING
    THRESHOLD 1000000
    MIN_RATE 0.05;
```

### Statistics Metadata

Store sampling information for transparency:

```sql
-- New columns in mysql.stats_meta or mysql.stats_histograms
ALTER TABLE mysql.stats_histograms ADD COLUMN (
    sample_method VARCHAR(32),      -- 'full' or 'progressive'
    sample_rate DOUBLE,             -- Effective sample rate
    rows_sampled BIGINT,            -- Actual rows processed
    ndv_confidence_low BIGINT,      -- 95% CI lower bound
    ndv_confidence_high BIGINT      -- 95% CI upper bound
);
```

## Singleton Tracking for Better Extrapolation

The Goodman/Chao1 estimators require counting "singletons" (values appearing exactly once). We can track this efficiently:

```go
type ProgressiveFMSketch struct {
    // ... existing fields

    // Singleton tracking using a Count-Min Sketch
    frequencySketch *CMSketch

    // Or simpler: track approximate singleton count
    approximateSingletons int64
    singletonSampleRate   float64
}

func (s *ProgressiveFMSketch) countSingletons() int64 {
    // Option 1: Use CMSketch to estimate frequency distribution
    // Option 2: Maintain a separate small sample for singleton counting
    // Option 3: Use statistical approximation based on observed NDV curve

    // Simple approximation: singletons ≈ k * NDV for some constant k
    // Based on Zipf's law, typically k ≈ 0.5-0.7 for real data
    return int64(float64(s.FMSketch.NDV()) * 0.6)
}
```

## Alternative Approaches Considered

### 1. HyperLogLog++ (Not Chosen)

- More accurate than FMSketch for large NDV
- But: Requires significant code changes
- Progressive sampling works with existing FMSketch

### 2. Streaming Distinct Count (Not Chosen)

- Algorithms like Theta Sketch, KMV
- But: More complex to implement
- Progressive sampling is simpler and sufficient

### 3. Fixed-Rate Sampling (Not Chosen)

- Simple: sample at fixed rate (e.g., 1%)
- But: Poor accuracy for smaller tables
- Progressive gives best of both worlds

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Underestimation for skewed data | Wrong query plans | Adaptive rate based on NDV growth |
| Overestimation for high-cardinality | Unnecessary index scans | Conservative upper bound |
| Inconsistent results across runs | User confusion | Deterministic sampling with seed |
| Regression for small tables | No benefit | Disable for tables < threshold |

## Testing Strategy

### Unit Tests

```go
func TestProgressiveFMSketch(t *testing.T) {
    tests := []struct {
        rows     int
        ndv      int
        expected float64 // Expected accuracy
    }{
        {1_000_000, 100_000, 0.99},
        {10_000_000, 500_000, 0.97},
        {100_000_000, 1_000_000, 0.95},
    }

    for _, tt := range tests {
        sketch := NewProgressiveFMSketch()
        // Feed synthetic data with known NDV
        // Verify estimated NDV is within expected accuracy
    }
}
```

### Integration Tests

1. Compare with full-scan NDV for various table sizes
2. Benchmark ANALYZE time improvements
3. Query plan quality comparison with progressive vs full NDV

### Regression Tests

1. Ensure existing ANALYZE behavior unchanged for small tables
2. Verify statistics persistence and reload
3. Check compatibility with existing hint system

## Conclusion

Progressive sampling for NDV estimation offers a practical path to dramatically reduce ANALYZE time for large tables while maintaining acceptable accuracy. The approach:

1. **Preserves accuracy** for smaller tables (< 500K rows)
2. **Scales efficiently** with table size
3. **Provides transparency** through confidence intervals
4. **Integrates cleanly** with existing FMSketch implementation

The expected **10-16x speedup** for 100M+ row tables makes ANALYZE operations practical in production environments where they are currently avoided due to resource consumption.

## References

1. Flajolet, P., & Martin, G. N. (1985). Probabilistic counting algorithms for data base applications
2. Chao, A. (1984). Nonparametric estimation of the number of classes in a population
3. Goodman, L. A. (1949). On the estimation of the number of classes in a population
4. Heule, S., Nunkesser, M., & Hall, A. (2013). HyperLogLog in practice: algorithmic engineering of a state of the art cardinality estimation algorithm
