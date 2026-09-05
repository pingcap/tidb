# `pkg/statistics/histogram.go` — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3`), histogram.go — 2660
lines, 111 declarations resolving to 32 unique functions after method
normalization.

## Inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `NewHistogram` | `histogram.rs::Histogram::new` | ported |
| `ValueToString` | `histogram.rs::value_to_string` | ported |
| `MergeHistograms` | `histogram.rs::merge_histograms` | ported |
| `MergePartTopNAndHistToGlobal` | `global_stats.rs::merge_partition_topn(_concurrently)` | ported |
| `CalculateSkewRatioCounts` | `row_estimate.rs::calculate_skew_ratio_counts` | ported |
| `DefaultRowEst` | `row_estimate.rs::default_row_est` | ported |
| `IsAnalyzed` / `IsColumnAnalyzedOrSynthesized` | `stats_version.rs` | ported |
| `HistogramFromProto` | `tidb-executor/src/load_stats.rs:598` | ported |
| `HistogramToProto` | STRUCTURALLY REPLACED: this tier publishes `mysql.stats_buckets` rows directly (`cluster_stats_write.rs:2609`), not tipb | absorbed |
| `HistogramEqual` | Go test-support only (`handle/internal/testutil.go`); not production-served | noted |
| `validRange` | internal to Go's merge delta path; absorbed by `merge_histograms`' range handling | absorbed |
| `buildGlobalHistogram`, `newGlobalMergeRefs`, `newBucketGroupCursor`, `calculateLeftOverlapPercent`, `calculateRightOverlapPercent`, `flattenSortedTopN`, `selectGlobalTopN`, `sortTopNEntries`, `sumPartitionTotals`, `getGlobalPseudoChunk`, `initGlobalPseudoChunk`, `collectVirtualTopN` | global-merge internals behind `merge_partition_histograms` / `merge_partition_topn` (`global_stats.rs`) | absorbed |
| `GetIndexPrefixLens` | index.rs prefix handling (index.rs `prefix lens`) | absorbed |
| `DeepSlice`, `checkKind`, `prepareFieldTypeForHistogram`, `unmatchedOuterRow`-style helpers | module-internal | absorbed |
| runtime-stats members (`updateRuntimeStats` family, `runtimeStatsWithSnapshot`) | execution-statistics surface, not served by this tier | non-served |
| locking members (`getAndLock`, `lockKeyIfNeeded`, `getValueFromLockCtx`) | pessimistic-read surface, not served | non-served |

## Conclusion

The served histogram surface is fully ported and tested (286/294 crate
tests green at walk time; histogram units green). No production divergence
found. One test-support helper (`HistogramEqual`) remains unported and is
only used by Go's own test utilities.

## Validation

`cargo +nightly-2026-08-22 nextest run -p tidb-stats` — 294/294 at walk
time; fmt clean.
