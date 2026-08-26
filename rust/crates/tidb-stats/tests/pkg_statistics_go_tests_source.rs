// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct ports of the Go unit tests in `pkg/statistics` (top-level package)
//! that earlier source-shaped modules did not already pin.
//!
//! Each test cites its Go file and function; golden strings are byte-exact.

use tidb_datatype::Datum;
use tidb_stats::builder::{build_hist_and_topn, BuildOptions, SampleCollector, SampleItem};
use tidb_stats::histogram::Histogram;
use tidb_stats::row_sample_collector::{
    RowSampleCollector, RowSampleRng, SamplePolicy, ScannedRow, SlotValue,
};
use tidb_stats::sample_collector::{
    legacy_sample_collector_from_proto, legacy_sample_collector_to_proto, LegacyRecordChunk,
    LegacySampleBuilder,
};
use tidb_stats::sorted_builder::SortedHistogramBuilder;
use tidb_stats::topn_decoded_string;
use tidb_stats::analyze_version_matches;
use tidb_txnkv::{Handle, IntHandle};

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

/// An integer sample encoded the way Go `codec.EncodeKey` encodes an int:
/// sign-flipped big-endian, so byte order is numeric order.
fn int_encoded(value: i64) -> Vec<u8> {
    ((value as u64) ^ (1 << 63)).to_be_bytes().to_vec()
}

fn int_item(value: i64, ordinal: isize) -> SampleItem {
    SampleItem {
        encoded: int_encoded(value),
        value: Datum::Int(value),
        ordinal,
    }
}

/// Go `Histogram.ToString(0)` / `BucketToString` for int-valued buckets
/// (`ValueToString` on a `Datum` kind renders via `fmt.Sprintf("%v")`, so an
/// integer prints as its decimal form).
fn histogram_to_string_column(hist: &Histogram) -> String {
    fn value_to_string(datum: &Datum) -> String {
        match datum {
            Datum::Int(value) => value.to_string(),
            Datum::Bytes(bytes) => {
                // Encoded key bytes for an int.
                let mut array = [0_u8; 8];
                array.copy_from_slice(bytes);
                (((u64::from_be_bytes(array)) ^ (1 << 63)) as i64).to_string()
            }
            other => panic!("this fixture only stores ints, not {other:?}"),
        }
    }
    let mut lines = vec![format!(
        "column:{} ndv:{} totColSize:{}",
        hist.id, hist.ndv, hist.tot_col_size
    )];
    let mut previous_count = 0_i64;
    for bucket in &hist.buckets {
        // Go's `BucketCount` is the bucket-local delta of the cumulative
        // `Bucket.Count`, and `ToString` prints that delta.
        let count = bucket.count - previous_count;
        previous_count = bucket.count;
        lines.push(format!(
            "num: {} lower_bound: {} upper_bound: {} repeats: {} ndv: {}",
            count,
            value_to_string(&bucket.lower_bound),
            value_to_string(&bucket.upper_bound),
            bucket.repeat,
            bucket.ndv,
        ));
    }
    lines.join("\n")
}

/// Decodes this fixture's sign-flipped big-endian key bytes back to the
/// decimal string Go's `TopN.DecodedString` prints for an int datum.
fn int_value_to_string(bytes: &[u8]) -> Result<String, std::convert::Infallible> {
    let mut array = [0_u8; 8];
    array.copy_from_slice(bytes);
    Ok((((u64::from_be_bytes(array)) ^ (1 << 63)) as i64).to_string())
}

struct StepRng(u64);

impl RowSampleRng for StepRng {
    fn int63(&mut self) -> i64 {
        // xorshift64*, a deterministic stand-in for Go's caller-owned
        // `math/rand.Rand`: the samplers consume any Int63 stream.
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)) >> 1) as i64
    }
}

fn handle_of(columns: &[Datum]) -> Result<Handle, std::convert::Infallible> {
    match columns.first() {
        Some(Datum::Int(value)) => Ok(IntHandle::new(*value).into()),
        _ => Ok(IntHandle::new(0).into()),
    }
}

/// Feeds one non-NULL int row to the collector.
fn offer_int(collector: &mut RowSampleCollector, value: i64, rng: &mut StepRng) {
    let encoded = value.to_be_bytes().to_vec();
    let columns = [Datum::Int(value)];
    let slots = [SlotValue {
        encoded_value: &encoded,
        size: encoded.len() as i64,
        is_null: false,
    }];
    collector
        .collect_with_rng(
            &ScannedRow {
                columns: &columns,
                slots: &slots,
            },
            rng,
        )
        .expect("no memory quota is installed");
}

// ---------------------------------------------------------------------------
// pkg/statistics/table_test.go — TestResolveAnalyzeVersionOnTableKeepsRequestedVersion
// ---------------------------------------------------------------------------

#[test]
fn resolve_analyze_version_on_table_keeps_requested_version() {
    // Go builds Table{HistColl{StatsVer: Version1}, LastAnalyzeVersion: 1}
    // and requires AnalyzeVersionMatchesForTableStats(tbl, Version2) ==
    // false. The Rust policy leaf takes the table's materialized fields.
    assert!(!analyze_version_matches(Some(1), false, 2));
    // The remaining branches of the Go source stay pinned too:
    // nil and pseudo tables always match...
    assert!(analyze_version_matches(None, false, 2));
    assert!(analyze_version_matches(Some(1), true, 2));
    // ...an analyzed table at the requested version matches...
    assert!(analyze_version_matches(Some(2), false, 2));
    // ...and an unanalyzed table matches.
    assert!(analyze_version_matches(Some(0), false, 2));
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestBuildStatsOnRowSample
// ---------------------------------------------------------------------------

#[test]
fn build_stats_on_row_sample() {
    let mut samples = Vec::new();
    for ordinal in 0..1000_i64 {
        samples.push(int_item(ordinal + 1, ordinal as isize));
    }
    let mut ordinal = 1000_isize;
    for _ in 1..10 {
        samples.push(int_item(2, ordinal));
        ordinal += 1;
    }
    for _ in 1..7 {
        samples.push(int_item(4, ordinal));
        ordinal += 1;
    }
    for _ in 1..5 {
        samples.push(int_item(7, ordinal));
        ordinal += 1;
    }
    for _ in 1..3 {
        samples.push(int_item(11, ordinal));
        ordinal += 1;
    }
    let collector = SampleCollector {
        samples,
        null_count: 0,
        count: 1021,
        ndv: 1000,
        total_size: 1021 * 8,
    };
    let out = build_hist_and_topn(
        1,
        &collector,
        BuildOptions {
            num_buckets: 5,
            num_topn: 4,
            ..BuildOptions::default()
        },
        true,
    );
    let topn_string =
        topn_decoded_string(out.topn.as_ref(), int_value_to_string).expect("infallible renderer");
    assert_eq!(
        topn_string,
        "TopN{length: 4, [(2, 10), (4, 7), (7, 5), (11, 3)]}"
    );
    assert_eq!(
        histogram_to_string_column(&out.histogram),
        "column:1 ndv:1000 totColSize:8168\n\
         num: 200 lower_bound: 1 upper_bound: 204 repeats: 1 ndv: 0\n\
         num: 200 lower_bound: 205 upper_bound: 404 repeats: 1 ndv: 0\n\
         num: 200 lower_bound: 405 upper_bound: 604 repeats: 1 ndv: 0\n\
         num: 200 lower_bound: 605 upper_bound: 804 repeats: 1 ndv: 0\n\
         num: 196 lower_bound: 805 upper_bound: 1000 repeats: 1 ndv: 0"
    );
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestBuildSampleFullNDV
// ---------------------------------------------------------------------------

#[test]
fn build_sample_full_ndv() {
    // Column NDV (103, from the FM sketch over values 100..200) exceeds the
    // sample NDV (3): the TopN list must be trimmed to sampleNDV-1 items.
    let mut samples = Vec::new();
    let mut ordinal = 0_isize;
    for _ in 1..41 {
        samples.push(int_item(2, ordinal));
        ordinal += 1;
    }
    for _ in 1..31 {
        samples.push(int_item(4, ordinal));
        ordinal += 1;
    }
    for _ in 1..25 {
        samples.push(int_item(7, ordinal));
        ordinal += 1;
    }
    let collector = SampleCollector {
        samples,
        null_count: 0,
        count: 200,
        ndv: 103,
        total_size: 94 * 8,
    };
    let out = build_hist_and_topn(
        1,
        &collector,
        BuildOptions {
            num_buckets: 0,
            num_topn: 100,
            ..BuildOptions::default()
        },
        true,
    );
    let topn_string =
        topn_decoded_string(out.topn.as_ref(), int_value_to_string).expect("infallible renderer");
    assert_eq!(topn_string, "TopN{length: 2, [(2, 85), (4, 63)]}");
    let topn = out.topn.expect("a positive TopN request builds one");
    assert_eq!(
        topn.num(),
        2,
        "TopN should be trimmed to sampleNDV-1 items when ndv > sampleNDV"
    );
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestBuildHistAndTopNUsesAnalyzeDefaultGlobals
// ---------------------------------------------------------------------------

#[test]
fn build_hist_and_topn_uses_analyze_default_globals() {
    // Go stores the session defaults into vardef globals; the Rust entrypoint
    // receives them explicitly through BuildOptions, which is where the
    // caller-owned global read landed.
    let options = |num_buckets: isize, num_topn: isize| BuildOptions {
        num_buckets,
        num_topn,
        default_num_buckets: 4,
        default_num_topn: 4,
    };
    let mut samples = Vec::new();
    let mut ordinal = 0_isize;
    let counts = [(1_i64, 20_i64), (2, 3), (3, 3), (4, 3), (5, 3), (6, 3)];
    for (value, count) in counts {
        for _ in 0..count {
            samples.push(int_item(value, ordinal));
            ordinal += 1;
        }
    }
    let collector = SampleCollector {
        samples,
        null_count: 0,
        count: 35,
        ndv: 6,
        total_size: 35 * 8,
    };

    let out = build_hist_and_topn(1, &collector, options(4, 4), true);
    let topn = out.topn.expect("topn requested");
    assert_eq!(topn.num(), 1);
    assert_eq!(out.histogram.buckets.len(), 2);

    let explicit = build_hist_and_topn(1, &collector, options(4, 5), true);
    let explicit_topn = explicit.topn.expect("topn requested");
    assert_eq!(explicit_topn.num(), 5);

    let explicit = build_hist_and_topn(1, &collector, options(5, 4), true);
    let explicit_topn = explicit.topn.expect("topn requested");
    assert_eq!(explicit_topn.num(), 1);
    assert!(explicit.histogram.buckets.len() > out.histogram.buckets.len());
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestWeightedSampling
// ---------------------------------------------------------------------------

#[test]
fn weighted_sampling() {
    // 1000 reservoir samples of 20 rows drawn from rows 0..100: every row's
    // empirical frequency must sit inside Go's Chernoff bound with delta
    // 0.5 around sampleNum*loopCnt/rowNum.
    const SAMPLE_NUM: usize = 20;
    const ROW_NUM: i64 = 100;
    const LOOP_CNT: u32 = 1000;
    let mut item_cnt = vec![0_u64; ROW_NUM as usize];
    for loop_index in 0..LOOP_CNT {
        let mut collector =
            RowSampleCollector::new(1, SamplePolicy::Reservoir { max_sample_size: SAMPLE_NUM });
        let mut rng = StepRng((loop_index as u64) | 1);
        for row in 0..ROW_NUM {
            offer_int(&mut collector, row, &mut rng);
        }
        let (_, _, sampled) = collector.into_parts(handle_of).expect("infallible handles");
        assert_eq!(sampled.len(), SAMPLE_NUM);
        for row in sampled {
            let Datum::Int(value) = row.columns[0] else {
                panic!("fixture stores ints");
            };
            item_cnt[value as usize] += 1;
        }
    }
    let exp_frequency = (SAMPLE_NUM as f64) * (LOOP_CNT as f64) / (ROW_NUM as f64);
    let delta = 0.5_f64;
    for (row, count) in item_cnt.iter().enumerate() {
        let count = *count as f64;
        assert!(
            count >= exp_frequency / (1.0 + delta) && count <= exp_frequency * (1.0 + delta),
            "The frequency {count} of row {row} exceeds the Chernoff Bound"
        );
    }
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestDistributedWeightedSampling
// ---------------------------------------------------------------------------

#[test]
fn distributed_weighted_sampling() {
    // Five per-node reservoir collectors of 10 rows each are merged into one
    // root collector, 1499 times (Go's loop runs `loopI := 1; loopI < 1500`).
    const SAMPLE_NUM: usize = 10;
    const ROW_NUM: i64 = 100;
    const BATCH: i64 = 5;
    const BATCH_SIZE: i64 = ROW_NUM / BATCH;
    const LOOP_CNT: i64 = 1500;
    let mut item_cnt = vec![0_u64; ROW_NUM as usize];
    for loop_index in 1..LOOP_CNT {
        let mut root = RowSampleCollector::new(
            1,
            SamplePolicy::Reservoir {
                max_sample_size: SAMPLE_NUM,
            },
        );
        for batch in 0..BATCH {
            let mut node = RowSampleCollector::new(
                1,
                SamplePolicy::Reservoir {
                    max_sample_size: SAMPLE_NUM,
                },
            );
            let mut rng = StepRng(((loop_index * 31 + batch) as u64) | 1);
            for row in 0..BATCH_SIZE {
                offer_int(&mut node, row + BATCH_SIZE * batch, &mut rng);
            }
            root.merge(node);
        }
        let (_, _, sampled) = root.into_parts(handle_of).expect("infallible handles");
        assert_eq!(sampled.len(), SAMPLE_NUM);
        for row in sampled {
            let Datum::Int(value) = row.columns[0] else {
                panic!("fixture stores ints");
            };
            item_cnt[value as usize] += 1;
        }
    }
    // Go compares against loopCnt (=1500), not the executed 1499 iterations.
    let exp_frequency = (SAMPLE_NUM as f64) * (LOOP_CNT as f64) / (ROW_NUM as f64);
    let delta = 0.5_f64;
    for (row, count) in item_cnt.iter().enumerate() {
        let count = *count as f64;
        assert!(
            count >= exp_frequency / (1.0 + delta) && count <= exp_frequency * (1.0 + delta),
            "the frequency {count} of row {row} exceeds the Chernoff Bound"
        );
    }
}

// ---------------------------------------------------------------------------
// pkg/statistics/sample_test.go — TestSampleSerial
// ---------------------------------------------------------------------------

/// The shared recordSet of `createTestSampleSuite`: cursor-based handles
/// 1..=10000 (Go `firstIsID`) paired with data values that are NULL below
/// index 1000 and shifted by +1 on every third index from 1000 and by +2 on
/// every fifth (Go's loops step indices, they do not test divisibility).
fn suite_rows() -> Vec<[Datum; 2]> {
    let mut rows = Vec::with_capacity(10000);
    for cursor in 1..=10000_i64 {
        let index = cursor - 1;
        let mut value = index;
        if index >= 1000 {
            if (index - 1000) % 3 == 0 {
                value += 1;
            }
            if (index - 1000) % 5 == 0 {
                value += 2;
            }
        }
        let datum = if index < 1000 {
            Datum::Null
        } else {
            Datum::Int(value)
        };
        rows.push([Datum::Int(cursor), datum]);
    }
    rows
}

fn suite_chunk(field_count: usize) -> LegacyRecordChunk {
    LegacyRecordChunk {
        field_count,
        rows: suite_rows().into_iter().map(Vec::from).collect(),
    }
}

fn encode_for_fm(datum: &Datum) -> Result<Vec<u8>, std::convert::Infallible> {
    Ok(tidb_codec::encode_value(std::slice::from_ref(datum)).expect("int/null encoding"))
}

#[test]
fn sample_serial_collect_column_stats() {
    // Go SubTestCollectColumnStats: one PK-handle column plus one sampled
    // column over the shared record set.
    let builder = LegacySampleBuilder {
        column_count: 1,
        max_sample_size: 10000,
        max_fm_sketch_size: 1000,
        cmsketch_depth: 8,
        cmsketch_width: 2048,
        collated_columns: vec![false],
    };
    let mut pk_builder = SortedHistogramBuilder::new(256, 1, 2);
    let collectors = builder
        .collect_column_stats(
            [suite_chunk(2)],
            Some(&mut pk_builder),
            |_, datum, _| Ok::<_, std::convert::Infallible>(datum),
            encode_for_fm,
        )
        .expect("collection succeeds");
    assert_eq!(collectors[0].null_count + collectors[0].count, 10000);
    assert_eq!(
        collectors[0].fm_sketch.as_ref().expect("fm").ndv(),
        6232,
        "golden FM-sketch NDV of the shifted column values"
    );
    assert_eq!(
        collectors[0].cmsketch.as_ref().expect("cms").total_count(),
        collectors[0].count as u64
    );
    assert_eq!(pk_builder.count(), 10000);
    assert_eq!(pk_builder.histogram().ndv, 10000);
}

#[test]
fn sample_serial_merge_sample_collector() {
    // Go SubTestMergeSampleCollector: two column collectors over the same
    // record set, merged into the first.
    let builder = LegacySampleBuilder {
        column_count: 2,
        max_sample_size: 1000,
        max_fm_sketch_size: 1000,
        cmsketch_depth: 8,
        cmsketch_width: 2048,
        collated_columns: vec![false, false],
    };
    let mut collectors = builder
        .collect_column_stats(
            [suite_chunk(2)],
            None,
            |_, datum, _| Ok::<_, std::convert::Infallible>(datum),
            encode_for_fm,
        )
        .expect("collection succeeds");
    assert_eq!(collectors.len(), 2);
    let (destination, source) = collectors.split_at_mut(1);
    destination[0].is_merger = true;
    destination[0].merge(&source[0]);
    assert_eq!(destination[0].fm_sketch.as_ref().expect("fm").ndv(), 9280);
    assert_eq!(destination[0].samples.len(), 1000);
    assert_eq!(destination[0].null_count, 1000);
    assert_eq!(destination[0].count, 19000);
    assert_eq!(
        destination[0]
            .cmsketch
            .as_ref()
            .expect("cms")
            .total_count(),
        destination[0].count as u64
    );
}

#[test]
fn sample_serial_collector_proto_conversion() {
    // Go SubTestCollectorProtoConversion: SampleCollectorToProto followed by
    // FromProto preserves every aggregate.
    let builder = LegacySampleBuilder {
        column_count: 2,
        max_sample_size: 10000,
        max_fm_sketch_size: 1000,
        cmsketch_depth: 8,
        cmsketch_width: 2048,
        collated_columns: vec![false, false],
    };
    let collectors = builder
        .collect_column_stats([suite_chunk(2)], None, |_, datum, _| {
            Ok::<_, std::convert::Infallible>(datum)
        }, encode_for_fm)
        .expect("collection succeeds");
    for collector in &collectors {
        let proto = legacy_sample_collector_to_proto(collector).expect("proto encoding");
        let restored = legacy_sample_collector_from_proto(&proto).expect("proto decoding");
        assert_eq!(restored.count, collector.count);
        assert_eq!(restored.null_count, collector.null_count);
        assert_eq!(
            restored.cmsketch.as_ref().map(|cms| cms.total_count()),
            collector.cmsketch.as_ref().map(|cms| cms.total_count())
        );
        assert_eq!(
            restored.fm_sketch.as_ref().map(|sketch| sketch.ndv()),
            collector.fm_sketch.as_ref().map(|sketch| sketch.ndv())
        );
        assert_eq!(restored.total_size, collector.total_size);
        assert_eq!(restored.samples.len(), collector.samples.len());
    }
}

// ---------------------------------------------------------------------------
// go-parity gaps: behavior the Rust workspace has not ported yet
// ---------------------------------------------------------------------------

// pkg/statistics/histogram_test.go — TestValueToString4InvalidKey.
#[test]
#[ignore = "go-parity-gap: statistics::ValueToString is not ported to the Rust workspace yet"]
fn value_to_string_4_invalid_key() {
}

// pkg/statistics/histogram_test.go — TestNewPseudoHistogramReuseChunk.
#[test]
#[ignore = "go-parity-gap: NewPseudoHistogram (shared Bounds chunk instance) is not ported yet"]
fn new_pseudo_histogram_reuse_chunk() {
}

// pkg/statistics/statistics_test.go — TestPruneTopN.
#[test]
#[ignore = "go-parity-gap: pruneTopNItem is crate-private in the Rust port and not exposed for testing"]
fn prune_top_n() {
}

// pkg/statistics/integration_test.go — TestExpBackoffEstimation.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack; no Rust integration harness owns this yet"]
fn exp_backoff_estimation() {
}

// pkg/statistics/integration_test.go — TestNULLOnFullSampling.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn null_on_full_sampling() {
}

// pkg/statistics/integration_test.go — TestAnalyzeSnapshot.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn analyze_snapshot() {
}

// pkg/statistics/integration_test.go — TestOutdatedStatsCheck.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn outdated_stats_check() {
}

// pkg/statistics/integration_test.go — TestShowHistogramsLoadStatus.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit plus SHOW executor"]
fn show_histograms_load_status() {
}

// pkg/statistics/integration_test.go — TestSingleColumnIndexNDV.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn single_column_index_ndv() {
}

// pkg/statistics/integration_test.go — TestColumnStatsLazyLoad.
#[test]
#[ignore = "go-parity-gap: needs the Go statistics handle and lazy-load path"]
fn column_stats_lazy_load() {
}

// pkg/statistics/integration_test.go — TestUpdateNotLoadIndexFMSketch.
#[test]
#[ignore = "go-parity-gap: needs the Go statistics handle update path"]
fn update_not_load_index_fm_sketch() {
}

// pkg/statistics/integration_test.go — TestIssue44369.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn issue44369() {
}

// pkg/statistics/integration_test.go — TestTableLastAnalyzeVersion.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit/session/storage stack"]
fn table_last_analyze_version() {
}

// pkg/statistics/integration_test.go — TestGlobalIndexWithHistoricalStats.
#[test]
#[ignore = "go-parity-gap: needs the Go testkit, global indexes, and historical stats"]
fn global_index_with_historical_stats() {
}

// pkg/statistics/integration_test.go — TestLastAnalyzeVersionNotChangedWithAsyncStatsLoad.
#[test]
#[ignore = "go-parity-gap: needs the Go async stats-load session path"]
fn last_analyze_version_not_changed_with_async_stats_load() {
}

// pkg/statistics/integration_test.go — TestSaveMetaToStorage.
#[test]
#[ignore = "go-parity-gap: needs the Go meta/storage persistence stack"]
fn save_meta_to_storage() {
}
