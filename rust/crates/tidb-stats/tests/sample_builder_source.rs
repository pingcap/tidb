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

//! Ports of `pkg/statistics/sample_test.go` cases that drive
//! `BuildHistAndTopN` over a hand-built `SampleCollector`.
//!
//! Go testport batch b042. The Go tests encode each sample through
//! `codec.EncodeKey`; here the order-preserving sign-flipped big-endian
//! integer encoding stands in (grouping only needs an injective,
//! order-presencoding mapping). Expected TopN strings, histogram buckets,
//! and scaled counts are the literal Go assertions from master.

use tidb_stats::builder::{build_hist_and_topn, BuildOptions, SampleCollector, SampleItem};

/// Sign-flipped big-endian encoding of an i64, matching the ordering of Go's
/// `codec.EncodeKey` for integers.
fn encoded_int(value: i64) -> Vec<u8> {
    ((value as u64) ^ (1 << 63)).to_be_bytes().to_vec()
}

fn item(value: i64, ordinal: usize) -> SampleItem {
    SampleItem {
        encoded: encoded_int(value),
        value: tidb_datatype::Datum::new_int(value),
        ordinal: ordinal as isize,
    }
}

/// Decodes [`encoded_int`] back to its decimal form for `DecodedString`.
fn decode_int(bytes: &[u8]) -> Result<String, std::convert::Infallible> {
    let mut array = [0_u8; 8];
    array.copy_from_slice(bytes);
    Ok(((u64::from_be_bytes(array) ^ (1 << 63)) as i64).to_string())
}

fn topn_string(topn: &tidb_stats::TopN) -> String {
    topn.decoded_string(decode_int)
        .expect("infallible decoder")
}

/// sample_test.go::TestBuildStatsOnRowSample.
///
/// 1000 distinct values 1..=1000 plus small runs of duplicates; the FM
/// sketch NDV (1000) exceeds what the duplicate tail contributes. With a
/// full sample (count == len(samples)) the scaling factor is exactly 1, so
/// the TopN carries raw occurrence counts and five equal-mass buckets cover
/// the remainder.
#[test]
fn source_build_stats_on_row_sample_matches_go_fixture() {
    let mut values: Vec<i64> = Vec::new();
    values.extend(1..=1000);
    values.extend(std::iter::repeat_n(2, 9));
    values.extend(std::iter::repeat_n(4, 6));
    values.extend(std::iter::repeat_n(7, 4));
    values.extend(std::iter::repeat_n(11, 2));

    let count = values.len() as i64;
    let collector = SampleCollector {
        samples: values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| item(*value, ordinal))
            .collect(),
        null_count: 0,
        count,
        ndv: 1000,
        total_size: count * 8,
    };

    // Go: BuildHistAndTopN(ctx, 5, 4, 1, collector, tp, true, nil)
    let options = BuildOptions {
        num_buckets: 5,
        num_topn: 4,
        ..BuildOptions::default()
    };
    let built = build_hist_and_topn(1, &collector, options, true);

    let topn = built.topn.expect("Go asserts a non-nil TopN");
    assert_eq!(
        topn_string(&topn),
        "TopN{length: 4, [(2, 10), (4, 7), (7, 5), (11, 3)]}"
    );

    let hist = &built.histogram;
    assert_eq!(hist.ndv, 1000);
    assert_eq!(hist.tot_col_size, 8168); // Go totColSize: 1021 * 8
    assert_eq!(hist.buckets.len(), 5, "Go ToString pins five buckets");

    // Go golden: num/repeat per bucket plus lower/upper bounds.
    let go_buckets = [
        (1, 204, 200, 1),
        (205, 404, 200, 1),
        (405, 604, 200, 1),
        (605, 804, 200, 1),
        (805, 1000, 196, 1),
    ];
    let mut prev_count = 0_i64;
    for (bucket, (lower, upper, num, repeat)) in hist.buckets.iter().zip(go_buckets) {
        assert_eq!(
            bucket.lower_bound,
            tidb_datatype::Datum::new_int(lower),
            "bucket lower bound"
        );
        assert_eq!(
            bucket.upper_bound,
            tidb_datatype::Datum::new_int(upper),
            "bucket upper bound"
        );
        assert_eq!(bucket.count - prev_count, num, "bucket mass");
        assert_eq!(bucket.repeat, repeat, "bucket repeat");
        prev_count = bucket.count;
    }
    assert_eq!(prev_count, 996, "cumulative rows outside the TopN");
}

/// sample_test.go::TestBuildSampleFullNDV.
///
/// The column NDV (103, from the FM sketch fed unseen values 100..200) is
/// far above the sample NDV (3), so pruning trims the TopN to
/// `sampleNDV - 1` entries and scales counts by count/len(samples).
#[test]
fn source_build_sample_full_ndv_trims_topn_to_sample_ndv_minus_one() {
    let mut values: Vec<i64> = Vec::new();
    values.extend(std::iter::repeat_n(2, 40));
    values.extend(std::iter::repeat_n(4, 30));
    values.extend(std::iter::repeat_n(7, 24));

    let collector = SampleCollector {
        samples: values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| item(*value, ordinal))
            .collect(),
        null_count: 0,
        count: 200,
        ndv: 103,
        total_size: values.len() as i64 * 8,
    };
    assert!(collector.ndv > 3, "column NDV must exceed the sample NDV");

    // Go: BuildHistAndTopN(ctx, 0, 100, 1, collector, tp, true, nil)
    let options = BuildOptions {
        num_buckets: 0,
        num_topn: 100,
        ..BuildOptions::default()
    };
    let built = build_hist_and_topn(1, &collector, options, true);

    let topn = built.topn.expect("Go asserts a non-nil TopN");
    assert_eq!(
        topn_string(&topn),
        "TopN{length: 2, [(2, 85), (4, 63)]}",
        "counts scale by 200/94 and truncate; list trims to sampleNDV-1"
    );
    assert_eq!(topn.num(), 2);
}

/// sample_test.go::TestBuildHistAndTopNUsesAnalyzeDefaultGlobals.
///
/// A `numTopN` equal to the active analyze default enables pruning; any
/// other value is treated as an explicit user request that must be honored
/// verbatim.
#[test]
fn source_build_hist_and_topn_uses_analyze_default_globals() {
    let mut values: Vec<i64> = vec![1_i64; 20];
    values.extend([2, 3, 4, 5, 6].iter().copied().flat_map(|v| [v; 3]));

    let collector = SampleCollector {
        samples: values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| item(*value, ordinal))
            .collect(),
        null_count: 0,
        count: values.len() as i64,
        ndv: 6,
        total_size: values.len() as i64 * 8,
    };

    // Defaults set to 4/4 like Go stores into vardef before the calls.
    let defaults = BuildOptions {
        default_num_buckets: 4,
        default_num_topn: 4,
        ..BuildOptions::default()
    };

    // numBuckets=4, numTopN=4: both match the default, pruning is allowed.
    let pruned = build_hist_and_topn(
        1,
        &collector,
        BuildOptions {
            num_buckets: 4,
            num_topn: 4,
            ..defaults.clone()
        },
        true,
    );
    assert_eq!(
        pruned.topn.as_ref().expect("TopN").num(),
        1,
        "pruning keeps only the entry that beats the out-of-list estimate"
    );
    assert_eq!(pruned.histogram.buckets.len(), 2);

    // numBuckets=4, numTopN=5: explicit user TopN size must be honored.
    let explicit = build_hist_and_topn(
        1,
        &collector,
        BuildOptions {
            num_buckets: 4,
            num_topn: 5,
            ..defaults.clone()
        },
        true,
    );
    assert_eq!(
        explicit.topn.as_ref().expect("TopN").num(),
        5,
        "an explicit TopN size is never pruned"
    );
    assert!(!explicit.histogram.buckets.is_empty());

    // numBuckets=5, numTopN=4: pruning again applies, more buckets remain.
    let wider = build_hist_and_topn(
        1,
        &collector,
        BuildOptions {
            num_buckets: 5,
            num_topn: 4,
            ..defaults
        },
        true,
    );
    assert_eq!(wider.topn.as_ref().expect("TopN").num(), 1);
    assert!(wider.histogram.buckets.len() > pruned.histogram.buckets.len());
}
