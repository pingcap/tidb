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

use tidb_stats::{
    AnalyzeHistogramLifecycle, AnalyzeResult, AnalyzeResults, AnalyzeTableId,
    NON_PARTITION_TABLE_ID,
};

#[derive(Debug, Default)]
struct HistogramProbe {
    destroyed: usize,
}

impl AnalyzeHistogramLifecycle for HistogramProbe {
    fn destroy_and_put_to_pool(&mut self) {
        self.destroyed += 1;
    }
}

type ResultProbe = AnalyzeResult<HistogramProbe, u8, u16, u32>;

#[test]
fn source_result_releases_fm_and_destroys_every_histogram_only() {
    let mut result = ResultProbe {
        histograms: vec![
            Some(HistogramProbe::default()),
            Some(HistogramProbe::default()),
        ],
        cmsketches: vec![Some(1)],
        top_ns: vec![Some(2)],
        fm_sketches: vec![Some(3), None],
        is_index: 1,
    };
    result.destroy_and_put_to_pool();
    assert!(result.fm_sketches.is_empty());
    assert_eq!(result.histograms[0].as_ref().unwrap().destroyed, 1);
    assert_eq!(result.histograms[1].as_ref().unwrap().destroyed, 1);
    assert_eq!(result.cmsketches, [Some(1)]);
    assert_eq!(result.top_ns, [Some(2)]);
    assert_eq!(result.is_index, 1);
}

#[test]
fn source_outer_result_destroys_every_inner_result_and_keeps_metadata() {
    let inner = || ResultProbe {
        histograms: vec![Some(HistogramProbe::default())],
        fm_sketches: vec![Some(4)],
        ..ResultProbe::default()
    };
    let mut results = AnalyzeResults {
        error: Some("measured".to_owned()),
        job: None,
        results: vec![inner(), inner()],
        table_id: AnalyzeTableId::new(42, NON_PARTITION_TABLE_ID),
        count: 10,
        stats_version: 2,
        snapshot: 99,
        base_count: 8,
        base_modify_count: 3,
        for_mv_index_or_global_index: true,
    };
    results.destroy_and_put_to_pool();
    assert!(results
        .results
        .iter()
        .all(|result| result.fm_sketches.is_empty()));
    assert!(results
        .results
        .iter()
        .all(|result| { result.histograms[0].as_ref().unwrap().destroyed == 1 }));
    assert_eq!(results.count, 10);
    assert_eq!(results.snapshot, 99);
    assert!(results.for_mv_index_or_global_index);
}

#[test]
#[should_panic(expected = "analyze result contains a nil histogram")]
fn source_nil_histogram_keeps_go_dereference_failure() {
    let mut result = ResultProbe {
        histograms: vec![None],
        fm_sketches: vec![Some(1)],
        ..ResultProbe::default()
    };
    result.destroy_and_put_to_pool();
}
