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

//! Analyze-result ownership and destruction from `pkg/statistics/analyze.go`.

use crate::{AnalyzeJob, AnalyzeTableId};

/// Histogram-side lifecycle invoked by Go `DestroyAndPutToPool`.
pub trait AnalyzeHistogramLifecycle {
    fn destroy_and_put_to_pool(&mut self);
}

/// One column or index result group.
#[derive(Debug)]
pub struct AnalyzeResult<H, C, T, F> {
    pub histograms: Vec<Option<H>>,
    pub cmsketches: Vec<Option<C>>,
    pub top_ns: Vec<Option<T>>,
    pub fm_sketches: Vec<Option<F>>,
    pub is_index: i32,
}

impl<H, C, T, F> Default for AnalyzeResult<H, C, T, F> {
    fn default() -> Self {
        Self {
            histograms: Vec::new(),
            cmsketches: Vec::new(),
            top_ns: Vec::new(),
            fm_sketches: Vec::new(),
            is_index: 0,
        }
    }
}

impl<H: AnalyzeHistogramLifecycle, C, T, F> AnalyzeResult<H, C, T, F> {
    /// Go releases every FM sketch reference, then destroys every histogram.
    /// CMSketch and TopN ownership is intentionally unchanged.
    pub fn destroy_and_put_to_pool(&mut self) {
        self.fm_sketches.clear();
        for histogram in &mut self.histograms {
            histogram
                .as_mut()
                .expect("analyze result contains a nil histogram")
                .destroy_and_put_to_pool();
        }
    }
}

/// Complete results returned by one analyze task.
#[derive(Debug)]
pub struct AnalyzeResults<H, C, T, F> {
    pub error: Option<String>,
    pub job: Option<AnalyzeJob>,
    pub results: Vec<Option<AnalyzeResult<H, C, T, F>>>,
    pub table_id: AnalyzeTableId,
    pub count: i64,
    pub stats_version: i32,
    pub snapshot: u64,
    pub base_count: i64,
    pub base_modify_count: i64,
    pub for_mv_index_or_global_index: bool,
}

impl<H: AnalyzeHistogramLifecycle, C, T, F> AnalyzeResults<H, C, T, F> {
    /// Go delegates destruction to every contained result and otherwise keeps
    /// the outer result metadata intact.
    pub fn destroy_and_put_to_pool(&mut self) {
        for result in &mut self.results {
            result
                .as_mut()
                .expect("analyze results contains a nil result")
                .destroy_and_put_to_pool();
        }
    }
}
