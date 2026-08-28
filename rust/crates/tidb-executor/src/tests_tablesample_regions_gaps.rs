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

//! Gap tests for Go `pkg/executor/sample_test.go` (items 560-564). All five
//! run `TABLESAMPLE regions ()`, whose executor
//! (`TableSampleExecutor.Open/Next`, pkg/executor/sample.go:51/:58, fed by
//! `tableRegionSampler.initRanges/pickRanges` :142/:154) reads REGION
//! BOUNDARIES out of the store and samples one row per region. This tier's
//! catalog has no regions, and the parser/executor pair does not accept
//! TABLESAMPLE yet.

/// Go `pkg/executor/sample_test.go:43::TestTableSampleBasic`: `select ...
/// tablesample regions()` on an empty table answers nothing; after three
/// inserts one row answers; added columns (with defaults, then a generated
/// column) and `_tidb_rowid` project through the sample; `MustHavePlan`
/// pins the `TableSample` operator; an AUTO_RANDOM pre-split table with
/// 1000 empty inserts answers `count(*) = 8` (one row per pre-split
/// region); a 100-region split table answers `limit 2` deterministically
/// (`a`, `b`). Needs region metadata + the region sampler.
#[test]
#[ignore = "go-parity-gap: TABLESAMPLE regions() execution (TableSampleExecutor, pkg/executor/sample.go:51, tableRegionSampler :94) needs store region boundaries this tier does not model"]
fn table_sample_regions_answers_one_row_per_region() {}

/// Go `pkg/executor/sample_test.go:74::TestTableSampleMultiRegions`: a
/// `shard_row_id_bits = 2 pre_split_regions = 2` table yields exactly 4
/// sampled rows, and the cross join of two such tables' samples yields 16;
/// the min sampled `a` is `0` and `where a = 0` filters the sample. Needs
/// pre-split region metadata.
#[test]
#[ignore = "go-parity-gap: TABLESAMPLE over pre-split regions (pkg/executor/sample.go:94 newTableRegionSampler) is unported; shard_row_id_bits/pre_split_regions DDL is also not modeled"]
fn table_sample_cross_joins_two_region_samples() {}

/// Go `pkg/executor/sample_test.go:96::TestTableSampleNoSplitTable`: with
/// region splitting disabled (`ddl.EnableSplitTableRegion = 0`), an empty
/// single-region table samples NOTHING while a table with one row samples
/// it — the sampler answers one row per NON-EMPTY region.
#[test]
#[ignore = "go-parity-gap: region-aware sampling (pkg/executor/sample.go:154 pickRanges) including the no-region table case is unported"]
fn table_sample_skips_empty_single_region_tables() {}

/// Go `pkg/executor/sample_test.go:111::TestTableSamplePlan`: `explain
/// analyze select a from t tablesample regions()` prints exactly two
/// operators whose second row matches `.*TableSample.*`. Needs explain
/// analyze plus the operator.
#[test]
#[ignore = "go-parity-gap: explain-analyze of TableSample (operator unported; explain-analyze counters also unported) cannot render the two-row plan"]
fn table_sample_appears_in_the_explain_analyze_plan() {}

/// Go `pkg/executor/sample_test.go:124::TestMaxChunkSize`: with session
/// `MaxChunkSize = 1` the 4-region sample still answers 4 rows — the
/// sampler's output is chunk-size independent (Go builds one chunk per
/// region regardless of `MaxChunkSize`). Needs the session chunk-size knob
/// and the sampler.
#[test]
#[ignore = "go-parity-gap: MaxChunkSize interaction with TableSampleExecutor chunk writes (pkg/executor/sample.go:119 writeChunk) is unported"]
fn table_sample_output_is_chunk_size_independent() {}
