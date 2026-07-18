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

//! Dependency-closed vectors for PhysicalShuffle metadata.
//!
//! The Go anchor is `TestTiFlashFineGrainedShuffle` at
//! `pkg/planner/core/casetest/integration_test.go:245`.

use tidb_planner::physical_shuffle::{PartitionSplitterType, PhysicalShufflePlan};

#[test]
fn tiflash_shuffle_explain_preserves_concurrency_and_data_sources() {
    let plan = PhysicalShufflePlan::init(5, 3, ["TableReader"]);
    assert_eq!(plan.plan_type(), "Shuffle");
    assert_eq!(plan.query_block_offset(), 3);
    assert_eq!(
        plan.explain_info(),
        "execution info: concurrency:5, data sources:[TableReader]"
    );
}

#[test]
fn empty_data_source_list_is_source_shaped() {
    assert_eq!(
        PhysicalShufflePlan::init(1, 0, std::iter::empty::<String>()).explain_info(),
        "execution info: concurrency:1, data sources:[]"
    );
}

#[test]
fn range_splitter_keeps_source_discriminant() {
    let plan = PhysicalShufflePlan::init(2, 0, ["DataSource"])
        .with_splitter_type(PartitionSplitterType::RANGE);
    assert_eq!(plan.splitter_type(), PartitionSplitterType::RANGE);
}
