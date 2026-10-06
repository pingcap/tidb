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

//! Dependency-closed tests for physical-property classifications.
//!
//! The Go plan-output anchor is `TestJSONPlanInExplain` at
//! `pkg/planner/core/casetest/plan_test.go:398`; these vectors isolate the
//! partition and match-result contracts from plan/protobuf construction.

use tidb_planner::physical_property::{ExchangeKind, MppPartitionType, PhysicalPropMatchResult};

#[test]
fn partition_types_preserve_source_exchange_mapping() {
    let cases = [
        (MppPartitionType::Any, 0, ExchangeKind::PassThrough),
        (MppPartitionType::Broadcast, 1, ExchangeKind::Broadcast),
        (MppPartitionType::Hash, 2, ExchangeKind::Hash),
        (
            MppPartitionType::SinglePartition,
            3,
            ExchangeKind::PassThrough,
        ),
    ];
    for (partition, raw, exchange) in cases {
        assert_eq!(MppPartitionType::from_raw(raw), partition);
        assert_eq!(partition.raw(), raw);
        assert_eq!(partition.exchange_kind(), exchange);
    }
    assert_eq!(
        MppPartitionType::from_raw(99).exchange_kind(),
        ExchangeKind::PassThrough
    );
}

#[test]
fn match_result_only_matched_variants_satisfy_property() {
    assert!(!PhysicalPropMatchResult::NotMatched.matched());
    assert!(PhysicalPropMatchResult::Matched.matched());
    assert!(PhysicalPropMatchResult::MatchedNeedMergeSort.matched());
}
