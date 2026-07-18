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

//! Dependency-closed vectors for PhysicalExchangeSender metadata.
//!
//! The Go anchor is `TestMPPExchangeSender` at
//! `pkg/planner/core/casetest/mpp/mpp_test.go:78`.

use tidb_planner::physical_exchange_sender::{
    CompressionMode, ExchangeType, PhysicalExchangeSenderPlan,
};

#[test]
fn mpp_sender_pass_through_matches_source_plan_tree() {
    let plan = PhysicalExchangeSenderPlan::init(
        ExchangeType::PASS_THROUGH,
        CompressionMode::NONE,
        "",
        [],
        0,
    );
    assert_eq!(plan.plan_type(), "ExchangeSender");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "ExchangeType: PassThrough");
}

#[test]
fn sender_explain_preserves_hash_metadata_and_task_order() {
    let plan = PhysicalExchangeSenderPlan::init(
        ExchangeType::HASH,
        CompressionMode::HIGH_COMPRESSION,
        "[name: a, collate: binary]",
        [11, 2],
        4,
    );
    assert_eq!(
        plan.explain_info(),
        "ExchangeType: HashPartition, Compression: HIGH_COMPRESSION, Hash Cols: [name: a, collate: binary], tasks: [11, 2], stream_count: 4"
    );
}

#[test]
fn broadcast_without_optional_metadata_has_compact_explain() {
    let plan =
        PhysicalExchangeSenderPlan::init(ExchangeType::BROADCAST, CompressionMode::NONE, "", [], 0);
    assert_eq!(plan.explain_info(), "ExchangeType: Broadcast");
}
