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

//! Dependency-closed vectors for PhysicalExchangeReceiver metadata.
//!
//! The Go anchor is `TestTiFlashFineGrainedShuffleWithMaxTiFlashThreads` at
//! `pkg/planner/core/integration_test.go:904`.

use tidb_planner::physical_exchange_receiver::PhysicalExchangeReceiverPlan;

#[test]
fn configured_tiflash_threads_are_visible_in_explain() {
    let plan = PhysicalExchangeReceiverPlan::init(10);
    assert_eq!(plan.plan_type(), "ExchangeReceiver");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "stream_count: 10");
}

#[test]
fn disabled_fine_grained_shuffle_keeps_explain_empty() {
    assert_eq!(PhysicalExchangeReceiverPlan::init(0).explain_info(), "");
}

#[test]
fn stream_count_boundary_is_lossless() {
    let plan = PhysicalExchangeReceiverPlan::init(u64::MAX);
    assert_eq!(plan.stream_count(), u64::MAX);
    assert_eq!(plan.explain_info(), format!("stream_count: {}", u64::MAX));
}
