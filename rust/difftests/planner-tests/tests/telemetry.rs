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

//! Dependency-closed vectors for TiFlash telemetry classification.
//!
//! The Go anchor is `TestMPPSharedCTEScan` at
//! `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568`.

use tidb_planner::telemetry::{is_tiflash_contained, PlanNode, StoreType, TablePlanKind};

#[test]
fn shared_cte_shape_reports_tiflash_exchange() {
    let plan = PlanNode::Physical(vec![PlanNode::TableReader {
        store: StoreType::TiFlash,
        table_plan: TablePlanKind::ExchangeSender,
    }]);
    assert_eq!(is_tiflash_contained(Some(&plan)), (true, true));
}

#[test]
fn non_tiflash_read_does_not_set_exchange_flag() {
    let plan = PlanNode::TableReader {
        store: StoreType::Other,
        table_plan: TablePlanKind::ExchangeSender,
    };
    assert_eq!(is_tiflash_contained(Some(&plan)), (false, false));
}
