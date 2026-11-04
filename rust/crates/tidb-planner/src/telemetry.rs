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

//! Go `pkg/planner/core/telemetry.go`.

use tidb_kv::store_type::StoreType;
use tidb_planner_core_operator_physicalop::PhysicalPlan;

/// Go `IsTiFlashContained`: whether the plan reads TiFlash, and whether that
/// read is an MPP exchange. The first table reader found decides.
///
/// Go unwraps an `Explain` to its target first; the Rust plan an EXPLAIN
/// prepares is already the target.
#[must_use]
pub fn is_tiflash_contained(plan: &PhysicalPlan) -> (bool, bool) {
    fn tiflash_process(plan: &PhysicalPlan, push_down: &mut bool, exchange_push_down: &mut bool) {
        if let PhysicalPlan::TableReader(table_reader) = plan {
            *push_down = table_reader.store_type == StoreType::TiFlash;
            // `plancodec.TypeExchangeSender`.
            if *push_down
                && table_reader
                    .table_plan
                    .as_deref()
                    .is_some_and(|table_plan| table_plan.tp() == "ExchangeSender")
            {
                *exchange_push_down = true;
            }
            return;
        }
        for child in plan.children() {
            tiflash_process(child, push_down, exchange_push_down);
            if *push_down {
                return;
            }
        }
    }
    let (mut push_down, mut exchange_push_down) = (false, false);
    tiflash_process(plan, &mut push_down, &mut exchange_push_down);
    (push_down, exchange_push_down)
}
