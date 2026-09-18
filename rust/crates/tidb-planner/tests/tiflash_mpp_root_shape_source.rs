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

//! The single-fragment TiFlash MPP root shape: Go's
//! `GenerateRootMPPTasks` (`pkg/planner/core/operator/physicalop/fragment.go:167`)
//! wraps the chosen fragment in a PassThrough `PhysicalExchangeSender`, and
//! `adjustReadReqType` (`physical_table_reader.go:299`) marks the reader MPP.
//! EXPLAIN renders `MppVersion: <newest>, data:ExchangeSender_<id>` on the
//! reader (`physical_table_reader.go:258`, newest answers
//! `kv.GetNewestMppVersion` = MppVersionV3, `pkg/kv/mpp.go:47`) and
//! `ExchangeType: PassThrough` on the sender
//! (`physical_exchange_sender.go:107`).

use tidb_planner::physical::{
    BasePhysicalPlan, ExchangeType, PhysicalExchangeSender, PhysicalPlan, PhysicalTableReader,
    PhysicalTableScan,
};
use tidb_planner::physical_table_reader::{ReadReqType, StoreType};

fn mpp_reader_over_sender(sender_id: i32) -> PhysicalTableReader {
    let scan_base = BasePhysicalPlan::with_id(8, "TableScan", 0);
    let scan = PhysicalPlan::TableScan(PhysicalTableScan {
        base: scan_base,
        store_type: StoreType::TiFlash,
        ..PhysicalTableScan::default()
    });
    let mut sender_base = BasePhysicalPlan::with_id(sender_id, "ExchangeSender", 0);
    sender_base.set_children(vec![scan]);
    PhysicalTableReader {
        base: BasePhysicalPlan::with_id(5, "TableReader", 0),
        table_plan: Some(Box::new(PhysicalPlan::ExchangeSender(
            PhysicalExchangeSender {
                base: sender_base,
                exchange_type: ExchangeType::PassThrough,
                hash_cols: Vec::new(),
            },
        ))),
        store_type: StoreType::TiFlash,
        is_common_handle: false,
        read_req_type: ReadReqType::Mpp,
    }
}

#[test]
fn mpp_reader_explain_carries_the_newest_mpp_version_and_the_sender() {
    let reader = mpp_reader_over_sender(9);
    assert_eq!(
        reader.explain_info(false),
        "MppVersion: 3, data:ExchangeSender_9"
    );
    assert_eq!(
        reader.explain_info(true),
        "MppVersion: 3, data:ExchangeSender"
    );
}

#[test]
fn passthrough_sender_explain_matches_go() {
    let reader = mpp_reader_over_sender(9);
    let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
        panic!("the sender hangs off TablePlan");
    };
    assert_eq!(sender.explain_info(false), "ExchangeType: PassThrough");
    assert_eq!(sender.explain_info(true), "ExchangeType: PassThrough");
}

#[test]
fn cop_reader_explain_stays_plain() {
    let mut reader = mpp_reader_over_sender(9);
    reader.read_req_type = ReadReqType::Cop;
    assert_eq!(reader.explain_info(false), "data:ExchangeSender_9");
}

#[test]
fn hash_sender_explain_lists_hash_columns_like_go() {
    // Go `physical_exchange_sender.go:107`: `HashPartition` prints
    // `, Hash Cols: ` plus `ExplainColumnList`.
    let mut sender = PhysicalExchangeSender::default();
    sender.exchange_type = ExchangeType::Hash;
    sender.hash_cols = vec![tidb_planner::physical_property::MppPartitionColumn::new(
        7, -1,
    )];
    let rendered = sender.explain_info(false);
    assert!(rendered.starts_with("ExchangeType: HashPartition, Hash Cols: "));
}
