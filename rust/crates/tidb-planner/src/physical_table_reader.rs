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

//! Dependency-closed metadata for the physical table-reader operator.
//!
//! The Go `PhysicalTableReader` owns a physical child tree, flattened table
//! plans, schema/context and statistics, partition metadata, dynamic access,
//! cost, index resolution, task attachment, and protobuf/runtime wiring. This
//! leaf ports the stable reader identity, request-type naming, clone field
//! behavior, table-scan cardinality error, explain/normalized strings, and
//! monotonic metadata accounting. Storage and planner graph operations remain
//! external boundaries.

/// The source plan-codec type assigned by `PhysicalTableReader.Init`.
pub const PLAN_TYPE: &str = "TableReader";

/// Opaque storage kind copied by the source Clone implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreType {
    /// TiKV storage.
    TiKv,
    /// TiFlash storage.
    TiFlash,
    /// Unknown storage values remain opaque to this leaf.
    Unknown(u8),
}

/// Source table-reader request modes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadReqType {
    /// Read through a regular coprocessor request.
    Cop,
    /// TiFlash batch-coprocessor request.
    BatchCop,
    /// TiFlash MPP request.
    Mpp,
    /// Unknown values use the source default (`cop`) name.
    Unknown(u8),
}

impl ReadReqType {
    /// Returns the source request-type name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::BatchCop => "batchCop",
            Self::Mpp => "mpp",
            Self::Cop | Self::Unknown(_) => "cop",
        }
    }
}

/// Error returned when a reader does not contain exactly one table scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableScanCountError {
    actual: usize,
}

impl TableScanCountError {
    /// Returns the observed number of table scans.
    #[must_use]
    pub const fn actual(self) -> usize {
        self.actual
    }
}

impl std::fmt::Display for TableScanCountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the count of table scan != 1")
    }
}

impl std::error::Error for TableScanCountError {}

/// Minimal initialized physical TableReader metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTableReaderPlan {
    query_block_offset: i32,
    table_plan_explain: Option<String>,
    table_plans_len: usize,
    table_scan_count: usize,
    table_scan_partition_info_count: usize,
    read_req_type: ReadReqType,
    store_type: StoreType,
    mpp_version: u32,
    is_common_handle: bool,
    table_plan_identity: Option<u64>,
    flattened_first_identity: Option<u64>,
}

impl PhysicalTableReaderPlan {
    /// Initializes source-shaped TableReader metadata.
    #[must_use]
    pub fn init(table_plan_explain: Option<impl Into<String>>, query_block_offset: i32) -> Self {
        let table_plan_explain = table_plan_explain.map(Into::into);
        let table_plans_len = usize::from(table_plan_explain.is_some());
        let table_scan_count = table_plans_len;
        let table_plan_identity = table_plan_explain.as_ref().map(|_| 0);
        Self {
            query_block_offset,
            table_plan_explain,
            table_plans_len,
            table_scan_count,
            table_scan_partition_info_count: 0,
            read_req_type: ReadReqType::Cop,
            store_type: StoreType::TiKv,
            mpp_version: 0,
            is_common_handle: false,
            table_plan_identity,
            flattened_first_identity: table_plan_identity,
        }
    }

    /// Supplies source flattened TablePlans and table-scan cardinality.
    #[must_use]
    pub const fn with_table_shape(
        mut self,
        table_plans_len: usize,
        table_scan_count: usize,
    ) -> Self {
        self.table_plans_len = table_plans_len;
        self.table_scan_count = table_scan_count;
        self
    }

    /// Supplies source request type and MPP version metadata.
    #[must_use]
    pub const fn with_read_req_type(
        mut self,
        read_req_type: ReadReqType,
        mpp_version: u32,
    ) -> Self {
        self.read_req_type = read_req_type;
        self.mpp_version = mpp_version;
        self
    }

    /// Supplies the source storage kind independently from request mode.
    #[must_use]
    pub const fn with_store_type(mut self, store_type: StoreType) -> Self {
        self.store_type = store_type;
        self
    }

    /// Supplies a stable opaque child identity for flattening checks.
    #[must_use]
    pub const fn with_table_plan_identity(mut self, identity: u64) -> Self {
        self.table_plan_identity = Some(identity);
        self.flattened_first_identity = Some(identity);
        self
    }

    /// Supplies source common-handle metadata.
    #[must_use]
    pub const fn with_common_handle(mut self, is_common_handle: bool) -> Self {
        self.is_common_handle = is_common_handle;
        self
    }

    /// Supplies opaque partition-info entries used by memory/clone contracts.
    #[must_use]
    pub const fn with_table_scan_partition_info_count(mut self, count: usize) -> Self {
        self.table_scan_partition_info_count = count;
        self
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to `Init`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the opaque child-plan ExplainID text.
    #[must_use]
    pub fn table_plan_explain(&self) -> Option<&str> {
        self.table_plan_explain.as_deref()
    }

    /// Returns the flattened TablePlans count.
    #[must_use]
    pub const fn table_plans_len(&self) -> usize {
        self.table_plans_len
    }

    /// Reports whether the first flattened child is the reader's TablePlan.
    #[must_use]
    pub const fn table_plan_is_first_flattened(&self) -> bool {
        match (
            self.table_plans_len,
            self.table_plan_identity,
            self.flattened_first_identity,
        ) {
            (1, Some(table_plan), Some(first_flattened)) => table_plan == first_flattened,
            _ => false,
        }
    }

    /// Returns the source read request mode.
    #[must_use]
    pub const fn read_req_type(&self) -> ReadReqType {
        self.read_req_type
    }

    /// Returns the source request mode's stable name.
    #[must_use]
    pub const fn read_req_name(&self) -> &'static str {
        self.read_req_type.name()
    }

    /// Returns the source storage kind copied by Clone.
    #[must_use]
    pub const fn store_type(&self) -> StoreType {
        self.store_type
    }

    /// Returns opaque table-scan entries represented by the child metadata.
    #[must_use]
    pub fn get_table_scans(&self) -> Vec<&str> {
        vec![self.table_plan_explain.as_deref().unwrap_or_default(); self.table_scan_count]
    }

    /// Returns the source table-scan count.
    #[must_use]
    pub const fn table_scan_count(&self) -> usize {
        self.table_scan_count
    }

    /// Returns the source common-handle flag.
    #[must_use]
    pub const fn is_common_handle(&self) -> bool {
        self.is_common_handle
    }

    /// Returns the single table scan or the source cardinality error.
    pub fn get_table_scan(&self) -> Result<(), TableScanCountError> {
        if self.table_scan_count == 1 {
            Ok(())
        } else {
            Err(TableScanCountError {
                actual: self.table_scan_count,
            })
        }
    }

    /// Clones source metadata and rebuilds flattened plans from the child.
    ///
    /// `TableScanAndPartitionInfos` is intentionally omitted by the Go Clone
    /// implementation, so the corresponding count is reset to zero here.
    #[must_use]
    pub fn clone_plan(&self) -> Self {
        let mut cloned = self.clone();
        // FlattenListPushDownPlan is rebuilt from the cloned TablePlan. The
        // derived flattened length and table-scan count therefore remain the
        // same for both one-node and multi-node child trees.
        cloned.flattened_first_identity = cloned.table_plan_identity;
        cloned.table_scan_partition_info_count = 0;
        cloned
    }

    /// Returns source ExplainInfo for this reader.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let table_plan_info = format!(
            "data:{}",
            self.table_plan_explain.as_deref().unwrap_or_default()
        );
        if self.read_req_type == ReadReqType::Mpp {
            format!("MppVersion: {}, {}", self.mpp_version, table_plan_info)
        } else {
            table_plan_info
        }
    }

    /// Returns source normalized ExplainInfo, which is always empty.
    #[must_use]
    pub const fn explain_normalized_info(&self) -> &'static str {
        ""
    }

    /// Returns source OperatorInfo.
    #[must_use]
    pub fn operator_info(&self) -> String {
        format!(
            "data:{}",
            self.table_plan_explain.as_deref().unwrap_or_default()
        )
    }

    /// Returns a monotonic metadata-size estimate matching source growth.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        let child_bytes = self.table_plan_explain.as_ref().map_or(0, String::len);
        3 + child_bytes + self.table_scan_partition_info_count
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalTableReaderPlan, ReadReqType, StoreType, PLAN_TYPE};

    #[test]
    fn request_type_names_match_source_defaults() {
        assert_eq!(ReadReqType::Cop.name(), "cop");
        assert_eq!(ReadReqType::BatchCop.name(), "batchCop");
        assert_eq!(ReadReqType::Mpp.name(), "mpp");
        assert_eq!(ReadReqType::Unknown(9).name(), "cop");
        assert_eq!(StoreType::Unknown(9), StoreType::Unknown(9));
    }

    #[test]
    fn init_preserves_reader_identity_offset_and_child_shape() {
        let plan = PhysicalTableReaderPlan::init(Some("TableFullScan"), -2);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "TableReader");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.table_plan_explain(), Some("TableFullScan"));
        assert_eq!(plan.table_plans_len(), 1);
        assert!(plan.table_plan_is_first_flattened());
        assert_eq!(plan.read_req_type(), ReadReqType::Cop);
        assert_eq!(plan.store_type(), StoreType::TiKv);
    }

    #[test]
    fn explain_info_and_normalized_info_match_source_branches() {
        let cop = PhysicalTableReaderPlan::init(Some("Selection"), 0);
        assert_eq!(cop.explain_info(), "data:Selection");
        assert_eq!(cop.operator_info(), "data:Selection");
        assert_eq!(cop.explain_normalized_info(), "");

        let mpp = PhysicalTableReaderPlan::init(Some("ExchangeSender"), 0)
            .with_read_req_type(ReadReqType::Mpp, 3);
        assert_eq!(mpp.explain_info(), "MppVersion: 3, data:ExchangeSender");
        assert_eq!(mpp.read_req_name(), "mpp");
    }

    #[test]
    fn table_scan_cardinality_error_matches_source_contract() {
        assert_eq!(
            PhysicalTableReaderPlan::init(None::<String>, 0)
                .get_table_scan()
                .unwrap_err()
                .to_string(),
            "the count of table scan != 1"
        );
        let plan = PhysicalTableReaderPlan::init(Some("scan"), 0).with_table_shape(2, 2);
        assert_eq!(plan.get_table_scans().len(), 2);
        assert_eq!(plan.table_scan_count(), 2);
        assert_eq!(plan.get_table_scan().unwrap_err().actual(), 2);
        assert!(PhysicalTableReaderPlan::init(Some("scan"), 0)
            .get_table_scan()
            .is_ok());
    }

    #[test]
    fn clone_preserves_reader_fields_but_rebuilds_partition_info() {
        let plan = PhysicalTableReaderPlan::init(Some("TableFullScan"), 0)
            .with_read_req_type(ReadReqType::BatchCop, 0)
            .with_store_type(StoreType::TiFlash)
            .with_table_plan_identity(42)
            .with_table_shape(3, 2)
            .with_common_handle(true)
            .with_table_scan_partition_info_count(2);
        let cloned = plan.clone_plan();
        assert_eq!(cloned.table_plan_explain(), plan.table_plan_explain());
        assert_eq!(cloned.table_plans_len(), 3);
        assert_eq!(cloned.table_scan_count(), 2);
        assert!(!cloned.table_plan_is_first_flattened());
        assert_eq!(cloned.read_req_type(), ReadReqType::BatchCop);
        assert_eq!(cloned.store_type(), StoreType::TiFlash);
        assert!(cloned.is_common_handle());
        assert_eq!(cloned.memory_usage(), plan.memory_usage() - 2);
    }

    #[test]
    fn memory_usage_grows_with_child_and_partition_metadata() {
        let empty = PhysicalTableReaderPlan::init(None::<String>, 0);
        let child = PhysicalTableReaderPlan::init(Some("TableFullScan"), 0);
        let partitioned = child.clone().with_table_scan_partition_info_count(2);
        assert!(child.memory_usage() > empty.memory_usage());
        assert!(partitioned.memory_usage() > child.memory_usage());
    }
}
