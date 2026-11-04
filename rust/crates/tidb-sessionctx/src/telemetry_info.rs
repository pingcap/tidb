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

//! Go `executor.TelemetryInfo` (`pkg/executor/adapter.go`): what one
//! statement's executor build observed, which `session.updateTelemetryMetric`
//! counts after `Exec`.
//!
//! Go hangs it on `ExecStmt.Ti` and hands the pointer to the executor
//! builder. The Rust builders reach a statement only through its
//! [`crate::StmtContext`], so the session's cell rides there, the way
//! `ReadFromTableCache` does.

/// Go `TelemetryInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TelemetryInfo {
    pub use_non_recursive: bool,
    pub use_recursive: bool,
    pub use_multi_schema_change: bool,
    pub use_exchange_partition: bool,
    pub use_flashback_to_cluster: bool,
    pub partition_telemetry: Option<PartitionTelemetryInfo>,
    pub account_lock_telemetry: Option<AccountLockTelemetryInfo>,
    pub use_index_merge: bool,
    pub use_table_lookup: bool,
    /// Go `plannercore.IsTiFlashContained(stmt.Plan)`, which
    /// `ExecuteStmt` computes from the statement's plan; recorded here as
    /// the plan is prepared, since the session holds no plan after `Exec`.
    pub tiflash_push_down: bool,
    /// The exchange half of `IsTiFlashContained`.
    pub tiflash_exchange_push_down: bool,
}

/// Go `PartitionTelemetryInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionTelemetryInfo {
    pub use_table_partition: bool,
    pub use_table_partition_list: bool,
    pub use_table_partition_range: bool,
    pub use_table_partition_hash: bool,
    pub use_table_partition_range_columns: bool,
    pub use_table_partition_range_columns_gt1: bool,
    pub use_table_partition_range_columns_gt2: bool,
    pub use_table_partition_range_columns_gt3: bool,
    pub use_table_partition_list_columns: bool,
    pub table_partition_max_partitions_num: u64,
    pub use_create_interval_partition: bool,
    pub use_add_interval_partition: bool,
    pub use_drop_interval_partition: bool,
    pub use_compact_table_partition: bool,
    pub use_reorganize_partition: bool,
}

/// Go `AccountLockTelemetryInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountLockTelemetryInfo {
    /// CREATE/ALTER USER statements that lock the user.
    pub lock_user: i64,
    /// CREATE/ALTER USER statements that unlock the user.
    pub unlock_user: i64,
    /// CREATE/ALTER USER statements.
    pub create_or_alter_user: i64,
}

impl TelemetryInfo {
    /// Go's `if b.Ti.PartitionTelemetry == nil { ... = &PartitionTelemetryInfo{} }`.
    pub fn partition_telemetry(&mut self) -> &mut PartitionTelemetryInfo {
        self.partition_telemetry.get_or_insert_with(PartitionTelemetryInfo::default)
    }

    /// Go's `if b.Ti.AccountLockTelemetry == nil { ... }`.
    pub fn account_lock_telemetry(&mut self) -> &mut AccountLockTelemetryInfo {
        self.account_lock_telemetry
            .get_or_insert_with(AccountLockTelemetryInfo::default)
    }
}
