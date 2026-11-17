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

//! Go `br/pkg/restore/snap_client`: the snapshot restore client: the SST
//! import path (`import.go`, `tikv_sender.go`), the online placement
//! rules, the schema side of `SnapClient` (`client.go`), the steps after a
//! table's data is in (`pipeline_items.go`) and the logical system table
//! restore (`systable_restore.go`).

pub mod client;
pub mod import;
pub mod pipeline_items;
pub mod placement_rule_manager;
pub mod systable_restore;
pub mod tikv_sender;

pub use client::SnapClient;
pub use import::{KvMode, RewriteMode, SnapFileImporter, SnapFileImporterOptions};
pub use tikv_sender::{
    sort_and_validate_file_ranges, PhysicalTable, RestoreTablesContext,
    MERGED_RANGE_COUNT_THRESHOLD,
};

#[cfg(test)]
mod tests;
