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

//! The executor's side of a table: Go's reader executors, DDL backfill and
//! in-process DDL over a [`KvTable`], as extension traits; the table itself is
//! `tidb_table::kv_table`.

use tidb_table::kv_table::*;

#[allow(unused_imports)]
use tidb_hack::GoCaseCompare;
#[allow(unused_imports)]
use tidb_kv::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
#[allow(unused_imports)]
use tidb_autoid::AutoIdAllocator;
#[allow(unused_imports)]
use tidb_table::kv_table::row_decoder::fill_handle_columns;
#[allow(unused_imports)]
use std::collections::HashSet;
#[allow(unused_imports)]
use tidb_codec::table_key::{encode_row_key_with_handle, get_table_handle_key_range};
#[allow(unused_imports)]
use tidb_datatype::{new_collation_enabled, Datum, FieldType, SessionTimeZone};
#[allow(unused_imports)]
use tidb_expr::expression::Expression;
#[allow(unused_imports)]
use tidb_table::kv_table::index_entries::duplicate_value_text;
#[allow(unused_imports)]
use tidb_hack::GoToLower;
#[allow(unused_imports)]
use tidb_tablecodec::encode_table_row;
#[allow(unused_imports)]
use tidb_txnkv::{CommonHandle, Key};

mod ddl_ops;
pub use tidb_ddl_build::backfilling::ReorgBatch;
mod partition_maintenance;
mod table_scan;
pub use table_scan::RowDecodeContextExt;
pub(crate) use table_scan::insert_extra_handle;
pub use ddl_ops::KvTableDdl;
pub use tidb_ddl_build::table_backfill::KvTableBackfill;
pub use partition_maintenance::KvTablePartitionMaintenance;
pub use table_scan::KvTableReader;
pub use table_scan::{
    capture_decoded_column_ids, IndexRangeCursor, RemoteIndexHandleCursor, RemoteRowCursor,
    RowCursor, StagedHandlesLookup, TableScanExec,
};
pub(crate) use table_scan::{FinishedLookup, FinishedLookupChunk};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_nonpublic_columns;
#[cfg(test)]
mod tests_row_decoder;
