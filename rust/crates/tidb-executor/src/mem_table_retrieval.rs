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

//! The per-read hand-off between a memory-table scan and its retriever.
//!
//! Go's `MemTableReaderExec` holds a `memTableRetriever` built from its
//! `PhysicalMemTable` (table, pruned columns, `Extractor`, `QueryTimeRange`)
//! and calls `retrieve(ctx, sctx)` when it is read, with the session as
//! `sctx`. The retrievers here need the owning `Session`, which an executor
//! cannot borrow, so the scan states what it needs (a [`MemTableRead`]) and
//! the session answers it: [`MemTableRetrieval`] is the statement's pending
//! requests and the rows already retrieved for it, keyed by plan node id.
//! A scan whose rows are missing makes the build fail with
//! `DriverError::MemTableRetrievalNeeded`; the session retrieves the
//! requested rows and rebuilds. No syntactic walk of the statement decides
//! which tables are read: the plan does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use tidb_datatype::Datum;
use tidb_planner::logical::mem_table::QueryTimeRange;
use tidb_planner::memtable_predicate_extractor::MemTablePredicateExtractor;

/// What one memory-table scan hands its retriever: Go's `PhysicalMemTable`
/// fields a `memTableRetriever` is built from.
#[derive(Clone, Debug)]
pub struct MemTableRead {
    /// The scan's plan node id; one retrieval answers one node, as Go's
    /// `MemTableReaderExec` owns one retriever.
    pub plan_id: i32,
    /// Go `DBName.O`.
    pub db_name: String,
    /// Go `Table.Name.O`.
    pub table_name: String,
    /// Go `Columns`: the columns left after memory-table column pruning, by
    /// name. For a cluster table's TiDB-store scan, the scan's columns.
    pub columns: Vec<String>,
    /// The scan's column ids, which the planner numbers `offset + 1` for a
    /// cluster table's TiDB-store scan (empty for a `PhysicalMemTable`).
    pub column_ids: Vec<i64>,
    /// Go `Extractor`.
    pub extractor: Option<MemTablePredicateExtractor>,
    /// Go `QueryTimeRange`.
    pub query_time_range: QueryTimeRange,
    /// Whether the scan is a cluster table's TiDB-store `TableScan` rather
    /// than a `PhysicalMemTable`.
    pub is_cluster_scan: bool,
}

impl MemTableRead {
    fn is_table_with_storage_sizes(&self) -> bool {
        !self.is_cluster_scan
            && (self.table_name.eq_ignore_ascii_case("TABLES")
                || self.table_name.eq_ignore_ascii_case("PARTITIONS"))
    }

    /// Go `memtableRetriever.updateStatsCacheIfNeed`: whether the pruned
    /// columns of `TABLES` / `PARTITIONS` need the table-size statistics.
    #[must_use]
    pub fn needs_table_storage_statistics(&self) -> bool {
        self.is_table_with_storage_sizes()
            && self.columns.iter().any(|column| {
                matches!(
                    column.to_ascii_uppercase().as_str(),
                    "TABLE_ROWS" | "AVG_ROW_LENGTH" | "DATA_LENGTH" | "INDEX_LENGTH"
                )
            })
    }

    /// Go `buildTableSizeStats`'s `needColLength` decision after logical
    /// column pruning. `TABLE_ROWS` needs only `mysql.stats_meta`; the
    /// variable-width size columns additionally require the more expensive
    /// histogram read.
    #[must_use]
    pub fn needs_table_storage_column_lengths(&self) -> bool {
        self.is_table_with_storage_sizes()
            && self.columns.iter().any(|column| {
                matches!(
                    column.to_ascii_uppercase().as_str(),
                    "AVG_ROW_LENGTH" | "DATA_LENGTH" | "INDEX_LENGTH"
                )
            })
    }
}

/// Go's `memTableRetriever` for the tables whose rows depend on the
/// statement's schema and a snapshot of the session alone, so a scan reads
/// them itself: [`MemTableRetriever::retrieve`] runs at the scan's first
/// `Next`, over the catalog the statement was built with. The session
/// installs one ([`MemTableRetrieval::install`]) when a scan asks for rows it
/// serves, so a statement that reads no memory table pays for none.
pub trait MemTableRetriever: Send + Sync {
    /// Whether this retriever builds the rows of `read`'s table; any other
    /// scan is answered by the session ([`MemTableRetrieval`]).
    fn serves(&self, read: &MemTableRead) -> bool;

    /// The rows of the scan `read`, one value per column of the table's
    /// schema, from `catalog`, the statement's schema image.
    fn retrieve(
        &self,
        read: &MemTableRead,
        catalog: &crate::Catalog,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, crate::DriverError>;
}

/// One statement's memory-table retrievals; see the module documentation.
/// Clones of a statement context share it.
#[derive(Default)]
pub struct MemTableRetrieval {
    state: Mutex<RetrievalState>,
}

#[derive(Default)]
struct RetrievalState {
    rows: HashMap<String, Arc<Vec<Vec<Datum>>>>,
    pending: Vec<MemTableRead>,
    retrievers: Vec<Arc<dyn MemTableRetriever>>,
}

impl MemTableRead {
    /// What identifies one retrieval within a statement: the node and
    /// everything its retriever is built from. A statement that is planned
    /// again (an uncorrelated subquery's value changes the plan around it)
    /// numbers its nodes afresh, so the node id alone could name another scan.
    fn key(&self) -> String {
        format!(
            "{}|{}.{}|{:?}|{:?}|{:?}|{:?}|{}",
            self.plan_id,
            self.db_name,
            self.table_name,
            self.columns,
            self.column_ids,
            self.extractor,
            self.query_time_range,
            self.is_cluster_scan
        )
    }
}

impl MemTableRetrieval {
    /// The rows retrieved for the scan `read`.
    #[must_use]
    pub fn rows(&self, read: &MemTableRead) -> Option<Arc<Vec<Vec<Datum>>>> {
        self.lock().rows.get(&read.key()).cloned()
    }

    /// Records that the scan `read` needs its rows, once per scan.
    pub fn request(&self, read: MemTableRead) {
        let mut state = self.lock();
        let key = read.key();
        if state.rows.contains_key(&key) || state.pending.iter().any(|pending| pending.key() == key)
        {
            return;
        }
        state.pending.push(read);
    }

    /// Removes and returns the requests the session has yet to answer.
    #[must_use]
    pub fn take_pending(&self) -> Vec<MemTableRead> {
        std::mem::take(&mut self.lock().pending)
    }

    /// Answers the request of the scan `read` with `rows`, one value per
    /// column of the table's schema.
    pub fn supply(&self, read: &MemTableRead, rows: Vec<Vec<Datum>>) {
        self.lock().rows.insert(read.key(), Arc::new(rows));
    }

    /// Installs a retriever the scans it serves read their rows through.
    pub fn install(&self, retriever: Arc<dyn MemTableRetriever>) {
        self.lock().retrievers.push(retriever);
    }

    /// The installed retriever that serves the scan `read`.
    #[must_use]
    pub fn retriever_for(&self, read: &MemTableRead) -> Option<Arc<dyn MemTableRetriever>> {
        self.lock()
            .retrievers
            .iter()
            .find(|retriever| retriever.serves(read))
            .map(Arc::clone)
    }

    /// Whether a request is waiting.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.lock().pending.is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RetrievalState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
