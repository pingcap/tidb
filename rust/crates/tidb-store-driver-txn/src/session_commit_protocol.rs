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

//! Transaction activation options and the separate bootstrap storage defaults.

use crate::transaction::CommitProtocol;

use tidb_vardef::global_sysvar_initial::{
    global_system_variable_initial_value, GlobalSysvarEnvironment, ENABLE_1PC, ENABLE_ASYNC_COMMIT,
    ON,
};

/// Options copied once when a SQL transaction becomes active (Go
/// `SetOptionsOnTxnActive`). The native client owns their commit-time meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTransactionOptions {
    /// Session permissions for native commit-protocol eligibility checks.
    pub commit_protocol: CommitProtocol,
    /// Session assertion checking level, fixed for this transaction.
    pub assertion_level: tidb_proto::KvrpcAssertionLevel,
    /// The session's statement ctx values: Go `SetOptionsOnTxnActive`'s
    /// `kv.ResourceGroupTagger`, and the statement ctx (with its `RUDetails`)
    /// every later call of the transaction is passed. `None` for a
    /// transaction no SQL session owns.
    pub kv_context: Option<tidb_txnkv::StatementKvContextCell>,
    /// The session's temporary tables, whose keys Go's
    /// `temporaryTableKVFilter` keeps out of the commit. `None` for a
    /// transaction no SQL session owns.
    pub temporary_tables: Option<TemporaryTableIds>,
    /// Go `SessionVars.TxnEntrySizeLimit` (`tidb_txn_entry_size_limit`): above
    /// 0 it overrides the configured one-entry size limit for this
    /// transaction (`LazyTxn` sets `kv.SizeLimits`).
    pub entry_size_limit: u64,
}

/// The ids of a session's temporary tables, refreshed by the session as its
/// infoschema changes; read by the commit's filter when it runs. Equal when
/// they are the same session's set.
#[derive(Clone, Debug, Default)]
pub struct TemporaryTableIds(
    pub std::sync::Arc<std::sync::RwLock<std::collections::HashSet<i64>>>,
);

impl PartialEq for TemporaryTableIds {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TemporaryTableIds {}

impl SessionTransactionOptions {
    /// Applies the activation snapshot before the transaction is exposed.
    pub fn apply<S: crate::transaction::StoreClient>(
        self,
        transaction: &mut crate::transaction::RealTransaction<S>,
    ) {
        // Go `NewTiKVTxn`'s `txn.SetKVFilter(TiDBKVFilter{})`, which
        // `session.doCommit` replaces with `temporaryTableKVFilter` for a
        // transaction that may hold temporary rows.
        match self.temporary_tables {
            Some(tables) => transaction.set_kv_filter(std::sync::Arc::new(
                TemporaryTableKVFilter { tables },
            )),
            None => transaction.set_kv_filter(std::sync::Arc::new(TiDBKVFilter)),
        }
        transaction.set_commit_protocol(self.commit_protocol);
        transaction.set_assertion_level(self.assertion_level);
        if self.entry_size_limit > 0 {
            transaction.set_entry_size_limit(self.entry_size_limit);
        }
        if let Some(context) = self.kv_context {
            transaction.set_statement_kv_context(context);
        }
    }

    /// Binds the session's statement ctx values to the transaction.
    #[must_use]
    pub fn with_statement_kv_context(
        mut self,
        context: tidb_txnkv::StatementKvContextCell,
    ) -> Self {
        self.kv_context = Some(context);
        self
    }

    /// The session's temporary tables, for the commit's filter.
    #[must_use]
    pub fn with_temporary_tables(mut self, tables: TemporaryTableIds) -> Self {
        self.temporary_tables = Some(tables);
        self
    }
}

/// Go `txn.TiDBKVFilter` (`pkg/store/driver/txn/txn_driver.go`): the pairs
/// the commit leaves out. An untouched index pair -- an UPDATE inside a
/// transaction writes one again only for the transaction's own reads -- is
/// never committed, and one that also presumes its key absent is an error.
#[derive(Clone, Copy, Debug, Default)]
pub struct TiDBKVFilter;

impl tikv_client::KvFilter for TiDBKVFilter {
    fn is_unnecessary_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        flags: tikv_client::MutationFlags,
    ) -> tikv_client::Result<bool> {
        let untouched = tidb_tablecodec::table_key::is_untouched_index_kv(key, value);
        if untouched && flags.presumes_key_not_exists() {
            // Go `kv.Key.String`: lower-case hex.
            let hex = |bytes: &[u8]| {
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            return Err(tikv_client::Error::StringError(format!(
                "unexpected path the untouched key={} value={} contains PresumeKeyNotExists flag \
                 keyFlags={flags:?}",
                hex(key),
                hex(value),
            )));
        }
        Ok(untouched)
    }
}

/// Go `session.temporaryTableKVFilter`: a temporary table's pairs are never
/// committed; every other pair goes through [`TiDBKVFilter`].
#[derive(Clone, Debug)]
pub struct TemporaryTableKVFilter {
    tables: TemporaryTableIds,
}

impl tikv_client::KvFilter for TemporaryTableKVFilter {
    fn is_unnecessary_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        flags: tikv_client::MutationFlags,
    ) -> tikv_client::Result<bool> {
        let tid = tidb_tablecodec::table_key::decode_table_id(key);
        if self
            .tables
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&tid)
        {
            return Ok(true);
        }
        TiDBKVFilter.is_unnecessary_key_value(key, value, flags)
    }
}

// Low-level storage and bootstrap callers have no SQL session. Preserve their
// explicit protocol selection and the native client's assertion default.
impl From<CommitProtocol> for SessionTransactionOptions {
    fn from(commit_protocol: CommitProtocol) -> Self {
        Self {
            commit_protocol,
            assertion_level: tidb_proto::KvrpcAssertionLevel::Off,
            kv_context: None,
            temporary_tables: None,
            entry_size_limit: 0,
        }
    }
}

/// Bootstrap permissions for low-level TiKV storage operations without a SQL
/// session. SQL transaction activation must use its session variable snapshot.
#[must_use]
pub fn bootstrap_commit_protocol() -> CommitProtocol {
    let environment = GlobalSysvarEnvironment {
        store_is_tikv: true,
        in_test: false,
        next_gen: false,
    };
    CommitProtocol {
        async_commit: global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, "OFF", environment)
            == ON,
        one_pc: global_system_variable_initial_value(ENABLE_1PC, "OFF", environment) == ON,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `temporaryTableKVFilter`: a temporary table's pair is never
    /// committed, whatever its value; another table's pair is judged by
    /// `TiDBKVFilter`; a table added to the set later is filtered too.
    #[test]
    fn temporary_table_pairs_are_filtered_out_of_the_commit() {
        use tikv_client::KvFilter;
        let ids = TemporaryTableIds::default();
        ids.0.write().unwrap().insert(5);
        let filter = TemporaryTableKVFilter { tables: ids.clone() };
        let record = |id: i64| {
            tidb_tablecodec::table_key::encode_row_key_with_handle(
                id,
                &tidb_tablecodec::table_key::RecordHandle::Int(1),
            )
        };
        let flags = tikv_client::MutationFlags::default();
        assert!(filter.is_unnecessary_key_value(&record(5), b"row", flags).unwrap());
        assert!(!filter.is_unnecessary_key_value(&record(6), b"row", flags).unwrap());
        ids.0.write().unwrap().insert(6);
        assert!(filter.is_unnecessary_key_value(&record(6), b"row", flags).unwrap());
    }

    /// A TiKV-backed node runs with both protocols on, and the registry default
    /// alone would have said otherwise.
    #[test]
    fn a_tikv_backed_node_enables_both_faster_commit_protocols() {
        let protocol = bootstrap_commit_protocol();
        assert!(protocol.async_commit);
        assert!(protocol.one_pc);

        let mock_store = GlobalSysvarEnvironment {
            store_is_tikv: false,
            in_test: false,
            next_gen: false,
        };
        assert_eq!(
            global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, "OFF", mock_store),
            "OFF"
        );
        assert_eq!(
            global_system_variable_initial_value(ENABLE_1PC, "OFF", mock_store),
            "OFF"
        );
    }
}
