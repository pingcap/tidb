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

//! Go `clientConn.prefetchPointPlanKeys` (`pkg/server/conn.go`): inside a
//! transaction, a multi-statement query reads (or, pessimistically, locks)
//! the keys of its point UPDATE and DELETE statements in one batch before
//! the statements run, which then read them from the snapshot's cache.

use super::*;

impl ClusterServerSession<'_> {
    /// Go `prefetchPointPlanKeys(ctx, stmts, sqls)` over the query's
    /// statements, each its own source text.
    ///
    /// # Errors
    ///
    /// A read of the prefetched keys failed, or the lock wait was
    /// interrupted; any other lock failure is only logged, as Go's.
    pub fn prefetch_point_plan_keys(&mut self, statements: &[String], sqls: &str) -> Result<(), SqlQueryError> {
        let Some(transaction) = self.node.txn.explicit.as_ref() else {
            return Ok(());
        };
        // Only prefetch in-transaction query for simplicity.
        let Some(start_ts) = transaction.start_ts_if_active() else {
            return Ok(());
        };
        let pessimistic = transaction.is_pessimistic();
        if pessimistic {
            // To support READ-COMMITTED, we need to avoid getting new TS for
            // each statement in the query.
            if self.session.is_read_committed() {
                return Ok(());
            }
            // Do not handle the case that ForUpdateTS is changed for
            // simplicity.
            if transaction.for_update_ts() != start_ts {
                return Ok(());
            }
        }
        let mut idx_keys = Vec::new();
        let mut row_keys = Vec::new();
        for sql in statements {
            let Ok(stmt) = self.session.parse(sql) else {
                return Ok(());
            };
            // A `use db` statement changes which schema the later ones
            // resolve against.
            if matches!(&stmt, tidb_ast::Stmt::Session(session) if matches!(session.as_ref(), tidb_ast::SessionStmt::Use(_)))
            {
                return Ok(());
            }
            let tidb_ast::Stmt::Dml(dml) = &stmt else {
                continue;
            };
            let ctx = self.session.statement_context_for_stmt(&stmt, true);
            let current_db = self.session.vars.current_db.clone();
            let keys = self
                .session
                .with_planning_catalog(|catalog| {
                    tidb_executor::driver::prefetch_point_plan_keys(dml, catalog, &current_db, &ctx)
                })
                .map_err(sql_query_error)?;
            if let Some((idx, row)) = keys {
                idx_keys.extend(idx);
                row_keys.extend(row);
            }
        }
        if idx_keys.is_empty() && row_keys.is_empty() {
            return Ok(());
        }
        let transaction = self.node.txn.explicit.as_ref().expect("checked above");
        let mut snapshot = transaction.snapshot().map_err(SqlQueryError::unknown)?;
        set_resource_group_tagger_for_multi_stmt_prefetch(snapshot.as_mut(), sqls);
        let to_keys = |keys: &[Vec<u8>]| keys.iter().cloned().map(tidb_txnkv::Key::from_bytes).collect::<Vec<_>>();
        let idx_vals = snapshot
            .batch_get(&to_keys(&idx_keys))
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        for (idx_key, idx_val) in idx_vals {
            let handle = tidb_tablecodec::decode_handle_in_index_value(&idx_val)
                .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))?;
            let Some(handle) = handle else {
                continue;
            };
            let tbl_id = tidb_tablecodec::table_key::decode_table_id(&idx_key);
            row_keys.push(tidb_tablecodec::table_key::encode_row_key(tbl_id, &handle.encoded()));
        }
        if pessimistic {
            let mut all_keys = row_keys;
            all_keys.extend(idx_keys);
            let wait = tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout());
            if let Err(error) = snapshot.do_lock_keys(&to_keys(&all_keys), wait) {
                if matches!(&error, tidb_kv::storage::StorageError::Sql(error) if error.code == tidb_errno::errcode::ErrQueryInterrupted)
                {
                    return Err(SqlQueryError::unknown(error.to_string()));
                }
                // Suppress the lock error, we are not going to handle it
                // here for simplicity.
                tidb_logutil::bg_logger().warn(
                    "lock keys error on prefetch",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                );
            }
        } else {
            snapshot
                .batch_get(&to_keys(&row_keys))
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        }
        Ok(())
    }
}

/// Go `setResourceGroupTaggerForMultiStmtPrefetch`: with Top SQL on, the
/// prefetch's reads carry the whole query's SQL digest.
fn set_resource_group_tagger_for_multi_stmt_prefetch(snapshot: &mut dyn ClusterSnapshot, sqls: &str) {
    if !tidb_topsql_state::top_profiling_enabled() {
        return;
    }
    let (normalized, digest) = tidb_parser::normalize_digest(sqls);
    tidb_topsql::attach_and_register_sql_info(None, &normalized, Some(&digest), false);
    if normalized.is_empty() {
        return;
    }
    let keyspace_name = tidb_keyspace::get_keyspace_name_bytes_by_settings();
    let sql_digest = digest.as_bytes().to_vec();
    let context = tidb_txnkv::StatementKvContextCell::default();
    context.publish(tidb_txnkv::StatementKvContext {
        resource_group_tagger: Some(std::sync::Arc::new(move || {
            let mut tagger = tidb_txnkv::ResourceGroupTagBuilder::new(keyspace_name);
            tagger.set_sql_digest(&sql_digest);
            tagger
        })),
        ..tidb_txnkv::StatementKvContext::default()
    });
    snapshot.set_statement_kv_context(&context);
}
