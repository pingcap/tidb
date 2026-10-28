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

//! Go `session.CreateSession4Test` over this node, and the one-statement
//! command a [`Session`] runs through it.
//!
//! Go's testkit opens its sessions on a booted domain and runs every
//! statement through `session.ExecuteStmt`, so DDL takes the job path and a
//! write commits through the store. [`crate::domain::mock_store`] holds that
//! domain; this file is the node's half: the session it opens and the
//! connection-order statement it runs.

use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_executor::ddl_exec::map_error;

use super::{ClusterServerSession, Domain};
use crate::domain::resultset_source::ResultSetSource;
use crate::domain::session_api::SqlQueryError;
use crate::{Session, StmtOutput};

impl Domain {
    /// Go `DDLExecutor().UpdateTableReplicaInfo(sctx, physicalID, available)`:
    /// the TiFlash replica manager's status change, as the persisted
    /// `ActionUpdateTiFlashReplicaStatus` job (none when the status already
    /// holds).
    ///
    /// # Errors
    ///
    /// The job's refusal, as Go returns it.
    pub fn update_table_replica_info(
        &self,
        physical_id: i64,
        available: bool,
    ) -> Result<(), SqlQueryError> {
        self.ddl
            .execute(
                &tidb_ddl::cluster_ddl::DdlStatement::UpdateTiFlashReplicaStatus {
                    table_id: physical_id,
                    available,
                },
                &tidb_executor::StmtContext::for_query(),
            )
            .map(drop)
    }

    /// Go `session.CreateSession4Test`: a session over this node that no
    /// client logged in. Go's testkit never calls `Auth`, so
    /// `SessionVars.User` stays nil and every privilege request passes; the
    /// session still enters the process list, as testkit registers it with
    /// its mock session manager.
    pub fn create_session_4_test(&self, connection_id: u64) -> Result<Session, SqlQueryError> {
        let mut opened = self.open_storage_session(connection_id)?;
        opened.set_restricted_sql(false);
        let guard = self.processes.register(
            connection_id,
            String::new(),
            String::new(),
            opened.current_database().to_owned(),
            None,
        );
        self.attach_connection(&mut opened, connection_id, guard);
        Ok(opened)
    }
}

impl Domain {
    /// This node's DDL notifier, owning: what Go's
    /// `statstestutil.HandleNextDDLEventWithTxn` runs until the statistics
    /// subscriber has handled every pending event. The caller stops it.
    pub(crate) fn owned_ddl_notifier_for_test(
        self: &Arc<Self>,
    ) -> Arc<tidb_ddl_notifier::DdlNotifier> {
        let notifier = super::ddl_notifier::build_notifier(self, std::time::Duration::ZERO);
        tidb_owner::Listener::on_become_owner(notifier.as_ref());
        notifier
    }
}

impl<'s> ClusterServerSession<'s> {
    /// Runs `body` over the driver session with a fresh committed snapshot
    /// bound to its tables, over the current schema: what a test that looks
    /// at the stored rows directly reads (Go's `tk.Session().GetStore()`
    /// snapshot at the current timestamp).
    pub(crate) fn with_read_snapshot<R>(
        &mut self,
        body: impl FnOnce(&mut Session) -> R,
    ) -> Result<R, tidb_executor::DriverError> {
        self.rebuild_catalog_if_stale();
        let snapshot = self
            .node
            .domain
            .transactions
            .open_snapshot("default")
            .map_err(tidb_executor::DriverError::unsupported)?;
        let previous =
            crate::statement_lifecycle::bind_snapshot(&self.node.slot, &self.session, snapshot);
        let result = body(&mut self.session);
        let _ = crate::statement_lifecycle::finish_snapshot(&self.node.slot);
        if let Some(previous) = previous {
            crate::statement_lifecycle::bind_snapshot(&self.node.slot, &self.session, previous);
        }
        Ok(result)
    }

    /// Binds a fresh committed snapshot to the driver session's tables and
    /// leaves it bound: a statement a caller opens directly on the session
    /// (a prepared execution, a held record set) reads through it until it
    /// closes, as a Go statement keeps the snapshot it started on. The next
    /// statement's own binding replaces it.
    pub(crate) fn bind_open_statement_snapshot(&mut self) -> Result<(), tidb_executor::DriverError> {
        self.rebuild_catalog_if_stale();
        let snapshot = self
            .node
            .domain
            .transactions
            .open_snapshot("default")
            .map_err(tidb_executor::DriverError::unsupported)?;
        let _previous =
            crate::statement_lifecycle::bind_snapshot(&self.node.slot, &self.session, snapshot);
        Ok(())
    }

    /// Go `clientConn.handleQuery`'s transaction-control step alone:
    /// `Some(in_transaction)` for a `BEGIN`/`COMMIT`/`ROLLBACK`, applied to
    /// this connection's transaction buffer, `None` for any other statement.
    pub(crate) fn run_transaction_control(
        &mut self,
        sql: &str,
    ) -> Result<Option<bool>, tidb_executor::DriverError> {
        self.control_transaction(sql).map_err(driver_error)
    }

    /// Go `clientConn.handleQuery` for one statement, read back as the rows a
    /// [`Session::run`] caller expects: parse once, then one
    /// [`ClusterServerSession::execute_stmt`], its result set drained here.
    pub(crate) fn run_statement_output(
        &mut self,
        sql: &str,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        let stmt = match self.session.parse_at_statement_boundary(sql).map_err(map_error) {
            Ok(stmt) => stmt,
            Err(error) => {
                self.session.record_parse_failure_coded(
                    error.code,
                    error.message.clone(),
                    error.identity,
                );
                return Err(driver_error(error));
            }
        };
        self.run_parsed_statement_output(sql, stmt)
    }

    /// Replaces the domain's analyzer this session's statements reach (its
    /// services' `analyze`, which `AnalyzeExec` runs through), for a test
    /// that observes or fails the analysis.
    #[cfg(test)]
    pub(crate) fn set_analyzer(
        &mut self,
        factory: &Domain,
        analyze: Arc<dyn super::ClusterAnalyze>,
    ) {
        let mut services = self
            .session
            .domain_services()
            .expect("a node session holds its domain's services")
            .clone();
        services.analyze = factory.analyze_service_over(Arc::clone(&analyze));
        self.session.install_domain_services(services);
        self.node.domain.set_analyze(analyze);
    }

    /// Rebuilds the session's catalog when the node's schema moved on.
    #[cfg(test)]
    pub(crate) fn refresh_catalog(&mut self) {
        self.rebuild_catalog_if_stale();
    }

    /// [`Self::run_statement_output`] over a statement the caller already
    /// parsed and bound (a parameterized statement's markers replaced by its
    /// values), which re-parsing `sql` would lose.
    pub(crate) fn run_parsed_statement_output(
        &mut self,
        sql: &str,
        stmt: Stmt,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        if let Some(mut result) = self
            .reborrow()
            .execute_stmt(sql, &stmt)
            .map_err(driver_error)?
        {
            return drain_result(result.source()).map_err(tidb_executor::DriverError::Mysql);
        }
        // Go `clientConn.handleQuery`: a LOAD DATA LOCAL or LOAD STATS parks
        // its file request, and the connection answers it with the client's
        // file -- here the session's installed client-file reader.
        let parked = self
            .node
            .parked_local_infile
            .as_ref()
            .map(|parked| parked.path.clone());
        if let Some(path) = parked {
            let data = self.session.read_client_local_file(&path)?;
            self.finish_local_infile(&data).map_err(driver_error)?;
        }
        // The outputs the session's own DDL arms give: CREATE TABLE reports
        // whether it created the table (an IF NOT EXISTS that found it leaves
        // the 1050 note), placement-policy DDL reports nothing created, and
        // the rest report the affected rows.
        Ok(match &stmt {
            Stmt::Ddl(ddl) => match &**ddl {
                tidb_ast::DdlStmt::CreateTable(_) => StmtOutput::Done(
                    !self
                        .session
                        .warnings()
                        .iter()
                        .any(|warning| warning.code() == 1050),
                ),
                tidb_ast::DdlStmt::CreatePlacementPolicy(_)
                | tidb_ast::DdlStmt::AlterPlacementPolicy(_)
                | tidb_ast::DdlStmt::DropPlacementPolicy(_) => StmtOutput::Done(false),
                _ => StmtOutput::Affected(self.node.ok_packet.affected_rows),
            },
            _ => StmtOutput::Affected(self.node.ok_packet.affected_rows),
        })
    }
}

impl super::NodeConn {
    /// Go testkit's `dom.StatsHandle().Update(ctx, is)`: the stats cache
    /// reloads every table whose `mysql.stats_meta` moved since it last read.
    pub(crate) fn update_stats_cache(&self) -> Result<(), tidb_executor::DriverError> {
        self.domain
            .analyze()
            .update_stats_cache(Vec::new())
            .map_err(driver_error)
    }

    /// Go testkit's `dom.StatsHandle().UpdateStatsCache(statstypes.CacheUpdate{
    /// Updated: tables})`: the node's statistics cache takes `tables` as
    /// they are, as Go's mock-statistics tests install theirs.
    pub(crate) fn update_stats_cache_with(&self, tables: Vec<Arc<tidb_stats::Table>>) {
        for table in tables {
            self.domain
                .stats
                .publish_analyzed_table(table.hist_coll.physical_id, table);
        }
    }

    /// Go testkit's `dom.StatsHandle().GetPhysicalTableStats(id, tblInfo)`
    /// without the cache-miss `PseudoTable`: the node's statistics object for
    /// `physical_id`, which a test copies and changes before publishing it
    /// back through [`Self::update_stats_cache_with`].
    pub(crate) fn stats_table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.domain.stats.get(physical_id)
    }

    /// A fresh timestamp from the store's oracle, for a test transaction
    /// written straight into the store.
    #[cfg(test)]
    pub(crate) fn fresh_timestamp_for_test(&self) -> Result<u64, String> {
        self.domain.transactions.fresh_timestamp()
    }

}

/// The rows and column types of an opened result, closed afterwards as the
/// connection's writer closes it.
fn drain_result(source: &mut dyn ResultSetSource) -> Result<StmtOutput, tidb_executor::MysqlError> {
    let infos = source.columns()?;
    let native = source.field_types().to_vec();
    let columns = infos
        .iter()
        .enumerate()
        .map(|(index, info)| {
            let field_type = native
                .get(index)
                .filter(|_| native.len() == infos.len())
                .cloned()
                .unwrap_or_else(|| column_field_type(info));
            (info.name.clone(), field_type)
        })
        .collect();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(1024)?;
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish()?;
    source.close()?;
    Ok(StmtOutput::Rows { columns, rows })
}

/// The field type a column definition packet describes.
fn column_field_type(info: &tidb_protocol::ColumnInfo) -> tidb_datatype::FieldType {
    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::from_mysql_type(
        info.type_code,
    ))
    .with_flags(u32::from(info.flag))
    .with_flen(i64::from(info.column_length))
    .with_decimal(i64::from(info.decimal))
}

fn driver_error(error: SqlQueryError) -> tidb_executor::DriverError {
    if let Some(cause) = crate::txn::driver_cause(&error) {
        return cause.clone();
    }
    let mut mapped = tidb_executor::MysqlError::from_parts(error.code, error.state, error.message);
    if let Some(identity) = error.identity {
        mapped = mapped.with_identity(identity);
    }
    tidb_executor::DriverError::Mysql(mapped)
}
