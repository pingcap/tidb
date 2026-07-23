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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The first shared-storage execution seam.
//!
//! [`Database`] intentionally combines one catalog and one SQL session.  That
//! is the right compact model for the single-session differential corpus, but
//! it cannot represent Go tests that create several sessions against one
//! store.  This module keeps those ownership domains separate without moving
//! `Database`'s non-`Send` session internals behind a global mutex:
//!
//! * [`Cluster`] owns only committed catalog data and is `Send + Sync`.
//! * [`Session`] owns a normal, local [`Database`] for its session values.
//! * a statement snapshots the catalog under a short lock, evaluates locally,
//!   then publishes any catalog effects through version CAS independently of
//!   whether the statement returns success or an error.
//!
//! This is deliberately an autocommit-only milestone. It supplies the real
//! concurrent shared-store boundary exercised by Go's `TestErrorRollback`,
//! including version-CAS conflict detection and complete-statement retries.
//! It does not claim explicit transaction snapshots/write buffers, lock
//! management, distributed retry errors, or shared auto-ID allocation.
//! Transaction-control statements and transaction-affecting settings are
//! rejected before state mutation until their versioned write-buffer protocol
//! exists.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Barrier, Condvar,
};
#[cfg(test)]
use std::time::Duration;

use tidb_ast::{
    ColumnOption, CreateTableTemporary, DdlStmt, DmlStmt, Expr, IndexConstraintKind, IndexOptions,
    Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind, SessionStmt, SetStmt,
    SetVariableValue, Stmt, SystemVariableScope, TableConstraint, UpdateKind,
};

use crate::result_schema::{
    is_bounded_catalog_count_column_select, resolve_catalog_count_column_select_field,
};
use crate::{
    col_names_to_result_fields, columns_from_adapted_fields, derive_join_output_metadata,
    project_join_output_fields, resolve_catalog_relation_select_fields,
    resolve_catalog_select_fields, CatalogTableSchema, Database, ExecError, FieldNameMetadata,
    IdentifierMetadata, JoinOutputChild, JoinOutputField, JoinOutputMetadata, Outcome,
    RenderedExecError, ResolvedResultField, StatementKind, StatementStatus, Table, NOT_NULL_FLAG,
};

/// Committed catalog data shared by all [`Session`]s in one [`Cluster`].
#[derive(Debug, Default)]
struct SharedCatalog {
    tables: BTreeMap<String, Table>,
    /// Changes only after a successful catalog publication.  It is retained
    /// now so the transaction slice can compare a session's begin version to
    /// the current committed version rather than retrofitting another shared
    /// state shape later.
    version: u64,
    /// Number of failed version compare-and-swap publications.  This is real
    /// runtime evidence, not an inferred final-row count.
    commit_conflicts: u64,
    /// Number of complete statements retried from a fresh catalog snapshot.
    retries: u64,
    #[cfg(test)]
    first_commit_barrier: Option<Arc<TestCommitBarrier>>,
}

/// Test-only timing control. It blocks only the first `parties` CAS attempts,
/// after each worker has executed against a snapshot but before any one can
/// publish. This creates genuine stale-version conflicts; it never changes a
/// result or manufactures a successful statement.
#[cfg(test)]
#[derive(Debug)]
struct TestCommitBarrier {
    barrier: Barrier,
    remaining: AtomicUsize,
}

/// A cluster-local catalog usable by concurrent SQL sessions.
#[derive(Debug, Clone, Default)]
pub struct Cluster {
    catalog: Arc<Mutex<SharedCatalog>>,
}

impl Cluster {
    /// Creates an empty committed catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an independent SQL session attached to this cluster.
    pub fn session(&self) -> Session {
        Session {
            cluster: self.clone(),
            database: Database::new(),
            statement_status: StatementStatus::default(),
            statement_status_published: false,
            #[cfg(test)]
            before_cas: None,
        }
    }

    /// Returns the observed failed compare-and-swap publications.
    pub fn commit_conflicts(&self) -> u64 {
        self.catalog
            .lock()
            .expect("shared catalog mutex poisoned")
            .commit_conflicts
    }

    /// Returns the observed full-statement retries caused by stale snapshots.
    pub fn retries(&self) -> u64 {
        self.catalog
            .lock()
            .expect("shared catalog mutex poisoned")
            .retries
    }

    #[cfg(test)]
    pub(crate) fn catalog_version(&self) -> u64 {
        self.catalog
            .lock()
            .expect("shared catalog mutex poisoned")
            .version
    }

    #[cfg(test)]
    pub(crate) fn synchronize_first_commits(&self, parties: usize) {
        let mut catalog = self.catalog.lock().expect("shared catalog mutex poisoned");
        catalog.first_commit_barrier = Some(Arc::new(TestCommitBarrier {
            barrier: Barrier::new(parties),
            remaining: AtomicUsize::new(parties),
        }));
    }

    #[cfg(test)]
    fn wait_before_test_cas(&self) {
        let barrier = self
            .catalog
            .lock()
            .expect("shared catalog mutex poisoned")
            .first_commit_barrier
            .clone();
        let Some(barrier) = barrier else {
            return;
        };
        if barrier
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            barrier.barrier.wait();
        }
    }
}

/// Per-connection state attached to one [`Cluster`].
///
/// A session is intentionally not `Clone` or `Send`: each worker creates and
/// owns one locally, exactly as Go's test owns one `testkit.TestKit` per
/// goroutine. Its `Database` retains local clocks, variables, RNG, and future
/// transaction state; only tables cross the cluster boundary.
#[derive(Debug)]
pub struct Session {
    cluster: Cluster,
    database: Database,
    /// Statement-scoped status owned by the connection rather than a cloned
    /// catalog-evaluation [`Database`]. This is the source-shaped boundary
    /// where Go's `ResetContextOfStmt`/`FinishExecuteStmt` pair publishes
    /// counters for both successful and erroring executions.
    statement_status: StatementStatus,
    /// Whether the latest command reached statement publication. Parse or
    /// framing failures must not inherit a prior statement's status.
    statement_status_published: bool,
    #[cfg(test)]
    before_cas: Option<Arc<TestCasGate>>,
}

/// Deterministic test-only pause immediately before one session's next CAS.
/// It changes timing only; catalog effects and outcomes still come from the
/// ordinary evaluation/publication protocol.
#[cfg(test)]
#[derive(Debug, Default)]
struct TestCasGate {
    state: Mutex<TestCasGateState>,
    changed: Condvar,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TestCasGateState {
    arrived: bool,
    released: bool,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct TestCasControl(Arc<TestCasGate>);

#[cfg(test)]
impl TestCasControl {
    pub(crate) fn wait_until_arrived(&self) {
        let state = self.0.state.lock().expect("test CAS gate poisoned");
        let (state, timeout) = self
            .0
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.arrived)
            .expect("test CAS gate poisoned while waiting");
        assert!(
            !timeout.timed_out() && state.arrived,
            "session never reached CAS"
        );
    }

    pub(crate) fn release(&self) {
        let mut state = self.0.state.lock().expect("test CAS gate poisoned");
        state.released = true;
        self.0.changed.notify_all();
    }
}

/// The catalog delta produced by one local statement attempt. Outcome and
/// effects are intentionally independent: Go DDL can return an error after
/// applying durable catalog changes.
#[derive(Debug)]
enum CatalogEffects {
    Unchanged,
    ReplaceTables(BTreeMap<String, Table>),
}

#[derive(Debug)]
struct StatementAttempt {
    working: Database,
    outcome: Result<Outcome, ExecError>,
    effects: CatalogEffects,
}

impl StatementAttempt {
    fn evaluate(session: &Database, tables: BTreeMap<String, Table>, stmt: &Stmt) -> Self {
        let original = tables.clone();
        let mut working = session.clone();
        working.tables = tables;
        let outcome = working.run(stmt);
        let effects = if working.tables == original {
            CatalogEffects::Unchanged
        } else {
            CatalogEffects::ReplaceTables(working.tables.clone())
        };
        Self {
            working,
            outcome,
            effects,
        }
    }
}

impl Session {
    /// Resolves one query's protocol columns from the same catalog snapshot
    /// that owns table declarations. Table-backed projections and bounded
    /// INNER/CROSS relation trees use declared `ColumnType` metadata, while
    /// LEFT/RIGHT/USING/NATURAL trees additionally use planner-shaped output metadata and
    /// the direct-column/wildcard projection contract;
    /// table-less projections reuse the existing expression resolver. A bare
    /// any accepted projection crossing an outer/coalescing join is routed through
    /// planner-shaped join output metadata so coalesced field order and
    /// outer-side row width stay aligned with `Database::build_join`. No
    /// returned `Datum` participates in this path; `Database::project_row`
    /// remains the sole row-value owner.
    pub fn resolve_query_result_columns(
        &self,
        sql: &str,
        default_collation: tidb_datatype::Collation,
        default_db: &str,
    ) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        let statement = tidb_parser::parse(sql).map_err(|error| format!("{error:?}"))?;
        let Stmt::Query(query) = statement else {
            return Err("automatic result metadata requires a plain SELECT".to_owned());
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            return Err("automatic result metadata requires a plain SELECT".to_owned());
        };
        if select.from.is_none() {
            return crate::resolve_query_result_columns(sql, default_collation, default_db)
                .map_err(|error| error.to_string());
        }
        let from = select
            .from
            .as_ref()
            .expect("checked that SELECT has a FROM clause");
        if select.with.is_some() {
            return Err("automatic result metadata for WITH queries requires schema".to_owned());
        }

        // Preserve the original single-table path, whose validation and
        // default-database behavior are already source-shaped. A binary or
        // nested relation now crosses the bounded multi-relation resolver,
        // which binds every catalog table before result metadata is adapted.
        let fields = if matches!(&from.left, JoinNode::Table(_)) && from.right.is_none() {
            let JoinNode::Table(table_ref) = &from.left else {
                unreachable!("single-table shape checked above");
            };
            let table_key = crate::catalog::table_key(&table_ref.name);
            let table = self
                .cluster
                .catalog
                .lock()
                .map_err(|_| "shared catalog mutex poisoned".to_owned())?
                .tables
                .get(&table_key)
                .cloned()
                .ok_or_else(|| format!("catalog table not found: {table_key}"))?;
            let database = table_ref
                .name
                .get(..table_ref.name.len().saturating_sub(1))
                .and_then(|path| path.first())
                .cloned()
                .unwrap_or_else(|| default_db.to_owned());
            let schema = CatalogTableSchema::from_columns(
                database,
                table_key,
                &table.cols,
                &table.col_types,
            )
            .map_err(|error| error.to_string())?;
            if is_bounded_catalog_count_column_select(&select) {
                vec![
                    resolve_catalog_count_column_select_field(&select, &schema, default_collation)
                        .map_err(|error| error.to_string())?,
                ]
            } else {
                resolve_catalog_select_fields(&select, &schema)
                    .map_err(|error| error.to_string())?
            }
        } else {
            let catalog = self
                .cluster
                .catalog
                .lock()
                .map_err(|_| "shared catalog mutex poisoned".to_owned())?;
            let schemas = catalog_relation_schemas(from, &catalog.tables, default_db)?;
            if join_output_required(from) {
                resolve_catalog_join_output_select_fields(&select, from, &schemas)?
            } else {
                reject_unsupported_automatic_join_shapes(from)?;
                resolve_catalog_relation_select_fields(&select, &schemas)
                    .map_err(|error| error.to_string())?
            }
        };
        let types = fields
            .iter()
            .map(|field| field.field_type.clone())
            .collect::<Vec<_>>();
        let names = fields
            .iter()
            .map(|field| field.names.clone())
            .collect::<Vec<_>>();
        Ok(columns_from_adapted_fields(&col_names_to_result_fields(
            &types, &names, default_db,
        )))
    }

    /// Parses and executes exactly one SQL statement for this session.
    ///
    /// The strict parser rejects trailing statements before [`Self::run`]
    /// can inspect or mutate the shared catalog. A parsed but unsupported
    /// command then follows the same positive capability envelope as callers
    /// that already own an AST. This is the protocol-neutral seam a future
    /// MySQL `COM_QUERY` adapter can call after packet/authentication handling;
    /// it intentionally does not accept or silently drop multi-statements.
    pub fn execute_sql(&mut self, sql: &str) -> Result<Outcome, ExecError> {
        self.statement_status_published = false;
        let statement = tidb_parser::parse(sql).map_err(ExecError::from)?;
        self.run(&statement)
    }

    /// Returns the connection's latest published statement status.
    ///
    /// Status is deliberately owned by [`Session`] rather than the cloned
    /// [`Database`] attempts used by the shared-catalog CAS loop. This keeps
    /// retry-local evaluation from publishing stale counters while retaining
    /// the previous status across the next statement boundary.
    pub fn statement_status(&self) -> &StatementStatus {
        &self.statement_status
    }

    /// Wraps a source-rendered execution error with this session's latest
    /// statement publication.
    ///
    /// This is the connection-facing handoff for errors returned by
    /// [`Self::run`]: the caller still owns rendering (including SQLSTATE,
    /// table/column context, and redaction), while the session contributes
    /// the exact ordered warning/message snapshot and `ExecSuccess` bit that
    /// Go keeps on `StatementContext` (`pkg/sessionctx/stmtctx/stmtctx.go:361-365,1129-1170`).
    /// Parse/framing failures that never reached `run` receive no stale
    /// previous status. The method never formats the [`ExecError`] or copies
    /// warnings into the ERR packet; `tidb-server` decides how to write the
    /// attached context.
    #[must_use]
    pub fn render_exec_error(
        &self,
        error: &ExecError,
        rendered_message: impl Into<Vec<u8>>,
    ) -> RenderedExecError {
        if self.statement_status_published {
            RenderedExecError::with_status(
                error,
                rendered_message,
                self.statement_status.previous(),
            )
        } else {
            RenderedExecError::new(error, rendered_message)
        }
    }

    /// Executes one uncompressed MySQL `COM_QUERY` packet against this
    /// session and records the original SQL in the request context.
    ///
    /// Packet framing is deliberately consumed through `tidb-protocol`, while
    /// request metadata is owned by `tidb-distsql`. Authentication,
    /// compression, and command dispatch beyond `COM_QUERY` remain outside
    /// this first connected read-only seam. Any framing or UTF-8 failure is
    /// returned before the SQL entrypoint can mutate state.
    pub fn execute_framed_query(
        &mut self,
        framed: &[u8],
        request: &mut tidb_distsql::DistSqlContext,
    ) -> Result<Outcome, ExecError> {
        self.statement_status_published = false;
        let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(framed));
        let payload = reader
            .read_packet()
            .map_err(|error| ExecError::Protocol(error.to_string()))?;
        let cursor = reader.into_inner();
        if cursor.position() != cursor.get_ref().len() as u64 {
            return Err(ExecError::Protocol(
                "trailing bytes after COM_QUERY packet".to_string(),
            ));
        }
        let Some((&command, sql_bytes)) = payload.split_first() else {
            return Err(ExecError::Protocol("empty command packet".to_string()));
        };
        if command != 0x03 {
            return Err(ExecError::Unsupported("COM_QUERY"));
        }
        let sql = std::str::from_utf8(sql_bytes)
            .map_err(|error| ExecError::Protocol(format!("query payload is not UTF-8: {error}")))?;
        request.request.original_sql = sql.to_owned();
        self.execute_sql(sql)
    }

    /// Executes one framed query and encodes only its text-protocol row
    /// packets.
    ///
    /// This is the next response-side seam after [`Self::execute_framed_query`]:
    /// [`tidb_protocol::PacketWriter`] frames each already-evaluated row, and
    /// [`tidb_protocol::encode_text_row`] owns length-encoding plus the SQL
    /// `NULL` marker. Column-definition packets, OK/EOF status packets,
    /// charset conversion, authentication, and command lifecycle remain
    /// explicit protocol/server owners; this method must not be mistaken for
    /// a complete MySQL result-set implementation.
    pub fn execute_framed_query_text_rows(
        &mut self,
        framed: &[u8],
        request: &mut tidb_distsql::DistSqlContext,
    ) -> Result<Vec<u8>, ExecError> {
        let outcome = self.execute_framed_query(framed, request)?;
        let Outcome::Rows(result) = outcome else {
            return Ok(Vec::new());
        };

        let mut encoded = Vec::new();
        let mut writer = tidb_protocol::PacketWriter::new(&mut encoded);
        for row in &result.rows {
            let values = row
                .iter()
                .map(|datum| {
                    if datum.is_null() {
                        Ok(None)
                    } else {
                        datum
                            .sql_string()
                            .map(|value| Some(value.into_bytes()))
                            .map_err(|error| {
                                ExecError::Protocol(format!(
                                    "result value is not valid UTF-8: {error}"
                                ))
                            })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let references = values.iter().map(Option::as_deref).collect::<Vec<_>>();
            let payload = tidb_protocol::encode_text_row(&references);
            writer
                .write_packet(&payload)
                .map_err(|error| ExecError::Protocol(error.to_string()))?;
        }
        writer
            .flush()
            .map_err(|error| ExecError::Protocol(error.to_string()))?;
        Ok(encoded)
    }

    /// Executes one framed query and encodes a complete text-protocol result
    /// sequence when the caller supplies the source-owned column metadata.
    ///
    /// The executor seed currently returns rows without schema/result-field
    /// metadata, so metadata ownership stays explicit at this boundary rather
    /// than being guessed from `Datum` values. `tidb-protocol` owns the exact
    /// column-count, column-definition, metadata-EOF, text-row, and terminal
    /// EOF/OK-shaped packet order; this method owns only query execution,
    /// source-shaped typed scalar formatting, published statement-status
    /// attachment, and logical-packet framing.
    ///
    /// Statement execution publishes before row conversion and response
    /// encoding begin. An error from those later phases therefore does not
    /// rewrite the published statement's `exec_success` bit: that bit describes
    /// SQL execution, not delivery of the response bytes. The current public
    /// boundary carries both phases through `ExecError`, so callers must not
    /// reinterpret a response-conversion failure as a failed SQL statement.
    /// Charset conversion, authentication, compression, temporal/JSON/enum/
    /// set formatting, and server result-set lifecycle remain outside this
    /// seed.
    pub fn execute_framed_query_text_result_set(
        &mut self,
        framed: &[u8],
        request: &mut tidb_distsql::DistSqlContext,
        columns: &[tidb_protocol::ColumnInfo],
        options: tidb_protocol::ResultSetOptions,
    ) -> Result<Vec<u8>, ExecError> {
        let outcome = self.execute_framed_query(framed, request)?;
        let status = crate::StatusResultSnapshot::from_status(
            &self.statement_status,
            options.status_flags,
            options.deprecate_eof,
            options.protocol_41,
        );
        if matches!(outcome, Outcome::Done) {
            let payload = tidb_protocol::encode_ok_packet(&status.ok_packet);
            let mut encoded = Vec::new();
            let mut writer = tidb_protocol::PacketWriter::new(&mut encoded);
            writer
                .write_packet(&payload)
                .map_err(|error| ExecError::Protocol(error.to_string()))?;
            writer
                .flush()
                .map_err(|error| ExecError::Protocol(error.to_string()))?;
            return Ok(encoded);
        }
        let Outcome::Rows(result) = outcome else {
            unreachable!("all outcomes are handled above");
        };

        let rows = result
            .rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                if row.len() != columns.len() {
                    return Err(ExecError::Protocol(format!(
                        "result row {row_index} has {} values, expected {}",
                        row.len(),
                        columns.len()
                    )));
                }
                row.iter()
                    .zip(columns)
                    .map(|(datum, column)| format_datum_for_column(column, datum))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let payloads =
            tidb_protocol::encode_text_result_set(columns, &rows, status.result_set_options)
                .map_err(|error| ExecError::Protocol(error.to_string()))?;

        let mut encoded = Vec::new();
        let mut writer = tidb_protocol::PacketWriter::new(&mut encoded);
        for payload in payloads {
            writer
                .write_packet(&payload)
                .map_err(|error| ExecError::Protocol(error.to_string()))?;
        }
        writer
            .flush()
            .map_err(|error| ExecError::Protocol(error.to_string()))?;
        Ok(encoded)
    }

    /// Returns this connection's `@@tidb_retry_limit` compatibility value.
    pub fn retry_limit(&self) -> i64 {
        self.database.tidb_retry_limit
    }

    /// Executes one autocommit statement against the shared committed catalog.
    ///
    /// A statement copies `{tables, version}` under a short lock, runs in this
    /// session's local executor without that lock, then conditionally publishes
    /// only if the version is unchanged. A stale snapshot retries the COMPLETE
    /// statement from a new catalog image. An error with no table delta is not
    /// published; an error with source-defined catalog effects is CAS-published
    /// before that same error is returned. This is not an explicit transaction
    /// implementation: the capability envelope rejects every transaction form
    /// before any snapshot is read.
    pub fn run(&mut self, stmt: &Stmt) -> Result<Outcome, ExecError> {
        self.statement_status_published = false;
        let kind = statement_kind(stmt);
        self.statement_status.begin_statement(kind);
        let result = self.run_statement(stmt);
        self.publish_statement_status(kind, &result);
        self.statement_status_published = true;
        result
    }

    /// Executes one statement after its connection status boundary has been
    /// established by [`Self::run`]. Keeping this body separate is important:
    /// the CAS loop may evaluate a complete statement more than once, but
    /// only its final outcome may publish statement status.
    fn run_statement(&mut self, stmt: &Stmt) -> Result<Outcome, ExecError> {
        if !shared_autocommit_capability(stmt) {
            return Err(ExecError::Unsupported("shared-session capability"));
        }

        // The source-proven local session commands neither read nor advance
        // the shared catalog version, so they cannot create a spurious
        // conflict. Their capability check accepts only immutable SET leaves.
        if matches!(stmt, Stmt::Session(_)) {
            return self
                .database
                .run_with_statement_status(stmt, &mut self.statement_status);
        }

        let mut retries = 0_i64;
        loop {
            let (tables, version) = {
                let catalog = self
                    .cluster
                    .catalog
                    .lock()
                    .expect("shared catalog mutex poisoned");
                (catalog.tables.clone(), catalog.version)
            };
            // Database::Clone is the compatibility adapter for this initial
            // seam: all existing statement evaluation remains in one place.
            // Outcome and catalog effects are captured independently because
            // an error can still carry source-defined durable DDL effects.
            let attempt = StatementAttempt::evaluate(&self.database, tables, stmt);
            let CatalogEffects::ReplaceTables(replacement) = &attempt.effects else {
                // An erroring DML/DDL with no catalog delta is never published
                // and therefore cannot create a false version conflict.
                self.database = attempt.working;
                return attempt.outcome;
            };

            #[cfg(test)]
            {
                self.cluster.wait_before_test_cas();
                self.wait_before_session_test_cas();
            }

            let mut catalog = self
                .cluster
                .catalog
                .lock()
                .expect("shared catalog mutex poisoned");
            if catalog.version == version {
                catalog.tables = replacement.clone();
                catalog.version = catalog
                    .version
                    .checked_add(1)
                    .expect("catalog version overflow");
                self.database = attempt.working;
                return attempt.outcome;
            }

            catalog.commit_conflicts = catalog
                .commit_conflicts
                .checked_add(1)
                .expect("commit conflict counter overflow");
            if retries >= self.database.tidb_retry_limit.max(0) {
                return Err(ExecError::WriteConflict);
            }
            retries += 1;
            // A stale CAS is a complete statement retry in Go. Clear any
            // future status entries produced during a partial attempt while
            // retaining the previously published statement for readback.
            self.statement_status.reset_for_retry();
            catalog.retries = catalog
                .retries
                .checked_add(1)
                .expect("retry counter overflow");
        }
    }

    #[cfg(test)]
    pub(crate) fn has_user_var(&self, name: &str) -> bool {
        self.database
            .user_vars
            .borrow()
            .contains_key(&name.to_ascii_lowercase())
    }

    #[cfg(test)]
    pub(crate) fn hold_next_cas(&mut self) -> TestCasControl {
        let gate = Arc::new(TestCasGate::default());
        self.before_cas = Some(gate.clone());
        TestCasControl(gate)
    }

    #[cfg(test)]
    fn wait_before_session_test_cas(&mut self) {
        let Some(gate) = self.before_cas.take() else {
            return;
        };
        let mut state = gate.state.lock().expect("test CAS gate poisoned");
        state.arrived = true;
        gate.changed.notify_all();
        while !state.released {
            state = gate
                .changed
                .wait(state)
                .expect("test CAS gate poisoned while blocked");
        }
    }

    /// Publishes source-shaped status after the statement's final success or
    /// error. `Database::run` remains the authoritative evaluator for
    /// existing `ROW_COUNT()`/`LAST_INSERT_ID()` expression behavior; this
    /// adapter mirrors its final session cells into the dependency-closed
    /// status owner without changing catalog or transaction state.
    fn publish_statement_status(
        &mut self,
        kind: StatementKind,
        result: &Result<Outcome, ExecError>,
    ) {
        match kind {
            StatementKind::Dml => {
                // Executable DML reports its count on success. A DML error
                // publishes zero, matching the source reset-before-execute
                // path even when an evaluator had no affected-row result.
                let affected = if result.is_ok() && self.database.previous_affected_rows >= 0 {
                    self.database.previous_affected_rows as u64
                } else {
                    0
                };
                self.statement_status.set_affected_rows(affected);
            }
            StatementKind::Ddl => self.statement_status.set_affected_rows(0),
            StatementKind::Select | StatementKind::Session | StatementKind::Unknown => {}
        }

        // `LAST_INSERT_ID(expr)` and generated AUTO_INCREMENT values write
        // this current-statement cell even when a later expression/DML check
        // returns an error. Preserve that source-visible handoff; when no
        // current value exists, StatementStatus intentionally retains its
        // previous published ID.
        let current_last_insert_id = self.database.statement_last_insert_id.borrow().to_owned();
        if let Some(last_insert_id) = current_last_insert_id {
            self.statement_status.set_last_insert_id(last_insert_id);
        }
        // Session warning producers such as checkReadOnly append directly to
        // this StatementStatus during execution. Do not invent an additional
        // warning from the final ExecError: finish publishes the handler's
        // exact ordered entries on both result branches, while source error
        // packets remain outside this leaf.
        self.statement_status
            .finish_statement_with_outcome(result.is_ok());
    }
}

/// Returns whether the planner-owned join-output metadata leaf is required.
///
/// Plain INNER/CROSS joins preserve the flattened child schema, so the
/// catalog relation resolver can continue to derive their fields directly.
/// Outer joins and USING/NATURAL joins need FullSchema-aware metadata: outer
/// joins change nullability (and RIGHT mirrors FullSchema), while coalescing
/// changes visible order and redundant-column mappings. Walk the complete tree
/// so a nested outer/coalescing join never reaches the flattened path.
fn join_output_required(join: &Join) -> bool {
    matches!(join.tp, JoinType::Left | JoinType::Right)
        || join.natural
        || !join.using.is_empty()
        || matches!(&join.left, JoinNode::Join(child) if join_output_required(child))
        || matches!(&join.right, Some(JoinNode::Join(child)) if join_output_required(child))
}

/// Resolves the narrow automatic response shape that has a source-owned join
/// output schema. Direct column references, aliases, and qualified/bare
/// wildcards can now cross the isolated projection contract because
/// `Database::project_row` evaluates the same select list against the
/// `build_join` relation whose columns are represented by this metadata.
/// Qualified redundant `USING` columns resolve through the planner FullSchema
/// mapping while bare output remains coalesced; expression typing is not
/// guessed here.
fn resolve_catalog_join_output_select_fields(
    select: &tidb_ast::SelectStmt,
    from: &Join,
    schemas: &[CatalogTableSchema],
) -> Result<Vec<ResolvedResultField>, String> {
    let metadata = join_output_metadata(from, schemas)?;
    project_join_output_fields(&select.fields, &metadata)
        .map_err(|error| format!("automatic join projection failed: {error}"))
}

/// One recursively resolved child schema. The map composes the child's
/// planner FullSchema positions into its visible executable fields.
struct AutomaticJoinChild {
    fields: Vec<JoinOutputField>,
    full_fields: Vec<JoinOutputField>,
    full_to_field_indices: Vec<usize>,
}

/// Derives a join node without collapsing its planner FullSchema to visible
/// output. This mirrors Go's `buildJoin`: a parent merges each child join's
/// FullSchema, then composes redundant-column mappings into its own output.
fn join_output_metadata(
    join: &Join,
    schemas: &[CatalogTableSchema],
) -> Result<JoinOutputMetadata, String> {
    let Some(right) = &join.right else {
        return Err("automatic join output metadata requires a binary join".to_owned());
    };
    let AutomaticJoinChild {
        fields: left_fields,
        full_fields: mut left_full_fields,
        full_to_field_indices: left_full_to_fields,
    } = join_output_node_inner(&join.left, schemas)?;
    let AutomaticJoinChild {
        fields: right_fields,
        full_fields: mut right_full_fields,
        full_to_field_indices: right_full_to_fields,
    } = join_output_node_inner(right, schemas)?;
    let left_width = left_fields.len();
    let right_width = right_fields.len();
    let mut metadata = derive_join_output_metadata(
        join,
        JoinOutputChild::Fields(left_fields),
        JoinOutputChild::Fields(right_fields),
    )
    .map_err(|error| format!("automatic join output metadata failed: {error:?}"))?;

    if join.tp == JoinType::Left {
        for field in &mut right_full_fields {
            field.nullable = true;
            field.field.field_type.flags &= !NOT_NULL_FLAG;
        }
    } else if join.tp == JoinType::Right {
        for field in &mut left_full_fields {
            field.nullable = true;
            field.field.field_type.flags &= !NOT_NULL_FLAG;
        }
    }

    // `derive_join_output_metadata` maps the immediate children's visible
    // fields into this output. Compose each child's FullSchema map through
    // that immediate map so aliases on nested redundant USING fields survive
    // all the way to the root projection resolver.
    let immediate_to_output = &metadata.full_to_output_indices;
    let mut full_to_output_indices =
        Vec::with_capacity(left_full_to_fields.len() + right_full_to_fields.len());
    if join.tp == JoinType::Right {
        // RIGHT FullSchema is outer(original right) + inner(original left),
        // even though a plain RIGHT executable row remains syntactic
        // left+right. The immediate metadata map bridges those orders.
        full_to_output_indices.extend(
            right_full_to_fields
                .iter()
                .map(|index| immediate_to_output[*index]),
        );
        full_to_output_indices.extend(
            left_full_to_fields
                .iter()
                .map(|index| immediate_to_output[right_width + *index]),
        );
        metadata.full_fields = right_full_fields;
        metadata.full_fields.extend(left_full_fields);
    } else {
        full_to_output_indices.extend(
            left_full_to_fields
                .iter()
                .map(|index| immediate_to_output[*index]),
        );
        full_to_output_indices.extend(
            right_full_to_fields
                .iter()
                .map(|index| immediate_to_output[left_width + *index]),
        );
        metadata.full_fields = left_full_fields;
        metadata.full_fields.extend(right_full_fields);
    }
    metadata.full_to_output_indices = full_to_output_indices;
    Ok(metadata)
}

fn join_output_node_inner(
    node: &JoinNode,
    schemas: &[CatalogTableSchema],
) -> Result<AutomaticJoinChild, String> {
    match node {
        JoinNode::Table(table_ref) => {
            let schema = catalog_schema_for_table(table_ref, schemas)?;
            let qualifier = table_ref
                .alias
                .as_deref()
                .filter(|alias| !alias.is_empty())
                .unwrap_or(&schema.table);
            let fields: Vec<JoinOutputField> = schema
                .columns
                .iter()
                .map(|column| {
                    let original_column = IdentifierMetadata::new(column.name.clone());
                    JoinOutputField::new(
                        ResolvedResultField {
                            names: FieldNameMetadata {
                                original_table: IdentifierMetadata::new(schema.table.clone()),
                                original_column: original_column.clone(),
                                database: IdentifierMetadata::new(schema.database.clone()),
                                table: IdentifierMetadata::new(qualifier),
                                column: original_column,
                            },
                            field_type: column.field_type.clone(),
                        },
                        false,
                    )
                })
                .collect();
            let full_to_field_indices = (0..fields.len()).collect();
            Ok(AutomaticJoinChild {
                full_fields: fields.clone(),
                fields,
                full_to_field_indices,
            })
        }
        JoinNode::Derived { .. } => Err(
            "automatic result metadata for derived relations requires planner schema".to_owned(),
        ),
        JoinNode::Join(join) => {
            let metadata = join_output_metadata(join, schemas)?;
            Ok(AutomaticJoinChild {
                fields: metadata.fields,
                full_fields: metadata.full_fields,
                full_to_field_indices: metadata.full_to_output_indices,
            })
        }
    }
}

fn catalog_schema_for_table<'a>(
    table_ref: &tidb_ast::TableRef,
    schemas: &'a [CatalogTableSchema],
) -> Result<&'a CatalogTableSchema, String> {
    let matches = match table_ref.name.as_slice() {
        [table] => schemas
            .iter()
            .filter(|schema| schema.table.eq_ignore_ascii_case(table))
            .collect::<Vec<_>>(),
        [database, table] => schemas
            .iter()
            .filter(|schema| {
                schema.database.eq_ignore_ascii_case(database)
                    && schema.table.eq_ignore_ascii_case(table)
            })
            .collect::<Vec<_>>(),
        _ => {
            return Err(format!(
                "automatic join output metadata has invalid table path: {}",
                table_ref.name.join(".")
            ));
        }
    };
    match matches.as_slice() {
        [schema] => Ok(schema),
        [] => Err(format!(
            "catalog table not found: {}",
            table_ref.name.join(".")
        )),
        _ => Err(format!(
            "catalog table path is ambiguous: {}",
            table_ref.name.join(".")
        )),
    }
}

/// Collects the unique catalog snapshots referenced by a bounded relation
/// tree. The catalog currently keys tables by their final identifier segment,
/// so aliases (including self-joins) reuse one authoritative snapshot rather
/// than creating an ambiguous duplicate schema entry.
fn catalog_relation_schemas(
    join: &Join,
    tables: &BTreeMap<String, Table>,
    default_db: &str,
) -> Result<Vec<CatalogTableSchema>, String> {
    let mut schemas = Vec::new();
    collect_catalog_relation_schemas(&join.left, tables, default_db, &mut schemas)?;
    if let Some(right) = &join.right {
        collect_catalog_relation_schemas(right, tables, default_db, &mut schemas)?;
    }
    if schemas.is_empty() {
        return Err("automatic result metadata requires catalog tables".to_owned());
    }
    Ok(schemas)
}

fn collect_catalog_relation_schemas(
    node: &JoinNode,
    tables: &BTreeMap<String, Table>,
    default_db: &str,
    schemas: &mut Vec<CatalogTableSchema>,
) -> Result<(), String> {
    match node {
        JoinNode::Table(table_ref) => {
            let table_key = crate::catalog::table_key(&table_ref.name);
            if schemas
                .iter()
                .any(|schema| schema.table.eq_ignore_ascii_case(&table_key))
            {
                return Ok(());
            }
            let table = tables
                .get(&table_key)
                .ok_or_else(|| format!("catalog table not found: {table_key}"))?;
            let database = table_ref
                .name
                .first()
                .cloned()
                .filter(|_| table_ref.name.len() > 1)
                .unwrap_or_else(|| default_db.to_owned());
            schemas.push(
                CatalogTableSchema::from_columns(
                    database,
                    table_key,
                    &table.cols,
                    &table.col_types,
                )
                .map_err(|error| error.to_string())?,
            );
            Ok(())
        }
        JoinNode::Derived { .. } => Err(
            "automatic result metadata for derived relations requires planner schema".to_owned(),
        ),
        JoinNode::Join(join) => {
            collect_catalog_relation_schemas(&join.left, tables, default_db, schemas)?;
            if let Some(right) = &join.right {
                collect_catalog_relation_schemas(right, tables, default_db, schemas)?;
            }
            Ok(())
        }
    }
}

/// Rejects relation leaves whose schema is not carried by the flattened
/// INNER/CROSS automatic response path. Outer and coalescing trees are routed
/// through `join_output_metadata` before reaching this fallback.
fn reject_unsupported_automatic_join_shapes(join: &Join) -> Result<(), String> {
    reject_unsupported_automatic_join_node(&join.left)?;
    if let Some(right) = &join.right {
        reject_unsupported_automatic_join_node(right)?;
    }
    Ok(())
}

fn reject_unsupported_automatic_join_node(node: &JoinNode) -> Result<(), String> {
    match node {
        JoinNode::Join(join) => reject_unsupported_automatic_join_shapes(join),
        JoinNode::Table(_) => Ok(()),
        JoinNode::Derived { .. } => Err(
            "automatic result metadata for derived relations requires planner schema".to_owned(),
        ),
    }
}

fn format_datum_for_column(
    column: &tidb_protocol::ColumnInfo,
    datum: &tidb_datatype::Datum,
) -> Result<Option<Vec<u8>>, ExecError> {
    let text_column = tidb_protocol::TextColumn {
        type_code: column.type_code,
        flag: column.flag,
        decimal: column.decimal,
        table_is_empty: column.table.is_empty(),
    };
    let scalar = match datum {
        tidb_datatype::Datum::Null => tidb_protocol::TextScalar::Null,
        tidb_datatype::Datum::Int(value) => tidb_protocol::TextScalar::Signed(*value),
        tidb_datatype::Datum::UInt(value) => tidb_protocol::TextScalar::Unsigned(*value),
        tidb_datatype::Datum::Real(value) => tidb_protocol::TextScalar::Float {
            value: *value,
            bit_size: if column.type_code == tidb_protocol::TYPE_FLOAT {
                32
            } else {
                64
            },
        },
        tidb_datatype::Datum::Float32(value) => tidb_protocol::TextScalar::Float {
            value: *value,
            bit_size: 32,
        },
        tidb_datatype::Datum::String(value) => tidb_protocol::TextScalar::Bytes(value.bytes()),
        tidb_datatype::Datum::Bytes(value) => tidb_protocol::TextScalar::Bytes(value),
        tidb_datatype::Datum::Decimal(value) => {
            let rendered = value.to_string();
            return tidb_protocol::format_text_value(
                text_column,
                tidb_protocol::TextScalar::Decimal(rendered.as_bytes()),
            )
            .map_err(|error| ExecError::Protocol(error.to_string()));
        }
        tidb_datatype::Datum::MinNotNull | tidb_datatype::Datum::MaxValue => {
            return Err(ExecError::Protocol(
                "range sentinel cannot be materialized in a result row".to_owned(),
            ));
        }
        other => {
            let rendered = other
                .to_bytes()
                .map_err(|error| ExecError::Protocol(error.to_string()))?;
            return tidb_protocol::format_text_value(
                text_column,
                tidb_protocol::TextScalar::Bytes(&rendered),
            )
            .map_err(|error| ExecError::Protocol(error.to_string()));
        }
    };
    tidb_protocol::format_text_value(text_column, scalar)
        .map_err(|error| ExecError::Protocol(error.to_string()))
}

/// Classifies the top-level AST exactly once at the connection boundary.
/// Administrative/diagnostic forms remain `Unknown` until their source
/// status contract is ported; they still go through the same begin/finish
/// lifecycle and therefore cannot leak the prior statement's current fields.
fn statement_kind(stmt: &Stmt) -> StatementKind {
    match stmt {
        Stmt::Ddl(_) => StatementKind::Ddl,
        Stmt::Dml(_) => StatementKind::Dml,
        Stmt::Query(_) => StatementKind::Select,
        Stmt::Session(_) => StatementKind::Session,
        Stmt::Admin(_) => StatementKind::Unknown,
    }
}

/// Positive capability envelope for the temporary local-`Database` adapter.
/// Every accepted expression and join predicate is pure, and every accepted
/// table statement is a source-proven ordinary-table shape. This is intentionally much narrower
/// than `Database::run`: cloning that executor is shallow for several `Rc`
/// session cells, so sequences, auto IDs, user-variable assignment, RNG,
/// and other side effects must be rejected BEFORE a clone is evaluated.
fn shared_autocommit_capability(stmt: &Stmt) -> bool {
    if let Stmt::Session(session) = stmt {
        return match session.as_ref() {
            SessionStmt::Use(_) => true,
            SessionStmt::Set(set) => safe_shared_session_set(set),
            _ => false,
        };
    }
    matches!(
        stmt,
        Stmt::Ddl(ddl) if safe_shared_ddl(ddl)
    ) || matches!(
        stmt,
        Stmt::Dml(dml) if safe_shared_dml(dml)
    ) || matches!(
        stmt,
        Stmt::Query(query) if safe_shared_query(query)
    )
}

/// Admits only local SET values whose evaluation cannot mutate an `Rc`-backed
/// session cell before the named assignment owns that mutation. The warning
/// producer needs ordered lists of the three no-op variables; retry-limit
/// retains its earlier single-integer boundary. GLOBAL/INSTANCE, DEFAULT,
/// function calls, user/system-variable reads, assignments, and composite
/// expressions all stop here before `Database` execution.
fn safe_shared_session_set(set: &SetStmt) -> bool {
    if set.assignments.len() == 1 {
        let assignment = &set.assignments[0];
        if assignment.scope == SystemVariableScope::Session
            && assignment.name.eq_ignore_ascii_case("tidb_retry_limit")
        {
            return matches!(assignment.value, SetVariableValue::Expr(Expr::Int(_)));
        }
    }

    !set.assignments.is_empty()
        && set.assignments.iter().all(|assignment| {
            assignment.scope == SystemVariableScope::Session
                && match assignment.name.to_ascii_lowercase().as_str() {
                    "tidb_enable_noop_functions" => {
                        safe_shared_noop_functions_value(&assignment.value)
                    }
                    "tx_read_only" | "transaction_read_only" => matches!(
                        assignment.value,
                        SetVariableValue::Expr(Expr::Int(_) | Expr::String(_) | Expr::Bool(_))
                    ),
                    _ => false,
                }
        })
}

fn safe_shared_noop_functions_value(value: &SetVariableValue) -> bool {
    match value {
        SetVariableValue::Expr(Expr::Int(_) | Expr::String(_)) => true,
        // SET's parser intentionally keeps bare OFF/ON/WARN as one-component
        // identifier expressions. The runtime recognizes exactly these three
        // spellings as enum literals; every other Column remains a rejected
        // read rather than crossing the shared-session side-effect gate.
        SetVariableValue::Expr(Expr::Column(path)) => matches!(
            path.as_slice(),
            [name]
                if name.eq_ignore_ascii_case("off")
                    || name.eq_ignore_ascii_case("on")
                    || name.eq_ignore_ascii_case("warn")
        ),
        _ => false,
    }
}

fn safe_shared_ddl(ddl: &DdlStmt) -> bool {
    match ddl {
        DdlStmt::DropTable(_) => true,
        DdlStmt::CreateTable(table) => {
            table.temporary == CreateTableTemporary::None
                && !table.on_commit_delete
                && table.like_table.is_none()
                && table.table_options.is_empty()
                && table.partitioning.is_none()
                && table.ctas.is_none()
                && table.columns.iter().all(|column| {
                    column.options.iter().all(|option| {
                        option.is_inline_primary_key()
                            || option.is_inline_unique_key()
                            || matches!(option, ColumnOption::NotNull | ColumnOption::Null)
                    })
                })
                && table.table_constraints.iter().all(|constraint| {
                    matches!(
                        constraint,
                        TableConstraint::Index(index)
                            if matches!(
                                index.kind,
                                IndexConstraintKind::PrimaryKey
                                    | IndexConstraintKind::Unique
                                    | IndexConstraintKind::UniqueKey
                                    | IndexConstraintKind::UniqueIndex
                            ) && index.options == IndexOptions::default()
                    )
                })
        }
        _ => false,
    }
}

fn safe_shared_dml(dml: &DmlStmt) -> bool {
    match dml {
        DmlStmt::Insert(insert) => {
            insert.hints.is_empty()
                && !insert.ignore
                && insert.partitions.is_empty()
                && insert.set_columns.is_empty()
                && insert.source.is_none()
                && !insert.source_parenthesized
                && insert.on_duplicate.is_empty()
                && insert.row_alias.is_none()
                && insert.column_aliases.is_empty()
                && insert.returning.is_empty()
                && !insert.set_syntax
                && !insert.replace
                && insert.rows.iter().flatten().all(shared_pure_expr)
        }
        DmlStmt::Update(update) => {
            update.hints.is_empty()
                && !update.ignore
                && matches!(&update.kind, UpdateKind::Single(table) if safe_shared_table_ref(table))
                && update
                    .assignments
                    .iter()
                    .all(|assignment| shared_pure_expr(&assignment.value))
                && update.where_clause.as_ref().is_none_or(shared_pure_expr)
                && update.order_by.is_empty()
                && update.limit.is_none()
                && update.returning.is_empty()
        }
        DmlStmt::Delete(_) | DmlStmt::DistributeTable(_) => false,
        _ => false,
    }
}

fn safe_shared_query(query: &QueryStmt) -> bool {
    let QueryStmt::Select(select) = query else {
        return false;
    };
    if is_bounded_catalog_count_column_select(select) {
        return true;
    }
    select.kind == SelectStatementKind::Select
        && select.with.is_none()
        && select.hints.is_empty()
        && !select.calc_found_rows
        && !select.distinct
        && !select.all
        && select.values.is_empty()
        && select.fields.iter().all(|field| match field {
            SelectField::Expr { expr, .. } => {
                shared_pure_expr(expr)
                    || (select.from.is_none() && shared_tableless_count_expr(expr))
            }
            // Wildcards are safe only when a source relation owns their
            // expansion. The automatic metadata resolver supplies the same
            // catalog/planner-shaped field order before execution; a
            // table-less wildcard remains an explicit unsupported shape.
            SelectField::Wildcard(_) => select.from.is_some(),
        })
        && select.from.as_ref().is_none_or(safe_shared_join)
        && select.where_clause.as_ref().is_none_or(shared_pure_expr)
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select
            .order_by
            .iter()
            .all(|item| shared_pure_expr(&item.expr))
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
}

/// Dependency-closed table-less aggregate projection admitted by the
/// shared-session clone/retry boundary. `COUNT` is deterministic for immutable
/// arguments and its result type is source-defined without consulting a
/// runtime row. Keeping the aggregate at the top level prevents an apparently
/// pure wrapper from hiding a function, variable write, subquery, clock, or
/// RNG. The separate catalog COUNT-column predicate owns the only admitted
/// table-backed aggregate shape; aggregates are not generally classified as
/// pure here.
fn shared_tableless_count_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Aggregate { name, args, .. }
            if name.eq_ignore_ascii_case("COUNT") && args.iter().all(shared_pure_expr)
    )
}

fn safe_shared_table_ref(table: &tidb_ast::TableRef) -> bool {
    table.partitions.is_empty()
        && table.as_of.is_none()
        && table.hints.is_empty()
        && table.sample.is_none()
}

/// Positive capability envelope for relation trees whose predicates are pure
/// and whose output metadata can be derived from the catalog snapshot plus the
/// bounded planner join-output adapter. INNER/CROSS/LEFT/RIGHT and
/// USING/NATURAL joins are executable here; derived and straight joins remain
/// explicit boundaries.
fn safe_shared_join(join: &Join) -> bool {
    if join.straight {
        return false;
    }
    if join.right.is_none() {
        return safe_shared_join_node(&join.left) && join.on.is_none();
    }
    if !join.on.as_ref().is_none_or(shared_pure_expr) {
        return false;
    }
    safe_shared_join_node(&join.left) && join.right.as_ref().is_some_and(safe_shared_join_node)
}

fn safe_shared_join_node(node: &JoinNode) -> bool {
    match node {
        JoinNode::Table(table) => safe_shared_table_ref(table),
        JoinNode::Join(join) => safe_shared_join(join),
        JoinNode::Derived { .. } => false,
    }
}

/// The side-effect-free expression forms evaluated by the source worker.
/// Dedicated scalar predicates recurse through the same positive envelope as
/// binary expressions, so admitting `BETWEEN`/`IN`/`LIKE` cannot hide an
/// assignment, variable access, function call, or subquery in a nested
/// operand. Functions remain closed as a family because their determinism is
/// name-dependent (sequences, last-insert-ID, clocks, and RNG are all parsed
/// as function nodes).
fn shared_pure_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_)
        | Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::Null
        | Expr::Bool(_) => true,
        Expr::Unary(_, value) | Expr::Paren(value) => shared_pure_expr(value),
        Expr::Binary(_, left, right) => shared_pure_expr(left) && shared_pure_expr(right),
        Expr::Row(values) => values.iter().all(shared_pure_expr),
        Expr::In { expr, list, .. } => shared_pure_expr(expr) && list.iter().all(shared_pure_expr),
        Expr::Between {
            expr, low, high, ..
        } => shared_pure_expr(expr) && shared_pure_expr(low) && shared_pure_expr(high),
        Expr::Like { expr, pattern, .. } | Expr::Regexp { expr, pattern, .. } => {
            shared_pure_expr(expr) && shared_pure_expr(pattern)
        }
        Expr::Is { expr, .. }
        | Expr::Cast(tidb_ast::CastExpr { expr, .. })
        | Expr::ConvertUsing { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Extract { value: expr, .. } => shared_pure_expr(expr),
        Expr::Position { substr, str } => shared_pure_expr(substr) && shared_pure_expr(str),
        Expr::Trim { expr, remstr, .. } => {
            shared_pure_expr(expr) && remstr.as_deref().is_none_or(shared_pure_expr)
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_none_or(shared_pure_expr)
                && when_clauses.iter().all(|(condition, result)| {
                    shared_pure_expr(condition) && shared_pure_expr(result)
                })
                && else_clause.as_deref().is_none_or(shared_pure_expr)
        }
        _ => false,
    }
}
