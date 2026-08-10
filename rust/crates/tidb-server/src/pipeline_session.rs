// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MySQL wire front for the new pipeline session (`tidb-session`).
//!
//! Adapts [`tidb_session::Session`] (parse -> plan -> execute over real
//! TiKV-format bytes) to the connection layer's [`QuerySession`] contract, so
//! the existing `COM_QUERY` text path can serve `CREATE TABLE` / `INSERT` /
//! `SELECT` through the pipeline.
//!
//! Write-result representation: `COM_QUERY` writes answer with a real OK
//! packet carrying their affected-row count, through the [`QuerySession`]
//! `execute_write` hook. The statement kind is decided before execution
//! ([`tidb_session::Session::statement_kind`]) so a write runs exactly once --
//! from the parse for every statement but `EXECUTE`, whose shape is that of
//! the statement it names.
//!
//! Catalog sharing: every session a [`PipelineSessionFactory`] opens shares
//! one `Arc<Mutex<Catalog>>`, as TiDB's sessions read the domain-owned
//! `infoschema` rather than private copies -- a table created on one
//! connection is visible on the others. The per-statement lock stands in for
//! Go's schema-version/lease machinery, which is a separate tier (deferred).
//!
//! Prepared statements and transaction control keep the trait's fail-closed
//! defaults.

use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_exec::{convert_result_field, ResultFieldMetadata, ResultFieldTypeMetadata};
use tidb_protocol::ColumnInfo;
use tidb_session::privilege::PrivilegeRegistry;
use tidb_session::process::ProcessRegistry;
use tidb_session::{GlobalSysvars, Session, SharedCatalog, StmtKind, StmtOutput, StmtResult};

use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};
use crate::wire_status::WireStatus;

/// One connection's pipeline-backed query session.
pub struct PipelineServerSession {
    session: Session,
}

impl PipelineServerSession {
    /// Creates a session over a fresh, empty pipeline catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: Session::new(),
        }
    }

    /// Creates a session over `catalog`, which its peers share.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Self {
            session: Session::with_catalog(catalog),
        }
    }
}

impl Default for PipelineServerSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Opens one pipeline [`Session`] per authenticated connection.
///
/// Every connection the factory opens shares one catalog, as the sessions of a
/// TiDB instance share the domain's schema state: a table one connection
/// creates is immediately visible to the others.
#[derive(Default)]
pub struct PipelineSessionFactory {
    catalog: SharedCatalog,
    /// The live connection list `SHOW PROCESSLIST` reads and `KILL` reaches
    /// into, shared by every connection this factory opens -- Go's one
    /// `sessmgr.Manager` per TiDB instance.
    processes: ProcessRegistry,
    /// The account/global-privilege registry every connection this factory
    /// opens shares -- Go's one `privilege.Manager` per `Domain`. Bootstraps
    /// with `root`@`%` holding every privilege, as Go's `mysql.user` table
    /// does on a fresh cluster.
    privileges: PrivilegeRegistry,
    /// The `SET GLOBAL`-scope sysvar table every connection this factory
    /// opens shares -- Go's one process-wide `GlobalVarsAccessor`. See
    /// [`tidb_session::vars`] for what "shares" means here: a new session
    /// snapshots this table's current overrides at open, but a live
    /// session's own `@@x` never moves just because another connection ran
    /// `SET GLOBAL`.
    global_vars: GlobalSysvars,
}

impl PipelineSessionFactory {
    /// Builds a factory over an EXISTING account table -- normally
    /// `ConfiguredUserStore::accounts()`, so that the accounts a connection
    /// can authenticate as and the accounts `CREATE USER`/`GRANT`/`DROP
    /// USER` manipulate are one set of rows. `Default` instead starts from a
    /// fresh table bootstrapped with `root`@`%`, for in-process sessions
    /// that have no wire front end.
    #[must_use]
    pub fn with_accounts(accounts: PrivilegeRegistry) -> Self {
        Self {
            privileges: accounts,
            ..Self::default()
        }
    }

    /// Builds a factory over an existing account table AND an existing
    /// GLOBAL-scope sysvar table -- the pairing [`ConfiguredUserStore`]
    /// needs so `SET GLOBAL default_password_lifetime` and the login path
    /// that reads it agree on one process-wide value.
    ///
    /// [`ConfiguredUserStore`]: crate::configured_user_store::ConfiguredUserStore
    #[must_use]
    pub fn with_accounts_and_globals(
        accounts: PrivilegeRegistry,
        global_vars: GlobalSysvars,
    ) -> Self {
        Self {
            privileges: accounts,
            global_vars,
            ..Self::default()
        }
    }

    /// The process list of every connection this factory has open.
    #[must_use]
    pub fn processes(&self) -> ProcessRegistry {
        self.processes.clone()
    }

    /// The account/global-privilege registry every connection this factory
    /// opens shares.
    #[must_use]
    pub fn privileges(&self) -> PrivilegeRegistry {
        self.privileges.clone()
    }

    /// The `SET GLOBAL`-scope sysvar table every connection this factory
    /// opens shares.
    #[must_use]
    pub fn global_vars(&self) -> GlobalSysvars {
        self.global_vars.clone()
    }
}

impl QuerySessionFactory for PipelineSessionFactory {
    type Session = PipelineServerSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        let mut session = PipelineServerSession::with_catalog(Arc::clone(&self.catalog));
        // Go sets `SessionVars.User` from the identity the handshake matched:
        // `CURRENT_USER()` reports that matched grant identity and `USER()`
        // the host the client actually connected from.
        let identity = &context.identity;
        session.session.set_user(
            format!("{}@{}", identity.username(), identity.host()),
            format!("{}@{}", identity.username(), context.peer_addr.ip()),
        );
        // Go sets `SessionVars.ConnectionID` from the connection the front
        // end accepted; `CONNECTION_ID()` reads it back. This was once
        // dropped entirely when `SessionContext` was threaded through here --
        // double-check it actually arrives (see the TCP-level test below).
        session.session.set_connection_id(context.connection_id);
        // Go's `session.Auth` calls `EnableSandBoxMode()` when
        // `ConnectionVerification` admitted an expired password, which
        // restricts this connection to the statement that fixes it.
        if identity.in_sandbox_mode() {
            session.session.enable_sandbox_mode();
        }
        // Go registers the connection with the session manager right after
        // authentication, which is what puts it in `SHOW PROCESSLIST` and
        // makes it reachable by `KILL`. The registration is owned by the
        // session, so the row disappears exactly when the connection ends.
        let guard = self.processes.register(
            context.connection_id,
            identity.username().to_owned(),
            context.peer_addr.to_string(),
            session.session.current_database().to_owned(),
            Some(Arc::new(ConnectionKillTarget::new(
                context.cancellation.clone(),
                context.close.clone(),
            ))),
        );
        session.session.attach_process(context.connection_id, guard);
        // No seeding here: the identity the handshake matched IS a row in
        // this same registry (`ConfiguredUserStore` verifies logins against
        // it), exactly as Go's one `mysql.user` holds both the
        // authentication and the privilege columns. `SHOW GRANTS` therefore
        // always finds at least USAGE for an authenticated session.
        session.session.attach_privileges(self.privileges.clone());
        // Snapshots this factory's current GLOBAL-scope overrides into the
        // new session's own copy -- Go's rule that a session's variables are
        // copied from the global tier once, at connect (see
        // `tidb_session::vars` for why a live session's `@@x` does not move
        // when a later `SET GLOBAL` runs on another connection).
        session.session.attach_globals(self.global_vars.clone());
        Ok(session)
    }
}

impl QuerySession for PipelineServerSession {
    /// The live status word Go reads with `cc.ctx.Status()` before every
    /// OK/EOF packet: this session owns a real transaction and a real
    /// `autocommit` variable, so both bits come from it rather than from a
    /// connection-lifetime constant.
    fn wire_status(&self) -> WireStatus {
        WireStatus::of_session(&self.session)
    }

    /// The count Go's `writeOkWith`/`writeEOF` read off `ctx.WarningCount()`.
    /// It is the same buffer `SHOW WARNINGS` reports, so both channels agree
    /// on both what warned and how many.
    fn warning_count(&self) -> u16 {
        self.session.wire_warning_count()
    }

    /// Go `clientConn.initResultEncoder`'s read: this session's
    /// `@@character_set_results`.
    fn result_charset(&self) -> String {
        self.session.result_charset()
    }

    /// The handshake's initial database and `COM_INIT_DB`, which Go serves
    /// with one `useDB` each.
    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.session.select_database(name).map_err(map_error)
    }

    /// BEGIN/COMMIT/ROLLBACK drive the session's transaction, and the caller
    /// answers with an OK packet whose status carries the returned flag.
    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        self.session.control_transaction(sql).map_err(map_error)
    }

    /// Writes and DDL answer with an OK packet carrying their affected-row
    /// count; the statement kind is decided by parsing alone so the statement
    /// runs exactly once.
    ///
    /// A `SET` statement answers the same way, which is what a connecting
    /// client expects for `SET NAMES` and friends.
    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // Both questions below are answered off ONE parse: whether this is a
        // `SET` to apply, and -- if it is not -- what shape its answer takes.
        // They remain two questions with two answers; only the lexing is
        // shared, and the session still owns it so the `sql_mode` in force is
        // the session's own.
        //
        // This is the front end's statement boundary, and it must be the
        // session's own: the `SET` arm below answers without ever reaching
        // `Session::run`, so a boundary that lived only there would leave a
        // `SET` appending its warnings to the PREVIOUS statement's buffer --
        // and reporting that statement's count on the wire.
        let stmt = self
            .session
            .parse_at_statement_boundary(sql)
            .map_err(map_error)?;
        if self
            .session
            .apply_set_stmt(&stmt)
            .map_err(map_error)?
            .is_some()
        {
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        if self.session.statement_kind_parsed(&stmt) != StmtKind::Write {
            return Ok(None);
        }
        let affected_rows = match self.session.run(sql).map_err(map_error)? {
            StmtResult::Affected(count) => count,
            // DDL affects zero rows, exactly as MySQL's OK packet reports.
            StmtResult::Done(_) => 0,
            StmtResult::Rows(_) => {
                return Err(SqlQueryError::unknown(
                    "a write statement unexpectedly produced rows",
                ))
            }
        };
        Ok(Some(WriteOutcome {
            affected_rows,
            last_insert_id: self.session.statement_insert_id(),
        }))
    }

    /// Go reports a prepared statement's marker count and result columns at
    /// PREPARE time. The columns come from planning the statement with every
    /// marker bound to NULL, which is side-effect free for a query; a
    /// statement that answers with an OK packet reports none.
    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        let parameter_count = self.session.parameter_count(sql).map_err(map_error)?;
        let result_columns = match self.session.statement_kind(sql).map_err(map_error)? {
            StmtKind::Query => {
                let probe: Vec<tidb_datatype::Datum> =
                    std::iter::repeat_n(tidb_datatype::Datum::Null, parameter_count).collect();
                match self.session.run_with_params(sql, &probe) {
                    Ok(StmtOutput::Rows { columns, .. }) => select_columns(&columns),
                    // A query whose metadata this tier cannot resolve without
                    // real values reports no columns at prepare time. That is
                    // a LAST resort and not a free one: a MySQL client frames
                    // the EXECUTE answer against this count, so zero here and
                    // a result set there is what it reports as
                    // `2014 Commands out of sync`. The probe must therefore be
                    // made to succeed for any shape a client will execute --
                    // see `prepared_handle_range_frames_its_binary_result_set`
                    // in `pipeline_mysql_client_source`, which pins both ends.
                    _ => Vec::new(),
                }
            }
            StmtKind::Write => Vec::new(),
        };
        Ok(PreparedGeneral::new(
            sql.to_owned(),
            parameter_count,
            result_columns,
        ))
    }

    fn execute_general<'a>(
        &'a mut self,
        statement: &PreparedGeneral,
        values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        let params = prepared_parameters(values);
        let (output, result_authority) = self
            .session
            .run_with_params_and_result_authority(statement.sql(), &params)
            .map_err(map_error)?;
        Ok(match output {
            StmtOutput::Rows { columns, rows } => {
                let field_types = columns.iter().map(|(_, field)| field.clone()).collect();
                GeneralExecuteOutcome::Rows(
                    QueryResult::new(Box::new(MaterializedResultSetSource::new(
                        select_columns(&columns),
                        rows,
                    )))
                    .with_cursor_materialization(
                        field_types,
                        result_authority.expect("a row result carries materialization authority"),
                    )
                    .with_statement_status(
                        self.session.wire_warning_count(),
                        WireStatus::of_session(&self.session),
                    ),
                )
            }
            StmtOutput::Affected(count) => GeneralExecuteOutcome::Write(WriteOutcome {
                affected_rows: count,
                last_insert_id: self.session.statement_insert_id(),
            }),
            StmtOutput::Done(_) => GeneralExecuteOutcome::Write(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }),
        })
    }

    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let source = match self.session.run_with_columns(sql).map_err(map_error)? {
            StmtOutput::Rows { columns, rows } => {
                MaterializedResultSetSource::new(select_columns(&columns), rows)
            }
            // Writes normally answer through `execute_write` (a real OK
            // packet). These arms remain for a caller that invokes `execute`
            // directly, which the trait permits: report the count as a
            // one-column result set rather than an invalid zero-column one.
            StmtOutput::Affected(count) => affected_rows_source(count),
            StmtOutput::Done(_) => affected_rows_source(0),
        };
        // The rows are already materialized, so the buffer this reads is the
        // finished statement's -- the same one Go's terminal `writeEOF` reads.
        Ok(QueryResult::new(Box::new(source)).with_statement_status(
            self.session.wire_warning_count(),
            WireStatus::of_session(&self.session),
        ))
    }
}

/// Binds one `COM_STMT_EXECUTE` parameter list to the driver's datum domain.
///
/// Go `ExecBinaryParam` builds one datum per parameter kind: a signed width
/// becomes an Int, an unsigned one a UInt, FLOAT and DOUBLE their own real
/// domains, DECIMAL is parsed from its digits, and a NULL parameter is a NULL
/// datum.
pub(crate) fn prepared_parameters(
    values: &[tidb_protocol::PreparedValue],
) -> Vec<tidb_datatype::Datum> {
    values
        .iter()
        .map(|value| match value {
            tidb_protocol::PreparedValue::SignedLongLong(value) => {
                tidb_datatype::Datum::Int(*value)
            }
            tidb_protocol::PreparedValue::UnsignedLongLong(value) => {
                tidb_datatype::Datum::UInt(*value)
            }
            tidb_protocol::PreparedValue::String(bytes) => {
                tidb_datatype::Datum::Bytes(bytes.clone())
            }
            tidb_protocol::PreparedValue::Float(value) => {
                tidb_datatype::Datum::Float32(f64::from(*value))
            }
            tidb_protocol::PreparedValue::Double(value) => tidb_datatype::Datum::Real(*value),
            tidb_protocol::PreparedValue::Decimal(digits) => {
                match std::str::from_utf8(digits) {
                    Ok(text) => {
                        tidb_datatype::Datum::Decimal(tidb_datatype::Decimal::from_literal(text))
                    }
                    // Go's own FromString reports truncation and keeps
                    // what it read; digits that are not even text cannot
                    // be a number at all.
                    Err(_) => tidb_datatype::Datum::Null,
                }
            }
            tidb_protocol::PreparedValue::Null => tidb_datatype::Datum::Null,
            // Go parses the rendered text into a Time or Duration datum.
            // This tier keeps temporal values as their formatted text --
            // the same documented divergence the temporal casts and the
            // date/time builtins carry -- so the text IS the value here.
            tidb_protocol::PreparedValue::Temporal(text) => {
                tidb_datatype::Datum::Bytes(text.clone().into_bytes())
            }
        })
        .collect()
}

fn map_error(error: tidb_executor::DriverError) -> SqlQueryError {
    let mapped = error.to_mysql_error();
    SqlQueryError::new(mapped.code, mapped.state, mapped.message)
}

/// Converts the pipeline's `(name, FieldType)` output schema to protocol
/// column metadata through the source-shaped `ConvertColumnInfo` port.
///
/// Simplification (documented): the seed driver does not yet resolve original
/// schema/table/column names for the wire, so those identifier fields are
/// empty and `org_name` mirrors the display name, as Go does for expression
/// result fields.
pub(crate) fn select_columns(columns: &[(String, FieldType)]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|(name, field_type)| {
            convert_result_field(&ResultFieldMetadata {
                schema: String::new(),
                table: String::new(),
                org_table: String::new(),
                name: name.clone(),
                org_name: name.clone(),
                empty_org_name: false,
                default_value: None,
                field_type: result_field_type(field_type),
            })
        })
        .collect()
}

fn result_field_type(field_type: &FieldType) -> ResultFieldTypeMetadata {
    ResultFieldTypeMetadata {
        code: field_type.code(),
        // Only the low 16 bits are MySQL wire column flags.
        flags: (field_type.flags() & u32::from(u16::MAX)) as u16,
        flen: length_to_option(field_type.flen()).map(|flen| flen as u32),
        decimal: length_to_option(field_type.decimal()).map(|decimal| decimal as u8),
        collation: field_type.collation(),
    }
}

const fn length_to_option(length: i64) -> Option<i64> {
    if length == UNSPECIFIED_LENGTH {
        None
    } else {
        Some(length)
    }
}

/// The one-column `affected_rows` result set that carries a write outcome on
/// the text path (see the module documentation for why not an OK packet).
pub(crate) fn affected_rows_source(count: u64) -> MaterializedResultSetSource {
    let field_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
    MaterializedResultSetSource::new(
        select_columns(&[("affected_rows".to_owned(), field_type)]),
        vec![vec![Datum::UInt(count)]],
    )
}

/// A fully materialized result-set source: the pipeline returns complete row
/// vectors, so batching only replays them to the incremental wire writer.
pub struct MaterializedResultSetSource {
    columns: Vec<ColumnInfo>,
    rows: std::vec::IntoIter<Vec<Datum>>,
}

impl MaterializedResultSetSource {
    /// Wraps already-computed columns and rows.
    #[must_use]
    pub fn new(columns: Vec<ColumnInfo>, rows: Vec<Vec<Datum>>) -> Self {
        Self {
            columns,
            rows: rows.into_iter(),
        }
    }
}

impl ResultSetSource for MaterializedResultSetSource {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        Ok(self.rows.by_ref().take(max_rows.max(1)).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(self.columns.clone())
    }

    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configured_user_store::ConfiguredUserStore;
    use crate::sql_node::ConnectionCancellation;
    use crate::wire_status::{SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS};
    use sha1::{Digest, Sha1};
    use std::net::SocketAddr;
    use tidb_protocol::{TYPE_LONGLONG, TYPE_VAR_STRING};

    const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
    const SALT: [u8; 20] = [7; 20];

    fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
        let stage_one = Sha1::digest(password);
        let stage_two = Sha1::digest(stage_one);
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(stage_two);
        let challenge = hasher.finalize();
        let mut response = [0; 20];
        for ((destination, stage_one), challenge) in response
            .iter_mut()
            .zip(stage_one.iter())
            .zip(challenge.iter())
        {
            *destination = stage_one ^ challenge;
        }
        response
    }

    fn open_session() -> PipelineServerSession {
        open_on(&PipelineSessionFactory::default(), 1)
    }

    fn open_on(factory: &PipelineSessionFactory, connection_id: u64) -> PipelineServerSession {
        let users =
            ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
                .expect("configured user store");
        let identity = users
            .authenticate_native("root", "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
            .expect("authenticated identity");
        let peer_addr: SocketAddr = "127.0.0.1:4000".parse().expect("peer address");
        factory
            .open_session(SessionContext {
                connection_id,
                peer_addr,
                identity,
                cancellation: ConnectionCancellation::default(),
                close: crate::sql_node::ConnectionClose::default(),
            })
            .expect("pipeline session opens without process authorities")
    }

    /// Go's sessions read the instance-wide schema state, so a table created
    /// on one connection is visible on every other one. The factory hands each
    /// session the same catalog, which is what makes that true here.
    #[test]
    fn a_table_created_on_one_connection_is_visible_on_another() {
        let factory = PipelineSessionFactory::default();
        let mut writer = open_on(&factory, 1);
        let mut reader = open_on(&factory, 2);

        writer
            .execute_write("CREATE TABLE t (a BIGINT)")
            .expect("create table succeeds");
        writer
            .execute_write("INSERT INTO t VALUES (7)")
            .expect("insert succeeds");

        let mut result = reader
            .execute("SELECT a FROM t")
            .expect("the peer session sees the table");
        let (_, rows) = drain(&mut result, 8);
        assert_eq!(rows, vec![vec![Datum::Int(7)]]);
    }

    fn drain(result: &mut QueryResult<'_>, batch: usize) -> (Vec<ColumnInfo>, Vec<Vec<Datum>>) {
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch_rows = source.next_batch(batch).expect("batch");
            if batch_rows.is_empty() {
                break;
            }
            rows.extend(batch_rows);
        }
        let columns = source.columns().expect("columns");
        source.finish().expect("finish");
        source.close().expect("close");
        (columns, rows)
    }

    /// A whole CREATE/INSERT/SELECT lifecycle through the QuerySession seam.
    #[test]
    fn text_path_serves_the_pipeline_lifecycle() {
        let mut session = open_session();

        // CREATE TABLE takes the supported write representation: a one-column
        // `affected_rows` result set reporting zero affected rows.
        let mut result = session
            .execute("CREATE TABLE t (a BIGINT, b BIGINT)")
            .expect("create table");
        let (columns, rows) = drain(&mut result, 2);
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "affected_rows");
        assert_eq!(columns[0].type_code, TYPE_LONGLONG);
        assert_eq!(rows, vec![vec![Datum::UInt(0)]]);
        drop(result);

        // INSERT reports its inserted-row count on the same path.
        let mut result = session
            .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .expect("insert");
        let (columns, rows) = drain(&mut result, 2);
        assert_eq!(columns[0].name, "affected_rows");
        assert_eq!(rows, vec![vec![Datum::UInt(3)]]);
        drop(result);

        // SELECT streams the pipeline rows with converted column metadata.
        let mut result = session
            .execute("SELECT a, a + b AS total FROM t WHERE a >= 2 ORDER BY a DESC")
            .expect("select");
        let (columns, rows) = drain(&mut result, 1);
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["a", "total"]
        );
        assert!(columns
            .iter()
            .all(|column| column.type_code == TYPE_LONGLONG));
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(3), Datum::Int(33)],
                vec![Datum::Int(2), Datum::Int(22)],
            ]
        );
    }

    #[test]
    fn expression_fields_use_their_written_text_as_the_name() {
        // Go names an unaliased field after `SelectField.Text`, the
        // ORIGINAL SQL bytes, not the AST's normalized/restored form --
        // `1 + 1` (with the written spacing) rather than `1+1`.
        let mut session = open_session();
        let mut result = session.execute("SELECT 1 + 1").expect("dual select");
        let (columns, rows) = drain(&mut result, 8);
        assert_eq!(columns[0].name, "1 + 1");
        assert_eq!(rows, vec![vec![Datum::Int(2)]]);
    }

    #[test]
    fn string_columns_convert_to_var_string_metadata() {
        let mut session = open_session();
        session
            .execute("CREATE TABLE s (v VARCHAR(16))")
            .expect("create table");
        let mut result = session.execute("SELECT v FROM s").expect("select");
        let (columns, rows) = drain(&mut result, 4);
        // Go's ConvertColumnInfo remaps VARCHAR to VAR_STRING for old clients.
        assert_eq!(columns[0].type_code, TYPE_VAR_STRING);
        assert!(rows.is_empty());
    }

    #[test]
    fn errors_map_to_sql_query_errors() {
        let mut session = open_session();
        let Err(parse) = session.execute("SELEC 1") else {
            panic!("a parse error must not produce a result");
        };
        assert_eq!(parse.code, 1064);
        assert_eq!(&parse.state, b"42000");

        // A statement kind that is unsupported regardless of catalog state,
        // so this assertion does not quietly become an unknown-table one.
        // (This was `CREATE SEQUENCE seq` until sequences were transcreated --
        // the assertion needs a kind that really is outside the domain.)
        let Err(unsupported) = session.execute("FLASHBACK TABLE t") else {
            panic!("an unsupported statement must not produce a result");
        };
        assert_eq!(unsupported.code, 1105);

        // DROP TABLE is supported, and a missing name is Go's own 1051 rather
        // than the 1146 a read of a missing table reports.
        let Err(bad_table) = session.execute_write("DROP TABLE nosuch") else {
            panic!("dropping a missing table must fail");
        };
        assert_eq!(bad_table.code, 1051);
        assert_eq!(&bad_table.state, b"42S02");

        // Prepared statements keep the trait's fail-closed defaults.
        assert!(session.prepare_point_read("SELECT 1").is_err());
        assert!(session.prepare_write("INSERT INTO t VALUES (1)").is_err());
        // Transaction control is claimed by the hook and reports the state.
        assert_eq!(session.control_transaction("BEGIN").unwrap(), Some(true));
        assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
        assert_eq!(session.control_transaction("SELECT 1").unwrap(), None);
    }

    /// A SQL-level `EXECUTE` of a prepared SELECT answers with ROWS, and the
    /// front end has to frame it as one.
    ///
    /// Found while sharing the parse across the pre-passes: `statement_kind`
    /// classifies every `Stmt::Session` as `Write` -- true of `USE`, `SET` and
    /// the transaction controls, but `EXECUTE` is a `Stmt::Session` whose
    /// answer is whatever the PREPARED statement answers. `execute_write`
    /// therefore claimed it, ran it, got rows back, and reported the internal
    /// "a write statement unexpectedly produced rows" instead of the result
    /// set. The shape of an `EXECUTE` is not decidable from its own parse, so
    /// the write path must not claim it.
    #[test]
    fn a_sql_level_execute_of_a_select_answers_with_rows() {
        let mut session = open_session();
        session
            .execute_write("PREPARE p FROM 'SELECT 1'")
            .expect("prepare answers with an OK packet");
        assert!(
            session
                .execute_write("EXECUTE p")
                .expect("execute")
                .is_none(),
            "EXECUTE must fall through to the result-set path"
        );
        let mut result = session.execute("EXECUTE p").expect("execute answers rows");
        assert_eq!(result.source().columns().expect("columns").len(), 1);
    }

    /// Connections do not share a catalog (documented deferral): a table made
    /// by one session is invisible to a session from the same factory.
    #[test]
    fn sessions_are_catalog_isolated() {
        let mut first = open_session();
        first.execute("CREATE TABLE t (a BIGINT)").expect("create");
        let mut second = open_session();
        assert!(second.execute("SELECT a FROM t").is_err());
    }

    /// Go reads `cc.ctx.Status()` afresh for every OK packet, so a DML inside
    /// an explicit transaction reports `SERVER_STATUS_IN_TRANS |
    /// SERVER_STATUS_AUTOCOMMIT` (0x0003), not the bare autocommit word.
    ///
    /// Connector/J with `useLocalTransactionState=true` acts on exactly this
    /// bit: told 0x0002 by the INSERT's OK packet, it concludes no transaction
    /// is open and never sends the COMMIT, and the writes are lost. The word is
    /// pinned per statement here because the wire is where the client reads it.
    #[test]
    fn begin_dml_commit_pins_the_status_word_of_every_ok_packet() {
        let mut session = open_session();
        session
            .execute_write("CREATE TABLE t (a BIGINT)")
            .expect("create table");
        assert_eq!(
            session.wire_status().bits(),
            SERVER_STATUS_AUTOCOMMIT,
            "outside a transaction: autocommit only"
        );

        assert_eq!(
            session.control_transaction("BEGIN").expect("begin"),
            Some(true)
        );
        assert_eq!(
            session.wire_status().bits(),
            SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS,
            "BEGIN opens the transaction (Go SetInTxn(true), isolation/base.go:114)"
        );

        session
            .execute_write("INSERT INTO t VALUES (7)")
            .expect("insert inside the transaction");
        assert_eq!(
            session.wire_status().bits(),
            SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS,
            "the DML's own OK packet still reports the open transaction"
        );

        // The result set framing a SELECT inside the transaction carries the
        // same word on its EOF, snapshotted with the statement.
        let result = session.execute("SELECT a FROM t").expect("select");
        assert_eq!(
            result.wire_status().bits(),
            SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS
        );
        drop(result);

        assert_eq!(
            session.control_transaction("COMMIT").expect("commit"),
            Some(false)
        );
        assert_eq!(
            session.wire_status().bits(),
            SERVER_STATUS_AUTOCOMMIT,
            "COMMIT clears IN_TRANS (Go executor/simple.go:792)"
        );
    }

    /// `SET autocommit = 0` clears `SERVER_STATUS_AUTOCOMMIT` itself -- Go's
    /// sysvar hook is `s.SetStatusFlag(mysql.ServerStatusAutocommit,
    /// isAutocommit)` (`pkg/sessionctx/variable/sysvar.go:2123`) -- so the SET's
    /// own OK packet carries 0x0000. The next data statement then LAZILY opens a
    /// transaction (`pkg/sessiontxn/isolation/base.go:323`, guarded by
    /// `!sessVars.IsAutocommit()`), so its OK packet carries 0x0001: in a
    /// transaction, not in autocommit.
    #[test]
    fn set_autocommit_zero_clears_the_autocommit_bit_and_the_next_dml_opens_a_transaction() {
        let mut session = open_session();
        session
            .execute_write("CREATE TABLE t (a BIGINT)")
            .expect("create table");

        session
            .execute_write("SET autocommit = 0")
            .expect("set autocommit");
        assert_eq!(
            session.wire_status().bits(),
            0,
            "SET autocommit=0 clears the autocommit bit and opens nothing yet"
        );

        session
            .execute_write("INSERT INTO t VALUES (7)")
            .expect("insert with autocommit off");
        assert_eq!(
            session.wire_status().bits(),
            SERVER_STATUS_IN_TRANS,
            "the lazy transaction is open, and autocommit is still off"
        );

        assert_eq!(
            session.control_transaction("COMMIT").expect("commit"),
            Some(false)
        );
        assert_eq!(
            session.wire_status().bits(),
            0,
            "COMMIT ends the transaction; autocommit stays off until it is SET back"
        );
    }
}
