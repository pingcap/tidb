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
//! `execute_write` hook. The statement kind is decided by parsing alone
//! ([`tidb_session::Session::statement_kind`]) so a write runs exactly once.
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
use tidb_session::{Session, SharedCatalog, StmtKind, StmtOutput, StmtResult};

use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};

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
}

impl PipelineSessionFactory {
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
        // An identity the handshake matched exists in Go's mysql.user by
        // construction, so SHOW GRANTS always finds at least USAGE for it.
        // The configured user store and the privilege registry are separate
        // structures here -- seed the account on first login so the same
        // holds (create_user is a no-op when it already exists).
        self.privileges
            .create_user(identity.username(), identity.host());
        session.session.attach_privileges(self.privileges.clone());
        Ok(session)
    }
}

impl QuerySession for PipelineServerSession {
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
        if self.session.apply_set(sql).map_err(map_error)?.is_some() {
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        if self.session.statement_kind(sql).map_err(map_error)? != StmtKind::Write {
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
                    // real values reports no columns at prepare time; the
                    // execute answer still carries its own metadata, which is
                    // where a client reads it.
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
        let params: Vec<tidb_datatype::Datum> = values
            .iter()
            // Go `ExecBinaryParam` builds one datum per parameter kind: a
            // signed width becomes an Int, an unsigned one a UInt, FLOAT and
            // DOUBLE their own real domains, DECIMAL is parsed from its
            // digits, and a NULL parameter is a NULL datum.
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
                        Ok(text) => tidb_datatype::Datum::Decimal(
                            tidb_datatype::Decimal::from_literal(text),
                        ),
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
            .collect();
        let output = self
            .session
            .run_with_params(statement.sql(), &params)
            .map_err(map_error)?;
        Ok(match output {
            StmtOutput::Rows { columns, rows } => {
                GeneralExecuteOutcome::Rows(QueryResult::new(Box::new(
                    MaterializedResultSetSource::new(select_columns(&columns), rows),
                )))
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
        Ok(QueryResult::new(Box::new(source)))
    }
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
fn select_columns(columns: &[(String, FieldType)]) -> Vec<ColumnInfo> {
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
fn affected_rows_source(count: u64) -> MaterializedResultSetSource {
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
    fn expression_fields_use_restored_text_names() {
        let mut session = open_session();
        let mut result = session.execute("SELECT 1 + 1").expect("dual select");
        let (columns, rows) = drain(&mut result, 8);
        assert_eq!(columns[0].name, "1+1");
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
        let Err(unsupported) = session.execute("CREATE SEQUENCE seq") else {
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

    /// Connections do not share a catalog (documented deferral): a table made
    /// by one session is invisible to a session from the same factory.
    #[test]
    fn sessions_are_catalog_isolated() {
        let mut first = open_session();
        first.execute("CREATE TABLE t (a BIGINT)").expect("create");
        let mut second = open_session();
        assert!(second.execute("SELECT a FROM t").is_err());
    }
}
