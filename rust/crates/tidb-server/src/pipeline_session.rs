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
use tidb_session::{Session, SharedCatalog, StmtKind, StmtOutput, StmtResult};

use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    QueryResult, QuerySession, QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};

/// MySQL `ER_PARSE_ERROR`.
const ER_PARSE_ERROR: u16 = 1064;

/// TiDB `ErrWriteConflict` (`pkg/errno`: 9007), whose SQL state is the
/// generic `HY000` TiDB uses for its own KV errors.
const ER_WRITE_CONFLICT: u16 = 9007;

/// MySQL `ER_DB_CREATE_EXISTS` (1007).
const ER_DB_CREATE_EXISTS: u16 = 1007;

/// MySQL `ER_NO_DB_ERROR` (1046).
const ER_NO_DB_ERROR: u16 = 1046;

/// MySQL `ER_BAD_DB_ERROR` (1049).
const ER_BAD_DB_ERROR: u16 = 1049;

/// MySQL `ER_UNKNOWN_SYSTEM_VARIABLE` (1193).
const ER_UNKNOWN_SYSTEM_VARIABLE: u16 = 1193;

/// MySQL `ER_INCORRECT_GLOBAL_LOCAL_VAR` (1238).
const ER_INCORRECT_GLOBAL_LOCAL_VAR: u16 = 1238;

/// MySQL `ER_SUBQUERY_NO_1_ROW` (1242), whose SQL state is `21000`.
const ER_SUBQUERY_NO_1_ROW: u16 = 1242;

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
}

impl QuerySessionFactory for PipelineSessionFactory {
    type Session = PipelineServerSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(PipelineServerSession::with_catalog(Arc::clone(
            &self.catalog,
        )))
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
    match error {
        tidb_executor::DriverError::Parse(message) => SqlQueryError::new(
            ER_PARSE_ERROR,
            *b"42000",
            format!("You have an error in your SQL syntax: {message}"),
        ),
        tidb_executor::DriverError::Unsupported(message) => SqlQueryError::unknown(message),
        tidb_executor::DriverError::Exec(error) => SqlQueryError::unknown(format!("{error:?}")),
        tidb_executor::DriverError::Txn(tidb_executor::TxnErrorKind::WriteConflict) => {
            SqlQueryError::new(
                ER_WRITE_CONFLICT,
                *b"HY000",
                "Write conflict, please retry the transaction".to_owned(),
            )
        }
        // Go: "The used SELECT statements have a different number of columns".
        tidb_executor::DriverError::WrongNumberOfColumnsInSelect => SqlQueryError::new(
            1222,
            *b"21000",
            "The used SELECT statements have a different number of columns".to_owned(),
        ),
        // Go: "Incorrect table definition; there can be only one auto column
        // and it must be defined as a key".
        tidb_executor::DriverError::WrongAutoKey => SqlQueryError::new(
            1075,
            *b"42000",
            "Incorrect table definition; there can be only one auto column and it must be defined as a key".to_owned(),
        ),
        // Go: "Incorrect column specifier for column '%-.192s'".
        tidb_executor::DriverError::WrongColumnSpecifier(name) => SqlQueryError::new(
            1063,
            *b"42000",
            format!("Incorrect column specifier for column '{name}'"),
        ),
        // Go: "Column '%-.192s' cannot be null".
        tidb_executor::DriverError::ColumnCannotBeNull(name) => {
            SqlQueryError::new(1048, *b"23000", format!("Column '{name}' cannot be null"))
        }
        // Go: "Field '%-.192s' doesn't have a default value".
        tidb_executor::DriverError::NoDefaultForField(name) => SqlQueryError::new(
            1364,
            *b"HY000",
            format!("Field '{name}' doesn't have a default value"),
        ),
        // Go: "Duplicate entry '%-.64s' for key '%-.192s'".
        tidb_executor::DriverError::DuplicateEntry { value, key } => SqlQueryError::new(
            1062,
            *b"23000",
            format!("Duplicate entry '{value}' for key '{key}'"),
        ),
        // Go: "Unknown table '%-.129s'" -- DROP TABLE's own code, distinct
        // from the 1146 a read of a missing table reports.
        tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::BadTable(name)) => {
            SqlQueryError::new(1051, *b"42S02", format!("Unknown table '{name}'"))
        }
        // Go: "Table '%-.192s' doesn't exist".
        tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::UnknownTable(name)) => {
            SqlQueryError::new(1146, *b"42S02", format!("Table '{name}' doesn't exist"))
        }
        // Go: "Unknown database '%-.192s'".
        tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::UnknownDatabase(
            name,
        )) => SqlQueryError::new(
            ER_BAD_DB_ERROR,
            *b"42000",
            format!("Unknown database '{name}'"),
        ),
        // Go: "Can't create database '%-.192s'; database exists".
        tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::DatabaseExists(
            name,
        )) => SqlQueryError::new(
            ER_DB_CREATE_EXISTS,
            *b"HY000",
            format!("Can't create database '{name}'; database exists"),
        ),
        // Go: "No database selected".
        tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::NoDatabaseSelected) => {
            SqlQueryError::new(ER_NO_DB_ERROR, *b"3D000", "No database selected".to_owned())
        }
        // Go: "Incorrect argument type to variable '%-.64s'".
        tidb_executor::DriverError::Var(tidb_executor::VarErrorKind::WrongTypeForVar(name)) => {
            SqlQueryError::new(
                1232,
                *b"42000",
                format!("Incorrect argument type to variable '{name}'"),
            )
        }
        // Go: "Variable '%-.64s' can't be set to the value of '%-.200s'".
        tidb_executor::DriverError::Var(tidb_executor::VarErrorKind::WrongValueForVar(
            name,
            value,
        )) => SqlQueryError::new(
            1231,
            *b"42000",
            format!("Variable '{name}' can't be set to the value of '{value}'"),
        ),
        // Go: "Unknown system variable '%-.64s'".
        tidb_executor::DriverError::Var(tidb_executor::VarErrorKind::UnknownSystemVariable(
            name,
        )) => SqlQueryError::new(
            ER_UNKNOWN_SYSTEM_VARIABLE,
            *b"HY000",
            format!("Unknown system variable '{name}'"),
        ),
        // Go: "Variable '%-.192s' is a %s variable".
        tidb_executor::DriverError::Var(tidb_executor::VarErrorKind::ReadOnlyVariable(name)) => {
            SqlQueryError::new(
                ER_INCORRECT_GLOBAL_LOCAL_VAR,
                *b"HY000",
                format!("Variable '{name}' is a read only variable"),
            )
        }
        tidb_executor::DriverError::SubqueryReturnsMoreThanOneRow => SqlQueryError::new(
            ER_SUBQUERY_NO_1_ROW,
            *b"21000",
            "Subquery returns more than 1 row".to_owned(),
        ),
        tidb_executor::DriverError::CatalogPoisoned => {
            SqlQueryError::unknown("the shared catalog is unusable after a failed statement")
        }
    }
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

        let Err(unsupported) = session.execute("ALTER TABLE t ADD COLUMN b INT") else {
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
