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

//! Live client-socket ownership for the bounded MySQL connection lifecycle.

use std::collections::HashMap;
use std::fmt;
use std::io::{BufReader, Write};
use crate::client_socket::ClientSocket;
use tidb_session::domain::peer_address::PeerAddress;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_protocol::result_encoder::ResultEncoder;
use tidb_protocol::{
    decode_command, decode_prepared_statement_close, decode_prepared_statement_fetch,
    decode_prepared_statement_send_long_data, encode_prepared_statement_prepare_response,
    split_prepared_statement_execute, Command, CompressionAlgorithm, PacketError, PacketIoReader,
    PacketIoWriter, PacketReader, PreparedParameterType, PreparedParameterTypes,
    DEFAULT_MAX_ALLOWED_PACKET,
};

use crate::auth_exchange::AuthSwitchRequest;
use crate::configured_user_store::{AuthenticationFailure, ConfiguredUserStore};
use crate::connection_resultset::{
    write_connection_binary_result_set_to_sink, write_connection_result_set_to_sink,
};
use crate::connection_writers::{
    access_denied_message, account_locked_message, prepared_parameter_column,
    prepared_statement_id, write_affected_rows_ok, write_affected_rows_ok_with_info,
    write_eof_or_ok, write_error, write_ok, write_packet_to, write_payload, write_query_error,
    write_query_error_at, write_unknown_statement, ConnectionPacketOutput, TcpResultSetSink,
    WireFraming,
};
use crate::cursor_state::{CursorFetchError, CursorState};
use crate::handshake::{
    negotiate_capabilities, parse_response, parse_response_header,
    InitialHandshake, CLIENT_COMPRESS, CLIENT_CONNECT_ATTRS, CLIENT_CONNECT_WITH_DB,
    CLIENT_FOUND_ROWS, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
    CLIENT_SSL, CLIENT_TRANSACTIONS, CLIENT_ZSTD_COMPRESSION_ALGORITHM, DEFAULT_COLLATION_ID,
};
use crate::mysql_tls::{ClientStream, MysqlServerTls};
use crate::native_password::generate_handshake_salt;
use crate::resultset_writer::ResultSetSink;
use crate::secure_transport::TransportKind;
use tidb_ast::Stmt;

use crate::driver::{QuerySession, QuerySessionFactory};
use crate::sql_node::{
    ConnectionCancellation, ConnectionClose, ConnectionTracker, GeneralExecuteOutcome,
    PreparedGeneral, QueryResult, SessionContext, SqlQueryError,
};
use crate::wire_status::{WireStatus, SERVER_STATUS_CURSOR_EXISTS, SERVER_STATUS_LAST_ROW_SEND};

/// Carries one client SQL payload's bytes for `@@character_set_client`.
///
/// Go keeps COM_QUERY bytes raw for every client charset: the scanner reads
/// them as a `string`, and only its literal conversions
/// (`Scanner.convert2Connection` for string literals, `convert2System` for
/// identifiers) consult the client encoding, refusing or replacing what it
/// cannot decode (`ErrInvalidCharacterString`, 1300). The text here carries
/// the bytes losslessly ([`tidb_hack::raw_bytes`]: valid UTF-8 as is, any
/// other byte as its escape character) and the parser applies Go's
/// conversions over the exact bytes. A `gbk`/`gb18030` client's every
/// non-ASCII byte is an escape, so the scanner's `skipRune` can step over
/// its multibyte characters by their source bytes.
fn decode_client_sql(bytes: &[u8], charset: &str) -> String {
    tidb_parser::carry_client_sql(bytes, tidb_datatype::find_encoding(charset)).into_owned()
}

enum PreparedCursorOpenError {
    Query(SqlQueryError),
    Transport(MysqlConnectionError),
}

fn open_prepared_cursor<O: ConnectionPacketOutput + ?Sized>(
    result: &mut QueryResult<'_>,
    output: &mut O,
    framing: WireFraming,
    result_encoder: ResultEncoder,
) -> Result<CursorState, PreparedCursorOpenError> {
    let cursor = CursorState::materialize_result(result).map_err(PreparedCursorOpenError::Query)?;
    let warnings = result.warning_count();
    let status = result.wire_status();
    let affected_rows = result.affected_rows();
    let last_insert_id = result.last_insert_id();
    let info = result.info().to_vec();
    let options = framing.result_set_with_output(
        status.with(SERVER_STATUS_CURSOR_EXISTS),
        warnings,
        affected_rows,
        last_insert_id,
        info,
        result_encoder,
    );
    let mut stream =
        tidb_protocol::BinaryResultSetStream::new(cursor.columns().to_vec(), options.clone())
            .map_err(|error| {
                PreparedCursorOpenError::Query(SqlQueryError::unknown(error.to_string()))
            })?;
    let metadata = stream.metadata_packets().map_err(|error| {
        PreparedCursorOpenError::Query(SqlQueryError::unknown(error.to_string()))
    })?;
    let mut sequence = 1;
    for packet in metadata {
        write_packet_to(output, sequence, &packet).map_err(PreparedCursorOpenError::Transport)?;
        sequence = sequence.wrapping_add(1);
    }
    // Legacy metadata framing already includes its EOF. Under
    // CLIENT_DEPRECATE_EOF the stream intentionally omits it, so cursor
    // execute supplies the one OK-as-EOF terminator that advertises the open
    // cursor without sending a row.
    if framing.deprecate_eof {
        write_eof_or_ok(output, sequence, options).map_err(PreparedCursorOpenError::Transport)?;
    }
    Ok(cursor)
}


const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
/// `CLIENT_MULTI_STATEMENTS`: the client may send several statements in one
/// COM_QUERY. Go's server advertises it (`defaultCapability`), and
/// `handleQuery` consults the CLIENT's bit before running a split text.
const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
/// `CLIENT_MULTI_RESULTS`: the client understands
/// `SERVER_MORE_RESULTS_EXISTS` chains. Advertised alongside
/// `CLIENT_MULTI_STATEMENTS`, as Go does.
const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
/// `CLIENT_LOCAL_FILES`: client-local transfer used by LOAD STATS.
const CLIENT_LOCAL_FILES: u32 = 1 << 7;
/// Go's `defaultCapability` (`pkg/server/server.go`) restricted to what this
/// node actually serves.
///
/// `CLIENT_CONNECT_WITH_DB` is load-bearing beyond selecting a schema. A real
/// libmysqlclient sets that bit in its *response* unconditionally, but only
/// writes the database field when the *server* advertised the bit. Omitting it
/// here therefore produced a response whose capability flags promised a field
/// the packet did not contain, so every field after the auth data -- database,
/// auth plugin, connection attributes -- was read one field early.
const SERVER_CAPABILITIES: u32 = CLIENT_PROTOCOL_41
    | CLIENT_COMPRESS
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_FOUND_ROWS
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_DEPRECATE_EOF
    | CLIENT_ZSTD_COMPRESSION_ALGORITHM
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_LOCAL_FILES;
const ER_ACCESS_DENIED_ERROR: u16 = 1045;
const ER_UNKNOWN_COM_ERROR: u16 = 1047;
/// SQLSTATE `08S01` for [`ER_UNKNOWN_COM_ERROR`]. Go resolves every ERR
/// packet's state through `mysql.MySQLState` (`pkg/parser/mysql/state.go`),
/// which maps `ErrUnknownCom` to `08S01`, not the `HY000` default.
const ER_UNKNOWN_COM_ERROR_STATE: [u8; 5] = *b"08S01";
/// Go's `mysql.ErrAccountHasBeenLocked` (`pkg/errno/errcode.go`): the login
/// errno an `ACCOUNT LOCK`'d (or ROLE) account gets, distinct from the
/// generic access-denied a bad password or unknown user gets.
const ER_ACCOUNT_HAS_BEEN_LOCKED: u16 = 3118;
/// Go `errno.ErrSecureTransportRequired`. It carries no SQLSTATE mapping in
/// `pkg/errno`, so it reports the default `HY000`.
const ER_SECURE_TRANSPORT_REQUIRED: u16 = 3159;
const SECURE_TRANSPORT_REQUIRED_MESSAGE: &str =
    "Connections using insecure transport are prohibited while --require_secure_transport=ON.";
/// Go's `mysql.ErUserAccessDeniedForUserAccountBlockedByPasswordLock`: the
/// errno an account auto-locked by `FAILED_LOGIN_ATTEMPTS` gets, distinct
/// again from the manual `ACCOUNT LOCK` above.
const ER_USER_ACCESS_DENIED_BLOCKED_BY_PASSWORD_LOCK: u16 = 3955;
/// Go's `mysql.ErrMustChangePasswordLogin`: the login errno an account with
/// an EXPIRED password gets when the server refuses expired logins.
const ER_MUST_CHANGE_PASSWORD_LOGIN: u16 = 1862;
const ER_UNKNOWN_ERROR: u16 = 1105;
const ER_WRONG_ARGUMENTS: u16 = 1210;
pub(crate) const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
pub(crate) const RESULT_BATCH_SIZE: usize = 128;

/// Go `clientConn.dispatch`'s default arm (`pkg/server/conn.go:1592-1593`)
/// for a command byte with no owner. This is deliberately distinct from
/// `ER_UNKNOWN_COM_ERROR` (1047), which is Go's errno for a *known* command
/// such as `COM_FIELD_LIST` that this bounded node has not implemented.
fn unknown_command_error(code: u8) -> SqlQueryError {
    SqlQueryError::unknown(format!("command {code} not supported now"))
}

struct ConnectionPreparedStatement {
    database: String,
    /// The parsed statement is immutable after PREPARE.  Keep it behind an
    /// `Arc` so COM_STMT_EXECUTE only clones a pointer; cloning the retained
    /// AST for every execute made YCSB's point reads pay a full tree copy.
    statement: Arc<PreparedGeneral>,
    parameter_types: Option<Vec<PreparedParameterType>>,
    metrics_label: &'static str,
    /// An open read-only cursor: the materialized result a cursor-mode
    /// execute stored for later `COM_STMT_FETCH` commands, with the columns
    /// it advertises and the next unread row. Go holds the same thing on the
    /// statement as a row container.
    cursor: Option<CursorState>,
    /// The `COM_STMT_SEND_LONG_DATA` buffer for each parameter, indexed by
    /// parameter ID: Go's `TiDBStatement.boundParams`, allocated
    /// `make([][]byte, paramCount)` at prepare time
    /// (`pkg/server/driver_tidb.go:358`). `None` is Go's nil slice -- "this
    /// parameter was never sent as long data" -- and is what makes an empty
    /// bound buffer distinguishable from an unbound one.
    bound_params: Vec<Option<Vec<u8>>>,
    /// Number of long-data bytes currently charged to the connection's
    /// session tracker for this statement.
    bound_long_data_bytes: i64,
    /// Sticky Go `AppendParam` state: a refused chunk is reported by the next
    /// EXECUTE and further SEND_LONG_DATA chunks remain silent.
    bound_params_mem_quota_exceeded: bool,
    bound_params_too_large: bool,
}

impl ConnectionPreparedStatement {
    /// Go `for i := range ts.boundParams { ts.boundParams[i] = nil }`.
    /// The vector keeps its prepare-time length; only the buffers go.
    fn clear_bound_params<S: QuerySession>(&mut self, session: &mut S) {
        if self.bound_long_data_bytes > 0 {
            session.release_long_data(self.bound_long_data_bytes);
            self.bound_long_data_bytes = 0;
        }
        for slot in &mut self.bound_params {
            *slot = None;
        }
        self.bound_params_mem_quota_exceeded = false;
        self.bound_params_too_large = false;
    }
}

/// Why one `COM_STMT_SEND_LONG_DATA` chunk could not be appended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AppendParamError {
    /// No such prepared statement handle on this connection.
    UnknownStatement,
    /// The parameter ID is at or past the statement's marker count.
    ParameterOutOfRange,
}

struct PreparedStatementRegistry {
    next_id: Option<u32>,
    statements: HashMap<u32, ConnectionPreparedStatement>,
}

impl Default for PreparedStatementRegistry {
    fn default() -> Self {
        Self {
            next_id: Some(1),
            statements: HashMap::new(),
        }
    }
}

impl PreparedStatementRegistry {
    fn session_states_handler<S: QuerySession>(
        &mut self,
        engine: &mut S,
        stmt: &Stmt,
    ) -> Result<(), SqlQueryError> {
        use tidb_session::session_states::{PreparedStmtInfo, ProtocolSessionStates};
        let effective = engine
            .migration_session()
            .and_then(|session| session.session_migration_statement(stmt).cloned());
        let stmt = effective.as_ref().unwrap_or(stmt);
        let is_show = matches!(stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::SessionStates));
        if is_show {
            let mut state = ProtocolSessionStates::default();
            for (&id, prepared) in &self.statements {
                if prepared.bound_params.iter().any(Option::is_some) {
                    state.cannot_migrate = Some("prepared statements have bound params");
                    break;
                }
                if prepared.cursor.is_some() {
                    state.cannot_migrate = Some("prepared statements have unfetched rows");
                    break;
                }
                let sql = prepared.statement.sql();
                let types: Vec<u8> = prepared
                    .parameter_types
                    .as_ref()
                    .map(|types| {
                        types
                            .iter()
                            .flat_map(|ty| {
                                [ty.type_code(), if ty.is_unsigned() { 0x80 } else { 0 }]
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                state.prepared.insert(
                    id,
                    PreparedStmtInfo {
                        name: String::new(),
                        stmt_text: sql.to_owned(),
                        stmt_db: prepared.database.clone(),
                        param_types: types,
                    },
                );
            }
            if let Some(session) = engine.migration_session() {
                session.set_protocol_session_states(state);
            }
        }
        let Stmt::Session(stmt) = stmt else {
            return Ok(());
        };
        let tidb_ast::SessionStmt::SetSessionStates(set) = &**stmt else {
            return Ok(());
        };
        let state = tidb_session::session_states::parse_session_states(&set.session_states)
            .map_err(|error| {
                let error = error.to_mysql_error();
                SqlQueryError::from(error)
            })?;
        for (id, info) in state.prepared_stmts {
            if !info.name.is_empty() {
                continue;
            }
            let types = info.param_types;
            if types.len() % 2 != 0 {
                return Err(SqlQueryError::unknown(
                    "invalid prepared parameter type state",
                ));
            }
            let database = info.stmt_db;
            let old_db = engine
                .migration_session()
                .map(|session| session.replace_migration_database(database.clone()));
            let result = engine.prepare_general(&info.stmt_text);
            if let Some(old_db) = old_db {
                if let Some(session) = engine.migration_session() {
                    session.replace_migration_database(old_db);
                }
            }
            let statement = result?;
            if !types.is_empty() && types.len() != statement.parameter_count() * 2 {
                return Err(SqlQueryError::unknown(
                    "invalid prepared parameter type count",
                ));
            }
            if let Some(mut old) = self.statements.remove(&id) {
                old.clear_bound_params(engine);
                engine.close_prepared(&old.statement);
            }
            let old_next = self.next_id;
            self.next_id = Some(id);
            let result = self.insert(statement);
            self.next_id = old_next;
            result.map_err(SqlQueryError::unknown)?;
            let statement = self
                .statements
                .get_mut(&id)
                .expect("inserted migration handle");
            statement.database = database;
            statement.parameter_types = (!types.is_empty()).then(|| {
                types
                    .chunks_exact(2)
                    .map(|pair| PreparedParameterType::new(pair[0], pair[1] & 0x80 != 0))
                    .collect()
            });
        }
        Ok(())
    }

    fn insert(&mut self, statement: PreparedGeneral) -> Result<u32, &'static str> {
        let statement_id = self
            .next_id
            .ok_or("prepared statement ID space exhausted")?;
        self.next_id = statement_id.checked_add(1);
        // Compute once at PREPARE; EXECUTE uses this immutable label.
        let metrics_label = match statement.prepared_ast() {
            Some(prepared) => prepared.statement().label(),
            None => tidb_parser::parse(statement.sql()).map_or("general", |stmt| stmt.label()),
        };
        self.statements.insert(
            statement_id,
            ConnectionPreparedStatement {
                database: String::new(),
                bound_params: vec![None; statement.parameter_count()],
                bound_long_data_bytes: 0,
                bound_params_mem_quota_exceeded: false,
                bound_params_too_large: false,
                statement: Arc::new(statement),
                parameter_types: None,
                metrics_label,
                cursor: None,
            },
        );
        Ok(statement_id)
    }

    fn get(&self, statement_id: u32) -> Option<&ConnectionPreparedStatement> {
        self.statements.get(&statement_id)
    }

    fn remember_parameter_types(
        &mut self,
        statement_id: u32,
        parameter_types: &[PreparedParameterType],
    ) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.parameter_types = Some(parameter_types.to_vec());
        }
    }

    fn remove<S: QuerySession>(
        &mut self,
        statement_id: u32,
        session: &mut S,
    ) -> Option<ConnectionPreparedStatement> {
        let mut statement = self.statements.remove(&statement_id)?;
        session.close_prepared(&statement.statement);
        if statement.bound_long_data_bytes > 0 {
            session.release_long_data(statement.bound_long_data_bytes);
            statement.bound_long_data_bytes = 0;
        }
        Some(statement)
    }

    /// Go `stmt.Reset` (`pkg/server/driver_tidb.go:151-160`): returns the
    /// statement to the state it had right after PREPARE. Go nils every
    /// `boundParams[i]`, drops the active cursor, and leaves the statement
    /// itself installed -- and `handleStmtReset` names exactly those two
    /// things it must clear, the open cursor and "the argument sent through
    /// `SEND_LONG_DATA`" (`pkg/server/conn_stmt.go:627-631`). The remembered
    /// parameter-type vector deliberately survives: Go's `TiDBStatement.Reset`
    /// leaves `paramsType` untouched, so a later execute may keep its
    /// new-parameter-bound flag clear.
    fn reset<S: QuerySession>(
        &mut self,
        statement_id: u32,
        session: &mut S,
    ) -> Result<Option<CursorState>, ()> {
        match self.statements.get_mut(&statement_id) {
            Some(statement) => {
                statement.clear_bound_params(session);
                Ok(statement.cursor.take())
            }
            None => Err(()),
        }
    }

    /// Go `TiDBStatement.AppendParam` (`pkg/server/driver_tidb.go:104-116`):
    /// appends one `COM_STMT_SEND_LONG_DATA` chunk to a parameter's buffer.
    ///
    /// A parameter ID at or past the prepared marker count is Go's
    /// `ErrWrongArguments("stmt_send_longdata")`. An empty chunk stores an
    /// empty buffer rather than nothing, which is how Go keeps "bound to the
    /// empty string" distinct from "never bound".
    fn append_param<S: QuerySession>(
        &mut self,
        statement_id: u32,
        parameter_id: usize,
        chunk: &[u8],
        session: &mut S,
    ) -> Result<(), AppendParamError> {
        let statement = self
            .statements
            .get(&statement_id)
            .ok_or(AppendParamError::UnknownStatement)?;
        let current_len = statement
            .bound_params
            .get(parameter_id)
            .ok_or(AppendParamError::ParameterOutOfRange)?
            .as_ref()
            .map_or(0, Vec::len);

        // Go treats an empty chunk as an explicit empty-string binding. It
        // runs this branch before either sticky overflow flag is consulted.
        if chunk.is_empty() {
            let statement = self
                .statements
                .get_mut(&statement_id)
                .expect("statement was present above");
            if let Some(buffer) = statement.bound_params[parameter_id].as_mut() {
                let released = i64::try_from(buffer.len()).unwrap_or(i64::MAX);
                if released > 0 {
                    session.release_long_data(released);
                    statement.bound_long_data_bytes =
                        statement.bound_long_data_bytes.saturating_sub(released);
                }
                buffer.clear();
            } else {
                statement.bound_params[parameter_id] = Some(Vec::new());
            }
            return Ok(());
        }

        if statement.bound_params_mem_quota_exceeded || statement.bound_params_too_large {
            return Ok(());
        }

        // Go bounds the accumulated value with `max_allowed_packet`, which
        // this node does not have as a session variable yet (gap #185); the
        // same hardcoded wire cap the packet reader enforces is therefore the
        // bound here, so a client cannot grow one parameter without limit.
        if current_len.saturating_add(chunk.len()) > DEFAULT_MAX_ALLOWED_PACKET {
            self.statements
                .get_mut(&statement_id)
                .expect("statement was present above")
                .bound_params_too_large = true;
            return Ok(());
        }

        let chunk_size = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        if !session.try_consume_long_data(chunk_size) {
            self.statements
                .get_mut(&statement_id)
                .expect("statement was present above")
                .bound_params_mem_quota_exceeded = true;
            return Ok(());
        }

        let statement = self
            .statements
            .get_mut(&statement_id)
            .expect("statement was present above");
        if statement.bound_params_mem_quota_exceeded || statement.bound_params_too_large {
            // A sticky refusal must not retain bytes if a session-specific
            // implementation accepted a charge concurrently with it.
            session.release_long_data(chunk_size);
            return Ok(());
        }
        let buffer = statement.bound_params[parameter_id].get_or_insert_with(Vec::new);
        buffer.extend_from_slice(chunk);
        statement.bound_long_data_bytes =
            statement.bound_long_data_bytes.saturating_add(chunk_size);
        Ok(())
    }

    fn bound_params(&self, statement_id: u32) -> &[Option<Vec<u8>>] {
        self.statements
            .get(&statement_id)
            .map_or(&[], |statement| statement.bound_params.as_slice())
    }

    /// Go's `stmt.Reset()` call inside `handleStmtExecute`, run right after
    /// `parseBinaryParams` has read the buffers
    /// (`pkg/server/conn_stmt.go:212-217`): long data is consumed by exactly
    /// one execute and never leaks into the next one.
    fn clear_bound_params<S: QuerySession>(&mut self, statement_id: u32, session: &mut S) {
        if let Some(statement) = self.statements.get_mut(&statement_id) {
            statement.clear_bound_params(session);
        }
    }

    fn long_data_error(&self, statement_id: u32, connection_id: u64) -> Option<SqlQueryError> {
        let statement = self.statements.get(&statement_id)?;
        if statement.bound_params_mem_quota_exceeded {
            let error = tidb_executor::mem_quota::memory_exceed_for_query(connection_id);
            return Some(SqlQueryError::from(error));
        }
        if statement.bound_params_too_large {
            return Some(SqlQueryError::from(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::servererr::ERR_NET_PACKET_TOO_LARGE
                        .generate_with_stack_by_args(&[]),
                ),
            ));
        }
        None
    }

    fn open_cursor(&mut self, statement_id: u32, state: CursorState) -> Option<CursorState> {
        self.statements
            .get_mut(&statement_id)
            .and_then(|statement| statement.cursor.replace(state))
    }

    fn take_cursor(&mut self, statement_id: u32) -> Option<CursorState> {
        self.statements
            .get_mut(&statement_id)
            .and_then(|statement| statement.cursor.take())
    }
}

/// Observable terminal state of one accepted connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionExit {
    /// The client sent `COM_QUIT`.
    Quit,
    /// The peer closed the socket between commands.
    PeerClosed,
    /// Authentication was rejected after an ERR packet was written.
    AuthenticationRejected,
    /// Authentication succeeded but a worker-local query session could not open.
    SessionRejected,
    /// A `KILL` / `KILL CONNECTION` ended the connection (Go closes the
    /// session's socket and the command loop stops).
    Killed,
}

/// MySQL command opcodes observed while serving one connection.
///
/// Command counts advance as soon as dispatch identifies the opcode. Success
/// counts advance only after the complete success response has been written.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionCommandCounts {
    /// Number of `COM_QUERY` commands, including rejected queries.
    pub text_query_commands: u64,
    /// Number of `COM_STMT_PREPARE` commands, including rejected prepares.
    pub stmt_prepare_commands: u64,
    /// Number of complete successful `COM_STMT_PREPARE` responses.
    pub stmt_prepare_successes: u64,
    /// Number of `COM_STMT_EXECUTE` commands, including rejected executes.
    pub stmt_execute_commands: u64,
    /// Number of complete successful `COM_STMT_EXECUTE` responses.
    pub stmt_execute_successes: u64,
    /// Number of `COM_STMT_CLOSE` commands, including malformed closes.
    pub stmt_close_commands: u64,
    /// Number of `COM_STMT_RESET` commands, including unknown handles.
    pub stmt_reset_commands: u64,
    /// Number of `COM_STMT_SEND_LONG_DATA` commands, including the ones whose
    /// handle or parameter ID was rejected. A successful one writes no packet.
    pub stmt_send_long_data_commands: u64,
    /// Number of `COM_STMT_FETCH` commands, including unknown handles.
    pub stmt_fetch_commands: u64,
    /// Number of complete successful `COM_STMT_FETCH` responses.
    pub stmt_fetch_successes: u64,
}

/// Successful lifecycle report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionReport {
    /// Stable server connection ID advertised in the handshake.
    pub connection_id: u64,
    /// Number of successful text-query or prepared-execute commands.
    pub queries: u64,
    /// Exact command opcodes observed on this connection.
    pub commands: ConnectionCommandCounts,
    /// Why the connection stopped.
    pub exit: ConnectionExit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AcceptedConnectionIdentity {
    connection_id: u64,
    peer_addr: PeerAddress,
}

/// Adds Go `clientConn.user`/`peerHost` error accounting to the ordinary
/// authenticated command writer without changing packet framing.
struct ClientErrorRecordingOutput<O> {
    inner: O,
    user: String,
    host: String,
}

impl<O> ClientErrorRecordingOutput<O> {
    fn new(inner: O, user: String, host: String) -> Self {
        Self { inner, user, host }
    }

    fn set_user(&mut self, user: String) {
        self.user = user;
    }
}

impl<O: Write> Write for ClientErrorRecordingOutput<O> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buffer)
    }

    // The packet writer hands every frame down as one vectored write of
    // header plus payload. Without this forwarding the default
    // `write_vectored` writes only the first slice, so each response left as
    // two TLS records and two syscalls; Go's `PacketIO` flushes a response as
    // one write.
    fn write_vectored(&mut self, buffers: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        self.inner.write_vectored(buffers)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<O: ConnectionPacketOutput> ConnectionPacketOutput for ClientErrorRecordingOutput<O> {
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) -> Result<u8, MysqlConnectionError> {
        self.inner.write_packet(sequence, payload)
    }

    fn write_packets(
        &mut self,
        sequence: u8,
        payloads: &[&[u8]],
    ) -> Result<u8, MysqlConnectionError> {
        self.inner.write_packets(sequence, payloads)
    }

    fn record_client_error(&self, code: u16) {
        tidb_error::tidb::infoschema::increment_error(code, &self.user, &self.host);
    }

    fn record_client_warning(&self, code: u16) {
        tidb_error::tidb::infoschema::increment_warning(code, &self.user, &self.host);
    }

    fn compressed_sequence(&self) -> Option<u8> {
        self.inner.compressed_sequence()
    }

    fn set_compressed_sequence(&mut self, sequence: u8) {
        self.inner.set_compressed_sequence(sequence);
    }
}

/// Records a failed text-protocol write in the statement's warning buffer:
/// go `driver_tidb.go:376` appends every error to StmtCtx. The DML
/// column-validation path already appends its own warnings, so those codes are
/// not appended twice (go's INSERT 1264 shows ONE row); a DDL error always is.

fn record_client_warnings<O: ConnectionPacketOutput + ?Sized, S: QuerySession>(
    output: &O,
    session: &S,
) {
    for code in session.warning_codes() {
        output.record_client_warning(code);
    }
}

/// Go Server.concurrentLimiter: one token channel shared by all connections
/// to this server. Connection count, authentication and idle sockets do not
/// consume these permits.
pub(crate) struct CommandLimiter {
    available: crossbeam_channel::Receiver<()>,
    returned: crossbeam_channel::Sender<()>,
}

impl CommandLimiter {
    pub(crate) fn new(limit: usize) -> Self {
        let (returned, available) = crossbeam_channel::bounded(limit);
        for _ in 0..limit {
            returned.send(()).expect("new token channel is connected");
        }
        Self {
            available,
            returned,
        }
    }

    fn acquire(&self) -> CommandToken<'_> {
        let started = Instant::now();
        self.available.recv().expect("server owns the token sender");
        crate::server_metrics::TOKENS.inc();
        // Go records integer microseconds on acquisition, not command runtime
        // (despite the histogram's `_seconds` name).
        crate::server_metrics::GET_TOKEN_DURATION_SECONDS
            .observe(started.elapsed().as_micros() as f64);
        CommandToken { limiter: self }
    }
}

/// The borrow keeps the channel owner alive through response streaming and
/// returns exactly one token on normal completion, error or panic recovery.
struct CommandToken<'a> {
    limiter: &'a CommandLimiter,
}

impl Drop for CommandToken<'_> {
    fn drop(&mut self) {
        self.limiter
            .returned
            .send(())
            .expect("server owns the token receiver");
        crate::server_metrics::TOKENS.dec();
    }
}

#[cfg(test)]
mod command_token_tests {
    use super::*;

    static METRICS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn command_token_wait_is_observed_on_acquisition() {
        let _metrics = METRICS.lock().unwrap();
        let before = crate::server_metrics::GET_TOKEN_DURATION_SECONDS.get_sample_count();
        let limiter = CommandLimiter::new(1);
        let token = limiter.acquire();
        let acquired = crate::server_metrics::GET_TOKEN_DURATION_SECONDS.get_sample_count();
        drop(token);
        assert_eq!(
            acquired,
            before + 1,
            "Go records token wait before executing the command"
        );
        assert_eq!(
            crate::server_metrics::GET_TOKEN_DURATION_SECONDS.get_sample_count(),
            acquired,
            "release must not record command execution time as token wait"
        );
    }

    #[test]
    fn command_token_returns_on_unwind_and_zero_has_no_permits() {
        let _metrics = METRICS.lock().unwrap();
        let limiter = CommandLimiter::new(2);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _first = limiter.acquire();
            let _second = limiter.acquire();
            assert!(limiter.available.is_empty());
            panic!("command failed");
        }));
        assert!(result.is_err());
        assert_eq!(limiter.available.len(), 2);
        // Go normalizes zero only while loading a config file. An explicit
        // zero CLI override produces an empty token channel, not unlimited.
        assert!(CommandLimiter::new(0).available.is_empty());
    }
}

/// Fatal socket/protocol failure that prevents orderly command continuation.
#[derive(Debug)]
pub enum MysqlConnectionError {
    /// Cloning or configuring the accepted TCP stream failed.
    Io(std::io::Error),
    /// MySQL packet framing failed.
    Packet(PacketError),
    /// The checked handshake encoder or parser rejected the peer.
    Handshake(String),
    /// A result failed after response bytes may already have escaped.
    PartialResult(String),
    /// The statement was published and then lost its answer, so whether it
    /// committed is unknown and this connection must end without a verdict.
    ///
    /// Go `pkg/server/conn.go:1288-1291`:
    ///
    /// > } else if terror.ErrResultUndetermined.Equal(err) {
    /// >     logutil.Logger(ctx).Warn("result undetermined, close this connection", zap.Error(err))
    /// >     server_metrics.DisconnectErrorUndetermined.Inc()
    /// >     return
    ///
    /// It `return`s from the command loop *without* writing an ERR packet: no
    /// SQL error code can express "unknown", and a client that receives an
    /// error is entitled to retry — which double-applies if the commit landed.
    /// A closed connection is the one answer the client cannot mistake for a
    /// verdict.
    ResultUndetermined(String),
    /// The connection's command loop panicked and was recovered.
    ///
    /// Go `pkg/server/conn.go` `Run`'s deferred `recover()`: a panicking
    /// statement logs "connection running loop panic" and closes ITS
    /// connection while the server keeps serving. Command recovery attempts
    /// ERR 1105 through the retained packet writer before cleanup, preserving
    /// its sequence, compression and buffered response bytes.
    Panicked(String),
}

impl fmt::Display for MysqlConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "TCP I/O failed: {error}"),
            Self::Packet(error) => write!(formatter, "MySQL packet failed: {error}"),
            Self::Handshake(message) => formatter.write_str(message),
            Self::PartialResult(message) => {
                write!(formatter, "result failed after bytes escaped: {message}")
            }
            Self::ResultUndetermined(message) => {
                write!(
                    formatter,
                    "result undetermined, close this connection: {message}"
                )
            }
            Self::Panicked(message) => {
                write!(formatter, "connection running loop panic: {message}")
            }
        }
    }
}

impl std::error::Error for MysqlConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Packet(error) => Some(error),
            Self::Handshake(_)
            | Self::PartialResult(_)
            | Self::ResultUndetermined(_)
            | Self::Panicked(_) => None,
        }
    }
}

impl From<PacketError> for MysqlConnectionError {
    fn from(error: PacketError) -> Self {
        Self::Packet(error)
    }
}

/// Go's `authSha` packet: the one-byte command that introduces a
/// caching_sha2 / sm3 authentication decision, and the `fastAuthFail`
/// decision itself. TiDB caches nothing and serves no RSA public key, so
/// `fastAuthFail` is the ONLY decision it ever sends
/// (`server/conn.go`'s "Currently we always send a FastAuthFail").
const SHA2_FAST_AUTH_COMMAND: u8 = 1;
const SHA2_FAST_AUTH_FAIL: u8 = 4;

/// Serves one accepted socket on a MySQL port that offers no TLS.
///
/// The port advertises no `CLIENT_SSL`, so a client that wants TLS is refused
/// by the negotiation rather than left waiting for an upgrade.
pub fn serve_mysql_connection<F: QuerySessionFactory>(
    stream: impl Into<ClientSocket>,
    peer_addr: impl Into<PeerAddress>,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
) -> Result<ConnectionReport, MysqlConnectionError> {
    serve_mysql_connection_with_tls(
        stream,
        peer_addr,
        cancellation,
        factory,
        users,
        tracker,
        max_allowed_packet,
        None,
    )
}

/// Serves one accepted socket through handshake, optional TLS upgrade,
/// authentication, commands, result/error writes, and exactly-once connection
/// cleanup.
///
/// `tls` is both the advertisement and the capability: `CLIENT_SSL` reaches the
/// initial handshake packet exactly when material is present, matching Go's
/// `s.capability |= mysql.ClientSSL` guard in `pkg/server/server.go`.
#[allow(clippy::too_many_arguments)]
pub fn serve_mysql_connection_with_tls<F: QuerySessionFactory>(
    stream: impl Into<ClientSocket>,
    peer_addr: impl Into<PeerAddress>,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    tls: Option<&MysqlServerTls>,
) -> Result<ConnectionReport, MysqlConnectionError> {
    serve_mysql_connection_with_runtime(
        stream.into(),
        peer_addr.into(),
        cancellation,
        factory,
        users,
        tracker,
        MysqlConnectionRuntime {
            max_allowed_packet,
            tls,
        },
    )
}

/// Go `pkg/server/conn.go`'s connection-loop exit accounting, which
/// `pkg/server/metrics` binds to the shared disconnection counter:
/// `DisconnectNormal` (`conn.go:1286`, a client that ended cleanly via
/// COM_QUIT), `DisconnectErrorUndetermined` (`conn.go:1288`), and
/// `DisconnectByClientWithError` (`conn.go:1256`, every other read or
/// dispatch failure, including a KILL-woken socket). An auth rejection
/// never reaches that loop in Go: it is `HandShakeErrorCounter`
/// (`server.go:824`) alone, and a per-connection panic is
/// `PanicCounter{session}` (`conn.go:1168`).
fn record_connection_exit_metrics(result: &Result<ConnectionReport, MysqlConnectionError>) {
    let disconnection = &crate::server_metrics::DISCONNECTION_TOTAL;
    match result {
        Ok(report) => match report.exit {
            ConnectionExit::Quit => {
                disconnection
                    .with_label_values(&[crate::server_metrics::disconnect::NORMAL])
                    .inc();
            }
            ConnectionExit::PeerClosed | ConnectionExit::Killed => {
                disconnection
                    .with_label_values(&[crate::server_metrics::disconnect::BY_CLIENT_WITH_ERROR])
                    .inc();
            }
            ConnectionExit::AuthenticationRejected => {
                crate::server_metrics::HANDSHAKE_ERROR_TOTAL.inc();
            }
            ConnectionExit::SessionRejected => {}
        },
        Err(MysqlConnectionError::ResultUndetermined(_)) => {
            disconnection
                .with_label_values(&[crate::server_metrics::disconnect::UNDETERMINED])
                .inc();
        }
        Err(MysqlConnectionError::Panicked(_)) => {
            crate::server_metrics::PANIC_TOTAL
                .with_label_values(&["session"])
                .inc();
            disconnection
                .with_label_values(&[crate::server_metrics::disconnect::BY_CLIENT_WITH_ERROR])
                .inc();
        }
        Err(_) => {
            disconnection
                .with_label_values(&[crate::server_metrics::disconnect::BY_CLIENT_WITH_ERROR])
                .inc();
        }
    }
}

pub(crate) struct MysqlConnectionRuntime<'a> {
    pub(crate) max_allowed_packet: usize,
    pub(crate) tls: Option<&'a MysqlServerTls>,
}

/// What Go `checkAuthPlugin` does with the connection after reading the
/// account's plugin.
enum AuthPluginDecision {
    /// Keep the response the client sent.
    Keep,
    /// Send an auth switch to this server-side plugin and use its answer.
    Switch(String),
    /// `auth_socket`: the response is the peer process's OS user.
    SocketUser,
}

/// Go `checkAuthPlugin`'s refusals.
enum AuthPluginRefusal {
    /// `ErrAccessDenied`: no account matches, or `auth_socket` off a Unix
    /// socket.
    AccessDenied,
    /// `ErrNotSupportedAuthMode`: a client without plugin support for an
    /// account on another plugin.
    NotSupportedAuthMode,
}

/// Go `clientConn.checkAuthPlugin` (`server/conn.go:939`): `server_plugin` is
/// `cc.authPlugin`, `client_plugin` is `resp.AuthPlugin`, which a switch
/// rewrites.
fn check_auth_plugin(
    server_plugin: &str,
    client_plugin: &mut String,
    plugin_auth: bool,
    users: &ConfiguredUserStore,
    user: &str,
    peer: &PeerAddress,
) -> Result<AuthPluginDecision, AuthPluginRefusal> {
    use tidb_mysql::consts::{
        AuthMySQLClearPassword, AuthNativePassword, AuthSocket, AuthTiDBAuthToken,
        AuthTiDBSessionToken,
    };
    // `tidb_session_token` is always permitted and skips the stored plugin.
    if client_plugin == AuthTiDBSessionToken {
        return Ok(AuthPluginDecision::Keep);
    }
    let Some(user_plugin) = users.match_identity_plugin(user, &peer.host()) else {
        return Err(AuthPluginRefusal::AccessDenied);
    };
    if user_plugin == AuthSocket {
        if !peer.is_unix_socket() {
            return Err(AuthPluginRefusal::AccessDenied);
        }
        AuthSocket.clone_into(client_plugin);
        return Ok(AuthPluginDecision::SocketUser);
    }
    if user_plugin.is_empty() {
        // No user plugin set, assuming MySQL Native Password.
        if client_plugin != AuthNativePassword && plugin_auth {
            AuthNativePassword.clone_into(client_plugin);
            return Ok(AuthPluginDecision::Switch(AuthNativePassword.to_owned()));
        }
        return Ok(AuthPluginDecision::Keep);
    }
    if server_plugin != user_plugin || server_plugin != client_plugin.as_str() {
        let user_plugin = if user_plugin == AuthTiDBAuthToken {
            AuthMySQLClearPassword.to_owned()
        } else {
            user_plugin
        };
        if plugin_auth {
            client_plugin.clone_from(&user_plugin);
            return Ok(AuthPluginDecision::Switch(user_plugin));
        }
        if user_plugin != AuthNativePassword {
            // MySQL 5.1 and older don't support authentication plugins yet.
            return Err(AuthPluginRefusal::NotSupportedAuthMode);
        }
    }
    Ok(AuthPluginDecision::Keep)
}

/// An authentication refusal: the error packet, then the connection's end.
#[allow(clippy::too_many_arguments)]
fn rejected_login<O: ConnectionPacketOutput + ?Sized>(
    output: &mut O,
    sequence: u8,
    code: u16,
    state: [u8; 5],
    message: impl AsRef<[u8]>,
    protocol_41: bool,
    connection_id: u64,
    commands: ConnectionCommandCounts,
) -> Result<ConnectionReport, MysqlConnectionError> {
    write_error(output, sequence, code, state, message, protocol_41)?;
    Ok(ConnectionReport {
        connection_id,
        queries: 0,
        commands,
        exit: ConnectionExit::AuthenticationRejected,
    })
}

pub(crate) fn serve_mysql_connection_with_runtime<F: QuerySessionFactory>(
    stream: ClientSocket,
    peer_addr: PeerAddress,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    tracker: &Arc<ConnectionTracker>,
    runtime: MysqlConnectionRuntime<'_>,
) -> Result<ConnectionReport, MysqlConnectionError> {
    if tracker.identity_lost() {
        return Err(MysqlConnectionError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "lost connection to PD",
        )));
    }
    // Go registers `compressionStats` in the server package's `init`, before
    // any connection it serves.
    crate::stat::register_compression_statistics();
    let mut lease = tracker.begin();
    // Go `server.go:303` + `conn.go:435-439`: the connection gauge enters
    // under the default label and leaves under the session's last group.
    crate::query_metrics::note_connection_start();
    // Go `server.go:591`, once a connection is accepted.
    tidb_util::logutil::bg_logger().debug("accept new connection success", &[]);
    let shutdown = cancellation.clone();
    let mut commands = ConnectionCommandCounts::default();
    // Keep worker containment for setup or cleanup panics. Command Run owns
    // its own recovery while the live session and framed writer still exist.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serve_connection_inner(
            stream,
            AcceptedConnectionIdentity {
                connection_id: lease.id(),
                peer_addr,
            },
            cancellation,
            factory,
            users,
            runtime,
            &mut commands,
            &tracker.command_limiter,
        )
    }))
    .unwrap_or_else(|payload| {
        Err(MysqlConnectionError::Panicked(connection_panic_message(
            payload,
            lease.id(),
        )))
    });
    let failed = result.is_err() && !shutdown.is_cancelled();
    if failed {
        lease.mark_failed();
    }
    let connection_id = lease.id();
    drop(lease);
    record_connection_exit_metrics(&result);
    crate::query_metrics::note_connection_end();
    // Go `onConn`'s deferred `logutil.Logger(ctx).Debug("connection closed")`.
    tidb_util::logutil::with_conn_id(&tidb_util::logutil::bg_logger(), connection_id)
        .debug("connection closed", &[]);
    result
}

#[allow(clippy::too_many_arguments)]
fn serve_connection_inner<F: QuerySessionFactory>(
    stream: ClientSocket,
    identity: AcceptedConnectionIdentity,
    cancellation: ConnectionCancellation,
    factory: &F,
    users: &ConfiguredUserStore,
    runtime: MysqlConnectionRuntime<'_>,
    commands: &mut ConnectionCommandCounts,
    command_limiter: &CommandLimiter,
) -> Result<ConnectionReport, MysqlConnectionError> {
    let AcceptedConnectionIdentity {
        connection_id,
        peer_addr,
    } = identity;
    stream.set_nodelay(true).map_err(MysqlConnectionError::Io)?;
    // The KILL path shuts the raw socket down from another thread, so it keeps
    // its own descriptor: a TLS session cannot be cloned, and shutting the
    // descriptor down is what wakes a blocked read either way.
    let stream_for_close = stream.try_clone().map_err(MysqlConnectionError::Io)?;
    // A second raw clone feeds the statement watchdog's client-EOF peek (the
    // original clone moves into `ConnectionClose` below).
    let stream_for_watch = stream_for_close
        .try_clone()
        .map_err(MysqlConnectionError::Io)?;
    // Reader and writer share one connection object, because after a TLS
    // upgrade there is exactly one TLS session and both directions run through
    // it.
    let socket = ClientStream::plain(stream);
    let mut output =
        ClientErrorRecordingOutput::new(socket.clone(), String::new(), peer_addr.host());
    let server_capabilities = if runtime.tls.is_some() {
        SERVER_CAPABILITIES | CLIENT_SSL
    } else {
        SERVER_CAPABILITIES
    };
    let salt = generate_handshake_salt();
    // Go `writeInitialHandshake`: the server advertises
    // `@@global.default_authentication_plugin` (`cc.authPlugin`).
    let server_plugin = users.default_authentication_plugin();
    let handshake = InitialHandshake {
        connection_id: u32::try_from(connection_id).unwrap_or(u32::MAX),
        salt: salt.to_vec(),
        capability: server_capabilities,
        collation: DEFAULT_COLLATION_ID,
        // Go `writeInitialHandshake` (`pkg/server/conn.go:496`) hardcodes this
        // one word, and only this one: the handshake precedes any session.
        status_flags: WireStatus::AUTOCOMMIT.bits(),
        server_version: tidb_mysql::runtime_versions().server_version,
        auth_plugin: server_plugin.clone(),
    };
    output
        .write_all(
            &handshake
                .encode_packet()
                .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?,
        )
        .map_err(MysqlConnectionError::Io)?;
    output.flush().map_err(MysqlConnectionError::Io)?;

    let mut reader =
        PacketReader::with_max_allowed_packet(socket.clone(), runtime.max_allowed_packet);
    reader.set_sequence(1);
    let mut auth_payload = reader.read_packet()?;
    // Go rejects a pre-4.1 client from the two-byte capability prefix before
    // attempting to parse Response41. Keeping this boundary ahead of TLS and
    // body parsing also preserves the source error when the remaining bytes
    // are not a valid Response41 packet.
    if auth_payload.len() >= 2 {
        let capability = u16::from_le_bytes([auth_payload[0], auth_payload[1]]);
        if u32::from(capability) & CLIENT_PROTOCOL_41 == 0 {
            return rejected_login(
                &mut output,
                2,
                tidb_error::mysql::errcode::ErrNotSupportedAuthMode,
                *b"08004",
                tidb_error::mysql::errname::ErrNotSupportedAuthMode.raw,
                false,
                connection_id,
                *commands,
            );
        }
    }
    // Go's `clientConn.handshake`: the common header is parsed first, and a
    // response that set CLIENT_SSL while the server holds a TLS config is a
    // *truncated* SSLRequest -- the socket is upgraded and the client repeats a
    // full HandshakeResponse41 over the encrypted stream, so this connection
    // reads two response packets. Sequence numbering is continuous across the
    // upgrade, which is why every later reply sequence shifts by one.
    let mut reply_sequence = 2_u8;
    if let Some(tls) = runtime.tls {
        let wants_tls = parse_response_header(&auth_payload)
            .is_ok_and(|(header, _)| header.capability & CLIENT_SSL != 0);
        if wants_tls {
            socket
                .upgrade_to_tls(tls)
                .map_err(MysqlConnectionError::Io)?;
            auth_payload = reader.read_packet()?;
            reply_sequence = 3;
        }
    }
    let response = match parse_response(&auth_payload) {
        Ok(response) => response,
        Err(error) => {
            // Go writes parser failures through the ordinary error boundary:
            // malformed packets are 1105/`malform packet error`, while the
            // connection-attribute hard limit retains its explicit text.
            let message = error.client_error_message();
            return rejected_login(
                &mut output,
                reply_sequence,
                ER_UNKNOWN_ERROR,
                *b"HY000",
                message.as_bytes(),
                true,
                connection_id,
                *commands,
            );
        }
    };
    let zstd_level = response.zstd_level;
    // Handshake parsing is byte-authoritative, matching Go strings. The
    // configured account/session owners are UTF-8-native today, so conversion
    // is explicit at that boundary rather than silently replacing bytes in
    // the protocol parser.
    let response_user = response.user.to_string_lossy().into_owned();
    let response_db_name = response.db_name.to_string_lossy().into_owned();
    output.set_user(response_user.clone());
    // Go rejects an insecure transport immediately after parsing the full
    // handshake response, before selecting an account plugin or sending an
    // AuthSwitchRequest. Keep the returned token so the account verifier
    // consumes this exact live-global decision instead of reading it again
    // after an interactive auth exchange.
    let transport = if peer_addr.is_unix_socket() {
        TransportKind::UnixSocket
    } else if socket.is_tls() {
        TransportKind::DirectTls
    } else {
        TransportKind::PlainTcp
    };
    let transport_admission = match users.admit_transport(transport) {
        Ok(admission) => admission
            .with_verified_client_certificate(socket.has_verified_client_certificate())
            .with_tls_peer(socket.verified_tls_peer()),
        Err(_) => {
            return rejected_login(
                &mut output,
                reply_sequence,
                ER_SECURE_TRANSPORT_REQUIRED,
                *b"HY000",
                SECURE_TRANSPORT_REQUIRED_MESSAGE,
                response.capability & CLIENT_PROTOCOL_41 != 0,
                connection_id,
                *commands,
            );
        }
    };
    let capabilities = match negotiate_capabilities(response.capability, server_capabilities) {
        Ok(capabilities) => capabilities,
        Err(_error) => {
            return rejected_login(
                &mut output,
                reply_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response_user, &peer_addr.host(), &response.auth),
                response.capability & CLIENT_PROTOCOL_41 != 0,
                connection_id,
                *commands,
            );
        }
    };
    let protocol_41 = capabilities & CLIENT_PROTOCOL_41 != 0;
    if !protocol_41 {
        return rejected_login(
            &mut output,
            reply_sequence,
            ER_ACCESS_DENIED_ERROR,
            *b"28000",
            access_denied_message(&response_user, &peer_addr.host(), &response.auth),
            protocol_41,
            connection_id,
            *commands,
        );
    }
    // Go `handleAuthPlugin` -> `checkAuthPlugin` (`server/conn.go:721`,
    // `:939`). This server registers no extension auth plugin, so Go's
    // `cc.extensions` is the empty registry.
    let extensions = crate::auth_plugin_registry::AuthPluginRegistry::default();
    let plugin_auth = capabilities & CLIENT_PLUGIN_AUTH != 0;
    let mut client_plugin = String::from_utf8_lossy(response.auth_plugin.as_bytes()).into_owned();
    let mut auth_response = response.auth.clone();
    let mut response_sequence = reply_sequence;
    let new_auth = match check_auth_plugin(
        &server_plugin,
        &mut client_plugin,
        plugin_auth,
        users,
        &response_user,
        &peer_addr,
    ) {
        Ok(AuthPluginDecision::Keep) => None,
        Ok(AuthPluginDecision::Switch(plugin)) => {
            // Go `authSwitchRequest`.
            let request =
                AuthSwitchRequest::new(extensions.client_plugin_name(&plugin), salt.to_vec())
                    .map_err(|error| MysqlConnectionError::Handshake(error.to_string()))?;
            write_payload(&mut output, response_sequence, &request.encode_payload())?;
            reader.set_sequence(response_sequence + 1);
            let data = reader.read_packet()?;
            response_sequence += 2;
            Some(data)
        }
        // Go: the peer process's OS user is the `auth_socket` response.
        Ok(AuthPluginDecision::SocketUser) => match stream_for_watch.peer_os_user() {
            Ok(user) => Some(user.into_bytes()),
            Err(error) => {
                return rejected_login(
                    &mut output,
                    response_sequence,
                    ER_UNKNOWN_ERROR,
                    *b"HY000",
                    error.to_string(),
                    protocol_41,
                    connection_id,
                    *commands,
                )
            }
        },
        Err(AuthPluginRefusal::AccessDenied) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response_user, &peer_addr.host(), &response.auth),
                protocol_41,
                connection_id,
                *commands,
            )
        }
        Err(AuthPluginRefusal::NotSupportedAuthMode) => {
            return rejected_login(
                &mut output,
                response_sequence,
                tidb_error::mysql::errcode::ErrNotSupportedAuthMode,
                *b"08004",
                tidb_error::mysql::errname::ErrNotSupportedAuthMode
                    .raw
                    .to_owned(),
                protocol_41,
                connection_id,
                *commands,
            )
        }
    };
    if let Some(data) = new_auth.filter(|data| !data.is_empty()) {
        auth_response = data;
    }
    if !plugin_auth {
        // MySQL 5.1 and older clients don't support authentication plugins.
        client_plugin = tidb_mysql::consts::AuthNativePassword.to_owned();
    }
    // Go `readOptionalSSLRequestAndHandshakeResponse`: the response plugin's
    // own exchange. `authSha`/`authSM3` implement neither the cached fast
    // path nor the RSA key exchange: they always answer `fastAuthFail` and
    // read the cleartext, NUL-trimmed; an empty response skips the exchange
    // (issue 40831).
    match client_plugin.as_str() {
        tidb_mysql::consts::AuthCachingSha2Password | tidb_mysql::consts::AuthTiDBSM3Password => {
            if !auth_response.is_empty() {
                write_payload(
                    &mut output,
                    response_sequence,
                    &[SHA2_FAST_AUTH_COMMAND, SHA2_FAST_AUTH_FAIL],
                )?;
                reader.set_sequence(response_sequence + 1);
                let cleartext = reader.read_packet()?;
                let start = cleartext
                    .iter()
                    .position(|byte| *byte != 0)
                    .unwrap_or(cleartext.len());
                let end = cleartext
                    .iter()
                    .rposition(|byte| *byte != 0)
                    .map_or(start, |last| last + 1);
                auth_response = cleartext[start..end].to_vec();
                response_sequence += 2;
            }
        }
        tidb_mysql::consts::AuthNativePassword
        | tidb_mysql::consts::AuthSocket
        | tidb_mysql::consts::AuthTiDBSessionToken
        | tidb_mysql::consts::AuthTiDBAuthToken
        | tidb_mysql::consts::AuthMySQLClearPassword
        | tidb_mysql::consts::AuthLDAPSASL
        | tidb_mysql::consts::AuthLDAPSimple => {}
        other if extensions.get(other).is_some() => {}
        _ => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_UNKNOWN_ERROR,
                *b"HY000",
                "Unknown auth plugin".to_owned(),
                protocol_41,
                connection_id,
                *commands,
            )
        }
    }
    // Go `openSessionAndDoAuth`: `auth_socket` is refused off a Unix socket.
    if !peer_addr.is_unix_socket() && client_plugin == tidb_mysql::consts::AuthSocket {
        let error = tidb_error::server_errors::ERR_ACCESS_DENIED_NO_PASSWORD
            .generate_with_stack_by_args(&[
                tidb_error::mysql::FormatArg::from(response_user.as_str()),
                tidb_error::mysql::FormatArg::from(peer_addr.host()),
            ]);
        return rejected_login(
            &mut output,
            response_sequence,
            tidb_error::tidb::errcode::ErrAccessDeniedNoPassword,
            *b"28000",
            error.message().to_owned(),
            protocol_41,
            connection_id,
            *commands,
        );
    }
    // The process-wide transport rule already ran before plugin selection.
    // This call consumes that proof and applies only the matched account's
    // own REQUIRE clause plus ordinary/bypassed identity authentication.
    let auth_result = users.authenticate_admitted(
        &response_user,
        &peer_addr.host(),
        &salt,
        &auth_response,
        &client_plugin,
        transport_admission,
    );
    let identity = match auth_result {
        Ok(identity) => identity,
        // Go refuses this one BEFORE authenticating, with its own errno,
        // because it is a property of the connection and not of the account.
        Err(AuthenticationFailure::SecureTransportRequired) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_SECURE_TRANSPORT_REQUIRED,
                *b"HY000",
                SECURE_TRANSPORT_REQUIRED_MESSAGE,
                protocol_41,
                connection_id,
                *commands,
            );
        }
        Err(AuthenticationFailure::AccountLocked) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_ACCOUNT_HAS_BEEN_LOCKED,
                *b"HY000",
                account_locked_message(&response_user, &peer_addr.host()),
                protocol_41,
                connection_id,
                *commands,
            );
        }
        // Go's 3955: the account auto-locked itself after too many
        // consecutive wrong passwords, and the message names the lock time
        // remaining.
        Err(AuthenticationFailure::AutoLocked(lockout)) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_USER_ACCESS_DENIED_BLOCKED_BY_PASSWORD_LOCK,
                *b"HY000",
                lockout.message(),
                protocol_41,
                connection_id,
                *commands,
            );
        }
        // Go's 1862: the password verified but has expired, and the server
        // is not admitting expired logins into a sandbox session.
        Err(AuthenticationFailure::PasswordExpired) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_MUST_CHANGE_PASSWORD_LOGIN,
                *b"HY000",
                tidb_session::privilege::PasswordExpiredLogin.message(),
                protocol_41,
                connection_id,
                *commands,
            );
        }
        Err(AuthenticationFailure::Storage(error)) => {
            if !error.is_result_undetermined() {
                write_error(
                    &mut output,
                    response_sequence,
                    error.code,
                    error.state,
                    error.message,
                    protocol_41,
                )?;
            }
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::AuthenticationRejected,
            });
        }
        Err(AuthenticationFailure::AccessDenied) => {
            return rejected_login(
                &mut output,
                response_sequence,
                ER_ACCESS_DENIED_ERROR,
                *b"28000",
                access_denied_message(&response_user, &peer_addr.host(), &auth_response),
                protocol_41,
                connection_id,
                *commands,
            );
        }
    };
    // The KILL handle is bound to this connection's own socket, so a `KILL`
    // reaching a connection that is idle between commands wakes it up rather
    // than leaving the row in the process list until the client happens to
    // send something.
    let close = ConnectionClose::with_wake(move || {
        let _ = stream_for_close.shutdown(std::net::Shutdown::Both);
    });
    // With mysql --compression-algorithms=zlib,zstd both flags are set, the
    // result is Zlib.
    let (compression, compression_algorithm) = if capabilities & CLIENT_COMPRESS != 0 {
        (CompressionAlgorithm::Zlib, tidb_mysql::consts::CompressionZlib)
    } else if capabilities & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
        (CompressionAlgorithm::Zstd, tidb_mysql::consts::CompressionZstd)
    } else {
        (CompressionAlgorithm::None, tidb_mysql::consts::CompressionNone)
    };
    let mut engine = match factory.open_session(SessionContext {
        connection_id,
        peer_addr,
        identity,
        client_found_rows: capabilities & CLIENT_FOUND_ROWS != 0,
        tls_state: socket.negotiated_tls().map(|(cipher_suite, version)| {
            tidb_session::vars::TlsConnectionState {
                version,
                cipher_suite,
            }
        }),
        compression_algorithm,
        compression_level: zstd_level,
        cancellation: cancellation.clone(),
        close: close.clone(),
    }) {
        Ok(session) => {
            // Go `server.go:1329`-adjacent TLS accounting: every TLS
            // handshake counts its negotiated version and cipher.
            if let Some((cipher, version)) = socket.negotiated_tls() {
                crate::server_metrics::TLS_CIPHER
                    .with_label_values(&[&tidb_util::tls::cipher_suite_name(cipher)])
                    .inc();
                crate::server_metrics::TLS_VERSION
                    .with_label_values(&[&tidb_util::tls::version_name(version)])
                    .inc();
            }
            session
        }
        Err(error) => {
            write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::SessionRejected,
            });
        }
    };
    // Go `TiDBDriver.OpenCtx` runs `session.SetCollation` with the
    // handshake's collation id as soon as the session exists, before the
    // initial database: character_set_client/connection/results take the
    // collation's charset and collation_connection the collation, written
    // directly, with no statement. A pymysql client's utf8mb4 handshake
    // therefore governs connection comparisons with utf8mb4_general_ci, not
    // this server's bin default. An id Go does not know fails the connection.
    let handshake_collation = response.collation;
    if let Err(error) = engine.set_collation(response.collation) {
        write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
        return Ok(ConnectionReport {
            connection_id,
            queries: 0,
            commands: *commands,
            exit: ConnectionExit::SessionRejected,
        });
    }
    // Go `clientConn.connectInfo`: the handshake's attributes ride the
    // session's `ConnectionInfo`.
    engine.set_connection_attributes(
        response
            .attrs
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.to_string_lossy().into_owned(),
                )
            })
            .collect(),
    );
    // Go's `openSessionAndDoAuth`: the handshake's initial database is applied
    // before the connection is reported ready, and a schema that does not
    // exist ends the connection with its own errno rather than the OK packet.
    // A handshake that carried no database leaves the session's empty
    // `CurrentDB` as it is.
    if !response_db_name.is_empty() {
        if let Err(error) = engine.select_database(&response_db_name) {
            write_query_error_at(&mut output, response_sequence, &error, protocol_41)?;
            return Ok(ConnectionReport {
                connection_id,
                queries: 0,
                commands: *commands,
                exit: ConnectionExit::SessionRejected,
            });
        }
    }
    write_ok(
        &mut output,
        response_sequence,
        engine.wire_status(),
        engine.warning_count(),
        protocol_41,
    )?;

    // Authentication is always ordinary MySQL packet I/O. Only after its OK
    // has been flushed does Go activate the negotiated command codec. Keep
    // that one transition here, where both read and write directions are
    // still owned together.
    // Go BufferedReadConn.DefaultReaderSize. Buffer the established command
    // stream below compression, after TLS upgrade can no longer consume raw bytes.
    let input = BufReader::with_capacity(16 * 1024, reader.into_inner());
    let mut reader = PacketIoReader::new(input, compression)?;
    reader.set_max_allowed_packet(runtime.max_allowed_packet);
    let mut output = PacketIoWriter::new(output, compression)?;
    output.set_zstd_level(zstd_level);

    // The connection-lifetime half of the framing, and ALL of it: only the
    // capabilities negotiated at handshake live this long. The status word and
    // the warning count are per-statement facts Go re-reads off the session at
    // every `writeOkWith`/`writeEOF`, so neither is cached here -- caching the
    // status is precisely what reported `autocommit` to a client that had an
    // open transaction, and cost it the transaction's writes.
    let framing = WireFraming {
        deprecate_eof: capabilities & CLIENT_DEPRECATE_EOF != 0,
        protocol_41,
    };
    CommandConnection {
        prepared: PreparedStatementRegistry::default(),
        engine,
        reader,
        output,
        close,
    }
    .run(CommandContext {
        connection_id,
        commands,
        command_limiter,
        capabilities,
        framing,
        cancellation,
        stream_for_watch,
        conn_info: ConnInfo {
            addr: peer_addr.remote_addr(),
            collation: handshake_collation,
            user: response_user.clone(),
        },
    })
}

/// Go `BufferedReadConn.IsAlive` over the connection's raw socket: a
/// non-blocking one-byte peek. Only an end of stream means the client is
/// gone; a timeout, data or any other error answers alive, as Go treats its
/// unknown states. Probes never overlap (Go's `mu.TryLock`): a probe that
/// finds another running answers alive.
struct ConnectionAliveProbe {
    socket: ClientSocket,
    checking: Mutex<()>,
}

impl ConnectionAliveProbe {
    fn is_alive(&self) -> bool {
        let Ok(_checking) = self.checking.try_lock() else {
            return true;
        };
        if self.socket.set_nonblocking(true).is_err() {
            return true;
        }
        let mut byte = [0_u8; 1];
        let peeked = self.socket.peek(&mut byte);
        let _ = self.socket.set_nonblocking(false);
        !matches!(peeked, Ok(0))
    }
}

/// Clears the statement's liveness probe when the statement ends (Go's
/// `clearConnectionAlive`).
struct ConnectionAliveGuard {
    killer: Arc<tidb_util::sqlkiller::SqlKiller>,
    registration: Option<tidb_util::sqlkiller::ConnectionAliveRegistration>,
}

impl Drop for ConnectionAliveGuard {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            self.killer.clear_is_connection_alive(registration);
        }
    }
}

/// Go `clientConn.setSQLKillerConnectionAlive`: installs the probe on the
/// session's SQL killer, which checks it at its own execution checkpoints
/// (`HandleSignal`, at most once a second) -- no background monitor, so a
/// short statement pays nothing.
fn set_sql_killer_connection_alive<S: QuerySession>(
    engine: &S,
    probe: &Arc<ConnectionAliveProbe>,
) -> Option<ConnectionAliveGuard> {
    let killer = engine.sql_killer()?;
    let probe = Arc::clone(probe);
    let registration = killer.set_is_connection_alive(Box::new(move || probe.is_alive()));
    Some(ConnectionAliveGuard {
        killer,
        registration: Some(registration),
    })
}

/// Go clientConn retains its session, statements and PacketIO until Run's
/// recovery has attempted the error response. A raw socket cannot replace this
/// writer: its codec, pending bytes and sequence belong to the same connection.
struct CommandConnection<S: QuerySession> {
    prepared: PreparedStatementRegistry,
    engine: S,
    reader: PacketIoReader<BufReader<ClientStream>>,
    output: PacketIoWriter<ClientErrorRecordingOutput<ClientStream>>,
    close: ConnectionClose,
}

struct CommandContext<'a> {
    connection_id: u64,
    commands: &'a mut ConnectionCommandCounts,
    command_limiter: &'a CommandLimiter,
    capabilities: u32,
    framing: WireFraming,
    cancellation: ConnectionCancellation,
    stream_for_watch: ClientSocket,
    conn_info: ConnInfo,
}

/// The handshake facts Go's `clientConn.String()` prints.
struct ConnInfo {
    /// `bufReadConn.RemoteAddr()`.
    addr: String,
    /// The handshake's collation id (`cc.collation`).
    collation: u8,
    /// The handshake's user (`cc.user`).
    user: String,
}

/// Go `clientConn.Run`'s log for a dispatch that answered an error
/// (`pkg/server/conn.go:1305-1337`): NOWAIT's expected refusal and the
/// MySQL client's `select $$` probe at debug level, every other failure as a
/// `command dispatched failed` warning carrying `errStrForLog(err)` -- the
/// error's `Error()` text, class prefix included.
fn log_dispatch_failure<S: QuerySession>(
    engine: &mut S,
    connection_id: u64,
    conn_info: &ConnInfo,
    payload: &[u8],
    prepared: &PreparedStatementRegistry,
    error: &SqlQueryError,
) {
    if error.identity
        == Some(tidb_error::catalog::tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET.identity())
    {
        tidb_log::debug(
            "Expected error for FOR UPDATE NOWAIT",
            &[crate::zap_error(error.error_string())],
        );
        return;
    }
    let status = engine.wire_status().bits();
    let command = payload.first().copied().unwrap_or(0);
    let command_name = tidb_mysql::consts::COMMAND_NAMES
        .iter()
        .find(|(code, _)| *code == command)
        .map(|(_, name)| *name);
    let data = payload.get(1..).unwrap_or_default();
    let session = engine.migration_session();
    let session = session.as_deref();
    // Go `getLastStmtInConn.String`. A session-less engine (the protocol
    // tests' fakes) has no redaction or length settings to apply.
    let statement_text = |sql: &str, prepared: bool| match session {
        Some(session) if prepared => session.dispatch_failure_prepared_sql(sql),
        Some(session) => session.dispatch_failure_sql(sql),
        None => tidb_session::slow_log::query_replacer(sql),
    };
    let statement_id = || {
        data.get(..4)
            .map(|id| u32::from_le_bytes([id[0], id[1], id[2], id[3]]))
            .unwrap_or(0)
    };
    let sql = match command {
        tidb_mysql::consts::ComInitDB => format!("Use {}", String::from_utf8_lossy(data)),
        tidb_mysql::consts::ComFieldList => {
            format!("ListFields {}", String::from_utf8_lossy(data))
        }
        tidb_mysql::consts::ComQuery | tidb_mysql::consts::ComStmtPrepare => {
            statement_text(&String::from_utf8_lossy(data), false)
        }
        tidb_mysql::consts::ComStmtExecute | tidb_mysql::consts::ComStmtFetch => {
            let id = statement_id();
            let text = match prepared.statements.get(&id) {
                Some(statement) => statement.statement.sql().to_owned(),
                None => format!("prepared statement not found, ID: {id}"),
            };
            statement_text(&text, true)
        }
        tidb_mysql::consts::ComStmtClose | tidb_mysql::consts::ComStmtReset => {
            format!("{} {}", command_name.unwrap_or_default(), statement_id())
        }
        _ => command_name.map_or_else(|| String::from_utf8_lossy(data).into_owned(), str::to_owned),
    };
    if sql == "select $$" {
        tidb_log::debug(
            "command dispatched failed for `select $$`, this is expected",
            &[],
        );
        return;
    }
    // Go `mysql.Collations[uint16(cc.collation)]`.
    let collation =
        tidb_mysql::charset::collation_name(u16::from(conn_info.collation)).unwrap_or_default();
    let fields = [
        tidb_log::Field::new(
            "connInfo",
            tidb_log::Value::Str(format!(
                "id:{connection_id}, addr:{} status:{status:b}, collation:{collation}, user:{}",
                conn_info.addr, conn_info.user
            )),
        ),
        tidb_log::Field::new(
            "command",
            tidb_log::Value::Str(command_name.unwrap_or_default().to_owned()),
        ),
        tidb_log::Field::new(
            "status",
            tidb_log::Value::Str(format!(
                "inTxn:{}, autocommit:{}",
                u8::from(status & crate::wire_status::SERVER_STATUS_IN_TRANS != 0),
                u8::from(status & crate::wire_status::SERVER_STATUS_AUTOCOMMIT != 0),
            )),
        ),
        tidb_log::Field::new("sql", tidb_log::Value::Str(sql)),
        tidb_log::Field::new(
            "txn_mode",
            tidb_log::Value::Str(session.map_or_else(
                || "OPTIMISTIC".to_owned(),
                |session| session.readable_txn_mode(),
            )),
        ),
        tidb_log::Field::new(
            "timestamp",
            tidb_log::Value::U64(session.map_or(0, |session| session.dispatch_failure_timestamp())),
        ),
        tidb_log::Field::new(
            "err",
            tidb_log::Value::Str(session.map_or_else(
                || error.error_string(),
                |session| session.err_str_for_log(&error.error_string(), error.identity),
            )),
        ),
    ];
    tidb_log::warn("command dispatched failed", &fields);
}

impl<S: QuerySession> CommandConnection<S> {
    fn run(
        mut self,
        context: CommandContext<'_>,
    ) -> Result<ConnectionReport, MysqlConnectionError> {
        let connection_id = context.connection_id;
        let protocol_41 = context.framing.protocol_41;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_connection_commands(&mut self, context)
        })) {
            Ok(result) => result,
            Err(payload) => {
                let message = connection_panic_message(payload, connection_id);
                let sequence = self.output.sequence();
                // Go attempts writeError even after a partial response and
                // logs a failed write without replacing the original panic.
                if let Err(error) = write_error(
                    &mut self.output,
                    sequence,
                    ER_UNKNOWN_ERROR,
                    *b"HY000",
                    &message,
                    protocol_41,
                ) {
                    // Go `terror.Log(cc.writeError(...))`.
                    tidb_log::error(
                        "encountered error",
                        &[crate::zap_error(error), crate::zap_stack()],
                    );
                }
                Err(MysqlConnectionError::Panicked(message))
            }
        }
    }
}

impl<S: QuerySession> Drop for CommandConnection<S> {
    fn drop(&mut self) {
        // Go closeConn closes transport before statements/session. Shutdown
        // also retires retained raw clones (KILL handles and test/session refs).
        self.close.request();
    }
}

fn connection_panic_message(payload: Box<dyn std::any::Any + Send>, connection_id: u64) -> String {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    // Go `clientConn.Run`'s recover: "connection running loop panic" with
    // `err` and `stack` under the connection's `conn` field.
    tidb_util::logutil::with_conn_id(&tidb_util::logutil::bg_logger(), connection_id).error(
        "connection running loop panic",
        &[
            tidb_log::Field::new("err", tidb_log::Value::Str(message.clone())),
            crate::zap_stack(),
        ],
    );
    message
}

fn run_connection_commands<S: QuerySession>(
    connection: &mut CommandConnection<S>,
    context: CommandContext<'_>,
) -> Result<ConnectionReport, MysqlConnectionError> {
    let CommandConnection {
        prepared,
        engine,
        reader,
        output,
        close,
    } = connection;
    let CommandContext {
        connection_id,
        commands,
        command_limiter,
        mut capabilities,
        framing,
        cancellation,
        stream_for_watch,
        conn_info,
    } = context;
    let protocol_41 = framing.protocol_41;
    let connection_alive = Arc::new(ConnectionAliveProbe {
        socket: stream_for_watch,
        checking: Mutex::new(()),
    });
    let mut queries = 0_u64;
    let mut last_metrics_sql_type = "general";
    // Go `clientConn.lastActive`: the moment the previous command finished;
    // `dispatch` observes the idle gap before every command (`conn.go:1446`).
    let mut last_active = std::time::Instant::now();
    // Go's `SetReadDeadline` before every packet is a netpoll bookkeeping
    // update, never a syscall. `set_read_timeout` is `setsockopt`, so the
    // socket is only rebound when `@@wait_timeout` actually changed; the
    // deadline itself still follows the session variable statement by
    // statement.
    let mut applied_read_timeout: Option<Option<std::time::Duration>> = None;
    loop {
        // A `KILL` that arrived while the previous command ran ends the
        // connection here, before it serves another one.
        if close.is_closed() {
            return Ok(ConnectionReport {
                connection_id,
                queries,
                commands: *commands,
                exit: ConnectionExit::Killed,
            });
        }
        // Go clientConn.Run resets both counters between commands. A request
        // and its response share an envelope sequence, but the next command
        // starts a new exchange, not a continuation of the previous response.
        reader.set_compressed_sequence(0);
        let wait_timeout = engine.wait_timeout();
        let read_timeout = (!wait_timeout.is_zero()).then_some(wait_timeout);
        if applied_read_timeout != Some(read_timeout) {
            reader
                .get_ref()
                .get_ref()
                .set_read_timeout(read_timeout)
                .map_err(MysqlConnectionError::Io)?;
            applied_read_timeout = Some(read_timeout);
        }
        reader.set_sequence(0);
        // Go `clientConn.readPacket`: the reader is rebound from the SESSION
        // variable before every packet, so the ceiling a client reads out of
        // `@@max_allowed_packet` is the one it is actually cut off at. This
        // node bound the reader once, from `NodeConfig`, and a server booted
        // with `--max-allowed-packet 1048576` therefore answered 67108864
        // while dropping the connection on a 2MB statement.
        if let Some(limit) = engine.max_allowed_packet() {
            reader.set_max_allowed_packet(limit);
        }
        let payload = match reader.read_packet() {
            Ok(payload) => payload,
            // A `KILL` shuts the socket down to wake an idle connection, so
            // the read failure it causes -- including the clean end of stream
            // the shutdown produces -- is that kill, not a peer disconnect.
            Err(_) if close.is_closed() => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
                    commands: *commands,
                    exit: ConnectionExit::Killed,
                });
            }
            Err(PacketError::EndOfStream) => {
                return Ok(ConnectionReport {
                    connection_id,
                    queries,
                    commands: *commands,
                    exit: ConnectionExit::PeerClosed,
                });
            }
            Err(error) => return Err(error.into()),
        };
        output.set_sequence(reader.sequence());
        if let Some(sequence) = reader.compressed_sequence() {
            output.set_compressed_sequence(sequence);
        }
        // Go `clientConn.dispatch` calls `initResultEncoder` here, once per
        // COMMAND: `@@character_set_results` can be `SET` between two
        // statements, and the second one has to go out in the new charset.
        // An unregistered name falls back to Go's unset state, which is what
        // `initResultEncoder` does when the read fails.
        let result_charset = engine.result_charset();
        let result_encoder =
            ResultEncoder::new(result_charset.as_ref()).unwrap_or_else(|_| ResultEncoder::null());
        let mut command_metrics = crate::query_metrics::CommandMetrics::start(
            payload.first().copied().unwrap_or(0),
            engine.metrics_resource_group(),
        );
        command_metrics.sql_type = last_metrics_sql_type;
        // Go `conn.go:1441-1447`: every dispatch first observes how long the
        // connection sat idle since the previous command, labeled by the
        // session's IN-TRANS status read as "0"/"1".
        {
            let idle = last_active.elapsed().as_secs_f64();
            let in_txn =
                engine.wire_status().bits() & crate::wire_status::SERVER_STATUS_IN_TRANS != 0;
            crate::server_metrics::CONN_IDLE_DURATION
                .with_label_values(&[if in_txn { "1" } else { "0" }])
                .observe(idle);
        }
        let _command_token = command_limiter.acquire();
        let _ = crate::connection_writers::take_query_failure();
        let dispatch_result = (|| -> Result<Option<ConnectionReport>, MysqlConnectionError> {
            let command = match decode_command(&payload) {
                Ok(command) => command,
                Err(error) => {
                    write_error(
                        output,
                        1,
                        ER_UNKNOWN_COM_ERROR,
                        ER_UNKNOWN_COM_ERROR_STATE,
                        error.to_string(),
                        protocol_41,
                    )?;
                    return Ok(None);
                }
            };
            let _query_cancellation = engine
                .query_cancellation()
                .map(|active| cancellation.install(active));
            match command {
                Command::Quit => {
                    return Ok(Some(ConnectionReport {
                        connection_id,
                        queries,
                        commands: *commands,
                        exit: ConnectionExit::Quit,
                    }));
                }
                // Go `clientConn.writeStats`: a RAW payload, not an OK packet --
                // `mysqladmin status` prints the line verbatim. Every counter but
                // uptime is the literal zero Go sends; it does not track them
                // either, so inventing numbers here would report a fiction the
                // server cannot back.
                Command::Statistics => {
                    let uptime = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |since| since.as_secs() as i64)
                        .saturating_sub(crate::real_tikv_node::server_start_unix_timestamp())
                        .max(0);
                    let line = format!(
                        "Uptime: {uptime}  Threads: 0  Questions: 0  Slow queries: 0  \
                     Opens: 0  Flush tables: 0  Open tables: 0  \
                     Queries per second avg: 0.000"
                    );
                    crate::connection_writers::write_payload(output, 1, line.as_bytes())?;
                }
                Command::Refresh(data) => {
                    // Go `handleRefresh` (`pkg/server/conn.go:2875-2882`) treats
                    // refresh targets other than 0x01 as no-ops.  `0x01` first
                    // runs `FLUSH PRIVILEGES` through the ordinary SQL path,
                    // which writes one OK, and then writes the command's own OK
                    // as well.  Keep both packets: clients expect the second
                    // response and consume it before sending their next command.
                    let Some(subcommand) = data.first().copied() else {
                        write_error(
                            output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            "malform packet error",
                            protocol_41,
                        )?;
                        return Ok(None);
                    };
                    let mut next_sequence = 1;
                    if subcommand == 0x01 {
                        const FLUSH_PRIVILEGES: &str = "FLUSH PRIVILEGES";
                        let executed = engine.parse_statement(FLUSH_PRIVILEGES).and_then(|stmt| {
                            engine
                                .execute_stmt(FLUSH_PRIVILEGES, &stmt)
                                .map(|result| drop(result))
                        });
                        if let Err(error) = executed {
                            write_query_error(output, &error, protocol_41)?;
                            record_client_warnings(&*output, &*engine);
                            return Ok(None);
                        }
                        write_affected_rows_ok_with_info(
                            output,
                            1,
                            engine.affected_rows(),
                            engine.last_insert_id(),
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                            &engine.statement_info(),
                        )?;
                        record_client_warnings(&*output, &*engine);
                        queries += 1;
                        next_sequence = 2;
                    }
                    write_ok(
                        output,
                        next_sequence,
                        engine.wire_status(),
                        engine.warning_count(),
                        protocol_41,
                    )?;
                    record_client_warnings(&*output, &*engine);
                }
                Command::Ping => write_ok(
                    output,
                    1,
                    engine.wire_status(),
                    engine.warning_count(),
                    protocol_41,
                )?,
                Command::Query(bytes) => {
                    let query_started = std::time::Instant::now();
                    commands.text_query_commands += 1;
                    // `decode_command` has already trimmed exactly one terminal
                    // NUL for issue 1989. Embedded and repeated NUL bytes remain
                    // parser-visible here.
                    let input_charset = engine.input_charset();
                    let sql = decode_client_sql(&bytes, input_charset.as_ref());
                    let sql = sql.as_str();
                    // Go `handleQuery` (`conn.go:1861`): the text parses as a
                    // whole, and each admitted statement runs in order. Every
                    // response but the LAST carries SERVER_MORE_RESULTS_EXISTS
                    // (`conn.go:2269`); an execution error aborts the remainder,
                    // as Go's loop returns on error.
                    let statements = match engine
                        .split_statements(sql, capabilities & CLIENT_MULTI_STATEMENTS != 0)
                    {
                        Ok(statements) => statements,
                        Err(error) => {
                            write_query_error(output, &error, protocol_41)?;
                            return Ok(None);
                        }
                    };
                    // Go `conn.go:1874`: a text that parsed to ZERO statements —
                    // comments, whitespace, bare semicolons — answers one plain
                    // OK packet.
                    if statements.is_empty() {
                        write_ok(
                            output,
                            1,
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                        )?;
                        queries += 1;
                        return Ok(None);
                    }
                    // Go `conn.go:1915`: `NumOfMultiQueryHistogram` observes
                    // how many statements one multi-statement text carried.
                    crate::server_metrics::MULTI_QUERY_NUM.observe(statements.len() as f64);
                    let last_index = statements.len().saturating_sub(1);
                    // One COM_QUERY, one packet numbering: the chained results
                    // CONTINUE the sequence rather than restarting at 1.
                    let mut sequence: u8 = 1;
                    let mut aborted = false;
                    for (statement_index, sql) in statements.iter().enumerate() {
                        let sql = sql.as_str();
                        let more_results = statement_index < last_index;
                        let stamp = |status: WireStatus| {
                            if more_results {
                                status.with(crate::wire_status::SERVER_STATUS_MORE_RESULTS_EXISTS)
                            } else {
                                status
                            }
                        };
                        // Go `handleQuery` parses the command ONCE and hands each
                        // node to `handleStmt`; every door below takes that node
                        // and none parses the text again.
                        let stmt = match engine.parse_statement(sql) {
                            Ok(stmt) => stmt,
                            Err(error) => {
                                // go session.go:1955-1968's parse-failure
                                // half: fresh warning context + the error row
                                // (see the session's own record_parse_failure).
                                engine.record_parse_failure(&error);
                                write_query_error_at(output, sequence, &error, protocol_41)?;
                                aborted = true;
                                break;
                            }
                        };
                        if let Err(error) = prepared.session_states_handler(engine, &stmt) {
                            engine.record_parse_failure(&error);
                            write_query_error_at(output, sequence, &error, protocol_41)?;
                            aborted = true;
                            break;
                        }
                        command_metrics.sql_type = stmt.label();
                        // Go `executor.go`: StmtNodeCounter counts every
                        // executed statement by its executor label.
                        tidb_executor::metrics::STATEMENT_TOTAL
                            .with_label_values(&["", engine.metrics_resource_group(), stmt.label()])
                            .inc();
                        // Go excludes text DDL from addQueryMetrics.
                        command_metrics.skip = matches!(stmt, Stmt::Ddl(_));
                        // Go `handleStmt`: the statement runs with the connection's
                        // liveness probe on its SQL killer, cleared when it ends.
                        let _connection_alive = engine
                            .should_install_connection_alive(&stmt)
                            .then(|| set_sql_killer_connection_alive(engine, &connection_alive))
                            .flatten();
                        // Go `handleStmt`: one `ExecuteStmt`. A result set is
                        // written as rows; no result set is an OK packet, after
                        // the client's file when the statement parked a request
                        // for one (`handleLoadData`, `handleLoadStats`).
                        let mut result = {
                            let executed = engine.execute_stmt(sql, &stmt);
                            match executed {
                                Ok(Some(result)) => result,
                                // Consumed here so no part of it still borrows the
                                // session below.
                                other => {
                                    if let Err(error) = other.map(drop) {
                                        write_query_error_at(
                                            output,
                                            sequence,
                                            &error,
                                            protocol_41,
                                        )?;
                                        record_client_warnings(&*output, &*engine);
                                        aborted = true;
                                        break;
                                    }
                                    if let Some(path) = engine.take_local_infile_request() {
                                        if capabilities & CLIENT_LOCAL_FILES == 0 {
                                            let error = SqlQueryError::from(
                                                tidb_error::mysql::MysqlError::from_terror(
                                                    &tidb_error::catalog::servererr::ERR_NOT_ALLOWED_COMMAND
                                                        .generate_with_stack_by_args(&[]),
                                                ),
                                            );
                                            write_query_error_at(
                                                output,
                                                sequence,
                                                &error,
                                                protocol_41,
                                            )?;
                                            record_client_warnings(&*output, &*engine);
                                            aborted = true;
                                            break;
                                        }
                                        let mut request = Vec::with_capacity(path.len() + 1);
                                        request.push(0xfb);
                                        request.extend_from_slice(path.as_bytes());
                                        write_payload(output, sequence, &request)?;
                                        if let Some(compressed_sequence) =
                                            output.compressed_sequence()
                                        {
                                            reader.set_compressed_sequence(compressed_sequence);
                                        }
                                        reader.set_sequence(sequence.wrapping_add(1));
                                        let mut data = Vec::new();
                                        loop {
                                            let packet = reader.read_packet()?;
                                            if packet.is_empty() {
                                                break;
                                            }
                                            data.extend_from_slice(&packet);
                                        }
                                        if let Some(compressed_sequence) =
                                            reader.compressed_sequence()
                                        {
                                            output.set_compressed_sequence(compressed_sequence);
                                        }
                                        sequence = reader.sequence();
                                        output.set_sequence(sequence);
                                        if let Err(error) = engine.finish_local_infile(&data) {
                                            write_query_error_at(
                                                output,
                                                sequence,
                                                &error,
                                                protocol_41,
                                            )?;
                                            record_client_warnings(&*output, &*engine);
                                            aborted = true;
                                            break;
                                        }
                                    }
                                    write_affected_rows_ok_with_info(
                                        output,
                                        sequence,
                                        engine.affected_rows(),
                                        engine.last_insert_id(),
                                        stamp(engine.wire_status()),
                                        engine.warning_count(),
                                        protocol_41,
                                        &engine.statement_info(),
                                    )?;
                                    record_client_warnings(&*output, &*engine);
                                    sequence = sequence.wrapping_add(1);
                                    queries += 1;
                                    continue;
                                }
                            }
                        };
                        let (write_result, next_sequence) = {
                            let statement_options = framing.result_set_with_output(
                                stamp(result.wire_status()),
                                result.warning_count(),
                                result.affected_rows(),
                                result.last_insert_id(),
                                result.info().to_vec(),
                                result_encoder,
                            );
                            let mut sink = TcpResultSetSink::new(output, sequence);
                            let written = write_connection_result_set_to_sink(
                                result.source(),
                                &mut sink,
                                statement_options,
                                RESULT_BATCH_SIZE,
                            );
                            (written, sink.next_sequence())
                        };
                        sequence = next_sequence;
                        match write_result {
                            Ok(_) => {
                                queries += 1;
                                drop(result);
                            }
                            Err(error) if !error.bytes_escaped => {
                                write_error(
                                    output,
                                    1,
                                    error.cause.code,
                                    error.cause.state,
                                    error.cause.message,
                                    protocol_41,
                                )?;
                                drop(result);
                                record_client_warnings(&*output, &*engine);
                                aborted = true;
                                break;
                            }
                            Err(error) => {
                                drop(result);
                                engine.finish_execute_stmt(query_started.elapsed());
                                return Err(MysqlConnectionError::PartialResult(
                                    error.cause.message,
                                ));
                            }
                        }
                        record_client_warnings(&*output, &*engine);
                    }
                    if !aborted {
                        engine.flush_multi_statement_warning();
                    }
                    engine.finish_execute_stmt(query_started.elapsed());
                    // Go charges the slow-query families inside
                    // `LogSlowQuery` (`adapter.go:2007-2024`) when the
                    // statement cost reaches the threshold. The families'
                    // `sql_type` label carries Go's internal/general scope
                    // split (`executor_metrics.TotalQueryProcHistogramGeneral`
                    // is pre-bound with `LblGeneral`), not the statement kind;
                    // the wire serves general sessions only. This node has no
                    // coprocessor time detail yet, so the cop/wait/mvcc
                    // histograms stay uncharged, and one observation covers
                    // the whole command.
                    {
                        let elapsed = query_started.elapsed();
                        if engine
                            .slow_log_threshold()
                            .is_some_and(|threshold| elapsed >= threshold)
                        {
                            crate::server_metrics::SLOW_QUERY_PROCESS_DURATION
                                .with_label_values(&["general"])
                                .observe(elapsed.as_secs_f64());
                            crate::server_metrics::SLOW_QUERY_TOTAL
                                .with_label_values(&["general"])
                                .inc();
                        }
                    }
                }
                Command::StmtPrepare(bytes) => {
                    commands.stmt_prepare_commands += 1;
                    let input_charset = engine.input_charset();
                    let sql = decode_client_sql(&bytes, input_charset.as_ref());
                    let sql = sql.as_str();
                    // Transaction control is claimed before any planner sees the
                    // statement, because a prepared `BEGIN` is a `BEGIN`: its
                    // meaning is the connection's transaction, not a plan. Left to
                    // the general path it would be *executed* by the prepare-time
                    // column probe and then executed again at EXECUTE, and neither
                    // run would reach `control_transaction` -- so the connection's
                    // transaction never opens and every statement of the
                    // transaction reads as an autocommit statement -- a point get
                    // among them at `MaxUint64`, seeing whatever is committed at
                    // the instant it runs rather than the transaction's snapshot.
                    // The predicate
                    // is the same one the text arm routes on, so a statement takes
                    // the same route whichever protocol carried it.
                    let statement = match engine.prepare_general(sql) {
                        Ok(statement) => statement,
                        Err(error) => {
                            write_query_error(output, &error, protocol_41)?;
                            return Ok(None);
                        }
                    };
                    let database = engine
                        .migration_session()
                        .map(|session| {
                            prepared.next_id = Some(session.allocate_prepared_statement_id());
                            session.current_database().to_owned()
                        })
                        .unwrap_or_default();
                    let result_columns = statement.result_columns().to_vec();
                    let parameter_count = statement.parameter_count();
                    let statement_id = match prepared.insert(statement) {
                        Ok(statement_id) => statement_id,
                        Err(message) => {
                            write_error(
                                output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                message,
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    prepared
                        .statements
                        .get_mut(&statement_id)
                        .expect("just inserted")
                        .database = database;
                    let parameter_columns = vec![prepared_parameter_column(); parameter_count];
                    // Go `conn_stmt.go:111`/`:129` frames the prepare metadata
                    // with `cc.writeEOF(ctx, cc.ctx.Status())` -- the live word,
                    // like every other EOF.
                    let packets = match encode_prepared_statement_prepare_response(
                        statement_id,
                        &parameter_columns,
                        &result_columns,
                        framing.result_set(engine.wire_status(), 0, result_encoder),
                    ) {
                        Ok(packets) => packets,
                        Err(error) => {
                            drop(prepared.remove(statement_id, engine));
                            write_error(
                                output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                error.to_string(),
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    let mut sink = TcpResultSetSink::new(output, 1);
                    for packet in packets {
                        sink.write_payload(&packet)
                            .map_err(|error| MysqlConnectionError::PartialResult(error.message))?;
                    }
                    sink.flush()
                        .map_err(|error| MysqlConnectionError::PartialResult(error.message))?;
                    commands.stmt_prepare_successes += 1;
                }
                Command::StmtExecute(bytes) => {
                    let execute_started = std::time::Instant::now();
                    commands.stmt_execute_commands += 1;
                    let statement_id = match prepared_statement_id(&bytes) {
                        Ok(statement_id) => statement_id,
                        Err(message) => {
                            write_error(
                                output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                message,
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    let Some(statement) = prepared.get(statement_id) else {
                        write_unknown_statement(output, statement_id, "stmt_execute", protocol_41)?;
                        return Ok(None);
                    };
                    // The statement is immutable after PREPARE.  Cloning the
                    // `Arc` keeps the registry borrow short without cloning the
                    // parser-owned AST on every execute.
                    command_metrics.sql_type = statement.metrics_label;
                    let statement_label = statement.metrics_label;
                    let prepared_statement = Arc::clone(&statement.statement);
                    let previous_types = statement.parameter_types.clone();
                    // The marker count is per statement: a point read owns one, a
                    // write owns one per bound column plus its handle.
                    let parameter_count = prepared_statement.parameter_count();
                    // Go reads `stmt.BoundParams()` into `parseBinaryParams` and
                    // then calls `stmt.Reset()` unconditionally -- on the decode
                    // error path too (`pkg/server/conn_stmt.go:212-217`), so a
                    // rejected execute still consumes the long data.
                    let long_data_error =
                        prepared.long_data_error(statement_id, engine.connection_id());
                    let bound_params = prepared.bound_params(statement_id).to_vec();
                    prepared.clear_bound_params(statement_id, engine);
                    // Go calls `stmt.Reset()` after parsing every execute packet,
                    // successful or not. The retained cursor therefore closes
                    // before a replacement execution starts, and a malformed
                    // replacement cannot leave the old cursor fetchable.
                    drop(prepared.take_cursor(statement_id));
                    if let Some(error) = long_data_error {
                        write_query_error(output, &error, protocol_41)?;
                        return Ok(None);
                    }
                    let execute_packet = match split_prepared_statement_execute(
                        &bytes,
                        parameter_count,
                        previous_types.as_deref(),
                    ) {
                        Ok(packet) => packet,
                        Err(error) => {
                            write_error(
                                output,
                                1,
                                error.mysql_error_code().unwrap_or(ER_UNKNOWN_ERROR),
                                *b"HY000",
                                error.to_string(),
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    if execute_packet.statement_id() != statement_id {
                        write_error(
                            output,
                            1,
                            ER_UNKNOWN_ERROR,
                            *b"HY000",
                            "prepared statement ID changed during decode",
                            protocol_41,
                        )?;
                        return Ok(None);
                    }
                    if let PreparedParameterTypes::New(types) = execute_packet.parameter_types() {
                        prepared.remember_parameter_types(statement_id, types);
                    }
                    let input_charset = engine.input_charset();
                    let execute = match execute_packet.decode(&bound_params, input_charset.as_ref())
                    {
                        Ok(execute) => execute,
                        Err(error) => {
                            write_error(
                                output,
                                1,
                                error.mysql_error_code().unwrap_or(ER_UNKNOWN_ERROR),
                                *b"HY000",
                                error.to_string(),
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    let values = execute.values;
                    // Go `executor.Compile` (`pkg/executor/compiler.go:125`)
                    // counts every optimized statement through CountStmtNode,
                    // including `EXECUTE` -- the prepared object supplies the
                    // inner statement for the label -- so the binary protocol
                    // feeds `tidb_executor_statement_total` exactly like the
                    // text protocol does.
                    tidb_executor::metrics::STATEMENT_TOTAL
                        .with_label_values(&["", engine.metrics_resource_group(), statement_label])
                        .inc();
                    let general = prepared_statement.as_ref();
                    if let Some(ast) = general.prepared_ast() {
                        if let Err(error) = prepared.session_states_handler(engine, ast.statement())
                        {
                            write_query_error(output, &error, protocol_41)?;
                            return Ok(None);
                        }
                    }
                    // Go `executePreparedStmtAndWriteResult`: the execute runs
                    // with the liveness probe when its prepared statement
                    // would on the text protocol.
                    let _connection_alive = general
                        .template()
                        .filter(|stmt| engine.should_install_connection_alive(stmt))
                        .and_then(|_| set_sql_killer_connection_alive(engine, &connection_alive));
                    // Go's read-only cursor: the execute materializes the
                    // rows, holds them on the statement, and answers with
                    // only the column definitions plus an EOF whose
                    // status advertises the open cursor; the rows travel
                    // later through COM_STMT_FETCH.
                    if execute.cursor_flags & tidb_protocol::CURSOR_TYPE_READ_ONLY != 0 {
                        let mut write_outcome = None;
                        match engine.execute_general(general, &values) {
                            Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                                match open_prepared_cursor(
                                    &mut result,
                                    output,
                                    framing,
                                    result_encoder,
                                ) {
                                    Ok(cursor) => {
                                        drop(result);
                                        drop(prepared.open_cursor(statement_id, cursor));
                                        queries += 1;
                                        commands.stmt_execute_successes += 1;
                                    }
                                    Err(PreparedCursorOpenError::Query(error)) => {
                                        drop(result);
                                        write_query_error(output, &error, protocol_41)?;
                                    }
                                    Err(PreparedCursorOpenError::Transport(error)) => {
                                        return Err(error);
                                    }
                                }
                            }
                            // Go clears the cursor bit when the statement
                            // produced no result set and answers a plain
                            // OK. The answer is written after the match
                            // because its warning count is read off the
                            // session, whose borrow the result arm holds
                            // for as long as the match scrutinee lives.
                            Ok(GeneralExecuteOutcome::Write(outcome)) => {
                                write_outcome = Some(outcome);
                            }
                            Err(error) => {
                                write_query_error(output, &error, protocol_41)?;
                            }
                        }
                        if let Some(outcome) = write_outcome {
                            let info = engine.statement_info();
                            write_affected_rows_ok_with_info(
                                output,
                                1,
                                outcome.affected_rows,
                                outcome.last_insert_id,
                                engine.wire_status(),
                                engine.warning_count(),
                                protocol_41,
                                &info,
                            )?;
                            queries += 1;
                            commands.stmt_execute_successes += 1;
                        }
                        record_client_warnings(&*output, &*engine);
                        return Ok(None);
                    }
                    let mut write_outcome = None;
                    match engine.execute_general(general, &values) {
                        Ok(GeneralExecuteOutcome::Rows(mut result)) => {
                            let write_result = {
                                let statement_options = framing.result_set_with_output(
                                    result.wire_status(),
                                    result.warning_count(),
                                    result.affected_rows(),
                                    result.last_insert_id(),
                                    result.info().to_vec(),
                                    result_encoder,
                                );
                                let mut sink = TcpResultSetSink::new(output, 1);
                                write_connection_binary_result_set_to_sink(
                                    result.source(),
                                    &mut sink,
                                    statement_options,
                                    RESULT_BATCH_SIZE,
                                )
                            };
                            match write_result {
                                Ok(_) => {
                                    queries += 1;
                                    commands.stmt_execute_successes += 1;
                                }
                                Err(error) if !error.bytes_escaped => {
                                    write_error(
                                        output,
                                        1,
                                        error.cause.code,
                                        error.cause.state,
                                        error.cause.message,
                                        protocol_41,
                                    )?;
                                }
                                Err(error) => {
                                    return Err(MysqlConnectionError::PartialResult(
                                        error.cause.message,
                                    ))
                                }
                            }
                        }
                        // Answered after the match: see the cursor arm
                        // above for why the session cannot be read while
                        // the scrutinee is alive.
                        Ok(GeneralExecuteOutcome::Write(outcome)) => {
                            write_outcome = Some(outcome);
                        }
                        Err(error) => {
                            write_query_error(output, &error, protocol_41)?;
                        }
                    }
                    if let Some(outcome) = write_outcome {
                        let info = engine.statement_info();
                        write_affected_rows_ok_with_info(
                            output,
                            1,
                            outcome.affected_rows,
                            outcome.last_insert_id,
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                            &info,
                        )?;
                        queries += 1;
                        commands.stmt_execute_successes += 1;
                    }
                    record_client_warnings(&*output, &*engine);
                    engine.finish_execute_stmt(execute_started.elapsed());
                }
                Command::StmtSendLongData(bytes) => {
                    commands.stmt_send_long_data_commands += 1;
                    // Go's `handleStmtSendLongData` returns nil on success, and a
                    // nil return from `clientConn.dispatch` writes NO packet at
                    // all (`pkg/server/conn.go:1578-1579`): the client is not
                    // reading, so any reply here would be consumed as the answer
                    // to the NEXT command. Its error returns, by contrast, DO
                    // reach the wire -- `dispatch`'s caller ends the failed
                    // command with `cc.writeError(ctx, err)`
                    // (`pkg/server/conn.go:1338`) -- so the two error shapes below
                    // are packets exactly as they are in Go.
                    match decode_prepared_statement_send_long_data(&bytes) {
                        Ok(long_data) => {
                            let statement_id = long_data.statement_id;
                            match prepared.append_param(
                                statement_id,
                                usize::from(long_data.parameter_id),
                                &long_data.chunk,
                                engine,
                            ) {
                                Ok(()) => {}
                                Err(AppendParamError::UnknownStatement) => {
                                    write_unknown_statement(
                                        output,
                                        statement_id,
                                        "stmt_send_longdata",
                                        protocol_41,
                                    )?;
                                }
                                // Go `AppendParam`'s own
                                // `ErrWrongArguments("stmt_send_longdata")`.
                                Err(AppendParamError::ParameterOutOfRange) => write_error(
                                    output,
                                    1,
                                    ER_WRONG_ARGUMENTS,
                                    *b"HY000",
                                    "Incorrect arguments to stmt_send_longdata",
                                    protocol_41,
                                )?,
                            }
                        }
                        Err(error) => write_error(
                            output,
                            1,
                            ER_WRONG_ARGUMENTS,
                            *b"HY000",
                            error.to_string(),
                            protocol_41,
                        )?,
                    }
                }
                Command::StmtClose(bytes) => {
                    commands.stmt_close_commands += 1;
                    if let Ok(statement_id) = decode_prepared_statement_close(&bytes) {
                        drop(prepared.remove(statement_id, engine));
                    }
                }
                Command::StmtReset(bytes) => {
                    commands.stmt_reset_commands += 1;
                    match decode_prepared_statement_close(&bytes) {
                        // The payload is the same four-byte statement id the
                        // close command carries.
                        Ok(statement_id) => match prepared.reset(statement_id, engine) {
                            Ok(cursor) => {
                                drop(cursor);
                                // COM_STMT_RESET runs no statement, so like Go's
                                // `writeOK` here it reports the buffer as it stands.
                                write_affected_rows_ok(
                                    output,
                                    1,
                                    0,
                                    0,
                                    engine.wire_status(),
                                    engine.warning_count(),
                                    protocol_41,
                                    &[],
                                )?;
                            }
                            Err(()) => write_unknown_statement(
                                output,
                                statement_id,
                                "stmt_reset",
                                protocol_41,
                            )?,
                        },
                        Err(error) => {
                            write_error(
                                output,
                                1,
                                ER_WRONG_ARGUMENTS,
                                *b"HY000",
                                error.to_string(),
                                protocol_41,
                            )?;
                        }
                    }
                }
                Command::StmtFetch(bytes) => {
                    commands.stmt_fetch_commands += 1;
                    let (statement_id, fetch_size) = match decode_prepared_statement_fetch(&bytes) {
                        Ok(decoded) => decoded,
                        Err(_error) => {
                            write_error(
                                output,
                                1,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                tidb_error::mysql::ERR_MALFORM_PACKET,
                                protocol_41,
                            )?;
                            return Ok(None);
                        }
                    };
                    if prepared.get(statement_id).is_none() {
                        write_unknown_statement(output, statement_id, "stmt_fetch", protocol_41)?;
                        return Ok(None);
                    }
                    let Some(mut cursor) = prepared.take_cursor(statement_id) else {
                        // Go `ErrSpCursorNotOpen` (1326).
                        write_error(
                            output,
                            1,
                            1326,
                            *b"24000",
                            "Cursor is not open",
                            protocol_41,
                        )?;
                        return Ok(None);
                    };
                    // Go sends up to fetch_size binary rows and an EOF; when the
                    // iterator is exhausted the EOF drops the cursor bit, sets
                    // ServerStatusLastRowSend, and the statement resets.
                    let (row_count, exhausted) = cursor.fetch_plan(fetch_size);
                    let status = if exhausted {
                        engine
                            .wire_status()
                            .with(SERVER_STATUS_LAST_ROW_SEND)
                            .without(SERVER_STATUS_CURSOR_EXISTS)
                    } else {
                        engine.wire_status().with(SERVER_STATUS_CURSOR_EXISTS)
                    };
                    match cursor.write_fetch(
                        output,
                        row_count,
                        // Go clears the statement warning buffer before FETCH,
                        // then writes the live transaction status.
                        framing.result_set(status, 0, result_encoder),
                    ) {
                        Ok(()) => {
                            if !exhausted {
                                drop(prepared.open_cursor(statement_id, cursor));
                            }
                            commands.stmt_fetch_successes += 1;
                        }
                        Err(CursorFetchError::Protocol { message, sequence }) => {
                            write_error(
                                output,
                                sequence,
                                ER_UNKNOWN_ERROR,
                                *b"HY000",
                                message,
                                protocol_41,
                            )?;
                        }
                        Err(CursorFetchError::Transport(error)) => return Err(error),
                    }
                }
                // The `mysql` client implements `USE db` as COM_INIT_DB, not as a
                // query, so this is the command an interactive `USE` arrives on.
                Command::InitDb(name) => {
                    let name = String::from_utf8_lossy(&name).into_owned();
                    match engine.select_database(&name) {
                        Ok(()) => write_ok(
                            output,
                            1,
                            engine.wire_status(),
                            engine.warning_count(),
                            protocol_41,
                        )?,
                        Err(error) => write_query_error(output, &error, protocol_41)?,
                    }
                }
                Command::SetOption(data) => {
                    // Go `handleSetOption` (conn_stmt.go:651-672): the two-byte
                    // option word toggles CLIENT_MULTI_STATEMENTS (0 = on, 1 =
                    // off) and the reply is an EOF packet carrying the live
                    // status -- never an ERR. JDBC sends this during connection
                    // setup with `allowMultiQueries=true`; answering ERR there
                    // fails the whole connection.
                    if data.len() < 2 {
                        return Err(MysqlConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "malform packet error",
                        )));
                    }
                    match u16::from_le_bytes([data[0], data[1]]) {
                        0 => capabilities |= CLIENT_MULTI_STATEMENTS,
                        1 => capabilities &= !CLIENT_MULTI_STATEMENTS,
                        _ => {
                            return Err(MysqlConnectionError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "malform packet error",
                            )));
                        }
                    }
                    write_eof_or_ok(
                        output,
                        1,
                        framing.result_set(
                            engine.wire_status(),
                            engine.warning_count(),
                            result_encoder,
                        ),
                    )?;
                }
                // Go answers COM_SHUTDOWN with handleQuery("SHUTDOWN") and
                // COM_CHANGE_USER with handleChangeUser; both need the full
                // server, which this read-only node does not implement yet.
                Command::FieldList(_)
                | Command::ResetConnection
                | Command::Shutdown
                | Command::ChangeUser(_) => write_error(
                    output,
                    1,
                    ER_UNKNOWN_COM_ERROR,
                    ER_UNKNOWN_COM_ERROR_STATE,
                    "command is not supported by the read-only Rust SQL node",
                    protocol_41,
                )?,
                Command::Unknown { code, .. } => {
                    // Go's default dispatch arm answers a genuinely unknown
                    // command with generic 1105/HY000 and the command byte in the
                    // message.  1047/08S01 belongs only to a known command that
                    // this node refuses, not to an unowned command byte.
                    let error = unknown_command_error(code);
                    write_query_error(output, &error, protocol_41)?;
                }
            }
            Ok(None)
        })();
        last_metrics_sql_type = command_metrics.sql_type;
        command_metrics.set_resource_groups(
            engine.metrics_resource_group(),
            engine.metrics_statement_resource_group(),
        );
        if dispatch_result.is_err() {
            crate::query_metrics::record_error();
        }
        if let Some(error) = crate::connection_writers::take_query_failure() {
            log_dispatch_failure(
                engine,
                connection_id,
                &conn_info,
                &payload,
                prepared,
                &error,
            );
        }
        last_active = std::time::Instant::now();
        if let Some(report) = dispatch_result? {
            return Ok(report);
        }
    }
}

#[cfg(test)]
mod client_error_recording_output_tests {
    use std::io::{IoSlice, Write};

    use super::ClientErrorRecordingOutput;

    /// Records each transport write as the packet writer's frame layer sees it.
    #[derive(Default)]
    struct RecordingSink {
        writes: Vec<Vec<u8>>,
    }

    impl Write for RecordingSink {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.writes.push(buffer.to_vec());
            Ok(buffer.len())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> std::io::Result<usize> {
            self.writes.push(
                buffers
                    .iter()
                    .flat_map(|buffer| buffer.iter().copied())
                    .collect(),
            );
            Ok(buffers.iter().map(|buffer| buffer.len()).sum())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The packet writer hands a frame down as one vectored write of header
    /// plus payload; the wrapper must keep it one transport write, as Go's
    /// `PacketIO` flushes a response as one write. The default
    /// `write_vectored` would forward only the first slice, splitting every
    /// response into two TLS records and two syscalls.
    #[test]
    fn a_vectored_frame_write_reaches_the_transport_as_one_write() {
        let mut output = ClientErrorRecordingOutput::new(
            RecordingSink::default(),
            "root".to_owned(),
            "localhost".to_owned(),
        );
        let header = [7u8, 0, 0, 1];
        let payload = b"\x00\x00\x00\x02\x00\x00\x00";

        let written = output
            .write_vectored(&[IoSlice::new(&header), IoSlice::new(payload)])
            .expect("the sink accepts the frame");

        assert_eq!(written, header.len() + payload.len());
        assert_eq!(
            output.inner.writes,
            vec![[header.as_slice(), payload.as_slice()].concat()],
            "header and payload must leave as one transport write"
        );
    }
}


#[cfg(test)]
mod undetermined_tests {
    use crate::sql_node::SqlQueryError;
    use tidb_error::terror::ERR_RESULT_UNDETERMINED;

    /// Go `pkg/server/conn.go:1288-1291` returns from the command loop for an
    /// undetermined result, writing no ERR packet: the client must not be able
    /// to read the failure as a verdict and retry a commit that may have
    /// landed.
    #[test]
    fn an_undetermined_result_is_never_answered_with_an_err_packet() {
        let undetermined = SqlQueryError::result_undetermined();
        assert!(undetermined.is_result_undetermined());
        assert_eq!(undetermined.message, ERR_RESULT_UNDETERMINED.message());

        // Every ordinary failure still gets its ERR packet; only this one does
        // not, and the refusal lives in the single shared writer so no answer
        // path can forget it.
        let ordinary = SqlQueryError::unknown("configured write did not commit: RolledBack");
        assert!(!ordinary.is_result_undetermined());
        let same_code_different_text = SqlQueryError::unknown("execution result undetermine");
        assert!(!same_code_different_text.is_result_undetermined());
        let different_code = SqlQueryError::new(1062, *b"23000", ERR_RESULT_UNDETERMINED.message());
        assert!(!different_code.is_result_undetermined());
    }
}

#[cfg(test)]
mod decode_client_sql_tests {
    use super::decode_client_sql;

    #[test]
    fn noop_client_charsets_carry_the_raw_bytes() {
        // Go scans COM_QUERY bytes raw for latin1, binary, utf8mb4, and any
        // name `FindEncoding` maps to binary: UTF-8 sent by a terminal under
        // a latin1 connection stays UTF-8 (no double encoding), and a byte
        // that is not UTF-8 survives as its escape for the parser.
        for charset in [
            "latin1",
            "binary",
            "utf8mb4",
            "utf8",
            "ascii",
            "not-a-charset",
        ] {
            assert_eq!(
                decode_client_sql("SELECT HEX('└─é')".as_bytes(), charset),
                "SELECT HEX('└─é')",
                "{charset}"
            );
            let sql = decode_client_sql(b"SELECT 'h\xe9llo'", charset);
            assert_eq!(
                tidb_hack::raw_bytes::str_to_bytes(&sql).as_ref(),
                b"SELECT 'h\xe9llo'",
                "{charset}"
            );
        }
    }

    #[test]
    fn multibyte_clients_carry_every_non_ascii_byte_as_an_escape() {
        // Go scans a gbk/gb18030 client's raw bytes too; the parser decodes
        // literals and identifiers (and refuses an invalid group with 1300).
        // 0xD6 0xD0 (GBK U+4E2D) happens to be valid UTF-8; it is escaped.
        for charset in ["gbk", "gb18030"] {
            let bytes = b"SELECT '\xd6\xd0\x81\x20'";
            let sql = decode_client_sql(bytes, charset);
            assert_eq!(sql.chars().count(), bytes.len(), "{charset}");
            assert_eq!(tidb_hack::raw_bytes::str_to_bytes(&sql).as_ref(), bytes);
        }
    }
}

#[cfg(test)]
mod unknown_command_tests {
    use super::unknown_command_error;

    #[test]
    fn unknown_command_keeps_go_generic_error_identity() {
        // Go's `default` dispatch (`pkg/server/conn.go:1592-1593`) formats
        // the raw command byte as decimal and uses `mysql.ErrUnknown`.
        let error = unknown_command_error(0xfa);
        assert_eq!(error.code, 1105);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(error.message, "command 250 not supported now");
    }
}

#[cfg(test)]
mod capability_tests {
    use super::{CLIENT_TRANSACTIONS, SERVER_CAPABILITIES};

    #[test]
    fn server_advertises_go_transaction_capability() {
        // Go's `defaultCapability` includes ClientTransactions, so a client
        // must be able to negotiate transaction-aware status and commands.
        assert_ne!(SERVER_CAPABILITIES & CLIENT_TRANSACTIONS, 0);
    }
}

#[cfg(test)]
mod dispatch_failure_log_tests {
    use super::*;
    use std::sync::Arc;

    struct NoSession;

    impl QuerySession for NoSession {
        fn parse_statement(&mut self, sql: &str) -> Result<tidb_ast::Stmt, SqlQueryError> {
            // A fixture session takes any command text; one that is not SQL
        // stands as a plain statement.
        Ok(tidb_parser::parse(sql)
            .unwrap_or_else(|_| tidb_parser::parse("SELECT 1").expect("a statement")))
        }

        fn execute_stmt<'a>(
            &'a mut self,
            _sql: &str,
            _stmt: &tidb_ast::Stmt,
        ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
            panic!("the log reads no statement")
        }
    }

    fn conn_info() -> ConnInfo {
        ConnInfo {
            addr: "127.0.0.1:50000".to_owned(),
            collation: 45,
            user: "root".to_owned(),
        }
    }

    /// Go `clientConn.Run` (`pkg/server/conn.go:1327`) warns `command
    /// dispatched failed` with `errStrForLog(err)`: the error's `Error()`
    /// text, class prefix included, and the statement `getLastStmtInConn`
    /// renders; NOWAIT's expected refusal is a debug line, not the warning.
    #[test]
    fn a_dispatch_failure_logs_the_error_text_go_prints() {
        let sink = Arc::new(tidb_log::MemorySink::default());
        let config = tidb_log::Config {
            level: "info".into(),
            disable_timestamp: true,
            ..tidb_log::Config::default()
        };
        let (logger, _) = tidb_log::init_test_logger(sink.clone(), &config).unwrap();
        let restore = tidb_log::replace_globals(logger);

        let duplicate =
            SqlQueryError::new(1062, *b"23000", "Duplicate entry '1' for key 't.PRIMARY'")
                .with_identity(Some(tidb_error::catalog::kv::ERR_KEY_EXISTS.identity()));
        let mut payload = vec![tidb_mysql::consts::ComQuery];
        payload.extend_from_slice(b"insert into t values (1)");
        let registry = PreparedStatementRegistry::default();
        log_dispatch_failure(
            &mut NoSession,
            7,
            &conn_info(),
            &payload,
            &registry,
            &duplicate,
        );
        let line = sink.last_line().expect("one warning");
        assert!(line.contains("command dispatched failed"), "{line}");
        assert!(
            line.contains("[kv:1062]Duplicate entry '1' for key 't.PRIMARY'"),
            "{line}"
        );
        assert!(line.contains("insert into t values (1)"), "{line}");
        assert!(line.contains("id:7, addr:127.0.0.1:50000"), "{line}");
        assert!(line.contains("user:root"), "{line}");

        let nowait = SqlQueryError::new(3572, *b"HY000", "NOWAIT").with_identity(Some(
            tidb_error::catalog::tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET.identity(),
        ));
        let before = sink.string().len();
        log_dispatch_failure(
            &mut NoSession,
            7,
            &conn_info(),
            &payload,
            &registry,
            &nowait,
        );
        assert_eq!(
            sink.string().len(),
            before,
            "a NOWAIT refusal is below info"
        );
        restore.restore();
    }
}

#[cfg(test)]
mod connection_alive_tests {
    use super::{ConnectionAliveProbe, Mutex};
    use std::net::{TcpListener, TcpStream};

    /// Go `BufferedReadConn.IsAlive`: a quiet client is alive, and only the
    /// client's end of stream makes the probe answer dead.
    #[test]
    fn the_probe_answers_dead_only_after_the_client_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let probe = ConnectionAliveProbe {
            socket: server.into(),
            checking: Mutex::new(()),
        };
        assert!(probe.is_alive(), "a quiet client is alive");
        drop(client);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while probe.is_alive() {
            assert!(std::time::Instant::now() < deadline, "EOF never observed");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // The probe restores blocking mode, so the command loop's next read
        // blocks again.
        let mut byte = [0_u8; 1];
        assert_eq!(std::io::Read::read(&mut &probe.socket, &mut byte).unwrap(), 0);
    }

    /// The same probe over a Unix-domain socket connection.
    #[cfg(unix)]
    #[test]
    fn the_probe_sees_a_unix_socket_client_close() {
        let (server, client) = std::os::unix::net::UnixStream::pair().unwrap();
        std::io::Write::write_all(&mut &client, b"x").unwrap();
        let probe = ConnectionAliveProbe {
            socket: server.into(),
            checking: Mutex::new(()),
        };
        assert!(probe.is_alive(), "a client byte is no end of stream");
        let mut byte = [0_u8; 1];
        assert_eq!(std::io::Read::read(&mut &probe.socket, &mut byte).unwrap(), 1);
        drop(client);
        assert!(!probe.is_alive(), "the closed peer is an end of stream");
    }

    /// The probe peeks rather than reads: a client byte stays for the
    /// command loop.
    #[test]
    fn the_probe_leaves_client_bytes_unread() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        std::io::Write::write_all(&mut client, b"x").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let probe = ConnectionAliveProbe {
            socket: server.into(),
            checking: Mutex::new(()),
        };
        assert!(probe.is_alive());
        let mut byte = [0_u8; 1];
        assert_eq!(std::io::Read::read(&mut &probe.socket, &mut byte).unwrap(), 1);
        assert_eq!(&byte, b"x");
    }
}

#[cfg(test)]
mod check_auth_plugin_tests {
    use tidb_session::domain::peer_address::PeerAddress;
    use tidb_session::privilege::{
        encode_password_for_plugin, PluginCredential, PrivilegeRegistry,
    };

    use super::{check_auth_plugin, AuthPluginDecision, AuthPluginRefusal};
    use crate::auth_plugin_registry::AuthPluginRegistry;
    use crate::configured_user_store::ConfiguredUserStore;

    fn tcp() -> PeerAddress {
        PeerAddress::Tcp("127.0.0.1:4000".parse().expect("address"))
    }

    fn store() -> ConfiguredUserStore {
        // The bootstrapped table holds the passwordless `root`@`%`.
        let accounts = PrivilegeRegistry::default();
        let sha2 = encode_password_for_plugin("caching_sha2_password", &PluginCredential::By("p"))
            .expect("hash");
        assert!(accounts.create_user_with_plugin("sha2", "%", &sha2, "caching_sha2_password"));
        assert!(accounts.create_user_with_plugin(
            "ldap",
            "%",
            "uid=ldap,dc=example,dc=com",
            "authentication_ldap_simple"
        ));
        assert!(accounts.create_user_with_plugin("tok", "%", "", "tidb_auth_token"));
        assert!(accounts.create_user_with_plugin("sock", "%", "", "auth_socket"));
        ConfiguredUserStore::from_accounts(accounts)
    }

    /// Runs Go's `checkAuthPlugin` and returns the decision with the
    /// response plugin it leaves behind.
    fn check(
        users: &ConfiguredUserStore,
        server_plugin: &str,
        client_plugin: &str,
        plugin_auth: bool,
        user: &str,
        peer: PeerAddress,
    ) -> (Result<AuthPluginDecision, AuthPluginRefusal>, String) {
        let mut client_plugin = client_plugin.to_owned();
        let decision = check_auth_plugin(
            server_plugin,
            &mut client_plugin,
            plugin_auth,
            users,
            user,
            &peer,
        );
        (decision, client_plugin)
    }

    /// Go `TestAuthPlugin2` (`server/conn_test.go:2040`): an account with no
    /// plugin to switch to (root, no password) is switched to native password
    /// when the client named none.
    #[test]
    fn a_passwordless_account_switches_to_native_password() {
        let users = store();
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "",
            true,
            "root",
            PeerAddress::Unix,
        );
        assert!(
            matches!(decision, Ok(AuthPluginDecision::Switch(p)) if p == "mysql_native_password")
        );
        assert_eq!(plugin, "mysql_native_password");
        // The client already speaks native password: nothing to switch.
        let (decision, _) = check(
            &users,
            "caching_sha2_password",
            "mysql_native_password",
            true,
            "root",
            tcp(),
        );
        assert!(matches!(decision, Ok(AuthPluginDecision::Keep)));
    }

    /// Go `TestLDAPAuthSwitch` (`server/conn_test.go:2501`): an LDAP simple
    /// account is switched to its plugin, which the client is asked for as
    /// `mysql_clear_password`.
    #[test]
    fn an_ldap_account_switches_to_cleartext() {
        let users = store();
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "",
            true,
            "ldap",
            PeerAddress::Unix,
        );
        assert!(
            matches!(&decision, Ok(AuthPluginDecision::Switch(p)) if p == "authentication_ldap_simple")
        );
        assert_eq!(plugin, "authentication_ldap_simple");
        let extensions = AuthPluginRegistry::default();
        assert_eq!(
            extensions.client_plugin_name(&plugin),
            "mysql_clear_password"
        );
        assert_eq!(
            extensions.client_plugin_name("authentication_ldap_sasl"),
            "authentication_ldap_sasl_client"
        );
    }

    #[test]
    fn the_account_plugin_selects_the_switch() {
        let users = store();
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "mysql_native_password",
            true,
            "sha2",
            tcp(),
        );
        assert!(
            matches!(decision, Ok(AuthPluginDecision::Switch(p)) if p == "caching_sha2_password")
        );
        assert_eq!(plugin, "caching_sha2_password");
        // Server, client and account agree.
        let (decision, _) = check(
            &users,
            "caching_sha2_password",
            "caching_sha2_password",
            true,
            "sha2",
            tcp(),
        );
        assert!(matches!(decision, Ok(AuthPluginDecision::Keep)));
        // `tidb_auth_token` is asked for in cleartext.
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "mysql_native_password",
            true,
            "tok",
            tcp(),
        );
        assert!(
            matches!(decision, Ok(AuthPluginDecision::Switch(p)) if p == "mysql_clear_password")
        );
        assert_eq!(plugin, "mysql_clear_password");
    }

    /// MySQL 5.1 and older clients cannot switch: native password still
    /// works, any other account plugin is `ErrNotSupportedAuthMode`.
    #[test]
    fn a_client_without_plugin_auth_cannot_switch() {
        let users = store();
        let (decision, _) = check(&users, "mysql_native_password", "", false, "root", tcp());
        assert!(matches!(decision, Ok(AuthPluginDecision::Keep)));
        let (decision, _) = check(&users, "mysql_native_password", "", false, "sha2", tcp());
        assert!(matches!(
            decision,
            Err(AuthPluginRefusal::NotSupportedAuthMode)
        ));
    }

    /// No account matches: access denied before any exchange; a session token
    /// is always permitted and verified later.
    #[test]
    fn an_unknown_user_is_refused_unless_it_presents_a_session_token() {
        let users = store();
        let (decision, _) = check(
            &users,
            "mysql_native_password",
            "mysql_native_password",
            true,
            "nobody",
            tcp(),
        );
        assert!(matches!(decision, Err(AuthPluginRefusal::AccessDenied)));
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "tidb_session_token",
            true,
            "nobody",
            tcp(),
        );
        assert!(matches!(decision, Ok(AuthPluginDecision::Keep)));
        assert_eq!(plugin, "tidb_session_token");
    }

    /// `auth_socket` answers with the peer's OS user, and only on a Unix
    /// socket.
    #[test]
    fn auth_socket_needs_a_unix_socket() {
        let users = store();
        let (decision, _) = check(
            &users,
            "mysql_native_password",
            "mysql_native_password",
            true,
            "sock",
            tcp(),
        );
        assert!(matches!(decision, Err(AuthPluginRefusal::AccessDenied)));
        let (decision, plugin) = check(
            &users,
            "mysql_native_password",
            "mysql_native_password",
            true,
            "sock",
            PeerAddress::Unix,
        );
        assert!(matches!(decision, Ok(AuthPluginDecision::SocketUser)));
        assert_eq!(plugin, "auth_socket");
    }
}
