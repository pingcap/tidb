// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::collections::VecDeque;
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{OomAction, StatementMemory};
use tidb_exec::real_tikv_dml::prepare_configured_write;
use tidb_exec::real_tikv_read::prepare_configured_point_read;
use tidb_planner::prepared_dml::{ConfiguredPreparedWrite, PreparedBindValue};
use tidb_planner::read_only_scan::{
    configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredTable,
};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, COM_INIT_DB, COM_PING, COM_QUERY, COM_QUIT,
    COM_STMT_CLOSE, COM_STMT_EXECUTE, COM_STMT_FETCH, COM_STMT_PREPARE, COM_STMT_RESET,
    COM_STMT_SEND_LONG_DATA, DEFAULT_MAX_ALLOWED_PACKET, TYPE_BLOB, TYPE_LONGLONG,
};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker, GeneralExecuteOutcome, PreparedGeneral, PreparedPointRead, PreparedWrite,
    PreparedStatement, QueryResult, QuerySession, QuerySessionFactory, ResultSetSource, SessionContext,
    SessionTransaction, SqlQueryError, WireStatus, WriteOutcome, SERVER_STATUS_IN_TRANS,
};
use tidb_session::ResultMaterializationAuthority;
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

#[derive(Default)]
struct Lifecycle {
    next_batches: usize,
    finished: usize,
    closed: usize,
    source_dropped: usize,
    snapshot_released: usize,
}

struct Rows {
    rows: VecDeque<Vec<Datum>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl ResultSetSource for Rows {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        Ok((0..max_rows).map_while(|_| self.rows.pop_front()).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![
            ColumnInfo {
                schema: "campaign20".to_owned(),
                table: "rows".to_owned(),
                org_table: "rows".to_owned(),
                name: "amount".to_owned(),
                org_name: "balance".to_owned(),
                column_length: 20,
                charset: 63,
                flag: 0x0001,
                decimal: 0,
                type_code: TYPE_LONGLONG,
                default_value: None,
            },
            ColumnInfo {
                schema: "campaign20".to_owned(),
                table: "rows".to_owned(),
                org_table: "rows".to_owned(),
                name: "id".to_owned(),
                org_name: "id".to_owned(),
                column_length: 20,
                charset: 63,
                flag: 0x0003,
                decimal: 0,
                type_code: TYPE_LONGLONG,
                default_value: None,
            },
        ])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().finished += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().closed += 1;
        Ok(())
    }
}

struct Session {
    queries: Arc<Mutex<Vec<String>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        self.queries.lock().unwrap().push(sql.to_owned());
        Ok(QueryResult::new(Box::new(Rows {
            rows: [
                vec![Datum::Int(-11), Datum::Int(7)],
                vec![Datum::Int(25), Datum::Int(8)],
            ]
            .into(),
            lifecycle: Arc::clone(&self.lifecycle),
        })))
    }
}

struct Factory {
    queries: Arc<Mutex<Vec<String>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySessionFactory for Factory {
    type Session = Session;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(Session {
            queries: Arc::clone(&self.queries),
            lifecycle: Arc::clone(&self.lifecycle),
        })
    }
}

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap()
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_salt(initial: &[u8]) -> [u8; 20] {
    assert_eq!(initial[0], 10);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let first = version_end + 1 + 4;
    let second = first + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    let mut salt = [0; 20];
    salt[..8].copy_from_slice(&initial[first..first + 8]);
    salt[8..].copy_from_slice(&initial[second..second + 12]);
    salt
}

fn native_response(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut challenge = Sha1::new();
    challenge.update(salt);
    challenge.update(stage_two);
    let challenge = challenge.finalize();
    let mut response = [0; 20];
    for index in 0..response.len() {
        response[index] = stage_one[index] ^ challenge[index];
    }
    response
}

fn authenticate(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &str,
    password: &[u8],
) {
    authenticate_with_eof_mode(client, reader, user, password, true);
}

fn authenticate_with_eof_mode(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &str,
    password: &[u8],
    deprecate_eof: bool,
) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    assert_eq!(initial[version_end + 16], 46);

    let mut capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS;
    if deprecate_eof {
        capabilities |= CLIENT_DEPRECATE_EOF;
    }
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    let auth = native_response(password, &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0); // zero connection attributes
    write_packet(client, 1, &response);
}

fn read_length_encoded_string<'a>(packet: &mut &'a [u8]) -> &'a [u8] {
    let length = usize::from(packet[0]);
    assert!(length < 0xfb, "test metadata uses one-byte lengths");
    *packet = &packet[1..];
    let (value, remaining) = packet.split_at(length);
    *packet = remaining;
    value
}

fn assert_column_packet(packet: &[u8], name: &[u8], org_name: &[u8], flags: u16) {
    let mut remaining = packet;
    assert_eq!(read_length_encoded_string(&mut remaining), b"def");
    assert_eq!(read_length_encoded_string(&mut remaining), b"campaign20");
    assert_eq!(read_length_encoded_string(&mut remaining), b"rows");
    assert_eq!(read_length_encoded_string(&mut remaining), b"rows");
    assert_eq!(read_length_encoded_string(&mut remaining), name);
    assert_eq!(read_length_encoded_string(&mut remaining), org_name);
    assert_eq!(remaining[0], 0x0c);
    assert_eq!(u16::from_le_bytes([remaining[1], remaining[2]]), 63);
    assert_eq!(remaining[7], TYPE_LONGLONG);
    assert_eq!(u16::from_le_bytes([remaining[8], remaining[9]]), flags);
}

fn assert_mysql_error(packet: &[u8], code: u16, state: &[u8; 5]) {
    assert_eq!(packet.first(), Some(&0xff), "ERR packet: {packet:?}");
    assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), code);
    assert_eq!(&packet[3..4], b"#");
    assert_eq!(&packet[4..9], state);
}

fn prepared_catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([ConfiguredTable::new(
        "campaign27",
        "rows",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_not_null("balance", 2),
        ],
    )])
    .unwrap()
}

fn prepared_balance_column() -> ColumnInfo {
    ColumnInfo {
        schema: "campaign27".to_owned(),
        table: "rows".to_owned(),
        org_table: "rows".to_owned(),
        name: "balance".to_owned(),
        org_name: "balance".to_owned(),
        column_length: 20,
        charset: 63,
        flag: 0x0001,
        decimal: 0,
        type_code: TYPE_LONGLONG,
        default_value: None,
    }
}

#[test]
fn prepared_point_range_keeps_its_two_marker_contract() {
    let template = prepare_configured_point_read(
        "SELECT balance FROM campaign27.rows WHERE id >= ? AND id <= ?",
        &prepared_catalog(),
    )
    .unwrap();
    let read = PreparedPointRead::new(
        template,
        vec![prepared_balance_column()],
        vec![FieldType::new(FieldTypeCode::LongLong)],
    )
    .unwrap();
    assert_eq!(read.parameter_count(), 2);
    assert_eq!(read.result_field_types()[0].code(), FieldTypeCode::LongLong);
    assert_eq!(PreparedStatement::PointRead(read).parameter_count(), 2);
}

struct SnapshotLease {
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        self.lifecycle.lock().unwrap().snapshot_released += 1;
    }
}

struct PreparedRows {
    value: Option<i64>,
    lifecycle: Arc<Mutex<Lifecycle>>,
    _snapshot: SnapshotLease,
}

impl ResultSetSource for PreparedRows {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        self.lifecycle.lock().unwrap().next_batches += 1;
        Ok(self
            .value
            .take()
            .map(|value| vec![vec![Datum::Int(value)]])
            .unwrap_or_default())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![prepared_balance_column()])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().finished += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().closed += 1;
        Ok(())
    }
}

impl Drop for PreparedRows {
    fn drop(&mut self) {
        self.lifecycle.lock().unwrap().source_dropped += 1;
    }
}

struct PreparedSession {
    executed_parameters: Arc<Mutex<Vec<i64>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySession for PreparedSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    fn prepare_point_read(&mut self, sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        let template = prepare_configured_point_read(sql, &prepared_catalog())
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        PreparedPointRead::new(
            template,
            vec![prepared_balance_column()],
            vec![FieldType::new(FieldTypeCode::LongLong)],
        )
    }

    fn execute_prepared_point_read<'a>(
        &'a mut self,
        statement: &PreparedPointRead,
        parameters: &[i64],
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let [value] = parameters else {
            return Err(SqlQueryError::unknown("expected one prepared value"));
        };
        self.executed_parameters.lock().unwrap().push(*value);
        let authority = ResultMaterializationAuthority::new(
            StatementMemory::new(-1, OomAction::Cancel, 1).with_tmp_storage_on_oom(false),
            8,
            8,
        );
        Ok(QueryResult::new(Box::new(PreparedRows {
            value: Some(*value + 100),
            lifecycle: Arc::clone(&self.lifecycle),
            _snapshot: SnapshotLease {
                lifecycle: Arc::clone(&self.lifecycle),
            },
        }))
        .with_statement_status(0, self.wire_status())
        .with_cursor_materialization(
            statement.result_field_types().to_vec(),
            authority,
        ))
    }

    fn wire_status(&self) -> WireStatus {
        WireStatus::AUTOCOMMIT.with(SERVER_STATUS_IN_TRANS)
    }

    fn prepare_write(&mut self, sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        let template = prepare_configured_write(sql, &prepared_catalog())
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(PreparedWrite::new(template))
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Binding is the real planner path; only publication is stubbed, so
        // this test owns the wire contract rather than storage behavior.
        let bound = statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        for parameter in parameters {
            if let PreparedBindValue::Int(value) = parameter {
                self.executed_parameters.lock().unwrap().push(*value);
            }
        }
        let affected_rows = match bound {
            ConfiguredPreparedWrite::InsertRows { rows, .. }
            | ConfiguredPreparedWrite::ReplaceRows { rows, .. } => rows.len() as u64,
            ConfiguredPreparedWrite::UpdatePoint { .. }
            | ConfiguredPreparedWrite::DeletePoint { .. } => 1,
        };
        Ok(WriteOutcome {
            affected_rows,
            last_insert_id: 0,
        })
    }
}

struct PreparedFactory {
    executed_parameters: Arc<Mutex<Vec<i64>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySessionFactory for PreparedFactory {
    type Session = PreparedSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(PreparedSession {
            executed_parameters: Arc::clone(&self.executed_parameters),
            lifecycle: Arc::clone(&self.lifecycle),
        })
    }
}

struct CursorEncodingRows {
    emitted: bool,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl ResultSetSource for CursorEncodingRows {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        if std::mem::replace(&mut self.emitted, true) {
            return Ok(Vec::new());
        }
        Ok(vec![vec![Datum::Bytes(b"not-an-integer".to_vec())]])
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        // Deliberately inconsistent with the exact Varchar FieldType below:
        // materialization remains valid, while binary FETCH must reject this
        // datum/metadata pair before advancing the cursor.
        Ok(vec![prepared_balance_column()])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().finished += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().closed += 1;
        Ok(())
    }
}

struct CursorEncodingSession {
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySession for CursorEncodingSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        Ok(PreparedGeneral::new(
            sql.to_owned(),
            0,
            vec![prepared_balance_column()],
        ))
    }

    fn execute_general<'a>(
        &'a mut self,
        _statement: &PreparedGeneral,
        _values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        let field = FieldType::new(FieldTypeCode::Varchar);
        let authority = ResultMaterializationAuthority::new(
            StatementMemory::new(-1, OomAction::Cancel, 1).with_tmp_storage_on_oom(false),
            8,
            8,
        );
        Ok(GeneralExecuteOutcome::Rows(
            QueryResult::new(Box::new(CursorEncodingRows {
                emitted: false,
                lifecycle: Arc::clone(&self.lifecycle),
            }))
            .with_cursor_materialization(vec![field], authority),
        ))
    }
}

struct CursorEncodingFactory {
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySessionFactory for CursorEncodingFactory {
    type Session = CursorEncodingSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(CursorEncodingSession {
            lifecycle: Arc::clone(&self.lifecycle),
        })
    }
}

#[test]
fn cursor_fetch_encoding_error_resets_cursor_atomically() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let worker_lifecycle = Arc::clone(&lifecycle);
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &CursorEncodingFactory {
                lifecycle: worker_lifecycle,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let mut prepare = vec![COM_STMT_PREPARE];
    prepare.extend_from_slice(b"SELECT broken_cursor_value");
    write_packet(&mut client, 0, &prepare);
    reader.set_sequence(1);
    let prepared = reader.read_packet().unwrap();
    assert_eq!(prepared[0], 0);
    assert_eq!(u16::from_le_bytes([prepared[5], prepared[6]]), 1);
    assert_eq!(u16::from_le_bytes([prepared[7], prepared[8]]), 0);
    let statement_id = u32::from_le_bytes(prepared[1..5].try_into().unwrap());
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);

    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(1); // CURSOR_TYPE_READ_ONLY
    execute.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [1]);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe);

    let mut fetch = vec![COM_STMT_FETCH];
    fetch.extend_from_slice(&statement_id.to_le_bytes());
    fetch.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1105, b"HY000");

    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1326, b"24000");
    assert_eq!(lifecycle.lock().unwrap().finished, 1);
    assert_eq!(lifecycle.lock().unwrap().closed, 1);

    write_packet(&mut client, 0, &[COM_QUIT]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

struct CursorReaderFailureRows {
    rows: VecDeque<Vec<Datum>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
    spill_path: PathBuf,
}

impl ResultSetSource for CursorReaderFailureRows {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        Ok((0..max_rows.max(1))
            .map_while(|_| self.rows.pop_front())
            .collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![ColumnInfo {
            schema: "test".to_owned(),
            table: "reader_failure".to_owned(),
            org_table: "reader_failure".to_owned(),
            name: "v".to_owned(),
            org_name: "v".to_owned(),
            column_length: 4096,
            charset: 63,
            flag: 0,
            decimal: 0,
            type_code: TYPE_BLOB,
            default_value: None,
        }])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().finished += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().closed += 1;
        let data_path = fs::read_dir(&self.spill_path)
            .map_err(|error| error.to_string())?
            .filter_map(Result::ok)
            .find_map(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                (name.contains("chunk.DataInDiskByRows") && !name.contains("Offset"))
                    .then(|| entry.path())
            })
            .ok_or_else(|| "cursor did not create a row spill file".to_owned())?;
        fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .map_err(|error| format!("open {}: {error}", data_path.display()))?
            .set_len(1024)
            .map_err(|error| format!("truncate {}: {error}", data_path.display()))
    }
}

struct CursorReaderFailureSession {
    lifecycle: Arc<Mutex<Lifecycle>>,
    storage: Arc<SpillStorage>,
    spill_path: PathBuf,
}

impl QuerySession for CursorReaderFailureSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        Ok(PreparedGeneral::new(
            sql.to_owned(),
            0,
            vec![ColumnInfo {
                schema: "test".to_owned(),
                table: "reader_failure".to_owned(),
                org_table: "reader_failure".to_owned(),
                name: "v".to_owned(),
                org_name: "v".to_owned(),
                column_length: 4096,
                charset: 63,
                flag: 0,
                decimal: 0,
                type_code: TYPE_BLOB,
                default_value: None,
            }],
        ))
    }

    fn execute_general<'a>(
        &'a mut self,
        _statement: &PreparedGeneral,
        _values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        let field = FieldType::new(FieldTypeCode::Varchar).with_flen(4096);
        let memory = StatementMemory::new(1, OomAction::Cancel, 9)
            .with_spill_storage(Arc::clone(&self.storage))
            .with_tmp_storage_on_oom(true);
        let authority = ResultMaterializationAuthority::new(memory, 2, 8);
        let rows = (0..32)
            .map(|value| vec![Datum::Bytes(vec![value; 2048])])
            .collect();
        Ok(GeneralExecuteOutcome::Rows(
            QueryResult::new(Box::new(CursorReaderFailureRows {
                rows,
                lifecycle: Arc::clone(&self.lifecycle),
                spill_path: self.spill_path.clone(),
            }))
            .with_cursor_materialization(vec![field], authority),
        ))
    }
}

struct CursorReaderFailureFactory {
    lifecycle: Arc<Mutex<Lifecycle>>,
    storage: Arc<SpillStorage>,
    spill_path: PathBuf,
}

impl QuerySessionFactory for CursorReaderFailureFactory {
    type Session = CursorReaderFailureSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(CursorReaderFailureSession {
            lifecycle: Arc::clone(&self.lifecycle),
            storage: Arc::clone(&self.storage),
            spill_path: self.spill_path.clone(),
        })
    }
}

#[test]
fn cursor_reader_error_is_reported_by_fetch_and_closes_cursor() {
    let spill_path =
        std::env::temp_dir().join(format!("tidb_cursor_reader_failure_{}", std::process::id()));
    let _ = fs::remove_dir_all(&spill_path);
    let storage = Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path: spill_path.clone(),
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .expect("isolated cursor spill authority"),
    );
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_storage = Arc::clone(&storage);
    let worker_spill_path = spill_path.clone();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &CursorReaderFailureFactory {
                lifecycle: worker_lifecycle,
                storage: worker_storage,
                spill_path: worker_spill_path,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let mut prepare = vec![COM_STMT_PREPARE];
    prepare.extend_from_slice(b"SELECT broken_cursor_reader");
    write_packet(&mut client, 0, &prepare);
    reader.set_sequence(1);
    let prepared = reader.read_packet().unwrap();
    let statement_id = u32::from_le_bytes(prepared[1..5].try_into().unwrap());
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);

    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(1); // CURSOR_TYPE_READ_ONLY
    execute.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [1]);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe);
    assert!(storage.global_tracker().bytes_consumed() > 0);

    let mut fetch = vec![COM_STMT_FETCH];
    fetch.extend_from_slice(&statement_id.to_le_bytes());
    fetch.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1105, b"HY000");

    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1326, b"24000");
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);
    assert_eq!(
        fs::read_dir(&spill_path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "_dir.lock")
            .count(),
        0
    );
    assert_eq!(lifecycle.lock().unwrap().finished, 1);
    assert_eq!(lifecycle.lock().unwrap().closed, 1);

    write_packet(&mut client, 0, &[COM_QUIT]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
    drop(storage);
    fs::remove_dir_all(spill_path).unwrap();
}

fn prepare_statement(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) -> u32 {
    prepare_statement_with_eof_mode(client, reader, true)
}

fn prepare_statement_with_eof_mode(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    deprecate_eof: bool,
) -> u32 {
    let mut command = vec![COM_STMT_PREPARE];
    command.extend_from_slice(b"SELECT balance FROM campaign27.rows WHERE id = ?");
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let prepare_ok = reader.read_packet().unwrap();
    assert_eq!(prepare_ok[0], 0);
    assert_eq!(u16::from_le_bytes([prepare_ok[5], prepare_ok[6]]), 1);
    assert_eq!(u16::from_le_bytes([prepare_ok[7], prepare_ok[8]]), 1);
    let metadata_packets = if deprecate_eof { 2 } else { 4 };
    for _ in 0..metadata_packets {
        assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    }
    u32::from_le_bytes(prepare_ok[1..5].try_into().unwrap())
}

fn prepared_execute_command(statement_id: u32, new_types: bool, value: i64) -> Vec<u8> {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0);
    command.extend_from_slice(&1_u32.to_le_bytes());
    command.push(0);
    command.push(u8::from(new_types));
    if new_types {
        command.extend_from_slice(&[TYPE_LONGLONG, 0]);
    }
    command.extend_from_slice(&value.to_le_bytes());
    command
}

fn open_prepared_point_cursor(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    new_types: bool,
    value: i64,
) -> u16 {
    let mut execute = prepared_execute_command(statement_id, new_types, value);
    execute[5] = 1; // CURSOR_TYPE_READ_ONLY
    write_packet(client, 0, &execute);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [1]);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    let execute_end = reader.read_packet().unwrap();
    assert_eq!(
        execute_end[0], 0xfe,
        "cursor execute sends metadata only, not a row: {execute_end:?}"
    );
    assert_eq!(u16::from_le_bytes([execute_end[1], execute_end[2]]), 0);
    u16::from_le_bytes([execute_end[3], execute_end[4]])
}

fn assert_prepared_binary_result(reader: &mut PacketReader<TcpStream>, expected_value: i64) {
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [1]);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    let row = reader.read_packet().unwrap();
    assert_eq!(&row[..2], [0, 0]);
    assert_eq!(
        i64::from_le_bytes(row[2..10].try_into().unwrap()),
        expected_value
    );
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe);
}

#[test]
fn a_prepared_write_answers_with_an_ok_packet_carrying_affected_rows() {
    // pkg/executor/test/seqtest/prepared_test.go:328 TestPreparedInsert
    // A write returns one OK packet and never a result set: the prepare
    // response must advertise one parameter per marker and zero result
    // columns, and the execute response must be OK with exact affected rows.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let executed = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_executed = Arc::clone(&executed);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let factory = PreparedFactory {
            executed_parameters: worker_executed,
            lifecycle: worker_lifecycle,
        };
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &factory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let mut command = vec![COM_STMT_PREPARE];
    command.extend_from_slice(b"INSERT INTO campaign27.rows (id, balance) VALUES (?, ?)");
    write_packet(&mut client, 0, &command);
    reader.set_sequence(1);
    let prepare_ok = reader.read_packet().unwrap();
    assert_eq!(prepare_ok[0], 0);
    let statement_id = u32::from_le_bytes(prepare_ok[1..5].try_into().unwrap());
    assert_eq!(
        u16::from_le_bytes([prepare_ok[5], prepare_ok[6]]),
        0,
        "a write advertises no result columns"
    );
    assert_eq!(
        u16::from_le_bytes([prepare_ok[7], prepare_ok[8]]),
        2,
        "a write advertises one parameter per marker"
    );
    // Two parameter-definition packets and no result-column definitions at
    // all: a write has no result set to describe.
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);

    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(0);
    execute.extend_from_slice(&1_u32.to_le_bytes());
    execute.push(0);
    execute.push(1);
    execute.extend_from_slice(&[TYPE_LONGLONG, 0, TYPE_LONGLONG, 0]);
    execute.extend_from_slice(&10_i64.to_le_bytes());
    execute.extend_from_slice(&100_i64.to_le_bytes());
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    let ok = reader.read_packet().unwrap();
    assert_eq!(
        ok[0],
        0,
        "a prepared write answers with OK, not a result set; server said: {}",
        String::from_utf8_lossy(&ok)
    );
    assert_eq!(ok[1], 1, "exactly one row was affected");
    assert_eq!(*executed.lock().unwrap(), vec![10, 100]);

    write_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(
        report.commands.stmt_execute_successes, 1,
        "the write counts as exactly one successful execute"
    );
}

#[test]
fn real_tcp_connection_runs_handshake_query_ping_quit_and_exact_cleanup() {
    // pkg/server/conn_test.go:789 TestDispatchClientProtocol41
    // pkg/server/conn_test.go:909 TestQueryEndWithZero
    // pkg/server/conn_test.go:2479 TestCloseConn
    // pkg/server/conn_test.go:2518 TestConnAddMetrics
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_queries = Arc::clone(&queries);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let factory = Factory {
            queries: worker_queries,
            lifecycle: worker_lifecycle,
        };
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &factory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    // pkg/server/internal/parse.StmtFetchCmd returns the plain
    // mysql.ErrMalformPacket for every non-eight-byte payload. Go's writeError
    // therefore reports the generic 1105 boundary, not ErrWrongArguments.
    write_packet(&mut client, 0, &[COM_STMT_FETCH]);
    reader.set_sequence(1);
    let malformed_fetch = reader.read_packet().unwrap();
    assert_mysql_error(&malformed_fetch, 1105, b"HY000");
    assert!(malformed_fetch.ends_with(b"malform packet error"));

    write_packet(&mut client, 0, &[COM_INIT_DB, b'x']);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"select balance as amount, id from campaign20.rows\0\0");
    write_packet(&mut client, 0, &query);
    reader.set_sequence(1);
    let payloads = (0..6)
        .map(|_| reader.read_packet().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(payloads[0].as_slice(), [2]);
    assert_column_packet(&payloads[1], b"amount", b"balance", 0x0001);
    assert_column_packet(&payloads[2], b"id", b"id", 0x0003);
    assert_eq!(payloads[3].as_slice(), b"\x03-11\x017");
    assert_eq!(payloads[4].as_slice(), b"\x0225\x018");
    assert_eq!(payloads[5][0], 0xfe);

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 1);
    assert_eq!(report.commands.text_query_commands, 1);
    assert_eq!(report.commands.stmt_prepare_commands, 0);
    assert_eq!(report.commands.stmt_prepare_successes, 0);
    assert_eq!(report.commands.stmt_execute_commands, 0);
    assert_eq!(report.commands.stmt_execute_successes, 0);
    assert_eq!(report.commands.stmt_close_commands, 0);
    assert_eq!(
        queries.lock().unwrap().as_slice(),
        ["select balance as amount, id from campaign20.rows\0"]
    );
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(lifecycle.finished, 1);
    assert_eq!(lifecycle.closed, 1);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn malformed_handshake_response_uses_the_generic_protocol_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &Factory { queries, lifecycle },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    reader.set_sequence(0);
    let _initial = reader.read_packet().unwrap();

    let mut malformed = CLIENT_PROTOCOL_41.to_le_bytes().to_vec();
    malformed.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    malformed.push(46);
    malformed.extend_from_slice(&[0; 23]);
    malformed.extend_from_slice(b"unterminated-user");
    write_packet(&mut client, 1, &malformed);
    reader.set_sequence(2);
    let error = reader.read_packet().unwrap();
    assert_mysql_error(&error, 1105, b"HY000");
    assert!(error.ends_with(b"malform packet error"), "{error:?}");

    drop(client);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::AuthenticationRejected);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
}

#[test]
fn handshake_without_protocol_41_uses_the_source_auth_mode_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &Factory { queries, lifecycle },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    reader.set_sequence(0);
    let _initial = reader.read_packet().unwrap();

    // Go rejects this from the two-byte capability prefix before parsing the
    // Response41 body. Since the client did not negotiate protocol 4.1, the
    // error packet uses the legacy shape without a SQLSTATE marker.
    let mut unsupported = 0_u32.to_le_bytes().to_vec();
    unsupported.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    unsupported.push(46);
    unsupported.extend_from_slice(&[0; 23]);
    unsupported.push(0);
    write_packet(&mut client, 1, &unsupported);
    reader.set_sequence(2);
    let error = reader.read_packet().unwrap();
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes(error[1..3].try_into().unwrap()), 1251);
    assert_eq!(
        &error[3..],
        b"Client does not support authentication protocol requested by server; consider upgrading MySQL client"
    );

    drop(client);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::AuthenticationRejected);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
}

#[test]
fn real_tcp_prepared_lifecycle_reports_exact_eight_binary_executes_and_type_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let executed_parameters = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_parameters = Arc::clone(&executed_parameters);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PreparedFactory {
                executed_parameters: worker_parameters,
                lifecycle: worker_lifecycle,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_statement(&mut client, &mut reader);
    for value in 1_i64..=8 {
        write_packet(
            &mut client,
            0,
            &prepared_execute_command(statement_id, value == 1, value),
        );
        assert_prepared_binary_result(&mut reader, value + 100);
        if value == 4 {
            // TiDBStatement.Reset clears the cursor and long-data buffers but
            // deliberately retains paramsType. The fifth execute keeps its
            // new-parameter-bound flag clear and must still decode as BIGINT.
            let mut reset = vec![COM_STMT_RESET];
            reset.extend_from_slice(&statement_id.to_le_bytes());
            write_packet(&mut client, 0, &reset);
            reader.set_sequence(1);
            assert_eq!(reader.read_packet().unwrap()[0], 0);
        }
    }
    let mut close = vec![COM_STMT_CLOSE];
    close.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &close);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 8);
    assert_eq!(report.commands.text_query_commands, 0);
    assert_eq!(report.commands.stmt_prepare_commands, 1);
    assert_eq!(report.commands.stmt_prepare_successes, 1);
    assert_eq!(report.commands.stmt_execute_commands, 8);
    assert_eq!(report.commands.stmt_execute_successes, 8);
    assert_eq!(report.commands.stmt_close_commands, 1);
    assert_eq!(
        executed_parameters.lock().unwrap().as_slice(),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(lifecycle.finished, 8);
    assert_eq!(lifecycle.closed, 8);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

fn assert_prepared_point_read_cursor_protocol(deprecate_eof: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let executed_parameters = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_parameters = Arc::clone(&executed_parameters);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PreparedFactory {
                executed_parameters: worker_parameters,
                lifecycle: worker_lifecycle,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate_with_eof_mode(
        &mut client,
        &mut reader,
        "alice",
        b"secret",
        deprecate_eof,
    );
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_statement_with_eof_mode(&mut client, &mut reader, deprecate_eof);
    assert_eq!(
        open_prepared_point_cursor(&mut client, &mut reader, statement_id, true, 7),
        0x0043,
        "execute advertises exactly IN_TRANS | AUTOCOMMIT | CURSOR_EXISTS"
    );
    assert_eq!(
        open_prepared_point_cursor(&mut client, &mut reader, statement_id, false, 8),
        0x0043,
        "replacement execute owns a fresh cursor"
    );

    let mut fetch = vec![COM_STMT_FETCH];
    fetch.extend_from_slice(&statement_id.to_le_bytes());
    fetch.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    let row = reader.read_packet().unwrap();
    assert_eq!(&row[..2], [0, 0]);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 108);
    let fetch_end = reader.read_packet().unwrap();
    assert_eq!(fetch_end[0], 0xfe);
    assert_eq!(u16::from_le_bytes([fetch_end[1], fetch_end[2]]), 0);
    let fetch_status = u16::from_le_bytes([fetch_end[3], fetch_end[4]]);
    assert_eq!(
        fetch_status, 0x0083,
        "final fetch reports exactly IN_TRANS | AUTOCOMMIT | LAST_ROW_SENT"
    );

    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1326, b"24000");

    assert_eq!(
        open_prepared_point_cursor(&mut client, &mut reader, statement_id, false, 9),
        0x0043,
    );
    let mut malformed = vec![COM_STMT_EXECUTE];
    malformed.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &malformed);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1210, b"HY000");
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    assert_mysql_error(&reader.read_packet().unwrap(), 1326, b"24000");

    assert_eq!(executed_parameters.lock().unwrap().as_slice(), [7, 8, 9]);
    assert_eq!(lifecycle.lock().unwrap().finished, 3);
    assert_eq!(lifecycle.lock().unwrap().closed, 3);

    write_packet(&mut client, 0, &[COM_QUIT]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

#[test]
fn prepared_point_read_honors_read_only_cursor_protocol() {
    assert_prepared_point_read_cursor_protocol(true);
    assert_prepared_point_read_cursor_protocol(false);
}

#[test]
fn cursor_execute_releases_snapshot_before_fetch_and_fetch_uses_only_materialized_rows() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let executed_parameters = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_parameters = Arc::clone(&executed_parameters);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PreparedFactory {
                executed_parameters: worker_parameters,
                lifecycle: worker_lifecycle,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_statement(&mut client, &mut reader);
    assert_eq!(
        open_prepared_point_cursor(&mut client, &mut reader, statement_id, true, 7),
        0x0043,
    );

    // A PING roundtrip is the event-loop barrier after COM_STMT_EXECUTE: the
    // worker has returned past `drop(result)` and installed the cursor, while
    // no FETCH has run. The storage source and snapshot lease must be gone.
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    let before_fetch = {
        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.next_batches, 2, "the source was drained to EOF");
        assert_eq!(lifecycle.finished, 1);
        assert_eq!(lifecycle.closed, 1);
        assert_eq!(lifecycle.source_dropped, 1);
        assert_eq!(lifecycle.snapshot_released, 1);
        (
            lifecycle.next_batches,
            lifecycle.finished,
            lifecycle.closed,
            lifecycle.source_dropped,
            lifecycle.snapshot_released,
        )
    };

    let mut fetch = vec![COM_STMT_FETCH];
    fetch.extend_from_slice(&statement_id.to_le_bytes());
    fetch.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    let row = reader.read_packet().unwrap();
    assert_eq!(&row[..2], [0, 0]);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 107);
    let fetch_end = reader.read_packet().unwrap();
    assert_eq!(fetch_end[0], 0xfe);
    assert_eq!(
        u16::from_le_bytes([fetch_end[3], fetch_end[4]]),
        0x0083,
        "the materialized row exhausted the cursor",
    );

    let after_fetch = {
        let lifecycle = lifecycle.lock().unwrap();
        (
            lifecycle.next_batches,
            lifecycle.finished,
            lifecycle.closed,
            lifecycle.source_dropped,
            lifecycle.snapshot_released,
        )
    };
    assert_eq!(after_fetch, before_fetch, "FETCH must not revisit storage");

    let mut close = vec![COM_STMT_CLOSE];
    close.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &close);
    write_packet(&mut client, 0, &[COM_QUIT]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
    assert_eq!(executed_parameters.lock().unwrap().as_slice(), [7]);
}

#[test]
fn real_tcp_malformed_prepared_execute_counts_command_without_success() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let executed_parameters = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_parameters = Arc::clone(&executed_parameters);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PreparedFactory {
                executed_parameters: worker_parameters,
                lifecycle: worker_lifecycle,
            },
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_statement(&mut client, &mut reader);
    let mut malformed = vec![COM_STMT_EXECUTE];
    malformed.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &malformed);
    reader.set_sequence(1);
    let error = reader.read_packet().unwrap();
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1210);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 0);
    assert_eq!(report.commands.text_query_commands, 0);
    assert_eq!(report.commands.stmt_prepare_commands, 1);
    assert_eq!(report.commands.stmt_prepare_successes, 1);
    assert_eq!(report.commands.stmt_execute_commands, 1);
    assert_eq!(report.commands.stmt_execute_successes, 0);
    assert_eq!(report.commands.stmt_close_commands, 0);
    assert!(executed_parameters.lock().unwrap().is_empty());
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(lifecycle.finished, 0);
    assert_eq!(lifecycle.closed, 0);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

struct RejectingSession;

impl QuerySession for RejectingSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::new(1142, *b"42000", "read denied"))
    }
}

struct RejectingFactory;

impl QuerySessionFactory for RejectingFactory {
    type Session = RejectingSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(RejectingSession)
    }
}

#[test]
fn query_error_is_written_as_err_and_connection_remains_command_aligned() {
    // pkg/server/conn_test.go:789 TestDispatchClientProtocol41
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &RejectingFactory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    write_packet(&mut client, 0, &[COM_QUERY, b'x']);
    reader.set_sequence(1);
    let error = reader.read_packet().unwrap();
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1142);
    assert_eq!(&error[4..9], b"42000");
    assert!(error.ends_with(b"read denied"));

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 0);
    assert_eq!(report.commands.text_query_commands, 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.failed(), 0);
}

/// A session that owns only the transaction state machine, faithfully mirroring
/// `RealTiKvServerSession`: `control_transaction` delegates to the same
/// classifier plus [`SessionTransaction`], and a text `execute` reports whether
/// a transaction is open — proving BEGIN/COMMIT change real session state rather
/// than merely painting an OK packet.
#[derive(Default)]
struct TransactionSession {
    transaction: SessionTransaction,
}

impl QuerySession for TransactionSession {
    /// The status word every OK packet of this session carries, read live off
    /// the transaction the session actually owns -- Go `cc.ctx.Status()`.
    fn wire_status(&self) -> WireStatus {
        WireStatus::autocommit_session(self.transaction.is_active())
    }

    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        if self.transaction.is_active() {
            return Err(SqlQueryError::unknown(
                "this test session runs no statement inside a transaction",
            ));
        }
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    /// One DML, answered with an OK packet the way MySQL answers a text-protocol
    /// write. It exists so the status word that OK packet carries can be pinned
    /// while a transaction this session really owns is open.
    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        if !sql.to_ascii_uppercase().starts_with("INSERT") {
            return Ok(None);
        }
        Ok(Some(WriteOutcome {
            affected_rows: 1,
            last_insert_id: 0,
        }))
    }

    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        match classify_transaction_control(sql) {
            None => Ok(None),
            Some(TransactionControl::Begin { mode }) => {
                self.transaction
                    .begin(mode)
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                Ok(Some(true))
            }
            Some(control @ (TransactionControl::Commit | TransactionControl::Rollback)) => {
                self.transaction
                    .end(control == TransactionControl::Commit)
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                Ok(Some(false))
            }
            // This test session drives only BEGIN/COMMIT/ROLLBACK.
            Some(
                TransactionControl::Savepoint(_)
                | TransactionControl::RollbackToSavepoint(_)
                | TransactionControl::ReleaseSavepoint(_),
            ) => Err(SqlQueryError::unknown(
                "savepoints are not part of this test",
            )),
            Some(TransactionControl::Unsupported(feature)) => Err(SqlQueryError::unknown(format!(
                "{feature} is not supported by the read-only Rust SQL node"
            ))),
        }
    }
}

struct TransactionFactory;

impl QuerySessionFactory for TransactionFactory {
    type Session = TransactionSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(TransactionSession::default())
    }
}

/// Reads one OK packet and returns its `status_flags`. The OK payload is
/// `header(1) | affected_rows(lenenc) | last_insert_id(lenenc) | status_flags(2)
/// | warnings(2)`; both length-encoded ints are zero here, so the flags sit at
/// bytes 3..5. See `encode_ok_like_packet` in tidb-protocol.
fn read_ok_status_flags(reader: &mut PacketReader<TcpStream>) -> u16 {
    let ok = reader.read_packet().unwrap();
    assert_eq!(ok[0], 0, "expected an OK packet, got {ok:02x?}");
    assert_eq!(&ok[1..3], &[0, 0], "affected_rows and last_insert_id are 0");
    u16::from_le_bytes([ok[3], ok[4]])
}

/// Reads one OK packet answering a write and returns its `status_flags`. The
/// payload is `header(1) | affected_rows(lenenc) | last_insert_id(lenenc) |
/// status_flags(2) | warnings(2)`; both counts below 251 encode as one byte.
fn read_write_ok_status_flags(reader: &mut PacketReader<TcpStream>, affected_rows: u8) -> u16 {
    let ok = reader.read_packet().unwrap();
    assert_eq!(ok[0], 0, "expected an OK packet, got {ok:02x?}");
    assert_eq!(ok[1], affected_rows, "affected_rows");
    assert_eq!(ok[2], 0, "last_insert_id");
    u16::from_le_bytes([ok[3], ok[4]])
}

/// The regression for the data loss: a DML inside `BEGIN` must report the OPEN
/// transaction on its own OK packet.
///
/// Go reads `cc.ctx.Status()` per statement (`pkg/server/conn.go`) and passes it
/// to `writeOkWith`, so the INSERT's OK carries 0x0003. Reporting 0x0002 there
/// tells Connector/J with `useLocalTransactionState=true` that no transaction is
/// open; it then skips the COMMIT and the writes are silently dropped. This
/// pins the whole word for BEGIN -> DML -> COMMIT.
#[test]
fn a_write_inside_a_transaction_reports_the_open_transaction_on_its_own_ok_packet() {
    const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &TransactionFactory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    // The post-authentication OK is written the same way, off the same live
    // status: a fresh session is in autocommit and in no transaction.
    assert_eq!(read_ok_status_flags(&mut reader), SERVER_STATUS_AUTOCOMMIT);

    let query = |sql: &str| {
        let mut command = vec![COM_QUERY];
        command.extend_from_slice(sql.as_bytes());
        command
    };

    // A write OUTSIDE any transaction: autocommit only.
    write_packet(&mut client, 0, &query("INSERT INTO t VALUES (1)"));
    reader.set_sequence(1);
    assert_eq!(
        read_write_ok_status_flags(&mut reader, 1),
        SERVER_STATUS_AUTOCOMMIT
    );

    write_packet(&mut client, 0, &query("BEGIN"));
    reader.set_sequence(1);
    assert_eq!(
        read_ok_status_flags(&mut reader),
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS
    );

    // The statement the bug was about.
    write_packet(&mut client, 0, &query("INSERT INTO t VALUES (2)"));
    reader.set_sequence(1);
    assert_eq!(
        read_write_ok_status_flags(&mut reader, 1),
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS,
        "the DML's OK packet must report the transaction Connector/J has to COMMIT"
    );

    // COM_PING answers off the same live status rather than a constant, so the
    // client cannot be told mid-transaction that no transaction is open.
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(
        read_ok_status_flags(&mut reader),
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS
    );

    write_packet(&mut client, 0, &query("COMMIT"));
    reader.set_sequence(1);
    assert_eq!(read_ok_status_flags(&mut reader), SERVER_STATUS_AUTOCOMMIT);

    write_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn begin_and_commit_dispatch_toggles_the_in_transaction_status_flag() {
    // pkg/session/tidb.go finishStmt / autocommit dispatch: BEGIN and COMMIT run
    // as transaction control, answering with an OK packet whose SERVER_STATUS_
    // IN_TRANS bit reflects whether a transaction is open — not as result sets.
    const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &TransactionFactory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let query = |sql: &str| {
        let mut command = vec![COM_QUERY];
        command.extend_from_slice(sql.as_bytes());
        command
    };

    // BEGIN opens the transaction: the OK packet advertises IN_TRANS.
    write_packet(&mut client, 0, &query("BEGIN"));
    reader.set_sequence(1);
    let flags = read_ok_status_flags(&mut reader);
    assert_eq!(
        flags,
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS,
        "BEGIN must report an open transaction"
    );

    // A text query while the transaction is open fails closed rather than
    // silently running outside the pinned snapshot: the state change is real.
    write_packet(&mut client, 0, &query("SELECT 1"));
    reader.set_sequence(1);
    assert_eq!(
        reader.read_packet().unwrap()[0],
        0xff,
        "text COM_QUERY inside a transaction is fail-closed, not a fake no-op"
    );

    // COMMIT ends the transaction: IN_TRANS clears, autocommit remains.
    write_packet(&mut client, 0, &query("COMMIT"));
    reader.set_sequence(1);
    let flags = read_ok_status_flags(&mut reader);
    assert_eq!(
        flags, SERVER_STATUS_AUTOCOMMIT,
        "COMMIT must report the transaction closed"
    );

    // A fresh BEGIN re-opens IN_TRANS, proving end() truly reset the state, and
    // ROLLBACK ends the transaction identically to COMMIT for a read-only txn.
    write_packet(&mut client, 0, &query("BEGIN"));
    reader.set_sequence(1);
    assert_eq!(
        read_ok_status_flags(&mut reader),
        SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS
    );
    write_packet(&mut client, 0, &query("ROLLBACK"));
    reader.set_sequence(1);
    assert_eq!(read_ok_status_flags(&mut reader), SERVER_STATUS_AUTOCOMMIT);

    write_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    // The four transaction-control statements each count as a query; the failed
    // in-transaction SELECT does not.
    assert_eq!(report.queries, 4);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.failed(), 0);
}

// ---------------------------------------------------------------------------
// COM_STMT_SEND_LONG_DATA (0x18): the silent command with a buffer.
// ---------------------------------------------------------------------------

fn long_data_catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([ConfiguredTable::new(
        "campaign187",
        "notes",
        187,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_char_not_null("note", 2, 64),
        ],
    )])
    .unwrap()
}

/// Records the exact bind values every prepared write receives, which is where
/// a long-data parameter must arrive as one concatenated byte string.
struct LongDataSession {
    bound: Arc<Mutex<Vec<Vec<PreparedBindValue>>>>,
}

impl QuerySession for LongDataSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    fn prepare_write(&mut self, sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        let template = prepare_configured_write(sql, &long_data_catalog())
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(PreparedWrite::new(template))
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.bound.lock().unwrap().push(parameters.to_vec());
        statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(WriteOutcome {
            affected_rows: 1,
            last_insert_id: 0,
        })
    }
}

struct LongDataFactory {
    bound: Arc<Mutex<Vec<Vec<PreparedBindValue>>>>,
}

impl QuerySessionFactory for LongDataFactory {
    type Session = LongDataSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(LongDataSession {
            bound: Arc::clone(&self.bound),
        })
    }
}

fn send_long_data_command(statement_id: u32, parameter_id: u16, chunk: &[u8]) -> Vec<u8> {
    let mut command = vec![COM_STMT_SEND_LONG_DATA];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.extend_from_slice(&parameter_id.to_le_bytes());
    command.extend_from_slice(chunk);
    command
}

/// Prepares the two-marker INSERT and consumes its three response packets.
fn prepare_long_data_insert(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) -> u32 {
    let mut command = vec![COM_STMT_PREPARE];
    command.extend_from_slice(b"INSERT INTO campaign187.notes (id, note) VALUES (?, ?)");
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let prepare_ok = reader.read_packet().unwrap();
    assert_eq!(prepare_ok[0], 0);
    assert_eq!(u16::from_le_bytes([prepare_ok[7], prepare_ok[8]]), 2);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    u32::from_le_bytes(prepare_ok[1..5].try_into().unwrap())
}

/// Asserts the server has nothing further to say, which is how a command that
/// must answer with silence is observed: not by parsing its reply, but by
/// counting that no reply exists.
fn assert_no_pending_packet(
    socket: &TcpStream,
    reader: &mut PacketReader<TcpStream>,
    context: &str,
) {
    socket
        .set_read_timeout(Some(std::time::Duration::from_millis(250)))
        .unwrap();
    let pending = reader.read_packet();
    socket.set_read_timeout(None).unwrap();
    assert!(
        pending.is_err(),
        "{context}: the server wrote an unexpected packet {:?}",
        pending.map(|packet| packet.first().copied())
    );
}

#[test]
fn send_long_data_writes_no_packet_and_lands_as_the_concatenated_parameter() {
    // pkg/server/conn_stmt.go:610-625 handleStmtSendLongData returns nil,
    // and pkg/server/conn.go:1578-1579 dispatches it: a nil return writes no
    // packet at all. Answering with an ERR desynchronises the stream, and
    // answering with silence alone would drop the data, so this test pins
    // both halves at once -- zero response packets AND the concatenated value.
    // pkg/server/driver_tidb.go:104-116 TiDBStatement.AppendParam appends.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let bound = Arc::new(Mutex::new(Vec::new()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_bound = Arc::clone(&bound);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let factory = LongDataFactory {
            bound: worker_bound,
        };
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &factory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let timeout_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_long_data_insert(&mut client, &mut reader);

    // Two chunks for parameter 1, exactly as a JDBC `setBlob` past
    // `blobSendChunkSize` or a C-API `mysql_stmt_send_long_data` loop sends
    // them. The client does not read between them.
    write_packet(
        &mut client,
        0,
        &send_long_data_command(statement_id, 1, b"the first half, "),
    );
    write_packet(
        &mut client,
        0,
        &send_long_data_command(statement_id, 1, b"then the second"),
    );
    // Nothing may be on the wire yet: not after the first chunk, not after
    // the second. This is the packet count, taken before the execute reply
    // can be mistaken for it.
    assert_no_pending_packet(&timeout_side, &mut reader, "after two long-data chunks");

    // The execute carries the OTHER parameter only: the long-data parameter
    // occupies no bytes in the value section at all.
    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(0);
    execute.extend_from_slice(&1_u32.to_le_bytes());
    execute.push(0); // null bitmap: neither parameter is NULL
    execute.push(1); // new parameter types bound
    execute.extend_from_slice(&[TYPE_LONGLONG, 0, TYPE_BLOB, 0]);
    execute.extend_from_slice(&7_i64.to_le_bytes());
    write_packet(&mut client, 0, &execute);

    reader.set_sequence(1);
    let ok = reader.read_packet().unwrap();
    assert_eq!(
        ok[0],
        0,
        "the execute is answered by an OK packet, and it is the FIRST packet \
         written since the prepare response; server said: {}",
        String::from_utf8_lossy(&ok)
    );
    assert_eq!(ok[1], 1, "exactly one row was affected");
    assert_no_pending_packet(&timeout_side, &mut reader, "after the execute reply");

    assert_eq!(
        *bound.lock().unwrap(),
        vec![vec![
            PreparedBindValue::Int(7),
            PreparedBindValue::Bytes(b"the first half, then the second".to_vec()),
        ]],
        "the two chunks land as one concatenated value, in chunk order"
    );

    write_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(
        report.commands.stmt_send_long_data_commands, 2,
        "both long-data commands were dispatched -- the silence is a handled \
         command, not an ignored one"
    );
    assert_eq!(report.commands.stmt_execute_successes, 1);
}

#[test]
fn stmt_reset_drops_the_long_data_buffer_before_the_next_execute() {
    // pkg/server/conn_stmt.go:627-631 names what RESET must clear: the open
    // cursor and "the argument sent through SEND_LONG_DATA".
    // pkg/server/driver_tidb.go:151-160 stmt.Reset nils every boundParams[i].
    // This is also the no-long-data control: its value arrives entirely in
    // the execute payload.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let bound = Arc::new(Mutex::new(Vec::new()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_bound = Arc::clone(&bound);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let factory = LongDataFactory {
            bound: worker_bound,
        };
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &factory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let timeout_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    let statement_id = prepare_long_data_insert(&mut client, &mut reader);

    write_packet(
        &mut client,
        0,
        &send_long_data_command(statement_id, 1, b"abandoned"),
    );
    assert_no_pending_packet(&timeout_side, &mut reader, "after the long-data chunk");

    // COM_STMT_RESET, unlike SEND_LONG_DATA, DOES answer: Go's
    // handleStmtReset ends in `cc.writeOK` on every path.
    let mut reset = vec![COM_STMT_RESET];
    reset.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &reset);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "RESET answers with OK");

    // Both parameters now arrive in the payload, the abandoned chunk having
    // been dropped. If the buffer survived the reset, parameter 1 would be
    // "abandoned" instead.
    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(0);
    execute.extend_from_slice(&1_u32.to_le_bytes());
    execute.push(0);
    execute.push(1);
    execute.extend_from_slice(&[TYPE_LONGLONG, 0, TYPE_BLOB, 0]);
    execute.extend_from_slice(&9_i64.to_le_bytes());
    execute.push(4);
    execute.extend_from_slice(b"kept");
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    let ok = reader.read_packet().unwrap();
    assert_eq!(ok[0], 0, "the execute after a reset succeeds");

    assert_eq!(
        *bound.lock().unwrap(),
        vec![vec![
            PreparedBindValue::Int(9),
            PreparedBindValue::Bytes(b"kept".to_vec()),
        ]],
        "the reset dropped the long-data buffer; the payload value is used"
    );

    write_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.commands.stmt_reset_commands, 1);
}
