// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};
use tidb_datatype::Datum;
use tidb_exec::real_tikv_dml::prepare_configured_write;
use tidb_exec::real_tikv_read::prepare_configured_point_read;
use tidb_planner::prepared_dml::{ConfiguredPreparedWrite, PreparedBindValue};
use tidb_planner::read_only_scan::{
    configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredTable,
};
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, COM_INIT_DB, COM_PING, COM_QUERY, COM_QUIT,
    COM_STMT_CLOSE, COM_STMT_EXECUTE, COM_STMT_PREPARE, DEFAULT_MAX_ALLOWED_PACKET, TYPE_LONGLONG,
};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker, PreparedPointRead, PreparedWrite, QueryResult, QuerySession,
    QuerySessionFactory, ResultSetSource, SessionContext, SessionTransaction, SqlQueryError,
    WriteOutcome,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

#[derive(Default)]
struct Lifecycle {
    finished: usize,
    closed: usize,
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
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    assert_eq!(initial[version_end + 16], 46);

    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
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

struct PreparedRows {
    value: Option<i64>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl ResultSetSource for PreparedRows {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
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
        Ok(PreparedPointRead::new(
            template,
            vec![prepared_balance_column()],
        ))
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
        Ok(QueryResult::new(Box::new(PreparedRows {
            value: Some(*value + 100),
            lifecycle: Arc::clone(&self.lifecycle),
        })))
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
            ConfiguredPreparedWrite::InsertRows { rows, .. } => rows.len() as u64,
            ConfiguredPreparedWrite::UpdatePoint { .. } | ConfiguredPreparedWrite::DeletePoint { .. } => 1,
        };
        Ok(WriteOutcome { affected_rows })
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

fn prepare_statement(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) -> u32 {
    let mut command = vec![COM_STMT_PREPARE];
    command.extend_from_slice(b"SELECT balance FROM campaign27.rows WHERE id = ?");
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let prepare_ok = reader.read_packet().unwrap();
    assert_eq!(prepare_ok[0], 0);
    assert_eq!(u16::from_le_bytes([prepare_ok[5], prepare_ok[6]]), 1);
    assert_eq!(u16::from_le_bytes([prepare_ok[7], prepare_ok[8]]), 1);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
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

/// A session that owns only the read-only transaction state machine, faithfully
/// mirroring `RealTiKvServerSession`: `control_transaction` delegates to the same
/// classifier plus [`SessionTransaction`], and a text `execute` fails closed
/// while a transaction is open — proving BEGIN/COMMIT change real session state
/// rather than merely painting an OK packet.
#[derive(Default)]
struct TransactionSession {
    transaction: SessionTransaction,
}

impl QuerySession for TransactionSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        if self.transaction.is_active() {
            return Err(SqlQueryError::unknown(
                "COM_QUERY statements inside an explicit transaction are not yet supported",
            ));
        }
        Err(SqlQueryError::unknown(
            "text execution is not part of this test",
        ))
    }

    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        match classify_transaction_control(sql) {
            None => Ok(None),
            Some(TransactionControl::Begin) => {
                self.transaction.begin();
                Ok(Some(true))
            }
            Some(TransactionControl::End) => {
                self.transaction.end();
                Ok(Some(false))
            }
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
