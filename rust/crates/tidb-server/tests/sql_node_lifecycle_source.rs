// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionTracker,
    MysqlConnectionError, QueryResult, QuerySession, QuerySessionFactory, SessionContext,
    SqlQueryError,
};

struct UnusedSession;

impl QuerySession for UnusedSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        panic!("malformed connection must not reach query execution")
    }
}

struct UnusedFactory;

impl QuerySessionFactory for UnusedFactory {
    type Session = UnusedSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        panic!("malformed connection must not reach session creation")
    }
}

#[test]
fn framing_failure_releases_connection_lease_exactly_once() {
    // pkg/server/conn_test.go:595 TestIssue1768
    // pkg/server/conn_test.go:1108 TestShutDown
    // pkg/server/conn_test.go:2054 TestMaxAllowedPacket
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).unwrap();
    let (server, _) = listener.accept().unwrap();
    drop(client);

    let tracker = Arc::new(ConnectionTracker::default());
    let users = ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap();
    let error = serve_mysql_connection(
        server,
        SocketAddr::from(([127, 0, 0, 1], 40000)),
        ConnectionCancellation::default(),
        &UnusedFactory,
        &users,
        &tracker,
        DEFAULT_MAX_ALLOWED_PACKET,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MysqlConnectionError::Io(_) | MysqlConnectionError::Packet(_)
    ));
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.failed(), 1);
}
