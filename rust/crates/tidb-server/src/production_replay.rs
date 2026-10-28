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

//! The integration corpus replayed through the production node over the
//! embedded store -- the stack `tidb-server --store unistore` serves, and the
//! one Go's own integration runner uses (`mockstore.EmbedUnistoreDriver`).
//!
//! The comparison is the corpus harness's own (`difftest_result_tests::replay`);
//! this module only supplies the backend, so a topic's counts here and from
//! `difftests/result-tests/tests/integration_diff.rs` measure the two engines
//! by one rule (`rust/docs/one-engine-corpus-execplan.md`).
//!
//!     INTEGRATION_TOPIC=executor/update cargo test -p tidb-server --lib \
//!         production_replay -- --ignored --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;

use difftest_result_tests::mysqltest_connections::{
    recording_server_time_zone, topic_database, MYSQL_TESTER_CONNECTION_SETUP,
};
use difftest_result_tests::mysqltest_script::ConnectionCmd;
use difftest_result_tests::replay::{run_topic, ReplayBackend, ReplaySession};
use tidb_executor::{DriverError, MysqlError};
use tidb_session::StmtOutput;

use tidb_session::domain::cluster_session_node::ClusterServerSession;
use crate::domain_test_support::{
    scramble, session_context, session_context_with_identity, ABC_HASH, SALT,
};
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_session::domain::session_api::{QuerySession, SqlQueryError};
use tidb_session::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use crate::QuerySessionFactory;

/// The connection mysql-tester starts every script on.
const DEFAULT_CONNECTION: &str = "default";

/// One connection: a production session and the output names of its last
/// result, which the comparison reads after the statement.
struct UnistoreSession {
    session: ClusterServerSession,
    output_names: Vec<tidb_datatype::FieldNameMetadata>,
    /// The connection's cancellation, which the connection loop installs the
    /// session's query cancellation into for every command: the kill target
    /// the process list's `KILL`/`max_execution_time` reaches.
    cancellation: tidb_session::domain::session_api::ConnectionCancellation,
}

fn driver_error(error: SqlQueryError) -> DriverError {
    DriverError::Mysql(
        MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}

fn source_error(error: MysqlError) -> DriverError {
    DriverError::Mysql(error)
}

impl ReplaySession for UnistoreSession {
    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, DriverError> {
        self.session
            .migration_session()
            .expect("a cluster session owns its SQL session")
            .split_statements(sql, client_multi_statements)
    }

    /// The connection loop's COM_QUERY order for one statement
    /// (`mysql_connection.rs`): parse once, then transaction control, then a
    /// write answered by an OK packet, and only then a result-set query.
    fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        self.output_names.clear();
        // `mysql_connection` installs the session's query cancellation for
        // the whole command, so a kill or `max_execution_time` interrupts it.
        let _query_cancellation = self
            .session
            .query_cancellation()
            .map(|active| self.cancellation.install(active));
        let stmt = match self.session.parse_statement(sql) {
            Ok(stmt) => stmt,
            Err(error) => {
                self.session
                    .record_parse_failure(&error);
                return Err(driver_error(error));
            }
        };
        // One `ExecuteStmt`, as the connection runs it. The rows a client
        // reads are the server's result-set packets, so the comparison reads
        // exactly those: the connection's own writer frames the result and it
        // is decoded as mysql-tester decodes it.
        let response = match self
            .session
            .execute_stmt(sql, &stmt)
            .map_err(driver_error)?
        {
            Some(mut result) => Some(
                crate::connection_resultset::write_connection_result_set(
                    result.source(),
                    tidb_protocol::ResultSetOptions::default(),
                    1024,
                )
                .map_err(|error| source_error(error.cause))?,
            ),
            None => None,
        };
        let Some(response) = response else {
            // LOAD STATS parks a file request the connection answers with the
            // client's file, which mysql-tester reads relative to
            // `tests/integrationtest`.
            if let Some(path) = self.session.take_local_infile_request() {
                let data =
                    std::fs::read(difftest_result_tests::replay::integrationtest_dir().join(&path))
                        .map_err(|error| {
                            DriverError::Mysql(MysqlError::unknown(format!(
                                "client file `{path}`: {error}"
                            )))
                        })?;
                self.session
                    .finish_local_infile(&data)
                    .map_err(driver_error)?;
            }
            return Ok(StmtOutput::Affected(self.session.affected_rows()));
        };
        if let Some(session) = self.session.migration_session() {
            self.output_names = session.last_result_output_names().0.to_vec();
        }
        decode_text_result_set(&response.framed)
    }

    fn last_result_output_names(&self) -> Vec<tidb_datatype::FieldNameMetadata> {
        self.output_names.clone()
    }
}

/// Decodes one text-protocol result set -- column count, column definitions,
/// EOF, length-encoded rows, EOF -- into the column names and each cell's
/// wire text, typed as a plain string so the harness renders it unchanged.
fn decode_text_result_set(framed: &[u8]) -> Result<StmtOutput, DriverError> {
    fn malformed(what: &str) -> DriverError {
        DriverError::Mysql(MysqlError::unknown(format!("malformed result set: {what}")))
    }
    fn packets(framed: &[u8]) -> Result<Vec<&[u8]>, DriverError> {
        let mut packets = Vec::new();
        let mut at = 0;
        while at < framed.len() {
            let header = framed
                .get(at..at + 4)
                .ok_or_else(|| malformed("short header"))?;
            let length = usize::from(header[0])
                | (usize::from(header[1]) << 8)
                | (usize::from(header[2]) << 16);
            let payload = framed
                .get(at + 4..at + 4 + length)
                .ok_or_else(|| malformed("short payload"))?;
            packets.push(payload);
            at += 4 + length;
        }
        Ok(packets)
    }
    /// A length-encoded integer; `None` is the NULL marker.
    fn length(buf: &[u8], at: &mut usize) -> Result<Option<u64>, DriverError> {
        let first = *buf.get(*at).ok_or_else(|| malformed("missing length"))?;
        *at += 1;
        let width = match first {
            0xfb => return Ok(None),
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            value => return Ok(Some(u64::from(value))),
        };
        let bytes = buf
            .get(*at..*at + width)
            .ok_or_else(|| malformed("short length"))?;
        *at += width;
        Ok(Some(
            bytes
                .iter()
                .rev()
                .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)),
        ))
    }
    fn text<'b>(buf: &'b [u8], at: &mut usize) -> Result<Option<&'b [u8]>, DriverError> {
        let Some(len) = length(buf, at)? else {
            return Ok(None);
        };
        let len = usize::try_from(len).map_err(|_| malformed("huge cell"))?;
        let bytes = buf
            .get(*at..*at + len)
            .ok_or_else(|| malformed("short cell"))?;
        *at += len;
        Ok(Some(bytes))
    }
    let is_eof = |packet: &[u8]| packet.first() == Some(&0xfe) && packet.len() < 9;
    let packets = packets(framed)?;
    let mut packets = packets.into_iter();
    let count_packet = packets.next().ok_or_else(|| malformed("no column count"))?;
    let count = length(count_packet, &mut 0)?.ok_or_else(|| malformed("null column count"))?;
    let text_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    let mut columns = Vec::new();
    for _ in 0..count {
        let definition = packets.next().ok_or_else(|| malformed("missing column"))?;
        let mut at = 0;
        // catalog, schema, table, org_table, then the display name.
        for _ in 0..4 {
            text(definition, &mut at)?;
        }
        let name = text(definition, &mut at)?.unwrap_or_default();
        columns.push((
            String::from_utf8_lossy(name).into_owned(),
            text_type.clone(),
        ));
    }
    if packets
        .as_slice()
        .first()
        .is_some_and(|packet| is_eof(packet))
    {
        packets.next();
    }
    let mut rows = Vec::new();
    for packet in packets {
        if is_eof(packet) {
            break;
        }
        let mut at = 0;
        let mut row = Vec::with_capacity(columns.len());
        for _ in 0..columns.len() {
            row.push(match text(packet, &mut at)? {
                Some(bytes) => tidb_datatype::Datum::Bytes(bytes.to_vec()),
                None => tidb_datatype::Datum::Null,
            });
        }
        rows.push(row);
    }
    Ok(StmtOutput::Rows { columns, rows })
}

/// Go `Domain.ExpensiveQueryHandle`, which the server's main starts over its
/// session manager: it is what interrupts a statement past its
/// `max_execution_time`. The harness is the server's main here.
struct ExpensiveQuerySweep {
    exit: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ExpensiveQuerySweep {
    fn start(manager: Arc<dyn tidb_util::memoryusagealarm::SessionManager>) -> Self {
        let (exit, receiver) = std::sync::mpsc::channel();
        let handle = tidb_session::expensivequery::Handle::new(receiver);
        handle.set_session_manager(manager);
        let thread = std::thread::spawn(move || handle.run());
        Self {
            exit,
            thread: Some(thread),
        }
    }
}

impl Drop for ExpensiveQuerySweep {
    fn drop(&mut self) {
        let _ = self.exit.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A topic's connections over one fresh embedded store.
struct UnistoreConnections {
    stack: UnistoreClusterStack,
    users: Arc<ConfiguredUserStore>,
    sessions: BTreeMap<String, UnistoreSession>,
    current: String,
    next_connection_id: u64,
    _expensive_query: Option<ExpensiveQuerySweep>,
}

impl UnistoreConnections {
    /// Opens a root connection with mysql-tester's per-connection setup, on
    /// `database` when one is named.
    fn open_root(&mut self, database: Option<&str>) -> Result<UnistoreSession, String> {
        let connection_id = self.next_connection_id;
        self.open_with(session_context(connection_id), database)
    }

    /// Logs `user` in from `host` the way mysql-tester does -- its password
    /// scrambled against the handshake salt, through the node's own account
    /// verification -- and opens the connection.
    fn open_account(
        &mut self,
        user: &str,
        host: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<UnistoreSession, String> {
        let remote = if host == "localhost" {
            "127.0.0.1"
        } else {
            host
        };
        let response = if password.is_empty() {
            Vec::new()
        } else {
            scramble(password.as_bytes(), &SALT).to_vec()
        };
        let identity = self
            .users
            .authenticate_native(user, remote, &SALT, &response)
            .map_err(|error| format!("log in `{user}`@`{host}`: {error:?}"))?;
        let connection_id = self.next_connection_id;
        self.open_with(
            session_context_with_identity(connection_id, identity),
            database,
        )
    }

    fn open_with(
        &mut self,
        context: tidb_session::domain::session_api::SessionContext,
        database: Option<&str>,
    ) -> Result<UnistoreSession, String> {
        let connection_id = self.next_connection_id;
        self.next_connection_id += 1;
        let cancellation = context.cancellation.clone();
        let session = self
            .stack
            .factory
            .open_session(context)
            .map_err(|error| format!("open connection {connection_id}: {}", error.message))?;
        let mut session = UnistoreSession {
            session,
            output_names: Vec::new(),
            cancellation,
        };
        // A handshake naming no schema leaves the connection with none
        // selected, as `mysql_connection` applies it: `connect (c, h, u,,)`
        // answers `SHOW TABLES` with 1046.
        if database.is_none() {
            session.session.deselect_database();
        }
        for setup in MYSQL_TESTER_CONNECTION_SETUP {
            session
                .run_with_columns(setup)
                .map_err(|error| format!("connection setup `{setup}`: {error:?}"))?;
        }
        if let Some(database) = database {
            session
                .run_with_columns(&format!("use `{database}`"))
                .map_err(|error| format!("use `{database}`: {error:?}"))?;
        }
        Ok(session)
    }
}

impl ReplayBackend for UnistoreConnections {
    fn open(topic: &str) -> Result<Self, String> {
        recording_server_time_zone();
        // The corpus was recorded by a server started with
        // `tests/integrationtest/config.toml` (`stats-lease = "0"`, table
        // locks enabled, new charset and expression indexes allowed): that
        // file is the environment the recorded answers belong to.
        let recording_config =
            difftest_result_tests::replay::integrationtest_dir().join("config.toml");
        let recording_config = recording_config.to_string_lossy().into_owned();
        let config = tidb_session::domain::node_config::NodeConfig::parse([
            "tidb-server",
            "--store",
            "unistore",
            "--port",
            "0",
            "--auth-file",
            "/dev/null",
            "--config",
            recording_config.as_str(),
        ])
        .map_err(|error| format!("node config: {error:?}"))?;
        let users = Arc::new(
            ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
                .map_err(|error| format!("configured user store: {error:?}"))?,
        );
        let stack = unistore_cluster_session_stack(&config, &users, None, None)
            .map_err(|error| format!("unistore stack: {error:?}"))?;
        let expensive_query = stack
            .factory
            .session_manager()
            .map(ExpensiveQuerySweep::start);
        let mut pool = UnistoreConnections {
            stack,
            users,
            sessions: BTreeMap::new(),
            current: DEFAULT_CONNECTION.to_owned(),
            next_connection_id: 1,
            _expensive_query: expensive_query,
        };
        let database = topic_database(topic);
        let mut session = pool.open_root(None)?;
        session
            .run_with_columns(&format!("create database if not exists `{database}`"))
            .map_err(|error| format!("create topic database `{database}`: {error:?}"))?;
        session
            .run_with_columns(&format!("use `{database}`"))
            .map_err(|error| format!("use topic database `{database}`: {error:?}"))?;
        pool.sessions.insert(DEFAULT_CONNECTION.to_owned(), session);
        Ok(pool)
    }

    fn apply(&mut self, cmd: &ConnectionCmd) -> Result<(), String> {
        match cmd {
            ConnectionCmd::Open {
                name,
                host,
                user,
                password,
                db,
            } => {
                let database = (!db.is_empty()).then_some(db.as_str());
                let session = if user == "root" {
                    self.open_root(database)?
                } else {
                    self.open_account(user, host, password, database)?
                };
                self.sessions.insert(name.clone(), session);
                self.current = name.clone();
                Ok(())
            }
            ConnectionCmd::Switch(name) => {
                if !self.sessions.contains_key(name) {
                    return Err(format!("`connection {name}` names no open connection"));
                }
                self.current = name.clone();
                Ok(())
            }
            ConnectionCmd::Close(name) => {
                if self.sessions.remove(name).is_none() {
                    return Err(format!("`disconnect {name}` names no open connection"));
                }
                if self.current == *name {
                    self.current = DEFAULT_CONNECTION.to_owned();
                }
                Ok(())
            }
        }
    }

    fn with_current<R>(&mut self, body: &mut dyn FnMut(&mut dyn ReplaySession) -> R) -> R {
        let session = self
            .sessions
            .get_mut(&self.current)
            .expect("the current connection is open: `apply` never leaves a dangling name");
        body(session)
    }
}

/// Replays each topic named by `INTEGRATION_TOPIC` (comma-separated) and
/// prints the harness's per-topic line, with divergences and hidden failures
/// under the harness's own switches.
#[test]
#[ignore = "corpus replay over the embedded store; name topics with INTEGRATION_TOPIC"]
fn replay_corpus_topics_on_unistore() {
    let Ok(topics) = std::env::var("INTEGRATION_TOPIC") else {
        return;
    };
    for topic in topics
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
    {
        let started = std::time::Instant::now();
        match run_topic::<UnistoreConnections>(topic) {
            Ok(report) => {
                eprintln!(
                    "{:>5} matched {:>5} diverged of {:>5} in {:>6}ms  {topic}  {:?} {:?}",
                    report.matched_total(),
                    report.divergences.len(),
                    report.total(),
                    started.elapsed().as_millis(),
                    report.matched,
                    report.skipped,
                );
                if std::env::var_os("INTEGRATION_SHOW_DIVERGENCES").is_some() {
                    eprintln!("{}", report.divergences.join(""));
                }
                // One tab-separated line per topic and per divergence for
                // `production_replay_stays_within_budget`, which runs this
                // test as its shards.
                eprintln!(
                    "REPLAY-TOPIC\t{topic}\t{}\t{}\t{}",
                    report.matched_total(),
                    report.divergences.len(),
                    report.skipped.get("OutOfDomain").copied().unwrap_or(0),
                );
                for divergence in &report.divergences {
                    let headline = divergence
                        .lines()
                        .find(|line| line.starts_with("--- "))
                        .unwrap_or("--- <no headline>");
                    eprintln!("REPLAY-DIVERGENCE\t{topic}\t{headline}");
                }
                // Debug-quoted, so a statement or error spanning lines stays
                // one field.
                for (sql, error) in &report.out_of_domain {
                    eprintln!("REPLAY-OUT-OF-DOMAIN\t{topic}\t{sql:?}\t{error:?}");
                }
            }
            Err(reason) => eprintln!("UNALIGNED {topic}: {reason}"),
        }
    }
}

/// The corpus replayed on the production engine, as a gate: every enrolled
/// topic must run to its end, and the divergence and out-of-domain counts may
/// only go down. The topics run in child processes of this test binary, eight
/// shards of [`replay_corpus_topics_on_unistore`], because each topic boots
/// its own embedded store over process-wide state (`isolate_process_globals`),
/// so two topics cannot share one process.
///
///     cargo test -p tidb-server --lib production_replay_stays_within_budget \
///         -- --ignored --nocapture
#[test]
#[ignore = "about seven minutes; the gate script runs it"]
fn production_replay_stays_within_budget() {
    // Measured 2026-10-25: only the in-process replay's three known stale
    // recordings (`session/nontransactional`'s USE_INDEX spacing) remain.
    // Lower these when a divergence is fixed; never raise them.
    const DIVERGENCE_BUDGET: usize = 3;
    const OUT_OF_DOMAIN_BUDGET: usize = 0;
    const SHARDS: usize = 8;

    let topics = difftest_result_tests::enrolled_topics::TOPICS;
    let mut shards = vec![Vec::new(); SHARDS];
    for (index, topic) in topics.iter().enumerate() {
        shards[index % SHARDS].push(*topic);
    }
    let executable = std::env::current_exe().expect("test binary");
    let children = shards
        .iter()
        .map(|shard| {
            std::process::Command::new(&executable)
                .args([
                    "production_replay::replay_corpus_topics_on_unistore",
                    "--ignored",
                    "--nocapture",
                    "--exact",
                ])
                .env("INTEGRATION_TOPIC", shard.join(","))
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn replay shard")
        })
        .collect::<Vec<_>>();

    let mut reported = BTreeMap::new();
    let mut divergences = Vec::new();
    let mut refused = Vec::new();
    let mut failures = Vec::new();
    for (index, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("replay shard output");
        let text = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            let tail: Vec<&str> = text.lines().rev().take(5).collect();
            failures.push(format!("shard {index} exited {}: {tail:?}", output.status));
        }
        for line in text.lines() {
            let fields: Vec<&str> = line.split('\t').collect();
            match fields.as_slice() {
                ["REPLAY-TOPIC", topic, _matched, diverged, out_of_domain] => {
                    reported.insert(
                        (*topic).to_owned(),
                        (
                            diverged.parse::<usize>().unwrap(),
                            out_of_domain.parse::<usize>().unwrap(),
                        ),
                    );
                }
                ["REPLAY-DIVERGENCE", topic, headline] => {
                    divergences.push(format!("[{topic}] {headline}"));
                }
                ["REPLAY-OUT-OF-DOMAIN", topic, sql, error] => {
                    refused.push(format!("[{topic}] {sql}\n    {error}"));
                }
                _ if line.starts_with("UNALIGNED ") => failures.push(line.to_owned()),
                // A stored table the session could not build: Go has no such
                // state, and the topic's later statements would fail on it.
                _ if line.contains("\"event\":\"table_skipped\"") => {
                    failures.push(format!("shard {index}: {line}"));
                }
                _ => {}
            }
        }
    }
    let missing: Vec<&str> = topics
        .iter()
        .copied()
        .filter(|topic| !reported.contains_key(*topic))
        .collect();
    assert!(
        failures.is_empty() && missing.is_empty(),
        "production replay did not finish every topic: {failures:#?}\nmissing: {missing:?}"
    );
    let diverged: usize = reported.values().map(|(diverged, _)| diverged).sum();
    let out_of_domain: usize = reported.values().map(|(_, skipped)| skipped).sum();
    let refusing: Vec<String> = reported
        .iter()
        .filter(|(_, (_, skipped))| *skipped > 0)
        .map(|(topic, (_, skipped))| format!("{topic} ({skipped})"))
        .collect();
    eprintln!(
        "production replay: {} topics, {diverged} diverged, {out_of_domain} out of domain {refusing:?}\n  {}\n  {}",
        reported.len(),
        divergences.join("\n  "),
        refused.join("\n  ")
    );
    assert!(
        diverged <= DIVERGENCE_BUDGET,
        "production replay diverged in {diverged} statements (budget {DIVERGENCE_BUDGET}):\n  {}",
        divergences.join("\n  ")
    );
    assert!(
        out_of_domain <= OUT_OF_DOMAIN_BUDGET,
        "{out_of_domain} statements Go answered were refused (budget {OUT_OF_DOMAIN_BUDGET}):\n  {}",
        refused.join("\n  ")
    );
}
