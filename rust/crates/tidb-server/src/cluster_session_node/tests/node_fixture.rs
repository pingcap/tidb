//! One mock node and one authenticated connection over it: the catalog these
//! tests plan against, the accounts they authenticate as, and the helpers that
//! open a connection and read its rows.
//!
//! The catalog is *loaded*, never created by a statement, because that is what
//! the production path does -- `load_catalog_from_cluster` reads a Go TiDB's
//! `TableInfo`s and the connection's tables are built over them. A second
//! connection on the same node is what makes a racing writer, or a peer that
//! must notice a DDL, expressible in SQL rather than in raw keys.

use super::super::*;
use super::mock_cluster::*;
use super::mock_seams::*;
use crate::configured_user_store::ConfiguredUserStore;
use crate::connection_writers::{write_query_error, ConnectionPacketOutput};
use crate::mysql_connection::MysqlConnectionError;
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    cluster_commit_error, ConnectionCancellation, ConnectionClose, SqlQueryError,
};
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, GoString};
use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
use tidb_model::column::ColumnInfo as ModelColumnInfo;
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::{SchemaState, TableInfo};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, TransactionCause,
    UndeterminedTransaction,
};

pub(super) const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
pub(super) const SALT: [u8; 20] = [7; 20];
/// Go `mysql.PriKeyFlag`.
pub(super) const PRI_KEY_FLAG: u32 = 1 << 1;
/// Go `mysql.AutoIncrementFlag`.
pub(super) const AUTO_INCREMENT_FLAG: u32 = 1 << 9;

#[derive(Default)]
struct RecordingPacketOutput(Vec<Vec<u8>>);

impl ConnectionPacketOutput for RecordingPacketOutput {
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) -> Result<u8, MysqlConnectionError> {
        self.0.push(payload.to_vec());
        Ok(sequence.wrapping_add(1))
    }
}

pub(super) fn assert_undetermined_closes_without_packet(query_error: &SqlQueryError) {
    let mut output = RecordingPacketOutput::default();
    let write_error = write_query_error(&mut output, query_error, true)
        .expect_err("an unknown commit verdict must close the connection");
    assert!(matches!(
        write_error,
        MysqlConnectionError::ResultUndetermined(message)
            if message == tidb_error::terror::ERR_RESULT_UNDETERMINED.message()
    ));
    assert!(
        output.0.is_empty(),
        "an unknown commit verdict must not write an ERR packet or any bytes"
    );
}

pub(super) fn assert_query_error_packet(
    query_error: &SqlQueryError,
    expected_code: u16,
    expected_message: &str,
) {
    let mut output = RecordingPacketOutput::default();
    write_query_error(&mut output, query_error, true).expect("a determinate error writes ERR");
    assert_eq!(output.0.len(), 1, "one determinate error writes one packet");
    let packet = &output.0[0];
    assert!(packet.len() >= 9, "protocol-41 ERR packet is complete");
    assert_eq!(packet[0], 0xff);
    assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), expected_code);
    assert_eq!(&packet[3..9], b"#HY000");
    assert!(
        String::from_utf8_lossy(&packet[9..]).contains(expected_message),
        "packet message {:?} did not contain {expected_message:?}",
        String::from_utf8_lossy(&packet[9..])
    );
}

pub(super) fn undetermined_cluster_commit_error(subject: &str) -> SqlQueryError {
    let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
        cause: TransactionCause::Transport {
            detail: "commit response lost".to_owned(),
        },
    });
    cluster_commit_error(&outcome, subject)
        .expect("an unknown commit verdict must produce a connection-fatal error")
}

/// A `BIGINT AUTO_INCREMENT PRIMARY KEY` column: the one shape whose written
/// value the server, not the client, decides.
pub(super) fn auto_increment_column(id: i64, offset: i64, name: &str) -> ModelColumnInfo {
    let mut field_type = FieldType::new(FieldTypeCode::LongLong);
    field_type.add_flags(PRI_KEY_FLAG | AUTO_INCREMENT_FLAG);
    let mut column = ModelColumnInfo::new(id, name, field_type);
    column.offset = offset;
    column
}

pub(super) fn column(id: i64, offset: i64, name: &str, primary: bool) -> ModelColumnInfo {
    let mut field_type = FieldType::new(FieldTypeCode::LongLong);
    if primary {
        field_type.add_flags(PRI_KEY_FLAG);
    }
    let mut column = ModelColumnInfo::new(id, name, field_type);
    column.offset = offset;
    column
}

/// One column shaped the way `mysql.user`/`mysql.tables_priv` shape theirs:
/// an `ENUM`/`SET` with its declared element list.
pub(super) fn named_value_column(
    id: i64,
    offset: i64,
    name: &str,
    code: FieldTypeCode,
    elems: &[&str],
) -> ModelColumnInfo {
    let mut field_type = FieldType::new(code);
    field_type.set_elems(
        elems
            .iter()
            .map(|elem| GoString::from(*elem))
            .collect::<Vec<_>>(),
    );
    let mut column = ModelColumnInfo::new(id, name, field_type);
    column.offset = offset;
    column
}

/// `app.t(id BIGINT PRIMARY KEY, v BIGINT)` and
/// `app.g(id BIGINT PRIMARY KEY, grp BIGINT)`, plus one table mid-DDL the
/// session must refuse by name, plus `app.acct` -- `mysql.user`'s own
/// shape, an `ENUM('N','Y')` privilege column beside a `SET` one, which is
/// what a `SELECT ... FROM mysql.user` has to serve.
pub(super) fn loaded_catalog() -> ClusterCatalog {
    let acct = TableInfo {
        id: 104,
        name: CiString::new("acct"),
        columns: vec![
            column(1, 0, "id", true),
            named_value_column(2, 1, "select_priv", FieldTypeCode::Enum, &["N", "Y"]),
            named_value_column(
                3,
                2,
                "table_priv",
                FieldTypeCode::Set,
                &["Select", "Insert", "Update", "Grant"],
            ),
        ]
        .into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let t = TableInfo {
        id: 101,
        name: CiString::new("t"),
        columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let g = TableInfo {
        id: 102,
        name: CiString::new("g"),
        columns: vec![column(1, 0, "id", true), column(2, 1, "grp", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    // `app.hnd(v BIGINT UNIQUE)`: no primary key, so its row handles come
    // from `KvTable`'s own `next_handle` counter (Go's `_tidb_rowid`)
    // rather than from a column, and the unique index gives a failure that
    // lands AFTER earlier rows of the same statement are already staged.
    // Both are what the rollback tests below need.
    let hnd = TableInfo {
        id: 105,
        name: CiString::new("hnd"),
        columns: vec![column(1, 0, "v", false)].into(),
        indices: vec![IndexInfo {
            id: 1,
            name: CiString::new("uv"),
            table: CiString::new("hnd"),
            columns: vec![IndexColumn {
                name: CiString::new("v"),
                offset: 0,
                length: -1,
                ..IndexColumn::default()
            }]
            .into(),
            unique: true,
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        }]
        .into(),
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    // `app.ai(id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)`: the one table
    // whose primary key the SERVER picks, so a statement that runs twice can
    // write a different row the second time without the client's SQL changing
    // a character. That is what the auto-increment retry cases need.
    let ai = TableInfo {
        id: 106,
        name: CiString::new("ai"),
        columns: vec![auto_increment_column(1, 0, "id"), column(2, 1, "v", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let pending = TableInfo {
        id: 103,
        name: CiString::new("t_pending"),
        columns: vec![column(1, 0, "id", false)].into(),
        state: SchemaState::NONE,
        ..TableInfo::default()
    };
    ClusterCatalog {
        schema_version: 11,
        databases: vec![LoadedDatabase {
            info: DBInfo {
                id: 5,
                name: CiString::new("app"),
                ..DBInfo::default()
            },
            tables: vec![t, g, pending, acct, hnd, ai],
        }],
    }
}

pub(super) fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
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

/// One mock node: the committed rows, the published catalog, and the
/// catalog writer, all shared by every connection opened on it -- which is
/// what lets a test watch one connection's DDL reach another's.
pub(super) struct MockNode {
    pub(super) cluster: Arc<MockCluster>,
    pub(super) catalog: Arc<SharedClusterCatalog>,
    pub(super) ddl: Arc<MockDdl>,
    pub(super) accounts: Arc<MockAccountWriter>,
    pub(super) sysvars: Arc<MockSysvarWriter>,
}

/// A detached copy of one registry's rows, since
/// [`PrivilegeRegistry::replace_from`] empties its source.
pub(super) fn clone_registry(source: &PrivilegeRegistry) -> PrivilegeRegistry {
    let copy = PrivilegeRegistry::bootstrapped_from(Vec::new());
    for (user, host) in source.accounts() {
        if source.is_role(&user, &host) {
            copy.create_role(&user, &host);
        } else {
            copy.create_user_with_plugin(
                &user,
                &host,
                &source.auth_string(&user, &host).unwrap_or_default(),
                &source.plugin(&user, &host).unwrap_or_default(),
            );
        }
    }
    for ((user, host), mask) in source.global_priv_masks() {
        copy.grant(&user, &host, mask);
    }
    copy
}

/// The node IS its committed store as far as an assertion is concerned, so
/// a test that only cares about rows and timestamps reads them directly.
impl std::ops::Deref for MockNode {
    type Target = MockCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl MockNode {
    pub(super) fn start() -> Self {
        let catalog = Arc::new(SharedClusterCatalog::new(loaded_catalog()));
        Self {
            cluster: Arc::new(MockCluster::default()),
            ddl: Arc::new(MockDdl::new(Arc::clone(&catalog))),
            accounts: Arc::new(MockAccountWriter::new()),
            sysvars: Arc::new(MockSysvarWriter::new()),
            catalog,
        }
    }
}

/// One authenticated connection over a fresh mock node, plus the node the
/// test inspects.
pub(super) fn open_session() -> (ClusterServerSession, MockNode) {
    let node = MockNode::start();
    let session = open_session_on(&node);
    (session, node)
}

/// A second connection to the same mock node, which is what makes a racing
/// writer -- or a peer that must notice a DDL -- expressible in SQL rather
/// than in raw keys.
pub(super) fn open_session_on(node: &MockNode) -> ClusterServerSession {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
    )
}

/// Opens a connection with an injected catalog writer, so error-path tests can
/// drive the same DDL route without constructing a real TiKV authority.
pub(super) fn open_session_on_with_ddl(
    node: &MockNode,
    ddl: Arc<dyn ClusterDdl>,
) -> ClusterServerSession {
    open_session_on_with_seams(
        node,
        ddl,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
    )
}

pub(super) fn open_session_on_with_accounts(
    node: &MockNode,
    accounts: Arc<dyn ClusterAccountWriter>,
) -> ClusterServerSession {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        accounts,
        Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
    )
}

pub(super) fn open_session_on_with_sysvars(
    node: &MockNode,
    sysvars: Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
) -> ClusterServerSession {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        sysvars,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
    )
}

pub(super) fn open_session_on_with_analyze(
    node: &MockNode,
    analyze: Arc<dyn ClusterAnalyze>,
) -> ClusterServerSession {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
        analyze,
    )
}

fn open_session_on_with_seams(
    node: &MockNode,
    ddl: Arc<dyn ClusterDdl>,
    accounts: Arc<dyn ClusterAccountWriter>,
    sysvars: Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
    analyze: Arc<dyn ClusterAnalyze>,
) -> ClusterServerSession {
    let cluster = Arc::clone(&node.cluster);
    let factory = ClusterSessionFactory::new(
        Arc::new(MockTransactions(cluster)),
        ddl,
        accounts,
        sysvars,
        analyze,
        Arc::clone(&node.catalog),
        node.accounts.live.clone(),
        node.sysvars.live.clone(),
        Arc::new(SharedStats::new(
            tidb_exec::stats_watch::StatsSnapshot::new(),
        )),
        Arc::new(crate::cluster_session::LocalTableAutoIds::default()),
    );
    let mut session = factory
        .open_session(session_context(1))
        .expect("the cluster session opens");
    // The catalog is loaded, not created here: `USE` is how a connection
    // reaches it, exactly as it does over the wire.
    session.execute_write("USE app").expect("USE app");
    session
}

pub(super) fn session_context(connection_id: u64) -> SessionContext {
    let users =
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store");
    let identity = users
        .authenticate_native("root", "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
        .expect("authenticated identity");
    let peer_addr: SocketAddr = "127.0.0.1:4000".parse().expect("peer address");
    SessionContext {
        connection_id,
        peer_addr,
        identity,
        cancellation: ConnectionCancellation::default(),
        close: ConnectionClose::default(),
        version_info: tidb_util::versioninfo::VersionInfo::build_default(),
    }
}

pub(super) fn rows(session: &mut ClusterServerSession, sql: &str) -> Vec<Vec<Datum>> {
    let mut result = session.execute(sql).expect("the query runs");
    let source = result.source();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(8).expect("batch");
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish().expect("finish");
    source.close().expect("close");
    rows
}
