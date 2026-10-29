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
use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::resultset_source::ResultSetSource;
use crate::domain::session_api::{ConnectionCancellation, ConnectionClose};
use tidb_store_driver_error::pessimistic_lock_error::cluster_commit_error;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, GoString};
use tidb_infoschema::cluster_catalog::{ClusterCatalog, LoadedDatabase};
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

/// The verdict the MySQL front end turns into "close without a packet":
/// `write_query_error` refuses an undetermined error before writing anything.
pub(super) fn assert_undetermined_closes_without_packet(query_error: &SqlQueryError) {
    assert!(
        query_error.is_result_undetermined(),
        "an unknown commit verdict must close the connection"
    );
    assert_eq!(
        query_error.message,
        tidb_terror::ERR_RESULT_UNDETERMINED.message()
    );
}

/// The determinate error the front end writes as one protocol-41 ERR packet:
/// its code, `#HY000` and the message.
pub(super) fn assert_query_error_packet(
    query_error: &SqlQueryError,
    expected_code: u16,
    expected_message: &str,
) {
    assert!(
        !query_error.is_result_undetermined(),
        "a determinate error writes ERR"
    );
    assert_eq!(query_error.code, expected_code);
    assert_eq!(&query_error.state, b"HY000");
    assert!(
        query_error.message.contains(expected_message),
        "packet message {:?} did not contain {expected_message:?}",
        query_error.message
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
        // A group another TiDB created, as meta's ResourceGroups hash holds it.
        resource_groups: vec![tidb_model::ResourceGroupInfo {
            settings: Some(Box::new(tidb_model::ResourceGroupSettings {
                ru_rate: 100,
                ..tidb_model::ResourceGroupSettings::new()
            })),
            id: 90,
            name: CiString::new("batch"),
            state: tidb_model::SchemaState::PUBLIC,
        }],
        policies: Vec::new(),
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
    /// The session factories its connections came from, alive as long as
    /// the node: Go's domain owns the internal-session pool its restricted
    /// SQL draws from for its whole life.
    pub(super) factories: std::sync::Mutex<Vec<Arc<Domain>>>,
    /// The node's auto-id registry, which every connection's table
    /// allocates through.
    pub(super) auto_ids: Arc<tidb_table::table_from_meta::LocalTableAutoIds>,
}

impl MockNode {
    /// Keeps `factory` alive for the node's lifetime.
    pub(super) fn keep(&self, factory: &Arc<Domain>) {
        self.factories
            .lock()
            .expect("factories")
            .push(Arc::clone(factory));
    }
}

/// A detached copy of one registry's rows, since
/// [`PrivilegeRegistry::replace_from`] empties its source.
pub(super) fn clone_registry(source: &PrivilegeRegistry) -> PrivilegeRegistry {
    let image = tidb_privilege::load::cluster_image_from_registry(source);
    tidb_privilege::load::registry_from_cluster(&image).registry
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
        Self::over(loaded_catalog())
    }

    /// A node whose published catalog is `loaded`, as the store's meta
    /// holds it.
    pub(super) fn over(loaded: ClusterCatalog) -> Self {
        let catalog = Arc::new(SharedClusterCatalog::new(loaded));
        Self {
            cluster: Arc::new(MockCluster::default()),
            ddl: Arc::new(MockDdl::new(Arc::clone(&catalog))),
            accounts: Arc::new(MockAccountWriter::new()),
            sysvars: Arc::new(MockSysvarWriter::new()),
            catalog,
            factories: std::sync::Mutex::default(),
            auto_ids: Arc::default(),
        }
    }
}

/// One authenticated connection over a fresh mock node, plus the node the
/// test inspects.
pub(super) fn open_session() -> (Session, MockNode) {
    let node = MockNode::start();
    let session = open_session_on(&node);
    (session, node)
}

/// A second connection to the same mock node, which is what makes a racing
/// writer -- or a peer that must notice a DDL -- expressible in SQL rather
/// than in raw keys.
pub(super) fn open_session_on(node: &MockNode) -> Session {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    )
}

/// Opens the same production cluster factory with a caller-supplied login
/// verdict. Privilege tests use this to distinguish a normal low-privilege
/// identity from one admitted by skip-grant-table without bypassing the
/// factory that carries that verdict into `Session`.
pub(super) fn open_session_on_with_context(node: &MockNode, context: SessionContext) -> Session {
    open_session_on_with_context_and_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
        context,
    )
}

/// Opens a connection with an injected catalog writer, so error-path tests can
/// drive the same DDL route without constructing a real TiKV authority.
pub(super) fn open_session_on_with_ddl(node: &MockNode, ddl: Arc<dyn ClusterDdl>) -> Session {
    open_session_on_with_seams(
        node,
        ddl,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    )
}

pub(super) fn open_session_on_with_sysvars(
    node: &MockNode,
    sysvars: Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
) -> Session {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        sysvars,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    )
}

pub(super) fn open_session_on_with_analyze(
    node: &MockNode,
    analyze: Arc<dyn ClusterAnalyze>,
) -> Session {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        analyze,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    )
}

pub(super) fn open_session_on_with_stats_lock(
    node: &MockNode,
    stats_lock: Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
) -> Session {
    open_session_on_with_seams(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        stats_lock,
    )
}

fn open_session_on_with_seams(
    node: &MockNode,
    ddl: Arc<dyn ClusterDdl>,
    accounts: Arc<dyn ClusterAccountWriter>,
    sysvars: Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
    analyze: Arc<dyn ClusterAnalyze>,
    stats_lock: Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
) -> Session {
    let mut session = open_session_on_with_context_and_seams(
        node,
        ddl,
        accounts,
        sysvars,
        analyze,
        stats_lock,
        session_context(1),
    );
    // The catalog is loaded, not created here: `USE` is how a connection
    // reaches it, exactly as it does over the wire.
    session
        .node_conn()
        .execute_write("USE app")
        .expect("USE app");
    session
}

fn open_session_on_with_context_and_seams(
    node: &MockNode,
    ddl: Arc<dyn ClusterDdl>,
    accounts: Arc<dyn ClusterAccountWriter>,
    sysvars: Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
    analyze: Arc<dyn ClusterAnalyze>,
    stats_lock: Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    context: SessionContext,
) -> Session {
    open_session_on_with_context_and_seams_and_usage(
        node, ddl, accounts, sysvars, analyze, stats_lock, context,
    )
    .0
}

pub(super) fn open_session_on_with_usage(
    node: &MockNode,
) -> (Session, Arc<tidb_stats_handle_usage::StatsUsageHandle>) {
    let (mut session, usage) = open_session_on_with_context_and_seams_and_usage(
        node,
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::new(MockStatsLock) as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
        session_context(1),
    );
    session
        .node_conn()
        .execute_write("USE app")
        .expect("USE app");
    (session, usage)
}

fn open_session_on_with_context_and_seams_and_usage(
    node: &MockNode,
    ddl: Arc<dyn ClusterDdl>,
    accounts: Arc<dyn ClusterAccountWriter>,
    sysvars: Arc<dyn tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter>,
    analyze: Arc<dyn ClusterAnalyze>,
    stats_lock: Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
    context: SessionContext,
) -> (Session, Arc<tidb_stats_handle_usage::StatsUsageHandle>) {
    let cluster = Arc::clone(&node.cluster);
    let factory = Domain::new(
        Arc::new(MockTransactions(cluster)),
        ddl,
        accounts,
        sysvars,
        analyze,
        stats_lock,
        Arc::clone(&node.catalog),
        node.accounts.live.clone(),
        node.sysvars.live.clone(),
        Arc::new(
            SharedStats::new(tidb_stats_handle_syncload::stats_watch::StatsSnapshot::new())
                .expect("statistics cache"),
        ),
        Arc::clone(&node.auto_ids) as _,
        crate::domain::cluster_session_node::open_pool_session,
    )
    .shared();
    node.keep(&factory);
    let usage = Arc::clone(&factory.stats_usage);
    let session = factory
        .open_session(context)
        .expect("the cluster session opens");
    (session, usage)
}

pub(super) fn session_context(connection_id: u64) -> SessionContext {
    let users =
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store");
    let identity = users
        .authenticate_native("root", "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
        .expect("authenticated identity");
    session_context_with_identity(connection_id, identity)
}

pub(super) fn session_context_with_identity(
    connection_id: u64,
    identity: crate::domain::configured_user_store::AuthenticatedIdentity,
) -> SessionContext {
    let peer_addr: SocketAddr = "127.0.0.1:4000".parse().expect("peer address");
    SessionContext {
        connection_id,
        peer_addr: peer_addr.into(),
        identity,
        client_found_rows: false,
        tls_state: None,
        compression_algorithm: tidb_mysql::consts::CompressionNone,
        compression_level: 0,
        cancellation: ConnectionCancellation::default(),
        close: ConnectionClose::default(),
    }
}

pub(super) fn rows(session: &mut Session, sql: &str) -> Vec<Vec<Datum>> {
    let mut result = session.node_conn().execute(sql).expect("the query runs");
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
