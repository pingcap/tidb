// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `testkit.CreateMockStoreAndDomain` (`pkg/testkit/mockstore.go`): an
//! embedded unistore booted through the node's own domain, and the sessions
//! Go's testkit opens on it with `session.CreateSession4Test`.
//!
//! A [`Session`] built over this domain runs each statement through its
//! node connection, so DDL is a persisted job, a write commits through the
//! store and a read goes through the coprocessor, exactly as Go's unit tests
//! run them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::domain::cluster_session_node::ClusterServerSession;
use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::unistore_node::{
    unistore_cluster_session_stack_from, BootstrapImage, UnistoreClusterStack,
};
use crate::{Session, StmtOutput};

/// Go `mockstore.NewMockStore` + `session.BootstrapSession`: one embedded
/// store and the domain booted over it.
pub struct MockStoreDomain {
    stack: UnistoreClusterStack,
    /// The node's account table and the rest of its configured users.
    users: Arc<ConfiguredUserStore>,
    /// Go testkit's `testKitIDGenerator`.
    next_connection_id: AtomicU64,
}

impl MockStoreDomain {
    /// Boots a fresh store with Go testkit's settings: a 500ms schema lease
    /// (`vardef.SetSchemaLease(500 * time.Millisecond)`) and the bootstrap's
    /// passwordless `root`@`%`.
    pub fn create() -> Result<Arc<Self>, String> {
        Self::create_with_accounts(None)
    }

    /// [`Self::create`] whose node keeps its accounts in `accounts`: the
    /// store bootstraps with that table's rows and every account change on
    /// it lands back in that table.
    pub fn create_with_accounts(
        accounts: Option<crate::privilege::PrivilegeRegistry>,
    ) -> Result<Arc<Self>, String> {
        let config = crate::domain::node_config::NodeConfig::parse([
            "tidb-server",
            "--store",
            "unistore",
            "--port",
            "0",
            "--lease-ms",
            "500",
            // Parse-time requirement only: the user store below is the one
            // the bootstrap seeds.
            "--auth-file",
            "/dev/null",
        ])
        .map_err(|error| format!("node config: {error:?}"))?;
        let users = Arc::new(match accounts {
            Some(accounts) => ConfiguredUserStore::from_accounts(accounts),
            None => ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
                .map_err(|error| format!("configured user store: {error:?}"))?,
        });
        let image = bootstrap_image(&config.cluster_security)?;
        let stack = unistore_cluster_session_stack_from(&config, &users, None, None, Some(&image))
            .map_err(|error| format!("unistore stack: {error:?}"))?;
        let domain = Arc::new(Self {
            stack,
            users: Arc::clone(&users),
            next_connection_id: AtomicU64::new(1),
        });
        remember_accounts_domain(&domain);
        Ok(domain)
    }

    /// Go `testkit.NewSession`: a fresh session over this domain.
    pub fn new_session(self: &Arc<Self>) -> Result<Session, String> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let mut connection = self
            .stack
            .factory
            .create_session_4_test(connection_id)
            .map_err(|error| format!("open session {connection_id}: {}", error.message))?;
        let mut session = Session::unbootstrapped(crate::SharedCatalog::default());
        std::mem::swap(&mut session, connection.driver_session_mut());
        remember_catalog_domain(&session.shared_catalog(), self);
        session.node = Some(Box::new(MockStoreConnection {
            connection,
            domain: Arc::clone(self),
        }));
        Ok(session)
    }
}

/// Go testkit's `tryMakeImage` + the image's use: the bootstrapped store
/// every mock store starts from, made once and shared.
///
/// The image lives in a file beside the test binary, so every test process
/// of one build starts from it (nextest runs each test in its own process);
/// its name carries the binary's identity and every input the bootstrap
/// reads from its environment, so a rebuild or a different environment
/// makes a fresh one. Go writes its image to a fixed `/tmp` path and leaves
/// using it disabled ("we figure out why the test image does not accelerate
/// out unit tests", `mockstore.go`); here the bootstrap is the larger part
/// of a store's boot, so the image is used.
fn bootstrap_image(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<Arc<BootstrapImage>, String> {
    static IMAGE: OnceLock<Arc<BootstrapImage>> = OnceLock::new();
    if let Some(image) = IMAGE.get() {
        return Ok(Arc::clone(image));
    }
    let path = bootstrap_image_path();
    let image = match path
        .as_ref()
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|bytes| BootstrapImage::from_bytes(&bytes).ok())
    {
        Some(image) => image,
        None => {
            let image = BootstrapImage::make(security)?;
            if let Some(path) = &path {
                // Written whole and renamed into place, so a concurrent
                // reader sees either no image or a complete one.
                let staging = path.with_extension(format!("tmp{}", std::process::id()));
                if std::fs::write(&staging, image.to_bytes()).is_ok() {
                    let _ = std::fs::rename(&staging, path);
                }
            }
            image
        }
    };
    Ok(Arc::clone(IMAGE.get_or_init(|| Arc::new(image))))
}

/// `<test binary>.<identity>.bootstrap-image`, or `None` when the binary
/// cannot be named.
fn bootstrap_image_path() -> Option<std::path::PathBuf> {
    use std::hash::{Hash, Hasher};
    let exe = std::env::current_exe().ok()?;
    let metadata = std::fs::metadata(&exe).ok()?;
    let mut identity = std::collections::hash_map::DefaultHasher::new();
    metadata.len().hash(&mut identity);
    metadata.modified().ok()?.hash(&mut identity);
    // What the bootstrap reads from its environment: the system time zone
    // it persists and the layout a debug build may be told to use.
    tidb_util::timeutil::infer_system_tz().hash(&mut identity);
    std::env::var("TIDB_RS_TEST_BOOTSTRAP_LAYOUT")
        .ok()
        .hash(&mut identity);
    let name = format!(
        "{}.{:016x}.bootstrap-image",
        exe.file_name()?.to_string_lossy(),
        identity.finish()
    );
    Some(exe.with_file_name(name))
}

/// Which domain each node session's catalog belongs to.
///
/// A test names "another connection to the same store" by the catalog it
/// shares (`Session::with_catalog(first.shared_catalog())`); Go's testkit
/// names it by the store (`testkit.NewTestKit(t, store)`). This table turns
/// the one into the other.
static CATALOG_DOMAINS: Mutex<Vec<(Weak<Mutex<crate::Catalog>>, Weak<MockStoreDomain>)>> =
    Mutex::new(Vec::new());

fn remember_catalog_domain(catalog: &crate::SharedCatalog, domain: &Arc<MockStoreDomain>) {
    let mut domains = CATALOG_DOMAINS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    domains.retain(|(catalog, domain)| catalog.strong_count() > 0 && domain.strong_count() > 0);
    domains.push((Arc::downgrade(catalog), Arc::downgrade(domain)));
}

/// Which domain keeps its accounts in each account table.
static ACCOUNT_DOMAINS: Mutex<Vec<Weak<MockStoreDomain>>> = Mutex::new(Vec::new());

fn remember_accounts_domain(domain: &Arc<MockStoreDomain>) {
    let mut domains = ACCOUNT_DOMAINS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    domains.retain(|domain| domain.strong_count() > 0);
    domains.push(Arc::downgrade(domain));
}

/// The live domain whose node keeps its accounts in `accounts`.
pub(crate) fn domain_of_accounts(
    accounts: &crate::privilege::PrivilegeRegistry,
) -> Option<Arc<MockStoreDomain>> {
    let domains = ACCOUNT_DOMAINS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    domains
        .iter()
        .filter_map(Weak::upgrade)
        .find(|domain| domain.users.accounts().is_same_table(accounts))
}

/// The domain a node session's catalog belongs to, if `catalog` is one.
pub(crate) fn domain_of_catalog(catalog: &crate::SharedCatalog) -> Option<Arc<MockStoreDomain>> {
    let domains = CATALOG_DOMAINS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    domains.iter().find_map(|(known, domain)| {
        let known = known.upgrade()?;
        Arc::ptr_eq(&known, catalog)
            .then(|| domain.upgrade())
            .flatten()
    })
}

/// A session's node connection while the session itself is outside it.
///
/// Between statements the [`Session`] holds its own state and this holds a
/// placeholder in its place; a statement swaps the session in, runs through
/// the connection, and swaps it back out.
pub struct MockStoreConnection {
    connection: ClusterServerSession,
    domain: Arc<MockStoreDomain>,
}

impl MockStoreConnection {
    /// Runs `sql` through the node connection with `session` inside it.
    pub(crate) fn run(
        &mut self,
        session: &mut Session,
        sql: &str,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        std::mem::swap(session, self.connection.driver_session_mut());
        let result = self.connection.run_statement_output(sql);
        std::mem::swap(session, self.connection.driver_session_mut());
        result
    }

    /// Applies a transaction-control statement through the node connection
    /// with `session` inside it; see
    /// [`ClusterServerSession::run_transaction_control`].
    pub(crate) fn control_transaction(
        &mut self,
        session: &mut Session,
        sql: &str,
    ) -> Result<Option<bool>, tidb_executor::DriverError> {
        std::mem::swap(session, self.connection.driver_session_mut());
        let result = self.connection.run_transaction_control(sql);
        std::mem::swap(session, self.connection.driver_session_mut());
        result
    }

    /// Runs an already-parsed `stmt` through the node connection with
    /// `session` inside it; see
    /// [`ClusterServerSession::run_parsed_statement_output`].
    pub(crate) fn run_parsed(
        &mut self,
        session: &mut Session,
        sql: &str,
        stmt: tidb_ast::Stmt,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        std::mem::swap(session, self.connection.driver_session_mut());
        let result = self.connection.run_parsed_statement_output(sql, Some(stmt));
        std::mem::swap(session, self.connection.driver_session_mut());
        result
    }

    /// Runs `body` over `session` inside the connection with a committed
    /// snapshot bound; see [`ClusterServerSession::with_read_snapshot`].
    pub(crate) fn with_read_snapshot<R>(
        &mut self,
        session: &mut Session,
        body: impl FnOnce(&mut Session) -> R,
    ) -> Result<R, tidb_executor::DriverError> {
        std::mem::swap(session, self.connection.driver_session_mut());
        let result = self.connection.with_read_snapshot(body);
        std::mem::swap(session, self.connection.driver_session_mut());
        result
    }

    /// Binds a committed snapshot for a statement opened directly on
    /// `session`; see [`ClusterServerSession::bind_open_statement_snapshot`].
    pub(crate) fn bind_open_statement_snapshot(
        &mut self,
        session: &mut Session,
    ) -> Result<(), tidb_executor::DriverError> {
        std::mem::swap(session, self.connection.driver_session_mut());
        let result = self.connection.bind_open_statement_snapshot();
        std::mem::swap(session, self.connection.driver_session_mut());
        result
    }

    /// Puts `session` back inside the connection, which then closes with it
    /// (Go `session.Close`: roll back, release table locks and pins).
    pub(crate) fn close(mut self, session: &mut Session) {
        std::mem::swap(session, self.connection.driver_session_mut());
        drop(self.connection);
    }

    /// The domain this connection is on.
    #[must_use]
    pub fn domain(&self) -> &Arc<MockStoreDomain> {
        &self.domain
    }
}
