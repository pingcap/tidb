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

use crate::domain::cluster_session_node::DomainTestSessions as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::domain::cluster_session_node::NodeConn;
use crate::domain::configured_user_store::ConfiguredUserStore;
use tidb_domain_core::node_config::{NodeConfig, NodeOptions, StatsLease};
use crate::domain::unistore_node::{
    unistore_cluster_session_stack_from, BootstrapImage, UnistoreClusterStack,
};
use crate::Session;

/// Go `mockstore.NewMockStore` + `session.BootstrapSession`: one embedded
/// store and the domain booted over it.
pub struct MockStoreDomain {
    stack: UnistoreClusterStack,
    /// The node's account table and the rest of its configured users.
    #[cfg(test)]
    users: Arc<ConfiguredUserStore>,
    /// Go testkit's `testKitIDGenerator`.
    next_connection_id: AtomicU64,
}

impl MockStoreDomain {
    /// The domain booted over this store.
    #[cfg(test)]
    pub(crate) fn domain(&self) -> &Arc<crate::domain::cluster_session_node::Domain> {
        &self.stack.factory
    }
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
        let config = testkit_config()?;
        let users = Arc::new(match accounts {
            Some(accounts) => ConfiguredUserStore::from_accounts(accounts),
            None => ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
                .map_err(|error| format!("configured user store: {error:?}"))?,
        });
        Self::boot(config, users, 0)
    }

    /// Go `CreateMockStore(t, mockstore.WithMockTiFlash(nodes))`: [`Self::create`]
    /// over a store whose PD also lists `nodes` TiFlash stores.
    pub fn create_with_mock_tiflash(nodes: usize) -> Result<Arc<Self>, String> {
        let users = Arc::new(
            ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
                .map_err(|error| format!("configured user store: {error:?}"))?,
        );
        Self::boot(testkit_config()?, users, nodes)
    }

    /// [`Self::create`] whose node authenticates and keeps its accounts in
    /// `users`, as a server boots over its configured user store: what a
    /// wire-protocol test serves its connections from.
    pub fn create_with_users(users: Arc<ConfiguredUserStore>) -> Result<Arc<Self>, String> {
        Self::boot(testkit_config()?, users, 0)
    }

    /// The session factory of this domain's node: Go's server, which opens
    /// each connection's session on the store.
    #[must_use]
    pub fn factory(&self) -> &Arc<crate::domain::cluster_session_node::Domain> {
        &self.stack.factory
    }

    fn boot(
        config: NodeConfig,
        users: Arc<ConfiguredUserStore>,
        mock_tiflash: usize,
    ) -> Result<Arc<Self>, String> {
        let image = bootstrap_image(&config.cluster_security)?;
        let stack =
            unistore_cluster_session_stack_from(&config, &users, None, None, Some(&image), mock_tiflash)
            .map_err(|error| format!("unistore stack: {error:?}"))?;
        let domain = Arc::new(Self {
            stack,
            #[cfg(test)]
            users: Arc::clone(&users),
            next_connection_id: AtomicU64::new(1),
        });
        remember_accounts_domain(&domain);
        Ok(domain)
    }

    /// Go `testkit.NewSession`: a fresh session over this domain.
    pub fn new_session(self: &Arc<Self>) -> Result<Session, String> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let mut session = self
            .stack
            .factory
            .create_session_4_test(connection_id)
            .map_err(|error| format!("open session {connection_id}: {}", error.message))?;
        session.node.as_mut().expect("a session on the store").store = Some(Arc::clone(self));
        remember_catalog_domain(&session.shared_catalog(), self);
        Ok(session)
    }
}

/// The node configuration Go's testkit boots its store with: a 500ms schema
/// lease (`vardef.SetSchemaLease(500 * time.Millisecond)`) and a negative
/// stats lease (`session.DisableStats4Test()`). With it Go loads every
/// histogram an ANALYZE or `h.Update` reads (`columnStatsFromStorage`'s
/// `lease > 0` condition), and no background worker reloads statistics.
fn testkit_config() -> Result<NodeConfig, String> {
    let mut config = NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
            config.lease = "500ms".to_owned();
        },
        NodeOptions {
            // A requirement of the account source only: the node's user
            // store is the one the bootstrap seeds.
            auth_file: Some("/dev/null".to_owned()),
            ..NodeOptions::default()
        },
    )
    .map_err(|error| format!("node config: {error:?}"))?;
    config.stats_lease = StatsLease::Disabled;
    Ok(config)
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
    domains.push((Arc::downgrade(catalog.inner()), Arc::downgrade(domain)));
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
#[cfg(test)]
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

/// Go `domain.GetDomain(sctx)`: the domain `session` is on, if it is a
/// mock-store session.
#[must_use]
pub fn domain_of(session: &Session) -> Option<Arc<MockStoreDomain>> {
    domain_of_catalog(&session.shared_catalog())
}

/// The domain a node session's catalog belongs to, if `catalog` is one.
pub(crate) fn domain_of_catalog(catalog: &crate::SharedCatalog) -> Option<Arc<MockStoreDomain>> {
    let domains = CATALOG_DOMAINS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    domains.iter().find_map(|(known, domain)| {
        let known = known.upgrade()?;
        Arc::ptr_eq(&known, catalog.inner())
            .then(|| domain.upgrade())
            .flatten()
    })
}

/// The testkit's view of a session's store half: the mock store it keeps.
impl NodeConn {
    fn mock_store(&self) -> &Arc<MockStoreDomain> {
        self.store.as_ref().expect("a session on a mock store")
    }
    /// event (its bit in `mysql.tidb_ddl_notifier.processed_by_flag`).
    pub(crate) fn handle_ddl_events(&self) -> Result<(), String> {
        let notifier = self
            .mock_store()
            .stack
            .factory
            .owned_ddl_notifier_for_test();
        let stats_bit = 1_u64 << tidb_ddl_notifier::STATS_META_HANDLER_ID.0;
        let pending_sql = format!(
            "SELECT count(*) FROM mysql.tidb_ddl_notifier WHERE processed_by_flag & {stats_bit} = 0"
        );
        let mut probe = self.mock_store().new_session()?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let result = loop {
            let pending = match probe.run(&pending_sql) {
                Ok(crate::StmtResult::Rows(rows)) => {
                    rows.first().and_then(|row| row.first()).cloned()
                }
                Ok(other) => break Err(format!("pending events read {other:?}")),
                Err(error) => break Err(format!("pending events read: {error:?}")),
            };
            if pending == Some(tidb_datatype::Datum::Int(0)) {
                break Ok(());
            }
            if std::time::Instant::now() >= deadline {
                break Err(format!(
                    "DDL notifier left statistics events pending: {pending:?}"
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        notifier.stop();
        result
    }

    /// columns its sessions recorded into `mysql.column_stats_usage`.
    #[cfg(test)]
    pub(crate) fn dump_col_stats_usage(&self) -> Result<(), String> {
        self.mock_store()
            .stack
            .factory
            .dump_col_stats_usage_to_kv("default")
    }

    /// removes the observer).
    #[cfg(test)]
    pub(crate) fn set_store_send_hook(&self, hook: Option<tidb_unistore::rpc::SendHook>) {
        self.mock_store().stack.rpc.set_send_hook(hook);
    }

    /// commits the delete of `key` straight into the embedded store.
    #[cfg(test)]
    pub(crate) fn delete_key_for_test(&self, key: &[u8]) -> Result<(), String> {
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let start = self.fresh_timestamp_for_test()?;
        let commit = self.fresh_timestamp_for_test()?;
        let handler = self.mock_store().stack.rpc.handler();
        let mut handler = handler
            .lock()
            .map_err(|_| "kv handler poisoned".to_owned())?;
        handler
            .store
            .prewrite(&tidb_unistore::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Del as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.to_vec(),
                start_version: start,
                lock_ttl: 3000,
                ..tidb_unistore::mvcc_store::PrewriteReq::default()
            })
            .map_err(|error| format!("prewrite: {error:?}"))?;
        handler
            .store
            .commit(&[key.to_vec()], start, commit)
            .map_err(|error| format!("commit: {error:?}"))
    }

}
