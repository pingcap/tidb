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

//! Shared cluster bootstrap, reload and ordered shutdown ownership.

use std::sync::Arc;
use std::time::Duration;

use crate::GlobalSysvars;

use tidb_cluster::catalog_watch::{CatalogReloadError, CatalogReloader, SharedCatalog};
use tidb_cluster::cluster_catalog::ClusterCatalog;
use tidb_cluster::real_tikv_catalog::reload_catalog_from_cluster;
use tidb_cluster::real_tikv_read::{
    ProductionReadProcessAuthority, ReadProcessShutdownError, RealOptimisticTransactionOpener,
};
use tidb_exec::real_tikv_stats::{
    load_stats_snapshot_and_loader, update_stats_cache_from_cluster, InitialStatsLoad,
};
use tidb_exec::stats_watch::{
    AsyncStatsLoader, SharedStats, StatsReloadError, StatsReloadReadResult, StatsReloader,
};
use tidb_pd_client::{EtcdClient, EtcdWatcher, PRIVILEGE_UPDATE_KEY, SYSVAR_UPDATE_KEY};
use tidb_txnkv::transaction::StoreClient;

use crate::domain::cluster_privileges::PrivilegeReloader;
use crate::domain::configured_user_store::{ConfiguredUserStore, ConfiguredUserStoreError};
use crate::domain::node_config::NodeConfig;
use crate::domain::session_api::SqlQueryError;

mod schema_following;

pub use schema_following::{
    connect_etcd_client, spawn_catalog_reloader, spawn_node_stats, spawn_privilege_watch,
    spawn_sysvar_watch,
};
pub use tidb_domain_core::session_time_zone::RealTiKvSessionTimeZone;

const PRODUCTION_CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);
pub fn apply_account_policies(
    config: &NodeConfig,
    users: ConfiguredUserStore,
) -> ConfiguredUserStore {
    users
        .accounts()
        .set_sandbox_mode_enabled(!config.disconnect_on_expired_password);
    users.with_skip_grant_table(config.skip_grant_table)
}

/// Opens the configured file-backed account source, except in TiDB's
/// skip-grant-table recovery mode where privilege storage must not be read at
/// all. The empty registry remains writable for account/role statements that
/// run after startup.
/// Go `infosync.ServerInfo.StartTimestamp`: pinned the first time startup
/// reaches the account store (every run path does, before any listener), so
/// every session's `Uptime` measures from process bootstrap.
pub fn server_start_unix_timestamp() -> i64 {
    static START: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64)
    })
}

/// The node's account store over `global_vars`, the process's sysvar
/// registry, which the server's main has already filled (Go `setGlobalVars`).
pub fn configured_account_store(
    config: &NodeConfig,
    global_vars: GlobalSysvars,
) -> Result<ConfiguredUserStore, RunConfiguredNodeError> {
    let users = if config.skip_grant_table {
        ConfiguredUserStore::empty_for_skip_grant_table()
    } else {
        ConfiguredUserStore::load(&config.auth_file).map_err(RunConfiguredNodeError::Auth)?
    };
    let users = apply_account_policies(config, users.with_global_vars(global_vars));
    // The start timestamp pins before any listener starts, as Go's does.
    server_start_unix_timestamp();
    Ok(users)
}

/// Builds this node's live account table from whichever source the
/// command line named, plus the live-refresh reload thread when
/// `--load-privileges` is set.
///
/// `--load-privileges` reads the cluster's own `mysql.*` through the
/// already-connected authority, so the accounts this node admits are exactly
/// the ones a Go TiDB wrote there. It is refused against a keyspace no TiDB
/// ever bootstrapped: an empty account table would accept nobody, and
/// reporting that as a successful load would hide the real cause.
///
/// The returned [`PrivilegeReloader`], when present, must be kept alive for
/// the node's whole run: dropping it stops the reload thread, so a caller
/// that let it go out of scope early would silently fall back to the
/// one-shot startup snapshot.
pub fn node_accounts(
    config: &NodeConfig,
    authority: &ProductionReadProcessAuthority,
    global_vars: GlobalSysvars,
) -> Result<(Arc<ConfiguredUserStore>, Option<PrivilegeReloader>), RunConfiguredNodeError> {
    if config.skip_grant_table || !config.load_privileges {
        let users = configured_account_store(config, global_vars)?;
        return Ok((Arc::new(users), None));
    }
    let accounts = tidb_exec::real_tikv_privileges::load_accounts_from_cluster(
        &authority.transaction_opener(),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    if !accounts.bootstrap.already_bootstrapped() {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            format!(
                "--load-privileges needs a cluster whose mysql.* a TiDB bootstrapped, but {}",
                accounts.bootstrap
            ),
        )));
    }
    // Go's privilege cache logs nothing on a successful load.
    let loaded = crate::domain::cluster_privileges::registry_from_cluster(&accounts.privileges);
    let users = ConfiguredUserStore::from_accounts(loaded.registry).with_global_vars(global_vars);
    // Seed the same startup snapshot's global variables now; `run_bound_*`
    // immediately replaces this with a fresh complete image and starts the
    // ongoing sysvar reloader before bind. Keeping the first image here makes
    // this account load internally coherent even if a caller inspects it
    // before handing it to the listener lifecycle.
    users.global_vars().load_from_cluster(accounts.sysvars);
    let users = apply_account_policies(config, users);
    let users = Arc::new(users);
    // Ticks at Go `LoadPrivilegeLoop`'s fallback cadence
    // (pkg/domain/domain.go:1394-1396): ten minutes when the etcd watch is up
    // (the watch makes a peer's GRANT prompt), five when it is not. This used
    // to tick every schema_lease/2 — one second at the benchmark lease —
    // which re-read every account/grant table and rebuilt the whole registry
    // hundreds of times more often than Go does.
    let reloader = PrivilegeReloader::spawn(
        users.accounts(),
        authority.transaction_opener(),
        privilege_reload_interval(!config.pd_endpoints.is_empty()),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    Ok((users, Some(reloader)))
}

#[cfg(test)]
fn load_cluster_sysvars_with_read(
    users: &ConfiguredUserStore,
    read: impl FnOnce() -> Result<Vec<(String, String)>, String>,
) -> Result<(), RunConfiguredNodeError> {
    let sysvars =
        read().map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let fresh = crate::GlobalSysvars::from_cluster_rows(sysvars);
    users.global_vars().replace_from(&fresh);
    Ok(())
}

pub fn load_cluster_startup_variables<S>(
    users: &ConfiguredUserStore,
    opener: &RealOptimisticTransactionOpener<S>,
) -> Result<(), RunConfiguredNodeError>
where
    S: StoreClient,
{
    let variables = tidb_exec::real_tikv_privileges::load_startup_variables_from_cluster(
        opener,
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    initialize_cluster_system_tz_with_read(|| Ok(variables.system_tz))?;
    // Go's InitMDLVariable publishes the persisted metadata-lock setting to
    // the process before Domain starts its schema-ack loop.  Keep this
    // explicit at the same startup boundary: the SQL registry may report the
    // default ON while the syncer would otherwise still observe its atomic
    // flag's initial false value and use the MDL-off etcd key layout.
    publish_startup_mdl_state(&variables.sysvars);
    let fresh = crate::GlobalSysvars::from_cluster_rows(variables.sysvars);
    users.global_vars().replace_from(&fresh);
    Ok(())
}

fn publish_startup_mdl_state(sysvars: &[(String, String)]) {
    let enabled = sysvars
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_ENABLE_MDL))
        .map_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_MDL, |(_, value)| {
            value.eq_ignore_ascii_case("ON") || value == "1"
        });
    tidb_vardef::set_enable_mdl(enabled);
}

fn initialize_cluster_system_tz_with_read(
    read: impl FnOnce() -> Result<String, String>,
) -> Result<(), RunConfiguredNodeError> {
    let system_tz =
        read().map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    tidb_util::timeutil::set_system_tz(&system_tz);
    Ok(())
}

#[cfg(test)]
fn prepare_cluster_sysvar_runtime_with_reads(
    config: &NodeConfig,
    users: &ConfiguredUserStore,
    startup_read: impl FnOnce() -> Result<Vec<(String, String)>, String>,
    reload_read: crate::domain::cluster_sysvar_seam::SysvarReloadRead,
) -> Result<Option<crate::domain::cluster_sysvar_seam::SysvarReloader>, RunConfiguredNodeError> {
    load_cluster_sysvars_with_read(users, startup_read)?;
    spawn_cluster_sysvar_reloader_with_read(config, users, reload_read)
}

#[cfg(test)]
fn spawn_cluster_sysvar_reloader_with_read(
    config: &NodeConfig,
    users: &ConfiguredUserStore,
    read: crate::domain::cluster_sysvar_seam::SysvarReloadRead,
) -> Result<Option<crate::domain::cluster_sysvar_seam::SysvarReloader>, RunConfiguredNodeError> {
    crate::domain::cluster_sysvar_seam::SysvarReloader::spawn_with_read(
        users.global_vars(),
        sysvar_reload_interval(config.schema_lease),
        crate::domain::cluster_sysvar_seam::SysvarPublicationFence::default(),
        read,
    )
    .map(Some)
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))
}

pub fn sysvar_reload_interval(_schema_lease: Duration) -> Duration {
    // Go `LoadSysVarCacheLoop` (pkg/domain/domain.go:1473): the fallback tick
    // is a FIXED 30 seconds, with or without etcd — the watch makes a peer's
    // `SET GLOBAL` prompt, and nothing scales this interval by the schema
    // lease. A short lease used to shrink this to one second here, which made
    // every pass re-read and JSON-decode the whole `mysql.global_variables`
    // image thirty times more often than Go does.
    let _ = _schema_lease;
    Duration::from_secs(30)
}

/// Go `LoadPrivilegeLoop`'s fallback cadence (pkg/domain/domain.go:1394-1396):
/// ten minutes when the etcd watch is up, five when it is not. The watch —
/// not the tick — is what makes an account change prompt; the tick is only
/// the safety net for a lost watch channel.
pub fn privilege_reload_interval(has_etcd_watch: bool) -> Duration {
    if has_etcd_watch {
        Duration::from_secs(10 * 60)
    } else {
        Duration::from_secs(5 * 60)
    }
}

/// Go `Server.Run`'s line when the listeners cannot be set up.
pub fn emit_connections_startup_failure(error: &impl std::fmt::Display) {
    tidb_log::error(
        "failed to create the server",
        &[crate::domain::zap_error(error), crate::domain::zap_stack()],
    );
}

/// Fallible unique process owner consumed after every server run path.
pub trait ProcessReadAuthority {
    /// Stops RegionCache, TiKV transport, and PD in dependency order.
    fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError>;
}

impl ProcessReadAuthority for ProductionReadProcessAuthority {
    fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError> {
        self.shutdown()
    }
}

/// Runs one node closure, drops every opener, then always shuts its authority.
pub fn run_with_process_shutdown<F, A, R>(
    factory: F,
    authority: A,
    run: R,
) -> Result<(), RunConfiguredNodeError>
where
    A: ProcessReadAuthority,
    R: FnOnce(F) -> Result<(), RunConfiguredNodeError>,
{
    // Go logs no final line once the server stopped.
    run_with_process_shutdown_and_final(factory, authority, run, || {})
}

fn run_with_process_shutdown_and_final<F, A, R, S>(
    factory: F,
    mut authority: A,
    run: R,
    on_success: S,
) -> Result<(), RunConfiguredNodeError>
where
    A: ProcessReadAuthority,
    R: FnOnce(F) -> Result<(), RunConfiguredNodeError>,
    S: FnOnce(),
{
    let run_result = run(factory);
    let shutdown_result = authority.shutdown_process();
    emit_process_shutdown_events(&shutdown_result);
    match (run_result, shutdown_result) {
        (Ok(()), Ok(())) => {
            on_success();
            Ok(())
        }
        (Err(run), Ok(())) => Err(run),
        (Ok(()), Err(authority)) => Err(RunConfiguredNodeError::Authority(authority)),
        (Err(run), Err(authority)) => Err(RunConfiguredNodeError::Combined {
            run: Box::new(run),
            authority,
        }),
    }
}

fn emit_process_shutdown_events(result: &Result<(), ReadProcessShutdownError>) {
    // Go `closeDDLOwnerMgrDomainAndStorage`: `terror.Log(storage.Close())`
    // logs a failed close only.
    if let Err(ReadProcessShutdownError::StoreClose(message)) = result {
        tidb_log::error(
            "encountered error",
            &[crate::domain::zap_error(message), crate::domain::zap_stack()],
        );
    }
}

/// Startup/runtime failure from the fully composed node.
#[derive(Debug)]
pub enum RunConfiguredNodeError {
    /// The required immutable account catalog was rejected.
    Auth(ConfiguredUserStoreError),
    /// Spill storage could not be leased or admitted before listener startup.
    Spill(tidb_util::spill_storage::SpillStorageOpenError),
    /// The process SIGINT/SIGTERM handler could not be installed.
    Signal(ctrlc::Error),
    /// Production query-authority construction failed.
    Engine(SqlQueryError),
    /// Listener or connection runtime failed (the server's `SqlNodeError`).
    Node(Box<dyn std::error::Error + Send + Sync>),
    /// Process authority shutdown failed after the node drained.
    Authority(ReadProcessShutdownError),
    /// Both node execution and process authority shutdown failed.
    Combined {
        /// Node startup, admission, or drain failure.
        run: Box<RunConfiguredNodeError>,
        /// Ordered process authority shutdown failure.
        authority: ReadProcessShutdownError,
    },
    /// The loggers could not be set up (Go `setupLog`) or flushed at exit
    /// (Go `syncLog`).
    Log(String),
}

impl std::fmt::Display for RunConfiguredNodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auth(error) => write!(formatter, "cannot load authentication catalog: {error}"),
            Self::Spill(error) => write!(formatter, "cannot initialize spill storage: {error}"),
            Self::Signal(error) => write!(formatter, "cannot install shutdown handler: {error}"),
            Self::Engine(error) => {
                write!(
                    formatter,
                    "cannot construct read authority: {}",
                    error.message
                )
            }
            Self::Node(error) => error.fmt(formatter),
            Self::Authority(error) => write!(formatter, "read authority shutdown failed: {error}"),
            Self::Combined { run, authority } => write!(
                formatter,
                "node failed: {run}; read authority shutdown also failed: {authority}"
            ),
            Self::Log(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for RunConfiguredNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auth(error) => Some(error),
            Self::Spill(error) => Some(error),
            Self::Signal(error) => Some(error),
            Self::Node(error) => Some(error.as_ref()),
            Self::Authority(error)
            | Self::Combined {
                authority: error, ..
            } => Some(error),
            Self::Engine(_) | Self::Log(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::node_config::NodeOptions;
    use crate::domain::secure_transport::TransportKind;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    
    

    #[test]
    fn configured_account_policy_makes_skip_grant_table_a_real_login_authority() {
        if crate::domain::isolate_process_globals() {
            return;
        }
        let mut config = NodeConfig::with_defaults(
            |config| config.path = "127.0.0.1:2379".to_owned(),
            NodeOptions {
                auth_file: Some("/tmp/users.tsv".to_owned()),
                ..NodeOptions::default()
            },
        )
        .expect("base config");
        config.skip_grant_table = true;
        let users = configured_account_store(&config, GlobalSysvars::new())
            .expect("skip-grant-table does not read the nonexistent auth file");
        assert_eq!(
            users.match_identity_plugin("no-row", "127.0.0.1").as_deref(),
            Some("mysql_native_password")
        );
        let identity = users
            .authenticate_native("no-row", "127.0.0.1", &[7; 20], b"arbitrary")
            .expect("the production startup policy reaches login");
        assert_eq!(identity.username(), "no-row");
        assert_eq!(identity.host(), "127.0.0.1");
        assert!(identity.privilege_bypassed());
    }

    #[test]
    fn cluster_startup_installs_the_bootstrap_system_timezone() {
        // SetSystemTZ runs once per process, including in earlier server tests.
        if crate::domain::isolate_process_globals() {
            return;
        }
        initialize_cluster_system_tz_with_read(|| Ok("Asia/Shanghai".to_owned()))
            .expect("the persisted timezone installs");
        assert_eq!(
            tidb_util::timeutil::get_system_tz().expect("system_tz is initialized"),
            "Asia/Shanghai"
        );
    }

    #[test]
    fn startup_mdl_state_matches_the_persisted_global_variable() {
        if crate::domain::isolate_process_globals() {
            return;
        }

        publish_startup_mdl_state(&[(
            tidb_vardef::tidb_vars::TIDB_ENABLE_MDL.to_owned(),
            "OFF".to_owned(),
        )]);
        assert!(!tidb_vardef::is_mdl_enabled(false));

        publish_startup_mdl_state(&[(
            tidb_vardef::tidb_vars::TIDB_ENABLE_MDL.to_owned(),
            "ON".to_owned(),
        )]);
        assert!(tidb_vardef::is_mdl_enabled(false));

        // An absent row has Go's DefTiDBEnableMDL default.
        publish_startup_mdl_state(&[]);
        assert!(tidb_vardef::is_mdl_enabled(false));
    }

    #[test]
    fn skip_grant_sysvar_runtime_is_fail_closed_at_first_login_and_reloads_without_accounts() {
        if crate::domain::isolate_process_globals() {
            return;
        }
        let mut config = NodeConfig::with_defaults(
            |config| config.path = "127.0.0.1:2379".to_owned(),
            NodeOptions {
                auth_file: Some("/tmp/users.tsv".to_owned()),
                ..NodeOptions::default()
            },
        )
        .expect("base config");
        config.skip_grant_table = true;
        // The test nudges the worker; a long tick proves the observed update
        // came from that runtime path rather than an incidental timer pass.
        config.schema_lease = Duration::from_secs(120);
        // Go's fixed fallback: the lease does not scale it.
        assert_eq!(
            sysvar_reload_interval(config.schema_lease),
            Duration::from_secs(30)
        );
        assert_eq!(
            sysvar_reload_interval(Duration::from_secs(10)),
            Duration::from_secs(30)
        );
        let users = configured_account_store(&config, GlobalSysvars::new())
            .expect("skip-grant startup creates no file-backed accounts");

        load_cluster_sysvars_with_read(&users, || {
            Ok(vec![(
                "require_secure_transport".to_owned(),
                "ON".to_owned(),
            )])
        })
        .expect("the persisted startup image loads before admission");
        assert!(
            users.is_empty(),
            "sysvar loading must not fabricate accounts"
        );
        assert_eq!(
            users.authenticate(
                "recovery",
                "192.0.2.7",
                &[7; 20],
                b"ignored",
                TransportKind::PlainTcp,
            ),
            Err(crate::domain::configured_user_store::AuthenticationFailure::SecureTransportRequired),
            "the very first admission must see persisted require_secure_transport=ON"
        );

        let reads = Arc::new(AtomicUsize::new(0));
        let reload_reads = Arc::clone(&reads);
        let mut reloader = spawn_cluster_sysvar_reloader_with_read(
            &config,
            &users,
            Box::new(move || {
                reload_reads.fetch_add(1, Ordering::SeqCst);
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "OFF".to_owned(),
                )])
            }),
        )
        .expect("the production runtime wiring starts")
        .expect("skip-grant mode owns a sysvar reloader");
        reloader.waker().nudge();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while reloader.stats().reloads == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        users
            .authenticate(
                "recovery",
                "192.0.2.7",
                &[7; 20],
                b"ignored",
                TransportKind::PlainTcp,
            )
            .expect("runtime OFF makes a later plaintext admission legal");
        assert!(users.is_empty(), "the reload remains sysvar-only");
        reloader.shutdown().expect("the worker stops cleanly");
    }

    #[test]
    fn ordinary_cluster_sysvars_are_also_installed_before_login_and_reloaded() {
        if crate::domain::isolate_process_globals() {
            return;
        }
        let mut config = NodeConfig::with_defaults(
            |config| config.path = "127.0.0.1:2379".to_owned(),
            NodeOptions {
                auth_file: Some("/tmp/users.tsv".to_owned()),
                ..NodeOptions::default()
            },
        )
        .expect("base config");
        config.schema_lease = Duration::from_secs(120);
        let users = ConfiguredUserStore::from_accounts(
            crate::privilege::PrivilegeRegistry::bootstrapped_from([]),
        );
        let reads = Arc::new(AtomicUsize::new(0));
        let reload_reads = Arc::clone(&reads);
        users
            .global_vars()
            .load_from_cluster([("require_secure_transport".to_owned(), "ON".to_owned())]);
        let mut reloader = prepare_cluster_sysvar_runtime_with_reads(
            &config,
            &users,
            || Ok(Vec::new()),
            Box::new(move || {
                reload_reads.fetch_add(1, Ordering::SeqCst);
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "ON".to_owned(),
                )])
            }),
        )
        .expect("ordinary cluster-backed nodes own the same sysvar runtime")
        .expect("the runtime always includes an ongoing reloader");
        assert_eq!(
            users
                .global_vars()
                .get("require_secure_transport")
                .as_deref(),
            Ok("OFF"),
            "a complete empty startup image removes a previously loaded ON row"
        );
        reloader.waker().nudge();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while reloader.stats().reloads == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            users
                .global_vars()
                .get("require_secure_transport")
                .as_deref(),
            Ok("ON")
        );
        assert_eq!(
            users.authenticate(
                "ordinary",
                "192.0.2.8",
                &[7; 20],
                b"ignored",
                TransportKind::PlainTcp,
            ),
            Err(crate::domain::configured_user_store::AuthenticationFailure::SecureTransportRequired),
            "the ongoing ordinary-node reload restores fail-closed admission"
        );
        reloader.shutdown().expect("the worker stops cleanly");
    }

    struct FactoryEvent(Arc<Mutex<Vec<&'static str>>>);

    impl Drop for FactoryEvent {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("factory_drop");
        }
    }

    struct AuthorityEvent {
        events: Arc<Mutex<Vec<&'static str>>>,
        result: Result<(), ReadProcessShutdownError>,
    }

    impl ProcessReadAuthority for AuthorityEvent {
        fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError> {
            self.events.lock().unwrap().push("authority_shutdown");
            self.result.clone()
        }
    }

    #[test]
    fn final_success_event_runs_only_after_factory_drop_and_authority_shutdown() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let final_events = Arc::clone(&events);
        run_with_process_shutdown_and_final(
            FactoryEvent(Arc::clone(&events)),
            AuthorityEvent {
                events: Arc::clone(&events),
                result: Ok(()),
            },
            |factory| {
                drop(factory);
                Ok(())
            },
            move || final_events.lock().unwrap().push("sql_node_stopped"),
        )
        .unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            ["factory_drop", "authority_shutdown", "sql_node_stopped"]
        );
    }

    #[test]
    fn authority_failure_suppresses_final_success_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let final_events = Arc::clone(&events);
        let result = run_with_process_shutdown_and_final(
            FactoryEvent(Arc::clone(&events)),
            AuthorityEvent {
                events: Arc::clone(&events),
                result: Err(ReadProcessShutdownError::StoreClose("close failed".to_owned())),
            },
            |factory| {
                drop(factory);
                Ok(())
            },
            move || final_events.lock().unwrap().push("sql_node_stopped"),
        );

        assert!(matches!(result, Err(RunConfiguredNodeError::Authority(_))));
        assert_eq!(
            *events.lock().unwrap(),
            ["factory_drop", "authority_shutdown"]
        );
    }
}
