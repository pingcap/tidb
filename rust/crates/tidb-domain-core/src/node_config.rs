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

//! Executable resources derived from the shared TiDB configuration owner.
//! TOML loading, validation and explicit Go CLI overrides (the server's
//! `cmd/tidb-server` half) precede this runtime projection; Rust-native
//! adapter arguments do not define another TOML schema.

use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use tidb_config::config_tree::Config as SourceConfig;
use tidb_pd_client::ClusterSecurity;
use tidb_spill_storage::{SpillEncryptionMethod, SpillStorageSpec};

/// The warm connection-worker pool never pre-spawns more than this many
/// threads; demand past it (or all of it, when the limit is 0/unlimited) is
/// served by one dedicated thread per accepted connection, mirroring Go's
/// `go s.onConn(clientConn)` per accept.
pub const MAX_CONNECTION_WORKERS: usize = 256;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 30_000;
/// Go's `tidb-server --lease` default: the DDL schema lease. The catalog
/// reload thread ticks at half of it, matching Go's domain reload loop.
const DEFAULT_SCHEMA_LEASE_MS: u64 = 45_000;
/// Process-wide memory-controller values from TiDB's `[instance]` section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryArbitratorConfig {
    /// Source `tidb_server_memory_limit` text.
    pub server_memory_limit: String,
    /// Source `tidb_mem_arbitrator_mode` text.
    pub mode: String,
    /// Source `tidb_mem_arbitrator_soft_limit` text.
    pub soft_limit: String,
}

/// Go `main.go`'s store dispatch: the engines this executable constructs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreKind {
    /// A real TiKV cluster through PD.
    TiKv,
    /// The embedded in-process store — Go's `--store unistore` (mockstore).
    Unistore,
}

/// Go `Performance.StatsLease` after duration parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatsLease {
    /// A negative lease: do not initialize or periodically load statistics.
    Disabled,
    /// A zero lease: run the loader with Go's three-second fallback.
    Zero,
    /// A positive lease: initialize and reload at this exact interval.
    Positive(Duration),
}

impl StatsLease {
    /// The interval used by Go's `loadStatsWorker`, or no worker for a
    /// negative lease.
    #[must_use]
    pub const fn reload_interval(self) -> Option<Duration> {
        match self {
            Self::Disabled => None,
            Self::Zero => Some(Duration::from_secs(3)),
            Self::Positive(interval) => Some(interval),
        }
    }

    /// The positive-only lease used by Go's slow-save version fence.
    #[must_use]
    pub const fn slow_save_interval(self) -> Duration {
        match self {
            Self::Positive(interval) => interval,
            Self::Disabled | Self::Zero => Duration::ZERO,
        }
    }
}

/// Complete startup input consumed by the concurrent SQL node.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeConfig {
    /// Go `cfg.Status.ReportStatus` (default true): whether the status
    /// HTTP listener starts.
    pub report_status: bool,
    /// Go `cfg.Status.StatusHost`.
    pub status_host: String,
    /// Go `cfg.Status.StatusPort` (default 10080).
    pub status_port: u16,
    /// Go `cfg.Socket` after `setGlobalVars`' `{Port}` substitution
    /// (`main.go:1110`) — the value `@@socket` reports, and the Unix-domain
    /// socket the server listens on beside `host:port` when non-empty.
    pub socket: String,
    /// Go `cfg.IsolationRead.Engines` (default `tikv,tiflash,tidb`), the
    /// startup value of `@@tidb_isolation_read_engines`.
    pub isolation_read_engines: Vec<String>,
    /// Address on which MySQL protocol connections are accepted.
    pub host: IpAddr,
    /// Go `--advertise-address`: the address a PEER should dial, which is
    /// what this node publishes in `/tidb/server/info`. Go resolves an
    /// unset one from the bind host unless that host is the wildcard, and
    /// leaves it empty otherwise -- the deferred local-IP lookup is the
    /// node's own, not the flag pass's.
    pub advertise_address: String,
    /// MySQL protocol port. Zero requests an ephemeral test port.
    pub port: u16,
    /// CPU indexes from Go's `--affinity-cpus` startup option.
    pub affinity_cpus: Vec<i64>,
    /// Go `--store`: which storage engine the node constructs over.
    pub store_kind: StoreKind,
    /// Plaintext PD endpoints in configured order. Empty for the in-process
    /// store, which has no control plane to dial.
    pub pd_endpoints: Vec<String>,
    /// Maximum accepted logical MySQL packet size.
    pub max_allowed_packet: usize,
    /// Immutable native-password account file. Empty when accounts would
    /// come from the cluster's own `mysql.*` (see [`Self::load_privileges`])
    /// or when [`Self::skip_grant_table`] deliberately uses neither source.
    pub auth_file: PathBuf,
    /// Load accounts and grants from the cluster's `mysql.*` tables at
    /// startup instead of from `--auth-file`.
    ///
    /// This is the bridge to a keyspace a Go TiDB bootstrapped: whatever
    /// `CREATE USER`/`GRANT` wrote there is what this node admits. Startup
    /// reads one snapshot; a [`crate::cluster_privileges::PrivilegeReloader`]
    /// then re-reads on the same `schema_lease / 2` cadence the catalog
    /// reloader uses, so a grant made afterwards reaches this node without a
    /// restart.
    pub load_privileges: bool,
    /// Go's `Instance.MaxConnections`: the configured cap on simultaneous
    /// client connections. `0` means unlimited, exactly as
    /// `server.go`'s `checkConnectionCount` treats it.
    pub max_connections: usize,
    /// Handshake, idle-command, and socket-write deadline for one connection.
    pub connection_timeout: Duration,
    /// Maximum recent deadlock records retained process-wide.
    pub deadlock_history_capacity: usize,
    /// Whether retryable in-statement deadlocks are retained.
    pub deadlock_history_collect_retryable: bool,
    /// DDL schema lease. A node that loaded its schema from the cluster
    /// re-reads the catalog every `schema_lease / 2`, so it is never more than
    /// one lease behind the cluster's schema version.
    pub schema_lease: Duration,
    /// Go `ddl.Start`'s `campaignOwner`: `Instance.TiDBEnableDDL`, the config
    /// file's `[instance] tidb_enable_ddl` overridden by `--run-ddl`
    /// (`main.go:751`). A node with it off never campaigns for DDL ownership
    /// and runs no DDL worker; its own DDL statements still queue for the
    /// cluster's owner.
    pub run_ddl: bool,
    /// Go `Performance.StatsLease`, retaining the negative/zero distinction
    /// that controls whether and how `loadStatsWorker` starts.
    pub stats_lease: StatsLease,
    /// Server certificate for inbound TLS on the MySQL port (TiDB's
    /// `[security] ssl-cert`). `None` with [`Self::auto_tls`] set generates a
    /// self-signed pair instead.
    pub ssl_cert: Option<PathBuf>,
    /// Private key matching [`Self::ssl_cert`] (TiDB's `[security] ssl-key`).
    pub ssl_key: Option<PathBuf>,
    /// Inbound client-certificate trust roots.
    pub ssl_ca: Option<PathBuf>,
    /// Minimum inbound TLS protocol from the shared configuration.
    pub min_tls_version: String,
    /// TiDB's `[security] disconnect-on-expired-password`, default `true`:
    /// refuse a login whose password has expired with 1862 instead of
    /// admitting it into a sandbox session.
    ///
    /// Go stores the INVERSE of this in a process-global atomic at startup
    /// (`cmd/tidb-server/main.go` around line 1067,
    /// `vardef.IsSandBoxModeEnabled.Store(!cfg.Security.DisconnectOnExpiredPassword)`),
    /// which is the only production writer of the flag the login path's
    /// expiry check reads. `--no-disconnect-on-expired-password` is that
    /// store, and it is what makes sandbox mode -- and the per-statement
    /// gate that restricts a sandboxed session to `SET PASSWORD`/`ALTER
    /// USER` -- reachable at all.
    pub disconnect_on_expired_password: bool,
    /// TiDB's `[security] enable-sem`: install the process-wide Security
    /// Enhanced Mode policy before any startup resource is admitted.
    pub sem_enabled: bool,
    /// TiDB's `[security] sem-config`: a nonempty path selects SEM v2.
    pub sem_config: String,
    /// TiDB's `[security] skip-grant-table`, accepted only when the process
    /// effective uid passes the source root-only validation.
    pub skip_grant_table: bool,
    /// Generate a self-signed certificate when no `--ssl-cert`/`--ssl-key` is
    /// configured, as TiDB's `[security] auto-tls` does.
    ///
    /// Defaults to false, as in Go. Enable it explicitly in the shared config;
    /// `--no-auto-tls` remains a native CLI override.
    pub auto_tls: bool,
    /// Cluster-facing gRPC transport security (TiDB's `[security]`
    /// `cluster-ssl-ca` / `cluster-ssl-cert` / `cluster-ssl-key`). Plaintext
    /// by default; setting a CA path engages TLS for the PD, TiKV, and etcd
    /// transports. `cluster-verify-cn` is rejected until this outbound-only
    /// node owns an inbound cluster endpoint on which it can be enforced.
    pub cluster_security: ClusterSecurity,
    /// Fully resolved process spill policy. Startup acquires the directory
    /// lease and validates capacity before opening any SQL listener.
    pub spill_storage: SpillStorageSpec,
    /// Process-wide global-memory controller policy.
    pub memory_arbitrator: MemoryArbitratorConfig,
    /// Effective Go process configuration installed before startup proceeds.
    pub global_config: SourceConfig,
}

/// Startup configuration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeConfigError {
    /// The caller requested usage text instead of startup.
    HelpRequested,
    /// A required option was omitted.
    MissingOption(&'static str),
    /// One option appeared more than once.
    DuplicateOption(String),
    /// The bounded executable does not implement this option.
    UnknownOption(String),
    /// The TOML file could not be read or decoded.
    ConfigFile {
        /// Configured path.
        path: PathBuf,
        /// Stable I/O or decode reason.
        reason: String,
    },
    /// Configuration validation succeeded; the executable must exit without booting.
    ConfigCheckSucceeded,
    /// An option did not have a following value.
    MissingValue(String),
    /// An option value was malformed or outside the admitted domain.
    InvalidValue {
        /// Option name.
        option: String,
        /// Stable reason for rejection.
        reason: String,
    },
    /// Only the real TiKV store is supported by this executable.
    UnsupportedStore(String),

    /// Configured-account mode must never bind a non-loopback address.
    NonLoopbackHost(IpAddr),
}

impl fmt::Display for NodeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HelpRequested => formatter.write_str("help requested"),
            Self::MissingOption(option) => write!(formatter, "missing required option {option}"),
            Self::DuplicateOption(option) => write!(formatter, "duplicate option {option}"),
            Self::UnknownOption(option) => write!(formatter, "unsupported option {option}"),
            Self::ConfigFile { path, reason } => {
                write!(formatter, "cannot load config {}: {reason}", path.display())
            }
            Self::ConfigCheckSucceeded => formatter.write_str("config check successful"),
            Self::MissingValue(option) => write!(formatter, "missing value for {option}"),
            Self::InvalidValue { option, reason } => {
                write!(formatter, "invalid value for {option}: {reason}")
            }
            Self::UnsupportedStore(store) => {
                write!(
                    formatter,
                    "unsupported store {store:?}; tikv and unistore are executable"
                )
            }
            Self::NonLoopbackHost(host) => write!(
                formatter,
                "refusing non-loopback MySQL listener {host} for --auth-file; use --load-privileges for cluster deployment"
            ),
        }
    }
}

impl std::error::Error for NodeConfigError {}

fn parse_file_schema_lease(value: &str) -> Result<Duration, NodeConfigError> {
    let parse = |value: &str| {
        serde_json::from_value::<tidb_config::configtypes::Duration>(serde_json::Value::String(
            value.to_owned(),
        ))
        .map(|duration| duration.0)
        .map_err(|error| error.to_string())
    };
    let nanos = parse(value)
        .or_else(|_| parse(&format!("{value}s")))
        .map_err(|reason| invalid("lease", &reason))?;
    if nanos < 0 {
        return Err(invalid("lease", "value must not be negative"));
    }
    if nanos == 0 {
        return Ok(Duration::from_millis(DEFAULT_SCHEMA_LEASE_MS));
    }
    Ok(Duration::from_nanos(
        u64::try_from(nanos).expect("positive i64 fits u64"),
    ))
}

fn parse_stats_lease(value: &str) -> Result<StatsLease, NodeConfigError> {
    let nanos = serde_json::from_value::<tidb_config::configtypes::Duration>(
        serde_json::Value::String(value.to_owned()),
    )
    .map(|duration| duration.0)
    .map_err(|error| invalid("performance.stats-lease", &error.to_string()))?;
    if nanos < 0 {
        return Ok(StatsLease::Disabled);
    }
    if nanos == 0 {
        return Ok(StatsLease::Zero);
    }
    Ok(StatsLease::Positive(Duration::from_nanos(
        u64::try_from(nanos).expect("nonnegative i64 fits u64"),
    )))
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// The command line's own node settings: the Rust adapter's options that
/// are no config-file key. The rest of the node comes from the config tree.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeOptions {
    /// `--auth-file`: the account TSV a node without the cluster's
    /// `mysql.user` serves.
    pub auth_file: Option<String>,
    /// `--load-privileges`: the cluster's `mysql.*` is the account source.
    pub load_privileges: bool,
    /// `--affinity-cpus`.
    pub affinity_cpus: Vec<i64>,
    /// `--connection-timeout-ms`.
    pub connection_timeout: Duration,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            auth_file: None,
            load_privileges: false,
            affinity_cpus: Vec::new(),
            connection_timeout: Duration::from_millis(DEFAULT_CONNECTION_TIMEOUT_MS),
        }
    }
}

impl NodeConfig {
    /// The native executable's deployment defaults, which a config file and
    /// the command line then override: a loopback listener over TiKV with no
    /// PD path yet.
    #[must_use]
    pub fn deployment_defaults() -> SourceConfig {
        let mut defaults = SourceConfig::default();
        defaults.host = "127.0.0.1".to_owned();
        defaults.store = tidb_config::store::StoreType("tikv".to_owned());
        defaults.path.clear();
        defaults
    }

    /// The node over the executable's defaults as `edit` leaves them, with no
    /// config file and no command-line flag: what Go's testkit runs under
    /// after `config.UpdateGlobal`.
    pub fn with_defaults(
        edit: impl FnOnce(&mut SourceConfig),
        options: NodeOptions,
    ) -> Result<Self, NodeConfigError> {
        let mut config = Self::deployment_defaults();
        edit(&mut config);
        Self::from_config(config, options)
    }

    /// The node over an effective config tree (Go `config.GetGlobalConfig()`
    /// once `main` has loaded it) and the command line's own options.
    pub fn from_config(
        mut global_config: SourceConfig,
        options: NodeOptions,
    ) -> Result<Self, NodeConfigError> {
        let store_kind = match global_config.store.0.as_str() {
            "tikv" => StoreKind::TiKv,
            "unistore" => StoreKind::Unistore,
            store => return Err(NodeConfigError::UnsupportedStore(store.to_owned())),
        };
        let skip_grant_table = global_config.security.skip_grant_table;
        let mut load_privileges = options.load_privileges;
        if store_kind == StoreKind::TiKv && options.auth_file.is_none() && !skip_grant_table {
            load_privileges = true;
        }
        let host = parse_ip("--host", &global_config.host)?;
        if !host.is_loopback() && !load_privileges && !skip_grant_table {
            return Err(NodeConfigError::NonLoopbackHost(host));
        }
        let port = u16::try_from(global_config.port)
            .map_err(|_| invalid("--port", "expected a port number"))?;
        let pd_endpoints = match store_kind {
            StoreKind::TiKv if global_config.path.is_empty() => {
                return Err(NodeConfigError::MissingOption("--path"));
            }
            StoreKind::TiKv => parse_pd_endpoints(global_config.path.clone())?,
            StoreKind::Unistore => Vec::new(),
        };
        let auth_file = match (options.auth_file, load_privileges, skip_grant_table) {
            (_, _, true) => PathBuf::new(),
            (Some(_), true, false) => return Err(invalid(
                "--load-privileges",
                "cannot be combined with --auth-file; the cluster's mysql.* is then the only account source",
            )),
            (Some(file), false, false) => PathBuf::from(file),
            (None, true, false) => PathBuf::new(),
            (None, false, false) => return Err(NodeConfigError::MissingOption("--auth-file")),
        };
        let schema_lease = parse_file_schema_lease(&global_config.lease)?;
        let stats_lease = parse_stats_lease(&global_config.performance.stats_lease)?;
        let spill_encryption = global_config
            .security
            .spilled_file_encryption_method
            .parse::<SpillEncryptionMethod>()
            .map_err(|error| {
                invalid(
                    "security.spilled-file-encryption-method",
                    &error.to_string(),
                )
            })?;
        global_config.update_temp_storage_path();
        let spill_storage = SpillStorageSpec {
            path: PathBuf::from(&global_config.temp_storage_path),
            quota_bytes: global_config.temp_storage_quota,
            encryption: spill_encryption,
        };
        let cluster_security = build_cluster_security(
            nonempty(global_config.security.cluster_ssl_ca.clone()),
            nonempty(global_config.security.cluster_ssl_cert.clone()),
            nonempty(global_config.security.cluster_ssl_key.clone()),
        )?;
        let status_port = u16::try_from(global_config.status.status_port)
            .map_err(|_| invalid("--status", "expected a port number"))?;
        // Empty instance strings mean "leave the system-variable default" in
        // Go setInstanceVar. They are not empty runtime memory policies.
        let instance_value = |configured: &str, default: &str| {
            if configured.is_empty() {
                default.to_owned()
            } else {
                configured.to_owned()
            }
        };
        Ok(Self {
            report_status: global_config.status.report_status,
            status_host: global_config.status.status_host.clone(),
            status_port,
            socket: global_config
                .socket
                .replacen("{Port}", &port.to_string(), 1),
            isolation_read_engines: global_config.isolation_read.engines.clone(),
            host,
            advertise_address: global_config.advertise_address.clone(),
            port,
            affinity_cpus: options.affinity_cpus,
            store_kind,
            pd_endpoints,
            max_allowed_packet: usize::try_from(global_config.max_allowed_packet)
                .map_err(|_| invalid("max-allowed-packet", "value does not fit this platform"))?,
            auth_file,
            max_connections: global_config.instance.max_connections as usize,
            connection_timeout: options.connection_timeout,
            deadlock_history_capacity: usize::try_from(
                global_config.pessimistic_txn.deadlock_history_capacity,
            )
            .map_err(|_| {
                invalid(
                    "pessimistic-txn.deadlock-history-capacity",
                    "value does not fit this platform",
                )
            })?,
            deadlock_history_collect_retryable: global_config
                .pessimistic_txn
                .deadlock_history_collect_retryable,
            schema_lease,
            run_ddl: global_config.instance.tidb_enable_ddl.load(),
            stats_lease,
            load_privileges,
            ssl_cert: nonempty(global_config.security.ssl_cert.clone()).map(PathBuf::from),
            ssl_key: nonempty(global_config.security.ssl_key.clone()).map(PathBuf::from),
            ssl_ca: nonempty(global_config.security.ssl_ca.clone()).map(PathBuf::from),
            min_tls_version: global_config.security.min_tls_version.clone(),
            auto_tls: global_config.security.auto_tls,
            disconnect_on_expired_password: global_config.security.disconnect_on_expired_password,
            sem_enabled: global_config.security.enable_sem,
            sem_config: global_config.security.sem_config.clone(),
            skip_grant_table,
            cluster_security,
            spill_storage,
            memory_arbitrator: MemoryArbitratorConfig {
                server_memory_limit: instance_value(
                    &global_config.instance.server_memory_limit,
                    tidb_vardef::defaults::DEF_TIDB_SERVER_MEMORY_LIMIT,
                ),
                mode: instance_value(
                    &global_config.instance.mem_arbitrator_mode,
                    tidb_vardef::defaults::DEF_TIDB_MEM_ARBITRATOR_MODE_TEXT,
                ),
                soft_limit: instance_value(
                    &global_config.instance.mem_arbitrator_soft_limit,
                    tidb_vardef::defaults::DEF_TIDB_MEM_ARBITRATOR_SOFT_LIMIT_TEXT,
                ),
            },
            global_config,
        })
    }
}

fn parse_ip(option: &str, value: &str) -> Result<IpAddr, NodeConfigError> {
    value
        .parse()
        .map_err(|_| invalid(option, "expected an IP address"))
}

fn parse_pd_endpoints(value: String) -> Result<Vec<String>, NodeConfigError> {
    let endpoints = value
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
        return Err(invalid(
            "--path",
            "expected a comma-separated PD endpoint list",
        ));
    }
    for endpoint in &endpoints {
        if endpoint.contains("//") || !endpoint.contains(':') {
            return Err(invalid(
                "--path",
                "PD endpoints must be plaintext host:port values",
            ));
        }
    }
    Ok(endpoints)
}

/// Assembles the cluster transport security from the parsed `[security]`
/// options, mirroring client-go's `Security.ToTLSConfig`: TLS engages only
/// when a CA is set, and the client key pair is loaded only when both the
/// cert and key are present. A cert or key without a CA, or one without the
/// other, is a misconfiguration the node rejects at startup rather than
/// silently ignore.
fn build_cluster_security(
    ca: Option<String>,
    cert: Option<String>,
    key: Option<String>,
) -> Result<ClusterSecurity, NodeConfigError> {
    if ca.is_none() && (cert.is_some() || key.is_some()) {
        return Err(invalid(
            "--cluster-ssl-ca",
            "cluster TLS material requires --cluster-ssl-ca; without a CA the transport stays plaintext",
        ));
    }
    match (&cert, &key) {
        (Some(_), None) => {
            return Err(invalid(
                "--cluster-ssl-key",
                "--cluster-ssl-cert requires --cluster-ssl-key",
            ))
        }
        (None, Some(_)) => {
            return Err(invalid(
                "--cluster-ssl-cert",
                "--cluster-ssl-key requires --cluster-ssl-cert",
            ))
        }
        _ => {}
    }
    Ok(ClusterSecurity::new(
        ca.unwrap_or_default(),
        cert.unwrap_or_default(),
        key.unwrap_or_default(),
        Vec::new(),
    ))
}
/// An option whose value the node refuses, and why.
pub fn invalid(option: &str, reason: &str) -> NodeConfigError {
    NodeConfigError::InvalidValue {
        option: option.to_owned(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{parse_stats_lease, StatsLease};

    #[test]
    fn stats_lease_uses_go_duration_parsing_and_allows_zero() {
        assert_eq!(
            parse_stats_lease("1500ms").unwrap(),
            StatsLease::Positive(Duration::from_millis(1500))
        );
        assert_eq!(parse_stats_lease("0s").unwrap(), StatsLease::Zero);
        assert_eq!(parse_stats_lease("-1s").unwrap(), StatsLease::Disabled);
        assert_eq!(
            parse_stats_lease("0s").unwrap().reload_interval(),
            Some(Duration::from_secs(3))
        );
        assert_eq!(parse_stats_lease("-1s").unwrap().reload_interval(), None);
    }
}
