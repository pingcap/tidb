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
//! TOML loading, validation and explicit Go CLI overrides precede this runtime
//! projection; Rust-native adapter arguments do not define another TOML schema.

use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use tidb_config::config_tree::Config as SourceConfig;
use tidb_config::kerneltype;
use tidb_pd_client::ClusterSecurity;
use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorageSpec};

/// The warm connection-worker pool never pre-spawns more than this many
/// threads; demand past it (or all of it, when the limit is 0/unlimited) is
/// served by one dedicated thread per accepted connection, mirroring Go's
/// `go s.onConn(clientConn)` per accept.
pub(crate) const MAX_CONNECTION_WORKERS: usize = 256;
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
    /// (`main.go:1110`) — the value `@@socket` reports. The listener itself
    /// remains TCP-only; the unix-socket LISTENER is unported.
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
    pub(crate) global_config: SourceConfig,
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

impl NodeConfig {
    /// Parses the source-shaped command line, including the executable name.
    ///
    /// Both `--name value` and `--name=value` are accepted. The parser is
    /// deliberately one-use and rejects duplicate values instead of applying
    /// last-option-wins behavior to security or topology settings.
    pub fn parse<I, S>(arguments: I) -> Result<Self, NodeConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut arguments = arguments.into_iter().map(Into::into);
        let _program = arguments.next();
        // main.go's own flag surface is consumed FIRST, so every Go spelling
        // is accepted exactly as initFlagSet accepts it; the node's options
        // remain for the loop below, untouched and in order.
        let (mut main_flags, remaining) =
            crate::main_flags::extract_main_go_flags(arguments.collect())
                .map_err(|error| invalid("main.go flags", &error.to_string()))?;
        let mut pending = remaining.into_iter().peekable();

        let mut host = None;
        let mut port = None;
        let mut affinity_cpus = None;
        let mut path = None;
        let mut store = None;
        let mut max_allowed_packet = None;
        let mut auth_file = None;
        let mut max_connections = None;
        let mut connection_timeout_ms = None;
        let mut schema_lease_ms = None;
        let mut load_privileges = false;
        let mut legacy_cluster_session_option = false;
        let mut ssl_cert = None;
        let mut ssl_key = None;
        let mut no_auto_tls = false;
        let mut no_disconnect_on_expired_password = false;
        let mut cluster_ssl_ca = None;
        let mut cluster_ssl_cert = None;
        let mut cluster_ssl_key = None;
        let mut config_path = None;
        let mut socket = None;

        while let Some(argument) = pending.next() {
            if argument == "--help" || argument == "-h" {
                return Err(NodeConfigError::HelpRequested);
            }
            if argument == "-P" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with('-'))
                    .ok_or_else(|| NodeConfigError::MissingValue("-P".to_owned()))?;
                set_once(&mut port, "-P", value)?;
                continue;
            }
            if argument == "--load-privileges" {
                if load_privileges {
                    return Err(NodeConfigError::DuplicateOption(argument));
                }
                load_privileges = true;
                continue;
            }
            if argument == "--no-auto-tls" {
                if no_auto_tls {
                    return Err(NodeConfigError::DuplicateOption(argument));
                }
                no_auto_tls = true;
                continue;
            }
            if argument == "--no-disconnect-on-expired-password" {
                if no_disconnect_on_expired_password {
                    return Err(NodeConfigError::DuplicateOption(argument));
                }
                no_disconnect_on_expired_password = true;
                continue;
            }
            if argument == "--cluster-session" {
                // Compatibility spelling only: both stores use the shared session.
                if legacy_cluster_session_option {
                    return Err(NodeConfigError::DuplicateOption(argument));
                }
                legacy_cluster_session_option = true;
                continue;
            }
            let (option, inline_value) = split_option(&argument)?;
            let value = match inline_value {
                Some(value) => value.to_owned(),
                None => pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue(option.to_owned()))?,
            };
            match option {
                "--host" => set_once(&mut host, option, value)?,
                "--affinity-cpus" => set_once(&mut affinity_cpus, option, value)?,
                "--port" => set_once(&mut port, option, value)?,
                "--socket" => set_once(&mut socket, option, value)?,
                "--path" => set_once(&mut path, option, value)?,
                "--store" => set_once(&mut store, option, value)?,
                "--max-allowed-packet" => {
                    set_once(&mut max_allowed_packet, option, value)?;
                }
                "--auth-file" => set_once(&mut auth_file, option, value)?,
                "--max-connections" => set_once(&mut max_connections, option, value)?,
                "--connection-timeout-ms" => {
                    set_once(&mut connection_timeout_ms, option, value)?;
                }
                "--lease-ms" => set_once(&mut schema_lease_ms, option, value)?,
                "--ssl-cert" => set_once(&mut ssl_cert, option, value)?,
                "--ssl-key" => set_once(&mut ssl_key, option, value)?,
                "--cluster-ssl-ca" => set_once(&mut cluster_ssl_ca, option, value)?,
                "--cluster-ssl-cert" => set_once(&mut cluster_ssl_cert, option, value)?,
                "--cluster-ssl-key" => set_once(&mut cluster_ssl_key, option, value)?,
                "--config" => set_once(&mut config_path, option, value)?,
                _ => return Err(NodeConfigError::UnknownOption(option.to_owned())),
            }
        }

        let max_allowed_packet = max_allowed_packet
            .as_deref()
            .map(|value| parse_positive_number::<u64>("--max-allowed-packet", value))
            .transpose()?;
        let max_connections = max_connections
            .as_deref()
            .map(|value| parse_connection_limit("--max-connections", value))
            .transpose()?;
        let mut defaults = SourceConfig::default();
        // Preserve the native executable's deployment defaults. TOML and
        // explicit flags override these through the single shared owner.
        defaults.host = "127.0.0.1".to_owned();
        defaults.store = tidb_config::store::StoreType("tikv".to_owned());
        defaults.path.clear();
        main_flags.host = host;
        main_flags.port = port;
        main_flags.store_path = path;
        main_flags.store = store.map(|store| store.to_ascii_lowercase());
        if let Some(socket) = socket {
            main_flags.socket = Some(socket);
        }
        if let Some(lease) = schema_lease_ms.as_deref() {
            let millis: u64 = parse_positive_number("--lease-ms", lease)?;
            main_flags.ddl_lease = Some(format!("{millis}ms"));
        }
        let (mut global_config, warnings) = tidb_config::config_tree::load::prepare_config(
            defaults,
            config_path.as_deref().map(std::path::Path::new),
            main_flags.config_check,
            main_flags.config_strict,
            |config| {
                crate::main_flags::override_config(config, &main_flags)?;
                // Rust-native CLI aliases override the same fields, before
                // SourceConfig::valid, rather than rebuilding a second config.
                if let Some(value) = &max_allowed_packet {
                    config.max_allowed_packet = *value;
                }
                if let Some(value) = &max_connections {
                    config.instance.max_connections = *value as u32;
                }
                for (target, value) in [
                    (&mut config.security.ssl_cert, &ssl_cert),
                    (&mut config.security.ssl_key, &ssl_key),
                    (&mut config.security.cluster_ssl_ca, &cluster_ssl_ca),
                    (&mut config.security.cluster_ssl_cert, &cluster_ssl_cert),
                    (&mut config.security.cluster_ssl_key, &cluster_ssl_key),
                ] {
                    if let Some(value) = value {
                        target.clone_from(value);
                    }
                }
                if no_auto_tls {
                    config.security.auto_tls = false;
                }
                if no_disconnect_on_expired_password {
                    config.security.disconnect_on_expired_password = false;
                }
                Ok(())
            },
        )
        .map_err(|reason| match config_path.as_ref() {
            Some(path) => NodeConfigError::ConfigFile {
                path: PathBuf::from(path),
                reason,
            },
            None => invalid("configuration", &reason),
        })?;
        for warning in warnings {
            eprintln!("{warning}");
        }
        validate_version_config(&global_config)?;
        if main_flags.config_check {
            return Err(NodeConfigError::ConfigCheckSucceeded);
        }

        let store_kind = match global_config.store.0.as_str() {
            "tikv" => StoreKind::TiKv,
            "unistore" => StoreKind::Unistore,
            store => return Err(NodeConfigError::UnsupportedStore(store.to_owned())),
        };
        let skip_grant_table = global_config.security.skip_grant_table;
        if store_kind == StoreKind::TiKv && auth_file.is_none() && !skip_grant_table {
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
        let auth_file = match (auth_file, load_privileges, skip_grant_table) {
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
            affinity_cpus: parse_affinity_cpus(affinity_cpus.as_deref().unwrap_or_default())?,
            store_kind,
            pd_endpoints,
            max_allowed_packet: usize::try_from(global_config.max_allowed_packet)
                .map_err(|_| invalid("max-allowed-packet", "value does not fit this platform"))?,
            auth_file,
            max_connections: global_config.instance.max_connections as usize,
            connection_timeout: Duration::from_millis(match connection_timeout_ms {
                Some(value) => parse_positive_number("--connection-timeout-ms", &value)?,
                None => DEFAULT_CONNECTION_TIMEOUT_MS,
            }),
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

    /// Initializes the same process globals Go reads when printing `-V`.
    pub fn initialize_versions_for_display<I, S>(arguments: I) -> Result<(), NodeConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut arguments = arguments.into_iter().map(Into::into);
        let _program = arguments.next();
        // main.go's own flag surface is consumed FIRST, so every Go spelling
        // is accepted exactly as initFlagSet accepts it; the node's options
        // remain for the loop below, untouched and in order.
        let (mut main_flags, remaining) =
            crate::main_flags::extract_main_go_flags(arguments.collect())
                .map_err(|error| invalid("main.go flags", &error.to_string()))?;
        let mut pending = remaining.into_iter().peekable();
        let mut config_path = None;
        let mut store = None;
        while let Some(argument) = pending.next() {
            if argument == "-V" {
                continue;
            }
            if argument == "--config" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue("--config".to_owned()))?;
                set_once(&mut config_path, "--config", value)?;
            } else if let Some(value) = argument.strip_prefix("--config=") {
                set_once(&mut config_path, "--config", value.to_owned())?;
            } else if argument == "--store" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue("--store".to_owned()))?;
                set_once(&mut store, "--store", value)?;
            } else if let Some(value) = argument.strip_prefix("--store=") {
                set_once(&mut store, "--store", value.to_owned())?;
            }
        }

        main_flags.store = store;
        let (config, warnings) = tidb_config::config_tree::load::prepare_config(
            SourceConfig::default(),
            config_path.as_deref().map(std::path::Path::new),
            false,
            main_flags.config_strict,
            |config| crate::main_flags::override_config(config, &main_flags),
        )
        .map_err(|reason| invalid("--config", &reason))?;
        for warning in warnings {
            eprintln!("{warning}");
        }
        validate_version_config(&config)?;
        install_process_globals(config);
        Ok(())
    }

    pub(crate) fn install_process_globals(&self) {
        let max_procs = self.global_config.performance.max_procs;
        install_process_globals(self.global_config.clone());
        tidb_util::cpu::install_cpu_count(max_procs, self.affinity_cpus.len());
    }

    pub(crate) fn startup_config_json(&self) -> Vec<u8> {
        serde_json::to_vec(&self.global_config).expect("effective global config is serializable")
    }

    /// Stable usage text printed by the executable for `--help`.
    #[must_use]
    pub const fn help_text() -> &'static str {
        "Usage: tidb-server [-V] [--config <tidb.toml>] --path <pd[,pd...]> \
[--max-connections <count>] [--connection-timeout-ms <milliseconds>] \
[--lease-ms <milliseconds>] \
[--auth-file <mode-0600-tsv> | --load-privileges] \
[--host <listen-ip>] [-P <port>|--port <port>] [--store tikv|unistore] \
[--affinity-cpus <cpu[,cpu...]>] \
[--max-allowed-packet <bytes>] \
[--ssl-cert <cert-pem> --ssl-key <key-pem>] [--no-auto-tls] \
[--no-disconnect-on-expired-password] \
[--cluster-ssl-ca <ca-pem> [--cluster-ssl-cert <cert-pem> --cluster-ssl-key <key-pem>]]"
    }
}

fn validate_version_config(config: &SourceConfig) -> Result<(), NodeConfigError> {
    if kerneltype::is_next_gen() {
        if !config.tidb_edition.is_empty()
            || !config.tidb_release_version.is_empty()
            || !config.server_version.is_empty()
        {
            return Err(invalid(
                "--config",
                "config options tidb-edition, tidb-release-version and server-version are not \
                 allowed to set in nextgen kernel",
            ));
        }
        let versions = tidb_mysql::runtime_versions();
        let component =
            tidb_mysql::normalize_tidb_release_version_for_next_gen(&versions.tidb_release_version);
        tidb_mysql::build_tidbx_server_version(component)
            .map_err(|error| invalid("--config", &error.to_string()))?;
    }
    Ok(())
}

fn install_process_globals(config: SourceConfig) {
    if kerneltype::is_next_gen() {
        tidb_config::deploymode::set(config.deploy_mode)
            .expect("validated next-generation deploy mode");
    }
    let configured_edition = config.tidb_edition.clone();
    let configured_release = config.tidb_release_version.clone();
    let configured_server = config.server_version.clone();
    tidb_txnkv::set_txn_entry_size_limit(config.performance.txn_entry_size_limit);
    tidb_txnkv::set_txn_total_size_limit(config.performance.txn_total_size_limit);
    tidb_config::config_tree::config::store_global_config(config);

    let defaults = tidb_mysql::runtime_versions();
    if kerneltype::is_next_gen() {
        let release =
            tidb_mysql::normalize_tidb_release_version_for_next_gen(&defaults.tidb_release_version)
                .to_owned();
        let server = tidb_mysql::build_tidbx_server_version(&release)
            .expect("next-generation release was validated");
        tidb_mysql::set_runtime_versions(release, server);
        return;
    }
    if !configured_edition.is_empty() {
        tidb_util::versioninfo::set_tidb_edition(configured_edition);
    }
    tidb_mysql::set_runtime_versions(
        if configured_release.is_empty() {
            defaults.tidb_release_version
        } else {
            configured_release
        },
        if configured_server.is_empty() {
            defaults.server_version
        } else {
            configured_server
        },
    );
}

fn split_option(argument: &str) -> Result<(&str, Option<&str>), NodeConfigError> {
    if !argument.starts_with("--") {
        return Err(NodeConfigError::UnknownOption(argument.to_owned()));
    }
    match argument.split_once('=') {
        Some((option, "")) => Err(NodeConfigError::MissingValue(option.to_owned())),
        Some((option, value)) => Ok((option, Some(value))),
        None => Ok((argument, None)),
    }
}

fn set_once(slot: &mut Option<String>, option: &str, value: String) -> Result<(), NodeConfigError> {
    if slot.replace(value).is_some() {
        return Err(NodeConfigError::DuplicateOption(option.to_owned()));
    }
    Ok(())
}

fn parse_ip(option: &str, value: &str) -> Result<IpAddr, NodeConfigError> {
    value
        .parse()
        .map_err(|_| invalid(option, "expected an IP address"))
}

fn parse_affinity_cpus(value: &str) -> Result<Vec<i64>, NodeConfigError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|cpu| !cpu.is_empty())
        .map(|cpu| {
            cpu.parse::<i64>()
                .map_err(|_| invalid("--affinity-cpus", "expected comma-separated CPU indexes"))
        })
        .collect()
}

fn parse_number<T>(option: &str, value: &str) -> Result<T, NodeConfigError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| invalid(option, "expected an unsigned decimal integer"))
}

fn parse_positive_number<T>(option: &str, value: &str) -> Result<T, NodeConfigError>
where
    T: std::str::FromStr + Default + PartialEq,
{
    let parsed = parse_number(option, value)?;
    if parsed == T::default() {
        return Err(invalid(option, "value must be greater than zero"));
    }
    Ok(parsed)
}

/// Parses `--max-connections` with Go's semantics: a plain uint32 where zero
/// means unlimited, not an error.
fn parse_connection_limit(option: &str, value: &str) -> Result<usize, NodeConfigError> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| invalid(option, "expected an unsigned 32-bit integer"))?;
    Ok(usize::try_from(parsed).expect("u32 fits usize"))
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

fn invalid(option: &str, reason: &str) -> NodeConfigError {
    NodeConfigError::InvalidValue {
        option: option.to_owned(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{parse_stats_lease, NodeConfig, NodeConfigError, StatsLease, StoreKind};

    #[test]
    fn command_token_flag_reaches_effective_config() {
        let config = NodeConfig::parse([
            "tidb-server",
            "--store",
            "unistore",
            "--cluster-session",
            "--auth-file",
            "/tmp/users.tsv",
            "--token-limit",
            "1",
        ])
        .unwrap();
        assert_eq!(config.global_config.token_limit, 1);
    }

    #[test]
    fn command_token_file_and_flag_precedence_matches_go() {
        let path =
            std::env::temp_dir().join(format!("tidb-command-token-{}.toml", std::process::id()));
        for (file_limit, flag, expected) in [
            (2u64, None, 2usize),
            (0, None, 1000),
            (99_999_999_999, None, 1024 * 1024),
            (2, Some("1"), 1),
            (2, Some("0"), 0),
            // main.go casts the signed flag to uint after loading the file.
            (2, Some("-1"), usize::MAX),
        ] {
            fs::write(&path, format!("token-limit = {file_limit}\n")).unwrap();
            let mut args = vec![
                "tidb-server",
                "--store",
                "unistore",
                "--cluster-session",
                "--auth-file",
                "/tmp/users.tsv",
                "--config",
                path.to_str().unwrap(),
            ];
            if let Some(flag) = flag {
                args.extend(["--token-limit", flag]);
            }
            let config = NodeConfig::parse(args);
            fs::remove_file(&path).unwrap();
            assert_eq!(config.unwrap().global_config.token_limit, expected);
        }
    }

    /// The cluster TLS options thread into a `ClusterSecurity`, and their
    /// consistency rules (CA required for any material, cert⇔key together)
    /// are enforced at startup rather than deferred to a connect failure.
    #[test]
    fn cluster_tls_options_build_security_and_reject_partial_material() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ];

        // No security options: plaintext, backward compatible.
        let plaintext = NodeConfig::parse(base).unwrap();
        assert!(!plaintext.cluster_security.is_tls_enabled());

        // CA + client key pair: full mutual TLS.
        let secured = NodeConfig::parse(
            base.iter()
                .copied()
                .chain([
                    "--cluster-ssl-ca",
                    "/tls/ca.pem",
                    "--cluster-ssl-cert",
                    "/tls/cert.pem",
                    "--cluster-ssl-key",
                    "/tls/key.pem",
                ])
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(secured.cluster_security.is_tls_enabled());
        assert_eq!(secured.cluster_security.ca_path(), "/tls/ca.pem");
        assert_eq!(secured.cluster_security.cert_path(), "/tls/cert.pem");
        assert!(secured.cluster_security.verify_cn().is_empty());

        // The accepted option is an inbound peer-CN allowlist. This node owns
        // only outbound cluster clients, so accepting it would falsely claim
        // a restriction no transport can enforce.
        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain(["--cluster-verify-cn", "tidb,tikv"])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::UnknownOption(option)) if option == "--cluster-verify-cn"
        ));

        // Cert without key is rejected.
        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain([
                        "--cluster-ssl-ca",
                        "/tls/ca.pem",
                        "--cluster-ssl-cert",
                        "/tls/cert.pem"
                    ])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::InvalidValue { .. })
        ));

        // Client material without a CA is rejected.
        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain([
                        "--cluster-ssl-cert",
                        "/tls/cert.pem",
                        "--cluster-ssl-key",
                        "/tls/key.pem"
                    ])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::InvalidValue { .. })
        ));
    }

    /// `--load-privileges` names the cluster's own `mysql.*` as the account
    /// source, which is only meaningful in place of `--auth-file`: two
    /// sources would be two answers to "may this user log in".
    #[test]
    fn the_account_source_is_exactly_one_of_the_auth_file_or_the_cluster() {
        let base = ["tidb-server", "--path", "127.0.0.1:2379"];

        let from_cluster = NodeConfig::parse(
            base.iter()
                .copied()
                .chain(["--load-privileges"])
                .collect::<Vec<_>>(),
        )
        .expect("--load-privileges alone is a complete account source");
        assert!(from_cluster.load_privileges);
        assert_eq!(from_cluster.auth_file.as_os_str(), "");

        let from_file = NodeConfig::parse(
            base.iter()
                .copied()
                .chain(["--auth-file", "/tmp/users.tsv"])
                .collect::<Vec<_>>(),
        )
        .expect("--auth-file alone is a complete account source");
        assert!(!from_file.load_privileges);

        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain(["--load-privileges", "--auth-file", "/tmp/users.tsv"])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::InvalidValue { .. })
        ));
        let ordinary = NodeConfig::parse(base).unwrap();
        assert!(ordinary.load_privileges);
        assert!(ordinary.auth_file.as_os_str().is_empty());
    }

    /// The only source surface for skip-grant-table is TiDB's security TOML
    /// field. Source validation admits it only to an effective-root process;
    /// the resolved NodeConfig then carries the exact startup policy that the
    /// configured account store consumes.
    #[test]
    fn skip_grant_table_is_root_only_and_reaches_the_resolved_node_config() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("wall clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tidb-skip-grant-{}-{nonce}.toml",
            std::process::id()
        ));
        fs::write(&path, "[security]\nskip-grant-table = true\n").expect("write source config");
        let path_text = path.to_string_lossy().into_owned();
        let parsed = NodeConfig::parse([
            "tidb-server",
            "--config",
            &path_text,
            "--path",
            "127.0.0.1:2379",
        ]);
        let combined = NodeConfig::parse([
            "tidb-server",
            "--config",
            &path_text,
            "--path",
            "127.0.0.1:2379",
            "--load-privileges",
            "--auth-file",
            "/definitely/not/read.tsv",
        ]);
        fs::remove_file(&path).expect("remove source config");

        #[cfg(unix)]
        let is_root = rustix::process::geteuid().as_raw() == 0;
        #[cfg(not(unix))]
        let is_root = false;
        if is_root {
            let parsed = parsed.expect("effective root may opt in");
            assert!(parsed.skip_grant_table);
            assert_eq!(parsed.auth_file.as_os_str(), "");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&parsed.startup_config_json())
                    .expect("startup projection is JSON")["security"]["skip-grant-table"],
                true,
            );

            let combined = combined.expect("recovery mode ignores both account sources");
            assert!(combined.skip_grant_table);
            assert_eq!(combined.auth_file.as_os_str(), "");
        } else {
            assert!(matches!(
                parsed,
                Err(NodeConfigError::ConfigFile { reason, .. })
                    if reason.contains("TiDB run with skip-grant-table need root privilege")
            ));
        }

        let ordinary = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ])
        .expect("ordinary config");
        assert!(!ordinary.skip_grant_table);
    }

    #[test]
    fn instance_memory_arbitrator_settings_are_admitted_as_one_startup_policy() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("wall clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tidb-memory-arbitrator-{}-{nonce}.toml",
            std::process::id()
        ));
        fs::write(
            &path,
            "[instance]\ntidb_server_memory_limit = '1GiB'\ntidb_mem_arbitrator_mode = 'priority'\ntidb_mem_arbitrator_soft_limit = '0.75'\n",
        )
        .expect("write source config");
        let path_text = path.to_string_lossy().into_owned();
        let parsed = NodeConfig::parse([
            "tidb-server",
            "--config",
            &path_text,
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ])
        .expect("the three source memory settings are one admitted policy");
        assert_eq!(parsed.memory_arbitrator.server_memory_limit, "1GiB");
        assert_eq!(parsed.memory_arbitrator.mode, "priority");
        assert_eq!(parsed.memory_arbitrator.soft_limit, "0.75");
        fs::remove_file(path).expect("remove source config");
    }

    /// Go `cmd/tidb-server/main.go` around line 1067 stores the INVERSE of
    /// `security.disconnect-on-expired-password` (default `true`) into the
    /// server-wide sandbox flag. This flag is that config option, and it is
    /// the only production writer of the flag: without it, sandbox mode --
    /// and the per-statement gate that restricts a sandboxed session -- are
    /// unreachable code.
    #[test]
    fn expired_passwords_disconnect_by_default_and_the_flag_opts_into_sandboxing() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--load-privileges",
        ];
        assert!(
            NodeConfig::parse(base)
                .expect("a complete configuration")
                .disconnect_on_expired_password
        );
        assert!(
            !NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain(["--no-disconnect-on-expired-password"])
                    .collect::<Vec<_>>(),
            )
            .expect("a complete configuration")
            .disconnect_on_expired_password
        );
        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain([
                        "--no-disconnect-on-expired-password",
                        "--no-disconnect-on-expired-password",
                    ])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::DuplicateOption(_))
        ));
    }

    #[test]
    fn file_settings_and_cli_overrides_share_the_startup_projection() {
        let path =
            std::env::temp_dir().join(format!("tidb-shared-config-{}.toml", std::process::id()));
        fs::write(
            &path,
            r#"
socket = "/tmp/from-file-{Port}.sock"
advertise-address = "from-file"
[status]
report-status = false
status-port = 10091
[log]
level = "warn"
[security]
auto-tls = true
ssl-ca = "ca.pem"
[performance]
stats-lease = "-1s"
"#,
        )
        .unwrap();
        let result = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--config",
            path.to_str().unwrap(),
            "--status",
            "10092",
            "--report-status=true",
            "--socket",
            "/tmp/from-cli-{Port}.sock",
            "--advertise-address",
            "from-cli",
            "--temp-dir",
            "/private/tmp/from-cli",
            "-L",
            "error",
            "--no-auto-tls",
            "--ssl-cert",
            "cert.pem",
            "--ssl-key",
            "key.pem",
        ]);
        fs::remove_file(path).unwrap();
        let config = result.unwrap();
        assert_eq!(config.stats_lease, StatsLease::Disabled);
        assert_eq!(config.status_port, 10092);
        assert!(config.report_status);
        assert_eq!(config.socket, "/tmp/from-cli-4000.sock");
        assert_eq!(config.advertise_address, "from-cli");
        assert!(!config.auto_tls);
        let json: serde_json::Value =
            serde_json::from_slice(&config.startup_config_json()).unwrap();
        assert_eq!(json["status"]["status-port"], 10092);
        assert_eq!(json["status"]["report-status"], true);
        assert_eq!(json["socket"], "/tmp/from-cli-{Port}.sock");
        assert_eq!(json["temp-dir"], "/private/tmp/from-cli");
        assert_eq!(json["log"]["level"], "error");
        assert_eq!(json["security"]["auto-tls"], false);
        assert_eq!(json["security"]["ssl-ca"], "ca.pem");
        assert_eq!(json["security"]["ssl-cert"], "cert.pem");
    }

    #[test]
    fn startup_config_projection_contains_the_effective_owned_values() {
        let config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
            "--port",
            "4406",
        ])
        .unwrap();
        let projected: serde_json::Value =
            serde_json::from_slice(&config.startup_config_json()).unwrap();
        assert_eq!(projected["host"], "127.0.0.1");
        assert_eq!(projected["port"], 4406);
        assert_eq!(projected["path"], "127.0.0.1:2379");
        assert_eq!(projected["store"], "tikv");
        assert_eq!(
            config.memory_arbitrator.server_memory_limit,
            tidb_vardef::defaults::DEF_TIDB_SERVER_MEMORY_LIMIT
        );
        assert_eq!(
            config.memory_arbitrator.mode,
            tidb_vardef::defaults::DEF_TIDB_MEM_ARBITRATOR_MODE_TEXT
        );
        assert_eq!(
            config.memory_arbitrator.soft_limit,
            tidb_vardef::defaults::DEF_TIDB_MEM_ARBITRATOR_SOFT_LIMIT_TEXT
        );
        // Go's config default: `Instance.MaxConnections` is 0 (unlimited).
        assert_eq!(projected["instance"]["max_connections"], 0);
        assert_eq!(
            config.stats_lease,
            StatsLease::Positive(Duration::from_secs(3))
        );
    }

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

    /// Go `ddl.Start`: `Instance.TiDBEnableDDL` defaults on and `--run-ddl`
    /// (main.go:751) overrides it, so a node can be kept out of the DDL
    /// owner election.
    #[test]
    fn run_ddl_defaults_on_and_follows_the_main_go_flag() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ];
        assert!(NodeConfig::parse(base).unwrap().run_ddl);
        let with_flag = |flag: &'static str| {
            NodeConfig::parse(base.iter().copied().chain([flag]).collect::<Vec<_>>())
                .unwrap()
                .run_ddl
        };
        assert!(!with_flag("--run-ddl=false"));
        assert!(with_flag("--run-ddl=true"));
    }

    #[test]
    fn affinity_cpu_list_matches_the_source_command_line_parser() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/users.tsv",
        ];
        assert!(NodeConfig::parse(base).unwrap().affinity_cpus.is_empty());
        assert_eq!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain(["--affinity-cpus", " 1, ,3,-1 "])
                    .collect::<Vec<_>>(),
            )
            .unwrap()
            .affinity_cpus,
            [1, 3, -1]
        );
        assert!(matches!(
            NodeConfig::parse(
                base.iter()
                    .copied()
                    .chain(["--affinity-cpus", "1,nope"])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::InvalidValue { option, .. })
                if option == "--affinity-cpus"
        ));
    }

    #[test]
    fn store_unistore_is_accepted_without_a_pd_path() {
        // Go's unistore arm has no PD to dial, so `--path` is not required.
        let config = NodeConfig::parse([
            "tidb-server",
            "--store",
            "unistore",
            "--auth-file",
            "/tmp/users.tsv",
        ])
        .expect("unistore parses without --path");
        assert_eq!(config.store_kind, StoreKind::Unistore);
        assert!(config.pd_endpoints.is_empty());
    }

    #[test]
    fn store_tikv_still_requires_the_pd_path() {
        let err = NodeConfig::parse([
            "tidb-server",
            "--store",
            "tikv",
            "--auth-file",
            "/tmp/users.tsv",
        ])
        .expect_err("tikv without --path refuses");
        assert!(format!("{err}").contains("--path"));
    }

    #[test]
    fn an_unknown_store_names_both_executables() {
        let err = NodeConfig::parse([
            "tidb-server",
            "--store",
            "mocktikv",
            "--path",
            "127.0.0.1:2379",
        ])
        .expect_err("unknown store refuses");
        assert!(format!("{err}").contains("tikv and unistore are executable"));
    }
}
