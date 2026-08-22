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

//! Bounded startup configuration for the first standalone Rust SQL node.
//!
//! The source TiDB binary accepts a much larger configuration surface. This
//! milestone admits only the values consumed by the executable read path: one
//! loopback TCP listener, plaintext PD seeds, and one ordered list of
//! signed-BIGINT table-column catalogs. Unknown or duplicate options fail startup so an
//! operator cannot believe an unsupported TiDB setting was applied.

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use tidb_config::config_tree::Config as SourceConfig;
use tidb_config::configtypes::parse_go_duration;
use tidb_config::{deploymode, kerneltype};
use tidb_pd_client::ClusterSecurity;
use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;
use tidb_util::disk::{SpillEncryptionMethod, SpillStorageSpec};
use tidb_util::versioninfo::VersionInfo;

const DEFAULT_MAX_CONNECTIONS: usize = 8;
const MAX_CONNECTION_WORKERS: usize = 256;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 30_000;
// The current configured reader only exposes fixed-width signed BIGINT rows.
// Keep the first in-memory TopN vertical deliberately small until the executor
// owns spill and general memory-quota semantics.
const DEFAULT_MAX_TOPN_ROWS: usize = 1_024;
/// Go's `tidb-server --lease` default: the DDL schema lease. The catalog
/// reload thread ticks at half of it, matching Go's domain reload loop.
const DEFAULT_SCHEMA_LEASE_MS: u64 = 45_000;
const MAX_CONFIGURED_TOPN_ROWS: usize = 65_536;
// The benchmark gate serves a full 32-table sysbench schema, so the loaded
// catalog must scale past the original campaign-sized surface. The per-column
// and per-index caps still bound each table's shape.
const MAX_CONFIGURED_READ_TABLES: usize = 4_096;
const MAX_CONFIGURED_READ_COLUMNS: usize = 4096;
const MAX_CONFIGURED_READ_INDEXES: usize = 64;

/// Storage shape of one configured column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredReadColumnKind {
    /// The table's sole signed integer clustered primary key.
    ClusteredPrimaryKey,
    /// A signed `BIGINT` stored non-null column decoded from the TiKV row payload.
    StoredNotNull,
    /// A signed `INT` (int32-domain) stored non-null column.
    StoredIntNotNull,
    /// A `CHAR(max_length)` stored non-null column (utf8mb4 bytes).
    StoredCharNotNull {
        /// Declared character length, per the SQL `CHAR(N)` width.
        max_length: u32,
    },
}

/// One atomic configured column descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadColumn {
    /// Table-visible column name.
    pub name: String,
    /// Stable column identifier from TiDB schema metadata.
    pub id: i64,
    /// Physical storage role admitted by this milestone.
    pub kind: ConfiguredReadColumnKind,
}

/// One configured secondary index descriptor over a single stored column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadIndex {
    /// Index-visible name, retained for diagnostics.
    pub name: String,
    /// Stable index identifier from TiDB schema metadata.
    pub index_id: i64,
    /// Stable identifier of the single indexed column.
    pub column_id: i64,
    /// Whether the index key enforces uniqueness.
    pub unique: bool,
}

/// One table shape admitted by the deployable read-only node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadTable {
    /// Schema name matched case-insensitively by the bounded planner.
    pub database: String,
    /// Table name matched case-insensitively by the bounded planner.
    pub table: String,
    /// Physical TiKV table identifier resolved by the fixture/owner.
    pub table_id: i64,
    /// Checked columns in configured order.
    pub columns: Vec<ConfiguredReadColumn>,
    /// Secondary indexes maintained by the write path, in configured order.
    /// Empty for a table without any declared index.
    pub indexes: Vec<ConfiguredReadIndex>,
}

/// One `<database>.<table>` name whose schema the node reads from the cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedTableName {
    /// Database name, matched case-insensitively against the stored catalog.
    pub database: String,
    /// Table name, matched case-insensitively against the stored catalog.
    pub table: String,
}

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

/// Complete startup input consumed by the concurrent SQL node.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Checked tables exposed to the bounded planner in command-line order.
    pub read_tables: Vec<ConfiguredReadTable>,
    /// Tables whose schema is read from the cluster's own stored catalog at
    /// startup instead of being described on the command line, as
    /// `<database>.<table>` pairs in command-line order.
    pub load_tables: Vec<LoadedTableName>,
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
    /// Serve the wide-SQL session driver over the whole cluster catalog
    /// instead of the bounded one- or two-table read surface.
    ///
    /// This mode names no table at all: it reads the cluster's entire stored
    /// catalog at boot, keeps following it, and gives every connection a
    /// session whose tables read and write through real transactions. It is
    /// therefore incompatible with `--read-table`/`--load-table`, which
    /// describe the bounded surface.
    pub cluster_session: bool,
    /// Fixed connection-worker count and accepted-socket queue capacity.
    pub max_connections: usize,
    /// Handshake, idle-command, and socket-write deadline for one connection.
    pub connection_timeout: Duration,
    /// Process-wide maximum ORDER BY LIMIT heap cardinality for the bounded executor.
    pub max_topn_rows: usize,
    /// Maximum recent deadlock records retained process-wide.
    pub deadlock_history_capacity: usize,
    /// Whether retryable in-statement deadlocks are retained.
    pub deadlock_history_collect_retryable: bool,
    /// DDL schema lease. A node that loaded its schema from the cluster
    /// re-reads the catalog every `schema_lease / 2`, so it is never more than
    /// one lease behind the cluster's schema version.
    pub schema_lease: Duration,
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
    /// TiDB's `[security] skip-grant-table`, accepted only when the process
    /// effective uid passes the source root-only validation.
    pub skip_grant_table: bool,
    /// Generate a self-signed certificate when no `--ssl-cert`/`--ssl-key` is
    /// configured, as TiDB's `[security] auto-tls` does.
    ///
    /// On by default, unlike `pkg/config`'s own `false`: the TiUP playground
    /// Go server this node is compared against runs with it enabled, and a
    /// MySQL port with no `CLIENT_SSL` is refused outright by clients that
    /// link MariaDB Connector/C (which requires the bit even under
    /// `--mysql-ssl=off`). `--no-auto-tls` restores the plaintext-only port.
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
    /// Coherent build identity plus the optional startup edition override.
    pub version_info: VersionInfo,
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
    /// The source config recognizes these leaves, but this node has no owner
    /// for their behavior and therefore refuses to pretend to honor them.
    UnsupportedConfigOptions(Vec<String>),
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
            Self::UnsupportedConfigOptions(options) => write!(
                formatter,
                "unsupported config options: {}",
                options.join(", ")
            ),
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

const SUPPORTED_CONFIG_LEAVES: &[&str] = &[
    "host",
    "instance.max_connections",
    "instance.tidb_mem_arbitrator_mode",
    "instance.tidb_mem_arbitrator_soft_limit",
    "instance.tidb_server_memory_limit",
    "lease",
    "max-allowed-packet",
    "path",
    "pessimistic-txn.deadlock-history-capacity",
    "pessimistic-txn.deadlock-history-collect-retryable",
    "port",
    "security.auto-tls",
    "security.cluster-ssl-ca",
    "security.cluster-ssl-cert",
    "security.cluster-ssl-key",
    "security.disconnect-on-expired-password",
    "security.enable-sem",
    "security.skip-grant-table",
    "security.spilled-file-encryption-method",
    "security.ssl-cert",
    "security.ssl-key",
    "server-version",
    "store",
    "tidb-edition",
    "tidb-release-version",
    "tmp-storage-path",
    "tmp-storage-quota",
];

struct LoadedSourceConfig {
    config: SourceConfig,
    defined: BTreeSet<String>,
}

impl LoadedSourceConfig {
    fn is_defined(&self, key: &str) -> bool {
        self.defined.contains(key)
    }
}

fn load_source_config(path: &str) -> Result<LoadedSourceConfig, NodeConfigError> {
    let path = PathBuf::from(path);
    let text = fs::read_to_string(&path).map_err(|error| NodeConfigError::ConfigFile {
        path: path.clone(),
        reason: error.to_string(),
    })?;
    let table: toml::Table =
        toml::from_str(&text).map_err(|error| NodeConfigError::ConfigFile {
            path: path.clone(),
            reason: error.to_string(),
        })?;
    let mut config = SourceConfig::default();
    config
        .load_str(path.to_string_lossy().as_ref(), &text)
        .map_err(|error| NodeConfigError::ConfigFile {
            path: path.clone(),
            reason: error.to_string(),
        })?;
    config
        .removed_variable_check(&text)
        .map_err(|reason| NodeConfigError::ConfigFile {
            path: path.clone(),
            reason,
        })?;

    let mut defined = BTreeSet::new();
    collect_toml_leaves(&table, "", &mut defined);
    let supported: BTreeSet<&str> = SUPPORTED_CONFIG_LEAVES.iter().copied().collect();
    let unsupported = defined
        .iter()
        .filter(|key| !supported.contains(key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(NodeConfigError::UnsupportedConfigOptions(unsupported));
    }
    Ok(LoadedSourceConfig { config, defined })
}

fn collect_toml_leaves(table: &toml::Table, prefix: &str, leaves: &mut BTreeSet<String>) {
    for (name, value) in table {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        match value {
            toml::Value::Table(nested) => collect_toml_leaves(nested, &path, leaves),
            _ => {
                leaves.insert(path);
            }
        }
    }
}

fn parse_file_schema_lease(value: &str) -> Result<Duration, NodeConfigError> {
    let nanos = parse_go_duration(value)
        .or_else(|_| parse_go_duration(&format!("{value}s")))
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

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn resolve_spill_storage(
    configured_base: Option<&str>,
    quota_bytes: i64,
    encryption: SpillEncryptionMethod,
    host: IpAddr,
    port: u16,
) -> SpillStorageSpec {
    let os_temp = std::env::temp_dir();
    let source_default = encoded_spill_path(&os_temp, "0.0.0.0", 4000);
    let base = match configured_base {
        None => os_temp,
        Some(path) if std::path::Path::new(path) == source_default => std::env::temp_dir(),
        Some(path) => PathBuf::from(path),
    };
    SpillStorageSpec {
        path: encoded_spill_path(&base, &host.to_string(), port),
        quota_bytes,
        encryption,
    }
}

fn encoded_spill_path(base: &std::path::Path, host: &str, port: u16) -> PathBuf {
    #[cfg(unix)]
    let uid = rustix::process::getuid().as_raw().to_string();
    #[cfg(not(unix))]
    let uid = String::new();
    encoded_spill_path_for_identity(base, host, port, "0.0.0.0", 10080, &uid)
}

fn encoded_spill_path_for_identity(
    base: &std::path::Path,
    host: &str,
    port: u16,
    status_host: &str,
    status_port: u16,
    uid: &str,
) -> PathBuf {
    let identity = format!("{host}:{port}/{status_host}:{status_port}");
    let encoded = URL_SAFE.encode(identity.as_bytes());
    base.join(format!("{uid}_tidb"))
        .join(encoded)
        .join("tmp-storage")
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
        let (main_flags, remaining) = crate::main_flags::extract_main_go_flags(arguments.collect())
            .map_err(|error| invalid("main.go flags", &error.to_string()))?;
        let mut pending = remaining.into_iter().peekable();

        let mut host = None;
        let mut port = None;
        let mut affinity_cpus = None;
        let mut path = None;
        let mut store = None;
        let mut read_tables = Vec::new();
        let mut load_tables = Vec::new();
        let mut max_allowed_packet = None;
        let mut auth_file = None;
        let mut max_connections = None;
        let mut connection_timeout_ms = None;
        let mut max_topn_rows = None;
        let mut schema_lease_ms = None;
        let mut load_privileges = false;
        let mut cluster_session = false;
        let mut ssl_cert = None;
        let mut ssl_key = None;
        let mut no_auto_tls = false;
        let mut no_disconnect_on_expired_password = false;
        let mut cluster_ssl_ca = None;
        let mut cluster_ssl_cert = None;
        let mut cluster_ssl_key = None;
        let mut config_path = None;
        let mut tidb_edition = None;
        let mut tidb_release_version = None;
        let mut server_version = None;
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
                if cluster_session {
                    return Err(NodeConfigError::DuplicateOption(argument));
                }
                cluster_session = true;
                continue;
            }
            if argument == "--read-table" {
                read_tables.push(parse_read_table(&mut pending)?);
                continue;
            }
            if argument == "--load-table" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue("--load-table".to_owned()))?;
                load_tables.push(parse_loaded_table_name(&value)?);
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
                "--read-table" => {
                    return Err(invalid(
                        option,
                        "expected separate <database> <table> <table-id> <column-count> values",
                    ));
                }
                "--max-allowed-packet" => {
                    set_once(&mut max_allowed_packet, option, value)?;
                }
                "--auth-file" => set_once(&mut auth_file, option, value)?,
                "--max-connections" => set_once(&mut max_connections, option, value)?,
                "--connection-timeout-ms" => {
                    set_once(&mut connection_timeout_ms, option, value)?;
                }
                "--load-table" => load_tables.push(parse_loaded_table_name(&value)?),
                "--max-topn-rows" => set_once(&mut max_topn_rows, option, value)?,
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

        let mut source = config_path.as_deref().map(load_source_config).transpose()?;
        let mut file_schema_lease = None;
        let mut temp_storage_base = None;
        let mut temp_storage_quota = -1;
        let mut spill_encryption = SpillEncryptionMethod::Plaintext;
        let mut file_auto_tls = None;
        let mut file_disconnect_on_expired_password = None;
        let mut sem_enabled = false;
        let mut memory_arbitrator = MemoryArbitratorConfig {
            server_memory_limit: "80%".to_owned(),
            mode: "disable".to_owned(),
            soft_limit: "0".to_owned(),
        };
        let defaults = SourceConfig::default();
        let mut deadlock_history_capacity =
            usize::try_from(defaults.pessimistic_txn.deadlock_history_capacity)
                .expect("source deadlock-history default fits usize");
        let mut deadlock_history_collect_retryable =
            defaults.pessimistic_txn.deadlock_history_collect_retryable;
        let mut file_skip_grant_table = false;
        if let Some(loaded) = source.as_ref() {
            let config = &loaded.config;
            if host.is_none() && loaded.is_defined("host") {
                host = Some(config.host.clone());
            }
            if port.is_none() && loaded.is_defined("port") {
                port = Some(config.port.to_string());
            }
            if path.is_none() && loaded.is_defined("path") {
                path = Some(config.path.clone());
            }
            if store.is_none() && loaded.is_defined("store") {
                store = Some(config.store.0.clone());
            }
            if max_allowed_packet.is_none() && loaded.is_defined("max-allowed-packet") {
                max_allowed_packet = Some(config.max_allowed_packet.to_string());
            }
            if max_connections.is_none() && loaded.is_defined("instance.max_connections") {
                max_connections = Some(config.instance.max_connections.to_string());
            }
            if loaded.is_defined("instance.tidb_server_memory_limit") {
                memory_arbitrator.server_memory_limit = config.instance.server_memory_limit.clone();
            }
            if loaded.is_defined("instance.tidb_mem_arbitrator_mode") {
                memory_arbitrator.mode = config.instance.mem_arbitrator_mode.clone();
            }
            if loaded.is_defined("instance.tidb_mem_arbitrator_soft_limit") {
                memory_arbitrator.soft_limit = config.instance.mem_arbitrator_soft_limit.clone();
            }
            if schema_lease_ms.is_none() && loaded.is_defined("lease") {
                file_schema_lease = Some(parse_file_schema_lease(&config.lease)?);
            }
            if ssl_cert.is_none() && loaded.is_defined("security.ssl-cert") {
                ssl_cert = nonempty(config.security.ssl_cert.clone());
            }
            if ssl_key.is_none() && loaded.is_defined("security.ssl-key") {
                ssl_key = nonempty(config.security.ssl_key.clone());
            }
            if cluster_ssl_ca.is_none() && loaded.is_defined("security.cluster-ssl-ca") {
                cluster_ssl_ca = nonempty(config.security.cluster_ssl_ca.clone());
            }
            if cluster_ssl_cert.is_none() && loaded.is_defined("security.cluster-ssl-cert") {
                cluster_ssl_cert = nonempty(config.security.cluster_ssl_cert.clone());
            }
            if cluster_ssl_key.is_none() && loaded.is_defined("security.cluster-ssl-key") {
                cluster_ssl_key = nonempty(config.security.cluster_ssl_key.clone());
            }
            if loaded.is_defined("security.auto-tls") {
                file_auto_tls = Some(config.security.auto_tls);
            }
            if loaded.is_defined("security.disconnect-on-expired-password") {
                file_disconnect_on_expired_password =
                    Some(config.security.disconnect_on_expired_password);
            }
            if loaded.is_defined("security.enable-sem") {
                sem_enabled = config.security.enable_sem;
            }
            if loaded.is_defined("pessimistic-txn.deadlock-history-capacity") {
                deadlock_history_capacity = usize::try_from(
                    config.pessimistic_txn.deadlock_history_capacity,
                )
                .map_err(|_| {
                    invalid(
                        "pessimistic-txn.deadlock-history-capacity",
                        "value does not fit this platform",
                    )
                })?;
            }
            if loaded.is_defined("pessimistic-txn.deadlock-history-collect-retryable") {
                deadlock_history_collect_retryable =
                    config.pessimistic_txn.deadlock_history_collect_retryable;
            }
            if loaded.is_defined("security.skip-grant-table") {
                file_skip_grant_table = config.security.skip_grant_table;
            }
            if loaded.is_defined("tmp-storage-path") {
                temp_storage_base = Some(config.temp_storage_path.clone());
            }
            if loaded.is_defined("tmp-storage-quota") {
                temp_storage_quota = config.temp_storage_quota;
            }
            if loaded.is_defined("security.spilled-file-encryption-method") {
                spill_encryption = config
                    .security
                    .spilled_file_encryption_method
                    .parse::<SpillEncryptionMethod>()
                    .map_err(|error| {
                        invalid(
                            "security.spilled-file-encryption-method",
                            &error.to_string(),
                        )
                    })?;
            }
            if loaded.is_defined("tidb-edition") {
                tidb_edition = Some(config.tidb_edition.clone());
            }
            if loaded.is_defined("tidb-release-version") {
                tidb_release_version = Some(config.tidb_release_version.clone());
            }
            if loaded.is_defined("server-version") {
                server_version = Some(config.server_version.clone());
            }
        }

        let host = parse_ip("--host", host.as_deref().unwrap_or("127.0.0.1"))?;
        // Cluster privilege mode is used behind TiProxy on a private network.
        // The configured auth-file mode keeps the original loopback-only
        // boundary because it has no cluster-backed account lifecycle.
        if !host.is_loopback() && !load_privileges && !file_skip_grant_table {
            return Err(NodeConfigError::NonLoopbackHost(host));
        }
        let port = parse_number("--port", port.as_deref().unwrap_or("4000"))?;
        let affinity_cpus = parse_affinity_cpus(affinity_cpus.as_deref().unwrap_or_default())?;
        let store = store.as_deref().unwrap_or("tikv");
        // Go `main.go` registers tikv, unistore and mocktikv; this
        // executable constructs the first two, and anything else refuses.
        let store_kind = if store.eq_ignore_ascii_case("tikv") {
            StoreKind::TiKv
        } else if store.eq_ignore_ascii_case("unistore") {
            StoreKind::Unistore
        } else {
            return Err(NodeConfigError::UnsupportedStore(store.to_owned()));
        };
        // Go's unistore path ignores `--path` (the embedded store has no PD
        // to dial); the tikv path requires it exactly as before.
        let pd_endpoints = match store_kind {
            StoreKind::TiKv => parse_pd_endpoints(required(path, "--path")?)?,
            StoreKind::Unistore => Vec::new(),
        };
        if cluster_session {
            // The bounded surface is described by naming tables; this one is
            // the cluster's whole catalog. Accepting both would leave two
            // answers to "what does this node serve".
            if !read_tables.is_empty() || !load_tables.is_empty() {
                return Err(invalid(
                    "--cluster-session",
                    "cannot be combined with --read-table or --load-table; this mode serves the \
                     cluster's whole loaded catalog",
                ));
            }
        } else {
            validate_read_tables(&read_tables, &load_tables)?;
            validate_load_tables(&read_tables, &load_tables)?;
        }
        let max_allowed_packet = match max_allowed_packet {
            Some(value) => parse_positive_number("--max-allowed-packet", &value)?,
            None => DEFAULT_MAX_ALLOWED_PACKET,
        };
        // Exactly one account source in ordinary mode. Skip-grant-table is a
        // recovery mode that deliberately uses neither source, matching Go's
        // decision not to start the privilege loader at all.
        let auth_file = match (auth_file, load_privileges, file_skip_grant_table) {
            (_, _, true) => PathBuf::new(),
            (Some(_), true, false) => {
                return Err(invalid(
                    "--load-privileges",
                    "cannot be combined with --auth-file; the cluster's mysql.* is then \
                     the only account source",
                ))
            }
            (Some(file), false, false) => PathBuf::from(file),
            (None, true, false) => PathBuf::new(),
            (None, false, false) => return Err(NodeConfigError::MissingOption("--auth-file")),
        };
        let max_connections = match max_connections {
            Some(value) => parse_positive_number("--max-connections", &value)?,
            None => DEFAULT_MAX_CONNECTIONS,
        };
        if max_connections > MAX_CONNECTION_WORKERS {
            return Err(invalid("--max-connections", "value must not exceed 256"));
        }
        let connection_timeout = Duration::from_millis(match connection_timeout_ms {
            Some(value) => parse_positive_number("--connection-timeout-ms", &value)?,
            None => DEFAULT_CONNECTION_TIMEOUT_MS,
        });
        let max_topn_rows = match max_topn_rows {
            Some(value) => parse_positive_number("--max-topn-rows", &value)?,
            None => DEFAULT_MAX_TOPN_ROWS,
        };
        if max_topn_rows > MAX_CONFIGURED_TOPN_ROWS {
            return Err(invalid("--max-topn-rows", "value must not exceed 65536"));
        }
        // A zero lease would make the reload thread spin; the parser rejects it
        // here so the node never has to.
        let schema_lease = match schema_lease_ms.as_deref() {
            Some(value) => Duration::from_millis(parse_positive_number("--lease-ms", value)?),
            // Go's own spelling: `--lease 45s` (main.go's ddl-lease flag)
            // drives the same schema lease, written flags overriding the
            // config file exactly as overrideConfig orders them.
            None => match main_flags.ddl_lease.as_deref() {
                Some(lease) => parse_file_schema_lease(lease)?,
                None => file_schema_lease
                    .unwrap_or_else(|| Duration::from_millis(DEFAULT_SCHEMA_LEASE_MS)),
            },
        };
        let auto_tls = if no_auto_tls {
            false
        } else {
            file_auto_tls.unwrap_or(true)
        };
        let disconnect_on_expired_password = if no_disconnect_on_expired_password {
            false
        } else {
            file_disconnect_on_expired_password.unwrap_or(true)
        };
        let cluster_security = build_cluster_security(
            cluster_ssl_ca.clone(),
            cluster_ssl_cert.clone(),
            cluster_ssl_key.clone(),
        )?;
        if let Some(loaded) = source.as_mut() {
            let config = &mut loaded.config;
            config.host = host.to_string();
            config.port = u32::from(port);
            config.store = tidb_config::store::StoreType("tikv".to_owned());
            config.path = pd_endpoints.join(",");
            config.max_allowed_packet = u64::try_from(max_allowed_packet).unwrap_or(u64::MAX);
            config.instance.max_connections =
                u32::try_from(max_connections).expect("bounded worker count fits u32");
            config.instance.server_memory_limit = memory_arbitrator.server_memory_limit.clone();
            config.instance.mem_arbitrator_mode = memory_arbitrator.mode.clone();
            config.instance.mem_arbitrator_soft_limit = memory_arbitrator.soft_limit.clone();
            if let Some(value) = schema_lease_ms.as_deref() {
                config.lease = format!("{value}ms");
            }
            config.security.ssl_cert = ssl_cert.clone().unwrap_or_default();
            config.security.ssl_key = ssl_key.clone().unwrap_or_default();
            config.security.auto_tls = auto_tls;
            config.security.disconnect_on_expired_password = disconnect_on_expired_password;
            config.security.enable_sem = sem_enabled;
            config.security.skip_grant_table = file_skip_grant_table;
            config.security.cluster_ssl_ca = cluster_ssl_ca.clone().unwrap_or_default();
            config.security.cluster_ssl_cert = cluster_ssl_cert.clone().unwrap_or_default();
            config.security.cluster_ssl_key = cluster_ssl_key.clone().unwrap_or_default();
            config.security.spilled_file_encryption_method =
                spill_encryption.as_config_value().to_owned();
            config.temp_storage_quota = temp_storage_quota;
            config.temp_storage_path = temp_storage_base.clone().unwrap_or_default();
            config
                .valid()
                .map_err(|reason| invalid("--config", &reason))?;
        }
        let spill_storage = resolve_spill_storage(
            temp_storage_base.as_deref(),
            temp_storage_quota,
            spill_encryption,
            host,
            port,
        );
        let deploy_mode = if kerneltype::is_next_gen() {
            Some(source.as_ref().map_or_else(
                || deploymode::get().to_string(),
                |loaded| loaded.config.deploy_mode.to_string(),
            ))
        } else {
            None
        };
        // Go `overrideConfig`: the flag wins; otherwise the bind host
        // stands in unless it is the wildcard, in which case Go defers to
        // a local-IP lookup the node performs later. The wildcard arm is
        // unreachable while this tier refuses a non-loopback bind, and is
        // kept because that refusal is the thing that will lift.
        let advertise_address = match main_flags.advertise_address.as_deref() {
            Some(advertise) if advertise.split(' ').count() > 1 => {
                return Err(invalid(
                    "--advertise-address",
                    "Only support one advertise-address",
                ));
            }
            Some(advertise) => advertise.to_owned(),
            None if host.to_string() != "0.0.0.0" => host.to_string(),
            None => String::new(),
        };
        let version_info = configured_version_info(
            tidb_edition.as_deref().unwrap_or_default(),
            tidb_release_version.as_deref().unwrap_or_default(),
            server_version.as_deref().unwrap_or_default(),
            store,
            deploy_mode,
        )?;

        Ok(Self {
            report_status: main_flags.report_status.unwrap_or(true),
            status_host: main_flags
                .status_host
                .clone()
                .unwrap_or_else(|| "0.0.0.0".to_owned()),
            status_port: main_flags
                .status_port
                .as_deref()
                .map(|port| {
                    port.parse::<u16>()
                        .map_err(|_| invalid("--status", "expected a port number"))
                })
                .transpose()?
                .unwrap_or(10080),
            // Go's config default plus `setGlobalVars`' one `{Port}`
            // replacement (`main.go:1109`).
            socket: socket
                .unwrap_or_else(|| "/tmp/tidb-{Port}.sock".to_owned())
                .replacen("{Port}", &port.to_string(), 1),
            isolation_read_engines: vec![
                "tikv".to_owned(),
                "tiflash".to_owned(),
                "tidb".to_owned(),
            ],
            host,
            advertise_address,
            port,
            affinity_cpus,
            store_kind,
            pd_endpoints,
            read_tables,
            load_tables,
            max_allowed_packet,
            auth_file,
            max_connections,
            connection_timeout,
            max_topn_rows,
            deadlock_history_capacity,
            deadlock_history_collect_retryable,
            schema_lease,
            load_privileges,
            cluster_session,
            ssl_cert: ssl_cert.map(PathBuf::from),
            ssl_key: ssl_key.map(PathBuf::from),
            auto_tls,
            disconnect_on_expired_password,
            sem_enabled,
            skip_grant_table: file_skip_grant_table,
            cluster_security,
            spill_storage,
            memory_arbitrator,
            version_info,
        })
    }

    /// Builds the identity printed by `-V` without requiring a runnable node topology.
    pub fn version_info_for_display<I, S>(arguments: I) -> Result<VersionInfo, NodeConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut arguments = arguments.into_iter().map(Into::into);
        let _program = arguments.next();
        // main.go's own flag surface is consumed FIRST, so every Go spelling
        // is accepted exactly as initFlagSet accepts it; the node's options
        // remain for the loop below, untouched and in order.
        let (main_flags, remaining) = crate::main_flags::extract_main_go_flags(arguments.collect())
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

        let source = config_path.as_deref().map(load_source_config).transpose()?;
        let defaults = SourceConfig::default();
        let config = source.as_ref().map_or(&defaults, |loaded| &loaded.config);
        let deploy_mode = kerneltype::is_next_gen().then(|| config.deploy_mode.to_string());
        configured_version_info(
            &config.tidb_edition,
            &config.tidb_release_version,
            &config.server_version,
            store.as_deref().unwrap_or(&config.store.0),
            deploy_mode,
        )
    }

    /// JSON projection of every startup value this bounded node owns.
    pub(crate) fn startup_config_json(&self) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "host": self.host.to_string(),
            "port": self.port,
            "path": self.pd_endpoints.join(","),
            "store": &self.version_info.store,
            "max-allowed-packet": self.max_allowed_packet,
            "instance": {
                "max_connections": self.max_connections,
                "tidb_server_memory_limit": self.memory_arbitrator.server_memory_limit,
                "tidb_mem_arbitrator_mode": self.memory_arbitrator.mode,
                "tidb_mem_arbitrator_soft_limit": self.memory_arbitrator.soft_limit,
            },
            "lease-ms": u64::try_from(self.schema_lease.as_millis()).unwrap_or(u64::MAX),
            "cluster-session": self.cluster_session,
            "load-privileges": self.load_privileges,
            "security": {
                "auto-tls": self.auto_tls,
                "disconnect-on-expired-password": self.disconnect_on_expired_password,
                "enable-sem": self.sem_enabled,
                "skip-grant-table": self.skip_grant_table,
                "ssl-cert": self.ssl_cert.as_ref().map(|path| path.to_string_lossy().into_owned()),
                "ssl-key": self.ssl_key.as_ref().map(|path| path.to_string_lossy().into_owned()),
            },
        }))
        .expect("owned startup config projection is serializable")
    }

    /// Stable usage text printed by the executable for `--help`.
    #[must_use]
    pub const fn help_text() -> &'static str {
        "Usage: tidb-server [-V] [--config <tidb.toml>] --path <pd[,pd...]> \
[--read-table <database> <table> <table-id> <column-count> \
<name>:<id>:<clustered-pk|stored-not-null> \
[<name>:<id>:<clustered-pk|stored-not-null> ...]] \
[--read-table <database> <table> <table-id> <column-count> <column> ...] \
[--load-table <database>.<table> ...] [--cluster-session] \
[--max-connections <count>] [--connection-timeout-ms <milliseconds>] \
[--max-topn-rows <rows>] [--lease-ms <milliseconds>] \
[--auth-file <mode-0600-tsv> | --load-privileges] \
[--host <listen-ip>] [-P <port>|--port <port>] [--store tikv] \
[--affinity-cpus <cpu[,cpu...]>] \
[--max-allowed-packet <bytes>] \
[--ssl-cert <cert-pem> --ssl-key <key-pem>] [--no-auto-tls] \
[--no-disconnect-on-expired-password] \
[--cluster-ssl-ca <ca-pem> [--cluster-ssl-cert <cert-pem> --cluster-ssl-key <key-pem>]]"
    }
}

fn configured_version_info(
    edition: &str,
    release_version: &str,
    server_version: &str,
    store: &str,
    deploy_mode: Option<String>,
) -> Result<VersionInfo, NodeConfigError> {
    let mut info = VersionInfo::build_default();
    if kerneltype::is_next_gen() {
        if !edition.is_empty() || !release_version.is_empty() || !server_version.is_empty() {
            return Err(invalid(
                "--config",
                "config options tidb-edition, tidb-release-version and server-version are not \
                 allowed to set in nextgen kernel",
            ));
        }
        let component =
            tidb_mysql::normalize_tidb_release_version_for_next_gen(&info.release_version)
                .to_owned();
        let server_version = tidb_mysql::build_tidbx_server_version(&component)
            .map_err(|error| invalid("--config", &error.to_string()))?;
        info = info.with_configured_versions(&component, &server_version);
    } else {
        info = info
            .with_configured_edition(edition)
            .with_configured_versions(release_version, server_version);
    }
    Ok(info.with_runtime_environment(
        tidb_config::config_tree::config::check_table_before_drop(),
        store,
        kerneltype::name(),
        deploy_mode,
    ))
}

fn parse_read_table<I>(
    arguments: &mut std::iter::Peekable<I>,
) -> Result<ConfiguredReadTable, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    let database = parse_identifier("--read-table", next_read_table_value(arguments)?)?;
    let table = parse_identifier("--read-table", next_read_table_value(arguments)?)?;
    let table_id = parse_positive_id("--read-table", next_read_table_value(arguments)?)?;
    let column_count: usize =
        parse_positive_number("--read-table", &next_read_table_value(arguments)?)?;
    if column_count > MAX_CONFIGURED_READ_COLUMNS {
        return Err(invalid("--read-table", "column count must not exceed 4096"));
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(parse_column_descriptor(
            "--read-table",
            next_read_table_value(arguments)?,
        )?);
    }
    validate_columns("--read-table", &columns)?;
    let indexes = parse_optional_indexes("--read-table", arguments, &columns)?;
    Ok(ConfiguredReadTable {
        database,
        table,
        table_id,
        columns,
        indexes,
    })
}

/// Parses the optional trailing secondary-index section of a `--read-table`.
///
/// The section is backward compatible: a table with no index simply omits it,
/// so parsing stops as soon as the next token is another option or the end of
/// the arguments. When present, one count precedes that many
/// `name:index_id:column_id[:unique]` descriptors, each over an existing
/// column. The optional `unique` suffix is explicit so existing command lines
/// retain their non-unique meaning.
fn parse_optional_indexes<I>(
    option: &str,
    arguments: &mut std::iter::Peekable<I>,
    columns: &[ConfiguredReadColumn],
) -> Result<Vec<ConfiguredReadIndex>, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    let has_index_section = matches!(arguments.peek(), Some(value) if !value.starts_with('-'));
    if !has_index_section {
        return Ok(Vec::new());
    }
    let index_count: usize = parse_number(option, &next_read_table_value(arguments)?)?;
    if index_count > MAX_CONFIGURED_READ_INDEXES {
        return Err(invalid(option, "index count must not exceed 64"));
    }
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        indexes.push(parse_index_descriptor(
            option,
            next_read_table_value(arguments)?,
            columns,
        )?);
    }
    Ok(indexes)
}

/// Parses one `name:index_id:column_id[:unique]` index descriptor.
fn parse_index_descriptor(
    option: &str,
    value: String,
    columns: &[ConfiguredReadColumn],
) -> Result<ConfiguredReadIndex, NodeConfigError> {
    let fields: Vec<&str> = value.split(':').collect();
    let (name, index_id, column_id, unique) = match fields.as_slice() {
        [name, index_id, column_id] => (*name, *index_id, *column_id, false),
        [name, index_id, column_id, "unique"] => (*name, *index_id, *column_id, true),
        _ => {
            return Err(invalid(
                option,
                "index descriptor must be name:index_id:column_id[:unique]",
            ));
        }
    };
    let name = parse_identifier(option, name.to_owned())?;
    let index_id = parse_positive_id(option, index_id.to_owned())?;
    let column_id = parse_positive_id(option, column_id.to_owned())?;
    if !columns.iter().any(|column| column.id == column_id) {
        return Err(invalid(
            option,
            "index column id does not match any configured column",
        ));
    }
    Ok(ConfiguredReadIndex {
        name,
        index_id,
        column_id,
        unique,
    })
}

fn next_read_table_value<I>(
    arguments: &mut std::iter::Peekable<I>,
) -> Result<String, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    match arguments.peek() {
        Some(value) if !value.starts_with('-') => Ok(arguments
            .next()
            .expect("peeked read-table value must remain available")),
        _ => Err(NodeConfigError::MissingValue("--read-table".to_owned())),
    }
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

fn required(value: Option<String>, option: &'static str) -> Result<String, NodeConfigError> {
    value.ok_or(NodeConfigError::MissingOption(option))
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

fn parse_positive_id(option: &str, value: String) -> Result<i64, NodeConfigError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| invalid(option, "expected a signed decimal integer"))?;
    if parsed <= 0 {
        return Err(invalid(option, "value must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_identifier(option: &str, value: String) -> Result<String, NodeConfigError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(invalid(
            option,
            "identifier must be nonempty and contain no NUL",
        ));
    }
    Ok(value)
}

fn parse_column_descriptor(
    option: &str,
    value: String,
) -> Result<ConfiguredReadColumn, NodeConfigError> {
    let mut fields = value.split(':');
    let (Some(name), Some(id), Some(kind)) = (fields.next(), fields.next(), fields.next()) else {
        return Err(invalid(
            option,
            "expected <name>:<id>:<kind>[:<char-length>]",
        ));
    };
    // Only `stored-char-not-null` carries a fourth `:<char-length>` field.
    let extra = fields.next();
    if fields.next().is_some() {
        return Err(invalid(
            option,
            "too many ':'-separated fields in column descriptor",
        ));
    }
    let name = parse_identifier(option, name.to_owned())?;
    let id = parse_positive_id(option, id.to_owned())?;
    let kind = match (kind, extra) {
        ("clustered-pk", None) => ConfiguredReadColumnKind::ClusteredPrimaryKey,
        ("stored-not-null", None) => ConfiguredReadColumnKind::StoredNotNull,
        ("stored-int-not-null", None) => ConfiguredReadColumnKind::StoredIntNotNull,
        ("stored-char-not-null", Some(length)) => ConfiguredReadColumnKind::StoredCharNotNull {
            max_length: parse_char_length(option, length)?,
        },
        ("stored-char-not-null", None) => {
            return Err(invalid(
                option,
                "stored-char-not-null requires a :<char-length> field",
            ));
        }
        (_, Some(_)) => {
            return Err(invalid(
                option,
                "only stored-char-not-null takes a :<char-length> field",
            ));
        }
        _ => {
            return Err(invalid(
                option,
                "column kind must be clustered-pk, stored-not-null, stored-int-not-null, or stored-char-not-null:<N>",
            ));
        }
    };
    Ok(ConfiguredReadColumn { name, id, kind })
}

/// Parses and range-checks a `CHAR(N)` length: a positive integer in MySQL's
/// `1..=255` character-count range.
fn parse_char_length(option: &str, value: &str) -> Result<u32, NodeConfigError> {
    let length: u32 = value
        .parse()
        .map_err(|_| invalid(option, "char length must be a positive integer"))?;
    if !(1..=255).contains(&length) {
        return Err(invalid(option, "char length must be between 1 and 255"));
    }
    Ok(length)
}

fn validate_columns(option: &str, columns: &[ConfiguredReadColumn]) -> Result<(), NodeConfigError> {
    if columns.is_empty() {
        return Err(invalid(option, "at least one column is required"));
    }

    let mut names = HashSet::with_capacity(columns.len());
    let mut ids = HashSet::with_capacity(columns.len());
    let mut clustered_primary_keys = 0;
    for column in columns {
        if !names.insert(column.name.to_lowercase()) {
            return Err(invalid(
                option,
                "column names must be unique case-insensitively",
            ));
        }
        if !ids.insert(column.id) {
            return Err(invalid(option, "column IDs must be unique"));
        }
        if column.kind == ConfiguredReadColumnKind::ClusteredPrimaryKey {
            clustered_primary_keys += 1;
        }
    }
    if clustered_primary_keys != 1 {
        return Err(invalid(option, "exactly one column must be clustered-pk"));
    }
    Ok(())
}

/// Splits `<database>.<table>`, the only shape `--load-table` accepts: the
/// cluster's stored catalog supplies everything else about the table.
fn parse_loaded_table_name(value: &str) -> Result<LoadedTableName, NodeConfigError> {
    let Some((database, table)) = value.split_once('.') else {
        return Err(invalid("--load-table", "expected <database>.<table>"));
    };
    Ok(LoadedTableName {
        database: parse_identifier("--load-table", database.to_owned())?,
        table: parse_identifier("--load-table", table.to_owned())?,
    })
}

fn validate_load_tables(
    read_tables: &[ConfiguredReadTable],
    load_tables: &[LoadedTableName],
) -> Result<(), NodeConfigError> {
    if load_tables.len() > MAX_CONFIGURED_READ_TABLES {
        return Err(invalid(
            "--load-table",
            &format!("loaded table count must not exceed {MAX_CONFIGURED_READ_TABLES}"),
        ));
    }
    let mut names = HashSet::with_capacity(load_tables.len());
    for loaded in load_tables {
        let name = (loaded.database.to_lowercase(), loaded.table.to_lowercase());
        if !names.insert(name.clone()) {
            return Err(invalid(
                "--load-table",
                "loaded table names must be unique case-insensitively",
            ));
        }
        if read_tables
            .iter()
            .any(|table| (table.database.to_lowercase(), table.table.to_lowercase()) == name)
        {
            return Err(invalid(
                "--load-table",
                "a table cannot be both described on the command line and loaded",
            ));
        }
    }
    Ok(())
}

fn validate_read_tables(
    tables: &[ConfiguredReadTable],
    load_tables: &[LoadedTableName],
) -> Result<(), NodeConfigError> {
    if tables.is_empty() && load_tables.is_empty() {
        return Err(NodeConfigError::MissingOption("--read-table"));
    }
    if tables.len() > MAX_CONFIGURED_READ_TABLES {
        return Err(invalid(
            "--read-table",
            &format!("configured table count must not exceed {MAX_CONFIGURED_READ_TABLES}"),
        ));
    }
    let mut names = HashSet::with_capacity(tables.len());
    let mut ids = HashSet::with_capacity(tables.len());
    for table in tables {
        if !names.insert((table.database.to_lowercase(), table.table.to_lowercase())) {
            return Err(invalid(
                "--read-table",
                "table names must be unique case-insensitively within each database",
            ));
        }
        if !ids.insert(table.table_id) {
            return Err(invalid("--read-table", "table IDs must be unique"));
        }
    }
    Ok(())
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
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        encoded_spill_path_for_identity, parse_column_descriptor, ConfiguredReadColumnKind,
        NodeConfig, NodeConfigError, StoreKind,
    };

    #[test]
    fn spill_path_identity_matches_source_encoding() {
        for (host, status_host, port, status_port, encoded) in [
            (
                "0.0.0.0",
                "0.0.0.0",
                4000,
                10080,
                "MC4wLjAuMDo0MDAwLzAuMC4wLjA6MTAwODA=",
            ),
            (
                "127.0.0.1",
                "127.16.5.1",
                4000,
                10080,
                "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxMDA4MA==",
            ),
            (
                "127.0.0.1",
                "127.16.5.1",
                4000,
                15532,
                "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxNTUzMg==",
            ),
        ] {
            assert_eq!(
                encoded_spill_path_for_identity(
                    Path::new("/tmp"),
                    host,
                    port,
                    status_host,
                    status_port,
                    "501",
                ),
                Path::new("/tmp")
                    .join("501_tidb")
                    .join(encoded)
                    .join("tmp-storage")
            );
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
            "--load-table",
            "test.rows",
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

    /// The command-line descriptor field must parse back to the same typed
    /// kind for every admitted shape, including the CHAR length as a fourth
    /// `:N` field — the same descriptor strings
    /// `real_tikv_node::served_table_descriptor` renders for the readiness
    /// event.
    #[test]
    fn column_descriptor_strings_round_trip_through_parse() {
        let cases = [
            (
                "clustered-pk",
                ConfiguredReadColumnKind::ClusteredPrimaryKey,
            ),
            ("stored-not-null", ConfiguredReadColumnKind::StoredNotNull),
            (
                "stored-int-not-null",
                ConfiguredReadColumnKind::StoredIntNotNull,
            ),
            (
                "stored-char-not-null:120",
                ConfiguredReadColumnKind::StoredCharNotNull { max_length: 120 },
            ),
            (
                "stored-char-not-null:1",
                ConfiguredReadColumnKind::StoredCharNotNull { max_length: 1 },
            ),
            (
                "stored-char-not-null:255",
                ConfiguredReadColumnKind::StoredCharNotNull { max_length: 255 },
            ),
        ];
        for (descriptor_name, kind) in cases {
            let descriptor = format!("c:3:{descriptor_name}");
            let parsed = parse_column_descriptor("--read-table", descriptor).unwrap();
            assert_eq!(parsed.name, "c");
            assert_eq!(parsed.id, 3);
            assert_eq!(parsed.kind, kind, "round-trip of {kind:?}");
        }
    }

    /// `--load-privileges` names the cluster's own `mysql.*` as the account
    /// source, which is only meaningful in place of `--auth-file`: two
    /// sources would be two answers to "may this user log in".
    #[test]
    fn the_account_source_is_exactly_one_of_the_auth_file_or_the_cluster() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--load-table",
            "test.rows",
        ];

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
        assert!(matches!(
            NodeConfig::parse(base),
            Err(NodeConfigError::MissingOption("--auth-file"))
        ));
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
            "--load-table",
            "test.rows",
        ]);
        let combined = NodeConfig::parse([
            "tidb-server",
            "--config",
            &path_text,
            "--path",
            "127.0.0.1:2379",
            "--load-table",
            "test.rows",
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
                Err(NodeConfigError::InvalidValue { option, reason })
                    if option == "--config"
                        && reason == "TiDB run with skip-grant-table need root privilege"
            ));
        }

        let ordinary = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--load-table",
            "test.rows",
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
            "--load-table",
            "test.rows",
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
            "--load-table",
            "test.rows",
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
    fn startup_config_projection_contains_the_effective_owned_values() {
        let config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--load-table",
            "test.rows",
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
        assert_eq!(projected["instance"]["max_connections"], 8);
    }

    #[test]
    fn affinity_cpu_list_matches_the_source_command_line_parser() {
        let base = [
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--load-table",
            "test.rows",
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
            "--read-table",
            "test",
            "t",
            "100",
            "1",
            "a:1:clustered-pk",
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
            "--read-table",
            "test",
            "t",
            "100",
            "1",
            "a:1:clustered-pk",
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
