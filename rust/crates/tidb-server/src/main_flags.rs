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

//! `cmd/tidb-server`'s flag surface: `initFlagSet` + `overrideConfig`.
//!
//! Go boundary: `cmd/tidb-server/main.go:240-316` (the flag definitions) and
//! `:708-926` (the override pass). The one semantic that matters everywhere
//! is Go's `fset.Visit` set-tracking: a flag overrides the config ONLY when
//! it was actually written on the command line — a flag left at its default
//! is invisible, so a config file's value survives it. Every field here is
//! therefore an `Option`, `None` meaning "not written".
//!
//! Go's `terror.MustNil` exits the process on a bad value; this transcreation
//! returns the same message as an `Err` and the caller exits, which is the
//! same observable behavior through the binary.
//!
//! Upstream coverage: `main_test.go`'s `TestOverrideConfigKeyspaceActivateMode`
//! (transcreated below). The Starter TLS sub-test of `TestInitDeployMode` and
//! the rest of that file are nextgen-kernel-only — Go itself skips them under
//! the Classic kernel this port builds.

use tidb_config::config_tree::config::Config;

/// The parsed command line, `None` per flag meaning "left unwritten".
///
/// Field names follow `main.go`'s flag variables one for one; each field IS
/// its flag's documentation, so per-field doc lines would only restate the
/// name.
#[allow(missing_docs)]
#[derive(Debug, Default)]
pub struct MainFlags {
    // Base (`main.go:248-269`)
    pub store: Option<String>,
    pub store_path: Option<String>,
    pub host: Option<String>,
    pub advertise_address: Option<String>,
    pub port: Option<String>,
    pub cors: Option<String>,
    pub socket: Option<String>,
    pub ddl_lease: Option<String>,
    pub token_limit: Option<i64>,
    pub repair_mode: Option<bool>,
    pub repair_list: Option<String>,
    pub temp_dir: Option<String>,
    pub cluster_ca: Option<String>,
    pub cluster_cert: Option<String>,
    pub cluster_key: Option<String>,
    pub sql_ca: Option<String>,
    pub sql_cert: Option<String>,
    pub sql_key: Option<String>,
    // Log (`main.go:272-275`)
    pub log_level: Option<String>,
    pub log_file: Option<String>,
    pub log_slow_query: Option<String>,
    pub log_general: Option<String>,
    // Status (`main.go:278-282`)
    pub report_status: Option<bool>,
    pub status_host: Option<String>,
    pub status_port: Option<String>,
    pub metrics_addr: Option<String>,
    pub metrics_interval: Option<u64>,
    // PROXY protocol (`main.go:288-290`)
    pub proxy_protocol_networks: Option<String>,
    pub proxy_protocol_header_timeout: Option<u64>,
    pub proxy_protocol_fallbackable: Option<bool>,
    // Bootstrap and security (`main.go:293-298`)
    pub initialize_secure: Option<bool>,
    pub initialize_insecure: Option<bool>,
    pub initialize_sql_file: Option<String>,
    pub disconnect_on_expired_password: Option<bool>,
    pub keyspace_name: Option<String>,
    pub service_scope: Option<String>,
    // Standby (`main.go:301-305`)
    pub standby_mode: Option<bool>,
    pub activation_timeout: Option<u64>,
    pub max_idle_seconds: Option<u64>,
    pub keyspace_activate: Option<bool>,
    pub starter_additional_params: Option<String>,
}

/// One unrecognized or malformed argument, with Go's flag-package shape.
#[derive(Debug, PartialEq, Eq)]
pub struct FlagParseError(pub String);

impl std::fmt::Display for FlagParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FlagParseError {}

impl MainFlags {
    /// Go `initFlagSet` + `fset.Parse`: one or two leading dashes, `=value`
    /// or a following argument; a boolean flag takes a bare form or `=bool`.
    pub fn parse(arguments: &[String]) -> Result<Self, FlagParseError> {
        let mut flags = Self::default();
        let mut queue = arguments.iter().peekable();
        while let Some(argument) = queue.next() {
            let Some(stripped) = argument
                .strip_prefix("--")
                .or_else(|| argument.strip_prefix('-'))
            else {
                return Err(FlagParseError(format!(
                    "unexpected non-flag argument {argument}"
                )));
            };
            let (name, inline) = match stripped.split_once('=') {
                Some((name, value)) => (name, Some(value.to_owned())),
                None => (stripped, None),
            };
            let mut take = || -> Result<String, FlagParseError> {
                if let Some(value) = inline.clone() {
                    return Ok(value);
                }
                queue
                    .next()
                    .cloned()
                    .ok_or_else(|| FlagParseError(format!("flag needs an argument: -{name}")))
            };
            let boolean = |inline: &Option<String>| -> Result<bool, FlagParseError> {
                match inline.as_deref() {
                    // Go `flagBoolean`: the bare spelling IS true.
                    None => Ok(true),
                    Some("true") | Some("1") => Ok(true),
                    Some("false") | Some("0") => Ok(false),
                    Some(other) => Err(FlagParseError(format!(
                        "invalid boolean value \"{other}\" for -{name}"
                    ))),
                }
            };
            match name {
                "store" => flags.store = Some(take()?),
                "path" => flags.store_path = Some(take()?),
                "host" => flags.host = Some(take()?),
                "advertise-address" => flags.advertise_address = Some(take()?),
                "P" => flags.port = Some(take()?),
                "cors" => flags.cors = Some(take()?),
                "socket" => flags.socket = Some(take()?),
                "lease" => flags.ddl_lease = Some(take()?),
                "token-limit" => {
                    let value = take()?;
                    flags.token_limit = Some(value.parse().map_err(|_| {
                        FlagParseError(format!("invalid value \"{value}\" for flag -token-limit"))
                    })?);
                }
                "repair-mode" => flags.repair_mode = Some(boolean(&inline)?),
                "repair-list" => flags.repair_list = Some(take()?),
                "temp-dir" => flags.temp_dir = Some(take()?),
                "cluster-ca" => flags.cluster_ca = Some(take()?),
                "cluster-cert" => flags.cluster_cert = Some(take()?),
                "cluster-key" => flags.cluster_key = Some(take()?),
                "sql-ca" => flags.sql_ca = Some(take()?),
                "sql-cert" => flags.sql_cert = Some(take()?),
                "sql-key" => flags.sql_key = Some(take()?),
                "L" => flags.log_level = Some(take()?),
                "log-file" => flags.log_file = Some(take()?),
                "log-slow-query" => flags.log_slow_query = Some(take()?),
                "log-general" => flags.log_general = Some(take()?),
                "report-status" => flags.report_status = Some(boolean(&inline)?),
                "status-host" => flags.status_host = Some(take()?),
                "status" => flags.status_port = Some(take()?),
                "metrics-addr" => flags.metrics_addr = Some(take()?),
                "metrics-interval" => {
                    let value = take()?;
                    flags.metrics_interval = Some(value.parse().map_err(|_| {
                        FlagParseError(format!(
                            "invalid value \"{value}\" for flag -metrics-interval"
                        ))
                    })?);
                }
                "proxy-protocol-networks" => {
                    flags.proxy_protocol_networks = Some(take()?);
                }
                "proxy-protocol-header-timeout" => {
                    let value = take()?;
                    flags.proxy_protocol_header_timeout = Some(value.parse().map_err(|_| {
                        FlagParseError(format!(
                            "invalid value \"{value}\" for flag -proxy-protocol-header-timeout"
                        ))
                    })?);
                }
                "proxy-protocol-fallbackable" => {
                    flags.proxy_protocol_fallbackable = Some(boolean(&inline)?);
                }
                "initialize-secure" => flags.initialize_secure = Some(boolean(&inline)?),
                "initialize-insecure" => flags.initialize_insecure = Some(boolean(&inline)?),
                "initialize-sql-file" => flags.initialize_sql_file = Some(take()?),
                "disconnect-on-expired-password" => {
                    flags.disconnect_on_expired_password = Some(boolean(&inline)?);
                }
                "keyspace-name" => flags.keyspace_name = Some(take()?),
                "tidb-service-scope" => flags.service_scope = Some(take()?),
                "standby" => flags.standby_mode = Some(boolean(&inline)?),
                "activation-timeout" => {
                    let value = take()?;
                    flags.activation_timeout = Some(value.parse().map_err(|_| {
                        FlagParseError(format!(
                            "invalid value \"{value}\" for flag -activation-timeout"
                        ))
                    })?);
                }
                "max-idle-seconds" => {
                    let value = take()?;
                    flags.max_idle_seconds = Some(value.parse().map_err(|_| {
                        FlagParseError(format!(
                            "invalid value \"{value}\" for flag -max-idle-seconds"
                        ))
                    })?);
                }
                "keyspace-activate" => flags.keyspace_activate = Some(boolean(&inline)?),
                "starter-additional-params" => {
                    flags.starter_additional_params = Some(take()?);
                }
                other => {
                    return Err(FlagParseError(format!(
                        "flag provided but not defined: -{other}"
                    )))
                }
            }
        }
        Ok(flags)
    }
}

/// Go `overrideConfig`: every written flag lands on the config; every unwritten
/// one is invisible. Order and precedence follow the source line for line.
pub fn override_config(cfg: &mut Config, flags: &MainFlags) -> Result<(), String> {
    // Base
    if let Some(host) = &flags.host {
        cfg.host = host.clone();
    }
    if let Some(advertise) = &flags.advertise_address {
        if advertise.split(' ').count() > 1 {
            return Err("Only support one advertise-address".to_owned());
        }
        cfg.advertise_address = advertise.clone();
    }
    // Go resolves an empty advertise address from the local IP when the host
    // is the wildcard; the deferred lookup is the node's, not this pass's.
    if cfg.advertise_address.is_empty() && cfg.host != "0.0.0.0" {
        cfg.advertise_address = cfg.host.clone();
    }
    if let Some(port) = &flags.port {
        cfg.port = port
            .parse::<u32>()
            .map_err(|_| format!("invalid port {port}"))?;
    }
    if let Some(cors) = &flags.cors {
        cfg.cors = cors.clone();
    }
    if let Some(store) = &flags.store {
        // Go casts the raw string (`config.StoreType(*store)`); validity is
        // the config check's job, exactly as in the source.
        cfg.store = tidb_config::store::StoreType(store.clone());
    }
    if let Some(path) = &flags.store_path {
        cfg.path = path.clone();
    }
    if let Some(socket) = &flags.socket {
        cfg.socket = socket.clone();
    }
    if let Some(lease) = &flags.ddl_lease {
        cfg.lease = lease.clone();
    }
    if let Some(limit) = flags.token_limit {
        cfg.token_limit = u32::try_from(limit).unwrap_or(0);
    }
    if let Some(repair) = flags.repair_mode {
        cfg.repair_mode = repair;
    }
    if let Some(list) = &flags.repair_list {
        if cfg.repair_mode {
            cfg.repair_table_list = list
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split(',')
                .map(|item| item.trim().to_owned())
                .filter(|item| !item.is_empty())
                .collect();
        }
    }
    if let Some(dir) = &flags.temp_dir {
        cfg.temp_dir = dir.clone();
    }

    // Log
    if let Some(level) = &flags.log_level {
        cfg.log.level = level.clone();
    }
    if let Some(file) = &flags.log_file {
        cfg.log.file.filename = file.clone();
    }
    if let Some(slow) = &flags.log_slow_query {
        cfg.log.slow_query_file = slow.clone();
    }
    if let Some(general) = &flags.log_general {
        cfg.log.general_log_file = general.clone();
    }

    // Status
    if let Some(report) = flags.report_status {
        cfg.status.report_status = report;
    }
    if let Some(host) = &flags.status_host {
        cfg.status.status_host = host.clone();
    }
    if let Some(port) = &flags.status_port {
        cfg.status.status_port = port
            .parse::<u32>()
            .map_err(|_| format!("invalid status port {port}"))?;
    }
    if let Some(addr) = &flags.metrics_addr {
        cfg.status.metrics_addr = addr.clone();
    }
    if let Some(interval) = flags.metrics_interval {
        cfg.status.metrics_interval = u32::try_from(interval).unwrap_or(0);
    }

    // PROXY protocol
    if let Some(networks) = &flags.proxy_protocol_networks {
        cfg.proxy_protocol.networks = networks.clone();
    }
    if let Some(timeout) = flags.proxy_protocol_header_timeout {
        cfg.proxy_protocol.header_timeout = u32::try_from(timeout).unwrap_or(0);
    }
    if let Some(fallbackable) = flags.proxy_protocol_fallbackable {
        cfg.proxy_protocol.fallbackable = fallbackable;
    }

    // Bootstrap and security
    if flags.initialize_secure.is_some() && flags.initialize_insecure.is_some() {
        return Err(
            "the options -initialize-insecure and -initialize-secure are mutually exclusive"
                .to_owned(),
        );
    }
    if let Some(secure) = flags.initialize_secure {
        cfg.security.secure_bootstrap = secure;
    }
    if let Some(insecure) = flags.initialize_insecure {
        cfg.security.secure_bootstrap = !insecure;
    }
    if let Some(disconnect) = flags.disconnect_on_expired_password {
        cfg.security.disconnect_on_expired_password = disconnect;
    }
    if let Some(file) = &flags.initialize_sql_file {
        if !std::path::Path::new(file).exists() {
            return Err(format!("can not access -initialize-sql-file {file}"));
        }
        cfg.initialize_sql_file = file.clone();
    }
    if let Some(keyspace) = &flags.keyspace_name {
        cfg.keyspace_name = keyspace.clone();
    }

    // Standby
    if let Some(standby) = flags.standby_mode {
        cfg.standby.standby_mode = standby;
    }
    if let Some(timeout) = flags.activation_timeout {
        cfg.standby.activation_timeout = u32::try_from(timeout).unwrap_or(0);
    }
    if let Some(idle) = flags.max_idle_seconds {
        cfg.standby.max_idle_seconds = u32::try_from(idle).unwrap_or(0);
    }
    if let Some(activate) = flags.keyspace_activate {
        cfg.keyspace_activate_mode = activate;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestOverrideConfigKeyspaceActivateMode` (`main_test.go:76-95`):
    /// `--keyspace-activate=true` lands on the config, and the starter
    /// params survive verbatim for the manager-client parser downstream.
    #[test]
    fn override_config_keyspace_activate_mode() {
        let flags = MainFlags::parse(&[
            "--keyspace-activate=true".to_owned(),
            "--starter-additional-params=pod-name=pod-1,pod-ip=10.0.0.1,pod-namespace=ns-1"
                .to_owned(),
        ])
        .expect("the fixture arguments parse");
        let mut cfg = Config::default();
        override_config(&mut cfg, &flags).expect("the override applies");
        assert!(cfg.keyspace_activate_mode);
        assert_eq!(
            flags.starter_additional_params.as_deref(),
            Some("pod-name=pod-1,pod-ip=10.0.0.1,pod-namespace=ns-1")
        );
    }

    /// The set-tracking rule the whole pass rides on: a flag left unwritten
    /// never touches the config, so a config-file value survives defaults.
    #[test]
    fn an_unwritten_flag_never_touches_the_config() {
        let flags =
            MainFlags::parse(&["--host".to_owned(), "10.0.0.9".to_owned()]).expect("parses");
        let mut cfg = Config::default();
        cfg.port = 4999;
        cfg.socket = "/custom.sock".to_owned();
        override_config(&mut cfg, &flags).expect("applies");
        assert_eq!(cfg.host, "10.0.0.9");
        assert_eq!(cfg.port, 4999, "the unwritten -P leaves the config alone");
        assert_eq!(cfg.socket, "/custom.sock");
    }

    /// Go: `-initialize-insecure` and `-initialize-secure` are mutually
    /// exclusive, and each writes `SecureBootstrap` with its own polarity.
    #[test]
    fn initialize_polarities_follow_go() {
        let mut cfg = Config::default();
        let secure = MainFlags::parse(&["--initialize-secure=true".to_owned()]).unwrap();
        override_config(&mut cfg, &secure).unwrap();
        assert!(cfg.security.secure_bootstrap);

        let mut cfg = Config::default();
        let insecure = MainFlags::parse(&["--initialize-insecure=true".to_owned()]).unwrap();
        override_config(&mut cfg, &insecure).unwrap();
        assert!(!cfg.security.secure_bootstrap);

        let both = MainFlags::parse(&[
            "--initialize-secure".to_owned(),
            "--initialize-insecure".to_owned(),
        ])
        .unwrap();
        let error = override_config(&mut Config::default(), &both).unwrap_err();
        assert!(error.contains("mutually exclusive"), "{error}");
    }

    /// The extractor accepts every `main.go` spelling and leaves the node's
    /// own options untouched and in order — the two surfaces coexist the way
    /// one binary demands.
    #[test]
    fn main_go_flags_are_extracted_and_node_options_survive() {
        let (flags, remaining) = extract_main_go_flags(vec![
            "--lease=45s".to_owned(),
            "--store".to_owned(),
            "unistore".to_owned(),
            "--repair-mode".to_owned(),
            "--token-limit".to_owned(),
            "1000".to_owned(),
            "--auth-file".to_owned(),
            "/tmp/u.tsv".to_owned(),
            "--keyspace-activate=true".to_owned(),
        ])
        .expect("the mixed line parses");
        assert_eq!(flags.ddl_lease.as_deref(), Some("45s"));
        assert_eq!(flags.repair_mode, Some(true));
        assert_eq!(flags.token_limit, Some(1000));
        assert_eq!(flags.keyspace_activate, Some(true));
        assert_eq!(
            remaining,
            vec!["--store", "unistore", "--auth-file", "/tmp/u.tsv"]
        );
    }
}

/// The names `main.go` defines that the node's own parser does not: consumed
/// here so every Go spelling is accepted, exactly as `initFlagSet` accepts
/// them, with the parsed values carried for the fields whose concepts the
/// node runs today (`--lease` drives the schema lease) and retained visibly
/// for the rest.
const MAIN_GO_ONLY_FLAGS: &[(&str, bool)] = &[
    // (name, is_boolean)
    ("lease", false),
    ("token-limit", false),
    ("plugin-dir", false),
    ("plugin-load", false),
    ("run-ddl", true),
    ("repair-mode", true),
    ("repair-list", false),
    ("temp-dir", false),
    ("cluster-ca", false),
    ("sql-ca", false),
    ("sql-cert", false),
    ("sql-key", false),
    ("L", false),
    ("log-file", false),
    ("log-slow-query", false),
    ("log-general", false),
    ("report-status", true),
    ("status-host", false),
    ("status", false),
    ("metrics-addr", false),
    ("metrics-interval", false),
    ("redact", true),
    ("proxy-protocol-networks", false),
    ("proxy-protocol-header-timeout", false),
    ("proxy-protocol-fallbackable", true),
    ("initialize-secure", true),
    ("initialize-insecure", true),
    ("initialize-sql-file", false),
    ("keyspace-name", false),
    ("tidb-service-scope", false),
    ("standby", true),
    ("activation-timeout", false),
    ("max-idle-seconds", false),
    ("keyspace-activate", true),
    ("starter-additional-params", false),
    ("advertise-address", false),
    ("cors", false),
    ("socket", false),
    ("config-check", true),
    ("config-strict", true),
];

/// Splits `main.go`-only flags out of a raw argument list, leaving the node's
/// own options untouched and in order. The extracted half parses through
/// [`MainFlags::parse`]; a malformed value there is the same error the full
/// parser gives.
pub fn extract_main_go_flags(
    arguments: Vec<String>,
) -> Result<(MainFlags, Vec<String>), FlagParseError> {
    let mut extracted = Vec::new();
    let mut remaining = Vec::new();
    let mut queue = arguments.into_iter().peekable();
    while let Some(argument) = queue.next() {
        let stripped = argument
            .strip_prefix("--")
            .or_else(|| argument.strip_prefix('-'));
        let owned = stripped.and_then(|body| {
            let name = body.split_once('=').map_or(body, |(name, _)| name);
            MAIN_GO_ONLY_FLAGS
                .iter()
                .find(|(candidate, _)| *candidate == name)
        });
        match owned {
            Some((_, is_boolean)) => {
                let has_inline = argument.contains('=');
                extracted.push(argument.clone());
                // A value-taking flag written as two arguments carries its
                // value along; a boolean never consumes the next argument.
                if !is_boolean && !has_inline {
                    if let Some(value) = queue.next() {
                        extracted.push(value);
                    }
                }
            }
            None => remaining.push(argument),
        }
    }
    let flags = MainFlags::parse(&extracted)?;
    Ok((flags, remaining))
}
