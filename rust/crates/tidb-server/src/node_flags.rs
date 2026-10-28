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

//! Go `cmd/tidb-server/main.go`'s command line onto the node: the flags
//! (`main_flags`), the config file load (`config.InitializeConfig`), the
//! version check and the process globals `main` installs from the result.
//! The node's projection of the loaded config is the session's
//! [`NodeConfig::from_config`].

use std::path::PathBuf;
use std::time::Duration;

use tidb_config::config_tree::Config as SourceConfig;
use tidb_config::kerneltype;
use tidb_session::domain::node_config::{invalid, NodeConfig, NodeConfigError, NodeOptions};

/// Parses the source-shaped command line, including the executable name.
///
/// Both `--name value` and `--name=value` are accepted. The parser is
/// deliberately one-use and rejects duplicate values instead of applying
/// last-option-wins behavior to security or topology settings.
pub fn parse_node_config<I, S>(arguments: I) -> Result<NodeConfig, NodeConfigError>
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
    let mut ssl_ca = None;
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
            "--ssl-ca" => set_once(&mut ssl_ca, option, value)?,
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
    // The native executable's deployment defaults. TOML and explicit
    // flags override these through the single shared owner.
    let defaults = NodeConfig::deployment_defaults();
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
    let (global_config, warnings) = tidb_config::config_tree::load::prepare_config(
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
                (&mut config.security.ssl_ca, &ssl_ca),
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

    NodeConfig::from_config(
        global_config,
        NodeOptions {
            auth_file,
            load_privileges,
            affinity_cpus: parse_affinity_cpus(affinity_cpus.as_deref().unwrap_or_default())?,
            connection_timeout: match connection_timeout_ms {
                Some(value) => Duration::from_millis(parse_positive_number(
                    "--connection-timeout-ms",
                    &value,
                )?),
                None => NodeOptions::default().connection_timeout,
            },
        },
    )
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
    install_global_config(config);
    Ok(())
}

/// Go `main`'s process globals from the loaded config: the global config
/// itself, the transaction size limits, the version strings and the CPU
/// count.
pub fn install_process_globals(config: &NodeConfig) {
    let max_procs = config.global_config.performance.max_procs;
    install_global_config(config.global_config.clone());
    tidb_util::cpu::install_cpu_count(max_procs, config.affinity_cpus.len());
}

/// Stable usage text printed by the executable for `--help`.
#[must_use]
pub const fn node_config_help_text() -> &'static str {
    "Usage: tidb-server [-V] [--config <tidb.toml>] --path <pd[,pd...]> \
[--max-connections <count>] [--connection-timeout-ms <milliseconds>] \
[--lease-ms <milliseconds>] \
[--auth-file <mode-0600-tsv> | --load-privileges] \
[--host <listen-ip>] [-P <port>|--port <port>] [--store tikv|unistore] \
[--affinity-cpus <cpu[,cpu...]>] \
[--max-allowed-packet <bytes>] \
[--ssl-cert <cert-pem> --ssl-key <key-pem> [--ssl-ca <ca-pem>]] [--no-auto-tls] \
[--no-disconnect-on-expired-password] \
[--cluster-ssl-ca <ca-pem> [--cluster-ssl-cert <cert-pem> --cluster-ssl-key <key-pem>]]"
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

fn install_global_config(config: SourceConfig) {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tidb_session::domain::node_config::{NodeConfigError, StatsLease, StoreKind};

    use super::parse_node_config;

    #[test]
    fn command_token_flag_reaches_effective_config() {
        let mut config = parse_node_config([
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
        config
            .global_config
            .labels
            .insert("zone".into(), "zone-1".into());
        config.global_config.lease = "45s".into();
        let info = tidb_session::domain::serverinfo_etcd::node_server_info(&config);
        assert_eq!(info.dynamic_info.labels, config.global_config.labels);
        assert_eq!(info.static_info.lease, "45s");
        assert_eq!(info.static_info.port, config.global_config.port);
        assert_eq!(
            info.static_info.status_port,
            config.global_config.status.status_port
        );
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
            let config = parse_node_config(args);
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
        let plaintext = parse_node_config(base).unwrap();
        assert!(!plaintext.cluster_security.is_tls_enabled());

        // CA + client key pair: full mutual TLS.
        let secured = parse_node_config(
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
            parse_node_config(
                base.iter()
                    .copied()
                    .chain(["--cluster-verify-cn", "tidb,tikv"])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::UnknownOption(option)) if option == "--cluster-verify-cn"
        ));

        // Cert without key is rejected.
        assert!(matches!(
            parse_node_config(
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
            parse_node_config(
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

        let from_cluster = parse_node_config(
            base.iter()
                .copied()
                .chain(["--load-privileges"])
                .collect::<Vec<_>>(),
        )
        .expect("--load-privileges alone is a complete account source");
        assert!(from_cluster.load_privileges);
        assert_eq!(from_cluster.auth_file.as_os_str(), "");

        let from_file = parse_node_config(
            base.iter()
                .copied()
                .chain(["--auth-file", "/tmp/users.tsv"])
                .collect::<Vec<_>>(),
        )
        .expect("--auth-file alone is a complete account source");
        assert!(!from_file.load_privileges);

        assert!(matches!(
            parse_node_config(
                base.iter()
                    .copied()
                    .chain(["--load-privileges", "--auth-file", "/tmp/users.tsv"])
                    .collect::<Vec<_>>(),
            ),
            Err(NodeConfigError::InvalidValue { .. })
        ));
        let ordinary = parse_node_config(base).unwrap();
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
        let parsed = parse_node_config([
            "tidb-server",
            "--config",
            &path_text,
            "--path",
            "127.0.0.1:2379",
        ]);
        let combined = parse_node_config([
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
                serde_json::to_value(&parsed.global_config)
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

        let ordinary = parse_node_config([
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
        let parsed = parse_node_config([
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
            parse_node_config(base)
                .expect("a complete configuration")
                .disconnect_on_expired_password
        );
        assert!(
            !parse_node_config(
                base.iter()
                    .copied()
                    .chain(["--no-disconnect-on-expired-password"])
                    .collect::<Vec<_>>(),
            )
            .expect("a complete configuration")
            .disconnect_on_expired_password
        );
        assert!(matches!(
            parse_node_config(
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
tls-version = "TLSv1.3"
[performance]
stats-lease = "-1s"
"#,
        )
        .unwrap();
        let result = parse_node_config([
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
            serde_json::to_value(&config.global_config).unwrap();
        assert_eq!(json["status"]["status-port"], 10092);
        assert_eq!(json["status"]["report-status"], true);
        assert_eq!(json["socket"], "/tmp/from-cli-{Port}.sock");
        assert_eq!(json["temp-dir"], "/private/tmp/from-cli");
        assert_eq!(json["log"]["level"], "error");
        assert_eq!(json["security"]["auto-tls"], false);
        assert_eq!(json["security"]["ssl-ca"], "ca.pem");
        assert_eq!(config.ssl_ca, Some(std::path::PathBuf::from("ca.pem")));
        assert_eq!(config.min_tls_version, "TLSv1.3");
        assert_eq!(json["security"]["ssl-cert"], "cert.pem");
    }

    #[test]
    fn startup_config_projection_contains_the_effective_owned_values() {
        let config = parse_node_config([
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
            serde_json::to_value(&config.global_config).unwrap();
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
        assert!(parse_node_config(base).unwrap().run_ddl);
        let with_flag = |flag: &'static str| {
            parse_node_config(base.iter().copied().chain([flag]).collect::<Vec<_>>())
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
        assert!(parse_node_config(base).unwrap().affinity_cpus.is_empty());
        assert_eq!(
            parse_node_config(
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
            parse_node_config(
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
        let config = parse_node_config([
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
        let err = parse_node_config([
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
        let err = parse_node_config([
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
