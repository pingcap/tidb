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

use tidb_config::config_tree::Config as SourceConfig;
use tidb_config_kerneltype as kerneltype;
use tidb_domain_core::node_config::{invalid, NodeConfig, NodeConfigError, NodeOptions};

/// Go `initFlagSet` + `config.InitializeConfig(..., overrideConfig, ...)`:
/// the command line (including the executable name) parsed by Go's one flag
/// set, laid over Go's defaults and the config file.
pub fn parse_node_config<I, S>(arguments: I) -> Result<NodeConfig, NodeConfigError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut main_flags = parse_main_flags(arguments)?;
    if main_flags.help {
        return Err(NodeConfigError::HelpRequested);
    }
    main_flags.store = main_flags.store.map(|store| store.to_ascii_lowercase());
    let config_path = main_flags.config_path.clone();
    let (global_config, warnings) = tidb_config::config_tree::load::prepare_config(
        NodeConfig::deployment_defaults(),
        config_path.as_deref().map(std::path::Path::new),
        main_flags.config_check,
        main_flags.config_strict,
        |config| crate::main_flags::override_config(config, &main_flags),
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
            affinity_cpus: parse_affinity_cpus(main_flags.affinity_cpus.as_deref().unwrap_or_default())?,
        },
    )
}

/// Go `initFlagSet`'s `fset.Parse(os.Args[1:])` over the command line,
/// including the executable name.
///
/// # Errors
///
/// A flag Go does not define, or a malformed value.
pub fn parse_main_flags<I, S>(arguments: I) -> Result<crate::main_flags::MainFlags, NodeConfigError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let arguments: Vec<String> = arguments.into_iter().map(Into::into).skip(1).collect();
    crate::main_flags::MainFlags::parse(&arguments)
        .map_err(|error| invalid("main.go flags", &error.to_string()))
}

/// Initializes the same process globals Go reads when printing `-V`.
pub fn initialize_versions_for_display<I, S>(arguments: I) -> Result<(), NodeConfigError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let main_flags = parse_main_flags(arguments)?;
    let config_path = main_flags.config_path.clone();
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
    tidb_cpu::install_cpu_count(max_procs, config.affinity_cpus.len());
}

/// Go `fset.Usage()` for `-help`: `flag.FlagSet.PrintDefaults` over
/// `initFlagSet`'s flags, sorted by name, as Go's flag package formats them.
#[must_use]
pub const fn node_config_help_text() -> &'static str {
    r#"Usage of tidb-server:
  -L string
    	log level: info, debug, warn, error, fatal (default "info")
  -P string
    	tidb server port (default "4000")
  -V	print version information and exit (default false)
  -activation-timeout uint
    	max time in second allowed for tidb to activate from standby, 0 means no limit
  -advertise-address string
    	tidb server advertise IP
  -affinity-cpus string
    	affinity cpu (cpu-no. separated by comma, e.g. 1,2,3)
  -cluster-ca string
    	cluster CA file path
  -cluster-cert string
    	cluster cert file path
  -cluster-key string
    	cluster key file path
  -config string
    	config file path
  -config-check
    	check config file validity and exit (default false)
  -config-strict
    	enforce config file validity (default false)
  -cors string
    	tidb server allow cors origin
  -disconnect-on-expired-password
    	the server disconnects the client when the password is expired (default true)
  -help
    	show the usage
  -host string
    	tidb server host (default "0.0.0.0")
  -initialize-insecure
    	bootstrap tidb-server in insecure mode (default true)
  -initialize-secure
    	bootstrap tidb-server in secure mode (default false)
  -initialize-sql-file string
    	SQL file to execute on first bootstrap
  -keyspace-activate
    	exit after activating the keyspace (default false)
  -keyspace-name string
    	keyspace name.
  -lease string
    	schema lease duration, very dangerous to change only if you know what you do (default "45s")
  -log-file string
    	log file path
  -log-general string
    	general log file path
  -log-slow-query string
    	slow query file path
  -max-idle-seconds uint
    	max idle seconds for a connection, 0 means no limit
  -metrics-addr string
    	prometheus pushgateway address, leaves it empty will disable prometheus push.
  -metrics-interval uint
    	prometheus client push interval in second, set "0" to disable prometheus push. (default 15)
  -path string
    	tidb storage path (default "/tmp/tidb")
  -plugin-dir string
    	the folder that hold plugin (default "/data/deploy/plugin")
  -plugin-load string
    	wait load plugin name(separated by comma)
  -proxy-protocol-fallbackable
    	enable proxy protocol fallback mode. If it is enabled, connection will return the client IP address when the client does not send PROXY Protocol Header and it will not return any error. (Note: This feature it does NOT follow the PROXY Protocol SPEC) (default false)
  -proxy-protocol-header-timeout uint
    	proxy protocol header read timeout, unit is second. (Deprecated: as proxy protocol using lazy mode, header read timeout no longer used) (default 5)
  -proxy-protocol-networks string
    	proxy protocol networks allowed IP or *, empty mean disable proxy protocol support
  -redact
    	remove sensitive words from marked tidb logs when using collect-log subcommand, e.g. ./tidb-server --redact=xxx collect-log <input> <output> (default false)
  -repair-list string
    	admin repair table list
  -repair-mode
    	enable admin repair mode (default false)
  -report-status
    	If enable status report HTTP service. (default true)
  -run-ddl
    	run ddl worker on this tidb-server (default true)
  -socket string
    	The socket file to use for connection. (default "/tmp/tidb-{Port}.sock")
  -sql-ca string
    	SQL CA file path
  -sql-cert string
    	SQL cert file path
  -sql-key string
    	SQL key file path
  -standby
    	start tidb-server as standby (default false)
  -starter-additional-params string
    	starter additional params in k=v,k=v format
  -status string
    	tidb server status port (default "10080")
  -status-host string
    	tidb server status host (default "0.0.0.0")
  -store string
    	registered store name, [tikv unistore mocktikv] (default "unistore")
  -temp-dir string
    	tidb temporary directory (default "/tmp/tidb")
  -tidb-service-scope string
    	tidb service scope
  -token-limit int
    	the limit of concurrent executed sessions (default 1000)
"#
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
        tidb_config_deploymode::set(config.deploy_mode)
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
        tidb_versioninfo::set_tidb_edition(configured_edition);
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

fn parse_affinity_cpus(value: &str) -> Result<Vec<i64>, NodeConfigError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|cpu| !cpu.is_empty())
        .map(|cpu| {
            cpu.parse::<i64>()
                .map_err(|_| invalid("-affinity-cpus", "expected comma-separated CPU indexes"))
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tidb_domain_core::node_config::{NodeConfigError, StatsLease, StoreKind};

    use super::parse_node_config;

    #[test]
    fn command_token_flag_reaches_effective_config() {
        let mut config = parse_node_config([
            "tidb-server",
            "--store",
            "unistore",
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
        let info = tidb_session::domain::serverinfo_etcd::get_server_info(&config);
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
        // A classic deployment's cluster TLS is `[security]` configuration;
        // Go's `-cluster-*` flags apply to Starter deployments only.
        let parse = |security: &str| {
            let path = std::env::temp_dir().join(format!(
                "tidb-cluster-tls-{}-{}.toml",
                std::process::id(),
                security.len()
            ));
            fs::write(&path, format!("[security]\n{security}")).unwrap();
            let config = parse_node_config([
                "tidb-server",
                "--store",
                "tikv",
                "--path",
                "127.0.0.1:2379",
                "--config",
                path.to_str().unwrap(),
            ]);
            fs::remove_file(&path).unwrap();
            config
        };

        // No security options: plaintext, backward compatible.
        assert!(!parse("").unwrap().cluster_security.is_tls_enabled());

        // CA + client key pair: full mutual TLS.
        let secured = parse(
            "cluster-ssl-ca = \"/tls/ca.pem\"\ncluster-ssl-cert = \"/tls/cert.pem\"\ncluster-ssl-key = \"/tls/key.pem\"\n",
        )
        .unwrap();
        assert!(secured.cluster_security.is_tls_enabled());
        assert_eq!(secured.cluster_security.ca_path(), "/tls/ca.pem");
        assert_eq!(secured.cluster_security.cert_path(), "/tls/cert.pem");
        assert!(secured.cluster_security.verify_cn().is_empty());

        // `-cluster-verify-cn` is no flag Go defines.
        let error = parse_node_config(["tidb-server", "--cluster-verify-cn", "tidb,tikv"]).unwrap_err();
        assert!(error.to_string().contains("flag provided but not defined: -cluster-verify-cn"), "{error}");

        // Cert without key is rejected.
        assert!(matches!(
            parse("cluster-ssl-ca = \"/tls/ca.pem\"\ncluster-ssl-cert = \"/tls/cert.pem\"\n"),
            Err(NodeConfigError::InvalidValue { .. })
        ));

        // Client material without a CA is rejected.
        assert!(matches!(
            parse("cluster-ssl-cert = \"/tls/cert.pem\"\ncluster-ssl-key = \"/tls/key.pem\"\n"),
            Err(NodeConfigError::InvalidValue { .. })
        ));
    }

    /// Go has one flag set and one account source, the store's
    /// `mysql.user`: an account-file option is no flag Go defines.
    #[test]
    fn account_file_options_are_not_go_flags() {
        for option in ["--auth-file", "--load-privileges"] {
            let error = parse_node_config(["tidb-server", option, "x"]).unwrap_err();
            assert!(
                error.to_string().contains(&format!("flag provided but not defined: {}", &option[1..])),
                "{error}"
            );
        }
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
        fs::remove_file(&path).expect("remove source config");

        #[cfg(unix)]
        let is_root = rustix::process::geteuid().as_raw() == 0;
        #[cfg(not(unix))]
        let is_root = false;
        if is_root {
            let parsed = parsed.expect("effective root may opt in");
            assert!(parsed.skip_grant_table);
            assert_eq!(
                serde_json::to_value(&parsed.global_config)
                    .expect("startup projection is JSON")["security"]["skip-grant-table"],
                true,
            );
        } else {
            assert!(matches!(
                parsed,
                Err(NodeConfigError::ConfigFile { reason, .. })
                    if reason.contains("TiDB run with skip-grant-table need root privilege")
            ));
        }

        let ordinary = parse_node_config([
            "tidb-server",
            "--store",
            "tikv",
            "--path",
            "127.0.0.1:2379",
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
        let base = ["tidb-server", "--store", "tikv", "--path", "127.0.0.1:2379"];
        assert!(
            parse_node_config(base)
                .expect("a complete configuration")
                .disconnect_on_expired_password
        );
        assert!(
            !parse_node_config(
                base.iter()
                    .copied()
                    .chain(["-disconnect-on-expired-password=false"])
                    .collect::<Vec<_>>(),
            )
            .expect("a complete configuration")
            .disconnect_on_expired_password
        );
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
ssl-cert = "cert.pem"
ssl-key = "key.pem"
tls-version = "TLSv1.3"
[performance]
stats-lease = "-1s"
"#,
        )
        .unwrap();
        let result = parse_node_config([
            "tidb-server",
            "--store",
            "tikv",
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
        ]);
        fs::remove_file(path).unwrap();
        let config = result.unwrap();
        assert_eq!(config.stats_lease, StatsLease::Disabled);
        assert_eq!(config.status_port, 10092);
        assert!(config.report_status);
        assert_eq!(config.socket, "/tmp/from-cli-4000.sock");
        assert_eq!(config.advertise_address, "from-cli");
        assert!(config.auto_tls);
        let json: serde_json::Value =
            serde_json::to_value(&config.global_config).unwrap();
        assert_eq!(json["status"]["status-port"], 10092);
        assert_eq!(json["status"]["report-status"], true);
        assert_eq!(json["socket"], "/tmp/from-cli-{Port}.sock");
        assert_eq!(json["temp-dir"], "/private/tmp/from-cli");
        assert_eq!(json["log"]["level"], "error");
        assert_eq!(json["security"]["auto-tls"], true);
        assert_eq!(json["security"]["ssl-ca"], "ca.pem");
        assert_eq!(config.ssl_ca, Some(std::path::PathBuf::from("ca.pem")));
        assert_eq!(config.min_tls_version, "TLSv1.3");
        assert_eq!(json["security"]["ssl-cert"], "cert.pem");
    }

    #[test]
    fn startup_config_projection_contains_the_effective_owned_values() {
        let config = parse_node_config([
            "tidb-server",
            "--store",
            "tikv",
            "--path",
            "127.0.0.1:2379",
            "-P",
            "4406",
        ])
        .unwrap();
        let projected: serde_json::Value =
            serde_json::to_value(&config.global_config).unwrap();
        assert_eq!(projected["host"], "0.0.0.0");
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
            "--store",
            "tikv",
            "--path",
            "127.0.0.1:2379",
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
            "--store",
            "tikv",
            "--path",
            "127.0.0.1:2379",
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
                if option == "-affinity-cpus"
        ));
    }

    #[test]
    fn store_unistore_is_accepted_without_a_pd_path() {
        // Go's unistore arm has no PD to dial, so `--path` is not required.
        let config = parse_node_config([
            "tidb-server",
            "--store",
            "unistore",
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
