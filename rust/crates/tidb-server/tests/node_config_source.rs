// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use tidb_server::{run_configured_node, NodeConfig, NodeConfigError, RunConfiguredNodeError};
use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorageOpenError};

fn required() -> Vec<&'static str> {
    vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379,127.0.0.1:2380",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ]
}

struct ConfigFile(PathBuf);

impl ConfigFile {
    fn write(name: &str, contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidb_rust_node_config_{name}_{}_{}.toml",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        std::fs::write(&path, contents).expect("write config fixture");
        Self(path)
    }
}

impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn configured_spill_policy_is_loaded_from_the_tidb_config_file() {
    let file = ConfigFile::write(
        "spill_policy",
        r#"
host = "127.0.0.1"
path = "127.0.0.1:2379"
store = "tikv"
tmp-storage-path = "/private/tmp/tidb-configured-spill"
tmp-storage-quota = 1048576

[security]
spilled-file-encryption-method = "AeS128-CtR"

[pessimistic-txn]
deadlock-history-capacity = 123
deadlock-history-collect-retryable = true
"#,
    );
    let path = file.0.to_string_lossy().into_owned();
    let config = NodeConfig::parse([
        "tidb-server",
        "--config",
        &path,
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ]);
    let config =
        config.unwrap_or_else(|error| panic!("configured startup must be admitted: {error}"));
    assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.pd_endpoints, ["127.0.0.1:2379"]);
    assert_eq!(config.spill_storage.quota_bytes, 1_048_576);
    assert_eq!(config.deadlock_history_capacity, 123);
    assert!(config.deadlock_history_collect_retryable);
    assert_eq!(
        config.spill_storage.encryption,
        SpillEncryptionMethod::Aes128Ctr
    );
    assert!(config
        .spill_storage
        .path
        .starts_with("/private/tmp/tidb-configured-spill"));
    assert_eq!(
        config.spill_storage.path.file_name().unwrap(),
        "tmp-storage"
    );
    assert_eq!(
        config
            .spill_storage
            .path
            .parent()
            .and_then(std::path::Path::file_name)
            .unwrap(),
        "MTI3LjAuMC4xOjQwMDAvMC4wLjAuMDoxMDA4MA=="
    );
    #[cfg(unix)]
    assert_eq!(
        config
            .spill_storage
            .path
            .parent()
            .and_then(std::path::Path::parent)
            .and_then(std::path::Path::file_name)
            .unwrap(),
        format!("{}_tidb", rustix::process::getuid().as_raw()).as_str()
    );
}

#[test]
fn configured_sem_is_installed_before_startup_resource_admission() {
    struct DisableSemOnDrop;

    impl Drop for DisableSemOnDrop {
        fn drop(&mut self) {
            tidb_util::sem_v2::disable();
            tidb_util::sem::disable();
        }
    }

    tidb_util::sem_v2::disable();
    tidb_util::sem::disable();
    let _reset = DisableSemOnDrop;
    let base = std::env::temp_dir().join(format!("tidb-server-sem-startup-{}", std::process::id()));
    let sem = ConfigFile::write(
        "sem_v2",
        &format!(
            "{{\"version\":\"1.0\",\"tidb_version\":{:?}}}",
            tidb_util::sem_v2::tidb_release_version()
        ),
    );
    let file = ConfigFile::write(
        "sem_startup",
        &format!(
            "tmp-storage-path = {:?}\ntmp-storage-quota = {}\n\n[security]\nenable-sem = true\nsem-config = {:?}\n",
            base,
            i64::MAX,
            sem.0,
        ),
    );
    let path = file.0.to_string_lossy().into_owned();
    let mut args = required();
    args.extend(["--config", &path]);

    let config = NodeConfig::parse(args).expect("security.enable-sem is an owned startup option");
    let error = run_configured_node(config).unwrap_err();
    assert!(matches!(
        error,
        RunConfiguredNodeError::Spill(SpillStorageOpenError::QuotaExceedsAvailable { .. })
    ));
    assert!(tidb_util::sem_v2::is_enabled());
    assert!(!tidb_util::sem::is_enabled());
    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn version_flag_prints_the_effective_source_identity_without_topology() {
    let defaults = Command::new(env!("CARGO_BIN_EXE_tidb-server"))
        .arg("-V")
        .output()
        .expect("run tidb-server -V");
    assert!(defaults.status.success(), "{:?}", defaults.status);
    assert!(
        String::from_utf8(defaults.stdout)
            .unwrap()
            .contains("Store: unistore")
    );

    let file = ConfigFile::write(
        "version_flag",
        "store = \"tikv\"\n\
         tidb-edition = \"Starter\"\n\
         tidb-release-version = \"v9.0.0\"\n\
         server-version = \"8.0.11-TiDB-v9.0.0\"\n",
    );
    let path = file.0.to_string_lossy().into_owned();
    let output = Command::new(env!("CARGO_BIN_EXE_tidb-server"))
        .args(["-V", "--config", path.as_str()])
        .output()
        .expect("run tidb-server -V");
    assert!(output.status.success(), "{:?}", output.status);
    let output = String::from_utf8(output.stdout).unwrap();
    assert!(output.contains("Store: tikv"), "{output}");
    assert!(output.contains("Edition: Starter"), "{output}");
    assert!(output.contains("Release Version: v9.0.0"), "{output}");
}

#[test]
fn explicit_cli_values_override_the_config_before_validation() {
    let file = ConfigFile::write(
        "cli_precedence",
        r#"
host = "0.0.0.0"
port = 70000
path = "not-a-pd-endpoint"
store = "bogus"
lease = "not-a-duration"
"#,
    );
    let path = file.0.to_string_lossy().into_owned();
    let mut args = required();
    args.extend([
        "--config",
        &path,
        "--host",
        "127.0.0.1",
        "--port",
        "4406",
        "--store",
        "TIKV",
        "--lease-ms",
        "9000",
    ]);
    let config = NodeConfig::parse(args).unwrap();
    assert_eq!(config.port, 4406);
    assert_eq!(config.schema_lease, Duration::from_secs(9));
    assert_eq!(config.pd_endpoints, ["127.0.0.1:2379", "127.0.0.1:2380"]);
}

#[test]
fn recognized_but_unowned_config_leaves_fail_closed() {
    for (name, contents, expected) in [
        ("log", "[log]\nlevel = \"debug\"\n", "log.level"),
        (
            "status",
            "[status]\nstatus-port = 10081\n",
            "status.status-port",
        ),
        (
            "sql_ca",
            "[security]\nssl-ca = \"ca.pem\"\n",
            "security.ssl-ca",
        ),
        (
            "cluster_verify_cn",
            "[security]\ncluster-verify-cn = [\"tidb\"]\n",
            "security.cluster-verify-cn",
        ),
    ] {
        let file = ConfigFile::write(name, contents);
        let path = file.0.to_string_lossy().into_owned();
        let mut args = required();
        args.extend(["--config", &path]);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::UnsupportedConfigOptions(options)) if options == [expected]
        ));
    }
}

#[test]
fn config_path_accepts_inline_syntax_and_reports_missing_files() {
    assert!(NodeConfig::help_text().contains("--config <tidb.toml>"));
    let file = ConfigFile::write("inline", "tmp-storage-quota = -1\n");
    let inline = format!("--config={}", file.0.display());
    let mut args = required()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    args.push(inline);
    assert!(NodeConfig::parse(args).is_ok());

    let mut missing = required();
    missing.extend(["--config", "/private/tmp/does-not-exist-tidb-config.toml"]);
    assert!(matches!(
        NodeConfig::parse(missing),
        Err(NodeConfigError::ConfigFile { .. })
    ));
}

#[test]
fn impossible_spill_quota_fails_before_auth_listener_or_cluster_startup() {
    let base = std::env::temp_dir().join(format!(
        "tidb-server-impossible-spill-quota-{}",
        std::process::id()
    ));
    let file = ConfigFile::write(
        "impossible_quota",
        &format!(
            "tmp-storage-path = {:?}\ntmp-storage-quota = {}\n",
            base,
            i64::MAX
        ),
    );
    let path = file.0.to_string_lossy().into_owned();
    let mut args = required();
    args.extend(["--config", &path]);
    let config = NodeConfig::parse(args).unwrap();
    let error = run_configured_node(config).unwrap_err();
    assert!(matches!(
        error,
        RunConfiguredNodeError::Spill(SpillStorageOpenError::QuotaExceedsAvailable { .. })
    ));
    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn source_tikv_startup_uses_the_shared_catalog() {
    // cmd/tidb-server/main_test.go:50 TestRunMain
    // pkg/config/config_test.go:960 TestConfig
    // pkg/config/store_test.go:23 TestStoreType
    let config = NodeConfig::parse(required()).unwrap();
    assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.port, 4000);
    assert_eq!(config.pd_endpoints, ["127.0.0.1:2379", "127.0.0.1:2380"]);
    assert_eq!(
        config.auth_file,
        std::path::Path::new("/tmp/campaign21-users.tsv")
    );
    // Go's `config.Instance.MaxConnections` default is 0: unlimited.
    assert_eq!(config.max_connections, 0);
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.deadlock_history_capacity, 10);
    assert!(!config.deadlock_history_collect_retryable);
}

#[test]
fn source_port_alias_and_long_option_share_one_value_slot() {
    let mut source_alias = required();
    source_alias.extend(["-P", "4406"]);
    assert_eq!(NodeConfig::parse(source_alias).unwrap().port, 4406);

    let mut ambiguous = required();
    ambiguous.extend(["-P", "4406", "--port", "4407"]);
    assert!(matches!(
        NodeConfig::parse(ambiguous),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--port"
    ));
}

#[test]
fn unsupported_or_ambiguous_startup_options_fail_closed() {
    // pkg/config/config_test.go:457 TestRemovedVariableCheck
    let mut duplicate = required();
    duplicate.extend(["--store", "tikv", "--store", "tikv"]);
    assert!(matches!(
        NodeConfig::parse(duplicate),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--store"
    ));

    let mut mock = required();
    mock.extend(["--store", "mocktikv"]);
    assert!(matches!(
        NodeConfig::parse(mock),
        Err(NodeConfigError::UnsupportedStore(store)) if store == "mocktikv"
    ));

    let file = ConfigFile::write("empty", "");
    let path = file.0.to_string_lossy().into_owned();
    let mut config_file = required();
    config_file.extend(["--config", &path]);
    assert!(NodeConfig::parse(config_file).is_ok());

    let mut duplicate_config = required();
    duplicate_config.extend(["--config", &path, "--config", &path]);
    assert!(matches!(
        NodeConfig::parse(duplicate_config),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--config"
    ));

    let mut removed_parallel_id = required();
    removed_parallel_id.extend(["--column-id", "3"]);
    assert!(matches!(
        NodeConfig::parse(removed_parallel_id),
        Err(NodeConfigError::UnknownOption(option)) if option == "--column-id"
    ));
}

#[test]
fn plaintext_native_password_boundary_cannot_bind_a_public_address() {
    // pkg/config/config_test.go:1850 TestTcpNoDelay
    let mut public = required();
    public.extend(["--host", "0.0.0.0"]);
    assert!(matches!(
        NodeConfig::parse(public),
        Err(NodeConfigError::NonLoopbackHost(address)) if address == IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    ));
}

#[test]
fn cluster_privilege_accounts_can_bind_a_non_loopback_listener() {
    let config = NodeConfig::parse(vec![
        "tidb-server",
        "--path",
        "172.31.36.41:2379",
        "--host",
        "0.0.0.0",
        "--port",
        "4000",
        "--cluster-session",
        "--load-privileges",
    ])
    .expect("cluster privilege mode must support a private-network listener");

    assert_eq!(config.host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert!(config.load_privileges);
}

#[test]
fn ordinary_tikv_startup_defaults_to_persisted_accounts() {
    let mut missing = required();
    let option = missing
        .iter()
        .position(|argument| *argument == "--auth-file")
        .unwrap();
    missing.drain(option..=option + 1);
    assert!(NodeConfig::parse(missing).unwrap().load_privileges);
}

#[test]
fn connection_limit_is_go_uint32_with_zero_unlimited() {
    // Go's flag is a plain uint32; every in-range value parses, including the
    // zero that `server.go`'s `checkConnectionCount` reads as unlimited.
    for count in ["0", "1", "256", "257", "4294967295"] {
        let mut args = required();
        args.extend(["--max-connections", count]);
        assert_eq!(
            NodeConfig::parse(args).unwrap().max_connections,
            count.parse::<u32>().unwrap() as usize
        );
    }
    for count in ["-1", "4294967296", "abc"] {
        let mut args = required();
        args.extend(["--max-connections", count]);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--max-connections"
        ));
    }
}

#[test]
fn connection_timeout_is_positive_explicit_and_documented() {
    let mut args = required();
    args.extend(["--connection-timeout-ms", "1250"]);
    assert_eq!(
        NodeConfig::parse(args).unwrap().connection_timeout,
        Duration::from_millis(1250)
    );

    let mut zero = required();
    zero.extend(["--connection-timeout-ms", "0"]);
    assert!(matches!(
        NodeConfig::parse(zero),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--connection-timeout-ms"
    ));
    assert!(NodeConfig::help_text().contains("--connection-timeout-ms"));
}

#[test]
fn packet_limit_and_help_are_checked_before_startup() {
    // cmd/tidb-server/main_test.go:56 TestExitCodeForSignal
    // pkg/config/config_test.go:1317 TestTxnTotalSizeLimitValid
    let mut args = required();
    args.extend(["--max-allowed-packet", "0"]);
    assert!(matches!(
        NodeConfig::parse(args),
        Err(NodeConfigError::InvalidValue { .. })
    ));
    assert_eq!(
        NodeConfig::parse(["tidb-server", "--help"]),
        Err(NodeConfigError::HelpRequested)
    );
    assert!(NodeConfig::help_text().contains("--store tikv|unistore"));
    assert!(!NodeConfig::help_text().contains("--read-table"));
    assert!(!NodeConfig::help_text().contains("--column-id"));
}

/// Go `overrideConfig`: `--advertise-address` is what a PEER dials, and
/// it is what this node publishes in `/tidb/server/info`. The flag wins;
/// with none, the bind host stands in -- unless that host is the
/// wildcard, where Go leaves the field empty for a later local-IP lookup
/// rather than advertising `0.0.0.0` to the cluster. More than one
/// address is refused by name.
///
/// The wildcard branch has no case here because this tier refuses a
/// non-loopback bind outright (`NonLoopbackHost`), so `--host 0.0.0.0`
/// never reaches the resolution; the branch is kept for the day that
/// gate lifts, which is when Go's deferred local-IP lookup starts to
/// matter.
#[test]
fn the_advertise_address_follows_gos_resolution() {
    let mut arguments = required();
    arguments.extend(["--advertise-address", "10.0.0.7"]);
    assert_eq!(
        NodeConfig::parse(arguments).unwrap().advertise_address,
        "10.0.0.7"
    );

    let mut arguments = required();
    arguments.extend(["--host", "127.0.0.1"]);
    assert_eq!(
        NodeConfig::parse(arguments).unwrap().advertise_address,
        "127.0.0.1",
        "an unset advertise address falls back to the bind host"
    );

    let mut arguments = required();
    arguments.extend(["--advertise-address", "10.0.0.7 10.0.0.9"]);
    let error = NodeConfig::parse(arguments).unwrap_err();
    assert!(
        format!("{error:?}").contains("advertise-address"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn every_store_uses_the_shared_session_without_table_descriptors() {
    for store in ["tikv", "unistore"] {
        let config = NodeConfig::parse([
            "tidb-server",
            "--store",
            store,
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/shared-session-users.tsv",
        ])
        .expect("the storage engine must not select another SQL engine");
        assert_eq!(format!("{:?}", config.store_kind).to_lowercase(), store);
    }
}

#[test]
fn static_table_options_cannot_select_an_alternate_sql_engine() {
    for args in [
        vec!["--read-table", "test", "t", "42", "1", "id:1:clustered-pk"],
        vec!["--load-table", "test.t"],
        vec!["--read-table=test.t"],
        vec!["--load-table=test.t"],
        vec!["--max-topn-rows", "3"],
    ] {
        let mut command = vec![
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--auth-file",
            "/tmp/shared-session-users.tsv",
        ];
        command.extend(args);
        assert!(
            NodeConfig::parse(command).is_err(),
            "schema must come from the shared catalog"
        );
    }
}

#[test]
fn legacy_cluster_session_flag_does_not_change_the_session_owner() {
    let ordinary = NodeConfig::parse(required()).unwrap();
    let mut legacy = required();
    legacy.push("--cluster-session");
    assert_eq!(NodeConfig::parse(legacy).unwrap(), ordinary);
}
