// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tidb_server::{NodeConfig, NodeConfigError};

fn required() -> [&'static str; 15] {
    [
        "tidb-server",
        "--path",
        "127.0.0.1:2379,127.0.0.1:2380",
        "--database",
        "campaign19",
        "--table",
        "rows",
        "--table-id",
        "42",
        "--column",
        "id:1:clustered-pk",
        "--column",
        "balance:2:stored-not-null",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ]
}

#[test]
fn source_tikv_startup_surface_is_explicit_and_bounded() {
    // cmd/tidb-server/main_test.go:50 TestRunMain
    // pkg/config/config_test.go:960 TestConfig
    // pkg/config/store_test.go:23 TestStoreType
    let config = NodeConfig::parse(required()).unwrap();
    assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.port, 4000);
    assert_eq!(config.pd_endpoints, ["127.0.0.1:2379", "127.0.0.1:2380"]);
    assert_eq!(config.read_table.database, "campaign19");
    assert_eq!(config.read_table.table, "rows");
    assert_eq!(config.read_table.table_id, 42);
    assert_eq!(config.read_table.columns.len(), 2);
    assert_eq!(config.read_table.columns[0].name, "id");
    assert_eq!(config.read_table.columns[0].id, 1);
    assert_eq!(config.read_table.columns[1].name, "balance");
    assert_eq!(config.read_table.columns[1].id, 2);
    assert_eq!(
        config.auth_file,
        std::path::Path::new("/tmp/campaign21-users.tsv")
    );
    assert_eq!(config.max_connections, 8);
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
}

#[test]
fn source_port_alias_and_long_option_share_one_value_slot() {
    let mut source_alias = required().to_vec();
    source_alias.extend(["-P", "4406"]);
    assert_eq!(NodeConfig::parse(source_alias).unwrap().port, 4406);

    let mut ambiguous = required().to_vec();
    ambiguous.extend(["-P", "4406", "--port", "4407"]);
    assert!(matches!(
        NodeConfig::parse(ambiguous),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--port"
    ));
}

#[test]
fn unsupported_or_ambiguous_startup_options_fail_closed() {
    // pkg/config/config_test.go:457 TestRemovedVariableCheck
    let mut duplicate = required().to_vec();
    duplicate.extend(["--store", "tikv", "--store", "tikv"]);
    assert!(matches!(
        NodeConfig::parse(duplicate),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--store"
    ));

    let mut mock = required().to_vec();
    mock.extend(["--store", "mocktikv"]);
    assert!(matches!(
        NodeConfig::parse(mock),
        Err(NodeConfigError::UnsupportedStore(store)) if store == "mocktikv"
    ));

    let mut config_file = required().to_vec();
    config_file.extend(["--config", "tidb.toml"]);
    assert!(matches!(
        NodeConfig::parse(config_file),
        Err(NodeConfigError::UnknownOption(option)) if option == "--config"
    ));

    let mut removed_parallel_id = required().to_vec();
    removed_parallel_id.extend(["--column-id", "3"]);
    assert!(matches!(
        NodeConfig::parse(removed_parallel_id),
        Err(NodeConfigError::UnknownOption(option)) if option == "--column-id"
    ));
}

#[test]
fn plaintext_native_password_boundary_cannot_bind_a_public_address() {
    // pkg/config/config_test.go:1850 TestTcpNoDelay
    let mut public = required().to_vec();
    public.extend(["--host", "0.0.0.0"]);
    assert!(matches!(
        NodeConfig::parse(public),
        Err(NodeConfigError::NonLoopbackHost(address)) if address == IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    ));
}

#[test]
fn authentication_file_is_required_before_startup() {
    let mut missing = required().to_vec();
    let option = missing
        .iter()
        .position(|argument| *argument == "--auth-file")
        .unwrap();
    missing.drain(option..=option + 1);
    assert_eq!(
        NodeConfig::parse(missing),
        Err(NodeConfigError::MissingOption("--auth-file"))
    );
}

#[test]
fn worker_count_is_positive_bounded_and_explicit() {
    for count in ["1", "256"] {
        let mut args = required().to_vec();
        args.extend(["--max-connections", count]);
        assert_eq!(
            NodeConfig::parse(args).unwrap().max_connections,
            count.parse::<usize>().unwrap()
        );
    }
    for count in ["0", "257"] {
        let mut args = required().to_vec();
        args.extend(["--max-connections", count]);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--max-connections"
        ));
    }
}

#[test]
fn connection_timeout_is_positive_explicit_and_documented() {
    let mut args = required().to_vec();
    args.extend(["--connection-timeout-ms", "1250"]);
    assert_eq!(
        NodeConfig::parse(args).unwrap().connection_timeout,
        Duration::from_millis(1250)
    );

    let mut zero = required().to_vec();
    zero.extend(["--connection-timeout-ms", "0"]);
    assert!(matches!(
        NodeConfig::parse(zero),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--connection-timeout-ms"
    ));
    assert!(NodeConfig::help_text().contains("--connection-timeout-ms"));
}

#[test]
fn topology_ids_packet_limit_and_help_are_checked_before_startup() {
    // cmd/tidb-server/main_test.go:56 TestExitCodeForSignal
    // pkg/config/config_test.go:1317 TestTxnTotalSizeLimitValid
    for (option, value) in [("--table-id", "0"), ("--max-allowed-packet", "0")] {
        let mut args = required().to_vec();
        if let Some(position) = args.iter().position(|arg| *arg == option) {
            args[position + 1] = value;
        } else {
            args.extend([option, value]);
        }
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { .. })
        ));
    }
    assert_eq!(
        NodeConfig::parse(["tidb-server", "--help"]),
        Err(NodeConfigError::HelpRequested)
    );
    assert!(NodeConfig::help_text().contains("--store tikv"));
    assert!(!NodeConfig::help_text().contains("--column-id"));
    assert!(NodeConfig::help_text().contains("stored-not-null"));
}

#[test]
fn repeated_column_descriptors_are_atomic_ordered_and_complete() {
    // pkg/config/config_test.go:1701 TestTableColumnCountLimit
    let config = NodeConfig::parse(required()).unwrap();
    assert_eq!(
        config
            .read_table
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.id))
            .collect::<Vec<_>>(),
        [("id", 1), ("balance", 2)]
    );

    for descriptor in [
        "id",
        "id:1",
        "id:1:clustered-pk:extra",
        "id:0:clustered-pk",
        "id:not-an-id:clustered-pk",
        "id:1:nullable",
    ] {
        let mut args = required().to_vec();
        let position = args
            .iter()
            .position(|argument| *argument == "id:1:clustered-pk")
            .unwrap();
        args[position] = descriptor;
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--column"
        ));
    }
}

#[test]
fn invalid_column_catalogs_fail_before_startup() {
    let cases = [
        ["--column", "ID:3:stored-not-null"].as_slice(),
        ["--column", "other:2:stored-not-null"].as_slice(),
        ["--column", "other:3:clustered-pk"].as_slice(),
    ];
    for extra in cases {
        let mut args = required().to_vec();
        args.extend_from_slice(extra);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--column"
        ));
    }

    let mut no_primary_key = required().to_vec();
    let primary = no_primary_key
        .iter()
        .position(|argument| *argument == "id:1:clustered-pk")
        .unwrap();
    no_primary_key[primary] = "id:1:stored-not-null";
    assert!(matches!(
        NodeConfig::parse(no_primary_key),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--column"
    ));

    let mut missing = required().to_vec();
    let option = missing
        .iter()
        .position(|argument| *argument == "--column")
        .unwrap();
    missing.drain(option..=option + 1);
    let option = missing
        .iter()
        .position(|argument| *argument == "--column")
        .unwrap();
    missing.drain(option..=option + 1);
    assert_eq!(
        NodeConfig::parse(missing),
        Err(NodeConfigError::MissingOption("--column"))
    );
}
