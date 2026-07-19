// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::{IpAddr, Ipv4Addr};

use tidb_server::{ConfiguredReadTable, NodeConfig, NodeConfigError};

fn required() -> [&'static str; 13] {
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
        "id",
        "--column-id",
        "1",
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
    assert_eq!(
        config.read_table,
        ConfiguredReadTable {
            database: "campaign19".to_owned(),
            table: "rows".to_owned(),
            table_id: 42,
            column: "id".to_owned(),
            column_id: 1,
        }
    );
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
}

#[test]
fn empty_password_boundary_cannot_bind_a_public_address() {
    // pkg/config/config_test.go:1850 TestTcpNoDelay
    let mut public = required().to_vec();
    public.extend(["--host", "0.0.0.0"]);
    assert!(matches!(
        NodeConfig::parse(public),
        Err(NodeConfigError::NonLoopbackHost(address)) if address == IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    ));
}

#[test]
fn topology_ids_packet_limit_and_help_are_checked_before_startup() {
    // cmd/tidb-server/main_test.go:56 TestExitCodeForSignal
    // pkg/config/config_test.go:1317 TestTxnTotalSizeLimitValid
    for (option, value) in [
        ("--table-id", "0"),
        ("--column-id", "-1"),
        ("--max-allowed-packet", "0"),
    ] {
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
}
