// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tidb_server::{ConfiguredReadColumnKind, NodeConfig, NodeConfigError};

fn required() -> Vec<&'static str> {
    vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379,127.0.0.1:2380",
        "--read-table",
        "campaign19",
        "rows",
        "42",
        "2",
        "id:1:clustered-pk",
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
    assert_eq!(config.read_tables.len(), 1);
    assert_eq!(config.read_tables[0].database, "campaign19");
    assert_eq!(config.read_tables[0].table, "rows");
    assert_eq!(config.read_tables[0].table_id, 42);
    assert_eq!(config.read_tables[0].columns.len(), 2);
    assert_eq!(config.read_tables[0].columns[0].name, "id");
    assert_eq!(config.read_tables[0].columns[0].id, 1);
    assert_eq!(config.read_tables[0].columns[1].name, "balance");
    assert_eq!(config.read_tables[0].columns[1].id, 2);
    assert_eq!(
        config.auth_file,
        std::path::Path::new("/tmp/campaign21-users.tsv")
    );
    assert_eq!(config.max_connections, 8);
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.max_topn_rows, 1_024);
}

/// A `--read-table` for the sysbench `sbtest1` shape with a trailing non-unique
/// index on `k`, followed by the required auth file.
fn indexed_table_args() -> Vec<&'static str> {
    vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--read-table",
        "sbtest",
        "sbtest1",
        "900",
        "4",
        "id:1:clustered-pk",
        "k:2:stored-int-not-null",
        "c:3:stored-char-not-null:120",
        "pad:4:stored-char-not-null:60",
        "1",         // index count
        "k_idx:5:2", // non-unique index k_idx (id 5) over column id 2
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ]
}

#[test]
fn a_trailing_index_section_declares_a_non_unique_secondary_index() {
    let config = NodeConfig::parse(indexed_table_args()).unwrap();
    let table = &config.read_tables[0];
    assert_eq!(table.columns.len(), 4);
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(table.indexes[0].name, "k_idx");
    assert_eq!(table.indexes[0].index_id, 5);
    assert_eq!(table.indexes[0].column_id, 2);
}

#[test]
fn a_table_without_an_index_section_has_no_indexes() {
    // `required()` stops at its columns; the next argument is another option, so
    // no index section is parsed — the pre-index descriptor grammar still holds.
    let config = NodeConfig::parse(required()).unwrap();
    assert!(config.read_tables[0].indexes.is_empty());
}

#[test]
fn an_index_over_an_unknown_column_is_rejected() {
    let mut args = indexed_table_args();
    let position = args.iter().position(|arg| *arg == "k_idx:5:2").unwrap();
    args[position] = "k_idx:5:99"; // column id 99 is not declared
    assert!(matches!(
        NodeConfig::parse(args),
        Err(NodeConfigError::InvalidValue { .. })
    ));
}

#[test]
fn a_malformed_index_descriptor_is_rejected() {
    let mut args = indexed_table_args();
    let position = args.iter().position(|arg| *arg == "k_idx:5:2").unwrap();
    args[position] = "k_idx:5"; // missing the column id field
    assert!(matches!(
        NodeConfig::parse(args),
        Err(NodeConfigError::InvalidValue { .. })
    ));
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

    let mut config_file = required();
    config_file.extend(["--config", "tidb.toml"]);
    assert!(matches!(
        NodeConfig::parse(config_file),
        Err(NodeConfigError::UnknownOption(option)) if option == "--config"
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
fn authentication_file_is_required_before_startup() {
    let mut missing = required();
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
        let mut args = required();
        args.extend(["--max-connections", count]);
        assert_eq!(
            NodeConfig::parse(args).unwrap().max_connections,
            count.parse::<usize>().unwrap()
        );
    }
    for count in ["0", "257"] {
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
fn topn_heap_cap_is_process_wide_positive_bounded_and_unambiguous() {
    // pkg/config/config_test.go:960 TestConfig
    // pkg/config/config_test.go:1317 TestTxnTotalSizeLimitValid
    // pkg/config/config_test.go:1689 TestIndexLimit
    let mut configured = required();
    configured.extend(["--max-topn-rows", "65536"]);
    assert_eq!(NodeConfig::parse(configured).unwrap().max_topn_rows, 65_536);

    let mut inline = required();
    inline.push("--max-topn-rows=16");
    assert_eq!(NodeConfig::parse(inline).unwrap().max_topn_rows, 16);

    for value in ["0", "65537", "18446744073709551616", "not-a-number"] {
        let mut args = required();
        args.extend(["--max-topn-rows", value]);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--max-topn-rows"
        ));
    }

    let mut duplicate = required();
    duplicate.extend(["--max-topn-rows=16", "--max-topn-rows", "32"]);
    assert!(matches!(
        NodeConfig::parse(duplicate),
        Err(NodeConfigError::DuplicateOption(option)) if option == "--max-topn-rows"
    ));
    assert!(NodeConfig::help_text().contains("--max-topn-rows <rows>"));
}

#[test]
fn topology_ids_packet_limit_and_help_are_checked_before_startup() {
    // cmd/tidb-server/main_test.go:56 TestExitCodeForSignal
    // pkg/config/config_test.go:1317 TestTxnTotalSizeLimitValid
    for (position, option, value) in [(6, "--read-table", "0"), (0, "--max-allowed-packet", "0")] {
        let mut args = required();
        if option == "--read-table" {
            args[position] = value;
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
    assert!(NodeConfig::help_text().contains("--read-table"));
    assert!(!NodeConfig::help_text().contains("--column-id"));
    assert!(NodeConfig::help_text().contains("stored-not-null"));
}

#[test]
fn repeated_column_descriptors_are_atomic_ordered_and_complete() {
    // pkg/config/config_test.go:1701 TestTableColumnCountLimit
    let config = NodeConfig::parse(required()).unwrap();
    assert_eq!(
        config.read_tables[0]
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
        let mut args = required();
        let position = args
            .iter()
            .position(|argument| *argument == "id:1:clustered-pk")
            .unwrap();
        args[position] = descriptor;
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
        ));
    }
}

#[test]
fn invalid_column_catalogs_fail_before_startup() {
    for extra in [
        "ID:3:stored-not-null",
        "other:2:stored-not-null",
        "other:3:clustered-pk",
    ] {
        let mut args = required();
        args[7] = "3";
        let auth_file = args
            .iter()
            .position(|argument| *argument == "--auth-file")
            .unwrap();
        args.insert(auth_file, extra);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
        ));
    }

    let mut no_primary_key = required();
    let primary = no_primary_key
        .iter()
        .position(|argument| *argument == "id:1:clustered-pk")
        .unwrap();
    no_primary_key[primary] = "id:1:stored-not-null";
    assert!(matches!(
        NodeConfig::parse(no_primary_key),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
    ));

    let mut missing = required();
    missing[7] = "0";
    assert!(matches!(
        NodeConfig::parse(missing),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
    ));
}

#[test]
fn configured_read_tables_are_atomic_ordered_and_globally_unique() {
    let mut args = required();
    args.extend([
        "--read-table",
        "campaign19",
        "accounts",
        "43",
        "2",
        "account_id:1:clustered-pk",
        "balance:2:stored-not-null",
    ]);
    let config = NodeConfig::parse(args).unwrap();
    assert_eq!(
        config
            .read_tables
            .iter()
            .map(|table| (
                table.database.as_str(),
                table.table.as_str(),
                table.table_id
            ))
            .collect::<Vec<_>>(),
        [("campaign19", "rows", 42), ("campaign19", "accounts", 43)]
    );

    for duplicate in [
        [
            "--read-table",
            "CAMPAIGN19",
            "ROWS",
            "43",
            "1",
            "id:1:clustered-pk",
        ],
        [
            "--read-table",
            "campaign19",
            "accounts",
            "42",
            "1",
            "id:1:clustered-pk",
        ],
    ] {
        let mut args = required();
        args.extend(duplicate);
        assert!(matches!(
            NodeConfig::parse(args),
            Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
        ));
    }

    let mut truncated = required();
    truncated.extend([
        "--read-table",
        "campaign19",
        "accounts",
        "43",
        "2",
        "id:1:clustered-pk",
        "--max-connections",
        "3",
    ]);
    assert_eq!(
        NodeConfig::parse(truncated),
        Err(NodeConfigError::MissingValue("--read-table".to_owned()))
    );

    let mut truncated_before_short_option = required();
    truncated_before_short_option.extend([
        "--read-table",
        "campaign19",
        "accounts",
        "43",
        "2",
        "id:1:clustered-pk",
        "-P",
        "4406",
    ]);
    assert_eq!(
        NodeConfig::parse(truncated_before_short_option),
        Err(NodeConfigError::MissingValue("--read-table".to_owned()))
    );

    let mut oversized = required();
    oversized.extend(["--read-table", "campaign19", "accounts", "43", "4097"]);
    assert!(matches!(
        NodeConfig::parse(oversized),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
    ));

    let mut too_many = required();
    too_many.extend([
        "--read-table",
        "campaign19",
        "accounts",
        "43",
        "1",
        "id:1:clustered-pk",
        "--read-table",
        "campaign19",
        "profiles",
        "44",
        "1",
        "id:1:clustered-pk",
    ]);
    assert!(matches!(
        NodeConfig::parse(too_many),
        Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
    ));

    let mut legacy = required();
    legacy.extend(["--database", "parallel-singular-path"]);
    assert!(matches!(
        NodeConfig::parse(legacy),
        Err(NodeConfigError::UnknownOption(option)) if option == "--database"
    ));
}

/// The deployable node accepts the sysbench `sbtest` column shape: a BIGINT
/// clustered PK, an INT column, and two CHAR(N) columns. This is the
/// configuration surface that unlocks string-column workloads through
/// `run_configured_node`.
fn sbtest_shaped() -> Vec<&'static str> {
    vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--read-table",
        "sbtest",
        "sbtest1",
        "108",
        "4",
        "id:1:clustered-pk",
        "k:2:stored-int-not-null",
        "c:3:stored-char-not-null:120",
        "pad:4:stored-char-not-null:60",
        "--auth-file",
        "/tmp/sbtest-users.tsv",
    ]
}

#[test]
fn typed_stored_columns_admit_bigint_int_and_char() {
    let config = NodeConfig::parse(sbtest_shaped()).unwrap();
    let columns = &config.read_tables[0].columns;
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0].name, "id");
    assert_eq!(
        columns[0].kind,
        ConfiguredReadColumnKind::ClusteredPrimaryKey
    );
    assert_eq!(columns[1].name, "k");
    assert_eq!(columns[1].kind, ConfiguredReadColumnKind::StoredIntNotNull);
    assert_eq!(columns[2].name, "c");
    assert_eq!(
        columns[2].kind,
        ConfiguredReadColumnKind::StoredCharNotNull { max_length: 120 }
    );
    assert_eq!(columns[3].name, "pad");
    assert_eq!(
        columns[3].kind,
        ConfiguredReadColumnKind::StoredCharNotNull { max_length: 60 }
    );
}

#[test]
fn char_column_length_is_required_and_range_checked() {
    // Replaces the CHAR `c` descriptor (index 10 in `sbtest_shaped`).
    let with_char = |descriptor: &'static str| {
        let mut args = sbtest_shaped();
        args[10] = descriptor;
        NodeConfig::parse(args)
    };

    // Baseline parses.
    assert!(with_char("c:3:stored-char-not-null:120").is_ok());

    for bad in [
        "c:3:stored-char-not-null",     // missing length
        "c:3:stored-char-not-null:0",   // below range
        "c:3:stored-char-not-null:256", // above MySQL CHAR max
        "c:3:stored-char-not-null:xyz", // non-numeric length
        "c:3:clustered-pk:120",         // length on a non-char kind
        "c:3:stored-not-null:120",      // length on a non-char kind
        "c:3:unknown-kind",             // unknown kind
        "c:3:stored-char-not-null:1:2", // too many fields
    ] {
        assert!(
            matches!(
                with_char(bad),
                Err(NodeConfigError::InvalidValue { option, .. }) if option == "--read-table"
            ),
            "descriptor {bad:?} must be rejected"
        );
    }
}

#[test]
fn load_table_names_a_schema_the_cluster_already_stores() {
    let config = NodeConfig::parse(vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--load-table",
        "campaign.rows",
        "--load-table",
        "campaign.Notes",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ])
    .unwrap();
    // A loaded table needs no command-line schema at all.
    assert!(config.read_tables.is_empty());
    assert_eq!(config.load_tables.len(), 2);
    assert_eq!(config.load_tables[0].database, "campaign");
    assert_eq!(config.load_tables[0].table, "rows");
    assert_eq!(config.load_tables[1].table, "Notes");
}

#[test]
fn load_table_requires_a_qualified_name() {
    let error = NodeConfig::parse(vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--load-table",
        "rows",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
    ])
    .unwrap_err();
    assert!(
        format!("{error:?}").contains("--load-table"),
        "unexpected error: {error:?}"
    );
}

/// `--cluster-session` is the one mode that names no table: it serves the
/// cluster's whole loaded catalog through the wide-SQL session driver.
#[test]
fn cluster_session_needs_no_table_and_rejects_a_bounded_one() {
    let config = NodeConfig::parse(vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--cluster-session",
        "--load-privileges",
    ])
    .unwrap();
    assert!(config.cluster_session);
    assert!(config.read_tables.is_empty());
    assert!(config.load_tables.is_empty());
    assert!(config.load_privileges);

    let error = NodeConfig::parse(vec![
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--cluster-session",
        "--load-table",
        "campaign.rows",
        "--load-privileges",
    ])
    .unwrap_err();
    assert!(
        format!("{error:?}").contains("--cluster-session"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn a_table_cannot_be_both_described_and_loaded() {
    let mut arguments = required();
    arguments.extend(["--load-table", "campaign19.rows"]);
    let error = NodeConfig::parse(arguments).unwrap_err();
    assert!(
        format!("{error:?}").contains("--load-table"),
        "unexpected error: {error:?}"
    );
}
