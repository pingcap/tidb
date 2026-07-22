// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Exact translations of all ten test functions in `pkg/parser/mysql` and
//! focused assertions for the previously untested package production tables.

#![allow(non_upper_case_globals)]

use tidb_mysql::*;

#[test]
fn test_sql_mode() {
    let modes = [
        ModeRealAsFloat,
        ModePipesAsConcat,
        ModeANSIQuotes,
        ModeIgnoreSpace,
        ModeNotUsed,
        ModeOnlyFullGroupBy,
        ModeNoUnsignedSubtraction,
        ModeNoDirInCreate,
        ModePostgreSQL,
        ModeOracle,
        ModeMsSQL,
        ModeDb2,
        ModeMaxdb,
        ModeNoKeyOptions,
        ModeNoTableOptions,
        ModeNoFieldOptions,
        ModeMySQL323,
        ModeMySQL40,
        ModeANSI,
        ModeNoAutoValueOnZero,
        ModeNoBackslashEscapes,
        ModeStrictTransTables,
        ModeStrictAllTables,
        ModeNoZeroInDate,
        ModeNoZeroDate,
        ModeInvalidDates,
        ModeErrorForDivisionByZero,
        ModeTraditional,
        ModeNoAutoCreateUser,
        ModeHighNotPrecedence,
        ModeNoEngineSubstitution,
        ModePadCharToFullLength,
    ];
    for (bit, mode) in modes.into_iter().enumerate() {
        assert_eq!(mode.0, 1_i64 << bit)
    }
    assert_eq!(ModeAllowInvalidDates.0, 1_i64 << 32);
    assert!(set_sql_mode(ModeNone, ModeStrictTransTables).has_strict_mode());
    assert_eq!(delete_sql_mode(ModeANSI | ModeOracle, ModeANSI), ModeOracle);
    assert_eq!(
        format_sql_mode_str("ansi,ANSI  "),
        "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
    );
    let invalid = get_sql_mode("ANSI_QUOTES,BOGUS").unwrap_err();
    assert_eq!(invalid.partial, ModeANSIQuotes);
    assert_eq!(invalid.value, "BOGUS");
}

#[test]
fn test_version_separator() {
    assert_eq!(VersionSeparator, "-TiDB-")
}

#[test]
fn test_build_tidbx_release_version() {
    assert_eq!(
        build_tidbx_release_version("v26.3.0").unwrap(),
        "CLOUD.202603.0"
    );
    assert_eq!(
        build_tidbx_release_version("v26.3.0-xxx").unwrap(),
        "CLOUD.202603.0-xxx"
    );
    assert_eq!(
        build_tidbx_server_version("v26.3.0").unwrap(),
        "8.0.11-TiDB-CLOUD.202603.0"
    );
    assert_eq!(
        build_tidbx_server_version("v26.3.0-xxx").unwrap(),
        "8.0.11-TiDB-CLOUD.202603.0-xxx"
    );
    for version in ["26.1.1", "v26xxxx", "v24.1.1", "v26.0.1", "v26.13.1"] {
        assert!(build_tidbx_release_version(version)
            .unwrap_err()
            .to_string()
            .contains("invalid TiDB release version"));
    }
}

#[test]
fn test_normalize_tidb_release_version_for_next_gen() {
    assert_eq!(
        normalize_tidb_release_version_for_next_gen(LEGACY_TIDB_RELEASE_VERSION_PLACEHOLDER),
        TIDBX_PLACEHOLDER_RELEASE_VERSION
    );
    assert_eq!(
        normalize_tidb_release_version_for_next_gen("v26.3.0"),
        "v26.3.0"
    );
}

#[test]
fn test_priv_string() {
    let mut privilege = UsagePriv;
    for bit in 0..=33 {
        assert!(!privilege.as_str().is_empty(), "{bit}-th");
        privilege = privilege << 1;
    }
}

#[test]
fn test_priv_column() {
    for privilege in ALL_GLOBAL_PRIVILEGES
        .iter()
        .chain(STATIC_GLOBAL_ONLY_PRIVILEGES)
        .chain(ALL_DATABASE_PRIVILEGES)
    {
        assert!(!privilege.column_string().is_empty(), "{privilege}");
        assert_eq!(
            privilege_from_column(privilege.column_string()),
            Some(*privilege)
        );
    }
}

#[test]
fn test_priv_set_string() {
    for privilege in ALL_TABLE_PRIVILEGES.iter().chain(ALL_COLUMN_PRIVILEGES) {
        assert!(!privilege.set_string().is_empty(), "{privilege}");
        assert_eq!(
            privilege_from_set_enum(privilege.set_string()),
            Some(*privilege)
        );
    }
}

#[test]
fn test_privs_has() {
    assert!(has_privilege(&[AllPriv], AllPriv));
    assert!(!has_privilege(&[AllPriv], InsertPriv));
    let privileges = [InsertPriv, SelectPriv];
    assert!(has_privilege(&privileges, SelectPriv));
    assert!(has_privilege(&privileges, InsertPriv));
    assert!(!has_privilege(&privileges, DropPriv));
}

#[test]
fn test_priv_all_consistency() {
    for bit in 1..33 {
        let privilege = PrivilegeType(1 << bit);
        assert!(
            !privilege.column_string().is_empty(),
            "priv fail {}",
            privilege.0
        )
    }
    assert_eq!(
        ALL_GLOBAL_PRIVILEGES.len() + 1,
        PRIVILEGE_USER_COLUMNS.len()
    );
    assert_eq!(PRIVILEGE_USER_COLUMNS.len() + 2, PRIVILEGE_NAMES.len());
}

#[test]
fn test_flags() {
    assert!(has_not_null_flag(NotNullFlag));
    assert!(has_uni_key_flag(UniqueKeyFlag));
    assert!(has_no_default_value_flag(NoDefaultValueFlag));
    assert!(has_auto_increment_flag(AutoIncrementFlag));
    assert!(has_unsigned_flag(UnsignedFlag));
    assert!(has_zerofill_flag(ZerofillFlag));
    assert!(has_binary_flag(BinaryFlag));
    assert!(has_pri_key_flag(PriKeyFlag));
    assert!(has_multiple_key_flag(MultipleKeyFlag));
    assert!(has_timestamp_flag(TimestampFlag));
    assert!(has_on_update_now_flag(OnUpdateNowFlag));
}

#[test]
fn production_tables_and_locale_helpers_match_source() {
    assert_eq!(CHARSET_IDS.len(), 41);
    assert_eq!(charset_name_to_id("utf8mb4"), 46);
    assert_eq!(charset_name_to_id("UTF8MB4"), 0);
    assert_eq!(charset_name_to_id("unknown"), 0);
    assert_eq!(collation_name(309), Some("utf8mb4_0900_bin"));
    assert_eq!(collation_id("utf8mb4_0900_ai_ci"), Some(255));
    for &(id, name) in COLLATIONS {
        assert_eq!(collation_id(name), Some(id));
        assert_eq!(collation_name(id), Some(name));
    }
    assert_eq!(
        format_by_locale("1234567.8", "2", "de_DE").unwrap(),
        ("1.234.567,80".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("1234567890.1234", "3", "en_IN").unwrap(),
        ("1,23,45,67,890.123".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("bad", "2", "not_REAL").unwrap(),
        ("0.00".to_owned(), false)
    );
    assert_eq!(
        format_by_locale("12.3.4", "2", "en_US").unwrap(),
        ("12.00".to_owned(), true)
    );
    assert_eq!(default_field_length_and_decimal(TypeLonglong), (20, 0));
    assert_eq!(
        default_field_length_and_decimal_for_cast(TypeJSON),
        (4_194_304, 0)
    );
    assert!(is_auth_plugin_clear_text(AuthCachingSha2Password));
    assert_eq!(command_name(ComQuery), Some("Query"));
    assert_eq!(RANGE_GRAPH.len(), 28);
}
