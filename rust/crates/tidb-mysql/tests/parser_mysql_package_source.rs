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
    assert_eq!(invalid.sql_error.code, 1231);
    assert_eq!(invalid.sql_error.state, "42000");
    assert_eq!(
        invalid.to_string(),
        "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'BOGUS'"
    );

    let oversized = "x".repeat(201);
    let invalid = get_sql_mode(&format!("ANSI_QUOTES,{oversized}")).unwrap_err();
    assert_eq!(invalid.partial, ModeANSIQuotes);
    assert_eq!(invalid.value, oversized);
    assert_eq!(
        invalid.sql_error.message,
        format!(
            "Variable 'sql_mode' can't be set to the value of '{}'",
            "x".repeat(200)
        )
    );
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
    assert_eq!(
        build_tidbx_release_version("v026.03.00").unwrap(),
        "CLOUD.202603.0"
    );
    assert_eq!(
        build_tidbx_release_version("v26.03.00-01+build").unwrap(),
        "CLOUD.202603.0-01"
    );
    for version in ["v18446744073709551615.1.1", "v100.1.1"] {
        assert!(build_tidbx_release_version(version).is_err());
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
fn runtime_versions_preserve_build_defaults_and_mutation_semantics() {
    static VERSION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = VERSION_TEST_LOCK.lock().unwrap();

    reset_runtime_versions();
    assert_eq!(
        runtime_versions(),
        RuntimeVersions {
            tidb_release_version: TIDB_RELEASE_VERSION.to_owned(),
            server_version: format!("8.0.11{VersionSeparator}{TIDB_RELEASE_VERSION}"),
        }
    );

    set_runtime_versions("v26.3.0", "8.0.11-TiDB-CLOUD.202603.0");
    assert_eq!(
        runtime_versions(),
        RuntimeVersions {
            tidb_release_version: "v26.3.0".to_owned(),
            server_version: "8.0.11-TiDB-CLOUD.202603.0".to_owned(),
        }
    );
    reset_runtime_versions();
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

#[test]
fn range_graph_matches_unicode_is_one_of_union() {
    for character in ['A', '_', '-', '١', 'Ⅷ', '½', '\u{0301}', '中', '🦀'] {
        assert!(is_range_graph(character), "U+{:04X}", character as u32);
    }
    for character in [' ', '\n', '\u{200D}', '\u{E000}', '\u{0378}'] {
        assert!(!is_range_graph(character), "U+{:04X}", character as u32);
    }
    assert_eq!(
        RANGE_GRAPH
            .iter()
            .filter(|category| **category == RangeGraphCategory::PunctuationDash)
            .count(),
        2
    );
    assert_eq!(
        RANGE_GRAPH
            .iter()
            .filter(|category| **category == RangeGraphCategory::NumberLetter)
            .count(),
        3
    );
}

#[test]
fn locale_unicode_edges_follow_go_without_panicking() {
    assert_eq!(
        format_by_locale("١٢٣٤.٥", "2", "en_US").unwrap(),
        ("0.00".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("Ⅷ", "2", "en_US").unwrap(),
        ("0.00".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("½", "2", "en_US").unwrap(),
        ("0.00".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("1234.5", "٢", "en_US").unwrap(),
        ("1,234".to_owned(), true)
    );
    // Go byte-groups this row into invalid UTF-8. Rust preserves the same
    // accepted Nd input but groups scalar values to keep its String valid.
    assert_eq!(
        format_by_locale("1٢34.5", "2", "en_US").unwrap(),
        ("1,٢34.50".to_owned(), true)
    );
    // These byte-grouping boundaries remain valid UTF-8, so Rust preserves
    // the Go result exactly instead of invoking the String normalization.
    assert_eq!(
        format_by_locale("12٣4.5", "2", "en_US").unwrap(),
        ("12,٣4.50".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("123٤.5", "2", "en_US").unwrap(),
        ("12,3٤.50".to_owned(), true)
    );
    // Nl and No are numeric categories, but Go unicode.IsDigit accepts only
    // Nd here, so scanning stops before them.
    assert_eq!(
        format_by_locale("1Ⅷ2.3", "2", "en_US").unwrap(),
        ("1.00".to_owned(), true)
    );
    assert_eq!(
        format_by_locale("1½2.3", "2", "en_US").unwrap(),
        ("1.00".to_owned(), true)
    );
}
