// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Exact source obligations for the shared MySQL/TiDB error authority.

use tidb_error::{mysql, tidb};

#[test]
fn parser_mysql_catalog_translates_all_source_entries() {
    // Sources: pkg/parser/mysql/errcode.go and errname.go.
    assert_eq!(mysql::errcode::ALL_CODES.len(), 954);
    assert_eq!(mysql::CATALOG.len(), 952);
    for &(name, code) in mysql::errcode::ALL_CODES {
        if name.starts_with("Err") || name.starts_with("Warn") {
            assert!(
                mysql::message_by_code(code).is_some(),
                "{name} ({code}) has no parser/MySQL message"
            );
        }
    }
    assert_eq!(
        mysql::message_by_code(mysql::errcode::ErrDupEntry),
        Some(&mysql::errname::ErrDupEntry)
    );
    assert!(mysql::errname::ErrDupEntry.redact_arg_pos.is_empty());
    assert_eq!(tidb::errname::ErrDupEntry.redact_arg_pos, &[0]);
}

#[test]
fn parser_mysql_state_table_is_complete_and_defaults_unknown_codes() {
    // Source: pkg/parser/mysql/state.go.
    assert_eq!(mysql::MYSQL_STATES.len(), 244);
    assert_eq!(mysql::mysql_state(mysql::errcode::ErrNoDB), "3D000");
    assert_eq!(
        mysql::mysql_state(mysql::errcode::ErrWarnDataOutOfRange),
        "22003"
    );
    assert_eq!(mysql::mysql_state(0), mysql::DEFAULT_MYSQL_STATE);
}

#[test]
fn sql_error_constructors_preserve_source_code_state_message_and_display() {
    // Source obligations:
    // pkg/parser/mysql/error_test.go:22:TestSQLError
    // pkg/parser/mysql/error_test.go:0:error_test.go
    let error = mysql::SqlError::new_f(mysql::errcode::ErrNoDB, "no db error", &[], &[]);
    assert_eq!(error.code, 1046);
    assert_eq!(error.state, "3D000");
    assert_eq!(error.message, "no db error");
    assert_eq!(error.to_string(), "ERROR 1046 (3D000): no db error");

    let custom = mysql::SqlError::new_f(0, "customized error", &[], &[]);
    assert_eq!(custom.to_string(), "ERROR 0 (HY000): customized error");

    let catalog = mysql::SqlError::new(mysql::errcode::ErrNoDB, &[]);
    assert_eq!(catalog.message, "No database selected");
    let unknown = mysql::SqlError::new(
        0,
        &[
            mysql::FormatArg::from("customized error"),
            mysql::FormatArg::nil(),
        ],
    );
    assert_eq!(unknown.message, "customized error<nil>");
}

#[test]
fn tidb_catalog_has_a_message_for_every_source_error_code() {
    // Source obligations:
    // pkg/errno/errname_test.go:29:TestAllErrCodeHasMsg
    // pkg/errno/errname_test.go:26:pkg/errno/errcode.go
    // pkg/errno/errname_test.go:0:errname_test.go
    assert_eq!(tidb::errcode::ALL_CODES.len(), 1166);
    assert_eq!(tidb::CATALOG.len(), 1164);
    for &(name, code) in tidb::errcode::ALL_CODES {
        if name.starts_with("Err") {
            assert!(
                tidb::message_by_code(code).is_some(),
                "{name} ({code}) has no TiDB message"
            );
        }
    }
}

#[test]
fn tidb_catalog_preserves_reserved_range_and_extended_metadata() {
    // Source obligations:
    // pkg/errno/errname_test.go:49:TestReservedErrCodeRange
    // pkg/errno/main_test.go:24:TestMain
    // pkg/errno/main_test.go:0:main_test.go
    for &(name, code) in tidb::errcode::ALL_CODES {
        if name.starts_with("Err") {
            assert!(
                !(8800..8900).contains(&code),
                "{name} uses reserved code {code}"
            );
        }
    }
    assert_eq!(tidb::errcode::ErrUserPrefixMismatch, 20_003);
    assert_eq!(
        tidb::errname::ErrWriteConflict.redact_arg_pos,
        &[3, 4, 5, 6]
    );
    assert_eq!(
        tidb::errname::ErrDBaccessDenied.raw,
        "Access denied for user '%-.48s'@'%-.255s' to database '%-.192s'"
    );
}
