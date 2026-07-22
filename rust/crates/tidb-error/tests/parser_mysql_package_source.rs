// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Exact translation of `pkg/parser/mysql/error_test.go` plus catalog/state
//! invariants required by the complete package claim.

use tidb_error::mysql::errcode::{ErrNoDB, ErrWrongValueForVar};
use tidb_error::mysql::{mysql_state, FormatArg, SqlError, CATALOG, DEFAULT_MYSQL_STATE};

#[test]
fn test_sql_error() {
    let error = SqlError::new_f(ErrNoDB, "no db error", &[], &[]);
    assert_eq!(error.to_string(), "ERROR 1046 (3D000): no db error");
    let error = SqlError::new_f(0, "customized error", &[], &[]);
    assert_eq!(error.to_string(), "ERROR 0 (HY000): customized error");
    let error = SqlError::new(ErrNoDB, &[]);
    assert_eq!(
        error.to_string(),
        "ERROR 1046 (3D000): No database selected"
    );
    let error = SqlError::new(0, &[FormatArg::from("customized error"), FormatArg::nil()]);
    assert_eq!(error.to_string(), "ERROR 0 (HY000): customized error<nil>");
}

#[test]
fn generated_catalog_and_state_are_total_for_source_contracts() {
    assert_eq!(CATALOG.len(), 952);
    assert_eq!(mysql_state(ErrNoDB), "3D000");
    assert_eq!(mysql_state(0), DEFAULT_MYSQL_STATE);
    assert_eq!(
        SqlError::new(ErrWrongValueForVar, &["sql_mode".into(), "bad".into()]).message,
        "Variable 'sql_mode' can't be set to the value of 'bad'"
    );
    assert_eq!(
        SqlError::new_f(0, "command %d not supported now", &[], &[3_u8.into()]).message,
        "command 3 not supported now"
    );
    assert_eq!(
        SqlError::new(0, &[1_i64.into(), 2_i64.into()]).message,
        "1 2"
    );
}
