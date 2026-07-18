// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use tidb_stats::auto_analyze_runtime::interval;
use tidb_stats::auto_analyze_runtime::*;

struct Sql {
    f64_row: Option<f64>,
    i64_row: Option<i64>,
    statements: Vec<SqlStatement>,
}
impl SqlPort for Sql {
    fn query_optional_f64(&mut self, statement: &SqlStatement) -> RuntimeResult<Option<f64>> {
        self.statements.push(statement.clone());
        Ok(self.f64_row)
    }
    fn query_optional_i64(&mut self, statement: &SqlStatement) -> RuntimeResult<Option<i64>> {
        self.statements.push(statement.clone());
        Ok(self.i64_row)
    }
    fn execute(&mut self, _: &SqlStatement) -> RuntimeResult<()> {
        Ok(())
    }
}

#[test]
fn average_duration_uses_table_and_partition_queries_and_truncates_seconds() {
    let mut sql = Sql {
        f64_row: Some(3.9),
        i64_row: None,
        statements: vec![],
    };
    assert_eq!(
        average_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        3_000_000_000
    );
    average_analysis_duration(&mut sql, "s", "t", &["p0".into()]).unwrap();
    assert!(sql.statements[0].sql.contains("partition_name = ''"));
    assert!(sql.statements[1].sql.contains("partition_name in (%?)"));
}

#[test]
fn absent_and_negative_average_rows_are_no_record() {
    let mut sql = Sql {
        f64_row: None,
        i64_row: None,
        statements: vec![],
    };
    assert_eq!(
        average_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        interval::NO_RECORD
    );
    sql.f64_row = Some(-1.0);
    assert_eq!(
        average_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        interval::NO_RECORD
    );
}

#[test]
fn failed_duration_preserves_absent_zero_positive_and_negative_states() {
    let mut sql = Sql {
        f64_row: None,
        i64_row: None,
        statements: vec![],
    };
    assert_eq!(
        last_failed_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        interval::NO_RECORD
    );
    sql.i64_row = Some(0);
    assert_eq!(
        last_failed_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        interval::JUST_FAILED
    );
    sql.i64_row = Some(5);
    assert_eq!(
        last_failed_analysis_duration(&mut sql, "s", "t", &[]).unwrap(),
        5_000_000_000
    );
    sql.i64_row = Some(-1);
    assert_eq!(
        last_failed_analysis_duration(&mut sql, "s", "t", &["p".into()]).unwrap(),
        interval::DEFAULT_FAILED_ANALYSIS_WAIT_NANOS
    );
    assert!(sql
        .statements
        .last()
        .unwrap()
        .sql
        .contains("GROUP BY partition_name"));
}
