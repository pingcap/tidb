// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use super::model::{RuntimeResult, SqlStatement, SqlValue};
use super::ports::SqlPort;

pub const NO_RECORD: i64 = -1;
pub const JUST_FAILED: i64 = 0;
pub const DEFAULT_FAILED_ANALYSIS_WAIT_NANOS: i64 = 30 * 60 * 1_000_000_000;

pub const AVG_TABLE: &str = "SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration FROM (SELECT start_time, end_time FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND state = 'finished' AND fail_reason IS NULL AND partition_name = '' ORDER BY id DESC LIMIT 5) AS recent_analyses;";
pub const AVG_PARTITIONS: &str = "SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration FROM (SELECT start_time, end_time FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND state = 'finished' AND fail_reason IS NULL AND partition_name in (%?) ORDER BY id DESC LIMIT 5) AS recent_analyses;";
pub const FAILED_TABLE: &str = "SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP) FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND state = 'failed' AND partition_name = '' ORDER BY id DESC LIMIT 1;";
pub const FAILED_PARTITIONS: &str = "SELECT MIN(TIMESTAMPDIFF(SECOND, aj.start_time, CURRENT_TIMESTAMP)) AS min_duration FROM (SELECT MAX(id) AS max_id FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND state = 'failed' AND partition_name IN (%?) GROUP BY partition_name) AS latest_failures JOIN mysql.analyze_jobs aj ON aj.id = latest_failures.max_id;";

#[must_use]
pub const fn average_duration_query(has_partitions: bool) -> &'static str {
    if has_partitions {
        AVG_PARTITIONS
    } else {
        AVG_TABLE
    }
}

#[must_use]
pub const fn last_failed_duration_query(has_partitions: bool) -> &'static str {
    if has_partitions {
        FAILED_PARTITIONS
    } else {
        FAILED_TABLE
    }
}

#[must_use]
pub fn average_analysis_duration_from_seconds(seconds: Option<f64>) -> i64 {
    match seconds {
        Some(value) if value.is_finite() && value >= 0.0 => {
            (value as i64).wrapping_mul(1_000_000_000)
        }
        _ => NO_RECORD,
    }
}

#[must_use]
pub fn last_failed_analysis_duration_from_seconds(seconds: Option<i64>) -> i64 {
    match seconds {
        None => NO_RECORD,
        Some(0) => JUST_FAILED,
        Some(value) if value < 0 => DEFAULT_FAILED_ANALYSIS_WAIT_NANOS,
        Some(value) => value.wrapping_mul(1_000_000_000),
    }
}

fn statement(sql: &str, schema: &str, table: &str, partitions: &[String]) -> SqlStatement {
    let mut params = vec![
        SqlValue::Identifier(schema.to_owned()),
        SqlValue::Identifier(table.to_owned()),
    ];
    if !partitions.is_empty() {
        params.push(SqlValue::IdentifierList(partitions.to_vec()));
    }
    SqlStatement {
        sql: sql.to_owned(),
        params,
    }
}

pub fn average_analysis_duration(
    sql: &mut impl SqlPort,
    schema: &str,
    table: &str,
    partitions: &[String],
) -> RuntimeResult<i64> {
    let query = if partitions.is_empty() {
        AVG_TABLE
    } else {
        AVG_PARTITIONS
    };
    Ok(
        match sql.query_optional_f64(&statement(query, schema, table, partitions))? {
            Some(seconds) if seconds.is_finite() && seconds >= 0.0 => {
                (seconds as i64).wrapping_mul(1_000_000_000)
            }
            _ => NO_RECORD,
        },
    )
}

pub fn last_failed_analysis_duration(
    sql: &mut impl SqlPort,
    schema: &str,
    table: &str,
    partitions: &[String],
) -> RuntimeResult<i64> {
    let query = if partitions.is_empty() {
        FAILED_TABLE
    } else {
        FAILED_PARTITIONS
    };
    Ok(
        match sql.query_optional_i64(&statement(query, schema, table, partitions))? {
            None => NO_RECORD,
            Some(0) => JUST_FAILED,
            Some(value) if value < 0 => DEFAULT_FAILED_ANALYSIS_WAIT_NANOS,
            Some(seconds) => seconds.wrapping_mul(1_000_000_000),
        },
    )
}
