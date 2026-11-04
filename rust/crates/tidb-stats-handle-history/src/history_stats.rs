// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `history_stats.go`: `RecordHistoricalStatsMeta` and
//! `RecordHistoricalStatsToStorage` as Go's statements on a statistics
//! session, plus the row planners the DDL statistics subscriber still uses.


use tidb_datatype::Datum;
use tidb_stats_util::JsonTable;

use tidb_syssession::stats_table_rows::*;

use tidb_log::{Field, Value};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_storage::stats_read_writer::StatsSessionPool;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext, FLAG_WRAP_TXN};

/// Pinned Go `history.RecordHistoricalStatsToStorage`'s preparation before its
/// per-block `INSERT` statements.
pub fn historical_stats_data_blocks(
    json: &JsonTable,
) -> Result<(u64, Vec<Vec<u8>>), StatsWriteError> {
    const MAX_COLUMN_SIZE: usize = 5 << 20;
    let version = match json.partitions.as_ref() {
        Some(partitions) if !partitions.is_empty() => partitions
            .values()
            .map(|partition| {
                partition
                    .as_ref()
                    .expect("nil partition in historical statistics JSON")
                    .version
            })
            .max()
            .unwrap_or(0),
        _ => json.version,
    };
    let blocks = tidb_stats_handle_storage::json::json_table_to_blocks(json, MAX_COLUMN_SIZE)
        .map_err(|error| StatsWriteError::HistoricalData(error.to_string()))?;
    Ok((version, blocks))
}

/// Go `history.RecordHistoricalStatsMeta`: the table's `stats_meta` counts at
/// `version`, locked, copied into `mysql.stats_meta_history`.
pub fn record_historical_stats_meta<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    version: u64,
    source: &str,
    table_id: i64,
) -> Result<(), SqlExecError> {
    if table_id == 0 || version == 0 {
        return Err(history_error(format!(
            "tableID {table_id}, version {version} are invalid"
        )));
    }
    let (rows, _) = exec_rows(
        sctx,
        "SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = %? and version = %? FOR UPDATE",
        &[SqlArg::Signed(table_id), SqlArg::Unsigned(version)],
    )?;
    let Some(row) = rows.first() else {
        return Err(history_error("no historical meta stats can be recorded".to_owned()));
    };
    let (modify_count, count) = (datum_i64(row.first()), datum_i64(row.get(1)));
    // NOW(6) matches create_time's datetime(6) precision.
    exec_rows(
        sctx,
        "REPLACE INTO mysql.stats_meta_history(table_id, modify_count, count, version, source, create_time) VALUES (%?, %?, %?, %?, %?, NOW(6))",
        &[
            SqlArg::Signed(table_id),
            SqlArg::Signed(modify_count),
            SqlArg::Signed(count),
            SqlArg::Unsigned(version),
            SqlArg::from(source),
        ],
    )?;
    Ok(())
}

/// Go `statsHistoryImpl.RecordHistoricalStatsMeta`'s per-table records over
/// already-targeted tables: each its own transaction on the statistics
/// pool, a failure logged.
pub fn record_historical_stats_meta_on(
    pool: &dyn StatsSessionPool,
    version: u64,
    source: &str,
    table_ids: &[i64],
) {
    if version == 0 {
        return;
    }
    for &table_id in table_ids {
        let result = pool.call_with_sctx(
            &mut |sctx| record_historical_stats_meta(sctx, version, source, table_id),
            &[FLAG_WRAP_TXN],
        );
        if let Err(error) = result {
            tidb_stats_handle_logutil::stats_logger().warn(
                "record historical stats meta failed",
                &[
                    Field::new("version", Value::U64(version)),
                    Field::new("source", Value::Str(source.to_owned())),
                    Field::new("tableID", Value::I64(table_id)),
                    tidb_logutil::zap_error(error.message),
                ],
            );
        }
    }
}

/// Go `history.RecordHistoricalStatsToStorage`: the dump's gzipped blocks
/// into `mysql.stats_history`; returns the dump's version.
pub fn record_historical_stats_to_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
    js: &JsonTable,
) -> Result<u64, SqlExecError> {
    let (version, blocks) =
        historical_stats_data_blocks(js).map_err(|error| Box::new(error) as SqlExecError)?;
    let ts = go_now_text();
    for (seq_no, block) in blocks.iter().enumerate() {
        exec_rows(
            sctx,
            "INSERT INTO mysql.stats_history(table_id, stats_data, seq_no, version, create_time) VALUES (%?, %?, %?, %?, %?)ON DUPLICATE KEY UPDATE stats_data=%?, create_time=%?",
            &[
                SqlArg::Signed(physical_id),
                SqlArg::Bytes(Some(block)),
                SqlArg::Signed(seq_no as i64),
                SqlArg::Unsigned(version),
                SqlArg::from(ts.as_str()),
                SqlArg::Bytes(Some(block)),
                SqlArg::from(ts.as_str()),
            ],
        )?;
    }
    Ok(version)
}

/// Go `time.Now().Format("2006-01-02 15:04:05.999999")`: local time, the
/// fraction without trailing zeros.
fn go_now_text() -> String {
    let now = chrono::Local::now();
    let mut text = now.format("%Y-%m-%d %H:%M:%S").to_string();
    let micros = now.timestamp_subsec_micros();
    if micros != 0 {
        let fraction = format!("{micros:06}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    text
}

fn history_error(message: String) -> SqlExecError {
    Box::new(std::io::Error::other(message))
}

/// Go `row.GetInt64(i)`.
fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}
