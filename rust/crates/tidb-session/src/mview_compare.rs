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

//! Go master `CompareMaterializedViewExec` (`pkg/executor/mview_compare.go`):
//! COMPARE MATERIALIZED VIEW recomputes the view's query at its last
//! successful refresh read TSO, reads the view itself at the AS OF
//! snapshot, full-outer-joins the two on the view's group key, and reports
//! the rows that differ -- as a one-row summary, or written to an OUTPUT
//! table.

use std::collections::HashMap;

use tidb_ast::CompareMaterializedViewStmt;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::DriverError;
use tidb_executor::sessionctx::StmtOutput;
use tidb_model::{ColumnInfo, TableInfo};
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use crate::mview_refresh::{annotate, init_mview_session_vars, plain, read_refresh_info_snapshot, set_session_vars, sys_sql};
use crate::Session;

const FP: &str = "github.com/pingcap/tidb/pkg/executor/";
const DIFFER_TYPE_BASE_NAME: &str = "_Differ_type_";
const DIFFER_TYPE: &str = "mview_differ";
const VACUUM_TYPE: &str = "mview_vacuum";
const EXCESSIVE_TYPE: &str = "mview_excessive";
const OUTPUT_WRITER_BATCH_SIZE: usize = 1024;
const OUTPUT_WRITER_BATCH_BYTES_CAP: u64 = 16 * 1024 * 1024;

/// Go master `formatCompareMaterializedViewSummary`.
fn format_compare_materialized_view_summary(
    last_success_read_tso: u64,
    diff_rows: i64,
    zone: &tidb_datatype::SessionTimeZone,
) -> String {
    use chrono::TimeZone as _;
    // Go `oracle.GetTimeFromTS(ts).In(loc).Format(types.TimeFSPFormat + " -07:00")`.
    let physical_ms = i64::try_from(last_success_read_tso >> 18).unwrap_or(i64::MAX);
    let result_time = chrono::Utc
        .timestamp_millis_opt(physical_ms)
        .single()
        .map(|at| at.with_timezone(zone).format("%Y-%m-%d %H:%M:%S%.6f %:z").to_string())
        .unwrap_or_default();
    let verb = if diff_rows == 0 { "no differences" } else { "differences" };
    format!(
        "There are {verb} result in {diff_rows} rows compared to source base tables at timestamp '{result_time}' (TSO: {last_success_read_tso})"
    )
}

/// Go master `classifyCompareMaterializedViewRowDiff`.
fn classify_compare_materialized_view_row_diff(
    current_missing: bool,
    recomputed_missing: bool,
    payload_changed: bool,
) -> Option<&'static str> {
    if recomputed_missing {
        Some(EXCESSIVE_TYPE)
    } else if current_missing {
        Some(VACUUM_TYPE)
    } else if payload_changed {
        Some(DIFFER_TYPE)
    } else {
        None
    }
}

/// Go master `chooseCompareMaterializedViewDifferTypeColumnName`.
fn choose_compare_materialized_view_differ_type_column_name(visible: &[ColumnInfo]) -> String {
    let used: Vec<String> = visible.iter().map(|column| column.name.lowercase().to_owned()).collect();
    let mut name = DIFFER_TYPE_BASE_NAME.to_owned();
    let mut suffix = 1;
    while used.contains(&tidb_hack::go_to_lower(&name)) {
        name = format!("{DIFFER_TYPE_BASE_NAME}{suffix}");
        suffix += 1;
    }
    name
}

/// Go master `buildCompareMaterializedViewSelectSQL`.
fn build_compare_materialized_view_select_sql(schema: &str, table: &str, visible: &[ColumnInfo]) -> Result<String, DriverError> {
    let escape = |sql: &str, args: &[SqlArg<'_>]| {
        tidb_sqlescape::escape_sql_text(sql, args).map_err(|error| plain(error.to_string()))
    };
    let mut columns = Vec::with_capacity(visible.len());
    for column in visible {
        columns.push(escape("%n", &[SqlArg::String(column.name.original().as_bytes())])?);
    }
    Ok(format!(
        "SELECT {} FROM {}",
        columns.join(", "),
        escape("%n.%n", &[SqlArg::String(schema.as_bytes()), SqlArg::String(table.as_bytes())])?
    ))
}

/// Go master `resolveCompareMaterializedViewOutputWriterBatchBytes`.
fn resolve_compare_materialized_view_output_writer_batch_bytes() -> usize {
    let limit = (tidb_txnkv::txn_total_size_limit() / 4).min(OUTPUT_WRITER_BATCH_BYTES_CAP);
    usize::try_from(limit).unwrap_or(usize::MAX).max(1)
}

/// One value as the SQL literal the output writer's INSERT carries: Go's
/// writer adds the datum itself (`AddRecord`); the literal reads back as
/// the same value of the output column's type.
fn datum_sql_literal(datum: &Datum) -> Result<String, DriverError> {
    let quoted = |bytes: &[u8]| {
        tidb_sqlescape::escape_sql_text("%?", &[SqlArg::String(bytes)]).map_err(|error| plain(error.to_string()))
    };
    Ok(match datum {
        Datum::Null => "NULL".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) | Datum::Float32(value) => format!("{value:?}"),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(value) => quoted(value.bytes())?,
        Datum::Bytes(value) => tidb_sqlescape::escape_sql_text("%?", &[SqlArg::Bytes(Some(value))])
            .map_err(|error| plain(error.to_string()))?,
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            format!("x'{}'", value.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>())
        }
        Datum::Duration(value) => quoted(value.to_string().as_bytes())?,
        Datum::Enum(value, _) => quoted(value.name_bytes())?,
        Datum::Set(value, _) => quoted(value.name_bytes())?,
        Datum::Time(value) => quoted(value.to_string().as_bytes())?,
        Datum::Json(value) => quoted(value.to_string().as_bytes())?,
        Datum::VectorFloat32(value) => quoted(value.to_string().as_bytes())?,
        Datum::Raw(_) | Datum::MinNotNull | Datum::MaxValue => {
            return Err(plain("compare materialized view: cannot write a raw or sentinel datum"))
        }
    })
}

/// Go master `compareMaterializedViewOutputWriter`: the OUTPUT table and the
/// session its rows are written on, in batches that each commit.
struct CompareOutputWriter {
    schema: String,
    table: String,
    session: Session,
    pending: Vec<String>,
    pending_bytes: usize,
    batch_bytes: usize,
}

impl CompareOutputWriter {
    /// Go `flushBatch`: the pending rows commit, then the injected flush
    /// error.
    fn flush_batch(&mut self) -> Result<(), DriverError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let prefix = tidb_sqlescape::escape_sql_text(
            "INSERT INTO %n.%n VALUES ",
            &[SqlArg::String(self.schema.as_bytes()), SqlArg::String(self.table.as_bytes())],
        )
        .map_err(|error| plain(error.to_string()))?;
        let rows = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        sys_sql(&mut self.session, &format!("{prefix}{}", rows.join(", ")), &[])?;
        if let Some(value) = tidb_failpoint::eval(&format!("{FP}mockCompareMaterializedViewOutputFlushError")) {
            return Err(plain(
                value.as_string().map_or("mock compare materialized view output flush error", |message| message).to_owned(),
            ));
        }
        Ok(())
    }

    /// Go `writeRow`: the row's view columns -- the recomputed image for a
    /// vacuumed row, the current one otherwise -- and its diff type.
    fn write_row(&mut self, row: &[Datum], diff_type: &str) -> Result<(), DriverError> {
        let mut literals = Vec::with_capacity(row.len() + 1);
        for datum in row {
            literals.push(datum_sql_literal(datum)?);
        }
        literals.push(datum_sql_literal(&Datum::String(tidb_datatype::StringDatum::new(diff_type, tidb_charset::Collation::Utf8Mb4Bin)))?);
        let tuple = format!("({})", literals.join(", "));
        self.pending_bytes += tuple.len();
        self.pending.push(tuple);
        if self.pending.len() >= OUTPUT_WRITER_BATCH_SIZE || self.pending_bytes >= self.batch_bytes {
            return self.flush_batch();
        }
        Ok(())
    }
}

/// The joined pair of one group key: the current view row and the
/// recomputed one, either missing.
struct ComparedRow<'a> {
    current: Option<&'a [Datum]>,
    recomputed: Option<&'a [Datum]>,
}

impl Session {
    /// Go master `CompareMaterializedViewExec.Next`.
    pub(crate) fn compare_materialized_view(&mut self, s: &CompareMaterializedViewStmt) -> Result<StmtOutput, DriverError> {
        let compare_snapshot_ts = self.calculate_as_of_ts_expr(&s.as_of)?;
        self.precheck_compare_materialized_view_current_state(s)?;
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot schema from"))?;
        let snapshot = crate::domain::cluster_session_node::get_snapshot_info_schema(&domain, compare_snapshot_ts)?;
        let (schema, info) = self.resolve_mview_statement_target(&snapshot, &s.view_name, "compare materialized view")?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&snapshot, &mview)?;
        let output = self.check_compare_materialized_view_output_table_not_exists(s)?;
        let last_success_read_tso = self.read_compare_materialized_view_last_success_read_tso(info.id, compare_snapshot_ts)?;

        // Go `mvmerge.BuildCompleteDiffSource` for the layout.
        let config = self.vars.build_parser_config();
        let charset = self.vars.get_system("character_set_connection").unwrap_or_else(|_| "utf8mb4".to_owned());
        let collation = self.vars.get_system("collation_connection").unwrap_or_else(|_| "utf8mb4_bin".to_owned());
        let parse = |sql: &str| {
            tidb_parser::parse_with_config_and_connection(sql, config, &charset, &collation)
                .map_err(|error| error.compatibility_message(sql))
        };
        let diff = tidb_planner::mview::build_complete_diff_source(&parse, &snapshot, &info).map_err(plain)?;
        let visible: Vec<ColumnInfo> = info
            .columns
            .iter_deref()
            .map(|column| column.read().clone())
            .filter(|column| !column.hidden && column.state == tidb_model::SchemaState::PUBLIC)
            .collect();
        // Go `buildCompareMaterializedViewDiffLayout`.
        if visible.len() != diff.mv_column_count {
            return Err(plain("compare materialized view: hidden MV columns are unsupported"));
        }

        let mut writer = match &output {
            Some((output_schema, output_table)) => {
                Some(self.open_compare_materialized_view_output_writer(output_schema, output_table, &info, &mview, &visible)?)
            }
            None => None,
        };
        let compared = self.compare_materialized_view_rows(
            &schema,
            &info,
            &mview,
            &visible,
            &diff.group_key_mv_offsets,
            diff.marker_mv_offset,
            compare_snapshot_ts,
            last_success_read_tso,
            writer.as_mut(),
        );
        let diff_rows = match compared {
            Ok(diff_rows) => diff_rows,
            Err(error) => {
                if let Some(writer) = writer {
                    self.rollback_compare_materialized_view_output_writer(writer);
                }
                return Err(error);
            }
        };
        if let Some(mut writer) = writer {
            if let Err(error) = writer.flush_batch() {
                self.rollback_compare_materialized_view_output_writer(writer);
                return Err(error);
            }
            return Ok(StmtOutput::Affected(0));
        }
        let (schema_columns, names) = tidb_planner::show_schema::build_compare_materialized_view_schema();
        let columns = schema_columns
            .columns
            .iter()
            .zip(names)
            .map(|(column, name)| (name.names.column.original.clone(), column.ret_type.clone().unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::String))))
            .collect();
        let summary = format_compare_materialized_view_summary(last_success_read_tso, diff_rows, &self.vars.session_time_zone());
        Ok(StmtOutput::Rows { columns, rows: vec![vec![Datum::String(tidb_datatype::StringDatum::new(summary, tidb_charset::Collation::Utf8Mb4Bin))]] })
    }

    /// Go master `precheckCompareMaterializedViewCurrentState`.
    fn precheck_compare_materialized_view_current_state(&self, s: &CompareMaterializedViewStmt) -> Result<(), DriverError> {
        let current = self.refresh_latest_info_schema();
        let (_, info) = self.resolve_mview_statement_target(&current, &s.view_name, "compare materialized view")?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&current, &mview)?;
        self.check_compare_materialized_view_output_table_not_exists(s).map(|_| ())
    }

    /// Go master `checkCompareMaterializedViewOutputTableNotExists`: the
    /// OUTPUT table's schema and name, when there is one.
    fn check_compare_materialized_view_output_table_not_exists(
        &self,
        s: &CompareMaterializedViewStmt,
    ) -> Result<Option<(String, String)>, DriverError> {
        let Some(output) = &s.output_table else {
            return Ok(None);
        };
        let (schema, name) = match output.as_slice() {
            [schema, name] => (schema.clone(), name.clone()),
            [name] if self.current_database().is_empty() => {
                let _ = name;
                return Err(DriverError::Mysql(MysqlError::from_terror(
                    &tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                )));
            }
            [name] => (self.current_database().to_owned(), name.clone()),
            _ => return Err(plain("compare materialized view: missing output table name")),
        };
        let catalog = self.refresh_latest_info_schema();
        if catalog.schema_by_name(&schema).is_none() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
            )));
        }
        if catalog.table_info_by_name(&schema, &name).is_some() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                    .gen_with_stack_by_args(&[format!("{schema}.{name}").into()]),
            )));
        }
        Ok(Some((schema, name)))
    }

    /// Go master `readCompareMaterializedViewLastSuccessReadTSO`: the view's
    /// refresh-info row as of the compare snapshot.
    fn read_compare_materialized_view_last_success_read_tso(
        &self,
        mview_id: i64,
        compare_snapshot_ts: u64,
    ) -> Result<u64, DriverError> {
        let mut sys = self.open_compare_materialized_view_snapshot_session(compare_snapshot_ts)?;
        let refresh_info = read_refresh_info_snapshot(&mut sys, mview_id)?;
        if refresh_info.last_success_read_tso_null {
            return Err(plain("compare materialized view: LAST_SUCCESS_READ_TSO is NULL"));
        }
        if refresh_info.last_success_read_tso > compare_snapshot_ts {
            return Err(plain(format!(
                "compare materialized view: LAST_SUCCESS_READ_TSO {} is newer than compare snapshot {compare_snapshot_ts}",
                refresh_info.last_success_read_tso
            )));
        }
        Ok(refresh_info.last_success_read_tso)
    }

    /// Go master `openCompareMaterializedViewSnapshotSession`: a system
    /// session reading at `snapshot_ts` (`SET @@tidb_snapshot`).
    fn open_compare_materialized_view_snapshot_session(&self, snapshot_ts: u64) -> Result<Session, DriverError> {
        let mut sys = self.open_refresh_system_session()?;
        let snapshot = if snapshot_ts == 0 { String::new() } else { snapshot_ts.to_string() };
        sys_sql(&mut sys, "SET @@tidb_snapshot = %?", &[SqlArg::String(snapshot.as_bytes())])?;
        Ok(sys)
    }

    /// Go master `openCompareMaterializedViewQueryExec`: `sql`'s rows and
    /// their types, read at `snapshot_ts` under the view's definition
    /// session (`ddl.InitMViewSession`).
    fn read_compare_materialized_view_query(
        &self,
        snapshot_ts: u64,
        mview: &tidb_model::table::MaterializedViewInfo,
        sql: &str,
    ) -> Result<(Vec<Vec<Datum>>, Vec<FieldType>), DriverError> {
        let mut sys = self.open_compare_materialized_view_snapshot_session(snapshot_ts)?;
        set_session_vars(&mut sys, &init_mview_session_vars(mview))?;
        sys.vars.in_mview_maintenance = true;
        let context = tidb_sqlexec::BackgroundContext;
        let (rows, fields) = crate::domain::cluster_session_node::execute_internal_under(&mut sys, &context, sql)
            .map_err(|error| match error.downcast::<tidb_store_driver_error::sql_query_error::SqlQueryError>() {
                Ok(error) => DriverError::Mysql((*error).into()),
                Err(error) => plain(error.to_string()),
            })?;
        if fields.is_empty() {
            return Err(plain("compare materialized view: query returned empty schema"));
        }
        let mut types = Vec::with_capacity(fields.len());
        for (offset, field) in fields.iter().enumerate() {
            let Some(column) = field.read().column.clone() else {
                return Err(plain(format!("compare materialized view: query returned nil column at offset {offset}")));
            };
            types.push(column.read().field_type.clone());
        }
        Ok((rows, types))
    }

    /// The rest of Go master `CompareMaterializedViewExec.Next`: the
    /// recomputed and current rows, their full outer hash join on the group
    /// key (current rows probe, recomputed rows build, `IsNullEQ` on a
    /// nullable key), and each differing row counted and written.
    #[allow(clippy::too_many_arguments)]
    fn compare_materialized_view_rows(
        &self,
        schema: &str,
        info: &TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        visible: &[ColumnInfo],
        group_keys: &[usize],
        marker: usize,
        compare_snapshot_ts: u64,
        last_success_read_tso: u64,
        mut writer: Option<&mut CompareOutputWriter>,
    ) -> Result<i64, DriverError> {
        let (recomputed, _) = self.read_compare_materialized_view_query(last_success_read_tso, mview, &mview.sql_content)?;
        let current_sql = build_compare_materialized_view_select_sql(schema, info.name.original(), visible)?;
        let (current, current_types) = self.read_compare_materialized_view_query(compare_snapshot_ts, mview, &current_sql)?;
        // Go `buildCompareMaterializedViewNullEQFlags`.
        let null_eq: Vec<bool> = group_keys
            .iter()
            .map(|&offset| !tidb_mysql::types::has_not_null_flag(visible[offset].field_type.flags() as usize))
            .collect();
        // Go `HashJoinV1`'s key: each key column's hash-key encoding; a NULL
        // key matches only where the key is null-safe.
        let join_key = |row: &[Datum]| -> Result<Option<Vec<Vec<u8>>>, DriverError> {
            let mut key = Vec::with_capacity(group_keys.len());
            for (index, &offset) in group_keys.iter().enumerate() {
                let value = row.get(offset).cloned().unwrap_or(Datum::Null);
                if matches!(value, Datum::Null) && !null_eq[index] {
                    return Ok(None);
                }
                let mut encoded = tidb_codec::hash_group_key(&[value], &current_types[offset])
                    .map_err(|error| plain(error.to_string()))?;
                key.push(encoded.pop().unwrap_or_default());
            }
            Ok(Some(key))
        };
        let mut build: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::new();
        for (index, row) in recomputed.iter().enumerate() {
            if let Some(key) = join_key(row)? {
                build.entry(key).or_default().push(index);
            }
        }
        let mut build_matched = vec![false; recomputed.len()];
        let mut joined = Vec::with_capacity(current.len());
        for row in &current {
            let matches = match join_key(row)? {
                Some(key) => build.get(&key).cloned().unwrap_or_default(),
                None => Vec::new(),
            };
            if matches.is_empty() {
                joined.push(ComparedRow { current: Some(row), recomputed: None });
            }
            for index in matches {
                build_matched[index] = true;
                joined.push(ComparedRow { current: Some(row), recomputed: Some(&recomputed[index]) });
            }
        }
        for (index, row) in recomputed.iter().enumerate() {
            if !build_matched[index] {
                joined.push(ComparedRow { current: None, recomputed: Some(row) });
            }
        }

        // Go `MarkTouchedRowsByColumn` over every non-group-key column.
        let payload: Vec<usize> = (0..visible.len()).filter(|offset| !group_keys.contains(offset)).collect();
        let null_row = vec![Datum::Null; visible.len()];
        let mut diff_rows = 0i64;
        for pair in joined {
            let current = pair.current.unwrap_or(&null_row);
            let recomputed = pair.recomputed.unwrap_or(&null_row);
            let payload_changed = payload.iter().any(|&offset| {
                tidb_executor::driver::mview_delta_apply::values_differ(&current[offset], &recomputed[offset])
            });
            let Some(diff_type) = classify_compare_materialized_view_row_diff(
                matches!(current.get(marker), None | Some(Datum::Null)),
                matches!(recomputed.get(marker), None | Some(Datum::Null)),
                payload_changed,
            ) else {
                continue;
            };
            diff_rows += 1;
            if let Some(writer) = writer.as_deref_mut() {
                // Go: the vacuumed row's image is the recomputed one.
                let image = if diff_type == VACUUM_TYPE { recomputed } else { current };
                writer.write_row(&image[..visible.len()], diff_type)?;
            }
        }
        Ok(diff_rows)
    }

    /// Go master `openCompareMaterializedViewOutputWriter`: the OUTPUT table
    /// created from the view's visible columns and the diff-type column,
    /// and the session writing it in the view's definition time zone.
    fn open_compare_materialized_view_output_writer(
        &self,
        output_schema: &str,
        output_table: &str,
        info: &TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        visible: &[ColumnInfo],
    ) -> Result<CompareOutputWriter, DriverError> {
        let diff_type_column = choose_compare_materialized_view_differ_type_column_name(visible);
        let mut ddl = self.open_refresh_system_session()?;
        let create = build_compare_materialized_view_output_create_table_sql(&ddl, output_schema, output_table, info, visible, &diff_type_column)?;
        sys_sql(&mut ddl, &create, &[])?;
        let mut session = match self.open_refresh_system_session() {
            Ok(session) => session,
            Err(error) => {
                drop_compare_materialized_view_output_table(&mut ddl, output_schema, output_table);
                return Err(error);
            }
        };
        // The query rows use the view's definition time zone, TIMESTAMP
        // values included.
        let zone = init_mview_session_vars(mview).into_iter().filter(|(name, _)| name == "time_zone").collect::<Vec<_>>();
        if let Err(error) = set_session_vars(&mut session, &zone) {
            drop_compare_materialized_view_output_table(&mut ddl, output_schema, output_table);
            return Err(annotate(&error, "compare materialized view: invalid definition timezone"));
        }
        Ok(CompareOutputWriter {
            schema: output_schema.to_owned(),
            table: output_table.to_owned(),
            session,
            pending: Vec::new(),
            pending_bytes: 0,
            batch_bytes: resolve_compare_materialized_view_output_writer_batch_bytes(),
        })
    }

    /// Go master `compareMaterializedViewOutputWriter.rollback`: the
    /// unwritten rows are dropped and so is the OUTPUT table
    /// (`cleanupCompareMaterializedViewOutputTable`).
    fn rollback_compare_materialized_view_output_writer(&self, writer: CompareOutputWriter) {
        drop(writer.session);
        match self.open_refresh_system_session() {
            Ok(mut ddl) => drop_compare_materialized_view_output_table(&mut ddl, &writer.schema, &writer.table),
            Err(error) => warn_output_cleanup(&writer.schema, &writer.table, &error),
        }
    }
}

fn warn_output_cleanup(schema: &str, table: &str, error: &DriverError) {
    tidb_logutil::bg_logger().warn(
        "failed to cleanup compare materialized view output table",
        &[
            tidb_log::Field::new("schema", tidb_log::Value::Str(schema.to_owned())),
            tidb_log::Field::new("table", tidb_log::Value::Str(table.to_owned())),
            tidb_logutil::zap_error(crate::mview_refresh::driver_message(error)),
        ],
    );
}

/// Go master `cleanupCompareMaterializedViewOutputTable`.
fn drop_compare_materialized_view_output_table(ddl: &mut Session, schema: &str, table: &str) {
    if let Err(error) =
        sys_sql(ddl, "DROP TABLE IF EXISTS %n.%n", &[SqlArg::String(schema.as_bytes()), SqlArg::String(table.as_bytes())])
    {
        warn_output_cleanup(schema, table, &error);
    }
}

/// Go master `buildCompareMaterializedViewOutputCreateTableSQL`: the view's
/// visible columns without key, auto-increment, no-default or on-update
/// flags, then the diff-type column, as `SHOW CREATE TABLE` prints them,
/// qualified by the OUTPUT table's schema.
fn build_compare_materialized_view_output_create_table_sql(
    ddl: &Session,
    output_schema: &str,
    output_table: &str,
    info: &TableInfo,
    visible: &[ColumnInfo],
    diff_type_column: &str,
) -> Result<String, DriverError> {
    use tidb_datatype::FieldTypeFlags as Flags;
    use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
    const EXCLUDED: u32 = Flags::PRI_KEY
        | Flags::UNIQUE_KEY
        | Flags::MULTIPLE_KEY
        | Flags::AUTO_INCREMENT
        | Flags::NO_DEFAULT_VALUE
        | Flags::ON_UPDATE_NOW;
    let mut columns = Vec::with_capacity(visible.len() + 1);
    for (offset, column) in visible.iter().enumerate() {
        let mut column = column.clone();
        column.offset = i64::try_from(offset).unwrap_or(i64::MAX);
        column.state = tidb_model::SchemaState::PUBLIC;
        let flags = column.field_type.flags() & !EXCLUDED;
        column.field_type.set_flag(flags);
        columns.push(column);
    }
    let charset = if info.charset.is_empty() { tidb_mysql::charset::DefaultCharset.to_owned() } else { info.charset.clone() };
    let collate = if info.collate.is_empty() {
        tidb_charset::charset::get_default_collation(&charset).map_err(|error| plain(error.to_string()))?
    } else {
        info.collate.clone()
    };
    let mut diff_type = FieldType::new(tidb_datatype::FieldTypeCode::Varchar)
        .with_charset_name(&charset)
        .with_collation_name(&collate);
    diff_type.set_flen(32);
    diff_type.set_flag(Flags::NOT_NULL);
    columns.push(ColumnInfo {
        id: i64::try_from(columns.len() + 1).unwrap_or(i64::MAX),
        name: tidb_ast::CiString::new(diff_type_column),
        offset: i64::try_from(columns.len()).unwrap_or(i64::MAX),
        field_type: diff_type,
        state: tidb_model::SchemaState::PUBLIC,
        ..ColumnInfo::default()
    });
    let output = TableInfo {
        name: tidb_ast::CiString::new(output_table),
        charset: info.charset.clone(),
        collate: info.collate.clone(),
        state: tidb_model::SchemaState::PUBLIC,
        columns: tidb_model::GoSharedPointerSlice::from_handles(columns.into_iter().map(|column| Some(tidb_model::GoShared::new(column))).collect()),
        ..TableInfo::default()
    };
    let common = tidb_table::table_from_meta::table_from_meta(&output, &tidb_table::table_from_meta::AutoIdSource::Unavailable)
        .map_err(plain)?;
    let ctx = ddl.statement_context(false);
    let table_def =
        tidb_executor::show::create::construct_result_of_show_create_table(output_schema, output_table, &common, &ctx, false)?;
    let ansi_quotes = ctx.sql_mode().ansi_quotes;
    let escape = |name: &str| tidb_executor::show::create::escape_name(name, ansi_quotes);
    let prefix = format!("CREATE TABLE {}", escape(output_table));
    let Some(rest) = table_def.strip_prefix(&prefix) else {
        return Err(plain(format!("compare materialized view: invalid output table DDL {table_def:?}")));
    };
    Ok(format!("CREATE TABLE {}.{}{rest}", escape(output_schema), escape(output_table)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_text_as_go() {
        // 2026-01-01 00:00:00 UTC, logical 7.
        let tso = (1_767_225_600_000u64 << 18) + 7;
        let zone = tidb_datatype::SessionTimeZone::Fixed { name: "+08:00".to_owned(), offset_secs: 8 * 3600 };
        assert_eq!(
            format_compare_materialized_view_summary(tso, 3, &zone),
            format!("There are differences result in 3 rows compared to source base tables at timestamp '2026-01-01 08:00:00.000000 +08:00' (TSO: {tso})")
        );
        assert!(format_compare_materialized_view_summary(tso, 0, &zone).starts_with("There are no differences result in 0 rows"));
    }

    #[test]
    fn differ_type_column_avoids_view_columns_as_go() {
        let column = |name: &str| ColumnInfo { name: tidb_ast::CiString::new(name), ..ColumnInfo::default() };
        assert_eq!(choose_compare_materialized_view_differ_type_column_name(&[column("a")]), "_Differ_type_");
        assert_eq!(
            choose_compare_materialized_view_differ_type_column_name(&[column("_differ_TYPE_"), column("_Differ_type_1")]),
            "_Differ_type_2"
        );
    }

    #[test]
    fn row_diff_classification_as_go() {
        assert_eq!(classify_compare_materialized_view_row_diff(true, true, true), Some(EXCESSIVE_TYPE));
        assert_eq!(classify_compare_materialized_view_row_diff(false, true, false), Some(EXCESSIVE_TYPE));
        assert_eq!(classify_compare_materialized_view_row_diff(true, false, true), Some(VACUUM_TYPE));
        assert_eq!(classify_compare_materialized_view_row_diff(false, false, true), Some(DIFFER_TYPE));
        assert_eq!(classify_compare_materialized_view_row_diff(false, false, false), None);
    }
}
