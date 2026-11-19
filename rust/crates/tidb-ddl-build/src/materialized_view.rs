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

//! Go master `pkg/ddl/materialized_view.go`'s table builders: the
//! materialized view log table (`BuildMaterializedViewLogTableInfo`) and the
//! schedule and alert clauses it stores.

use tidb_ast::{
    ColumnDef, ColumnType, CreateMaterializedViewLogStmt, CreateTableStmt, CreateTableTemporary, Expr,
    MLogAccumulationAlertClause, MLogPurgeClause,
};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_model::table::{
    materialized_view_log_table_name, MaterializedViewLogInfo, MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME,
    MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME,
};
use tidb_model::{ColumnInfo, GoShared, TableInfo};
use tidb_terror::MysqlError;

use crate::table_info_build::{build_table_info_with_field_types, DdlAdmissionError, Refusal};

/// Go `blobMaxLength`.
const BLOB_MAX_LENGTH: i64 = 65535;

fn unsupported(message: String) -> MysqlError {
    MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(message))
}

/// Go `FieldTypeForMaterializedViewLogColumn`: the base column's type
/// without its key, auto-increment and on-update flags, a maximal BLOB
/// length left unspecified.
#[must_use]
pub fn field_type_for_materialized_view_log_column(base: &ColumnInfo) -> FieldType {
    let mut field_type = base.field_type.clone();
    field_type.del_flag(
        FieldTypeFlags::PRI_KEY
            | FieldTypeFlags::UNIQUE_KEY
            | FieldTypeFlags::MULTIPLE_KEY
            | FieldTypeFlags::AUTO_INCREMENT
            | FieldTypeFlags::ON_UPDATE_NOW,
    );
    // Go `normalizeMaterializedViewLogBlobFlen`.
    if field_type.code() == FieldTypeCode::Blob && field_type.flen() == BLOB_MAX_LENGTH {
        field_type.set_flen(tidb_datatype::UNSPECIFIED_LENGTH);
    }
    field_type
}

/// A column definition whose type Go's caller builds already
/// (`ast.ColumnDef{Name, Tp: &ft}`): it declares no options, and its declared
/// type is never read.
#[must_use]
pub fn built_type_column_def(name: &str) -> ColumnDef {
    ColumnDef {
        qualifier: Vec::new(),
        name: name.to_owned(),
        ty: ColumnType {
            name: String::new(),
            args: Vec::new(),
            unsigned: false,
            zerofill: false,
            binary: false,
            charset: None,
        },
        options: Vec::new(),
    }
}

/// Go `refreshMLogAddedColumnFromBase`'s type: the base column's type
/// without its key, auto-increment and on-update flags.
#[must_use]
pub fn mlog_added_column_field_type(base: &ColumnInfo) -> FieldType {
    let mut field_type = base.field_type.clone();
    field_type.del_flag(
        FieldTypeFlags::PRI_KEY
            | FieldTypeFlags::UNIQUE_KEY
            | FieldTypeFlags::MULTIPLE_KEY
            | FieldTypeFlags::AUTO_INCREMENT
            | FieldTypeFlags::ON_UPDATE_NOW,
    );
    field_type
}

/// Go `setMLogAddedColumnOriginDefault`: a NOT NULL log column added to a
/// log with rows reads a single space (a non-binary string) or its type's
/// zero value in the rows before it.
///
/// # Errors
///
/// The origin default cannot be built.
pub fn set_mlog_added_column_origin_default(col: &mut ColumnInfo) -> Result<(), MysqlError> {
    if !col.field_type.has_flag(FieldTypeFlags::NOT_NULL) {
        return Ok(());
    }
    if use_single_space_default_for_mlog_string_column(col) {
        return col
            .set_origin_default_value(tidb_model::ColumnDefaultValue::str(" "))
            .map_err(|error| MysqlError::from_terror(&error));
    }
    if !col.get_origin_default_value().is_nil() {
        return Ok(());
    }
    crate::table_info_build::stamp_origin_default_without_session(col).map_err(|error| error.to_mysql_error())
}

/// Go `useSingleSpaceDefaultForMLogStringColumn`.
fn use_single_space_default_for_mlog_string_column(col: &ColumnInfo) -> bool {
    col.field_type.charset_name() != "binary"
        && matches!(
            col.field_type.code(),
            FieldTypeCode::String
                | FieldTypeCode::Varchar
                | FieldTypeCode::VarString
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
        )
}

/// Go `syncMLogColumnsAfterAddColumn`: a log tracks its public columns but
/// its two meta columns.
pub fn sync_mlog_columns_after_add_column(info: &mut TableInfo) {
    let Some(log) = &info.materialized_view_log else {
        return;
    };
    let tracked: Vec<_> = info
        .columns
        .iter_deref()
        .filter_map(|column| {
            let column = column.read();
            (column.state == tidb_model::SchemaState::PUBLIC && !is_mlog_meta_column_name(column.name.lowercase()))
                .then(|| column.name.clone())
        })
        .collect();
    log.write().columns = tracked.into();
}

/// Go `isMLogMetaColumnName`.
fn is_mlog_meta_column_name(name: &str) -> bool {
    name == tidb_hack::go_to_lower(tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME)
        || name == tidb_hack::go_to_lower(tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME)
}

/// Go `checkMaterializedViewLogColumnSupportedForOp`.
///
/// # Errors
///
/// A JSON column, or a BLOB column of the binary charset.
pub fn check_materialized_view_log_column_supported_for_op(operation: &str, col: &ColumnInfo) -> Result<(), MysqlError> {
    let code = col.field_type.code();
    if code == FieldTypeCode::Json {
        return Err(unsupported(format!("{operation} does not support JSON column {}", col.name.original())));
    }
    if code.is_type_blob() && col.field_type.charset_name() == "binary" {
        return Err(unsupported(format!("{operation} does not support BLOB column {}", col.name.original())));
    }
    Ok(())
}

/// Go `CheckMaterializedViewLogColumnSupported`.
///
/// # Errors
///
/// See [`check_materialized_view_log_column_supported_for_op`].
pub fn check_materialized_view_log_column_supported(col: &ColumnInfo) -> Result<(), MysqlError> {
    check_materialized_view_log_column_supported_for_op("CREATE MATERIALIZED VIEW LOG", col)
}

/// Go `restoreNodeToCanonicalSQL`: `DefaultRestoreFlags |
/// RestoreStringWithoutCharset`.
#[must_use]
pub fn restore_node_to_canonical_sql(expr: &Expr) -> String {
    expr.restore_with_flags(
        tidb_parser_format::RestoreFlags::DEFAULT | tidb_parser_format::RestoreFlags::STRING_WITHOUT_CHARSET,
    )
}

/// Go `BuildAndValidateMViewScheduleExpr`: the expression's canonical SQL,
/// when its type is DATE, DATETIME or TIMESTAMP.
///
/// # Errors
///
/// The expression does not build, or its type is another one
/// (`ErrGeneralUnsupportedDDL`).
pub fn build_and_validate_mview_schedule_expr(
    ctx: &dyn tidb_expr::rewriter::ColumnResolver,
    expr: &Expr,
    clause: &str,
) -> Result<String, MysqlError> {
    let sql = restore_node_to_canonical_sql(expr);
    let built = tidb_expr::simple_expr::build_simple_expr(ctx, expr, &tidb_expr::simple_expr::BuildOptions::default())
        .map_err(|error| MysqlError::unknown(error.to_string()))?;
    let Some(field_type) = built.static_type() else {
        return Err(MysqlError::unknown(format!("failed to infer expression type for {clause}")));
    };
    let code = field_type.code();
    if !matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp) {
        return Err(unsupported(format!(
            "{clause} expression must return DATE/DATETIME/TIMESTAMP, but got {}",
            tidb_datatype::type_str(code)
        )));
    }
    Ok(sql)
}

/// Go `BuildMLogAccumulationAlertRows`: the persisted ALERT ROWS threshold,
/// `None` when the statement has none.
///
/// # Errors
///
/// A negative row count.
pub fn build_mlog_accumulation_alert_rows(alert: Option<&MLogAccumulationAlertClause>) -> Result<Option<u64>, MysqlError> {
    let Some(alert) = alert else {
        return Ok(None);
    };
    u64::try_from(alert.rows)
        .map(Some)
        .map_err(|_| MysqlError::unknown(format!("invalid ALERT ROWS value: {} (must be non-negative)", alert.rows)))
}

/// Go `buildMLogPurgeMeta`: the purge method and schedule an ALTER
/// MATERIALIZED VIEW LOG stores.
///
/// # Errors
///
/// `PURGE IMMEDIATE`, or a schedule expression that is not a time.
pub fn build_mlog_purge_meta(
    ctx: &dyn tidb_expr::rewriter::ColumnResolver,
    purge: Option<&MLogPurgeClause>,
) -> Result<(String, String, String), MysqlError> {
    let Some(purge) = purge else {
        return Ok(Default::default());
    };
    if purge.immediate {
        return Err(unsupported("PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG".to_owned()));
    }
    let start_with = match &purge.start_with {
        Some(expr) => build_and_validate_mview_schedule_expr(ctx, expr, "PURGE START WITH")?,
        None => String::new(),
    };
    let next = match &purge.next {
        Some(expr) => build_and_validate_mview_schedule_expr(ctx, expr, "PURGE NEXT")?,
        None => String::new(),
    };
    Ok(("DEFERRED".to_owned(), start_with, next))
}

/// Go master `buildMViewRefreshMeta`: the refresh method and the canonical
/// START WITH / NEXT expressions (FAST with no schedule when unwritten).
///
/// # Errors
///
/// A schedule expression that is not a time.
pub fn build_mview_refresh_meta(
    ctx: &dyn tidb_expr::rewriter::ColumnResolver,
    refresh: Option<&tidb_ast::MViewRefreshClause>,
) -> Result<(String, String, String), MysqlError> {
    let Some(refresh) = refresh else {
        return Ok(("FAST".to_owned(), String::new(), String::new()));
    };
    match refresh.method {
        tidb_ast::MViewRefreshMethod::Fast => {
            let start_with = match &refresh.start_with {
                Some(expr) => build_and_validate_mview_schedule_expr(ctx, expr, "REFRESH START WITH")?,
                None => String::new(),
            };
            let next = match &refresh.next {
                Some(expr) => build_and_validate_mview_schedule_expr(ctx, expr, "REFRESH NEXT")?,
                None => String::new(),
            };
            Ok(("FAST".to_owned(), start_with, next))
        }
    }
}

/// Go `strconv.Quote` (`%q`): a double-quoted string with Go's escapes.
fn go_quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\u{b}' => out.push_str("\\v"),
            ch if (ch as u32) < 0x20 || ch as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Go master `mviewAttrAlertWarning`.
pub const MVIEW_ATTR_ALERT_WARNING: &str = "mview_alert_warning";
/// Go master `mviewAttrAlertOverdue`.
pub const MVIEW_ATTR_ALERT_OVERDUE: &str = "mview_alert_overdue";
/// Go master `mviewAttrAlertRefreshFailed`.
pub const MVIEW_ATTR_ALERT_REFRESH_FAILED: &str = "mview_alert_refresh_failed";

/// Go master `parseMViewAttributes`: `ATTRIBUTES` as the alert warning and
/// overdue seconds and whether a failed refresh alerts.
///
/// # Errors
///
/// Go's plain errors for a malformed list, an unknown or repeated key, or a
/// bad value.
pub fn parse_mview_attributes(attrs: &str) -> Result<(i64, i64, bool), MysqlError> {
    let attrs = attrs.trim();
    if attrs.is_empty() {
        return Ok((0, 0, false));
    }
    let plain = |message: String| MysqlError::unknown(message);
    let mut seen: Vec<String> = Vec::new();
    let (mut warning, mut overdue, mut refresh_failed) = (0i64, 0i64, false);
    for raw in attrs.split(',') {
        let kv = raw.trim();
        if kv.is_empty() {
            return Err(plain("invalid ATTRIBUTES format: empty key-value pair".to_owned()));
        }
        let pos = kv.find('=');
        let Some(pos) = pos.filter(|&pos| pos > 0 && pos < kv.len() - 1) else {
            return Err(plain(format!("invalid ATTRIBUTES format: {}", go_quote(kv))));
        };
        let key = tidb_hack::go_to_lower(kv[..pos].trim());
        let value = kv[pos + 1..].trim();
        if key.is_empty() || value.is_empty() {
            return Err(plain(format!("invalid ATTRIBUTES format: {}", go_quote(kv))));
        }
        if seen.contains(&key) {
            return Err(plain(format!("duplicate ATTRIBUTES key: {key}")));
        }
        seen.push(key.clone());
        let seconds = |key: &str| {
            value.parse::<i64>().ok().filter(|&seconds| seconds >= 0).ok_or_else(|| {
                plain(format!(
                    "invalid ATTRIBUTES value for {key}: {value} (must be non-negative integer seconds)"
                ))
            })
        };
        match key.as_str() {
            MVIEW_ATTR_ALERT_WARNING => warning = seconds(&key)?,
            MVIEW_ATTR_ALERT_OVERDUE => overdue = seconds(&key)?,
            MVIEW_ATTR_ALERT_REFRESH_FAILED => {
                refresh_failed = match tidb_hack::go_to_lower(value).as_str() {
                    "yes" => true,
                    "no" => false,
                    _ => {
                        return Err(plain(format!(
                            "invalid ATTRIBUTES value for {key}: {value} (must be yes or no)"
                        )))
                    }
                };
            }
            _ => return Err(plain(format!("unsupported ATTRIBUTES key: {key}"))),
        }
    }
    if warning > 0 && overdue > 0 && warning > overdue {
        return Err(plain(format!(
            "invalid ATTRIBUTES: {MVIEW_ATTR_ALERT_WARNING} ({warning}) must be less than or equal to {MVIEW_ATTR_ALERT_OVERDUE} ({overdue})"
        )));
    }
    Ok((warning, overdue, refresh_failed))
}

/// Go `BuildMaterializedViewLogTableInfo`: the log table of `base` for
/// `stmt`, its tracked columns followed by the DML-type and old/new columns,
/// with its `MaterializedViewLogInfo`.
///
/// # Errors
///
/// The log name is too long, a column is repeated, unknown or not
/// supported, the table does not build, or a purge clause is refused.
#[allow(clippy::too_many_arguments)]
pub fn build_materialized_view_log_table_info(
    context: &tidb_metabuild::Context,
    expr_ctx: &dyn tidb_expr::rewriter::ColumnResolver,
    schema_name: &str,
    schema_charset: &str,
    schema_collate: &str,
    placement_policy_ref: Option<&tidb_model::GoShared<tidb_model::PolicyRefInfo>>,
    base: &TableInfo,
    stmt: &CreateMaterializedViewLogStmt,
    enable_check_constraint: bool,
) -> Refusal<TableInfo> {
    let mlog_name = materialized_view_log_table_name(&base.name);
    crate::check_too_long_identifier(mlog_name.original()).map_err(DdlAdmissionError::from_mysql_error)?;

    let base_name = stmt.table.last().map_or("", String::as_str);
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::with_capacity(stmt.cols.len() + 2);
    let mut field_types = Vec::with_capacity(stmt.cols.len() + 2);
    let column_def = built_type_column_def;
    for col in &stmt.cols {
        let lower = tidb_hack::go_to_lower(col);
        let column_exists = || {
            DdlAdmissionError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS.gen_with_stack_by_args(&[col.clone().into()]),
            )
        };
        if !seen.insert(lower.clone()) {
            return Err(column_exists());
        }
        if lower == tidb_hack::go_to_lower(MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME)
            || lower == tidb_hack::go_to_lower(MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME)
        {
            return Err(column_exists());
        }
        let Some(base_col) = base.columns.iter_deref().find(|c| c.read().name.lowercase() == lower) else {
            return Err(DdlAdmissionError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                    .gen_with_stack_by_args(&[col.clone().into(), base_name.to_owned().into()]),
            ));
        };
        let base_col = base_col.read();
        check_materialized_view_log_column_supported(&base_col).map_err(DdlAdmissionError::from_mysql_error)?;
        field_types.push(field_type_for_materialized_view_log_column(&base_col));
        columns.push(column_def(col));
    }
    for (name, code, flen) in [
        (MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, FieldTypeCode::Varchar, 1),
        (MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME, FieldTypeCode::Tiny, 4),
    ] {
        let mut field_type = FieldType::new(code);
        field_type.set_flen(flen);
        field_type.set_flag(FieldTypeFlags::NOT_NULL);
        field_types.push(field_type);
        columns.push(column_def(name));
    }

    let create = CreateTableStmt {
        temporary: CreateTableTemporary::None,
        on_commit_delete: false,
        if_not_exists: false,
        name: vec![schema_name.to_owned(), mlog_name.original().to_owned()],
        like_table: None,
        columns,
        table_constraints: Vec::new(),
        table_options: stmt.options.clone(),
        partitioning: None,
        splits: Vec::new(),
        ctas: None,
    };
    let mut info = build_table_info_with_field_types(
        &create,
        &field_types,
        schema_name,
        schema_charset,
        schema_collate,
        context,
        enable_check_constraint,
    )?;
    crate::table_info_build::inherit_database_placement(&mut info, placement_policy_ref);

    let log_accumulation_alert_rows = build_mlog_accumulation_alert_rows(stmt.accumulation_alert.as_ref())
        .map_err(DdlAdmissionError::from_mysql_error)?;
    let (mut purge_method, mut purge_start_with, mut purge_next) = Default::default();
    if let Some(purge) = &stmt.purge {
        if purge.immediate {
            return Err(DdlAdmissionError::from_mysql_error(unsupported(
                "PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG".to_owned(),
            )));
        }
        purge_method = "DEFERRED".to_owned();
        if let Some(start_with) = &purge.start_with {
            purge_start_with = build_and_validate_mview_schedule_expr(expr_ctx, start_with, "PURGE START WITH")
                .map_err(DdlAdmissionError::from_mysql_error)?;
        }
        let Some(next) = &purge.next else {
            return Err(DdlAdmissionError::from_mysql_error(unsupported(
                "PURGE NEXT is required for CREATE MATERIALIZED VIEW LOG".to_owned(),
            )));
        };
        purge_next = build_and_validate_mview_schedule_expr(expr_ctx, next, "PURGE NEXT")
            .map_err(DdlAdmissionError::from_mysql_error)?;
    }
    info.materialized_view_log = Some(GoShared::new(MaterializedViewLogInfo {
        base_table_id: base.id,
        columns: stmt.cols.iter().map(|col| tidb_ast::CiString::new(col.clone())).collect::<Vec<_>>().into(),
        purge_method,
        purge_start_with,
        purge_next,
        log_accumulation_alert_rows,
        purge_schedule_sql_mode: context.get_sql_mode().0,
        ..Default::default()
    }));
    Ok(info)
}

/// Go master `SetMaterializedScheduleEvalSession`'s evaluation state: the
/// schedule's SQL mode, the UTC time zone, and the statement clock it reads
/// `NOW()` from.
struct ScheduleEvalContext {
    sql_mode: tidb_mysql::SqlMode,
    now: chrono::DateTime<chrono::Utc>,
}

impl ScheduleEvalContext {
    fn date_modes(&self) -> tidb_datatype::DateModes {
        tidb_datatype::DateModes {
            no_zero_date: self.sql_mode.has_no_zero_date_mode(),
            no_zero_in_date: self.sql_mode.has_no_zero_in_date_mode(),
            allow_invalid_dates: self.sql_mode.has_allow_invalid_dates_mode(),
        }
    }
}

impl tidb_expr::rewriter::ColumnResolver for ScheduleEvalContext {
    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        ScheduleEvalContext::date_modes(self)
    }
}

impl tidb_exprctx::context::Columns for ScheduleEvalContext {
    fn get(&self, _path: &[String]) -> Option<tidb_datatype::Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((self.now.timestamp(), self.now.timestamp_subsec_nanos(), 0))
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        ScheduleEvalContext::date_modes(self)
    }
    fn strict_sql_mode(&self) -> bool {
        self.sql_mode.has_strict_mode()
    }
    /// Go `MaterializedScheduleErrLevelsWithSQLMode`.
    fn truncate_level(&self) -> tidb_exprctx::context::ErrorLevel {
        if self.sql_mode.has_strict_mode() {
            tidb_exprctx::context::ErrorLevel::Error
        } else {
            tidb_exprctx::context::ErrorLevel::Warn
        }
    }
    fn division_by_zero_level(&self) -> tidb_exprctx::context::ErrorLevel {
        use tidb_exprctx::context::ErrorLevel;
        if !self.sql_mode.has_error_for_division_by_zero_mode() {
            ErrorLevel::Ignore
        } else if self.sql_mode.has_strict_mode() {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        }
    }
    /// Go `MaterializedScheduleTypeFlagsWithSQLMode`.
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        let strict = self.sql_mode.has_strict_mode();
        let allow_invalid = self.sql_mode.has_allow_invalid_dates_mode();
        tidb_datatype::STRICT_FLAGS
            .with_truncate_as_warning(!strict)
            .with_ignore_invalid_date_err(allow_invalid)
            .with_ignore_zero_in_date_err(!strict || allow_invalid)
            .with_cast_time_to_year_through_concat(true)
    }
    fn cast_time_to_year_through_concat(&self) -> bool {
        true
    }
}

/// Go master `MaterializedScheduleTimeToUnixSeconds`: the time read as UTC.
///
/// # Errors
///
/// A time Go's `GoTime` cannot build.
pub fn materialized_schedule_time_to_unix_seconds(time: tidb_datatype::Time) -> Result<i64, MysqlError> {
    time.go_time(&chrono::Utc)
        .map(|time| time.timestamp())
        .map_err(|error| MysqlError::unknown(error.to_string()))
}

/// Go master `EvalMaterializedScheduleExpr` under
/// `SetMaterializedScheduleEvalSession`: the stored expression parsed under
/// its SQL mode and evaluated in UTC at `now`; `None` for NULL.
///
/// # Errors
///
/// The expression does not parse, build or evaluate, or evaluates to a value
/// that is not a DATE, DATETIME or TIMESTAMP.
pub fn eval_materialized_schedule_expr(
    expr_sql: &str,
    schedule_sql_mode: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<tidb_datatype::Time>, MysqlError> {
    let sql_mode = tidb_mysql::SqlMode(schedule_sql_mode);
    let ctx = ScheduleEvalContext { sql_mode, now };
    let expr = tidb_model::generated_expr::parse_expression_with_sql_mode(
        expr_sql,
        tidb_sessionctx::scanner_sql_mode(sql_mode),
    )
    .map_err(|error| MysqlError::unknown(error.compatibility_message(expr_sql)))?;
    let built = tidb_expr::simple_expr::build_simple_expr(&ctx, &expr, &tidb_expr::simple_expr::BuildOptions::default())
        .map_err(|error| MysqlError::unknown(error.to_string()))?;
    let value = built.eval(&ctx, tidb_chunk::row::Row::empty()).map_err(|error| tidb_expr::EvalErrorSql::to_mysql_error(&error))?;
    let wrong = |what: &str| {
        MysqlError::unknown(format!(
            "materialized schedule expression evaluated to {what}, expected DATE/DATETIME/TIMESTAMP"
        ))
    };
    match value {
        tidb_datatype::Datum::Null => Ok(None),
        // Go's DATE/DATETIME/TIMESTAMP check: every time is one of them.
        tidb_datatype::Datum::Time(time) => Ok(Some(time)),
        other => Err(wrong(other.kind().as_str())),
    }
}

/// Go master `deriveMaterializedScheduleNextUnixSecondsForDDL`: the next
/// purge or refresh time (unix seconds, `None` for NULL) a DDL stores for a
/// `START WITH` / `NEXT` schedule, and whether the stored value is replaced.
/// A `START WITH` within ten seconds of `now` defers to `NEXT` when there is
/// one.
///
/// # Errors
///
/// A schedule expression does not evaluate to a time.
pub fn derive_materialized_schedule_next_unix_seconds(
    start_expr: &str,
    next_expr: &str,
    schedule_sql_mode: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(Option<i64>, bool), MysqlError> {
    let (start_expr, next_expr) = (start_expr.trim(), next_expr.trim());
    if start_expr.is_empty() && next_expr.is_empty() {
        return Ok((None, true));
    }
    let eval = |expr: &str| eval_materialized_schedule_expr(expr, schedule_sql_mode, now);
    let unix = |time: Option<tidb_datatype::Time>| time.map(materialized_schedule_time_to_unix_seconds).transpose();
    if !start_expr.is_empty() {
        let Some(start_at) = eval(start_expr)? else {
            return Ok((None, true));
        };
        if next_expr.is_empty() {
            return Ok((unix(Some(start_at))?, true));
        }
        let near_now = now + chrono::Duration::seconds(10);
        if start_at.go_time(&chrono::Utc).map_err(|error| MysqlError::unknown(error.to_string()))? < near_now {
            return Ok((unix(eval(next_expr)?)?, true));
        }
        return Ok((unix(Some(start_at))?, true));
    }
    Ok((unix(eval(next_expr)?)?, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoColumns;

    impl tidb_expr::rewriter::ColumnResolver for NoColumns {
        fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    fn create_log(sql: &str) -> CreateMaterializedViewLogStmt {
        let tidb_ast::Stmt::Ddl(ddl) = tidb_parser::parse(sql).expect(sql) else {
            panic!("{sql}");
        };
        let tidb_ast::DdlStmt::CreateMaterializedViewLog(stmt) = ddl.into_inner() else {
            panic!("{sql}");
        };
        *stmt
    }

    fn table(json: &str) -> TableInfo {
        serde_json::from_str(json).expect("a Go TableInfo")
    }

    /// Go master's `$mlog$t` / `$mlog$t2` (captured from a Go master node
    /// with the status API after `CREATE MATERIALIZED VIEW LOG`): the builder
    /// makes the same table, up to what the job stamps afterwards (the IDs it
    /// allocates, `State` and `UpdateTS`).
    #[test]
    fn materialized_view_log_table_info_is_go_masters() {
        for (base, mlog, sql) in [
            (
                include_str!("testdata/mview_go_base_t2.json"),
                include_str!("testdata/mview_go_mlog_t2.json"),
                "create materialized view log on t2 (x)",
            ),
            (
                include_str!("testdata/mview_go_base_t.json"),
                include_str!("testdata/mview_go_mlog_t.json"),
                "create materialized view log on t (a, b, c, d, e, f) shard_row_id_bits = 2 purge start with now() next date_add(now(), interval 1 hour) alert rows 100",
            ),
        ] {
            let base = table(base);
            let go: serde_json::Value = serde_json::from_str(mlog).unwrap();
            let stmt = create_log(sql);
            let context = tidb_metabuild::Context::new([]);
            let mut built = build_materialized_view_log_table_info(
                &context, &NoColumns, "e29", "utf8mb4", "utf8mb4_bin", None, &base, &stmt, false,
            )
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            // What the job stamps.
            built.id = go["id"].as_i64().unwrap();
            built.state = tidb_model::SchemaState::PUBLIC;
            built.update_ts = go["update_timestamp"].as_u64().unwrap();
            let rust: serde_json::Value = serde_json::to_value(&built).unwrap();
            assert_eq!(rust, go, "{sql}");
        }
    }

    /// Go's refusals, in Go's order (`TestCreateMaterializedViewLog*` and a
    /// Go master node's answers).
    #[test]
    fn materialized_view_log_refusals_are_go_masters() {
        let base = table(include_str!("testdata/mview_go_base_t.json"));
        let context = tidb_metabuild::Context::new([]);
        for (sql, code, message) in [
            ("create materialized view log on t (a, a)", 1060, "Duplicate column name 'a'"),
            ("create materialized view log on t (_MLOG$_DML_TYPE)", 1060, "Duplicate column name '_MLOG$_DML_TYPE'"),
            ("create materialized view log on t (nosuch)", 1054, "Unknown column 'nosuch' in 't'"),
            ("create materialized view log on t (g)", 8200, "CREATE MATERIALIZED VIEW LOG does not support BLOB column g"),
            ("create materialized view log on t (a) purge next 5", 8200, "PURGE NEXT expression must return DATE/DATETIME/TIMESTAMP, but got bigint"),
            ("create materialized view log on t (a) purge next 'abc'", 8200, "PURGE NEXT expression must return DATE/DATETIME/TIMESTAMP, but got var_string"),
            ("create materialized view log on t (a) purge immediate", 8200, "PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG"),
            ("create materialized view log on t (a) alert rows -1", 1105, "invalid ALERT ROWS value: -1 (must be non-negative)"),
        ] {
            let error = build_materialized_view_log_table_info(
                &context, &NoColumns, "e29", "utf8mb4", "utf8mb4_bin", None, &base, &create_log(sql), false,
            )
            .expect_err(sql)
            .to_mysql_error();
            assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
        }
    }

    /// Go master `deriveMaterializedScheduleNextUnixSecondsForDDL` at a fixed
    /// clock: a START WITH within ten seconds of now defers to NEXT, a later
    /// one is the next time itself, NULL clears the stored time, and a value
    /// that is not a time is refused. A Go master node stored now() + 3600
    /// for `START WITH now() NEXT date_add(now(), interval 1 hour)`.
    #[test]
    fn materialized_schedule_next_time_follows_go_master() {
        use chrono::TimeZone as _;
        let now = chrono::Utc.with_ymd_and_hms(2026, 11, 18, 3, 4, 5).unwrap();
        let mode = 1_436_549_152; // Go's default SQL mode.
        let derive = |start: &str, next: &str| derive_materialized_schedule_next_unix_seconds(start, next, mode, now);
        assert_eq!(derive("", "").unwrap(), (None, true));
        assert_eq!(
            derive("NOW()", "DATE_ADD(NOW(), INTERVAL 1 HOUR)").unwrap(),
            (Some(now.timestamp() + 3600), true)
        );
        assert_eq!(derive("NOW()", "").unwrap(), (Some(now.timestamp()), true));
        assert_eq!(
            derive("TIMESTAMP('2030-01-02 03:04:05')", "DATE_ADD(NOW(), INTERVAL 1 HOUR)").unwrap(),
            (Some(chrono::Utc.with_ymd_and_hms(2030, 1, 2, 3, 4, 5).unwrap().timestamp()), true)
        );
        assert_eq!(derive("", "DATE_ADD(NOW(), INTERVAL 2 DAY)").unwrap(), (Some(now.timestamp() + 2 * 86400), true));
        assert_eq!(derive("NULL", "NOW()").unwrap(), (None, true));
        let error = derive("", "5").unwrap_err();
        assert!(error.message.contains("evaluated to bigint, expected DATE/DATETIME/TIMESTAMP"), "{}", error.message);
    }
}
