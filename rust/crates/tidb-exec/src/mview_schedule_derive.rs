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

//! Shared derivation core for the materialized-view schedule expressions
//! (`deriveCreateMaterializedScheduleNextUnixSeconds`, master
//! `94a9cbedab`): the near-now decision tree both the log purge and the
//! view refresh derivations walk, evaluated through the driver's FROM-less
//! SELECT under the recorded SQL mode and schedule zone.

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_executor::StmtContext;
use tidb_model::ResolvedTimeZone;

/// One evaluated schedule expression: the native time plus whether the
/// expression's type was DATETIME (Go's `evalCreateMaterializedView
/// ScheduleExprToDatetime` converts to `TypeDatetime`; the persisted
/// deadline does not depend on that conversion, only on the value).
#[derive(Clone, Copy, Debug)]
pub struct ScheduleEvaluation {
    /// The evaluated `types.Time` value.
    pub time: tidb_datatype::Time,
    /// Whether the expression's type was DATETIME.
    #[allow(dead_code)]
    pub is_datetime: bool,
}

/// Go's decision-tree outcome: the deadline to persist (SQL NULL when
/// `None`) and whether the persisted deadline should be overwritten.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduleDecision {
    /// The evaluated `NEXT_*_UNIX_SECONDS` deadline.
    pub next_unix_seconds: Option<i64>,
    /// Go `shouldUpdate`.
    pub should_update: bool,
}

/// Evaluates one schedule expression through a FROM-less `SELECT` under the
/// schedule evaluation context.
fn evaluate_schedule_expression(
    expr: &str,
    empty_catalog: &tidb_executor::Catalog,
    eval_context: &StmtContext,
) -> Result<Option<ScheduleEvaluation>, String> {
    let sql = format!("SELECT {expr}");
    let (columns, rows) = tidb_executor::run_select_meta_in(
        &sql,
        empty_catalog,
        tidb_executor::DEFAULT_DATABASE,
        eval_context,
    )
    .map_err(|error| error.to_string())?;
    if columns.len() != 1 {
        return Err(format!(
            "the schedule expression must evaluate to one column, got {}",
            columns.len()
        ));
    }
    let Some(row) = rows.first() else {
        return Err("the schedule expression evaluated to no row".to_owned());
    };
    Ok(match &row[0] {
        Datum::Null => None,
        Datum::Time(time) => Some(ScheduleEvaluation {
            time: *time,
            is_datetime: columns[0].1.code() == tidb_datatype::FieldTypeCode::Datetime,
        }),
        other => {
            return Err(format!(
                "the schedule expression must return DATETIME/TIMESTAMP, got {other:?}"
            ))
        }
    })
}

/// Converts an evaluated time to unix seconds under the schedule zone (Go
/// `expression.MaterializedScheduleTimeToUnixSeconds`).
fn schedule_time_to_unix_seconds(
    time: &tidb_datatype::Time,
    zone: &ResolvedTimeZone,
) -> Result<i64, String> {
    let core = time.core_time();
    let unix = match zone {
        ResolvedTimeZone::Local => core
            .to_datetime(&chrono::Local)
            .map(|datetime| datetime.timestamp()),
        ResolvedTimeZone::Named(zone) => {
            core.to_datetime(zone).map(|datetime| datetime.timestamp())
        }
        ResolvedTimeZone::Fixed { offset_seconds, .. } => {
            let offset = chrono::FixedOffset::east_opt(i32::try_from(*offset_seconds).unwrap_or(0))
                .ok_or("invalid schedule time zone offset")?;
            core.to_datetime(&offset)
                .map(|datetime| datetime.timestamp())
        }
    };
    unix.map_err(|error| error.to_string())
}

/// Builds the schedule evaluation context from the recorded definition SQL
/// mode and schedule zone (Go's
/// `setCreateMaterializedViewScheduleEvalSession`, with the owner's live
/// wall clock behind the lazy statement clock).
pub fn schedule_eval_context(
    sql_mode: u64,
    zone: &ResolvedTimeZone,
    source_zone_name: &str,
) -> StmtContext {
    let session_zone = match zone {
        ResolvedTimeZone::Local => SessionTimeZone::Local,
        ResolvedTimeZone::Named(zone) => SessionTimeZone::Named(*zone),
        ResolvedTimeZone::Fixed { offset_seconds, .. } => SessionTimeZone::Fixed {
            name: source_zone_name.to_owned(),
            offset_secs: i32::try_from(*offset_seconds).unwrap_or(0),
        },
    };
    context_with_schedule(sql_mode, session_zone)
}

fn context_with_schedule(sql_mode: u64, session_zone: SessionTimeZone) -> StmtContext {
    StmtContext::for_query()
        .with_lazy_clock(None, session_zone)
        .with_ddl_sql_mode(i64::try_from(sql_mode).unwrap_or_default())
}

/// Go `deriveCreateMaterializedScheduleNextUnixSeconds`: the shared
/// near-now decision tree. Both expressions empty yields `(None, true)`
/// with no evaluation.
#[allow(clippy::too_many_arguments)]
pub fn derive_schedule_decision(
    start_expr: &str,
    next_expr: &str,
    zone: &ResolvedTimeZone,
    sql_mode: u64,
    context: &StmtContext,
    schema_name: &str,
    table_name: &str,
    log_null: &dyn Fn(&str, &str, &str, &str, &str),
) -> Result<ScheduleDecision, String> {
    let start_expr = start_expr.trim();
    let next_expr = next_expr.trim();
    if start_expr.is_empty() && next_expr.is_empty() {
        return Ok(ScheduleDecision {
            next_unix_seconds: None,
            should_update: true,
        });
    }

    let empty_catalog = tidb_executor::Catalog::default();
    let eval_context = schedule_eval_context(sql_mode, zone, source_zone_name(zone));

    // Go `loadCreateMaterializedViewScheduleNow`.
    let now = evaluate_schedule_expression("NOW(6)", &empty_catalog, &eval_context)?
        .ok_or("SELECT NOW(6) evaluated to NULL")?
        .time;

    let to_unix =
        |evaluation: &ScheduleEvaluation| schedule_time_to_unix_seconds(&evaluation.time, zone);

    // Go: the near-now threshold is now + 10s in the schedule zone.
    let threshold = {
        let threshold_core = now.core_time().add_duration(10_000_000_000);
        tidb_datatype::Time::new(threshold_core, now.kind(), i64::from(now.fsp()))
            .map_err(|error| format!("near-now threshold: {error}"))?
    };

    if !start_expr.is_empty() {
        let Some(start_at) =
            evaluate_schedule_expression(start_expr, &empty_catalog, &eval_context)?
        else {
            log_null(schema_name, table_name, "START WITH", start_expr, next_expr);
            return Ok(ScheduleDecision {
                next_unix_seconds: None,
                should_update: true,
            });
        };
        let decide = |evaluation: &ScheduleEvaluation| {
            to_unix(evaluation).map(|next_unix_seconds| ScheduleDecision {
                next_unix_seconds: Some(next_unix_seconds),
                should_update: true,
            })
        };
        if next_expr.is_empty() {
            return decide(&start_at);
        }
        if start_at.time.compare(threshold) == std::cmp::Ordering::Less {
            let Some(next_at) =
                evaluate_schedule_expression(next_expr, &empty_catalog, &eval_context)?
            else {
                log_null(schema_name, table_name, "NEXT", start_expr, next_expr);
                return Ok(ScheduleDecision {
                    next_unix_seconds: None,
                    should_update: true,
                });
            };
            return decide(&next_at);
        }
        return decide(&start_at);
    }

    let Some(next_at) = evaluate_schedule_expression(next_expr, &empty_catalog, &eval_context)?
    else {
        log_null(schema_name, table_name, "NEXT", start_expr, next_expr);
        return Ok(ScheduleDecision {
            next_unix_seconds: None,
            should_update: true,
        });
    };
    to_unix(&next_at).map(|next_unix_seconds| ScheduleDecision {
        next_unix_seconds: Some(next_unix_seconds),
        should_update: true,
    })
}

fn source_zone_name(zone: &ResolvedTimeZone) -> &'static str {
    match zone {
        ResolvedTimeZone::Named(_) | ResolvedTimeZone::Fixed { .. } => "",
        ResolvedTimeZone::Local => "Local",
    }
}
