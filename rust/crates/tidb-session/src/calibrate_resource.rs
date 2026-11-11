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

//! Go `pkg/executor/internal/calibrateresource`'s `Executor`: `CALIBRATE
//! RESOURCE` over this session — its option list, the cluster's servers and
//! the `METRICS_SCHEMA` queries; the estimation itself is the executor's.

use chrono::{DateTime, Duration, Utc};
use tidb_ast::CalibrateResourceOption;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_driver_error::DriverError;
use tidb_executor::calibrate_resource::{self as calibrate, TimePointValue, TimeSeriesValues};
use tidb_executor::StmtOutput;

use crate::Session;

/// Go `maxDuration`: the longest window a calibration reads.
const MAX_DURATION: Duration = Duration::hours(24);
/// Go `minDuration`: the shortest.
const MIN_DURATION: Duration = Duration::minutes(1);

fn unknown(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go `time.DateTime`, in the session's time zone.
fn format_date_time(time: DateTime<Utc>, location: &tidb_datatype::SessionTimeZone) -> String {
    time.with_timezone(location).format("%Y-%m-%d %H:%M:%S").to_string()
}

impl Session {
    /// Go `Executor.Next`: the estimated capacity in `QUOTA`.
    pub(crate) fn calibrate_resource(
        &mut self,
        stmt: &tidb_ast::CalibrateResourceStmt,
    ) -> Result<StmtOutput, DriverError> {
        if !tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_SUPPORT_DISABLED,
            )));
        }
        let quota = if stmt.options.is_empty() {
            self.static_calibrate(stmt)?
        } else {
            self.dynamic_calibrate(stmt)?
        };
        Ok(StmtOutput::Rows {
            columns: vec![("QUOTA".to_owned(), FieldType::new(FieldTypeCode::LongLong))],
            rows: vec![vec![Datum::UInt(quota)]],
        })
    }

    /// Go `infoschema.GetClusterServerInfo(e.Ctx())`.
    fn calibrate_cluster_info(&mut self) -> Result<Vec<tidb_domain::cluster_topology::ClusterServer>, DriverError> {
        let mut warnings = Vec::new();
        calibrate::get_cluster_server_info(self.cluster_topology.as_deref(), &mut warnings).map_err(unknown)
    }

    /// Go `staticCalibrate`.
    fn static_calibrate(&mut self, stmt: &tidb_ast::CalibrateResourceStmt) -> Result<u64, DriverError> {
        let resource_group_ctl = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .and_then(|domain| domain.runaway_manager())
            .map(|manager| std::sync::Arc::clone(&manager.resource_group_ctl))
            .ok_or_else(|| unknown("resource group controller is not initialized"))?;
        let cluster_info = self.calibrate_cluster_info()?;
        let ru_cfg = resource_group_ctl.get_config();
        calibrate::static_calibrate(&ru_cfg, stmt.workload, &cluster_info).map_err(unknown)
    }

    /// Go `dynamicCalibrate`: the TiDB and the TiFlash estimates of the
    /// window together; one of them failing is no quota of its own.
    fn dynamic_calibrate(&mut self, stmt: &tidb_ast::CalibrateResourceStmt) -> Result<u64, DriverError> {
        let (start_ts, end_ts) = self.parse_calibrate_duration(&stmt.options)?;
        let cluster_info = self.calibrate_cluster_info()?;
        let tidb_quota = self.get_tidb_quota(&cluster_info, start_ts, end_ts);
        let tiflash_quota = self.get_tiflash_quota(&cluster_info, start_ts, end_ts);
        match (tidb_quota, tiflash_quota) {
            (Err(error), Err(_)) => Err(error),
            (tidb_quota, tiflash_quota) => {
                Ok((tidb_quota.unwrap_or_default() + tiflash_quota.unwrap_or_default()) as u64)
            }
        }
    }

    /// Go `getTiDBQuota`.
    fn get_tidb_quota(
        &mut self,
        cluster_info: &[tidb_domain::cluster_topology::ClusterServer],
        start_ts: DateTime<Utc>,
        end_ts: DateTime<Utc>,
    ) -> Result<f64, DriverError> {
        let location = self.vars.session_time_zone();
        let start_time = format_date_time(start_ts, &location);
        let end_time = format_date_time(end_ts, &location);

        let total_kv_cpu_quota = calibrate::get_tikv_total_cpu_quota(cluster_info)
            .map_err(|error| unknown(calibrate::err_no_cpu_quota_metrics(&error)))?;
        let total_tidb_cpu = calibrate::get_tidb_total_cpu_quota(cluster_info);
        let rus = self.get_ru_per_sec("resource_manager_resource_unit", &start_time, &end_time)?;
        let tikv_cpus = self.get_component_cpu_usage_per_sec("tikv", &start_time, &end_time)?;
        let tidb_cpus = self.get_component_cpu_usage_per_sec("tidb", &start_time, &end_time)?;
        calibrate::get_tidb_quota(rus, tikv_cpus, tidb_cpus, total_kv_cpu_quota, total_tidb_cpu, start_ts, end_ts)
            .map_err(unknown)
    }

    /// Go `getTiFlashQuota`.
    fn get_tiflash_quota(
        &mut self,
        cluster_info: &[tidb_domain::cluster_topology::ClusterServer],
        start_ts: DateTime<Utc>,
        end_ts: DateTime<Utc>,
    ) -> Result<f64, DriverError> {
        let location = self.vars.session_time_zone();
        let start_time = format_date_time(start_ts, &location);
        let end_time = format_date_time(end_ts, &location);

        let total_tiflash_logical_cores = calibrate::get_tiflash_logical_cores(cluster_info)
            .map_err(|error| unknown(calibrate::err_no_cpu_quota_metrics(&error)))?;
        // Go `getTiFlashCPUUsagePerSec`.
        let query = format!("SELECT time, sum(value) FROM METRICS_SCHEMA.tiflash_process_cpu_usage where time >= '{start_time}' and time <= '{end_time}' and job = 'tiflash' GROUP BY time ORDER BY time asc");
        let tiflash_cpus = self.get_values_from_metrics(&query)?;
        let tiflash_rus =
            self.get_ru_per_sec("tiflash_resource_manager_resource_unit", &start_time, &end_time)?;
        calibrate::get_tiflash_quota(tiflash_rus, tiflash_cpus, total_tiflash_logical_cores).map_err(unknown)
    }

    /// Go `getRUPerSec` and `getTiFlashRUPerSec`.
    fn get_ru_per_sec(&mut self, table: &str, start_time: &str, end_time: &str) -> Result<TimeSeriesValues, DriverError> {
        let query = format!("SELECT time, value FROM METRICS_SCHEMA.{table} where time >= '{start_time}' and time <= '{end_time}' ORDER BY time asc");
        self.get_values_from_metrics(&query)
    }

    /// Go `getComponentCPUUsagePerSec`.
    fn get_component_cpu_usage_per_sec(
        &mut self,
        component: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<TimeSeriesValues, DriverError> {
        let query = format!("SELECT time, sum(value) FROM METRICS_SCHEMA.process_cpu_usage where time >= '{start_time}' and time <= '{end_time}' and job like '%{component}' GROUP BY time ORDER BY time asc");
        self.get_values_from_metrics(&query)
    }

    /// Go `getValuesFromMetrics`: `query` on this session
    /// (`ExecOptionUseCurSession`), a row whose time is no instant of the
    /// session's zone left out.
    fn get_values_from_metrics(&mut self, query: &str) -> Result<TimeSeriesValues, DriverError> {
        let stmt = tidb_parser::parse(query).map_err(|error| unknown(error.compatibility_message(query)))?;
        let StmtOutput::Rows { rows, .. } = self.execute_statement_owned(stmt, query)? else {
            return Ok(TimeSeriesValues::default());
        };
        let location = self.vars.session_time_zone();
        let mut vals = Vec::with_capacity(rows.len());
        for row in rows {
            let (Some(Datum::Time(time)), Some(value)) = (row.first(), row.get(1)) else {
                continue;
            };
            let Ok(tp) = time.go_time(&location) else {
                continue;
            };
            let val = match value {
                Datum::Real(val) => *val,
                _ => 0.0,
            };
            vals.push(TimePointValue { tp: tp.with_timezone(&Utc), val });
        }
        Ok(TimeSeriesValues::new(vals))
    }

    /// Go `parseTsExpr`.
    fn parse_ts_expr(&mut self, ts_expr: &tidb_ast::Expr) -> Result<DateTime<Utc>, DriverError> {
        let ts = self.calculate_as_of_ts_expr(ts_expr)?;
        // Go `oracle.GetTimeFromTS`.
        Ok(DateTime::from_timestamp_millis(tikv_client::oracle::extract_physical(ts)).unwrap_or_default())
    }

    /// [`Self::parse_ts_expr`] of a `DATE_ADD` / `DATE_SUB` call. `DURATION
    /// <expr>` has no unit, which no SQL text can say: the call's date and
    /// interval are evaluated and `types.ParseDurationValue` refuses the unit.
    fn parse_date_arith_ts(&mut self, call: &tidb_ast::Expr, unit: &str) -> Result<DateTime<Utc>, DriverError> {
        if !unit.is_empty() {
            return self.parse_ts_expr(call);
        }
        let tidb_ast::Expr::Func { args, .. } = call else {
            unreachable!("a date arithmetic call")
        };
        let [date, tidb_ast::Expr::Interval { value, .. }] = args.as_slice() else {
            unreachable!("a date and an interval")
        };
        self.parse_ts_expr(date)?;
        // A NULL interval is a NULL date, which is no timestamp.
        if matches!(self.eval_value(value)?, Datum::Null) {
            self.parse_ts_expr(&tidb_ast::Expr::Null)?;
        }
        Err(unknown(format!("invalid single timeunit - {unit}")))
    }

    /// Go `parseCalibrateDuration`: the window the options name. `None` is
    /// Go's zero time.
    fn parse_calibrate_duration(
        &mut self,
        options: &[CalibrateResourceOption],
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), DriverError> {
        let mut start_time = None;
        let mut end_time = None;
        // startTimeExpr and endTimeExpr are used to calc endTime by FuncCallExpr when duration begin with `interval`.
        let mut start_time_expr = None;
        let mut end_time_expr = None;
        for op in options {
            match op {
                CalibrateResourceOption::StartTime(ts) => {
                    start_time_expr = Some(ts.clone());
                    start_time = Some(self.parse_ts_expr(ts)?);
                }
                CalibrateResourceOption::EndTime(ts) => {
                    end_time_expr = Some(ts.clone());
                    end_time = Some(self.parse_ts_expr(ts)?);
                }
                CalibrateResourceOption::DurationString(_)
                | CalibrateResourceOption::DurationInterval { .. }
                | CalibrateResourceOption::Duration(_) => {}
            }
        }
        for op in options {
            let (value, unit) = match op {
                CalibrateResourceOption::StartTime(_) | CalibrateResourceOption::EndTime(_) => continue,
                // string duration
                CalibrateResourceOption::DurationString(value) => {
                    let nanos =
                        tidb_parser::parse_config_duration(value).map_err(|error| unknown(error.to_string()))?;
                    let dur = Duration::nanoseconds(nanos);
                    // If startTime is not set, startTime will be now() - duration.
                    let start = *start_time.get_or_insert_with(|| end_time.unwrap_or_else(Utc::now) - dur);
                    // If endTime is set, duration will be ignored.
                    end_time.get_or_insert(start + dur);
                    continue;
                }
                CalibrateResourceOption::DurationInterval { value, unit } => (value, unit.as_str()),
                CalibrateResourceOption::Duration(value) => (value, ""),
            };
            // interval duration
            let date_arith = |name: &str, date: tidb_ast::Expr| tidb_ast::Expr::Func {
                name: name.to_owned(),
                args: vec![
                    date,
                    tidb_ast::Expr::Interval { value: Box::new(value.clone()), unit: unit.to_owned() },
                ],
                origin_position: 0,
            };
            // If startTime is not set, startTime will be now() - duration.
            let from_time_expr = match start_time_expr.take() {
                Some(expr) => expr,
                None => {
                    let to_time_expr = end_time_expr.clone().filter(|_| end_time.is_some()).unwrap_or_else(|| {
                        tidb_ast::Expr::Func {
                            name: "CURRENT_TIMESTAMP".to_owned(),
                            args: Vec::new(),
                            origin_position: 0,
                        }
                    });
                    let expr = date_arith("DATE_SUB", to_time_expr);
                    start_time = Some(self.parse_date_arith_ts(&expr, unit)?);
                    expr
                }
            };
            // If endTime is set, duration will be ignored.
            if end_time.is_none() {
                end_time = Some(self.parse_date_arith_ts(&date_arith("DATE_ADD", from_time_expr.clone()), unit)?);
            }
            start_time_expr = Some(from_time_expr);
        }

        let Some(start_time) = start_time else {
            return Err(unknown("start time should not be 0"));
        };
        let end_time = end_time.unwrap_or_else(Utc::now);
        // check the duration
        let dur = end_time - start_time;
        // add the buffer duration
        if dur > MAX_DURATION + Duration::minutes(1) {
            return Err(unknown("the duration of calibration is too long, which could lead to inaccurate output. Please make the duration between 1m0s and 24h0m0s"));
        }
        // We only need to consider the case where the duration is slightly enlarged.
        if dur < MIN_DURATION {
            return Err(unknown("the duration of calibration is too short, which could lead to inaccurate output. Please make the duration between 1m0s and 24h0m0s"));
        }
        Ok((start_time, end_time))
    }
}
