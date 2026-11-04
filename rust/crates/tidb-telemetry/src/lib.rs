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

//! Go `pkg/telemetry`: the usage report a node logs every
//! [`REPORT_INTERVAL`] while telemetry is enabled (`telemetry.go`).

use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_log::{Field, Value};

mod data;
mod data_feature_usage;
mod data_window;
mod ttl;

pub use data_window::{
    rotate_sub_window, BuiltinFunctionsUsage, CURRENT_COPR_CACHE_HIT_RATIO_GTE0_COUNT,
    CURRENT_COPR_CACHE_HIT_RATIO_GTE100_COUNT, CURRENT_COPR_CACHE_HIT_RATIO_GTE10_COUNT,
    CURRENT_COPR_CACHE_HIT_RATIO_GTE1_COUNT, CURRENT_COPR_CACHE_HIT_RATIO_GTE20_COUNT,
    CURRENT_COPR_CACHE_HIT_RATIO_GTE40_COUNT, CURRENT_COPR_CACHE_HIT_RATIO_GTE80_COUNT,
    CURRENT_EXECUTE_COUNT, CURRENT_TIFLASH_EXCHANGE_PUSH_DOWN_COUNT, CURRENT_TIFLASH_PUSH_DOWN_COUNT,
    CURRENT_TIFLASH_TABLE_SCAN_COUNT, CURRENT_TIFLASH_TABLE_SCAN_WITH_FAST_SCAN_COUNT,
    GLOBAL_BUILTIN_FUNCTIONS_USAGE, SUB_WINDOW_SIZE, WINDOW_SIZE,
};

// Go's `export_test.go` exports these to the package tests.
#[doc(hidden)]
pub use data_feature_usage::{
    get_feature_usage, get_txn_usage_info, FeatureUsage, NewClusterIndexUsage, PlacementPolicyUsage,
    ResourceControlUsage, TxnUsage,
};
#[doc(hidden)]
pub use ttl::{TtlHistItem, TtlUsageCounter};

/// Go `ReportInterval`.
pub const REPORT_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// What Go's telemetry reads from its `sessionctx.Context`.
pub trait Session {
    /// Go `GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar` (and
    /// `GetSessionVars().GetGlobalSystemVar`, which reads the same store).
    ///
    /// # Errors
    ///
    /// The accessor's error.
    fn get_global_sys_var(&self, name: &str) -> Result<String, String>;

    /// Go `GetSessionVars().CostModelVersion`.
    fn cost_model_version(&self) -> i64;

    /// Go `GetSessionVars().EnablePaging`.
    fn enable_paging(&self) -> bool;

    /// Go `ExecRestrictedSQL(ctx, nil, sql)`.
    ///
    /// # Errors
    ///
    /// The statement's error.
    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<Datum>>, String>;

    /// Go `RefreshTxnCtx`.
    ///
    /// # Errors
    ///
    /// The refresh's error.
    fn refresh_txn_ctx(&mut self) -> Result<(), String>;

    /// Go `GetDomainInfoSchema(ctx)` / `GetLatestInfoSchema()`.
    fn info_schema(&self) -> Arc<tidb_infoschema::SessionExtendedInfoSchema>;
}

/// Go `getTelemetryGlobalVariable`.
fn get_telemetry_global_variable(ctx: &dyn Session) -> Result<bool, String> {
    let value = ctx.get_global_sys_var(tidb_vardef::tidb_vars::TIDB_ENABLE_TELEMETRY);
    // Go returns `TiDBOptOn(val)` beside the error.
    let enabled = value.as_deref().is_ok_and(tidb_vardef::option_values::tidb_opt_on);
    value.map(|_| enabled)
}

/// Go `IsTelemetryEnabled`.
///
/// # Errors
///
/// The global-variable read's error.
pub fn is_telemetry_enabled(ctx: &dyn Session) -> Result<bool, String> {
    if !tidb_config::config_tree::config::get_global_config().enable_telemetry {
        return Ok(false);
    }
    get_telemetry_global_variable(ctx)
}

/// Go `ReportUsageData`: generates the latest usage data and logs it.
///
/// # Errors
///
/// The enabled check's error.
pub fn report_usage_data(ctx: &mut dyn Session) -> Result<(), String> {
    if !is_telemetry_enabled(&*ctx)? {
        return Ok(());
    }
    let data = data::generate_telemetry_data(ctx);
    data::post_report_telemetry_data();
    let raw_json = serde_json::to_string(&data).map_err(|error| error.to_string())?;
    logger().info("", &[Field::new("telemetry data", Value::Str(raw_json))]);
    Ok(())
}

/// Go `InitialRun`: reports the telemetry configuration and runs once.
///
/// # Errors
///
/// The enabled check's error.
pub fn initial_run(ctx: &mut dyn Session) -> Result<(), String> {
    let enabled = is_telemetry_enabled(&*ctx)?;
    logger().info(
        "Telemetry configuration",
        &[
            Field::new(
                "report_interval",
                Value::Duration(i64::try_from(REPORT_INTERVAL.as_nanos()).unwrap_or(i64::MAX)),
            ),
            Field::new("enabled", Value::Bool(enabled)),
        ],
    );
    report_usage_data(ctx)
}

/// Go `Logger`: the background logger with category `telemetry`.
#[must_use]
pub fn logger() -> tidb_logutil::Logger {
    tidb_logutil::bg_logger().with_fields(&[Field::new(
        tidb_logutil::LOG_FIELD_CATEGORY,
        Value::Str("telemetry".to_owned()),
    )])
}

/// Go `PostReportTelemetryDataForTest`.
pub fn post_report_telemetry_data_for_test() {
    data_feature_usage::post_report_table_partition_usage();
}

/// Go `PostSavepointCount`.
pub fn post_savepoint_count() {
    data_feature_usage::post_savepoint_count();
}

#[cfg(test)]
mod tests;
