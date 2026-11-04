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

//! Go `data.go`: one report.

use serde::Serialize;

use crate::data_feature_usage::{self, FeatureUsage};
use crate::data_window::{self, WindowData};
use crate::Session;

/// Go `telemetryData`.
#[derive(Debug, Serialize)]
pub struct TelemetryData {
    #[serde(rename = "reportTimestamp")]
    pub report_timestamp: i64,
    #[serde(rename = "featureUsage")]
    pub feature_usage: Option<FeatureUsage>,
    #[serde(rename = "windowedStats")]
    pub windowed_stats: Vec<WindowData>,
}

/// Go `generateTelemetryData`.
pub(crate) fn generate_telemetry_data(sctx: &mut dyn Session) -> TelemetryData {
    TelemetryData {
        report_timestamp: chrono::Utc::now().timestamp(),
        feature_usage: data_feature_usage::get_feature_usage(sctx).ok(),
        windowed_stats: data_window::get_window_data(),
    }
}

/// Go `postReportTelemetryData`.
pub(crate) fn post_report_telemetry_data() {
    data_feature_usage::post_report_txn_usage();
    data_feature_usage::post_report_cte_usage();
    data_feature_usage::post_report_account_lock_usage();
    data_feature_usage::post_report_multi_schema_change_usage();
    data_feature_usage::post_report_exchange_partition_usage();
    data_feature_usage::post_report_table_partition_usage();
    data_feature_usage::post_report_non_transactional_counter();
    data_feature_usage::post_savepoint_count();
    data_feature_usage::post_report_lazy_pessimistic_unique_check_set_count();
    data_feature_usage::post_report_ddl_usage();
    data_feature_usage::post_report_index_merge_usage();
    data_feature_usage::post_store_batch_usage();
    data_feature_usage::post_report_fair_locking_usage_counter();
}
