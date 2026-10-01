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

//! Metric observation adapters backed by the native PD package owner.

use crate::error::PdOperation;
use tikv_client::pd_metrics;

/// Register the complete Go collector set once, before opening the PD client.
pub fn init_dashboard_series() {
    pd_metrics::init_and_register_metrics(Default::default());
}

/// Metadata commands defer total duration and separately record failed duration.
/// Stream request durations belong to the native TSO stream, not metadata RPCs.
pub fn observe_cmd(operation: PdOperation, seconds: f64, succeeded: bool) {
    let metrics = pd_metrics::global_metrics();
    let (total, failed) = match operation {
        PdOperation::GetMembers => (
            &metrics.cmd_duration_get_all_members,
            &metrics.cmd_failed_duration_get_all_members,
        ),
        PdOperation::GetRegion => (
            &metrics.cmd_duration_get_region,
            &metrics.cmd_failed_duration_get_region,
        ),
        PdOperation::GetPrevRegion => (
            &metrics.cmd_duration_get_prev_region,
            &metrics.cmd_failed_duration_get_prev_region,
        ),
        PdOperation::GetRegionById => (
            &metrics.cmd_duration_get_region_by_id,
            &metrics.cmd_failed_duration_get_region_by_id,
        ),
        PdOperation::ScanRegions => (
            &metrics.cmd_duration_scan_regions,
            &metrics.cmd_failed_duration_scan_regions,
        ),
        PdOperation::BatchScanRegions => (
            &metrics.cmd_duration_batch_scan_regions,
            &metrics.cmd_failed_duration_batch_scan_regions,
        ),
        PdOperation::GetStore => (
            &metrics.cmd_duration_get_store,
            &metrics.cmd_failed_duration_get_store,
        ),
        PdOperation::GetAllStores => (
            &metrics.cmd_duration_get_all_stores,
            &metrics.cmd_failed_duration_get_all_stores,
        ),
        PdOperation::GetGcState => (
            &metrics.cmd_duration_get_gc_state,
            &metrics.cmd_failed_duration_get_gc_state,
        ),
        // Native TSO owns its command and stream timing. Go does not time
        // StoreGlobalConfig with these command collectors.
        PdOperation::Tso | PdOperation::StoreGlobalConfig => return,
    };
    total.observe(seconds);
    if !succeeded {
        failed.observe(seconds);
    }
}

/// Record a delivered timestamp result's wait duration. Caller cancellation or
/// disconnection does not take Go Request.waitCtx's result-observation branch.
pub fn observe_tso_wait(seconds: f64, succeeded: bool) {
    let metrics = pd_metrics::global_metrics();
    if succeeded {
        metrics.cmd_duration_tso_wait.observe(seconds);
    } else {
        metrics.cmd_failed_duration_tso_wait.observe(seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(family: &str, kind: &str) -> u64 {
        prometheus::gather()
            .iter()
            .filter(|f| f.get_name() == family)
            .flat_map(|f| f.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "type" && l.get_value() == kind)
            })
            .map(|m| m.get_histogram().get_sample_count())
            .sum()
    }

    #[test]
    fn failed_metadata_command_counts_total_and_failure_without_tso_stream_sample() {
        init_dashboard_series();
        let total = count("pd_client_cmd_handle_cmds_duration_seconds", "get_gc_state");
        let failed = count(
            "pd_client_cmd_handle_failed_cmds_duration_seconds",
            "get_gc_state",
        );
        let stream = count(
            "pd_client_request_handle_requests_duration_seconds",
            "get_gc_state",
        );
        observe_cmd(crate::error::PdOperation::GetGcState, 0.25, false);
        assert_eq!(
            count("pd_client_cmd_handle_cmds_duration_seconds", "get_gc_state"),
            total + 1
        );
        assert_eq!(
            count(
                "pd_client_cmd_handle_failed_cmds_duration_seconds",
                "get_gc_state"
            ),
            failed + 1
        );
        assert_eq!(
            count(
                "pd_client_request_handle_requests_duration_seconds",
                "get_gc_state"
            ),
            stream
        );
    }
    #[test]
    fn shared_owner_preserves_tso_result_classes_and_go_series() {
        init_dashboard_series();
        // A second initializer is a no-op, including native-client setup.
        tikv_client::pd_metrics::init_and_register_metrics(Default::default());
        let success = count("pd_client_cmd_handle_cmds_duration_seconds", "wait");
        let failure = count("pd_client_cmd_handle_failed_cmds_duration_seconds", "wait");
        observe_tso_wait(0.1, false);
        assert_eq!(
            count("pd_client_cmd_handle_cmds_duration_seconds", "wait"),
            success
        );
        assert_eq!(
            count("pd_client_cmd_handle_failed_cmds_duration_seconds", "wait"),
            failure + 1
        );
        observe_tso_wait(0.1, true);
        assert_eq!(
            count("pd_client_cmd_handle_cmds_duration_seconds", "wait"),
            success + 1
        );
        let families = prometheus::gather();
        let stream = families
            .iter()
            .find(|f| f.get_name() == "pd_client_request_handle_requests_duration_seconds")
            .unwrap();
        let mut kinds = stream
            .get_metric()
            .iter()
            .flat_map(|m| m.get_label())
            .filter(|l| l.get_name() == "type")
            .map(|l| l.get_value())
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        assert_eq!(
            kinds,
            ["query_region", "query_region-failed", "tso", "tso-failed"]
        );
        assert!(families
            .iter()
            .any(|f| f.get_name() == "resource_manager_client_token_request_duration"));
    }
}
