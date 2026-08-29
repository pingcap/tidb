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

//! Gap tests for Go `pkg/executor/analyze_utils_test.go` (the three tests this
//! tier cannot run): the pre-analyze FLUSH STATS_DELTA CLUSTER broadcast
//! helpers. Go's analyze flushes pending stats deltas cluster-wide before
//! building column tasks (`pkg/executor/analyze.go:101
//! flushStatsDeltaForAnalyze`), tolerating an older peer's rejection of the
//! BroadcastQuery executor during a rolling upgrade. This tier has no
//! multi-node broadcast surface: the helpers under test
//! (`isUnsupportedBroadcastQueryErr`, `collectStatsDeltaFlushObjectsForAnalyze`,
//! `canBroadcastToTiDBRPCForTest`) have no Rust counterpart, so each Go
//! contract is recorded here rather than approximated.

use crate::analyze::panic_recovery::{get_analyze_panic_error, AnalyzePanicValue};

/// Go `pkg/executor/analyze_utils_test.go:28::TestIsUnsupportedBroadcastQueryErr`:
/// `isUnsupportedBroadcastQueryErr` (`pkg/executor/analyze.go:165`) is true
/// only for errors containing both "exec type" and "doesn't support yet"
/// (the PBPlanBuilder rolling-upgrade rejection), false for nil, "context
/// canceled", and "region unavailable".
#[test]
#[ignore = "go-parity-gap: no BroadcastQuery broadcast surface; isUnsupportedBroadcastQueryErr (pkg/executor/analyze.go:165) has no Rust counterpart"]
fn is_unsupported_broadcast_query_err_distinguishes_rolling_upgrade_rejections() {
    // Cross-check the one adjacent behavior this tier DOES own while the
    // broadcast half stays ignored: a non-sentinel panic message still
    // propagates verbatim (see tests_analyze_panic_recovery_source).
    let err = get_analyze_panic_error(AnalyzePanicValue::Error(
        "this exec type 17 doesn't support yet".to_owned(),
    ));
    assert!(matches!(
        err,
        crate::analyze::panic_recovery::AnalyzePanicError::Propagated(_)
    ));
}
