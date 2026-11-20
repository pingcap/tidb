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

//! Go master `pkg/mvservice/service_helper_test.go`, its session-free cases
//! (the session-backed ones run against a node in `tidb-session`).

use crate::server_maintainer::ServerInfo;
use crate::service_helper::{latest_server_infos_by_instance, need_analyze_mlog, MLogStats};

/// Go `TestLatestServerInfosByInstanceKeepsNewestDDLID`.
#[test]
fn test_latest_server_infos_by_instance_keeps_newest_ddlid() {
    let server = |id: &str, ip: &str, start: i64| {
        (
            ServerInfo { id: id.to_owned(), ip: ip.to_owned(), port: 4000, start_timestamp: start },
            format!("{ip}:4000"),
        )
    };
    let got = latest_server_infos_by_instance(vec![
        server("old-node", "127.0.0.1", 100),
        server("new-node", "127.0.0.1", 200),
        server("peer-node", "127.0.0.2", 150),
    ]);
    assert_eq!(got.len(), 2);
    assert!(!got.contains_key("old-node"));
    assert_eq!(got["new-node"], server("new-node", "127.0.0.1", 200).0);
    assert_eq!(got["peer-node"], server("peer-node", "127.0.0.2", 150).0);
    // The same instances listed newest first keep the same answer.
    let got = latest_server_infos_by_instance(vec![
        server("new-node", "127.0.0.1", 200),
        server("old-node", "127.0.0.1", 100),
        server("peer-node", "127.0.0.2", 150),
    ]);
    assert_eq!(got.len(), 2);
    assert!(got.contains_key("new-node"));
}

/// Go `TestNeedAnalyzeMLog` (with Go's `AutoAnalyzeMinCnt` 1000).
#[test]
fn test_need_analyze_m_log() {
    const MIN: i64 = 1000;
    let analyzed = |realtime_count, modify_count| MLogStats {
        realtime_count,
        modify_count,
        analyzed: true,
        analyze_row_count: realtime_count as f64,
        ..MLogStats::default()
    };
    let unanalyzed = |realtime_count| MLogStats { realtime_count, ..MLogStats::default() };
    let cases: [(&str, Option<MLogStats>, f64, bool, &str); 7] = [
        ("nil stats", None, 0.5, false, ""),
        ("pseudo stats", Some(MLogStats { pseudo: true, ..unanalyzed(MIN + 1) }), 0.5, false, ""),
        ("below min count", Some(unanalyzed(MIN - 1)), 0.0, false, ""),
        ("unanalyzed reaches min count even when ratio disabled", Some(unanalyzed(MIN)), 0.0, true, "mlog unanalyzed"),
        ("analyzed modify trigger disabled by zero ratio", Some(analyzed(1000, 2000)), 0.0, false, ""),
        ("analyzed below ratio", Some(analyzed(1000, 200)), 0.5, false, ""),
        ("analyzed above ratio", Some(analyzed(1000, 600)), 0.5, true, "too many mlog modifications"),
    ];
    for (name, stats, ratio, need, reason) in cases {
        let (got_need, got_reason) = need_analyze_mlog(stats.as_ref(), ratio, MIN);
        assert_eq!(got_need, need, "{name}");
        assert!(got_reason.contains(reason), "{name}: {got_reason}");
        if !need {
            assert!(got_reason.is_empty(), "{name}");
        }
    }
}

/// Go `TestBuildResolvedMVRefreshAlertSQL`.
#[test]
fn test_build_resolved_mv_refresh_alert_sql() {
    use crate::service_helper::{
        build_clear_disabled_mv_refresh_alert_level_sql, build_clear_resolved_mv_refresh_alert_level_sql,
        build_delete_resolved_mv_refresh_alert_sql,
    };
    let now = crate::time_proxy::mvs_unix(1_700_000_000, 0);
    let ids = [103, 104];
    assert_eq!(
        build_delete_resolved_mv_refresh_alert_sql(&ids),
        "DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NULL"
    );
    assert_eq!(
        build_clear_resolved_mv_refresh_alert_level_sql(&now, &ids),
        format!(
            "UPDATE mysql.tidb_mview_refresh_alert SET ALERT_LEVEL = NULL, UPDATE_TIME = '{}' WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NOT NULL AND ALERT_LEVEL IS NOT NULL",
            now.naive_local().format("%Y-%m-%d %H:%M:%S")
        )
    );
    assert_eq!(
        build_clear_disabled_mv_refresh_alert_level_sql(),
        "UPDATE mysql.tidb_mview_refresh_alert AS a
JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
SET a.ALERT_LEVEL = NULL, a.UPDATE_TIME = NOW(6)
WHERE i.NEXT_REFRESH_UNIX_SECONDS IS NULL AND a.ALERT_LEVEL IS NOT NULL"
    );
}
