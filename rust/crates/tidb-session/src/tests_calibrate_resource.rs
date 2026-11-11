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

//! `CALIBRATE RESOURCE` over a window of metrics, pinned by the statements,
//! the mocked metric rows and the expected quotas of Go's
//! `pkg/executor/internal/calibrateresource/calibrate_resource_test.go`. Its
//! hardware vectors are the executor's `calibrate_resource` tests.

use std::collections::HashMap;
use std::sync::MutexGuard;

use chrono::{DateTime, Datelike as _, Duration, Timelike as _, Utc};
use tidb_datatype::{CoreTime, Datum, Time, TimeType};
use tidb_executor::calibrate_resource::mock;

use crate::tests_support::row_text;
use crate::Session;

/// Go `types.MakeDatums`'s string.
fn s(text: &str) -> Datum {
    Datum::Bytes(text.as_bytes().to_vec())
}

/// Go `datetime(s)`: a `DATETIME(6)`.
fn dt(text: &str) -> Datum {
    let field = |range: std::ops::Range<usize>| text[range].parse::<u8>().unwrap();
    let microsecond = text.get(20..).map_or(0, |fraction| fraction.parse().unwrap());
    let time = CoreTime::from_date(
        text[0..4].parse().unwrap(),
        field(5..7),
        field(8..10),
        field(11..13),
        field(14..16),
        field(17..19),
        microsecond,
    );
    Datum::Time(Time::new(time, TimeType::DateTime, 6).unwrap())
}

/// Go `TestCalibrateResource`'s failpoints: `mockClusterInfo`,
/// `mockMetricsResponse`, `mockGOMAXPROCS`, `mockMetricsTableData` and
/// `mockMetricsDataFilter`.
struct CalibrateContext {
    session: Session,
    instances: Vec<&'static str>,
    mock_data: HashMap<String, Vec<Vec<Datum>>>,
    _mocks: MutexGuard<'static, ()>,
}

const METRICS_DATA: &str = "# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 49943
# HELP tikv_server_cpu_cores_quota Total CPU cores quota for TiKV server
# TYPE tikv_server_cpu_cores_quota gauge
tikv_server_cpu_cores_quota 8
# HELP tiflash_proxy_tikv_scheduler_write_flow The write flow passed through at scheduler level.
# TYPE tiflash_proxy_tikv_scheduler_write_flow gauge
tiflash_proxy_tikv_scheduler_write_flow 0
# HELP tiflash_proxy_tikv_server_cpu_cores_quota Total CPU cores quota for TiKV server
# TYPE tiflash_proxy_tikv_server_cpu_cores_quota gauge
tiflash_proxy_tikv_server_cpu_cores_quota 20
";

impl CalibrateContext {
    fn new() -> Self {
        let mocks = crate::tests_inspection_result::lock_mocks();
        let mut context = Self {
            session: Session::new(),
            instances: vec![
                "pd,127.0.0.1:32379,127.0.0.1:32380,mock-version,mock-githash,0",
                "tidb,127.0.0.1:34000,30080,mock-version,mock-githash,1001",
                "tikv,127.0.0.1:30160,30180,mock-version,mock-githash,0",
                "tikv,127.0.0.1:30161,30181,mock-version,mock-githash,0",
                "tikv,127.0.0.1:30162,30182,mock-version,mock-githash,0",
            ],
            mock_data: HashMap::new(),
            _mocks: mocks,
        };
        context.session.run("SET GLOBAL tidb_enable_resource_control='ON'").unwrap();
        mock::set_cluster_info(Some(context.instances.join(";")));
        mock::set_metrics_response(Some(METRICS_DATA.to_owned()));
        mock::set_gomaxprocs(Some(8));
        mock::set_metrics_data_filter(true);
        context.set_metrics();
        context
    }

    fn set_metrics(&self) {
        tidb_executor::metrics_reader::mock::set_metrics_table_data(Some(self.mock_data.clone()));
    }

    /// Go `mockData[table] = rows`.
    fn mock(&mut self, table: &str, rows: &[Vec<Datum>]) {
        self.mock_data.insert(table.to_owned(), rows.to_vec());
        self.set_metrics();
    }

    /// Go `delete(mockData, table)`.
    fn unmock(&mut self, table: &str) {
        self.mock_data.remove(table);
        self.set_metrics();
    }

    fn add_tiflash(&mut self) {
        self.instances.push("tiflash,127.0.0.1:3930,33940,mock-version,mock-githash,0");
        mock::set_cluster_info(Some(self.instances.join(";")));
    }

    /// Go `tk.MustQueryWithContext(ctx, sql).Check(testkit.Rows(quota))`.
    #[track_caller]
    fn query(&mut self, sql: &str, quota: &str) {
        assert_eq!(row_text(self.session.run(sql)), [[quota]], "{sql}");
    }

    /// Go `require.ErrorContains(t, err, message)` of the statement's `Next`.
    #[track_caller]
    fn error(&mut self, sql: &str, message: &str) {
        let error = self.session.run(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }

    /// Go `datetimeBeforeNow`'s `now`, in the session's time zone.
    fn now(&self) -> DateTime<tidb_datatype::SessionTimeZone> {
        Utc::now().with_timezone(&self.session.vars.session_time_zone())
    }
}

impl Drop for CalibrateContext {
    fn drop(&mut self) {
        tidb_executor::metrics_reader::mock::set_metrics_table_data(None);
        mock::set_cluster_info(None);
        mock::set_metrics_response(None);
        mock::set_gomaxprocs(None);
        mock::set_metrics_data_filter(false);
        tidb_vardef::ENABLE_RESOURCE_CONTROL.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Go `TestCalibrateResource`'s first statements: the feature switch, and a
/// cluster with no TiKV.
#[test]
fn calibrate_resource_needs_resource_control_and_a_cpu_quota_like_go() {
    let mut ctx = CalibrateContext::new();
    mock::set_cluster_info(None);
    ctx.session.run("SET GLOBAL tidb_enable_resource_control='OFF'").unwrap();
    ctx.error(
        "CALIBRATE RESOURCE",
        "Resource control feature is disabled. Run `SET GLOBAL tidb_enable_resource_control='on'` to enable the feature",
    );
    ctx.session.run("SET GLOBAL tidb_enable_resource_control='ON'").unwrap();
    // empty metrics error
    for sql in [
        "CALIBRATE RESOURCE",
        "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'",
        "CALIBRATE RESOURCE DURATION interval 2 minute",
    ] {
        ctx.error(sql, "There is no CPU quota metrics, no server with type 'tikv' is found");
    }
    // error sql
    assert!(ctx.session.run("CALIBRATE RESOURCE WORKLOAD tpcc START_TIME '2020-02-12 10:35:00'").is_err());
    // What a Go testkit answers the options it refuses.
    ctx.error("CALIBRATE RESOURCE DURATION '10s'", "unknown unit s");
    ctx.error("CALIBRATE RESOURCE END_TIME NULL", "invalid as of timestamp: as of timestamp cannot be NULL");
    ctx.error("CALIBRATE RESOURCE DURATION 5", "invalid single timeunit - ");
    let null = "invalid as of timestamp: as of timestamp cannot be NULL";
    ctx.error("CALIBRATE RESOURCE DURATION NULL", null);
    ctx.error("CALIBRATE RESOURCE START_TIME '2023-04-18 08:00:00' DURATION NULL", null);
    ctx.error("CALIBRATE RESOURCE END_TIME '2023-04-18 08:00:00' DURATION NULL", null);
    ctx.error(
        "CALIBRATE RESOURCE START_TIME 'abc' DURATION NULL",
        "invalid as of timestamp: cannot parse AS OF TIMESTAMP expression as datetime or TSO",
    );

    // The hardware estimate reads the controller's RU model: TPCC's 2869.5
    // RU per TiKV core over 3 TiKVs of 8 cores.
    mock::set_cluster_info(Some(ctx.instances.join(";")));
    mock::set_gomaxprocs(Some(40));
    ctx.query("CALIBRATE RESOURCE", "68868");
    ctx.query("CALIBRATE RESOURCE WORKLOAD TPCC", "68868");
    // No TiFlash has no logical core.
    ctx.query("CALIBRATE RESOURCE WORKLOAD TPCH_10", "0");
}

/// Go `TestCalibrateResource` from its first metric rows on.
#[test]
#[allow(clippy::too_many_lines)]
fn calibrate_resource_estimates_from_a_window_of_metrics_like_go() {
    let mut ctx = CalibrateContext::new();
    let now = ctx.now();
    let before = |seconds: i64| {
        let time = now - Duration::seconds(seconds);
        #[allow(clippy::cast_possible_truncation)]
        let core = CoreTime::from_date(
            time.year() as u16,
            time.month() as u8,
            time.day() as u8,
            time.hour() as u8,
            time.minute() as u8,
            time.second() as u8,
            time.nanosecond() / 1000,
        );
        Datum::Time(Time::new(core, TimeType::DateTime, 6).unwrap())
    };

    let ru1 = vec![
        vec![before(1240), Datum::Real(2250.0)],
        vec![before(1210), Datum::Real(2200.0)],
        vec![before(610), Datum::Real(2200.0)],
        vec![before(550), Datum::Real(2100.0)],
        vec![before(490), Datum::Real(2250.0)],
        vec![before(430), Datum::Real(2300.0)],
        vec![before(370), Datum::Real(2230.0)],
        vec![before(310), Datum::Real(2210.0)],
        vec![before(250), Datum::Real(2250.0)],
        vec![before(190), Datum::Real(2330.0)],
        vec![before(130), Datum::Real(2330.0)],
        vec![before(70), Datum::Real(2300.0)],
        vec![before(10), Datum::Real(2280.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru1);
    let cpu1 = vec![
        vec![before(1240), s("tidb-0"), s("tidb"), Datum::Real(1.234)],
        vec![before(1210), s("tidb-0"), s("tidb"), Datum::Real(1.212)],
        vec![before(610), s("tidb-0"), s("tidb"), Datum::Real(1.212)],
        vec![before(550), s("tidb-0"), s("tidb"), Datum::Real(1.233)],
        vec![before(490), s("tidb-0"), s("tidb"), Datum::Real(1.234)],
        vec![before(430), s("tidb-0"), s("tidb"), Datum::Real(1.213)],
        vec![before(370), s("tidb-0"), s("tidb"), Datum::Real(1.209)],
        vec![before(310), s("tidb-0"), s("tidb"), Datum::Real(1.213)],
        vec![before(250), s("tidb-0"), s("tidb"), Datum::Real(1.236)],
        vec![before(190), s("tidb-0"), s("tidb"), Datum::Real(1.228)],
        vec![before(130), s("tidb-0"), s("tidb"), Datum::Real(1.219)],
        vec![before(70), s("tidb-0"), s("tidb"), Datum::Real(1.220)],
        vec![before(10), s("tidb-0"), s("tidb"), Datum::Real(1.221)],
        vec![before(1240), s("tikv-1"), s("tikv"), Datum::Real(2.219)],
        vec![before(1210), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![before(610), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![before(550), s("tikv-1"), s("tikv"), Datum::Real(2.233)],
        vec![before(490), s("tikv-1"), s("tikv"), Datum::Real(2.234)],
        vec![before(430), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![before(370), s("tikv-1"), s("tikv"), Datum::Real(2.209)],
        vec![before(310), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![before(250), s("tikv-1"), s("tikv"), Datum::Real(2.236)],
        vec![before(190), s("tikv-1"), s("tikv"), Datum::Real(2.228)],
        vec![before(130), s("tikv-1"), s("tikv"), Datum::Real(2.219)],
        vec![before(70), s("tikv-1"), s("tikv"), Datum::Real(2.220)],
        vec![before(10), s("tikv-1"), s("tikv"), Datum::Real(2.281)],
        vec![before(1240), s("tikv-0"), s("tikv"), Datum::Real(2.280)],
        vec![before(1210), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![before(610), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![before(550), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![before(490), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![before(430), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![before(370), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![before(310), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![before(250), s("tikv-0"), s("tikv"), Datum::Real(2.286)],
        vec![before(190), s("tikv-0"), s("tikv"), Datum::Real(2.288)],
        vec![before(130), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![before(70), s("tikv-0"), s("tikv"), Datum::Real(2.280)],
        vec![before(10), s("tikv-0"), s("tikv"), Datum::Real(2.281)],
        vec![before(1240), s("tikv-2"), s("tikv"), Datum::Real(2.281)],
        vec![before(1210), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![before(610), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![before(550), s("tikv-2"), s("tikv"), Datum::Real(2.133)],
        vec![before(490), s("tikv-2"), s("tikv"), Datum::Real(2.134)],
        vec![before(430), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![before(370), s("tikv-2"), s("tikv"), Datum::Real(2.109)],
        vec![before(310), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![before(250), s("tikv-2"), s("tikv"), Datum::Real(2.136)],
        vec![before(190), s("tikv-2"), s("tikv"), Datum::Real(2.128)],
        vec![before(130), s("tikv-2"), s("tikv"), Datum::Real(2.119)],
        vec![before(70), s("tikv-2"), s("tikv"), Datum::Real(2.120)],
        vec![before(10), s("tikv-2"), s("tikv"), Datum::Real(2.281)],
    ];
    ctx.mock("process_cpu_usage", &cpu1);

    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE DURATION '11m'", "8161");
    ctx.query("CALIBRATE RESOURCE DURATION interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 11 minute END_TIME now()", "8161");
    ctx.query("CALIBRATE RESOURCE END_TIME now() START_TIME now() - interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 11 minute DURATION interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE DURATION interval 11 minute START_TIME now() - interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE END_TIME now() DURATION interval 11 minute", "8161");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 21 minute END_TIME now() - interval 1 minute", "8141");
    ctx.query("CALIBRATE RESOURCE DURATION '20m' START_TIME now() - interval 21 minute", "8141");
    ctx.query("CALIBRATE RESOURCE DURATION interval 20 minute START_TIME now() - interval 21 minute", "8141");

    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 21 minute END_TIME now() - interval 20 minute", "7978");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 4 minute", "8297");
    ctx.query("CALIBRATE RESOURCE DURATION interval 4 minute", "8297");
    ctx.query("CALIBRATE RESOURCE DURATION interval 4 minute END_TIME now()", "8297");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 8 minute", "8223");
    ctx.query("CALIBRATE RESOURCE DURATION interval 8 minute", "8223");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 8 minute END_TIME now() - interval 4 minute", "8147");
    ctx.query("CALIBRATE RESOURCE START_TIME now() - interval 8 minute DURATION interval 4 minute", "8147");
    ctx.query("CALIBRATE RESOURCE DURATION interval 4 minute START_TIME now() - interval 8 minute", "8147");
    ctx.query("CALIBRATE RESOURCE DURATION interval 4 minute END_TIME now() - interval 4 minute", "8147");
    ctx.query("CALIBRATE RESOURCE END_TIME date_sub(now(), interval 4 minute ) DURATION '4m'", "8147");

    // construct data for dynamic calibrate
    let ru1 = vec![
        vec![dt("2020-02-12 10:35:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:36:00"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:37:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:38:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:39:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:40:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:41:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:42:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:43:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:44:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:45:00"), Datum::Real(2280.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru1);

    let cpu1 = vec![
        vec![dt("2020-02-12 10:35:00"), s("tidb-0"), s("tidb"), Datum::Real(1.212)],
        vec![dt("2020-02-12 10:36:00"), s("tidb-0"), s("tidb"), Datum::Real(1.233)],
        vec![dt("2020-02-12 10:37:00"), s("tidb-0"), s("tidb"), Datum::Real(1.234)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(1.213)],
        vec![dt("2020-02-12 10:39:00"), s("tidb-0"), s("tidb"), Datum::Real(1.209)],
        vec![dt("2020-02-12 10:40:00"), s("tidb-0"), s("tidb"), Datum::Real(1.213)],
        vec![dt("2020-02-12 10:41:00"), s("tidb-0"), s("tidb"), Datum::Real(1.236)],
        vec![dt("2020-02-12 10:42:00"), s("tidb-0"), s("tidb"), Datum::Real(1.228)],
        vec![dt("2020-02-12 10:43:00"), s("tidb-0"), s("tidb"), Datum::Real(1.219)],
        vec![dt("2020-02-12 10:44:00"), s("tidb-0"), s("tidb"), Datum::Real(1.220)],
        vec![dt("2020-02-12 10:45:00"), s("tidb-0"), s("tidb"), Datum::Real(1.221)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-1"), s("tikv"), Datum::Real(2.233)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-1"), s("tikv"), Datum::Real(2.234)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-1"), s("tikv"), Datum::Real(2.209)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-1"), s("tikv"), Datum::Real(2.236)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-1"), s("tikv"), Datum::Real(2.228)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-1"), s("tikv"), Datum::Real(2.219)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-1"), s("tikv"), Datum::Real(2.220)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-1"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-0"), s("tikv"), Datum::Real(2.286)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-0"), s("tikv"), Datum::Real(2.288)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-0"), s("tikv"), Datum::Real(2.280)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-0"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-2"), s("tikv"), Datum::Real(2.133)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-2"), s("tikv"), Datum::Real(2.134)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-2"), s("tikv"), Datum::Real(2.109)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-2"), s("tikv"), Datum::Real(2.136)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-2"), s("tikv"), Datum::Real(2.128)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-2"), s("tikv"), Datum::Real(2.119)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-2"), s("tikv"), Datum::Real(2.120)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-2"), s("tikv"), Datum::Real(2.281)],
    ];
    ctx.mock("process_cpu_usage", &cpu1);

    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'", "8161");
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'", "8161");

    let cpu2 = vec![
        vec![dt("2020-02-12 10:35:00"), s("tidb-0"), s("tidb"), Datum::Real(3.212)],
        vec![dt("2020-02-12 10:36:00"), s("tidb-0"), s("tidb"), Datum::Real(3.233)],
        vec![dt("2020-02-12 10:37:00"), s("tidb-0"), s("tidb"), Datum::Real(3.234)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:39:00"), s("tidb-0"), s("tidb"), Datum::Real(3.209)],
        vec![dt("2020-02-12 10:40:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:41:00"), s("tidb-0"), s("tidb"), Datum::Real(3.236)],
        vec![dt("2020-02-12 10:42:00"), s("tidb-0"), s("tidb"), Datum::Real(3.228)],
        vec![dt("2020-02-12 10:43:00"), s("tidb-0"), s("tidb"), Datum::Real(3.219)],
        vec![dt("2020-02-12 10:44:00"), s("tidb-0"), s("tidb"), Datum::Real(3.220)],
        vec![dt("2020-02-12 10:45:00"), s("tidb-0"), s("tidb"), Datum::Real(3.221)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-1"), s("tikv"), Datum::Real(2.233)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-1"), s("tikv"), Datum::Real(2.234)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-1"), s("tikv"), Datum::Real(2.209)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-1"), s("tikv"), Datum::Real(2.236)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-1"), s("tikv"), Datum::Real(2.228)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-1"), s("tikv"), Datum::Real(2.219)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-1"), s("tikv"), Datum::Real(2.220)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-1"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-0"), s("tikv"), Datum::Real(2.286)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-0"), s("tikv"), Datum::Real(2.288)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-0"), s("tikv"), Datum::Real(2.280)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-0"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-2"), s("tikv"), Datum::Real(2.133)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-2"), s("tikv"), Datum::Real(2.134)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-2"), s("tikv"), Datum::Real(2.109)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-2"), s("tikv"), Datum::Real(2.136)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-2"), s("tikv"), Datum::Real(2.128)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-2"), s("tikv"), Datum::Real(2.119)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-2"), s("tikv"), Datum::Real(2.120)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-2"), s("tikv"), Datum::Real(2.281)],
    ];
    ctx.mock("process_cpu_usage", &cpu2);

    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'", "5616");
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'", "5616");
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'", "5616");
    ctx.query("CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' START_TIME '2020-02-12 10:35:00'", "5616");
    ctx.query("CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' DURATION '5m' START_TIME '2020-02-12 10:35:00' ", "5616");

    // Statistical time points do not correspond
    let ru_modify1 = vec![
        vec![dt("2020-02-12 10:25:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:26:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:27:00"), Datum::Real(4.0)],
        vec![dt("2020-02-12 10:28:00"), Datum::Real(6.0)],
        vec![dt("2020-02-12 10:29:00"), Datum::Real(3.0)],
        vec![dt("2020-02-12 10:30:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:31:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:32:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:33:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:34:00"), Datum::Real(8.0)],
        vec![dt("2020-02-12 10:35:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:36:00"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:37:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:38:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:39:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:40:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:41:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:42:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:43:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:44:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:45:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:46:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:47:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:48:00"), Datum::Real(8.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru_modify1);
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'", "5616");

    let ru_modify2 = vec![
        vec![dt("2020-02-12 10:25:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:26:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:27:00"), Datum::Real(4.0)],
        vec![dt("2020-02-12 10:28:00"), Datum::Real(6.0)],
        vec![dt("2020-02-12 10:29:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:30:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:31:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:32:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:33:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:34:00"), Datum::Real(8.0)],
        vec![dt("2020-02-12 10:35:00"), Datum::Real(29.0)],
        vec![dt("2020-02-12 10:36:00"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:37:00"), Datum::Real(49.0)],
        vec![dt("2020-02-12 10:38:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:39:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:40:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:41:00"), Datum::Real(47.0)],
        vec![dt("2020-02-12 10:42:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:43:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:44:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:45:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:47:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:49:00"), Datum::Real(2250.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru_modify2);
    let cpu2_mofidy = vec![
        vec![dt("2020-02-12 10:29:00"), s("tidb-0"), s("tidb"), Datum::Real(3.212)],
        vec![dt("2020-02-12 10:36:00"), s("tidb-0"), s("tidb"), Datum::Real(3.233)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:39:00"), s("tidb-0"), s("tidb"), Datum::Real(3.209)],
        vec![dt("2020-02-12 10:40:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:42:00"), s("tidb-0"), s("tidb"), Datum::Real(3.228)],
        vec![dt("2020-02-12 10:43:00"), s("tidb-0"), s("tidb"), Datum::Real(3.219)],
        vec![dt("2020-02-12 10:44:00"), s("tidb-0"), s("tidb"), Datum::Real(3.220)],
        vec![dt("2020-02-12 10:45:00"), s("tidb-0"), s("tidb"), Datum::Real(3.221)],
        vec![dt("2020-02-12 10:46:00"), s("tidb-0"), s("tidb"), Datum::Real(3.220)],
        vec![dt("2020-02-12 10:47:00"), s("tidb-0"), s("tidb"), Datum::Real(3.236)],
        vec![dt("2020-02-12 10:48:00"), s("tidb-0"), s("tidb"), Datum::Real(3.220)],
        vec![dt("2020-02-12 10:49:00"), s("tidb-0"), s("tidb"), Datum::Real(3.234)],
        vec![dt("2020-02-12 10:29:00"), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-1"), s("tikv"), Datum::Real(2.233)],
        vec![dt("2020-02-12 10:49:00"), s("tikv-1"), s("tikv"), Datum::Real(2.234)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-1"), s("tikv"), Datum::Real(2.209)],
        vec![dt("2020-02-12 10:46:00"), s("tikv-1"), s("tikv"), Datum::Real(3.220)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:47:00"), s("tikv-1"), s("tikv"), Datum::Real(2.236)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-1"), s("tikv"), Datum::Real(2.228)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-1"), s("tikv"), Datum::Real(2.219)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-1"), s("tikv"), Datum::Real(2.220)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-1"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:29:00"), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:49:00"), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:47:00"), s("tikv-0"), s("tikv"), Datum::Real(2.286)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-0"), s("tikv"), Datum::Real(2.288)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-0"), s("tikv"), Datum::Real(2.280)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-0"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:29:00"), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-2"), s("tikv"), Datum::Real(2.133)],
        vec![dt("2020-02-12 10:49:00"), s("tikv-2"), s("tikv"), Datum::Real(2.134)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-2"), s("tikv"), Datum::Real(2.109)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
        vec![dt("2020-02-12 10:47:00"), s("tikv-2"), s("tikv"), Datum::Real(2.136)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-2"), s("tikv"), Datum::Real(2.128)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-2"), s("tikv"), Datum::Real(2.119)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-2"), s("tikv"), Datum::Real(2.120)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-2"), s("tikv"), Datum::Real(2.281)],
        vec![dt("2020-02-12 10:48:00"), s("tikv-2"), s("tikv"), Datum::Real(3.220)],
    ];
    ctx.mock("process_cpu_usage", &cpu2_mofidy);
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'", "5631");

    let ru_modify3 = vec![
        vec![dt("2020-02-12 10:25:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:26:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:27:00"), Datum::Real(4.0)],
        vec![dt("2020-02-12 10:28:00"), Datum::Real(6.0)],
        vec![dt("2020-02-12 10:29:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:30:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:31:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:32:00"), Datum::Real(5.0)],
        vec![dt("2020-02-12 10:33:00"), Datum::Real(7.0)],
        vec![dt("2020-02-12 10:34:00"), Datum::Real(8.0)],
        vec![dt("2020-02-12 10:35:00"), Datum::Real(29.0)],
        vec![dt("2020-02-12 10:36:20"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:37:00"), Datum::Real(49.0)],
        vec![dt("2020-02-12 10:38:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:39:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:40:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:41:00"), Datum::Real(47.0)],
        vec![dt("2020-02-12 10:42:20"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:43:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:44:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:45:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:47:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:49:00"), Datum::Real(2250.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru_modify3);
    // because there are 20s difference in two time points, the result is changed.
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'", "5633");

    let ru2 = vec![
        vec![dt("2020-02-12 10:25:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:26:00"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:27:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:28:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:29:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:30:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:31:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:32:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:33:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:34:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:35:00"), Datum::Real(2280.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru2);
    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'", "The workload in selected time window is too low");

    let ru3 = vec![
        vec![dt("2020-02-12 10:25:00"), Datum::Real(2200.0)],
        vec![dt("2020-02-12 10:27:00"), Datum::Real(2100.0)],
        vec![dt("2020-02-12 10:28:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:30:00"), Datum::Real(2300.0)],
        vec![dt("2020-02-12 10:31:00"), Datum::Real(2230.0)],
        vec![dt("2020-02-12 10:33:00"), Datum::Real(2210.0)],
        vec![dt("2020-02-12 10:34:00"), Datum::Real(2250.0)],
        vec![dt("2020-02-12 10:36:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:37:00"), Datum::Real(2330.0)],
        vec![dt("2020-02-12 10:39:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:40:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:42:00"), Datum::Real(2280.0)],
        vec![dt("2020-02-12 10:43:00"), Datum::Real(2280.0)],
    ];
    ctx.mock("resource_manager_resource_unit", &ru3);
    let cpu3 = vec![
        vec![dt("2020-02-12 10:26:00"), s("tidb-0"), s("tidb"), Datum::Real(3.212)],
        vec![dt("2020-02-12 10:29:00"), s("tidb-0"), s("tidb"), Datum::Real(3.233)],
        vec![dt("2020-02-12 10:32:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:35:00"), s("tidb-0"), s("tidb"), Datum::Real(3.209)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:41:00"), s("tidb-0"), s("tidb"), Datum::Real(3.228)],
        vec![dt("2020-02-12 10:44:00"), s("tidb-0"), s("tidb"), Datum::Real(3.219)],

        vec![dt("2020-02-12 10:26:00"), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![dt("2020-02-12 10:29:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:32:00"), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.289)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-0"), s("tikv"), Datum::Real(2.286)],
    ];
    ctx.mock("process_cpu_usage", &cpu3);
    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'", "The workload in selected time window is too low");

    // flash back to init data.
    ctx.mock("resource_manager_resource_unit", &ru1);
    ctx.mock("process_cpu_usage", &cpu2);

    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00'", "the duration of calibration is too long");

    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '1m'", "5337");

    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:35:40'", "the duration of calibration is too short");

    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '25h'", "the duration of calibration is too long");

    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-13 10:46:00'", "the duration of calibration is too long");

    let rows = vec![
        vec![dt("2020-02-12 10:35:00"), s("tidb-0"), s("tidb"), Datum::Real(0.212)],
        vec![dt("2020-02-12 10:36:00"), s("tidb-0"), s("tidb"), Datum::Real(0.233)],
        vec![dt("2020-02-12 10:37:00"), s("tidb-0"), s("tidb"), Datum::Real(0.234)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:39:00"), s("tidb-0"), s("tidb"), Datum::Real(0.209)],
        vec![dt("2020-02-12 10:40:00"), s("tidb-0"), s("tidb"), Datum::Real(0.213)],
        vec![dt("2020-02-12 10:41:00"), s("tidb-0"), s("tidb"), Datum::Real(0.236)],
        vec![dt("2020-02-12 10:42:00"), s("tidb-0"), s("tidb"), Datum::Real(0.228)],
        vec![dt("2020-02-12 10:43:00"), s("tidb-0"), s("tidb"), Datum::Real(0.219)],
        vec![dt("2020-02-12 10:44:00"), s("tidb-0"), s("tidb"), Datum::Real(0.220)],
        vec![dt("2020-02-12 10:45:00"), s("tidb-0"), s("tidb"), Datum::Real(0.221)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-1"), s("tikv"), Datum::Real(0.233)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-1"), s("tikv"), Datum::Real(0.234)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-1"), s("tikv"), Datum::Real(0.213)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-1"), s("tikv"), Datum::Real(0.209)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-1"), s("tikv"), Datum::Real(0.236)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-1"), s("tikv"), Datum::Real(0.228)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-1"), s("tikv"), Datum::Real(0.219)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-1"), s("tikv"), Datum::Real(0.220)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-1"), s("tikv"), Datum::Real(0.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-0"), s("tikv"), Datum::Real(0.282)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-0"), s("tikv"), Datum::Real(0.283)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-0"), s("tikv"), Datum::Real(0.284)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-0"), s("tikv"), Datum::Real(0.289)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-0"), s("tikv"), Datum::Real(0.283)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-0"), s("tikv"), Datum::Real(0.286)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-0"), s("tikv"), Datum::Real(0.288)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-0"), s("tikv"), Datum::Real(0.289)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-0"), s("tikv"), Datum::Real(0.280)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-0"), s("tikv"), Datum::Real(0.281)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-2"), s("tikv"), Datum::Real(0.133)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-2"), s("tikv"), Datum::Real(0.134)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-2"), s("tikv"), Datum::Real(0.113)],
        vec![dt("2020-02-12 10:39:00"), s("tikv-2"), s("tikv"), Datum::Real(0.109)],
        vec![dt("2020-02-12 10:40:00"), s("tikv-2"), s("tikv"), Datum::Real(0.113)],
        vec![dt("2020-02-12 10:41:00"), s("tikv-2"), s("tikv"), Datum::Real(0.136)],
        vec![dt("2020-02-12 10:42:00"), s("tikv-2"), s("tikv"), Datum::Real(0.128)],
        vec![dt("2020-02-12 10:43:00"), s("tikv-2"), s("tikv"), Datum::Real(0.119)],
        vec![dt("2020-02-12 10:44:00"), s("tikv-2"), s("tikv"), Datum::Real(0.120)],
        vec![dt("2020-02-12 10:45:00"), s("tikv-2"), s("tikv"), Datum::Real(0.281)],
    ];
    ctx.mock("process_cpu_usage", &rows);

    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-13 10:35:01'", "The workload in selected time window is too low, with which TiDB is unable to reach a capacity estimation");

    let rows = vec![
        vec![dt("2020-02-12 10:35:00"), s("tidb-0"), s("tidb"), Datum::Real(3.212)],
        vec![dt("2020-02-12 10:36:00"), s("tidb-0"), s("tidb"), Datum::Real(3.233)],
        vec![dt("2020-02-12 10:37:00"), s("tidb-0"), s("tidb"), Datum::Real(3.234)],
        vec![dt("2020-02-12 10:38:00"), s("tidb-0"), s("tidb"), Datum::Real(3.213)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-1"), s("tikv"), Datum::Real(2.212)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-1"), s("tikv"), Datum::Real(2.233)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-1"), s("tikv"), Datum::Real(2.234)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-1"), s("tikv"), Datum::Real(2.213)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-0"), s("tikv"), Datum::Real(2.282)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-0"), s("tikv"), Datum::Real(2.284)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-0"), s("tikv"), Datum::Real(2.283)],
        vec![dt("2020-02-12 10:35:00"), s("tikv-2"), s("tikv"), Datum::Real(2.112)],
        vec![dt("2020-02-12 10:36:00"), s("tikv-2"), s("tikv"), Datum::Real(2.133)],
        vec![dt("2020-02-12 10:37:00"), s("tikv-2"), s("tikv"), Datum::Real(2.134)],
        vec![dt("2020-02-12 10:38:00"), s("tikv-2"), s("tikv"), Datum::Real(2.113)],
    ];
    ctx.mock("process_cpu_usage", &rows);
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'", "5492");

    // tiflash
    let rows = vec![
        vec![dt("2023-09-19 19:50:39.322000"), Datum::Real(465919.8102127319)],
        vec![dt("2023-09-19 19:51:39.322000"), Datum::Real(819764.9742611333)],
        vec![dt("2023-09-19 19:52:39.322000"), Datum::Real(520180.7089147462)],
        vec![dt("2023-09-19 19:53:39.322000"), Datum::Real(790496.4071700446)],
        vec![dt("2023-09-19 19:54:39.322000"), Datum::Real(545216.2174551424)],
        vec![dt("2023-09-19 19:55:39.322000"), Datum::Real(714332.5760632281)],
        vec![dt("2023-09-19 19:56:39.322000"), Datum::Real(577119.1037253677)],
        vec![dt("2023-09-19 19:57:39.322000"), Datum::Real(678005.0740038564)],
        vec![dt("2023-09-19 19:58:39.322000"), Datum::Real(592239.6784597588)],
        vec![dt("2023-09-19 19:59:39.322000"), Datum::Real(666552.6950822703)],
        vec![dt("2023-09-19 20:00:39.322000"), Datum::Real(689703.5663975218)],
    ];
    ctx.mock("resource_manager_resource_unit", &rows);

    let rows = vec![
        vec![dt("2023-09-19 19:50:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.10511111111111152)],
        vec![dt("2023-09-19 19:51:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.1293333333333332)],
        vec![dt("2023-09-19 19:52:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.11088888888888908)],
        vec![dt("2023-09-19 19:53:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.12333333333333357)],
        vec![dt("2023-09-19 19:54:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.1160000000000006)],
        vec![dt("2023-09-19 19:55:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.11888888888888813)],
        vec![dt("2023-09-19 19:56:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.1106666666666658)],
        vec![dt("2023-09-19 19:57:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.11311111111111055)],
        vec![dt("2023-09-19 19:58:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.11222222222222247)],
        vec![dt("2023-09-19 19:59:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.11488888888888923)],
        vec![dt("2023-09-19 20:00:39.324000"), s("127.0.0.1:10080"), s("tidb"), Datum::Real(0.12733333333333371)],
        vec![dt("2023-09-19 19:50:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:51:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.02222222222222222)],
        vec![dt("2023-09-19 19:52:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:53:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:54:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.08888888888888888)],
        vec![dt("2023-09-19 19:55:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:56:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:57:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:58:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 19:59:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
        vec![dt("2023-09-19 20:00:39.325000"), s("127.0.0.1:20180"), s("tikv"), Datum::Real(0.04444444444444444)],
    ];
    ctx.mock("process_cpu_usage", &rows);

    let rows = vec![
        vec![dt("2023-09-19 19:50:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:51:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:52:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:53:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2022-09-19 19:54:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:55:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:56:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:57:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:58:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 19:59:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
        vec![dt("2023-09-19 20:00:39.329000"), s("127.0.0.1:10080"), Datum::Real(20.0)],
    ];
    ctx.mock("tidb_server_maxprocs", &rows);

    // change mock for cluster info, add tiflash
    ctx.add_tiflash();

    ctx.error("CALIBRATE RESOURCE START_TIME '2023-09-19 19:50:39' DURATION '10m'", "The workload in selected time window is too low");

    let rows = vec![
        vec![dt("2023-09-19 19:50:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(18.577777777777776)],
        vec![dt("2023-09-19 19:51:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(17.666666666666668)],
        vec![dt("2023-09-19 19:52:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(18.339038812074868)],
        vec![dt("2023-09-19 19:53:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(17.82222222222222)],
        vec![dt("2023-09-19 19:54:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(18.177777777777774)],
        vec![dt("2023-09-19 19:55:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(17.911111111111108)],
        vec![dt("2023-09-19 19:56:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(17.177777777777774)],
        vec![dt("2023-09-19 19:57:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(16.17957550838982)],
        vec![dt("2023-09-19 19:58:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(16.844444444444445)],
        vec![dt("2023-09-19 19:59:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(17.71111111111111)],
        vec![dt("2023-09-19 20:00:39.327000"), s("127.0.0.1:20292"), s("tiflash"), Datum::Real(18.066666666666666)],
    ];
    ctx.mock("tiflash_process_cpu_usage", &rows);

    let rows = vec![
        vec![dt("2023-09-19 19:50:39.318000"), Datum::Real(487049.3164728853)],
        vec![dt("2023-09-19 19:51:39.318000"), Datum::Real(821600.8181867122)],
        vec![dt("2023-09-19 19:52:39.318000"), Datum::Real(507566.26041673025)],
        vec![dt("2023-09-19 19:53:39.318000"), Datum::Real(771038.8122556474)],
        vec![dt("2023-09-19 19:54:39.318000"), Datum::Real(529128.4530634031)],
        vec![dt("2023-09-19 19:55:39.318000"), Datum::Real(777912.9275530444)],
        vec![dt("2023-09-19 19:56:39.318000"), Datum::Real(557595.6206041124)],
        vec![dt("2023-09-19 19:57:39.318000"), Datum::Real(688658.1706168016)],
        vec![dt("2023-09-19 19:58:39.318000"), Datum::Real(556400.2766714202)],
        vec![dt("2023-09-19 19:59:39.318000"), Datum::Real(712467.4348424983)],
        vec![dt("2023-09-19 20:00:39.318000"), Datum::Real(659167.0340155548)],
    ];
    ctx.mock("tiflash_resource_manager_resource_unit", &rows);

    ctx.query("CALIBRATE RESOURCE START_TIME '2023-09-19 19:50:39' DURATION '10m'", "729439");

    ctx.unmock("process_cpu_usage");
    ctx.query("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'", "729439");

    ctx.unmock("tiflash_process_cpu_usage");
    ctx.error("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'", "query metric error: pd unavailable");
}
