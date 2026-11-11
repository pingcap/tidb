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

//! Go `pkg/executor/internal/calibrateresource`: the capacity estimation of
//! `CALIBRATE RESOURCE`, by hardware (`staticCalibrate`) and by the metrics
//! of a time window (`getTiDBQuota`, `getTiFlashQuota`). The statement's
//! session half (the option list, the metrics queries) is the session's.

use chrono::{DateTime, Duration, Utc};
use tidb_ast::CalibrateWorkload;
use tidb_domain::cluster_topology::{ClusterServer, ClusterTopology};
use tikv_client::pd_resource_group::controller::RUConfig;

/// What Go's failpoints of the calibration hand it.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use std::sync::{Mutex, PoisonError};

    static CLUSTER_INFO: Mutex<Option<String>> = Mutex::new(None);
    static METRICS_RESPONSE: Mutex<Option<String>> = Mutex::new(None);
    static GOMAXPROCS: Mutex<Option<usize>> = Mutex::new(None);
    static METRICS_DATA_FILTER: Mutex<bool> = Mutex::new(false);

    /// Go's `infoschema/mockClusterInfo` failpoint: the servers, each
    /// `type,address,statusAddr,version,gitHash,serverID`, joined by `;`.
    pub fn set_cluster_info(servers: Option<String>) {
        *CLUSTER_INFO.lock().unwrap_or_else(PoisonError::into_inner) = servers;
    }

    /// Go's `mockMetricsResponse` failpoint: the body every server's
    /// `/metrics` answers.
    pub fn set_metrics_response(body: Option<String>) {
        *METRICS_RESPONSE.lock().unwrap_or_else(PoisonError::into_inner) = body;
    }

    /// Go's `mockGOMAXPROCS` failpoint.
    pub fn set_gomaxprocs(procs: Option<usize>) {
        *GOMAXPROCS.lock().unwrap_or_else(PoisonError::into_inner) = procs;
    }

    /// Go's `mockMetricsDataFilter` failpoint.
    pub fn set_metrics_data_filter(enabled: bool) {
        *METRICS_DATA_FILTER.lock().unwrap_or_else(PoisonError::into_inner) = enabled;
    }

    pub(super) fn cluster_info() -> Option<String> {
        CLUSTER_INFO.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(super) fn metrics_response() -> Option<String> {
        METRICS_RESPONSE.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(super) fn gomaxprocs() -> Option<usize> {
        *GOMAXPROCS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn metrics_data_filter() -> bool {
        *METRICS_DATA_FILTER.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Go `baseResourceCost`: the resource cost rate of one kind of workload per
/// 1 tikv cpu.
struct BaseResourceCost {
    /// The average tidb cpu time per 1 tikv cpu time; the tidb cpu time is
    /// converted to the kv cpu time by this ratio.
    tidb_to_kv_cpu_ratio: f64,
    /// The kv cpu time for calculating RU; it is smaller than the actual cpu
    /// usage.
    kv_cpu: f64,
    /// The read bytes rate per 1 tikv cpu.
    read_bytes: u64,
    /// The write bytes rate per 1 tikv cpu.
    write_bytes: u64,
    /// The average tikv read requests per 1 tikv cpu.
    read_req_count: u64,
    /// The average tikv write requests per 1 tikv cpu.
    write_req_count: u64,
}

/// Go `units.MiB`.
const MIB: u64 = 1024 * 1024;

/// Go `workloadBaseRUCostMap`: the base data to estimate the RU capacity of
/// one tikv cpu, from benchmarks of each workload. `TPCH_10` is no entry;
/// `staticCalibrate` answers it before the lookup.
fn workload_base_ru_cost(workload: CalibrateWorkload) -> Option<BaseResourceCost> {
    match workload {
        CalibrateWorkload::Tpcc => Some(BaseResourceCost {
            tidb_to_kv_cpu_ratio: 0.6,
            kv_cpu: 0.15,
            read_bytes: MIB / 2,
            write_bytes: MIB,
            read_req_count: 300,
            write_req_count: 1750,
        }),
        CalibrateWorkload::OltpReadWrite => Some(BaseResourceCost {
            tidb_to_kv_cpu_ratio: 1.25,
            kv_cpu: 0.35,
            read_bytes: (MIB as f64 * 4.25) as u64,
            write_bytes: MIB / 3,
            read_req_count: 1600,
            write_req_count: 1400,
        }),
        CalibrateWorkload::OltpReadOnly => Some(BaseResourceCost {
            tidb_to_kv_cpu_ratio: 2.0,
            kv_cpu: 0.52,
            read_bytes: MIB * 28,
            write_bytes: 0,
            read_req_count: 4500,
            write_req_count: 0,
        }),
        CalibrateWorkload::OltpWriteOnly => Some(BaseResourceCost {
            tidb_to_kv_cpu_ratio: 1.0,
            kv_cpu: 0.0,
            read_bytes: 0,
            write_bytes: MIB,
            read_req_count: 0,
            write_req_count: 3550,
        }),
        CalibrateWorkload::Tpch10 => None,
    }
}

/// Go `valuableUsageThreshold`: a cpu usage above it is accepted alone.
const VALUABLE_USAGE_THRESHOLD: f64 = 0.2;
/// Go `lowUsageThreshold`: a cpu usage below it is too low to estimate from.
const LOW_USAGE_THRESHOLD: f64 = 0.1;
/// Go `discardRate`: the share of the highest and of the lowest estimates
/// left out of the mean.
const DISCARD_RATE: f64 = 0.1;

/// Go `errLowUsage`.
pub const ERR_LOW_USAGE: &str = "The workload in selected time window is too low, with which TiDB is unable to reach a capacity estimation; please select another time window with higher workload, or calibrate resource by hardware instead";

/// Go `errNoCPUQuotaMetrics.FastGenByArgs(err.Error())`.
#[must_use]
pub fn err_no_cpu_quota_metrics(cause: &str) -> String {
    format!("There is no CPU quota metrics, {cause}")
}

const SERVER_TYPE_TIDB: &str = "tidb";
const SERVER_TYPE_TIKV: &str = "tikv";
const SERVER_TYPE_TIFLASH: &str = "tiflash";

/// Go `infoschema.GetClusterServerInfo` as the calibration reads it: the
/// servers of the session's cluster discovery, none for a session without
/// one.
///
/// # Errors
/// The discovery's.
pub fn get_cluster_server_info(
    topology: Option<&ClusterTopology>,
    warnings: &mut Vec<String>,
) -> Result<Vec<ClusterServer>, String> {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(servers) = mock::cluster_info() {
        return Ok(servers
            .split(';')
            .map(|server| {
                let parts: Vec<&str> = server.split(',').collect();
                ClusterServer {
                    server_type: parts[0].to_owned(),
                    address: parts[1].to_owned(),
                    status_address: parts[2].to_owned(),
                    version: parts[3].to_owned(),
                    git_hash: parts[4].to_owned(),
                    start_timestamp: 0,
                    server_id: parts[5].parse().unwrap_or_default(),
                }
            })
            .collect());
    }
    topology.map_or_else(|| Ok(Vec::new()), |topology| topology.servers(warnings))
}

/// Go `staticCalibrate` after the controller's `GetConfig`: the capacity the
/// hardware gives `workload` (`TPCC` when the statement names none).
///
/// # Errors
/// No TiKV, or a server whose CPU quota cannot be read.
pub fn static_calibrate(
    ru_cfg: &RUConfig,
    workload: Option<CalibrateWorkload>,
    cluster_info: &[ClusterServer],
) -> Result<u64, String> {
    if workload == Some(CalibrateWorkload::Tpch10) {
        return static_calibrate_tpch10(cluster_info, ru_cfg);
    }

    let mut total_kv_cpu_quota =
        get_tikv_total_cpu_quota(cluster_info).map_err(|error| err_no_cpu_quota_metrics(&error))?;
    let total_tidb_cpu_quota = get_tidb_total_cpu_quota(cluster_info);

    // The default workload to calculate the RU capacity.
    let workload = workload.unwrap_or(CalibrateWorkload::Tpcc);
    let base_cost =
        workload_base_ru_cost(workload).ok_or_else(|| "unknown workload 'ast.CalibrateResourceType'".to_owned())?;

    if total_tidb_cpu_quota / base_cost.tidb_to_kv_cpu_ratio < total_kv_cpu_quota {
        total_kv_cpu_quota = total_tidb_cpu_quota / base_cost.tidb_to_kv_cpu_ratio;
    }
    let ru_per_kv_cpu = ru_cfg.read_base_cost * base_cost.read_req_count as f64
        + ru_cfg.cpu_ms_cost * base_cost.kv_cpu * 1000.0 // convert to ms
        + ru_cfg.read_bytes_cost * base_cost.read_bytes as f64
        + ru_cfg.write_base_cost * base_cost.write_req_count as f64
        + ru_cfg.write_bytes_cost * base_cost.write_bytes as f64;
    let quota = total_kv_cpu_quota * ru_per_kv_cpu;
    Ok(quota as u64)
}

/// Go `staticCalibrateTpch10`.
fn static_calibrate_tpch10(cluster_info: &[ClusterServer], ru_cfg: &RUConfig) -> Result<u64, String> {
    // Each TiFlash logical core's cpu time and read bytes per second.
    const CPU_TIME_PER_CPU_PER_SEC: f64 = 263.74;
    const READ_BYTES_PER_CPU_PER_SEC: f64 = 1_004_497_904.22;
    let ru_per_cpu =
        ru_cfg.cpu_ms_cost * CPU_TIME_PER_CPU_PER_SEC + ru_cfg.read_bytes_cost * READ_BYTES_PER_CPU_PER_SEC;
    let total_tiflash_logical_cores = get_tiflash_logical_cores(cluster_info)?;
    let quota = total_tiflash_logical_cores * ru_per_cpu;
    Ok(quota as u64)
}

/// Go `getTiDBTotalCPUQuota`: `GOMAXPROCS` of this node for every TiDB.
#[must_use]
pub fn get_tidb_total_cpu_quota(cluster_info: &[ClusterServer]) -> f64 {
    let cpu_quota = tidb_txnkv::rpc::go_max_procs();
    #[cfg(any(test, feature = "test-util"))]
    let cpu_quota = mock::gomaxprocs().unwrap_or(cpu_quota);
    let instance_num = count(cluster_info, SERVER_TYPE_TIDB);
    cpu_quota as f64 * instance_num as f64
}

/// Go `getTiKVTotalCPUQuota`.
///
/// # Errors
/// No TiKV, or one whose CPU quota cannot be read.
pub fn get_tikv_total_cpu_quota(cluster_info: &[ClusterServer]) -> Result<f64, String> {
    let instance_num = count(cluster_info, SERVER_TYPE_TIKV);
    if instance_num == 0 {
        return Err("no server with type 'tikv' is found".to_owned());
    }
    let cpu_quota = fetch_server_cpu_quota(cluster_info, SERVER_TYPE_TIKV, "tikv_server_cpu_cores_quota")?;
    Ok(cpu_quota * instance_num as f64)
}

/// Go `getTiFlashLogicalCores`.
///
/// # Errors
/// A TiFlash whose CPU quota cannot be read.
pub fn get_tiflash_logical_cores(cluster_info: &[ClusterServer]) -> Result<f64, String> {
    let instance_num = count(cluster_info, SERVER_TYPE_TIFLASH);
    if instance_num == 0 {
        return Ok(0.0);
    }
    let cpu_quota =
        fetch_server_cpu_quota(cluster_info, SERVER_TYPE_TIFLASH, "tiflash_proxy_tikv_server_cpu_cores_quota")?;
    Ok(cpu_quota * instance_num as f64)
}

/// Go `count`.
fn count(cluster_info: &[ClusterServer], ty: &str) -> usize {
    cluster_info.iter().filter(|server| server.server_type == ty).count()
}

/// Go `timePointValue`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TimePointValue {
    pub tp: DateTime<Utc>,
    pub val: f64,
}

/// Go `timeSeriesValues`.
#[derive(Debug, Default)]
pub struct TimeSeriesValues {
    pub vals: Vec<TimePointValue>,
    idx: usize,
}

impl TimeSeriesValues {
    #[must_use]
    pub fn new(vals: Vec<TimePointValue>) -> Self {
        Self { vals, idx: 0 }
    }

    fn is_end(&self) -> bool {
        self.idx >= self.vals.len()
    }

    fn next(&mut self) {
        self.idx += 1;
    }

    fn get_time(&self) -> DateTime<Utc> {
        self.vals[self.idx].tp
    }

    fn get_value(&self) -> f64 {
        self.vals[self.idx].val
    }

    /// Go `advance`: moves to the first point within 10 seconds of `target`
    /// or after it, and says whether it is within.
    fn advance(&mut self, target: DateTime<Utc>) -> bool {
        while self.idx < self.vals.len() {
            let tp = self.vals[self.idx].tp;
            if tp + Duration::seconds(10) > target {
                return tp - Duration::seconds(10) < target;
            }
            self.idx += 1;
        }
        false
    }

    /// Go's `mockMetricsDataFilter` failpoint: the points of the window.
    #[cfg(any(test, feature = "test-util"))]
    fn filter(&mut self, start_ts: DateTime<Utc>, end_ts: DateTime<Utc>) {
        if mock::metrics_data_filter() {
            self.vals.retain(|point| !(point.tp > end_ts || point.tp < start_ts));
        }
    }
}

/// Go's builtin `max` over two floats: a NaN operand is the answer.
fn go_max(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        return f64::NAN;
    }
    left.max(right)
}

/// Go `getTiDBQuota` over the series its three queries read: the RU the
/// window's workload would reach at full CPU.
///
/// # Errors
/// [`ERR_LOW_USAGE`].
pub fn get_tidb_quota(
    mut rus: TimeSeriesValues,
    mut tikv_cpus: TimeSeriesValues,
    mut tidb_cpus: TimeSeriesValues,
    total_kv_cpu_quota: f64,
    total_tidb_cpu: f64,
    start_ts: DateTime<Utc>,
    end_ts: DateTime<Utc>,
) -> Result<f64, String> {
    #[cfg(any(test, feature = "test-util"))]
    {
        tikv_cpus.filter(start_ts, end_ts);
        tidb_cpus.filter(start_ts, end_ts);
        rus.filter(start_ts, end_ts);
    }
    #[cfg(not(any(test, feature = "test-util")))]
    let _ = (start_ts, end_ts);
    let mut quotas = Vec::new();
    loop {
        if rus.is_end() || tikv_cpus.is_end() || tidb_cpus.is_end() {
            break;
        }
        // make time point match
        let max_time = rus.get_time().max(tikv_cpus.get_time()).max(tidb_cpus.get_time());
        if !rus.advance(max_time) || !tikv_cpus.advance(max_time) || !tidb_cpus.advance(max_time) {
            continue;
        }
        let (tikv_quota, tidb_quota) =
            (tikv_cpus.get_value() / total_kv_cpu_quota, tidb_cpus.get_value() / total_tidb_cpu);
        // If one of the two cpu usage is greater than the `valuableUsageThreshold`, we can accept it.
        // And if both are greater than the `lowUsageThreshold`, we can also accept it.
        let valuable = tikv_quota > VALUABLE_USAGE_THRESHOLD || tidb_quota > VALUABLE_USAGE_THRESHOLD;
        let low = tikv_quota < LOW_USAGE_THRESHOLD || tidb_quota < LOW_USAGE_THRESHOLD;
        if valuable || !low {
            quotas.push(rus.get_value() / go_max(tikv_quota, tidb_quota));
        }
        rus.next();
        tidb_cpus.next();
        tikv_cpus.next();
    }
    setup_quotas(quotas)
}

/// Go `getTiFlashQuota` over the series its two queries read.
///
/// # Errors
/// [`ERR_LOW_USAGE`].
pub fn get_tiflash_quota(
    mut tiflash_rus: TimeSeriesValues,
    mut tiflash_cpus: TimeSeriesValues,
    total_tiflash_logical_cores: f64,
) -> Result<f64, String> {
    let mut quotas = Vec::new();
    loop {
        if tiflash_rus.is_end() || tiflash_cpus.is_end() {
            break;
        }
        // make time point match
        let max_time = tiflash_rus.get_time().max(tiflash_cpus.get_time());
        if !tiflash_rus.advance(max_time) || !tiflash_cpus.advance(max_time) {
            continue;
        }
        let tiflash_quota = tiflash_cpus.get_value() / total_tiflash_logical_cores;
        if tiflash_quota > LOW_USAGE_THRESHOLD {
            quotas.push(tiflash_rus.get_value() / tiflash_quota);
        }
        tiflash_rus.next();
        tiflash_cpus.next();
    }
    setup_quotas(quotas)
}

/// Go `setupQuotas`: the mean of the estimates without the highest and the
/// lowest [`DISCARD_RATE`] of them.
fn setup_quotas(mut quotas: Vec<f64>) -> Result<f64, String> {
    if quotas.len() < 2 {
        return Err(ERR_LOW_USAGE.to_owned());
    }
    quotas.sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
    let lower_bound = (quotas.len() as f64 * DISCARD_RATE).round() as usize;
    let upper_bound = quotas.len() - lower_bound;
    let sum: f64 = quotas[lower_bound..upper_bound].iter().sum();
    Ok(sum / (upper_bound - lower_bound) as f64)
}

/// Go `fetchServerCPUQuota`: `metric_name`'s value in the `/metrics` of the
/// first reachable server of `server_type`.
fn fetch_server_cpu_quota(
    server_infos: &[ClusterServer],
    server_type: &str,
    metric_name: &str,
) -> Result<f64, String> {
    fetch_store_metrics(server_infos, server_type, |addr, response| {
        let body = match response {
            MetricsResponse::Ok(body) => body,
            MetricsResponse::Status(status) => return Err(format!("request {addr} failed: {status}")),
        };
        for line in body.lines() {
            if !line.starts_with(metric_name) {
                continue;
            }
            let value = line.get(metric_name.len() + 1..).unwrap_or_default();
            return tidb_goruntime::go_strconv::parse_float64(value).map_err(|error| {
                let cause = match error {
                    tidb_goruntime::go_strconv::ParseFloatError::Syntax => "invalid syntax",
                    tidb_goruntime::go_strconv::ParseFloatError::Range => "value out of range",
                };
                format!("strconv.ParseFloat: parsing {value:?}: {cause}")
            });
        }
        Err(format!("metrics '{metric_name}' not found from server '{addr}'"))
    })
}

/// What a server's `/metrics` answered: the body of a 200, else the status
/// line (Go `resp.Status`).
enum MetricsResponse {
    Ok(String),
    Status(String),
}

/// Go `fetchStoreMetrics`: `on_resp` over the `/metrics` answer of the first
/// server of `server_type` that answers at all.
fn fetch_store_metrics(
    servers_info: &[ClusterServer],
    server_type: &str,
    on_resp: impl Fn(&str, MetricsResponse) -> Result<f64, String>,
) -> Result<f64, String> {
    let mut first_err = None;
    for srv in servers_info {
        if srv.server_type != server_type {
            continue;
        }
        if srv.status_address.is_empty() {
            continue;
        }
        #[cfg(any(test, feature = "test-util"))]
        if let Some(data) = mock::metrics_response() {
            return on_resp(&srv.address, MetricsResponse::Ok(data));
        }
        let url = format!(
            "{}://{}/metrics",
            crate::memtable_retrievers::internal_http_schema(),
            srv.status_address
        );
        match internal_http_get(&url) {
            Ok(response) => return on_resp(&srv.address, response),
            Err(error) => {
                first_err.get_or_insert(error);
            }
        }
    }
    Err(first_err.unwrap_or_else(|| format!("no server with type '{server_type}' is found")))
}

/// Go `util.InternalHTTPClient().Do(req)`: the client of the global
/// config's cluster security.
fn internal_http_get(url: &str) -> Result<MetricsResponse, String> {
    let security = &tidb_config::config_tree::config::get_global_config().security;
    let security = tidb_pd_client::ClusterSecurity::new(
        security.cluster_ssl_ca.clone(),
        security.cluster_ssl_cert.clone(),
        security.cluster_ssl_key.clone(),
        Vec::new(),
    );
    let response = tidb_store_driver_txn::cluster_http::internal_http_client(&security)?
        .get(url)
        .send()
        .map_err(|error| error.to_string())?;
    if response.status() != reqwest::StatusCode::OK {
        return Ok(MetricsResponse::Status(response.status().to_string()));
    }
    response.text().map(MetricsResponse::Ok).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, PoisonError};
    use tikv_client::pd_resource_group::controller::{default_config, generate_ru_config};

    /// The mocks are the process's: one calibration at a time.
    static MOCKS: Mutex<()> = Mutex::new(());

    /// Go `TestCalibrateResource`'s failpoints: the cluster, every server's
    /// `/metrics`, and `GOMAXPROCS`.
    fn mocks(instances: &[&str], gomaxprocs: usize) -> MutexGuard<'static, ()> {
        let guard = MOCKS.lock().unwrap_or_else(PoisonError::into_inner);
        mock::set_cluster_info(Some(instances.join(";")));
        mock::set_metrics_response(Some(METRICS_DATA.to_owned()));
        mock::set_gomaxprocs(Some(gomaxprocs));
        guard
    }

    fn clear() {
        mock::set_cluster_info(None);
        mock::set_metrics_response(None);
        mock::set_gomaxprocs(None);
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

    const INSTANCES: [&str; 5] = [
        "pd,127.0.0.1:32379,127.0.0.1:32380,mock-version,mock-githash,0",
        "tidb,127.0.0.1:34000,30080,mock-version,mock-githash,1001",
        "tikv,127.0.0.1:30160,30180,mock-version,mock-githash,0",
        "tikv,127.0.0.1:30161,30181,mock-version,mock-githash,0",
        "tikv,127.0.0.1:30162,30182,mock-version,mock-githash,0",
    ];

    /// Go `TestCalibrateResource`'s `oldCfg`: the RU model before 7.5.
    fn old_ru_config() -> RUConfig {
        let mut config = default_config();
        let unit = &mut config.base_config.request_unit;
        unit.read_base_cost = 0.25;
        unit.read_cost_per_byte = 0.000_015_258_789_062_5;
        unit.write_base_cost = 1.0;
        unit.write_cost_per_byte = 0.000_976_562_5;
        unit.cpu_ms_cost = 0.333_333_333_333_333_3;
        generate_ru_config(&config)
    }

    fn servers() -> Vec<ClusterServer> {
        get_cluster_server_info(None, &mut Vec::new()).unwrap()
    }

    #[test]
    fn the_hardware_capacity_of_each_workload_is_gos() {
        let _mocks = mocks(&INSTANCES, 40);
        let config = old_ru_config();
        let quota = |workload| static_calibrate(&config, workload, &servers());
        assert_eq!(quota(None), Ok(69768));
        assert_eq!(quota(Some(CalibrateWorkload::Tpcc)), Ok(69768));
        assert_eq!(quota(Some(CalibrateWorkload::OltpReadWrite)), Ok(55823));
        assert_eq!(quota(Some(CalibrateWorkload::OltpReadOnly)), Ok(34926));
        assert_eq!(quota(Some(CalibrateWorkload::OltpWriteOnly)), Ok(109_776));
        // No TiFlash has no logical core.
        assert_eq!(quota(Some(CalibrateWorkload::Tpch10)), Ok(0));

        // change total tidb cpu to less than tikv_cpu_quota
        mock::set_gomaxprocs(Some(8));
        assert_eq!(quota(None), Ok(38760));
        clear();
    }

    #[test]
    fn tpch10_is_the_tiflash_cores_capacity_like_go() {
        let mut instances = INSTANCES.to_vec();
        instances.push("tiflash,127.0.0.1:3930,33940,mock-version,mock-githash,0");
        let _mocks = mocks(&instances, 40);
        let config = old_ru_config();
        // Go: uint64(20 * (CPUMsCost*263.74 + ReadBytesCost*1004497904.22)).
        let ru_per_cpu = 0.333_333_333_333_333_3 * 263.74 + 0.000_015_258_789_062_5 * 1_004_497_904.22;
        assert_eq!(
            static_calibrate(&config, Some(CalibrateWorkload::Tpch10), &servers()),
            Ok((20.0 * ru_per_cpu) as u64)
        );
        assert_eq!(static_calibrate(&config, Some(CalibrateWorkload::Tpch10), &servers()), Ok(308_306));
        clear();
    }

    #[test]
    fn a_cluster_without_a_cpu_quota_is_refused_like_go() {
        let _mocks = mocks(&INSTANCES[..2], 40);
        let config = old_ru_config();
        assert_eq!(
            static_calibrate(&config, None, &servers()),
            Err("There is no CPU quota metrics, no server with type 'tikv' is found".to_owned())
        );
        mock::set_cluster_info(Some(INSTANCES.join(";")));
        mock::set_metrics_response(Some("process_cpu_seconds_total 49943\n".to_owned()));
        assert_eq!(
            static_calibrate(&config, None, &servers()),
            Err("There is no CPU quota metrics, metrics 'tikv_server_cpu_cores_quota' not found from server '127.0.0.1:30160'".to_owned())
        );
        mock::set_metrics_response(Some("tikv_server_cpu_cores_quota eight\n".to_owned()));
        assert_eq!(
            static_calibrate(&config, None, &servers()),
            Err("There is no CPU quota metrics, strconv.ParseFloat: parsing \"eight\": invalid syntax".to_owned())
        );
        clear();
    }

    #[test]
    fn an_unreachable_server_is_the_first_error_like_go() {
        let _mocks = mocks(&INSTANCES, 40);
        mock::set_metrics_response(None);
        // A server without a status address is skipped; none answers.
        mock::set_cluster_info(Some("tikv,127.0.0.1:30160,,mock-version,mock-githash,0".to_owned()));
        assert_eq!(
            get_tikv_total_cpu_quota(&servers()),
            Err("no server with type 'tikv' is found".to_owned())
        );
        // A refused connection is remembered, and the next server answers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live = listener.local_addr().unwrap();
        let dead = {
            let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            closed.local_addr().unwrap()
        };
        std::thread::spawn(move || {
            use std::io::{Read as _, Write as _};
            for body in ["tikv_server_cpu_cores_quota 6\n", ""] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                assert!(request[..read].starts_with(b"GET /metrics HTTP/1.1\r\n"));
                let status = if body.is_empty() { "404 Not Found" } else { "200 OK" };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        mock::set_cluster_info(Some(format!(
            "tikv,127.0.0.1:1,{dead},v,h,0;tikv,127.0.0.1:2,{live},v,h,0"
        )));
        assert_eq!(get_tikv_total_cpu_quota(&servers()), Ok(12.0));
        assert_eq!(
            get_tikv_total_cpu_quota(&servers()),
            Err("request 127.0.0.1:2 failed: 404 Not Found".to_owned())
        );
        mock::set_cluster_info(Some(format!("tikv,127.0.0.1:1,{dead},v,h,0")));
        let error = get_tikv_total_cpu_quota(&servers()).unwrap_err();
        assert!(error.contains(&dead.to_string()), "{error}");
        clear();
    }

    fn series(points: &[(i64, f64)]) -> TimeSeriesValues {
        TimeSeriesValues::new(
            points
                .iter()
                .map(|(seconds, val)| TimePointValue {
                    tp: DateTime::from_timestamp(*seconds, 0).unwrap(),
                    val: *val,
                })
                .collect(),
        )
    }

    #[test]
    fn advance_stops_at_the_point_within_ten_seconds_like_go() {
        let at = |seconds| DateTime::from_timestamp(seconds, 0).unwrap();
        let mut values = series(&[(0, 1.0), (60, 2.0), (120, 3.0)]);
        // 51s is past the first point's reach and before the second's.
        assert!(!values.advance(at(50)));
        assert_eq!(values.get_value(), 2.0);
        assert!(values.advance(at(51)));
        assert!(values.advance(at(69)));
        assert_eq!(values.get_value(), 2.0);
        assert!(!values.advance(at(70)));
        assert_eq!(values.get_value(), 3.0);
        assert!(!values.advance(at(130)));
        assert!(values.is_end());
    }

    #[test]
    fn the_mean_leaves_out_a_tenth_at_each_end_like_go() {
        assert_eq!(setup_quotas(vec![1.0]), Err(ERR_LOW_USAGE.to_owned()));
        assert_eq!(setup_quotas(vec![1.0, 4.0]), Ok(2.5));
        // Five estimates: round(0.5) is 1 at each end.
        assert_eq!(setup_quotas(vec![5.0, 1.0, 100.0, 3.0, 4.0]), Ok(4.0));
        // Four: round(0.4) is none.
        assert_eq!(setup_quotas(vec![1.0, 2.0, 3.0, 6.0]), Ok(3.0));
    }

    #[test]
    fn a_window_of_low_usage_is_no_estimate_like_go() {
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let end = DateTime::from_timestamp(600, 0).unwrap();
        let quota = |tikv: f64, tidb: f64| {
            get_tidb_quota(
                series(&[(0, 100.0), (60, 200.0)]),
                series(&[(0, tikv), (60, tikv)]),
                series(&[(0, tidb), (60, tidb)]),
                10.0,
                10.0,
                start,
                end,
            )
        };
        // One usage above 0.2 is accepted whatever the other.
        assert_eq!(quota(2.5, 0.1), Ok((100.0 / 0.25 + 200.0 / 0.25) / 2.0));
        // Both between 0.1 and 0.2 are accepted.
        assert_eq!(quota(1.5, 1.0), Ok((100.0 / 0.15 + 200.0 / 0.15) / 2.0));
        // One below 0.1 with none above 0.2 is too low.
        assert_eq!(quota(1.5, 0.5), Err(ERR_LOW_USAGE.to_owned()));
        // TiFlash accepts a usage above 0.1 only.
        let tiflash = |cpu: f64| {
            get_tiflash_quota(series(&[(0, 100.0), (60, 200.0)]), series(&[(0, cpu), (60, cpu)]), 10.0)
        };
        assert_eq!(tiflash(1.0), Err(ERR_LOW_USAGE.to_owned()));
        assert_eq!(tiflash(2.0), Ok(750.0));
    }
}
