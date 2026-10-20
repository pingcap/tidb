// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `model.go`: the request/response accounting interfaces and the KV and
//! SQL resource calculators.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use super::config::{RUConfig, DEFAULT_AVG_BATCH_PROPORTION};
use crate::proto::resource_manager::Consumption;

/// The basic unit of resource request management: RRU (read request unit)
/// or WRU (write request unit).
pub type RequestUnit = f64;

/// The network traffic type of the instance a request is sent to.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum AccessLocationType {
    /// Neither the source nor the target instance has a "zone" label; the
    /// cluster is likely not deployed across availability zones.
    #[default]
    AccessUnknown = 0,
    /// The source and target instance are in the same zone.
    AccessLocalZone = 1,
    /// The source and target instance are in different zones.
    AccessCrossZone = 2,
}

impl From<crate::kv::AccessLocationType> for AccessLocationType {
    fn from(location: crate::kv::AccessLocationType) -> Self {
        match location {
            crate::kv::AccessLocationType::LocalZone => AccessLocationType::AccessLocalZone,
            crate::kv::AccessLocationType::CrossZone => AccessLocationType::AccessCrossZone,
            crate::kv::AccessLocationType::Unknown | crate::kv::AccessLocationType::Other(_) => {
                AccessLocationType::AccessUnknown
            }
        }
    }
}

/// The request information provider. A request tells whether it is a write
/// and, if so, how many bytes it writes.
///
/// Go's optional `RequestSource`, `PredictedReadBytes` and `IsCop` provider
/// interfaces are the defaulted methods: a provider that does not implement
/// them reports an empty request source, no prediction hint and a non-cop
/// request, exactly as Go treats a missing optional interface.
pub trait RequestInfo: Send + Sync {
    fn is_write(&self) -> bool;
    fn write_bytes(&self) -> u64;
    fn replica_number(&self) -> i64;
    fn store_id(&self) -> u64;
    fn request_size(&self) -> u64;
    fn access_location_type(&self) -> AccessLocationType;
    /// The source of the request for RU attribution.
    fn request_source(&self) -> &str {
        ""
    }
    /// The caller-supplied read-bytes hint. Only coprocessor reads use it.
    fn predicted_read_bytes(&self) -> u64 {
        0
    }
    /// Whether this request targets the coprocessor endpoint (CmdCop /
    /// CmdCopStream). Only coprocessor reads take part in paging pre-charge.
    fn is_cop(&self) -> bool {
        false
    }
}

/// The response information provider: bytes read and KV CPU cost.
pub trait ResponseInfo: Send + Sync {
    fn read_bytes(&self) -> u64;
    fn kv_cpu(&self) -> Duration;
    /// Whether the request returned successfully. If not, the WRU cost of the
    /// request is paid back.
    fn succeed(&self) -> bool;
    fn response_size(&self) -> u64;
}

impl RequestInfo for crate::resource_control::RequestInfo {
    fn is_write(&self) -> bool {
        self.write_bytes.is_some()
    }
    fn write_bytes(&self) -> u64 {
        self.write_bytes.unwrap_or_default()
    }
    fn replica_number(&self) -> i64 {
        self.replica_number
    }
    fn store_id(&self) -> u64 {
        self.store_id
    }
    fn request_size(&self) -> u64 {
        self.request_size
    }
    fn access_location_type(&self) -> AccessLocationType {
        self.access_location.into()
    }
    fn predicted_read_bytes(&self) -> u64 {
        self.predicted_read_bytes
    }
    fn is_cop(&self) -> bool {
        self.is_coprocessor
    }
}

impl ResponseInfo for crate::resource_control::ResponseInfo {
    fn read_bytes(&self) -> u64 {
        self.read_bytes
    }
    fn kv_cpu(&self) -> Duration {
        self.kv_cpu
    }
    fn succeed(&self) -> bool {
        true
    }
    fn response_size(&self) -> u64 {
        self.response_size
    }
}

pub(crate) fn predicted_read_bytes(req: &dyn RequestInfo) -> u64 {
    req.predicted_read_bytes()
}

pub(crate) fn is_cop_request(req: &dyn RequestInfo) -> bool {
    req.is_cop()
}

pub(crate) fn request_source(req: &dyn RequestInfo) -> &str {
    req.request_source()
}

/// The predicted read-bytes hint for coprocessor reads. The boolean reports
/// whether the request takes part in cop-read pre-charge accounting at all;
/// `(0, true)` is a cop read without a usable hint.
pub(crate) fn paging_read_estimate(req: &dyn RequestInfo) -> (u64, bool) {
    if req.is_write() || !is_cop_request(req) {
        return (0, false);
    }
    (predicted_read_bytes(req), true)
}

/// The predicted read-bytes hint for coprocessor reads; writes and non-cop
/// reads always answer 0.
pub(crate) fn estimated_read_bytes(req: &dyn RequestInfo) -> u64 {
    let (bytes_for_est, ok) = paging_read_estimate(req);
    if !ok {
        return 0;
    }
    bytes_for_est
}

/// Calculates the resource consumption of a request.
pub trait ResourceCalculator: Send + Sync {
    /// Periodic consumption off the request path, such as SQL CPU cost.
    fn trickle(&self, consumption: &mut Consumption);
    /// Consumption before the KV request: base and write request cost.
    fn before_kv_request(&self, consumption: &mut Consumption, req: &dyn RequestInfo);
    /// Consumption after the KV request: read request and KV CPU cost.
    fn after_kv_request(
        &self,
        consumption: &mut Consumption,
        req: &dyn RequestInfo,
        res: &dyn ResponseInfo,
    );
    /// Go's `calc.(*KVCalculator)` type assertion.
    fn as_kv_calculator(&self) -> Option<&KVCalculator> {
        None
    }
}

/// Calculates the KV-side consumption.
#[derive(Clone, Debug)]
pub struct KVCalculator {
    pub ru_config: Arc<RUConfig>,
}

pub(crate) fn new_kv_calculator(cfg: Arc<RUConfig>) -> KVCalculator {
    KVCalculator { ru_config: cfg }
}

impl ResourceCalculator for KVCalculator {
    fn trickle(&self, _consumption: &mut Consumption) {}

    fn before_kv_request(&self, consumption: &mut Consumption, req: &dyn RequestInfo) {
        let cfg = &self.ru_config;
        if req.is_write() {
            consumption.kv_write_rpc_count += 1.0;
            // Write bytes are known in advance, so the WRU cost is charged here.
            self.calculate_write_cost(consumption, req);
        } else {
            consumption.kv_read_rpc_count += 1.0;
            // Read bytes are unknown before execution: charge the base cost.
            consumption.r_r_u +=
                cfg.read_base_cost + cfg.read_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION;
            // Paging pre-charge.
            let bytes_for_est = estimated_read_bytes(req);
            if bytes_for_est > 0 {
                consumption.r_r_u += cfg.read_bytes_cost * bytes_for_est as f64;
            }
        }
        if req.access_location_type() == AccessLocationType::AccessCrossZone {
            if req.is_write() {
                consumption.write_cross_az_traffic_bytes = consumption
                    .write_cross_az_traffic_bytes
                    .wrapping_add(req.request_size());
            } else {
                consumption.read_cross_az_traffic_bytes = consumption
                    .read_cross_az_traffic_bytes
                    .wrapping_add(req.request_size());
            }
        }
    }

    fn after_kv_request(
        &self,
        consumption: &mut Consumption,
        req: &dyn RequestInfo,
        res: &dyn ResponseInfo,
    ) {
        if !req.is_write() {
            // Only a read request reports its KV CPU cost.
            self.calculate_cpu_cost(consumption, res);
            // Paging settlement.
            let bytes_for_est = estimated_read_bytes(req);
            if bytes_for_est > 0 {
                consumption.r_r_u -= self.ru_config.read_bytes_cost * bytes_for_est as f64;
            }
        } else if !res.succeed() {
            // A failed write pays back its WRU cost.
            self.pay_back_write_cost(consumption, req);
        }
        // A write may also read data; that counts as RRU even on failure.
        self.calculate_read_cost(consumption, res);
        calculate_cross_az_traffic(consumption, req, res);
    }

    fn as_kv_calculator(&self) -> Option<&KVCalculator> {
        Some(self)
    }
}

impl KVCalculator {
    pub(crate) fn calculate_write_cost(
        &self,
        consumption: &mut Consumption,
        req: &dyn RequestInfo,
    ) {
        let cfg = &self.ru_config;
        let write_bytes = req.write_bytes() as f64;
        consumption.write_bytes += write_bytes;
        // Written data is replicated to every replica.
        let mut replica_nums = req.replica_number() as u64;
        if replica_nums == 0 {
            replica_nums = 1;
        }
        consumption.w_r_u += (cfg.write_base_cost
            + cfg.write_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION
            + cfg.write_bytes_cost * write_bytes)
            * replica_nums as f64;
        // Raft replication across N replicas is assumed to cost
        // writeBytes * (N - 1) of cross-AZ traffic. An "unknown" location is
        // likely not a cross-AZ deployment and counts nothing.
        if req.access_location_type() != AccessLocationType::AccessUnknown {
            consumption.write_cross_az_traffic_bytes = consumption
                .write_cross_az_traffic_bytes
                .wrapping_add(req.write_bytes().wrapping_mul(replica_nums.wrapping_sub(1)));
        }
    }

    pub(crate) fn calculate_read_cost(
        &self,
        consumption: &mut Consumption,
        res: &dyn ResponseInfo,
    ) {
        let read_bytes = res.read_bytes() as f64;
        consumption.read_bytes += read_bytes;
        consumption.r_r_u += self.ru_config.read_bytes_cost * read_bytes;
    }

    pub(crate) fn calculate_cpu_cost(&self, consumption: &mut Consumption, res: &dyn ResponseInfo) {
        let kv_cpu_ms = res.kv_cpu().as_nanos() as i64 as f64 / 1_000_000.0;
        consumption.total_cpu_time_ms += kv_cpu_ms;
        consumption.r_r_u += self.ru_config.cpu_ms_cost * kv_cpu_ms;
    }

    pub(crate) fn pay_back_write_cost(&self, consumption: &mut Consumption, req: &dyn RequestInfo) {
        let write_bytes = req.write_bytes() as f64;
        consumption.write_bytes -= write_bytes;
        consumption.w_r_u -=
            self.ru_config.write_base_cost + self.ru_config.write_bytes_cost * write_bytes;
    }

    pub(crate) fn paging_precharge_rru(&self, req: &dyn RequestInfo) -> f64 {
        let bytes_for_est = estimated_read_bytes(req);
        if bytes_for_est == 0 {
            return 0.0;
        }
        self.ru_config.read_bytes_cost * bytes_for_est as f64
    }
}

pub(crate) fn calculate_cross_az_traffic(
    consumption: &mut Consumption,
    req: &dyn RequestInfo,
    res: &dyn ResponseInfo,
) {
    if req.access_location_type() == AccessLocationType::AccessCrossZone {
        if req.is_write() {
            consumption.write_cross_az_traffic_bytes = consumption
                .write_cross_az_traffic_bytes
                .wrapping_add(res.response_size());
        } else {
            consumption.read_cross_az_traffic_bytes = consumption
                .read_cross_az_traffic_bytes
                .wrapping_add(res.response_size());
        }
    }
}

pub(crate) fn clone_consumption(consumption: Option<&Consumption>) -> Consumption {
    consumption.cloned().unwrap_or_default()
}

fn adjust_paging_precharge_rru(
    calculators: &[Box<dyn ResourceCalculator>],
    consumption: &mut Consumption,
    req: &dyn RequestInfo,
    sign: f64,
) {
    for calc in calculators {
        if let Some(kv_calc) = calc.as_kv_calculator() {
            consumption.r_r_u += sign * kv_calc.paging_precharge_rru(req);
        }
    }
}

/// The request-side consumption reported to the server: the token delta
/// without the paging pre-charge. Without a pre-charge the token delta
/// itself is reported.
pub(crate) fn reported_request_consumption<'a>(
    calculators: &[Box<dyn ResourceCalculator>],
    req: &dyn RequestInfo,
    token_delta: &'a Consumption,
) -> Cow<'a, Consumption> {
    if estimated_read_bytes(req) == 0 {
        return Cow::Borrowed(token_delta);
    }
    let mut reported = clone_consumption(Some(token_delta));
    adjust_paging_precharge_rru(calculators, &mut reported, req, -1.0);
    Cow::Owned(reported)
}

/// The response-side consumption reported to the server: the actual usage,
/// not the paging settlement.
pub(crate) fn reported_response_consumption<'a>(
    calculators: &[Box<dyn ResourceCalculator>],
    req: &dyn RequestInfo,
    token_delta: &'a Consumption,
) -> Cow<'a, Consumption> {
    if estimated_read_bytes(req) == 0 {
        return Cow::Borrowed(token_delta);
    }
    let mut reported = clone_consumption(Some(token_delta));
    adjust_paging_precharge_rru(calculators, &mut reported, req, 1.0);
    Cow::Owned(reported)
}

/// Calculates the SQL-side consumption.
#[derive(Clone, Debug)]
pub struct SQLCalculator {
    pub ru_config: Arc<RUConfig>,
}

pub(crate) fn new_sql_calculator(cfg: Arc<RUConfig>) -> SQLCalculator {
    SQLCalculator { ru_config: cfg }
}

impl ResourceCalculator for SQLCalculator {
    /// Updates the SQL layer CPU consumption.
    fn trickle(&self, consumption: &mut Consumption) {
        let delta = get_sql_process_cpu_time(self.ru_config.is_single_group_by_keyspace)
            - consumption.sql_layer_cpu_time_ms;
        consumption.total_cpu_time_ms += delta;
        consumption.sql_layer_cpu_time_ms += delta;
    }

    fn before_kv_request(&self, _consumption: &mut Consumption, _req: &dyn RequestInfo) {}

    fn after_kv_request(
        &self,
        _consumption: &mut Consumption,
        _req: &dyn RequestInfo,
        _res: &dyn ResponseInfo,
    ) {
    }
}

pub(crate) fn get_ru_value_from_consumption(custom: Option<&Consumption>) -> f64 {
    match custom {
        Some(custom) => custom.r_r_u + custom.w_r_u,
        None => 0.0,
    }
}

pub(crate) fn add(custom1: Option<&mut Consumption>, custom2: Option<&Consumption>) {
    let (Some(custom1), Some(custom2)) = (custom1, custom2) else {
        return;
    };
    custom1.r_r_u += custom2.r_r_u;
    custom1.w_r_u += custom2.w_r_u;
    custom1.read_bytes += custom2.read_bytes;
    custom1.write_bytes += custom2.write_bytes;
    custom1.total_cpu_time_ms += custom2.total_cpu_time_ms;
    custom1.sql_layer_cpu_time_ms += custom2.sql_layer_cpu_time_ms;
    custom1.kv_read_rpc_count += custom2.kv_read_rpc_count;
    custom1.kv_write_rpc_count += custom2.kv_write_rpc_count;
    custom1.read_cross_az_traffic_bytes = custom1
        .read_cross_az_traffic_bytes
        .wrapping_add(custom2.read_cross_az_traffic_bytes);
    custom1.write_cross_az_traffic_bytes = custom1
        .write_cross_az_traffic_bytes
        .wrapping_add(custom2.write_cross_az_traffic_bytes);
    custom1.tikv_r_u_v2 += custom2.tikv_r_u_v2;
    custom1.tidb_r_u_v2 += custom2.tidb_r_u_v2;
    custom1.tiflash_r_u_v2 += custom2.tiflash_r_u_v2;
}

/// The positive per-field progress from `last` to `now`; `last` advances to
/// `now` for every field that grew.
pub(crate) fn update_delta_consumption(last: &mut Consumption, now: &Consumption) -> Consumption {
    let mut delta = Consumption::default();
    macro_rules! advance {
        ($($field:ident),* $(,)?) => {$(
            if now.$field > last.$field {
                delta.$field = now.$field - last.$field;
                last.$field = now.$field;
            }
        )*};
    }
    advance!(
        r_r_u,
        w_r_u,
        read_bytes,
        write_bytes,
        total_cpu_time_ms,
        sql_layer_cpu_time_ms,
        kv_read_rpc_count,
        kv_write_rpc_count,
        read_cross_az_traffic_bytes,
        write_cross_az_traffic_bytes,
        tikv_r_u_v2,
        tidb_r_u_v2,
        tiflash_r_u_v2,
    );
    delta
}

pub(crate) fn sub(custom1: Option<&mut Consumption>, custom2: Option<&Consumption>) {
    let (Some(custom1), Some(custom2)) = (custom1, custom2) else {
        return;
    };
    custom1.r_r_u -= custom2.r_r_u;
    custom1.w_r_u -= custom2.w_r_u;
    custom1.read_bytes -= custom2.read_bytes;
    custom1.write_bytes -= custom2.write_bytes;
    custom1.total_cpu_time_ms -= custom2.total_cpu_time_ms;
    custom1.sql_layer_cpu_time_ms -= custom2.sql_layer_cpu_time_ms;
    custom1.kv_read_rpc_count -= custom2.kv_read_rpc_count;
    custom1.kv_write_rpc_count -= custom2.kv_write_rpc_count;
    custom1.read_cross_az_traffic_bytes = custom1
        .read_cross_az_traffic_bytes
        .wrapping_sub(custom2.read_cross_az_traffic_bytes);
    custom1.write_cross_az_traffic_bytes = custom1
        .write_cross_az_traffic_bytes
        .wrapping_sub(custom2.write_cross_az_traffic_bytes);
    custom1.tikv_r_u_v2 -= custom2.tikv_r_u_v2;
    custom1.tidb_r_u_v2 -= custom2.tidb_r_u_v2;
    custom1.tiflash_r_u_v2 -= custom2.tiflash_r_u_v2;
}

pub(crate) fn equal_ru(custom1: &Consumption, custom2: &Consumption) -> bool {
    custom1.r_r_u == custom2.r_r_u
        && custom1.w_r_u == custom2.w_r_u
        && custom1.tikv_r_u_v2 == custom2.tikv_r_u_v2
        && custom1.tidb_r_u_v2 == custom2.tidb_r_u_v2
        && custom1.tiflash_r_u_v2 == custom2.tiflash_r_u_v2
}

/// The cumulative user+system CPU time (in ms) since the process started.
pub(crate) fn get_sql_process_cpu_time(is_single_group_by_keyspace: bool) -> f64 {
    if is_single_group_by_keyspace {
        return get_sys_process_cpu_time();
    }
    get_group_process_cpu_time()
}

/// Go reads gosigar's `ProcTime` for the current pid: user plus system time,
/// each in whole milliseconds.
fn get_sys_process_cpu_time() -> f64 {
    match process_cpu_time_ms() {
        Some((user, sys)) => (user.wrapping_add(sys)) as f64,
        None => {
            log::error!("getCPUTime get pid failed");
            0.0
        }
    }
}

#[cfg(unix)]
fn process_cpu_time_ms() -> Option<(u64, u64)> {
    use std::os::raw::{c_int, c_long};

    #[repr(C)]
    struct Timeval {
        tv_sec: c_long,
        #[cfg(target_os = "macos")]
        tv_usec: i32,
        #[cfg(not(target_os = "macos"))]
        tv_usec: c_long,
    }

    #[repr(C)]
    struct Rusage {
        ru_utime: Timeval,
        ru_stime: Timeval,
        ru_rest: [c_long; 14],
    }

    extern "C" {
        fn getrusage(who: c_int, usage: *mut Rusage) -> c_int;
    }

    const RUSAGE_SELF: c_int = 0;
    let mut usage = std::mem::MaybeUninit::<Rusage>::zeroed();
    // SAFETY: `usage` is a writable, properly aligned `struct rusage`.
    let result = unsafe { getrusage(RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // SAFETY: getrusage succeeded and initialized the structure.
    let usage = unsafe { usage.assume_init() };
    let millis = |time: &Timeval| -> u64 {
        (time.tv_sec as i64 as u64)
            .wrapping_mul(1000)
            .wrapping_add((time.tv_usec as i64 / 1000) as u64)
    };
    Some((millis(&usage.ru_utime), millis(&usage.ru_stime)))
}

#[cfg(not(unix))]
fn process_cpu_time_ms() -> Option<(u64, u64)> {
    None
}

/// Go leaves multi-group CPU attribution unimplemented and answers 0.
fn get_group_process_cpu_time() -> f64 {
    0.0
}
