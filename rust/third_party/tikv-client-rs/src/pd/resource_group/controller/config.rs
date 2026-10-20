// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `config.go`: controller constants, the server-stored controller
//! configuration and the request-unit model coefficients.

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use serde_derive::Serialize;
use serde_json::{Map, Value};

use super::model::RequestUnit;
use super::util::{new_duration, Duration};

/// The weight applied to a new "sample" of RU usage (with one sample per
/// main loop update interval).
pub(crate) const MOVING_AVG_FACTOR: f64 = 0.5;
pub(crate) const NOTIFY_FRACTION: f64 = 0.1;
pub(crate) const TOKEN_RESERVE_FRACTION: f64 = 0.8;
pub(crate) const CONSUMPTIONS_REPORTING_THRESHOLD: f64 = 100.0;
pub(crate) const EXTENDED_REPORTING_PERIOD_FACTOR: u32 = 4;
/// The interval to clean up the deleted resource groups in memory.
pub(crate) const DEFAULT_GROUP_CLEANUP_INTERVAL: StdDuration = StdDuration::from_secs(5 * 60);
/// The interval to update the state of the resource groups.
pub(crate) const DEFAULT_GROUP_STATE_UPDATE_INTERVAL: StdDuration = StdDuration::from_secs(1);
/// How long acquired tokens are expected to last. It should be a factor of
/// the Prometheus sampling period of 15 seconds.
pub(crate) const DEFAULT_TARGET_PERIOD: StdDuration = StdDuration::from_secs(5);
/// The max duration to wait for the token before throwing error.
pub(crate) const DEFAULT_MAX_WAIT_DURATION: StdDuration = StdDuration::from_secs(30);
/// The upper bound of backoff delay for local token bucket RPC.
pub(crate) const DEFAULT_LTB_TOKEN_RPC_MAX_DELAY: StdDuration = StdDuration::from_secs(1);
/// The times to retry when waiting for the token.
pub(crate) const DEFAULT_WAIT_RETRY_TIMES: i64 = 20;
/// The interval to retry when waiting for the token.
pub(crate) const DEFAULT_WAIT_RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// 1 RU = 8 storage read requests.
pub(crate) const DEFAULT_READ_BASE_COST: f64 = 1. / 8.;
/// 1 RU = 2 storage read batch requests.
pub(crate) const DEFAULT_READ_PER_BATCH_BASE_COST: f64 = 1. / 2.;
/// 1 RU = 1 storage write request.
pub(crate) const DEFAULT_WRITE_BASE_COST: f64 = 1.;
/// 1 RU = 1 storage write batch request.
pub(crate) const DEFAULT_WRITE_PER_BATCH_BASE_COST: f64 = 1.;
/// 1 RU = 64 KiB read bytes.
pub(crate) const DEFAULT_READ_COST_PER_BYTE: f64 = 1. / (64. * 1024.);
/// 1 RU = 1 KiB written bytes.
pub(crate) const DEFAULT_WRITE_COST_PER_BYTE: f64 = 1. / 1024.;
/// 1 RU = 3 millisecond CPU time.
pub(crate) const DEFAULT_CPU_MS_COST: f64 = 1. / 3.;
/// The resource manager does not run in microservice mode by default, so
/// degraded mode is disabled.
pub(crate) const DEFAULT_DEGRADED_MODE_WAIT_DURATION: StdDuration = StdDuration::ZERO;
pub(crate) const DEFAULT_AVG_BATCH_PROPORTION: f64 = 0.7;

/// The parameters for local bucket RPC.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct TokenRPCParams {
    /// The interval to retry when waiting for the token.
    #[serde(rename = "wait-retry-interval")]
    pub wait_retry_interval: Duration,
    /// The times to retry when waiting for the token.
    #[serde(rename = "wait-retry-times")]
    pub wait_retry_times: i64,
}

/// The configuration for local bucket. Not exported to the server side.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LocalBucketConfig {
    pub token_rpc_params: TokenRPCParams,
}

/// The controller options the client needs.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BaseConfig {
    /// How long the client waits for the server before entering degraded mode.
    pub degraded_mode_wait_duration: Duration,
    /// The max wait time duration for local token bucket.
    pub ltb_max_wait_duration: Duration,
    /// The upper bound of backoff delay for local token bucket RPC.
    pub ltb_token_rpc_max_delay: Duration,
    /// The coefficients of the RRU and WRU cost. Modify carefully.
    pub request_unit: RequestUnitConfig,
    /// Whether the resource control client enables trace logging.
    pub enable_controller_trace_log: bool,
    /// Which RU calculation version to use per keyspace.
    pub ru_version_policy: Option<RUVersionPolicy>,
}

/// An RU calculation version.
pub type RUVersion = i32;

/// The default RU calculation version.
pub const RU_VERSION_V1: RUVersion = 1;
/// The RU calculation version with detailed metrics.
pub const RU_VERSION_V2: RUVersion = 2;
/// The RU calculation version used when no policy is configured.
pub const DEFAULT_RU_VERSION: RUVersion = RU_VERSION_V1;

/// RU calculation version per keyspace.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RUVersionPolicy {
    #[serde(rename = "default")]
    pub default: RUVersion,
    #[serde(rename = "overrides", skip_serializing_if = "HashMap::is_empty")]
    pub overrides: HashMap<u32, RUVersion>,
}

/// The configuration of the resource manager controller.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Config {
    pub base_config: BaseConfig,
    pub local_bucket_config: LocalBucketConfig,
}

impl Config {
    /// Go `Config.Adjust`.
    pub fn adjust(&mut self) {
        if self.base_config.ltb_max_wait_duration.duration.is_zero() {
            self.base_config.ltb_max_wait_duration = new_duration(DEFAULT_MAX_WAIT_DURATION);
        }
        let params = &mut self.local_bucket_config.token_rpc_params;
        if params.wait_retry_interval.duration.is_zero() {
            params.wait_retry_interval = new_duration(DEFAULT_WAIT_RETRY_INTERVAL);
        }
        // Adjust the client settings: calculate the retry times.
        let max_delay =
            super::util::duration_nanos(self.base_config.ltb_token_rpc_max_delay.duration);
        let interval = super::util::duration_nanos(params.wait_retry_interval.duration);
        if max_delay != interval.wrapping_mul(params.wait_retry_times) {
            params.wait_retry_times = max_delay / interval;
        }
    }

    /// Go `json.Marshal(config)`: embedded `BaseConfig` and
    /// `LocalBucketConfig` fields are promoted to the top-level object.
    pub fn marshal_json(&self) -> String {
        #[derive(Serialize)]
        struct Wire<'a> {
            #[serde(rename = "degraded-mode-wait-duration")]
            degraded_mode_wait_duration: &'a Duration,
            #[serde(rename = "ltb-max-wait-duration")]
            ltb_max_wait_duration: &'a Duration,
            #[serde(rename = "ltb-token-rpc-max-delay")]
            ltb_token_rpc_max_delay: &'a Duration,
            #[serde(rename = "request-unit")]
            request_unit: &'a RequestUnitConfig,
            // Go's `,string` option quotes the boolean.
            #[serde(rename = "enable-controller-trace-log")]
            enable_controller_trace_log: String,
            #[serde(rename = "ru-version-policy", skip_serializing_if = "Option::is_none")]
            ru_version_policy: Option<&'a RUVersionPolicy>,
            #[serde(rename = "token-rpc-params")]
            token_rpc_params: &'a TokenRPCParams,
        }
        let base = &self.base_config;
        serde_json::to_string(&Wire {
            degraded_mode_wait_duration: &base.degraded_mode_wait_duration,
            ltb_max_wait_duration: &base.ltb_max_wait_duration,
            ltb_token_rpc_max_delay: &base.ltb_token_rpc_max_delay,
            request_unit: &base.request_unit,
            enable_controller_trace_log: base.enable_controller_trace_log.to_string(),
            ru_version_policy: base.ru_version_policy.as_ref(),
            token_rpc_params: &self.local_bucket_config.token_rpc_params,
        })
        .expect("controller config is always serializable")
    }

    /// Go `json.Unmarshal(data, config)`: fields present in `data` overwrite
    /// the current values, absent and unknown fields are left unchanged, and
    /// object keys match field names case-insensitively.
    pub fn unmarshal_json(&mut self, data: &[u8]) -> Result<(), String> {
        let value: Value = serde_json::from_slice(data).map_err(|error| error.to_string())?;
        let object = match value {
            Value::Object(object) => object,
            Value::Null => return Ok(()),
            other => return Err(type_error("Config", &other)),
        };
        let base = &mut self.base_config;
        for (key, value) in &object {
            match field(key) {
                "degraded-mode-wait-duration" => {
                    decode_duration(value, &mut base.degraded_mode_wait_duration)?
                }
                "ltb-max-wait-duration" => decode_duration(value, &mut base.ltb_max_wait_duration)?,
                "ltb-token-rpc-max-delay" => {
                    decode_duration(value, &mut base.ltb_token_rpc_max_delay)?
                }
                "request-unit" => base.request_unit.unmarshal_value(value)?,
                "enable-controller-trace-log" => match value {
                    Value::Null => {}
                    Value::String(text) if text == "true" => {
                        base.enable_controller_trace_log = true
                    }
                    Value::String(text) if text == "false" => {
                        base.enable_controller_trace_log = false
                    }
                    other => {
                        return Err(format!(
                        "json: invalid use of ,string struct tag, trying to unmarshal {} into bool",
                        other
                    ))
                    }
                },
                "ru-version-policy" => match value {
                    Value::Null => base.ru_version_policy = None,
                    Value::Object(policy) => {
                        let current = base.ru_version_policy.get_or_insert_with(Default::default);
                        current.unmarshal_object(policy)?;
                    }
                    other => return Err(type_error("RUVersionPolicy", other)),
                },
                "token-rpc-params" => match value {
                    Value::Null => {}
                    Value::Object(params) => {
                        let current = &mut self.local_bucket_config.token_rpc_params;
                        for (key, value) in params {
                            match field(key) {
                                "wait-retry-interval" => {
                                    decode_duration(value, &mut current.wait_retry_interval)?
                                }
                                "wait-retry-times" if !value.is_null() => {
                                    current.wait_retry_times = decode_i64(value)?;
                                }
                                _ => {}
                            }
                        }
                    }
                    other => return Err(type_error("TokenRPCParams", other)),
                },
                _ => {}
            }
        }
        Ok(())
    }
}

impl RUVersionPolicy {
    fn unmarshal_object(&mut self, object: &Map<String, Value>) -> Result<(), String> {
        for (key, value) in object {
            match key.to_ascii_lowercase().as_str() {
                "default" => {
                    if !value.is_null() {
                        self.default = decode_i32(value)?;
                    }
                }
                "overrides" => match value {
                    Value::Null => self.overrides.clear(),
                    Value::Object(overrides) => {
                        for (keyspace, version) in overrides {
                            let keyspace: u32 = keyspace.parse().map_err(|_| {
                                format!("json: cannot unmarshal number {keyspace} into Go value of type uint32")
                            })?;
                            self.overrides.insert(keyspace, decode_i32(version)?);
                        }
                    }
                    other => return Err(type_error("map[uint32]int32", other)),
                },
                _ => {}
            }
        }
        Ok(())
    }
}

/// The canonical field name for a case-insensitively matched JSON key.
fn field(key: &str) -> &'static str {
    const FIELDS: &[&str] = &[
        "degraded-mode-wait-duration",
        "ltb-max-wait-duration",
        "ltb-token-rpc-max-delay",
        "request-unit",
        "enable-controller-trace-log",
        "ru-version-policy",
        "token-rpc-params",
        "wait-retry-interval",
        "wait-retry-times",
        "read-base-cost",
        "read-per-batch-base-cost",
        "read-cost-per-byte",
        "write-base-cost",
        "write-per-batch-base-cost",
        "write-cost-per-byte",
        "read-cpu-ms-cost",
    ];
    FIELDS
        .iter()
        .find(|name| name.eq_ignore_ascii_case(key))
        .copied()
        .unwrap_or("")
}

fn type_error(target: &str, value: &Value) -> String {
    let kind = match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    format!("json: cannot unmarshal {kind} into Go value of type {target}")
}

fn decode_duration(value: &Value, target: &mut Duration) -> Result<(), String> {
    match value {
        Value::String(text) => {
            *target = new_duration(super::util::parse_duration(text)?);
            Ok(())
        }
        // Go calls UnmarshalJSON, whose strconv.Unquote rejects non-strings.
        _ => Err("invalid syntax".to_owned()),
    }
}

fn decode_f64(value: &Value) -> Result<f64, String> {
    value.as_f64().ok_or_else(|| type_error("float64", value))
}

fn decode_i64(value: &Value) -> Result<i64, String> {
    value.as_i64().ok_or_else(|| type_error("int", value))
}

fn decode_i32(value: &Value) -> Result<i32, String> {
    value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| type_error("int32", value))
}

/// Go `DefaultConfig`.
pub fn default_config() -> Config {
    Config {
        base_config: BaseConfig {
            degraded_mode_wait_duration: new_duration(DEFAULT_DEGRADED_MODE_WAIT_DURATION),
            request_unit: default_request_unit_config(),
            enable_controller_trace_log: false,
            ltb_max_wait_duration: new_duration(DEFAULT_MAX_WAIT_DURATION),
            ltb_token_rpc_max_delay: new_duration(DEFAULT_LTB_TOKEN_RPC_MAX_DELAY),
            ru_version_policy: None,
        },
        local_bucket_config: LocalBucketConfig {
            token_rpc_params: TokenRPCParams {
                wait_retry_interval: new_duration(DEFAULT_WAIT_RETRY_INTERVAL),
                wait_retry_times: DEFAULT_WAIT_RETRY_TIMES,
            },
        },
    }
}

/// The coefficients of the RRU and WRU cost. Modify carefully.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RequestUnitConfig {
    /// The base cost for a read request, independent of bytes and CPU.
    #[serde(rename = "read-base-cost")]
    pub read_base_cost: f64,
    /// The base cost for a read request with batch.
    #[serde(rename = "read-per-batch-base-cost")]
    pub read_per_batch_base_cost: f64,
    /// The cost for each byte read. 1 RU = 64 KiB by default.
    #[serde(rename = "read-cost-per-byte")]
    pub read_cost_per_byte: f64,
    /// The base cost for a write request, independent of bytes and CPU.
    #[serde(rename = "write-base-cost")]
    pub write_base_cost: f64,
    /// The base cost for a write request with batch.
    #[serde(rename = "write-per-batch-base-cost")]
    pub write_per_batch_base_cost: f64,
    /// The cost for each byte written. 1 RU = 1 KiB by default.
    #[serde(rename = "write-cost-per-byte")]
    pub write_cost_per_byte: f64,
    /// The cost for each millisecond of CPU time. 1 RU = 3 ms by default.
    #[serde(rename = "read-cpu-ms-cost")]
    pub cpu_ms_cost: f64,
}

impl RequestUnitConfig {
    fn unmarshal_value(&mut self, value: &Value) -> Result<(), String> {
        let object = match value {
            Value::Null => return Ok(()),
            Value::Object(object) => object,
            other => return Err(type_error("RequestUnitConfig", other)),
        };
        for (key, value) in object {
            if value.is_null() {
                continue;
            }
            let target = match field(key) {
                "read-base-cost" => &mut self.read_base_cost,
                "read-per-batch-base-cost" => &mut self.read_per_batch_base_cost,
                "read-cost-per-byte" => &mut self.read_cost_per_byte,
                "write-base-cost" => &mut self.write_base_cost,
                "write-per-batch-base-cost" => &mut self.write_per_batch_base_cost,
                "write-cost-per-byte" => &mut self.write_cost_per_byte,
                "read-cpu-ms-cost" => &mut self.cpu_ms_cost,
                _ => continue,
            };
            *target = decode_f64(value)?;
        }
        Ok(())
    }
}

/// Go `DefaultRequestUnitConfig`.
pub fn default_request_unit_config() -> RequestUnitConfig {
    RequestUnitConfig {
        read_base_cost: DEFAULT_READ_BASE_COST,
        read_per_batch_base_cost: DEFAULT_READ_PER_BATCH_BASE_COST,
        read_cost_per_byte: DEFAULT_READ_COST_PER_BYTE,
        write_base_cost: DEFAULT_WRITE_BASE_COST,
        write_per_batch_base_cost: DEFAULT_WRITE_PER_BATCH_BASE_COST,
        write_cost_per_byte: DEFAULT_WRITE_COST_PER_BYTE,
        cpu_ms_cost: DEFAULT_CPU_MS_COST,
    }
}

/// The read/write request unit standards, calculated from a
/// `RequestUnitConfig`, plus the client options.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RUConfig {
    // RU model config.
    pub read_base_cost: RequestUnit,
    pub read_per_batch_base_cost: RequestUnit,
    pub read_bytes_cost: RequestUnit,
    pub write_base_cost: RequestUnit,
    pub write_per_batch_base_cost: RequestUnit,
    pub write_bytes_cost: RequestUnit,
    pub cpu_ms_cost: RequestUnit,
    /// The CPU statistics need to distinguish between different environments.
    pub(crate) is_single_group_by_keyspace: bool,

    // Client options.
    pub ltb_max_wait_duration: StdDuration,
    pub wait_retry_interval: StdDuration,
    pub wait_retry_times: i64,
    pub degraded_mode_wait_duration: StdDuration,
}

/// Go `DefaultRUConfig`.
pub fn default_ru_config() -> RUConfig {
    generate_ru_config(&default_config())
}

/// Go `GenerateRUConfig`.
pub fn generate_ru_config(config: &Config) -> RUConfig {
    let unit = &config.base_config.request_unit;
    RUConfig {
        read_base_cost: unit.read_base_cost,
        read_per_batch_base_cost: unit.read_per_batch_base_cost,
        read_bytes_cost: unit.read_cost_per_byte,
        write_base_cost: unit.write_base_cost,
        write_per_batch_base_cost: unit.write_per_batch_base_cost,
        write_bytes_cost: unit.write_cost_per_byte,
        cpu_ms_cost: unit.cpu_ms_cost,
        is_single_group_by_keyspace: false,
        ltb_max_wait_duration: config.base_config.ltb_max_wait_duration.duration,
        wait_retry_interval: config
            .local_bucket_config
            .token_rpc_params
            .wait_retry_interval
            .duration,
        wait_retry_times: config.local_bucket_config.token_rpc_params.wait_retry_times,
        degraded_mode_wait_duration: config.base_config.degraded_mode_wait_duration.duration,
    }
}
