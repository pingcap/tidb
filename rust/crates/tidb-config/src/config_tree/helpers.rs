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

//! Self-contained pieces of Go `pkg/config/config.go` and
//! `config_util.go`: the error-message-extension preparation, the `CSE`
//! and `TrxSummary` sub-sections, the `max_allowed_packet` validity rule,
//! `FlattenConfigItems`, and `MergeConfigItems`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::configtypes::Duration;

use super::config::Config;

/// Go `ConfReloadFunc`.
pub type ConfReloadFunc = fn(old_conf: &Config, new_conf: &Config);

/// Go `CloneConf`: clone through the public JSON representation so fields
/// tagged `json:"-"` are reset exactly as in the source.
pub fn clone_conf(config: &Config) -> Result<Config, String> {
    let content = serde_json::to_vec(config).map_err(|error| error.to_string())?;
    serde_json::from_slice(&content).map_err(|error| error.to_string())
}

/// max_allowed_packet must be in `[1024, 1<<30]` and a multiple of 1024.
pub const MAX_ALLOWED_PACKET_UNIT: u64 = 1024;
/// Minimum max_allowed_packet (Go `minMaxAllowedPacket`).
pub const MIN_MAX_ALLOWED_PACKET: u64 = MAX_ALLOWED_PACKET_UNIT;
/// Maximum max_allowed_packet (Go `maxOfMaxAllowedPacket`).
pub const MAX_OF_MAX_ALLOWED_PACKET: u64 = 1 << 30;
/// Default max_allowed_packet (Go `DefMaxAllowedPacket`).
pub const DEF_MAX_ALLOWED_PACKET: u64 = 64 << 20;

/// Go `validMaxAllowedPacket`.
pub fn valid_max_allowed_packet(v: u64) -> bool {
    (MIN_MAX_ALLOWED_PACKET..=MAX_OF_MAX_ALLOWED_PACKET).contains(&v)
        && v.is_multiple_of(MAX_ALLOWED_PACKET_UNIT)
}

/// Appends configured suffixes to matching user-facing errors (Go
/// `ErrorMessageExtension`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorMessageExtension {
    /// The (anchored) pattern to match against SQL errors.
    #[serde(rename = "pattern")]
    pub pattern: String,
    /// The suffix appended to matching errors.
    #[serde(rename = "suffix")]
    pub suffix: String,
    /// Prepared matcher populated when the global config is published.
    #[serde(skip)]
    pub regexp: Option<regex::Regex>,
}

impl PartialEq for ErrorMessageExtension {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
            && self.suffix == other.suffix
            && self.regexp.as_ref().map(regex::Regex::as_str)
                == other.regexp.as_ref().map(regex::Regex::as_str)
    }
}

impl Eq for ErrorMessageExtension {}

/// Prepares/validates error-message extensions (Go
/// `prepareErrorMessageExtensions`): compiles each pattern, drops or errors
/// on invalid ones, then sorts by descending pattern length, then pattern,
/// then suffix. Returns `(prepared, first_error)`.
pub fn prepare_error_message_extensions(
    extensions: &[ErrorMessageExtension],
    ignore_invalid: bool,
) -> Result<(Vec<ErrorMessageExtension>, Option<String>), String> {
    let mut prepared: Vec<ErrorMessageExtension> = Vec::with_capacity(extensions.len());
    let mut first_err: Option<String> = None;
    for ext in extensions {
        if ext.pattern.trim().is_empty() {
            let msg = "empty error-msg-extension pattern".to_string();
            if ignore_invalid {
                first_err.get_or_insert(msg);
                continue;
            }
            return Err(msg);
        }
        // Go uses RE2 (regex crate is also RE2-based): operator patterns
        // cannot cause catastrophic backtracking.
        let matcher = match regex::Regex::new(&ext.pattern) {
            Ok(matcher) => matcher,
            Err(_) => {
                let msg = format!("invalid error-msg-extension regexp {:?}", ext.pattern);
                if ignore_invalid {
                    first_err.get_or_insert(msg);
                    continue;
                }
                return Err(msg);
            }
        };
        let mut prepared_extension = ext.clone();
        prepared_extension.regexp = Some(matcher);
        prepared.push(prepared_extension);
    }
    prepared.sort_by(|a, b| {
        if a.pattern.len() != b.pattern.len() {
            return b.pattern.len().cmp(&a.pattern.len());
        }
        if a.pattern != b.pattern {
            return a.pattern.cmp(&b.pattern);
        }
        a.suffix.cmp(&b.suffix)
    });
    Ok((prepared, first_err))
}

/// Cloud storage engine config (Go `CSE`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Cse {
    /// The columnar store type: `tiflash`, `columnar`, or `both`.
    #[serde(rename = "columnar-store-type")]
    pub columnar_store_type: String,
    /// Columnar collect timeout (nanoseconds, Go `time.Duration`).
    #[serde(rename = "columnar-collect-timeout")]
    pub columnar_collect_timeout: i64,
}

impl Cse {
    /// Go `IsTiFlashEnabled`.
    pub fn is_tiflash_enabled(&self) -> bool {
        self.columnar_store_type == "tiflash" || self.columnar_store_type == "both"
    }
    /// Go `IsColumnarStoreEnabled`.
    pub fn is_columnar_store_enabled(&self) -> bool {
        self.columnar_store_type == "columnar" || self.columnar_store_type == "both"
    }
    /// Go `Valid`.
    pub fn valid(&self) -> bool {
        matches!(
            self.columnar_store_type.as_str(),
            "tiflash" | "columnar" | "both"
        )
    }
    /// The default CSE (Go `defaultConf.CSE`).
    pub fn default_config() -> Cse {
        Cse {
            columnar_store_type: "tiflash".into(),
            columnar_collect_timeout: Duration(5 * 1_000_000_000).0,
        }
    }
}

/// Transaction-summary collector config (Go `TrxSummary`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TrxSummary {
    /// How many transaction summaries each node keeps.
    #[serde(rename = "transaction-summary-capacity")]
    pub transaction_summary_capacity: usize,
    /// Min execution duration to be recorded, in seconds-ish units.
    #[serde(rename = "transaction-id-digest-min-duration")]
    pub transaction_id_digest_min_duration: usize,
}

impl Default for TrxSummary {
    // Go `DefaultTrxSummary`.
    fn default() -> Self {
        TrxSummary {
            transaction_summary_capacity: 500,
            transaction_id_digest_min_duration: 2147483647,
        }
    }
}

impl TrxSummary {
    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        if self.transaction_summary_capacity > 5000 {
            return Err(
                "transaction-summary.transaction-summary-capacity should not be larger than 5000"
                    .into(),
            );
        }
        Ok(())
    }
}

/// A flattened config value (a JSON scalar or array; nested maps are
/// flattened away).
pub type FlatValue = serde_json::Value;

/// Go `dynamicConfigItems`.
pub const DYNAMIC_CONFIG_ITEMS: &[&str] = &[
    "TxnLocalLatches.Capacity",
    "Log.Level",
    "Log.ExpensiveThreshold",
    "Instance.SlowThreshold",
    "Instance.CheckMb4ValueInUTF8",
    "Performance.MaxProcs",
    "Performance.MaxMemory",
    "Performance.StmtCountLimit",
    "Performance.PseudoEstimateRatio",
    "Performance.TCPKeepAlive",
    "Performance.CrossJoin",
    "OpenTracing.Enable",
    "TiKVClient.StoreLimit",
    "CompatibleKillQuery",
    "TreatOldVersionUTF8AsUTF8MB4",
];

/// Go `MergeConfigItems`: applies runtime-dynamic fields and reports all
/// other changed fields as rejected.
pub fn merge_config_items(dst: &mut Config, new: &Config) -> (Vec<String>, Vec<String>) {
    let mut accepted = Vec::new();
    macro_rules! merge_dynamic {
        ($name:literal, $dst:expr, $new:expr) => {
            if $dst != $new {
                $dst = $new.clone();
                accepted.push($name.to_owned());
            }
        };
    }

    merge_dynamic!(
        "TxnLocalLatches.Capacity",
        dst.txn_local_latches.capacity,
        new.txn_local_latches.capacity
    );
    merge_dynamic!("Log.Level", dst.log.level, new.log.level);
    merge_dynamic!(
        "Log.ExpensiveThreshold",
        dst.log.expensive_threshold,
        new.log.expensive_threshold
    );
    merge_dynamic!(
        "Instance.SlowThreshold",
        dst.instance.slow_threshold,
        new.instance.slow_threshold
    );
    merge_dynamic!(
        "Instance.CheckMb4ValueInUTF8",
        dst.instance.check_mb4_value_in_utf8,
        new.instance.check_mb4_value_in_utf8
    );
    merge_dynamic!(
        "Performance.MaxProcs",
        dst.performance.max_procs,
        new.performance.max_procs
    );
    merge_dynamic!(
        "Performance.MaxMemory",
        dst.performance.max_memory,
        new.performance.max_memory
    );
    merge_dynamic!(
        "Performance.StmtCountLimit",
        dst.performance.stmt_count_limit,
        new.performance.stmt_count_limit
    );
    merge_dynamic!(
        "Performance.PseudoEstimateRatio",
        dst.performance.pseudo_estimate_ratio,
        new.performance.pseudo_estimate_ratio
    );
    merge_dynamic!(
        "Performance.TCPKeepAlive",
        dst.performance.tcp_keep_alive,
        new.performance.tcp_keep_alive
    );
    merge_dynamic!(
        "Performance.CrossJoin",
        dst.performance.cross_join,
        new.performance.cross_join
    );
    merge_dynamic!(
        "OpenTracing.Enable",
        dst.open_tracing.enable,
        new.open_tracing.enable
    );
    merge_dynamic!(
        "TiKVClient.StoreLimit",
        dst.tikv_client.store_limit,
        new.tikv_client.store_limit
    );
    merge_dynamic!(
        "CompatibleKillQuery",
        dst.compatible_kill_query,
        new.compatible_kill_query
    );
    merge_dynamic!(
        "TreatOldVersionUTF8AsUTF8MB4",
        dst.treat_old_version_utf8_as_utf8mb4,
        new.treat_old_version_utf8_as_utf8mb4
    );

    let dst_value = ordered_config_value(dst);
    let new_value = ordered_config_value(new);
    let mut rejected = Vec::new();
    collect_config_differences(&dst_value, &new_value, "", &mut rejected);
    (accepted, rejected)
}

#[derive(Debug, PartialEq)]
enum OrderedValue {
    Object(Vec<(String, OrderedValue)>),
    Array(Vec<OrderedValue>),
    Scalar(serde_json::Value),
}

impl<'de> Deserialize<'de> for OrderedValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OrderedValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(field) = map.next_entry()? {
                    fields.push(field);
                }
                Ok(OrderedValue::Object(fields))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(OrderedValue::Array(values))
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(value.into()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(value.into()))
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| OrderedValue::Scalar(number.into()))
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(value.to_owned())
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(value.into()))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(serde_json::Value::Null))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(OrderedValue::Scalar(serde_json::Value::Null))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

fn ordered_config_value(config: &Config) -> OrderedValue {
    let json = serde_json::to_string(config).expect("Config must serialize");
    let mut value: OrderedValue =
        serde_json::from_str(&json).expect("Config JSON must deserialize");
    let OrderedValue::Object(root) = &mut value else {
        unreachable!("Config serializes as an object")
    };

    let txn_latches = OrderedValue::Object(vec![
        (
            "Enabled".to_owned(),
            OrderedValue::Scalar(config.txn_local_latches.enabled.into()),
        ),
        (
            "Capacity".to_owned(),
            OrderedValue::Scalar((config.txn_local_latches.capacity as u64).into()),
        ),
    ]);
    let position = root
        .iter()
        .position(|(name, _)| name == "tmp-storage-quota")
        .expect("Config contains TempStorageQuota")
        + 1;
    root.insert(position, ("TxnLocalLatches".to_owned(), txn_latches));

    let error_message_extensions = OrderedValue::Array(
        config
            .error_message_extensions
            .iter()
            .map(|extension| {
                OrderedValue::Object(vec![
                    (
                        "pattern".to_owned(),
                        OrderedValue::Scalar(extension.pattern.clone().into()),
                    ),
                    (
                        "suffix".to_owned(),
                        OrderedValue::Scalar(extension.suffix.clone().into()),
                    ),
                    (
                        "Regexp".to_owned(),
                        OrderedValue::Scalar(
                            extension
                                .regexp
                                .as_ref()
                                .map(|regexp| regexp.as_str().into())
                                .unwrap_or(serde_json::Value::Null),
                        ),
                    ),
                ])
            })
            .collect(),
    );
    root.iter_mut()
        .find(|(name, _)| name == "error-msg-extension")
        .map(|(_, value)| *value = error_message_extensions)
        .expect("Config contains ErrorMessageExtensions");

    let observability = &config.keyspace_observability_values;
    let observability_values = OrderedValue::Object(vec![
        (
            "MetricLabels".to_owned(),
            unordered_json_value(&observability.metric_labels),
        ),
        (
            "SlowLogFields".to_owned(),
            OrderedValue::Array(
                observability
                    .slow_log_fields
                    .iter()
                    .map(|field| {
                        OrderedValue::Object(vec![
                            (
                                "Name".to_owned(),
                                OrderedValue::Scalar(field.name.clone().into()),
                            ),
                            (
                                "Value".to_owned(),
                                OrderedValue::Scalar(field.value.clone().into()),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
        (
            "StmtLogFields".to_owned(),
            unordered_json_value(&observability.stmt_log_fields),
        ),
    ]);
    let position = root
        .iter()
        .position(|(name, _)| name == "keyspace-observability")
        .expect("Config contains KeyspaceObservability")
        + 1;
    root.insert(
        position,
        (
            "KeyspaceObservabilityValues".to_owned(),
            observability_values,
        ),
    );

    let performance = root
        .iter_mut()
        .find(|(name, _)| name == "performance")
        .map(|(_, value)| value)
        .expect("Config contains Performance");
    let OrderedValue::Object(fields) = performance else {
        unreachable!("Performance serializes as an object")
    };
    let position = fields
        .iter()
        .position(|(name, _)| name == "max-txn-ttl")
        .expect("Performance contains MaxTxnTTL")
        + 1;
    fields.insert(
        position,
        (
            "MemProfileInterval".to_owned(),
            OrderedValue::Scalar(config.performance.mem_profile_interval.clone().into()),
        ),
    );

    let experimental = root
        .iter_mut()
        .find(|(name, _)| name == "experimental")
        .map(|(_, value)| value)
        .expect("Config contains Experimental");
    let OrderedValue::Object(fields) = experimental else {
        unreachable!("Experimental serializes as an object")
    };
    fields.push((
        "EnableNewCharset".to_owned(),
        OrderedValue::Scalar(config.experimental.enable_new_charset.into()),
    ));
    value
}

fn unordered_json_value(value: &impl Serialize) -> OrderedValue {
    fn convert(value: serde_json::Value) -> OrderedValue {
        match value {
            serde_json::Value::Array(values) => {
                OrderedValue::Array(values.into_iter().map(convert).collect())
            }
            serde_json::Value::Object(values) => OrderedValue::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, convert(value)))
                    .collect(),
            ),
            value => OrderedValue::Scalar(value),
        }
    }

    convert(serde_json::to_value(value).expect("configuration value must serialize"))
}

fn collect_config_differences(
    left: &OrderedValue,
    right: &OrderedValue,
    prefix: &str,
    differences: &mut Vec<String>,
) {
    match (left, right) {
        (OrderedValue::Object(left), OrderedValue::Object(right)) => {
            for (key, left) in left {
                let path = if prefix.is_empty() {
                    go_field_name(key)
                } else {
                    format!("{prefix}.{}", go_field_name(key))
                };
                match right.iter().find(|(right_key, _)| right_key == key) {
                    Some((_, right))
                        if matches!(
                            path.as_str(),
                            "Labels"
                                | "KeyspaceObservabilityValues.MetricLabels"
                                | "KeyspaceObservabilityValues.StmtLogFields"
                        ) =>
                    {
                        if !semantic_value_eq(left, right) {
                            differences.push(path);
                        }
                    }
                    Some((_, right)) => {
                        collect_config_differences(left, right, &path, differences);
                    }
                    None => differences.push(path),
                }
            }
        }
        _ if left != right => differences.push(prefix.to_owned()),
        _ => {}
    }
}

fn semantic_value_eq(left: &OrderedValue, right: &OrderedValue) -> bool {
    match (left, right) {
        (OrderedValue::Object(left), OrderedValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .iter()
                        .find(|(right_key, _)| right_key == key)
                        .is_some_and(|(_, right_value)| semantic_value_eq(value, right_value))
                })
        }
        (OrderedValue::Array(left), OrderedValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| semantic_value_eq(left, right))
        }
        _ => left == right,
    }
}

// Go `mergeConfigItems` walks struct fields, so its result uses Go field
// names rather than TOML/JSON tags. The explicit irregular names below are
// the acronym/casing cases that cannot be recovered by title-casing tags.
fn go_field_name(tag: &str) -> String {
    match tag {
        "allow-expression-index" => return "AllowsExpressionIndex".into(),
        "auto-tls" => return "AutoTLS".into(),
        "autoscaler-addr" => return "TiFlashComputeAutoScalerAddr".into(),
        "autoscaler-cluster-id" => return "AutoScalerClusterID".into(),
        "autoscaler-type" => return "TiFlashComputeAutoScalerType".into(),
        "cors" => return "Cors".into(),
        "ddl_slow_threshold" => return "DDLSlowOprThreshold".into(),
        "deprecate-integer-display-length" => return "DeprecateIntegerDisplayWidth".into(),
        "enable-32bits-connection-id" => return "Enable32BitsConnectionID".into(),
        "enable-batch-dml" => return "EnableBatchDML".into(),
        "enable-load-fmsketch" => return "EnableLoadFMSketch".into(),
        "enable-tcp4-only" => return "EnableTCP4Only".into(),
        "error-msg-extension" => return "ErrorMessageExtensions".into(),
        "grpc-keepalive-time" => return "GRPCKeepAliveTime".into(),
        "grpc-keepalive-timeout" => return "GRPCKeepAliveTimeout".into(),
        "in-mem-slow-query-topn-num" => return "InMemSlowQueryTopNNum".into(),
        "is-tiflashcompute-fixed-pool" => return "IsTiFlashComputeFixedPool".into(),
        "keyspace-activate" => return "KeyspaceActivateMode".into(),
        "oom-use-tmp-storage" => return "OOMUseTmpStorage".into(),
        "opentracing" => return "OpenTracing".into(),
        "standby-mode" => return "StandByMode".into(),
        "tmp-storage-path" => return "TempStoragePath".into(),
        "tmp-storage-quota" => return "TempStorageQuota".into(),
        "tls-version" => return "MinTLSVersion".into(),
        "transaction-summary" => return "TrxSummary".into(),
        "treat-old-version-utf8-as-utf8mb4" => return "TreatOldVersionUTF8AsUTF8MB4".into(),
        "use-autoscaler" => return "UseAutoScaler".into(),
        "pd-client" => return "PDClient".into(),
        "tikv-client" => return "TiKVClient".into(),
        "ru-v2" => return "RUV2".into(),
        "top-sql" => return "TopSQL".into(),
        "cse" => return "CSE".into(),
        "record-db-qps" => return "RecordQPSbyDB".into(),
        "gogc" => return "GOGC".into(),
        "tidb_check_mb4_value_in_utf8" => return "CheckMb4ValueInUTF8".into(),
        "tidb_enable_collect_execution_info" => return "EnableCollectExecutionInfo".into(),
        "tidb_enable_slow_log" => return "EnableSlowLog".into(),
        "tidb_expensive_query_time_threshold" => return "ExpensiveQueryTimeThreshold".into(),
        "tidb_expensive_txn_time_threshold" => return "ExpensiveTxnTimeThreshold".into(),
        "tidb_force_priority" => return "ForcePriority".into(),
        "tidb_instance_plan_cache_max_size" => return "InstancePlanCacheMaxMemSize".into(),
        "tidb_mem_arbitrator_mode" => return "MemArbitratorMode".into(),
        "tidb_mem_arbitrator_soft_limit" => return "MemArbitratorSoftLimit".into(),
        "tidb_mem_quota_binding_cache" => return "MemQuotaBindingCache".into(),
        "tidb_memory_usage_alarm_ratio" => return "MemoryUsageAlarmRatio".into(),
        "tidb_pprof_sql_cpu" => return "EnablePProfSQLCPU".into(),
        "tidb_record_plan_in_slow_log" => return "RecordPlanInSlowLog".into(),
        "tidb_schema_cache_size" => return "SchemaCacheSize".into(),
        "tidb_server_memory_limit" => return "ServerMemoryLimit".into(),
        "tidb_server_memory_limit_gc_trigger" => return "ServerMemoryLimitGCTrigger".into(),
        "tidb_slow_log_threshold" => return "SlowThreshold".into(),
        "tidb_stats_cache_mem_quota" => return "StatsCacheMemQuota".into(),
        "tidb_stmt_summary_enable_persistent" => return "StmtSummaryEnablePersistent".into(),
        "tidb_stmt_summary_file_max_backups" => return "StmtSummaryFileMaxBackups".into(),
        "tidb_stmt_summary_file_max_days" => return "StmtSummaryFileMaxDays".into(),
        "tidb_stmt_summary_file_max_size" => return "StmtSummaryFileMaxSize".into(),
        "tidb_stmt_summary_filename" => return "StmtSummaryFilename".into(),
        "tidb_stmt_summary_max_stmt_count" => return "StmtSummaryMaxStmtCount".into(),
        _ => {}
    }

    tag.split(['-', '_'])
        .map(|word| match word {
            "tidb" => "TiDB".into(),
            "tikv" => "TiKV".into(),
            "tiflash" => "TiFlash".into(),
            "dxf" => "DXF".into(),
            "ru" => "RU".into(),
            "sql" => "SQL".into(),
            "ssl" => "SSL".into(),
            "ca" => "CA".into(),
            "cn" => "CN".into(),
            "rsa" => "RSA".into(),
            "jwks" => "JWKS".into(),
            "sem" => "SEM".into(),
            "tcp" => "TCP".into(),
            "grpc" => "GRPC".into(),
            "qps" => "QPS".into(),
            "db" => "DB".into(),
            "ddl" => "DDL".into(),
            "oom" => "OOM".into(),
            "id" => "ID".into(),
            "url" => "URL".into(),
            "uri" => "URI".into(),
            "ttl" => "TTL".into(),
            "gc" => "GC".into(),
            "mpp" => "MPP".into(),
            "fm" => "FM".into(),
            "rpc" => "RPC".into(),
            "utf8" => "UTF8".into(),
            "pprof" => "PProf".into(),
            "rc" => "RC".into(),
            "ts" => "TS".into(),
            "tmp" => "Temp".into(),
            _ => {
                let mut chars = word.chars();
                match chars.next() {
                    Some(first) => first.to_uppercase().chain(chars).collect(),
                    None => String::new(),
                }
            }
        })
        .collect()
}

/// Flattens a nested config map into dotted keys (Go `FlattenConfigItems`).
/// Arrays are not flattened.
pub fn flatten_config_items(
    nested: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, FlatValue> {
    let mut flat = HashMap::new();
    flatten(&mut flat, &serde_json::Value::Object(nested.clone()), "");
    flat
}

fn flatten(flat: &mut HashMap<String, FlatValue>, nested: &serde_json::Value, prefix: &str) {
    match nested {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(flat, v, &path);
            }
        }
        // Don't flatten arrays or scalars.
        other => {
            flat.insert(prefix.to_string(), other.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_allowed_packet_validity() {
        assert!(valid_max_allowed_packet(1024));
        assert!(valid_max_allowed_packet(64 << 20));
        assert!(valid_max_allowed_packet(1 << 30));
        assert!(!valid_max_allowed_packet(0));
        assert!(!valid_max_allowed_packet(1023));
        assert!(!valid_max_allowed_packet(1500)); // not a multiple of 1024
        assert!(!valid_max_allowed_packet((1 << 30) + 1024));
    }

    #[test]
    fn clone_conf_uses_the_public_json_shape() {
        let mut source = Config::default();
        source.txn_local_latches.enabled = true;
        source.txn_local_latches.capacity = 42;
        source
            .keyspace_observability_values
            .metric_labels
            .insert("keyspace_meta_tier".into(), "premium".into());
        let cloned = clone_conf(&source).unwrap();
        assert_eq!(cloned.host, source.host);
        assert_eq!(cloned.port, source.port);
        assert_eq!(cloned.txn_local_latches, Default::default());
        assert!(cloned
            .keyspace_observability_values
            .metric_labels
            .is_empty());
    }

    // Go TestMergeConfigItems.
    #[test]
    fn test_merge_config_items() {
        let original = Config::default();
        let mut old = original.clone();
        let mut new = old.clone();

        new.performance.max_procs = 123;
        new.performance.max_memory = 123;
        new.performance.cross_join = false;
        new.performance.pseudo_estimate_ratio = 123.0;
        new.tikv_client.store_limit = 123;

        new.store = crate::store::StoreType("tiflash".to_owned());
        new.port = 2333;
        new.advertise_address = "1.2.3.4".to_owned();
        new.instance.slow_threshold = 2345;

        let (accepted, rejected) = merge_config_items(&mut old, &new);
        assert_eq!(
            accepted,
            [
                "Instance.SlowThreshold",
                "Performance.MaxProcs",
                "Performance.MaxMemory",
                "Performance.PseudoEstimateRatio",
                "Performance.CrossJoin",
                "TiKVClient.StoreLimit",
            ],
            "Go walks dynamic fields in reflected declaration order"
        );
        assert_eq!(rejected.len(), 3);
        assert_eq!(
            rejected,
            ["AdvertiseAddress", "Port", "Store"],
            "Go returns reflected struct-field paths"
        );
        assert!(accepted
            .iter()
            .all(|item| DYNAMIC_CONFIG_ITEMS.contains(&item.as_str())));
        assert!(rejected
            .iter()
            .all(|item| !DYNAMIC_CONFIG_ITEMS.contains(&item.as_str())));

        assert_eq!(old.performance.max_procs, new.performance.max_procs);
        assert_eq!(old.performance.max_memory, new.performance.max_memory);
        assert_eq!(old.performance.cross_join, new.performance.cross_join);
        assert_eq!(
            old.performance.pseudo_estimate_ratio,
            new.performance.pseudo_estimate_ratio
        );
        assert_eq!(old.tikv_client.store_limit, new.tikv_client.store_limit);
        assert_eq!(old.instance.slow_threshold, new.instance.slow_threshold);

        assert_eq!(old.store, original.store);
        assert_eq!(old.port, original.port);
        assert_eq!(old.advertise_address, original.advertise_address);

        let mut old = Config::default();
        let mut new = old.clone();
        new.token_limit += 1;
        new.temp_dir = "/different-temp".into();
        let (_, rejected) = merge_config_items(&mut old, &new);
        assert_eq!(rejected, ["TokenLimit", "TempDir"]);

        let mut old = Config::default();
        let mut new = old.clone();
        new.experimental.enable_new_charset = true;
        new.keyspace_observability_values
            .metric_labels
            .insert("keyspace_meta_tier".into(), "premium".into());
        let (_, rejected) = merge_config_items(&mut old, &new);
        assert_eq!(
            rejected,
            [
                "Experimental.EnableNewCharset",
                "KeyspaceObservabilityValues.MetricLabels",
            ]
        );

        let mut old = Config::default();
        old.error_message_extensions = vec![ErrorMessageExtension {
            pattern: "^same$".into(),
            suffix: "suffix".into(),
            regexp: None,
        }];
        let mut new = old.clone();
        new.error_message_extensions[0].regexp = Some(regex::Regex::new("^same$").unwrap());
        let (_, rejected) = merge_config_items(&mut old, &new);
        assert_eq!(rejected, ["ErrorMessageExtensions"]);
    }

    // Covers the standalone parts of Go
    // TestErrorMessageExtensionInvalidRegexp (the Load/deploy-mode paths
    // land with the Config struct).
    #[test]
    fn error_message_extension_prepare() {
        // Invalid regexp, strict.
        let exts = vec![ErrorMessageExtension {
            pattern: "[".into(),
            suffix: "x".into(),
            ..Default::default()
        }];
        assert!(prepare_error_message_extensions(&exts, false)
            .unwrap_err()
            .contains("invalid error-msg-extension regexp"));

        // Whitespace-only pattern, strict.
        let exts = vec![ErrorMessageExtension {
            pattern: " \t".into(),
            suffix: "x".into(),
            ..Default::default()
        }];
        assert!(prepare_error_message_extensions(&exts, false)
            .unwrap_err()
            .contains("empty error-msg-extension pattern"));

        // ignore_invalid collects the first error and drops bad entries.
        let exts = vec![
            ErrorMessageExtension {
                pattern: "".into(),
                suffix: "empty".into(),
                ..Default::default()
            },
            ErrorMessageExtension {
                pattern: "^ok$".into(),
                suffix: "good".into(),
                ..Default::default()
            },
        ];
        let (prepared, first) = prepare_error_message_extensions(&exts, true).unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].pattern, "^ok$");
        assert!(first.unwrap().contains("empty error-msg-extension pattern"));

        // Sort: longer patterns first, then lexicographic.
        let exts = vec![
            ErrorMessageExtension {
                pattern: "ab".into(),
                suffix: "".into(),
                ..Default::default()
            },
            ErrorMessageExtension {
                pattern: "abcd".into(),
                suffix: "".into(),
                ..Default::default()
            },
            ErrorMessageExtension {
                pattern: "aa".into(),
                suffix: "".into(),
                ..Default::default()
            },
        ];
        let (prepared, _) = prepare_error_message_extensions(&exts, false).unwrap();
        assert_eq!(
            prepared
                .iter()
                .map(|e| e.pattern.as_str())
                .collect::<Vec<_>>(),
            vec!["abcd", "aa", "ab"]
        );
    }

    #[test]
    fn cse_and_trx_summary() {
        let cse = Cse::default_config();
        assert!(cse.valid());
        assert!(cse.is_tiflash_enabled());
        assert!(!cse.is_columnar_store_enabled());
        assert!(Cse {
            columnar_store_type: "both".into(),
            columnar_collect_timeout: 0
        }
        .is_columnar_store_enabled());
        assert!(!Cse {
            columnar_store_type: "bogus".into(),
            columnar_collect_timeout: 0
        }
        .valid());

        let mut ts = TrxSummary::default();
        assert_eq!(ts.transaction_summary_capacity, 500);
        ts.valid().unwrap();
        ts.transaction_summary_capacity = 5001;
        assert!(ts.valid().is_err());
    }

    // Go TestFlattenConfig.
    #[test]
    fn flatten_config() {
        let json = r#"{
            "k0": 233333,
            "k1": "v1",
            "k2": ["v2-1", "v2-2", "v2-3"],
            "k3": [{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}],
            "k4": { "k4-1": [1, 2, 3, 4], "k4-2": [5, 6, 7, 8], "k4-3": [666] }
        }"#;
        let nested: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(json).unwrap();
        let flat = flatten_config_items(&nested);
        assert_eq!(flat.len(), 7);
        assert_eq!(flat["k0"], serde_json::json!(233333));
        assert_eq!(flat["k1"], serde_json::json!("v1"));
        assert_eq!(flat["k2"], serde_json::json!(["v2-1", "v2-2", "v2-3"]));
        assert_eq!(
            flat["k3"],
            serde_json::json!([{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}])
        );
        assert_eq!(flat["k4.k4-1"], serde_json::json!([1, 2, 3, 4]));
        assert_eq!(flat["k4.k4-2"], serde_json::json!([5, 6, 7, 8]));
        assert_eq!(flat["k4.k4-3"], serde_json::json!([666]));
    }
}
