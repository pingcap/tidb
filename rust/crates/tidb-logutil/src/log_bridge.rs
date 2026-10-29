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

//! Routes `log` crate records into the global logger.
//!
//! client-rust, the Rust client-go, logs through the `log` facade. Go's
//! client-go logs through zap into TiDB's global logger (`log.L()`), so its
//! lines land in TiDB's log file in the unified format at client-go's own
//! levels, filtered by TiDB's configured level. This logger is the Rust seam
//! that does the same: a record's formatted arguments are the entry message
//! and its level maps onto zap's (zap has no trace level, so TRACE logs at
//! debug, as in the `tracing` bridge).
//!
//! client-rust hands client-go's zap fields over as the record's key-values
//! (`log`'s `kv_serde`), in the order the call site names them; each becomes
//! the zap field Go's call site builds: numbers and bools stay typed
//! (`zap.Int`/`zap.Uint64`/`zap.Bool`/`zap.Float64`), Display/Debug captures
//! (`zap.Stringer`, `zap.Error`, `zap.Duration` rendered as Go's
//! `Duration.String()`, `zap.Reflect` as Go's JSON) are strings, and serde
//! sequences are arrays (`zap.Strings`). The unified encoder then quotes and
//! escapes them as Go's zap text encoder does.
//!
//! The caller slot cannot name a Go `file.go:line` for a Rust call site; it
//! carries the record's Rust source file and line instead, shortened like
//! every other caller by `getCallerString`. A record without a file names
//! its module path.

use std::sync::Once;

use tidb_log::{Field, Level, Value};

/// Installs the logger as the process-wide `log` logger, once. It reads the
/// current global logger per record, so later `init_logger`/`replace_logger`
/// calls take effect without reinstalling.
pub(super) fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = ::log::set_logger(&UnifiedLogLogger);
    });
    level_changed();
}

/// Re-reads the global level into `log`'s static filter, so records below
/// it (client-rust's per-request debug/trace records) are skipped at their
/// call sites instead of reaching the logger.
pub(super) fn level_changed() {
    ::log::set_max_level(max_level(super::get_level()));
}

fn max_level(level: Level) -> ::log::LevelFilter {
    match level {
        Level::Debug => ::log::LevelFilter::Trace,
        Level::Info => ::log::LevelFilter::Info,
        Level::Warn => ::log::LevelFilter::Warn,
        _ => ::log::LevelFilter::Error,
    }
}

fn level_of(level: ::log::Level) -> Level {
    match level {
        ::log::Level::Error => Level::Error,
        ::log::Level::Warn => Level::Warn,
        ::log::Level::Info => Level::Info,
        ::log::Level::Debug | ::log::Level::Trace => Level::Debug,
    }
}

struct UnifiedLogLogger;

impl ::log::Log for UnifiedLogLogger {
    fn enabled(&self, metadata: &::log::Metadata<'_>) -> bool {
        super::bg_logger().enabled(level_of(metadata.level()))
    }

    fn log(&self, record: &::log::Record<'_>) {
        let logger = super::bg_logger();
        let level = level_of(record.level());
        if !logger.enabled(level) {
            return;
        }
        let file = record.file().or(record.module_path()).unwrap_or_default();
        let message = record.args().to_string();
        let fields = zap_fields(record.key_values());
        logger.emit_at(
            level,
            file,
            record.line().unwrap_or_default(),
            &message,
            &fields,
        );
    }

    fn flush(&self) {
        let _ = super::sync();
    }
}

/// The record's key-values as zap fields, in call-site order.
fn zap_fields(source: &dyn ::log::kv::Source) -> Vec<Field> {
    struct Collect(Vec<Field>);

    impl<'kvs> ::log::kv::VisitSource<'kvs> for Collect {
        fn visit_pair(
            &mut self,
            key: ::log::kv::Key<'kvs>,
            value: ::log::kv::Value<'kvs>,
        ) -> Result<(), ::log::kv::Error> {
            self.0.push(Field::new(key.as_str(), zap_value(&value)));
            Ok(())
        }
    }

    let mut fields = Collect(Vec::with_capacity(source.count()));
    let _ = source.visit(&mut fields);
    fields.0
}

/// One key-value as the zap field value Go's call site builds.
fn zap_value(value: &::log::kv::Value<'_>) -> Value {
    if let Some(text) = value.to_borrowed_str() {
        return Value::Str(text.to_owned());
    }
    match serde_json::to_value(value) {
        // JSON has no NaN/Inf: serde_json maps them to null, zap.Float64
        // spells them.
        Ok(serde_json::Value::Null) if value.to_f64().is_some() => {
            Value::F64(value.to_f64().unwrap_or_default())
        }
        Ok(json) => json_value(json),
        Err(_) => Value::Str(value.to_string()),
    }
}

fn json_value(json: serde_json::Value) -> Value {
    match json {
        // zap.Reflect of a nil value encodes JSON `null`.
        serde_json::Value::Null => Value::Reflect("null".to_owned()),
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Value::I64(value)
            } else if let Some(value) = number.as_u64() {
                Value::U64(value)
            } else {
                Value::F64(number.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(value) => Value::Str(value),
        serde_json::Value::Array(items) => {
            Value::Array(items.into_iter().map(json_value).collect())
        }
        serde_json::Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .map(|(key, value)| Field::new(key, json_value(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_map_onto_zap_levels_with_trace_at_debug() {
        assert_eq!(level_of(::log::Level::Error), Level::Error);
        assert_eq!(level_of(::log::Level::Warn), Level::Warn);
        assert_eq!(level_of(::log::Level::Info), Level::Info);
        assert_eq!(level_of(::log::Level::Debug), Level::Debug);
        assert_eq!(level_of(::log::Level::Trace), Level::Debug);
        assert_eq!(max_level(Level::Debug), ::log::LevelFilter::Trace);
        assert_eq!(max_level(Level::Info), ::log::LevelFilter::Info);
        assert_eq!(max_level(Level::Warn), ::log::LevelFilter::Warn);
        assert_eq!(max_level(Level::Error), ::log::LevelFilter::Error);
        assert_eq!(max_level(Level::Fatal), ::log::LevelFilter::Error);
    }

    /// client-rust's zap fields arrive as `log` key-values and render as
    /// client-go's zap fields do in TiDB's log: typed numbers and bools,
    /// Display captures as quoted-when-needed strings, `zap.Strings` as an
    /// array, `zap.Reflect(nil)` as `null`.
    #[test]
    fn record_key_values_render_as_zap_fields() {
        let error = std::io::Error::other("region not found");
        let urls = vec![
            "http://127.0.0.1:2379".to_owned(),
            "http://[::1]:2379".to_owned(),
        ];
        let key_values: &[(&str, ::log::kv::Value<'_>)] = &[
            ("storeID", ::log::kv::Value::from(5_u64)),
            ("attempt", ::log::kv::Value::from(-2_i64)),
            ("error", ::log::kv::Value::from_display(&error)),
            ("addr", ::log::kv::Value::from("127.0.0.1:20160")),
            ("sorted-urls", ::log::kv::Value::from_serde(&urls)),
            ("canceled", ::log::kv::Value::from(false)),
            ("ratio", ::log::kv::Value::from(0.25_f64)),
            ("weight", ::log::kv::Value::from(f64::INFINITY)),
            ("txnStartTS", ::log::kv::Value::from_serde(&None::<u64>)),
        ];
        let record = ::log::Record::builder()
            .args(format_args!("batch recv loop met error"))
            .level(::log::Level::Warn)
            .key_values(&key_values)
            .build();
        let fields = zap_fields(record.key_values());
        let rendered = tidb_log::TextEncoder::default().encode_entry(
            &tidb_log::Entry {
                time: chrono::Local::now().fixed_offset(),
                level: level_of(record.level()),
                logger_name: String::new(),
                caller: Some(("batch.rs".to_owned(), 42)),
                message: record.args().to_string(),
                stack: String::new(),
            },
            &fields,
        );
        assert!(
            rendered.ends_with(
                "[WARN] [batch.rs:42] [\"batch recv loop met error\"] [storeID=5] \
                 [attempt=-2] [error=\"region not found\"] [addr=127.0.0.1:20160] \
                 [sorted-urls=\"[http://127.0.0.1:2379,\\\"http://[::1]:2379\\\"]\"] \
                 [canceled=false] [ratio=0.25] [weight=+Inf] [txnStartTS=null]\n"
            ),
            "{rendered}"
        );
    }
}
