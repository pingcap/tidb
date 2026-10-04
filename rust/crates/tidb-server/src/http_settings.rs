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

//! Go SettingsHandler, using the existing process and durable SQL owners.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tidb_config::config_tree::config::{get_global_config, update_global};
use tidb_session::GlobalSysvars;

// Go applies fields in source order. Serialize an individual handler invocation
// so its config publication and live deadlock policy cannot cross another POST.
static UPDATE: Mutex<()> = Mutex::new(());
type GlobalSetter = Arc<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>;

/// Shared process variables and the node's internal-session GLOBAL writer.
#[derive(Clone)]
pub struct Settings {
    globals: GlobalSysvars,
    set_global: GlobalSetter,
}

impl Settings {
    /// Binds process variables and the existing durable GLOBAL writer.
    pub fn new(globals: GlobalSysvars, set_global: GlobalSetter) -> Self {
        Self {
            globals,
            set_global,
        }
    }

    /// Reads canonical configuration at request time, including SQL updates.
    pub fn read(&self) -> Result<String, String> {
        serde_json::to_string_pretty(get_global_config().as_ref()).map_err(|e| e.to_string())
    }

    /// Applies fields in Go order, retaining preceding effects on error.
    pub fn apply(&self, form: &HashMap<String, String>) -> Result<(), String> {
        let _guard = UPDATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let get = |name: &str| form.get(name).filter(|s| !s.is_empty());
        if let Some(level) = get("log_level") {
            tidb_util::logutil::set_level(level)?;
            update_global(|config| config.log.level.clone_from(level));
        }
        if let Some(value) = get("tidb_general_log") {
            self.globals
                .set_instance("tidb_general_log", bit(value)?.into())
                .map_err(|e| format!("{e:?}"))?;
        }
        for name in ["tidb_enable_async_commit", "tidb_enable_1pc"] {
            if let Some(value) = get(name) {
                (self.set_global)(name, bit(value)?)?;
            }
        }
        if let Some(value) = get("ddl_slow_threshold") {
            let value = integer(value)?;
            if value > 0 {
                // HTTP stores uint32 directly; SQL's signed range clamp does
                // not apply to this Go handler. Reuse the raw instance slot.
                self.globals
                    .set_startup("ddl_slow_threshold", (value as u32).to_string());
            }
        }
        if let Some(value) = get("check_mb4_value_in_utf8") {
            self.globals
                .set_instance("tidb_check_mb4_value_in_utf8", bit(value)?.into())
                .map_err(|e| format!("{e:?}"))?;
        }
        if let Some(value) = get("deadlock_history_capacity") {
            let capacity = integer(value).map_err(|_| "illegal argument")?;
            if !(0..=10000).contains(&capacity) {
                return Err(
                    "deadlock_history_capacity out of range, should be in 0 to 10000".into(),
                );
            }
            update_global(|config| {
                config.pessimistic_txn.deadlock_history_capacity = capacity as usize
            });
            tidb_executor::deadlock_history::GLOBAL_DEADLOCK_HISTORY.resize(capacity as usize);
        }
        if let Some(value) = get("deadlock_history_collect_retryable") {
            let value = match value.as_str() {
                "1" | "t" | "T" | "true" | "TRUE" | "True" => true,
                "0" | "f" | "F" | "false" | "FALSE" | "False" => false,
                _ => return Err("illegal argument".into()),
            };
            update_global(|config| {
                config.pessimistic_txn.deadlock_history_collect_retryable = value
            });
            tidb_exec::configure_deadlock_history(
                get_global_config()
                    .pessimistic_txn
                    .deadlock_history_capacity as usize,
                value,
            );
        }
        if let Some(value) = get("tidb_enable_mutation_checker") {
            (self.set_global)("tidb_enable_mutation_checker", bit(value)?)?;
        }
        if let Some(value) = get("transaction_summary_capacity") {
            let capacity = integer(value).map_err(|_| "illegal argument")?;
            if !(0..=5000).contains(&capacity) {
                return Err(
                    "transaction_summary_capacity out of range, should be in 0 to 5000".into(),
                );
            }
            update_global(|config| {
                config.trx_summary.transaction_summary_capacity = capacity as usize
            });
            tidb_exec::txn_summary::RECORDER.resize(capacity as usize);
        }
        if let Some(value) = get("transaction_id_digest_min_duration") {
            let duration = integer(value).map_err(|_| "illegal argument")?;
            if !(0..=2147483647).contains(&duration) {
                return Err(
                    "transaction_id_digest_min_duration out of range, should be in 0 to 2147483647"
                        .into(),
                );
            }
            update_global(|config| {
                config.trx_summary.transaction_id_digest_min_duration = duration as usize
            });
            tidb_exec::txn_summary::RECORDER
                .set_min_duration(std::time::Duration::from_millis(duration as u64));
        }
        Ok(())
    }
}

fn bit(value: &str) -> Result<&'static str, String> {
    match value {
        "0" => Ok("OFF"),
        "1" => Ok("ON"),
        _ => Err("illegal argument".into()),
    }
}
fn integer(value: &str) -> Result<i64, String> {
    value.parse::<i64>().map_err(|error| {
        let reason = match error.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        format!("strconv.Atoi: parsing {value:?}: {reason}")
    })
}

/// net/url ParseQuery: first duplicate wins; a POST body precedes query values.
/// Reject malformed escapes and unescaped semicolons before applying any fields.
pub(crate) fn parse_form(query: &str, body: &str) -> Result<HashMap<String, String>, String> {
    fn decode(value: &str) -> Result<String, String> {
        let mut bytes = Vec::with_capacity(value.len());
        let mut input = value.bytes();
        while let Some(byte) = input.next() {
            bytes.push(match byte {
                b'+' => b' ',
                b'%' => {
                    let a = input.next().and_then(|v| (v as char).to_digit(16));
                    let b = input.next().and_then(|v| (v as char).to_digit(16));
                    match (a, b) {
                        (Some(a), Some(b)) => (a * 16 + b) as u8,
                        _ => return Err("invalid URL escape".into()),
                    }
                }
                b => b,
            });
        }
        String::from_utf8(bytes).map_err(|_| "invalid UTF-8 form value".into())
    }
    let mut form = HashMap::new();
    for data in [body, query] {
        for pair in data.split('&').filter(|p| !p.is_empty()) {
            if pair.contains(';') {
                return Err("invalid semicolon separator in query".into());
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            form.entry(decode(key)?).or_insert(decode(value)?);
        }
    }
    Ok(form)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_settings_share_owners_and_preserve_go_field_order() {
        let original = get_global_config();
        struct Restore(Arc<tidb_config::config_tree::Config>, String);
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
                let _ = tidb_util::logutil::set_level(&self.1);
                tidb_exec::configure_deadlock_history(
                    self.0.pessimistic_txn.deadlock_history_capacity as usize,
                    self.0.pessimistic_txn.deadlock_history_collect_retryable,
                );
            }
        }
        let _restore = Restore(original.clone(), original.log.level.clone());
        let globals = GlobalSysvars::new();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::clone(&writes);
        let settings = Settings::new(
            globals.clone(),
            Arc::new(move |key, value| {
                calls
                    .lock()
                    .unwrap()
                    .push((key.to_owned(), value.to_owned()));
                if value == "OFF" {
                    Err("durable write failed".into())
                } else {
                    Ok(())
                }
            }),
        );
        let form = parse_form("", "log_level=error&tidb_general_log=1&tidb_enable_async_commit=1&tidb_enable_1pc=1&tidb_enable_mutation_checker=1&ddl_slow_threshold=200&check_mb4_value_in_utf8=0&deadlock_history_capacity=5&deadlock_history_collect_retryable=true").unwrap();
        settings.apply(&form).unwrap();
        assert_eq!(format!("{:?}", tidb_util::logutil::get_level()), "Error");
        assert_eq!(get_global_config().log.level, "error");
        assert_eq!(globals.get("tidb_general_log").unwrap(), "ON");
        assert_eq!(globals.get("ddl_slow_threshold").unwrap(), "200");
        assert_eq!(globals.get("tidb_check_mb4_value_in_utf8").unwrap(), "OFF");
        assert!(!get_global_config().instance.check_mb4_value_in_utf8.load());
        assert_eq!(
            get_global_config()
                .pessimistic_txn
                .deadlock_history_capacity,
            5
        );
        assert!(
            get_global_config()
                .pessimistic_txn
                .deadlock_history_collect_retryable
        );
        assert_eq!(
            *writes.lock().unwrap(),
            [
                ("tidb_enable_async_commit".into(), "ON".into()),
                ("tidb_enable_1pc".into(), "ON".into()),
                ("tidb_enable_mutation_checker".into(), "ON".into())
            ]
        );
        // Earlier settings persist on a later parse/validation/storage failure.
        assert!(settings
            .apply(
                &parse_form(
                    "",
                    "log_level=warn&tidb_general_log=bad&check_mb4_value_in_utf8=1"
                )
                .unwrap()
            )
            .is_err());
        assert_eq!(get_global_config().log.level, "warn");
        assert_eq!(globals.get("tidb_check_mb4_value_in_utf8").unwrap(), "OFF");
        assert_eq!(
            settings
                .apply(
                    &parse_form(
                        "",
                        "tidb_general_log=0&tidb_enable_async_commit=0&check_mb4_value_in_utf8=1"
                    )
                    .unwrap()
                )
                .unwrap_err(),
            "durable write failed"
        );
        assert_eq!(globals.get("tidb_general_log").unwrap(), "OFF");
        assert_eq!(globals.get("tidb_check_mb4_value_in_utf8").unwrap(), "OFF");
        for form in [
            "deadlock_history_capacity=-1",
            "deadlock_history_capacity=10001",
            "deadlock_history_capacity=no",
            "deadlock_history_collect_retryable=TrUe",
            "tidb_general_log=true",
            "check_mb4_value_in_utf8=2",
            "transaction_summary_capacity=-1",
            "transaction_summary_capacity=5001",
            "transaction_id_digest_min_duration=-1",
            "transaction_id_digest_min_duration=2147483648",
        ] {
            assert!(
                settings.apply(&parse_form("", form).unwrap()).is_err(),
                "{form}"
            );
        }
        // Go Atoi -> positive check -> uint32 store, with no SQL clamp.
        settings
            .apply(&parse_form("", "ddl_slow_threshold=4294967295").unwrap())
            .unwrap();
        assert_eq!(globals.get("ddl_slow_threshold").unwrap(), "4294967295");
        settings
            .apply(&parse_form("", "ddl_slow_threshold=-1").unwrap())
            .unwrap();
        assert_eq!(globals.get("ddl_slow_threshold").unwrap(), "4294967295");
        settings
            .apply(&parse_form("", "ddl_slow_threshold=4294967296").unwrap())
            .unwrap();
        assert_eq!(globals.get("ddl_slow_threshold").unwrap(), "0");
        settings
            .apply(&parse_form("", "unknown=ignored&log_level=").unwrap())
            .unwrap();
        // SQL and HTTP now publish/read the same canonical UTF-8 control.
        globals
            .set_instance("tidb_check_mb4_value_in_utf8", "ON".into())
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&settings.read().unwrap()).unwrap();
        assert_eq!(json["instance"]["tidb_check_mb4_value_in_utf8"], "true");
    }

    #[test]
    fn transaction_observation_batch_settings_share_recorder_and_ordered_failures() {
        if crate::isolate_process_globals() {
            return;
        }
        use tidb_exec::txn_summary::RECORDER;
        let settings = Settings::new(GlobalSysvars::default(), Arc::new(|_, _| Ok(())));
        settings
            .apply(
                &parse_form(
                    "",
                    "transaction_summary_capacity=3&transaction_id_digest_min_duration=0",
                )
                .unwrap(),
            )
            .unwrap();
        RECORDER.on_transaction_end(1 << 18, vec!["alpha".into()]);
        assert_eq!(RECORDER.rows().len(), 1);
        assert_eq!(
            get_global_config().trx_summary.transaction_summary_capacity,
            3
        );
        assert_eq!(
            get_global_config()
                .trx_summary
                .transaction_id_digest_min_duration,
            0
        );
        let error = settings
            .apply(
                &parse_form(
                    "",
                    "transaction_summary_capacity=0&transaction_id_digest_min_duration=-1",
                )
                .unwrap(),
            )
            .unwrap_err();
        assert_eq!(
            error,
            "transaction_id_digest_min_duration out of range, should be in 0 to 2147483647"
        );
        assert!(RECORDER.rows().is_empty());
        assert_eq!(
            get_global_config().trx_summary.transaction_summary_capacity,
            0
        );
        for (field, max) in [
            ("transaction_summary_capacity", 5000_i64),
            ("transaction_id_digest_min_duration", 2147483647),
        ] {
            for value in [0, max] {
                settings
                    .apply(&parse_form("", &format!("{field}={value}")).unwrap())
                    .unwrap();
            }
            for value in ["invalid".to_owned(), (max + 1).to_string(), "-1".to_owned()] {
                assert!(settings
                    .apply(&parse_form("", &format!("{field}={value}")).unwrap())
                    .is_err());
            }
        }
    }

    #[test]
    fn runtime_settings_parse_form_uses_body_then_query_and_first_value() {
        let form = parse_form(
            "x=query&only=query",
            "x=body&x=later&blank=&escaped=%26%3D&space=a+b",
        )
        .unwrap();
        assert_eq!(form["x"], "body");
        assert_eq!(form["only"], "query");
        assert_eq!(form["blank"], "");
        assert_eq!(form["escaped"], "&=");
        assert_eq!(form["space"], "a b");
        assert_eq!(
            integer("bad").unwrap_err(),
            "strconv.Atoi: parsing \"bad\": invalid syntax"
        );
        assert_eq!(
            integer("9223372036854775808").unwrap_err(),
            "strconv.Atoi: parsing \"9223372036854775808\": value out of range"
        );
        for data in ["x=%", "x=%x0", "x=a;b"] {
            assert!(parse_form(data, "valid=earlier").is_err());
        }
    }
}
