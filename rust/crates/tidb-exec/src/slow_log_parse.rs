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

//! SEED of Go `pkg/sessionctx/variable`, covering `slow_log.go`'s rule
//! parsing: the per-field `Parse` half of `SlowLogRuleFieldAccessors`,
//! `ParseSlowLogFieldValue`, the rule-entry/rule-set grammar,
//! `ParseSessionSlowLogRules`/`ParseGlobalSlowLogRules`, and the canonical
//! `encodeRules` spelling with its CRC64/ECMA hash.
//!
//! The accessors' `Setter`/`Match` halves close over the live `SessionVars`,
//! statement context, and execution details; their composition contract is
//! already owned by `slow_log_match.rs` and `slow_log_threshold.rs`, and
//! their bindings land with the surfaces they read. `plan_digest` is
//! registered into the map by `pkg/executor/adapter_slow_log.go`'s `init`,
//! not by the variable package; both halves are one process here, so the
//! table includes it — which is also what makes the upstream count of 39
//! hold.
//!
//! Go scans rule text with `\s*(\w+)\s*:\s*([^,]+)\s*`; the scanner here
//! reproduces that regex's observable behavior, including its quiet skipping
//! of colon-less junk between matches. Conditions come out of a Go map, so
//! their order is unspecified there; sorted field order here is a
//! determinism refinement — visible in `encodeRules` output, whose order Go
//! itself does not fix.

use std::collections::BTreeMap;

use crate::slow_log_match::UNSET_CONNECTION_ID;
use crate::slow_log_rules::{GlobalSlowLogRules, SlowLogCondition, SlowLogRule, SlowLogRules};
use crate::slow_log_threshold::SlowLogValue;
use tidb_util::sqlescape::format_go_float64;

/// How one slow-log field's threshold text parses (the `Parse` member of Go
/// `SlowLogFieldAccessor`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlowLogParseKind {
    /// Go's private `parseInt64`: non-negative signed.
    Int64,
    /// Go's private `parseUint64`.
    Uint64,
    /// Go's private `parseFloat64`: finite and non-negative.
    Float64,
    /// Go's private `parseBool` (`strconv.ParseBool`).
    Bool,
    /// Go `ParseString`: the text itself.
    Text,
}

/// The rule-relevant fields and their parse kinds — Go
/// `SlowLogRuleFieldAccessors`' keys with each entry's `Parse` member,
/// including the `plan_digest` entry `adapter_slow_log.go` registers.
pub const SLOW_LOG_RULE_FIELD_PARSERS: [(&str, SlowLogParseKind); 39] = [
    ("conn_id", SlowLogParseKind::Uint64),
    ("session_alias", SlowLogParseKind::Text),
    ("db", SlowLogParseKind::Text),
    ("exec_retry_count", SlowLogParseKind::Uint64),
    ("query_time", SlowLogParseKind::Float64),
    ("parse_time", SlowLogParseKind::Float64),
    ("compile_time", SlowLogParseKind::Float64),
    ("rewrite_time", SlowLogParseKind::Float64),
    ("optimize_time", SlowLogParseKind::Float64),
    ("wait_ts", SlowLogParseKind::Float64),
    ("is_internal", SlowLogParseKind::Bool),
    ("digest", SlowLogParseKind::Text),
    ("num_cop_tasks", SlowLogParseKind::Int64),
    ("mem_max", SlowLogParseKind::Int64),
    ("disk_max", SlowLogParseKind::Int64),
    ("write_sql_response_total", SlowLogParseKind::Float64),
    ("succ", SlowLogParseKind::Bool),
    ("resource_group", SlowLogParseKind::Text),
    ("kv_total", SlowLogParseKind::Float64),
    ("pd_total", SlowLogParseKind::Float64),
    ("unpacked_bytes_sent_tikv_total", SlowLogParseKind::Int64),
    (
        "unpacked_bytes_received_tikv_total",
        SlowLogParseKind::Int64,
    ),
    (
        "unpacked_bytes_sent_tikv_cross_zone",
        SlowLogParseKind::Int64,
    ),
    (
        "unpacked_bytes_received_tikv_cross_zone",
        SlowLogParseKind::Int64,
    ),
    ("unpacked_bytes_sent_tiflash_total", SlowLogParseKind::Int64),
    (
        "unpacked_bytes_received_tiflash_total",
        SlowLogParseKind::Int64,
    ),
    (
        "unpacked_bytes_sent_tiflash_cross_zone",
        SlowLogParseKind::Int64,
    ),
    (
        "unpacked_bytes_received_tiflash_cross_zone",
        SlowLogParseKind::Int64,
    ),
    ("process_time", SlowLogParseKind::Float64),
    ("backoff_time", SlowLogParseKind::Float64),
    ("total_keys", SlowLogParseKind::Uint64),
    ("process_keys", SlowLogParseKind::Uint64),
    ("cop_mvcc_read_amplification", SlowLogParseKind::Float64),
    ("prewrite_time", SlowLogParseKind::Float64),
    ("commit_time", SlowLogParseKind::Float64),
    ("write_keys", SlowLogParseKind::Uint64),
    ("write_size", SlowLogParseKind::Uint64),
    ("prewrite_region", SlowLogParseKind::Uint64),
    ("plan_digest", SlowLogParseKind::Text),
];

/// Go `SlowLogConnIDStr`, lowercased as the map keys are.
pub const SLOW_LOG_CONN_ID_FIELD: &str = "conn_id";

/// The failures this parsing surface reports; the messages the upstream
/// tests pin are byte-exact.
#[derive(Clone, Debug, PartialEq)]
pub enum SlowLogParseError {
    /// Go `ParseSlowLogFieldValue`'s unknown-field error.
    UnknownField(String),
    /// Go's private `parseInt64`/`parseFloat64` non-negative guard.
    NonNegative(String),
    /// Go's private `parseFloat64` finite guard.
    NotFinite(String),
    /// A `strconv` failure, wrapped by the rule-entry parser.
    Syntax(String),
    /// Go `parseSlowLogRuleEntry`'s zero-match error.
    RuleFormat(String),
    /// Go `parseSlowLogRuleSet`'s rule-count limit.
    TooManyRules(usize),
    /// Go's ConnID rejection when `allowConnID` is false.
    ConnIdNotAllowed(String),
    /// Go's wrapper around a field-value failure.
    InvalidValue(String, String),
}

impl std::fmt::Display for SlowLogParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownField(name) => {
                write!(formatter, "unknown slow log field name:{name}")
            }
            Self::NonNegative(value) => {
                write!(
                    formatter,
                    "threshold value must be non-negative, got {value}"
                )
            }
            Self::NotFinite(value) => {
                write!(formatter, "threshold value must be finite, got {value}")
            }
            Self::Syntax(message) => formatter.write_str(message),
            Self::RuleFormat(raw) => write!(formatter, "invalid slow log rule format:{raw}"),
            Self::TooManyRules(count) => {
                write!(
                    formatter,
                    "invalid slow log rules count:{count}, limit is 10"
                )
            }
            Self::ConnIdNotAllowed(value) => write!(formatter, "do not allow ConnID value:{value}"),
            Self::InvalidValue(value, error) => {
                write!(
                    formatter,
                    "invalid slow log format, value:{value}, err:{error}"
                )
            }
        }
    }
}

impl std::error::Error for SlowLogParseError {}

fn parse_kind_of(field_name: &str) -> Option<SlowLogParseKind> {
    SLOW_LOG_RULE_FIELD_PARSERS
        .iter()
        .find(|(name, _)| *name == field_name)
        .map(|(_, kind)| *kind)
}

/// Go `ParseSlowLogFieldValue`: field names resolve case-insensitively.
pub fn parse_slow_log_field_value(
    field_name: &str,
    value: &str,
) -> Result<SlowLogValue, SlowLogParseError> {
    let lowered = field_name.to_lowercase();
    let Some(kind) = parse_kind_of(&lowered) else {
        return Err(SlowLogParseError::UnknownField(field_name.to_owned()));
    };
    match kind {
        SlowLogParseKind::Int64 => {
            let parsed: i64 = value
                .parse()
                .map_err(|_| SlowLogParseError::Syntax(format!("invalid int64 value: {value}")))?;
            if parsed < 0 {
                return Err(SlowLogParseError::NonNegative(parsed.to_string()));
            }
            Ok(SlowLogValue::Signed(parsed))
        }
        SlowLogParseKind::Uint64 => {
            // Go's strconv.ParseUint accepts no sign prefix at all, where
            // Rust's u64 parse tolerates a leading '+'.
            if value.starts_with('+') {
                return Err(SlowLogParseError::Syntax(format!(
                    "invalid uint64 value: {value}"
                )));
            }
            value
                .parse()
                .map(SlowLogValue::Unsigned)
                .map_err(|_| SlowLogParseError::Syntax(format!("invalid uint64 value: {value}")))
        }
        SlowLogParseKind::Float64 => {
            let parsed: f64 = value.parse().map_err(|_| {
                SlowLogParseError::Syntax(format!("invalid float64 value: {value}"))
            })?;
            if parsed.is_nan() || parsed.is_infinite() {
                return Err(SlowLogParseError::NotFinite(format_go_float64(parsed)));
            }
            if parsed < 0.0 {
                return Err(SlowLogParseError::NonNegative(format_go_float64(parsed)));
            }
            Ok(SlowLogValue::Float(parsed))
        }
        SlowLogParseKind::Bool => match value {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(SlowLogValue::Boolean(true)),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(SlowLogValue::Boolean(false)),
            _ => Err(SlowLogParseError::Syntax(format!(
                "invalid bool value: {value}"
            ))),
        },
        SlowLogParseKind::Text => Ok(SlowLogValue::Text(value.to_owned())),
    }
}

/// Go's `slowLogFieldRe` (`\s*(\w+)\s*:\s*([^,]+)\s*`) scan: every word run
/// followed by an optional-space colon and a non-empty run up to the next
/// comma. Colon-less words between matches are skipped, exactly as the regex
/// engine skips them.
fn scan_rule_fields(raw: &str) -> Vec<(String, String)> {
    let bytes = raw.as_bytes();
    let mut fields = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if !(bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_') {
            index += 1;
            continue;
        }
        let key_start = index;
        while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
        {
            index += 1;
        }
        let key_end = index;
        let mut after_spaces = index;
        while after_spaces < bytes.len() && (bytes[after_spaces] as char).is_whitespace() {
            after_spaces += 1;
        }
        if after_spaces < bytes.len() && bytes[after_spaces] == b':' {
            let value_start = after_spaces + 1;
            let mut value_end = value_start;
            while value_end < bytes.len() && bytes[value_end] != b',' {
                value_end += 1;
            }
            if value_end > value_start {
                fields.push((
                    raw[key_start..key_end].to_owned(),
                    raw[value_start..value_end].to_owned(),
                ));
                index = value_end;
                continue;
            }
            index = value_start;
        }
    }
    fields
}

/// Go's private `parseSlowLogRuleEntry`: one semicolon-separated rule.
/// Returns the bound connection ID (or [`UNSET_CONNECTION_ID`]) and the
/// rule; a blank entry is no rule at all.
fn parse_slow_log_rule_entry(
    raw_rule: &str,
    allow_conn_id: bool,
) -> Result<(i64, Option<SlowLogRule>), SlowLogParseError> {
    let mut conn_id = UNSET_CONNECTION_ID;
    let raw_rule = raw_rule.trim();
    if raw_rule.is_empty() {
        return Ok((conn_id, None));
    }

    let matches = scan_rule_fields(raw_rule);
    if matches.is_empty() {
        return Err(SlowLogParseError::RuleFormat(raw_rule.to_owned()));
    }
    let mut field_map: BTreeMap<String, SlowLogValue> = BTreeMap::new();
    for (raw_name, raw_value) in matches {
        let field_name = raw_name.trim().to_lowercase();
        let value = raw_value.trim();
        let trimmed = value.trim_matches(|c| c == '"' || c == '\'');
        let field_value = parse_slow_log_field_value(&field_name, trimmed).map_err(|error| {
            SlowLogParseError::InvalidValue(value.to_owned(), error.to_string())
        })?;

        if field_name == SLOW_LOG_CONN_ID_FIELD {
            if !allow_conn_id {
                return Err(SlowLogParseError::ConnIdNotAllowed(value.to_owned()));
            }
            if let SlowLogValue::Unsigned(id) = field_value {
                #[expect(clippy::cast_possible_wrap, reason = "Go's int64(uint64) wrap")]
                {
                    conn_id = id as i64;
                }
            }
        }
        field_map.insert(field_name, field_value);
    }

    let conditions = field_map
        .into_iter()
        .map(|(field, threshold)| SlowLogCondition { field, threshold })
        .collect();
    Ok((conn_id, Some(SlowLogRule { conditions })))
}

/// Go's private `parseSlowLogRuleSet`: semicolon-separated rules, at most
/// ten, grouped by their bound connection ID.
fn parse_slow_log_rule_set(
    raw_rules: &str,
    allow_conn_id: bool,
) -> Result<Option<BTreeMap<i64, SlowLogRules>>, SlowLogParseError> {
    let raw_rules = raw_rules.trim();
    if raw_rules.is_empty() {
        return Ok(None);
    }
    let rules: Vec<&str> = raw_rules.split(';').collect();
    if rules.len() > 10 {
        return Err(SlowLogParseError::TooManyRules(rules.len()));
    }

    let mut result: BTreeMap<i64, SlowLogRules> = BTreeMap::new();
    for raw in rules {
        let (conn_id, rule) = parse_slow_log_rule_entry(raw, allow_conn_id)?;
        let Some(rule) = rule else { continue };
        let grouped = result.entry(conn_id).or_default();
        for condition in &rule.conditions {
            grouped.fields.insert(condition.field.clone());
        }
        grouped.rules.push(rule);
    }
    Ok(Some(result))
}

/// Go `ParseSessionSlowLogRules`: the `UnsetConnID` group, with its raw form
/// re-encoded canonically; `None` when nothing binds to the session scope.
pub fn parse_session_slow_log_rules(
    raw_rules: &str,
) -> Result<Option<SlowLogRules>, SlowLogParseError> {
    let Some(mut grouped) = parse_slow_log_rule_set(raw_rules, false)? else {
        return Ok(None);
    };
    let Some(mut rules) = grouped.remove(&UNSET_CONNECTION_ID) else {
        return Ok(None);
    };
    rules.raw_rules = encode_rules(&rules);
    Ok(Some(rules))
}

/// The `%v` spelling Go writes for one threshold inside `encodeRules`.
fn threshold_text(threshold: &SlowLogValue) -> String {
    match threshold {
        SlowLogValue::Signed(value) => value.to_string(),
        SlowLogValue::Unsigned(value) => value.to_string(),
        SlowLogValue::Float(value) => format_go_float64(*value),
        SlowLogValue::Boolean(value) => value.to_string(),
        SlowLogValue::Text(value) => value.clone(),
    }
}

/// Go's private `encodeRules`: `field:value` pairs joined by commas, rules
/// joined by semicolons.
#[must_use]
pub fn encode_rules(rules: &SlowLogRules) -> String {
    if rules.rules.is_empty() {
        return String::new();
    }
    let mut encoded = String::new();
    for (rule_index, rule) in rules.rules.iter().enumerate() {
        for (condition_index, condition) in rule.conditions.iter().enumerate() {
            if condition_index > 0 {
                encoded.push(',');
            }
            encoded.push_str(&condition.field);
            encoded.push(':');
            encoded.push_str(&threshold_text(&condition.threshold));
        }
        if rule_index < rules.rules.len() - 1 {
            encoded.push(';');
        }
    }
    encoded
}

/// Go `crc64.ECMA` (the ECMA-182 reflected polynomial).
const CRC64_ECMA: u64 = 0xC96C_5795_D787_0F42;

/// Go `crc64.Checksum` over the ECMA table.
#[must_use]
pub fn crc64_ecma(data: &[u8]) -> u64 {
    let mut crc = !0_u64;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ CRC64_ECMA
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Go `ParseGlobalSlowLogRules`: every connection group, re-encoded and
/// hashed. Group order comes out of a Go map there; connection-ID order here.
pub fn parse_global_slow_log_rules(
    raw_rules: &str,
) -> Result<GlobalSlowLogRules, SlowLogParseError> {
    let rules_map = parse_slow_log_rule_set(raw_rules, true)?.unwrap_or_default();
    let raw_slice: Vec<String> = rules_map.values().map(encode_rules).collect();
    let raw_rules = raw_slice.join(";");
    let raw_rules_hash = crc64_ecma(raw_rules.as_bytes());
    Ok(GlobalSlowLogRules {
        raw_rules,
        raw_rules_hash,
        rules_map,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    // Go `TestParseSingleSlowLogField`, over the same 39-entry table.
    #[test]
    fn field_values_parse_by_their_kind() {
        assert_eq!(SLOW_LOG_RULE_FIELD_PARSERS.len(), 39);

        // int64 fields.
        assert_eq!(
            parse_slow_log_field_value("Mem_max", "123").unwrap(),
            SlowLogValue::Signed(123)
        );
        assert!(parse_slow_log_field_value("Mem_max", "abc").is_err());
        let error = parse_slow_log_field_value("Mem_max", "-100").unwrap_err();
        assert!(error.to_string().contains("non-negative"));
        assert_eq!(
            parse_slow_log_field_value("Mem_max", "0").unwrap(),
            SlowLogValue::Signed(0)
        );

        // uint64 fields.
        assert_eq!(
            parse_slow_log_field_value("Conn_ID", "456").unwrap(),
            SlowLogValue::Unsigned(456)
        );
        assert!(parse_slow_log_field_value("Conn_ID", "-1").is_err());

        // float64 fields.
        assert_eq!(
            parse_slow_log_field_value("Query_time", "1.234").unwrap(),
            SlowLogValue::Float(1.234)
        );
        assert_eq!(
            parse_slow_log_field_value("Query_time", "1.5e6").unwrap(),
            SlowLogValue::Float(1.5e6)
        );
        assert_eq!(
            parse_slow_log_field_value("cop_mvcc_read_amplification", "10.5").unwrap(),
            SlowLogValue::Float(10.5)
        );
        assert!(parse_slow_log_field_value("cop_mvcc_read_amplification", "abc").is_err());
        assert!(parse_slow_log_field_value("Query_time", "abc").is_err());
        let error = parse_slow_log_field_value("Query_time", "-1.5").unwrap_err();
        assert!(error.to_string().contains("non-negative"));
        let error = parse_slow_log_field_value("cop_mvcc_read_amplification", "-0.1").unwrap_err();
        assert!(error.to_string().contains("non-negative"));
        assert_eq!(
            parse_slow_log_field_value("Query_time", "0").unwrap(),
            SlowLogValue::Float(0.0)
        );

        // string fields.
        assert_eq!(
            parse_slow_log_field_value("DB", "testdb").unwrap(),
            SlowLogValue::Text("testdb".to_owned())
        );

        // bool fields.
        assert_eq!(
            parse_slow_log_field_value("Succ", "true").unwrap(),
            SlowLogValue::Boolean(true)
        );
        assert_eq!(
            parse_slow_log_field_value("Succ", "false").unwrap(),
            SlowLogValue::Boolean(false)
        );
        assert!(parse_slow_log_field_value("Succ", "notabool").is_err());

        // unknown field.
        let error = parse_slow_log_field_value("NonExistField", "xxx").unwrap_err();
        assert!(error.to_string().contains("unknown slow log field name"));
    }

    fn condition_of<'r>(rules: &'r SlowLogRules, index: usize, field: &str) -> &'r SlowLogValue {
        rules.rules[index]
            .conditions
            .iter()
            .find(|condition| condition.field == field)
            .map(|condition| &condition.threshold)
            .unwrap_or_else(|| panic!("{field} in rule {index}"))
    }

    // Go `TestParseSessionSlowLogRules`.
    #[test]
    fn session_rules_parse_group_and_reject_conn_id() {
        let error = parse_session_slow_log_rules(
            "Conn_ID: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1",
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "do not allow ConnID value:123");

        let rules = parse_session_slow_log_rules(
            "Exec_retry_count: 10, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rules.rules.len(), 1);
        assert_eq!(
            condition_of(&rules, 0, "exec_retry_count"),
            &SlowLogValue::Unsigned(10)
        );
        assert_eq!(
            condition_of(&rules, 0, "db"),
            &SlowLogValue::Text("db1".to_owned())
        );
        assert_eq!(
            condition_of(&rules, 0, "succ"),
            &SlowLogValue::Boolean(true)
        );
        assert_eq!(
            condition_of(&rules, 0, "query_time"),
            &SlowLogValue::Float(0.5276)
        );
        let expected_fields: BTreeSet<String> = [
            "exec_retry_count",
            "db",
            "succ",
            "query_time",
            "resource_group",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
        assert_eq!(rules.fields, expected_fields);
        // The canonical raw form is re-encoded, sorted by field.
        assert_eq!(
            rules.raw_rules,
            "db:db1,exec_retry_count:10,query_time:0.5276,resource_group:rg1,succ:true"
        );

        // A trailing semicolon changes nothing.
        let trailing = parse_session_slow_log_rules(
            "Exec_retry_count: 10, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;",
        )
        .unwrap()
        .unwrap();
        assert_eq!(trailing.fields, expected_fields);
        assert_eq!(trailing.rules.len(), 1);

        // Two rules OR together.
        let two = parse_session_slow_log_rules(
            "Exec_retry_count: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;\
             Exec_retry_count: 124, DB: db2, Succ: false, Query_time: 1.5276",
        )
        .unwrap()
        .unwrap();
        assert_eq!(two.rules.len(), 2);
        assert_eq!(
            condition_of(&two, 0, "exec_retry_count"),
            &SlowLogValue::Unsigned(123)
        );
        assert_eq!(
            condition_of(&two, 1, "exec_retry_count"),
            &SlowLogValue::Unsigned(124)
        );
        assert_eq!(condition_of(&two, 1, "succ"), &SlowLogValue::Boolean(false));
        assert_eq!(two.fields, expected_fields);

        // Blank input parses to nothing.
        assert!(parse_session_slow_log_rules("  ").unwrap().is_none());
        assert!(parse_session_slow_log_rules("  ; ; ").unwrap().is_none());

        // Eleven rules exceed the limit.
        let long_rules = "Conn_ID:1;".repeat(11);
        let error = parse_session_slow_log_rules(&long_rules).unwrap_err();
        assert!(error.to_string().contains("invalid slow log rules count"));
    }

    // Quoted values and the regex's junk-skipping.
    #[test]
    fn rule_text_tolerates_quotes_and_junk_words() {
        let rules = parse_session_slow_log_rules(r#"DB: "db1", Succ: 'true'"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            condition_of(&rules, 0, "db"),
            &SlowLogValue::Text("db1".to_owned())
        );
        assert_eq!(
            condition_of(&rules, 0, "succ"),
            &SlowLogValue::Boolean(true)
        );

        // A colon-less word between fields is skipped, as Go's regex skips it.
        let rules = parse_session_slow_log_rules("junk DB: db1")
            .unwrap()
            .unwrap();
        assert_eq!(
            condition_of(&rules, 0, "db"),
            &SlowLogValue::Text("db1".to_owned())
        );

        // No field at all is a format error.
        let error = parse_session_slow_log_rules("no colon here").unwrap_err();
        assert!(error.to_string().contains("invalid slow log rule format"));
    }

    // Go `ParseGlobalSlowLogRules`: ConnID-bound and unbound groups, the
    // canonical raw string, and its CRC64/ECMA hash.
    #[test]
    fn global_rules_group_by_connection() {
        let global = parse_global_slow_log_rules("Conn_ID: 7, Query_time: 1.5; DB: db1").unwrap();
        assert_eq!(global.rules_map.len(), 2);
        assert!(global.rules_map.contains_key(&UNSET_CONNECTION_ID));
        assert!(global.rules_map.contains_key(&7));
        assert_eq!(global.raw_rules, "db:db1;conn_id:7,query_time:1.5");
        assert_eq!(
            global.raw_rules_hash,
            crc64_ecma(global.raw_rules.as_bytes())
        );

        // Empty input is an empty, hashed container.
        let empty = parse_global_slow_log_rules("").unwrap();
        assert!(empty.rules_map.is_empty());
        assert_eq!(empty.raw_rules, "");
        assert_eq!(empty.raw_rules_hash, crc64_ecma(b""));
    }

    // The CRC64/ECMA implementation against Go's own check value: Go
    // `crc64.Checksum([]byte("123456789"), crc64.MakeTable(crc64.ECMA))`.
    #[test]
    fn crc64_matches_the_ecma_check_value() {
        assert_eq!(crc64_ecma(b"123456789"), 0x995D_C9BB_DF19_39FA);
        assert_eq!(crc64_ecma(b""), 0);
    }
}
