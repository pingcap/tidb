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

//! Go `pkg/sessionctx/variable/slow_log.go`'s rule syntax for
//! `tidb_slow_log_rules`: `field:value[,field:value...][;rule...]`, at most ten
//! rules, each field one of [`FIELDS`] with a value of that field's type.
//!
//! The parse and encode half (`parseSlowLogRuleSet`, `encodeRules`) is what a
//! `SET` accepts and what a session reads back; the match half (the
//! accessors' `Match`, `ShouldWriteSlowLog` in `adapter_slow_log.go`) is what
//! decides whether a finished statement is written to the slow log.

/// The type a field's threshold parses as (Go's `parseUint64`, `parseInt64`,
/// `parseFloat64`, `parseBool` and `ParseString`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Uint,
    Int,
    Float,
    Bool,
    Text,
}

/// `SlowLogRuleFieldAccessors`: the lower-cased field names and their types.
const FIELDS: &[(&str, Kind)] = &[
    ("conn_id", Kind::Uint),
    ("session_alias", Kind::Text),
    ("db", Kind::Text),
    ("exec_retry_count", Kind::Uint),
    ("query_time", Kind::Float),
    ("parse_time", Kind::Float),
    ("compile_time", Kind::Float),
    ("rewrite_time", Kind::Float),
    ("optimize_time", Kind::Float),
    ("wait_ts", Kind::Float),
    ("is_internal", Kind::Bool),
    ("digest", Kind::Text),
    ("num_cop_tasks", Kind::Int),
    ("mem_max", Kind::Int),
    ("disk_max", Kind::Int),
    ("write_sql_response_total", Kind::Float),
    ("succ", Kind::Bool),
    ("resource_group", Kind::Text),
    ("kv_total", Kind::Float),
    ("pd_total", Kind::Float),
    ("unpacked_bytes_sent_tikv_total", Kind::Int),
    ("unpacked_bytes_received_tikv_total", Kind::Int),
    ("unpacked_bytes_sent_tikv_cross_zone", Kind::Int),
    ("unpacked_bytes_received_tikv_cross_zone", Kind::Int),
    ("unpacked_bytes_sent_tiflash_total", Kind::Int),
    ("unpacked_bytes_received_tiflash_total", Kind::Int),
    ("unpacked_bytes_sent_tiflash_cross_zone", Kind::Int),
    ("unpacked_bytes_received_tiflash_cross_zone", Kind::Int),
    ("process_time", Kind::Float),
    ("backoff_time", Kind::Float),
    ("total_keys", Kind::Uint),
    ("process_keys", Kind::Uint),
    ("cop_mvcc_read_amplification", Kind::Float),
    ("prewrite_time", Kind::Float),
    ("commit_time", Kind::Float),
    ("write_keys", Kind::Uint),
    ("write_size", Kind::Uint),
    ("prewrite_region", Kind::Uint),
    // Registered by the executor package's `init` (`adapter_slow_log.go`).
    ("plan_digest", Kind::Text),
];

/// One parsed threshold, printed by `encodeRules` as Go's `%v`.
#[derive(Clone, Debug, PartialEq)]
enum Threshold {
    Uint(u64),
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

impl Threshold {
    fn encode(&self) -> String {
        match self {
            Self::Uint(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            // `%v` of a float64 is `%g` with the shortest representation.
            Self::Float(value) => tidb_datatype::go_strconv::format_float(*value, b'g', -1, 64),
            Self::Bool(value) => value.to_string(),
            Self::Text(value) => value.clone(),
        }
    }
}

/// Go's `strconv` error text for a failed parse.
fn syntax_error(function: &str, text: &str) -> String {
    format!("strconv.{function}: parsing {text:?}: invalid syntax")
}

fn range_error(function: &str, text: &str) -> String {
    format!("strconv.{function}: parsing {text:?}: value out of range")
}

fn parse_uint(text: &str) -> Result<Threshold, String> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(syntax_error("ParseUint", text));
    }
    text.parse::<u64>()
        .map(Threshold::Uint)
        .map_err(|_| range_error("ParseUint", text))
}

fn parse_int(text: &str) -> Result<Threshold, String> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(syntax_error("ParseInt", text));
    }
    let value = text
        .parse::<i64>()
        .map_err(|_| range_error("ParseInt", text))?;
    if value < 0 {
        return Err(format!("threshold value must be non-negative, got {value}"));
    }
    Ok(Threshold::Int(value))
}

fn parse_float(text: &str) -> Result<Threshold, String> {
    let value = text
        .parse::<f64>()
        .map_err(|_| syntax_error("ParseFloat", text))?;
    let spelled_infinity = text
        .trim_start_matches(['+', '-'])
        .to_ascii_lowercase()
        .starts_with("inf");
    if value.is_infinite() && !spelled_infinity {
        return Err(range_error("ParseFloat", text));
    }
    if value.is_nan() || value.is_infinite() {
        return Err(format!(
            "threshold value must be finite, got {}",
            tidb_datatype::go_strconv::format_float(value, b'g', -1, 64)
        ));
    }
    if value < 0.0 {
        return Err(format!(
            "threshold value must be non-negative, got {}",
            tidb_datatype::go_strconv::format_float(value, b'g', -1, 64)
        ));
    }
    Ok(Threshold::Float(value))
}

fn parse_bool(text: &str) -> Result<Threshold, String> {
    match text {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(Threshold::Bool(true)),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(Threshold::Bool(false)),
        _ => Err(syntax_error("ParseBool", text)),
    }
}

/// Go `ParseSlowLogFieldValue`.
fn parse_field_value(field: &str, value: &str) -> Result<Threshold, String> {
    let Some((_, kind)) = FIELDS
        .iter()
        .find(|(name, _)| *name == field.to_lowercase())
    else {
        return Err(format!("unknown slow log field name:{field}"));
    };
    match kind {
        Kind::Uint => parse_uint(value),
        Kind::Int => parse_int(value),
        Kind::Float => parse_float(value),
        Kind::Bool => parse_bool(value),
        Kind::Text => Ok(Threshold::Text(value.to_owned())),
    }
}

fn is_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

/// `slowLogFieldRe.FindAllStringSubmatch`: every leftmost match of
/// `\s*(\w+)\s*:\s*([^,]+)\s*`, as its (field, value) groups.
fn field_matches(rule: &str) -> Vec<(String, String)> {
    let chars = rule.chars().collect::<Vec<_>>();
    let mut matches = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let mut at = start;
        while at < chars.len() && chars[at].is_whitespace() {
            at += 1;
        }
        let field_start = at;
        while at < chars.len() && is_word(chars[at]) {
            at += 1;
        }
        let field_end = at;
        if field_end == field_start {
            start += 1;
            continue;
        }
        while at < chars.len() && chars[at].is_whitespace() {
            at += 1;
        }
        if at >= chars.len() || chars[at] != ':' {
            start += 1;
            continue;
        }
        at += 1;
        let colon_end = at;
        while at < chars.len() && chars[at].is_whitespace() {
            at += 1;
        }
        let mut value_start = at;
        while at < chars.len() && chars[at] != ',' {
            at += 1;
        }
        if at == value_start {
            // `[^,]+` needs a character: the whitespace the `\s*` took may be
            // given back to it, otherwise this start does not match.
            if value_start == colon_end {
                start += 1;
                continue;
            }
            value_start -= 1;
        }
        matches.push((
            chars[field_start..field_end].iter().collect::<String>(),
            chars[value_start..at].iter().collect::<String>(),
        ));
        start = at;
    }
    matches
}

/// One rule: its conditions by lower-cased field name, in the order each
/// field first appeared (Go ranges over a map, so its order is unspecified).
struct Rule {
    conditions: Vec<(String, Threshold)>,
    conn_id: Option<u64>,
}

/// Go `parseSlowLogRuleEntry`.
fn parse_rule(raw: &str, allow_conn_id: bool) -> Result<Option<Rule>, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let matches = field_matches(raw);
    if matches.is_empty() {
        return Err(format!("invalid slow log rule format:{raw}"));
    }
    let mut conditions: Vec<(String, Threshold)> = Vec::new();
    let mut conn_id = None;
    for (field, value) in matches {
        let field = field.trim().to_lowercase();
        let value = value.trim();
        let threshold = parse_field_value(&field, value.trim_matches(['"', '\'']))
            .map_err(|error| format!("invalid slow log format, value:{value}, err:{error}"))?;
        if field == "conn_id" {
            if !allow_conn_id {
                return Err(format!("do not allow ConnID value:{value}"));
            }
            if let Threshold::Uint(id) = threshold {
                conn_id = Some(id);
            }
        }
        match conditions.iter_mut().find(|(name, _)| *name == field) {
            Some((_, existing)) => *existing = threshold,
            None => conditions.push((field, threshold)),
        }
    }
    Ok(Some(Rule {
        conditions,
        conn_id,
    }))
}

/// Go `parseSlowLogRuleSet`: the rules by ConnID (`None` for an unbound
/// rule), in source order.
fn parse_rule_set(raw: &str, allow_conn_id: bool) -> Result<Vec<(Option<u64>, Vec<Rule>)>, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let rules = raw.split(';').collect::<Vec<_>>();
    if rules.len() > 10 {
        return Err(format!(
            "invalid slow log rules count:{}, limit is 10",
            rules.len()
        ));
    }
    let mut grouped: Vec<(Option<u64>, Vec<Rule>)> = Vec::new();
    for rule in rules {
        let Some(rule) = parse_rule(rule, allow_conn_id)? else {
            continue;
        };
        match grouped.iter_mut().find(|(id, _)| *id == rule.conn_id) {
            Some((_, group)) => group.push(rule),
            None => grouped.push((rule.conn_id, vec![rule])),
        }
    }
    Ok(grouped)
}

/// Go `encodeRules`.
fn encode_rules(rules: &[Rule]) -> String {
    rules
        .iter()
        .map(|rule| {
            rule.conditions
                .iter()
                .map(|(field, threshold)| format!("{field}:{}", threshold.encode()))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// `SetSession`'s `ParseSessionSlowLogRules`: validates the value and returns
/// what `GetSession` reads back (the encoded unbound rules, or empty).
///
/// # Errors
/// Go's message for a malformed rule set.
pub fn session_rules(raw: &str) -> Result<String, String> {
    let grouped = parse_rule_set(raw, false)?;
    Ok(grouped
        .iter()
        .find(|(id, _)| id.is_none())
        .map(|(_, rules)| encode_rules(rules))
        .unwrap_or_default())
}

/// `SetGlobal`'s `ParseGlobalSlowLogRules`: validates the value and returns
/// its `RawRules`, the groups' encodings joined by `;`.
///
/// # Errors
/// Go's message for a malformed rule set.
pub fn global_rules(raw: &str) -> Result<String, String> {
    let grouped = parse_rule_set(raw, true)?;
    Ok(grouped
        .iter()
        .map(|(_, rules)| encode_rules(rules))
        .collect::<Vec<_>>()
        .join(";"))
}

/// The values Go's `SlowLogRuleFieldAccessors[...].Match` functions compare
/// a threshold against, read from the session and the statement's
/// `SlowQueryLogItems` once the statement finished.
#[derive(Clone, Debug, Default)]
pub struct SlowLogRuleValues {
    /// Go `SessionVars.ConnectionID`.
    pub conn_id: u64,
    /// Go `SessionVars.SessionAlias`.
    pub session_alias: String,
    /// Go `SessionVars.CurrentDB`.
    pub db: String,
    /// Go `StmtCtx.ExecRetryCount`.
    pub exec_retry_count: u64,
    /// Go `GetTotalCostDuration`.
    pub query_time: std::time::Duration,
    /// Go `DurationParse`.
    pub parse_time: std::time::Duration,
    /// Go `DurationCompile`.
    pub compile_time: std::time::Duration,
    /// Go `RewritePhaseInfo.DurationRewrite`.
    pub rewrite_time: std::time::Duration,
    /// Go `DurationOptimizer.Total`.
    pub optimize_time: std::time::Duration,
    /// Go `DurationWaitTS`.
    pub wait_ts: std::time::Duration,
    /// Go `InRestrictedSQL`.
    pub is_internal: bool,
    /// Go `StmtCtx.SQLDigest()`.
    pub digest: String,
    /// Go `CopTasksDetails().NumCopTasks`, `None` for no cop task.
    pub num_cop_tasks: Option<i64>,
    /// Go `MemTracker.MaxConsumed()`.
    pub mem_max: i64,
    /// Go `DiskTracker.MaxConsumed()`.
    pub disk_max: i64,
    /// Go `StmtExecDetails.WriteSQLRespDuration`.
    pub write_sql_response_total: std::time::Duration,
    /// Go `StmtCtx.ExecSuccess`.
    pub succ: bool,
    /// Go `StmtCtx.ResourceGroupName`.
    pub resource_group: String,
    /// Go `ctx.Value(util.ExecDetailsKey)`'s snapshot, `None` without one.
    pub kv_exec_detail: Option<crate::slow_log::KvExecDetails>,
    /// Go `StmtCtx.GetExecDetails()`.
    pub exec_detail: tidb_exec::exec_details::ExecDetails,
    /// Go `GetPlanDigest`'s digest.
    pub plan_digest: String,
}

/// Go `matchZero`: a missing detail matches only a zero threshold.
fn match_zero(threshold: &Threshold) -> bool {
    match threshold {
        Threshold::Uint(value) => *value == 0,
        Threshold::Int(value) => *value == 0,
        Threshold::Float(value) => *value == 0.0,
        Threshold::Bool(_) | Threshold::Text(_) => false,
    }
}

/// Go `matchGE` over a float64 value: only a float64 threshold matches.
fn match_ge_float(threshold: &Threshold, value: f64) -> bool {
    matches!(threshold, Threshold::Float(threshold) if value >= *threshold)
}

/// Go `matchGE` over an int64 value.
fn match_ge_int(threshold: &Threshold, value: i64) -> bool {
    matches!(threshold, Threshold::Int(threshold) if value >= *threshold)
}

/// Go `matchGE` over a uint64 value.
fn match_ge_uint(threshold: &Threshold, value: u64) -> bool {
    matches!(threshold, Threshold::Uint(threshold) if value >= *threshold)
}

/// Go `MatchEqual` over a string value.
fn match_equal_text(threshold: &Threshold, value: &str) -> bool {
    matches!(threshold, Threshold::Text(threshold) if threshold == value)
}

/// Go `MatchEqual` over a bool value.
fn match_equal_bool(threshold: &Threshold, value: bool) -> bool {
    matches!(threshold, Threshold::Bool(threshold) if *threshold == value)
}

/// Go `uint64FromNonNegative` followed by `matchGE`.
fn match_ge_non_negative(threshold: &Threshold, value: i64) -> bool {
    u64::try_from(value).is_ok_and(|value| match_ge_uint(threshold, value))
}

/// Go `SlowLogRuleFieldAccessors[field].Match`.
fn match_condition(field: &str, threshold: &Threshold, values: &SlowLogRuleValues) -> bool {
    let seconds = |d: std::time::Duration| d.as_secs_f64();
    let kv = values.kv_exec_detail.as_ref();
    let kv_match = |value: Option<f64>| match value {
        Some(value) => match_ge_float(threshold, value),
        None => match_zero(threshold),
    };
    let kv_int_match = |value: Option<i64>| match value {
        Some(value) => match_ge_int(threshold, value),
        None => match_zero(threshold),
    };
    let detail = &values.exec_detail;
    let scan = detail.cop_exec_details.scan_detail.as_ref();
    let commit = detail.commit_detail.as_ref();
    match field {
        "conn_id" => match_ge_uint(threshold, values.conn_id),
        "session_alias" => match_equal_text(threshold, &values.session_alias),
        "db" => match threshold {
            Threshold::Text(text) => {
                tidb_hack::go_to_lower(text) == tidb_hack::go_to_lower(&values.db)
            }
            _ => false,
        },
        "exec_retry_count" => match_ge_uint(threshold, values.exec_retry_count),
        "query_time" => match_ge_float(threshold, seconds(values.query_time)),
        "parse_time" => match_ge_float(threshold, seconds(values.parse_time)),
        "compile_time" => match_ge_float(threshold, seconds(values.compile_time)),
        "rewrite_time" => match_ge_float(threshold, seconds(values.rewrite_time)),
        "optimize_time" => match_ge_float(threshold, seconds(values.optimize_time)),
        "wait_ts" => match_ge_float(threshold, seconds(values.wait_ts)),
        "is_internal" => match_equal_bool(threshold, values.is_internal),
        "digest" => match_equal_text(threshold, &values.digest),
        "num_cop_tasks" => match values.num_cop_tasks {
            Some(count) => match_ge_int(threshold, count),
            None => match_zero(threshold),
        },
        "mem_max" => match_ge_int(threshold, values.mem_max),
        "disk_max" => match_ge_int(threshold, values.disk_max),
        "write_sql_response_total" => {
            match_ge_float(threshold, seconds(values.write_sql_response_total))
        }
        "succ" => match_equal_bool(threshold, values.succ),
        "resource_group" => match threshold {
            Threshold::Text(text) => {
                tidb_hack::go_to_lower(text) == tidb_hack::go_to_lower(&values.resource_group)
            }
            _ => false,
        },
        "kv_total" => kv_match(kv.map(|d| seconds(d.wait_kv_resp_duration))),
        "pd_total" => kv_match(kv.map(|d| seconds(d.wait_pd_resp_duration))),
        "unpacked_bytes_sent_tikv_total" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_sent_kv_total))
        }
        "unpacked_bytes_received_tikv_total" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_received_kv_total))
        }
        "unpacked_bytes_sent_tikv_cross_zone" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_sent_kv_cross_zone))
        }
        "unpacked_bytes_received_tikv_cross_zone" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_received_kv_cross_zone))
        }
        "unpacked_bytes_sent_tiflash_total" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_sent_mpp_total))
        }
        "unpacked_bytes_received_tiflash_total" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_received_mpp_total))
        }
        "unpacked_bytes_sent_tiflash_cross_zone" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_sent_mpp_cross_zone))
        }
        "unpacked_bytes_received_tiflash_cross_zone" => {
            kv_int_match(kv.map(|d| d.unpacked_bytes_received_mpp_cross_zone))
        }
        "process_time" => match_ge_float(
            threshold,
            seconds(detail.cop_exec_details.time_detail.process_time),
        ),
        "backoff_time" => match_ge_float(threshold, seconds(detail.cop_exec_details.backoff_time)),
        "total_keys" => match scan {
            Some(scan) => match_ge_non_negative(threshold, scan.total_keys),
            None => match_zero(threshold),
        },
        "process_keys" => match scan {
            Some(scan) => match_ge_non_negative(threshold, scan.processed_keys),
            None => match_zero(threshold),
        },
        "cop_mvcc_read_amplification" => match scan {
            Some(scan) if scan.processed_keys > 0 => match_ge_float(
                threshold,
                scan.total_keys as f64 / scan.processed_keys as f64,
            ),
            _ => match_zero(threshold),
        },
        "prewrite_time" => match commit {
            Some(commit) => match_ge_float(threshold, seconds(commit.prewrite_time)),
            None => match_zero(threshold),
        },
        "commit_time" => match commit {
            Some(commit) => match_ge_float(threshold, seconds(commit.commit_time)),
            None => match_zero(threshold),
        },
        "write_keys" => match commit {
            Some(commit) => match_ge_non_negative(threshold, commit.write_keys as i64),
            None => match_zero(threshold),
        },
        "write_size" => match commit {
            Some(commit) => match_ge_non_negative(threshold, commit.write_size as i64),
            None => match_zero(threshold),
        },
        "prewrite_region" => match commit {
            Some(commit) => match_ge_non_negative(threshold, i64::from(commit.prewrite_region_num)),
            None => match_zero(threshold),
        },
        // Go `MatchEqual(threshold, strings.ToLower(items.PlanDigest))`.
        "plan_digest" => match_equal_text(threshold, &values.plan_digest.to_lowercase()),
        _ => false,
    }
}

/// The rules visible to one session: its own (`SessionVars.SlowLogRules`)
/// and, from Go's `GlobalSlowLogRules`, the ones bound to its connection and
/// the unbound ones.
#[derive(Default)]
pub struct EffectiveSlowLogRules {
    session: Vec<Rule>,
    connection: Vec<Rule>,
    cluster: Vec<Rule>,
}

impl EffectiveSlowLogRules {
    /// Go `updateAllRuleFields`'s inputs: the session's encoded rules
    /// (`@@session.tidb_slow_log_rules`) and the global ones
    /// (`GlobalSlowLogRules.RawRules`), for connection `conn_id`. A value
    /// that does not parse contributes no rules, as Go keeps none for it.
    #[must_use]
    pub fn new(session_raw: &str, global_raw: &str, conn_id: u64) -> Self {
        let session = parse_rule_set(session_raw, false)
            .unwrap_or_default()
            .into_iter()
            .find(|(id, _)| id.is_none())
            .map(|(_, rules)| rules)
            .unwrap_or_default();
        let mut connection = Vec::new();
        let mut cluster = Vec::new();
        for (id, rules) in parse_rule_set(global_raw, true).unwrap_or_default() {
            match id {
                None => cluster = rules,
                Some(id) if id == conn_id => connection = rules,
                Some(_) => {}
            }
        }
        Self {
            session,
            connection,
            cluster,
        }
    }

    /// Go `len(SlowLogRules.EffectiveFields) != 0`: every rule carries at
    /// least one condition, so the field set is empty exactly when no rule
    /// applies to the session.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.session.is_empty() && self.connection.is_empty() && self.cluster.is_empty()
    }

    /// Go `ShouldWriteSlowLog`: the session's rules, then the connection's,
    /// then the unbound global ones; rules are ORed, a rule's conditions
    /// ANDed.
    #[must_use]
    pub fn should_write_slow_log(&self, values: &SlowLogRuleValues) -> bool {
        [&self.session, &self.connection, &self.cluster]
            .into_iter()
            .any(|rules| {
                rules.iter().any(|rule| {
                    rule.conditions
                        .iter()
                        .all(|(field, threshold)| match_condition(field, threshold, values))
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_parse_and_encode_like_go() {
        assert_eq!(session_rules("").unwrap(), "");
        assert_eq!(session_rules("query_time:1").unwrap(), "query_time:1");
        assert_eq!(
            session_rules("Query_Time: 1.5 , succ:true; db:'test'").unwrap(),
            "query_time:1.5,succ:true;db:test"
        );
        assert_eq!(
            session_rules("query_time:1,query_time:2").unwrap(),
            "query_time:2"
        );
        assert_eq!(
            session_rules("query_time:1000000").unwrap(),
            "query_time:1e+06"
        );
        assert_eq!(
            session_rules("zzz").unwrap_err(),
            "invalid slow log rule format:zzz"
        );
        assert_eq!(
            session_rules("conn_id:5").unwrap_err(),
            "do not allow ConnID value:5"
        );
        assert_eq!(
            global_rules("conn_id:5,query_time:1;db:x").unwrap(),
            "conn_id:5,query_time:1;db:x"
        );
        assert_eq!(
            session_rules("query_time:-1").unwrap_err(),
            "invalid slow log format, value:-1, err:threshold value must be non-negative, got -1"
        );
        assert_eq!(
            session_rules("nosuch:1").unwrap_err(),
            "invalid slow log format, value:1, err:unknown slow log field name:nosuch"
        );
        assert_eq!(
            session_rules("total_keys:abc").unwrap_err(),
            "invalid slow log format, value:abc, err:strconv.ParseUint: parsing \"abc\": invalid syntax"
        );
        assert!(session_rules(&"a:1;".repeat(11))
            .unwrap_err()
            .contains("limit is 10"));
    }
    /// Go `ShouldWriteSlowLog` / `Match`: rules OR, conditions AND; the
    /// session's rules, then the ones bound to this connection, then the
    /// unbound global ones; a missing detail matches only a zero threshold.
    #[test]
    fn rules_match_like_go() {
        let values = SlowLogRuleValues {
            conn_id: 7,
            db: "Test".to_owned(),
            query_time: std::time::Duration::from_millis(1500),
            succ: true,
            resource_group: "RG1".to_owned(),
            plan_digest: "ABC".to_owned(),
            ..SlowLogRuleValues::default()
        };
        let rules = |session: &str, global: &str| EffectiveSlowLogRules::new(session, global, 7);
        assert!(rules("", "").is_empty());
        assert!(rules("query_time:1", "").should_write_slow_log(&values));
        assert!(!rules("query_time:2", "").should_write_slow_log(&values));
        assert!(!rules("query_time:1,succ:false", "").should_write_slow_log(&values));
        assert!(rules("query_time:2;db:TEST", "").should_write_slow_log(&values));
        assert!(rules("resource_group:rg1", "").should_write_slow_log(&values));
        // Go lowers the plan digest, not the threshold.
        assert!(rules("plan_digest:abc", "").should_write_slow_log(&values));
        assert!(!rules("plan_digest:ABC", "").should_write_slow_log(&values));
        // No cop task or commit: only a zero threshold matches.
        assert!(rules("num_cop_tasks:0", "").should_write_slow_log(&values));
        assert!(!rules("num_cop_tasks:1", "").should_write_slow_log(&values));
        assert!(rules("write_keys:0", "").should_write_slow_log(&values));
        assert!(rules("kv_total:0", "").should_write_slow_log(&values));
        assert!(!rules("kv_total:0.1", "").should_write_slow_log(&values));
        // Global rules: bound to this connection, to another, or unbound.
        let bound = rules("", "conn_id:7,query_time:1");
        assert!(!bound.is_empty());
        assert!(bound.should_write_slow_log(&values));
        let other = rules("", "conn_id:8,query_time:1");
        assert!(other.is_empty());
        assert!(rules("", "succ:true").should_write_slow_log(&values));
        // A session value that does not parse contributes no rules.
        assert!(rules("conn_id:7", "").is_empty());
    }
}
