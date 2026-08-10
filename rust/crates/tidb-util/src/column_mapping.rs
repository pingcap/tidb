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

//! Schema/table column mapping with prefix, suffix, and partition-ID rules.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::table_rule_selector::{InsertType, Selector, TrieSelector};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartitionRule {
    instance_id_bits: usize,
    schema_id_bits: usize,
    table_id_bits: usize,
    max_origin_id: i64,
}

static PARTITION_RULE: RwLock<PartitionRule> = RwLock::new(PartitionRule {
    instance_id_bits: 4,
    schema_id_bits: 7,
    table_id_bits: 8,
    max_origin_id: 17_592_186_044_416,
});

fn partition_rule() -> PartitionRule {
    *PARTITION_RULE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Sets the bit size of the instance/schema/table IDs and recomputes the
/// maximum origin ID, mirroring Go's package-level `SetPartitionRule`.
pub fn set_partition_rule(instance_id_size: usize, schema_id_size: usize, table_id_size: usize) {
    let used_bits = instance_id_size
        .checked_add(schema_id_size)
        .and_then(|bits| bits.checked_add(table_id_size))
        .expect("partition ID bit sizes overflow");
    assert!(used_bits <= 63, "partition ID fields must leave a sign bit");

    *PARTITION_RULE
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = PartitionRule {
        instance_id_bits: instance_id_size,
        schema_id_bits: schema_id_size,
        table_id_bits: table_id_size,
        max_origin_id: 1_i64 << (63 - used_bits),
    };
}

/// A column value flowing through the mapping. Go uses `any`; the mapping
/// functions only handle integers and strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// An integer column value (Go's `int`/`int64`/... collapse to `i64`).
    Int(i64),
    /// A string column value.
    Str(String),
}

/// `add prefix` expression.
pub const ADD_PREFIX: &str = "add prefix";
/// `add suffix` expression.
pub const ADD_SUFFIX: &str = "add suffix";
/// `partition id` expression.
pub const PARTITION_ID: &str = "partition id";

/// The error type for column-mapping operations. Mirrors the messages of the
/// `github.com/pingcap/errors` `*f` helpers (their `" is not valid"` etc.
/// suffixes) that the source's tests assert on.
#[derive(Debug, Clone)]
pub struct ColumnMappingError(String);

impl ColumnMappingError {
    fn new(msg: impl Into<String>) -> Self {
        ColumnMappingError(msg.into())
    }

    fn annotate(self, context: &str) -> Self {
        ColumnMappingError(format!("{context}: {}", self.0))
    }
}

impl fmt::Display for ColumnMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ColumnMappingError {}

fn not_found(msg: impl AsRef<str>) -> ColumnMappingError {
    ColumnMappingError::new(format!("{} not found", msg.as_ref()))
}

fn not_valid(msg: impl AsRef<str>) -> ColumnMappingError {
    ColumnMappingError::new(format!("{} is not valid", msg.as_ref()))
}

fn not_supported(msg: impl AsRef<str>) -> ColumnMappingError {
    ColumnMappingError::new(format!("{} is not supported", msg.as_ref()))
}

/// A rule to map a column.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Rule {
    /// The schema pattern this rule matches.
    #[serde(rename = "schema-pattern")]
    pub pattern_schema: String,
    /// The table pattern this rule matches.
    #[serde(rename = "table-pattern")]
    pub pattern_table: String,
    /// The source column (modify / add-refer-column / ignore).
    #[serde(rename = "source-column")]
    pub source_column: String,
    /// The target column (add column / modify).
    #[serde(rename = "target-column")]
    pub target_column: String,
    /// How to handle the mapping.
    pub expression: String,
    /// Expression arguments.
    pub arguments: Vec<String>,
    /// The create-table query.
    #[serde(rename = "create-table-query")]
    pub create_table_query: String,
}

impl Rule {
    /// Converts the schema/table patterns to lowercase.
    pub fn to_lower(&mut self) {
        self.pattern_schema = self.pattern_schema.to_lowercase();
        self.pattern_table = self.pattern_table.to_lowercase();
    }

    /// Checks the validity of the rule.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown expression, a missing target column, or
    /// the wrong number of arguments for the expression.
    pub fn valid(&self) -> Result<(), ColumnMappingError> {
        if !matches!(
            self.expression.as_str(),
            ADD_PREFIX | ADD_SUFFIX | PARTITION_ID
        ) {
            return Err(not_found(format!("expression {}", self.expression)));
        }

        if self.target_column.is_empty() {
            return Err(not_valid("rule need to be applied a target column"));
        }

        if (self.expression == ADD_PREFIX || self.expression == ADD_SUFFIX)
            && self.arguments.len() != 1
        {
            return Err(not_valid(format!(
                "arguments {:?} for add prefix/suffix",
                self.arguments
            )));
        }

        if self.expression == PARTITION_ID {
            match self.arguments.len() {
                3 | 4 => return Ok(()),
                _ => {
                    return Err(not_valid(format!(
                        "arguments {:?} for patition id",
                        self.arguments
                    )))
                }
            }
        }

        Ok(())
    }

    /// Normalizes the rule into an easier-to-process form (fills the optional
    /// separator argument of a 3-argument `partition id` with the default).
    pub fn adjust(&mut self) {
        if self.expression == PARTITION_ID && self.arguments.len() == 3 {
            self.arguments.push(String::new());
        }
    }

    /// Checks the source and target column positions.
    fn adjust_column_position(
        &self,
        source: i64,
        target: i64,
    ) -> Result<(i64, i64), ColumnMappingError> {
        if target == -1 {
            return Err(not_found(format!("target column {}", self.target_column)));
        }
        Ok((source, target))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MappingInfo {
    ignore: bool,
    source_position: i64,
    target_position: i64,
    rule: Option<Arc<Rule>>,
    instance_id: i64,
    schema_id: i64,
    table_id: i64,
}

impl MappingInfo {
    fn ignored() -> Self {
        MappingInfo {
            ignore: true,
            source_position: 0,
            target_position: 0,
            rule: None,
            instance_id: 0,
            schema_id: 0,
            table_id: 0,
        }
    }
}

struct MappingState {
    selector: TrieSelector<Arc<Rule>>,
    cache: HashMap<String, MappingInfo>,
}

/// Thread-safe column mapping built from schema/table rules.
pub struct Mapping {
    state: Mutex<MappingState>,
    case_sensitive: bool,
}

impl Mapping {
    /// Returns a new column mapping built from `rules`.
    ///
    /// # Errors
    ///
    /// Returns an error if any rule is invalid or conflicts on insertion.
    pub fn new(case_sensitive: bool, rules: &[Rule]) -> Result<Self, ColumnMappingError> {
        let mapping = Mapping {
            state: Mutex::new(MappingState {
                selector: TrieSelector::new(),
                cache: HashMap::new(),
            }),
            case_sensitive,
        };
        for rule in rules {
            mapping
                .add_rule(rule.clone())
                .map_err(|error| error.annotate(&format!("initial rule {rule:?} in mapping")))?;
        }
        Ok(mapping)
    }

    fn add_or_update_rule(
        &self,
        mut rule: Rule,
        is_update: bool,
    ) -> Result<(), ColumnMappingError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        rule.adjust();

        let insert_type = if is_update {
            InsertType::Replace
        } else {
            InsertType::Insert
        };
        let (schema, table) = (rule.pattern_schema.clone(), rule.pattern_table.clone());
        let rule_debug = format!("{rule:?}");
        let mut state = self.state();
        state.cache.clear();
        if let Err(error) =
            state
                .selector
                .insert(&schema, &table, Some(Arc::new(rule)), insert_type)
        {
            let method = if is_update { "update" } else { "add" };
            return Err(ColumnMappingError::new(error.to_string())
                .annotate(&format!("{method} rule {rule_debug} into mapping")));
        }
        Ok(())
    }

    /// Adds a rule into the mapping.
    ///
    /// # Errors
    ///
    /// Propagates validation or insertion errors.
    pub fn add_rule(&self, rule: Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, false)
    }

    /// Updates a mapping rule.
    ///
    /// # Errors
    ///
    /// Propagates validation or insertion errors.
    pub fn update_rule(&self, rule: Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, true)
    }

    /// Removes a rule from the mapping.
    ///
    /// # Errors
    ///
    /// Propagates removal errors from the selector.
    pub fn remove_rule(&self, mut rule: Rule) -> Result<(), ColumnMappingError> {
        if !self.case_sensitive {
            rule.to_lower();
        }
        let mut state = self.state();
        state.cache.clear();
        state
            .selector
            .remove(&rule.pattern_schema, &rule.pattern_table)
            .map_err(|error| {
                ColumnMappingError::new(error.to_string())
                    .annotate(&format!("remove rule {rule:?} from mapping"))
            })
    }

    /// Handles a row value, rewriting the target column per the matched rule.
    /// Returns the values and the `[source, target]` positions, or `None` when
    /// the table is ignored or unmatched.
    ///
    /// # Errors
    ///
    /// Returns an error on ambiguous rules or an inapplicable value type.
    pub fn handle_row_value(
        &self,
        schema: &str,
        table: &str,
        columns: &[&str],
        mut values: Vec<Value>,
    ) -> Result<(Vec<Value>, Option<Vec<i64>>), ColumnMappingError> {
        let (schema, table) = self.normalize(schema, table);
        let info = self.query_column_info(&schema, &table, columns)?;
        if info.ignore {
            return Ok((values, None));
        }

        let rule = info.rule.as_ref().expect("a matched mapping has a rule");
        match rule.expression.as_str() {
            ADD_PREFIX => add_prefix(rule, info.target_position, &mut values)?,
            ADD_SUFFIX => add_suffix(rule, info.target_position, &mut values)?,
            PARTITION_ID => partition_id(&info, &mut values)?,
            expression => return Err(not_found(format!("column mapping expression {expression}"))),
        }

        Ok((
            values,
            Some(vec![info.source_position, info.target_position]),
        ))
    }

    /// Passes unmatched DDL through and rejects matched DDL because DDL column
    /// rewriting is not implemented by this package.
    ///
    /// # Errors
    ///
    /// Returns an error for a matched table or an ambiguous rule.
    pub fn handle_ddl(
        &self,
        schema: &str,
        table: &str,
        columns: &[&str],
        statement: &str,
    ) -> Result<(String, Option<Vec<i64>>), ColumnMappingError> {
        let (normalized_schema, normalized_table) = self.normalize(schema, table);
        let info = self.query_column_info(&normalized_schema, &normalized_table, columns)?;
        if info.ignore {
            return Ok((statement.to_string(), None));
        }

        self.reset_cache();
        let rule = info.rule.expect("a matched mapping has a rule");
        Err(ColumnMappingError::new(format!(
            "ddl {statement} @ column mapping rule {schema}/{table}:{rule:?} not implemented"
        )))
    }

    fn normalize(&self, schema: &str, table: &str) -> (String, String) {
        if self.case_sensitive {
            (schema.to_string(), table.to_string())
        } else {
            (schema.to_lowercase(), table.to_lowercase())
        }
    }

    fn query_column_info(
        &self,
        schema: &str,
        table: &str,
        columns: &[&str],
    ) -> Result<MappingInfo, ColumnMappingError> {
        let key = table_name(schema, table);
        let mut state = self.state();
        if let Some(info) = state.cache.get(&key) {
            return Ok(info.clone());
        }

        let rules = state.selector.match_rules(schema, table);
        if rules.is_empty() {
            let info = MappingInfo::ignored();
            state.cache.insert(key, info.clone());
            return Ok(info);
        }

        let mut schema_rules = Vec::<Arc<Rule>>::new();
        let mut table_rules = Vec::<Arc<Rule>>::with_capacity(1);
        for rule in rules {
            if rule.pattern_table.is_empty() {
                schema_rules.push(rule);
            } else {
                table_rules.push(rule);
            }
        }

        let rule = if table.is_empty() || table_rules.is_empty() {
            if schema_rules.len() != 1 {
                return Err(not_supported(format!(
                    "`{schema}`.`{table}` matches {} schema column mapping rules which should be one. It's",
                    schema_rules.len()
                )));
            }
            schema_rules.into_iter().next().unwrap()
        } else {
            if table_rules.len() != 1 {
                return Err(not_supported(format!(
                    "`{schema}`.`{table}` matches {} table column mapping rules which should be one. It's",
                    table_rules.len()
                )));
            }
            table_rules.into_iter().next().unwrap()
        };

        let source_position = find_column_position(columns, &rule.source_column);
        let target_position = find_column_position(columns, &rule.target_column);
        let (source_position, target_position) =
            rule.adjust_column_position(source_position, target_position)?;
        let mut info = MappingInfo {
            ignore: false,
            source_position,
            target_position,
            rule: Some(Arc::clone(&rule)),
            instance_id: 0,
            schema_id: 0,
            table_id: 0,
        };

        if rule.expression == PARTITION_ID {
            let (instance_id, schema_id, table_id) = compute_partition_id(schema, table, &rule)?;
            info.instance_id = instance_id;
            info.schema_id = schema_id;
            info.table_id = table_id;
        }

        state.cache.insert(key, info.clone());
        Ok(info)
    }

    fn reset_cache(&self) {
        self.state().cache.clear();
    }

    fn state(&self) -> MutexGuard<'_, MappingState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.state().cache.len()
    }
}

fn find_column_position(cols: &[&str], col: &str) -> i64 {
    cols.iter().position(|c| *c == col).map_or(-1, |i| i as i64)
}

fn table_name(schema: &str, table: &str) -> String {
    format!("`{schema}`.`{table}`")
}

fn add_prefix(rule: &Rule, target: i64, vals: &mut [Value]) -> Result<(), ColumnMappingError> {
    let prefix = &rule.arguments[0];
    let Value::Str(origin) = &vals[target as usize] else {
        return Err(not_valid(format!(
            "column {target} value is not string, but {:?}, which",
            vals[target as usize]
        )));
    };
    vals[target as usize] = Value::Str(format!("{prefix}{origin}"));
    Ok(())
}

fn add_suffix(rule: &Rule, target: i64, vals: &mut [Value]) -> Result<(), ColumnMappingError> {
    let suffix = &rule.arguments[0];
    let Value::Str(origin) = &vals[target as usize] else {
        return Err(not_valid(format!(
            "column {target} value is not string, but {:?}, which",
            vals[target as usize]
        )));
    };
    vals[target as usize] = Value::Str(format!("{origin}{suffix}"));
    Ok(())
}

fn partition_id(info: &MappingInfo, vals: &mut [Value]) -> Result<(), ColumnMappingError> {
    let target = info.target_position as usize;
    let (mut origin_id, is_chars) = match &vals[target] {
        Value::Int(v) => (*v, false),
        Value::Str(s) => {
            let parsed = s.parse::<i64>().map_err(|_| {
                not_valid(format!(
                    "column {target} value is not int, but {s:?}, which"
                ))
            })?;
            (parsed, true)
        }
    };

    let max_origin_id = partition_rule().max_origin_id;
    if origin_id >= max_origin_id || origin_id < 0 {
        return Err(not_valid(format!(
            "id must less than {max_origin_id}, greater than or equal to 0, but got {origin_id}, which"
        )));
    }

    origin_id |= info.instance_id | info.schema_id | info.table_id;
    vals[target] = if is_chars {
        Value::Str(origin_id.to_string())
    } else {
        Value::Int(origin_id)
    };
    Ok(())
}

fn compute_partition_id(
    schema: &str,
    table: &str,
    rule: &Rule,
) -> Result<(i64, i64, i64), ColumnMappingError> {
    let partition_rule = partition_rule();
    let instance_bits = partition_rule.instance_id_bits;
    let schema_bits = partition_rule.schema_id_bits;
    let table_bits = partition_rule.table_id_bits;

    let mut shift_cnt: u32 = 63;
    let mut instance_id = 0_i64;
    if instance_bits > 0 && !rule.arguments[0].is_empty() {
        shift_cnt -= instance_bits as u32;
        let unsigned = parse_uint(&rule.arguments[0], instance_bits)?;
        instance_id = (unsigned << shift_cnt) as i64;
    }

    let sep = &rule.arguments[3];

    let mut schema_id = 0_i64;
    if schema_bits > 0 && !rule.arguments[1].is_empty() {
        shift_cnt -= schema_bits as u32;
        schema_id = compute_id(schema, &rule.arguments[1], sep, schema_bits, shift_cnt)?;
    }

    let mut table_id = 0_i64;
    if table_bits > 0 && !rule.arguments[2].is_empty() {
        shift_cnt -= table_bits as u32;
        table_id = compute_id(table, &rule.arguments[2], sep, table_bits, shift_cnt)?;
    }

    Ok((instance_id, schema_id, table_id))
}

fn compute_id(
    name: &str,
    prefix: &str,
    sep: &str,
    bit_size: usize,
    shift_count: u32,
) -> Result<i64, ColumnMappingError> {
    if name == prefix {
        return Ok(0);
    }

    let prefix = format!("{prefix}{sep}");
    if prefix.len() >= name.len() || prefix.as_bytes() != &name.as_bytes()[..prefix.len()] {
        return Err(not_valid(format!("{prefix} is not the prefix of {name}")));
    }

    let id_str = &name[prefix.len()..];
    let id = parse_uint(id_str, bit_size).map_err(|_| {
        not_valid(format!(
            "the suffix of {id_str} can't be converted to int64"
        ))
    })?;
    Ok((id << shift_count) as i64)
}

/// `strconv.ParseUint(s, 10, bit_size)`: base-10, rejecting a value that does
/// not fit in `bit_size` bits (or non-digits).
fn parse_uint(s: &str, bit_size: usize) -> Result<u64, ColumnMappingError> {
    let value: u64 = s.parse().map_err(|_| not_valid(format!("parsing {s:?}")))?;
    if bit_size < 64 && value >= (1_u64 << bit_size) {
        return Err(not_valid(format!(
            "value {value} out of range for {bit_size} bits"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Partition sizing is process-wide, so tests that change it are serialized.
    static PARTITION_LOCK: Mutex<()> = Mutex::new(());

    fn rule(
        pattern_schema: &str,
        pattern_table: &str,
        source_column: &str,
        target_column: &str,
        expression: &str,
        arguments: &[&str],
        create_table_query: &str,
    ) -> Rule {
        Rule {
            pattern_schema: pattern_schema.to_string(),
            pattern_table: pattern_table.to_string(),
            source_column: source_column.to_string(),
            target_column: target_column.to_string(),
            expression: expression.to_string(),
            arguments: arguments.iter().map(|s| s.to_string()).collect(),
            create_table_query: create_table_query.to_string(),
        }
    }

    #[test]
    fn rule_validity() {
        let mut r = rule("test*", "abc*", "id", "id", "Error", &[], "xxx");
        assert!(r.valid().is_err());

        r.target_column = String::new();
        assert!(r.valid().is_err());

        r.expression = ADD_PREFIX.to_string();
        r.target_column = "id".to_string();
        assert!(r.valid().is_err());

        r.arguments = vec!["1".to_string()];
        assert!(r.valid().is_ok());

        r.expression = PARTITION_ID.to_string();
        assert!(r.valid().is_err());

        r.arguments = vec!["1".to_string(), "test_".to_string(), "t_".to_string()];
        assert!(r.valid().is_ok());
    }

    #[test]
    fn handle() {
        let rules = [rule(
            "Test*",
            "xxx*",
            "",
            "id",
            ADD_PREFIX,
            &["instance_id:"],
            "xx",
        )];
        let m = Mapping::new(false, &rules).unwrap();
        assert_eq!(m.cache_len(), 0);

        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &["age", "id"],
                vec![Value::Int(1), Value::Str("1".into())],
            )
            .unwrap();
        assert_eq!(
            vals,
            vec![Value::Int(1), Value::Str("instance_id:1".into())]
        );
        assert_eq!(poss, Some(vec![-1, 1]));

        // cache hit
        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &["name"],
                vec![Value::Int(1), Value::Str("1".into())],
            )
            .unwrap();
        assert_eq!(
            vals,
            vec![Value::Int(1), Value::Str("instance_id:1".into())]
        );
        assert_eq!(poss, Some(vec![-1, 1]));

        // resetCache: target column now missing -> error
        m.reset_cache();
        assert!(m
            .handle_row_value("test", "xxx", &["name"], vec![Value::Str("1".into())])
            .is_err());

        // DDL on matched table -> error; unmatched -> pass through
        assert!(m
            .handle_ddl("test", "xxx", &["id", "age"], "create table xxx")
            .is_err());
        let (statement, poss) = m
            .handle_ddl("abc", "xxx", &["id", "age"], "create table xxx")
            .unwrap();
        assert_eq!(statement, "create table xxx");
        assert_eq!(poss, None);
    }

    #[test]
    fn rule_lifecycle_and_table_priority() {
        let schema_rule = rule("db*", "", "", "id", ADD_PREFIX, &["schema:"], "");
        let table_rule = rule("db*", "special", "", "id", ADD_SUFFIX, &["-table"], "");
        let mapping = Mapping::new(false, &[schema_rule.clone(), table_rule.clone()]).unwrap();

        let (values, _) = mapping
            .handle_row_value("DB1", "special", &["id"], vec![Value::Str("7".into())])
            .unwrap();
        assert_eq!(values, vec![Value::Str("7-table".into())]);

        let (values, _) = mapping
            .handle_row_value("DB1", "other", &["id"], vec![Value::Str("7".into())])
            .unwrap();
        assert_eq!(values, vec![Value::Str("schema:7".into())]);

        mapping
            .update_rule(rule(
                "db*",
                "special",
                "",
                "id",
                ADD_PREFIX,
                &["table:"],
                "",
            ))
            .unwrap();
        let (values, _) = mapping
            .handle_row_value("DB1", "special", &["id"], vec![Value::Str("7".into())])
            .unwrap();
        assert_eq!(values, vec![Value::Str("table:7".into())]);

        mapping.remove_rule(table_rule).unwrap();
        let (values, _) = mapping
            .handle_row_value("DB1", "special", &["id"], vec![Value::Str("7".into())])
            .unwrap();
        assert_eq!(values, vec![Value::Str("schema:7".into())]);
    }

    #[test]
    fn query_column_info_partition() {
        let _guard = PARTITION_LOCK.lock().unwrap();
        set_partition_rule(4, 7, 8);
        let rules = [rule(
            "test*",
            "xxx*",
            "",
            "id",
            PARTITION_ID,
            &["8", "test_", "xxx_"],
            "xx",
        )];
        let m = Mapping::new(false, &rules).unwrap();

        let info = m
            .query_column_info("test_2", "t_1", &["id", "name"])
            .unwrap();
        assert!(info.ignore);

        let info = m
            .query_column_info("test_2", "xxx_1", &["id", "name"])
            .unwrap();
        assert_eq!(info.source_position, -1);
        assert_eq!(info.target_position, 0);
        assert_eq!(info.instance_id, 8 << 59);
        assert_eq!(info.schema_id, 2 << 52);
        assert_eq!(info.table_id, 1 << 44);

        m.reset_cache();
        set_partition_rule(0, 0, 3);
        let info = m
            .query_column_info("test_2", "xxx_1", &["id", "name"])
            .unwrap();
        assert_eq!(info.instance_id, 0);
        assert_eq!(info.schema_id, 0);
        assert_eq!(info.table_id, 1 << 60);
    }

    #[test]
    fn set_partition_rule_updates_config() {
        let _guard = PARTITION_LOCK.lock().unwrap();
        set_partition_rule(4, 7, 8);
        assert_eq!(
            partition_rule(),
            PartitionRule {
                instance_id_bits: 4,
                schema_id_bits: 7,
                table_id_bits: 8,
                max_origin_id: 1 << 44,
            }
        );

        set_partition_rule(0, 3, 4);
        assert_eq!(
            partition_rule(),
            PartitionRule {
                instance_id_bits: 0,
                schema_id_bits: 3,
                table_id_bits: 4,
                max_origin_id: 1 << 56,
            }
        );
    }

    #[test]
    fn compute_partition_id_vectors() {
        let _guard = PARTITION_LOCK.lock().unwrap();
        set_partition_rule(4, 7, 8);

        let r = rule("", "", "", "", "", &["test", "t"], "");
        assert!(compute_partition_id("test_1", "t_1", &r).is_err());
        assert!(compute_partition_id("test", "t", &r).is_err());

        let r = rule("", "", "", "", "", &["2", "test", "t", "_"], "");
        let (i, s, t) = compute_partition_id("test_1", "t_1", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 1 << 52, 1 << 44));

        let (i, s, t) = compute_partition_id("test", "t_3", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 0, 3 << 44));

        let (i, s, t) = compute_partition_id("test_5", "t", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 5 << 52, 0));

        assert!(compute_partition_id("unrelated", "t_6", &r)
            .unwrap_err()
            .to_string()
            .starts_with("test_ is not the prefix of unrelated"));
        assert!(compute_partition_id("test", "x", &r)
            .unwrap_err()
            .to_string()
            .starts_with("t_ is not the prefix of x"));
        assert!(compute_partition_id("test_0", "t_0xa", &r)
            .unwrap_err()
            .to_string()
            .starts_with("the suffix of 0xa can't be converted to int64"));
        assert!(compute_partition_id("test_0", "t_", &r)
            .unwrap_err()
            .to_string()
            .starts_with("t_ is not the prefix of t_"));
        assert!(compute_partition_id("testx", "t_3", &r)
            .unwrap_err()
            .to_string()
            .starts_with("test_ is not the prefix of testx"));

        set_partition_rule(4, 0, 8);
        let r = rule("", "", "", "", "", &["2", "test_", "t_", ""], "");
        let (i, s, t) = compute_partition_id("test_1", "t_1", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 0, 1 << 51));
        let (i, s, t) = compute_partition_id("test_", "t_", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 0, 0));

        set_partition_rule(4, 7, 8);
        let r = rule("", "", "", "", "", &["", "test_", "t_", ""], "");
        let (i, s, t) = compute_partition_id("test_1", "t_1", &r).unwrap();
        assert_eq!((i, s, t), (0, 1 << 56, 1 << 48));

        let r = rule("", "", "", "", "", &["2", "", "t_", ""], "");
        let (i, s, t) = compute_partition_id("test_1", "t_1", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 0, 1 << 51));

        let r = rule("", "", "", "", "", &["2", "test_", "", ""], "");
        let (i, s, t) = compute_partition_id("test_1", "t_1", &r).unwrap();
        assert_eq!((i, s, t), (2 << 59, 1 << 52, 0));
    }

    #[test]
    fn partition_id_vectors() {
        let _guard = PARTITION_LOCK.lock().unwrap();
        set_partition_rule(4, 7, 8);
        let mut info = MappingInfo {
            ignore: false,
            source_position: 0,
            target_position: 1,
            rule: None,
            instance_id: 2 << 59,
            schema_id: 1 << 52,
            table_id: 1 << 44,
        };

        // wrong type: a non-numeric string
        let mut vals = vec![Value::Int(1), Value::Str("ha".into())];
        assert!(partition_id(&info, &mut vals).is_err());

        // exceed maxOriginID
        let mut vals = vec![Value::Str("ha".into()), Value::Int(1 << 44)];
        assert!(partition_id(&info, &mut vals).is_err());

        let mut vals = vec![Value::Str("ha".into()), Value::Int(1)];
        partition_id(&info, &mut vals).unwrap();
        assert_eq!(vals[1], Value::Int(2 << 59 | 1 << 52 | 1 << 44 | 1));

        info.instance_id = 0;
        let mut vals = vec![Value::Str("ha".into()), Value::Str("123".into())];
        partition_id(&info, &mut vals).unwrap();
        assert_eq!(
            vals[1],
            Value::Str((1_i64 << 52 | 1 << 44 | 123).to_string())
        );
    }

    #[test]
    fn case_sensitive() {
        let rules = [rule(
            "Test*",
            "xxx*",
            "",
            "id",
            ADD_PREFIX,
            &["instance_id:"],
            "xx",
        )];
        let m = Mapping::new(true, &rules).unwrap();
        assert_eq!(m.cache_len(), 0);

        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &["age", "id"],
                vec![Value::Int(1), Value::Str("1".into())],
            )
            .unwrap();
        assert_eq!(vals, vec![Value::Int(1), Value::Str("1".into())]);
        assert_eq!(poss, None);
    }

    #[test]
    fn rule_uses_public_config_field_names() {
        let rule = rule(
            "db*",
            "table*",
            "source",
            "target",
            ADD_SUFFIX,
            &["-archive"],
            "create table target(id bigint)",
        );
        let encoded = serde_json::to_value(&rule).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({
                "schema-pattern": "db*",
                "table-pattern": "table*",
                "source-column": "source",
                "target-column": "target",
                "expression": "add suffix",
                "arguments": ["-archive"],
                "create-table-query": "create table target(id bigint)",
            })
        );
        assert_eq!(serde_json::from_value::<Rule>(encoded).unwrap(), rule);
    }

    #[test]
    fn mapping_supports_concurrent_callers() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Mapping>();

        let mapping = Arc::new(
            Mapping::new(
                false,
                &[rule(
                    "db*",
                    "table*",
                    "",
                    "id",
                    ADD_PREFIX,
                    &["tenant:"],
                    "",
                )],
            )
            .unwrap(),
        );
        let start = Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let mapping = Arc::clone(&mapping);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for id in 0..128 {
                        let (values, positions) = mapping
                            .handle_row_value(
                                "DB1",
                                "TABLE1",
                                &["id"],
                                vec![Value::Str(id.to_string())],
                            )
                            .unwrap();
                        assert_eq!(values, vec![Value::Str(format!("tenant:{id}"))]);
                        assert_eq!(positions, Some(vec![-1, 0]));
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
