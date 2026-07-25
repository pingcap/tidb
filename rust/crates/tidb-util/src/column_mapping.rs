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

//! Complete transcreation of Go `pkg/util/column-mapping` (Go package
//! `column`, `column.go`). A DM column-mapping engine: it matches
//! schema/table patterns via the [`crate::table_rule_selector`] trie and
//! rewrites a target column value by adding a prefix/suffix or computing a
//! bit-packed partition ID.
//!
//! Faithful Rust adaptations of Go idioms, none changing observable behavior:
//! - Go's `[]any` column values are the [`Value`] enum. The mapping functions
//!   only ever handle integers and strings, so those are the only variants;
//!   Go's "unsupported type" error is thus unrepresentable by construction.
//! - Go's package-level partition-bit variables (mutated by
//!   [`set_partition_rule`]) are `static` atomics.
//! - Go's `RWMutex`-guarded `Mapping` becomes `&mut self` methods — Rust's
//!   borrow checker enforces the exclusive access the mutex provided.
//! - `Rule`s are shared as `Rc<Rule>` (Go's `*Rule`); the `yaml/json/toml`
//!   struct tags (config deserialization, untested) are out of scope.

use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use crate::table_rule_selector::{InsertType, Selector, TrieSelector};

/// For partition ID; see [`partition_id`] for the bit-layout definition.
static INSTANCE_ID_BIT_SIZE: AtomicUsize = AtomicUsize::new(4);
static SCHEMA_ID_BIT_SIZE: AtomicUsize = AtomicUsize::new(7);
static TABLE_ID_BIT_SIZE: AtomicUsize = AtomicUsize::new(8);
static MAX_ORIGIN_ID: AtomicI64 = AtomicI64::new(17_592_186_044_416);

/// Sets the bit size of the instance/schema/table IDs and recomputes the
/// maximum origin ID, mirroring Go's package-level `SetPartitionRule`.
pub fn set_partition_rule(instance_id_size: usize, schema_id_size: usize, table_id_size: usize) {
    INSTANCE_ID_BIT_SIZE.store(instance_id_size, Ordering::Relaxed);
    SCHEMA_ID_BIT_SIZE.store(schema_id_size, Ordering::Relaxed);
    TABLE_ID_BIT_SIZE.store(table_id_size, Ordering::Relaxed);
    MAX_ORIGIN_ID.store(
        1_i64 << (64 - instance_id_size - schema_id_size - table_id_size - 1),
        Ordering::Relaxed,
    );
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
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Rule {
    /// The schema pattern this rule matches.
    pub pattern_schema: String,
    /// The table pattern this rule matches.
    pub pattern_table: String,
    /// The source column (modify / add-refer-column / ignore).
    pub source_column: String,
    /// The target column (add column / modify).
    pub target_column: String,
    /// How to handle the mapping.
    pub expression: String,
    /// Expression arguments.
    pub arguments: Vec<String>,
    /// The create-table query.
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
    rule: Option<Rc<Rule>>,
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

/// Maps columns to something by rules.
pub struct Mapping {
    selector: TrieSelector<Rc<Rule>>,
    case_sensitive: bool,
    cache: HashMap<String, MappingInfo>,
}

impl Mapping {
    /// Returns a new column mapping built from `rules`.
    ///
    /// # Errors
    ///
    /// Returns an error if any rule is invalid or conflicts on insertion.
    pub fn new(case_sensitive: bool, rules: &[Rule]) -> Result<Self, ColumnMappingError> {
        let mut m = Mapping {
            selector: TrieSelector::new(),
            case_sensitive,
            cache: HashMap::new(),
        };
        for rule in rules {
            m.add_rule(rule.clone())
                .map_err(|e| e.annotate(&format!("initial rule {rule:?} in mapping")))?;
        }
        Ok(m)
    }

    fn add_or_update_rule(
        &mut self,
        mut rule: Rule,
        is_update: bool,
    ) -> Result<(), ColumnMappingError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        rule.adjust();

        self.reset_cache();
        let insert_type = if is_update {
            InsertType::Replace
        } else {
            InsertType::Insert
        };
        let (schema, table) = (rule.pattern_schema.clone(), rule.pattern_table.clone());
        let rule_dbg = format!("{rule:?}");
        let result = self
            .selector
            .insert(&schema, &table, Some(Rc::new(rule)), insert_type);
        if let Err(e) = result {
            let method = if is_update { "update" } else { "add" };
            let ce = ColumnMappingError::new(e.to_string());
            return Err(ce.annotate(&format!("{method} rule {rule_dbg} into mapping")));
        }
        Ok(())
    }

    /// Adds a rule into the mapping.
    ///
    /// # Errors
    ///
    /// Propagates validation or insertion errors.
    pub fn add_rule(&mut self, rule: Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, false)
    }

    /// Updates a mapping rule.
    ///
    /// # Errors
    ///
    /// Propagates validation or insertion errors.
    pub fn update_rule(&mut self, rule: Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, true)
    }

    /// Removes a rule from the mapping.
    ///
    /// # Errors
    ///
    /// Propagates removal errors from the selector.
    pub fn remove_rule(&mut self, mut rule: Rule) -> Result<(), ColumnMappingError> {
        if !self.case_sensitive {
            rule.to_lower();
        }
        self.reset_cache();
        self.selector
            .remove(&rule.pattern_schema, &rule.pattern_table)
            .map_err(|e| {
                ColumnMappingError::new(e.to_string())
                    .annotate(&format!("remove rule {rule:?} from mapping"))
            })
    }

    /// Handles a row value, rewriting the target column per the matched rule.
    /// Returns the (possibly rewritten) values and the `[source, target]`
    /// positions (or `None` when the table is ignored / unmatched).
    ///
    /// # Errors
    ///
    /// Returns an error on ambiguous rules or an inapplicable value type.
    pub fn handle_row_value(
        &mut self,
        schema: &str,
        table: &str,
        columns: &[&str],
        mut vals: Vec<Value>,
    ) -> Result<(Vec<Value>, Option<Vec<i64>>), ColumnMappingError> {
        let (schema_l, table_l) = self.normalize(schema, table);

        let info = self.query_column_info(&schema_l, &table_l, columns)?;
        if info.ignore {
            return Ok((vals, None));
        }

        let rule = info.rule.clone().expect("a non-ignored info has a rule");
        let target = info.target_position;
        match rule.expression.as_str() {
            ADD_PREFIX => add_prefix(&rule, target, &mut vals)?,
            ADD_SUFFIX => add_suffix(&rule, target, &mut vals)?,
            PARTITION_ID => partition_id(&info, &mut vals)?,
            other => return Err(not_found(format!("column mapping expression {other}"))),
        }

        Ok((vals, Some(vec![info.source_position, info.target_position])))
    }

    /// Handles a DDL statement. Ignored/unmatched tables pass the statement
    /// through; a matched table is a not-yet-implemented error, matching Go.
    ///
    /// # Errors
    ///
    /// Returns an error for a matched table (DDL column mapping is unimplemented)
    /// or an ambiguous rule.
    pub fn handle_ddl(
        &mut self,
        schema: &str,
        table: &str,
        columns: &[&str],
        statement: &str,
    ) -> Result<(String, Option<Vec<i64>>), ColumnMappingError> {
        let (schema_l, table_l) = self.normalize(schema, table);

        let info = self.query_column_info(&schema_l, &table_l, columns)?;
        if info.ignore {
            return Ok((statement.to_string(), None));
        }

        self.reset_cache();
        let rule = info.rule.expect("a non-ignored info has a rule");
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
        &mut self,
        schema: &str,
        table: &str,
        columns: &[&str],
    ) -> Result<MappingInfo, ColumnMappingError> {
        let key = table_name(schema, table);
        if let Some(ci) = self.cache.get(&key) {
            return Ok(ci.clone());
        }

        let rules = self.selector.match_rules(schema, table);
        if rules.is_empty() {
            let info = MappingInfo::ignored();
            self.cache.insert(key, info.clone());
            return Ok(info);
        }

        // Classify rules into schema-level and table-level; table-level rules
        // have the highest priority.
        let mut schema_rules: Vec<Rc<Rule>> = Vec::new();
        let mut table_rules: Vec<Rc<Rule>> = Vec::with_capacity(1);
        for rule in rules {
            if rule.pattern_table.is_empty() {
                schema_rules.push(rule);
            } else {
                table_rules.push(rule);
            }
        }

        // Only one expression per table is supported for now.
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
            rule: Some(Rc::clone(&rule)),
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

        self.cache.insert(key, info.clone());
        Ok(info)
    }

    fn reset_cache(&mut self) {
        self.cache = HashMap::new();
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

    let max_origin_id = MAX_ORIGIN_ID.load(Ordering::Relaxed);
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
    let instance_bits = INSTANCE_ID_BIT_SIZE.load(Ordering::Relaxed);
    let schema_bits = SCHEMA_ID_BIT_SIZE.load(Ordering::Relaxed);
    let table_bits = TABLE_ID_BIT_SIZE.load(Ordering::Relaxed);

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
    use std::sync::Mutex;

    // Serializes tests that mutate the process-global partition-bit config,
    // which Go runs sequentially within one package.
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

    // Go `TestRule`.
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

    // Go `TestHandle`.
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
        let mut m = Mapping::new(false, &rules).unwrap();
        assert_eq!(m.cache.len(), 0);

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

    // Go `TestQueryColumnInfo`.
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
        let mut m = Mapping::new(false, &rules).unwrap();

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

    // Go `TestSetPartitionRule`.
    #[test]
    fn set_partition_rule_updates_config() {
        let _guard = PARTITION_LOCK.lock().unwrap();
        set_partition_rule(4, 7, 8);
        assert_eq!(INSTANCE_ID_BIT_SIZE.load(Ordering::Relaxed), 4);
        assert_eq!(SCHEMA_ID_BIT_SIZE.load(Ordering::Relaxed), 7);
        assert_eq!(TABLE_ID_BIT_SIZE.load(Ordering::Relaxed), 8);
        assert_eq!(MAX_ORIGIN_ID.load(Ordering::Relaxed), 1 << 44);

        set_partition_rule(0, 3, 4);
        assert_eq!(INSTANCE_ID_BIT_SIZE.load(Ordering::Relaxed), 0);
        assert_eq!(SCHEMA_ID_BIT_SIZE.load(Ordering::Relaxed), 3);
        assert_eq!(TABLE_ID_BIT_SIZE.load(Ordering::Relaxed), 4);
        assert_eq!(MAX_ORIGIN_ID.load(Ordering::Relaxed), 1 << 56);
    }

    // Go `TestComputePartitionID`.
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

    // Go `TestPartitionID`.
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

    // Go `TestCaseSensitive`.
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
        let mut m = Mapping::new(true, &rules).unwrap();
        assert_eq!(m.cache.len(), 0);

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
}
