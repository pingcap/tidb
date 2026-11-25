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

//! MySQL-replication-style table filter.
//!
//! A rule whose value begins with `~` is a Go regular expression. Any other value is a glob-like
//! pattern handled by the trie [`Selector`](tidb_table_rule_selector).
//! Regex look-around (`(?!...)`, `(?=...)`) is rejected. The selector and
//! result cache are internally synchronized for concurrent callers.

mod schema;
pub use schema::{is_system_schema, DM_HEARTBEAT_SCHEMA, INSPECTION_SCHEMA_NAME};

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tidb_go_regexp::RegexpPredicate;
use tidb_mysql::to_lowercase as go_simple_lowercase;

use tidb_table_filter::{MySQLReplicationRules, Table};
use tidb_table_rule_selector::{InsertType, Selector, TrieSelector};

/// The action a matched rule implies (Go `ActionType`): keep (`DO`) or drop.
pub type ActionType = bool;
/// Keep the table (Go `Do`).
pub const DO: ActionType = true;
/// Drop the table (Go `Ignore`).
pub const IGNORE: ActionType = false;

/// Filter rules (Go `Rules` = `tfilter.MySQLReplicationRules`).
pub type Rules = MySQLReplicationRules;

/// A filter build error (Go returns `error` from `New`).
#[derive(Debug)]
pub struct FilterError(String);

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FilterError {}

// The kind of a trie-stored rule (Go's `dbRule`/`tblRule*` iota).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RuleKind {
    Db,
    TblFull,
    TblOnlyDbPart,
    TblOnlyTblPart,
}

// A rule stored at a trie node (Go `nodeEndRule`).
#[derive(Clone)]
struct NodeEndRule {
    // The compiled table regex, set only for `TblOnlyDbPart`.
    r: Option<Arc<RegexpPredicate>>,
    kind: RuleKind,
    is_allow_list: bool,
}

type Patterns = HashMap<String, Arc<RegexpPredicate>>;

/// A table filter in the style of MySQL replication rules (Go `Filter`).
pub struct Filter {
    selector: TrieSelector<NodeEndRule>,
    pattern_map: Patterns,
    rules: Option<Rules>,
    cache: RwLock<HashMap<String, ActionType>>,
    case_sensitive: bool,
    single_table_allow: bool,
    literal_schema_allow: bool,
}

impl Filter {
    /// Go `New`: builds a filter from the rules, lowercasing them when the
    /// filter is case-insensitive.
    pub fn new(case_sensitive: bool, mut rules: Option<Rules>) -> Result<Filter, FilterError> {
        if !case_sensitive {
            if let Some(r) = rules.as_mut() {
                r.to_lower();
            }
        }
        let selector: TrieSelector<NodeEndRule> = TrieSelector::new();
        let mut pattern_map = Patterns::new();
        init_rules(&selector, &mut pattern_map, rules.as_ref(), case_sensitive)?;
        let single_table_allow = rules.as_ref().is_some_and(|rules| {
            let ([schema], [table]) = (rules.do_dbs.as_slice(), rules.do_tables.as_slice()) else {
                return false;
            };
            !schema.starts_with('~') && *schema == table.schema
                && rules.ignore_dbs.is_empty() && rules.ignore_tables.is_empty()
        });
        let literal_schema_allow = rules.as_ref().is_some_and(|rules| {
            let [schema] = rules.do_dbs.as_slice() else { return false };
            schema.is_ascii() && !schema.starts_with('~')
                && !schema.contains(['*', '?', '[', '`'])
        });
        Ok(Filter {
            selector,
            pattern_map,
            rules,
            cache: RwLock::new(HashMap::new()),
            case_sensitive,
            single_table_allow,
            literal_schema_allow,
        })
    }

    fn read_cache(&self) -> RwLockReadGuard<'_, HashMap<String, ActionType>> {
        self.cache.read().unwrap_or_else(|error| error.into_inner())
    }

    fn write_cache(&self) -> RwLockWriteGuard<'_, HashMap<String, ActionType>> {
        self.cache
            .write()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Go `ApplyOn`: returns the (case-normalised) clones of the input tables
    /// that pass the filter. Deprecated in Go; kept for parity.
    pub fn apply_on(&self, stbs: &[Table]) -> Vec<Table> {
        if self.rules.is_none() {
            return stbs.to_vec();
        }
        let mut tbs = Vec::new();
        for tb in stbs {
            let schema = self.normalize_name(&tb.schema);
            let table = self.normalize_name(&tb.name);
            if self.matches_normalized(&schema, &table) {
                tbs.push(Table {
                    schema: schema.into_owned(),
                    name: table.into_owned(),
                });
            }
        }
        tbs
    }

    /// Go `Apply`: returns the original input tables that pass the filter.
    pub fn apply(&self, stbs: &[Table]) -> Vec<Table> {
        if self.rules.is_none() {
            return stbs.to_vec();
        }
        let mut tbs = Vec::new();
        for tb in stbs {
            if self.matches(tb) {
                tbs.push(tb.clone());
            }
        }
        tbs
    }

    /// Go `Match`: whether `tb` should be kept (not filtered out).
    pub fn matches(&self, tb: &Table) -> bool {
        let Some(_rules) = self.rules.as_ref() else {
            return true;
        };
        let schema = self.normalize_name(&tb.schema);
        let table = self.normalize_name(&tb.name);
        self.matches_normalized(&schema, &table)
    }

    fn matches_normalized(&self, schema: &str, table: &str) -> bool {
        let literal_schema = self.literal_schema_allow && !schema.contains('`');
        if literal_schema && self.rules.as_ref().expect("literal schema rule").do_dbs[0] != schema {
            // This quoted name cannot start with the allowed schema's key.
            return false;
        }
        let table_len = if table.is_empty() { 0 } else { table.len() + 3 };
        let mut name = String::with_capacity(schema.len() + 2 + table_len);
        name.push('`');
        name.push_str(schema);
        name.push('`');
        if !table.is_empty() {
            name.push_str(".`");
            name.push_str(table);
            name.push('`');
        }
        if let Some(&cached) = self.read_cache().get(&name) {
            return cached == DO;
        }
        let keep = (literal_schema || self.filter_on_schemas(schema))
            && self.filter_on_tables(schema, table);
        self.write_cache().insert(name, keep);
        keep == DO
    }

    fn normalize_name<'a>(&self, name: &'a str) -> Cow<'a, str> {
        if self.case_sensitive
            || name.bytes().all(|byte| byte.is_ascii() && !byte.is_ascii_uppercase())
        {
            Cow::Borrowed(name)
        } else {
            Cow::Owned(go_simple_lowercase(name))
        }
    }

    fn filter_on_schemas(&self, schema: &str) -> bool {
        let rules = self.rules.as_ref().expect("rules present");
        if !rules.do_dbs.is_empty() {
            if !self.match_db(&rules.do_dbs, schema, true) {
                return false;
            }
        } else if !rules.ignore_dbs.is_empty()
            && self.match_db(&rules.ignore_dbs, schema, false)
        {
            return false;
        }
        true
    }

    fn filter_on_tables(&self, schema: &str, table: &str) -> bool {
        // Schema-level statements (create/drop/alter database) carry no table.
        if table.is_empty() {
            return true;
        }
        let rules = self.rules.as_ref().expect("rules present");
        if !rules.do_tables.is_empty() && self.match_table(&rules.do_tables, schema, table, true) {
            return true;
        }
        if !rules.ignore_tables.is_empty()
            && self.match_table(&rules.ignore_tables, schema, table, false)
        {
            return false;
        }
        rules.do_tables.is_empty()
    }

    fn match_db(&self, pattern_dbs: &[String], a: &str, is_allow_check: bool) -> bool {
        for b in pattern_dbs {
            if let Some(rest) = b.strip_prefix('~') {
                if self.match_string(rest, a) {
                    return true;
                }
            }
        }
        let rule_set = self.selector.match_rules(a, "").unwrap_or_default();
        rule_set
            .iter()
            .any(|r| r.kind == RuleKind::Db && r.is_allow_list == is_allow_check)
    }

    fn match_table(
        &self,
        pattern_tbs: &[Table],
        schema: &str,
        table: &str,
        is_allow_check: bool,
    ) -> bool {
        // The schema phase already matched the single allow rule. A quoted
        // schema can alias a compound selector key, so preserve the normal
        // trie reads for that case.
        if self.single_table_allow && !schema.contains('`') {
            if let Some(pattern) = pattern_tbs[0].name.strip_prefix('~') {
                if self.match_string(pattern, table) {
                    return true;
                }
            }
            return self.match_table_in_trie(schema, table, is_allow_check);
        }
        for ptb in pattern_tbs {
            let db_is_regex = ptb.schema.starts_with('~');
            let tbl_is_regex = ptb.name.starts_with('~');
            if db_is_regex && tbl_is_regex {
                if self.match_string(&ptb.schema[1..], schema)
                    && self.match_string(&ptb.name[1..], table)
                {
                    return true;
                }
            } else if db_is_regex && !tbl_is_regex {
                if !self.match_string(&ptb.schema[1..], schema) {
                    continue;
                }
                let rule_set = self.selector.match_rules(table, "").unwrap_or_default();
                if rule_set.iter().any(|r| {
                    r.kind == RuleKind::TblOnlyTblPart && r.is_allow_list == is_allow_check
                }) {
                    return true;
                }
            }
            let rule_set = self
                .selector
                .match_rules(schema, "")
                .unwrap_or_default();
            for r in &rule_set {
                if r.kind == RuleKind::TblOnlyDbPart
                    && r.is_allow_list == is_allow_check
                    && r.r.as_ref().is_some_and(|re| re.is_match(table))
                {
                    return true;
                }
            }
            if self.match_table_in_trie(schema, table, is_allow_check) {
                return true;
            }
        }
        false
    }

    fn match_table_in_trie(&self, schema: &str, table: &str, is_allow_check: bool) -> bool {
        self.selector
            .match_rules(schema, table)
            .unwrap_or_default()
            .iter()
            .any(|r| r.kind == RuleKind::TblFull && r.is_allow_list == is_allow_check)
    }

    fn match_string(&self, pattern: &str, t: &str) -> bool {
        match self.pattern_map.get(pattern) {
            Some(re) => re.is_match(t),
            None => pattern == t,
        }
    }
}

// Go `initRules`: compiles regexes and inserts trie rules.
fn init_rules(
    selector: &TrieSelector<NodeEndRule>,
    pattern_map: &mut Patterns,
    rules: Option<&Rules>,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    let Some(rules) = rules else {
        return Ok(());
    };
    for db in &rules.do_dbs {
        if db.is_empty() {
            return Err(FilterError(
                "DoDB rule's DB string cannot be empty".to_owned(),
            ));
        }
        init_schema_rule(selector, pattern_map, db, true, case_sensitive)?;
    }
    for table in &rules.do_tables {
        if table.schema.is_empty() || table.name.is_empty() {
            return Err(FilterError(
                "DoTables rule's DB string or Table string cannot be empty".to_owned(),
            ));
        }
        init_table_rule(
            selector,
            pattern_map,
            &table.schema,
            &table.name,
            true,
            case_sensitive,
        )?;
    }
    for db in &rules.ignore_dbs {
        if db.is_empty() {
            return Err(FilterError(
                "IgnoreDB rule's DB string cannot be empty".to_owned(),
            ));
        }
        init_schema_rule(selector, pattern_map, db, false, case_sensitive)?;
    }
    for table in &rules.ignore_tables {
        if table.schema.is_empty() || table.name.is_empty() {
            return Err(FilterError(
                "IgnoreTables rule's DB string or Table string cannot be empty".to_owned(),
            ));
        }
        init_table_rule(
            selector,
            pattern_map,
            &table.schema,
            &table.name,
            false,
            case_sensitive,
        )?;
    }
    Ok(())
}

// Go `initOneRegex`: compiles `origin` (case-insensitively when the filter
// is), caching it under its original string key.
fn init_one_regex(
    pattern_map: &mut Patterns,
    origin: &str,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    if !pattern_map.contains_key(origin) {
        let predicate = tidb_go_regexp::compile_predicate(origin, !case_sensitive, false, false)
            .map_err(FilterError)?;
        pattern_map.insert(origin.to_owned(), Arc::new(predicate));
    }
    Ok(())
}

// Go `initSchemaRule`.
fn init_schema_rule(
    selector: &TrieSelector<NodeEndRule>,
    pattern_map: &mut Patterns,
    db_str: &str,
    is_allow_list: bool,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    if let Some(rest) = db_str.strip_prefix('~') {
        return init_one_regex(pattern_map, rest, case_sensitive);
    }
    selector
        .insert(
            db_str,
            "",
            Some(NodeEndRule {
                r: None,
                kind: RuleKind::Db,
                is_allow_list,
            }),
            InsertType::Append,
        )
        .map_err(|error| FilterError(error.to_string()))
}

// Go `initTableRule`.
fn init_table_rule(
    selector: &TrieSelector<NodeEndRule>,
    pattern_map: &mut Patterns,
    db_str: &str,
    table_str: &str,
    is_allow_list: bool,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    let db_is_regex = db_str.starts_with('~');
    let tbl_is_regex = table_str.starts_with('~');
    if db_is_regex && tbl_is_regex {
        init_one_regex(pattern_map, &db_str[1..], case_sensitive)?;
        init_one_regex(pattern_map, &table_str[1..], case_sensitive)?;
    } else if db_is_regex && !tbl_is_regex {
        init_one_regex(pattern_map, &db_str[1..], case_sensitive)?;
        selector
            .insert(
                table_str,
                "",
                Some(NodeEndRule {
                    r: None,
                    kind: RuleKind::TblOnlyTblPart,
                    is_allow_list,
                }),
                InsertType::Append,
            )
            .map_err(|error| FilterError(error.to_string()))?;
    } else if !db_is_regex && tbl_is_regex {
        init_one_regex(pattern_map, &table_str[1..], case_sensitive)?;
        selector
            .insert(
                db_str,
                "",
                Some(NodeEndRule {
                    r: pattern_map.get(&table_str[1..]).cloned(),
                    kind: RuleKind::TblOnlyDbPart,
                    is_allow_list,
                }),
                InsertType::Append,
            )
            .map_err(|error| FilterError(error.to_string()))?;
    } else {
        selector
            .insert(
                db_str,
                table_str,
                Some(NodeEndRule {
                    r: None,
                    kind: RuleKind::TblFull,
                    is_allow_list,
                }),
                InsertType::Append,
            )
            .map_err(|error| FilterError(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
