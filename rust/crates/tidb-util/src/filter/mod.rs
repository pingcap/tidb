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
//! A rule whose value begins with `~` is a regular expression (compiled with
//! the `regex` crate, matching Go's `regexp`); any other value is a glob-like
//! pattern handled by the trie [`Selector`](crate::table_rule_selector).
//! Regex look-around (`(?!...)`, `(?=...)`) is rejected. The selector and
//! result cache are internally synchronized for concurrent callers.

mod schema;
pub use schema::{is_system_schema, DM_HEARTBEAT_SCHEMA, INSPECTION_SCHEMA_NAME};

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use regex::Regex;
use regex_syntax::ast::parse::Parser;
use regex_syntax::ast::{self, Ast, Visitor};
use tidb_mysql::to_lowercase as go_simple_lowercase;

use crate::table_filter::{MySQLReplicationRules, Table};
use crate::table_rule_selector::{InsertType, Selector, SelectorError, TrieSelector};

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
pub enum FilterError {
    /// A DoDB/IgnoreDB rule had an empty database string.
    EmptyDb,
    /// A DoTables/IgnoreTables rule had an empty schema or table string.
    EmptyTable,
    /// A regex rule failed to compile.
    Regex(String),
    /// The trie selector rejected an insert.
    Selector(SelectorError),
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::EmptyDb => f.write_str("DoDB/IgnoreDB rule's DB string cannot be empty"),
            FilterError::EmptyTable => f.write_str(
                "DoTables/IgnoreTables rule's DB string or Table string cannot be empty",
            ),
            FilterError::Regex(e) => write!(f, "{e}"),
            FilterError::Selector(e) => write!(f, "{e}"),
        }
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
    r: Option<Regex>,
    kind: RuleKind,
    is_allow_list: bool,
}

/// A table filter in the style of MySQL replication rules (Go `Filter`).
pub struct Filter {
    selector: TrieSelector<NodeEndRule>,
    pattern_map: HashMap<String, Regex>,
    rules: Option<Rules>,
    cache: RwLock<HashMap<String, ActionType>>,
    case_sensitive: bool,
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
        let mut pattern_map: HashMap<String, Regex> = HashMap::new();
        init_rules(&selector, &mut pattern_map, rules.as_ref(), case_sensitive)?;
        Ok(Filter {
            selector,
            pattern_map,
            rules,
            cache: RwLock::new(HashMap::new()),
            case_sensitive,
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
    #[must_use]
    pub fn apply_on(&self, stbs: &[Table]) -> Vec<Table> {
        if self.rules.is_none() {
            return stbs.to_vec();
        }
        let mut tbs = Vec::new();
        for tb in stbs {
            let mut new_tb = tb.clone();
            if !self.case_sensitive {
                new_tb.schema = go_simple_lowercase(&new_tb.schema);
                new_tb.name = go_simple_lowercase(&new_tb.name);
            }
            if self.matches(&new_tb) {
                tbs.push(new_tb);
            }
        }
        tbs
    }

    /// Go `Apply`: returns the original input tables that pass the filter.
    #[must_use]
    pub fn apply(&self, stbs: &[Table]) -> Vec<Table> {
        if self.rules.is_none() {
            return stbs.to_vec();
        }
        let mut tbs = Vec::new();
        for tb in stbs {
            let probe = if self.case_sensitive {
                tb.clone()
            } else {
                Table::new(
                    go_simple_lowercase(&tb.schema),
                    go_simple_lowercase(&tb.name),
                )
            };
            if self.matches(&probe) {
                tbs.push(tb.clone());
            }
        }
        tbs
    }

    /// Go `Match`: whether `tb` should be kept (not filtered out).
    #[must_use]
    pub fn matches(&self, tb: &Table) -> bool {
        let Some(_rules) = self.rules.as_ref() else {
            return true;
        };
        let mut new_tb = tb.clone();
        if !self.case_sensitive {
            new_tb.schema = go_simple_lowercase(&new_tb.schema);
            new_tb.name = go_simple_lowercase(&new_tb.name);
        }
        let name = new_tb.to_string();
        if let Some(&cached) = self.read_cache().get(&name) {
            return cached == DO;
        }
        let do_ = self.filter_on_schemas(&new_tb) && self.filter_on_tables(&new_tb);
        self.write_cache().insert(new_tb.to_string(), do_);
        do_ == DO
    }

    fn filter_on_schemas(&self, tb: &Table) -> bool {
        let rules = self.rules.as_ref().expect("rules present");
        if !rules.do_dbs.is_empty() {
            if !self.match_db(&rules.do_dbs, &tb.schema, true) {
                return false;
            }
        } else if !rules.ignore_dbs.is_empty()
            && self.match_db(&rules.ignore_dbs, &tb.schema, false)
        {
            return false;
        }
        true
    }

    fn filter_on_tables(&self, tb: &Table) -> bool {
        // Schema-level statements (create/drop/alter database) carry no table.
        if tb.name.is_empty() {
            return true;
        }
        let rules = self.rules.as_ref().expect("rules present");
        if !rules.do_tables.is_empty() && self.match_table(&rules.do_tables, tb, true) {
            return true;
        }
        if !rules.ignore_tables.is_empty() && self.match_table(&rules.ignore_tables, tb, false) {
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
        let rule_set = self.selector.match_rules(a, "");
        rule_set
            .iter()
            .any(|r| r.kind == RuleKind::Db && r.is_allow_list == is_allow_check)
    }

    fn match_table(&self, pattern_tbs: &[Table], tb: &Table, is_allow_check: bool) -> bool {
        for ptb in pattern_tbs {
            let db_is_regex = ptb.schema.starts_with('~');
            let tbl_is_regex = ptb.name.starts_with('~');
            if db_is_regex && tbl_is_regex {
                if self.match_string(&ptb.schema[1..], &tb.schema)
                    && self.match_string(&ptb.name[1..], &tb.name)
                {
                    return true;
                }
            } else if db_is_regex && !tbl_is_regex {
                if !self.match_string(&ptb.schema[1..], &tb.schema) {
                    continue;
                }
                let rule_set = self.selector.match_rules(&tb.name, "");
                if rule_set.iter().any(|r| {
                    r.kind == RuleKind::TblOnlyTblPart && r.is_allow_list == is_allow_check
                }) {
                    return true;
                }
            }
            let rule_set = self.selector.match_rules(&tb.schema, "");
            for r in &rule_set {
                if r.kind == RuleKind::TblOnlyDbPart
                    && r.is_allow_list == is_allow_check
                    && r.r.as_ref().is_some_and(|re| re.is_match(&tb.name))
                {
                    return true;
                }
            }
            let rule_set = self.selector.match_rules(&tb.schema, &tb.name);
            if rule_set
                .iter()
                .any(|r| r.kind == RuleKind::TblFull && r.is_allow_list == is_allow_check)
            {
                return true;
            }
        }
        false
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
    pattern_map: &mut HashMap<String, Regex>,
    rules: Option<&Rules>,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    let Some(rules) = rules else {
        return Ok(());
    };
    for db in &rules.do_dbs {
        if db.is_empty() {
            return Err(FilterError::EmptyDb);
        }
        init_schema_rule(selector, pattern_map, db, true, case_sensitive)?;
    }
    for table in &rules.do_tables {
        if table.schema.is_empty() || table.name.is_empty() {
            return Err(FilterError::EmptyTable);
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
            return Err(FilterError::EmptyDb);
        }
        init_schema_rule(selector, pattern_map, db, false, case_sensitive)?;
    }
    for table in &rules.ignore_tables {
        if table.schema.is_empty() || table.name.is_empty() {
            return Err(FilterError::EmptyTable);
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
    pattern_map: &mut HashMap<String, Regex>,
    origin: &str,
    case_sensitive: bool,
) -> Result<(), FilterError> {
    if !pattern_map.contains_key(origin) {
        let re = compile_go_regexp(origin, case_sensitive).map_err(FilterError::Regex)?;
        pattern_map.insert(origin.to_owned(), re);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RegexReplacement {
    start: usize,
    end: usize,
    value: &'static str,
}

#[derive(Default)]
struct GoRegexpVisitor {
    replacements: Vec<RegexReplacement>,
}

impl GoRegexpVisitor {
    fn perl_class(class: &ast::ClassPerl) -> RegexReplacement {
        let value = match (&class.kind, class.negated) {
            (ast::ClassPerlKind::Digit, false) => "[0-9]",
            (ast::ClassPerlKind::Digit, true) => "[^0-9]",
            (ast::ClassPerlKind::Space, false) => "[\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Space, true) => "[^\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Word, false) => "[0-9A-Za-z_]",
            (ast::ClassPerlKind::Word, true) => "[^0-9A-Za-z_]",
        };
        RegexReplacement {
            start: class.span.start.offset,
            end: class.span.end.offset,
            value,
        }
    }
}

impl Visitor for GoRegexpVisitor {
    type Output = Vec<RegexReplacement>;
    type Err = std::convert::Infallible;

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.replacements)
    }

    fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
        match node {
            Ast::ClassPerl(class) => self.replacements.push(Self::perl_class(class)),
            Ast::Assertion(assertion) => {
                let value = match assertion.kind {
                    ast::AssertionKind::WordBoundary => Some("(?-u:\\b)"),
                    ast::AssertionKind::NotWordBoundary => Some("(?-u:\\B)"),
                    _ => None,
                };
                if let Some(value) = value {
                    self.replacements.push(RegexReplacement {
                        start: assertion.span.start.offset,
                        end: assertion.span.end.offset,
                        value,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ast::ClassSetItem) -> Result<(), Self::Err> {
        if let ast::ClassSetItem::Perl(class) = item {
            self.replacements.push(Self::perl_class(class));
        }
        Ok(())
    }
}

// Go's regexp package defines its Perl character classes and word boundaries
// over ASCII. Rust regex deliberately makes the same spellings Unicode-aware.
// Rewrite only those source constructs; Unicode literals, `.`, and `\p{...}`
// retain their normal rune semantics.
fn go_regexp_pattern(pattern: &str) -> Result<String, String> {
    let ast = Parser::new()
        .parse(pattern)
        .map_err(|error| error.to_string())?;
    let mut replacements =
        ast::visit(&ast, GoRegexpVisitor::default()).expect("the regexp visitor is infallible");
    if replacements.is_empty() {
        return Ok(pattern.to_owned());
    }
    replacements.sort_unstable_by_key(|replacement| std::cmp::Reverse(replacement.start));
    let mut result = pattern.to_owned();
    for replacement in replacements {
        result.replace_range(replacement.start..replacement.end, replacement.value);
    }
    Ok(result)
}

pub(crate) fn compile_go_regexp(pattern: &str, case_sensitive: bool) -> Result<Regex, String> {
    let pattern = go_regexp_pattern(pattern)?;
    let pattern = if case_sensitive {
        pattern
    } else {
        format!("(?i){pattern}")
    };
    Regex::new(&pattern).map_err(|error| error.to_string())
}

// Go `initSchemaRule`.
fn init_schema_rule(
    selector: &TrieSelector<NodeEndRule>,
    pattern_map: &mut HashMap<String, Regex>,
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
        .map_err(FilterError::Selector)
}

// Go `initTableRule`.
fn init_table_rule(
    selector: &TrieSelector<NodeEndRule>,
    pattern_map: &mut HashMap<String, Regex>,
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
            .map_err(FilterError::Selector)?;
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
            .map_err(FilterError::Selector)?;
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
            .map_err(FilterError::Selector)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
