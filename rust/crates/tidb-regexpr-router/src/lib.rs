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

//! Regular-expression-aware routing from Go `pkg/util/regexpr-router`.
//!
//! Each route rule delegates schema/table selection to [`tidb_filter`].
//! Table-level matches take priority over schema-level matches, and more than
//! one selected rule is rejected. Rule validation and extractor configuration
//! are shared with [`tidb_table_router`].

use tidb_filter::{Filter, Rules};
use tidb_table_filter::Table as FilterTable;
use tidb_table_router::TableRule;
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

const MIN_CACHED_RULES: usize = 8;
const MAX_CACHED_MATCHES: usize = 1024;

/// The integer classification used by the Go router's filter wrappers.
pub type FilterType = i32;

/// A table-level filter (`TblFilter` in Go).
pub const TBL_FILTER: FilterType = 1;

/// A schema-level filter (`SchmFilter` in Go).
pub const SCHM_FILTER: FilterType = 2;

/// An error returned while constructing or applying a regexp route table.
#[derive(Debug)]
pub struct RegExprRouterError(String);

impl RegExprRouterError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for RegExprRouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegExprRouterError {}

struct FilterWrapper {
    filter: Filter,
    raw_rule: TableRule,
    target: FilterTable,
    typ: FilterType,
}

#[derive(Clone, Copy, Default)]
struct Matches {
    schema: MatchingLevel,
    table: MatchingLevel,
}

#[derive(Clone, Copy, Default)]
struct MatchingLevel {
    first: Option<usize>,
    ambiguous: bool,
}

impl MatchingLevel {
    fn add(&mut self, index: usize) {
        if self.first.is_none() {
            self.first = Some(index);
        } else {
            self.ambiguous = true;
        }
    }
}

/// Routes source schema/table names through regexp-aware filter rules.
pub struct RouteTable {
    filters: Vec<FilterWrapper>,
    case_sensitive: bool,
    matches: RwLock<HashMap<String, Matches>>,
}

impl RouteTable {
    /// Go `NewRegExprRouter`.
    pub fn new(case_sensitive: bool, rules: &mut [TableRule]) -> Result<Self, RegExprRouterError> {
        let mut router = Self {
            filters: Vec::new(),
            case_sensitive,
            matches: RwLock::new(HashMap::new()),
        };
        for rule in rules {
            router.add_rule(rule)?;
        }
        Ok(router)
    }

    /// Go `AddRule`.
    ///
    /// Case-insensitive routers mutate the caller's source patterns to
    /// lowercase, matching the Go pointer-based API.
    pub fn add_rule(&mut self, rule: &mut TableRule) -> Result<(), RegExprRouterError> {
        rule.valid()
            .map_err(|error| RegExprRouterError::new(error.to_string()))?;
        if !self.case_sensitive {
            rule.to_lower();
        }

        let target = FilterTable {
            schema: rule.target_schema.clone(),
            name: rule.target_table.clone(),
        };
        let (typ, rules) = if rule.table_pattern.is_empty() {
            (
                SCHM_FILTER,
                Rules {
                    do_dbs: vec![rule.schema_pattern.clone()],
                    ..Rules::default()
                },
            )
        } else {
            (
                TBL_FILTER,
                Rules {
                    do_tables: vec![FilterTable {
                        schema: rule.schema_pattern.clone(),
                        name: rule.table_pattern.clone(),
                    }],
                    do_dbs: vec![rule.schema_pattern.clone()],
                    ..Rules::default()
                },
            )
        };
        let filter = Filter::new(self.case_sensitive, Some(rules)).map_err(|error| {
            RegExprRouterError::new(format!("add rule {rule:?} into table router: {error}"))
        })?;
        self.filters.push(FilterWrapper {
            filter,
            raw_rule: rule.clone(),
            target,
            typ,
        });
        self.matches.get_mut().unwrap_or_else(|e| e.into_inner()).clear();
        Ok(())
    }

    fn match_rules(&self, schema: &str, table: &str) -> Matches {
        // Match the name representation used by each replication filter's cache.
        let key = (self.filters.len() >= MIN_CACHED_RULES).then(|| {
            if table.is_empty() {
                format!("`{schema}`")
            } else {
                format!("`{schema}`.`{table}`")
            }
        });
        if let Some(key) = &key {
            let cache = self.matches.read().unwrap_or_else(|e| e.into_inner());
            if let Some(&cached) = cache.get(key) {
                return cached;
            }
        }
        let current = FilterTable {
            schema: schema.to_owned(),
            name: table.to_owned(),
        };
        let mut matched = Matches::default();
        for (index, wrapper) in self.filters.iter().enumerate() {
            if wrapper.filter.matches(&current) {
                if wrapper.typ == TBL_FILTER {
                    matched.table.add(index);
                } else {
                    matched.schema.add(index);
                }
            }
        }
        if let Some(key) = key {
            let mut cache = self.matches.write().unwrap_or_else(|e| e.into_inner());
            if let Some(&cached) = cache.get(&key) {
                return cached;
            }
            if cache.len() >= MAX_CACHED_MATCHES {
                if let Some(existing) = cache.keys().next().cloned() {
                    cache.remove(&existing);
                }
            }
            cache.insert(key, matched);
        }
        matched
    }

    /// Go `Route`.
    pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), RegExprRouterError> {
        let matched = self.match_rules(schema, table);
        let selected = if table.is_empty() || matched.table.first.is_none() {
            matched.schema
        } else {
            matched.table
        };
        if selected.ambiguous {
            return Err(Self::multiple_rules_error(schema, table));
        }
        let (target_schema, target_table) = selected
            .first
            .map(|i| (self.filters[i].target.schema.as_str(), self.filters[i].target.name.as_str()))
            .unwrap_or_default();
        let target_schema = if target_schema.is_empty() { schema } else { target_schema };
        let target_table = if target_table.is_empty() { table } else { target_table };
        Ok((target_schema.to_owned(), target_table.to_owned()))
    }

    /// Go `AllRules`, preserving insertion order within each rule class.
    pub fn all_rules(&self) -> (Vec<TableRule>, Vec<TableRule>) {
        let mut schema_rules = Vec::new();
        let mut table_rules = Vec::new();
        for wrapper in &self.filters {
            if wrapper.typ == SCHM_FILTER {
                schema_rules.push(wrapper.raw_rule.clone());
            } else {
                table_rules.push(wrapper.raw_rule.clone());
            }
        }
        (schema_rules, table_rules)
    }

    /// Go `FetchExtendColumn`.
    pub fn fetch_extend_column(
        &self,
        schema: &str,
        table: &str,
        source: &str,
    ) -> (Vec<String>, Vec<String>) {
        let matched = self.match_rules(schema, table);
        let Some(index) = matched.table.first.or(matched.schema.first) else {
            return (Vec::new(), Vec::new());
        };
        self.filters[index].raw_rule.extract_columns(schema, table, source)
    }

    fn multiple_rules_error(schema: &str, table: &str) -> RegExprRouterError {
        RegExprRouterError::new(format!("table {schema}.{table} matches more than one rule"))
    }
}

#[cfg(test)]
mod tests;
