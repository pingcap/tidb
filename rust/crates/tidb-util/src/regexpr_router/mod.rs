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
//! Each route rule delegates schema/table selection to [`crate::filter`].
//! Table-level matches take priority over schema-level matches, and more than
//! one selected rule is rejected. Rule validation and extractor configuration
//! are shared with [`crate::table_router`].

use crate::filter::{Filter, Rules};
use crate::table_filter::Table as FilterTable;
use crate::table_router::TableRule;
use regex::Regex;
use std::fmt;

/// The integer classification used by the Go router's filter wrappers.
pub type FilterType = i32;

/// A table-level filter (`TblFilter` in Go).
pub const TBL_FILTER: FilterType = 1;

/// A schema-level filter (`SchmFilter` in Go).
pub const SCHM_FILTER: FilterType = 2;

/// An error returned while constructing or applying a regexp route table.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Routes source schema/table names through regexp-aware filter rules.
pub struct RouteTable {
    filters: Vec<FilterWrapper>,
    case_sensitive: bool,
}

impl RouteTable {
    /// Go `NewRegExprRouter`.
    pub fn new(case_sensitive: bool, rules: &mut [TableRule]) -> Result<Self, RegExprRouterError> {
        let mut router = Self {
            filters: Vec::new(),
            case_sensitive,
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

        let target = FilterTable::new(&rule.target_schema, &rule.target_table);
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
                    do_tables: vec![FilterTable::new(&rule.schema_pattern, &rule.table_pattern)],
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
        Ok(())
    }

    /// Go `Route`.
    pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), RegExprRouterError> {
        let current = FilterTable::new(schema, table);
        let mut table_rules = Vec::new();
        let mut schema_rules = Vec::new();
        for wrapper in &self.filters {
            if wrapper.filter.matches(&current) {
                if wrapper.typ == TBL_FILTER {
                    table_rules.push(wrapper);
                } else {
                    schema_rules.push(wrapper);
                }
            }
        }

        let selected = if table.is_empty() || table_rules.is_empty() {
            if schema_rules.len() > 1 {
                return Err(Self::multiple_rules_error(schema, table));
            }
            schema_rules.first()
        } else {
            if table_rules.len() > 1 {
                return Err(Self::multiple_rules_error(schema, table));
            }
            table_rules.first()
        };

        let mut target_schema = selected
            .map(|wrapper| wrapper.target.schema.clone())
            .unwrap_or_default();
        let mut target_table = selected
            .map(|wrapper| wrapper.target.name.clone())
            .unwrap_or_default();
        if target_schema.is_empty() {
            target_schema = schema.to_owned();
        }
        if target_table.is_empty() {
            target_table = table.to_owned();
        }
        Ok((target_schema, target_table))
    }

    /// Go `AllRules`, preserving insertion order within each rule class.
    #[must_use]
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
    #[must_use]
    pub fn fetch_extend_column(
        &self,
        schema: &str,
        table: &str,
        source: &str,
    ) -> (Vec<String>, Vec<String>) {
        let current = FilterTable::new(schema, table);
        let mut schema_rules = Vec::new();
        let mut table_rules = Vec::new();
        for wrapper in &self.filters {
            if wrapper.filter.matches(&current) {
                if wrapper.raw_rule.table_pattern.is_empty() {
                    schema_rules.push(&wrapper.raw_rule);
                } else {
                    table_rules.push(&wrapper.raw_rule);
                }
            }
        }
        let selected = if table_rules.is_empty() {
            schema_rules.first()
        } else {
            table_rules.first()
        };
        let Some(rule) = selected else {
            return (Vec::new(), Vec::new());
        };

        let mut columns = Vec::new();
        let mut values = Vec::new();
        if let Some(extractor) = &rule.table_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(table, &extractor.table_regexp));
        }
        if let Some(extractor) = &rule.schema_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(schema, &extractor.schema_regexp));
        }
        if let Some(extractor) = &rule.source_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(source, &extractor.source_regexp));
        }
        (columns, values)
    }

    fn multiple_rules_error(schema: &str, table: &str) -> RegExprRouterError {
        RegExprRouterError::new(format!("table {schema}.{table} matches more than one rule"))
    }
}

fn extract_value(value: &str, pattern: &str) -> String {
    let Some(captures) = Regex::new(pattern)
        .ok()
        .and_then(|regexp| regexp.captures(value))
    else {
        return String::new();
    };
    let mut result = String::new();
    for index in 1..captures.len() {
        if let Some(capture) = captures.get(index) {
            result.push_str(capture.as_str());
        }
    }
    result
}

#[cfg(test)]
mod tests;
