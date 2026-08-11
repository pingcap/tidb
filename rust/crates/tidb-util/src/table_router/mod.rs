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

//! Schema/table routing from Go `pkg/util/table-router`.
//!
//! Rules use the complete byte-oriented wildcard implementation from
//! [`crate::table_rule_selector`]. Table rules take priority over schema
//! rules, while extractor regular expressions concatenate capture groups in
//! source order. Configuration field names retain the Go JSON/TOML/YAML tags.

use crate::table_rule_selector::{InsertType, Selector, TrieSelector};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tidb_mysql::to_lowercase as go_simple_lowercase;

/// An error returned by table-rule validation or routing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRouterError(String);

impl TableRouterError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn annotate(self, context: impl fmt::Display) -> Self {
        Self(format!("{context}: {}", self.0))
    }
}

impl fmt::Display for TableRouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TableRouterError {}

/// Extracts capture groups from a table name into one target column.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableExtractor {
    /// Destination column receiving the extracted value.
    #[serde(rename = "target-column")]
    pub target_column: String,
    /// Regular expression matched against the source table name.
    #[serde(rename = "table-regexp")]
    pub table_regexp: String,
    #[serde(skip)]
    regexp: Option<Regex>,
}

impl TableExtractor {
    /// Creates a table-name extractor.
    #[must_use]
    pub fn new(target_column: impl Into<String>, table_regexp: impl Into<String>) -> Self {
        Self {
            target_column: target_column.into(),
            table_regexp: table_regexp.into(),
            regexp: None,
        }
    }
}

/// Extracts capture groups from a schema name into one target column.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaExtractor {
    /// Destination column receiving the extracted value.
    #[serde(rename = "target-column")]
    pub target_column: String,
    /// Regular expression matched against the source schema name.
    #[serde(rename = "schema-regexp")]
    pub schema_regexp: String,
    #[serde(skip)]
    regexp: Option<Regex>,
}

impl SchemaExtractor {
    /// Creates a schema-name extractor.
    #[must_use]
    pub fn new(target_column: impl Into<String>, schema_regexp: impl Into<String>) -> Self {
        Self {
            target_column: target_column.into(),
            schema_regexp: schema_regexp.into(),
            regexp: None,
        }
    }
}

/// Extracts capture groups from a source name into one target column.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SourceExtractor {
    /// Destination column receiving the extracted value.
    #[serde(rename = "target-column")]
    pub target_column: String,
    /// Regular expression matched against the source identifier.
    #[serde(rename = "source-regexp")]
    pub source_regexp: String,
    #[serde(skip)]
    regexp: Option<Regex>,
}

impl SourceExtractor {
    /// Creates a source-name extractor.
    #[must_use]
    pub fn new(target_column: impl Into<String>, source_regexp: impl Into<String>) -> Self {
        Self {
            target_column: target_column.into(),
            source_regexp: source_regexp.into(),
            regexp: None,
        }
    }
}

/// A rule mapping one source schema/table pattern to a target.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableRule {
    /// Optional table-name extractor.
    #[serde(
        rename = "extract-table",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub table_extractor: Option<TableExtractor>,
    /// Optional schema-name extractor.
    #[serde(
        rename = "extract-schema",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_extractor: Option<SchemaExtractor>,
    /// Optional source-name extractor.
    #[serde(
        rename = "extract-source",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub source_extractor: Option<SourceExtractor>,
    /// Source schema wildcard pattern.
    #[serde(rename = "schema-pattern")]
    pub schema_pattern: String,
    /// Source table wildcard pattern; empty denotes a schema-level rule.
    #[serde(rename = "table-pattern")]
    pub table_pattern: String,
    /// Routed schema name.
    #[serde(rename = "target-schema")]
    pub target_schema: String,
    /// Routed table name; empty preserves the source table name.
    #[serde(rename = "target-table")]
    pub target_table: String,
}

impl TableRule {
    /// Creates a route rule without extractors.
    #[must_use]
    pub fn new(
        schema_pattern: impl Into<String>,
        table_pattern: impl Into<String>,
        target_schema: impl Into<String>,
        target_table: impl Into<String>,
    ) -> Self {
        Self {
            schema_pattern: schema_pattern.into(),
            table_pattern: table_pattern.into(),
            target_schema: target_schema.into(),
            target_table: target_table.into(),
            ..Self::default()
        }
    }

    /// Go `TableRule.Valid`: validates required fields and compiles extractors.
    pub fn valid(&mut self) -> Result<(), TableRouterError> {
        if self.schema_pattern.is_empty() {
            return Err(TableRouterError::new(
                "schema pattern of table route rule should not be empty",
            ));
        }
        if self.target_schema.is_empty() {
            return Err(TableRouterError::new(
                "target schema of table route rule should not be empty",
            ));
        }

        if let Some(extractor) = &mut self.table_extractor {
            extractor.regexp = Some(Regex::new(&extractor.table_regexp).map_err(|_| {
                TableRouterError::new(format!(
                    "table extractor table regexp illegal {}",
                    extractor.table_regexp
                ))
            })?);
            if extractor.target_column.is_empty() {
                return Err(TableRouterError::new(
                    "table extractor target column cannot be empty",
                ));
            }
        }
        if let Some(extractor) = &mut self.schema_extractor {
            extractor.regexp = Some(Regex::new(&extractor.schema_regexp).map_err(|_| {
                TableRouterError::new(format!(
                    "schema extractor schema regexp illegal {}",
                    extractor.schema_regexp
                ))
            })?);
            if extractor.target_column.is_empty() {
                return Err(TableRouterError::new(
                    "schema extractor target column cannot be empty",
                ));
            }
        }
        if let Some(extractor) = &mut self.source_extractor {
            extractor.regexp = Some(Regex::new(&extractor.source_regexp).map_err(|_| {
                TableRouterError::new(format!(
                    "source extractor source regexp illegal {}",
                    extractor.source_regexp
                ))
            })?);
            if extractor.target_column.is_empty() {
                return Err(TableRouterError::new(
                    "source extractor target column cannot be empty",
                ));
            }
        }
        Ok(())
    }

    /// Go `TableRule.ToLower`: lowercases only source patterns.
    pub fn to_lower(&mut self) {
        self.schema_pattern = go_simple_lowercase(&self.schema_pattern);
        self.table_pattern = go_simple_lowercase(&self.table_pattern);
    }
}

#[derive(Clone)]
enum RuleEntry {
    Rule(Arc<TableRule>),
    #[cfg(test)]
    Invalid(String),
}

type ClassifiedRules = (Vec<Arc<TableRule>>, Vec<Arc<TableRule>>);

/// Routes source schema/table names according to [`TableRule`] values.
pub struct Table {
    selector: TrieSelector<RuleEntry>,
    case_sensitive: bool,
}

impl Table {
    /// Go `NewTableRouter`.
    pub fn new(case_sensitive: bool, rules: &mut [TableRule]) -> Result<Self, TableRouterError> {
        let router = Self {
            selector: TrieSelector::new(),
            case_sensitive,
        };
        for rule in rules {
            router.add_rule(rule).map_err(|error| {
                error.annotate(format_args!("initial rule {rule:?} in table router"))
            })?;
        }
        Ok(router)
    }

    /// Go `AddRule`.
    pub fn add_rule(&self, rule: &mut TableRule) -> Result<(), TableRouterError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        self.selector
            .insert(
                &rule.schema_pattern,
                &rule.table_pattern,
                Some(RuleEntry::Rule(Arc::new(rule.clone()))),
                InsertType::Insert,
            )
            .map_err(|error| {
                TableRouterError::new(error.to_string())
                    .annotate(format_args!("add rule {rule:?} into table router"))
            })
    }

    /// Go `UpdateRule`.
    pub fn update_rule(&self, rule: &mut TableRule) -> Result<(), TableRouterError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        self.selector
            .insert(
                &rule.schema_pattern,
                &rule.table_pattern,
                Some(RuleEntry::Rule(Arc::new(rule.clone()))),
                InsertType::Replace,
            )
            .map_err(|error| {
                TableRouterError::new(error.to_string())
                    .annotate(format_args!("update rule {rule:?} into table router"))
            })
    }

    /// Go `RemoveRule`.
    pub fn remove_rule(&self, rule: &mut TableRule) -> Result<(), TableRouterError> {
        if !self.case_sensitive {
            rule.to_lower();
        }
        self.selector
            .remove(&rule.schema_pattern, &rule.table_pattern)
            .map_err(|error| {
                TableRouterError::new(error.to_string())
                    .annotate(format_args!("remove rule {rule:?} from table router"))
            })
    }

    /// Go `Route`: table-level rules have priority over schema-level rules.
    pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), TableRouterError> {
        let (schema_match, table_match) = if self.case_sensitive {
            (schema.to_owned(), table.to_owned())
        } else {
            (go_simple_lowercase(schema), go_simple_lowercase(table))
        };
        let (schema_rules, table_rules) =
            Self::classify(self.selector.match_rules(&schema_match, &table_match))?;

        let selected = if table.is_empty() || table_rules.is_empty() {
            if schema_rules.len() > 1 {
                return Err(Self::multiple_rules_error(
                    schema,
                    table,
                    "schema",
                    &schema_rules,
                ));
            }
            schema_rules.first()
        } else {
            if table_rules.len() > 1 {
                return Err(Self::multiple_rules_error(
                    schema,
                    table,
                    "table",
                    &table_rules,
                ));
            }
            table_rules.first()
        };

        let mut target_schema = selected
            .map(|rule| rule.target_schema.clone())
            .unwrap_or_default();
        let mut target_table = selected
            .map(|rule| rule.target_table.clone())
            .unwrap_or_default();
        if target_schema.is_empty() {
            target_schema = schema.to_owned();
        }
        if target_table.is_empty() {
            target_table = table.to_owned();
        }
        Ok((target_schema, target_table))
    }

    /// Go `FetchExtendColumn`.
    pub fn fetch_extend_column(
        &self,
        schema: &str,
        table: &str,
        source: &str,
    ) -> (Vec<String>, Vec<String>) {
        // The Go method deliberately does not lowercase these inputs before
        // matching, even for a case-insensitive router.
        let Ok((schema_rules, table_rules)) =
            Self::classify(self.selector.match_rules(schema, table))
        else {
            return (Vec::new(), Vec::new());
        };
        let rule = if table_rules.is_empty() {
            schema_rules.first()
        } else {
            table_rules.first()
        };
        let Some(rule) = rule else {
            return (Vec::new(), Vec::new());
        };

        let mut columns = Vec::new();
        let mut values = Vec::new();
        if let Some(extractor) = &rule.table_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(table, extractor.regexp.as_ref()));
        }
        if let Some(extractor) = &rule.schema_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(schema, extractor.regexp.as_ref()));
        }
        if let Some(extractor) = &rule.source_extractor {
            columns.push(extractor.target_column.clone());
            values.push(extract_value(source, extractor.regexp.as_ref()));
        }
        (columns, values)
    }

    fn classify(entries: Vec<RuleEntry>) -> Result<ClassifiedRules, TableRouterError> {
        let mut schema_rules = Vec::with_capacity(entries.len());
        let mut table_rules = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                RuleEntry::Rule(rule) if rule.table_pattern.is_empty() => {
                    schema_rules.push(rule);
                }
                RuleEntry::Rule(rule) => table_rules.push(rule),
                #[cfg(test)]
                RuleEntry::Invalid(value) => {
                    return Err(TableRouterError::new(format!(
                        "table route rule {value:?} is not valid"
                    )));
                }
            }
        }
        Ok((schema_rules, table_rules))
    }

    fn multiple_rules_error(
        schema: &str,
        table: &str,
        level: &str,
        rules: &[Arc<TableRule>],
    ) -> TableRouterError {
        TableRouterError::new(format!(
            "`{schema}`.`{table}` matches {} {level} route rules which is more than one.\nThe first two rules are {:?}, {:?}.\nIt's not supported",
            rules.len(), rules[0], rules[1]
        ))
    }

    #[cfg(test)]
    fn insert_invalid_for_test(
        &self,
        schema_pattern: &str,
        table_pattern: &str,
        value: &str,
    ) -> Result<(), TableRouterError> {
        self.selector
            .insert(
                schema_pattern,
                table_pattern,
                Some(RuleEntry::Invalid(value.to_owned())),
                InsertType::Insert,
            )
            .map_err(|error| TableRouterError::new(error.to_string()))
    }
}

fn extract_value(value: &str, regexp: Option<&Regex>) -> String {
    let Some(captures) = regexp.and_then(|regexp| regexp.captures(value)) else {
        return String::new();
    };
    let mut result = String::new();
    for index in 1..captures.len() {
        if let Some(value) = captures.get(index) {
            result.push_str(value.as_str());
        }
    }
    result
}

#[cfg(test)]
mod tests;
