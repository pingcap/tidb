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

//! Go `pkg/planner/core/memtable_infoschema_extractor.go`: the predicate
//! extractor of the `information_schema` tables that list schema objects.
//!
//! Every value is lower-cased and every `LIKE` becomes a case-insensitive
//! regexp, so `table_name = 'T'` finds table `t`: the readers look objects up
//! by name, case-insensitively. Go's reader-side listing helpers
//! (`ListSchemas`, `ListSchemasAndTables`, `ListColumns`, ...) select the
//! objects this filter keeps; this port's readers materialize every visible
//! object, and [`InfoSchemaBaseExtractor::keeps_row`] applies the same
//! [`InfoSchemaBaseExtractor::filter`] to each row.

use std::collections::BTreeMap;

use regex::Regex;
use tidb_datatype::{Datum, FieldName};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_hack::go_to_lower;

use crate::memtable_predicate_extractor::{
    extract_string_from_string_set, extract_string_from_string_slice, row_value, ExtractHelper,
    StringSet,
};

/// Go's typed `InfoSchema*Extractor` wrappers, by the table they serve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InfoSchemaExtractorKind {
    /// Go `InfoSchemaIndexesExtractor` (`TIDB_INDEXES`).
    Indexes,
    /// Go `InfoSchemaTablesExtractor` (`TABLES`).
    Tables,
    /// Go `InfoSchemaDDLExtractor` (`DDL_JOBS`).
    Ddl,
    /// Go `InfoSchemaViewsExtractor` (`VIEWS`).
    Views,
    /// Go `InfoSchemaKeyColumnUsageExtractor` (`KEY_COLUMN_USAGE`).
    KeyColumnUsage,
    /// Go `InfoSchemaTableConstraintsExtractor` (`TABLE_CONSTRAINTS`).
    TableConstraints,
    /// Go `InfoSchemaPartitionsExtractor` (`PARTITIONS`).
    Partitions,
    /// Go `InfoSchemaStatisticsExtractor` (`STATISTICS`).
    Statistics,
    /// Go `InfoSchemaSchemataExtractor` (`SCHEMATA`).
    Schemata,
    /// Go `InfoSchemaCheckConstraintsExtractor` (`CHECK_CONSTRAINTS`).
    CheckConstraints,
    /// Go `InfoSchemaTiDBCheckConstraintsExtractor` (`TIDB_CHECK_CONSTRAINTS`).
    TiDBCheckConstraints,
    /// Go `InfoSchemaReferConstExtractor` (`REFERENTIAL_CONSTRAINTS`).
    ReferConst,
    /// Go `InfoSchemaSequenceExtractor` (`SEQUENCES`).
    Sequence,
    /// Go `InfoSchemaColumnsExtractor` (`COLUMNS`).
    Columns,
    /// Go `InfoSchemaTiDBIndexUsageExtractor` (`TIDB_INDEX_USAGE`).
    TiDBIndexUsage,
}

const TABLE_SCHEMA: &str = "table_schema";
const TABLE_NAME: &str = "table_name";
const TIDB_TABLE_ID: &str = "tidb_table_id";
const PARTITION_NAME: &str = "partition_name";
const TIDB_PARTITION_ID: &str = "tidb_partition_id";
const INDEX_NAME: &str = "index_name";
const SCHEMA_NAME: &str = "schema_name";
const DB_NAME: &str = "db_name";
const CONSTRAINT_SCHEMA: &str = "constraint_schema";
const CONSTRAINT_NAME: &str = "constraint_name";
const TABLE_ID: &str = "table_id";
const SEQUENCE_SCHEMA: &str = "sequence_schema";
const SEQUENCE_NAME: &str = "sequence_name";
const COLUMN_NAME: &str = "column_name";
const DDL_STATE_NAME: &str = "state";

/// Go `patternMatchable`: the name columns a `LIKE` is turned into a regexp
/// for.
const PATTERN_MATCHABLE: &[&str] = &[
    TABLE_SCHEMA,
    TABLE_NAME,
    INDEX_NAME,
    SCHEMA_NAME,
    CONSTRAINT_SCHEMA,
    SEQUENCE_SCHEMA,
    SEQUENCE_NAME,
    COLUMN_NAME,
];

impl InfoSchemaExtractorKind {
    /// Go `colNames`, as each `NewInfoSchema*Extractor` sets it.
    const fn col_names(self) -> &'static [&'static str] {
        match self {
            Self::Indexes | Self::Views => &[TABLE_SCHEMA, TABLE_NAME],
            Self::Tables => &[TABLE_SCHEMA, TABLE_NAME, TIDB_TABLE_ID],
            Self::Ddl => &[DB_NAME, TABLE_NAME, DDL_STATE_NAME],
            Self::KeyColumnUsage | Self::TableConstraints => {
                &[TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_SCHEMA]
            }
            Self::Partitions => &[TABLE_SCHEMA, TABLE_NAME, TIDB_PARTITION_ID, PARTITION_NAME],
            Self::Statistics | Self::TiDBIndexUsage => &[TABLE_SCHEMA, TABLE_NAME, INDEX_NAME],
            Self::Schemata => &[SCHEMA_NAME],
            Self::CheckConstraints => &[CONSTRAINT_SCHEMA, CONSTRAINT_NAME],
            Self::TiDBCheckConstraints => {
                &[CONSTRAINT_SCHEMA, TABLE_NAME, TABLE_ID, CONSTRAINT_NAME]
            }
            Self::ReferConst => &[CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME],
            Self::Sequence => &[SEQUENCE_SCHEMA, SEQUENCE_NAME],
            Self::Columns => &[TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME],
        }
    }
}

/// Go `InfoSchemaBaseExtractor`.
#[derive(Clone, Debug)]
pub struct InfoSchemaBaseExtractor {
    helper: ExtractHelper,
    /// Which typed wrapper this is.
    pub kind: InfoSchemaExtractorKind,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `ColPredicates`: each extracted column's lower-cased values.
    pub col_predicates: BTreeMap<String, StringSet>,
    /// Go `colsRegexp`: each column's compiled case-insensitive regexps.
    cols_regexp: BTreeMap<String, Vec<Regex>>,
    /// Go `LikePatterns`: each column's patterns as written, for EXPLAIN.
    pub like_patterns: BTreeMap<String, Vec<String>>,
    /// Go `colNames`.
    col_names: &'static [&'static str],
}

impl InfoSchemaBaseExtractor {
    /// Go `NewInfoSchema*Extractor()`.
    #[must_use]
    pub fn new(kind: InfoSchemaExtractorKind) -> Self {
        Self {
            helper: ExtractHelper::default(),
            kind,
            skip_request: false,
            col_predicates: BTreeMap::new(),
            cols_regexp: BTreeMap::new(),
            like_patterns: BTreeMap::new(),
            col_names: kind.col_names(),
        }
    }

    /// Go `InfoSchemaBaseExtractor.Extract`, and `InfoSchemaDDLExtractor`'s
    /// override that extracts but returns every predicate: its reader uses
    /// the state to decide whether to scan history jobs and filters nothing,
    /// so the Selection stays.
    pub(crate) fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let original = (self.kind == InfoSchemaExtractorKind::Ddl).then(|| predicates.clone());
        self.col_predicates.clear();
        self.like_patterns.clear();
        self.cols_regexp.clear();
        let mut remained = predicates;
        for col_name in self.col_names {
            let (rest, skip_request, values) = self
                .helper
                .extract_col(schema, names, remained, col_name, true);
            remained = rest;
            self.skip_request = skip_request;
            if skip_request {
                break;
            }
            if !values.is_empty() {
                self.col_predicates.insert((*col_name).to_owned(), values);
            }
        }
        for col_name in self.col_names {
            if !PATTERN_MATCHABLE.contains(col_name) {
                continue;
            }
            let (new_remained, like_patterns) = ExtractHelper::extract_like_pattern_col(
                schema,
                names,
                remained.clone(),
                col_name,
                true,
                true,
            );
            if like_patterns.is_empty() {
                continue;
            }
            // EXPLAIN shows the patterns as written.
            let (_, old_like_patterns) = ExtractHelper::extract_like_pattern_col(
                schema,
                names,
                remained.clone(),
                col_name,
                true,
                false,
            );
            // Go compiles `(?i)<pattern>`; a pattern that does not compile
            // leaves every predicate in place.
            let regs: Option<Vec<Regex>> = like_patterns
                .iter()
                .map(|pattern| {
                    tidb_util::go_regexp::compile_with_flags(pattern, true, false, false).ok()
                })
                .collect();
            if let Some(regs) = regs {
                remained = new_remained;
                self.like_patterns
                    .insert((*col_name).to_owned(), old_like_patterns);
                self.cols_regexp.insert((*col_name).to_owned(), regs);
            }
        }
        original.unwrap_or(remained)
    }

    /// Go `InfoSchemaBaseExtractor.ExplainInfo`.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request:true".to_owned();
        }
        let mut parts = Vec::new();
        for (col_name, preds) in &self.col_predicates {
            if !preds.is_empty() {
                parts.push(format!(
                    "{col_name}:[{}]",
                    extract_string_from_string_set(preds)
                ));
            }
        }
        for (col_name, patterns) in &self.like_patterns {
            if !patterns.is_empty() {
                parts.push(format!(
                    "{col_name}_pattern:[{}]",
                    extract_string_from_string_slice(patterns)
                ));
            }
        }
        parts.join(", ")
    }

    /// Go `InfoSchemaBaseExtractor.filter(colName, val)`: true when a row
    /// whose `col_name` is `val` must NOT be shown.
    #[must_use]
    pub fn filter(&self, col_name: &str, val: &str) -> bool {
        if self.skip_request {
            return true;
        }
        if self
            .cols_regexp
            .get(col_name)
            .is_some_and(|regs| regs.iter().any(|re| !re.is_match(val)))
        {
            return true;
        }
        let to_lower = self
            .helper
            .extract_lower_string
            .get(col_name)
            .copied()
            .unwrap_or(false);
        match self.col_predicates.get(col_name) {
            Some(pred_vals) if !pred_vals.is_empty() => {
                if to_lower {
                    return !pred_vals.contains(&go_to_lower(val));
                }
                if let Some(func) = self.helper.pushed_down_funcs.get(col_name) {
                    return !pred_vals.contains(&func.apply(val));
                }
                !pred_vals.contains(val)
            }
            // No predicate on this column filters nothing.
            _ => false,
        }
    }

    /// Applies [`Self::filter`] to every extracted column of one
    /// materialized row. A NULL name matches no claimed value or pattern.
    /// `DDL_JOBS` keeps its predicates above the scan, so it filters nothing
    /// here.
    #[must_use]
    pub fn keeps_row(&self, columns: &[&str], row: &[Datum]) -> bool {
        if self.kind == InfoSchemaExtractorKind::Ddl {
            return true;
        }
        if self.skip_request {
            return false;
        }
        self.col_names
            .iter()
            .all(|col_name| match row_value(columns, row, col_name) {
                None => true,
                Some(None) => {
                    self.col_predicates
                        .get(*col_name)
                        .is_none_or(StringSet::is_empty)
                        && !self.cols_regexp.contains_key(*col_name)
                }
                Some(Some(value)) => !self.filter(col_name, &value),
            })
    }
}
