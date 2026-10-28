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

//! Go `pkg/planner/core/show_predicate_extractor.go`: `ShowBaseExtractor`,
//! which takes a SHOW's literal LIKE pattern (or `DESCRIBE t col`'s column)
//! off the plan and hands it to the executor as a name or wildcard filter.

use tidb_datatype::CollationCollator;
use tidb_ast::{Expr, ShowStmt, ShowStmtType};
use tidb_hack::go_to_lower;
use tidb_util::stringutil::{compile_pattern, is_exact_match};

/// Go `ShowBaseExtractor`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShowBaseExtractor {
    /// Go `ShowStmt.Tp`, which names the key `ExplainInfo` prints.
    pub tp: ShowStmtType,
    /// Go `field`: the lowercased exact name a wildcard-free pattern names.
    pub field: String,
    /// Go `fieldPattern`: the lowercased pattern, when it has wildcards.
    pub field_pattern: String,
}

/// Go `driver.ValueExpr.GetString()` for a literal pattern: the bytes of a
/// string, hex or bit literal; empty for a numeric, NULL or boolean one,
/// whose datum carries no bytes. `None` for an expression that is not a
/// literal.
fn value_expr_string(expr: &Expr) -> Option<String> {
    Some(match expr {
        Expr::String(value) | Expr::RawString(value) => value.clone(),
        Expr::CharsetString { value, .. } => value.clone(),
        Expr::Hex(digits) => {
            let bytes: Vec<u8> = (0..digits.len())
                .step_by(2)
                .filter_map(|at| u8::from_str_radix(digits.get(at..at + 2)?, 16).ok())
                .collect();
            String::from_utf8_lossy(&bytes).into_owned()
        }
        Expr::Bit(value) => String::from_utf8_lossy(value.as_bytes()).into_owned(),
        Expr::Int(_) | Expr::Decimal(_) | Expr::Float(_) | Expr::Null | Expr::Bool(_) => {
            String::new()
        }
        _ => return None,
    })
}

impl ShowBaseExtractor {
    /// Go `newShowBaseExtractor(show).Extract()`: the extractor when it
    /// claimed the pattern (or the DESCRIBE column), `None` otherwise.
    #[must_use]
    pub fn extract(show: &ShowStmt) -> Option<Self> {
        let mut extractor = Self {
            tp: show.tp,
            ..Self::default()
        };
        if let Some(pattern) = &show.pattern {
            // Go: a column name (`SHOW COLUMNS FROM t LIKE abc`) and any other
            // expression stay a selection.
            let pattern = value_expr_string(pattern)?;
            let (value, types) = compile_pattern(pattern.as_bytes(), b'\\');
            if is_exact_match(&types) {
                extractor.field = go_to_lower(value.into_iter().collect::<String>());
            } else {
                extractor.field_pattern = go_to_lower(pattern);
            }
            return Some(extractor);
        }
        match show.column.as_ref().and_then(|path| path.last()) {
            Some(column) if !column.is_empty() => {
                extractor.field = go_to_lower(column.clone());
                Some(extractor)
            }
            _ => None,
        }
    }

    /// Go `Field()`.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Go `FieldPatternLike()`: the utf8mb4 default collation's wildcard
    /// over the lowercased pattern, with `\\` as the escape.
    #[must_use]
    pub fn field_pattern_like(&self) -> Option<tidb_datatype::WildcardPattern> {
        (!self.field_pattern.is_empty())
            .then(|| tidb_datatype::Collation::Utf8Mb4Bin.pattern(&self.field_pattern, b'\\'))
    }

    /// Go `ExplainInfo`.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let key = match self.tp {
            ShowStmtType::Variables | ShowStmtType::Columns => "field",
            ShowStmtType::Tables | ShowStmtType::TableStatus => "table",
            ShowStmtType::Databases => "database",
            ShowStmtType::Collation => "collation",
            ShowStmtType::StatsHealthy => "db_name",
            _ => "",
        };
        let mut parts = Vec::new();
        if !self.field.is_empty() {
            parts.push(format!("{key}:[{}]", self.field));
        }
        if !self.field_pattern.is_empty() {
            parts.push(format!("{key}_pattern:[{}]", self.field_pattern));
        }
        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn show_like(tp: ShowStmtType, pattern: Expr) -> ShowStmt {
        let mut show = ShowStmt::new(tp);
        show.pattern = Some(pattern);
        show
    }

    /// Go `TestShowLikeExtractor` shapes: an exact literal is a lowercased
    /// field, a wildcard literal a lowercased pattern, a column no extractor.
    #[test]
    fn literal_patterns_are_claimed_and_columns_are_not() {
        let exact = ShowBaseExtractor::extract(&show_like(
            ShowStmtType::Tables,
            Expr::String("T\\_1".to_owned()),
        ))
        .unwrap();
        assert_eq!((exact.field.as_str(), exact.field_pattern.as_str()), ("t_1", ""));
        assert_eq!(exact.explain_info(), "table:[t_1]");

        let wildcard = ShowBaseExtractor::extract(&show_like(
            ShowStmtType::Databases,
            Expr::String("Test%".to_owned()),
        ))
        .unwrap();
        assert_eq!(wildcard.field_pattern, "test%");
        assert_eq!(wildcard.explain_info(), "database_pattern:[test%]");

        assert!(ShowBaseExtractor::extract(&show_like(
            ShowStmtType::Columns,
            Expr::Column(vec!["abc".to_owned()]),
        ))
        .is_none());
    }

    /// Go: `DESCRIBE t col` reaches the extractor through `Column`.
    #[test]
    fn describe_column_is_an_exact_field() {
        let mut show = ShowStmt::new(ShowStmtType::Columns);
        show.column = Some(vec!["A".to_owned()]);
        assert_eq!(ShowBaseExtractor::extract(&show).unwrap().field, "a");
        assert!(ShowBaseExtractor::extract(&ShowStmt::new(ShowStmtType::Columns)).is_none());
    }
}
