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

//! The comparable PROPERTY of an `EXPLAIN`, extracted from plan text.
//!
//! `tests/integrationtest` records 4,598 `EXPLAIN` statements, and their
//! recorded text cannot be compared verbatim: this tier's printer
//! deliberately diverges from Go's in ways that have nothing to do with the
//! question a plan case asks (no `cop[tikv]` task and no `TableReader`
//! wrapper, `Sort`+`Limit` instead of `TopN`, build-order operator ids,
//! always-present `Projection` -- each named in `tidb_executor::explain`'s
//! module docs). Matching that text would mean rewriting the printer to
//! describe executors this tier does not have.
//!
//! What a plan case actually guards is an ACCESS decision. Take
//! `access_path_selection.result`: the assertion is that `IDX_ab` beats
//! `IDX_a` because `IDX_a` would need a double scan -- a statement about
//! which index was chosen and over what range, not about a string. So the
//! property extracted here is the multiset of ACCESS rows: for each leaf that
//! reads data, its operator name, its access object (`table:t,
//! index:IDX_ab(a, b)`), its `range:`, and whether the estimate came from
//! real statistics or `stats:pseudo`. Everything above the leaves -- the
//! shape divergences above -- is deliberately dropped.
//!
//! A case whose plan text yields NO access row has no extractable property
//! and must be skipped by name by the caller rather than compared to nothing.
#![allow(dead_code)]

use std::collections::BTreeMap;

/// What to do with a statement whose recording is plan text.
#[derive(Debug, PartialEq, Eq)]
pub enum PlanStatement {
    /// Run this SQL instead and compare the access property. The recorded
    /// statement's `FORMAT` clause is dropped: it selects which NUMERIC
    /// columns ride along the operator tree (`plan_tree` prints no `estRows`,
    /// `verbose` adds `estCost`), and the access property lives in none of
    /// them, so this tier's own default `EXPLAIN` answers the same question.
    /// Go itself builds these formats from one tree and one column list --
    /// `prepareOperatorInfo` appends `estRows` unless the format is
    /// `plan_tree` (`pkg/planner/core/common_plans.go`).
    RunDefaultExplain(String),
    /// The recording is not a text operator tree, so there is no access row to
    /// read a property out of.
    NotComparable(&'static str),
}

/// Plan formats whose recording is the same operator tree in text.
const TEXT_TREE_FORMATS: &[&str] = &["row", "brief", "traditional", "plan_tree", "plan_cache"];

/// Classifies one statement as a plan statement, if it is one.
pub fn plan_statement(sql: &str) -> Option<PlanStatement> {
    let sql = sql.trim().trim_end_matches(';');
    let keyword = "explain";
    let boundary = sql
        .as_bytes()
        .get(keyword.len())
        .is_some_and(|c| !c.is_ascii_alphanumeric() && *c != b'_');
    if !boundary || !sql[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = sql[keyword.len()..].trim_start();
    let lower = rest.to_ascii_lowercase();

    if lower.starts_with("analyze") {
        return Some(PlanStatement::NotComparable(
            "EXPLAIN ANALYZE records execution counters, not a plan alone",
        ));
    }
    let Some(after_format) = lower.strip_prefix("format") else {
        return Some(PlanStatement::RunDefaultExplain(sql.to_owned()));
    };
    let Some(value) = after_format.trim_start().strip_prefix('=') else {
        return Some(PlanStatement::NotComparable(
            "`format` without `=` is not a format clause this reader models",
        ));
    };
    let value = value.trim_start();
    let quote = value.starts_with(['\'', '"']);
    let name_end = if quote {
        value[1..]
            .find(value.as_bytes()[0] as char)
            .map(|at| at + 1)
    } else {
        value.find(char::is_whitespace)
    };
    let Some(name_end) = name_end else {
        return Some(PlanStatement::NotComparable("unterminated format name"));
    };
    let name = &value[usize::from(quote)..name_end];
    if !TEXT_TREE_FORMATS.contains(&name) {
        return Some(PlanStatement::NotComparable(
            "EXPLAIN FORMAT is not a text operator tree",
        ));
    }
    // The format clause's own span in the ORIGINAL sql, whose case and spacing
    // the lowercased copy mirrors byte for byte.
    let clause_len = rest.len() - value.len() + name_end + usize::from(quote);
    Some(PlanStatement::RunDefaultExplain(format!(
        "explain {}",
        rest[clause_len..].trim_start()
    )))
}

/// Operator name substrings that mark a row as a data-reading leaf.
const ACCESS_OPERATORS: &[&str] = &["Scan", "Point_Get", "Point Get"];

/// One table's access decision: how it was read, over what range, from what
/// statistics.
pub type AccessProperty = BTreeMap<String, Vec<String>>;

/// Extracts the access property of one plan's rows, KEYED BY TABLE.
///
/// `rows` are the plan's lines, tab-separated. Only the FIRST and the last two
/// columns are read -- the operator id, the access object and the operator
/// info -- because every text tree format shares those and differs only in the
/// numeric columns wedged between them (`plan_tree` drops `estRows`,
/// `verbose` adds `estCost`, and so on).
///
/// Keying by table is what makes the two sides comparable at all. This tier's
/// recorder prints the scan it built for the query's own source and records NO
/// node for several operators it really does run (an Apply for a correlated
/// subquery above all -- see `tidb_executor::explain`), so a Go plan routinely
/// carries access rows for tables this side never printed. Comparing the flat
/// multisets would report every one of those as a divergence, which is a
/// statement about the printer, not about a decision. Per table, the question
/// is the one the case asks: for THIS table, which object was read, over which
/// range.
///
/// `TableRowIDScan` rows are dropped: they are the second half of Go's double
/// read, a consequence of the index choice rather than a choice, and this tier
/// has no such operator to print.
pub fn access_property(rows: &[String]) -> AccessProperty {
    let mut out: AccessProperty = BTreeMap::new();
    for row in rows {
        let columns: Vec<&str> = row.split('\t').collect();
        let Some(operator) = columns.first().map(|id| operator_name(id)) else {
            continue;
        };
        if !ACCESS_OPERATORS.iter().any(|kind| operator.contains(kind))
            || operator.starts_with("TableRowIDScan")
            || columns.len() < 3
        {
            continue;
        }
        let access_object = columns[columns.len() - 2].trim();
        let info = columns[columns.len() - 1];
        let mut property = format!("{operator} {access_object}");
        if let Some(range) = range_of(info) {
            property.push_str(&format!(" {range}"));
        }
        if info.contains("stats:pseudo") {
            property.push_str(" stats:pseudo");
        }
        let table = access_object
            .split(", ")
            .find_map(|part| part.strip_prefix("table:"))
            .unwrap_or("")
            .to_owned();
        out.entry(table).or_default().push(property);
    }
    for properties in out.values_mut() {
        properties.sort();
    }
    out
}

/// Strips a plan id of its tree-drawing prefix, its `_N` suffix and its
/// `(Build)`/`(Probe)` qualifier, leaving the operator name.
///
/// Ids are build-order here and plan-construction order in Go, so the number
/// is never comparable. The qualifier names which side of a join the operator
/// feeds -- shape, not an access decision, and the two sides mark it on
/// different rows because their trees differ.
fn operator_name(id: &str) -> &str {
    let name = id.trim_start_matches(|c: char| !c.is_ascii_alphanumeric());
    let name = name.split('(').next().unwrap_or(name);
    match name.rfind('_') {
        Some(at)
            if !name[at + 1..].is_empty() && name[at + 1..].bytes().all(|c| c.is_ascii_digit()) =>
        {
            &name[..at]
        }
        _ => name,
    }
}

/// The `range:` clause of an operator info column, up to the next clause.
fn range_of(info: &str) -> Option<&str> {
    let start = info.find("range:")?;
    let rest = &info[start..];
    // A range list contains commas of its own (`range:[1,1], [3,3]`), so the
    // clause ends at the next `, <key>:` clause rather than the next comma.
    let mut from = "range:".len();
    while let Some(offset) = rest[from..].find(", ") {
        let comma = from + offset;
        let tail = &rest[comma + 2..];
        let key_len = tail
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != ' ')
            .unwrap_or(tail.len());
        if tail[key_len..].starts_with(':') {
            return Some(rest[..comma].trim());
        }
        from = comma + 2;
    }
    Some(rest.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(columns: &[&str]) -> String {
        columns.join("\t")
    }

    #[test]
    fn keeps_the_chosen_index_and_its_range_and_drops_the_shape() {
        let go = vec![
            row(&["IndexLookUp_10", "10.00", "root", "", ""]),
            row(&[
                "├─IndexRangeScan_8(Build)",
                "10.00",
                "cop[tikv]",
                "table:t, index:IDX_ab(a, b)",
                "range:[1 2,1 2], keep order:false, stats:pseudo",
            ]),
            row(&[
                "└─TableRowIDScan_9(Probe)",
                "10.00",
                "cop[tikv]",
                "table:t",
                "keep order:false",
            ]),
        ];
        let property = access_property(&go);
        assert_eq!(
            property.get("t").map(Vec::as_slice),
            Some(
                [
                    "IndexRangeScan table:t, index:IDX_ab(a, b) range:[1 2,1 2] stats:pseudo"
                        .to_owned()
                ]
                .as_slice()
            ),
            "the row-ID scan and the Build/Probe qualifier are shape, not a decision"
        );
    }

    #[test]
    fn a_plan_of_the_same_table_in_the_plan_tree_format_reads_the_same() {
        // `plan_tree` prints no estRows column; the property must not move.
        let plan_tree = vec![row(&[
            "IndexRangeScan_8",
            "cop[tikv]",
            "table:t, index:IDX_ab(a, b)",
            "range:[1 2,1 2], keep order:false, stats:pseudo",
        ])];
        let row_format = vec![row(&[
            "IndexRangeScan_8",
            "10.00",
            "cop[tikv]",
            "table:t, index:IDX_ab(a, b)",
            "range:[1 2,1 2], keep order:false, stats:pseudo",
        ])];
        assert_eq!(access_property(&plan_tree), access_property(&row_format));
    }

    #[test]
    fn each_table_keeps_its_own_decision() {
        let go = vec![
            row(&["HashJoin_5", "1.00", "root", "", "inner join"]),
            row(&[
                "├─TableFullScan_6",
                "1.00",
                "cop[tikv]",
                "table:a",
                "stats:pseudo",
            ]),
            row(&[
                "└─IndexRangeScan_7",
                "1.00",
                "cop[tikv]",
                "table:b, index:i(x)",
                "range:[1,1], stats:pseudo",
            ]),
        ];
        let property = access_property(&go);
        assert_eq!(property.keys().collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn a_plan_with_no_access_row_has_no_property() {
        assert!(
            access_property(&[row(&["Projection_3", "1.00", "root", "", "1->Column#1"])])
                .is_empty()
        );
    }

    #[test]
    fn a_format_clause_is_dropped_and_a_non_tree_format_is_refused() {
        assert_eq!(
            plan_statement("explain format = 'plan_tree' select * from t;"),
            Some(PlanStatement::RunDefaultExplain(
                "explain select * from t".to_owned()
            ))
        );
        assert_eq!(
            plan_statement("EXPLAIN format='brief' SELECT 1;"),
            Some(PlanStatement::RunDefaultExplain(
                "explain SELECT 1".to_owned()
            ))
        );
        assert!(matches!(
            plan_statement("explain format = 'json' select 1;"),
            Some(PlanStatement::NotComparable(_))
        ));
        assert!(matches!(
            plan_statement("explain analyze select 1;"),
            Some(PlanStatement::NotComparable(_))
        ));
        assert_eq!(
            plan_statement("explain select 1;"),
            Some(PlanStatement::RunDefaultExplain(
                "explain select 1".to_owned()
            ))
        );
        assert_eq!(plan_statement("select 1;"), None);
    }

    #[test]
    fn range_clause_survives_its_own_commas() {
        assert_eq!(
            range_of("range:[1,1], [3,3], keep order:false"),
            Some("range:[1,1], [3,3]")
        );
        assert_eq!(range_of("keep order:false"), None);
    }
}
