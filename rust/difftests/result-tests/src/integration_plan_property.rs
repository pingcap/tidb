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
//! question a plan case asks (`Sort`+`Limit` instead of a cop-side `TopN`,
//! build-order operator ids, always-present `Projection` -- each named in
//! `tidb_executor::explain`'s module docs; the reader boundary and the
//! `cop[tikv]` task that used to head this list are recorded now). Matching
//! that text would mean rewriting the printer to describe executors this
//! tier does not have.
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
//!
//! # Which statements ARE plan statements
//!
//! `EXPLAIN`, `DESCRIBE` and `DESC` are one statement in TiDB's parser: all
//! three enter `parseExplainStmt` (`pkg/parser/set_explain_parser.go`) and
//! build the same `ExplainStmt`. What splits a plan from a column list is not
//! the keyword but the token AFTER it -- Go's `ExplainableStmt` switch. A name
//! there makes the statement a `SHOW COLUMNS` (`DESC t`, and equally
//! `EXPLAIN t`), whose recording is a row result to compare cell by cell.
//!
//! Reading only the leading word got both halves wrong, and neither error
//! looked like one. `DESC <select>` was compared as ROWS against recorded plan
//! text and diverged on every line -- 23 manufactured divergences across the
//! corpus (`explain_generate_column_substitute` 64 -> 46, `select` 20 -> 16,
//! `bindinfo/bind` 19 -> 18, `executor/partition/issues` 6 -> 5), every one of
//! them now a matched `PlanProperty` over the same compared total. `EXPLAIN
//! <table>` went the other way: its `SHOW COLUMNS` rows carry no access row,
//! so it was skipped as `PlanWithoutProperty` and never compared at all. It is
//! compared now, and it FAILS on a real gap (`int(11)` where TiDB 8.x prints
//! `int`) -- the same gap its `desc t` and `show columns from t` neighbours
//! were already reporting.
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
    /// The recording is not comparable, but the statement still PLANS on the
    /// real server and its side effects are observable afterwards: TiDB's
    /// `explain format='hint' select /*+ inl_merge_join(t2) */ ...` leaves
    /// warning 1815 for the `show warnings` the corpus runs next. Run the
    /// carried default-format spelling for those effects, discard its output,
    /// and count the skip -- skipping COLD leaves the follow-up reading a
    /// warning buffer the recording's server filled and ours never did.
    RunAndDiscard {
        /// The default-format `EXPLAIN` over the same target.
        sql: String,
        /// Why the output itself is not compared.
        reason: &'static str,
    },
}

/// Plan formats whose recording is the same operator tree in text.
const TEXT_TREE_FORMATS: &[&str] = &["row", "brief", "traditional", "plan_tree", "plan_cache"];

/// The three spellings that reach Go's `parseExplainStmt`
/// (`pkg/parser/set_explain_parser.go`). They are one statement in the parser;
/// the leading word carries no information beyond entering it.
const EXPLAIN_SPELLINGS: &[&str] = &["explain", "describe", "desc"];

/// What may follow the keyword and still be a PLAN. This is Go's
/// `ExplainableStmt` switch verbatim (`parseExplainStmt`): anything else that
/// looks like a name is a table, and `EXPLAIN`/`DESC <table>` is then a
/// `SHOW COLUMNS` -- a row result, not an operator tree. The keyword does not
/// decide this; what follows it does.
const EXPLAINABLE_LEADS: &[&str] = &[
    "select", "insert", "replace", "update", "delete", "alter", "with", "table", "values", "import",
];

/// Whether `rest` begins an explainable statement rather than a table name.
fn is_explainable(rest: &str) -> bool {
    if rest.starts_with('(') {
        return true;
    }
    let word_end = rest
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    EXPLAINABLE_LEADS
        .iter()
        .any(|lead| rest[..word_end].eq_ignore_ascii_case(lead))
}

/// Classifies one statement as a plan statement, if it is one.
pub fn plan_statement(sql: &str) -> Option<PlanStatement> {
    let sql = sql.trim().trim_end_matches(';');
    // Matched over BYTES rather than a `&str` slice: the spellings are all
    // ASCII, and a statement whose first bytes are not (`create database
    // 数据库...`, which `ddl/db` has) would have `sql[..keyword.len()]` land
    // inside a multi-byte character and panic. Bytes have no such boundary,
    // so the non-ASCII statement is simply not an EXPLAIN.
    let keyword = EXPLAIN_SPELLINGS.iter().find(|keyword| {
        let bytes = sql.as_bytes();
        bytes.len() > keyword.len()
            && bytes[..keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
            && !bytes[keyword.len()].is_ascii_alphanumeric()
            && bytes[keyword.len()] != b'_'
    })?;
    let rest = sql[keyword.len()..].trim_start();
    let lower = rest.to_ascii_lowercase();

    if lower.starts_with("analyze") {
        return Some(PlanStatement::NotComparable(
            "EXPLAIN ANALYZE records execution counters, not a plan alone",
        ));
    }
    let Some(after_format) = lower.strip_prefix("format") else {
        // `DESC t` / `EXPLAIN t` is a column list, and its recording is a row
        // result to compare cell by cell -- not a plan.
        if !is_explainable(rest) {
            return None;
        }
        return Some(PlanStatement::RunDefaultExplain(format!("explain {rest}")));
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
    // The format clause's own span in the ORIGINAL sql, whose case and spacing
    // the lowercased copy mirrors byte for byte.
    let clause_len = rest.len() - value.len() + name_end + usize::from(quote);
    let explained = rest[clause_len..].trim_start();
    if !TEXT_TREE_FORMATS.contains(&name) {
        if is_explainable(explained) {
            return Some(PlanStatement::RunAndDiscard {
                sql: format!("explain {explained}"),
                reason: "EXPLAIN FORMAT is not a text operator tree",
            });
        }
        return Some(PlanStatement::NotComparable(
            "EXPLAIN FORMAT is not a text operator tree",
        ));
    }
    if !is_explainable(explained) {
        return None;
    }
    Some(PlanStatement::RunDefaultExplain(format!(
        "explain {explained}"
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
        // A non-tree format over an EXPLAINABLE target is run for its side
        // effects and discarded (see `RunAndDiscard`'s own doc); the output
        // is still never compared.
        assert!(matches!(
            plan_statement("explain format = 'json' select 1;"),
            Some(PlanStatement::RunAndDiscard { .. })
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

    /// `DESC`/`DESCRIBE`/`EXPLAIN` are one statement in Go's parser
    /// (`parseExplainStmt`), and the split between a plan and a column list is
    /// made by what FOLLOWS the keyword, not by the keyword.
    #[test]
    fn desc_of_a_query_is_a_plan_and_desc_of_a_table_is_not() {
        for keyword in ["explain", "desc", "describe", "DESC", "Describe"] {
            assert_eq!(
                plan_statement(&format!("{keyword} select * from t;")),
                Some(PlanStatement::RunDefaultExplain(
                    "explain select * from t".to_owned()
                )),
                "{keyword} of a query is a plan"
            );
            assert_eq!(
                plan_statement(&format!("{keyword} t;")),
                None,
                "{keyword} of a table is a column list"
            );
        }
        assert_eq!(
            plan_statement("desc format = 'brief' select 1;"),
            Some(PlanStatement::RunDefaultExplain(
                "explain select 1".to_owned()
            ))
        );
        assert_eq!(
            plan_statement("desc (select 1) t;"),
            Some(PlanStatement::RunDefaultExplain(
                "explain (select 1) t".to_owned()
            ))
        );
        // The keyword must stand alone: `describe` is not `desc`'s argument,
        // and a name that merely starts with one of the spellings is a name.
        assert_eq!(plan_statement("description_of_t"), None);
        assert_eq!(plan_statement("descending select 1"), None);
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
