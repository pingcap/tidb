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

//! Which recorded `EXPLAIN`s are not comparable as text.
//!
//! A plan recorded as a text operator tree is compared whole, like any row
//! result, by running the statement as written. The other formats (JSON,
//! DOT, hints, binary, `EXPLAIN ANALYZE`'s execution counters) are not
//! compared; a non-tree format over an explainable statement still plans on
//! the real server, so it runs for its side effects (`explain format='hint'
//! select /*+ inl_merge_join(t2) */ ...` leaves warning 1815 for the next
//! `show warnings`) and its output is discarded.
//!
//! `EXPLAIN`, `DESCRIBE` and `DESC` are one statement in TiDB's parser
//! (`parseExplainStmt`, `pkg/parser/set_explain_parser.go`); the token after
//! the keyword decides whether it is a plan or a `SHOW COLUMNS` (`DESC t`).

/// A recorded plan statement whose output is not compared.
#[derive(Debug, PartialEq, Eq)]
pub enum PlanStatement {
    /// The recording is not a text operator tree.
    NotComparable(&'static str),
    /// The recording is not a text operator tree, but the statement plans on
    /// the real server: run the default-format `sql` for its side effects and
    /// discard the output.
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

/// Classifies one recorded statement; `None` when it is compared as written.
pub fn plan_statement(sql: &str) -> Option<PlanStatement> {
    let sql = sql.trim().trim_end_matches(';');
    // Matched over bytes: a statement starting with a multi-byte character
    // is simply not an EXPLAIN.
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
    let after_format = lower.strip_prefix("format")?;
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
    if TEXT_TREE_FORMATS.contains(&name) {
        return None;
    }
    // The format clause's own span in the original sql, whose case and
    // spacing the lowercased copy mirrors byte for byte.
    let clause_len = rest.len() - value.len() + name_end + usize::from(quote);
    let explained = rest[clause_len..].trim_start();
    if is_explainable(explained) {
        return Some(PlanStatement::RunAndDiscard {
            sql: format!("explain {explained}"),
            reason: "EXPLAIN FORMAT is not a text operator tree",
        });
    }
    Some(PlanStatement::NotComparable(
        "EXPLAIN FORMAT is not a text operator tree",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_tree_runs_as_written_and_other_formats_are_not_compared() {
        for sql in [
            "explain select 1;",
            "EXPLAIN format='brief' SELECT 1;",
            "explain format = 'plan_tree' select * from t;",
            "desc format = 'brief' select 1;",
            "select 1;",
        ] {
            assert_eq!(plan_statement(sql), None, "{sql}");
        }
        assert_eq!(
            plan_statement("explain format = 'json' select 1;"),
            Some(PlanStatement::RunAndDiscard {
                sql: "explain select 1".to_owned(),
                reason: "EXPLAIN FORMAT is not a text operator tree",
            })
        );
        assert!(matches!(
            plan_statement("explain format = 'json' t;"),
            Some(PlanStatement::NotComparable(_))
        ));
        assert!(matches!(
            plan_statement("explain analyze select 1;"),
            Some(PlanStatement::NotComparable(_))
        ));
    }

    /// The keyword must stand alone: a name that merely starts with one of
    /// the spellings is a name.
    #[test]
    fn a_name_starting_with_an_explain_spelling_is_not_an_explain() {
        assert_eq!(plan_statement("description_of_t"), None);
        assert_eq!(plan_statement("descending select 1"), None);
        for keyword in ["explain", "desc", "describe", "DESC", "Describe"] {
            assert!(matches!(
                plan_statement(&format!("{keyword} format='dot' select * from t;")),
                Some(PlanStatement::RunAndDiscard { .. })
            ));
        }
    }
}
