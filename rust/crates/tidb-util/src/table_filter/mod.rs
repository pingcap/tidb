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

//! Complete transcreation of Go `pkg/util/table-filter` (Go package `filter`).
//!
//! Checks whether a schema/table (or column) should be included for processing,
//! using accept/deny rules with `*`, `?`, `[...]` wildcards, `/regexp/`,
//! quoted identifiers, `@file` imports, and legacy MySQL-replication rules.
//!
//! Go's `Filter`/`matcher` interfaces become the [`Filter`] trait (as
//! `Box<dyn Filter>`) and the internal `Matcher` enum. Wildcards compile to
//! regexes via the `regex` crate, which is RE2-lineage like Go's `regexp`, so
//! match results are identical; only the regex/io compiler *error text* differs
//! from Go's standard library (documented at the affected call sites and tests).

mod column_filter;
mod compat;
mod matchers;
mod parser;

pub use column_filter::{parse_column_filter, ColumnFilter};
pub use compat::{
    new_schemas_filter, new_tables_filter, parse_mysql_replication_rules, MySQLReplicationRules,
    Table,
};

use matchers::TableRule;
use parser::{MatcherParser, RuleParser, TableRulesParser};
use std::fmt;

/// Error returned by filter parsing.
#[derive(Debug, Clone)]
pub struct FilterError(String);

impl FilterError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        FilterError(msg.into())
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FilterError {}

/// Checks if a table/schema should be included for processing.
pub trait Filter {
    /// Checks if a table can be processed after applying the filter.
    fn match_table(&self, schema: &str, table: &str) -> bool;
    /// Checks if a schema can be processed after applying the filter.
    fn match_schema(&self, schema: &str) -> bool;
    /// Returns a case-insensitive version of this filter (compares in
    /// lowercase).
    fn to_lower(&self) -> Box<dyn Filter>;
}

/// The concrete rule-list implementation of [`Filter`].
pub(crate) struct TableFilter {
    rules: Vec<TableRule>,
}

impl TableFilter {
    pub(crate) fn new(rules: Vec<TableRule>) -> Self {
        TableFilter { rules }
    }
}

impl Filter for TableFilter {
    fn match_table(&self, schema: &str, table: &str) -> bool {
        for rule in &self.rules {
            if rule.schema.match_string(schema) && rule.table.match_string(table) {
                return rule.positive;
            }
        }
        false
    }

    fn match_schema(&self, schema: &str) -> bool {
        for rule in &self.rules {
            if rule.schema.match_string(schema) && (rule.positive || rule.table.match_all_strings())
            {
                return rule.positive;
            }
        }
        false
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        let rules = self
            .rules
            .iter()
            .map(|r| TableRule {
                schema: r.schema.to_lower(),
                table: r.table.to_lower(),
                positive: r.positive,
            })
            .collect();
        Box::new(TableFilter { rules })
    }
}

/// Wraps a filter, lowercasing inputs before matching.
struct LoweredFilter {
    wrapped: Box<dyn Filter>,
}

impl Filter for LoweredFilter {
    fn match_table(&self, schema: &str, table: &str) -> bool {
        self.wrapped
            .match_table(&schema.to_lowercase(), &table.to_lowercase())
    }

    fn match_schema(&self, schema: &str) -> bool {
        self.wrapped.match_schema(&schema.to_lowercase())
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        // Go returns the same `loweredFilter` (lowering an already-lowered
        // filter is idempotent). Re-lowering the already-lowered inner filter is
        // observably equivalent and avoids cloning a trait object.
        Box::new(LoweredFilter {
            wrapped: self.wrapped.to_lower(),
        })
    }
}

/// A filter which matches everything.
struct AllFilter;

impl Filter for AllFilter {
    fn match_table(&self, _schema: &str, _table: &str) -> bool {
        true
    }

    fn match_schema(&self, _schema: &str) -> bool {
        true
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        Box::new(AllFilter)
    }
}

/// Parses a filter from a list of serialized rules. The parsed filter is
/// case-sensitive by default.
pub fn parse(args: &[&str]) -> Result<Box<dyn Filter>, FilterError> {
    let mut p = TableRulesParser {
        rules: Vec::with_capacity(args.len()),
        mp: MatcherParser::new(),
    };

    for arg in args {
        p.parse(arg, true)?;
    }

    p.rules.reverse();

    Ok(Box::new(TableFilter { rules: p.rules }))
}

/// Returns a case-insensitive version of the input filter.
pub fn case_insensitive(f: Box<dyn Filter>) -> Box<dyn Filter> {
    Box::new(LoweredFilter {
        wrapped: f.to_lower(),
    })
}

/// Creates a filter which matches everything.
pub fn all() -> Box<dyn Filter> {
    Box::new(AllFilter)
}

#[cfg(test)]
pub(crate) mod testutil {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A unique temporary directory that removes itself on drop.
    pub(crate) struct TempDir {
        dir: PathBuf,
    }

    impl TempDir {
        pub(crate) fn new() -> Self {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("tidb_tablefilter_{}_{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            TempDir { dir }
        }

        /// Writes `content` to `name` in this dir and returns the full path.
        pub(crate) fn write(&self, name: &str, content: &str) -> String {
            let p = self.dir.join(name);
            std::fs::write(&p, content).unwrap();
            p.to_str().unwrap().to_string()
        }

        /// Returns the full path of `name` in this dir (without creating it).
        pub(crate) fn path(&self, name: &str) -> String {
            self.dir.join(name).to_str().unwrap().to_string()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::TempDir;
    use super::{all, case_insensitive, parse};

    struct TableCase {
        args: &'static [&'static str],
        tables: &'static [(&'static str, &'static str)],
        accepted_cs: &'static [bool],
        accepted_ci: &'static [bool],
    }

    // Go `TestMatchTables`.
    #[test]
    fn match_tables() {
        let cases = [
            TableCase {
                args: &[],
                tables: &[("foo", "bar")],
                accepted_cs: &[false],
                accepted_ci: &[false],
            },
            TableCase {
                args: &["*.*"],
                tables: &[("foo", "bar")],
                accepted_cs: &[true],
                accepted_ci: &[true],
            },
            TableCase {
                args: &["foo.*"],
                tables: &[("foo", "bar"), ("foo1", "bar"), ("foo2", "bar")],
                accepted_cs: &[true, false, false],
                accepted_ci: &[true, false, false],
            },
            TableCase {
                args: &["*.*", "!foo1.*"],
                tables: &[("foo", "bar"), ("foo1", "bar"), ("foo2", "bar")],
                accepted_cs: &[true, false, true],
                accepted_ci: &[true, false, true],
            },
            TableCase {
                args: &["foo.bar1"],
                tables: &[("foo", "bar"), ("foo", "bar1"), ("fff", "bar1")],
                accepted_cs: &[false, true, false],
                accepted_ci: &[false, true, false],
            },
            TableCase {
                args: &["*.*", "!foo.bar"],
                tables: &[("foo", "bar"), ("foo", "bar1"), ("fff", "bar1")],
                accepted_cs: &[false, true, true],
                accepted_ci: &[false, true, true],
            },
            TableCase {
                args: &["/^foo/.*", r"!/^foo/./^sbtest-\d/"],
                tables: &[("foo", "sbtest"), ("foo1", "sbtest-1"), ("fff", "bar")],
                accepted_cs: &[true, false, false],
                accepted_ci: &[true, false, false],
            },
            TableCase {
                args: &["*.*", "!foo[bar].*", "!bar?.*", r"!special\\.*"],
                tables: &[
                    ("foor", "a"),
                    ("foo[bar]", "b"),
                    ("ba", "c"),
                    ("bar?", "d"),
                    (r"special\", "e"),
                    (r"special\\", "f"),
                    ("bazzz", "g"),
                    (r"special\$", "h"),
                    ("afooa", "i"),
                ],
                accepted_cs: &[false, true, true, false, false, true, true, true, true],
                accepted_ci: &[false, true, true, false, false, true, true, true, true],
            },
            TableCase {
                args: &["*.*", "!/^FOO/.*", "!*./FoO$/"],
                tables: &[
                    ("FOO1", "a"),
                    ("foo2", "b"),
                    ("BoO3", "cFoO"),
                    ("Foo4", "dfoo"),
                    ("5", "5"),
                ],
                accepted_cs: &[false, true, false, true, true],
                accepted_ci: &[false, false, false, false, true],
            },
            TableCase {
                args: &["*.*", "!a?b?./f[0-9]/"],
                tables: &[("abbd", "f1"), ("aaaa", "f2"), ("5", "5"), ("abbc", "fa")],
                accepted_cs: &[false, true, true, true],
                accepted_ci: &[false, true, true, true],
            },
            TableCase {
                args: &["*.*", "!/t[0-8]/.a??"],
                tables: &[("t1", "a01"), ("t9", "a02"), ("5", "5"), ("t8", "a001")],
                accepted_cs: &[false, true, true, true],
                accepted_ci: &[false, true, true, true],
            },
            TableCase {
                args: &["*.*", "!a*.A*"],
                tables: &[("aB", "Ab"), ("AaB", "aab"), ("acB", "Afb")],
                accepted_cs: &[false, true, false],
                accepted_ci: &[false, false, false],
            },
            TableCase {
                args: &["BAR.*"],
                tables: &[("bar", "a"), ("BAR", "a")],
                accepted_cs: &[false, true],
                accepted_ci: &[true, true],
            },
            TableCase {
                args: &["# comment", "x.y", "   \t"],
                tables: &[("x", "y"), ("y", "y")],
                accepted_cs: &[true, false],
                accepted_ci: &[true, false],
            },
            TableCase {
                args: &["p_123$.45", "中文.表名"],
                tables: &[
                    ("p_123", "45"),
                    ("p_123$", "45"),
                    ("英文", "表名"),
                    ("中文", "表名"),
                ],
                accepted_cs: &[false, true, false, true],
                accepted_ci: &[false, true, false, true],
            },
            TableCase {
                args: &[r"\\\..*"],
                tables: &[(r"\.", "a"), (r"\\\.", "b"), (r"\a", "c")],
                accepted_cs: &[true, false, false],
                accepted_ci: &[true, false, false],
            },
            TableCase {
                args: &["[!a-z].[^a-z]"],
                tables: &[
                    ("!", "z"),
                    ("!", "^"),
                    ("!", "9"),
                    ("a", "z"),
                    ("a", "^"),
                    ("a", "9"),
                    ("1", "z"),
                    ("1", "^"),
                    ("1", "9"),
                ],
                accepted_cs: &[true, true, false, false, false, false, true, true, false],
                accepted_ci: &[true, true, false, false, false, false, true, true, false],
            },
            TableCase {
                args: &["\"some \"\"quoted\"\"\".`identifiers?`"],
                tables: &[
                    (r#"some "quoted""#, "identifiers?"),
                    (r#"some "quoted""#, "identifiers!"),
                    (r#"some ""quoted"""#, "identifiers?"),
                    (r#"SOME "QUOTED""#, "IDENTIFIERS?"),
                    ("some\t\"quoted\"", "identifiers?"),
                ],
                accepted_cs: &[true, false, false, false, false],
                accepted_ci: &[true, false, false, true, false],
            },
            TableCase {
                args: &["db*.*", "!*.cfg*", "*.cfgsample"],
                tables: &[
                    ("irrelevant", "table"),
                    ("db1", "tbl1"),
                    ("db1", "cfg1"),
                    ("db1", "cfgsample"),
                    ("else", "cfgsample"),
                ],
                accepted_cs: &[false, true, false, true, true],
                accepted_ci: &[false, true, false, true, true],
            },
            TableCase {
                args: &["*.*", "!S.D[!a-d]"],
                tables: &[("S", "D1"), ("S", "Da"), ("S", "Db"), ("S", "Daa")],
                accepted_cs: &[false, true, true, true],
                accepted_ci: &[false, true, true, true],
            },
            TableCase {
                args: &["*.*", "!S.D[a-d]"],
                tables: &[("S", "D1"), ("S", "Da"), ("S", "Db"), ("S", "Daa")],
                accepted_cs: &[true, false, false, true],
                accepted_ci: &[true, false, false, true],
            },
        ];

        for tc in cases {
            let fcs = parse(tc.args).unwrap();
            let fci = case_insensitive(parse(tc.args).unwrap());
            for (i, &(schema, name)) in tc.tables.iter().enumerate() {
                assert_eq!(
                    fcs.match_table(schema, name),
                    tc.accepted_cs[i],
                    "cs args={:?} tbl={schema}.{name}",
                    tc.args
                );
                assert_eq!(
                    fci.match_table(schema, name),
                    tc.accepted_ci[i],
                    "ci args={:?} tbl={schema}.{name}",
                    tc.args
                );
            }
        }
    }

    struct SchemaCase {
        args: &'static [&'static str],
        schemas: &'static [&'static str],
        accepted_cs: &'static [bool],
        accepted_ci: &'static [bool],
    }

    // Go `TestMatchSchemas`.
    #[test]
    fn match_schemas() {
        let cases = [
            SchemaCase {
                args: &[],
                schemas: &["foo"],
                accepted_cs: &[false],
                accepted_ci: &[false],
            },
            SchemaCase {
                args: &["*.*"],
                schemas: &["foo"],
                accepted_cs: &[true],
                accepted_ci: &[true],
            },
            SchemaCase {
                args: &["foo.*"],
                schemas: &["foo", "foo1"],
                accepted_cs: &[true, false],
                accepted_ci: &[true, false],
            },
            SchemaCase {
                args: &["*.*", "!foo1.*"],
                schemas: &["foo", "foo1"],
                accepted_cs: &[true, false],
                accepted_ci: &[true, false],
            },
            SchemaCase {
                args: &["foo.bar1"],
                schemas: &["foo", "foo1"],
                accepted_cs: &[true, false],
                accepted_ci: &[true, false],
            },
            SchemaCase {
                args: &["*.*", "!foo.bar"],
                schemas: &["foo", "foo1"],
                accepted_cs: &[true, true],
                accepted_ci: &[true, true],
            },
            SchemaCase {
                args: &["/^foo/.*", r"!/^foo/./^sbtest-\d/"],
                schemas: &["foo", "foo2"],
                accepted_cs: &[true, true],
                accepted_ci: &[true, true],
            },
            SchemaCase {
                args: &["*.*", "!FOO*.*", "!*.*FoO"],
                schemas: &["foo", "FOO", "foobar", "FOOBAR", "bar", "BAR"],
                accepted_cs: &[true, false, true, false, true, true],
                accepted_ci: &[false, false, false, false, true, true],
            },
        ];

        for tc in cases {
            let fcs = parse(tc.args).unwrap();
            let fci = case_insensitive(parse(tc.args).unwrap());
            for (i, &schema) in tc.schemas.iter().enumerate() {
                assert_eq!(
                    fcs.match_schema(schema),
                    tc.accepted_cs[i],
                    "cs args={:?} schema={schema}",
                    tc.args
                );
                assert_eq!(
                    fci.match_schema(schema),
                    tc.accepted_ci[i],
                    "ci args={:?} schema={schema}",
                    tc.args
                );
            }
        }
    }

    // Go `TestParseFailures2`. The two `/regexp/` and `[!]` cases hit the regex
    // compiler; Go asserts `regexp`'s error text, Rust's `regex` rejects the
    // same patterns with different wording, so those assert the shared
    // `error parsing regexp:` prefix. All other messages are reproduced exactly.
    #[test]
    fn parse_failures() {
        let cases: &[(&str, &str)] = &[
            (
                "/^t[0-9]+((?!_copy).)*$/.*",
                "invalid pattern: error parsing regexp:",
            ),
            (
                "/^t[0-9]+sp(?=copy).*/.*",
                "invalid pattern: error parsing regexp:",
            ),
            (
                "a.b.c",
                "syntax error: stray characters after table pattern",
            ),
            ("a%b.c", "unexpected special character '%'"),
            (
                r"a\tb.c",
                r"cannot escape a letter or number (\t), it is reserved for future extension",
            ),
            ("[].*", "syntax error: failed to parse character class"),
            ("[!].*", "invalid pattern: error parsing regexp:"),
            ("[.*", "syntax error: failed to parse character class"),
            (r"[\d\D].*", "syntax error: failed to parse character class"),
            ("db", "wrong table pattern"),
            ("db.", "syntax error: missing pattern"),
            (
                "`db`*.*",
                "syntax error: missing '.' between schema and table patterns",
            ),
            ("/db.*", "syntax error: incomplete regexp"),
            ("`db.*", "syntax error: incomplete quoted identifier"),
            (r#""db.*"#, "syntax error: incomplete quoted identifier"),
            (r"db\", r"syntax error: cannot place \ at end of line"),
            ("db.tbl#not comment", "unexpected special character '#'"),
        ];
        for (arg, want) in cases {
            let err = parse(&[arg]).err().unwrap();
            assert!(err.to_string().contains(want), "arg={arg:?} err={err}");
        }
    }

    // Go `TestImport2`.
    #[test]
    fn import() {
        let dir = TempDir::new();
        let path1 = dir.write("1.txt", "\n\t\tdb?.tbl?\n\t\tdb02.tbl02\n\t");
        let path2 = dir.write("2.txt", "\n\t\tdb03.tbl03\n\t\t!db4.tbl4\n\t");

        let f = parse(&[&format!("@{path1}"), &format!("@{path2}"), "db04.tbl04"]).unwrap();

        assert!(f.match_table("db1", "tbl1"));
        assert!(f.match_table("db2", "tbl2"));
        assert!(f.match_table("db3", "tbl3"));
        assert!(!f.match_table("db4", "tbl4"));
        assert!(!f.match_table("db01", "tbl01"));
        assert!(f.match_table("db02", "tbl02"));
        assert!(f.match_table("db03", "tbl03"));
        assert!(f.match_table("db04", "tbl04"));
    }

    // Go `TestRecursiveImport2`.
    #[test]
    fn recursive_import() {
        let dir = TempDir::new();
        let path3 = dir.write("3.txt", "db1.tbl1");
        let path4 = dir.write("4.txt", &format!("# comment\n\n@{path3}"));

        let err = parse(&[&format!("@{path4}")]).err().unwrap();
        assert!(
            err.to_string()
                .contains("4.txt:3: importing filter files recursively is not allowed"),
            "err={err}"
        );

        let missing = dir.path("5.txt");
        let err = parse(&[&format!("@{missing}")]).err().unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("cannot open filter file: open") && msg.contains("5.txt"),
            "err={msg}"
        );
    }

    // Go `TestAll`.
    #[test]
    fn test_all() {
        let f = all();
        assert!(f.match_table("db1", "tbl1"));
        assert!(f.match_schema("db1"));

        let f = case_insensitive(f);
        assert!(f.match_table("db1", "tbl1"));
        assert!(f.match_schema("db1"));
    }
}
