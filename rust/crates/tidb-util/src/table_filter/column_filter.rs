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

//! Transcreation of Go `pkg/util/table-filter/column_filter.go`.

use super::matchers::ColumnRule;
use super::parser::{ColumnRulesParser, MatcherParser, RuleParser};
use super::FilterError;

/// Checks if a column should be included for processing.
pub trait ColumnFilter {
    /// Checks if a column can be processed after applying the column filter.
    fn match_column(&self, column: &str) -> bool;
}

struct ColumnFilterImpl {
    rules: Vec<ColumnRule>,
}

impl ColumnFilter for ColumnFilterImpl {
    fn match_column(&self, column: &str) -> bool {
        // Column names and aliases are not case-sensitive on any platform, so
        // always match in lowercase.
        // See https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
        let lowercase_column = column.to_lowercase();
        for rule in &self.rules {
            if rule.column.match_string(&lowercase_column) {
                return rule.positive;
            }
        }
        false
    }
}

/// Parses a column filter from a list of serialized column-filter rules. The
/// parsed column filter is case-insensitive.
pub fn parse_column_filter(args: &[&str]) -> Result<Box<dyn ColumnFilter>, FilterError> {
    let mut p = ColumnRulesParser {
        rules: Vec::with_capacity(args.len()),
        mp: MatcherParser::new(),
    };

    for arg in args {
        p.parse(arg, true)?;
    }

    p.rules.reverse();

    Ok(Box::new(ColumnFilterImpl { rules: p.rules }))
}

#[cfg(test)]
mod tests {
    use super::parse_column_filter;
    use crate::table_filter::testutil::TempDir;

    // Go `TestMatchColumns`.
    #[test]
    fn match_columns() {
        struct Case {
            args: &'static [&'static str],
            columns: &'static [&'static str],
            accepted: &'static [bool],
        }
        let cases = [
            Case {
                args: &[],
                columns: &["foo"],
                accepted: &[false],
            },
            Case {
                args: &["*"],
                columns: &["foo"],
                accepted: &[true],
            },
            Case {
                args: &["foo*"],
                columns: &["foo", "foo1", "foo2"],
                accepted: &[true, true, true],
            },
            Case {
                args: &["*", "!foo1*"],
                columns: &["foo", "foo1", "foo2"],
                accepted: &[true, false, true],
            },
            Case {
                args: &["/^foo/"],
                columns: &["foo", "foo1", "fff"],
                accepted: &[true, true, false],
            },
            Case {
                args: &["*", "!foo[bar]", "!bar?", r"!special\\"],
                columns: &[
                    "food",
                    "foor",
                    "foo[bar]",
                    "ba",
                    "bar?",
                    r"special\",
                    r"special\\",
                    "bazzz",
                    r"special\$",
                    "afooa",
                ],
                accepted: &[
                    true, false, true, true, false, false, true, true, true, true,
                ],
            },
            Case {
                args: &["*", "!/a?b?f[0-9]/"],
                columns: &["abbdf1", "aaaaf2", "55", "abbcfa"],
                accepted: &[false, false, true, true],
            },
            Case {
                args: &["BAR"],
                columns: &["bar", "BAR"],
                accepted: &[true, true],
            },
            Case {
                args: &["# comment", "x", "   \t"],
                columns: &["x", "y"],
                accepted: &[true, false],
            },
            Case {
                args: &["p_123$", "中文"],
                columns: &["p_123", "p_123$", "英文", "中文"],
                accepted: &[false, true, false, true],
            },
            Case {
                args: &[r"\\\."],
                columns: &[r"\.", r"\\\.", r"\a"],
                accepted: &[true, false, false],
            },
            Case {
                args: &["[!a-z]"],
                columns: &["!", "a", "1"],
                accepted: &[true, false, true],
            },
            Case {
                args: &["\"some \"\"quoted\"\"\""],
                columns: &[
                    r#"some "quoted""#,
                    r#"some ""quoted"""#,
                    r#"SOME "QUOTED""#,
                    "some\t\"quoted\"",
                ],
                accepted: &[true, false, true, false],
            },
            Case {
                args: &["db*", "!cfg*", "cfgsample", r"a\.b\.c"],
                columns: &["irrelevant", "db1", "cfg1", "cfgsample", "a.b.c"],
                accepted: &[false, true, false, true, true],
            },
            Case {
                args: &["*", "!D[!a-d]"],
                columns: &["S", "Da", "Db", "Daa", "dD", "de"],
                accepted: &[true, true, true, true, true, false],
            },
            Case {
                args: &[r"?\.?"],
                columns: &["a", "a.b", "中文.英文", "我.你"],
                accepted: &[false, true, false, true],
            },
            Case {
                args: &["*", r"!?\.?"],
                columns: &["a", ".b", ".英文", "我.你", "我.你.他"],
                accepted: &[true, true, true, false, true],
            },
        ];

        for tc in cases {
            let f = parse_column_filter(tc.args).unwrap();
            for (col, &want) in tc.columns.iter().zip(tc.accepted) {
                assert_eq!(f.match_column(col), want, "args={:?} col={col}", tc.args);
            }
        }
    }

    // Go `TestParseFailures`.
    #[test]
    fn parse_failures() {
        // (arg, expected error substring). Go asserts the exact `regexp` error
        // text for the compile-failure cases; Rust's `regex` rejects the same
        // patterns with different wording, so those two cases assert the shared
        // `error parsing regexp:` prefix instead.
        let cases: &[(&str, &str)] = &[
            (
                "/^t[0-9]+((?!_copy))*$/",
                "invalid pattern: error parsing regexp:",
            ),
            (r"a%b\.c", "unexpected special character '%'"),
            (
                r"a\tb\.c",
                r"cannot escape a letter or number (\t), it is reserved for future extension",
            ),
            (r"[]\.*", "syntax error: failed to parse character class"),
            (r"[!]\.*", "invalid pattern: error parsing regexp:"),
            ("[.*", "syntax error: failed to parse character class"),
            (r"[\d\D].*", "syntax error: failed to parse character class"),
            ("db.", "unexpected special character '.'"),
            (r"/db\.*", "syntax error: incomplete regexp"),
            (r"`db\.*", "syntax error: incomplete quoted identifier"),
            (r#""db\.*"#, "syntax error: incomplete quoted identifier"),
            (r"db\", r"syntax error: cannot place \ at end of line"),
            (r"db\.tbl#not comment", "unexpected special character '#'"),
        ];
        for (arg, want) in cases {
            let err = parse_column_filter(&[arg]).err().unwrap();
            assert!(err.to_string().contains(want), "arg={arg:?} err={err}");
        }
    }

    // Go `TestImport`.
    #[test]
    fn import() {
        let dir = TempDir::new();
        let path1 = dir.write(
            "1.txt",
            "\n\t\tcol?tql?\n\t\tcol?\\.tql?\n\t\tcol02\\.tql02\n\t",
        );
        let path2 = dir.write("2.txt", "\n\t\tcol03\\.tql03\n\t\t!col4\\.tql4\n\t");

        let f = parse_column_filter(&[&format!("@{path1}"), &format!("@{path2}"), r"col04\.tql04"])
            .unwrap();

        assert!(f.match_column("col1tql1"));
        assert!(f.match_column("col2.tql2"));
        assert!(f.match_column("col3.tql3"));
        assert!(!f.match_column("col4.tql4"));
        assert!(!f.match_column("col01tql01"));
        assert!(!f.match_column("col01.tql01"));
        assert!(f.match_column("col02.tql02"));
        assert!(f.match_column("col03.tql03"));
        assert!(f.match_column("col04.tql04"));
    }

    // Go `TestRecursiveImport`.
    #[test]
    fn recursive_import() {
        let dir = TempDir::new();
        let path3 = dir.write("3.txt", "col1");
        let path4 = dir.write("4.txt", &format!("# comment\n\n@{path3}"));

        let err = parse_column_filter(&[&format!("@{path4}")]).err().unwrap();
        assert!(
            err.to_string()
                .contains("4.txt:3: importing filter files recursively is not allowed"),
            "err={err}"
        );

        let missing = dir.path("5.txt");
        let err = parse_column_filter(&[&format!("@{missing}")])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("cannot open filter file: open") && msg.contains("5.txt"),
            "err={msg}"
        );
    }
}
