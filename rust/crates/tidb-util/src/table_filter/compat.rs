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

//! Transcreation of Go `pkg/util/table-filter/compat.go`.
//!
//! Legacy MySQL-replication filter compatibility, plus schema/table set
//! filters.
//!
//! Go's `Table`/`MySQLReplicationRules` carry `toml`/`json`/`yaml` struct tags
//! for config deserialization. No test exercises that serialization and this
//! crate does not wire up serde/toml/yaml, so the tags are out of scope; the
//! observable filter behavior (the only thing under test) is fully ported.

use super::matchers::{new_regexp_matcher, Matcher, TableRule};
use super::{Filter, FilterError, TableFilter};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// A qualified table name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Table {
    /// The name of the schema (database) containing this table.
    pub schema: String,
    /// The unqualified table name.
    pub name: String,
}

impl Table {
    /// Creates a new [`Table`].
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Table {
            schema: schema.into(),
            name: name.into(),
        }
    }

    // Go keeps `lessThan` behind `//nolint:unused`; ported for completeness.
    #[allow(dead_code)]
    fn less_than(&self, other: &Table) -> bool {
        self.schema < other.schema || self.schema == other.schema && self.name < other.name
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.name.is_empty() {
            write!(f, "`{}`.`{}`", self.schema, self.name)
        } else {
            write!(f, "`{}`", self.schema)
        }
    }
}

/// A set of rules based on MySQL's replication filter.
#[derive(Clone, Debug, Default)]
pub struct MySQLReplicationRules {
    /// An allowlist of tables.
    pub do_tables: Vec<Table>,
    /// An allowlist of schemas.
    pub do_dbs: Vec<String>,
    /// A blocklist of tables.
    pub ignore_tables: Vec<Table>,
    /// A blocklist of schemas.
    pub ignore_dbs: Vec<String>,
}

impl MySQLReplicationRules {
    /// Converts all entries to lowercase.
    ///
    /// Deprecated: use [`super::case_insensitive`] instead.
    pub fn to_lower(&mut self) {
        for table in &mut self.do_tables {
            table.name = table.name.to_lowercase();
            table.schema = table.schema.to_lowercase();
        }
        for table in &mut self.ignore_tables {
            table.name = table.name.to_lowercase();
            table.schema = table.schema.to_lowercase();
        }
        for db in &mut self.ignore_dbs {
            *db = db.to_lowercase();
        }
        for db in &mut self.do_dbs {
            *db = db.to_lowercase();
        }
    }
}

struct SchemasFilter {
    schemas: HashSet<String>,
}

impl Filter for SchemasFilter {
    fn match_table(&self, schema: &str, _table: &str) -> bool {
        self.match_schema(schema)
    }

    fn match_schema(&self, schema: &str) -> bool {
        self.schemas.contains(schema)
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        Box::new(SchemasFilter {
            schemas: self.schemas.iter().map(|s| s.to_lowercase()).collect(),
        })
    }
}

/// Creates a filter which only accepts a list of schemas.
pub fn new_schemas_filter(schemas: &[&str]) -> Box<dyn Filter> {
    Box::new(SchemasFilter {
        schemas: schemas.iter().map(|s| s.to_string()).collect(),
    })
}

struct TablesFilter {
    schemas: HashMap<String, HashSet<String>>,
}

impl Filter for TablesFilter {
    fn match_table(&self, schema: &str, table: &str) -> bool {
        match self.schemas.get(schema) {
            Some(tables) => tables.contains(table),
            None => false,
        }
    }

    fn match_schema(&self, schema: &str) -> bool {
        self.schemas.contains_key(schema)
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        let mut lowered: HashMap<String, HashSet<String>> = HashMap::new();
        for (schema, tables) in &self.schemas {
            let entry = lowered.entry(schema.to_lowercase()).or_default();
            for table in tables {
                entry.insert(table.to_lowercase());
            }
        }
        Box::new(TablesFilter { schemas: lowered })
    }
}

/// Creates a filter which only accepts a list of tables.
pub fn new_tables_filter(tables: &[Table]) -> Box<dyn Filter> {
    let mut schemas: HashMap<String, HashSet<String>> = HashMap::new();
    for table in tables {
        schemas
            .entry(table.schema.clone())
            .or_default()
            .insert(table.name.clone());
    }
    Box::new(TablesFilter { schemas })
}

/// A filter which passes only if both wrapped filters pass.
struct BothFilter {
    a: Box<dyn Filter>,
    b: Box<dyn Filter>,
}

impl Filter for BothFilter {
    fn match_table(&self, schema: &str, table: &str) -> bool {
        self.a.match_table(schema, table) && self.b.match_table(schema, table)
    }

    fn match_schema(&self, schema: &str) -> bool {
        self.a.match_schema(schema) && self.b.match_schema(schema)
    }

    fn to_lower(&self) -> Box<dyn Filter> {
        Box::new(BothFilter {
            a: self.a.to_lower(),
            b: self.b.to_lower(),
        })
    }
}

/// Applies the legacy glob replacements after `regex::escape`, mirroring Go's
/// `legacyWildcardReplacer` over `regexp.QuoteMeta`. `[!` -> `[^` must win over
/// `[`, so it is tried first.
fn legacy_wildcard_replace(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        let rest = &b[i..];
        if rest.starts_with(br"\[!") {
            out.push_str("[^");
            i += 3;
        } else if rest.starts_with(br"\*") {
            out.push_str(".*");
            i += 2;
        } else if rest.starts_with(br"\?") {
            out.push('.');
            i += 2;
        } else if rest.starts_with(br"\[") {
            out.push('[');
            i += 2;
        } else if rest.starts_with(br"\]") {
            out.push(']');
            i += 2;
        } else {
            let len = utf8_char_len(b[i]);
            out.push_str(std::str::from_utf8(&b[i..i + len]).expect("valid UTF-8"));
            i += len;
        }
    }
    out
}

fn utf8_char_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

fn matcher_from_legacy_pattern(pattern: &str) -> Result<Matcher, FilterError> {
    if pattern.is_empty() {
        return Err(FilterError::new("pattern cannot be empty"));
    }
    if pattern.as_bytes()[0] == b'~' {
        // this is a regexp pattern.
        return new_regexp_matcher(&pattern[1..]);
    }
    if !pattern.contains(['?', '*', '[']) {
        // this is a literal string.
        return Ok(Matcher::Str(pattern.to_string()));
    }
    // this is a wildcard.
    let pat = format!("(?s)^{}$", legacy_wildcard_replace(&regex::escape(pattern)));
    new_regexp_matcher(&pat)
}

/// Constructs up to 2 filters from the MySQL replication rules. Tables have to
/// pass *both* filters to be processed.
pub fn parse_mysql_replication_rules(
    rules: Option<&MySQLReplicationRules>,
) -> Result<Box<dyn Filter>, FilterError> {
    let rules = match rules {
        Some(r) => r,
        None => return Ok(super::all()),
    };

    let mut schemas: &[String] = &rules.do_dbs;
    let mut positive = true;
    if schemas.is_empty() {
        schemas = &rules.ignore_dbs;
        positive = false;
    }

    let mut schema_rules: Vec<TableRule> = Vec::new();
    for schema in schemas {
        let m = matcher_from_legacy_pattern(schema)?;
        schema_rules.push(TableRule {
            schema: m,
            table: Matcher::True,
            positive,
        });
    }
    if !positive {
        schema_rules.push(TableRule {
            schema: Matcher::True,
            table: Matcher::True,
            positive: true,
        });
    }

    let mut tables: &[Table] = &rules.do_tables;
    positive = true;
    if tables.is_empty() {
        tables = &rules.ignore_tables;
        positive = false;
    }

    let mut table_rules: Vec<TableRule> = Vec::new();
    for table in tables {
        let sm = matcher_from_legacy_pattern(&table.schema)?;
        let tm = matcher_from_legacy_pattern(&table.name)?;
        table_rules.push(TableRule {
            schema: sm,
            table: tm,
            positive,
        });
    }
    if !positive {
        table_rules.push(TableRule {
            schema: Matcher::True,
            table: Matcher::True,
            positive: true,
        });
    }

    Ok(Box::new(BothFilter {
        a: Box::new(TableFilter::new(schema_rules)),
        b: Box::new(TableFilter::new(table_rules)),
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        new_schemas_filter, new_tables_filter, parse_mysql_replication_rules,
        MySQLReplicationRules, Table,
    };
    use crate::table_filter::case_insensitive;

    fn t(schema: &str, name: &str) -> Table {
        Table::new(schema, name)
    }

    // Go `TestSchemaFilter`.
    #[test]
    fn schema_filter() {
        let sf0 = case_insensitive(new_schemas_filter(&["foo?", "bar"]));
        assert!(sf0.match_table("foo?", "a"));
        assert!(!sf0.match_table("food", "a"));
        assert!(sf0.match_table("bar", "b"));
        assert!(sf0.match_table("BAR", "b"));

        let sf1 = new_schemas_filter(&[r"\baz"]);
        assert!(!sf1.match_schema("baz"));
        assert!(!sf1.match_schema("Baz"));
        assert!(sf1.match_schema(r"\baz"));
        assert!(!sf1.match_schema(r"\Baz"));

        let sf2 = new_schemas_filter(&[]);
        assert!(!sf2.match_table("aaa", "bbb"));
    }

    // Go `TestTableFilter`.
    #[test]
    fn table_filter() {
        let tf0 = case_insensitive(new_tables_filter(&[t("foo?", "bar*"), t("BAR?", "FOO*")]));
        assert!(tf0.match_table("foo?", "bar*"));
        assert!(tf0.match_table("bar?", "foo*"));
        assert!(tf0.match_table("FOO?", "BAR*"));
        assert!(!tf0.match_table("foo?", "bar"));
        assert!(!tf0.match_table("BARD", "FOO*"));

        let tf1 = new_tables_filter(&[t(r"\baz", "BAR")]);
        assert!(!tf1.match_schema("baz"));
        assert!(!tf1.match_schema("Baz"));
        assert!(tf1.match_schema(r"\baz"));
        assert!(!tf1.match_schema(r"\Baz"));

        let tf2 = new_tables_filter(&[]);
        assert!(!tf2.match_table("aaa", "bbb"));
    }

    // Go `TestLegacyFilter`.
    #[test]
    fn legacy_filter() {
        struct Case {
            rules: MySQLReplicationRules,
            accepted: Vec<Table>,
            rejected: Vec<Table>,
        }
        let mk = |do_dbs: &[&str],
                  do_tables: &[Table],
                  ignore_dbs: &[&str],
                  ignore_tables: &[Table],
                  accepted: Vec<Table>,
                  rejected: Vec<Table>| Case {
            rules: MySQLReplicationRules {
                do_dbs: do_dbs.iter().map(|s| s.to_string()).collect(),
                do_tables: do_tables.to_vec(),
                ignore_dbs: ignore_dbs.iter().map(|s| s.to_string()).collect(),
                ignore_tables: ignore_tables.to_vec(),
            },
            accepted,
            rejected,
        };
        let cases = vec![
            mk(&[], &[], &[], &[], vec![t("foo", "bar")], vec![]),
            mk(
                &["foo"],
                &[],
                &["foo"],
                &[],
                vec![t("foo", "bar")],
                vec![t("foo1", "bar")],
            ),
            mk(
                &[],
                &[],
                &["foo1"],
                &[],
                vec![t("foo", "bar")],
                vec![t("foo1", "bar")],
            ),
            mk(
                &[],
                &[t("foo", "bar1")],
                &[],
                &[],
                vec![t("foo", "bar1")],
                vec![t("foo", "bar"), t("foo1", "bar"), t("foo1", "bar1")],
            ),
            mk(
                &[],
                &[],
                &[],
                &[t("foo", "bar")],
                vec![t("foo", "bar1"), t("foo1", "bar"), t("foo1", "bar1")],
                vec![t("foo", "bar")],
            ),
            mk(
                &["~^foo"],
                &[],
                &[],
                &[t("~^foo", r"~^sbtest-\d")],
                vec![t("foo", "sbtest"), t("foo", r"sbtest-\d")],
                vec![t("fff", "bar"), t("foo1", "sbtest-1")],
            ),
            mk(
                &[],
                &[],
                &["foo[bar]", "baz?", r"special\"],
                &[],
                vec![
                    t("foo[bar]", "1"),
                    t("food", "2"),
                    t("fo", "3"),
                    t(r"special\\", "4"),
                    t("bazzz", "9"),
                    t(r"special\$", "10"),
                    t("afooa", "11"),
                ],
                vec![
                    t("foor", "5"),
                    t("baz?", "6"),
                    t("baza", "7"),
                    t(r"special\", "8"),
                ],
            ),
            mk(
                &[r"!@#$%^&*\?"],
                &[],
                &[],
                &[],
                vec![t(r"!@#$%^&abcdef\g", "1")],
                vec![t("abcdef", "2")],
            ),
            mk(
                &["1[!abc]", "2[^abc]", r"3[\d]"],
                &[],
                &[],
                &[],
                vec![
                    t("1!", "1"),
                    t("1z", "4"),
                    t("2^", "3"),
                    t("2a", "5"),
                    t("3d", "6"),
                    t(r"3\", "8"),
                ],
                vec![t("1a", "2"), t("30", "7")],
            ),
            mk(
                &["foo", "bar"],
                &[t("*", "a"), t("*", "b")],
                &[],
                &[],
                vec![t("foo", "a"), t("foo", "b"), t("bar", "a"), t("bar", "b")],
                vec![t("foo", "c"), t("baz", "a")],
            ),
        ];

        let f = parse_mysql_replication_rules(None).unwrap();
        assert!(f.match_table("foo", "bar"));

        for tc in cases {
            let f = case_insensitive(parse_mysql_replication_rules(Some(&tc.rules)).unwrap());
            for tbl in &tc.accepted {
                assert!(f.match_table(&tbl.schema, &tbl.name), "accept case {tbl}");
            }
            for tbl in &tc.rejected {
                assert!(!f.match_table(&tbl.schema, &tbl.name), "reject case {tbl}");
            }
        }
    }

    // Go `TestParseLegacyFailures`.
    #[test]
    fn parse_legacy_failures() {
        // Go asserts `regexp`'s `missing closing ]` for `[a`; Rust's `regex`
        // rejects the same pattern with different wording, so that case asserts
        // the shared `error parsing regexp:` prefix instead.
        let cases: &[(&str, &str)] = &[
            ("[a", "error parsing regexp:"),
            ("", "pattern cannot be empty"),
        ];
        for (arg, want) in cases {
            let rules = MySQLReplicationRules {
                do_dbs: vec![arg.to_string()],
                ..Default::default()
            };
            let err = parse_mysql_replication_rules(Some(&rules)).err().unwrap();
            assert!(err.to_string().contains(want), "arg={arg:?} err={err}");
        }
    }
}
