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

//! Routes a source schema/table to a target schema/table, with extend columns
//! whose values may be extracted from the source names by regular expression.
//!
//! Transcreation of the whole Go package `pkg/util/regexpr-router`
//! (`package regexprrouter`, file `regexpr_router.go`).

use std::fmt;

use regex::Regex;

use crate::filter::{self, FilterError, Rules, Table};
use crate::table_router::{
    RouterError, SchemaExtractor, SourceExtractor, TableExtractor, TableRule,
};

/// The type of filter.
///
/// Go: `type FilterType = int32` — an *alias*, not a defined type, so any
/// `int32` is a `FilterType` and only [`TBL_FILTER`] and [`SCHM_FILTER`] carry
/// meaning. Kept as an alias here because it is exported Go API.
///
/// boundary: stands for `regexprrouter.FilterType`. The only consumer is the
/// `filterWrapper.typ == TblFilter` test inside [`RouteTable::route`] and
/// [`RouteTable::all_rules`], whose `else` branch treats every other value as a
/// schema filter. That field is private and is assigned one of the two
/// constants on every construction path, so the third-value reading is
/// unreachable in this port.
pub type FilterType = i32;

/// Table filter. Go: `TblFilter FilterType = iota + 1`.
pub const TBL_FILTER: FilterType = 1;
/// Schema filter. Go: `SchmFilter`.
pub const SCHM_FILTER: FilterType = 2;

/// Errors returned by the regexp router.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RegExprRouterError {
    /// Go: `errors.Trace(err)` over `rule.Valid()` in `AddRule`.
    Router(RouterError),
    /// Go: `errors.Annotatef(err, "add rule %+v into table router", rule)` over
    /// the `filter.New` failure in `AddRule`. The first field is the rendered
    /// annotation, already carrying the rule.
    Annotated(String, FilterError),
    /// Go: `errors.Errorf("table %s.%s matches more than one rule", schema, table)`.
    MatchesMoreThanOneRule {
        /// The source schema being routed.
        schema: String,
        /// The source table being routed.
        table: String,
    },
}

impl fmt::Display for RegExprRouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegExprRouterError::Router(err) => write!(f, "{err}"),
            RegExprRouterError::Annotated(ctx, inner) => write!(f, "{ctx}: {inner}"),
            RegExprRouterError::MatchesMoreThanOneRule { schema, table } => {
                write!(f, "table {schema}.{table} matches more than one rule")
            }
        }
    }
}

impl std::error::Error for RegExprRouterError {}

impl From<RouterError> for RegExprRouterError {
    fn from(err: RouterError) -> RegExprRouterError {
        RegExprRouterError::Router(err)
    }
}

/// One rule compiled into a `filter.Filter` plus the target it routes to.
///
/// Go: `type filterWrapper struct` (unexported, so this stays private too).
#[derive(Debug)]
struct FilterWrapper {
    /// Go: `filter *filter.Filter`. Never nil once `AddRule` returns, and the
    /// only constructor sets it, so it is held by value rather than as an
    /// `Option`: a missing filter must never be readable as "matches
    /// everything".
    filter: filter::Filter,
    /// Go: `rawRule *router.TableRule`.
    raw_rule: TableRule,
    /// Go: `target filter.Table`.
    target: Table,
    /// Go: `typ FilterType`.
    typ: FilterType,
}

/// A route table: an ordered list of rules, each matched by its own
/// `filter.Filter`.
///
/// Go: `type RouteTable struct`.
#[derive(Debug)]
pub struct RouteTable {
    filters: Vec<FilterWrapper>,
    case_sensitive: bool,
}

/// Creates a [`RouteTable`].
///
/// Go: `func NewRegExprRouter(caseSensitive bool, rules []*router.TableRule) (*RouteTable, error)`.
///
/// Quirk 1: each rule is validated and (for a case-insensitive router)
/// lowercased *in place*, so the caller's rules come back rewritten. `rules` is
/// `&mut` to keep that visible.
pub fn new_reg_expr_router(
    case_sensitive: bool,
    rules: &mut [TableRule],
) -> Result<RouteTable, RegExprRouterError> {
    let mut r = RouteTable {
        filters: Vec::new(),
        case_sensitive,
    };
    for rule in rules.iter_mut() {
        r.add_rule(rule)?;
    }
    Ok(r)
}

impl RouteTable {
    /// Whether this router matches case-sensitively. Go: the unexported
    /// `caseSensitive` field.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Adds a rule into the route table.
    ///
    /// Go: `func (r *RouteTable) AddRule(rule *router.TableRule) error`.
    ///
    /// Quirk 2: `ToLower` lowercases only `SchemaPattern` and `TablePattern`.
    /// `TargetSchema`/`TargetTable` are copied into the wrapper's target
    /// verbatim, so a case-insensitive router still routes to the target names
    /// exactly as the caller spelled them.
    ///
    /// Quirk 3: for a table rule Go registers the table pattern *and* the bare
    /// schema pattern in `DoDBs`. The schema allowlist is what lets
    /// `Route(schema, "")` (an empty table name) still match the rule.
    ///
    /// boundary: Go stores the caller's `*router.TableRule` pointer, so later
    /// edits to the caller's rule are visible through `AllRules` and
    /// `FetchExtendColumn`. This port stores a clone taken at insert time, so a
    /// routing decision cannot change under a reader that never asked for it.
    /// No in-tree caller mutates a rule after adding it.
    pub fn add_rule(&mut self, rule: &mut TableRule) -> Result<(), RegExprRouterError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        let target = Table {
            schema: rule.target_schema.clone(),
            name: rule.target_table.clone(),
        };
        // Snapshot the annotation before the rule is moved into the wrapper;
        // Go formats it lazily from the still-live pointer.
        let ctx = format!("add rule {rule} into table router");
        let (typ, rules) = if rule.table_pattern.is_empty() {
            // raw schema rule
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
                    do_tables: vec![Table {
                        schema: rule.schema_pattern.clone(),
                        name: rule.table_pattern.clone(),
                    }],
                    do_dbs: vec![rule.schema_pattern.clone()],
                    ..Rules::default()
                },
            )
        };
        let raw_filter = filter::new(self.case_sensitive, Some(rules))
            .map_err(|err| RegExprRouterError::Annotated(ctx, err))?;
        self.filters.push(FilterWrapper {
            filter: raw_filter,
            raw_rule: rule.clone(),
            target,
            typ,
        });
        Ok(())
    }

    /// Routes a source schema/table to its target schema/table.
    ///
    /// Go: `func (r *RouteTable) Route(schema, table string) (targetSchema string, targetTable string, err error)`.
    ///
    /// Quirk 4: an ambiguous match is an error, but only *within* a tier. A
    /// table that matches one table rule and three schema rules routes by the
    /// table rule without complaint; the schema rules are never counted.
    ///
    /// Quirk 5: a rule with an empty `TargetSchema`/`TargetTable` falls back to
    /// the *source* schema/table, so a matched rule can be indistinguishable
    /// from no match at all.
    ///
    /// Quirk 6: an empty `table` skips the table tier entirely even when table
    /// rules matched, because the schema pattern is also in the filter's
    /// `DoDBs`.
    pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), RegExprRouterError> {
        let cur_table = Table {
            schema: schema.to_owned(),
            name: table.to_owned(),
        };
        let mut tbl_rules: Vec<&FilterWrapper> = Vec::new();
        let mut schm_rules: Vec<&FilterWrapper> = Vec::new();
        for wrapper in &self.filters {
            if wrapper.filter.matches(&cur_table) {
                if wrapper.typ == TBL_FILTER {
                    tbl_rules.push(wrapper);
                } else {
                    schm_rules.push(wrapper);
                }
            }
        }
        let mut target_schema = String::new();
        let mut target_table = String::new();
        if table.is_empty() || tbl_rules.is_empty() {
            // 1. no need to match table or
            // 2. match no table
            if schm_rules.len() > 1 {
                return Err(RegExprRouterError::MatchesMoreThanOneRule {
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                });
            }
            if schm_rules.len() == 1 {
                target_schema = schm_rules[0].target.schema.clone();
                target_table = schm_rules[0].target.name.clone();
            }
        } else {
            if tbl_rules.len() > 1 {
                return Err(RegExprRouterError::MatchesMoreThanOneRule {
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                });
            }
            target_schema = tbl_rules[0].target.schema.clone();
            target_table = tbl_rules[0].target.name.clone();
        }
        if target_schema.is_empty() {
            target_schema = schema.to_owned();
        }
        if target_table.is_empty() {
            target_table = table.to_owned();
        }
        Ok((target_schema, target_table))
    }

    /// Returns the schema-level rules and the table-level rules, each in
    /// insertion order.
    ///
    /// Go: `func (r *RouteTable) AllRules() (schmRouteRules []router.TableRule, tableRouteRules []router.TableRule)`.
    ///
    /// boundary: Go's two named results start as `nil` and only ever grow by
    /// `append`, so "no schema rules" is a nil slice a caller could tell apart
    /// from an empty one. Both are returned as `Vec` here: unlike
    /// [`RouteTable::fetch_extend_column`] the two slices are independent and
    /// every in-tree caller only ranges over them or takes their length, for
    /// which nil and empty behave identically in Go.
    ///
    /// boundary: Go copies each `*TableRule` into the result (`*f.rawRule`), a
    /// shallow copy that still shares the three extractor pointers with the
    /// stored rule. The clones here are deep, so mutating a returned rule's
    /// extractor cannot reach back into the router.
    pub fn all_rules(&self) -> (Vec<TableRule>, Vec<TableRule>) {
        let mut schm_route_rules = Vec::new();
        let mut table_route_rules = Vec::new();
        for f in &self.filters {
            if f.typ == SCHM_FILTER {
                schm_route_rules.push(f.raw_rule.clone());
            } else {
                table_route_rules.push(f.raw_rule.clone());
            }
        }
        (schm_route_rules, table_route_rules)
    }

    /// Finds the extend columns and their values for a source schema/table.
    ///
    /// Go: `func (r *RouteTable) FetchExtendColumn(schema, table, source string) (cols []string, vals []string)`.
    ///
    /// Quirk 7: this re-partitions the matched rules by `rule.TablePattern == ""`
    /// rather than by the wrapper's `typ`, which was derived from exactly that
    /// test in `AddRule`. The redundancy is Go's; the two can never disagree.
    ///
    /// Quirk 8: an ambiguous match is silently resolved by taking the first
    /// rule, where [`RouteTable::route`] would have errored.
    ///
    /// Quirk 9: no lowercasing happens here even for a case-insensitive router,
    /// so the extractor regexps see the caller's original casing while the
    /// patterns they were selected by were lowercased.
    ///
    /// Go's named results start as `nil` and are only appended to together, so
    /// `None` stands for Go's `(nil, nil)` — a caller can tell that apart from
    /// two empty slices.
    pub fn fetch_extend_column(
        &self,
        schema: &str,
        table: &str,
        source: &str,
    ) -> Option<(Vec<String>, Vec<String>)> {
        let cur_table = Table {
            schema: schema.to_owned(),
            name: table.to_owned(),
        };
        let mut schema_rules: Vec<&TableRule> = Vec::new();
        let mut table_rules: Vec<&TableRule> = Vec::new();
        for f in &self.filters {
            if f.filter.matches(&cur_table) {
                if f.raw_rule.table_pattern.is_empty() {
                    schema_rules.push(&f.raw_rule);
                } else {
                    table_rules.push(&f.raw_rule);
                }
            }
        }
        if table_rules.is_empty() && schema_rules.is_empty() {
            return None;
        }
        // table level rules have highest priority
        let rule = if table_rules.is_empty() {
            schema_rules[0]
        } else {
            table_rules[0]
        };

        let mut cols: Vec<String> = Vec::new();
        let mut vals: Vec<String> = Vec::new();

        if let Some(ext) = rule.table_extractor.as_ref() {
            cols.push(ext.target_column.clone());
            vals.push(extract_val(table, Extractor::Table(ext)));
        }

        if let Some(ext) = rule.schema_extractor.as_ref() {
            cols.push(ext.target_column.clone());
            vals.push(extract_val(schema, Extractor::Schema(ext)));
        }

        if let Some(ext) = rule.source_extractor.as_ref() {
            cols.push(ext.target_column.clone());
            vals.push(extract_val(source, Extractor::Source(ext)));
        }

        if cols.is_empty() {
            return None;
        }
        Some((cols, vals))
    }
}

/// Which of the three extractors a value is being pulled out of.
///
/// boundary: stands for the `ext any` parameter of this package's
/// `func extractVal(s string, ext any) string` together with its
/// `switch e := ext.(type)` over exactly `*router.TableExtractor`,
/// `*router.SchemaExtractor` and `*router.SourceExtractor`. That switch has
/// **no** `default:` arm, so any other dynamic type leaves `params` nil and
/// yields the empty string. A closed three-variant enum removes that silent
/// path rather than adding a catch-all, and both call sites in this file
/// already pass one of the three.
///
/// `crate::table_router` declares a private enum of the same shape for the
/// *different* Go function `(*router.TableRule).extractVal`. It cannot be
/// reused: it is private to that module, and the two Go functions differ in
/// substance — that one reads the regexp the rule compiled during `Valid`,
/// this one recompiles the raw regexp string on every call (see
/// [`extract_val`]).
#[derive(Clone, Copy, Debug)]
enum Extractor<'a> {
    Table(&'a TableExtractor),
    Schema(&'a SchemaExtractor),
    Source(&'a SourceExtractor),
}

/// Extracts a value from `s` with the extractor's regexp.
///
/// Go: `func extractVal(s string, ext any) string` — this package's own free
/// function, not `(*router.TableRule).extractVal`.
///
/// Quirk 10: it calls `regexp.Compile` on the raw regexp string on every call
/// and discards the pre-compiled regexp that `TableRule.Valid` stored on the
/// extractor. `AddRule` always runs `Valid` first, so in practice the compile
/// cannot fail — but Go *ignores* a compile error here (leaving `params` nil
/// and returning the empty string) where `(*router.TableRule).extractVal` would
/// dereference a nil regexp and panic.
///
/// Quirk 11: capture group 0 (the whole match) is skipped and every other group
/// is concatenated with no separator, so a two-group regexp over `source_s1_s1`
/// yields `s1s1`.
fn extract_val(s: &str, ext: Extractor<'_>) -> String {
    let pattern = match ext {
        Extractor::Table(e) => e.table_regexp.as_str(),
        Extractor::Schema(e) => e.schema_regexp.as_str(),
        Extractor::Source(e) => e.source_regexp.as_str(),
    };
    // Go: `if regExpr, err := regexp.Compile(...); err == nil` — a compile
    // failure leaves `params` nil, which the loop below renders as "".
    //
    // boundary: Go compiles with `regexp` (RE2); the `regex` crate is the same
    // automaton family and rejects the same constructs, differing only in that
    // `\d`/`\w`/`\s` are Unicode-aware rather than ASCII-only.
    let Ok(re) = Regex::new(pattern) else {
        return String::new();
    };
    // Go: `FindStringSubmatch` returns nil when there is no match, so the loop
    // never runs and the result is the empty string.
    let Some(caps) = re.captures(s) else {
        return String::new();
    };
    let mut val = String::new();
    for idx in 0..caps.len() {
        if idx > 0 {
            // Go renders a non-participating group as the empty string.
            val.push_str(caps.get(idx).map_or("", |m| m.as_str()));
        }
    }
    val
}

/// Transcreated from `pkg/util/regexpr-router/regexpr_router_test.go`, the only
/// upstream coverage of this package: `TestCreateRouter`, `TestAddRule`,
/// `TestSchemaRoute`, `TestTableRoute`, `TestRegExprRoute`,
/// `TestFetchExtendColumn`, `TestAllRule` and `TestDupMatch`. Four extra tests
/// are marked `_written`: upstream never exercises the case-insensitive target
/// casing, `extractVal`'s no-match and multi-group paths on their own, or
/// `Route` with an empty table name against table rules.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_router::new_table_router;

    fn schema_rule(schema_pattern: &str, target_schema: &str) -> TableRule {
        TableRule {
            schema_pattern: schema_pattern.to_owned(),
            target_schema: target_schema.to_owned(),
            ..TableRule::default()
        }
    }

    fn table_rule(
        schema_pattern: &str,
        table_pattern: &str,
        target_schema: &str,
        target_table: &str,
    ) -> TableRule {
        TableRule {
            schema_pattern: schema_pattern.to_owned(),
            table_pattern: table_pattern.to_owned(),
            target_schema: target_schema.to_owned(),
            target_table: target_table.to_owned(),
            ..TableRule::default()
        }
    }

    /// Go: `TestCreateRouter`.
    #[test]
    fn test_create_router() {
        assert!(new_reg_expr_router(true, &mut []).is_ok());
        assert!(new_reg_expr_router(false, &mut []).is_ok());
    }

    /// Go: `TestAddRule`. The same rule slice is added to a case-sensitive
    /// router and then to a case-insensitive one, so the second pass sees the
    /// rules the first pass left behind — Go's order is preserved.
    #[test]
    fn test_add_rule() {
        let mut r = new_reg_expr_router(true, &mut []).expect("create case-sensitive router");
        let mut rules = [
            schema_rule("test1", "dtest1"),
            table_rule("test2", "table2", "dtest2", "dtable2"),
        ];
        for rule in rules.iter_mut() {
            r.add_rule(rule)
                .expect("add rule into case-sensitive router");
        }
        let mut r = new_reg_expr_router(false, &mut []).expect("create case-insensitive router");
        for rule in rules.iter_mut() {
            r.add_rule(rule)
                .expect("add rule into case-insensitive router");
        }
    }

    /// Go: `TestSchemaRoute`. Both routers are built from the same rule slice
    /// and must agree.
    #[test]
    fn test_schema_route() {
        let mut rules = vec![
            schema_rule("test1", "dtest1"),
            schema_rule("gtest*", "dtest"),
        ];
        let old_router = new_table_router(true, &mut rules).expect("create table router");
        let new_router = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        let input_tables = [
            // match rule 1
            Table::new("test1", "table1"),
            // match rule 2
            Table::new("gtesttest", "atable"),
            // match neither
            Table::new("ptest", "atableg"),
        ];
        let expected_result = [
            Table::new("dtest1", "table1"),
            Table::new("dtest", "atable"),
            Table::new("ptest", "atableg"),
        ];
        for (input, expected) in input_tables.iter().zip(expected_result.iter()) {
            let (old_schema, old_table) = old_router
                .route(&input.schema, &input.name)
                .expect("table router route");
            let (new_schema, new_table) = new_router
                .route(&input.schema, &input.name)
                .expect("regexp router route");
            assert_eq!(expected.schema, old_schema);
            assert_eq!(expected.name, old_table);
            assert_eq!(expected.schema, new_schema);
            assert_eq!(expected.name, new_table);
        }
    }

    /// Go: `TestTableRoute`.
    #[test]
    fn test_table_route() {
        let mut rules = vec![
            table_rule("test1", "table1", "dtest1", "dtable1"),
            table_rule("test*", "table2", "dtest2", "dtable2"),
            table_rule("test3", "table*", "dtest3", "dtable3"),
        ];
        let mut input_tables = Vec::new();
        let mut exp_tables = Vec::new();
        for i in 1..=3 {
            input_tables.push(Table::new(format!("test{i}"), format!("table{i}")));
            exp_tables.push(Table::new(format!("dtest{i}"), format!("dtable{i}")));
        }
        let old_router = new_table_router(true, &mut rules).expect("create table router");
        let new_router = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        for (input, expected) in input_tables.iter().zip(exp_tables.iter()) {
            // Go discards both errors here with `_`.
            let (old_sch, old_tbl) = old_router
                .route(&input.schema, &input.name)
                .expect("table router route");
            let (new_sch, new_tbl) = new_router
                .route(&input.schema, &input.name)
                .expect("regexp router route");
            assert_eq!(expected.schema, new_sch);
            assert_eq!(expected.name, new_tbl);
            assert_eq!(expected.schema, old_sch);
            assert_eq!(expected.name, old_tbl);
        }
    }

    fn reg_expr_rules() -> Vec<TableRule> {
        vec![
            schema_rule("~test.[0-9]+", "dtest1"),
            table_rule(
                "~test2?[animal|human]",
                "~tbl.*[cat|dog]+",
                "dtest2",
                "dtable2",
            ),
            table_rule("~test3_(schema)?.*", "test3_*", "dtest3", "dtable3"),
            table_rule(
                "test4s_*",
                "~testtable_[donot_delete]?",
                "dtest4",
                "dtable4",
            ),
        ]
    }

    /// Go: `TestRegExprRoute`.
    #[test]
    fn test_reg_expr_route() {
        let mut rules = reg_expr_rules();
        let input_table = [
            // match rule 1
            Table::new("tests100", "table1"),
            // match rule 2
            Table::new("test2animal", "tbl_animal_dogcat"),
            // match rule 3
            Table::new("test3_schema_meta", "test3_tail"),
            // match rule 4
            Table::new("test4s_2022", "testtable_donot_delete"),
            // match nothing
            Table::new("mytst5566", "gtable"),
        ];
        let expected_output = [
            Table::new("dtest1", "table1"),
            Table::new("dtest2", "dtable2"),
            Table::new("dtest3", "dtable3"),
            Table::new("dtest4", "dtable4"),
            Table::new("mytst5566", "gtable"),
        ];
        let new_router = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        for (input, expected) in input_table.iter().zip(expected_output.iter()) {
            let (new_schm, new_name) = new_router
                .route(&input.schema, &input.name)
                .expect("regexp router route");
            assert_eq!(expected.schema, new_schm);
            assert_eq!(expected.name, new_name);
        }
    }

    /// Go: `TestFetchExtendColumn`.
    #[test]
    fn test_fetch_extend_column() {
        let mut rules = vec![
            TableRule {
                schema_pattern: "schema*".to_owned(),
                table_pattern: "t*".to_owned(),
                target_schema: "test".to_owned(),
                target_table: "t".to_owned(),
                table_extractor: Some(TableExtractor::new("table_name", "table_(.*)")),
                schema_extractor: Some(SchemaExtractor::new("schema_name", "schema_(.*)")),
                source_extractor: Some(SourceExtractor::new("source_name", "source_(.*)_(.*)")),
            },
            TableRule {
                schema_pattern: "~s?chema.*".to_owned(),
                target_schema: "test".to_owned(),
                target_table: "t2".to_owned(),
                schema_extractor: Some(SchemaExtractor::new("schema_name", "(.*)")),
                source_extractor: Some(SourceExtractor::new("source_name", "(.*)")),
                ..TableRule::default()
            },
        ];
        let r = new_reg_expr_router(false, &mut rules).expect("create regexp router");

        // table level rules have highest priority
        let (extend_col, extend_val) = r
            .fetch_extend_column("schema_s1", "table_t1", "source_s1_s1")
            .expect("table rule matches");
        assert_eq!(extend_col, ["table_name", "schema_name", "source_name"]);
        // `source_(.*)_(.*)` has two capture groups, concatenated with no
        // separator (quirk 11).
        assert_eq!(extend_val, ["t1", "s1", "s1s1"]);

        // only schema rules
        let (extend_col2, extend_val2) = r
            .fetch_extend_column("schema_s2", "a_table_t2", "source_s2")
            .expect("schema rule matches");
        assert_eq!(extend_col2, ["schema_name", "source_name"]);
        assert_eq!(extend_val2, ["schema_s2", "source_s2"]);
    }

    /// Go: `TestAllRule`.
    #[test]
    fn test_all_rule() {
        let mut rules = reg_expr_rules();
        let r = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        let (schema_rules, table_rules) = r.all_rules();
        assert_eq!(1, schema_rules.len());
        assert_eq!(3, table_rules.len());
        assert_eq!(rules[0].schema_pattern, schema_rules[0].schema_pattern);
        for i in 0..3 {
            assert_eq!(rules[i + 1].schema_pattern, table_rules[i].schema_pattern);
            assert_eq!(rules[i + 1].table_pattern, table_rules[i].table_pattern);
        }
    }

    /// Go: `TestDupMatch`.
    #[test]
    fn test_dup_match() {
        let mut rules = vec![
            table_rule("~test[0-9]+.*", "~.*", "dtest1", ""),
            table_rule("~test2?[a|b]", "~tbl2", "dtest2", "dtable2"),
            schema_rule("mytest*", "mytest"),
            schema_rule("~mytest(_meta)?_schema", "test"),
        ];
        let input_tables = [
            // match rule1 and rule2
            Table::new("test2a", "tbl2"),
            // match rule3 and rule4
            Table::new("mytest_meta_schema", ""),
        ];
        let r = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        for input in &input_tables {
            let err = r
                .route(&input.schema, &input.name)
                .expect_err("ambiguous match");
            assert!(
                err.to_string().contains("matches more than one rule"),
                "unexpected error: {err}"
            );
        }
    }

    /// Written: upstream never checks that a case-insensitive router leaves the
    /// target names alone while lowercasing the patterns (quirk 2).
    #[test]
    fn test_case_insensitive_keeps_target_case_written() {
        let mut rules = vec![table_rule("TeSt*", "TaBle*", "DTest", "DTable")];
        let r = new_reg_expr_router(false, &mut rules).expect("create regexp router");
        // The caller's rule was rewritten in place (quirk 1), patterns only.
        assert_eq!("test*", rules[0].schema_pattern);
        assert_eq!("table*", rules[0].table_pattern);
        assert_eq!("DTest", rules[0].target_schema);
        assert_eq!("DTable", rules[0].target_table);

        let (schema, table) = r.route("TEST1", "TABLE1").expect("route");
        assert_eq!("DTest", schema);
        assert_eq!("DTable", table);
    }

    /// Written: upstream never routes with an empty table name against a table
    /// rule, which skips the table tier entirely (quirk 6).
    #[test]
    fn test_route_empty_table_uses_schema_tier_written() {
        let mut rules = vec![table_rule("test*", "table*", "dtest", "dtable")];
        let r = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        // The table rule matched, but with an empty table name only the schema
        // tier is consulted — and it is empty, so nothing is rewritten.
        let (schema, table) = r.route("test1", "").expect("route");
        assert_eq!("test1", schema);
        assert_eq!("", table);
    }

    /// Written: upstream never observes `extractVal` on a non-matching regexp,
    /// on a regexp with no capture group, or with several groups.
    #[test]
    fn test_extract_val_written() {
        let no_match = TableExtractor::new("c", "nope_(.*)");
        assert_eq!("", extract_val("table_1", Extractor::Table(&no_match)));

        // Group 0 is skipped, so a regexp with no capture group yields "".
        let no_group = SchemaExtractor::new("c", "schema.*");
        assert_eq!("", extract_val("schema_1", Extractor::Schema(&no_group)));

        // Several groups concatenate with no separator (quirk 11).
        let multi = SourceExtractor::new("c", "(a+)-(b+)-(c+)");
        assert_eq!("aabbbc", extract_val("aa-bbb-c", Extractor::Source(&multi)));
    }

    /// Written: upstream never observes an unmatched table, where Go returns
    /// two nil slices — `None` here.
    #[test]
    fn test_fetch_extend_column_no_match_written() {
        let mut rules = vec![schema_rule("test*", "dtest")];
        let r = new_reg_expr_router(true, &mut rules).expect("create regexp router");
        // Matches, but the rule has no extractor at all.
        assert_eq!(None, r.fetch_extend_column("test1", "t1", "src"));
        // Matches nothing.
        assert_eq!(None, r.fetch_extend_column("other", "t1", "src"));
    }
}
