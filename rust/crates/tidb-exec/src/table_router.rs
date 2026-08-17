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

//! Routes a source schema/table to a target schema/table.
//!
//! Transcreation of the whole Go package `pkg/util/table-router`
//! (`package router`, file `router.go`).
//!
//! A [`TableRouter`] holds [`TableRule`]s in the wildcard trie of
//! [`crate::table_rule_selector`]; the rule patterns use that package's syntax
//! (`*`, `?`, `[...]`). A rule with an empty `table_pattern` is a *schema level*
//! rule, one with a non-empty `table_pattern` is a *table level* rule, and table
//! level rules win. A rule may additionally carry up to three *extractors*,
//! which pull capture groups out of the table, schema or source name into an
//! extra column.
//!
//! ## Reproduced Go quirks
//!
//! These are visible through the Go package's own API and are reproduced, not
//! fixed. Each is also marked at its site.
//!
//! 1. `(*TableRule).Valid` compiles each extractor's regexp *before* checking
//!    that its target column is non-empty, so a rule that is wrong in both ways
//!    reports the regexp error.
//! 2. `(*Table).AddRule`/`UpdateRule` run `Valid` on the rule *as written* and
//!    only then `ToLower` it, so the patterns are validated in their original
//!    case and rewritten in place afterwards. `RemoveRule` skips `Valid`
//!    entirely but still lowercases.
//! 3. `(*Table).Route` lowercases its arguments for a case-insensitive router,
//!    but `(*Table).FetchExtendColumn` does not — the same names can therefore
//!    route and extract against different rules.
//! 4. `(*Table).Route` returns an error when the trie hands back something that
//!    is not a rule, while `FetchExtendColumn` silently returns nothing for the
//!    very same trie contents.
//! 5. `(*Table).Route`'s "too many rules" check uses the *original* `table`
//!    argument for the `len(table) == 0` test but the *lowercased* one for the
//!    trie lookup.
//! 6. `(*TableRule).extractVal` drops capture group 0 and concatenates every
//!    remaining group with no separator, so `source_(.*)_(.*)` over
//!    `source_s1_s1` yields `s1s1` rather than a pair.
//! 7. `(*Table).FetchExtendColumn` takes `schemaRules[0]`/`tableRules[0]` with
//!    no "more than one" check, unlike `Route`, so an ambiguous match silently
//!    picks whichever rule the trie happened to yield first.

use std::fmt;

use regex::Regex;

use crate::table_rule_selector::{
    new_trie_selector, InsertType, Selector, SelectorError, TrieSelector,
};

/// Errors returned by the table router.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RouterError {
    /// Go: `errors.New("schema pattern of table route rule should not be empty")`.
    EmptySchemaPattern,
    /// Go: `errors.New("target schema of table route rule should not be empty")`.
    EmptyTargetSchema,
    /// Go: `fmt.Errorf("table extractor table regexp illegal %s", tableRe)`.
    TableRegexpIllegal(String),
    /// Go: `errors.New("table extractor target column cannot be empty")`.
    EmptyTableTargetColumn,
    /// Go: `fmt.Errorf("schema extractor schema regexp illegal %s", schemaRe)`.
    SchemaRegexpIllegal(String),
    /// Go: `errors.New("schema extractor target column cannot be empty")`.
    EmptySchemaTargetColumn,
    /// Go: `fmt.Errorf("source extractor source regexp illegal %s", sourceRe)`.
    SourceRegexpIllegal(String),
    /// Go: `errors.New("source extractor target column cannot be empty")`.
    EmptySourceTargetColumn,
    /// Go: `errors.NotValidf("table route rule %+v", rules[i])`.
    NotValid(String),
    /// Go: `errors.NotSupportedf(...)`. The Go format strings deliberately end
    /// in `It's`, so the rendered message reads `... It's not supported`.
    NotSupported(String),
    /// Go: an error surfaced from the embedded `selector.Selector`.
    Selector(SelectorError),
    /// Go: `errors.Annotatef(err, ...)`.
    Annotated(String, Box<RouterError>),
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterError::EmptySchemaPattern => {
                write!(f, "schema pattern of table route rule should not be empty")
            }
            RouterError::EmptyTargetSchema => {
                write!(f, "target schema of table route rule should not be empty")
            }
            RouterError::TableRegexpIllegal(re) => {
                write!(f, "table extractor table regexp illegal {re}")
            }
            RouterError::EmptyTableTargetColumn => {
                write!(f, "table extractor target column cannot be empty")
            }
            RouterError::SchemaRegexpIllegal(re) => {
                write!(f, "schema extractor schema regexp illegal {re}")
            }
            RouterError::EmptySchemaTargetColumn => {
                write!(f, "schema extractor target column cannot be empty")
            }
            RouterError::SourceRegexpIllegal(re) => {
                write!(f, "source extractor source regexp illegal {re}")
            }
            RouterError::EmptySourceTargetColumn => {
                write!(f, "source extractor target column cannot be empty")
            }
            RouterError::NotValid(what) => write!(f, "{what} not valid"),
            RouterError::NotSupported(what) => write!(f, "{what} not supported"),
            RouterError::Selector(err) => write!(f, "{err}"),
            RouterError::Annotated(ctx, inner) => write!(f, "{ctx}: {inner}"),
        }
    }
}

impl std::error::Error for RouterError {}

impl From<SelectorError> for RouterError {
    fn from(err: SelectorError) -> RouterError {
        RouterError::Selector(err)
    }
}

impl RouterError {
    fn annotate(self, ctx: impl Into<String>) -> RouterError {
        RouterError::Annotated(ctx.into(), Box::new(self))
    }
}

/// Extracts the table name into a column.
///
/// Go: `type TableExtractor struct`.
#[derive(Clone, Debug, Default)]
pub struct TableExtractor {
    /// Go: the unexported `regexp *regexp.Regexp` field, filled in by
    /// [`TableRule::valid`]. `None` before the rule has been validated.
    regexp: Option<Regex>,
    /// Go: `TargetColumn`.
    pub target_column: String,
    /// Go: `TableRegexp`.
    pub table_regexp: String,
}

/// Extracts the schema name into a column.
///
/// Go: `type SchemaExtractor struct`.
#[derive(Clone, Debug, Default)]
pub struct SchemaExtractor {
    /// See [`TableExtractor::regexp`].
    regexp: Option<Regex>,
    /// Go: `TargetColumn`.
    pub target_column: String,
    /// Go: `SchemaRegexp`.
    pub schema_regexp: String,
}

/// Extracts the source name into a column.
///
/// Go: `type SourceExtractor struct`.
#[derive(Clone, Debug, Default)]
pub struct SourceExtractor {
    /// See [`TableExtractor::regexp`].
    regexp: Option<Regex>,
    /// Go: `TargetColumn`.
    pub target_column: String,
    /// Go: `SourceRegexp`.
    pub source_regexp: String,
}

/// Which of the three extractors a value is being pulled out of.
///
/// boundary: stands for the `ext any` parameter of `(*TableRule).extractVal`
/// together with its `switch e := ext.(type)` over exactly
/// `*TableExtractor`, `*SchemaExtractor` and `*SourceExtractor`. That switch has
/// **no** `default:` arm, so any other dynamic type silently produces the empty
/// string. Modelling the parameter as a closed three-variant enum removes the
/// silent path entirely rather than adding a catch-all, and every Go call site
/// already passes one of the three.
#[derive(Clone, Copy, Debug)]
enum Extractor<'a> {
    Table(&'a TableExtractor),
    Schema(&'a SchemaExtractor),
    Source(&'a SourceExtractor),
}

/// A rule to route a schema/table to a target schema/table. Pattern format
/// refers to [`crate::table_rule_selector`].
///
/// Go: `type TableRule struct`.
///
/// boundary: the `json`/`toml`/`yaml` struct tags have no counterpart; this port
/// has no deserialization surface for them.
#[derive(Clone, Debug, Default)]
pub struct TableRule {
    /// Go: `TableExtractor`.
    pub table_extractor: Option<TableExtractor>,
    /// Go: `SchemaExtractor`.
    pub schema_extractor: Option<SchemaExtractor>,
    /// Go: `SourceExtractor`.
    pub source_extractor: Option<SourceExtractor>,
    /// Go: `SchemaPattern`.
    pub schema_pattern: String,
    /// Go: `TablePattern`.
    pub table_pattern: String,
    /// Go: `TargetSchema`.
    pub target_schema: String,
    /// Go: `TargetTable`.
    pub target_table: String,
}

impl fmt::Display for TableRule {
    /// Stands for Go's `%+v` rendering of a `*TableRule`, used inside several
    /// error messages.
    ///
    /// boundary: Go prints the three extractor pointers as hex addresses, which
    /// cannot be reproduced and carry no information; they render as `<nil>` or
    /// as the pointed-to struct here. No caller parses these messages.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "&{{")?;
        match &self.table_extractor {
            Some(e) => write!(
                f,
                "TableExtractor:&{{TargetColumn:{} TableRegexp:{}}}",
                e.target_column, e.table_regexp
            )?,
            None => write!(f, "TableExtractor:<nil>")?,
        }
        match &self.schema_extractor {
            Some(e) => write!(
                f,
                " SchemaExtractor:&{{TargetColumn:{} SchemaRegexp:{}}}",
                e.target_column, e.schema_regexp
            )?,
            None => write!(f, " SchemaExtractor:<nil>")?,
        }
        match &self.source_extractor {
            Some(e) => write!(
                f,
                " SourceExtractor:&{{TargetColumn:{} SourceRegexp:{}}}",
                e.target_column, e.source_regexp
            )?,
            None => write!(f, " SourceExtractor:<nil>")?,
        }
        write!(
            f,
            " SchemaPattern:{} TablePattern:{} TargetSchema:{} TargetTable:{}}}",
            self.schema_pattern, self.table_pattern, self.target_schema, self.target_table
        )
    }
}

impl TableRule {
    /// Checks the validity of the rule and compiles its extractor regexps into
    /// the rule.
    ///
    /// Go: `func (t *TableRule) Valid() error`. It mutates the receiver, so this
    /// takes `&mut self`.
    ///
    /// Quirk 1: the regexp of each extractor is compiled before its target
    /// column is checked, so an extractor that is wrong in both ways reports the
    /// regexp error.
    ///
    /// boundary: Go compiles with `regexp` (RE2). The `regex` crate is the same
    /// automaton family; it rejects lookaround and backreferences just as RE2
    /// does, and differs only in that `\d`/`\w`/`\s` are Unicode-aware rather
    /// than ASCII-only.
    pub fn valid(&mut self) -> Result<(), RouterError> {
        if self.schema_pattern.is_empty() {
            return Err(RouterError::EmptySchemaPattern);
        }

        if self.target_schema.is_empty() {
            return Err(RouterError::EmptyTargetSchema);
        }

        if let Some(ext) = self.table_extractor.as_mut() {
            let re = Regex::new(&ext.table_regexp)
                .map_err(|_| RouterError::TableRegexpIllegal(ext.table_regexp.clone()))?;
            if ext.target_column.is_empty() {
                return Err(RouterError::EmptyTableTargetColumn);
            }
            ext.regexp = Some(re);
        }
        if let Some(ext) = self.schema_extractor.as_mut() {
            let re = Regex::new(&ext.schema_regexp)
                .map_err(|_| RouterError::SchemaRegexpIllegal(ext.schema_regexp.clone()))?;
            if ext.target_column.is_empty() {
                return Err(RouterError::EmptySchemaTargetColumn);
            }
            ext.regexp = Some(re);
        }
        if let Some(ext) = self.source_extractor.as_mut() {
            let re = Regex::new(&ext.source_regexp)
                .map_err(|_| RouterError::SourceRegexpIllegal(ext.source_regexp.clone()))?;
            if ext.target_column.is_empty() {
                return Err(RouterError::EmptySourceTargetColumn);
            }
            ext.regexp = Some(re);
        }
        Ok(())
    }

    /// Converts the schema and table patterns to lower case.
    ///
    /// Go: `func (t *TableRule) ToLower()`. The Go comment reads "covert
    /// schema/table parttern to lower case"; the typos are Go's.
    ///
    /// boundary: Go uses `strings.ToLower` (Unicode simple case folding); Rust's
    /// `str::to_lowercase` applies the full mapping, which differs only for
    /// characters whose lowercase form grows longer.
    pub fn to_lower(&mut self) {
        self.schema_pattern = self.schema_pattern.to_lowercase();
        self.table_pattern = self.table_pattern.to_lowercase();
    }

    /// Matches a value via the extractor's regexp.
    ///
    /// Go: `func (*TableRule) extractVal(s string, ext any) string`. The
    /// receiver is unused in Go, so this is a free function here.
    ///
    /// Quirk 6: capture group 0 (the whole match) is skipped and every other
    /// group is concatenated without a separator.
    fn extract_val(s: &str, ext: Extractor<'_>) -> String {
        let re = match ext {
            Extractor::Table(e) => e.regexp.as_ref(),
            Extractor::Schema(e) => e.regexp.as_ref(),
            Extractor::Source(e) => e.regexp.as_ref(),
        };
        // boundary: Go dereferences the nil regexp of an unvalidated rule and
        // panics. Panicking here keeps that contract instead of inventing an
        // empty extraction, which would silently write a wrong column value.
        let re = re.expect("extractor regexp is compiled by TableRule::valid");

        // Go: `regexp.FindStringSubmatch` returns nil when there is no match, so
        // the loop below never runs and the result is the empty string.
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
}

/// What the router stores in the trie.
///
/// boundary: Go's selector stores `any`, and both `Route` and
/// `FetchExtendColumn` recover the rule with `rules[i].(*TableRule)` and handle
/// the failure. The upstream test reaches that path on purpose by inserting a
/// bare `string` through the promoted `Selector.Insert`, so the failure is
/// observable and this enum keeps it representable — [`RouterRule::Other`]
/// carries Go's `%+v` rendering of the foreign value, exactly what the error
/// message needs.
#[derive(Clone, Debug)]
pub enum RouterRule {
    /// A real routing rule.
    ///
    /// Boxed only to keep the enum small: `TableRule` carries three optional
    /// extractors while [`RouterRule::Other`] is a single `String`.
    Table(Box<TableRule>),
    /// Anything else that was put into the trie, rendered as Go's `%+v` would.
    Other(String),
}

/// Routes a schema/table to a target schema/table by the given route rules.
///
/// Go: `type Table struct` embedding `selector.Selector`.
///
/// boundary: renamed from Go's `Table` to `TableRouter` (after Go's constructor
/// `NewTableRouter`) so that it does not read as a table value; `filter::Table`
/// in this crate is the table *name* type.
pub struct TableRouter {
    selector: TrieSelector<RouterRule>,
    case_sensitive: bool,
}

impl fmt::Debug for TableRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `TrieSelector` guards its arena with an `RwLock` and has no `Debug`.
        f.debug_struct("TableRouter")
            .field("case_sensitive", &self.case_sensitive)
            .finish_non_exhaustive()
    }
}

/// Returns a table router.
///
/// Go: `func NewTableRouter(caseSensitive bool, rules []*TableRule) (*Table, error)`.
///
/// Quirk 2: each rule is validated and then lowercased *in place*, so the
/// caller's rules come back rewritten. `rules` is `&mut` to keep that visible.
pub fn new_table_router(
    case_sensitive: bool,
    rules: &mut [TableRule],
) -> Result<TableRouter, RouterError> {
    let r = TableRouter {
        selector: new_trie_selector(),
        case_sensitive,
    };

    for rule in rules.iter_mut() {
        let ctx = format!("initial rule {rule} in table router");
        r.add_rule(rule).map_err(|err| err.annotate(ctx))?;
    }

    Ok(r)
}

impl TableRouter {
    /// The underlying trie.
    ///
    /// Go embeds `selector.Selector` in `Table`, promoting `Insert`, `Match`,
    /// `Remove` and `AllRules` onto the router. They are reachable through this
    /// accessor instead; the upstream test uses the promoted `Insert` to put a
    /// non-rule value into the trie.
    pub fn selector(&self) -> &TrieSelector<RouterRule> {
        &self.selector
    }

    /// Whether the router matches case-sensitively. Go: the unexported
    /// `caseSensitive` field.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Adds a rule into the table router.
    ///
    /// Go: `func (r *Table) AddRule(rule *TableRule) error`.
    ///
    /// boundary: Go stores the caller's `*TableRule` pointer, so later edits to
    /// the caller's rule are seen by the router (and by rules already handed
    /// back from the match cache). This port stores a clone taken at insert
    /// time; a caller who edits a rule must re-insert it with
    /// [`TableRouter::update_rule`], which is what every in-tree caller and the
    /// upstream test already do. Cloning is the safe direction: a routing
    /// decision cannot change under a reader that never asked for it.
    pub fn add_rule(&self, rule: &mut TableRule) -> Result<(), RouterError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }

        self.selector
            .insert(
                &rule.schema_pattern,
                &rule.table_pattern,
                Some(RouterRule::Table(Box::new(rule.clone()))),
                InsertType::Insert,
            )
            .map_err(|err| {
                RouterError::from(err).annotate(format!("add rule {rule} into table router"))
            })?;

        Ok(())
    }

    /// Updates a rule.
    ///
    /// Go: `func (r *Table) UpdateRule(rule *TableRule) error`.
    pub fn update_rule(&self, rule: &mut TableRule) -> Result<(), RouterError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }

        self.selector
            .insert(
                &rule.schema_pattern,
                &rule.table_pattern,
                Some(RouterRule::Table(Box::new(rule.clone()))),
                InsertType::Replace,
            )
            .map_err(|err| {
                RouterError::from(err).annotate(format!("update rule {rule} into table router"))
            })?;

        Ok(())
    }

    /// Removes a rule from the table router.
    ///
    /// Go: `func (r *Table) RemoveRule(rule *TableRule) error`.
    ///
    /// Quirk 2: unlike add/update this never calls `Valid`, so an invalid rule
    /// can still be used to remove — only its patterns are read.
    pub fn remove_rule(&self, rule: &mut TableRule) -> Result<(), RouterError> {
        if !self.case_sensitive {
            rule.to_lower();
        }

        self.selector
            .remove(&rule.schema_pattern, &rule.table_pattern)
            .map_err(|err| {
                RouterError::from(err).annotate(format!("remove rule {rule} from table router"))
            })?;

        Ok(())
    }

    /// Routes a schema/table to a target schema/table. Routing one schema/table
    /// to multiple targets is not supported.
    ///
    /// Go: `func (r *Table) Route(schema, table string) (string, string, error)`.
    ///
    /// Quirk 5: the `len(table) == 0` test reads the caller's `table`, while the
    /// trie is probed with the lowercased one.
    pub fn route(&self, schema: &str, table: &str) -> Result<(String, String), RouterError> {
        let (schema_l, table_l) = if self.case_sensitive {
            (schema.to_string(), table.to_string())
        } else {
            (schema.to_lowercase(), table.to_lowercase())
        };

        let rules = self.selector.match_rules(&schema_l, &table_l);
        let mut schema_rules: Vec<&TableRule> = Vec::new();
        let mut table_rules: Vec<&TableRule> = Vec::new();
        // classify rules into schema level rules and table level;
        // table level rules have highest priority
        let matched = rules.unwrap_or_default();
        for matched_rule in &matched {
            let rule = match matched_rule {
                RouterRule::Table(rule) => rule.as_ref(),
                // Quirk 4: `Route` errors out here; `FetchExtendColumn` returns
                // nothing for the same trie contents.
                RouterRule::Other(rendered) => {
                    return Err(RouterError::NotValid(format!(
                        "table route rule {rendered}"
                    )));
                }
            };

            if rule.table_pattern.is_empty() {
                schema_rules.push(rule);
            } else {
                table_rules.push(rule);
            }
        }

        let mut target_schema = String::new();
        let mut target_table = String::new();

        if table.is_empty() || table_rules.is_empty() {
            if schema_rules.len() > 1 {
                return Err(RouterError::NotSupported(format!(
                    "`{}`.`{}` matches {} schema route rules which is more than one.\nThe first two rules are {}, {}.\nIt's",
                    schema,
                    table,
                    schema_rules.len(),
                    schema_rules[0],
                    schema_rules[1]
                )));
            }

            if schema_rules.len() == 1 {
                target_schema = schema_rules[0].target_schema.clone();
                target_table = schema_rules[0].target_table.clone();
            }
        } else {
            if table_rules.len() > 1 {
                return Err(RouterError::NotSupported(format!(
                    "`{}`.`{}` matches {} table route rules which is more than one.\nThe first two rules are {}, {}.\nIt's",
                    schema,
                    table,
                    table_rules.len(),
                    table_rules[0],
                    table_rules[1]
                )));
            }

            target_schema = table_rules[0].target_schema.clone();
            target_table = table_rules[0].target_table.clone();
        }

        if target_schema.is_empty() {
            target_schema = schema.to_string();
        }

        if target_table.is_empty() {
            target_table = table.to_string();
        }

        Ok((target_schema, target_table))
    }

    /// Gets the extract rule and returns the extracted columns and values.
    ///
    /// Go: `func (r *Table) FetchExtendColumn(schema, table, source string) (cols []string, vals []string)`.
    ///
    /// Quirk 3: no lowercasing happens here even for a case-insensitive router.
    /// Quirk 4: a non-rule in the trie yields nothing instead of an error.
    /// Quirk 7: an ambiguous match silently takes the first rule.
    ///
    /// Go returns two `nil` slices when there is nothing to extract, and the two
    /// slices are only ever appended to together. `None` therefore stands for
    /// Go's `(nil, nil)`, which a caller can tell apart from two empty slices.
    pub fn fetch_extend_column(
        &self,
        schema: &str,
        table: &str,
        source: &str,
    ) -> Option<(Vec<String>, Vec<String>)> {
        let rules = self.selector.match_rules(schema, table);
        let mut schema_rules: Vec<&TableRule> = Vec::new();
        let mut table_rules: Vec<&TableRule> = Vec::new();
        let matched = rules.unwrap_or_default();
        for matched_rule in &matched {
            let rule = match matched_rule {
                RouterRule::Table(rule) => rule.as_ref(),
                RouterRule::Other(_) => return None,
            };
            if rule.table_pattern.is_empty() {
                schema_rules.push(rule);
            } else {
                table_rules.push(rule);
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
            vals.push(TableRule::extract_val(table, Extractor::Table(ext)));
        }

        if let Some(ext) = rule.schema_extractor.as_ref() {
            cols.push(ext.target_column.clone());
            vals.push(TableRule::extract_val(schema, Extractor::Schema(ext)));
        }

        if let Some(ext) = rule.source_extractor.as_ref() {
            cols.push(ext.target_column.clone());
            vals.push(TableRule::extract_val(source, Extractor::Source(ext)));
        }

        // Go's named results start as nil slices and are only appended to
        // together, so "no extractor at all" is (nil, nil).
        if cols.is_empty() {
            return None;
        }
        Some((cols, vals))
    }
}

/// Transcreated from `pkg/util/table-router/router_test.go` (`TestRoute`,
/// `TestCaseSensitive`, `TestFetchExtendColumn`), the only upstream coverage of
/// this package. Two extra tests are marked `_written`: upstream never exercises
/// `(*TableRule).Valid`'s extractor branches on their own, nor `ToLower`.
#[cfg(test)]
mod tests {
    use super::*;

    fn rule(
        schema_pattern: &str,
        table_pattern: &str,
        target_schema: &str,
        target_table: &str,
    ) -> TableRule {
        TableRule {
            schema_pattern: schema_pattern.into(),
            table_pattern: table_pattern.into(),
            target_schema: target_schema.into(),
            target_table: target_table.into(),
            ..TableRule::default()
        }
    }

    /// Go: `TestRoute`.
    #[test]
    fn test_route() {
        let mut rules = vec![
            rule("Test_1_*", "abc*", "t1", "abc"),
            rule("test_1_*", "test*", "t2", "test"),
            rule("test_1_*", "", "test", ""),
            rule("test_2_*", "abc*", "t1", "abc"),
            rule("test_2_*", "test*", "t2", "test"),
        ];

        let mut cases: Vec<[String; 4]> = [
            ["test_1_a", "abc1", "t1", "abc"],
            ["test_2_a", "abc2", "t1", "abc"],
            ["test_1_a", "test1", "t2", "test"],
            ["test_2_a", "test2", "t2", "test"],
            ["test_1_a", "xyz", "test", "xyz"],
        ]
        .iter()
        .map(|cs| cs.map(str::to_string))
        .collect();

        // initial table router
        let router = new_table_router(false, &mut rules).expect("router builds");

        // insert duplicate rules
        for rule in rules.iter_mut() {
            assert!(router.add_rule(rule).is_err());
        }
        for cs in &cases {
            let (schema, table) = router.route(&cs[0], &cs[1]).expect("route succeeds");
            assert_eq!(cs[2], schema);
            assert_eq!(cs[3], table);
        }

        // update rules
        rules[0].target_table = "xxx".into();
        cases[0][3] = "xxx".into();
        router.update_rule(&mut rules[0]).expect("update succeeds");
        for cs in &cases {
            let (schema, table) = router.route(&cs[0], &cs[1]).expect("route succeeds");
            assert_eq!(cs[2], schema);
            assert_eq!(cs[3], table);
        }

        // remove rule
        router.remove_rule(&mut rules[0]).expect("remove succeeds");
        // remove not existing rule
        assert!(router.remove_rule(&mut rules[0]).is_err());
        let (schema, table) = router
            .route(&cases[0][0], &cases[0][1])
            .expect("route succeeds");
        assert_eq!("test", schema);
        assert_eq!("abc1", table);
        // Go deletes the removed rule from its local slices here
        // (`rules = rules[1:]`, `cases = cases[1:]`); neither is read again.

        // mismatched
        let (schema, _) = router.route("test_3_a", "").expect("route succeeds");
        assert_eq!("test_3_a", schema);
        // test multiple schema level rules
        router
            .add_rule(&mut rule("test_*", "", "error", ""))
            .expect("add succeeds");
        assert!(router.route("test_1_a", "").is_err());
        // test multiple table level rules
        router
            .add_rule(&mut rule("test_1_*", "tes*", "error", "error"))
            .expect("add succeeds");
        assert!(router.route("test_1_a", "test").is_err());
        // invalid rule: a value in the trie that is not a `TableRule`
        router
            .selector()
            .insert(
                "test_1_*",
                "abc*",
                Some(RouterRule::Other("error".into())),
                InsertType::Insert,
            )
            .expect("raw insert succeeds");
        assert!(router.route("test_1_a", "abc").is_err());

        // Add/Update invalid table route rule
        let mut invalid_rule = TableRule {
            schema_pattern: "test*".into(),
            table_pattern: "abc*".into(),
            ..TableRule::default()
        };
        assert!(router.add_rule(&mut invalid_rule).is_err());
        assert!(router.update_rule(&mut invalid_rule).is_err());
    }

    /// Go: `TestCaseSensitive`. Renamed to keep it distinct from the filter
    /// module's test of the same upstream name.
    #[test]
    fn test_case_sensitive_router() {
        // we test case insensitive in `test_route`
        let mut rules = vec![
            rule("Test_1_*", "abc*", "t1", "abc"),
            rule("test_1_*", "test*", "t2", "test"),
            rule("test_1_*", "", "test", ""),
            rule("test_2_*", "abc*", "t1", "abc"),
            rule("test_2_*", "test*", "t2", "test"),
        ];

        let cases = [
            ["test_1_a", "abc1", "test", "abc1"],
            ["test_2_a", "abc2", "t1", "abc"],
            ["test_1_a", "test1", "t2", "test"],
            ["test_2_a", "test2", "t2", "test"],
            ["test_1_a", "xyz", "test", "xyz"],
        ];

        // initial table router
        let router = new_table_router(true, &mut rules).expect("router builds");

        // insert duplicate rules
        for rule in rules.iter_mut() {
            assert!(router.add_rule(rule).is_err());
        }
        for cs in &cases {
            let (schema, table) = router.route(cs[0], cs[1]).expect("route succeeds");
            assert_eq!(cs[2], schema);
            assert_eq!(cs[3], table);
        }
    }

    /// Go: `TestFetchExtendColumn`.
    #[test]
    fn test_fetch_extend_column() {
        let mut rules = vec![
            TableRule {
                schema_pattern: "schema*".into(),
                table_pattern: "t*".into(),
                target_schema: "test".into(),
                target_table: "t".into(),
                table_extractor: Some(TableExtractor {
                    target_column: "table_name".into(),
                    table_regexp: "table_(.*)".into(),
                    regexp: None,
                }),
                schema_extractor: Some(SchemaExtractor {
                    target_column: "schema_name".into(),
                    schema_regexp: "schema_(.*)".into(),
                    regexp: None,
                }),
                source_extractor: Some(SourceExtractor {
                    target_column: "source_name".into(),
                    source_regexp: "source_(.*)_(.*)".into(),
                    regexp: None,
                }),
            },
            TableRule {
                schema_pattern: "schema*".into(),
                target_schema: "test".into(),
                target_table: "t2".into(),
                schema_extractor: Some(SchemaExtractor {
                    target_column: "schema_name".into(),
                    schema_regexp: "(.*)".into(),
                    regexp: None,
                }),
                source_extractor: Some(SourceExtractor {
                    target_column: "source_name".into(),
                    source_regexp: "(.*)".into(),
                    regexp: None,
                }),
                ..TableRule::default()
            },
        ];
        let r = new_table_router(false, &mut rules).expect("router builds");
        let expected: [Vec<&str>; 4] = [
            vec!["table_name", "schema_name", "source_name"],
            vec!["t1", "s1", "s1s1"],
            vec!["schema_name", "source_name"],
            vec!["schema_s2", "source_s2"],
        ];

        // table level rules have highest priority
        let (extend_col, extend_val) = r
            .fetch_extend_column("schema_s1", "table_t1", "source_s1_s1")
            .expect("extractors present");
        assert_eq!(expected[0], extend_col);
        assert_eq!(expected[1], extend_val);

        // only schema rules
        let (extend_col2, extend_val2) = r
            .fetch_extend_column("schema_s2", "a_table_t2", "source_s2")
            .expect("extractors present");
        assert_eq!(expected[2], extend_col2);
        assert_eq!(expected[3], extend_val2);
    }

    /// Upstream never calls `(*TableRule).Valid` directly, so its eight failure
    /// modes and quirk 1's ordering are only reachable through `AddRule` on a
    /// rule that is wrong in one specific way. Written here.
    #[test]
    fn test_valid_written() {
        let mut r = TableRule::default();
        assert_eq!(r.valid(), Err(RouterError::EmptySchemaPattern));

        let mut r = rule("s*", "", "", "");
        assert_eq!(r.valid(), Err(RouterError::EmptyTargetSchema));

        // Quirk 1: the regexp is compiled before the target column is checked,
        // so a rule that is wrong in both ways reports the regexp error.
        let mut r = rule("s*", "", "target", "");
        r.table_extractor = Some(TableExtractor {
            target_column: String::new(),
            table_regexp: "(?=x)".into(),
            regexp: None,
        });
        assert_eq!(
            r.valid(),
            Err(RouterError::TableRegexpIllegal("(?=x)".into()))
        );

        r.table_extractor = Some(TableExtractor {
            target_column: String::new(),
            table_regexp: "t_(.*)".into(),
            regexp: None,
        });
        assert_eq!(r.valid(), Err(RouterError::EmptyTableTargetColumn));

        let mut r = rule("s*", "", "target", "");
        r.schema_extractor = Some(SchemaExtractor {
            target_column: String::new(),
            schema_regexp: "(?=x)".into(),
            regexp: None,
        });
        assert_eq!(
            r.valid(),
            Err(RouterError::SchemaRegexpIllegal("(?=x)".into()))
        );
        r.schema_extractor
            .as_mut()
            .expect("set above")
            .schema_regexp = "s_(.*)".into();
        assert_eq!(r.valid(), Err(RouterError::EmptySchemaTargetColumn));

        let mut r = rule("s*", "", "target", "");
        r.source_extractor = Some(SourceExtractor {
            target_column: String::new(),
            source_regexp: "(?=x)".into(),
            regexp: None,
        });
        assert_eq!(
            r.valid(),
            Err(RouterError::SourceRegexpIllegal("(?=x)".into()))
        );
        r.source_extractor
            .as_mut()
            .expect("set above")
            .source_regexp = "s_(.*)".into();
        assert_eq!(r.valid(), Err(RouterError::EmptySourceTargetColumn));
    }

    /// Upstream exercises `ToLower` only through `AddRule`. Written here so the
    /// in-place mutation of quirk 2 is pinned directly, together with quirk 3's
    /// missing lowercasing in `fetch_extend_column`.
    #[test]
    fn test_to_lower_and_fetch_case_written() {
        let mut r = rule("Sch*", "Tbl*", "Target", "TargetTbl");
        r.to_lower();
        assert_eq!(r.schema_pattern, "sch*");
        assert_eq!(r.table_pattern, "tbl*");
        // Only the patterns are lowercased; the targets keep their case.
        assert_eq!(r.target_schema, "Target");
        assert_eq!(r.target_table, "TargetTbl");

        let mut rules = vec![TableRule {
            schema_pattern: "Schema*".into(),
            table_pattern: "T*".into(),
            target_schema: "test".into(),
            target_table: "t".into(),
            schema_extractor: Some(SchemaExtractor {
                target_column: "schema_name".into(),
                schema_regexp: "schema_(.*)".into(),
                regexp: None,
            }),
            ..TableRule::default()
        }];
        let router = new_table_router(false, &mut rules).expect("router builds");
        // Quirk 2: the caller's rule came back lowercased.
        assert_eq!(rules[0].schema_pattern, "schema*");
        // Quirk 3: `route` lowercases its arguments...
        assert_eq!(
            router.route("SCHEMA_A", "T1").expect("route succeeds"),
            ("test".to_string(), "t".to_string())
        );
        // ...but `fetch_extend_column` does not, so the same names miss.
        assert!(router
            .fetch_extend_column("SCHEMA_A", "T1", "src")
            .is_none());
        assert_eq!(
            router.fetch_extend_column("schema_a", "t1", "src"),
            Some((vec!["schema_name".to_string()], vec!["a".to_string()]))
        );
        assert!(!router.case_sensitive());
    }
}
