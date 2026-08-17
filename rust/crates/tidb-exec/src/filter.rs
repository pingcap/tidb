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

//! Table filter in the style of MySQL replication rules.
//!
//! Transcreation of the whole Go package `pkg/util/filter` (`package filter`,
//! files `filter.go` and `schema.go`).
//!
//! A [`Filter`] is built from a [`Rules`] set holding four lists — `do_dbs`,
//! `do_tables`, `ignore_dbs`, `ignore_tables`. Each entry is either
//!
//! * a *pattern*, matched by the wildcard trie of
//!   [`crate::table_rule_selector`] (`*`, `?`, `[...]`), or
//! * a *regular expression*, written with a leading `~`, matched by the
//!   `regex` crate.
//!
//! ## Safe direction
//!
//! A filter that wrongly *includes* a table the user meant to exclude leaks
//! data; a filter that wrongly *excludes* one only loses replication of that
//! table. So wherever this port has to choose, it chooses to exclude. Concretely
//! this shows up in two places, both marked with `// boundary:` below:
//!
//! * the compiled regex of a `TBL_RULE_ONLY_DB_PART` rule is carried *inside*
//!   the [`RuleKind`] variant, so a rule of that kind can never be missing its
//!   regex and silently be treated as "no table restriction";
//! * regex compilation failures abort [`Filter::new`] instead of being skipped,
//!   so no rule can be silently dropped.
//!
//! ## Reproduced Go quirks
//!
//! These are visible through the Go package's own API and are reproduced, not
//! fixed. Each is also marked at its site.
//!
//! 1. `Filter.filterOnSchemas` uses `if len(DoDBs) > 0 { ... } else if
//!    len(IgnoreDBs) > 0 { ... }`: once any `do_dbs` entry exists, `ignore_dbs`
//!    is never consulted at all. "Ignore beats do" holds only at table level.
//! 2. `Filter.filterOnTables` returns `true` as soon as a `do_tables` rule
//!    matches, *before* looking at `ignore_tables` — so at table level "do beats
//!    ignore", the opposite of the schema level.
//! 3. `Filter.matchTable` re-runs its last two trie lookups once per entry of
//!    the pattern list even though neither lookup depends on the entry. The
//!    result is that a `do_tables`/`ignore_tables` list which is merely
//!    *non-empty* enables the `TBL_RULE_ONLY_DB_PART` and `TBL_RULE_FULL`
//!    checks for every one of its entries.
//! 4. `Filter.matchTable`'s `dbIsRegex && !tblIsRegex` branch `continue`s when
//!    the schema regex does not match, skipping the shared lookups of quirk 3
//!    for that entry only.
//! 5. `Filter.initSchemaRule` and `Filter.initTableRule` insert table-level
//!    rules into the *schema* level of the trie: a `~db`/`tbl` rule stores
//!    `tbl` as a schema pattern, and a `db`/`~tbl` rule stores `db` as a schema
//!    pattern with no table part. Kind tagging is the only thing keeping these
//!    from cross-matching, which `Filter.matchDB` and `Filter.matchTable`
//!    re-check on every hit.
//! 6. `Filter.matchString` falls back to `pattern == t` when the pattern was
//!    never compiled, so a `~` rule silently degrades to string equality if its
//!    regex is absent from the pattern map.
//! 7. `New` lowercases the caller's `*Rules` in place when `caseSensitive` is
//!    false — a mutation of an argument that outlives the call.
//! 8. `Filter.ApplyOn` returns a `nil` slice when nothing matched while
//!    `Filter.Apply` returns an empty non-`nil` slice; the upstream tests assert
//!    on exactly that difference. Both are modelled as `Option<Vec<Table>>`.
//! 9. `Filter.ApplyOn` returns the *lowercased clones* it matched on, while
//!    `Filter.Apply` returns the *original* tables — so the same rule set gives
//!    differently-cased output depending on which entry point is used.
//! 10. The error strings mix singular and plural: `DoDB`/`IgnoreDB` for the
//!     schema lists but `DoTables`/`IgnoreTables` for the table lists.

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use regex::Regex;

use crate::table_rule_selector::{
    new_trie_selector, InsertType, Selector, SelectorError, TrieSelector,
};

// ---------------------------------------------------------------------------
// Types Go aliases from `pkg/util/table-filter`.
// ---------------------------------------------------------------------------

/// A qualified table name.
///
/// Go: `filter.Table` is a type alias for `tfilter.Table`
/// (`pkg/util/table-filter/compat.go`). `pkg/util/table-filter` is not ported
/// yet, so the two types it contributes to this package's API are defined here.
///
/// boundary: stands for Go `tfilter.Table` including its `String`, `Clone` and
/// `lessThan` methods. The serde tags (`toml:"db-name"`, `json:"db-name"`,
/// `yaml:"tbl-name"`, ...) are not reproduced; this port has no deserialization
/// surface for them.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct Table {
    /// Name of the schema (database) containing this table.
    pub schema: String,
    /// Unqualified table name.
    pub name: String,
}

impl Table {
    /// Builds a table from a schema and a table name.
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Table {
            schema: schema.into(),
            name: name.into(),
        }
    }

    /// Go: `(*Table).lessThan`. Go marks it `nolint:unused`; it is kept because
    /// the derived [`Ord`] must agree with it — schema first, then name.
    pub fn less_than(&self, u: &Table) -> bool {
        self.schema < u.schema || self.schema == u.schema && self.name < u.name
    }
}

impl fmt::Display for Table {
    /// Go: `(*Table).String`, which implements `fmt.Stringer`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.name.is_empty() {
            write!(f, "`{}`.`{}`", self.schema, self.name)
        } else {
            write!(f, "`{}`", self.schema)
        }
    }
}

/// A set of rules based on MySQL's replication table filter.
///
/// Go: `filter.Rules` is a type alias for `tfilter.MySQLReplicationRules`.
///
/// boundary: Go stores `[]*Table`, so two rules can alias one table value and
/// `ToLower` is observable through every alias. This port stores `Vec<Table>`
/// by value; no in-tree caller shares a `*Table` between rule lists.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Rules {
    /// Allowlist of tables.
    pub do_tables: Vec<Table>,
    /// Allowlist of schemas.
    pub do_dbs: Vec<String>,
    /// Blocklist of tables.
    pub ignore_tables: Vec<Table>,
    /// Blocklist of schemas.
    pub ignore_dbs: Vec<String>,
}

impl Rules {
    /// Converts all entries to lowercase.
    ///
    /// Go: `(*MySQLReplicationRules).ToLower`. Go's nil-receiver early return
    /// has no counterpart: a nil `*Rules` is `None` here and never reaches this
    /// method.
    ///
    /// boundary: Go uses `strings.ToLower`, which lowercases by Unicode simple
    /// case folding. Rust's `str::to_lowercase` applies full Unicode lowercase
    /// mapping, which differs only for characters whose lowercase form is
    /// longer than one character (e.g. `İ`). Schema and table identifiers in the
    /// upstream tests are ASCII.
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

// ---------------------------------------------------------------------------
// filter.go
// ---------------------------------------------------------------------------

/// Do or ignore something. Go: `type ActionType bool` with `Do = true` and
/// `Ignore = false`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActionType {
    /// Go: `Do ActionType = true`.
    Do,
    /// Go: `Ignore ActionType = false`.
    Ignore,
}

impl ActionType {
    /// Go: the `ActionType(...)` conversion in `Filter.Match`.
    fn from_bool(b: bool) -> ActionType {
        if b {
            ActionType::Do
        } else {
            ActionType::Ignore
        }
    }
}

/// Errors returned while building a [`Filter`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FilterError {
    /// Go: `errors.Errorf("DoDB rule's DB string cannot be empty")`.
    EmptyDoDb,
    /// Go: `errors.Errorf("DoTables rule's DB string or Table string cannot be empty")`.
    EmptyDoTables,
    /// Go: `errors.Errorf("IgnoreDB rule's DB string cannot be empty")`.
    EmptyIgnoreDb,
    /// Go: `errors.Errorf("IgnoreTables rule's DB string or Table string cannot be empty")`.
    EmptyIgnoreTables,
    /// Go: `errors.Trace(err)` around `regexp.Compile`. Carries the rendered
    /// compilation error.
    InvalidRegex(String),
    /// Go: the error returned by `Selector.Insert`.
    Selector(SelectorError),
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The Go messages are reproduced verbatim, including the singular
        // `DoDB`/`IgnoreDB` next to the plural `DoTables`/`IgnoreTables`.
        match self {
            FilterError::EmptyDoDb => write!(f, "DoDB rule's DB string cannot be empty"),
            FilterError::EmptyDoTables => {
                write!(
                    f,
                    "DoTables rule's DB string or Table string cannot be empty"
                )
            }
            FilterError::EmptyIgnoreDb => write!(f, "IgnoreDB rule's DB string cannot be empty"),
            FilterError::EmptyIgnoreTables => write!(
                f,
                "IgnoreTables rule's DB string or Table string cannot be empty"
            ),
            FilterError::InvalidRegex(msg) => write!(f, "{msg}"),
            FilterError::Selector(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FilterError {}

impl From<SelectorError> for FilterError {
    fn from(err: SelectorError) -> FilterError {
        FilterError::Selector(err)
    }
}

/// Which of the four rule shapes a trie entry stands for.
///
/// Go: the `dbRule`/`tblRuleFull`/`tblRuleOnlyDBPart`/`tblRuleOnlyTblPart`
/// `iota` block plus the `r *regexp.Regexp` field of `nodeEndRule`.
///
/// boundary: Go stores `kind int` and `r *regexp.Regexp` as independent fields,
/// so `r` is nil for three of the four kinds and `matchTable` must guard
/// `rule.kind == tblRuleOnlyDBPart` before dereferencing it. Carrying the regex
/// inside the variant makes the guard structural: a `TblRuleOnlyDbPart` always
/// has its table regex, and no other kind can have one. That is the safe
/// direction for a filter — a missing regex can never be read as "matches
/// everything".
#[derive(Clone, Debug)]
enum RuleKind {
    /// Go: `dbRule`. A whole-schema do/ignore rule.
    DbRule,
    /// Go: `tblRuleFull`. Both schema and table are trie patterns.
    TblRuleFull,
    /// Go: `tblRuleOnlyDBPart`. The schema is a trie pattern and the table is
    /// the carried regex.
    TblRuleOnlyDbPart(Regex),
    /// Go: `tblRuleOnlyTblPart`. The schema is a regex (checked separately) and
    /// the table is a trie pattern stored at the trie's schema level.
    TblRuleOnlyTblPart,
}

/// Go: `type nodeEndRule struct`.
#[derive(Clone, Debug)]
struct NodeEndRule {
    kind: RuleKind,
    is_allow_list: bool,
}

/// Go: `type cache struct` — a `map[string]ActionType` behind a `sync.RWMutex`.
#[derive(Debug, Default)]
struct Cache {
    items: RwLock<HashMap<String, ActionType>>,
}

impl Cache {
    /// Go: `(*cache).query`.
    fn query(&self, key: &str) -> Option<ActionType> {
        self.items
            .read()
            .expect("filter cache lock poisoned")
            .get(key)
            .copied()
    }

    /// Go: `(*cache).set`.
    fn set(&self, key: String, action: ActionType) {
        self.items
            .write()
            .expect("filter cache lock poisoned")
            .insert(key, action);
    }
}

/// Implements table filter in the style of MySQL replication rules.
///
/// Go: `type Filter struct`.
///
/// boundary: Go embeds `selector.Selector`, which promotes `Insert`, `Remove`
/// and `AllRules` onto `*Filter`. Those take `any` rules and would let a caller
/// put a non-`*nodeEndRule` into the trie, which every match path then
/// type-asserts. No in-tree caller uses them on a `*Filter`, so the selector
/// stays private here and the promoted methods have no counterpart —
/// `Filter.Match` (which Go defines itself, shadowing `Selector.Match`) is the
/// only matching entry point.
pub struct Filter {
    // `TrieSelector` holds its arena behind an `RwLock` and has no `Debug`, so
    // `Filter`'s own `Debug` (below) prints the rule set and case sensitivity
    // only. Go's `%+v` on a `*Filter` would print the trie too.
    selector: TrieSelector<NodeEndRule>,
    pattern_map: HashMap<String, Regex>,
    /// Go: `rules *Rules`. `None` is Go's nil pointer, which every entry point
    /// tests for and answers "keep everything".
    rules: Option<Rules>,
    c: Cache,
    case_sensitive: bool,
}

impl fmt::Debug for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Filter")
            .field("rules", &self.rules)
            .field("case_sensitive", &self.case_sensitive)
            .field("cache", &self.c)
            .finish_non_exhaustive()
    }
}

/// Creates a filter from the rules.
///
/// Go: `func New(caseSensitive bool, rules *Rules) (*Filter, error)`.
///
/// boundary: Go calls `rules.ToLower()` on the caller's pointer, so building a
/// case-insensitive filter rewrites the caller's rule set in place (quirk 7).
/// This port takes the rules by value and lowercases its own copy; the caller
/// can observe the lowercased form through [`Filter::rules`].
pub fn new(case_sensitive: bool, rules: Option<Rules>) -> Result<Filter, FilterError> {
    let mut rules = rules;
    if !case_sensitive {
        if let Some(rules) = rules.as_mut() {
            rules.to_lower();
        }
    }

    let mut f = Filter {
        selector: new_trie_selector(),
        pattern_map: HashMap::new(),
        rules,
        c: Cache::default(),
        case_sensitive,
    };
    f.init_rules()?;
    Ok(f)
}

impl Filter {
    /// The rule set this filter was built from, after the lowercasing `New`
    /// applies for a case-insensitive filter. Go: the unexported `rules` field,
    /// reachable from the same package.
    pub fn rules(&self) -> Option<&Rules> {
        self.rules.as_ref()
    }

    /// Whether this filter matches case-sensitively. Go: the unexported
    /// `caseSensitive` field.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Initializes the rules into regexes or trie nodes.
    ///
    /// Go: `(*Filter).initRules`. The four loops run in this exact order and
    /// the first failure aborts, so a later malformed rule is never reported
    /// when an earlier one is already bad.
    fn init_rules(&mut self) -> Result<(), FilterError> {
        let Some(rules) = self.rules.clone() else {
            return Ok(());
        };

        for db in &rules.do_dbs {
            if db.is_empty() {
                return Err(FilterError::EmptyDoDb);
            }
            self.init_schema_rule(db, true)?;
        }

        for table in &rules.do_tables {
            if table.schema.is_empty() || table.name.is_empty() {
                return Err(FilterError::EmptyDoTables);
            }
            self.init_table_rule(&table.schema, &table.name, true)?;
        }

        for db in &rules.ignore_dbs {
            if db.is_empty() {
                return Err(FilterError::EmptyIgnoreDb);
            }
            self.init_schema_rule(db, false)?;
        }

        for table in &rules.ignore_tables {
            if table.schema.is_empty() || table.name.is_empty() {
                return Err(FilterError::EmptyIgnoreTables);
            }
            self.init_table_rule(&table.schema, &table.name, false)?;
        }

        Ok(())
    }

    /// Go: `(*Filter).initOneRegex`. Compiles `origin_str` once and memoizes it
    /// under the *uncased* key, so the `(?i)` prefix is invisible to lookups.
    ///
    /// boundary: Go uses `regexp` (RE2). The `regex` crate is the same
    /// automaton family and rejects the same lookaround constructs, which the
    /// upstream `TestInvalidRegex` depends on. The classes `\d`, `\w` and `\s`
    /// are ASCII-only in Go and Unicode-aware in the `regex` crate; that
    /// difference is only reachable for non-ASCII identifiers.
    fn init_one_regex(&mut self, origin_str: &str) -> Result<(), FilterError> {
        if !self.pattern_map.contains_key(origin_str) {
            let compile_str = if self.case_sensitive {
                origin_str.to_string()
            } else {
                format!("(?i){origin_str}")
            };
            let reg = Regex::new(&compile_str)
                .map_err(|err| FilterError::InvalidRegex(err.to_string()))?;
            self.pattern_map.insert(origin_str.to_string(), reg);
        }
        Ok(())
    }

    /// Go: `(*Filter).initSchemaRule`. A `~`-prefixed schema is compiled and
    /// *not* inserted into the trie; a plain schema is inserted at the trie's
    /// schema level with an empty table part.
    fn init_schema_rule(&mut self, db_str: &str, is_allow_list: bool) -> Result<(), FilterError> {
        if let Some(rest) = db_str.strip_prefix('~') {
            return self.init_one_regex(rest);
        }
        self.selector.insert(
            db_str,
            "",
            Some(NodeEndRule {
                kind: RuleKind::DbRule,
                is_allow_list,
            }),
            InsertType::Append,
        )?;
        Ok(())
    }

    /// Go: `(*Filter).initTableRule`.
    ///
    /// Quirk 5: only the `!dbIsRegex && !tblIsRegex` shape ends up at the trie's
    /// table level. `~db`/`tbl` stores the *table* pattern as a schema-level
    /// key, and `db`/`~tbl` stores the *schema* pattern as a schema-level key
    /// with the table regex hanging off the rule.
    fn init_table_rule(
        &mut self,
        db_str: &str,
        table_str: &str,
        is_allow_list: bool,
    ) -> Result<(), FilterError> {
        let db_regex = db_str.strip_prefix('~');
        let tbl_regex = table_str.strip_prefix('~');
        match (db_regex, tbl_regex) {
            (Some(db), Some(tbl)) => {
                let (db, tbl) = (db.to_string(), tbl.to_string());
                self.init_one_regex(&db)?;
                self.init_one_regex(&tbl)?;
                // Nothing is inserted into the trie: `matchTable`'s first branch
                // matches this shape with both regexes directly.
            }
            (Some(db), None) => {
                let db = db.to_string();
                self.init_one_regex(&db)?;
                self.selector.insert(
                    table_str,
                    "",
                    Some(NodeEndRule {
                        kind: RuleKind::TblRuleOnlyTblPart,
                        is_allow_list,
                    }),
                    InsertType::Append,
                )?;
            }
            (None, Some(tbl)) => {
                let tbl = tbl.to_string();
                self.init_one_regex(&tbl)?;
                // Go reads `f.patternMap[tableStr[1:]]` back out here; it was
                // just populated, so the lookup always succeeds.
                let regex = self
                    .pattern_map
                    .get(&tbl)
                    .expect("initOneRegex just inserted this pattern")
                    .clone();
                self.selector.insert(
                    db_str,
                    "",
                    Some(NodeEndRule {
                        kind: RuleKind::TblRuleOnlyDbPart(regex),
                        is_allow_list,
                    }),
                    InsertType::Append,
                )?;
            }
            (None, None) => {
                self.selector.insert(
                    db_str,
                    table_str,
                    Some(NodeEndRule {
                        kind: RuleKind::TblRuleFull,
                        is_allow_list,
                    }),
                    InsertType::Append,
                )?;
            }
        }
        Ok(())
    }

    /// Applies the filter rules and converts schema/table names to lower case
    /// when the filter is not case sensitive.
    ///
    /// Go: `(*Filter).ApplyOn`, marked `Deprecated`.
    ///
    /// Quirk 8/9: the returned tables are the *lowercased clones*, and nothing
    /// matched yields Go's `nil` slice — modelled as `None`. `stbs` is likewise
    /// `Option` because Go returns the argument untouched, `nil`-ness included,
    /// when the filter has no rules.
    pub fn apply_on(&self, stbs: Option<&[Table]>) -> Option<Vec<Table>> {
        if self.rules.is_none() {
            return stbs.map(<[Table]>::to_vec);
        }

        let mut tbs: Vec<Table> = Vec::new();
        for tb in stbs.unwrap_or(&[]) {
            let mut new_tb = tb.clone();
            if !self.case_sensitive {
                new_tb.schema = new_tb.schema.to_lowercase();
                new_tb.name = new_tb.name.to_lowercase();
            }

            if self.matches(&new_tb) {
                tbs.push(new_tb);
            }
        }

        // Go starts from `var tbs []*Table` and only appends, so the result is
        // nil unless at least one table matched.
        if tbs.is_empty() {
            None
        } else {
            Some(tbs)
        }
    }

    /// Applies the filter rules on tables.
    ///
    /// Go: `(*Filter).Apply`.
    ///
    /// Quirk 8/9: unlike [`Filter::apply_on`] this returns the *original*
    /// tables, and an empty-but-not-`nil` slice when nothing matched. It is
    /// still `Option` because Go returns the argument verbatim when the filter
    /// has no rules, and that argument may itself be `nil`.
    pub fn apply(&self, stbs: Option<&[Table]>) -> Option<Vec<Table>> {
        if self.rules.is_none() {
            return stbs.map(<[Table]>::to_vec);
        }
        // Go: `tbs := make([]*Table, 0)`, non-nil from the start.
        let mut tbs: Vec<Table> = Vec::new();
        for tb in stbs.unwrap_or(&[]) {
            let new_tb = if self.case_sensitive {
                tb.clone()
            } else {
                Table {
                    schema: tb.schema.to_lowercase(),
                    name: tb.name.to_lowercase(),
                }
            };

            if self.matches(&new_tb) {
                tbs.push(tb.clone());
            }
        }
        Some(tbs)
    }

    /// Returns true if the specified table should not be removed.
    ///
    /// boundary: named `matches` because Go's `(*Filter).Match` shadows the
    /// embedded `Selector.Match`, and `match` is a Rust keyword.
    pub fn matches(&self, tb: &Table) -> bool {
        let Some(rules) = self.rules.as_ref() else {
            return true;
        };
        let mut new_tb = tb.clone();
        if !self.case_sensitive {
            new_tb.schema = new_tb.schema.to_lowercase();
            new_tb.name = new_tb.name.to_lowercase();
        }

        let name = new_tb.to_string();
        let do_action = match self.c.query(&name) {
            Some(action) => action,
            None => {
                let action = ActionType::from_bool(
                    self.filter_on_schemas(rules, &new_tb) && self.filter_on_tables(rules, &new_tb),
                );
                // Go recomputes `newTb.String()` for the set; it is the same
                // string as `name`.
                self.c.set(name, action);
                action
            }
        };
        do_action == ActionType::Do
    }

    /// Go: `(*Filter).filterOnSchemas`.
    ///
    /// Quirk 1: `do_dbs` and `ignore_dbs` are chained with `else if`, so a
    /// non-empty `do_dbs` disables `ignore_dbs` completely.
    fn filter_on_schemas(&self, rules: &Rules, tb: &Table) -> bool {
        if !rules.do_dbs.is_empty() {
            // not matched do db rules, ignore update
            if !self.find_matched_do_dbs(rules, tb) {
                return false;
            }
        } else if !rules.ignore_dbs.is_empty() {
            // matched ignore db rules, ignore update
            if self.find_matched_ignore_dbs(rules, tb) {
                return false;
            }
        }

        true
    }

    /// Go: `(*Filter).findMatchedDoDBs`.
    fn find_matched_do_dbs(&self, rules: &Rules, tb: &Table) -> bool {
        self.match_db(&rules.do_dbs, &tb.schema, true)
    }

    /// Go: `(*Filter).findMatchedIgnoreDBs`.
    fn find_matched_ignore_dbs(&self, rules: &Rules, tb: &Table) -> bool {
        self.match_db(&rules.ignore_dbs, &tb.schema, false)
    }

    /// Go: `(*Filter).filterOnTables`.
    ///
    /// Quirk 2: a `do_tables` hit returns `true` immediately, so `ignore_tables`
    /// cannot veto it. The trailing `len(DoTables) == 0` is what makes a
    /// non-empty allowlist exclude everything it did not name.
    fn filter_on_tables(&self, rules: &Rules, tb: &Table) -> bool {
        // schema statement like create/drop/alter database
        if tb.name.is_empty() {
            return true;
        }

        if !rules.do_tables.is_empty() && self.match_table(&rules.do_tables, tb, true) {
            return true;
        }

        if !rules.ignore_tables.is_empty() && self.match_table(&rules.ignore_tables, tb, false) {
            return false;
        }

        rules.do_tables.is_empty()
    }

    /// Go: `(*Filter).matchDB`.
    ///
    /// Quirk 5: the trie lookup can surface `TBL_RULE_ONLY_TBL_PART` and
    /// `TBL_RULE_ONLY_DB_PART` entries, which also live at the schema level; the
    /// `kind` test is the only thing keeping them out.
    fn match_db(&self, pattern_dbs: &[String], a: &str, is_allow_list_check: bool) -> bool {
        for b in pattern_dbs {
            if let Some(rest) = b.strip_prefix('~') {
                if self.match_string(rest, a) {
                    return true;
                }
            }
        }
        if let Some(rule_set) = self.selector.match_rules(a, "") {
            for rule in &rule_set {
                if matches!(rule.kind, RuleKind::DbRule)
                    && rule.is_allow_list == is_allow_list_check
                {
                    return true;
                }
            }
        }
        false
    }

    /// Go: `(*Filter).matchTable`.
    ///
    /// Quirks 3 and 4 live here: the `TBL_RULE_ONLY_DB_PART` and
    /// `TBL_RULE_FULL` lookups at the end of the loop body do not read `ptb` at
    /// all, yet they run once per entry — except for entries taken by the
    /// `continue` in the regex-schema branch.
    fn match_table(&self, pattern_tbs: &[Table], tb: &Table, is_allow_list_check: bool) -> bool {
        for ptb in pattern_tbs {
            let db_regex = ptb.schema.strip_prefix('~');
            let tbl_regex = ptb.name.strip_prefix('~');
            match (db_regex, tbl_regex) {
                (Some(db), Some(tbl)) => {
                    if self.match_string(db, &tb.schema) && self.match_string(tbl, &tb.name) {
                        return true;
                    }
                }
                (Some(db), None) => {
                    if !self.match_string(db, &tb.schema) {
                        // Quirk 4: skips the two shared lookups below.
                        continue;
                    }
                    // The table pattern was stored at the trie's schema level,
                    // so the table name is matched as if it were a schema.
                    if let Some(rule_set) = self.selector.match_rules(&tb.name, "") {
                        for rule in &rule_set {
                            if matches!(rule.kind, RuleKind::TblRuleOnlyTblPart)
                                && rule.is_allow_list == is_allow_list_check
                            {
                                return true;
                            }
                        }
                    }
                }
                // Go has no branch for the remaining two shapes; they fall
                // straight through to the shared lookups.
                (None, _) => {}
            }

            if let Some(rule_set) = self.selector.match_rules(&tb.schema, "") {
                for rule in &rule_set {
                    if let RuleKind::TblRuleOnlyDbPart(re) = &rule.kind {
                        if rule.is_allow_list == is_allow_list_check && re.is_match(&tb.name) {
                            return true;
                        }
                    }
                }
            }
            if let Some(rule_set) = self.selector.match_rules(&tb.schema, &tb.name) {
                for rule in &rule_set {
                    if matches!(rule.kind, RuleKind::TblRuleFull)
                        && rule.is_allow_list == is_allow_list_check
                    {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Go: `(*Filter).matchString`.
    ///
    /// Quirk 6: an uncompiled pattern degrades to string equality instead of
    /// erroring.
    fn match_string(&self, pattern: &str, t: &str) -> bool {
        if let Some(re) = self.pattern_map.get(pattern) {
            return re.is_match(t);
        }
        pattern == t
    }
}

// ---------------------------------------------------------------------------
// schema.go
// ---------------------------------------------------------------------------

/// The heartbeat schema name.
///
/// boundary: Go declares `DMHeartbeatSchema` as a package-level `var`, so a
/// caller could reassign it. No in-tree code does, so it is a `const` here.
pub const DM_HEARTBEAT_SCHEMA: &str = "dm_heartbeat";

/// The `INSPECTION_SCHEMA` database name.
///
/// boundary: Go declares `InspectionSchemaName` as a package-level `var`; see
/// [`DM_HEARTBEAT_SCHEMA`].
pub const INSPECTION_SCHEMA_NAME: &str = "inspection_schema";

/// Checks whether `schema` is a system schema. Case insensitive: the caller
/// must pass an already-lowercased name.
///
/// Go: `func IsSystemSchema(schema string) bool`. Go asserts the lowercase
/// precondition with `intest.AssertFunc`, which only runs in `intest` builds;
/// `debug_assert!` is the same "checked in test builds, free in release" shape.
pub fn is_system_schema(schema: &str) -> bool {
    debug_assert!(
        schema == schema.to_lowercase(),
        "IsSystemSchema expects a lowercased schema name, got {schema}"
    );
    schema == DM_HEARTBEAT_SCHEMA
        || schema == INSPECTION_SCHEMA_NAME
        || tidb_metadef::is_mem_or_sys_db(schema)
}

/// Transcreated from `pkg/util/filter/filter_test.go` (`TestFilterOnSchema`,
/// `TestCaseSensitiveApply`, `TestMaxBox`, `TestCaseSensitive`,
/// `TestInvalidRegex`, `TestMatchReturnsBool`) and
/// `pkg/util/filter/schema_test.go` (`TestIsSystemSchema`). No test here is
/// newly written: the upstream files cover every exported symbol.
#[cfg(test)]
mod tests {
    use super::*;

    /// Go: `cloneTables`. Go returns `nil` for a `nil` input, which the caller
    /// compares against the (possibly `nil`) original input.
    fn clone_tables(tbs: Option<&[Table]>) -> Option<Vec<Table>> {
        tbs.map(<[Table]>::to_vec)
    }

    /// One row of the `cases` slice shared by `TestFilterOnSchema` and
    /// `TestCaseSensitiveApply`.
    struct ApplyCase {
        rules: Rules,
        input: Option<Vec<Table>>,
        output: Option<Vec<Table>>,
        case_sensitive: bool,
    }

    fn tbl(schema: &str, name: &str) -> Table {
        Table::new(schema, name)
    }

    /// Go: `TestFilterOnSchema`.
    #[test]
    fn test_filter_on_schema() {
        let cases = vec![
            // empty rules
            ApplyCase {
                rules: Rules::default(),
                input: None,
                output: None,
                case_sensitive: false,
            },
            ApplyCase {
                rules: Rules::default(),
                input: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                output: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                case_sensitive: false,
            },
            // schema-only rules
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo".into()],
                    do_dbs: vec!["foo".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", ""),
                    tbl("foo1", "bar"),
                    tbl("foo1", ""),
                ]),
                output: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                case_sensitive: false,
            },
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo1".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", ""),
                    tbl("foo1", "bar"),
                    tbl("foo1", ""),
                ]),
                output: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                case_sensitive: false,
            },
            // DoTable rules (without regex)
            ApplyCase {
                rules: Rules {
                    do_tables: vec![tbl("foo", "bar1")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", "bar1"),
                    tbl("foo", ""),
                    tbl("fff", "bar1"),
                ]),
                output: Some(vec![tbl("foo", "bar1"), tbl("foo", "")]),
                case_sensitive: false,
            },
            // ignoreTable rules (without regex)
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("foo", "bar")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", "bar1"),
                    tbl("foo", ""),
                    tbl("fff", "bar1"),
                ]),
                output: Some(vec![tbl("foo", "bar1"), tbl("foo", ""), tbl("fff", "bar1")]),
                case_sensitive: false,
            },
            // all regexp
            ApplyCase {
                rules: Rules {
                    do_dbs: vec!["~^foo".into()],
                    ignore_tables: vec![tbl("~^foo", r"~^sbtest-\d")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "sbtest"),
                    tbl("foo1", "sbtest-1"),
                    tbl("foo2", ""),
                    tbl("fff", "bar"),
                ]),
                output: Some(vec![tbl("foo", "sbtest"), tbl("foo2", "")]),
                case_sensitive: false,
            },
            // test rule with * or ?
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo[bar]".into(), "foo?".into(), r"special\".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foor", "a"),
                    tbl("foo[bar]", "b"),
                    tbl("fo", "c"),
                    tbl("foo?", "d"),
                    tbl(r"special\", "e"),
                ]),
                output: Some(vec![tbl("foo[bar]", "b"), tbl("fo", "c")]),
                case_sensitive: false,
            },
            // ensure non case-insensitive
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["~^FOO".into()],
                    ignore_tables: vec![tbl("~.*", "~FoO$")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("FOO1", "a"),
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoO"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                output: Some(vec![tbl("5", "5")]),
                case_sensitive: false,
            },
            // ensure case-insensitive
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["~^FOO".into()],
                    ignore_tables: vec![tbl("~.*", "~FoO$")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("FOO1", "a"),
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoo"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                output: Some(vec![
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoo"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                case_sensitive: true,
            },
            // the rule whose schema part is not regex and the table part is regex
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a?b?", "~f[0-9]")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("abbd", "f1"),
                    tbl("aaaa", "f2"),
                    tbl("5", "5"),
                    tbl("abbc", "fa"),
                ]),
                output: Some(vec![tbl("aaaa", "f2"), tbl("5", "5"), tbl("abbc", "fa")]),
                case_sensitive: false,
            },
            // the rule whose schema part is regex and the table part is not regex
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("~t[0-8]", "a??")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("t1", "a01"),
                    tbl("t9", "a02"),
                    tbl("5", "5"),
                    tbl("t9", "a001"),
                ]),
                output: Some(vec![tbl("t9", "a02"), tbl("5", "5"), tbl("t9", "a001")]),
                case_sensitive: false,
            },
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a*", "A*")],
                    ..Rules::default()
                },
                input: Some(vec![tbl("aB", "Ab"), tbl("AaB", "aab"), tbl("acB", "Afb")]),
                output: Some(vec![tbl("AaB", "aab")]),
                case_sensitive: true,
            },
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a*", "A*")],
                    ..Rules::default()
                },
                input: Some(vec![tbl("aB", "Ab"), tbl("AaB", "aab"), tbl("acB", "Afb")]),
                // Quirk 8: `ApplyOn` returns Go's nil slice here.
                output: None,
                case_sensitive: false,
            },
        ];

        for (i, tt) in cases.into_iter().enumerate() {
            let ft = new(tt.case_sensitive, Some(tt.rules)).expect("filter builds");
            let origin_input = clone_tables(tt.input.as_deref());
            let got = ft.apply_on(tt.input.as_deref());
            assert_eq!(tt.input, origin_input, "case {i}: input must not change");
            assert_eq!(tt.output, got, "case {i}");
        }
    }

    /// Go: `TestCaseSensitiveApply`.
    #[test]
    fn test_case_sensitive_apply() {
        let cases = vec![
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo".into()],
                    do_dbs: vec!["foo".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", ""),
                    tbl("foo1", "bar"),
                    tbl("foo1", ""),
                ]),
                output: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                case_sensitive: false,
            },
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo1".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", ""),
                    tbl("foo1", "bar"),
                    tbl("foo1", ""),
                ]),
                output: Some(vec![tbl("foo", "bar"), tbl("foo", "")]),
                case_sensitive: false,
            },
            // ignoreTable rules (without regex)
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("Foo", "bAr")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "bar"),
                    tbl("foo", "bar1"),
                    tbl("foo", ""),
                    tbl("fff", "bar1"),
                ]),
                output: Some(vec![tbl("foo", "bar1"), tbl("foo", ""), tbl("fff", "bar1")]),
                case_sensitive: false,
            },
            // all regexp
            ApplyCase {
                rules: Rules {
                    do_dbs: vec!["~^foo".into()],
                    ignore_tables: vec![tbl("~^foo", r"~^sbtest-\d")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foo", "sbtest"),
                    tbl("foo1", "sbtest-1"),
                    tbl("foo2", ""),
                    tbl("fff", "bar"),
                ]),
                output: Some(vec![tbl("foo", "sbtest"), tbl("foo2", "")]),
                case_sensitive: false,
            },
            // test rule with * or ?
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["foo[bar]".into(), "foo?".into(), r"special\".into()],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("foor", "a"),
                    tbl("foo[bar]", "b"),
                    tbl("Fo", "c"),
                    tbl("foo?", "d"),
                    tbl(r"special\", "e"),
                ]),
                // Quirk 9: `Apply` gives back the original, un-lowercased `Fo`.
                output: Some(vec![tbl("foo[bar]", "b"), tbl("Fo", "c")]),
                case_sensitive: false,
            },
            // ensure non case-insensitive
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["~^FOO".into()],
                    ignore_tables: vec![tbl("~.*", "~FoO$")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("FOO1", "a"),
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoO"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                output: Some(vec![tbl("5", "5")]),
                case_sensitive: false,
            },
            // ensure case-insensitive
            ApplyCase {
                rules: Rules {
                    ignore_dbs: vec!["~^FOO".into()],
                    ignore_tables: vec![tbl("~.*", "~FoO$")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("FOO1", "a"),
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoo"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                output: Some(vec![
                    tbl("foo2", "b"),
                    tbl("BoO3", "cFoo"),
                    tbl("Foo4", "dfoo"),
                    tbl("5", "5"),
                ]),
                case_sensitive: true,
            },
            // the rule whose schema part is not regex and the table part is regex
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a?b?", "~f[0-9]")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("abBd", "f1"),
                    tbl("aAAa", "f2"),
                    tbl("5", "5"),
                    tbl("abbc", "FA"),
                ]),
                output: Some(vec![tbl("aAAa", "f2"), tbl("5", "5"), tbl("abbc", "FA")]),
                case_sensitive: false,
            },
            // the rule whose schema part is regex and the table part is not regex
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("~t[0-8]", "A??")],
                    ..Rules::default()
                },
                input: Some(vec![
                    tbl("t1", "a01"),
                    tbl("t9", "A02"),
                    tbl("5", "5"),
                    tbl("T9", "a001"),
                ]),
                output: Some(vec![tbl("t9", "A02"), tbl("5", "5"), tbl("T9", "a001")]),
                case_sensitive: false,
            },
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a*", "A*")],
                    ..Rules::default()
                },
                input: Some(vec![tbl("aB", "Ab"), tbl("AaB", "aab"), tbl("acB", "Afb")]),
                output: Some(vec![tbl("AaB", "aab")]),
                case_sensitive: true,
            },
            ApplyCase {
                rules: Rules {
                    ignore_tables: vec![tbl("a*", "A*")],
                    ..Rules::default()
                },
                input: Some(vec![tbl("aB", "Ab"), tbl("AaB", "aab"), tbl("acB", "Afb")]),
                // Quirk 8: `Apply` returns an empty but non-nil slice here,
                // where `ApplyOn` returns nil for the same inputs.
                output: Some(vec![]),
                case_sensitive: false,
            },
        ];

        for (i, tt) in cases.into_iter().enumerate() {
            let ft = new(tt.case_sensitive, Some(tt.rules)).expect("filter builds");
            let origin_input = clone_tables(tt.input.as_deref());
            let got = ft.apply(tt.input.as_deref());
            assert_eq!(tt.input, origin_input, "case {i}: input must not change");
            assert_eq!(tt.output, got, "case {i}");
        }
    }

    /// Go: `TestMaxBox`.
    #[test]
    fn test_max_box() {
        let rules = Rules {
            do_tables: vec![tbl("test1", "t1")],
            ignore_tables: vec![tbl("test1", "t2")],
            ..Rules::default()
        };

        let r = new(false, Some(rules)).expect("filter builds");

        let x = tbl("test1", "");
        let input = [x.clone()];
        let res = r.apply_on(Some(&input)).expect("one table matched");
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], x);
    }

    /// Go: `TestCaseSensitive`.
    #[test]
    fn test_case_sensitive() {
        // ensure case-sensitive rules are really case-sensitive
        let rules = Rules {
            ignore_dbs: vec!["~^FOO".into()],
            ignore_tables: vec![tbl("~.*", "~FoO$")],
            ..Rules::default()
        };
        let r = new(true, Some(rules)).expect("filter builds");

        let input = vec![
            tbl("FOO1", "a"),
            tbl("foo2", "b"),
            tbl("BoO3", "cFoO"),
            tbl("Foo4", "dfoo"),
            tbl("5", "5"),
        ];
        let actual = r.apply_on(Some(&input));
        let expected = Some(vec![tbl("foo2", "b"), tbl("Foo4", "dfoo"), tbl("5", "5")]);
        assert_eq!(expected, actual);

        let input_table = tbl("FOO", "a");
        assert!(!r.matches(&input_table));

        let rules = Rules {
            do_dbs: vec!["BAR".into()],
            ..Rules::default()
        };

        let r = new(false, Some(rules)).expect("filter builds");
        let input_table = tbl("bar", "a");
        assert!(r.matches(&input_table));

        let input_table = tbl("BAR", "a");
        let origin_input_table = input_table.clone();
        assert!(r.matches(&input_table));
        assert_eq!(input_table, origin_input_table);
    }

    /// Go: `TestInvalidRegex`. Both patterns use lookaround, which RE2 rejects
    /// and the `regex` crate rejects for the same reason.
    #[test]
    fn test_invalid_regex() {
        let cases = [
            Rules {
                do_dbs: vec!["~^t[0-9]+((?!_copy).)*$".into()],
                ..Rules::default()
            },
            Rules {
                do_dbs: vec!["~^t[0-9]+sp(?=copy).*".into()],
                ..Rules::default()
            },
        ];
        for tc in cases {
            assert!(new(true, Some(tc)).is_err());
        }
    }

    /// Go: `TestMatchReturnsBool`.
    #[test]
    fn test_match_returns_bool() {
        let rules = Rules {
            do_dbs: vec!["sns".into()],
            ..Rules::default()
        };
        let f = new(true, Some(rules)).expect("filter builds");
        assert!(f.matches(&tbl("sns", "")));
        assert!(!f.matches(&tbl("other", "")));

        let f = new(true, None).expect("filter builds");
        assert!(f.matches(&tbl("other", "")));
    }

    /// Go: `TestIsSystemSchema` in `schema_test.go`. Go lowercases each name
    /// with `ast.NewCIStr(name).L` first; that is `strings.ToLower`, so the
    /// Rust test lowercases directly.
    #[test]
    fn test_is_system_schema() {
        let cases: [(&str, bool); 11] = [
            ("information_schema", true),
            ("performance_schema", true),
            ("mysql", true),
            ("sys", true),
            ("INFORMATION_SCHEMA", true),
            ("PERFORMANCE_SCHEMA", true),
            ("MYSQL", true),
            ("SYS", true),
            ("not_system_schema", false),
            ("METRICS_SCHEMA", true),
            ("INSPECTION_SCHEMA", true),
        ];

        for (name, expected) in cases {
            assert_eq!(
                expected,
                is_system_schema(&name.to_lowercase()),
                "schema name = {name}"
            );
        }
    }

    /// Go has no direct test for `(*Table).String`, `(*Table).lessThan` or
    /// `DMHeartbeatSchema`; these are exercised only indirectly through the
    /// cache key and `IsSystemSchema`. Written here so every exported symbol of
    /// the package has coverage.
    #[test]
    fn test_table_helpers_written() {
        assert_eq!(tbl("db", "t").to_string(), "`db`.`t`");
        assert_eq!(tbl("db", "").to_string(), "`db`");
        assert!(tbl("a", "z").less_than(&tbl("b", "a")));
        assert!(tbl("a", "a").less_than(&tbl("a", "b")));
        assert!(!tbl("a", "b").less_than(&tbl("a", "b")));
        assert!(is_system_schema(DM_HEARTBEAT_SCHEMA));
        assert!(is_system_schema(INSPECTION_SCHEMA_NAME));
    }

    /// Go has no direct test for `(*Rules).ToLower` in this package, but `New`
    /// depends on it and quirk 7 makes it observable. Written here.
    #[test]
    fn test_rules_to_lower_written() {
        let rules = Rules {
            do_tables: vec![tbl("Db", "Tb")],
            do_dbs: vec!["Db".into()],
            ignore_tables: vec![tbl("IDb", "ITb")],
            ignore_dbs: vec!["IDb".into()],
        };
        let f = new(false, Some(rules)).expect("filter builds");
        let lowered = f.rules().expect("rules are present");
        assert_eq!(lowered.do_tables, vec![tbl("db", "tb")]);
        assert_eq!(lowered.do_dbs, vec!["db".to_string()]);
        assert_eq!(lowered.ignore_tables, vec![tbl("idb", "itb")]);
        assert_eq!(lowered.ignore_dbs, vec!["idb".to_string()]);
        assert!(!f.case_sensitive());
    }

    /// Go has no test for the empty-string rule errors of `initRules`; each
    /// message is only reachable through `New`. Written here so all four
    /// branches and their exact wording are pinned.
    #[test]
    fn test_empty_rule_errors_written() {
        let err = new(
            true,
            Some(Rules {
                do_dbs: vec![String::new()],
                ..Rules::default()
            }),
        )
        .expect_err("empty DoDB is rejected");
        assert_eq!(err.to_string(), "DoDB rule's DB string cannot be empty");

        let err = new(
            true,
            Some(Rules {
                do_tables: vec![tbl("db", "")],
                ..Rules::default()
            }),
        )
        .expect_err("empty DoTables name is rejected");
        assert_eq!(
            err.to_string(),
            "DoTables rule's DB string or Table string cannot be empty"
        );

        let err = new(
            true,
            Some(Rules {
                ignore_dbs: vec![String::new()],
                ..Rules::default()
            }),
        )
        .expect_err("empty IgnoreDB is rejected");
        assert_eq!(err.to_string(), "IgnoreDB rule's DB string cannot be empty");

        let err = new(
            true,
            Some(Rules {
                ignore_tables: vec![tbl("", "t")],
                ..Rules::default()
            }),
        )
        .expect_err("empty IgnoreTables schema is rejected");
        assert_eq!(
            err.to_string(),
            "IgnoreTables rule's DB string or Table string cannot be empty"
        );
    }
}
