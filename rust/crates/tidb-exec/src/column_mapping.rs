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

//! Column mapping rules.
//!
//! Transcreation of the whole Go package `pkg/util/column-mapping`
//! (`package column`, single file `column.go`).
//!
//! A [`Mapping`] holds rules keyed by schema/table wildcard patterns in a
//! [`TrieSelector`] and rewrites one column of a row (or reports that a DDL
//! cannot be handled). Three built-in expressions exist: prefix a string
//! column, suffix a string column, and fold an instance/schema/table id into
//! the high bits of an integer column ("partition id").
//!
//! The partition-id layout uses process-wide bit widths configured by
//! [`set_partition_rule`], reproducing Go's package-level `instanceIDBitSize`
//! / `schemaIDBitSize` / `tableIDBitSize` / `maxOriginID` variables.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;

use crate::table_rule_selector::{
    new_trie_selector, InsertType, RuleSet, SchemaRules, Selector, SelectorError, TableRules,
    TrieSelector,
};

// Go: the package-level `var (...)` block holding the partition-id bit widths.
// Go stores plain `int`/`int64` globals mutated by `SetPartitionRule` without
// synchronisation; the port uses relaxed atomics so the same shared-mutable
// behaviour is expressible without data races.
//
/// Go: `instanceIDBitSize`, for partition ID, ref definition of `partitionID`.
static INSTANCE_ID_BIT_SIZE: AtomicI64 = AtomicI64::new(4);
/// Go: `schemaIDBitSize`.
static SCHEMA_ID_BIT_SIZE: AtomicI64 = AtomicI64::new(7);
/// Go: `tableIDBitSize`.
static TABLE_ID_BIT_SIZE: AtomicI64 = AtomicI64::new(8);
/// Go: `maxOriginID`, initialised to `17592186044416` (`1 << 44`).
static MAX_ORIGIN_ID: AtomicI64 = AtomicI64::new(17592186044416);

fn instance_id_bit_size() -> i64 {
    INSTANCE_ID_BIT_SIZE.load(Ordering::Relaxed)
}

fn schema_id_bit_size() -> i64 {
    SCHEMA_ID_BIT_SIZE.load(Ordering::Relaxed)
}

fn table_id_bit_size() -> i64 {
    TABLE_ID_BIT_SIZE.load(Ordering::Relaxed)
}

fn max_origin_id() -> i64 {
    MAX_ORIGIN_ID.load(Ordering::Relaxed)
}

/// Sets bit size of schema ID and table ID. Go: `SetPartitionRule`.
///
/// `maxOriginID` is recomputed as `1 << (64 - instance - schema - table - 1)`.
/// Go evaluates that shift on an `int64`, so a shift count of 64 or more
/// yields `0`; [`i64::checked_shl`] reproduces that, and a negative shift
/// count (Go converts it to a huge `uint`, also yielding `0`) lands in the
/// same branch.
pub fn set_partition_rule(instance_id_size: i64, schema_id_size: i64, table_id_size: i64) {
    INSTANCE_ID_BIT_SIZE.store(instance_id_size, Ordering::Relaxed);
    SCHEMA_ID_BIT_SIZE.store(schema_id_size, Ordering::Relaxed);
    TABLE_ID_BIT_SIZE.store(table_id_size, Ordering::Relaxed);
    let shift = 64 - instance_id_size - schema_id_size - table_id_size - 1;
    let max = if (0..64).contains(&shift) {
        1i64 << shift
    } else {
        0
    };
    MAX_ORIGIN_ID.store(max, Ordering::Relaxed);
}

/// Indicates how to handle column mapping. Go: `type Expr string`.
///
/// boundary: Go's `Expr` is a bare `string`, so any text can be stored in
/// `Rule.Expression` and `Exprs[expr]` simply misses. [`Expr::Other`] carries
/// that arbitrary text, which keeps the "unknown expression" error path
/// reachable (the upstream test drives it with the literal `"Error"`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Expr {
    /// Go: `AddPrefix Expr = "add prefix"`. Arguments contain the prefix.
    AddPrefix,
    /// Go: `AddSuffix Expr = "add suffix"`. Arguments contain the suffix.
    AddSuffix,
    /// Go: `PartitionID Expr = "partition id"`.
    ///
    /// Arguments contain `[instance_id, prefix of schema, prefix of table]`
    /// and an ID like `[1:1 bit][2:9 bits][3:10 bits][4:44 bits] int64` is
    /// computed (using the default bit lengths):
    ///
    /// - `#1` useless, no reason
    /// - `#2` schema ID (schema suffix)
    /// - `#3` table ID (table suffix)
    /// - `#4` origin ID (`>= 0`, `<= 17592186044415`)
    ///
    /// otherwise `schema = arguments[1]` or `arguments[1] + arguments[3] +
    /// schema suffix`, and `table = arguments[2]` or `arguments[2] +
    /// arguments[3] + table suffix`. Example: `schema = schema_1`,
    /// `table = t_1` gives `arguments[1] = "schema"`, `arguments[2] = "t"`,
    /// `arguments[3] = "_"`. If `arguments[1]`/`arguments[2]` is empty the
    /// schema/table ID is not used to compute the partition ID, and if the
    /// length of arguments is `< 4`, `arguments[3]` is set to the empty string.
    PartitionID,
    /// Any other string an `Expr` may hold; never present in [`exprs`].
    Other(String),
}

impl Expr {
    /// The exact string Go stores in the `Expr` value.
    pub fn as_str(&self) -> &str {
        match self {
            Expr::AddPrefix => "add prefix",
            Expr::AddSuffix => "add suffix",
            Expr::PartitionID => "partition id",
            Expr::Other(s) => s,
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Self {
        match s {
            "add prefix" => Expr::AddPrefix,
            "add suffix" => Expr::AddSuffix,
            "partition id" => Expr::PartitionID,
            other => Expr::Other(other.to_owned()),
        }
    }
}

/// The signature every built-in column mapping expression has.
///
/// Go: the value type of `var Exprs map[Expr]func(*mappingInfo, []any) ([]any, error)`.
pub type ExprFn =
    fn(&MappingInfo, Vec<ColumnValue>) -> Result<Vec<ColumnValue>, ColumnMappingError>;

/// Some built-in expression for column mapping. Go: `var Exprs`.
///
/// Only supports some poor expressions now; we would unify `tableInfo` later
/// and support more.
///
/// boundary: Go exports `Exprs` as a mutable package-level map, so external
/// code could register further expressions. No in-tree caller mutates it, so
/// the port exposes a lookup returning `None` for an unregistered expression,
/// which is exactly what `Exprs[expr]` with the comma-ok form yields.
pub fn exprs(expr: &Expr) -> Option<ExprFn> {
    match expr {
        Expr::AddPrefix => Some(add_prefix as ExprFn),
        Expr::AddSuffix => Some(add_suffix as ExprFn),
        Expr::PartitionID => Some(partition_id as ExprFn),
        Expr::Other(_) => None,
    }
}

/// One column value of a row.
///
/// boundary: Go passes `[]any` and `partitionID` type-switches over a fixed
/// list of integer types. This enum names exactly the arms of that switch plus
/// [`ColumnValue::Str`], and [`ColumnValue::Other`] stands for Go's `default:`
/// arm, which errors out. `Int16` and `Uint8` are listed explicitly *because*
/// Go's switch omits them (see the note on [`partition_id`]); they take the
/// same path as `Other`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ColumnValue {
    /// Go `int`.
    Int(i64),
    /// Go `int8`.
    Int8(i8),
    /// Go `int16`; absent from `partitionID`'s type switch.
    Int16(i16),
    /// Go `int32`.
    Int32(i32),
    /// Go `int64`.
    Int64(i64),
    /// Go `uint`.
    Uint(u64),
    /// Go `uint8`; absent from `partitionID`'s type switch.
    Uint8(u8),
    /// Go `uint16`.
    Uint16(u16),
    /// Go `uint32`.
    Uint32(u32),
    /// Go `uint64`.
    Uint64(u64),
    /// Go `string`.
    Str(String),
    /// Any other dynamic type an `any` may carry. `type_name` renders like
    /// Go's `%T` and `display` like `%v`.
    Other {
        /// Go's `%T` rendering of the value.
        type_name: String,
        /// Go's `%v` rendering of the value.
        display: String,
    },
}

impl ColumnValue {
    /// Go's `%T` verb.
    fn type_name(&self) -> String {
        match self {
            ColumnValue::Int(_) => "int".to_owned(),
            ColumnValue::Int8(_) => "int8".to_owned(),
            ColumnValue::Int16(_) => "int16".to_owned(),
            ColumnValue::Int32(_) => "int32".to_owned(),
            ColumnValue::Int64(_) => "int64".to_owned(),
            ColumnValue::Uint(_) => "uint".to_owned(),
            ColumnValue::Uint8(_) => "uint8".to_owned(),
            ColumnValue::Uint16(_) => "uint16".to_owned(),
            ColumnValue::Uint32(_) => "uint32".to_owned(),
            ColumnValue::Uint64(_) => "uint64".to_owned(),
            ColumnValue::Str(_) => "string".to_owned(),
            ColumnValue::Other { type_name, .. } => type_name.clone(),
        }
    }
}

impl fmt::Display for ColumnValue {
    /// Go's `%v` verb.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnValue::Int(v) | ColumnValue::Int64(v) => write!(f, "{v}"),
            ColumnValue::Int8(v) => write!(f, "{v}"),
            ColumnValue::Int16(v) => write!(f, "{v}"),
            ColumnValue::Int32(v) => write!(f, "{v}"),
            ColumnValue::Uint(v) | ColumnValue::Uint64(v) => write!(f, "{v}"),
            ColumnValue::Uint8(v) => write!(f, "{v}"),
            ColumnValue::Uint16(v) => write!(f, "{v}"),
            ColumnValue::Uint32(v) => write!(f, "{v}"),
            ColumnValue::Str(v) => f.write_str(v),
            ColumnValue::Other { display, .. } => f.write_str(display),
        }
    }
}

/// Errors produced by this package.
///
/// Go builds these with `github.com/pingcap/errors` helpers; `Display`
/// reproduces the text those helpers render, which the upstream test matches
/// with `require.Regexp`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ColumnMappingError {
    /// Go: `errors.NotFoundf`.
    NotFound(String),
    /// Go: `errors.NotValidf`.
    NotValid(String),
    /// Go: `errors.NotSupportedf`.
    NotSupported(String),
    /// Go: `errors.Errorf`.
    Message(String),
    /// Go: `errors.Annotate` / `errors.Annotatef`.
    Annotated(String, Box<ColumnMappingError>),
    /// An error surfaced by the underlying rule selector.
    Selector(SelectorError),
}

impl fmt::Display for ColumnMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnMappingError::NotFound(what) => write!(f, "{what} not found"),
            ColumnMappingError::NotValid(what) => write!(f, "{what} not valid"),
            ColumnMappingError::NotSupported(what) => write!(f, "{what} not supported"),
            ColumnMappingError::Message(msg) => f.write_str(msg),
            ColumnMappingError::Annotated(ctx, inner) => write!(f, "{ctx}: {inner}"),
            ColumnMappingError::Selector(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ColumnMappingError {}

impl From<SelectorError> for ColumnMappingError {
    fn from(err: SelectorError) -> Self {
        ColumnMappingError::Selector(err)
    }
}

impl ColumnMappingError {
    fn annotate(self, ctx: impl Into<String>) -> ColumnMappingError {
        ColumnMappingError::Annotated(ctx.into(), Box::new(self))
    }
}

/// Go's `%v` for a `[]string`, e.g. `[1 test_ t_]`.
fn fmt_args(args: &[String]) -> String {
    format!("[{}]", args.join(" "))
}

/// A rule to map column.
///
/// TODO: we will do it later, if we need to implement a real column mapping,
/// we need table structure of source and target system.
///
/// Go: `type Rule struct`. The yaml/json/toml tags are `schema-pattern`,
/// `table-pattern`, `source-column`, `target-column`, `expression`,
/// `arguments` and `create-table-query`.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Rule {
    /// Go: `PatternSchema`.
    pub pattern_schema: String,
    /// Go: `PatternTable`.
    pub pattern_table: String,
    /// Go: `SourceColumn`; modify, add refer column, ignore.
    pub source_column: String,
    /// Go: `TargetColumn`; add column, modify.
    pub target_column: String,
    /// Go: `Expression`.
    pub expression: Option<Expr>,
    /// Go: `Arguments`.
    pub arguments: Vec<String>,
    /// Go: `CreateTableQuery`.
    pub create_table_query: String,
}

impl Rule {
    /// Go's `%+v` on a `*Rule`, used inside annotated error messages.
    ///
    /// boundary: Go renders `Arguments: nil` as `[]`; an empty
    /// [`Rule::arguments`] renders the same way, so a nil/empty distinction is
    /// not observable through this rendering.
    fn plus_v(&self) -> String {
        format!(
            "&{{PatternSchema:{} PatternTable:{} SourceColumn:{} TargetColumn:{} Expression:{} Arguments:{} CreateTableQuery:{}}}",
            self.pattern_schema,
            self.pattern_table,
            self.source_column,
            self.target_column,
            self.expression
                .as_ref()
                .map(Expr::as_str)
                .unwrap_or_default(),
            fmt_args(&self.arguments),
            self.create_table_query,
        )
    }

    /// Converts schema/table pattern to lower case. Go: `(*Rule).ToLower`.
    ///
    /// Note the Go doc comment reads "covert ... parttern"; the behaviour is
    /// ASCII/Unicode lowercasing of both patterns, and nothing else.
    pub fn to_lower(&mut self) {
        self.pattern_schema = self.pattern_schema.to_lowercase();
        self.pattern_table = self.pattern_table.to_lowercase();
    }

    /// Checks validity of rule. Go: `(*Rule).Valid`.
    ///
    /// - add prefix/suffix: it should have target column and one argument.
    /// - partition id: it should have 3 to 4 arguments.
    pub fn valid(&self) -> Result<(), ColumnMappingError> {
        // boundary: Go's `Rule.Expression` is a `string`, so the zero value is
        // `""`, which misses in `Exprs` and produces
        // `errors.NotFoundf("expression %s", "")`. `Option<Expr>` models the
        // unset field and renders the same empty text.
        let expr = self.expression.clone().unwrap_or_else(|| Expr::from(""));
        if exprs(&expr).is_none() {
            return Err(ColumnMappingError::NotFound(format!("expression {expr}")));
        }

        if self.target_column.is_empty() {
            return Err(ColumnMappingError::NotValid(
                "rule need to be applied a target column".to_owned(),
            ));
        }

        if (expr == Expr::AddPrefix || expr == Expr::AddSuffix) && self.arguments.len() != 1 {
            return Err(ColumnMappingError::NotValid(format!(
                "arguments {} for add prefix/suffix",
                fmt_args(&self.arguments)
            )));
        }

        if expr == Expr::PartitionID {
            return match self.arguments.len() {
                3 | 4 => Ok(()),
                // Note the Go message misspells "partition".
                _ => Err(ColumnMappingError::NotValid(format!(
                    "arguments {} for patition id",
                    fmt_args(&self.arguments)
                ))),
            };
        }

        Ok(())
    }

    /// Normalizes the rule into an easier-to-process form, e.g. filling in
    /// optional arguments with the default values. Go: `(*Rule).Adjust`.
    pub fn adjust(&mut self) {
        if self.expression.as_ref() == Some(&Expr::PartitionID) && self.arguments.len() == 3 {
            self.arguments.push(String::new());
        }
    }

    /// Check source and target position. Go: `(*Rule).adjustColumnPosition`.
    ///
    /// Quirk reproduced: Go takes `source` only to hand it straight back, and
    /// validates nothing about it; the only check is that a target column was
    /// found. The error also carries `source` and `target` unchanged through
    /// the named return values, so a Go caller that ignored the error would
    /// still see the original positions. Rust's `Result` cannot express that,
    /// and every Go caller propagates the error, so the values are dropped.
    fn adjust_column_position(
        &self,
        source: i64,
        target: i64,
    ) -> Result<(i64, i64), ColumnMappingError> {
        // if not found target, ignore it
        if target == -1 {
            return Err(ColumnMappingError::NotFound(format!(
                "target column {}",
                self.target_column
            )));
        }

        Ok((source, target))
    }
}

/// Go: `type mappingInfo struct`. Private in Go and only reachable through
/// [`Mapping`], but the upstream test constructs and compares it directly.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MappingInfo {
    /// Go: `ignore`. Set when no rule matched the schema/table.
    ignore: bool,
    /// Go: `sourcePosition`; `-1` when the source column is absent.
    source_position: i64,
    /// Go: `targetPosition`.
    target_position: i64,
    /// Go: `rule *Rule`; `None` is Go's `nil`, which the ignore path leaves
    /// unset.
    rule: Option<Rule>,

    /// Go: `instanceID`.
    instance_id: i64,
    /// Go: `schemaID`.
    schema_id: i64,
    /// Go: `tableID`.
    table_id: i64,
}

impl MappingInfo {
    /// Whether no rule matched, i.e. the row/DDL passes through unchanged.
    pub fn ignore(&self) -> bool {
        self.ignore
    }

    /// Position of the rule's source column, `-1` when absent.
    pub fn source_position(&self) -> i64 {
        self.source_position
    }

    /// Position of the rule's target column.
    pub fn target_position(&self) -> i64 {
        self.target_position
    }

    /// The matched rule, `None` on the ignore path.
    pub fn rule(&self) -> Option<&Rule> {
        self.rule.as_ref()
    }
}

/// Maps column to something by rules. Go: `type Mapping struct`.
///
/// Go embeds `selector.Selector`, promoting `Insert`/`Match`/`Remove`/
/// `AllRules` onto `Mapping`; the port implements
/// [`Selector<Rule>`](crate::table_rule_selector::Selector) for `Mapping` by
/// delegation, which is the same public surface.
pub struct Mapping {
    selector: TrieSelector<Rule>,
    case_sensitive: bool,
    // Go: `cache struct { sync.RWMutex; infos map[string]*mappingInfo }`.
    // Go shares one `*mappingInfo` between cache and caller; nothing mutates
    // an info after it is cached, so cloning on read is equivalent.
    cache: RwLock<HashMap<String, MappingInfo>>,
}

impl Mapping {
    /// Returns a column mapping. Go: `NewMapping`.
    ///
    /// boundary: Go takes `[]*Rule` and `AddRule` lowercases and adjusts each
    /// rule *in place*, so the caller's own rules are mutated. `&mut [Rule]`
    /// keeps that observable side effect (the upstream `TestQueryColumnInfo`
    /// compares the stored rule against the caller's slice element, which only
    /// matches because both were normalised).
    pub fn new(case_sensitive: bool, rules: &mut [Rule]) -> Result<Mapping, ColumnMappingError> {
        let m = Mapping {
            selector: new_trie_selector(),
            case_sensitive,
            cache: RwLock::new(HashMap::new()),
        };
        m.reset_cache();

        for rule in rules.iter_mut() {
            let described = rule.plus_v();
            m.add_rule(rule)
                .map_err(|err| err.annotate(format!("initial rule {described} in mapping")))?;
        }

        Ok(m)
    }

    /// Go: `(*Mapping).addOrUpdateRule`.
    ///
    /// boundary: Go short-circuits on a nil `*Mapping` receiver and on a nil
    /// `rule`, returning `nil`. Rust cannot call a method on an absent
    /// receiver and `&mut Rule` cannot be null, so both guards are unreachable
    /// rather than omitted.
    fn add_or_update_rule(
        &self,
        rule: &mut Rule,
        is_update: bool,
    ) -> Result<(), ColumnMappingError> {
        rule.valid()?;
        if !self.case_sensitive {
            rule.to_lower();
        }
        rule.adjust();

        self.reset_cache();
        let insert_type = if is_update {
            InsertType::Replace
        } else {
            InsertType::Insert
        };
        let res = self.selector.insert(
            &rule.pattern_schema,
            &rule.pattern_table,
            Some(rule.clone()),
            insert_type,
        );
        if let Err(err) = res {
            let method = if is_update { "update" } else { "add" };
            return Err(ColumnMappingError::from(err)
                .annotate(format!("{method} rule {} into mapping", rule.plus_v())));
        }

        Ok(())
    }

    /// Adds a rule into mapping. Go: `(*Mapping).AddRule`.
    pub fn add_rule(&self, rule: &mut Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, false)
    }

    /// Updates mapping rule. Go: `(*Mapping).UpdateRule`.
    pub fn update_rule(&self, rule: &mut Rule) -> Result<(), ColumnMappingError> {
        self.add_or_update_rule(rule, true)
    }

    /// Removes a rule from mapping. Go: `(*Mapping).RemoveRule`.
    ///
    /// Quirk reproduced: unlike [`Mapping::add_rule`] this lowercases the
    /// caller's rule but never calls `Adjust`, and it does not validate the
    /// rule either.
    pub fn remove_rule(&self, rule: &mut Rule) -> Result<(), ColumnMappingError> {
        if !self.case_sensitive {
            rule.to_lower();
        }

        self.reset_cache();
        self.selector
            .remove(&rule.pattern_schema, &rule.pattern_table)
            .map_err(|err| {
                ColumnMappingError::from(err)
                    .annotate(format!("remove rule {} from mapping", rule.plus_v()))
            })
    }

    /// Handles row value. Go: `(*Mapping).HandleRowValue`.
    ///
    /// Returns the (possibly rewritten) values and, when a rule applied, the
    /// `[sourcePosition, targetPosition]` pair. Go returns a nil `[]int` when
    /// no rule applied, which the caller can distinguish from a populated
    /// slice, so it is modelled as `Option`.
    ///
    /// boundary: Go returns `vals, nil, nil` for a nil `*Mapping` receiver;
    /// unreachable here.
    #[allow(clippy::type_complexity)]
    pub fn handle_row_value(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        vals: Vec<ColumnValue>,
    ) -> Result<(Vec<ColumnValue>, Option<[i64; 2]>), ColumnMappingError> {
        let (schema_l, table_l) = self.fold_case(schema, table);

        let info = self.query_column_info(&schema_l, &table_l, columns)?;
        if info.ignore {
            return Ok((vals, None));
        }

        let rule = info
            .rule
            .as_ref()
            .expect("a non-ignored mapping info always carries a rule");
        let expression = rule.expression.clone().unwrap_or_else(|| Expr::from(""));
        let Some(exp) = exprs(&expression) else {
            return Err(ColumnMappingError::NotFound(format!(
                "column mapping expression {expression}"
            )));
        };

        let vals = exp(&info, vals)?;

        Ok((vals, Some([info.source_position, info.target_position])))
    }

    /// Handles ddl. Go: `(*Mapping).HandleDDL`.
    ///
    /// Quirk reproduced: when a rule *does* match, Go resets the whole cache
    /// and then returns an error saying the DDL is not implemented. The cache
    /// reset is a side effect on an error path and is observable through a
    /// later [`Mapping::handle_row_value`] having to recompute its info.
    ///
    /// boundary: Go returns `statement, nil, err` on both error paths, i.e.
    /// the caller's own input echoed back beside the error; `Result` drops it,
    /// and no in-tree caller reads a returned statement together with a
    /// non-nil error.
    #[allow(clippy::type_complexity)]
    pub fn handle_ddl(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        statement: &str,
    ) -> Result<(String, Option<[i64; 2]>), ColumnMappingError> {
        let (schema_l, table_l) = self.fold_case(schema, table);

        let info = self.query_column_info(&schema_l, &table_l, columns)?;

        if info.ignore {
            return Ok((statement.to_owned(), None));
        }

        self.reset_cache();
        // only output erro now, wait fix it manually
        Err(ColumnMappingError::Message(format!(
            "ddl {statement} @ column mapping rule {schema}/{table}:{} not implemented",
            info.rule
                .as_ref()
                .expect("a non-ignored mapping info always carries a rule")
                .plus_v()
        )))
    }

    /// Go: the shared `schemaL, tableL := ...` prologue of `HandleRowValue`
    /// and `HandleDDL`.
    fn fold_case(&self, schema: &str, table: &str) -> (String, String) {
        if self.case_sensitive {
            (schema.to_owned(), table.to_owned())
        } else {
            (schema.to_lowercase(), table.to_lowercase())
        }
    }

    /// Go: `(*Mapping).queryColumnInfo`.
    fn query_column_info(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<MappingInfo, ColumnMappingError> {
        let key = table_name(schema, table);
        {
            let cache = self.cache.read().expect("mapping cache lock poisoned");
            if let Some(ci) = cache.get(&key) {
                return Ok(ci.clone());
            }
        }

        let info = MappingInfo {
            ignore: true,
            ..MappingInfo::default()
        };
        // Go's `Match` returns a nil `RuleSet` when nothing matched and the
        // code only tests `len(rules) == 0`, so both nil and empty take this
        // branch.
        let rules: RuleSet<Rule> = self.selector.match_rules(schema, table).unwrap_or_default();
        if rules.is_empty() {
            self.cache
                .write()
                .expect("mapping cache lock poisoned")
                .insert(key, info.clone());

            return Ok(info);
        }

        // classify rules into schema level rules and table level
        // table level rules have highest priority
        //
        // boundary: Go re-checks `rules[i].(*Rule)` and returns
        // `errors.NotValidf("column mapping rule %+v", ...)` for a foreign
        // rule type. The port's selector is generic in the rule type
        // (`TrieSelector<Rule>`), so a non-`Rule` entry cannot be inserted and
        // that error is unrepresentable rather than silently dropped.
        let mut schema_rules: Vec<&Rule> = Vec::new();
        let mut table_rules: Vec<&Rule> = Vec::with_capacity(1);
        for rule in &rules {
            if rule.pattern_table.is_empty() {
                schema_rules.push(rule);
            } else {
                table_rules.push(rule);
            }
        }

        // only support one expression for one table now, refine it later
        let rule: &Rule = if table.is_empty() || table_rules.is_empty() {
            if schema_rules.len() != 1 {
                return Err(ColumnMappingError::NotSupported(format!(
                    "`{schema}`.`{table}` matches {} schema column mapping rules which should be one. It's",
                    schema_rules.len()
                )));
            }

            schema_rules[0]
        } else {
            if table_rules.len() != 1 {
                return Err(ColumnMappingError::NotSupported(format!(
                    "`{schema}`.`{table}` matches {} table column mapping rules which should be one. It's",
                    table_rules.len()
                )));
            }

            table_rules[0]
        };
        // Quirk: Go follows this with `if rule == nil { ...cache ignore info,
        // return... }`. `rule` was just read out of a non-empty slice of
        // `*Rule` that only `AddRule` ever fills, and `AddRule` never inserts
        // a nil rule, so the branch is dead. It has no counterpart here
        // because `&Rule` cannot be null.

        // compute source and target column position
        let source_position = find_column_position(columns, &rule.source_column);
        let target_position = find_column_position(columns, &rule.target_column);

        let (source_position, target_position) =
            rule.adjust_column_position(source_position, target_position)?;

        let mut info = MappingInfo {
            ignore: false,
            source_position,
            target_position,
            rule: Some(rule.clone()),
            instance_id: 0,
            schema_id: 0,
            table_id: 0,
        };

        // if expr is partition ID, compute schema and table ID
        if rule.expression.as_ref() == Some(&Expr::PartitionID) {
            let (instance_id, schema_id, table_id) = compute_partition_id(schema, table, rule)?;
            info.instance_id = instance_id;
            info.schema_id = schema_id;
            info.table_id = table_id;
        }

        self.cache
            .write()
            .expect("mapping cache lock poisoned")
            .insert(key, info.clone());

        Ok(info)
    }

    /// Go: `(*Mapping).resetCache`.
    fn reset_cache(&self) {
        *self.cache.write().expect("mapping cache lock poisoned") = HashMap::new();
    }
}

/// Delegation for Go's embedded `selector.Selector` in `Mapping`.
impl Selector<Rule> for Mapping {
    fn insert(
        &self,
        schema: &str,
        table: &str,
        rule: Option<Rule>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        self.selector.insert(schema, table, rule, insert_type)
    }

    fn match_rules(&self, schema: &str, table: &str) -> Option<RuleSet<Rule>> {
        self.selector.match_rules(schema, table)
    }

    fn remove(&self, schema: &str, table: &str) -> Result<(), SelectorError> {
        self.selector.remove(schema, table)
    }

    fn all_rules(&self) -> (SchemaRules<Rule>, TableRules<Rule>) {
        self.selector.all_rules()
    }
}

/// Go: `findColumnPosition`. Returns `-1` when the column is absent.
fn find_column_position(cols: &[String], col: &str) -> i64 {
    for (i, c) in cols.iter().enumerate() {
        if c == col {
            return i as i64;
        }
    }

    -1
}

/// Go: `tableName`.
fn table_name(schema: &str, table: &str) -> String {
    format!("`{schema}`.`{table}`")
}

/// Go: `addPrefix`. Arguments contain the prefix.
fn add_prefix(
    info: &MappingInfo,
    mut vals: Vec<ColumnValue>,
) -> Result<Vec<ColumnValue>, ColumnMappingError> {
    let prefix = &info
        .rule
        .as_ref()
        .expect("addPrefix dereferences info.rule")
        .arguments[0];
    let pos = info.target_position as usize;
    let ColumnValue::Str(origin_str) = &vals[pos] else {
        return Err(ColumnMappingError::NotValid(format!(
            "column {} value is not string, but {}, which is",
            info.target_position, vals[pos]
        )));
    };

    // fast to concatenated string
    let mut raw_byte = String::with_capacity(prefix.len() + origin_str.len());
    raw_byte.push_str(prefix);
    raw_byte.push_str(origin_str);

    vals[pos] = ColumnValue::Str(raw_byte);
    Ok(vals)
}

/// Go: `addSuffix`. Arguments contain the suffix.
fn add_suffix(
    info: &MappingInfo,
    mut vals: Vec<ColumnValue>,
) -> Result<Vec<ColumnValue>, ColumnMappingError> {
    let suffix = &info
        .rule
        .as_ref()
        .expect("addSuffix dereferences info.rule")
        .arguments[0];
    let pos = info.target_position as usize;
    let ColumnValue::Str(origin_str) = &vals[pos] else {
        return Err(ColumnMappingError::NotValid(format!(
            "column {} value is not string, but {}, which is",
            info.target_position, vals[pos]
        )));
    };

    let mut raw_byte = String::with_capacity(suffix.len() + origin_str.len());
    raw_byte.push_str(origin_str);
    raw_byte.push_str(suffix);

    vals[pos] = ColumnValue::Str(raw_byte);
    Ok(vals)
}

/// Go: `partitionID`.
///
/// Quirk reproduced: Go's type switch lists `int`, `int8`, `int32`, `int64`,
/// `uint`, `uint16`, `uint32`, `uint64` and `string` — it skips `int16` and
/// `uint8`, so values of those two types fall into `default:` and are rejected
/// as invalid even though every neighbouring width is accepted. The port keeps
/// them as distinct [`ColumnValue`] variants that take the `default:` path.
///
/// The second quirk: only a `string` input produces a `string` output; every
/// integer width collapses to `int64` on the way out.
fn partition_id(
    info: &MappingInfo,
    mut vals: Vec<ColumnValue>,
) -> Result<Vec<ColumnValue>, ColumnMappingError> {
    // only int64 now
    let pos = info.target_position as usize;
    let mut is_chars = false;
    let mut origin_id: i64 = match &vals[pos] {
        ColumnValue::Int(v) => *v,
        ColumnValue::Int8(v) => i64::from(*v),
        ColumnValue::Int32(v) => i64::from(*v),
        ColumnValue::Int64(v) => *v,
        ColumnValue::Uint(v) => *v as i64,
        ColumnValue::Uint16(v) => i64::from(*v),
        ColumnValue::Uint32(v) => i64::from(*v),
        ColumnValue::Uint64(v) => *v as i64,
        ColumnValue::Str(raw_id) => {
            let Ok(parsed) = parse_int(raw_id) else {
                return Err(ColumnMappingError::NotValid(format!(
                    "column {} value is not int, but {}, which is",
                    info.target_position, vals[pos]
                )));
            };
            is_chars = true;
            parsed
        }
        other @ (ColumnValue::Int16(_) | ColumnValue::Uint8(_) | ColumnValue::Other { .. }) => {
            return Err(ColumnMappingError::NotValid(format!(
                "type {}({})",
                other.type_name(),
                other
            )));
        }
    };

    let max = max_origin_id();
    if origin_id >= max || origin_id < 0 {
        return Err(ColumnMappingError::NotValid(format!(
            "id must less than {max}, greater than or equal to 0, but got {origin_id}, which is"
        )));
    }

    // Go: `originID = info.instanceID | info.schemaID | info.tableID | originID`.
    origin_id |= info.instance_id | info.schema_id | info.table_id;
    if is_chars {
        vals[pos] = ColumnValue::Str(origin_id.to_string());
    } else {
        vals[pos] = ColumnValue::Int64(origin_id);
    }

    Ok(vals)
}

/// Go: `strconv.ParseInt(s, 10, 64)`.
fn parse_int(s: &str) -> Result<i64, ()> {
    let (sign, digits) = match s.strip_prefix('+') {
        Some(rest) => (1i64, rest),
        None => match s.strip_prefix('-') {
            Some(rest) => (-1i64, rest),
            None => (1i64, s),
        },
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(());
    }
    let magnitude: i128 = digits.parse::<i128>().map_err(|_| ())?;
    let value = magnitude * i128::from(sign);
    i64::try_from(value).map_err(|_| ())
}

/// Go: `strconv.ParseUint(s, 10, bitSize)`. A sign prefix is not permitted and
/// the value must fit in `bitSize` bits.
fn parse_uint(s: &str, bit_size: i64) -> Result<u64, ()> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(());
    }
    let value: u128 = s.parse::<u128>().map_err(|_| ())?;
    // Go treats `bitSize == 0` as 64; every caller here guards `bitSize > 0`.
    let bits = if bit_size == 0 { 64 } else { bit_size };
    if !(1..=64).contains(&bits) {
        return Err(());
    }
    let limit: u128 = 1u128 << bits;
    if value >= limit {
        return Err(());
    }
    u64::try_from(value).map_err(|_| ())
}

/// Go: `computePartitionID`.
///
/// boundary: Go uses named return values, so an error path still hands the
/// caller whatever `instanceID`/`schemaID`/`tableID` had been computed so far.
/// The only in-tree caller (`queryColumnInfo`) discards them on error, as does
/// the upstream test, so `Result<(i64, i64, i64), _>` loses nothing observable.
///
/// Reads the process-wide bit widths set by [`set_partition_rule`].
fn compute_partition_id(
    schema: &str,
    table: &str,
    rule: &Rule,
) -> Result<(i64, i64, i64), ColumnMappingError> {
    let mut instance_id: i64 = 0;
    let mut schema_id: i64 = 0;
    let mut table_id: i64 = 0;

    let instance_bits = instance_id_bit_size();
    let schema_bits = schema_id_bit_size();
    let table_bits = table_id_bit_size();

    let mut shift_cnt: i64 = 63;
    if instance_bits > 0 && !rule.arguments[0].is_empty() {
        shift_cnt -= instance_bits;
        let Ok(instance_id_unsign) = parse_uint(&rule.arguments[0], instance_bits) else {
            // Go returns `strconv`'s own error here rather than one of the
            // `errors.*f` wrappers, e.g.
            // `strconv.ParseUint: parsing "test": invalid syntax`.
            return Err(ColumnMappingError::Message(format!(
                "strconv.ParseUint: parsing {:?}: invalid syntax",
                rule.arguments[0]
            )));
        };
        instance_id = shift_left(instance_id_unsign, shift_cnt);
    }

    // Go indexes `rule.Arguments[3]` unconditionally, so a rule that reaches
    // this line with fewer than four arguments panics. `Rule::adjust` pads a
    // three-argument partition-id rule to four, and every path that skips
    // `Adjust` errors out above; Rust's slice indexing panics identically.
    let sep = &rule.arguments[3];

    if schema_bits > 0 && !rule.arguments[1].is_empty() {
        shift_cnt -= schema_bits;
        schema_id = compute_id(schema, &rule.arguments[1], sep, schema_bits, shift_cnt)?;
    }

    if table_bits > 0 && !rule.arguments[2].is_empty() {
        shift_cnt -= table_bits;
        table_id = compute_id(table, &rule.arguments[2], sep, table_bits, shift_cnt)?;
    }

    Ok((instance_id, schema_id, table_id))
}

/// Go: `int64(x << shiftCount)` where `x` is a `uint64`. A shift count outside
/// `0..64` cannot occur for any in-range bit-width configuration; Go would
/// wrap the count into a huge `uint` and yield `0`, so that is what is
/// produced here rather than a panic.
fn shift_left(value: u64, shift: i64) -> i64 {
    if !(0..64).contains(&shift) {
        return 0;
    }
    (value << shift) as i64
}

/// Go: `computeID`.
///
/// Quirk reproduced: the guard is `len(prefix) >= len(name)`, so a `name` that
/// equals `prefix + sep` exactly (e.g. name `t_`, prefix `t`, sep `_`) is
/// rejected with "t_ is not the prefix of t_" — the upstream test pins that
/// message and annotates it with "needs a better error messag[e]".
fn compute_id(
    name: &str,
    prefix: &str,
    sep: &str,
    bit_size: i64,
    shift_count: i64,
) -> Result<i64, ColumnMappingError> {
    if name == prefix {
        return Ok(0);
    }

    let prefix = format!("{prefix}{sep}");
    // Go slices `name[:len(prefix)]` on bytes; comparing the byte prefix is
    // the same test and cannot split a UTF-8 boundary because the length check
    // above already guarantees `len(prefix) < len(name)`.
    if prefix.len() >= name.len() || prefix.as_bytes() != &name.as_bytes()[..prefix.len()] {
        return Err(ColumnMappingError::NotValid(format!(
            "{prefix} is not the prefix of {name}"
        )));
    }

    let id_str = &name[prefix.len()..];
    let Ok(id) = parse_uint(id_str, bit_size) else {
        return Err(ColumnMappingError::NotValid(format!(
            "the suffix of {id_str} can't be converted to int64"
        )));
    };

    Ok(shift_left(id, shift_count))
}

/// Transcreated from `pkg/util/column-mapping/column_test.go`, the only
/// upstream coverage of this package: `TestRule`, `TestHandle`,
/// `TestQueryColumnInfo`, `TestSetPartitionRule`, `TestComputePartitionID`,
/// `TestPartitionID` and `TestCaseSensitive`. No extra tests are written here.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    /// The partition-id bit widths are process-wide in Go too, but `go test`
    /// runs the functions of one package sequentially, so upstream needs no
    /// lock. Rust's harness runs tests in parallel within one binary, so every
    /// test that reads or writes those globals takes this lock.
    static PARTITION_RULE_LOCK: Mutex<()> = Mutex::new(());

    fn lock_partition_rule() -> MutexGuard<'static, ()> {
        PARTITION_RULE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    /// Go's positional `&Rule{...}` literal: schema pattern, table pattern,
    /// source column, target column, expression, arguments, create query.
    fn rule(
        pattern_schema: &str,
        pattern_table: &str,
        source_column: &str,
        target_column: &str,
        expression: Expr,
        arguments: Option<Vec<String>>,
        create_table_query: &str,
    ) -> Rule {
        Rule {
            pattern_schema: pattern_schema.to_owned(),
            pattern_table: pattern_table.to_owned(),
            source_column: source_column.to_owned(),
            target_column: target_column.to_owned(),
            expression: Some(expression),
            // Go's literal passes a nil `[]string`, which behaves like an
            // empty one everywhere this package touches it.
            arguments: arguments.unwrap_or_default(),
            create_table_query: create_table_query.to_owned(),
        }
    }

    /// Go: `TestRule`.
    #[test]
    fn test_rule() {
        // test invalid rules
        let mut in_valid_rule = rule(
            "test*",
            "abc*",
            "id",
            "id",
            Expr::from("Error"),
            None,
            "xxx",
        );
        assert!(in_valid_rule.valid().is_err());

        in_valid_rule.target_column = String::new();
        assert!(in_valid_rule.valid().is_err());

        in_valid_rule.expression = Some(Expr::AddPrefix);
        in_valid_rule.target_column = "id".to_owned();
        assert!(in_valid_rule.valid().is_err());

        in_valid_rule.arguments = args(&["1"]);
        assert!(in_valid_rule.valid().is_ok());

        in_valid_rule.expression = Some(Expr::PartitionID);
        assert!(in_valid_rule.valid().is_err());

        in_valid_rule.arguments = args(&["1", "test_", "t_"]);
        assert!(in_valid_rule.valid().is_ok());
    }

    /// Go: `TestHandle`.
    #[test]
    fn test_handle() {
        let mut rules = vec![rule(
            "Test*",
            "xxx*",
            "",
            "id",
            Expr::AddPrefix,
            Some(args(&["instance_id:"])),
            "xx",
        )];

        // initial column mapping
        let m = Mapping::new(false, &mut rules).expect("new mapping");
        assert_eq!(m.cache.read().expect("cache").len(), 0);

        // test add prefix, add suffix is similar
        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &cols(&["age", "id"]),
                vec![ColumnValue::Int(1), ColumnValue::Str("1".to_owned())],
            )
            .expect("handle row value");
        assert_eq!(
            vals,
            vec![
                ColumnValue::Int(1),
                ColumnValue::Str("instance_id:1".to_owned())
            ]
        );
        assert_eq!(poss, Some([-1, 1]));

        // test cache
        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &cols(&["name"]),
                vec![ColumnValue::Int(1), ColumnValue::Str("1".to_owned())],
            )
            .expect("cached handle row value");
        assert_eq!(
            vals,
            vec![
                ColumnValue::Int(1),
                ColumnValue::Str("instance_id:1".to_owned())
            ]
        );
        assert_eq!(poss, Some([-1, 1]));

        // test resetCache
        m.reset_cache();
        assert!(m
            .handle_row_value(
                "test",
                "xxx",
                &cols(&["name"]),
                vec![ColumnValue::Str("1".to_owned())],
            )
            .is_err());

        // test DDL
        assert!(m
            .handle_ddl("test", "xxx", &cols(&["id", "age"]), "create table xxx")
            .is_err());

        let (statement, poss) = m
            .handle_ddl("abc", "xxx", &cols(&["id", "age"]), "create table xxx")
            .expect("unmatched ddl passes through");
        assert_eq!(statement, "create table xxx");
        assert_eq!(poss, None);
    }

    /// Go: `TestQueryColumnInfo`.
    #[test]
    fn test_query_column_info() {
        let _guard = lock_partition_rule();
        set_partition_rule(4, 7, 8);
        let mut rules = vec![rule(
            "test*",
            "xxx*",
            "",
            "id",
            Expr::PartitionID,
            Some(args(&["8", "test_", "xxx_"])),
            "xx",
        )];

        // initial column mapping
        let m = Mapping::new(false, &mut rules).expect("new mapping");

        // test mismatch
        let info = m
            .query_column_info("test_2", "t_1", &cols(&["id", "name"]))
            .expect("mismatch query");
        assert!(info.ignore);

        // test matched
        let info = m
            .query_column_info("test_2", "xxx_1", &cols(&["id", "name"]))
            .expect("matched query");
        assert_eq!(
            info,
            MappingInfo {
                ignore: false,
                source_position: -1,
                target_position: 0,
                rule: Some(rules[0].clone()),
                instance_id: 8i64 << 59,
                schema_id: 2i64 << 52,
                table_id: 1i64 << 44,
            }
        );

        m.reset_cache();
        set_partition_rule(0, 0, 3);
        let info = m
            .query_column_info("test_2", "xxx_1", &cols(&["id", "name"]))
            .expect("matched query after re-configuring the partition rule");
        assert_eq!(
            info,
            MappingInfo {
                ignore: false,
                source_position: -1,
                target_position: 0,
                rule: Some(rules[0].clone()),
                instance_id: 0,
                schema_id: 0,
                table_id: 1i64 << 60,
            }
        );
    }

    /// Go: `TestSetPartitionRule`.
    #[test]
    fn test_set_partition_rule() {
        let _guard = lock_partition_rule();
        set_partition_rule(4, 7, 8);
        assert_eq!(instance_id_bit_size(), 4);
        assert_eq!(schema_id_bit_size(), 7);
        assert_eq!(table_id_bit_size(), 8);
        assert_eq!(max_origin_id(), 1i64 << 44);

        set_partition_rule(0, 3, 4);
        assert_eq!(instance_id_bit_size(), 0);
        assert_eq!(schema_id_bit_size(), 3);
        assert_eq!(table_id_bit_size(), 4);
        assert_eq!(max_origin_id(), 1i64 << 56);
    }

    /// Go: `TestComputePartitionID`.
    #[test]
    fn test_compute_partition_id() {
        let _guard = lock_partition_rule();
        set_partition_rule(4, 7, 8);

        let mut r = Rule {
            arguments: args(&["test", "t"]),
            ..Rule::default()
        };
        assert!(compute_partition_id("test_1", "t_1", &r).is_err());
        assert!(compute_partition_id("test", "t", &r).is_err());

        r = Rule {
            arguments: args(&["2", "test", "t", "_"]),
            ..Rule::default()
        };
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_1", "t_1", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 1i64 << 52);
        assert_eq!(table_id, 1i64 << 44);

        // test default partition ID to zero
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test", "t_3", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 0);
        assert_eq!(table_id, 3i64 << 44);

        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_5", "t", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 5i64 << 52);
        assert_eq!(table_id, 0);

        let err = compute_partition_id("unrelated", "t_6", &r).expect_err("prefix mismatch");
        assert!(
            err.to_string()
                .starts_with("test_ is not the prefix of unrelated"),
            "{err}"
        );

        let err = compute_partition_id("test", "x", &r).expect_err("prefix mismatch");
        assert!(
            err.to_string().starts_with("t_ is not the prefix of x"),
            "{err}"
        );

        let err = compute_partition_id("test_0", "t_0xa", &r).expect_err("bad suffix");
        assert!(
            err.to_string()
                .starts_with("the suffix of 0xa can't be converted to int64"),
            "{err}"
        );

        let err = compute_partition_id("test_0", "t_", &r).expect_err("prefix mismatch");
        // needs a better error messag
        assert!(
            err.to_string().starts_with("t_ is not the prefix of t_"),
            "{err}"
        );

        let err = compute_partition_id("testx", "t_3", &r).expect_err("prefix mismatch");
        assert!(
            err.to_string()
                .starts_with("test_ is not the prefix of testx"),
            "{err}"
        );

        set_partition_rule(4, 0, 8);
        r = Rule {
            arguments: args(&["2", "test_", "t_", ""]),
            ..Rule::default()
        };
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_1", "t_1", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 0);
        assert_eq!(table_id, 1i64 << 51);

        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_", "t_", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 0);
        assert_eq!(table_id, 0);

        // test ignore instance ID
        set_partition_rule(4, 7, 8);
        r = Rule {
            arguments: args(&["", "test_", "t_", ""]),
            ..Rule::default()
        };
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_1", "t_1", &r).expect("compute");
        assert_eq!(instance_id, 0);
        assert_eq!(schema_id, 1i64 << 56);
        assert_eq!(table_id, 1i64 << 48);

        // test ignore schema ID
        r = Rule {
            arguments: args(&["2", "", "t_", ""]),
            ..Rule::default()
        };
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_1", "t_1", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 0);
        assert_eq!(table_id, 1i64 << 51);

        // test ignore schema ID
        r = Rule {
            arguments: args(&["2", "test_", "", ""]),
            ..Rule::default()
        };
        let (instance_id, schema_id, table_id) =
            compute_partition_id("test_1", "t_1", &r).expect("compute");
        assert_eq!(instance_id, 2i64 << 59);
        assert_eq!(schema_id, 1i64 << 52);
        assert_eq!(table_id, 0);
    }

    /// Go: `TestPartitionID`.
    #[test]
    fn test_partition_id() {
        let _guard = lock_partition_rule();
        set_partition_rule(4, 7, 8);
        let mut info = MappingInfo {
            instance_id: 2i64 << 59,
            schema_id: 1i64 << 52,
            table_id: 1i64 << 44,
            target_position: 1,
            ..MappingInfo::default()
        };

        // test wrong type
        assert!(partition_id(
            &info,
            vec![ColumnValue::Int(1), ColumnValue::Str("ha".to_owned())]
        )
        .is_err());

        // test exceed maxOriginID
        assert!(partition_id(
            &info,
            vec![ColumnValue::Str("ha".to_owned()), ColumnValue::Int(1 << 44)]
        )
        .is_err());

        let vals = partition_id(
            &info,
            vec![ColumnValue::Str("ha".to_owned()), ColumnValue::Int(1)],
        )
        .expect("partition id");
        assert_eq!(
            vals,
            vec![
                ColumnValue::Str("ha".to_owned()),
                ColumnValue::Int64(2i64 << 59 | 1i64 << 52 | 1i64 << 44 | 1)
            ]
        );

        info.instance_id = 0;
        let vals = partition_id(
            &info,
            vec![
                ColumnValue::Str("ha".to_owned()),
                ColumnValue::Str("123".to_owned()),
            ],
        )
        .expect("partition id");
        assert_eq!(
            vals,
            vec![
                ColumnValue::Str("ha".to_owned()),
                ColumnValue::Str(format!("{}", 1i64 << 52 | 1i64 << 44 | 123))
            ]
        );
    }

    /// Go: `TestCaseSensitive`.
    #[test]
    fn test_case_sensitive() {
        // we test case insensitive in TestHandle
        let mut rules = vec![rule(
            "Test*",
            "xxx*",
            "",
            "id",
            Expr::AddPrefix,
            Some(args(&["instance_id:"])),
            "xx",
        )];

        // case sensitive
        // initial column mapping
        let m = Mapping::new(true, &mut rules).expect("new mapping");
        assert_eq!(m.cache.read().expect("cache").len(), 0);

        // test add prefix, add suffix is similar
        let (vals, poss) = m
            .handle_row_value(
                "test",
                "xxx",
                &cols(&["age", "id"]),
                vec![ColumnValue::Int(1), ColumnValue::Str("1".to_owned())],
            )
            .expect("handle row value");
        assert_eq!(
            vals,
            vec![ColumnValue::Int(1), ColumnValue::Str("1".to_owned())]
        );
        assert_eq!(poss, None);
    }
}
