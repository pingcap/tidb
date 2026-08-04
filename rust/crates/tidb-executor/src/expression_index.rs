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

//! An index key part that is an EXPRESSION rather than a column:
//! `CREATE INDEX idx ON t((a + 1))`.
//!
//! Mirrors Go `pkg/ddl/create_table.go`'s `BuildHiddenColumnInfo` and
//! `precheckBuildHiddenColumnInfo`, plus the `illegalFunctionChecker` half of
//! `pkg/ddl/generated_column.go`.
//!
//! # There is no expression-index machinery, because Go has none either
//!
//! TiDB does not store expressions in index metadata. It rewrites
//! `((a + 1))` into a HIDDEN VIRTUAL GENERATED COLUMN named
//! `_V$_<index name>_<part>` holding `a + 1`, and indexes THAT column. Every
//! later step -- writing index entries, maintaining them across `UPDATE` and
//! `DELETE`, reading them back, `ADMIN CHECK TABLE` -- is the ordinary
//! generated-column path in [`crate::generated_column`], which already writes
//! index entries from the materialized row. So this module builds a column
//! and nothing else; there is deliberately no second code path to keep in
//! step with the first.
//!
//! What that DOES cost is one invariant, enforced in [`crate::kv_table`]: a
//! hidden column exists in the row and in index offsets but must appear in no
//! user-visible column enumeration -- not `SELECT *`, not an `INSERT`'s
//! arity, not `SHOW CREATE TABLE`, `DESC`, or `information_schema.COLUMNS`.
//! `KvTable` keeps hidden columns as a contiguous TAIL and records their
//! count, so "the visible columns" is a prefix slice and a visible offset IS
//! a physical offset. That removes the mapping an interior hidden column
//! would have forced between the two numberings -- and a mapping that can be
//! forgotten at one call site is exactly how a hidden column leaks into an
//! answer.
//!
//! # Captured from Go (`difftests/gorun`), not assumed
//!
//! ```text
//! create table t (a int, b int); create index idx on t((a+1));
//! show create table t   ->  KEY `idx` ((`a` + 1))        -- not the column
//! select * from t       ->  1|2                          -- two columns
//! desc t                ->  a, b                         -- two rows
//! information_schema.columns for `te` -> a|1, z|2        -- hidden absent
//! insert into te values (5)  -> 1136 when te has 2 visible columns
//! show index from te    ->  Column_name NULL, Expression `a` + 1
//! admin check table t   ->  OK after INSERT, UPDATE, DELETE and REPLACE
//! alter table t drop index idx; show create table t -> hidden column gone
//! create table t2 (a int, index ((a+1))) -> KEY `expression_index`, then
//!                                           `expression_index_2`
//! ```
//!
//! and the refusals, each with the errno `gorun` reported:
//!
//! ```text
//! index ((a))                  3762  Expression index on a column is not supported.
//! index i ((rand()))           3758  ... contains a disallowed function
//! index i ((a+@@max_connections)) 3758
//! index i ((values(a)))        3758
//! index i ((abs(a)))           8200  Unsupported creating expression index containing
//!                                    unsafe functions without allow-expression-index
//! index i ((sum(a)))           1111  Invalid use of group function
//! index i (((a,a)))            3800  ... cannot refer to a row value
//! index i ((zz+1))             1054  Unknown column 'zz' in 'expression'
//! index i ((a+1)) on auto_inc  3754  ... cannot refer to an auto-increment column
//! a user column named `_V$_i_0` 1060 Duplicate column name
//! ```
//!
//! `index i ((lower(a)))` is ACCEPTED: Go admits a function call in an
//! expression index only from the `GAFunction4ExpressionIndex` whitelist
//! (`pkg/sessionctx/variable/varsutil.go`), everything else being 8200 while
//! `allow-expression-index` is off, which is the default.
//!
//! # The gate is a LIST, and an ORDER
//!
//! Two things decide what an expression index is allowed to be, and neither
//! is a principle that can be re-derived:
//!
//! * WHICH function names pass, which is [`GA_FUNCTIONS`] transcribed from
//!   Go verbatim. `lower` is on it and `lcase` is not, though they are the
//!   same builtin under two names -- captured, `((lcase(a)))` is 8200 and
//!   `((lower(a)))` is accepted. Any reasoning about which functions are
//!   "safe" gets that pair wrong.
//! * WHICH ERROR an expression that trips several rules reports. Go's
//!   `illegalFunctionChecker` COLLECTS flags over the whole key part and
//!   `checkIllegalFn4Generated` reports them in a fixed order -- 3758, 1111,
//!   3800, 3593, the arity failure, and only THEN 8200. So `((abs(a) +
//!   sum(a)))` is 1111 and `((abs(rand())))` is 3758, captured both ways.
//!   A walk that returns at the first offending node answers by TREE
//!   POSITION instead, which is a different function of the same input;
//!   [`AdmissibilityScan`] is a scan for that reason.
//!
//! Two SHAPES escape the gate entirely, and the port has to escape with
//! them:
//!
//! * A GENERATED COLUMN. Go passes `typeColumn` there and the 8200 arm is
//!   `genType == typeIndex` only, so `c char(20) as (lcase(a))` is accepted
//!   -- and so is a plain `KEY (c)` over it afterwards. Captured.
//! * A COLUMNAR (VECTOR / FULLTEXT) INDEX. `pkg/ddl/create_table.go` guards
//!   the hidden-column build with `if constr.Tp != ast.ConstraintColumnar`,
//!   so `VECTOR INDEX vi((vec_cosine_distance(v, '[1,2,3]')))` never meets
//!   the GA list even though `vec_cosine_distance` is not on it. Captured:
//!   that statement fails for a MISSING TIFLASH REPLICA, not for 8200,
//!   while `create index i on t((vec_cosine_distance(v,'[1,2,3]')))` --
//!   an ordinary index over the same call -- IS 8200.
//!
//! # The SECOND gate: the result TYPE
//!
//! Passing the function gate is not enough. `pkg/ddl/index.go`'s
//! `checkIndexColumn` runs over every key part `buildIndexColumns` builds --
//! the hidden column included -- and four of its arms report a different
//! error when `col.Hidden` is set: 3761 for 1167, 3753 for 3152, 3757 for
//! 1170. [`crate::ddl::index_prefix::stored_index_length`] is that function,
//! and [`build_hidden_columns`] runs it here.
//!
//! What makes it a rule of its own rather than a widening of the function
//! gate is that it reads a TYPE. Fifteen GA functions are refused by it --
//! the thirteen with a JSON result as 3753 and `json_unquote`/`json_pretty`
//! as 3757 -- but so is `CAST(x AS JSON)`, which is no function call at all,
//! while `json_extract(j,'$.a')+0` is ACCEPTED because the arithmetic makes
//! the result a bigint. Captured all three ways.
//!
//! The type it reads is Go's, not this crate's. A JSON-returning builtin is
//! typed `VarString` here -- there is no BinaryJSON cell to hold a JSON value
//! in, see [`tidb_expr::rewriter::go_result_type_code`] -- so reading
//! `static_type()` straight would answer "not JSON" for `json_extract` and
//! accept the index TiDB refuses. [`go_result_type`] is the one place that
//! divergence is undone.
//!
//! ## The WIDTH half of the same gate
//!
//! Three more ways to reach it read the hidden column's flen rather than its
//! family. They need Go's argument-driven width -- a string builtin copies
//! `args[0]`'s flen onto its result and `baseBuiltinFunc.getRetTp` re-types a
//! wide one as MEDIUM/LONG blob, see
//! [`tidb_expr::rewriter`]'s result-type table -- and all three now answer
//! Go's code:
//!
//! ```text
//! index i((lower(mt)))   -- MEDIUMTEXT arg -> mediumblob result  -> 3757
//! index i((lower(t)))    -- TEXT arg -> var_string(65535)        -> 1071
//! index i((lower(v)))    -- varchar(0) arg -> var_string(0)      -> 3761
//! ```
//!
//! The TEXT row is the one that shows the rule is the WIDTH and not the
//! argument's family: 65535 is one short of `getRetTp`'s MEDIUM boundary, so
//! the result is no blob at all and the refusal is the index-too-long 1071.
//!
//! `CAST(... AS ... ARRAY)` is the remaining shape, and it is the opposite
//! direction: Go ACCEPTS it as a multi-valued index, whose hidden column is
//! JSON with `IsArray` set -- which is exactly why the 3753 arm tests
//! `!col.FieldType.IsArray()`. This tier declines it earlier, at 1105, so the
//! array arm is written to Go's rule and unreachable until multi-valued
//! indexes land.
//!
//! Two smaller residuals, both the safe direction (this tier refuses what Go
//! accepts, never the reverse): `MATCH ... AGAINST` and `DEFAULT(a)` are
//! ACCEPTED by Go's checker -- neither is a `FuncCallExpr` -- and are
//! declined here. And a name that is no builtin at all is 3758 in Go
//! (`expression.IsFunctionSupported`) where this answers 8200, which needs
//! the whole `funcs` registry to tell from a builtin merely off the GA list.

use tidb_ast::{Expr, IndexPart};
use tidb_datatype::FieldType;

use crate::driver::DriverError;
use crate::generated_column::{GeneratedColumn, TableColumnResolver};

/// Go `pkg/ddl/executor.go`'s `expressionIndexPrefix`.
const HIDDEN_COLUMN_PREFIX: &str = "_V$";

/// Go `mysql.MaxColumnNameLength`.
const MAX_COLUMN_NAME_LENGTH: usize = 64;

/// The hidden column an expression key part is rewritten into.
pub struct HiddenIndexColumn {
    /// The generated name, `_V$_<index name>_<part index>`.
    pub name: String,
    /// The column's type, which is the expression's own result type.
    pub field_type: FieldType,
    /// The virtual generation that computes the indexed value.
    pub generated: GeneratedColumn,
}

/// The functions Go allows a FUNCTION CALL in an expression index to be:
/// `variable.GAFunction4ExpressionIndex`
/// (`pkg/sessionctx/variable/varsutil.go`), paired with the argument count
/// its `baseFunctionClass{minArgs, maxArgs}` admits
/// (`pkg/expression/builtin.go`'s `funcs` map, read through
/// `VerifyArgsWrapper`). `None` is Go's `maxArgs == -1`, "no upper bound".
///
/// A call outside this list is 8200 unless the server was started with
/// `allow-expression-index`, which defaults off and which this tier does not
/// model as settable, so the list IS the gate here. The list is transcribed
/// VERBATIM rather than derived from a principle: an earlier attempt to drop
/// an entry as redundant was falsified by the Go source, which lists it.
const GA_FUNCTIONS: &[(&str, usize, Option<usize>)] = &[
    ("lower", 1, Some(1)),
    ("upper", 1, Some(1)),
    ("md5", 1, Some(1)),
    ("reverse", 1, Some(1)),
    ("vitess_hash", 1, Some(1)),
    ("tidb_shard", 1, Some(1)),
    ("json_type", 1, Some(1)),
    ("json_extract", 2, None),
    ("json_unquote", 1, Some(1)),
    ("json_array", 0, None),
    ("json_object", 0, None),
    ("json_set", 3, None),
    ("json_insert", 3, None),
    ("json_replace", 3, None),
    ("json_remove", 2, None),
    ("json_contains", 2, Some(3)),
    ("json_contains_path", 3, None),
    ("json_valid", 1, Some(1)),
    ("json_array_append", 3, None),
    ("json_array_insert", 3, None),
    ("json_merge_patch", 2, None),
    ("json_merge_preserve", 2, None),
    ("json_pretty", 1, Some(1)),
    ("json_quote", 1, Some(1)),
    ("json_schema_valid", 2, Some(2)),
    ("json_search", 3, None),
    ("json_storage_size", 1, Some(1)),
    ("json_depth", 1, Some(1)),
    ("json_keys", 1, Some(2)),
    ("json_length", 1, Some(2)),
];

/// Go `expression.IllegalFunctions4GeneratedColumns`
/// (`pkg/expression/function_traits.go`), transcribed VERBATIM. Every name on
/// it is 3758 in an expression index -- reported BEFORE the 8200 GA gate, so
/// `abs(rand())` is 3758 and not 8200 even though `abs` is the outer call.
///
/// The list is what makes an expression index deterministic: each name either
/// reads the clock (`now`, `curdate`, `sysdate`), the session (`user`,
/// `database`, `connection_id`, `@x` via `get_var`), or an entropy source
/// (`rand`, `uuid`) -- values that would be frozen into the stored key at
/// write time and no longer match on the way back out.
const ILLEGAL_FUNCTIONS: &[&str] = &[
    "benchmark",
    "connection_id",
    "curdate",
    "current_date",
    "current_resource_group",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "curtime",
    "database",
    "found_rows",
    "get_lock",
    "getvar",
    "is_free_lock",
    "is_used_lock",
    "json_merge",
    "last_insert_id",
    "load_file",
    "localtime",
    "localtimestamp",
    "name_const",
    "now",
    "rand",
    "random_bytes",
    "release_all_locks",
    "release_lock",
    "row_count",
    "row",
    "schema",
    "session_user",
    "setvar",
    "sleep",
    "sysdate",
    "system_user",
    "tidb_bounded_staleness",
    "tidb_current_tso",
    "tidb_is_ddl_owner",
    "tidb_row_checksum",
    "tidb_version",
    "unix_timestamp",
    "user",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    "uuid",
    "uuid_v4",
    "uuid_v7",
    "uuid_short",
    "values",
    "version",
];

/// Go `illegalFunctionChecker` for `typeIndex`: one pass that COLLECTS every
/// flag the expression trips, which `checkIllegalFn4Generated` then reports in
/// a FIXED order.
///
/// The order is the whole point, and it is why this is a scan and not an
/// early return. `((abs(a) + sum(a)))` is 1111 and not 8200 -- Go sees both
/// the aggregate and the non-GA call, and reports the aggregate, whichever
/// came first in the tree. An early-returning walk answers by TREE POSITION
/// instead, which is a different function of the same input.
#[derive(Default)]
struct AdmissibilityScan {
    /// Go `hasIllegalFunc`: a blocked function, a subquery, `values(x)`, or a
    /// variable. Reported first, as 3758.
    illegal_func: bool,
    /// Go `hasAggFunc`: an aggregate or `GROUPING()`. Reported as 1111.
    agg_func: bool,
    /// Go `hasRowVal`: a row value. Reported as 3800.
    row_val: bool,
    /// Go `hasWindowFunc`. Reported as 3593.
    window_func: bool,
    /// Go `otherErr`: the arity failure `VerifyArgsWrapper` raises, and the
    /// forms this tier declines to build. First one wins, as in Go.
    other: Option<DriverError>,
    /// Go `hasNotGAFunc4ExprIdx`: reported LAST, as 8200.
    not_ga_func: bool,
}

impl AdmissibilityScan {
    /// Go's `Enter`, which returns `skipChildren` for the arms that set a
    /// terminal flag -- so the subtree under an aggregate or a row value is
    /// never scanned and cannot set a flag of its own.
    fn walk(&mut self, expr: &Expr) {
        match expr {
            // Leaves and the operators Go never routes through a function
            // check: `+`, unary minus, `IS NULL`, `IN`, `BETWEEN`, `LIKE`,
            // `REGEXP`, `CASE`, `COLLATE`, a charset introducer.
            Expr::Column(_)
            | Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::Null
            | Expr::Bool(_) => {}

            // Go: `*ast.AggregateFuncExpr` -> ErrInvalidGroupFuncUse (1111).
            Expr::Aggregate { .. } | Expr::GroupConcat { .. } => self.agg_func = true,
            // Go: `*ast.RowExpr` -> ErrFunctionalIndexRowValueIsNotAllowed.
            Expr::Row(_) => self.row_val = true,
            // Go: `*ast.WindowFuncExpr` -> ErrWindowInvalidWindowFuncUse.
            Expr::Window { .. } => self.window_func = true,
            // Go: `*ast.SubqueryExpr, *ast.ValuesExpr, *ast.VariableExpr` ->
            // ErrFunctionalIndexFunctionIsNotAllowed (3758).
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::CompareSubquery { .. }
            | Expr::UserVar(_)
            | Expr::SysVar { .. } => self.illegal_func = true,

            Expr::Paren(inner) | Expr::Unary(_, inner) => self.walk(inner),
            Expr::Binary(_, left, right) => {
                self.walk(left);
                self.walk(right);
            }
            Expr::Is { expr, .. } | Expr::Collate { expr, .. } => self.walk(expr),
            Expr::Like { expr, pattern, .. } | Expr::Regexp { expr, pattern, .. } => {
                self.walk(expr);
                self.walk(pattern);
            }
            Expr::In { expr, list, .. } => {
                self.walk(expr);
                list.iter().for_each(|e| self.walk(e));
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.walk(expr);
                self.walk(low);
                self.walk(high);
            }
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                for expr in value
                    .iter()
                    .map(AsRef::as_ref)
                    .chain(else_clause.iter().map(AsRef::as_ref))
                {
                    self.walk(expr);
                }
                for (condition, result) in when_clauses {
                    self.walk(condition);
                    self.walk(result);
                }
            }
            Expr::Cast(cast) => {
                // `DATE 'lit'` / `TIME 'lit'` / `TIMESTAMP 'lit'` are not
                // casts to Go's walk at all: its parser builds them as
                // `*ast.FuncCallExpr` named `dateliteral`/`timeliteral`/
                // `timestampliteral`, none of which is on the GA list, so Go
                // answers 8200. Captured:
                //
                // ```text
                // create index i on t((timestamp '2020-01-01 00:00:00'));
                //   [ddl:8200]Unsupported creating expression index containing
                //   unsafe functions without allow-expression-index in config
                // ```
                //
                // The refusal is also the one this index could not survive:
                // the literal is folded in the writing session's `@@time_zone`
                // and stored in the key, so a reader in another zone would
                // compute a key that no longer matches its own rows.
                if matches!(
                    cast.style,
                    tidb_ast::CastStyle::DateLiteral
                        | tidb_ast::CastStyle::TimeLiteral
                        | tidb_ast::CastStyle::TimestampLiteral
                ) {
                    self.not_ga_func = true;
                    return;
                }
                // An ordinary `*ast.FuncCastExpr` is not a function call, so
                // it never meets the GA list -- `((cast(a as char(10))))` is
                // ACCEPTED by Go, captured. The ARRAY form is multi-valued
                // indexing, a feature of its own that this tier does not
                // maintain, so it is declined rather than built as an
                // ordinary scalar index -- which would index the whole JSON
                // document under a multi-valued index's name.
                if cast.array {
                    self.decline(DriverError::unsupported(
                        "a multi-valued index (CAST(... AS ... ARRAY)) is not supported yet",
                    ));
                    return;
                }
                self.walk(&cast.expr);
            }

            Expr::Func { name, args, .. } | Expr::GenericFuncCall { name, args, .. } => {
                self.call(name, args);
                args.iter().for_each(|arg| self.walk(arg));
            }

            // Go reaches these as `*ast.FuncCallExpr` under a name its
            // grammar picks -- `extract`, `locate`, `trim`, `weight_string`,
            // `timestampadd`, `timestampdiff`, `get_format`, `convert`,
            // `json_memberof`. NONE of those names is on the GA list, so the
            // verdict does not depend on which: every one of them is 8200,
            // captured through `gorun` for each form below. Their ARGUMENTS
            // are still scanned, because Go scans them and a blocked function
            // or an aggregate inside one outranks the 8200.
            Expr::Extract { value: expr, .. }
            | Expr::Interval { value: expr, .. }
            | Expr::WeightString { expr, .. }
            | Expr::ConvertUsing { expr, .. }
            | Expr::GetFormat { expr, .. } => {
                self.not_ga_func = true;
                self.walk(expr);
            }
            Expr::Position {
                substr: left,
                str: right,
            }
            | Expr::TimestampDiff {
                expr1: left,
                expr2: right,
                ..
            }
            | Expr::TimestampAdd {
                interval: left,
                expr: right,
                ..
            }
            | Expr::MemberOf {
                expr: left,
                array: right,
            } => {
                self.not_ga_func = true;
                self.walk(left);
                self.walk(right);
            }
            Expr::Trim { expr, remstr, .. } => {
                self.not_ga_func = true;
                self.walk(expr);
                remstr.iter().for_each(|e| self.walk(e));
            }

            // Every remaining form is DECLINED rather than guessed at. Go
            // reaches some of them outside the function check and ACCEPTS
            // them -- `MATCH ... AGAINST` and `DEFAULT(a)` are both captured
            // as accepted -- so this is a wrong-REFUSE, the safe direction:
            // it can only turn an index this tier would have to evaluate
            // into an error, never build one whose stored keys disagree with
            // the rows they index.
            _ => self.decline(DriverError::unsupported(
                "this expression form is not supported in an expression index yet",
            )),
        }
    }

    /// Go's `*ast.FuncCallExpr` arm, in its own order: `GROUPING()` first,
    /// then the blocked list, then arity, then the GA list.
    fn call(&mut self, name: &str, args: &[Expr]) {
        let name = name.to_ascii_lowercase();
        // Go: `ast.Grouping` is counted as an aggregate, issue #49909.
        if name == "grouping" {
            self.agg_func = true;
            return;
        }
        if ILLEGAL_FUNCTIONS.contains(&name.as_str()) {
            self.illegal_func = true;
            return;
        }
        match GA_FUNCTIONS.iter().find(|(ga, ..)| *ga == name) {
            Some((_, min, max)) => {
                // Go `VerifyArgsWrapper` -> `baseFunctionClass.verifyArgsByCount`.
                if args.len() < *min || max.is_some_and(|max| args.len() > max) {
                    self.decline(DriverError::WrongParamCountToNativeFct(name));
                }
            }
            // Go also answers 3758 for a name that is not a builtin AT ALL
            // (`IsFunctionSupported`), which needs the whole `funcs` registry
            // to tell apart from a builtin that is merely off the GA list.
            // Both are refusals; this tier reports the 8200 for both, and the
            // difference is one errno on an already-failing statement.
            None => self.not_ga_func = true,
        }
    }

    /// Go's `otherErr`, which is set once and then kept.
    fn decline(&mut self, error: DriverError) {
        if self.other.is_none() {
            self.other = Some(error);
        }
    }

    /// Go `checkIllegalFn4Generated`'s report order for `typeIndex`. Changing
    /// it changes which error a statement that trips several flags reports.
    fn verdict(self, index_name: &str) -> Result<(), DriverError> {
        if self.illegal_func {
            return Err(DriverError::FunctionalIndexFunctionNotAllowed(
                index_name.to_owned(),
            ));
        }
        if self.agg_func {
            return Err(DriverError::InvalidGroupFuncUse);
        }
        if self.row_val {
            return Err(DriverError::FunctionalIndexRowValue(index_name.to_owned()));
        }
        if self.window_func {
            return Err(DriverError::WindowInvalidWindowFuncUse(
                index_name.to_owned(),
            ));
        }
        if let Some(error) = self.other {
            return Err(error);
        }
        if self.not_ga_func {
            return Err(DriverError::UnsafeFunctionInExpressionIndex);
        }
        Ok(())
    }
}

fn check_admissible(index_name: &str, expr: &Expr) -> Result<(), DriverError> {
    let mut scan = AdmissibilityScan::default();
    scan.walk(expr);
    scan.verdict(index_name)
}

/// Go `BuildHiddenColumnInfo`: turns an index's EXPRESSION key parts into
/// hidden virtual generated columns, one per part, leaving the ordinary
/// column parts alone.
///
/// `names`/`types` are the table's columns in physical order, so a built
/// expression indexes the same row the write and read paths pass around --
/// the property [`crate::generated_column`] relies on.
///
/// Returns one entry per expression part, paired with that part's position in
/// `parts` so the caller can put the hidden column's offset back in the right
/// key slot.
pub fn build_hidden_columns(
    index_name: &str,
    parts: &[IndexPart],
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<(usize, HiddenIndexColumn)>, DriverError> {
    let mut built = Vec::new();
    for (position, part) in parts.iter().enumerate() {
        let IndexPart::Expr { expr, .. } = part else {
            continue;
        };
        let name = format!("{HIDDEN_COLUMN_PREFIX}_{index_name}_{position}");
        // Go `precheckBuildHiddenColumnInfo`: the generated name is subject
        // to the ordinary identifier length limit, reported against the
        // hidden column rather than the index.
        if name.chars().count() > MAX_COLUMN_NAME_LENGTH {
            return Err(DriverError::TooLongIdent("hidden column".to_owned()));
        }
        check_admissible(index_name, expr)?;
        // Go: the hidden column's name must not already be taken. Captured as
        // 1060 against a user column literally called `_V$_idxh_0`.
        if names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
            return Err(DriverError::DuplicateColumnName(name));
        }

        let resolver = TableColumnResolver::new(names, types, zone.clone());
        let built_expr = match tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver) {
            Ok(built_expr) => built_expr,
            Err(_) => {
                return Err(match resolver.missing_name() {
                    // Go reports 1054 with the clause `expression`, not the
                    // `generated column function` a column generation uses.
                    Some(missing) => DriverError::UnknownColumnInClause {
                        column: missing,
                        clause: "expression".to_owned(),
                    },
                    None => DriverError::unsupported(
                        "this expression index's expression is not supported yet",
                    ),
                });
            }
        };
        if let Some(missing) = resolver.missing_name() {
            return Err(DriverError::UnknownColumnInClause {
                column: missing,
                clause: "expression".to_owned(),
            });
        }
        // Go `checkIllegalFn4GeneratedColumn`'s `hasNotGAFunc4ExprIdx` arm:
        // an expression index may only use the functions on
        // `variable.GAFunction4ExpressionIndex` unless the server was started
        // with `allow-expression-index`, and everything else is 8200
        // `Unsupported creating expression index containing unsafe functions
        // without allow-expression-index in config`. `TIMESTAMP 'lit'` is a
        // `FuncCallExpr` named `timestampliteral` to that walk and is not on
        // the list, so Go refuses it -- captured:
        //
        // ```text
        // create table e(a int, key idx((timestamp '2020-01-01 10:00:00+00:00')));
        //   [ddl:8200]Unsupported creating expression index containing
        //   unsafe functions without allow-expression-index in config
        // create table e(a int, key idx((a+1)));  -- accepted
        // ```
        //
        // Only the ZONE-READING half of that gate is ported here, because it
        // is the half this index cannot survive: the value goes into the
        // stored key at write time, so an expression that folds differently
        // per session would read back rows whose key no longer matches. The
        // rest of the allow-list is a separate unit; refusing a subset of
        // what Go refuses can only turn an accepted statement into Go's own
        // error, never the reverse.
        if resolver.zone_was_read() {
            return Err(DriverError::unsupported(
                "an expression index over a temporal literal is not supported (Go answers 8200)",
            ));
        }
        // Go `BuildHiddenColumnInfo`: an expression that IS a column is 3762,
        // checked on the BUILT expression so `((a))` and `(((a)))` are both
        // caught, exactly as Go's `expr.(*expression.Column)` is.
        if matches!(built_expr, tidb_expr::expression::Expression::Column(_)) {
            return Err(DriverError::FunctionalIndexOnField);
        }

        // Go takes the hidden column's type from `expr.GetType()`. Here the
        // scalar function has already coerced its own result into exactly this
        // type before returning it (`ScalarFunction::coerce_to_ret_type`), so
        // the cast `materialize` then applies is a no-op rather than a second,
        // disagreeing conversion.
        //
        // An expression with no static type has nothing to encode the index
        // entry as, so it is refused rather than given a guessed one: an
        // index whose key type disagrees with the value it stores reads back
        // the wrong rows and `ADMIN CHECK TABLE` would call it consistent.
        let Some(field_type) = built_expr.static_type().cloned() else {
            return Err(DriverError::unsupported(
                "an expression index over an expression with no static type is not supported yet",
            ));
        };
        let expr_text = expr.restore_with_flags(hidden_restore_flags());
        // Go `pkg/ddl/index.go`'s `checkIndexColumn`, which `buildIndexColumns`
        // runs over EVERY non-columnar key part including this hidden one.
        // Reported against the EXPRESSION, which is what `col.Hidden` selects
        // there: 3761 rather than 1167, 3753 rather than 3152, 3757 rather
        // than 1170. See [`crate::ddl::index_prefix::stored_index_length`].
        //
        // An expression key part carries no declared length -- there is no
        // syntax for `((lower(a))(10))` -- so the length is always
        // unspecified, which is what turns the BLOB arm from "say how much"
        // into an outright 3757.
        crate::ddl::index_prefix::key_part_length(
            &go_result_type(&built_expr, &field_type),
            crate::ddl::index_prefix::IndexedColumn::Expression(&expr_text),
            None,
            true,
        )?;
        built.push((
            position,
            HiddenIndexColumn {
                name,
                field_type,
                generated: GeneratedColumn {
                    expr_text,
                    // Go sets `GeneratedStored: false`: the value is
                    // recomputed on every read, and only the INDEX stores it.
                    stored: false,
                    dependencies: resolver.dependency_names(),
                    expr: built_expr,
                    source: expr.clone(),
                    build_zone: zone.clone(),
                    // Unreachable by the refusal above, and stated rather than
                    // assumed: an index whose hidden column re-folded per
                    // session would read back rows its stored key no longer
                    // matches.
                    zone_sensitive: false,
                },
            },
        ));
    }
    Ok(built)
}

/// The field type Go's `checkIndexColumn` would see for this hidden column.
///
/// It is the expression's own type EXCEPT where this workspace deliberately
/// reports a different one. There is one such family --
/// [`tidb_expr::rewriter::go_result_type_code`] documents it in full: a
/// JSON-returning builtin evaluates to canonical JSON TEXT here and is typed
/// `VarString`, because there is no BinaryJSON cell to put a JSON value in.
///
/// The refusal must not inherit that. `checkIndexColumn` is asking what TiDB
/// calls the result, and reading `static_type()` straight would answer
/// `VarString` for `json_extract` and accept the index Go answers 3753 for --
/// captured, `create table t(j json, index i((j->'$.a')))` is 3753 and
/// `index i((j->>'$.a')))` is 3757, `->` and `->>` being `json_extract` and
/// `json_unquote(json_extract(...))` to the parser.
///
/// Only the TOP-level function decides, as Go's `expr.GetType()` does:
/// `json_extract(j,'$.a')+0` is a bigint to both and is ACCEPTED, captured.
fn go_result_type(expr: &tidb_expr::expression::Expression, reported: &FieldType) -> FieldType {
    let tidb_expr::expression::Expression::ScalarFunction(function) = expr else {
        return reported.clone();
    };
    match tidb_expr::rewriter::go_result_type_code(function.func_name.lowercase()) {
        Some(code) => FieldType::new(code),
        None => reported.clone(),
    }
}

/// Go restores an expression index's key part with the same flag set a
/// generated column uses, which is why `SHOW CREATE TABLE` prints
/// `` KEY `idx` ((`a` + 1)) ``.
fn hidden_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME
}

/// Whether an index has at least one expression key part, which is what makes
/// the caller take the hidden-column path at all.
#[must_use]
pub fn has_expression_part(parts: &[IndexPart]) -> bool {
    parts.iter().any(|p| matches!(p, IndexPart::Expr { .. }))
}
