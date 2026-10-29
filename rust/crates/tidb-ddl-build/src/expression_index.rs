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
//! What that DOES cost is one invariant, enforced in [`tidb_table::kv_table`]: a
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
//! 1170. [`crate::index::check_index_column`] is that function, and
//! [`crate::index::build_index_columns`] runs it over the hidden column the
//! caller appends, after [`build_hidden_columns`] -- where Go runs it.
//!
//! What makes it a rule of its own rather than a widening of the function
//! gate is that it reads a TYPE. Fifteen GA functions are refused by it --
//! the thirteen with a JSON result as 3753 and `json_unquote`/`json_pretty`
//! as 3757 -- but so is `CAST(x AS JSON)`, which is no function call at all,
//! while `json_extract(j,'$.a')+0` is ACCEPTED because the arithmetic makes
//! the result a bigint. Captured all three ways.
//!
//! Index admission reads the expression's authoritative static type, shared
//! with evaluation, generated columns and result metadata.
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
//! `!col.FieldType.IsArray()`. The cast is legal only as the whole key part
//! (Go `illegalFunctionChecker`); the write path then files one entry per
//! array element (`KvTable::indexed_value_tuples`).
//!
//! Two smaller residuals, both the safe direction (this tier refuses what Go
//! accepts, never the reverse): `MATCH ... AGAINST` and `DEFAULT(a)` are
//! ACCEPTED by Go's checker -- neither is a `FuncCallExpr` -- and are
//! declined here. And a name that is no builtin at all is 3758 in Go
//! (`expression.IsFunctionSupported`) where this answers 8200, which needs
//! the whole `funcs` registry to tell from a builtin merely off the GA list.

use tidb_ast::{Expr, IndexPart};
use tidb_datatype::FieldType;

use tidb_terror::MysqlError;

use crate::generated_column::is_disallowed_generated_function;
use tidb_table::generated_column::TableColumnResolver;
use tidb_table::generated_column::GeneratedColumn;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

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
/// (`pkg/sessionctx/variable/varsutil.go`).
///
/// A call outside this list is 8200 unless the server was started with
/// `allow-expression-index`, which defaults off and which this tier does not
/// model as settable, so the list IS the gate here. The list is transcribed
/// VERBATIM rather than derived from a principle: an earlier attempt to drop
/// an entry as redundant was falsified by the Go source, which lists it.
const GA_FUNCTIONS: &[&str] = &[
    "lower",
    "upper",
    "md5",
    "reverse",
    "vitess_hash",
    "tidb_shard",
    "json_type",
    "json_extract",
    "json_unquote",
    "json_array",
    "json_object",
    "json_set",
    "json_insert",
    "json_replace",
    "json_remove",
    "json_contains",
    "json_contains_path",
    "json_valid",
    "json_array_append",
    "json_array_insert",
    "json_merge_patch",
    "json_merge_preserve",
    "json_pretty",
    "json_quote",
    "json_schema_valid",
    "json_search",
    "json_storage_size",
    "json_depth",
    "json_keys",
    "json_length",
];

/// Go `checkIllegalFn4Generated`'s `genType`: a generated column or an
/// expression index key part.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GenType {
    /// Go `typeColumn`.
    Column,
    /// Go `typeIndex`.
    #[default]
    Index,
}

/// Go `illegalFunctionChecker`: one pass that COLLECTS every flag the
/// expression trips, which `checkIllegalFn4Generated` then reports in a FIXED
/// order.
///
/// The order is the whole point, and it is why this is a scan and not an
/// early return. `((abs(a) + sum(a)))` is 1111 and not 8200 -- Go sees both
/// the aggregate and the non-GA call, and reports the aggregate, whichever
/// came first in the tree. An early-returning walk answers by TREE POSITION
/// instead, which is a different function of the same input.
#[derive(Default)]
struct AdmissibilityScan {
    /// Which object the expression generates; it picks the error identities
    /// and whether the GA list applies.
    gen_type: GenType,
    /// Go `hasIllegalFunc`: a blocked or unknown function, a subquery,
    /// `values(x)`, or a variable. Reported first, as 3102 or 3758.
    illegal_func: bool,
    /// Go `hasAggFunc`: an aggregate or `GROUPING()`. Reported as 1111.
    agg_func: bool,
    /// Go `hasRowVal`: a row value. Reported as 3764 or 3800.
    row_val: bool,
    /// Go `hasWindowFunc`. Reported as 3593.
    window_func: bool,
    /// Go `otherErr`: the arity failure `VerifyArgsWrapper` raises, a nested
    /// `CAST(... AS ... ARRAY)`, and the forms this tier declines to build.
    other: Option<MysqlError>,
    /// Go `hasNotGAFunc4ExprIdx`: reported LAST, as 8200, for an index only.
    not_ga_func: bool,
    /// Go `hasCastArrayFunc`: a generated column may not contain the cast at
    /// all (1235, reported after everything else).
    cast_array: bool,
    /// Go `disallowCastArrayFunc`, inverted: true until the walk enters its
    /// first node other than parentheses, so `CAST(... AS ... ARRAY)` is
    /// admitted only as the whole expression.
    cast_array_allowed: bool,
}

impl AdmissibilityScan {
    /// Go's `Enter`, which returns `skipChildren` for the arms that set a
    /// terminal flag -- so the subtree under an aggregate or a row value is
    /// never scanned and cannot set a flag of its own.
    fn walk(&mut self, expr: &Expr) {
        // Go's `Enter` returns before `c.disallowCastArrayFunc = true` only
        // for parentheses.
        let cast_array_allowed = if matches!(expr, Expr::Paren(_)) {
            self.cast_array_allowed
        } else {
            std::mem::replace(&mut self.cast_array_allowed, false)
        };
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
            | Expr::CharsetString { .. }
            | Expr::Null
            | Expr::Bool(_) => {}
            // `_charset X'..'` is the same introduced literal one level down.
            Expr::CharsetBinary { value, .. } => self.walk(value),

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
                // With `allow-expression-index` (the integration tests' config)
                // Go accepts it, and the hidden column refolds the literal in
                // each writing session's zone.
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
                // ACCEPTED by Go, captured. The ARRAY form makes the key part
                // multi-valued, admitted only as the whole expression.
                self.cast_array |= cast.array;
                if cast.array && !cast_array_allowed {
                    self.fail(tidb_terror::MysqlError::from_terror(&tidb_plannererrors::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&["Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions".into()])));
                    return;
                }
                self.walk(&cast.expr);
            }

            Expr::Func { name, args, .. } | Expr::GenericFuncCall { name, args, .. } => {
                if self.call(name, args) {
                    args.iter().for_each(|arg| self.walk(arg));
                }
            }
            // Go: `*ast.VariableExpr` (`@a := expr` included) -> 3102/3758.
            Expr::Assign { .. } => self.illegal_func = true,

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

            // Neither is a `FuncCallExpr` to Go's walk. A generated column
            // leaves them to the expression builder.
            Expr::ParamMarker { .. } | Expr::Default(_) if self.gen_type == GenType::Column => {}
            Expr::MatchAgainst { against, .. } if self.gen_type == GenType::Column => {
                self.walk(against);
            }
            // Every remaining form is DECLINED rather than guessed at. Go
            // reaches some of them outside the function check and ACCEPTS
            // them -- `MATCH ... AGAINST` and `DEFAULT(a)` are both captured
            // as accepted -- so this is a wrong-REFUSE, the safe direction:
            // it can only turn an index this tier would have to evaluate
            // into an error, never build one whose stored keys disagree with
            // the rows they index.
            _ => self.decline(tidb_terror::MysqlError::unknown(
                "this expression form is not supported in an expression index yet",
            )),
        }
    }

    /// Go's `*ast.FuncCallExpr` arm, in its own order: `GROUPING()` first,
    /// then the blocked list and `IsFunctionSupported`, then arity, then the
    /// GA list. Returns whether Go goes on into the arguments.
    fn call(&mut self, name: &str, args: &[Expr]) -> bool {
        let name = name.to_ascii_lowercase();
        // Go: `ast.Grouping` is counted as an aggregate, issue #49909.
        if name == "grouping" {
            self.agg_func = true;
            return false;
        }
        if is_disallowed_generated_function(&name)
            || tidb_expr::builtin_registry::function_class(&name).is_none()
        {
            self.illegal_func = true;
            return false;
        }
        // Go `VerifyArgsWrapper` -> `baseFunctionClass.verifyArgsByCount`,
        // over the argument count Go's AST carries: DATE_ADD and its
        // siblings hold the interval's unit as a third argument.
        let count = if matches!(
            name.as_str(),
            "date_add" | "date_sub" | "adddate" | "subdate"
        ) && matches!(args, [_, Expr::Interval { .. }])
        {
            3
        } else {
            args.len()
        };
        if tidb_expr::builtin_registry::verify_args_by_count(&name, count).is_err() {
            self.fail(tidb_terror::MysqlError::from_terror(
                &tidb_expr::expression_errors::ERR_INCORRECT_PARAMETER_COUNT
                    .generate_with_stack_by_args(&[name.to_string().into()]),
            ));
            return false;
        }
        if !GA_FUNCTIONS.contains(&name.as_str()) {
            self.not_ga_func = true;
        }
        true
    }

    /// Go's `c.otherErr = err`: the latest error the walk raises replaces any
    /// earlier one.
    fn fail(&mut self, error: MysqlError) {
        self.other = Some(error);
    }

    /// A form this tier declines to build. Go accepts it, so a Go error the
    /// walk raises elsewhere still wins.
    fn decline(&mut self, error: MysqlError) {
        if self.other.is_none() {
            self.other = Some(error);
        }
    }

    /// Go `checkIllegalFn4Generated`'s report order. Changing it changes
    /// which error a statement that trips several flags reports.
    fn verdict(self, name: &str) -> Result<(), MysqlError> {
        if self.illegal_func {
            return Err(match self.gen_type {
                GenType::Column => tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED
                        .generate_with_stack_by_args(&[name.to_owned().into()]),
                ),
                GenType::Index => tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED
                        .generate_with_stack_by_args(&[name.to_owned().into()]),
                ),
            });
        }
        if self.agg_func {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_plannererrors::ERR_INVALID_GROUP_FUNC_USE
                    .generate_with_stack_by_args(&[]),
            ));
        }
        if self.row_val {
            return Err(match self.gen_type {
                GenType::Column => tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED
                        .generate_with_stack_by_args(&[name.to_owned().into()]),
                ),
                GenType::Index => tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED
                        .generate_with_stack_by_args(&[name.to_owned().into()]),
                ),
            });
        }
        if self.window_func {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_plannererrors::ERR_WINDOW_INVALID_WINDOW_FUNC_USE
                    .generate_with_stack_by_args(&[name.to_owned().into()]),
            ));
        }
        if let Some(error) = self.other {
            return Err(error);
        }
        // Go: `!config.GetGlobalConfig().Experimental.AllowsExpressionIndex`.
        if self.gen_type == GenType::Index
            && self.not_ga_func
            && !tidb_config::config_tree::config::get_global_config()
                .experimental
                .allows_expression_index
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_EXPRESSION_INDEX
                    .generate_with_stack_by_args(&[]),
            ));
        }
        if self.gen_type == GenType::Column && self.cast_array {
            return Err(tidb_terror::MysqlError::from_terror(&tidb_plannererrors::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&["Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions".into()])));
        }
        Ok(())
    }
}

/// Go `checkIllegalFn4Generated` (`pkg/ddl/generated_column.go:351`): the
/// function, aggregate, row-value, window and arity rules a generated
/// column's expression or an expression index key part must pass, `name`
/// being the column or the index.
pub fn check_illegal_fn_4_generated(
    name: &str,
    gen_type: GenType,
    expr: &Expr,
) -> Result<(), MysqlError> {
    let mut scan = AdmissibilityScan {
        gen_type,
        cast_array_allowed: true,
        ..AdmissibilityScan::default()
    };
    scan.walk(expr);
    scan.verdict(name)
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
) -> Result<Vec<(usize, HiddenIndexColumn)>, MysqlError> {
    build_hidden_columns_with_like_default_escape(
        index_name,
        parts,
        names,
        types,
        zone,
        b'\\',
        tidb_mysql::DefaultCharset,
    )
}

/// Statement-aware form of [`build_hidden_columns`]. `connection_charset` is
/// the DDL session's `character_set_connection`: Go's parser gave each plain
/// string literal of the key part that charset, so the hidden column's
/// stored expression writes it as the `_charset` introducer.
pub fn build_hidden_columns_with_like_default_escape(
    index_name: &str,
    parts: &[IndexPart],
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
    connection_charset: &str,
) -> Result<Vec<(usize, HiddenIndexColumn)>, MysqlError> {
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
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_TOO_LONG_IDENT.generate_with_stack_by_args(&[
                    ("hidden column".to_owned()).to_string().into(),
                ]),
            ));
        }
        check_illegal_fn_4_generated(&tidb_hack::go_to_lower(index_name), GenType::Index, expr)?;
        // Go: the hidden column's name must not already be taken. Captured as
        // 1060 against a user column literally called `_V$_idxh_0`.
        if names.iter().any(|n| n.go_equal_fold(&name.go_to_lower())) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                    .generate_with_stack_by_args(&[name.to_string().into()]),
            ));
        }

        let resolver = TableColumnResolver::with_like_default_escape(
            names,
            types,
            zone.clone(),
            like_default_escape,
        );
        let built_expr = match tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver) {
            Ok(built_expr) => built_expr,
            Err(error) => {
                return Err(match (resolver.missing_name(), error) {
                    // Go reports 1054 with the clause `expression`, not the
                    // `generated column function` a column generation uses.
                    (Some(missing), _) => tidb_terror::MysqlError::from_terror(
                        &tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(
                            &[
                                tidb_hack::go_to_lower(missing).into(),
                                ("expression".to_owned()).to_string().into(),
                            ],
                        ),
                    ),
                    (None, tidb_expr::EvalError::Unsupported(_)) => {
                        tidb_terror::MysqlError::unknown(
                            "this expression index's expression is not supported yet",
                        )
                    }
                    // Go returns `BuildSimpleExpr`'s own error, such as
                    // `CAST(j AS JSON ARRAY)`'s 1235.
                    (None, error) => error.to_mysql_error(),
                });
            }
        };
        if let Some(missing) = resolver.missing_name() {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                    tidb_hack::go_to_lower(missing).into(),
                    ("expression".to_owned()).to_string().into(),
                ]),
            ));
        }
        // Go `BuildHiddenColumnInfo`: an expression that IS a column is 3762,
        // checked on the BUILT expression so `((a))` and `(((a)))` are both
        // caught, exactly as Go's `expr.(*expression.Column)` is.
        if matches!(built_expr, tidb_expr::expression::Expression::Column(_)) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_FUNCTIONAL_INDEX_ON_FIELD
                    .generate_with_stack_by_args(&[]),
            ));
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
        let Some(mut field_type) = built_expr.static_type().cloned() else {
            return Err(tidb_terror::MysqlError::unknown(
                "an expression index over an expression with no static type is not supported yet",
            ));
        };
        // Go `BuildHiddenColumnInfo` resets flags a wrong type inference may
        // have left, widens an unspecified temporal fsp to the maximum, and
        // gives an array the binary collation ("will influence how TiKV
        // handles the index value").
        field_type.del_flags(
            tidb_datatype::FieldTypeFlags::PRI_KEY
                | tidb_datatype::FieldTypeFlags::UNIQUE_KEY
                | tidb_datatype::FieldTypeFlags::AUTO_INCREMENT,
        );
        if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Datetime
                | tidb_datatype::FieldTypeCode::Date
                | tidb_datatype::FieldTypeCode::Timestamp
                | tidb_datatype::FieldTypeCode::Duration
        ) && field_type.decimal() == tidb_datatype::UNSPECIFIED_LENGTH
        {
            field_type.set_decimal(tidb_datatype::MAX_FSP);
        }
        if field_type.is_array() {
            field_type.set_charset_name("binary");
            field_type.set_collation_name("binary");
        }
        let expr_text =
            expr.restore_with_flags_and_string_charset(hidden_restore_flags(), connection_charset);
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
                    build_like_default_escape: like_default_escape,
                    // Go rebuilds the hidden column from its text in each
                    // writing session (`resolveGeneratedColumns`), so a
                    // temporal literal folds in that session's zone.
                    zone_sensitive: resolver.zone_was_read(),
                    like_default_escape_sensitive: resolver.like_default_escape_was_read(),
                },
            },
        ));
    }
    Ok(built)
}

/// Go restores an expression index's key part with the same flag set a
/// generated column uses, which is why `SHOW CREATE TABLE` prints
/// `` KEY `idx` ((`a` + 1)) ``.
fn hidden_restore_flags() -> tidb_parser_format::RestoreFlags {
    tidb_parser_format::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_parser_format::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_parser_format::RestoreFlags::NAME_BACK_QUOTES
        | tidb_parser_format::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_parser_format::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_parser_format::RestoreFlags::WITHOUT_TABLE_NAME
}

/// Whether an index has at least one expression key part, which is what makes
/// the caller take the hidden-column path at all.
#[must_use]
pub fn has_expression_part(parts: &[IndexPart]) -> bool {
    parts.iter().any(|p| matches!(p, IndexPart::Expr { .. }))
}
