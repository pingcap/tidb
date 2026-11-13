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

//! Go `pkg/expression/constant_fold.go`, reduced to the part this tier can
//! observe.

use tidb_exprctx::context::Columns;
use crate::expression::Expression;
use tidb_datatype::Datum;

/// How Go's expression rewriter constructs functions under the current AST
/// parent (`newFunctionWithInit`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConstantFoldMode {
    /// `expression.NewFunction`: fold the newly built function normally.
    #[default]
    Normal,
    /// `expression.NewFunctionTryFold`: keep the function when folding warns.
    Try,
    /// `expression.NewFunctionBase`: do not fold descendants in this scope.
    Disabled,
}

/// Runs the NOT NULL half of Go's `foldConstant`
/// (`pkg/expression/constant_fold.go`) over a freshly built tree, bottom up.
///
/// Go replaces a wholly-constant subtree with a `Constant` and derives its
/// `NotNullFlag` from the computed value. This walk derives only that flag;
/// it does not supply the folded value that some parent type constructors
/// inspect. Callers needing that representation must use the construction
/// fold, not treat this metadata-only walk as a substitute.
///
/// The flag is not decoration: `SHOW COLUMNS` on a view reads it, which is how
/// `CREATE VIEW v AS SELECT CAST('' AS CHAR(32))` reports `NO` for a column no
/// base table declared NOT NULL.
pub fn derive_constant_null_flag(expr: &mut Expression) {
    if folding_suspended() {
        return;
    }
    let _ = fold_value(expr);
}

thread_local! {
    static FOLDING_SUSPENDED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn folding_suspended() -> bool {
    FOLDING_SUSPENDED.with(std::cell::Cell::get)
}

/// Restores constant folding when dropped; see [`suspend_constant_folding`].
#[must_use = "folding resumes when the guard is dropped"]
pub struct FoldingSuspended(bool);

impl Drop for FoldingSuspended {
    fn drop(&mut self) {
        FOLDING_SUSPENDED.with(|flag| flag.set(self.0));
    }
}

/// Turns constant folding off on this thread until the guard drops. A pass
/// that only reads a plan's SHAPE (privilege collection plans a statement a
/// second time) must not run builtins: some have effects
/// (`RELEASE_ALL_LOCKS()`), and they happen once, in the statement's own plan.
pub fn suspend_constant_folding() -> FoldingSuspended {
    FoldingSuspended(FOLDING_SUSPENDED.with(|flag| flag.replace(true)))
}

/// Folds exactly the function just built, under Go's selected construction
/// mode. Its arguments have already been rewritten and folded individually;
/// recursively evaluating them here would duplicate warnings and side effects.
pub fn fold_constant_in_mode(
    expr: &mut Expression,
    ctx: &dyn crate::Columns,
    mode: ConstantFoldMode,
) {
    fold_constant_in_mode_inner(expr, ctx, mode, false);
}

/// Folds a planner expression while retaining constant coercions whose
/// evaluation can emit a statement warning. Go builds those casts and
/// charset conversions with the live statement context, so a planner-side
/// `NoColumns` fold would otherwise replace them with a value and permanently
/// lose the warning at execution. The flag is deliberately opt-in: DDL and
/// expression-unit callers that use an actual warning context retain the
/// ordinary Go construction-time fold.
pub fn fold_constant_in_mode_preserving_warning_casts(
    expr: &mut Expression,
    ctx: &dyn crate::Columns,
    mode: ConstantFoldMode,
) {
    fold_constant_in_mode_inner(expr, ctx, mode, true);
}

fn fold_constant_in_mode_inner(
    expr: &mut Expression,
    ctx: &dyn crate::Columns,
    mode: ConstantFoldMode,
    preserve_warning_casts: bool,
) {
    if folding_suspended() {
        return;
    }
    if mode == ConstantFoldMode::Disabled {
        return;
    }
    // Go newFunctionImpl only bookmarks warnings for NewFunctionTryFold.
    // Build a replacement without changing the original function, so rejecting
    // a warning-producing fold needs neither a tree copy nor a rollback.
    let warning_bookmark = (mode == ConstantFoldMode::Try).then(|| ctx.warning_count());
    // A fold attempt whose evaluation ERRORED must leave no warnings: go
    // keeps the original expression and the execution re-evaluates it,
    // re-emitting the diagnostics. The swallowed error returns `None` like
    // every non-foldable case; the count delta identifies it. The bookmark
    // is PER-INVOCATION -- the stash is shared across the statement's fold
    // attempts, so a rollback must not reach into an earlier node's kept
    // diagnostics.
    let error_bookmark = ctx.warning_count();
    let replacement = fold_current_value_in(expr, ctx, preserve_warning_casts);
    if let Some(bookmark) = warning_bookmark {
        if ctx.warning_count() > bookmark {
            ctx.truncate_warnings(bookmark);
            return;
        }
    }
    if replacement.is_none() && ctx.warning_count() > error_bookmark {
        ctx.truncate_warnings(error_bookmark);
    }
    let replacement = match replacement {
        // Go `ifNullFoldHandler` (issue 51765): an IFNULL whose first
        // argument folds to NULL takes the charset and collation of its
        // second argument before the fold keeps them.
        Some(Folded::IfNullSecond(second)) => {
            if let Expression::ScalarFunction(function) = expr {
                let second_type = function.args[1].static_type().cloned();
                if let (Some(ret_type), Some(second_type)) = (function.ret_type.as_mut(), second_type) {
                    ret_type.set_charset_name(second_type.charset_name());
                    ret_type.set_collation_name(second_type.collation_name());
                }
            }
            Some(*second)
        }
        other => other,
    };
    match replacement {
        Some(Folded::IfNullSecond(_)) => unreachable!("peeled above"),
        Some(Folded::Constant(folded, is_deferred)) => {
            let mut folded = Expression::Constant(folded);
            crate::collation_derive::keep_collation_after_fold(expr, &mut folded);
            let original = std::mem::replace(expr, folded);
            if is_deferred {
                let Expression::Constant(folded) = expr else {
                    unreachable!()
                };
                folded.deferred_expr = Some(Box::new(original));
            }
        }
        Some(Folded::Branch(mut branch)) => {
            crate::collation_derive::keep_collation_after_fold(expr, &mut branch);
            *expr = branch;
        }
        None => {}
    }
}

/// What a construction-time fold replaces a node with.
enum Folded {
    /// The node's value, and whether it is deferred (re-evaluated per run).
    Constant(crate::constant::Constant, bool),
    /// One of the node's own arguments: a special handler picked the branch
    /// a constant condition decides.
    Branch(Expression),
    /// IFNULL's second argument, picked because the first folds to NULL.
    IfNullSecond(Box<Folded>),
}

fn fold_current_value_in(
    expr: &Expression,
    ctx: &dyn crate::Columns,
    preserve_warning_casts: bool,
) -> Option<Folded> {
    if preserve_warning_casts && has_runtime_warning_cast(expr) {
        return None;
    }
    // Go foldConstant's ordinary scalar arm inspects the already-built args;
    // it does not recursively revisit every descendant at each parent.
    let func = match expr {
        Expression::ScalarFunction(func) => func,
        _ => return None,
    };
    if is_unfoldable_in(func.func_name.lowercase(), func.args.len(), ctx) {
        return None;
    }
    // Go: "we should not fold the extension function, because it may have a
    // side effect".
    if func.is_extension_func() {
        return None;
    }
    if let Some(folded) = special_fold(expr, func, ctx) {
        return folded;
    }
    let mut has_null_arg = false;
    let mut all_const_arg = true;
    let mut is_deferred_const = false;
    for arg in &func.args {
        match arg {
            Expression::Constant(constant) => {
                has_null_arg |= constant.value.is_null();
                is_deferred_const |= constant.literal_value().is_none();
            }
            _ => all_const_arg = false,
        }
    }
    if !all_const_arg {
        return None;
    }
    // Go `expression_rewriter.go:3016-3029`: a deferred function (the clock
    // family) folds into a Constant that carries the function as
    // `DeferredExpr`, so a cached plan re-evaluates it on every execution
    // instead of serving the folding-time value. `UNIX_TIMESTAMP` with
    // arguments is explicitly excluded (it is a normal expression of its
    // argument).
    let deferred_self = is_deferred_function(func.func_name.lowercase(), func.args.len());
    let value = crate::eval_expression_once(expr, ctx).ok()?;
    let mut ret_type = expr.static_type()?.clone();
    if !has_null_arg {
        if value.is_null() {
            ret_type.del_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
        } else {
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        }
    }
    let mut folded = crate::constant::Constant::new(value, ret_type);
    // Go keeps the first positive subquery reference only for an immutable
    // folded result; deferred results retain their dependency expression.
    if !is_deferred_const && !deferred_self {
        folded.subquery_ref_id = func
            .args
            .iter()
            .find_map(|arg| match arg {
                Expression::Constant(constant) if constant.subquery_ref_id > 0 => {
                    Some(constant.subquery_ref_id)
                }
                _ => None,
            })
            .unwrap_or(0);
    }
    // Go FoldConstant preserves the expression's collation on replacement.
    folded.collation = func.collation.clone();
    Some(Folded::Constant(folded, is_deferred_const || deferred_self))
}

/// Go `specialFoldHandler` (`constant_fold.go:31-37`): `if`, `ifnull`,
/// `case` and `isnull` fold on their deciding argument alone, so a constant
/// condition selects its branch and `isnull` of a NOT NULL argument is 0.
/// The outer `None` falls through to the ordinary fold; `Some(None)` keeps
/// the node. Arguments arrive already folded, as Go's recursion leaves them.
fn special_fold(
    expr: &Expression,
    func: &crate::scalar_function::ScalarFunction,
    ctx: &dyn crate::Columns,
) -> Option<Option<Folded>> {
    let name = func.func_name.lowercase();
    if !matches!(name, "if" | "ifnull" | "case" | "isnull") {
        return None;
    }
    // Go skips these handlers for `MaybeOverOptimized4PlanCache`: a branch
    // picked from a parameter's value would outlive the execution. A built
    // expression may be reused across executions here even without the plan
    // cache, so any mutable constant keeps the node for the ordinary fold,
    // which defers it.
    if crate::expr_util::predicates::maybe_over_optimized_4_plan_cache(
        true,
        std::slice::from_ref(expr),
    ) {
        return None;
    }
    // Go `EvalInt` on the condition, which `wrapWithIsTrue` has made a truth
    // value: an evaluation error keeps the node for the runtime to report.
    let truth = |constant: &crate::constant::Constant| -> Option<Option<bool>> {
        let value = constant.eval_in(ctx).ok()?;
        crate::truthy_of(&value).ok()
    };
    // Go's handlers fold the condition themselves (`foldConstant(ctx,
    // args[0])`) before reading it, so a constant condition still wrapped
    // in `istrue` decides the branch.
    let folded_condition = |argument: &Expression| -> Option<crate::constant::Constant> {
        match argument {
            Expression::Constant(constant) => Some(constant.clone()),
            other => match fold_current_value_in(other, ctx, false) {
                Some(Folded::Constant(constant, _)) => Some(constant),
                Some(Folded::IfNullSecond(second)) => match *second {
                    Folded::Constant(constant, _) => Some(constant),
                    _ => None,
                },
                _ => None,
            },
        }
    };
    let branch = |argument: &Expression| -> Folded {
        match argument {
            Expression::Constant(constant) => {
                let deferred = constant.literal_value().is_none();
                Folded::Constant(constant.clone(), deferred)
            }
            other => Folded::Branch(other.clone()),
        }
    };
    Some(match name {
        "isnull" => {
            let argument = func.args.first()?;
            if let Expression::Constant(constant) = argument {
                let deferred = constant.literal_value().is_none();
                let value = crate::eval_expression_once(expr, ctx).ok()?;
                let mut folded =
                    crate::constant::Constant::new(value, expr.static_type()?.clone());
                folded.collation = func.collation.clone();
                Some(Folded::Constant(folded, deferred))
            } else if argument
                .static_type()
                .is_some_and(|field| field.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL))
            {
                Some(Folded::Constant(crate::constant::Constant::new_zero(), false))
            } else {
                None
            }
        }
        "if" => {
            let [condition, then, otherwise] = func.args.as_slice() else {
                return Some(None);
            };
            let Some(condition) = folded_condition(condition) else {
                return Some(None);
            };
            let Some(value) = truth(&condition) else {
                return Some(None);
            };
            Some(branch(if value == Some(true) { then } else { otherwise }))
        }
        "ifnull" => {
            let [first, second] = func.args.as_slice() else {
                return Some(None);
            };
            let Some(constant) = folded_condition(first) else {
                return Some(None);
            };
            if constant.value.is_null() {
                Some(Folded::IfNullSecond(Box::new(branch(second))))
            } else {
                let deferred = constant.literal_value().is_none();
                Some(Folded::Constant(constant, deferred))
            }
        }
        _ => {
            // `caseWhenHandler`: the first constant-true condition decides;
            // a non-constant one leaves every later branch unknown.
            let arguments = &func.args;
            let decimal = expr.static_type().map(tidb_datatype::FieldType::decimal);
            let with_decimal = |folded: Folded| match (folded, decimal) {
                (Folded::Constant(mut constant, deferred), Some(decimal)) => {
                    if let Some(field) = constant.ret_type.as_mut() {
                        field.set_decimal(decimal);
                    }
                    Folded::Constant(constant, deferred)
                }
                (folded, _) => folded,
            };
            let mut index = 0;
            while index + 1 < arguments.len() {
                let Some(condition) = folded_condition(&arguments[index]) else {
                    return Some(None);
                };
                let Some(value) = truth(&condition) else {
                    return Some(None);
                };
                if value == Some(true) {
                    return Some(Some(with_decimal(branch(&arguments[index + 1]))));
                }
                index += 2;
            }
            if arguments.len() % 2 == 1 {
                Some(with_decimal(branch(&arguments[arguments.len() - 1])))
            } else {
                None
            }
        }
    })
}

/// Constant integer casts, builtins that wrap their arguments in Go's
/// `WrapWithCastAsInt`, and `CHAR(... USING charset)` use the statement context
/// to report truncation, out-of-range, or invalid-byte diagnostics. A
/// no-column planner context cannot retain those diagnostics, so callers that
/// are preparing an executable plan keep these nodes unfolded for runtime.
///
/// String/byte inputs always go through Go's prefix scanner and may report
/// truncation.  Real, float32, and decimal inputs are retained too: their
/// range checks report overflow through the same statement context, and
/// deciding whether a particular value is in range here would duplicate the
/// type-specific conversion rules.  Keeping the numeric carriers unfolded is
/// value-preserving and makes the runtime warning owner unambiguous.
fn has_runtime_warning_cast(expr: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    let name = function.func_name.lowercase();
    let indexes: &[usize] = match name {
        "cast"
            if function.get_static_type().is_some_and(|field| {
                use tidb_datatype::FieldTypeCode;
                !field.is_array()
                    && matches!(
                        field.code(),
                        FieldTypeCode::Tiny
                            | FieldTypeCode::Short
                            | FieldTypeCode::Int24
                            | FieldTypeCode::Long
                            | FieldTypeCode::LongLong
                            | FieldTypeCode::Bit
                            | FieldTypeCode::NewDecimal
                            | FieldTypeCode::Date
                            | FieldTypeCode::NewDate
                            | FieldTypeCode::Datetime
                            | FieldTypeCode::Timestamp
                    )
            }) =>
        {
            &[0]
        }
        "vitess_hash"
        | "tidb_shard" => &[0],
        // `FORMAT(number, decimals)` wraps its second argument to ETInt.
        "format" => &[1],
        // `builtinCharSig.evalString` appends ErrInvalidCharacterString
        // (1300) while decoding the bytes produced by CHAR's numeric
        // arguments. The final constant is NULL for the no-USING form, which
        // uses the binary signature and has no decode warning to preserve.
        "char_func" => {
            return function.args.last().is_some_and(|charset| {
                matches!(
                    charset,
                    Expression::Constant(constant) if !constant.value.is_null()
                )
            });
        }
        _ => return false,
    };
    if !indexes.iter().any(|&index| {
        matches!(
            function.args.get(index),
            Some(Expression::Constant(constant))
                if matches!(
                    constant.value,
                    Datum::String(_)
                        | Datum::Bytes(_)
                        | Datum::Real(_)
                        | Datum::Float32(_)
                        | Datum::Decimal(_)
                )
        )
    }) {
        return false;
    }
    true
}

/// One node of Go's `foldConstant`: folds bottom up, returning the constant
/// value the node reduces to, or `None` when it does not reduce to one.
///
/// `None` is Go's "return the expression unfolded", which is what a parent
/// reads as `allConstArg = false`. An evaluation that FAILS also returns
/// `None` and leaves the flag alone, exactly as Go's `err != nil` arm returns
/// before the folded `Constant` is built.
fn fold_value(expr: &mut Expression) -> Option<Datum> {
    let func = match expr {
        Expression::Constant(constant) => return constant.literal_value().cloned(),
        Expression::Column(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {
            return None
        }
        Expression::ScalarFunction(func) => func,
    };
    if is_unfoldable_call(func.func_name.lowercase(), func.args.len()) || func.is_extension_func() {
        // Go still folds this node's ARGUMENTS -- `foldConstant` recurses
        // through `NewFunction` as each level is built -- it only refuses to
        // fold the unfoldable node itself.
        for arg in &mut func.args {
            let _ = fold_value(arg);
        }
        return None;
    }
    let mut has_null_arg = false;
    let mut all_const_arg = true;
    for arg in &mut func.args {
        match fold_value(arg) {
            Some(Datum::Null) => has_null_arg = true,
            Some(_) => {}
            None => all_const_arg = false,
        }
    }
    if !all_const_arg {
        return None;
    }
    // A constant tree reads no row and no session state, so Go's empty
    // `chunk.Row{}` over a resolver that answers nothing is the whole input.
    let chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty_chunk(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let value = expr
        .eval(&tidb_exprctx::context::NoColumns, chunk.get_row(0))
        .ok()?;
    if !has_null_arg {
        let Expression::ScalarFunction(func) = expr else {
            unreachable!("matched as a scalar function above")
        };
        if let Some(ret_type) = func.ret_type.as_mut() {
            if matches!(value, Datum::Null) {
                ret_type.del_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
            } else {
                ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            }
        }
    }
    Some(value)
}

/// Whether Go's `foldConstant` would have REPLACED this subtree with a
/// `*Constant` by the time an enclosing function's `getFunction` runs its
/// `args[i].(*Constant)` type switch.
///
/// Go folds during construction, so the switch never sees the unfolded shape.
/// This rewriter keeps the shape (see [`derive_constant_null_flag`]), so every
/// dispatch that Go keys on constant-ness has to ask this question instead of
/// testing the node kind -- and the two answers differ for exactly the cases
/// that matter: `CONCAT('1:00',':00')` is a `*Constant` in Go and a
/// `ScalarFunction` here, while a column reference is neither.
///
/// The predicate is `foldConstant`'s own gate: not one of the
/// [`is_unfoldable`] names, and every argument folds too. The one arm not
/// reproduced is Go's `err != nil` escape -- a constant subtree whose
/// evaluation FAILS is left unfolded there and reported constant here. That
/// arm only changes which of two error-or-domain choices a doomed expression
/// takes, never a value, so it is not worth evaluating the subtree once per
/// row to reproduce.
pub(crate) fn folds_to_constant(expr: &Expression) -> bool {
    match expr {
        Expression::Constant(_) => true,
        Expression::Column(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {
            false
        }
        Expression::ScalarFunction(func) => {
            !is_unfoldable_call(func.func_name.lowercase(), func.args.len())
                && !func.is_extension_func()
                && func.args.iter().all(folds_to_constant)
        }
    }
}

/// Go `unFoldableFunctions` (`pkg/expression/function_traits.go`): the
/// functions whose result is not a property of their arguments -- a clock, a
/// counter, a random source, a session variable, or a side effect.
pub fn is_unfoldable(name: &str) -> bool {
    matches!(
        name,
        "sysdate"
            | "found_rows"
            | "rand"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "sleep"
            | "row"
            | "values"
            | "setvar"
            | "getvar"
            | "getparam"
            | "benchmark"
            | "dayname"
            // Information functions read session properties in Go. The
            // planner fold has only `NoColumns`, where their Rust evaluators
            // otherwise return NULL instead of Go's missing-property error;
            // retaining the function lets the live session supply the value.
            | "database"
            | "schema"
            | "current_user"
            | "current_role"
            | "current_resource_group"
            | "user"
            | "session_user"
            | "system_user"
            | "connection_id"
            | "row_count"
            | "version"
            | "tidb_version"
            // Reads the statement summaries through the session's SQL
            // executor and checks its PROCESS privilege.
            | "tidb_decode_sql_digests"
            // Reads the SESSION transaction context at evaluation: folding
            // it at plan time would freeze the zero the statement had before
            // its first read opened a snapshot.
            | "tidb_current_tso"
            | "nextval"
            | "lastval"
            | "setval"
            | "any_value"
    )
}

/// Go folds the information functions as `NewFunction` builds them, against
/// the live session (`CurrentDB`, `CurrentUserPropReader`,
/// `SessionVarsPropReader`): `CONNECTION_ID()` becomes its connection's id
/// in the plan. A context with no session behind it (the planner's zone-only
/// fallback, a scope rebuilt without its statement) cannot answer them, so
/// there they stay calls for the live session to evaluate.
fn is_unfoldable_in(name: &str, arg_count: usize, ctx: &dyn crate::Columns) -> bool {
    let reads_session = matches!(
        name,
        "database"
            | "schema"
            | "current_user"
            | "current_role"
            | "current_resource_group"
            | "user"
            | "session_user"
            | "system_user"
            | "connection_id"
            | "row_count"
            | "version"
            | "tidb_version"
    );
    if reads_session {
        return ctx.connection_id().is_none();
    }
    is_unfoldable_call(name, arg_count)
}

/// Go does not classify `LAST_INSERT_ID` in `unFoldableFunctions`: the
/// one-argument form is folded by `NewFunction`, and that construction-time
/// evaluation publishes its value through `SessionVars`. Only the zero-
/// argument reader must remain runtime-bound because its result comes from
/// the previous statement. Keep that arity distinction at the fold gate while
/// leaving [`is_unfoldable`] as the source-name table used by other callers.
fn is_unfoldable_call(name: &str, arg_count: usize) -> bool {
    if name == "last_insert_id" {
        return arg_count == 0;
    }
    is_unfoldable(name)
}

/// Go `IsDeferredFunctions` (`pkg/expression/function_traits.go:159-171`),
/// with the caller's `UNIX_TIMESTAMP`-with-arguments exception
/// (`expression_rewriter.go:3021`): these foldable clock functions must be
/// re-evaluated per execution when a plan cache reuses the built tree.
fn is_deferred_function(name: &str, arg_count: usize) -> bool {
    let clock = matches!(
        name,
        "now"
            | "random_bytes"
            | "current_timestamp"
            | "utc_time"
            | "curtime"
            | "current_time"
            | "utc_timestamp"
            | "unix_timestamp"
            | "curdate"
            | "current_date"
            | "utc_date"
    );
    clock && (name != "unix_timestamp" || arg_count == 0)
}

#[cfg(test)]
mod deferred_function_tests {
    use tidb_exprctx::Columns;
    use super::*;
    use tidb_exprctx::context::NoColumns;
    use crate::expression::{Expression, ScalarFunction};
    use tidb_ast::CiString;
    use tidb_datatype::FieldType;

    /// A context whose statement clock the test pins, so NOW() evaluations
    /// are deterministic and two executions can observe the difference.
    struct WarningCollector(std::cell::RefCell<Vec<(u16, String)>>);

    impl Columns for WarningCollector {
        fn get(&self, _path: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
        fn warning_count(&self) -> usize {
            self.0.borrow().len()
        }
    }

    struct Clock(u64);

    impl Columns for Clock {
        fn get(&self, _path: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((self.0 as i64, 0, 0))
        }
    }

    #[test]
    fn folding_preserves_first_positive_subquery_reference() {
        for (references, expected) in [([77, 88], 77), ([-7, 77], 77), ([-7, 0], 0)] {
            let args = references
                .into_iter()
                .map(|reference| {
                    let mut constant = crate::constant::Constant::new(
                        Datum::Int(1),
                        FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    );
                    constant.subquery_ref_id = reference;
                    Expression::Constant(constant)
                })
                .collect();
            let folded = crate::new_function::new_function(
                &NoColumns,
                "plus",
                FieldType::new(tidb_datatype::FieldTypeCode::Unspecified),
                args,
            )
            .unwrap();
            let Expression::Constant(constant) = folded else {
                panic!("unfolded sum")
            };
            assert_eq!(constant.value, Datum::Int(2));
            assert_eq!(constant.subquery_ref_id, expected);
        }
    }

    /// Go `TryFoldFunctions` includes `ast.Interval` (function_traits.go:81):
    /// INTERVAL's arguments are try-folded, and a warning during that fold
    /// keeps the function UNFOLDED with no warning raised at build time. The
    /// walk therefore must not descend into INTERVAL's arguments.
    #[test]
    fn interval_scope_does_not_raise_child_warnings_during_fold() {
        // INTERVAL(1, 0, <cast that warns 1292>): the cast child must not be
        // folded, so the fold itself must not append the 1292 warning.
        let mut expr = crate::new_function::new_function_base(
            &crate::NoColumns,
            "interval",
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(0),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                crate::new_function::new_function_base(
                    &crate::NoColumns,
                    "intdiv",
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    vec![
                        Expression::Constant(crate::constant::Constant::new(
                            Datum::Int(1),
                            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )),
                        Expression::Constant(crate::constant::Constant::new(
                            Datum::Int(0),
                            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )),
                    ],
                ).unwrap(),
            ],
        ).unwrap();
        let warnings = WarningCollector(std::cell::RefCell::new(Vec::new()));
        fold_constant_in_mode(&mut expr, &warnings, ConstantFoldMode::Normal);
        assert!(
            warnings.0.borrow().is_empty(),
            "child warnings leaked into the fold: {:?}",
            warnings.0.borrow()
        );
    }

    /// Go's zero-argument LAST_INSERT_ID reads `SessionVarsPropReader`.
    /// Folding in a planner scope without session properties must therefore
    /// leave that reader runtime-bound rather than freezing NULL.
    #[test]
    fn last_insert_id_requires_runtime_session_state() {
        let mut expr = crate::new_function::new_function_base(
            &crate::NoColumns,
            "last_insert_id",
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![],
        ).unwrap();
        fold_constant_in_mode(&mut expr, &NoColumns, ConstantFoldMode::Normal);
        assert!(matches!(expr, Expression::ScalarFunction(_)));
    }

    /// Go's one-argument LAST_INSERT_ID is intentionally absent from
    /// `unFoldableFunctions`: `NewFunction` folds it immediately and the fold
    /// publishes the value through the live statement context. Keep that
    /// side effect when the generic folder is called with a session context.
    #[test]
    fn last_insert_id_argument_folds_and_publishes_during_construction() {
        struct RecordingColumns(std::cell::Cell<Option<u64>>);

        impl Columns for RecordingColumns {
            fn get(&self, _path: &[String]) -> Option<Datum> {
                None
            }

            fn set_last_insert_id(&self, value: u64) {
                self.0.set(Some(value));
            }
        }

        let int_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut expr = crate::new_function::new_function_base(
            &crate::NoColumns,
            "last_insert_id",
            int_type.clone(),
            vec![Expression::Constant(crate::constant::Constant::new(
                Datum::Int(42),
                int_type,
            ))],
        ).unwrap();
        let context = RecordingColumns(std::cell::Cell::new(None));
        fold_constant_in_mode(&mut expr, &context, ConstantFoldMode::Normal);

        assert!(matches!(expr, Expression::Constant(_)));
        assert_eq!(context.0.get(), Some(42));
    }

    /// Go's information builtins carry `CurrentDB`, `CurrentUserPropReader`,
    /// or `SessionVarsPropReader` even though they are absent from
    /// `unFoldableFunctions`. A planner scope has no such properties, so the
    /// Rust fold must retain each call until a live session evaluates it.
    #[test]
    fn session_information_functions_require_runtime_context() {
        for name in [
            "database",
            "schema",
            "current_user",
            "current_role",
            "current_resource_group",
            "user",
            "session_user",
            "system_user",
            "connection_id",
            "row_count",
            "version",
            "tidb_version",
        ] {
            let mut expr = crate::new_function::new_function_base(
                &crate::NoColumns,
                name,
                FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                vec![],
            ).unwrap();
            fold_constant_in_mode(&mut expr, &NoColumns, ConstantFoldMode::Normal);
            assert!(
                matches!(expr, Expression::ScalarFunction(_)),
                "{name} was frozen during the no-session fold"
            );
        }
    }

    #[test]
    fn benchmark_scope_keeps_its_subtree_unfolded() {
        // BENCHMARK(1, CAST('x' AS SIGNED)): the inner cast folds only if the
        // walk descends into the unfoldable parent's children.
        let inner = crate::new_function::new_function_base(
            &crate::NoColumns,
            "plus",
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(2),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
            ],
        ).unwrap();
        let mut expr = crate::new_function::new_function_base(
            &crate::NoColumns,
            "benchmark",
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                inner,
            ],
        ).unwrap();
        fold_constant_in_mode(&mut expr, &NoColumns, ConstantFoldMode::Normal);

        // The benchmark's cast argument must still be a scalar function.
        match &expr {
            Expression::ScalarFunction(benchmark) => match &benchmark.get_args()[1] {
                Expression::ScalarFunction(_) => {}
                other => panic!("the cast argument was folded: {other:?}"),
            },
            other => panic!("benchmark itself was folded: {other:?}"),
        }
    }

    #[test]
    fn deferred_function_fold_re_evaluates_per_execution() {
        let now_node = crate::new_function::new_function_base(
            &crate::NoColumns,
            "now",
            FieldType::new(tidb_datatype::FieldTypeCode::Datetime),
            vec![],
        ).unwrap();

        // Fold once under clock 1000.
        let mut expr = now_node.clone();
        fold_constant_in_mode(&mut expr, &Clock(1000), ConstantFoldMode::Try);

        // The folded constant must carry the deferred provenance: evaluating
        // the SAME folded node under a LATER statement clock must serve the
        // fresh value, not the folding-time one (Go `Constant.DeferredExpr`,
        // expression_rewriter.go:3016-3029).
        // Re-executing under a LATER statement clock must re-evaluate: the
        // folding-time value (00:16:40) must not be served again.
        let later_value = expr
            .eval(
                &Clock(5000),
                tidb_chunk::chunk::Chunk::new(&[], 1, 1).get_row(0),
            )
            .unwrap();
        let later = later_value.sql_string().unwrap();
        assert!(
            later.contains("01:23:20"),
            "clock 5000 must render 01:23:20, got {later}"
        );
        // And the first execution's own value stayed at its own clock.
        let first_value = expr
            .eval(
                &Clock(1000),
                tidb_chunk::chunk::Chunk::new(&[], 1, 1).get_row(0),
            )
            .unwrap();
        let first = first_value.sql_string().unwrap();
        assert!(
            first.contains("00:16:40"),
            "clock 1000 must render 00:16:40, got {first}"
        );
    }
}

/// Re-homes the warnings a fold emitted straight into a LIVE statement
/// context: the statement boundary's warning reset clears the live list
/// between planning and execution, so entries appended during PLANNING
/// never reach `SHOW WARNINGS`. Moving the fold's delta into the thread
/// stash hands them to the record-set's post-execution drain
/// (`tidb_context::fold_warnings::take_fold_warnings`), which runs after the reset.
pub fn move_live_warnings_to_stash(ctx: &dyn crate::Columns, bookmark: usize) {
    if ctx.warning_count() > bookmark {
        for warning in ctx.take_warnings_since(bookmark) {
            tidb_context::fold_warnings::record_fold_sql_warn(warning);
        }
    }
}


/// The columns context the fold evaluates against: reads like
/// [`ZonedNoColumns`](crate::ZonedNoColumns) but every warning the evaluated
/// expression raises lands in [`tidb_context::fold_warnings::record_fold_warning`] instead of being
/// dropped.
pub struct FoldWarningContext {
    zone: tidb_datatype::SessionTimeZone,
}

impl FoldWarningContext {
    /// Builds one folding context for the given statement time zone.
    #[must_use]
    pub fn new(zone: tidb_datatype::SessionTimeZone) -> Self {
        Self { zone }
    }
}

impl Columns for FoldWarningContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone.clone()
    }

    fn append_warning(&self, code: u16, message: &str) {
        tidb_context::fold_warnings::record_fold_warning(code, message);
    }

    fn append_error_warning(&self, error: &tidb_mysql::errors::SqlError) {
        tidb_context::fold_warnings::record_fold_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            error.code,
            error.message.clone(),
            error.identity,
        ));
    }

    fn warning_count(&self) -> usize {
        tidb_context::fold_warnings::fold_warnings_len()
    }

    fn truncate_warnings(&self, bookmark: usize) {
        tidb_context::fold_warnings::truncate_fold_warnings(bookmark);
    }
}
