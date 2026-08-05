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

use crate::expression::Expression;
use tidb_datatype::Datum;

/// Runs the NOT NULL half of Go's `foldConstant`
/// (`pkg/expression/constant_fold.go`) over a freshly built tree, bottom up.
///
/// Go REPLACES a wholly-constant subtree with a `Constant`, and while doing so
/// stamps `NotNullFlag` on or off according to the value it computed. That
/// flag is the only part of the substitution anything downstream of this
/// rewriter can observe -- an evaluable tree gives the same rows either way --
/// so this walk computes the same fold and keeps only its flag, rather than
/// rewriting nodes this tier still evaluates directly.
///
/// The flag is not decoration: `SHOW COLUMNS` on a view reads it, which is how
/// `CREATE VIEW v AS SELECT CAST('' AS CHAR(32))` reports `NO` for a column no
/// base table declared NOT NULL.
pub fn derive_constant_null_flag(expr: &mut Expression) {
    let _ = fold_value(expr);
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
        Expression::Constant(constant) => return Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => return None,
        Expression::ScalarFunction(func) => func,
    };
    if is_unfoldable(func.func_name.lowercase()) {
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
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let value = expr
        .eval(&crate::context::NoColumns, chunk.get_row(0))
        .ok()?;
    if !has_null_arg {
        let Expression::ScalarFunction(func) = expr else {
            unreachable!("matched as a scalar function above")
        };
        if let Some(ret_type) = func.ret_type.as_mut() {
            if matches!(value, Datum::Null) {
                ret_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
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
        Expression::Column(_) | Expression::CorrelatedColumn(_) => false,
        Expression::ScalarFunction(func) => {
            !is_unfoldable(func.func_name.lowercase()) && func.args.iter().all(folds_to_constant)
        }
    }
}

/// The `*Constant`'s VALUE that Go's dispatch reads off a folded subtree --
/// `unaryMinusFunctionClass.handleIntOverflow` is the one rule here that needs
/// the value and not merely the fact of constancy (`arg.Value.GetInt64()`).
///
/// `None` for anything [`folds_to_constant`] rejects, and for a fold whose
/// evaluation fails -- which is Go's own `err != nil` arm, where the subtree
/// stays unfolded and the type switch therefore misses it. The evaluation runs
/// against no columns because a foldable subtree reads none; that is exactly
/// the `chunk.Row{}` Go's `foldConstant` passes.
pub(crate) fn folded_value(expr: &Expression) -> Option<Datum> {
    if let Expression::Constant(constant) = expr {
        return Some(constant.value.clone());
    }
    if !folds_to_constant(expr) {
        return None;
    }
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expr.eval(&crate::context::NoColumns, chunk.get_row(0)).ok()
}

/// Go `unFoldableFunctions` (`pkg/expression/function_traits.go`): the
/// functions whose result is not a property of their arguments -- a clock, a
/// counter, a random source, a session variable, or a side effect.
fn is_unfoldable(name: &str) -> bool {
    // Go's `GetVar` is one name; this rewriter encodes the signature the
    // session's current value picked into the name (`getvar_int`,
    // `getvar_string`, ...), so a bare `"getvar"` arm below would never match
    // anything the rewriter builds. The prefix test is what actually fires.
    if name.starts_with("getvar_") {
        return true;
    }
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
            | "getparam"
            | "benchmark"
            | "dayname"
            | "nextval"
            | "lastval"
            | "setval"
            | "any_value"
    )
}
