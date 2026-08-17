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

//! CNF/DNF normal-form handling.
//!
//! Go sources: `pkg/expression/expression.go:851`-`:945` (the splitters and
//! flatteners), `pkg/expression/util.go:1205`-`:1330` (the DNF filter
//! extraction and relaxation), `pkg/expression/schema.go:134`
//! (`ExprFromSchema`, needed by `DeriveRelaxedFiltersFromDNF`).
//!
//! The COMPOSERS -- Go `ComposeCNFCondition` / `ComposeDNFCondition` -- are
//! already ported in `simple_expr.rs` and are used from here, not rewritten.

use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::schema::Schema;
use crate::simple_expr::{compose_cnf_condition, compose_dnf_condition};
use std::collections::BTreeMap;

/// Go `splitNormalFormItems` (`expression.go:920`): splits a normal form at
/// its TOP level only.
///
/// The difference from `extractBinaryOpItems` is real but subtle: this one
/// recurses only while the node still IS `func_name`, so it never descends
/// past a leaf. In practice on an `and`/`or` tree the two agree; they differ
/// on an input that is not a scalar function at all, where this returns the
/// single item.
fn split_normal_form_items(on_expr: &Expression, func_name: &str) -> Vec<Expression> {
    if let Expression::ScalarFunction(function) = on_expr {
        if function.func_name.lowercase() == func_name {
            return function
                .get_args()
                .iter()
                .flat_map(|arg| split_normal_form_items(arg, func_name))
                .collect();
        }
    }
    vec![on_expr.clone()]
}

/// Go `SplitCNFItems` (`expression.go:939`): the conjuncts of `on_expr`.
///
/// `a AND b AND c` becomes `[a, b, c]`. A non-`AND` expression is a
/// one-element CNF, so the result is never empty.
#[must_use]
pub fn split_cnf_items(on_expr: &Expression) -> Vec<Expression> {
    split_normal_form_items(on_expr, "and")
}

/// Go `SplitDNFItems` (`expression.go:945`): the disjuncts of `on_expr`.
///
/// `a OR b OR c` becomes `[a, b, c]`. A non-`OR` expression is a one-element
/// DNF, so the result is never empty.
#[must_use]
pub fn split_dnf_items(on_expr: &Expression) -> Vec<Expression> {
    split_normal_form_items(on_expr, "or")
}

/// Go `extractBinaryOpItems` (`expression.go:851`): the leaves of a nested
/// binary-operator tree.
fn extract_binary_op_items(function: &ScalarFunction, func_name: &str) -> Vec<Expression> {
    let mut result = Vec::new();
    for arg in function.get_args() {
        match arg {
            Expression::ScalarFunction(inner) if inner.func_name.lowercase() == func_name => {
                result.extend(extract_binary_op_items(inner, func_name));
            }
            other => result.push(other.clone()),
        }
    }
    result
}

/// Go `FlattenDNFConditions` (`expression.go:865`): the leaves of a nested
/// `OR` tree.
///
/// `or(or(a=1, a=2), or(a=3, a=4))` becomes `[a=1, a=2, a=3, a=4]`.
///
/// The caller has already established that `dnf_condition` IS the `OR`, which
/// is why the argument is a [`ScalarFunction`] and not an `Expression`: unlike
/// [`split_dnf_items`], this one descends into the ARGUMENTS first and so
/// would silently treat a non-`OR` root as a one-level tree.
#[must_use]
pub fn flatten_dnf_conditions(dnf_condition: &ScalarFunction) -> Vec<Expression> {
    extract_binary_op_items(dnf_condition, "or")
}

/// Go `FlattenCNFConditions` (`expression.go:871`): the leaves of a nested
/// `AND` tree.
///
/// `and(and(a>1, a>2), and(a>3, a>4))` becomes `[a>1, a>2, a>3, a>4]`.
#[must_use]
pub fn flatten_cnf_conditions(cnf_condition: &ScalarFunction) -> Vec<Expression> {
    extract_binary_op_items(cnf_condition, "and")
}

/// Go `ExprFromSchema` (`schema.go:134`): whether every column `expr` reads
/// comes from `schema`.
///
/// A correlated column and a constant are always "from" the schema -- they are
/// fixed with respect to it. A column NOT in the schema makes the whole
/// expression foreign.
#[must_use]
pub fn expr_from_schema(expr: &Expression, schema: &Schema) -> bool {
    match expr {
        Expression::Column(column) => schema.contains(column),
        Expression::ScalarFunction(function) => function
            .get_args()
            .iter()
            .all(|arg| expr_from_schema(arg, schema)),
        Expression::CorrelatedColumn(_) | Expression::Constant(_) => true,
    }
}

/// The hash code of `expr`, as Go's `expr.HashCode()` returns it.
///
/// `Expression::hash_code` fills a lazy cache and therefore needs `&mut`; the
/// callers below hold shared references, so they pay one clone. The code is
/// structural and context-free, so the clone changes nothing observable.
fn hash_code_of(expr: &Expression) -> Vec<u8> {
    expr.clone().hash_code().to_vec()
}

/// Go `extractFiltersFromDNF` (`util.go:1222`): lifts out the conjuncts that
/// occur in EVERY disjunct of `dnf_func`, and returns what remains.
///
/// `(a AND b) OR (a AND c)` yields `([a], b OR c)`. When a disjunct consists
/// only of lifted conjuncts the whole DNF is implied by them, so the remainder
/// is `None` and the caller deletes the original condition.
fn extract_filters_from_dnf(dnf_func: &ScalarFunction) -> (Vec<Expression>, Option<Expression>) {
    let dnf_items = flatten_dnf_conditions(dnf_func);
    let mut code_map: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let mut hashcode_to_expr: BTreeMap<Vec<u8>, Expression> = BTreeMap::new();

    for (index, dnf_item) in dnf_items.iter().enumerate() {
        // Go's `innerMap` guards against `(t.a=t1.a AND t.a=t1.a) OR (...)`,
        // where one disjunct mentions the same conjunct twice: it must count
        // ONCE, or the "occurs in every disjunct" test passes on a repeat.
        let mut inner_seen: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        for cnf_item in split_cnf_items(dnf_item) {
            let code = hash_code_of(&cnf_item);
            if index == 0 {
                code_map.insert(code.clone(), 1);
                hashcode_to_expr.insert(code, cnf_item);
            } else if code_map.contains_key(&code) && inner_seen.insert(code.clone(), ()).is_none()
            {
                *code_map
                    .get_mut(&code)
                    .expect("presence checked immediately above") += 1;
            }
        }
    }

    // Keep only the conjuncts seen in every disjunct.
    let disjunct_count = dnf_items.len();
    for (hashcode, count) in &code_map {
        if *count < disjunct_count {
            hashcode_to_expr.remove(hashcode);
        }
    }
    if hashcode_to_expr.is_empty() {
        return (
            Vec::new(),
            Some(Expression::ScalarFunction(dnf_func.clone())),
        );
    }

    let mut new_dnf_items = Vec::with_capacity(dnf_items.len());
    let mut only_need_extracted = false;
    for dnf_item in &dnf_items {
        let cnf_items = split_cnf_items(dnf_item);
        let mut new_cnf_items = Vec::with_capacity(cnf_items.len());
        for cnf_item in cnf_items {
            if !hashcode_to_expr.contains_key(&hash_code_of(&cnf_item)) {
                new_cnf_items.push(cnf_item);
            }
        }
        // A disjunct that is ENTIRELY made of extracted conjuncts makes the
        // whole DNF equal to the extracted part.
        if new_cnf_items.is_empty() {
            only_need_extracted = true;
            break;
        }
        if let Some(composed) = compose_cnf_condition(new_cnf_items) {
            new_dnf_items.push(composed);
        }
    }

    // Go sorts the extracted filters by hash code before returning so plan and
    // test output stay deterministic; a `BTreeMap` keyed by that same hash code
    // already iterates in exactly that order.
    let extracted: Vec<Expression> = hashcode_to_expr.into_values().collect();
    if only_need_extracted {
        return (extracted, None);
    }
    (extracted, compose_dnf_condition(new_dnf_items))
}

/// Go `ExtractFiltersFromDNFs` (`util.go:1205`): for each DNF condition in
/// `conditions`, lifts the conjuncts common to all its disjuncts up into the
/// top-level conjunction.
///
/// The lifted filters are APPENDED to the end of the returned slice, and each
/// original DNF is either replaced by its remainder or deleted outright. Go
/// walks `conditions` BACKWARDS so that deleting an element cannot disturb a
/// not-yet-visited index; the same loop is kept here for the same reason, and
/// because it also fixes the order the extracted filters are appended in.
#[must_use]
pub fn extract_filters_from_dnfs(mut conditions: Vec<Expression>) -> Vec<Expression> {
    let mut all_extracted = Vec::new();
    for index in (0..conditions.len()).rev() {
        let Expression::ScalarFunction(function) = &conditions[index] else {
            continue;
        };
        if function.func_name.lowercase() != "or" {
            continue;
        }
        let (extracted, remained) = extract_filters_from_dnf(function);
        all_extracted.extend(extracted);
        match remained {
            Some(remained) => conditions[index] = remained,
            None => {
                conditions.remove(index);
            }
        }
    }
    conditions.extend(all_extracted);
    conditions
}

/// Go `DeriveRelaxedFiltersFromDNF` (`util.go:1296`): weakens a DNF condition
/// to one that mentions only columns of `schema`.
///
/// The result is a SUPERSET of the original: every row satisfying the original
/// satisfies the derived filter, so it is safe to push down early. `None` is
/// Go's nil and means the derived expression is the universal set -- no useful
/// filter could be derived.
///
/// For `t1`'s schema, `(t1.a=1 AND t2.a=1) OR (t1.a=2 AND t2.a=2)` relaxes to
/// `t1.a=1 OR t1.a=2`, while `t1.a=1 OR t2.a=1` relaxes to nothing.
#[must_use]
pub fn derive_relaxed_filters_from_dnf(expr: &Expression, schema: &Schema) -> Option<Expression> {
    let Expression::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func_name.lowercase() != "or" {
        return None;
    }
    let dnf_items = flatten_dnf_conditions(function);
    let mut new_dnf_items = Vec::with_capacity(dnf_items.len());
    for dnf_item in &dnf_items {
        let cnf_items = split_cnf_items(dnf_item);
        let mut new_cnf_items = Vec::with_capacity(cnf_items.len());
        for cnf_item in cnf_items {
            let is_nested_dnf = matches!(
                &cnf_item,
                Expression::ScalarFunction(inner) if inner.func_name.lowercase() == "or"
            );
            if is_nested_dnf {
                // A nested DNF relaxes recursively; if IT relaxes to the
                // universal set, the conjunct simply drops.
                if let Some(relaxed) = derive_relaxed_filters_from_dnf(&cnf_item, schema) {
                    new_cnf_items.push(relaxed);
                }
                continue;
            }
            // A simple conjunct survives only if the schema fully covers it.
            if expr_from_schema(&cnf_item, schema) {
                new_cnf_items.push(cnf_item);
            }
        }
        // A disjunct mentioning no column of the schema is unconstrained, so
        // the whole disjunction is the universal set.
        if new_cnf_items.is_empty() {
            return None;
        }
        if let Some(composed) = compose_cnf_condition(new_cnf_items) {
            new_dnf_items.push(composed);
        }
    }
    compose_dnf_condition(new_dnf_items)
}
