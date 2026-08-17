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

//! The function-trait tables from `pkg/expression/function_traits.go` and
//! `pkg/expression/collation.go` that the `util.go` predicates are defined
//! against.
//!
//! `unFoldableFunctions` was ALREADY PORTED, privately, in
//! `scalar_function.rs` (where `ConstLevel` needs it). It is re-exported here
//! rather than written twice. `constant_fold.rs` keys the same idea on the
//! signature-suffixed names the AST rewriter mints (`getvar_int`, ...); the
//! shared table already accepts both spellings.

/// Go `unFoldableFunctions` (`function_traits.go:48`): functions whose result
/// is not a property of their arguments -- a clock, a counter, a random
/// source, a session variable, a sequence, or a side effect.
///
/// `name` must be Go's `FuncName.L`, i.e. already lowercased. This is the
/// table already ported in `scalar_function.rs`; the alias exists so a reader
/// following `util.go` finds it under the name `util.go`'s predicates use.
pub use crate::scalar_function::is_unfoldable_function;

/// Go `mutableEffectsFunctions` (`function_traits.go:224`): functions that are
/// mutable or have side effects.
///
/// The time functions are here for the reason Go's comment gives: `SYSDATE()`
/// re-reads the clock per call while `NOW()` does not, and treating the whole
/// family as mutable is the safe side of that difference.
#[must_use]
pub fn is_mutable_effects_function(name: &str) -> bool {
    matches!(
        name,
        "now"
            | "current_timestamp"
            | "utc_time"
            | "curtime"
            | "current_time"
            | "utc_timestamp"
            | "unix_timestamp"
            | "sysdate"
            | "curdate"
            | "current_date"
            | "utc_date"
            | "rand"
            | "random_bytes"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "uuid_short"
            | "sleep"
            | "setvar"
            | "getvar"
            | "any_value"
    )
}

/// Go `CollationStrictnessGroup` (`collation.go:177`): the strictness group of
/// a collation, or `None` for a collation Go's map does not list.
#[must_use]
pub fn collation_strictness_group(collation: &str) -> Option<i32> {
    match collation {
        "utf8_general_ci" | "utf8mb4_general_ci" => Some(1),
        "utf8_unicode_ci" | "utf8mb4_unicode_ci" => Some(2),
        "ascii_bin" | "latin1_bin" | "utf8_bin" | "utf8mb4_bin" => Some(3),
        "binary" => Some(4),
        _ => None,
    }
}

/// Go `CollationStrictness` (`collation.go:192`): the groups that are STRICTER
/// than `group`. An unequal ordering that holds in a weak collation still
/// holds in every stricter one.
#[must_use]
pub fn stricter_collation_groups(group: i32) -> &'static [i32] {
    match group {
        1 | 2 => &[3, 4],
        3 => &[4],
        4 => &[],
        _ => &[],
    }
}

/// Go `checkCollationStrictness` (`util.go:717`): true iff `new_func_coll` is
/// NOT WEAKER than `coll`.
#[must_use]
pub fn check_collation_strictness(coll: &str, new_func_coll: &str) -> bool {
    let (Some(coll_group), Some(new_group)) = (
        collation_strictness_group(coll),
        collation_strictness_group(new_func_coll),
    ) else {
        return false;
    };
    coll_group == new_group || stricter_collation_groups(coll_group).contains(&new_group)
}

/// Go `logicalOps` (`util.go:860`): the functions that already RETURN a truth
/// value.
///
/// `wrapWithIsTrue` reads this table to decide whether an `istrue` wrapper
/// would be redundant. Note what is NOT here: arithmetic, `CAST`, and a bare
/// column all need the wrapper, because an integer is not the same thing as a
/// truth value once `NOT` is pushed away from it.
#[must_use]
pub fn is_logical_op(name: &str) -> bool {
    matches!(
        name,
        "lt" | "ge"
            | "gt"
            | "le"
            | "eq"
            | "ne"
            | "not"
            | "like"
            | "and"
            | "or"
            | "xor"
            | "in"
            | "isnull"
            | "isfalse"
            | "istrue"
            | "istrue_with_null"
            | "nulleq"
            | "regexp"
    )
}

/// Go `oppositeOp` (`util.go:881`): the operator that `NOT f(...)` becomes.
///
/// `None` for a function with no opposite, which is what `pushNotAcrossExpr`'s
/// `switch` already restricts itself to.
#[must_use]
pub fn opposite_op(name: &str) -> Option<&'static str> {
    Some(match name {
        "lt" => "ge",
        "ge" => "lt",
        "gt" => "le",
        "le" => "gt",
        "eq" => "ne",
        "ne" => "eq",
        "or" => "and",
        "and" => "or",
        _ => return None,
    })
}
