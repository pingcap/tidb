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

//! The MIN/MAX ordering of Go `pkg/executor/aggfuncs` (`func_max_min.go`),
//! shared by the root aggregation and the partial stage a scan computes over
//! its staged rows.

use tidb_datatype::CollationCollator;
use tidb_datatype::{Collation, Datum};
use tidb_expr::expression::Expression;

/// The collation a MIN/MAX partial orders under: its ARGUMENT expression's
/// derived one.
///
/// Go builds `collate.GetCollator(RetTp.GetCollate())` for every MIN/MAX
/// signature (`aggfuncs/builder.go:460-468`), and `RetTp` is the aggregate
/// ARGUMENT's type -- so a `utf8mb4_general_ci` column orders
/// case-insensitively and a computed argument such as `UPPER(s)` orders under
/// whatever IT derived. A partial aggregate computed at the scan has to use
/// the same collation as the root one or the two disagree: over 'a', 'B', 'A'
/// in `utf8mb4_general_ci`, a binary comparison answers max='a' min='A' where
/// TiDB answers max='B' min='a'.
#[must_use]
pub fn extreme_collation(input: Option<&Expression>) -> Collation {
    input.map_or(Collation::DEFAULT, |expression| {
        tidb_expr::collation_derive::collation_of_node(expression)
    })
}

/// The MIN/MAX ordering of `candidate` against `current`. An ENUM or SET
/// orders by its NAME under the collator -- Go `maxMin4Enum`/`maxMin4Set`,
/// and the cop's `Datum.Compare` (`compareMysqlEnum`) -- so MIN over
/// enum('a','B','c') under general_ci is 'a', where the binary bytes would
/// pick 'B'.
///
/// # Errors
///
/// An unordered scalar pair, as [`tidb_expr::compare_datums_with_collation`].
pub fn compare_extreme(
    candidate: &Datum,
    current: &Datum,
    collation: Collation,
) -> Result<std::cmp::Ordering, tidb_expr::EvalError> {
    fn name(datum: &Datum) -> Option<&[u8]> {
        match datum {
            Datum::Enum(value, _) => Some(value.name_bytes()),
            Datum::Set(value, _) => Some(value.name_bytes()),
            _ => None,
        }
    }
    if let (Some(left), Some(right)) = (name(candidate), name(current)) {
        return Ok(collation.compare(left, right));
    }
    tidb_expr::compare_datums_with_collation(candidate, current, collation)
}

/// Whether `candidate` becomes the new MIN/MAX, under [`extreme_collation`].
#[must_use]
pub fn extreme_replaces(
    candidate: &Datum,
    current: &Datum,
    is_max: bool,
    collation: Collation,
) -> bool {
    compare_extreme(candidate, current, collation).is_ok_and(|ordering| {
        if is_max {
            ordering.is_gt()
        } else {
            ordering.is_lt()
        }
    })
}
