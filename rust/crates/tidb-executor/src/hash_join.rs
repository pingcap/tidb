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

//! The equal-condition analysis and hash-key encoding behind [`JoinExec`]'s
//! hash path (Go `pkg/executor/join/hash_join_v2.go` plus the
//! `LogicalJoin.EqualConditions` split the planner performs above it).
//!
//! # What the hash table is, and is not
//!
//! The table here is a PROBE ACCELERATOR, not a second implementation of `=`.
//! Every candidate pair a bucket produces is still handed to the join's own
//! `ON` evaluation, exactly as the nested loop hands it every pair. That
//! leaves one -- and only one -- correctness obligation on this module:
//!
//! > if `eq(a, b)` evaluates TRUE, then `key(a) == key(b)`.
//!
//! False POSITIVES (two unequal values that collide into one bucket) cost a
//! condition evaluation and are then rejected; false NEGATIVES would silently
//! drop rows. So every rule below is written to be conservative: a key shape
//! this module cannot encode EXACTLY makes the whole join fall back to the
//! nested loop rather than hash a guess.
//!
//! # Why a class, and why it must match on both sides
//!
//! `t.a = s.a` does not mean "the bytes are equal": MySQL picks a comparison
//! DOMAIN from the operand types, and `eval_binary_full` reproduces that
//! dispatch (float dominates decimal dominates integer; two byte-valued
//! operands compare under a collation instead). An `INT = DOUBLE` join key
//! compares in the float domain, where `1` and `1.0` are equal but hash
//! differently under any exact integer encoding -- a false negative.
//!
//! The gate is therefore that BOTH key columns have the same
//! [`EvalType`], which pins the domain, and that the domain is one this
//! module can encode injectively. Mixed domains keep the nested loop.
//!
//! # NULL
//!
//! `eq` is never TRUE when either operand is `NULL` -- not even for two
//! `NULL`s -- so a `NULL` key never matches anything, including another
//! `NULL`. That is the same rule `tidb-exec`'s `join_key_eq` states. Here it
//! is expressed structurally: a row whose key contains `NULL` produces no
//! key at all, so it is never inserted into the table and never probes it.
//! A NaN `DOUBLE` is treated the same way, because `NaN = NaN` is false.

use std::collections::HashMap;
use tidb_datatype::{Collation, Datum, EvalType, FieldType};
use tidb_expr::expression::Expression;

/// The comparison domain a hash join key column is encoded in.
///
/// One variant per arm of `eval_binary_full`'s comparison dispatch that this
/// module can encode injectively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyClass {
    /// Both keys are integral. Go `types.CompareInt` (and this tier's
    /// `integer_cmp`) never reinterprets one side's bit pattern in the
    /// other's domain, so the key is the value's exact `i128` position:
    /// `-1` and `2^64-1` land in different buckets, as they must.
    Int,
    /// Both keys are floating point; the comparison promotes to `f64`.
    Real,
    /// Both keys are exact decimals; the comparison is exact and
    /// scale-insensitive, so the key is Go `MyDecimal.ToHashKey`.
    Decimal,
    /// Both keys are byte-valued (`CHAR`/`VARCHAR`/`BINARY`/`ENUM`/`SET`).
    /// The key is the comparison collation's SORT KEY, which is what makes a
    /// case-insensitive join match `'a'` with `'A'` and a PAD SPACE
    /// collation match `'a'` with `'a  '` -- the same rule the `GROUP BY`
    /// path applies in `hash_agg.rs`'s `group_key_part`.
    Str(Collation),
}

impl KeyClass {
    /// The class two key columns of `field` type share, or `None` when their
    /// comparison domain is one this module refuses to encode.
    ///
    /// `collation` is the one the expression derivation stamped on the `eq`
    /// itself, which is the collation its own evaluation runs under -- so a
    /// string key hashes under exactly the rule that will later be asked to
    /// confirm the match.
    ///
    /// `Datetime`/`Timestamp`/`Duration`/`Json`/`VectorFloat32` are
    /// deliberately absent: their equality involves timezone resolution,
    /// fractional-second precision, or structural folding that this unit has
    /// not proven injective, and an unproven key is a dropped row.
    fn of(left: &FieldType, right: &FieldType, collation: Collation) -> Option<Self> {
        let eval_type = left.eval_type();
        if eval_type != right.eval_type() {
            return None;
        }
        match eval_type {
            EvalType::Int => Some(KeyClass::Int),
            EvalType::Real => Some(KeyClass::Real),
            EvalType::Decimal => Some(KeyClass::Decimal),
            EvalType::String => Some(KeyClass::Str(collation)),
            _ => None,
        }
    }
}

/// One `probe.col = build.col` conjunct the hash table indexes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EquiKey {
    /// The key column's offset inside a LEFT-child row.
    pub(crate) left: usize,
    /// The key column's offset inside a RIGHT-child row.
    pub(crate) right: usize,
    /// The domain both offsets are encoded in.
    pub(crate) class: KeyClass,
}

/// The `ON` clause split the way Go's planner splits `LogicalJoin`:
/// `EqualConditions` that the hash table can index, and everything else.
pub(crate) struct EquiSplit {
    /// The indexable `col = col` conjuncts, in `ON` order (Go prints them in
    /// this order inside `equal:[...]`).
    pub(crate) keys: Vec<EquiKey>,
    /// One flag per flattened `ON` conjunct: `true` where that conjunct
    /// became an [`EquiKey`]. `EXPLAIN` renders the `true` positions as
    /// `equal:[...]` and the rest as `other cond:`, so the printed plan and
    /// the executed plan cannot drift apart.
    pub(crate) equal_mask: Vec<bool>,
}

/// Flattens an `AND` tree into its conjuncts, left to right.
///
/// Go's `expression.SplitCNFItems`; the driver hands the join a single
/// rewritten `ON` expression, so the split happens here rather than at the
/// plan layer.
pub(crate) fn split_conjuncts(expr: &Expression) -> Vec<&Expression> {
    let mut out = Vec::new();
    push_conjuncts(expr, &mut out);
    out
}

fn push_conjuncts<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    if let Expression::ScalarFunction(f) = expr {
        if f.func_name.lowercase() == "and" && f.args.len() == 2 {
            push_conjuncts(&f.args[0], out);
            push_conjuncts(&f.args[1], out);
            return;
        }
    }
    out.push(expr);
}

/// Splits `conditions` (the join's `ON` clause) into hash keys and the rest.
///
/// `left_width` is the number of columns the LEFT child contributes; the
/// join evaluates its conditions against the concatenated row, so a column
/// whose index is below `left_width` belongs to the left child and any other
/// to the right.
///
/// A conjunct becomes a key only when it is literally `eq(<col>, <col>)`
/// with one column from each side. A cast, a computed expression, or two
/// columns from the SAME side is left in the residual set: the first two
/// would need the key to be built from an evaluated expression (Go does
/// that; this unit does not), and the third is a filter, not a join key.
pub(crate) fn split_equi(conditions: &[Expression], left_width: usize) -> EquiSplit {
    let mut keys = Vec::new();
    let mut equal_mask = Vec::new();
    for condition in conditions {
        for conjunct in split_conjuncts(condition) {
            let key = equi_key(conjunct, left_width);
            equal_mask.push(key.is_some());
            keys.extend(key);
        }
    }
    EquiSplit { keys, equal_mask }
}

fn equi_key(conjunct: &Expression, left_width: usize) -> Option<EquiKey> {
    let Expression::ScalarFunction(f) = conjunct else {
        return None;
    };
    if f.func_name.lowercase() != "eq" || f.args.len() != 2 {
        return None;
    }
    let (Expression::Column(a), Expression::Column(b)) = (&f.args[0], &f.args[1]) else {
        return None;
    };
    let (a_index, b_index) = (
        usize::try_from(a.index).ok()?,
        usize::try_from(b.index).ok()?,
    );
    let (left_col, right_col, left, right) = match (a_index < left_width, b_index < left_width) {
        (true, false) => (a, b, a_index, b_index - left_width),
        (false, true) => (b, a, b_index, a_index - left_width),
        // Both columns on one side: a filter this join applies, not a key
        // that pairs the two children.
        _ => return None,
    };
    let class = KeyClass::of(
        left_col.ret_type.as_ref()?,
        right_col.ret_type.as_ref()?,
        f.derived_collation(),
    )?;
    Some(EquiKey { left, right, class })
}

/// Encodes one key column of one row, or `None` when the value can never
/// satisfy `eq` (a `NULL`, or a NaN `DOUBLE`).
///
/// # Errors
/// [`KeyError`] when the datum is outside the statically determined class.
/// The class comes from the key columns' own field types and the chunks are
/// typed, so this is an invariant violation rather than a data condition --
/// it is surfaced instead of guessed, because a guess here silently drops
/// rows.
fn key_part(class: KeyClass, datum: &Datum) -> Result<Option<Vec<u8>>, KeyError> {
    if matches!(datum, Datum::Null) {
        return Ok(None);
    }
    Ok(match class {
        // `integer_cmp` orders a signed and an unsigned operand on one number
        // line without reinterpreting either bit pattern, so the exact `i128`
        // position of the value IS its equality class.
        KeyClass::Int => match datum {
            Datum::Int(value) => Some(i128::from(*value).to_be_bytes().to_vec()),
            Datum::UInt(value) => Some(i128::from(*value).to_be_bytes().to_vec()),
            _ => return Err(KeyError),
        },
        // A float comparison promotes an integral operand to `f64`, so an
        // integral datum in a float key column encodes as its `f64` value.
        // `-0.0 == 0.0` is TRUE, so the two must share a key; `NaN = NaN` is
        // FALSE, so a NaN never gets one.
        KeyClass::Real => {
            let value = match datum {
                Datum::Real(value) | Datum::Float32(value) => *value,
                Datum::Int(value) => *value as f64,
                Datum::UInt(value) => *value as f64,
                _ => return Err(KeyError),
            };
            if value.is_nan() {
                None
            } else if value == 0.0 {
                Some(0.0f64.to_be_bytes().to_vec())
            } else {
                Some(value.to_be_bytes().to_vec())
            }
        }
        // Decimal comparison is exact and promotes an integral operand to a
        // scale-0 decimal. `MyDecimal.ToHashKey` is Go's own answer to
        // "equal decimals, equal bytes" -- it normalizes away the trailing
        // zeros that make `1.0` and `1.00` differ in representation.
        KeyClass::Decimal => {
            let value = match datum {
                Datum::Decimal(value) => value.clone(),
                Datum::Int(value) => tidb_datatype::Decimal::from_int(*value),
                Datum::UInt(value) => tidb_datatype::Decimal::from_uint(*value),
                _ => return Err(KeyError),
            };
            Some(value.to_hash_key().map_err(|_| KeyError)?.0)
        }
        // Two byte-valued operands compare under the collation the
        // expression derivation stamped on this `eq`; its sort key is equal
        // exactly when the collation calls the values equal.
        KeyClass::Str(collation) => match datum.as_raw_bytes() {
            Some(bytes) => Some(collation.key(bytes)),
            None => return Err(KeyError),
        },
    })
}

/// A datum outside its key column's statically determined class.
#[derive(Debug)]
pub(crate) struct KeyError;

/// The whole key of one row: every key column's encoding, length-prefixed so
/// two columns cannot borrow each other's bytes (`('ab', 'c')` and
/// `('a', 'bc')` are different keys).
///
/// `None` when any part is `None` -- one unmatched key column is enough to
/// make the row match nothing.
pub(crate) fn row_key(
    keys: &[EquiKey],
    row: &[Datum],
    offset: impl Fn(&EquiKey) -> usize,
) -> Result<Option<Vec<u8>>, KeyError> {
    let mut encoded = Vec::new();
    for key in keys {
        let Some(part) = key_part(key.class, &row[offset(key)])? else {
            return Ok(None);
        };
        encoded.extend_from_slice(&(part.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&part);
    }
    Ok(Some(encoded))
}

/// The materialized build side: the rows themselves plus, per key, the build
/// row indices that carry it IN BUILD ORDER.
///
/// The order is not incidental. The nested loop this replaces emits, for one
/// probe row, its matches in build-input order; keeping each bucket sorted
/// by build index is what makes the hash join's output byte-identical to it
/// rather than merely equivalent as a set.
pub(crate) struct BuildTable {
    pub(crate) rows: Vec<Vec<Datum>>,
    buckets: HashMap<Vec<u8>, Vec<u32>>,
}

impl BuildTable {
    /// Indexes `rows` by their key.
    ///
    /// # Errors
    /// [`KeyError`] from [`key_part`].
    pub(crate) fn build(
        rows: Vec<Vec<Datum>>,
        keys: &[EquiKey],
        build_is_left: bool,
    ) -> Result<Self, KeyError> {
        let offset = |key: &EquiKey| if build_is_left { key.left } else { key.right };
        let mut buckets: HashMap<Vec<u8>, Vec<u32>> = HashMap::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            // A row whose key holds a NULL matches nothing, so it is simply
            // not indexed. The build side is always the NON-preserved one,
            // so no outer-join row is lost by dropping it here.
            if let Some(key) = row_key(keys, row, offset)? {
                buckets.entry(key).or_default().push(index as u32);
            }
        }
        Ok(BuildTable { rows, buckets })
    }

    /// The build rows that could match `key`, in build order.
    pub(crate) fn probe(&self, key: &[u8]) -> &[u32] {
        self.buckets.get(key).map_or(&[], Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::Decimal;

    fn int_key(datum: &Datum) -> Option<Vec<u8>> {
        key_part(KeyClass::Int, datum).unwrap()
    }

    /// The signed/unsigned boundary the nested loop's `join_key_eq` calls
    /// out: neither `-1` nor `2^63` may be reinterpreted into the other's
    /// domain, so they must not share a bucket with anything on the far side.
    #[test]
    fn int_keys_do_not_reinterpret_across_signedness() {
        assert_ne!(int_key(&Datum::Int(-1)), int_key(&Datum::UInt(u64::MAX)));
        assert_ne!(
            int_key(&Datum::Int(i64::MIN)),
            int_key(&Datum::UInt(1 << 63))
        );
        // The same VALUE in either domain still collides, as it must.
        assert_eq!(int_key(&Datum::Int(7)), int_key(&Datum::UInt(7)));
    }

    /// A `NULL` key never matches -- not even another `NULL`.
    #[test]
    fn null_produces_no_key() {
        assert!(int_key(&Datum::Null).is_none());
        assert!(key_part(KeyClass::Real, &Datum::Null).unwrap().is_none());
        assert!(key_part(KeyClass::Decimal, &Datum::Null).unwrap().is_none());
    }

    /// `NaN = NaN` is FALSE, and `-0.0 = 0.0` is TRUE.
    #[test]
    fn real_key_follows_float_equality() {
        assert!(key_part(KeyClass::Real, &Datum::Real(f64::NAN))
            .unwrap()
            .is_none());
        assert_eq!(
            key_part(KeyClass::Real, &Datum::Real(-0.0)).unwrap(),
            key_part(KeyClass::Real, &Datum::Real(0.0)).unwrap()
        );
        // An integral datum in a float key column promotes, as the
        // comparison itself does.
        assert_eq!(
            key_part(KeyClass::Real, &Datum::Int(3)).unwrap(),
            key_part(KeyClass::Real, &Datum::Real(3.0)).unwrap()
        );
    }

    /// The property the whole module rests on for decimals: equal values
    /// hash equal, regardless of the scale they were written with.
    #[test]
    fn decimal_key_agrees_with_decimal_equality() {
        let values = [
            "1",
            "1.0",
            "1.00",
            "0",
            "-0",
            "0.000",
            "-1.5",
            "-1.500",
            "12345678901234567890",
            "0.1",
            "0.10",
        ];
        for a in values {
            for b in values {
                let da = Decimal::from_signed_literal(a);
                let db = Decimal::from_signed_literal(b);
                let ka = key_part(KeyClass::Decimal, &Datum::Decimal(da.clone())).unwrap();
                let kb = key_part(KeyClass::Decimal, &Datum::Decimal(db.clone())).unwrap();
                assert_eq!(
                    da == db,
                    ka == kb,
                    "{a} vs {b}: equality and hash key disagree"
                );
            }
        }
        // An integral datum in a decimal key column promotes to scale 0.
        assert_eq!(
            key_part(KeyClass::Decimal, &Datum::Int(1)).unwrap(),
            key_part(
                KeyClass::Decimal,
                &Datum::Decimal(Decimal::from_signed_literal("1.000"))
            )
            .unwrap()
        );
    }

    /// A multi-column key must not let one column borrow the next one's
    /// bytes.
    #[test]
    fn multi_column_keys_are_unambiguous() {
        let keys = [
            EquiKey {
                left: 0,
                right: 0,
                class: KeyClass::Str(Collation::Utf8Mb4Bin),
            },
            EquiKey {
                left: 1,
                right: 1,
                class: KeyClass::Str(Collation::Utf8Mb4Bin),
            },
        ];
        let offset = |key: &EquiKey| key.left;
        let ab_c = row_key(
            &keys,
            &[Datum::new_string("ab"), Datum::new_string("c")],
            offset,
        )
        .unwrap();
        let a_bc = row_key(
            &keys,
            &[Datum::new_string("a"), Datum::new_string("bc")],
            offset,
        )
        .unwrap();
        assert_ne!(ab_c, a_bc);
    }
}
