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

//! Adjacent-key grouping for sorted executor batches.
//!
//! This is the observable contract of Go's
//! `pkg/executor/internal/vecgroupchecker`: evaluate each grouping expression,
//! split a non-empty chunk into equal adjacent runs, remember whether the next
//! chunk starts with the previous chunk's last key, and expose those runs in
//! order. Rust evaluates rows directly; Go's vectorized temporary-column pool
//! is an implementation detail rather than part of that contract.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Collation, Datum, StringDatum};
use tidb_expr::{collation_derive::collation_of_node, expression::Expression};
use tidb_expr::{Columns, EvalError};

/// Splits sorted chunks into adjacent equal-key groups.
#[derive(Clone, Debug)]
pub struct VecGroupChecker {
    group_by_items: Vec<Expression>,
    collations: Vec<Collation>,
    previous_last_key: Option<Vec<u8>>,
    group_offsets: Vec<usize>,
    next_group_id: usize,
}

impl VecGroupChecker {
    /// Creates a checker for `group_by_items`.
    #[must_use]
    pub fn new(group_by_items: Vec<Expression>) -> Self {
        let collations = group_by_items.iter().map(collation_of_node).collect();
        Self {
            group_by_items,
            collations,
            previous_last_key: None,
            group_offsets: Vec::new(),
            next_group_id: 0,
        }
    }

    /// Evaluates the grouping expressions and splits `chunk` into runs.
    ///
    /// The caller, like the upstream stream/window/merge executors, must not
    /// pass an empty chunk.
    pub fn split_into_groups(
        &mut self,
        ctx: &impl Columns,
        chunk: &Chunk,
    ) -> Result<bool, EvalError> {
        let rows = chunk.num_rows();
        assert!(rows != 0, "VecGroupChecker requires a non-empty chunk");
        self.reset();

        // No grouping expressions means the entire input is the single global
        // group, continuing every preceding chunk including the first one.
        if self.group_by_items.is_empty() {
            self.group_offsets.push(rows);
            return Ok(true);
        }

        let mut keys = Vec::with_capacity(rows);
        for row_index in 0..rows {
            let row = chunk.get_row(row_index);
            let mut key = Vec::with_capacity(self.group_by_items.len());
            for item in &self.group_by_items {
                key.push(item.eval(ctx, row)?);
            }
            keys.push(key);
        }
        self.split_evaluated(&keys, &self.collations.clone())
    }

    /// Splits already-evaluated adjacent keys. Window execution uses this
    /// after its ORDER BY expressions have been evaluated once for sorting.
    pub(crate) fn split_evaluated(
        &mut self,
        keys: &[Vec<Datum>],
        collations: &[Collation],
    ) -> Result<bool, EvalError> {
        self.split_key_iter(keys.iter().map(Vec::as_slice), collations)
    }

    /// Splits the selected rows without copying their already-owned keys.
    pub(crate) fn split_indexed(
        &mut self,
        indices: &[usize],
        keys: &[Vec<Datum>],
        collations: &[Collation],
    ) -> Result<bool, EvalError> {
        self.split_key_iter(
            indices.iter().map(|index| keys[*index].as_slice()),
            collations,
        )
    }

    fn split_key_iter<'a, I>(
        &mut self,
        keys: I,
        collations: &[Collation],
    ) -> Result<bool, EvalError>
    where
        I: Clone + DoubleEndedIterator<Item = &'a [Datum]> + ExactSizeIterator,
    {
        assert!(
            keys.len() != 0,
            "VecGroupChecker requires at least one evaluated key"
        );
        if keys.clone().any(|key| key.len() != collations.len()) {
            return Err(EvalError::Unsupported(
                "group key width does not match its collations",
            ));
        }

        self.reset();
        let first = keys
            .clone()
            .next()
            .expect("the non-empty iterator has a first key");
        let last = keys
            .clone()
            .next_back()
            .expect("the non-empty iterator has a last key");
        let first_encoded = encode_boundary_key(first, collations)?;
        let last_encoded = encode_boundary_key(last, collations)?;
        let continues_previous = self.previous_last_key.as_ref() == Some(&first_encoded);
        self.previous_last_key = Some(last_encoded.clone());

        // Upstream has this fast path and its callers guarantee sorted input.
        // Besides avoiding work, preserving it keeps that precondition exact.
        if first_encoded == last_encoded {
            self.group_offsets.push(keys.len());
            return Ok(continues_previous);
        }

        let mut adjacent = keys.clone();
        let mut previous = adjacent
            .next()
            .expect("the non-empty iterator has a first key");
        for (index, current) in adjacent.enumerate() {
            if !keys_equal(previous, current, collations)? {
                let position = index + 1;
                self.group_offsets.push(position);
            }
            previous = current;
        }
        self.group_offsets.push(keys.len());
        Ok(continues_previous)
    }

    /// Returns the next half-open group range.
    ///
    /// As in the upstream internal API, callers must check [`Self::is_exhausted`]
    /// before calling this method.
    pub fn get_next_group(&mut self) -> (usize, usize) {
        let begin = if self.next_group_id == 0 {
            0
        } else {
            self.group_offsets[self.next_group_id - 1]
        };
        let end = self.group_offsets[self.next_group_id];
        self.next_group_id += 1;
        (begin, end)
    }

    /// Whether every group in the current chunk has been consumed.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.next_group_id >= self.group_offsets.len()
    }

    /// Clears current-chunk state while retaining the previous chunk's key.
    pub fn reset(&mut self) {
        self.group_offsets.clear();
        self.next_group_id = 0;
    }

    /// Number of groups in the current chunk.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.group_offsets.len()
    }
}

fn encode_boundary_key(values: &[Datum], collations: &[Collation]) -> Result<Vec<u8>, EvalError> {
    let values = values
        .iter()
        .zip(collations)
        .map(|(value, collation)| match value {
            Datum::String(_) | Datum::Bytes(_) | Datum::Enum(_, _) | Datum::Set(_, _) => {
                Datum::String(StringDatum::new(value.go_bytes(), *collation))
            }
            value => value.clone(),
        })
        .collect::<Vec<_>>();
    tidb_codec::Encoder::new(true)
        .encode_key(&values)
        .map_err(|_| EvalError::Unsupported("group boundary key cannot be encoded"))
}

fn keys_equal(
    left: &[Datum],
    right: &[Datum],
    collations: &[Collation],
) -> Result<bool, EvalError> {
    debug_assert_eq!(left.len(), right.len());
    debug_assert_eq!(left.len(), collations.len());
    for ((left, right), collation) in left.iter().zip(right).zip(collations) {
        if !tidb_expr::compare_datums_with_collation(left, right, *collation)?.is_eq() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{BinaryJSON, Decimal, FieldType, FieldTypeCode, MysqlEnum};
    use tidb_expr::{column::Column, NoColumns};

    fn column(index: usize, field_type: FieldType) -> Expression {
        let mut column = Column::new(index as i64 + 1, field_type);
        column.index = index as i64;
        Expression::Column(column)
    }

    fn ranges(checker: &mut VecGroupChecker) -> Vec<(usize, usize)> {
        let mut ranges = Vec::new();
        while !checker.is_exhausted() {
            ranges.push(checker.get_next_group());
        }
        ranges
    }

    #[test]
    fn groups_continue_across_chunk_boundaries() {
        let field = FieldType::new(FieldTypeCode::LongLong);
        let mut checker = VecGroupChecker::new(vec![column(0, field.clone())]);
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 4);
        for value in [1, 1, 2, 2] {
            chunk.append_int64(0, value);
        }
        assert!(!checker.split_into_groups(&NoColumns, &chunk).unwrap());
        assert_eq!(checker.group_count(), 2);
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 4)]);

        let mut next = Chunk::new_with_capacity(&[field], 3);
        for value in [2, 3, 3] {
            next.append_int64(0, value);
        }
        assert!(checker.split_into_groups(&NoColumns, &next).unwrap());
        assert_eq!(ranges(&mut checker), [(0, 1), (1, 3)]);
    }

    #[test]
    fn source_group_count_matrix() {
        let cases = [
            (&[1024, 1][..], 1, 1025, &[false, false][..]),
            (&[1024, 1][..], 1025, 1, &[false, true][..]),
            (&[1, 1][..], 2, 1, &[false, true][..]),
            (&[1, 1][..], 1, 2, &[false, false][..]),
            (&[2, 2][..], 2, 2, &[false, false][..]),
            (&[2, 2][..], 4, 1, &[false, true][..]),
        ];
        let field = FieldType::new(FieldTypeCode::LongLong);

        for (chunk_rows, same_rows, expected_groups, expected_flags) in cases {
            let mut checker = VecGroupChecker::new(vec![column(0, field.clone())]);
            let mut global_row = 0usize;
            let mut groups = 0usize;
            for (chunk_index, rows) in chunk_rows.iter().copied().enumerate() {
                let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), rows);
                for _ in 0..rows {
                    chunk.append_int64(0, (global_row / same_rows) as i64);
                    global_row += 1;
                }
                let continues = checker.split_into_groups(&NoColumns, &chunk).unwrap();
                assert_eq!(continues, expected_flags[chunk_index]);
                groups += checker.group_count() - usize::from(continues);
            }
            assert_eq!(groups, expected_groups);
        }
    }

    #[test]
    fn collation_and_padding_define_string_groups() {
        let keys = ["aaa", "AAA", "😜", "😃", "À", "A"]
            .into_iter()
            .map(|value| vec![Datum::new_string(value)])
            .collect::<Vec<_>>();
        let mut checker = VecGroupChecker::new(Vec::new());

        checker
            .split_evaluated(&keys, &[Collation::Binary])
            .unwrap();
        assert_eq!(ranges(&mut checker).len(), 6);

        checker = VecGroupChecker::new(Vec::new());
        checker
            .split_evaluated(&keys, &[Collation::Utf8Mb4GeneralCi])
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 4), (4, 6)]);

        checker = VecGroupChecker::new(Vec::new());
        checker
            .split_evaluated(&keys, &[Collation::Utf8Mb4UnicodeCi])
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 4), (4, 6)]);

        let padded = ["a", "a  ", "a    "]
            .into_iter()
            .map(|value| vec![Datum::new_string(value)])
            .collect::<Vec<_>>();
        checker = VecGroupChecker::new(Vec::new());
        checker
            .split_evaluated(&padded, &[Collation::Utf8Mb4Bin])
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 3)]);
    }

    #[test]
    fn previous_key_owns_variable_length_values() {
        let field = FieldType::new(FieldTypeCode::VarString);
        let mut checker = VecGroupChecker::new(vec![column(0, field.clone())]);
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 1);
        chunk.append_string(0, "abc");
        checker.split_into_groups(&NoColumns, &chunk).unwrap();

        chunk.reset();
        chunk.append_string(0, "replacement that grows the source buffer");

        let mut next = Chunk::new_with_capacity(&[field], 1);
        next.append_string(0, "abc");
        assert!(checker.split_into_groups(&NoColumns, &next).unwrap());
    }

    #[test]
    fn previous_key_deep_clones_decimal_and_json_values() {
        let originals = [
            Datum::Decimal(Decimal::from_int(123)),
            Datum::Json(BinaryJSON::parse(r#"{"123":123}"#).unwrap()),
        ];
        let replacements = [
            Datum::Decimal(Decimal::from_int(456)),
            Datum::Json(BinaryJSON::parse(r#"{"456":456}"#).unwrap()),
        ];

        for (original, replacement) in originals.into_iter().zip(replacements) {
            let mut checker = VecGroupChecker::new(Vec::new());
            let mut first = vec![vec![original.clone()]];
            checker
                .split_evaluated(&first, &[Collation::Binary])
                .unwrap();
            first[0][0] = replacement;
            assert!(checker
                .split_evaluated(&[vec![original]], &[Collation::Binary])
                .unwrap());
        }
    }

    #[test]
    fn nulls_are_equal_only_to_adjacent_nulls() {
        let keys = vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Null],
        ];
        let mut checker = VecGroupChecker::new(Vec::new());
        checker
            .split_evaluated(&keys, &[Collation::Binary, Collation::Binary])
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 3), (3, 4)]);
    }

    #[test]
    fn enum_grouping_uses_the_member_name_in_the_string_domain() {
        let keys = vec![
            vec![Datum::new_enum(
                MysqlEnum::new("same", 1),
                Collation::Utf8Mb4GeneralCi,
            )],
            vec![Datum::new_enum(
                MysqlEnum::new("same", 2),
                Collation::Utf8Mb4GeneralCi,
            )],
            vec![Datum::new_enum(
                MysqlEnum::new("other", 3),
                Collation::Utf8Mb4GeneralCi,
            )],
        ];
        let mut checker = VecGroupChecker::new(Vec::new());
        checker
            .split_evaluated(&keys, &[Collation::Utf8Mb4GeneralCi])
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 3)]);
    }

    #[test]
    fn cross_chunk_float_identity_uses_the_source_encoded_key() {
        let field = FieldType::new(FieldTypeCode::Double);
        let chunk = |values: &[f64]| {
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), values.len());
            for value in values {
                chunk.append_float64(0, *value);
            }
            chunk
        };
        let mut checker = VecGroupChecker::new(vec![column(0, field.clone())]);
        checker
            .split_into_groups(&NoColumns, &chunk(&[-0.0]))
            .unwrap();
        assert!(checker
            .split_into_groups(&NoColumns, &chunk(&[0.0]))
            .unwrap());

        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        checker = VecGroupChecker::new(vec![column(0, field.clone())]);
        checker
            .split_into_groups(&NoColumns, &chunk(&[nan]))
            .unwrap();
        assert!(checker
            .split_into_groups(&NoColumns, &chunk(&[nan]))
            .unwrap());
        assert!(!checker
            .split_into_groups(&NoColumns, &chunk(&[f64::from_bits(0x7ff8_0000_0000_0002)]),)
            .unwrap());

        checker = VecGroupChecker::new(vec![column(0, field.clone())]);
        checker
            .split_into_groups(&NoColumns, &chunk(&[-0.0, 0.0, nan, nan]))
            .unwrap();
        assert_eq!(ranges(&mut checker), [(0, 2), (2, 3), (3, 4)]);
    }

    #[test]
    fn empty_items_and_reset_match_the_internal_contract() {
        let field = FieldType::new(FieldTypeCode::LongLong);
        let mut chunk = Chunk::new_with_capacity(&[field], 2);
        chunk.append_int64(0, 1);
        chunk.append_int64(0, 2);
        let mut checker = VecGroupChecker::new(Vec::new());
        assert!(checker.split_into_groups(&NoColumns, &chunk).unwrap());
        assert_eq!(checker.get_next_group(), (0, 2));
        assert!(checker.is_exhausted());

        checker
            .split_evaluated(
                &[vec![Datum::Int(1)], vec![Datum::Int(2)]],
                &[Collation::Binary],
            )
            .unwrap();
        assert_eq!(checker.get_next_group(), (0, 1));
        assert!(!checker.is_exhausted());
        checker.reset();
        assert!(checker.is_exhausted());
        assert_eq!(checker.group_count(), 0);
    }
}
