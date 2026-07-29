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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The SORT path: the executor's one total order over datums, and the
//! configured/prepared `ORDER BY` key contracts validated and applied on top of
//! it. Free functions only.
//!
//! The live consumer is the bounded single-table node's
//! `tidb-server::sorting_result_set`; `tidb-executor`'s `sort` operator owns
//! the chunk-based path. The `ORDER BY`/`LIMIT` RESOLUTION that used to live
//! here (positional items, alias resolution, `LIMIT` truncation) belonged to
//! the retired `Database` engine.

use std::{cmp::Ordering, error::Error, fmt};

use tidb_datatype::{Collation, Datum, DatumKind};
use tidb_planner::configured_order_limit_contract::ConfiguredOrderKey;
use tidb_planner::read_only_scan::{ConfiguredScalarType, PreparedOrderColumn};

use crate::Row;

/// Compares ordered key pairs through the executor's one total-order
/// authority. Callers that restrict their datum domain must validate it before
/// invoking this function; comparison itself remains shared with every other
/// `ORDER BY` path.
fn cmp_key_pairs<'a>(pairs: impl IntoIterator<Item = (&'a Datum, &'a Datum, bool)>) -> Ordering {
    for (av, bv, desc) in pairs {
        let ord = sort_value_cmp(av, bv);
        if ord != Ordering::Equal {
            return if desc { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

/// A checked configured-order execution failure.
///
/// The planner contract represents only signed-BIGINT order keys. Keeping
/// malformed physical rows distinct from an ordinary tie prevents a widened
/// executor from inventing NULL/zero/coercion semantics before it owns them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderError {
    /// A planner-resolved key does not fit the promised physical FullSchema.
    FullSchemaOffset {
        /// The invalid planner-resolved physical key offset.
        offset: usize,
        /// The promised physical FullSchema width.
        width: usize,
    },
    /// A materialized row is not the width promised by the planner.
    RowWidth {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-promised physical FullSchema width.
        expected: usize,
        /// Actual number of decoded datum slots.
        actual: usize,
    },
    /// A configured signed-BIGINT key decoded as another datum kind.
    KeyDatum {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-resolved physical key offset.
        offset: usize,
        /// Actual datum kind at that physical key offset.
        kind: DatumKind,
    },
}

impl fmt::Display for ConfiguredOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FullSchemaOffset { offset, width } => {
                write!(
                    formatter,
                    "configured ORDER BY offset {offset} exceeds FullSchema width {width}"
                )
            }
            Self::RowWidth {
                row_index,
                expected,
                actual,
            } => write!(
                formatter,
                "configured ORDER BY row {row_index} has width {actual}, expected {expected}"
            ),
            Self::KeyDatum {
                row_index,
                offset,
                kind,
            } => write!(
                formatter,
                "configured ORDER BY row {row_index} key at offset {offset} decoded as {kind:?}"
            ),
        }
    }
}

impl Error for ConfiguredOrderError {}

/// Stably orders materialized configured rows by planner-resolved FullSchema
/// keys.
///
/// This is the first executable consumer of
/// `ConfiguredOrderKey`: every key is a checked physical offset into a row of
/// exactly `full_schema_width` signed-BIGINT datums. The full input is
/// validated before mutation, then Rust's stable slice sort keeps source order
/// for rows whose complete key tuple ties. NULLs, unsigned/mixed types,
/// collations, spilling, and parallel merge execution intentionally belong to
/// later owners rather than being guessed here.
pub fn stable_order_configured_rows(
    rows: &mut [Row],
    full_schema_width: usize,
    keys: &[ConfiguredOrderKey],
) -> Result<(), ConfiguredOrderError> {
    validate_configured_order_rows(rows, full_schema_width, keys)?;

    rows.sort_by(|left, right| compare_configured_rows(left, right, keys));
    Ok(())
}

/// Validates rows before a configured ordering consumer indexes their physical
/// FullSchema offsets.
///
/// A bounded TopN validates each row before it enters its heap;
/// [`compare_configured_rows`] can then stay allocation-free and infallible in
/// the heap's hot comparison path. The contract intentionally accepts only
/// signed-BIGINT key datums until a wider planner/executor contract owns the
/// missing coercion and collation semantics.
pub fn validate_configured_order_rows(
    rows: &[Row],
    full_schema_width: usize,
    keys: &[ConfiguredOrderKey],
) -> Result<(), ConfiguredOrderError> {
    for key in keys {
        if key.full_offset() >= full_schema_width {
            return Err(ConfiguredOrderError::FullSchemaOffset {
                offset: key.full_offset(),
                width: full_schema_width,
            });
        }
    }

    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != full_schema_width {
            return Err(ConfiguredOrderError::RowWidth {
                row_index,
                expected: full_schema_width,
                actual: row.len(),
            });
        }
        for key in keys {
            let value = &row[key.full_offset()];
            if !matches!(value, Datum::Int(_)) {
                return Err(ConfiguredOrderError::KeyDatum {
                    row_index,
                    offset: key.full_offset(),
                    kind: value.kind(),
                });
            }
        }
    }
    Ok(())
}

/// Compares two already validated configured rows by their planner-resolved
/// physical keys.
///
/// Callers must first run [`validate_configured_order_rows`] over every row
/// they may pass here with the same `full_schema_width` and `keys`. This keeps
/// the comparator suitable for a `BinaryHeap` without silently inventing a
/// fallback ordering for malformed physical data.
pub fn compare_configured_rows(left: &Row, right: &Row, keys: &[ConfiguredOrderKey]) -> Ordering {
    cmp_key_pairs(keys.iter().map(|key| {
        (
            &left[key.full_offset()],
            &right[key.full_offset()],
            key.direction().is_descending(),
        )
    }))
}

/// A checked prepared-read ordering failure.
///
/// Unlike [`ConfiguredOrderError`], whose keys are always signed BIGINT, a
/// prepared read's keys carry the projected column's scalar type. Keeping a
/// mistyped physical row distinct from an ordinary tie prevents the executor
/// from inventing a fallback order for data the column's type does not admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedOrderError {
    /// A resolved key offset does not fit the projected output row width.
    OutputOffset {
        /// The invalid planner-resolved output offset.
        offset: usize,
        /// The projected output row width.
        width: usize,
    },
    /// A materialized row is not the projected output width.
    RowWidth {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-promised projected output width.
        expected: usize,
        /// Actual number of decoded datum slots.
        actual: usize,
    },
    /// A key datum's kind does not match the projected column's scalar type.
    KeyDatum {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-resolved output key offset.
        offset: usize,
        /// Actual datum kind decoded at that offset.
        kind: DatumKind,
    },
}

impl fmt::Display for PreparedOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputOffset { offset, width } => write!(
                formatter,
                "prepared ORDER BY offset {offset} exceeds output width {width}"
            ),
            Self::RowWidth {
                row_index,
                expected,
                actual,
            } => write!(
                formatter,
                "prepared ORDER BY row {row_index} has width {actual}, expected {expected}"
            ),
            Self::KeyDatum {
                row_index,
                offset,
                kind,
            } => write!(
                formatter,
                "prepared ORDER BY row {row_index} key at offset {offset} decoded as {kind:?}"
            ),
        }
    }
}

impl Error for PreparedOrderError {}

/// The datum kind a projected column's scalar type stores in an output row.
///
/// A signed integer column decodes to [`Datum::Int`]; an unsigned `BIGINT`
/// decodes to [`Datum::UInt`]; a `DOUBLE` decodes to [`Datum::Real`]; a `CHAR`
/// column decodes to [`Datum::Bytes`] (its `utf8mb4` bytes); `DATE`/`DATETIME`/
/// `TIMESTAMP` decode to [`Datum::Time`]; `TIME` decodes to
/// [`Datum::Duration`]. A nullable column additionally decodes to
/// [`Datum::Null`]; a `NOT NULL` one never may, so a `NULL` there stays a
/// decode contract violation. Any other pairing is a decode contract violation
/// the ordering must not silently reorder.
const fn scalar_type_admits(
    scalar_type: ConfiguredScalarType,
    nullable: bool,
    datum: &Datum,
) -> bool {
    if matches!(datum, Datum::Null) {
        return nullable;
    }
    match scalar_type {
        ConfiguredScalarType::BigInt | ConfiguredScalarType::Int => matches!(datum, Datum::Int(_)),
        ConfiguredScalarType::UnsignedBigInt => matches!(datum, Datum::UInt(_)),
        ConfiguredScalarType::Double => matches!(datum, Datum::Real(_)),
        ConfiguredScalarType::Char { .. } | ConfiguredScalarType::Varchar { .. } => {
            matches!(datum, Datum::Bytes(_))
        }
        ConfiguredScalarType::Decimal { .. } => matches!(datum, Datum::Decimal(_)),
        ConfiguredScalarType::Date
        | ConfiguredScalarType::Datetime { .. }
        | ConfiguredScalarType::Timestamp { .. } => matches!(datum, Datum::Time(_)),
        ConfiguredScalarType::Duration { .. } => matches!(datum, Datum::Duration(_)),
    }
}

/// Validates materialized prepared-read rows before ordering indexes them.
///
/// Mirrors [`validate_configured_order_rows`] but admits each projected
/// column's own scalar type instead of a single signed-BIGINT domain, so the
/// comparator can stay allocation-free and infallible in the sort's hot path.
pub fn validate_prepared_order_rows(
    rows: &[Row],
    output_width: usize,
    keys: &[PreparedOrderColumn],
) -> Result<(), PreparedOrderError> {
    for key in keys {
        if key.output_offset() >= output_width {
            return Err(PreparedOrderError::OutputOffset {
                offset: key.output_offset(),
                width: output_width,
            });
        }
    }

    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != output_width {
            return Err(PreparedOrderError::RowWidth {
                row_index,
                expected: output_width,
                actual: row.len(),
            });
        }
        for key in keys {
            let datum = &row[key.output_offset()];
            if !scalar_type_admits(key.scalar_type(), key.is_nullable(), datum) {
                return Err(PreparedOrderError::KeyDatum {
                    row_index,
                    offset: key.output_offset(),
                    kind: datum.kind(),
                });
            }
        }
    }
    Ok(())
}

/// Compares two already validated prepared-read rows by their resolved keys.
///
/// String columns compare under their `utf8mb4_bin` collation through the
/// crate-shared [`Collation`] authority, which trims trailing spaces exactly as
/// TiDB's Go collator does. Every other pairing — including `NULL`, which sorts
/// before all non-`NULL` values ascending — defers to [`sort_value_cmp`], the
/// same total order the in-process sort executor applies, so the two sorts
/// cannot disagree. Callers must first run [`validate_prepared_order_rows`].
///
/// `utf8mb4_bin` is hardcoded here rather than carried per key because
/// `ConfiguredScalarType` can name no other string collation, and the catalog
/// loader (`configured_string_is_binary`) refuses at load any stored column
/// whose collation differs. Widening that gate without first giving this
/// comparator the column's real collation would silently order a
/// `utf8mb4_general_ci` column by raw bytes.
fn compare_prepared_rows(left: &Row, right: &Row, keys: &[PreparedOrderColumn]) -> Ordering {
    for key in keys {
        let offset = key.output_offset();
        let ordering = match (&left[offset], &right[offset]) {
            (Datum::Bytes(a), Datum::Bytes(b)) => Collation::Utf8Mb4Bin.compare(a, b),
            (a, b) => sort_value_cmp(a, b),
        };
        if ordering != Ordering::Equal {
            return if key.direction().is_descending() {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

/// Stably orders materialized prepared-read rows by planner-resolved keys.
///
/// The prepared point/range read has no `LIMIT`, so its `ORDER BY` is a
/// SQL-layer sort over the fully projected output rows rather than a bounded
/// coprocessor TopN. The full input is validated before mutation, then Rust's
/// stable sort keeps source order for rows whose complete key tuple ties.
pub fn stable_order_prepared_rows(
    rows: &mut [Row],
    output_width: usize,
    keys: &[PreparedOrderColumn],
) -> Result<(), PreparedOrderError> {
    validate_prepared_order_rows(rows, output_width, keys)?;
    rows.sort_by(|left, right| compare_prepared_rows(left, right, keys));
    Ok(())
}

/// A total order for `ORDER BY`: `NULL`s sort first (MySQL ascending default),
/// integers/floats numerically, strings by byte order (`utf8mb4_bin`). Signed
/// and unsigned integers retain MySQL's mixed-domain ordering: every negative
/// signed value precedes UInt, while nonnegative signed values compare by
/// magnitude. This matters for real `INT UNSIGNED`/`BIGINT UNSIGNED` storage,
/// not merely unsigned literal expressions.
fn sort_value_cmp(a: &Datum, b: &Datum) -> Ordering {
    if let Some(ordering) = a.compare_sentinel_order(b) {
        return ordering;
    }
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Int(x), Datum::Int(y)) => x.cmp(y),
        (Datum::UInt(x), Datum::UInt(y)) => x.cmp(y),
        (Datum::Int(x), Datum::UInt(_)) if *x < 0 => Ordering::Less,
        (Datum::Int(x), Datum::UInt(y)) => (*x as u64).cmp(y),
        (Datum::UInt(_), Datum::Int(y)) if *y < 0 => Ordering::Greater,
        (Datum::UInt(x), Datum::Int(y)) => x.cmp(&(*y as u64)),
        (Datum::String(x), Datum::String(y)) => x.bytes().cmp(y.bytes()),
        (Datum::Bytes(x), Datum::Bytes(y)) => x.cmp(y),
        (Datum::Decimal(x), Datum::Decimal(y)) => x.cmp(y),
        (Datum::Time(x), Datum::Time(y)) => x.compare(*y),
        (Datum::Duration(x), Datum::Duration(y)) => x.compare(*y),
        // `Datum::Real` is always finite, so `partial_cmp` always
        // succeeds here — falls back to `Equal` only in the truly
        // impossible NaN/infinite case, same as the mixed-type fallback.
        (Datum::Real(x), Datum::Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use tidb_planner::configured_order_limit_contract::{
        ConfiguredOrderDirection, ConfiguredOrderKey,
    };

    use super::{
        stable_order_configured_rows, stable_order_prepared_rows, ConfiguredOrderError,
        ConfiguredScalarType, Datum, DatumKind, PreparedOrderColumn, PreparedOrderError, Row,
    };

    #[test]
    fn configured_order_uses_fullschema_offsets_directions_and_stable_ties() {
        let keys = [
            ConfiguredOrderKey::new(2, ConfiguredOrderDirection::Ascending),
            ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Descending),
        ];
        let mut rows: Vec<Row> = vec![
            vec![Datum::Int(100), Datum::Int(9), Datum::Int(2)],
            vec![Datum::Int(200), Datum::Int(8), Datum::Int(2)],
            vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
            vec![Datum::Int(400), Datum::Int(10), Datum::Int(1)],
        ];

        stable_order_configured_rows(&mut rows, 3, &keys).expect("configured signed BIGINT rows");

        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(400), Datum::Int(10), Datum::Int(1)],
                vec![Datum::Int(100), Datum::Int(9), Datum::Int(2)],
                vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
                vec![Datum::Int(200), Datum::Int(8), Datum::Int(2)],
            ],
            "equal complete keys retain source order"
        );
    }

    #[test]
    fn configured_order_rejects_invalid_fullschema_rows_before_sorting() {
        let key = ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Ascending);
        let mut wrong_width = vec![vec![Datum::Int(2), Datum::Int(1)], vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_configured_rows(&mut wrong_width, 2, &[key]),
            Err(ConfiguredOrderError::RowWidth {
                row_index: 1,
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            wrong_width[0][0],
            Datum::Int(2),
            "validation precedes mutation"
        );

        let mut wrong_kind = vec![vec![Datum::Int(1), Datum::UInt(2)]];
        assert_eq!(
            stable_order_configured_rows(&mut wrong_kind, 2, &[key]),
            Err(ConfiguredOrderError::KeyDatum {
                row_index: 0,
                offset: 1,
                kind: DatumKind::UInt,
            })
        );

        let mut rows = vec![vec![Datum::Int(1), Datum::Int(2)]];
        let outside = ConfiguredOrderKey::new(2, ConfiguredOrderDirection::Descending);
        assert_eq!(
            stable_order_configured_rows(&mut rows, 2, &[outside]),
            Err(ConfiguredOrderError::FullSchemaOffset {
                offset: 2,
                width: 2,
            })
        );
    }

    fn bytes_row(value: &str) -> Row {
        vec![Datum::new_bytes(value.as_bytes().to_vec())]
    }

    #[test]
    fn prepared_order_sorts_utf8mb4_bin_char_column_by_bytes() {
        // sysbench read 4: `SELECT c ... ORDER BY c`, one projected CHAR column.
        let key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 120 },
            false,
        );
        let mut rows = vec![bytes_row("banana"), bytes_row("apple"), bytes_row("cherry")];
        stable_order_prepared_rows(&mut rows, 1, &[key]).expect("utf8mb4_bin char rows");
        assert_eq!(
            rows,
            vec![bytes_row("apple"), bytes_row("banana"), bytes_row("cherry")]
        );
    }

    #[test]
    fn prepared_order_char_descending_and_trailing_space_ties_stably() {
        let descending = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Descending,
            ConfiguredScalarType::Char { max_length: 8 },
            false,
        );
        let mut rows = vec![bytes_row("a"), bytes_row("c"), bytes_row("b")];
        stable_order_prepared_rows(&mut rows, 1, &[descending]).expect("descending char rows");
        assert_eq!(rows, vec![bytes_row("c"), bytes_row("b"), bytes_row("a")]);

        // utf8mb4_bin is PAD SPACE: "a " and "a" tie, so the stable sort keeps
        // the source order of the tied rows (the shared Collation authority
        // trims the trailing space exactly as TiDB's Go collator does).
        let ascending = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 8 },
            false,
        );
        let mut padded = vec![bytes_row("a "), bytes_row("a"), bytes_row("a  ")];
        stable_order_prepared_rows(&mut padded, 1, &[ascending]).expect("padded char rows");
        assert_eq!(
            padded,
            vec![bytes_row("a "), bytes_row("a"), bytes_row("a  ")],
            "PAD SPACE ties retain source order"
        );
    }

    #[test]
    fn prepared_order_signed_int_column_compares_numerically() {
        let key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        let mut rows = vec![
            vec![Datum::Int(30)],
            vec![Datum::Int(-5)],
            vec![Datum::Int(2)],
        ];
        stable_order_prepared_rows(&mut rows, 1, &[key]).expect("signed int rows");
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(-5)],
                vec![Datum::Int(2)],
                vec![Datum::Int(30)]
            ]
        );
    }

    #[test]
    fn prepared_order_rejects_mistyped_and_out_of_range_rows_before_sorting() {
        // A CHAR key over an integer datum is a decode contract violation.
        let char_key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 4 },
            false,
        );
        let mut mistyped = vec![bytes_row("b"), vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_prepared_rows(&mut mistyped, 1, &[char_key]),
            Err(PreparedOrderError::KeyDatum {
                row_index: 1,
                offset: 0,
                kind: DatumKind::Int,
            })
        );
        assert_eq!(mistyped[0], bytes_row("b"), "validation precedes mutation");

        let int_key = PreparedOrderColumn::new(
            1,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        let mut rows = vec![vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_prepared_rows(&mut rows, 1, &[int_key]),
            Err(PreparedOrderError::OutputOffset {
                offset: 1,
                width: 1
            })
        );

        let mut narrow = vec![vec![Datum::Int(1), Datum::Int(2)], vec![Datum::Int(3)]];
        let offset_zero = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        assert_eq!(
            stable_order_prepared_rows(&mut narrow, 2, &[offset_zero]),
            Err(PreparedOrderError::RowWidth {
                row_index: 1,
                expected: 2,
                actual: 1,
            })
        );
    }

    /// MySQL's `ORDER BY` treats `NULL` as smaller than every non-`NULL`
    /// value, so it sorts first ascending and last descending. This is the
    /// same rule the in-process sort executor applies through
    /// `sort_value_cmp`, verified here on the prepared-read comparator so the
    /// two sorts cannot disagree.
    #[test]
    fn prepared_order_sorts_nulls_first_ascending_and_last_descending() {
        let rows = || -> Vec<Row> {
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Null],
                vec![Datum::Int(-1)],
                vec![Datum::Null],
            ]
        };

        let mut ascending = rows();
        stable_order_prepared_rows(
            &mut ascending,
            1,
            &[PreparedOrderColumn::new(
                0,
                ConfiguredOrderDirection::Ascending,
                ConfiguredScalarType::BigInt,
                true,
            )],
        )
        .expect("a nullable key admits NULL");
        assert_eq!(
            ascending,
            vec![
                vec![Datum::Null],
                vec![Datum::Null],
                vec![Datum::Int(-1)],
                vec![Datum::Int(2)],
            ]
        );

        let mut descending = rows();
        stable_order_prepared_rows(
            &mut descending,
            1,
            &[PreparedOrderColumn::new(
                0,
                ConfiguredOrderDirection::Descending,
                ConfiguredScalarType::BigInt,
                true,
            )],
        )
        .expect("a nullable key admits NULL");
        assert_eq!(
            descending,
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Int(-1)],
                vec![Datum::Null],
                vec![Datum::Null],
            ]
        );
    }

    /// A `NOT NULL` column that decoded to `NULL` is a decode contract
    /// violation, not an orderable value; the sort must refuse it rather than
    /// place it anywhere.
    #[test]
    fn a_null_in_a_not_null_order_key_still_fails_closed() {
        let mut rows: Vec<Row> = vec![vec![Datum::Int(1)], vec![Datum::Null]];
        assert_eq!(
            stable_order_prepared_rows(
                &mut rows,
                1,
                &[PreparedOrderColumn::new(
                    0,
                    ConfiguredOrderDirection::Ascending,
                    ConfiguredScalarType::BigInt,
                    false,
                )],
            ),
            Err(PreparedOrderError::KeyDatum {
                row_index: 1,
                offset: 0,
                kind: DatumKind::Null,
            })
        );
    }

    /// Every widened scalar type orders by its own value domain. Before the
    /// comparator delegated to `sort_value_cmp`, an unsigned, floating-point,
    /// or decimal key compared as `Equal` and the sort silently kept source
    /// order.
    #[test]
    fn prepared_order_compares_every_widened_scalar_domain() {
        let cases: [(ConfiguredScalarType, [Datum; 3]); 3] = [
            (
                ConfiguredScalarType::UnsignedBigInt,
                [Datum::UInt(u64::MAX), Datum::UInt(0), Datum::UInt(1 << 63)],
            ),
            (
                ConfiguredScalarType::Double,
                [
                    Datum::new_real(2.5),
                    Datum::new_real(-1.0),
                    Datum::new_real(0.0),
                ],
            ),
            (
                ConfiguredScalarType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                [
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(12)),
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(-3)),
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(0)),
                ],
            ),
        ];
        for (scalar_type, values) in cases {
            let mut rows: Vec<Row> = values.iter().map(|v| vec![v.clone()]).collect();
            stable_order_prepared_rows(
                &mut rows,
                1,
                &[PreparedOrderColumn::new(
                    0,
                    ConfiguredOrderDirection::Ascending,
                    scalar_type,
                    false,
                )],
            )
            .expect("widened scalar rows are orderable");
            assert_eq!(
                rows,
                vec![
                    vec![values[1].clone()],
                    vec![values[2].clone()],
                    vec![values[0].clone()],
                ],
                "{scalar_type:?} must order by value"
            );
        }
    }
}
