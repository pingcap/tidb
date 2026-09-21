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
pub(crate) struct VecGroupChecker {
    group_by_items: Vec<Expression>,
    collations: Vec<Collation>,
    previous_last_key: Option<Vec<u8>>,
    group_offsets: Vec<usize>,
    same_group: Vec<bool>,
    next_group_id: usize,
}

impl VecGroupChecker {
    /// Creates a checker for `group_by_items`.
    #[must_use]
    pub(crate) fn new(group_by_items: Vec<Expression>) -> Self {
        let collations = group_by_items
            .iter()
            .map(|item| {
                // GetCollator uses the exact FieldType spelling, including its
                // binary fallback for unknown or differently-cased names.
                item.static_type().map_or_else(
                    || collation_of_node(item),
                    |field| {
                        field
                            .runtime_collator_with_mode(true)
                            .new_collation()
                            .unwrap_or(Collation::Binary)
                    },
                )
            })
            .collect();
        Self {
            group_by_items,
            collations,
            previous_last_key: None,
            group_offsets: Vec::new(),
            same_group: Vec::new(),
            next_group_id: 0,
        }
    }

    /// Evaluates the grouping expressions and splits `chunk` into runs.
    ///
    /// The caller, like the upstream stream/window/merge executors, must not
    /// pass an empty chunk.
    pub(crate) fn split_into_groups(
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

        // Go evaluates each item's first and last row before considering
        // any interior row. Equal encoded boundaries skip the vector pass.
        let mut first = Vec::with_capacity(self.group_by_items.len());
        let mut last = Vec::with_capacity(self.group_by_items.len());
        for (item, collation) in self.group_by_items.iter().zip(&self.collations) {
            first.push(eval_group_item(item, ctx, chunk.get_row(0), *collation)?);
            last.push(eval_group_item(
                item,
                ctx,
                chunk.get_row(rows - 1),
                *collation,
            )?);
        }
        let first_encoded = encode_boundary_key(ctx, &first, &self.collations)?;
        let last_encoded = encode_boundary_key(ctx, &last, &self.collations)?;
        let continues_previous = self
            .previous_last_key
            .as_ref()
            .filter(|key| !key.is_empty())
            == Some(&first_encoded);
        let one_group = first_encoded == last_encoded;
        self.previous_last_key = Some(last_encoded);
        if one_group {
            self.group_offsets.push(rows);
            return Ok(continues_previous);
        }

        // Resolve one key column at a time, retaining only the prior datum
        // and a reusable row mask instead of allocating a key Vec per row.
        self.same_group.resize(rows, true);
        self.same_group[0] = false;
        for (item, collation) in self.group_by_items.iter().zip(&self.collations) {
            if resolve_integer_column(item, chunk, &mut self.same_group) {
                continue;
            }
            let mut previous = eval_group_item(item, ctx, chunk.get_row(0), *collation)?;
            for row in 1..rows {
                let current = eval_group_item(item, ctx, chunk.get_row(row), *collation)?;
                if self.same_group[row] && !same_group_value(&previous, &current, *collation)? {
                    self.same_group[row] = false;
                }
                previous = current;
            }
        }
        self.group_offsets.extend(
            self.same_group
                .iter()
                .enumerate()
                .skip(1)
                .filter_map(|(row, same)| (!same).then_some(row)),
        );
        self.group_offsets.push(rows);
        Ok(continues_previous)
    }

    /// Splits already-evaluated adjacent keys for source-contract fixtures.
    #[cfg(test)]
    pub(crate) fn split_evaluated(
        &mut self,
        keys: &[Vec<Datum>],
        collations: &[Collation],
    ) -> Result<bool, EvalError> {
        self.split_key_iter(keys.iter().map(Vec::as_slice), collations)
    }

    #[cfg(test)]
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
        let first_encoded = encode_boundary_key(&tidb_expr::NoColumns, first, collations)?;
        let last_encoded = encode_boundary_key(&tidb_expr::NoColumns, last, collations)?;
        let continues_previous = self
            .previous_last_key
            .as_ref()
            .filter(|key| !key.is_empty())
            == Some(&first_encoded);
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
    pub(crate) fn get_next_group(&mut self) -> (usize, usize) {
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
    pub(crate) fn is_exhausted(&self) -> bool {
        self.next_group_id >= self.group_offsets.len()
    }

    /// Clears current-chunk state while retaining the previous chunk's key.
    pub(crate) fn reset(&mut self) {
        self.group_offsets.clear();
        self.same_group.clear();
        self.next_group_id = 0;
    }

    /// Number of groups in the current chunk.
    #[must_use]
    pub(crate) fn group_count(&self) -> usize {
        self.group_offsets.len()
    }
}

/// Bare integer columns already have Go's ETInt representation. Compare their
/// cells directly without allocating temporary datums; respect chunk selection.
fn resolve_integer_column(expr: &Expression, chunk: &Chunk, same_group: &mut [bool]) -> bool {
    if let Expression::Column(column) = expr {
        let is_int = column.get_static_type().is_some_and(|field_type| {
            matches!(
                field_type.code(),
                tidb_datatype::FieldTypeCode::Tiny
                    | tidb_datatype::FieldTypeCode::Short
                    | tidb_datatype::FieldTypeCode::Int24
                    | tidb_datatype::FieldTypeCode::Long
                    | tidb_datatype::FieldTypeCode::LongLong
                    | tidb_datatype::FieldTypeCode::Year
            )
        });
        if let Some(index) = is_int
            .then(|| usize::try_from(column.index).ok())
            .flatten()
            .filter(|index| *index < chunk.num_cols())
        {
            let cells = chunk.column(index);
            if cells.type_size() == 8 {
                let mut previous = chunk.get_row(0).idx();
                for (row, same) in same_group.iter_mut().enumerate().skip(1) {
                    let physical = chunk.get_row(row).idx();
                    if *same {
                        let (previous_null, null) =
                            (cells.is_null(previous), cells.is_null(physical));
                        if previous_null != null
                            || (!null && cells.get_int64(previous) != cells.get_int64(physical))
                        {
                            *same = false;
                        }
                    }
                    previous = physical;
                }
                return true;
            }
        }
    }
    false
}

/// The Go checker calls EvalInt/EvalReal/EvalString, not the generic Eval.
/// Hybrid values therefore enter the key's declared evaluation domain before
/// either boundary encoding or adjacent comparison.
fn eval_group_item(
    item: &Expression,
    ctx: &impl Columns,
    row: tidb_chunk::row::Row<'_>,
    collation: Collation,
) -> Result<Datum, EvalError> {
    let value = item.eval(ctx, row)?;
    Ok(
        match (item.static_type().map(|field| field.eval_type()), value) {
            (Some(tidb_datatype::EvalType::Int), Datum::UInt(value)) => Datum::Int(value as i64),
            (
                Some(tidb_datatype::EvalType::Int),
                Datum::Bit(value) | Datum::BinaryLiteral(value),
            ) => {
                let warnings = GroupWarnings(ctx);
                let zone = ctx.time_zone();
                let context = tidb_datatype::ConversionContext::new(
                    ctx.type_flags(),
                    tidb_datatype::ConversionLocation::from_time_zone(&zone),
                    &warnings,
                );
                let (integer, error) = value.to_int_with_context(&context);
                if let Some(error) = error {
                    return Err(EvalError::Conversion(error));
                }
                Datum::Int(integer as i64)
            }
            (Some(tidb_datatype::EvalType::Real), Datum::Float32(value)) => Datum::Real(value),
            (Some(tidb_datatype::EvalType::String), value @ (Datum::Enum(..) | Datum::Set(..))) => {
                Datum::String(StringDatum::new(value.go_bytes(), collation))
            }
            (_, value) => value,
        },
    )
}

struct GroupWarnings<'a, C>(&'a C);

impl<C: Columns> tidb_datatype::ConversionWarningAppender for GroupWarnings<'_, C> {
    fn append_conversion_warning(&self, warning: tidb_error::terror::TerrorError) {
        let warning = warning.to_sql_error();
        self.0.append_warning(warning.code, &warning.message);
    }
}

/// Go compares evaluated columns in their own domains. Avoid scalar SQL
/// coercion (and its temporary datum clones) when only equality is needed.
fn same_group_value(left: &Datum, right: &Datum, collation: Collation) -> Result<bool, EvalError> {
    if let (Some(left), Some(right)) = (left.as_raw_bytes(), right.as_raw_bytes()) {
        return Ok(if tidb_datatype::new_collation_enabled() {
            collation.immutable_key(left) == collation.immutable_key(right)
        } else {
            left == right
        });
    }
    Ok(match (left, right) {
        (Datum::Null, Datum::Null) => true,
        (Datum::Null, _) | (_, Datum::Null) => false,
        (Datum::Int(left), Datum::Int(right)) => left == right,
        (Datum::Real(left), Datum::Real(right)) => left == right,
        (Datum::Decimal(left), Datum::Decimal(right)) => left.cmp(right).is_eq(),
        (Datum::Time(left), Datum::Time(right)) => left.compare(*right).is_eq(),
        (Datum::Duration(left), Datum::Duration(right)) => {
            left.nanoseconds() == right.nanoseconds()
        }
        (Datum::Json(left), Datum::Json(right)) => {
            tidb_datatype::compare_binary_json(left, right).is_eq()
        }
        (Datum::VectorFloat32(left), Datum::VectorFloat32(right)) => left.compare(right).is_eq(),
        _ => tidb_expr::compare_datums_with_collation(left, right, collation)?.is_eq(),
    })
}

fn encode_boundary_key(
    ctx: &impl Columns,
    values: &[Datum],
    collations: &[Collation],
) -> Result<Vec<u8>, EvalError> {
    let values = values
        .iter()
        .zip(collations)
        .map(|(value, collation)| match value {
            Datum::String(_) | Datum::Bytes(_) | Datum::Enum(_, _) | Datum::Set(_, _) => {
                Datum::String(StringDatum::new(value.go_bytes(), *collation))
            }
            // Go creates fresh boundary decimals through ToString/FromString,
            // dropping Datum.Length/Frac from the source column.
            Datum::Decimal(value) => {
                Datum::Decimal(tidb_datatype::Decimal::from_literal(&value.to_string()))
            }
            value => value.clone(),
        })
        .collect::<Vec<_>>();
    match tidb_codec::Encoder::new(tidb_datatype::new_collation_enabled())
        .encode_key_in_timezone(&ctx.time_zone(), &values)
    {
        Ok(encoded) => Ok(encoded),
        Err(tidb_codec::CodecError::InvalidMysqlTimestamp(value)) => {
            ctx.handle_truncate(&format!("Incorrect time value: '{}'", value.core_time()))?;
            // EncodeMySQLTime returns nil on conversion failure, discarding
            // even an already encoded prefix. ErrCtx may keep the statement.
            Ok(Vec::new())
        }
        Err(_) => Err(EvalError::Unsupported(
            "group boundary key cannot be encoded",
        )),
    }
}

#[cfg(test)]
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
    fn runtime_collation_mode_and_exact_names_match_go() {
        // This switch is process-global; run the fixture alone in a child
        // test process so concurrent executor tests cannot observe it.
        const CHILD: &str = "TIDB_GROUP_CHECKER_COLLATION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "vec_group_checker::tests::runtime_collation_mode_and_exact_names_match_go",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        for enabled in [false, true] {
            tidb_datatype::set_new_collation_enabled(enabled);
            for name in [
                "utf8mb4_general_ci",
                "UTF8MB4_GENERAL_CI",
                "unknown_collation",
                "utf8mb4_bin",
            ] {
                let field = FieldType::new(FieldTypeCode::VarString).with_collation_name(name);
                let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 4);
                for value in ["A", "A ", "a", "b"] {
                    chunk.append_string(0, value);
                }
                let mut checker = VecGroupChecker::new(vec![column(0, field)]);
                assert!(!checker.split_into_groups(&NoColumns, &chunk).unwrap());
                let ci = enabled && name == "utf8mb4_general_ci";
                let expected = if !enabled {
                    vec![(0, 1), (1, 2), (2, 3), (3, 4)]
                } else if ci {
                    vec![(0, 3), (3, 4)]
                } else {
                    vec![(0, 2), (2, 3), (3, 4)]
                };
                assert_eq!(
                    ranges(&mut checker),
                    expected,
                    "enabled={enabled}, name={name}"
                );
                let expected_key: &[u8] = if ci {
                    &[1, 0, 66, 0, 0, 0, 0, 0, 0, 249]
                } else {
                    &[1, 98, 0, 0, 0, 0, 0, 0, 0, 248]
                };
                assert_eq!(checker.previous_last_key.as_deref(), Some(expected_key));
                chunk.reset();
                chunk.append_string(0, "B ");
                assert_eq!(checker.split_into_groups(&NoColumns, &chunk).unwrap(), ci);
            }
        }
    }

    #[test]
    fn timestamp_boundary_errors_follow_statement_policy() {
        use tidb_datatype::{CoreTime, Time, TimeType};
        use tidb_expr::{ErrorLevel, SessionTimeZone};
        struct Context {
            level: ErrorLevel,
            zone: SessionTimeZone,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.level
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.zone.clone()
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        for zone in [
            SessionTimeZone::utc(),
            SessionTimeZone::Fixed {
                name: "+08:00".to_owned(),
                offset_secs: 8 * 3600,
            },
        ] {
            for level in [ErrorLevel::Error, ErrorLevel::Warn, ErrorLevel::Ignore] {
                let context = Context {
                    level,
                    zone: zone.clone(),
                    warnings: Default::default(),
                };
                let field = FieldType::new(FieldTypeCode::Timestamp);
                let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 2);
                for day in [1, 2] {
                    chunk.append_datum(
                        0,
                        &Datum::Time(
                            Time::new(
                                CoreTime::from_date(2020, 0, day, 0, 0, 0, 0),
                                TimeType::Timestamp,
                                0,
                            )
                            .unwrap(),
                        ),
                    );
                }
                let mut checker = VecGroupChecker::new(vec![column(0, field)]);
                let result = checker.split_into_groups(&context, &chunk);
                let first_message = "Incorrect time value: '{2020 0 1 0 0 0 0}'";
                if !zone.is_utc() && level == ErrorLevel::Error {
                    assert_eq!(
                        result,
                        Err(EvalError::TruncatedWrongValue(first_message.to_owned()))
                    );
                    assert_eq!(checker.group_count(), 0);
                    assert!(checker.previous_last_key.is_none());
                } else {
                    assert!(!result.as_ref().unwrap());
                    assert_eq!(
                        ranges(&mut checker),
                        if zone.is_utc() {
                            vec![(0, 1), (1, 2)]
                        } else {
                            vec![(0, 2)]
                        }
                    );
                    let expected_key: &[u8] = if zone.is_utc() {
                        &[4, 25, 165, 4, 0, 0, 0, 0, 0]
                    } else {
                        &[]
                    };
                    assert_eq!(checker.previous_last_key.as_deref(), Some(expected_key));
                }
                let expected_warnings = if !zone.is_utc() && level == ErrorLevel::Warn {
                    vec![
                        (1292, first_message.to_owned()),
                        (
                            1292,
                            "Incorrect time value: '{2020 0 2 0 0 0 0}'".to_owned(),
                        ),
                    ]
                } else {
                    vec![]
                };
                assert_eq!(*context.warnings.borrow(), expected_warnings);
                if result.is_ok() {
                    // Go treats a zero-length previous key as no prior group.
                    assert!(!checker.split_into_groups(&context, &chunk).unwrap());
                }
            }
        }
    }

    #[test]
    fn typed_group_boundaries_match_go_evaluation_domains() {
        use tidb_datatype::{
            BinaryLiteral, CoreTime, FieldTypeFlags, MySqlDuration, MysqlSet, Time, TimeType,
            VectorFloat32,
        };
        let time = |day, kind| {
            Datum::Time(Time::new(CoreTime::from_date(2020, 1, day, 12, 0, 0, 0), kind, 0).unwrap())
        };
        let string = |value: &str| {
            Datum::String(StringDatum::new(
                value.as_bytes(),
                Collation::Utf8Mb4GeneralCi,
            ))
        };
        let enum_value =
            |name, value| Datum::new_enum(MysqlEnum::new(name, value), Collation::Utf8Mb4GeneralCi);
        let set_value =
            |name, value| Datum::new_set(MysqlSet::new(name, value), Collation::Utf8Mb4GeneralCi);
        // Endpoint bytes and ranges captured from all Go evaluation domains,
        // with both vectorized evaluation modes and a +08:00 session zone.
        let cases = [
            (
                "int",
                FieldTypeCode::LongLong,
                0,
                [Datum::Int(1), Datum::Int(1), Datum::Int(2)],
                "038000000000000002",
            ),
            (
                "uint",
                FieldTypeCode::LongLong,
                FieldTypeFlags::UNSIGNED,
                [Datum::UInt(1), Datum::UInt(1), Datum::UInt(u64::MAX)],
                "037fffffffffffffff",
            ),
            (
                "bit",
                FieldTypeCode::Bit,
                0,
                [
                    Datum::Bit(BinaryLiteral::from(vec![1])),
                    Datum::Bit(BinaryLiteral::from(vec![0, 1])),
                    Datum::Bit(BinaryLiteral::from(vec![2])),
                ],
                "038000000000000002",
            ),
            (
                "float",
                FieldTypeCode::Float,
                0,
                [
                    Datum::Float32(1.0),
                    Datum::Float32(1.0),
                    Datum::Float32(2.0),
                ],
                "05c000000000000000",
            ),
            (
                "double",
                FieldTypeCode::Double,
                0,
                [Datum::Real(1.0), Datum::Real(1.0), Datum::Real(2.0)],
                "05c000000000000000",
            ),
            (
                "decimal",
                FieldTypeCode::NewDecimal,
                0,
                [
                    Datum::Decimal(Decimal::from_literal("1.0")),
                    Datum::Decimal(Decimal::from_literal("1.00")),
                    Datum::Decimal(Decimal::from_literal("2.00")),
                ],
                "0603028200",
            ),
            (
                "date",
                FieldTypeCode::Date,
                0,
                [
                    time(1, TimeType::Date),
                    time(1, TimeType::Date),
                    time(2, TimeType::Date),
                ],
                "0419a544c000000000",
            ),
            (
                "datetime",
                FieldTypeCode::Datetime,
                0,
                [
                    time(1, TimeType::DateTime),
                    time(1, TimeType::DateTime),
                    time(2, TimeType::DateTime),
                ],
                "0419a544c000000000",
            ),
            (
                "timestamp",
                FieldTypeCode::Timestamp,
                0,
                [
                    time(1, TimeType::Timestamp),
                    time(1, TimeType::Timestamp),
                    time(2, TimeType::Timestamp),
                ],
                "0419a5444000000000",
            ),
            (
                "duration",
                FieldTypeCode::Duration,
                0,
                [
                    Datum::Duration(MySqlDuration::new(0, 0, 1, 0, 0).unwrap()),
                    Datum::Duration(MySqlDuration::new(0, 0, 1, 0, 0).unwrap()),
                    Datum::Duration(MySqlDuration::new(0, 0, 2, 0, 0).unwrap()),
                ],
                "078000000077359400",
            ),
            (
                "json",
                FieldTypeCode::Json,
                0,
                [
                    Datum::Json(BinaryJSON::parse("1").unwrap()),
                    Datum::Json(BinaryJSON::parse("1.0").unwrap()),
                    Datum::Json(BinaryJSON::parse("2").unwrap()),
                ],
                "0a090200000000000000",
            ),
            (
                "vector",
                FieldTypeCode::VectorFloat32,
                0,
                [
                    Datum::VectorFloat32(VectorFloat32::parse("[1]").unwrap()),
                    Datum::VectorFloat32(VectorFloat32::parse("[1]").unwrap()),
                    Datum::VectorFloat32(VectorFloat32::parse("[2]").unwrap()),
                ],
                "140100000000000040",
            ),
            (
                "string",
                FieldTypeCode::VarString,
                0,
                [string("a"), string("A"), string("b")],
                "010042000000000000f9",
            ),
            (
                "enum",
                FieldTypeCode::Enum,
                0,
                [enum_value("a", 1), enum_value("A", 2), enum_value("b", 3)],
                "010042000000000000f9",
            ),
            (
                "enum_int",
                FieldTypeCode::Enum,
                FieldTypeFlags::ENUM_SET_AS_INT,
                [enum_value("a", 1), enum_value("a", 1), enum_value("b", 2)],
                "038000000000000002",
            ),
            (
                "set",
                FieldTypeCode::Set,
                0,
                [set_value("a", 1), set_value("A", 2), set_value("b", 4)],
                "010042000000000000f9",
            ),
        ];
        let ctx = crate::stmt_context::StmtContext::default().with_time_zone(
            tidb_expr::SessionTimeZone::Fixed {
                name: "+08:00".to_owned(),
                offset_secs: 8 * 3600,
            },
        );
        let mut mismatches = Vec::new();
        for (name, code, flags, values, expected_key) in cases {
            let mut field = FieldType::new(code);
            field.add_flags(flags);
            field.set_collation(Collation::Utf8Mb4GeneralCi);
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 4);
            chunk.append_null(0);
            for value in &values {
                chunk.append_datum(0, value);
            }
            let mut checker = VecGroupChecker::new(vec![column(0, field)]);
            match checker.split_into_groups(&ctx, &chunk) {
                Ok(continues) => assert!(!continues, "{name}"),
                Err(error) => {
                    mismatches.push(format!("{name}: {error:?}"));
                    continue;
                }
            }
            let actual_ranges = ranges(&mut checker);
            if actual_ranges != [(0, 1), (1, 3), (3, 4)] {
                mismatches.push(format!("{name}: ranges {actual_ranges:?}"));
            }
            let encoded = checker
                .previous_last_key
                .as_ref()
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            if encoded != expected_key {
                mismatches.push(format!(
                    "{name}: boundary {encoded}, expected {expected_key}"
                ));
            }
            chunk.reset();
            chunk.append_datum(0, &values[2]);
            assert!(checker.split_into_groups(&ctx, &chunk).unwrap(), "{name}");
            assert_eq!(ranges(&mut checker), [(0, 1)], "{name}");
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn bit_boundary_overflow_preserves_go_error_policy() {
        use tidb_expr::ErrorLevel;
        struct Context {
            level: ErrorLevel,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.level
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let message = "Truncated incorrect BINARY value: '0x010000000000000000'";
        for level in [ErrorLevel::Error, ErrorLevel::Warn, ErrorLevel::Ignore] {
            let context = Context {
                level,
                warnings: Default::default(),
            };
            let field = FieldType::new(FieldTypeCode::Bit);
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 2);
            let value = Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![
                1, 0, 0, 0, 0, 0, 0, 0, 0,
            ]));
            chunk.append_datum(0, &value);
            chunk.append_datum(0, &value);
            let mut checker = VecGroupChecker::new(vec![column(0, field)]);
            let result = checker.split_into_groups(&context, &chunk);
            if level == ErrorLevel::Error {
                let Err(EvalError::Conversion(error)) = result else {
                    panic!("expected Go's conversion error, got {result:?}");
                };
                let error = error.to_sql_error();
                assert_eq!((error.code, error.message.as_str()), (1292, message));
                assert!(checker.previous_last_key.is_none());
                assert_eq!(checker.group_count(), 0);
            } else {
                assert!(!result.unwrap());
                assert_eq!(ranges(&mut checker), [(0, 2)]);
                assert_eq!(
                    checker.previous_last_key.as_deref(),
                    Some(&[3, 127, 255, 255, 255, 255, 255, 255, 255][..])
                );
            }
            let expected = if level == ErrorLevel::Warn {
                vec![(1292, message.to_owned()); 2]
            } else {
                vec![]
            };
            assert_eq!(*context.warnings.borrow(), expected);
        }
    }

    #[test]
    fn integer_column_groups_follow_selected_rows_and_nulls() {
        for code in [
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
            FieldTypeCode::Year,
        ] {
            let field = FieldType::new(code);
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 7);
            for value in [Some(7), None, Some(2), Some(2), None, Some(1), Some(1)] {
                match value {
                    Some(value) => chunk.append_int64(0, value),
                    None => chunk.append_null(0),
                }
            }
            chunk.set_sel(Some(vec![4, 1, 5, 6, 2, 3, 0]));
            let mut checker = VecGroupChecker::new(vec![column(0, field)]);
            assert!(!checker.split_into_groups(&NoColumns, &chunk).unwrap());
            assert_eq!(ranges(&mut checker), vec![(0, 2), (2, 4), (4, 6), (6, 7)]);
            chunk.set_sel(Some(vec![0]));
            assert!(checker.split_into_groups(&NoColumns, &chunk).unwrap());
            assert_eq!(ranges(&mut checker), vec![(0, 1)]);
        }
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
    fn equal_boundary_keys_skip_interior_evaluation_warnings() {
        #[derive(Default)]
        struct Warnings(std::cell::RefCell<Vec<(u16, String)>>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }
        let field = FieldType::new(FieldTypeCode::Varchar);
        let expr = Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("cast_signed"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![column(0, field.clone())],
        ));
        let mut chunk = Chunk::new_with_capacity(&[field], 32);
        for _ in 0..32 {
            chunk.append_string(0, "1bad");
        }
        let context = Warnings::default();
        let mut checker = VecGroupChecker::new(vec![expr]);
        assert!(!checker.split_into_groups(&context, &chunk).unwrap());
        assert_eq!(ranges(&mut checker), [(0, 32)]);
        assert_eq!(context.0.borrow().len(), 2);
        assert!(context.0.borrow().iter().all(|(code, _)| *code == 1292));

        // Different boundaries require a complete column evaluation after
        // the two endpoint evaluations, including rows already compared.
        context.0.borrow_mut().clear();
        chunk.append_string(0, "2bad");
        assert!(checker.split_into_groups(&context, &chunk).unwrap());
        assert_eq!(ranges(&mut checker), [(0, 32), (32, 33)]);
        assert_eq!(context.0.borrow().len(), 35);
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
