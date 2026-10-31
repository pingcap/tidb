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

//! Shared DML metadata and expression lowering for declared column defaults.

use super::*;

/// Everything both omission and explicit DEFAULT need from `ColumnInfo`.
/// Keeping the flags beside the stored default prevents INSERT, UPDATE and
/// ON DUPLICATE from growing subtly different nil-default implementations.
#[derive(Clone, Debug)]
pub(crate) struct ColumnDefaultMeta {
    pub(crate) default_value: Option<tidb_table::column_default::ColumnDefault>,
    pub(crate) not_null: bool,
    pub(crate) no_default_value: bool,
    pub(crate) name: String,
    pub(crate) field_type: tidb_datatype::FieldType,
    pub(crate) column_info_version: u64,
    pub(crate) generated: bool,
}

/// INSERT's top-level DEFAULT additionally runs
/// `CheckNoDefaultValueForInsert`; defaults inside UPDATE/ON DUPLICATE and
/// nested scalar expressions call only `GetColDefaultValue`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DefaultUse {
    Insert,
    Expression,
}

/// Captures the default-bearing metadata once per statement. INSERT omission,
/// INSERT's explicit `DEFAULT`, and UPDATE's explicit `DEFAULT` must all read
/// the same facts rather than grow separate default implementations.
pub(crate) fn column_metadata(table: &TableEntry) -> Vec<ColumnDefaultMeta> {
    match table {
        TableEntry::Kv(kv) => kv
            .visible_columns()
            .into_iter()
            .map(|(_, column)| kv_column_metadata(column))
            .collect(),
        // A matrix-backed table carries no column metadata, so every column
        // is nullable with no default -- the original mock behavior.
        TableEntry::Mem(mem) => mem
            .columns
            .iter()
            .map(|(name, field_type)| ColumnDefaultMeta {
                default_value: None,
                not_null: false,
                no_default_value: false,
                name: name.clone(),
                field_type: field_type.clone(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                generated: false,
            })
            .collect(),
        // Writes through either object are refused before row evaluation.
        TableEntry::View(view) => view
            .columns
            .iter()
            .map(|(name, field_type)| ColumnDefaultMeta {
                default_value: None,
                not_null: false,
                no_default_value: false,
                name: name.clone(),
                field_type: field_type.clone(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                generated: false,
            })
            .collect(),
        TableEntry::Sequence(_) => Vec::new(),
    }
}

/// [`column_metadata`] over every column of the stored row, hidden ones
/// included, in `TableInfo.Columns` order: what a write indexes by a stored
/// offset reads.
pub(crate) fn stored_column_metadata(table: &TableEntry) -> Vec<ColumnDefaultMeta> {
    match table {
        TableEntry::Kv(kv) => kv.columns().iter().map(kv_column_metadata).collect(),
        _ => column_metadata(table),
    }
}

/// [`ColumnDefaultMeta`] of one stored column.
pub(crate) fn kv_column_metadata(column: &tidb_table::kv_table::KvColumn) -> ColumnDefaultMeta {
    ColumnDefaultMeta {
        default_value: column.default_value.clone(),
        not_null: column
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL),
        no_default_value: column
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE),
        name: column.name.clone(),
        field_type: column.field_type.clone(),
        column_info_version: column.column_info_version,
        generated: column.generated.is_some(),
    }
}

/// The implicit clock assignments for one update target, prepared once per
/// statement. Explicitly assigned columns are excluded even when the written
/// expression preserves their old value, matching Go's `modified[i]` rule.
pub(crate) struct PreparedOnUpdateNow {
    columns: Vec<(
        usize,
        tidb_table::column_default::ColumnDefault,
        ColumnDefaultMeta,
    )>,
}

impl PreparedOnUpdateNow {
    pub(crate) fn new(
        metadata: &[ColumnDefaultMeta],
        explicitly_assigned: impl IntoIterator<Item = usize>,
    ) -> Result<Self, DriverError> {
        let assigned: std::collections::BTreeSet<usize> = explicitly_assigned.into_iter().collect();
        let mut columns = Vec::new();
        for (offset, meta) in metadata.iter().enumerate() {
            if assigned.contains(&offset)
                || !meta
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW)
            {
                continue;
            }
            let value =
                tidb_table::column_default::on_update_current_timestamp(&meta.name, &meta.field_type)?;
            columns.push((offset, value, meta.clone()));
        }
        Ok(Self { columns })
    }

    /// Applies the implicit clock only after some ordinary assignment really
    /// changed the row. A no-op update returns `false` without reading the
    /// statement clock.
    pub(crate) fn apply(
        &self,
        old_row: &[Datum],
        new_row: &mut [Datum],
        ctx: &crate::StmtContext,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<bool, DriverError> {
        if new_row == old_row {
            return Ok(false);
        }
        for (offset, default, meta) in &self.columns {
            new_row[*offset] = tidb_table::column_default::evaluate(
                default,
                &meta.field_type,
                meta.column_info_version,
                ctx.write_conversion_flags(),
                ctx,
                row,
            )
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        }
        Ok(true)
    }
}

/// The value an omitted column takes, following Go `GetColDefaultValue` and
/// `getColDefaultValueFromNil`: the stored `DEFAULT` when one was written, or
/// NULL for a nullable column; a NOT NULL column with no default is Go's
/// `ErrNoDefaultForField` under strict mode, and under a non-strict mode the
/// same message as a WARNING plus that type's zero value
/// (`getColDefaultValueFromNil`'s `if !strictSQLMode` arm). Captured from
/// TiDB under `sql_mode = ''`: `INSERT INTO t (b) VALUES (9)` into
/// `t(a INT NOT NULL, b INT NOT NULL DEFAULT 3)` is accepted, warns 1364 and
/// stores `0`.
pub(crate) fn column_default(
    meta: &[ColumnDefaultMeta],
    offset: usize,
    ctx: &crate::StmtContext,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Datum, DriverError> {
    materialize_column_default(&meta[offset], DefaultUse::Insert, ctx, row)
}

/// Materializes one column default at the statement-planning point selected
/// by the caller: an INSERT first runs Go `table.CheckNoDefaultValueForInsert`,
/// then both take Go `table.GetColDefaultValue`.
pub(crate) fn materialize_column_default(
    meta: &ColumnDefaultMeta,
    use_kind: DefaultUse,
    ctx: &crate::StmtContext,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Datum, DriverError> {
    if use_kind == DefaultUse::Insert {
        tidb_table::column_default::check_no_default_value_for_insert(
            meta.default_value.as_ref(),
            &meta.field_type,
            &meta.name,
            meta.not_null,
            meta.no_default_value,
            ctx,
        )?;
    }
    tidb_table::column_default::get_col_default_value(
        meta.default_value.as_ref(),
        &meta.field_type,
        &meta.name,
        meta.column_info_version,
        meta.not_null,
        ctx,
        row,
    )
    .map_err(DriverError::from)
}

/// Go `InsertValues.setValueForRefColumn`: the row an INSERT whose values
/// name its columns evaluates into. A column holds its default, which counts
/// as written. A column without one holds its type's zero value and stays
/// unwritten, so the row build still refuses (or warns about) it when the
/// statement leaves it out; the auto-increment column holds its zero and
/// stays unwritten as well, so it still takes an id. A generated column
/// reads the same way until the row build computes it.
pub(crate) fn set_value_for_ref_column(
    meta: &[ColumnDefaultMeta],
    extra_handle_offset: Option<usize>,
    row: &mut [Datum],
    assigned: &mut [bool],
    ctx: &crate::StmtContext,
    input: tidb_chunk::row::Row<'_>,
) -> Result<(), DriverError> {
    for (offset, column) in meta.iter().enumerate() {
        // `_tidb_rowid` is not one of Go's `Table.Cols()`.
        if Some(offset) == extra_handle_offset {
            continue;
        }
        match materialize_column_default(column, DefaultUse::Insert, ctx, input) {
            Ok(value) => {
                row[offset] = value;
                assigned[offset] = !column
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT);
            }
            Err(error) if error.equal(&tidb_table::table_errors::ERR_NO_DEFAULT_VALUE) => {
                row[offset] = crate::bad_null::zero_value(&column.field_type);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use tidb_datatype::{CoreTime, FieldTypeCode, FieldTypeFlags, MysqlEnum, Time, TimeType};

    #[derive(Clone)]
    enum Expected {
        Value(Datum),
        Error,
    }

    fn assert_materialized(
        field_type: &FieldType,
        default_value: Option<tidb_table::column_default::ColumnDefault>,
        strict: bool,
        expected: Expected,
    ) {
        let ctx = crate::StmtContext::for_dml(false, strict, false).with_time_zone(
            tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
        );
        let meta = ColumnDefaultMeta {
            default_value,
            not_null: field_type.has_flag(FieldTypeFlags::NOT_NULL),
            no_default_value: field_type.has_flag(FieldTypeFlags::NO_DEFAULT_VALUE),
            name: "a".to_owned(),
            field_type: field_type.clone(),
            column_info_version: tidb_model::column::COLUMN_INFO_VERSION2,
            generated: false,
        };
        let actual = materialize_column_default(
            &meta,
            DefaultUse::Expression,
            &ctx,
            tidb_chunk::row::Row::empty(),
        );
        match expected {
            Expected::Value(expected) => assert_eq!(actual.unwrap(), expected, "{field_type:?}"),
            Expected::Error => assert!(actual.is_err(), "{field_type:?}: {actual:?}"),
        }
    }

    #[test]
    fn test_get_default_value() {
        // Direct port of both loops in pkg/table/column_test.go's
        // TestGetDefaultValue. Each row is materialized once from
        // DefaultValue and once from OriginDefaultValue; the expression row
        // deliberately has no origin expression, just as ColumnInfo does.
        let not_null_bigint =
            FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::NOT_NULL);
        let nullable_bigint = FieldType::new(FieldTypeCode::LongLong);
        let not_null_enum = FieldType::new(FieldTypeCode::Enum)
            .with_flags(FieldTypeFlags::NOT_NULL)
            .with_elems(["abc", "def"]);
        let timestamp =
            FieldType::new(FieldTypeCode::Timestamp).with_flags(FieldTypeFlags::TIMESTAMP);
        let auto_bigint = FieldType::new(FieldTypeCode::LongLong)
            .with_flags(FieldTypeFlags::NOT_NULL | FieldTypeFlags::AUTO_INCREMENT);

        let zero_timestamp =
            Datum::new_time(Time::new(CoreTime::from_raw(0), TimeType::Timestamp, 0).unwrap());
        let local_timestamp = Datum::new_time(
            Time::new(
                CoreTime::from_date(2019, 5, 6, 12, 48, 49, 0),
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
        );
        let enum_first = Datum::new_enum(MysqlEnum::new("abc", 1), not_null_enum.collation());

        let value = |datum| Some(tidb_table::column_default::ColumnDefault::Value(datum));
        let computed_one = Some(tidb_table::column_default::ColumnDefault::Computed(Box::new(
            tidb_table::column_default::ComputedDefault {
                text: "1".to_owned(),
                kind: tidb_table::column_default::ComputedDefaultKind::Expression,
                expr: Ok(Expression::Constant(tidb_expr::constant::Constant::new(
                    Datum::Int(1),
                    not_null_bigint.clone(),
                ))),
                added_origin_safety: tidb_table::column_default::AddedOriginSafety::Safe,
            },
        )));

        let cases = vec![
            (
                not_null_bigint.clone(),
                value(Datum::Real(1.0)),
                value(Datum::Real(1.0)),
                false,
                Expected::Value(Datum::Int(1)),
                Expected::Value(Datum::Int(1)),
            ),
            (
                not_null_bigint.clone(),
                None,
                None,
                false,
                Expected::Value(Datum::Int(0)),
                Expected::Value(Datum::Int(0)),
            ),
            (
                nullable_bigint,
                None,
                None,
                false,
                Expected::Value(Datum::Null),
                Expected::Value(Datum::Null),
            ),
            (
                not_null_enum,
                None,
                None,
                false,
                Expected::Value(enum_first.clone()),
                Expected::Value(enum_first),
            ),
            (
                timestamp.clone(),
                value(Datum::new_string("0000-00-00 00:00:00")),
                value(Datum::new_string("0000-00-00 00:00:00")),
                false,
                Expected::Value(zero_timestamp.clone()),
                Expected::Value(zero_timestamp),
            ),
            (
                timestamp.clone(),
                value(Datum::new_string("2019-05-06 19:48:49")),
                value(Datum::new_string("2019-05-06 19:48:49")),
                true,
                Expected::Value(local_timestamp.clone()),
                Expected::Value(local_timestamp),
            ),
            (
                timestamp,
                value(Datum::new_string("not valid date")),
                value(Datum::new_string("not valid date")),
                true,
                Expected::Error,
                Expected::Error,
            ),
            (
                not_null_bigint.clone(),
                None,
                None,
                true,
                Expected::Error,
                Expected::Error,
            ),
            (
                auto_bigint,
                None,
                None,
                true,
                Expected::Value(Datum::Int(0)),
                Expected::Value(Datum::Int(0)),
            ),
            (
                not_null_bigint,
                computed_one,
                None,
                false,
                Expected::Value(Datum::Int(1)),
                Expected::Value(Datum::Int(0)),
            ),
        ];

        for (field_type, current, origin, strict, expected_current, expected_origin) in cases {
            assert_materialized(&field_type, current, strict, expected_current);
            assert_materialized(&field_type, origin, strict, expected_origin);
        }
    }
}
