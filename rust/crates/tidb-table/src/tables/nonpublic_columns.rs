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

//! Columns Go's DDL is adding, dropping or changing: the write path's half of
//! `WritableCols` / `DeletableCols` (`pkg/table/tables/tables.go`:
//! `AddRecord`, `updateRecord`, and the rows `RemoveRecord` is handed) and
//! `checkDataForModifyColumn`.
//!
//! A write row holds each column at its Go `Offset` in `TableCommon.Columns`
//! ([`TableCommon::deletable_columns`]); Go's DDL keeps the non-public columns
//! after the public ones, so a public row is its prefix. Go's planner scans
//! `WritableCols` for an UPDATE
//! and `DeletableCols` for a DELETE; this tier's readers produce public rows,
//! so the slots are filled here from the stored record, the same bytes Go's
//! scan decodes. Skipping them lost a value another session (one schema
//! version ahead, where the column is public) had written, which is exactly
//! the case Go's comment in `buildDataSource` describes.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_datatype::{ConversionFlags, Datum, FieldType, FieldTypeCode, SessionTimeZone};

use super::{is_column_writable, TableCommon, KvTableError, TableHandle};
use tidb_kv::storage::TableStorage;

impl TableCommon {
    /// Appends a column a DDL is adding, dropping or changing to a table
    /// built from columns: its `ColumnInfo` carries the state (and, for a
    /// MODIFY COLUMN's changing column, `ChangeStateInfo`), and both
    /// `TableInfo.Columns` and [`Self::deletable_columns`] list it last, as
    /// Go's DDL places it.
    pub fn add_nonpublic_column(&mut self, column: crate::column::Column) {
        debug_assert_ne!(column.read().state, tidb_model::SchemaState::PUBLIC);
        let offset = self.columns.len();
        column.info.write().offset = offset as i64;
        let shared = column.info.clone();
        self.update_meta_cloned(|meta| meta.columns.push_handle_go(Some(shared)));
        self.columns_mut().push(column);
    }

    /// Records Go `ColumnInfo.ChangingFieldType` for the public column at
    /// `offset`.
    pub fn set_changing_field_type(&mut self, offset: usize, field_type: FieldType) {
        self.changing_field_types.insert(offset, field_type);
    }

    /// Records the key parts of index `index_id` that use their column's
    /// changing type (Go `IndexColumn.UseChangingType`).
    pub fn set_changing_index_parts(&mut self, index_id: i64, parts: Vec<usize>) {
        if !parts.is_empty() {
            self.changing_index_parts.insert(index_id, parts);
        }
    }

    /// Go `model.GetIdxChangingFieldType`: the type key part `position` of
    /// index `index_id` encodes, the column's changing type when the part
    /// uses it.
    #[must_use]
    pub(crate) fn get_idx_changing_field_type(
        &self,
        index_id: i64,
        position: usize,
        offset: usize,
    ) -> Option<FieldType> {
        if self
            .changing_index_parts
            .get(&index_id)
            .is_some_and(|parts| parts.contains(&position))
        {
            if let Some(changing) = self.changing_field_types.get(&offset) {
                return Some(changing.clone());
            }
        }
        self.columns.get(offset).map(|column| column.read().field_type.clone())
    }

    /// Go `index.castIndexValuesToChangingTypes`: a key part using its
    /// column's changing type casts the value with strict flags. The rows
    /// reaching an index were already admitted by
    /// [`Self::check_data_for_modify_column`] under the same cast, so a
    /// failure leaves the original value available to the caller.
    pub(crate) fn cast_index_values_to_changing_types(
        &self,
        index_id: i64,
        position: usize,
        offset: usize,
        value: &Datum,
    ) -> Option<Datum> {
        if !self
            .changing_index_parts
            .get(&index_id)
            .is_some_and(|parts| parts.contains(&position))
        {
            return None;
        }
        let changing = self.changing_field_types.get(&offset)?;
        // Go `CastColumnValueWithStrictMode`: `strictCtx`.
        match value.convert_to_in_context(
            changing,
            &tidb_datatype::ConversionContext::strict(),
            &SessionTimeZone::utc(),
        ) {
            Ok(converted) if converted.error.is_none() => Some(converted.value),
            _ => None,
        }
    }

    /// Go `table.CastColumnValue(ctx, value, col, false, false)`: what a
    /// changing column holds for its dependency's value.
    fn cast_into_column(
        value: Datum,
        column: &crate::column::Column,
        ctx: Option<&dyn tidb_tblctx::MutateContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Datum, KvTableError> {
        match ctx {
            Some(ctx) => {
                crate::column::cast_value(value, &column.read().field_type, column.read().name.original(), ctx, false)
                    .map_err(|error| KvTableError::Mysql(error.to_mysql_error()))
            }
            None => Ok(value
                .convert_to_in_context(
                    &column.read().field_type,
                    &tidb_datatype::ConversionContext::new(
                        flags,
                        tidb_datatype::ConversionLocation::from_time_zone(zone),
                        &tidb_context::IGNORE_CONVERSION_WARNINGS,
                    ),
                    zone,
                )
                .ok()
                .filter(|converted| converted.error.is_none())
                .map_or(Datum::Null, |converted| converted.value)),
        }
    }

    /// Go `checkDataForModifyColumn` for every public column a MODIFY COLUMN
    /// is checking writes against: the value must cast to the changing type
    /// under strict flags, and a VARCHAR -> CHAR change must not lose
    /// trailing spaces.
    pub(crate) fn check_data_for_modify_column(
        &self,
        row: &[Datum],
        ctx: Option<&dyn tidb_tblctx::MutateContext>,
    ) -> Result<(), KvTableError> {
        for (offset, changing) in &self.changing_field_types {
            let Some(data) = row.get(*offset) else {
                continue;
            };
            let value = match ctx {
                Some(ctx) => crate::column::cast_table_value_with_flags(
                    data.clone(),
                    changing,
                    "",
                    ctx,
                    tidb_datatype::STRICT_FLAGS,
                    false,
                )
                .map_err(|error| KvTableError::Mysql(error.to_mysql_error()))?,
                None => match data.convert_to_in_context(
                    changing,
                    &tidb_datatype::ConversionContext::strict(),
                    &SessionTimeZone::utc(),
                ) {
                    Ok(converted) if converted.error.is_none() => converted.value,
                    _ => {
                        return Err(KvTableError::Mysql(tidb_terror::MysqlError::unknown(
                            "data truncation error during modify column",
                        )))
                    }
                },
            };
            if changing.code() == FieldTypeCode::String && datum_text(&value) != datum_text(data) {
                return Err(KvTableError::Mysql(tidb_terror::MysqlError::unknown(
                    "data truncation error during modify column",
                )));
            }
        }
        Ok(())
    }

    /// Go `AddRecord`'s loop: the write row of an INSERT, `None` when the
    /// table has no non-public column. A delete-only column gets no value; a
    /// changing column casts the column it replaces; any other its original
    /// default.
    pub(crate) fn insert_write_row(
        &self,
        row: &[Datum],
        ctx: Option<&dyn tidb_tblctx::MutateContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        if !self.has_nonpublic_columns() {
            return Ok(None);
        }
        let mut write_row = self.public_write_row(row);
        for (offset, column) in self.nonpublic_columns_with_offsets() {
            write_row[offset] = if !is_column_writable(column) {
                Datum::Null
            } else if let Some(dependency) = dependency_column_offset(column) {
                Self::cast_into_column(
                    row.get(dependency).cloned().unwrap_or(Datum::Null),
                    column,
                    ctx,
                    flags,
                    zone,
                )?
            } else {
                column
                    .origin_default_value(flags, zone)
                    .map_err(|error| KvTableError::Decode(error.to_string()))?
            };
        }
        Ok(Some(write_row))
    }

    /// A write row over [`Self::deletable_columns`] holding `row`'s public
    /// values; the non-public slots start NULL.
    fn public_write_row(&self, row: &[Datum]) -> Vec<Datum> {
        let public = self.public_columns.len();
        let mut write_row = row[..row.len().min(public)].to_vec();
        write_row.resize(self.columns.len(), Datum::Null);
        write_row
    }

    /// The columns a DDL holds in a non-public state, with their offsets.
    fn nonpublic_columns_with_offsets(&self) -> Vec<(usize, &crate::column::Column)> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.read().state != tidb_model::SchemaState::PUBLIC)
            .collect()
    }

    /// What the stored record holds for each non-public column, as Go's
    /// scan of `WritableCols` / `DeletableCols` decodes it: the value when
    /// present, else a changing column's cast of its dependency and any
    /// other column's original default.
    fn stored_nonpublic_values(
        &mut self,
        store: &mut dyn TableStorage,
        handle: &TableHandle,
        old: &[Datum],
        ctx: Option<&dyn tidb_tblctx::MutateContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Vec<(usize, Datum)>, KvTableError> {
        let columns = Arc::clone(&self.columns);
        let nonpublic: Vec<(usize, &crate::column::Column)> = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.read().state != tidb_model::SchemaState::PUBLIC)
            .collect();
        let types: BTreeMap<i64, FieldType> = nonpublic
            .iter()
            .map(|(_, column)| (column.read().id, column.read().field_type.clone()))
            .collect();
        let stored = match self.stored_record(store, handle)? {
            Some((_, entry)) => {
                tidb_tablecodec::decode_owned_table_row_to_map(entry, &types, Some(zone))
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?
            }
            None => BTreeMap::new(),
        };
        nonpublic
            .into_iter()
            .map(|(offset, column)| {
                let value = match stored.get(&column.read().id) {
                    Some(value) => Ok(value.clone()),
                    None => match dependency_column_offset(column) {
                        Some(dependency) => Self::cast_into_column(
                            old.get(dependency).cloned().unwrap_or(Datum::Null),
                            column,
                            ctx,
                            flags,
                            zone,
                        ),
                        None => column
                            .origin_default_value(flags, zone)
                            .map_err(|error| KvTableError::Decode(error.to_string())),
                    },
                };
                value.map(|value| (offset, value))
            })
            .collect()
    }

    /// Go `updateRecord`'s rows: the old and new write rows of an UPDATE,
    /// `None` when the table has no non-public column. A writable column
    /// keeps what the row holds, except that a changing one casts its
    /// dependency's new value; a delete-only changing column's old slot is
    /// the cast of the dependency's old value, which its index entries were
    /// filed under.
    pub(crate) fn update_write_rows(
        &mut self,
        store: &mut dyn TableStorage,
        handle: &TableHandle,
        old: &[Datum],
        new: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<Option<(Vec<Datum>, Vec<Datum>)>, KvTableError> {
        if !self.has_nonpublic_columns() {
            return Ok(None);
        }
        let flags = ctx.write_conversion_flags();
        let zone = ctx.session_zone();
        let stored = self.stored_nonpublic_values(store, handle, old, Some(ctx), flags, &zone)?;
        let mut old_row = self.public_write_row(old);
        let mut new_row = self.public_write_row(new);
        let columns = Arc::clone(&self.columns);
        for (offset, stored) in stored {
            let column = &columns[offset];
            let (old_value, new_value) = match (is_column_writable(column), dependency_column_offset(column)) {
                (false, Some(dependency)) => {
                    let value = Self::cast_into_column(
                        old.get(dependency).cloned().unwrap_or(Datum::Null),
                        column,
                        Some(ctx),
                        flags,
                        &zone,
                    )?;
                    (value.clone(), value)
                }
                (false, None) => (stored.clone(), stored),
                (true, Some(dependency)) => (
                    stored,
                    Self::cast_into_column(
                        new.get(dependency).cloned().unwrap_or(Datum::Null),
                        column,
                        Some(ctx),
                        flags,
                        &zone,
                    )?,
                ),
                (true, None) => (stored.clone(), stored),
            };
            old_row[offset] = old_value;
            new_row[offset] = new_value;
        }
        Ok(Some((old_row, new_row)))
    }

    /// The row `RemoveRecord` removes: the public row extended with what the
    /// record holds for each non-public column (Go `DeletableCols`).
    pub(crate) fn delete_write_row(
        &mut self,
        store: &mut dyn TableStorage,
        handle: &TableHandle,
        old: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        if !self.has_nonpublic_columns() {
            return Ok(None);
        }
        let zone = ctx.session_zone();
        let stored = self.stored_nonpublic_values(
            store,
            handle,
            old,
            Some(ctx),
            ctx.write_conversion_flags(),
            &zone,
        )?;
        let mut row = self.public_write_row(old);
        for (offset, value) in stored {
            row[offset] = value;
        }
        Ok(Some(row))
    }
}

/// Go `ChangeStateInfo.DependencyColumnOffset`: for the temporary column a
/// MODIFY COLUMN writes, the offset of the column whose value it casts.
fn dependency_column_offset(column: &crate::column::Column) -> Option<usize> {
    let column = column.read();
    if column.state == tidb_model::SchemaState::PUBLIC {
        return None;
    }
    let info = column.change_state_info.as_ref()?;
    let offset = info.read().dependency_column_offset;
    usize::try_from(offset).ok()
}

/// A value's text for the VARCHAR -> CHAR comparison (Go `GetString`).
fn datum_text(value: &Datum) -> Vec<u8> {
    match value {
        Datum::String(text) => text.bytes().to_vec(),
        Datum::Bytes(bytes) => bytes.clone(),
        other => other.to_bytes().unwrap_or_default(),
    }
}
