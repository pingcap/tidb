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
//! A write row is the public columns followed by one slot per non-public
//! column in metadata order. Go's planner scans `WritableCols` for an UPDATE
//! and `DeletableCols` for a DELETE; this tier's readers produce public rows,
//! so the slots are filled here from the stored record, the same bytes Go's
//! scan decodes. Skipping them lost a value another session (one schema
//! version ahead, where the column is public) had written, which is exactly
//! the case Go's comment in `buildDataSource` describes.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_datatype::{ConversionFlags, Datum, FieldType, FieldTypeCode, SessionTimeZone};

use super::{KvColumn, KvTable, KvTableError, NonPublicColumn, TableHandle};

impl KvTable {
    /// Adds a column a DDL is adding, dropping or changing.
    pub fn add_nonpublic_column(&mut self, column: NonPublicColumn) {
        self.max_column_id = self.max_column_id.max(column.column.id);
        Arc::make_mut(&mut self.nonpublic_columns).push(column);
    }

    /// The columns only the write path sees.
    #[must_use]
    pub fn nonpublic_columns(&self) -> &[NonPublicColumn] {
        &self.nonpublic_columns
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

    /// The column a write row holds at `offset`: a public column, or past
    /// them a non-public one.
    #[must_use]
    pub(crate) fn write_column(&self, offset: usize) -> Option<&KvColumn> {
        self.columns.get(offset).or_else(|| {
            offset
                .checked_sub(self.columns.len())
                .and_then(|position| self.nonpublic_columns.get(position))
                .map(|column| &column.column)
        })
    }

    /// Go `model.GetIdxChangingFieldType`: the type key part `position` of
    /// index `index_id` encodes, the column's changing type when the part
    /// uses it.
    #[must_use]
    pub(crate) fn index_part_type(
        &self,
        index_id: i64,
        position: usize,
        offset: usize,
    ) -> Option<&FieldType> {
        if self
            .changing_index_parts
            .get(&index_id)
            .is_some_and(|parts| parts.contains(&position))
        {
            if let Some(changing) = self.changing_field_types.get(&offset) {
                return Some(changing);
            }
        }
        self.write_column(offset).map(|column| &column.field_type)
    }

    /// Go `index.castIndexValuesToChangingTypes`: a key part using its
    /// column's changing type casts the value with strict flags. The rows
    /// reaching an index were already admitted by
    /// [`Self::check_data_for_modify_column`] under the same cast, so a
    /// failure leaves the value as it was rather than inventing an error Go
    /// cannot reach.
    pub(crate) fn cast_index_part(
        &self,
        index_id: i64,
        position: usize,
        offset: usize,
        value: Datum,
    ) -> Datum {
        if !self
            .changing_index_parts
            .get(&index_id)
            .is_some_and(|parts| parts.contains(&position))
        {
            return value;
        }
        let Some(changing) = self.changing_field_types.get(&offset) else {
            return value;
        };
        match value.convert_to(changing, tidb_datatype::STRICT_FLAGS) {
            Ok(converted) if converted.event.is_none() => converted.value,
            _ => value,
        }
    }

    /// Go `table.CastColumnValue(ctx, value, col, false, false)`: what a
    /// changing column holds for its dependency's value.
    fn cast_into_column(
        value: Datum,
        column: &KvColumn,
        ctx: Option<&crate::StmtContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Datum, KvTableError> {
        match ctx {
            Some(ctx) => {
                crate::driver::cast_table_value(value, &column.field_type, &column.name, ctx, false)
                    .map_err(|error| KvTableError::ColumnCast(error.to_mysql_error()))
            }
            None => Ok(value
                .convert_to_in(&column.field_type, flags, zone)
                .map_or(Datum::Null, |converted| converted.value)),
        }
    }

    /// Go `checkDataForModifyColumn` for every public column a MODIFY COLUMN
    /// is checking writes against: the value must cast to the changing type
    /// under strict flags, and a VARCHAR -> CHAR change must not lose
    /// trailing spaces.
    pub(in crate::kv_table) fn check_data_for_modify_column(
        &self,
        row: &[Datum],
        ctx: Option<&crate::StmtContext>,
    ) -> Result<(), KvTableError> {
        for (offset, changing) in &self.changing_field_types {
            let Some(data) = row.get(*offset) else {
                continue;
            };
            let value = match ctx {
                Some(ctx) => crate::driver::cast_table_value_with_flags(
                    data.clone(),
                    changing,
                    "",
                    ctx,
                    tidb_datatype::STRICT_FLAGS,
                    false,
                )
                .map_err(|error| KvTableError::ColumnCast(error.to_mysql_error()))?,
                None => match data.convert_to(changing, tidb_datatype::STRICT_FLAGS) {
                    Ok(converted) if converted.event.is_none() => converted.value,
                    _ => {
                        return Err(KvTableError::ColumnCast(crate::MysqlError::new(
                            tidb_error::mysql::errcode::ErrUnknown,
                            "data truncation error during modify column",
                        )))
                    }
                },
            };
            if changing.code() == FieldTypeCode::String && datum_text(&value) != datum_text(data) {
                return Err(KvTableError::ColumnCast(crate::MysqlError::new(
                    tidb_error::mysql::errcode::ErrUnknown,
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
    pub(in crate::kv_table) fn insert_write_row(
        &self,
        row: &[Datum],
        ctx: Option<&crate::StmtContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        if self.nonpublic_columns.is_empty() {
            return Ok(None);
        }
        let mut write_row = row[..row.len().min(self.columns.len())].to_vec();
        write_row.resize(self.columns.len(), Datum::Null);
        for column in self.nonpublic_columns.iter() {
            let value = if !column.is_writable() {
                Datum::Null
            } else if let Some(dependency) = column.dependency {
                Self::cast_into_column(
                    row.get(dependency).cloned().unwrap_or(Datum::Null),
                    &column.column,
                    ctx,
                    flags,
                    zone,
                )?
            } else {
                column
                    .column
                    .origin_default_value(flags, zone)
                    .map_err(|error| KvTableError::Decode(error.to_string()))?
            };
            write_row.push(value);
        }
        Ok(Some(write_row))
    }

    /// What the stored record holds for each non-public column, as Go's
    /// scan of `WritableCols` / `DeletableCols` decodes it: the value when
    /// present, else a changing column's cast of its dependency and any
    /// other column's original default.
    fn stored_nonpublic_values(
        &mut self,
        handle: &TableHandle,
        old: &[Datum],
        ctx: Option<&crate::StmtContext>,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Vec<Datum>, KvTableError> {
        let types: BTreeMap<i64, FieldType> = self
            .nonpublic_columns
            .iter()
            .map(|column| (column.column.id, column.column.field_type.clone()))
            .collect();
        let stored = match self.stored_record(handle)? {
            Some((_, entry)) => {
                tidb_tablecodec::decode_table_row_to_map(&entry, &types, Some(zone))
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?
            }
            None => BTreeMap::new(),
        };
        let nonpublic = Arc::clone(&self.nonpublic_columns);
        nonpublic
            .iter()
            .map(|column| match stored.get(&column.column.id) {
                Some(value) => Ok(value.clone()),
                None => match column.dependency {
                    Some(dependency) => Self::cast_into_column(
                        old.get(dependency).cloned().unwrap_or(Datum::Null),
                        &column.column,
                        ctx,
                        flags,
                        zone,
                    ),
                    None => column
                        .column
                        .origin_default_value(flags, zone)
                        .map_err(|error| KvTableError::Decode(error.to_string())),
                },
            })
            .collect()
    }

    /// Go `updateRecord`'s rows: the old and new write rows of an UPDATE,
    /// `None` when the table has no non-public column. A writable column
    /// keeps what the row holds, except that a changing one casts its
    /// dependency's new value; a delete-only changing column's old slot is
    /// the cast of the dependency's old value, which its index entries were
    /// filed under.
    pub(in crate::kv_table) fn update_write_rows(
        &mut self,
        handle: &TableHandle,
        old: &[Datum],
        new: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<Option<(Vec<Datum>, Vec<Datum>)>, KvTableError> {
        if self.nonpublic_columns.is_empty() {
            return Ok(None);
        }
        let flags = ctx.write_conversion_flags();
        let zone = ctx.session_zone();
        let stored = self.stored_nonpublic_values(handle, old, Some(ctx), flags, &zone)?;
        let public = self.columns.len();
        let mut old_row = old[..old.len().min(public)].to_vec();
        old_row.resize(public, Datum::Null);
        let mut new_row = new[..new.len().min(public)].to_vec();
        new_row.resize(public, Datum::Null);
        let nonpublic = Arc::clone(&self.nonpublic_columns);
        for (column, stored) in nonpublic.iter().zip(stored) {
            let (old_value, new_value) = match (column.is_writable(), column.dependency) {
                (false, Some(dependency)) => {
                    let value = Self::cast_into_column(
                        old.get(dependency).cloned().unwrap_or(Datum::Null),
                        &column.column,
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
                        &column.column,
                        Some(ctx),
                        flags,
                        &zone,
                    )?,
                ),
                (true, None) => (stored.clone(), stored),
            };
            old_row.push(old_value);
            new_row.push(new_value);
        }
        Ok(Some((old_row, new_row)))
    }

    /// The row `RemoveRecord` removes: the public row extended with what the
    /// record holds for each non-public column (Go `DeletableCols`).
    pub(in crate::kv_table) fn delete_write_row(
        &mut self,
        handle: &TableHandle,
        old: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        if self.nonpublic_columns.is_empty() {
            return Ok(None);
        }
        let zone = ctx.session_zone();
        let stored = self.stored_nonpublic_values(
            handle,
            old,
            Some(ctx),
            ctx.write_conversion_flags(),
            &zone,
        )?;
        let public = self.columns.len();
        let mut row = old[..old.len().min(public)].to_vec();
        row.resize(public, Datum::Null);
        row.extend(stored);
        Ok(Some(row))
    }
}

/// A value's text for the VARCHAR -> CHAR comparison (Go `GetString`).
fn datum_text(value: &Datum) -> Vec<u8> {
    match value {
        Datum::String(text) => text.bytes().to_vec(),
        Datum::Bytes(bytes) => bytes.clone(),
        other => other.to_bytes().unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn column(name: &str, id: i64, code: FieldTypeCode, origin: Option<Datum>) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(code),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: origin,
            comment: String::new(),
            generated: None,
        }
    }

    fn stored(table: &mut KvTable, handle: &TableHandle) -> BTreeMap<i64, Datum> {
        let (_, entry) = table.stored_record(handle).unwrap().unwrap();
        let types = [1_i64, 2, 3, 4]
            .into_iter()
            .map(|id| (id, FieldType::new(FieldTypeCode::LongLong)))
            .collect();
        tidb_tablecodec::decode_table_row_to_map(&entry, &types, None).unwrap()
    }

    /// Go `AddRecord` / `updateRecord` / `RemoveRecord` over `WritableCols`:
    /// an INSERT writes an added column's original default and a changing
    /// column's cast of its dependency; an UPDATE keeps what the row holds
    /// for the first and recasts the second; a delete-only column gets no
    /// value.
    #[test]
    fn writes_carry_nonpublic_columns_like_go() {
        let mut table = KvTable::new(
            42,
            vec![
                column("a", 1, FieldTypeCode::LongLong, None),
                column("b", 2, FieldTypeCode::LongLong, None),
            ],
        );
        table.add_nonpublic_column(NonPublicColumn {
            column: column("c", 3, FieldTypeCode::LongLong, Some(Datum::Int(7))),
            state: tidb_model::SchemaState::WRITE_ONLY,
            dependency: None,
        });
        table.add_nonpublic_column(NonPublicColumn {
            column: column("_Col$_b_0", 4, FieldTypeCode::LongLong, None),
            state: tidb_model::SchemaState::WRITE_REORGANIZATION,
            dependency: Some(1),
        });
        let ctx = crate::StmtContext::for_dml(false, false, false);
        let handle = table
            .insert_row_with_row_id_checked(&[Datum::Int(1), Datum::Int(5)], None, 0, &ctx, false)
            .unwrap();
        let row = stored(&mut table, &handle);
        assert_eq!(row.get(&3), Some(&Datum::Int(7)));
        assert_eq!(row.get(&4), Some(&Datum::Int(5)));

        table
            .update_row_with_old_context(
                &handle,
                Some(&[Datum::Int(1), Datum::Int(5)]),
                &[Datum::Int(1), Datum::Int(9)],
                &ctx,
            )
            .unwrap();
        let row = stored(&mut table, &handle);
        assert_eq!(row.get(&2), Some(&Datum::Int(9)));
        assert_eq!(row.get(&3), Some(&Datum::Int(7)));
        assert_eq!(row.get(&4), Some(&Datum::Int(9)));

        // Once delete-only, a column is no longer written.
        let mut later = table.clone_for_test_nonpublic();
        Arc::make_mut(&mut later.nonpublic_columns)[0].state = tidb_model::SchemaState::DELETE_ONLY;
        let handle = later
            .insert_row_with_row_id_checked(
                &[Datum::Int(2), Datum::Int(3)],
                Some(2),
                0,
                &ctx,
                false,
            )
            .unwrap();
        let row = stored(&mut later, &handle);
        assert_eq!(row.get(&3), None);
        assert_eq!(row.get(&4), Some(&Datum::Int(3)));
    }
}

#[cfg(test)]
impl KvTable {
    /// A copy sharing this table's storage, for state changes in tests.
    fn clone_for_test_nonpublic(&self) -> KvTable {
        let mut copy =
            KvTable::with_storage(self.table_id, self.columns.to_vec(), self.store.clone_box());
        copy.nonpublic_columns = Arc::clone(&self.nonpublic_columns);
        copy
    }
}
