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

//! Go's DDL work over one table that the in-process DDL and the DDL
//! worker share: MODIFY COLUMN's row conversion and data check, and the
//! reload of a table from the metadata a DDL wrote.

use super::*;

/// Go's DDL work over a [`KvTable`] (`modify_column.go`'s row conversion and check, the reload from metadata), above the table layer.
pub trait KvTableDdl {

    /// Creates an index over the existing rows, which Go backfills as part of
    /// the DDL. A unique index whose existing rows already collide is
    /// rejected with the duplicate it found, leaving the table unchanged.
    fn create_index_with_context(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<(), KvTableError>;

    fn create_index_in(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError>;

    fn backfill_index(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError>;

    /// Go `RecoverIndexExec`: files every row's missing entries under the
    /// index named `index_name`, partition by partition, and answers
    /// `(ADDED_COUNT, SCAN_COUNT)` -- the rows whose entries were written and
    /// the rows scanned (a row a partial index's condition excludes is
    /// scanned and skipped). A row whose every entry is already stored is
    /// left alone, a unique entry naming another row included (Go logs that
    /// broken constraint and skips it). Recovering a clustered PRIMARY KEY,
    /// whose key is the record itself, answers `(0, 0)`.
    ///
    /// `Ok(None)` is no writable index of that name.
    fn recover_index(
        &mut self,
        index_name: &str,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<Option<(i64, i64)>, KvTableError>;

    /// Go `checkModifyColumnData`: the first row, in handle order, whose old
    /// value Go's check query matches
    /// ([`crate::ddl::modify_column_type::modify_column_data_violation`]).
    /// `Ok(Some(..))` carries Go's verdict on that row -- 1138 for a NULL,
    /// 1265 naming the value otherwise -- and `Ok(None)` means every row
    /// passed or there was nothing to check.
    fn check_modify_column_data(
        &mut self,
        offset: usize,
        new_type: &tidb_datatype::FieldType,
        check_value_range: bool,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<Option<KvTableError>, KvTableError>;

}

impl KvTableDdl for KvTable {
    fn create_index_with_context(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<(), KvTableError> {
        self.create_index_in(index, ctx, &RowDecodeContext::for_ddl(ctx))
            .map(|_| ())
    }

    fn create_index_in(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError> {
        // Go's transaction owns backfill rollback. The nontransactional test
        // store needs one COW image for the entire backfill instead of repair
        // writes that can themselves fail or hide the original error.
        let original =
            (!self.store().has_external_statement_rollback()).then(|| self.store().clone_box());
        let result = self.backfill_index(index, ctx, decode_context);
        if result.is_err() {
            if let Some(original) = original {
                *self.store_mut() = original;
            }
        }
        result
    }

    fn backfill_index(
        &mut self,
        index: KvIndex,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError> {
        let zone = ctx.time_zone();
        if self
            .indexes()
            .iter()
            .any(|existing| existing.name.go_lower_eq(&index.name))
        {
            return Err(KvTableError::Mysql(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_DUP_KEY_NAME
                        .generate_with_stack_by_args(&[(index.name.clone()).into()]),
                ),
            ));
        }
        // Go's index worker uses a full RowDecoder map. Every generated
        // column is therefore recomputed under the DDL statement context
        // before an index entry is persisted, including STORED columns whose
        // old bytes may predate this backfill.
        let rows = self.scan_physical_rows_with_handles(decode_context)?;
        let mut added = 0;
        for (physical_id, handle, row) in &rows {
            let physical_id = *physical_id;
            if !self.index_condition_holds(&index, row, &zone)? {
                continue;
            }
            added += 1;
            let duplicate_value = duplicate_value_text(&self.index_values(&index, row));
            for entry in self.index_entry_keys(&index, row, handle, physical_id, &zone)? {
                let key = Key::from_bytes(entry.key);
                if entry.distinct {
                    self.check_insert_key(
                        &key,
                        &duplicate_value,
                        &self.qualified_key(&index.name),
                        false,
                    )?;
                }
                // The same entry value an INSERT writes, restored data and
                // all -- a backfill that stored a simpler one would leave the
                // index holding two different formats for the same table.
                let value = self.index_entry_value(
                    &index,
                    &entry.values,
                    row,
                    handle,
                    entry.distinct,
                    physical_id,
                    &zone,
                )?;
                self.store_mut().set(key, value).map_err(KvTableError::from)?;
            }
        }
        self.record_mv_key_part_source(&index);
        self.indexes_mut().push(index);
        Ok(added)
    }

    fn recover_index(
        &mut self,
        index_name: &str,
        ctx: &dyn tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<Option<(i64, i64)>, KvTableError> {
        let Some(index) = self
            .indexes()
            .iter()
            .find(|index| index.name.go_lower_eq(index_name))
            .cloned()
        else {
            return Ok(None);
        };
        if index.clustered_primary {
            return Ok(Some((0, 0)));
        }
        let zone = ctx.time_zone();
        let rows = self.scan_physical_rows_with_handles(decode_context)?;
        let (mut added, mut scanned) = (0_i64, 0_i64);
        for (physical_id, handle, row) in &rows {
            let physical_id = *physical_id;
            scanned += 1;
            if !self.index_condition_holds(&index, row, &zone)? {
                continue;
            }
            let entries = self.index_entry_keys(&index, row, handle, physical_id, &zone)?;
            // Go `batchMarkDup`: a row is skipped only when every key it
            // files is found.
            let mut all_found = true;
            for entry in &entries {
                match self.store_mut().get(&Key::from_bytes(entry.key.clone())) {
                    Ok(value) if !value.is_empty() => {}
                    Ok(_) | Err(StorageError::NotFound) => all_found = false,
                    Err(error) => return Err(error.into()),
                }
            }
            if all_found {
                continue;
            }
            // `index.Create(..., table.WithIgnoreAssertion,
            // table.DupKeyCheckSkip)`: every entry of the row, unchecked.
            for entry in entries {
                let value = self.index_entry_value(
                    &index,
                    &entry.values,
                    row,
                    handle,
                    entry.distinct,
                    physical_id,
                    &zone,
                )?;
                self.store_mut()
                    .set(Key::from_bytes(entry.key), value)
                    .map_err(KvTableError::from)?;
            }
            added += 1;
        }
        Ok(Some((added, scanned)))
    }

    fn check_modify_column_data(
        &mut self,
        offset: usize,
        new_type: &tidb_datatype::FieldType,
        check_value_range: bool,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<Option<KvTableError>, KvTableError> {
        let old_type = self.columns()[offset].field_type.clone();
        let column = tidb_hack::go_to_lower(&self.columns()[offset].name);
        if crate::ddl::modify_column_type::modify_column_data_violation(
            &old_type,
            new_type,
            check_value_range,
            &Datum::Null,
        )
        .is_none()
        {
            return Ok(None);
        }
        let mut cursor = self.row_cursor_projected_with_context(
            Some(&[offset]),
            None,
            &RowDecodeContext::for_query(ctx),
        )?;
        while let Some((_, row)) = cursor.next_row()? {
            let value = &row[0];
            if crate::ddl::modify_column_type::modify_column_data_violation(
                &old_type,
                new_type,
                check_value_range,
                value,
            ) != Some(true)
            {
                continue;
            }
            if value.is_null() {
                return Ok(Some(KvTableError::Mysql(
                    tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_INVALID_USE_OF_NULL
                            .generate_with_stack_by_args(&[]),
                    ),
                )));
            }
            let shown = value.sql_bytes().unwrap_or_default();
            return Ok(Some(KvTableError::Mysql(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::types::ERR_TRUNCATED.generate_with_stack(format!(
                        "Data truncated for column '{}', value is '{}'",
                        (column),
                        (String::from_utf8_lossy(&shown).into_owned())
                    )),
                ),
            )));
        }
        Ok(None)
    }

}
