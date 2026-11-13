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

/// Go's DDL work over a [`TableCommon`] (`modify_column.go`'s row conversion and check, the reload from metadata), above the table layer.
pub trait KvTableDdl {

    /// Creates an index over the existing rows, which Go backfills as part of
    /// the DDL. A unique index whose existing rows already collide is
    /// rejected with the duplicate it found, leaving the table unchanged.
    fn create_index_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError>;

    fn create_index_in(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_exprctx::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError>;

    fn backfill_index(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_exprctx::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError>;


}

impl KvTableDdl for TableCommon {
    fn create_index_with_context(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        self.create_index_in(store, index, ctx, &RowDecodeContext::for_ddl(ctx))
            .map(|_| ())
    }

    fn create_index_in(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_exprctx::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError> {
        // Go's transaction owns backfill rollback: the backfill's writes sit
        // in one stage that a failure cleans up.
        let stage = store.staging();
        let result = self.backfill_index(store, index, ctx, decode_context);
        if result.is_err() {
            store.cleanup(stage);
        } else {
            store.release(stage);
        }
        result
    }

    fn backfill_index(
        &mut self,
        store: &mut dyn TableStorage,
        index: Index,
        ctx: &dyn tidb_exprctx::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<usize, KvTableError> {
        let zone = ctx.time_zone();
        if self
            .indexes()
            .iter()
            .any(|existing| existing.read().name.original().go_lower_eq(index.read().name.original()))
        {
            return Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_DUP_KEY_NAME
                        .gen_with_stack_by_args(&[(index.read().name.original().to_owned()).into()]),
                ),
            ));
        }
        // Go's index worker uses a full RowDecoder map. Every generated
        // column is therefore recomputed under the DDL statement context
        // before an index entry is persisted, including STORED columns whose
        // old bytes may predate this backfill.
        let rows = self.scan_physical_rows_with_handles(store, decode_context)?;
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
                    let duplicate_key = self.qualified_key(index.read().name.original());
                    self.check_insert_key(store, &key, &duplicate_value, &duplicate_key, false)?;
                }
                // The same entry value an INSERT writes, restored data and
                // all -- a backfill that stored a simpler one would leave the
                // index holding two different formats for the same table.
                let value = self.gen_index_value(
                    &index,
                    &entry.values,
                    row,
                    handle,
                    entry.distinct,
                    physical_id,
                    &zone,
                )?;
                store.set(key, value).map_err(KvTableError::from)?;
            }
        }
        self.add_index(index, false);
        Ok(added)
    }


}
