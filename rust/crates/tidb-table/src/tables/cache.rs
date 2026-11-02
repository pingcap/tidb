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

use super::{TableCommon, KvTableError};
use tidb_txnkv::Key;

const CACHE_TABLE_SIZE_LIMIT: usize = 64 * (1 << 20);

impl TableCommon {
    /// Whether Go's persisted table-cache state is enabled.
    #[must_use]
    pub fn is_cached(&self) -> bool {
        self.cache_status() == tidb_model::TableCacheStatusType::ENABLE
    }

    /// Whether the table is in any non-disabled cache state. Go constructs
    /// the cached-table wrapper and blocks ordinary DDL for both ENABLE and
    /// the transient SWITCHING state.
    #[must_use]
    pub fn is_cache_table(&self) -> bool {
        self.cache_status() != tidb_model::TableCacheStatusType::DISABLE
    }

    /// Go `TableInfo.Lock`.
    #[must_use]
    pub fn lock(&self) -> Option<tidb_model::TableLockInfo> {
        self.meta.read().lock.as_ref().map(|lock| lock.read().clone())
    }

    /// Go `TableInfo.IsLocked`: a lock some session still holds.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.meta
            .read()
            .lock
            .as_ref()
            .is_some_and(|lock| !lock.read().sessions.is_empty())
    }



    /// Enables table caching after the two source admission checks.
    ///
    /// # Errors
    ///
    /// `ErrOptOnCacheTable` for a partitioned table or a
    /// table whose encoded keys and values exceed TiDB's 64 MiB limit, and
    /// [`KvTableError::Storage`] when the backend scan fails.
    pub fn enable_cache(&mut self, store: &mut dyn tidb_kv::storage::TableStorage) -> Result<(), KvTableError> {
        if self.is_cached() {
            return Ok(());
        }
        if self.partition.is_some() {
            return Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                        .generate_with_stack_by_args(&["partition mode".into()]),
                ),
            ));
        }
        if self.cache_storage_size_exceeds(store, CACHE_TABLE_SIZE_LIMIT)? {
            return Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                        .generate_with_stack_by_args(&["table too large".into()]),
                ),
            ));
        }
        self.update_meta(|meta| {
            meta.table_cache_status_type = tidb_model::TableCacheStatusType::ENABLE;
        });
        Ok(())
    }

    /// Go `checkCacheTableSize`: the table's keys and values in `store`
    /// against `limit`.
    fn cache_storage_size_exceeds(
        &self,
        store: &mut dyn tidb_kv::storage::TableStorage,
        limit: usize,
    ) -> Result<bool, KvTableError> {
        let prefix = Key::from_bytes(tidb_tablecodec::table_key::gen_table_prefix(self.table_id));
        let upper_bound = prefix.prefix_next();
        let mut iterator = store
            .iter(Some(&prefix), Some(&upper_bound))
            .map_err(KvTableError::from)?;
        let mut size = 0usize;
        while iterator.valid() {
            size = size
                .saturating_add(iterator.key().as_bytes().len())
                .saturating_add(iterator.value().len());
            if size > limit {
                iterator.close();
                return Ok(true);
            }
            if let Err(error) = iterator.next() {
                iterator.close();
                return Err(KvTableError::from(error));
            }
        }
        iterator.close();
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn cache_admission_measures_the_encoded_table_image() {
        let column = crate::column::Column {
            info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("id"),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            }),
            generated: None,
            default_value: None,
            origin_default: None,
        };
        let mut table = TableCommon::new(42, vec![column]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        assert!(!table.cache_storage_size_exceeds(&mut store, 0).unwrap());
        table
            .insert_row(&mut store, &[Datum::Int(1)], &tidb_exprctx::NoColumns)
            .unwrap();
        assert!(table.cache_storage_size_exceeds(&mut store, 0).unwrap());
    }
}
