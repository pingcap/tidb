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

use super::{KvTable, KvTableError};
use tidb_txnkv::Key;

const CACHE_TABLE_SIZE_LIMIT: usize = 64 * (1 << 20);

impl KvTable {
    /// Whether Go's persisted table-cache state is enabled.
    #[must_use]
    pub fn is_cached(&self) -> bool {
        self.cache_status == tidb_model::TableCacheStatusType::ENABLE
    }

    /// Whether the table is in any non-disabled cache state. Go constructs
    /// the cached-table wrapper and blocks ordinary DDL for both ENABLE and
    /// the transient SWITCHING state.
    #[must_use]
    pub fn is_cache_table(&self) -> bool {
        self.cache_status != tidb_model::TableCacheStatusType::DISABLE
    }

    /// Restores Go's persisted table-cache state while loading `TableInfo`.
    pub fn set_cache_status(&mut self, status: tidb_model::TableCacheStatusType) {
        self.cache_status = status;
    }

    /// Enables table caching after the two source admission checks.
    ///
    /// # Errors
    ///
    /// [`KvTableError::CacheTableUnsupported`] for a partitioned table or a
    /// table whose encoded keys and values exceed TiDB's 64 MiB limit, and
    /// [`KvTableError::Storage`] when the backend scan fails.
    pub fn enable_cache(&mut self) -> Result<(), KvTableError> {
        if self.is_cached() {
            return Ok(());
        }
        if self.partition.is_some() {
            return Err(KvTableError::CacheTableUnsupported("partition mode"));
        }
        if self.cache_storage_size_exceeds(CACHE_TABLE_SIZE_LIMIT)? {
            return Err(KvTableError::CacheTableUnsupported("table too large"));
        }
        self.cache_status = tidb_model::TableCacheStatusType::ENABLE;
        Ok(())
    }

    /// Disables table caching. Repeating NOCACHE is a no-op in Go.
    pub fn disable_cache(&mut self) {
        self.cache_status = tidb_model::TableCacheStatusType::DISABLE;
    }

    fn cache_storage_size_exceeds(&mut self, limit: usize) -> Result<bool, KvTableError> {
        let prefix = Key::from_bytes(tidb_codec::table_key::gen_table_prefix(self.table_id));
        let upper_bound = prefix.prefix_next();
        let mut iterator = self
            .store
            .iter(Some(&prefix), Some(&upper_bound))
            .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
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
                return Err(KvTableError::Storage(format!("{error:?}")));
            }
        }
        iterator.close();
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_table::KvColumn;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn cache_admission_measures_the_encoded_table_image() {
        let column = KvColumn {
            name: "id".to_owned(),
            id: 1,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            generated: None,
        };
        let mut table = KvTable::new(42, vec![column]);
        assert!(!table.cache_storage_size_exceeds(0).unwrap());
        table
            .insert_row(&[Datum::Int(1)], &tidb_expr::NoColumns)
            .unwrap();
        assert!(table.cache_storage_size_exceeds(0).unwrap());
    }
}
