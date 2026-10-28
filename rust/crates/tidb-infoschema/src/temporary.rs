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

//! A session's temporary tables over the shared schema image: Go's
//! `SessionExtendedInfoSchema` (LOCAL tables shadow the shared image for one
//! session) and the GLOBAL temporary tables whose rows live in the session.

use crate::*;

impl Catalog {
    /// Attaches a session's LOCAL temporary tables for the duration of one
    /// statement -- Go `temptable.AttachLocalTemporaryTableInfoSchema`, which
    /// wraps the infoschema in a `SessionExtendedInfoSchema` whose
    /// `LocalTemporaryTables` resolve first. Each entry is `(folded
    /// database, folded name, table)`.
    pub fn attach_local_temporary_tables(&mut self, tables: Vec<(String, String, KvTable)>) {
        // The version bump is gated on an ACTUAL visibility change: this runs
        // around EVERY statement via [`crate::txn`]'s overlay guard, and the
        // common case is a session with NO temporary tables, where attaching
        // inserts nothing. Bumping unconditionally moved the key-decode
        // metadata epoch twice per statement, which rebuilt the whole
        // TIDB_DECODE_KEY snapshot per statement (measured: ~60% of process
        // CPU in its allocation trail under sysbench point_select). Go's row-
        // decode caches hang off the INFOSCHEMA version, which a session
        // mounting its own temp tables never moves either.
        if tables.is_empty() {
            return;
        }
        let base = self.schema.metadata_version;
        self.bump_metadata_version();
        self.overlay_versions = Some((base, self.schema.metadata_version));
        self.local_temporary = tables
            .into_iter()
            .map(|(database, table, kv)| {
                (
                    CatalogTableKey { database, table },
                    std::sync::Arc::new(TableEntry::Kv(Arc::new(kv))),
                )
            })
            .collect();
    }

    /// Detaches the session's LOCAL temporary tables -- Go
    /// `temptable.DetachLocalTemporaryTableInfoSchema` -- returning them as
    /// the statement left them.
    pub fn take_local_temporary_tables(&mut self) -> Vec<(String, String, KvTable)> {
        // A statement that changed no schema leaves the image it found, so
        // detaching restores that image's version.
        let restored = self
            .overlay_versions
            .take()
            .and_then(|(base, attached)| (attached == self.schema.metadata_version).then_some(base));
        if self.local_temporary.is_empty() {
            return Vec::new();
        }
        self.bump_metadata_version();
        if let Some(base) = restored {
            self.schema.metadata_version = base;
        }
        std::mem::take(&mut self.local_temporary)
            .into_iter()
            .filter_map(|(key, entry)| match Arc::unwrap_or_clone(entry) {
                TableEntry::Kv(table) => Some((key.database, key.table, Arc::unwrap_or_clone(table))),
                _ => None,
            })
            .collect()
    }

    /// Every GLOBAL temporary table in the catalog, by physical id, for the
    /// session that has to swap its own rows in.
    ///
    /// A global temporary table's `TableInfo` is shared by every session (it
    /// is created by a real DDL job) while its ROWS are private to one and
    /// die with the transaction that wrote them -- Go
    /// `temptable.TemporaryTableSnapshotInterceptor`, whose `iterTable`
    /// answers an EMPTY iterator for `TempTableGlobal` so nothing outside the
    /// current transaction's own buffer is ever visible.
    pub fn global_temporary_table_ids(&self) -> Vec<(String, String)> {
        let mut ids = Vec::new();
        for (folded_database, schema) in self.schema.databases.iter() {
            for (folded_name, entry) in &schema.tables {
                if matches!(&**entry, TableEntry::Kv(table)
                    if table.temp_table_type() == tidb_model::TempTableType::GLOBAL)
                {
                    ids.push((folded_database.clone(), folded_name.clone()));
                }
            }
        }
        ids.sort();
        ids
    }

    /// [`Self::global_temporary_table_ids`], served from a memo: `txn`'s
    /// overlay guard asks this twice per statement, and the answer only
    /// moves when the table set does -- Go asks the same question of
    /// `temptable`'s own session map in O(1). Keyed on `metadata_version`,
    /// because exactly its mutators move the table set.
    pub fn global_temporary_table_ids_memo(&mut self) -> &[(String, String)] {
        let epoch = self.schema.metadata_version;
        if self
            .schema.temporary_sweep
            .as_ref()
            .is_none_or(|(at, _)| *at != epoch)
        {
            self.schema.temporary_sweep = Some((epoch, self.global_temporary_table_ids()));
        }
        match self.schema.temporary_sweep.as_ref() {
            Some((_, globals)) => globals.as_slice(),
            None => unreachable!("just stored"),
        }
    }

    /// A mutable handle on a table for the temporary-table overlay, which
    /// swaps row storage in and out without changing any schema.
    ///
    /// It deliberately does NOT bump [`Catalog::version`]: moving a session's
    /// own rows into and out of the slot they are read through is not a
    /// schema change, and counting it as one would abort every concurrent
    /// transaction whenever any session touched a temporary table.
    pub fn temporary_overlay_table_mut(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut KvTable> {
        // The row overlay keeps the schema epoch, but this public handle
        // can also change an ID. Never retain a directory across that borrow.
        self.invalidate_table_id_names();
        match self
            .database_mut(database)?
            .tables
            .get_mut(name)
            .map(std::sync::Arc::make_mut)
        {
            Some(TableEntry::Kv(table)) => Some(std::sync::Arc::make_mut(table)),
            _ => None,
        }
    }

    /// Resolves a durable DDL target without the session's local name
    /// overlay (Go `DetachLocalTemporaryTableInfoSchema`).
    pub fn persistent_table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.schema_entry(&CatalogTableKey::new(database, name))
            .map(|entry| &**entry)
    }
}
