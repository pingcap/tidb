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

//! Go `SequenceOperatorProvider`'s schema half: a statement's pinned view of
//! the sequences and the session's LASTVAL state.

use crate::*;

/// A statement's infoschema view for sequence lookup and session LASTVAL state.
/// Sharing the schema map is constant-time; names are resolved only when a
/// sequence expression is used, like Go's SequenceOperatorProvider.
/// The statement's sequences: the names Go's `sequence.GetSequenceByName`
/// resolves against the statement's infoschema.
#[derive(Debug, Default)]
pub struct SequenceSnapshot {
    databases: Arc<HashMap<String, Arc<Database>>>,
    local_temporary: Vec<(CatalogTableKey, Arc<TableEntry>)>,
    current_db: String,
}

impl SequenceSnapshot {
    #[must_use]
    pub fn new(catalog: &Catalog, current_db: &str) -> Self {
        Self {
            databases: Arc::clone(&catalog.schema.databases),
            local_temporary: catalog.local_temporary.clone(),
            current_db: current_db.go_to_lower(),
        }
    }
}

impl tidb_util::misc::SequenceSchema for SequenceSnapshot {
    fn current_db(&self) -> &str {
        &self.current_db
    }

    fn object(&self, database: &str, name: &str) -> Option<Option<&dyn tidb_util::misc::SequenceTable>> {
        let key = CatalogTableKey::new(database, name);
        let entry = self
            .local_temporary
            .iter()
            .find(|(local, _)| *local == key)
            .map(|(_, entry)| entry)
            .or_else(|| self.databases.get(&key.database)?.tables.get(&key.table))?;
        Some(match entry.as_ref() {
            TableEntry::Sequence(sequence) => Some(sequence as &dyn tidb_util::misc::SequenceTable),
            _ => None,
        })
    }
}
