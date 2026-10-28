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
#[derive(Debug, Default)]
pub struct SequenceSnapshot {
    databases: Arc<HashMap<String, Arc<Database>>>,
    /// The session's LOCAL temporary tables, which Go's
    /// `GetSequenceByName(ctx.GetInfoSchema(), ...)` finds first.
    local_temporary: Vec<(CatalogTableKey, Arc<TableEntry>)>,
    current_db: String,
    /// Go SessionVars.SequenceState is keyed by sequence ID, not its name.
    pub last_values: Arc<std::sync::Mutex<HashMap<i64, i64>>>,
}

impl SequenceSnapshot {
    /// Pins the catalog's copy-on-write schema map and shares the session's
    /// LASTVAL state. Allocator handles remain shared with the catalog.
    #[must_use]
    pub fn new(
        catalog: &Catalog,
        current_db: &str,
        last_values: Arc<std::sync::Mutex<HashMap<i64, i64>>>,
    ) -> Self {
        Self {
            databases: Arc::clone(&catalog.databases),
            local_temporary: catalog.local_temporary.clone(),
            current_db: current_db.go_to_lower(),
            last_values,
        }
    }

    /// Go `getSchemaAndSequence` with the `CurrentDB` default: the schema and
    /// sequence names as written.
    pub fn schema_and_name<'a>(&'a self, path: &'a [String]) -> Option<(&'a str, &'a str)> {
        match path {
            [name] => Some((self.current_db.as_str(), name.as_str())),
            [database, name] => Some((database.as_str(), name.as_str())),
            _ => None,
        }
    }

    /// The `(database, name)` Go's sequence errors name.
    pub fn key(&self, path: &[String]) -> (String, String) {
        match path {
            [name] => (self.current_db.to_string(), name.go_to_lower()),
            [database, name] => (database.go_to_lower(), name.go_to_lower()),
            [] => (String::new(), String::new()),
            [leading @ .., name] => (
                leading
                    .iter()
                    .map(|part| part.go_to_lower())
                    .collect::<Vec<_>>()
                    .join("."),
                name.go_to_lower(),
            ),
        }
    }

    /// Go GetSequenceByName: look up one table in the statement's schema,
    /// then distinguish an absent table (1146) from a wrong object (1347).
    pub fn resolve(&self, path: &[String]) -> Result<&SequenceDef, tidb_expr::EvalError> {
        let entry = self.schema_and_name(path).and_then(|(database, name)| {
            let key = CatalogTableKey::new(database, name);
            self.local_temporary
                .iter()
                .find(|(local, _)| *local == key)
                .map(|(_, entry)| entry)
                .or_else(|| self.databases.get(&key.database)?.tables.get(&key.table))
        });
        match entry.map(Arc::as_ref) {
            Some(TableEntry::Sequence(sequence)) => Ok(sequence),
            Some(_) => Err(tidb_expr::EvalError::Sequence({
                let (database, name) = self.key(path);
                tidb_expr::SequenceEvalError::WrongObject(database, name)
            })),
            None => Err(tidb_expr::EvalError::Sequence({
                let (database, name) = self.key(path);
                tidb_expr::SequenceEvalError::NotASequence(database, name)
            })),
        }
    }

    /// Resolves a sequence without advancing it or changing LASTVAL state.
    pub fn validate_path(&self, path: &[String]) -> Result<(), tidb_expr::EvalError> {
        self.resolve(path).map(|_| ())
    }
}
