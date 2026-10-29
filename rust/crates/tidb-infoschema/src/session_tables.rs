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

//! Go `infoschema.SessionTables` (`pkg/infoschema/sessiontables.go`): a
//! session's LOCAL temporary tables, keyed by the lowercased schema and
//! table names (`schemaMap[schema.L].tables[table.L]`). A table keeps the
//! name it was created with (`tbl.Meta().Name.O`).

use tidb_hack::go_to_lower;
use tidb_table::kv_table::KvTable;

/// The session's LOCAL temporary tables: `(schema.L, table.L, table)`.
pub type SessionTables = Vec<(String, String, KvTable)>;

/// Where `schema.name` is in `tables`; the lookup lowercases both names.
#[must_use]
pub fn position(tables: &[(String, String, KvTable)], schema: &str, name: &str) -> Option<usize> {
    let (schema, name) = (go_to_lower(schema), go_to_lower(name));
    tables
        .iter()
        .position(|(db, table, _)| *db == schema && *table == name)
}

/// Go `SessionTables.TableByName`.
#[must_use]
pub fn table_by_name<'a>(
    tables: &'a [(String, String, KvTable)],
    schema: &str,
    name: &str,
) -> Option<&'a KvTable> {
    position(tables, schema, name).map(|index| &tables[index].2)
}
