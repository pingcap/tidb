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

//! Go `br/pkg/restore/utils/common.go`: the value types shared across the
//! restore pipeline.

use tidb_ast::CiString;
use tidb_model::{DBInfo, TableInfo};

use super::rewrite_rule::RewriteRules;

/// Go `utils.CreatedTable`: a table created on the restore path but not yet
/// filled with data.
///
/// boundary: Go's `OldTable *metautil.Table` is the schema snapshot read back
/// out of the backup metafile. Nothing in this package looks inside it, so it
/// stays an opaque payload rather than dragging `br/pkg/metautil` across.
#[derive(Clone, Debug)]
pub struct CreatedTable<Old> {
    /// Go `CreatedTable.RewriteRule`.
    pub rewrite_rule: Option<RewriteRules>,
    /// Go `CreatedTable.Table`.
    pub table: Option<TableInfo>,
    /// Go `CreatedTable.OldTable`.
    pub old_table: Option<Old>,
    /// Go `CreatedTable.TargetDB`: the downstream database name.
    pub target_db: CiString,
}

impl<Old> Default for CreatedTable<Old> {
    fn default() -> Self {
        Self {
            rewrite_rule: None,
            table: None,
            old_table: None,
            target_db: CiString::default(),
        }
    }
}

impl CreatedTable<crate::metautil::Table> {
    /// Go `TargetDBName`: the downstream database name, falling back to the
    /// source database for an identity route.
    #[must_use]
    pub fn target_db_name(&self) -> CiString {
        if !self.target_db.original().is_empty() {
            return self.target_db.clone();
        }
        self.old_table
            .as_ref()
            .map(|t| t.db.name.clone())
            .unwrap_or_default()
    }
}

/// Go `DatabaseRestorePlan`: the immutable source database and the target
/// it is created as.
#[derive(Clone)]
pub struct DatabaseRestorePlan {
    /// Go `Source`.
    pub source: crate::metautil::Database,
    /// Go `Target`.
    pub target: DBInfo,
    /// Go `Reused`: the target already existed downstream when the snapshot
    /// phase created it.
    pub reused: bool,
}

/// Go `TableRestorePlan`: the immutable source table and its target
/// identity. The source owns the backup IDs, files, checksums and
/// statistics; the target clones are what the DDL creates.
#[derive(Clone)]
pub struct TableRestorePlan {
    /// Go `Source`.
    pub source: crate::metautil::Table,
    /// Go `TargetDB`.
    pub target_db: DBInfo,
    /// Go `TargetInfo`.
    pub target_info: TableInfo,
}

impl TableRestorePlan {
    /// Go `TargetTable`: the target in the DDL layer's shape, without the
    /// source-only files and statistics.
    #[must_use]
    pub fn target_table(&self) -> crate::metautil::Table {
        crate::metautil::Table::with_schema(self.target_db.clone(), self.target_info.clone())
    }
}

/// Go `DatabaseSettingsCompatible`: two source schemas merge into one target
/// without losing database-level settings.
#[must_use]
pub fn database_settings_compatible(first: &DBInfo, second: &DBInfo) -> bool {
    if !first.charset.eq_ignore_ascii_case(&second.charset)
        || !first.collate.eq_ignore_ascii_case(&second.collate)
    {
        return false;
    }
    match (&first.placement_policy_ref, &second.placement_policy_ref) {
        (Some(a), Some(b)) => a.read().name.lowercase() == b.read().name.lowercase(),
        (a, b) => a.is_none() && b.is_none(),
    }
}

/// Go `SourceDBPrecedes`: the deterministic winner when source schemas
/// merge into one target.
#[must_use]
pub fn source_db_precedes(candidate: &DBInfo, existing: &DBInfo) -> bool {
    if candidate.name.lowercase() != existing.name.lowercase() {
        return candidate.name.lowercase() < existing.name.lowercase();
    }
    candidate.id < existing.id
}
