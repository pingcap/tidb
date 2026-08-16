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

//! Go `br/pkg/restore/utils/common.go`: the one value type shared across the
//! restore pipeline.

use tidb_model::TableInfo;

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
}

impl<Old> Default for CreatedTable<Old> {
    fn default() -> Self {
        Self {
            rewrite_rule: None,
            table: None,
            old_table: None,
        }
    }
}
