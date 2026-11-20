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

//! Go master `pkg/util/mviewutil` (`util.go`): the checks and key-layout
//! lookups the planner and the DDL share for materialized views.

pub mod refresh_source;

use tidb_ast::{JoinNode, QueryStmt};
use tidb_model::{SchemaState, TableInfo};
use tidb_terror::MysqlError;

/// Go `RefreshAdvisoryLockName`: the lock shared by a materialized view's
/// refresh and the DDL that must exclude an in-flight refresh of it.
#[must_use]
pub fn refresh_advisory_lock_name(schema_id: i64, mview_id: i64) -> String {
    format!("mv_refresh_{schema_id}_{mview_id}")
}

/// Go master `ShadowTablePrefix`: an out-of-place refresh's shadow tables are
/// `__mv_shadow_<mviewID>_<UnixNano>`.
pub const SHADOW_TABLE_PREFIX: &str = "__mv_shadow_";

/// Go master `PurgeAdvisoryLockName`: "identifies the lock shared by a log
/// purge and DDL retiring that physical log".
#[must_use]
pub fn purge_advisory_lock_name(mlog_id: i64) -> String {
    format!("mlog_purge_{mlog_id}")
}

fn unsupported(message: &str) -> MysqlError {
    MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.gen_with_stack(message))
}

/// Go `CheckMaterializedViewSelect`: the SELECT clauses a materialized view
/// does not support, checked before the planner builds the SELECT so they
/// are not reported as unrelated planner errors.
///
/// # Errors
///
/// A CTE, a locking clause, SELECT INTO, or a single table read AS OF or
/// TABLESAMPLE.
pub fn check_materialized_view_select(select: &QueryStmt) -> Result<(), MysqlError> {
    let QueryStmt::Select(select) = select else {
        return Ok(());
    };
    if select.with.is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW does not support common table expressions"));
    }
    if select.lock.is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW does not support locking clauses"));
    }
    if select.into_outfile.is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW does not support SELECT INTO"));
    }
    let Some(from) = &select.from else {
        return Ok(());
    };
    if from.right.is_some() {
        return Ok(());
    }
    let JoinNode::Table(table) = &from.left else {
        return Ok(());
    };
    if table.as_of.is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW does not support AS OF"));
    }
    if table.sample.is_some() {
        return Err(unsupported("CREATE MATERIALIZED VIEW does not support TABLESAMPLE"));
    }
    Ok(())
}

/// Go `FindVisibleIndexWithPrefixCoveringColumns`: the first public visible
/// key layout a MIN/MAX refresh can use.
#[must_use]
pub fn find_visible_index_with_prefix_covering_columns(base: &TableInfo, group_by_cols: &[String]) -> Option<String> {
    find_indexes_with_prefix_covering_columns(base, group_by_cols, "", true).into_iter().next()
}

/// Go `FindVisibleIndexesWithPrefixCoveringColumns`: every public visible key
/// layout a MIN/MAX refresh can use.
#[must_use]
pub fn find_visible_indexes_with_prefix_covering_columns(base: &TableInfo, group_by_cols: &[String]) -> Vec<String> {
    find_indexes_with_prefix_covering_columns(base, group_by_cols, "", true)
}

/// Go `HasIndexWithPrefixCoveringColumns`: whether the table has a key layout
/// whose leading columns cover every group-by column without a prefix length,
/// leaving out `excluded_index_name` (a DDL check evaluating the table as it
/// will be without that index).
#[must_use]
pub fn has_index_with_prefix_covering_columns(
    base: &TableInfo,
    group_by_cols: &[String],
    excluded_index_name: &str,
    require_visible_public: bool,
) -> bool {
    !find_indexes_with_prefix_covering_columns(base, group_by_cols, excluded_index_name, require_visible_public)
        .is_empty()
}

fn find_indexes_with_prefix_covering_columns(
    base: &TableInfo,
    group_by_cols: &[String],
    excluded_index_name: &str,
    require_visible_public: bool,
) -> Vec<String> {
    let prefix_len = group_by_cols.len();
    if prefix_len == 0 {
        return Vec::new();
    }
    let group_by: Vec<String> = group_by_cols.iter().map(|col| tidb_hack::go_to_lower(col)).collect();
    let excluded = tidb_hack::go_to_lower(excluded_index_name);
    let mut names = Vec::new();
    if base.pk_is_handle && prefix_len == 1 && excluded != tidb_hack::go_to_lower(tidb_mysql::consts::PrimaryKeyName) {
        if let Some(pk) = base.get_pk_col_info() {
            if group_by.iter().any(|col| col == pk.read().name.lowercase()) {
                names.push(tidb_mysql::consts::PrimaryKeyName.to_owned());
            }
        }
    }
    for index in base.indices.iter_deref() {
        let index = index.read();
        if index.columns.len() < prefix_len {
            continue;
        }
        if require_visible_public && (index.state != SchemaState::PUBLIC || index.invisible) {
            continue;
        }
        if !excluded.is_empty() && index.name.lowercase() == excluded {
            continue;
        }
        if index_prefix_covers_columns(&index, prefix_len, &group_by) {
            names.push(index.name.original().to_owned());
        }
    }
    names
}

fn index_prefix_covers_columns(index: &tidb_model::IndexInfo, prefix_len: usize, group_by: &[String]) -> bool {
    let mut matched: Vec<String> = Vec::with_capacity(prefix_len);
    for column in index.columns.iter_deref().take(prefix_len) {
        let column = column.read();
        if column.length > 0 {
            return false;
        }
        let name = column.name.lowercase().to_owned();
        if !group_by.contains(&name) || matched.contains(&name) {
            return false;
        }
        matched.push(name);
    }
    matched.len() == prefix_len
}
