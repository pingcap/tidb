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

//! `pkg/util/mviewutil`: SELECT-shape checks and index-layout lookups shared
//! by materialized-view DDL and refresh (Go master `94a9cbedab`).
//!
//! Go's `CheckMaterializedViewSelect` accepts an `ast.ResultSetNode` and
//! returns `nil` for anything that is not a `*ast.SelectStmt`; the Rust
//! carrier's `CREATE MATERIALIZED VIEW` statement stores the query as a
//! [`tidb_ast::QueryStmt`], so the same assertion is the `Select` arm of that
//! enum.

use std::collections::HashSet;

use tidb_ast::QueryStmt;
use tidb_ast::{JoinNode, SelectStmt, TableRef};
use tidb_error::terror::TerrorError;
use tidb_model::index::IndexInfo;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;

use crate::dbterror::ERR_GENERAL_UNSUPPORTED_DDL;

/// Go `CheckMaterializedViewSelect`: checks the SELECT clauses that are not
/// supported by materialized views. It is called before the planner builds
/// the SELECT so unsupported clauses do not get reported as unrelated
/// planner errors. A query that is not a plain `SELECT` returns `Ok`,
/// exactly as Go's `*ast.SelectStmt` assertion falls through.
pub fn check_materialized_view_select(query: &QueryStmt) -> Result<(), TerrorError> {
    // Go: `sel, ok := selectNode.(*ast.SelectStmt); if !ok { return nil }`.
    let sel: &SelectStmt = match query {
        QueryStmt::Select(sel) => sel,
        QueryStmt::SetOpr(_) => return Ok(()),
    };
    if sel.with.is_some() {
        return Err(ERR_GENERAL_UNSUPPORTED_DDL.fast_generate(
            "Unsupported %s",
            &[tidb_error::mysql::FormatArg::from(
                "CREATE MATERIALIZED VIEW does not support common table expressions",
            )],
        ));
    }
    if let Some(lock) = &sel.lock {
        // Go: `sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone`.
        // The Rust carrier never stores an all-none lock: a parsed lock is
        // always `FOR UPDATE` or `FOR SHARE`.
        let _ = lock;
        return Err(ERR_GENERAL_UNSUPPORTED_DDL.fast_generate(
            "Unsupported %s",
            &[tidb_error::mysql::FormatArg::from(
                "CREATE MATERIALIZED VIEW does not support locking clauses",
            )],
        ));
    }
    if sel.into_outfile.is_some() {
        // Go checks `sel.SelectIntoOpt != nil`, covering OUTFILE, DUMPFILE
        // and variable forms alike.
        return Err(ERR_GENERAL_UNSUPPORTED_DDL.fast_generate(
            "Unsupported %s",
            &[tidb_error::mysql::FormatArg::from(
                "CREATE MATERIALIZED VIEW does not support SELECT INTO",
            )],
        ));
    }
    let join = match &sel.from {
        Some(join) => join,
        None => return Ok(()),
    };
    // Go: `sel.From.TableRefs == nil || sel.From.TableRefs.Right != nil`.
    if join.right.is_some() {
        return Ok(());
    }
    // Go: `ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)`.
    let table_ref: &TableRef = match &join.left {
        JoinNode::Table(table_ref) => table_ref,
        _ => return Ok(()),
    };
    if table_ref.as_of.is_some() {
        return Err(ERR_GENERAL_UNSUPPORTED_DDL.fast_generate(
            "Unsupported %s",
            &[tidb_error::mysql::FormatArg::from(
                "CREATE MATERIALIZED VIEW does not support AS OF",
            )],
        ));
    }
    if table_ref.sample.is_some() {
        return Err(ERR_GENERAL_UNSUPPORTED_DDL.fast_generate(
            "Unsupported %s",
            &[tidb_error::mysql::FormatArg::from(
                "CREATE MATERIALIZED VIEW does not support TABLESAMPLE",
            )],
        ));
    }
    Ok(())
}

/// Go `FindVisibleIndexWithPrefixCoveringColumns`: returns the first public
/// visible key layout usable by MIN/MAX materialized-view refresh.
#[must_use]
pub fn find_visible_index_with_prefix_covering_columns(
    base_table_info: Option<&TableInfo>,
    group_by_cols: &[String],
) -> Option<String> {
    find_indexes_with_prefix_covering_columns(base_table_info, group_by_cols, "", true)
        .into_iter()
        .next()
}

/// Go `FindVisibleIndexesWithPrefixCoveringColumns`: returns all public
/// visible key layouts usable by MIN/MAX materialized-view refresh.
#[must_use]
pub fn find_visible_indexes_with_prefix_covering_columns(
    base_table_info: Option<&TableInfo>,
    group_by_cols: &[String],
) -> Vec<String> {
    find_indexes_with_prefix_covering_columns(base_table_info, group_by_cols, "", true)
}

/// Go `HasIndexWithPrefixCoveringColumns`: reports whether the table has a
/// key layout whose leading columns cover all group-by columns without
/// prefix length. `excluded_index_name` is used by DDL checks to evaluate a
/// post-DDL table shape where that index should not be considered.
#[must_use]
pub fn has_index_with_prefix_covering_columns(
    base_table_info: Option<&TableInfo>,
    group_by_cols: &[String],
    excluded_index_name: &str,
    require_visible_public: bool,
) -> bool {
    !find_indexes_with_prefix_covering_columns(
        base_table_info,
        group_by_cols,
        excluded_index_name,
        require_visible_public,
    )
    .is_empty()
}

fn find_indexes_with_prefix_covering_columns(
    base_table_info: Option<&TableInfo>,
    group_by_cols: &[String],
    excluded_index_name: &str,
    require_visible_public: bool,
) -> Vec<String> {
    // Go: `if baseTableInfo == nil { return nil }`.
    let base_table_info = match base_table_info {
        Some(table) => table,
        None => return Vec::new(),
    };
    let prefix_len = group_by_cols.len();
    if prefix_len == 0 {
        return Vec::new();
    }
    let group_by_set: HashSet<String> = group_by_cols
        .iter()
        .map(|col| col.to_ascii_lowercase())
        .collect();

    let mut index_names: Vec<String> = Vec::with_capacity(base_table_info.indices.len());
    let excluded_index_name = excluded_index_name.to_ascii_lowercase();
    if base_table_info.pk_is_handle
        && prefix_len == 1
        && excluded_index_name != tidb_mysql::consts::PrimaryKeyName.to_ascii_lowercase()
    {
        if let Some(pk_col) = base_table_info.get_pk_col_info() {
            if group_by_set.contains(pk_col.read().name.lowercase()) {
                index_names.push(tidb_mysql::consts::PrimaryKeyName.to_owned());
            }
        }
    }

    for handle in base_table_info.indices.iter_handles() {
        // Go: `if idx == nil || len(idx.Columns) < prefixLen { continue }`.
        let Some(index) = handle else {
            continue;
        };
        let index = index.read();
        if index.columns.len() < prefix_len {
            continue;
        }
        if require_visible_public && (index.state != SchemaState::PUBLIC || index.invisible) {
            continue;
        }
        if !excluded_index_name.is_empty() && index.name.lowercase() == excluded_index_name {
            continue;
        }
        if index_prefix_covers_columns(&index, prefix_len, &group_by_set) {
            index_names.push(index.name.original().to_owned());
        }
    }
    index_names
}

fn index_prefix_covers_columns(
    index: &IndexInfo,
    prefix_len: usize,
    group_by_set: &HashSet<String>,
) -> bool {
    let mut matched: HashSet<String> = HashSet::with_capacity(prefix_len);
    for position in 0..prefix_len {
        let idx_col = index.columns.get(position).expect("length checked above");
        let idx_col = idx_col.read();
        if idx_col.length > 0 {
            return false;
        }
        let name = idx_col.name.lowercase().to_owned();
        if !group_by_set.contains(&name) {
            return false;
        }
        if !matched.insert(name) {
            return false;
        }
    }
    matched.len() == prefix_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeFlags;
    use tidb_model::{ColumnInfo, GoShared, GoSharedPointerSlice, IndexColumn};

    fn select_query(sql: &str) -> QueryStmt {
        match tidb_parser::parse(sql).expect("parse fixture") {
            tidb_ast::Stmt::Query(query) => (*query).clone(),
            other => panic!("expected a query statement, got {other:?}"),
        }
    }

    /// Every refusal branch carries Go's `ErrGeneralUnsupportedDDL` identity
    /// (8200, `Unsupported %s`) with Go's exact detail text.
    fn assert_unsupported(error: &TerrorError, detail: &str) {
        assert_eq!(error.code().value(), 8200);
        assert_eq!(
            error.message(),
            format!("Unsupported CREATE MATERIALIZED VIEW does not support {detail}")
        );
    }

    #[test]
    fn check_select_refuses_unsupported_clauses() {
        // WITH.
        assert_unsupported(
            &check_materialized_view_select(&select_query(
                "WITH cte AS (SELECT 1) SELECT * FROM t",
            ))
            .expect_err("WITH refused"),
            "common table expressions",
        );

        // Locking clause.
        assert_unsupported(
            &check_materialized_view_select(&select_query("SELECT * FROM t FOR UPDATE"))
                .expect_err("lock refused"),
            "locking clauses",
        );
        assert_unsupported(
            &check_materialized_view_select(&select_query("SELECT * FROM t LOCK IN SHARE MODE"))
                .expect_err("share lock refused"),
            "locking clauses",
        );

        // SELECT INTO.
        assert_unsupported(
            &check_materialized_view_select(&select_query("SELECT * FROM t INTO OUTFILE '/tmp/x'"))
                .expect_err("SELECT INTO refused"),
            "SELECT INTO",
        );

        // AS OF on the single table.
        assert_unsupported(
            &check_materialized_view_select(&select_query(
                "SELECT * FROM t AS OF TIMESTAMP '2026-01-01 00:00:00'",
            ))
            .expect_err("AS OF refused"),
            "AS OF",
        );

        // TABLESAMPLE on the single table.
        assert_unsupported(
            &check_materialized_view_select(&select_query("SELECT * FROM t TABLESAMPLE REGION ()"))
                .expect_err("TABLESAMPLE refused"),
            "TABLESAMPLE",
        );
    }

    #[test]
    fn check_select_accepts_supported_shapes() {
        // Plain single-table SELECT.
        assert!(check_materialized_view_select(&select_query("SELECT * FROM t")).is_ok());

        // No FROM at all (Go: `sel.From == nil`).
        assert!(check_materialized_view_select(&select_query("SELECT 1")).is_ok());

        // A right operand (comma join) falls through.
        assert!(check_materialized_view_select(&select_query("SELECT * FROM a, b")).is_ok());

        // A derived table on the left is not a `*ast.TableName`.
        assert!(
            check_materialized_view_select(&select_query("SELECT * FROM (SELECT 1) x")).is_ok()
        );

        // A set operation falls through Go's type assertion.
        assert!(check_materialized_view_select(&select_query("SELECT 1 UNION SELECT 2")).is_ok());
    }

    fn index(name: &str, columns: &[(&str, i64)], public: bool, invisible: bool) -> IndexInfo {
        IndexInfo {
            name: tidb_ast::CiString::new(name),
            columns: GoSharedPointerSlice::from_nullable(
                columns
                    .iter()
                    .map(|(col, length)| {
                        Some(IndexColumn {
                            name: tidb_ast::CiString::new(*col),
                            length: *length,
                            ..Default::default()
                        })
                    })
                    .collect(),
            ),
            state: if public {
                SchemaState::PUBLIC
            } else {
                SchemaState::NONE
            },
            invisible,
            ..Default::default()
        }
    }

    fn table_with_indices(indices: Vec<IndexInfo>, pk_col: Option<&str>) -> TableInfo {
        let mut table = TableInfo::default();
        table.indices =
            GoSharedPointerSlice::from_nullable(indices.into_iter().map(Some).collect());
        if let Some(pk) = pk_col {
            table.pk_is_handle = true;
            let mut column = ColumnInfo::default();
            column.name = tidb_ast::CiString::new(pk);
            column.add_flag(u64::from(FieldTypeFlags::PRI_KEY));
            table.columns = GoSharedPointerSlice::from_nullable(vec![Some(column)]);
        }
        table
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn find_visible_index_with_prefix_covering_columns_prefers_primary() {
        let table = table_with_indices(vec![index("idx_a", &[("a", 0)], true, false)], Some("a"));
        // Go's PK branch precedes the index scan and reports `PRIMARY`.
        assert_eq!(
            find_visible_index_with_prefix_covering_columns(Some(&table), &cols(&["A"])),
            Some("PRIMARY".to_owned())
        );
    }

    #[test]
    fn find_visible_indexes_filters_state_visibility_prefix_and_exclusion() {
        let table = table_with_indices(
            vec![
                index("idx_ok", &[("a", 0), ("b", 0)], true, false),
                index("idx_invisible", &[("a", 0), ("b", 0)], true, true),
                index("idx_nonpublic", &[("a", 0), ("b", 0)], false, false),
                index("idx_prefix", &[("a", 10), ("b", 0)], true, false),
                index("idx_short", &[("a", 0)], true, false),
                index("idx_wrong_col", &[("x", 0), ("b", 0)], true, false),
                index("idx_dup_col", &[("a", 0), ("a", 0)], true, false),
            ],
            None,
        );
        assert_eq!(
            find_visible_indexes_with_prefix_covering_columns(Some(&table), &cols(&["a", "B"])),
            vec!["idx_ok".to_owned()],
            "invisible, non-public, prefix, short, wrong-column and duplicated matches are skipped"
        );
        assert!(!has_index_with_prefix_covering_columns(
            Some(&table),
            &cols(&["a", "b"]),
            "idx_ok",
            true,
        ));
        assert!(has_index_with_prefix_covering_columns(
            Some(&table),
            &cols(&["a", "b"]),
            "",
            true,
        ));
    }

    #[test]
    fn empty_group_by_and_nil_table_return_no_layouts() {
        let table = table_with_indices(vec![index("idx_a", &[("a", 0)], true, false)], None);
        assert!(find_visible_indexes_with_prefix_covering_columns(Some(&table), &[]).is_empty());
        assert!(find_visible_indexes_with_prefix_covering_columns(None, &cols(&["a"])).is_empty());
        // Go's `excludedIndexName != strings.ToLower(mysql.PrimaryKeyName)`
        // guard: excluding PRIMARY disables the PK-handle branch itself.
        let pk_table = table_with_indices(Vec::new(), Some("a"));
        assert!(has_index_with_prefix_covering_columns(
            Some(&pk_table),
            &cols(&["a"]),
            "",
            true,
        ));
        assert!(!has_index_with_prefix_covering_columns(
            Some(&pk_table),
            &cols(&["a"]),
            "primary",
            true,
        ));
    }
}
