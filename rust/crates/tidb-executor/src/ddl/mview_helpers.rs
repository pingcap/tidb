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

//! The remaining pure surface of pinned Go `pkg/ddl/materialized_view.go`
//! (master `94a9cbedab`): the import-into option builder shared by MV
//! maintenance, and the ALTER-path purge-meta validation.

use tidb_ast::MLogPurgeClause;
use tidb_model::TableInfo;

/// Go `BuildMViewImportIntoOptions` (`materialized_view.go:335`): the WITH
/// options shared by MV IMPORT INTO execution. `disable_precheck` always
/// leads; a positive thread count and a non-empty disk quota follow in Go's
/// order. The disk quota is SQL-escaped exactly as Go's
/// `sqlescape.MustEscapeSQL("disk_quota=%?", ..)` produces.
#[must_use]
pub fn build_m_view_import_into_options(
    import_threads: i64,
    import_disk_quota: &str,
) -> Vec<String> {
    let mut options = vec!["disable_precheck".to_owned()];
    if import_threads > 0 {
        options.push(format!("thread={import_threads}"));
    }
    if !import_disk_quota.is_empty() {
        options.push(format!("disk_quota='{}'", escape_sql(import_disk_quota)));
    }
    options
}

/// Go `sqlescape.MustEscapeSQL`'s single-quote doubling for the value
/// interpolated into the option string.
fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

/// Go `buildCreateMaterializedViewInsertSQL` (`mview_worker.go:493`): the
/// REPLACE INTO statement the insert-path build executes to populate the
/// view from its definition. Errors when the view metadata or its
/// `SQLContent` is missing.
pub fn build_create_materialized_view_insert_sql(
    schema_name: &str,
    view: &TableInfo,
) -> Result<String, String> {
    let sql_content = sql_content_of(view)?;
    Ok(format!(
        "REPLACE INTO {}.{} {}",
        quote_name(schema_name),
        quote_name(&view.name.original()),
        sql_content
    ))
}

/// Go `buildCreateMaterializedViewImportSQL` (`mview_worker.go:456`): the
/// IMPORT INTO statement the import-path build executes (TiKV stores).
pub fn build_create_materialized_view_import_sql(
    schema_name: &str,
    view: &TableInfo,
    thread_cnt: i64,
    disk_quota: &str,
) -> Result<String, String> {
    let sql_content = sql_content_of(view)?;
    let options = build_m_view_import_into_options(thread_cnt, disk_quota);
    Ok(format!(
        "IMPORT INTO {}.{} FROM ({}) WITH {}",
        quote_name(schema_name),
        quote_name(&view.name.original()),
        sql_content,
        options.join(", ")
    ))
}

/// Go `hasCreateMaterializedViewBuildRows`'s probe: a one-row existence
/// check the phase-2 tick runs to detect residual build rows from a crashed
/// prior attempt.
#[must_use]
pub fn build_create_materialized_view_build_rows_check_sql(
    schema_name: &str,
    mview_name: &str,
) -> String {
    format!(
        "SELECT 1 FROM {}.{} LIMIT 1",
        quote_name(schema_name),
        quote_name(mview_name)
    )
}

fn sql_content_of(view: &TableInfo) -> Result<String, String> {
    let meta = view
        .materialized_view
        .as_ref()
        .ok_or("create materialized view: invalid select sql".to_owned())?;
    let content = meta.read().sql_content.clone();
    if content.is_empty() {
        return Err("create materialized view: invalid select sql".to_owned());
    }
    Ok(content)
}

/// Go `sqlescape.MustEscapeSQL("%n", name)`'s back-quote wrapping.
fn quote_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `buildMLogPurgeMeta` (`materialized_view.go:692`): the ALTER
/// MATERIALIZED VIEW LOG purge clause, validated through the same canonical
/// schedule-expression builder the create path uses. Note Go's ALTER-path
/// refusal wording differs from the create path's
/// ("for ALTER MATERIALIZED VIEW LOG").
///
/// # Errors
/// `PURGE IMMEDIATE` or a non-DATETIME/TIMESTAMP schedule expression.
pub fn build_m_log_purge_meta(
    purge: Option<&MLogPurgeClause>,
) -> Result<(String, String, String), String> {
    use super::mview_schedule_expr::build_and_validate_m_view_schedule_expr;

    let Some(purge) = purge else {
        return Ok((String::new(), String::new(), String::new()));
    };
    if purge.immediate {
        return Err("PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG".to_owned());
    }

    let method = "DEFERRED".to_owned();
    let mut start_with = String::new();
    let mut next = String::new();
    if let Some(expr) = &purge.start_with {
        start_with = build_and_validate_m_view_schedule_expr(expr, "PURGE START WITH")
            .map_err(|error| error.to_string())?;
    }
    if let Some(expr) = &purge.next {
        next = build_and_validate_m_view_schedule_expr(expr, "PURGE NEXT")
            .map_err(|error| error.to_string())?;
    }
    Ok((method, start_with, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::GoShared;

    #[test]
    fn import_into_options_match_gos_order_and_escaping() {
        assert_eq!(
            build_m_view_import_into_options(0, ""),
            vec!["disable_precheck".to_owned()]
        );
        assert_eq!(
            build_m_view_import_into_options(4, "50GiB"),
            vec![
                "disable_precheck".to_owned(),
                "thread=4".to_owned(),
                "disk_quota='50GiB'".to_owned(),
            ]
        );
        // A negative thread count and an empty quota drop their options.
        assert_eq!(
            build_m_view_import_into_options(-1, ""),
            vec!["disable_precheck".to_owned()]
        );
    }

    fn purge_clause(sql: &str) -> Option<MLogPurgeClause> {
        let parsed = tidb_parser::parse(sql).expect("the ALTER parses");
        let tidb_ast::Stmt::Ddl(ddl) = &parsed else {
            panic!("a DDL statement")
        };
        let tidb_ast::DdlStmt::AlterMaterializedViewLog(alter) = ddl.as_ref() else {
            panic!("an ALTER MATERIALIZED VIEW LOG statement")
        };
        for action in &alter.actions {
            if let tidb_ast::AlterMaterializedViewLogAction::Purge(clause) = action {
                return Some(clause.clone());
            }
        }
        None
    }

    #[test]
    fn insert_sql_matches_gos_replace_into_shape() {
        let mut view = TableInfo::default();
        view.name = tidb_ast::CiString::new("mv");
        let mut meta = tidb_model::MaterializedViewInfo::default();
        meta.sql_content = "SELECT `id`, count(1) FROM `mv_base` GROUP BY `id`".to_owned();
        view.materialized_view = Some(GoShared::new(meta));

        let sql = build_create_materialized_view_insert_sql("u6", &view).expect("the SQL builds");
        assert_eq!(
            sql,
            "REPLACE INTO `u6`.`mv` SELECT `id`, count(1) FROM `mv_base` GROUP BY `id`"
        );
    }

    #[test]
    fn import_sql_matches_gos_import_into_shape() {
        let mut view = TableInfo::default();
        view.name = tidb_ast::CiString::new("mv");
        let mut meta = tidb_model::MaterializedViewInfo::default();
        meta.sql_content = "SELECT `id` FROM `mv_base` GROUP BY `id`".to_owned();
        view.materialized_view = Some(GoShared::new(meta));

        let sql = build_create_materialized_view_import_sql("u6", &view, 4, "50GiB")
            .expect("the SQL builds");
        assert_eq!(
            sql,
            "IMPORT INTO `u6`.`mv` FROM (SELECT `id` FROM `mv_base` GROUP BY `id`) WITH disable_precheck, thread=4, disk_quota='50GiB'"
        );
    }

    #[test]
    fn missing_sql_content_refuses() {
        let view = TableInfo::default();
        let error = build_create_materialized_view_insert_sql("u6", &view)
            .expect_err("no metadata refuses");
        assert_eq!(error, "create materialized view: invalid select sql");
    }

    #[test]
    fn build_rows_check_sql_matches_go() {
        assert_eq!(
            build_create_materialized_view_build_rows_check_sql("u6", "mv"),
            "SELECT 1 FROM `u6`.`mv` LIMIT 1"
        );
    }

    #[test]
    fn purge_meta_validation_matches_go() {
        assert_eq!(
            build_m_log_purge_meta(None).expect("no clause is valid"),
            (String::new(), String::new(), String::new())
        );
        let clause = purge_clause("ALTER MATERIALIZED VIEW LOG ON t PURGE IMMEDIATE")
            .expect("the clause is present");
        assert_eq!(
            build_m_log_purge_meta(Some(&clause)).expect_err("IMMEDIATE refuses"),
            "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG"
        );
    }
}
