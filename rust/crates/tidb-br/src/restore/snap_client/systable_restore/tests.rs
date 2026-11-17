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

//! Go `snap_client/systable_restore_test.go` over the in-memory cluster.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_model::DBInfo;

use super::*;
use crate::restore::snap_client::client::tests::{client, db_info};
use crate::test_util::cluster::MockCluster;

fn varchar(collation: &str) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Varchar)
        .with_charset_name("utf8mb4")
        .with_collation_name(collation);
    ft.set_flen(32);
    ft
}

fn col(name: &str, ft: FieldType) -> ColumnInfo {
    ColumnInfo {
        name: CiString::new(name),
        field_type: ft,
        ..Default::default()
    }
}

fn tbl(id: i64, name: &str, cols: Vec<ColumnInfo>) -> TableInfo {
    TableInfo {
        id,
        name: CiString::new(name),
        columns: cols.into(),
        ..Default::default()
    }
}

fn bin_cols(names: &[&str]) -> Vec<ColumnInfo> {
    names
        .iter()
        .map(|n| col(n, varchar("utf8mb4_bin")))
        .collect()
}

fn temp_mysql() -> DBInfo {
    db_info(&temporary_db_name("mysql"), 90)
}

/// Go `TestRestoreSystemSchemas`-like: existing tables are replaced into
/// by column name, a missing one is renamed into place, the statistics
/// and unrecoverable ones are skipped; then privileges are reloaded, the
/// pseudo bindings deduplicated and the temporary database dropped.
#[test]
fn restore_system_schemas_logically() {
    let ctx = Context::background();
    let c = MockCluster::default();
    c.add_db(
        db_info("mysql", 1),
        vec![
            tbl(
                2,
                "user",
                bin_cols(&["user", "host", "super_priv", "operate_view_priv"]),
            ),
            tbl(
                3,
                "bind_info",
                bin_cols(&["original_sql", "last_used_date"]),
            ),
        ],
    );
    let mut rc = client(&c);
    rc.set_with_sys_table(true);
    let temp = temp_mysql();
    let backed_up = vec![
        tbl(91, "user", bin_cols(&["host", "user", "super_priv"])),
        tbl(92, "bind_info", bin_cols(&["original_sql"])),
        tbl(93, "role_edges", bin_cols(&["from_user"])),
        tbl(94, "stats_meta", bin_cols(&["version"])),
        tbl(95, "tidb", bin_cols(&["variable_name"])),
    ];
    rc.databases.insert(
        temp.name.original().to_owned(),
        metautil::Database::new(
            temp.clone(),
            backed_up
                .into_iter()
                .map(|t| metautil::Table::with_schema(temp.clone(), t))
                .collect(),
        ),
    );
    c.state().stmts.clear();
    rc.restore_system_schemas(&ctx, tidb_table_filter::all().as_ref(), false)
        .unwrap();
    let stmts = c.state().stmts.clone();
    assert_eq!(
        stmts,
        [
            "UPDATE `__tidb_br_temporary_mysql`.`user` SET User_attributes = JSON_REMOVE(User_attributes, '$.resource_group');",
            "REPLACE INTO `mysql`.`user`(`user`,`host`,`super_priv`,`operate_view_priv`) SELECT `user`,`host`,`super_priv`,IF(`Super_priv` = 'Y', 'Y', 'N') FROM `__tidb_br_temporary_mysql`.`user`;",
            "REPLACE INTO `mysql`.`bind_info`(`original_sql`,`last_used_date`) SELECT `original_sql`,NULL FROM `__tidb_br_temporary_mysql`.`bind_info`;",
            "RENAME TABLE `__tidb_br_temporary_mysql`.`role_edges` TO `mysql`.`role_edges`;",
            STMT_REMOVE_DUPLICATED_PSEUDO_BINDING,
            "DROP DATABASE IF EXISTS `__tidb_br_temporary_mysql`",
            // Go drops every temporary system database once its step
            // succeeded, backed up or not.
            "DROP DATABASE IF EXISTS `__tidb_br_temporary_sys`",
            "DROP DATABASE IF EXISTS `__tidb_br_temporary_workload_schema`",
        ]
    );
    assert_eq!(c.state().privilege_notified, 1);

    // Filtered out, or without WITH_SYS_TABLE, nothing runs.
    c.state().stmts.clear();
    rc.set_with_sys_table(false);
    rc.restore_system_schemas(&ctx, tidb_table_filter::all().as_ref(), false)
        .unwrap();
    assert!(
        c.state()
            .stmts
            .iter()
            .all(|s| s.starts_with("DROP DATABASE IF EXISTS")),
        "{:?}",
        c.state().stmts
    );
}

/// A failed statement keeps the temporary database for a retry.
#[test]
fn failed_system_table_keeps_temporary_database() {
    let ctx = Context::background();
    let c = MockCluster::default();
    c.add_db(db_info("mysql", 1), Vec::new());
    let mut rc = client(&c);
    rc.set_with_sys_table(true);
    let temp = temp_mysql();
    rc.databases.insert(
        temp.name.original().to_owned(),
        metautil::Database::new(
            temp.clone(),
            vec![metautil::Table::with_schema(
                temp.clone(),
                tbl(93, "role_edges", bin_cols(&["a"])),
            )],
        ),
    );
    c.state()
        .fail_sql
        .push(("RENAME TABLE".into(), errors::errorf("boom")));
    c.state().stmts.clear();
    let err = rc
        .restore_system_schemas(&ctx, tidb_table_filter::all().as_ref(), false)
        .unwrap_err();
    assert!(
        err.to_string().contains(
            "error during merging temporary tables into system tables, table: role_edges"
        ),
        "{err}"
    );
    assert!(c
        .state()
        .stmts
        .iter()
        .all(|s| !s.starts_with("DROP DATABASE")));
}

/// Go `TestCheckSysTableCompatibility`.
#[test]
fn check_sys_table_compatibility_cases() {
    let c = MockCluster::default();
    c.add_db(
        db_info("mysql", 1),
        vec![
            tbl(
                2,
                "user",
                bin_cols(&["host", "user", "operate_view_priv", "super_priv"]),
            ),
            tbl(
                3,
                "db",
                vec![
                    col("host", varchar("utf8mb4_bin")),
                    col("db", varchar("utf8mb4_general_ci")),
                ],
            ),
        ],
    );
    let temp = temp_mysql();
    let backup = |t: TableInfo| metautil::Table::with_schema(temp.clone(), t);

    // Same columns in another order: compatible and physically loadable.
    let user = backup(tbl(
        91,
        "user",
        bin_cols(&["super_priv", "operate_view_priv", "user", "host"]),
    ));
    assert!(check_sys_table_compatibility(&c, std::slice::from_ref(&user), false).unwrap());

    // A column the backup lacks but logical restore can fill: not physical.
    let old_user = backup(tbl(91, "user", bin_cols(&["host", "user", "super_priv"])));
    assert!(!check_sys_table_compatibility(&c, &[old_user], false).unwrap());

    // An extra backup column is refused.
    let wide = backup(tbl(
        91,
        "user",
        bin_cols(&["host", "user", "operate_view_priv", "super_priv", "extra"]),
    ));
    let err = check_sys_table_compatibility(&c, &[wide], false).unwrap_err();
    assert!(
        err.to_string()
            .contains("missing column in cluster data, table: user, col: extra"),
        "{err}"
    );

    // utf8mb4_bin into utf8mb4_general_ci only with the collation check.
    let db = backup(tbl(92, "db", bin_cols(&["host", "db"])));
    assert!(check_sys_table_compatibility(&c, std::slice::from_ref(&db), false).is_err());
    assert!(!check_sys_table_compatibility(&c, &[db], true).unwrap());

    // A renameable table missing downstream.
    let roles = backup(tbl(93, "role_edges", bin_cols(&["a"])));
    let err = check_sys_table_compatibility(&c, &[roles], false).unwrap_err();
    assert!(
        err.to_string().contains("missed system table: role_edges"),
        "{err}"
    );

    // Non-renameable tables are not checked.
    let other = backup(tbl(94, "stats_meta", bin_cols(&["a"])));
    assert!(check_sys_table_compatibility(&c, &[other], false).unwrap());
}

/// Go `TestCheckPrivilegeTableRowsCollateCompatibility`: rows that would
/// collide under `utf8mb4_general_ci` are refused.
#[test]
fn privilege_rows_collate_compatibility() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let rc = client(&c);
    let up = tbl(1, "db", bin_cols(&["host", "db"]));
    let down = tbl(
        2,
        "db",
        vec![
            col("host", varchar("utf8mb4_bin")),
            col("db", varchar("utf8mb4_general_ci")),
        ],
    );
    let count = |n: i64| vec![vec![tidb_datatype::Datum::new_int(n)]];
    c.state().rows = vec![
        ("GROUP BY".into(), count(2)),
        (
            "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.db".into(),
            count(3),
        ),
    ];
    let err = rc
        .check_privilege_table_rows_collate_compatibility(&ctx, "mysql", "db", &up, &down)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("upstream count 3 != downstream count 2"),
        "{err}"
    );
    c.state().rows[0].1 = count(3);
    rc.check_privilege_table_rows_collate_compatibility(&ctx, "mysql", "db", &up, &down)
        .unwrap();
    let bad = tbl(2, "db", bin_cols(&["host", "db"]));
    let err = rc
        .check_privilege_table_rows_collate_compatibility(&ctx, "mysql", "db", &up, &bad)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("downstream table mysql.db column db collate"),
        "{err}"
    );
}

/// Go `TestTemporaryTableChecker`.
#[test]
fn temporary_table_checker() {
    let stats = TemporaryTableChecker {
        load_stats_physical: true,
        load_sys_table_physical: false,
    };
    assert_eq!(
        stats
            .check_temporary_tables("__TiDB_BR_Temporary_mysql", "stats_meta")
            .as_deref(),
        Some("mysql")
    );
    assert_eq!(
        stats.check_temporary_tables("__TiDB_BR_Temporary_mysql", "user"),
        None
    );
    assert_eq!(stats.check_temporary_tables("mysql", "stats_meta"), None);
    let sys = TemporaryTableChecker {
        load_stats_physical: false,
        load_sys_table_physical: true,
    };
    assert_eq!(
        sys.check_temporary_tables("__TiDB_BR_Temporary_mysql", "user")
            .as_deref(),
        Some("mysql")
    );
    assert!(TemporaryTableChecker::default()
        .check_temporary_tables("__TiDB_BR_Temporary_mysql", "user")
        .is_none());
}
