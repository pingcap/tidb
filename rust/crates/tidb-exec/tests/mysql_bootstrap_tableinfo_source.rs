// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The acceptance bar for [`tidb_exec::table_info_build`]: every `mysql.*`
//! `CREATE TABLE` a fresh TiDB bootstrap runs must lower to the `TableInfo`
//! that real TiDB builds for it.
//!
//! `tests/data/mysql_bootstrap_tableinfos.json` is the ground truth. It is not
//! hand-written and not inferred from a `SHOW CREATE TABLE`: it is the JSON
//! `encoding/json` produces from the `*model.TableInfo` values Go's own
//! `ddl.BuildTableInfoFromAST` returns for `pkg/session`'s
//! `systemTablesOfBaseNextGenVersion`, evaluated under the exact metabuild
//! context classic bootstrap uses —
//!
//! ```text
//! evalCtx := exprstatic.NewEvalContext(exprstatic.WithSQLMode(mysql.ModeNone))
//! mbCtx := metabuild.NewContext(
//!     metabuild.WithExprCtx(exprstatic.NewExprContext(exprstatic.WithEvalCtx(evalCtx))),
//!     metabuild.WithClusteredIndexDefMode(vardef.ClusteredIndexDefModeIntOnly),
//! )
//! ```
//!
//! Both settings are load-bearing and neither is the library default:
//!
//! * `ModeNone` is why `mysql.global_priv`'s `Priv LONGTEXT NOT NULL DEFAULT ''`
//!   builds at all — under the default strict mode Go rejects it outright with
//!   `BLOB/TEXT/JSON column 'Priv' can't have a default value`.
//! * `ClusteredIndexDefModeIntOnly` is why `mysql.tidb`'s `VARCHAR` primary key
//!   is NOT a clustered common handle. `session.go` sets it explicitly "for the
//!   bootstrap SQLs"; under the library default (`On`) `mysql.tidb` would come
//!   out `is_common_handle: true`, which would put `VARIABLE_NAME` in the record
//!   KEY. A live v8.5.7 cluster's own `mysql.tidb` rows carry `VARIABLE_NAME` in
//!   the row VALUE (see `mysql_system_tables`'s captured `BOOTSTRAPPED_ROW`),
//!   so `IntOnly` is the setting that agrees with a real cluster's bytes.

use std::collections::BTreeMap;

use tidb_ast::{DdlStmt, Stmt};
use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
use tidb_metadef::BOOTSTRAP_TABLES;
use tidb_model::table_info::TableInfo;

/// The Go-built `TableInfo` of every `mysql.*` bootstrap table, by table name.
const GO_TABLE_INFOS: &str = include_str!("data/mysql_bootstrap_tableinfos.json");

fn go_table_infos() -> BTreeMap<String, TableInfo> {
    serde_json::from_str(GO_TABLE_INFOS).expect("the captured Go TableInfos decode")
}

fn lower(create_sql: &str) -> TableInfo {
    let parsed = tidb_parser::parse(create_sql).expect("the metadef statement parses");
    let Stmt::Ddl(ddl) = &parsed else {
        panic!("a metadef statement is a DDL");
    };
    let DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("a metadef statement is a CREATE TABLE");
    };
    build_table_info(
        create,
        "utf8mb4",
        "utf8mb4_bin",
        ClusteredIndexDefMode::IntOnly,
    )
    .expect("a bootstrap CREATE TABLE is admitted")
}

#[test]
fn the_corpus_is_the_whole_bootstrap_table_list() {
    // A table added to Go's own list without a fresh capture would otherwise
    // pass silently by simply never being compared.
    assert_eq!(BOOTSTRAP_TABLES.len(), 52);
    let go = go_table_infos();
    assert_eq!(go.len(), BOOTSTRAP_TABLES.len());
    for table in BOOTSTRAP_TABLES {
        assert!(go.contains_key(table.name), "{} has no capture", table.name);
    }
}

#[test]
fn every_mysql_bootstrap_table_lowers_to_the_table_info_go_builds() {
    let go = go_table_infos();
    let mut mismatched = Vec::new();
    for table in BOOTSTRAP_TABLES {
        let expected = go.get(table.name).expect("every table is captured");
        let mut ours = lower(table.create_sql);
        // The only two fields the builder deliberately leaves to its caller:
        // the ID comes from the bootstrap's reserved range or from a global-ID
        // allocation, and the timestamp from the publishing transaction.
        ours.id = expected.id;
        ours.update_ts = expected.update_ts;
        let ours = serde_json::to_value(&ours).expect("our TableInfo serializes");
        let expected = serde_json::to_value(expected).expect("the capture serializes");
        if ours != expected {
            mismatched.push((table.name, field_diff(&ours, &expected)));
        }
    }
    assert!(
        mismatched.is_empty(),
        "these mysql.* tables do not lower to Go's own TableInfo: {mismatched:#?}"
    );
}

/// Names the JSON paths where two `TableInfo`s differ, so a failure points at
/// the offending field rather than dumping two whole tables.
fn field_diff(ours: &serde_json::Value, expected: &serde_json::Value) -> Vec<String> {
    fn walk(path: &str, ours: &serde_json::Value, expected: &serde_json::Value, out: &mut Vec<String>) {
        match (ours, expected) {
            (serde_json::Value::Object(ours), serde_json::Value::Object(expected)) => {
                for key in ours.keys().chain(expected.keys()) {
                    let child = format!("{path}.{key}");
                    if out.iter().any(|seen| seen.starts_with(&child)) {
                        continue;
                    }
                    walk(
                        &child,
                        ours.get(key).unwrap_or(&serde_json::Value::Null),
                        expected.get(key).unwrap_or(&serde_json::Value::Null),
                        out,
                    );
                }
            }
            (serde_json::Value::Array(ours), serde_json::Value::Array(expected))
                if ours.len() == expected.len() =>
            {
                for (index, (ours, expected)) in ours.iter().zip(expected).enumerate() {
                    walk(&format!("{path}[{index}]"), ours, expected, out);
                }
            }
            (ours, expected) if ours != expected => {
                out.push(format!("{path}: ours {ours} vs Go {expected}"));
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk("", ours, expected, &mut out);
    out.truncate(8);
    out
}

#[test]
fn the_bootstrap_table_ids_are_the_reserved_ones_go_assigns() {
    let go = go_table_infos();
    for table in BOOTSTRAP_TABLES {
        assert_eq!(
            go[table.name].id, table.id,
            "{} is created under a different reserved id",
            table.name
        );
        assert!(
            tidb_metadef::is_reserved_id(table.id),
            "{} must not come out of the user id space",
            table.name
        );
    }
}

#[test]
fn a_non_clustered_composite_primary_key_lowers_to_an_index_not_a_handle() {
    // `mysql.user` is the shape the whole privilege reader depends on: the
    // PRIMARY KEY is an ordinary IndexInfo, NOT the row handle, which is why
    // every column including Host and User lives in the row value.
    let user = lower(
        BOOTSTRAP_TABLES
            .iter()
            .find(|table| table.name == "user")
            .expect("mysql.user is a bootstrap table")
            .create_sql,
    );
    assert!(!user.pk_is_handle);
    assert!(!user.is_common_handle);
    let primary = &user.indices[0];
    assert_eq!(primary.name.original(), "PRIMARY");
    assert!(primary.primary && primary.unique);
    assert_eq!(
        primary
            .columns
            .iter()
            .map(|column| column.name.original())
            .collect::<Vec<_>>(),
        ["Host", "User"]
    );
    // ... and the secondary KEY beside it, which is neither primary nor unique.
    let secondary = &user.indices[1];
    assert_eq!(secondary.name.original(), "i_user");
    assert!(!secondary.primary && !secondary.unique);
    assert_eq!(user.max_index_id, 2);
}

#[test]
fn a_declared_default_is_stored_in_gos_own_string_form() {
    let user = lower(
        BOOTSTRAP_TABLES
            .iter()
            .find(|table| table.name == "user")
            .expect("mysql.user is a bootstrap table")
            .create_sql,
    );
    let default_of = |name: &str| {
        user.columns
            .iter()
            .find(|column| column.name.lowercase() == name)
            .and_then(|column| column.default_value.clone())
    };
    let text = |name: &str| match default_of(name) {
        Some(tidb_model::column::ColumnDefaultValue::Str(bytes)) => {
            Some(String::from_utf8(bytes).expect("a UTF-8 default"))
        }
        _ => None,
    };
    // Go stores every default as a STRING, whatever the column type is.
    assert_eq!(text("select_priv").as_deref(), Some("N"));
    assert_eq!(text("max_user_connections").as_deref(), Some("0"));
    assert_eq!(text("password_last_changed").as_deref(), Some("CURRENT_TIMESTAMP"));
    // `smallint unsigned DEFAULT NULL` declares a default and stores none, so
    // the column is NOT marked as having no default value.
    assert_eq!(default_of("password_reuse_history"), None);
}
