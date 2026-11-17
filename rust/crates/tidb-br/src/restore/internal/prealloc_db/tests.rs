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

//! Go `prealloc_db/db_test.go`, over the in-memory cluster.

use std::cell::Cell;
use std::collections::HashSet;

use tidb_ast::CiString;
use tidb_model::{
    ActionType, DBInfo, GoShared, HistoryInfo, Job, PartitionDefinition, PartitionInfo, PolicyInfo,
    PolicyRefInfo, TTLInfo, TableInfo,
};

use tidb_model::table::SequenceInfo;

use super::*;
use crate::glue::GlobalIdAllocator;
use crate::restore::internal::prealloc_table_id;
use crate::test_util::cluster::MockCluster;

struct TestAllocator(Cell<i64>);

impl GlobalIdAllocator for TestAllocator {
    fn get_global_id(&self) -> Result<i64> {
        Ok(self.0.get())
    }
    fn advance_global_ids(&self, n: i32) -> Result<i64> {
        let old = self.0.get();
        self.0.set(old + i64::from(n));
        Ok(old)
    }
}

fn db_info(name: &str, id: i64) -> DBInfo {
    DBInfo {
        id,
        name: CiString::new(name),
        charset: "utf8mb4".into(),
        collate: "utf8mb4_bin".into(),
        ..Default::default()
    }
}

fn policy_ref(name: &str) -> Option<GoShared<PolicyRefInfo>> {
    Some(GoShared::new(PolicyRefInfo {
        name: CiString::new(name),
        ..Default::default()
    }))
}

fn table(db: &DBInfo, id: i64, name: &str) -> metautil::Table {
    metautil::Table::with_schema(
        db.clone(),
        TableInfo {
            id,
            name: CiString::new(name),
            ..Default::default()
        },
    )
}

fn register(db: &DB, tables: &[metautil::Table], allocated_to: i64) {
    let ids = prealloc_table_id::new_and_prealloc(tables, &TestAllocator(Cell::new(allocated_to)))
        .unwrap();
    db.register_preallocated_ids(Arc::new(ids));
}

/// Go `NewDB` clears `sql_mode` and, with a mode, sets the placement mode.
#[test]
fn new_db_sets_session_modes() {
    let c = MockCluster::default();
    let (_, support) = new_db(&c, "STRICT").unwrap();
    assert!(support);
    let (_, support) = new_db(&c, "").unwrap();
    assert!(!support);
    assert_eq!(
        c.state().stmts,
        [
            "set @@sql_mode=''",
            "set @@tidb_placement_mode='STRICT';",
            "set @@sql_mode=''"
        ]
    );
    c.state().fail_sql.push((
        "tidb_placement_mode".into(),
        Arc::new(tidb_terror::TerrorError::compatible(
            tidb_terror::TerrorCode::new(1193),
            "Unknown system variable",
        )),
    ));
    let (_, support) = new_db(&c, "STRICT").unwrap();
    assert!(!support, "an unknown variable means no placement support");
}

/// Go `TestDB_ExecDDL`: a query job runs its query; an empty one is
/// skipped. Create-schema and create-table jobs go through the DDL.
#[test]
fn exec_ddl() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let db = DB::from_session(c.session());
    let job = |type_: ActionType, schema: &str, query: &str, history: HistoryInfo| {
        let mut j = Job::default();
        j.type_ = type_;
        j.schema_name = schema.into();
        j.query = query.into();
        j.binlog_info = Some(GoShared::new(history));
        j
    };
    let query_job = |q: &str| job(ActionType::ACTION_ADD_INDEX, "", q, HistoryInfo::default());
    db.exec_ddl(&ctx, &query_job("CREATE DATABASE IF NOT EXISTS test_db;"))
        .unwrap();
    db.exec_ddl(&ctx, &query_job("")).unwrap();
    assert_eq!(c.state().stmts, ["CREATE DATABASE IF NOT EXISTS test_db;"]);

    let schema_job = job(
        ActionType::ACTION_CREATE_SCHEMA,
        "",
        "",
        HistoryInfo {
            db_info: Some(GoShared::new(db_info("d", 2))),
            ..Default::default()
        },
    );
    db.exec_ddl(&ctx, &schema_job).unwrap();
    db.exec_ddl(&ctx, &schema_job).unwrap();
    let history = || HistoryInfo {
        db_info: Some(GoShared::new(db_info("d", 2))),
        table_info: Some(GoShared::new(TableInfo {
            id: 3,
            name: CiString::new("t"),
            ..Default::default()
        })),
        ..Default::default()
    };
    let table_job = job(ActionType::ACTION_CREATE_TABLE, "d", "", history());
    db.exec_ddl(&ctx, &table_job).unwrap();
    assert!(c.table("d", "t").is_some());
    db.exec_ddl(&ctx, &table_job).unwrap();
    assert_eq!(
        c.state().single_calls.len(),
        2,
        "an existing table is ignored by the BRIE glue"
    );

    // A table job with a query switches to its database first.
    let alter_job = job(
        ActionType::ACTION_ADD_INDEX,
        "d",
        "alter table t add index i(a)",
        history(),
    );
    db.exec_ddl(&ctx, &alter_job).unwrap();
    let stmts = c.state().stmts.clone();
    assert_eq!(
        &stmts[stmts.len() - 2..],
        ["use `d`;", "alter table t add index i(a)"]
    );
}

/// Go `TestCreateDoesNotMutateInputMetadata`: the create clears placement
/// and disables TTL on a copy only.
#[test]
fn create_does_not_mutate_input_metadata() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let db = DB::from_session(c.session());
    let mut info = db_info("immutable_metadata", 100);
    info.placement_policy_ref = policy_ref("missing_policy");
    assert!(!db.create_database(&ctx, &info, false, None).unwrap());
    assert!(info.placement_policy_ref.is_some());
    assert!(c.state().dbs["immutable_metadata"]
        .0
        .placement_policy_ref
        .is_none());

    let mut t = table(&info, 101, "t");
    t.info = Some(TableInfo {
        placement_policy_ref: policy_ref("missing_policy"),
        ttl_info: Some(GoShared::new(TTLInfo {
            column_name: CiString::new("id"),
            enable: true,
            ..Default::default()
        })),
        ..t.info().clone()
    });
    register(&db, std::slice::from_ref(&t), 1000);
    db.create_table(&ctx, &t, &HashSet::new(), false, None)
        .unwrap();
    assert!(t.info().placement_policy_ref.is_some());
    assert!(t.info().ttl_info.as_ref().unwrap().read().enable);
    let created = c.table("immutable_metadata", "t").unwrap();
    assert!(created.placement_policy_ref.is_none());
    assert!(!created.ttl_info.as_ref().unwrap().read().enable);
    assert_eq!(created.id, 1001, "the preallocated ID");
    assert!(c.state().single_calls[0].2.id_allocated);
    assert!(
        db.create_database(&ctx, &info, false, None).unwrap(),
        "an existing database is reused"
    );
}

/// Go `TestPolicyMode`: a referred policy is created once, when its first
/// database or table (or partition) is created.
#[test]
fn policies_created_on_first_reference() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let db = DB::from_session(c.session());
    let policies: PolicyMap = Mutex::new(
        ["fivereplicas", "p_part", "unused"]
            .iter()
            .map(|n| {
                (
                    (*n).to_owned(),
                    PolicyInfo {
                        name: CiString::new(*n),
                        ..Default::default()
                    },
                )
            })
            .collect(),
    );
    let mut info = db_info("d", 1);
    info.placement_policy_ref = policy_ref("fiveReplicas");
    db.create_database(&ctx, &info, true, Some(&policies))
        .unwrap();
    let mut t = table(&info, 2, "t");
    let def = PartitionDefinition {
        id: 3,
        name: CiString::new("p0"),
        placement_policy_ref: policy_ref("p_part"),
        ..Default::default()
    };
    t.info = Some(TableInfo {
        placement_policy_ref: policy_ref("fivereplicas"),
        partition: Some(GoShared::new(PartitionInfo {
            definitions: vec![def].into(),
            ..Default::default()
        })),
        ..t.info().clone()
    });
    register(&db, std::slice::from_ref(&t), 10);
    db.create_table(&ctx, &t, &HashSet::new(), true, Some(&policies))
        .unwrap();
    let created: Vec<String> = c
        .state()
        .policies
        .iter()
        .map(|p| p.name.original().to_owned())
        .collect();
    assert_eq!(created, ["fivereplicas", "p_part"]);
    assert_eq!(
        policies.lock().unwrap().keys().collect::<Vec<_>>(),
        ["unused"]
    );
}

/// Go `TestCreateTablesInDb`: one batch create, by database, with the
/// preallocated IDs; a session without batch create does nothing.
#[test]
fn create_tables_in_db() {
    let ctx = Context::background();
    let c = MockCluster::default();
    c.add_db(db_info("test", 1), Vec::new());
    c.add_db(db_info("other", 2), Vec::new());
    let db = DB::from_session(c.session());
    let test = db_info("test", 1);
    let other = db_info("other", 2);
    let tables = vec![
        table(&test, 3, "test3"),
        table(&other, 2, "o2"),
        table(&test, 1, "test1"),
    ];
    assert!(db
        .create_tables(&ctx, &tables, &HashSet::new(), false, None)
        .unwrap_err()
        .to_string()
        .contains("preallocedIDs is nil"));
    register(&db, &tables, 0);
    db.create_tables(&ctx, &tables, &HashSet::new(), false, None)
        .unwrap();
    assert_eq!(
        c.state().batch_calls,
        vec![vec![
            (
                "test".to_owned(),
                vec!["test3".to_owned(), "test1".to_owned()]
            ),
            ("other".to_owned(), vec!["o2".to_owned()])
        ]]
    );
    assert_eq!(c.table("test", "test3").unwrap().id, 3, "a free ID is kept");

    let c2 = MockCluster::default();
    c2.state().no_batch = true;
    let db2 = DB::from_session(c2.session());
    register(&db2, &tables, 0);
    db2.create_tables(&ctx, &tables, &HashSet::new(), false, None)
        .unwrap();
    assert!(c2.state().batch_calls.is_empty() && c2.state().single_calls.is_empty());
}

/// Go `TestRestoreAutoIncID`'s alter and `restoreSequence`: a table an
/// incremental restore found existing gets its auto ID base; a cycling
/// sequence gets its round then its value.
#[test]
fn create_table_post_restore() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let db = DB::from_session(c.session());
    let d = db_info("test", 1);
    let mut t = table(&d, 5, "\"t\"");
    t.info = Some(TableInfo {
        auto_inc_id: 142,
        ..t.info().clone()
    });
    let corrected: HashSet<UniqueTableName> = [UniqueTableName {
        db: "test".into(),
        table: "\"t\"".into(),
    }]
    .into_iter()
    .collect();
    db.create_table_post_restore(&ctx, &t, &HashSet::new())
        .unwrap();
    assert!(
        c.state().stmts.is_empty(),
        "a table not in the DDL jobs is left alone"
    );
    db.create_table_post_restore(&ctx, &t, &corrected).unwrap();
    assert_eq!(
        c.state().stmts,
        ["alter table `test`.`\"t\"` auto_increment = 142;"]
    );

    let mut s = table(&d, 6, "s");
    s.info = Some(TableInfo {
        auto_inc_id: 7,
        sequence: Some(GoShared::new(SequenceInfo {
            cycle: true,
            increment: -1,
            min_value: -5,
            max_value: 9,
            ..Default::default()
        })),
        ..s.info().clone()
    });
    c.state().stmts.clear();
    db.create_table_post_restore(&ctx, &s, &HashSet::new())
        .unwrap();
    assert_eq!(
        c.state().stmts,
        [
            "do setval(`test`.`s`, -5);",
            "do nextval(`test`.`s`);",
            "do setval(`test`.`s`, 7);"
        ]
    );
}
