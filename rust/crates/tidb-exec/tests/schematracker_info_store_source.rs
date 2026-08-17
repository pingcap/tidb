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

//! Source-backed tests for `pkg/ddl/schematracker/info_store.go`, ported from
//! `pkg/ddl/schematracker/info_store_test.go`.
//!
//! Go's `infoschema.ErrDatabaseNotExists.Equal(err)` class check becomes a
//! match on the local [`InfoStoreError`] variant; the assertions additionally
//! pin the rendered message text, which Go's `Equal` does not inspect.

use tidb_ast::CiString;
use tidb_exec::schematracker_info_store::{InfoStore, InfoStoreError};
use tidb_model::db::DBInfo;
use tidb_model::table_info::TableInfo;

fn db_info(name: &CiString) -> DBInfo {
    DBInfo {
        name: name.clone(),
        ..DBInfo::default()
    }
}

fn table_info(name: &CiString) -> TableInfo {
    TableInfo {
        name: name.clone(),
        ..TableInfo::default()
    }
}

/// Go `TestInfoStoreLowerCaseTableNames`
/// (`pkg/ddl/schematracker/info_store_test.go:28-94`).
#[test]
fn source_lower_case_table_names_selects_the_folding_rule() {
    let db_name = CiString::new("DBName");
    let lower_db_name = CiString::new("dbname");
    let table_name = CiString::new("TableName");
    let lower_table_name = CiString::new("tablename");

    // case-sensitive (info_store_test.go:36-65)

    let mut store = InfoStore::new(0);
    store.put_schema(db_info(&db_name));
    assert!(store.schema_by_name(&db_name).is_some());
    assert!(store.schema_by_name(&lower_db_name).is_none());

    let err = store
        .put_table(&lower_db_name, table_info(&table_name))
        .unwrap_err();
    assert_eq!(
        err,
        InfoStoreError::DatabaseNotExists {
            database: "dbname".to_owned()
        }
    );
    assert_eq!(err.to_string(), "Unknown database 'dbname'");
    store.put_table(&db_name, table_info(&table_name)).unwrap();
    assert!(store.table_by_name(&db_name, &table_name).is_ok());

    let err = store
        .table_by_name(&lower_table_name, &table_name)
        .unwrap_err();
    assert_eq!(
        err,
        InfoStoreError::DatabaseNotExists {
            database: "tablename".to_owned()
        }
    );
    let err = store
        .table_by_name(&db_name, &lower_table_name)
        .unwrap_err();
    assert_eq!(
        err,
        InfoStoreError::TableNotExists {
            database: "DBName".to_owned(),
            table: "tablename".to_owned()
        }
    );
    assert_eq!(err.to_string(), "Table 'DBName.tablename' doesn't exist");

    assert_eq!(store.all_schema_names(), vec!["DBName".to_owned()]);
    assert!(store
        .all_table_names_of_schema(&CiString::new("wrong-db"))
        .is_err());
    assert_eq!(
        store.all_table_names_of_schema(&db_name).unwrap(),
        vec!["TableName".to_owned()]
    );

    // compare-insensitive (info_store_test.go:67-93)

    let mut store = InfoStore::new(2);
    store.put_schema(db_info(&db_name));
    assert!(store.schema_by_name(&db_name).is_some());
    let got = store.schema_by_name(&lower_db_name).expect("schema stored");
    // The stored value keeps the original spelling even though the key folded.
    assert_eq!(got.name.original(), db_name.original());

    store
        .put_table(&lower_db_name, table_info(&table_name))
        .unwrap();
    assert!(store.table_by_name(&db_name, &table_name).is_ok());
    let got = store
        .table_by_name(&db_name, &lower_table_name)
        .expect("table stored");
    assert_eq!(got.name.original(), table_name.original());

    assert_eq!(store.all_schema_names(), vec!["dbname".to_owned()]);
    assert!(store
        .all_table_names_of_schema(&CiString::new("wrong-db"))
        .is_err());
    assert_eq!(
        store.all_table_names_of_schema(&db_name).unwrap(),
        vec!["tablename".to_owned()]
    );
}

/// Go `TestInfoStoreDeleteTables`
/// (`pkg/ddl/schematracker/info_store_test.go:96-156`).
#[test]
fn source_delete_tables_and_schemas() {
    let mut store = InfoStore::new(0);
    let db_name1 = CiString::new("DBName1");
    let db_name2 = CiString::new("DBName2");
    let table_name1 = CiString::new("TableName1");
    let table_name2 = CiString::new("TableName2");

    store.put_schema(db_info(&db_name1));
    store.put_table(&db_name1, table_info(&table_name1)).unwrap();
    store.put_table(&db_name1, table_info(&table_name2)).unwrap();

    assert_eq!(store.all_schema_names(), vec!["DBName1".to_owned()]);
    let mut table_names = store.all_table_names_of_schema(&db_name1).unwrap();
    table_names.sort();
    assert_eq!(
        table_names,
        vec!["TableName1".to_owned(), "TableName2".to_owned()]
    );

    // db2 not created (info_store_test.go:120-126)
    assert!(!store.delete_schema(&db_name2));
    assert_eq!(
        store
            .put_table(&db_name2, table_info(&table_name1))
            .unwrap_err(),
        InfoStoreError::DatabaseNotExists {
            database: "DBName2".to_owned()
        }
    );
    assert_eq!(
        store.delete_table(&db_name2, &table_name1).unwrap_err(),
        InfoStoreError::DatabaseNotExists {
            database: "DBName2".to_owned()
        }
    );

    store.put_schema(db_info(&db_name2));
    store.put_table(&db_name2, table_info(&table_name1)).unwrap();

    let mut schema_names = store.all_schema_names();
    schema_names.sort();
    assert_eq!(
        schema_names,
        vec!["DBName1".to_owned(), "DBName2".to_owned()]
    );
    assert_eq!(
        store.all_table_names_of_schema(&db_name2).unwrap(),
        vec!["TableName1".to_owned()]
    );

    assert_eq!(
        store.delete_table(&db_name2, &table_name2).unwrap_err(),
        InfoStoreError::TableNotExists {
            database: "DBName2".to_owned(),
            table: "TableName2".to_owned()
        }
    );
    store.delete_table(&db_name2, &table_name1).unwrap();
    assert_eq!(
        store.all_table_names_of_schema(&db_name2).unwrap(),
        Vec::<String>::new()
    );

    // delete db will remove its tables (info_store_test.go:148-155)
    assert!(store.delete_schema(&db_name1));
    assert_eq!(
        store.table_by_name(&db_name1, &table_name1).unwrap_err(),
        InfoStoreError::DatabaseNotExists {
            database: "DBName1".to_owned()
        }
    );
    assert_eq!(store.all_schema_names(), vec!["DBName2".to_owned()]);
}
