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

//! A loaded table's stored metadata reaches the SQL a node session runs over
//! it: each test publishes one `ClusterCatalog` -- metadata as the store's
//! meta holds it, some of which SQL cannot create (a column mid-DDL, a
//! stored DEFAULT, a hidden index column) -- and drives a session on that
//! node.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use super::super::*;
use super::node_fixture::*;
use tidb_domain_core::cluster_session::SkippedTable;
use crate::StmtResult;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType};
use tidb_stats_handle_storage::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use tidb_model::column::{ColumnDefaultValue, ColumnInfo};
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::table::ViewInfo;
use tidb_model::{GoAny, GoShared, SchemaState, TableInfo};

/// A session on a node that publishes `catalog`, the tables its catalog
/// left out, and the node.
fn node_session(catalog: ClusterCatalog) -> (Session, Vec<SkippedTable>, MockNode) {
    let node = MockNode::over(catalog);
    let session = open_session_on(&node);
    let skipped = session.node().skipped_tables().to_vec();
    (session, skipped, node)
}

fn one_database(tables: Vec<TableInfo>) -> ClusterCatalog {
    ClusterCatalog {
        resource_groups: Vec::new(),
        policies: Vec::new(),
        schema_version: 7,
        databases: vec![tidb_infoschema::cluster_catalog::LoadedDatabase {
            info: DBInfo {
                id: 5,
                name: CiString::new("app"),
                ..DBInfo::default()
            },
            tables,
        }],
    }
}

fn one_table_catalog(table: TableInfo) -> ClusterCatalog {
    one_database(vec![table])
}

/// One database `app` holding `t(id BIGINT PRIMARY KEY, v BIGINT)`, plus a
/// table still mid-DDL that the session must refuse by name.
fn base_catalog() -> ClusterCatalog {
    let base = TableInfo {
        id: 101,
        name: CiString::new("t"),
        columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let pending = TableInfo {
        id: 102,
        name: CiString::new("t_pending"),
        columns: vec![column(1, 0, "id", false)].into(),
        state: SchemaState::NONE,
        ..TableInfo::default()
    };
    one_database(vec![base, pending])
}

/// One database `app` holding `d(id BIGINT PRIMARY KEY, v BIGINT DEFAULT
/// 7)`, as the loader receives it from the cluster: the DEFAULT lives on
/// the stored `ColumnInfo`, not on anything this node builds by hand.
fn default_catalog() -> ClusterCatalog {
    let mut v = column(2, 1, "v", false);
    v.default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
    v.origin_default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
    one_table_catalog(TableInfo {
        id: 301,
        name: CiString::new("d"),
        columns: vec![column(1, 0, "id", true), v].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    })
}

/// One database `app` holding `ci(id BIGINT PRIMARY KEY, c VARCHAR(32)
/// COLLATE utf8mb4_general_ci)`, built the way the cluster loader builds
/// it: the column's `FieldType` is the one decoded from the stored
/// descriptor, collation name and all.
fn ci_catalog() -> ClusterCatalog {
    // Decoded from the stored descriptor rather than built by hand, so this
    // exercises the same `From<JsonFieldType>` the catalog loader uses --
    // including its duty to fill BOTH the collation name and the cached
    // `Collation` enum.
    let text: FieldType = serde_json::from_str(
        r#"{"Tp":15,"Flag":0,"Flen":32,"Decimal":0,"Charset":"utf8mb4",
            "Collate":"utf8mb4_general_ci","Elems":null,
            "ElemsIsBinaryLit":null,"Array":false}"#,
    )
    .unwrap();
    assert_eq!(text.collation(), tidb_charset::Collation::Utf8Mb4GeneralCi);
    let mut c = ColumnInfo::new(2, "c", text);
    c.offset = 1;
    one_table_catalog(TableInfo {
        id: 201,
        name: CiString::new("ci"),
        columns: vec![column(1, 0, "id", true), c].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    })
}

/// An index on a column, as the cluster stores it. `prefix` is Go's
/// `IndexColumn.Length`, which is `UnspecifiedLength` (-1) for a whole
/// column and a byte count for a prefix index.
fn index(id: i64, name: &str, column: &str, offset: i64, prefix: i64) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(name),
        columns: vec![IndexColumn {
            name: CiString::new(column),
            offset,
            length: prefix,
            ..IndexColumn::default()
        }]
        .into(),
        state: SchemaState::PUBLIC,
        ..IndexInfo::default()
    }
}

fn result_rows(result: Result<StmtResult, tidb_driver_error::DriverError>) -> Vec<Vec<Datum>> {
    match result.unwrap() {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn kv_table<'a>(
    catalog: &'a tidb_infoschema::Catalog,
    name: &str,
) -> &'a tidb_table::kv_table::KvTable {
    let tidb_infoschema::TableEntry::Kv(kv) = catalog
        .table_in("app", name)
        .expect("the table is in the catalog")
    else {
        panic!("a cluster table is a kv table");
    };
    kv
}

#[test]
fn cluster_views_are_registered_from_go_table_info() {
    let base = TableInfo {
        id: 101,
        name: CiString::new("t"),
        columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let view = TableInfo {
        id: 102,
        name: CiString::new("revenue0"),
        charset: "utf8mb4".to_owned(),
        collate: "utf8mb4_bin".to_owned(),
        columns: vec![
            column(1, 0, "supplier_no", false),
            column(2, 1, "total_revenue", false),
        ]
        .into(),
        state: SchemaState::PUBLIC,
        view: Some(GoShared::new(ViewInfo {
            select_stmt: "SELECT `app`.`t`.`id` AS `supplier_no`, \
                SUM(`app`.`t`.`v`) AS `total_revenue` FROM `app`.`t` \
                GROUP BY `app`.`t`.`id`"
                .to_owned(),
            // Go's `CREATE VIEW` records its definer, whose privileges a
            // `SQL SECURITY DEFINER` body is checked against.
            definer: Some(Box::new(tidb_parser::auth::UserIdentity {
                username: "root".to_owned(),
                hostname: "%".to_owned(),
                ..tidb_parser::auth::UserIdentity::default()
            })),
            ..ViewInfo::default()
        })),
        ..TableInfo::default()
    };
    let (mut session, skipped, _node) = node_session(one_database(vec![base, view]));
    assert!(
        skipped.is_empty(),
        "cluster view must not be skipped: {skipped:?}"
    );
    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT supplier_no, total_revenue FROM revenue0 \
             WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue0)",
        )
        .expect("the stored view body is planned through the cluster catalog");
}

#[test]
fn loaded_column_ndv_reaches_grouped_cluster_plans() {
    struct FixedStorageStatistics {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        column_length_requests: Arc<std::sync::Mutex<Vec<bool>>>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        values: tidb_stats_handle_cache::table_size_stats::TableRowStatsSnapshot,
    }

    impl crate::TableStorageStatsProvider for FixedStorageStatistics {
        fn load_table_row_stats(
            &self,
            _resource_group: &str,
            need_column_lengths: bool,
            _physical_ids: &[i64],
        ) -> Result<tidb_stats_handle_cache::table_size_stats::TableRowStatsSnapshot, String> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.column_length_requests
                .lock()
                .unwrap()
                .push(need_column_lengths);
            if self.fail.load(std::sync::atomic::Ordering::Acquire) {
                return Err("stats read failed".to_owned());
            }
            Ok(self.values.clone())
        }
    }

    let table = TableInfo {
        id: 130,
        name: CiString::new("order_line"),
        columns: vec![
            column(1, 0, "ol_o_id", true),
            column(2, 1, "ol_d_id", false),
            column(3, 2, "ol_amount", false),
        ]
        .into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let version = 42;
    let item = |id, ndv| {
        let mut item = ClusterStatsItem {
            id,
            is_index: false,
            stats_ver: 2,
            flag: 1,
            load_status: tidb_stats::StatsLoadedStatus::full_load(),
            histogram: Default::default(),
            topn: None,
            cms: None,
            fm_sketch: None,
        };
        item.histogram.id = id;
        item.histogram.ndv = ndv;
        item.histogram.last_update_version = version;
        item.histogram
            .append_bucket(Datum::Int(1), Datum::Int(3_000_065), 3_000_065, 1);
        item
    };
    let loaded_stats = ClusterTableStats {
        table_id: table.id,
        version,
        snapshot: version,
        last_analyze_version: version,
        last_stats_hist_version: version,
        modify_count: 0,
        row_count: 3_000_065,
        columns: vec![item(1, 3_000_065), item(2, 10), item(3, 3_000_065)],
        indexes: Vec::new(),
    };
    let loaded_stats = loaded_stats.to_statistics_table(&table);
    let table_id = table.id;
    assert!(!loaded_stats.hist_coll.pseudo);
    assert_eq!(
        loaded_stats
            .hist_coll
            .stable_columns()
            .iter()
            .map(|column| column.read().unwrap().histogram.id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let (mut session, skipped, _node) = node_session(one_table_catalog(table));
    // Go testkit's `h.Update`: the node's statistics cache holds the loaded
    // table, which every catalog over the node plans from.
    session.update_stats_cache_with(vec![loaded_stats]);
    let storage_stats_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let column_length_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let fail_storage_stats = Arc::new(std::sync::atomic::AtomicBool::new(false));
    session.set_table_storage_stats_provider(Arc::new(FixedStorageStatistics {
        calls: Arc::clone(&storage_stats_reads),
        column_length_requests: Arc::clone(&column_length_requests),
        fail: Arc::clone(&fail_storage_stats),
        // Three fixed-width BIGINT columns: 3,000,065 rows of 24 bytes
        // estimate a DATA_LENGTH of 72,001,560.
        values: tidb_stats_handle_cache::table_size_stats::TableRowStatsSnapshot {
            table_rows: [(table_id, 3_000_065)].into_iter().collect(),
            column_lengths: Default::default(),
        },
    }));
    assert!(skipped.is_empty(), "{skipped:?}");
    {
        let shared = session.shared_catalog();
        let catalog = shared.lock().unwrap();
        let statistics = catalog
            .table_statistics_in(&shared.stats_handle(), 130)
            .expect("loaded statistics reach the session's catalog");
        assert!(!statistics.hist_coll.pseudo);
        assert_eq!(
            statistics.hist_coll.get_column(2).unwrap().read().unwrap().histogram.ndv,
            10
        );
    }
    session
        .run(
            "SELECT TABLE_NAME FROM information_schema.tables \
             WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
        )
        .unwrap();
    assert_eq!(
        storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
        0,
        "Go skips the two restricted reads when no size column is requested"
    );
    assert!(column_length_requests.lock().unwrap().is_empty());
    let rows = result_rows(session.run(
        "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
         FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
    ));
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(3_000_065),
            Datum::Int(24),
            Datum::Int(72_001_560),
            Datum::Int(0),
        ]]
    );
    assert_eq!(
        storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(*column_length_requests.lock().unwrap(), vec![true]);
    {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        assert_eq!(
            kv_table(&catalog, "order_line").storage_statistics(),
            (0, 0, 0, 0),
            "statement-local TableSizeStats must not mutate the shared catalog"
        );
    }
    let rows = result_rows(session.run(
        "WITH s AS (SELECT TABLE_ROWS FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line') \
         SELECT TABLE_ROWS FROM s",
    ));
    assert_eq!(rows, vec![vec![Datum::Int(3_000_065)]]);
    assert_eq!(
        storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
        2,
        "the resolved memory-table scan inside a CTE must refresh"
    );
    assert_eq!(
        *column_length_requests.lock().unwrap(),
        vec![true, false],
        "TABLE_ROWS alone reads stats_meta without scanning stats_histograms"
    );
    session
        .run(
            "SELECT TABLE_NAME FROM information_schema.partitions \
             WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
        )
        .unwrap();
    assert_eq!(
        storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
        3,
        "Go's LogicalMemTable.PruneColumns does not list PARTITIONS, so \
         the scan keeps its size columns and updateStatsCacheIfNeed \
         (infoschema_reader.go:646-661) refreshes even for a \
         TABLE_NAME-only projection"
    );
    fail_storage_stats.store(true, std::sync::atomic::Ordering::Release);
    let rows = result_rows(session.run(
        "SELECT TABLE_ROWS FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
    ));
    assert_eq!(
        rows,
        vec![vec![Datum::Int(0)]],
        "a seam provider that refuses its read is served with zero size \
         columns; Go's own provider path warns and serves its cache instead"
    );
    let rows = result_rows(session.run(
        "EXPLAIN FORMAT='brief' SELECT ol_d_id, SUM(ol_amount) \
         FROM order_line WHERE ol_o_id BETWEEN 1 AND 1775 GROUP BY ol_d_id",
    ));
    let text = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let grouped = rows
        .iter()
        .enumerate()
        .find(|(row, _)| text(*row, 0).contains("HashAgg"))
        .map(|(row, _)| row)
        .expect("the GROUP BY has a physical aggregation");
    assert_eq!(text(grouped, 1), "1.00", "{rows:#?}");
}

#[test]
fn wide_sql_runs_on_cluster_storage_without_committing() {
    let (mut session, skipped, node) = node_session(base_catalog());
    // The unserved table is named in the refusal rather than silently
    // vanishing from the session's catalog.
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].name, "app.t_pending");
    assert_eq!(
        skipped[0].reason,
        "its schema state is 0 rather than public"
    );

    // The statements share one uncommitted buffer, which Go reads only
    // inside the transaction that wrote it (through a union scan).
    session.run("BEGIN").unwrap();
    assert!(matches!(
        session
            .run("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap(),
        StmtResult::Affected(3)
    ));
    // The rows are staged, not committed: the cluster is untouched and the
    // session's buffer holds the bytes.
    assert_eq!(node.rows(), 0);
    assert_eq!(session.node().buffer.len(), 3);

    // The whole read path -- scan, WHERE, an expression over columns,
    // ORDER BY -- runs over the buffer-in-front-of-snapshot storage.
    let rows =
        result_rows(session.run("SELECT id, v + id * 2 FROM t WHERE v > 10 ORDER BY id DESC"));
    let values: Vec<String> = rows
        .iter()
        .map(|row| format!("{:?}|{:?}", row[0], row[1]))
        .collect();
    assert_eq!(
        values,
        vec!["Int(3)|Int(36)".to_owned(), "Int(2)|Int(24)".to_owned()]
    );

    // A staged DELETE hides a staged row, and the cluster still sees none
    // of it.
    assert!(matches!(
        session.run("DELETE FROM t WHERE id = 2").unwrap(),
        StmtResult::Affected(1)
    ));
    assert_eq!(
        result_rows(session.run("SELECT id FROM t ORDER BY id")).len(),
        2
    );
    assert_eq!(node.rows(), 0);
}

/// A column's DEFAULT is stored twice once a table is loaded: on the
/// cluster's `ColumnInfo` (where `SHOW CREATE TABLE` and the system-row
/// writer read it) and on the `KvColumn` the driver's INSERT path reads.
/// The loader must carry it across, or an omitted column silently stores
/// NULL where TiDB stores the default -- a wrong row, written without a
/// word.
#[test]
fn a_cluster_columns_declared_default_reaches_the_row_an_insert_writes() {
    let (mut session, skipped, _node) = node_session(default_catalog());
    assert!(
        skipped.is_empty(),
        "a table with a literal DEFAULT is served"
    );
    session.run("INSERT INTO d (id) VALUES (1)").unwrap();
    assert_eq!(
        result_rows(session.run("SELECT v FROM d")),
        vec![vec![Datum::Int(7)]]
    );
}

#[test]
fn loaded_hidden_columns_preserve_native_layout_and_index_values() {
    let mut hidden = column(3, 1, "_V$_vi_0", false);
    hidden.hidden = true;
    hidden.generated_expr_string = "`v` + 1".to_owned();
    let mut generated = column(4, 3, "g", false);
    generated.generated_expr_string = "`id` + `v`".to_owned();
    let mut catalog = base_catalog();
    catalog.databases[0].tables.truncate(1);
    let info = &mut catalog.databases[0].tables[0];
    // A visible column may follow a hidden column in stored Go metadata.
    info.columns = vec![
        column(1, 0, "id", true),
        hidden,
        column(2, 2, "v", false),
        generated,
    ]
    .into();
    info.indices = vec![index(7, "vi", "_V$_vi_0", 1, -1)].into();
    let (mut session, skipped, _node) = node_session(catalog);
    assert!(skipped.is_empty(), "{skipped:?}");
    session
        .run("INSERT INTO t(id,v) VALUES (1,10),(2,20)")
        .unwrap();
    assert_eq!(
        result_rows(session.run("SELECT * FROM t ORDER BY id")),
        [
            vec![Datum::Int(1), Datum::Int(10), Datum::Int(11)],
            vec![Datum::Int(2), Datum::Int(20), Datum::Int(22)],
        ]
    );
    session.run("UPDATE t SET v=30 WHERE id=1").unwrap();
    assert_eq!(
        result_rows(session.run("SELECT id,v,g FROM t FORCE INDEX(vi) ORDER BY id")),
        [
            vec![Datum::Int(1), Datum::Int(30), Datum::Int(31)],
            vec![Datum::Int(2), Datum::Int(20), Datum::Int(22)],
        ]
    );
}

#[test]
fn loaded_generated_columns_keep_virtual_and_stored_semantics() {
    let mut virtual_column = column(2, 1, "v", false);
    virtual_column.generated_expr_string = "`id` + 10".to_owned();
    let mut stored_column = column(3, 2, "s", false);
    stored_column.generated_expr_string = "`id` + 20".to_owned();
    stored_column.generated_stored = true;
    let mut catalog = base_catalog();
    catalog.databases[0].tables.truncate(1);
    catalog.databases[0].tables[0].columns =
        vec![column(1, 0, "id", true), virtual_column, stored_column].into();
    let (mut session, skipped, node) = node_session(catalog);
    assert!(skipped.is_empty());
    // A stored generated value is decoded, not recomputed by the reader.
    node.committed.lock().unwrap().insert(
        tidb_tablecodec::table_key::encode_row_key_with_handle(
            101,
            &tidb_tablecodec::table_key::RecordHandle::Int(9),
        ),
        tidb_tablecodec::encode_table_row(None, &[Datum::Int(999)], &[3], true, None).unwrap(),
    );
    session.run("INSERT INTO t(id) VALUES (1),(2),(3)").unwrap();
    assert_eq!(
        result_rows(session.run("SELECT * FROM t ORDER BY id")),
        vec![
            vec![Datum::Int(1), Datum::Int(11), Datum::Int(21)],
            vec![Datum::Int(2), Datum::Int(12), Datum::Int(22)],
            vec![Datum::Int(3), Datum::Int(13), Datum::Int(23)],
            vec![Datum::Int(9), Datum::Int(19), Datum::Int(999)],
        ]
    );
    session
        .run("UPDATE t SET id=id+10 WHERE id>=1 AND id<=3 AND v=12")
        .unwrap();
    session
        .run("DELETE FROM t WHERE id>=1 AND id<=12 AND s=23")
        .unwrap();
    assert_eq!(
        result_rows(session.run("SELECT * FROM t ORDER BY id")),
        vec![
            vec![Datum::Int(1), Datum::Int(11), Datum::Int(21)],
            vec![Datum::Int(9), Datum::Int(19), Datum::Int(999)],
            vec![Datum::Int(12), Datum::Int(22), Datum::Int(32)],
        ]
    );
}

#[test]
fn a_clustered_composite_primary_remains_the_table_path() {
    let mut primary = index(1, "PRIMARY", "a", 0, -1);
    primary.primary = true;
    primary.unique = true;
    primary.columns = vec![
        IndexColumn {
            name: CiString::new("a"),
            offset: 0,
            length: -1,
            ..IndexColumn::default()
        },
        IndexColumn {
            name: CiString::new("b"),
            offset: 1,
            length: -1,
            ..IndexColumn::default()
        },
    ]
    .into();
    let table = TableInfo {
        id: 303,
        name: CiString::new("clustered"),
        columns: vec![
            column(1, 0, "a", true),
            column(2, 1, "b", true),
            column(3, 2, "v", false),
        ]
        .into(),
        indices: vec![primary].into(),
        is_common_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let (mut session, skipped, _node) = node_session(one_table_catalog(table));
    assert!(skipped.is_empty(), "{skipped:?}");

    let rows = result_rows(session.run("EXPLAIN FORMAT='brief' SELECT SUM(v) FROM clustered"));
    let operators: Vec<_> = rows
        .iter()
        .filter_map(|row| match &row[0] {
            Datum::Bytes(value) => Some(String::from_utf8_lossy(value)),
            _ => None,
        })
        .collect();
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("TableFullScan")),
        "{operators:#?}"
    );
    assert!(
        operators
            .iter()
            .all(|operator| !operator.contains("IndexFullScan")),
        "{operators:#?}"
    );

    let rows = result_rows(session.run("SHOW CREATE TABLE clustered"));
    let Datum::String(create) = &rows[0][1] else {
        panic!("expected CREATE TABLE text");
    };
    let create = String::from_utf8_lossy(create.bytes());
    assert_eq!(create.matches("PRIMARY KEY").count(), 1, "{create}");
}

/// A default whose value is not a literal -- `CURRENT_TIMESTAMP`, whose
/// instant belongs to each INSERT and not to the moment the catalog was
/// loaded -- refuses the table by name. Carrying it as the frozen load
/// instant, or dropping it, would both write rows TiDB does not.
#[test]
fn a_cluster_column_whose_default_is_not_a_literal_refuses_the_table() {
    let catalog = default_catalog();
    let column = catalog.databases[0].tables[0]
        .columns
        .get(1)
        .expect("the fixture has its value column");
    let mut column = column.write();
    column.default_value = ColumnDefaultValue::string_bytes(b"CURRENT_TIMESTAMP".to_vec()).into();
    column.origin_default_value = GoAny::nil();
    drop(column);
    let (_, skipped, _node) = node_session(catalog);
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].name, "app.d");
    assert!(
        skipped[0].reason.starts_with("its column v has a default"),
        "{}",
        skipped[0].reason
    );
}

fn text_rows(result: Result<StmtResult, tidb_driver_error::DriverError>) -> Vec<String> {
    result_rows(result)
        .iter()
        .map(|row| {
            row.iter()
                .map(|d| match d {
                    Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                    Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// A `utf8mb4_general_ci` column loaded FROM THE CLUSTER keeps its
/// collation all the way into execution: `cluster_table` copies the
/// stored `FieldType`, the planner's `Column` carries it, and
/// `collation_of_node` reads the collation name back off that type. So
/// `ORDER BY`, `SELECT DISTINCT` and `GROUP BY` all run case-insensitively
/// on the node, where a byte-ordered comparator would give `B, C, a, b`
/// and four groups.
#[test]
fn a_case_insensitive_cluster_column_orders_groups_and_dedups_by_its_collation() {
    let (mut session, skipped, _node) = node_session(ci_catalog());
    assert!(skipped.is_empty(), "a _ci table is served, not refused");
    session
        .run("INSERT INTO ci (id, c) VALUES (1,'a'),(2,'B'),(3,'b'),(4,'C')")
        .unwrap();
    // ORDER BY: `_ci` folds case, and equal keys keep insertion order.
    assert_eq!(
        text_rows(session.run("SELECT c FROM ci ORDER BY c")),
        vec!["a", "B", "b", "C"]
    );
    // DISTINCT: 'b' folds into 'B', so three rows, not four.
    assert_eq!(
        text_rows(session.run("SELECT DISTINCT c FROM ci ORDER BY c")),
        vec!["a", "B", "C"]
    );
    // GROUP BY: the same identity gives the folded pair one group of 2.
    assert_eq!(
        text_rows(session.run("SELECT c, COUNT(*) FROM ci GROUP BY c ORDER BY c")),
        vec!["a|Int(1)", "B|Int(2)", "C|Int(1)"]
    );
    // WHERE string equality and a self-join key derive the same collation.
    assert_eq!(
        text_rows(session.run("SELECT COUNT(*) FROM ci WHERE c = 'B'")),
        vec!["Int(2)"]
    );
    assert_eq!(
        text_rows(session.run("SELECT COUNT(*) FROM ci a, ci b WHERE a.c = b.c")),
        vec!["Int(6)"]
    );
}

#[test]
fn hash_join_preserves_the_planned_comparison_collation() {
    let (mut session, _, _node) = node_session(ci_catalog());
    session
        .run("INSERT INTO ci (id,c) VALUES (1,'B'),(2,'b')")
        .unwrap();
    for (comparison, expected) in [
        ("a.c = b.c", 4),
        ("a.c COLLATE utf8mb4_bin = b.c COLLATE utf8mb4_bin", 2),
    ] {
        assert_eq!(
            result_rows(session.run(&format!(
                "SELECT COUNT(*) FROM ci a JOIN ci b ON {comparison}"
            ))),
            vec![vec![Datum::Int(expected)]],
            "{comparison}"
        );
    }
}

#[test]
fn a_snapshot_row_is_visible_and_shadowed_by_a_staged_write() {
    let (mut session, _, node) = node_session(base_catalog());
    // A committed row, which the next transaction reads from its snapshot.
    session.run("INSERT INTO t (id, v) VALUES (9, 90)").unwrap();
    assert_eq!(node.rows(), 1);
    session.run("BEGIN").unwrap();
    assert_eq!(
        format!("{:?}", result_rows(session.run("SELECT id, v FROM t"))),
        "[[Int(9), Int(90)]]"
    );
    // The same row updated in this transaction reads back the staged value,
    // while the committed bytes underneath stay as they were.
    session.run("UPDATE t SET v = 91 WHERE id = 9").unwrap();
    assert_eq!(
        format!("{:?}", result_rows(session.run("SELECT v FROM t"))),
        "[[Int(91)]]"
    );
    assert_eq!(node.rows(), 1);
    let committed = node.snapshot();
    assert_eq!(committed.len(), 1);
    session.run("ROLLBACK").unwrap();
    assert_eq!(node.snapshot(), committed);
}

/// A table without a primary key uses the hidden `_tidb_rowid`. Separate
/// SQL sessions on one node must draw those handles from one allocator,
/// or concurrent inserts reuse the same record key and one row overwrites
/// the other.
#[test]
fn non_clustered_tables_share_the_hidden_rowid_allocator() {
    let table = TableInfo {
        id: 403,
        name: CiString::new("no_pk"),
        columns: vec![column(1, 0, "v", false)].into(),
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let (mut first, first_skipped, node) = node_session(one_table_catalog(table));
    let mut second = open_session_on(&node);
    assert!(first_skipped.is_empty());
    assert!(second.node().skipped_tables().is_empty());

    first.run("BEGIN").unwrap();
    second.run("BEGIN").unwrap();
    first.run("INSERT INTO no_pk (v) VALUES (10)").unwrap();
    second.run("INSERT INTO no_pk (v) VALUES (20)").unwrap();
    first.run("COMMIT").unwrap();
    second.run("COMMIT").unwrap();

    // Two record keys, one per session's row.
    assert_eq!(node.rows(), 2, "peer sessions must not overwrite rowids");
}

/// Prefix metadata must reach both key construction and access bounds.
/// Equal prefixes still require the full-value residual predicate.
#[test]
fn a_loaded_prefix_index_preserves_full_value_filtering() {
    let mut catalog = ci_catalog();
    catalog.databases[0].tables[0].indices = vec![index(1, "idx", "c", 1, 4)].into();
    let (mut session, skipped, _node) = node_session(catalog);
    assert!(skipped.is_empty());
    session
        .run("INSERT INTO ci(id,c) VALUES (1,'alphabet'),(2,'alphanumeric'),(3,'beta')")
        .unwrap();
    for (value, expected) in [("alphabet", 1), ("alphanumeric", 2), ("beta", 3)] {
        assert_eq!(
            result_rows(session.run(&format!(
                "SELECT id FROM ci FORCE INDEX(idx) WHERE c='{value}'"
            ))),
            vec![vec![Datum::Int(expected)]]
        );
    }
}

/// Go `TableInfo.Charset`/`Collate` survive the load. They are the table
/// default `SHOW CREATE TABLE` prints, what
/// `information_schema.tables.table_collation` reports, and the value a
/// column's own charset is compared against when the printer decides
/// whether to emit an explicit CHARACTER SET clause.
#[test]
fn a_stored_table_charset_survives_the_load() {
    let table = TableInfo {
        id: 205,
        name: CiString::new("ch"),
        columns: vec![column(1, 0, "id", true)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        charset: "latin1".to_owned(),
        collate: "latin1_bin".to_owned(),
        ..TableInfo::default()
    };
    let (session, skipped, _node) = node_session(one_table_catalog(table));
    assert!(skipped.is_empty());
    let catalog = session.shared_catalog();
    let catalog = catalog.lock().unwrap();
    let kv = kv_table(&catalog, "ch");
    assert_eq!(kv.charset().charset.name(), "latin1");
    assert_eq!(kv.charset().collation.name(), "latin1_bin");
}

/// Go `DBInfo.Charset`/`Collate` survive the load: they are what
/// `SHOW CREATE DATABASE` and `information_schema.schemata` report, and
/// the default a table created without its own charset inherits.
#[test]
fn a_stored_database_charset_survives_the_load() {
    let table = TableInfo {
        id: 204,
        name: CiString::new("t"),
        columns: vec![column(1, 0, "id", true)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    };
    let mut catalog_source = one_table_catalog(table);
    catalog_source.databases[0].info.charset = "latin1".to_owned();
    catalog_source.databases[0].info.collate = "latin1_bin".to_owned();
    let (session, skipped, _node) = node_session(catalog_source);
    assert!(skipped.is_empty());
    let catalog = session.shared_catalog();
    let catalog = catalog.lock().unwrap();
    let (name, charset) = catalog
        .database_definition("app")
        .expect("the database is in the catalog");
    assert_eq!(name, "app");
    assert_eq!(charset.charset.name(), "latin1");
    assert_eq!(charset.collation.name(), "latin1_bin");
}

/// Go `TableInfo.Comment` survives the load: `SHOW CREATE TABLE` prints
/// it and `information_schema.tables.table_comment` reports it.
#[test]
fn a_stored_table_comment_survives_the_load() {
    let table = TableInfo {
        id: 203,
        name: CiString::new("cm"),
        columns: vec![column(1, 0, "id", true)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        comment: "the stored comment".to_owned(),
        ..TableInfo::default()
    };
    let (session, skipped, _node) = node_session(one_table_catalog(table));
    assert!(skipped.is_empty());
    let catalog = session.shared_catalog();
    let catalog = catalog.lock().unwrap();
    assert_eq!(kv_table(&catalog, "cm").comment(), "the stored comment");
}

/// Go never chooses an INVISIBLE index for an access path (captured:
/// `EXPLAIN` shows a full table scan, and `USE INDEX(inv)` is
/// `[planner:1176]Key 'inv' doesn't exist in table 'iv'`). The index is
/// still loaded and still maintained by writes -- only the planner is
/// blind to it -- and the full table scan answers the query exactly.
#[test]
fn an_invisible_index_is_loaded_but_never_chosen() {
    let table = TableInfo {
        id: 202,
        name: CiString::new("iv"),
        columns: vec![column(1, 0, "id", true), column(2, 1, "a", false)].into(),
        pk_is_handle: true,
        state: SchemaState::PUBLIC,
        indices: vec![index(1, "inv", "a", 1, -1)].into(),
        ..TableInfo::default()
    };
    table
        .indices
        .get(0)
        .expect("the fixture has its index")
        .write()
        .comment = "cluster comment".to_owned();
    let invisible = table.clone_like_go();
    invisible
        .indices
        .get(0)
        .expect("the fixture has its index")
        .write()
        .invisible = true;

    for (info, expect_plan_index) in [(table, true), (invisible, false)] {
        let (mut session, skipped, _node) = node_session(one_table_catalog(info));
        assert!(skipped.is_empty());
        session
            .run("INSERT INTO iv (id, a) VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        // The answer is the same either way: with the index hidden the full
        // table scan still returns exactly the matching row.
        assert_eq!(
            format!(
                "{:?}",
                result_rows(session.run("SELECT id FROM iv WHERE a = 20"))
            ),
            "[[Int(2)]]"
        );
        // The index is loaded and maintained either way; only the planner's
        // view of it differs.
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let kv = kv_table(&catalog, "iv");
        assert_eq!(kv.indexes().len(), 1);
        assert_eq!(kv.indexes()[0].comment, "cluster comment");
        assert_eq!(kv.indexes()[0].visible, expect_plan_index);
        assert_eq!(kv.plan_indexes().count(), usize::from(expect_plan_index));
    }
}
