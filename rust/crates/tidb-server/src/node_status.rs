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

//! The status port's handlers over this node's domain: the peer coprocessor
//! service and the HTTP GLOBAL settings, assembled from what the domain's
//! session factory supplies.

use tidb_session::domain::cluster_session_node::DomainSessionSettings as _;
use std::sync::Arc;

use tidb_domain_core::domain::Domain;

/// The peer coprocessor service of this node (Go `tikvHandlerTool`'s
/// coprocessor over the node's own sessions).
pub(crate) fn peer_service(factory: &Arc<Domain>) -> crate::peer_rpc::PeerService {
    crate::peer_rpc::PeerService::new(
        factory.peer_session_builder(),
        factory.processes(),
        factory.privileges(),
        factory.server_info(),
    )
    .with_cluster_peer_client(
        tidb_session::domain::cluster_session_node::node_services(factory).cluster_peer.clone(),
    )
    .with_broadcast_query_runner(factory.broadcast_query_runner())
}

/// A free local port for a test node's status listener, which its server
/// info must name before the node is built.
#[cfg(test)]
pub(crate) fn free_status_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free local port")
        .port()
}

/// Go `startStatusHTTP` for a test node: the status listener at the
/// configured address, serving the node's coprocessor (the TiDB tasks its
/// CLUSTER_* readers send) as a running tidb-server does.
#[cfg(test)]
pub(crate) fn serve_status_for_test(
    config: &tidb_domain_core::node_config::NodeConfig,
    factory: &Arc<Domain>,
) -> crate::http_status::StatusServer {
    crate::http_status::start_status_listener_with_routes(
        &config.status_host,
        config.status_port,
        Arc::new(crate::sql_node::ConnectionTracker::default()),
        "test".into(),
        "test".into(),
        crate::http_status::StatusRoutes {
            peer: Some(peer_service(factory)),
            server_info: factory.server_info(),
            ..Default::default()
        },
    )
    .expect("the test node's status listener")
}

/// The HTTP GLOBAL settings handler of this node.
pub(crate) fn status_settings(factory: &Arc<Domain>) -> crate::http_settings::Settings {
    crate::http_settings::Settings::new(factory.global_vars(), factory.http_global_setter())
}

/// Go `StatsHandler`'s dump (the named table of the latest schema,
/// `is.TableByName`, through `DumpStatsToJSON`) and `StatsHistoryHandler`'s
/// (`DumpHistoricalStatsBySnapshot` as of the `{snapshot}` time).
pub(crate) fn stats_dump(factory: &Arc<Domain>) -> crate::http_status::StatsDump {
    let factory = Arc::clone(factory);
    Arc::new(move |database: &str, table: &str, request: crate::http_status::StatsDumpRequest<'_>| {
        let latest_table = || {
            let catalog = factory.catalog_snapshot();
            let (_, info) = catalog
                .table_info_by_name(database, table)
                .ok_or_else(|| format!("[schema:1146]Table '{database}.{table}' doesn't exist"))?;
            let info = info.read().clone();
            Ok::<_, String>(info)
        };
        let json = match request {
            crate::http_status::StatsDumpRequest::Latest { dump_partition_stats } => {
                factory.dump_stats_to_json(database, &latest_table()?, dump_partition_stats)?
            }
            crate::http_status::StatsDumpRequest::HistoricalAt(ts) => {
                factory.dump_historical_stats_by_snapshot(database, &latest_table()?, ts)?.0
            }
            crate::http_status::StatsDumpRequest::Snapshot(snapshot) => {
                let snapshot = stats_history_snapshot(&factory, snapshot)?;
                // Go `getSnapshotTableInfo`, falling back to the latest schema.
                let info = match tidb_session::domain::cluster_session_node::get_snapshot_info_schema(&factory, snapshot)
                    .map_err(|error| error.to_mysql_error().message)
                    .and_then(|catalog| {
                        catalog
                            .table_info_by_name(database, table)
                            .map(|(_, info)| info.read().clone())
                            .ok_or_else(|| format!("[schema:1146]Table '{database}.{table}' doesn't exist"))
                    }) {
                    Ok(info) => info,
                    Err(error) => {
                        tidb_logutil::bg_logger().info(
                            "fail to get snapshot TableInfo in historical stats API, switch to use latest infoschema",
                            &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                        );
                        latest_table()?
                    }
                };
                factory.dump_historical_stats_by_snapshot(database, &info, snapshot)?.0
            }
        };
        serde_json::to_string(&json).map_err(|error| error.to_string())
    })
}

/// `StatsHistoryHandler`'s first steps: `CheckHistoricalStatsEnable`, then
/// `types.ParseTime(snapshot, TypeTimestamp, 6)` in the local zone,
/// `GoTime(time.Local)` and `oracle.GoTimeToTS`.
fn stats_history_snapshot(factory: &Domain, snapshot: &str) -> Result<u64, String> {
    let enabled = factory
        .global_vars
        .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
        .map(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        .map_err(|error| format!("{error:?}"))?;
    if !enabled {
        return Err(format!("{} should be enabled", tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS));
    }
    let wrong_value = || {
        tidb_datatype::types_errors::ERR_WRONG_VALUE
            .generate(format!("Incorrect datetime value: '{snapshot}'"))
            .to_string()
    };
    let parsed = tidb_datatype::parse_time(
        snapshot,
        tidb_datatype::TimeType::Timestamp,
        6,
        false,
        false,
        false,
        &chrono::Local,
    )
    .map_err(|_| wrong_value())?;
    let at = parsed.time.go_time(&chrono::Local).map_err(|_| wrong_value())?;
    Ok(tikv_client::oracle::compose_ts(at.timestamp_millis(), 0))
}

/// Go `SchemaTableStorage`.
#[derive(serde::Serialize)]
struct SchemaTableStorage {
    table_schema: String,
    table_name: String,
    table_rows: i64,
    avg_row_length: i64,
    data_length: i64,
    max_data_length: i64,
    index_length: i64,
    data_free: i64,
}

/// Go `SchemaStorageHandler.ServeHTTP`: the named database and table must
/// exist, then `getSchemaTablesStorageInfo` selects their rows of
/// `INFORMATION_SCHEMA.TABLES` on a session of its own; one table answers its
/// row, otherwise the list.
pub(crate) fn schema_storage(factory: &Arc<Domain>) -> crate::http_status::SchemaStorage {
    let factory = Arc::clone(factory);
    Arc::new(move |database: Option<&str>, table: Option<&str>| {
        let catalog = factory.catalog_snapshot();
        let mut conditions = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut is_single = false;
        if let Some(database) = database {
            let schema = catalog.schema_by_name(database).ok_or_else(|| {
                tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[database.to_owned().into()])
                    .to_string()
            })?;
            conditions.push("TABLE_SCHEMA = %?");
            params.push(schema.read().name.original().to_owned());
            if let Some(table) = table {
                let (_, info) = catalog.table_info_by_name(database, table).ok_or_else(|| {
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .gen_with_stack_by_args(&[database.to_owned().into(), table.to_owned().into()])
                        .to_string()
                })?;
                conditions.push("TABLE_NAME = %?");
                params.push(info.read().name.original().to_owned());
                is_single = true;
            }
        }
        let mut sql = "select TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE from INFORMATION_SCHEMA.TABLES".to_owned();
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        let args: Vec<tidb_sqlescape::SqlArg<'_>> =
            params.iter().map(|param| tidb_sqlescape::SqlArg::String(param.as_bytes())).collect();
        let rows = factory
            .advanced_sys_session_pool()
            .with_session(|session| {
                session.with_session_context(|context| {
                    tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, &sql, &args)
                        .map_err(tidb_syssession::SysSessionError::new)
                })
            })
            .map_err(|error| error.to_string())?;
        // Go `chunk.Row.GetString` / `GetInt64`, which read NULL as empty.
        let text = |datum: Option<&tidb_datatype::Datum>| match datum {
            Some(tidb_datatype::Datum::Null) | None => String::new(),
            Some(datum) => datum.sql_string().unwrap_or_default(),
        };
        let int = |datum: Option<&tidb_datatype::Datum>| match datum {
            Some(tidb_datatype::Datum::Int(value)) => *value,
            Some(tidb_datatype::Datum::UInt(value)) => *value as i64,
            _ => 0,
        };
        let messages: Vec<SchemaTableStorage> = rows
            .iter()
            .map(|row| SchemaTableStorage {
                table_schema: text(row.first()),
                table_name: text(row.get(1)),
                table_rows: int(row.get(2)),
                avg_row_length: int(row.get(3)),
                data_length: int(row.get(4)),
                max_data_length: int(row.get(5)),
                index_length: int(row.get(6)),
                data_free: int(row.get(7)),
            })
            .collect();
        if is_single {
            let first = messages.first().ok_or_else(|| "the table has no storage row".to_owned())?;
            return serde_json::to_string(first).map_err(|error| error.to_string());
        }
        serde_json::to_string(&messages).map_err(|error| error.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stack() -> tidb_session::domain::unistore_node::UnistoreClusterStack {
        let config = crate::parse_node_config([
            "tidb-server",
            "--store",
            "unistore",
            "--port",
            "0",
            "--socket",
            "",
            "--auth-file",
            "/dev/null",
        ])
        .unwrap();
        let users = Arc::new(tidb_session::domain::configured_user_store::ConfiguredUserStore::parse(&format!(
            "root\t%\tmysql_native_password\t{}\n",
            crate::domain_test_support::ABC_HASH
        ))
        .unwrap());
        tidb_session::domain::unistore_node::unistore_cluster_session_stack(&config, &users, None, None).unwrap()
    }

    /// [`stack`] whose status port is served, as a running tidb-server's is.
    fn served_stack() -> (
        tidb_session::domain::unistore_node::UnistoreClusterStack,
        crate::http_status::StatusServer,
    ) {
        let status_port = free_status_port().to_string();
        let config = crate::parse_node_config([
            "tidb-server",
            "--store",
            "unistore",
            "--port",
            "0",
            "--status",
            status_port.as_str(),
            "--socket",
            "",
            "--auth-file",
            "/dev/null",
        ])
        .unwrap();
        let users = Arc::new(tidb_session::domain::configured_user_store::ConfiguredUserStore::parse(&format!(
            "root\t%\tmysql_native_password\t{}\n",
            crate::domain_test_support::ABC_HASH
        ))
        .unwrap());
        let stack =
            tidb_session::domain::unistore_node::unistore_cluster_session_stack(&config, &users, None, None)
                .unwrap();
        let status = serve_status_for_test(&config, &stack.factory);
        (stack, status)
    }

    /// A result's rows as the text the wire carries.
    fn text_rows(
        result: Result<tidb_session::StmtResult, tidb_driver_error::DriverError>,
    ) -> Vec<Vec<String>> {
        let tidb_session::StmtResult::Rows(rows) = result.unwrap() else {
            panic!("expected rows")
        };
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|datum| match datum {
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        tidb_datatype::Datum::Int(value) => value.to_string(),
                        tidb_datatype::Datum::UInt(value) => value.to_string(),
                        other => other
                            .as_raw_bytes()
                            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                            .unwrap_or_else(|| format!("{other:?}")),
                    })
                    .collect()
            })
            .collect()
    }

    /// `CREATE BINDING FROM HISTORY USING PLAN DIGEST` builds each binding from the
    /// statement the summary recorded under that plan digest and the plan hint it
    /// captured (Go `constructSQLBindOPFromPlanDigest`). Expected rows: Go 1.27 on
    /// this branch (`TestZZ` probe `BH2`).
    #[test]
    fn create_binding_from_history_uses_the_recorded_plan_hint_like_go() {
        use tidb_session::domain::cluster_session_node::DomainSessions as _;
        let (stack, _status) = served_stack();
        let mut session = stack
            .factory
            .open_session(crate::domain_test_support::session_context(1134))
            .unwrap();
        session.run("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci").unwrap();
        session.run("CREATE DATABASE IF NOT EXISTS bh1").unwrap();
        session.run("USE bh1").unwrap();
        for ddl in [
            "CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, KEY ib (b), KEY ic (c))",
            "CREATE TABLE u (a INT, b INT, KEY ia (a))",
            "INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8)",
            "INSERT INTO u VALUES (1,1),(2,2)",
            "ANALYZE TABLE t, u",
        ] {
            session.run(ddl).unwrap();
        }
        for query in [
            "SELECT * FROM t WHERE b = 2",
            "SELECT * FROM t WHERE c > 5",
            "SELECT t.a FROM t JOIN u ON t.a = u.a WHERE t.b > 0",
        ] {
            session.run(query).unwrap();
        }
        for (variable, pattern) in [
            ("@pd1", "select * from `t` where `b`%"),
            ("@pd2", "select * from `t` where `c`%"),
            ("@pd3", "select `t` . `a` from `t` join%"),
        ] {
            session.run(&format!(
                    "SET {variable} = (SELECT plan_digest FROM information_schema.statements_summary \
                     WHERE schema_name = 'bh1' AND digest_text LIKE '{pattern}' LIMIT 1)"
                )).unwrap();
        }
        session.run("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd1").unwrap();
        // The second spelling covers the same statement, so Go skips it with a note.
        session.run("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd2, @pd2").unwrap();
        assert_eq!(
            text_rows(session.run( "SHOW WARNINGS")),
            [["Warning", "1105", "37da0e4188d56394b943d77850b13c70ea2c8b962a72de5ce6700ff3a5d34d9f is ignored because it corresponds to the same SQL digest as another Plan Digest"]]
        );
        session.run("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd3").unwrap();
        assert_eq!(
            text_rows(session.run(
                "SELECT original_sql, bind_sql, default_db, status, charset, collation, source, \
                 sql_digest, plan_digest FROM mysql.bind_info WHERE source = 'history' \
                 ORDER BY original_sql",
            )),
            [
                [
                    "select * from `bh1` . `t` where `b` = ?",
                    "SELECT /*+ use_index(@`sel_1` `bh1`.`t`) no_order_index(@`sel_1` `bh1`.`t` `primary`)*/ * FROM `bh1`.`t` WHERE `b` = 2",
                    "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                    "859898f263e060dc0f7e3f98d6b1b676127b047c0e4bc332524fa75bdf1bb7aa",
                    "ddbd56d504e06fd3d48488e68ac58e7d084ddfd86eac943d609b4b70a9c6e575",
                ],
                [
                    "select * from `bh1` . `t` where `c` > ?",
                    "SELECT /*+ use_index(@`sel_1` `bh1`.`t`) no_order_index(@`sel_1` `bh1`.`t` `primary`)*/ * FROM `bh1`.`t` WHERE `c` > 5",
                    "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                    "b6d21e1ea8c2ce350cdee0dce9d442878d9437ea491a5a3179894d1b39cfb5c6",
                    "37da0e4188d56394b943d77850b13c70ea2c8b962a72de5ce6700ff3a5d34d9f",
                ],
                [
                    "select `t` . `a` from `bh1` . `t` join `bh1` . `u` on `t` . `a` = `u` . `a` where `t` . `b` > ?",
                    "SELECT /*+ merge_join(`bh1`.`u`) use_index(@`sel_1` `bh1`.`u` `ia`) order_index(@`sel_1` `bh1`.`u` `ia`) use_index(@`sel_1` `bh1`.`t`) order_index(@`sel_1` `bh1`.`t` `primary`)*/ `t`.`a` FROM `bh1`.`t` JOIN `bh1`.`u` ON `t`.`a` = `u`.`a` WHERE `t`.`b` > 0",
                    "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                    "28597854f0874fca45c632da580407490e8ba7ed63f267ab6449e5e2f06c5346",
                    "900f2884f174fbe7eef63c4e3e582e61db570e297feffbf7ec39994b8e6cc639",
                ],
            ]
        );
        assert_eq!(
            session.run("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST 'nosuchdigest'").unwrap_err().to_mysql_error().message,
            "can't find any plans for 'nosuchdigest'"
        );
        assert_eq!(
            session.run("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST ' , '").unwrap_err().to_mysql_error().message,
            "plan digest is empty"
        );
        session.run("SELECT * FROM t WHERE b = 2").unwrap();
        assert_eq!(
            text_rows(session.run( "SELECT @@last_plan_from_binding")),
            [["1"]]
        );
    }

    fn run(factory: &Arc<Domain>, sql: &str) {
        factory
            .advanced_sys_session_pool()
            .with_session(|session| {
                session.with_session_context(|context| {
                    tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, sql, &[]).map_err(tidb_syssession::SysSessionError::new)
                })
            })
            .unwrap();
    }

    /// Go `RegionHandler` for `RegionHot`: PD's hot read and write leaders,
    /// each with the table or index its range covers.
    #[test]
    fn hot_regions_name_their_tables_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        run(factory, "CREATE TABLE test.hot (id INT PRIMARY KEY, b INT, KEY ib (b))");
        let catalog_factory = Arc::clone(factory);
        let schema = move || catalog_factory.catalog_snapshot();
        let (table_id, index_id) = {
            let (_, table) = factory.catalog_snapshot().table_info_by_name("test", "hot").unwrap();
            let table = table.read();
            let index_id = table.indices.iter_deref().next().unwrap().read().id;
            (table.id, index_id)
        };
        let (address, requests) = crate::region_routes::hot_scatter_tests::pd_fixture(|_, path| {
            let (region, rate, degree) = if path.ends_with("/read") { (7, 10.0, 3) } else { (8, 20.0, 4) };
            format!(
                r#"{{"as_peer":{{}},"as_leader":{{"1":{{"total_flow_bytes":0,"regions_count":1,"statistics":[{{"store_id":1,"region_id":{region},"hot_degree":{degree},"flow_bytes":{rate},"byte_rate":{rate},"key_rate":1,"query_rate":1,"anti_count":0,"last_update_time":"2026-01-01T00:00:00Z"}}]}}}}}}"#
            )
        });
        let (record_start, record_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
        let (index_start, index_end) = tidb_tablecodec::get_table_index_key_range(table_id, index_id);
        let store = crate::region_routes::hot_scatter_tests::Store {
            pd_http: Some(
                tidb_txnkv::driver::tikv_store::new_pd_http_client_with_mock_service_discovery(
                    "test",
                    &[format!("http://{address}")],
                    Vec::new(),
                )
                .unwrap(),
            ),
            pd_addrs: Vec::new(),
            regions: vec![(7, record_start, record_end), (8, index_start, index_end)],
        };
        let answer = crate::region_routes::region_response(
            "/regions/hot",
            &store,
            &schema,
            &tidb_pd_client::ClusterSecurity::plaintext(),
        )
        .expect("served")
        .unwrap();
        let hot: serde_json::Value = serde_json::from_str(&answer).unwrap();
        assert_eq!(hot["read"][0]["region_id"], 7);
        assert_eq!(hot["read"][0]["region_metric"]["max_hot_degree"], 3);
        assert_eq!(hot["read"][0]["region_metric"]["flow_bytes"], 10);
        assert_eq!(hot["read"][0]["region_metric"]["region_count"], 0);
        assert_eq!(hot["read"][0]["db_name"], "test");
        assert_eq!(hot["read"][0]["table_name"], "hot");
        assert_eq!(hot["read"][0]["table_id"], table_id);
        assert_eq!(hot["read"][0]["index_name"], "");
        assert_eq!(hot["write"][0]["region_id"], 8);
        assert_eq!(hot["write"][0]["index_name"], "ib");
        assert_eq!(hot["write"][0]["index_id"], index_id);
        let paths: Vec<String> = requests.lock().unwrap().iter().map(|(_, path, _)| path.clone()).collect();
        assert_eq!(paths, ["/pd/api/v1/hotspot/regions/read", "/pd/api/v1/hotspot/regions/write"]);
    }

    /// Go `TestUpgrade` (`pkg/server/handler/tests`): `/upgrade/start`
    /// enters the upgrading state once the DDL owner acknowledges it,
    /// `/upgrade/show` reports the servers' versions, `/upgrade/finish`
    /// leaves it; a repeated operation says so.
    #[test]
    fn upgrade_routes_answer_like_go() {
        let stack = stack();
        let upgrade = crate::node_status::cluster_upgrade(&stack.factory).expect("the DDL is started");
        let answer = |path: &str, method: &str| {
            crate::upgrade_handler::cluster_upgrade_response(
                path,
                &format!("{method} {path} HTTP/1.1\r\n\r\n"),
                Some(&upgrade),
            )
            .expect(path)
        };
        let is_upgrading = || {
            tidb_session::sync_upgrade::is_upgrading_cluster_state(upgrade.state_syncer.as_ref()).unwrap()
        };

        assert_eq!(
            answer("/upgrade/start", "GET").unwrap_err(),
            "This API only support POST method"
        );
        // test upgrade start
        assert_eq!(answer("/upgrade/start", "POST").unwrap(), ["\"success!\""]);
        assert!(is_upgrading());
        // Do start upgrade again.
        assert_eq!(
            answer("/upgrade/start", "POST").unwrap(),
            ["\"It's a duplicated operation and the cluster is already in upgrading state.\""]
        );
        assert!(is_upgrading());

        // A user DDL submitted while upgrading is paused by the system
        // (`processJobDuringUpgrade`) until `/upgrade/finish` resumes it.
        let ddl_factory = Arc::clone(&stack.factory);
        let ddl = std::thread::spawn(move || run(&ddl_factory, "CREATE TABLE test.during_upgrade (a INT)"));
        let job_state = || -> Option<String> {
            let mut state = None;
            stack
                .factory
                .advanced_sys_session_pool()
                .with_session(|session| {
                    session.with_session_context(|context| {
                        let rows = tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, "SELECT job_meta FROM mysql.tidb_ddl_job", &[])
                            .map_err(tidb_syssession::SysSessionError::new)?;
                        state = rows.first().map(|row| {
                            let mut job = tidb_model::Job::default();
                            job.decode(row[0].go_bytes()).unwrap();
                            job.state.to_string()
                        });
                        Ok(())
                    })
                })
                .unwrap();
            state
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while job_state().as_deref() != Some("paused") {
            assert!(std::time::Instant::now() < deadline, "the job is not paused: {:?}", job_state());
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(!ddl.is_finished());

        // test upgrade show for 1 server
        let shown = answer("/upgrade/show", "POST").unwrap();
        assert_eq!(shown.len(), 2, "{shown:?}");
        let cluster_info: serde_json::Value = serde_json::from_str(&shown[0]).unwrap();
        let ddl_id = stack
            .factory
            .server_info()
            .unwrap()
            .get_local_server_info()
            .static_info
            .id
            .clone();
        assert_eq!(cluster_info["owner_id"], ddl_id);
        assert_eq!(cluster_info["servers_num"], 1);
        assert_eq!(cluster_info["upgraded_percent"], 100);
        assert_eq!(cluster_info["is_all_server_version_consistent"], true);
        assert!(cluster_info.get("all_servers_diff_info").is_none());
        assert_eq!(shown[1], "\"success!\"");
        assert!(is_upgrading());

        // test upgrade finish
        assert_eq!(answer("/upgrade/finish", "POST").unwrap(), ["\"success!\""]);
        assert!(!is_upgrading());
        // Go `ResumeAllJobsBySystem` resumed the job before the answer.
        assert_ne!(job_state().as_deref(), Some("paused"));
        ddl.join().expect("the resumed job finishes");
        assert_eq!(job_state(), None);
        // test upgrade show failed
        assert_eq!(
            answer("/upgrade/show", "POST").unwrap(),
            ["\"The cluster state is normal.\"", "\"success!\""]
        );
        // Do finish upgrade again.
        assert_eq!(
            answer("/upgrade/finish", "POST").unwrap(),
            ["\"It's a duplicated operation and the cluster is already in normal state.\""]
        );
        assert!(!is_upgrading());
        assert_eq!(answer("/upgrade/stop", "POST").unwrap_err(), "wrong operation:stop");
    }

    /// Go `TestTiFlashReplica` (`pkg/server/handler/tests`): the replica
    /// list TiFlash polls, the progress it posts, a dropped table read from
    /// the DDL history once (and not again after FLASHBACK), and a
    /// partitioned table's partitions.
    #[test]
    fn tiflash_replica_deprecated_answers_like_go() {
        let config = crate::parse_node_config([
            "tidb-server", "--store", "unistore", "--port", "0", "--socket", "", "--auth-file", "/dev/null",
        ])
        .unwrap();
        let users = Arc::new(
            tidb_session::domain::configured_user_store::ConfiguredUserStore::parse(&format!(
                "root\t%\tmysql_native_password\t{}\n",
                crate::domain_test_support::ABC_HASH
            ))
            .unwrap(),
        );
        // Go `mockstore.WithMockTiFlash(2)`.
        let stack =
            tidb_session::domain::unistore_node::unistore_cluster_session_stack_from(&config, &users, None, None, None, 2)
                .unwrap();
        let factory = &stack.factory;
        let handler = crate::node_status::flash_replica_deprecated(factory);
        #[derive(serde::Deserialize, Debug)]
        struct Info {
            id: i64,
            replica_count: u64,
            location_labels: Option<Vec<String>>,
            available: bool,
            high_priority: bool,
        }
        let fetch = || -> Vec<Info> {
            let answer = crate::tikv_handler::flash_replica_deprecated_response(
                "/tiflash/replica-deprecated",
                "GET /tiflash/replica-deprecated HTTP/1.1\r\n\r\n",
                Some(&handler),
            )
            .expect("served")
            .unwrap()
            .expect("a GET answers a list");
            serde_json::from_str(&answer).unwrap()
        };
        let post = |body: &str| -> Result<(), String> {
            crate::tikv_handler::flash_replica_deprecated_response(
                "/tiflash/replica-deprecated",
                &format!("POST /tiflash/replica-deprecated HTTP/1.1\r\n\r\n{body}"),
                Some(&handler),
            )
            .expect("served")
            .map(|answer| assert!(answer.is_none()))
        };
        run(factory, "create database tidb");
        run(factory, "create table tidb.test (a int auto_increment primary key, b varchar(20))");
        run(
            factory,
            "create table tidb.pt (a int primary key, b varchar(20), key idx(a, b)) partition by range (a) \
             (partition p0 values less than (256), partition p1 values less than (512), partition p2 values less than (1024))",
        );
        // Set GC safe point and enable GC.
        let time_before_drop = (chrono::Local::now() - chrono::Duration::hours(48)).format("%Y%m%d-%H:%M:%S %z");
        run(
            factory,
            &format!(
                "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '{time_before_drop}', ''),('tikv_gc_enable','true','') \
                 ON DUPLICATE KEY UPDATE variable_value = '{time_before_drop}'"
            ),
        );
        assert!(fetch().is_empty());

        run(factory, "alter table tidb.test set tiflash replica 2 location labels 'a','b'");
        let data = fetch();
        assert_eq!(data.len(), 1, "{data:?}");
        assert_eq!(data[0].replica_count, 2);
        assert_eq!(data[0].location_labels.as_deref(), Some(&["a".to_owned(), "b".to_owned()][..]));
        assert!(!data[0].available);
        assert!(!data[0].high_priority);

        assert_eq!(
            post(r#"{"id":184,"region_count":3,"flash_region_count":3}"#).unwrap_err(),
            "[schema:1146]Table which ID = 184 does not exist."
        );
        let table_id = factory.catalog_snapshot().table_info_by_name("tidb", "test").unwrap().1.read().id;
        assert_eq!(data[0].id, table_id);
        // A partial report records the progress instead of the cache's
        // availability.
        post(&format!(r#"{{"id":{table_id},"region_count":4,"flash_region_count":1}}"#)).unwrap();
        assert_eq!(
            tidb_domain_infosync::tiflash_manager::get_tiflash_progress_from_cache(table_id),
            Some(0.25)
        );
        assert!(!fetch()[0].available);
        assert!(post("not json").is_err());
        post(&format!(r#"{{"id":{table_id},"region_count":3,"flash_region_count":3}}"#)).unwrap();
        let check = || {
            let data = fetch();
            assert_eq!(data.len(), 1, "{data:?}");
            assert_eq!(data[0].id, table_id);
            assert_eq!(data[0].replica_count, 2);
            assert_eq!(data[0].location_labels.as_deref(), Some(&["a".to_owned(), "b".to_owned()][..]));
            assert!(data[0].available);
        };
        check();

        // Should not take effect.
        run(factory, "alter table tidb.test set tiflash replica 2 location labels 'a','b'");
        // Test for get dropped table tiflash replica info.
        run(factory, "drop table tidb.test");
        check();
        // Test unique table id replica info.
        run(factory, "flashback table tidb.test");
        check();
        run(factory, "drop table tidb.test");
        check();
        run(factory, "flashback table tidb.test");
        check();

        // Test for partition table.
        run(factory, "alter table tidb.pt set tiflash replica 2 location labels 'a','b'");
        run(factory, "alter table tidb.test set tiflash replica 0");
        let data = fetch();
        assert_eq!(data.len(), 3, "{data:?}");
        assert_eq!(data[0].replica_count, 2);
        assert!(data.iter().all(|info| !info.available));
        let pids: Vec<i64> = data.iter().map(|info| info.id).collect();

        // Mock for partition 1 replica was available.
        post(&format!(r#"{{"id":{},"region_count":3,"flash_region_count":3}}"#, pids[1])).unwrap();
        let available: Vec<bool> = fetch().iter().map(|info| info.available).collect();
        assert_eq!(available, [false, true, false]);

        // Mock for partition 0,2 replica was available.
        post(&format!(r#"{{"id":{},"region_count":3,"flash_region_count":3}}"#, pids[0])).unwrap();
        post(&format!(r#"{{"id":{},"region_count":3,"flash_region_count":3}}"#, pids[2])).unwrap();
        let available: Vec<bool> = fetch().iter().map(|info| info.available).collect();
        assert_eq!(available, [true, true, true]);


        // Test for get truncated table tiflash replica info.
        run(factory, "truncate table tidb.pt");
        run(factory, "alter table tidb.pt set tiflash replica 0");
        let data = fetch();
        assert_eq!(data.iter().map(|info| info.id).collect::<Vec<_>>(), pids);
        assert!(data.iter().all(|info| info.available), "{data:?}");
    }

    /// Go `TestExtractHandler`: `/extract_task/dump?type=plan` answers the
    /// package's file name, `isDump` streams the file, and another type is
    /// refused.
    #[test]
    fn extract_task_dump_answers_like_go() {
        let base = std::env::temp_dir().join(format!("extract-route-{}", std::process::id()));
        tidb_planner_extstore::set_global_ext_storage_for_test(Some(Arc::new(
            tidb_planner_extstore::LocalStorage::new(&base).unwrap(),
        )));
        let stack = stack();
        tidb_session::domain::cluster_session_node::setup_extract_handle(&stack.factory);
        let handle = stack.factory.get_extract_handle();
        let answer = |path: &str| {
            let response = crate::extractor_handler::extract_task_serve_response(path, handle.as_ref()).expect(path);
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let body = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
                .unwrap()
                .to_vec();
            (status, headers, body)
        };
        assert!(crate::extractor_handler::extract_task_serve_response("/extract_task", handle.as_ref()).is_none());

        let (status, _, body) = answer("/extract_task/dump?type=other");
        assert_eq!((status, String::from_utf8(body).unwrap().as_str()), (400, "unknown extract task type"));
        // The history view needs persisted statement summaries.
        let (status, _, body) = answer("/extract_task/dump?type=plan");
        assert_eq!(
            (status, String::from_utf8(body).unwrap().as_str()),
            (400, "tidb_stmt_summary_enable_persistent should be enabled for extract task")
        );
        let (status, _, body) = answer("/extract_task/dump?type=plan&begin=2024-13-01%2000:00:00");
        assert_eq!(status, 400, "{}", String::from_utf8_lossy(&body));

        let (status, headers, body) =
            answer("/extract_task/dump?type=PLAN&isHistoryView=false&begin=2000-01-01%2000:00:00&end=2100-01-01%2000:00:00");
        let name = String::from_utf8(body).unwrap();
        assert_eq!(status, 200, "{name}");
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        assert!(name.starts_with("extract_") && name.ends_with(".zip"), "{name}");
        let on_disk = std::fs::read(base.join("extract").join(&name)).unwrap();
        let files: std::collections::BTreeMap<String, Vec<u8>> =
            tidb_replayer::zip::read_zip(&on_disk).unwrap().into_iter().collect();
        assert_eq!(String::from_utf8(files["extract_meta.txt"].clone()).unwrap(), "SkipStats = \"false\"\ntaskType = \"Plan\"\n");

        let (status, headers, body) =
            answer("/extract_task/dump?type=plan&isHistoryView=0&isDump=true&isSkipStats=1");
        assert_eq!(status, 200);
        assert_eq!(headers["content-type"], "application/zip");
        let disposition = headers["content-disposition"].to_str().unwrap().to_owned();
        assert!(disposition.starts_with("attachment; filename=\"extract_") && disposition.ends_with(".zip.zip\""), "{disposition}");
        let files: std::collections::BTreeMap<String, Vec<u8>> =
            tidb_replayer::zip::read_zip(&body).unwrap().into_iter().collect();
        assert_eq!(String::from_utf8(files["extract_meta.txt"].clone()).unwrap(), "SkipStats = \"true\"\ntaskType = \"Plan\"\n");

        tidb_planner_extstore::set_global_ext_storage_for_test(None);
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Go `MvccTxnHandler` over the embedded store: a row's MVCC record by
    /// handle, by hex key and by its transaction's start ts.
    #[test]
    fn mvcc_routes_answer_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        run(factory, "CREATE TABLE test.mv (id INT PRIMARY KEY, v INT)");
        run(factory, "INSERT INTO test.mv VALUES (1, 10)");
        let store = factory.region_store.clone().expect("the embedded store has a region cache");
        let catalog_factory = Arc::clone(factory);
        let schema = move || catalog_factory.catalog_snapshot();
        let answer = |path: &str| crate::mvcc_routes::mvcc_response(path, store.as_ref(), &schema).expect(path);
        let json = |path: &str| -> serde_json::Value { serde_json::from_str(&answer(path).unwrap()).unwrap() };
        let table_id = factory
            .catalog_snapshot()
            .table_info_by_name("test", "mv")
            .map(|(_, table)| table.read().id)
            .unwrap();
        let key: String = tidb_tablecodec::table_key::encode_row_key_with_handle(
            table_id,
            &tidb_tablecodec::table_key::RecordHandle::Int(1),
        )
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect();

        let by_key = json("/mvcc/key/test/mv/1");
        assert_eq!(by_key["key"], key);
        assert!(by_key["region_id"].as_u64().unwrap() > 0);
        let writes = by_key["value"]["info"]["writes"].as_array().unwrap();
        assert_eq!(writes.len(), 1, "{by_key}");
        // A Put write: Go's `omitempty` drops the zero `type`.
        assert!(writes[0].get("type").is_none());
        let start_ts = writes[0]["start_ts"].as_u64().unwrap();
        assert!(writes[0]["commit_ts"].as_u64().unwrap() > start_ts);
        assert!(writes[0]["short_value"].is_string());

        assert_eq!(json(&format!("/mvcc/hex/{}", key.to_lowercase())), by_key);
        let by_txn = json(&format!("/mvcc/txn/{start_ts}/test/mv"));
        assert_eq!(by_txn["key"], key);
        assert_eq!(by_txn["value"]["info"]["writes"], by_key["value"]["info"]["writes"]);
        // Go walks region by region from the table's start: a write in a
        // later region is found too.
        run(factory, "SPLIT TABLE test.mv BETWEEN (0) AND (100000) REGIONS 2");
        run(factory, "INSERT INTO test.mv VALUES (60000, 20)");
        let later = json("/mvcc/key/test/mv/60000");
        let later_start_ts = later["value"]["info"]["writes"][0]["start_ts"].as_u64().unwrap();
        let later_by_txn = json(&format!("/mvcc/txn/{later_start_ts}/test/mv"));
        assert_eq!(later_by_txn["key"], later["key"]);
        assert_ne!(later_by_txn["region_id"], by_txn["region_id"]);
        // A start ts that wrote nothing: Go `WriteData(nil)`.
        assert_eq!(answer("/mvcc/txn/1/test/mv").unwrap(), "null");

        assert_eq!(answer("/mvcc/key/test/mv/abc").unwrap_err(), "strconv.ParseInt: parsing \"abc\": invalid syntax");
        assert_eq!(answer("/mvcc/hex/zz").unwrap_err(), "encoding/hex: invalid byte: U+007A 'z'");
        assert_eq!(answer("/mvcc/hex/abc").unwrap_err(), "encoding/hex: odd length hex string");
        assert_eq!(answer("/mvcc/key/test/nope/1").unwrap_err(), "[schema:1146]Table 'test.nope' doesn't exist");
        assert_eq!(answer("/mvcc/key/test/mv(p0)/1").unwrap_err(), "mv is not a partitionted table");
    }

    /// Go `TestDeleteKeyHandler`: behind `enableTestAPI`, POST deletes an
    /// index entry or a row's record key, which `ADMIN CHECK` then reports
    /// as an inconsistency.
    #[cfg(feature = "failpoints")]
    #[test]
    fn delete_key_routes_answer_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        let catalog_factory = Arc::clone(factory);
        let schema = move || catalog_factory.catalog_snapshot();
        let answer = |method: &str, path: &str| {
            crate::mvcc_routes::delete_key_response(path, &format!("{method} {path} HTTP/1.1"), &schema, &factory.transactions)
        };
        // Go `FastCheckTableExec` (`tidb_enable_fast_table_check`, the
        // default) or, with `fast` off, `CheckTableExec`.
        let check_with = |fast: bool, sql: &str| {
            factory
                .advanced_sys_session_pool()
                .with_session(|session| {
                    session.with_session_context(|context| {
                        let fast = format!("set @@tidb_enable_fast_table_check = {}", u8::from(fast));
                        tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, &fast, &[])
                            .and_then(|_| tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, sql, &[]))
                            .map_err(tidb_syssession::SysSessionError::new)
                    })
                })
                .map(|_| ())
                .map_err(|error| error.to_string())
        };
        let check = |sql: &str| check_with(false, sql);
        run(factory, "create database tidb");
        run(factory, "create table tidb.delete_idx (a int primary key, b int, key idx_ab(a, b))");
        run(factory, "insert into tidb.delete_idx values (1, 2)");
        run(factory, "create table tidb.delete_row (a int primary key, b int, key idx_b(b))");
        run(factory, "insert into tidb.delete_row values (1, 2)");

        assert!(answer("POST", "/test/delete/rowkey/tidb/delete_row?handle=1").is_none(), "no test API without enableTestAPI");
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/server/enableTestAPI", "return").unwrap();
        assert_eq!(
            answer("GET", "/test/delete/rowkey/tidb/delete_row?handle=1").unwrap().unwrap_err(),
            "This api only support POST method"
        );
        assert_eq!(
            answer("POST", "/test/delete/indexkey/tidb/delete_idx/nope?handle=1&a=1&b=2").unwrap().unwrap_err(),
            "Index nope not found! not found"
        );

        check("admin check index tidb.delete_idx idx_ab").unwrap();
        // Go `HandleTask`'s `kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)`
        // on every read of the checked table (background workers read their
        // own tables meanwhile).
        let (_, delete_idx) = factory.catalog_snapshot().table_info_by_name("tidb", "delete_idx").unwrap();
        let table_prefix = tidb_tablecodec::table_key::gen_table_prefix(delete_idx.read().id);
        let sources: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let recorded = Arc::clone(&sources);
        stack.rpc.set_send_hook(Some(Arc::new(move |request| {
            use tikv_client::proto::coprocessor;
            if let Some(request) = request.as_any().downcast_ref::<coprocessor::Request>() {
                if request.ranges.iter().any(|range| range.start.starts_with(&table_prefix)) {
                    recorded
                        .lock()
                        .unwrap()
                        .push(request.context.as_ref().map(|context| context.request_source.clone()).unwrap_or_default());
                }
            }
        })));
        check_with(true, "admin check table tidb.delete_idx").unwrap();
        stack.rpc.set_send_hook(None);
        let sources = std::mem::take(&mut *sources.lock().unwrap());
        assert!(!sources.is_empty() && sources.iter().all(|source| source == "internal_admin"), "{sources:?}");
        // Go `kv.WithInternalSourceType(ctx, kv.InternalTxnTools)`.
        let prewrites: Arc<std::sync::Mutex<Vec<(String, Vec<Vec<u8>>)>>> = Arc::default();
        let recorded = Arc::clone(&prewrites);
        stack.rpc.set_send_hook(Some(Arc::new(move |request| {
            use tikv_client::proto::kvrpcpb;
            if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
                recorded.lock().unwrap().push((
                    request.context.as_ref().map(|context| context.request_source.clone()).unwrap_or_default(),
                    request.mutations.iter().map(|mutation| mutation.key.clone()).collect(),
                ));
            }
        })));
        let deleted: serde_json::Value = serde_json::from_str(
            &answer("POST", "/test/delete/indexkey/tidb/delete_idx/idx_ab?handle=1&a=1&b=2").unwrap().unwrap(),
        )
        .unwrap();
        stack.rpc.set_send_hook(None);
        let deleted_key: Vec<u8> = (0..deleted["key"].as_str().unwrap().len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&deleted["key"].as_str().unwrap()[i..i + 2], 16).unwrap())
            .collect();
        let sources: Vec<String> = std::mem::take(&mut *prewrites.lock().unwrap())
            .into_iter()
            .filter(|(_, keys)| keys.contains(&deleted_key))
            .map(|(source, _)| source)
            .collect();
        assert_eq!(sources, ["internal_tools"]);
        // Go `admin.CheckIndicesCount`'s error is `CheckTableExec`'s for
        // `ADMIN CHECK INDEX`.
        let error = check("admin check index tidb.delete_idx idx_ab").unwrap_err();
        assert!(error.contains("table count 1 != index(idx_ab) count 0"), "{error}");
        // `FastCheckTableExec` names the row whose entry is gone.
        let error = check_with(true, "admin check index tidb.delete_idx idx_ab").unwrap_err();
        assert!(
            error.contains(
                "data inconsistency in table: delete_idx, index: idx_ab, handle: 1, \
                 index-values:\"\" != record-values:\"handle: 1, values: [KindInt64 1 KindInt64 2]\""
            ),
            "{error}"
        );
        let (_, table) = factory.catalog_snapshot().table_info_by_name("tidb", "delete_idx").unwrap();
        let table_id = table.read().id;
        let index_id = table.read().indices.iter_deref().find(|index| index.read().name.original() == "idx_ab").unwrap().read().id;
        let key = deleted["key"].as_str().unwrap();
        let prefix: String = tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index_id)
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect();
        assert!(key.starts_with(&prefix), "{key}");

        check("admin check table tidb.delete_row").unwrap();
        let deleted: serde_json::Value =
            serde_json::from_str(&answer("POST", "/test/delete/rowkey/tidb/delete_row?handle=1").unwrap().unwrap()).unwrap();
        let error = check("admin check table tidb.delete_row").unwrap_err();
        assert!(error.contains("data inconsistency"), "{error}");
        let error = check_with(true, "admin check table tidb.delete_row").unwrap_err();
        assert!(
            error.contains(
                "data inconsistency in table: delete_row, index: idx_b, handle: 1, \
                 index-values:\"handle: 1, values: [KindInt64 2]\" != record-values:\"\""
            ),
            "{error}"
        );
        check_with(true, "admin check table tidb.delete_idx").unwrap_err();
        let (_, table) = factory.catalog_snapshot().table_info_by_name("tidb", "delete_row").unwrap();
        let key: String = tidb_tablecodec::table_key::encode_row_key_with_handle(
            table.read().id,
            &tidb_tablecodec::table_key::RecordHandle::Int(1),
        )
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect();
        assert_eq!(deleted["key"], key);
        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/server/enableTestAPI");
    }

    /// Go `TestHandler`'s `gcworker.RunResolveLocks` over the embedded store
    /// (the deps `/test/gc/resolvelock` resolves with): a lock older than the
    /// safe point is resolved; one newer is left.
    #[test]
    fn gc_resolve_locks_resolves_the_embedded_stores_locks_like_go() {
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let stack = stack();
        let ts = || stack.factory.transactions.fresh_timestamp().unwrap();
        let handler = Arc::clone(stack.rpc.handler());
        let prewrite = |key: &[u8], start: u64| {
            handler
                .lock()
                .unwrap()
                .store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation { op: KvrpcOp::Put as i32, key: key.to_vec(), value: b"v".to_vec(), ..KvrpcMutation::default() }],
                    primary_lock: key.to_vec(),
                    start_version: start,
                    lock_ttl: 1,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
                })
                .unwrap();
        };
        let locks = || {
            let mut keys: Vec<Vec<u8>> = handler
                .lock()
                .unwrap()
                .store
                .scan_lock(&[], &[], u64::MAX, 16)
                .into_iter()
                .map(|lock| lock.key)
                .collect();
            keys.sort();
            keys
        };
        prewrite(b"x_gc_old", ts());
        let safe_point = ts();
        prewrite(b"x_gc_new", ts());
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(locks(), [b"x_gc_new".to_vec(), b"x_gc_old".to_vec()]);
        let deps = tidb_gcworker::node::gc_worker_deps(&stack.factory, stack.opener.store(), Arc::new(stack.opener.clone()));
        tidb_gcworker::run_resolve_locks(deps, safe_point, "testGCWorker", 3).unwrap();
        assert_eq!(locks(), [b"x_gc_new".to_vec()]);
    }

    /// Go `DDLHookHandler`: `ctc_hook` holds each reorganizing state of a
    /// `MODIFY COLUMN` job half a second (`beforeRunOneJobStep`), so the
    /// three states take at least 1.5s; `default_hook` removes it.
    #[cfg(feature = "failpoints")]
    #[test]
    fn ddl_hook_route_holds_column_type_changes_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        let hook = |method: &str, body: &str| {
            crate::http_status::ddl_hook_answer(
                "/test/ddl/hook",
                &format!(
                    "{method} /test/ddl/hook HTTP/1.1\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n{body}"
                ),
            )
            .unwrap()
        };
        run(factory, "create table test.hooked (a int)");
        run(factory, "insert into test.hooked values (1)");
        assert_eq!(hook("GET", "ddl_hook=ctc_hook").unwrap_err(), "This api only support POST method");
        assert_eq!(hook("POST", "ddl_hook=ctc_hook").unwrap(), "\"success!\"");
        assert!(tidb_failpoint::list().iter().any(|(name, _)| name == "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"));
        let start = std::time::Instant::now();
        run(factory, "alter table test.hooked modify column a varchar(10)");
        assert!(start.elapsed() >= std::time::Duration::from_millis(1500), "{:?}", start.elapsed());
        assert_eq!(hook("POST", "ddl_hook=default_hook").unwrap(), "\"success!\"");
        assert!(!tidb_failpoint::list().iter().any(|(name, _)| name == "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"));
    }

    /// Go `TestGetIndexMVCC` and `TestGetTableMVCC`'s query-string forms:
    /// an index entry (and its temporary entry) by the index's column values
    /// and handle, NULL and empty values, a clustered table's row and index
    /// by its primary key's values, a partition's index, and `decode`.
    #[test]
    fn mvcc_index_and_clustered_key_routes_answer_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        // Go `prepareData`.
        run(factory, "create database tidb");
        run(factory, "create table tidb.test (a int auto_increment primary key, b varchar(20))");
        run(factory, "insert tidb.test values (1, 1)");
        run(factory, "update tidb.test set b = b + 1 where a = 1");
        run(factory, "insert tidb.test values (2, 2)");
        run(factory, "insert tidb.test (a) values (3)");
        run(factory, "insert tidb.test values (4, '')");
        run(factory, "alter table tidb.test add index idx1 (a, b)");
        run(factory, "alter table tidb.test drop index idx1");
        run(factory, "alter table tidb.test add index idx1 (a, b)");
        run(factory, "alter table tidb.test add unique index idx2 (a, b)");
        run(
            factory,
            "create table tidb.pt (a int primary key, b varchar(20), key idx(a, b)) partition by range (a) \
             (partition p0 values less than (256), partition p1 values less than (512), partition p2 values less than (1024))",
        );
        run(factory, "insert into tidb.pt values (42, '123'), (256, 'b'), (666, 'def')");
        run(factory, "create table tidb.t (a double, b varchar(20), c int, primary key(a,b) clustered, key idx(c))");
        run(factory, "insert into tidb.t values(1.1,'111',1),(2.2,'222',2)");
        // A trailing space the new collation's PAD SPACE key drops.
        run(factory, "insert into tidb.t values(3.3,'ab ',3)");

        let store = factory.region_store.clone().expect("the embedded store has a region cache");
        let catalog_factory = Arc::clone(factory);
        let schema = move || catalog_factory.catalog_snapshot();
        let answer = |path: &str| crate::mvcc_routes::mvcc_response(path, store.as_ref(), &schema).expect(path);
        let json = |path: &str| -> serde_json::Value {
            serde_json::from_str(&answer(path).unwrap_or_else(|error| panic!("{path}: {error}"))).unwrap()
        };
        // Go `decodeKeyMvcc`.
        let decode_key_mvcc = |path: &str, valid: bool| {
            let data = json(path);
            let data = data.as_array().unwrap_or_else(|| panic!("{path}: {data}"));
            assert_eq!(data.len(), 2, "{path}");
            let info = &data[0]["value"]["info"];
            if valid {
                assert!(!info["writes"].as_array().unwrap_or_else(|| panic!("{path}: {info}")).is_empty());
            } else {
                assert!(info.is_object(), "{path}: {info}");
                assert!(info.get("lock").is_none() && info.get("writes").is_none() && info.get("values").is_none());
            }
            data[0]["key"].as_str().unwrap().to_owned()
        };
        // tests for normal index key
        let idx1_key = decode_key_mvcc("/mvcc/index/tidb/test/idx1/1?a=1&b=2", true);
        decode_key_mvcc("/mvcc/index/tidb/test/idx2/1?a=1&b=2", true);
        // tests for index key which includes null
        decode_key_mvcc("/mvcc/index/tidb/test/idx1/3?a=3&b", true);
        decode_key_mvcc("/mvcc/index/tidb/test/idx2/3?a=3&b", true);
        // tests for index key which includes empty string
        decode_key_mvcc("/mvcc/index/tidb/test/idx1/4?a=4&b=", true);
        decode_key_mvcc("/mvcc/index/tidb/test/idx2/3?a=4&b=", true);
        decode_key_mvcc("/mvcc/index/tidb/t/idx?a=1.1&b=111&c=1", true);
        // tests for wrong key
        decode_key_mvcc("/mvcc/index/tidb/test/idx1/5?a=5&b=1", false);
        decode_key_mvcc("/mvcc/index/tidb/test/idx2/5?a=5&b=1", false);
        // tests for missing column value
        assert_eq!(
            answer("/mvcc/index/tidb/test/idx1/1?a=1").unwrap_err(),
            "Missing value for index column b. bad request"
        );
        assert_eq!(
            answer("/mvcc/index/tidb/test/idx2/1?a=1").unwrap_err(),
            "Missing value for index column b. bad request"
        );
        decode_key_mvcc("/mvcc/index/tidb/pt(p2)/idx/666?a=666&b=def", true);

        // The index key is Go's: the table's idx1 entry for (1, '2') and
        // handle 1, then its temporary index key.
        let (table_id, idx1_id) = {
            let (_, table) = factory.catalog_snapshot().table_info_by_name("tidb", "test").unwrap();
            let table = table.read();
            let idx1 = table.indices.iter_deref().find(|index| index.read().name.lowercase() == "idx1").unwrap();
            let id = idx1.read().id;
            (table.id, id)
        };
        let mut expected = tidb_tablecodec::table_key::encode_index_seek_key(
            table_id,
            idx1_id,
            &tidb_codec::encode_key(&[
                tidb_datatype::Datum::Int(1),
                tidb_datatype::Datum::new_string_datum("2"),
            ])
            .unwrap(),
        );
        expected.push(tidb_codec::INT_FLAG);
        tidb_codec::encode_int(&mut expected, 1);
        assert_eq!(idx1_key, expected.iter().map(|byte| format!("{byte:02X}")).collect::<String>());
        let both = json("/mvcc/index/tidb/test/idx1/1?a=1&b=2");
        tidb_tablecodec::index_key_to_temp_index_key(&mut expected).unwrap();
        assert_eq!(both[1]["key"], expected.iter().map(|byte| format!("{byte:02X}")).collect::<String>());

        // Go `TestGetTableMVCC`'s decode and clustered forms.
        let decoded = json("/mvcc/key/tidb/test/1?decode=true");
        assert!(decoded["key"].is_string() && decoded["info"].is_object(), "{decoded}");
        assert!(decoded.get("decode_error").is_none(), "{decoded}");
        // Both versions of the row, keyed by their start ts; the int handle
        // `a` is the key, not a stored column.
        let data = decoded["data"].as_object().unwrap();
        let mut rows: Vec<_> = data.values().cloned().collect();
        rows.sort_by_key(|row| row["b"].to_string());
        assert_eq!(rows, [serde_json::json!({"b": "1"}), serde_json::json!({"b": "2"})], "{decoded}");
        let decoded = json("/mvcc/key/tidb/pt(p0)/42?decode=true");
        assert_eq!(decoded["data"].as_object().unwrap().values().next().unwrap()["b"], "123");
        assert_eq!(
            answer("/mvcc/key/tidb/t/42").unwrap_err(),
            "For clustered index tables, please use query strings to specify the column values. bad request"
        );
        assert_eq!(answer("/mvcc/key/tidb/t?a=1.1").unwrap_err(), "Missing value for index column b. bad request");
        let decoded = json("/mvcc/key/tidb/t?a=1.1&b=111&decode=1");
        let row = decoded["data"].as_object().unwrap().values().next().unwrap().clone();
        assert_eq!(row["c"], "1", "{decoded}");
        // The clustered key is encoded under the new collation, as the row's
        // key is.
        let padded = json("/mvcc/key/tidb/t?a=3.3&b=ab%20&decode=1");
        let row = padded["data"].as_object().unwrap_or_else(|| panic!("{padded}")).values().next().unwrap().clone();
        assert_eq!(row["c"], "3", "{padded}");
        // Without `decode`, the plain record.
        let plain = json("/mvcc/key/tidb/t?a=1.1&b=111");
        assert_eq!(plain["key"], decoded["key"]);
        assert!(plain["value"]["info"]["writes"].is_array());
        assert_eq!(
            answer("/mvcc/key/tidb/test?a=1").unwrap_err(),
            "Clustered common handle not found. bad request"
        );
        assert_eq!(
            answer("/mvcc/index/tidb/test/nope/1?a=1&b=2").unwrap_err(),
            "Index nope not found! not found"
        );
        assert_eq!(
            answer("/mvcc/index/tidb/test/idx1/1?a=1&a=2&b=2").unwrap_err(),
            "Invalid query form for column 'a', it's values are [1 2]. Column value should be unique for one index record. bad request"
        );
        assert_eq!(answer("/mvcc/index/tidb/test/idx1/1?a=%zz").unwrap_err(), "invalid URL escape \"%zz\"");
    }

    /// Go `RegionHandler` and `TableHandler`'s `regions` / `ranges` over the
    /// embedded store's region cache: the table's regions carry PD's meta,
    /// a region's frames name the table, ranges are the table's key spans.
    #[test]
    fn region_routes_answer_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        run(factory, "CREATE TABLE test.rt (id INT PRIMARY KEY, b INT, KEY ib (b))");
        run(factory, "SPLIT TABLE test.rt BETWEEN (0) AND (100000) REGIONS 2");
        let store = factory.region_store.clone().expect("the embedded store has a region cache");
        let catalog_factory = Arc::clone(factory);
        let schema = move || catalog_factory.catalog_snapshot();
        let security = tidb_pd_client::ClusterSecurity::plaintext();
        let answer = |path: &str| {
            crate::region_routes::region_response(path, store.as_ref(), &schema, &security).expect(path)
        };
        let json = |path: &str| -> serde_json::Value { serde_json::from_str(&answer(path).unwrap()).unwrap() };

        let table_id = factory
            .catalog_snapshot()
            .table_info_by_name("test", "rt")
            .map(|(_, table)| table.read().id)
            .unwrap();
        let regions = json("/tables/test/rt/regions");
        assert_eq!(regions["name"], "rt");
        assert_eq!(regions["id"], table_id);
        let record_regions = regions["record_regions"].as_array().unwrap();
        assert_eq!(record_regions.len(), 2, "{regions}");
        assert!(record_regions[0]["leader"]["store_id"].as_u64().unwrap() > 0);
        assert!(record_regions[0]["region_epoch"]["version"].as_u64().unwrap() > 0);
        assert_eq!(regions["indices"][0]["name"], "ib");
        assert_eq!(regions["indices"][0]["id"], 1);
        // The split is at a record key, so the index's range is in one
        // region (the split also cuts at the record prefix).
        let index_regions = regions["indices"][0]["regions"].as_array().unwrap();
        assert_eq!(index_regions.len(), 1, "{regions}");

        // The second record region starts at the split key: its first frame
        // is that record of the table.
        let region_id = record_regions[1]["region_id"].as_u64().unwrap();
        let detail = json(&format!("/regions/{region_id}"));
        assert_eq!(detail["region_id"], region_id);
        let frames = detail["frames"].as_array().unwrap();
        assert!(
            frames.iter().any(|frame| frame["db_name"] == "test"
                && frame["table_name"] == "rt"
                && frame["table_id"] == table_id
                && frame["is_record"] == true
                && frame["record_id"] == 50000),
            "{detail}"
        );
        // Go `strconv.ParseInt(id, 0, 64)`: a hex region id is a region id.
        assert_eq!(json(&format!("/regions/{region_id:#x}"))["region_id"], region_id);
        assert_eq!(
            answer("/regions/abc").unwrap_err(),
            "strconv.ParseInt: parsing \"abc\": invalid syntax"
        );
        assert!(json("/regions/meta").as_array().is_some_and(|metas| !metas.is_empty()));

        let ranges = json("/tables/test/rt/ranges");
        let hex = |key: Vec<u8>| key.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        assert_eq!(ranges["table"]["start_key_hex"], hex(tidb_tablecodec::table_key::encode_table_prefix(table_id)));
        assert_eq!(ranges["record"]["end_key_hex"], hex(tidb_tablecodec::table_key::encode_table_prefix(table_id + 1)));
        assert_eq!(
            ranges["indices"]["ib"]["start_key_hex"],
            hex(tidb_tablecodec::table_key::encode_table_index_prefix(table_id, 1))
        );
        assert_eq!(
            answer("/tables/test/nope/regions").unwrap_err(),
            "[schema:1146]Table 'test.nope' doesn't exist"
        );
        // Go `/regions/hot` needs the store's PD HTTP client, which the
        // embedded store has none of.
        assert_eq!(answer("/regions/hot").unwrap_err(), tidb_store_helper::PD_HTTP_CLIENT_UNAVAILABLE);
        // Go `getPDAddr`: no PD addresses.
        assert_eq!(
            answer("/tables/test/rt/scatter").unwrap_err(),
            "scatter record error: pd unavailable"
        );
    }

    /// Go `SchemaStorageHandler` and `StatsHistoryHandler` over a node:
    /// the named database and table must exist; the history route needs
    /// `tidb_enable_historical_stats` and a parsable snapshot.
    #[test]
    fn schema_storage_and_stats_history_answer_like_go() {
        let stack = stack();
        let factory = &stack.factory;
        run(factory, "CREATE TABLE test.stored (a INT)");
        let storage = schema_storage(factory);
        let one = storage(Some("test"), Some("stored")).unwrap();
        assert!(one.starts_with("{\"table_schema\":\"test\",\"table_name\":\"stored\",\"table_rows\":"), "{one}");
        assert!(storage(Some("test"), None).unwrap().starts_with('['));
        assert_eq!(storage(Some("nodb"), None).unwrap_err(), "[schema:1049]Unknown database 'nodb'");
        assert_eq!(
            storage(Some("test"), Some("nope")).unwrap_err(),
            "[schema:1146]Table 'test.nope' doesn't exist"
        );

        let dump = stats_dump(factory);
        let snapshot = crate::http_status::StatsDumpRequest::Snapshot;
        run(factory, "SET GLOBAL tidb_enable_historical_stats = OFF");
        assert_eq!(
            dump("test", "stored", snapshot("2026-11-05 01:00:00")).unwrap_err(),
            "tidb_enable_historical_stats should be enabled"
        );
        // Go checks the switch before it parses the snapshot.
        assert_eq!(
            dump("test", "stored", snapshot("xyz")).unwrap_err(),
            "tidb_enable_historical_stats should be enabled"
        );
        run(factory, "SET GLOBAL tidb_enable_historical_stats = ON");
        assert_eq!(
            dump("test", "stored", snapshot("xyz")).unwrap_err(),
            "[types:1292]Incorrect datetime value: 'xyz'"
        );
        dump("test", "stored", snapshot(&chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string())).unwrap();
    }
}

/// Go `TTLJobTriggerHandler.ServeHTTP`'s `dom.TTLJobManager().GetCommandCli()`
/// and `ttlcient.TriggerNewTTLJob`, read at request time as Go reads the
/// domain's manager.
/// Go `NewClusterUpgradeHandler(store)`'s reach: the DDL's state syncer, the
/// domain's etcd client, sessions over the domain, the server-info syncer
/// and the DDL owner manager. `None` before the DDL starts.
pub fn cluster_upgrade(factory: &Arc<Domain>) -> Option<crate::upgrade_handler::ClusterUpgrade> {
    let sess_pool = Arc::new(tidb_session::ddl_session_pool::NodeDdlSessionPool::default());
    sess_pool.bind(factory);
    Some(crate::upgrade_handler::ClusterUpgrade {
        state_syncer: factory.ddl.state_syncer()?,
        etcd_cli: factory
            .etcd_client()
            .map(|client| client as Arc<dyn tidb_owner::OwnerStore>),
        sess_pool,
        server_info: factory.server_info()?,
        ddl_owner: factory.ddl.owner_manager()?,
    })
}

/// Go `FlashReplicaDeprecatedHandler{tikvHandler}`'s reach: the domain and
/// sessions over it.
pub fn flash_replica_deprecated(factory: &Arc<Domain>) -> crate::tikv_handler::FlashReplicaDeprecated {
    let sess_pool = Arc::new(tidb_session::ddl_session_pool::NodeDdlSessionPool::default());
    sess_pool.bind(factory);
    crate::tikv_handler::FlashReplicaDeprecated {
        domain: Arc::downgrade(factory),
        sess_pool,
    }
}

pub fn ttl_job_trigger(factory: &Arc<Domain>) -> crate::http_status::TtlJobTrigger {
    let domain = Arc::downgrade(factory);
    Arc::new(move |db_name: &str, table_name: &str| {
        let domain = domain.upgrade().ok_or_else(|| "the domain is closed".to_owned())?;
        let manager = domain
            .ttl_job_manager()
            .ok_or_else(|| "the TTL job manager is not started".to_owned())?;
        let cli = manager.get_command_cli();
        let response = tidb_ttl_client::command::trigger_new_ttl_job(
            &tidb_ttl_client::Context::background(),
            cli.as_ref(),
            db_name,
            table_name,
        )?;
        serde_json::to_string(&response).map_err(|error| error.to_string())
    })
}

/// Go `StatsPriorityQueueHandler`'s `sh.do.StatsHandle().GetPriorityQueueSnapshot()`.
pub fn stats_priority_queue(factory: &Arc<Domain>) -> crate::http_status::StatsPriorityQueue {
    let domain = Arc::downgrade(factory);
    Arc::new(move || {
        let domain = domain.upgrade().ok_or_else(|| "the domain is closed".to_owned())?;
        let snapshot = domain.get_priority_queue_snapshot()?;
        serde_json::to_string(&snapshot).map_err(|error| error.to_string())
    })
}

/// Go `Server.UpdateProcessCPUTime`, over the domain's client list.
struct ProcessCpuTimeUpdater(tidb_sessmgr::ProcessRegistry);

impl tidb_topsql_collector::ProcessCpuTimeUpdater for ProcessCpuTimeUpdater {
    fn update_process_cpu_time(&self, conn_id: u64, sql_id: u64, cpu_time: std::time::Duration) {
        self.0.update_process_cpu_time(conn_id, sql_id, cpu_time);
    }
}

/// Go `main`'s `topsql.SetupTopProfiling(keyspace.GetKeyspaceNameBytesBySettings(), svr, dom)`,
/// before `svr.Run`. The domain binds itself as the RU-version provider when
/// its resource-groups controller starts, so none is passed here.
pub(crate) fn setup_top_profiling(factory: &Arc<Domain>) {
    tidb_topsql::setup_top_profiling(
        tidb_keyspace::get_keyspace_name_bytes_by_settings().map(<[u8]>::to_vec).unwrap_or_default(),
        Arc::new(ProcessCpuTimeUpdater(factory.processes())),
        None,
    );
}

/// Go `ProfileHandler`'s `session.CreateSession(h.Store)`: one system session
/// per request, its restricted SQL the metric profile's queries.
pub(crate) fn metrics_profile_session(factory: &Arc<Domain>) -> crate::tikv_handler::MetricsProfileSession {
    use tidb_session::domain::cluster_session_node::DomainSessions as _;
    let factory = Arc::clone(factory);
    Arc::new(move |body| {
        let mut session = factory.open_system_session().map_err(|error| error.message)?;
        // Go `ProfileBuilder`'s `kv.WithInternalSourceType(...,
        // kv.InternalTxnOthers)`.
        session.set_internal_request_source(tidb_txnkv::INTERNAL_TXN_OTHERS);
        let session = std::cell::RefCell::new(session);
        body(&|sql| {
            session
                .borrow_mut()
                .exec_restricted_sql_rows(tidb_txnkv::INTERNAL_TXN_OTHERS, sql)
                .map_err(|error| error.to_mysql_error().message)
        })
    })
}
