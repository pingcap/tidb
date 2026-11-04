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
        let sql = tidb_sqlescape::escape_sql_text(&sql, &args).map_err(|error| format!("{error:?}"))?;
        let rows = factory
            .advanced_sys_session_pool()
            .with_session(|session| {
                session.with_session_context(|context| {
                    context.query_internal(&sql).map_err(tidb_syssession::SysSessionError::new)
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

    fn run(factory: &Arc<Domain>, sql: &str) {
        factory
            .advanced_sys_session_pool()
            .with_session(|session| {
                session.with_session_context(|context| {
                    context.query_internal(sql).map_err(tidb_syssession::SysSessionError::new)
                })
            })
            .unwrap();
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
