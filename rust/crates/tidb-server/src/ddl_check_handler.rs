// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `DDLCheckHandler` and `collectRecordSetRows` in `tikv_handler.go`.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use tidb_domain_core::domain::Domain;
use tidb_session::domain::cluster_session_node::DomainSessions;
use tidb_session::domain::resultset_source::ResultSetSource;

/// Go `collectRecordSetRows` / `session.ResultSetToStringSlice`.
fn collect_record_set_rows(
    result: &mut tidb_session::domain::session_api::QueryResult<'_>,
) -> Result<Vec<Vec<String>>, String> {
    let source = result.source();
    let collected = (|| {
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(1024).map_err(|error| error.to_string())?;
            if batch.is_empty() {
                break;
            }
            for row in batch {
                rows.push(
                    row.iter()
                        .map(|datum| {
                            if matches!(datum, tidb_datatype::Datum::Null) {
                                Ok("<nil>".to_owned())
                            } else {
                                datum.sql_string().map_err(|error| error.to_string())
                            }
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                );
            }
        }
        source.finish().map_err(|error| error.to_string())?;
        Ok(rows)
    })();
    let closed = source.close().map_err(|error| error.to_string());
    match collected {
        Ok(rows) => closed.map(|()| rows),
        Err(error) => Err(error),
    }
}

/// Go `DDLCheckHandler.ServeHTTP`: execution errors are diagnostic results;
/// session construction and record-set collection errors are HTTP errors.
pub(crate) fn serve(
    path: &str,
    request: &str,
    domain: Option<&std::sync::Arc<Domain>>,
) -> Option<axum::response::Response> {
    if !tidb_config_kerneltype::is_next_gen() {
        return None;
    }
    let domain = domain?;
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    let ["", "ddl", "check", db, table, index] = route.split('/').collect::<Vec<_>>()[..] else {
        return None;
    };
    let result = (|| {
        if request.split_whitespace().next() != Some("POST") {
            return Err("This api only support POST method".to_owned());
        }
        // Go's mux decodes path parameters before handing them to the handler.
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8()
                .map(|s| s.into_owned())
                .map_err(|e| e.to_string())
        };
        let (db, table, index) = (decode(db)?, decode(table)?, decode(index)?);
        if db.is_empty() || table.is_empty() || index.is_empty() {
            return Err("db, table and index are required".into());
        }
        let mut session = domain
            .open_system_session()
            .map_err(|error| error.to_string())?;
        session
            .set_internal_system_var("tidb_enable_fast_table_check", "ON")
            .map_err(|error| error.to_string())?;
        let quote = |s: &str| format!("`{}`", s.replace('`', "``"));
        let sql = format!(
            "admin check index {}.{} {}",
            quote(&db),
            quote(&table),
            quote(&index)
        );
        let executed = session
            .parse_at_statement_boundary(&sql)
            .map_err(|error| {
                tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(error.to_string())
            })
            .and_then(|stmt| session.node_conn().execute_stmt(&sql, &stmt));
        let mut result =
            serde_json::json!({"db": db, "table": table, "index": index, "check_sql": sql});
        match executed {
            Err(error) => {
                result["result"] = "failed".into();
                result["error"] = error.to_string().into();
            }
            Ok(record_set) => {
                if let Some(mut record_set) = record_set {
                    let rows = collect_record_set_rows(&mut record_set)?;
                    if !rows.is_empty() {
                        result["rows"] = serde_json::to_value(rows).unwrap();
                    }
                }
                result["result"] = "success".into();
            }
        }
        Ok(result)
    })();
    Some(match result {
        Ok(value) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            tidb_stringutil::go_json_marshal(&value),
        )
            .into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    })
}
