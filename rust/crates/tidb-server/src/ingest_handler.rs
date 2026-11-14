// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/server/handler/tikvhandler/tikv_handler.go:IngestConcurrencyHandler`.
use tidb_domain_core::cluster_transactions::ClusterTransactions;
use tidb_ingestor_ingestctrl::rate_limiter_param::IngestParam;

/// Go `startStatusHTTP`'s four ingest-control routes.
pub fn ingest_param(path: &str) -> Option<IngestParam> {
    match path.split_once('?').map_or(path, |(path, _)| path) {
        "/ingest/max-batch-split-ranges" => Some(IngestParam::MaxBatchSplitRanges),
        "/ingest/max-split-ranges-per-sec" => Some(IngestParam::MaxSplitRangesPerSec),
        "/ingest/max-ingest-inflight" => Some(IngestParam::MaxInflight),
        "/ingest/max-ingest-per-sec" => Some(IngestParam::MaxPerSecond),
        _ => None,
    }
}
/// Go `IngestParam`'s string value.
fn param_name(param: IngestParam) -> &'static str {
    match param {
        IngestParam::MaxBatchSplitRanges => "max_batch_split_ranges",
        IngestParam::MaxSplitRangesPerSec => "max_split_ranges_per_sec",
        IngestParam::MaxInflight => "max_inflight",
        IngestParam::MaxPerSecond => "max_per_second",
    }
}
/// Go `IngestConcurrencyHandler.ServeHTTP` over the node's metadata owner.
pub fn ingest_response(
    path: &str,
    request: &str,
    store: Option<&dyn ClusterTransactions>,
) -> Option<(u16, &'static str, String)> {
    let param = ingest_param(path)?;
    let method = request.split_whitespace().next().unwrap_or_default();
    let body = request.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    Some(serve_http(param, method, body, |param, value| {
        store
            .ok_or_else(|| {
                "ingest parameter metadata is unavailable from this storage owner".to_owned()
            })?
            .ingest_parameter(param, value)
    }))
}
/// Go `IngestConcurrencyHandler.ServeHTTP`: commit first, then publish local
/// parameters. GET always reads storage, including the `is_null` bit.
fn serve_http(
    param: IngestParam,
    method: &str,
    body: &str,
    mut access: impl FnMut(IngestParam, Option<f64>) -> Result<Option<f64>, String>,
) -> (u16, &'static str, String) {
    let result = match method {
        // Go `WriteData` of `{"value", "is_null"}`: keys sorted, a null
        // value read as 0.
        "GET" => access(param, None).map(|value| {
            crate::http_status::write_data(&format!(
                "{{\"is_null\":{},\"value\":{}}}",
                value.is_none(),
                tidb_stringutil::go_json_float(value.unwrap_or(0.0)),
            ))
        }),
        "POST" => {
            #[derive(serde::Deserialize, Default)]
            struct Payload {
                #[serde(default)]
                value: Option<f64>,
            }
            // Go Decode accepts an initial JSON value and ignores trailing data.
            let mut decoder =
                serde_json::Deserializer::from_str(body).into_iter::<Option<Payload>>();
            match decoder
                .next()
                .unwrap_or_else(|| Err(<serde_json::Error as serde::de::Error>::custom("EOF")))
            {
                Err(error) => Err(error.to_string()),
                Ok(payload) => {
                    let value = payload.unwrap_or_default().value.unwrap_or_default();
                    if value < 0.0 {
                        Err("value must be >= 0".to_owned())
                    } else {
                        access(param, Some(value)).map(|_| {
                            let old_value = param.update_global(value);
                            tidb_logutil::bg_logger().info(
                                "set ingest concurrency",
                                &[
                                    tidb_log::Field::new(
                                        "param",
                                        tidb_log::Value::Str(param_name(param).to_owned()),
                                    ),
                                    tidb_log::Field::new(
                                        "oldValue",
                                        tidb_log::Value::F64(old_value),
                                    ),
                                    tidb_log::Field::new("newValue", tidb_log::Value::F64(value)),
                                ],
                            );
                            crate::http_status::write_data("{\"message\":\"success\"}")
                        })
                    }
                }
            }
        }
        _ => {
            return (
                405,
                "text/plain; charset=utf-8",
                "method not allowed".into(),
            );
        }
    };
    match result {
        Ok(body) => (200, "application/json", body),
        Err(error) => (400, "text/plain; charset=utf-8", error),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tidb_meta::transaction::{MemoryTransaction, Mutator};
    /// Go `pkg/server/handler/tests/http_handler_serial_test.go:TestIngestParam`.
    /// Uses the real metadata encoding on both sides of each HTTP operation.
    #[test]
    fn test_ingest_param() {
        let m = Mutator::new(MemoryTransaction::default());
        tidb_ingestor_ingestctrl::rate_limiter_param::initialize_rate_limiter_param(&m).unwrap();
        for (url, default, modified) in [
            ("/ingest/max-batch-split-ranges", 2048.0, 1000.0),
            ("/ingest/max-split-ranges-per-sec", 0.0, 2000.0),
            ("/ingest/max-ingest-inflight", 0.0, 1000.0),
            ("/ingest/max-ingest-per-sec", 0.0, 2000.0),
        ] {
            let param = ingest_param(url).unwrap();
            let access = |p: IngestParam, v: Option<f64>| {
                if let Some(v) = v {
                    p.set(&m, v).map_err(|e| e.to_string())?;
                    Ok(None)
                } else {
                    p.get(&m).map_err(|e| e.to_string())
                }
            };
            let (status, _, body) = serve_http(param, "GET", "", access);
            assert_eq!(status, 200);
            // Go `WriteData`: `json.MarshalIndent(data, "", " ")`.
            assert_eq!(
                body,
                format!("{{\n \"is_null\": false,\n \"value\": {default}\n}}")
            );
            assert_eq!(
                serve_http(param, "POST", &format!("{{\"value\":{modified}}}"), access),
                (
                    200,
                    "application/json",
                    "{\n \"message\": \"success\"\n}".to_owned()
                )
            );
            let (status, _, body) = serve_http(param, "GET", "", access);
            assert_eq!(status, 200);
            let json: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(json["value"], modified);
        }
    }
}
