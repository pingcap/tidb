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

//! Go `pkg/executor/metrics_reader.go`'s `MetricRetriever`: a
//! `metrics_schema` table's rows, read from the cluster's Prometheus with the
//! table's PromQL over the scan's time range, once per quantile.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tidb_datatype::Datum;
use tidb_domain_infosync::prometheus_addr::{get_prometheus_addr, PrometheusAddrError};
use tidb_goruntime::go_strconv;
use tidb_infoschema::metric_table_def::{
    gen_label_condition_values, get_metric_table_def, MetricTableDef,
};
use tidb_planner::memtable_predicate_extractor::MetricTableExtractor;

/// Go `promReadTimeout`.
const PROM_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Go `epQueryRange`.
const EP_QUERY_RANGE: &str = "/api/v1/query_range";

/// Go `pmodel.SampleStream`: one series and its samples.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SampleStream {
    /// Go `Metric`: the series' labels.
    pub metric: BTreeMap<String, String>,
    /// Go `Values`: `(Timestamp, Value)`, the timestamp in milliseconds.
    pub values: Vec<(i64, f64)>,
}

/// Go `pmodel.Matrix`.
pub type Matrix = Vec<SampleStream>;

/// What Go's `mockMetricsTableData` and `mockMetricsPromData` failpoints read
/// from the statement's context.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use super::{Datum, Matrix};
    use std::collections::HashMap;
    use std::sync::{Mutex, PoisonError};

    static METRICS_TABLE_DATA: Mutex<Option<HashMap<String, Vec<Vec<Datum>>>>> = Mutex::new(None);
    static METRICS_PROM_DATA: Mutex<Option<Matrix>> = Mutex::new(None);

    /// Go `context.WithValue(ctx, "__mockMetricsTableData", data)` with the
    /// `mockMetricsTableData` failpoint on: each named table's rows.
    pub fn set_metrics_table_data(data: Option<HashMap<String, Vec<Vec<Datum>>>>) {
        *METRICS_TABLE_DATA.lock().unwrap_or_else(PoisonError::into_inner) = data;
    }

    /// Go `context.WithValue(ctx, MockMetricsPromDataKey{}, matrix)` with the
    /// `mockMetricsPromData` failpoint on: every query's answer.
    pub fn set_metrics_prom_data(data: Option<Matrix>) {
        *METRICS_PROM_DATA.lock().unwrap_or_else(PoisonError::into_inner) = data;
    }

    pub(super) fn metrics_table_data(table: &str) -> Option<Vec<Vec<Datum>>> {
        METRICS_TABLE_DATA
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|data| data.get(table).cloned())
    }

    pub(super) fn metrics_prom_data() -> Option<Matrix> {
        METRICS_PROM_DATA.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// Go `MetricTableDef.genColumnInfos`'s names, lowercase: `time`, the labels,
/// `quantile` for a quantile table, `value`.
pub fn column_names(table: &str) -> Vec<String> {
    let mut names = vec!["time".to_owned()];
    if let Some(tbl_def) = get_metric_table_def(table) {
        names.extend(tbl_def.labels.iter().map(|label| label.to_lowercase()));
        if tbl_def.quantile > 0.0 {
            names.push("quantile".to_owned());
        }
    }
    names.push("value".to_owned());
    names
}

/// Go `promv1.Error` or any other error of a query.
#[derive(Debug, PartialEq)]
enum QueryError {
    /// Go `*promv1.Error`'s `Msg` and `Detail`.
    Prom { msg: String, detail: String },
    Other(String),
}

/// Go `MetricRetriever`: the reader of one `metrics_schema` table scan.
pub struct MetricRetriever<'a> {
    /// Go `table.Name.L`.
    pub table: &'a str,
    /// Go `extractor`.
    pub extractor: &'a MetricTableExtractor,
    /// Go `SessionVars.MetricSchemaStep`, in seconds.
    pub metric_schema_step: i64,
    /// Go `SessionVars.MetricSchemaRangeDuration`, in seconds.
    pub metric_schema_range_duration: i64,
}

impl MetricRetriever<'_> {
    /// Go `MetricRetriever.retrieve`.
    pub fn retrieve(&self) -> Result<Vec<Vec<Datum>>, String> {
        if self.extractor.skip_request {
            return Ok(Vec::new());
        }
        #[cfg(any(test, feature = "test-util"))]
        if let Some(rows) = mock::metrics_table_data(self.table) {
            return Ok(rows);
        }
        let tbl_def = get_metric_table_def(self.table)
            .ok_or_else(|| format!("can not find metric table: {}", self.table))?;
        let mut total_rows = Vec::new();
        let default_quantiles = [tbl_def.quantile];
        let quantiles = if self.extractor.quantiles.is_empty() {
            &default_quantiles[..]
        } else {
            &self.extractor.quantiles[..]
        };
        for quantile in quantiles {
            let matrix = self.query_metric(tbl_def, *quantile).map_err(|error| match error {
                QueryError::Prom { msg, detail } => {
                    format!("query metric error, msg: {msg}, detail: {detail}")
                }
                QueryError::Other(error) => format!("query metric error: {error}"),
            })?;
            total_rows.extend(self.gen_rows(tbl_def, &matrix, *quantile));
        }
        Ok(total_rows)
    }

    /// Go `MetricRetriever.queryMetric`.
    fn query_metric(&self, tbl_def: &MetricTableDef, quantile: f64) -> Result<Matrix, QueryError> {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(matrix) = mock::metrics_prom_data() {
            return Ok(matrix);
        }
        // Add retry to avoid network error.
        let mut prometheus_addr = Err(PrometheusAddrError::NotSet);
        for _ in 0..5 {
            prometheus_addr = get_prometheus_addr();
            if matches!(prometheus_addr, Ok(_) | Err(PrometheusAddrError::NotSet)) {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let prometheus_addr = prometheus_addr.map_err(|error| QueryError::Other(error.to_string()))?;
        let deadline = Instant::now() + PROM_READ_TIMEOUT;
        let prom_ql = tbl_def.gen_prom_ql(
            self.metric_schema_range_duration,
            &self.extractor.label_conditions,
            quantile,
        );
        // Add retry to avoid network error.
        let mut result = Err(QueryError::Other(String::new()));
        for _ in 0..5 {
            result = query_range(
                &prometheus_addr,
                &prom_ql,
                self.extractor.start_time,
                self.extractor.end_time,
                self.metric_schema_step,
                deadline,
            );
            if result.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        result
    }

    /// Go `MetricRetriever.genRows`.
    fn gen_rows(&self, tbl_def: &MetricTableDef, matrix: &Matrix, quantile: f64) -> Vec<Vec<Datum>> {
        let mut rows = Vec::new();
        for m in matrix {
            for v in &m.values {
                rows.push(self.gen_record(tbl_def, &m.metric, *v, quantile));
            }
        }
        rows
    }

    /// Go `MetricRetriever.genRecord`.
    fn gen_record(
        &self,
        tbl_def: &MetricTableDef,
        metric: &BTreeMap<String, String>,
        pair: (i64, f64),
        quantile: f64,
    ) -> Vec<Datum> {
        let mut record = Vec::with_capacity(2 + tbl_def.labels.len() + 1);
        // Record order should keep same with genColumnInfos.
        record.push(time_datum(pair.0));
        for label in tbl_def.labels {
            let mut v = metric.get(*label).cloned().unwrap_or_default();
            if v.is_empty() {
                v = self
                    .extractor
                    .label_conditions
                    .get(&label.to_lowercase())
                    .map(gen_label_condition_values)
                    .unwrap_or_default();
            }
            record.push(Datum::Bytes(v.into_bytes()));
        }
        if tbl_def.quantile > 0.0 {
            record.push(Datum::Real(quantile));
        }
        if pair.1.is_nan() {
            record.push(Datum::Null);
        } else {
            record.push(Datum::Real(pair.1));
        }
        record
    }
}

/// Go `types.NewTime(types.FromGoTime(time.UnixMilli(ts)), mysql.TypeDatetime,
/// types.MaxFsp)`: the sample's time on the node's own clock.
fn time_datum(unix_milli: i64) -> Datum {
    use chrono::TimeZone;
    match chrono::Local.timestamp_millis_opt(unix_milli) {
        chrono::LocalResult::Single(time) | chrono::LocalResult::Ambiguous(time, _) => {
            tidb_datatype::Time::new(
                tidb_datatype::core_time_from_datetime(time),
                tidb_datatype::TimeType::DateTime,
                6,
            )
            .map_or(Datum::Null, Datum::Time)
        }
        chrono::LocalResult::None => Datum::Null,
    }
}

/// Go `promv1.formatTime`.
fn format_time(time: chrono::DateTime<chrono::Utc>) -> String {
    let seconds = time.timestamp() as f64 + f64::from(time.timestamp_subsec_nanos()) / 1e9;
    go_strconv::format_float(seconds, b'f', -1, 64)
}

/// Go `promv1.httpAPI.QueryRange` over `api.NewClient(api.Config{Address})`,
/// with `deadline` as its context's.
fn query_range(
    address: &str,
    query: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    step: i64,
    deadline: Instant,
) -> Result<Matrix, QueryError> {
    let other = |error: reqwest::Error| QueryError::Other(error.to_string());
    let url = format!("{}{EP_QUERY_RANGE}", address.trim_end_matches('/'));
    // Go `url.Values.Encode` writes the keys sorted.
    let args = [
        ("end", format_time(end)),
        ("query", query.to_owned()),
        ("start", format_time(start)),
        ("step", go_strconv::format_float(step as f64, b'f', -1, 64)),
    ];
    let client = reqwest::blocking::Client::builder().build().map_err(other)?;
    let remaining = || deadline.saturating_duration_since(Instant::now());
    // Go `DoGetFallback`: the request as a POST, and on a 405 or 501 a GET.
    let encoded = args
        .iter()
        .map(|(key, value)| format!("{key}={}", query_escape(value)))
        .collect::<Vec<_>>()
        .join("&");
    let response = client
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(encoded.clone())
        .timeout(remaining())
        .send()
        .map_err(other)?;
    let response = if matches!(response.status().as_u16(), 405 | 501) {
        client.get(format!("{url}?{encoded}")).timeout(remaining()).send().map_err(other)?
    } else {
        response
    };
    let code = response.status().as_u16();
    let body = response.bytes().map_err(other)?;
    let data = api_response_data(code, &body)?;
    query_result(&data)
}

/// Go `url.QueryEscape`.
fn query_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => escaped.push(char::from(byte)),
            b' ' => escaped.push('+'),
            _ => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
}

/// Go `apiClientImpl.Do` after the round trip: the response's `data`, or the
/// error its code and body state.
fn api_response_data(code: u16, body: &[u8]) -> Result<Vec<u8>, QueryError> {
    // These are the codes that Prometheus sends when it returns an error.
    let api_error = matches!(code, 422 | 400);
    if code / 100 != 2 && !api_error {
        let msg = match code / 100 {
            4 => format!("client error: {code}"),
            5 => format!("server error: {code}"),
            _ => format!("bad response code {code}"),
        };
        return Err(QueryError::Prom {
            msg,
            detail: String::from_utf8_lossy(body).into_owned(),
        });
    }
    let prom = |msg: String| QueryError::Prom { msg, detail: String::new() };
    let mut result = serde_json::Value::Null;
    if code != 204 {
        result = serde_json::from_slice(body).map_err(|error| prom(error.to_string()))?;
    }
    if result["status"] == "error" {
        return Err(prom(result["error"].as_str().unwrap_or_default().to_owned()));
    }
    if api_error && result["status"] == "success" {
        return Err(prom("inconsistent body for response code".to_owned()));
    }
    Ok(match &result["data"] {
        serde_json::Value::Null => Vec::new(),
        data => data.to_string().into_bytes(),
    })
}

/// Go `queryResult.UnmarshalJSON`: a matrix's series; a scalar or a vector
/// has no rows for `genRows`.
fn query_result(data: &[u8]) -> Result<Matrix, QueryError> {
    let other = |error: String| QueryError::Other(error);
    let value: serde_json::Value =
        serde_json::from_slice(data).map_err(|error| other(error.to_string()))?;
    match value["resultType"].as_str().unwrap_or_default() {
        "scalar" | "vector" => Ok(Vec::new()),
        "matrix" => value["result"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .map(|stream| {
                let metric = stream["metric"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(name, value)| (name.clone(), value.as_str().unwrap_or_default().to_owned()))
                    .collect();
                let values = stream["values"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .map(|pair| Ok((sample_time(&pair[0])?, sample_value(&pair[1])?)))
                    .collect::<Result<_, String>>()?;
                Ok(SampleStream { metric, values })
            })
            .collect::<Result<_, String>>()
            .map_err(other),
        value_type => Err(other(format!("unexpected value type {value_type:?}"))),
    }
}

/// Go `pmodel.Time.UnmarshalJSON`: seconds with up to three fraction digits,
/// as milliseconds.
fn sample_time(value: &serde_json::Value) -> Result<i64, String> {
    let text = value.to_string();
    let invalid = |_| format!("invalid time {text:?}");
    let Some((seconds, fraction)) = text.split_once('.') else {
        return Ok(text.parse::<i64>().map_err(invalid)? * 1000);
    };
    let v = seconds.parse::<i64>().map_err(invalid)? * 1000;
    let va = format!("{fraction:0<3.3}").parse::<i64>().map_err(invalid)?;
    // If the value was something like -0.1 the negative is lost in the
    // parsing because of the leading zero, this ensures that we capture it.
    if seconds.starts_with('-') && v + va > 0 {
        return Ok(-(v + va));
    }
    Ok(v + va)
}

/// Go `pmodel.SampleValue.UnmarshalJSON`: a quoted float.
fn sample_value(value: &serde_json::Value) -> Result<f64, String> {
    let text = value
        .as_str()
        .ok_or_else(|| "sample value must be a quoted string".to_owned())?;
    go_strconv::parse_float64(text).map_err(|error| {
        let reason = match error {
            go_strconv::ParseFloatError::Syntax => "invalid syntax",
            go_strconv::ParseFloatError::Range => "value out of range",
        };
        format!("strconv.ParseFloat: parsing {text:?}: {reason}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn extractor() -> MetricTableExtractor {
        MetricTableExtractor::new()
    }

    fn retriever<'a>(table: &'a str, extractor: &'a MetricTableExtractor) -> MetricRetriever<'a> {
        MetricRetriever {
            table,
            extractor,
            metric_schema_step: 60,
            metric_schema_range_duration: 60,
        }
    }

    /// Go `genRecord`: the time, each label (the metric's, else the scan's
    /// conditions joined by `|`), the quantile of a quantile table, and the
    /// value, NULL for NaN.
    #[test]
    fn a_record_is_time_labels_quantile_and_value_like_go() {
        let mut extractor = extractor();
        extractor
            .label_conditions
            .insert("sql_type".to_owned(), ["select", "insert"].map(str::to_owned).into());
        let retriever = retriever("tidb_query_duration", &extractor);
        let def = get_metric_table_def("tidb_query_duration").unwrap();
        assert_eq!(def.labels, ["instance", "sql_type"]);
        let matrix = vec![SampleStream {
            metric: BTreeMap::from([("instance".to_owned(), "127.0.0.1:10080".to_owned())]),
            values: vec![(1_577_103_095_250, 0.1), (1_577_103_155_000, f64::NAN)],
        }];
        let rows = retriever.gen_rows(def, &matrix, 0.85);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0][1..],
            [
                Datum::Bytes(b"127.0.0.1:10080".to_vec()),
                Datum::Bytes(b"insert|select".to_vec()),
                Datum::Real(0.85),
                Datum::Real(0.1),
            ]
        );
        assert_eq!(rows[1][4], Datum::Null);
        let Datum::Time(time) = &rows[0][0] else { panic!("{:?}", rows[0][0]) };
        assert_eq!(time.fsp(), 6);
        use chrono::TimeZone;
        let local = chrono::Local.timestamp_millis_opt(1_577_103_095_250).unwrap();
        assert_eq!(time.to_string(), local.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        // A table without a quantile has no quantile column.
        let def = get_metric_table_def("up").unwrap();
        assert_eq!(def.quantile, 0.0);
        let row = retriever.gen_record(def, &BTreeMap::new(), (0, 1.0), 0.0);
        assert_eq!(row.len(), 1 + def.labels.len() + 1);
    }

    #[test]
    fn a_skipped_request_reads_nothing_like_go() {
        let mut extractor = extractor();
        extractor.skip_request = true;
        assert_eq!(retriever("no_such_table", &extractor).retrieve(), Ok(Vec::new()));
        extractor.skip_request = false;
        assert_eq!(
            retriever("no_such_table", &extractor).retrieve(),
            Err("can not find metric table: no_such_table".to_owned())
        );
    }

    /// Go `pmodel.Time.UnmarshalJSON` and `SampleValue.UnmarshalJSON`.
    #[test]
    fn samples_decode_like_prometheus_common() {
        let time = |text: &str| sample_time(&serde_json::from_str(text).unwrap());
        assert_eq!(time("1577103095"), Ok(1_577_103_095_000));
        assert_eq!(time("1577103095.25"), Ok(1_577_103_095_250));
        assert_eq!(time("1577103095.7819"), Ok(1_577_103_095_781));
        assert_eq!(time("-0.1"), Ok(-100));
        let value = |text: &str| sample_value(&serde_json::from_str(text).unwrap());
        assert_eq!(value(r#""0.1""#), Ok(0.1));
        assert_eq!(value(r#""+Inf""#), Ok(f64::INFINITY));
        assert!(value(r#""NaN""#).unwrap().is_nan());
        assert_eq!(value("0.1"), Err("sample value must be a quoted string".to_owned()));
    }

    /// Go `apiClientImpl.Do`'s reading of a response's code and body.
    #[test]
    fn a_response_is_its_data_or_its_error_like_client_golang() {
        let prom = |msg: &str, detail: &str| {
            Err(QueryError::Prom { msg: msg.to_owned(), detail: detail.to_owned() })
        };
        assert_eq!(api_response_data(404, b"gone"), prom("client error: 404", "gone"));
        assert_eq!(api_response_data(503, b"busy"), prom("server error: 503", "busy"));
        assert_eq!(api_response_data(302, b""), prom("bad response code 302", ""));
        assert_eq!(
            api_response_data(400, br#"{"status":"error","errorType":"bad_data","error":"parse error"}"#),
            prom("parse error", "")
        );
        assert_eq!(
            api_response_data(422, br#"{"status":"success"}"#),
            prom("inconsistent body for response code", "")
        );
        assert!(matches!(api_response_data(200, b"<html>"), Err(QueryError::Prom { .. })));
        assert_eq!(api_response_data(204, b""), Ok(Vec::new()));
        let data = api_response_data(200, br#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#).unwrap();
        assert_eq!(query_result(&data), Ok(Vec::new()));
        assert_eq!(
            query_result(br#"{"resultType":"string","result":[]}"#),
            Err(QueryError::Other(r#"unexpected value type "string""#.to_owned()))
        );
        assert_eq!(query_result(br#"{"resultType":"vector","result":[]}"#), Ok(Vec::new()));
    }

    /// One Prometheus answering `responses` in order, and the requests it saw.
    fn prometheus(responses: Vec<(u16, &'static str)>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (code, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    request.extend_from_slice(&buffer[..read]);
                    let text = String::from_utf8_lossy(&request).into_owned();
                    let Some((head, sent)) = text.split_once("\r\n\r\n") else { continue };
                    let length = head
                        .lines()
                        .find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ").map(str::to_owned))
                        .map_or(0, |length| length.parse::<usize>().unwrap());
                    if sent.len() >= length {
                        requests.push(format!("{} {sent}", head.lines().next().unwrap()));
                        break;
                    }
                }
                write!(
                    stream,
                    "HTTP/1.1 {code} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (address, server)
    }

    const MATRIX: &str = r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"127.0.0.1:10080"},"values":[[1577103095.25,"0.1"],[1577103155,"NaN"]]}]}}"#;

    /// Go `QueryRange`: a POST of the form `end`, `query`, `start`, `step`
    /// to `/api/v1/query_range`, and the matrix it answers.
    #[test]
    fn a_range_query_posts_its_form_and_reads_the_matrix_like_client_golang() {
        use chrono::TimeZone;
        let (address, server) = prometheus(vec![(200, MATRIX)]);
        let start = chrono::Utc.timestamp_millis_opt(1_577_103_095_500).unwrap();
        let end = chrono::Utc.timestamp_opt(1_577_103_695, 0).unwrap();
        let matrix = query_range(
            &address,
            r#"sum(rate(x{a="b"}[60s]))"#,
            start,
            end,
            60,
            Instant::now() + PROM_READ_TIMEOUT,
        )
        .unwrap();
        assert_eq!(matrix.len(), 1);
        assert_eq!(
            matrix[0].metric,
            BTreeMap::from([("instance".to_owned(), "127.0.0.1:10080".to_owned())])
        );
        assert_eq!(matrix[0].values[0], (1_577_103_095_250, 0.1));
        assert_eq!(matrix[0].values[1].0, 1_577_103_155_000);
        assert!(matrix[0].values[1].1.is_nan());
        assert_eq!(
            server.join().unwrap(),
            ["POST /api/v1/query_range HTTP/1.1 end=1577103695&query=sum%28rate%28x%7Ba%3D%22b%22%7D%5B60s%5D%29%29&start=1577103095.5&step=60"]
        );
    }

    /// Go `DoGetFallback`: a 405 or a 501 to the POST is asked again as a GET.
    #[test]
    fn a_refused_post_falls_back_to_get_like_client_golang() {
        use chrono::TimeZone;
        let (address, server) = prometheus(vec![(405, ""), (200, MATRIX)]);
        let time = chrono::Utc.timestamp_opt(10, 0).unwrap();
        let matrix = query_range(&address, "up", time, time, 30, Instant::now() + PROM_READ_TIMEOUT).unwrap();
        assert_eq!(matrix.len(), 1);
        let requests = server.join().unwrap();
        assert_eq!(requests[0], "POST /api/v1/query_range HTTP/1.1 end=10&query=up&start=10&step=30");
        assert_eq!(requests[1], "GET /api/v1/query_range?end=10&query=up&start=10&step=30 HTTP/1.1 ");
    }
}
