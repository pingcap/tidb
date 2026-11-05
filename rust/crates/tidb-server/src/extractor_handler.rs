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

//! Go `pkg/server/handler/extractorhandler`: the status port's
//! `/extract_task/dump`, which runs an extract task and answers its file
//! name, or streams the file with `isDump`.

use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use tidb_domain::extract::{ExtractHandle, ExtractTask, ExtractType};

const EXTRACT_PLAN_TASK_TYPE: &str = "plan";

/// Go `handler.Type` / `Begin` / `End` / `IsDump` / `IsSkipStats` /
/// `IsHistoryView`.
const TYPE: &str = "type";
const BEGIN: &str = "begin";
const END: &str = "end";
const IS_DUMP: &str = "isDump";
const IS_SKIP_STATS: &str = "isSkipStats";
const IS_HISTORY_VIEW: &str = "isHistoryView";

/// Go `ExtractTaskServeHandler.ServeHTTP`; `None` for another route.
pub fn extract_task_serve_response(
    path: &str,
    handler: Option<&Arc<ExtractHandle>>,
) -> Option<axum::response::Response> {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    if route != "/extract_task/dump" {
        return None;
    }
    let respond = |code: u16, content_type: &str, disposition: Option<String>, body: Vec<u8>| {
        let mut builder = axum::http::Response::builder()
            .status(code)
            .header("content-type", content_type)
            .header("content-length", body.len());
        if let Some(disposition) = disposition {
            builder = builder.header("content-disposition", disposition);
        }
        builder.body(axum::body::Body::from(body)).expect("status response headers")
    };
    // Go `handler.WriteError`.
    let write_error = |message: String| respond(400, "text/plain", None, message.into_bytes());
    let values = crate::mvcc_routes::url_query(query);
    let (task, is_dump) = match build_extract_task(&values) {
        Ok(built) => built,
        Err(error) => {
            tidb_logutil::bg_logger().error("build extract task failed", &[tidb_logutil::zap_error(&error)]);
            return Some(write_error(error));
        }
    };
    let Some(handler) = handler else {
        return Some(write_error("the extract handle is not set up".to_owned()));
    };
    let name = match handler.extract_task(&task) {
        Ok(name) => name,
        Err(error) => {
            tidb_logutil::bg_logger().error("extract task failed", &[tidb_logutil::zap_error(error.to_string())]);
            return Some(write_error(error.to_string()));
        }
    };
    if !is_dump {
        return Some(respond(200, "text/plain; charset=utf-8", None, name.into_bytes()));
    }
    // For dump mode, stream the file content directly.
    match stream_extract_response(&name) {
        Ok(body) => Some(respond(
            200,
            "application/zip",
            Some(format!("attachment; filename=\"{name}.zip\"")),
            body,
        )),
        Err(error) => {
            tidb_logutil::bg_logger().warn("stream extract response failed", &[tidb_logutil::zap_error(&error)]);
            Some(write_error(error))
        }
    }
}

/// Go `streamExtractResponse`: the extract file from the global storage.
fn stream_extract_response(name: &str) -> Result<Vec<u8>, String> {
    let path = format!("{}/{name}", tidb_domain::extract::get_extract_task_dir_name());
    let storage = tidb_planner_extstore::get_global_ext_storage()?;
    storage.read_file(&path).map_err(|error| error.to_string())
}

fn build_extract_task(values: &crate::mvcc_routes::Values) -> Result<(ExtractTask, bool), String> {
    let extract_task_type = query_get(values, TYPE);
    if extract_task_type.to_lowercase() == EXTRACT_PLAN_TASK_TYPE {
        return build_extract_plan_task(values);
    }
    tidb_logutil::bg_logger().error("unknown extract task type", &[]);
    Err("unknown extract task type".to_owned())
}

fn build_extract_plan_task(values: &crate::mvcc_routes::Values) -> Result<(ExtractTask, bool), String> {
    let begin_str = query_get(values, BEGIN);
    let end_str = query_get(values, END);
    let now = || chrono::Local::now().fixed_offset();
    let begin = if begin_str.is_empty() {
        now() + chrono::Duration::minutes(30)
    } else {
        parse_time_format(begin_str).inspect_err(|error| {
            tidb_logutil::bg_logger().error(
                "extract task begin time failed",
                &[
                    tidb_logutil::zap_error(error),
                    tidb_log::Field::new("begin", tidb_log::Value::Str(begin_str.to_owned())),
                ],
            );
        })?
    };
    let end = if end_str.is_empty() {
        now()
    } else {
        parse_time_format(end_str).inspect_err(|error| {
            tidb_logutil::bg_logger().error(
                "extract task end time failed",
                &[
                    tidb_logutil::zap_error(error),
                    tidb_log::Field::new("end", tidb_log::Value::Str(end_str.to_owned())),
                ],
            );
        })?
    };
    let is_dump = extract_bool_param(IS_DUMP, false, values);
    Ok((
        ExtractTask {
            extract_type: ExtractType::Plan,
            is_background_job: false,
            begin,
            end,
            skip_stats: extract_bool_param(IS_SKIP_STATS, false, values),
            use_history_view: extract_bool_param(IS_HISTORY_VIEW, true, values),
        },
        is_dump,
    ))
}

/// Go `time.Parse(types.TimeFormat, s)`: a UTC time.
fn parse_time_format(s: &str) -> Result<DateTime<FixedOffset>, String> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .map(|time| time.and_utc().fixed_offset())
        .map_err(|_| format!("parsing time {s:?} as \"2006-01-02 15:04:05\": cannot parse {s:?} as \"2006\""))
}

/// Go `extractBoolParam`.
fn extract_bool_param(param: &str, default_value: bool, values: &crate::mvcc_routes::Values) -> bool {
    let s = query_get(values, param);
    if s.is_empty() {
        return default_value;
    }
    // Go `strconv.ParseBool`.
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => true,
        "0" | "f" | "F" | "FALSE" | "false" | "False" => false,
        _ => default_value,
    }
}

/// Go `url.Values.Get`.
fn query_get<'a>(values: &'a crate::mvcc_routes::Values, key: &str) -> &'a str {
    values
        .iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, list)| list.first())
        .map_or("", String::as_str)
}
