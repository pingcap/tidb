// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/server/handler/tikvhandler/dxf.go`.

use std::collections::HashMap;
use tidb_domain_core::domain::Domain;
use tidb_dxf::schstatus::{TtlFlag, TtlInfo};

/// Go `strconv.Atoi`: preserve syntax/range errors and Go quoting.
fn atoi(value: &str) -> Result<isize, String> {
    value.parse::<isize>().map_err(|error| {
        let reason = match error.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        format!(
            "strconv.Atoi: parsing {}: {reason}",
            tidb_go_strconv::quote_bytes(value.as_bytes())
        )
    })
}

/// Go `strconv.ParseFloat`.
fn parse_float(value: &str) -> Result<f64, String> {
    tidb_goruntime::go_strconv::parse_float64(value).map_err(|error| {
        let reason = match error {
            tidb_goruntime::go_strconv::ParseFloatError::Syntax => "invalid syntax",
            tidb_goruntime::go_strconv::ParseFloatError::Range => "value out of range",
        };
        format!(
            "strconv.ParseFloat: parsing {}: {reason}",
            tidb_go_strconv::quote_bytes(value.as_bytes())
        )
    })
}

/// Go `dxfOperationDefaultTTL`.
const DEFAULT_TTL: i64 = 3_600_000_000_000;

/// Go `parseTTLInfo`.
fn parse_ttl_info(form: &HashMap<String, String>) -> Result<TtlInfo, String> {
    let value = form.get("ttl").map_or("", String::as_str);
    let ttl = if value.is_empty() {
        DEFAULT_TTL
    } else {
        tidb_config_configtypes::parse_duration(value)
            .map_err(|error| format!("invalid ttl {value}, error {error}"))?
    };
    Ok(TtlInfo {
        ttl_nanoseconds: ttl,
        expire_time: (chrono::Local::now() + chrono::Duration::nanoseconds(ttl)).fixed_offset(),
    })
}

/// Go `parsePauseScaleInFlag`.
fn parse_pause_scale_in_flag(form: &HashMap<String, String>) -> Result<TtlFlag, String> {
    match form.get("action").map_or("", String::as_str) {
        "pause_scale_in" => Ok(TtlFlag {
            enabled: true,
            ttl_info: parse_ttl_info(form)?,
        }),
        "resume_scale_in" => Ok(TtlFlag::default()),
        action => Err(format!("invalid action {action}")),
    }
}

/// Go `parseTaskHistoryQuery`.
fn parse_task_history_query(query: &str) -> Result<(isize, i64, String), String> {
    let form = crate::http_settings::parse_form(query, "")?;
    let page_size_str = form.get("page_size").map_or("", String::as_str);
    let page_size = if page_size_str.is_empty() {
        tidb_dxf_storage::DEFAULT_HISTORY_TASK_PAGE_SIZE
    } else {
        page_size_str
            .parse::<isize>()
            .map_err(|_| format!("invalid page_size {page_size_str}"))?
    };
    tidb_dxf_storage::validate_history_task_page_size(page_size)
        .map_err(|_| format!("invalid page_size {page_size}"))?;
    let page_token_str = form.get("page_token").map_or("", String::as_str);
    let page_token = if page_token_str.is_empty() {
        0
    } else {
        page_token_str
            .parse::<i64>()
            .ok()
            .filter(|token| *token > 0)
            .ok_or_else(|| format!("invalid page_token {page_token_str}"))?
    };
    let keyspace = form.get("keyspace").cloned().unwrap_or_default();
    if !keyspace.is_empty() && tidb_naming::check_keyspace_name(&keyspace).is_err() {
        return Err(format!("invalid keyspace {keyspace}"));
    }
    Ok((page_size, page_token, keyspace))
}

/// Go `handler.WriteData` / `WriteErrorWithCode`.
fn response(
    code: axum::http::StatusCode,
    value: Result<serde_json::Value, String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match value {
        Ok(value) => (
            code,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            tidb_stringutil::go_json_marshal(&value),
        )
            .into_response(),
        Err(error) => (code, error).into_response(),
    }
}

/// Go `Server.startStatusHTTP`: DXF endpoints are registered only for SYSTEM.
pub(crate) fn serve(
    path: &str,
    request: &str,
    domain: Option<&Domain>,
) -> Option<axum::response::Response> {
    serve_with_context(
        path,
        request,
        domain,
        &tidb_gocontext::Context::background(),
    )
}

/// Go `DXFTaskHistoryHandler.ServeHTTP`: inherit the request's context.
pub(crate) fn serve_with_context(
    path: &str,
    request: &str,
    domain: Option<&Domain>,
    request_context: &tidb_gocontext::Context,
) -> Option<axum::response::Response> {
    use axum::http::StatusCode as Code;
    let domain = domain?;
    if !tidb_config_kerneltype::is_next_gen() || domain.store_keyspace() != "SYSTEM" {
        return None;
    }
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    let method = request.split_whitespace().next().unwrap_or("");
    let task_id = route
        .strip_prefix("/dxf/task/")
        .and_then(|tail| tail.strip_suffix("/max_runtime_slots"))
        .filter(|id| !id.contains('/'));
    let history_job = route
        .strip_prefix("/dxf/import-into/history/job/")
        .and_then(|tail| tail.split_once('/'))
        .filter(|(_, id)| !id.contains('/'));
    let expected = match route {
        "/dxf/schedule/status" | "/dxf/task/active" | "/dxf/task/history" => "GET",
        "/dxf/schedule" => "POST",
        "/dxf/schedule/tune" => "GET and POST",
        "/dxf/schedule/max_concurrent_task" | "/dxf/schedule/task_cleanup_batch_size" => {
            "GET and POST"
        }
        _ if task_id.is_some() => "POST",
        _ if history_job.is_some() => "GET",
        _ => return None,
    };
    if route != "/dxf/schedule/tune"
        && method != expected
        && !(expected == "GET and POST" && matches!(method, "GET" | "POST"))
    {
        return Some(response(
            Code::BAD_REQUEST,
            Err(format!("This api only support {expected} method")),
        ));
    }
    let form = match crate::http_status::request_form(query, request) {
        Ok(form) => form,
        Err(error) => return Some(response(Code::BAD_REQUEST, Err(error))),
    };
    let background = tidb_gocontext::Context::background();
    let parent = if route == "/dxf/task/history" {
        request_context
    } else {
        &background
    };
    let (ctx, cancel) =
        tidb_gocontext::Context::with_timeout(parent, std::time::Duration::from_secs(10));
    let ctx = tidb_dxf_storage::with_internal_source_type(&ctx, tidb_txnkv::INTERNAL_DIST_TASK);
    let result: Result<serde_json::Value, (Code, String)> = (|| {
        let internal =
            |error: tidb_dxf_handle::HandleError| (Code::INTERNAL_SERVER_ERROR, error.to_string());
        match route {
            "/dxf/schedule/status" => {
                let syncer = domain.server_info().ok_or_else(|| {
                    (
                        Code::INTERNAL_SERVER_ERROR,
                        "server info syncer is not initialized".into(),
                    )
                })?;
                Ok(serde_json::to_value(
                    tidb_dxf_handle::get_schedule_status(&ctx, &syncer).map_err(internal)?,
                )
                .unwrap())
            }
            "/dxf/task/active" => Ok(serde_json::to_value(
                tidb_dxf_handle::get_active_task_summary(&ctx).map_err(internal)?,
            )
            .unwrap()),
            "/dxf/task/history" => {
                let (size, token, keyspace) =
                    parse_task_history_query(query).map_err(|error| (Code::BAD_REQUEST, error))?;
                Ok(serde_json::to_value(
                    tidb_dxf_handle::list_history_tasks(&ctx, size, token, &keyspace)
                        .map_err(internal)?,
                )
                .unwrap())
            }
            "/dxf/schedule/tune" => {
                let keyspace = form.get("keyspace").map_or("", String::as_str);
                let bad = |error: String| (Code::BAD_REQUEST, error);
                if keyspace.is_empty() || tidb_naming::check_keyspace_name(keyspace).is_err() {
                    return Err(bad(format!("invalid or empty target keyspace {keyspace}")));
                }
                domain
                    .transactions
                    .load_keyspace(keyspace)
                    .map_err(|error| {
                        bad(format!(
                            "failed to load keyspace {keyspace} from PD: {error}"
                        ))
                    })?;
                match method {
                    "GET" => Ok(serde_json::to_value(
                        tidb_dxf_handle::get_schedule_tune_factors(&ctx, keyspace)
                            .map_err(internal)?,
                    )
                    .unwrap()),
                    "POST" => {
                        let ttl_info = parse_ttl_info(&form).map_err(bad)?;
                        let value = form.get("amplify_factor").map_or("", String::as_str);
                        let factor = parse_float(value).map_err(|error| {
                            bad(format!("invalid amplify_factor {value}, error {error}"))
                        })?;
                        if factor < tidb_dxf::schstatus::MIN_AMPLIFY_FACTOR
                            || factor > tidb_dxf::schstatus::MAX_AMPLIFY_FACTOR
                        {
                            return Err(bad(format!(
                                "amplify_factor {factor:.6} is out of range [{:.6}, {:.6}]",
                                tidb_dxf::schstatus::MIN_AMPLIFY_FACTOR,
                                tidb_dxf::schstatus::MAX_AMPLIFY_FACTOR
                            )));
                        }
                        let factors = tidb_dxf::schstatus::TtlTuneFactors {
                            ttl_info,
                            tune_factors: tidb_dxf::schstatus::TuneFactors {
                                amplify_factor: factor,
                            },
                        };
                        let store = domain.meta_store.get().ok_or_else(|| {
                            (
                                Code::INTERNAL_SERVER_ERROR,
                                "store is not initialized".to_owned(),
                            )
                        })?;
                        store
                            .run_in_new_txn(
                                &ctx,
                                tidb_txnkv::INTERNAL_DIST_TASK,
                                true,
                                &mut |txn| {
                                    tidb_meta::transaction::Mutator::new(txn)
                                        .set_dxf_schedule_tune_factors(keyspace, &factors)
                                },
                            )
                            .map_err(|error| (Code::INTERNAL_SERVER_ERROR, error))?;
                        Ok(serde_json::to_value(factors).unwrap())
                    }
                    _ => Err(bad("This api only support GET and POST method".into())),
                }
            }
            "/dxf/schedule" => {
                let flag =
                    parse_pause_scale_in_flag(&form).map_err(|error| (Code::BAD_REQUEST, error))?;
                tidb_dxf_handle::update_pause_scale_in_flag(&ctx, &flag).map_err(|error| {
                    (
                        Code::INTERNAL_SERVER_ERROR,
                        format!("failed to update pause scale-in flag, error {error}"),
                    )
                })?;
                Ok(serde_json::to_value(flag).unwrap())
            }
            _ if history_job.is_some() => {
                let (keyspace, id) = history_job.unwrap();
                let keyspace = percent_encoding::percent_decode_str(keyspace)
                    .decode_utf8()
                    .map_err(|error| (Code::BAD_REQUEST, error.to_string()))?;
                let id = percent_encoding::percent_decode_str(id)
                    .decode_utf8()
                    .map_err(|error| (Code::BAD_REQUEST, error.to_string()))?;
                if keyspace.is_empty() || tidb_naming::check_keyspace_name(&keyspace).is_err() {
                    return Err((
                        Code::BAD_REQUEST,
                        format!("invalid or empty target keyspace {keyspace}"),
                    ));
                }
                let job_id = id
                    .parse::<i64>()
                    .ok()
                    .filter(|id| *id > 0)
                    .ok_or_else(|| (Code::BAD_REQUEST, format!("invalid job id {id}")))?;
                let manager = tidb_dxf_storage::get_dxf_svc_task_mgr()
                    .map_err(|error| (Code::INTERNAL_SERVER_ERROR, error.to_string()))?;
                let info = tidb_dxf_importinto_jobhistory::get_from_history(
                    &ctx, &manager, &keyspace, job_id,
                )
                .map_err(|error| {
                    let code = if error.is(&tidb_dxf_storage::StorageError::TaskNotFound) {
                        Code::NOT_FOUND
                    } else {
                        Code::BAD_REQUEST
                    };
                    (code, error.to_string())
                })?;
                Ok(serde_json::to_value(info).unwrap())
            }
            _ if task_id.is_some() => {
                let bad = |message: String| (Code::BAD_REQUEST, message);
                let id_str = task_id.unwrap();
                let id = atoi(id_str)
                    .map_err(|error| bad(format!("invalid task ID {id_str}, error {error}")))?
                    as i64;
                if id <= 0 {
                    return Err(bad("invalid task ID".into()));
                }
                let value_str = form.get("value").map_or("", String::as_str);
                let slots = atoi(value_str)
                    .map_err(|error| bad(format!("invalid value {value_str}, error {error}")))?;
                if slots <= 0 {
                    return Err(bad(format!("invalid value {slots}")));
                }
                let (headers, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
                let is_form =
                    headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .any(|(key, value)| {
                            key.eq_ignore_ascii_case("content-type")
                                && value.trim().split(';').next()
                                    == Some("application/x-www-form-urlencoded")
                        });
                let mut steps = Vec::new();
                for part in (if is_form { body } else { "" })
                    .split('&')
                    .chain(query.split('&'))
                {
                    let values = crate::http_settings::parse_form(part, "").map_err(bad)?;
                    if let Some(value) = values.get("target_step") {
                        let step = atoi(value).map_err(|error| {
                            bad(format!("invalid target step {value}, error {error}"))
                        })? as i64;
                        steps.push(tidb_dxf::Step(step));
                    }
                }
                let manager = tidb_dxf_storage::get_task_manager()
                    .map_err(|error| (Code::INTERNAL_SERVER_ERROR, error.to_string()))?;
                let task = manager
                    .get_task_by_id(&ctx, id)
                    .map_err(|error| bad(error.to_string()))?;
                if slots >= task.base.required_slots {
                    return Err(bad(format!(
                        "max runtime slots should be less than required slots({})",
                        task.base.required_slots
                    )));
                }
                let mut step_strings = Vec::new();
                for step in &steps {
                    if !tidb_dxf::is_valid_business_step(&task.base.tp, *step) {
                        return Err(bad(format!(
                            "invalid target step {} for task type {}",
                            step.0, task.base.tp
                        )));
                    }
                    step_strings.push(tidb_dxf::step_2_str(&task.base.tp, *step));
                }
                let mut params = task.base.extra_params.clone();
                params.max_runtime_slots = slots;
                params.target_steps = steps;
                manager
                    .update_task_extra_params(&ctx, id, &params)
                    .map_err(|error| (Code::INTERNAL_SERVER_ERROR, error.to_string()))?;
                Ok(
                    serde_json::json!({"task_id": id, "task_key": task.base.key, "required_slots": task.base.required_slots,
                    "max_runtime_slots": slots, "target_steps": step_strings}),
                )
            }
            _ => {
                let concurrent = route.ends_with("max_concurrent_task");
                if method == "POST" {
                    let value_str = form.get("value").map_or("", String::as_str);
                    let value = atoi(value_str).map_err(|error| {
                        (
                            Code::BAD_REQUEST,
                            format!("invalid value {value_str}, error {error}"),
                        )
                    })?;
                    if concurrent {
                        tidb_dxf::set_max_concurrent_task(value)
                            .map_err(|e| (Code::BAD_REQUEST, e.to_string()))?;
                    } else {
                        tidb_dxf::set_task_cleanup_batch_size(value)
                            .map_err(|e| (Code::BAD_REQUEST, e.to_string()))?;
                    }
                }
                let (key, value) = if concurrent {
                    ("max_concurrent_task", tidb_dxf::get_max_concurrent_task())
                } else {
                    (
                        "task_cleanup_batch_size",
                        tidb_dxf::get_task_cleanup_batch_size(),
                    )
                };
                Ok(serde_json::json!({key: value, "persistence": "memory_only"}))
            }
        }
    })();
    cancel.cancel();
    Some(match result {
        Ok(value) => response(Code::OK, Ok(value)),
        Err((code, error)) => response(code, Err(error)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestParsePauseScaleInFlag`.
    #[test]
    fn test_parse_pause_scale_in_flag() {
        assert!(parse_pause_scale_in_flag(&HashMap::new())
            .unwrap_err()
            .contains("invalid action"));
        let mut form = HashMap::from([
            ("action".into(), "pause_scale_in".into()),
            ("ttl".into(), "invalid".into()),
        ]);
        assert!(parse_pause_scale_in_flag(&form)
            .unwrap_err()
            .contains("invalid ttl"));
        form.remove("ttl");
        for ttl in [DEFAULT_TTL, 5 * DEFAULT_TTL] {
            let before = chrono::Local::now().fixed_offset();
            let flag = parse_pause_scale_in_flag(&form).unwrap();
            assert!(flag.enabled);
            assert_eq!(flag.ttl_info.ttl_nanoseconds, ttl);
            assert!(flag.ttl_info.expire_time >= before + chrono::Duration::nanoseconds(ttl));
            assert!(
                flag.ttl_info.expire_time
                    <= chrono::Local::now().fixed_offset() + chrono::Duration::nanoseconds(ttl)
            );
            form.insert("ttl".into(), "5h".into());
        }
    }
}
