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

//! Go `domain/infosync`'s PD HTTP client (`GlobalInfoSyncerInit`): the
//! store's client with the info syncer's caller id and `pdResponseHandler`,
//! which turns any answer but 200, 404 and 412 into `ErrHTTPServiceError`.

/// Go infosync `GlobalInfoSyncerInit`'s client: the store's PD HTTP client
/// `WithCallerID("tidb-info-syncer").WithRespHandler(pdResponseHandler)`,
/// which the placement, label-rule, TiFlash replica and affinity managers
/// share.
pub fn info_syncer_pd_http_client(
    pd_http_client: &tikv_client::pd_http::Client,
) -> tikv_client::pd_http::Client {
    let handler: tikv_client::pd_http::RespHandleFunc = std::sync::Arc::new(pd_response_handler);
    pd_http_client
        .with_caller_id("tidb-info-syncer")
        .with_resp_handler(handler)
}

/// Go infosync `pdResponseHandler`: a 200 answer decodes into the result; a
/// 404 or 412 answer is no error and leaves the result's zero value; any
/// other status is `ErrHTTPServiceError` carrying the body, without the
/// status.
fn pd_response_handler(
    resp: tikv_client::pd_http::HttpResponse,
    res: Option<&mut dyn tikv_client::pd_http::ResponseTarget>,
) -> tikv_client::Result<()> {
    if resp.status_code == 200 {
        return match res {
            Some(res) => res.unmarshal(&resp.body),
            None => Ok(()),
        };
    }
    tidb_logutil::bg_logger().warn(
        "response not 200",
        &[
            tidb_log::Field::new(
                "method",
                tidb_log::Value::Str(resp.request.method.as_str().to_owned()),
            ),
            tidb_log::Field::new("url", tidb_log::Value::Str(resp.request.url.clone())),
            tidb_log::Field::new(
                "http status",
                tidb_log::Value::U64(u64::from(resp.status_code)),
            ),
        ],
    );
    if resp.status_code != 404 && resp.status_code != 412 {
        return Err(tikv_client::Error::StringError(format!(
            "{}{}",
            http_service_error_prefix(),
            String::from_utf8_lossy(&resp.body)
        )));
    }
    Ok(())
}

// Go `ErrHTTPServiceError.FastGen("%s", body)`'s text before the body.
fn http_service_error_prefix() -> String {
    // `[<class>:<code>]` of the definition, with an empty message.
    crate::infosync_errors::ERR_HTTP_SERVICE_ERROR
        .generate("")
        .to_string()
}

/// Whether `message` is the info syncer's `ErrHTTPServiceError` (see
/// [`info_syncer_pd_http_client`]), which Go keeps as a coded TiDB error.
pub fn is_http_service_error(message: &str) -> bool {
    message.starts_with(&http_service_error_prefix())
}

/// The info syncer's `ErrHTTPServiceError` that `message` carries, as the
/// coded error Go returns (`ErrHTTPServiceError.FastGen("%s", body)`).
pub fn http_service_error(message: &str) -> Option<tidb_terror::TerrorError> {
    message
        .strip_prefix(&http_service_error_prefix())
        .map(|body| crate::infosync_errors::ERR_HTTP_SERVICE_ERROR.generate(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A non-200 PD answer through the info syncer's handler is Go's coded
    /// `ErrHTTPServiceError` (8243) carrying PD's body; other failures are not.
    #[test]
    fn http_service_errors_stay_coded_with_the_body() {
        let message = format!("{}rule not found", http_service_error_prefix());
        let error = http_service_error(&message).expect("an info syncer HTTP error");
        assert_eq!(error.to_sql_error().code, 8243);
        assert_eq!(error.message(), "rule not found");
        assert!(http_service_error("connection refused").is_none());
    }
}
