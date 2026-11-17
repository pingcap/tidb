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

//! Go `GCSStorage`'s storage client for what `opendal` cannot send: the
//! JSON API's object rewrite, which Go's `ObjectHandle.CopierFrom(...).Run`
//! sends to copy an object from any bucket.

use std::sync::Arc;

use crate::error::{Error, Result};

/// Go `storage.ScopeReadWrite`, the scope `opendal`'s GCS service asks for.
const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// One token loader handed to `opendal` and to [`GcsClient`], so both send
/// the same, once-fetched, token.
pub(crate) struct SharedTokenLoader(pub Arc<reqsign::GoogleTokenLoader>);

impl std::fmt::Debug for SharedTokenLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedTokenLoader")
    }
}

#[async_trait::async_trait]
impl reqsign::GoogleTokenLoad for SharedTokenLoader {
    async fn load(&self, _client: reqwest12::Client) -> anyhow::Result<Option<reqsign::GoogleToken>> {
        self.0.load().await
    }
}

/// The token loader `opendal`'s GCS builder assembles from the storage's
/// credentials blob; `None` is anonymous (Go `option.WithoutAuthentication`).
pub(crate) fn token_loader(
    credentials_blob: &str,
    no_credentials: bool,
    http: &reqwest12::Client,
) -> Result<Option<Arc<reqsign::GoogleTokenLoader>>> {
    if no_credentials && credentials_blob.is_empty() {
        return Ok(None);
    }
    // `opendal`'s GCS builder: the blob, else the environment's and the
    // well-known location's credentials, else the VM metadata.
    let mut credentials = reqsign::GoogleCredentialLoader::default();
    if !credentials_blob.is_empty() {
        use base64::Engine as _;
        credentials = credentials.with_content(&base64::engine::general_purpose::STANDARD.encode(credentials_blob));
    }
    let mut loader = reqsign::GoogleTokenLoader::new(GCS_SCOPE, http.clone());
    if let Ok(Some(credential)) = credentials.load() {
        loader = loader.with_credentials(credential);
    }
    Ok(Some(Arc::new(loader)))
}

/// Go `GCSStorage`'s client: what its `CopyFrom` sends.
pub(crate) struct GcsClient {
    pub http: reqwest12::Client,
    pub token: Option<Arc<reqsign::GoogleTokenLoader>>,
    /// The JSON API's base, `opendal`'s endpoint.
    pub endpoint: String,
}

/// Go `url.PathEscape`, which the Go storage client applies to bucket and
/// object names: the unreserved bytes and `$&+:=@` stay, `/` and every
/// other byte is escaped.
fn escape_segment(segment: &str) -> String {
    let mut escaped = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'$' | b'&' | b'+' | b':' | b'=' | b'@') {
            escaped.push(byte as char);
        } else {
            escaped.push_str(&format!("%{byte:02X}"));
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    /// Go `url.PathEscape("p/a b+c=d;e?f")`.
    #[test]
    fn escape_segment_is_go_path_escape() {
        assert_eq!(super::escape_segment("p/a b+c=d;e?f,g@h"), "p%2Fa%20b+c=d%3Be%3Ff%2Cg@h");
    }
}

impl GcsClient {
    /// Go `ObjectHandle.CopierFrom(src).Run`: the rewrite of `src_bucket` /
    /// `src_name` to `dst_bucket` / `dst_name`, repeated with its rewrite
    /// token until the service reports it done.
    pub(crate) async fn rewrite(&self, src_bucket: &str, src_name: &str, dst_bucket: &str, dst_name: &str) -> Result<()> {
        let base = format!(
            "{}/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
            self.endpoint.trim_end_matches('/'),
            escape_segment(src_bucket),
            escape_segment(src_name),
            escape_segment(dst_bucket),
            escape_segment(dst_name),
        );
        let mut rewrite_token: Option<String> = None;
        loop {
            let url = match &rewrite_token {
                Some(token) => format!("{base}?rewriteToken={}", escape_segment(token)),
                None => base.clone(),
            };
            let mut req = http::Request::post(url)
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(b"{}".to_vec())
                .map_err(|_| Error::other("invalid GCS rewrite request"))?;
            if let Some(loader) = &self.token {
                let token = loader.load().await.map_err(|_| Error::other("cannot load the GCS token"))?;
                if let Some(token) = token {
                    reqsign::GoogleSigner::new("storage")
                        .sign(&mut req, &token)
                        .map_err(|_| Error::other("cannot sign the GCS request"))?;
                }
            }
            let req = reqwest12::Request::try_from(req).map_err(|_| Error::other("invalid GCS rewrite request"))?;
            let resp = self.http.execute(req).await.map_err(|e| Error::other(format!("GCS rewrite: {}", e.without_url())))?;
            let status = resp.status();
            let body = resp.bytes().await.unwrap_or_default();
            if !status.is_success() {
                let reason = serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.pointer("/error/message").and_then(|m| m.as_str()).map(str::to_owned))
                    .unwrap_or_default();
                return Err(Error::other(format!("GCS rewrite: status {}, {reason}", status.as_u16())));
            }
            let response: serde_json::Value =
                serde_json::from_slice(&body).map_err(|_| Error::other("GCS rewrite: an unreadable response"))?;
            if response.get("done").and_then(serde_json::Value::as_bool).unwrap_or(false) {
                return Ok(());
            }
            let Some(token) = response.get("rewriteToken").and_then(serde_json::Value::as_str) else {
                return Err(Error::other("GCS rewrite: not done and no rewrite token"));
            };
            rewrite_token = Some(token.to_owned());
        }
    }
}
