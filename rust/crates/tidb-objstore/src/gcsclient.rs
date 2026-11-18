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

/// Where a GCS storage's tokens come from: a service account or external
/// account (`reqsign`'s loader), or an `authorized_user` refresh token, which
/// Go's `google.CredentialsFromJSON` also accepts and `reqsign` does not.
pub(crate) enum GcsToken {
    Reqsign(reqsign::GoogleTokenLoader),
    AuthorizedUser(AuthorizedUser, reqwest12::Client),
}

impl GcsToken {
    pub(crate) async fn load(&self) -> anyhow::Result<Option<reqsign::GoogleToken>> {
        match self {
            Self::Reqsign(loader) => loader.load().await,
            Self::AuthorizedUser(user, http) => reqsign::GoogleTokenLoad::load(user, http.clone()).await,
        }
    }
}

/// One token source handed to `opendal` and to [`GcsClient`], so both send
/// the same tokens.
pub(crate) struct SharedTokenLoader(pub Arc<GcsToken>);

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

/// An `authorized_user` credentials file (`gcloud auth application-default
/// login`): an OAuth2 refresh token exchanged at `token_uri`.
#[derive(serde::Deserialize)]
pub(crate) struct AuthorizedUser {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(default)]
    token_uri: String,
}

impl std::fmt::Debug for AuthorizedUser {
    /// Credentials never reach debug output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorizedUser { credentials: [REDACTED] }")
    }
}

#[async_trait::async_trait]
impl reqsign::GoogleTokenLoad for AuthorizedUser {
    /// Go `google.credentialsFile.tokenSource` for `authorized_user`: the
    /// OAuth2 refresh-token grant.
    async fn load(&self, client: reqwest12::Client) -> anyhow::Result<Option<reqsign::GoogleToken>> {
        let endpoint = if self.token_uri.is_empty() { "https://oauth2.googleapis.com/token" } else { &self.token_uri };
        let response = client
            .post(endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("refresh_token", &self.refresh_token),
            ])
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("GCS credential refresh failed"))?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("GCS credential refresh: HTTP {}", response.status().as_u16()));
        }
        #[derive(serde::Deserialize)]
        struct Token {
            access_token: String,
            expires_in: usize,
        }
        let bytes = response.bytes().await.map_err(|_| anyhow::anyhow!("invalid GCS credential response"))?;
        let token: Token =
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid GCS credential response"))?;
        Ok(Some(reqsign::GoogleToken::new(&token.access_token, token.expires_in, GCS_SCOPE)))
    }
}

/// The token source of a storage's credentials blob (Go
/// `option.WithCredentialsJSON`), else of the environment's and the
/// well-known location's credentials, else of the VM metadata; `None` is
/// anonymous (Go `option.WithoutAuthentication`).
pub(crate) fn token_loader(
    credentials_blob: &str,
    no_credentials: bool,
    http: &reqwest12::Client,
) -> Result<Option<Arc<GcsToken>>> {
    if no_credentials && credentials_blob.is_empty() {
        return Ok(None);
    }
    if !credentials_blob.is_empty() {
        let value: serde_json::Value =
            serde_json::from_str(credentials_blob).map_err(|_| Error::other("invalid GCS credentials JSON"))?;
        if value["type"] == "authorized_user" {
            let user: AuthorizedUser = serde_json::from_value(value)
                .map_err(|_| Error::other("invalid GCS authorized_user credentials"))?;
            return Ok(Some(Arc::new(GcsToken::AuthorizedUser(user, http.clone()))));
        }
    }
    let mut credentials = reqsign::GoogleCredentialLoader::default();
    if !credentials_blob.is_empty() {
        use base64::Engine as _;
        credentials = credentials.with_content(&base64::engine::general_purpose::STANDARD.encode(credentials_blob));
    }
    let mut loader = reqsign::GoogleTokenLoader::new(GCS_SCOPE, http.clone());
    match credentials.load() {
        Ok(Some(credential)) => loader = loader.with_credentials(credential),
        // Go `google.FindDefaultCredentials`: without an explicit, an
        // environment or a well-known credentials file, only Compute
        // Engine's metadata server remains.
        _ if credentials_blob.is_empty() && !on_gce(http) => {
            return Err(crate::berrors::annotate(
                crate::berrors::ERR_STORAGE_INVALID_CONFIG,
                "google: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information Or you should provide '--gcs.credentials-file'",
            ));
        }
        _ => {}
    }
    Ok(Some(Arc::new(GcsToken::Reqsign(loader))))
}

/// Go `metadata.OnGCE`, bounded: the metadata server (`GCE_METADATA_HOST`,
/// else `169.254.169.254`) answers with `Metadata-Flavor: Google`.
fn on_gce(http: &reqwest12::Client) -> bool {
    let endpoint = std::env::var("GCE_METADATA_HOST")
        .map(|host| format!("http://{host}/"))
        .unwrap_or_else(|_| "http://169.254.169.254/".to_owned());
    // reqwest arms the timeout when the request is sent, which needs the
    // runtime's timer.
    let probe = async {
        http.get(endpoint)
            .header("Metadata-Flavor", "Google")
            .timeout(std::time::Duration::from_millis(500))
            .send()
            .await
    };
    crate::cloud::RUNTIME.block_on(probe).is_ok_and(|response| {
        response.headers().get("Metadata-Flavor").is_some_and(|value| value == "Google")
    })
}

pub(crate) struct GcsClient {
    pub http: reqwest12::Client,
    pub token: Option<Arc<GcsToken>>,
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
