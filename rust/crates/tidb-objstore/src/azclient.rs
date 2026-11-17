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

//! Go `AzureBlobStorage`'s blob client for what `opendal` cannot send:
//! `StartCopyFromURL` from a blob of any container, and the properties that
//! report the copy's progress.

use std::time::Duration;

use crate::error::{berrors, Error, Result};

/// Go `azblobPremisedCopySpeedPerSecond / 1000`: 100 MiB a second.
const PREMISED_COPY_SPEED_PER_MILLISECOND: u64 = 100 * 1024 * 1024 / 1000;
/// Go `azblobCopyPollPendingMinimalDuration`.
const COPY_POLL_PENDING_MINIMAL_DURATION: Duration = Duration::from_secs(2);
/// The Blob service version the requests ask for.
const API_VERSION: &str = "2023-11-03";

/// Go `AzureBlobStorage`'s container client: the storage's endpoint,
/// container and credential, as `opendal`'s `Azblob` service holds them.
pub(crate) struct AzureClient {
    pub http: reqwest12::Client,
    /// Go `resolvedServiceEndpoint`.
    pub endpoint: String,
    pub container: String,
    /// The `AzureStorageLoader` configuration `opendal` builds its credential
    /// from: the account and its shared key or SAS token.
    pub credential: reqsign::AzureStorageConfig,
}

/// Go `urlOfObjectByEndpoint`: `path.Join(endpoint's path, container,
/// object)` on the endpoint.
pub(crate) fn url_of_object_by_endpoint(endpoint: &str, container: &str, object: &str) -> Result<String> {
    let mut url = reqwest12::Url::parse(endpoint).map_err(|_| Error::other(format!("{endpoint} isn't a valid url")))?;
    let path = crate::gopath::join(&[url.path(), container, object]);
    url.set_path(&path);
    Ok(url.to_string())
}

/// Go `progress`: the `finished/total` bytes of a pending copy.
fn progress(text: &str) -> Result<(u64, u64)> {
    let parse = || -> Option<(u64, u64)> {
        let (finished, total) = text.split_once('/')?;
        Some((finished.trim().parse().ok()?, total.trim().parse().ok()?))
    };
    parse().ok_or_else(|| Error::other(format!("failed to parse progress {text}")))
}

impl AzureClient {
    async fn send(&self, mut req: http::Request<Vec<u8>>) -> Result<reqwest12::Response> {
        let now = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let headers = req.headers_mut();
        headers.insert("x-ms-version", http::HeaderValue::from_static(API_VERSION));
        headers.insert("x-ms-date", now.parse().map_err(|_| Error::other("invalid Azure request date"))?);
        let credential = reqsign::AzureStorageLoader::new(self.credential.clone())
            .load()
            .await
            .map_err(|_| Error::other("cannot load the Azure credential"))?;
        if let Some(credential) = credential {
            reqsign::AzureStorageSigner::new()
                .sign(&mut req, &credential)
                .map_err(|_| Error::other("cannot sign the Azure request"))?;
        }
        let req = reqwest12::Request::try_from(req).map_err(|_| Error::other("invalid Azure request"))?;
        self.http.execute(req).await.map_err(|e| Error::other(format!("Azure: {}", e.without_url())))
    }

    fn header(resp: &reqwest12::Response, name: &str) -> String {
        resp.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned()
    }

    /// The service's error code of a failed response.
    async fn failure(resp: reqwest12::Response) -> Error {
        let status = resp.status().as_u16();
        let code = Self::header(&resp, "x-ms-error-code");
        Error::other(format!("Azure: status {status}, code {code}"))
    }

    /// Go `AzureBlobStorage.CopyFrom` past its source check: a
    /// `StartCopyFromURL` of `src_url` to blob `dst`, then the blob's
    /// properties until the copy with that ID succeeds, fails or is
    /// aborted, waiting between polls as Go does.
    pub(crate) async fn copy_from_url(&self, src_url: &str, dst: &str) -> Result<()> {
        let dst_url = url_of_object_by_endpoint(&self.endpoint, &self.container, dst)?;
        let req = http::Request::put(dst_url.as_str())
            .header("x-ms-copy-source", src_url)
            .header(http::header::CONTENT_LENGTH, "0")
            .body(Vec::new())
            .map_err(|_| Error::other("invalid Azure copy request"))?;
        let resp = self.send(req).await.map_err(|e| e.annotate(format!("failed to copy blob from {src_url} to {dst}")))?;
        if !resp.status().is_success() {
            return Err(Self::failure(resp).await.annotate(format!("failed to copy blob from {src_url} to {dst}")));
        }
        let copy_id = Self::header(&resp, "x-ms-copy-id");
        loop {
            let req = http::Request::head(dst_url.as_str())
                .body(Vec::new())
                .map_err(|_| Error::other("invalid Azure properties request"))?;
            let resp = self.send(req).await.map_err(|e| e.annotate("failed to check asynchronous copy status"))?;
            if !resp.status().is_success() {
                return Err(Self::failure(resp).await.annotate("failed to check asynchronous copy status"));
            }
            let prop_copy_id = Self::header(&resp, "x-ms-copy-id");
            if prop_copy_id != copy_id {
                return Err(berrors::annotate(
                    berrors::ERR_STORAGE_UNKNOWN,
                    format!(
                        "failed to check copy status: copy ID not match; copy id = {copy_id}; resp.copyID = {prop_copy_id};"
                    ),
                ));
            }
            match Self::header(&resp, "x-ms-copy-status").as_str() {
                "success" => return Ok(()),
                "failed" | "aborted" => {
                    return Err(berrors::annotate(
                        berrors::ERR_STORAGE_UNKNOWN,
                        format!(
                            "asynchronous copy failed or aborted: {}",
                            Self::header(&resp, "x-ms-copy-status-description")
                        ),
                    ))
                }
                "pending" => {
                    let (finished, total) = progress(&Self::header(&resp, "x-ms-copy-progress"))
                        .map_err(|e| e.annotate("failed to parse progress"))?;
                    let remaining = total.saturating_sub(finished);
                    let to_sleep = Duration::from_millis(remaining / PREMISED_COPY_SPEED_PER_MILLISECOND)
                        .max(COPY_POLL_PENDING_MINIMAL_DURATION);
                    tokio::time::sleep(to_sleep).await;
                }
                other => {
                    return Err(berrors::annotate(
                        berrors::ERR_STORAGE_UNKNOWN,
                        format!("unknown copy status: {other}"),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `urlOfObjectByEndpoint` and `progress`.
    #[test]
    fn object_url_and_progress_follow_go() {
        assert_eq!(
            url_of_object_by_endpoint("http://127.0.0.1:10000/devstoreaccount1", "c", "p/a b.sst").unwrap(),
            "http://127.0.0.1:10000/devstoreaccount1/c/p/a%20b.sst"
        );
        assert_eq!(
            url_of_object_by_endpoint("https://acct.blob.core.windows.net", "c", "o").unwrap(),
            "https://acct.blob.core.windows.net/c/o"
        );
        assert_eq!(progress("10/20").unwrap(), (10, 20));
        assert!(progress("x").is_err());
    }
}
