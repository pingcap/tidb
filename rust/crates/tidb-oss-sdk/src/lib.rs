// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `github.com/aliyun/alibabacloud-oss-go-sdk-v2` as consumed by TiDB.
mod assume_role;
pub mod credentials;
mod default_provider;
pub mod signer;
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use std::sync::Arc;
/// Go OSS SDK HTTP transport: every request is signed with OSS V4 credentials.
pub struct Transport {
    /// Go `Config.HTTPClient`.
    pub client: reqwest12::Client,
    /// Go `Config.CredentialsProvider`.
    pub credentials: Arc<credentials::Credentials>,
    /// Go `Config.Region`.
    pub region: String,
    /// Go request `Bucket`.
    pub bucket: String,
    /// Go SDK IP endpoint addressing.
    pub path_style: bool,
}
impl HttpFetch for Transport {
    /// Go `SignerV4.Sign` at the final request boundary.
    async fn fetch(
        &self,
        mut request: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let error =
            || opendal::Error::new(opendal::ErrorKind::Unexpected, "OSS request signing failed");
        let mut url = url::Url::parse(&request.uri().to_string()).map_err(|_| error())?;
        let path = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_err(|_| error())?;
        let key = if self.path_style {
            path.strip_prefix(&format!("/{}/", self.bucket))
                .unwrap_or("")
        } else {
            path.trim_start_matches('/')
        }
        .to_owned();
        let credential = self
            .credentials
            .load()
            .await
            .map_err(|_| error())?
            .ok_or_else(error)?;
        request.headers_mut().remove(http::header::AUTHORIZATION);
        let method = request.method().as_str().to_owned();
        signer::sign(
            &method,
            &mut url,
            request.headers_mut(),
            &self.bucket,
            &key,
            &self.region,
            &credential,
            None,
            chrono::Utc::now(),
        )
        .map_err(|_| error())?;
        *request.uri_mut() = url.as_str().parse().map_err(|_| error())?;
        HttpClient::with(self.client.clone()).fetch(request).await
    }
}
