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

//! Go `s3store.s3Client`: the credentials an S3 storage signs with, and the
//! `CopyObject` request, which `opendal` cannot make for a source in another
//! bucket (its copy source is always its own bucket).

use std::sync::Arc;

use tidb_proto::backup::S3;

use crate::error::{Error, Result};
use crate::storeapi::Options;

/// The AWS credentials an S3 storage signs with; `None` is anonymous.
pub(crate) type Credentials = Option<Arc<dyn reqsign::AwsCredentialLoad>>;

/// One credential loader handed to `opendal` and to [`S3Client`], so both
/// sign with the same, once-loaded, credentials.
pub(crate) struct SharedCredentials(pub Arc<dyn reqsign::AwsCredentialLoad>);

#[async_trait::async_trait]
impl reqsign::AwsCredentialLoad for SharedCredentials {
    async fn load_credential(&self, client: reqwest12::Client) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        self.0.load_credential(client).await
    }
}

/// The client every S3 request is sent with.
///
/// Go `http.DefaultTransport`'s `ProxyFromEnvironment`: `HTTP_PROXY` /
/// `HTTPS_PROXY` minus `NO_PROXY`, never for a loopback host, and never the
/// operating system's proxy settings (which reqwest's `system-proxy`
/// feature, enabled elsewhere in the build, would otherwise read).
pub(crate) fn http_client() -> Result<reqwest12::Client> {
    let env = |upper: &str, lower: &str| {
        std::env::var(upper).ok().filter(|v| !v.is_empty()).or_else(|| std::env::var(lower).ok().filter(|v| !v.is_empty()))
    };
    let mut builder = reqwest12::Client::builder().no_proxy();
    for (scheme, proxy) in [("http", env("HTTP_PROXY", "http_proxy")), ("https", env("HTTPS_PROXY", "https_proxy"))] {
        let Some(proxy) = proxy else { continue };
        let proxy = reqwest12::Url::parse(&proxy)
            .or_else(|_| reqwest12::Url::parse(&format!("http://{proxy}")))
            .map_err(|_| Error::other("invalid proxy URL"))?;
        let custom = reqwest12::Proxy::custom(move |url| {
            let loopback = match url.host() {
                Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                None => true,
            };
            (url.scheme() == scheme && !loopback).then(|| proxy.clone())
        });
        builder = builder.proxy(custom.no_proxy(reqwest12::NoProxy::from_env()));
    }
    builder.build().map_err(|_| Error::other("cannot create the S3 HTTP client"))
}

/// Go `NewS3Storage`'s credentials: an explicit profile
/// (`WithSharedConfigProfile`) wins; else `autoNewCred`'s static keys; else
/// `LoadDefaultConfig`'s chain (environment, `AWS_PROFILE`, credential
/// process, web identity, instance metadata). A role ARN assumes the role
/// with those as its source. KS3 (`NewKS3Storage`) reads the static keys,
/// the environment and the shared profile only.
pub(crate) fn credentials(s3: &S3, region: &str, opts: &Options, http: &reqwest12::Client, ks3: bool) -> Result<Credentials> {
    if opts.no_credentials {
        return Ok(None);
    }
    let static_keys = !s3.access_key.is_empty() && !s3.secret_access_key.is_empty();
    let mut cfg = reqsign::AwsConfig::default().from_profile().from_env();
    cfg.region = Some(region.to_owned());
    if static_keys {
        cfg.access_key_id = Some(s3.access_key.clone());
        cfg.secret_access_key = Some(s3.secret_access_key.clone());
        cfg.session_token = (!s3.session_token.is_empty()).then(|| s3.session_token.clone());
    }
    let base: Arc<dyn reqsign::AwsCredentialLoad> = if !s3.profile.is_empty() {
        Arc::new(tidb_aws_sdk::ProfileCredentials::new(&s3.profile, region))
    } else if static_keys || ks3 {
        Arc::new(reqsign::AwsDefaultLoader::new(http.clone(), cfg.clone()))
    } else {
        Arc::new(crate::cloud::RUNTIME.block_on(tidb_aws_sdk::ProfileCredentials::default_chain(region)))
    };
    if s3.role_arn.is_empty() {
        return Ok(Some(base));
    }
    let assume_role = reqsign::AwsConfig {
        region: Some(region.to_owned()),
        role_arn: Some(s3.role_arn.clone()),
        external_id: (!s3.external_id.is_empty()).then(|| s3.external_id.clone()),
        sts_regional_endpoints: "regional".to_owned(),
        ..reqsign::AwsConfig::default()
    };
    let loader = reqsign::AwsAssumeRoleLoader::new(http.clone(), assume_role, Box::new(SharedCredentials(base)))
        .map_err(|_| Error::other("the S3 assume-role credentials are misconfigured"))?;
    Ok(Some(Arc::new(loader)))
}

/// `opendal`'s S3 `build_endpoint`: the bucket's URL from the configured
/// endpoint (else the environment's, else AWS's regional one), in virtual
/// host or path style.
pub(crate) fn bucket_endpoint(s3: &S3, region: &str, opts: &Options) -> String {
    let configured = if !s3.endpoint.is_empty() {
        Some(s3.endpoint.clone())
    } else if opts.no_credentials {
        None
    } else {
        reqsign::AwsConfig::default().from_profile().from_env().endpoint_url
    };
    let mut endpoint = match configured {
        Some(endpoint) if endpoint.starts_with("http") => endpoint,
        Some(endpoint) => format!("https://{endpoint}"),
        None => "https://s3.amazonaws.com".to_owned(),
    };
    endpoint = endpoint.replace(&format!("//{}.", s3.bucket), "//");
    if let Ok(url) = reqwest12::Url::parse(&endpoint) {
        endpoint = url.to_string().trim_end_matches('/').to_owned();
    }
    if endpoint == "https://s3.amazonaws.com" {
        endpoint = format!("https://s3.{region}.amazonaws.com");
    }
    if s3.force_path_style {
        format!("{endpoint}/{}", s3.bucket)
    } else {
        endpoint.replace("//", &format!("//{}.", s3.bucket))
    }
}

/// Go `s3Client`: what its `CopyObject` sends.
pub(crate) struct S3Client {
    pub http: reqwest12::Client,
    pub credentials: Credentials,
    pub region: String,
    /// The bucket's URL ([`bucket_endpoint`]).
    pub endpoint: String,
    pub sse: String,
    pub sse_kms_key_id: String,
    pub sse_kms_encryption_context: String,
    /// The bucket, for KS3's V2 signature.
    pub bucket: String,
    /// Go `KS3Storage`: V2-signed requests and ListObjects (V1).
    pub ks3: bool,
    pub path_style: bool,
}

/// Go `noSuchKey`.
const NO_SUCH_KEY: &str = "NoSuchKey";

impl S3Client {
    /// One signed request of a permission check: its status and body.
    async fn send_check(&self, method: reqwest12::Method, url: reqwest12::Url) -> Result<(u16, Vec<u8>)> {
        let mut request = self.http.request(method, url).build().map_err(|_| Error::other("invalid request"))?;
        if let Some(loader) = &self.credentials {
            let credential = loader
                .load_credential(self.http.clone())
                .await
                .map_err(|_| Error::other("cannot load S3 credentials"))?
                .ok_or_else(|| Error::other("S3 credentials not found"))?;
            if self.ks3 {
                let url = request.url().clone();
                let method = request.method().as_str().to_owned();
                crate::s3_transport::sign_v2(
                    &method,
                    &url,
                    request.headers_mut(),
                    &self.bucket,
                    self.path_style,
                    &credential,
                    chrono::Utc::now(),
                )
                .map_err(|()| Error::other("signing failed"))?;
            } else {
                reqsign::AwsV4Signer::new("s3", &self.region)
                    .sign(&mut request, &credential)
                    .map_err(|_| Error::other("signing failed"))?;
            }
        }
        let response = self.http.execute(request).await.map_err(|_| Error::other("HTTP request failed"))?;
        let status = response.status().as_u16();
        let body = response.bytes().await.map_err(|_| Error::other("HTTP response failed"))?.to_vec();
        Ok((status, body))
    }

    /// Go `s3Client.CheckBucketExistence` (`HeadBucket`),
    /// `CheckListObjects` (`ListObjectsV2`, V1 for KS3, one key under the
    /// prefix), `CheckGetObject` (a random key, `NoSuchKey` meaning the
    /// permission is there) and `CheckPutAndDeleteObject`.
    pub(crate) fn check_permission(
        &self,
        ctx: &tidb_gocontext::Context,
        perm: crate::storeapi::Permission,
        prefix: &crate::storeapi::BucketPrefix,
        op: &opendal::Operator,
    ) -> Result<()> {
        use crate::storeapi::Permission as P;
        if matches!(perm, P::PutObject | P::PutAndDeleteObject) {
            let key = prefix.object_key(&format!("perm-check/{}", uuid::Uuid::new_v4()));
            let put = crate::cloud::run(ctx, op.write(&key, b"check".to_vec())).map(|_| ());
            // Go deletes the probe even when the put failed.
            let delete = crate::cloud::run(ctx, op.delete(&key));
            return put.and(delete);
        }
        let bucket = reqwest12::Url::parse(&self.endpoint).map_err(|_| Error::other("invalid S3 bucket URL"))?;
        let (method, url) = match perm {
            P::AccessBuckets => (reqwest12::Method::HEAD, bucket),
            P::ListObjects => {
                let mut url = bucket;
                if !self.ks3 {
                    url.query_pairs_mut().append_pair("list-type", "2");
                }
                url.query_pairs_mut().append_pair("max-keys", "1").append_pair("prefix", prefix.prefix_str());
                (reqwest12::Method::GET, url)
            }
            _ => {
                let mut url = bucket;
                let key = prefix.object_key(&format!("perm-check/{}", uuid::Uuid::new_v4()));
                let path = format!("{}/{}", url.path().trim_end_matches('/'), opendal::raw::percent_encode_path(&key));
                url.set_path(&path);
                (reqwest12::Method::GET, url)
            }
        };
        let check = self.send_check(method, url);
        crate::error::check_ctx(ctx)?;
        let (status, body) = crate::cloud::RUNTIME.block_on(async {
            tokio::pin!(check);
            loop {
                tokio::select! {
                    result = &mut check => return result,
                    () = tokio::time::sleep(std::time::Duration::from_millis(20)) => crate::error::check_ctx(ctx)?,
                }
            }
        })?;
        if (200..300).contains(&status) {
            return Ok(());
        }
        #[derive(serde::Deserialize)]
        struct ServiceError {
            #[serde(rename = "Code")]
            code: String,
        }
        let code = quick_xml::de::from_reader::<_, ServiceError>(body.as_slice()).map(|e| e.code).unwrap_or_default();
        if perm == P::GetObject && code == NO_SUCH_KEY {
            return Ok(());
        }
        Err(Error::other(if code.is_empty() { format!("HTTP {status}") } else { format!("{code}: HTTP {status}") }))
    }
    /// Go `s3Client.CopyObject`: a server-side copy of `from_key` in
    /// `from_bucket` to `to_key` in this bucket, under this storage's SSE
    /// settings (a copy does not keep the source's encryption).
    pub(crate) async fn copy_object(&self, from_bucket: &str, from_key: &str, to_key: &str) -> Result<()> {
        let invalid = |what: &str| Error::other(format!("invalid S3 copy {what}"));
        let target = format!("{}/{}", self.endpoint, opendal::raw::percent_encode_path(to_key));
        let mut req = http::Request::put(target).header(http::header::CONTENT_LENGTH, "0");
        // Go `path.Join(FromLoc.Bucket, fromKey)`.
        let source = crate::gopath::join(&[from_bucket, from_key]);
        req = req.header("x-amz-copy-source", opendal::raw::percent_encode_path(&source));
        for (name, value) in [
            ("x-amz-server-side-encryption", &self.sse),
            ("x-amz-server-side-encryption-aws-kms-key-id", &self.sse_kms_key_id),
            ("x-amz-server-side-encryption-context", &self.sse_kms_encryption_context),
        ] {
            if !value.is_empty() {
                req = req.header(name, value.as_str());
            }
        }
        let mut req = req.body(Vec::new()).map_err(|_| invalid("request"))?;
        if let Some(credentials) = &self.credentials {
            let credential = credentials
                .load_credential(self.http.clone())
                .await
                .map_err(|_| Error::other("cannot load the S3 credentials"))?;
            if let Some(credential) = credential {
                reqsign::AwsV4Signer::new("s3", &self.region)
                    .sign(&mut req, &credential)
                    .map_err(|_| Error::other("cannot sign the S3 request"))?;
            }
        }
        let req = reqwest12::Request::try_from(req).map_err(|_| invalid("request"))?;
        let resp = self.http.execute(req).await.map_err(|e| Error::other(format!("S3 CopyObject: {}", e.without_url())))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        // A 200 can still carry an `<Error>` document.
        if status.is_success() && !body.contains("<Error>") {
            return Ok(());
        }
        let code = body
            .split_once("<Code>")
            .and_then(|(_, rest)| rest.split_once("</Code>"))
            .map_or("", |(code, _)| code);
        Err(Error::other(format!("S3 CopyObject: status {}, code {code}", status.as_u16())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `opendal`'s `build_endpoint`, which the copy request must address
    /// exactly as the storage's other requests do.
    #[test]
    fn bucket_endpoint_follows_opendal() {
        let opts = Options { no_credentials: true, ..Options::default() };
        let s3 = |endpoint: &str, path_style: bool| S3 {
            bucket: "bkt".into(),
            endpoint: endpoint.into(),
            force_path_style: path_style,
            ..S3::default()
        };
        assert_eq!(bucket_endpoint(&s3("", false), "us-west-2", &opts), "https://bkt.s3.us-west-2.amazonaws.com");
        assert_eq!(bucket_endpoint(&s3("", true), "us-west-2", &opts), "https://s3.us-west-2.amazonaws.com/bkt");
        assert_eq!(bucket_endpoint(&s3("http://127.0.0.1:9000/", true), "r", &opts), "http://127.0.0.1:9000/bkt");
        assert_eq!(bucket_endpoint(&s3("minio.local:443", true), "r", &opts), "https://minio.local/bkt");
        assert_eq!(bucket_endpoint(&s3("https://bkt.oss.example.com", false), "r", &opts), "https://bkt.oss.example.com");
    }
}
