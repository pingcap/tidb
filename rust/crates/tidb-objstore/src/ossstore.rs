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

//! Go `pkg/objstore/ossstore`: an S3 backend whose provider is OSS
//! (`s3like.OSSProvider`) is an `s3like.Storage` over the OSS SDK client.
//! `opendal`'s OSS service carries the data; every request it sends is signed
//! here with the OSS V4 signer (`tidb-oss-sdk`), as the SDK client signs it,
//! with Go's static, default-chain or assumed-role credentials and their
//! refresher.

use std::sync::Arc;
use std::time::Duration;

use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use tidb_gocontext::Context;
use tidb_proto::backup::S3;

use crate::cloud::{self, CloudClient, CloudStorage, Flavor, RUNTIME};
use crate::error::{berrors, check_ctx, Error, Result};
use crate::storeapi::{BucketPrefix, Options, Permission};

/// Go `defaultRegion`.
const DEFAULT_REGION: &str = "cn-hangzhou";

/// Go `noSuchKey`.
const NO_SUCH_KEY: &str = "NoSuchKey";

/// The OSS SDK V4 signer under `opendal`'s OSS service: Go's `client`
/// upload options (the storage class on an upload) and its signature.
struct Transport {
    client: reqwest12::Client,
    credentials: Arc<tidb_oss_sdk::credentials::Credentials>,
    region: String,
    bucket: String,
    path_style: bool,
    class: String,
}

impl HttpFetch for Transport {
    async fn fetch(&self, mut req: http::Request<opendal::Buffer>) -> opendal::Result<http::Response<HttpBody>> {
        let error = || opendal::Error::new(opendal::ErrorKind::Unexpected, "OSS request signing failed");
        let mut url = url::Url::parse(&req.uri().to_string()).map_err(|_| error())?;
        let upload = req.method() == http::Method::POST && url.query_pairs().any(|(key, _)| key == "uploads");
        if !upload {
            req.headers_mut().remove("x-oss-server-side-encryption");
            req.headers_mut().remove("x-oss-server-side-encryption-key-id");
        } else if !self.class.is_empty() {
            req.headers_mut().insert("x-oss-storage-class", self.class.parse().map_err(|_| error())?);
        }
        let path = percent_encoding::percent_decode_str(url.path()).decode_utf8().map_err(|_| error())?;
        let key = if self.path_style {
            path.strip_prefix(&format!("/{}/", self.bucket)).unwrap_or("")
        } else {
            path.trim_start_matches('/')
        }
        .to_owned();
        let cred = self.credentials.load().await.map_err(|_| error())?.ok_or_else(error)?;
        let method = req.method().as_str().to_owned();
        req.headers_mut().remove(http::header::AUTHORIZATION);
        tidb_oss_sdk::signer::sign(
            &method,
            &mut url,
            req.headers_mut(),
            &self.bucket,
            &key,
            &self.region,
            &cred,
            None,
            chrono::Utc::now(),
        )
        .map_err(|_| error())?;
        *req.uri_mut() = url.as_str().parse().map_err(|_| error())?;
        HttpClient::with(self.client.clone()).fetch(req).await
    }
}

/// Go `client`'s requests `opendal` cannot make: the presigned GET
/// (`PresignObject`) and the cross-bucket `CopyObject`, and the credentials'
/// lifecycle (`OSSStore.Close`).
pub(crate) struct OssClient {
    http: reqwest12::Client,
    credentials: Arc<tidb_oss_sdk::credentials::Credentials>,
    /// The detected bucket region.
    region: String,
    /// The endpoint presigned URLs name (not the internal one).
    endpoint: String,
    /// The endpoint data requests go to.
    data_endpoint: String,
    path_style: bool,
}

impl OssClient {
    /// The URL of `key` in `bucket` under `endpoint`.
    fn object_url(&self, endpoint: &str, bucket: &str, key: &str) -> Result<url::Url> {
        let mut target = url::Url::parse(endpoint).map_err(|_| Error::other("invalid OSS endpoint"))?;
        if self.path_style {
            target.set_path(&format!("/{bucket}/{key}"));
        } else {
            let host = format!("{bucket}.{}", target.host_str().unwrap_or(""));
            target.set_host(Some(&host)).map_err(|_| Error::other("invalid OSS endpoint"))?;
            target.set_path(&format!("/{key}"));
        }
        Ok(target)
    }

    async fn credential(&self) -> Result<reqsign::AliyunCredential> {
        self.credentials
            .load()
            .await
            .map_err(|()| Error::other("OSS credentials unavailable"))?
            .ok_or_else(|| Error::other("OSS credentials unavailable"))
    }

    /// Go `client.PresignObject`.
    pub(crate) fn presign(&self, bucket: &str, key: &str, expire: Duration) -> Result<String> {
        let mut target = self.object_url(&self.endpoint, bucket, key)?;
        RUNTIME.block_on(async {
            let cred = self.credential().await?;
            tidb_oss_sdk::signer::sign(
                "GET",
                &mut target,
                &mut http::HeaderMap::new(),
                bucket,
                key,
                &self.region,
                &cred,
                Some(expire),
                chrono::Utc::now(),
            )
            .map_err(|()| Error::other("OSS presigning failed"))?;
            Ok(target.to_string())
        })
    }

    /// Go `client.CopyObject`: `SourceBucket` / `SourceKey` into this bucket.
    pub(crate) async fn copy_object(&self, from_bucket: &str, from_key: &str, bucket: &str, to_key: &str) -> Result<()> {
        let mut target = self.object_url(&self.data_endpoint, bucket, to_key)?;
        let mut headers = http::HeaderMap::new();
        let source = format!("/{from_bucket}/{}", opendal::raw::percent_encode_path(from_key));
        headers.insert("x-oss-copy-source", source.parse().map_err(|_| Error::other("invalid OSS copy source"))?);
        let cred = self.credential().await?;
        tidb_oss_sdk::signer::sign("PUT", &mut target, &mut headers, bucket, to_key, &self.region, &cred, None, chrono::Utc::now())
            .map_err(|()| Error::other("OSS signing failed"))?;
        let response = self
            .http
            .put(target)
            .headers(headers)
            .header(http::header::CONTENT_LENGTH, "0")
            .send()
            .await
            .map_err(|_| Error::other("OSS CopyObject request failed"))?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(Error::other(format!("OSS CopyObject: HTTP {}", response.status().as_u16())))
    }

    /// Go `OSSStore.Close`: stop the credential refresher.
    pub(crate) fn close(&self) {
        RUNTIME.block_on(self.credentials.close());
    }
}

impl Drop for OssClient {
    /// A storage dropped without `Close` stops its refresher too.
    fn drop(&mut self) {
        self.credentials.abort_refresh();
    }
}

/// Go `canUseInternalEndpoint`.
fn can_use_internal_endpoint(ecs_region: &str, bucket_region: &str) -> bool {
    !ecs_region.is_empty() && ecs_region == bucket_region
}

/// Go `trimOSSRegionID`.
fn trim_oss_region_id(region: &str) -> &str {
    region.strip_prefix("oss-").unwrap_or(region)
}

/// Runs `future` on the storage runtime, stopping when `ctx` is done.
fn run_ctx<T>(ctx: &Context, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    check_ctx(ctx)?;
    RUNTIME.block_on(async {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result,
                () = tokio::time::sleep(Duration::from_millis(10)) => check_ctx(ctx)?,
            }
        }
    })
}

/// A signed GET of `target` (`key` names the object, empty for a bucket
/// request): its status and body.
async fn signed_get(
    http: &reqwest12::Client,
    credentials: &tidb_oss_sdk::credentials::Credentials,
    mut target: url::Url,
    bucket: &str,
    key: &str,
    region: &str,
) -> Result<(u16, Vec<u8>)> {
    let cred = credentials
        .load()
        .await
        .map_err(|()| Error::other("failed to get credentials from default provider"))?
        .ok_or_else(|| Error::other("failed to get credentials from default provider"))?;
    let mut headers = http::HeaderMap::new();
    tidb_oss_sdk::signer::sign("GET", &mut target, &mut headers, bucket, key, region, &cred, None, chrono::Utc::now())
        .map_err(|()| Error::other("OSS signing failed"))?;
    let response = http.get(target).headers(headers).send().await.map_err(|_| Error::other("HTTP request failed"))?;
    let status = response.status().as_u16();
    let body = response.bytes().await.map_err(|_| Error::other("invalid OSS response"))?.to_vec();
    Ok((status, body))
}

/// Go `s3like.CheckPermissions` over the OSS `client`'s checks.
fn check_permissions(ctx: &Context, client: &OssClient, storage: &CloudStorage, perms: &[Permission]) -> Result<()> {
    let bucket = storage.bucket.as_str();
    for perm in perms {
        let result = match perm {
            // Go `IsBucketExist`: the SDK's GetBucketAcl.
            Permission::AccessBuckets => run_ctx(ctx, async {
                let mut target = client.object_url(&client.data_endpoint, bucket, "")?;
                target.set_query(Some("acl"));
                match signed_get(&client.http, &client.credentials, target, bucket, "", &client.region).await? {
                    (status, _) if (200..300).contains(&status) => Ok(()),
                    (status, _) => Err(Error::other(format!("HTTP {status}"))),
                }
            }),
            Permission::ListObjects => run_ctx(ctx, async {
                let mut target = client.object_url(&client.data_endpoint, bucket, "")?;
                target
                    .query_pairs_mut()
                    .append_pair("list-type", "2")
                    .append_pair("max-keys", "1")
                    .append_pair("prefix", storage.bucket_prefix.prefix_str());
                match signed_get(&client.http, &client.credentials, target, bucket, "", &client.region).await? {
                    (status, _) if (200..300).contains(&status) => Ok(()),
                    (status, _) => Err(Error::other(format!("HTTP {status}"))),
                }
            }),
            Permission::GetObject => run_ctx(ctx, async {
                let key = storage.bucket_prefix.object_key(&format!("perm-check/{}", uuid::Uuid::new_v4()));
                let target = client.object_url(&client.data_endpoint, bucket, &key)?;
                match signed_get(&client.http, &client.credentials, target, bucket, &key, &client.region).await? {
                    (status, _) if (200..300).contains(&status) => Ok(()),
                    (status, body) => {
                        #[derive(serde::Deserialize)]
                        struct ServiceError {
                            #[serde(rename = "Code")]
                            code: String,
                        }
                        if quick_xml::de::from_reader::<_, ServiceError>(body.as_slice()).is_ok_and(|e| e.code == NO_SUCH_KEY) {
                            return Ok(());
                        }
                        Err(Error::other(format!("HTTP {status}")))
                    }
                }
            }),
            Permission::PutObject | Permission::PutAndDeleteObject => {
                let key = storage.bucket_prefix.object_key(&format!("perm-check/{}", uuid::Uuid::new_v4()));
                let put = cloud::run(ctx, storage.op.write(&key, b"check".to_vec())).map(|_| ());
                // Go deletes the probe object even when the put failed.
                let delete = cloud::run(ctx, storage.op.delete(&key));
                put.and(delete)
            }
        };
        result.map_err(|e| {
            berrors::annotate(
                berrors::ERR_STORAGE_INVALID_PERMISSION,
                format!("check permission failed due to permission {}: {e}", perm.as_str()),
            )
        })?;
    }
    Ok(())
}

/// Go `NewOSSStorage`.
pub(crate) fn new_oss_storage(ctx: &Context, s3: &S3, opts: &Options) -> Result<CloudStorage> {
    check_ctx(ctx)?;
    let bucket = s3.bucket.as_str();
    if bucket.is_empty() {
        return Err(berrors::annotate(berrors::ERR_STORAGE_INVALID_CONFIG, "bucket name is empty"));
    }
    let http = crate::s3client::http_client()?;
    let static_keys = !s3.access_key.is_empty() && !s3.secret_access_key.is_empty();
    let credentials = Arc::new(tidb_oss_sdk::credentials::Credentials::new(
        if static_keys { &s3.access_key } else { "" },
        if static_keys { &s3.secret_access_key } else { "" },
        if static_keys { &s3.session_token } else { "" },
        if static_keys { "" } else { &s3.role_arn },
        &s3.external_id,
    ));
    let ecs_region = RUNTIME
        .block_on(credentials.ecs_region())
        .map_err(|()| Error::other("failed to get initial OSS credentials"))?;
    let region = if s3.region.is_empty() { DEFAULT_REGION } else { &s3.region };
    let endpoint = if s3.endpoint.is_empty() { format!("https://oss-{region}.aliyuncs.com") } else { s3.endpoint.clone() };
    let path_style = url::Url::parse(&endpoint)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.parse::<std::net::IpAddr>().is_ok()))
        .unwrap_or(false);
    let mut probe = OssClient {
        http: http.clone(),
        credentials: Arc::clone(&credentials),
        region: region.to_owned(),
        endpoint: endpoint.clone(),
        data_endpoint: endpoint.clone(),
        path_style,
    };
    // Go `GetBucketLocation`, then the region check.
    let mut location = probe.object_url(&endpoint, bucket, "")?;
    location.set_query(Some("location"));
    let detected = run_ctx(ctx, async {
        let (status, body) = signed_get(&http, &credentials, location, bucket, "", region).await?;
        if !(200..300).contains(&status) {
            return Err(Error::other(format!("HTTP {status}")));
        }
        #[derive(serde::Deserialize)]
        struct Location {
            #[serde(rename = "$text")]
            region: String,
        }
        let location: Location =
            quick_xml::de::from_reader(body.as_slice()).map_err(|_| Error::other("invalid OSS location response"))?;
        Ok(trim_oss_region_id(&location.region).to_owned())
    })
    .map_err(|e| e.annotate(format!("failed to get location of bucket {bucket}")))?;
    if !s3.region.is_empty() && s3.region != detected {
        return Err(Error::other(format!(
            "bucket and region are not matched, bucket={bucket}, input region={}, real region={detected}",
            s3.region
        )));
    }
    let endpoint = if s3.endpoint.is_empty() { format!("https://oss-{detected}.aliyuncs.com") } else { endpoint };
    let data_endpoint = if s3.endpoint.is_empty() && can_use_internal_endpoint(&ecs_region, &detected) {
        format!("https://oss-{detected}-internal.aliyuncs.com")
    } else {
        endpoint.clone()
    };
    let transport = Transport {
        client: http.clone(),
        credentials: Arc::clone(&credentials),
        region: detected.clone(),
        bucket: bucket.to_owned(),
        path_style,
        class: s3.storage_class.clone(),
    };
    let op = {
        let _guard = RUNTIME.enter();
        let mut builder = opendal::services::Oss::default().allow_anonymous().bucket(bucket).endpoint(&data_endpoint);
        if path_style {
            builder = builder.addressing_style("path");
        }
        if !s3.sse.is_empty() {
            builder = builder.server_side_encryption(&s3.sse);
        }
        if !s3.sse_kms_key_id.is_empty() {
            builder = builder.server_side_encryption_key_id(&s3.sse_kms_key_id);
        }
        opendal::Operator::new(builder)
            .map_err(|e| cloud::native_error(&e))?
            .layer(opendal::layers::HttpClientLayer::new(HttpClient::with(transport)))
            .finish()
    };
    probe.region = detected;
    probe.endpoint = endpoint;
    probe.data_endpoint = data_endpoint;
    let prefix = crate::storeapi::Prefix::new(&s3.prefix).as_str().to_owned();
    let storage = CloudStorage {
        op,
        flavor: Flavor::S3,
        bucket: bucket.to_owned(),
        raw_prefix: prefix.clone(),
        bucket_prefix: BucketPrefix::new(bucket, &prefix),
        access_rec: opts.access_recording.clone(),
        client: CloudClient::Oss(probe),
    };
    if let CloudClient::Oss(client) = &storage.client {
        check_permissions(ctx, client, &storage, &opts.check_permissions)?;
    }
    if !static_keys {
        RUNTIME
            .block_on(credentials.start_refresh())
            .map_err(|()| Error::other("failed to start OSS credential refresher"))?;
    }
    Ok(storage)
}

#[cfg(test)]
mod tests {
    /// Go `ossstore.TestCanUseInternalEndpoint`.
    #[test]
    fn test_can_use_internal_endpoint() {
        assert!(!super::can_use_internal_endpoint("", "cn-hangzhou"));
        assert!(!super::can_use_internal_endpoint("cn-beijing", "cn-hangzhou"));
        assert!(super::can_use_internal_endpoint("cn-hangzhou", "cn-hangzhou"));
    }

    /// Go `trimOSSRegionID`.
    #[test]
    fn test_trim_oss_region_id() {
        assert_eq!(super::trim_oss_region_id("oss-cn-hangzhou"), "cn-hangzhou");
        assert_eq!(super::trim_oss_region_id("cn-hangzhou"), "cn-hangzhou");
    }
}
