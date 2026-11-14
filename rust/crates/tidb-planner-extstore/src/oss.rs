// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `ossstore.NewOSSStorage` and its OSS v4 native transport.
use super::{cloud::RUNTIME, object_storage::ObjectStorage};
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use std::{sync::Arc, time::Duration};
struct Transport {
    client: reqwest12::Client,
    credentials: Arc<tidb_oss_sdk::credentials::Credentials>,
    region: String,
    bucket: String,
    path_style: bool,
    class: String,
}
impl HttpFetch for Transport {
    /// Go `ossstore.client` upload options and the OSS SDK V4 signer.
    async fn fetch(
        &self,
        mut req: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let error =
            || opendal::Error::new(opendal::ErrorKind::Unexpected, "OSS request signing failed");
        let mut url = url::Url::parse(&req.uri().to_string()).map_err(|_| error())?;
        let upload = req.method() == http::Method::POST
            && url.query_pairs().any(|(key, _)| key == "uploads");
        if !upload {
            req.headers_mut().remove("x-oss-server-side-encryption");
            req.headers_mut()
                .remove("x-oss-server-side-encryption-key-id");
        }
        if upload {
            for (name, value) in [("x-oss-storage-class", &self.class)] {
                if !value.is_empty() {
                    req.headers_mut()
                        .insert(name, value.parse().map_err(|_| error())?);
                }
            }
        }
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
        let cred = self
            .credentials
            .load()
            .await
            .map_err(|_| error())?
            .ok_or_else(error)?;
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
/// Go `NewOSSStorage`: GetBucketLocation, region validation, then the data client.
pub(super) fn new(
    url: &url::Url,
    prefix: &str,
    context: Option<&tidb_gocontext::Context>,
) -> Result<Arc<ObjectStorage>, String> {
    let options = super::cloud::query_options(url);
    let get = |key: &str| options.get(key).map_or("", String::as_str);
    let bucket = url.host_str().unwrap_or("");
    if bucket.is_empty() {
        return Err("please specify the bucket for s3".into());
    }
    if get("access-key").is_empty() != get("secret-access-key").is_empty() {
        return Err("access_key and secret_access_key must both be set".into());
    }
    let client = reqwest12::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|_| "cannot create OSS HTTP client".to_owned())?;
    let credentials = Arc::new(tidb_oss_sdk::credentials::Credentials::new(
        get("access-key"),
        get("secret-access-key"),
        get("session-token"),
        if get("access-key").is_empty() {
            get("role-arn")
        } else {
            ""
        },
        get("external-id"),
    ));
    let ecs_region = RUNTIME
        .block_on(credentials.ecs_region())
        .map_err(|_| "failed to get OSS credentials or ECS region".to_owned())?;
    let region = if get("region").is_empty() {
        "cn-hangzhou"
    } else {
        get("region")
    };
    let endpoint = if get("endpoint").is_empty() {
        format!("https://oss-{region}.aliyuncs.com")
    } else {
        get("endpoint").to_owned()
    };
    let mut target = url::Url::parse(&endpoint).map_err(|_| "invalid OSS endpoint".to_owned())?;
    let path_style = target
        .host_str()
        .is_some_and(|host| host.parse::<std::net::IpAddr>().is_ok());
    if path_style {
        target.set_path(&format!("/{bucket}/"));
    } else {
        target
            .set_host(Some(&format!(
                "{bucket}.{}",
                target.host_str().unwrap_or("")
            )))
            .map_err(|_| "invalid OSS endpoint".to_owned())?;
        target.set_path("/");
    }
    target.set_query(Some("location"));
    let detected = super::cloud::run_with_context(context, async {
        let cred = credentials
            .load()
            .await
            .map_err(|_| "failed to get credentials from default provider".to_owned())?
            .ok_or_else(|| "failed to get credentials from default provider".to_owned())?;
        let mut headers = http::HeaderMap::new();
        tidb_oss_sdk::signer::sign(
            "GET",
            &mut target,
            &mut headers,
            bucket,
            "",
            region,
            &cred,
            None,
            chrono::Utc::now(),
        )
        .map_err(|_| "OSS signing failed".to_owned())?;
        let response = client
            .get(target)
            .headers(headers)
            .send()
            .await
            .map_err(|_| "failed to get location of bucket".to_owned())?;
        if !response.status().is_success() {
            return Err(format!(
                "failed to get location of bucket: HTTP {}",
                response.status().as_u16()
            ));
        }
        #[derive(serde::Deserialize)]
        struct Location {
            #[serde(rename = "$text")]
            region: String,
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| "invalid OSS location response".to_owned())?;
        let location: Location = quick_xml::de::from_reader(bytes.as_ref())
            .map_err(|_| "invalid OSS location response".to_owned())?;
        Ok(location
            .region
            .strip_prefix("oss-")
            .unwrap_or(&location.region)
            .to_owned())
    })?;
    if !get("region").is_empty() && get("region") != detected {
        return Err(format!(
            "bucket and region are not matched, bucket={bucket}, input region={}, real region={detected}",
            get("region")
        ));
    }
    let endpoint = if get("endpoint").is_empty() {
        format!("https://oss-{detected}.aliyuncs.com")
    } else {
        endpoint
    };
    let data_endpoint =
        if get("endpoint").is_empty() && can_use_internal_endpoint(&ecs_region, &detected) {
            format!("https://oss-{detected}-internal.aliyuncs.com")
        } else {
            endpoint.clone()
        };
    if let Some(ctx) = context {
        RUNTIME.block_on(async {
            for permission in ["ListObjects","GetObject","AccessBucket"] {
                if let Some(error)=ctx.err(){return Err(error.to_string());}
                let mut target=url::Url::parse(&data_endpoint).map_err(|_|"invalid OSS endpoint".to_owned())?;
                let prefix=prefix.trim_matches('/');let prefix=if prefix.is_empty(){String::new()}else{format!("{prefix}/")};
                let key=if permission=="GetObject"{format!("{prefix}perm-check/{}",uuid::Uuid::new_v4())}else{String::new()};
                if path_style{target.set_path(&format!("/{bucket}/{key}"));}else{target.set_host(Some(&format!("{bucket}.{}",target.host_str().unwrap_or("")))).map_err(|_|"invalid OSS endpoint".to_owned())?;target.set_path(&format!("/{key}"));}
                if permission=="ListObjects"{target.query_pairs_mut().append_pair("list-type","2").append_pair("max-keys","1").append_pair("prefix",&prefix);}
                else if permission=="AccessBucket"{target.set_query(Some("acl"));}
                let cred=credentials.load().await.map_err(|_|"OSS credentials unavailable".to_owned())?.ok_or_else(||"OSS credentials unavailable".to_owned())?;
                let mut headers=http::HeaderMap::new();tidb_oss_sdk::signer::sign("GET",&mut target,&mut headers,bucket,&key,&detected,&cred,None,chrono::Utc::now()).map_err(|_|"OSS signing failed".to_owned())?;
                let operation=async {
                    let response=client.get(target).headers(headers).send().await.map_err(|_|format!("permission {permission}: HTTP request failed"))?;
                    if response.status().is_success(){return Ok(());}
                    let status=response.status().as_u16();
                    if permission=="GetObject" {
                        #[derive(serde::Deserialize)]struct Error {#[serde(rename="Code")] code:String}
                        let body=response.bytes().await.map_err(|_|"invalid OSS error response".to_owned())?;
                        if quick_xml::de::from_reader::<_,Error>(body.as_ref()).is_ok_and(|e|e.code=="NoSuchKey"){return Ok(());}
                    }
                    Err(format!("permission {permission}: HTTP {status}"))
                };
                tokio::pin!(operation);
                loop{tokio::select!{result=&mut operation=>{result?;break;},_=tokio::time::sleep(Duration::from_millis(10))=>{if let Some(error)=ctx.err(){return Err(error.to_string());}}}}
            }Ok(())
        })?;
    }
    let transport = Transport {
        client: client.clone(),
        credentials: credentials.clone(),
        region: detected.clone(),
        bucket: bucket.into(),
        path_style,
        class: get("storage-class").into(),
    };
    let _guard = RUNTIME.enter();
    let mut builder = opendal::services::Oss::default()
        .allow_anonymous()
        .bucket(bucket)
        .endpoint(&data_endpoint)
        .http_client(HttpClient::with(transport));
    if path_style {
        builder = builder.addressing_style("path");
    }
    if !get("access-key").is_empty() {
        builder = builder
            .access_key_id(get("access-key"))
            .access_key_secret(get("secret-access-key"))
            .security_token(get("session-token"));
    }
    if !get("sse").is_empty() {
        builder = builder.server_side_encryption(get("sse"));
    }
    if !get("sse-kms-key-id").is_empty() {
        builder = builder.server_side_encryption_key_id(get("sse-kms-key-id"));
    }
    let operator = opendal::Operator::new(builder)
        .map_err(|e| super::cloud::native_error(e).to_string())?
        .finish();
    if get("access-key").is_empty() {
        RUNTIME
            .block_on(credentials.start_refresh())
            .map_err(|_| "failed to start OSS credential refresher".to_owned())?;
    }
    let lifecycle_credentials = credentials.clone();
    let owned_bucket = bucket.to_owned();
    let presigner = Arc::new(move |key: &str, expire: Duration| {
        let mut target =
            url::Url::parse(&endpoint).map_err(|_| "invalid OSS endpoint".to_owned())?;
        if path_style {
            target.set_path(&format!("/{owned_bucket}/{key}"));
        } else {
            target
                .set_host(Some(&format!(
                    "{owned_bucket}.{}",
                    target.host_str().unwrap_or("")
                )))
                .map_err(|_| "invalid OSS endpoint".to_owned())?;
            target.set_path(&format!("/{key}"));
        }
        RUNTIME.block_on(async {
            let cred = credentials
                .load()
                .await
                .map_err(|_| "OSS credentials unavailable".to_owned())?
                .ok_or_else(|| "OSS credentials unavailable".to_owned())?;
            tidb_oss_sdk::signer::sign(
                "GET",
                &mut target,
                &mut http::HeaderMap::new(),
                &owned_bucket,
                key,
                &detected,
                &cred,
                Some(expire),
                chrono::Utc::now(),
            )
            .map_err(|_| "OSS presigning failed".to_owned())?;
            Ok(target.to_string())
        })
    });
    Ok(Arc::new(ObjectStorage {
        credentials: Some(lifecycle_credentials),
        operator,
        presigner: Some(presigner),
        scheme: "s3",
        bucket: bucket.into(),
        prefix: format!("{}/", prefix.trim_matches('/'))
            .trim_start_matches('/')
            .into(),
    }))
}

/// Go `ossstore.canUseInternalEndpoint`.
fn can_use_internal_endpoint(ecs_region: &str, bucket_region: &str) -> bool {
    !ecs_region.is_empty() && ecs_region == bucket_region
}
#[cfg(test)]
mod tests {
    /// Go `ossstore.TestCanUseInternalEndpoint`.
    #[test]
    fn test_can_use_internal_endpoint() {
        assert!(!super::can_use_internal_endpoint("", "cn-hangzhou"));
        assert!(!super::can_use_internal_endpoint(
            "cn-beijing",
            "cn-hangzhou"
        ));
        assert!(super::can_use_internal_endpoint(
            "cn-hangzhou",
            "cn-hangzhou"
        ));
    }
}
