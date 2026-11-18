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

//! What Go's S3 SDK clients put on a request that `opendal` does not: the
//! canned ACL of `s3Client.PutObject` / `CreateMultipartUpload`
//! (`options.Acl`), and `s3store.NewKS3Storage`'s KS3 SDK V2 signature in
//! place of SigV4.
use base64::Engine;
use hmac::Mac;
use http::{HeaderMap, Request, Response};
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use std::sync::Arc;

pub(crate) struct Transport {
    pub client: reqwest12::Client,
    /// `None` sends the request anonymously (Go `NoCredentials`).
    pub credentials: Option<Arc<dyn reqsign::AwsCredentialLoad>>,
    pub region: String,
    pub bucket: String,
    pub path_style: bool,
    pub ks3: bool,
    pub acl: String,
}
impl HttpFetch for Transport {
    /// Go `client.PutObject` / KS3 SDK `v2.Sign` before sending a request.
    async fn fetch(
        &self,
        mut req: Request<opendal::Buffer>,
    ) -> opendal::Result<Response<HttpBody>> {
        let error = || {
            opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "object storage signing failed",
            )
        };
        if !self.acl.is_empty()
            && (req.method() == http::Method::PUT
                || req.method() == http::Method::POST
                    && req
                        .uri()
                        .query()
                        .is_some_and(|q| q.split('&').any(|p| p == "uploads" || p == "uploads=")))
        {
            req.headers_mut()
                .insert("x-amz-acl", self.acl.parse().map_err(|_| error())?);
        }
        let Some(credentials) = &self.credentials else {
            return HttpClient::with(self.client.clone()).fetch(req).await;
        };
        let cred = credentials
            .load_credential(self.client.clone())
            .await
            .map_err(|_| error())?
            .ok_or_else(error)?;
        req.headers_mut().remove(http::header::AUTHORIZATION);
        if self.ks3 {
            req.headers_mut().remove("x-amz-date");
            req.headers_mut().remove("x-amz-content-sha256");
            let url = url::Url::parse(&req.uri().to_string()).map_err(|_| error())?;
            let method = req.method().as_str().to_owned();
            sign_v2(
                &method,
                &url,
                req.headers_mut(),
                &self.bucket,
                self.path_style,
                &cred,
                chrono::Utc::now(),
            )
            .map_err(|_| error())?;
        } else {
            reqsign::AwsV4Signer::new("s3", &self.region)
                .sign(&mut req, &cred)
                .map_err(|_| error())?;
        }
        HttpClient::with(self.client.clone()).fetch(req).await
    }
}
/// Go KS3 SDK `v2.signer.build`: canonical headers/resource and HMAC-SHA1.
pub(crate) fn sign_v2(
    method: &str,
    url: &url::Url,
    headers: &mut HeaderMap,
    bucket: &str,
    path_style: bool,
    cred: &reqsign::AwsCredential,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), ()> {
    let date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    headers.insert("date", date.parse().map_err(|_| ())?);
    // Go KS3 SDK v2 does not transmit SessionToken, including when the
    // native preparatory AWS signer added it.
    headers.remove("x-amz-security-token");
    let mut canonical = std::collections::BTreeMap::new();
    for name in headers
        .keys()
        .filter(|name| name.as_str().starts_with("x-amz-"))
    {
        let values = headers
            .get_all(name)
            .iter()
            .map(|v| v.to_str().map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        canonical.insert(name.as_str(), values.join(","));
    }
    let mut resource = if path_style {
        url.path().to_owned()
    } else {
        format!("/{bucket}{}", url.path())
    };
    if resource == format!("/{bucket}") {
        resource.push('/');
    }
    let allowed = [
        "acl",
        "lifecycle",
        "location",
        "logging",
        "notification",
        "policy",
        "requestPayment",
        "torrent",
        "uploadId",
        "uploads",
        "versionId",
        "versioning",
        "versions",
        "website",
        "delete",
        "thumbnail",
        "cors",
        "pfop",
        "querypfop",
        "partNumber",
        "response-content-type",
        "response-content-language",
        "response-expires",
        "response-cache-control",
        "response-content-disposition",
        "response-content-encoding",
        "tagging",
        "fetch",
        "copy",
        "mirror",
        "restore",
    ];
    let mut query = std::collections::BTreeMap::<String, Vec<String>>::new();
    for (name, value) in url.query_pairs() {
        if allowed.contains(&name.as_ref()) {
            query
                .entry(name.into_owned())
                .or_default()
                .push(value.into_owned());
        }
    }
    if !query.is_empty() {
        resource.push('?');
        resource.push_str(
            &query
                .into_iter()
                .map(|(name, values)| {
                    let value = values.join(",");
                    if value.is_empty() {
                        name
                    } else {
                        format!("{name}={value}")
                    }
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    let get = |name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    };
    let mut text = format!(
        "{method}\n{}\n{}\n{date}\n",
        get("content-md5"),
        get("content-type")
    );
    for (name, value) in canonical {
        text.push_str(&format!("{name}:{value}\n"));
    }
    text.push_str(&resource);
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(cred.secret_access_key.as_bytes())
        .map_err(|_| ())?;
    mac.update(text.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    headers.insert(
        "authorization",
        format!("AWS {}:{signature}", cred.access_key_id)
            .parse()
            .map_err(|_| ())?,
    );
    headers
        .get_mut("authorization")
        .unwrap()
        .set_sensitive(true);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `zzprobe/o15_extstore.TestKS3V2Signature`.
    #[test]
    fn test_ks3_v2_signature() {
        let url = url::Url::parse("http://127.0.0.1:1234/bucket/base/tenant/file").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-acl", "private".parse().unwrap());
        let mut cred = reqsign::AwsCredential {
            access_key_id: "fixture".into(),
            secret_access_key: "fixture".into(),
            session_token: None,
            expires_in: None,
        };
        sign_v2(
            "PUT",
            &url,
            &mut headers,
            "bucket",
            true,
            &cred,
            chrono::DateTime::from_timestamp(1702743657, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(headers["date"], "Sat, 16 Dec 2023 16:20:57 GMT");
        assert_eq!(
            headers["authorization"],
            "AWS fixture:LUEDixzKXtZyrC5iF53ozMJNQJw="
        );
        // Go `TestKS3V2SignatureToken` captures the same signature with STS credentials.
        cred.session_token = Some("session_fixture".into());
        headers.insert("x-amz-security-token", "session_fixture".parse().unwrap());
        sign_v2(
            "PUT",
            &url,
            &mut headers,
            "bucket",
            true,
            &cred,
            chrono::DateTime::from_timestamp(1702743657, 0).unwrap(),
        )
        .unwrap();
        assert!(headers["authorization"].is_sensitive());
        assert!(!headers.contains_key("x-amz-security-token"));
        assert_eq!(
            headers["authorization"],
            "AWS fixture:LUEDixzKXtZyrC5iF53ozMJNQJw="
        );
    }
}
