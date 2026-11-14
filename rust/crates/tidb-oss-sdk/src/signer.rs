// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go OSS SDK `signer.SignerV4`, used by `ossstore.NewOSSStorage`.
use hmac::Mac;
use sha2::Digest;
/// Go `signer.escapePath`: URI encoding retains separators; query encoding does not.
fn escape(value: &str, separator: bool) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) || byte == b'/' && !separator {
            result.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(result, "%{byte:02X}").unwrap();
        }
    }
    result
}
/// Go `SignerV4.authHeader` / `authQuery` with no AdditionalHeaders.
pub fn sign(
    method: &str,
    url: &mut url::Url,
    headers: &mut http::HeaderMap,
    bucket: &str,
    key: &str,
    region: &str,
    cred: &reqsign::AliyunCredential,
    expires: Option<std::time::Duration>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), ()> {
    let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let scope = format!("{date}/{region}/oss/aliyun_v4_request");
    if let Some(expires) = expires {
        let mut query = url.query_pairs_mut();
        if let Some(token) = &cred.security_token {
            query.append_pair("x-oss-security-token", token);
        }
        query
            .append_pair("x-oss-signature-version", "OSS4-HMAC-SHA256")
            .append_pair("x-oss-date", &datetime)
            .append_pair("x-oss-expires", &expires.as_secs().to_string())
            .append_pair(
                "x-oss-credential",
                &format!("{}/{scope}", cred.access_key_id),
            );
    } else {
        headers.insert("x-oss-date", datetime.parse().map_err(|_| ())?);
        headers.insert(
            "date",
            now.format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .map_err(|_| ())?,
        );
        headers.insert(
            "x-oss-content-sha256",
            http::HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        if let Some(token) = &cred.security_token {
            let mut value: http::HeaderValue = token.parse().map_err(|_| ())?;
            value.set_sensitive(true);
            headers.insert("x-oss-security-token", value);
        }
    }
    let mut query = url
        .query_pairs()
        .map(|(name, value)| (escape(&name, true), escape(&value, true)))
        .collect::<Vec<_>>();
    query.sort();
    let query = query
        .into_iter()
        .map(|(name, value)| {
            if value.is_empty() {
                name
            } else {
                format!("{name}={value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    url.set_query((!query.is_empty()).then_some(&query));
    let mut names = headers
        .keys()
        .filter(|name| {
            name.as_str().starts_with("x-oss-")
                || matches!(name.as_str(), "content-type" | "content-md5")
        })
        .map(|name| name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    let mut canonical_headers = String::new();
    for name in names {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(
            &headers
                .get_all(name)
                .iter()
                .map(|v| v.to_str().map(str::trim))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ())?
                .join(","),
        );
        canonical_headers.push('\n');
    }
    let resource = escape(&format!("/{bucket}/{key}"), false);
    let payload = headers
        .get("x-oss-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");
    let canonical = format!("{method}\n{resource}\n{query}\n{canonical_headers}\n\n{payload}");
    let hash = hex::encode(sha2::Sha256::digest(canonical.as_bytes()));
    let text = format!("OSS4-HMAC-SHA256\n{datetime}\n{scope}\n{hash}");
    /// Go `SignerV4.calcSignature`: derive each HMAC signing-key stage.
    fn hmac(key: &[u8], value: &str) -> Vec<u8> {
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
        mac.update(value.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
    let key = hmac(
        format!("aliyun_v4{}", cred.access_key_secret).as_bytes(),
        &date,
    );
    let key = hmac(&key, region);
    let key = hmac(&key, "oss");
    let key = hmac(&key, "aliyun_v4_request");
    let signature = hex::encode(hmac(&key, &text));
    if expires.is_some() {
        url.query_pairs_mut()
            .append_pair("x-oss-signature", &signature);
    } else {
        headers.insert(
            "authorization",
            format!(
                "OSS4-HMAC-SHA256 Credential={}/{scope},Signature={signature}",
                cred.access_key_id
            )
            .parse()
            .map_err(|_| ())?,
        );
    }
    if let Some(value) = headers.get_mut("authorization") {
        value.set_sensitive(true);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `SignerV4.Sign`: deterministic credentials and request test fixture.
    fn fixture(token: bool) -> (url::Url, http::HeaderMap, reqsign::AliyunCredential) {
        let mut url = url::Url::parse("http://bucket.oss-cn-hangzhou.aliyuncs.com").unwrap();
        url.query_pairs_mut().extend_pairs([
            ("param1", "value1"),
            ("+param1", "value3"),
            ("|param1", "value4"),
            ("+param2", ""),
            ("|param2", ""),
            ("param2", ""),
        ]);
        let mut headers = http::HeaderMap::new();
        for (name, value) in [
            ("x-oss-head1", "value"),
            ("abc", "value"),
            ("zabc", "value"),
            ("xyz", "value"),
            ("content-type", "text/plain"),
        ] {
            headers.insert(name, value.parse().unwrap());
        }
        let cred = reqsign::AliyunCredential {
            access_key_id: "ak".into(),
            access_key_secret: "sk".into(),
            security_token: token.then(|| "token".into()),
            expires_in: None,
        };
        (url, headers, cred)
    }
    /// Go OSS SDK `TestV4AuthHeader`.
    #[test]
    fn test_v4_auth_header() {
        let (mut url, mut headers, cred) = fixture(false);
        sign(
            "PUT",
            &mut url,
            &mut headers,
            "bucket",
            "1234+-/123/1.txt",
            "cn-hangzhou",
            &cred,
            None,
            chrono::DateTime::from_timestamp(1702743657, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(
            headers["authorization"],
            "OSS4-HMAC-SHA256 Credential=ak/20231216/cn-hangzhou/oss/aliyun_v4_request,Signature=e21d18daa82167720f9b1047ae7e7f1ce7cb77a31e8203a7d5f4624fa0284afe"
        );
    }
    /// Go OSS SDK `TestV4AuthHeaderToken`.
    #[test]
    fn test_v4_auth_header_token() {
        let (mut url, mut headers, cred) = fixture(true);
        sign(
            "PUT",
            &mut url,
            &mut headers,
            "bucket",
            "1234+-/123/1.txt",
            "cn-hangzhou",
            &cred,
            None,
            chrono::DateTime::from_timestamp(1702784856, 0).unwrap(),
        )
        .unwrap();
        assert!(headers["authorization"].is_sensitive());
        assert!(headers["x-oss-security-token"].is_sensitive());
        assert_eq!(
            headers["authorization"],
            "OSS4-HMAC-SHA256 Credential=ak/20231217/cn-hangzhou/oss/aliyun_v4_request,Signature=b94a3f999cf85bcdc00d332fbd3734ba03e48382c36fa4d5af5df817395bd9ea"
        );
    }
    /// Go OSS SDK `TestV4AuthQuery` / `TestV4AuthQueryToken`.
    #[test]
    fn test_v4_auth_query() {
        for (token, time, expected) in [
            (
                false,
                1702781677,
                "a39966c61718be0d5b14e668088b3fa07601033f6518ac7b523100014269c0fe",
            ),
            (
                true,
                1702785388,
                "3817ac9d206cd6dfc90f1c09c00be45005602e55898f26f5ddb06d7892e1f8b5",
            ),
        ] {
            let (mut url, mut headers, cred) = fixture(token);
            headers.insert(
                "content-type",
                http::HeaderValue::from_static("application/octet-stream"),
            );
            sign(
                "PUT",
                &mut url,
                &mut headers,
                "bucket",
                "1234+-/123/1.txt",
                "cn-hangzhou",
                &cred,
                Some(std::time::Duration::from_secs(599)),
                chrono::DateTime::from_timestamp(time, 0).unwrap(),
            )
            .unwrap();
            let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
            assert_eq!(query["x-oss-signature"], expected);
            assert_eq!(query["x-oss-expires"], "599");
        }
    }
}
