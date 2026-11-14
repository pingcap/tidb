// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go credentials-go `RAMRoleARNCredentialsProvider` and Meter `CredentialCache.callAssumeRole`.
use base64::Engine;
use hmac::Mac;
use std::collections::BTreeMap;

pub(crate) struct Role {
    pub arn: String,
    pub external_id: String,
    pub session_name: String,
    pub endpoint: String,
    pub duration: u64,
    pub policy: String,
    pub meter: bool,
}
impl Role {
    /// Go `RAMRoleARNCredentialsProviderBuilder.Build`, as configured by NewOSSStorage.
    pub fn new(arn: &str, external_id: &str) -> Self {
        let region = std::env::var("ALIBABA_CLOUD_STS_REGION").unwrap_or_default();
        let vpc = std::env::var("ALIBABA_CLOUD_VPC_ENDPOINT_ENABLED")
            .is_ok_and(|v| v.eq_ignore_ascii_case("true"));
        let prefix = if vpc { "sts-vpc" } else { "sts" };
        let endpoint = if region.is_empty() {
            "https://sts.aliyuncs.com".into()
        } else {
            format!("https://{prefix}.{region}.aliyuncs.com")
        };
        Self {
            arn: arn.into(),
            external_id: external_id.into(),
            session_name: std::env::var("ALIBABA_CLOUD_ROLE_SESSION_NAME")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| {
                    format!("credentials-go-{}", chrono::Utc::now().timestamp_micros())
                }),
            endpoint,
            duration: 3600,
            policy: String::new(),
            meter: false,
        }
    }
    /// Go Meter `CredentialCache.callAssumeRole`: one-hour sessions at the configured STS region.
    pub fn meter(arn: &str, region: &str) -> Self {
        let mut role = Self::new(arn, "");
        role.endpoint = format!("https://{}", meter_endpoint(region));
        role.meter = true;
        role.session_name.clear();
        role
    }
    /// Go `RAMRoleARNCredentialsProvider.getCredentials`: signed RPC query and form.
    fn request(
        &self,
        client: &reqwest::Client,
        base: &reqsign_aliyun_oss::Credential,
    ) -> Result<reqwest::Request, ()> {
        self.request_at(
            client,
            base,
            chrono::Utc::now(),
            &uuid::Uuid::new_v4().to_string(),
        )
    }
    /// Go SDK time/nonce inputs to the signed AssumeRole request.
    fn request_at(
        &self,
        client: &reqwest::Client,
        base: &reqsign_aliyun_oss::Credential,
        now: chrono::DateTime<chrono::Utc>,
        nonce: &str,
    ) -> Result<reqwest::Request, ()> {
        let mut query = BTreeMap::from([
            ("Version", "2015-04-01".to_owned()),
            ("Action", "AssumeRole".into()),
            (
                "Format",
                if self.meter {
                    "json".into()
                } else {
                    "JSON".into()
                },
            ),
            ("Timestamp", now.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            ("SignatureMethod", "HMAC-SHA1".into()),
            ("SignatureVersion", "1.0".into()),
            ("SignatureNonce", nonce.to_owned()),
            ("AccessKeyId", base.access_key_id.clone()),
        ]);
        if let Some(token) = &base.security_token {
            if !token.is_empty() {
                query.insert("SecurityToken", token.clone());
            }
        }
        let mut form = BTreeMap::from([
            ("RoleArn", self.arn.clone()),
            (
                "RoleSessionName",
                if self.meter && self.session_name.is_empty() {
                    format!("oss-sdk-session-{}", now.timestamp())
                } else {
                    self.session_name.clone()
                },
            ),
            ("DurationSeconds", self.duration.to_string()),
        ]);
        if !self.external_id.is_empty() {
            form.insert("ExternalId", self.external_id.clone());
        }
        if !self.policy.is_empty() {
            form.insert("Policy", self.policy.clone());
        }
        let mut params = query.clone();
        params.extend(form.clone());
        let canonical = params
            .into_iter()
            .map(|(key, value)| format!("{}={}", escape(key), escape(&value)))
            .collect::<Vec<_>>()
            .join("&");
        let text = format!("POST&%2F&{}", escape(&canonical));
        let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(
            format!("{}&", base.access_key_secret).as_bytes(),
        )
        .map_err(|_| ())?;
        mac.update(text.as_bytes());
        query.insert(
            "Signature",
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()),
        );
        let mut url = url::Url::parse(&self.endpoint).map_err(|_| ())?;
        url.query_pairs_mut().extend_pairs(&query);
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&form)
            .finish();
        client
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept-Encoding", "identity")
            .body(body)
            .build()
            .map_err(|_| ())
    }
    /// Go `RAMRoleARNCredentialsProvider.getCredentials` response validation.
    pub async fn load(
        &self,
        base: &reqsign_aliyun_oss::Credential,
    ) -> Result<reqsign_aliyun_oss::Credential, ()> {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| ())?;
        let response = client
            .execute(self.request(&client, base)?)
            .await
            .map_err(|_| ())?;
        if !response.status().is_success() {
            return Err(());
        }
        let bytes = response.bytes().await.map_err(|_| ())?;
        let data: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| ())?;
        let credentials = &data["Credentials"];
        let field = |name: &str| credentials[name].as_str().map(str::to_owned).ok_or(());
        Ok(reqsign_aliyun_oss::Credential {
            access_key_id: field("AccessKeyId")?,
            access_key_secret: field("AccessKeySecret")?,
            security_token: Some(field("SecurityToken")?),
            expires_in: Some(field("Expiration")?.parse().map_err(|_| ())?),
        })
    }
}
/// Go credentials SDK `utils.GetURLFormedMap` / RFC3986 query escaping.
fn escape(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            output.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(output, "%{byte:02X}").unwrap();
        }
    }
    output
}
/// Go STS SDK `Client.Init` endpoint map and regional endpoint rule.
fn meter_endpoint(region: &str) -> String {
    match region {
        "cn-north-2-gov-1" | "cn-shenzhen-finance-1" => format!("sts-vpc.{region}.aliyuncs.com"),
        "ap-northeast-2-pop"
        | "cn-beijing-finance-1"
        | "cn-beijing-finance-pop"
        | "cn-beijing-gov-1"
        | "cn-beijing-nu16-b01"
        | "cn-edge-1"
        | "cn-fujian"
        | "cn-haidian-cm12-c01"
        | "cn-hangzhou-bj-b01"
        | "cn-hangzhou-finance"
        | "cn-hangzhou-internal-prod-1"
        | "cn-hangzhou-internal-test-1"
        | "cn-hangzhou-internal-test-2"
        | "cn-hangzhou-internal-test-3"
        | "cn-hangzhou-test-306"
        | "cn-hongkong-finance-pop"
        | "cn-huhehaote-nebula-1"
        | "cn-qingdao-nebula"
        | "cn-shanghai-et15-b01"
        | "cn-shanghai-et2-b01"
        | "cn-shanghai-inner"
        | "cn-shanghai-internal-test-1"
        | "cn-shenzhen-inner"
        | "cn-shenzhen-st4-d01"
        | "cn-shenzhen-su18-b01"
        | "cn-wuhan"
        | "cn-yushanfang"
        | "cn-zhangbei"
        | "cn-zhangbei-na61-b01"
        | "cn-zhangjiakou-na62-a01"
        | "cn-zhengzhou-nebula-1"
        | "eu-west-1-oxs"
        | "rus-west-1-pop" => "sts.aliyuncs.com".into(),
        _ => format!("sts.{region}.aliyuncs.com"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `zzprobe/o15_meter_sdk.TestMeterSDKAssumeRole`: exact captured SDK signature and endpoints.
    #[test]
    fn test_meter_sdk_assume_role() {
        let mut role = Role::meter("roleArn", "cn-hangzhou");
        role.session_name = "oss-sdk-session-1702743657".into();
        let base = reqsign_aliyun_oss::Credential {
            access_key_id: "fixture".into(),
            access_key_secret: "fixture".into(),
            security_token: Some("token_fixture".into()),
            expires_in: None,
        };
        let request = role
            .request_at(
                &reqwest::Client::new(),
                &base,
                "2026-11-14T09:12:03Z".parse().unwrap(),
                "539245f8c30f8f7df08d9f262a2faa0e",
            )
            .unwrap();
        let query: BTreeMap<_, _> = request.url().query_pairs().collect();
        assert_eq!(query["Signature"], "2ntWiH1hzbiG01r8a2RxImWJBh8=");
        for (region, expected) in [
            ("cn-hangzhou", "sts.cn-hangzhou.aliyuncs.com"),
            ("cn-north-2-gov-1", "sts-vpc.cn-north-2-gov-1.aliyuncs.com"),
            ("eu-west-1-oxs", "sts.aliyuncs.com"),
        ] {
            assert_eq!(meter_endpoint(region), expected);
        }
    }
    /// Go credentials-go `TestRAMRoleARNCredentialsProvider_getCredentialsWithRequestCheck`.
    #[test]
    fn test_ram_role_arn_credentials_provider_get_credentials_with_request_check() {
        let mut role = Role::new("roleArn", "externalId");
        role.endpoint = "https://sts.cn-beijing.aliyuncs.com".into();
        role.session_name = "rsn".into();
        role.duration = 1000;
        role.policy = "policy".into();
        let base = reqsign_aliyun_oss::Credential {
            access_key_id: "akid".into(),
            access_key_secret: "aksecret".into(),
            security_token: Some("ststoken".into()),
            expires_in: None,
        };
        let request = role.request(&reqwest::Client::new(), &base).unwrap();
        assert_eq!(
            request.url().host_str(),
            Some("sts.cn-beijing.aliyuncs.com")
        );
        let query: BTreeMap<_, _> = request.url().query_pairs().collect();
        assert_eq!(query["SecurityToken"], "ststoken");
        let form: BTreeMap<_, _> =
            url::form_urlencoded::parse(request.body().unwrap().as_bytes().unwrap()).collect();
        assert_eq!(form["Policy"], "policy");
        assert_eq!(form["RoleArn"], "roleArn");
        assert_eq!(form["RoleSessionName"], "rsn");
        assert_eq!(form["DurationSeconds"], "1000");
    }
}
