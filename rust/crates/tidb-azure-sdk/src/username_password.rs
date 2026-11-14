// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `azidentity.UsernamePasswordCredential` and its MSAL public-client flow.
use super::{Token, SCOPE};
use azure_core::http::{ClientOptions, Context, Method, Pipeline, Request, Url};
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;

const TRUST13: &str = "http://docs.oasis-open.org/ws-sx/ws-trust/200512";
const TRUST2005: &str = "http://schemas.xmlsoap.org/ws/2005/02/trust";

/// Go `UsernamePasswordCredential`, with public-client cache serialization.
pub(super) struct UsernamePasswordCredential {
    tenant: String,
    client: String,
    username: String,
    password: String,
    authority: Url,
    pipeline: Pipeline,
    state: tokio::sync::Mutex<State>,
}
#[derive(Default)]
struct State {
    endpoint: Option<Url>,
    token: Option<Token>,
    refresh_token: String,
    discovered: bool,
}
/// Go `AuthenticationFailedError`: never include credentials or server bodies.
fn failure() -> String {
    "Azure username/password authentication failed".to_owned()
}
impl UsernamePasswordCredential {
    /// Go `NewUsernamePasswordCredential` with EnvironmentCredential options.
    pub(super) fn new(
        tenant: String,
        client: String,
        username: String,
        password: String,
    ) -> Result<Self, String> {
        let authority = std::env::var("AZURE_AUTHORITY_HOST")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "https://login.microsoftonline.com/".to_owned());
        Self::with_options(
            tenant,
            client,
            username,
            password,
            &authority,
            ClientOptions::default(),
        )
    }
    /// Go `newPublicClient`: validate the authority and construct its HTTP pipeline.
    fn with_options(
        tenant: String,
        client: String,
        username: String,
        password: String,
        authority: &str,
        options: ClientOptions,
    ) -> Result<Self, String> {
        if tenant.is_empty()
            || !tenant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
        {
            return Err(failure());
        }
        let authority = Url::parse(authority).map_err(|_| failure())?;
        if authority.scheme() != "https" || authority.host_str().is_none() {
            return Err(failure());
        }
        Ok(Self {
            tenant,
            client,
            username,
            password,
            authority,
            pipeline: Pipeline::new(None, None, options, Vec::new(), Vec::new(), None),
            state: tokio::sync::Mutex::new(State::default()),
        })
    }
    /// Go `comm.Client`: execute through the native Azure pipeline without exposing its errors.
    async fn request(&self, url: Url, body: Option<(&str, String)>) -> Result<Vec<u8>, String> {
        if url.scheme() != "https" {
            return Err(failure());
        }
        let mut request = Request::new(
            url,
            if body.is_some() {
                Method::Post
            } else {
                Method::Get
            },
        );
        request.insert_header("client-request-id", uuid::Uuid::new_v4().to_string());
        if let Some((content_type, body)) = body {
            request.insert_header("content-type", content_type.to_owned());
            request.set_body(body);
        }
        let response = self
            .pipeline
            .send(&Context::new(), &mut request, None)
            .await
            .map_err(|_| failure())?;
        if !response.status().is_success() {
            return Err(failure());
        }
        Ok(response.into_body().as_ref().to_vec())
    }
    /// Go `authority.Client.TenantDiscovery`: resolve the token endpoint once.
    async fn endpoint(&self) -> Result<Url, String> {
        let path = if self.tenant.eq_ignore_ascii_case("adfs") {
            format!("/{}/.well-known/openid-configuration", self.tenant)
        } else {
            format!("/{}/v2.0/.well-known/openid-configuration", self.tenant)
        };
        let discovery = if !trusted_host(self.authority.host_str().unwrap_or(""))
            && !self.tenant.eq_ignore_ascii_case("adfs")
        {
            let value = self.discover().await?;
            Url::parse(
                value["tenant_discovery_endpoint"]
                    .as_str()
                    .ok_or_else(failure)?,
            )
            .map_err(|_| failure())?
        } else {
            self.authority.join(&path).map_err(|_| failure())?
        };
        let value: Value =
            serde_json::from_slice(&self.request(discovery, None).await?).map_err(|_| failure())?;
        for key in ["authorization_endpoint", "token_endpoint", "issuer"] {
            if value[key].as_str().is_none_or(str::is_empty) {
                return Err(failure());
            }
        }
        let issuer = Url::parse(value["issuer"].as_str().unwrap()).map_err(|_| failure())?;
        if issuer.host_str() != self.authority.host_str()
            && !trusted_host(issuer.host_str().unwrap_or(""))
        {
            return Err(failure());
        }
        let endpoint =
            Url::parse(value["token_endpoint"].as_str().unwrap()).map_err(|_| failure())?;
        if endpoint.scheme() != "https" {
            return Err(failure());
        }
        Ok(endpoint)
    }
    /// Go `authority.Client.AADInstanceDiscovery`, including custom-authority validation.
    async fn discover(&self) -> Result<Value, String> {
        let host = self.authority.host_str().unwrap_or("");
        let host = if trusted_host(host) {
            host
        } else {
            "login.microsoftonline.com"
        };
        let mut endpoint = Url::parse(&format!("https://{host}/common/discovery/instance"))
            .map_err(|_| failure())?;
        endpoint
            .query_pairs_mut()
            .append_pair("api-version", "1.1")
            .append_pair(
                "authorization_endpoint",
                &format!(
                    "{}{}{}",
                    self.authority.as_str().trim_end_matches('/'),
                    format!("/{}/", self.tenant),
                    "oauth2/v2.0/authorize"
                ),
            );
        serde_json::from_slice(&self.request(endpoint, None).await?).map_err(|_| failure())
    }
    /// Go `publicClient.GetToken`: silent cache/refresh precedes password authentication.
    pub(super) async fn get_token(&self) -> Result<Token, String> {
        let mut state = self.state.lock().await;
        if let Some(token) = &state.token {
            if token.expires > Utc::now() + chrono::Duration::minutes(5) {
                return Ok(Token {
                    token: token.token.clone(),
                    expires: token.expires,
                });
            }
        }
        let endpoint = match &state.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let endpoint = self.endpoint().await?;
                state.endpoint = Some(endpoint.clone());
                endpoint
            }
        };
        if !state.refresh_token.is_empty() {
            let scopes = format!("{SCOPE} openid offline_access profile");
            let form = [
                ("grant_type", "refresh_token"),
                ("refresh_token", state.refresh_token.as_str()),
                ("client_id", self.client.as_str()),
                ("client_info", "1"),
                ("scope", scopes.as_str()),
            ];
            if let Ok(value) = self.token_request(endpoint.clone(), &form).await {
                return cache_token(&mut state, value);
            }
        }
        let mut form = vec![
            ("client_id", self.client.clone()),
            ("client_info", "1".into()),
            ("scope", format!("{SCOPE} openid offline_access profile")),
            ("username", self.username.clone()),
            ("password", self.password.clone()),
        ];
        let grant = if self.tenant.eq_ignore_ascii_case("adfs") {
            ("password".to_owned(), None)
        } else {
            let mut realm = self.authority.clone();
            realm.set_path("/common/UserRealm/");
            realm
                .path_segments_mut()
                .map_err(|_| failure())?
                .pop_if_empty()
                .push(&self.username);
            realm.query_pairs_mut().append_pair("api-version", "1.0");
            let value: Value =
                serde_json::from_slice(&self.request(realm, None).await?).map_err(|_| failure())?;
            for key in [
                "account_type",
                "domain_name",
                "cloud_instance_name",
                "cloud_audience_urn",
            ] {
                if value[key].as_str().is_none_or(str::is_empty) {
                    return Err(failure());
                }
            }
            match value["account_type"].as_str() {
                Some("Managed") => ("password".to_owned(), None),
                Some("Federated") => {
                    if value["federation_protocol"]
                        .as_str()
                        .is_none_or(str::is_empty)
                    {
                        return Err(failure());
                    }
                    let mex = Url::parse(
                        value["federation_metadata_url"]
                            .as_str()
                            .ok_or_else(failure)?,
                    )
                    .map_err(|_| failure())?;
                    let xml = self.request(mex, None).await?;
                    let endpoint = mex_endpoint(std::str::from_utf8(&xml).map_err(|_| failure())?)?;
                    let soap = soap_request(
                        &endpoint,
                        value["cloud_audience_urn"].as_str().unwrap(),
                        &self.username,
                        &self.password,
                    );
                    let response = self
                        .request(
                            Url::parse(&endpoint).map_err(|_| failure())?,
                            Some(("application/soap+xml; charset=utf-8", soap)),
                        )
                        .await?;
                    let (grant, assertion) =
                        saml_assertion(std::str::from_utf8(&response).map_err(|_| failure())?)?;
                    use base64::Engine;
                    (
                        grant.to_owned(),
                        Some(base64::engine::general_purpose::STANDARD.encode(assertion)),
                    )
                }
                _ => return Err(failure()),
            }
        };
        form.push(("grant_type", grant.0));
        if let Some(assertion) = grant.1 {
            form.push(("assertion", assertion));
        }
        let pairs: Vec<_> = form
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        let value = self.token_request(endpoint, &pairs).await?;
        if !state.discovered && !self.tenant.eq_ignore_ascii_case("adfs") {
            self.discover().await?;
            state.discovered = true;
        }
        cache_token(&mut state, value)
    }
    /// Go `accesstokens.Client.FromUsernamePassword` / `FromSamlGrant` / `FromRefreshToken`.
    async fn token_request(&self, endpoint: Url, form: &[(&str, &str)]) -> Result<Value, String> {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form.iter().copied())
            .finish();
        serde_json::from_slice(
            &self
                .request(endpoint, Some(("application/x-www-form-urlencoded", body)))
                .await?,
        )
        .map_err(|_| failure())
    }
}
/// Go MSAL `aadTrustedHostList`, used for issuer validation.
fn trusted_host(host: &str) -> bool {
    matches!(
        host,
        "login.windows.net"
            | "login.partner.microsoftonline.cn"
            | "login.chinacloudapi.cn"
            | "login.microsoftonline.de"
            | "login-us.microsoftonline.com"
            | "login.microsoftonline.us"
            | "login.microsoftonline.com"
            | "login.microsoft.com"
            | "sts.windows.net"
            | "login.usgovcloudapi.net"
    )
}
/// Go `publicClient.token` and the in-memory MSAL access/refresh token cache.
fn cache_token(state: &mut State, value: Value) -> Result<Token, String> {
    let token = value["access_token"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(failure)?
        .to_owned();
    let seconds = value["expires_in"]
        .as_i64()
        .or_else(|| value["expires_in"].as_str()?.parse().ok())
        .ok_or_else(failure)?;
    let expires = Utc::now()
        .checked_add_signed(chrono::Duration::try_seconds(seconds).ok_or_else(failure)?)
        .ok_or_else(failure)?;
    if let Some(refresh) = value["refresh_token"].as_str() {
        state.refresh_token = refresh.to_owned();
    }
    state.token = Some(Token {
        token: token.clone(),
        expires,
    });
    Ok(Token { token, expires })
}
/// Go `defs.NewFromDef`: follow username-token policies through SOAP bindings to ports.
fn mex_endpoint(xml: &str) -> Result<String, String> {
    let doc = roxmltree::Document::parse(xml).map_err(|_| failure())?;
    let mut policies = Vec::new();
    for policy in doc.descendants().filter(|n| n.has_tag_name("Policy")) {
        let id = policy
            .attributes()
            .find(|a| a.name() == "Id" || a.name() == "ID")
            .map(|a| a.value());
        if policy
            .descendants()
            .any(|n| n.has_tag_name("WssUsernameToken10"))
            && policy
                .descendants()
                .any(|n| n.has_tag_name("TransportBinding"))
        {
            if let Some(id) = id {
                policies.push(format!("#{id}"));
            }
        }
    }
    let mut bindings = HashMap::new();
    for binding in doc
        .descendants()
        .filter(|n| n.has_tag_name("binding") && n.attribute("name").is_some())
    {
        if !binding
            .descendants()
            .any(|n| n.attribute("transport") == Some("http://schemas.xmlsoap.org/soap/http"))
        {
            continue;
        }
        if !binding.descendants().any(|n| {
            n.has_tag_name("PolicyReference")
                && n.attribute("URI")
                    .is_some_and(|uri| policies.iter().any(|p| p == uri))
        }) {
            continue;
        }
        let action = binding
            .descendants()
            .find_map(|n| n.attribute("soapAction"))
            .ok_or_else(failure)?;
        let modern = if action == format!("{TRUST13}/RST/Issue") {
            true
        } else if action == format!("{TRUST2005}/RST/Issue") {
            false
        } else {
            return Err(failure());
        };
        bindings.insert(binding.attribute("name").unwrap(), modern);
    }
    for port in doc.descendants().filter(|n| n.has_tag_name("port")) {
        let binding = port
            .attribute("binding")
            .unwrap_or("")
            .rsplit(':')
            .next()
            .unwrap();
        if bindings.get(binding) != Some(&true) {
            continue;
        } // Go rejects WS-Trust 2005.
        let address = port
            .descendants()
            .find(|n| n.has_tag_name("Address"))
            .and_then(|n| n.text())
            .unwrap_or("")
            .trim();
        if address.is_empty() {
            return Err(failure());
        }
        return Ok(address.to_owned());
    }
    Err(failure())
}
/// Go `Endpoint.BuildTokenRequestMessageUsernamePassword` (WS-Trust 1.3).
fn soap_request(endpoint: &str, audience: &str, username: &str, password: &str) -> String {
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&#34;")
            .replace('\'', "&#39;")
    };
    let now = Utc::now();
    let created = now.format("%Y-%m-%dT%H:%M:%S%.3fZ");
    let expires = (now + chrono::Duration::minutes(10))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    format!(
        r#"<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"><s:Header><wsa:Action s:mustUnderstand="1">{TRUST13}/RST/Issue</wsa:Action><wsa:messageID>urn:uuid:{message_id}</wsa:messageID><wsa:ReplyTo><wsa:Address>http://www.w3.org/2005/08/addressing/anonymous</wsa:Address></wsa:ReplyTo><wsa:To s:mustUnderstand="1">{endpoint}</wsa:To><wsse:Security s:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><wsu:Timestamp wsu:Id="MSATimeStamp"><wsu:Created>{created}</wsu:Created><wsu:Expires>{expires}</wsu:Expires></wsu:Timestamp><wsse:UsernameToken wsu:Id="UnPwSecTok13-{token_id}"><wsse:Username>{username}</wsse:Username><wsse:Password>{password}</wsse:Password></wsse:UsernameToken></wsse:Security></s:Header><s:Body><wst:RequestSecurityToken xmlns:wst="{TRUST13}"><wsp:AppliesTo xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy"><wsa:EndpointReference><wsa:Address>{audience}</wsa:Address></wsa:EndpointReference></wsp:AppliesTo><wst:KeyType>{TRUST13}/Bearer</wst:KeyType><wst:RequestType>{TRUST13}/Issue</wst:RequestType></wst:RequestSecurityToken></s:Body></s:Envelope>"#,
        endpoint = escape(endpoint),
        audience = escape(audience),
        username = escape(username),
        password = escape(password),
        message_id = uuid::Uuid::new_v4(),
        token_id = uuid::Uuid::new_v4()
    )
}
/// Go `wstrust.Client.samlAssertion`: preserve the exact returned assertion XML.
fn saml_assertion(xml: &str) -> Result<(&'static str, &str), String> {
    let doc = roxmltree::Document::parse(xml).map_err(|_| failure())?;
    let token = doc
        .descendants()
        .find(|n| n.has_tag_name("RequestedSecurityToken"))
        .ok_or_else(failure)?;
    let assertion = token
        .children()
        .find(|n| n.tag_name().name() == "Assertion")
        .ok_or_else(failure)?;
    let grant = match assertion.tag_name().namespace() {
        Some("urn:oasis:names:tc:SAML:1.0:assertion") => {
            "urn:ietf:params:oauth:grant-type:saml1_1-bearer"
        }
        Some("urn:oasis:names:tc:SAML:2.0:assertion") => {
            "urn:ietf:params:oauth:grant-type:saml2-bearer"
        }
        _ => return Err(failure()),
    };
    let raw = &xml[token.range()];
    let start = raw.find('>').ok_or_else(failure)? + 1;
    let end = raw.rfind("</").ok_or_else(failure)?;
    Ok((grant, &raw[start..end]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::http::{
        headers::Headers, AsyncRawResponse, Body, HttpClient, StatusCode, Transport,
    };
    use std::sync::{Arc, Mutex};
    #[derive(Debug)]
    struct Sts {
        calls: Mutex<Vec<String>>,
        federated: bool,
        refresh: bool,
        tokens: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl HttpClient for Sts {
        /// Go `zzprobe/o15_azure_identity.transport.Do`: exact Go token request and XML capture.
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            let path = request.url().path();
            self.calls.lock().unwrap().push(path.to_owned());
            let body = if path.contains("openid-configuration") {
                r#"{"authorization_endpoint":"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize","token_endpoint":"https://login.microsoftonline.com/tenant/oauth2/v2.0/token","issuer":"https://login.microsoftonline.com/tenant/v2.0"}"#.to_owned()
            } else if path.contains("discovery/instance") {
                r#"{"tenant_discovery_endpoint":"https://login.microsoftonline.com/tenant/v2.0/.well-known/openid-configuration","metadata":[{"preferred_network":"login.microsoftonline.com","preferred_cache":"login.microsoftonline.com","aliases":["login.microsoftonline.com"]}]}"#.to_owned()
            } else if request.url().host_str() == Some("federation.example.invalid") {
                if request.method() == Method::Post {
                    let Body::Bytes(body) = request.body() else {
                        panic!("SOAP body")
                    };
                    let xml =
                        roxmltree::Document::parse(std::str::from_utf8(body).unwrap()).unwrap();
                    assert!(xml.descendants().any(|n| n.tag_name().name() == "Username"
                        && n.text() == Some("fixture@example.invalid")));
                    assert!(xml
                        .descendants()
                        .any(|n| n.tag_name().name() == "Password" && n.text() == Some("fixture")));
                    include_str!("testdata/saml.xml").to_owned()
                } else {
                    include_str!("testdata/mex.xml").to_owned()
                }
            } else if path.contains("UserRealm") {
                if self.federated {r#"{"account_type":"Federated","domain_name":"example.invalid","cloud_instance_name":"microsoftonline.com","cloud_audience_urn":"urn:federation:MicrosoftOnline","federation_protocol":"WSTrust","federation_metadata_url":"https://federation.example.invalid/mex"}"#}
                else {r#"{"account_type":"Managed","domain_name":"example.invalid","cloud_instance_name":"microsoftonline.com","cloud_audience_urn":"urn:federation:MicrosoftOnline"}"#}.to_owned()
            } else {
                assert!(path.ends_with("/token"));
                let Body::Bytes(body) = request.body() else {
                    panic!("token body")
                };
                let form: HashMap<_, _> = url::form_urlencoded::parse(body).collect();
                if self.refresh
                    && self
                        .tokens
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        > 0
                {
                    assert_eq!(form["grant_type"], "refresh_token");
                    assert_eq!(form["refresh_token"], "fixture-refresh");
                    assert_eq!(form["client_info"], "1");
                    for scope in [SCOPE, "openid", "offline_access", "profile"] {
                        assert!(form["scope"].split_whitespace().any(|value| value == scope));
                    }
                    return Ok(AsyncRawResponse::from_bytes(StatusCode::Ok,Headers::new(),azure_core::Bytes::from_static(br#"{"access_token":"fixture-token","expires_in":3600,"token_type":"Bearer"}"#)));
                }
                assert_eq!(form["client_id"], "client");
                assert_eq!(form["username"], "fixture@example.invalid");
                assert_eq!(form["password"], "fixture");
                assert_eq!(
                    form["grant_type"],
                    if self.federated {
                        "urn:ietf:params:oauth:grant-type:saml2-bearer"
                    } else {
                        "password"
                    }
                );
                for scope in [SCOPE, "openid", "offline_access", "profile"] {
                    assert!(form["scope"].split_whitespace().any(|value| value == scope));
                }
                if self.federated {
                    use base64::Engine;
                    assert_eq!(base64::engine::general_purpose::STANDARD.decode(form["assertion"].as_bytes()).unwrap(),b"\n<saml:Assertion xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" ID=\"fixture\">fixture</saml:Assertion>\n");
                }
                if self.refresh {r#"{"access_token":"fixture-token","expires_in":1,"refresh_token":"fixture-refresh","token_type":"Bearer"}"#} else {r#"{"access_token":"fixture-token","expires_in":3600,"token_type":"Bearer"}"#}.to_owned()
            };
            Ok(AsyncRawResponse::from_bytes(
                StatusCode::Ok,
                Headers::new(),
                azure_core::Bytes::from(body),
            ))
        }
    }
    /// Go `zzprobe/o15_azure_identity.TestEnvironmentCredentialUsernamePassword`.
    #[test]
    fn test_environment_credential_username_password() {
        test_credential(false, false);
    }
    /// Go `zzprobe/o15_azure_identity.TestEnvironmentCredentialFederated`.
    #[test]
    fn test_environment_credential_federated() {
        test_credential(true, false);
    }
    /// Go `zzprobe/o15_azure_identity.TestEnvironmentCredentialRefreshToken`.
    #[test]
    fn test_environment_credential_refresh_token() {
        test_credential(false, true);
    }
    /// Go `testCredential`: credential acquisition and cache reuse over the Go fixture.
    fn test_credential(federated: bool, refresh: bool) {
        let sts = Arc::new(Sts {
            calls: Mutex::new(Vec::new()),
            federated,
            refresh,
            tokens: Default::default(),
        });
        let credential = UsernamePasswordCredential::with_options(
            "tenant".into(),
            "client".into(),
            "fixture@example.invalid".into(),
            "fixture".into(),
            "https://login.microsoftonline.com/",
            ClientOptions {
                transport: Some(Transport::new(sts.clone())),
                ..Default::default()
            },
        )
        .unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert_eq!(credential.get_token().await.unwrap().token, "fixture-token");
            assert_eq!(credential.get_token().await.unwrap().token, "fixture-token");
        });
        let mut expected = vec![
            "/tenant/v2.0/.well-known/openid-configuration",
            "/common/UserRealm/fixture@example.invalid",
        ];
        if federated {
            expected.extend(["/mex", "/token"]);
        }
        expected.extend(["/tenant/oauth2/v2.0/token", "/common/discovery/instance"]);
        if refresh {
            expected.push("/tenant/oauth2/v2.0/token");
        }
        assert_eq!(*sts.calls.lock().unwrap(), expected);
    }
}
