// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `azidentity.DefaultAzureCredential` as consumed by TiDB object storage.
mod username_password;
use azure_core::credentials::TokenCredential;
use chrono::{TimeZone, Utc};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
const SCOPE: &str = "https://storage.azure.com/.default";
const RESOURCE: &str = "https://storage.azure.com/";
/// Go `azcore.AccessToken`, with no Debug implementation exposing the token.
pub struct Token {
    pub token: String,
    pub expires: chrono::DateTime<Utc>,
}
#[derive(Clone, Copy)]
enum Developer {
    Cli,
    DeveloperCli,
    PowerShell,
}
enum Source {
    UsernamePassword(username_password::UsernamePasswordCredential),
    Native(Arc<dyn TokenCredential>, bool),
    Managed {
        credential: Arc<dyn TokenCredential>,
        probe: std::sync::atomic::AtomicBool,
        imds: bool,
        user_assigned: bool,
    },
    Developer(Developer),
    Unavailable,
}
/// Go `DefaultAzureCredential` / `ChainedTokenCredential`: serialize selection,
/// then retain the first successful credential even when later calls fail.
pub struct DefaultAzureCredential {
    sources: Vec<Source>,
    selected: AtomicUsize,
    selecting: tokio::sync::Mutex<()>,
}
/// Go `os.Getenv`.
fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}
impl DefaultAzureCredential {
    /// Go `NewDefaultAzureCredential(nil)`.
    pub fn new() -> Result<Self, String> {
        let names = [
            "EnvironmentCredential",
            "WorkloadIdentityCredential",
            "ManagedIdentityCredential",
            "AzureCLICredential",
            "AzureDeveloperCLICredential",
            "AzurePowerShellCredential",
        ];
        let selected = match std::env::var("AZURE_TOKEN_CREDENTIALS") {
            Err(_) => (0..6).collect::<Vec<_>>(),
            Ok(value) if value == "dev" => vec![3, 4, 5],
            Ok(value) if value == "prod" => vec![0, 1, 2],
            Ok(value) => vec![names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(&value))
                .ok_or("invalid AZURE_TOKEN_CREDENTIALS value")?],
        };
        let mut sources = Vec::new();
        for index in selected.iter().copied() {
            let source = match index {
                0 => environment(),
                1 => azure_identity::WorkloadIdentityCredential::new(None)
                    .map(|c| Source::Native(c, true))
                    .unwrap_or(Source::Unavailable),
                2 => {
                    let id = env("AZURE_CLIENT_ID");
                    let user_assigned = !id.is_empty();
                    let options = azure_identity::ManagedIdentityCredentialOptions {
                        user_assigned_id: (!id.is_empty())
                            .then(|| azure_identity::UserAssignedId::ClientId(id)),
                        ..Default::default()
                    };
                    let imds =
                        env("IDENTITY_ENDPOINT").is_empty() && env("MSI_ENDPOINT").is_empty();
                    let probe = selected.len() > 1 && imds;
                    azure_identity::ManagedIdentityCredential::new(Some(options))
                        .map(|credential| Source::Managed {
                            credential,
                            probe: std::sync::atomic::AtomicBool::new(probe),
                            imds,
                            user_assigned,
                        })
                        .unwrap_or(Source::Unavailable)
                }
                3 => Source::Developer(Developer::Cli),
                4 => Source::Developer(Developer::DeveloperCli),
                5 => Source::Developer(Developer::PowerShell),
                _ => unreachable!(),
            };
            sources.push(source);
        }
        Ok(Self {
            sources,
            selected: AtomicUsize::new(usize::MAX),
            selecting: tokio::sync::Mutex::new(()),
        })
    }
    /// Go `DefaultAzureCredential.GetToken` for the storage audience.
    pub async fn get_token(&self) -> Result<Token, String> {
        let selected = self.selected.load(Ordering::Acquire);
        if selected != usize::MAX {
            return self.sources[selected]
                .get_token()
                .await
                .map_err(|_| "Azure credential authentication failed".into());
        }
        let _guard = self.selecting.lock().await;
        let selected = self.selected.load(Ordering::Acquire);
        if selected != usize::MAX {
            return self.sources[selected]
                .get_token()
                .await
                .map_err(|_| "Azure credential authentication failed".into());
        }
        for (index, source) in self.sources.iter().enumerate() {
            match source.get_token().await {
                Ok(token) => {
                    self.selected.store(index, Ordering::Release);
                    return Ok(token);
                }
                Err(true) => return Err("Azure credential authentication failed".into()),
                Err(false) => {}
            }
        }
        Err("Azure credentials unavailable".into())
    }
}
/// Go `NewEnvironmentCredential`: credential selection and eager configuration validation.
fn environment() -> Source {
    let tenant = env("AZURE_TENANT_ID");
    let client = env("AZURE_CLIENT_ID");
    if tenant.is_empty() || client.is_empty() {
        return Source::Unavailable;
    }
    let secret = env("AZURE_CLIENT_SECRET");
    if !secret.is_empty() {
        return azure_identity::ClientSecretCredential::new(&tenant, client, secret.into(), None)
            .map(|c| Source::Native(c, true))
            .unwrap_or(Source::Unavailable);
    }
    let certificate = env("AZURE_CLIENT_CERTIFICATE_PATH");
    if !certificate.is_empty() {
        let result = (|| {
            let mut data = std::fs::read(certificate).map_err(|_| ())?;
            let password = env("AZURE_CLIENT_CERTIFICATE_PASSWORD");
            // Go ParseCertificates accepts PEM and PKCS12; native Azure accepts PKCS12.
            if data.starts_with(b"-----BEGIN") {
                let key = openssl::pkey::PKey::private_key_from_pem(&data).map_err(|_| ())?;
                let mut certs = openssl::x509::X509::stack_from_pem(&data).map_err(|_| ())?;
                if certs.is_empty() {
                    return Err(());
                }
                let first = certs.remove(0);
                let mut chain = openssl::stack::Stack::new().map_err(|_| ())?;
                for cert in certs {
                    chain.push(cert).map_err(|_| ())?;
                }
                data = openssl::pkcs12::Pkcs12::builder()
                    .pkey(&key)
                    .cert(&first)
                    .ca(chain)
                    .build2(&password)
                    .map_err(|_| ())?
                    .to_der()
                    .map_err(|_| ())?;
            }
            let options = azure_identity::ClientCertificateCredentialOptions {
                password: (!password.is_empty()).then(|| password.into()),
                ..Default::default()
            };
            azure_identity::ClientCertificateCredential::new(
                tenant,
                client,
                data.as_slice().into(),
                Some(options),
            )
            .map_err(|_| ())
        })();
        return result
            .map(|c| Source::Native(c, true))
            .unwrap_or(Source::Unavailable);
    }
    let username = env("AZURE_USERNAME");
    let password = env("AZURE_PASSWORD");
    if !username.is_empty() && !password.is_empty() {
        return username_password::UsernamePasswordCredential::new(
            tenant, client, username, password,
        )
        .map(Source::UsernamePassword)
        .unwrap_or(Source::Unavailable);
    }
    Source::Unavailable
}
impl Source {
    /// Go `ChainedTokenCredential.GetToken` source result classification.
    async fn get_token(&self) -> Result<Token, bool> {
        match self {
            Self::Unavailable => Err(false),
            Self::UsernamePassword(credential) => credential.get_token().await.map_err(|_| true),
            Self::Developer(tool) => developer_token(*tool).await.map_err(|_| false),
            Self::Native(credential, terminal) => {
                native_token(credential).await.map_err(|_| *terminal)
            }
            Self::Managed {
                credential,
                probe,
                imds,
                user_assigned,
            } => {
                if probe.load(Ordering::Acquire) {
                    reqwest12::Client::new()
                        .get("http://169.254.169.254/metadata/identity/oauth2/token")
                        .timeout(std::time::Duration::from_secs(1))
                        .send()
                        .await
                        .map_err(|_| false)?;
                    probe.store(false, Ordering::Release);
                }
                native_token(credential)
                    .await
                    .map_err(|error| !managed_unavailable(&error, *imds, *user_assigned))
            }
        }
    }
}
/// Go native `TokenCredential.GetToken` converted without formatting secrets.
async fn native_token(credential: &Arc<dyn TokenCredential>) -> azure_core::Result<Token> {
    let token = credential.get_token(&[SCOPE], None).await?;
    let expires = Utc
        .timestamp_opt(token.expires_on.unix_timestamp(), 0)
        .single()
        .ok_or_else(|| {
            azure_core::Error::with_message(
                azure_core::error::ErrorKind::DataConversion,
                "invalid token expiration",
            )
        })?;
    Ok(Token {
        token: token.token.secret().to_owned(),
        expires,
    })
}
/// Go `managedIdentityClient.GetToken`: only unavailable IMDS identities permit fallback.
fn managed_unavailable(error: &azure_core::Error, imds: bool, user_assigned: bool) -> bool {
    if !imds {
        return false;
    }
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(cause) = current {
        if let Some(error) = cause.downcast_ref::<azure_core::Error>() {
            match error.kind() {
                azure_core::error::ErrorKind::DataConversion => return true,
                azure_core::error::ErrorKind::HttpResponse { status, .. } => {
                    return (*status == azure_core::http::StatusCode::BadRequest && !user_assigned)
                        || (*status == azure_core::http::StatusCode::Forbidden
                            && error.to_string().contains("unreachable"));
                }
                _ => {}
            }
        }
        current = cause.source();
    }
    false
}
/// Go AzureCLI/AzureDeveloperCLI/AzurePowerShell credential GetToken and parsing.
async fn developer_token(tool: Developer) -> Result<Token, ()> {
    let mut command = match tool {
        Developer::Cli => {
            let mut c = tokio::process::Command::new("az");
            c.args([
                "account",
                "get-access-token",
                "-o",
                "json",
                "--resource",
                RESOURCE,
            ]);
            c
        }
        Developer::DeveloperCli => {
            let mut c = tokio::process::Command::new("azd");
            c.args([
                "auth",
                "token",
                "-o",
                "json",
                "--no-prompt",
                "--scope",
                SCOPE,
            ]);
            c
        }
        Developer::PowerShell => {
            use base64::Engine;
            let bytes: Vec<_> = include_str!("powershell.ps1")
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect();
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            let mut c = tokio::process::Command::new("pwsh");
            c.args([
                "-NoProfile",
                "-NonInteractive",
                "-OutputFormat",
                "Text",
                "-EncodedCommand",
                &encoded,
            ]);
            c
        }
    };
    command.kill_on_drop(true);
    // Go shellExec applies a ten-second deadline when no earlier deadline exists.
    let output = tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    if !output.status.success() {
        return Err(());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|_| ())?;
    let (token, expires) = match tool {
        Developer::Cli => {
            let seconds = value["expires_on"].as_i64().unwrap_or(0);
            let expires = if seconds == 0 {
                let value = value["expiresOn"].as_str().ok_or(())?;
                let naive = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|_| ())?;
                chrono::Local
                    .from_local_datetime(&naive)
                    .earliest()
                    .ok_or(())?
                    .with_timezone(&Utc)
            } else {
                Utc.timestamp_opt(seconds, 0).single().ok_or(())?
            };
            (value["accessToken"].as_str().ok_or(())?, expires)
        }
        Developer::DeveloperCli => (
            value["token"].as_str().ok_or(())?,
            chrono::DateTime::parse_from_rfc3339(value["expiresOn"].as_str().ok_or(())?)
                .map_err(|_| ())?
                .with_timezone(&Utc),
        ),
        Developer::PowerShell => (
            value["Token"].as_str().ok_or(())?,
            Utc.timestamp_opt(value["ExpiresOn"].as_i64().ok_or(())?, 0)
                .single()
                .ok_or(())?,
        ),
    };
    Ok(Token {
        token: token.to_owned(),
        expires,
    })
}

/// Go Azure shared-key/SAS or identity credential used by a storage client.
pub struct Credentials {
    static_credential: Option<reqsign::AzureStorageCredential>,
    identity: Option<DefaultAzureCredential>,
    direct: Option<Arc<dyn TokenCredential>>,
    cached: std::sync::Mutex<Cache>,
    refreshing: tokio::sync::Mutex<()>,
}
#[derive(Default)]
struct Cache {
    credential: Option<reqsign::AzureStorageCredential>,
    last_attempt: Option<chrono::DateTime<Utc>>,
}
impl Credentials {
    /// Go Azure credential selection performed by the caller's client builder.
    pub fn new(
        config: reqsign::AzureStorageConfig,
        service_principal: bool,
    ) -> Result<Self, String> {
        let mut result = Self {
            static_credential: None,
            identity: None,
            direct: None,
            cached: std::sync::Mutex::new(Cache::default()),
            refreshing: tokio::sync::Mutex::new(()),
        };
        if let Some(token) = config.sas_token {
            result.static_credential = Some(
                reqsign::AzureStorageCredential::SharedAccessSignature(token),
            );
        } else if let (Some(name), Some(key)) = (config.account_name, config.account_key) {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(&key)
                .map_err(|_| "invalid Azure shared key".to_owned())?;
            result.static_credential = Some(reqsign::AzureStorageCredential::SharedKey(name, key));
        } else if service_principal {
            result.direct = Some(
                azure_identity::ClientSecretCredential::new(
                    config.tenant_id.as_deref().unwrap_or(""),
                    config.client_id.unwrap_or_default(),
                    config.client_secret.unwrap_or_default().into(),
                    None,
                )
                .map_err(|_| "cannot create Azure client-secret credential".to_owned())?,
            );
        } else {
            result.identity = Some(DefaultAzureCredential::new()?);
        }
        Ok(result)
    }
    /// Go `BearerTokenPolicy.checkHTTPSForAuth` distinguishes bearer credentials.
    pub fn is_bearer(&self) -> bool {
        self.static_credential.is_none()
    }
    /// Go `BearerTokenPolicy.handleChallenge` expires a token after HTTP 401.
    pub fn expire(&self) {
        self.cached.lock().unwrap().credential = None;
    }
    /// Go `temporal.Resource.Get` and `BearerTokenPolicy.shouldRefresh`.
    pub async fn load(&self) -> Result<reqsign::AzureStorageCredential, String> {
        if let Some(credential) = &self.static_credential {
            return Ok(credential.clone());
        }
        let now = Utc::now();
        let valid = {
            let cache = self.cached.lock().unwrap();
            match &cache.credential {
                Some(reqsign::AzureStorageCredential::BearerToken(_, expires))
                    if *expires >= now =>
                {
                    if *expires >= now + chrono::Duration::minutes(5)
                        || cache
                            .last_attempt
                            .is_some_and(|last| last + chrono::Duration::seconds(30) > now)
                    {
                        return Ok(cache.credential.as_ref().unwrap().clone());
                    }
                    cache.credential.clone()
                }
                _ => None,
            }
        };
        let _guard = if let Some(old) = valid {
            match self.refreshing.try_lock() {
                Ok(guard) => guard,
                Err(_) => return Ok(old),
            }
        } else {
            self.refreshing.lock().await
        };
        let old = {
            let mut cache = self.cached.lock().unwrap();
            // Another caller may have refreshed while we awaited the lock.
            if let Some(reqsign::AzureStorageCredential::BearerToken(_, expires)) =
                &cache.credential
            {
                if *expires >= now
                    && (*expires >= now + chrono::Duration::minutes(5)
                        || cache
                            .last_attempt
                            .is_some_and(|last| last + chrono::Duration::seconds(30) > now))
                {
                    return Ok(cache.credential.as_ref().unwrap().clone());
                }
            }
            cache.last_attempt = Some(now);
            cache.credential.clone()
        };
        let token = if let Some(direct) = &self.direct {
            native_token(direct)
                .await
                .map_err(|_| "Azure authentication failed".to_owned())
        } else {
            self.identity.as_ref().unwrap().get_token().await
        };
        match token {
            Ok(token) => {
                let credential =
                    reqsign::AzureStorageCredential::BearerToken(token.token, token.expires);
                self.cached.lock().unwrap().credential = Some(credential.clone());
                Ok(credential)
            }
            Err(error) => {
                // Go temporal.Resource.Get ignores eager refresh errors while
                // the previous token remains valid, but never serves it expired.
                if let Some(reqsign::AzureStorageCredential::BearerToken(_, expires)) = &old {
                    if *expires >= now {
                        return Ok(old.unwrap());
                    }
                }
                Err(error)
            }
        }
    }
}
/// Go Azure storage pipeline authorization at the final HTTP request boundary.
pub struct Transport {
    pub client: reqwest12::Client,
    pub credentials: Arc<Credentials>,
}
impl opendal::raw::HttpFetch for Transport {
    /// Go `sharedKeyPolicy.Do` / `BearerTokenPolicy.Do`: authenticate each request.
    async fn fetch(
        &self,
        mut request: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::raw::HttpBody>> {
        let error = || {
            opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Azure request authentication failed",
            )
        };
        if self.credentials.is_bearer() && request.uri().scheme_str() != Some("https") {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConfigInvalid,
                "Azure bearer credentials require HTTPS",
            ));
        }
        let credential = self.credentials.load().await.map_err(|_| error())?;
        request.headers_mut().remove(http::header::AUTHORIZATION);
        if !matches!(
            credential,
            reqsign::AzureStorageCredential::SharedAccessSignature(..)
        ) {
            reqsign::AzureStorageSigner::new()
                .sign(&mut request, &credential)
                .map_err(|_| error())?;
        }
        let response = opendal::raw::HttpClient::with(self.client.clone())
            .fetch(request)
            .await?;
        if response.status() == http::StatusCode::UNAUTHORIZED {
            self.credentials.expire();
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `azidentity.TestEnvironmentCredential`: username/password selection
    /// and missing required environment variables.
    #[test]
    fn test_environment_credential() {
        let required = [
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_USERNAME",
            "AZURE_PASSWORD",
        ];
        if let Ok(missing) = std::env::var("O15_ENV_CREDENTIAL_CHILD") {
            assert_eq!(
                matches!(environment(), Source::Unavailable),
                !missing.is_empty()
            );
            return;
        }
        for missing in std::iter::once("").chain(required) {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tests::test_environment_credential",
                    "--nocapture",
                ])
                .env("O15_ENV_CREDENTIAL_CHILD", missing)
                .env_remove("AZURE_CLIENT_SECRET")
                .env_remove("AZURE_CLIENT_CERTIFICATE_PATH")
                .env_remove("AZURE_AUTHORITY_HOST");
            for name in required {
                command.env(name, "fixture");
            }
            if !missing.is_empty() {
                command.env_remove(missing);
            }
            assert!(
                command.status().unwrap().success(),
                "missing variable: {missing}"
            );
        }
    }

    /// Go `zzprobe/o15_azure_identity.TestDefaultAzureCredential`.
    #[test]
    fn test_default_azure_credential() {
        use std::os::unix::fs::PermissionsExt;
        if std::env::var_os("O15_AZURE_CHILD").is_none() {
            for selection in [
                "AzureCLICredential",
                "azureclicredential",
                "AzureDeveloperCLICredential",
                "AzurePowerShellCredential",
                "dev",
                "",
                "DEV",
                "unknown",
            ] {
                let root = tempfile::tempdir().unwrap();
                for (name, body) in [
                    (
                        "az",
                        r#"printf '%s' '{"accessToken":"cli_fixture","expires_on":4070908800}'"#,
                    ),
                    (
                        "azd",
                        r#"printf '%s' '{"token":"azd_fixture","expiresOn":"2099-01-01T00:00:00Z"}'"#,
                    ),
                    (
                        "pwsh",
                        r#"printf '%s' '{"Token":"pwsh_fixture","ExpiresOn":4070908800}'"#,
                    ),
                ] {
                    let path = root.path().join(name);
                    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
                }
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "tests::test_default_azure_credential",
                        "--nocapture",
                    ])
                    .env("O15_AZURE_CHILD", root.path())
                    .env("PATH", format!("{}:/bin:/usr/bin", root.path().display()))
                    .env("AZURE_TOKEN_CREDENTIALS", selection)
                    .status()
                    .unwrap();
                assert!(status.success(), "{selection}");
            }
            return;
        }
        let selection = env("AZURE_TOKEN_CREDENTIALS");
        if ["", "DEV", "unknown"].contains(&selection.as_str()) {
            assert!(DefaultAzureCredential::new().is_err());
            return;
        }
        let credential = DefaultAzureCredential::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let token = runtime.block_on(credential.get_token()).unwrap();
        let (wanted, tool) = match selection.as_str() {
            "AzureDeveloperCLICredential" => ("azd_fixture", "azd"),
            "AzurePowerShellCredential" => ("pwsh_fixture", "pwsh"),
            _ => ("cli_fixture", "az"),
        };
        assert_eq!(token.token, wanted);
        std::fs::remove_file(
            std::path::PathBuf::from(std::env::var_os("O15_AZURE_CHILD").unwrap()).join(tool),
        )
        .unwrap();
        assert!(runtime.block_on(credential.get_token()).is_err());
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    #[derive(Debug)]
    struct FailingRefresh(std::sync::atomic::AtomicUsize);
    #[async_trait::async_trait]
    impl TokenCredential for FailingRefresh {
        /// Go `TokenCredential.GetToken`: controlled token-provider test fixture.
        async fn get_token(
            &self,
            _: &[&str],
            _: Option<azure_core::credentials::TokenRequestOptions<'_>>,
        ) -> azure_core::Result<azure_core::credentials::AccessToken> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(azure_core::credentials::AccessToken::new(
                    "fixture",
                    azure_core::time::OffsetDateTime::now_utc()
                        + azure_core::time::Duration::minutes(1),
                ))
            } else {
                Err(azure_core::Error::with_message(
                    azure_core::error::ErrorKind::Credential,
                    "fixture failure",
                ))
            }
        }
    }
    /// Go Azure SDK temporal `TestExpiringResourceError`.
    #[test]
    fn test_expiring_resource_error() {
        let native = Arc::new(FailingRefresh(AtomicUsize::new(0)));
        let credential = Credentials {
            static_credential: None,
            identity: None,
            direct: Some(native.clone()),
            cached: std::sync::Mutex::new(Cache::default()),
            refreshing: tokio::sync::Mutex::new(()),
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let check = |value: reqsign::AzureStorageCredential| {
                if let reqsign::AzureStorageCredential::BearerToken(token, _) = value {
                    assert_eq!(token, "fixture");
                } else {
                    panic!("not a bearer token");
                }
            };
            check(credential.load().await.unwrap());
            credential.cached.lock().unwrap().last_attempt =
                Some(Utc::now() - chrono::Duration::hours(1));
            for _ in 0..3 {
                check(credential.load().await.unwrap());
                assert_eq!(native.0.load(Ordering::SeqCst), 2);
            }
            if let Some(reqsign::AzureStorageCredential::BearerToken(_, expires)) =
                &mut credential.cached.lock().unwrap().credential
            {
                *expires = Utc::now() - chrono::Duration::hours(1);
            }
            assert!(credential.load().await.is_err());
            assert_eq!(native.0.load(Ordering::SeqCst), 3);
        });
    }
    /// Go `TestManagedIdentityCredential_GetTokenIMDS400` and `GetTokenUnexpectedJSON`.
    #[test]
    fn test_managed_identity_credential_get_token_imds400() {
        use azure_core::{error::ErrorKind, http::StatusCode, Error};
        let error = Error::with_message(
            ErrorKind::HttpResponse {
                status: StatusCode::BadRequest,
                error_code: None,
                raw_response: None,
            },
            "fixture",
        );
        assert!(managed_unavailable(&error, true, false));
        assert!(!managed_unavailable(&error, true, true));
        assert!(!managed_unavailable(&error, false, false));
        assert!(managed_unavailable(
            &Error::with_message(ErrorKind::DataConversion, "fixture"),
            true,
            false
        ));
        assert!(!managed_unavailable(
            &Error::with_message(ErrorKind::Io, "fixture"),
            true,
            false
        ));
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[derive(Debug)]
    struct CountingToken(AtomicUsize);
    #[async_trait::async_trait]
    impl TokenCredential for CountingToken {
        /// Go `TokenCredential.GetToken`: controlled token-provider test fixture.
        async fn get_token(
            &self,
            _: &[&str],
            _: Option<azure_core::credentials::TokenRequestOptions<'_>>,
        ) -> azure_core::Result<azure_core::credentials::AccessToken> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(azure_core::credentials::AccessToken::new(
                "fixture",
                azure_core::time::OffsetDateTime::now_utc() + azure_core::time::Duration::hours(1),
            ))
        }
    }
    /// Go `TestBearerTokenPolicy_RequiresHTTPS` and `TestBearerTokenPolicy_TokenExpired`.
    #[test]
    fn test_bearer_token_policy_requires_https() {
        use opendal::raw::HttpFetch;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let native = Arc::new(CountingToken(AtomicUsize::new(0)));
            let credentials = Arc::new(Credentials {
                static_credential: None,
                identity: None,
                direct: Some(native.clone()),
                cached: std::sync::Mutex::new(Cache::default()),
                refreshing: tokio::sync::Mutex::new(()),
            });
            let transport = Transport {
                client: reqwest12::Client::new(),
                credentials: credentials.clone(),
            };
            let request = http::Request::get("http://127.0.0.1:1/container/blob")
                .body(opendal::Buffer::new())
                .unwrap();
            assert!(transport.fetch(request).await.is_err());
            assert_eq!(native.0.load(Ordering::SeqCst), 0);
            credentials.load().await.unwrap();
            credentials.load().await.unwrap();
            assert_eq!(native.0.load(Ordering::SeqCst), 1);
            credentials.expire();
            credentials.load().await.unwrap();
            assert_eq!(native.0.load(Ordering::SeqCst), 2);
        });
    }
}
