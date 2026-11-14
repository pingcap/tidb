// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `ossstore.NewOSSStorage`: static credentials or the Aliyun default chain.
use reqsign_aliyun_oss as native;
use reqsign_core::{Context, OsEnv, ProvideCredential};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
enum Provider {
    Static(native::StaticCredentialProvider),
    Default(crate::default_provider::DefaultProvider),

    #[cfg(test)]
    Test(Arc<dyn Fn() -> Result<Option<native::Credential>, ()> + Send + Sync>),
}
/// Go OSS SDK credentials provider and temporary-credential cache.
pub struct Credentials {
    context: Context,
    provider: Provider,
    role: Option<crate::assume_role::Role>,
    refresh_threshold: Duration,
    cached: Mutex<Option<native::Credential>>,
    refresh_lock: tokio::sync::Mutex<()>,
    background_active: AtomicBool,
    background: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::task::JoinHandle<()>,
        )>,
    >,
}
impl Credentials {
    /// Go `providers.NewDefaultCredentialsProvider`, in Go's provider order.
    pub fn new(access: &str, secret: &str, token: &str, role: &str, external_id: &str) -> Self {
        let context = Context::new()
            .with_env(OsEnv)
            .with_file_read(reqsign_file_read_tokio::TokioFileRead)
            .with_http_send(reqsign_http_send_reqwest::ReqwestHttpSend::new(
                reqwest::Client::new(),
            ));
        let provider = if !access.is_empty() && !secret.is_empty() {
            let mut provider = native::StaticCredentialProvider::new(access, secret);
            if !token.is_empty() {
                provider = provider.with_security_token(token);
            }
            Provider::Static(provider)
        } else {
            Provider::Default(crate::default_provider::DefaultProvider::new(&context))
        };
        Self {
            context,
            provider,
            role: (!role.is_empty()).then(|| crate::assume_role::Role::new(role, external_id)),
            refresh_threshold: Duration::from_secs(180),
            cached: Mutex::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            background_active: AtomicBool::new(false),
            background: Mutex::new(None),
        }
    }
    /// Go `NewOSSStorage`: detect ECS before deciding whether to use its internal endpoint.
    pub async fn ecs_region(&self) -> Result<String, ()> {
        let Provider::Default(provider) = &self.provider else {
            return Ok(String::new());
        };
        let _guard = self.refresh_lock.lock().await;
        provider.load(&self.context).await?.ok_or(())?;
        if !provider.uses_ecs_metadata() {
            return Ok(String::new());
        }
        let response = reqwest::Client::new()
            .get("http://100.100.100.200/latest/meta-data/region-id")
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|_| ())?;
        if !response.status().is_success() {
            return Err(());
        }
        String::from_utf8(response.bytes().await.map_err(|_| ())?.to_vec()).map_err(|_| ())
    }
    /// Go Meter `NewCredentialCache`: 15-minute refresh threshold and regional STS.
    pub fn new_meter(access: &str, secret: &str, token: &str, role: &str, region: &str) -> Self {
        let mut credentials = Self::new(access, secret, token, "", "");
        if !role.is_empty() {
            credentials.role = Some(crate::assume_role::Role::meter(role, region));
            credentials.refresh_threshold = Duration::from_secs(15 * 60);
        }
        credentials
    }
    /// Go Meter `CredentialCache.StartBackgroundRefresh`: check existing credentials every five minutes.
    pub fn start_meter_refresh(self: &Arc<Self>) {
        if self.role.is_none() {
            return;
        }
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
        let owner = Arc::downgrade(self);
        let worker = tokio::spawn(async move {
            loop {
                tokio::select! {_=&mut cancelled=>break,_=tokio::time::sleep(Duration::from_secs(300))=>{}}
                let Some(owner) = owner.upgrade() else { break };
                let has_cache = owner
                    .cached
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some();
                if has_cache {
                    let _ = owner.refresh_once().await;
                }
            }
        });
        *self
            .background
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((cancel, worker));
    }
    /// Go `credentialRefresher.GetCredentials`: background mode returns the last snapshot.
    pub async fn load(&self) -> Result<Option<reqsign::AliyunCredential>, ()> {
        let credential = if self.background_active.load(Ordering::Acquire) {
            self.cached
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        } else {
            self.refresh_once().await?
        };
        Ok(credential.map(|cred| reqsign::AliyunCredential {
            access_key_id: cred.access_key_id,
            access_key_secret: cred.access_key_secret,
            security_token: cred.security_token,
            expires_in: None,
        }))
    }
    /// Go Meter `CredentialCache.needsRefresh` and credentials-go's expiration window.
    fn needs_refresh(&self, expiration: reqsign_core::time::Timestamp) -> bool {
        let threshold = reqsign_core::time::Timestamp::now() + self.refresh_threshold;
        if self.role.as_ref().is_some_and(|role| role.meter) {
            threshold > expiration
        } else {
            threshold >= expiration
        }
    }
    /// Go `credentialRefresher.refreshOnce` and credentials-go's 180s expiration window.
    async fn refresh_once(&self) -> Result<Option<native::Credential>, ()> {
        let _guard = self.refresh_lock.lock().await;
        let cached = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(cred) = cached.filter(|cred| {
            cred.expires_in
                .is_some_and(|expiry| !self.needs_refresh(expiry))
        }) {
            return Ok(Some(cred));
        }
        let credential = match &self.provider {
            Provider::Static(p) => p.provide_credential(&self.context).await.map_err(|_| ())?,
            Provider::Default(p) => p.load(&self.context).await?,
            #[cfg(test)]
            Provider::Test(p) => p()?,
        };
        let credential = match (&self.role, credential) {
            (Some(role), Some(base)) => Some(role.load(&base).await?),
            (_, credential) => credential,
        };
        if let Some(credential) = &credential {
            *self
                .cached
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(credential.clone());
        }
        Ok(credential)
    }
    /// Go `credentialRefresher.startRefresh`: initialize and refresh every five seconds.
    pub async fn start_refresh(self: &Arc<Self>) -> Result<(), ()> {
        self.refresh_once().await?.ok_or(())?;
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
        let owner = Arc::downgrade(self);
        let worker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=&mut cancelled=>break,
                    _=tokio::time::sleep(Duration::from_secs(5))=>{},
                }
                let Some(owner) = owner.upgrade() else { break };
                // Go preserves the current credential after a refresh failure.
                let _ = owner.refresh_once().await;
            }
        });
        *self
            .background
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((cancel, worker));
        self.background_active.store(true, Ordering::Release);
        Ok(())
    }
    /// Go `credentialRefresher.close`: cancel the loop and wait for its current refresh.
    pub async fn close(&self) {
        let worker = self
            .background
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((cancel, worker)) = worker {
            let _ = cancel.send(());
            let _ = worker.await;
        }
    }
    /// Rust resource drop also stops the Go-owned refresh loop when Close is omitted.
    /// Go `OSSStorage.Close`: Rust drop also cancels its credential refresh worker.
    pub fn abort_refresh(&self) {
        if let Some((cancel, worker)) = self
            .background
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = cancel.send(());
            worker.abort();
        }
    }
}
impl Drop for Credentials {
    /// Go `OSSStorage.Close`, applied when the Rust owner is dropped.
    fn drop(&mut self) {
        self.abort_refresh();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go SDK `TestCredentialCache_needsRefresh`.
    #[test]
    fn test_credential_cache_needs_refresh() {
        let credentials =
            Credentials::new_meter("fixture", "fixture", "", "roleArn", "cn-hangzhou");
        assert_eq!(credentials.refresh_threshold, Duration::from_secs(900));
        let now = reqsign_core::time::Timestamp::now();
        assert!(credentials.needs_refresh(now + Duration::from_secs(600)));
        assert!(!credentials.needs_refresh(now + Duration::from_secs(1200)));
        assert!(credentials.needs_refresh(now - Duration::from_secs(60)));
    }
    /// Go SDK `TestCredentialCache_GetCredentials_WithValidCache` and `ConcurrentAccess`.
    #[tokio::test]
    async fn test_credential_cache_get_credentials_with_valid_cache() {
        let credentials = Arc::new(Credentials::new_meter(
            "fixture",
            "fixture",
            "",
            "roleArn",
            "cn-hangzhou",
        ));
        *credentials.cached.lock().unwrap() = Some(native::Credential {
            access_key_id: "cached-access-key".into(),
            access_key_secret: "cached-access-secret".into(),
            security_token: Some("cached-security-token".into()),
            expires_in: Some(reqsign_core::time::Timestamp::now() + Duration::from_secs(3600)),
        });
        let mut readers = Vec::new();
        for _ in 0..50 {
            let credentials = credentials.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..10 {
                    let credential = credentials.load().await.unwrap().unwrap();
                    assert_eq!(credential.access_key_id, "cached-access-key");
                    assert_eq!(credential.access_key_secret, "cached-access-secret");
                    assert_eq!(
                        credential.security_token.as_deref(),
                        Some("cached-security-token")
                    );
                }
            }));
        }
        for reader in readers {
            reader.await.unwrap();
        }
    }
    /// Go `ossstore.TestCredentialRefresher`, using Tokio's virtual clock for synctest.
    #[tokio::test(start_paused = true)]
    async fn test_credential_refresher() {
        let start = tokio::time::Instant::now();
        let mut credentials = Credentials::new("fixture", "fixture", "", "", "");
        credentials.provider = Provider::Test(Arc::new(move || {
            Ok(Some(native::Credential {
                access_key_id: start.elapsed().as_nanos().to_string(),
                ..Default::default()
            }))
        }));
        let credentials = Arc::new(credentials);
        credentials.refresh_once().await.unwrap();
        assert_eq!(
            credentials.load().await.unwrap().unwrap().access_key_id,
            "0"
        );
        credentials.start_refresh().await.unwrap();
        for _ in 0..13 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(5)).await;
        }
        tokio::task::yield_now().await;
        let timestamp = credentials
            .load()
            .await
            .unwrap()
            .unwrap()
            .access_key_id
            .parse::<u128>()
            .unwrap();
        assert!(timestamp >= Duration::from_secs(60).as_nanos());
        credentials.close().await;
        tokio::time::advance(Duration::from_secs(65)).await;
        assert_eq!(
            credentials
                .load()
                .await
                .unwrap()
                .unwrap()
                .access_key_id
                .parse::<u128>()
                .unwrap(),
            timestamp
        );
    }
}
