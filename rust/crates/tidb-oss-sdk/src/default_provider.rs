// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go credentials-go `DefaultCredentialsProvider`, preserving selected-provider ownership.
use reqsign_aliyun_oss as native;
use reqsign_core::{Context, ProvideCredential, ProvideCredentialDyn};
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug)]
struct Environment;
impl ProvideCredential for Environment {
    type Credential = native::Credential;
    /// Go `EnvironmentVariableCredentialsProvider.GetCredentials`, without OSS-specific aliases.
    async fn provide_credential(
        &self,
        ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let env = ctx.env_vars();
        let access = env
            .get("ALIBABA_CLOUD_ACCESS_KEY_ID")
            .filter(|v| !v.is_empty());
        let secret = env
            .get("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
            .filter(|v| !v.is_empty());
        Ok(match (access, secret) {
            (Some(access), Some(secret)) => Some(native::Credential {
                access_key_id: access.clone(),
                access_key_secret: secret.clone(),
                security_token: env
                    .get("ALIBABA_CLOUD_SECURITY_TOKEN")
                    .filter(|v| !v.is_empty())
                    .cloned(),
                expires_in: None,
            }),
            _ => None,
        })
    }
}
#[derive(Debug)]
pub(crate) struct DefaultProvider {
    chain: Vec<(
        &'static str,
        Box<dyn ProvideCredentialDyn<Credential = native::Credential>>,
    )>,
    last: AtomicUsize,
}
impl DefaultProvider {
    /// Go `NewDefaultCredentialsProvider`: append only configured optional providers.
    pub fn new(ctx: &Context) -> Self {
        let env = ctx.env_vars();
        let mut chain: Vec<(
            &'static str,
            Box<dyn ProvideCredentialDyn<Credential = native::Credential>>,
        )> = vec![("env", Box::new(Environment))];
        if [
            "ALIBABA_CLOUD_OIDC_TOKEN_FILE",
            "ALIBABA_CLOUD_OIDC_PROVIDER_ARN",
            "ALIBABA_CLOUD_ROLE_ARN",
        ]
        .iter()
        .all(|key| env.get(*key).is_some_and(|v| !v.is_empty()))
        {
            chain.push((
                "oidc",
                Box::new(native::AssumeRoleWithOidcCredentialProvider::new()),
            ));
        }
        chain.push((
            "cli_profile",
            Box::new(native::ConfigFileCredentialProvider::new()),
        ));
        chain.push((
            "profile",
            Box::new(native::CredentialsFileCredentialProvider::new()),
        ));
        if !env
            .get("ALIBABA_CLOUD_ECS_METADATA_DISABLED")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            chain.push((
                "ecs_ram_role",
                Box::new(native::EcsRamRoleCredentialProvider::new()),
            ));
        }
        if env
            .get("ALIBABA_CLOUD_CREDENTIALS_URI")
            .is_some_and(|v| !v.is_empty())
        {
            chain.push((
                "credentials_uri",
                Box::new(native::CredentialsUriCredentialProvider::new()),
            ));
        }
        Self {
            chain,
            last: AtomicUsize::new(usize::MAX),
        }
    }
    /// Go `DefaultCredentialsProvider.GetCredentials`: retain the last provider, including its failures.
    pub async fn load(&self, ctx: &Context) -> Result<Option<native::Credential>, ()> {
        let last = self.last.load(Ordering::Acquire);
        if last != usize::MAX {
            return self.chain[last]
                .1
                .provide_credential_dyn(ctx)
                .await
                .map_err(|_| ());
        }
        for (index, (_, provider)) in self.chain.iter().enumerate() {
            self.last.store(index, Ordering::Release);
            if let Ok(Some(credential)) = provider.provide_credential_dyn(ctx).await {
                return Ok(Some(credential));
            }
        }
        Err(())
    }
    /// Go `Credentials.ProviderName` for the NewOSSStorage ECS-region decision.
    pub fn uses_ecs_metadata(&self) -> bool {
        self.chain
            .get(self.last.load(Ordering::Acquire))
            .is_some_and(|(name, _)| *name == "ecs_ram_role")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go credentials-go `TestDefaultCredentialsProvider` and `TestDefaultCredentialsProvider_GetCredentials`.
    #[tokio::test]
    async fn test_default_credentials_provider_get_credentials() {
        let mut env = std::collections::HashMap::from([
            ("ALIBABA_CLOUD_ACCESS_KEY_ID".into(), "akid".into()),
            ("ALIBABA_CLOUD_ACCESS_KEY_SECRET".into(), "aksecret".into()),
            ("ALIBABA_CLOUD_ECS_METADATA_DISABLED".into(), "true".into()),
        ]);
        let context = Context::new().with_env(reqsign_core::StaticEnv {
            home_dir: None,
            envs: env.clone(),
        });
        let provider = DefaultProvider::new(&context);
        assert_eq!(
            provider
                .chain
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["env", "cli_profile", "profile"]
        );
        for _ in 0..2 {
            let credential = provider.load(&context).await.unwrap().unwrap();
            assert_eq!(credential.access_key_id, "akid");
            assert_eq!(credential.access_key_secret, "aksecret");
        }
        // The selected environment provider remains selected instead of falling through.
        env.remove("ALIBABA_CLOUD_ACCESS_KEY_ID");
        let context = context.with_env(reqsign_core::StaticEnv {
            home_dir: None,
            envs: env,
        });
        assert!(provider.load(&context).await.unwrap().is_none());
        assert_eq!(provider.last.load(Ordering::Acquire), 0);
    }
}
