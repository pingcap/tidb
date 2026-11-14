// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go AWS SDK credential configuration shared by objstore and metering_sdk.
use aws_credential_types::provider::ProvideCredentials;

/// Go AWS SDK profile credentials: explicit profiles bypass ambient credentials,
/// while source profiles, role assumption, SSO and credential processes remain
/// the native SDK's responsibility.
pub struct ProfileCredentials(aws_credential_types::provider::SharedCredentialsProvider);
impl ProfileCredentials {
    /// Go `config.LoadDefaultConfig` default credential chain.
    pub async fn default_chain(region: &str) -> Self {
        let chain = aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
            .region(aws_types::region::Region::new(region.to_owned()))
            .build()
            .await;
        Self(aws_credential_types::provider::SharedCredentialsProvider::new(chain))
    }
    /// Go `config.WithSharedConfigProfile`: an explicitly selected AWS profile.
    pub fn new(profile: &str, region: &str) -> Self {
        let config = aws_config::provider_config::ProviderConfig::without_region()
            .with_region(Some(aws_types::region::Region::new(region.to_owned())));
        Self(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_config::profile::ProfileFileCredentialsProvider::builder()
                    .configure(&config)
                    .profile_name(profile)
                    .build(),
            ),
        )
    }
}
#[async_trait::async_trait]
impl reqsign::AwsCredentialLoad for ProfileCredentials {
    /// Go `aws.CredentialsProvider.Retrieve`.
    async fn load_credential(
        &self,
        _client: reqwest12::Client,
    ) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        let credentials = self
            .0
            .provide_credentials()
            .await
            .map_err(|_| anyhow::anyhow!("cannot load S3 profile credentials"))?;
        Ok(Some(reqsign::AwsCredential {
            access_key_id: credentials.access_key_id().to_owned(),
            secret_access_key: credentials.secret_access_key().to_owned(),
            session_token: credentials.session_token().map(str::to_owned),
            expires_in: credentials.expiry().map(chrono::DateTime::from),
        }))
    }
}

impl ProfileCredentials {
    /// Go `metering_sdk/provider.NewS3Provider` config.LoadDefaultConfig and
    /// stscreds.NewAssumeRoleProvider, including the shared endpoint and defaults.
    pub async fn meter(
        region: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        token: &str,
        role: &str,
    ) -> Result<(Self, String), String> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if !region.is_empty() {
            loader = loader.region(aws_types::region::Region::new(region.to_owned()));
        }
        if !endpoint.is_empty() {
            loader = loader.endpoint_url(endpoint);
        }
        if !access_key.is_empty() && !secret_key.is_empty() {
            loader = loader.credentials_provider(aws_credential_types::Credentials::new(
                access_key,
                secret_key,
                (!token.is_empty()).then(|| token.to_owned()),
                None,
                "StaticCredentials",
            ));
        }
        let config = loader.load().await;
        let region = config
            .region()
            .map(|r| r.as_ref().to_owned())
            .unwrap_or_default();
        let credentials = if role.is_empty() {
            config
                .credentials_provider()
                .ok_or_else(|| "AWS credentials provider not found".to_owned())?
        } else {
            let session = format!(
                "aws-go-sdk-{}",
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            );
            let provider = aws_config::sts::AssumeRoleProvider::builder(role)
                .configure(&config)
                .session_name(session)
                .session_length(std::time::Duration::from_secs(900))
                .build()
                .await;
            aws_credential_types::provider::SharedCredentialsProvider::new(provider)
        };
        Ok((Self(credentials), region))
    }
}
