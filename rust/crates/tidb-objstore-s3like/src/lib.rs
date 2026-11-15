//! Go `pkg/objstore/s3like`: the S3 backend options a storage URL or BR
//! configuration carries, and their application to `backuppb.S3`
//! (`store.go`, `interface.go`). The S3 client itself is
//! `tidb-planner-extstore`'s; flag parsing is not ported.

use tidb_proto::backup::S3;

/// Go `OSSProvider`.
pub const OSS_PROVIDER: &str = "oss-sdk";
/// Go `KS3SDKProvider`.
pub const KS3_SDK_PROVIDER: &str = "ks3-sdk";

/// Go `S3AccessKey`, the URL query key of the access key.
pub const S3_ACCESS_KEY: &str = "access-key";
/// Go `S3SecretAccessKey`.
pub const S3_SECRET_ACCESS_KEY: &str = "secret-access-key";
/// Go `S3RoleARN`.
pub const S3_ROLE_ARN: &str = "role-arn";
/// Go `S3ExternalID`.
pub const S3_EXTERNAL_ID: &str = "external-id";

/// Go `domainAWS`.
const DOMAIN_AWS: &str = "amazonaws.com";

/// Go `berrors.ErrStorageInvalidConfig` annotated with `msg`.
pub fn invalid_config(msg: &str) -> String {
    format!("{msg}: [BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config")
}

/// Go `S3BackendOptions`. Deliberately no `Debug`: it holds credentials.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct S3BackendOptions {
    /// Go `Endpoint` (json `endpoint`).
    pub endpoint: String,
    /// Go `Region` (json `region`).
    pub region: String,
    /// Go `StorageClass` (json `storage-class`).
    pub storage_class: String,
    /// Go `Sse` (json `sse`).
    pub sse: String,
    /// Go `SseKmsKeyID` (json `sse-kms-key-id`).
    pub sse_kms_key_id: String,
    /// Go `ACL` (json `acl`).
    pub acl: String,
    /// Go `AccessKey` (json `access-key`).
    pub access_key: String,
    /// Go `SecretAccessKey` (json `secret-access-key`).
    pub secret_access_key: String,
    /// Go `SessionToken` (json `session-token`).
    pub session_token: String,
    /// Go `Provider` (json `provider`).
    pub provider: String,
    /// Go `ForcePathStyle` (json `force-path-style`).
    pub force_path_style: bool,
    /// Go `UseAccelerateEndpoint` (json `use-accelerate-endpoint`).
    pub use_accelerate_endpoint: bool,
    /// Go `RoleARN` (json `role-arn`).
    pub role_arn: String,
    /// Go `ExternalID` (json `external-id`).
    pub external_id: String,
    /// Go `Profile` (json `profile`).
    pub profile: String,
    /// Go `ObjectLockEnabled` (json `object-lock-enabled`).
    pub object_lock_enabled: bool,
}

impl S3BackendOptions {
    /// Go `S3BackendOptions.Apply`: checks the endpoint and the key pair,
    /// then copies the options onto `s3`.
    pub fn apply(&self, s3: &mut S3) -> Result<(), String> {
        if !self.endpoint.is_empty() {
            let u = tidb_go_url::parse(&self.endpoint).map_err(|e| e.to_string())?;
            if u.scheme.is_empty() {
                return Err("scheme not found in endpoint".into());
            }
            if u.host.is_empty() {
                return Err("host not found in endpoint".into());
            }
        }
        if self.profile.is_empty() {
            if self.access_key.is_empty() && !self.secret_access_key.is_empty() {
                return Err(invalid_config("access_key not found"));
            }
            if !self.access_key.is_empty() && self.secret_access_key.is_empty() {
                return Err(invalid_config("secret_access_key not found"));
            }
        }
        s3.endpoint = self
            .endpoint
            .strip_suffix('/')
            .unwrap_or(&self.endpoint)
            .to_owned();
        s3.region.clone_from(&self.region);
        s3.storage_class.clone_from(&self.storage_class);
        s3.sse.clone_from(&self.sse);
        s3.sse_kms_key_id.clone_from(&self.sse_kms_key_id);
        s3.acl.clone_from(&self.acl);
        s3.access_key.clone_from(&self.access_key);
        s3.secret_access_key.clone_from(&self.secret_access_key);
        s3.session_token.clone_from(&self.session_token);
        s3.force_path_style = self.force_path_style;
        s3.role_arn.clone_from(&self.role_arn);
        s3.external_id.clone_from(&self.external_id);
        s3.provider.clone_from(&self.provider);
        s3.profile.clone_from(&self.profile);
        Ok(())
    }

    /// Go `S3BackendOptions.SetForcePathStyle`: only ever turns path style
    /// off, for providers and AWS endpoints that need virtual-hosted style.
    pub fn set_force_path_style(&mut self, raw_url: &str) {
        if matches!(self.provider.as_str(), "alibaba" | "netease" | "tencent")
            || self.use_accelerate_endpoint
            || use_virtual_host_style_for_aws_s3(self, raw_url)
        {
            self.force_path_style = false;
        }
    }
}

/// Go `useVirtualHostStyleForAWSS3`. An explicit `force-path-style` in the
/// URL wins.
fn use_virtual_host_style_for_aws_s3(opts: &S3BackendOptions, raw_url: &str) -> bool {
    if raw_url.is_empty()
        || raw_url.contains("force-path-style")
        || raw_url.contains("force_path_style")
    {
        return false;
    }
    opts.provider == "aws" || opts.endpoint.contains(DOMAIN_AWS) || !opts.role_arn.is_empty()
}
