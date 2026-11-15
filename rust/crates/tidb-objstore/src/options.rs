//! Go `pkg/objstore`'s GCS and Azure Blob backend options (`gcs.go`,
//! `azblob.go`) and the query-parameter targets of `ExtractQueryParameters`.
//! The storage clients and flag parsing are not ported.

use base64::Engine as _;
use sha2::Digest as _;
use tidb_objstore_s3like::S3BackendOptions;
use tidb_proto::backup::{AzureBlobStorage, AzureCustomerKey, Gcs};

/// Go `GCSBackendOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GCSBackendOptions {
    /// Go `Endpoint` (json `endpoint`).
    pub endpoint: String,
    /// Go `StorageClass` (json `storage-class`).
    pub storage_class: String,
    /// Go `PredefinedACL` (json `predefined-acl`).
    pub predefined_acl: String,
    /// Go `CredentialsFile` (json `credentials-file`).
    pub credentials_file: String,
}

impl GCSBackendOptions {
    /// Go `GCSBackendOptions.apply`: a credentials file is read into the
    /// backend's credentials blob.
    pub(crate) fn apply(&self, gcs: &mut Gcs) -> Result<(), String> {
        gcs.endpoint.clone_from(&self.endpoint);
        gcs.storage_class.clone_from(&self.storage_class);
        gcs.predefined_acl.clone_from(&self.predefined_acl);
        if !self.credentials_file.is_empty() {
            let blob = std::fs::read(&self.credentials_file)
                .map_err(|e| format!("open {}: {}", self.credentials_file, go_os_error(&e)))?;
            gcs.credentials_blob = String::from_utf8_lossy(&blob).into_owned();
        }
        Ok(())
    }
}

/// Go `syscall.Errno.Error` for the errors an `os.ReadFile` reports.
fn go_os_error(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".into(),
        std::io::ErrorKind::PermissionDenied => "permission denied".into(),
        std::io::ErrorKind::IsADirectory => "is a directory".into(),
        _ => error.to_string(),
    }
}

/// Go `AzblobBackendOptions`. Deliberately no `Debug`: it holds credentials.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AzblobBackendOptions {
    /// Go `Endpoint` (json `endpoint`).
    pub endpoint: String,
    /// Go `AccountName` (json `account-name`).
    pub account_name: String,
    /// Go `AccountKey` (json `account-key`).
    pub account_key: String,
    /// Go `AccessTier` (json `access-tier`).
    pub access_tier: String,
    /// Go `SASToken` (json `sas-token`).
    pub sas_token: String,
    /// Go `EncryptionScope` (json `encryption-scope`).
    pub encryption_scope: String,
    /// Go `EncryptionKey` (json `encryption-key`).
    pub encryption_key: String,
}

impl AzblobBackendOptions {
    /// Go `AzblobBackendOptions.apply`: an absent encryption key falls back
    /// to `$AZURE_ENCRYPTION_KEY`; a key travels as base64 with its SHA-256.
    pub(crate) fn apply(&mut self, azblob: &mut AzureBlobStorage) -> Result<(), String> {
        azblob.endpoint.clone_from(&self.endpoint);
        azblob.storage_class.clone_from(&self.access_tier);
        azblob.account_name.clone_from(&self.account_name);
        azblob.shared_key.clone_from(&self.account_key);
        azblob.access_sig.clone_from(&self.sas_token);
        azblob.encryption_scope.clone_from(&self.encryption_scope);
        if self.encryption_key.is_empty() {
            self.encryption_key = std::env::var("AZURE_ENCRYPTION_KEY").unwrap_or_default();
        }
        if !self.encryption_key.is_empty() {
            let key = self.encryption_key.as_bytes();
            let engine = base64::engine::general_purpose::STANDARD;
            azblob.encryption_key = Some(AzureCustomerKey {
                encryption_key: engine.encode(key),
                encryption_key_sha256: engine.encode(sha2::Sha256::digest(key)),
            });
        }
        Ok(())
    }
}

/// The options structs `ExtractQueryParameters` fills. Go finds each
/// field by its json tag through reflection; each impl names the same tags.
pub trait QueryParameters {
    /// Go `ExtractQueryParameters`'s per-field step: a string field takes
    /// `param`; a bool field takes it only when `strconv.ParseBool` accepts
    /// it; a tag naming no field is ignored.
    fn set_query_parameter(&mut self, tag: &str, param: &[u8]);
}

/// Go `strconv.ParseBool`.
fn parse_bool(param: &[u8]) -> Option<bool> {
    match param {
        b"1" | b"t" | b"T" | b"TRUE" | b"true" | b"True" => Some(true),
        b"0" | b"f" | b"F" | b"FALSE" | b"false" | b"False" => Some(false),
        _ => None,
    }
}

macro_rules! query_parameters {
    ($ty:ty, strings { $($s:literal => $sf:ident),* $(,)? }, bools { $($b:literal => $bf:ident),* $(,)? }) => {
        impl QueryParameters for $ty {
            fn set_query_parameter(&mut self, tag: &str, param: &[u8]) {
                match tag {
                    $($s => self.$sf = String::from_utf8_lossy(param).into_owned(),)*
                    $($b => {
                        if let Some(value) = parse_bool(param) {
                            self.$bf = value;
                        }
                    })*
                    _ => {}
                }
            }
        }
    };
}

query_parameters!(S3BackendOptions, strings {
    "endpoint" => endpoint,
    "region" => region,
    "storage-class" => storage_class,
    "sse" => sse,
    "sse-kms-key-id" => sse_kms_key_id,
    "sse-kms-context" => sse_kms_context,
    "acl" => acl,
    "access-key" => access_key,
    "secret-access-key" => secret_access_key,
    "session-token" => session_token,
    "provider" => provider,
    "role-arn" => role_arn,
    "external-id" => external_id,
    "profile" => profile,
}, bools {
    "force-path-style" => force_path_style,
    "use-accelerate-endpoint" => use_accelerate_endpoint,
    "object-lock-enabled" => object_lock_enabled,
});

query_parameters!(GCSBackendOptions, strings {
    "endpoint" => endpoint,
    "storage-class" => storage_class,
    "predefined-acl" => predefined_acl,
    "credentials-file" => credentials_file,
}, bools {});

query_parameters!(AzblobBackendOptions, strings {
    "endpoint" => endpoint,
    "account-name" => account_name,
    "account-key" => account_key,
    "access-tier" => access_tier,
    "sas-token" => sas_token,
    "encryption-scope" => encryption_scope,
    "encryption-key" => encryption_key,
}, bools {});
