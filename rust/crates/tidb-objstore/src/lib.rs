//! Go `pkg/objstore`: storage URLs parsed into `backuppb` storage backends
//! (`parse.go`), the storage interface (`storeapi`, `objectio`), the
//! local, memory, no-op, HDFS and cloud storages, compression
//! (`compressedio`, `compress.go`) and `New` (`storage.go`).
//!
//! Errors are Go's `error.Error()` text; as in Go, none of them quotes a
//! credential-bearing URL unredacted.

mod options;

mod cloud;
pub mod compress;
pub mod compressedio;
pub mod error;
pub mod gopath;
mod hdfs;
mod local;
pub mod locking;
mod memstore;
mod noop;
mod ossstore;
mod s3_transport;
mod azclient;
mod gcsclient;
mod s3client;
pub mod objectio;
mod storage;
pub mod storeapi;
#[cfg(test)]
mod storage_tests;

pub use cloud::{new_azure_blob_storage, new_gcs_storage, new_s3_storage, CloudStorage};
pub use error::{berrors, Error, ErrorKind};
pub use hdfs::HdfsStorage;
pub use local::{LocalStorage, LOCAL_URI_PREFIX, TOMBSTONE_SIZE};
pub use memstore::MemStorage;
pub use noop::{NoopStorage, NoopWriter};
pub use storage::{create, new, new_from_url, new_with_default_opt, read_data_in_range, validate_cloud_storage_uri};

pub use options::{AzblobBackendOptions, GCSBackendOptions, QueryParameters};

use tidb_go_url::URL;
use tidb_objstore_s3like::{invalid_config, S3BackendOptions, KS3_SDK_PROVIDER, OSS_PROVIDER};
use tidb_proto::backup::{
    storage_backend::Backend, AzureBlobStorage, Gcs, Hdfs, Local, Noop, StorageBackend, S3,
};

/// Go `BackendOptions`: backend configuration the storage URL does not
/// express.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct BackendOptions {
    /// Go `S3` (json `s3`).
    pub s3: S3BackendOptions,
    /// Go `GCS` (json `gcs`).
    pub gcs: GCSBackendOptions,
    /// Go `Azblob` (json `azblob`).
    pub azblob: AzblobBackendOptions,
}

/// Go `BackendOptions.Redacted`: a copy with the credentials masked, so the
/// options can be logged.
impl BackendOptions {
    pub fn redacted(&self) -> Self {
        let mut o = self.clone();
        for secret in [
            &mut o.s3.access_key,
            &mut o.s3.secret_access_key,
            &mut o.s3.session_token,
            &mut o.s3.sse_kms_context,
            &mut o.azblob.account_key,
            &mut o.azblob.sas_token,
            &mut o.azblob.encryption_key,
        ] {
            mask_if_set(secret);
        }
        // A SAS token or userinfo may be written into the endpoint.
        for endpoint in [
            &mut o.s3.endpoint,
            &mut o.gcs.endpoint,
            &mut o.azblob.endpoint,
        ] {
            *endpoint = redact_endpoint(endpoint);
        }
        o
    }
}

/// Go `redactEndpoint`: unlike storage URLs, endpoints cannot be local
/// paths, so a scheme and host are required before `RedactURL`.
fn redact_endpoint(endpoint: &str) -> String {
    if endpoint.is_empty() {
        return String::new();
    }
    match tidb_go_url::parse(endpoint) {
        Ok(u) if !u.scheme.is_empty() && !u.host.is_empty() => redact_url(endpoint),
        _ => INVALID_URL_PLACEHOLDER.into(),
    }
}

/// Go `maskIfSet`: an unset credential stays empty.
fn mask_if_set(secret: &mut String) {
    if !secret.is_empty() {
        *secret = tidb_ast::REDACTED_VALUE.into();
    }
}

/// Go `InvalidURLPlaceholder`.
pub const INVALID_URL_PLACEHOLDER: &str = tidb_ast::INVALID_URL_PLACEHOLDER;

/// Go `RedactURL`, which is `ast.RedactURL`.
pub fn redact_url(raw_url: &str) -> String {
    tidb_ast::redact_url(raw_url)
}

/// Go `ParseRawURL`: `+` is escaped first, because an AWS secret key may
/// hold one and a query `+` decodes to a space. The error names no part of
/// the URL: Go's `url.Error` and its reason can both quote it.
pub fn parse_raw_url(raw_url: &str) -> Result<URL, String> {
    tidb_go_url::parse(raw_url.replace('+', "%2B"))
        .map_err(|_| invalid_config("parse storage URL failed: invalid format"))
}

/// Go `ParseBackendFromURL`.
pub fn parse_backend_from_url(
    u: &mut URL,
    options: Option<&BackendOptions>,
) -> Result<StorageBackend, String> {
    parse_backend_inner(u, "", options)
}

/// Go `ParseBackend`.
pub fn parse_backend(
    raw_url: &str,
    options: Option<&BackendOptions>,
) -> Result<StorageBackend, String> {
    if raw_url.is_empty() {
        return Err(invalid_config("empty store is not allowed"));
    }
    let mut u = parse_raw_url(raw_url)?;
    parse_backend_inner(&mut u, raw_url, options)
}

fn backend(backend: Backend) -> StorageBackend {
    StorageBackend {
        backend: Some(backend),
    }
}

/// Go `strings.Trim(s, "/")`.
fn trim_slashes(path: &[u8]) -> String {
    let start = path.iter().position(|&c| c != b'/').unwrap_or(path.len());
    let end = path
        .iter()
        .rposition(|&c| c != b'/')
        .map_or(start, |i| i + 1);
    String::from_utf8_lossy(&path[start..end.max(start)]).into_owned()
}

/// Go `parseBackend`.
fn parse_backend_inner(
    u: &mut URL,
    raw_url: &str,
    options: Option<&BackendOptions>,
) -> Result<StorageBackend, String> {
    let raw_url = if raw_url.is_empty() {
        // Go: "try to handle hdfs for ParseBackendFromURL caller".
        u.string()
    } else {
        raw_url.to_owned()
    };
    let bucket = String::from_utf8_lossy(&u.host).into_owned();
    match u.scheme.as_slice() {
        b"" => {
            let path = go_abs(&raw_url).ok_or_else(|| {
                invalid_config(&format!(
                    "covert data-source-dir '{raw_url}' to absolute path failed"
                ))
            })?;
            Ok(backend(Backend::Local(Local { path })))
        }
        b"local" | b"file" => Ok(backend(Backend::Local(Local {
            path: String::from_utf8_lossy(&u.path).into_owned(),
        }))),
        b"hdfs" => Ok(backend(Backend::Hdfs(Hdfs { remote: raw_url }))),
        b"noop" => Ok(backend(Backend::Noop(Noop {}))),
        b"s3" | b"ks3" | b"oss" => {
            if bucket.is_empty() {
                return Err(invalid_config(&format!(
                    "please specify the bucket for s3 in {}",
                    redact_url(&raw_url)
                )));
            }
            let mut s3 = S3 {
                bucket,
                prefix: trim_slashes(&u.path),
                ..S3::default()
            };
            let mut s3_options = options.map_or_else(
                || S3BackendOptions {
                    force_path_style: true,
                    ..S3BackendOptions::default()
                },
                |options| options.s3.clone(),
            );
            extract_query_parameters(u, &mut s3_options)?;
            s3_options.set_force_path_style(&raw_url);
            s3_options.apply(&mut s3)?;
            match u.scheme.as_slice() {
                b"ks3" => s3.provider = KS3_SDK_PROVIDER.into(),
                b"oss" => s3.provider = OSS_PROVIDER.into(),
                _ => {}
            }
            Ok(backend(Backend::S3(s3)))
        }
        b"gs" | b"gcs" => {
            if bucket.is_empty() {
                return Err(invalid_config(&format!(
                    "please specify the bucket for gcs in {}",
                    redact_url(&raw_url)
                )));
            }
            let mut gcs = Gcs {
                bucket,
                prefix: trim_slashes(&u.path),
                ..Gcs::default()
            };
            let mut gcs_options = options.map(|o| o.gcs.clone()).unwrap_or_default();
            extract_query_parameters(u, &mut gcs_options)?;
            gcs_options.apply(&mut gcs)?;
            Ok(backend(Backend::Gcs(gcs)))
        }
        b"azure" | b"azblob" => {
            if bucket.is_empty() {
                return Err(invalid_config(&format!(
                    "please specify the bucket for azblob in {}",
                    redact_url(&raw_url)
                )));
            }
            let mut azblob = AzureBlobStorage {
                bucket,
                prefix: trim_slashes(&u.path),
                ..AzureBlobStorage::default()
            };
            let mut azblob_options = options.map(|o| o.azblob.clone()).unwrap_or_default();
            extract_query_parameters(u, &mut azblob_options)?;
            azblob_options.apply(&mut azblob)?;
            Ok(backend(Backend::AzureBlobStorage(azblob)))
        }
        scheme => Err(invalid_config(&format!(
            "storage {} not support yet",
            String::from_utf8_lossy(scheme)
        ))),
    }
}

/// Go `ExtractQueryParameters`: moves the URL's query parameters into
/// `options`, matching each normalized key to a json tag, then clears the
/// query. Go walks the query map in random order, so two keys that
/// normalize alike pick an arbitrary winner there; here the later key in
/// byte order wins. A query that cannot be fully decoded is an error and
/// leaves `options` unchanged: `URL.Query` would silently drop a pair such
/// as `sse-kms-context`.
pub fn extract_query_parameters(
    u: &mut URL,
    options: &mut dyn QueryParameters,
) -> Result<(), String> {
    let (values, err) = tidb_go_url::parse_query(&u.raw_query);
    if err.is_some() {
        // The decoder error may quote part of the query.
        return Err(invalid_config(
            "parse storage URL query failed: invalid format",
        ));
    }
    for (key, params) in values.0 {
        let Some(param) = params.first() else {
            continue;
        };
        let key = normalize_query_parameter_key(&String::from_utf8_lossy(&key));
        options.set_query_parameter(&key, param);
    }
    u.raw_query.clear();
    Ok(())
}

/// Go `NormalizeQueryParameterKey`: `_` becomes `-`, then lowercase.
pub fn normalize_query_parameter_key(key: &str) -> String {
    key.replace('_', "-").to_lowercase()
}

/// Go `FormatBackendURL`: the URL that reconstructs `backend`, without the
/// options (and so without secrets).
pub fn format_backend_url(backend: &StorageBackend) -> URL {
    let mut u = URL::default();
    match &backend.backend {
        Some(Backend::Local(local)) => {
            u.scheme = b"local".to_vec();
            u.path = local.path.clone().into_bytes();
        }
        Some(Backend::Noop(_)) => {
            u.scheme = b"noop".to_vec();
            u.path = b"/".to_vec();
        }
        Some(Backend::S3(s3)) => {
            u.scheme = match s3.provider.as_str() {
                OSS_PROVIDER => b"oss".to_vec(),
                KS3_SDK_PROVIDER => b"ks3".to_vec(),
                _ => b"s3".to_vec(),
            };
            u.host = s3.bucket.clone().into_bytes();
            u.path = s3.prefix.clone().into_bytes();
        }
        Some(Backend::Gcs(gcs)) => {
            u.scheme = b"gcs".to_vec();
            u.host = gcs.bucket.clone().into_bytes();
            u.path = gcs.prefix.clone().into_bytes();
        }
        Some(Backend::AzureBlobStorage(azblob)) => {
            u.scheme = b"azure".to_vec();
            u.host = azblob.bucket.clone().into_bytes();
            u.path = azblob.prefix.clone().into_bytes();
        }
        _ => {}
    }
    u
}

/// Go `IsLocalPath`.
pub fn is_local_path(p: &str) -> Result<bool, String> {
    tidb_go_url::parse(p)
        .map(|u| is_local(&u))
        .map_err(|_| invalid_config("invalid storage URL"))
}

/// Go `IsLocal`: the `local`, `file` or empty scheme.
pub fn is_local(u: &URL) -> bool {
    matches!(u.scheme.as_slice(), b"local" | b"file" | b"")
}

/// Go `IsS3Like`: the `s3` or `oss` scheme.
pub fn is_s3_like(u: &URL) -> bool {
    matches!(u.scheme.as_slice(), b"s3" | b"oss")
}

/// Go `filepath.Abs` on Unix: `Clean` of the path, joined to the working
/// directory first when relative. `None` when the working directory is
/// unavailable.
fn go_abs(path: &str) -> Option<String> {
    if path.starts_with('/') {
        return Some(go_clean(path));
    }
    let wd = std::env::current_dir().ok()?;
    let wd = wd.to_str()?;
    Some(go_clean(&format!("{wd}/{path}")))
}

/// Go `path.Clean` (Unix `filepath.Clean`): the shortest lexically
/// equivalent path.
fn go_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".into();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            part => parts.push(part),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".into(),
        (false, false) => joined,
    }
}

#[cfg(test)]
mod tests;
