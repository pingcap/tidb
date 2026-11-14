// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `metering_sdk/storage` and its native object-storage providers.

use crate::config::MeteringConfig;
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use tidb_gocontext::Context;

/// Go `ObjectStorageProvider`.
pub trait ObjectStorageProvider: Send + Sync {
    fn upload(&self, ctx: &Context, path: &str, data: &[u8]) -> Result<(), String>;
    fn download(&self, ctx: &Context, path: &str) -> Result<Vec<u8>, String>;
    fn delete(&self, ctx: &Context, path: &str) -> Result<(), String>;
    /// Go `ObjectStorageProvider.Exists`.
    fn exists(&self, ctx: &Context, path: &str) -> Result<bool, String>;
    /// Go `ObjectStorageProvider.List`.
    fn list(&self, ctx: &Context, prefix: &str) -> Result<Vec<String>, String>;
}

/// Go `LocalFSProvider`.
pub struct LocalFSProvider {
    base_path: PathBuf,
    prefix: String,
    create_dirs: bool,
    permissions: u32,
}
impl LocalFSProvider {
    /// Go `NewLocalFSProvider`.
    pub fn new(config: &MeteringConfig) -> Result<Self, String> {
        if config.tp != "localfs" {
            return Err(format!(
                "invalid provider type: {}, expected: localfs",
                config.tp
            ));
        }
        let (base, create_dirs, permissions) = config
            .local_fs
            .as_ref()
            .map(|value| {
                let permissions =
                    if value.permissions.starts_with('0') && value.permissions.len() > 1 {
                        u32::from_str_radix(&value.permissions, 8).unwrap_or(0o755)
                    } else {
                        0o755
                    };
                (value.base_path.as_str(), value.create_dirs, permissions)
            })
            .unwrap_or(("", true, 0o755));
        let base_path = PathBuf::from(if base.is_empty() {
            "./metering-data"
        } else {
            base
        });
        if create_dirs {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(permissions)
                .create(&base_path)
                .map_err(|error| {
                    format!(
                        "failed to create base directory {}: {error}",
                        base_path.display()
                    )
                })?;
        }
        Ok(Self {
            base_path,
            prefix: config.prefix.clone(),
            create_dirs,
            permissions,
        })
    }
    /// Go `LocalFSProvider.buildPath`.
    fn build_path(&self, path: &str) -> PathBuf {
        let path = build_path(&self.prefix, path);
        // Go filepath.Join retains the base when the later component starts '/'.
        self.base_path.join(path.trim_start_matches('/'))
    }
}
/// Go cloud providers' `buildPath` (TrimSuffix/TrimPrefix each remove one '/').
fn build_path(prefix: &str, path: &str) -> String {
    if prefix.is_empty() {
        path.into()
    } else {
        format!(
            "{}/{}",
            prefix.strip_suffix('/').unwrap_or(prefix),
            path.strip_prefix('/').unwrap_or(path)
        )
    }
}
impl ObjectStorageProvider for LocalFSProvider {
    /// Go `LocalFSProvider.Upload` (local filesystem operations do not inspect ctx).
    fn upload(&self, _ctx: &Context, path: &str, data: &[u8]) -> Result<(), String> {
        let full = self.build_path(path);
        let parent = full.parent().unwrap_or(Path::new("."));
        if self.create_dirs {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(self.permissions)
                .create(parent)
                .map_err(|error| {
                    format!("failed to create directory {}: {error}", parent.display())
                })?;
        }
        let mut file = std::fs::File::create(&full)
            .map_err(|error| format!("failed to create file {}: {error}", full.display()))?;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(self.permissions));
        file.write_all(data)
            .map_err(|error| format!("failed to write data to file {}: {error}", full.display()))
    }
    /// Go `LocalFSProvider.Download`.
    fn download(&self, _ctx: &Context, path: &str) -> Result<Vec<u8>, String> {
        let full = self.build_path(path);
        let mut file = std::fs::File::open(&full).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("file not found: {path}")
            } else {
                format!("failed to open file {}: {error}", full.display())
            }
        })?;
        let mut result = Vec::new();
        file.read_to_end(&mut result)
            .map_err(|error| error.to_string())?;
        Ok(result)
    }
    /// Go `LocalFSProvider.Delete`.
    fn delete(&self, _ctx: &Context, path: &str) -> Result<(), String> {
        let full = self.build_path(path);
        match std::fs::remove_file(&full) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("failed to delete file {}: {error}", full.display())),
        }
    }
    /// Go `LocalFSProvider.Exists`.
    fn exists(&self, _ctx: &Context, path: &str) -> Result<bool, String> {
        let full = self.build_path(path);
        match std::fs::metadata(&full) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(format!(
                "failed to check file existence {}: {error}",
                full.display()
            )),
        }
    }
    /// Go `LocalFSProvider.List`.
    fn list(&self, _ctx: &Context, prefix: &str) -> Result<Vec<String>, String> {
        fn walk(
            base: &Path,
            dir: &Path,
            prefix: &str,
            files: &mut Vec<String>,
        ) -> std::io::Result<()> {
            let mut children = std::fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
            children.sort_by_key(std::fs::DirEntry::file_name);
            for child in children {
                let path = child.path();
                if child.file_type()?.is_dir() {
                    walk(base, &path, prefix, files)?;
                } else {
                    let name = path
                        .strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    if name.starts_with(prefix) {
                        files.push(name);
                    }
                }
            }
            Ok(())
        }
        let expected = if prefix.is_empty() {
            self.prefix
                .strip_suffix('/')
                .unwrap_or(&self.prefix)
                .to_owned()
        } else {
            build_path(&self.prefix, prefix)
        };
        let mut files = Vec::new();
        walk(&self.base_path, &self.base_path, &expected, &mut files)
            .map_err(|error| format!("failed to list files with prefix {prefix}: {error}"))?;
        Ok(files)
    }
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("storage runtime")
});
struct CloudProvider {
    operator: OnceLock<Result<opendal::Operator, String>>,
    config: MeteringConfig,
    prefix: String,
}
impl CloudProvider {
    /// Go `S3Provider.client`: AWS defers endpoint/region validation until an operation.
    fn operator(&self) -> Result<&opendal::Operator, String> {
        self.operator
            .get_or_init(|| make_cloud_operator(&self.config))
            .as_ref()
            .map_err(Clone::clone)
    }
}

/// Go SDK context-bound cloud operation. Drop the native request future on cancellation.
fn run<T>(
    ctx: &Context,
    future: impl std::future::Future<Output = opendal::Result<T>>,
) -> Result<T, String> {
    if let Some(error) = ctx.err() {
        return Err(error.to_string());
    }
    RUNTIME.block_on(async {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result.map_err(|error| format!("object storage: {}", error.kind())),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                    if let Some(error) = ctx.err() {return Err(error.to_string());}
                }
            }
        }
    })
}
impl ObjectStorageProvider for CloudProvider {
    /// Go `S3Provider.Upload`, `OSSProvider.Upload`, `AzureProvider.Upload`.
    fn upload(&self, ctx: &Context, path: &str, data: &[u8]) -> Result<(), String> {
        run(
            ctx,
            self.operator()?
                .write(&build_path(&self.prefix, path), data.to_vec()),
        )
        .map(|_| ())
    }
    /// Go cloud providers' `Download`.
    fn download(&self, ctx: &Context, path: &str) -> Result<Vec<u8>, String> {
        run(ctx, self.operator()?.read(&build_path(&self.prefix, path))).map(|data| data.to_vec())
    }
    /// Go cloud providers' `Delete`.
    fn delete(&self, ctx: &Context, path: &str) -> Result<(), String> {
        run(
            ctx,
            self.operator()?.delete(&build_path(&self.prefix, path)),
        )
    }
    /// Go cloud providers' `Exists`.
    fn exists(&self, ctx: &Context, path: &str) -> Result<bool, String> {
        run(
            ctx,
            self.operator()?.exists(&build_path(&self.prefix, path)),
        )
    }
    /// Go cloud providers' `List`: the returned names retain the configured prefix.
    fn list(&self, ctx: &Context, prefix: &str) -> Result<Vec<String>, String> {
        let full = build_path(&self.prefix, prefix);
        // List a containing directory, then apply S3/OSS/Azure's string-prefix semantics.
        let directory = full
            .rsplit_once('/')
            .map(|(dir, _)| format!("{dir}/"))
            .unwrap_or_default();
        run(
            ctx,
            self.operator()?
                .list_with(&directory)
                .recursive(true)
                .into_future(),
        )
        .map(|entries| {
            entries
                .into_iter()
                .filter(|entry| !entry.metadata().is_dir() && entry.path().starts_with(&full))
                .map(|entry| entry.path().to_owned())
                .collect()
        })
    }
}
use std::future::IntoFuture;

/// Go `NewObjectStorageProvider`: provider selection and Go constructor validation.
pub fn new_object_storage_provider(
    config: &MeteringConfig,
) -> Result<Arc<dyn ObjectStorageProvider>, String> {
    if config.tp == "localfs" {
        return Ok(Arc::new(LocalFSProvider::new(config)?));
    }
    let operator = OnceLock::new();
    if config.tp != "s3" {
        let _ = operator.set(Ok(make_cloud_operator(config)?));
    }
    Ok(Arc::new(CloudProvider {
        operator,
        config: config.clone(),
        prefix: config.prefix.clone(),
    }))
}

/// Go cloud provider constructors over the native transport.
fn make_cloud_operator(config: &MeteringConfig) -> Result<opendal::Operator, String> {
    let map_error =
        |error: opendal::Error| format!("object storage configuration: {}", error.kind());
    // Constructors initialize native HTTP clients while this runtime is entered.
    let _runtime = RUNTIME.enter();
    let operator = match config.tp.as_str() {
        "s3" => {
            let mut builder = opendal::services::S3::default().bucket(&config.bucket);
            if !config.endpoint.is_empty() {
                builder = builder.endpoint(&config.endpoint);
            }
            let credentials = config.aws.clone().unwrap_or_default();
            let (provider, region) = RUNTIME.block_on(tidb_aws_sdk::ProfileCredentials::meter(
                &config.region,
                &config.endpoint,
                &credentials.access_key,
                &credentials.secret_access_key,
                &credentials.session_token,
                &credentials.assume_role_arn,
            ))?;
            builder = builder
                .region(&region)
                .customized_credential_load(Box::new(provider));
            let endpoint_is_ip = url::Url::parse(&config.endpoint)
                .ok()
                .and_then(|url| url.host_str().map(str::to_owned))
                .is_some_and(|host| host.parse::<std::net::IpAddr>().is_ok());
            if !credentials.s3_force_path_style && !endpoint_is_ip {
                builder = builder.enable_virtual_host_style();
            }
            opendal::Operator::new(builder).map_err(map_error)?.finish()
        }
        "oss" => {
            if config.bucket.is_empty() {
                return Err("bucket name is required for OSS provider".into());
            }
            if config.region.is_empty() {
                return Err("region is required for OSS provider".into());
            }
            let endpoint = if config.endpoint.is_empty() {
                format!("https://oss-{}.aliyuncs.com", config.region)
            } else {
                config.endpoint.clone()
            };
            let credentials = config.oss.clone().unwrap_or_default();
            let provider = Arc::new(tidb_oss_sdk::credentials::Credentials::new_meter(
                &credentials.access_key,
                &credentials.secret_access_key,
                &credentials.session_token,
                &credentials.assume_role_arn,
                &config.region,
            ));
            provider.start_meter_refresh();
            let client = reqwest12::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .map_err(|_| "cannot create OSS HTTP client".to_owned())?;
            let path_style = url::Url::parse(&endpoint)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .is_some_and(|host| host.parse::<std::net::IpAddr>().is_ok());
            let transport = tidb_oss_sdk::Transport {
                client,
                credentials: provider,
                region: config.region.clone(),
                bucket: config.bucket.clone(),
                path_style,
            };
            let mut builder = opendal::services::Oss::default()
                .allow_anonymous()
                .bucket(&config.bucket)
                .endpoint(&endpoint)
                .http_client(opendal::raw::HttpClient::with(transport));
            if path_style {
                builder = builder.addressing_style("path");
            }
            opendal::Operator::new(builder).map_err(map_error)?.finish()
        }
        "azure" => {
            if config.bucket.is_empty() {
                return Err("container name is required for Azure provider".into());
            }
            let credentials = config.azure.clone().unwrap_or_default();
            if config.endpoint.is_empty() && credentials.account_name.is_empty() {
                return Err("azure account name or endpoint is required".into());
            }
            if !credentials.account_key.is_empty() && credentials.account_name.is_empty() {
                return Err("azure account name is required when account key is set".into());
            }
            let endpoint = if config.endpoint.is_empty() {
                format!("https://{}.blob.core.windows.net", credentials.account_name)
            } else {
                config
                    .endpoint
                    .strip_suffix('/')
                    .unwrap_or(&config.endpoint)
                    .into()
            };
            let mut credential_config = reqsign::AzureStorageConfig::default();
            if !credentials.account_key.is_empty() {
                credential_config.account_name = Some(credentials.account_name.clone());
                credential_config.account_key = Some(credentials.account_key.clone());
            } else if !credentials.sas_token.is_empty() {
                credential_config.sas_token = Some(
                    credentials
                        .sas_token
                        .strip_prefix('?')
                        .unwrap_or(&credentials.sas_token)
                        .to_owned(),
                );
            }
            let sas = credential_config.sas_token.clone().unwrap_or_default();
            let provider = Arc::new(tidb_azure_sdk::Credentials::new(credential_config, false)?);
            let transport = tidb_azure_sdk::Transport {
                client: reqwest12::Client::new(),
                credentials: provider,
            };
            use opendal::Configurator;
            let builder = opendal::services::AzblobConfig {
                sas_token: Some(sas),
                ..Default::default()
            }
            .into_builder()
            .container(&config.bucket)
            .endpoint(&endpoint)
            .http_client(opendal::raw::HttpClient::with(transport));
            opendal::Operator::new(builder).map_err(map_error)?.finish()
        }
        "gcs" => return Err("provider GCS  not implemented yet".into()),
        tp => return Err(format!("unsupported provider type: {tp}")),
    };
    Ok(operator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MeteringAzureConfig, MeteringLocalFSConfig};
    /// Go `NewLocalFSProvider`: temporary-directory test fixture.
    fn fixture() -> (tempfile::TempDir, MeteringConfig) {
        let root = tempfile::tempdir().unwrap();
        let config = MeteringConfig {
            tp: "localfs".into(),
            local_fs: Some(MeteringLocalFSConfig {
                base_path: root.path().to_string_lossy().into_owned(),
                create_dirs: true,
                permissions: "0755".into(),
            }),
            ..Default::default()
        };
        (root, config)
    }
    /// Go SDK `TestLocalFSProvider_Upload_Download`, `Exists`, `Delete` and `Download_NonExistentFile`.
    #[test]
    fn test_local_fs_provider_upload_download() {
        let (_root, config) = fixture();
        let p = LocalFSProvider::new(&config).unwrap();
        let ctx = Context::background();
        for path in ["file.txt", "nested/path/file.txt"] {
            assert!(!p.exists(&ctx, path).unwrap());
            p.upload(&ctx, path, b"hello world").unwrap();
            assert!(p.exists(&ctx, path).unwrap());
            assert_eq!(p.download(&ctx, path).unwrap(), b"hello world");
            p.upload(&ctx, path, b"updated").unwrap();
            assert_eq!(p.download(&ctx, path).unwrap(), b"updated");
            p.delete(&ctx, path).unwrap();
            p.delete(&ctx, path).unwrap();
            assert_eq!(
                p.download(&ctx, path).unwrap_err(),
                format!("file not found: {path}")
            );
        }
    }
    /// Go SDK `TestLocalFSProvider_WithPrefix`, `List` and `BuildPath`.
    #[test]
    fn test_local_fs_provider_with_prefix() {
        let (root, mut config) = fixture();
        config.prefix = "data/".into();
        let p = LocalFSProvider::new(&config).unwrap();
        let ctx = Context::background();
        for path in ["a.txt", "b.txt", "sub/a.txt"] {
            p.upload(&ctx, path, path.as_bytes()).unwrap();
        }
        assert_eq!(
            p.list(&ctx, "").unwrap(),
            ["data/a.txt", "data/b.txt", "data/sub/a.txt"]
        );
        assert_eq!(p.list(&ctx, "sub/a").unwrap(), ["data/sub/a.txt"]);
        assert_eq!(p.build_path("/a.txt"), root.path().join("data/a.txt"));
        assert!(p.list(&ctx, "missing").unwrap().is_empty());
    }
    /// Go SDK `TestNewLocalFSProvider` and `TestParseFileMode`.
    #[test]
    fn test_new_local_fs_provider() {
        let (root, mut config) = fixture();
        let local = config.local_fs.as_mut().unwrap();
        local.base_path = root
            .path()
            .join("not-created")
            .to_string_lossy()
            .into_owned();
        local.create_dirs = false;
        let p = LocalFSProvider::new(&config).unwrap();
        assert!(!p.base_path.exists());
        assert!(p.upload(&Context::background(), "a", b"b").is_err());
        config.local_fs.as_mut().unwrap().create_dirs = true;
        for (text, mode) in [
            ("0755", 0o755),
            ("0644", 0o644),
            ("0600", 0o600),
            ("755", 0o755),
            ("0invalid", 0o755),
            ("", 0o755),
        ] {
            config.local_fs.as_mut().unwrap().permissions = text.into();
            let p = LocalFSProvider::new(&config).unwrap();
            assert_eq!(p.permissions, mode);
        }
    }
    /// Go SDK `TestBuildAzureServiceURL`, provider validation before any request.
    #[test]
    fn test_build_azure_service_url() {
        let mut c = MeteringConfig {
            tp: "azure".into(),
            ..Default::default()
        };
        assert_eq!(
            new_object_storage_provider(&c).err().unwrap(),
            "container name is required for Azure provider"
        );
        c.bucket = "container".into();
        assert_eq!(
            new_object_storage_provider(&c).err().unwrap(),
            "azure account name or endpoint is required"
        );
        c.endpoint = "http://127.0.0.1:1/".into();
        c.azure = Some(MeteringAzureConfig {
            account_key: "key".into(),
            ..Default::default()
        });
        assert_eq!(
            new_object_storage_provider(&c).err().unwrap(),
            "azure account name is required when account key is set"
        );
    }
}

#[cfg(test)]
mod cloud_tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    /// Go `TestMeterSDKS3Readback`, captured under `zzprobe/o15_meter_sdk`.
    /// This is a signed HTTP loopback readback, not a live AWS account test.
    #[test]
    fn test_meter_sdk_s3_readback() {
        meter_cloud_readback("s3");
    }
    /// Go `zzprobe/o15_meter_sdk.TestMeterSDKOSSReadback`.
    #[test]
    fn test_meter_sdk_oss_readback() {
        meter_cloud_readback("oss");
    }
    /// Go `zzprobe/o15_meter_sdk.TestMeterSDKAzureReadback`.
    #[test]
    fn test_meter_sdk_azure_readback() {
        meter_cloud_readback("azure");
    }
    fn meter_cloud_readback(scheme: &str) {
        let scheme = scheme.to_owned();
        let wire_scheme = scheme.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut object: Option<Vec<u8>> = None;
            let mut methods = Vec::new();
            for _ in 0..6 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let path = parts[1].to_owned();
                assert_eq!(path, "/bucket/capture/object");
                let mut length = 0;
                let mut signed = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                        if name.eq_ignore_ascii_case("authorization") {
                            signed = value.trim().starts_with(match wire_scheme.as_str() {
                                "oss" => "OSS4-HMAC-SHA256 ",
                                "azure" => "SharedKey account:",
                                _ => "AWS4-HMAC-SHA256 ",
                            });
                        }
                    }
                }
                assert!(signed, "request must be signed");
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let (code, response) = match method.as_str() {
                    "PUT" => {
                        object = Some(body);
                        (200, Vec::new())
                    }
                    "GET" => (200, object.clone().unwrap()),
                    "HEAD" => (if object.is_some() { 200 } else { 404 }, Vec::new()),
                    "DELETE" => {
                        object = None;
                        (if wire_scheme == "azure" { 202 } else { 204 }, Vec::new())
                    }
                    _ => panic!("unexpected method"),
                };
                let length = if method == "HEAD" {
                    object.as_ref().map_or(0, Vec::len)
                } else {
                    response.len()
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Length: {length}\r\nLast-Modified: Tue, 01 Jan 2019 00:00:00 GMT\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                socket.write_all(&response).unwrap();
                methods.push(method);
            }
            methods
        });
        let config=crate::config::new_from_uri(&format!("{scheme}://bucket/capture?region=us-east-1&endpoint=http://{addr}&access-key=fixture&secret-access-key=fixture&force-path-style=true&account-name=account&account-key=Zml4dHVyZQ%3D%3D")).unwrap();
        let p = new_object_storage_provider(&config).unwrap();
        let ctx = Context::background();
        assert!(!p.exists(&ctx, "object").unwrap());
        p.upload(&ctx, "object", b"payload").unwrap();
        assert_eq!(p.download(&ctx, "object").unwrap(), b"payload");
        assert!(p.exists(&ctx, "object").unwrap());
        p.delete(&ctx, "object").unwrap();
        assert!(!p.exists(&ctx, "object").unwrap());
        assert_eq!(
            server.join().unwrap(),
            ["HEAD", "PUT", "GET", "HEAD", "DELETE", "HEAD"]
        );
    }
}

#[cfg(test)]
mod aws_tests;
