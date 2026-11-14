// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `objstore/s3store` and `s3like.Storage` as used by extstore.
use super::Storage;
use std::future::IntoFuture;
use std::{
    io,
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};
pub(super) static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("external storage runtime")
});
/// Go `ast.RedactURL`: native transport errors expose only their class.
pub(super) fn native_error(error: opendal::Error) -> io::Error {
    // Native transport errors may contain signed URLs or credentials. Expose the
    // classified failure without those request details.
    let kind = if error.kind() == opendal::ErrorKind::NotFound {
        io::ErrorKind::NotFound
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, format!("object storage: {}", error.kind()))
}
/// Go `s3like.Storage` synchronous operations over the native async transport.
pub(super) fn run<T>(
    future: impl std::future::Future<Output = opendal::Result<T>>,
) -> io::Result<T> {
    RUNTIME.block_on(future).map_err(native_error)
}

/// Go SDK requests take the constructor or operation's context. Dropping the
/// request future closes an in-flight request when the caller cancels.
pub(super) fn run_with_context<T>(
    context: Option<&tidb_gocontext::Context>,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    RUNTIME.block_on(async {
        tokio::pin!(future);
        loop {
            if let Some(error) = context.and_then(tidb_gocontext::Context::err) {
                return Err(error.to_string());
            }
            tokio::select! {
                result = &mut future => return match context.and_then(tidb_gocontext::Context::err) {
                    Some(error) => Err(error.to_string()),
                    None => result,
                },
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
}

/// Go `objstore.ExtractQueryParameters`.
pub(super) fn query_options(url: &url::Url) -> std::collections::HashMap<String, String> {
    let mut options = std::collections::HashMap::new();
    for (key, value) in url.query_pairs() {
        options
            .entry(key.to_lowercase().replace('_', "-"))
            .or_insert_with(|| value.into_owned());
    }
    options
}

/// Go `s3like.Storage`, with a native S3 transport and Go's bucket prefix.
pub struct S3Storage {
    operator: opendal::Operator,
    bucket: String,
    prefix: String,
    credentials: Arc<dyn reqsign::AwsCredentialLoad>,
    region: String,
    client: reqwest12::Client,
    ks3: bool,
    path_style: bool,
}
#[derive(Clone)]
struct SharedCredentials(Arc<dyn reqsign::AwsCredentialLoad>);
#[async_trait::async_trait]
impl reqsign::AwsCredentialLoad for SharedCredentials {
    /// Go `aws.CredentialsProvider.Retrieve`: share the storage provider cache.
    async fn load_credential(
        &self,
        client: reqwest12::Client,
    ) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        self.0.load_credential(client).await
    }
}
impl S3Storage {
    /// Go `NewS3Storage` and `S3BackendOptions.Apply`.
    pub fn new(url: &url::Url, prefix: &str) -> Result<Arc<Self>, String> {
        Self::new_with_context(url, prefix, None)
    }
    /// Go `NewS3Storage` with the caller's constructor context.
    pub(super) fn new_with_context(
        url: &url::Url,
        prefix: &str,
        context: Option<&tidb_gocontext::Context>,
    ) -> Result<Arc<Self>, String> {
        let bucket = url.host_str().unwrap_or("").to_owned();
        if bucket.is_empty() {
            return Err("please specify the bucket for s3: [BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config".into());
        }
        let mut options = std::collections::HashMap::new();
        for (key, value) in url.query_pairs() {
            options
                .entry(key.to_lowercase().replace('_', "-"))
                .or_insert_with(|| value.into_owned());
        }
        let get = |key: &str| options.get(key).map_or("", String::as_str);
        let endpoint = get("endpoint");
        if !endpoint.is_empty() {
            let parsed =
                url::Url::parse(endpoint).map_err(|_| "scheme not found in endpoint".to_owned())?;
            if parsed.host_str().is_none() {
                return Err("host not found in endpoint".into());
            }
        }
        if get("profile").is_empty() {
            if get("access-key").is_empty() && !get("secret-access-key").is_empty() {
                return Err("access_key not found: [BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config".into());
            }
            if !get("access-key").is_empty() && get("secret-access-key").is_empty() {
                return Err("secret_access_key not found: [BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config".into());
            }
        }
        let ks3 = url.scheme() == "ks3" || get("provider") == "ks3-sdk";
        if ks3 && get("region").is_empty() {
            return Err("ks3 region is empty".into());
        }
        if ks3 && !get("role-arn").is_empty() {
            return Err("ks3 does not support role arn".into());
        }
        let boolean = |key, default| match get(key) {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => true,
            "0" | "f" | "F" | "false" | "FALSE" | "False" => false,
            _ => default,
        };
        let provider = get("provider");
        let mut path_style = boolean("force-path-style", true);
        let explicit_style =
            url.as_str().contains("force-path-style") || url.as_str().contains("force_path_style");
        if matches!(provider, "alibaba" | "netease" | "tencent")
            || boolean("use-accelerate-endpoint", false)
            || (!explicit_style
                && (provider == "aws"
                    || endpoint.contains("amazonaws.com")
                    || !get("role-arn").is_empty()))
        {
            path_style = false;
        }
        let client = reqwest12::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|_| "cannot create S3 HTTP client".to_owned())?;
        let mut credential_config = reqsign::AwsConfig::default().from_profile().from_env();
        if get("profile").is_empty() && !get("access-key").is_empty() {
            credential_config.access_key_id = Some(get("access-key").to_owned());
            credential_config.secret_access_key = Some(get("secret-access-key").to_owned());
            credential_config.session_token =
                (!get("session-token").is_empty()).then(|| get("session-token").to_owned());
        }
        let mut credentials: Arc<dyn reqsign::AwsCredentialLoad> = if get("profile").is_empty() {
            if !ks3 && get("access-key").is_empty() {
                Arc::new(
                    RUNTIME.block_on(tidb_aws_sdk::ProfileCredentials::default_chain(
                        if get("region").is_empty() {
                            "us-east-1"
                        } else {
                            get("region")
                        },
                    )),
                )
            } else {
                Arc::new(reqsign::AwsDefaultLoader::new(
                    client.clone(),
                    credential_config.clone(),
                ))
            }
        } else {
            Arc::new(tidb_aws_sdk::ProfileCredentials::new(
                get("profile"),
                if get("region").is_empty() {
                    "us-east-1"
                } else {
                    get("region")
                },
            ))
        };
        if !get("role-arn").is_empty() {
            credential_config.role_arn = Some(get("role-arn").to_owned());
            credential_config.external_id =
                (!get("external-id").is_empty()).then(|| get("external-id").to_owned());
            credential_config.region = Some(
                if get("region").is_empty() {
                    "us-east-1"
                } else {
                    get("region")
                }
                .to_owned(),
            );
            credentials = Arc::new(
                reqsign::AwsAssumeRoleLoader::new(
                    client.clone(),
                    credential_config,
                    Box::new(SharedCredentials(credentials)),
                )
                .map_err(|_| "cannot initialize S3 assume-role credentials".to_owned())?,
            );
        }
        let build = |region: &str| -> Result<opendal::Operator, String> {
            let _guard = RUNTIME.enter();
            let transport = super::s3_transport::Transport {
                client: client.clone(),
                credentials: credentials.clone(),
                region: region.to_owned(),
                bucket: bucket.clone(),
                path_style,
                ks3,
                acl: get("acl").to_owned(),
            };
            let mut builder = opendal::services::S3::default()
                .bucket(&bucket)
                .region(region)
                .customized_credential_load(Box::new(SharedCredentials(credentials.clone())))
                .http_client(opendal::raw::HttpClient::with(transport));
            if ks3 {
                builder = builder.disable_list_objects_v2();
            }
            if !endpoint.is_empty() {
                builder = builder.endpoint(endpoint);
            }
            if !path_style {
                builder = builder.enable_virtual_host_style();
            }
            if !get("access-key").is_empty() {
                builder = builder
                    .access_key_id(get("access-key"))
                    .secret_access_key(get("secret-access-key"))
                    .session_token(get("session-token"));
            }
            if !get("storage-class").is_empty() {
                builder = builder.default_storage_class(get("storage-class"));
            }
            if !get("sse").is_empty() {
                builder = builder.server_side_encryption(get("sse"));
            }
            if !get("sse-kms-key-id").is_empty() {
                builder = builder.server_side_encryption_aws_kms_key_id(get("sse-kms-key-id"));
            }
            opendal::Operator::new(builder)
                .map(|builder| builder.finish())
                .map_err(|error| native_error(error).to_string())
        };
        let region = if get("region").is_empty() {
            "us-east-1"
        } else {
            get("region")
        };
        let mut operator = build(region)?;
        let mut resolved_region = region.to_owned();
        if !ks3 && (provider.is_empty() || provider == "aws") {
            // Go manager.GetBucketRegion: a HEAD bucket request, with credentials
            // retained for compatible endpoints. The URL is never included in errors.
            let response = run_with_context(context, async {
                let signed = operator
                    .presign_stat("", Duration::from_secs(60))
                    .await
                    .map_err(|error| native_error(error).to_string())?;
                let operation = async {
                    let mut bucket_url =
                        url::Url::parse(&signed.uri().to_string()).map_err(|_| ())?;
                    bucket_url.set_query(None);
                    let credential = credentials
                        .load_credential(client.clone())
                        .await
                        .map_err(|_| ())?
                        .ok_or(())?;
                    let mut request = client.head(bucket_url).build().map_err(|_| ())?;
                    reqsign::AwsV4Signer::new("s3", region)
                        .sign(&mut request, &credential)
                        .map_err(|_| ())?;
                    client.execute(request).await.map_err(|error| {
                        #[cfg(test)]
                        eprintln!("region lookup transport: {:?}", error.without_url());
                        ()
                    })
                };
                operation
                    .await
                    .map_err(|_| format!("failed to get region of bucket {bucket}"))
            })?;
            if !response.status().is_success() {
                return Err(format!(
                    "failed to get region of bucket {bucket}: HTTP {}",
                    response.status().as_u16()
                ));
            }
            let detected = response
                .headers()
                .get("x-amz-bucket-region")
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty())
                .unwrap_or("us-east-1");
            if !get("region").is_empty() && get("region") != detected {
                return Err(format!(
                    "s3 bucket and region are not matched, bucket={bucket}, input region={}, real region={detected}",
                    get("region")
                ));
            }
            if region != detected {
                operator = build(detected)?;
            }
            resolved_region = detected.to_owned();
        }
        let prefix = prefix.trim_matches('/');
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        Ok(Arc::new(Self {
            operator,
            bucket,
            prefix,
            credentials,
            region: resolved_region,
            client,
            ks3,
            path_style,
        }))
    }
    /// Go `s3like.CheckPermissions`: ListObjects, GetObject, then AccessBucket.
    pub fn check_permissions(&self, ctx: &tidb_gocontext::Context) -> Result<(), String> {
        let operation = async {
            let signed = self
                .operator
                .presign_stat("", Duration::from_secs(60))
                .await
                .map_err(|e| native_error(e).to_string())?;
            let mut bucket_url = url::Url::parse(&signed.uri().to_string())
                .map_err(|_| "invalid S3 bucket URI".to_owned())?;
            bucket_url.set_query(None);
            let credentials = self
                .credentials
                .load_credential(self.client.clone())
                .await
                .map_err(|_| "cannot load S3 credentials".to_owned())?
                .ok_or_else(|| "S3 credentials not found".to_owned())?;
            let signer = reqsign::AwsV4Signer::new("s3", &self.region);
            for permission in ["ListObjects", "GetObject", "AccessBucket"] {
                let mut url = bucket_url.clone();
                let method = if permission == "AccessBucket" {
                    reqwest12::Method::HEAD
                } else {
                    reqwest12::Method::GET
                };
                if permission == "ListObjects" {
                    if !self.ks3 {
                        url.query_pairs_mut().append_pair("list-type", "2");
                    }
                    url.query_pairs_mut()
                        .append_pair("max-keys", "1")
                        .append_pair("prefix", &self.prefix);
                } else if permission == "GetObject" {
                    url.set_path(&format!(
                        "{}/{}perm-check/{}",
                        url.path().trim_end_matches('/'),
                        self.prefix,
                        uuid::Uuid::new_v4()
                    ));
                }
                let mut request = self
                    .client
                    .request(method, url)
                    .build()
                    .map_err(|_| format!("permission {permission}: invalid request"))?;
                if self.ks3 {
                    let url = request.url().clone();
                    let method = request.method().as_str().to_owned();
                    super::s3_transport::sign_v2(
                        &method,
                        &url,
                        request.headers_mut(),
                        &self.bucket,
                        self.path_style,
                        &credentials,
                        chrono::Utc::now(),
                    )
                    .map_err(|_| format!("permission {permission}: signing failed"))?;
                } else {
                    signer
                        .sign(&mut request, &credentials)
                        .map_err(|_| format!("permission {permission}: signing failed"))?;
                }
                let response = self
                    .client
                    .execute(request)
                    .await
                    .map_err(|_| format!("permission {permission}: HTTP request failed"))?;
                let status = response.status();
                if status.is_success() {
                    continue;
                }
                if permission == "GetObject" {
                    #[derive(serde::Deserialize)]
                    struct S3Error {
                        #[serde(rename = "Code")]
                        code: String,
                    }
                    let body = response
                        .bytes()
                        .await
                        .map_err(|_| format!("permission {permission}: HTTP response failed"))?;
                    if quick_xml::de::from_reader::<_, S3Error>(body.as_ref())
                        .is_ok_and(|error| error.code == "NoSuchKey")
                    {
                        continue;
                    }
                }
                return Err(format!("permission {permission}: HTTP {}", status.as_u16()));
            }
            Ok(())
        };
        RUNTIME.block_on(async {
            tokio::pin!(operation);
            loop {
                if let Some(error)=ctx.err() {return Err(error.to_string());}
                tokio::select! { result=&mut operation => return result, _=tokio::time::sleep(Duration::from_millis(10))=>{} }
            }
        })
    }
    /// Go `BucketPrefix.ObjectKey`.
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}
struct S3Writer(opendal::Writer);
impl tidb_replayer::ObjectWriter for S3Writer {
    /// Go `s3Writer.Write`.
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        run(self.0.write(data.to_vec())).map(|()| data.len())
    }
    /// Go `s3Writer.Close`.
    fn close(&mut self) -> io::Result<()> {
        run(self.0.close()).map(|_| ())
    }
}
impl Storage for S3Storage {
    /// Go `s3like.Storage.URI`.
    fn uri(&self) -> String {
        format!(
            "{}://{}/{}",
            if self.ks3 { "ks3" } else { "s3" },
            self.bucket,
            self.prefix
        )
    }
    /// Go `s3like.Storage.WriteFile`.
    fn write_file(&self, name: &str, data: &[u8]) -> io::Result<()> {
        run(self.operator.write(&self.key(name), data.to_vec())).map(|_| ())
    }
    /// Go `s3like.Storage.ReadFile`.
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        run(self.operator.read(&self.key(name))).map(|data| data.to_vec())
    }
    /// Go `s3like.Storage.FileExists`.
    fn file_exists(&self, name: &str) -> io::Result<bool> {
        run(self.operator.exists(&self.key(name)))
    }
    /// Go `s3like.Storage.DeleteFile`.
    fn delete_file(&self, name: &str) -> Result<(), String> {
        run(self.operator.delete(&self.key(name))).map_err(|error| error.to_string())
    }
    /// Go `s3like.Storage.DeleteFiles`.
    fn delete_files(&self, names: &[&str]) -> Result<(), String> {
        for name in names {
            self.delete_file(name)?;
        }
        Ok(())
    }
    /// Go `s3like.Storage.Open`.
    fn open(&self, name: &str) -> io::Result<Box<dyn super::storage::Reader>> {
        self.read_file(name)
            .map(|data| Box::new(io::Cursor::new(data)) as _)
    }
    /// Go `s3like.Storage.Create`.
    fn create_writer(
        &self,
        path: &Path,
    ) -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>> {
        run(self.operator.writer(&self.key(&path.to_string_lossy())))
            .map(|writer| Box::new(S3Writer(writer)) as _)
    }
    /// Go `s3like.Storage.Rename`.
    fn rename(&self, old: &str, new: &str) -> io::Result<()> {
        let bytes = self.read_file(old)?;
        self.write_file(new, &bytes)?;
        self.delete_file(old).map_err(io::Error::other)
    }
    /// Go `s3like.Storage.WalkDir`.
    fn walk_dir(&self, sub_dir: &str) -> io::Result<Vec<String>> {
        let sub_dir = sub_dir.trim_matches('/');
        let prefix = self.key(&if sub_dir.is_empty() {
            String::new()
        } else {
            format!("{sub_dir}/")
        });
        run(self
            .operator
            .list_with(&prefix)
            .recursive(true)
            .into_future())
        .map(|entries| {
            entries
                .into_iter()
                .filter(|e| !e.metadata().is_dir())
                .map(|e| {
                    e.path()
                        .strip_prefix(&self.prefix)
                        .unwrap_or(e.path())
                        .to_owned()
                })
                .collect()
        })
    }
    /// Go `s3like.Storage.PresignFile`.
    fn presign_file(&self, path: &str, expire: Duration) -> Result<String, String> {
        if self.ks3 {
            return Err("KS3 backend does not support PresignFile".into());
        }
        if expire.is_zero() {
            return Err("presign expiration must be positive".into());
        }
        run(self.operator.presign_read(&self.key(path), expire))
            .map(|req| req.uri().to_string())
            .map_err(|error| error.to_string())
    }
    /// Go `s3like.Storage.Close`.
    fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    /// Go `zzprobe/o15_extstore.TestExtStorageExplicitProfile`.
    #[test]
    fn test_ext_storage_explicit_profile() {
        profile_fixture(false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageDefaultCredentialProcess`.
    #[test]
    fn test_ext_storage_default_credential_process() {
        profile_fixture(true);
    }
    /// Go `s3store.NewS3Storage`: profile and credential-process test fixture.
    fn profile_fixture(process: bool) {
        if std::env::var_os("O15_PROFILE_TEST_CHILD").is_none() {
            let root = std::env::temp_dir().join(format!("o15-profile-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("credentials"),"[wanted]\naws_access_key_id = wanted_fixture\naws_secret_access_key = wanted_fixture\n[ambient]\naws_access_key_id = ambient_fixture\naws_secret_access_key = ambient_fixture\n").unwrap();
            std::fs::write(root.join("process"),r#"printf '%s' '{"Version":1,"AccessKeyId":"wanted_fixture","SecretAccessKey":"wanted_fixture"}'"#).unwrap();
            std::fs::write(
                root.join("config"),
                format!(
                    "[profile process]\ncredential_process = /bin/sh {}\n",
                    root.join("process").display()
                ),
            )
            .unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    if process {
                        "cloud::tests::test_ext_storage_default_credential_process"
                    } else {
                        "cloud::tests::test_ext_storage_explicit_profile"
                    },
                    "--nocapture",
                ])
                .env("O15_PROFILE_TEST_CHILD", "1")
                .env("AWS_SHARED_CREDENTIALS_FILE", root.join("credentials"))
                .env("AWS_CONFIG_FILE", root.join("config"))
                .env("AWS_PROFILE", "ambient")
                .env("AWS_ACCESS_KEY_ID", "environment_fixture")
                .env("AWS_SECRET_ACCESS_KEY", "environment_fixture")
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env_remove("AWS_SESSION_TOKEN")
                .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
                .env_remove("AWS_ROLE_ARN");
            if process {
                command
                    .env("AWS_PROFILE", "process")
                    .env_remove("AWS_ACCESS_KEY_ID")
                    .env_remove("AWS_SECRET_ACCESS_KEY");
            }
            let result = command.status().unwrap();
            std::fs::remove_dir_all(root).unwrap();
            assert!(result.success());
            return;
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut matched = true;
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut length = 0;
                let mut explicit_profile = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("authorization") {
                            explicit_profile = value.contains("Credential=wanted_fixture/");
                        }
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                matched &= explicit_profile;
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                write!(socket,"HTTP/1.1 200 OK\r\nx-amz-bucket-region: us-east-1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
            matched
        });
        let profile = if process { "" } else { "&profile=wanted" };
        let storage = crate::new_ext_storage(
            &format!("s3://bucket/base?region=us-east-1{profile}&endpoint=http://{addr}"),
            "tenant",
        )
        .unwrap();
        storage.write_file("file", b"payload").unwrap();
        assert!(
            server.join().unwrap(),
            "explicit profile must override AWS_PROFILE and environment credentials"
        );
    }

    /// Go `zzprobe/o15_extstore.TestExtStorageNamespaceClean`.
    #[test]
    fn test_ext_storage_namespace_clean() {
        let root = std::env::temp_dir().join(format!("o15-path-{}", uuid::Uuid::new_v4()));
        for (path, namespace, expected) in [
            ("/base", "../tenant", "/tenant"),
            ("/base/./old/..", "/tenant//x/../y", "/base/tenant/y"),
            ("/base", "", "/base"),
        ] {
            let storage =
                crate::new_ext_storage(&format!("file://{}{path}", root.display()), namespace)
                    .unwrap();
            assert_eq!(
                storage.uri(),
                format!("file://{}{expected}", root.display())
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageRelativeLocal`.
    #[test]
    fn test_ext_storage_relative_local() {
        let cwd = std::env::current_dir().unwrap();
        let storage = crate::new_ext_storage("", "").unwrap();
        assert_eq!(storage.uri(), format!("file://{}", cwd.display()));
        let name = format!("o15-relative-{}", uuid::Uuid::new_v4());
        let storage = crate::new_ext_storage(&format!("{name}/base/../dir"), "tenant").unwrap();
        assert_eq!(
            storage.uri(),
            format!("file://{}/{name}/dir/tenant", cwd.display())
        );
        std::fs::remove_dir_all(cwd.join(name)).unwrap();
    }
    /// Go `TestExtStorageCloudNamespace`, captured under `zzprobe/o15_extstore`.
    #[test]
    fn test_ext_storage_cloud_namespace() {
        cloud_namespace(false, false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageKS3Namespace`.
    #[test]
    fn test_ext_storage_ks3_namespace() {
        cloud_namespace(true, false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageOSSNamespace`.
    #[test]
    fn test_ext_storage_oss_namespace() {
        cloud_namespace(false, true);
    }
    /// Go `TestExtStorage`: shared S3-compatible namespace fixture.
    fn cloud_namespace(ks3: bool, oss: bool) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut objects = std::collections::BTreeMap::<String, Vec<u8>>::new();
            for _ in 0..if oss { 9 } else { 8 } {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let uri = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                let mut length = 0;
                let mut signed = false;
                let mut acl = false;
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
                            signed = value.trim().starts_with(if ks3 {
                                "AWS fixture:"
                            } else if oss {
                                "OSS4-HMAC-SHA256 "
                            } else {
                                "AWS4-HMAC-SHA256 "
                            });
                        }
                        if name.eq_ignore_ascii_case("x-amz-acl") {
                            acl = value.trim() == "private";
                        }
                    }
                }
                assert!(signed);
                if method == "PUT" && !oss {
                    assert!(acl);
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let path = uri.path();
                if oss && uri.query_pairs().any(|(key, _)| key == "location") {
                    let body = "<LocationConstraint>oss-us-east-1</LocationConstraint>";
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                    continue;
                }
                let list = uri.query_pairs().any(|(key, _)| key == "prefix");
                if list {
                    assert_eq!(uri.query_pairs().any(|(key, _)| key == "list-type"), !ks3);
                }
                if !list {
                    assert!(path.starts_with("/bucket/base/tenant/"));
                }
                let (code, response) = match method.as_str() {
                    "PUT" => {
                        objects.insert(path.into(), body);
                        (200, Vec::new())
                    }
                    "HEAD" => (
                        if objects.contains_key(path) { 200 } else { 404 },
                        Vec::new(),
                    ),
                    "GET" if list => {
                        let mut xml =
                            "<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated>"
                                .to_owned();
                        for (name, bytes) in &objects {
                            xml.push_str(&format!("<Contents><Key>{}</Key><Size>{}</Size><LastModified>2019-01-01T00:00:00Z</LastModified></Contents>",name.strip_prefix("/bucket/").unwrap(),bytes.len()));
                        }
                        xml.push_str("</ListBucketResult>");
                        (200, xml.into_bytes())
                    }
                    "GET" => (200, objects.get(path).unwrap().clone()),
                    "DELETE" => {
                        objects.remove(path);
                        (204, Vec::new())
                    }
                    _ => panic!("unexpected method"),
                };
                let length = if method == "HEAD" {
                    objects.get(path).map_or(0, Vec::len)
                } else {
                    response.len()
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Length: {length}\r\nLast-Modified: Tue, 01 Jan 2019 00:00:00 GMT\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                socket.write_all(&response).unwrap();
            }
            assert!(objects.is_empty());
        });
        let scheme = if ks3 {
            "ks3"
        } else if oss {
            "oss"
        } else {
            "s3"
        };
        let uri = format!(
            "{scheme}://bucket/base?acl=private&provider=minio&region=us-east-1&access-key=fixture&secret-access-key=fixture&endpoint=http://{addr}"
        );
        let s = crate::new_ext_storage(&uri, "tenant").unwrap();
        assert_eq!(
            s.uri(),
            format!("{}://bucket/base/tenant/", if oss { "s3" } else { scheme })
        );
        s.write_file("file", b"payload").unwrap();
        assert_eq!(s.read_file("file").unwrap(), b"payload");
        assert!(s.file_exists("file").unwrap());
        s.rename("file", "renamed").unwrap();
        assert_eq!(s.walk_dir("").unwrap(), ["renamed"]);
        if ks3 {
            assert_eq!(
                s.presign_file("renamed", Duration::from_secs(60))
                    .unwrap_err(),
                "KS3 backend does not support PresignFile"
            );
        } else {
            let signed = s.presign_file("renamed", Duration::from_secs(60)).unwrap();
            let url = url::Url::parse(&signed).unwrap();
            assert_eq!(url.path(), "/bucket/base/tenant/renamed");
            assert!(url.query_pairs().any(|(key, _)| key
                == if oss {
                    "x-oss-signature"
                } else {
                    "X-Amz-Signature"
                }));
        }
        s.delete_file("renamed").unwrap();
        server.join().unwrap();
    }
}
