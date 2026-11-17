//! Go `pkg/objstore/s3like` (`store.go`) with the `s3store` client, and the
//! GCS (`gcs.go`) and Azure Blob (`azblob.go`) storages, over the
//! `opendal` object transports `tidb-planner-extstore` also uses. Keys,
//! listing, ranges and URIs follow the Go storages; the SDK clients are
//! replaced by `opendal`'s.
//!
//! Errors never carry a request URL or credential: an `opendal` failure is
//! reported as its kind, annotated with the bucket and key the way Go
//! annotates SDK errors.

use std::future::IntoFuture;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::backup::{AzureBlobStorage, Gcs, S3};

use crate::error::{berrors, check_ctx, Error, ErrorKind, Result};
use crate::gopath;
use crate::objectio::{BufferedWriter, Reader, Writer};
use crate::storeapi::{self, BucketPrefix, Options, ReaderOption, Storage, WalkOption, WriterOption};

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("object storage runtime")
});

/// An `opendal` failure without its request details.
fn native_error(error: &opendal::Error) -> Error {
    let kind = if error.kind() == opendal::ErrorKind::NotFound { ErrorKind::NotFound } else { ErrorKind::Other };
    Error::new(kind, format!("object storage: {}", error.kind()))
}

/// Runs `future` on the storage runtime, stopping when `ctx` is done.
fn run<T>(ctx: &Context, future: impl std::future::Future<Output = opendal::Result<T>>) -> Result<T> {
    check_ctx(ctx)?;
    RUNTIME.block_on(async {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result.map_err(|e| native_error(&e)),
                () = tokio::time::sleep(Duration::from_millis(20)) => check_ctx(ctx)?,
            }
        }
    })
}

/// Which Go storage a [`CloudStorage`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flavor {
    /// Go `s3like.Storage`.
    S3,
    /// Go `GCSStorage`.
    Gcs,
    /// Go `AzureBlobStorage`.
    Azure,
}

/// Go `s3like.Storage`, `GCSStorage` or `AzureBlobStorage`.
pub struct CloudStorage {
    op: opendal::Operator,
    flavor: Flavor,
    bucket: String,
    /// Go `gcs.Prefix` / `options.Prefix` as configured.
    raw_prefix: String,
    /// Go `s3like.Storage.bucketPrefix`.
    bucket_prefix: BucketPrefix,
    access_rec: Option<std::sync::Arc<tidb_objstore_recording::AccessStats>>,
    /// The provider's client for what `opendal` cannot send (a copy from
    /// another bucket or container).
    client: CloudClient,
}

/// Go `s3like.Storage.s3Cli`, `GCSStorage`'s client and
/// `AzureBlobStorage`'s container client.
enum CloudClient {
    S3(crate::s3client::S3Client),
    Gcs(crate::gcsclient::GcsClient),
    Azure(crate::azclient::AzureClient),
}

impl CloudStorage {
    /// The object key of `name`.
    fn key(&self, name: &str) -> String {
        match self.flavor {
            Flavor::S3 => self.bucket_prefix.object_key(name),
            // Go `path.Join(prefix, name)`.
            Flavor::Gcs | Flavor::Azure => {
                let joined = gopath::join(&[&self.raw_prefix, name]);
                joined.trim_start_matches('/').to_owned()
            }
        }
    }

    fn file_info(&self, name: &str) -> String {
        match self.flavor {
            Flavor::S3 => format!("file info: input.bucket='{}', input.key='{}'", self.bucket, self.key(name)),
            Flavor::Gcs => format!("file info: input.bucket='{}', input.key='{}'", self.bucket, self.key(name)),
            Flavor::Azure => format!("file info: bucket(container)='{}', key='{}'", self.bucket, self.key(name)),
        }
    }

    /// Go `GetBucketPrefix`.
    #[must_use]
    pub fn bucket_prefix(&self) -> &BucketPrefix {
        &self.bucket_prefix
    }
}

impl Storage for CloudStorage {
    fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> Result<()> {
        run(ctx, self.op.write(&self.key(name), data.to_vec()))
            .map_err(|e| e.annotate(format!("failed to write file, {}", self.file_info(name))))?;
        if let Some(rec) = &self.access_rec {
            rec.rec_write(data.len());
        }
        Ok(())
    }

    fn read_file(&self, ctx: &Context, name: &str) -> Result<Vec<u8>> {
        let data = run(ctx, self.op.read(&self.key(name)))
            .map_err(|e| e.annotate(format!("failed to read file, {}", self.file_info(name))))?
            .to_vec();
        if let Some(rec) = &self.access_rec {
            rec.rec_read(data.len());
        }
        Ok(data)
    }

    fn file_exists(&self, ctx: &Context, name: &str) -> Result<bool> {
        run(ctx, self.op.exists(&self.key(name)))
    }

    fn delete_file(&self, ctx: &Context, name: &str) -> Result<()> {
        run(ctx, self.op.delete(&self.key(name)))
            .map_err(|e| e.annotate(format!("failed to delete file, {}", self.file_info(name))))
    }

    fn delete_files(&self, ctx: &Context, names: &[String]) -> Result<()> {
        // Go s3like batches 1000 keys per DeleteObjects call.
        for batch in names.chunks(1000) {
            let keys: Vec<String> = batch.iter().map(|n| self.key(n)).collect();
            run(ctx, self.op.delete_iter(keys))?;
        }
        Ok(())
    }

    fn open(&self, ctx: &Context, path: &str, o: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        let key = self.key(path);
        let size = run(ctx, self.op.stat(&key))
            .map_err(|e| e.annotate(format!("failed to open file, {}", self.file_info(path))))?
            .content_length() as i64;
        let start = o.and_then(|o| o.start_offset).unwrap_or(0);
        let end = o.and_then(|o| o.end_offset).filter(|e| *e != 0).unwrap_or(size);
        if start > size || (end > size && size > 0) || (start > 0 && start == size && end > start) {
            return Err(berrors::annotate(
                berrors::ERR_STORAGE_UNKNOWN,
                format!("open file '{path}' failed, expected range: [{start},{end}), got: <empty>"),
            ));
        }
        Ok(Box::new(CloudReader {
            op: self.op.clone(),
            ctx: ctx.clone(),
            key,
            pos: start,
            end,
            size,
            buf: Vec::new(),
            buf_pos: 0,
            access_rec: self.access_rec.clone(),
        }))
    }

    fn walk_dir(&self, ctx: &Context, opt: Option<&WalkOption>, f: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        let default = WalkOption::default();
        let opt = opt.unwrap_or(&default);
        let (list_prefix, trim) = match self.flavor {
            Flavor::S3 => (
                self.bucket_prefix.object_key(&storeapi::Prefix::new(&opt.sub_dir).object_key(&opt.obj_prefix)),
                self.bucket_prefix.prefix_str().to_owned(),
            ),
            Flavor::Gcs | Flavor::Azure => {
                let mut prefix = gopath::join(&[&self.raw_prefix, &opt.sub_dir]);
                if prefix == "." {
                    prefix.clear();
                }
                if !prefix.is_empty() && !prefix.ends_with('/') {
                    prefix.push('/');
                }
                prefix.push_str(&opt.obj_prefix);
                (prefix.trim_start_matches('/').to_owned(), self.raw_prefix.clone())
            }
        };
        let page = if opt.list_count > 0 { opt.list_count as usize } else { 1000 };
        let start_after = match self.flavor {
            Flavor::S3 if !opt.start_after.is_empty() => Some(self.key(&opt.start_after)),
            Flavor::Gcs if !opt.start_after.is_empty() => Some(self.key(&opt.start_after)),
            _ => None,
        };
        let entries = run(ctx, async {
            let mut builder = self.op.list_with(&list_prefix).recursive(true).limit(page);
            if let Some(after) = &start_after {
                builder = builder.start_after(after);
            }
            builder.await
        })?;
        let mut keys: Vec<(String, i64)> = entries
            .into_iter()
            .filter(|e| !e.metadata().is_dir() || e.path().ends_with('/'))
            .map(|e| (e.path().to_owned(), e.metadata().content_length() as i64))
            .filter(|(path, _)| path.starts_with(&list_prefix))
            .collect();
        keys.sort();
        for (key, size) in keys {
            check_ctx(ctx)?;
            let trimmed = key.strip_prefix(&trim).unwrap_or(&key);
            let trimmed = trimmed.strip_prefix('/').unwrap_or(trimmed);
            if self.flavor == Flavor::S3 && size <= 0 && trimmed.ends_with('/') {
                continue;
            }
            if self.flavor != Flavor::Azure && !opt.start_after.is_empty() && trimmed <= opt.start_after.as_str() {
                continue;
            }
            f(trimmed, size)?;
        }
        Ok(())
    }

    fn uri(&self) -> String {
        match self.flavor {
            Flavor::S3 => format!("s3://{}/{}", self.bucket, self.bucket_prefix.prefix_str()),
            Flavor::Gcs => format!("gcs://{}/{}", self.bucket, self.raw_prefix),
            Flavor::Azure => format!("azure://{}/{}", self.bucket, self.raw_prefix),
        }
    }

    fn create(&self, ctx: &Context, path: &str, option: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        let part_size = option.map_or(0, |o| o.part_size);
        let concurrency = option.map_or(1, |o| o.concurrency.max(1));
        let chunk = if part_size > 0 { part_size as usize } else { tidb_objstore_s3like::WRITE_BUFFER_SIZE };
        let key = self.key(path);
        let writer = run(ctx, self.op.writer_with(&key).chunk(chunk).concurrent(concurrency).into_future_writer())
            .map_err(|e| e.annotate(format!("failed to create file, {}", self.file_info(path))))?;
        let inner = Box::new(CloudWriter { writer: Some(writer), ctx: ctx.clone() });
        if self.flavor == Flavor::S3 {
            Ok(Box::new(BufferedWriter::new(
                inner,
                chunk,
                crate::compressedio::CompressType::NoCompression,
                self.access_rec.clone(),
            )))
        } else {
            Ok(inner)
        }
    }

    fn rename(&self, ctx: &Context, old: &str, new: &str) -> Result<()> {
        if self.flavor == Flavor::S3 {
            let content = self.read_file(ctx, old)?;
            self.write_file(ctx, new, &content)?;
            return self.delete_file(ctx, old);
        }
        run(ctx, self.op.copy(&self.key(old), &self.key(new)))?;
        self.delete_file(ctx, old)
    }

    fn presign_file(&self, ctx: &Context, file_name: &str, expire: Duration) -> Result<String> {
        if expire.is_zero() {
            return Err(Error::other("presign expiration must be positive"));
        }
        run(ctx, self.op.presign_read(&self.key(file_name), expire)).map(|req| req.uri().to_string())
    }

    fn features(&self) -> storeapi::Features {
        match self.flavor {
            Flavor::S3 => storeapi::FEATURE_STRONG_CONSISTENCY | storeapi::FEATURE_SUPPORTS_START_AFTER,
            // Go `GCSStorage.Features` / `AzureBlobStorage.Features`.
            Flavor::Gcs => storeapi::FEATURE_STRONG_CONSISTENCY | storeapi::FEATURE_SUPPORTS_START_AFTER,
            Flavor::Azure => storeapi::FEATURE_STRONG_CONSISTENCY,
        }
    }

    /// Go's S3, GCS and Azure storages are `Copier`s.
    fn as_copier(&self) -> Option<&dyn storeapi::Copier> {
        Some(self)
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

impl storeapi::Copier for CloudStorage {
    /// Go `s3like.Storage.CopyFrom` (a server-side `CopyObject` from another
    /// S3 storage), `GCSStorage.CopyFrom` (the rewrite API) and
    /// `AzureBlobStorage.CopyFrom` (`StartCopyFromURL`, waited on), each
    /// from a storage of its own kind in any bucket or container.
    fn copy_from(&self, ctx: &Context, e: &dyn Storage, spec: &storeapi::CopySpec) -> Result<()> {
        let source = e.as_any().and_then(|a| a.downcast_ref::<CloudStorage>()).filter(|s| s.flavor == self.flavor);
        let Some(source) = source else {
            return Err(berrors::annotate(
                berrors::ERR_STORAGE_INVALID_CONFIG,
                match self.flavor {
                    Flavor::S3 => format!("CopyFrom is only supported by S3 storage, get {}", e.uri()),
                    Flavor::Gcs => format!("GCSStorage.CopyFrom supports only GCSStorage, get {}", e.uri()),
                    Flavor::Azure => format!("AzureBlobStorage.CopyFrom supports *AzureBlobStorage only, got {}", e.uri()),
                },
            ));
        };
        let (from, to) = (source.key(&spec.from), self.key(&spec.to));
        let copy = async {
            match (&self.client, &source.client) {
                (CloudClient::S3(client), _) => client
                    .copy_object(&source.bucket_prefix.bucket, &from, &to)
                    .await
                    .map_err(|e| e.annotate(format!("failed to copy object, bucket={} from={from} to={to}", self.bucket))),
                (CloudClient::Gcs(client), _) => client
                    .rewrite(&source.bucket, &from, &self.bucket, &to)
                    .await
                    .map_err(|e| e.annotate(format!("failed to copy {}/{from} to {}/{to}", source.bucket, self.bucket))),
                (CloudClient::Azure(client), CloudClient::Azure(source_client)) => {
                    let url = crate::azclient::url_of_object_by_endpoint(&source_client.endpoint, &source.bucket, &from)
                        .map_err(|e| e.annotate(format!("failed to get url of object {}", spec.from)))?;
                    client.copy_from_url(&url, &to).await
                }
                (CloudClient::Azure(_), _) => Err(Error::other("an Azure storage's source is not Azure")),
            }
        };
        check_ctx(ctx)?;
        RUNTIME.block_on(async {
            tokio::select! {
                result = copy => result,
                () = async { while check_ctx(ctx).is_ok() { tokio::time::sleep(Duration::from_millis(20)).await } } => check_ctx(ctx),
            }
        })
    }
}

/// Helper so `writer_with(..)` builders resolve to a writer future.
trait IntoFutureWriter {
    fn into_future_writer(self) -> impl std::future::Future<Output = opendal::Result<opendal::Writer>>;
}

impl<F: std::future::IntoFuture<Output = opendal::Result<opendal::Writer>>> IntoFutureWriter for F {
    fn into_future_writer(self) -> impl std::future::Future<Output = opendal::Result<opendal::Writer>> {
        self.into_future()
    }
}

/// The part size a cloud reader fetches per range request.
const READ_CHUNK: i64 = 4 * 1024 * 1024;

/// Go `s3ObjectReader`: reads `[pos, end)` of an object by range requests.
struct CloudReader {
    op: opendal::Operator,
    ctx: Context,
    key: String,
    pos: i64,
    end: i64,
    size: i64,
    buf: Vec<u8>,
    buf_pos: usize,
    access_rec: Option<std::sync::Arc<tidb_objstore_recording::AccessStats>>,
}

impl Read for CloudReader {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if self.buf_pos >= self.buf.len() {
            if self.pos >= self.end {
                return Ok(0);
            }
            let upto = (self.pos + READ_CHUNK).min(self.end);
            let range = self.pos as u64..upto as u64;
            self.buf = run(&self.ctx, self.op.read_with(&self.key).range(range).into_future()).map_err(io::Error::from)?.to_vec();
            self.buf_pos = 0;
            if self.buf.is_empty() {
                return Ok(0);
            }
        }
        let n = p.len().min(self.buf.len() - self.buf_pos);
        p[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        self.pos += n as i64;
        if let Some(rec) = &self.access_rec {
            rec.rec_read(n);
        }
        Ok(n)
    }
}

impl Seek for CloudReader {
    /// Go `s3ObjectReader.Seek`: a new position within the object.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(o) => o as i64,
            SeekFrom::Current(o) => self.pos + o,
            SeekFrom::End(o) => self.size + o,
        };
        if target < 0 {
            return Err(io::Error::other(berrors::annotate(
                berrors::ERR_INVALID_ARGUMENT,
                format!("Seek: offset '{target}' out of range."),
            )));
        }
        if target == self.pos {
            return Ok(target as u64);
        }
        let buffered_start = self.pos - self.buf_pos as i64;
        let buffered_end = buffered_start + self.buf.len() as i64;
        if target >= buffered_start && target < buffered_end {
            self.buf_pos = (target - buffered_start) as usize;
        } else {
            self.buf.clear();
            self.buf_pos = 0;
        }
        self.pos = target;
        if self.end < target {
            self.end = self.size.max(target);
        }
        Ok(target as u64)
    }
}

impl Reader for CloudReader {
    fn get_file_size(&self) -> Result<i64> {
        Ok(self.size)
    }
}

/// An `opendal` multipart writer as Go's `objectio.Writer`.
struct CloudWriter {
    writer: Option<opendal::Writer>,
    ctx: Context,
}

impl Writer for CloudWriter {
    fn write(&mut self, ctx: &Context, p: &[u8]) -> Result<usize> {
        let w = self.writer.as_mut().ok_or_else(|| Error::other("writer closed"))?;
        run(ctx, w.write(p.to_vec()))?;
        Ok(p.len())
    }

    fn close(&mut self, ctx: &Context) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            let _ = &self.ctx;
            run(ctx, w.close())?;
        }
        Ok(())
    }

    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer> {
        self
    }
}

/// Go `s3store.NewS3Storage` (and the KS3 / OSS providers, which `opendal`
/// reaches through the S3 protocol).
pub fn new_s3_storage(ctx: &Context, s3: &S3, opts: &Options) -> Result<CloudStorage> {
    check_ctx(ctx)?;
    if s3.bucket.is_empty() {
        return Err(berrors::annotate(berrors::ERR_STORAGE_INVALID_CONFIG, "bucket name is empty"));
    }
    let region = if s3.region.is_empty() { "us-east-1" } else { &s3.region };
    let mut builder = opendal::services::S3::default().bucket(&s3.bucket).region(region);
    if !s3.endpoint.is_empty() {
        builder = builder.endpoint(&s3.endpoint);
    }
    if !s3.force_path_style {
        builder = builder.enable_virtual_host_style();
    }
    let http = crate::s3client::http_client()?;
    let credentials = crate::s3client::credentials(s3, region, opts, &http)?;
    match &credentials {
        None => builder = builder.allow_anonymous().disable_config_load().disable_ec2_metadata(),
        Some(loader) => {
            builder = builder.customized_credential_load(Box::new(crate::s3client::SharedCredentials(Arc::clone(loader))));
        }
    }
    if !s3.storage_class.is_empty() {
        builder = builder.default_storage_class(&s3.storage_class);
    }
    if !s3.sse.is_empty() {
        builder = builder.server_side_encryption(&s3.sse);
    }
    if !s3.sse_kms_key_id.is_empty() {
        builder = builder.server_side_encryption_aws_kms_key_id(&s3.sse_kms_key_id);
    }
    let op = {
        let _guard = RUNTIME.enter();
        opendal::Operator::new(builder).map_err(|e| native_error(&e))?.finish()
    };
    let s3_client = crate::s3client::S3Client {
        http,
        credentials,
        region: region.to_owned(),
        endpoint: crate::s3client::bucket_endpoint(s3, region, opts),
        sse: s3.sse.clone(),
        sse_kms_key_id: s3.sse_kms_key_id.clone(),
        sse_kms_encryption_context: s3.sse_kms_encryption_context.clone(),
    };
    let storage = CloudStorage {
        op,
        flavor: Flavor::S3,
        bucket: s3.bucket.clone(),
        raw_prefix: s3.prefix.clone(),
        bucket_prefix: BucketPrefix::new(&s3.bucket, &s3.prefix),
        access_rec: opts.access_recording.clone(),
        client: CloudClient::S3(s3_client),
    };
    check_permissions(ctx, &storage, &opts.check_permissions)?;
    Ok(storage)
}

/// Go `NewGCSStorage`.
pub fn new_gcs_storage(ctx: &Context, gcs: &Gcs, opts: &Options) -> Result<CloudStorage> {
    check_ctx(ctx)?;
    let mut builder = opendal::services::Gcs::default().bucket(&gcs.bucket);
    if !gcs.endpoint.is_empty() {
        builder = builder.endpoint(&gcs.endpoint);
    }
    let http = crate::s3client::http_client()?;
    let token = crate::gcsclient::token_loader(&gcs.credentials_blob, opts.no_credentials, &http)?;
    match &token {
        Some(loader) => {
            builder = builder.customized_token_loader(Box::new(crate::gcsclient::SharedTokenLoader(Arc::clone(loader))));
        }
        None => builder = builder.allow_anonymous().disable_vm_metadata().disable_config_load(),
    }
    let gcs_client = crate::gcsclient::GcsClient {
        http,
        token,
        endpoint: if gcs.endpoint.is_empty() { "https://storage.googleapis.com".to_owned() } else { gcs.endpoint.clone() },
    };
    if !gcs.predefined_acl.is_empty() {
        builder = builder.predefined_acl(&gcs.predefined_acl);
    }
    if !gcs.storage_class.is_empty() {
        builder = builder.default_storage_class(&gcs.storage_class);
    }
    let op = {
        let _guard = RUNTIME.enter();
        opendal::Operator::new(builder).map_err(|e| native_error(&e))?.finish()
    };
    let storage = CloudStorage {
        op,
        flavor: Flavor::Gcs,
        bucket: gcs.bucket.clone(),
        raw_prefix: gcs.prefix.clone(),
        bucket_prefix: BucketPrefix::new(&gcs.bucket, &gcs.prefix),
        access_rec: opts.access_recording.clone(),
        client: CloudClient::Gcs(gcs_client),
    };
    check_permissions(ctx, &storage, &opts.check_permissions)?;
    Ok(storage)
}

/// Go `newAzureBlobStorage`.
pub fn new_azure_blob_storage(ctx: &Context, az: &AzureBlobStorage, opts: &Options) -> Result<CloudStorage> {
    check_ctx(ctx)?;
    let account = if az.account_name.is_empty() {
        std::env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default()
    } else {
        az.account_name.clone()
    };
    let endpoint = if az.endpoint.is_empty() {
        format!("https://{account}.blob.core.windows.net")
    } else {
        az.endpoint.clone()
    };
    let mut builder = opendal::services::Azblob::default().container(&az.bucket).endpoint(&endpoint).account_name(&account);
    let mut credential = reqsign::AzureStorageConfig { account_name: Some(account.clone()), ..Default::default() };
    if !az.shared_key.is_empty() {
        builder = builder.account_key(&az.shared_key);
        credential.account_key = Some(az.shared_key.clone());
    } else if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
        builder = builder.account_key(&key);
        credential.account_key = Some(key);
    }
    if !az.access_sig.is_empty() {
        builder = builder.sas_token(&az.access_sig);
        credential.sas_token = Some(az.access_sig.clone());
    }
    let azure_client = crate::azclient::AzureClient {
        http: crate::s3client::http_client()?,
        endpoint: endpoint.clone(),
        container: az.bucket.clone(),
        credential,
    };
    // Go's CPKInfo. The `opendal` client has no encryption-scope option, so
    // a configured scope is not sent.
    if let Some(key) = &az.encryption_key {
        builder = builder
            .encryption_algorithm("AES256")
            .encryption_key(&key.encryption_key)
            .encryption_key_sha256(&key.encryption_key_sha256);
    }
    let op = {
        let _guard = RUNTIME.enter();
        opendal::Operator::new(builder).map_err(|e| native_error(&e))?.finish()
    };
    let storage = CloudStorage {
        op,
        flavor: Flavor::Azure,
        bucket: az.bucket.clone(),
        raw_prefix: az.prefix.clone(),
        bucket_prefix: BucketPrefix::new(&az.bucket, &az.prefix),
        access_rec: opts.access_recording.clone(),
        client: CloudClient::Azure(azure_client),
    };
    check_permissions(ctx, &storage, &opts.check_permissions)?;
    Ok(storage)
}

/// Go `s3like.CheckPermissions` / `gcs.CheckPermissions`.
fn check_permissions(ctx: &Context, storage: &CloudStorage, perms: &[storeapi::Permission]) -> Result<()> {
    use storeapi::Permission as P;
    for perm in perms {
        let result = match perm {
            P::AccessBuckets | P::ListObjects => run(ctx, storage.op.list_with(&storage.key("")).limit(1).into_future()).map(|_| ()),
            P::GetObject => match run(ctx, storage.op.stat(&storage.key("not-exists"))) {
                Err(e) if e.is_not_found() => Ok(()),
                other => other.map(|_| ()),
            },
            P::PutObject | P::PutAndDeleteObject => {
                let key = storage.key(&format!("perm-check/{}", uuid::Uuid::new_v4()));
                run(ctx, storage.op.write(&key, b"check".to_vec()))
                    .and_then(|_| run(ctx, storage.op.delete(&key)))
            }
        };
        result.map_err(|e| {
            berrors::annotate(
                "[BR:ExternalStorage:ErrStorageInvalidPermission]external storage permission",
                format!("check permission {perm:?} failed due to {e}"),
            )
        })?;
    }
    Ok(())
}
