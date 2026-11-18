//! Go `pkg/objstore/storage.go`: a storage for a `backuppb` backend.

use std::io::Read;
use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_proto::backup::{storage_backend::Backend, StorageBackend};

use crate::error::{berrors, Result};
use crate::storeapi::{Options, ReaderOption, Storage};

/// Go `Create`.
pub fn create(ctx: &Context, backend: &StorageBackend, send_creds: bool) -> Result<Arc<dyn Storage>> {
    new(ctx, backend, Some(&Options { send_credentials: send_creds, ..Options::default() }))
}

/// Go `NewWithDefaultOpt`.
pub fn new_with_default_opt(ctx: &Context, backend: &StorageBackend) -> Result<Arc<dyn Storage>> {
    new(ctx, backend, None)
}

/// Go `NewFromURL`.
pub fn new_from_url(ctx: &Context, uri: &str) -> Result<Arc<dyn Storage>> {
    if uri.is_empty() {
        return Err(berrors::annotate(berrors::ERR_STORAGE_INVALID_CONFIG, "empty store is not allowed"));
    }
    let u = crate::parse_raw_url(uri).map_err(crate::Error::other)?;
    if u.scheme == b"memstore" {
        return Ok(Arc::new(crate::memstore::MemStorage::new()));
    }
    let backend = crate::parse_backend(uri, None).map_err(crate::Error::other)?;
    new_with_default_opt(ctx, &backend)
}

/// Go `New`.
pub fn new(ctx: &Context, backend: &StorageBackend, opts: Option<&Options>) -> Result<Arc<dyn Storage>> {
    let default = Options::default();
    let opts = opts.unwrap_or(&default);
    match &backend.backend {
        Some(Backend::Local(local)) => Ok(Arc::new(crate::local::LocalStorage::new(&local.path)?)),
        Some(Backend::Hdfs(hdfs)) => Ok(Arc::new(crate::hdfs::HdfsStorage::new(&hdfs.remote))),
        // Go: the KS3 SDK provider is `s3store.NewKS3Storage` (inside
        // `new_s3_storage`), the OSS provider `ossstore.NewOSSStorage`.
        Some(Backend::S3(s3)) if s3.provider == tidb_objstore_s3like::OSS_PROVIDER => {
            Ok(Arc::new(crate::ossstore::new_oss_storage(ctx, s3, opts)?))
        }
        Some(Backend::S3(s3)) => Ok(Arc::new(crate::cloud::new_s3_storage(ctx, s3, opts)?)),
        Some(Backend::Noop(_)) => Ok(Arc::new(crate::noop::NoopStorage)),
        Some(Backend::Gcs(gcs)) => Ok(Arc::new(crate::cloud::new_gcs_storage(ctx, gcs, opts)?)),
        Some(Backend::AzureBlobStorage(az)) => Ok(Arc::new(crate::cloud::new_azure_blob_storage(ctx, az, opts)?)),
        Some(Backend::CloudDynamic(_)) => Err(berrors::annotate(
            berrors::ERR_STORAGE_INVALID_CONFIG,
            "storage *backuppb.StorageBackend_CloudDynamic is not supported yet",
        )),
        None => Err(berrors::annotate(
            berrors::ERR_STORAGE_INVALID_CONFIG,
            "storage <nil> is not supported yet",
        )),
    }
}

/// Go `ReadDataInRange`: reads `p.len()` bytes at `start`.
pub fn read_data_in_range(ctx: &Context, storage: &dyn Storage, name: &str, start: i64, p: &mut [u8]) -> Result<usize> {
    if start < 0 {
        return Err(berrors::annotate(
            berrors::ERR_INVALID_ARGUMENT,
            format!("invalid negative start offset: {start}"),
        ));
    }
    let end = start
        .checked_add(p.len() as i64)
        .ok_or_else(|| berrors::annotate(berrors::ERR_INVALID_ARGUMENT, format!("range calculation overflow: start={start}, len={}", p.len())))?;
    let mut rd = storage.open(ctx, name, Some(&ReaderOption { start_offset: Some(start), end_offset: Some(end), prefetch_size: 0 }))?;
    let mut n = 0;
    while n < p.len() {
        match rd.read(&mut p[n..]) {
            Ok(0) => break,
            Ok(m) => n += m,
            Err(e) => {
                let _ = rd.close();
                return Err(e.into());
            }
        }
    }
    let _ = rd.close();
    if n < p.len() {
        return Err(crate::Error::other(if n == 0 { "EOF" } else { "unexpected EOF" }));
    }
    Ok(n)
}

/// Go `ValidateCloudStorageURI` (`helper.go`), which validates
/// `tidb_cloud_storage_uri`: the storage opens and may list, read and reach
/// its bucket.
pub fn validate_cloud_storage_uri(ctx: &Context, uri: &str) -> Result<()> {
    let backend = crate::parse_backend(uri, None).map_err(crate::Error::other)?;
    let storage = new(
        ctx,
        &backend,
        Some(&Options {
            check_permissions: vec![
                crate::storeapi::Permission::ListObjects,
                crate::storeapi::Permission::GetObject,
                crate::storeapi::Permission::AccessBuckets,
            ],
            ..Options::default()
        }),
    )?;
    storage.close();
    Ok(())
}
