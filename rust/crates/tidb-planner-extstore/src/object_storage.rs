// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `GCSStorage` and `AzureBlobStorage` operations over native object transports.
use super::{cloud::run, Storage};
use std::future::IntoFuture;
use std::{io, path::Path, time::Duration};
pub(super) type Presigner =
    std::sync::Arc<dyn Fn(&str, Duration) -> Result<String, String> + Send + Sync>;
pub(super) struct ObjectStorage {
    pub credentials: Option<std::sync::Arc<tidb_oss_sdk::credentials::Credentials>>,
    pub presigner: Option<Presigner>,
    pub operator: opendal::Operator,
    pub scheme: &'static str,
    pub bucket: String,
    pub prefix: String,
}
impl ObjectStorage {
    /// Go `GCSStorage.objectName` / `AzureBlobStorage.withPrefix`.
    fn key(&self, name: &str) -> String {
        if self.scheme == "s3" {
            return format!("{}{name}", self.prefix);
        }
        super::clean_path(
            Path::new(&self.prefix)
                .join(name.trim_start_matches('/'))
                .as_path(),
        )
        .to_string_lossy()
        .into_owned()
    }
}
struct Writer(opendal::Writer);
impl tidb_replayer::ObjectWriter for Writer {
    /// Go `gcsWriter.Write` / `azblobUploader.Write`.
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        run(self.0.write(data.to_vec())).map(|()| data.len())
    }
    /// Go `gcsWriter.Close` / `azblobUploader.Close`.
    fn close(&mut self) -> io::Result<()> {
        run(self.0.close()).map(|_| ())
    }
}
impl Storage for ObjectStorage {
    /// Go `GCSStorage.URI` / `AzureBlobStorage.URI`.
    fn uri(&self) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.prefix)
    }
    /// Go `GCSStorage.WriteFile` / `AzureBlobStorage.WriteFile`.
    fn write_file(&self, name: &str, data: &[u8]) -> io::Result<()> {
        run(self.operator.write(&self.key(name), data.to_vec())).map(|_| ())
    }
    /// Go `GCSStorage.ReadFile` / `AzureBlobStorage.ReadFile`.
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        run(self.operator.read(&self.key(name))).map(|data| data.to_vec())
    }
    /// Go `GCSStorage.FileExists` / `AzureBlobStorage.FileExists`.
    fn file_exists(&self, name: &str) -> io::Result<bool> {
        run(self.operator.exists(&self.key(name)))
    }
    /// Go `GCSStorage.DeleteFile` / `AzureBlobStorage.DeleteFile`.
    fn delete_file(&self, name: &str) -> Result<(), String> {
        run(self.operator.delete(&self.key(name))).map_err(|e| e.to_string())
    }
    /// Go `GCSStorage.DeleteFiles` / `AzureBlobStorage.DeleteFiles`.
    fn delete_files(&self, names: &[&str]) -> Result<(), String> {
        for name in names {
            self.delete_file(name)?;
        }
        Ok(())
    }
    /// Go `GCSStorage.Open` / `AzureBlobStorage.Open` with default options.
    fn open(&self, name: &str) -> io::Result<Box<dyn super::storage::Reader>> {
        self.read_file(name)
            .map(|data| Box::new(io::Cursor::new(data)) as _)
    }
    /// Go `GCSStorage.Create` / `AzureBlobStorage.Create` with default options.
    fn create_writer(
        &self,
        path: &Path,
    ) -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>> {
        run(self.operator.writer(&self.key(&path.to_string_lossy())))
            .map(|writer| Box::new(Writer(writer)) as _)
    }
    /// Go `GCSStorage.Rename` / `AzureBlobStorage.Rename`.
    fn rename(&self, old: &str, new: &str) -> io::Result<()> {
        let bytes = self.read_file(old)?;
        self.write_file(new, &bytes)?;
        self.delete_file(old).map_err(io::Error::other)
    }
    /// Go `GCSStorage.WalkDir` / `AzureBlobStorage.WalkDir` with SubDir.
    fn walk_dir(&self, sub_dir: &str) -> io::Result<Vec<String>> {
        let root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix.trim_end_matches('/'))
        };
        let dir = if sub_dir.is_empty() {
            root.clone()
        } else {
            format!("{}/", self.key(sub_dir).trim_end_matches('/'))
        };
        run(self.operator.list_with(&dir).recursive(true).into_future()).map(|entries| {
            entries
                .into_iter()
                .filter(|e| !e.metadata().is_dir())
                .map(|e| e.path().strip_prefix(&root).unwrap_or(e.path()).to_owned())
                .collect()
        })
    }
    /// Go `GCSStorage.PresignFile` / `AzureBlobStorage.PresignFile`.
    fn presign_file(&self, path: &str, expire: Duration) -> Result<String, String> {
        if let Some(presigner) = &self.presigner {
            return presigner(&self.key(path), expire);
        }
        if self.scheme == "azure" {
            return Err("AzureBlobStorage backend does not support PresignFile".into());
        }
        run(self.operator.presign_read(&self.key(path), expire))
            .map(|request| request.uri().to_string())
            .map_err(|e| e.to_string())
    }
    /// Go `GCSStorage.Close` / `AzureBlobStorage.Close`.
    fn close(&self) {
        if let Some(credentials) = &self.credentials {
            super::cloud::RUNTIME.block_on(credentials.close());
        }
    }
}

impl Drop for ObjectStorage {
    /// Go `OSSStorage.Close`: stop the credential worker if the owner is dropped.
    fn drop(&mut self) {
        if let Some(credentials) = &self.credentials {
            credentials.abort_refresh();
        }
    }
}
