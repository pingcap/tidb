// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `storeapi.Storage`: the external-storage operations used by extstore's consumers.
use std::{io, path::Path, time::Duration};

/// Go `objectio.Reader` with `GetFileSize` and native Read/Seek.
pub trait Reader: io::Read + io::Seek + Send {
    /// Go `Reader.GetFileSize`.
    fn get_file_size(&self) -> io::Result<u64>;
}
impl Reader for std::fs::File {
    /// Go `localFile.GetFileSize`.
    fn get_file_size(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}
impl Reader for io::Cursor<Vec<u8>> {
    /// Go `objectio.Reader.GetFileSize` for the buffered storage reader.
    fn get_file_size(&self) -> io::Result<u64> {
        Ok(self.get_ref().len() as u64)
    }
}

/// Go `storeapi.Storage`; one interface for local and cloud-backed plan archives.
pub trait Storage: Send + Sync {
    /// Go `Storage.URI` (no credential query parameters).
    fn uri(&self) -> String;
    /// Go `Storage.WriteFile`.
    fn write_file(&self, name: &str, data: &[u8]) -> io::Result<()>;
    /// Go `Storage.ReadFile`.
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>>;
    /// Go `Storage.FileExists`.
    fn file_exists(&self, name: &str) -> io::Result<bool>;
    /// Go `Storage.DeleteFile`.
    fn delete_file(&self, name: &str) -> Result<(), String>;
    /// Go `Storage.DeleteFiles`.
    fn delete_files(&self, names: &[&str]) -> Result<(), String>;
    /// Go `Storage.Open` with default reader options.
    fn open(&self, name: &str) -> io::Result<Box<dyn Reader>>;
    /// Go `Storage.Create` with default writer options.
    fn create_writer(&self, path: &Path)
        -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>>;
    /// Go `Storage.Rename`.
    fn rename(&self, old: &str, new: &str) -> io::Result<()>;
    /// Go `Storage.WalkDir` with `WalkOption.SubDir`.
    fn walk_dir(&self, sub_dir: &str) -> io::Result<Vec<String>>;
    /// Go `Storage.PresignFile`.
    fn presign_file(&self, path: &str, expire: Duration) -> Result<String, String>;
    /// Go `Storage.Close`.
    fn close(&self);
}
impl tidb_replayer::Storage for dyn Storage {
    type Writer = Box<dyn tidb_replayer::ObjectWriter + Send>;
    /// Go `storeapi.Storage.Create`.
    fn create(&self, path: &Path) -> io::Result<Self::Writer> {
        self.create_writer(path)
    }
}
impl Storage for super::LocalStorage {
    /// Go `LocalStorage.URI`.
    fn uri(&self) -> String {
        self.uri()
    }
    /// Go `LocalStorage.WriteFile`.
    fn write_file(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.write_file(name, data)
    }
    /// Go `LocalStorage.ReadFile`.
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        self.read_file(name)
    }
    /// Go `LocalStorage.FileExists`.
    fn file_exists(&self, name: &str) -> io::Result<bool> {
        self.file_exists(name)
    }
    /// Go `LocalStorage.DeleteFile`.
    fn delete_file(&self, name: &str) -> Result<(), String> {
        self.delete_file(name)
    }
    /// Go `LocalStorage.DeleteFiles`.
    fn delete_files(&self, names: &[&str]) -> Result<(), String> {
        self.delete_files(names)
    }
    /// Go `LocalStorage.Open`.
    fn open(&self, name: &str) -> io::Result<Box<dyn Reader>> {
        self.open(name).map(|file| Box::new(file) as _)
    }
    /// Go `LocalStorage.Create`.
    fn create_writer(
        &self,
        path: &Path,
    ) -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>> {
        tidb_replayer::Storage::create(self, path).map(|writer| Box::new(writer) as _)
    }
    /// Go `LocalStorage.Rename`.
    fn rename(&self, old: &str, new: &str) -> io::Result<()> {
        self.rename(old, new)
    }
    /// Go `LocalStorage.WalkDir`.
    fn walk_dir(&self, sub_dir: &str) -> io::Result<Vec<String>> {
        self.walk_dir(sub_dir)
    }
    /// Go `LocalStorage.PresignFile`.
    fn presign_file(&self, path: &str, _expire: Duration) -> Result<String, String> {
        Ok(self.presign_file(path))
    }
    /// Go `LocalStorage.Close`.
    fn close(&self) {}
}
