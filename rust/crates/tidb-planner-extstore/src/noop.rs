// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/objstore/noop.go`.
use super::storage::{Reader, Storage};
use std::{io, path::Path, time::Duration};

/// Go `noopStorage`.
pub struct NoopStorage;
impl Storage for NoopStorage {
    /// Go `noopStorage.URI`.
    fn uri(&self) -> String {
        "noop:///".into()
    }
    /// Go `noopStorage.WriteFile`.
    fn write_file(&self, _: &str, _: &[u8]) -> io::Result<()> {
        Ok(())
    }
    /// Go `noopStorage.ReadFile`.
    fn read_file(&self, _: &str) -> io::Result<Vec<u8>> {
        Ok(Vec::new())
    }
    /// Go `noopStorage.FileExists`.
    fn file_exists(&self, _: &str) -> io::Result<bool> {
        Ok(false)
    }
    /// Go `noopStorage.DeleteFile`.
    fn delete_file(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
    /// Go `noopStorage.DeleteFiles`.
    fn delete_files(&self, _: &[&str]) -> Result<(), String> {
        Ok(())
    }
    /// Go `noopStorage.Open`.
    fn open(&self, _: &str) -> io::Result<Box<dyn Reader>> {
        Ok(Box::new(NoopReader))
    }
    /// Go `noopStorage.Create`.
    fn create_writer(&self, _: &Path) -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>> {
        Ok(Box::new(NoopWriter))
    }
    /// Go `noopStorage.Rename`.
    fn rename(&self, _: &str, _: &str) -> io::Result<()> {
        Ok(())
    }
    /// Go `noopStorage.WalkDir`.
    fn walk_dir(&self, _: &str) -> io::Result<Vec<String>> {
        Ok(Vec::new())
    }
    /// Go `noopStorage.PresignFile`.
    fn presign_file(&self, _: &str, _: Duration) -> Result<String, String> {
        Ok(String::new())
    }
    /// Go `noopStorage.Close`.
    fn close(&self) {}
}
/// Go `noopReader`.
struct NoopReader;
impl io::Read for NoopReader {
    /// Go `noopReader.Read`.
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        Ok(buffer.len())
    }
}
impl io::Seek for NoopReader {
    /// Go `noopReader.Seek`.
    fn seek(&mut self, offset: io::SeekFrom) -> io::Result<u64> {
        Ok(match offset {
            io::SeekFrom::Start(offset) => offset,
            io::SeekFrom::End(offset) | io::SeekFrom::Current(offset) => offset as u64,
        })
    }
}
impl Reader for NoopReader {
    /// Go `noopReader.GetFileSize`.
    fn get_file_size(&self) -> io::Result<u64> {
        Ok(0)
    }
}
/// Go `NoopWriter`.
struct NoopWriter;
impl tidb_replayer::ObjectWriter for NoopWriter {
    /// Go `NoopWriter.Write`.
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        Ok(buffer.len())
    }
    /// Go `NoopWriter.Close`.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestExtStorageNoop`, captured under `zzprobe/o15_extstore`.
    #[test]
    fn test_ext_storage_noop() {
        let s = crate::new_ext_storage("noop://blackhole?access-key=fixture", "tenant").unwrap();
        assert_eq!(s.uri(), "noop:///");
        s.write_file("file", b"payload").unwrap();
        assert!(s.read_file("file").unwrap().is_empty());
        assert!(!s.file_exists("file").unwrap());
        let mut reader = s.open("file").unwrap();
        let mut bytes = *b"unchanged";
        assert_eq!(reader.read(&mut bytes).unwrap(), 9);
        assert_eq!(&bytes, b"unchanged");
        assert_eq!(reader.seek(io::SeekFrom::End(7)).unwrap(), 7);
        assert_eq!(reader.get_file_size().unwrap(), 0);
        let mut writer = s.create_writer(Path::new("file")).unwrap();
        assert_eq!(writer.write(&bytes).unwrap(), 9);
        writer.close().unwrap();
        s.rename("file", "new").unwrap();
        s.delete_file("missing").unwrap();
        s.delete_files(&["missing", "new"]).unwrap();
        assert!(s.walk_dir("").unwrap().is_empty());
        assert_eq!(s.presign_file("file", Duration::from_secs(60)).unwrap(), "");
        s.close();
    }
}
