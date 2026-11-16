//! Go `pkg/objstore/noop.go`: a storage that stores nothing.

use std::io::{self, Read, Seek, SeekFrom};
use std::time::Duration;

use tidb_gocontext::Context;

use crate::error::Result;
use crate::objectio::{Reader, Writer};
use crate::storeapi::{ReaderOption, Storage, WalkOption, WriterOption};

/// Go `noopStorage`.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopStorage;

impl Storage for NoopStorage {
    fn delete_file(&self, _: &Context, _: &str) -> Result<()> {
        Ok(())
    }
    fn delete_files(&self, _: &Context, _: &[String]) -> Result<()> {
        Ok(())
    }
    fn write_file(&self, _: &Context, _: &str, _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn read_file(&self, _: &Context, _: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn file_exists(&self, _: &Context, _: &str) -> Result<bool> {
        Ok(false)
    }
    fn open(&self, _: &Context, _: &str, _: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        Ok(Box::new(NoopReader))
    }
    fn walk_dir(&self, _: &Context, _: Option<&WalkOption>, _: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        Ok(())
    }
    fn uri(&self) -> String {
        "noop:///".to_owned()
    }
    fn create(&self, _: &Context, _: &str, _: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        Ok(Box::new(NoopWriter))
    }
    fn rename(&self, _: &Context, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn presign_file(&self, _: &Context, _: &str, _: Duration) -> Result<String> {
        Ok(String::new())
    }
}

/// Go `noopReader`: every read fills the buffer.
struct NoopReader;

impl Read for NoopReader {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        Ok(p.len())
    }
}

impl Seek for NoopReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        Ok(match pos {
            SeekFrom::Start(o) => o,
            SeekFrom::Current(o) | SeekFrom::End(o) => o as u64,
        })
    }
}

impl Reader for NoopReader {
    fn get_file_size(&self) -> Result<i64> {
        Ok(0)
    }
}

/// Go `NoopWriter`.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopWriter;

impl Writer for NoopWriter {
    fn write(&mut self, _: &Context, p: &[u8]) -> Result<usize> {
        Ok(p.len())
    }
    fn close(&mut self, _: &Context) -> Result<()> {
        Ok(())
    }
    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer> {
        self
    }
}
