//! Go `pkg/objstore/memstore.go`: storage held in memory, for tests.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use tidb_gocontext::Context;

use crate::error::{check_ctx, Error, Result};
use crate::objectio::{Reader, Writer};
use crate::storeapi::{ReaderOption, Storage, WalkOption, WriterOption};

/// Go `memFile`: a file's current contents.
#[derive(Default)]
struct MemFile {
    data: RwLock<Arc<Vec<u8>>>,
}

impl MemFile {
    fn get(&self) -> Arc<Vec<u8>> {
        self.data.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn store(&self, data: Vec<u8>) {
        *self.data.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(data);
    }
}

/// Go `MemStorage`.
#[derive(Default)]
pub struct MemStorage {
    data_store: RwLock<HashMap<String, Arc<MemFile>>>,
}

impl MemStorage {
    /// Go `NewMemStorage`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn load(&self, name: &str) -> Option<Arc<MemFile>> {
        self.data_store.read().unwrap_or_else(PoisonError::into_inner).get(name).cloned()
    }
}

fn not_found(name: &str) -> Error {
    Error::new(crate::ErrorKind::NotFound, format!("cannot find the file: {name}"))
}

impl Storage for MemStorage {
    fn delete_file(&self, ctx: &Context, name: &str) -> Result<()> {
        check_ctx(ctx)?;
        let mut store = self.data_store.write().unwrap_or_else(PoisonError::into_inner);
        store.remove(name).map(|_| ()).ok_or_else(|| not_found(name))
    }

    fn delete_files(&self, ctx: &Context, names: &[String]) -> Result<()> {
        for name in names {
            self.delete_file(ctx, name)?;
        }
        Ok(())
    }

    fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> Result<()> {
        check_ctx(ctx)?;
        let mut store = self.data_store.write().unwrap_or_else(PoisonError::into_inner);
        store.entry(name.to_owned()).or_default().store(data.to_vec());
        Ok(())
    }

    fn read_file(&self, ctx: &Context, name: &str) -> Result<Vec<u8>> {
        check_ctx(ctx)?;
        self.load(name).map(|f| f.get().as_ref().clone()).ok_or_else(|| not_found(name))
    }

    fn file_exists(&self, ctx: &Context, name: &str) -> Result<bool> {
        check_ctx(ctx)?;
        Ok(self.load(name).is_some())
    }

    fn open(&self, ctx: &Context, path: &str, o: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        check_ctx(ctx)?;
        let data = self.load(path).ok_or_else(|| not_found(path))?.get();
        let mut start = 0;
        let mut end = data.len() as i64;
        if let Some(o) = o {
            if let Some(s) = o.start_offset {
                start = s;
            }
            if let Some(e) = o.end_offset {
                end = e;
            }
        }
        let size = data.len() as i64;
        let mut br = io::Cursor::new(ArcBytes(data));
        br.seek(SeekFrom::Start(start as u64)).map_err(Error::from)?;
        Ok(Box::new(MemFileReader { br, pos: start, end, size, closed: false }))
    }

    fn walk_dir(&self, ctx: &Context, opt: Option<&WalkOption>, f: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        let mut names: Vec<String> = {
            let store = self.data_store.read().unwrap_or_else(PoisonError::into_inner);
            store
                .keys()
                .filter(|name| {
                    opt.is_none_or(|opt| {
                        (opt.sub_dir.is_empty() || name.starts_with(&opt.sub_dir))
                            && (opt.obj_prefix.is_empty() || crate::gopath::base(name).starts_with(&opt.obj_prefix))
                    })
                })
                .cloned()
                .collect()
        };
        names.sort();
        for name in names {
            check_ctx(ctx)?;
            let Some(file) = self.load(&name) else { continue };
            f(&name, file.get().len() as i64)?;
        }
        Ok(())
    }

    fn uri(&self) -> String {
        "memstore://".to_owned()
    }

    fn create(&self, ctx: &Context, name: &str, _option: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        check_ctx(ctx)?;
        let file = Arc::new(MemFile::default());
        self.data_store.write().unwrap_or_else(PoisonError::into_inner).insert(name.to_owned(), file.clone());
        Ok(Box::new(MemFileWriter { buf: Vec::new(), file, closed: false }))
    }

    fn rename(&self, ctx: &Context, old: &str, new: &str) -> Result<()> {
        check_ctx(ctx)?;
        let mut store = self.data_store.write().unwrap_or_else(PoisonError::into_inner);
        let file = store.remove(old).ok_or_else(|| Error::other(format!("the file doesn't exist: {old}")))?;
        store.insert(new.to_owned(), file);
        Ok(())
    }

    fn presign_file(&self, _ctx: &Context, file_name: &str, _expire: Duration) -> Result<String> {
        Ok(crate::gopath::base(file_name))
    }

    fn close(&self) {
        self.data_store.write().unwrap_or_else(PoisonError::into_inner).clear();
    }
}

/// File bytes shared with the storage.
struct ArcBytes(Arc<Vec<u8>>);

impl AsRef<[u8]> for ArcBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Go `memFileReader`.
struct MemFileReader {
    br: io::Cursor<ArcBytes>,
    pos: i64,
    end: i64,
    size: i64,
    closed: bool,
}

impl Read for MemFileReader {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if self.closed {
            return Ok(0);
        }
        let max = (self.end - self.pos).min(p.len() as i64);
        if max <= 0 {
            return Ok(0);
        }
        let n = self.br.read(&mut p[..max as usize])?;
        self.pos += n as i64;
        Ok(n)
    }
}

impl Seek for MemFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if self.closed {
            return Err(io::Error::other("reader closed"));
        }
        let n = self.br.seek(pos)?;
        self.pos = n as i64;
        Ok(n)
    }
}

impl Reader for MemFileReader {
    fn get_file_size(&self) -> Result<i64> {
        Ok(self.size)
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }
}

/// Go `memFileWriter`: the file's contents are replaced on close.
struct MemFileWriter {
    buf: Vec<u8>,
    file: Arc<MemFile>,
    closed: bool,
}

impl Writer for MemFileWriter {
    fn write(&mut self, ctx: &Context, p: &[u8]) -> Result<usize> {
        check_ctx(ctx)?;
        if self.closed {
            return Err(Error::other("writer closed"));
        }
        self.buf.extend_from_slice(p);
        Ok(p.len())
    }

    fn close(&mut self, ctx: &Context) -> Result<()> {
        check_ctx(ctx)?;
        self.file.store(std::mem::take(&mut self.buf));
        self.closed = true;
        Ok(())
    }

    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer> {
        self
    }
}
