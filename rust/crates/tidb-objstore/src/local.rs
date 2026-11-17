//! Go `pkg/objstore/local.go`: storage over a local directory.

use std::fs;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::time::Duration;

use tidb_gocontext::Context;

use crate::error::{Error, Result};
use crate::gopath;
use crate::objectio::{Reader, Writer};
use crate::storeapi::{self, ReaderOption, Storage, WalkOption, WriterOption};

/// Go `LocalURIPrefix`.
pub const LOCAL_URI_PREFIX: &str = "file://";

/// Go `TombstoneSize`.
pub const TOMBSTONE_SIZE: i64 = -1;

/// Go `LocalStorage`.
#[derive(Clone, Debug)]
pub struct LocalStorage {
    base: String,
    /// Go `IgnoreEnoentForDelete`.
    pub ignore_enoent_for_delete: bool,
}

/// Go `pathExists`.
fn path_exists(path: &str) -> Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(gopath::path_error("stat", path, &e)),
    }
}

/// Go `mkdirAll` (0o777 with a cleared umask).
fn mkdir_all(base: &str) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mask = rustix::process::umask(rustix::fs::Mode::empty());
    let result = fs::DirBuilder::new().recursive(true).mode(0o777).create(base);
    rustix::process::umask(mask);
    result.map_err(|e| gopath::path_error("mkdir", base, &e))
}

impl LocalStorage {
    /// Go `NewLocalStorage`.
    pub fn new(base: &str) -> Result<Self> {
        if !path_exists(base)? {
            mkdir_all(base)?;
        }
        Ok(Self { base: base.to_owned(), ignore_enoent_for_delete: false })
    }

    /// Go `Base`.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    fn path(&self, name: &str) -> String {
        gopath::join(&[&self.base, name])
    }

    /// Go `CopyFrom`: hard-links `from` of `source` as `to`.
    pub fn copy_from(&self, source: &LocalStorage, from: &str, to: &str) -> Result<()> {
        let from = gopath::join(&[&source.base, from]);
        let to = self.path(to);
        mkdir_all(&gopath::dir(&to))?;
        fs::hard_link(&from, &to).map_err(|e| gopath::link_error("link", &from, &to, &e))
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        &self,
        base: &str,
        path: &str,
        opt: &WalkOption,
        f: &mut dyn FnMut(&str, i64) -> Result<()>,
    ) -> Result<bool> {
        // Go `filepath.Walk`: Lstat, then the directory's sorted names.
        let meta = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if !opt.include_tombstone {
                    return Ok(true);
                }
                let relative = gopath::rel(base, path).unwrap_or_default();
                if !relative.starts_with(&opt.obj_prefix) {
                    return Ok(true);
                }
                let rel_path = gopath::rel(&self.base, path).unwrap_or_default();
                if !opt.start_after.is_empty() && rel_path <= opt.start_after {
                    return Ok(true);
                }
                f(&rel_path, TOMBSTONE_SIZE)?;
                return Ok(true);
            }
            Err(e) => return Err(gopath::path_error("lstat", path, &e)),
        };
        if meta.is_dir() {
            if path != base {
                if opt.skip_sub_dir {
                    return Ok(true);
                }
                let relative_dir = gopath::rel(&self.base, path).unwrap_or_default();
                if should_skip_local_subtree(&relative_dir, &opt.start_after) {
                    return Ok(true);
                }
            }
            let mut names: Vec<String> = match fs::read_dir(path) {
                Ok(entries) => entries
                    .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
                    .collect(),
                Err(e) => return Err(gopath::path_error("open", path, &e)),
            };
            names.sort();
            for name in names {
                self.walk(base, &gopath::join(&[path, &name]), opt, f)?;
            }
            return Ok(true);
        }
        let relative = gopath::rel(base, path).unwrap_or_default();
        if !relative.starts_with(&opt.obj_prefix) {
            return Ok(true);
        }
        let rel_path = gopath::rel(&self.base, path).unwrap_or_default();
        if !opt.start_after.is_empty() && rel_path <= opt.start_after {
            return Ok(true);
        }
        let size = if meta.file_type().is_file() {
            meta.len() as i64
        } else {
            match fs::metadata(path) {
                Ok(stat) => stat.len() as i64,
                Err(_) => 0,
            }
        };
        f(&rel_path, size)?;
        Ok(true)
    }
}

/// Go `shouldSkipLocalSubtree`.
fn should_skip_local_subtree(dir: &str, start_after: &str) -> bool {
    if start_after.is_empty() {
        return false;
    }
    let prefix = format!("{dir}/");
    if start_after.starts_with(&prefix) {
        return false;
    }
    prefix.as_str() <= start_after
}

impl Storage for LocalStorage {
    fn delete_file(&self, _ctx: &Context, name: &str) -> Result<()> {
        if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/objstore/local_delete_file_err") {
            return Err(Error::other(v.string()));
        }
        let path = self.path(name);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if self.ignore_enoent_for_delete && e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(gopath::path_error("remove", &path, &e).annotate(format!("failed to delete file {name}"))),
        }
    }

    fn delete_files(&self, ctx: &Context, names: &[String]) -> Result<()> {
        for name in names {
            self.delete_file(ctx, name)?;
        }
        Ok(())
    }

    fn write_file(&self, _ctx: &Context, name: &str, data: &[u8]) -> Result<()> {
        if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/objstore/local_write_file_err") {
            return Err(Error::other(v.string()));
        }
        let target = self.path(name);
        let tmp_path = format!("{target}.tmp.{}", uuid::Uuid::new_v4());
        let write = |p: &str| -> io::Result<()> {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o644).open(p)?;
            file.write_all(data)
        };
        if let Err(err) = write(&tmp_path) {
            let err = gopath::path_error("open", &tmp_path, &err);
            let path = gopath::dir(&tmp_path);
            let exists = path_exists(&path).map_err(|e| {
                err.clone().annotate(format!("after failed to write file, failed to check path exists : {e}"))
            })?;
            if exists {
                return Err(err);
            }
            mkdir_all(&path)
                .map_err(|e| err.clone().annotate(format!("after failed to write file, failed to mkdir : {e}")))?;
            write(&tmp_path).map_err(|e| gopath::path_error("open", &tmp_path, &e))?;
        }
        fs::rename(&tmp_path, &target).map_err(|e| gopath::link_error("rename", &tmp_path, &target, &e))
    }

    fn read_file(&self, _ctx: &Context, name: &str) -> Result<Vec<u8>> {
        let path = self.path(name);
        fs::read(&path).map_err(|e| gopath::path_error("open", &path, &e))
    }

    fn file_exists(&self, _ctx: &Context, name: &str) -> Result<bool> {
        path_exists(&self.path(name))
    }

    fn walk_dir(&self, _ctx: &Context, opt: Option<&WalkOption>, f: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        let default = WalkOption::default();
        let opt = opt.unwrap_or(&default);
        let base = gopath::join(&[&self.base, &opt.sub_dir]);
        self.walk(&base, &base, opt, f).map(|_| ())
    }

    fn uri(&self) -> String {
        format!("{LOCAL_URI_PREFIX}{}", self.base)
    }

    fn open(&self, _ctx: &Context, path: &str, o: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        let full = self.path(path);
        let mut file = fs::File::open(&full).map_err(|e| gopath::path_error("open", &full, &e))?;
        let mut pos = 0;
        let mut end_pos = -1;
        if let Some(o) = o {
            if let Some(end) = o.end_offset {
                end_pos = end;
            }
            if let Some(start) = o.start_offset {
                file.seek(SeekFrom::Start(start as u64)).map_err(|e| gopath::path_error("seek", &full, &e))?;
                pos = start;
            }
        }
        Ok(Box::new(LocalFile { file, pos, end_pos }))
    }

    fn create(&self, _ctx: &Context, name: &str, option: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        let filename = self.path(name);
        let dir = gopath::dir(&filename);
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o750)
                .create(&dir)
                .map_err(|e| gopath::path_error("mkdir", &dir, &e))?;
        }
        let file = fs::File::create(&filename).map_err(|e| gopath::path_error("open", &filename, &e))?;
        let buf = match option {
            Some(o) if o.part_size > 0 => BufWriter::with_capacity(o.part_size as usize, file),
            _ => BufWriter::with_capacity(4096, file),
        };
        Ok(Box::new(FlushStorageWriter { writer: Some(buf) }))
    }

    fn rename(&self, _ctx: &Context, old: &str, new: &str) -> Result<()> {
        let (old, new) = (self.path(old), self.path(new));
        fs::rename(&old, &new).map_err(|e| gopath::link_error("rename", &old, &new, &e))
    }

    fn presign_file(&self, _ctx: &Context, file_name: &str, _expire: Duration) -> Result<String> {
        Ok(match file_name.rfind('/') {
            Some(at) => file_name[at + 1..].to_owned(),
            None => file_name.to_owned(),
        })
    }

    fn features(&self) -> storeapi::Features {
        storeapi::FEATURE_SUPPORTS_START_AFTER
    }

    fn as_copier(&self) -> Option<&dyn storeapi::Copier> {
        Some(self)
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

impl storeapi::Copier for LocalStorage {
    /// Go `LocalStorage.CopyFrom`: a hard link from another local storage.
    fn copy_from(&self, _ctx: &Context, e: &dyn Storage, spec: &storeapi::CopySpec) -> Result<()> {
        let Some(source) = e.as_any().and_then(|a| a.downcast_ref::<LocalStorage>()) else {
            return Err(Error::other(format!(
                "expect source to be LocalStorage, got {}: [BR:Common:ErrInvalidArgument]invalid argument",
                e.uri()
            )));
        };
        LocalStorage::copy_from(self, source, &spec.from, &spec.to)
    }
}

/// Go `localFile`: a file read up to an optional end offset.
struct LocalFile {
    file: fs::File,
    pos: i64,
    end_pos: i64,
}

impl Read for LocalFile {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if self.end_pos == -1 {
            return self.file.read(p);
        }
        let p_end = self.end_pos - self.pos;
        if p_end <= 0 {
            return Ok(0);
        }
        let len = p.len().min(p_end as usize);
        let n = self.file.read(&mut p[..len])?;
        self.pos += n as i64;
        Ok(n)
    }
}

impl Seek for LocalFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let n = self.file.seek(pos)?;
        self.pos = n as i64;
        Ok(n)
    }
}

impl Reader for LocalFile {
    fn get_file_size(&self) -> Result<i64> {
        Ok(self.file.metadata().map_err(Error::from)?.len() as i64)
    }
}

/// Go `flushStorageWriter` over a buffered file.
struct FlushStorageWriter {
    writer: Option<BufWriter<fs::File>>,
}

impl Writer for FlushStorageWriter {
    fn write(&mut self, _ctx: &Context, p: &[u8]) -> Result<usize> {
        let w = self.writer.as_mut().ok_or_else(|| Error::other("write to a closed file"))?;
        w.write_all(p).map_err(Error::from)?;
        Ok(p.len())
    }

    fn close(&mut self, _ctx: &Context) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            w.flush().map_err(Error::from)?;
        }
        Ok(())
    }

    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer> {
        self
    }
}
