// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The rotating file sink behind `pingcap/log`'s lumberjack writer
//! (`gopkg.in/natefinch/lumberjack.v2` v2.2.1 with `LocalTime: true`):
//! size-based rotation with timestamped backups, retention by count/age, and
//! optional gzip compression of rotated files. `pingcap/log`'s
//! `initFileLog` validation runs when the sink is built; the file itself is
//! opened lazily on the first write, like lumberjack.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A log sink: either a rotating file or a raw stream (stdout/stderr).
pub enum Sink {
    /// Rotating file writer (lumberjack semantics).
    File(RotatingFile),
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// Shared, thread-safe sink handle. Loggers writing to the same filename
/// share one handle (the Go tests assert syncer identity).
pub type SharedSink = Arc<Mutex<Sink>>;

impl Sink {
    /// Writes one encoded log line, dropping write errors.
    pub fn write_line(&mut self, line: &str) {
        let _ = self.write_bytes(line.as_bytes());
    }

    /// Writes encoded bytes (zap `WriteSyncer.Write`).
    pub fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Sink::File(f) => f.write(bytes),
            Sink::Stdout => io::stdout().write_all(bytes),
            Sink::Stderr => io::stderr().write_all(bytes),
        }
    }

    /// Flushes the sink (zap `WriteSyncer.Sync`; lumberjack syncs its open
    /// file).
    pub fn sync(&mut self) -> io::Result<()> {
        match self {
            Sink::File(f) => f.sync(),
            Sink::Stdout => io::stdout().flush(),
            Sink::Stderr => io::stderr().flush(),
        }
    }
}

const MEGABYTE: i64 = 1024 * 1024;
// lumberjack `backupTimeFormat` ("2006-01-02T15-04-05.000").
const BACKUP_TIME_FORMAT: &str = "%Y-%m-%dT%H-%M-%S%.3f";
const COMPRESS_SUFFIX: &str = ".gz";

/// Lumberjack-style rotating file writer.
pub struct RotatingFile {
    path: PathBuf,
    file: Option<File>,
    size: u64,
    /// Max size in MB before rotation (`pingcap/log` turns an unset
    /// max-size into its `defaultLogMaxSize` of 300).
    pub max_size_mb: i64,
    /// Max rotated files kept (0 = unlimited).
    pub max_backups: i64,
    /// Max age of rotated files in days (0 = unlimited).
    pub max_days: i64,
    /// gzip-compress rotated files.
    pub compress: bool,
}

impl RotatingFile {
    /// Builds the rotating file writer.
    pub fn open(
        path: &Path,
        max_size_mb: i64,
        max_backups: i64,
        compress: bool,
    ) -> io::Result<RotatingFile> {
        Self::open_with_max_days(path, max_size_mb, max_backups, 0, compress)
    }

    /// Builds a rotating file writer with count and age retention, after
    /// `pingcap/log`'s `initFileLog` checks: the directory is created, the
    /// path must not be a directory, and the file must be writable or
    /// creatable (a probe file is created and removed again, since
    /// lumberjack creates the log lazily).
    pub fn open_with_max_days(
        path: &Path,
        max_size_mb: i64,
        max_backups: i64,
        max_days: i64,
        compress: bool,
    ) -> io::Result<RotatingFile> {
        std::fs::create_dir_all(log_dir(path)).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot create log directory: {error}"),
            )
        })?;
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.is_dir() => {
                return Err(io::Error::other("can't use directory as log file name"));
            }
            Ok(_) => {
                OpenOptions::new()
                    .append(true)
                    .open(path)
                    .map_err(|error| {
                        io::Error::new(error.kind(), format!("can't write to log file: {error}"))
                    })?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                File::create(path).map_err(|error| {
                    io::Error::new(error.kind(), format!("can't create log file: {error}"))
                })?;
                let _ = std::fs::remove_file(path);
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("error checking log file: {error}"),
                ));
            }
        }
        Ok(RotatingFile {
            path: path.to_path_buf(),
            file: None,
            size: 0,
            max_size_mb: if max_size_mb == 0 { 300 } else { max_size_mb },
            max_backups,
            max_days,
            compress,
        })
    }

    fn max_bytes(&self) -> i64 {
        self.max_size_mb.saturating_mul(MEGABYTE)
    }

    /// Appends bytes (lumberjack `Write`): a write larger than the max size
    /// is refused, the file is opened on first use, and the file rotates
    /// first when the write would exceed the max size.
    pub fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        let write_len = buf.len() as i64;
        if write_len > self.max_bytes() {
            return Err(io::Error::other(format!(
                "write length {write_len} exceeds maximum file size {}",
                self.max_bytes()
            )));
        }
        if self.file.is_none() {
            self.open_existing_or_new(write_len)?;
        }
        if self.size as i64 + write_len > self.max_bytes() {
            self.rotate()?;
        }
        if let Some(f) = &mut self.file {
            f.write_all(buf)?;
            self.size += buf.len() as u64;
        }
        Ok(())
    }

    /// lumberjack `Sync`: fsyncs the open file.
    pub fn sync(&mut self) -> io::Result<()> {
        match &self.file {
            Some(file) => file.sync_all(),
            None => Ok(()),
        }
    }

    // lumberjack `openExistingOrNew`.
    fn open_existing_or_new(&mut self, write_len: i64) -> io::Result<()> {
        self.mill_run_once();
        let info = match std::fs::metadata(&self.path) {
            Ok(info) => info,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return self.open_new(),
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("error getting log file info: {error}"),
                ));
            }
        };
        if info.len() as i64 + write_len >= self.max_bytes() {
            return self.rotate();
        }
        match OpenOptions::new().append(true).open(&self.path) {
            Ok(file) => {
                self.file = Some(file);
                self.size = info.len();
                Ok(())
            }
            // lumberjack ignores a failed reopen and starts a new file.
            Err(_) => self.open_new(),
        }
    }

    // lumberjack `rotate`: close, open a new file, then mill.
    fn rotate(&mut self) -> io::Result<()> {
        self.file = None;
        self.open_new()?;
        self.mill_run_once();
        Ok(())
    }

    // lumberjack `openNew`: move an existing file to its backup name, then
    // create the log truncated with the old file's mode (0600 when new).
    fn open_new(&mut self) -> io::Result<()> {
        std::fs::create_dir_all(log_dir(&self.path)).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("can't make directories for new logfile: {error}"),
            )
        })?;
        let mut mode = 0o600;
        if let Ok(info) = std::fs::metadata(&self.path) {
            mode = file_mode(&info);
            std::fs::rename(&self.path, self.backup_path()).map_err(|error| {
                io::Error::new(error.kind(), format!("can't rename log file: {error}"))
            })?;
        }
        let file = open_truncated(&self.path, mode).map_err(|error| {
            io::Error::new(error.kind(), format!("can't open new logfile: {error}"))
        })?;
        self.file = Some(file);
        self.size = 0;
        Ok(())
    }

    fn backup_path(&self) -> PathBuf {
        // lumberjack `backupName`: <prefix>-<local timestamp><ext>.
        let now = chrono::Local::now().format(BACKUP_TIME_FORMAT);
        let (prefix, ext) = prefix_and_ext(&self.path);
        self.path.with_file_name(format!("{prefix}-{now}{ext}"))
    }

    // lumberjack `millRunOnce`: drop backups beyond `max_backups`, then those
    // older than `max_days`, then gzip the remaining uncompressed backups.
    fn mill_run_once(&self) {
        if self.max_backups == 0 && self.max_days == 0 && !self.compress {
            return;
        }
        let mut files = self.old_log_files();
        let mut remove = Vec::new();
        if self.max_backups > 0 && (self.max_backups as usize) < files.len() {
            let mut preserved = std::collections::HashSet::new();
            let mut remaining = Vec::new();
            for (name, timestamp) in files {
                // Only count the uncompressed or the compressed file, not both.
                preserved.insert(
                    name.strip_suffix(COMPRESS_SUFFIX)
                        .unwrap_or(&name)
                        .to_owned(),
                );
                if preserved.len() > self.max_backups as usize {
                    remove.push(name);
                } else {
                    remaining.push((name, timestamp));
                }
            }
            files = remaining;
        }
        if self.max_days > 0 {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(self.max_days);
            let mut remaining = Vec::new();
            for (name, timestamp) in files {
                if timestamp < cutoff {
                    remove.push(name);
                } else {
                    remaining.push((name, timestamp));
                }
            }
            files = remaining;
        }
        let dir = log_dir(&self.path);
        for name in remove {
            let _ = std::fs::remove_file(dir.join(name));
        }
        if self.compress {
            for (name, _) in files {
                if !name.ends_with(COMPRESS_SUFFIX) {
                    let _ = gzip_file(&dir.join(name));
                }
            }
        }
    }

    // lumberjack `oldLogFiles`: backups named `<prefix>-<timestamp><ext>`
    // (optionally `.gz`), newest first. `time.Parse` reads the timestamp as
    // UTC although `LocalTime` wrote it in local time; the age cutoff
    // compares those instants, exactly like the source.
    fn old_log_files(&self) -> Vec<(String, chrono::DateTime<chrono::Utc>)> {
        let Ok(entries) = std::fs::read_dir(log_dir(&self.path)) else {
            return Vec::new();
        };
        let (prefix, ext) = prefix_and_ext(&self.path);
        let prefix = format!("{prefix}-");
        let compressed_ext = format!("{ext}{COMPRESS_SUFFIX}");
        let mut files: Vec<(String, chrono::DateTime<chrono::Utc>)> = entries
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| !kind.is_dir()))
            .filter_map(|entry| {
                let name = entry.file_name().to_str()?.to_owned();
                let timestamp = time_from_name(&name, &prefix, &ext)
                    .or_else(|| time_from_name(&name, &prefix, &compressed_ext))?;
                Some((name, timestamp))
            })
            .collect();
        files.sort_by_key(|(_, timestamp)| std::cmp::Reverse(*timestamp));
        files
    }
}

// lumberjack `timeFromName`.
fn time_from_name(name: &str, prefix: &str, ext: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let timestamp = name.strip_prefix(prefix)?.strip_suffix(ext)?;
    chrono::NaiveDateTime::parse_from_str(timestamp, BACKUP_TIME_FORMAT)
        .ok()
        .map(|naive| naive.and_utc())
}

// Go `filepath.Dir` of the log path.
fn log_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

// Go `filepath.Ext` split of the base name: the extension starts at the
// last `.`.
fn prefix_and_ext(path: &Path) -> (String, String) {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    match name.rfind('.') {
        Some(index) => (name[..index].to_owned(), name[index..].to_owned()),
        None => (name, String::new()),
    }
}

#[cfg(unix)]
fn file_mode(info: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    info.permissions().mode() & 0o7777
}

#[cfg(not(unix))]
fn file_mode(_info: &std::fs::Metadata) -> u32 {
    0o600
}

#[cfg(unix)]
fn open_truncated(path: &Path, mode: u32) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(mode)
        .open(path)
}

#[cfg(not(unix))]
fn open_truncated(path: &Path, _mode: u32) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
}

// lumberjack `compressLogFile`: `<file>.gz`, then the source is removed.
fn gzip_file(path: &Path) -> io::Result<()> {
    let data = std::fs::read(path)?;
    let gz_path = {
        let mut p = path.as_os_str().to_owned();
        p.push(COMPRESS_SUFFIX);
        PathBuf::from(p)
    };
    let out = File::create(&gz_path)?;
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::default());
    enc.write_all(&data)?;
    enc.finish()?;
    std::fs::remove_file(path)
}
