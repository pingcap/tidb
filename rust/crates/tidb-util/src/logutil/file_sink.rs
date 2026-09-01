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

//! The rotating file sink behind `pingcap/log`'s lumberjack writer:
//! size-based rotation with timestamped backups, retention by count/age, and
//! optional gzip compression of rotated files.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A log sink: either a rotating file or a raw stream (stdout/stderr).
pub enum Sink {
    /// Rotating file writer (lumberjack semantics subset).
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
    /// Writes one encoded log line.
    pub fn write_line(&mut self, line: &str) {
        match self {
            Sink::File(f) => {
                let _ = f.write(line.as_bytes());
            }
            Sink::Stdout => {
                let _ = std::io::stdout().write_all(line.as_bytes());
            }
            Sink::Stderr => {
                let _ = std::io::stderr().write_all(line.as_bytes());
            }
        }
    }
}

/// Lumberjack-style rotating file writer.
pub struct RotatingFile {
    path: PathBuf,
    file: Option<File>,
    size: u64,
    /// Max size in MB before rotation (lumberjack `MaxSize`; 0 = 100MB
    /// lumberjack default).
    pub max_size_mb: i64,
    /// Max rotated files kept (0 = unlimited).
    pub max_backups: i64,
    /// Max age of rotated files in days (0 = unlimited).
    pub max_days: i64,
    /// gzip-compress rotated files.
    pub compress: bool,
}

impl RotatingFile {
    /// Opens (creating if needed) the rotating file.
    pub fn open(
        path: &Path,
        max_size_mb: i64,
        max_backups: i64,
        compress: bool,
    ) -> std::io::Result<RotatingFile> {
        Self::open_with_max_days(path, max_size_mb, max_backups, 0, compress)
    }

    /// Opens a rotating file writer with count and age retention.
    pub fn open_with_max_days(
        path: &Path,
        max_size_mb: i64,
        max_backups: i64,
        max_days: i64,
        compress: bool,
    ) -> std::io::Result<RotatingFile> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(RotatingFile {
            path: path.to_path_buf(),
            file: Some(file),
            size,
            max_size_mb: if max_size_mb <= 0 { 100 } else { max_size_mb },
            max_backups,
            max_days,
            compress,
        })
    }

    fn max_bytes(&self) -> u64 {
        self.max_size_mb as u64 * 1024 * 1024
    }

    /// Appends bytes, rotating first when the write would exceed the max
    /// size (lumberjack semantics).
    pub fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if self.size + buf.len() as u64 > self.max_bytes() && self.size > 0 {
            self.rotate()?;
        }
        if let Some(f) = &mut self.file {
            f.write_all(buf)?;
            self.size += buf.len() as u64;
        }
        Ok(())
    }

    // Rotate: rename to `name-<timestamp><ext>`, reopen fresh, compress
    // and prune backups.
    fn rotate(&mut self) -> std::io::Result<()> {
        self.file = None;
        let backup = self.backup_path();
        std::fs::rename(&self.path, &backup)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.file = Some(file);
        self.size = 0;
        if self.compress {
            let _ = gzip_file(&backup);
        }
        self.prune_backups();
        Ok(())
    }

    fn backup_path(&self) -> PathBuf {
        // lumberjack: name-2006-01-02T15-04-05.000.ext
        let now = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S%.3f");
        let stem = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let ext = self
            .path
            .extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default();
        self.path.with_file_name(format!("{stem}-{now}{ext}"))
    }

    fn prune_backups(&self) {
        if self.max_backups <= 0 && self.max_days <= 0 {
            return;
        }
        let Some(dir) = self.path.parent() else {
            return;
        };
        let Some(stem) = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
        else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        }) else {
            return;
        };
        let extension = self
            .path
            .extension()
            .map(|value| format!(".{}", value.to_string_lossy()))
            .unwrap_or_default();
        let prefix = format!("{stem}-");
        let mut backups: Vec<(PathBuf, chrono::NaiveDateTime, String)> = entries
            .flatten()
            .map(|e| e.path())
            .filter_map(|path| {
                if path.is_dir() {
                    return None;
                }
                let name = path.file_name()?.to_str()?;
                let base_name = name.strip_suffix(".gz").unwrap_or(name);
                let timestamp = base_name.strip_prefix(&prefix)?.strip_suffix(&extension)?;
                let timestamp =
                    chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H-%M-%S%.3f")
                        .ok()?;
                let base_name = base_name.to_string();
                Some((path, timestamp, base_name))
            })
            .collect();
        backups.sort_by_key(|left| std::cmp::Reverse(left.1));

        let cutoff = (self.max_days > 0)
            .then(|| chrono::Local::now().naive_local() - chrono::Duration::days(self.max_days));
        let mut preserved = HashSet::new();
        for (path, timestamp, base_name) in backups {
            preserved.insert(base_name);
            let too_many = self.max_backups > 0 && preserved.len() > self.max_backups as usize;
            let too_old = cutoff.is_some_and(|cutoff| timestamp < cutoff);
            if too_many || too_old {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn gzip_file(path: &Path) -> std::io::Result<()> {
    let data = std::fs::read(path)?;
    let gz_path = {
        let mut p = path.as_os_str().to_owned();
        p.push(".gz");
        PathBuf::from(p)
    };
    let out = File::create(&gz_path)?;
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::default());
    enc.write_all(&data)?;
    enc.finish()?;
    std::fs::remove_file(path)
}
