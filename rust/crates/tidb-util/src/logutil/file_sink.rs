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
//! size-based rotation with timestamped backups, retention by count, and
//! optional gzip compression of rotated files.

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
        if self.max_backups <= 0 {
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
        let mut backups: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p != &self.path
                    && p.file_name()
                        .map(|n| n.to_string_lossy().starts_with(&format!("{stem}-")))
                        .unwrap_or(false)
            })
            .collect();
        backups.sort();
        while backups.len() > self.max_backups as usize {
            let victim = backups.remove(0);
            let _ = std::fs::remove_file(victim);
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
