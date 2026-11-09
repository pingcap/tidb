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

//! Go `storage.go` and `storage_unix.go`.

use std::io;
use std::path::Path;

/// Go `StorageSize`: the storage's capacity and available size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageSize {
    /// Total bytes.
    pub capacity: u64,
    /// Bytes available to an unprivileged user.
    pub available: u64,
}

/// Go `GetStorageSize`: the capacity and available size of the storage
/// holding `dir`.
///
/// Go counts in `Frsize` where `statfs` has it (the fundamental block size;
/// macOS's `Bsize` is the optimal transfer size on some mounts), which is
/// `statvfs`'s `f_frsize` everywhere.
///
/// # Errors
/// `dir` cannot be stat'ed: "cannot get disk capacity at {dir}".
pub fn get_storage_size(dir: impl AsRef<Path>) -> io::Result<StorageSize> {
    let dir = dir.as_ref();
    let stat = rustix::fs::statvfs(dir).map_err(|err| {
        io::Error::new(io::ErrorKind::Other, format!("cannot get disk capacity at {}: {err}", dir.display()))
    })?;
    let block_size = stat.f_frsize;
    Ok(StorageSize {
        // Available blocks * size per block = available space in bytes
        available: stat.f_bavail * block_size,
        capacity: stat.f_blocks * block_size,
    })
}

/// Go `SameDisk`: whether `dir1` and `dir2` are on the same disk.
///
/// # Errors
/// A directory cannot be stat'ed.
pub fn same_disk(dir1: impl AsRef<Path>, dir2: impl AsRef<Path>) -> io::Result<bool> {
    let st1 = rustix::fs::stat(dir1.as_ref())?;
    let st2 = rustix::fs::stat(dir2.as_ref())?;
    Ok(st1.st_dev == st2.st_dev)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestGetStorageSize`: only ensure we can get storage size.
    #[test]
    fn test_get_storage_size() {
        let d = tempfile::tempdir().unwrap();
        let size = get_storage_size(d.path()).unwrap();
        assert!(size.capacity > 0);
        assert!(size.available > 0);
    }
}
