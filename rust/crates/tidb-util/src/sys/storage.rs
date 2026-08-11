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

//! Filesystem-capacity query from Go `pkg/util/sys/storage`.
//!
//! Linux and macOS use the caller-available block count (`f_bavail`) and
//! filesystem block size (`f_bsize`) from `statfs`. Windows uses the
//! caller-scoped value from `GetDiskFreeSpaceExW` through `fs4`; unlike the
//! volume-wide free count, this honors per-user disk quotas. Go's remaining
//! build variant does not query the OS and returns `math.MaxInt64`.

use std::io;
use std::path::Path;

#[cfg(any(test, not(any(target_os = "linux", target_os = "macos", windows))))]
const OTHER_PLATFORM_CAPACITY: u64 = i64::MAX as u64;

/// Go `GetTargetDirectoryCapacity`: returns bytes available to the caller on
/// the filesystem containing `path`.
pub fn get_target_directory_capacity(path: impl AsRef<Path>) -> io::Result<u64> {
    get_target_directory_capacity_impl(path.as_ref())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_target_directory_capacity_impl(path: &Path) -> io::Result<u64> {
    let stat = rustix::fs::statvfs(path).map_err(io::Error::from)?;
    Ok(stat.f_bavail.wrapping_mul(stat.f_bsize))
}

#[cfg(windows)]
fn get_target_directory_capacity_impl(path: &Path) -> io::Result<u64> {
    fs4::available_space(path)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn get_target_directory_capacity_impl(_path: &Path) -> io::Result<u64> {
    Ok(OTHER_PLATFORM_CAPACITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_directory_has_capacity_like_the_go_test() {
        assert!(get_target_directory_capacity(".").unwrap() >= 1);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn missing_directory_reports_the_os_error() {
        let root = tempfile::tempdir().unwrap();
        assert!(get_target_directory_capacity(root.path().join("missing")).is_err());
    }

    #[test]
    fn other_platform_fallback_is_go_math_max_int64() {
        assert_eq!(OTHER_PLATFORM_CAPACITY, 9_223_372_036_854_775_807);
    }
}
