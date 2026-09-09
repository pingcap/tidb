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

//! Source-shaped tests for Go `pkg/util/sys/storage`.

use tidb_util::sys::storage::get_target_directory_capacity;

/// Go `pkg/util/sys/storage/sys_test.go::TestGetTargetDirectoryCapacity`.
#[test]
fn current_directory_has_positive_capacity() {
    let capacity = get_target_directory_capacity(".").expect("current directory capacity");
    assert!(capacity >= 1, "could not get capacity: {capacity}");
}

/// Go multiplies `syscall.Statfs_t.Bavail` by `syscall.Statfs_t.Bsize`.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn uses_statfs_available_bytes() {
    let stat = rustix::fs::statfs(".").expect("current directory statfs");
    let expected = stat.f_bavail.wrapping_mul(u64::from(stat.f_bsize));
    let capacity = get_target_directory_capacity(".").expect("current directory capacity");
    assert_eq!(capacity, expected);
}

/// Regression for the supported-platform `syscall.Statfs` error boundary.
/// Go returns the operating-system error for a path that does not exist; the
/// Rust adapter must not silently turn that failure into a zero capacity.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn missing_directory_returns_the_operating_system_error() {
    use std::io::ErrorKind;

    let root = tempfile::tempdir().expect("temporary directory");
    let missing = root.path().join("missing");
    let error = get_target_directory_capacity(missing).expect_err("missing path must fail");
    assert_eq!(error.kind(), ErrorKind::NotFound);
}
