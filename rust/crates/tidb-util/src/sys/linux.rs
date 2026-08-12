// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Operating-system identity, process affinity, and Unix peer credentials.
//!
//! This is the native Rust mapping of Go `pkg/util/sys/linux`. The Go package
//! name is historical: Linux has richer implementations while other targets
//! retain the same public operations.

use std::io;

/// Returns the operating-system identity reported by Go `OSVersion`.
#[cfg(target_os = "linux")]
pub fn os_version() -> io::Result<String> {
    let name = rustix::system::uname();
    Ok(format!(
        "{} {}.{}",
        name.sysname().to_string_lossy(),
        name.release().to_string_lossy(),
        name.machine().to_string_lossy()
    ))
}

/// Returns Go's `GOOS.GOARCH` identity on non-Linux targets.
#[cfg(not(target_os = "linux"))]
pub fn os_version() -> io::Result<String> {
    Ok(format!("{}.{}", go_os(), go_arch()))
}

#[cfg(not(target_os = "linux"))]
const fn go_os() -> &'static str {
    if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "freebsd") {
        "freebsd"
    } else if cfg!(target_os = "openbsd") {
        "openbsd"
    } else if cfg!(target_os = "netbsd") {
        "netbsd"
    } else if cfg!(target_os = "dragonfly") {
        "dragonfly"
    } else if cfg!(target_os = "solaris") {
        "solaris"
    } else if cfg!(target_os = "illumos") {
        "illumos"
    } else if cfg!(target_os = "aix") {
        "aix"
    } else {
        std::env::consts::OS
    }
}

#[cfg(not(target_os = "linux"))]
const fn go_arch() -> &'static str {
    if cfg!(target_arch = "x86_64") {
        "amd64"
    } else if cfg!(target_arch = "x86") {
        "386"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else if cfg!(target_arch = "arm") {
        "arm"
    } else if cfg!(target_arch = "powerpc64") {
        if cfg!(target_endian = "little") {
            "ppc64le"
        } else {
            "ppc64"
        }
    } else if cfg!(target_arch = "s390x") {
        "s390x"
    } else if cfg!(target_arch = "riscv64") {
        "riscv64"
    } else {
        std::env::consts::ARCH
    }
}

/// Restricts the current process thread to the listed CPU indexes on Linux.
///
/// Go's `CPUSet.Set` ignores negative and out-of-range indexes. Rust does the
/// same before passing the resulting mask to `sched_setaffinity`.
#[cfg(target_os = "linux")]
pub fn set_affinity(cpus: &[i64]) -> io::Result<()> {
    let mut set = rustix::thread::CpuSet::new();
    for &cpu in cpus {
        if let Ok(cpu) = usize::try_from(cpu) {
            if cpu < rustix::thread::CpuSet::MAX_CPU {
                set.set(cpu);
            }
        }
    }
    rustix::thread::sched_setaffinity(None, &set).map_err(io::Error::from)
}

/// CPU affinity is unsupported and therefore a no-op off Linux, as in Go.
#[cfg(not(target_os = "linux"))]
pub fn set_affinity(_cpus: &[i64]) -> io::Result<()> {
    Ok(())
}

/// Returns the effective UID at the other end of a Unix-domain stream.
#[cfg(target_os = "linux")]
pub fn get_sock_uid(stream: &std::os::unix::net::UnixStream) -> io::Result<u32> {
    nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map(|credentials| credentials.uid())
        .map_err(io::Error::from)
}

/// Returns the effective UID at the other end of a Unix-domain stream.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
))]
pub fn get_sock_uid(stream: &std::os::unix::net::UnixStream) -> io::Result<u32> {
    nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::LocalPeerCred)
        .map(|credentials| credentials.uid())
        .map_err(io::Error::from)
}

/// Unix-domain sockets are unavailable on Windows.
#[cfg(windows)]
pub fn get_sock_uid<T>(_stream: &T) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "UNIX domain socket is not supported on Windows",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operating_system_version_is_not_empty() {
        let version = os_version().unwrap();
        assert!(!version.is_empty());
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        assert_eq!(version, "darwin.arm64");
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    #[test]
    fn unix_socket_peer_uid_is_the_current_effective_user() {
        let (left, _right) = std::os::unix::net::UnixStream::pair().unwrap();
        assert_eq!(
            get_sock_uid(&left).unwrap(),
            rustix::process::geteuid().as_raw()
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn affinity_is_a_noop_off_linux() {
        set_affinity(&[-1, 0, i64::MAX]).unwrap();
    }
}
