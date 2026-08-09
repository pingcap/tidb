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

//! Transcreation of Go `pkg/util/disk/tempDir.go`.
//!
//! The directory named by `tmp-storage-path` is created once, on first use by
//! a spilling operator, and swept of files left behind by a previous run.
//!
//! WHERE THE PATH COMES FROM: Go reads `config.GetGlobalConfig()
//! .TempStoragePath` on every call. This crate sits below `tidb-config`, so
//! the path lives here in a process-global that the server sets once from its
//! config ([`set_temp_storage_path`]). Until it is set, the default is the
//! same shape Go's `tempStorageDirName` has -- see
//! [`encode_def_temp_storage_dir`].

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, RwLock};

/// Go `lockFile`.
pub const LOCK_FILE: &str = "_dir.lock";
/// Go `recordDir`.
pub const RECORD_DIR: &str = "record";

/// Go `config.DefHost`.
const DEF_HOST: &str = "0.0.0.0";
/// Go `config.DefStatusHost`.
const DEF_STATUS_HOST: &str = "0.0.0.0";
/// Go `config.DefPort`.
const DEF_PORT: u32 = 4000;
/// Go `config.DefStatusPort`.
const DEF_STATUS_PORT: u32 = 10080;

fn path_cell() -> &'static RwLock<PathBuf> {
    static PATH: OnceLock<RwLock<PathBuf>> = OnceLock::new();
    PATH.get_or_init(|| {
        RwLock::new(encode_def_temp_storage_dir(
            &std::env::temp_dir(),
            DEF_HOST,
            DEF_STATUS_HOST,
            DEF_PORT,
            DEF_STATUS_PORT,
            "",
        ))
    })
}

/// Go's `sf singleflight.Group` for `"tempDir"`: one initialization at a time.
/// Go collapses concurrent callers onto one result; this serializes them onto
/// the same work, which reaches the same state because
/// [`check_and_init_temp_dir`] re-checks existence under the lock.
fn init_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Go `config.GetGlobalConfig().TempStoragePath`.
#[must_use]
pub fn temp_storage_path() -> PathBuf {
    path_cell().read().expect("temp storage path").clone()
}

/// Sets the process-wide `tmp-storage-path`. The server calls this once from
/// its config; tests call it to redirect spill files into a scratch dir.
pub fn set_temp_storage_path(path: impl Into<PathBuf>) {
    *path_cell().write().expect("temp storage path") = path.into();
}

/// Go `config.encodeDefTempStorageDir`:
/// `<temp_dir>/<uid>_tidb/<base64url("host:port/status_host:status_port")>/tmp-storage`.
///
/// SEAM: Go fills `os_uid` from `user.Current().Uid`, falling back to `""`
/// when that fails. Reading the process uid needs a libc binding this
/// workspace does not carry, so the caller supplies it; the built-in default
/// path uses `""`, which is Go's own fallback value.
#[must_use]
pub fn encode_def_temp_storage_dir(
    temp_dir: &Path,
    host: &str,
    status_host: &str,
    port: u32,
    status_port: u32,
    os_uid: &str,
) -> PathBuf {
    let dir_name =
        base64_url_encode(format!("{host}:{port}/{status_host}:{status_port}").as_bytes());
    temp_dir
        .join(format!("{os_uid}_tidb"))
        .join(dir_name)
        .join("tmp-storage")
}

/// Go `base64.URLEncoding.EncodeToString`: the RFC 4648 URL alphabet, padded.
fn base64_url_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for group in input.chunks(3) {
        let b0 = u32::from(group[0]);
        let b1 = group.get(1).map_or(0, |b| u32::from(*b));
        let b2 = group.get(2).map_or(0, |b| u32::from(*b));
        let bits = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(bits >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(bits >> 12) as usize & 0x3f] as char);
        out.push(if group.len() > 1 {
            ALPHABET[(bits >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if group.len() > 2 {
            ALPHABET[bits as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Go `CheckAndInitTempDir`: initializes the temp directory unless it already
/// exists.
pub fn check_and_init_temp_dir() -> io::Result<()> {
    let _guard = init_lock().lock().expect("temp dir init");
    if check_temp_dir_exist() {
        return Ok(());
    }
    tracing::info!("Tmp-storage-path not found. Try to initialize TempDir.");
    initialize_temp_dir_locked()
}

/// Go `checkTempDirExist`.
fn check_temp_dir_exist() -> bool {
    fs::metadata(temp_storage_path()).is_ok()
}

/// Go `InitializeTempDir`.
pub fn initialize_temp_dir() -> io::Result<()> {
    let _guard = init_lock().lock().expect("temp dir init");
    initialize_temp_dir_locked()
}

fn initialize_temp_dir_locked() -> io::Result<()> {
    let temp_dir = temp_storage_path();
    if fs::metadata(&temp_dir).is_err() {
        fs::create_dir_all(&temp_dir)?;
    }

    // Go takes the `_dir.lock` fslock here; see the module doc of
    // `crate::disk` for why it is absent.

    // Go: when the directory holds files besides the lock file and the record
    // directory, sweep them -- they are a previous run's spill files. Go does
    // this in a goroutine; doing it inline keeps the first spill from racing
    // the sweep for the very file it is about to create, which Go tolerates
    // only because its file names are random too.
    let entries: Vec<_> = fs::read_dir(&temp_dir)?.collect::<Result<Vec<_>, _>>()?;
    if entries.len() > 2 {
        for entry in entries {
            let name = entry.file_name();
            if name == LOCK_FILE || name == RECORD_DIR {
                continue;
            }
            let path = temp_dir.join(&name);
            let removed = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
            if let Err(error) = removed {
                tracing::warn!(?path, %error, "Remove temporary file error");
            }
        }
    }
    Ok(())
}

/// Go `CleanUp`: releases the directory lock when TiDB exits. The lock is not
/// taken here (see the `crate::disk` module doc), so this is a no-op that
/// keeps the shutdown call site source-shaped.
pub fn clean_up() {}

/// Go `CheckAndCreateDir`.
pub fn check_and_create_dir(path: &Path) -> io::Result<()> {
    if fs::metadata(path).is_err() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_url_matches_go() {
        // Go: base64.URLEncoding.EncodeToString([]byte("0.0.0.0:4000/0.0.0.0:10080"))
        assert_eq!(
            base64_url_encode(b"0.0.0.0:4000/0.0.0.0:10080"),
            "MC4wLjAuMDo0MDAwLzAuMC4wLjA6MTAwODA="
        );
        assert_eq!(base64_url_encode(b""), "");
        assert_eq!(base64_url_encode(b"a"), "YQ==");
        assert_eq!(base64_url_encode(b"ab"), "YWI=");
        assert_eq!(base64_url_encode(b"abc"), "YWJj");
        assert_eq!(base64_url_encode(&[0xfb, 0xff]), "-_8=");
    }

    #[test]
    fn test_encode_def_temp_storage_dir() {
        for (host, status_host, port, status_port, encoded) in [
            (
                "0.0.0.0",
                "0.0.0.0",
                4000,
                10080,
                "MC4wLjAuMDo0MDAwLzAuMC4wLjA6MTAwODA=",
            ),
            (
                "127.0.0.1",
                "127.16.5.1",
                4000,
                10080,
                "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxMDA4MA==",
            ),
            (
                "127.0.0.1",
                "127.16.5.1",
                4000,
                15532,
                "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxNTUzMg==",
            ),
        ] {
            let dir = encode_def_temp_storage_dir(
                Path::new("/tmp"),
                host,
                status_host,
                port,
                status_port,
                "501",
            );
            assert_eq!(
                dir,
                Path::new("/tmp")
                    .join("501_tidb")
                    .join(encoded)
                    .join("tmp-storage")
            );
        }
    }
}
