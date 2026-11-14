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

//! Go `pkg/objstore` local backend used by BR metadata reading.
//! Remote backend construction is not implemented here.

use std::path::PathBuf;

/// Go `objstore.LocalStorage`.
#[derive(Clone, Debug)]
pub struct LocalStorage {
    base: PathBuf,
}

impl LocalStorage {
    /// Go `objstore.ParseBackend`'s empty/local/file scheme arms followed by
    /// `NewLocalStorage`. No URL or query is included in errors.
    pub fn from_url(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("empty store is not allowed".into());
        }
        let base = if let Some((scheme, tail)) = raw.split_once(':') {
            if !scheme.contains('/') {
                if !matches!(scheme.to_ascii_lowercase().as_str(), "local" | "file") {
                    return Err(
                        "BR metadata storage backend is not implemented (expected local or file)"
                            .into(),
                    );
                }
                let path = tail.split(['?', '#']).next().unwrap_or("");
                let path = if let Some(authority) = path.strip_prefix("//") {
                    authority.find('/').map_or("", |i| &authority[i..])
                } else if path.starts_with('/') {
                    path
                } else {
                    // Go net/url puts this in Opaque, not Path.
                    ""
                };
                let mut decoded = Vec::with_capacity(path.len());
                let mut bytes = path.bytes();
                while let Some(b) = bytes.next() {
                    if b == b'%' {
                        let hi = bytes.next().and_then(|b| (b as char).to_digit(16));
                        let lo = bytes.next().and_then(|b| (b as char).to_digit(16));
                        let (Some(hi), Some(lo)) = (hi, lo) else {
                            return Err("invalid destination URL escape".into());
                        };
                        decoded.push((hi * 16 + lo) as u8);
                    } else {
                        decoded.push(b);
                    }
                }
                use std::os::unix::ffi::OsStringExt;
                PathBuf::from(std::ffi::OsString::from_vec(decoded))
            } else {
                std::path::absolute(raw).map_err(|e| e.to_string())?
            }
        } else {
            std::path::absolute(raw).map_err(|e| e.to_string())?
        };
        if !base.is_dir() {
            std::fs::create_dir_all(&base).map_err(|e| e.to_string())?;
        }
        Ok(Self { base })
    }

    /// Go `LocalStorage.ReadFile`.
    pub fn read_file(&self, name: &str) -> Result<Vec<u8>, String> {
        std::fs::read(self.path(name)).map_err(|e| e.to_string())
    }

    /// Go `filepath.Join(l.base, name)`: a leading slash in name does not
    /// discard the base, unlike Rust's Path::join.
    fn path(&self, name: &str) -> PathBuf {
        self.base.join(name.trim_start_matches('/'))
    }

    /// Go `LocalStorage.WriteFile`: publish only after the temporary write
    /// completes. Error messages never contain storage credentials.
    pub fn write_file(&self, name: &str, data: &[u8]) -> Result<(), String> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        if let Some(value) =
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/objstore/local_write_file_err")
        {
            return Err(value.string());
        }
        let path = self.path(name);
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|e| e.to_string())?;
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(format!(".tmp.{}", hex::encode(random)));
        let tmp = PathBuf::from(tmp);
        let open = || {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o644)
                .open(&tmp)
        };
        let mut file = match open() {
            Ok(file) => file,
            Err(error) => {
                let parent = tmp.parent().unwrap();
                if parent.try_exists().map_err(|e| e.to_string())? {
                    return Err(error.to_string());
                }
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                open().map_err(|e| e.to_string())?
            }
        };
        file.write_all(data).map_err(|e| e.to_string())?;
        drop(file);
        std::fs::rename(tmp, path).map_err(|e| e.to_string())
    }

    /// Go `LocalStorage.FileExists` / `pathExists`.
    pub fn file_exists(&self, name: &str) -> Result<bool, String> {
        self.path(name).try_exists().map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `pkg/objstore/parse_test.go:TestCreateStorage`, local/file cases.
    #[test]
    fn test_create_storage() {
        let dir = tempfile::tempdir().unwrap();
        for scheme in ["local", "file"] {
            let storage =
                LocalStorage::from_url(&format!("{scheme}://{}", dir.path().display())).unwrap();
            assert_eq!(storage.base, dir.path());
        }
    }
}
