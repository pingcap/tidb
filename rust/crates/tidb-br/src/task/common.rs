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

//! Go `br/pkg/task/common.go`: BRIE's common task settings and key reader.
//! Fields not reached by the SQL builder and CLI flag parsing remain unported.

use tidb_proto::backup::CipherInfo;

/// Go `task.TLSConfig`.
#[derive(Default)]
pub struct TLSConfig {
    /// Go `CA`.
    pub ca: String,
    /// Go `Cert`.
    pub cert: String,
    /// Go `Key` (path only).
    pub key: String,
}

/// Go `task.Config`, fields consumed by `executorBuilder.buildBRIE`.
/// Deliberately no Debug: Storage and CipherInfo can contain credentials.
pub struct Config {
    /// Go `Storage` (private task input, never SHOW output).
    pub storage: String,
    /// Go `PD`.
    pub pd: Vec<String>,
    /// Go `TLS`.
    pub tls: TLSConfig,
    /// Go `RateLimit`, bytes per second.
    pub rate_limit: u64,
    /// Go `Concurrency`.
    pub concurrency: u32,
    /// Go `Checksum`.
    pub checksum: bool,
    /// Go `ChecksumConcurrency`.
    pub checksum_concurrency: usize,
    /// Go `SendCreds`.
    pub send_creds: bool,
    /// Go `CipherInfo`.
    pub cipher_info: CipherInfo,
    /// Go `FilterStr`.
    pub filter_str: Vec<String>,
    /// Go `TableFilter`.
    pub table_filter: Box<dyn tidb_table_filter::Filter>,
}

/// Go `DefaultConfig`'s SQL-builder fields. Values from `TestDefault`.
pub fn default_config() -> Config {
    Config {
        storage: String::new(),
        pd: vec!["127.0.0.1:2379".into()],
        tls: TLSConfig::default(),
        rate_limit: 0,
        concurrency: 0,
        checksum: false,
        checksum_concurrency: 4,
        send_creds: true,
        cipher_info: CipherInfo {
            cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
            ..Default::default()
        },
        filter_str: vec!["*.*".into()],
        table_filter: tidb_table_filter::case_insensitive(tidb_table_filter::all()),
    }
}

/// Go `checkCipherKey`.
fn check_cipher_key(key: &str, file: &str) -> Result<(), String> {
    if key.is_empty() == file.is_empty() {
        Err("exactly one of cipher key or keyfile path should be provided: [BR:Common:ErrInvalidArgument]invalid argument".into())
    } else {
        Ok(())
    }
}

/// Go `GetCipherKeyContent`. Decode errors never include the key.
pub fn get_cipher_key_content(key: &str, file: &str) -> Result<Vec<u8>, String> {
    check_cipher_key(key, file)?;
    let data = if key.is_empty() {
        let mut data =
            std::fs::read(file).map_err(|err| format!("failed to read cipher file: {err}"))?;
        if data.last() == Some(&b'\n') {
            data.pop();
        }
        data
    } else {
        key.as_bytes().to_vec()
    };
    hex::decode(data).map_err(|_| "cipher key must be a valid hexadecimal string: [BR:Common:ErrInvalidArgument]invalid argument".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestCheckCipherKey`.
    #[test]
    fn test_check_cipher_key() {
        for (key, file, ok) in [
            ("0123456789abcdef0123456789abcdef", "", true),
            ("0123456789abcdef0123456789abcdef", "/tmp/abc", false),
            ("", "/tmp/abc", true),
            ("", "", false),
        ] {
            assert_eq!(check_cipher_key(key, file).is_ok(), ok);
        }
    }
    /// Go `TestGetCipherKey`.
    #[test]
    fn test_get_cipher_key() {
        assert!(get_cipher_key_content("this is not a hex string", "")
            .unwrap_err()
            .contains("cipher key must be a valid hexadecimal string"));
    }
    /// Go `TestDefault`, fields used by the SQL builder (no CLI flag parser).
    #[test]
    fn test_default() {
        let cfg = default_config();
        assert_eq!(cfg.pd, ["127.0.0.1:2379"]);
        assert!(!cfg.checksum);
        assert!(cfg.send_creds);
        assert_eq!(cfg.concurrency, 0);
        assert_eq!(cfg.checksum_concurrency, 4);
        assert_eq!(cfg.rate_limit, 0);
        assert_eq!(cfg.cipher_info.cipher_type, 1);
        assert_eq!(cfg.filter_str, ["*.*"]);
        assert!(cfg.table_filter.match_table("test", "a"));
    }
}
