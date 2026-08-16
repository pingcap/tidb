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

//! Go `br/pkg/encryption/master_key/file_backend.go`: a master key read from a
//! file on disk, ported from TiKV's `FileBackend`. The file must hold exactly
//! the hex-encoded 32-byte key plus a trailing newline — 65 bytes, no more, no
//! less, which is what makes a truncated or reformatted key file a hard error
//! rather than a silent wrong-key decryption.

use std::path::Path;

use super::common::Iv;
use super::mem_backend::MemAesGcmBackend;
use super::pb::EncryptedContent;
use super::Backend;

/// Go `AesGcmKeyLen`: the AES-256 key length.
pub const AES_GCM_KEY_LEN: usize = 32;

/// Go `FileBackend`.
pub struct FileBackend {
    mem_cache: MemAesGcmBackend,
}

impl FileBackend {
    /// The wrapped in-memory backend. Go's field is package-private and its
    /// test reaches straight into it; this accessor is that reach.
    pub fn mem_cache(&self) -> &MemAesGcmBackend {
        &self.mem_cache
    }

    /// Go `Encrypt`: seals with a fresh random GCM IV.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent, String> {
        let iv = Iv::new_gcm()?;
        self.mem_cache.encrypt_content(plaintext, &iv)
    }
}

impl Backend for FileBackend {
    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>, String> {
        self.mem_cache.decrypt_content(content)
    }

    fn close(&self) {
        // nothing to close
    }
}

/// Go `createFileBackend`.
pub fn create_file_backend(key_path: &Path) -> Result<FileBackend, String> {
    // FileBackend uses AES-256-GCM.
    let key_len = AES_GCM_KEY_LEN;

    let content = std::fs::read(key_path)
        .map_err(|error| format!("failed to read master key file from disk: {error}"))?;

    let file_len = content.len();
    let expected_len = key_len * 2 + 1; // hex-encoded key + newline

    if file_len != expected_len {
        return Err(format!(
            "mismatch master key file size, expected {expected_len}, actual {file_len}"
        ));
    }
    if content[file_len - 1] != b'\n' {
        return Err("master key file should end with newline".to_owned());
    }

    let key = decode_hex(&content[..file_len - 1])
        .map_err(|error| format!("failed to decode hex format master key from file: {error}"))?;

    let backend = MemAesGcmBackend::new(&key)
        .map_err(|error| format!("failed to create MemAesGcmBackend: {error}"))?;

    Ok(FileBackend { mem_cache: backend })
}

/// Go `encoding/hex.DecodeString`, narrowed to what the key file needs.
fn decode_hex(text: &[u8]) -> Result<Vec<u8>, String> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if !text.len().is_multiple_of(2) {
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    text.chunks_exact(2)
        .map(|pair| match (digit(pair[0]), digit(pair[1])) {
            (Some(high), Some(low)) => Ok(high << 4 | low),
            _ => Err("encoding/hex: invalid byte".to_owned()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::super::common::METADATA_KEY_AES_GCM_TAG;
    use super::super::mem_backend::{GCM_TAG_NOT_FOUND, WRONG_MASTER_KEY};
    use super::*;

    fn unhex(text: &str) -> Vec<u8> {
        decode_hex(text.as_bytes()).expect("hex")
    }

    /// Go `createMasterKeyFile` plus its `TempKeyFile` wrapper; `tempfile`'s
    /// `NamedTempFile` already removes the file on drop, so `Cleanup` has no
    /// Rust counterpart.
    fn create_master_key_file() -> tempfile::NamedTempFile {
        let mut file = tempfile::Builder::new()
            .prefix("test_key_")
            .suffix(".txt")
            .tempfile()
            .expect("temp file");
        file.write_all(b"c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139\n")
            .expect("write key");
        file.flush().expect("flush");
        file
    }

    /// Go `TestFileBackendAes256Gcm`.
    #[test]
    fn test_file_backend_aes256_gcm() {
        let pt = unhex("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749");
        let ct = unhex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980");
        let iv_bytes = unhex("cafabd9672ca6c79a2fbdc22");

        let temp_key_file = create_master_key_file();
        let backend = create_file_backend(temp_key_file.path()).unwrap();

        let iv = Iv::from_slice(&iv_bytes).unwrap();
        let encrypted_content = backend.mem_cache().encrypt_content(&pt, &iv).unwrap();
        assert_eq!(encrypted_content.content, ct);

        let plaintext = backend.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);
    }

    /// Go `TestFileBackendAuthenticate`.
    #[test]
    fn test_file_backend_authenticate() {
        let pt = [1u8, 2, 3];

        let temp_key_file = create_master_key_file();
        let backend = create_file_backend(temp_key_file.path()).unwrap();

        let encrypted_content = backend.encrypt(&pt).unwrap();
        let plaintext = backend.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        // Test checksum mismatch
        let mut encrypted_content1 = encrypted_content.clone();
        encrypted_content1
            .metadata
            .get_mut(METADATA_KEY_AES_GCM_TAG)
            .unwrap()[0] ^= 0xFF;
        let error = backend.decrypt(&encrypted_content1).unwrap_err();
        assert!(error.contains(WRONG_MASTER_KEY), "{error}");

        // Test checksum not found
        let mut encrypted_content2 = encrypted_content.clone();
        encrypted_content2.metadata.remove(METADATA_KEY_AES_GCM_TAG);
        let error = backend.decrypt(&encrypted_content2).unwrap_err();
        assert!(error.contains(GCM_TAG_NOT_FOUND), "{error}");

        backend.close();
    }

    /// `unwrap_err` needs `Debug` on the success type, which the backends do
    /// not implement; this extracts the message instead.
    fn error_of<T>(result: Result<T, String>) -> String {
        result.err().expect("expected an error")
    }

    /// Not in the Go package's test set: every rejection `createFileBackend`
    /// can raise, since a mis-shaped key file must never fall through to a
    /// wrong-key decryption.
    #[test]
    fn test_create_file_backend_rejections() {
        fn write(contents: &[u8]) -> tempfile::NamedTempFile {
            let mut file = tempfile::NamedTempFile::new().expect("temp file");
            file.write_all(contents).expect("write");
            file.flush().expect("flush");
            file
        }

        let missing = std::path::Path::new("/nonexistent/br-master-key");
        assert!(error_of(create_file_backend(missing))
            .starts_with("failed to read master key file from disk"));

        let short = write(b"c3d99825\n");
        assert_eq!(
            error_of(create_file_backend(short.path())),
            "mismatch master key file size, expected 65, actual 9"
        );

        // Right length, no trailing newline.
        let no_newline =
            write(b"c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc01290911390");
        assert_eq!(
            error_of(create_file_backend(no_newline.path())),
            "master key file should end with newline"
        );

        // Right length and newline, but not hex.
        let not_hex = write(b"zzd99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139\n");
        assert!(error_of(create_file_backend(not_hex.path()))
            .starts_with("failed to decode hex format master key from file"));
    }
}
