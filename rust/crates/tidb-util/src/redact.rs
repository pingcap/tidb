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

//! `pkg/util/redact`: redacting and de-redacting sensitive log content.
//!
//! Faithful adaptations:
//! - The redact modes are `github.com/pingcap/errors`'s string constants
//!   `"ON"`, `"OFF"`, `"MARKER"`; the process-wide enable flag defaults to
//!   the empty string, exactly like `errors.RedactLogEnabled`.
//! - [`de_redact`] reproduces Go's rune-by-rune, line-by-line state machine
//!   (marker `‹`/`›` with doubled delimiters escaped), including its error
//!   on a truncated escape.
//!
//! - [`TaskInfoRedacted`] clones a BR stream-backup task, scrubs exactly the
//!   S3/GCS/Azure credential fields changed by Go, and preserves gogo's
//!   compact protobuf text without mutating the source task.

use tidb_proto::backup;

mod compact_text;

/// `errors.RedactLogEnable`: redaction on, values replaced.
pub const REDACT_LOG_ENABLE: &str = "ON";
/// `errors.RedactLogDisable`: redaction off.
pub const REDACT_LOG_DISABLE: &str = "OFF";
/// `errors.RedactLogMarker`: redaction by wrapping values in `‹...›`.
pub const REDACT_LOG_MARKER: &str = "MARKER";

/// Go `redact.String`: redacts `input` according to `mode`.
///
/// `MARKER` wraps the value in `‹...›`, doubling any interior marker rune;
/// `OFF` returns the input unchanged; `ON` erases it. Any other mode is a
/// programming error and yields the empty string (Go asserts in tests).
#[must_use]
pub fn string(mode: &str, input: &str) -> String {
    match mode {
        "MARKER" => {
            let mut b = String::with_capacity(input.len() + 2);
            b.push('‹');
            for c in input.chars() {
                if c == '‹' || c == '›' {
                    b.push(c);
                    b.push(c);
                } else {
                    b.push(c);
                }
            }
            b.push('›');
            b
        }
        "OFF" => input.to_owned(),
        "ON" => String::new(),
        _ => {
            crate::intest::assert_with_message(false, "invalid redact mode");
            String::new()
        }
    }
}

/// Go `redact.Stringer`: a [`std::fmt::Display`] adapter that redacts the
/// wrapped value's rendering according to `mode`, like [`string`].
pub struct RedactStringer<'a> {
    mode: &'a str,
    inner: &'a dyn std::fmt::Display,
}

/// Go `redact.Stringer`: wraps `input` so its `Display` output is redacted.
#[must_use]
pub fn stringer<'a>(mode: &'a str, input: &'a dyn std::fmt::Display) -> RedactStringer<'a> {
    RedactStringer { mode, inner: input }
}

impl std::fmt::Display for RedactStringer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&string(self.mode, &self.inner.to_string()))
    }
}

/// A truncated marker escape: a `‹` in marker context with no following
/// rune, mirroring the error Go returns from `bufio.Reader.ReadRune`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeRedactError;

impl std::fmt::Display for DeRedactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unexpected EOF in the middle of a redact marker escape")
    }
}

impl std::error::Error for DeRedactError {}

/// Go `redact.DeRedact`: de-redacts marker-wrapped content, working line by
/// line. `remove` replaces each redacted span with `?`; otherwise it unwraps
/// the span. `sep` is written after every scanned line (Go passes `"\n"`).
///
/// This ports Go's `bufio.Scanner`/`bufio.Reader` state machine directly: a
/// line is scanned rune by rune; `‹` opens a span (interior `‹‹`/`››`
/// collapse to one), `›` closes it. An unterminated `‹` is emitted verbatim
/// at end of line (Go writes back the buffered content).
pub fn de_redact(remove: bool, input: &str, sep: &str) -> Result<String, DeRedactError> {
    de_redact_bytes(remove, input.as_bytes(), sep)
}

fn de_redact_bytes(remove: bool, input: &[u8], sep: &str) -> Result<String, DeRedactError> {
    let mut out = String::new();
    for line in scan_lines(input) {
        de_redact_line(remove, &decode_go_utf8(line), &mut out)?;
        out.push_str(sep);
    }
    Ok(out)
}

/// Splits like Go's default `bufio.ScanLines`: on `\n`, dropping a single
/// trailing `\r`, yielding a final unterminated line if non-empty, and
/// silently stopping when a token reaches `bufio.MaxScanTokenSize`. Go's
/// `DeRedact` deliberately does not inspect `Scanner.Err()`.
fn scan_lines(input: &[u8]) -> Vec<&[u8]> {
    const MAX_SCAN_TOKEN_SIZE: usize = 64 * 1024;

    let mut lines = Vec::new();
    let mut start = 0usize;
    while start < input.len() {
        let newline = input[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| start + offset);
        let end = newline.unwrap_or(input.len());
        let raw = &input[start..end];
        if raw.len() >= MAX_SCAN_TOKEN_SIZE {
            break;
        }
        lines.push(raw.strip_suffix(b"\r").unwrap_or(raw));
        let Some(newline) = newline else {
            break;
        };
        start = newline + 1;
    }
    lines
}

/// Go's `bufio.Reader.ReadRune` consumes one byte for each malformed UTF-8
/// rune. `String::from_utf8_lossy` may collapse a longer malformed sequence,
/// so decode explicitly to keep Go's byte advancement.
fn decode_go_utf8(input: &[u8]) -> String {
    let mut decoded = String::new();
    let mut rest = input;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                decoded.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                decoded.push_str(std::str::from_utf8(&rest[..valid]).unwrap_or_default());
                decoded.push('\u{fffd}');
                rest = &rest[valid + 1..];
            }
        }
    }
    decoded
}

/// The per-line core of [`de_redact`], appending to `out`.
fn de_redact_line(remove: bool, line: &str, out: &mut String) -> Result<(), DeRedactError> {
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0usize;
    let mut start = false;
    let mut buf = String::new();

    // Reads the next rune, advancing the cursor; None at end of line (EOF).
    let read = |i: &mut usize| -> Option<char> {
        let c = chars.get(*i).copied();
        if c.is_some() {
            *i += 1;
        }
        c
    };

    while let Some(ch) = read(&mut i) {
        if ch == '‹' {
            if start {
                // Must read the escaped rune; EOF here is Go's error path.
                match read(&mut i) {
                    None => return Err(DeRedactError),
                    Some(pch) if pch == ch => buf.push(ch),
                    Some(pch) => {
                        buf.push(ch);
                        buf.push(pch);
                    }
                }
            } else {
                start = true;
                buf.clear();
            }
        } else if ch == '›' {
            if start {
                // Peek the next rune; a doubled `››` stays inside the span,
                // otherwise the span closes and the rune is un-read.
                let pch = read(&mut i);
                if pch == Some(ch) {
                    buf.push(ch);
                } else {
                    start = false;
                    if pch.is_some() {
                        i -= 1; // unread
                    }
                    if remove {
                        out.push('?');
                    } else {
                        out.push_str(&buf);
                    }
                }
            } else {
                out.push(ch);
            }
        } else if start {
            buf.push(ch);
        } else {
            out.push(ch);
        }
    }
    if start {
        out.push('‹');
        out.push_str(&buf);
    }
    Ok(())
}

/// Go `redact.DeRedactFile`: de-redacts `input` into `output` (a path, or
/// `"-"` for standard output), line by line with `\n` separators.
pub fn de_redact_file(remove: bool, input: &str, output: &str) -> std::io::Result<()> {
    use std::io::{Read, Write};

    let mut input_file = std::fs::File::open(input)?;
    let mut output_file: Box<dyn Write> = if output == "-" {
        Box::new(std::io::stdout())
    } else {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o644);
        }
        Box::new(options.open(output)?)
    };

    let mut content = Vec::new();
    input_file.read_to_end(&mut content)?;
    let result = de_redact_bytes(remove, &content, "\n")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    output_file.write_all(result.as_bytes())
}

/// Go `redact.InitRedact`: sets the process-wide flag to `ON`/`OFF`.
pub fn init_redact(redact_log: bool) {
    let mode = if redact_log {
        REDACT_LOG_ENABLE
    } else {
        REDACT_LOG_DISABLE
    };
    set_redact_mode(mode);
}

/// Publishes the validated `tidb_redact_log` value to the one process-wide
/// redaction authority shared by errors and utility helpers.
pub fn set_redact_mode(mode: &str) {
    let mode = match mode {
        REDACT_LOG_ENABLE => tidb_error::mysql::RedactionMode::Enabled,
        REDACT_LOG_MARKER => tidb_error::mysql::RedactionMode::Marker,
        _ => tidb_error::mysql::RedactionMode::Disabled,
    };
    tidb_error::mysql::set_redaction_mode(mode);
}

/// Go `redact.NeedRedact`: whether redaction is currently enabled (the flag
/// is neither `OFF` nor its uninitialized empty value).
#[must_use]
pub fn need_redact() -> bool {
    tidb_error::mysql::redaction_mode() != tidb_error::mysql::RedactionMode::Disabled
}

/// Go `redact.Value`: `?` when redaction is enabled, else `arg` unchanged.
#[must_use]
pub fn value(arg: &str) -> String {
    if need_redact() {
        "?".to_owned()
    } else {
        arg.to_owned()
    }
}

/// Go `redact.Key`: `?` when redaction is enabled, else the upper-case hex
/// encoding of `key` (`strings.ToUpper(hex.EncodeToString(key))`).
#[must_use]
pub fn key(key: &[u8]) -> String {
    if need_redact() {
        return "?".to_owned();
    }
    let mut s = String::with_capacity(key.len() * 2);
    for b in key {
        // Upper-case hex, matching Go's ToUpper(EncodeToString(...)).
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// Go `redact.WriteRedact`: appends `v` to `build`, redacted per `redact`.
///
/// `MARKER` wraps `v` in `‹...›`; `ON` writes `?`; anything else writes `v`.
pub fn write_redact(build: &mut String, v: &str, redact: &str) {
    if redact == REDACT_LOG_MARKER {
        build.push('‹');
        build.push_str(v);
        build.push('›');
    } else if redact == REDACT_LOG_ENABLE {
        build.push('?');
    } else {
        build.push_str(v);
    }
}

/// Go `redact.TaskInfoRedacted`: a display wrapper for a BR stream-backup
/// task whose storage credentials are scrubbed without mutating the task.
#[derive(Debug, Clone, Copy)]
pub struct TaskInfoRedacted<'a> {
    /// The task to render. `None` is Go's nil `Info` pointer.
    pub info: Option<&'a backup::StreamBackupTaskInfo>,
}

impl std::fmt::Display for TaskInfoRedacted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(info) = self.info else {
            return f.write_str("nil");
        };

        let mut redacted = info.clone();
        if let Some(storage) = &mut redacted.storage {
            use backup::storage_backend::Backend;

            match &mut storage.backend {
                Some(Backend::S3(s3)) => {
                    s3.access_key = "[REDACTED]".to_owned();
                    s3.secret_access_key = "[REDACTED]".to_owned();
                    s3.sse_kms_key_id = "[REDACTED]".to_owned();
                }
                Some(Backend::Gcs(gcs)) => {
                    gcs.credentials_blob = "[REDACTED]".to_owned();
                }
                Some(Backend::AzureBlobStorage(azure)) => {
                    azure.shared_key = "[REDACTED]".to_owned();
                    azure.access_sig = "[REDACTED]".to_owned();
                    azure.encryption_key = Some(backup::AzureCustomerKey {
                        encryption_key: "[REDACTED]".to_owned(),
                        ..Default::default()
                    });
                }
                _ => {}
            }
        }

        f.write_str(&compact_text::stream_backup_task_info(&redacted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn de_redact_keeps_go_scanner_and_invalid_utf8_behavior() {
        let accepted = format!("{}\nTAIL", "x".repeat(65_535));
        let output = de_redact(false, &accepted, "|").unwrap();
        assert_eq!(output.len(), 65_541);
        assert!(output.ends_with("xx|TAIL|"));

        let rejected = format!("{}\nTAIL", "x".repeat(65_536));
        assert_eq!(de_redact(false, &rejected, "|").unwrap(), "");

        assert_eq!(
            de_redact_bytes(false, &[b'a', 0xff, 0xfe, b'b'], "").unwrap(),
            "a\u{fffd}\u{fffd}b"
        );
    }

    #[test]
    fn de_redact_file_accepts_go_style_invalid_utf8() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.log");
        let output = directory.path().join("output.log");
        std::fs::write(&input, [b'a', 0xff, 0xfe, b'b']).unwrap();

        de_redact_file(false, input.to_str().unwrap(), output.to_str().unwrap()).unwrap();
        assert_eq!(
            std::fs::read(output).unwrap(),
            "a\u{fffd}\u{fffd}b\n".as_bytes()
        );
    }

    #[test]
    fn de_redact_file_keeps_go_same_path_open_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("same.log");
        std::fs::write(&path, "‹secret›").unwrap();

        de_redact_file(false, path.to_str().unwrap(), path.to_str().unwrap()).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"");
    }

    // Cross-check with tidb-error's shared flag (extension beyond the Go
    // test): Go's helpers and SQL-error formatting read the same
    // `errors.RedactLogEnabled` singleton, so a mode written through that
    // owner must be visible here too, including MARKER (which `NeedRedact`
    // treats as enabled).
    #[test]
    fn redact_init_visible_via_tidb_error_mode() {
        let secret = "secret";
        init_redact(false);
        tidb_error::mysql::set_redaction_mode(tidb_error::mysql::RedactionMode::Marker);
        assert!(need_redact());
        assert_eq!(value(secret), "?");
        assert_eq!(key(secret.as_bytes()), "?");

        init_redact(false);
    }

    // WriteRedact mirrors String's three branches.
    #[test]
    fn write_redact_modes() {
        let mut b = String::new();
        write_redact(&mut b, "v", REDACT_LOG_MARKER);
        assert_eq!(b, "‹v›");
        let mut b = String::new();
        write_redact(&mut b, "v", REDACT_LOG_ENABLE);
        assert_eq!(b, "?");
        let mut b = String::new();
        write_redact(&mut b, "v", REDACT_LOG_DISABLE);
        assert_eq!(b, "v");
    }

    // BR TestRedactBackend: exact compact protobuf text plus the source
    // object's non-mutation contract for every credential-bearing backend.
    #[test]
    fn task_info_redacts_backends_without_mutating_source() {
        use tidb_proto::backup::{
            storage_backend, AzureBlobStorage, AzureCustomerKey, Gcs, StorageBackend,
            StreamBackupTaskInfo, S3,
        };

        let mut info = StreamBackupTaskInfo {
            name: "test".to_owned(),
            storage: Some(StorageBackend {
                backend: Some(storage_backend::Backend::S3(S3 {
                    endpoint: "http://".to_owned(),
                    bucket: "test".to_owned(),
                    prefix: "test".to_owned(),
                    access_key: "12abCD!@#[]{}?/\\".to_owned(),
                    secret_access_key: "12abCD!@#[]{}?/\\".to_owned(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        let original = info.clone();
        assert_eq!(
            TaskInfoRedacted { info: Some(&info) }.to_string(),
            "storage:<s3:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" access_key:\"[REDACTED]\" secret_access_key:\"[REDACTED]\" sse_kms_key_id:\"[REDACTED]\" > > name:\"test\" "
        );
        assert_eq!(info, original);

        info.storage = Some(StorageBackend {
            backend: Some(storage_backend::Backend::Gcs(Gcs {
                endpoint: "http://".to_owned(),
                bucket: "test".to_owned(),
                prefix: "test".to_owned(),
                credentials_blob: "12abCD!@#[]{}?/\\".to_owned(),
                ..Default::default()
            })),
        });
        let original = info.clone();
        assert_eq!(
            TaskInfoRedacted { info: Some(&info) }.to_string(),
            "storage:<gcs:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" credentials_blob:\"[REDACTED]\" > > name:\"test\" "
        );
        assert_eq!(info, original);

        info.storage = Some(StorageBackend {
            backend: Some(storage_backend::Backend::AzureBlobStorage(
                AzureBlobStorage {
                    endpoint: "http://".to_owned(),
                    bucket: "test".to_owned(),
                    prefix: "test".to_owned(),
                    shared_key: "12abCD!@#[]{}?/\\".to_owned(),
                    access_sig: "12abCD!@#[]{}?/\\".to_owned(),
                    encryption_key: Some(AzureCustomerKey {
                        encryption_key: "12abCD!@#[]{}?/\\".to_owned(),
                        encryption_key_sha256: "12abCD!@#[]{}?/\\".to_owned(),
                    }),
                    ..Default::default()
                },
            )),
        });
        let original = info.clone();
        assert_eq!(
            TaskInfoRedacted { info: Some(&info) }.to_string(),
            "storage:<azure_blob_storage:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" shared_key:\"[REDACTED]\" access_sig:\"[REDACTED]\" encryption_key:<encryption_key:\"[REDACTED]\" > > > name:\"test\" "
        );
        assert_eq!(info, original);
    }

    #[test]
    fn task_info_preserves_noncredential_backends_and_compact_text_rules() {
        use std::collections::HashMap;

        use tidb_proto::backup::{
            storage_backend, Bucket, CloudDynamic, Hdfs, Local, Noop, StorageBackend,
            StreamBackupTaskInfo,
        };

        assert_eq!(TaskInfoRedacted { info: None }.to_string(), "nil");

        let cases = [
            (
                storage_backend::Backend::Noop(Noop {}),
                r#"storage:<noop:<> > "#,
            ),
            (
                storage_backend::Backend::Local(Local {
                    path: "a\n\"\\\x01".to_owned(),
                }),
                r#"storage:<local:<path:"a\n\"\\\001" > > "#,
            ),
            (
                storage_backend::Backend::CloudDynamic(CloudDynamic {
                    bucket: Some(Bucket {
                        endpoint: "e".to_owned(),
                        ..Default::default()
                    }),
                    provider_name: "p".to_owned(),
                    attrs: HashMap::from([
                        ("z".to_owned(), "2".to_owned()),
                        ("a".to_owned(), "1".to_owned()),
                    ]),
                }),
                r#"storage:<cloud_dynamic:<bucket:<endpoint:"e" > provider_name:"p" attrs:<key:"a" value:"1" > attrs:<key:"z" value:"2" > > > "#,
            ),
            (
                storage_backend::Backend::Hdfs(Hdfs {
                    remote: "hdfs:///x".to_owned(),
                }),
                r#"storage:<hdfs:<remote:"hdfs:///x" > > "#,
            ),
        ];

        for (backend, expected) in cases {
            let info = StreamBackupTaskInfo {
                storage: Some(StorageBackend {
                    backend: Some(backend),
                }),
                ..Default::default()
            };
            let original = info.clone();
            assert_eq!(TaskInfoRedacted { info: Some(&info) }.to_string(), expected);
            assert_eq!(info, original);
        }
    }

    #[test]
    fn task_info_preserves_dependency_closed_security_config() {
        use tidb_proto::{
            backup::{
                stream_backup_task_security_config, CipherInfo, CompressionType, MasterKeyConfig,
                StreamBackupTaskInfo, StreamBackupTaskSecurityConfig,
            },
            encryptionpb::{
                master_key, AwsKms, AzureKms, EncryptionMethod, GcpKms, MasterKey, MasterKeyFile,
                MasterKeyKms, MasterKeyPlaintext,
            },
        };

        let plain = StreamBackupTaskInfo {
            start_ts: 1,
            end_ts: 2,
            name: "n".to_owned(),
            table_filter: vec!["a".to_owned(), "b".to_owned()],
            compression_type: CompressionType::Zstd as i32,
            security_config: Some(StreamBackupTaskSecurityConfig {
                encryption: Some(
                    stream_backup_task_security_config::Encryption::PlaintextDataKey(CipherInfo {
                        cipher_type: EncryptionMethod::Aes256Ctr as i32,
                        cipher_key: vec![0, b'\n', b'\\', b'"', 0xff],
                    }),
                ),
            }),
            ..Default::default()
        };
        assert_eq!(
            TaskInfoRedacted { info: Some(&plain) }.to_string(),
            r#"start_ts:1 end_ts:2 name:"n" table_filter:"a" table_filter:"b" compression_type:ZSTDZSTD security_config:<plaintext_data_key:<cipher_type:AES256_CTRAES256_CTR cipher_key:"\000\n\\\"\377" > > "#
        );

        let master = StreamBackupTaskInfo {
            security_config: Some(StreamBackupTaskSecurityConfig {
                encryption: Some(
                    stream_backup_task_security_config::Encryption::MasterKeyConfig(
                        MasterKeyConfig {
                            encryption_type: EncryptionMethod::Aes128Ctr as i32,
                            master_keys: vec![
                                MasterKey {
                                    backend: Some(master_key::Backend::Plaintext(
                                        MasterKeyPlaintext {},
                                    )),
                                },
                                MasterKey {
                                    backend: Some(master_key::Backend::File(MasterKeyFile {
                                        path: "/k".to_owned(),
                                    })),
                                },
                                MasterKey {
                                    backend: Some(master_key::Backend::Kms(Box::new(
                                        MasterKeyKms {
                                            vendor: "v".to_owned(),
                                            key_id: "id".to_owned(),
                                            azure_kms: Some(AzureKms {
                                                tenant_id: "t".to_owned(),
                                                ..Default::default()
                                            }),
                                            gcp_kms: Some(GcpKms {
                                                credential: "c".to_owned(),
                                            }),
                                            aws_kms: Some(AwsKms {
                                                access_key: "a".to_owned(),
                                                secret_access_key: "s".to_owned(),
                                            }),
                                            ..Default::default()
                                        },
                                    ))),
                                },
                            ],
                        },
                    ),
                ),
            }),
            ..Default::default()
        };
        assert_eq!(
            TaskInfoRedacted {
                info: Some(&master)
            }
            .to_string(),
            r#"security_config:<master_key_config:<encryption_type:AES128_CTRAES128_CTR master_keys:<plaintext:<> > master_keys:<file:<path:"/k" > > master_keys:<kms:<vendor:"v" key_id:"id" azure_kms:<tenant_id:"t" > gcp_kms:<credential:"c" > aws_kms:<access_key:"a" secret_access_key:"s" > > > > > "#
        );
    }

    #[test]
    fn task_info_matches_go_all_fields_goldens() {
        use std::collections::HashMap;

        use tidb_proto::{
            backup::{
                storage_backend, stream_backup_task_security_config, AzureBlobStorage,
                AzureCustomerKey, Bucket, CloudDynamic, Gcs, MasterKeyConfig, StorageBackend,
                StreamBackupTaskInfo, StreamBackupTaskSecurityConfig, S3,
            },
            encryptionpb::{
                master_key, AwsKms, AzureKms, EncryptionMethod, GcpKms, MasterKey, MasterKeyKms,
            },
        };

        fn check(info: &StreamBackupTaskInfo, expected: &str) {
            let original = info.clone();
            assert_eq!(TaskInfoRedacted { info: Some(info) }.to_string(), expected);
            assert_eq!(*info, original);
        }

        let s3 = StreamBackupTaskInfo {
            storage: Some(StorageBackend {
                backend: Some(storage_backend::Backend::S3(S3 {
                    endpoint: "1".to_owned(),
                    region: "2".to_owned(),
                    bucket: "3".to_owned(),
                    prefix: "4".to_owned(),
                    storage_class: "5".to_owned(),
                    sse: "6".to_owned(),
                    acl: "7".to_owned(),
                    access_key: "8".to_owned(),
                    secret_access_key: "9".to_owned(),
                    force_path_style: true,
                    sse_kms_key_id: "11".to_owned(),
                    role_arn: "12".to_owned(),
                    external_id: "13".to_owned(),
                    object_lock_enabled: true,
                    session_token: "15".to_owned(),
                    provider: "16".to_owned(),
                    profile: "17".to_owned(),
                })),
            }),
            ..Default::default()
        };
        check(
            &s3,
            r#"storage:<s3:<endpoint:"1" region:"2" bucket:"3" prefix:"4" storage_class:"5" sse:"6" acl:"7" access_key:"[REDACTED]" secret_access_key:"[REDACTED]" force_path_style:true sse_kms_key_id:"[REDACTED]" role_arn:"12" external_id:"13" object_lock_enabled:true session_token:"15" provider:"16" profile:"17" > > "#,
        );

        let gcs = StreamBackupTaskInfo {
            storage: Some(StorageBackend {
                backend: Some(storage_backend::Backend::Gcs(Gcs {
                    endpoint: "1".to_owned(),
                    bucket: "2".to_owned(),
                    prefix: "3".to_owned(),
                    storage_class: "4".to_owned(),
                    predefined_acl: "5".to_owned(),
                    credentials_blob: "6".to_owned(),
                })),
            }),
            ..Default::default()
        };
        check(
            &gcs,
            r#"storage:<gcs:<endpoint:"1" bucket:"2" prefix:"3" storage_class:"4" predefined_acl:"5" credentials_blob:"[REDACTED]" > > "#,
        );

        let azure = StreamBackupTaskInfo {
            storage: Some(StorageBackend {
                backend: Some(storage_backend::Backend::AzureBlobStorage(
                    AzureBlobStorage {
                        endpoint: "1".to_owned(),
                        bucket: "2".to_owned(),
                        prefix: "3".to_owned(),
                        storage_class: "4".to_owned(),
                        account_name: "5".to_owned(),
                        shared_key: "6".to_owned(),
                        access_sig: "8".to_owned(),
                        encryption_scope: "9".to_owned(),
                        encryption_key: Some(AzureCustomerKey {
                            encryption_key: "10a".to_owned(),
                            encryption_key_sha256: "10b".to_owned(),
                        }),
                    },
                )),
            }),
            ..Default::default()
        };
        check(
            &azure,
            r#"storage:<azure_blob_storage:<endpoint:"1" bucket:"2" prefix:"3" storage_class:"4" account_name:"5" shared_key:"[REDACTED]" access_sig:"[REDACTED]" encryption_scope:"9" encryption_key:<encryption_key:"[REDACTED]" > > > "#,
        );

        let cloud = StreamBackupTaskInfo {
            storage: Some(StorageBackend {
                backend: Some(storage_backend::Backend::CloudDynamic(CloudDynamic {
                    bucket: Some(Bucket {
                        endpoint: "1".to_owned(),
                        region: "3".to_owned(),
                        bucket: "4".to_owned(),
                        prefix: "5".to_owned(),
                        storage_class: "6".to_owned(),
                    }),
                    provider_name: "2".to_owned(),
                    attrs: HashMap::from([("k".to_owned(), "v".to_owned())]),
                })),
            }),
            ..Default::default()
        };
        check(
            &cloud,
            r#"storage:<cloud_dynamic:<bucket:<endpoint:"1" region:"3" bucket:"4" prefix:"5" storage_class:"6" > provider_name:"2" attrs:<key:"k" value:"v" > > > "#,
        );

        let kms = StreamBackupTaskInfo {
            security_config: Some(StreamBackupTaskSecurityConfig {
                encryption: Some(
                    stream_backup_task_security_config::Encryption::MasterKeyConfig(
                        MasterKeyConfig {
                            encryption_type: EncryptionMethod::Sm4Ctr as i32,
                            master_keys: vec![MasterKey {
                                backend: Some(master_key::Backend::Kms(Box::new(MasterKeyKms {
                                    vendor: "1".to_owned(),
                                    key_id: "2".to_owned(),
                                    region: "3".to_owned(),
                                    endpoint: "4".to_owned(),
                                    azure_kms: Some(AzureKms {
                                        tenant_id: "1".to_owned(),
                                        client_id: "2".to_owned(),
                                        client_secret: "3".to_owned(),
                                        key_vault_url: "4".to_owned(),
                                        hsm_name: "5".to_owned(),
                                        hsm_url: "6".to_owned(),
                                        client_certificate: "7".to_owned(),
                                        client_certificate_path: "8".to_owned(),
                                        client_certificate_password: "9".to_owned(),
                                    }),
                                    gcp_kms: Some(GcpKms {
                                        credential: "6".to_owned(),
                                    }),
                                    aws_kms: Some(AwsKms {
                                        access_key: "7".to_owned(),
                                        secret_access_key: "8".to_owned(),
                                    }),
                                }))),
                            }],
                        },
                    ),
                ),
            }),
            ..Default::default()
        };
        check(
            &kms,
            r#"security_config:<master_key_config:<encryption_type:SM4_CTRSM4_CTR master_keys:<kms:<vendor:"1" key_id:"2" region:"3" endpoint:"4" azure_kms:<tenant_id:"1" client_id:"2" client_secret:"3" key_vault_url:"4" hsm_name:"5" hsm_url:"6" client_certificate:"7" client_certificate_path:"8" client_certificate_password:"9" > gcp_kms:<credential:"6" > aws_kms:<access_key:"7" secret_access_key:"8" > > > > > "#,
        );
    }
}
