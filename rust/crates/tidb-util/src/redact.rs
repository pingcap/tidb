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
//! - [`de_redact`] reproduces Go's reader/writer, rune-by-rune, line-by-line state machine
//!   (marker `‹`/`›` with doubled delimiters escaped), including its error
//!   on a truncated escape.
//!
//! - [`TaskInfoRedacted`] clones a BR stream-backup task, scrubs exactly the
//!   S3/GCS/Azure credential fields changed by Go, and preserves gogo's
//!   compact protobuf text without mutating the source task.

use tidb_proto::backup;

mod compact_text;

/// Go `redact.String`: redacts `input` according to `mode`.
///
/// `MARKER` wraps the value in `‹...›`, doubling any interior marker rune;
/// `OFF` returns the input unchanged; `ON` erases it. Any other mode is a
/// programming error and yields the empty string (Go asserts in tests).
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
struct RedactStringer<'a> {
    mode: &'a str,
    inner: &'a dyn std::fmt::Display,
}

/// Go `redact.Stringer`: wraps `input` so its `Display` output is redacted.
pub fn stringer<'a>(
    mode: &'a str,
    input: &'a dyn std::fmt::Display,
) -> impl std::fmt::Display + 'a {
    RedactStringer { mode, inner: input }
}

impl std::fmt::Display for RedactStringer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&string(self.mode, &self.inner.to_string()))
    }
}

/// Go `redact.DeRedact`: de-redacts marker-wrapped content from `input` into
/// `output`, working line by line. `remove` replaces each redacted span with
/// `?`; otherwise it unwraps the span. `sep` is written after every scanned
/// line (Go passes `"\n"`).
///
/// This ports Go's `bufio.Scanner`/`bufio.Reader` state machine directly: a
/// line is scanned rune by rune; `‹` opens a span (interior `‹‹`/`››`
/// collapse to one), `›` closes it. An unterminated `‹` is emitted verbatim
/// at end of line (Go writes back the buffered content).
pub fn de_redact(
    remove: bool,
    input: impl std::io::Read,
    output: impl std::io::Write,
    sep: &str,
) -> std::io::Result<()> {
    use std::io::BufRead;

    const MAX_SCAN_TOKEN_SIZE: usize = 64 * 1024;

    let mut input = std::io::BufReader::new(input);
    let mut output = std::io::BufWriter::new(output);
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = match input.read_until(b'\n', &mut line) {
            Ok(read) => read,
            // Go deliberately does not inspect Scanner.Err().
            Err(_) => break,
        };
        if read == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        if line.len() >= MAX_SCAN_TOKEN_SIZE {
            break;
        };
        de_redact_line(remove, &decode_go_utf8(&line), &mut output)?;
        let _ = std::io::Write::write_all(&mut output, sep.as_bytes());
    }
    let _ = std::io::Write::flush(&mut output);
    Ok(())
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
fn de_redact_line(remove: bool, line: &str, out: &mut impl std::io::Write) -> std::io::Result<()> {
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
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "unexpected EOF in the middle of a redact marker escape",
                        ));
                    }
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
                        let _ = std::io::Write::write_all(out, b"?");
                    } else {
                        std::io::Write::write_all(out, buf.as_bytes())?;
                    }
                }
            } else {
                let mut encoded = [0; 4];
                let _ = std::io::Write::write_all(out, ch.encode_utf8(&mut encoded).as_bytes());
            }
        } else if start {
            buf.push(ch);
        } else {
            let mut encoded = [0; 4];
            let _ = std::io::Write::write_all(out, ch.encode_utf8(&mut encoded).as_bytes());
        }
    }
    if start {
        let _ = std::io::Write::write_all(out, "‹".as_bytes());
        let _ = std::io::Write::write_all(out, buf.as_bytes());
    }
    Ok(())
}

/// Go `redact.DeRedactFile`: de-redacts `input` into `output` (a path, or
/// `"-"` for standard output), line by line with `\n` separators.
pub fn de_redact_file(remove: bool, input: &str, output: &str) -> std::io::Result<()> {
    use std::io::Write;

    let input_file = std::fs::File::open(input)?;
    let output_file: Box<dyn Write> = if output == "-" {
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

    de_redact(remove, input_file, output_file, "\n")
}

/// Go `redact.InitRedact`: sets the process-wide flag to `ON`/`OFF`.
pub fn init_redact(redact_log: bool) {
    let mode = if redact_log {
        tidb_error::mysql::RedactionMode::Enabled
    } else {
        tidb_error::mysql::RedactionMode::Disabled
    };
    tidb_error::mysql::set_redaction_mode(mode);
}

/// Go `redact.NeedRedact`: whether redaction is currently enabled (the flag
/// is neither `OFF` nor its uninitialized empty value).
pub fn need_redact() -> bool {
    tidb_error::mysql::redaction_mode() != tidb_error::mysql::RedactionMode::Disabled
}

/// Go `redact.Value`: `?` when redaction is enabled, else `arg` unchanged.
pub fn value(arg: &str) -> String {
    if need_redact() {
        "?".to_owned()
    } else {
        arg.to_owned()
    }
}

/// Go `redact.Key`: `?` when redaction is enabled, else the upper-case hex
/// encoding of `key` (`strings.ToUpper(hex.EncodeToString(key))`).
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
    if redact == "MARKER" {
        build.push('‹');
        build.push_str(v);
        build.push('›');
    } else if redact == "ON" {
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

    fn redaction_mode_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
        GUARD.lock().unwrap_or_else(|error| error.into_inner())
    }

    #[test]
    fn test_redact() {
        for (mode, input, output) in [
            ("OFF", "fxcv", "fxcv"),
            ("OFF", "f‹xcv", "f‹xcv"),
            ("ON", "f‹xcv", ""),
            ("MARKER", "f‹xcv", "‹f‹‹xcv›"),
            ("MARKER", "f›xcv", "‹f››xcv›"),
        ] {
            assert_eq!(string(mode, input), output);
            assert_eq!(stringer(mode, &input).to_string(), output);
        }
    }

    #[test]
    fn test_de_redact() {
        for (remove, input, output) in [
            (true, "‹fxcv›ggg", "?ggg"),
            (false, "‹fxcv›ggg", "fxcvggg"),
            (true, "fxcv", "fxcv"),
            (false, "fxcv", "fxcv"),
            (true, "‹fxcv›ggg‹fxcv›eee", "?ggg?eee"),
            (false, "‹fxcv›ggg‹fxcv›eee", "fxcvgggfxcveee"),
            (true, "‹›", "?"),
            (false, "‹›", ""),
            (true, "gg‹ee", "gg‹ee"),
            (false, "gg‹ee", "gg‹ee"),
            (true, "gg›ee", "gg›ee"),
            (false, "gg›ee", "gg›ee"),
            (true, "gg‹ee‹ee", "gg‹ee‹ee"),
            (false, "gg‹ee‹gg", "gg‹ee‹gg"),
            (true, "gg›ee›gg", "gg›ee›gg"),
            (false, "gg›ee›ee", "gg›ee›ee"),
        ] {
            let mut actual = Vec::new();
            de_redact(remove, input.as_bytes(), &mut actual, "").unwrap();
            assert_eq!(String::from_utf8(actual).unwrap(), output);
        }
    }

    #[test]
    fn test_redact_init_and_value_and_key() {
        let _guard = redaction_mode_guard();
        let secret = "secret";

        init_redact(false);
        assert_eq!(value(secret), secret);
        assert_eq!(key(secret.as_bytes()), "736563726574");

        init_redact(true);
        assert_eq!(value(secret), "?");
        assert_eq!(key(secret.as_bytes()), "?");

        init_redact(false);
    }

    // Go permits callers to discard the direct redact helper results. Keep
    // this package from adding a Rust-only diagnostic contract.
    #[test]
    #[deny(unused_must_use)]
    fn return_values_may_be_ignored_like_go() {
        let input = "secret";

        string("OFF", input);
        stringer("OFF", &input);
        need_redact();
        value(input);
        key(input.as_bytes());
    }
}
