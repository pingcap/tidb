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
//! PACKAGE IN PROGRESS: Go's `TaskInfoRedacted` (a `backup.StreamBackup
//! TaskInfo` wrapper that scrubs S3/GCS/Azure credentials) is not ported --
//! it depends on the `kvproto` BR backup protobufs, which are not in this
//! workspace's proto set. It has no upstream test. Everything else in the
//! package is ported with its full test suite.

use std::sync::Mutex;

/// `errors.RedactLogEnable`: redaction on, values replaced.
pub const REDACT_LOG_ENABLE: &str = "ON";
/// `errors.RedactLogDisable`: redaction off.
pub const REDACT_LOG_DISABLE: &str = "OFF";
/// `errors.RedactLogMarker`: redaction by wrapping values in `‹...›`.
pub const REDACT_LOG_MARKER: &str = "MARKER";

/// The process-wide redact-log flag (`errors.RedactLogEnabled`). Its zero
/// value is the empty string, meaning "not initialized" (no redaction).
static REDACT_LOG_ENABLED: Mutex<String> = Mutex::new(String::new());

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
            debug_assert!(false, "invalid redact mode");
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
    let mut out = String::new();
    for line in scan_lines(input) {
        de_redact_line(remove, &line, &mut out)?;
        out.push_str(sep);
    }
    Ok(out)
}

/// Splits like Go's default `bufio.ScanLines`: on `\n`, dropping a single
/// trailing `\r`, and yielding a final unterminated line if non-empty. An
/// empty input yields no lines.
fn scan_lines(input: &str) -> Vec<String> {
    if input.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<String> = input
        .split('\n')
        .map(|raw| raw.strip_suffix('\r').unwrap_or(raw).to_owned())
        .collect();
    // split('\n') on "a\n" yields ["a", ""]; that trailing "" is the absence
    // of a final line, not an empty final line, so drop it.
    if input.ends_with('\n') {
        lines.pop();
    }
    lines
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
    let content = std::fs::read_to_string(input)?;
    let result = de_redact(remove, &content, "\n")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if output == "-" {
        use std::io::Write;
        std::io::stdout().write_all(result.as_bytes())
    } else {
        std::fs::write(output, result)
    }
}

/// Go `redact.InitRedact`: sets the process-wide flag to `ON`/`OFF`.
pub fn init_redact(redact_log: bool) {
    let mode = if redact_log {
        REDACT_LOG_ENABLE
    } else {
        REDACT_LOG_DISABLE
    };
    *REDACT_LOG_ENABLED.lock().unwrap() = mode.to_owned();
}

/// Go `redact.NeedRedact`: whether redaction is currently enabled (the flag
/// is neither `OFF` nor its uninitialized empty value).
#[must_use]
pub fn need_redact() -> bool {
    let mode = REDACT_LOG_ENABLED.lock().unwrap();
    *mode != REDACT_LOG_DISABLE && !mode.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestRedact.
    #[test]
    fn redact_string() {
        for (mode, input, output) in [
            ("OFF", "fxcv", "fxcv"),
            ("OFF", "f‹xcv", "f‹xcv"),
            ("ON", "f‹xcv", ""),
            ("MARKER", "f‹xcv", "‹f‹‹xcv›"),
            ("MARKER", "f›xcv", "‹f››xcv›"),
        ] {
            assert_eq!(string(mode, input), output, "{mode} {input}");
            assert_eq!(stringer(mode, &input).to_string(), output, "{mode} {input}");
        }
    }

    // Go TestDeRedact.
    #[test]
    fn de_redact_cases() {
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
            assert_eq!(de_redact(remove, input, "").unwrap(), output, "{input}");
        }
    }

    // Go TestRedactInitAndValueAndKey. This is the only test touching the
    // process-wide flag; keep it in its own #[test] so it does not race with
    // other redact tests (none of which read the flag).
    #[test]
    fn redact_init_value_key() {
        let secret = "secret";

        init_redact(false);
        assert_eq!(value(secret), secret);
        // "secret" hex has no a-f nibbles, so lower == upper here (Go's test
        // compares against lower-case EncodeToString and still passes).
        assert_eq!(key(secret.as_bytes()), "736563726574");

        init_redact(true);
        assert_eq!(value(secret), "?");
        assert_eq!(key(secret.as_bytes()), "?");

        // Reset so the shared flag does not leak into other tests.
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
}
