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

//! The unified-log-format text encoder (`zap_text_encoder.go`).
//!
//! Byte-level contract per the TiKV RFC 2018-12-19-unified-log-format:
//! `[time] [LEVEL] [caller] [message] [key=value] ...` with the source's
//! exact quoting (`needDoubleQuotes`) and escaping (`tryAddRuneSelf`,
//! U+FFFD for invalid UTF-8 bytes) rules.
//!
//! Adaptations: zap's `Field` universe becomes the [`Value`] enum covering
//! the field types the source encodes (strings, ints, floats with
//! NaN/±Inf spellings, bools, Go-formatted durations, base64 binary,
//! JSON-reflected values, arrays, objects, and errors with the
//! basic+Verbose pair); Go's reflection-driven `AddReflected` takes a
//! pre-serialized JSON string.

use std::fmt::Write as _;

use chrono::{DateTime, FixedOffset, Timelike};

/// Log level (zap levels the encoder renders).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Level {
    /// DEBUG.
    Debug,
    /// INFO.
    Info,
    /// WARN.
    Warn,
    /// ERROR.
    Error,
    /// DPANIC.
    DPanic,
    /// PANIC.
    Panic,
    /// FATAL.
    Fatal,
}

impl Level {
    /// zapcore `CapitalLevelEncoder`.
    pub fn capital(&self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
            Level::DPanic => "DPANIC",
            Level::Panic => "PANIC",
            Level::Fatal => "FATAL",
        }
    }

    /// Parses zap's lowercase level spelling.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            "dpanic" => Ok(Self::DPanic),
            "panic" => Ok(Self::Panic),
            "fatal" => Ok(Self::Fatal),
            _ => Err(format!("unrecognized level: {value}")),
        }
    }
}

/// A field value (the zap field types `addFields` encodes).
#[derive(Clone, Debug)]
pub enum Value {
    /// A string.
    Str(String),
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A float (NaN/+Inf/-Inf spelled like the source).
    F64(f64),
    /// A bool.
    Bool(bool),
    /// A complex number (zap renders `real+imagi`, always retaining `+`).
    Complex {
        /// Real component.
        real: f64,
        /// Imaginary component.
        imag: f64,
    },
    /// A duration in nanoseconds (zap `StringDurationEncoder` renders Go
    /// `Duration.String()`).
    Duration(i64),
    /// Binary bytes (base64, Go `AddBinary`).
    Binary(Vec<u8>),
    /// A byte string treated as UTF-8-ish text (Go `AddByteString`).
    ByteString(Vec<u8>),
    /// A pre-serialized JSON value (Go `AddReflected`).
    Reflect(String),
    /// An array of values (Go `AppendArray`).
    Array(Vec<Value>),
    /// An object of key/value pairs (Go `AppendObject`).
    Object(Vec<Field>),
    /// An error: basic message plus optional rich `%+v` verbose form (Go
    /// `encodeError` renders `key` and `keyVerbose`).
    Error {
        /// `err.Error()`.
        basic: String,
        /// `fmt.Sprintf("%+v", err)` when the error is a formatter.
        verbose: Option<String>,
    },
}

/// A key/value log field.
#[derive(Clone, Debug)]
pub struct Field {
    /// Field key.
    pub key: String,
    /// Field value.
    pub value: Value,
}

impl Field {
    /// Convenience constructor.
    pub fn new(key: impl Into<String>, value: Value) -> Field {
        Field {
            key: key.into(),
            value,
        }
    }
}

/// A log entry (the zapcore `Entry` subset the encoder consumes).
pub struct Entry {
    /// Entry time.
    pub time: DateTime<FixedOffset>,
    /// Level.
    pub level: Level,
    /// Logger name (Go `LoggerName`), empty for none.
    pub logger_name: String,
    /// Caller as raw `file` path + line; `None` when undefined.
    pub caller: Option<(String, u32)>,
    /// Message.
    pub message: String,
    /// Stacktrace (Go `Stack`), empty for none.
    pub stack: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// The unified-format encoder returned by Go `NewTextEncoder`. `with` fields
/// (logger context) are retained and emitted before entry fields, matching
/// zap's `Clone()`+buffer semantics for both text and JSON output.
#[derive(Default, Clone)]
pub struct TextEncoder {
    /// Output selected by Go `Config.Format`.
    format: OutputFormat,
    /// Context fields stored by `logger.With` (Go: the encoder's own buffer).
    context: Vec<Field>,
    /// Go `disableErrorVerbose`.
    pub disable_error_verbose: bool,
    /// Go `cfg.DisableTimestamp` (drops the time section).
    pub disable_timestamp: bool,
}

/// Go `DefaultTimeEncoder` layout `2006/01/02 15:04:05.000 -07:00`.
pub fn format_time(t: &DateTime<FixedOffset>) -> String {
    let base = t.format("%Y/%m/%d %H:%M:%S");
    let millis = t.nanosecond() / 1_000_000;
    let offset = t.offset().local_minus_utc();
    let (sign, off) = if offset < 0 {
        ('-', -offset)
    } else {
        ('+', offset)
    };
    format!(
        "{base}.{millis:03} {sign}{:02}:{:02}",
        off / 3600,
        (off % 3600) / 60
    )
}

/// Go `DefaultTimeEncoder` alias.
pub fn default_time_encoder(t: &DateTime<FixedOffset>) -> String {
    format_time(t)
}

/// Go `getCallerString`: strip the directory, keep only
/// `[A-Za-z0-9._-]` bytes of the file name, append `:line`.
pub fn caller_string(file: &str, line: u32) -> String {
    if file.is_empty() && line == 0 {
        return "<unknown>".to_owned();
    }
    let base = match file.rfind('/') {
        Some(i) => &file[i + 1..],
        None => file,
    };
    let mut s = String::with_capacity(base.len() + 6);
    for b in base.bytes() {
        if b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_' {
            s.push(b as char);
        }
    }
    let _ = write!(s, ":{line}");
    s
}

/// Go `ShortCallerEncoder` alias.
pub fn short_caller_encoder(file: &str, line: u32) -> String {
    caller_string(file, line)
}

// Go `needDoubleQuotes`.
fn need_double_quotes(s: &[u8]) -> bool {
    s.iter()
        .any(|&b| b <= 0x20 || matches!(b, b'\\' | b'"' | b'[' | b']' | b'='))
}

// Go `safeAddString`/`safeAddByteString`: JSON-style escaping with
// `�` for invalid UTF-8 bytes.
fn safe_add_bytes(out: &mut String, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut i = 0;
    while i < s.len() {
        let b = s[i];
        if b < 0x80 {
            match b {
                b'\\' | b'"' => {
                    out.push('\\');
                    out.push(b as char);
                }
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                _ if b >= 0x20 => out.push(b as char),
                _ => {
                    out.push_str("\\u00");
                    out.push(HEX[(b >> 4) as usize] as char);
                    out.push(HEX[(b & 0xf) as usize] as char);
                }
            }
            i += 1;
            continue;
        }
        match std::str::from_utf8(&s[i..]) {
            Ok(rest) => {
                let character = rest.chars().next().expect("non-empty UTF-8 tail");
                out.push(character);
                i += character.len_utf8();
            }
            Err(e) => {
                let valid = e.valid_up_to();
                if valid > 0 {
                    let character = std::str::from_utf8(&s[i..i + valid])
                        .expect("validated UTF-8 prefix")
                        .chars()
                        .next()
                        .expect("non-empty UTF-8 prefix");
                    out.push(character);
                    i += character.len_utf8();
                } else {
                    out.push_str("\\ufffd");
                    i += 1;
                }
            }
        }
    }
}

fn append_string_with_quote(out: &mut String, s: &[u8]) {
    if !need_double_quotes(s) {
        safe_add_bytes(out, s);
        return;
    }
    out.push('"');
    safe_add_bytes(out, s);
    out.push('"');
}

// Go `addElementSeparator`.
fn add_element_separator(out: &mut String) {
    match out.as_bytes().last() {
        None | Some(b'{') | Some(b'[') | Some(b':') | Some(b',') | Some(b' ') | Some(b'=') => {}
        Some(_) => out.push(','),
    }
}

// Go `appendFloat` (strconv.AppendFloat 'g'-shortest formatting for the
// common cases; NaN/±Inf spelled explicitly).
fn append_float(out: &mut String, v: f64) {
    add_element_separator(out);
    if v.is_nan() {
        out.push_str("NaN");
    } else if v.is_infinite() {
        out.push_str(if v > 0.0 { "+Inf" } else { "-Inf" });
    } else {
        let _ = write!(out, "{v}");
    }
}

fn append_value(out: &mut String, v: &Value) {
    match v {
        Value::Str(s) => {
            add_element_separator(out);
            append_string_with_quote(out, s.as_bytes());
        }
        Value::I64(x) => {
            add_element_separator(out);
            let _ = write!(out, "{x}");
        }
        Value::U64(x) => {
            add_element_separator(out);
            let _ = write!(out, "{x}");
        }
        Value::F64(x) => append_float(out, *x),
        Value::Bool(b) => {
            add_element_separator(out);
            let _ = write!(out, "{b}");
        }
        Value::Complex { real, imag } => {
            add_element_separator(out);
            let _ = write!(out, "{real}+{imag}i");
        }
        Value::Duration(ns) => {
            add_element_separator(out);
            append_string_with_quote(
                out,
                tidb_config::configtypes::format_go_duration(*ns).as_bytes(),
            );
        }
        Value::Binary(bytes) => {
            add_element_separator(out);
            append_string_with_quote(out, base64_std(bytes).as_bytes());
        }
        Value::ByteString(bytes) => {
            add_element_separator(out);
            append_string_with_quote(out, bytes);
        }
        Value::Reflect(json) => {
            add_element_separator(out);
            append_string_with_quote(out, json.as_bytes());
        }
        Value::Array(items) => {
            add_element_separator(out);
            let mut inner = String::from("[");
            for item in items {
                append_value(&mut inner, item);
            }
            inner.push(']');
            append_string_with_quote(out, inner.as_bytes());
        }
        Value::Object(fields) => {
            add_element_separator(out);
            let mut inner = String::from("{");
            for f in fields {
                add_key(&mut inner, &f.key);
                append_value(&mut inner, &f.value);
            }
            inner.push('}');
            append_string_with_quote(out, inner.as_bytes());
        }
        Value::Error { basic, .. } => {
            // Only reachable through add_field, which handles the pair.
            add_element_separator(out);
            append_string_with_quote(out, basic.as_bytes());
        }
    }
}

// Go `addKey`.
fn add_key(out: &mut String, key: &str) {
    add_element_separator(out);
    append_string_with_quote(out, key.as_bytes());
    out.push('=');
}

fn begin_quote_field(out: &mut String) {
    if !out.is_empty() {
        out.push(' ');
    }
    out.push('[');
}

fn end_quote_field(out: &mut String) {
    out.push(']');
}

fn add_field(out: &mut String, f: &Field, disable_error_verbose: bool) {
    if let Value::Error { basic, verbose } = &f.value {
        // Go `encodeError`: `[key=basic]` then `[keyVerbose=verbose]`.
        begin_quote_field(out);
        add_key(out, &f.key);
        add_element_separator(out);
        append_string_with_quote(out, basic.as_bytes());
        end_quote_field(out);
        if disable_error_verbose {
            return;
        }
        if let Some(verbose) = verbose {
            if verbose != basic {
                begin_quote_field(out);
                add_key(out, &format!("{}Verbose", f.key));
                add_element_separator(out);
                append_string_with_quote(out, verbose.as_bytes());
                end_quote_field(out);
            }
        }
        return;
    }
    begin_quote_field(out);
    add_key(out, &f.key);
    append_value(out, &f.value);
    end_quote_field(out);
}

// Go stdlib base64.StdEncoding (padded).
fn base64_std(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        out.push(TABLE[(b[0] >> 2) as usize] as char);
        out.push(TABLE[(((b[0] & 0x3) << 4) | (b[1] >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b[1] & 0xf) << 2) | (b[2] >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b[2] & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

impl TextEncoder {
    /// Creates an encoder from a [`crate::Config`].
    pub fn new(cfg: &crate::Config) -> Result<TextEncoder, String> {
        let format = match cfg.format.as_str() {
            "text" | "" => OutputFormat::Text,
            "json" => OutputFormat::Json,
            other => return Err(format!("unsupport log format: {other}")),
        };
        Ok(TextEncoder {
            format,
            context: Vec::new(),
            disable_error_verbose: cfg.disable_error_verbose,
            disable_timestamp: cfg.disable_timestamp,
        })
    }

    /// Returns an encoder with the given context fields appended (zap
    /// `logger.With`).
    pub fn with_fields(&self, fields: &[Field]) -> TextEncoder {
        let mut clone = self.clone();
        clone.context.extend_from_slice(fields);
        clone
    }

    /// Go `EncodeEntry`: renders one line including the trailing newline.
    ///
    /// This compatibility wrapper panics only when a pre-serialized reflected
    /// value is invalid for JSON. Logger plumbing uses [`Self::try_encode_entry`]
    /// and reports that error through its error sink.
    pub fn encode_entry(&self, ent: &Entry, fields: &[Field]) -> String {
        self.try_encode_entry(ent, fields)
            .expect("failed to encode log entry")
    }

    /// Fallible Go `EncodeEntry` equivalent.
    pub fn try_encode_entry(&self, ent: &Entry, fields: &[Field]) -> Result<String, String> {
        match self.format {
            OutputFormat::Text => Ok(self.encode_text_entry(ent, fields)),
            OutputFormat::Json => {
                for field in self.context.iter().chain(fields) {
                    validate_json_field(field)?;
                }
                Ok(self.encode_json_entry(ent, fields))
            }
        }
    }

    fn encode_text_entry(&self, ent: &Entry, fields: &[Field]) -> String {
        let mut out = String::new();
        if !self.disable_timestamp {
            begin_quote_field(&mut out);
            add_element_separator(&mut out);
            out.push_str(&format_time(&ent.time));
            end_quote_field(&mut out);
        }
        {
            begin_quote_field(&mut out);
            add_element_separator(&mut out);
            out.push_str(ent.level.capital());
            end_quote_field(&mut out);
        }
        if !ent.logger_name.is_empty() {
            begin_quote_field(&mut out);
            add_element_separator(&mut out);
            append_string_with_quote(&mut out, ent.logger_name.as_bytes());
            end_quote_field(&mut out);
        }
        if let Some((file, line)) = &ent.caller {
            begin_quote_field(&mut out);
            add_element_separator(&mut out);
            out.push_str(&caller_string(file, *line));
            end_quote_field(&mut out);
        }
        if !ent.message.is_empty() {
            begin_quote_field(&mut out);
            add_element_separator(&mut out);
            append_string_with_quote(&mut out, ent.message.as_bytes());
            end_quote_field(&mut out);
        }
        for field in &self.context {
            add_field(&mut out, field, self.disable_error_verbose);
        }
        for f in fields {
            add_field(&mut out, f, self.disable_error_verbose);
        }
        if !ent.stack.is_empty() {
            begin_quote_field(&mut out);
            add_key(&mut out, "stack");
            add_element_separator(&mut out);
            append_string_with_quote(&mut out, ent.stack.as_bytes());
            end_quote_field(&mut out);
        }
        out.push('\n');
        out
    }

    fn encode_json_entry(&self, ent: &Entry, fields: &[Field]) -> String {
        let mut out = String::from("{");
        let mut first = true;
        add_json_field(
            &mut out,
            &mut first,
            "level",
            &Value::Str(ent.level.capital().to_owned()),
        );
        if !self.disable_timestamp {
            add_json_field(
                &mut out,
                &mut first,
                "time",
                &Value::Str(format_time(&ent.time)),
            );
        }
        if !ent.logger_name.is_empty() {
            add_json_field(
                &mut out,
                &mut first,
                "name",
                &Value::Str(ent.logger_name.clone()),
            );
        }
        if let Some((file, line)) = &ent.caller {
            add_json_field(
                &mut out,
                &mut first,
                "caller",
                &Value::Str(caller_string(file, *line)),
            );
        }
        if !ent.message.is_empty() {
            add_json_field(
                &mut out,
                &mut first,
                "message",
                &Value::Str(ent.message.clone()),
            );
        }
        for field in self.context.iter().chain(fields) {
            add_json_log_field(&mut out, &mut first, field);
        }
        if !ent.stack.is_empty() {
            add_json_field(
                &mut out,
                &mut first,
                "stack",
                &Value::Str(ent.stack.clone()),
            );
        }
        out.push_str("}\n");
        out
    }
}

fn add_json_log_field(out: &mut String, first: &mut bool, field: &Field) {
    if let Value::Error { basic, verbose } = &field.value {
        add_json_field(out, first, &field.key, &Value::Str(basic.clone()));
        if let Some(verbose) = verbose {
            if verbose != basic {
                add_json_field(
                    out,
                    first,
                    &format!("{}Verbose", field.key),
                    &Value::Str(verbose.clone()),
                );
            }
        }
        return;
    }
    add_json_field(out, first, &field.key, &field.value);
}

fn add_json_field(out: &mut String, first: &mut bool, key: &str, value: &Value) {
    if !*first {
        out.push(',');
    }
    *first = false;
    append_json_string(out, key.as_bytes());
    out.push(':');
    append_json_value(out, value);
}

fn append_json_string(out: &mut String, value: &[u8]) {
    out.push('"');
    safe_add_bytes(out, value);
    out.push('"');
}

fn append_json_value(out: &mut String, value: &Value) {
    match value {
        Value::Str(value) => append_json_string(out, value.as_bytes()),
        Value::I64(value) => {
            let _ = write!(out, "{value}");
        }
        Value::U64(value) => {
            let _ = write!(out, "{value}");
        }
        Value::F64(value) if value.is_nan() => out.push_str("\"NaN\""),
        Value::F64(value) if value.is_infinite() => {
            out.push_str(if value.is_sign_positive() {
                "\"+Inf\""
            } else {
                "\"-Inf\""
            });
        }
        Value::F64(value) => {
            let _ = write!(out, "{value}");
        }
        Value::Bool(value) => {
            let _ = write!(out, "{value}");
        }
        Value::Complex { real, imag } => {
            append_json_string(out, format!("{real}+{imag}i").as_bytes());
        }
        Value::Duration(value) => append_json_string(
            out,
            tidb_config::configtypes::format_go_duration(*value).as_bytes(),
        ),
        Value::Binary(value) => append_json_string(out, base64_std(value).as_bytes()),
        Value::ByteString(value) => append_json_string(out, value),
        Value::Reflect(value) => out.push_str(value),
        Value::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                append_json_value(out, value);
            }
            out.push(']');
        }
        Value::Object(fields) => {
            out.push('{');
            let mut first = true;
            for field in fields {
                add_json_log_field(out, &mut first, field);
            }
            out.push('}');
        }
        Value::Error { basic, .. } => append_json_string(out, basic.as_bytes()),
    }
}

fn validate_json_field(field: &Field) -> Result<(), String> {
    validate_json_value(&field.value).map_err(|error| format!("{}: {error}", field.key))
}

fn validate_json_value(value: &Value) -> Result<(), String> {
    match value {
        Value::Reflect(json) => serde_json::from_str::<serde_json::Value>(json)
            .map(|_| ())
            .map_err(|error| format!("cannot encode reflected value: {error}")),
        Value::Array(values) => values.iter().try_for_each(validate_json_value),
        Value::Object(fields) => fields.iter().try_for_each(validate_json_field),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn enc() -> TextEncoder {
        TextEncoder::default()
    }

    fn entry(level: Level, file: &str, line: u32, msg: &str) -> Entry {
        Entry {
            time: FixedOffset::east_opt(8 * 3600)
                .unwrap()
                .timestamp_opt(1547192741, 165_279_177)
                .unwrap(),
            level,
            logger_name: String::new(),
            caller: Some((file.to_string(), line)),
            message: msg.to_string(),
            stack: String::new(),
        }
    }

    fn encode_no_time(e: &Entry, fields: &[Field]) -> String {
        let mut enc = enc();
        enc.disable_timestamp = true;
        let s = enc.encode_entry(e, fields);
        s.trim_end_matches('\n').to_string()
    }

    // Goldens from Go zap_log_test.go `testLog`.
    #[test]
    fn log_goldens() {
        assert_eq!(
            encode_no_time(
                &entry(Level::Info, "zap_log_test.go", 50, "failed to fetch URL"),
                &[
                    Field::new("url", Value::Str("http://example.com".into())),
                    Field::new("attempt", Value::I64(3)),
                    Field::new("backoff", Value::Duration(1_000_000_000)),
                ]
            ),
            r#"[INFO] [zap_log_test.go:50] ["failed to fetch URL"] [url=http://example.com] [attempt=3] [backoff=1s]"#
        );

        assert_eq!(
            encode_no_time(
                &entry(
                    Level::Info,
                    "zap_log_test.go",
                    55,
                    "failed to \"fetch\" [URL]: http://example.com"
                ),
                &[]
            ),
            r#"[INFO] [zap_log_test.go:55] ["failed to \"fetch\" [URL]: http://example.com"]"#
        );

        assert_eq!(
            encode_no_time(
                &entry(Level::Debug, "zap_log_test.go", 56, "Slow query"),
                &[
                    Field::new(
                        "sql",
                        Value::Str("SELECT * FROM TABLE\n\tWHERE ID=\"abc\"".into())
                    ),
                    Field::new("duration", Value::Duration(1_300_000_000)),
                    Field::new("process keys", Value::I64(1500)),
                ]
            ),
            r#"[DEBUG] [zap_log_test.go:56] ["Slow query"] [sql="SELECT * FROM TABLE\n\tWHERE ID=\"abc\""] [duration=1.3s] ["process keys"=1500]"#
        );

        assert_eq!(
            encode_no_time(&entry(Level::Info, "zap_log_test.go", 62, "Welcome"), &[]),
            "[INFO] [zap_log_test.go:62] [Welcome]"
        );
        assert_eq!(
            encode_no_time(
                &entry(Level::Info, "zap_log_test.go", 63, "Welcome TiDB"),
                &[]
            ),
            r#"[INFO] [zap_log_test.go:63] ["Welcome TiDB"]"#
        );
        assert_eq!(
            encode_no_time(&entry(Level::Info, "zap_log_test.go", 64, "欢迎"), &[]),
            "[INFO] [zap_log_test.go:64] [欢迎]"
        );
        assert_eq!(
            encode_no_time(
                &entry(Level::Info, "zap_log_test.go", 65, "欢迎来到 TiDB"),
                &[]
            ),
            r#"[INFO] [zap_log_test.go:65] ["欢迎来到 TiDB"]"#
        );
        assert_eq!(
            encode_no_time(
                &entry(Level::Warn, "zap_log_test.go", 66, "Type"),
                &[
                    Field::new("Counter", Value::F64(f64::NAN)),
                    Field::new("Score", Value::F64(f64::INFINITY)),
                ]
            ),
            "[WARN] [zap_log_test.go:66] [Type] [Counter=NaN] [Score=+Inf]"
        );

        // `logger.With` context fields render before entry fields.
        let with = enc().with_fields(&[
            Field::new("connID", Value::U64(1)),
            Field::new("traceID", Value::Str("dse1121".into())),
        ]);
        let mut e = entry(Level::Info, "zap_log_test.go", 71, "new connection");
        e.time = FixedOffset::east_opt(0)
            .unwrap()
            .timestamp_opt(0, 0)
            .unwrap();
        let mut with_enc = with.clone();
        with_enc.disable_timestamp = true;
        assert_eq!(
            with_enc.encode_entry(&e, &[]).trim_end_matches('\n'),
            "[INFO] [zap_log_test.go:71] [\"new connection\"] [connID=1] [traceID=dse1121]"
        );
    }

    // The "Testing typs" golden covering the field-type universe.
    #[test]
    fn field_types_golden() {
        let fields = vec![
            Field::new("filed1", Value::Str("noquote".into())),
            Field::new("filed2", Value::Str("in quote".into())),
            Field::new(
                "urls",
                Value::Array(vec![
                    Value::Str("http://mock1.com:2347".into()),
                    Value::Str("http://mock2.com:2432".into()),
                ]),
            ),
            Field::new(
                "urls-peer",
                Value::Array(vec![Value::Str("t1".into()), Value::Str("t2 fine".into())]),
            ),
            Field::new(
                "store ids",
                Value::Array(vec![Value::U64(1), Value::U64(4), Value::U64(5)]),
            ),
            Field::new(
                "object",
                Value::Object(vec![Field::new("username", Value::Str("user1".into()))]),
            ),
            Field::new(
                "object2",
                Value::Object(vec![Field::new("username", Value::Str("user 2".into()))]),
            ),
            Field::new("binary", Value::Binary(b"ab123".to_vec())),
            Field::new("is processed", Value::Bool(true)),
            Field::new("bytestring", Value::ByteString(b"noquote".to_vec())),
            Field::new("bytestring", Value::ByteString(b"in quote".to_vec())),
            Field::new("int8", Value::I64(1)),
            Field::new("ptr", Value::U64(10)),
            Field::new("reflect", Value::Reflect("[1,2]".into())),
            Field::new("stringer", Value::Str("127.0.0.1".into())),
            Field::new("array bools", Value::Array(vec![Value::Bool(true)])),
            Field::new(
                "array bools",
                Value::Array(vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                ]),
            ),
            Field::new(
                "complex128",
                Value::Complex {
                    real: 1.0,
                    imag: 2.0,
                },
            ),
            Field::new(
                "test",
                Value::Array(vec![
                    Value::Str("💖".into()),
                    Value::Str("�".into()),
                    Value::Str("☺☻☹".into()),
                    Value::Str("日a本b語ç日ð本Ê語þ日¥本¼語i日©".into()),
                    Value::Str(
                        "日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©"
                            .into(),
                    ),
                    Value::ByteString(vec![0x80, 0x80, 0x80, 0x80]),
                    Value::Str("<car><mirror>XML</mirror></car>".into()),
                ]),
            ),
            Field::new("duration", Value::Duration(10_000_000_000)),
        ];
        let out = encode_no_time(
            &entry(Level::Info, "zap_log_test.go", 72, "Testing typs"),
            &fields,
        );
        assert_eq!(
            out,
            r#"[INFO] [zap_log_test.go:72] ["Testing typs"] [filed1=noquote] [filed2="in quote"] [urls="[http://mock1.com:2347,http://mock2.com:2432]"] [urls-peer="[t1,\"t2 fine\"]"] ["store ids"="[1,4,5]"] [object="{username=user1}"] [object2="{username=\"user 2\"}"] [binary="YWIxMjM="] ["is processed"=true] [bytestring=noquote] [bytestring="in quote"] [int8=1] [ptr=10] [reflect="[1,2]"] [stringer=127.0.0.1] ["array bools"="[true]"] ["array bools"="[true,true,false]"] [complex128=1+2i] [test="[💖,�,☺☻☹,日a本b語ç日ð本Ê語þ日¥本¼語i日©,日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©,\\ufffd\\ufffd\\ufffd\\ufffd,<car><mirror>XML</mirror></car>]"] [duration=10s]"#
        );

        // Invalid-UTF-8 bytes render as literal \ufffd escapes, one per
        // invalid byte, and don't trigger quoting (Go checks raw bytes).
        let mut s = String::new();
        append_string_with_quote(&mut s, &[0xed, 0xa0, 0x80, 0x80]);
        assert_eq!(s, r"\ufffd\ufffd\ufffd\ufffd");
    }

    // Go TestTimeEncoder goldens.
    #[test]
    fn test_time_encoder() {
        let t = FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .timestamp_opt(1547192741, 165_279_177)
            .unwrap();
        assert_eq!(format_time(&t), "2019/01/11 15:45:41.165 +08:00");
        let utc = t.with_timezone(&FixedOffset::east_opt(0).unwrap());
        assert_eq!(format_time(&utc), "2019/01/11 07:45:41.165 +00:00");
    }

    // Go TestZapCaller goldens.
    #[test]
    fn test_zap_caller() {
        assert_eq!(caller_string("server.go", 132), "server.go:132");
        assert_eq!(
            caller_string("server/coordinator.go", 20),
            "coordinator.go:20"
        );
        assert_eq!(
            caller_string(r"z\test_coordinator1.go", 20),
            "ztest_coordinator1.go:20"
        );
        assert_eq!(caller_string("", 0), "<unknown>");
    }

    /// Exact output rows from pinned `zap_log_test.go::TestLogJSON`.
    #[test]
    fn test_log_json() {
        let encoder = TextEncoder::new(&crate::Config {
            format: "json".to_owned(),
            disable_timestamp: true,
            ..crate::Config::default()
        })
        .expect("the pinned Go NewTextEncoder accepts JSON format");

        let first = encoder.encode_entry(
            &entry(Level::Info, "zap_log_test.go", 314, "failed to fetch URL"),
            &[
                Field::new("url", Value::Str("http://example.com".into())),
                Field::new("attempt", Value::I64(3)),
                Field::new("backoff", Value::Duration(1_000_000_000)),
            ],
        );
        assert_eq!(
            first.trim_end_matches('\n'),
            r#"{"level":"INFO","caller":"zap_log_test.go:314","message":"failed to fetch URL","url":"http://example.com","attempt":3,"backoff":"1s"}"#
        );

        let with_connection = encoder.with_fields(&[
            Field::new("connID", Value::Str("1".into())),
            Field::new("traceID", Value::Str("dse1121".into())),
        ]);
        let second = with_connection.encode_entry(
            &entry(Level::Info, "zap_log_test.go", 319, "new connection"),
            &[],
        );
        assert_eq!(
            second.trim_end_matches('\n'),
            r#"{"level":"INFO","caller":"zap_log_test.go:319","message":"new connection","connID":"1","traceID":"dse1121"}"#
        );
    }

    // Go `NewTextEncoder` delegates JSON output to zap's JSON encoder. The
    // zap encoder writes level before time and does not apply the text-only
    // `DisableErrorVerbose` setting.
    #[test]
    fn json_encoder_semantics() {
        let encoder = TextEncoder::new(&crate::Config {
            format: "json".to_owned(),
            disable_error_verbose: true,
            ..crate::Config::default()
        })
        .unwrap();
        let output = encoder.encode_entry(
            &entry(Level::Error, "zap_log_test.go", 1, "failed"),
            &[Field::new(
                "error",
                Value::Error {
                    basic: "boom".into(),
                    verbose: Some("boom\nstack".into()),
                },
            )],
        );

        assert_eq!(
            output.trim_end_matches('\n'),
            r#"{"level":"ERROR","time":"2019/01/11 15:45:41.165 +08:00","caller":"zap_log_test.go:1","message":"failed","error":"boom","errorVerbose":"boom\nstack"}"#
        );

        let error = encoder
            .try_encode_entry(
                &entry(Level::Info, "zap_log_test.go", 1, "reflected"),
                &[Field::new("reflect", Value::Reflect("{".into()))],
            )
            .unwrap_err();
        assert!(error.contains("cannot encode reflected value"));
    }

    // Go `encodeError` pair rendering.
    #[test]
    fn error_fields() {
        let f = Field::new(
            "error",
            Value::Error {
                basic: "boom".into(),
                verbose: Some("boom\nstack".into()),
            },
        );
        let mut out = String::new();
        add_field(&mut out, &f, false);
        assert_eq!(out, r#"[error=boom] [errorVerbose="boom\nstack"]"#);

        let mut out = String::new();
        add_field(&mut out, &f, true);
        assert_eq!(out, "[error=boom]");
    }
}
