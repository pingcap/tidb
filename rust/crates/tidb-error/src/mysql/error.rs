// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct SQLError, NewErr, and NewErrf translation.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

use super::{message_by_code, mysql_state};

/// Portable ErrBadConn message from the Go source.
pub const ERR_BAD_CONN: &str = "connection was bad";
/// Portable ErrMalformPacket message from the Go source.
pub const ERR_MALFORM_PACKET: &str = "malform packet error";

/// Global argument-redaction mode used by source error constructors.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum RedactionMode {
    /// Preserve arguments.
    #[default]
    Disabled = 0,
    /// Replace sensitive arguments with a question mark.
    Enabled = 1,
    /// Surround sensitive arguments with redaction markers.
    Marker = 2,
}

static REDACTION_MODE: AtomicU8 = AtomicU8::new(RedactionMode::Disabled as u8);

/// Sets the process-wide source-compatible error argument redaction mode.
pub fn set_redaction_mode(mode: RedactionMode) {
    REDACTION_MODE.store(mode as u8, Ordering::Relaxed);
}

/// Returns the process-wide error argument redaction mode.
#[must_use]
pub fn redaction_mode() -> RedactionMode {
    match REDACTION_MODE.load(Ordering::Relaxed) {
        1 => RedactionMode::Enabled,
        2 => RedactionMode::Marker,
        _ => RedactionMode::Disabled,
    }
}

/// Owned representation of a Go fmt argument.
///
/// The source catalogs use only string, decimal, display, debug, type, and
/// fixed/dynamic string precision. Separate forms preserve that vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatArg {
    display: String,
    debug: String,
    type_name: String,
    precision: Option<isize>,
}

impl FormatArg {
    /// Constructs an argument with explicit Go display/debug/type forms.
    #[must_use]
    pub fn new(
        display: impl Into<String>,
        debug: impl Into<String>,
        type_name: impl Into<String>,
    ) -> Self {
        Self {
            display: display.into(),
            debug: debug.into(),
            type_name: type_name.into(),
            precision: None,
        }
    }

    /// Constructs the Go nil interface representation.
    #[must_use]
    pub fn nil() -> Self {
        Self::new("<nil>", "<nil>", "<nil>")
    }
}

impl From<&str> for FormatArg {
    fn from(value: &str) -> Self {
        Self::new(value, format!("{value:?}"), "string")
    }
}

impl From<String> for FormatArg {
    fn from(value: String) -> Self {
        Self::new(value.clone(), format!("{value:?}"), "string")
    }
}

macro_rules! integer_format_arg {
    ($($type:ty => $go_name:literal),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                Self {
                    display: value.to_string(),
                    debug: value.to_string(),
                    type_name: $go_name.to_owned(),
                    precision: isize::try_from(value).ok(),
                }
            }
        }
    )+};
}

integer_format_arg! {
    i8 => "int8", i16 => "int16", i32 => "int32", i64 => "int64", isize => "int",
    u8 => "uint8", u16 => "uint16", u32 => "uint32", u64 => "uint64", usize => "uint",
}

impl From<bool> for FormatArg {
    fn from(value: bool) -> Self {
        Self::new(value.to_string(), value.to_string(), "bool")
    }
}

macro_rules! float_format_arg {
    ($($type:ty => $go_name:literal),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                Self::new(value.to_string(), value.to_string(), $go_name)
            }
        }
    )+};
}
float_format_arg! { f32 => "float32", f64 => "float64" }

impl From<char> for FormatArg {
    fn from(value: char) -> Self {
        Self::new(value.to_string(), format!("{value:?}"), "int32")
    }
}

/// MySQL wire error information produced while executing SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlError {
    /// Protocol error number.
    pub code: u16,
    /// Rendered error message.
    pub message: String,
    /// Five-byte SQLSTATE string.
    pub state: &'static str,
}

impl SqlError {
    /// Source NewErr constructor.
    #[must_use]
    pub fn new(code: u16, args: &[FormatArg]) -> Self {
        let message = message_by_code(code).map_or_else(
            || sprint(args),
            |entry| format_template(entry.raw, entry.redact_arg_pos, args),
        );
        Self {
            code,
            message,
            state: mysql_state(code),
        }
    }

    /// Source NewErrf constructor.
    #[must_use]
    pub fn new_f(code: u16, format: &str, redact_arg_pos: &[usize], args: &[FormatArg]) -> Self {
        Self {
            code,
            message: format_template(format, redact_arg_pos, args),
            state: mysql_state(code),
        }
    }
}

// fmt.Sprint inserts a space between adjacent operands only when neither one
// is a string. This subtle rule matters for unknown error codes.
fn sprint(args: &[FormatArg]) -> String {
    let mut output = String::new();
    let mut previous_was_string = false;
    for (index, argument) in args.iter().enumerate() {
        let is_string = argument.type_name == "string";
        if index != 0 && !previous_was_string && !is_string {
            output.push(' ');
        }
        output.push_str(&argument.display);
        previous_was_string = is_string;
    }
    output
}

impl fmt::Display for SqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ERROR {} ({}): {}",
            self.code, self.state, self.message
        )
    }
}

impl Error for SqlError {}

fn format_template(template: &str, redact_positions: &[usize], args: &[FormatArg]) -> String {
    let bytes = template.as_bytes();
    let mut output = String::with_capacity(template.len());
    let mut cursor = 0;
    let mut argument_index = 0;

    while cursor < bytes.len() {
        if bytes[cursor] != b'%' {
            let next = bytes[cursor..]
                .iter()
                .position(|byte| *byte == b'%')
                .map_or(bytes.len(), |offset| cursor + offset);
            output.push_str(&template[cursor..next]);
            cursor = next;
            continue;
        }
        if bytes.get(cursor + 1) == Some(&b'%') {
            output.push('%');
            cursor += 2;
            continue;
        }

        let spec_start = cursor;
        cursor += 1;
        let mut alternate = false;
        while let Some(flag @ (b'#' | b'-' | b'+' | b' ' | b'0')) = bytes.get(cursor).copied() {
            alternate |= flag == b'#';
            cursor += 1;
        }
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }

        let mut precision = None;
        let mut bad_precision = false;
        if bytes.get(cursor) == Some(&b'.') {
            cursor += 1;
            if bytes.get(cursor) == Some(&b'*') {
                let dynamic = args.get(argument_index).and_then(|arg| arg.precision);
                argument_index += 1;
                match dynamic {
                    Some(value) if value >= 0 => precision = usize::try_from(value).ok(),
                    _ => bad_precision = true,
                }
                cursor += 1;
            } else {
                let start = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor += 1;
                }
                precision = template[start..cursor].parse().ok();
            }
        }

        let Some(verb) = bytes.get(cursor).copied() else {
            output.push_str("%!(NOVERB)");
            break;
        };
        cursor += 1;
        if bad_precision {
            output.push_str("%!(BADPREC)");
        }
        let Some(argument) = args.get(argument_index) else {
            output.push_str("%!");
            output.push(char::from(verb));
            output.push_str("(MISSING)");
            continue;
        };
        let sensitive = redact_positions.contains(&argument_index);
        argument_index += 1;

        // Go redaction happens before fmt.Sprintf: Enabled replaces the
        // interface argument with the string "?". The original verb still
        // applies, including fmt's type-mismatch diagnostics and `%#v`
        // quoting. Marker mode instead wraps the original verb rendering.
        let redacted;
        let argument = if sensitive && redaction_mode() == RedactionMode::Enabled {
            redacted = FormatArg::from("?");
            &redacted
        } else {
            argument
        };
        let rendered = match verb {
            b's' => argument.display.clone(),
            b'd' if argument.type_name == "string" => {
                format!("%!d(string={})", argument.display)
            }
            b'd' => argument.display.clone(),
            b'v' if alternate => argument.debug.clone(),
            b'v' => argument.display.clone(),
            b'T' => argument.type_name.clone(),
            _ => {
                output.push_str(&template[spec_start..cursor]);
                continue;
            }
        };
        let rendered = match precision {
            Some(maximum) => rendered.chars().take(maximum).collect(),
            None => rendered,
        };
        match (sensitive, redaction_mode()) {
            (true, RedactionMode::Marker) => {
                output.push('‹');
                for character in rendered.chars() {
                    output.push(character);
                    if matches!(character, '‹' | '›') {
                        output.push(character);
                    }
                }
                output.push('›');
            }
            _ => output.push_str(&rendered),
        }
    }

    if argument_index < args.len() {
        output.push_str("%!(EXTRA ");
        for (offset, argument) in args[argument_index..].iter().enumerate() {
            if offset != 0 {
                output.push_str(", ");
            }
            output.push_str(&argument.type_name);
            output.push('=');
            output.push_str(&argument.display);
        }
        output.push(')');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::errcode::{ErrDupEntry, ErrNoDB};

    #[test]
    fn source_error_rendering_and_redaction() {
        assert_eq!(
            SqlError::new(ErrNoDB, &[]).to_string(),
            "ERROR 1046 (3D000): No database selected"
        );
        assert_eq!(
            SqlError::new_f(0, "customized error", &[], &[]).to_string(),
            "ERROR 0 (HY000): customized error"
        );

        set_redaction_mode(RedactionMode::Enabled);
        assert_eq!(
            SqlError::new_f(
                ErrDupEntry,
                "Duplicate entry '%-.64s' for key '%-.192s'",
                &[0],
                &[FormatArg::from("secret"), FormatArg::from("primary")]
            )
            .message,
            "Duplicate entry '?' for key 'primary'"
        );

        let arguments = [
            FormatArg::from("secret"),
            FormatArg::from(7_i64),
            FormatArg::from("value"),
            FormatArg::from("debug"),
        ];
        assert_eq!(
            SqlError::new_f(0, "%s %d %v %#v", &[0, 1, 2, 3], &arguments).message,
            "? %!d(string=?) ? \"?\""
        );

        set_redaction_mode(RedactionMode::Marker);
        assert_eq!(
            SqlError::new_f(0, "%s %d %v %#v", &[0, 1, 2, 3], &arguments).message,
            "‹secret› ‹7› ‹value› ‹\"debug\"›"
        );
        set_redaction_mode(RedactionMode::Disabled);
    }
}
