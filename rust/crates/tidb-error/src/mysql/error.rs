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
/// Typed forms preserve Go's success and `%!verb(type=value)` mismatch paths;
/// the source catalog vocabulary itself is string, decimal, display, and
/// fixed/dynamic string precision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatArg {
    display: String,
    debug: String,
    type_name: String,
    precision: Option<isize>,
    kind: FormatKind,
    character: Option<char>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormatKind {
    String,
    Signed,
    Unsigned,
    Bool,
    Float,
    Char,
    Nil,
    Custom,
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
            kind: FormatKind::Custom,
            character: None,
        }
    }

    /// Constructs the Go nil interface representation.
    #[must_use]
    pub fn nil() -> Self {
        Self {
            kind: FormatKind::Nil,
            ..Self::new("<nil>", "<nil>", "<nil>")
        }
    }
}

impl From<&str> for FormatArg {
    fn from(value: &str) -> Self {
        Self {
            kind: FormatKind::String,
            ..Self::new(value, format!("{value:?}"), "string")
        }
    }
}

impl From<String> for FormatArg {
    fn from(value: String) -> Self {
        Self {
            kind: FormatKind::String,
            ..Self::new(value.clone(), format!("{value:?}"), "string")
        }
    }
}

macro_rules! integer_format_arg {
    ($($type:ty => $go_name:literal => $kind:ident),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                let kind = FormatKind::$kind;
                Self {
                    display: value.to_string(),
                    debug: if kind == FormatKind::Unsigned {
                        format!("0x{value:x}")
                    } else {
                        value.to_string()
                    },
                    type_name: $go_name.to_owned(),
                    precision: isize::try_from(value).ok(),
                    kind,
                    character: None,
                }
            }
        }
    )+};
}

integer_format_arg! {
    i8 => "int8" => Signed, i16 => "int16" => Signed, i32 => "int32" => Signed,
    i64 => "int64" => Signed, isize => "int" => Signed,
    u8 => "uint8" => Unsigned, u16 => "uint16" => Unsigned, u32 => "uint32" => Unsigned,
    u64 => "uint64" => Unsigned, usize => "uint" => Unsigned,
}

impl From<bool> for FormatArg {
    fn from(value: bool) -> Self {
        Self {
            kind: FormatKind::Bool,
            ..Self::new(value.to_string(), value.to_string(), "bool")
        }
    }
}

macro_rules! float_format_arg {
    ($($type:ty => $go_name:literal),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                let display = if value.is_nan() {
                    "NaN".to_owned()
                } else if value == <$type>::INFINITY {
                    "+Inf".to_owned()
                } else if value == <$type>::NEG_INFINITY {
                    "-Inf".to_owned()
                } else if value != 0.0 && (value.abs() >= 1e6 || value.abs() < 1e-4) {
                    let scientific = format!("{value:e}");
                    let (mantissa, exponent) = scientific
                        .split_once('e')
                        .expect("Rust scientific formatting contains e");
                    let exponent = exponent.parse::<i32>().expect("numeric exponent");
                    format!("{mantissa}e{exponent:+03}")
                } else {
                    value.to_string()
                };
                Self {
                    kind: FormatKind::Float,
                    ..Self::new(display.clone(), display, $go_name)
                }
            }
        }
    )+};
}
float_format_arg! { f32 => "float32", f64 => "float64" }

impl From<char> for FormatArg {
    fn from(value: char) -> Self {
        let codepoint = u32::from(value).to_string();
        Self {
            display: codepoint.clone(),
            debug: codepoint,
            type_name: "int32".to_owned(),
            precision: None,
            kind: FormatKind::Char,
            character: Some(value),
        }
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

fn mismatch(verb: u8, argument: &FormatArg) -> String {
    if argument.kind == FormatKind::Nil {
        format!("%!{}(<nil>)", char::from(verb))
    } else {
        format!(
            "%!{}({}={})",
            char::from(verb),
            argument.type_name,
            argument.display
        )
    }
}

fn quoted_character(character: char) -> String {
    let escaped = match character {
        '\u{7}' => "\\a".to_owned(),
        '\u{8}' => "\\b".to_owned(),
        '\u{c}' => "\\f".to_owned(),
        '\n' => "\\n".to_owned(),
        '\r' => "\\r".to_owned(),
        '\t' => "\\t".to_owned(),
        '\u{b}' => "\\v".to_owned(),
        '\\' => "\\\\".to_owned(),
        '\'' => "\\'".to_owned(),
        value if value.is_control() => format!("\\u{:04x}", u32::from(value)),
        value => value.to_string(),
    };
    format!("'{escaped}'")
}

fn integer_character(argument: &FormatArg) -> char {
    let codepoint = match argument.kind {
        FormatKind::Signed => argument
            .display
            .parse::<i128>()
            .ok()
            .and_then(|value| u32::try_from(value).ok()),
        FormatKind::Unsigned => argument
            .display
            .parse::<u128>()
            .ok()
            .and_then(|value| u32::try_from(value).ok()),
        FormatKind::Char => argument.character.map(u32::from),
        _ => None,
    };
    codepoint
        .and_then(char::from_u32)
        .unwrap_or(char::REPLACEMENT_CHARACTER)
}

fn truncate(value: &str, precision: Option<usize>) -> String {
    precision.map_or_else(
        || value.to_owned(),
        |maximum| value.chars().take(maximum).collect(),
    )
}

#[derive(Clone, Copy, Debug, Default)]
struct FormatSpec {
    alternate: bool,
    left: bool,
    plus: bool,
    space: bool,
    zero: bool,
    width: Option<usize>,
    precision: Option<usize>,
}

fn pad_width(value: String, spec: FormatSpec, numeric: bool) -> String {
    let Some(padding) = spec
        .width
        .map(|width| width.saturating_sub(value.chars().count()))
        .filter(|padding| *padding > 0)
    else {
        return value;
    };
    if spec.left {
        return format!("{value}{}", " ".repeat(padding));
    }
    if numeric && spec.zero && spec.precision.is_none() {
        let sign_length = usize::from(value.starts_with(['+', '-', ' ']));
        let rest = &value[sign_length..];
        let prefix_length = sign_length
            + if rest.starts_with("0x") || rest.starts_with("0X") {
                2
            } else {
                0
            };
        let (prefix, rest) = value.split_at(prefix_length);
        return format!("{prefix}{}{rest}", "0".repeat(padding));
    }
    format!("{}{value}", " ".repeat(padding))
}

fn decimal_integer(argument: &FormatArg, spec: FormatSpec) -> String {
    let value = argument.display.as_str();
    let (negative, digits) = value
        .strip_prefix('-')
        .map_or((false, value), |digits| (true, digits));
    let sign = if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    let zeroes = spec
        .precision
        .map(|minimum| minimum.saturating_sub(digits.len()))
        .unwrap_or(0);
    pad_width(format!("{sign}{}{digits}", "0".repeat(zeroes)), spec, true)
}

fn binary_float_hex(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if matches!(argument.display.as_str(), "+Inf" | "-Inf" | "NaN") {
        return pad_width(argument.display.clone(), spec, true);
    }
    let (negative, exponent_bits, fraction, stored_fraction_bits, bias, subnormal_exponent) =
        if argument.type_name == "float32" {
            let bits = argument
                .display
                .parse::<f32>()
                .expect("stored finite float32")
                .to_bits();
            (
                bits >> 31 != 0,
                u64::from((bits >> 23) & 0xff),
                u64::from(bits & 0x7f_ffff),
                23_u32,
                127_i32,
                -149_i32,
            )
        } else {
            let bits = argument
                .display
                .parse::<f64>()
                .expect("stored finite float64")
                .to_bits();
            (
                bits >> 63 != 0,
                (bits >> 52) & 0x7ff,
                bits & 0x000f_ffff_ffff_ffff,
                52_u32,
                1023_i32,
                -1074_i32,
            )
        };
    if exponent_bits == 0 && fraction == 0 {
        let fraction = spec.precision.map_or_else(String::new, |precision| {
            if precision == 0 {
                String::new()
            } else {
                format!(".{}", "0".repeat(precision))
            }
        });
        let sign = if negative {
            "-"
        } else if spec.plus {
            "+"
        } else if spec.space {
            " "
        } else {
            ""
        };
        let point = if spec.alternate && fraction.is_empty() {
            "."
        } else {
            &fraction
        };
        let raw = format!(
            "{sign}{}{point}{}+00",
            if upper { "0X0" } else { "0x0" },
            if upper { 'P' } else { 'p' }
        );
        return pad_width(raw, spec, true);
    }
    let (mut exponent, fraction, fraction_bits) = if exponent_bits == 0 {
        let highest_bit = 63 - fraction.leading_zeros();
        let fraction_without_leader = fraction ^ (1_u64 << highest_bit);
        (
            subnormal_exponent + i32::try_from(highest_bit).expect("fraction bit index"),
            fraction_without_leader,
            highest_bit,
        )
    } else {
        (
            i32::try_from(exponent_bits).expect("small exponent") - bias,
            fraction,
            stored_fraction_bits,
        )
    };

    let exact_nibbles = fraction_bits.div_ceil(4);
    let aligned_fraction = fraction << (exact_nibbles * 4 - fraction_bits);
    let fraction = if let Some(precision) = spec.precision {
        let target_bits = precision.saturating_mul(4);
        if target_bits >= usize::try_from(fraction_bits).expect("small fraction width") {
            let exact_nibbles = usize::try_from(exact_nibbles).expect("small nibble count");
            let mut digits = if exact_nibbles == 0 {
                String::new()
            } else if upper {
                format!("{aligned_fraction:0exact_nibbles$X}")
            } else {
                format!("{aligned_fraction:0exact_nibbles$x}")
            };
            digits.push_str(&"0".repeat(precision.saturating_sub(exact_nibbles)));
            digits
        } else {
            let target_bits = u32::try_from(target_bits).expect("target below fraction width");
            let dropped = fraction_bits - target_bits;
            let significand = (1_u64 << fraction_bits) | fraction;
            let mut retained = significand >> dropped;
            let remainder = significand & ((1_u64 << dropped) - 1);
            let halfway = 1_u64 << (dropped - 1);
            if remainder > halfway || remainder == halfway && retained & 1 == 1 {
                retained += 1;
            }
            if retained == 2_u64 << target_bits {
                exponent += 1;
                retained = 1_u64 << target_bits;
            }
            let fraction = retained ^ (1_u64 << target_bits);
            if precision == 0 {
                String::new()
            } else if upper {
                format!("{fraction:0precision$X}")
            } else {
                format!("{fraction:0precision$x}")
            }
        }
    } else {
        let exact_nibbles = usize::try_from(exact_nibbles).expect("small nibble count");
        let mut digits = if upper {
            format!("{aligned_fraction:0exact_nibbles$X}")
        } else {
            format!("{aligned_fraction:0exact_nibbles$x}")
        };
        while digits.ends_with('0') {
            digits.pop();
        }
        digits
    };
    let point = if fraction.is_empty() && !spec.alternate {
        String::new()
    } else {
        format!(".{fraction}")
    };
    let prefix = if upper { "0X1" } else { "0x1" };
    let exponent_marker = if upper { 'P' } else { 'p' };
    let sign = if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    pad_width(
        format!("{sign}{prefix}{point}{exponent_marker}{exponent:+03}"),
        spec,
        true,
    )
}

fn apply_float_sign(value: String, spec: FormatSpec) -> String {
    if value.starts_with(['+', '-']) || value == "NaN" {
        value
    } else if spec.plus {
        format!("+{value}")
    } else if spec.space {
        format!(" {value}")
    } else {
        value
    }
}

fn scientific_float(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if matches!(argument.display.as_str(), "+Inf" | "-Inf" | "NaN") {
        return pad_width(apply_float_sign(argument.display.clone(), spec), spec, true);
    }
    let precision = spec.precision.unwrap_or(6);
    let raw = if argument.type_name == "float32" {
        let value = argument
            .display
            .parse::<f32>()
            .expect("stored finite float32");
        format!("{value:.precision$e}")
    } else {
        let value = argument
            .display
            .parse::<f64>()
            .expect("stored finite float64");
        format!("{value:.precision$e}")
    };
    let (mut mantissa, exponent) = raw.split_once('e').expect("scientific format exponent");
    let owned_mantissa;
    if spec.alternate && !mantissa.contains('.') {
        owned_mantissa = format!("{mantissa}.");
        mantissa = &owned_mantissa;
    }
    let exponent = exponent.parse::<i32>().expect("numeric exponent");
    let marker = if upper { 'E' } else { 'e' };
    pad_width(
        apply_float_sign(format!("{mantissa}{marker}{exponent:+03}"), spec),
        spec,
        true,
    )
}

fn general_float(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if matches!(argument.display.as_str(), "+Inf" | "-Inf" | "NaN") {
        return pad_width(apply_float_sign(argument.display.clone(), spec), spec, true);
    }
    let value = argument
        .display
        .parse::<f64>()
        .expect("stored finite float");
    let precision = spec.precision.unwrap_or(6).max(1);
    let exponent = if value == 0.0 {
        0
    } else {
        value.abs().log10().floor() as i32
    };
    let mut rendered = if exponent < -4 || exponent >= i32::try_from(precision).unwrap_or(i32::MAX)
    {
        let scientific_spec = FormatSpec {
            width: None,
            plus: false,
            space: false,
            zero: false,
            left: false,
            precision: Some(precision - 1),
            ..spec
        };
        scientific_float(argument, upper, scientific_spec)
    } else {
        let decimal_places =
            usize::try_from(i32::try_from(precision).unwrap_or(i32::MAX) - exponent - 1)
                .unwrap_or(0);
        if argument.type_name == "float32" {
            let value = argument.display.parse::<f32>().expect("stored float32");
            format!("{value:.decimal_places$}")
        } else {
            format!("{value:.decimal_places$}")
        }
    };
    if !spec.alternate {
        let exponent = rendered.find(['e', 'E']);
        let suffix = exponent.map(|position| rendered.split_off(position));
        if rendered.contains('.') {
            while rendered.ends_with('0') {
                rendered.pop();
            }
            if rendered.ends_with('.') {
                rendered.pop();
            }
        }
        if let Some(suffix) = suffix {
            rendered.push_str(&suffix);
        }
    } else if !rendered.contains('.') {
        if let Some(position) = rendered.find(['e', 'E']) {
            rendered.insert(position, '.');
        } else {
            rendered.push('.');
        }
    }
    if upper {
        rendered = rendered.replace('e', "E");
    }
    pad_width(apply_float_sign(rendered, spec), spec, true)
}

fn render_argument(verb: u8, spec: FormatSpec, argument: &FormatArg) -> String {
    match verb {
        b's' => match argument.kind {
            FormatKind::String | FormatKind::Custom => {
                pad_width(truncate(&argument.display, spec.precision), spec, false)
            }
            _ => mismatch(verb, argument),
        },
        b'd' => match argument.kind {
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char | FormatKind::Custom => {
                decimal_integer(argument, spec)
            }
            _ => mismatch(verb, argument),
        },
        b'q' => match argument.kind {
            FormatKind::String => pad_width(
                format!("{:?}", truncate(&argument.display, spec.precision)),
                spec,
                false,
            ),
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => {
                pad_width(quoted_character(integer_character(argument)), spec, false)
            }
            _ => mismatch(verb, argument),
        },
        b'c' => match argument.kind {
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => {
                pad_width(integer_character(argument).to_string(), spec, false)
            }
            _ => mismatch(verb, argument),
        },
        b'v' if spec.alternate => match argument.kind {
            FormatKind::String => pad_width(
                format!("{:?}", truncate(&argument.display, spec.precision)),
                spec,
                false,
            ),
            _ => pad_width(argument.debug.clone(), spec, false),
        },
        b'v' => match argument.kind {
            FormatKind::String | FormatKind::Custom => {
                pad_width(truncate(&argument.display, spec.precision), spec, false)
            }
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => {
                decimal_integer(argument, spec)
            }
            _ => pad_width(argument.display.clone(), spec, false),
        },
        b'T' => pad_width(argument.type_name.clone(), spec, false),
        b't' if argument.kind == FormatKind::Bool => {
            pad_width(argument.display.clone(), spec, false)
        }
        b'f' if argument.kind == FormatKind::Float => {
            if matches!(argument.display.as_str(), "+Inf" | "-Inf" | "NaN") {
                pad_width(apply_float_sign(argument.display.clone(), spec), spec, true)
            } else {
                let precision = spec.precision.unwrap_or(6);
                let mut rendered = if argument.type_name == "float32" {
                    argument.display.parse::<f32>().map_or_else(
                        |_| argument.display.clone(),
                        |value| format!("{value:.precision$}"),
                    )
                } else {
                    argument.display.parse::<f64>().map_or_else(
                        |_| argument.display.clone(),
                        |value| format!("{value:.precision$}"),
                    )
                };
                if spec.alternate && !rendered.contains('.') {
                    rendered.push('.');
                }
                pad_width(apply_float_sign(rendered, spec), spec, true)
            }
        }
        b'e' | b'E' if argument.kind == FormatKind::Float => {
            scientific_float(argument, verb == b'E', spec)
        }
        b'g' | b'G' if argument.kind == FormatKind::Float => {
            general_float(argument, verb == b'G', spec)
        }
        b'x' | b'X' if argument.kind == FormatKind::String => {
            let upper = verb == b'X';
            let bytes = argument.display.as_bytes();
            let bytes = &bytes[..spec.precision.unwrap_or(bytes.len()).min(bytes.len())];
            let rendered = bytes.iter().fold(
                String::with_capacity(bytes.len() * 2),
                |mut output, byte| {
                    use std::fmt::Write as _;
                    let _ = if upper {
                        write!(output, "{byte:02X}")
                    } else {
                        write!(output, "{byte:02x}")
                    };
                    output
                },
            );
            pad_width(rendered, spec, false)
        }
        b'x' | b'X' if argument.kind == FormatKind::Float => {
            binary_float_hex(argument, verb == b'X', spec)
        }
        b'x' | b'X' if argument.kind == FormatKind::Signed => {
            let value = argument
                .display
                .parse::<i128>()
                .expect("stored signed integer");
            let sign = if value < 0 {
                "-"
            } else if spec.plus {
                "+"
            } else if spec.space {
                " "
            } else {
                ""
            };
            let magnitude = value.unsigned_abs();
            let mut digits = if verb == b'X' {
                format!("{magnitude:X}")
            } else {
                format!("{magnitude:x}")
            };
            if let Some(precision) = spec.precision {
                digits.insert_str(0, &"0".repeat(precision.saturating_sub(digits.len())));
            }
            let prefix = if spec.alternate {
                if verb == b'X' {
                    "0X"
                } else {
                    "0x"
                }
            } else {
                ""
            };
            let width_spec = FormatSpec {
                width: spec.width.map(|width| {
                    width + usize::from(spec.zero && spec.precision.is_none()) * prefix.len()
                }),
                ..spec
            };
            pad_width(format!("{sign}{prefix}{digits}"), width_spec, true)
        }
        b'x' | b'X' if matches!(argument.kind, FormatKind::Unsigned | FormatKind::Char) => {
            let value = if argument.kind == FormatKind::Char {
                u128::from(u32::from(argument.character.expect("char argument")))
            } else {
                argument
                    .display
                    .parse::<u128>()
                    .expect("stored unsigned integer")
            };
            let prefix = if spec.alternate {
                if verb == b'X' {
                    "0X"
                } else {
                    "0x"
                }
            } else {
                ""
            };
            let mut digits = if verb == b'X' {
                format!("{value:X}")
            } else {
                format!("{value:x}")
            };
            if let Some(precision) = spec.precision {
                digits.insert_str(0, &"0".repeat(precision.saturating_sub(digits.len())));
            }
            let sign = if spec.plus {
                "+"
            } else if spec.space {
                " "
            } else {
                ""
            };
            let width_spec = FormatSpec {
                width: spec.width.map(|width| {
                    width + usize::from(spec.zero && spec.precision.is_none()) * prefix.len()
                }),
                ..spec
            };
            pad_width(format!("{sign}{prefix}{digits}"), width_spec, true)
        }
        _ => mismatch(verb, argument),
    }
}

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

        cursor += 1;
        let mut spec = FormatSpec::default();
        while let Some(flag @ (b'#' | b'-' | b'+' | b' ' | b'0')) = bytes.get(cursor).copied() {
            match flag {
                b'#' => spec.alternate = true,
                b'-' => spec.left = true,
                b'+' => spec.plus = true,
                b' ' => spec.space = true,
                b'0' => spec.zero = true,
                _ => unreachable!(),
            }
            cursor += 1;
        }
        let width_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor > width_start {
            spec.width = template[width_start..cursor].parse().ok();
        }

        let mut bad_precision = false;
        if bytes.get(cursor) == Some(&b'.') {
            cursor += 1;
            if bytes.get(cursor) == Some(&b'*') {
                let dynamic = args.get(argument_index).and_then(|arg| arg.precision);
                argument_index += 1;
                match dynamic {
                    Some(value) if value >= 0 => spec.precision = usize::try_from(value).ok(),
                    _ => bad_precision = true,
                }
                cursor += 1;
            } else {
                let start = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor += 1;
                }
                spec.precision = template[start..cursor].parse().ok();
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
        let rendered = render_argument(verb, spec, argument);
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
