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

//! Stateful text formatting from `pkg/util/format/format.go`.

use std::fmt;
use std::io::{self, Write};

const STATE_TEXT: u8 = 0;
const STATE_BEGIN_LINE: u8 = 1;
const STATE_PERCENT: u8 = 2;
const STATE_BEGIN_LINE_PERCENT: u8 = 3;

/// The source formatter contract: a writer with stateful `%i` and `%u`
/// indentation commands.
///
/// Rust has no variadic equivalent of `fmt.Fprintf`, so arguments are passed as
/// display trait objects. The source tests use `%d`; `%s` and `%v` share the
/// same display substitution while `%%` emits a literal percent sign.
pub trait Formatter: Write {
    /// Rewrites indentation commands, substitutes the supplied display
    /// arguments, writes the result, and returns the written byte count.
    fn format(&mut self, format: &str, args: &[&dyn fmt::Display]) -> io::Result<usize>;
}

/// Stateful formatter returned by Go's `IndentFormatter` constructor.
pub struct IndentFormatter<W> {
    writer: W,
    indent: Vec<u8>,
    indent_level: isize,
    state: u8,
}

impl<W: Write> IndentFormatter<W> {
    /// Creates an indented formatter over `writer` using one `indent` per level.
    pub fn new(writer: W, indent: impl AsRef<str>) -> Self {
        Self {
            writer,
            indent: indent.as_ref().as_bytes().to_vec(),
            indent_level: 0,
            state: STATE_BEGIN_LINE,
        }
    }

    /// Consumes the formatter and returns its underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }

    fn format_inner(
        &mut self,
        flat: bool,
        format: &str,
        args: &[&dyn fmt::Display],
    ) -> io::Result<usize> {
        let mut buffer = Vec::with_capacity(format.len());
        for &byte in format.as_bytes() {
            match self.state {
                STATE_TEXT => match byte {
                    b'\n' => {
                        buffer.push(if flat && self.indent_level != 0 {
                            b' '
                        } else {
                            byte
                        });
                        self.state = STATE_BEGIN_LINE;
                    }
                    b'%' => self.state = STATE_PERCENT,
                    _ => buffer.push(byte),
                },
                STATE_BEGIN_LINE => match byte {
                    b'\n' => buffer.push(if flat && self.indent_level != 0 {
                        b' '
                    } else {
                        byte
                    }),
                    b'%' => self.state = STATE_BEGIN_LINE_PERCENT,
                    _ => {
                        self.push_indent(flat, &mut buffer);
                        buffer.push(byte);
                        self.state = STATE_TEXT;
                    }
                },
                STATE_BEGIN_LINE_PERCENT => match byte {
                    b'i' => {
                        self.indent_level += 1;
                        self.state = STATE_BEGIN_LINE;
                    }
                    b'u' => {
                        self.indent_level -= 1;
                        self.state = STATE_BEGIN_LINE;
                    }
                    _ => {
                        self.push_indent(flat, &mut buffer);
                        buffer.extend_from_slice(&[b'%', byte]);
                        self.state = STATE_TEXT;
                    }
                },
                STATE_PERCENT => match byte {
                    b'i' => {
                        self.indent_level += 1;
                        self.state = STATE_TEXT;
                    }
                    b'u' => {
                        self.indent_level -= 1;
                        self.state = STATE_TEXT;
                    }
                    _ => {
                        buffer.extend_from_slice(&[b'%', byte]);
                        self.state = STATE_TEXT;
                    }
                },
                _ => unreachable!("formatter state is private and always valid"),
            }
        }

        // This deliberately does not reset the state. Go appends the dangling
        // percent to the fmt string while retaining stPERC/stBOLPERC for the
        // next call; fmt.Fprintf renders that terminal percent as NOVERB.
        if matches!(self.state, STATE_PERCENT | STATE_BEGIN_LINE_PERCENT) {
            buffer.push(b'%');
        }

        let substituted = substitute_display_arguments(&buffer, args)?;
        write_counted(&mut self.writer, &substituted)
    }

    fn push_indent(&self, flat: bool, buffer: &mut Vec<u8>) {
        if flat || self.indent_level <= 0 {
            return;
        }
        for _ in 0..self.indent_level {
            buffer.extend_from_slice(&self.indent);
        }
    }
}

impl<W: Write> Write for IndentFormatter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.writer.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write> Formatter for IndentFormatter<W> {
    fn format(&mut self, format: &str, args: &[&dyn fmt::Display]) -> io::Result<usize> {
        self.format_inner(false, format, args)
    }
}

/// Stateful formatter returned by Go's `FlatFormatter` constructor.
pub struct FlatFormatter<W>(IndentFormatter<W>);

impl<W: Write> FlatFormatter<W> {
    /// Creates a formatter that flattens indented newlines to spaces.
    pub fn new(writer: W) -> Self {
        Self(IndentFormatter::new(writer, ""))
    }

    /// Consumes the formatter and returns its underlying writer.
    pub fn into_inner(self) -> W {
        self.0.into_inner()
    }
}

impl<W: Write> Write for FlatFormatter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<W: Write> Formatter for FlatFormatter<W> {
    fn format(&mut self, format: &str, args: &[&dyn fmt::Display]) -> io::Result<usize> {
        self.0.format_inner(true, format, args)
    }
}

/// Applies TiDB's SQL display escaping exactly: NUL, quote, newline, carriage
/// return, and backslash are replaced; all other Unicode scalar values remain.
pub fn output_format(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\0' => output.push_str("\\0"),
            '\'' => output.push_str("''"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\\' => output.push_str("\\\\"),
            _ => output.push(character),
        }
    }
    output
}

fn substitute_display_arguments(format: &[u8], args: &[&dyn fmt::Display]) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(format.len());
    let mut index = 0;
    let mut argument = 0;
    while index < format.len() {
        if format[index] != b'%' {
            output.push(format[index]);
            index += 1;
            continue;
        }
        if format.get(index + 1) == Some(&b'%') {
            output.push(b'%');
            index += 2;
            continue;
        }
        let Some(&verb) = format.get(index + 1) else {
            output.extend_from_slice(b"%!(NOVERB)");
            index += 1;
            continue;
        };
        if !matches!(verb, b'd' | b's' | b'v') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported printf verb %{}", char::from(verb)),
            ));
        }
        let value = args.get(argument).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing printf argument")
        })?;
        output.extend_from_slice(value.to_string().as_bytes());
        argument += 1;
        index += 2;
    }
    if argument != args.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unused printf argument",
        ));
    }
    Ok(output)
}

fn write_counted(writer: &mut impl Write, mut buffer: &[u8]) -> io::Result<usize> {
    let total = buffer.len();
    while !buffer.is_empty() {
        let written = writer.write(buffer)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write formatted output",
            ));
        }
        buffer = &buffer[written..];
    }
    Ok(total)
}
