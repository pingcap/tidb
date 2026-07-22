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

//! Stateful text formatting from `pkg/parser/format/format.go`.

use std::fmt;
use std::io::{self, Write};

const STATE_TEXT: u8 = 0;
const STATE_BEGIN_LINE: u8 = 1;

/// One typed part of a formatter template.
///
/// Commands are structural rather than encoded into a fully rendered string,
/// so formatted values can contain `%i`, `%u`, percent signs, or newlines
/// without being reinterpreted by the indentation state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatFragment<'a> {
    /// Literal template text. Newlines participate in indentation state.
    Text(&'a str),
    /// Opaque, preformatted value text.
    Value(String),
    /// Increase the indentation level.
    Indent,
    /// Decrease the indentation level.
    Unindent,
}

impl<'a> FormatFragment<'a> {
    /// Creates literal template text.
    pub const fn text(text: &'a str) -> Self {
        Self::Text(text)
    }

    /// Formats a value with Rust's complete native formatting surface and
    /// stores the result as opaque text.
    pub fn value(arguments: fmt::Arguments<'_>) -> Self {
        Self::Value(fmt::format(arguments))
    }
}

/// The source formatter contract: a writer with stateful indent and unindent
/// commands.
///
/// Go embeds `%i` and `%u` beside printf verbs in one runtime string. Rust uses
/// [`FormatFragment`] to separate template text, formatted values, and the two
/// commands. That preserves the full native formatting surface (width,
/// precision, positional arguments, radix, and custom `Display`/`Debug`) and
/// guarantees that command-looking bytes inside a value remain ordinary data.
pub trait Formatter: Write {
    /// Applies `fragments`, writes the result, and returns the written byte
    /// count.
    fn format(&mut self, fragments: &[FormatFragment<'_>]) -> Result<usize, FormatWriteError>;
}

/// A formatter write failure retaining the exact successful byte count.
#[derive(Debug)]
pub struct FormatWriteError {
    /// Bytes successfully written before `error`.
    pub written: usize,
    /// The underlying writer error.
    pub error: io::Error,
}

impl fmt::Display for FormatWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "formatter write failed after {} bytes: {}",
            self.written, self.error
        )
    }
}

impl std::error::Error for FormatWriteError {}

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
        fragments: &[FormatFragment<'_>],
    ) -> Result<usize, FormatWriteError> {
        let mut buffer = Vec::new();
        for fragment in fragments {
            match fragment {
                FormatFragment::Text(text) => {
                    self.push_template_text(flat, text.as_bytes(), &mut buffer);
                }
                FormatFragment::Value(value) => {
                    if self.state == STATE_BEGIN_LINE {
                        self.push_indent(flat, &mut buffer);
                    }
                    buffer.extend_from_slice(value.as_bytes());
                    // A source printf verb moves the template state out of
                    // beginning-of-line even when its formatted value is empty.
                    // Newlines inside the value are opaque and do not alter it.
                    self.state = STATE_TEXT;
                }
                FormatFragment::Indent => self.indent_level += 1,
                FormatFragment::Unindent => self.indent_level -= 1,
            }
        }
        write_counted(&mut self.writer, &buffer)
    }

    fn push_template_text(&mut self, flat: bool, text: &[u8], buffer: &mut Vec<u8>) {
        for &byte in text {
            if byte == b'\n' {
                buffer.push(if flat && self.indent_level != 0 {
                    b' '
                } else {
                    byte
                });
                self.state = STATE_BEGIN_LINE;
            } else {
                if self.state == STATE_BEGIN_LINE {
                    self.push_indent(flat, buffer);
                }
                buffer.push(byte);
                self.state = STATE_TEXT;
            }
        }
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
    fn format(&mut self, fragments: &[FormatFragment<'_>]) -> Result<usize, FormatWriteError> {
        self.format_inner(false, fragments)
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
    fn format(&mut self, fragments: &[FormatFragment<'_>]) -> Result<usize, FormatWriteError> {
        self.0.format_inner(true, fragments)
    }
}

/// Applies TiDB's SQL display escaping exactly: NUL, single quote, newline, and
/// carriage return are replaced. Backslashes and all other Unicode scalar
/// values remain unchanged.
pub fn output_format(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\0' => output.push_str("\\0"),
            '\'' => output.push_str("''"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            _ => output.push(character),
        }
    }
    output
}

fn write_counted(writer: &mut impl Write, mut buffer: &[u8]) -> Result<usize, FormatWriteError> {
    let total = buffer.len();
    let mut written_total = 0;
    while !buffer.is_empty() {
        let written = writer.write(buffer).map_err(|error| FormatWriteError {
            written: written_total,
            error,
        })?;
        if written == 0 {
            return Err(FormatWriteError {
                written: written_total,
                error: io::Error::new(io::ErrorKind::WriteZero, "failed to write formatted output"),
            });
        }
        written_total += written;
        buffer = &buffer[written..];
    }
    Ok(total)
}
