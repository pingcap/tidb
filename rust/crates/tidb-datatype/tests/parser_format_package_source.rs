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

//! Complete formatter/output half of `pkg/parser/format`.

use std::io::{self, Write};

use tidb_datatype::{
    output_format, FlatFormatter, FormatFragment as F, Formatter, IndentFormatter,
};

#[test]
fn original_test_format() {
    let mut indented = IndentFormatter::new(Vec::new(), "\t");
    let written = indented
        .format(&[
            F::text("abc"),
            F::value(format_args!("{}", 3)),
            F::text("%e"),
            F::Indent,
            F::text("\nx\ny\n"),
            F::Unindent,
            F::text("z\n"),
        ])
        .expect("formatting should succeed");
    let output = indented.into_inner();
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e\n\tx\n\ty\nz\n");

    let mut flat = FlatFormatter::new(Vec::new());
    let written = flat
        .format(&[
            F::text("abc"),
            F::value(format_args!("{}", 3)),
            F::text("%e"),
            F::Indent,
            F::text("\nx\ny\n"),
            F::Unindent,
            F::text("z\n"),
            F::Indent,
            F::text("\n"),
        ])
        .expect("formatting should succeed");
    let output = flat.into_inner();
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e x y z\n ");
}

#[test]
fn formatter_state_crosses_calls() {
    let mut indented = IndentFormatter::new(Vec::new(), "\t");
    indented
        .format(&[
            F::text("abc"),
            F::value(format_args!("{}", 3)),
            F::text("%e"),
            F::Indent,
            F::text("\nx\n"),
        ])
        .unwrap();
    indented
        .format(&[F::text("y\n"), F::Unindent, F::text("z\n")])
        .unwrap();
    assert_eq!(indented.into_inner(), b"abc3%e\n\tx\n\ty\nz\n");

    let mut flat = FlatFormatter::new(Vec::new());
    flat.format(&[F::text("top"), F::Indent, F::text("\nchild\n")])
        .unwrap();
    flat.format(&[F::text("next"), F::Unindent, F::text("\ntail\n")])
        .unwrap();
    assert_eq!(flat.into_inner(), b"top child next\ntail\n");
}

#[test]
fn formatted_values_are_opaque_to_commands_and_newline_state() {
    let mut indented = IndentFormatter::new(Vec::new(), "  ");
    indented
        .format(&[
            F::Indent,
            F::value(format_args!("%i\n%%u")),
            F::text("\ntail\n"),
        ])
        .unwrap();
    assert_eq!(indented.into_inner(), b"  %i\n%%u\n  tail\n");
}

#[test]
fn formatter_uses_native_width_precision_indexing_and_radix() {
    let mut formatter = IndentFormatter::new(Vec::new(), "  ");
    formatter
        .format(&[
            F::value(format_args!(
                "{1:08} {0:.3} {1:#x} {name:?}",
                "abcdef",
                42,
                name = "雪"
            )),
            F::Indent,
            F::text("\nbody\n"),
            F::Unindent,
        ])
        .unwrap();
    assert_eq!(
        formatter.into_inner(),
        "00000042 abc 0x2a \"雪\"\n  body\n".as_bytes()
    );
}

#[derive(Default)]
struct FailingWriter {
    bytes: Vec<u8>,
    remaining: usize,
}

impl Write for FailingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("writer failed"));
        }
        let written = self.remaining.min(buffer.len());
        self.bytes.extend_from_slice(&buffer[..written]);
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn formatter_error_retains_partial_write_count() {
    let writer = FailingWriter {
        bytes: Vec::new(),
        remaining: 4,
    };
    let mut formatter = IndentFormatter::new(writer, "  ");
    let error = formatter
        .format(&[F::text("abcdef")])
        .expect_err("writer must fail after four bytes");
    assert_eq!(error.written, 4);
    assert_eq!(error.error.kind(), io::ErrorKind::Other);
    assert_eq!(formatter.into_inner().bytes, b"abcd");
}

#[test]
fn output_format_uses_the_exact_current_replacement_set() {
    assert_eq!(output_format("'\0abc\n\rdef"), "''\\0abc\\n\\rdef");
    assert_eq!(
        output_format("plain\\slash'quote\0nul\nline\rcarriage\t雪"),
        "plain\\slash''quote\\0nul\\nline\\rcarriage\t雪"
    );
}
