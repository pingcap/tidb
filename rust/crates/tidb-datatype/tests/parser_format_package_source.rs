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
    let mut output = Vec::new();
    let mut indented = IndentFormatter::new(&mut output, "\t");
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
    drop(indented);
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e\n\tx\n\ty\nz\n");

    let mut output = Vec::new();
    let mut flat = FlatFormatter::new(&mut output);
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
    drop(flat);
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e x y z\n ");
}

#[test]
fn formatter_state_crosses_calls() {
    let mut output = Vec::new();
    let mut indented = IndentFormatter::new(&mut output, "\t");
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
    drop(indented);
    assert_eq!(output, b"abc3%e\n\tx\n\ty\nz\n");

    let mut output = Vec::new();
    let mut flat = FlatFormatter::new(&mut output);
    flat.format(&[F::text("top"), F::Indent, F::text("\nchild\n")])
        .unwrap();
    flat.format(&[F::text("next"), F::Unindent, F::text("\ntail\n")])
        .unwrap();
    drop(flat);
    assert_eq!(output, b"top child next\ntail\n");
}

#[test]
fn formatted_values_are_opaque_to_commands_and_newline_state() {
    let mut output = Vec::new();
    let mut indented = IndentFormatter::new(&mut output, "  ");
    indented
        .format(&[
            F::Indent,
            F::value(format_args!("%i\n%%u")),
            F::text("\ntail\n"),
        ])
        .unwrap();
    drop(indented);
    assert_eq!(output, b"  %i\n%%u\n  tail\n");
}

#[test]
fn formatter_preserves_non_utf8_template_and_value_bytes() {
    let mut output = Vec::new();
    let mut indented = IndentFormatter::new(&mut output, "  ");
    indented
        .format(&[
            F::Indent,
            F::text_bytes(&[0xff, b'\n']),
            F::value_bytes(&[0xfe, b'\n']),
            F::text_bytes(&[b'\n', 0xfd]),
        ])
        .unwrap();
    drop(indented);
    assert_eq!(
        output,
        &[b' ', b' ', 0xff, b'\n', b' ', b' ', 0xfe, b'\n', b'\n', b' ', b' ', 0xfd]
    );
}

#[test]
fn formatter_uses_native_width_precision_indexing_and_radix() {
    let mut output = Vec::new();
    let mut formatter = IndentFormatter::new(&mut output, "  ");
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
    drop(formatter);
    assert_eq!(
        output,
        "00000042 abc 0x2a \"雪\"\n  body\n".as_bytes()
    );
}

#[derive(Default)]
struct BoundedWriter {
    bytes: Vec<u8>,
    chunk_size: usize,
    calls: usize,
    fail: bool,
}

impl Write for BoundedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.fail {
            self.fail = false;
            return Err(io::Error::other("writer failed"));
        }
        let written = self.chunk_size.min(buffer.len());
        self.bytes.extend_from_slice(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn formatter_performs_one_source_write_and_propagates_errors() {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        chunk_size: 2,
        calls: 0,
        fail: false,
    };
    let mut formatter = IndentFormatter::new(&mut writer, "  ");
    assert_eq!(formatter.format(&[F::text("abcdef")]).unwrap(), 2);
    drop(formatter);
    assert_eq!(writer.calls, 1);
    assert_eq!(writer.bytes, b"ab");

    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        chunk_size: 0,
        calls: 0,
        fail: false,
    };
    let mut formatter = IndentFormatter::new(&mut writer, "  ");
    assert_eq!(formatter.format(&[F::text("abcdef")]).unwrap(), 0);
    drop(formatter);
    assert_eq!(writer.calls, 1);
    assert!(writer.bytes.is_empty());

    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        chunk_size: usize::MAX,
        calls: 0,
        fail: true,
    };
    let mut formatter = IndentFormatter::new(&mut writer, "  ");
    let error = formatter
        .format(&[F::text("abcdef")])
        .expect_err("writer errors must be returned");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    drop(formatter);
    assert_eq!(writer.calls, 1);
    assert!(writer.bytes.is_empty());
}

#[test]
fn formatter_state_advances_before_errors_and_direct_writes_are_opaque() {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        chunk_size: usize::MAX,
        calls: 0,
        fail: true,
    };
    let mut formatter = IndentFormatter::new(&mut writer, "  ");
    formatter
        .format(&[F::text("a"), F::Indent, F::text("\n")])
        .unwrap_err();
    assert_eq!(formatter.format(&[F::text("b\n")]).unwrap(), 4);
    drop(formatter);
    assert_eq!(writer.calls, 2);
    assert_eq!(writer.bytes, b"  b\n");

    let mut output = Vec::new();
    let mut formatter = IndentFormatter::new(&mut output, "  ");
    assert_eq!(formatter.format(&[F::Indent]).unwrap(), 0);
    formatter.write_all(b"raw\n").unwrap();
    assert_eq!(formatter.format(&[F::text("tail\n")]).unwrap(), 7);
    drop(formatter);
    assert_eq!(output, b"raw\n  tail\n");
}

#[test]
fn output_format_uses_the_exact_current_replacement_set() {
    assert_eq!(output_format("'\0abc\n\rdef"), "''\\0abc\\n\\rdef");
    assert_eq!(
        output_format("plain\\slash'quote\0nul\nline\rcarriage\t雪"),
        "plain\\slash''quote\\0nul\\nline\\rcarriage\t雪"
    );
}
