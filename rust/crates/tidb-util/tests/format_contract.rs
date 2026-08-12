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

//! Executable package contract for `pkg/util/format`.

use std::io::{self, Write};

use tidb_util::format::{
    output_format, FlatFormatter, FormatFragment as F, Formatter, IndentFormatter,
};

struct ObservingWriter {
    bytes: Vec<u8>,
    limit: usize,
    calls: usize,
    fail: bool,
}

impl Write for ObservingWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.fail {
            return Err(io::Error::other("writer failed"));
        }
        let written = self.limit.min(input.len());
        self.bytes.extend_from_slice(&input[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn format_package_semantics() {
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
        .unwrap();
    let output = indented.into_inner();
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e\n\tx\n\ty\nz\n");

    let mut flat = FlatFormatter::new(Vec::new());
    flat.format(&[
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
    .unwrap();
    assert_eq!(flat.into_inner(), b"abc3%e x y z\n ");

    assert_eq!(
        output_format("slash\\quote'\0nul\nline\rcarriage"),
        "slash\\\\quote''\\0nul\\nline\\rcarriage"
    );
}

#[test]
fn output_format_preserves_go_string_byte_domain() {
    assert_eq!(
        output_format(&b"bad:\xff|truncated:\xe2\x82|quote:'|slash:\\|nul:\0"[..]),
        "bad:\u{fffd}|truncated:\u{fffd}\u{fffd}|quote:''|slash:\\\\|nul:\\0"
    );
}

#[test]
fn formatter_performs_one_source_write_and_returns_its_count() {
    let writer = ObservingWriter {
        bytes: Vec::new(),
        limit: 3,
        calls: 0,
        fail: false,
    };
    let mut formatter = IndentFormatter::new(writer, "  ");

    assert_eq!(formatter.format(&[F::text("abcdef")]).unwrap(), 3);
    let writer = formatter.into_inner();
    assert_eq!(writer.calls, 1);
    assert_eq!(writer.bytes, b"abc");
}

#[test]
fn empty_format_still_observes_the_source_writer() {
    let writer = ObservingWriter {
        bytes: Vec::new(),
        limit: usize::MAX,
        calls: 0,
        fail: true,
    };
    let mut formatter = IndentFormatter::new(writer, "  ");

    assert_eq!(
        formatter.format(&[]).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    let writer = formatter.into_inner();
    assert_eq!(writer.calls, 1);
    assert!(writer.bytes.is_empty());
}
