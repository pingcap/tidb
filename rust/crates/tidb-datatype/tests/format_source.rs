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

//! Direct assertions from `pkg/parser/format/format_test.go`.

use tidb_datatype::{
    output_format, FlatFormatter, FormatFragment as F, Formatter, IndentFormatter,
};

#[test]
fn test_format() {
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

    assert_eq!(output_format("'\0abc\n\rdef"), "''\\0abc\\n\\rdef");
}

#[test]
fn formatter_state_crosses_calls_and_values_remain_opaque() {
    let mut indented = IndentFormatter::new(Vec::new(), "  ");
    indented
        .format(&[F::text("root"), F::Indent, F::text("\nchild\n")])
        .unwrap();
    indented
        .format(&[
            F::text("next "),
            F::value(format_args!("%i/%u/%%\n")),
            F::Unindent,
            F::text("tail\n"),
        ])
        .unwrap();
    assert_eq!(
        indented.into_inner(),
        b"root\n  child\n  next %i/%u/%%\ntail\n"
    );
}

#[test]
fn output_format_escapes_the_complete_source_replacement_set() {
    assert_eq!(
        output_format("plain\\slash'quote\0nul\nline\rcarriage\t雪"),
        "plain\\slash''quote\\0nul\\nline\\rcarriage\t雪"
    );
}
