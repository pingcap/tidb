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

use tidb_util::format::{
    output_format, FlatFormatter, FormatFragment as F, Formatter, IndentFormatter,
};

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
