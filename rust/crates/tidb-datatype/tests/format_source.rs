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

//! Direct assertions from `pkg/util/format/format_test.go`.

use tidb_datatype::{output_format, FlatFormatter, Formatter, IndentFormatter};

#[test]
fn test_format() {
    let mut indented = IndentFormatter::new(Vec::new(), "\t");
    let written = indented
        .format("abc%d%%e%i\nx\ny\n%uz\n", &[&3])
        .expect("formatting should succeed");
    let output = indented.into_inner();
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e\n\tx\n\ty\nz\n");

    let mut flat = FlatFormatter::new(Vec::new());
    let written = flat
        .format("abc%d%%e%i\nx\ny\n%uz\n%i\n", &[&3])
        .expect("formatting should succeed");
    let output = flat.into_inner();
    assert_eq!(written, output.len());
    assert_eq!(output, b"abc3%e x y z\n ");

    assert_eq!(output_format("'\0abc\n\rdef"), "''\\0abc\\n\\rdef");
}

#[test]
fn formatter_state_crosses_calls_and_retains_dangling_percent_state() {
    let mut indented = IndentFormatter::new(Vec::new(), "  ");
    indented.format("root%i\nchild\n", &[]).unwrap();
    indented.format("next%", &[]).unwrap();
    // fmt.Fprintf renders the dangling percent as NOVERB, while the formatter
    // keeps stPERC. The next call's leading `u` is the pending unindent.
    indented.format("u\ntail\n", &[]).unwrap();
    assert_eq!(
        indented.into_inner(),
        b"root\n  child\n  next%!(NOVERB)\ntail\n"
    );
}

#[test]
fn output_format_escapes_the_complete_source_replacement_set() {
    assert_eq!(
        output_format("plain\\slash'quote\0nul\nline\rcarriage\t雪"),
        "plain\\\\slash''quote\\0nul\\nline\\rcarriage\t雪"
    );
}
