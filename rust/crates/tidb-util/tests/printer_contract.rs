// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Public byte-string contract for Go `pkg/util/printer`.

use tidb_util::printer::get_print_result_bytes;

#[test]
fn table_rendering_preserves_go_string_bytes_and_byte_widths() {
    let columns: [&[u8]; 2] = [b"a\xff", b"b"];
    let rows: Vec<Vec<&[u8]>> = vec![vec![b"x", b"y\xfe"]];

    assert_eq!(
        get_print_result_bytes(&columns, &rows).as_deref(),
        Some(&b"+----+----+\n| a\xff | b  |\n+----+----+\n| x  | y\xfe |\n+----+----+\n"[..])
    );
}
