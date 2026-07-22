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

//! Stable Rust owner for Go's `BenchmarkConvertBinaryStringLiterals`.

use std::hint::black_box;

use tidb_ast::NodeText;
use tidb_datatype::Encoding;

fn query(clause: &str, count: usize) -> Vec<u8> {
    format!(
        "SELECT * FROM t1 WHERE {}",
        vec![clause; count].join(" OR ")
    )
    .into_bytes()
}

fn mixed_query(count: usize) -> Vec<u8> {
    let binary = "c1 = _binary '\u{1}\u{2}\u{3}'";
    let printable = "c1 = 'hello world'";
    let clauses = (0..count)
        .map(|index| if index % 2 == 0 { binary } else { printable })
        .collect::<Vec<_>>();
    format!("SELECT * FROM t1 WHERE {}", clauses.join(" OR ")).into_bytes()
}

fn main() {
    let cases = [
        query("c1 = 12345", 1),
        query("c1 = 12345", 200),
        query("c1 = 'hello world'", 1),
        query("c1 = 'hello world'", 200),
        query("c1 = _binary '\u{1}\u{2}\u{3}'", 1),
        query("c1 = _binary '\u{1}\u{2}\u{3}'", 200),
        mixed_query(2),
        mixed_query(200),
    ];

    for source in cases {
        for _ in 0..1_000 {
            let mut node = NodeText::default();
            node.set_text(Some(Encoding::Utf8), black_box(source.clone()));
            black_box(node.text());
        }
    }
}
