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

//! Benchmark translation for `BenchmarkGetCharsetDesc`.

use std::hint::black_box;

fn main() {
    if cfg!(test) {
        return;
    }

    let charsets = ["utf8", "utf8mb4", "ascii", "latin1", "binary"];
    let charset = charsets[black_box(0)];
    for _ in 0..1_000_000 {
        let _ = black_box(tidb_datatype::get_charset_info(black_box(charset)));
    }
}
