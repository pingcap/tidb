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

//! Process-per-input harness for source `FuzzJSONExtract`.
//!
//! Input is `path_len:u32 little-endian || path UTF-8 || JSON UTF-8`. Invalid
//! UTF-8, JSON, and paths are uninteresting inputs, exactly as in the source
//! target. Any extracted value must retain a valid nonzero binary-JSON type.

use std::io::{self, Read};

use tidb_datatype::{parse_json_path_expr, BinaryJSON};

fn main() {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input).unwrap();
    if input.len() < 4 {
        return;
    }
    let path_len = u32::from_le_bytes(input[..4].try_into().unwrap()) as usize;
    if path_len > input.len() - 4 {
        return;
    }
    let Ok(path) = std::str::from_utf8(&input[4..4 + path_len]) else {
        return;
    };
    let Ok(document) = std::str::from_utf8(&input[4 + path_len..]) else {
        return;
    };
    let Ok(document) = BinaryJSON::parse(document) else {
        return;
    };
    let Ok(path) = parse_json_path_expr(path) else {
        return;
    };
    if let Some(extracted) = document.extract(&[path]).unwrap() {
        assert_ne!(extracted.type_code(), 0);
        BinaryJSON::from_raw(extracted.type_code(), extracted.value().to_vec()).unwrap();
    }
}
