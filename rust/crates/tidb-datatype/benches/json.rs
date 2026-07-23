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

//! Executable equivalents of the JSON benchmarks in `pkg/types`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::{
    decode_escaped_unicode, merge_binary_json, merge_patch_binary_json, BinaryJSON,
};

const ITERATIONS: usize = 10_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn main() {
    if cfg!(test) {
        return;
    }

    let marshal = BinaryJSON::parse(r#"{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}"#)
        .expect("static benchmark JSON");
    measure("BenchmarkBinaryMarshal", || {
        black_box(black_box(&marshal).to_string());
    });

    measure("BenchmarkDecodeEscapedUnicode", || {
        black_box(
            decode_escaped_unicode(black_box(b"D83DDE0A"))
                .expect("static escaped Unicode benchmark input"),
        );
    });

    let left = BinaryJSON::parse(
        r#"{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}"#,
    )
    .expect("static benchmark JSON");
    let right = BinaryJSON::parse(
        r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}"#,
    )
    .expect("static benchmark JSON");

    measure("BenchmarkMergePatchBinary", || {
        black_box(
            merge_patch_binary_json(black_box(&[left.clone(), right.clone()]))
                .expect("static merge-patch inputs"),
        );
    });
    measure("BenchmarkMergeBinary", || {
        black_box(
            merge_binary_json(black_box(&[left.clone(), right.clone()]))
                .expect("static merge inputs"),
        );
    });
}
