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

use super::*;

/// Go `TestProfileToDatum` (`flamegraph_test.go`).
#[test]
fn test_profile_to_datum() {
    let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/test.pprof")).expect("testdata/test.pprof");
    let data = Collector.profile_reader_to_datums(&data).expect("profile parses");
    let datums: &[(&str, &str, &str, i64, i64, &str)] = &[
        ("root", "100%", "100%", 0, 0, "root"),
        ("├─runtime.main", "87.50%", "87.50%", 1, 1, "c:/go/src/runtime/proc.go:203"),
        ("│ └─main.main", "87.50%", "100%", 1, 2, "Z:/main.go:46"),
        ("│   ├─main.collatz", "68.75%", "78.57%", 1, 3, "Z:/main.go:22"),
        ("│   │ └─crypto/cipher.(*ctr).XORKeyStream", "68.75%", "100%", 1, 4, "c:/go/src/crypto/cipher/ctr.go:84"),
        ("│   │   ├─crypto/cipher.(*ctr).refill", "62.50%", "90.91%", 1, 5, "c:/go/src/crypto/cipher/ctr.go:60"),
        ("│   │   │ ├─crypto/aes.(*aesCipherAsm).Encrypt", "56.25%", "90.00%", 1, 6, "c:/go/src/crypto/aes/cipher_asm.go:68"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "12.50%", "22.22%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:49"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:45"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:39"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:37"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:43"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:41"),
        ("│   │   │ │ ├─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:51"),
        ("│   │   │ │ └─crypto/aes.encryptBlockAsm", "6.25%", "11.11%", 1, 7, "c:/go/src/crypto/aes/asm_amd64.s:11"),
        ("│   │   │ └─crypto/aes.(*aesCipherAsm).Encrypt", "6.25%", "10.00%", 1, 6, "c:/go/src/crypto/aes/cipher_asm.go:58"),
        ("│   │   └─crypto/cipher.(*ctr).refill", "6.25%", "9.09%", 1, 5, "c:/go/src/crypto/cipher/ctr.go:60"),
        ("│   ├─main.collatz", "12.50%", "14.29%", 1, 3, "Z:/main.go:30"),
        ("│   │ └─main.collatz", "12.50%", "100%", 1, 4, "Z:/main.go:22"),
        ("│   │   └─crypto/cipher.(*ctr).XORKeyStream", "12.50%", "100%", 1, 5, "c:/go/src/crypto/cipher/ctr.go:84"),
        ("│   │     └─crypto/cipher.(*ctr).refill", "12.50%", "100%", 1, 6, "c:/go/src/crypto/cipher/ctr.go:60"),
        ("│   │       ├─crypto/aes.(*aesCipherAsm).Encrypt", "6.25%", "50.00%", 1, 7, "c:/go/src/crypto/aes/cipher_asm.go:68"),
        ("│   │       │ └─crypto/aes.encryptBlockAsm", "6.25%", "100%", 1, 8, "c:/go/src/crypto/aes/asm_amd64.s:45"),
        ("│   │       └─crypto/aes.(*aesCipherAsm).Encrypt", "6.25%", "50.00%", 1, 7, "c:/go/src/crypto/aes/cipher_asm.go:65"),
        ("│   │         └─crypto/internal/subtle.InexactOverlap", "6.25%", "100%", 1, 8, "c:/go/src/crypto/internal/subtle/aliasing.go:33"),
        ("│   │           └─crypto/internal/subtle.AnyOverlap", "6.25%", "100%", 1, 9, "c:/go/src/crypto/internal/subtle/aliasing.go:20"),
        ("│   └─main.collatz", "6.25%", "7.14%", 1, 3, "Z:/main.go:20"),
        ("│     └─runtime.memmove", "6.25%", "100%", 1, 4, "c:/go/src/runtime/memmove_amd64.s:362"),
        ("├─runtime.mstart", "6.25%", "6.25%", 2, 1, "c:/go/src/runtime/proc.go:1146"),
        ("│ └─runtime.systemstack", "6.25%", "100%", 2, 2, "c:/go/src/runtime/asm_amd64.s:370"),
        ("│   └─runtime.bgscavenge.func2", "6.25%", "100%", 2, 3, "c:/go/src/runtime/mgcscavenge.go:315"),
        ("│     └─runtime.(*mheap).scavengeLocked", "6.25%", "100%", 2, 4, "c:/go/src/runtime/mheap.go:1446"),
        ("│       └─runtime.(*mspan).scavenge", "6.25%", "100%", 2, 5, "c:/go/src/runtime/mheap.go:589"),
        ("│         └─runtime.sysUnused", "6.25%", "100%", 2, 6, "c:/go/src/runtime/mem_windows.go:33"),
        ("│           └─runtime.stdcall3", "6.25%", "100%", 2, 7, "c:/go/src/runtime/os_windows.go:837"),
        ("└─runtime.morestack", "6.25%", "6.25%", 3, 1, "c:/go/src/runtime/asm_amd64.s:449"),
        ("  └─runtime.newstack", "6.25%", "100%", 3, 2, "c:/go/src/runtime/stack.go:1038"),
        ("    └─runtime.gopreempt_m", "6.25%", "100%", 3, 3, "c:/go/src/runtime/proc.go:2653"),
        ("      └─runtime.goschedImpl", "6.25%", "100%", 3, 4, "c:/go/src/runtime/proc.go:2625"),
        ("        └─runtime.schedule", "6.25%", "100%", 3, 5, "c:/go/src/runtime/proc.go:2524"),
        ("          └─runtime.findrunnable", "6.25%", "100%", 3, 6, "c:/go/src/runtime/proc.go:2170"),
    ];
    assert_eq!(data.len(), datums.len());
    for (i, (row, expected)) in data.iter().zip(datums).enumerate() {
        let expected = vec![
            Datum::new_string_datum(expected.0),
            Datum::new_string_datum(expected.1),
            Datum::new_string_datum(expected.2),
            Datum::Int(expected.3),
            Datum::Int(expected.4),
            Datum::new_string_datum(expected.5),
        ];
        assert_eq!(row, &expected, "row {i:2}");
    }
}

/// Go `ParseGoroutines` over a `debug=2` dump, as PD answers `pd_profile_goroutines`.
#[test]
fn parse_goroutines_rows() {
    let dump = "goroutine 1 [running]:\nmain.main()\n\t/x/main.go:10 +0x1d\nruntime.main()\n\t/go/proc.go:250 +0x1\nruntime.goexit()\n\t/go/asm.s:1 +0x1\n\ngoroutine 7 [chan receive]:\nmain.worker()\n\t/x/w.go:3 +0x2";
    let rows = Collector.parse_goroutines(dump.as_bytes()).expect("parses");
    let text: Vec<(String, i64, String, String)> = rows
        .iter()
        .map(|row| match &row[1] { Datum::Int(id) => (text_of(&row[0]), *id, text_of(&row[2]), text_of(&row[3])), other => panic!("{other:?}") })
        .collect();
    assert_eq!(
        text,
        vec![
            ("main.main()".to_owned(), 1, "running".to_owned(), "/x/main.go:10 +0x1d".to_owned()),
            ("├─runtime.main()".to_owned(), 1, "running".to_owned(), "/go/proc.go:250 +0x1".to_owned()),
            ("└─runtime.goexit()".to_owned(), 1, "running".to_owned(), "/go/asm.s:1 +0x1".to_owned()),
            ("main.worker()".to_owned(), 7, "chan receive".to_owned(), "/x/w.go:3 +0x2".to_owned()),
        ]
    );
    assert_eq!(Collector.parse_goroutines(b"no colon").unwrap_err(), "goroutine incompatible with current go version");
}

/// The runtime profiles this process keeps no samples of are Go's empty profile; an unknown name is Go's error.
#[test]
fn profile_graph_without_runtime_profiles() {
    for name in ["heap", "allocs", "mutex", "block"] {
        let rows = Collector.profile_graph(name).expect(name);
        assert_eq!(rows.len(), 1, "{name}");
        assert_eq!(text_of(&rows[0][0]), "root");
    }
    assert!(Collector.profile_graph("goroutine").expect("goroutine").is_empty());
    assert_eq!(Collector.profile_graph("nosuch").unwrap_err(), "cannot retrieve nosuch profile");
}

#[test]
fn percentage_matches_go_formatting() {
    assert_eq!(flamegraph::percentage(1, 1), "100%");
    assert_eq!(flamegraph::percentage(1, 8), "12.50%");
    assert_eq!(flamegraph::percentage(1, 300), "0.33%");
    assert_eq!(flamegraph::percentage(1, 300_000), "0.00033%");
    assert_eq!(flamegraph::percentage(1, 0), "0%");
}

fn text_of(datum: &Datum) -> String {
    String::from_utf8(datum.go_bytes().to_vec()).expect("utf-8")
}
