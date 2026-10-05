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

//! Go `pkg/util/schemacmp/charset_collation_test.go` near 1:1.

use tidb_schemacmp::{charset, collation, Lattice, Value};

fn error_contains(error: &tidb_schemacmp::IncompatibleError, needle: &str) {
    assert!(
        error.to_string().contains(needle),
        "error {error:?} does not contain {needle:?}"
    );
}

fn unwrap_str(value: Value) -> String {
    let Value::Str(text) = value else {
        panic!("expected a string, got {value:?}");
    };
    text.to_utf8_lossy_go()
}

// Go `TestCharsetCompare`.
#[test]
fn test_charset_compare() {
    // Ensure normalization makes comparisons case-insensitive.
    let cmp = charset("UTF8").compare(&charset("utf8")).unwrap();
    assert_eq!(cmp, 0);

    let cmp = charset("UTF8MB3").compare(&charset("utf8")).unwrap();
    assert_eq!(cmp, 0);

    let err = charset("uTF8").compare(&charset("GBK")).unwrap_err();
    error_contains(&err, "incompatible charset (utf8 vs gbk)");

    let cmp = charset("latin1").compare(&charset("utf8mb4")).unwrap();
    assert_eq!(cmp, -1);

    let cmp = charset("utf8mb4").compare(&charset("utf8mb3")).unwrap();
    assert_eq!(cmp, 1);

    let err = charset("other1").compare(&charset("other2")).unwrap_err();
    error_contains(&err, "incompatible charset (other1 vs other2)");

    let err = charset("other1").compare(&charset("utf8")).unwrap_err();
    error_contains(&err, "incompatible charset (other1 vs utf8)");
}

// Go `TestCollationCompare`.
#[test]
fn test_collation_compare() {
    // Ensure Compare only depends on the normalized kind, not the original
    // input string.
    let cmp = collation("UTF8_BIN")
        .compare(&collation("utf8_bin"))
        .unwrap();
    assert_eq!(cmp, 0);

    // Collations without a suffix (no underscore) should also compare
    // correctly.
    let cmp = collation("binary").compare(&collation("BINARY")).unwrap();
    assert_eq!(cmp, 0);

    // binary collation is different with other charset's _bin collation
    let err = collation("binary")
        .compare(&collation("utf8mb4_bin"))
        .unwrap_err();
    error_contains(&err, "incompatible collation (binary vs utf8mb4_bin)");

    let cmp = collation("UTF8MB3_BIN")
        .compare(&collation("utf8_bin"))
        .unwrap();
    assert_eq!(cmp, 0);

    let cmp = collation("LATIN1_BIN")
        .compare(&collation("utf8mb4_bin"))
        .unwrap();
    assert_eq!(cmp, -1);

    let err = collation("UTF8_BIN")
        .compare(&collation("GBK_BIN"))
        .unwrap_err();
    // note the error message is charset because collation suffix is the same
    error_contains(&err, "incompatible charset (utf8 vs gbk)");

    let cmp = collation("utf8mb4_general_ci")
        .compare(&collation("utf8_general_ci"))
        .unwrap();
    assert_eq!(cmp, 1);

    let err = collation("utf8mb4_general_ci")
        .compare(&collation("utf8mb4_0900_ai_ci"))
        .unwrap_err();
    error_contains(
        &err,
        "incompatible collation (utf8mb4_general_ci vs utf8mb4_0900_ai_ci)",
    );

    let err = collation("other_cs_bin")
        .compare(&collation("other_cs_ci"))
        .unwrap_err();
    error_contains(&err, "incompatible collation (other_cs_bin vs other_cs_ci)");

    // special fallback cases, where collation is not set and charset is known
    // to TiDB
    let err = collation("unknowCS")
        .compare(&collation("unknowCS2"))
        .unwrap_err();
    error_contains(&err, "incompatible charset (unknowcs vs unknowcs2)");

    let cmp = collation("unknowCS")
        .compare(&collation("unknowCS"))
        .unwrap();
    assert_eq!(cmp, 0);
}

// Go `TestCharsetJoin`.
#[test]
fn test_charset_join() {
    let join = charset("utf8").join(&charset("latin1")).unwrap();
    assert_eq!(unwrap_str(join.unwrap()), "utf8mb4");

    let join = charset("latin1").join(&charset("utf8mb3")).unwrap();
    assert_eq!(unwrap_str(join.unwrap()), "utf8mb4");
}

// Go `TestCollationJoin`.
#[test]
fn test_collation_join() {
    let join = collation("utf8_bin")
        .join(&collation("latin1_bin"))
        .unwrap();
    assert_eq!(unwrap_str(join.unwrap()), "utf8mb4_bin");

    let join = collation("latin1_general_cs")
        .join(&collation("utf8_general_cs"))
        .unwrap();
    assert_eq!(unwrap_str(join.unwrap()), "utf8mb4_general_cs");
}
