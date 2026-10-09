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

//! The session's `sql_mode` reaching Go's `buildIndexColumns`, which is the
//! one DDL rule where a non-strict mode ACCEPTS a statement a strict one
//! refuses -- and stores a DIFFERENT catalog for it.
//!
//! Every expectation is captured from a real TiDB session (mockstore,
//! `pkg/session`), warnings and SHOW CREATE TABLE text included.

use crate::tests_support::{row_text, show_create, warnings_of};
use crate::*;

fn permissive() -> Session {
    let mut session = Session::new();
    session.run("SET sql_mode = ''").unwrap();
    session
}

/// Under a non-strict mode a SINGLE, NON-UNIQUE key part that runs past the
/// 3072-byte limit is truncated to `maxIndexLength / bytes-per-character` with
/// warning 1071, rather than refused.
///
/// The reported number is the sum as WRITTEN (12000 / 8000 / 5000), not the
/// stored one, and the stored prefix is in CHARACTERS -- so utf8mb4 keys on
/// 768 and latin1 on 3072.
#[test]
fn a_non_strict_mode_truncates_a_single_over_long_key_part() {
    let mut session = permissive();

    for (ddl, table, reported, body) in [
        (
            "CREATE TABLE tc1 (a VARCHAR(3000), KEY(a)) CHARSET=utf8mb4",
            "tc1",
            12000,
            "KEY `a` (`a`(768))",
        ),
        (
            "CREATE TABLE p1 (a VARCHAR(3000), KEY(a(2000))) CHARSET=utf8mb4",
            "p1",
            8000,
            "KEY `a` (`a`(768))",
        ),
        (
            "CREATE TABLE p2 (a TEXT, KEY(a(2000))) CHARSET=utf8mb4",
            "p2",
            8000,
            "KEY `a` (`a`(768))",
        ),
        (
            "CREATE TABLE p6 (a VARCHAR(5000) CHARACTER SET latin1, KEY(a)) CHARSET=utf8mb4",
            "p6",
            5000,
            "KEY `a` (`a`(3072))",
        ),
    ] {
        session.run(ddl).unwrap();
        assert_eq!(
            warnings_of(&session),
            [(
                1071,
                format!(
                    "Specified key was too long ({reported} bytes); max key length is 3072 bytes"
                )
            )],
            "for {ddl}"
        );
        assert!(
            show_create(&mut session, table).contains(body),
            "expected {body} in {}",
            show_create(&mut session, table)
        );
    }
}

/// The three shapes a non-strict mode does NOT rescue, because Go's downgrade
/// arm requires all three of: non-strict, non-unique, exactly one key part.
#[test]
fn a_non_strict_mode_still_refuses_a_unique_or_multi_part_over_long_key() {
    let mut session = permissive();

    // UNIQUE: the sum is an error whatever the mode.
    assert!(matches!(
        session.run("CREATE TABLE tc2 (a VARCHAR(3000), UNIQUE KEY(a)) CHARSET=utf8mb4"),
        Err(DriverError::TooLongKey {
            length: 12000,
            max: 3072
        })
    ));
    // Two key parts: likewise.
    assert!(matches!(
        session.run("CREATE TABLE p4 (a VARCHAR(1000), b VARCHAR(1000), KEY(a,b)) CHARSET=utf8mb4"),
        Err(DriverError::TooLongKey {
            length: 4000,
            max: 3072
        })
    ));
    // ADD INDEX: Go re-runs `buildIndexColumns` in the DDL job worker with a
    // NIL context, which reads as strict -- captured, this fails 1071 even
    // though the identical key inside a CREATE TABLE is truncated above.
    session
        .run("CREATE TABLE tc3 (a VARCHAR(3000)) CHARSET=utf8mb4")
        .unwrap();
    assert!(matches!(
        session.run("ALTER TABLE tc3 ADD KEY(a)"),
        Err(DriverError::TooLongKey {
            length: 12000,
            max: 3072
        })
    ));
    // ... and the table keeps no index.
    assert!(!show_create(&mut session, "tc3").contains("KEY"));
}

/// The CONTROL: under the DEFAULT strict mode the same CREATE TABLE is
/// refused, so the truncation above is the MODE's doing and not a relaxation
/// of the limit.
#[test]
fn a_strict_mode_refuses_the_over_long_key_the_permissive_one_truncates() {
    let mut session = Session::new();
    assert!(matches!(
        session.run("CREATE TABLE tc1 (a VARCHAR(3000), KEY(a)) CHARSET=utf8mb4"),
        Err(DriverError::TooLongKey {
            length: 12000,
            max: 3072
        })
    ));
    // A key that FITS is unaffected in either mode.
    session
        .run("CREATE TABLE ok1 (a VARCHAR(255), b VARCHAR(255), KEY(a,b)) CHARSET=utf8mb4")
        .unwrap();
    assert!(show_create(&mut session, "ok1").contains("KEY `a` (`a`,`b`)"));
    assert!(warnings_of(&session).is_empty());
}

/// Go `detachCNFCondAndBuildRangeForIndex` (issue 26029 in Go): with a
/// prefix key part and consecutive-merge, the points are copied BEFORE
/// `UnionRanges` fuses them, and the tail column's ranges are appended to that
/// copy. Appending `col3 != v` to the fused `col2 in (72, 73)` built
/// `[x 72 -inf, x 73 v), (x 72 v, x 73 +inf]`, two overlapping ranges, and the
/// lookup returned the one matching row twice.
#[test]
fn a_prefix_index_appends_tail_ranges_to_each_unmerged_point() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE IDT_20755 (COL1 varchar(20) DEFAULT NULL, COL2 tinyint(16) DEFAULT NULL, \
         COL3 timestamp NULL DEFAULT NULL, KEY u_m_col (COL1(10), COL2, COL3)) \
         DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        "INSERT INTO IDT_20755 VALUES('xxxxxxxxxxxxxxx', 73, '2010-06-03 07:29:05')",
    ] {
        session.run(sql).unwrap();
    }
    let sql = "select * from IDT_20755 use index (u_m_col) where col1 = 'xxxxxxxxxxxxxxx' \
        and col2 in (72, 73) and col3 != '2024-10-19 08:55:32'";
    let explain = row_text(session.run(&format!("explain format='brief' {sql}")))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain.contains(
            "[\"xxxxxxxxxx\" 72 -inf,\"xxxxxxxxxx\" 72 2024-10-19 08:55:32), \
             (\"xxxxxxxxxx\" 72 2024-10-19 08:55:32,\"xxxxxxxxxx\" 72 +inf], \
             [\"xxxxxxxxxx\" 73 -inf,\"xxxxxxxxxx\" 73 2024-10-19 08:55:32)"
        ),
        "{explain}"
    );
    assert_eq!(
        row_text(session.run(sql)),
        vec![vec!["xxxxxxxxxxxxxxx", "73", "2010-06-03 07:29:05"]]
    );
}
