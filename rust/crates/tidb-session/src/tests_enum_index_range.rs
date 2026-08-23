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

//! Index ranges over an `ENUM` column: Go's `handleEnumFromBinOp` and the
//! `EnumSetAsIntFlag` that decides when it runs.

use crate::tests_support::row_text;
use crate::Session;

/// Table and queries are the source corpus's own:
/// `tests/integrationtest/t/black_list.test`'s `TestExprBlackListForEnum`.
fn fixture() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t(a enum('a','b','c'), b enum('a','b','c'), c int, \
             index idx(b,a))",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
}

/// The `range:` text of the first scan an `EXPLAIN` prints. A range list is
/// comma-separated itself, so it runs to the next `, keep order:` rather than
/// to the next comma.
fn range(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .find_map(|row| {
            row.iter().find_map(|cell| {
                let text = cell.split_once("range:")?.1;
                Some(
                    text.split_once(", keep order:")
                        .map_or(text, |(r, _)| r)
                        .to_owned(),
                )
            })
        })
        .unwrap_or_default()
}

/// An `ENUM` equality over an index answers the row, rather than nothing.
///
/// An index stores an `ENUM` as the member's NUMBER, which is what a
/// `Datum::Enum` encodes to. The ranger's endpoint conversion has a fast path
/// for a string literal whose collation and length already fit the target,
/// resting on Go's `ConvertTo` returning such a value unchanged -- true for
/// `VARCHAR`, false for `ENUM`, where `ConvertTo` resolves the member. Taking
/// the fast path left `Datum::String("a")` in the point, the key codec wrote
/// it as text, and the range matched no key at all: `b = 'a'` answered ZERO
/// rows where the same query without the index answers one.
#[test]
fn an_enum_equality_over_an_index_finds_its_row() {
    let mut session = fixture();

    assert_eq!(
        row_text(session.run("SELECT * FROM t IGNORE INDEX (idx) WHERE b = 'a'")),
        vec![vec!["a", "a", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX (idx) WHERE b = 'a'")),
        vec![vec!["a", "a", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX (idx) WHERE b = 'b'")),
        vec![vec!["b", "b", "2"]]
    );
}

/// A comparison against a STRING is a point set, one point per admitted
/// member, and never an interval.
///
/// Go stops building intervals for an `ENUM` in `handleEnumFromBinOp`: the
/// key orders by member NUMBER while a string comparison orders by NAME, so
/// `b > 'a'` is not an interval over anything the key stores. Go walks every
/// member, keeps the ones the comparison admits, and emits each as its own
/// point range.
#[test]
fn a_string_comparison_over_an_enum_is_a_point_per_member() {
    let mut session = fixture();

    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT * FROM t USE INDEX (idx) WHERE b > 'a'"
        ),
        "[\"b\",\"b\"], [\"c\",\"c\"]"
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX (idx) WHERE b > 'a'")),
        vec![vec!["b", "b", "2"], vec!["c", "c", "3"]]
    );
    // The corpus's own two-column case, whose recorded range this is.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT * FROM t USE INDEX (idx) WHERE b = 1 AND a > 'a'"
        ),
        "[\"a\" \"b\",\"a\" \"b\"], [\"a\" \"c\",\"a\" \"c\"]"
    );
}

/// A comparison against an INTEGER is an ordinary interval over the member
/// numbers.
///
/// Go's `getBaseCmpType` calls an `ENUM` Hybrid, so `enum <cmp> int` compares
/// as `ETInt`; `WrapWithCastAsInt` then hands the comparison its own clone of
/// the column stamped `EnumSetAsIntFlag`, whose `EvalType()` is `ETInt`. Both
/// the `conditionChecker` collation gate and `buildFromBinOp`'s enum arm read
/// that eval type, so neither applies: the numbers are what both sides
/// compare, and an interval over them is right.
#[test]
fn an_integer_comparison_over_an_enum_is_an_interval_over_the_numbers() {
    let mut session = fixture();

    // The corpus's own two, with the ranges TiDB records for them.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT * FROM t USE INDEX (idx) WHERE b = 1 AND a = 1"
        ),
        "[\"a\" \"a\",\"a\" \"a\"]"
    );
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT * FROM t USE INDEX (idx) WHERE b = 1 AND a > 1"
        ),
        "(\"a\" \"a\",\"a\" +inf]"
    );
    // An interval, not the two points a string comparison would give --- and
    // it answers the same rows either way.
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX (idx) WHERE b = 1")),
        vec![vec!["a", "a", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX (idx) WHERE b > 1")),
        vec![vec!["b", "b", "2"], vec!["c", "c", "3"]]
    );
}

/// Two `ENUM` endpoints order by member NUMBER, not by name.
///
/// Go's `rangePointCmp` opens with `rangePointEnumCmp`, which compares the
/// members' numbers -- the order the index key is written in. Every sweep
/// over merged endpoints reads that order, so ordering them by name instead
/// hands the scan a range list that is not in key order whenever the
/// declaration order and the collation disagree. `ENUM('c','b','a')` is the
/// plain case: `'c'` is member 1 and sorts FIRST in the key while sorting
/// last by name.
#[test]
fn enum_endpoints_order_by_member_number_not_name() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE e(x enum('c','b','a'), y int, index ix(x))")
        .unwrap();
    session
        .run("INSERT INTO e VALUES ('a',1),('b',2),('c',3)")
        .unwrap();

    // Members admitted by name (`'c'` and `'b'` both exceed `'a'`), listed by
    // number: `'c'` is 1 and `'b'` is 2.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT * FROM e USE INDEX (ix) WHERE x > 'a'"
        ),
        "[\"c\",\"c\"], [\"b\",\"b\"]"
    );
    let mut rows = row_text(session.run("SELECT y FROM e USE INDEX (ix) WHERE x > 'a'"));
    rows.sort();
    assert_eq!(rows, vec![vec!["2"], vec!["3"]]);

    // And the whole index still reads in key order, which is declaration
    // order rather than alphabetical.
    assert_eq!(
        row_text(session.run("SELECT x FROM e USE INDEX (ix) ORDER BY x")),
        vec![vec!["c"], vec!["b"], vec!["a"]]
    );
}
