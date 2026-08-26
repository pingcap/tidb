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

//! A merge join whose two key columns have DIFFERENT types, pinned against
//! `t/executor/merge_join.test`'s `TestMergeJoinDifferentTypes`.
//!
//! Go builds one compare function per key pair from BOTH sides at once --
//! `initCompareFuncs` (`physical_merge_join.go:332`) calls
//! `expression.GetCmpFunction(left[i], right[i])`, which dispatches on
//! `GetAccurateCmpType(lhs, rhs)` and, for the integer class, ends in
//! `types.CompareInt(arg0, isUnsigned0, arg1, isUnsigned1)`
//! (`pkg/types/compare.go:90`), where EACH side contributes its own
//! unsigned flag.
//!
//! The typed fast paths in `tidb_executor::join` read only ONE side's field
//! type, so a signed-vs-unsigned pair (or `bigint` vs `bit`) used to fall
//! into the branch reserved for a NULL key and compare Less forever: the
//! merge walked both inputs to exhaustion and emitted NOTHING. These are
//! the recorded answers, which are non-empty.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The rows of one statement as `|`-joined text.
fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

/// `int` against `int unsigned`: the pair still matches on equal values.
#[test]
fn a_signed_key_joins_an_unsigned_key_of_the_same_value() {
    let mut session = Session::new();
    session.run("create table t(c1 int)").unwrap();
    session.run("create table t1(c1 int unsigned)").unwrap();
    session.run("insert into t values (1)").unwrap();
    session.run("insert into t1 values (1)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select /*+ TIDB_SMJ(t,t1) */ t.c1 from t , t1 where t.c1 = t1.c1",
        ),
        vec!["1".to_owned()],
        "a signed key equals an unsigned key of the same value, so the \
         merge join emits the row"
    );
}

/// `bigint signed` against `bigint unsigned`: only the values that are equal
/// under Go's `CompareInt` match. `-1` does NOT meet `18446744073709551615`,
/// and the two `pow(2, 63)` columns land on different sides of the signed
/// boundary, so only the two-by-two zero pairing survives.
#[test]
fn a_mixed_sign_bigint_join_matches_only_where_compare_int_says_equal() {
    let mut session = Session::new();
    session
        .run("create table t1(a bigint signed, b bigint, index idx_a(a))")
        .unwrap();
    session
        .run("create table t2(a bigint unsigned, b bigint, index idx_a(a))")
        .unwrap();
    session
        .run("insert into t1 values(-1, 0), (-1, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0)")
        .unwrap();
    session
        .run("insert into t2 values(18446744073709551615, 0), (18446744073709551615, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0)")
        .unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select t1.a, t2.a from t1 join t2 on t1.a=t2.a order by t1.a",
        ),
        vec![
            "0|0".to_owned(),
            "0|0".to_owned(),
            "0|0".to_owned(),
            "0|0".to_owned(),
        ],
        "the two zero rows on each side pair four ways; -1 never equals \
         18446744073709551615"
    );
}

/// `bigint` against `bit(1)`: the non-integer key shape must fall through to
/// the generic comparison rather than being read as a NULL key.
#[test]
fn a_bigint_key_joins_a_bit_key() {
    let mut session = Session::new();
    session
        .run("create table t1(a bigint, b bit(1), index idx_a(a))")
        .unwrap();
    session
        .run("create table t2(a bit(1) not null, b bit(1), index idx_a(a))")
        .unwrap();
    session.run("insert into t1 values(1, 1)").unwrap();
    session.run("insert into t2 values(1, 1)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select hex(t1.a), hex(t2.a) from t1 inner join t2 on t1.a=t2.a",
        ),
        vec!["1|1".to_owned()],
        "a bigint 1 equals a bit(1) 1"
    );
}
