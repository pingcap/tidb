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

//! An index join whose outer key is SIGNED and whose indexed inner key is
//! UNSIGNED, pinned to the answers a real Go TiDB gives.
//!
//! Go admits this pair and converts per probe --
//! `constructDatumLookupKey` (`index_lookup_merge_join.go:658`):
//!
//! ```text
//! innerValue, err := outerValue.ConvertTo(sc.TypeCtx(), innerColType)
//! if ErrOverflow || ErrWarnDataOutOfRange { return nil, nil }   // read nothing
//! cmp, _ := outerValue.Compare(sc.TypeCtx(), &innerValue, collator)
//! if cmp != 0 { return nil, nil }                               // read nothing
//! ```
//!
//! so a `-1` outer key finds no unsigned entry rather than probing the
//! wrong bytes, and `18446744073709551615` is matched by no signed outer
//! value. Captured from `tidb-server v9.0.0-beta.2.pre-nightly` on this
//! exact fixture: the rows below, and an `IndexJoin` for the hinted plan.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

fn fixture() -> Session {
    let mut session = Session::new();
    session
        .run("create table u(b bigint unsigned, k int, index ib(b))")
        .unwrap();
    session.run("create table s(b bigint, k int)").unwrap();
    session
        .run("insert into u values (1,10),(2,20),(18446744073709551615,30)")
        .unwrap();
    session.run("insert into s values (1,1),(2,2),(-1,3)").unwrap();
    session
}

/// The join answers, whichever strategy the planner picks.
#[test]
fn a_signed_key_joins_an_unsigned_indexed_key_on_the_values_that_convert() {
    let mut session = fixture();
    let rows = row_text(session.run("select s.b, u.b, s.k, u.k from s join u on s.b = u.b order by s.k"))
        .into_iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec!["1|1|1|10".to_owned(), "2|2|2|20".to_owned()],
        "`-1` converts to no unsigned value and `18446744073709551615` to no \
         signed one, so only the two representable keys match"
    );
}

/// The same answers when the probe is driven one outer row at a time, which
/// is the path `constructDatumLookupKey` guards: a value outside the inner
/// column's domain must read NOTHING rather than the wrong index entry.
#[test]
fn an_out_of_domain_probe_reads_nothing() {
    let mut session = fixture();
    let rows = row_text(session.run("select u.k from s join u on s.b = u.b where s.b < 0"))
        .into_iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>();
    assert!(
        rows.is_empty(),
        "a negative outer key has no unsigned counterpart: {rows:?}"
    );
}
