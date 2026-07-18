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

use tidb_datatype::Datum;
use tidb_exec::group_concat::{
    decode_base_partial, encode_base_partial, DistinctGroupConcatState, GroupConcatState,
    OrderedGroupConcatState,
};
use tidb_exec::{Database, Outcome};

#[test]
fn database_group_concat_consumes_canonical_plain_distinct_and_ordered_states() {
    let mut database = Database::new();
    for sql in [
        "create table gc_live (k int, a varchar(8), b varchar(8))",
        "insert into gc_live values (1,'0','0'),(1,'1','1'),(1,null,'2'),(1,'2','2')",
    ] {
        assert_eq!(
            database.run(&tidb_parser::parse(sql).unwrap()).unwrap(),
            Outcome::Done
        );
    }
    let Outcome::Rows(result) = database
        .run(&tidb_parser::parse("select group_concat(a, b separator ' ') from gc_live").unwrap())
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Datum::new_string("00 11 22")]]);

    let Outcome::Rows(result) = database
        .run(
            &tidb_parser::parse(
                "select group_concat(distinct a order by a desc separator '-') from gc_live",
            )
            .unwrap(),
        )
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Datum::new_string("2-1-0")]]);
}

#[test]
fn distinct_keys_preserve_tuple_boundaries_and_merge_unseen_values() {
    let mut destination = DistinctGroupConcatState::new(" ", 0);
    destination.update(b"2:ab1:c", b"abc");
    destination.update(b"1:a2:bc", b"abc");
    destination.update(b"2:ab1:c", b"duplicate");
    let mut source = DistinctGroupConcatState::new(" ", 0);
    source.update(b"1:x", b"x");
    destination.merge_from(&source);
    assert!(!destination.finalize());
    let mut values = destination
        .finish()
        .unwrap()
        .split(|byte| *byte == b' ')
        .collect::<Vec<_>>();
    values.sort_unstable();
    // Go ranges over a map here, so SQL deliberately specifies no output
    // order without ORDER BY. Assert membership, not one invented map order.
    assert_eq!(
        values,
        vec![b"abc".as_slice(), b"abc".as_slice(), b"x".as_slice()]
    );
}

#[test]
fn unordered_distinct_empty_value_uses_buffer_length_separator_rule() {
    let mut state = DistinctGroupConcatState::new(",", 0);
    state.update(b"empty", b"");
    state.update(b"x", b"x");
    state.finalize();
    assert_eq!(state.finish(), Some(b"x".as_slice()));
}

#[test]
fn ordered_state_bounds_top_n_truncates_separator_and_rejects_merge() {
    let mut state = OrderedGroupConcatState::new("---", 10, vec![false]);
    state.update(vec![b"c".to_vec()], b"ccc".to_vec());
    state.update(vec![b"a".to_vec()], b"aaa".to_vec());
    assert!(state.update(vec![b"b".to_vec()], b"bbb".to_vec()));
    assert!(state.finalize());
    assert_eq!(state.finish(), Some(b"aaa---bbb-".as_slice()));
    assert!(state.was_truncated());

    // Go's sentinel is aggregate-lifetime, so another over-budget update
    // after Reset does not report a second warning transition.
    state.reset();
    assert!(!state.update(vec![b"a".to_vec()], b"01234567890".to_vec()));
    state.finalize();
    assert_eq!(state.finish(), Some(b"0123456789".as_slice()));
    let other = OrderedGroupConcatState::new("---", 10, vec![false]);
    assert!(state.merge_from(&other).is_err());
}

#[test]
fn ordered_distinct_dedupes_before_top_n_and_retains_evicted_keys() {
    let mut state = OrderedGroupConcatState::new_distinct(",", 5, vec![false]);
    state.update_distinct(b"z".to_vec(), vec![b"z".to_vec()], b"zzzz".to_vec());
    state.update_distinct(b"a".to_vec(), vec![b"a".to_vec()], b"a".to_vec());
    state.update_distinct(b"b".to_vec(), vec![b"b".to_vec()], b"b".to_vec());
    // The original z row was shortened/evicted by top-N. Go's valSet is not
    // pruned with the heap, so the duplicate key still cannot re-enter.
    state.update_distinct(b"z".to_vec(), vec![b"0".to_vec()], b"0".to_vec());
    state.finalize();
    assert_eq!(state.finish(), Some(b"a,b,z".as_slice()));
}

#[test]
fn ordered_separator_uses_row_position_even_when_first_value_is_empty() {
    let mut state = OrderedGroupConcatState::new(",", 0, vec![false]);
    state.update(vec![b"a".to_vec()], Vec::new());
    state.update(vec![b"b".to_vec()], b"b".to_vec());
    state.finalize();
    assert_eq!(state.finish(), Some(b",b".as_slice()));
}

#[test]
fn base_partial_spill_matches_source_native_shape() {
    let empty = GroupConcatState::new(" ", 0);
    assert_eq!(encode_base_partial(&empty), vec![0]);
    assert_eq!(decode_base_partial(&[0], " ", 0).unwrap().finish(), None);

    let mut state = GroupConcatState::new(" ", 0);
    state.update(&[Some("123"), Some("456")]);
    let encoded = encode_base_partial(&state);
    assert_eq!(encoded[0], 1);
    assert_eq!(
        decode_base_partial(&encoded, " ", 0).unwrap().finish(),
        Some(b"123 456".as_slice())
    );
    assert!(decode_base_partial(&[1], " ", 0).is_err());

    let mut empty_buffer = GroupConcatState::new(" ", 0);
    empty_buffer.update_bytes(&[Some(b"")]);
    let encoded = encode_base_partial(&empty_buffer);
    assert_eq!(encoded.len(), 1 + std::mem::size_of::<isize>());
    assert_eq!(encoded[0], 1);
    assert_eq!(
        decode_base_partial(&encoded, " ", 0).unwrap().finish(),
        Some(&[][..])
    );

    for value in [
        "平352p凯额6辰c".repeat(1024),
        "123a啊f24f去rsgvsfg".repeat(1024),
    ] {
        let mut state = GroupConcatState::new(" ", 0);
        state.update_bytes(&[Some(value.as_bytes())]);
        let encoded = encode_base_partial(&state);
        assert_eq!(
            decode_base_partial(&encoded, " ", 0).unwrap().finish(),
            Some(value.as_bytes())
        );
    }
}
