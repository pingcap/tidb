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

//! Source-backed tests for the bounded non-DISTINCT `GROUP_CONCAT` state.

use tidb_exec::group_concat::GroupConcatState;

#[test]
fn group_concat_update_skips_null_and_preserves_separator_order() {
    // Source: pkg/executor/aggfuncs/func_group_concat.go:222-249.
    // Direct Go coverage: pkg/executor/aggfuncs/func_group_concat_test.go:42
    // (TestGroupConcat), non-DISTINCT string-buffer case.
    let mut state = GroupConcatState::new(" ", 0);
    assert!(!state.update(&[None, Some("0"), Some("1"), None, Some("2")]));
    assert_eq!(state.finish(), Some(b"0 1 2".as_slice()));
    assert_eq!(state.finish_str(), Some("0 1 2"));
}

#[test]
fn group_concat_merge_and_empty_source_match_source() {
    // Source: pkg/executor/aggfuncs/func_group_concat.go:255-275.
    // Direct Go coverage: pkg/executor/aggfuncs/func_group_concat_test.go:37
    // (TestMergePartialResult4GroupConcat).
    let mut destination = GroupConcatState::new(" ", 0);
    destination.update(&[Some("0"), Some("1"), Some("2"), Some("3"), Some("4")]);
    let mut source = GroupConcatState::new(" ", 0);
    source.update(&[Some("2"), Some("3"), Some("4")]);
    assert!(!destination.merge_from(&source));
    assert_eq!(destination.finish(), Some(b"0 1 2 3 4 2 3 4".as_slice()));

    let before = destination.finish().unwrap().to_vec();
    assert!(!destination.merge_from(&GroupConcatState::new(" ", 0)));
    assert_eq!(destination.finish(), Some(before.as_slice()));
}

#[test]
fn group_concat_max_len_is_byte_truncation_and_sentinel_survives_reset() {
    // Source: pkg/executor/aggfuncs/func_group_concat.go:61-76, 285-292.
    // Direct Go coverage: pkg/executor/aggfuncs/func_group_concat_test.go:42
    // (TestGroupConcat), GroupConcatMaxLen values 4..7.
    let expected = [
        (4, b"44 3".as_slice()),
        (5, b"44 33".as_slice()),
        (6, b"44 33 ".as_slice()),
        (7, b"44 33 2".as_slice()),
    ];
    for (max_len, expected) in expected {
        let mut state = GroupConcatState::new(" ", max_len);
        assert!(state.update(&[Some("44"), Some("33"), Some("22"), Some("11"), Some("00"),]));
        assert_eq!(state.finish(), Some(expected));
        assert!(state.was_truncated());
        state.reset();
        assert_eq!(state.finish(), None);
        assert!(state.was_truncated());
    }

    // The source uses bytes.Buffer.Truncate, so a UTF-8 boundary is not a
    // special case: max_len is a byte count, not a character count.
    let mut raw = GroupConcatState::new(b"|", 2);
    assert!(raw.update_bytes(&[Some("é".as_bytes()), Some(b"x".as_slice())]));
    assert_eq!(raw.finish(), Some(&[0xc3, 0xa9][..]));
}

#[test]
fn group_concat_partial_state_size_is_stable() {
    // Source: pkg/executor/aggfuncs/func_group_concat.go:221-227 and
    // pkg/executor/aggfuncs/func_group_concat_test.go:66 (TestMemGroupConcat).
    // The Go allocator's bytes.Buffer/memory-delta accounting and all
    // DISTINCT/ORDER variants remain external to this state owner.
    assert_eq!(
        GroupConcatState::partial_state_size(),
        std::mem::size_of::<GroupConcatState>()
    );
}
