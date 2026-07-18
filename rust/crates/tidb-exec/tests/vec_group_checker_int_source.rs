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

//! Source-backed tests for integer vector-group boundaries and cursor state.

use tidb_exec::vec_group_checker_int::{IntGroupChecker, IntGroupError};

fn repeated_values(chunk_rows: &[usize], same_num: usize) -> Vec<Vec<Option<i64>>> {
    let mut next = 0usize;
    chunk_rows
        .iter()
        .map(|&rows| {
            (0..rows)
                .map(|_| {
                    let value = Some((next / same_num) as i64);
                    next += 1;
                    value
                })
                .collect()
        })
        .collect()
}

#[test]
fn vec_group_checker_group_count_matches_source() {
    // Source: pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-151,524-564
    // and pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141-202.
    let cases = [
        (&[1024, 1][..], 1025, &[false, false][..], 1),
        (&[1024, 1][..], 1, &[false, true][..], 1025),
        (&[1, 1][..], 1, &[false, true][..], 2),
        (&[1, 1][..], 2, &[false, false][..], 1),
        (&[2, 2][..], 2, &[false, false][..], 2),
        (&[2, 2][..], 1, &[false, true][..], 4),
    ];

    for (chunk_rows, expected_groups, expected_flags, same_num) in cases {
        let mut checker = IntGroupChecker::new();
        let chunks = repeated_values(chunk_rows, same_num);
        let mut group_count = 0usize;
        for (index, values) in chunks.iter().enumerate() {
            let same_as_previous = checker
                .split_into_groups(values)
                .expect("source precondition supplies a non-empty chunk");
            assert_eq!(same_as_previous, expected_flags[index]);
            group_count += checker.group_count() - usize::from(same_as_previous);
            while checker.next_group().is_some() {}
        }
        assert_eq!(group_count, expected_groups);
    }
}

#[test]
fn vec_group_checker_preserves_null_equality_and_cursor_ranges() {
    let mut checker = IntGroupChecker::new();
    assert_eq!(
        checker.split_into_groups(&[None, None, Some(1), Some(1)]),
        Ok(false)
    );
    assert_eq!(checker.group_count(), 2);
    assert_eq!(checker.next_group(), Some((0, 2)));
    assert_eq!(checker.next_group(), Some((2, 4)));
    assert!(checker.is_exhausted());
    assert_eq!(checker.next_group(), None);

    checker.reset();
    assert_eq!(checker.group_count(), 0);
    assert!(checker.is_exhausted());
    assert_eq!(checker.split_into_groups(&[Some(1), Some(2)]), Ok(true));
    assert_eq!(checker.split_into_groups(&[Some(2), Some(2)]), Ok(true));
    assert_eq!(
        checker.split_into_groups(&[]),
        Err(IntGroupError::EmptyChunk)
    );
}
