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

//! Source-backed tests for slow-log field splitting.

use tidb_exec::slow_log_split::split_by_colon;

#[test]
fn slow_log_split_matches_plain_nested_empty_and_invalid_cases() {
    // Source: pkg/executor/slow_query.go:810-920.
    // Direct Go coverage: pkg/executor/slow_query_test.go:619
    // (TestSplitByColon).
    let cases = [
        ("", vec![], vec![]),
        ("123a", vec!["123a"], vec![""]),
        ("1a: 2b", vec!["1a"], vec!["2b"]),
        (
            "1a: [2b 3c] 4d: 5e",
            vec!["1a", "4d"],
            vec!["[2b 3c]", "5e"],
        ),
        (
            "1a: [2b,3c] 4d: 5e",
            vec!["1a", "4d"],
            vec!["[2b,3c]", "5e"],
        ),
        (
            "1a: [2b,[3c: 3cc]] 4d: 5e",
            vec!["1a", "4d"],
            vec!["[2b,[3c: 3cc]]", "5e"],
        ),
        (
            "1a: {2b 3c} 4d: 5e",
            vec!["1a", "4d"],
            vec!["{2b 3c}", "5e"],
        ),
        (
            "1a: {2b,3c} 4d: 5e",
            vec!["1a", "4d"],
            vec!["{2b,3c}", "5e"],
        ),
        (
            "1a: {2b,{3c: 3cc}} 4d: 5e",
            vec!["1a", "4d"],
            vec!["{2b,{3c: 3cc}}", "5e"],
        ),
        (
            "Cop_proc_avg: 0 Cop_proc_addr: Cop_proc_max: Cop_proc_min: ",
            vec![
                "Cop_proc_avg",
                "Cop_proc_addr",
                "Cop_proc_max",
                "Cop_proc_min",
            ],
            vec!["0", "", "", ""],
        ),
    ];

    for (line, fields, values) in cases {
        let fields: Vec<String> = fields.into_iter().map(str::to_owned).collect();
        let values: Vec<String> = values.into_iter().map(str::to_owned).collect();
        assert_eq!(split_by_colon(line), Some((fields, values)), "{line}");
    }

    for line in ["1a: {{{2b,{3c: 3cc}} 4d: 5e", "1a: [2b,[3c: 3cc]]]] 4d: 5e"] {
        assert_eq!(split_by_colon(line), None, "{line}");
    }
}

#[test]
fn slow_log_split_preserves_timestamp_and_empty_tail_values() {
    assert_eq!(
        split_by_colon("Time: 2021-09-08T14:39:54.506967433+08:00"),
        Some((
            vec!["Time".to_owned()],
            vec!["2021-09-08T14:39:54.506967433+08:00".to_owned()]
        ))
    );
    assert_eq!(
        split_by_colon("Key: "),
        Some((vec!["Key".to_owned()], vec![String::new()]))
    );
}
