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

//! The shared `RunTest` case runner for the `pkg/parser/parser_test.go`
//! table transcreations. Included by each `parser_run_test_*_source.rs`
//! family file. It is a sibling module of the aggregated integration-test
//! root, so each family file reaches it by path rather than re-including it.

use tidb_parser::parse_multi;

pub fn run_cases(cases: &[(&str, bool, &str)]) {
    for &(source, valid, expected) in cases {
        let parsed = parse_multi(source);
        if !valid {
            assert!(parsed.is_err(), "source SQL unexpectedly parsed: {source}");
            continue;
        }
        let statements =
            parsed.unwrap_or_else(|error| panic!("source SQL: {source}; error: {error:?}"));
        let restored = statements
            .iter()
            .map(|statement| statement.restore())
            .collect::<Vec<_>>()
            .join("; ");
        assert_eq!(restored, expected, "source SQL: {source}");
        for statement in statements {
            let restored = statement.restore();
            let reparsed = parse_multi(&restored)
                .unwrap_or_else(|error| panic!("round trip: {source}; error: {error:?}"));
            assert_eq!(reparsed.len(), 1, "round trip statement count: {source}");
            assert_eq!(reparsed[0].restore(), restored, "round trip: {source}");
        }
    }
}
