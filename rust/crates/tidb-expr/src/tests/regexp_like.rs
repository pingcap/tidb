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

//! Rewritten-expression coverage for `REGEXP_LIKE`.

use super::{chunk_e, e};

/// Scalar rows from `pkg/expression/builtin_regexp_test.go:210`
/// (`TestRegexpLike`).  Run through both evaluators: named calls must not be
/// limited to the AST tier while live SQL uses the rewritten scalar tier.
#[test]
fn regexp_like_source_vectors_reach_the_rewritten_evaluator() {
    for (expr, want) in [
        ("regexp_like('a', 'a')", "INT:1"),
        ("regexp_like('b', 'a')", "INT:0"),
        ("regexp_like('abc', 'AbC', 'i')", "INT:1"),
        ("regexp_like('good\\nday', '^day', 'm')", "INT:1"),
        ("regexp_like(NULL, 'a')", "NULL"),
        ("regexp_like('a', NULL)", "NULL"),
        ("regexp_like('a', 'a', NULL)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "AST: {expr}");
        assert_eq!(chunk_e(expr), want, "rewritten: {expr}");
    }

    assert!(chunk_e("regexp_like('abc', 'abc', 'p')").contains("Invalid match type"));
}
