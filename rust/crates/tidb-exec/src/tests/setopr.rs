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

//! Table-less set-operation and public query-dispatch tests.

use super::*;

#[test]
fn set_operations() {
    assert_eq!(run("select 1 union select 2"), "RS:1;2");
    assert_eq!(run("select 2 union select 1"), "RS:1;2");
    assert_eq!(run("select 1 union all select 1"), "RS:1;1");
    assert_eq!(run("select 1 union select 2 union select 1"), "RS:1;2");
    assert_eq!(run("select 1 except select 1"), "RS:");
    assert_eq!(run("select 1 intersect select 1"), "RS:1");
    assert_eq!(
        run("select 'a' union select 'b' union select 'a'"),
        "RS:a;b"
    );
}

#[test]
fn query_envelope_dispatches_select_and_set_operations() {
    assert_eq!(run("select 1"), "RS:1");
    assert_eq!(run("select 1 union select 2"), "RS:1;2");
    assert_eq!(
        run("with c as (select 1 as n) select n from c union select 2"),
        "Unsupported(\"WITH before set operation\")"
    );
}

/// A parenthesized nested term folds into its surrounding set operation just
/// like a flat multi-term expression (confirmed via `gorun`).
#[test]
fn nested_set_op_term() {
    assert_eq!(
        run("select 1 union (select 2 union all select 1)"),
        "RS:1;2"
    );
    assert_eq!(
        run("select 1 union all (select 2 union all select 1)"),
        "RS:1;1;2"
    );
    assert_eq!(
        run("select 1 intersect (select 1 union all select 2)"),
        "RS:1"
    );
}
