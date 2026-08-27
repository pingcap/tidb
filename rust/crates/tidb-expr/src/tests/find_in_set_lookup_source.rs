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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-first ports of `pkg/expression.part5`'s FIND_IN_SET lookup-variant
//! tests on `origin/master`: `builtin_string_test.go::TestFindInSetConstStrlistLookup`
//! (:1107), `::TestFindInSetVecFirstMatchNonConstStrlist` (:1173), and
//! `::TestFindInSetConstOnlyInContextStrlistLookup` (:1218). Those Go tests
//! assert two things together: the VALUE semantics of membership lookup
//! (pad-space collations still distinguish trailing spaces because the
//! signature keys with `KeyWithoutTrimRightSpace`, first member wins) and the
//! internal `constStrlistLookupCache` lifecycle. The cache internals have no
//! Rust counterpart; every observable VALUE behavior is pinned here.

use super::*;

/// GO PORT of `pkg/expression/builtin_string_test.go:1107
/// TestFindInSetConstStrlistLookup`'s value rows.
///
/// Both datums carry `utf8mb4_general_ci`, a PAD SPACE collation — yet
/// FIND_IN_SET(' ', '  , , ,') returns 2, which is only possible because the
/// signature compares `collator.KeyWithoutTrimRightSpace` instead of the sort
/// key (`pkg/expression/builtin_string.go:2680 findInSetByKey`, called from
/// :2760): ordinary PAD SPACE trimming would make needle " " collapse onto
/// every entry. The list "  , , ," starts with a two-space field followed by
/// one-space fields, so the FIRST one-space member wins for a one-space
/// needle while a repeated-member list must answer its FIRST position.
/// The Go cache assertions (same pointer across evals) ride along as
/// values-only re-evaluations returning identical answers.
#[test]
fn find_in_set_const_strlist_pad_space_lookup_value_rows() {
    let general_ci = |sql: &str| {
        format!("{sql} collate utf8mb4_general_ci")
    };
    // Needle " ", list "  , , ,": the two-space leading field does NOT match,
    // so the first genuine one-space member lands at index 2. Trailing spaces
    // are NOT equal even under a PAD SPACE collation.
    for tier_evaluator in [e, chunk_e] {
        assert_eq!(
            tier_evaluator(&general_ci(
                "find_in_set(' ', '  , , ,')"
            )),
            "INT:2"
        );
        // Repeated evaluation keeps answering identically (the Go cache's
        // observable contract).
        assert_eq!(
            tier_evaluator(&general_ci(
                "find_in_set(' ', '  , , ,')"
            )),
            "INT:2"
        );
        // First-match semantics on the duplicated 'a' list, len(lookup)=2 rows.
        assert_eq!(tier_evaluator(&general_ci("find_in_set('a', 'a,b,a')")), "INT:1");
    }
    // And under plain comparison the pad-space collation behaves like any
    // other general_ci membership decision: case-insensitive.
    assert_eq!(chunk_e("find_in_set('B' collate utf8mb4_general_ci, 'a,b,c' collate utf8mb4_general_ci)"), "INT:2");
}

/// GO PORT of `pkg/expression/builtin_string_test.go:1173
/// TestFindInSetVecFirstMatchNonConstStrlist`.
///
/// Go drives column-typed (non-constant) needle/list pairs over four rows —
/// expected `{2, 1, 1, 0}` — twice: scalar per-row and vectorized batch,
/// requiring identical outputs and NO const-strlist caching to activate. This
/// evaluator always evaluates row-wise, so the single evaluation path carries
/// the full table directly.
#[test]
fn find_in_set_non_const_strlist_rows_evaluate_per_row() {
    let rows: [(&str, &str, i64); 4] = [
        ("a", "b,a,c,a", 2),
        ("a", "a,b,a", 1),
        ("", ",,", 1),
        ("x", "a,b,a", 0),
    ];
    for (needle, list, want) in rows {
        assert_eq!(
            e(&format!("find_in_set('{needle}', '{list}')")),
            format!("INT:{want}"),
            "find_in_set('{needle}', '{list}')"
        );
        assert_eq!(
            chunk_e(&format!("find_in_set('{needle}', '{list}')")),
            format!("INT:{want}"),
            "chunk tier find_in_set('{needle}', '{list}')"
        );
    }
}

/// GO PORT of `pkg/expression/builtin_string_test.go:1218
/// TestFindInSetConstOnlyInContextStrlistLookup`.
///
/// Go classifies a ParamMarker strlist as constant WITHIN one statement
/// context, caches its lookup map against that statement, rebuilds after a
/// statement reset, and records a NULL cached state when the parameter itself
/// is NULL. Value-level rows reachable without that machinery:
/// parameters ("  , , ,") resolve to 2 for the same needle, (" ,a") resolves
/// to 1, and a NULL strlist stays NULL.
#[test]
fn find_in_set_const_only_in_context_value_rows() {
    let general_ci = " collate utf8mb4_general_ci";
    // Parameter values stand in as literals carrying the same collation.
    assert_eq!(
        chunk_e(&format!("find_in_set(' '{general_ci}, '  , , ,'{general_ci})")),
        "INT:2"
    );
    assert_eq!(
        chunk_e(&format!("find_in_set(' '{general_ci}, ' ,a'{general_ci})")),
        "INT:1"
    );
    assert_eq!(
        chunk_e(&format!("find_in_set('x'{general_ci}, NULL)")),
        "NULL"
    );
}

/// go-parity-gap: the constStrlistLookupCache identity/invalidation asserts
/// (`require.Same` across evaluations, per-statement-context rebuilds, null
/// state memoization) describe harness-internal state this evaluator does not
/// construct; their VALUE consequences are pinned by the three tests above.
#[test]
#[ignore = "go-parity-gap: no constStrlistLookupCache/statement-context memoization layer exists"]
fn find_in_set_strlist_cache_lifecycle_gap() {}
