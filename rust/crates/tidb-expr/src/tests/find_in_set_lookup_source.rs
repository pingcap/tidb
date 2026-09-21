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
//! internal `constStrlistLookupCache` lifecycle. The Rust cache is exercised
//! below with the same lookup value, context replacement, and NULL memoization
//! contract.

use super::*;

/// GO PORT of `pkg/expression/builtin_string_test.go:1107
/// TestFindInSetConstStrlistLookup`'s value rows.
///
/// The Go signature uses `utf8mb4_general_ci`, a PAD SPACE collation, yet
/// FIND_IN_SET(' ', '  , , ,') returns 2, which is only possible because it
/// compares `collator.KeyWithoutTrimRightSpace` instead of the sort key
/// (`pkg/expression/builtin_string.go:2680 findInSetByKey`, called from
/// :2760): ordinary PAD SPACE trimming would make needle " " collapse onto
/// every entry. The list "  , , ," starts with a two-space field followed by
/// one-space fields, so the FIRST one-space member wins for a one-space
/// needle while a repeated-member list must answer its FIRST position. The
/// chunk cases below carry the explicit collation metadata; the AST cases
/// retain this evaluator's documented connection-collation boundary.
#[test]
fn find_in_set_const_strlist_pad_space_lookup_value_rows() {
    let spaces = "find_in_set(' ', '  , , ,')";
    let repeated = "find_in_set('a', 'a,b,a')";
    // Needle " ", list "  , , ,": the two-space leading field does NOT match,
    // so the first genuine one-space member lands at index 2. Trailing spaces
    // are NOT equal even under a PAD SPACE collation.
    for (tier_name, tier_evaluator) in [("ast", e as fn(&str) -> String), ("chunk", chunk_e)] {
        let value = tier_evaluator(spaces);
        assert_eq!(value, "INT:2", "{tier_name} tier result: {value}");
        // Repeated evaluation keeps answering identically (the Go cache's
        // observable contract).
        assert_eq!(tier_evaluator(spaces), "INT:2");
        // First-match semantics on the duplicated 'a' list.
        assert_eq!(tier_evaluator(repeated), "INT:1");
    }
    let spaces_collated =
        "find_in_set(' ' collate utf8mb4_general_ci, '  , , ,' collate utf8mb4_general_ci)";
    let repeated_collated =
        "find_in_set('a' collate utf8mb4_general_ci, 'a,b,a' collate utf8mb4_general_ci)";
    assert_eq!(chunk_e(spaces_collated), "INT:2");
    assert_eq!(chunk_e(repeated_collated), "INT:1");
    // And under plain comparison the pad-space collation behaves like any
    // other general_ci membership decision: case-insensitive.
    assert_eq!(
        chunk_e("find_in_set('B' collate utf8mb4_general_ci, 'a,b,c' collate utf8mb4_general_ci)"),
        "INT:2"
    );
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
        chunk_e(&format!(
            "find_in_set(' '{general_ci}, '  , , ,'{general_ci})"
        )),
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

/// GO PORT of the internal `constStrlistLookupCache` lifecycle assertions.
#[test]
fn find_in_set_strlist_cache_lifecycle() {
    use crate::builtin_ext::{build_find_in_set_lookup, BuiltinFuncCache};
    use std::sync::Arc;

    let cache = BuiltinFuncCache::default();
    let collation = tidb_datatype::Collation::Utf8Mb4GeneralCi;
    let list = Datum::new_collation_string("a,b,a", collation);
    let first = cache
        .get_or_init_cache(11, || build_find_in_set_lookup(&list, collation))
        .expect("lookup constructor");
    let hit = cache
        .get_or_init_cache(
            11,
            || -> Result<crate::builtin_ext::FindInSetLookup, &'static str> {
                panic!("same context must not rebuild")
            },
        )
        .expect("lookup hit");
    assert!(Arc::ptr_eq(&first, &hit));
    assert_eq!(
        crate::builtin_ext::find_in_set_lookup(
            &Datum::new_collation_string("a", collation),
            &first,
            collation,
        )
        .expect("lookup value"),
        Datum::Int(1)
    );

    let null = cache
        .get_or_init_cache(12, || build_find_in_set_lookup(&Datum::Null, collation))
        .expect("NULL lookup constructor");
    assert!(null.is_null);
    assert!(cache.get_cache(11).is_none());
}
