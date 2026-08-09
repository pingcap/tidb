// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct transcreation of `pkg/parser/consistent_test.go`.

use std::collections::BTreeSet;

use super::super::{
    is_reserved,
    keyword_catalog::KEYWORDS,
    keywords::{BUILTIN_FUNC_KEYWORDS, GENERAL_KEYWORDS, WINDOW_FUNC_KEYWORDS},
    Lexer, TokenKind,
};

fn assert_sorted_unique(name: &str, words: &[&str]) {
    for pair in words.windows(2) {
        assert!(
            pair[0] < pair[1],
            "{name} must be strictly sorted: {:?} is followed by {:?}",
            pair[0],
            pair[1]
        );
    }
}

#[test]
fn test_keyword_consistent() {
    assert_sorted_unique("tokenMap", GENERAL_KEYWORDS);
    assert_sorted_unique("btFuncTokenMap", BUILTIN_FUNC_KEYWORDS);
    assert_sorted_unique("windowFuncTokenMap", WINDOW_FUNC_KEYWORDS);

    let general: BTreeSet<_> = GENERAL_KEYWORDS.iter().copied().collect();
    let window: BTreeSet<_> = WINDOW_FUNC_KEYWORDS.iter().copied().collect();
    assert!(
        general.is_disjoint(&window),
        "ordinary and window-function token maps must stay disjoint"
    );

    // `aliases` in misc.go contains four spellings which deliberately share
    // another spelling's token. The Rust lexer collapses keyword token IDs,
    // so preserve the equivalent invariant: both spellings must remain in
    // the same keyword map and scan to the same lexical class.
    let aliases = [
        ("SCHEMA", "DATABASE"),
        ("SCHEMAS", "DATABASES"),
        ("DEC", "DECIMAL"),
        ("SUBSTR", "SUBSTRING"),
    ];
    for (alias, canonical) in aliases {
        assert_ne!(alias, canonical);
        assert!(general.contains(alias), "missing alias {alias}");
        assert!(general.contains(canonical), "missing target {canonical}");
        assert_eq!(
            Lexer::new(alias).next_token().kind,
            Lexer::new(canonical).next_token().kind,
            "{alias} and {canonical} must share a lexical class"
        );
    }

    // Go's grammar declares 815 unique keyword spellings: tokenMap plus the
    // window-function map, less the four aliases that share token IDs.
    assert_eq!(
        GENERAL_KEYWORDS.len() + WINDOW_FUNC_KEYWORDS.len() - aliases.len(),
        815
    );

    // `KEYWORDS` is generated from the grammar's ReservedKeyword,
    // UnReservedKeyword and TiDBKeyword productions (NotKeywordToken is
    // intentionally absent). Every catalog row must still be recognized by
    // the lexer, and the independently maintained reserved predicate must
    // agree with the grammar-derived catalog.
    let mut catalog_words = BTreeSet::new();
    for keyword in KEYWORDS {
        assert!(
            catalog_words.insert(keyword.word),
            "duplicate catalog keyword {}",
            keyword.word
        );
        assert!(
            general.contains(keyword.word) || window.contains(keyword.word),
            "catalog keyword {} is absent from the lexer token maps",
            keyword.word
        );
        assert_eq!(
            is_reserved(keyword.word),
            keyword.reserved,
            "reserved status drifted for {}",
            keyword.word
        );
        assert_eq!(
            Lexer::new(keyword.word).next_token().kind,
            TokenKind::Keyword,
            "catalog keyword {} no longer scans as a keyword",
            keyword.word
        );
    }
}
