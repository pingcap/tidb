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

//! Direct transcreation of `pkg/parser/keywords_test.go`.

use tidb_lexer::KEYWORDS;

#[test]
fn test_keywords() {
    assert_eq!(KEYWORDS[0].word, "ADD");
    assert!(KEYWORDS[0].reserved);
    assert!(KEYWORDS.iter().any(|keyword| keyword.word == "ADMIN"));
}

#[test]
fn test_keywords_length() {
    // `pkg/parser/keywords.go` on this branch -- the list Go's
    // `information_schema.KEYWORDS` reads -- holds 684 keywords, 233 reserved.
    assert_eq!(KEYWORDS.len(), 684);
    assert_eq!(
        KEYWORDS.iter().filter(|keyword| keyword.reserved).count(),
        233
    );
}

#[test]
fn test_keywords_match_this_branchs_keywords_go() {
    // Go-master's parser.y adds these; this branch's keywords.go does not
    // list them, so neither may the catalog (the Go oracle answers 684 rows).
    for word in [
        "ALERT",
        "AUTO",
        "FAST",
        "IMMEDIATE",
        "MATERIALIZED",
        "OPERATE",
    ] {
        assert!(
            KEYWORDS.iter().all(|keyword| keyword.word != word),
            "{word} is not in this branch's keywords.go"
        );
    }
}

#[test]
fn test_keywords_sorting() {
    for pair in KEYWORDS.windows(2) {
        if pair[0].section == pair[1].section {
            assert!(
                pair[0].word <= pair[1].word,
                "{} should come after {}",
                pair[0].word,
                pair[1].word
            );
        }
    }
}
