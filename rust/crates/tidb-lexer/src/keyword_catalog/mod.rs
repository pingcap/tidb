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

//! Complete TiDB SQL keyword catalog transcreated from `pkg/parser/keywords.go`.

/// One entry in TiDB's public SQL keyword catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keyword {
    /// Uppercase SQL keyword spelling.
    pub word: &'static str,
    /// Whether the keyword is reserved by TiDB's grammar.
    pub reserved: bool,
    /// Source section used to preserve TiDB's grouped ordering.
    pub section: &'static str,
}

mod reserved;
mod tidb_specific;
mod unreserved;

const KEYWORD_COUNT: usize = 684;

/// Concatenates the section parts into one contiguous array, in the same
/// order the Go parser declares them, entirely at compile time.
const fn build_keywords() -> [Keyword; KEYWORD_COUNT] {
    let mut out = [Keyword {
        word: "",
        reserved: false,
        section: "",
    }; KEYWORD_COUNT];
    let mut pos = 0;
    {
        let src = reserved::KEYWORDS_RESERVED;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    {
        let src = unreserved::KEYWORDS_UNRESERVED;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    {
        let src = tidb_specific::KEYWORDS_TIDB;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    out
}

static KEYWORDS_ARRAY: [Keyword; KEYWORD_COUNT] = build_keywords();

/// All TiDB SQL keywords in the same order and sections as the Go parser.
pub static KEYWORDS: &[Keyword] = &KEYWORDS_ARRAY;
