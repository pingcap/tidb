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

//! Lightweight parsing strings from `pkg/types/string.go`.

/// Shared input contract for temporal parsers.
pub trait SourceString {
    /// Returns the current text.
    fn as_str(&self) -> &str;
}

/// Stable source string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlainStr<'a>(pub &'a str);

impl SourceString for PlainStr<'_> {
    fn as_str(&self) -> &str {
        self.0
    }
}

/// A string that may have originated in an aliased decode buffer.
///
/// Rust borrowing prevents mutation while this value is observed; `freeze`
/// retains the Go error-construction boundary when an owned diagnostic is
/// required.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HackedStr<'a>(pub &'a str);

impl HackedStr<'_> {
    /// `FreezeStr`.
    pub fn freeze(self) -> String {
        self.0.to_owned()
    }
}

impl SourceString for HackedStr<'_> {
    fn as_str(&self) -> &str {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_string_wrappers_preserve_and_freeze_text() {
        assert_eq!(PlainStr("stable").as_str(), "stable");
        let hacked = HackedStr("aliased");
        assert_eq!(hacked.as_str(), "aliased");
        assert_eq!(hacked.freeze(), "aliased");
    }
}
