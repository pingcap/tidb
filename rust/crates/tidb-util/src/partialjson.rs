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

//! Complete transcreation of Go `pkg/util/partialjson` (`extract.go`):
//! extract requested top-level members from a JSON object without parsing the
//! rest of the document.
//!
//! Go hand-rolls a `topLevelJSONTokenIter` over `encoding/json`'s
//! `Decoder.Token()` stream and returns each member as a `[]json.Token`. That
//! token vector is an artifact of Go's decoder API, not of the package's
//! contract; the equal Rust library is `serde_json` (already a dependency),
//! driven in streaming mode. Each requested member is captured as a
//! [`RawValue`] — its exact source text, a strictly stronger representation
//! than Go's re-tokenized stream.
//!
//! Observable semantics preserved (verified against the real Go package, not
//! assumed):
//! - **Early stop**: parsing halts as soon as every requested name has been
//!   seen — content after that point (even invalid JSON, e.g. a truncated
//!   tail, which is this package's reason to exist) never fails the call.
//!   `serde_json`'s `deserialize_map` insists on consuming the closing brace
//!   after the visitor returns, so the visitor hands its results out through a
//!   side channel and sets a `done` flag; a parse error raised after `done`
//!   is by definition in the never-requested tail and is ignored, exactly the
//!   bytes Go never reads.
//! - **First occurrence wins** for a duplicated member name.
//! - **Empty `names` reads nothing**: Go's loop never touches the iterator, so
//!   even garbage content succeeds with an empty result.
//! - A missing requested name, a non-object top level, or malformed JSON
//!   before the last requested member is an error; Go returns a nil map
//!   alongside, which `Err` already expresses.
//!
//! Documented deviation: error *text* comes from `serde_json` rather than Go's
//! `encoding/json` (e.g. Go's `invalid character 'a'` and `io.EOF`), exactly
//! as this workspace's table-filter port carries `regex`-crate wording rather
//! than Go `regexp` wording. Which inputs fail is unchanged.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
pub use serde_json::value::RawValue;

/// Error returned by [`extract_top_level_members`].
#[derive(Debug, Clone)]
pub struct PartialJsonError(String);

impl fmt::Display for PartialJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PartialJsonError {}

struct ExtractSeed<'c> {
    remain: HashSet<String>,
    out: &'c RefCell<HashMap<String, Box<RawValue>>>,
    done: &'c Cell<bool>,
}

impl<'de> Visitor<'de> for ExtractSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The counterpart of Go's "expected '{' for topLevelJSONTokenIter".
        f.write_str("a top-level JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while !self.remain.is_empty() {
            let Some(name) = map.next_key::<String>()? else {
                // The object ended before every requested name was found; Go
                // surfaces the iterator's io.EOF here.
                return Err(serde::de::Error::custom(
                    "EOF before all requested top-level members were found",
                ));
            };
            // `remove` is true only for the FIRST occurrence of a requested
            // name, so a duplicate is discarded exactly as in Go.
            if self.remain.remove(&name) {
                let value = map.next_value::<Box<RawValue>>()?;
                self.out.borrow_mut().insert(name, value);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        // All members captured; anything the deserializer trips over past this
        // point is in the tail Go never reads.
        self.done.set(true);
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for ExtractSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

/// Extracts the given top-level members from a JSON object. It stops parsing
/// as soon as all names are found. Port of Go's `ExtractTopLevelMembers`; each
/// member is returned as its exact source text ([`RawValue`]) instead of Go's
/// decoder-specific `[]json.Token`.
///
/// # Errors
///
/// Fails when the top level is not an object, the JSON before the last
/// requested member is malformed, or the object ends before every requested
/// name is found.
pub fn extract_top_level_members(
    content: &[u8],
    names: &[&str],
) -> Result<HashMap<String, Box<RawValue>>, PartialJsonError> {
    // Go's loop condition is `len(remainNames) > 0`; with no names it never
    // touches the iterator, so even invalid content succeeds.
    if names.is_empty() {
        return Ok(HashMap::new());
    }

    let out = RefCell::new(HashMap::with_capacity(names.len()));
    let done = Cell::new(false);
    let seed = ExtractSeed {
        remain: names.iter().map(|s| (*s).to_string()).collect(),
        out: &out,
        done: &done,
    };
    let mut de = serde_json::Deserializer::from_slice(content);
    // No `de.end()`: content after the top-level object is never inspected.
    match seed.deserialize(&mut de) {
        Ok(()) => Ok(out.into_inner()),
        // An error after `done` was raised while closing over the unread tail
        // (serde_json's `end_map`), which the early stop deliberately skips.
        Err(_) if done.get() => Ok(out.into_inner()),
        Err(e) => Err(PartialJsonError(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::extract_top_level_members;

    fn raw(map: &super::HashMap<String, Box<super::RawValue>>, key: &str) -> String {
        map[key].get().to_string()
    }

    // Go `TestIter`'s failure cases, driven through the public API (the
    // iterator itself is Go's private tokenizer, replaced by serde_json).
    // The same inputs fail; the error text is serde_json's.
    #[test]
    fn failure_cases() {
        for content in ["{", "[]", "{a}", "{]", "{}", r#"{"a": 1}"#] {
            // `{}`/`{"a": 1}` fail because the requested name is missing.
            let err = extract_top_level_members(content.as_bytes(), &["missing"]);
            assert!(err.is_err(), "content: {content}");
        }
    }

    // Go `TestIter`'s success cases: the captured members equal the exact
    // source text of each value.
    #[test]
    fn success_cases() {
        let got = extract_top_level_members(br#"{"a": 1, "b": "val"}"#, &["a", "b"]).unwrap();
        assert_eq!(raw(&got, "a"), "1");
        assert_eq!(raw(&got, "b"), r#""val""#);

        let content =
            r#"{"a": 1, "long1": {"skip": "skip"}, "b": "val", "long2": [0,0,{"skip":2}]}"#;
        let got =
            extract_top_level_members(content.as_bytes(), &["a", "long1", "b", "long2"]).unwrap();
        assert_eq!(raw(&got, "a"), "1");
        assert_eq!(raw(&got, "long1"), r#"{"skip": "skip"}"#);
        assert_eq!(raw(&got, "b"), r#""val""#);
        assert_eq!(raw(&got, "long2"), r#"[0,0,{"skip":2}]"#);
    }

    // Early stop, first-occurrence-wins, and the empty-names shortcut — all
    // verified against the real Go package's outputs before porting.
    #[test]
    fn go_observable_semantics() {
        // Content after the last requested member is never parsed, even if it
        // is not valid JSON (e.g. a truncated document).
        let got = extract_top_level_members(br#"{"a": 1, "b": 2, GARBAGE"#, &["a"]).unwrap();
        assert_eq!(raw(&got, "a"), "1");

        // Requested members can appear in any order relative to `names`, and a
        // later member may be the stopping point.
        let got =
            extract_top_level_members(br#"{"x": {"y": [1,2]}, "z": "s"}"#, &["z", "x"]).unwrap();
        assert_eq!(raw(&got, "x"), r#"{"y": [1,2]}"#);
        assert_eq!(raw(&got, "z"), r#""s""#);

        // A duplicated name keeps its first value.
        let got = extract_top_level_members(br#"{"a": 1, "a": 2}"#, &["a"]).unwrap();
        assert_eq!(raw(&got, "a"), "1");

        // Empty names: the content is never read at all.
        let got = extract_top_level_members(b"complete garbage", &[]).unwrap();
        assert!(got.is_empty());

        // Malformed JSON BEFORE the last requested member still fails: the
        // early-stop tolerance never masks a broken needed value.
        assert!(extract_top_level_members(br#"{"b": GARBAGE, "a": 1}"#, &["a"]).is_err());
    }
}
