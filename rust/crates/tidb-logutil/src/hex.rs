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

//! Transcreation of Go `pkg/util/logutil/hex.go`: pretty-printing values
//! for logs with byte slices hex-encoded.
//!
//! Go walks arbitrary values with `reflect`; Rust has no runtime struct
//! reflection, so the walk happens over an explicit [`PrettyValue`] tree
//! that message types build from their fields (a `From` impl per logged
//! proto type). The rendering — `{Field:value ...}` with `[]byte` fields
//! hex-encoded and nil pointers as `<nil>` — matches the source.

use std::fmt;
use std::fmt::Write as _;

/// A value tree for pretty-printing (the shapes Go's `prettyPrint`
/// reflects over).
pub enum PrettyValue {
    /// A `[]byte`: rendered hex-encoded.
    Bytes(Vec<u8>),
    /// Any other slice: elements space-separated in brackets (Go `%s` on
    /// the slice value).
    Slice(Vec<PrettyValue>),
    /// A struct: `{Name:value Name:value}`.
    Struct(Vec<(&'static str, PrettyValue)>),
    /// A nil pointer: `<nil>`.
    Nil,
    /// A scalar rendered with its display form (Go `%v`).
    Display(String),
}

/// Go `prettyPrint`.
pub fn pretty_print(w: &mut String, val: &PrettyValue) {
    match val {
        PrettyValue::Bytes(b) => {
            for byte in b {
                let _ = write!(w, "{byte:02x}");
            }
        }
        PrettyValue::Slice(items) => {
            w.push('[');
            for (i, item) in items.iter().enumerate() {
                if i != 0 {
                    w.push(' ');
                }
                pretty_print(w, item);
            }
            w.push(']');
        }
        PrettyValue::Struct(fields) => {
            w.push('{');
            for (i, (name, v)) in fields.iter().enumerate() {
                if i != 0 {
                    w.push(' ');
                }
                let _ = write!(w, "{name}:");
                pretty_print(w, v);
            }
            w.push('}');
        }
        PrettyValue::Nil => w.push_str("<nil>"),
        PrettyValue::Display(s) => w.push_str(s),
    }
}

/// Go `Hex`: wraps a value tree in a `Display` (`fmt.Stringer`) adapter.
pub struct Hex(pub PrettyValue);

impl fmt::Display for Hex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        pretty_print(&mut s, &self.0);
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Goldens from Go hex_test.go.
    #[test]
    fn pretty_print_goldens() {
        let mut buf = String::new();
        let byte_slice = "asd2fsdafs中文3af".as_bytes().to_vec();
        pretty_print(&mut buf, &PrettyValue::Bytes(byte_slice));
        assert_eq!(buf, "61736432667364616673e4b8ade69687336166");

        let mut buf = String::new();
        pretty_print(
            &mut buf,
            &PrettyValue::Bytes(vec![1, 2, 3, b'a', b'b', b'c', b'\'']),
        );
        assert_eq!(buf, "01020361626327");

        // kv.KeyRange{StartKey: "_txxey23_i263", EndKey: nil}
        let mut buf = String::new();
        pretty_print(
            &mut buf,
            &PrettyValue::Struct(vec![
                ("StartKey", PrettyValue::Bytes(b"_txxey23_i263".to_vec())),
                ("EndKey", PrettyValue::Bytes(Vec::new())),
            ]),
        );
        assert_eq!(buf, "{StartKey:5f747878657932335f69323633 EndKey:}");
    }

    // The metapb.Region golden from Go TestHex, over an explicit tree.
    #[test]
    fn hex_region_golden() {
        let region = PrettyValue::Struct(vec![
            ("Id", PrettyValue::Display("6662".into())),
            (
                "StartKey",
                PrettyValue::Bytes(vec![
                    b't', 200, b'\\', 0, 0, 0, b'\\', 0, 0, 0, 37, b'-', 0, 0, 0, 0, 0, 0, 0, 37,
                ]),
            ),
            ("EndKey", PrettyValue::Bytes(b"3asg3asd".to_vec())),
            ("RegionEpoch", PrettyValue::Nil),
            ("Peers", PrettyValue::Slice(Vec::new())),
            ("EncryptionMeta", PrettyValue::Nil),
            ("IsInFlashback", PrettyValue::Display("false".into())),
            ("FlashbackStartTs", PrettyValue::Display("0".into())),
        ]);
        assert_eq!(
            Hex(region).to_string(),
            "{Id:6662 StartKey:74c85c0000005c000000252d0000000000000025 EndKey:3361736733617364 RegionEpoch:<nil> Peers:[] EncryptionMeta:<nil> IsInFlashback:false FlashbackStartTs:0}"
        );
    }
}
