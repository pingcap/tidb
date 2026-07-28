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

//! Serde adapters that reproduce `encoding/json` semantics for the meta model.
//!
//! Go marshals a nil slice or nil map as `null` and unmarshals `null` into any
//! field by leaving it at its zero value. Rust's owned `Vec`/`String`/`BTreeMap`
//! have no nil state, so every such field decodes `null` through
//! [`null_default`] instead of failing on a type mismatch.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Reproduces `encoding/json`'s float formatting, which `serde_json` does not:
/// Go prints an integral float as `0`/`1`, not `0.0`/`1.0`, and switches to
/// exponent form only outside `[1e-6, 1e21)`.
///
/// Go: `pkg/encoding/json/encode.go`, `floatEncoder.encode`.
struct GoFloatFormatter;

impl serde_json::ser::Formatter for GoFloatFormatter {
    fn write_f32<W: std::io::Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        self.write_f64(writer, f64::from(value))
    }

    fn write_f64<W: std::io::Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        let magnitude = value.abs();
        if magnitude != 0.0 && !(1e-6..1e21).contains(&magnitude) {
            // Go emits a signed exponent with no padding: 1e+21, 1e-07 -> 1e-7.
            let exponential = format!("{value:e}");
            let (mantissa, exponent) = exponential.split_once('e').unwrap_or((&exponential, "0"));
            let (sign, digits) = exponent
                .strip_prefix('-')
                .map_or(('+', exponent), |rest| ('-', rest));
            write!(writer, "{mantissa}e{sign}{digits}")
        } else {
            write!(writer, "{value}")
        }
    }
}

/// Serializes to the exact bytes Go's `json.Marshal` produces.
///
/// `encoding/json` escapes `<`, `>` and `&` so that output can be embedded in
/// HTML, and escapes U+2028/U+2029 so it can be embedded in JavaScript.
/// `serde_json` emits all five literally. None of those five bytes/runes can
/// appear outside a JSON string literal, so rewriting them over the finished
/// document is exact rather than a heuristic.
///
/// This matters for real catalog values: a CHECK constraint's `expr_string`,
/// a generated column's expression, and a partition expression all routinely
/// contain `<` or `>`.
///
/// Float formatting is corrected at the same time, via [`GoFloatFormatter`].
pub fn to_go_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut encoded = Vec::new();
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut encoded,
        GoFloatFormatter,
    ))?;
    let mut out = Vec::with_capacity(encoded.len());
    let mut rest = encoded.as_slice();
    while let Some((&byte, tail)) = rest.split_first() {
        match byte {
            b'<' => out.extend_from_slice(b"\\u003c"),
            b'>' => out.extend_from_slice(b"\\u003e"),
            b'&' => out.extend_from_slice(b"\\u0026"),
            // U+2028 / U+2029 in UTF-8.
            0xE2 if tail.starts_with(b"\x80\xA8") || tail.starts_with(b"\x80\xA9") => {
                out.extend_from_slice(if tail[1] == 0xA8 {
                    b"\\u2028"
                } else {
                    b"\\u2029"
                });
                rest = &tail[2..];
                continue;
            }
            other => out.push(other),
        }
        rest = tail;
    }
    Ok(out)
}

/// Deserializes `null` (and a missing field, via `#[serde(default)]`) into
/// `T::default()`, matching Go's zero-value handling of a JSON null.
pub fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Serializes an empty slice as `null` rather than `[]`.
///
/// A Go slice field that is never explicitly allocated stays nil, and
/// `encoding/json` writes nil as `null`. TiDB's catalog writer leaves these
/// slices nil whenever they are empty, so emitting `null` is what makes a
/// value read from TiKV re-serialize to the same bytes.
pub fn null_if_empty<S, T>(value: &[T], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    if value.is_empty() {
        serializer.serialize_none()
    } else {
        value.serialize(serializer)
    }
}

/// Go `len(v) == 0` for the `omitempty` check on a slice.
#[expect(clippy::ptr_arg, reason = "serde's skip_serializing_if signature")]
pub fn is_empty_vec<T>(value: &Vec<T>) -> bool {
    value.is_empty()
}

/// Go `s == ""` for the `omitempty` check on a string.
pub fn is_empty_str(value: &str) -> bool {
    value.is_empty()
}

/// Go `n == 0` for the `omitempty` check on a numeric field.
pub fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

/// Go `n == 0` for the `omitempty` check on an unsigned numeric field.
pub fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Go `!b` for the `omitempty` check on a bool field.
pub fn is_false(value: &bool) -> bool {
    !*value
}

/// Serializes an integer-keyed map in Go's key order.
///
/// `encoding/json` renders a `map[int64]bool`'s keys as strings and sorts them
/// by that string form, so `{2, 10}` comes out as `10` then `2`. A `BTreeMap`
/// orders numerically; sorting the rendered keys here restores Go's order.
pub fn go_int_key_map<S>(
    value: &std::collections::BTreeMap<i64, bool>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let mut rendered: Vec<(String, bool)> = value
        .iter()
        .map(|(key, flag)| (key.to_string(), *flag))
        .collect();
    rendered.sort_by(|left, right| left.0.cmp(&right.0));

    let mut map = serializer.serialize_map(Some(rendered.len()))?;
    for (key, flag) in &rendered {
        map.serialize_entry(key, flag)?;
    }
    map.end()
}
