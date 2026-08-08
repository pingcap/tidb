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
//! Go marshals a nil slice or nil map as `null`. Fresh derived Rust values use
//! [`null_default`] where an owned field needs Go's zero value, while the
//! receiver-mutating job/backfill codecs use the seeds here to preserve Go's
//! distinct null rules for scalars, pointers, slices, and maps.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Reports whether an incoming JSON object key matches a Go struct-field tag.
///
/// `encoding/json` prefers exact matches and then accepts ASCII-folded field
/// names. The model's persisted tags are all ASCII and unique under folding,
/// so `eq_ignore_ascii_case` is the exact observable rule for these objects.
pub(crate) fn go_json_field_matches(incoming: &str, tag: &str) -> bool {
    incoming == tag || incoming.eq_ignore_ascii_case(tag)
}

/// Receiver-mutating object decoder used by Go `json.Unmarshal` ports.
pub(crate) trait GoJsonMerge {
    /// Decodes one non-null JSON object into the existing receiver.
    fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>;
}

/// Deserializes a non-pointer field while treating JSON null as a no-op.
pub(crate) struct NullNoopSeed<'a, T>(pub(crate) &'a mut T);

impl<'de, T> DeserializeSeed<'de> for NullNoopSeed<'_, T>
where
    T: Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NullNoopVisitor<'a, T>(&'a mut T);

        impl<'de, T> Visitor<'de> for NullNoopVisitor<'_, T>
        where
            T: Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a value of the destination field type")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                *self.0 = T::deserialize(deserializer)?;
                Ok(())
            }
        }

        deserializer.deserialize_option(NullNoopVisitor(self.0))
    }
}

/// Deserializes a pointer-like object field into its existing allocation.
/// JSON null clears the pointer; a non-null object preserves omitted fields.
pub(crate) struct OptionMergeSeed<'a, T>(pub(crate) &'a mut Option<T>);

impl<'de, T> DeserializeSeed<'de> for OptionMergeSeed<'_, T>
where
    T: Default + GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionMergeVisitor<'a, T>(&'a mut Option<T>);

        impl<'de, T> Visitor<'de> for OptionMergeVisitor<'_, T>
        where
            T: Default + GoJsonMerge,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a JSON object")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                self.0
                    .get_or_insert_with(T::default)
                    .go_json_merge(deserializer)
            }
        }

        deserializer.deserialize_option(OptionMergeVisitor(self.0))
    }
}

/// Merges a JSON object into a Go map field and clears the map on JSON null.
pub(crate) struct OptionStringMapMergeSeed<'a, V>(pub(crate) &'a mut Option<BTreeMap<String, V>>);

impl<'de, V> DeserializeSeed<'de> for OptionStringMapMergeSeed<'_, V>
where
    V: Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionMapVisitor<'a, V> {
            destination: &'a mut Option<BTreeMap<String, V>>,
            marker: PhantomData<V>,
        }

        impl<'de, V> Visitor<'de> for OptionMapVisitor<'_, V>
        where
            V: Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a JSON object")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.destination = None;
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.destination = None;
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_map(self)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let destination = self.destination.get_or_insert_with(BTreeMap::new);
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<V>()?;
                    destination.insert(key, value);
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(OptionMapVisitor {
            destination: self.0,
            marker: PhantomData,
        })
    }
}

/// Assigns a Go byte slice only after its base64 value decodes successfully.
pub(crate) struct OptionBytesSeed<'a>(pub(crate) &'a mut Option<Vec<u8>>);

impl<'de> DeserializeSeed<'de> for OptionBytesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        *self.0 = go_bytes::deserialize(deserializer)?;
        Ok(())
    }
}

/// Consumes one unknown JSON field, matching Go's default unknown-field rule.
pub(crate) fn ignore_unknown<'de, A>(map: &mut A) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    map.next_value::<IgnoredAny>()?;
    Ok(())
}

/// Go `[]byte` JSON encoding: padded standard base64, with `null` retaining a
/// nil slice and `""` retaining an allocated empty slice.
pub mod go_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    fn encode(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let second = *chunk.get(1).unwrap_or(&0);
            let third = *chunk.get(2).unwrap_or(&0);
            let value = (u32::from(chunk[0]) << 16) | (u32::from(second) << 8) | u32::from(third);
            for position in 0..4 {
                if position <= chunk.len() {
                    output.push(ALPHABET[((value >> (18 - 6 * position)) & 0x3f) as usize] as char);
                } else {
                    output.push('=');
                }
            }
        }
        output
    }

    fn decode<E: serde::de::Error>(text: &str) -> Result<Vec<u8>, E> {
        // `encoding/base64.StdEncoding`, used by `encoding/json`, requires
        // padded four-byte quanta and ignores CR/LF only.
        let compact: Vec<u8> = text
            .bytes()
            .filter(|byte| !matches!(byte, b'\r' | b'\n'))
            .collect();
        if compact.len() % 4 != 0 {
            return Err(E::custom("illegal base64 data"));
        }

        let mut output = Vec::with_capacity(compact.len() / 4 * 3);
        let quartet_count = compact.len() / 4;
        for (quartet_index, quartet) in compact.chunks_exact(4).enumerate() {
            let is_last = quartet_index + 1 == quartet_count;
            let value = |byte| {
                ALPHABET
                    .iter()
                    .position(|candidate| *candidate == byte)
                    .map(|position| position as u32)
                    .ok_or_else(|| E::custom("illegal base64 data"))
            };
            let first = value(quartet[0])?;
            let second = value(quartet[1])?;
            output.push(((first << 2) | (second >> 4)) as u8);

            if quartet[2] == b'=' {
                if !is_last || quartet[3] != b'=' {
                    return Err(E::custom("illegal base64 data"));
                }
                continue;
            }
            let third = value(quartet[2])?;
            output.push((((second & 0x0f) << 4) | (third >> 2)) as u8);

            if quartet[3] == b'=' {
                if !is_last {
                    return Err(E::custom("illegal base64 data"));
                }
                continue;
            }
            let fourth = value(quartet[3])?;
            output.push((((third & 0x03) << 6) | fourth) as u8);
        }
        Ok(output)
    }

    /// Serializes nil as `null` and bytes as padded standard base64.
    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some(bytes) => serializer.serialize_str(&encode(bytes)),
        }
    }

    /// Deserializes Go's nil/empty/base64 byte-slice JSON forms.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        match Option::<String>::deserialize(deserializer)? {
            None => Ok(None),
            Some(text) => decode(&text).map(Some),
        }
    }
}

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
