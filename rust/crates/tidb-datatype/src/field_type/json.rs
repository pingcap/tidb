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

//! Go-compatible JSON representation for parser field types.

use std::fmt;

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{FieldType, FieldTypeCode};
use crate::Collation;

impl FieldType {
    /// Serializes the source JSON field names and values.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&JsonFieldType::from(self))
    }

    /// Deserializes the source JSON representation.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<JsonFieldType>(data).map(Into::into)
    }
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct JsonFieldType {
    #[serde(default)]
    Tp: u8,
    #[serde(default)]
    Flag: u64,
    #[serde(default)]
    Flen: i64,
    #[serde(default)]
    Decimal: i64,
    #[serde(default)]
    Charset: String,
    #[serde(default)]
    Collate: String,
    #[serde(default)]
    Elems: Option<Vec<String>>,
    #[serde(default)]
    ElemsIsBinaryLit: Option<Vec<bool>>,
    #[serde(default)]
    Array: bool,
}

fn go_json_ascii_tag_matches(incoming: &str, tag: &str) -> bool {
    if incoming == tag {
        return true;
    }
    // Every jsonFieldType member name is ASCII. Go bytes.EqualFold has only
    // two non-ASCII SimpleFold classes that can equal an ASCII rune: long-s
    // with S/s and Kelvin sign with K/k.
    incoming.chars().zip(tag.bytes()).all(|(left, right)| {
        let left = match left {
            'a'..='z' => left.to_ascii_uppercase(),
            '\u{017f}' => 'S',
            '\u{212a}' => 'K',
            other => other,
        };
        left == (right as char).to_ascii_uppercase()
    }) && incoming.chars().count() == tag.len()
}

impl<'de> Deserialize<'de> for JsonFieldType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonFieldTypeVisitor;

        impl<'de> Visitor<'de> for JsonFieldTypeVisitor {
            type Value = JsonFieldType;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Go jsonFieldType object or null")
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(JsonFieldType::default())
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(JsonFieldType::default())
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut value = JsonFieldType::default();
                while let Some(key) = map.next_key::<String>()? {
                    if go_json_ascii_tag_matches(&key, "Tp") {
                        if let Some(next) = map.next_value::<Option<u8>>()? {
                            value.Tp = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Flag") {
                        if let Some(next) = map.next_value::<Option<u64>>()? {
                            value.Flag = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Flen") {
                        if let Some(next) = map.next_value::<Option<i64>>()? {
                            value.Flen = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Decimal") {
                        if let Some(next) = map.next_value::<Option<i64>>()? {
                            value.Decimal = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Charset") {
                        if let Some(next) = map.next_value::<Option<String>>()? {
                            value.Charset = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Collate") {
                        if let Some(next) = map.next_value::<Option<String>>()? {
                            value.Collate = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Elems") {
                        value.Elems = map.next_value()?;
                    } else if go_json_ascii_tag_matches(&key, "ElemsIsBinaryLit") {
                        value.ElemsIsBinaryLit = map.next_value()?;
                    } else if go_json_ascii_tag_matches(&key, "Array") {
                        if let Some(next) = map.next_value::<Option<bool>>()? {
                            value.Array = next;
                        }
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }

        deserializer.deserialize_any(JsonFieldTypeVisitor)
    }
}

// Go marshals `types.FieldType` through its own MarshalJSON/UnmarshalJSON,
// which use the `jsonFieldType` shape; serde does the same so a FieldType
// nested in any meta-model struct round-trips byte-identically.
impl Serialize for FieldType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        JsonFieldType::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FieldType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        JsonFieldType::deserialize(deserializer).map(Into::into)
    }
}

impl From<&FieldType> for JsonFieldType {
    fn from(field: &FieldType) -> Self {
        Self {
            Tp: field.array_element_code().mysql_type(),
            Flag: field.flags,
            Flen: field.flen,
            Decimal: field.decimal,
            Charset: field.charset_name.clone(),
            Collate: field.collation_name.clone(),
            Elems: field.elems_present.then(|| field.elems.clone()),
            ElemsIsBinaryLit: field
                .elems_is_binary_literal_present
                .then(|| field.elems_is_binary_literal.clone()),
            Array: field.array,
        }
    }
}

impl From<JsonFieldType> for FieldType {
    fn from(field: JsonFieldType) -> Self {
        let mut result = Self::parser(FieldTypeCode::from_mysql_type(field.Tp));
        result.flags = field.Flag;
        result.flen = field.Flen;
        result.decimal = field.Decimal;
        result.charset_name = field.Charset;
        result.collation_name = field.Collate;
        result.collation =
            Collation::from_name(&result.collation_name).unwrap_or(Collation::Binary);
        result.elems_present = field.Elems.is_some();
        result.elems = field.Elems.unwrap_or_default();
        result.elems_is_binary_literal_present = field.ElemsIsBinaryLit.is_some();
        result.elems_is_binary_literal = field.ElemsIsBinaryLit.unwrap_or_default();
        result.array = field.Array;
        result
    }
}
