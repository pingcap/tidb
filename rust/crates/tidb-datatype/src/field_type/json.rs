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

use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{FieldType, FieldTypeCode};
use crate::go_runtime::{go_64_slice_decode_capacity, GoSharedSlice, GoSliceElementLayout};
use crate::GoString;

impl FieldType {
    /// Serializes the source JSON field names and values.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&JsonFieldType::from(self))
    }

    /// Deserializes the source JSON representation.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<JsonFieldType>(data).map(Into::into)
    }

    /// Mirrors `json.Unmarshal` into an existing `*FieldType`. The source
    /// custom unmarshaller decodes a fresh temporary and replaces the receiver
    /// only after the complete object succeeds.
    pub fn unmarshal_json(&mut self, data: &[u8]) -> Result<(), serde_json::Error> {
        let decoded = Self::from_json(data)?;
        *self = decoded;
        Ok(())
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
    Charset: GoString,
    #[serde(default)]
    Collate: GoString,
    #[serde(default)]
    Elems: GoSharedSlice<GoString>,
    #[serde(default)]
    ElemsIsBinaryLit: GoSharedSlice<bool>,
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

struct SharedSliceSeed<'a, T> {
    destination: &'a mut GoSharedSlice<T>,
    element_size: usize,
    layout: GoSliceElementLayout,
}

impl<'de, T> DeserializeSeed<'de> for SharedSliceSeed<'_, T>
where
    T: Deserialize<'de> + Clone + Default,
{
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        // Go decodes JSON null into an existing non-pointer scalar as a
        // no-op. A fresh array slot is its zero value; a duplicate array key
        // can therefore retain a prior slot value through a null element.
        let values = Option::<Vec<Option<T>>>::deserialize(deserializer)?;
        let Some(values) = values else {
            *self.destination = GoSharedSlice::default();
            return Ok(());
        };
        let decoded_len = values.len();
        for (index, value) in values.into_iter().enumerate() {
            let capacity = go_64_slice_decode_capacity(
                self.destination.capacity(),
                index + 1,
                self.element_size,
                self.layout,
            );
            self.destination.prepare_decode_slot(index, capacity);
            if let Some(value) = value {
                self.destination.set_decode_slot(index, value);
            }
        }
        self.destination.finish_decode(decoded_len);
        Ok(())
    }
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
                        if let Some(next) = map.next_value::<Option<GoString>>()? {
                            value.Charset = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Collate") {
                        if let Some(next) = map.next_value::<Option<GoString>>()? {
                            value.Collate = next;
                        }
                    } else if go_json_ascii_tag_matches(&key, "Elems") {
                        map.next_value_seed(SharedSliceSeed {
                            destination: &mut value.Elems,
                            element_size: 16,
                            layout: GoSliceElementLayout::PointerBearing,
                        })?;
                    } else if go_json_ascii_tag_matches(&key, "ElemsIsBinaryLit") {
                        map.next_value_seed(SharedSliceSeed {
                            destination: &mut value.ElemsIsBinaryLit,
                            element_size: 1,
                            layout: GoSliceElementLayout::NoPointers,
                        })?;
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
// which use the `jsonFieldType` shape. This serde surface preserves that
// field shape, ordered overwrite behavior, slice headers, and Go marshal
// escapes. The shared tolerant parser for raw invalid UTF-8 and lone UTF-16
// surrogates remains a separate package-wide decode seam.
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
            Charset: GoString::from(&field.charset_name),
            Collate: GoString::from(&field.collation_name),
            Elems: field.elems.clone(),
            ElemsIsBinaryLit: field.elems_is_binary_literal.clone(),
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
        result.charset_name = field.Charset.to_utf8_lossy_go();
        result.collation_name = field.Collate.to_utf8_lossy_go();
        result.collation = crate::get_collator_with_mode(true, &result.collation_name)
            .new_collation()
            .expect("new-collation lookup always returns a concrete collation");
        result.elems = field.Elems;
        result.elems_is_binary_literal = field.ElemsIsBinaryLit;
        result.array = field.Array;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elems_preserve_source_slice_state_and_aliasing() {
        let mut field_type = FieldType::parser(FieldTypeCode::Enum);
        assert!(!field_type.elems().is_allocated());

        field_type.set_elems(Some(Vec::<GoString>::new()));
        assert!(field_type.elems().is_allocated());
        assert!(field_type.elems().is_empty());

        field_type.set_elems(Some(vec![GoString::from("a")]));
        let alias = field_type.elems();
        alias.set(0, GoString::from("b"));
        assert_eq!(field_type.elems_snapshot(), ["b"]);

        field_type.set_elems(None::<Vec<GoString>>);
        assert!(!field_type.elems().is_allocated());
        assert!(field_type.elems().is_empty());
    }

    #[test]
    fn slice_json_preserves_go_growth_duplicate_and_null_element_rules() {
        for (length, capacity) in [(1, 1), (2, 2), (3, 4), (5, 8)] {
            let elements = (0..length)
                .map(|index| format!(r#""e{index}""#))
                .collect::<Vec<_>>()
                .join(",");
            let decoded =
                FieldType::from_json(format!(r#"{{"Elems":[{elements}]}}"#).as_bytes()).unwrap();
            assert_eq!(decoded.elems.len(), length);
            assert_eq!(decoded.elems.capacity(), capacity);
        }

        for (length, capacity) in [(1, 8), (8, 8), (9, 16)] {
            let flags = std::iter::repeat_n("false", length)
                .collect::<Vec<_>>()
                .join(",");
            let decoded =
                FieldType::from_json(format!(r#"{{"ElemsIsBinaryLit":[{flags}]}}"#).as_bytes())
                    .unwrap();
            assert_eq!(decoded.elems_is_binary_literal.len(), length);
            assert_eq!(decoded.elems_is_binary_literal.capacity(), capacity);
        }

        let duplicate = FieldType::from_json(
            br#"{"Elems":["old","second","third"],"Elems":[null,"new"],"ElemsIsBinaryLit":[true],"ElemsIsBinaryLit":[null]}"#,
        )
        .unwrap();
        assert_eq!(
            duplicate.elems.snapshot(),
            vec![GoString::from("old"), GoString::from("new")]
        );
        assert_eq!(duplicate.elems.capacity(), 4);
        assert_eq!(duplicate.elems_is_binary_literal.snapshot(), [true]);
        assert_eq!(duplicate.elems_is_binary_literal.capacity(), 8);

        let truncated = FieldType::from_json(
            br#"{"Elems":["kept","dropped"],"Elems":[null],"ElemsIsBinaryLit":[true,false],"ElemsIsBinaryLit":[null]}"#,
        )
        .unwrap();
        assert_eq!(truncated.elems.snapshot(), ["kept"]);
        assert_eq!(truncated.elems.len(), 1);
        assert_eq!(truncated.elems.capacity(), 2);
        assert_eq!(truncated.elems_is_binary_literal.snapshot(), [true]);
        assert_eq!(truncated.elems_is_binary_literal.len(), 1);
        assert_eq!(truncated.elems_is_binary_literal.capacity(), 8);

        let fresh_nulls =
            FieldType::from_json(br#"{"Elems":[null],"ElemsIsBinaryLit":[null]}"#).unwrap();
        assert_eq!(fresh_nulls.elems.snapshot(), vec![GoString::default()]);
        assert_eq!(fresh_nulls.elems_is_binary_literal.snapshot(), [false]);

        let nil = FieldType::parser(FieldTypeCode::Enum);
        let empty = FieldType::parser(FieldTypeCode::Enum).with_elems(Vec::<String>::new());
        assert!(std::str::from_utf8(&nil.to_json().unwrap())
            .unwrap()
            .contains(r#""Elems":null"#));
        assert!(std::str::from_utf8(&empty.to_json().unwrap())
            .unwrap()
            .contains(r#""Elems":[]"#));
    }

    #[test]
    fn clone_deep_copy_and_binary_marker_mutations_follow_go_headers() {
        assert!(std::panic::catch_unwind(|| FieldType::clone_pointer(None::<&FieldType>)).is_err());
        assert!(FieldType::deep_copy_pointer(None::<&FieldType>).is_none());
        assert_eq!(FieldType::memory_usage_pointer(None), 0);
        FieldType::clean_elem_binary_literals_pointer(None);

        let mut source = FieldType::parser(FieldTypeCode::Enum);
        source.set_elems(GoSharedSlice::from_vec_with_capacity(
            vec![GoString::from("a"), GoString::from("b")],
            4,
        ));
        source.elems_is_binary_literal =
            GoSharedSlice::from_vec_with_capacity(vec![true, false], 8);
        assert_eq!(FieldType::memory_usage_pointer(Some(&source)), 194);
        let source_elements = source.elems();
        let mut cloned = source.clone();
        assert!(source_elements.backing_ptr_eq(&cloned.elems()));
        assert!(source
            .elems_is_binary_literal
            .backing_ptr_eq(&cloned.elems_is_binary_literal));
        cloned.set_elem_with_binary_literal(1, "shared", true);
        assert_eq!(source.elem(1), "shared");
        assert!(source.elem_is_binary_literal(1));

        let clone_pointer = FieldType::clone_pointer(Some(&source));
        assert!(!std::ptr::eq(&source, clone_pointer.as_ref()));
        assert!(source.elems().backing_ptr_eq(&clone_pointer.elems()));

        let mut deep = source.deep_copy_like_go();
        assert_eq!(deep.elems.capacity(), deep.elems.len());
        assert_eq!(
            deep.elems_is_binary_literal.capacity(),
            deep.elems_is_binary_literal.len()
        );
        assert!(!source.elems().backing_ptr_eq(&deep.elems()));
        assert!(!source
            .elems_is_binary_literal
            .backing_ptr_eq(&deep.elems_is_binary_literal));
        deep.set_elem(0, "independent");
        assert_eq!(source.elem(0), "a");

        let mut cleaned = source.clone();
        cleaned.clean_elem_binary_literals();
        assert!(!cleaned.elems_is_binary_literal.is_allocated());
        assert!(source.elem_is_binary_literal(0));

        let allocated_empty =
            FieldType::from_json(br#"{"Elems":[],"ElemsIsBinaryLit":[]}"#).unwrap();
        let empty_clone = allocated_empty.clone();
        assert!(allocated_empty.elems.backing_ptr_eq(&empty_clone.elems));
        assert!(allocated_empty
            .elems_is_binary_literal
            .backing_ptr_eq(&empty_clone.elems_is_binary_literal));
        let deep_empty = allocated_empty.deep_copy_like_go();
        assert!(!deep_empty.elems.is_allocated());
        assert!(!deep_empty.elems_is_binary_literal.is_allocated());

        let nil = FieldType::parser(FieldTypeCode::Enum);
        let nil_clone = nil.clone();
        assert!(!nil_clone.elems.is_allocated());
        assert!(!nil_clone.elems_is_binary_literal.is_allocated());

        let old_elements = source.elems();
        let old_flags = source.elems_is_binary_literal.clone();
        let mut replaced = source.clone();
        replaced.set_elems(vec![GoString::from("replacement")]);
        assert!(!old_elements.backing_ptr_eq(&replaced.elems()));
        assert_eq!(source.elems.snapshot(), ["a", "shared"]);
        assert!(old_flags.backing_ptr_eq(&replaced.elems_is_binary_literal));
    }

    #[test]
    fn binary_marker_allocation_and_panics_happen_after_element_mutation() {
        let base = FieldType::parser(FieldTypeCode::Enum).with_elems(["a", "b"]);
        let mut clone = base.clone();
        clone.set_elem_with_binary_literal(1, "shared", true);
        assert_eq!(base.elem(1), "shared");
        assert!(!base.elems_is_binary_literal.is_allocated());
        assert!(clone.elem_is_binary_literal(1));

        let mut false_marker = FieldType::parser(FieldTypeCode::Enum).with_elems(["a"]);
        false_marker.set_elem_with_binary_literal(0, "plain", false);
        assert_eq!(false_marker.elem(0), "plain");
        assert!(!false_marker.elems_is_binary_literal.is_allocated());

        let mut allocated_empty =
            FieldType::from_json(br#"{"Elems":["a"],"ElemsIsBinaryLit":[]}"#).unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            allocated_empty.set_elem_with_binary_literal(0, "changed", true);
        }))
        .is_err());
        assert_eq!(allocated_empty.elem(0), "changed");
        assert!(allocated_empty.elems_is_binary_literal.is_allocated());
        assert!(allocated_empty.elems_is_binary_literal.is_empty());

        let mut short =
            FieldType::from_json(br#"{"Elems":["a","b"],"ElemsIsBinaryLit":[false]}"#).unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            short.set_elem_with_binary_literal(1, "changed", true);
        }))
        .is_err());
        assert_eq!(short.elem(1), "changed");
        assert_eq!(short.elems_is_binary_literal.snapshot(), [false]);
    }

    #[test]
    fn failed_unmarshal_keeps_receiver_and_shared_alias_unchanged() {
        let mut destination = FieldType::parser(FieldTypeCode::Enum).with_elems(["old"]);
        let alias = destination.elems();
        assert!(destination
            .unmarshal_json(br#"{"Elems":["new",7],"Flen":8}"#)
            .is_err());
        assert!(alias.backing_ptr_eq(&destination.elems()));
        assert_eq!(alias.snapshot(), ["old"]);
        assert_eq!(destination.flen(), super::super::UNSPECIFIED_LENGTH);
    }

    #[test]
    fn binary_elements_preserve_bytes_clone_memory_restore_and_json_replacement() {
        let invalid = GoString::from_bytes(vec![0xe2, 0x82]);
        let mut field_type = FieldType::parser(FieldTypeCode::Enum);
        field_type.set_elems(GoSharedSlice::from_vec(vec![invalid.clone()]));
        field_type.set_elem_with_binary_literal(0, invalid.clone(), true);

        assert_eq!(field_type.elem(0).as_bytes(), [0xe2, 0x82]);
        assert!(field_type.elem_is_binary_literal(0));
        assert_eq!(field_type.memory_usage(), 139);
        assert_eq!(field_type.restore_bytes(), b"ENUM('\xe2\x82')");
        assert_eq!(field_type.compact_str(false), "enum('��')");

        let clone = field_type.clone();
        assert!(field_type.elems().backing_ptr_eq(&clone.elems()));
        assert!(field_type.elem(0).backing_ptr_eq(&clone.elem(0)));
        let deep = field_type.deep_copy_like_go();
        assert!(!field_type.elems().backing_ptr_eq(&deep.elems()));
        assert!(field_type.elem(0).backing_ptr_eq(&deep.elem(0)));

        let json = field_type.to_json().unwrap();
        assert!(std::str::from_utf8(&json)
            .unwrap()
            .contains(r#""Elems":["\ufffd\ufffd"]"#));
        let decoded = FieldType::from_json(&json).unwrap();
        assert_eq!(decoded.elem(0).as_bytes(), "��".as_bytes());
        assert_ne!(decoded.elem(0).as_bytes(), invalid.as_bytes());
    }

    #[test]
    fn marshal_uses_go_string_escaping_and_exact_collation_spelling() {
        let field_type = FieldType::parser(FieldTypeCode::VarString)
            .with_charset_name("<&>\u{2028}\u{2029}")
            .with_collation_name("BINARY")
            .with_elems(["<&>\u{2028}\u{2029}"]);
        assert_eq!(
            field_type.to_json().unwrap(),
            br#"{"Tp":253,"Flag":0,"Flen":-1,"Decimal":-1,"Charset":"\u003c\u0026\u003e\u2028\u2029","Collate":"BINARY","Elems":["\u003c\u0026\u003e\u2028\u2029"],"ElemsIsBinaryLit":null,"Array":false}"#
        );
        assert!(!field_type.is_binary_string());
        assert!(field_type.is_character_string());
        assert_eq!(
            field_type.runtime_collator_with_mode(true),
            crate::Collator::New(crate::Collation::Utf8Mb4Bin)
        );

        let decoded = FieldType::from_json(br#"{"Tp":253,"Collate":"BINARY"}"#).unwrap();
        assert_eq!(decoded.collation_name(), "BINARY");
        assert!(!decoded.is_binary_string());
        assert!(decoded.need_restored_data());
        assert_eq!(
            decoded.runtime_collator_with_mode(true),
            crate::Collator::New(crate::Collation::Utf8Mb4Bin)
        );
    }
}
