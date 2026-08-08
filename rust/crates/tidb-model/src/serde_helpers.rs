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
use serde_json::value::RawValue;

/// An owned Go slice of non-pointer values with nil/allocation identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoValueSlice<T>(Option<Vec<T>>);

impl<T> GoValueSlice<T> {
    /// Constructs an allocated slice, including the allocated-empty state.
    pub fn allocated(values: Vec<T>) -> Self {
        Self(Some(values))
    }

    /// Returns whether the Go slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.0.is_some()
    }

    /// Returns the source `len`, for which nil and empty are both zero.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, Vec::len)
    }

    /// Returns whether the source length is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears elements without changing nil versus allocated-empty identity.
    pub fn clear(&mut self) {
        if let Some(values) = &mut self.0 {
            values.clear();
        }
    }

    /// Iterates the slice values.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.as_deref().unwrap_or_default().iter()
    }

    /// Iterates mutable values, allocating an empty slice before mutation.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.0.get_or_insert_with(Vec::new).iter_mut()
    }

    pub(crate) fn raw_mut(&mut self) -> &mut Option<Vec<T>> {
        &mut self.0
    }
}

impl<T: PartialEq> GoValueSlice<T> {
    /// Reports whether the source slice contains `needle`.
    #[must_use]
    pub fn contains(&self, needle: &T) -> bool {
        self.iter().any(|value| value == needle)
    }
}

impl<T> From<Vec<T>> for GoValueSlice<T> {
    fn from(values: Vec<T>) -> Self {
        Self::allocated(values)
    }
}

impl<'a, T> IntoIterator for &'a GoValueSlice<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut GoValueSlice<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> std::ops::Index<usize> for GoValueSlice<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0.as_ref().expect("index of nil Go slice")[index]
    }
}

impl<T> std::ops::IndexMut<usize> for GoValueSlice<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0.as_mut().expect("index of nil Go slice")[index]
    }
}

impl<T: Serialize> Serialize for GoValueSlice<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// An owned Go slice of pointers with nil/allocation and null-element identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoPointerSlice<T>(Option<Vec<Option<T>>>);

impl<T> GoPointerSlice<T> {
    /// Constructs an allocated slice of non-null pointers.
    pub fn from_values(values: Vec<T>) -> Self {
        Self(Some(values.into_iter().map(Some).collect()))
    }

    /// Constructs an allocated slice that may contain null pointers.
    pub fn from_nullable(values: Vec<Option<T>>) -> Self {
        Self(Some(values))
    }

    /// Returns whether the Go slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.0.is_some()
    }

    /// Returns the source `len`, for which nil and empty are both zero.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, Vec::len)
    }

    /// Returns whether the source length is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears elements without changing nil versus allocated-empty identity.
    pub fn clear(&mut self) {
        if let Some(values) = &mut self.0 {
            values.clear();
        }
    }

    /// Appends a non-null pointer, allocating the slice when nil.
    pub fn push(&mut self, value: T) {
        self.0.get_or_insert_with(Vec::new).push(Some(value));
    }

    /// Iterates non-null pointees. Encountering a null pointer panics at the
    /// same dereference boundary as the corresponding Go algorithm.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.0
            .iter()
            .flatten()
            .map(|value| value.as_ref().expect("nil pointer in Go slice"))
    }

    /// Iterates mutable non-null pointees, preserving pointer positions.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.0
            .get_or_insert_with(Vec::new)
            .iter_mut()
            .map(|value| value.as_mut().expect("nil pointer in Go slice"))
    }

    /// Exposes nullable elements to tests and codecs that own the pointer
    /// boundary rather than dereferencing it.
    pub fn nullable(&self) -> Option<&[Option<T>]> {
        self.0.as_deref()
    }

    pub(crate) fn raw_mut(&mut self) -> &mut Option<Vec<Option<T>>> {
        &mut self.0
    }
}

impl<T> From<Vec<T>> for GoPointerSlice<T> {
    fn from(values: Vec<T>) -> Self {
        Self::from_values(values)
    }
}

/// Iterator that dereferences Go pointer-slice elements at the use site.
pub struct GoPointerIter<'a, T>(std::slice::Iter<'a, Option<T>>);

impl<'a, T> Iterator for GoPointerIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|value| value.as_ref().expect("nil pointer in Go slice"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<T> ExactSizeIterator for GoPointerIter<'_, T> {}

/// Mutable iterator that dereferences Go pointer-slice elements at the use
/// site.
pub struct GoPointerIterMut<'a, T>(std::slice::IterMut<'a, Option<T>>);

impl<'a, T> Iterator for GoPointerIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|value| value.as_mut().expect("nil pointer in Go slice"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<T> ExactSizeIterator for GoPointerIterMut<'_, T> {}

impl<'a, T> IntoIterator for &'a GoPointerSlice<T> {
    type Item = &'a T;
    type IntoIter = GoPointerIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GoPointerIter(self.0.as_deref().unwrap_or_default().iter())
    }
}

impl<'a, T> IntoIterator for &'a mut GoPointerSlice<T> {
    type Item = &'a mut T;
    type IntoIter = GoPointerIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GoPointerIterMut(self.0.get_or_insert_with(Vec::new).iter_mut())
    }
}

impl<T> std::ops::Index<usize> for GoPointerSlice<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.as_ref().expect("index of nil Go pointer slice")[index]
            .as_ref()
            .expect("nil pointer in Go slice")
    }
}

impl<T> std::ops::IndexMut<usize> for GoPointerSlice<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.as_mut().expect("index of nil Go pointer slice")[index]
            .as_mut()
            .expect("nil pointer in Go slice")
    }
}

impl<T: Serialize> Serialize for GoPointerSlice<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

struct RawObjectMembers<'de>(Vec<(String, &'de RawValue)>);

struct RawArrayMembers<'de>(Vec<&'de RawValue>);

impl<'de> Deserialize<'de> for RawObjectMembers<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawObjectVisitor;

        impl<'de> Visitor<'de> for RawObjectVisitor {
            type Value = RawObjectMembers<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut members = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<&'de RawValue>()?;
                    members.push((key, value));
                }
                Ok(RawObjectMembers(members))
            }
        }

        deserializer.deserialize_map(RawObjectVisitor)
    }
}

impl<'de> Deserialize<'de> for RawArrayMembers<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawArrayVisitor;

        impl<'de> Visitor<'de> for RawArrayVisitor {
            type Value = RawArrayMembers<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut elements = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                while let Some(value) = sequence.next_element::<&'de RawValue>()? {
                    elements.push(value);
                }
                Ok(RawArrayMembers(elements))
            }
        }

        deserializer.deserialize_seq(RawArrayVisitor)
    }
}

struct IndependentRawMapAccess<'de, E> {
    members: std::vec::IntoIter<(String, &'de RawValue)>,
    pending_value: Option<&'de RawValue>,
    marker: PhantomData<E>,
}

impl<'de, E> MapAccess<'de> for IndependentRawMapAccess<'de, E>
where
    E: serde::de::Error,
{
    type Error = E;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((key, value)) = self.members.next() else {
            return Ok(None);
        };
        self.pending_value = Some(value);
        seed.deserialize(serde::de::value::StringDeserializer::<E>::new(key))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let raw = self
            .pending_value
            .take()
            .ok_or_else(|| E::custom("JSON object value requested before its key"))?;
        let mut deserializer = serde_json::Deserializer::from_str(raw.get());
        seed.deserialize(&mut deserializer)
            .map_err(|error| E::custom(error.to_string()))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.members.len())
    }
}

/// Buffers one syntactically valid object's members as borrowed raw JSON.
///
/// Each value is then decoded through an independent deserializer. A semantic
/// error in one member therefore cannot poison the parent stream, allowing the
/// caller to retain Go's first error and still process every later member.
pub(crate) fn deserialize_go_object<'de, D, V>(
    deserializer: D,
    visitor: V,
) -> Result<V::Value, D::Error>
where
    D: Deserializer<'de>,
    V: Visitor<'de>,
{
    let members = RawObjectMembers::deserialize(deserializer)?;
    visitor.visit_map(IndependentRawMapAccess {
        members: members.0.into_iter(),
        pending_value: None,
        marker: PhantomData,
    })
}

/// Implements the receiver-mutating object loop shared by persisted model
/// structs. Values are buffered as raw members first, so duplicate keys retain
/// source order and a recoverable value error does not prevent later fields
/// from being applied, matching `encoding/json`.
macro_rules! impl_go_json_merge_object {
    ($type:ty, $destination:ident, $map:ident, $key:ident, { $($body:tt)* }) => {
        impl $crate::serde_helpers::GoJsonMerge for $type {
            fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct MergeVisitor<'a>(&'a mut $type);

                impl<'de> serde::de::Visitor<'de> for MergeVisitor<'_> {
                    type Value = ();

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("a JSON object")
                    }

                    fn visit_map<A>(self, mut $map: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::MapAccess<'de>,
                    {
                        let $destination = self.0;
                        let mut first_error = None;
                        while let Some($key) = serde::de::MapAccess::next_key::<String>(&mut $map)? {
                            let field_result = (|| -> Result<(), A::Error> {
                                $($body)*
                                Ok(())
                            })();
                            if let Err(error) = field_result {
                                if $crate::serde_helpers::is_fatal_json_error(&error) {
                                    return Err(error);
                                }
                                first_error.get_or_insert(error);
                            }
                        }
                        if let Some(error) = first_error {
                            return Err(error);
                        }
                        Ok(())
                    }
                }

                $crate::serde_helpers::deserialize_go_object(deserializer, MergeVisitor(self))
            }
        }
    };
}

pub(crate) use impl_go_json_merge_object;

/// Implements fresh-value `Deserialize` through [`GoJsonMerge`]. A JSON null
/// leaves the Go zero value unchanged; every non-null value is delegated to
/// the ordered object decoder.
macro_rules! impl_go_json_deserialize {
    ($type:ty) => {
        impl<'de> serde::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct ObjectOrNullVisitor;

                impl<'de> serde::de::Visitor<'de> for ObjectOrNullVisitor {
                    type Value = $type;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("null or a JSON object")
                    }

                    fn visit_none<E>(self) -> Result<Self::Value, E> {
                        Ok(<$type>::default())
                    }

                    fn visit_unit<E>(self) -> Result<Self::Value, E> {
                        Ok(<$type>::default())
                    }

                    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
                    where
                        D: serde::Deserializer<'de>,
                    {
                        let mut destination = <$type>::default();
                        $crate::serde_helpers::GoJsonMerge::go_json_merge(
                            &mut destination,
                            deserializer,
                        )?;
                        Ok(destination)
                    }
                }

                deserializer.deserialize_option(ObjectOrNullVisitor)
            }
        }
    };
}

pub(crate) use impl_go_json_deserialize;

/// Reports whether an incoming JSON object key matches a Go struct-field tag.
///
/// `encoding/json` prefers exact matches and then accepts Unicode SimpleFold
/// field names. The model's persisted tags are ASCII and unique under folding.
pub(crate) fn go_json_field_matches(incoming: &str, tag: &str) -> bool {
    if incoming == tag {
        return true;
    }
    // Every persisted model tag is ASCII. In the Unicode SimpleFold classes
    // used by bytes.EqualFold, the only non-ASCII runes equivalent to ASCII
    // are long-s and Kelvin sign. Handling those classes plus ASCII folding
    // is therefore the complete Go rule for this tag universe.
    incoming.chars().zip(tag.bytes()).all(|(left, right)| {
        let left = match left {
            'a'..='z' => left.to_ascii_uppercase(),
            '\u{017f}' => 'S',
            '\u{212a}' => 'K',
            other => other,
        };
        let right = (right as char).to_ascii_uppercase();
        left == right
    }) && incoming.chars().count() == tag.len()
}

const FATAL_JSON_ERROR_PREFIX: &str = "__go_custom_unmarshal_fatal__: ";

/// Marks errors returned by a Go `UnmarshalJSON` equivalent as fatal.
pub(crate) struct FatalSeed<S>(pub(crate) S);

impl<'de, S> DeserializeSeed<'de> for FatalSeed<S>
where
    S: DeserializeSeed<'de>,
{
    type Value = S::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.0
            .deserialize(deserializer)
            .map_err(|error| serde::de::Error::custom(format!("{FATAL_JSON_ERROR_PREFIX}{error}")))
    }
}

/// Deserializes a value while marking its error as a fatal custom-unmarshal
/// result.
pub(crate) struct FatalValueSeed<T>(PhantomData<T>);

impl<T> FatalValueSeed<T> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'de, T> DeserializeSeed<'de> for FatalValueSeed<T>
where
    T: Deserialize<'de>,
{
    type Value = T;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        FatalSeed(PhantomData::<T>).deserialize(deserializer)
    }
}

/// Detects a fatal custom-unmarshal error after it crosses nested raw-member
/// decoder boundaries.
pub(crate) fn is_fatal_json_error(error: &impl std::fmt::Display) -> bool {
    error.to_string().contains(FATAL_JSON_ERROR_PREFIX)
}

/// Removes the internal fatal-propagation marker at a public JSON boundary.
pub(crate) fn normalize_fatal_json_error(error: serde_json::Error) -> serde_json::Error {
    let message = error.to_string();
    if !message.contains(FATAL_JSON_ERROR_PREFIX) {
        return error;
    }
    <serde_json::Error as serde::de::Error>::custom(message.replace(FATAL_JSON_ERROR_PREFIX, ""))
}

/// Receiver-mutating object decoder used by Go `json.Unmarshal` ports.
pub(crate) trait GoJsonMerge {
    /// Decodes one non-null JSON object into the existing receiver.
    fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>;
}

/// Reproduces `encoding/json`'s receiver-mutating decode for `ast.CIStr`.
/// The owning AST type deliberately accepts an additional string shorthand;
/// persisted model fields do not: Go's source type is the two-field `O`/`L`
/// object, with ordinary struct duplicate, fold, null, and partial-error rules.
impl GoJsonMerge for tidb_ast::CiString {
    fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CiStringVisitor<'a>(&'a mut tidb_ast::CiString);

        impl<'de> Visitor<'de> for CiStringVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an ast.CIStr JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut original = self.0.original().to_owned();
                let mut lowercase = self.0.lowercase().to_owned();
                let mut first_error = None;
                while let Some(key) = map.next_key::<String>()? {
                    let result = if go_json_field_matches(&key, "O") {
                        map.next_value_seed(NullNoopSeed(&mut original))
                    } else if go_json_field_matches(&key, "L") {
                        map.next_value_seed(NullNoopSeed(&mut lowercase))
                    } else {
                        ignore_unknown(&mut map)
                    };
                    if let Err(error) = result {
                        first_error.get_or_insert(error);
                    }
                }

                *self.0 = serde_json::from_value(serde_json::json!({
                    "O": original,
                    "L": lowercase,
                }))
                .expect("two strings always form a valid ast.CIStr");
                if let Some(error) = first_error {
                    return Err(error);
                }
                Ok(())
            }
        }

        deserialize_go_object(deserializer, CiStringVisitor(self))
    }
}

impl_go_json_merge_object!(tidb_parser::auth::UserIdentity, destination, map, key, {
    if go_json_field_matches(&key, "Username") {
        map.next_value_seed(NullNoopSeed(&mut destination.username))?;
    } else if go_json_field_matches(&key, "Hostname") {
        map.next_value_seed(NullNoopSeed(&mut destination.hostname))?;
    } else if go_json_field_matches(&key, "CurrentUser") {
        map.next_value_seed(NullNoopSeed(&mut destination.current_user))?;
    } else if go_json_field_matches(&key, "AuthUsername") {
        map.next_value_seed(NullNoopSeed(&mut destination.auth_username))?;
    } else if go_json_field_matches(&key, "AuthHostname") {
        map.next_value_seed(NullNoopSeed(&mut destination.auth_hostname))?;
    } else if go_json_field_matches(&key, "AuthPlugin") {
        map.next_value_seed(NullNoopSeed(&mut destination.auth_plugin))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

/// Merges a non-pointer Go struct field into its existing value. JSON null is
/// a no-op; a non-null object preserves omitted subfields.
pub(crate) struct ValueMergeSeed<'a, T>(pub(crate) &'a mut T);

impl<'de, T> DeserializeSeed<'de> for ValueMergeSeed<'_, T>
where
    T: GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueMergeVisitor<'a, T>(&'a mut T);

        impl<'de, T> Visitor<'de> for ValueMergeVisitor<'_, T>
        where
            T: GoJsonMerge,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a JSON object")
            }

            fn visit_none<E>(self) -> Result<(), E> {
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<(), E> {
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<(), D::Error>
            where
                D: Deserializer<'de>,
            {
                self.0.go_json_merge(deserializer)
            }
        }

        deserializer.deserialize_option(ValueMergeVisitor(self.0))
    }
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

/// Deserializes a slice-like field, clearing it on JSON null.
pub(crate) struct NullDefaultSeed<'a, T>(pub(crate) &'a mut T);

impl<'de, T> DeserializeSeed<'de> for NullDefaultSeed<'_, T>
where
    T: Default + Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NullDefaultVisitor<'a, T>(&'a mut T);

        impl<'de, T> Visitor<'de> for NullDefaultVisitor<'_, T>
        where
            T: Default + Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a value of the destination field type")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.0 = T::default();
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.0 = T::default();
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

        deserializer.deserialize_option(NullDefaultVisitor(self.0))
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

/// Deserializes a boxed pointer field into its existing allocation.
pub(crate) struct OptionBoxMergeSeed<'a, T>(pub(crate) &'a mut Option<Box<T>>);

impl<'de, T> DeserializeSeed<'de> for OptionBoxMergeSeed<'_, T>
where
    T: Default + GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionBoxMergeVisitor<'a, T>(&'a mut Option<Box<T>>);

        impl<'de, T> Visitor<'de> for OptionBoxMergeVisitor<'_, T>
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
                    .get_or_insert_with(|| Box::new(T::default()))
                    .go_json_merge(deserializer)
            }
        }

        deserializer.deserialize_option(OptionBoxMergeVisitor(self.0))
    }
}

/// Deserializes a scalar pointer into its existing allocation. A non-null
/// invalid value allocates a zero pointee before returning the recoverable
/// type error, matching encoding/json's pointer walk.
pub(crate) struct OptionScalarSeed<'a, T>(pub(crate) &'a mut Option<T>);

impl<'de, T> DeserializeSeed<'de> for OptionScalarSeed<'_, T>
where
    T: Default + Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionScalarVisitor<'a, T>(&'a mut Option<T>);

        impl<'de, T> Visitor<'de> for OptionScalarVisitor<'_, T>
        where
            T: Default + Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a scalar pointer value")
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
                let destination = self.0.get_or_insert_with(T::default);
                let decoded = T::deserialize(deserializer)?;
                *destination = decoded;
                Ok(())
            }
        }

        deserializer.deserialize_option(OptionScalarVisitor(self.0))
    }
}

/// Replaces a Go pointer slice while retaining null elements and continuing
/// after recoverable element errors.
pub(crate) struct OptionPointerSliceSeed<'a, T>(pub(crate) &'a mut Option<Vec<Option<T>>>);

impl<'de, T> DeserializeSeed<'de> for OptionPointerSliceSeed<'_, T>
where
    T: Default + GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PointerSliceVisitor<'a, T>(&'a mut Option<Vec<Option<T>>>);

        impl<'de, T> Visitor<'de> for PointerSliceVisitor<'_, T>
        where
            T: Default + GoJsonMerge,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of nullable JSON objects")
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
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let mut existing = self.0.take().unwrap_or_default().into_iter();
                let mut decoded = Vec::with_capacity(elements.len());
                let mut first_error = None;
                for raw in elements {
                    let previous = existing.next().flatten();
                    if raw.get() == "null" {
                        decoded.push(None);
                        continue;
                    }
                    let mut value = previous.unwrap_or_default();
                    let mut element = serde_json::Deserializer::from_str(raw.get());
                    if let Err(error) = value
                        .go_json_merge(&mut element)
                        .and_then(|()| element.end())
                    {
                        if is_fatal_json_error(&error) {
                            decoded.push(Some(value));
                            decoded.extend(existing);
                            *self.0 = Some(decoded);
                            return Err(serde::de::Error::custom(error));
                        }
                        first_error.get_or_insert_with(|| error.to_string());
                    }
                    decoded.push(Some(value));
                }
                *self.0 = Some(decoded);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(PointerSliceVisitor(self.0))
    }
}

/// Replaces an optional Go slice of non-pointer values. JSON null retains a
/// nil slice, a null array element leaves that element at its zero value, and
/// recoverable element errors do not suppress later elements.
pub(crate) struct OptionValueSliceSeed<'a, T>(pub(crate) &'a mut Option<Vec<T>>);

impl<'de, T> DeserializeSeed<'de> for OptionValueSliceSeed<'_, T>
where
    T: Default + Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueSliceVisitor<'a, T>(&'a mut Option<Vec<T>>);

        impl<'de, T> Visitor<'de> for ValueSliceVisitor<'_, T>
        where
            T: Default + Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of JSON values")
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
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let mut existing = self.0.take().unwrap_or_default().into_iter();
                let mut decoded = Vec::with_capacity(elements.len());
                let mut first_error = None;
                for raw in elements {
                    let mut value = existing.next().unwrap_or_default();
                    if raw.get() != "null" {
                        let mut element = serde_json::Deserializer::from_str(raw.get());
                        match T::deserialize(&mut element).and_then(|value| {
                            element.end()?;
                            Ok(value)
                        }) {
                            Ok(element_value) => value = element_value,
                            Err(error) => {
                                first_error.get_or_insert_with(|| error.to_string());
                            }
                        }
                    }
                    decoded.push(value);
                }
                *self.0 = Some(decoded);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(ValueSliceVisitor(self.0))
    }
}

/// Replaces an optional Go slice of non-pointer structs while decoding each
/// object through [`GoJsonMerge`]. JSON null produces the struct zero value.
pub(crate) struct OptionObjectSliceSeed<'a, T>(pub(crate) &'a mut Option<Vec<T>>);

impl<'de, T> DeserializeSeed<'de> for OptionObjectSliceSeed<'_, T>
where
    T: Default + GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectSliceVisitor<'a, T>(&'a mut Option<Vec<T>>);

        impl<'de, T> Visitor<'de> for ObjectSliceVisitor<'_, T>
        where
            T: Default + GoJsonMerge,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of JSON objects")
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
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let mut existing = self.0.take().unwrap_or_default().into_iter();
                let mut decoded = Vec::with_capacity(elements.len());
                let mut first_error = None;
                for raw in elements {
                    let mut value = existing.next().unwrap_or_default();
                    if raw.get() != "null" {
                        let mut element = serde_json::Deserializer::from_str(raw.get());
                        if let Err(error) = value
                            .go_json_merge(&mut element)
                            .and_then(|()| element.end())
                        {
                            if is_fatal_json_error(&error) {
                                decoded.push(value);
                                *self.0 = Some(decoded);
                                return Err(serde::de::Error::custom(error));
                            }
                            first_error.get_or_insert_with(|| error.to_string());
                        }
                    }
                    decoded.push(value);
                }
                *self.0 = Some(decoded);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(ObjectSliceVisitor(self.0))
    }
}

/// Merges a JSON object into a Go map field and clears the map on JSON null.
pub(crate) struct OptionStringMapMergeSeed<'a, V>(pub(crate) &'a mut Option<BTreeMap<String, V>>);

impl<'de, V> DeserializeSeed<'de> for OptionStringMapMergeSeed<'_, V>
where
    V: Default + Deserialize<'de>,
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
            V: Default + Deserialize<'de>,
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
                deserialize_go_object(deserializer, self)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let destination = self.destination.get_or_insert_with(BTreeMap::new);
                let mut first_error = None;
                while let Some(key) = map.next_key::<String>()? {
                    let mut value = V::default();
                    match map.next_value_seed(NullDefaultSeed(&mut value)) {
                        Ok(()) => {
                            destination.insert(key, value);
                        }
                        Err(error) => {
                            destination.insert(key, value);
                            first_error.get_or_insert(error);
                        }
                    }
                }
                if let Some(error) = first_error {
                    return Err(error);
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

/// Merges a Go map whose value type implements custom `UnmarshalJSON`.
/// Value errors abort immediately, before insertion or later members.
pub(crate) struct OptionStringMapFatalMergeSeed<'a, V>(
    pub(crate) &'a mut Option<BTreeMap<String, V>>,
);

impl<'de, V> DeserializeSeed<'de> for OptionStringMapFatalMergeSeed<'_, V>
where
    V: Default + Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FatalMapVisitor<'a, V> {
            destination: &'a mut Option<BTreeMap<String, V>>,
            marker: PhantomData<V>,
        }

        impl<'de, V> Visitor<'de> for FatalMapVisitor<'_, V>
        where
            V: Default + Deserialize<'de>,
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
                deserialize_go_object(deserializer, self)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let destination = self.destination.get_or_insert_with(BTreeMap::new);
                while let Some(key) = map.next_key::<String>()? {
                    let mut value = V::default();
                    map.next_value_seed(FatalSeed(NullDefaultSeed(&mut value)))?;
                    destination.insert(key, value);
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(FatalMapVisitor {
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
        if !compact.len().is_multiple_of(4) {
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

    fn write_raw_fragment<W: std::io::Write + ?Sized>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> std::io::Result<()> {
        // Go validates and compacts a Marshaler/RawMessage result before it is
        // appended to the parent document. Preserve keys, duplicates, and
        // number lexemes while removing JSON whitespace outside strings.
        let mut in_string = false;
        let mut escaped = false;
        for byte in fragment.bytes() {
            if in_string {
                writer.write_all(&[byte])?;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
            } else if byte == b'"' {
                in_string = true;
                writer.write_all(&[byte])?;
            } else if !matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
                writer.write_all(&[byte])?;
            }
        }
        Ok(())
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
