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

//! Receiver-mutating codecs for Go slices and maps with shared backing.

use std::collections::BTreeMap;

use serde::de::{DeserializeSeed, Visitor};
use serde::{Deserialize, Deserializer};

use crate::go_runtime::{
    go_64_slice_decode_capacity, GoShared, GoSharedSlice, GoSliceElementLayout,
};
use crate::serde_helpers::{
    is_fatal_json_error, GoJsonMerge, RawArrayMembers, RawObjectMembers, SharedStringSliceSeed,
};

/// Decodes a Go slice of scalar values while preserving receiver backing,
/// capacity, hidden initialized slots, null no-op, and recoverable errors.
pub(crate) struct SharedScalarSliceSeed<'a, T> {
    destination: &'a mut GoSharedSlice<T>,
    element_size: usize,
    layout: GoSliceElementLayout,
}

impl<'a, T> SharedScalarSliceSeed<'a, T> {
    pub(crate) fn new(
        destination: &'a mut GoSharedSlice<T>,
        element_size: usize,
        layout: GoSliceElementLayout,
    ) -> Self {
        Self {
            destination,
            element_size,
            layout,
        }
    }
}

impl<'de, T> DeserializeSeed<'de> for SharedScalarSliceSeed<'_, T>
where
    T: Clone + Default + Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarSliceVisitor<'a, T> {
            destination: &'a mut GoSharedSlice<T>,
            element_size: usize,
            layout: GoSliceElementLayout,
        }

        impl<'de, T> Visitor<'de> for ScalarSliceVisitor<'_, T>
        where
            T: Clone + Default + Deserialize<'de>,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of scalar values")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.destination = GoSharedSlice::default();
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.destination = GoSharedSlice::default();
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let decoded_len = elements.len();
                let mut first_error = None;
                for (index, raw) in elements.into_iter().enumerate() {
                    let capacity = go_64_slice_decode_capacity(
                        self.destination.capacity(),
                        index + 1,
                        self.element_size,
                        self.layout,
                    );
                    self.destination.prepare_decode_slot(index, capacity);
                    if raw.get() == "null" {
                        continue;
                    }
                    let mut element = serde_json::Deserializer::from_str(raw.get());
                    match T::deserialize(&mut element).and_then(|value| {
                        element.end()?;
                        Ok(value)
                    }) {
                        Ok(value) => self.destination.set_decode_slot(index, value),
                        Err(error) => {
                            first_error.get_or_insert_with(|| error.to_string());
                        }
                    }
                }
                self.destination.finish_decode(decoded_len);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(ScalarSliceVisitor {
            destination: self.destination,
            element_size: self.element_size,
            layout: self.layout,
        })
    }
}

/// Decodes a Go slice of non-pointer structs through each struct's ordered
/// receiver codec. Fatal custom-unmarshal errors return before final length
/// truncation, after installing the partially mutated current slot.
pub(crate) struct SharedObjectSliceSeed<'a, T> {
    destination: &'a mut GoSharedSlice<T>,
    element_size: usize,
    layout: GoSliceElementLayout,
}

impl<'a, T> SharedObjectSliceSeed<'a, T> {
    pub(crate) fn new(
        destination: &'a mut GoSharedSlice<T>,
        element_size: usize,
        layout: GoSliceElementLayout,
    ) -> Self {
        Self {
            destination,
            element_size,
            layout,
        }
    }
}

impl<'de, T> DeserializeSeed<'de> for SharedObjectSliceSeed<'_, T>
where
    T: Clone + Default + GoJsonMerge,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectSliceVisitor<'a, T> {
            destination: &'a mut GoSharedSlice<T>,
            element_size: usize,
            layout: GoSliceElementLayout,
        }

        impl<'de, T> Visitor<'de> for ObjectSliceVisitor<'_, T>
        where
            T: Clone + Default + GoJsonMerge,
        {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of JSON structs")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.destination = GoSharedSlice::default();
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.destination = GoSharedSlice::default();
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let decoded_len = elements.len();
                let mut first_error = None;
                for (index, raw) in elements.into_iter().enumerate() {
                    let capacity = go_64_slice_decode_capacity(
                        self.destination.capacity(),
                        index + 1,
                        self.element_size,
                        self.layout,
                    );
                    self.destination.prepare_decode_slot(index, capacity);
                    if raw.get() == "null" {
                        continue;
                    }
                    let mut value = self.destination.decode_slot(index);
                    let mut element = serde_json::Deserializer::from_str(raw.get());
                    let result = value
                        .go_json_merge(&mut element)
                        .and_then(|()| element.end());
                    self.destination.set_decode_slot(index, value);
                    if let Err(error) = result {
                        if is_fatal_json_error(&error) {
                            return Err(serde::de::Error::custom(error));
                        }
                        first_error.get_or_insert_with(|| error.to_string());
                    }
                }
                self.destination.finish_decode(decoded_len);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(ObjectSliceVisitor {
            destination: self.destination,
            element_size: self.element_size,
            layout: self.layout,
        })
    }
}

/// Decodes Go `[][]string`, reusing both the outer slice and each inner slice
/// header/backing. A null inner value clears that inner slice; a wrong type
/// retains the previous header and is recoverable.
pub(crate) struct SharedNestedStringSliceSeed<'a>(
    pub(crate) &'a mut GoSharedSlice<GoSharedSlice<String>>,
);

impl<'de> DeserializeSeed<'de> for SharedNestedStringSliceSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NestedStringSliceVisitor<'a>(&'a mut GoSharedSlice<GoSharedSlice<String>>);

        impl<'de> Visitor<'de> for NestedStringSliceVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an array of string arrays")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.0 = GoSharedSlice::default();
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.0 = GoSharedSlice::default();
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                let RawArrayMembers(elements) = RawArrayMembers::deserialize(deserializer)?;
                let decoded_len = elements.len();
                let mut first_error = None;
                for (index, raw) in elements.into_iter().enumerate() {
                    let capacity = go_64_slice_decode_capacity(
                        self.0.capacity(),
                        index + 1,
                        24,
                        GoSliceElementLayout::PointerBearing,
                    );
                    self.0.prepare_decode_slot(index, capacity);
                    if raw.get() == "null" {
                        self.0.set_decode_slot(index, GoSharedSlice::default());
                        continue;
                    }
                    let mut value = self.0.decode_slot(index);
                    let mut element = serde_json::Deserializer::from_str(raw.get());
                    let result = SharedStringSliceSeed(&mut value)
                        .deserialize(&mut element)
                        .and_then(|()| element.end());
                    self.0.set_decode_slot(index, value);
                    if let Err(error) = result {
                        first_error.get_or_insert_with(|| error.to_string());
                    }
                }
                self.0.finish_decode(decoded_len);
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(NestedStringSliceVisitor(self.0))
    }
}

/// Merges a Go `map[int64]bool`, retaining the shared map allocation and old
/// keys. Values decode into a fresh false slot before key parsing, so invalid
/// values still insert false for valid keys and invalid keys are not inserted.
pub(crate) struct SharedIntBoolMapSeed<'a>(
    pub(crate) &'a mut Option<GoShared<BTreeMap<i64, bool>>>,
);

impl<'de> DeserializeSeed<'de> for SharedIntBoolMapSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IntBoolMapVisitor<'a>(&'a mut Option<GoShared<BTreeMap<i64, bool>>>);

        impl<'de> Visitor<'de> for IntBoolMapVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or an object with signed integer keys")
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
                let RawObjectMembers(members) = RawObjectMembers::deserialize(deserializer)?;
                let destination = self.0.get_or_insert_with(|| GoShared::new(BTreeMap::new()));
                let mut first_error = None;
                for (key, raw) in members {
                    let mut value = false;
                    if raw.get() != "null" {
                        let mut element = serde_json::Deserializer::from_str(raw.get());
                        match bool::deserialize(&mut element).and_then(|decoded| {
                            element.end()?;
                            Ok(decoded)
                        }) {
                            Ok(decoded) => value = decoded,
                            Err(error) => {
                                first_error.get_or_insert_with(|| error.to_string());
                            }
                        }
                    }
                    match key.parse::<i64>() {
                        Ok(key) => {
                            destination.write().insert(key, value);
                        }
                        Err(error) => {
                            first_error.get_or_insert_with(|| error.to_string());
                        }
                    }
                }
                if let Some(error) = first_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(IntBoolMapVisitor(self.0))
    }
}
