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

//! Source-shaped Go interface values used by persisted model metadata.
//!
//! A Rust `Any` downcast is not a Go interface contract: it cannot state the
//! source dynamic type, copy a slice header while retaining its backing, or
//! reproduce Go's panicking equality boundary. [`GoAnyValue`] therefore makes
//! those operations explicit and object safe. The built-in implementation,
//! [`ColumnDefaultValue`], covers the values produced by `encoding/json` plus
//! the pointer, byte-slice, array, struct, and defined-string shapes needed at
//! model call boundaries. Other source types can implement the same hooks
//! without participating in Rust reflection.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use tidb_datatype::GoString;

use crate::go_runtime::{
    go_64_slice_decode_capacity, GoShared, GoSharedSlice, GoSliceElementLayout,
};

/// The underlying source kind of a dynamic Go type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GoTypeKind {
    /// `bool` or a defined boolean type.
    Bool,
    /// `int64` or a defined type with that representation.
    Int64,
    /// `uint64` or a defined type with that representation.
    Uint64,
    /// `byte` (`uint8`) or a defined type with that representation.
    Byte,
    /// `float64` or a defined type with that representation.
    Float64,
    /// `string` or a defined string type.
    String,
    /// A Go slice, including `[]byte`.
    Slice,
    /// A Go map.
    Map,
    /// A Go pointer.
    Pointer,
    /// A Go array.
    Array,
    /// A Go struct.
    Struct,
    /// A source type whose operations are entirely supplied by custom hooks.
    Other,
}

/// Exact source dynamic-type identity plus its diagnostic spelling.
///
/// Defined types retain the import path separately from the display spelling,
/// so two packages with the same package name do not collapse into one type.
#[derive(Clone, Debug)]
pub struct GoTypeIdentity {
    identity: String,
    display: String,
    kind: GoTypeKind,
}

impl PartialEq for GoTypeIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.identity == other.identity
    }
}

impl Eq for GoTypeIdentity {}

impl PartialOrd for GoTypeIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GoTypeIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.kind, &self.identity).cmp(&(other.kind, &other.identity))
    }
}

impl Hash for GoTypeIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.identity.hash(state);
    }
}

impl GoTypeIdentity {
    /// Constructs a built-in or unnamed composite type whose source spelling
    /// is also its complete identity.
    #[must_use]
    pub fn unnamed(spelling: impl Into<String>, kind: GoTypeKind) -> Self {
        let spelling = spelling.into();
        Self {
            identity: spelling.clone(),
            display: spelling,
            kind,
        }
    }

    /// Constructs a defined Go type. `package_path` is the canonical import
    /// path; `display` is the source diagnostic spelling such as `model.Name`.
    #[must_use]
    pub fn defined(
        package_path: impl Into<String>,
        name: impl Into<String>,
        display: impl Into<String>,
        kind: GoTypeKind,
    ) -> Self {
        let package_path = package_path.into();
        let name = name.into();
        Self {
            identity: format!("{package_path}.{name}"),
            display: display.into(),
            kind,
        }
    }

    /// The exact identity token used by interface comparison.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The type spelling used by Go-style diagnostics.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display
    }

    /// The source type's underlying kind.
    #[must_use]
    pub const fn kind(&self) -> GoTypeKind {
        self.kind
    }
}

/// Error produced by a dynamic value's explicit Go JSON projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoAnyJsonError(String);

impl GoAnyJsonError {
    /// Constructs a source-facing projection error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for GoAnyJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for GoAnyJsonError {}

/// Owned JSON projection returned by [`GoAnyValue::go_json_value`].
///
/// Go strings remain byte preserving until serialization; byte slices retain
/// their special base64 representation instead of becoming numeric arrays.
#[derive(Clone, Debug)]
pub enum GoJsonValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON signed integer.
    Int(i64),
    /// JSON unsigned integer.
    Uint(u64),
    /// JSON floating point number.
    Float(f64),
    /// JSON string with Go's arbitrary-byte replacement rule.
    String(GoString),
    /// Go `[]byte`: nil is null, non-nil is padded base64.
    Bytes(Option<Vec<u8>>),
    /// JSON array.
    Array(Vec<GoJsonValue>),
    /// JSON object with lexically ordered map keys.
    Object(BTreeMap<String, GoJsonValue>),
    /// JSON object whose Go string keys may contain arbitrary bytes. Entries
    /// are already sorted by their unchanged source bytes.
    GoObject(Vec<(GoString, GoJsonValue)>),
    /// JSON object in source struct-field order.
    Struct(Vec<(String, GoJsonValue)>),
}

impl Serialize for GoJsonValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Uint(value) => serializer.serialize_u64(*value),
            Self::Float(value) if value.is_finite() => serializer.serialize_f64(*value),
            Self::Float(value) => Err(serde::ser::Error::custom(format!(
                "json: unsupported value: {value}"
            ))),
            Self::String(value) => value.serialize(serializer),
            Self::Bytes(value) => crate::serde_helpers::go_bytes::serialize(value, serializer),
            Self::Array(values) => values.serialize(serializer),
            Self::Object(values) => values.serialize(serializer),
            Self::GoObject(fields) => {
                let mut object = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    object.serialize_entry(&name.to_utf8_lossy_go(), value)?;
                }
                object.end()
            }
            Self::Struct(fields) => {
                let mut object = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    object.serialize_entry(name, value)?;
                }
                object.end()
            }
        }
    }
}

/// Comparable-value projection used after exact dynamic types match.
///
/// Slices and maps never produce this projection: comparing interfaces that
/// contain either type panics, including typed nil values.
#[derive(Clone, Copy, Debug)]
pub enum GoEqualityProjection<'a> {
    /// Boolean value.
    Bool(bool),
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    Uint(u64),
    /// Byte value.
    Byte(u8),
    /// Floating point value.
    Float(f64),
    /// String bytes.
    String(&'a GoString),
    /// Pointer identity, with `None` representing a typed nil pointer.
    Pointer(Option<&'a GoShared<GoAny>>),
    /// Comparable Go array elements.
    Array(&'a [GoAny]),
    /// Comparable Go struct fields, in declaration order.
    Struct(&'a [(String, GoAny)]),
    /// Explicit comparable bytes for a custom dynamic type.
    Opaque(&'a [u8]),
}

fn equality_projection_eq(left: GoEqualityProjection<'_>, right: GoEqualityProjection<'_>) -> bool {
    match (left, right) {
        (GoEqualityProjection::Bool(left), GoEqualityProjection::Bool(right)) => left == right,
        (GoEqualityProjection::Int(left), GoEqualityProjection::Int(right)) => left == right,
        (GoEqualityProjection::Uint(left), GoEqualityProjection::Uint(right)) => left == right,
        (GoEqualityProjection::Byte(left), GoEqualityProjection::Byte(right)) => left == right,
        (GoEqualityProjection::Float(left), GoEqualityProjection::Float(right)) => left == right,
        (GoEqualityProjection::String(left), GoEqualityProjection::String(right)) => left == right,
        (GoEqualityProjection::Pointer(left), GoEqualityProjection::Pointer(right)) => {
            match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => left.ptr_eq(right),
                _ => false,
            }
        }
        (GoEqualityProjection::Array(left), GoEqualityProjection::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| left.go_equal(right))
        }
        (GoEqualityProjection::Struct(left), GoEqualityProjection::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|((_, left), (_, right))| left.go_equal(right))
        }
        (GoEqualityProjection::Opaque(left), GoEqualityProjection::Opaque(right)) => left == right,
        _ => panic!("inconsistent Go equality hooks for one dynamic type"),
    }
}

/// Explicit view of the built-in model value domain.
///
/// Custom [`GoAnyValue`] implementations return [`Self::Custom`]; consumers
/// never downcast through Rust `Any`.
#[derive(Clone, Copy, Debug)]
pub enum GoAnyView<'a> {
    /// Signed integer.
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// Byte.
    Byte(u8),
    /// Floating point number.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// Exact built-in Go string.
    String(&'a GoString),
    /// A defined string type.
    DefinedString(&'a GoTypeIdentity, &'a GoString),
    /// A Go byte slice.
    Bytes(&'a GoAnyBytes),
    /// A Go interface slice.
    Slice(&'a GoAnySlice),
    /// A Go string-keyed interface map.
    Map(&'a GoAnyMap),
    /// A Go pointer.
    Pointer(&'a GoAnyPointer),
    /// A Go array.
    Array(&'a GoAnyArray),
    /// A Go struct.
    Struct(&'a GoAnyStruct),
    /// A value supplied by an external hook implementation.
    Custom,
}

/// Object-safe behavior required from a value stored in a Go interface.
///
/// `copy_for_interface` is intentionally distinct from cloning a Rust owner:
/// the implementation decides which headers, maps, and pointers remain shared.
pub trait GoAnyValue: fmt::Debug + Send + Sync {
    /// Exact source dynamic type.
    fn go_type(&self) -> GoTypeIdentity;

    /// Copies the source interface's dynamic value.
    fn copy_for_interface(&self) -> Box<dyn GoAnyValue>;

    /// Projects the dynamic value through Go `encoding/json` semantics.
    fn go_json_value(&self) -> Result<GoJsonValue, GoAnyJsonError>;

    /// Appends the dynamic value's Go default `%v` bytes. A byte buffer is
    /// deliberate: Go strings may contain bytes that no Rust `str` can hold.
    fn append_go_format(&self, output: &mut Vec<u8>);

    /// Projects a comparable value. Returning `None` marks the dynamic type
    /// uncomparable and makes interface equality panic.
    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>>;

    /// Explicit built-in view; custom values use the default.
    fn view(&self) -> GoAnyView<'_> {
        GoAnyView::Custom
    }

    /// Exact Go `value.(string)` boundary. Defined strings deliberately use
    /// the default `None` even though their underlying bytes are strings.
    fn builtin_string(&self) -> Option<&GoString> {
        None
    }
}

/// One Go interface header. `None` is a nil interface; `Some` may still hold
/// a typed nil pointer, slice, map, or byte slice.
#[derive(Default)]
pub struct GoAny(Option<Box<dyn GoAnyValue>>);

impl GoAny {
    /// Constructs a nil interface.
    #[must_use]
    pub const fn nil() -> Self {
        Self(None)
    }

    /// Stores one dynamic value in an interface.
    #[must_use]
    pub fn new(value: impl GoAnyValue + 'static) -> Self {
        Self(Some(Box::new(value)))
    }

    /// Stores an already object-safe dynamic value.
    #[must_use]
    pub fn from_boxed(value: Box<dyn GoAnyValue>) -> Self {
        Self(Some(value))
    }

    /// Whether the interface itself is nil. A typed nil dynamic value is
    /// deliberately non-nil here.
    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.0.is_none()
    }

    /// Exact source dynamic type, absent only for a nil interface.
    #[must_use]
    pub fn dynamic_type(&self) -> Option<GoTypeIdentity> {
        self.0.as_deref().map(|value| value.go_type())
    }

    /// Explicit built-in model view, absent for a nil interface.
    #[must_use]
    pub fn view(&self) -> Option<GoAnyView<'_>> {
        self.0.as_deref().map(|value| value.view())
    }

    /// Exact Go type assertion to the built-in `string` type.
    #[must_use]
    pub fn builtin_string(&self) -> Option<&GoString> {
        self.0.as_deref().and_then(|value| value.builtin_string())
    }

    /// Projects this interface through the dynamic JSON hook.
    pub fn go_json_value(&self) -> Result<GoJsonValue, GoAnyJsonError> {
        match self.0.as_deref() {
            None => Ok(GoJsonValue::Null),
            Some(value) => value.go_json_value(),
        }
    }

    /// Exact bytes produced by the dynamic value's formatting hook.
    #[must_use]
    pub fn go_format_bytes(&self) -> Vec<u8> {
        let Some(value) = self.0.as_deref() else {
            return b"<nil>".to_vec();
        };
        let mut output = Vec::new();
        value.append_go_format(&mut output);
        output
    }

    /// Exact Go interface equality. Unequal dynamic types return false before
    /// comparability is inspected; equal uncomparable types panic.
    #[must_use]
    pub fn go_equal(&self, other: &Self) -> bool {
        let (Some(left), Some(right)) = (self.0.as_deref(), other.0.as_deref()) else {
            return self.0.is_none() && other.0.is_none();
        };
        dynamic_values_equal(left, right)
    }
}

fn dynamic_values_equal(left: &dyn GoAnyValue, right: &dyn GoAnyValue) -> bool {
    let go_type = left.go_type();
    if go_type != right.go_type() {
        return false;
    }
    let left = left.equality_projection().unwrap_or_else(|| {
        panic!(
            "runtime error: comparing uncomparable type {}",
            go_type.display_name()
        )
    });
    let right = right.equality_projection().unwrap_or_else(|| {
        panic!(
            "runtime error: comparing uncomparable type {}",
            go_type.display_name()
        )
    });
    equality_projection_eq(left, right)
}

impl Clone for GoAny {
    fn clone(&self) -> Self {
        Self(self.0.as_deref().map(|value| value.copy_for_interface()))
    }
}

impl fmt::Debug for GoAny {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => formatter.write_str("GoAny(nil)"),
            Some(value) => formatter.debug_tuple("GoAny").field(value).finish(),
        }
    }
}

impl fmt::Display for GoAny {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&GoString::from_bytes(self.go_format_bytes()).to_utf8_lossy_go())
    }
}

impl PartialEq for GoAny {
    fn eq(&self, other: &Self) -> bool {
        self.go_equal(other)
    }
}

impl Serialize for GoAny {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.go_json_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GoAny {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AnyVisitor;

        impl<'de> Visitor<'de> for AnyVisitor {
            type Value = GoAny;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON value decoded into a Go interface")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(GoAny::nil())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(GoAny::nil())
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Bool(value).into())
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                // `encoding/json` decodes numbers stored in an interface as
                // float64, regardless of their lexical form.
                Ok(ColumnDefaultValue::Float(value as f64).into())
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Float(value as f64).into())
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::Float(value).into())
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(ColumnDefaultValue::str(value).into())
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                while let Some(value) = sequence.next_element::<GoAny>()? {
                    values.push(value);
                }
                let capacity = go_64_slice_decode_capacity(
                    0,
                    values.len(),
                    2 * std::mem::size_of::<usize>(),
                    GoSliceElementLayout::PointerBearing,
                );
                Ok(
                    ColumnDefaultValue::Slice(GoAnySlice::from_values_with_capacity(
                        values, capacity,
                    ))
                    .into(),
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, GoAny>()? {
                    values.insert(key, value);
                }
                Ok(ColumnDefaultValue::Map(GoAnyMap::allocated(values)).into())
            }
        }

        deserializer.deserialize_any(AnyVisitor)
    }
}

/// A defined Go string value. Its underlying bytes serialize and format like
/// a string, but a type assertion to built-in `string` must fail.
#[derive(Clone, Debug)]
pub struct GoDefinedString {
    go_type: GoTypeIdentity,
    value: GoString,
}

impl GoDefinedString {
    /// Constructs a defined string value.
    #[must_use]
    pub fn new(go_type: GoTypeIdentity, value: impl Into<GoString>) -> Self {
        assert_eq!(go_type.kind(), GoTypeKind::String);
        Self {
            go_type,
            value: value.into(),
        }
    }

    /// Exact source dynamic type.
    #[must_use]
    pub fn go_type(&self) -> &GoTypeIdentity {
        &self.go_type
    }

    /// Underlying Go string bytes.
    #[must_use]
    pub fn value(&self) -> &GoString {
        &self.value
    }
}

/// Go `[]byte` with independent copied headers and shared mutable backing.
#[derive(Clone, Debug, Default)]
pub struct GoAnyBytes {
    bytes: GoSharedSlice<u8>,
}

impl GoAnyBytes {
    /// Constructs an allocated byte slice.
    #[must_use]
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self {
            bytes: GoSharedSlice::from_vec(bytes),
        }
    }

    /// Constructs an allocated byte slice with fully initialized capacity.
    #[must_use]
    pub fn from_vec_with_capacity(bytes: Vec<u8>, capacity: usize) -> Self {
        Self {
            bytes: GoSharedSlice::from_vec_with_capacity(bytes, capacity),
        }
    }

    /// Copies the source slice header.
    #[must_use]
    pub fn header(&self) -> GoSharedSlice<u8> {
        self.bytes.clone()
    }

    /// Whether the slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.bytes.is_allocated()
    }

    /// Visible length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Visible capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Mutates one shared backing-array byte.
    pub fn set(&self, index: usize, value: u8) {
        self.bytes.set(index, value);
    }

    /// Reports backing-array identity.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        self.bytes.backing_ptr_eq(&other.bytes)
    }
}

/// Go `[]interface{}` with source slice-header copy behavior.
#[derive(Clone, Debug, Default)]
pub struct GoAnySlice {
    values: GoSharedSlice<GoAny>,
}

impl GoAnySlice {
    /// Constructs an allocated interface slice.
    #[must_use]
    pub fn from_values(values: Vec<GoAny>) -> Self {
        Self {
            values: GoSharedSlice::from_vec(values),
        }
    }

    /// Constructs an allocated interface slice with observable spare capacity.
    #[must_use]
    pub fn from_values_with_capacity(values: Vec<GoAny>, capacity: usize) -> Self {
        Self {
            values: GoSharedSlice::from_vec_with_capacity(values, capacity),
        }
    }

    /// Whether the slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.values.is_allocated()
    }

    /// Visible length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Visible capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Copies one element interface.
    #[must_use]
    pub fn get(&self, index: usize) -> GoAny {
        self.values.get(index)
    }

    /// Replaces one element through every header sharing the backing array.
    pub fn set(&self, index: usize, value: GoAny) {
        self.values.set(index, value);
    }

    /// Reslices this header to length zero without changing sibling headers.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Reports backing-array identity.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        self.values.backing_ptr_eq(&other.values)
    }
}

/// Go `map[string]interface{}` with shared map identity.
#[derive(Clone, Debug, Default)]
pub struct GoAnyMap {
    values: Option<GoShared<BTreeMap<GoString, GoAny>>>,
}

impl GoAnyMap {
    /// Constructs an allocated map, including the allocated-empty case.
    #[must_use]
    pub fn allocated<K>(values: impl IntoIterator<Item = (K, GoAny)>) -> Self
    where
        K: Into<GoString>,
    {
        Self {
            values: Some(GoShared::new(
                values
                    .into_iter()
                    .map(|(key, value)| (key.into(), value))
                    .collect(),
            )),
        }
    }

    /// Whether the map is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.values.is_some()
    }

    /// Copies one map value interface.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<GoAny> {
        self.values.as_ref().and_then(|values| {
            values
                .read()
                .iter()
                .find(|(candidate, _)| candidate.as_bytes() == key.as_bytes())
                .map(|(_, value)| value.clone())
        })
    }

    /// Mutates the shared map, allocating it when nil.
    pub fn insert(&mut self, key: impl Into<GoString>, value: GoAny) -> Option<GoAny> {
        self.values
            .get_or_insert_with(|| GoShared::new(BTreeMap::new()))
            .write()
            .insert(key.into(), value)
    }

    /// Reports source map identity.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.values, &other.values) {
            (None, None) => true,
            (Some(left), Some(right)) => left.ptr_eq(right),
            _ => false,
        }
    }
}

/// Go `*interface{}` with shared pointee identity.
#[derive(Clone, Debug, Default)]
pub struct GoAnyPointer {
    value: Option<GoShared<GoAny>>,
}

impl GoAnyPointer {
    /// Allocates a non-nil pointer.
    #[must_use]
    pub fn new(value: GoAny) -> Self {
        Self {
            value: Some(GoShared::new(value)),
        }
    }

    /// Returns a shared pointee handle.
    #[must_use]
    pub fn pointee(&self) -> Option<GoShared<GoAny>> {
        self.value.clone()
    }

    /// Reports source pointer identity.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.value, &other.value) {
            (None, None) => true,
            (Some(left), Some(right)) => left.ptr_eq(right),
            _ => false,
        }
    }
}

/// A Go array value. Cloning copies every element interface value.
#[derive(Clone, Debug)]
pub struct GoAnyArray {
    go_type: GoTypeIdentity,
    values: Vec<GoAny>,
}

impl GoAnyArray {
    /// Constructs an array from source element interfaces.
    #[must_use]
    pub fn new(go_type: GoTypeIdentity, values: Vec<GoAny>) -> Self {
        assert_eq!(go_type.kind(), GoTypeKind::Array);
        Self { go_type, values }
    }

    /// Exact source array type, including its length and element type.
    #[must_use]
    pub fn go_type(&self) -> &GoTypeIdentity {
        &self.go_type
    }

    /// Array elements in source order.
    #[must_use]
    pub fn values(&self) -> &[GoAny] {
        &self.values
    }

    /// Mutates this copied array value only.
    pub fn set(&mut self, index: usize, value: GoAny) {
        self.values[index] = value;
    }
}

/// A Go struct value. Cloning copies its fields while nested map/pointer/slice
/// values retain their own source alias semantics.
#[derive(Clone, Debug)]
pub struct GoAnyStruct {
    go_type: GoTypeIdentity,
    fields: Vec<(String, GoAny)>,
}

impl GoAnyStruct {
    /// Constructs fields in declaration/JSON order.
    #[must_use]
    pub fn new(go_type: GoTypeIdentity, fields: Vec<(String, GoAny)>) -> Self {
        assert_eq!(go_type.kind(), GoTypeKind::Struct);
        Self { go_type, fields }
    }

    /// Exact source struct type.
    #[must_use]
    pub fn go_type(&self) -> &GoTypeIdentity {
        &self.go_type
    }

    /// Struct fields in declaration order.
    #[must_use]
    pub fn fields(&self) -> &[(String, GoAny)] {
        &self.fields
    }

    /// Mutates this copied struct value only.
    pub fn set(&mut self, index: usize, value: GoAny) {
        self.fields[index].1 = value;
    }
}

/// Built-in model dynamic values. Composite wrappers encode the ownership
/// rule selected by the Go source type rather than inheriting `Vec`/map clone.
#[derive(Clone, Debug)]
pub enum ColumnDefaultValue {
    /// Go `int64`.
    Int(i64),
    /// Go `uint64`.
    Uint(u64),
    /// Go `byte`.
    Byte(u8),
    /// Go `float64`.
    Float(f64),
    /// Go `bool`.
    Bool(bool),
    /// Exact built-in Go `string`.
    Str(GoString),
    /// A defined Go string type.
    DefinedString(GoDefinedString),
    /// Go `[]byte`.
    Bytes(GoAnyBytes),
    /// Go `[]interface{}`.
    Slice(GoAnySlice),
    /// Go `map[string]interface{}`.
    Map(GoAnyMap),
    /// Go `*interface{}`.
    Pointer(GoAnyPointer),
    /// A Go array with its exact dynamic type.
    Array(GoAnyArray),
    /// A Go struct with its exact dynamic type.
    Struct(GoAnyStruct),
}

impl ColumnDefaultValue {
    /// A built-in string from UTF-8 text.
    #[must_use]
    pub fn str(value: &str) -> Self {
        Self::Str(GoString::from(value))
    }

    /// A built-in string from arbitrary Go bytes.
    #[must_use]
    pub fn string_bytes(value: impl Into<GoString>) -> Self {
        Self::Str(value.into())
    }

    /// A defined string value with exact package-path identity.
    #[must_use]
    pub fn defined_string(go_type: GoTypeIdentity, value: impl Into<GoString>) -> Self {
        Self::DefinedString(GoDefinedString::new(go_type, value))
    }

    fn go_type_identity(&self) -> GoTypeIdentity {
        match self {
            Self::Int(_) => GoTypeIdentity::unnamed("int64", GoTypeKind::Int64),
            Self::Uint(_) => GoTypeIdentity::unnamed("uint64", GoTypeKind::Uint64),
            Self::Byte(_) => GoTypeIdentity::unnamed("uint8", GoTypeKind::Byte),
            Self::Float(_) => GoTypeIdentity::unnamed("float64", GoTypeKind::Float64),
            Self::Bool(_) => GoTypeIdentity::unnamed("bool", GoTypeKind::Bool),
            Self::Str(_) => GoTypeIdentity::unnamed("string", GoTypeKind::String),
            Self::DefinedString(value) => value.go_type.clone(),
            Self::Bytes(_) => GoTypeIdentity::unnamed("[]uint8", GoTypeKind::Slice),
            Self::Slice(_) => GoTypeIdentity::unnamed("[]interface {}", GoTypeKind::Slice),
            Self::Map(_) => GoTypeIdentity::unnamed("map[string]interface {}", GoTypeKind::Map),
            Self::Pointer(_) => GoTypeIdentity::unnamed("*interface {}", GoTypeKind::Pointer),
            Self::Array(value) => value.go_type.clone(),
            Self::Struct(value) => value.go_type.clone(),
        }
    }
}

impl GoAnyValue for ColumnDefaultValue {
    fn go_type(&self) -> GoTypeIdentity {
        self.go_type_identity()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_value(&self) -> Result<GoJsonValue, GoAnyJsonError> {
        match self {
            Self::Int(value) => Ok(GoJsonValue::Int(*value)),
            Self::Uint(value) => Ok(GoJsonValue::Uint(*value)),
            Self::Byte(value) => Ok(GoJsonValue::Uint(u64::from(*value))),
            Self::Float(value) if value.is_finite() => Ok(GoJsonValue::Float(*value)),
            Self::Float(value) => Err(GoAnyJsonError::new(format!(
                "json: unsupported value: {value}"
            ))),
            Self::Bool(value) => Ok(GoJsonValue::Bool(*value)),
            Self::Str(value) => Ok(GoJsonValue::String(value.clone())),
            Self::DefinedString(value) => Ok(GoJsonValue::String(value.value.clone())),
            Self::Bytes(value) => Ok(GoJsonValue::Bytes(
                value.bytes.is_allocated().then(|| value.bytes.snapshot()),
            )),
            Self::Slice(value) => {
                if !value.values.is_allocated() {
                    return Ok(GoJsonValue::Null);
                }
                value
                    .values
                    .snapshot()
                    .into_iter()
                    .map(|value| value.go_json_value())
                    .collect::<Result<Vec<_>, _>>()
                    .map(GoJsonValue::Array)
            }
            Self::Map(value) => {
                let Some(values) = &value.values else {
                    return Ok(GoJsonValue::Null);
                };
                let values = values.read().clone();
                values
                    .into_iter()
                    .map(|(key, value)| value.go_json_value().map(|value| (key, value)))
                    .collect::<Result<Vec<_>, _>>()
                    .map(GoJsonValue::GoObject)
            }
            Self::Pointer(value) => match &value.value {
                None => Ok(GoJsonValue::Null),
                Some(value) => value.read().go_json_value(),
            },
            Self::Array(value) => value
                .values
                .iter()
                .map(GoAny::go_json_value)
                .collect::<Result<Vec<_>, _>>()
                .map(GoJsonValue::Array),
            Self::Struct(value) => value
                .fields
                .iter()
                .map(|(name, value)| value.go_json_value().map(|value| (name.clone(), value)))
                .collect::<Result<Vec<_>, _>>()
                .map(GoJsonValue::Struct),
        }
    }

    fn append_go_format(&self, output: &mut Vec<u8>) {
        match self {
            Self::Int(value) => output.extend_from_slice(value.to_string().as_bytes()),
            Self::Uint(value) => output.extend_from_slice(value.to_string().as_bytes()),
            Self::Byte(value) => output.extend_from_slice(value.to_string().as_bytes()),
            Self::Float(value) if value.is_nan() => output.extend_from_slice(b"NaN"),
            Self::Float(value) if *value == f64::INFINITY => output.extend_from_slice(b"+Inf"),
            Self::Float(value) if *value == f64::NEG_INFINITY => output.extend_from_slice(b"-Inf"),
            Self::Float(value) => output.extend_from_slice(value.to_string().as_bytes()),
            Self::Bool(value) => output.extend_from_slice(value.to_string().as_bytes()),
            Self::Str(value) => output.extend_from_slice(value.as_bytes()),
            Self::DefinedString(value) => output.extend_from_slice(value.value.as_bytes()),
            Self::Bytes(value) => {
                output.push(b'[');
                for (index, byte) in value.bytes.snapshot().iter().enumerate() {
                    if index != 0 {
                        output.push(b' ');
                    }
                    output.extend_from_slice(byte.to_string().as_bytes());
                }
                output.push(b']');
            }
            Self::Slice(value) => append_interface_sequence(&value.values.snapshot(), output),
            Self::Map(value) => {
                output.extend_from_slice(b"map[");
                if let Some(values) = &value.values {
                    let values = values.read().clone();
                    for (index, (key, value)) in values.iter().enumerate() {
                        if index != 0 {
                            output.push(b' ');
                        }
                        output.extend_from_slice(key.as_bytes());
                        output.push(b':');
                        output.extend_from_slice(&value.go_format_bytes());
                    }
                }
                output.push(b']');
            }
            Self::Pointer(value) => match &value.value {
                None => output.extend_from_slice(b"<nil>"),
                Some(value) => {
                    output.extend_from_slice(format!("0x{:x}", value.identity_address()).as_bytes())
                }
            },
            Self::Array(value) => append_interface_sequence(&value.values, output),
            Self::Struct(value) => {
                output.push(b'{');
                for (index, (_, field)) in value.fields.iter().enumerate() {
                    if index != 0 {
                        output.push(b' ');
                    }
                    output.extend_from_slice(&field.go_format_bytes());
                }
                output.push(b'}');
            }
        }
    }

    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
        match self {
            Self::Int(value) => Some(GoEqualityProjection::Int(*value)),
            Self::Uint(value) => Some(GoEqualityProjection::Uint(*value)),
            Self::Byte(value) => Some(GoEqualityProjection::Byte(*value)),
            Self::Float(value) => Some(GoEqualityProjection::Float(*value)),
            Self::Bool(value) => Some(GoEqualityProjection::Bool(*value)),
            Self::Str(value) => Some(GoEqualityProjection::String(value)),
            Self::DefinedString(value) => Some(GoEqualityProjection::String(&value.value)),
            Self::Bytes(_) | Self::Slice(_) | Self::Map(_) => None,
            Self::Pointer(value) => Some(GoEqualityProjection::Pointer(value.value.as_ref())),
            Self::Array(value) => Some(GoEqualityProjection::Array(&value.values)),
            Self::Struct(value) => Some(GoEqualityProjection::Struct(&value.fields)),
        }
    }

    fn view(&self) -> GoAnyView<'_> {
        match self {
            Self::Int(value) => GoAnyView::Int(*value),
            Self::Uint(value) => GoAnyView::Uint(*value),
            Self::Byte(value) => GoAnyView::Byte(*value),
            Self::Float(value) => GoAnyView::Float(*value),
            Self::Bool(value) => GoAnyView::Bool(*value),
            Self::Str(value) => GoAnyView::String(value),
            Self::DefinedString(value) => GoAnyView::DefinedString(&value.go_type, &value.value),
            Self::Bytes(value) => GoAnyView::Bytes(value),
            Self::Slice(value) => GoAnyView::Slice(value),
            Self::Map(value) => GoAnyView::Map(value),
            Self::Pointer(value) => GoAnyView::Pointer(value),
            Self::Array(value) => GoAnyView::Array(value),
            Self::Struct(value) => GoAnyView::Struct(value),
        }
    }

    fn builtin_string(&self) -> Option<&GoString> {
        match self {
            Self::Str(value) => Some(value),
            _ => None,
        }
    }
}

fn append_interface_sequence(values: &[GoAny], output: &mut Vec<u8>) {
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b' ');
        }
        output.extend_from_slice(&value.go_format_bytes());
    }
    output.push(b']');
}

impl PartialEq for ColumnDefaultValue {
    fn eq(&self, other: &Self) -> bool {
        dynamic_values_equal(self, other)
    }
}

impl Serialize for ColumnDefaultValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.go_json_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl From<ColumnDefaultValue> for GoAny {
    fn from(value: ColumnDefaultValue) -> Self {
        Self::new(value)
    }
}

impl From<Option<ColumnDefaultValue>> for GoAny {
    fn from(value: Option<ColumnDefaultValue>) -> Self {
        value.map_or_else(Self::nil, Self::from)
    }
}

impl PartialEq<Option<ColumnDefaultValue>> for GoAny {
    fn eq(&self, other: &Option<ColumnDefaultValue>) -> bool {
        match (self.0.as_deref(), other.as_ref()) {
            (None, None) => true,
            (Some(left), Some(right)) => dynamic_values_equal(left, right),
            _ => false,
        }
    }
}

impl PartialEq<GoAny> for Option<ColumnDefaultValue> {
    fn eq(&self, other: &GoAny) -> bool {
        other == self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct HookedValue([u8; 1]);

    impl GoAnyValue for HookedValue {
        fn go_type(&self) -> GoTypeIdentity {
            GoTypeIdentity::defined(
                "example.com/hooks",
                "Value",
                "hooks.Value",
                GoTypeKind::Other,
            )
        }

        fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
            Box::new(Self(self.0))
        }

        fn go_json_value(&self) -> Result<GoJsonValue, GoAnyJsonError> {
            Ok(GoJsonValue::String(GoString::from_bytes(self.0.to_vec())))
        }

        fn append_go_format(&self, output: &mut Vec<u8>) {
            output.extend_from_slice(format!("hook:{}", self.0[0]).as_bytes());
        }

        fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
            Some(GoEqualityProjection::Opaque(&self.0))
        }
    }

    #[test]
    fn nil_typed_nil_and_defined_string_keep_exact_dynamic_types() {
        let nil = GoAny::nil();
        let typed_nil_pointer = GoAny::from(ColumnDefaultValue::Pointer(GoAnyPointer::default()));
        let typed_nil_slice = GoAny::from(ColumnDefaultValue::Slice(GoAnySlice::default()));
        assert!(nil.is_nil());
        assert!(!typed_nil_pointer.is_nil());
        assert!(!typed_nil_slice.is_nil());
        assert!(!nil.go_equal(&typed_nil_pointer));
        assert_eq!(
            String::from_utf8(
                crate::serde_helpers::to_go_json(&typed_nil_pointer.go_json_value().unwrap())
                    .unwrap()
            )
            .unwrap(),
            "null"
        );

        let named_type = GoTypeIdentity::defined(
            "example.com/a/model",
            "Default",
            "model.Default",
            GoTypeKind::String,
        );
        let builtin = GoAny::from(ColumnDefaultValue::string_bytes(vec![0xff, b'a']));
        let named = GoAny::from(ColumnDefaultValue::defined_string(
            named_type.clone(),
            vec![0xff, b'a'],
        ));
        assert_eq!(builtin.builtin_string().unwrap().as_bytes(), [0xff, b'a']);
        assert!(named.builtin_string().is_none());
        assert_ne!(builtin.dynamic_type(), named.dynamic_type());
        assert!(!builtin.go_equal(&named));
        assert_eq!(named.dynamic_type(), Some(named_type));
    }

    #[test]
    fn interface_copy_obeys_slice_map_pointer_array_and_struct_ownership() {
        let mut source_slice =
            GoAnySlice::from_values_with_capacity(vec![GoAny::from(ColumnDefaultValue::Int(1))], 3);
        let copied_slice = source_slice.clone();
        assert!(source_slice.backing_ptr_eq(&copied_slice));
        assert_eq!(copied_slice.capacity(), 3);
        copied_slice.set(0, ColumnDefaultValue::Int(2).into());
        assert!(source_slice
            .get(0)
            .go_equal(&ColumnDefaultValue::Int(2).into()));
        source_slice.clear();
        assert_eq!(source_slice.len(), 0);
        assert_eq!(copied_slice.len(), 1);

        let mut source_map = GoAnyMap::allocated(BTreeMap::from([(
            "k".to_owned(),
            GoAny::from(ColumnDefaultValue::Int(1)),
        )]));
        let mut copied_map = source_map.clone();
        assert!(source_map.ptr_eq(&copied_map));
        copied_map.insert("added".to_owned(), ColumnDefaultValue::Bool(true).into());
        assert!(source_map.get("added").is_some());

        let source_pointer = GoAnyPointer::new(ColumnDefaultValue::Int(1).into());
        let copied_pointer = source_pointer.clone();
        assert!(source_pointer.ptr_eq(&copied_pointer));
        *copied_pointer.pointee().unwrap().write() = ColumnDefaultValue::Int(4).into();
        assert!(source_pointer
            .pointee()
            .unwrap()
            .read()
            .go_equal(&ColumnDefaultValue::Int(4).into()));
        source_map.insert(
            "pointer".to_owned(),
            ColumnDefaultValue::Pointer(source_pointer.clone()).into(),
        );

        let nested_slice = GoAny::from(ColumnDefaultValue::Slice(copied_slice));
        let mut array = GoAnyArray::new(
            GoTypeIdentity::unnamed("[2]interface {}", GoTypeKind::Array),
            vec![
                nested_slice.clone(),
                ColumnDefaultValue::Map(source_map.clone()).into(),
            ],
        );
        let array_copy = array.clone();
        array.set(0, ColumnDefaultValue::Bool(false).into());
        assert!(matches!(
            array_copy.values()[0].view(),
            Some(GoAnyView::Slice(_))
        ));
        let GoAnyView::Slice(nested_copy) = array_copy.values()[0].view().unwrap() else {
            unreachable!();
        };
        nested_copy.set(0, ColumnDefaultValue::Int(9).into());
        let GoAnyView::Slice(nested_source) = nested_slice.view().unwrap() else {
            unreachable!();
        };
        assert!(nested_source
            .get(0)
            .go_equal(&ColumnDefaultValue::Int(9).into()));
        let Some(GoAnyView::Map(nested_map)) = array_copy.values()[1].view() else {
            unreachable!();
        };
        let nested_pointer = nested_map.get("pointer").unwrap();
        let Some(GoAnyView::Pointer(nested_pointer)) = nested_pointer.view() else {
            unreachable!();
        };
        assert!(nested_pointer.ptr_eq(&source_pointer));

        let mut structure = GoAnyStruct::new(
            GoTypeIdentity::defined(
                "example.com/model",
                "Wrapper",
                "model.Wrapper",
                GoTypeKind::Struct,
            ),
            vec![("V".to_owned(), nested_slice)],
        );
        let structure_copy = structure.clone();
        structure.set(0, GoAny::nil());
        assert!(!structure_copy.fields()[0].1.is_nil());
    }

    #[test]
    fn explicit_hooks_project_json_format_and_uncomparable_equality() {
        let value = GoAny::from(ColumnDefaultValue::Slice(GoAnySlice::from_values(vec![
            ColumnDefaultValue::string_bytes(vec![b'a', 0xff]).into(),
            GoAny::nil(),
        ])));
        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&value).unwrap()).unwrap(),
            r#"["a\ufffd",null]"#
        );
        assert_eq!(value.to_string(), "[a\u{fffd} <nil>]");
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            value.go_equal(&value)
        }))
        .is_err());

        let array_type = GoTypeIdentity::unnamed("[1]interface {}", GoTypeKind::Array);
        let comparable = GoAny::from(ColumnDefaultValue::Array(GoAnyArray::new(
            array_type,
            vec![ColumnDefaultValue::Int(1).into()],
        )));
        assert!(comparable.go_equal(&comparable.clone()));

        let nested_uncomparable = GoAny::from(ColumnDefaultValue::Array(GoAnyArray::new(
            GoTypeIdentity::unnamed("[1]interface {}", GoTypeKind::Array),
            vec![value],
        )));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            nested_uncomparable.go_equal(&nested_uncomparable)
        }))
        .is_err());

        let non_finite = GoAny::from(ColumnDefaultValue::Float(f64::INFINITY));
        assert!(non_finite.go_json_value().is_err());
        assert!(crate::serde_helpers::to_go_json(&non_finite).is_err());

        let custom = GoAny::new(HookedValue([b'x']));
        let copied = custom.clone();
        assert!(custom.go_equal(&copied));
        assert_eq!(custom.to_string(), "hook:120");
        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&custom).unwrap()).unwrap(),
            r#""x""#
        );
        assert!(matches!(custom.view(), Some(GoAnyView::Custom)));
    }
}
