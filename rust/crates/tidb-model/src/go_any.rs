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
use std::collections::{BTreeMap, HashSet};
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

    /// Constructs the exact unnamed pointer type to this source type.
    #[must_use]
    pub fn pointer_to(&self) -> Self {
        Self {
            identity: format!("*{}", self.identity),
            display: format!("*{}", self.display),
            kind: GoTypeKind::Pointer,
        }
    }

    /// Constructs the exact unnamed slice type whose element has this source
    /// type. The element's package-qualified identity remains part of dynamic
    /// type equality while diagnostics use Go's ordinary display spelling.
    #[must_use]
    pub fn slice_of(&self) -> Self {
        Self {
            identity: format!("[]{}", self.identity),
            display: format!("[]{}", self.display),
            kind: GoTypeKind::Slice,
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

/// Source error class produced by a dynamic value's explicit Go JSON
/// projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoAnyJsonErrorKind {
    /// Go `encoding/json.UnsupportedValueError`. Rust cannot retain the
    /// source `reflect.Value`, so [`GoAnyJsonError`] retains its exact dynamic
    /// type and `Str` payload instead.
    UnsupportedValue,
    /// An error supplied by a custom [`GoAnyValue`] hook.
    Custom,
}

/// Error produced by a dynamic value's explicit Go JSON projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoAnyJsonError {
    kind: GoAnyJsonErrorKind,
    message: String,
    dynamic_type: Option<GoTypeIdentity>,
    unsupported_value_description: Option<String>,
}

impl GoAnyJsonError {
    /// Constructs an error supplied by a custom projection hook.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            kind: GoAnyJsonErrorKind::Custom,
            message: message.into(),
            dynamic_type: None,
            unsupported_value_description: None,
        }
    }

    /// Constructs Go `encoding/json.UnsupportedValueError` with the source
    /// value represented by its exact dynamic type and `Str` description.
    #[must_use]
    pub fn unsupported_value(dynamic_type: GoTypeIdentity, description: impl Into<String>) -> Self {
        let description = description.into();
        Self {
            kind: GoAnyJsonErrorKind::UnsupportedValue,
            message: format!("json: unsupported value: {description}"),
            dynamic_type: Some(dynamic_type),
            unsupported_value_description: Some(description),
        }
    }

    /// Source error class.
    #[must_use]
    pub const fn kind(&self) -> GoAnyJsonErrorKind {
        self.kind
    }

    /// Exact dynamic type of Go's unsupported `reflect.Value`, when this is
    /// an [`GoAnyJsonErrorKind::UnsupportedValue`] error.
    #[must_use]
    pub fn dynamic_type(&self) -> Option<&GoTypeIdentity> {
        self.dynamic_type.as_ref()
    }

    /// Go `UnsupportedValueError.Str`, when representable.
    #[must_use]
    pub fn unsupported_value_description(&self) -> Option<&str> {
        self.unsupported_value_description.as_deref()
    }
}

impl fmt::Display for GoAnyJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GoAnyJsonError {}

// Go 1.25.10 encoding/json waits until the 1001st nested map, non-byte
// slice, or pointer before paying for current-path identity tracking.
const GO_JSON_START_DETECTING_CYCLES_AFTER: usize = 1_000;

/// Identity key used by Go `encoding/json` for one recursive reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GoJsonReferenceIdentity {
    /// A typed Go pointer identity.
    Pointer(usize),
    /// A Go map allocation identity. It is distinct from [`Self::Pointer`]
    /// even when their process-local tokens happen to be numerically equal.
    Map(usize),
    /// A Go slice's first-element/backing identity and visible length.
    Slice {
        /// Process-local identity of the slice's first element/backing array.
        backing: usize,
        /// Visible slice length, which is part of `encoding/json`'s key.
        len: usize,
    },
}

/// One non-nil map, non-byte slice, or pointer in a JSON projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoJsonReference {
    identity: GoJsonReferenceIdentity,
    dynamic_type: GoTypeIdentity,
}

impl GoJsonReference {
    /// Constructs a source reference from its process-local identity and
    /// exact dynamic type.
    #[must_use]
    pub fn new(identity: GoJsonReferenceIdentity, dynamic_type: GoTypeIdentity) -> Self {
        Self {
            identity,
            dynamic_type,
        }
    }

    /// Source identity key.
    #[must_use]
    pub const fn identity(&self) -> GoJsonReferenceIdentity {
        self.identity
    }

    /// Exact dynamic type used by pointer identity and cycle errors.
    #[must_use]
    pub fn dynamic_type(&self) -> &GoTypeIdentity {
        &self.dynamic_type
    }
}

/// One declarative, object-safe JSON projection step.
///
/// Returning children instead of recursively projecting them lets
/// [`GoJsonContext`] preserve Go's depth-first hook order on an explicit heap
/// stack. Custom composite values use the referenced variants to join the
/// same current-path cycle domain as built-in model values.
#[derive(Debug)]
pub enum GoJsonProjection {
    /// A completed scalar, null, byte slice, or custom JSON value.
    Value(GoJsonValue),
    /// A Go array's child interfaces in source order.
    Array(Vec<GoAny>),
    /// A Go struct's JSON fields in source order.
    Struct(Vec<(String, GoAny)>),
    /// A custom non-byte slice and its child interfaces.
    ReferencedArray(GoJsonReference, Vec<GoAny>),
    /// A custom map and its already ordered JSON fields.
    ReferencedObject(GoJsonReference, Vec<(GoString, GoAny)>),
    /// A custom pointer and its loaded pointee interface.
    ReferencedPointer(GoJsonReference, GoAny),
    /// Built-in model `[]interface{}`; its elements are loaded after the
    /// traversal enters the source slice reference.
    InterfaceSlice(GoAnySlice),
    /// Built-in model `map[string]interface{}`; its fields are loaded after
    /// the traversal enters the source map reference.
    InterfaceMap(GoAnyMap),
    /// Built-in model `*interface{}`; its pointee is loaded after the
    /// traversal enters the source pointer reference.
    InterfacePointer(GoAnyPointer),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GoJsonSeenReference {
    // Go's ptrSeen key for pointers is an interface, so its dynamic pointer
    // type is part of equality. Map and slice keys erase their source type.
    Pointer(GoTypeIdentity, usize),
    Map(usize),
    Slice { backing: usize, len: usize },
}

impl GoJsonSeenReference {
    fn new(identity: GoJsonReferenceIdentity, dynamic_type: &GoTypeIdentity) -> Self {
        match identity {
            GoJsonReferenceIdentity::Pointer(identity) => {
                Self::Pointer(dynamic_type.clone(), identity)
            }
            GoJsonReferenceIdentity::Map(identity) => Self::Map(identity),
            GoJsonReferenceIdentity::Slice { backing, len } => Self::Slice { backing, len },
        }
    }
}

/// One Go `encoding/json` traversal state.
///
/// Projection work lives on the heap, so Go's depth-1001 cycle boundary does
/// not depend on the host thread's fixed call-stack size.
#[derive(Debug, Default)]
pub struct GoJsonContext {
    reference_depth: usize,
    seen_references: HashSet<GoJsonSeenReference>,
}

enum GoJsonWork {
    Project(GoAny),
    FinishArray(usize),
    FinishGoObject(Vec<GoString>),
    FinishStruct(Vec<String>),
    ExitReference(Option<GoJsonSeenReference>),
}

struct GoJsonTraversal<'a> {
    context: &'a mut GoJsonContext,
}

impl Drop for GoJsonTraversal<'_> {
    fn drop(&mut self) {
        // Go empties ptrSeen through defers and resets ptrLevel before an
        // encodeState is reused. This also runs when a custom hook panics.
        self.context.reference_depth = 0;
        self.context.seen_references.clear();
    }
}

impl GoJsonContext {
    /// Constructs a fresh Go JSON traversal.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Projects an interface through a stack-safe Go traversal.
    pub fn project(&mut self, value: &GoAny) -> Result<GoJsonValue, GoAnyJsonError> {
        self.reference_depth = 0;
        self.seen_references.clear();
        GoJsonTraversal { context: self }.run_interface(value)
    }

    fn project_dynamic(&mut self, value: &dyn GoAnyValue) -> Result<GoJsonValue, GoAnyJsonError> {
        self.reference_depth = 0;
        self.seen_references.clear();
        GoJsonTraversal { context: self }.run_dynamic(value)
    }
}

impl GoJsonTraversal<'_> {
    fn run_interface(&mut self, value: &GoAny) -> Result<GoJsonValue, GoAnyJsonError> {
        let projection = match value.0.as_deref() {
            None => GoJsonProjection::Value(GoJsonValue::Null),
            Some(value) => value.go_json_projection()?,
        };
        self.run(projection)
    }

    fn run_dynamic(&mut self, value: &dyn GoAnyValue) -> Result<GoJsonValue, GoAnyJsonError> {
        self.run(value.go_json_projection()?)
    }

    fn run(&mut self, root: GoJsonProjection) -> Result<GoJsonValue, GoAnyJsonError> {
        let mut work = Vec::new();
        let mut projected = Vec::new();
        self.schedule_projection(root, &mut work, &mut projected)?;

        while let Some(step) = work.pop() {
            match step {
                GoJsonWork::Project(value) => {
                    let projection = match value.0.as_deref() {
                        None => GoJsonProjection::Value(GoJsonValue::Null),
                        Some(value) => value.go_json_projection()?,
                    };
                    self.schedule_projection(projection, &mut work, &mut projected)?;
                }
                GoJsonWork::FinishArray(len) => {
                    let values = take_projected_tail(&mut projected, len);
                    projected.push(GoJsonValue::Array(values));
                }
                GoJsonWork::FinishGoObject(names) => {
                    let values = take_projected_tail(&mut projected, names.len());
                    projected.push(GoJsonValue::GoObject(
                        names.into_iter().zip(values).collect(),
                    ));
                }
                GoJsonWork::FinishStruct(names) => {
                    let values = take_projected_tail(&mut projected, names.len());
                    projected.push(GoJsonValue::Struct(names.into_iter().zip(values).collect()));
                }
                GoJsonWork::ExitReference(seen_reference) => {
                    self.exit_reference(seen_reference);
                }
            }
        }

        debug_assert_eq!(self.context.reference_depth, 0);
        debug_assert!(self.context.seen_references.is_empty());
        assert_eq!(projected.len(), 1, "Go JSON projection produced no root");
        Ok(projected.pop().expect("checked one Go JSON root"))
    }

    fn schedule_projection(
        &mut self,
        projection: GoJsonProjection,
        work: &mut Vec<GoJsonWork>,
        projected: &mut Vec<GoJsonValue>,
    ) -> Result<(), GoAnyJsonError> {
        match projection {
            GoJsonProjection::Value(value) => projected.push(value),
            GoJsonProjection::Array(values) => {
                self.schedule_array(None, values, work)?;
            }
            GoJsonProjection::Struct(fields) => self.schedule_struct(fields, work),
            GoJsonProjection::ReferencedArray(reference, values) => {
                self.schedule_array(Some(reference), values, work)?;
            }
            GoJsonProjection::ReferencedObject(reference, fields) => {
                self.schedule_go_object(Some(reference), fields, work)?;
            }
            GoJsonProjection::ReferencedPointer(reference, value) => {
                self.schedule_pointer(reference, value, work)?;
            }
            GoJsonProjection::InterfaceSlice(value) => {
                if !value.values.is_allocated() {
                    projected.push(GoJsonValue::Null);
                } else {
                    let reference = GoJsonReference::new(
                        value.json_reference_identity(),
                        GoTypeIdentity::unnamed("[]interface {}", GoTypeKind::Slice),
                    );
                    let seen_reference = self.enter_reference(&reference)?;
                    let values = value.values.snapshot();
                    self.schedule_array_after_enter(seen_reference, values, work);
                }
            }
            GoJsonProjection::InterfaceMap(value) => {
                let Some(values) = &value.values else {
                    projected.push(GoJsonValue::Null);
                    return Ok(());
                };
                let reference = GoJsonReference::new(
                    GoJsonReferenceIdentity::Map(values.identity_address()),
                    GoTypeIdentity::unnamed("map[string]interface {}", GoTypeKind::Map),
                );
                let seen_reference = self.enter_reference(&reference)?;
                let fields = values.read().clone().into_iter().collect::<Vec<_>>();
                self.schedule_go_object_after_enter(seen_reference, fields, work);
            }
            GoJsonProjection::InterfacePointer(value) => {
                let Some(value) = &value.value else {
                    projected.push(GoJsonValue::Null);
                    return Ok(());
                };
                let reference = GoJsonReference::new(
                    GoJsonReferenceIdentity::Pointer(value.identity_address()),
                    GoTypeIdentity::unnamed("*interface {}", GoTypeKind::Pointer),
                );
                let seen_reference = self.enter_reference(&reference)?;
                let value = value.read().clone();
                self.schedule_pointer_after_enter(seen_reference, value, work);
            }
        }
        Ok(())
    }

    fn schedule_array(
        &mut self,
        reference: Option<GoJsonReference>,
        values: Vec<GoAny>,
        work: &mut Vec<GoJsonWork>,
    ) -> Result<(), GoAnyJsonError> {
        if let Some(reference) = &reference {
            let seen_reference = self.enter_reference(reference)?;
            self.schedule_array_after_enter(seen_reference, values, work);
        } else {
            work.push(GoJsonWork::FinishArray(values.len()));
            work.extend(values.into_iter().rev().map(GoJsonWork::Project));
        }
        Ok(())
    }

    fn schedule_array_after_enter(
        &self,
        seen_reference: Option<GoJsonSeenReference>,
        values: Vec<GoAny>,
        work: &mut Vec<GoJsonWork>,
    ) {
        work.push(GoJsonWork::ExitReference(seen_reference));
        work.push(GoJsonWork::FinishArray(values.len()));
        work.extend(values.into_iter().rev().map(GoJsonWork::Project));
    }

    fn schedule_go_object(
        &mut self,
        reference: Option<GoJsonReference>,
        fields: Vec<(GoString, GoAny)>,
        work: &mut Vec<GoJsonWork>,
    ) -> Result<(), GoAnyJsonError> {
        if let Some(reference) = &reference {
            let seen_reference = self.enter_reference(reference)?;
            self.schedule_go_object_after_enter(seen_reference, fields, work);
        } else {
            let (names, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
            work.push(GoJsonWork::FinishGoObject(names));
            work.extend(values.into_iter().rev().map(GoJsonWork::Project));
        }
        Ok(())
    }

    fn schedule_go_object_after_enter(
        &self,
        seen_reference: Option<GoJsonSeenReference>,
        fields: Vec<(GoString, GoAny)>,
        work: &mut Vec<GoJsonWork>,
    ) {
        let (names, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        work.push(GoJsonWork::ExitReference(seen_reference));
        work.push(GoJsonWork::FinishGoObject(names));
        work.extend(values.into_iter().rev().map(GoJsonWork::Project));
    }

    fn schedule_struct(&self, fields: Vec<(String, GoAny)>, work: &mut Vec<GoJsonWork>) {
        let (names, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        work.push(GoJsonWork::FinishStruct(names));
        work.extend(values.into_iter().rev().map(GoJsonWork::Project));
    }

    fn schedule_pointer(
        &mut self,
        reference: GoJsonReference,
        value: GoAny,
        work: &mut Vec<GoJsonWork>,
    ) -> Result<(), GoAnyJsonError> {
        let seen_reference = self.enter_reference(&reference)?;
        self.schedule_pointer_after_enter(seen_reference, value, work);
        Ok(())
    }

    fn schedule_pointer_after_enter(
        &self,
        seen_reference: Option<GoJsonSeenReference>,
        value: GoAny,
        work: &mut Vec<GoJsonWork>,
    ) {
        work.push(GoJsonWork::ExitReference(seen_reference));
        work.push(GoJsonWork::Project(value));
    }

    fn enter_reference(
        &mut self,
        reference: &GoJsonReference,
    ) -> Result<Option<GoJsonSeenReference>, GoAnyJsonError> {
        self.context.reference_depth += 1;
        let tracked = self.context.reference_depth > GO_JSON_START_DETECTING_CYCLES_AFTER;
        let seen_reference =
            tracked.then(|| GoJsonSeenReference::new(reference.identity, &reference.dynamic_type));
        if seen_reference
            .as_ref()
            .is_some_and(|reference| !self.context.seen_references.insert(reference.clone()))
        {
            self.context.reference_depth -= 1;
            return Err(GoAnyJsonError::unsupported_value(
                reference.dynamic_type.clone(),
                format!(
                    "encountered a cycle via {}",
                    reference.dynamic_type.display_name()
                ),
            ));
        }
        Ok(seen_reference)
    }

    fn exit_reference(&mut self, seen_reference: Option<GoJsonSeenReference>) {
        if let Some(seen_reference) = &seen_reference {
            let removed = self.context.seen_references.remove(seen_reference);
            debug_assert!(removed, "tracked Go JSON reference was not removed");
        }
        self.context.reference_depth -= 1;
    }
}

fn take_projected_tail(projected: &mut Vec<GoJsonValue>, len: usize) -> Vec<GoJsonValue> {
    let start = projected
        .len()
        .checked_sub(len)
        .expect("Go JSON projection child underflow");
    projected.split_off(start)
}

/// Owned JSON value assembled from [`GoAnyValue::go_json_projection`].
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
    /// A complete JSON value already emitted through the Go formatter.
    Raw(String),
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
            Self::Raw(value) => serde_json::value::RawValue::from_string(value.clone())
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
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
    /// Pointer identity for a custom typed pointee.
    PointerAddress(Option<usize>),
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
        (
            GoEqualityProjection::PointerAddress(left),
            GoEqualityProjection::PointerAddress(right),
        ) => left == right,
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

    /// Supplies one declarative Go `encoding/json` projection step. Nested
    /// children are returned rather than recursively visited so the central
    /// traversal remains stack-safe and preserves source hook order.
    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError>;

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

    /// Explicit model JobArgs view. This keeps Go's dynamic-type assertion
    /// source-shaped without introducing Rust `Any` or unsafe downcasts.
    fn job_args_value(&self) -> Option<&crate::job_args::JobArgsValue> {
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

    /// Exact typed JobArgs value stored in this interface, when present.
    #[must_use]
    pub(crate) fn job_args_value(&self) -> Option<&crate::job_args::JobArgsValue> {
        self.0.as_deref().and_then(|value| value.job_args_value())
    }

    /// Projects this interface through the dynamic JSON hook.
    pub fn go_json_value(&self) -> Result<GoJsonValue, GoAnyJsonError> {
        GoJsonContext::new().project(self)
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

    /// Whether no bytes are visible through this header.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
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
    // `GoSharedSlice` intentionally hides its Arc. This token follows the
    // same header-copy/backing-allocation lifetime and supplies the source
    // first-element identity needed by encoding/json cycle detection.
    backing_identity: Option<GoShared<()>>,
}

impl GoAnySlice {
    /// Constructs an allocated interface slice.
    #[must_use]
    pub fn from_values(values: Vec<GoAny>) -> Self {
        Self {
            values: GoSharedSlice::from_vec(values),
            backing_identity: Some(GoShared::new(())),
        }
    }

    /// Constructs an allocated interface slice with observable spare capacity.
    #[must_use]
    pub fn from_values_with_capacity(values: Vec<GoAny>, capacity: usize) -> Self {
        Self {
            values: GoSharedSlice::from_vec_with_capacity(values, capacity),
            backing_identity: Some(GoShared::new(())),
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

    /// Whether no interface values are visible through this header.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
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

    fn json_reference_identity(&self) -> GoJsonReferenceIdentity {
        let backing = self
            .backing_identity
            .as_ref()
            .expect("allocated GoAnySlice is missing its backing identity")
            .identity_address();
        GoJsonReferenceIdentity::Slice {
            backing,
            len: self.values.len(),
        }
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

fn go_non_finite_float_description(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value == f64::INFINITY {
        "+Inf"
    } else {
        debug_assert_eq!(value, f64::NEG_INFINITY);
        "-Inf"
    }
}

impl GoAnyValue for ColumnDefaultValue {
    fn go_type(&self) -> GoTypeIdentity {
        self.go_type_identity()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        match self {
            Self::Int(value) => Ok(GoJsonProjection::Value(GoJsonValue::Int(*value))),
            Self::Uint(value) => Ok(GoJsonProjection::Value(GoJsonValue::Uint(*value))),
            Self::Byte(value) => Ok(GoJsonProjection::Value(GoJsonValue::Uint(u64::from(
                *value,
            )))),
            Self::Float(value) if value.is_finite() => {
                Ok(GoJsonProjection::Value(GoJsonValue::Float(*value)))
            }
            Self::Float(value) => Err(GoAnyJsonError::unsupported_value(
                self.go_type_identity(),
                go_non_finite_float_description(*value),
            )),
            Self::Bool(value) => Ok(GoJsonProjection::Value(GoJsonValue::Bool(*value))),
            Self::Str(value) => Ok(GoJsonProjection::Value(GoJsonValue::String(value.clone()))),
            Self::DefinedString(value) => Ok(GoJsonProjection::Value(GoJsonValue::String(
                value.value.clone(),
            ))),
            Self::Bytes(value) => Ok(GoJsonProjection::Value(GoJsonValue::Bytes(
                value.bytes.is_allocated().then(|| value.bytes.snapshot()),
            ))),
            Self::Slice(value) if !value.values.is_allocated() => {
                Ok(GoJsonProjection::Value(GoJsonValue::Null))
            }
            Self::Slice(value) => Ok(GoJsonProjection::InterfaceSlice(value.clone())),
            Self::Map(value) if !value.is_allocated() => {
                Ok(GoJsonProjection::Value(GoJsonValue::Null))
            }
            Self::Map(value) => Ok(GoJsonProjection::InterfaceMap(value.clone())),
            Self::Pointer(value) if value.value.is_none() => {
                Ok(GoJsonProjection::Value(GoJsonValue::Null))
            }
            Self::Pointer(value) => Ok(GoJsonProjection::InterfacePointer(value.clone())),
            Self::Array(value) => Ok(GoJsonProjection::Array(value.values.clone())),
            Self::Struct(value) => Ok(GoJsonProjection::Struct(value.fields.clone())),
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
        GoJsonContext::new()
            .project_dynamic(self)
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

        fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
            Ok(GoJsonProjection::Value(GoJsonValue::String(
                GoString::from_bytes(self.0.to_vec()),
            )))
        }

        fn append_go_format(&self, output: &mut Vec<u8>) {
            output.extend_from_slice(format!("hook:{}", self.0[0]).as_bytes());
        }

        fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
            Some(GoEqualityProjection::Opaque(&self.0))
        }
    }

    #[derive(Debug)]
    struct OrderedHook {
        id: u8,
        log: GoShared<Vec<u8>>,
        children: Vec<GoAny>,
    }

    impl GoAnyValue for OrderedHook {
        fn go_type(&self) -> GoTypeIdentity {
            GoTypeIdentity::defined(
                "example.com/hooks",
                "Ordered",
                "hooks.Ordered",
                GoTypeKind::Other,
            )
        }

        fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
            Box::new(Self {
                id: self.id,
                log: self.log.clone(),
                children: self.children.clone(),
            })
        }

        fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
            self.log.write().push(self.id);
            if self.children.is_empty() {
                Ok(GoJsonProjection::Value(GoJsonValue::Uint(u64::from(
                    self.id,
                ))))
            } else {
                Ok(GoJsonProjection::Array(self.children.clone()))
            }
        }

        fn append_go_format(&self, output: &mut Vec<u8>) {
            output.extend_from_slice(self.id.to_string().as_bytes());
        }

        fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
            None
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
        let error = non_finite.go_json_value().unwrap_err();
        assert_eq!(error.kind(), GoAnyJsonErrorKind::UnsupportedValue);
        assert_eq!(
            error.dynamic_type().unwrap(),
            &GoTypeIdentity::unnamed("float64", GoTypeKind::Float64)
        );
        assert_eq!(error.unsupported_value_description(), Some("+Inf"));
        assert_eq!(error.to_string(), "json: unsupported value: +Inf");
        assert!(crate::serde_helpers::to_go_json(&non_finite).is_err());

        let custom = GoAny::new(HookedValue(*b"x"));
        let copied = custom.clone();
        assert!(custom.go_equal(&copied));
        assert_eq!(custom.to_string(), "hook:120");
        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&custom).unwrap()).unwrap(),
            r#""x""#
        );
        assert!(matches!(custom.view(), Some(GoAnyView::Custom)));
    }

    #[test]
    fn json_projection_hooks_remain_depth_first_on_the_work_stack() {
        let log = GoShared::new(Vec::new());
        let leaf = |id| {
            GoAny::new(OrderedHook {
                id,
                log: log.clone(),
                children: Vec::new(),
            })
        };
        let left = GoAny::new(OrderedHook {
            id: 2,
            log: log.clone(),
            children: vec![leaf(4)],
        });
        let root = GoAny::new(OrderedHook {
            id: 1,
            log: log.clone(),
            children: vec![left, leaf(3)],
        });

        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&root).unwrap()).unwrap(),
            "[[4],3]"
        );
        assert_eq!(log.read().as_slice(), &[1, 2, 4, 3]);
    }

    fn assert_cycle_error(error: &GoAnyJsonError, display_type: &str, kind: GoTypeKind) {
        assert_eq!(error.kind(), GoAnyJsonErrorKind::UnsupportedValue);
        let dynamic_type = error.dynamic_type().unwrap();
        assert_eq!(dynamic_type.kind(), kind);
        assert_eq!(dynamic_type.display_name(), display_type);
        let description = format!("encountered a cycle via {display_type}");
        assert_eq!(
            error.unsupported_value_description(),
            Some(description.as_str())
        );
        assert_eq!(
            error.to_string(),
            format!("json: unsupported value: {description}")
        );
    }

    #[test]
    fn json_projection_detects_direct_pointer_and_slice_cycles() {
        let pointer = GoAnyPointer::new(GoAny::nil());
        let pointer_value = GoAny::from(ColumnDefaultValue::Pointer(pointer.clone()));
        let pointee = pointer.pointee().unwrap();
        *pointee.write() = pointer_value.clone();

        let error = pointer_value.go_json_value().unwrap_err();
        assert_cycle_error(&error, "*interface {}", GoTypeKind::Pointer);
        *pointee.write() = GoAny::nil();

        let slice = GoAnySlice::from_values(vec![GoAny::nil()]);
        let slice_value = GoAny::from(ColumnDefaultValue::Slice(slice.clone()));
        slice.set(0, slice_value.clone());

        let error = slice_value.go_json_value().unwrap_err();
        assert_cycle_error(&error, "[]interface {}", GoTypeKind::Slice);
        slice.set(0, GoAny::nil());
    }

    #[test]
    fn json_projection_detects_indirect_map_pointer_cycle() {
        let mut map = GoAnyMap::allocated(Vec::<(String, GoAny)>::new());
        let pointer = GoAnyPointer::new(ColumnDefaultValue::Map(map.clone()).into());
        map.insert(
            "pointer",
            ColumnDefaultValue::Pointer(pointer.clone()).into(),
        );
        let map_value = GoAny::from(ColumnDefaultValue::Map(map.clone()));

        let error = map_value.go_json_value().unwrap_err();
        assert_cycle_error(&error, "map[string]interface {}", GoTypeKind::Map);

        map.insert("pointer", GoAny::nil());
    }

    #[test]
    fn json_projection_allows_shared_acyclic_aliases() {
        let pointer = GoAnyPointer::new(ColumnDefaultValue::Int(1).into());
        let pointer_value = GoAny::from(ColumnDefaultValue::Pointer(pointer));
        let map = GoAnyMap::allocated([("k", ColumnDefaultValue::Int(2).into())]);
        let map_value = GoAny::from(ColumnDefaultValue::Map(map));
        let slice = GoAnySlice::from_values(vec![ColumnDefaultValue::Int(3).into()]);
        let slice_value = GoAny::from(ColumnDefaultValue::Slice(slice));
        let root = GoAny::from(ColumnDefaultValue::Array(GoAnyArray::new(
            GoTypeIdentity::unnamed("[6]interface {}", GoTypeKind::Array),
            vec![
                pointer_value.clone(),
                pointer_value,
                map_value.clone(),
                map_value,
                slice_value.clone(),
                slice_value,
            ],
        )));

        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&root).unwrap()).unwrap(),
            r#"[1,1,{"k":2},{"k":2},[3],[3]]"#
        );
    }

    #[test]
    fn json_projection_removes_tracked_aliases_before_the_next_sibling() {
        let pointer = GoAnyPointer::new(ColumnDefaultValue::Int(1).into());
        let mut branch = GoAny::from(ColumnDefaultValue::Pointer(pointer));
        for _ in 0..=GO_JSON_START_DETECTING_CYCLES_AFTER {
            branch = ColumnDefaultValue::Slice(GoAnySlice::from_values(vec![branch])).into();
        }
        let root = GoAny::from(ColumnDefaultValue::Array(GoAnyArray::new(
            GoTypeIdentity::unnamed("[2]interface {}", GoTypeKind::Array),
            vec![branch.clone(), branch],
        )));

        assert!(root.go_json_value().is_ok());
    }

    #[test]
    fn json_slice_cycle_key_includes_visible_len_after_threshold() {
        let shared = GoAnySlice::from_values(vec![GoAny::nil()]);
        let mut empty_alias = shared.clone();
        empty_alias.clear();
        shared.set(0, ColumnDefaultValue::Slice(empty_alias).into());
        let cleanup = shared.clone();

        let mut root = GoAny::from(ColumnDefaultValue::Slice(shared));
        for _ in 0..GO_JSON_START_DETECTING_CYCLES_AFTER {
            root = ColumnDefaultValue::Slice(GoAnySlice::from_values(vec![root])).into();
        }

        assert!(root.go_json_value().is_ok());
        cleanup.set(0, GoAny::nil());
    }

    fn enter_repeated_reference(
        context: &mut GoJsonContext,
        depth: usize,
    ) -> Result<(), GoAnyJsonError> {
        let reference = GoJsonReference::new(
            GoJsonReferenceIdentity::Pointer(1),
            GoTypeIdentity::unnamed("*interface {}", GoTypeKind::Pointer),
        );
        let mut traversal = GoJsonTraversal { context };
        let mut entered = Vec::with_capacity(depth);
        for _ in 0..depth {
            match traversal.enter_reference(&reference) {
                Ok(seen_reference) => entered.push(seen_reference),
                Err(error) => {
                    while let Some(seen_reference) = entered.pop() {
                        traversal.exit_reference(seen_reference);
                    }
                    return Err(error);
                }
            }
        }
        while let Some(seen_reference) = entered.pop() {
            traversal.exit_reference(seen_reference);
        }
        Ok(())
    }

    #[test]
    fn json_cycle_threshold_and_traversal_cleanup_match_go_1_25() {
        let mut context = GoJsonContext::new();
        assert!(
            enter_repeated_reference(&mut context, GO_JSON_START_DETECTING_CYCLES_AFTER + 1,)
                .is_ok()
        );

        let error =
            enter_repeated_reference(&mut context, GO_JSON_START_DETECTING_CYCLES_AFTER + 2)
                .unwrap_err();
        assert_cycle_error(&error, "*interface {}", GoTypeKind::Pointer);

        assert_eq!(context.reference_depth, 0);
        assert!(context.seen_references.is_empty());
        assert!(enter_repeated_reference(&mut context, 1).is_ok());
    }
}
