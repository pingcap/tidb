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

//! Go `pkg/util/schemacmp/lattice.go`: the join-semilattice vocabulary.
//!
//! Go's `Lattice` interface becomes the [`Lattice`] trait over boxed trait
//! objects; Go's dynamic `interface{}` payloads become the explicit [`Value`]
//! domain. Error messages preserve the exact Go `fmt.Sprintf` renderings of
//! the `ErrMsg*` templates.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use tidb_datatype::{FieldType, GoString};
use tidb_model::go_any::GoAny;
use tidb_model::go_any::GoAnyView;

/// Go `ErrMsgTypeMismatch`.
pub const ERR_MSG_TYPE_MISMATCH: &str = "type mismatch (%T vs %T)";
/// Go `ErrMsgTupleLengthMismatch`.
pub const ERR_MSG_TUPLE_LENGTH_MISMATCH: &str = "tuple length mismatch (%d vs %d)";
/// Go `ErrMsgDistinctSingletons`.
pub const ERR_MSG_DISTINCT_SINGLETONS: &str = "distinct singletons (%v vs %v)";
/// Go `ErrMsgIncompatibleType`.
pub const ERR_MSG_INCOMPATIBLE_TYPE: &str = "incompatible mysql type (%v vs %v)";
/// Go `ErrMsgIncompatibleCharset`.
pub const ERR_MSG_INCOMPATIBLE_CHARSET: &str = "incompatible charset (%v vs %v)";
/// Go `ErrMsgIncompatibleCollation`.
pub const ERR_MSG_INCOMPATIBLE_COLLATION: &str = "incompatible collation (%v vs %v)";
/// Go `ErrMsgAtTupleIndex`.
pub const ERR_MSG_AT_TUPLE_INDEX: &str = "at tuple index %d: %v";
/// Go `ErrMsgAtMapKey`.
pub const ERR_MSG_AT_MAP_KEY: &str = "at map key %q: %v";
/// Go `ErrMsgNonInclusiveBitSets`.
pub const ERR_MSG_NON_INCLUSIVE_BIT_SETS: &str = "non-inclusive bit sets (%#x vs %#x)";
/// Go `ErrMsgContradictingOrders`.
pub const ERR_MSG_CONTRADICTING_ORDERS: &str = "combining contradicting orders (%d && %d)";
/// Go `ErrMsgStringListElemMismatch`.
pub const ERR_MSG_STRING_LIST_ELEM_MISMATCH: &str =
    "at string list index %d: distinct values (%q vs %q)";

/// Go `IncompatibleError`: the error type for incompatible schema.
///
/// Go keeps the `Msg` template plus `Args` and renders through `fmt.Sprintf`;
/// this transcreation renders eagerly at construction, byte-for-byte matching
/// the Go output for every argument shape this package produces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompatibleError {
    message: String,
}

impl IncompatibleError {
    /// Builds an error from an already-rendered message. This is the
    /// counterpart of Go's direct `IncompatibleError{Msg: ...}` literals with
    /// verb-free messages.
    pub(crate) fn raw(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The rendered message, exactly as Go's `Error()` returns it.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for IncompatibleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for IncompatibleError {}

/// Renders a string with Go `%q` (`strconv.Quote`) semantics for the
/// identifier-shaped keys this package quotes.
fn go_quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(crate) fn type_mismatch_error(a: &dyn Lattice, b: &dyn Lattice) -> IncompatibleError {
    IncompatibleError::raw(format!(
        "type mismatch ({} vs {})",
        a.go_type_name(),
        b.go_type_name()
    ))
}

pub(crate) fn tuple_length_mismatch_error(a: usize, b: usize) -> IncompatibleError {
    IncompatibleError::raw(format!("tuple length mismatch ({a} vs {b})"))
}

pub(crate) fn distinct_singletons_error(a: &str, b: &str) -> IncompatibleError {
    IncompatibleError::raw(format!("distinct singletons ({a} vs {b})"))
}

pub(crate) fn incompatible_type_error(a: u8, b: u8) -> IncompatibleError {
    IncompatibleError::raw(format!("incompatible mysql type ({a} vs {b})"))
}

pub(crate) fn incompatible_charset_error(a: &str, b: &str) -> IncompatibleError {
    IncompatibleError::raw(format!("incompatible charset ({a} vs {b})"))
}

pub(crate) fn incompatible_collation_error(a: &str, b: &str) -> IncompatibleError {
    IncompatibleError::raw(format!("incompatible collation ({a} vs {b})"))
}

pub(crate) fn wrap_tuple_index_error(index: usize, inner: &IncompatibleError) -> IncompatibleError {
    IncompatibleError::raw(format!("at tuple index {index}: {inner}"))
}

pub(crate) fn wrap_map_key_error(key: &str, inner: &IncompatibleError) -> IncompatibleError {
    IncompatibleError::raw(format!("at map key {}: {}", go_quote(key), inner))
}

/// Custom equality, mirroring Go's `Equality` interface.
pub trait Equality: fmt::Debug {
    /// Returns true if this instance should be equal to another object.
    fn equals(&self, other: &dyn Equality) -> bool;

    /// Rust downcast hook standing in for Go's dynamic type assertions.
    fn as_any(&self) -> &dyn Any;
}

/// The explicit domain of Go's `interface{}` values flowing through
/// `Unwrap()` and `Singleton`.
///
/// Each variant records the Go dynamic type it stands for, so Go's interface
/// equality (`!=` between distinct dynamic types is "not equal", never an
/// error) is preserved by cross-variant inequality.
#[derive(Clone, Debug)]
pub enum Value {
    /// Go `nil`.
    Nil,
    /// Go `bool`.
    Bool(bool),
    /// Go `int`.
    Int(i64),
    /// Go `int64`.
    Int64(i64),
    /// Go `uint`.
    Uint(u64),
    /// Go `uint64`.
    Uint64(u64),
    /// Go `byte`.
    Byte(u8),
    /// Go `float64`.
    Float64(f64),
    /// Go `string`.
    Str(GoString),
    /// Go `[]string`.
    StringList(Vec<GoString>),
    /// Go `[]interface{}` (a `Tuple`'s unwrapping).
    List(Vec<Value>),
    /// Go `map[string]interface{}` (a lattice map's unwrapping).
    Map(BTreeMap<String, Value>),
    /// Go `*types.FieldType`.
    FieldType(Box<FieldType>),
    /// Go `ast.IndexType`.
    IndexType(tidb_ast::IndexType),
    /// A Go value with custom equality (Go's `Equality` interface).
    Equality(Rc<dyn Equality>),
    /// Any other Go interface value, carried through [`GoAny`].
    Any(GoAny),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Nil, Self::Nil) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) | (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Uint(a), Self::Uint(b)) | (Self::Uint64(a), Self::Uint64(b)) => a == b,
            (Self::Byte(a), Self::Byte(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a == b,
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::StringList(a), Self::StringList(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            (Self::FieldType(a), Self::FieldType(b)) => a == b,
            (Self::IndexType(a), Self::IndexType(b)) => a == b,
            (Self::Equality(a), Self::Equality(b)) => a.equals(b.as_ref()),
            (Self::Any(a), Self::Any(b)) => a.go_equal(b),
            _ => false,
        }
    }
}

impl Value {
    /// Renders this value the way Go's `%v` verb does, for error arguments
    /// and for `DEFAULT` clauses in restored SQL.
    #[must_use]
    pub fn go_format(&self) -> String {
        match self {
            Self::Nil => "<nil>".to_owned(),
            Self::Bool(value) => value.to_string(),
            Self::Int(value) | Self::Int64(value) => value.to_string(),
            Self::Uint(value) | Self::Uint64(value) => value.to_string(),
            Self::Byte(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
            Self::Str(value) => value.to_utf8_lossy_go(),
            Self::IndexType(value) => value.sql().to_owned(),
            Self::Any(value) => value.to_string(),
            other => format!("{other:?}"),
        }
    }

    /// Converts a [`GoAny`] interface payload into this domain, keeping the
    /// built-in shapes comparable with values this package itself produces
    /// (for example a joined column's synthesized string default).
    #[must_use]
    pub fn from_go_any(value: &GoAny) -> Self {
        match value.view() {
            None => Self::Nil,
            Some(GoAnyView::Bool(inner)) => Self::Bool(inner),
            Some(GoAnyView::Int(inner)) => Self::Int64(inner),
            Some(GoAnyView::Uint(inner)) => Self::Uint64(inner),
            Some(GoAnyView::Byte(inner)) => Self::Byte(inner),
            Some(GoAnyView::Float(inner)) => Self::Float64(inner),
            Some(GoAnyView::String(inner)) => Self::Str(inner.clone()),
            Some(_) => Self::Any(value.clone()),
        }
    }
}

/// Go `Lattice`: implemented for types which form a join-semilattice.
pub trait Lattice: fmt::Debug {
    /// Returns the underlying object supporting the lattice. This operation
    /// is deep.
    fn unwrap(&self) -> Value;

    /// Compares this instance with another instance.
    ///
    /// Returns -1 if `self < other`, 0 if `self == other`, 1 if
    /// `self > other`, and [`IncompatibleError`] if the two instances are not
    /// ordered.
    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError>;

    /// Finds the "least upper bound" of two lattice instances. The result is
    /// `>=` both inputs. Returns an error if the join does not exist.
    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError>;

    /// The Go dynamic type name rendered by the `%T` verb in Go's error
    /// messages.
    fn go_type_name(&self) -> &'static str;

    /// Rust downcast hook standing in for Go's dynamic type assertions.
    fn as_any(&self) -> &dyn Any;

    /// Mutable downcast hook: Go mutates shared slice backings in place
    /// (`typ.setAntiKeyFlags`), which owned Rust values express as in-place
    /// mutation instead.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Owned downcast hook for consuming a joined result.
    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    /// Deep copy, standing in for Go's implicit value copies.
    fn clone_lattice(&self) -> Box<dyn Lattice>;
}

impl Clone for Box<dyn Lattice> {
    fn clone(&self) -> Self {
        self.clone_lattice()
    }
}

fn cast<T: 'static>(other: &dyn Lattice) -> Option<&T> {
    other.as_any().downcast_ref::<T>()
}

/// One `iter` visit: the key plus each side's entry, `None` for the side
/// lacking the key.
type MapIterAction<'a> = &'a mut dyn FnMut(
    &str,
    Option<&dyn Lattice>,
    Option<&dyn Lattice>,
) -> Result<(), IncompatibleError>;

/// Go `Bool`: a boolean implementing `Lattice` where `false < true`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bool(pub bool);

impl Lattice for Bool {
    fn unwrap(&self) -> Value {
        Value::Bool(self.0)
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if self == b => Ok(0),
            Some(_) if self.0 => Ok(1),
            Some(_) => Ok(-1),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) => Ok(Box::new(Self(self.0 || b.0))),
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.Bool"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(*self)
    }
}

/// Go's unexported `singleton` struct.
#[derive(Clone, Debug)]
struct Singleton {
    value: Value,
}

/// Go `Singleton`: wraps an unordered value. Distinct instances of
/// `Singleton` are incompatible.
#[must_use]
pub fn singleton(value: Value) -> Box<dyn Lattice> {
    Box::new(Singleton { value })
}

impl Lattice for Singleton {
    fn unwrap(&self) -> Value {
        self.value.clone()
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if self.value != b.value => Err(distinct_singletons_error(
                &self.value.go_format(),
                &b.value.go_format(),
            )),
            Some(_) => Ok(0),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if self.value != b.value => Err(distinct_singletons_error(
                &self.value.go_format(),
                &b.value.go_format(),
            )),
            Some(_) => Ok(self.clone_lattice()),
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.singleton"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go's unexported `equalitySingleton` struct.
#[derive(Clone, Debug)]
struct EqualitySingleton {
    value: Rc<dyn Equality>,
}

/// Go `EqualitySingleton`: wraps an unordered value with equality defined by
/// custom code instead of the `==` operator.
#[must_use]
pub fn equality_singleton(value: Rc<dyn Equality>) -> Box<dyn Lattice> {
    Box::new(EqualitySingleton { value })
}

impl Lattice for EqualitySingleton {
    fn unwrap(&self) -> Value {
        Value::Equality(Rc::clone(&self.value))
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if !self.value.equals(b.value.as_ref()) => Err(distinct_singletons_error(
                &format!("{:?}", self.value),
                &format!("{:?}", b.value),
            )),
            Some(_) => Ok(0),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if !self.value.equals(b.value.as_ref()) => Err(distinct_singletons_error(
                &format!("{:?}", self.value),
                &format!("{:?}", b.value),
            )),
            Some(_) => Ok(self.clone_lattice()),
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.equalitySingleton"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go `BitSet` (a `uint`): a set of bits where `a < b` iff `a` is a subset of
/// `b`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitSet(pub u64);

impl Lattice for BitSet {
    fn unwrap(&self) -> Value {
        Value::Uint(self.0)
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) if self == b => Ok(0),
            Some(b) if self.0 & !b.0 == 0 => Ok(-1),
            Some(b) if b.0 & !self.0 == 0 => Ok(1),
            Some(b) => Err(IncompatibleError::raw(format!(
                "non-inclusive bit sets ({:#x} vs {:#x})",
                self.0, b.0
            ))),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        match cast::<Self>(other) {
            None => Err(type_mismatch_error(self, other)),
            Some(b) => Ok(Box::new(Self(self.0 | b.0))),
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.BitSet"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(*self)
    }
}

macro_rules! ordered_lattice {
    ($(#[$doc:meta])* $name:ident, $inner:ty, $value_variant:ident, $go_name:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(pub $inner);

        impl Lattice for $name {
            fn unwrap(&self) -> Value {
                Value::$value_variant(self.0)
            }

            fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
                match cast::<Self>(other) {
                    None => Err(type_mismatch_error(self, other)),
                    Some(b) if self == b => Ok(0),
                    Some(b) if self > b => Ok(1),
                    Some(_) => Ok(-1),
                }
            }

            fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
                match cast::<Self>(other) {
                    None => Err(type_mismatch_error(self, other)),
                    Some(b) if self >= b => Ok(Box::new(*self)),
                    Some(b) => Ok(Box::new(*b)),
                }
            }

            fn go_type_name(&self) -> &'static str {
                $go_name
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }

            fn into_any(self: Box<Self>) -> Box<dyn Any> {
                self
            }

            fn clone_lattice(&self) -> Box<dyn Lattice> {
                Box::new(*self)
            }
        }
    };
}

ordered_lattice!(
    /// Go `Byte`: a byte implementing `Lattice`.
    Byte,
    u8,
    Byte,
    "schemacmp.Byte"
);
ordered_lattice!(
    /// Go `Int`: an int implementing `Lattice`.
    Int,
    i64,
    Int,
    "schemacmp.Int"
);
ordered_lattice!(
    /// Go `Int64`: an int64 implementing `Lattice`.
    Int64,
    i64,
    Int64,
    "schemacmp.Int64"
);
ordered_lattice!(
    /// Go `Uint`: a uint implementing `Lattice`.
    Uint,
    u64,
    Uint,
    "schemacmp.Uint"
);

/// Go's unexported `fieldTp` struct: a MySQL column field type implementing
/// `Lattice`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FieldTp {
    value: u8,
}

/// Go `FieldTp`: used for the column field type
/// (`github.com/pingcap/tidb/pkg/parser/types.FieldType.Tp`).
#[must_use]
pub fn field_tp(value: u8) -> Box<dyn Lattice> {
    Box::new(FieldTp { value })
}

fn is_type_blob(tp: u8) -> bool {
    // Go `types.IsTypeBlob`.
    tidb_datatype::FieldTypeCode::from_mysql_type(tp).is_type_blob()
}

impl Lattice for FieldTp {
    fn unwrap(&self) -> Value {
        Value::Byte(self.value)
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };

        if self.value == b.value {
            return Ok(0);
        }

        // TODO(from Go): add more comparable type check here.
        if tidb_mysql::util::is_integer_type(self.value)
            && tidb_mysql::util::is_integer_type(b.value)
        {
            // special handle for integer type.
            return Ok(crate::util::compare_mysql_integer_type(self.value, b.value));
        }

        if is_type_blob(self.value) && is_type_blob(b.value) {
            // special handle for blob type.
            return Ok(crate::util::compare_mysql_blob_type(self.value, b.value));
        }

        Err(incompatible_type_error(self.value, b.value))
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };

        if self.value == b.value {
            return Ok(self.clone_lattice());
        }

        if tidb_mysql::util::is_integer_type(self.value)
            && tidb_mysql::util::is_integer_type(b.value)
        {
            // special handle for integer type.
            if crate::util::compare_mysql_integer_type(self.value, b.value) < 0 {
                return Ok(Box::new(*b));
            }
            return Ok(self.clone_lattice());
        }

        if is_type_blob(self.value) && is_type_blob(b.value) {
            // special handle for blob type.
            if crate::util::compare_mysql_blob_type(self.value, b.value) < 0 {
                return Ok(Box::new(*b));
            }
            return Ok(self.clone_lattice());
        }

        Err(incompatible_type_error(self.value, b.value))
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.fieldTp"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(*self)
    }
}

/// Go `Tuple`: a tuple of `Lattice` instances. Given two tuples `a` and `b`,
/// `a <= b` iff `a[i] <= b[i]` for all `i`.
#[derive(Clone, Debug, Default)]
pub struct Tuple(pub Vec<Box<dyn Lattice>>);

/// Go `CombineCompareResult`: combines two comparison results.
pub fn combine_compare_result(x: i32, y: i32) -> Result<i32, IncompatibleError> {
    if x == y || y == 0 {
        Ok(x)
    } else if x == 0 {
        Ok(y)
    } else {
        Err(IncompatibleError::raw(format!(
            "combining contradicting orders ({x} && {y})"
        )))
    }
}

impl Lattice for Tuple {
    fn unwrap(&self) -> Value {
        Value::List(self.0.iter().map(|value| value.unwrap()).collect())
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };
        if self.0.len() != b.0.len() {
            return Err(tuple_length_mismatch_error(self.0.len(), b.0.len()));
        }

        let mut result = 0;
        for (index, left) in self.0.iter().enumerate() {
            let res = left
                .compare(b.0[index].as_ref())
                .map_err(|error| wrap_tuple_index_error(index, &error))?;
            result = combine_compare_result(result, res)
                .map_err(|error| wrap_tuple_index_error(index, &error))?;
        }
        Ok(result)
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };
        if self.0.len() != b.0.len() {
            return Err(tuple_length_mismatch_error(self.0.len(), b.0.len()));
        }

        let mut result = Vec::with_capacity(self.0.len());
        for (index, left) in self.0.iter().enumerate() {
            let res = left
                .join(b.0[index].as_ref())
                .map_err(|error| wrap_tuple_index_error(index, &error))?;
            result.push(res);
        }
        Ok(Box::new(Self(result)))
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.Tuple"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go's unexported `maybe` struct.
#[derive(Clone, Debug)]
struct Maybe(Option<Box<dyn Lattice>>);

/// Go `Maybe`: includes `nil` as the universal lower bound of the original
/// lattice.
#[must_use]
pub fn maybe(inner: Option<Box<dyn Lattice>>) -> Box<dyn Lattice> {
    Box::new(Maybe(inner))
}

/// Go `MaybeSingletonInterface`: a convenient function calling
/// `Maybe(Singleton(value))`.
#[must_use]
pub fn maybe_singleton_interface(value: &GoAny) -> Box<dyn Lattice> {
    if value.is_nil() {
        return maybe(None);
    }
    maybe(Some(singleton(Value::from_go_any(value))))
}

/// Go `MaybeSingletonString`: a convenient function calling
/// `Maybe(Singleton(s))`.
#[must_use]
pub fn maybe_singleton_string(s: &str) -> Box<dyn Lattice> {
    if s.is_empty() {
        return maybe(None);
    }
    maybe(Some(singleton(Value::Str(GoString::from(s)))))
}

impl Lattice for Maybe {
    fn unwrap(&self) -> Value {
        match &self.0 {
            Some(inner) => inner.unwrap(),
            None => Value::Nil,
        }
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };
        match (&self.0, &b.0) {
            (None, None) => Ok(0),
            (None, Some(_)) => Ok(-1),
            (Some(_), None) => Ok(1),
            (Some(left), Some(right)) => left.compare(right.as_ref()),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };
        match (&self.0, &b.0) {
            (None, _) => Ok(b.clone_lattice()),
            (_, None) => Ok(self.clone_lattice()),
            (Some(left), Some(right)) => {
                let join = left.join(right.as_ref())?;
                Ok(Box::new(Self(Some(join))))
            }
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.maybe"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go `StringList`: a list of string where `a <= b` iff `a == b[:len(a)]`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StringList(pub Vec<GoString>);

impl Lattice for StringList {
    fn unwrap(&self) -> Value {
        Value::StringList(self.0.clone())
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };
        let min_len = self.0.len().min(b.0.len());
        for index in 0..min_len {
            if self.0[index] != b.0[index] {
                return Err(IncompatibleError::raw(format!(
                    "at string list index {}: distinct values ({} vs {})",
                    index,
                    go_quote(&self.0[index].to_utf8_lossy_go()),
                    go_quote(&b.0[index].to_utf8_lossy_go()),
                )));
            }
        }
        match self.0.len().cmp(&b.0.len()) {
            std::cmp::Ordering::Equal => Ok(0),
            std::cmp::Ordering::Less => Ok(-1),
            std::cmp::Ordering::Greater => Ok(1),
        }
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let cmp = self.compare(other)?;
        if cmp <= 0 {
            Ok(other.clone_lattice())
        } else {
            Ok(self.clone_lattice())
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.StringList"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go `LatticeMap`: a map of `Lattice` objects keyed by strings.
pub trait LatticeMap: fmt::Debug {
    /// Creates an empty map of the same type as the receiver (Go `New`).
    fn new_empty(&self) -> Box<dyn LatticeMap>;

    /// Inserts a key-value pair into the map (Go `Insert`).
    fn insert(&mut self, key: &str, value: Box<dyn Lattice>);

    /// Obtains the lattice object at the given key; `None` if the key does
    /// not exist (Go `Get` returning nil).
    fn get(&self, key: &str) -> Option<Box<dyn Lattice>>;

    /// Iterates the map (Go `ForEach`).
    fn for_each(
        &self,
        action: &mut dyn FnMut(&str, &dyn Lattice) -> Result<(), IncompatibleError>,
    ) -> Result<(), IncompatibleError>;

    /// Returns the comparison result when the value is compared with a
    /// non-existing entry (Go `CompareWithNil`).
    fn compare_with_nil(&self, value: &dyn Lattice) -> Result<i32, IncompatibleError>;

    /// Returns the result when the value is joined with a non-existing entry;
    /// `Ok(None)` means the joined result should be non-existing (Go
    /// `JoinWithNil` returning nil, nil).
    fn join_with_nil(
        &self,
        value: &dyn Lattice,
    ) -> Result<Option<Box<dyn Lattice>>, IncompatibleError>;

    /// Returns true if two incompatible entries should be deleted instead of
    /// propagating the error (Go `ShouldDeleteIncompatibleJoin`).
    fn should_delete_incompatible_join(&self) -> bool;

    /// Rust downcast hook standing in for Go's dynamic type assertions.
    fn as_any(&self) -> &dyn Any;

    /// Mutable downcast hook (see [`Lattice::as_any_mut`]).
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Deep copy, standing in for Go's implicit value copies.
    fn clone_map(&self) -> Box<dyn LatticeMap>;
}

/// Go's unexported `latticeMap` wrapper struct.
#[derive(Debug)]
pub(crate) struct MapLattice {
    pub(crate) inner: Box<dyn LatticeMap>,
}

impl Clone for MapLattice {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_map(),
        }
    }
}

/// Go `Map`: wraps a `LatticeMap` instance into a `Lattice`.
#[must_use]
pub fn map_lattice(inner: Box<dyn LatticeMap>) -> Box<dyn Lattice> {
    Box::new(MapLattice { inner })
}

impl MapLattice {
    /// Go `latticeMap.iter`: visits the union of both key sets, passing
    /// `None` for the side that lacks the key.
    fn iter(
        &self,
        other: &dyn Lattice,
        action: MapIterAction<'_>,
    ) -> Result<(), IncompatibleError> {
        let Some(b) = cast::<Self>(other) else {
            return Err(type_mismatch_error(self, other));
        };

        let mut visited_keys = std::collections::BTreeSet::new();
        self.inner.for_each(&mut |key, av| {
            visited_keys.insert(key.to_owned());
            let bv = b.inner.get(key);
            action(key, Some(av), bv.as_deref())
        })?;

        b.inner.for_each(&mut |key, bv| {
            if visited_keys.contains(key) {
                return Ok(());
            }
            action(key, None, Some(bv))
        })
    }
}

impl Lattice for MapLattice {
    fn unwrap(&self) -> Value {
        let mut result = BTreeMap::new();
        // TODO(from Go): add err handle
        let _ = self.inner.for_each(&mut |key, value| {
            result.insert(key.to_owned(), value.unwrap());
            Ok(())
        });
        Value::Map(result)
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let mut result = 0;
        self.iter(other, &mut |key, av, bv| {
            let cmp_res = match (av, bv) {
                (Some(av), Some(bv)) => av.compare(bv),
                (Some(av), None) => self.inner.compare_with_nil(av),
                (None, bv) => self
                    .inner
                    .compare_with_nil(bv.expect("iter passes at least one side"))
                    .map(|res| -res),
                // Unreachable: `iter` always passes at least one side.
            }
            .map_err(|error| wrap_map_key_error(key, &error))?;
            result = combine_compare_result(result, cmp_res)
                .map_err(|error| wrap_map_key_error(key, &error))?;
            Ok(())
        })?;
        Ok(result)
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let mut result = self.inner.new_empty();
        self.iter(other, &mut |key, av, bv| {
            let join_res = match (av, bv) {
                (Some(av), Some(bv)) => av.join(bv).map(Some),
                (Some(av), None) => self.inner.join_with_nil(av),
                (None, bv) => self
                    .inner
                    .join_with_nil(bv.expect("iter passes at least one side")),
            };
            match join_res {
                Err(error) => {
                    if !self.inner.should_delete_incompatible_join() {
                        return Err(wrap_map_key_error(key, &error));
                    }
                }
                Ok(Some(joined)) => result.insert(key, joined),
                Ok(None) => {}
            }
            Ok(())
        })?;
        Ok(Box::new(Self { inner: result }))
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.latticeMap"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}
