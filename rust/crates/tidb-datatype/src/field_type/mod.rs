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

mod aggregate;
mod value;

use crate::{output_format, Charset, Collation, EvalType};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

pub use aggregate::{agg_field_type, aggregate_eval_type, merge_field_type, set_type_flag};
pub use value::{
    default_field_type_for_value, parser_default_field_type_for_value, FieldTypeValue,
};

/// Parser normalization used for ENUM/SET display length.
pub fn enum_set_display_length(code: FieldTypeCode, elems: &[impl AsRef<str>]) -> i64 {
    enum_set_display_length_from_lengths(code, elems.iter().map(|elem| elem.as_ref().len()))
}

/// Parser normalization used for ENUM/SET display length when the source
/// elements are byte strings rather than Rust UTF-8 strings.
pub fn enum_set_display_length_from_lengths(
    code: FieldTypeCode,
    lengths: impl IntoIterator<Item = usize>,
) -> i64 {
    let lengths = lengths.into_iter().map(|length| length as i64);
    match code {
        FieldTypeCode::Enum => lengths.max().unwrap_or(0),
        FieldTypeCode::Set => {
            let lengths = lengths.collect::<Vec<_>>();
            lengths.iter().sum::<i64>() + lengths.len().saturating_sub(1) as i64
        }
        _ => UNSPECIFIED_LENGTH,
    }
}

/// Source `HasCharset` rule shared by parser syntax and runtime field types.
pub const fn field_type_has_charset(code: FieldTypeCode, flags: u32) -> bool {
    match code {
        FieldTypeCode::Varchar
        | FieldTypeCode::String
        | FieldTypeCode::VarString
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => flags & FieldTypeFlags::BINARY == 0,
        FieldTypeCode::Enum | FieldTypeCode::Set => true,
        _ => false,
    }
}

/// Source sentinel for unspecified field length or decimal scale.
pub const UNSPECIFIED_LENGTH: i64 = -1;
/// MySQL's maximum DECIMAL scale accepted by parser field metadata.
pub const MAX_DECIMAL_SCALE: i64 = 30;
/// MySQL's maximum DECIMAL precision accepted by parser field metadata.
pub const MAX_DECIMAL_WIDTH: i64 = 65;
/// Variable-width storage sentinel from the source package.
pub const VAR_STORAGE_LEN: i64 = -1;

/// MySQL/TiDB field flag bit positions from `pkg/parser/mysql/type.go`.
///
/// The values remain plain `u32` masks so callers can combine source flags
/// without introducing a Rust-only bitflag abstraction.
pub struct FieldTypeFlags;

impl FieldTypeFlags {
    /// Column is declared NOT NULL.
    pub const NOT_NULL: u32 = 1 << 0;
    /// Column participates in a primary key.
    pub const PRI_KEY: u32 = 1 << 1;
    /// Column has a unique-key flag.
    pub const UNIQUE_KEY: u32 = 1 << 2;
    /// Column participates in a non-unique key.
    pub const MULTIPLE_KEY: u32 = 1 << 3;
    /// Field is represented as a BLOB family type.
    pub const BLOB: u32 = 1 << 4;
    /// Numeric field uses unsigned interpretation.
    pub const UNSIGNED: u32 = 1 << 5;
    /// Numeric field uses zero-filled display.
    pub const ZEROFILL: u32 = 1 << 6;
    /// Field uses binary string semantics.
    pub const BINARY: u32 = 1 << 7;
    /// Field is an ENUM.
    pub const ENUM: u32 = 1 << 8;
    /// Column is auto-incrementing.
    pub const AUTO_INCREMENT: u32 = 1 << 9;
    /// Column has timestamp semantics.
    pub const TIMESTAMP: u32 = 1 << 10;
    /// Field is a SET.
    pub const SET: u32 = 1 << 11;
    /// Column has no default value.
    pub const NO_DEFAULT_VALUE: u32 = 1 << 12;
    /// Column updates to the current time automatically.
    pub const ON_UPDATE_NOW: u32 = 1 << 13;
    /// Column is part of a partition key.
    pub const PART_KEY: u32 = 1 << 14;
    /// Numeric field flag alias used by the source.
    pub const NUM: u32 = 1 << 15;
    /// Group flag alias sharing the NUM bit.
    pub const GROUP: u32 = 1 << 15;
    /// Field has a unique constraint.
    pub const UNIQUE: u32 = 1 << 16;
    /// Binary comparison is requested.
    pub const BIN_CMP: u32 = 1 << 17;
    /// Parser should convert the field to JSON.
    pub const PARSE_TO_JSON: u32 = 1 << 18;
    /// Field represents a boolean value.
    pub const IS_BOOLEAN: u32 = 1 << 19;
    /// Prevent implicit NULL insertion.
    pub const PREVENT_NULL_INSERT: u32 = 1 << 20;
    /// ENUM/SET values may be represented as integers.
    pub const ENUM_SET_AS_INT: u32 = 1 << 21;
    /// Index metadata marks this field for removal.
    pub const DROP_COLUMN_INDEX: u32 = 1 << 22;
    /// Field is generated from an expression.
    pub const GENERATED_COLUMN: u32 = 1 << 23;
    /// Charset was introduced with an underscore introducer.
    pub const UNDERSCORE_CHARSET: u32 = 1 << 24;
}

/// MySQL field type codes needed by the currently supported datum domain.
///
/// The names follow `pkg/parser/mysql/type.go`. `UInt` is intentionally not a
/// separate code: Go represents it as `TypeLonglong` plus `UnsignedFlag`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldTypeCode {
    /// MySQL `TypeUnspecified`.
    Unspecified,
    /// MySQL `TypeTiny`.
    Tiny,
    /// MySQL `TypeShort`.
    Short,
    /// MySQL `TypeLong`.
    Long,
    /// MySQL `TypeFloat`.
    Float,
    /// MySQL `TypeTimestamp`.
    Timestamp,
    /// MySQL `TypeInt24`.
    Int24,
    /// MySQL `TypeDate`.
    Date,
    /// MySQL `TypeDuration` (`TIME`).
    Duration,
    /// MySQL `TypeDatetime`.
    Datetime,
    /// MySQL `TypeYear`.
    Year,
    /// MySQL `TypeNewDate`.
    NewDate,
    /// MySQL `TypeVarchar`.
    Varchar,
    /// MySQL `TypeBit`.
    Bit,
    /// MySQL `TypeJSON`.
    Json,
    /// MySQL `TypeEnum`.
    Enum,
    /// MySQL `TypeSet`.
    Set,
    /// MySQL `TypeTinyBlob`.
    TinyBlob,
    /// MySQL `TypeMediumBlob`.
    MediumBlob,
    /// MySQL `TypeLongBlob`.
    LongBlob,
    /// MySQL `TypeString` (`CHAR`).
    String,
    /// MySQL `TypeGeometry`.
    Geometry,
    /// TiDB `TypeTiDBVectorFloat32`.
    VectorFloat32,
    /// MySQL `TypeNull`.
    Null,
    /// MySQL `TypeLonglong`; unsignedness remains a flag.
    LongLong,
    /// MySQL `TypeDouble`.
    Double,
    /// MySQL `TypeNewDecimal`.
    NewDecimal,
    /// MySQL `TypeVarString`.
    VarString,
    /// MySQL `TypeBlob`, including text when paired with a character collation.
    Blob,
    /// A type byte that is not yet represented by a named source type.
    Unknown(u8),
}

impl FieldTypeCode {
    /// Converts the source MySQL type byte into a typed Rust code.
    pub const fn from_mysql_type(tp: u8) -> Self {
        match tp {
            0 => Self::Unspecified,
            1 => Self::Tiny,
            2 => Self::Short,
            3 => Self::Long,
            4 => Self::Float,
            5 => Self::Double,
            6 => Self::Null,
            7 => Self::Timestamp,
            8 => Self::LongLong,
            9 => Self::Int24,
            10 => Self::Date,
            11 => Self::Duration,
            12 => Self::Datetime,
            13 => Self::Year,
            14 => Self::NewDate,
            15 => Self::Varchar,
            16 => Self::Bit,
            0xE1 => Self::VectorFloat32,
            0xF5 => Self::Json,
            0xF6 => Self::NewDecimal,
            0xF7 => Self::Enum,
            0xF8 => Self::Set,
            0xF9 => Self::TinyBlob,
            0xFA => Self::MediumBlob,
            0xFB => Self::LongBlob,
            0xFC => Self::Blob,
            0xFD => Self::VarString,
            0xFE => Self::String,
            0xFF => Self::Geometry,
            other => Self::Unknown(other),
        }
    }

    /// Returns the source MySQL type byte, preserving unknown values.
    pub const fn mysql_type(self) -> u8 {
        match self {
            Self::Unspecified => 0,
            Self::Tiny => 1,
            Self::Short => 2,
            Self::Long => 3,
            Self::Float => 4,
            Self::Double => 5,
            Self::Null => 6,
            Self::Timestamp => 7,
            Self::LongLong => 8,
            Self::Int24 => 9,
            Self::Date => 10,
            Self::Duration => 11,
            Self::Datetime => 12,
            Self::Year => 13,
            Self::NewDate => 14,
            Self::Varchar => 15,
            Self::Bit => 16,
            Self::VectorFloat32 => 0xE1,
            Self::Json => 0xF5,
            Self::NewDecimal => 0xF6,
            Self::Enum => 0xF7,
            Self::Set => 0xF8,
            Self::TinyBlob => 0xF9,
            Self::MediumBlob => 0xFA,
            Self::LongBlob => 0xFB,
            Self::Blob => 0xFC,
            Self::VarString => 0xFD,
            Self::String => 0xFE,
            Self::Geometry => 0xFF,
            Self::Unknown(other) => other,
        }
    }

    /// Returns Go `mysql.GetDefaultFieldLengthAndDecimal` metadata.
    pub const fn default_length_and_decimal(self) -> (i64, i64) {
        match self {
            Self::Bit => (1, 0),
            Self::Tiny => (4, 0),
            Self::Short => (6, 0),
            Self::Int24 => (9, 0),
            Self::Long => (11, 0),
            Self::LongLong => (20, 0),
            Self::Double => (22, -1),
            Self::Float => (12, -1),
            Self::NewDecimal => (10, 0),
            Self::Duration => (10, 0),
            Self::Date => (10, 0),
            Self::Timestamp => (19, 0),
            Self::Datetime => (19, 0),
            Self::Year => (4, 0),
            Self::String => (1, 0),
            Self::Varchar | Self::VarString => (5, 0),
            Self::TinyBlob => (255, 0),
            Self::Blob => (65_535, 0),
            Self::MediumBlob => (16_777_215, 0),
            Self::LongBlob | Self::Json => (4_294_967_295, 0),
            Self::Null => (0, 0),
            Self::Enum | Self::Set => (-1, 0),
            Self::Unspecified
            | Self::NewDate
            | Self::Geometry
            | Self::VectorFloat32
            | Self::Unknown(_) => (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH),
        }
    }

    /// Returns Go `mysql.GetDefaultFieldLengthAndDecimalForCast` metadata.
    pub const fn default_length_and_decimal_for_cast(self) -> (i64, i64) {
        match self {
            Self::String => (0, -1),
            Self::Date => (10, 0),
            Self::Datetime => (19, 0),
            Self::NewDecimal => (10, 0),
            Self::Duration => (10, 0),
            Self::LongLong => (22, 0),
            Self::Double => (22, -1),
            Self::Float => (12, -1),
            Self::Json => (4_194_304, 0),
            _ => (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH),
        }
    }

    /// Mirrors `pkg/parser/types/field_type.go::IsVarLengthType`.
    pub const fn is_var_length_type(self) -> bool {
        matches!(
            self,
            Self::Varchar
                | Self::VarString
                | Self::Json
                | Self::Blob
                | Self::TinyBlob
                | Self::MediumBlob
                | Self::LongBlob
                | Self::VectorFloat32
        )
    }

    /// Mirrors `pkg/types/etc.go::IsTypeBlob`.
    pub const fn is_type_blob(self) -> bool {
        matches!(
            self,
            Self::TinyBlob | Self::MediumBlob | Self::Blob | Self::LongBlob
        )
    }

    /// Mirrors `pkg/types/etc.go::IsTypeChar`.
    pub const fn is_type_char(self) -> bool {
        matches!(self, Self::String | Self::Varchar)
    }

    /// Mirrors `pkg/parser/types.IsTypeVector`.
    pub const fn is_type_vector(self) -> bool {
        matches!(self, Self::VectorFloat32)
    }

    /// Mirrors `pkg/types/etc.go::IsTypeVarchar`.
    pub const fn is_type_varchar(self) -> bool {
        matches!(self, Self::VarString | Self::Varchar)
    }

    /// Mirrors `pkg/types/etc.go::IsTypeUnspecified`.
    pub const fn is_type_unspecified(self) -> bool {
        matches!(self, Self::Unspecified)
    }

    /// Mirrors `pkg/types/etc.go::IsTypePrefixable`.
    pub const fn is_type_prefixable(self) -> bool {
        self.is_type_blob() || self.is_type_char()
    }

    /// Mirrors `pkg/types/etc.go::IsTypeFractionable`.
    pub const fn is_type_fractionable(self) -> bool {
        matches!(self, Self::Datetime | Self::Duration | Self::Timestamp)
    }

    /// Mirrors `pkg/types/etc.go::IsTypeTime`.
    pub const fn is_type_time(self) -> bool {
        matches!(self, Self::Datetime | Self::Date | Self::Timestamp)
    }

    /// Mirrors `pkg/types/etc.go::IsTypeFloat`.
    pub const fn is_type_float(self) -> bool {
        matches!(self, Self::Float)
    }

    /// Mirrors `pkg/types/etc.go::IsTypeInteger`.
    pub const fn is_type_integer(self) -> bool {
        matches!(
            self,
            Self::Tiny | Self::Short | Self::Int24 | Self::Long | Self::LongLong | Self::Year
        )
    }

    /// Mirrors `pkg/types/etc.go::IsTypeStoredAsInteger`.
    pub const fn is_type_stored_as_integer(self) -> bool {
        self.is_type_integer()
            || matches!(
                self,
                Self::Datetime | Self::Date | Self::Timestamp | Self::Duration
            )
    }

    /// Mirrors `pkg/types/etc.go::IsTypeNumeric`.
    pub const fn is_type_numeric(self) -> bool {
        matches!(
            self,
            Self::Bit
                | Self::Tiny
                | Self::Int24
                | Self::Long
                | Self::LongLong
                | Self::NewDecimal
                | Self::Float
                | Self::Double
                | Self::Short
        )
    }

    /// Mirrors `pkg/types/etc.go::IsTypeTemporal`.
    pub const fn is_type_temporal(self) -> bool {
        matches!(
            self,
            Self::Duration | Self::Datetime | Self::Timestamp | Self::Date | Self::NewDate
        )
    }

    /// Mirrors `pkg/types/etc.go::IsTemporalWithDate`.
    pub const fn is_temporal_with_date(self) -> bool {
        self.is_type_time()
    }

    /// Returns whether Go TiDB classifies this as a string SQL type.
    pub const fn is_string(self) -> bool {
        self.is_type_char()
            || self.is_type_blob()
            || self.is_type_varchar()
            || self.is_type_unspecified()
    }
}

/// The source-backed `FieldType` metadata required to choose binary versus
/// character string signatures during expression construction.
///
/// Charset is derived from the registered collation, so contradictory string
/// metadata cannot be represented. Length, decimal, and raw flags preserve
/// the parser-owned metadata without imposing SQL warning or formatting policy.
#[derive(Debug, Clone)]
pub struct FieldType {
    code: FieldTypeCode,
    flags: u32,
    flen: i64,
    decimal: i64,
    collation: Collation,
    charset_name: String,
    collation_name: String,
    elems: Vec<String>,
    elems_present: bool,
    elems_is_binary_literal: Vec<bool>,
    elems_is_binary_literal_present: bool,
    array: bool,
}

impl PartialEq for FieldType {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
            && self.flags == other.flags
            && self.flen == other.flen
            && self.decimal == other.decimal
            && self.charset_name == other.charset_name
            && self.collation_name == other.collation_name
            && self.elems == other.elems
            && self.elems_is_binary_literal == other.elems_is_binary_literal
            && self.array == other.array
    }
}

impl Eq for FieldType {}

impl Hash for FieldType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.code.mysql_type().hash(state);
        self.flags.hash(state);
        self.flen.hash(state);
        self.decimal.hash(state);
        self.charset_name.hash(state);
        self.collation_name.hash(state);
        self.elems.hash(state);
        self.elems_is_binary_literal.hash(state);
        self.array.hash(state);
    }
}

impl FieldType {
    /// Mirrors `types.NewFieldType` plus `DefaultCharsetForType` for the
    /// supported codes: character strings default to utf8mb4, all other types
    /// default to binary metadata, and parser length/decimal fields start at
    /// [`UNSPECIFIED_LENGTH`].
    pub fn new(code: FieldTypeCode) -> Self {
        let collation = if code.is_type_char() || code.is_type_varchar() {
            Collation::DEFAULT
        } else {
            Collation::Binary
        };
        let (flen, decimal) = if code.is_type_integer() {
            code.default_length_and_decimal()
        } else {
            (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH)
        };
        Self {
            code,
            flags: 0,
            flen,
            decimal,
            collation,
            charset_name: collation.charset().name().to_owned(),
            collation_name: collation.name().to_owned(),
            elems: Vec::new(),
            elems_present: false,
            elems_is_binary_literal: Vec::new(),
            elems_is_binary_literal_present: false,
            array: false,
        }
    }

    /// Mirrors `pkg/parser/types.NewFieldType`, whose charset/collation are empty.
    pub fn parser(code: FieldTypeCode) -> Self {
        Self {
            code,
            flags: 0,
            flen: UNSPECIFIED_LENGTH,
            decimal: UNSPECIFIED_LENGTH,
            collation: Collation::Binary,
            charset_name: String::new(),
            collation_name: String::new(),
            elems: Vec::new(),
            elems_present: false,
            elems_is_binary_literal: Vec::new(),
            elems_is_binary_literal_present: false,
            array: false,
        }
    }

    /// Returns the MySQL field type code represented by this metadata.
    pub const fn code(&self) -> FieldTypeCode {
        if self.array {
            FieldTypeCode::Json
        } else {
            self.code
        }
    }

    /// Returns the element type byte even when this metadata describes an ARRAY.
    pub const fn array_element_code(&self) -> FieldTypeCode {
        self.code
    }

    /// Mirrors `FieldType.SetType`, including clearing the ARRAY marker.
    pub fn set_code(&mut self, code: FieldTypeCode) {
        self.code = code;
        self.array = false;
    }

    /// Mirrors `FieldType.Init`, preserving unrelated metadata.
    pub fn init(&mut self, code: FieldTypeCode) {
        self.code = code;
        self.flen = UNSPECIFIED_LENGTH;
        self.decimal = UNSPECIFIED_LENGTH;
    }

    /// Builder form of `set_code` used by value inference and consumers.
    pub fn with_code(mut self, code: FieldTypeCode) -> Self {
        self.set_code(code);
        self
    }

    /// Returns whether MySQL's `UnsignedFlag` is set.
    pub const fn is_unsigned(&self) -> bool {
        self.has_flag(FieldTypeFlags::UNSIGNED)
    }

    /// Sets or clears MySQL's `UnsignedFlag` equivalent.
    pub const fn with_unsigned(mut self, unsigned: bool) -> Self {
        if unsigned {
            self.flags |= FieldTypeFlags::UNSIGNED;
        } else {
            self.flags &= !FieldTypeFlags::UNSIGNED;
        }
        self
    }

    /// Returns the raw source field flags.
    pub const fn flags(&self) -> u32 {
        self.flags
    }

    /// Returns whether a source flag mask is set.
    pub const fn has_flag(&self, flag: u32) -> bool {
        self.flags & flag != 0
    }

    /// Replaces all source field flags.
    pub const fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Adds source flags with bitwise OR.
    pub const fn with_added_flags(mut self, flags: u32) -> Self {
        self.flags |= flags;
        self
    }

    /// Keeps only source flags selected by the mask with bitwise AND.
    pub const fn with_and_flags(mut self, flags: u32) -> Self {
        self.flags &= flags;
        self
    }

    /// Toggles source flags with bitwise XOR.
    pub const fn with_toggled_flags(mut self, flags: u32) -> Self {
        self.flags ^= flags;
        self
    }

    /// Removes source flags with bitwise AND-NOT.
    pub const fn with_removed_flags(mut self, flags: u32) -> Self {
        self.flags &= !flags;
        self
    }

    /// Returns the parser's display/storage length metadata.
    pub const fn flen(&self) -> i64 {
        self.flen
    }

    /// Returns the parser's decimal/FSP metadata.
    pub const fn decimal(&self) -> i64 {
        self.decimal
    }

    /// Returns default parser metadata for this field's type code.
    pub const fn default_length_and_decimal(&self) -> (i64, i64) {
        self.code().default_length_and_decimal()
    }

    /// Returns default CAST metadata for this field's type code.
    pub const fn default_length_and_decimal_for_cast(&self) -> (i64, i64) {
        self.code().default_length_and_decimal_for_cast()
    }

    /// Replaces the parser's display/storage length metadata.
    pub const fn with_flen(mut self, flen: i64) -> Self {
        self.flen = flen;
        self
    }

    /// Replaces the parser's decimal/FSP metadata.
    pub const fn with_decimal(mut self, decimal: i64) -> Self {
        self.decimal = decimal;
        self
    }

    /// Mirrors `FieldType.IsDecimalValid` for DECIMAL metadata.
    pub const fn is_decimal_valid(&self) -> bool {
        if !matches!(self.code(), FieldTypeCode::NewDecimal) {
            return true;
        }
        self.decimal >= 0
            && self.decimal <= MAX_DECIMAL_SCALE
            && self.flen > 0
            && self.flen <= MAX_DECIMAL_WIDTH
            && self.flen >= self.decimal
    }

    /// Returns the registered collation metadata.
    pub const fn collation(&self) -> Collation {
        self.collation
    }

    /// Derives the field's character set through the collation registry.
    pub fn charset(&self) -> Charset {
        Charset::from_name(&self.charset_name).unwrap_or_else(|| self.collation.charset())
    }

    /// Returns the exact parser-owned charset spelling.
    pub fn charset_name(&self) -> &str {
        &self.charset_name
    }

    /// Returns the exact parser-owned collation spelling.
    pub fn collation_name(&self) -> &str {
        &self.collation_name
    }

    /// Replaces the field's registered collation.
    pub fn with_collation(mut self, collation: Collation) -> Self {
        self.collation = collation;
        self.charset_name = collation.charset().name().to_owned();
        self.collation_name = collation.name().to_owned();
        self
    }

    /// Mirrors `FieldType.SetCharset` and preserves source spelling.
    pub fn with_charset_name(mut self, charset: impl Into<String>) -> Self {
        self.charset_name = charset.into();
        self
    }

    /// Mirrors `FieldType.SetCollate` and preserves source spelling.
    pub fn with_collation_name(mut self, collation: impl Into<String>) -> Self {
        let collation = collation.into();
        if let Some(registered) = Collation::from_name(&collation) {
            self.collation = registered;
        }
        self.collation_name = collation;
        self
    }

    /// Replaces ENUM/SET elements. Go's `SetElems` does not touch the
    /// independently owned, lazily allocated binary-literal marker slice.
    pub fn with_elems(mut self, elems: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.elems = elems.into_iter().map(Into::into).collect();
        self.elems_present = true;
        self
    }

    /// Returns ENUM/SET elements in declaration order.
    pub fn elems(&self) -> &[String] {
        &self.elems
    }

    /// Updates one ENUM/SET element without changing binary-literal markers.
    pub fn set_elem(&mut self, index: usize, element: impl Into<String>) {
        self.elems[index] = element.into();
    }

    /// Returns one ENUM/SET element.
    pub fn elem(&self, index: usize) -> &str {
        &self.elems[index]
    }

    /// Updates an ENUM/SET element and lazily allocates binary-literal flags.
    pub fn set_elem_with_binary_literal(
        &mut self,
        index: usize,
        element: impl Into<String>,
        is_binary_literal: bool,
    ) {
        self.elems[index] = element.into();
        if is_binary_literal {
            if self.elems_is_binary_literal.is_empty() {
                self.elems_is_binary_literal.resize(self.elems.len(), false);
                self.elems_is_binary_literal_present = true;
            }
            self.elems_is_binary_literal[index] = true;
        }
    }

    /// Mirrors `FieldType.GetElemIsBinaryLit`: an absent marker slice returns
    /// false, while a present slice retains Go's indexed access semantics.
    pub fn elem_is_binary_literal(&self, index: usize) -> bool {
        if self.elems_is_binary_literal.is_empty() {
            false
        } else {
            self.elems_is_binary_literal[index]
        }
    }

    /// Clears the lazily allocated element binary-literal flags.
    pub fn clean_elem_binary_literals(&mut self) {
        self.elems_is_binary_literal.clear();
        self.elems_is_binary_literal_present = false;
    }

    /// Marks this metadata as an ARRAY type. `code()` then returns JSON, as Go does.
    pub fn with_array(mut self, array: bool) -> Self {
        self.array = array;
        self
    }

    /// Returns whether this metadata carries the ARRAY marker.
    pub const fn is_array(&self) -> bool {
        self.array
    }

    /// Returns a cloned element type when this is ARRAY metadata.
    pub fn array_type(&self) -> Self {
        let mut clone = self.clone();
        clone.array = false;
        clone
    }

    /// Mirrors parser `FieldType.EvalType` exactly.
    pub const fn eval_type(&self) -> EvalType {
        match self.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Bit
            | FieldTypeCode::Year => EvalType::Int,
            FieldTypeCode::Float | FieldTypeCode::Double => EvalType::Real,
            FieldTypeCode::NewDecimal => EvalType::Decimal,
            FieldTypeCode::Date | FieldTypeCode::Datetime => EvalType::Datetime,
            FieldTypeCode::Timestamp => EvalType::Timestamp,
            FieldTypeCode::Duration => EvalType::Duration,
            FieldTypeCode::Json => EvalType::Json,
            FieldTypeCode::VectorFloat32 => EvalType::VectorFloat32,
            FieldTypeCode::Enum | FieldTypeCode::Set
                if self.flags & FieldTypeFlags::ENUM_SET_AS_INT != 0 =>
            {
                EvalType::Int
            }
            _ => EvalType::String,
        }
    }

    /// Mirrors parser `FieldType.Hybrid`.
    pub const fn is_hybrid(&self) -> bool {
        matches!(
            self.code(),
            FieldTypeCode::Enum | FieldTypeCode::Bit | FieldTypeCode::Set
        )
    }

    /// Mirrors the expression-oriented `FieldType.Equal` rules.
    pub fn equal(&self, other: &Self) -> bool {
        let type_equal = self.code() == other.code()
            || matches!(
                (self.code(), other.code()),
                (FieldTypeCode::Varchar, FieldTypeCode::VarString)
                    | (FieldTypeCode::VarString, FieldTypeCode::Varchar)
            );
        let flen_equal = self.flen == other.flen
            || (self.eval_type() == EvalType::Real && self.decimal == UNSPECIFIED_LENGTH)
            || self.eval_type() == EvalType::Json;
        let ignore_decimal = matches!(self.eval_type(), EvalType::Int | EvalType::String);
        type_equal
            && (ignore_decimal || self.decimal == other.decimal)
            && self.charset_name == other.charset_name
            && self.collation_name == other.collation_name
            && flen_equal
            && self.is_unsigned() == other.is_unsigned()
            && self.elems == other.elems
    }

    /// Mirrors `FieldType.PartialEqual`, including NOT NULL semantics.
    pub fn partial_equal(&self, other: &Self, unsafe_string_length: bool) -> bool {
        if self.has_flag(FieldTypeFlags::NOT_NULL) != other.has_flag(FieldTypeFlags::NOT_NULL) {
            return false;
        }
        if !unsafe_string_length
            || self.eval_type() != EvalType::String
            || other.eval_type() != EvalType::String
        {
            return self.equal(other);
        }
        self.charset_name == other.charset_name
            && self.collation_name == other.collation_name
            && self.is_unsigned() == other.is_unsigned()
            && self.elems == other.elems
    }

    /// Mirrors `FieldType.HasCharset`.
    pub const fn has_charset(&self) -> bool {
        field_type_has_charset(self.code(), self.flags())
    }

    /// Returns whether this is one of the currently ported string SQL types.
    pub const fn is_string(&self) -> bool {
        self.code().is_string()
    }

    /// Mirrors `FieldType.IsVarLengthType`.
    pub const fn is_var_length_type(&self) -> bool {
        self.code().is_var_length_type()
    }

    /// Directly mirrors `pkg/types/etc.go::IsBinaryStr`: a type is a binary
    /// string only when it is a string SQL type whose collation is `binary`.
    pub const fn is_binary_string(&self) -> bool {
        self.is_string() && matches!(self.collation, Collation::Binary)
    }

    /// Returns whether this is a non-binary character string.
    pub const fn is_character_string(&self) -> bool {
        self.is_string() && !self.is_binary_string()
    }

    /// Mirrors `pkg/types/etc.go::NeedRestoredData` with the new-collation
    /// switch enabled, as used by current TiDB storage paths.
    pub const fn need_restored_data(&self) -> bool {
        self.need_restored_data_with_collation(true)
    }

    /// Mirrors `pkg/types/etc.go::NeedRestoredDataWithCollate` for the
    /// collations represented by this dependency leaf.
    pub const fn need_restored_data_with_collation(&self, use_new_collate: bool) -> bool {
        if !use_new_collate || !self.is_character_string() {
            return false;
        }
        self.code().is_type_varchar()
            || !matches!(
                self.collation,
                Collation::Binary
                    | Collation::AsciiBin
                    | Collation::Latin1Bin
                    | Collation::Utf8Bin
                    | Collation::Utf8Mb4Bin
            )
    }

    /// Mirrors `FieldType.SetFlenUnderLimit`.
    pub fn set_flen_under_limit(&mut self, flen: i64) {
        self.flen = if self.code() == FieldTypeCode::NewDecimal {
            flen.min(MAX_DECIMAL_WIDTH)
        } else {
            flen
        };
    }

    /// Mirrors `FieldType.SetDecimalUnderLimit`.
    pub fn set_decimal_under_limit(&mut self, decimal: i64) {
        self.decimal = if self.code() == FieldTypeCode::NewDecimal {
            decimal.min(MAX_DECIMAL_SCALE)
        } else {
            decimal
        };
    }

    /// Mirrors `FieldType.UpdateFlenAndDecimalUnderLimit`.
    pub fn update_flen_and_decimal_under_limit(
        &mut self,
        old: &Self,
        decimal_delta: i64,
        flen_delta: i64,
    ) {
        if self.code() != FieldTypeCode::NewDecimal {
            return;
        }
        if old.decimal < 0 {
            self.decimal = MAX_DECIMAL_SCALE;
        } else {
            self.decimal = old.decimal + decimal_delta;
        }
        self.flen = if old.flen < 0 {
            MAX_DECIMAL_WIDTH
        } else {
            (old.flen
                + flen_delta
                + if old.decimal < 0 {
                    MAX_DECIMAL_SCALE
                } else {
                    0
                })
            .min(MAX_DECIMAL_WIDTH)
        };
    }

    /// Formats the compact information-schema spelling. The boolean is the
    /// source `TiDBStrictIntegerDisplayWidth` switch.
    pub fn compact_str(&self, strict_integer_display_width: bool) -> String {
        let mut suffix = String::new();
        let (default_flen, default_decimal) = self.code().default_length_and_decimal();
        let decimal_not_default = self.decimal != default_decimal
            && self.decimal != 0
            && self.decimal != UNSPECIFIED_LENGTH;
        let display_flen = if self.flen == UNSPECIFIED_LENGTH {
            default_flen
        } else {
            self.flen
        };
        let display_decimal = if self.decimal == UNSPECIFIED_LENGTH {
            default_decimal
        } else {
            self.decimal
        };
        match self.code() {
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                let elems = self
                    .elems
                    .iter()
                    .map(|elem| output_format(elem))
                    .collect::<Vec<_>>()
                    .join("','");
                suffix = format!("('{elems}')");
            }
            FieldTypeCode::Timestamp | FieldTypeCode::Datetime | FieldTypeCode::Duration => {
                if decimal_not_default {
                    suffix = format!("({display_decimal})");
                }
            }
            FieldTypeCode::Double | FieldTypeCode::Float => {
                if decimal_not_default {
                    suffix = format!("({display_flen},{display_decimal})");
                }
            }
            FieldTypeCode::NewDecimal => suffix = format!("({display_flen},{display_decimal})"),
            FieldTypeCode::Bit
            | FieldTypeCode::Varchar
            | FieldTypeCode::String
            | FieldTypeCode::VarString => suffix = format!("({display_flen})"),
            FieldTypeCode::Tiny => {
                if !strict_integer_display_width
                    || self.has_flag(FieldTypeFlags::ZEROFILL)
                    || display_flen == 1
                {
                    suffix = format!("({display_flen})");
                }
            }
            FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if !strict_integer_display_width || self.has_flag(FieldTypeFlags::ZEROFILL) {
                    suffix = format!("({display_flen})");
                }
            }
            FieldTypeCode::Year => suffix = format!("({})", self.flen),
            FieldTypeCode::VectorFloat32 if self.flen != UNSPECIFIED_LENGTH => {
                suffix = format!("({})", self.flen)
            }
            FieldTypeCode::Null => suffix = "(0)".to_owned(),
            _ => {}
        }
        format!("{}{}", type_to_str(self.code(), &self.charset_name), suffix)
    }

    /// Mirrors `FieldType.InfoSchemaStr`.
    pub fn info_schema_str(&self, strict_integer_display_width: bool) -> String {
        let suffix = if self.is_unsigned()
            && !matches!(self.code(), FieldTypeCode::Bit | FieldTypeCode::Year)
        {
            " unsigned"
        } else {
            ""
        };
        format!("{}{suffix}", self.compact_str(strict_integer_display_width))
    }

    /// Mirrors `FieldType.String` with the legacy display-width switch disabled.
    pub fn source_string(&self) -> String {
        let mut parts = vec![self.compact_str(false)];
        if self.is_unsigned() {
            parts.push("UNSIGNED".to_owned());
        }
        if self.has_flag(FieldTypeFlags::ZEROFILL) {
            parts.push("ZEROFILL".to_owned());
        }
        if self.has_flag(FieldTypeFlags::BINARY) && self.code() != FieldTypeCode::String {
            parts.push("BINARY".to_owned());
        }
        if self.code().is_type_char() || self.code().is_type_blob() {
            if !self.charset_name.is_empty() && self.charset_name != "binary" {
                parts.push(format!("CHARACTER SET {}", self.charset_name));
            }
            if !self.collation_name.is_empty() && self.collation_name != "binary" {
                parts.push(format!("COLLATE {}", self.collation_name));
            }
        }
        parts.join(" ")
    }

    /// Restores the field type using Go's default restore flags.
    pub fn restore(&self) -> String {
        let mut output = type_to_str(self.code(), &self.charset_name).to_ascii_uppercase();
        let (precision, scale) = match self.code() {
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                output.push('(');
                for (index, elem) in self.elems.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    output.push('\'');
                    output.push_str(&elem.replace('\'', "''"));
                    output.push('\'');
                }
                output.push(')');
                (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH)
            }
            FieldTypeCode::Timestamp | FieldTypeCode::Datetime | FieldTypeCode::Duration => {
                (self.decimal, UNSPECIFIED_LENGTH)
            }
            FieldTypeCode::Unspecified
            | FieldTypeCode::Float
            | FieldTypeCode::Double
            | FieldTypeCode::NewDecimal => (self.flen, self.decimal),
            _ => (self.flen, UNSPECIFIED_LENGTH),
        };
        if precision != UNSPECIFIED_LENGTH {
            output.push_str(&format!("({precision}"));
            if scale != UNSPECIFIED_LENGTH {
                output.push_str(&format!(",{scale}"));
            }
            output.push(')');
        }
        if self.is_unsigned() {
            output.push_str(" UNSIGNED");
        }
        if self.has_flag(FieldTypeFlags::ZEROFILL) {
            output.push_str(" ZEROFILL");
        }
        if self.has_flag(FieldTypeFlags::BINARY) && self.charset_name != "binary" {
            output.push_str(" BINARY");
        }
        if self.code().is_type_char() || self.code().is_type_blob() {
            if !self.charset_name.is_empty() && self.charset_name != "binary" {
                output.push_str(" CHARACTER SET ");
                output.push_str(&self.charset_name.to_ascii_uppercase());
            }
            if !self.collation_name.is_empty() && self.collation_name != "binary" {
                output.push_str(" COLLATE ");
                output.push_str(&self.collation_name);
            }
        }
        output
    }

    /// Restores the restricted type grammar used by `CAST` expressions.
    pub fn restore_as_cast_type(&self, explicit_charset: bool) -> String {
        let mut output = String::new();
        match self.array_element_code() {
            FieldTypeCode::VarString | FieldTypeCode::String => {
                let binary = self.charset_name == "binary" && self.collation_name == "binary";
                output.push_str(if binary { "BINARY" } else { "CHAR" });
                if self.flen != UNSPECIFIED_LENGTH {
                    output.push_str(&format!("({})", self.flen));
                }
                if explicit_charset && !binary {
                    if self.has_flag(FieldTypeFlags::BINARY) {
                        output.push_str(" BINARY");
                    }
                    if self.charset_name != "binary"
                        && self.charset_name != "utf8mb4"
                        && !self.charset_name.is_empty()
                    {
                        output.push_str(" CHARSET ");
                        output.push_str(&self.charset_name.to_ascii_uppercase());
                    }
                }
            }
            FieldTypeCode::Date => output.push_str("DATE"),
            FieldTypeCode::Datetime => {
                output.push_str("DATETIME");
                if self.decimal > 0 {
                    output.push_str(&format!("({})", self.decimal));
                }
            }
            FieldTypeCode::NewDecimal => {
                output.push_str("DECIMAL");
                if self.flen > 0 && self.decimal > 0 {
                    output.push_str(&format!("({}, {})", self.flen, self.decimal));
                } else if self.flen > 0 {
                    output.push_str(&format!("({})", self.flen));
                }
            }
            FieldTypeCode::Duration => {
                output.push_str("TIME");
                if self.decimal > 0 {
                    output.push_str(&format!("({})", self.decimal));
                }
            }
            FieldTypeCode::LongLong => {
                output.push_str(if self.is_unsigned() {
                    "UNSIGNED"
                } else {
                    "SIGNED"
                });
            }
            FieldTypeCode::Json => output.push_str("JSON"),
            FieldTypeCode::Double => output.push_str("DOUBLE"),
            FieldTypeCode::Float => output.push_str("FLOAT"),
            FieldTypeCode::Year => output.push_str("YEAR"),
            FieldTypeCode::VectorFloat32 => output.push_str("VECTOR"),
            _ => {}
        }
        if self.is_array() {
            output.push_str(" ARRAY");
        }
        output
    }

    /// Returns the source storage-width estimate.
    pub fn storage_length(&self) -> i64 {
        match self.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Double
            | FieldTypeCode::Float
            | FieldTypeCode::Year
            | FieldTypeCode::Duration
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
            | FieldTypeCode::Bit => 8,
            FieldTypeCode::NewDecimal => {
                const DIGITS_TO_BYTES: [i64; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
                let integer = self.flen - self.decimal;
                integer / 9 * 4
                    + DIGITS_TO_BYTES[(integer % 9) as usize]
                    + self.decimal / 9 * 4
                    + DIGITS_TO_BYTES[(self.decimal % 9) as usize]
            }
            _ => VAR_STORAGE_LEN,
        }
    }

    /// Serializes the source JSON field names and values.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&JsonFieldType::from(self))
    }

    /// Deserializes the source JSON representation.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<JsonFieldType>(data).map(Into::into)
    }

    /// Returns Rust-owned memory retained by this value.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.charset_name.capacity()
            + self.collation_name.capacity()
            + self.elems.capacity() * std::mem::size_of::<String>()
            + self.elems.iter().map(String::capacity).sum::<usize>()
            + self.elems_is_binary_literal.capacity() * std::mem::size_of::<bool>()
    }
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct JsonFieldType {
    Tp: u8,
    Flag: u32,
    Flen: i64,
    Decimal: i64,
    Charset: String,
    Collate: String,
    Elems: Option<Vec<String>>,
    ElemsIsBinaryLit: Option<Vec<bool>>,
    Array: bool,
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

impl fmt::Display for FieldType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.source_string())
    }
}

/// Returns the source type label for one code.
pub fn type_str(code: FieldTypeCode) -> &'static str {
    type_to_str(code, "")
}

/// Returns the source type label, applying binary text/blob aliases.
pub fn type_to_str(code: FieldTypeCode, charset: &str) -> &'static str {
    // Go compares against `charset.CharsetBin` exactly; parser-owned spelling
    // is intentionally not normalized here.
    let binary = charset == "binary";
    match code {
        FieldTypeCode::Bit => "bit",
        FieldTypeCode::Blob => {
            if binary {
                "blob"
            } else {
                "text"
            }
        }
        FieldTypeCode::Date => "date",
        FieldTypeCode::Datetime => "datetime",
        FieldTypeCode::Unspecified => "unspecified",
        FieldTypeCode::NewDecimal => "decimal",
        FieldTypeCode::Double => "double",
        FieldTypeCode::Enum => "enum",
        FieldTypeCode::Float => "float",
        FieldTypeCode::Geometry => "geometry",
        FieldTypeCode::VectorFloat32 => "vector",
        FieldTypeCode::Int24 => "mediumint",
        FieldTypeCode::Json => "json",
        FieldTypeCode::Long => "int",
        FieldTypeCode::LongLong => "bigint",
        FieldTypeCode::LongBlob => {
            if binary {
                "longblob"
            } else {
                "longtext"
            }
        }
        FieldTypeCode::MediumBlob => {
            if binary {
                "mediumblob"
            } else {
                "mediumtext"
            }
        }
        FieldTypeCode::Null => {
            if binary {
                "binary"
            } else {
                "null"
            }
        }
        FieldTypeCode::Set => "set",
        FieldTypeCode::Short => "smallint",
        FieldTypeCode::String => {
            if binary {
                "binary"
            } else {
                "char"
            }
        }
        FieldTypeCode::Duration => "time",
        FieldTypeCode::Timestamp => "timestamp",
        FieldTypeCode::Tiny => "tinyint",
        FieldTypeCode::TinyBlob => {
            if binary {
                "tinyblob"
            } else {
                "tinytext"
            }
        }
        FieldTypeCode::Varchar => "varchar",
        FieldTypeCode::VarString => "var_string",
        FieldTypeCode::Year => "year",
        FieldTypeCode::NewDate => "",
        FieldTypeCode::Unknown(_) => "",
    }
}

/// Converts a source type label to its code, including blob/binary aliases.
pub fn str_to_type(label: &str) -> FieldTypeCode {
    let label = label
        .replacen("blob", "text", 1)
        .replacen("binary", "char", 1);
    match label.as_str() {
        "bit" => FieldTypeCode::Bit,
        "text" => FieldTypeCode::Blob,
        "date" => FieldTypeCode::Date,
        "datetime" => FieldTypeCode::Datetime,
        "unspecified" => FieldTypeCode::Unspecified,
        "decimal" => FieldTypeCode::NewDecimal,
        "double" => FieldTypeCode::Double,
        "enum" => FieldTypeCode::Enum,
        "float" => FieldTypeCode::Float,
        "geometry" => FieldTypeCode::Geometry,
        "vector" => FieldTypeCode::VectorFloat32,
        "mediumint" => FieldTypeCode::Int24,
        "json" => FieldTypeCode::Json,
        "int" => FieldTypeCode::Long,
        "bigint" => FieldTypeCode::LongLong,
        "longtext" => FieldTypeCode::LongBlob,
        "mediumtext" => FieldTypeCode::MediumBlob,
        "null" => FieldTypeCode::Null,
        "set" => FieldTypeCode::Set,
        "smallint" => FieldTypeCode::Short,
        "char" => FieldTypeCode::String,
        "time" => FieldTypeCode::Duration,
        "timestamp" => FieldTypeCode::Timestamp,
        "tinyint" => FieldTypeCode::Tiny,
        "tinytext" => FieldTypeCode::TinyBlob,
        "varchar" => FieldTypeCode::Varchar,
        "var_string" => FieldTypeCode::VarString,
        "year" => FieldTypeCode::Year,
        _ => FieldTypeCode::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FieldType, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_SCALE, MAX_DECIMAL_WIDTH,
        UNSPECIFIED_LENGTH,
    };
    use crate::{Charset, Collation};

    /// Source: `pkg/types/field_type.go::DefaultCharsetForType` and
    /// `pkg/types/etc_test.go::TestIsBinaryStr`.
    #[test]
    fn binary_signature_selection_uses_string_type_and_collation() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        assert_eq!(integer.collation(), Collation::Binary);
        assert!(!integer.is_binary_string());

        let binary = FieldType::new(FieldTypeCode::Blob);
        assert!(binary.is_binary_string());
        assert_eq!(binary.charset(), Charset::Binary);

        let character = FieldType::new(FieldTypeCode::VarString);
        assert!(character.is_character_string());
        assert_eq!(character.collation(), Collation::Utf8Mb4Bin);
        assert_eq!(character.charset(), Charset::Utf8Mb4);

        // A blob type with a character collation is text in Go; the SQL type
        // name alone does not select the binary CHAR_LENGTH signature.
        let text = FieldType::new(FieldTypeCode::Blob).with_collation(Collation::Utf8Mb4Bin);
        assert!(text.is_character_string());
    }

    /// Source: the supported scalar rows in
    /// `pkg/types/field_type_test.go::TestDefaultTypeForValue`.
    #[test]
    fn go_supported_scalar_type_metadata_vectors() {
        let vectors = [
            (FieldTypeCode::Null, false, Collation::Binary),
            (FieldTypeCode::LongLong, false, Collation::Binary),
            (FieldTypeCode::Double, false, Collation::Binary),
            (FieldTypeCode::NewDecimal, false, Collation::Binary),
            (FieldTypeCode::VarString, false, Collation::Utf8Mb4Bin),
            (FieldTypeCode::Blob, false, Collation::Binary),
        ];

        for (code, unsigned, collation) in vectors {
            let field_type = FieldType::new(code);
            assert_eq!(field_type.code(), code);
            assert_eq!(field_type.is_unsigned(), unsigned);
            assert_eq!(field_type.collation(), collation);
        }

        let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert!(unsigned.is_unsigned());
    }

    /// Source: `pkg/types/etc.go` and `pkg/types/etc_test.go`.
    ///
    /// Keep these partitions explicit: they are the exact type-byte tables
    /// used by the Go predicates, rather than a Rust-centric approximation.
    #[test]
    fn source_type_predicates_match_go_tables() {
        let all = [
            FieldTypeCode::Unspecified,
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Long,
            FieldTypeCode::Float,
            FieldTypeCode::Double,
            FieldTypeCode::Null,
            FieldTypeCode::Timestamp,
            FieldTypeCode::LongLong,
            FieldTypeCode::Int24,
            FieldTypeCode::Date,
            FieldTypeCode::Duration,
            FieldTypeCode::Datetime,
            FieldTypeCode::Year,
            FieldTypeCode::NewDate,
            FieldTypeCode::Varchar,
            FieldTypeCode::Bit,
            FieldTypeCode::Json,
            FieldTypeCode::NewDecimal,
            FieldTypeCode::Enum,
            FieldTypeCode::Set,
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::LongBlob,
            FieldTypeCode::Blob,
            FieldTypeCode::VarString,
            FieldTypeCode::String,
            FieldTypeCode::Geometry,
            FieldTypeCode::VectorFloat32,
            FieldTypeCode::Unknown(0xdd),
        ];
        let varchar = [FieldTypeCode::VarString, FieldTypeCode::Varchar];
        let blob = [
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::Blob,
            FieldTypeCode::LongBlob,
        ];
        let char_types = [FieldTypeCode::String, FieldTypeCode::Varchar];
        let prefixable = [
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::Blob,
            FieldTypeCode::LongBlob,
            FieldTypeCode::String,
            FieldTypeCode::Varchar,
        ];
        let fractionable = [
            FieldTypeCode::Datetime,
            FieldTypeCode::Duration,
            FieldTypeCode::Timestamp,
        ];
        let time = [
            FieldTypeCode::Datetime,
            FieldTypeCode::Date,
            FieldTypeCode::Timestamp,
        ];
        let integer = [
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
            FieldTypeCode::Year,
        ];
        let stored_as_integer = [
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
            FieldTypeCode::Year,
            FieldTypeCode::Datetime,
            FieldTypeCode::Date,
            FieldTypeCode::Timestamp,
            FieldTypeCode::Duration,
        ];
        let numeric = [
            FieldTypeCode::Bit,
            FieldTypeCode::Tiny,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
            FieldTypeCode::NewDecimal,
            FieldTypeCode::Float,
            FieldTypeCode::Double,
            FieldTypeCode::Short,
        ];
        let temporal = [
            FieldTypeCode::Duration,
            FieldTypeCode::Datetime,
            FieldTypeCode::Timestamp,
            FieldTypeCode::Date,
            FieldTypeCode::NewDate,
        ];
        let strings = [
            FieldTypeCode::String,
            FieldTypeCode::Varchar,
            FieldTypeCode::VarString,
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::Blob,
            FieldTypeCode::LongBlob,
            FieldTypeCode::Unspecified,
        ];

        for code in all {
            assert_eq!(
                FieldTypeCode::from_mysql_type(code.mysql_type()),
                code,
                "type round trip for {code:?}"
            );
            assert_eq!(code.is_type_blob(), blob.contains(&code));
            assert_eq!(code.is_type_char(), char_types.contains(&code));
            assert_eq!(code.is_type_varchar(), varchar.contains(&code));
            assert_eq!(code.is_type_prefixable(), prefixable.contains(&code));
            assert_eq!(code.is_type_fractionable(), fractionable.contains(&code));
            assert_eq!(code.is_type_time(), time.contains(&code));
            assert_eq!(code.is_type_float(), code == FieldTypeCode::Float);
            assert_eq!(code.is_type_integer(), integer.contains(&code));
            assert_eq!(
                code.is_type_stored_as_integer(),
                stored_as_integer.contains(&code)
            );
            assert_eq!(code.is_type_numeric(), numeric.contains(&code));
            assert_eq!(code.is_type_temporal(), temporal.contains(&code));
            assert_eq!(code.is_temporal_with_date(), time.contains(&code));
            assert_eq!(code.is_string(), strings.contains(&code));
        }
        assert!(FieldTypeCode::Unspecified.is_type_unspecified());
        assert!(!FieldTypeCode::String.is_type_unspecified());
    }

    /// Source: `pkg/types/etc_test.go::TestIsBinaryStr` and
    /// `pkg/types/etc_test.go::TestIsNonBinaryStr`.
    #[test]
    fn source_binary_and_non_binary_string_rows() {
        let bit = FieldType::new(FieldTypeCode::Bit)
            .with_collation(Collation::Utf8Bin)
            .with_unsigned(true);
        assert!(!bit.is_binary_string());
        assert!(!bit.is_character_string());

        let binary_blob = FieldType::new(FieldTypeCode::Blob);
        assert!(binary_blob.is_binary_string());
        assert!(!binary_blob.is_character_string());

        let text_blob = FieldType::new(FieldTypeCode::Blob).with_collation(Collation::Utf8Bin);
        assert!(!text_blob.is_binary_string());
        assert!(text_blob.is_character_string());

        let binary_varchar =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        assert!(binary_varchar.is_binary_string());
        let text_varchar =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4GeneralCi);
        assert!(text_varchar.is_character_string());
    }

    /// Source: `pkg/types/etc_test.go::TestNeedRestoredData`.
    #[test]
    fn source_need_restored_data_rows() {
        let rows = [
            (FieldTypeCode::String, Collation::Binary, false),
            (FieldTypeCode::VarString, Collation::Binary, false),
            (FieldTypeCode::String, Collation::Utf8Mb4Bin, false),
            (FieldTypeCode::VarString, Collation::Utf8Mb4Bin, true),
            (FieldTypeCode::String, Collation::Utf8Mb4GeneralCi, true),
            (FieldTypeCode::VarString, Collation::Utf8Mb4GeneralCi, true),
            (FieldTypeCode::String, Collation::Utf8Mb4UnicodeCi, true),
            (FieldTypeCode::VarString, Collation::Utf8Mb4UnicodeCi, true),
            (FieldTypeCode::String, Collation::Utf8Bin, false),
            (FieldTypeCode::VarString, Collation::Utf8Bin, true),
        ];
        for (code, collation, expected) in rows {
            let field_type = FieldType::new(code).with_collation(collation);
            assert_eq!(field_type.need_restored_data(), expected);
            assert!(!field_type.need_restored_data_with_collation(false));
        }
    }

    /// Source: `pkg/parser/types/field_type_test.go::TestFieldType` and
    /// `pkg/parser/mysql/util.go::GetDefaultFieldLengthAndDecimal`.
    #[test]
    fn field_type_metadata_flags_lengths_and_decimal_rows() {
        let duration = FieldType::new(FieldTypeCode::Duration);
        assert_eq!(duration.flen(), UNSPECIFIED_LENGTH);
        assert_eq!(duration.decimal(), UNSPECIFIED_LENGTH);
        assert!(duration.is_decimal_valid());
        let duration = duration.with_decimal(5);
        assert_eq!(duration.decimal(), 5);

        let flagged = FieldType::new(FieldTypeCode::Long)
            .with_flen(5)
            .with_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL);
        assert_eq!(flagged.flen(), 5);
        assert!(flagged.is_unsigned());
        assert!(flagged.has_flag(FieldTypeFlags::ZEROFILL));
        assert_eq!(
            flagged
                .clone()
                .with_removed_flags(FieldTypeFlags::ZEROFILL)
                .flags(),
            FieldTypeFlags::UNSIGNED
        );
        assert_eq!(
            flagged
                .clone()
                .with_toggled_flags(FieldTypeFlags::UNSIGNED)
                .flags(),
            FieldTypeFlags::ZEROFILL
        );
        assert_eq!(
            flagged.with_and_flags(FieldTypeFlags::ZEROFILL).flags(),
            FieldTypeFlags::ZEROFILL
        );

        let defaults = [
            (FieldTypeCode::Bit, (1, 0)),
            (FieldTypeCode::Tiny, (4, 0)),
            (FieldTypeCode::Long, (11, 0)),
            (FieldTypeCode::LongLong, (20, 0)),
            (FieldTypeCode::Float, (12, -1)),
            (FieldTypeCode::Double, (22, -1)),
            (FieldTypeCode::NewDecimal, (10, 0)),
            (FieldTypeCode::Duration, (10, 0)),
            (FieldTypeCode::Varchar, (5, 0)),
            (FieldTypeCode::Blob, (65_535, 0)),
            (FieldTypeCode::Json, (4_294_967_295, 0)),
            (FieldTypeCode::Enum, (-1, 0)),
        ];
        for (code, expected) in defaults {
            assert_eq!(code.default_length_and_decimal(), expected);
            assert_eq!(FieldType::new(code).default_length_and_decimal(), expected);
        }
        assert_eq!(
            FieldTypeCode::String.default_length_and_decimal_for_cast(),
            (0, -1)
        );
        assert_eq!(
            FieldTypeCode::Json.default_length_and_decimal_for_cast(),
            (4_194_304, 0)
        );
        assert_eq!(
            FieldTypeCode::Unknown(0xdd).default_length_and_decimal(),
            (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH)
        );

        assert!(FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(10)
            .with_decimal(2)
            .is_decimal_valid());
        assert!(!FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(MAX_DECIMAL_WIDTH + 1)
            .with_decimal(2)
            .is_decimal_valid());
        assert!(!FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(10)
            .with_decimal(MAX_DECIMAL_SCALE + 1)
            .is_decimal_valid());
        assert!(FieldType::new(FieldTypeCode::Long)
            .with_flen(0)
            .with_decimal(-100)
            .is_decimal_valid());

        let variable = [
            FieldTypeCode::Varchar,
            FieldTypeCode::VarString,
            FieldTypeCode::Json,
            FieldTypeCode::Blob,
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::LongBlob,
            FieldTypeCode::VectorFloat32,
        ];
        for code in variable {
            assert!(code.is_var_length_type());
            assert!(FieldType::new(code).is_var_length_type());
        }
        assert!(!FieldTypeCode::String.is_var_length_type());
        assert!(!FieldTypeCode::LongLong.is_var_length_type());
    }
}
