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
mod builder;
mod clone;
mod json;
mod memory;
mod value;

use crate::go_runtime::GoSharedSlice;
use crate::{output_format, Charset, Collation, EvalType, GoString};
use std::fmt;
use std::hash::{Hash, Hasher};

pub use aggregate::{agg_field_type, aggregate_eval_type, merge_field_type, set_type_flag};
pub use builder::FieldTypeBuilder;
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

/// Default for Go's process-wide `parsertypes.TiDBStrictIntegerDisplayWidth`.
///
/// A running TiDB node replaces the Go variable from
/// `deprecate-integer-display-length` during process initialization. The Rust
/// startup hook remains an integration seam, so source-shaped formatters keep
/// accepting an explicit policy until that mutable process cell is wired.
pub const STRICT_INTEGER_DISPLAY_WIDTH: bool = true;

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
/// The UTF-8 parser-owned charset and collation spellings are independent,
/// just as in Go. A resolved collation is cached only for typed convenience;
/// source predicates and runtime collator lookup remain spelling-authoritative.
/// Programmatic non-UTF-8 charset/collation strings remain an explicit native
/// surface seam. Length, decimal, and raw flags preserve the parser-owned
/// metadata without imposing SQL warning or formatting policy.
#[derive(Debug, Clone)]
pub struct FieldType {
    code: FieldTypeCode,
    // Go `pkg/parser/types.FieldType.flag` is a `uint`. Keep the complete
    // 64-bit word even though every currently defined MySQL flag fits in the
    // low 32 bits and the tipb wire field is only 32 bits wide.
    flags: u64,
    flen: i64,
    decimal: i64,
    collation: Collation,
    charset_name: String,
    collation_name: String,
    elems: GoSharedSlice<GoString>,
    elems_is_binary_literal: GoSharedSlice<bool>,
    array: bool,
}

impl PartialEq for FieldType {
    fn eq(&self, other: &Self) -> bool {
        if self.code != other.code
            || self.flags != other.flags
            || self.flen != other.flen
            || self.decimal != other.decimal
            || self.charset_name != other.charset_name
            || self.collation_name != other.collation_name
            || self.array != other.array
        {
            return false;
        }
        let self_elems = self.elems.snapshot();
        let other_elems = other.elems.snapshot();
        let self_flags = self.elems_is_binary_literal.snapshot();
        let other_flags = other.elems_is_binary_literal.snapshot();
        self_elems == other_elems && self_flags == other_flags
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
        self.elems.snapshot().hash(state);
        self.elems_is_binary_literal.snapshot().hash(state);
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
            elems: GoSharedSlice::default(),
            elems_is_binary_literal: GoSharedSlice::default(),
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
            elems: GoSharedSlice::default(),
            elems_is_binary_literal: GoSharedSlice::default(),
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
            self.flags |= FieldTypeFlags::UNSIGNED as u64;
        } else {
            self.flags &= !(FieldTypeFlags::UNSIGNED as u64);
        }
        self
    }

    /// Returns the raw source field flags.
    pub const fn flags(&self) -> u32 {
        self.flags as u32
    }

    /// Returns the complete Go `uint` flag word.
    ///
    /// Existing execution consumers intentionally use [`Self::flags`] because
    /// all defined MySQL flags and the tipb field are 32-bit. Metadata codecs
    /// and `pkg/meta/model.ColumnInfo` use this method so unknown high bits are
    /// not lost during JSON round trips or bitwise mutation.
    pub const fn raw_flags(&self) -> u64 {
        self.flags
    }

    /// Returns whether a source flag mask is set.
    pub const fn has_flag(&self, flag: u32) -> bool {
        self.flags & flag as u64 != 0
    }

    /// Replaces all source field flags.
    pub const fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags as u64;
        self
    }

    /// Replaces the complete Go `uint` flag word.
    pub const fn with_raw_flags(mut self, flags: u64) -> Self {
        self.flags = flags;
        self
    }

    /// Adds source flags with bitwise OR.
    pub const fn with_added_flags(mut self, flags: u32) -> Self {
        self.flags |= flags as u64;
        self
    }

    /// Adds flags across the complete Go `uint` word.
    pub const fn with_added_raw_flags(mut self, flags: u64) -> Self {
        self.flags |= flags;
        self
    }

    /// Keeps only source flags selected by the mask with bitwise AND.
    pub const fn with_and_flags(mut self, flags: u32) -> Self {
        self.flags &= flags as u64;
        self
    }

    /// Keeps only flags selected by a complete Go `uint` mask.
    pub const fn with_and_raw_flags(mut self, flags: u64) -> Self {
        self.flags &= flags;
        self
    }

    /// Toggles source flags with bitwise XOR.
    pub const fn with_toggled_flags(mut self, flags: u32) -> Self {
        self.flags ^= flags as u64;
        self
    }

    /// Toggles flags across the complete Go `uint` word.
    pub const fn with_toggled_raw_flags(mut self, flags: u64) -> Self {
        self.flags ^= flags;
        self
    }

    /// Removes source flags with bitwise AND-NOT.
    pub const fn with_removed_flags(mut self, flags: u32) -> Self {
        self.flags &= !(flags as u64);
        self
    }

    /// Removes flags across the complete Go `uint` word.
    pub const fn with_removed_raw_flags(mut self, flags: u64) -> Self {
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

    /// Resolves the exact source spelling through the process collation mode.
    pub fn runtime_collator(&self) -> crate::Collator {
        crate::get_collator(&self.collation_name)
    }

    /// Resolves the exact source spelling under a caller-captured mode.
    pub fn runtime_collator_with_mode(&self, use_new_collation: bool) -> crate::Collator {
        crate::get_collator_with_mode(use_new_collation, &self.collation_name)
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
        self.collation = crate::get_collator_with_mode(true, &collation)
            .new_collation()
            .expect("new-collation lookup always returns a concrete collation");
        self.collation_name = collation;
        self
    }

    /// Replaces ENUM/SET elements. Go's `SetElems` does not touch the
    /// independently owned, lazily allocated binary-literal marker slice.
    pub fn with_elems(mut self, elems: impl IntoIterator<Item = impl Into<GoString>>) -> Self {
        self.elems = GoSharedSlice::from_vec(elems.into_iter().map(Into::into).collect());
        self
    }

    /// Mirrors Go `GetElems`: copies the slice header and shares its backing.
    #[must_use]
    pub fn elems(&self) -> GoSharedSlice<GoString> {
        self.elems.clone()
    }

    /// Borrows visible ENUM/SET byte strings without cloning. The callback
    /// must not re-enter the same backing through a shallow FieldType clone.
    pub fn with_elems_visible<R>(&self, read: impl FnOnce(&[GoString]) -> R) -> R {
        self.elems.with_visible(read)
    }

    /// Clones ENUM/SET string headers for read-only consumers that cannot use
    /// the scoped borrowed view. The immutable byte backing remains shared.
    #[must_use]
    pub fn elems_snapshot(&self) -> Vec<GoString> {
        self.elems.snapshot()
    }

    /// Mirrors Go `SetElems`: replaces only this receiver's slice header.
    pub fn set_elems(&mut self, elems: impl Into<GoSharedSlice<GoString>>) {
        self.elems = elems.into();
    }

    /// Updates one ENUM/SET element without changing binary-literal markers.
    pub fn set_elem(&mut self, index: usize, element: impl Into<GoString>) {
        self.elems.set(index, element.into());
    }

    /// Returns one ENUM/SET element.
    pub fn elem(&self, index: usize) -> GoString {
        self.elems.get(index)
    }

    /// Updates an ENUM/SET element and lazily allocates binary-literal flags.
    pub fn set_elem_with_binary_literal(
        &mut self,
        index: usize,
        element: impl Into<GoString>,
        is_binary_literal: bool,
    ) {
        self.elems.set(index, element.into());
        if is_binary_literal {
            if !self.elems_is_binary_literal.is_allocated() {
                self.elems_is_binary_literal =
                    GoSharedSlice::from_vec(vec![false; self.elems.len()]);
            }
            self.elems_is_binary_literal.set(index, true);
        }
    }

    /// Mirrors `FieldType.GetElemIsBinaryLit`: an absent marker slice returns
    /// false, while a present slice retains Go's indexed access semantics.
    pub fn elem_is_binary_literal(&self, index: usize) -> bool {
        if self.elems_is_binary_literal.is_empty() {
            false
        } else {
            self.elems_is_binary_literal.get(index)
        }
    }

    /// Clears the lazily allocated element binary-literal flags.
    pub fn clean_elem_binary_literals(&mut self) {
        if self.elems_is_binary_literal.is_allocated() {
            self.elems_is_binary_literal = GoSharedSlice::default();
        }
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
                if self.flags & FieldTypeFlags::ENUM_SET_AS_INT as u64 != 0 =>
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
            && self.elems.snapshot() == other.elems.snapshot()
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
            && self.elems.snapshot() == other.elems.snapshot()
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
    pub fn is_binary_string(&self) -> bool {
        self.is_string() && self.collation_name == "binary"
    }

    /// Returns whether this is a non-binary character string.
    pub fn is_character_string(&self) -> bool {
        self.is_string() && !self.is_binary_string()
    }

    /// Mirrors `pkg/types/etc.go::NeedRestoredData` with the new-collation
    /// switch enabled, as used by current TiDB storage paths.
    pub fn need_restored_data(&self) -> bool {
        self.need_restored_data_with_collation(true)
    }

    /// Mirrors `pkg/types/etc.go::NeedRestoredDataWithCollate` for the
    /// collations represented by this dependency leaf.
    pub fn need_restored_data_with_collation(&self, use_new_collate: bool) -> bool {
        if !use_new_collate || !self.is_character_string() {
            return false;
        }
        // Go's trailing `ft.GetCollate() != "utf8mb4_0900_bin"` guard, which
        // overrides the VARCHAR exemption below: this collation NEVER carries
        // restored data, whatever the SQL type.
        if self.collation_name == "utf8mb4_0900_bin" {
            return false;
        }
        // `collate.IsBinCollation`, whose membership is the same list as
        // `crate::collation::is_bin_collation` -- `utf8mb4_0900_bin` included,
        // `gbk_bin` excluded because its sort key transcodes the data.
        let bin_collation = crate::is_bin_collation(&self.collation_name);
        !bin_collation || self.code().is_type_varchar()
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

    /// Mirrors Go `FieldType.SetFlag`: replaces the flag word.
    pub fn set_flags(&mut self, flags: u32) {
        self.flags = flags as u64;
    }

    /// Mirrors Go `FieldType.SetFlag` across the complete `uint` word.
    pub fn set_raw_flags(&mut self, flags: u64) {
        self.flags = flags;
    }

    /// Mirrors Go `FieldType.AddFlag`: `flags |= f`.
    pub fn add_flags(&mut self, flags: u32) {
        self.flags |= flags as u64;
    }

    /// Mirrors Go `FieldType.AddFlag` across the complete `uint` word.
    pub fn add_raw_flags(&mut self, flags: u64) {
        self.flags |= flags;
    }

    /// Mirrors Go `FieldType.AndFlag`: `flags &= f`.
    pub fn and_flags(&mut self, flags: u32) {
        self.flags &= flags as u64;
    }

    /// Mirrors Go `FieldType.AndFlag` across the complete `uint` word.
    pub fn and_raw_flags(&mut self, flags: u64) {
        self.flags &= flags;
    }

    /// Mirrors Go `FieldType.ToggleFlag`: `flags ^= f`.
    pub fn toggle_flags(&mut self, flags: u32) {
        self.flags ^= flags as u64;
    }

    /// Mirrors Go `FieldType.ToggleFlag` across the complete `uint` word.
    pub fn toggle_raw_flags(&mut self, flags: u64) {
        self.flags ^= flags;
    }

    /// Mirrors Go `FieldType.DelFlag`: `flags &= ^f`.
    pub fn del_flags(&mut self, flags: u32) {
        self.flags &= !(flags as u64);
    }

    /// Mirrors Go `FieldType.DelFlag` across the complete `uint` word.
    pub fn del_raw_flags(&mut self, flags: u64) {
        self.flags &= !flags;
    }

    /// Mirrors Go `FieldType.SetFlen`: sets the length unconditionally
    /// (unlike [`set_flen_under_limit`](Self::set_flen_under_limit)).
    pub fn set_flen(&mut self, flen: i64) {
        self.flen = flen;
    }

    /// Mirrors Go `FieldType.SetDecimal`: sets the scale unconditionally.
    pub fn set_decimal(&mut self, decimal: i64) {
        self.decimal = decimal;
    }

    /// Mirrors Go `FieldType.SetCharset`: sets the charset name.
    pub fn set_charset_name(&mut self, charset: impl Into<String>) {
        self.charset_name = charset.into();
    }

    /// Mirrors Go `FieldType.SetCollate`: sets the collation name.
    ///
    /// Source predicates read this exact, case-sensitive spelling. The typed
    /// cache uses the same exact-name lookup and `utf8mb4_bin` fallback as
    /// Go's new-collation runtime; it must never normalize a spelling such as
    /// `BINARY` into a different source collation.
    pub fn set_collation_name(&mut self, collation: impl Into<String>) {
        let collation = collation.into();
        self.collation = crate::get_collator_with_mode(true, &collation)
            .new_collation()
            .expect("new-collation lookup always returns a concrete collation");
        self.collation_name = collation;
    }

    /// Sets the collation from an already-resolved [`Collation`], refreshing
    /// the name strings that spell it.
    ///
    /// Go stores only the names and resolves them on demand; this typed setter
    /// writes the canonical source spellings as well as the cache.
    pub fn set_collation(&mut self, collation: Collation) {
        self.collation = collation;
        self.charset_name = collation.charset().name().to_owned();
        self.collation_name = collation.name().to_owned();
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
                suffix.push_str("('");
                self.elems.with_visible(|elements| {
                    for (index, elem) in elements.iter().enumerate() {
                        if index != 0 {
                            suffix.push_str("','");
                        }
                        // Go `OutputFormat` ranges over a string, replacing
                        // each invalid byte with U+FFFD before applying its
                        // four rune escapes.
                        suffix.push_str(&output_format(&elem.to_utf8_lossy_go()));
                    }
                });
                suffix.push_str("')");
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

    /// Mirrors Go `table.Column.GetTypeDesc`: the compact spelling with the
    /// `unsigned` and `zerofill` words a column carries as FLAGS rather than
    /// in its type, which is what `SHOW CREATE TABLE` and `SHOW COLUMNS`
    /// print. `YEAR` takes neither and `BIT` takes no `unsigned`, exactly as
    /// Go excludes them.
    ///
    /// [`Self::info_schema_str`] is the same text WITHOUT `zerofill`, because
    /// `information_schema.columns.COLUMN_TYPE` reads Go's `InfoSchemaStr`
    /// instead; the two surfaces really do differ on that one word.
    pub fn type_desc(&self, strict_integer_display_width: bool) -> String {
        let mut desc = self.info_schema_str(strict_integer_display_width);
        if self.has_flag(FieldTypeFlags::ZEROFILL) && self.code() != FieldTypeCode::Year {
            desc.push_str(" zerofill");
        }
        desc
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

    /// Restores the field type using Go's default restore flags into its
    /// authoritative byte domain. ENUM/SET members may contain invalid UTF-8,
    /// which Go preserves rather than replacing.
    #[must_use]
    pub fn restore_bytes(&self) -> Vec<u8> {
        let mut output = type_to_str(self.code(), &self.charset_name)
            .to_ascii_uppercase()
            .into_bytes();
        let (precision, scale) = match self.code() {
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                output.push(b'(');
                self.elems.with_visible(|elements| {
                    for (index, elem) in elements.iter().enumerate() {
                        if index != 0 {
                            output.push(b',');
                        }
                        output.push(b'\'');
                        for byte in elem.as_bytes() {
                            output.push(*byte);
                            if *byte == b'\'' {
                                output.push(*byte);
                            }
                        }
                        output.push(b'\'');
                    }
                });
                output.push(b')');
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
            output.extend_from_slice(format!("({precision}").as_bytes());
            if scale != UNSPECIFIED_LENGTH {
                output.extend_from_slice(format!(",{scale}").as_bytes());
            }
            output.push(b')');
        }
        if self.is_unsigned() {
            output.extend_from_slice(b" UNSIGNED");
        }
        if self.has_flag(FieldTypeFlags::ZEROFILL) {
            output.extend_from_slice(b" ZEROFILL");
        }
        if self.has_flag(FieldTypeFlags::BINARY) && self.charset_name != "binary" {
            output.extend_from_slice(b" BINARY");
        }
        if self.code().is_type_char() || self.code().is_type_blob() {
            if !self.charset_name.is_empty() && self.charset_name != "binary" {
                output.extend_from_slice(b" CHARACTER SET ");
                output.extend_from_slice(self.charset_name.to_uppercase().as_bytes());
            }
            if !self.collation_name.is_empty() && self.collation_name != "binary" {
                output.extend_from_slice(b" COLLATE ");
                output.extend_from_slice(self.collation_name.as_bytes());
            }
        }
        output
    }

    /// UTF-8 display projection of [`Self::restore_bytes`]. Invalid source
    /// bytes are replaced one at a time; byte-sensitive callers must use the
    /// authoritative method instead.
    #[must_use]
    pub fn restore(&self) -> String {
        GoString::from(self.restore_bytes()).to_utf8_lossy_go()
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
                        output.push_str(&self.charset_name.to_uppercase());
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
        // Go replaces `char` with `binary` for every `IsTypeChar` code, which
        // turns `varchar` into `varbinary` -- `VarString` is not one of them.
        FieldTypeCode::Varchar => {
            if binary {
                "varbinary"
            } else {
                "varchar"
            }
        }
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

    // The Go-compatible mutators (SetFlag/AddFlag/AndFlag/ToggleFlag/DelFlag,
    // SetFlen/SetDecimal, SetCharset/SetCollate, SetElems).
    #[test]
    fn go_style_setters() {
        let mut ft = FieldType::new(FieldTypeCode::Long);
        ft.set_flags(FieldTypeFlags::NOT_NULL);
        assert_eq!(ft.flags(), FieldTypeFlags::NOT_NULL);
        ft.add_flags(FieldTypeFlags::UNSIGNED);
        assert_eq!(
            ft.flags(),
            FieldTypeFlags::NOT_NULL | FieldTypeFlags::UNSIGNED
        );
        ft.del_flags(FieldTypeFlags::NOT_NULL);
        assert_eq!(ft.flags(), FieldTypeFlags::UNSIGNED);
        ft.toggle_flags(FieldTypeFlags::UNSIGNED);
        assert_eq!(ft.flags(), 0);
        ft.set_flags(FieldTypeFlags::NOT_NULL | FieldTypeFlags::UNSIGNED);
        ft.and_flags(FieldTypeFlags::UNSIGNED);
        assert_eq!(ft.flags(), FieldTypeFlags::UNSIGNED);

        // Unconditional set (unlike the _under_limit variants).
        ft.set_flen(1234);
        assert_eq!(ft.flen(), 1234);
        ft.set_decimal(5);
        assert_eq!(ft.decimal(), 5);

        ft.set_charset_name("utf8mb4");
        assert_eq!(ft.charset_name(), "utf8mb4");
        ft.set_collation_name("utf8mb4_bin");
        assert_eq!(ft.collation_name(), "utf8mb4_bin");

        ft.set_elems(vec![GoString::from("a"), GoString::from("b")]);
        assert_eq!(ft.elems_snapshot(), ["a", "b"]);
    }

    /// The exact source spelling owns predicates while the typed cache follows
    /// Go's exact runtime-map lookup and fallback.
    #[test]
    fn set_collation_name_keeps_cached_enum_in_sync() {
        // Starts as a character type (utf8mb4, non-binary).
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        assert!(!ft.is_binary_string());

        // Mirrors what `apply_derived_collation` / `set_explicit_collation`
        // do: set the charset string, then the collation string.
        ft.set_charset_name("binary");
        ft.set_collation_name("binary");
        assert_eq!(ft.collation(), Collation::Binary);
        assert!(ft.is_binary_string());

        // Mirrors `builtin_arithmetic`'s binary-flagged numeric result path:
        // same two calls, starting from a fresh binary-by-default type. A
        // Numeric types never satisfy `IsBinaryStr`, but the cache still
        // resolves the canonical lower-case spelling.
        let mut arith =
            FieldType::new(FieldTypeCode::LongLong).with_collation(Collation::Utf8Mb4Bin);
        arith.set_charset_name("binary");
        arith.set_collation_name("binary");
        assert_eq!(arith.collation(), Collation::Binary);
        assert!(!arith.is_binary_string());

        // The typed setter writes the canonical name strings too.
        let mut both = FieldType::new(FieldTypeCode::VarString);
        both.set_collation(Collation::Binary);
        assert!(both.is_binary_string());
        assert_eq!(both.collation_name(), "binary");
        assert_eq!(both.charset_name(), "binary");

        // Switching back to a character collation must also flip the cache.
        ft.set_charset_name("utf8mb4");
        ft.set_collation_name("utf8mb4_bin");
        assert_eq!(ft.collation(), Collation::Utf8Mb4Bin);
        assert!(!ft.is_binary_string());

        // Go's new-collation map lookup is case-sensitive. `BINARY` is not
        // `binary`: predicates remain non-binary and runtime resolution uses
        // the utf8mb4_bin fallback instead of the normalized Binary cache.
        ft.set_collation_name("BINARY");
        assert_eq!(ft.collation_name(), "BINARY");
        assert_eq!(ft.collation(), Collation::Utf8Mb4Bin);
        assert!(!ft.is_binary_string());
        assert_eq!(
            ft.runtime_collator_with_mode(true),
            crate::Collator::New(Collation::Utf8Mb4Bin)
        );
    }
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

    #[test]
    fn test_is_type_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::TinyBlob, true),
            (FieldTypeCode::MediumBlob, true),
            (FieldTypeCode::Blob, true),
            (FieldTypeCode::LongBlob, true),
            (FieldTypeCode::Int24, false),
        ] {
            assert_eq!(code.is_type_blob(), expected, "blob: {code:?}");
        }
        for (code, expected) in [
            (FieldTypeCode::String, true),
            (FieldTypeCode::Varchar, true),
            (FieldTypeCode::Long, false),
        ] {
            assert_eq!(code.is_type_char(), expected, "char: {code:?}");
        }
    }

    #[test]
    fn test_is_type_temporal_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::Duration, true),
            (FieldTypeCode::Datetime, true),
            (FieldTypeCode::Timestamp, true),
            (FieldTypeCode::Date, true),
            (FieldTypeCode::NewDate, true),
            (FieldTypeCode::Unknown(b't'), false),
        ] {
            assert_eq!(code.is_type_temporal(), expected, "{code:?}");
        }
    }

    #[test]
    fn test_is_temporal_with_date_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::Datetime, true),
            (FieldTypeCode::Date, true),
            (FieldTypeCode::Timestamp, true),
            (FieldTypeCode::Unknown(b't'), false),
        ] {
            assert_eq!(code.is_temporal_with_date(), expected, "{code:?}");
        }
    }

    #[test]
    fn test_is_type_prefixable_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::Unknown(b't'), false),
            (FieldTypeCode::Blob, true),
        ] {
            assert_eq!(code.is_type_prefixable(), expected, "{code:?}");
        }
    }

    #[test]
    fn test_is_type_fractionable_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::Datetime, true),
            (FieldTypeCode::Duration, true),
            (FieldTypeCode::Timestamp, true),
            (FieldTypeCode::Unknown(b't'), false),
        ] {
            assert_eq!(code.is_type_fractionable(), expected, "{code:?}");
        }
    }

    #[test]
    fn test_is_type_numeric_source_rows() {
        for (code, expected) in [
            (FieldTypeCode::Bit, true),
            (FieldTypeCode::Tiny, true),
            (FieldTypeCode::Int24, true),
            (FieldTypeCode::Long, true),
            (FieldTypeCode::LongLong, true),
            (FieldTypeCode::NewDecimal, true),
            (FieldTypeCode::Unspecified, false),
            (FieldTypeCode::Float, true),
            (FieldTypeCode::Double, true),
            (FieldTypeCode::Short, true),
            (FieldTypeCode::Unknown(b't'), false),
        ] {
            assert_eq!(code.is_type_numeric(), expected, "{code:?}");
        }
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
            // Go `NeedRestoredDataWithCollate` ends with an explicit
            // `ft.GetCollate() != "utf8mb4_0900_bin"` guard that OVERRIDES the
            // VARCHAR exemption, so this collation never carries restored
            // data. Both rows below are storage-format decisions: emitting
            // restored data Go does not emit makes the index and row bytes
            // mutually undecodable.
            (FieldTypeCode::String, Collation::Utf8Mb40900Bin, false),
            (FieldTypeCode::VarString, Collation::Utf8Mb40900Bin, false),
            (FieldTypeCode::Varchar, Collation::Utf8Mb40900Bin, false),
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

    #[test]
    fn parser_field_type_preserves_the_complete_go_uint_flag_word() {
        const HIGH: u64 = 1_u64 << 63;
        let mut field_type = FieldType::parser(FieldTypeCode::Long)
            .with_raw_flags(HIGH | FieldTypeFlags::UNSIGNED as u64);

        assert_eq!(
            field_type.raw_flags(),
            HIGH | FieldTypeFlags::UNSIGNED as u64
        );
        assert_eq!(field_type.flags(), FieldTypeFlags::UNSIGNED);
        assert!(field_type.has_flag(FieldTypeFlags::UNSIGNED));

        field_type.toggle_raw_flags(HIGH | FieldTypeFlags::ZEROFILL as u64);
        assert_eq!(
            field_type.raw_flags(),
            (FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL) as u64
        );
        field_type.add_raw_flags(HIGH);
        field_type.del_raw_flags(FieldTypeFlags::UNSIGNED as u64);
        assert_eq!(
            field_type.raw_flags(),
            HIGH | FieldTypeFlags::ZEROFILL as u64
        );
        field_type.and_raw_flags(HIGH);
        assert_eq!(field_type.raw_flags(), HIGH);

        let encoded = field_type.to_json().unwrap();
        assert_eq!(
            String::from_utf8(encoded.clone()).unwrap(),
            r#"{"Tp":3,"Flag":9223372036854775808,"Flen":-1,"Decimal":-1,"Charset":"","Collate":"","Elems":null,"ElemsIsBinaryLit":null,"Array":false}"#
        );
        let decoded = FieldType::from_json(&encoded).unwrap();
        assert_eq!(decoded.raw_flags(), HIGH);

        // The legacy low-word setters intentionally replace the whole Go word,
        // just as Go SetFlag does when called with a low-bit value.
        field_type.set_flags(FieldTypeFlags::NOT_NULL);
        assert_eq!(field_type.raw_flags(), FieldTypeFlags::NOT_NULL as u64);
    }

    #[test]
    fn parser_field_type_json_matches_go_member_dispatch() {
        const HIGH: u64 = 1_u64 << 63;

        let zero = FieldType::from_json(b"null").unwrap();
        assert_eq!(zero.raw_flags(), 0);
        assert_eq!(zero.flen(), 0);
        assert_eq!(zero.decimal(), 0);
        assert_eq!(zero.charset_name(), "");
        assert_eq!(zero.collation_name(), "");
        assert!(zero.elems().is_empty());
        assert!(!zero.elem_is_binary_literal(0));
        assert!(!zero.is_array());

        let decoded = FieldType::from_json(
            r#"{
                "Flag": 1,
                "fLaG": 9223372036854775808,
                "Flen": 7,
                "flen": null,
                "Charſet": "utf8",
                "CHARSET": null,
                "Array": true,
                "array": null,
                "Elems": ["a"],
                "elems": null,
                "ElemsIsBinaryLit": [true],
                "elemsisbinarylit": [],
                "Unknown": {"ignored": true}
            }"#
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(decoded.raw_flags(), HIGH);
        assert_eq!(decoded.flen(), 7);
        assert_eq!(decoded.charset_name(), "utf8");
        assert!(decoded.is_array());
        assert!(!decoded.elems.is_allocated());
        assert!(decoded.elems_is_binary_literal.is_allocated());
        assert!(decoded.elems_is_binary_literal.is_empty());

        assert!(FieldType::from_json(br#"{"Flag":"not-a-number"}"#).is_err());
    }
}
