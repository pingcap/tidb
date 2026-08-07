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

//! Decode failures for the catalog codec.

use std::fmt;

/// Go `strconv.Atoi`'s two failure classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegerParseFailure {
    /// No valid base-ten integer syntax.
    Syntax,
    /// Valid syntax outside signed 64-bit range.
    Range,
}

/// Go's `structure.ErrInvalidHashKeyFlag` / `meta.ErrInvalidString` surface,
/// narrowed to the decode failures this crate can actually produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaError {
    /// The key does not start with the `m` meta namespace prefix.
    NotMetaKey,
    /// The mem-comparable bytes/uint framing inside the key is malformed.
    MalformedKey,
    /// The type byte did not match the expected structure flag.
    ///
    /// Go: `ErrInvalidHashKeyFlag`, "invalid encoded ... key flag %c".
    UnexpectedTypeFlag(u8),
    /// A field name did not carry the source function's required prefix.
    ///
    /// Go: `[meta:1300]` / `ErrInvalidString.GenWithStack(message)`.
    InvalidFieldPrefix(&'static str),
    /// The suffix passed Go's prefix check but failed `strconv.Atoi`.
    ///
    /// `partial` preserves Go's saturated result on range failure. `traced`
    /// records whether the source wrapped the `*strconv.NumError` with
    /// `errors.Trace`; the rendered message is identical either way.
    InvalidFieldInteger {
        /// Raw suffix passed to `strconv.Atoi`.
        value: Vec<u8>,
        /// Go's returned integer alongside the error.
        partial: i64,
        /// Syntax or range failure.
        failure: IntegerParseFailure,
        /// Whether the owning Go parser calls `errors.Trace`.
        traced: bool,
    },
    /// A scalar value was not the decimal ASCII integer Go writes.
    ///
    /// Go: `strconv.ParseInt` failing inside `TxStructure.GetInt64`.
    InvalidIntValue,
    /// A scalar value was not a decimal ASCII unsigned integer.
    InvalidUnsignedIntValue,
    /// A scalar value was not a Go-compatible floating-point string.
    InvalidFloatValue,
    /// A catalog JSON value did not parse into its model struct.
    InvalidJson(String),
    /// Go `detachMagicByte`: JSON-handler version is not current.
    IncompatibleMagicType,
    /// Go `detachMagicByte`: no handler owns the magic-byte range.
    UnknownMagicType,
    /// The raw transaction implementation failed.
    Storage(String),
    /// Go context cancellation observed during a table listing.
    Cancelled,
    /// Go `GetOldestSchemaVersion` found no MVCC write record.
    NoSchemaVersionWrite,
    /// Go `meta.ErrDBExists`.
    DatabaseExists,
    /// Go `meta.ErrDBNotExists`.
    DatabaseNotExists,
    /// Go `meta.ErrTableExists`.
    TableExists,
    /// Go `meta.ErrTableNotExists`.
    TableNotExists,
    /// Go `meta.ErrDDLReorgElementNotExist` marker.
    DdlReorgElementNotExist,
    /// Go `meta.ErrPolicyExists`.
    PolicyExists,
    /// Go `meta.ErrPolicyNotExists`.
    PolicyNotExists,
    /// Go `GetPolicy`'s ID-specific wrapping of `ErrPolicyNotExists`.
    PolicyIdNotExists(i64),
    /// Go `meta.ErrMaskingPolicyExists`.
    MaskingPolicyExists,
    /// Go `meta.ErrMaskingPolicyNotExists`.
    MaskingPolicyNotExists,
    /// Go's ID-specific masking-policy existence context.
    MaskingPolicyIdExists(i64),
    /// Go's ID-specific masking-policy missing context.
    MaskingPolicyIdNotExists(i64),
    /// Go `meta.ErrMaskingPolicyExprInvalidColumn` marker.
    MaskingPolicyExpressionInvalidColumn,
    /// Go `meta.ErrResourceGroupExists`.
    ResourceGroupExists,
    /// Go `meta.ErrResourceGroupNotExists`.
    ResourceGroupNotExists,
    /// Go `GetResourceGroup`'s ID-specific missing context.
    ResourceGroupIdNotExists(i64),
    /// A create operation received Go's invalid zero object ID.
    InvalidObjectId(&'static str),
    /// A generated global ID crossed Go `metadef.MaxUserGlobalID`.
    GlobalIdExceedsLimit {
        /// ID produced by the increment.
        generated: i64,
        /// Inclusive source limit.
        limit: i64,
    },
}

impl fmt::Display for MetaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotMetaKey => formatter.write_str("invalid encoded meta key prefix"),
            Self::MalformedKey => formatter.write_str("malformed encoded meta key"),
            Self::UnexpectedTypeFlag(flag) => {
                write!(formatter, "invalid encoded key flag {}", *flag as char)
            }
            Self::InvalidFieldPrefix(message) => write!(formatter, "[meta:1300]{message}"),
            Self::InvalidFieldInteger { value, failure, .. } => write!(
                formatter,
                "strconv.Atoi: parsing {}: {}",
                GoQuotedString(value),
                match failure {
                    IntegerParseFailure::Syntax => "invalid syntax",
                    IntegerParseFailure::Range => "value out of range",
                }
            ),
            Self::InvalidIntValue => formatter.write_str("invalid meta integer value"),
            Self::InvalidUnsignedIntValue => {
                formatter.write_str("invalid meta unsigned integer value")
            }
            Self::InvalidFloatValue => formatter.write_str("invalid meta floating-point value"),
            Self::InvalidJson(message) => write!(formatter, "invalid meta JSON value: {message}"),
            Self::IncompatibleMagicType => {
                formatter.write_str("incompatible magic type handling module")
            }
            Self::UnknownMagicType => formatter.write_str("unknown magic type handling module"),
            Self::Storage(message) => write!(formatter, "meta transaction failed: {message}"),
            Self::Cancelled => formatter.write_str("context canceled"),
            Self::NoSchemaVersionWrite => {
                formatter.write_str("There is no Write MVCC info for the schema version key")
            }
            Self::DatabaseExists => formatter.write_str("[meta:1007]database already exists"),
            Self::DatabaseNotExists => formatter.write_str("[meta:1049]database doesn't exist"),
            Self::TableExists => formatter.write_str("[meta:1050]table already exists"),
            Self::TableNotExists => formatter.write_str("[meta:1146]table doesn't exist"),
            Self::DdlReorgElementNotExist => {
                formatter.write_str("[meta:8235]DDL reorg element does not exist")
            }
            Self::PolicyExists => formatter.write_str("[meta:8238]policy already exists"),
            Self::PolicyNotExists => formatter.write_str("[meta:8239]policy doesn't exist"),
            Self::PolicyIdNotExists(id) => {
                write!(formatter, "[meta:8239]policy id : {id} doesn't exist")
            }
            Self::MaskingPolicyExists => {
                formatter.write_str("[meta:8268]masking policy already exists")
            }
            Self::MaskingPolicyNotExists => {
                formatter.write_str("[meta:8269]masking policy doesn't exist")
            }
            Self::MaskingPolicyIdExists(id) => write!(
                formatter,
                "masking policy id : {id} already exists: [meta:8268]masking policy already exists"
            ),
            Self::MaskingPolicyIdNotExists(id) => write!(
                formatter,
                "masking policy id : {id} doesn't exist: [meta:8269]masking policy doesn't exist"
            ),
            Self::MaskingPolicyExpressionInvalidColumn => formatter.write_str(
                "[meta:8275]masking policy expression can only reference the target column '%-.64s'",
            ),
            Self::ResourceGroupExists => formatter.write_str("[meta:8248]group already exists"),
            Self::ResourceGroupNotExists => formatter.write_str("[meta:8249]group doesn't exist"),
            Self::ResourceGroupIdNotExists(id) => write!(
                formatter,
                "[meta:8249]resource group id : {id} doesn't exist"
            ),
            Self::InvalidObjectId(object) => write!(formatter, "{object}.ID is invalid"),
            Self::GlobalIdExceedsLimit { generated, limit } => {
                write!(formatter, "global id:{generated} exceeds the limit:{limit}")
            }
        }
    }
}

impl std::error::Error for MetaError {}

impl MetaError {
    /// TiDB/MySQL error number for source errors declared in `meta.go`.
    #[must_use]
    pub const fn code(&self) -> Option<u16> {
        match self {
            Self::InvalidFieldPrefix(_) => Some(1300),
            Self::DatabaseExists => Some(1007),
            Self::DatabaseNotExists => Some(1049),
            Self::TableExists => Some(1050),
            Self::TableNotExists => Some(1146),
            Self::DdlReorgElementNotExist => Some(8235),
            Self::PolicyExists => Some(8238),
            Self::PolicyNotExists | Self::PolicyIdNotExists(_) => Some(8239),
            Self::ResourceGroupExists => Some(8248),
            Self::ResourceGroupNotExists | Self::ResourceGroupIdNotExists(_) => Some(8249),
            Self::MaskingPolicyExists | Self::MaskingPolicyIdExists(_) => Some(8268),
            Self::MaskingPolicyNotExists | Self::MaskingPolicyIdNotExists(_) => Some(8269),
            Self::MaskingPolicyExpressionInvalidColumn => Some(8275),
            _ => None,
        }
    }
}

struct GoQuotedString<'a>(&'a [u8]);

impl fmt::Display for GoQuotedString<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"")?;
        let mut rest = self.0;
        while !rest.is_empty() {
            match std::str::from_utf8(rest) {
                Ok(valid) => {
                    for character in valid.chars() {
                        match character {
                            '\\' => formatter.write_str("\\\\")?,
                            '"' => formatter.write_str("\\\"")?,
                            '\n' => formatter.write_str("\\n")?,
                            '\r' => formatter.write_str("\\r")?,
                            '\t' => formatter.write_str("\\t")?,
                            character if character.is_control() => {
                                write!(formatter, "\\u{:04x}", character as u32)?;
                            }
                            character => write!(formatter, "{character}")?,
                        }
                    }
                    break;
                }
                Err(error) => {
                    let valid = &rest[..error.valid_up_to()];
                    for character in std::str::from_utf8(valid)
                        .expect("valid_up_to is valid UTF-8")
                        .chars()
                    {
                        write!(formatter, "{character}")?;
                    }
                    let invalid = rest[error.valid_up_to()];
                    write!(formatter, "\\x{invalid:02x}")?;
                    rest = &rest[error.valid_up_to() + 1..];
                }
            }
        }
        formatter.write_str("\"")
    }
}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, MetaError>;
