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

use tidb_structure::StructureError;

/// Go `strconv.Atoi`'s two failure classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegerParseFailure {
    /// No valid base-ten integer syntax.
    Syntax,
    /// Valid syntax outside signed 64-bit range.
    Range,
}

/// Go `pkg/meta`'s errors, and the `pkg/structure` and `pkg/kv` errors a
/// metadata transaction propagates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaError {
    /// An error Go `pkg/structure` raised under the metadata transaction.
    Structure(StructureError),
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
    /// Go `kv.ErrCannotSetNilValue`: `Set` received an empty value.
    CannotSetNilValue,
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
            Self::Structure(error) => return error.fmt(formatter),
            // Go `errors.Wrapf(ErrMaskingPolicy..., "masking policy id : %d ...")`.
            Self::MaskingPolicyIdExists(id) => {
                write!(formatter, "masking policy id : {id} already exists: ")?;
            }
            Self::MaskingPolicyIdNotExists(id) => {
                write!(formatter, "masking policy id : {id} doesn't exist: ")?;
            }
            _ => {}
        }
        if let Some(message) = self.classed_message() {
            return match self.proto() {
                Some(proto) => write!(formatter, "{}", proto.generate(message)),
                None => formatter.write_str(&message),
            };
        }
        match self {
            Self::InvalidFieldInteger { value, failure, .. } => write!(
                formatter,
                "strconv.Atoi: parsing {}: {}",
                tidb_go_strconv::quote_bytes(value),
                match failure {
                    IntegerParseFailure::Syntax => "invalid syntax",
                    IntegerParseFailure::Range => "value out of range",
                }
            ),
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
            Self::InvalidObjectId(object) => write!(formatter, "{object}.ID is invalid"),
            Self::GlobalIdExceedsLimit { generated, limit } => {
                write!(formatter, "global id:{generated} exceeds the limit:{limit}")
            }
            _ => unreachable!("every classed meta error has a message"),
        }
    }
}

impl std::error::Error for MetaError {}

impl From<StructureError> for MetaError {
    fn from(error: StructureError) -> Self {
        Self::Structure(error)
    }
}

impl MetaError {
    /// The message Go's `*terror.Error` carries for an error raised from a
    /// meta definition (`ErrDBNotExists.GenWithStack("database doesn't
    /// exist")`, ...); `None` for the plain errors.
    fn classed_message(&self) -> Option<String> {
        Some(match self {
            Self::InvalidFieldPrefix(message) => (*message).to_owned(),
            Self::CannotSetNilValue => "can not set nil value".to_owned(),
            Self::DatabaseExists => "database already exists".to_owned(),
            Self::DatabaseNotExists => "database doesn't exist".to_owned(),
            Self::TableExists => "table already exists".to_owned(),
            Self::TableNotExists => "table doesn't exist".to_owned(),
            Self::DdlReorgElementNotExist => "DDL reorg element does not exist".to_owned(),
            Self::PolicyExists => "policy already exists".to_owned(),
            Self::PolicyNotExists => "policy doesn't exist".to_owned(),
            Self::PolicyIdNotExists(id) => format!("policy id : {id} doesn't exist"),
            Self::MaskingPolicyExists | Self::MaskingPolicyIdExists(_) => {
                "masking policy already exists".to_owned()
            }
            Self::MaskingPolicyNotExists | Self::MaskingPolicyIdNotExists(_) => {
                "masking policy doesn't exist".to_owned()
            }
            Self::MaskingPolicyExpressionInvalidColumn => {
                "masking policy expression can only reference the target column '%-.64s'".to_owned()
            }
            Self::ResourceGroupExists => "group already exists".to_owned(),
            Self::ResourceGroupNotExists => "group doesn't exist".to_owned(),
            Self::ResourceGroupIdNotExists(id) => format!("resource group id : {id} doesn't exist"),
            _ => return None,
        })
    }

    /// The `*terror.Error` Go returns as is (`errors.Trace` keeps it the
    /// cause): `None` for the plain errors, the structure errors and the
    /// ones Go wraps with more text.
    #[must_use]
    pub fn to_terror(&self) -> Option<tidb_terror::TerrorError> {
        if matches!(self, Self::MaskingPolicyIdExists(_) | Self::MaskingPolicyIdNotExists(_)) {
            return None;
        }
        Some(self.proto()?.generate(self.classed_message()?))
    }

    /// The Go definition this error is raised as (`pkg/meta/meta.go`,
    /// `pkg/structure`'s, and the `pkg/kv` errors they propagate):
    /// class and code travel with the value.
    #[must_use]
    pub fn proto(&self) -> Option<&'static tidb_terror::TerrorError> {
        use tidb_kv::kv_errors as kv;
        use crate::meta_errors as meta;
        Some(match self {
            Self::Structure(error) => return error.proto(),
            Self::InvalidFieldPrefix(_) => &meta::ERR_INVALID_STRING,
            Self::CannotSetNilValue => &kv::ERR_CANNOT_SET_NIL_VALUE,
            Self::DatabaseExists => &meta::ERR_DB_EXISTS,
            Self::DatabaseNotExists => &meta::ERR_DB_NOT_EXISTS,
            Self::TableExists => &meta::ERR_TABLE_EXISTS,
            Self::TableNotExists => &meta::ERR_TABLE_NOT_EXISTS,
            Self::DdlReorgElementNotExist => &meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST,
            Self::PolicyExists => &meta::ERR_POLICY_EXISTS,
            Self::PolicyNotExists | Self::PolicyIdNotExists(_) => &meta::ERR_POLICY_NOT_EXISTS,
            Self::ResourceGroupExists => &meta::ERR_RESOURCE_GROUP_EXISTS,
            Self::ResourceGroupNotExists | Self::ResourceGroupIdNotExists(_) => {
                &meta::ERR_RESOURCE_GROUP_NOT_EXISTS
            }
            Self::MaskingPolicyExists | Self::MaskingPolicyIdExists(_) => {
                &meta::ERR_MASKING_POLICY_EXISTS
            }
            Self::MaskingPolicyNotExists | Self::MaskingPolicyIdNotExists(_) => {
                &meta::ERR_MASKING_POLICY_NOT_EXISTS
            }
            Self::MaskingPolicyExpressionInvalidColumn => {
                &meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN
            }
            _ => return None,
        })
    }

    /// TiDB/MySQL error number of [`Self::proto`].
    #[must_use]
    pub fn code(&self) -> Option<u16> {
        self.proto()
            .and_then(|proto| u16::try_from(proto.code().value()).ok())
    }
}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, MetaError>;
