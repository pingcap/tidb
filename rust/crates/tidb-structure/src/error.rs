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

//! The errors Go `pkg/structure` raises itself (`structure.go`'s error
//! prototypes, the key decoders' failures, `strconv` over a stored integer,
//! and `kv.ErrNotExist` where a list read propagates it).

use std::fmt;

/// One error `pkg/structure` raises.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StructureError {
    /// The key does not start with the structure's prefix.
    InvalidKeyPrefix,
    /// The mem-comparable bytes/uint framing inside the key is malformed.
    MalformedKey,
    /// The type byte did not match the expected structure flag.
    ///
    /// Go: `ErrInvalidHashKeyFlag`, "invalid encoded ... key flag %c".
    UnexpectedTypeFlag(u8),
    /// Go `structure.ErrWriteOnSnapshot`: a mutation reached a `TxStructure`
    /// whose Go `readWriter` is nil.
    WriteOnSnapshot,
    /// Go `structure.ErrInvalidListIndex.GenWithStack("invalid list index %d")`.
    InvalidListIndex(i64),
    /// Go `structure.ErrInvalidListMetaData`: a stored list meta value was not
    /// exactly sixteen bytes.
    InvalidListMetaData,
    /// Go `kv.ErrNotExist` where the source propagates it instead of mapping a
    /// missing key to nil (list data reads).
    KeyNotExist,
    /// A scalar value was not the decimal ASCII integer Go writes.
    ///
    /// Go: `strconv.ParseInt` failing inside `TxStructure.GetInt64`.
    InvalidIntValue,
}

impl fmt::Display for StructureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyPrefix => formatter.write_str("invalid encoded meta key prefix"),
            Self::MalformedKey => formatter.write_str("malformed encoded meta key"),
            Self::UnexpectedTypeFlag(flag) => {
                write!(formatter, "invalid encoded key flag {}", *flag as char)
            }
            Self::WriteOnSnapshot => self.classed(formatter, "write on snapshot"),
            Self::InvalidListIndex(index) => {
                self.classed(formatter, format_args!("invalid list index {index}"))
            }
            Self::InvalidListMetaData => self.classed(formatter, "invalid list meta data"),
            Self::KeyNotExist => self.classed(formatter, "Error: key not exist"),
            Self::InvalidIntValue => formatter.write_str("invalid meta integer value"),
        }
    }
}

impl std::error::Error for StructureError {}

impl StructureError {
    /// The Go definition this error is raised as (`pkg/structure/structure.go`,
    /// and `pkg/kv`'s `ErrNotExist`): class and code travel with the value.
    #[must_use]
    pub fn proto(&self) -> Option<&'static tidb_terror::TerrorError> {
        use crate::structure_errors as structure;
        Some(match self {
            Self::UnexpectedTypeFlag(_) => &structure::ERR_INVALID_HASH_KEY_FLAG,
            Self::InvalidListIndex(_) => &structure::ERR_INVALID_LIST_INDEX,
            Self::InvalidListMetaData => &structure::ERR_INVALID_LIST_META_DATA,
            Self::WriteOnSnapshot => &structure::ERR_WRITE_ON_SNAPSHOT,
            Self::KeyNotExist => &tidb_kv::kv_errors::ERR_NOT_EXIST,
            _ => return None,
        })
    }

    /// TiDB/MySQL error number of [`Self::proto`].
    #[must_use]
    pub fn code(&self) -> Option<u16> {
        self.proto()
            .and_then(|proto| u16::try_from(proto.code().value()).ok())
    }

    /// Go `*terror.Error`'s `Error()`: `[<class>:<code>]<message>`.
    fn classed(&self, formatter: &mut fmt::Formatter<'_>, message: impl fmt::Display) -> fmt::Result {
        match self.proto() {
            Some(proto) => write!(formatter, "{}", proto.generate(message.to_string())),
            None => write!(formatter, "{message}"),
        }
    }
}
