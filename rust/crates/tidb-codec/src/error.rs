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

use std::fmt;

/// A malformed or unsupported TiDB codec value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CodecError {
    /// More bytes are required to decode the declared value.
    InsufficientBytes,
    /// The input cannot be a canonical encoding for the requested type.
    InvalidEncoding(&'static str),
    /// The datum kind is not part of the dependency-closed codec domain.
    UnsupportedDatum(&'static str),
    /// An exact decimal is outside TiDB DECIMAL's production bounds.
    DecimalOutOfRange,
    /// Fractional digits were discarded to fit the requested schema scale.
    DecimalTruncated,
    /// Integer digits do not fit the requested schema precision and scale.
    DecimalOverflow,
    /// The value tag needs a typed codec that has not crossed this boundary.
    UnsupportedValueTag(u8),
    /// A named source failpoint injected its production error path.
    InjectedFailure(&'static str),
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientBytes => formatter.write_str("insufficient bytes to decode value"),
            Self::InvalidEncoding(reason) => write!(formatter, "invalid encoded value: {reason}"),
            Self::UnsupportedDatum(kind) => write!(formatter, "unsupported datum kind: {kind}"),
            Self::DecimalOutOfRange => {
                formatter.write_str("decimal precision or scale is out of range")
            }
            Self::DecimalTruncated => {
                formatter.write_str("decimal fractional digits were truncated")
            }
            Self::DecimalOverflow => formatter.write_str("decimal integer digits overflow"),
            Self::UnsupportedValueTag(flag) => {
                write!(formatter, "unsupported encoded value tag {flag}")
            }
            Self::InjectedFailure(name) => write!(formatter, "injected codec failure: {name}"),
        }
    }
}

impl std::error::Error for CodecError {}
