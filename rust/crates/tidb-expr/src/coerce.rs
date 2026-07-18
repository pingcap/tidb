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

//! Shared scalar coercion helpers.
//!
//! Representation remains owned exclusively by `tidb-datatype`; this module
//! owns only evaluator operations over that representation.

use std::cmp::Ordering;

use tidb_datatype::{Datum, Decimal, StringDatum};

use crate::context::EvalError;

/// The integral portion of a datum, retaining signedness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Integer {
    Signed(i64),
    Unsigned(u64),
}

pub(crate) fn integer_of(value: &Datum) -> Result<Option<Integer>, EvalError> {
    Ok(match value {
        Datum::Int(value) => Some(Integer::Signed(*value)),
        Datum::UInt(value) => Some(Integer::Unsigned(*value)),
        Datum::String(_) | Datum::Bytes(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Null => {
            None
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel integer coercion"));
        }
    })
}

pub(crate) fn integer_cmp(lhs: Integer, rhs: Integer) -> Ordering {
    match (lhs, rhs) {
        (Integer::Signed(a), Integer::Signed(b)) => a.cmp(&b),
        (Integer::Unsigned(a), Integer::Unsigned(b)) => a.cmp(&b),
        (Integer::Signed(a), Integer::Unsigned(_)) if a < 0 => Ordering::Less,
        (Integer::Signed(a), Integer::Unsigned(b)) => (a as u64).cmp(&b),
        (Integer::Unsigned(_), Integer::Signed(b)) if b < 0 => Ordering::Greater,
        (Integer::Unsigned(a), Integer::Signed(b)) => a.cmp(&(b as u64)),
    }
}

pub(crate) fn integer_bits(value: Integer) -> u64 {
    match value {
        Integer::Signed(value) => value as u64,
        Integer::Unsigned(value) => value,
    }
}

pub(crate) fn integer_to_decimal(value: Integer) -> Decimal {
    match value {
        Integer::Signed(value) => Decimal::from_int(value),
        Integer::Unsigned(value) => Decimal::from_uint(value),
    }
}

pub(crate) fn integer_to_f64(value: Integer) -> f64 {
    match value {
        Integer::Signed(value) => value as f64,
        Integer::Unsigned(value) => value as f64,
    }
}

pub(crate) fn bool_int(value: bool) -> Datum {
    Datum::Int(i64::from(value))
}

/// The truthiness of a definite numeric scalar.
pub fn truthy_of(value: &Datum) -> Result<Option<bool>, EvalError> {
    Ok(match value {
        Datum::Int(value) => Some(*value != 0),
        Datum::UInt(value) => Some(*value != 0),
        Datum::Decimal(value) => Some(!value.is_zero()),
        Datum::Real(value) => Some(*value != 0.0),
        Datum::Null | Datum::String(_) | Datum::Bytes(_) => None,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel truth coercion"));
        }
    })
}

/// Returns a string datum's UTF-8 text without replacement.
pub(crate) fn string_text(value: &StringDatum) -> Result<&str, EvalError> {
    value
        .as_utf8()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))
}

/// Coerces a scalar to text, preserving NULL and rejecting invalid UTF-8.
pub(crate) fn coerce_str(value: &Datum) -> Result<Option<String>, EvalError> {
    match value {
        Datum::String(value) => Ok(Some(string_text(value)?.to_string())),
        Datum::Bytes(value) => std::str::from_utf8(value)
            .map(|text| Some(text.to_string()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum")),
        Datum::Int(value) => Ok(Some(value.to_string())),
        Datum::UInt(value) => Ok(Some(value.to_string())),
        Datum::Decimal(value) => Ok(Some(value.to_string())),
        Datum::Real(value) => Ok(Some(value.to_string())),
        Datum::Null => Ok(None),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel string coercion"))
        }
    }
}

/// Coerces a scalar through Go TiDB's `EvalString` byte boundary.
///
/// `CONCAT` and `CONCAT_WS` operate on Go strings, which are byte sequences
/// rather than UTF-8 values.  Their source signatures therefore preserve an
/// invalid UTF-8 suffix from a binary argument instead of raising the checked
/// decoding error used by character-semantic functions.  Keep this helper
/// separate from [`coerce_str`] so callers that actually need Unicode text do
/// not silently acquire replacement or lossy-decoding behavior.
pub(crate) fn coerce_str_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    Ok(match value {
        Datum::String(value) => Some(value.bytes().to_vec()),
        Datum::Bytes(value) => Some(value.clone()),
        Datum::Int(value) => Some(value.to_string().into_bytes()),
        Datum::UInt(value) => Some(value.to_string().into_bytes()),
        Datum::Decimal(value) => Some(value.to_string().into_bytes()),
        Datum::Real(value) => Some(value.to_string().into_bytes()),
        Datum::Null => None,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel byte coercion"));
        }
    })
}
