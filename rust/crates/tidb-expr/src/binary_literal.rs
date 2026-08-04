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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `tidb_ast::Expr::Hex`/`Expr::Bit` evaluation, called directly from
//! `crate::eval_in`.
//!
//! Real MySQL/TiDB's own `types.HexLiteral`/`types.BitLiteral` are BOTH
//! just `[]byte` under the hood (`pkg/types/binary_literal.go`), and a hex
//! or bit literal's default, general-context value IS its raw bytes
//! (confirmed via `gorun`: `SELECT 0x41` is the byte `'A'`, not the
//! integer `65`; `CONCAT('x', 0x41)` is `'xA'`; `HEX(0x1A)` round-trips
//! to `'1A'`).
//!
//! The DATUM KIND, though, is not `KindBytes`: Go's `SetValue` stores both
//! literal types as `KindBinaryLiteral` (`pkg/types/datum.go:626-630`), and
//! that kind is what an ARITHMETIC, comparison or numeric-function context
//! reads as the literal's unsigned INTEGER value. Carrying it as
//! `Datum::Bytes` collapsed those two meanings into one kind, and the
//! collapse cost real answers rather than only refusals -- `gorun` says
//! `0x1A + 1` is 27, `-0x1A` is -26, `ABS(b'11')` is 3 and `0x1A > 25` is 1,
//! where this tier answered `Unsupported`, `-0`, `0` and `0`. Producing Go's
//! kind makes every one of those correct through the machinery that was
//! already in place for the chunk tier's `Datum::BinaryLiteral`.
//!
//! `Datum::BinaryLiteral` retains every raw octet without a UTF-8
//! conversion. A lone `0xFF`, embedded NUL, and valid text-shaped bytes
//! therefore follow the same representation path; only an operation that
//! explicitly needs character semantics performs checked decoding.

use tidb_datatype::BinaryLiteral;

use crate::{Datum, EvalError};

/// Decodes a normalized hex-literal digit string (`tidb_ast::Expr::Hex`'s
/// own field — already lowercase, even-length hex digit PAIRS, confirmed
/// by that type's own doc) into its raw-byte `Datum::BinaryLiteral`.
pub(crate) fn hex_literal_value(digits: &str) -> Result<Datum, EvalError> {
    let bytes: Vec<u8> = digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char)
                .to_digit(16)
                .expect("normalized hex digit");
            let lo = (pair[1] as char)
                .to_digit(16)
                .expect("normalized hex digit");
            ((hi << 4) | lo) as u8
        })
        .collect();
    bytes_to_value(bytes)
}

/// Decodes a normalized bit-literal digit string (`tidb_ast::Expr::Bit`'s
/// own field — leading-zero-stripped binary digits, `""` for the empty
/// literal `b''`, `"0"` for an all-zero literal like `b'0'`/`b'000'`,
/// confirmed by `tidb_parser::expr::normalize_bit`) into the MINIMAL
/// big-endian byte sequence representing its numeric value — confirmed
/// via `gorun`: `HEX(b'101')` is `'05'` (1 byte, `ceil(3 bits / 8)`),
/// `HEX(b'111111111')` is `'01FF'` (2 bytes, `ceil(9 bits / 8)`),
/// `HEX(b'')` is `''` (0 bytes). A literal wider than 64 bits — real
/// MySQL/TiDB's own `BIT` column max width, though a bare literal
/// EXPRESSION isn't confirmed to enforce this same limit — is
/// `Unsupported` rather than guessed at.
pub(crate) fn bit_literal_value(digits: &str) -> Result<Datum, EvalError> {
    if digits.is_empty() {
        return bytes_to_value(Vec::new());
    }
    if digits.len() > 64 {
        return Err(EvalError::Unsupported("bit literal wider than 64 bits"));
    }
    let value = u64::from_str_radix(digits, 2).expect("normalized binary digits");
    let byte_len = digits.len().div_ceil(8);
    let bytes = value.to_be_bytes();
    bytes_to_value(bytes[8 - byte_len..].to_vec())
}

/// Source `Datum.SetValueWithDefaultCollation`/`SetValue`, whose
/// `case BitLiteral` and `case HexLiteral` arms BOTH read
/// "Store as BinaryLiteral for Bit and Hex literals" and call
/// `SetBinaryLiteral` (`pkg/types/datum.go:626-630`). A hex or bit literal
/// is therefore `KindBinaryLiteral` in Go from the moment the parser driver
/// builds it -- never `KindBytes` -- and that kind is what tells an
/// arithmetic, comparison or numeric-function context to read the octets as
/// an unsigned INTEGER rather than as text.
///
/// Carrying it as `Datum::Bytes` was not merely a refusal: it made
/// `-0x1A` answer `-0` instead of Go's `-26`, `ABS(b'11')` answer `0`
/// instead of `3`, and `0x1A > 25` answer `0` instead of `1` -- wrong
/// values, silently.
fn bytes_to_value(bytes: Vec<u8>) -> Result<Datum, EvalError> {
    Ok(Datum::new_binary_literal(BinaryLiteral::from(bytes)))
}
