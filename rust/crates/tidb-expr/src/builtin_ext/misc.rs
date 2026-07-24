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

//! Stateless miscellaneous scalar builtins. This is a separate family so
//! unrelated expression workers do not have to edit the central dispatcher.

use crate::coerce::coerce_str;
use crate::{Datum, Decimal, EvalError};
use des::cipher::{Block, BlockCipherEncrypt, KeyInit};
use des::Des;

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("ANY_VALUE", [value]) => Some(Ok(value.clone())),
        // Go's nameConstFunctionClass selects a typed signature from the
        // second argument and every builtinNameConst*Sig evaluator returns
        // that argument directly.  The first argument is column-label
        // metadata, not part of the scalar value.  This value-only leaf can
        // therefore preserve every representable Datum without converting
        // it through a text or numeric signature.  ETDatetime, ETDuration,
        // ETJson, and ETVectorFloat32 remain explicit boundaries because the
        // seed Datum domain intentionally has no corresponding variants.
        ("NAME_CONST", [_, value]) => Some(Ok(value.clone())),
        ("IS_UUID", [value]) => Some(is_uuid(value)),
        ("UUID_VERSION", [value]) => Some(uuid_version(value)),
        ("UUID_TIMESTAMP", [value]) => Some(uuid_timestamp(value)),
        ("UUID_TO_BIN", [value]) => Some(uuid_to_bin(value, None)),
        ("UUID_TO_BIN", [value, flag]) => Some(uuid_to_bin(value, Some(flag))),
        ("BIN_TO_UUID", [value]) => Some(bin_to_uuid(value, None)),
        ("BIN_TO_UUID", [value, flag]) => Some(bin_to_uuid(value, Some(flag))),
        ("TIDB_SHARD", [value]) => Some(tidb_shard(value)),
        ("VITESS_HASH", [value]) => Some(vitess_hash(value)),
        _ => None,
    }
}

/// `UUID_TO_BIN(string_uuid, swap_flag)`, ported from
/// `builtinUUIDToBinSig.evalString` in `pkg/expression/builtin_miscellaneous.go`.
/// The Go signature is binary `ETString`: successful output is therefore raw
/// sixteen-byte data, not UTF-8 text.  `StringDatum` and `Bytes` both retain
/// those bytes; numeric values use the source's `ETString` coercion.  The
/// strict whitespace check is intentionally performed before `parse_uuid`,
/// because MySQL rejects surrounding spaces although `google/uuid.Parse`
/// accepts the inner spelling.
fn uuid_to_bin(value: &Datum, flag: Option<&Datum>) -> Result<Datum, EvalError> {
    let Some(input) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    if std::str::from_utf8(&input)
        .map(|text| text.trim() != text)
        .unwrap_or(false)
    {
        return Err(EvalError::Unsupported("invalid UUID_TO_BIN whitespace"));
    }
    let Some(uuid) = parse_uuid(&input) else {
        return Err(EvalError::Unsupported("invalid UUID for UUID_TO_BIN"));
    };
    let mut output = uuid;
    if eval_int_flag(flag) != 0 {
        output = swap_binary_uuid(&output);
    }
    Ok(Datum::Bytes(output.to_vec()))
}

/// `BIN_TO_UUID(binary_uuid, swap_flag)`, ported from
/// `builtinBinToUUIDSig.evalString` in the same Go source.  The first
/// argument is consumed as raw string bytes and must be exactly one UUID
/// payload (16 bytes); unlike `coerce_str`, this path deliberately accepts
/// arbitrary non-UTF-8 bytes so binary UUID data cannot be corrupted by a
/// text conversion.
fn bin_to_uuid(value: &Datum, flag: Option<&Datum>) -> Result<Datum, EvalError> {
    let Some(input) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    if input.len() != 16 {
        return Err(EvalError::Unsupported("invalid binary UUID length"));
    }
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&input);
    let output = if eval_int_flag(flag) != 0 {
        format_uuid_swapped(&uuid)
    } else {
        format_uuid(&uuid)
    };
    Ok(Datum::new_string(output))
}

/// The Go `EvalString` payload boundary used by UUID_TO_BIN/BIN_TO_UUID.
/// String/bytes datums are returned byte-for-byte; scalar numeric values are
/// stringified, while NULL propagates without evaluating the optional flag.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        _ => Ok(coerce_str(value)?.map(String::into_bytes)),
    }
}

fn eval_int_flag(flag: Option<&Datum>) -> i64 {
    flag.filter(|value| !matches!(value, Datum::Null))
        .map(crate::cast::to_i64_signed)
        .unwrap_or(0)
}

/// Go's `swapBinaryUUID` permutation, shared by both directions.  Applying it
/// to the sixteen bytes before formatting is equivalent to Go's
/// `swapStringUUID` output permutation.
fn swap_binary_uuid(uuid: &[u8; 16]) -> [u8; 16] {
    [
        uuid[6], uuid[7], uuid[4], uuid[5], uuid[0], uuid[1], uuid[2], uuid[3], uuid[8], uuid[9],
        uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15],
    ]
}

fn format_uuid(uuid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid[0],
        uuid[1],
        uuid[2],
        uuid[3],
        uuid[4],
        uuid[5],
        uuid[6],
        uuid[7],
        uuid[8],
        uuid[9],
        uuid[10],
        uuid[11],
        uuid[12],
        uuid[13],
        uuid[14],
        uuid[15]
    )
}

/// Go's `swapStringUUID` is not the inverse byte permutation used by
/// UUID_TO_BIN: it rearranges the textual UUID fields as `B-C-A_suffix-A_prefix`.
/// Formatting from bytes keeps that exact field boundary without any lossy
/// UTF-8 round trip.
fn format_uuid_swapped(uuid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid[4],
        uuid[5],
        uuid[6],
        uuid[7],
        uuid[2],
        uuid[3],
        uuid[0],
        uuid[1],
        uuid[8],
        uuid[9],
        uuid[10],
        uuid[11],
        uuid[12],
        uuid[13],
        uuid[14],
        uuid[15]
    )
}

/// Vitess' shard-key hash: DES-ECB of the big-endian key under an all-zero
/// 64-bit key. Port of `vitess.HashUint64` (`pkg/util/vitess/vitess_hash.go`),
/// shared by `TIDB_SHARD` (which takes it mod 256) and `VITESS_HASH` (which
/// returns it whole). RustCrypto's audited DES is used directly; replacing this
/// with a lookup or output-derived hash would silently change shard placement.
fn vitess_hash_u64(shard_key: u64) -> u64 {
    let mut block = Block::<Des>::default();
    block.copy_from_slice(&shard_key.to_be_bytes());
    let cipher =
        Des::new_from_slice(&[0_u8; 8]).expect("DES accepts its fixed-width all-zero Vitess key");
    cipher.encrypt_block(&mut block);
    u64::from_be_bytes(block.into())
}

/// `TIDB_SHARD(value)`, ported from `builtinTidbShardSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB first casts the one argument to signed `ETInt`, then Vitess-hashes its
/// two's-complement `uint64` bits and takes the big-endian ciphertext's low
/// byte. The bucket count is 256, so the low byte is exactly the modulo.
fn tidb_shard(value: &Datum) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(Datum::Null);
    }
    // `WrapWithCastAsInt` selects the ETInt cast signature before
    // `builtinTidbShardSig.evalInt` runs. Reuse the same integer-prefix
    // conversion used by this evaluator's SIGNED cast for scalar values.
    let shard_key = crate::cast::to_i64_signed(value) as u64;
    Ok(Datum::UInt(vitess_hash_u64(shard_key) % 256))
}

/// `VITESS_HASH(shard_key)`, ported from `builtinVitessHashSig.evalInt`. Like
/// `TIDB_SHARD`, the ETInt-coerced argument is Vitess-hashed, but the whole
/// 64-bit digest is returned. The result column is UNSIGNED, so it is a
/// `Datum::UInt`.
fn vitess_hash(value: &Datum) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(Datum::Null);
    }
    let shard_key = crate::cast::to_i64_signed(value) as u64;
    Ok(Datum::UInt(vitess_hash_u64(shard_key)))
}

/// `IS_UUID(value)`, ported from `builtinIsUUIDSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// The Go implementation deliberately delegates validation to
/// `github.com/google/uuid.Parse`, after rejecting leading/trailing
/// whitespace. That parser accepts RFC UUID text, raw 32-hex text, URNs,
/// and its 38-byte "Microsoft style" form where only the *middle* 36 bytes
/// are examined. Keep that last behavior: it is explicitly covered by
/// TiDB's `TestIsUUID` and is why this is not a canonical-format validator.
fn is_uuid(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    if value.trim() != value {
        return Ok(Datum::Int(0));
    }
    Ok(Datum::Int(i64::from(
        parse_uuid(value.as_bytes()).is_some(),
    )))
}

/// `UUID_VERSION(value)`, ported from `builtinUUIDVersionSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB invokes `github.com/google/uuid.Parse` over an `ETString` argument,
/// then returns the high nibble of UUID byte 6. The scalar domain retains the
/// string coercion and all parser-accepted UUID spellings. It deliberately
/// does not reproduce TiDB's diagnostic payload for malformed UUIDs (error
/// 1411 and its value/type text): [`EvalError`] has no SQL error-code carrier
/// yet, so malformed input is an evaluator error instead. The current
/// [`Datum::String`] domain is UTF-8 text rather than TiDB's raw `BINARY` byte
/// strings, so invalid-UTF-8 arguments and their charset/binary metadata are
/// not representable; every successful UUID spelling is ASCII. No collation
/// or warning behavior is omitted for successful scalar values.
fn uuid_version(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    let Some(bytes) = parse_uuid(value.as_bytes()) else {
        return Err(EvalError::Unsupported("invalid UUID for UUID_VERSION"));
    };
    Ok(Datum::Int(i64::from(bytes[6] >> 4)))
}

/// `UUID_TIMESTAMP(value)`, ported from `builtinUUIDTimestampSig.evalDecimal`
/// in `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB accepts the UUID spellings parsed by `google/uuid.Parse`, returns
/// `NULL` for a valid UUID that does not carry a timestamp, and renders the
/// Version 1, 6, and 7 time as an exact `DECIMAL(18,6)`. The timestamp
/// decoding below is a direct port of `google/uuid.UUID.Time` and
/// `Time.UnixTime` from TiDB's pinned module: the 1582 UUID epoch is converted
/// to Unix microseconds before making a fixed six-place decimal. Invalid text
/// remains an evaluator error, matching TiDB's error outcome; `EvalError`
/// cannot yet preserve TiDB error 1411's diagnostic payload.
fn uuid_timestamp(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    let Some(uuid) = parse_uuid(value.as_bytes()) else {
        return Err(EvalError::Unsupported("invalid UUID for UUID_TIMESTAMP"));
    };

    let timestamp_100ns = match uuid[6] >> 4 {
        1 => uuid_v1_timestamp_100ns(&uuid),
        6 => uuid_v6_timestamp_100ns(&uuid),
        7 => uuid_v7_timestamp_100ns(&uuid),
        _ => return Ok(Datum::Null),
    };
    // `google/uuid.Time.UnixTime` subtracts the UUID epoch in 100ns ticks,
    // then TiDB divides its nanoseconds by 1000 and rounds at six decimal
    // places. The result is precisely truncation toward zero to microseconds.
    let unix_micros = (timestamp_100ns - UUID_EPOCH_100NS) / 10;
    Ok(Datum::Decimal(decimal_micros(unix_micros)))
}

const UUID_EPOCH_100NS: i64 = 122_192_928_000_000_000;

fn uuid_v1_timestamp_100ns(uuid: &[u8; 16]) -> i64 {
    i64::from(u32::from_be_bytes([uuid[0], uuid[1], uuid[2], uuid[3]]))
        | (i64::from(u16::from_be_bytes([uuid[4], uuid[5]])) << 32)
        | (i64::from(u16::from_be_bytes([uuid[6], uuid[7]]) & 0x0fff) << 48)
}

fn uuid_v6_timestamp_100ns(uuid: &[u8; 16]) -> i64 {
    (i64::from(u32::from_be_bytes([uuid[0], uuid[1], uuid[2], uuid[3]])) << 28)
        | (i64::from(u16::from_be_bytes([uuid[4], uuid[5]])) << 12)
        | i64::from(u16::from_be_bytes([uuid[6], uuid[7]]) & 0x0fff)
}

fn uuid_v7_timestamp_100ns(uuid: &[u8; 16]) -> i64 {
    let first_eight = u64::from_be_bytes([
        uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7],
    ]);
    ((first_eight >> 16) * 10_000) as i64 + UUID_EPOCH_100NS
}

fn decimal_micros(micros: i64) -> Decimal {
    let (negative, magnitude) = if micros < 0 {
        (true, micros.unsigned_abs())
    } else {
        (false, micros as u64)
    };
    let value = format!("{}.{:06}", magnitude / 1_000_000, magnitude % 1_000_000);
    // `Decimal::from_literal` intentionally accepts a sign-free AST literal,
    // so construct and negate only after parsing the magnitude.
    let value = Decimal::from_literal(&value);
    if negative {
        value.negate()
    } else {
        value
    }
}

/// Exact parse shape of the `google/uuid.Parse` implementation TiDB calls,
/// decoded to its 16 bytes. Parsing works on bytes because Go selects forms
/// by byte length and must retain its intentionally permissive 38-byte form.
fn parse_uuid(value: &[u8]) -> Option<[u8; 16]> {
    let canonical = match value.len() {
        36 => value,
        45 if value[..9].eq_ignore_ascii_case(b"urn:uuid:") => &value[9..],
        // `google/uuid.Parse` deliberately inspects only the inner 36 bytes
        // of a 38-byte value; retain its ignored final byte exactly.
        38 => &value[1..37],
        32 => value,
        _ => return None,
    };

    let mut bytes = [0_u8; 16];
    if canonical.len() == 32 {
        for (byte, pair) in bytes.iter_mut().zip(canonical.chunks_exact(2)) {
            *byte = hex_pair(pair[0], pair[1])?;
        }
        return Some(bytes);
    }

    if canonical.len() != 36
        || canonical[8] != b'-'
        || canonical[13] != b'-'
        || canonical[18] != b'-'
        || canonical[23] != b'-'
    {
        return None;
    }

    let mut hex = canonical.iter().copied().filter(|byte| *byte != b'-');
    for byte in &mut bytes {
        *byte = hex_pair(hex.next()?, hex.next()?)?;
    }
    if hex.next().is_some() {
        return None;
    }
    Some(bytes)
}

fn hex_pair(high: u8, low: u8) -> Option<u8> {
    Some((hex_nibble(high)? << 4) | hex_nibble(low)?)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;

    /// Exact scalar vectors from `TestAnyValue` in
    /// `pkg/expression/builtin_miscellaneous_test.go`. Each Go signature's
    /// evaluator delegates directly to its one argument.
    #[test]
    fn any_value_returns_its_argument() {
        let source_float = "3.1415926"
            .parse::<f64>()
            .expect("the exact Go test vector is a valid float");
        let cases = [
            (Datum::Null, Datum::Null),
            (Datum::Int(1234), Datum::Int(1234)),
            (Datum::Int(-0x99), Datum::Int(-0x99)),
            (Datum::Real(source_float), Datum::Real(source_float)),
            (
                Datum::new_string("Hello, World".to_string()),
                Datum::new_string("Hello, World".to_string()),
            ),
        ];
        for (argument, want) in cases {
            assert_eq!(
                dispatch("ANY_VALUE", std::slice::from_ref(&argument))
                    .expect("ANY_VALUE must dispatch")
                    .expect("ANY_VALUE must evaluate"),
                want
            );
        }
    }

    #[test]
    fn dispatch_declines_wrong_arity_and_foreign_functions() {
        assert!(dispatch("ANY_VALUE", &[]).is_none());
        assert!(dispatch("ANY_VALUE", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(dispatch("NAME_CONST", &[]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::new_string("name".to_string())]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::Int(1)]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]).is_none());
    }

    /// Exact scalar vectors from `TestNameConst` in
    /// `pkg/expression/builtin_miscellaneous_test.go`.  Every Go
    /// `builtinNameConst*Sig` returns its second argument without changing the
    /// payload; this direct table keeps NULL, signed/unsigned integers,
    /// real, string, binary, and decimal values in their original Datum
    /// domains.  Go's typed temporal/duration/JSON/vector signatures and
    /// FieldType/column-label metadata are deliberately not fabricated here:
    /// the seed Datum domain has no representable variants for them.
    #[test]
    fn name_const_preserves_representable_value_domains() {
        let decimal = crate::Decimal::from_literal("123.123");
        let source_float = "3.14159"
            .parse::<f64>()
            .expect("the exact Go vector is a valid float");
        let cases = [
            (Datum::new_string("test_int"), Datum::Int(3)),
            (Datum::new_string("test_uint"), Datum::UInt(u64::MAX)),
            (Datum::new_string("test_float"), Datum::Real(source_float)),
            (Datum::new_string("test_string"), Datum::new_string("TiDB")),
            (
                Datum::new_string("test_binary"),
                Datum::new_bytes(vec![0, 0xff, 0x80]),
            ),
            (Datum::new_string("test_null"), Datum::Null),
            (Datum::new_string("test_decimal"), Datum::Decimal(decimal)),
            // A NULL label is accepted by Go's ETString conversion; it does
            // not alter the value returned by NAME_CONST.
            (Datum::Null, Datum::Int(-7)),
        ];
        for (name, value) in cases {
            let got = dispatch("NAME_CONST", &[name, value.clone()])
                .expect("NAME_CONST must dispatch for two arguments")
                .expect("NAME_CONST must preserve the scalar value");
            assert_eq!(got, value);
        }
    }

    /// Exact version vectors from `TestUUIDVersion` in
    /// `pkg/expression/builtin_miscellaneous_test.go`; the extra spellings
    /// exercise the same `google/uuid.Parse` compatibility shape as TiDB.
    #[test]
    fn uuid_version_matches_go_uuid_parse_and_version_nibble() {
        let cases = [
            ("5f13f854-d74a-11f0-9b7a-0ae0156bd76b", 1),
            ("c6437ef1-5b86-3a4e-a071-c2d4ad414e65", 3),
            ("a3e3b4a1-ea6d-471e-9860-8303a8b261f6", 4),
            ("271a8175-dadd-5df9-b0bd-20a4a0b441e6", 5),
            ("1f0e48c1-7860-69cc-9b3f-35f89c103d4d", 6),
            ("019b1440-87b7-7380-ab00-ce413e795004", 7),
            ("6ccd780cbaba102695645b8c656024db", 1),
            ("urn:uuid:6ccd780c-baba-1026-9564-5b8c656024db", 1),
            ("{99a9ad03-5298-11ec-8f5c-00ff90147ac3*", 1),
            ("123e4567-e89b-02d3-a456-426614174000", 0),
        ];
        for (text, want) in cases {
            assert_eq!(
                dispatch("UUID_VERSION", &[Datum::new_string(text.to_string())])
                    .expect("UUID_VERSION must dispatch")
                    .expect("well-formed UUID must evaluate"),
                Datum::Int(want),
                "UUID_VERSION({text:?})"
            );
        }
        assert_eq!(
            dispatch("UUID_VERSION", &[Datum::Null])
                .expect("UUID_VERSION must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("UUID_VERSION", &[Datum::new_string("abc".to_string())])
                .expect("UUID_VERSION must dispatch")
                .is_err()
        );
    }

    /// Exact vectors from `TestIsUUID` in
    /// `pkg/expression/builtin_miscellaneous_test.go`, including the
    /// `google/uuid.Parse` 38-byte compatibility quirk.
    #[test]
    fn is_uuid_matches_go_parse_acceptance() {
        let cases = [
            ("6ccd780c-baba-1026-9564-5b8c656024db", 1),
            ("6CCD780C-BABA-1026-9564-5B8C656024DB", 1),
            ("6ccd780cbaba102695645b8c656024db", 1),
            ("{6ccd780c-baba-1026-9564-5b8c656024db}", 1),
            ("6ccd780c-baba-1026-9564-5b8c6560", 0),
            ("6CCD780C-BABA-1026-9564-5B8C656024DQ", 0),
            (" 6ccd780c-baba-1026-9564-5b8c656024db", 0),
            ("6ccd780c-baba-1026-9564-5b8c656024db ", 0),
            (" 6ccd780c-baba-1026-9564-5b8c656024db ", 0),
            // `uuid.Parse` examines only the middle 36 bytes in a 38-byte
            // input; the leading `{` and trailing `*` are both ignored.
            ("{99a9ad03-5298-11ec-8f5c-00ff90147ac3*", 1),
            ("urn:uuid:99a9ad03-5298-11ec-8f5c-00ff90147ac3", 1),
        ];
        for (text, want) in cases {
            assert_eq!(
                dispatch("IS_UUID", &[Datum::new_string(text.to_string())])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(want),
                "IS_UUID({text:?})"
            );
        }
        assert_eq!(
            dispatch("IS_UUID", &[Datum::Null])
                .expect("IS_UUID must dispatch")
                .expect("IS_UUID must evaluate"),
            Datum::Null
        );
    }

    #[test]
    fn is_uuid_coerces_the_supported_scalar_domain_to_etstring() {
        // `isUUIDFunctionClass` builds its argument as ETString, so native
        // scalar values are text-coerced before the parser runs.
        for value in [Datum::Int(1), Datum::Real(1.0)] {
            assert_eq!(
                dispatch("IS_UUID", &[value])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(0)
            );
        }
    }

    /// Exact source vectors from `TestUUIDTimestamp` in
    /// `pkg/expression/builtin_miscellaneous_test.go`, plus Go's accepted
    /// compact UUID spelling and its invalid-input error outcome.
    #[test]
    fn uuid_timestamp_matches_go_versioned_timestamp_semantics() {
        let cases = [
            ("5f13f854-d74a-11f0-9b7a-0ae0156bd76b", "1765537487.118139"),
            ("1f0e48c1-7860-69cc-9b3f-35f89c103d4d", "1766995078.970004"),
            ("019b1440-87b7-7380-ab00-ce413e795004", "1765571332.023000"),
            ("6ccd780cbaba102695645b8c656024db", "-11129156903.290674"),
        ];
        for (text, want) in cases {
            let expected = if let Some(magnitude) = want.strip_prefix('-') {
                crate::Decimal::from_literal(magnitude).negate()
            } else {
                crate::Decimal::from_literal(want)
            };
            assert_eq!(
                dispatch("UUID_TIMESTAMP", &[Datum::new_string(text.to_string())])
                    .expect("UUID_TIMESTAMP must dispatch")
                    .expect("timestamp UUID must evaluate"),
                Datum::Decimal(expected),
                "UUID_TIMESTAMP({text:?})"
            );
        }
        for text in [
            "c6437ef1-5b86-3a4e-a071-c2d4ad414e65",
            "a3e3b4a1-ea6d-471e-9860-8303a8b261f6",
            "271a8175-dadd-5df9-b0bd-20a4a0b441e6",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            assert_eq!(
                dispatch("UUID_TIMESTAMP", &[Datum::new_string(text.to_string())])
                    .expect("UUID_TIMESTAMP must dispatch")
                    .expect("valid non-timestamp UUID must evaluate"),
                Datum::Null,
                "UUID_TIMESTAMP({text:?})"
            );
        }
        assert_eq!(
            dispatch("UUID_TIMESTAMP", &[Datum::Null])
                .expect("UUID_TIMESTAMP must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("UUID_TIMESTAMP", &[Datum::new_string("abc".to_string())])
                .expect("UUID_TIMESTAMP must dispatch")
                .is_err()
        );
    }

    /// Exact success/NULL/error vectors from `TestUUIDToBin` and
    /// `TestBinToUUID` in `pkg/expression/builtin_miscellaneous_test.go`.
    /// UUID_TO_BIN returns raw bytes (including the swap permutation), while
    /// BIN_TO_UUID accepts those bytes without UTF-8 decoding and restores a
    /// lower-case canonical spelling. Warning-count behavior for a malformed
    /// textual swap flag is a session boundary; its ETInt value conversion is
    /// still covered here.
    #[test]
    fn uuid_binary_builtins_match_go_swap_and_raw_byte_vectors() {
        let canonical = "6ccd780c-baba-1026-9564-5b8c656024db";
        let normal = vec![
            0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb,
        ];
        let swapped = vec![
            0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb,
        ];

        for spelling in [
            canonical,
            "6CCD780C-BABA-1026-9564-5B8C656024DB",
            "6ccd780cbaba102695645b8c656024db",
            "{6ccd780c-baba-1026-9564-5b8c656024db}",
        ] {
            assert_eq!(
                dispatch("UUID_TO_BIN", &[Datum::new_string(spelling)])
                    .expect("UUID_TO_BIN must dispatch")
                    .expect("valid UUID must evaluate"),
                Datum::Bytes(normal.clone()),
                "UUID_TO_BIN({spelling:?})"
            );
        }
        assert_eq!(
            dispatch(
                "UUID_TO_BIN",
                &[Datum::new_string(canonical), Datum::Int(1)],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("swap flag must evaluate"),
            Datum::Bytes(swapped.clone())
        );
        assert_eq!(
            dispatch("UUID_TO_BIN", &[Datum::new_string(canonical), Datum::Null],)
                .expect("UUID_TO_BIN must dispatch")
                .expect("NULL swap flag defaults to zero"),
            Datum::Bytes(normal.clone())
        );
        // Go records a truncation warning for the textual flag "a" but its
        // ETInt value is zero; the warning channel is outside this seed while
        // the value-domain result remains pinned here.
        assert_eq!(
            dispatch(
                "UUID_TO_BIN",
                &[Datum::new_string(canonical), Datum::new_string("a")],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::Bytes(normal.clone())
        );
        assert_eq!(
            dispatch("UUID_TO_BIN", &[Datum::Null])
                .expect("UUID_TO_BIN must dispatch")
                .expect("NULL UUID must evaluate"),
            Datum::Null
        );
        for invalid in [
            "6ccd780c-baba-1026-9564-5b8c6560",
            " 6ccd780c-baba-1026-9564-5b8c656024db",
            "6ccd780c-baba-1026-9564-5b8c656024db ",
            " 6ccd780c-baba-1026-9564-5b8c656024db ",
        ] {
            assert!(
                dispatch("UUID_TO_BIN", &[Datum::new_string(invalid)])
                    .expect("UUID_TO_BIN must dispatch")
                    .is_err(),
                "invalid UUID_TO_BIN input {invalid:?}"
            );
        }

        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(normal.clone())])
                .expect("BIN_TO_UUID must dispatch")
                .expect("binary UUID must evaluate"),
            Datum::new_string(canonical)
        );
        assert_eq!(
            dispatch(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::Int(1)],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("swap flag must evaluate"),
            Datum::new_string("baba1026-780c-6ccd-9564-5b8c656024db")
        );
        assert_eq!(
            dispatch(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::new_string("a")],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::new_string(canonical)
        );
        // A raw binary UUID is valid even though the payload is not UTF-8.
        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(swapped)])
                .expect("BIN_TO_UUID must dispatch")
                .expect("raw bytes must evaluate"),
            Datum::new_string("1026baba-6ccd-780c-9564-5b8c656024db")
        );
        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Null])
                .expect("BIN_TO_UUID must dispatch")
                .expect("NULL binary UUID must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(normal[..15].to_vec())])
                .expect("BIN_TO_UUID must dispatch")
                .is_err()
        );
        assert!(dispatch("UUID_TO_BIN", &[]).is_none());
        assert!(dispatch(
            "UUID_TO_BIN",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_none());
        assert!(dispatch("BIN_TO_UUID", &[]).is_none());
        assert!(dispatch(
            "BIN_TO_UUID",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_none());
    }

    /// Exact vectors from `TestTidbShard` in
    /// `pkg/expression/builtin_miscellaneous_test.go`. The ciphertext bytes
    /// are produced by Vitess' `HashUint64` (DES-ECB, all-zero key), not by a
    /// result-derived lookup. The additional scalar/string rows pin the
    /// `WrapWithCastAsInt` coercion boundary before hashing.
    #[test]
    fn tidb_shard_matches_vitess_des_and_etint_coercion() {
        let integer_cases = [
            (Datum::Int(-1), 81),
            (Datum::Int(0), 167),
            (Datum::Int(1), 214),
            (Datum::Int(9_999_999_999_999_999), 63),
            // An unsigned source is cast to ETInt and retains its two's-
            // complement bits before `HashUint64` receives the uint64.
            (Datum::UInt(u64::MAX), 81),
        ];
        for (value, want) in integer_cases {
            assert_eq!(
                dispatch("TIDB_SHARD", &[value])
                    .expect("TIDB_SHARD must dispatch")
                    .expect("integer TIDB_SHARD must evaluate"),
                Datum::UInt(want),
            );
        }

        for (text, want) in [
            ("abc", 167),
            ("ope", 167),
            ("wopddd", 167),
            ("1", 214),
            ("-1", 81),
            ("1.9", 214),
        ] {
            assert_eq!(
                dispatch("TIDB_SHARD", &[Datum::new_string(text.to_string())])
                    .expect("TIDB_SHARD must dispatch")
                    .expect("string TIDB_SHARD must evaluate"),
                Datum::UInt(want),
                "TIDB_SHARD({text:?})",
            );
        }

        assert_eq!(
            dispatch("TIDB_SHARD", &[Datum::Real(1.9)])
                .expect("TIDB_SHARD must dispatch")
                .expect("real TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            dispatch(
                "TIDB_SHARD",
                &[Datum::Decimal(crate::Decimal::from_literal("1.9"))],
            )
            .expect("TIDB_SHARD must dispatch")
            .expect("decimal TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            dispatch("TIDB_SHARD", &[Datum::Null])
                .expect("TIDB_SHARD must dispatch")
                .expect("NULL TIDB_SHARD must evaluate"),
            Datum::Null,
        );

        // The Go function class rejects every arity other than one before
        // evaluation; this family dispatch has the same boundary.
        assert!(dispatch("TIDB_SHARD", &[]).is_none());
        assert!(dispatch("TIDB_SHARD", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(dispatch("UNKNOWN", &[Datum::Int(1)]).is_none());
    }

    /// `VITESS_HASH` returns the whole Vitess DES digest as an UNSIGNED value.
    /// Vectors from `pkg/util/vitess/vitess_hash_test.go` TestVitessHash plus
    /// the two's-complement `u64::MAX` case.
    #[test]
    fn vitess_hash_full_digest() {
        let cases = [
            (Datum::Int(30_375_298_039), 221_350_820_965_191_987_u64),
            (Datum::Int(1123), 223_867_565_019_887_818),
            (Datum::Int(30_573_721_600), 2_233_051_190_281_965_565),
            (Datum::Int(116), 2_168_352_374_666_430_780),
            (Datum::Int(1), 1_615_456_034_434_468_822),
            (Datum::Int(0), 10_134_873_677_816_210_343),
            // An unsigned source is cast to ETInt and keeps its two's-complement
            // bits before the hash receives the uint64.
            (Datum::UInt(u64::MAX), 3_843_066_582_818_235_473),
        ];
        for (value, want) in cases {
            assert_eq!(
                dispatch("VITESS_HASH", &[value])
                    .expect("VITESS_HASH must dispatch")
                    .expect("VITESS_HASH must evaluate"),
                Datum::UInt(want),
            );
        }
        assert_eq!(
            dispatch("VITESS_HASH", &[Datum::Null])
                .expect("VITESS_HASH must dispatch")
                .expect("NULL VITESS_HASH must evaluate"),
            Datum::Null,
        );
        assert!(dispatch("VITESS_HASH", &[]).is_none());
    }
}
