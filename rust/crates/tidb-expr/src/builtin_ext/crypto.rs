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

//! `crypto` family builtins. Every builtin here is transcreated from
//! `pkg/expression/builtin_encryption.go`; test vectors come from
//! `pkg/expression/builtin_encryption_test.go`.
//!
//! Deliberately not yet ported from `builtin_encryption.go` because their
//! required session or binary-value contracts are not present:
//! - `AES_ENCRYPT`/`AES_DECRYPT` (`builtinAesEncrypt*`/`builtinAesDecrypt*`):
//!   the default `block_encryption_mode` (`aes-128-ecb`, no IV) two-argument
//!   form is ported below; it is session-independent. The other modes in the
//!   `aesModes` table (192/256 key sizes and the CBC/OFB/CFB modes that need an
//!   IV argument) are selected by the `block_encryption_mode` session variable
//!   and remain a session boundary, as does the ignored-IV warning for a third
//!   argument in a no-IV mode.
//! - `UNCOMPRESS`/`UNCOMPRESSED_LENGTH` (`builtinUncompressSig`/
//!   `builtinUncompressedLengthSig`): ported below. Both decode the zlib
//!   framing (a 4-byte little-endian original-length prefix + a zlib stream),
//!   and DEFLATE *decoders* are interoperable, so they faithfully consume Go
//!   `compress/zlib` output. Their corruption warnings belong to the statement
//!   context and are not fabricated here.
//! - `COMPRESS` (`builtinCompressSig`): deferred. Its output bytes are produced
//!   by Go's `compress/flate` *encoder*, whose exact framing (note the
//!   `00 00 FF FF` sync-flush artifact Go emits) is not byte-reproducible by a
//!   different DEFLATE encoder such as the `flate2`/miniz_oxide backend. TiDB's
//!   own test pins those exact bytes, so a non-identical encoder would not be a
//!   faithful transcreation; only the interoperable decoders are ported.
//! - `RANDOM_BYTES` (`builtinRandomBytesSig`): non-deterministic
//!   (`crypto/rand`) — unverifiable against a static golden file.
//! - `PASSWORD` (`builtinPasswordSig`): the value contract is ported below;
//!   its deprecation warning remains a statement-context boundary because
//!   this value-only family dispatch has no warning channel.
//! - `VALIDATE_PASSWORD_STRENGTH` (`builtinValidatePasswordStrengthSig`):
//!   reads the current user and `validate_password.*` global system
//!   variables via the session validation plugin — session state out of
//!   the dispatch domain.
//! - `ENCODE`/`DECODE` (`builtinEncodeSig`/`builtinDecodeSig`): deprecated
//!   password-keyed stream crypt (`encrypt.SQLEncode`/`SQLDecode`) now has
//!   its UTF-8/default-charset scalar value contract below; connection
//!   charset/session warning behavior and arbitrary typed collation metadata
//!   remain explicit boundaries.
//! - `SM3` (`builtinSM3Sig`): TiDB extension hash backed by
//!   `pkg/parser/auth`; the complete expression package must route it through
//!   the existing Rust parser-auth implementation.

use std::io::Read;

use aes::cipher::{Block, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};
use aes::Aes128;
use flate2::read::ZlibDecoder;
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

use crate::{Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
/// `SHA` is a true alias of `SHA1`: `builtin.go`'s `funcs` map registers
/// both `ast.SHA` and `ast.SHA1` to the same `sha1FunctionClass`.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("MD5", 1) => Some(hash_unary::<Md5>(&vals[0])),
        ("SHA" | "SHA1", 1) => Some(hash_unary::<Sha1>(&vals[0])),
        ("SHA2", 2) => Some(sha2_hash(&vals[0], &vals[1])),
        ("PASSWORD", 1) => Some(password_hash(&vals[0])),
        ("ENCODE", 2) => Some(sql_encode(&vals[0], &vals[1])),
        ("DECODE", 2) => Some(sql_decode(&vals[0], &vals[1])),
        ("AES_ENCRYPT", 2) => Some(aes_encrypt(&vals[0], &vals[1])),
        ("AES_DECRYPT", 2) => Some(aes_decrypt(&vals[0], &vals[1])),
        ("UNCOMPRESS", 1) => Some(uncompress(&vals[0])),
        ("UNCOMPRESSED_LENGTH", 1) => Some(uncompressed_length(&vals[0])),
        _ => None,
    }
}

/// Inflates a zlib stream, returning `None` on any decode error. Port of the
/// decode half of `pkg/expression/builtin_encryption.go`'s `inflate`
/// (`compress/zlib`), which reads the whole stream and verifies its Adler-32
/// checksum; trailing bytes past the stream end are ignored.
fn inflate(data: &[u8]) -> Option<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).ok().map(|_| out)
}

/// `UNCOMPRESS(payload)`. Port of `builtinUncompressSig.evalString`. The 4-byte
/// little-endian prefix records the original length and the remainder is a zlib
/// stream. Empty input yields an empty string; NULL, a too-short/corrupted
/// payload, an undecodable stream, or a stored length below the decompressed
/// length all yield NULL. Go additionally raises a warning, which belongs to the
/// statement context and is not fabricated in this value-only dispatch.
fn uncompress(arg: &Datum) -> Result<Datum, EvalError> {
    let Some(payload) = sql_string_bytes(arg)? else {
        return Ok(Datum::Null);
    };
    if payload.is_empty() {
        return Ok(Datum::new_string(Vec::new()));
    }
    if payload.len() <= 4 {
        // corrupted
        return Ok(Datum::Null);
    }
    let length = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let Some(bytes) = inflate(&payload[4..]) else {
        return Ok(Datum::Null);
    };
    if length < bytes.len() as u32 {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(bytes))
}

/// `UNCOMPRESSED_LENGTH(payload)`. Port of
/// `builtinUncompressedLengthSig.evalInt`: returns the 4-byte little-endian
/// prefix as the original length. NULL yields NULL; empty or too-short/corrupted
/// input yields 0.
fn uncompressed_length(arg: &Datum) -> Result<Datum, EvalError> {
    let Some(payload) = sql_string_bytes(arg)? else {
        return Ok(Datum::Null);
    };
    if payload.is_empty() {
        return Ok(Datum::Int(0));
    }
    if payload.len() <= 4 {
        // corrupted
        return Ok(Datum::Int(0));
    }
    let length = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    Ok(Datum::Int(i64::from(length)))
}

/// AES block size (bytes) and, for the default `aes-128-ecb`, the derived key
/// length.
const AES_BLOCK_SIZE: usize = 16;

/// Derives the encryption key from a password using MySQL's algorithm: fold the
/// key bytes into `AES_BLOCK_SIZE` bytes by XOR, wrapping. Port of
/// `encrypt.DeriveKeyMySQL` with `blockSize == 16` (the default `aes-128-ecb`).
fn derive_key_mysql(key: &[u8]) -> [u8; AES_BLOCK_SIZE] {
    let mut rkey = [0u8; AES_BLOCK_SIZE];
    for (i, &k) in key.iter().enumerate() {
        rkey[i % AES_BLOCK_SIZE] ^= k;
    }
    rkey
}

/// PKCS7-pads `data` in place to a multiple of `AES_BLOCK_SIZE`. Port of
/// `encrypt.PKCS7Pad`: a full-block input still gains a whole padding block.
fn pkcs7_pad(data: &mut Vec<u8>) {
    let pad_len = AES_BLOCK_SIZE - (data.len() % AES_BLOCK_SIZE);
    data.extend(std::iter::repeat_n(pad_len as u8, pad_len));
}

/// PKCS7-unpads `data`, returning `None` on invalid padding. Port of
/// `encrypt.PKCS7Unpad`.
fn pkcs7_unpad(data: &[u8]) -> Option<&[u8]> {
    let length = data.len();
    if length == 0 || !length.is_multiple_of(AES_BLOCK_SIZE) {
        return None;
    }
    let pad = data[length - 1];
    let pad_len = pad as usize;
    if pad_len > AES_BLOCK_SIZE || pad_len == 0 {
        return None;
    }
    for &v in &data[length - pad_len..length - 1] {
        if v != pad {
            return None;
        }
    }
    Some(&data[..length - pad_len])
}

/// `AES_ENCRYPT(str, key_str)` in the default `aes-128-ecb` mode. Port of
/// `builtinAesEncryptSig.evalString` + `encrypt.AESEncryptWithECB`. Either
/// argument being NULL yields NULL.
fn aes_encrypt(str_arg: &Datum, key_arg: &Datum) -> Result<Datum, EvalError> {
    let Some(plain) = sql_string_bytes(str_arg)? else {
        return Ok(Datum::Null);
    };
    let Some(key) = sql_string_bytes(key_arg)? else {
        return Ok(Datum::Null);
    };
    let cipher = Aes128::new_from_slice(&derive_key_mysql(&key))
        .expect("derived AES-128 key is exactly 16 bytes");

    let mut data = plain;
    pkcs7_pad(&mut data);
    for chunk in data.chunks_exact_mut(AES_BLOCK_SIZE) {
        let mut block = Block::<Aes128>::default();
        block.copy_from_slice(chunk);
        cipher.encrypt_block(&mut block);
        chunk.copy_from_slice(&block);
    }
    Ok(Datum::new_string(data))
}

/// `AES_DECRYPT(crypt_str, key_str)` in the default `aes-128-ecb` mode. Port of
/// `builtinAesDecryptSig.evalString` + `encrypt.AESDecryptWithECB`. Either
/// argument being NULL yields NULL, and any decryption error (a length that is
/// not a whole number of blocks, or invalid PKCS7 padding) also yields NULL, as
/// the Go evaluator maps such errors to NULL.
fn aes_decrypt(crypt_arg: &Datum, key_arg: &Datum) -> Result<Datum, EvalError> {
    let Some(crypt) = sql_string_bytes(crypt_arg)? else {
        return Ok(Datum::Null);
    };
    let Some(key) = sql_string_bytes(key_arg)? else {
        return Ok(Datum::Null);
    };
    if !crypt.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Ok(Datum::Null);
    }
    let cipher = Aes128::new_from_slice(&derive_key_mysql(&key))
        .expect("derived AES-128 key is exactly 16 bytes");

    let mut data = crypt;
    for chunk in data.chunks_exact_mut(AES_BLOCK_SIZE) {
        let mut block = Block::<Aes128>::default();
        block.copy_from_slice(chunk);
        cipher.decrypt_block(&mut block);
        chunk.copy_from_slice(&block);
    }
    match pkcs7_unpad(&data) {
        Some(plain) => Ok(Datum::new_string(plain.to_vec())),
        None => Ok(Datum::Null),
    }
}

/// Lowercase hex of `bytes` — the same "0123456789abcdef" alphabet Go's
/// `encoding/hex.EncodeToString` (used by `builtinMD5Sig`) and
/// `fmt.Sprintf("%x", ...)` (used by `builtinSHA1Sig`/`builtinSHA2Sig`)
/// both produce.
fn hex_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(ALPHABET[usize::from(b >> 4)] as char);
        out.push(ALPHABET[usize::from(b & 0xf)] as char);
    }
    out
}

/// Uppercase hex with the same alphabet as Go's `%X` formatter.
fn hex_upper(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(ALPHABET[usize::from(b >> 4)] as char);
        out.push(ALPHABET[usize::from(b & 0xf)] as char);
    }
    out
}

/// `PASSWORD(str)`: double-SHA1 the source `EvalString` bytes, prefix the
/// uppercase hexadecimal digest with `*`, and return an empty string for an
/// empty input. Port of `builtinPasswordSig.evalString` and
/// `auth.EncodePasswordBytes` (`pkg/expression/builtin_encryption.go` and
/// `pkg/parser/auth/mysql_native_password.go`). The Go evaluator also emits
/// `errDeprecatedSyntaxNoReplacement`; warning propagation belongs to the
/// statement context and is intentionally not fabricated in this value-only
/// dispatch.
fn password_hash(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = hash_input(value)? else {
        return Ok(Datum::Null);
    };
    if bytes.is_empty() {
        return Ok(Datum::new_string(""));
    }
    let stage1 = Sha1::digest(bytes.as_slice());
    let stage2 = Sha1::digest(stage1.as_slice());
    Ok(Datum::new_string(format!(
        "*{}",
        hex_upper(stage2.as_slice())
    )))
}

/// The deterministic MySQL 3.21 stream cipher used by TiDB's deprecated
/// `ENCODE(str, password)`/`DECODE(str, password)` functions.  This is a
/// direct port of `pkg/util/encrypt/crypt.go` (`randStruct`, `sqlCrypt`,
/// `SQLEncode`, and `SQLDecode`), including Go's uint32 wrapping arithmetic,
/// password whitespace skipping, 256-byte substitution-table shuffle, and
/// one-byte rolling shift.  The value-only Rust datum domain preserves the
/// arbitrary output bytes inside `Datum::String`; charset conversion and
/// deprecation warnings require session state and are deliberately outside
/// this leaf.
fn sql_encode(data: &Datum, password: &Datum) -> Result<Datum, EvalError> {
    let Some(mut data) = sql_string_bytes(data)? else {
        return Ok(Datum::Null);
    };
    let Some(password) = sql_string_bytes(password)? else {
        return Ok(Datum::Null);
    };
    SqlCrypt::new(&password).encode(&mut data);
    Ok(Datum::new_string(data))
}

/// `DECODE(str, password)`, the inverse stream operation of [`sql_encode`].
fn sql_decode(data: &Datum, password: &Datum) -> Result<Datum, EvalError> {
    let Some(mut data) = sql_string_bytes(data)? else {
        return Ok(Datum::Null);
    };
    let Some(password) = sql_string_bytes(password)? else {
        return Ok(Datum::Null);
    };
    SqlCrypt::new(&password).decode(&mut data);
    Ok(Datum::new_string(data))
}

/// The Go `EvalString` byte boundary used by ENCODE/DECODE.  Unlike
/// `coerce_str`, this deliberately accepts arbitrary string/bytes payloads;
/// SQL's deprecated stream functions operate on bytes, not Unicode scalar
/// values. Numeric values use their ordinary SQL decimal rendering.
fn sql_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::String(value) => Some(value.bytes().to_vec()),
        Datum::Bytes(value) => Some(value.clone()),
        Datum::Int(value) => Some(value.to_string().into_bytes()),
        Datum::UInt(value) => Some(value.to_string().into_bytes()),
        Datum::Decimal(value) => Some(value.to_string().into_bytes()),
        Datum::Real(value) => Some(value.to_string().into_bytes()),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel string argument"));
        }
        other => Some(
            other
                .to_bytes()
                .map_err(|_| EvalError::Unsupported("datum string argument"))?,
        ),
    })
}

#[derive(Clone)]
struct SqlCrypt {
    rand: SqlRand,
    decode: [u8; 256],
    encode: [u8; 256],
    shift: u32,
}

#[derive(Clone, Copy)]
struct SqlRand {
    seed1: u32,
    seed2: u32,
    max_value: u32,
    max_value_dbl: f64,
}

impl SqlRand {
    fn new(password: &[u8]) -> Self {
        let mut nr = 1_345_345_333u32;
        let mut add = 7u32;
        let mut nr2 = 0x1234_5671u32;
        for &password_byte in password {
            if password_byte == b' ' || password_byte == b'\t' {
                continue;
            }
            let byte = u32::from(password_byte);
            nr ^= ((nr & 63).wrapping_add(add))
                .wrapping_mul(byte)
                .wrapping_add(nr << 8);
            nr2 = nr2.wrapping_add((nr2 << 8) ^ nr);
            add = add.wrapping_add(byte);
        }
        let max_value = 0x3fff_ffff;
        Self {
            seed1: (nr & 0x7fff_ffff) % max_value,
            seed2: (nr2 & 0x7fff_ffff) % max_value,
            max_value,
            max_value_dbl: f64::from(max_value),
        }
    }

    fn next(&mut self) -> f64 {
        self.seed1 = (self.seed1.wrapping_mul(3).wrapping_add(self.seed2)) % self.max_value;
        self.seed2 = (self.seed1.wrapping_add(self.seed2).wrapping_add(33)) % self.max_value;
        f64::from(self.seed1) / self.max_value_dbl
    }
}

impl SqlCrypt {
    fn new(password: &[u8]) -> Self {
        let mut rand = SqlRand::new(password);
        let mut decode = [0u8; 256];
        for (index, value) in decode.iter_mut().enumerate() {
            *value = index as u8;
        }
        for i in 0..256 {
            let index = (rand.next() * 255.0) as usize;
            decode.swap(index, i);
        }
        let mut encode = [0u8; 256];
        for (i, &value) in decode.iter().enumerate() {
            encode[value as usize] = i as u8;
        }
        Self {
            rand,
            decode,
            encode,
            shift: 0,
        }
    }

    fn encode(&mut self, data: &mut [u8]) {
        for byte in data {
            self.shift ^= (self.rand.next() * 255.0) as u32;
            let index_byte = *byte;
            let index = usize::from(index_byte);
            *byte = self.encode[index] ^ self.shift as u8;
            self.shift ^= u32::from(index_byte);
        }
    }

    fn decode(&mut self, data: &mut [u8]) {
        for byte in data {
            self.shift ^= (self.rand.next() * 255.0) as u32;
            let index = usize::from(*byte ^ self.shift as u8);
            *byte = self.decode[index];
            self.shift ^= u32::from(*byte);
        }
    }
}

/// `MD5(str)` / `SHA(str)` / `SHA1(str)`: the digest of the argument's
/// string bytes as lowercase hex (32 chars for MD5, 40 for SHA-1); `NULL`
/// argument propagates to `NULL`. Port of `builtinMD5Sig.evalString` and
/// `builtinSHA1Sig.evalString` (`pkg/expression/builtin_encryption.go`),
/// which differ only in the hash used — both eval the argument as a string
/// (numbers arrive as their decimal text) and hex-encode the sum.
fn hash_unary<D: Digest>(v: &Datum) -> Result<Datum, EvalError> {
    match hash_input(v)? {
        Some(bytes) => Ok(Datum::new_string(hex_lower(
            D::digest(bytes.as_slice()).as_slice(),
        ))),
        None => Ok(Datum::Null),
    }
}

/// Returns the exact bytes consumed by Go's `EvalString` at the hash
/// boundary. String and binary datums are already byte payloads (a TiDB Go
/// string is not required to be UTF-8), while scalar numerics use their
/// decimal text representation. Keeping this boundary byte-oriented lets
/// binary/GBK values reach the digest unchanged instead of rejecting them as
/// invalid UTF-8 in the shared human-text coercion helper.
fn hash_input(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    Ok(match value {
        Datum::String(value) => Some(value.bytes().to_vec()),
        Datum::Bytes(value) => Some(value.clone()),
        Datum::Int(value) => Some(value.to_string().into_bytes()),
        Datum::UInt(value) => Some(value.to_string().into_bytes()),
        Datum::Decimal(value) => Some(value.to_string().into_bytes()),
        Datum::Real(value) => Some(value.to_string().into_bytes()),
        Datum::Null => None,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel hash argument"));
        }
        other => Some(
            other
                .to_bytes()
                .map_err(|_| EvalError::Unsupported("datum hash argument"))?,
        ),
    })
}

/// `SHA2(str, n)`: the SHA-2 family digest as lowercase hex. Port of
/// `builtinSHA2Sig.evalString` (`pkg/expression/builtin_encryption.go`):
/// `NULL` in either argument propagates; the switch over `n` maps
/// `SHA0`(0) and `SHA256`(256) to SHA-256, `SHA224`/`SHA384`/`SHA512` to
/// their hashes, and leaves the hasher nil for EVERY other value — which
/// the Go code returns as `NULL` (not an error). Its function class declares
/// the second argument `ETInt`, so [`sha2_length`] ports that implicit cast
/// before applying the switch.
fn sha2_hash(arg: &Datum, len: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = hash_input(arg)? else {
        return Ok(Datum::Null);
    };
    let Some(n) = sha2_length(len)? else {
        return Ok(Datum::Null);
    };
    let hex = match n {
        0 | 256 => hex_lower(Sha256::digest(bytes.as_slice()).as_slice()),
        224 => hex_lower(Sha224::digest(bytes.as_slice()).as_slice()),
        384 => hex_lower(Sha384::digest(bytes.as_slice()).as_slice()),
        512 => hex_lower(Sha512::digest(bytes.as_slice()).as_slice()),
        _ => return Ok(Datum::Null),
    };
    Ok(Datum::new_string(hex))
}

/// Ports the `ETInt` coercion requested by `sha2FunctionClass.getFunction`.
/// Decimal inputs round half away from zero (`builtinCastDecimalAsIntSig`),
/// while floats round ties to even (`builtinCastRealAsIntSig` through
/// `types.ConvertFloatToInt`). String inputs follow `StrToInt`'s leading
/// numeric-prefix rule; its warnings are intentionally absent because this
/// evaluator has no statement-context warning channel. An out-of-range input
/// becomes an invalid hash length, hence `SHA2` returns `NULL` just as Go
/// does after its saturating integer conversion.
fn sha2_length(value: &Datum) -> Result<Option<i64>, EvalError> {
    Ok(match value {
        Datum::Int(n) => Some(*n),
        Datum::UInt(n) => Some(*n as i64),
        Datum::Decimal(d) => Some(d.round_to_i64_saturating()),
        Datum::Real(f) => Some(round_float_to_i64_saturating(*f)),
        Datum::String(s) => {
            Some(parse_string_i64_saturating(s.as_utf8().map_err(|_| {
                EvalError::Unsupported("invalid UTF-8 SHA2 length")
            })?))
        }
        Datum::Bytes(s) => Some(parse_string_i64_saturating(
            std::str::from_utf8(s)
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 SHA2 length"))?,
        )),
        Datum::Null => None,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel SHA2 length"));
        }
        other => Some(
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("SHA2 length conversion"))?
                .value,
        ),
    })
}

/// `types.ConvertFloatToInt` uses `math.RoundToEven`, then clamps overflow.
fn round_float_to_i64_saturating(value: f64) -> i64 {
    let rounded = value.round_ties_even();
    if rounded < i64::MIN as f64 {
        i64::MIN
    } else if rounded >= i64::MAX as f64 {
        i64::MAX
    } else {
        rounded as i64
    }
}

/// The result side of Go's `types.StrToInt`: after optional whitespace and a
/// sign, consume decimal digits until the first non-digit. A missing numeric
/// prefix becomes zero with a warning; warnings have no observable result in
/// this evaluator. Overflow clamps, which is sufficient here because neither
/// endpoint is a recognized SHA-2 hash length.
fn parse_string_i64_saturating(value: &str) -> i64 {
    let value = value.trim_start();
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => match value.strip_prefix('+') {
            Some(rest) => (false, rest),
            None => (false, value),
        },
    };
    let digits = &digits[..digits.bytes().take_while(u8::is_ascii_digit).count()];
    if digits.is_empty() {
        return 0;
    }
    let Ok(magnitude) = digits.parse::<u64>() else {
        return if negative { i64::MIN } else { i64::MAX };
    };
    if negative {
        let min_magnitude = i64::MAX as u64 + 1;
        if magnitude >= min_magnitude {
            i64::MIN
        } else {
            -(magnitude as i64)
        }
    } else if magnitude > i64::MAX as u64 {
        i64::MAX
    } else {
        magnitude as i64
    }
}

#[cfg(test)]
mod tests {
    use super::{dispatch, hex_upper};
    use crate::Datum;
    use crate::Decimal;

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals)
            .expect("name/arity should dispatch to the crypto family")
            .expect("evaluation should succeed")
    }

    fn s(text: &str) -> Datum {
        Datum::new_string(text.to_string())
    }

    fn dec(text: &str) -> Datum {
        Datum::Decimal(Decimal::from_literal(text))
    }

    fn bytes_of(d: &Datum) -> Vec<u8> {
        match d {
            Datum::String(value) => value.bytes().to_vec(),
            Datum::Bytes(value) => value.clone(),
            other => panic!("expected string/bytes datum, got {other:?}"),
        }
    }

    /// Vectors from `TestAESEncrypt`/`TestAESDecrypt` (default `aes-128-ecb`;
    /// the Go test's tuple is `{mode, str, key_args, expected_hex}`, so the
    /// second field is the plaintext and the key is `args[0]`). GBK/other-mode
    /// cases need session state and are out of this domain.
    #[test]
    fn aes_ecb_go_vectors() {
        let cases: &[(Datum, Datum, &str)] = &[
            (
                s("pingcap"),
                s("1234567890123456"),
                "697BFE9B3F8C2F289DD82C88C7BC95C4",
            ),
            (s("pingcap"), s("123"), "996E0CA8688D7AD20819B90B273E01C6"),
            (
                s("pingcap"),
                Datum::Int(123),
                "996E0CA8688D7AD20819B90B273E01C6",
            ),
            (
                s("pingcap"),
                s("123456789012345678901234"),
                "6F1589686860C8E8C7A40A78B25FF2C0",
            ),
        ];
        for (str_arg, key_arg, want) in cases {
            let crypt = call("AES_ENCRYPT", &[str_arg.clone(), key_arg.clone()]);
            assert_eq!(
                hex_upper(&bytes_of(&crypt)),
                *want,
                "AES_ENCRYPT({str_arg:?}, {key_arg:?})"
            );
            // Round-trip decrypt recovers the plaintext.
            let plain = call("AES_DECRYPT", &[crypt.clone(), key_arg.clone()]);
            assert_eq!(
                bytes_of(&plain),
                bytes_of(str_arg),
                "AES_DECRYPT round-trip"
            );
        }

        // NULL in either argument yields NULL.
        assert_eq!(call("AES_ENCRYPT", &[Datum::Null, s("k")]), Datum::Null);
        assert_eq!(call("AES_ENCRYPT", &[s("x"), Datum::Null]), Datum::Null);
        assert_eq!(call("AES_DECRYPT", &[Datum::Null, s("k")]), Datum::Null);
        // A ciphertext whose length is not a whole number of blocks -> NULL.
        assert_eq!(
            call("AES_DECRYPT", &[s("not-16-bytes"), s("key")]),
            Datum::Null
        );
        // Valid-length but undecryptable-to-good-padding ciphertext -> NULL.
        assert_eq!(
            call("AES_DECRYPT", &[s("0123456789abcdef"), s("wrong-key")]),
            Datum::Null
        );
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    /// UNCOMPRESS / UNCOMPRESSED_LENGTH decoding Go `compress/zlib` output. The
    /// compressed literals come from goeval (`compress('hello')`), since Go's
    /// DEFLATE-encoder bytes are not reproducible here and `COMPRESS` is
    /// deferred; the decoders faithfully consume that output.
    #[test]
    fn uncompress_go_vectors() {
        // compress('hello') = LE(5) + zlib stream.
        let hello = hex_bytes("05000000789CCA48CDC9C907040000FFFF062C0215");
        assert_eq!(
            call("UNCOMPRESS", &[Datum::new_string(hello.clone())]),
            s("hello")
        );
        assert_eq!(
            call("UNCOMPRESSED_LENGTH", &[Datum::new_string(hello)]),
            Datum::Int(5)
        );

        // Empty input -> empty string / 0.
        assert_eq!(call("UNCOMPRESS", &[s("")]), s(""));
        assert_eq!(call("UNCOMPRESSED_LENGTH", &[s("")]), Datum::Int(0));

        // Too-short (<= 4 bytes) payload is corrupted: NULL / 0.
        assert_eq!(
            call("UNCOMPRESS", &[Datum::new_string(vec![0u8])]),
            Datum::Null
        );
        assert_eq!(
            call(
                "UNCOMPRESSED_LENGTH",
                &[Datum::new_string(vec![0xAA, 0xBB])]
            ),
            Datum::Int(0)
        );

        // A valid-length prefix but an undecodable zlib stream -> NULL.
        assert_eq!(
            call(
                "UNCOMPRESS",
                &[Datum::new_string(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])]
            ),
            Datum::Null
        );

        // NULL propagation.
        assert_eq!(call("UNCOMPRESS", &[Datum::Null]), Datum::Null);
        assert_eq!(call("UNCOMPRESSED_LENGTH", &[Datum::Null]), Datum::Null);
    }

    /// Vectors from `TestMD5Hash` (default charset cases only — GBK cases
    /// need session charset conversion, out of this domain).
    #[test]
    fn md5_go_vectors() {
        let cases: &[(Datum, &str)] = &[
            (s(""), "d41d8cd98f00b204e9800998ecf8427e"),
            (s("a"), "0cc175b9c0f1b6a831c399e269772661"),
            (s("ab"), "187ef4436122d1cc2f40dc2b92f0eba0"),
            (s("abc"), "900150983cd24fb0d6963f7d28e17f72"),
            (Datum::Int(123), "202cb962ac59075b964b07152d234b70"),
            (s("123"), "202cb962ac59075b964b07152d234b70"),
            (dec("123.123"), "46ddc40585caa8abc07c460b3485781e"),
            (s("一二三"), "8093a32450075324682d01456d6e3919"),
            (s("ㅂ123"), "0e85d0f68c104b65a15d727e26705596"),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call("MD5", std::slice::from_ref(arg)),
                s(want),
                "MD5({arg:?})"
            );
        }
        assert_eq!(call("MD5", &[Datum::Null]), Datum::Null);
    }

    /// Vectors from `TestSha1Hash` (default charset cases; the empty-string
    /// vector is charset-independent). `SHA` must alias `SHA1`.
    #[test]
    fn sha1_go_vectors() {
        let cases: &[(Datum, &str)] = &[
            (s("test"), "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
            (s("c4pt0r"), "034923dcabf099fc4c8917c0ab91ffcd4c2578a6"),
            (s("pingcap"), "73bf9ef43a44f42e2ea2894d62f0917af149a006"),
            (s("foobar"), "8843d7f92416211de9ebb963ff4ce28125932878"),
            (Datum::Int(1024), "128351137a9c47206c4507dcf2e6fbeeca3a9079"),
            (dec("123.45"), "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"),
            (s(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call("SHA1", std::slice::from_ref(arg)),
                s(want),
                "SHA1({arg:?})"
            );
            assert_eq!(
                call("SHA", std::slice::from_ref(arg)),
                s(want),
                "SHA({arg:?})"
            );
        }
        assert_eq!(call("SHA1", &[Datum::Null]), Datum::Null);
        assert_eq!(call("SHA", &[Datum::Null]), Datum::Null);
    }

    /// Vectors from `TestSha2Hash` (default charset cases; empty-string
    /// vectors are charset-independent). Invalid lengths — anything but
    /// 0/224/256/384/512 — are `NULL`, as are `NULL` arguments.
    #[test]
    fn sha2_go_vectors() {
        let cases: &[(Datum, i64, &str)] = &[
            (
                s("pingcap"),
                0,
                "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a",
            ),
            (
                s("pingcap"),
                224,
                "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7",
            ),
            (
                s("pingcap"),
                256,
                "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a",
            ),
            (
                s("pingcap"),
                384,
                "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51",
            ),
            (
                s("pingcap"),
                512,
                "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80",
            ),
            (
                Datum::Int(13572468),
                0,
                "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe",
            ),
            (
                Datum::Int(13572468),
                224,
                "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4",
            ),
            (
                Datum::Int(13572468),
                256,
                "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe",
            ),
            (
                dec("13572468.123"),
                384,
                "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89",
            ),
            (
                dec("13572468.123"),
                512,
                "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96",
            ),
            (
                s(""),
                0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                s(""),
                224,
                "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
            ),
            (
                s(""),
                256,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                s(""),
                384,
                "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
            ),
            (
                s(""),
                512,
                "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
            ),
        ];
        for (arg, n, want) in cases {
            assert_eq!(
                call("SHA2", &[arg.clone(), Datum::Int(*n)]),
                s(want),
                "SHA2({arg:?}, {n})"
            );
        }
        // `builtinSHA2Sig.evalString`: an unrecognized hash length leaves
        // the hasher nil and the result is NULL, not an error.
        assert_eq!(call("SHA2", &[s("pingcap"), Datum::Int(123)]), Datum::Null);
        assert_eq!(call("SHA2", &[Datum::Null, Datum::Int(224)]), Datum::Null);
        assert_eq!(call("SHA2", &[s("pingcap"), Datum::Null]), Datum::Null);
    }

    /// `sha2FunctionClass` requests `ETInt` for its second argument. These
    /// source-derived coercion edges keep that function-class contract from
    /// being lost at the extension dispatch seam.
    #[test]
    fn sha2_hash_length_coercion() {
        let sha256 = s("2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881");
        let sha224 = s("54a2f7f92a5f975d8096af77a126edda7da60c5aa872ef1b871701ae");
        assert_eq!(call("SHA2", &[s("x"), s("abc")]), sha256);
        assert_eq!(call("SHA2", &[s("x"), s("224suffix")]), sha224);
        assert_eq!(call("SHA2", &[s("x"), dec("255.5")]), sha256);
        assert_eq!(call("SHA2", &[s("x"), Datum::Real(256.5)]), sha256);
        assert_eq!(call("SHA2", &[s("x"), dec("256.5")]), Datum::Null);
        assert_eq!(call("SHA2", &[s("x"), Datum::Real(255.49)]), Datum::Null);
    }

    /// Go's connection charset conversion turns these source strings into
    /// GBK bytes before the hash evaluator sees them. A raw-byte datum is the
    /// value-only equivalent at this Rust boundary; the digest must consume
    /// those bytes rather than rejecting them as non-UTF-8 text.
    #[test]
    fn hash_go_vectors_preserve_gbk_and_binary_bytes() {
        let gbk = Datum::new_bytes([0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd]);
        let gbk_with_digits =
            Datum::new_bytes([0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd, b'1', b'2', b'3']);
        assert_eq!(
            call("SHA", std::slice::from_ref(&gbk)),
            s("30cda4eed59a2ff592f2881f39d42fed6e10cad8")
        );
        assert_eq!(
            call("SHA", std::slice::from_ref(&gbk_with_digits)),
            s("1e24acbf708cd889c1d5be90abc1f14eaf14d0b4")
        );
        assert_eq!(
            call("MD5", std::slice::from_ref(&gbk)),
            s("a45d4af7b243e7f393fa09bed72ac73e")
        );
        assert_eq!(
            call("SHA2", &[gbk.clone(), Datum::Int(0)]),
            s("b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af")
        );
        assert_eq!(
            call("SHA2", &[gbk.clone(), Datum::Int(224)]),
            s("2362f577783f6cd6cc10b0308f946f479fef868a39d6339b5d74cc6d")
        );
        assert_eq!(
            call("SHA2", &[gbk.clone(), Datum::Int(256)]),
            s("b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af")
        );
        assert_eq!(
            call("SHA2", &[gbk.clone(), Datum::Int(384)]),
            s("54e75070f1faab03e7ce808ca2824ed4614ad1d58ee1409d8c1e4fd72ecab12c92ac3a2f919721c2aa09b23e5f3cc8aa")
        );
        assert_eq!(
            call("SHA2", &[gbk.clone(), Datum::Int(512)]),
            s("54fae3d0bb68bb4645af4a97a01fee1a6e3ecf7850f1ba41a994a46d23b60082262d00d9c635ff7ed02203e4806794dfa57c3654b3a4549bfb77ef1ddeab0224")
        );
        assert_eq!(
            call("SHA2", &[gbk_with_digits.clone(), Datum::Int(0)]),
            s("de059637dd572c2e21df1dd6d04512ad3a34f71964f14338e966356a091c0e7e")
        );
        assert_eq!(
            call("SHA2", &[gbk_with_digits.clone(), Datum::Int(224)]),
            s("a192909220fea1b74bcea87740f7550a2c03cf4f92d4c78ccedc9e3f")
        );
        assert_eq!(
            call("SHA2", &[gbk_with_digits.clone(), Datum::Int(256)]),
            s("de059637dd572c2e21df1dd6d04512ad3a34f71964f14338e966356a091c0e7e")
        );
        assert_eq!(
            call("SHA2", &[gbk_with_digits.clone(), Datum::Int(384)]),
            s("a487131f07fd46f66d7300be3c10bdae255e3296334a239240b28d32f038983331b276bd717363673e54733b594e7781")
        );
        assert_eq!(
            call("SHA2", &[gbk_with_digits, Datum::Int(512)]),
            s("9336a0844a5a1dc656d02ded28bf768cef9c39b47bd7292c75fc0d27fcb509ca765d24d502e5906e8afe1803fd5ea325e3d855a0206df6bc08fef5e7e34b0082")
        );
        assert_eq!(
            call("MD5", &[Datum::new_bytes([0xff, 0x00, b'a'])]),
            s("310e56cdb9dccaf757dbcab30054500e")
        );
    }

    /// Vectors from `TestPassword`. `PASSWORD` hashes the exact bytes at the
    /// Go `EvalString` boundary; the GBK row is represented directly as its
    /// valid encoded bytes because connection-charset conversion is outside
    /// this value-only evaluator. The deprecated-function warning is likewise
    /// a statement-context concern and has no result-side representation here.
    #[test]
    fn password_go_vectors() {
        let cases: &[(Datum, &str)] = &[
            (s(""), ""),
            (s("abc"), "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"),
            (Datum::Int(123), "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257"),
            (
                Datum::Real(1.23),
                "*A589EEBA8D3F9E1A34A7EE518FAC4566BFAD5BB6",
            ),
            (s("一二三四"), "*D207780722F22B23C254CAC0580D3B6738C19E18"),
            (
                Datum::new_bytes([0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd, 0xcb, 0xc4]),
                "*48E0460AD45CF66AC6B8C18CB8B4BC8A403D935B",
            ),
            (dec("123.123"), "*B15B84262DB34BFB2C817A45A55C405DC7C52BB1"),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call("PASSWORD", std::slice::from_ref(arg)),
                s(want),
                "PASSWORD({arg:?})"
            );
        }
        assert_eq!(call("PASSWORD", &[Datum::Null]), Datum::Null);
        assert_eq!(
            call("PASSWORD", &[Datum::new_bytes([0xff, 0x00, b'a'])]),
            s("*F5A241511384DB827F22D2A2188A456E87F7D4F2")
        );
    }

    /// Representable default-charset rows from `TestSQLDecode` and
    /// `TestSQLEncode` in `pkg/expression/builtin_encryption_test.go:62,81`.
    /// The Go tests also run the same table under `gbk`; connection-charset
    /// conversion and typed collation metadata are session boundaries, while
    /// the UTF-8/default-charset rows below exercise the complete
    /// deterministic stream-cipher value contract.  `Datum::String` keeps
    /// the arbitrary decoded bytes so this test can compare the exact
    /// uppercase hex that the source test obtains via `toHex`.
    #[test]
    fn sql_encode_decode_go_vectors() {
        let cases = [
            ("", "", ""),
            ("pingcap", "1234567890123456", "2C35B5A4ADF391"),
            ("pingcap", "asdfjasfwefjfjkj", "351CC412605905"),
            (
                "pingcap123",
                "123456789012345678901234",
                "7698723DC6DFE7724221",
            ),
            ("pingcap#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449"),
            ("pingcap", "", "4A77B524BD2C5C"),
            (
                "分布式データベース",
                "pass1234@#$%%^^&",
                "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE",
            ),
            (
                "分布式データベース",
                "分布式7782734adgwy1242",
                "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC",
            ),
            ("pingcap", "密匙", "CE5C02A5010010"),
            (
                "pingcap数据库",
                "数据库passwd12345667",
                "36D5F90D3834E30E396BE3226E3B4ED3",
            ),
            ("数据库5667", "123.435", "B22196D0569386237AE12F8AAB"),
        ];
        for (origin, password, encoded_hex) in cases {
            let decoded = call("DECODE", &[s(origin), s(password)]);
            let decoded = match decoded {
                Datum::String(value) => value.bytes().to_vec(),
                other => panic!("DECODE must return a string, got {other:?}"),
            };
            assert_eq!(hex_upper(&decoded), encoded_hex, "DECODE({origin:?})");

            let crypt = decode_hex(encoded_hex);
            assert_eq!(
                call("ENCODE", &[Datum::new_bytes(crypt), s(password)]),
                s(origin),
                "ENCODE({origin:?})"
            );
        }
        for (name, args) in [
            ("DECODE", vec![Datum::Null, s("password")]),
            ("DECODE", vec![s("data"), Datum::Null]),
            ("ENCODE", vec![Datum::Null, s("password")]),
            ("ENCODE", vec![Datum::new_bytes(b"data"), Datum::Null]),
        ] {
            assert_eq!(call(name, &args), Datum::Null);
        }
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        assert!(value.len().is_multiple_of(2));
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = pair[0].to_ascii_uppercase();
                let low = pair[1].to_ascii_uppercase();
                (hex_digit(high) << 4) | hex_digit(low)
            })
            .collect()
    }

    fn hex_digit(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'A'..=b'F' => value - b'A' + 10,
            _ => panic!("invalid hex digit {value}"),
        }
    }

    /// Foreign names and wrong arities fall through (`None`), never a
    /// claimed-but-wrong answer.
    #[test]
    fn dispatch_declines_foreign_names_and_arities() {
        assert!(dispatch("MD5X", &[s("a")]).is_none());
        assert!(dispatch("MD5", &[s("a"), s("b")]).is_none());
        assert!(dispatch("SHA1", &[]).is_none());
        assert!(dispatch("SHA2", &[s("a")]).is_none());
        assert!(dispatch("PASSWORD", &[]).is_none());
        assert!(dispatch("PASSWORD", &[s("a"), s("b")]).is_none());
        assert!(dispatch("ENCODE", &[s("a")]).is_none());
        assert!(dispatch("DECODE", &[s("a")]).is_none());
        // The 2-arg default `aes-128-ecb` form is handled; the 3-arg IV form and
        // other arities remain a session boundary and decline.
        assert!(dispatch("AES_ENCRYPT", &[s("a")]).is_none());
        assert!(dispatch("AES_ENCRYPT", &[s("a"), s("k"), s("iv")]).is_none());
        assert!(dispatch("AES_DECRYPT", &[s("a"), s("k"), s("iv")]).is_none());
        // COMPRESS is deferred (Go DEFLATE-encoder bytes are not reproducible).
        assert!(dispatch("COMPRESS", &[s("a")]).is_none());
        assert!(dispatch("UNCOMPRESS", &[]).is_none());
        assert!(dispatch("UNCOMPRESSED_LENGTH", &[s("a"), s("b")]).is_none());
    }
}
