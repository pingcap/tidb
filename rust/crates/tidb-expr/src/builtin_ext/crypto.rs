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
//! Session-dependent functions receive their statement snapshot through
//! [`crate::Columns`]. `AES_ENCRYPT`/`AES_DECRYPT` therefore use the live
//! `block_encryption_mode`, including all accepted key sizes and ECB/CBC/OFB/
//! CFB modes, IV validation, NULL propagation, and the ignored-IV warning.
//!
//! `COMPRESS`, `UNCOMPRESS`, and `UNCOMPRESSED_LENGTH` implement TiDB's binary
//! framing: a 4-byte little-endian original-length prefix followed by a zlib
//! stream. The compressed block layout is deliberately left to Rust's zlib
//! encoder; SQL observes a standards-compliant stream and the framing, not a
//! particular standard-library DEFLATE strategy. The inverse functions require
//! a complete checksummed stream and append TiDB's corruption warnings to the
//! statement context.
//!
use std::io::Write;

use flate2::write::ZlibEncoder;
use flate2::{Compression, Decompress, FlushDecompress, Status};
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

use crate::{BlockEncryptionMode, Columns, Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
/// `SHA` is a true alias of `SHA1`: `builtin.go`'s `funcs` map registers
/// both `ast.SHA` and `ast.SHA1` to the same `sha1FunctionClass`.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("MD5", 1) => Some(hash_unary::<Md5>(&vals[0])),
        ("SHA" | "SHA1", 1) => Some(hash_unary::<Sha1>(&vals[0])),
        ("SHA2", 2) => Some(sha2_hash(&vals[0], &vals[1])),
        ("SM3", 1) => Some(sm3_hash(&vals[0])),
        ("RANDOM_BYTES", 1) => Some(random_bytes(&vals[0])),
        ("RANDOM_BYTES", _) => Some(Err(EvalError::WrongParameterCount("random_bytes"))),
        ("PASSWORD", 1) => Some(password_hash(&vals[0])),
        ("VALIDATE_PASSWORD_STRENGTH", 1) => Some(validate_password_strength(&vals[0], ctx)),
        ("ENCODE", 2) => Some(sql_encode(&vals[0], &vals[1])),
        ("DECODE", 2) => Some(sql_decode(&vals[0], &vals[1])),
        ("COMPRESS", 1) => Some(compress(&vals[0])),
        ("AES_ENCRYPT" | "AES_DECRYPT", _) => {
            eval_aes_lazy(name, vals.len(), |i| Ok(vals[i].clone()), ctx)
        }
        ("UNCOMPRESS", 1) => Some(uncompress(&vals[0], ctx)),
        ("UNCOMPRESSED_LENGTH", 1) => Some(uncompressed_length(&vals[0], ctx)),
        _ => None,
    }
}

/// `COMPRESS(payload)`. Empty input remains empty. Non-empty input is framed
/// as the original byte length followed by a zlib stream. A trailing dot keeps
/// a stream ending in ASCII space from being changed by SQL trailing-space
/// handling, matching TiDB's public format.
fn compress(arg: &Datum) -> Result<Datum, EvalError> {
    let Some(payload) = sql_string_bytes(arg)? else {
        return Ok(Datum::Null);
    };
    if payload.is_empty() {
        return Ok(Datum::new_string(Vec::new()));
    }

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    if encoder.write_all(&payload).is_err() {
        return Ok(Datum::Null);
    }
    let Ok(compressed) = encoder.finish() else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(frame_compressed(
        payload.len() as u32,
        compressed,
    )))
}

fn frame_compressed(original_len: u32, compressed: Vec<u8>) -> Vec<u8> {
    let append_suffix = compressed.last() == Some(&b' ');
    let mut framed = Vec::with_capacity(4 + compressed.len() + usize::from(append_suffix));
    framed.extend_from_slice(&original_len.to_le_bytes());
    framed.extend_from_slice(&compressed);
    if append_suffix {
        framed.push(b'.');
    }
    framed
}

/// Inflates a zlib stream, returning `None` on any decode error. Port of the
/// decode half of `pkg/expression/builtin_encryption.go`'s `inflate`
/// (`compress/zlib`), which reads the whole stream and verifies its Adler-32
/// checksum; trailing bytes past the stream end are ignored.
fn inflate(data: &[u8]) -> Option<Vec<u8>> {
    let mut decoder = Decompress::new(true);
    let mut out = Vec::new();
    let mut input_offset = 0;
    let mut chunk = [0; 8 * 1024];
    loop {
        let input_before = decoder.total_in();
        let output_before = decoder.total_out();
        let status = decoder
            .decompress(&data[input_offset..], &mut chunk, FlushDecompress::None)
            .ok()?;
        input_offset = usize::try_from(decoder.total_in()).ok()?;
        let produced = usize::try_from(decoder.total_out() - output_before).ok()?;
        out.extend_from_slice(&chunk[..produced]);
        if status == Status::StreamEnd {
            return Some(out);
        }
        // `compress/zlib.NewReader` refuses a stream that ends before the
        // DEFLATE terminator and Adler-32 checksum.  The high-level flate2
        // reader can instead report a successful zero-byte read for that
        // truncated input, so require either stream completion or forward
        // progress toward it here.
        if decoder.total_in() == input_before && produced == 0 {
            return None;
        }
    }
}

/// `UNCOMPRESS(payload)`. Port of `builtinUncompressSig.evalString`. The 4-byte
/// little-endian prefix records the original length and the remainder is a zlib
/// stream. Empty input yields an empty string; NULL, a too-short/corrupted
/// payload, an undecodable stream, or a stored length below the decompressed
/// length all yield NULL and append the same statement warning as Go.
fn uncompress(arg: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let Some(payload) = sql_string_bytes(arg)? else {
        return Ok(Datum::Null);
    };
    if payload.is_empty() {
        return Ok(Datum::new_string(Vec::new()));
    }
    if payload.len() <= 4 {
        ctx.append_warning(1259, "ZLIB: Input data corrupted");
        return Ok(Datum::Null);
    }
    let length = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let Some(bytes) = inflate(&payload[4..]) else {
        ctx.append_warning(1259, "ZLIB: Input data corrupted");
        return Ok(Datum::Null);
    };
    if length < bytes.len() as u32 {
        ctx.append_warning(
            1258,
            "ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)",
        );
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(bytes))
}

/// `UNCOMPRESSED_LENGTH(payload)`. Port of
/// `builtinUncompressedLengthSig.evalInt`: returns the 4-byte little-endian
/// prefix as the original length. NULL yields NULL; empty or too-short/corrupted
/// input yields 0.
fn uncompressed_length(arg: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let Some(payload) = sql_string_bytes(arg)? else {
        return Ok(Datum::Null);
    };
    if payload.is_empty() {
        return Ok(Datum::Int(0));
    }
    if payload.len() <= 4 {
        ctx.append_warning(1259, "ZLIB: Input data corrupted");
        return Ok(Datum::Int(0));
    }
    let length = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    Ok(Datum::Int(i64::from(length)))
}

const AES_BLOCK_SIZE: usize = 16;

/// Evaluates AES arguments in Go's signature order. ECB deliberately ignores
/// a third argument without evaluating it and emits warning 1618; IV modes
/// require and evaluate exactly three arguments. Keeping the evaluator as a
/// closure lets both AST and chunk paths share this ordering without copying
/// their row-access machinery.
pub(crate) fn eval_aes_lazy<F>(
    name: &str,
    arg_count: usize,
    mut eval: F,
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>>
where
    F: FnMut(usize) -> Result<Datum, EvalError>,
{
    let function = if name.eq_ignore_ascii_case("aes_encrypt") {
        "aes_encrypt"
    } else if name.eq_ignore_ascii_case("aes_decrypt") {
        "aes_decrypt"
    } else {
        return None;
    };
    let mode = ctx.block_encryption_mode();
    if mode.iv_required() {
        if arg_count != 3 {
            return Some(Err(EvalError::WrongParameterCount(function)));
        }
    } else if !(2..=3).contains(&arg_count) {
        return Some(Err(EvalError::WrongParameterCount(function)));
    }

    let input = match eval(0).and_then(|value| sql_string_bytes(&value)) {
        Ok(Some(value)) => value,
        Ok(None) => return Some(Ok(Datum::Null)),
        Err(error) => return Some(Err(error)),
    };
    let password = match eval(1).and_then(|value| sql_string_bytes(&value)) {
        Ok(Some(value)) => value,
        Ok(None) => return Some(Ok(Datum::Null)),
        Err(error) => return Some(Err(error)),
    };
    let iv = if mode.iv_required() {
        match eval(2).and_then(|value| sql_string_bytes(&value)) {
            Ok(Some(value)) => Some(value),
            Ok(None) => return Some(Ok(Datum::Null)),
            Err(error) => return Some(Err(error)),
        }
    } else if arg_count == 3 {
        ctx.append_warning(1618, "<IV> option ignored");
        None
    } else {
        None
    };
    Some(eval_aes_bytes(
        function,
        &input,
        &password,
        iv.as_deref(),
        mode,
    ))
}

fn eval_aes_bytes(
    function: &'static str,
    input: &[u8],
    password: &[u8],
    iv: Option<&[u8]>,
    mode: BlockEncryptionMode,
) -> Result<Datum, EvalError> {
    let iv = if mode.iv_required() {
        let iv = iv.expect("an IV mode evaluates its third argument");
        if iv.len() < AES_BLOCK_SIZE {
            return Err(EvalError::IncorrectArguments(format!(
                "The initialization vector supplied to {function} is too short. Must be at least {AES_BLOCK_SIZE} bytes long"
            )));
        }
        Some(&iv[..AES_BLOCK_SIZE])
    } else {
        None
    };
    let key = tidb_util::encrypt::derive_key_mysql(password, mode.key_size());
    let encrypted = function == "aes_encrypt";
    let result = match mode {
        BlockEncryptionMode::Aes128Ecb
        | BlockEncryptionMode::Aes192Ecb
        | BlockEncryptionMode::Aes256Ecb => {
            if encrypted {
                tidb_util::encrypt::aes_encrypt_with_ecb(input, &key)
            } else {
                tidb_util::encrypt::aes_decrypt_with_ecb(input, &key)
            }
        }
        BlockEncryptionMode::Aes128Cbc
        | BlockEncryptionMode::Aes192Cbc
        | BlockEncryptionMode::Aes256Cbc => {
            let iv = iv.expect("CBC mode requires an IV");
            if encrypted {
                tidb_util::encrypt::aes_encrypt_with_cbc(input, &key, iv)
            } else {
                tidb_util::encrypt::aes_decrypt_with_cbc(input, &key, iv)
            }
        }
        BlockEncryptionMode::Aes128Ofb
        | BlockEncryptionMode::Aes192Ofb
        | BlockEncryptionMode::Aes256Ofb => {
            let iv = iv.expect("OFB mode requires an IV");
            if encrypted {
                tidb_util::encrypt::aes_encrypt_with_ofb(input, &key, iv)
            } else {
                tidb_util::encrypt::aes_decrypt_with_ofb(input, &key, iv)
            }
        }
        BlockEncryptionMode::Aes128Cfb
        | BlockEncryptionMode::Aes192Cfb
        | BlockEncryptionMode::Aes256Cfb => {
            let iv = iv.expect("CFB mode requires an IV");
            if encrypted {
                tidb_util::encrypt::aes_encrypt_with_cfb(input, &key, iv)
            } else {
                tidb_util::encrypt::aes_decrypt_with_cfb(input, &key, iv)
            }
        }
    };
    Ok(result.map_or(Datum::Null, Datum::new_string))
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
    Ok(Datum::new_string(tidb_parser::auth::encode_password_bytes(
        &bytes,
    )))
}

/// The deterministic MySQL 3.21 stream cipher used by TiDB's deprecated
/// `ENCODE(str, password)` function. The byte algorithm is owned once by
/// `tidb_util::encrypt`, the direct transcreation of `pkg/util/encrypt`.
fn sql_encode(data: &Datum, password: &Datum) -> Result<Datum, EvalError> {
    let Some(data) = sql_string_bytes(data)? else {
        return Ok(Datum::Null);
    };
    let Some(password) = sql_string_bytes(password)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(tidb_util::encrypt::sql_encode(
        &data, &password,
    )))
}

/// `DECODE(str, password)`, the inverse stream operation of [`sql_encode`].
fn sql_decode(data: &Datum, password: &Datum) -> Result<Datum, EvalError> {
    let Some(data) = sql_string_bytes(data)? else {
        return Ok(Datum::Null);
    };
    let Some(password) = sql_string_bytes(password)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(tidb_util::encrypt::sql_decode(
        &data, &password,
    )))
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

struct EvalGlobalVars<'a>(&'a dyn Columns);

impl tidb_util::password_validation::GlobalVarAccessor for EvalGlobalVars<'_> {
    type Error = EvalError;

    fn get_global_sys_var(&self, name: &str) -> Result<String, Self::Error> {
        let value = self
            .0
            .sysvar(Some(tidb_ast::SysVarScope::Global), name)
            .ok_or(EvalError::Unsupported(
                "validate_password globals require a session",
            ))?;
        match value {
            Datum::Bytes(value) => String::from_utf8(value).map_err(|_| {
                EvalError::IncorrectArguments(format!(
                    "invalid UTF-8 value for global system variable {name}"
                ))
            }),
            Datum::String(value) => value.as_utf8().map(str::to_owned).map_err(|_| {
                EvalError::IncorrectArguments(format!(
                    "invalid UTF-8 value for global system variable {name}"
                ))
            }),
            _ => Err(EvalError::IncorrectArguments(format!(
                "non-string value for global system variable {name}"
            ))),
        }
    }
}

fn password_eval_error(error: tidb_util::password_validation::PwdError<EvalError>) -> EvalError {
    use tidb_util::password_validation::PwdError;
    match error {
        PwdError::Accessor(error) => error,
        PwdError::ParseInt(error) => EvalError::IncorrectArguments(format!(
            "invalid validate_password numeric setting: {error}"
        )),
        PwdError::NotValid(reason) => EvalError::IncorrectArguments(reason),
    }
}

fn identity_username(identity: &str) -> &str {
    identity.rsplit_once('@').map_or(identity, |(user, _)| user)
}

/// `VALIDATE_PASSWORD_STRENGTH(str)`.
fn validate_password_strength(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_util::password_validation::{self, PasswordUser};

    let Some(password) = sql_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let password = tidb_datatype::GoString::from_bytes(password).to_utf8_lossy_go();
    if password.chars().count() < 4 {
        return Ok(Datum::Int(0));
    }

    let globals = EvalGlobalVars(ctx);
    if !password_validation::validation_enabled(&globals).map_err(password_eval_error)? {
        return Ok(Datum::Int(0));
    }
    let current_user = ctx.current_user();
    let login_user = ctx.login_user();
    let user = current_user.as_deref().map(|current| PasswordUser {
        auth_username: identity_username(current),
        username: login_user
            .as_deref()
            .map(identity_username)
            .unwrap_or_else(|| identity_username(current)),
    });
    let warning = password_validation::validate_user_name_in_password(&password, user, &globals)
        .map_err(password_eval_error)?;
    if !warning.is_empty() {
        return Ok(Datum::Int(0));
    }
    let warning = password_validation::validate_password_low_policy(&password, &globals)
        .map_err(password_eval_error)?;
    if !warning.is_empty() {
        return Ok(Datum::Int(25));
    }
    let warning = password_validation::validate_password_medium_policy(&password, &globals)
        .map_err(password_eval_error)?;
    if !warning.is_empty() {
        return Ok(Datum::Int(50));
    }
    if !password_validation::validate_dictionary_password(&password, &globals)
        .map_err(password_eval_error)?
    {
        return Ok(Datum::Int(75));
    }
    Ok(Datum::Int(100))
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

/// `SM3(str)`: the parser-auth SM3 digest rendered as lowercase hex. TiDB's
/// SQL builtin and password plugin deliberately share this digest owner.
fn sm3_hash(value: &Datum) -> Result<Datum, EvalError> {
    match hash_input(value)? {
        Some(bytes) => Ok(Datum::new_string(hex_lower(&tidb_parser::auth::sm3_hash(
            &bytes,
        )))),
        None => Ok(Datum::Null),
    }
}

/// `RANDOM_BYTES(len)`: one fresh OS-random binary string per evaluation.
/// The bytes themselves are unspecified; the stable SQL contract is the
/// requested length, NULL propagation, and inclusive 1..=1024 range gate.
fn random_bytes(length: &Datum) -> Result<Datum, EvalError> {
    let Some(length) = encryption_int_argument(length)? else {
        return Ok(Datum::Null);
    };
    if !(1..=1024).contains(&length) {
        return Err(EvalError::DataOutOfRange {
            value: "length",
            expression: "random_bytes",
        });
    }

    let mut bytes = vec![0; length as usize];
    getrandom::fill(&mut bytes)
        .map_err(|_| EvalError::Unsupported("fail to generate random bytes"))?;
    Ok(Datum::new_string(bytes))
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
/// the second argument `ETInt`, so [`encryption_int_argument`] ports that implicit cast
/// before applying the switch.
fn sha2_hash(arg: &Datum, len: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = hash_input(arg)? else {
        return Ok(Datum::Null);
    };
    let Some(n) = encryption_int_argument(len)? else {
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

/// Ports the `ETInt` coercion requested by the encryption function classes.
/// Decimal inputs round half away from zero (`builtinCastDecimalAsIntSig`),
/// while floats round ties to even (`builtinCastRealAsIntSig` through
/// `types.ConvertFloatToInt`). String inputs follow `StrToInt`'s leading
/// numeric-prefix rule; its warnings are intentionally absent because this
/// evaluator has no statement-context warning channel. An out-of-range input
/// becomes an invalid hash length, hence `SHA2` returns `NULL` just as Go
/// does after its saturating integer conversion.
fn encryption_int_argument(value: &Datum) -> Result<Option<i64>, EvalError> {
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
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::{dispatch, frame_compressed};
    use crate::{Columns, Datum, Decimal, EvalError};

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals, &crate::NoColumns)
            .expect("name/arity should dispatch to the crypto family")
            .expect("evaluation should succeed")
    }

    fn call_with(name: &str, vals: &[Datum], context: &impl Columns) -> Datum {
        dispatch(name, vals, context)
            .expect("name/arity should dispatch to the crypto family")
            .expect("evaluation should succeed")
    }

    #[derive(Default)]
    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[derive(Default)]
    struct PasswordContext {
        globals: HashMap<String, String>,
        current_user: Option<String>,
        login_user: Option<String>,
    }

    impl Columns for PasswordContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn current_user(&self) -> Option<String> {
            self.current_user.clone()
        }

        fn login_user(&self) -> Option<String> {
            self.login_user.clone()
        }

        fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
            matches!(scope, Some(tidb_ast::SysVarScope::Global))
                .then(|| self.globals.get(name).cloned())
                .flatten()
                .map(|value| Datum::Bytes(value.into_bytes()))
        }
    }

    fn password_context(enabled: bool) -> PasswordContext {
        let mut globals = HashMap::from([
            ("validate_password.enable".to_owned(), "OFF".to_owned()),
            ("validate_password.policy".to_owned(), "MEDIUM".to_owned()),
            (
                "validate_password.check_user_name".to_owned(),
                "ON".to_owned(),
            ),
            ("validate_password.length".to_owned(), "8".to_owned()),
            (
                "validate_password.mixed_case_count".to_owned(),
                "1".to_owned(),
            ),
            ("validate_password.number_count".to_owned(), "1".to_owned()),
            (
                "validate_password.special_char_count".to_owned(),
                "1".to_owned(),
            ),
            ("validate_password.dictionary".to_owned(), "1234".to_owned()),
        ]);
        if enabled {
            globals.insert("validate_password.enable".to_owned(), "ON".to_owned());
        }
        PasswordContext {
            globals,
            current_user: Some("testuser@%".to_owned()),
            login_user: Some("testuser@127.0.0.1".to_owned()),
        }
    }

    fn password_call(value: Datum, context: &PasswordContext) -> Datum {
        dispatch("VALIDATE_PASSWORD_STRENGTH", &[value], context)
            .expect("password strength should dispatch")
            .expect("password strength should evaluate")
    }

    #[test]
    fn validate_password_strength_go_vectors() {
        let disabled = password_context(false);
        assert_eq!(password_call(s("!Abc87654321"), &disabled), Datum::Int(0));

        let enabled = password_context(true);
        for (input, expected) in [
            (Datum::Null, Datum::Null),
            (s("123"), Datum::Int(0)),
            (Datum::Bytes(vec![b'a', 0xf0, 0x9f, 0x92]), Datum::Int(25)),
            (s("testuser123"), Datum::Int(0)),
            (s("resutset123"), Datum::Int(0)),
            (s("12345"), Datum::Int(25)),
            (s("12345678"), Datum::Int(50)),
            (s("!Abc12345678"), Datum::Int(75)),
            (s("!Abc87654321"), Datum::Int(100)),
        ] {
            assert_eq!(password_call(input, &enabled), expected);
        }
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

    fn hex_upper(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 16] = b"0123456789ABCDEF";
        let mut output = String::with_capacity(bytes.len() * 2);
        for &byte in bytes {
            output.push(ALPHABET[usize::from(byte >> 4)] as char);
            output.push(ALPHABET[usize::from(byte & 0x0f)] as char);
        }
        output
    }

    #[test]
    fn random_bytes_go_contract() {
        for length in [1, 32, 1024] {
            assert_eq!(
                bytes_of(&call("RANDOM_BYTES", &[Datum::Int(length)])).len(),
                length as usize
            );
        }
        assert_eq!(call("RANDOM_BYTES", &[Datum::Null]), Datum::Null);

        for length in [Datum::Int(0), Datum::Int(-32), Datum::Int(1025)] {
            assert_eq!(
                dispatch("RANDOM_BYTES", &[length], &crate::NoColumns).unwrap(),
                Err(EvalError::DataOutOfRange {
                    value: "length",
                    expression: "random_bytes",
                })
            );
        }
        for args in [&[][..], &[Datum::Int(1), Datum::Int(2)][..]] {
            assert_eq!(
                dispatch("RANDOM_BYTES", args, &crate::NoColumns).unwrap(),
                Err(EvalError::WrongParameterCount("random_bytes"))
            );
        }
    }

    /// Vectors from `TestAESEncrypt`/`TestAESDecrypt` (default `aes-128-ecb`;
    /// the Go test's tuple is `{mode, str, key_args, expected_hex}`, so the
    /// second field is the plaintext and the key is `args[0]`). The live SQL
    /// test covers the other statement-selected modes.
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

    /// COMPRESS framing and both inverse functions. The decoder also consumes
    /// a stream produced by Go's `compress/zlib`, proving cross-runtime format
    /// compatibility without coupling Rust to Go's DEFLATE block layout.
    #[test]
    fn compress_and_uncompress_vectors() {
        let binary = Datum::new_bytes([b'a', 0, 0xff, b' ']);
        let compressed = call("COMPRESS", std::slice::from_ref(&binary));
        let compressed_bytes = bytes_of(&compressed);
        assert_eq!(&compressed_bytes[..4], &4u32.to_le_bytes());
        assert_eq!(
            bytes_of(&call("UNCOMPRESS", std::slice::from_ref(&compressed))),
            bytes_of(&binary)
        );
        assert_eq!(call("UNCOMPRESSED_LENGTH", &[compressed]), Datum::Int(4));

        // The strict decoder streams output beyond its fixed scratch buffer;
        // validation must not turn a large, complete stream into corruption.
        let large = Datum::new_bytes(vec![b'x'; 20_000]);
        let compressed = call("COMPRESS", std::slice::from_ref(&large));
        assert_eq!(
            bytes_of(&call("UNCOMPRESS", std::slice::from_ref(&compressed))),
            bytes_of(&large)
        );

        // The suffix rule is part of the SQL framing, independent of which
        // inputs a particular zlib implementation happens to encode that way.
        assert_eq!(
            frame_compressed(1, vec![0x78, b' ']),
            vec![1, 0, 0, 0, 0x78, b' ', b'.']
        );

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
        assert_eq!(call("COMPRESS", &[s("")]), s(""));
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
        assert_eq!(call("COMPRESS", &[Datum::Null]), Datum::Null);
        assert_eq!(call("UNCOMPRESS", &[Datum::Null]), Datum::Null);
        assert_eq!(call("UNCOMPRESSED_LENGTH", &[Datum::Null]), Datum::Null);
    }

    /// Go's `zlib.NewReader` rejects a truncated stream even when it has not
    /// produced output yet. Keep that validation and the public warning codes
    /// on the shared inverse-function boundary.
    #[test]
    fn uncompress_requires_a_complete_zlib_stream_and_reports_corruption() {
        let warnings = WarningContext::default();
        assert_eq!(
            call_with(
                "UNCOMPRESS",
                &[Datum::new_string(b"12345".to_vec())],
                &warnings,
            ),
            Datum::Null
        );
        assert_eq!(
            warnings.0.borrow().as_slice(),
            &[(1259, "ZLIB: Input data corrupted".to_owned())]
        );

        warnings.0.borrow_mut().clear();
        let hello = hex_bytes("02000000789CCA48CDC9C907040000FFFF062C0215");
        assert_eq!(
            call_with("UNCOMPRESS", &[Datum::new_string(hello)], &warnings,),
            Datum::Null
        );
        assert_eq!(warnings.0.borrow()[0].0, 1258);

        warnings.0.borrow_mut().clear();
        assert_eq!(
            call_with(
                "UNCOMPRESSED_LENGTH",
                &[Datum::new_string(vec![0x01, 0x00])],
                &warnings,
            ),
            Datum::Int(0)
        );
        assert_eq!(warnings.0.borrow()[0].0, 1259);
    }

    /// Parser-auth's source-owned SM3 vectors, exercised through the SQL
    /// builtin boundary so the expression layer cannot silently diverge.
    #[test]
    fn sm3_go_vectors() {
        for (input, expected) in [
            (
                "abc",
                "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
            ),
            (
                "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                "debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732",
            ),
        ] {
            assert_eq!(call("SM3", &[s(input)]), s(expected));
        }
        assert_eq!(call("SM3", &[Datum::Null]), Datum::Null);
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
        let ctx = crate::NoColumns;
        assert!(dispatch("MD5X", &[s("a")], &ctx).is_none());
        assert!(dispatch("MD5", &[s("a"), s("b")], &ctx).is_none());
        assert!(dispatch("SHA1", &[], &ctx).is_none());
        assert!(dispatch("SHA2", &[s("a")], &ctx).is_none());
        assert!(dispatch("PASSWORD", &[], &ctx).is_none());
        assert!(dispatch("PASSWORD", &[s("a"), s("b")], &ctx).is_none());
        assert!(dispatch("ENCODE", &[s("a")], &ctx).is_none());
        assert!(dispatch("DECODE", &[s("a")], &ctx).is_none());
        assert!(matches!(
            dispatch("AES_ENCRYPT", &[s("a")], &ctx),
            Some(Err(crate::EvalError::WrongParameterCount("aes_encrypt")))
        ));
        assert!(dispatch("COMPRESS", &[], &ctx).is_none());
        assert!(dispatch("COMPRESS", &[s("a"), s("b")], &ctx).is_none());
        assert!(dispatch("UNCOMPRESS", &[], &ctx).is_none());
        assert!(dispatch("UNCOMPRESSED_LENGTH", &[s("a"), s("b")], &ctx).is_none());
    }
}
