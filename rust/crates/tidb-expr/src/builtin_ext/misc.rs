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

//! Miscellaneous scalar builtins. This is a separate family so unrelated
//! expression workers do not have to edit the central dispatcher.

use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::coerce::coerce_str;
use std::borrow::Cow;

use crate::{Datum, Decimal, EvalError};
use tidb_infoschema_context::MetaOnlyInfoSchema;
use tidb_vitess::hash_uint64;

/// Go `expression.DecodeKeyFromString`, which `planner/core`'s `init` sets
/// (`tidbCodecFuncHelper.decodeKeyFromString`): the decoded form of the
/// hexadecimal key, warning in the type context and returning the input when
/// it does not decode.
pub type DecodeKeyFromStringFn = fn(&dyn crate::Columns, &dyn MetaOnlyInfoSchema, &[u8]) -> Vec<u8>;

/// Go `expression.EncodeRecordKeyFromRow` / `EncodeIndexKeyFromRow`, which
/// `planner/core`'s `init` sets (`encodeHandleFromRow` /
/// `encodeIndexKeyFromRow`), over the arguments Rust has already evaluated.
pub type EncodeKeyFromRowFn = fn(
    &dyn crate::Columns,
    &dyn tidb_expropt::PrivilegeChecker,
    &dyn MetaOnlyInfoSchema,
    &tidb_exprctx::EncodeKeyArgs<'_>,
) -> Result<Vec<u8>, tidb_exprctx::EncodeKeyError>;

static DECODE_KEY_FROM_STRING: OnceLock<DecodeKeyFromStringFn> = OnceLock::new();
static ENCODE_RECORD_KEY_FROM_ROW: OnceLock<EncodeKeyFromRowFn> = OnceLock::new();
static ENCODE_INDEX_KEY_FROM_ROW: OnceLock<EncodeKeyFromRowFn> = OnceLock::new();

/// Sets Go `expression.DecodeKeyFromString` (`planner/core`'s `init`). The
/// first setting stays, as one Go binary's `init` runs once.
pub fn set_decode_key_from_string(decode: DecodeKeyFromStringFn) {
    let _ = DECODE_KEY_FROM_STRING.set(decode);
}

/// Sets Go `expression.EncodeRecordKeyFromRow` (`planner/core`'s `init`).
pub fn set_encode_record_key_from_row(encode: EncodeKeyFromRowFn) {
    let _ = ENCODE_RECORD_KEY_FROM_ROW.set(encode);
}

/// Sets Go `expression.EncodeIndexKeyFromRow` (`planner/core`'s `init`).
pub fn set_encode_index_key_from_row(encode: EncodeKeyFromRowFn) {
    let _ = ENCODE_INDEX_KEY_FROM_ROW.set(encode);
}

/// Go `builtinTiDBDecodeKeySig.evalString`: the latest infoschema's decoding
/// of the key, the key itself when `DecodeKeyFromString` is not set.
pub(crate) fn tidb_decode_key(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let input = value
        .sql_bytes()
        .map_err(|_| EvalError::IncorrectArguments("tidb_decode_key".to_owned()))?;
    let is = tidb_expropt::InfoSchemaPropReader.get_latest_info_schema(ctx).map_err(opt_prop_error)?;
    let Some(decode) = DECODE_KEY_FROM_STRING.get() else {
        return Ok(Datum::new_string_datum(input));
    };
    Ok(Datum::new_string_datum(decode(ctx, &*is, &input)))
}

/// Go `ErrSpecificAccessDenied` (1227) for the PROCESS privilege
/// `TIDB_DECODE_SQL_DIGESTS` requires, at build time and at evaluation.
pub(crate) fn err_specific_access_denied() -> EvalError {
    err_specific_access_denied_for("PROCESS")
}

/// Go `expropt`'s reader errors (`getPropProvider`'s missing or mistyped
/// property), returned as they are.
pub(crate) fn opt_prop_error(error: tidb_expropt::ExprOptError) -> EvalError {
    let message = error.message().to_owned();
    error.into_eval().unwrap_or(EvalError::Raw(message))
}

/// Go `TIDB_DECODE_SQL_DIGESTS`' check, at build and at evaluation:
/// `GetPrivilegeChecker(ctx)`, then `RequestVerification("", "", "",
/// mysql.ProcessPriv)`.
pub fn check_process_privilege(ctx: &dyn crate::Columns) -> Result<(), EvalError> {
    let priv_checker = tidb_expropt::PrivilegeCheckerPropReader.get_privilege_checker(ctx).map_err(opt_prop_error)?;
    if !priv_checker.request_verification("", "", "", tidb_mysql::ProcessPriv) {
        return Err(err_specific_access_denied());
    }
    Ok(())
}

/// Go `plannererrors.ErrSpecificAccessDenied.FastGenByArgs(privilege)`.
fn err_specific_access_denied_for(privilege: &str) -> EvalError {
    EvalError::Conversion(
        tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
            .gen_with_stack_by_args(&[tidb_mysql::errors::FormatArg::from(privilege)]),
    )
}

/// Go `builtinTiDBEncodeRecordKeySig.evalString` (`index` false) and
/// `builtinTiDBEncodeIndexKeySig.evalString`: the lower-case hex of the
/// record or index key the names and values give, NULL for a NULL name.
pub(crate) fn tidb_encode_key(values: &[Datum], index: bool, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let is = tidb_expropt::InfoSchemaPropReader.get_latest_info_schema(ctx).map_err(opt_prop_error)?;
    let encode = if index { &ENCODE_INDEX_KEY_FROM_ROW } else { &ENCODE_RECORD_KEY_FROM_ROW };
    let Some(encode) = encode.get() else {
        return Err(EvalError::Raw(
            if index { "EncodeIndexKeyFromRow is not initialized" } else { "EncodeRecordKeyFromRow is not initialized" }.to_owned(),
        ));
    };
    let priv_checker = tidb_expropt::PrivilegeCheckerPropReader.get_privilege_checker(ctx).map_err(opt_prop_error)?;
    let names = if index { 3 } else { 2 };
    let mut evaluated = Vec::with_capacity(names);
    for value in &values[..names] {
        let Some(name) = crate::arg_eval_type::eval_string(value)? else {
            return Ok(Datum::Null);
        };
        evaluated.push(String::from_utf8_lossy(&name).into_owned());
    }
    let args = tidb_exprctx::EncodeKeyArgs {
        db: &evaluated[0],
        table: &evaluated[1],
        index: index.then(|| evaluated[2].as_str()),
        values: &values[names..],
    };
    match encode(ctx, &*priv_checker, &*is, &args) {
        Ok(key) => Ok(Datum::new_string_datum(key.iter().map(|byte| format!("{byte:02x}")).collect::<String>())),
        Err(tidb_exprctx::EncodeKeyError::Eval(error)) => Err(error),
        // Go `ErrTableaccessDenied.FastGenByArgs("SELECT",
        // sv.User.AuthUsername, sv.User.AuthHostname, tblName)`.
        Err(tidb_exprctx::EncodeKeyError::SpecificAccessDenied) => {
            let user = tidb_expropt::SessionVarsPropReader.get_session_vars(ctx).map_err(opt_prop_error)?.user();
            let (user, host) =
                user.as_deref().map_or(("", ""), |user| (user.auth_username.as_str(), user.auth_hostname.as_str()));
            Err(EvalError::Conversion(tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(&[
                "SELECT".into(),
                user.to_owned().into(),
                host.to_owned().into(),
                args.table.to_owned().into(),
            ])))
        }
    }
}

/// Go `builtinTiDBMVCCInfoSig.evalString`: TiKV's MVCC record of the
/// hex-encoded key, and, for an index key, its temporary index key's when
/// that holds anything, as a JSON array of `{"key", "mvcc"}`.
pub(crate) fn tidb_mvcc_info(key: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let priv_checker = tidb_expropt::PrivilegeCheckerPropReader.get_privilege_checker(ctx).map_err(opt_prop_error)?;
    if !priv_checker.request_verification("", "", "", tidb_mysql::SuperPriv) {
        return Err(err_specific_access_denied_for("SUPER"));
    }
    let Some(s) = crate::arg_eval_type::eval_string(key)? else {
        return Ok(Datum::Null);
    };
    let s = String::from_utf8_lossy(&s).into_owned();
    let mut encoded_key = tidb_goruntime::go_hex::decode_string(&s).map_err(EvalError::Raw)?;
    let store = tidb_expropt::KvStorePropReader.get_kv_store(ctx).map_err(opt_prop_error)?;
    let Some(h_store) = store.as_helper_storage() else {
        return Err(EvalError::Raw("storage is not a helper.Storage".to_owned()));
    };
    // Go `helper.NewHelper(hStore).GetMvccByEncodedKey`, and whether its
    // MVCC info holds a write, a value or a lock.
    let get_mvcc = |key: &[u8]| {
        h_store
            .mvcc_get_by_key(key)
            .map(|response| {
                let has_info = response
                    .info
                    .as_ref()
                    .is_some_and(|info| !info.writes.is_empty() || !info.values.is_empty() || info.lock.is_some());
                (tidb_kv::mvcc_json::marshal(&response), has_info)
            })
            .map_err(EvalError::Raw)
    };
    let (resp, _) = get_mvcc(&encoded_key)?;
    // Go `json.Marshal` of `[]*mvccInfoResult{{Key, Resp}}`.
    let entry = |key: &str, resp: &str| format!("{{\"key\":{},\"mvcc\":{resp}}}", serde_json::Value::from(key));
    let mut mvcc_info = vec![entry(&s, &resp)];
    if tidb_tablecodec::table_key::is_index_key(&encoded_key) && !tidb_tablecodec::table_key::is_temp_index_key(&encoded_key) {
        tidb_tablecodec::index_key_to_temp_index_key(&mut encoded_key)
            .map_err(|error| EvalError::Raw(format!("{error:?}")))?;
        let hex_str: String = encoded_key.iter().map(|byte| format!("{byte:02x}")).collect();
        let (res, has_info) = get_mvcc(&encoded_key)?;
        if has_info {
            mvcc_info.push(entry(&hex_str, &res));
        }
    }
    Ok(Datum::new_string_datum(format!("[{}]", mvcc_info.join(","))))
}

/// Go `builtinTiDBDecodeSQLDigestsSig.evalString`: the JSON array of
/// statement texts for a JSON array of digests, `null` for each item that is
/// not a string or whose digest no statement summary knows, each text cut to
/// `truncate_length` bytes plus `...` when that is positive. A digest list
/// that is not a JSON array is a 1210 warning and NULL.
pub(crate) fn tidb_decode_sql_digests(
    digests: &Datum,
    truncate_length: Option<&Datum>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    check_process_privilege(ctx)?;
    let Some(digests_str) = crate::arg_eval_type::eval_string(digests)? else {
        return Ok(Datum::Null);
    };
    let stmt_truncate_length = match truncate_length {
        Some(value) => crate::arg_eval_type::eval_int(value)?.unwrap_or(0),
        None => 0,
    };
    // Go `json.Unmarshal(digestsStr, &digests)` into `[]any`: invalid UTF-8
    // decodes to U+FFFD byte by byte, and a JSON `null` is the empty list.
    let decoded = serde_json::from_str::<Option<Vec<serde_json::Value>>>(
        &tidb_goruntime::decode_go_utf8_lossy(digests_str.as_slice()),
    );
    let Ok(items) = decoded.map(Option::unwrap_or_default) else {
        const ERR_MSG_MAX_LENGTH: usize = 32;
        let mut shown = digests_str;
        if shown.len() > ERR_MSG_MAX_LENGTH {
            shown.truncate(ERR_MSG_MAX_LENGTH);
            shown.extend_from_slice(b"...");
        }
        // Go `errIncorrectArgs.FastGen(...)` (`ErrWrongArguments`, 1210).
        ctx.append_go_warning(
            &crate::expression_errors::ERR_INCORRECT_ARGS,
            &format!(
                "The argument can't be unmarshalled as JSON array: '{}'",
                String::from_utf8_lossy(&shown)
            ),
        );
        return Ok(Datum::Null);
    };
    // Query the SQL statements by digests.
    let mut retriever = crate::expr_util::SqlDigestTextRetriever::new();
    for item in &items {
        if let Some(digest) = item.as_str() {
            retriever.sql_digests_map.insert(digest.to_owned(), String::new());
        }
    }
    // Go bounds the query with a context timed out at `max_execution_time`
    // (20s at most), which it reads from `GetSessionVars`; the restricted
    // SQL executor takes no deadline here, so only the executor is read.
    let exec = tidb_expropt::SqlExecutorPropReader.get_sql_executor(ctx).map_err(opt_prop_error)?;
    if let Err(error) = retriever.retrieve_global(exec.as_ref()) {
        // Go `errUnknown.FastGen(...)` (1105), the builtin answering NULL.
        ctx.append_warning(
            1105,
            &format!("Retrieving statements information failed with error: {error}"),
        );
        return Ok(Datum::Null);
    }
    let texts = retriever.sql_digests_map;
    // Go `json.Marshal(result)` over `[]any` holding nil or the texts.
    let mut result = String::from("[");
    for (position, item) in items.iter().enumerate() {
        if position > 0 {
            result.push(',');
        }
        let text = item
            .as_str()
            .and_then(|digest| texts.get(digest))
            .filter(|text| !text.is_empty());
        match text {
            Some(text) => {
                let mut stmt = text.as_bytes().to_vec();
                if stmt_truncate_length > 0 && stmt.len() as i64 > stmt_truncate_length {
                    stmt.truncate(stmt_truncate_length as usize);
                    stmt.extend_from_slice(b"...");
                }
                result.push_str(&tidb_goruntime::GoString::from_bytes(stmt).to_go_json_literal());
            }
            None => result.push_str("null"),
        }
    }
    result.push(']');
    Ok(Datum::new_string_datum(result.into_bytes()))
}

#[derive(Default)]
struct UuidClock {
    last_v1: u64,
    clock_sequence: u16,
    node: [u8; 6],
    initialized: bool,
    last_v7: u64,
}

fn uuid_clock() -> &'static Mutex<UuidClock> {
    static CLOCK: OnceLock<Mutex<UuidClock>> = OnceLock::new();
    CLOCK.get_or_init(|| Mutex::new(UuidClock::default()))
}

fn unix_nanos() -> Result<u128, EvalError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|_| EvalError::Unsupported("system clock is before the Unix epoch"))
}

fn random_bytes(bytes: &mut [u8]) -> Result<(), EvalError> {
    getrandom::fill(bytes).map_err(|_| EvalError::Unsupported("OS random source unavailable"))
}

/// `UUID()`: RFC 9562 version 1, following the clock/sequence layout used by
/// TiDB's pinned `google/uuid.NewUUID`. The node identifier uses that
/// library's process-random fallback instead of exposing a hardware address;
/// it remains process-stable, and the 14-bit clock sequence advances whenever
/// the clock does not advance.
pub(crate) fn uuid_v1() -> Result<Datum, EvalError> {
    let unix_100ns = u64::try_from(unix_nanos()? / 100)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let timestamp =
        unix_100ns
            .checked_add(UUID_EPOCH_100NS as u64)
            .ok_or(EvalError::Unsupported(
                "system clock exceeds the UUID time domain",
            ))?;
    let mut state = uuid_clock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !state.initialized {
        let mut seed = [0_u8; 8];
        random_bytes(&mut seed)?;
        state.clock_sequence = (u16::from_be_bytes([seed[0], seed[1]]) & 0x3fff) | 0x8000;
        state.node.copy_from_slice(&seed[2..]);
        state.initialized = true;
    }
    if timestamp <= state.last_v1 {
        state.clock_sequence = ((state.clock_sequence + 1) & 0x3fff) | 0x8000;
    }
    state.last_v1 = timestamp;

    let mut uuid = [0_u8; 16];
    uuid[..4].copy_from_slice(&(timestamp as u32).to_be_bytes());
    uuid[4..6].copy_from_slice(&((timestamp >> 32) as u16).to_be_bytes());
    uuid[6..8].copy_from_slice(&(((timestamp >> 48) as u16 & 0x0fff) | 0x1000).to_be_bytes());
    uuid[8..10].copy_from_slice(&state.clock_sequence.to_be_bytes());
    uuid[10..].copy_from_slice(&state.node);
    Ok(Datum::new_string_datum(format_uuid(&uuid)))
}

/// `UUID_V4()`: 122 random bits with the RFC version and variant bits.
pub(crate) fn uuid_v4() -> Result<Datum, EvalError> {
    let mut uuid = [0_u8; 16];
    random_bytes(&mut uuid)?;
    uuid[6] = (uuid[6] & 0x0f) | 0x40;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    Ok(Datum::new_string_datum(format_uuid(&uuid)))
}

/// `UUID_V7()`: the Unix-millisecond prefix and sub-millisecond sequence
/// used by TiDB's pinned `google/uuid.NewV7`, plus random tail bits.
pub(crate) fn uuid_v7() -> Result<Datum, EvalError> {
    let mut uuid = [0_u8; 16];
    random_bytes(&mut uuid)?;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;

    let nanos = unix_nanos()?;
    let millis = u64::try_from(nanos / 1_000_000)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let sub_millis = u64::try_from((nanos % 1_000_000) >> 8)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let mut state = uuid_clock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut combined = millis
        .checked_mul(1 << 12)
        .and_then(|value| value.checked_add(sub_millis))
        .ok_or(EvalError::Unsupported(
            "system clock exceeds the UUID time domain",
        ))?;
    if combined <= state.last_v7 {
        combined = state.last_v7.checked_add(1).ok_or(EvalError::Unsupported(
            "system clock exceeds the UUID time domain",
        ))?;
    }
    state.last_v7 = combined;
    let millis = combined >> 12;
    let sequence = combined & 0x0fff;

    let time = millis.to_be_bytes();
    uuid[..6].copy_from_slice(&time[2..]);
    uuid[6] = 0x70 | (sequence >> 8) as u8;
    uuid[7] = sequence as u8;
    Ok(Datum::new_string_datum(format_uuid(&uuid)))
}

/// `UUID_TO_BIN(string_uuid, swap_flag)`, ported from
/// `builtinUUIDToBinSig.evalString` in `pkg/expression/builtin_miscellaneous.go`.
/// The Go signature is binary `ETString`: successful output is therefore raw
/// sixteen-byte data, not UTF-8 text.  `StringDatum` and `Bytes` both retain
/// those bytes; numeric values use the source's `ETString` coercion.  The
/// strict whitespace check is intentionally performed before `parse_uuid`,
/// because MySQL rejects surrounding spaces although `google/uuid.Parse`
/// accepts the inner spelling.
pub(crate) fn uuid_to_bin(value: &Datum, flag: Option<&Datum>) -> Result<Datum, EvalError> {
    let Some(input) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    // Go `builtinUUIDToBinSig.evalString`: surrounding spaces and an
    // unparseable UUID are both `errWrongValueForType("string", val,
    // "uuid_to_bin")` (1411).
    let invalid = || EvalError::WrongValueForType {
        value_class: "string",
        value: String::from_utf8_lossy(&input).into_owned(),
        function: "uuid_to_bin",
    };
    if std::str::from_utf8(&input)
        .map(|text| text.trim() != text)
        .unwrap_or(false)
    {
        return Err(invalid());
    }
    let Some(uuid) = parse_uuid(&input) else {
        return Err(invalid());
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
pub(crate) fn bin_to_uuid(
    ctx: &dyn crate::Columns,
    value: &Datum,
    flag: Option<&Datum>,
) -> Result<Datum, EvalError> {
    // go's flag argument reaches the signature through `WrapWithCastAsInt`,
    // a plan-time CAST whose constant fold warns `Truncated incorrect
    // INTEGER value` BEFORE the 1411 payload error is raised at exec -- so
    // the warning survives the error (captured: BIN_TO_UUID('中文测试',
    // 'ünïcödé') answers the error with the truncation warning kept).
    // Coercing the flag ahead of the payload check reproduces that buffer.
    let flag_int = match flag {
        Some(flag @ (Datum::String(_) | Datum::Bytes(_))) => {
            crate::cast::report_int_truncation(flag, ctx)?;
            crate::cast::to_i64_signed(flag)
        }
        Some(flag) if !matches!(flag, Datum::Null) => crate::cast::to_i64_signed(flag),
        _ => 0,
    };
    let Some(input) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    if input.len() != 16 {
        // go `builtinBinToUUIDSig.evalString`:
        // `types.ErrWrongValueForType("string", str, "bin_to_uuid")` (1411) —
        // the message word is the WIRE class "string" even though the check
        // is the 16-byte payload length (captured on the oracle:
        // "Incorrect string value: '1' for function bin_to_uuid").
        return Err(EvalError::WrongValueForType {
            value_class: "string",
            value: String::from_utf8_lossy(&input).into_owned(),
            function: "bin_to_uuid",
        });
    }
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&input);
    let output = if flag_int != 0 {
        format_uuid_swapped(&uuid)
    } else {
        format_uuid(&uuid)
    };
    Ok(Datum::new_string_datum(output))
}

/// The Go `EvalString` payload boundary used by UUID_TO_BIN/BIN_TO_UUID.
/// String/bytes datums are returned byte-for-byte; scalar numeric values are
/// stringified, while NULL propagates without evaluating the optional flag.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    crate::coerce::coerce_str_bytes(value)
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

/// `TIDB_SHARD(value)`, ported from `builtinTidbShardSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB first casts the one argument to signed `ETInt`, then Vitess-hashes its
/// two's-complement `uint64` bits and takes the big-endian ciphertext's low
/// byte. The bucket count is 256, so the low byte is exactly the modulo.
pub(crate) fn tidb_shard(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(Datum::Null);
    }
    // `WrapWithCastAsInt` selects the ETInt cast signature before
    // `builtinTidbShardSig.evalInt` runs. Reuse the same integer-prefix
    // conversion used by this evaluator's SIGNED cast for scalar values.
    let shard_key = crate::cast::to_i64_signed_with_warnings(value, ctx)? as u64;
    Ok(Datum::UInt(hash_uint64(shard_key) % 256))
}

/// `VITESS_HASH(shard_key)`, ported from `builtinVitessHashSig.evalInt`. Like
/// `TIDB_SHARD`, the ETInt-coerced argument is Vitess-hashed, but the whole
/// 64-bit digest is returned. The result column is UNSIGNED, so it is a
/// `Datum::UInt`.
pub(crate) fn vitess_hash(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(Datum::Null);
    }
    let shard_key = crate::cast::to_i64_signed_with_warnings(value, ctx)? as u64;
    Ok(Datum::UInt(hash_uint64(shard_key)))
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
pub(crate) fn is_uuid(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(is_uuid_bytes(&value))))
}

pub(crate) fn is_uuid_bytes(value: &[u8]) -> bool {
    // The parser requires ASCII in every accepted spelling, except for the
    // two ignored bytes around its 38-byte form. Go checks those for space.
    if value.len() == 38
        && [value[0], value[37]].into_iter().any(|byte| matches!(byte, b' ' | b'\t'..=b'\r'))
    {
        return false;
    }
    uuid_body(value).is_some_and(uuid_body_is_hex)
}

fn uuid_body_is_hex(body: &[u8]) -> bool {
    if body.len() == 32 {
        return uuid_hex(body.as_array::<32>().unwrap());
    }
    uuid_has_separators(body)
        && uuid_hex(body[..8].as_array::<8>().unwrap())
        && uuid_hex(body[9..13].as_array::<4>().unwrap())
        && uuid_hex(body[14..18].as_array::<4>().unwrap())
        && uuid_hex(body[19..23].as_array::<4>().unwrap())
        && uuid_hex(body[24..].as_array::<12>().unwrap())
}

#[inline]
fn uuid_hex<const N: usize>(bytes: &[u8; N]) -> bool {
    // A fixed-size reduction lets the compiler check the bytes in parallel.
    bytes.iter().fold(true, |valid, byte| valid & byte.is_ascii_hexdigit())
}

/// `UUID_VERSION(value)`, ported from `builtinUUIDVersionSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// Reads the version nibble after validating Go's accepted UUID forms.
pub(crate) fn uuid_version(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = uuid_input(value)? else {
        return Ok(Datum::Null);
    };
    uuid_version_bytes(&value).map(Datum::Int)
}

pub(crate) fn uuid_version_bytes(value: &[u8]) -> Result<i64, EvalError> {
    let body = valid_uuid_body(value, "uuid_version")?;
    Ok(i64::from(uuid_version_nibble(body)))
}

/// `UUID_TIMESTAMP(value)`, ported from `builtinUUIDTimestampSig`.
/// Versions 1, 6 and 7 carry a time represented as DECIMAL(18,6).
pub(crate) fn uuid_timestamp(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = uuid_input(value)? else {
        return Ok(Datum::Null);
    };
    Ok(uuid_timestamp_micros(&value)?.map_or(Datum::Null, |micros| {
        Datum::Decimal(Decimal::from_scaled_i128(i128::from(micros), 6))
    }))
}

pub(crate) fn uuid_timestamp_micros(value: &[u8]) -> Result<Option<i64>, EvalError> {
    let body = valid_uuid_body(value, "uuid_timestamp")?;
    let version = uuid_version_nibble(body);
    if !matches!(version, 1 | 6 | 7) {
        return Ok(None);
    }
    let mut prefix = [0; 8];
    if body.len() == 32 {
        decode_uuid_prefix(&body[..16], &mut prefix);
    } else {
        decode_uuid_prefix(&body[..8], &mut prefix[..4]);
        decode_uuid_prefix(&body[9..13], &mut prefix[4..6]);
        decode_uuid_prefix(&body[14..18], &mut prefix[6..]);
    }

    let timestamp_100ns = match version {
        1 => uuid_v1_timestamp_100ns(&prefix),
        6 => uuid_v6_timestamp_100ns(&prefix),
        _ => uuid_v7_timestamp_100ns(&prefix),
    };
    // Go splits 100ns ticks into seconds and nanoseconds, and divides the
    // nanoseconds by 1,000. Both steps truncate toward zero.
    Ok(Some((timestamp_100ns - UUID_EPOCH_100NS) / 10))
}

fn uuid_input(value: &Datum) -> Result<Option<Cow<'_, [u8]>>, EvalError> {
    match value {
        Datum::String(value) => Ok(Some(Cow::Borrowed(value.bytes()))),
        Datum::Bytes(value) => Ok(Some(Cow::Borrowed(value))),
        _ => Ok(coerce_str(value)?.map(|text| Cow::Owned(text.into_bytes()))),
    }
}

fn valid_uuid_body<'a>(value: &'a [u8], function: &'static str) -> Result<&'a [u8], EvalError> {
    uuid_body(value).filter(|body| uuid_body_is_hex(body)).ok_or_else(|| {
        EvalError::WrongValueForType {
            value_class: "string",
            value: String::from_utf8_lossy(value).into_owned(),
            function,
        }
    })
}

fn uuid_version_nibble(body: &[u8]) -> u8 {
    uuid_hex_nibble(body[if body.len() == 32 { 12 } else { 14 }])
}

fn decode_uuid_prefix(hex: &[u8], output: &mut [u8]) {
    for (byte, &[high, low]) in output.iter_mut().zip(hex.as_chunks::<2>().0) {
        *byte = uuid_hex_nibble(high) << 4 | uuid_hex_nibble(low);
    }
}

fn uuid_hex_nibble(digit: u8) -> u8 {
    // For a validated hex digit, bit 6 distinguishes letters from numbers.
    (digit & 15) + ((digit >> 6) * 9)
}

const UUID_EPOCH_100NS: i64 = 122_192_928_000_000_000;

fn uuid_v1_timestamp_100ns(uuid: &[u8; 8]) -> i64 {
    i64::from(u32::from_be_bytes([uuid[0], uuid[1], uuid[2], uuid[3]]))
        | (i64::from(u16::from_be_bytes([uuid[4], uuid[5]])) << 32)
        | (i64::from(u16::from_be_bytes([uuid[6], uuid[7]]) & 0x0fff) << 48)
}

fn uuid_v6_timestamp_100ns(uuid: &[u8; 8]) -> i64 {
    (i64::from(u32::from_be_bytes([uuid[0], uuid[1], uuid[2], uuid[3]])) << 28)
        | (i64::from(u16::from_be_bytes([uuid[4], uuid[5]])) << 12)
        | i64::from(u16::from_be_bytes([uuid[6], uuid[7]]) & 0x0fff)
}

fn uuid_v7_timestamp_100ns(uuid: &[u8; 8]) -> i64 {
    let first_eight = u64::from_be_bytes([
        uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7],
    ]);
    ((first_eight >> 16) * 10_000) as i64 + UUID_EPOCH_100NS
}

/// Exact parse shape of the `google/uuid.Parse` implementation TiDB calls,
/// decoded to its 16 bytes. Parsing works on bytes because Go selects forms
/// by byte length and must retain its intentionally permissive 38-byte form.
fn parse_uuid(value: &[u8]) -> Option<[u8; 16]> {
    let canonical = uuid_body(value)?;

    let mut bytes = [0_u8; 16];
    if canonical.len() == 32 {
        for (byte, pair) in bytes.iter_mut().zip(canonical.as_chunks::<2>().0) {
            *byte = hex_pair(pair[0], pair[1])?;
        }
        return Some(bytes);
    }

    if !uuid_has_separators(canonical) {
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

fn uuid_body(value: &[u8]) -> Option<&[u8]> {
    match value.len() {
        32 | 36 => Some(value),
        45 if value[..9].eq_ignore_ascii_case(b"urn:uuid:") => Some(&value[9..]),
        // Go's parser inspects only the inner 36 bytes of this form.
        38 => Some(&value[1..37]),
        _ => None,
    }
}

fn uuid_has_separators(body: &[u8]) -> bool {
    body[8] == b'-' && body[13] == b'-' && body[18] == b'-' && body[23] == b'-'
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
    use std::cell::RefCell;

    use super::hash_uint64;
    use crate::Datum;

    /// Go's builtin test door over a context-free session.
    fn eval_builtin_func(name: &str, vals: &[Datum]) -> Option<Result<Datum, crate::EvalError>> {
        crate::tests::eval_builtin_func(name, vals, &crate::NoColumns)
    }
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
use tidb_charset::Collation;

    #[derive(Default)]
    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

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
                Datum::new_string_datum("Hello, World".to_string()),
                Datum::new_string_datum("Hello, World".to_string()),
            ),
        ];
        for (argument, want) in cases {
            assert_eq!(
                eval_builtin_func("ANY_VALUE", std::slice::from_ref(&argument))
                    .expect("ANY_VALUE must dispatch")
                    .expect("ANY_VALUE must evaluate"),
                want
            );
        }
    }

    /// Go `TestAnyValueHybridStringEvalWithIntSig`: ENUM, SET and BIT select
    /// the integer signature but must still expose their underlying hybrid
    /// bytes in string context. Rust has one value dispatcher rather than
    /// separate scalar/vector signatures, so both halves are asserted here:
    /// `ANY_VALUE` preserves the hybrid datum and string coercion reads its
    /// name/raw bytes instead of its integer ordinal.
    #[test]
    fn test_any_value_hybrid_string_eval_with_int_sig() {
        let cases = [
            (
                Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin),
                b"b".as_slice(),
            ),
            (
                Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin),
                b"a,b".as_slice(),
            ),
            (
                Datum::Bit(BinaryLiteral::from(vec![0x01])),
                b"\x01".as_slice(),
            ),
        ];

        for (argument, expected_string) in cases {
            let result = eval_builtin_func("ANY_VALUE", std::slice::from_ref(&argument))
                .expect("ANY_VALUE must dispatch")
                .expect("hybrid source row must evaluate");
            assert_eq!(result, argument, "the hybrid datum must be preserved");
            assert_eq!(
                crate::coerce::coerce_str_bytes(&result)
                    .expect("hybrid string coercion must succeed")
                    .expect("source rows are non-NULL"),
                expected_string
            );
        }
    }

    #[test]
    fn dispatch_declines_wrong_arity_and_foreign_functions() {
        assert!(eval_builtin_func("ANY_VALUE", &[]).is_none());
        assert!(eval_builtin_func("ANY_VALUE", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(eval_builtin_func("NAME_CONST", &[]).is_none());
        assert!(eval_builtin_func("NAME_CONST", &[Datum::new_string_datum("name".to_string())]).is_none());
        assert!(eval_builtin_func("NAME_CONST", &[Datum::Int(1)]).is_none());
        assert!(eval_builtin_func("NAME_CONST", &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]).is_none());
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
            (Datum::new_string_datum("test_int"), Datum::Int(3)),
            (Datum::new_string_datum("test_uint"), Datum::UInt(u64::MAX)),
            (Datum::new_string_datum("test_float"), Datum::Real(source_float)),
            (Datum::new_string_datum("test_string"), Datum::new_string_datum("TiDB")),
            (
                Datum::new_string_datum("test_binary"),
                Datum::new_bytes_datum(vec![0, 0xff, 0x80]),
            ),
            (Datum::new_string_datum("test_null"), Datum::Null),
            (Datum::new_string_datum("test_decimal"), Datum::Decimal(decimal)),
            // A NULL label is accepted by Go's ETString conversion; it does
            // not alter the value returned by NAME_CONST.
            (Datum::Null, Datum::Int(-7)),
        ];
        for (name, value) in cases {
            let got = eval_builtin_func("NAME_CONST", &[name, value.clone()])
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
                eval_builtin_func("UUID_VERSION", &[Datum::new_string_datum(text.to_string())])
                    .expect("UUID_VERSION must dispatch")
                    .expect("well-formed UUID must evaluate"),
                Datum::Int(want),
                "UUID_VERSION({text:?})"
            );
        }
        assert_eq!(
            eval_builtin_func("UUID_VERSION", &[Datum::Null])
                .expect("UUID_VERSION must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            eval_builtin_func("UUID_VERSION", &[Datum::new_string_datum("abc".to_string())])
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
                eval_builtin_func("IS_UUID", &[Datum::new_string_datum(text.to_string())])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(want),
                "IS_UUID({text:?})"
            );
        }
        assert_eq!(
            eval_builtin_func("IS_UUID", &[Datum::Null])
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
                eval_builtin_func("IS_UUID", &[value])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(0)
            );
        }
    }

    #[test]
    fn is_uuid_preserves_go_byte_string_boundaries() {
        for value in [
            Datum::new_string_datum(vec![0xff]),
            Datum::Bytes(vec![0xff]),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
        ] {
            assert_eq!(
                eval_builtin_func("IS_UUID", &[value])
                    .expect("IS_UUID must dispatch")
                    .expect("invalid bytes are a non-UUID value, not an evaluation error"),
                Datum::Int(0)
            );
        }

        let mut invalid_wrapper = vec![0xff];
        invalid_wrapper.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3*");
        assert_eq!(
            eval_builtin_func("IS_UUID", &[Datum::Bytes(invalid_wrapper)])
                .expect("IS_UUID must dispatch")
                .expect("Go's 38-byte UUID parse ignores the invalid wrapper bytes"),
            Datum::Int(1)
        );

        let mut leading_space = vec![b' '];
        leading_space.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3");
        leading_space.push(0xff);
        assert_eq!(
            eval_builtin_func("IS_UUID", &[Datum::Bytes(leading_space)])
                .expect("IS_UUID must dispatch")
                .expect("Go checks surrounding whitespace before its UUID parse"),
            Datum::Int(0)
        );
    }

    #[test]
    fn uuid_metadata_accepts_go_non_utf8_wrappers() {
        let value = Datum::new_string_datum(b"\xff00000000-0000-7000-8000-000000000000\xfe".as_slice());
        assert_eq!(
            eval_builtin_func("UUID_VERSION", std::slice::from_ref(&value)).unwrap().unwrap(),
            Datum::Int(7)
        );
        assert_eq!(
            eval_builtin_func("UUID_TIMESTAMP", &[value]).unwrap().unwrap(),
            Datum::Decimal(crate::Decimal::from_literal("0.000000"))
        );
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
                eval_builtin_func("UUID_TIMESTAMP", &[Datum::new_string_datum(text.to_string())])
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
                eval_builtin_func("UUID_TIMESTAMP", &[Datum::new_string_datum(text.to_string())])
                    .expect("UUID_TIMESTAMP must dispatch")
                    .expect("valid non-timestamp UUID must evaluate"),
                Datum::Null,
                "UUID_TIMESTAMP({text:?})"
            );
        }
        assert_eq!(
            eval_builtin_func("UUID_TIMESTAMP", &[Datum::Null])
                .expect("UUID_TIMESTAMP must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            eval_builtin_func("UUID_TIMESTAMP", &[Datum::new_string_datum("abc".to_string())])
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
                eval_builtin_func("UUID_TO_BIN", &[Datum::new_string_datum(spelling)])
                    .expect("UUID_TO_BIN must dispatch")
                    .expect("valid UUID must evaluate"),
                Datum::Bytes(normal.clone()),
                "UUID_TO_BIN({spelling:?})"
            );
        }
        assert_eq!(
            eval_builtin_func(
                "UUID_TO_BIN",
                &[Datum::new_string_datum(canonical), Datum::Int(1)],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("swap flag must evaluate"),
            Datum::Bytes(swapped.clone())
        );
        assert_eq!(
            eval_builtin_func("UUID_TO_BIN", &[Datum::new_string_datum(canonical), Datum::Null],)
                .expect("UUID_TO_BIN must dispatch")
                .expect("NULL swap flag defaults to zero"),
            Datum::Bytes(normal.clone())
        );
        // Go records a truncation warning for the textual flag "a" but its
        // ETInt value is zero; the warning channel is outside this seed while
        // the value-domain result remains pinned here.
        assert_eq!(
            eval_builtin_func(
                "UUID_TO_BIN",
                &[Datum::new_string_datum(canonical), Datum::new_string_datum("a")],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::Bytes(normal.clone())
        );
        assert_eq!(
            eval_builtin_func("UUID_TO_BIN", &[Datum::Null])
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
                eval_builtin_func("UUID_TO_BIN", &[Datum::new_string_datum(invalid)])
                    .expect("UUID_TO_BIN must dispatch")
                    .is_err(),
                "invalid UUID_TO_BIN input {invalid:?}"
            );
        }

        assert_eq!(
            eval_builtin_func("BIN_TO_UUID", &[Datum::Bytes(normal.clone())])
                .expect("BIN_TO_UUID must dispatch")
                .expect("binary UUID must evaluate"),
            Datum::new_string_datum(canonical)
        );
        assert_eq!(
            eval_builtin_func(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::Int(1)],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("swap flag must evaluate"),
            Datum::new_string_datum("baba1026-780c-6ccd-9564-5b8c656024db")
        );
        assert_eq!(
            eval_builtin_func(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::new_string_datum("a")],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::new_string_datum(canonical)
        );
        // A raw binary UUID is valid even though the payload is not UTF-8.
        assert_eq!(
            eval_builtin_func("BIN_TO_UUID", &[Datum::Bytes(swapped)])
                .expect("BIN_TO_UUID must dispatch")
                .expect("raw bytes must evaluate"),
            Datum::new_string_datum("1026baba-6ccd-780c-9564-5b8c656024db")
        );
        assert_eq!(
            eval_builtin_func("BIN_TO_UUID", &[Datum::Null])
                .expect("BIN_TO_UUID must dispatch")
                .expect("NULL binary UUID must evaluate"),
            Datum::Null
        );
        assert!(
            eval_builtin_func("BIN_TO_UUID", &[Datum::Bytes(normal[..15].to_vec())])
                .expect("BIN_TO_UUID must dispatch")
                .is_err()
        );
        assert!(eval_builtin_func("UUID_TO_BIN", &[]).is_none());
        assert!(eval_builtin_func(
            "UUID_TO_BIN",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_none());
        assert!(eval_builtin_func("BIN_TO_UUID", &[]).is_none());
        assert!(eval_builtin_func(
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
                eval_builtin_func("TIDB_SHARD", &[value])
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
                eval_builtin_func("TIDB_SHARD", &[Datum::new_string_datum(text.to_string())])
                    .expect("TIDB_SHARD must dispatch")
                    .expect("string TIDB_SHARD must evaluate"),
                Datum::UInt(want),
                "TIDB_SHARD({text:?})",
            );
        }

        assert_eq!(
            eval_builtin_func("TIDB_SHARD", &[Datum::Real(1.9)])
                .expect("TIDB_SHARD must dispatch")
                .expect("real TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            eval_builtin_func(
                "TIDB_SHARD",
                &[Datum::Decimal(crate::Decimal::from_literal("1.9"))],
            )
            .expect("TIDB_SHARD must dispatch")
            .expect("decimal TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            eval_builtin_func("TIDB_SHARD", &[Datum::Null])
                .expect("TIDB_SHARD must dispatch")
                .expect("NULL TIDB_SHARD must evaluate"),
            Datum::Null,
        );

        // The Go function class rejects every arity other than one before
        // evaluation; this family dispatch has the same boundary.
        assert!(eval_builtin_func("TIDB_SHARD", &[]).is_none());
        assert!(eval_builtin_func("TIDB_SHARD", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(eval_builtin_func("UNKNOWN", &[Datum::Int(1)]).is_none());
    }

    #[test]
    fn hash_builtins_preserve_implicit_signed_cast_overflow_warnings() {
        for (name, expected) in [
            ("VITESS_HASH", Datum::UInt(hash_uint64((-2_i64) as u64))),
            (
                "TIDB_SHARD",
                Datum::UInt(hash_uint64((-2_i64) as u64) % 256),
            ),
        ] {
            let ctx = WarningContext::default();
            let value = crate::tests::eval_builtin_func(
                name,
                &[Datum::new_string_datum("18446744073709551614".to_owned())],
                &ctx,
            )
            .expect("the builtin must dispatch")
            .expect("the implicit cast must evaluate");
            assert_eq!(value, expected);
            assert_eq!(
                *ctx.0.borrow(),
                vec![(
                    8030,
                    "Cast to signed converted positive out-of-range integer to its negative complement"
                        .to_owned(),
                )],
            );
        }

        let ctx = WarningContext::default();
        let value = crate::tests::eval_builtin_func(
            "VITESS_HASH",
            &[Datum::new_string_datum("18446744073709551616".to_owned())],
            &ctx,
        )
        .expect("VITESS_HASH must dispatch")
        .expect("the overflowing implicit cast must retain its best-effort value");
        assert_eq!(value, Datum::UInt(hash_uint64(u64::MAX)));
        assert_eq!(
            *ctx.0.borrow(),
            vec![(
                1292,
                "Truncated incorrect INTEGER value: '18446744073709551616'".to_owned(),
            )],
        );
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
                eval_builtin_func("VITESS_HASH", &[value])
                    .expect("VITESS_HASH must dispatch")
                    .expect("VITESS_HASH must evaluate"),
                Datum::UInt(want),
            );
        }
        assert_eq!(
            eval_builtin_func("VITESS_HASH", &[Datum::Null])
                .expect("VITESS_HASH must dispatch")
                .expect("NULL VITESS_HASH must evaluate"),
            Datum::Null,
        );
        assert!(eval_builtin_func("VITESS_HASH", &[]).is_none());
    }

    /// A region store over canned MVCC records, keyed by encoded key.
    #[derive(Debug, Default)]
    struct MvccRegions(std::collections::HashMap<Vec<u8>, tikv_client::proto::kvrpcpb::MvccGetByKeyResponse>);

    impl tidb_kv::region_map::RegionStore for MvccRegions {
        fn split_regions(&self, _: Vec<Vec<u8>>, _: bool, _: Option<i64>) -> (Vec<u64>, Option<String>) {
            unreachable!()
        }

        fn wait_scatter_region_finish(&self, _: u64, _: i64) -> Result<(), String> {
            unreachable!()
        }

        fn check_region_in_scattering(&self, _: u64) -> Result<bool, String> {
            unreachable!()
        }

        fn load_regions_in_key_range(&self, _: &[u8], _: &[u8]) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
            unreachable!()
        }

        fn mvcc_get_by_key(&self, key: &[u8]) -> Result<tikv_client::proto::kvrpcpb::MvccGetByKeyResponse, String> {
            Ok(self.0.get(key).cloned().unwrap_or_default())
        }
    }

    /// A `kv.Storage` that is a `helper.Storage` over `MvccRegions`, or not.
    struct MvccStore(Option<MvccRegions>);

    impl tidb_expropt::KvStorage for MvccStore {
        fn as_helper_storage(&self) -> Option<&dyn tidb_kv::region_map::RegionStore> {
            self.0.as_ref().map(|regions| regions as &dyn tidb_kv::region_map::RegionStore)
        }
    }

    /// A privilege checker that grants every privilege or none.
    struct SuperChecker(bool);

    impl tidb_expropt::PrivilegeChecker for SuperChecker {
        fn request_verification(&self, _: &str, _: &str, _: &str, _: tidb_mysql::PrivilegeType) -> bool {
            self.0
        }

        fn request_dynamic_verification(&self, _: &str, _: bool) -> bool {
            unreachable!()
        }
    }

    /// An eval context with Go's KV-store and privilege-checker properties.
    struct MvccContext(tidb_expropt::OptionalEvalPropProviders);

    impl MvccContext {
        fn new(super_privilege: bool, store: Option<MvccRegions>) -> Self {
            let mut props = tidb_expropt::OptionalEvalPropProviders::new();
            let store: std::sync::Arc<dyn tidb_expropt::KvStorage> = std::sync::Arc::new(MvccStore(store));
            props.add(std::sync::Arc::new(tidb_expropt::KvStorePropProvider::new(move || std::sync::Arc::clone(&store))));
            let checker: std::sync::Arc<dyn tidb_expropt::PrivilegeChecker> = std::sync::Arc::new(SuperChecker(super_privilege));
            props.add(std::sync::Arc::new(tidb_expropt::PrivilegeCheckerProvider::new(move || std::sync::Arc::clone(&checker))));
            MvccContext(props)
        }
    }

    impl crate::Columns for MvccContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_optional_prop_provider(
            &self,
            key: tidb_exprctx::OptionalEvalPropKey,
        ) -> Option<std::sync::Arc<dyn tidb_exprctx::DynOptionalEvalPropProvider>> {
            self.0.get(key)
        }
    }

    /// Go `builtinTiDBDecodeSQLDigestsSig.evalString`: PROCESS is asked
    /// through `GetPrivilegeChecker`, and a context without the property
    /// is `getPropProvider`'s error.
    #[test]
    fn tidb_decode_sql_digests_asks_process_through_the_privilege_checker() {
        let digests = Datum::new_string_datum("[]".to_owned());
        let error = super::tidb_decode_sql_digests(&digests, None, &MvccContext::new(false, None)).unwrap_err();
        assert!(format!("{error:?}").contains("PROCESS"), "{error:?}");
        let error = super::tidb_decode_sql_digests(&digests, None, &crate::NoColumns).unwrap_err();
        assert!(
            matches!(error, crate::EvalError::Raw(ref message) if message == "optional property: 'OptPropPrivilegeChecker' not exists in EvalContext"),
            "{error:?}"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Go `builtinTiDBMVCCInfoSig.evalString`: the key's record, and an
    /// index key's temporary index record only when it holds anything;
    /// SUPER is required, and the key is Go's `hex.DecodeString`.
    #[test]
    fn tidb_mvcc_info_answers_the_key_and_its_temp_index_key() {
        let eval = |ctx: &MvccContext, key: &str| super::tidb_mvcc_info(&Datum::new_string_datum(key.to_owned()), ctx);
        let text = |datum: Datum| match datum {
            Datum::String(s) => String::from_utf8(s.bytes().to_vec()).unwrap(),
            other => panic!("a string, not {other:?}"),
        };
        let record_key = tidb_tablecodec::table_key::encode_row_key_with_handle(1, &tidb_tablecodec::table_key::RecordHandle::Int(1));
        let mut index_key = tidb_tablecodec::table_key::encode_table_index_prefix(1, 2);
        index_key.extend_from_slice(b"\x03\x80\x00\x00\x00\x00\x00\x00\x01");
        let mut temp_key = index_key.clone();
        tidb_tablecodec::index_key_to_temp_index_key(&mut temp_key).unwrap();
        let response = tikv_client::proto::kvrpcpb::MvccGetByKeyResponse {
            info: Some(tikv_client::proto::kvrpcpb::MvccInfo {
                writes: vec![tikv_client::proto::kvrpcpb::MvccWrite { start_ts: 1, commit_ts: 2, ..Default::default() }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let write = tidb_kv::mvcc_json::marshal(&response);
        assert!(write.contains("\"commit_ts\":2"), "{write}");
        let mut regions = MvccRegions([(record_key.clone(), response.clone()), (index_key.clone(), response.clone())].into());
        let ctx = MvccContext::new(true, Some(MvccRegions(regions.0.clone())));
        assert_eq!(
            text(eval(&ctx, &hex(&record_key)).unwrap()),
            format!(r#"[{{"key":"{}","mvcc":{write}}}]"#, hex(&record_key))
        );
        // The temporary index key holds nothing: only the key's record.
        assert_eq!(
            text(eval(&ctx, &hex(&index_key)).unwrap()),
            format!(r#"[{{"key":"{}","mvcc":{write}}}]"#, hex(&index_key))
        );
        regions.0.insert(temp_key.clone(), response.clone());
        let ctx = MvccContext::new(true, Some(regions));
        assert_eq!(
            text(eval(&ctx, &hex(&index_key)).unwrap()),
            format!(r#"[{{"key":"{}","mvcc":{write}}},{{"key":"{}","mvcc":{write}}}]"#, hex(&index_key), hex(&temp_key))
        );
        assert!(matches!(
            eval(&ctx, "7g").unwrap_err(),
            crate::EvalError::Raw(message) if message == "encoding/hex: invalid byte: U+0067 'g'"
        ));
        assert!(eval(&ctx, "").is_ok());
        let ctx = MvccContext::new(false, Some(MvccRegions::default()));
        assert!(format!("{:?}", eval(&ctx, &hex(&record_key)).unwrap_err()).contains("SUPER"));
        // A store that is not a `helper.Storage`.
        let error = eval(&MvccContext::new(true, None), &hex(&record_key)).unwrap_err();
        assert!(matches!(error, crate::EvalError::Raw(message) if message == "storage is not a helper.Storage"));
        // A context without the properties: Go's `getPropProvider` error.
        let error = super::tidb_mvcc_info(&Datum::new_string_datum(hex(&record_key)), &crate::NoColumns).unwrap_err();
        assert!(matches!(error, crate::EvalError::Raw(ref message) if message.contains("not exists in EvalContext")), "{error:?}");
    }
}
