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

//! Go `br/pkg/utils/key.go` lands complete: the key-format parser BR's CLI uses
//! to read `--start`/`--end` (`ParseKey`), the exclusive-end-key comparators
//! (`CompareEndKey`, `CompareBytesExt`), the segment-set intersection that
//! clamps backup ranges into a filter (`IntersectAll`), the BR timestamp
//! rendering (`FormatDate`), and the txn-meta-key predicates and encoder
//! (`IsMetaDBKey`, `IsMetaDDLJobHistoryKey`, `IsDBOrDDLJobHistoryKey`,
//! `EncodeTxnMetaKey`, `IsMetaAutoIDKey`), with all five of the file's test
//! functions.
//!
//! This module covers one file of Go package `br/pkg/utils`, not the whole
//! package; the rest of `br/pkg/utils` (retry, progress, worker pools, schema
//! helpers) is out of scope here.
//!
//! An "end key" in BR is always the *exclusive* upper bound of a range, and the
//! empty slice means "no upper bound" — greater than every other key. That one
//! asymmetry is what [`compare_end_key`] encodes and what
//! [`compare_bytes_ext`] generalizes: each side independently declares whether
//! its empty value means infinity. [`intersect_all`] then walks two sorted,
//! non-overlapping segment lists in lockstep and emits their pointwise
//! intersection.
//!
//! # Narrowings and boundaries
//!
//! `tidb-util` is the workspace's bottom crate, so every dependency that lives
//! above it is recovered locally rather than dropped:
//!
//! - **`pkg/kv.KeyRange`** is redeclared as [`KeyRange`] with the same two
//!   fields. `pkg/kv` sits above this crate.
//! - **`pkg/util/codec`** — `codec.EncodeBytes`, `codec.DecodeBytes`,
//!   `codec.EncodeUint`, `codec.DecodeUint` and `codec.EncodeUintDesc` are the
//!   memcomparable byte/number codecs. They are ported (crate `tidb-codec`),
//!   but that crate is *above* `tidb-util`, so [`encode_bytes`],
//!   [`decode_bytes`], [`encode_uint`], [`decode_uint`] and
//!   [`encode_uint_desc`] are local, private mirrors of exactly those five
//!   functions, kept byte-for-byte compatible (8-byte groups, `0xFF - padCount`
//!   marker, big-endian u64, bitwise-complemented big-endian u64).
//! - **`pkg/tablecodec`** — `tablecodec.EncodeMetaKey` / `DecodeMetaKey` become
//!   [`encode_meta_key`] / [`decode_meta_key`], with `metaPrefix` (`b"m"`) and
//!   `structure.HashData` (`b'h'`) inlined as [`META_PREFIX`] and
//!   [`HASH_DATA_FLAG`].
//! - **`pkg/meta`** — `meta.IsAutoIncrementIDKey`, `IsAutoTableIDKey`,
//!   `IsAutoRandomTableIDKey` and `IsSequenceKey` are prefix tests against
//!   `"IID:"`, `"TID:"`, `"TARID:"` and `"SID:"`; they are inlined into
//!   [`is_meta_auto_id_key`] as [`AUTO_ID_FIELD_PREFIXES`].
//! - **`br/pkg/errors.ErrInvalidArgument`** — [`ParseKeyError::UnknownFormat`]
//!   renders the same `"unknown format: ..."` annotation text the CLI matches
//!   on; the BR error-code registry itself is not part of this file.
//! - **`log.L().DPanic` + `br/pkg/logutil.StringifyKeys`** — the unreachable
//!   arm of [`intersect_all`] logs at error level through `tidb_log` with the
//!   ranges hex-rendered locally. Go's `DPanic` aborts in development builds
//!   only; there is no development-mode global here, and the arm is genuinely
//!   unreachable (`clamp_in_one_range` always overwrites the reason before it
//!   can fail), so the Rust side always logs and continues.
//! - **`oracle.GetTimeFromTS`** (client-go) is not part of this file; the
//!   `TestDateFormat` fixture needs it, so the tests carry the two-line
//!   `physical = ts >> 18` millisecond extraction inline.

use std::cmp::Ordering;

use chrono::{DateTime, Offset, TimeZone, Timelike};
use tidb_log::{Field, Value};

/// Go `ParseKey` rejected its input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseKeyError {
    /// The `format` argument was none of `raw`, `escaped`, `hex`.
    UnknownFormat,
    /// `hex.DecodeString` failed.
    InvalidHex(String),
    /// An escape sequence was truncated or malformed.
    InvalidEscape(String),
}

impl std::fmt::Display for ParseKeyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Go: errors.Annotate(berrors.ErrInvalidArgument, "unknown format").
            Self::UnknownFormat => write!(formatter, "unknown format: invalid argument"),
            Self::InvalidHex(message) | Self::InvalidEscape(message) => {
                write!(formatter, "{message}")
            }
        }
    }
}

impl std::error::Error for ParseKeyError {}

/// Go `ParseKey`: decodes `key` according to `format`.
///
/// `raw` takes the bytes as-is, `escaped` runs the PD-compatible unescaper, and
/// `hex` decodes a hex string.
pub fn parse_key(format: &str, key: &str) -> Result<Vec<u8>, ParseKeyError> {
    match format {
        "raw" => Ok(key.as_bytes().to_vec()),
        "escaped" => unescaped_key(key.as_bytes()),
        "hex" => decode_hex(key.as_bytes()),
        _ => Err(ParseKeyError::UnknownFormat),
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Go `encoding/hex.DecodeString`.
fn decode_hex(text: &[u8]) -> Result<Vec<u8>, ParseKeyError> {
    if !text.len().is_multiple_of(2) {
        return Err(ParseKeyError::InvalidHex(
            "encoding/hex: odd length hex string".to_owned(),
        ));
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    for pair in text.chunks_exact(2) {
        let (Some(high), Some(low)) = (hex_digit(pair[0]), hex_digit(pair[1])) else {
            return Err(ParseKeyError::InvalidHex(
                "encoding/hex: invalid byte".to_owned(),
            ));
        };
        out.push(high << 4 | low);
    }
    Ok(out)
}

/// Escape characters Go maps one-for-one, in the order of its two lookup
/// strings: `abfnrtv\'"` -> `\a\b\f\n\r\t\v\\'"`.
const ESCAPE_KEYS: &[u8] = br#"abfnrtv\'""#;
const ESCAPE_VALUES: &[u8] = b"\x07\x08\x0c\n\r\t\x0b\\'\"";

/// Go `unescapedKey`, itself a port of pd-ctl's region-command key parser.
///
/// The oddity worth naming: Go writes `fmt.Sscanf(..., "%02x", &c)` for `\x`
/// escapes and *discards* the error, so a `\x` with no hex digits after it
/// leaves `c` holding the backslash that was just read, and that backslash is
/// what lands in the output. The octal branch does check its error. Both
/// behaviors are reproduced.
fn unescaped_key(text: &[u8]) -> Result<Vec<u8>, ParseKeyError> {
    let mut buf = Vec::new();
    let mut cursor = 0usize;
    while cursor < text.len() {
        let mut current = text[cursor];
        cursor += 1;
        if current != b'\\' {
            buf.push(current);
            continue;
        }
        // Go: `n := r.Next(1)`; an empty read is `io.EOF`.
        if cursor >= text.len() {
            return Err(ParseKeyError::InvalidEscape("EOF".to_owned()));
        }
        let next = text[cursor];
        cursor += 1;
        if let Some(index) = ESCAPE_KEYS.iter().position(|candidate| *candidate == next) {
            buf.push(ESCAPE_VALUES[index]);
            continue;
        }
        if next == b'x' {
            // Go reads at most two more bytes and scans them as `%02x`.
            let end = (cursor + 2).min(text.len());
            let digits = &text[cursor..end];
            cursor = end;
            if let Some(value) = scan_radix(digits, 16) {
                current = value;
            }
            // On a scan failure Go leaves `c` untouched: it still holds `\`.
            buf.push(current);
            continue;
        }
        // Go: `n = append(n, r.Next(2)...)` then scans `n` as `%03o`.
        let end = (cursor + 2).min(text.len());
        let mut octal = vec![next];
        octal.extend_from_slice(&text[cursor..end]);
        cursor = end;
        let Some(value) = scan_radix(&octal, 8) else {
            return Err(ParseKeyError::InvalidEscape(format!(
                "expected integer, got {:?}",
                String::from_utf8_lossy(&octal)
            )));
        };
        buf.push(value);
    }
    Ok(buf)
}

/// Go `fmt.Sscanf` with a width-limited `%x`/`%o` verb into a `byte`: consume
/// the leading run of digits valid in `radix`, stopping at the first byte that
/// is not one. Returns `None` when there is no digit at all (Go's scan error).
fn scan_radix(digits: &[u8], radix: u32) -> Option<u8> {
    let mut value: u32 = 0;
    let mut seen = false;
    for byte in digits {
        let Some(digit) = (*byte as char).to_digit(radix) else {
            break;
        };
        value = value.wrapping_mul(radix).wrapping_add(digit);
        seen = true;
    }
    seen.then_some(value as u8)
}

/// Go `CompareEndKey`: compares two keys that BOTH represent the EXCLUSIVE end
/// of a range, where the empty key is the very end and so is greater than any
/// other key.
///
/// Not applicable when either argument is not an exclusive range end.
pub fn compare_end_key(a: &[u8], b: &[u8]) -> Ordering {
    if a.is_empty() {
        if b.is_empty() {
            return Ordering::Equal;
        }
        return Ordering::Greater;
    }
    if b.is_empty() {
        return Ordering::Less;
    }
    a.cmp(b)
}

/// Go `CompareBytesExt`: like `bytes.Compare`, but each side independently
/// declares whether its empty value should be read as positive infinity.
pub fn compare_bytes_ext(
    a: &[u8],
    a_empty_as_inf: bool,
    b: &[u8],
    b_empty_as_inf: bool,
) -> Ordering {
    let a_inf = a.is_empty() && a_empty_as_inf;
    let b_inf = b.is_empty() && b_empty_as_inf;
    if a_inf && b_inf {
        return Ordering::Equal;
    }
    if a_inf {
        return Ordering::Greater;
    }
    if b_inf {
        return Ordering::Less;
    }
    a.cmp(b)
}

/// Go `kv.KeyRange`: a half-open `[start_key, end_key)` interval where an empty
/// `end_key` means unbounded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive lower bound; empty means "from the very beginning".
    pub start_key: Vec<u8>,
    /// Exclusive upper bound; empty means "to the very end".
    pub end_key: Vec<u8>,
}

impl KeyRange {
    /// Builds a range from two byte slices.
    pub fn new(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> KeyRange {
        KeyRange {
            start_key: start_key.into(),
            end_key: end_key.into(),
        }
    }
}

/// Go `failedToClampReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClampResult {
    /// Go `successClamp`.
    Success,
    /// Go `leftNotOverlapped`: `ToClamp: |___|` sits right of `Range: |__|`.
    LeftNotOverlapped,
    /// Go `rightNotOverlapped`: `ToClamp: |___|` sits left of `Range: |__|`.
    RightNotOverlapped,
    /// Go `buggyUnknown`.
    BuggyUnknown,
}

/// Go `clampInOneRange`.
fn clamp_in_one_range(mut rng: KeyRange, clamp_in: &KeyRange) -> (KeyRange, ClampResult) {
    let mut reason = ClampResult::BuggyUnknown;
    if compare_bytes_ext(&rng.start_key, false, &clamp_in.start_key, false) == Ordering::Less {
        rng.start_key.clone_from(&clamp_in.start_key);
        reason = ClampResult::LeftNotOverlapped;
    }
    if compare_bytes_ext(&rng.end_key, true, &clamp_in.end_key, true) == Ordering::Greater {
        rng.end_key.clone_from(&clamp_in.end_key);
        reason = ClampResult::RightNotOverlapped;
    }
    // An empty region counts as "failed" too.
    if compare_bytes_ext(&rng.start_key, false, &rng.end_key, true) != Ordering::Less {
        return (KeyRange::default(), reason);
    }
    (rng, ClampResult::Success)
}

/// Go `IntersectAll`: the intersection of two sets of segments.
///
/// Treat a "set of segments" as a set of points that need not be adjacent; then
/// `intersect_all(s1, s2) = { point | point in both s1 and s2 }`. Both inputs
/// must already be sorted by start key with no overlaps (Go's `spans.Collapse`
/// produces that shape).
///
/// OWNERSHIP: for speed, Go mutates its first argument, and so does this — `s1`
/// is taken by `&mut` and its start keys are advanced in place. Clone it first
/// if you still need the original.
pub fn intersect_all(s1: &mut [KeyRange], s2: &[KeyRange]) -> Vec<KeyRange> {
    let mut current_clamping = 0usize;
    let mut current_clamp_target = 0usize;
    let mut rs = Vec::with_capacity(s1.len());
    while current_clamp_target < s2.len() && current_clamping < s1.len() {
        let cin = &s2[current_clamp_target];
        let crg = s1[current_clamping].clone();
        let (rng, result) = clamp_in_one_range(crg.clone(), cin);
        match result {
            ClampResult::Success => {
                rs.push(rng);
                if compare_bytes_ext(&crg.end_key, true, &cin.end_key, true) != Ordering::Greater {
                    current_clamping += 1;
                } else {
                    // The clamped range was not fully consumed.
                    s1[current_clamping].start_key.clone_from(&cin.end_key);
                }
            }
            ClampResult::LeftNotOverlapped => current_clamping += 1,
            ClampResult::RightNotOverlapped => current_clamp_target += 1,
            ClampResult::BuggyUnknown => {
                // boundary: Go `log.L().DPanic` + `br/pkg/logutil.StringifyKeys`
                // / `StringifyRange`. Unreachable; log instead of aborting.
                tidb_log::error(
                    "Unreachable path reached",
                    &[
                        Field::new("current-clamping", Value::Str(stringify_range(&crg))),
                        Field::new("current-target", Value::Str(stringify_range(cin))),
                    ],
                );
                break;
            }
        }
    }
    rs
}

/// boundary: `br/pkg/logutil.StringifyRange`, narrowed to a hex rendering.
fn stringify_range(rng: &KeyRange) -> String {
    format!(
        "[{}, {})",
        hex_string(&rng.start_key),
        hex_string(&rng.end_key)
    )
}

fn hex_string(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `DateFormat`: the layout BR renders backup/restore timestamps with.
pub const DATE_FORMAT: &str = "2006-01-02 15:04:05.999999999 -0700";

/// Go `FormatDate`: renders `ts` with [`DATE_FORMAT`].
///
/// Go's `.999999999` drops trailing zeros from the fraction and omits the
/// decimal point entirely when the fraction is zero; `-0700` is the numeric UTC
/// offset with no colon.
pub fn format_date<Tz: TimeZone>(ts: &DateTime<Tz>) -> String {
    let local = ts.naive_local();
    let mut out = local.format("%Y-%m-%d %H:%M:%S").to_string();

    let nanosecond = local.nanosecond() % 1_000_000_000;
    if nanosecond != 0 {
        let fraction = format!("{nanosecond:09}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }

    let offset_seconds = ts.offset().fix().local_minus_utc();
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let magnitude = offset_seconds.abs();
    out.push(' ');
    out.push(sign);
    out.push_str(&format!(
        "{:02}{:02}",
        magnitude / 3600,
        magnitude % 3600 / 60
    ));
    out
}

/// Go `IsMetaDBKey`.
pub fn is_meta_db_key(key: &[u8]) -> bool {
    key.starts_with(b"mDB")
}

/// Go `IsMetaDDLJobHistoryKey`.
pub fn is_meta_ddl_job_history_key(key: &[u8]) -> bool {
    key.starts_with(b"mDDLJobH")
}

/// Go `IsDBOrDDLJobHistoryKey`.
pub fn is_db_or_ddl_job_history_key(key: &[u8]) -> bool {
    key.starts_with(b"mD")
}

/// boundary: `pkg/tablecodec.metaPrefix`.
const META_PREFIX: &[u8] = b"m";
/// boundary: `pkg/structure.HashData`.
const HASH_DATA_FLAG: u64 = b'h' as u64;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xff;
const ENC_PAD: u8 = 0x00;

/// boundary: `pkg/util/codec.EncodeBytes` — memcomparable byte encoding, eight
/// bytes per group followed by a `0xFF - padCount` marker.
fn encode_bytes(mut b: Vec<u8>, data: &[u8]) -> Vec<u8> {
    let len = data.len();
    b.reserve((len / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1));
    let mut index = 0usize;
    while index <= len {
        let remain = len - index;
        let pad_count;
        if remain >= ENC_GROUP_SIZE {
            b.extend_from_slice(&data[index..index + ENC_GROUP_SIZE]);
            pad_count = 0;
        } else {
            pad_count = ENC_GROUP_SIZE - remain;
            b.extend_from_slice(&data[index..]);
            b.resize(b.len() + pad_count, ENC_PAD);
        }
        b.push(ENC_MARKER - pad_count as u8);
        index += ENC_GROUP_SIZE;
    }
    b
}

/// boundary: `pkg/util/codec.DecodeBytes` — returns `(leftover, decoded)`.
fn decode_bytes(mut b: &[u8]) -> Result<(&[u8], Vec<u8>), &'static str> {
    let mut buf = Vec::with_capacity(b.len());
    loop {
        if b.len() < ENC_GROUP_SIZE + 1 {
            return Err("insufficient bytes to decode value");
        }
        let group = &b[..ENC_GROUP_SIZE];
        let marker = b[ENC_GROUP_SIZE];
        let pad_count = ENC_MARKER - marker;
        if pad_count as usize > ENC_GROUP_SIZE {
            return Err("invalid marker byte");
        }
        let real_group_size = ENC_GROUP_SIZE - pad_count as usize;
        buf.extend_from_slice(&group[..real_group_size]);
        let padding = &group[real_group_size..];
        b = &b[ENC_GROUP_SIZE + 1..];
        if pad_count != 0 {
            if padding.iter().any(|byte| *byte != ENC_PAD) {
                return Err("invalid padding byte");
            }
            break;
        }
    }
    Ok((b, buf))
}

/// boundary: `pkg/util/codec.EncodeUint` — big-endian `u64`.
fn encode_uint(mut b: Vec<u8>, value: u64) -> Vec<u8> {
    b.extend_from_slice(&value.to_be_bytes());
    b
}

/// boundary: `pkg/util/codec.EncodeUintDesc` — big-endian `!value`, so the
/// encoding sorts descending.
fn encode_uint_desc(mut b: Vec<u8>, value: u64) -> Vec<u8> {
    b.extend_from_slice(&(!value).to_be_bytes());
    b
}

/// boundary: `pkg/util/codec.DecodeUint`.
fn decode_uint(b: &[u8]) -> Result<(&[u8], u64), &'static str> {
    if b.len() < 8 {
        return Err("insufficient bytes to decode value");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&b[..8]);
    Ok((&b[8..], u64::from_be_bytes(bytes)))
}

/// boundary: `pkg/tablecodec.EncodeMetaKey`.
fn encode_meta_key(key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut ek = META_PREFIX.to_vec();
    ek = encode_bytes(ek, key);
    ek = encode_uint(ek, HASH_DATA_FLAG);
    encode_bytes(ek, field)
}

/// boundary: `pkg/tablecodec.DecodeMetaKey` — returns `(key, field)`.
fn decode_meta_key(ek: &[u8]) -> Result<(Vec<u8>, Vec<u8>), &'static str> {
    if !ek.starts_with(META_PREFIX) {
        return Err("invalid encoded hash data key prefix");
    }
    let (rest, key) = decode_bytes(&ek[META_PREFIX.len()..])?;
    let (rest, flag) = decode_uint(rest)?;
    if flag != HASH_DATA_FLAG {
        return Err("invalid encoded hash data key flag");
    }
    let (_, field) = decode_bytes(rest)?;
    Ok((key, field))
}

/// Go `EncodeTxnMetaKey`: wraps a meta key/field pair into the MVCC txn key
/// layout — memcomparable-encoded meta key followed by the descending
/// timestamp.
pub fn encode_txn_meta_key(key: &[u8], field: &[u8], ts: u64) -> Vec<u8> {
    let k = encode_meta_key(key, field);
    let txn_key = encode_bytes(Vec::new(), &k);
    encode_uint_desc(txn_key, ts)
}

/// boundary: `pkg/meta.IsAutoIncrementIDKey` / `IsAutoTableIDKey` /
/// `IsAutoRandomTableIDKey` / `IsSequenceKey`.
const AUTO_ID_FIELD_PREFIXES: [&[u8]; 4] = [b"IID:", b"TID:", b"TARID:", b"SID:"];

/// Go `IsMetaAutoIDKey`: reports whether `key` is a txn meta key whose field is
/// one of the auto-ID counter types — auto-increment (`IID`), auto-table-id
/// (`TID`), auto-random (`TARID`), or sequence (`SID`).
///
/// These keys hold a single int64 counter that always fits in the WriteCF
/// shortValue payload — they have no DefaultCF cross-reference, so per-CF
/// deduplication by TS is safe for them.
pub fn is_meta_auto_id_key(key: &[u8]) -> bool {
    if key.len() < 8 {
        return false;
    }
    let Ok((_, meta_key_bytes)) = decode_bytes(&key[..key.len() - 8]) else {
        return false;
    };
    let Ok((_, field)) = decode_meta_key(&meta_key_bytes) else {
        return false;
    };
    AUTO_ID_FIELD_PREFIXES
        .iter()
        .any(|prefix| field.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestParseKey`.
    #[test]
    fn test_parse_key() {
        // test rawKey
        let raw_cases: [(&str, &[u8]); 6] = [
            ("1234", b"1234"),
            ("abcd", b"abcd"),
            ("1a2b", b"1a2b"),
            ("AA", b"AA"),
            ("\x07", b"\x07"),
            ("\\'", b"\\'"),
        ];
        for (raw, expected) in raw_cases {
            assert_eq!(parse_key("raw", raw).unwrap(), expected);
        }

        // test EscapedKey
        let escaped_cases: [(&str, &[u8]); 5] = [
            ("\\a\\x1", b"\x07\x01"),
            ("\\b\\f", b"\x08\x0c"),
            ("\\n\\r", b"\n\r"),
            ("\\t\\v", b"\t\x0b"),
            ("\\'", b"'"),
        ];
        for (escaped, expected) in escaped_cases {
            assert_eq!(parse_key("escaped", escaped).unwrap(), expected);
        }

        // test hexKey
        let hex_cases: [(&[u8], &[u8]); 8] = [
            (b"1234", b"1234"),
            (b"abcd", b"abcd"),
            (b"1a2b", b"1a2b"),
            (b"AA", b"AA"),
            (b"\x07", b"\x07"),
            (b"\\'", b"\\'"),
            (b"\x01", b"\x01"),
            (b"\xAA", b"\xAA"),
        ];
        for (plain, expected) in hex_cases {
            let key = hex_string(plain);
            assert_eq!(parse_key("hex", &key).unwrap(), expected);
        }

        // test other
        for (plain, _) in hex_cases {
            let error = parse_key("notSupport", &String::from_utf8_lossy(plain)).unwrap_err();
            assert!(
                error.to_string().starts_with("unknown format"),
                "{error} should start with `unknown format`"
            );
        }
    }

    /// Go `TestCompareEndKey`.
    #[test]
    fn test_compare_end_key() {
        let cases: [(&[u8], &[u8], Ordering); 6] = [
            (b"1", b"2", Ordering::Less),
            (b"1", b"1", Ordering::Equal),
            (b"2", b"1", Ordering::Greater),
            (b"1", b"", Ordering::Less),
            (b"", b"", Ordering::Equal),
            (b"", b"1", Ordering::Greater),
        ];
        for (key1, key2, expected) in cases {
            assert_eq!(compare_end_key(key1, key2), expected);
        }
    }

    fn assert_elements_match(actual: &[KeyRange], expected: &[KeyRange]) {
        let mut actual: Vec<&KeyRange> = actual.iter().collect();
        let mut expected: Vec<&KeyRange> = expected.iter().collect();
        let key = |rng: &&KeyRange| (rng.start_key.clone(), rng.end_key.clone());
        actual.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(actual, expected);
    }

    /// Go `TestClampKeyRanges`.
    #[test]
    fn test_clamp_key_ranges() {
        fn r(a: &str, b: &str) -> KeyRange {
            KeyRange::new(a.as_bytes(), b.as_bytes())
        }

        let cases: Vec<(Vec<KeyRange>, Vec<KeyRange>, Vec<KeyRange>)> = vec![
            (
                vec![r("0001", "0002"), r("0003", "0004"), r("0005", "0008")],
                vec![r("0001", "0004"), r("0006", "0008")],
                vec![r("0001", "0002"), r("0003", "0004"), r("0006", "0008")],
            ),
            (
                vec![r("0001", "0002"), r("00021", "0003"), r("0005", "0009")],
                vec![r("0001", "0004"), r("0005", "0008")],
                vec![r("0001", "0002"), r("00021", "0003"), r("0005", "0008")],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0152")],
                vec![r("0001", "0100"), r("0150", "0200")],
                vec![
                    r("0001", "0050"),
                    r("0051", "0095"),
                    r("0098", "0100"),
                    r("0150", "0152"),
                ],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0152")],
                vec![r("0001", "0100"), r("0150", "")],
                vec![
                    r("0001", "0050"),
                    r("0051", "0095"),
                    r("0098", "0100"),
                    r("0150", "0152"),
                ],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "")],
                vec![r("0001", "0100"), r("0150", "0200")],
                vec![
                    r("0001", "0050"),
                    r("0051", "0095"),
                    r("0098", "0100"),
                    r("0150", "0200"),
                ],
            ),
            (vec![r("", "0050")], vec![r("", "")], vec![r("", "0050")]),
        ];

        for (ranges, clamp_in, result) in cases {
            let mut lhs = ranges.clone();
            assert_elements_match(&intersect_all(&mut lhs, &clamp_in), &result);
            let mut rhs = clamp_in.clone();
            assert_elements_match(&intersect_all(&mut rhs, &ranges), &result);
        }
    }

    /// boundary: `client-go/oracle.GetTimeFromTS`, whose only use here is this
    /// fixture: the physical part of a TSO is its top 46 bits, in milliseconds.
    fn time_from_ts(ts: u64) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis((ts >> 18) as i64).expect("valid TSO")
    }

    /// Go `TestDateFormat`.
    #[test]
    fn test_date_format() {
        let cases = [
            (434604259287760897u64, "2022-07-15 19:14:39.534 +0800"),
            (434605479096221697, "2022-07-15 20:32:12.734 +0800"),
            (434605478903808000, "2022-07-15 20:32:12 +0800"),
        ];
        let time_zone = chrono_tz::Asia::Shanghai;
        for (ts, target) in cases {
            let date = format_date(&time_from_ts(ts).with_timezone(&time_zone));
            assert_eq!(date, target);
        }
    }

    /// Go `TestPrefix`.
    #[test]
    fn test_prefix() {
        assert!(is_meta_db_key(b"mDBs"));
        assert!(!is_meta_db_key(b"mDDL"));
        assert!(is_meta_ddl_job_history_key(b"mDDLJobHistory"));
        assert!(!is_meta_ddl_job_history_key(b"mDDL"));
        assert!(is_db_or_ddl_job_history_key(b"mDL"));
        assert!(is_db_or_ddl_job_history_key(b"mDB:"));
        assert!(is_db_or_ddl_job_history_key(b"mDDLHistory"));
        assert!(!is_db_or_ddl_job_history_key(b"DDL"));
    }

    /// Not in the Go file's test set: pins the narrowed codec helpers against
    /// the byte layouts `pkg/util/codec` documents, so the local mirrors cannot
    /// drift from `tidb-codec`.
    #[test]
    fn test_narrowed_codec_matches_go_layout() {
        assert_eq!(
            encode_bytes(Vec::new(), b""),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 247]
        );
        assert_eq!(
            encode_bytes(Vec::new(), b"\x01\x02\x03"),
            vec![1, 2, 3, 0, 0, 0, 0, 0, 250]
        );
        assert_eq!(
            encode_bytes(Vec::new(), b"\x01\x02\x03\x00"),
            vec![1, 2, 3, 0, 0, 0, 0, 0, 251]
        );
        assert_eq!(
            encode_bytes(Vec::new(), b"\x01\x02\x03\x04\x05\x06\x07\x08"),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
        );
        for payload in [&b""[..], b"\x01\x02\x03", b"12345678", b"123456789"] {
            let encoded = encode_bytes(Vec::new(), payload);
            let (leftover, decoded) = decode_bytes(&encoded).unwrap();
            assert!(leftover.is_empty());
            assert_eq!(decoded, payload);
        }
        assert_eq!(encode_uint(Vec::new(), 1), vec![0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(
            encode_uint_desc(Vec::new(), 1),
            vec![255, 255, 255, 255, 255, 255, 255, 254]
        );
    }

    /// Not in the Go file's test set: `EncodeTxnMetaKey` and `IsMetaAutoIDKey`
    /// are untested upstream, and they are the two functions whose dependencies
    /// were narrowed, so they get a round trip here.
    #[test]
    fn test_encode_txn_meta_key_round_trip() {
        let txn_key = encode_txn_meta_key(b"DB:1", b"TID:2", 42);
        assert!(is_meta_auto_id_key(&txn_key));

        for field in [&b"IID:2"[..], b"TID:2", b"TARID:2", b"SID:2"] {
            assert!(is_meta_auto_id_key(&encode_txn_meta_key(b"DB:1", field, 7)));
        }
        for field in [&b"Table:2"[..], b"DBs", b"ID"] {
            assert!(!is_meta_auto_id_key(&encode_txn_meta_key(
                b"DB:1", field, 7
            )));
        }
        assert!(!is_meta_auto_id_key(b"short"));
        assert!(!is_meta_auto_id_key(&[0u8; 16]));

        // The timestamp is stored descending: a later TS sorts first.
        let early = encode_txn_meta_key(b"DB:1", b"TID:2", 1);
        let late = encode_txn_meta_key(b"DB:1", b"TID:2", 2);
        assert!(late < early);
    }

    /// Not in the Go file's test set: `CompareBytesExt` is only exercised
    /// indirectly through `IntersectAll`, but it is public API.
    #[test]
    fn test_compare_bytes_ext() {
        assert_eq!(compare_bytes_ext(b"", true, b"", true), Ordering::Equal);
        assert_eq!(compare_bytes_ext(b"", true, b"z", true), Ordering::Greater);
        assert_eq!(compare_bytes_ext(b"z", true, b"", true), Ordering::Less);
        // Without the inf flag the empty key is just the smallest key.
        assert_eq!(compare_bytes_ext(b"", false, b"z", false), Ordering::Less);
        assert_eq!(compare_bytes_ext(b"", false, b"", false), Ordering::Equal);
        // Mixed: only the side that opted in becomes infinity.
        assert_eq!(compare_bytes_ext(b"", false, b"", true), Ordering::Less);
    }
}
