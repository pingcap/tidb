# Encoding divergence inventory: Go `pkg/tablecodec` + `pkg/util/codec` vs Rust `tidb-tablecodec` + `tidb-codec`

Scope:

| Go package | Rust crate |
| --- | --- |
| `pkg/util/codec` | `rust/crates/tidb-codec` |
| `pkg/tablecodec` | `rust/crates/tidb-tablecodec` |

Method: read both sides function by function and compare semantics, not names. A
claim only counts as a finding if it names the Go file and line, the Rust file
and line, and a concrete input that distinguishes the two.

Ranking:

1. **Corruption** — silent wrong bytes on encode, or the same bytes decoding to a
   different value. A Go node and a Rust node sharing one TiKV disagree about
   data.
2. **Accept/refuse asymmetry** — one side errors where the other succeeds.
3. **Diagnostic** — message or leftover-slice differences with no observable byte
   or value difference.

Line numbers on the Rust side are as of commit `a3b77f1b35` (the state audited);
two of the findings were fixed in `f4517b7a92` and are marked FIXED.

---

## Findings

### F1 — rank 1 — Common (clustered) handles were missing Go's 9-byte zero padding and its stop-at-zero column cut — FIXED

- Go: `pkg/kv/key.go:283-307` (`NewCommonHandle`), `pkg/kv/key.go:357-359`
  (`Encoded`), `pkg/tablecodec/tablecodec.go:106-116` (`EncodeRowKey`,
  `EncodeRowKeyWithHandle`), `pkg/tablecodec/tablecodec.go:330-343`
  (`DecodeRowKey`).
- Rust: `rust/crates/tidb-codec/src/table_key.rs:501-511` (`encode_handle`),
  `:493-499` (`validate_common_handle`), `:199-220` (`encoded_columns`).

Go's `NewCommonHandle` does two things the Rust copy did not:

1. If the encoded handle is shorter than 9 bytes it stores a **zero-padded**
   9-byte copy, and `Encoded()` returns that padded form. Every record key Go
   writes therefore reaches `RecordRowKeyLen` (19).
2. When splitting the handle into columns it **stops at the first zero byte**,
   treating it as padding rather than decoding it as a `NilFlag` datum.

**Distinguishing input.** A clustered table whose primary key is a single
`DECIMAL(1,0)` column holding `0`. `codec.EncodeKey` of that datum is 4 bytes:
`06 01 00 80` (decimal flag, precision 1, frac 0, and the one-byte binary body —
`DecimalBinSize(1,0) == 1`).

- Go writes `t <8-byte tableID> _r 06 01 00 80 00 00 00 00 00` — 20 bytes.
- Rust wrote `t <8-byte tableID> _r 06 01 00 80` — 15 bytes. Go's `DecodeRowKey`
  rejects that key outright, because `len(key) < RecordRowKeyLen`, and returns
  `invalid key`.
- In the other direction, Rust's `decode_record_key` on Go's 20-byte key saw a
  9-byte handle, failed the int-handle branch, and then decoded the five trailing
  zero bytes as five `Datum::Null` columns — one column in Go, six in Rust from
  identical bytes.

This was live, not theoretical: `rust/crates/tidb-exec/src/system_row_write.rs:213-225`
builds the record key from the **unpadded** `encoded` while building its
in-memory `Handle::Common` from `CommonHandle::new(encoded)`, which *does* pad
(`rust/crates/tidb-txnkv/src/handle.rs:146-171`). The key written and the handle
returned for the same row disagreed inside one Rust process.

**Fix applied.** `table_key.rs` now pads to `MIN_COMMON_HANDLE_LEN = 9` on encode
and stops at a zero byte in both `validate_common_handle` and
`encoded_columns`, matching `pkg/kv/key.go:283-307` exactly. The stop-at-zero
rule is safe because a clustered handle column is never NULL, so `NilFlag` never
legitimately appears at a datum boundary inside a common handle.

**Fixture status.** None covers this. `crates/tidb-codec/tests/table_key_source.rs`
and `table_row_key_source.rs` exercise only int handles and opaque handle bytes
of length ≥ 9. The fixture that would have caught it: a Go-generated
`EncodeRowKeyWithHandle` byte string for a single-column `DECIMAL(1,0)`
clustered primary key, asserted byte-for-byte, plus a `decode_record_key`
round-trip asserting one column.

### F2 — rank 2 — hash-key integer tag ignored the UNSIGNED flag — FIXED

- Go: `pkg/util/codec/codec.go:352-357` — `flag = uvarintFlag; if !mysql.HasUnsignedFlag(tp.GetFlag()) && row.GetInt64(idx) < 0 { flag = varintFlag }`.
- Rust: `rust/crates/tidb-codec/src/package.rs:180-188` (`encode_hash_datum`) —
  chose the tag from the sign alone.

**Distinguishing input.** Field type `BIGINT UNSIGNED`, value `18446744073709551615`
carried as `Datum::Int(-1)` (the chunk word). Go emits `09 ff ff ff ff ff ff ff ff`;
Rust emitted `08 ff ff ff ff ff ff ff ff`. Only the tag byte differs, but it is
the first byte fed to the hasher, so `HashChunkColumns` buckets differently and
`EqualChunkRow` reports "not equal" where Go reports "equal".

Reachable only when an unsigned column arrives as `Datum::Int`; a `Datum::UInt`
already took the correct branch, and `serialize_keys` (`package.rs:534-541`)
already checked the flag correctly — which is why the omission in
`encode_hash_datum` reads as an oversight rather than a decision.

**Fix applied.** `package.rs` now reproduces Go's condition verbatim.

**Fixture status.** None. A Go `EncodeHashChunkRowIdx` fixture over an unsigned
bigint column holding `2^64-1` would catch it.

### F3 — rank 2 — `peek` / `CutOne` accept malformed mem-comparable bytes in Go; Rust refuses

- Go: `pkg/util/codec/codec.go:1558-1575` (`peekBytes`). It reads the marker,
  breaks as soon as `padCount != 0`, and never checks `padCount > encGroupSize`
  nor validates the padding bytes. `DecodeBytes` (`pkg/util/codec/bytes.go:111-129`)
  does check both — the fast peek path does not.
- Rust: `rust/crates/tidb-codec/src/bytes.rs:87-110` (`peek_bytes_len`), reached
  from `datum.rs:468` (`peek_one_len` → `cut_one`). It validates both.

**Distinguishing inputs.**

- `01 00 00 00 00 00 00 00 00 00` — a bytes datum whose marker is `0x00`, so
  `padCount = 0xFF - 0x00 = 255 > 8`. Go's `CutOne` returns all 10 bytes with no
  error; Rust's `cut_one` returns `InvalidEncoding("invalid bytes marker")`.
- `01 01 02 03 04 05 06 07 08 FE` — marker `0xFE` means one pad byte, but the
  eighth group byte is `0x08`, not `0x00`. Go's `CutOne` accepts; Rust rejects
  with `InvalidEncoding("invalid bytes padding")`.

Risk direction: a Rust node refuses to skip over a value a Go node walks past.
Not corruption, but it turns a tolerated corrupt row into a hard scan failure.

### F4 — rank 2 — `peek` on a truncated varint returns a wrong length with no error in Go

- Go: `pkg/util/codec/codec.go:1592-1604` (`peekVarint`, `peekUvarint`). Both
  return `n` from `encoding/binary`, and only treat `n < 0` as an error. When the
  buffer runs out mid-varint `binary.Varint` returns `n == 0`, so these return
  `0, nil` and `peek` reports a total length of 1.
- Rust: `rust/crates/tidb-codec/src/datum.rs:474-481` propagates
  `CodecError::InsufficientBytes`.

**Distinguishing input.** `08 80` (varint flag followed by one continuation byte
and nothing else). Go's `CutOne` returns `data = [08]`, `remain = [80]`, no
error — the caller then re-enters `peek` on `[80]` and gets
`invalid encoded key flag 128`. Rust's `cut_one` fails immediately with
`InsufficientBytes`.

### F5 — rank 3 — `DecodeComparableVarint` returns a different leftover slice

- Go: `pkg/util/codec/number.go:260-263`. The single-byte fast path returns `b`,
  the slice **including** the tag byte, because `b = b[1:]` on line 264 comes
  after the early return. `DecodeComparableUvarint` (`:236-243`) advances first
  and does not have this shape.
- Rust: `rust/crates/tidb-codec/src/number.rs:174-176` returns the advanced
  remainder.

**Distinguishing input.** `[0x08, 0xff]` → Go returns `(remain = [08 ff], 0)`;
Rust returns `(remain = [ff], 0)`.

Encoded bytes are identical on both sides, so nothing on the wire diverges. Go's
only production caller — `lightning/pkg/importer/table_import.go:1778` and
`:1786` — discards the remainder. Rust's own test at
`crates/tidb-codec/src/tests/number.rs:109-114` decodes a *sequence* of
comparable varints, which Go could not do; that test documents the divergence
rather than the Go contract. Left as-is deliberately: matching Go here would
make Rust's sequence decoding wrong, and no persisted bytes depend on it.

### F6 — rank 2 — `decode_range` drops Go's `idxColumnTypes` schema hints

- Go: `pkg/util/codec/codec.go:1288-1320` (`DecodeRange`). When
  `idxColumnTypes != nil`, a DATE/DATETIME/TIMESTAMP column is decoded through
  `DecodeAsDateTime` and a FLOAT column through `DecodeAsFloat32`; everything
  else falls back to `DecodeOne`.
- Rust: `rust/crates/tidb-codec/src/datum.rs:427-452` (`decode_range`) always
  uses `decode_one` and takes no type argument.

**Distinguishing input.** An index range key holding one DATETIME column,
`04 <8-byte packed uint of 2000-01-01 00:00:00>`. Go with
`idxColumnTypes = [mysql.TypeDatetime]` yields a `KindMysqlTime` datum that
renders `2000-01-01 00:00:00`; Rust yields `Datum::UInt(1013821689802817536)`.
The same key with `mysql.TypeFloat` yields a float32-narrowed datum in Go and a
full-width `Datum::Real` in Rust.

The typed helpers exist — `decode_as_datetime` (`package.rs:107`) and
`decode_as_float32` (`package.rs:136`) — they are just never wired into
`decode_range`, so every caller has to know to bypass it. Written up rather than
fixed because it is an API-shape change with callers to update.

### F7 — rank 2 — `hash_group_key` clamps an unspecified decimal length to zero

- Go: `pkg/util/codec/codec.go:1826-1832` passes `ft.GetFlen()` and
  `ft.GetDecimal()` straight into `EncodeDecimal`.
  `pkg/util/codec/decimal.go:26-28` derives precision **only when it is exactly
  0**, and `MyDecimal.WriteBin` rejects a negative precision with `ErrBadNumber`.
- Rust: `rust/crates/tidb-codec/src/package.rs:395-400` passes
  `field_type.flen().max(0)` and `field_type.decimal().max(0)`, which turns
  `UnspecifiedLength` (`-1`) into `0` and therefore into the derive branch.

**Distinguishing input.** An `ETDecimal` column with `Flen = -1`, `Decimal = -1`,
value `1.5`. Go's `HashGroupKey` returns `ErrBadNumber` and aborts the whole
group-key batch; Rust emits `06 02 01 <derived body>` and continues.

Not fixed: the right behaviour is arguably Rust's, and choosing between
"reproduce Go's error" and "keep deriving" needs a decision about whether an
unspecified-length decimal can reach this path at all.

### F8 — rank 2 — `encode_mysql_time` converts the time zone in the UTC case where Go skips it

- Go: `pkg/util/codec/codec.go:215-223` — the conversion runs only when
  `tp == mysql.TypeTimestamp && loc != time.UTC`.
- Rust: `rust/crates/tidb-codec/src/package.rs:91-96` — converts for every
  `TimeType::Timestamp`, whatever the source zone.

Zero times are unaffected: both sides short-circuit
(`pkg/types/time.go:363-373`, `crates/tidb-datatype/src/mysql_time.rs:329-331`).
The divergence is a **non-normalizable** timestamp.

**Distinguishing input.** `2000-02-30 00:00:00` typed as `TypeTimestamp`, encoded
with `loc = time.UTC`. Go skips the conversion entirely and encodes
`ToPackedUint()` successfully. Rust calls `convert_time_zone(&Utc, &Utc)`, whose
`to_datetime` rejects day 30 of February, and returns
`CodecError::InvalidEncoding("invalid MySQL timestamp")`.

Not fixed: deciding "this generic `TZ` is UTC" needs a type-level or trait
change to `encode_mysql_time`'s signature, which is larger than a certain fix.

---

## Verified equal

Each item below was compared function by function, including the edge cases
named. These are the places the next reader does **not** need to look.

### `pkg/util/codec/bytes.go` ↔ `crates/tidb-codec/src/bytes.rs`

- `EncodeBytes` / `encode_bytes`: group size 8, pad byte `0x00`, marker
  `0xFF - padCount`. Both emit a **full trailing all-pad group** when the input
  length is an exact multiple of 8 (Go's loop condition `idx <= dLen`, Rust's
  `(0..=len).step_by(8)` plus the `padding != 0` break produce the same groups).
  Verified for `[]` → `00 00 00 00 00 00 00 00 F7`, `[1,2,3]` →
  `01 02 03 00 00 00 00 00 FA`, and `[1..8]` →
  `01..08 FF 00 00 00 00 00 00 00 00 F7`.
- `EncodeBytesDesc` / `encode_bytes_desc`: encode then bitwise-complement the
  appended region only. Go's `fastReverseBytes` word trick and Rust's byte loop
  produce identical output.
- `decodeBytes` / `decode_bytes_inner`: the `padCount > encGroupSize` rejection,
  the padding-byte validity check (`0x00` ascending, `0xFF` descending), and the
  descending complement all match. Go complements the whole buffer at the end;
  Rust complements per byte during append — same result.
- `EncodedBytesLength` / `encoded_bytes_len`: identical formula, including the
  `len % 8 == 0` case where both return `len + 8 + 1 + len/8`.
- `EncodeCompactBytes` / `DecodeCompactBytes`: same varint length prefix; both
  reject a negative declared length and a declared length past the buffer.
- Fixture: `crates/tidb-codec/tests/bytes_source.rs` already pins these vectors
  byte-for-byte, in both ascending and descending form.

### `pkg/util/codec/number.go` ↔ `crates/tidb-codec/src/number.rs`

- `EncodeIntToCmpUint` / `DecodeCmpUintToInt`: the `^ 0x8000000000000000` sign
  flip, big-endian layout, and the descending bitwise complement, for `int64` and
  `uint64`, ascending and descending. `i64::MIN` → `00 00 00 00 00 00 00 00`,
  `i64::MAX` → `FF FF FF FF FF FF FF FF` on both sides.
- `EncodeVarint` / `EncodeUvarint`: Go's zig-zag (`ux = uint64(x) << 1`, then
  complement when negative) and LEB128 reproduced exactly.
- `DecodeUvarint` vs Go `encoding/binary.Uvarint`: the 64-bit overflow rule
  matches in every case, including index 9 with a byte `> 1` (both error), index
  9 with byte `0x80` (both error, Go one iteration later), index 9 with byte `1`
  or `0` (both accept), and a buffer that ends mid-varint (both report
  insufficient).
- `EncodeComparableVarint` / `EncodeComparableUvarint` and both decoders: tag
  constants (`negativeTagEnd = 8`, `positiveTagStart = 247`), the single-byte
  value range `[0, 239]`, all eight length thresholds on each sign, the
  most-significant-byte trim, and both `errDecodeInvalid` overflow guards
  (`first > positiveTagStart && v > MaxInt64`, `first < negativeTagEnd && v <= MaxInt64`).
  Rust's `(1..8).find(|len| v >= -((1 << (len*8)) - 1))` is a faithful rewrite of
  Go's `if` ladder; checked at every boundary, e.g. `-255` → `07 01`, `-256` →
  `06 FF 00`.
- Fixture: `crates/tidb-codec/tests/number_boundaries.rs` pins the negative
  comparable-varint transitions against the Go generator at
  `rust/difftests/transaction-tests/fixtures/generate_number_boundaries.go`.
- Only F5 (leftover slice) differs.

### `pkg/util/codec/float.go` ↔ `crates/tidb-codec/src/float.rs`

- `encodeFloatToCmpUint64` / `decodeCmpUintToFloat` are identical, including all
  three cases a port usually gets wrong:
  - **Negative zero.** `-0.0 >= 0` is true in Go and `-0.0 >= 0.0` is true in
    Rust, so both take the `bits | signMask` branch and both `-0.0` and `+0.0`
    encode to `80 00 00 00 00 00 00 00`; both decode back to `+0.0`. There is no
    asymmetry and no separate `-0` normalization needed.
  - **NaN.** `NaN >= 0` is false on both sides, so both take the complement
    branch and produce the same bytes for the same NaN payload.
  - **±Infinity** round-trips identically, and `-Inf < 0 < +Inf` holds in the
    encoded byte order on both sides.
- Descending forms are the bitwise complement of the ascending forms on both
  sides.

### `pkg/util/codec/codec.go` datum layer ↔ `datum.rs` / `package.rs`

- Flag constants `NilFlag=0, bytes=1, compactBytes=2, int=3, uint=4, float=5,
  decimal=6, duration=7, varint=8, uvarint=9, json=10, vectorFloat32=20,
  max=250` — `codec.go:41-55` vs `datum.rs:30-54`. All equal, and
  `IntHandleFlag == intFlag == 3` is reproduced by `table_key.rs` reusing
  `INT_FLAG`.
- `EncodeKey` (comparable) vs `EncodeValue` (compact) dispatch per datum kind
  (`codec.go:109-169`, `264-298` vs `datum.rs:80-233`): which kinds take the
  fixed 8-byte form and which take varint/compact-bytes, `MinNotNull → bytesFlag`,
  `MaxValue → maxFlag`, enum/set/bit always going through the *unsigned* path in
  both, duration always `durationFlag + EncodeInt(nanoseconds)` in both (never
  the varint form), and time always `uintFlag + packed uint` in both.
- `DecodeOne` (`codec.go:1339-1419`) vs `decode_one`: every flag maps to the same
  datum kind, and `durationFlag` builds a duration at fsp 6 (`types.MaxFsp`) on
  both sides.
- `DecodeRange`'s trailing single byte (`codec.go:1307-1319`): `NilFlag` → empty
  datum, `bytesFlag` → `MinNotNull`, `maxFlag` and `maxFlag+1` (the `PrefixNext`
  byte) → `MaxValue`, anything else an error. Matches `datum.rs:442-450`
  including the `maxFlag + 1` case.
- `HashCode` (`codec.go:1907-1963`) vs `datum.rs:236-298`, including the two Go
  quirks a port tends to "clean up" and both of which Rust faithfully keeps:
  `KindMysqlTime` emits `uintFlag` **twice** (once literally, once from
  `encodeUnsignedInt(..., comparable=true)`), and `KindMysqlDecimal` emits
  `decimalFlag` followed by a *compact-bytes* rendering of the decimal's string
  form rather than its binary form. Also: `HashCode` for strings uses the **raw**
  bytes, not the collation key, because `encodeString` only applies the collator
  when `comparable` is true — Rust matches.
- `SerializeKeys` length prefixes (`codec.go:748-764`, `789-813`, `829-841` vs
  `package.rs:524-571`): decimal uses a **1-byte** `uint8` length under
  `KeepVarColumnLength`; strings, sets, JSON and non-int enums use a **4-byte
  little-endian** `uint32`. Bit and int-backed enum push `uintFlag` under
  `NeedSignFlag`. All match, including the asymmetry that decimal alone is
  1 byte.
- `valueSizeOfSignedInt` / `valueSizeOfUnsignedInt` (`codec.go:275-309` vs
  `package.rs:50-72`), including the `0 - v - 1` negation and the initial `>> 6`
  vs `>> 7` difference between the signed and unsigned forms.
- `ConvertByCollation` / `ConvertByCollationStr` (`codec.go:1868-1877`) map onto
  `package.rs:75-82`.

### `pkg/tablecodec/tablecodec.go` key layout ↔ `table_key.rs` / `row_index.rs`

- Prefix bytes and lengths: `t`, `_r`, `_i`, `m`; `idLen = 8`, `prefixLen = 11`,
  `RecordRowKeyLen = 19`, `TableSplitKeyLen = 9`. Table and index IDs both use
  `codec.EncodeInt` (sign-flipped big-endian), not the unsigned form.
- `GenTableRecordPrefix`, `GenTableIndexPrefix`, `GenTablePrefix`,
  `EncodeTablePrefix`, `EncodeTableIndexPrefix`, `EncodeIndexSeekKey`,
  `EncodeRowKey`, `CutRowKeyPrefix`, `CutIndexPrefix`, `DecodeIndexID`,
  `TruncateToRowKeyLen`, `GetTableHandleKeyRange` — all byte-identical.
  `DecodeIndexID` slices at `1 + 8 + 2 = 11` on both sides.
- `DecodeKeyHead` (`tablecodec.go:261-292`) vs `decode_key_head`: same order of
  checks — table prefix, then table ID, then `_r` short-circuits as a record key,
  then `_i` plus index ID, else invalid.
- `DecodeRecordKey` (`:143-176`) vs `decode_record_key`: `len(key) <= prefixLen`
  rejected, exactly-8 trailing bytes means an int handle, otherwise a common
  handle. `DecodeRowKey` (`:330-343`) vs `decode_row_key`: the
  `len < RecordRowKeyLen`, `key[0] == 't'`, `key[9:11] == "_r"` triple check, and
  the exactly-`RecordRowKeyLen` int-handle fast path.
- `EncodeRecordKey`'s partition rule (`:124-131`): a `PartitionHandle` replaces
  the record prefix with the *partition's* own record prefix and then appends the
  inner handle's encoding. `encode_record_key` reproduces this, including that
  the partition ID — not the table ID — ends up in the key.
- Meta keys (`:221-256`): `m` + `EncodeBytes(key)` + `EncodeUint('h')` +
  `EncodeBytes(field)`, the `structure.HashData` flag check on decode, and the
  fact that `DecodeMetaKey` ignores anything after the encoded field.
- `rowindexcodec.GetKeyKind` vs `row_index.rs:41-51`: the minimal
  `len >= 11 && key[0] == 't'` then `key[9..11]` classification, with no
  validation of anything else.
- `IsRecordKey` / `IsIndexKey` / `IsTableKey` predicates and their exact index
  arithmetic (`k[10] == 'r'`, `k[10] == 'i'`, `len(k) == 9`).
- Fixtures already covering this area: `crates/tidb-codec/tests/table_key_source.rs`
  (11 source-row tests, including `non_unique_int_index_key_matches_go_gen_index_key`
  and `non_unique_index_value_is_the_single_zero_byte`),
  `table_row_key_source.rs`, and the 1020-line
  `crates/tidb-tablecodec/tests/tablecodec_package_source.rs`.

---

## Not verifiable in this environment

This machine cannot execute freshly built binaries: `syspolicyd` is wedged and
every newly created executable hangs in `_dyld_start`. `cargo check` and
`cargo clippy` are the only gates that ran, and they ran clean on the two changed
files. Nothing in this document was confirmed by running a test, a fixture, a Go
program, `gorun`, or `goeval`. Every claim — including the two applied fixes — is
derived from reading both sources.

Specifically unverified:

- Neither fix was exercised by a test. `crates/tidb-codec/tests/table_key_source.rs`
  and `table_row_key_source.rs` were not run after the `encode_handle` change,
  so it is not proven that no existing assertion depended on the unpadded form.
- No fixture was generated from Go for any input named in this document.
- The byte strings quoted above were derived by hand from the two
  implementations, not captured from a running node.
