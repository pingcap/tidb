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

**Counts: 27 findings — 10 rank 1, 13 rank 2, 4 rank 3 (the rank-3 entries F20,
F27 and parts of F10 group several small items each). Two fixed. Verified-equal
inventory covers the byte codec, the number codec, the float codec, the datum
layer, the key layout, the decimal payload codec, row format v2, and the index
key/value layout.**

The three worst:

1. **F13** — row format v2 writes the decimal's natural precision/scale instead
   of the column's declared `(flen, decimal)`. `DECIMAL(10,4)` holding `11.99`
   is 7 bytes in Go and 5 bytes in Rust. Every decimal column in every v2 row.
2. **F9** — a negative zero with non-zero scale round-trips as `-0` through Go's
   `FromBin` and is normalized to `+0` by Rust, changing both the rendered value
   and the re-encoded mem-comparable bytes (`04 04 7F FF` vs `04 04 80 00`) —
   and therefore the sort position of an index key.
3. **F1** — common (clustered) handles were missing Go's 9-byte zero padding, so
   Rust wrote record keys Go's `DecodeRowKey` rejects outright, and read Go's
   padded keys as extra NULL handle columns. Fixed.

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

### F9 — rank 1 — a negative zero with non-zero scale survives Go's `FromBin` and cannot exist in Rust

- Go: `pkg/types/mydecimal.go:1522` (`d.negative = mask != 0`, unconditional),
  and the zero reset at `:1574-1576` which fires only when
  `digitsInt == 0 && digitsFrac == 0`. On the encode side `WriteBin`'s
  sign-suppression guard at `:1326-1329` is `digitsIntFrom + fracSizeFrom == 0`,
  where `fracSizeFrom` is a **byte** size, so any value with `digitsFrac > 0`
  keeps `mask = -1` even when every word is zero.
- Rust: `crates/tidb-datatype/src/decimal.rs:96-98` — `new_with_storage` sets
  `negative: negative && !is_zero`, and every construction, including
  `from_bin`'s `to_decimal()` (`:1725-1730`), funnels through it. A negative zero
  cannot be represented.

**Distinguishing input (decode).** The 4 bytes `04 04 7F FF` fed to
`codec.DecodeDecimal` / `decode_decimal`. Precision 4, scale 4, `binSize = 2`,
`mask = -1`, one zero word, `digitsInt = 0`, `digitsFrac = 4`.

- Go: `negative = true`, and `String()`'s `Round(4, ModeHalfUp)` takes the
  `frac >= digitsFrac` early return (`:859-863`) which never touches `negative`,
  so `ToString` emits **`-0.0000`**.
- Rust: `new_with_storage(true, "0000", 4, 4)` → `negative = false` → **`0.0000`**.

**Distinguishing input (re-encode).** Re-encoding the value decoded above.

- Go keeps `mask = -1`, writes `FF FF`, then `bin[0] ^= 0x80` → **`04 04 7F FF`**
  (a fixed point).
- Rust has `mask = 0`, writes `00 00` → **`04 04 80 00`**.

These are mem-comparable index-key bytes: Go orders `7F FF` strictly **before**
`80 00`, so a value written by another producer and rewritten through the Rust
path silently changes both its byte identity and its sort position.

Producer reachability: Go's own arithmetic cannot mint this state — `DecimalNeg`
guards `IsZero()` (`:1682-1689`), `Round`'s zero path clears `negative` (`:972`),
and `FromString` normalizes (`:533-542`). It is reachable through `FromBin`,
i.e. bytes arriving from TiKV, an older TiDB, or a MySQL-format producer, and
through `UnmarshalJSON`. The same divergence therefore applies to
`Decimal::from_mysql_json_value` (`crates/tidb-datatype/src/decimal.rs:2086-2101`).

**Fixture status.** Not covered, and the existing fixtures cannot catch it:
`crates/tidb-codec/tests/decimal_go_vectors.rs:28` includes `"-0.0"` and
`"-0.0000"`, but those go through the parse path where Go normalizes too, so
both sides agree by construction. The fixture that would catch it must start
from **bytes**: feed `04 04 7F FF` to `decode_decimal` and assert the rendered
string and the re-encoded bytes.

### F10 — rank 2 — decimal header shapes Go accepts (or crashes on) and Rust refuses

Rust routes every decimal header through `validate_decimal_shape`
(`crates/tidb-codec/src/decimal.rs:201-210`), which rejects `precision == 0`,
`precision > 81`, `scale > 30`, and `scale > precision`. Go has no equivalent
gate, so four header shapes behave differently:

- **`scale > 30` on the wire.** Go `FromBin` (`pkg/types/mydecimal.go:1476`) has
  no scale ceiling; only `binSize > 40` gates it (`:1504`). Input: header
  `28 23` (precision 40, scale 35) plus 19 payload bytes → Go returns a decimal
  with `digitsFrac = 35` and no error; Rust returns `DecimalOutOfRange`. No
  current TiDB writer emits this, because `EncodeDecimal` clamps `frac` to 30
  (`pkg/util/codec/decimal.go:29-31`) — this is a foreign/legacy-producer
  refusal.
- **`precision == 0`.** Input `00 00 00`. Go's `DecimalPeak`
  (`mydecimal.go:2467-2478`) returns 2, so `codec.peek` / `SetRawValues` cut a
  3-byte "decimal" and move on; Go's `DecodeDecimal` instead **panics** at
  `mydecimal.go:1510` (`dCopy[0] ^= 0x80` on an empty slice). Rust rejects in
  both paths.
- **`scale > precision` where `precision - scale` is a negative multiple of 9.**
  `DecimalBinSize` only rejects `xInt < 0`, and `xInt` is 0 here. Input: header
  `01 0A` (precision 1, scale 10) → `binSize = 1`; `DecimalPeak` returns 3 and
  `peek` accepts, then `FromBin` calls `readWord(bin[0:], 4)` on a 1-byte copy
  and **panics**. Rust rejects.
- **Truncated payload.** Go's `DecodeDecimal` does `b = b[binSize:]` *before*
  inspecting `err` (`pkg/util/codec/decimal.go:63-64`), and `FromBin` tolerates a
  short input by zero-filling. Input `14 00 80` (precision 20, scale 0 →
  `binSize = 9`, one payload byte) → Go panics slicing `b[9:]`; Rust returns
  `InsufficientBytes` (`crates/tidb-codec/src/decimal.rs:152-154`).

One encode-side case in the same family: `EncodeDecimal(b, d, 40, 31)` clamps
`frac` to 30 and encodes (`pkg/util/codec/decimal.go:29-31`), while
`encode_decimal_fixed(buf, d, 40, 31)` returns `DecimalOutOfRange` and writes
nothing (`crates/tidb-codec/src/decimal.rs:186-189`). Low reachability from
current callers, since `Datum.Frac()` is wire-sourced and already ≤ 30.

### F11 — rank 3 — `valueSizeOfDecimal` under-reports by one byte in Go; Rust is exact

- Go: `pkg/util/codec/decimal.go:37-46` omits the `frac > MaxDecimalScale` clamp
  that `EncodeDecimal` applies six lines above it, so Go's size estimate
  disagrees with Go's own encoder.
- Rust: `decimal_encoded_len` shares `decimal_shape` with the encoder.

**Distinguishing input.** `0.` followed by forty `1` digits (`digitsFrac = 40`,
reachable in production from `DecimalDiv`, which keeps whole base-1e9 fraction
words). `PrecisionAndFrac()` is `(40, 40)`. Go's estimate is
`2 + DecimalBinSize(40,40) = 20`; Go's actual `EncodeDecimal` output is
`2 + WriteBin(40,30) = 21`; Rust reports and writes 21.

Flagged rather than "fixed": this is a Go bug, and anything sizing a buffer off
Go's `EstimateValueSize` is already one byte short.

### F12 — rank 1 (latent) — `to_bin` keeps the *leading* 81 integer digits where Go keeps the *trailing* 81

- Go: `pkg/types/mydecimal.go:456-475` — `FromString` starts its scan at the end
  of the integer part, so an over-wide value keeps its **low** 81 digits.
- Rust: `crates/tidb-datatype/src/decimal.rs:1607` reassigns
  `digits_int = words_int * 9` and `:1619` then seeds `si = digits_int`, so the
  right-to-left scan walks `digits[0..81]` — the **leading** 81 digits.

**Distinguishing input.** `Decimal::from_literal("1" + "0"*81)` (82 integer
digits) → `to_bin(81, 0)` encodes 1e80. The Go-parsed equivalent holds the low 81
digits, i.e. zero.

Latent, not live: `Decimal::parse_mysql` pre-clamps to the last 81 digits exactly
like Go (`decimal.rs:202-203`). It is reachable via `from_literal` and via
`mul` / `add`, which do not clamp coefficient width. If such a value ever reaches
`to_bin` it is silent wrong bytes, which is why it is ranked 1 despite the
reachability caveat.

### F13 — rank 1 — row format v2 writes the decimal's *natural* precision/scale, not the column's declared shape

- Go: `pkg/util/rowcodec/encoder.go:213-214` —
  `codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(), d.Frac())`, and
  `pkg/util/codec/decimal.go:25-35` uses those verbatim unless `precision == 0`.
- Rust: `crates/tidb-codec/src/rowcodec.rs:364-367` —
  `let (precision, scale) = value.precision_and_frac();` and then
  `encode_decimal_fixed(..., precision, scale)`. The declared shape is not merely
  ignored: `tidb-datatype/src/datum/mod.rs:231` defines
  `Datum::Decimal(Decimal)` with no length/frac field, so it cannot be carried.

This is the common path, not a corner. `pkg/types/datum.go:1573-1575`
(`convertToMysqlDecimal`) does `ret.SetLength(target.GetFlen())` and
`ret.SetFrac(target.GetDecimal())`, so **every** decimal datum that goes through
`CastValue` on the INSERT/UPDATE path carries the column's declared shape, and
`codec.DecodeOne` (`codec.go:1377-1387`) re-establishes it on decode.

**Distinguishing input.** Column `c DECIMAL(10,4)`, column ID 1, value `11.99`.

- Go writes payload `0A 04` + `WriteBin(10,4)` = 5 bytes → a 7-byte value and a
  row offset of `07 00`.
- Rust writes payload `06 04` + `WriteBin(6,4)` = 3 bytes → a 5-byte value and a
  row offset of `05 00`.

Different length and different bytes for the identical logical row. Downstream:
the v2 raw-handle checksum and the TiCDC column checksums are computed over these
bytes and so disagree with TiDB's; Go's `DatumMapDecoder` reading a Rust-written
row recovers `Length = 6` instead of `10`. Live Rust call sites:
`crates/tidb-tablecodec/src/table_row.rs:246,248`,
`crates/tidb-tablecodec/src/table_index.rs:803,896`,
`crates/tidb-exec/src/system_row_write.rs`.

**Fixture status.** Not covered, and structurally *cannot* be covered by the
current suite — see the fixture note at the end of this section. One
Go-generated hex vector for a `DECIMAL(10,4)` row would catch it immediately.

### F14 — rank 1 — `decode_row_to_map` rounds decimals where Go's `DatumMapDecoder` does not

- Go: `pkg/util/rowcodec/decoder.go:127-134` — the `DatumMapDecoder`'s
  `TypeNewDecimal` branch sets the decimal and its `Length`/`Frac` and performs
  **no rounding**. Only the *chunk* decoder rounds
  (`decoder.go:315-328`, when `frac > col.Ft.GetDecimal()`).
- Rust: `crates/tidb-codec/src/rowcodec.rs:711-718` has one shared
  `decode_column_datum` that always rounds, used by both `decode_row_to_map`
  (`:394`) and `decode_row_to_datums` (`:435`).

**Distinguishing input.** The small row
`80 00 01 00 00 00 01 04 00 | 04 02 <WriteBin(4,2) of 11.99>` decoded with
`ColumnInfo { id: 1, field_type: NewDecimal.with_decimal(1) }`. Go's
`DecodeToDatumMap` yields `11.99` with `Length = 4`, `Frac = 2`; Rust's
`decode_row_to_map` yields `12.0`. Rust also has nowhere to put the recovered
precision/frac, so that part of the round trip is lost for every decimal.

### F15 — rank 1 — common-handle decode is missing Go's `NeedRestoredData` guard, so Rust returns a collation sort key as the column value

- Go: `pkg/util/rowcodec/decoder.go:277-285` — inside the common-handle loop,
  `if types.NeedRestoredData(col.Ft) { return false }` **before** decoding the
  handle column, so the value is taken from the row's restored-data column
  instead.
- Rust: `crates/tidb-codec/src/rowcodec.rs:655-668` — the `Handle::Common` arm
  has no such check. The sibling old-bytes path at `:610-612` *does* check it,
  which is what marks this as an omission rather than a decision.

**Distinguishing input.**
`CREATE TABLE t(name VARCHAR(10) COLLATE utf8mb4_general_ci, PRIMARY KEY(name) CLUSTERED)`
with row value `"AbC"`. `NeedRestoredData` is true, the row value does not carry
column `name`, and the handle is the mem-comparable key of `"AbC"`.

- Go: `tryAppendHandleColumn` returns false, the column falls through to
  default/NULL, and the real value comes from the restored-data column — `AbC`.
- Rust: decodes the handle bytes and yields the **case-folded collation sort
  key** as the string.

The same applies to the truncated prefix-index case Go calls out at
`decoder.go:244-246`.

### F16 — rank 1 — two handle-resolution predicates differ

**F16a: `is_pk_handle` short-circuits the integer-handle path.** Go
`decoder.go:273` is `if handle.IsInt() && col.ID == decoder.handleColIDs[0]` —
`col.IsPKHandle` is deliberately not consulted in the chunk decoder (only in the
*bytes* decoder, `decoder.go:456`). Rust `rowcodec.rs:646-647` adds
`column.is_pk_handle ||`.

Input: `columns = [ColumnInfo { id: 5, is_pk_handle: true, field_type: LongLong }]`,
`handle_column_ids = [1]`, `handle = Handle::Int(7)`, the row does not contain
column 5, `defaults = [Datum::Int(99)]`. Go yields `99`; Rust yields `7`.

**F16b: an explicitly-NULL column never consults the handle in Rust.** Go
`decoder.go:234-254` and `:413-430` call `tryAppendHandleColumn` /
`tryDecodeHandle` **before** the `isNil` branch, for both the not-found and the
explicitly-NULL cases. Rust `rowcodec.rs:441` and `:556` let NULL win
immediately and consult the handle only when the column is `Missing`.

Input: row `80 00 00 00 01 00 | 01` (0 not-null, 1 null, null ID `01`),
`handle_column_ids = [1]`, `handle = Handle::Int(7)`, column
`{ id: 1, LongLong }`. Go's `DecodeToChunk` yields `7`; Rust's
`decode_row_to_datums` yields `NULL`.

### F17 — rank 1 (low reachability) — negative column IDs under payload-triggered large promotion

- Go: `encoder.go:81` sets `rowFlagLarge` only from `colID > 255`; the
  size-triggered promotion at `:152-162` widens the **already byte-truncated**
  IDs (`r.colIDs32[j] = uint32(r.colIDs[j])`), and the partition was sorted by
  `smallNotNullSorter` (`common.go:166-168`) as bytes.
- Rust: `row_encoder.rs:110` decides `large` from the payload up front, then
  sorts (`:116-117`) and writes (`:131`) with `*id as u32`.

Input: `columns = [{ id: -1, value: 66_000 bytes }, { id: 3, value: b"z" }]`
(payload > 65535). Go emits IDs `[3, 255]`; Rust emits `[3, 4294967295]` —
different bytes and a different ID ordering. No negative ID is stored in a row
value today, so this is latent, but it is a concrete byte divergence in a shared
encoder API.

### F18 — rank 1 — BIT / binary-literal overflow: Go refuses, Rust silently writes `0xFF…FF`

- Go: `encoder.go:203-210` —
  `val, err = d.GetBinaryLiteral().ToInt(types.StrictContext); if err != nil { return }`.
  `ToInt` (`pkg/types/binary_literal.go:110-114`) returns `math.MaxUint64`
  **and** an error past 8 significant bytes, and `StrictContext`
  (`pkg/types/context.go:269`) propagates it, so `Encoder.Encode` returns
  `nil, err`.
- Rust: `rowcodec.rs:360-362` → `binary_literal_to_uint` (`:783-796`) returns
  `u64::MAX` with no error.

Input: a `Datum::Bit(BinaryLiteral)` whose bytes are
`01 00 00 00 00 00 00 00 00` (9 bytes, leading byte non-zero). Go returns
`ErrTruncatedWrongVal` and writes nothing; Rust writes the payload
`FF FF FF FF FF FF FF FF` into the row. (The *checksum* path is consistent on
both sides — Go `common.go:334-336` uses `DefaultStmtNoWarningContext` and keeps
`MaxUint64`, matching `rowcodec.rs:882-887`.)

### F19 — rank 2 — row v2 accept/refuse asymmetries

| Case | Go | Rust | Distinguishing input |
| --- | --- | --- | --- |
| Bytes-decoder handle predicate | `decoder.go:456`: `col.IsPKHandle \|\| col.ID == ExtraHandleID (-1)` | `rowcodec.rs:614-615`: `is_pk_handle \|\| handle_column_ids.first() == Some(&id)` | `{id: -1, is_pk_handle: false}`, `handle_column_ids = [3]`, `Handle::Int(9)` → Go emits `IntFlag + EncodeInt(9)`, Rust emits `[NilFlag]`. Reverse: `{id: 3, is_pk_handle: false}`, `handle_column_ids = [3]` → Go **panics** in `IntHandle.EncodedCol` (`pkg/kv/key.go:251-253`), Rust returns `None`. |
| Trailing bytes | `row.go:118-172` ignores them | `rowcodec.rs:385,415,489,541` require an empty remainder | any valid v2 row plus one `0x00` |
| Odd integer width | `common.go:106-147` default branch reads 8 bytes for any other length | `row_decoder.rs:128-170` errors outside {1,2,4,8} | a `LongLong` column with a 9-byte payload |
| Column-count overflow | `row.go:174-188` silently wraps `numNotNullCols`/`numNullCols` at `uint16` (per partition) | `row_encoder.rs:86-90` errors past 65535 **total** | 40 000 not-null + 40 000 null columns; or 70 000 null-only (Go wraps to 4464) |
| Decreasing offsets | `row.go:98-116` trusts them, then panics slicing | `row_layout.rs:209-214` typed `InvalidOffset` | small row, `not_null_count = 2`, offsets `05 00, 03 00` |
| Corrupt packed time | `Time.FromPackedUint` never validates | `rowcodec.rs:725-727` → `from_date_checked` bounds-checks | only corrupt values; zero dates pass on both sides |
| Unknown field type | `decoder.go:511-544` **panics** | `rowcodec.rs:941` returns `UnknownFieldType` | `FieldTypeCode::NewDate` (`0x0E`), `Geometry` |
| ID/value length mismatch | `encoder.go:74-78` panics | `rowcodec.rs:299-304` errors | `column_ids.len() != values.len()` |
| Missing/duplicate output offset | `decoder.go:401-446` silently yields 0 and lets duplicates overwrite | `rowcodec.rs:546-577` errors | `output_offsets` missing a requested column ID |
| Empty slice | `common.go:226-228` `IsNewFormat` panics | `row_layout.rs:324-326` returns false | `&[]` |

### F20 — rank 3 — row v2 diagnostic-only differences

- **Checksum buffer prefix.** Go `encoder.go:258-260` CRCs the value bytes
  *including any caller-supplied prefix already in `buf`* (documented at
  `encoder.go:42`); Rust `rowcodec.rs:322-325` CRCs only `&buffer[start..]`.
  Unreachable: the sole production caller, `tablecodec.go:371`, does
  `valBuf = valBuf[:0]` first.
- **`CalculateRawChecksum` aliasing.** Go `row.go:319-321` writes into `r.data`,
  which `fromBytes` aliases directly into the caller's buffer — Go mutates its
  input in place. Rust `rowcodec.rs:501-518` copies. Same checksum.
- **Duplicate column IDs.** Go uses unstable `sort.Sort` per partition, Rust
  `sort_unstable_by_key`; with duplicate IDs the value↔ID pairing can differ. Go
  itself is unspecified here.
- **Duration fsp.** Go `decoder.go:152-153` assigns `col.Ft.GetDecimal()` raw
  (can be `-1`); Rust `rowcodec.rs:737-740` runs it through `check_fsp`
  (`-1 → 0`). Not observable in rendered output.
- **BIT byte size.** Go `decoder.go:169` `(Flen+7)>>3` panics inside
  `NewBinaryLiteralFromUint` for a result of 0 or > 8; Rust `rowcodec.rs:751-753`
  clamps and returns a typed error. Identical arithmetic for `flen >= 0`.
- **Endianness.** Go `common.go:61-87` reinterprets raw bytes as
  `[]uint16`/`[]uint32` via `unsafe.Slice` (host order); Rust reads and writes
  explicit little-endian. Identical on every supported platform.
- **`need_restored_data`.** `crates/tidb-datatype/src/field_type/mod.rs:915-928`
  omits Go's `ft.GetCollate() != "utf8mb4_0900_bin"` clause
  (`pkg/types/etc.go:147-154`), so a `VARCHAR … COLLATE utf8mb4_0900_bin` is
  classified as needing restored data in Rust but not in Go. Outside this
  package, but it feeds F15 and the first row of F19.

### F21 — rank 1 — bin-collation trailing-space restore uses the compact form where Go uses the mem-comparable form

- Go: `pkg/tablecodec/tablecodec.go:899-903` — `newResults[i]` is rebuilt as
  `rowcodec.BytesFlag (0x01)` followed by `codec.EncodeBytes(padded)`, i.e. the
  **mem-comparable** form, so the entry matches the other elements of the array
  (which came from `CutIndexKeyNew`).
- Rust: `crates/tidb-tablecodec/src/table_index.rs:1008-1011` calls
  `tidb_codec::encode_value(...)`, and `datum.rs:192-195` emits
  `COMPACT_BYTES_FLAG (0x02)` plus compact bytes.

**Distinguishing input.** Clustered-v1 table, index column
`b CHAR(10) COLLATE utf8mb4_bin`, stored value `"ab  "` (two trailing spaces),
restored padding count 2, so `results[i]` from the key is
`01 61 62 00 00 00 00 00 00 f9`.

- Go: `01 61 62 20 20 00 00 00 00 fb`
- Rust: `02 08 61 62 20 20`

Both are self-describing, so `DecodeOne` recovers the same string, but the bytes
and the length differ and the array stops being representation-homogeneous.

### F22 — rank 1 — prefix-index truncation counts invalid UTF-8 differently

- Go: `tablecodec.go:1845-1851` — `utf8.RuneCount(colValue)` then
  `bytes.Runes(colValue)`. Go's `DecodeRune` yields `(RuneError, 1)` per
  **byte**.
- Rust: `crates/tidb-tablecodec/src/table_index.rs:636-642` —
  `String::from_utf8_lossy(bytes)` then `.chars()`. Rust applies the Unicode
  **maximal-subpart** rule, so one `U+FFFD` can stand for 2-3 bytes.

**Distinguishing input.** A non-binary, non-ASCII charset column with index
prefix length 2 and stored bytes `E0 A0 41`.

- Go: `RuneCount == 3` (`0xE0` → RuneError, `0xA0` → RuneError, `A`), `3 > 2`, so
  it truncates to two runes → `EF BF BD EF BF BD` (6 bytes).
- Rust: `from_utf8_lossy` yields `"\u{FFFD}A"` because `E0 A0` is one maximal
  subpart, `chars().count() == 2 <= 2`, so **no truncation** → `E0 A0 41`
  (3 bytes).

Reachable via a `latin1` column (`0xE0` = à, `0xA0` = NBSP, `0x41` = A — latin1
bytes ≥ 0x80 are invalid UTF-8 and TiDB stores them raw) or via `utf8mb4` with
`tidb_skip_utf8_check = 1`. Different index key bytes for the same row.

**Fixture status.** `crates/tidb-tablecodec/tests/tablecodec_package_source.rs:783-793`
asserts Rust's own lossy semantics rather than a Go vector, so it *pins* the
divergence instead of catching it.

### F23 — rank 1 (conditional) — the rune-truncation branch drops Go's Bytes→String conversion

- Go: `tablecodec.go:1850` — the rune branch always ends with
  `v.SetString(truncateStr, tblCol.GetCollate())`, turning a `KindBytes` datum
  into a collation-carrying `KindString`.
- Rust: `table_index.rs:645-652` — `Datum::Bytes(_) => Datum::new_bytes(...)`
  keeps it as `Bytes` with `Collation::Binary`.

The consequence is in `encode_key` (`datum.rs:117-124`): a `Datum::String` goes
through `string_key()` and becomes a collation sort key, while a `Datum::Bytes`
goes in raw. For a `utf8mb4_bin` column the bin collator trims trailing spaces,
so `Datum::Bytes(b"abc   ")` on a `c(8)` prefix index encodes
`01 61 62 63 20 20 20 00 fc` in Rust versus the space-trimmed sort key in Go.
Conditional on whether the write path can hand a `Bytes` datum to a non-binary
column; the branch itself is unconditionally divergent, and
`tablecodec_package_source.rs:784-793` again asserts the Rust behaviour.

### F24 — rank 2 — `decode_restored_values_v5` errors on every partially-restored index

- Go: `tablecodec.go:870-877` builds `colIDOffsets` over **all** columns but
  decodes with only `colInfosNeedRestore`. `pkg/util/rowcodec/decoder.go:401-406`
  allocates `values := make([][]byte, len(outputOffset))` and iterates only
  `decoder.columns`, deliberately leaving the non-restored slots **nil** — which
  is exactly what `noRestoreData := len(newResults[i]) == 0` (`:878-882`) tests.
- Rust: `table_index.rs:976-989` calls `decode_row_to_old_bytes`, and
  `crates/tidb-codec/src/rowcodec.rs:570-576` returns
  `RowPackageError::InvalidOutputOffset` if **any** output offset went unwritten.
  Whenever `restore_columns.len() < offsets.len()` that is guaranteed.

**Distinguishing input.**
`CREATE TABLE t (a VARCHAR(10) COLLATE utf8mb4_general_ci, b INT, PRIMARY KEY(a) CLUSTERED, KEY idx(b));`
Reading `idx` gives `columns_len = 1` (column `b`, LongLong, needs no restored
data) with handle restored data present for `a`. Go returns `results` unchanged;
Rust returns `Err(InvalidOutputOffset(0))`. Both call sites are affected:
`table_index.rs:1048-1053` (index columns) and `:1077-1082` (handle columns).
This is exactly the v5 space optimisation Go documents at
`tablecodec.go:1636-1638`.

Note this is the same Rust behaviour flagged as an accept/refuse asymmetry in the
row-v2 table (`rowcodec.rs:546-577`); here it is not merely stricter, it breaks
the normal clustered-v1 read path.

### F25 — rank 2 — the v0-extensible common-handle segment is re-encoded with the wrong helper

- Go: `tablecodec.go:2025` — `decodeIndexKvGeneral` uses
  `decodeHandleInIndexKey(segs.CommonHandle)`, which at `:1077-1079` returns a
  **plain `IntHandle`** when the segment decodes to exactly one `KindInt64`
  datum; `reEncodeHandle` (`:823-828`) then emits `codec.EncodeValue(IntDatum)`.
- Rust: `table_index.rs:1063-1064` calls `common_handle(encoded)`, always
  producing a `CommonHandle`, and `encoded_handle_columns` returns the raw column
  slice.

**Distinguishing input.** Index value
`00 7f 00 09 03 80 00 00 00 00 00 07 e6` — 13 bytes: `tailLen = 0`,
`CommonHandleFlag`, length 9, handle = `IntFlag + EncodeInt(2022)`. This is a
`CommonHandleVersion == 0` clustered table whose single PK column is a type that
`isSingleIntPKFromTableInfo` (`pkg/ddl/create_table.go:1725`) rejects — `YEAR` is
the clean example, since only `TypeLong/Longlong/Tiny/Short/Int24` set
`PKIsHandle`.

- Go result element: `08 ec 1f` (varint flag + zig-zag 2022)
- Rust result element: `03 80 00 00 00 00 00 07 e6`

Same decoded value, different bytes and length. The V1 twin is **not** divergent:
Go `:1969-1975` also uses `NewCommonHandle` there, and Rust's
`index_value_version(value) == 1 && !handle.is_int()` guard (`:1068`)
reproduces `reEncodeHandleConsiderNewCollation`.

### F26 — rank 2 — a handle-less extensible index value is refused

Go `tablecodec.go:1084-1105` leaves `handle` nil when neither the common-handle
nor the int-handle segment is present and still returns successfully (or a
`PartitionHandle` wrapping nil). Rust `table_index.rs:572` returns
`Err("index value has no handle")`.

**Distinguishing input.** `00 7e 80 00 00 00 00 00 00 2a` — 10 bytes: a global
non-unique v0 value, `tailLen = 0`, partition 42. Go returns
`PartitionHandle{42, nil}`; Rust errors.

### F27 — rank 3 — index-layer diagnostics

- **Segment precedence.** Go `:1089-1097` lets a `CommonHandle` segment
  overwrite an already-decoded `IntHandle`; Rust `:567-572` uses
  `if int … else if common`. Unreachable: `genIndexValueVersion0`
  (`:1750-1782`) makes the two mutually exclusive, and the padding path can never
  produce `tailLen >= 8`.
- **Partition threshold.** Go `:1030` gates on `len(value) >= 9`; Rust `:590` on
  `>= 8`. For `len == 8` the splitter yields `partition_id: None` anyway, so the
  outcomes agree.
- **Truncation arity.** Go `:1817-1821` ranges over `indexedValues` and tolerates
  a short slice; Rust `:662-667` requires exact equality and returns
  `Metadata("index values and columns count mismatch")`. No in-tree Go caller
  actually passes a short slice.
- **Truncation charset source.** Go `:1835` reads `tblCol.GetCharset()`; Rust
  `:633` reads `field_type.collation().charset()`, bypassing
  `FieldType::charset()` (`field_type/mod.rs:696-698`) which prefers the explicit
  charset name. Every registered collation maps back to its own charset, so no
  divergent input was constructed — but it reads the wrong field and would drift
  if the metadata were ever inconsistent.
- **Panic vs error.** Rust returns typed errors where Go slice-indexes and
  panics: `cut_index_key` on a key under 19 bytes (`:236-238` vs `:1016`),
  `split_extensible_index_value` on `len < header + tailLen` (`:520-522` vs
  `:1907-1909`), `is_temp_index_key` on a short key (`:319` vs `:1315`),
  `decode_int_handle_in_index_value` on fewer than 8 bytes (`:398` vs `:1110`).
  Also `table_index.rs:1068`: on a v1 value whose handle is an int, Go panics in
  `IntHandle.NumCols()` (`pkg/kv/key.go:246`) while Rust silently takes the
  re-encode branch.
- **Unknown index version byte.** Go `:2061-2066` re-splits an unrecognized
  version with the header-1 V0 layout; Rust `:610` routes anything non-zero to
  header-3. No version 2 exists today.

**Structural note.** F21, F24 and F25 all live in `decode_index_kv` and its
helpers, which have **no callers and no tests anywhere in the Rust workspace**.
The write side (`generate_index_key` / `generate_index_value`) and the segment
splitters are in good shape; the read side has never been run.

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

### `pkg/types/mydecimal.go` payload codec ↔ `crates/tidb-datatype/src/decimal.rs`

- **Word split and the leading-digit boundary.** `WriteBin`'s `digitsInt/9` full
  words plus `dig2bytes[leadingDigits]` head plus `dig2bytes[trailingDigits]`
  tail (`mydecimal.go:1304-1314`, `:1371-1401`) is line-for-line identical to
  `to_bin` (`decimal.rs:1775-1885`), including the
  `stop = wordIdxFrom + wordsIntFrom + wordsFracFrom` snapshot taken *after* the
  leading-word increment and the
  `for trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i`
  regrouping loop. No off-by-one in the leading-word split.
- **Sign transform.** `mask = -1` negative / `0` positive, XORed into each word
  before `writeWord`, then `bin[0] ^= 0x80` exactly once
  (`mydecimal.go:1301-1303`, `:1410` vs `decimal.rs:1773`, `:1895`). Pad bytes
  are `byte(mask)` → `0xFF` for negative on both sides. Decode: `mask = 0` iff
  `bin[0]&0x80 > 0` (`:1497-1499` vs `:1927`).
- **`writeWord` / `readWord`** at all four sizes, including the `int8`
  sign-extension in sizes 1/2/4 and the explicit `0xFF000000` branch in size 3
  (`:1594-1631` vs `:1499-1545`).
- **`DecimalBinSize` for every legal `(precision, frac)`**, including
  `precision % 9 == 0` and `frac % 9 == 0` (`dig2bytes[0] == 0`).
- **Truncation and overflow still write bytes.** `encode_decimal_fixed` pushes
  precision, scale and the full payload into the buffer *before* returning
  `DecimalTruncated` / `DecimalOverflow` (`crates/tidb-codec/src/decimal.rs:109-116`),
  matching Go returning the appended `b` alongside `ErrTruncated` / `ErrOverflow`.
  The soft/hard split is preserved (`DecimalCodecWarning` vs `DecimalCodecError`).
- **Natural precision derivation** when `precision == 0`: `PrecisionAndFrac` plus
  `removeLeadingZeros` (`:1453-1461`, `:289-303`) vs `decimal_shape`
  (`crates/tidb-codec/src/decimal.rs:191-198`), including the `precision == 0 → 1`
  floor and the un-clamped precision beside the 30-clamped scale. Rust's
  normalized coefficient drops an all-zero leading integer word that Go keeps,
  but `remove_leading_zeros` returns a correspondingly shifted
  `(word_idx, digits_int)` pair, so the emitted bytes are identical — checked on
  `0`, `0.0`, `0.5`, `00.5`, `0012.5` and the 40-digit-fraction case.
- **`FromBin` corruption checks.** The leading-word `>= powers10[leadingDigits+1]`
  test and the per-word `> wordMax` test accept and reject the same inputs. Go's
  `uint64(int32)` sign-extends and Rust's `as u32` zero-extends, but any negative
  word lands above the threshold either way (`:1532`, `:1544`, `:1557`, `:1568`
  vs `:1971`, `:1986`, `:2000`, `:2012`).
- **`fixWordCntError`** and its `FromBin` consequence branches, including the
  `binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize` skip
  (`:1513-1521` vs `:1943-1956`).
- **`resultFrac` vs the derived scale.** Go keeps `resultFrac = frac` while
  `digitsFrac` may be smaller after the `fixWordCntError` truncation branch;
  Rust carries only the derived `digits_frac`. Provably not observable:
  `String()`'s `Round(resultFrac, ModeHalfUp)` (`:277-282`) re-applies the
  identical `wordsInt + wordsFracTo > wordBufLen` clamp at `:834-838` and lands
  back on exactly `digitsFrac`. Checked on `(precision=81, frac=30)`, where both
  print 27 fraction digits.
- **No descending or inverted decimal variant exists on either side** —
  `codec.encode` writes decimals identically whether or not `comparable1` is set
  (`codec.go:134-136`), so there is nothing to diverge from.

### Row format v2 (`pkg/util/rowcodec` ↔ `rowcodec.rs` / `row_encoder.rs` / `row_decoder.rs` / `row_layout.rs`)

- **Header.** Version byte `128`, flag byte (`rowFlagLarge = 1`,
  `rowFlagChecksum = 2`), then `numNotNullCols` and `numNullCols` as `u16` LE;
  header length 6 (`row.go:27-29`, `:174-178` vs `row_layout.rs:28`, `:51-70`,
  `row_encoder.rs:124-127`).
- **Large/small threshold.** `> 255` on the column ID (`encoder.go:81`) and
  `> math.MaxUint16` on the cumulative payload (`encoder.go:152`), boundary
  confirmed at exactly 65535 small / 65536 large on both sides — the case Go's
  `Test65535Bug` pins. (Except the negative-ID interaction in F17.)
- **ID and offset widths.** Small = 1-byte IDs plus 2-byte LE offsets; large =
  4-byte IDs plus 4-byte LE offsets. Offsets are **cumulative end** offsets, with
  index 0 starting at 0 (`row.go:98-147`, `:179-185` vs
  `row_layout.rs:106-122`, `:191-217`, `:299-313`).
- **ID ordering.** Two independent ascending partitions, not-null first, compared
  as **unsigned**; lookup is a binary search of the not-null partition and then
  the null partition, with a negative lookup ID resolving to "not found" on both
  sides (`common.go:149-209`, `row.go:190-234` vs `row_encoder.rs:115-121`,
  `row_layout.rs:274-286`).
- **Per-value integer encoding.** Shortest-of-1/2/4/8-byte little-endian
  two's-complement, and signed and unsigned select widths differently from each
  other in exactly the same way on both sides — e.g. `128` is 2 bytes `80 00`
  signed and 1 byte `80` unsigned (`common.go:89-134` vs
  `row_encoder.rs:167-194`). Explicitly **not** the mem-comparable form on either
  side.
- **Per-type value encoding** (`encoder.go:174-227` vs `rowcodec.rs:330-375`):
  int/uint compact LE; string and bytes raw with no length prefix; float via
  `codec.EncodeFloat`, i.e. 8-byte **big-endian** with the sign transform, NaN
  and `-0.0` handled identically; time as `ToPackedUint` then compact uint, with
  the time-zone conversion only for `TypeTimestamp`; duration as a compact
  **signed** nanosecond count; enum and set as a compact uint of `.Value`; bit
  and binary literal as a compact uint of the big-endian literal (value matches;
  overflow does not — F18); JSON as `TypeCode` byte plus `Value`; vector-float32
  via `SerializeTo`; `MinNotNull` / `MaxValue` / raw unsupported on both sides.
  Decimal is the exception (F13).
- **NULL handling.** A NULL column lands in the null-ID partition, contributes
  **zero** value bytes and **no** offset entry (`encoder.go:105-122` vs
  `row_encoder.rs:96-106`).
- **Missing-column / default resolution order.** commit-TS pseudo-column →
  virtual generated → row-checksum pseudo-column → present-and-not-null → handle
  → NULL → default → NULL (`decoder.go:213-266` vs `rowcodec.rs:420-461`). Every
  step matches except the handle-vs-NULL ordering and the handle predicates
  (F15, F16). Missing + nullable → NULL, missing + default → default, missing +
  NOT NULL with no default → NULL, on both the datum path and the old-bytes path
  (the non-empty default filter matches `decoder.go:436-440`).
  `DatumMapDecoder` omits missing columns from the map entirely on both sides.
- **Checksum framing.** `checksumMaskVersion = 0b0111`,
  `checksumFlagExtra = 0b1000`, versions `{0,1,2}` accepted. Encode sets the flag
  on the header byte *before* hashing, appends the header byte, CRC32-IEEE over
  `row‖header`, extends with `Handle.Encoded()`, then 4 bytes LE
  (`encoder.go:253-264` vs `rowcodec.rs:319-326`). `IntHandle.Encoded()` is the
  8-byte mem-comparable `EncodeInt` on both sides. The
  `CalculateRawChecksum` v1-uses-key / non-v1-uses-handle branch matches. The
  column-level TiCDC checksum (`common.go:295-352` vs `rowcodec.rs:799-909`)
  matches type for type, including the `Inf`/`NaN` → 0 float transform and the
  `Null`/`Geometry` no-ops.
- **Old-datum conversion.** All 15 branches of `fieldType2Flag`
  (`decoder.go:511-544` vs `rowcodec.rs:913-943`), and `encodeOldDatum`
  (`decoder.go:491-508` vs `rowcodec.rs:581-603`): bytes → compact-bytes flag,
  int → varint flag, uint → uvarint flag, everything else flag plus raw.
- **Handle pseudo-column IDs.** `ExtraHandleID = -1`, `ExtraPhysTblID = -3`,
  `ExtraRowChecksumID = -4`, `ExtraCommitTSID = -5`; `commitTS == 0 → NULL`;
  virtual generated column → NULL checked before lookup.
- **v1-vs-v2 selection.** Purely `rowcodec.Encoder.Enable` on write
  (`tablecodec.go:365-374`) and `rowcodec.IsNewFormat(b)` on the leading byte on
  read (`tablecodec.go:552`), mirrored at
  `crates/tidb-tablecodec/src/table_row.rs:246-248`. Go's `flatten`
  (`tablecodec.go:403-437`) is applied only on the v1 path and is exactly the
  flattening v2 performs inline, consistently on both sides.
- **Row-key helpers.** `IsRowKey`, `IsNewFormat`, and all four
  `RemoveKeyspacePrefix` guard conditions (`common.go:221-228`, `:356-374` vs
  `row_layout.rs:324-336`, `rowcodec.rs:947-962`).

### Index key and value layout (`pkg/tablecodec` ↔ `crates/tidb-tablecodec/src/table_index.rs`)

The four layout flags are **not** what one would guess from the byte values.
Go `tablecodec.go:68-78`: `CommonHandleFlag = 0x7f`, `PartitionIDFlag = 0x7e`,
`IndexVersionFlag = 0x7d`, `RestoreDataFlag = rowcodec.CodecVer = 0x80`. Rust
`table_index.rs:88-94` has all four exactly right, along with
`MaxOldEncodeValueLen = 9`, `UnCommitIndexKVFlag = '1' (0x31)` and
`IntHandleFlag = 3`.

- **Index key prefix.** `'t' + EncodeInt(tableID) + "_i" + EncodeInt(indexID)`,
  `PREFIX_LEN = 11`, index values start at offset 19
  (`tablecodec.go:719-725`, `:1138-1143`).
- **Old-vs-new dispatch is by length and byte 0, not by a last-byte or parity
  trick.** `len(value) <= 9` selects the legacy layout
  (`tablecodec.go:1003-1010` vs `table_index.rs:1029`, `:1045-1047`).
  `getIndexVersion`'s predicate is
  `len > 9 && tailLen ∈ {0,1} && v[1] == 0x7d → v[2]`
  (`:971-980` vs `index_value_version`, `:484-494`).
- **Segment splitters.** V0-extensible header 1, V1 header 3, `tail = v[len-tailLen:]`,
  int handle `= tail[:8]` when `len(tail) >= 8` (`:1906-1948` vs
  `split_extensible_index_value`, `:507-559`).
- **Common-handle segment** `0x7f + u16 BE length + bytes`, on both write and
  read (`:1868-1874`, `:1913-1917` vs `:383-388`, `:529-545`).
- **Partition segment** `0x7e + EncodeInt(partitionID)`, read as `v[1:9]`
  (`:1876-1880`, `:1919-1922` vs `:390-393`, `:546-554`).
- **Padding rule.** `else if len(idxVal) < 10 { tailLen += 10 - len; append zeros }`,
  then `if untouched { tailLen++; push 0x31 }`, then `idxVal[0] = tailLen`
  (`:1778-1791` vs `:810-820`).
- **Legacy value.** An 8-byte **raw big-endian** handle when distinct (note:
  *not* sign-flipped — `EncodeHandleInUniqueIndexValue` uses
  `binary.BigEndian.PutUint64`, and `DecodeIntHandleInIndexValue` reinterprets as
  `i64`), otherwise the single byte `'0'` (0x30), plus `0x31` when untouched
  (`:1793-1811`, `:1855-1866`, `:1110` vs `:371-381`, `:396-406`, `:822-833`).
- **V1 header** `[tailLen, 0x7d, 0x01]`, with `tailLen` only ever 0 or 1
  (`:1682-1732` vs `:850`, `:898-901`).
- **`GenIndexKey`.** `distinct` is computed **before** truncation; the
  `PartitionIDFlag + partitionID` appears in the *key* only when
  `!distinct && globalIndexVersion >= 1`; the suffix is
  `IntHandleFlag + EncodeInt(h)` for an int handle and `h.Encoded()` otherwise
  (`:1235-1289` vs `:679-721`).
- **`IndexKVIsUnique`** three-branch predicate (`:2057-2067` vs `:606-617`) and
  **`IsUntouchedIndexKValue`** including the `{1, 4, 9}` length set and the
  `tailLen < 8` / `tailLen == 9` split (`:1174-1194` vs `:348-367`).
- **Temp-index handling.** `IsTempIndexKey`, the temp-index ID mask/prefix round
  trip, and `TempIndexValueElem.Encode/DecodeOne` including `EncodeUint`
  (unflipped) for int handles, the `'p'` + 8-byte partition segment, and the
  `u16` BE lengths (`:1292-1319`, `:1413-1541` vs `:288-326`, `:1197-1305`).
- **`decodeRestoredValues` (V4).** Go's `handleColIDs = []int64{-1}` is dead code
  because `DecodeToBytesNoHandle` passes `handle = nil`; Rust passing `&[]`
  (`:936-953`) is equivalent.
- **`TryGetCommonPkColumnRestoredIds`** and **`buildRestoredColumn`**
  (bin collation → unsigned LongLong) (`:1652-1670`, `:917-939` vs `:907-922`,
  `:955-974`).
- **`collate.IsBinCollation`** member set — the same six names, including
  `utf8mb4_0900_bin` (`pkg/util/collate/collate.go:356-360` vs
  `collation.rs:301-306`).

---

## Fixture coverage

Where a byte-level Go fixture already exists, the area above is marked verified.
Where one does not, the finding names the fixture that would catch it. The two
structural gaps worth stating on their own:

- **No fixture pins row-format-v2 bytes against Go-produced output.**
  `crates/tidb-codec/tests/rowcodec_package_source.rs` has 15 tests, all
  self-round-trips; its one Go hex vector (line 63) is a keyspace *key*, not a
  row. `row_encoder_source.rs:23-46` does pin `encode_raw_int` /
  `encode_raw_uint` byte for byte — genuinely useful — but `:49-119` verifies the
  row layout only by feeding it back into `RowLayout::parse`.
  `row_layout_source.rs` builds its expectations with local `put_u16` / `put_u32`
  helpers. Because encode and decode are each other's inverse, every one of these
  round-trip tests passes while producing bytes TiDB never writes: one
  Go-generated hex vector for a `DECIMAL(10,4)` row would have caught F13
  immediately.
- **No fixture exists for `decode_index_kv`, `decode_restored_values*`, or
  `generate_index_value`'s byte output.** `table_key_source.rs:103-155` covers
  four hard-coded non-unique int index keys and the `0x30` value;
  `tablecodec_package_source.rs` covers `IsUntouchedIndexKValue`, the temp-index
  codec, `decode_index_handle` with a partition, the global-index NULL key, and
  V1 uniqueness. The index *read* path is untested and, per F24, uncalled.

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
