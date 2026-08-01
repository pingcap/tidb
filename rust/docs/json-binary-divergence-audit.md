# JSON surface: `pkg/types/json_*.go` vs `tidb-datatype` binary JSON

TiDB's JSON is a binary format that is written into rows and index keys, not a
text blob. A divergence here is bytes a Go node misreads, persisted. That is why
this surface is audited on the same principle as the row encoding.

Go sources read:

- `pkg/types/json_constants.go` — type codes, layout constants, precedence table
- `pkg/types/json_binary.go` — grammar comment, encode/decode, text form, hash
- `pkg/types/json_binary_functions.go` — comparison, extract, modify, merge
- `pkg/types/json_path_expr.go` — path parsing

Rust sources read:

- `rust/crates/tidb-datatype/src/binary_json.rs`
- `rust/crates/tidb-datatype/src/binary_json_ops.rs`
- `rust/crates/tidb-datatype/src/json_path.rs`

**Nothing here was executed.** This machine cannot run freshly built binaries
(`syspolicyd` is wedged; every new executable hangs at `_dyld_start`). Every
finding is derived by reading both sides. `cargo check` and `cargo clippy` are
the only gates that ran; the Go side was never run at all.

**The JSON type is not a stub.** ~4150 Rust lines in `tidb-datatype` against
~3350 Go lines of production code, and every exported Go entry point in
`json_binary_functions.go` has a Rust counterpart. The findings below are real
divergences inside a working implementation, not gaps.

Counts: **8 divergences found** (1 rank-1, 1 rank-2, 3 rank-3, 3 rank-4),
**3 fixed** in this branch, **5 written up**. **27 areas verified equal.**

---

## Structural note read before the findings

There are two JSON implementations in the Rust tree, not one.

- `tidb-datatype::BinaryJSON` — the binary format. This is what
  `Datum::String -> Json` conversion goes through
  (`rust/crates/tidb-datatype/src/datum_convert.rs:645-648`), so it is what a
  `CAST(... AS JSON)` or an `INSERT` into a JSON column persists.
- `tidb-expr/src/builtin_ext/json/` — ~4300 lines working on `serde_json::Value`
  (aliased `Json`), which is what the live SQL builtins evaluate
  (`rust/crates/tidb-expr/src/builtin_ext/json/report.rs:89` decides
  `JSON_TYPE`'s answer from `Json::Number(n).is_u64()`, never from a type code).

That split matters for reading the ranks below: a `tidb-datatype` divergence in
a *function* may not be user-visible today because the expression layer does not
call it, while a `tidb-datatype` divergence in the *format* is on the persisted
path regardless. Finding 1 is on the persisted path. This audit did not edit
`tidb-expr`; the second implementation belongs to another unit, but it is worth
saying plainly that two JSON representations is itself the largest standing risk
on this surface.

---

## Rank 1 — different bytes for the same document

### 1. A JSON number past `i64::MAX` was stored as a DOUBLE instead of UNSIGNED INTEGER  — FIXED

- Go: `pkg/types/json_binary.go:894-911` (`appendBinaryNumber`). Order is
  int64, then `strconv.ParseUint(string(x), 10, 64)`, then float64. The
  in-source comment is explicit: *"Then uint64 (valid in MySQL JSON, not in JSON
  decode library)"*. `json_binary.go:552-553` uses `decoder.UseNumber()`, so the
  raw number text reaches `appendBinaryNumber` intact.
- Rust (before the fix): `rust/crates/tidb-datatype/src/binary_json.rs:458-463`,
  `normalize_parsed_numbers`, rewrote every parsed number in
  `(i64::MAX, u64::MAX]` to an `f64` *before* encoding. `encode_number`'s own
  `as_u64()` branch (`binary_json.rs:1123`) was therefore dead for every
  text-parsed document.

Distinguishing input: `CAST('18446744073709551615' AS JSON)`.

| | type code | payload bytes | text round-trip | `JSON_TYPE` |
| --- | --- | --- | --- | --- |
| Go | `0x0a` (UINT64) | `ff ff ff ff ff ff ff ff` | `18446744073709551615` | `UNSIGNED INTEGER` |
| Rust (before) | `0x0b` (FLOAT64) | `00 00 00 00 00 00 f0 43` | `1.8446744073709552e19` | `DOUBLE` |

Nine payload bytes differ for a document a Go node can write and a Rust node
must read back identically. Fixed by deleting `normalize_parsed_numbers` and its
call site.

Caveat worth recording: `pkg/types/json_binary_test.go:141-142` carries a
comment claiming *"we can't parse '9223372036854775808' to JSON::Uint64 now,
because go builtin JSON parser treats that as DOUBLE"*, and the test only
exercises `CreateBinaryJSON(uint64(1<<63))`, never the text path. That comment
is stale relative to the `ParseUint` fallback now in `appendBinaryNumber` — it
is the likely origin of the Rust behavior. The code, not the comment, is the
source of truth, and the code returns `JSONTypeCodeUint64`. This is the one
finding here that a single `SELECT JSON_TYPE('18446744073709551615')` against a
real `tidb-server` would settle in a second, and it has not been run.

---

## Rank 2 — different comparison answer, therefore different row order

### 2. Two doubles compared with the integer-widening epsilon  — FIXED

- Go: `pkg/types/json_binary_functions.go:824-832`. `FLOAT64` against `FLOAT64`
  dispatches to `compareFloat64` (`:756`), which is exact (`x < y`, `x == y`).
  The `1e-8` slack in `compareFloat64PrecisionLoss` (`:737`, `floatEpsilon` at
  `:733`) is reached *only* from `compareFloat64Int64` and
  `compareFloat64Uint64` (`:783`, `:787`) — i.e. only when one side is an
  integer that had to be widened to a double.
- Rust (before the fix): `binary_json.rs:873-879` applied the epsilon to the
  whole catch-all arm, which is entered for double-vs-double as well as
  double-vs-integer.

Distinguishing input: `CAST('1.0' AS JSON)` against `CAST('1.000000001' AS JSON)`.

| | answer |
| --- | --- |
| Go | `Less` (difference `1e-9` is a real difference between two doubles) |
| Rust (before) | `Equal` |

This is `ORDER BY` on a JSON column and JSON index ordering, so it is wrong row
order with no error and no warning. Fixed by splitting the arm: when neither
side yields an `i64` or a `u64` (which is exactly the FLOAT64-vs-FLOAT64 case,
since a binary INT64/UINT64 always decodes to a `serde_json` integer variant),
compare exactly; otherwise keep the epsilon.

---

## Rank 3 — wrong value from a function

### 3. An array selection on a non-array autowrapped too eagerly  — FIXED

- Go: `pkg/types/json_binary_functions.go:303-322`. When the current value is
  *not* an array, the rule is stated on the selection itself, and the comment
  says so: *"If the current object is not an array, still append them if the
  selection includes 0 or last. But for asterisk, it still returns NULL."*
  Concretely: `Index` autowraps iff `index == 0 || index == -1`; `Range`
  autowraps iff `start == 0 && end >= -1`; `Asterisk` never autowraps (it
  matches neither `case` in the type switch).
- Rust (before the fix): `binary_json_ops.rs:427` asked
  `selection_includes_zero(selection, 1)` — "would this selection have picked
  index 0 of a *real* one-element array?" That is a different question, and it
  says yes for three selections where Go says no.

| input | Go | Rust (before) |
| --- | --- | --- |
| `JSON_EXTRACT('1', '$[*]')` | `NULL` | `1` |
| `JSON_EXTRACT('1', '$[last to last]')` | `NULL` | `1` |
| `JSON_EXTRACT('1', '$[0 to last-1]')` | `NULL` | `1` |

`$[0]`, `$[last]`, `$[0 to 5]` and `$[0 to last]` agreed before and after.
Fixed by replacing the helper with `autowraps_non_array`, a direct transcription
of the Go type switch.

### 4. `JSON_MERGE_PRESERVE` folds pairwise instead of grouping adjacent objects  — NOT FIXED

- Go: `pkg/types/json_binary_functions.go:984-1001` (`MergeBinaryJSON`) walks the
  argument list, and whenever it meets an object it takes the whole *run* of
  adjacent objects (`getAdjacentObjects`, `:1003`) and merges them in one call to
  `mergeBinaryObject` (`:1028`), which unions duplicate keys into an array.
  Only then are the per-run results flattened by `mergeBinaryArray` (`:1012`).
- Rust: `binary_json_ops.rs:362-377` (`merge_binary_json`) is a strict left fold
  of a binary `merge_preserve_node` (`:742`). Once a non-object has turned the
  accumulator into an array, every later object is *appended* to that array
  rather than merged with the object next to it.

Distinguishing input: `JSON_MERGE_PRESERVE('[1]', '{"a":1}', '{"a":2}')`.

| | result |
| --- | --- |
| Go | `[1, {"a": [1, 2]}]` |
| Rust | `[1, {"a": 1}, {"a": 2}]` |

Two- and three-argument cases where the objects are the leading run agree:
`JSON_MERGE_PRESERVE('{"a":1}','{"a":2}','{"a":3}')` gives `{"a": [1, 2, 3]}` on
both sides, and `JSON_MERGE_PRESERVE('{"a":1}','[2]','{"a":3}')` gives
`[{"a": 1}, 2, {"a": 3}]` on both sides. The divergence needs an array to
interrupt a run of two or more objects.

Not fixed because restoring Go's shape is an algorithm swap, not a line edit,
and its result cannot be checked here against even one document. The fix is a
faithful port of `MergeBinaryJSON` + `getAdjacentObjects` + `mergeBinaryObject`
onto `JSONNode`, replacing the fold in `merge_binary_json`.

### 5. Text output does not escape U+2028 / U+2029  — NOT FIXED

- Go: `pkg/types/json_binary.go:470-486`. `jsonMarshalStringTo` escapes LINE
  SEPARATOR and PARAGRAPH SEPARATOR unconditionally, with the reason in a
  comment (they break JSONP).
- Rust: `binary_json.rs:1381` and `:1399` (`format_value`) and `:1426`
  (`format_node`) delegate string quoting to `serde_json::to_string`, which
  emits both characters raw.

Distinguishing input: `CAST('" "' AS JSON)`.

| | printed text | bytes on the wire |
| --- | --- | --- |
| Go | `" "` | `22 5c 75 32 30 32 38 22` (8 bytes, ASCII) |
| Rust | `"<U+2028>"` | `22 e2 80 a8 22` (5 bytes) |

The stored binary is identical on both sides — only the text projection differs
— so this is rank 3, not rank 1. It is still a wire-visible difference in the
result of every `SELECT` of such a document.

Everything else about the two escapers agrees: `\\`, `\"`, `\b`, `\f`, `\n`,
`\r`, `\t` by name; all other bytes below `0x20` as lowercase `\u00xx`; `0x7f`
left raw (Go's `jsonSafeSet` marks `` true at `json_constants.go:169`, and
`serde_json` only escapes below `0x20`).

Not fixed because it means hand-writing a `jsonMarshalStringTo` equivalent and
routing three call sites through it, which is more than a certain one-liner.

---

## Rank 4 — diagnostic and acceptance differences

### 6. Lone surrogates: Go substitutes U+FFFD, Rust errors

Two related cases, both from Go tolerating what `serde_json` refuses.

- Parse: `CAST('"\ud800"' AS JSON)`. Go's `json.Valid` accepts it and
  `encoding/json` decodes an unpaired surrogate to U+FFFD, so the document is
  the one-character string U+FFFD. `serde_json` rejects it ("lone leading
  surrogate in hex escape"), so `BinaryJSON::parse`
  (`binary_json.rs:256-266`) returns `InvalidText`.
- `JSON_UNQUOTE`: Go's `unquoteJSONString`
  (`json_binary_functions.go:132-151`) hands a `\uXXXX\uYYYY` pair to
  `decodeOneEscapedUnicode` (`:166`), which calls `utf16.DecodeRune` — and
  `utf16.DecodeRune` returns U+FFFD for an invalid pair rather than failing.
  So `JSON_UNQUOTE('"\ud800\ud800"')` yields U+FFFD in Go. Rust
  (`binary_json.rs:595-597`) requires the second half to be in
  `0xdc00..=0xdfff` and returns `InvalidText` otherwise.

Well-formed surrogate pairs agree exactly, including the
`0x10000 + ((first - 0xd800) << 10) + (second - 0xdc00)` arithmetic.

### 7. A JSON string holding invalid UTF-8 errors instead of printing `�`

Go's `jsonMarshalStringTo` (`json_binary.go:460-469`) replaces each invalid byte
with a literal `�` escape and keeps going; the binary value is free to hold
arbitrary bytes because `appendBinaryString` (`:920`) just appends them. Rust's
`decode_value` (`binary_json.rs:1246-1251`) and `decode_object`
(`:1294-1299`) both run `std::str::from_utf8` and turn a failure into
`InvalidBinary`, so such a document cannot be read at all.

Confidence note: I did not find a SQL path that produces invalid UTF-8 inside a
JSON string — TiDB validates the charset on the way in (`ErrInvalidJSONCharset`)
— so this may be unreachable in practice. It is listed because a Rust node must
be able to read whatever a Go node wrote, and Go's writer imposes no constraint.

### 8. `$[N to]` — Go accepts it as `$[N]`, Rust rejects it

`pkg/types/json_path_expr.go:462-480`. `tryReadString(toStr)` consumes `to` and
only rewinds on *failure*; the `&& unicode.IsSpace(s.peek())` that follows can
then fail with the stream already past `to`, leaving a plain index selection and
a stream positioned at `]`. So Go parses `$[0 to]` successfully, as `$[0]`.
Rust's `parse_array` (`json_path.rs:312-315`) returns `invalid()` as soon as
`to` is not followed by whitespace.

`SELECT JSON_EXTRACT('[1,2,3]', '$[0 to]')` — Go: `1`. Rust: error 3143,
invalid JSON path expression.

`$[0 to3]`, `$[0 tox]` and `$[0 to ]` are rejected by both.

Also in this bucket, not separately numbered:

- Whitespace-only text. `BinaryJSON::parse` short-circuits on
  `text.trim().is_empty()` (`binary_json.rs:257`) and reports "empty document";
  Go's length check is `len(s) == 0` (`json_binary.go:535`), so `'   '` falls
  through to `json.Valid` and reports *"The document root must not be followed
  by other values."* Same rejection, different message.
- A FLOAT64 payload holding NaN. Go carries it (`GetFloat64`) and only fails at
  marshal time; Rust's `decode_value` (`binary_json.rs:1240`) fails at
  `Number::from_f64`, so `compare_binary_json` silently falls back to raw byte
  comparison (`:729`). Not reachable from JSON text, which has no NaN literal.

---

## Verified equal

Each of these was read on both sides and matched.

**Binary layout**

1. All twelve type codes: `0x01` object, `0x03` array, `0x04` literal, `0x09`
   int64, `0x0a` uint64, `0x0b` float64, `0x0c` string, `0x0d` opaque, `0x0e`
   date, `0x0f` datetime, `0x10` timestamp, `0x11` duration.
   (`json_constants.go:28-53` / `binary_json.rs:24-46`.)
2. The three literal codes: null `0x00`, true `0x01`, false `0x02`.
3. `headerSize` 8, `dataSizeOff` 4, `keyEntrySize` 6, `keyLenOff` 4,
   `valEntrySize` 5 (`json_constants.go:177-184` / `binary_json.rs:55-57`).
4. Every multi-byte field little-endian (`jsonEndian = binary.LittleEndian`).
5. Object field order — `element-count`, `size`, key-entry table, value-entry
   table, key bytes, value bytes — and the exact offset arithmetic
   (`appendBinaryObject`, `json_binary.go:1006-1042` / `encode_object`,
   `binary_json.rs:1145-1192`). Array likewise, without the key tables.
6. **Object key sort order is plain bytewise, not length-first.**
   `slices.SortFunc(fields, cmp.Compare(i.key, j.key))` at
   `json_binary.go:1019-1021` compares Go strings, i.e. byte order — TiDB does
   *not* use MySQL's length-then-bytes rule. Rust sorts
   `left.0.as_bytes().cmp(right.0.as_bytes())` (`binary_json.rs:1147`). Same
   bytes for `{"b":1,"aa":2}`: `aa` before `b`.
7. **Inlining threshold: only literals are ever inlined**, never small ints,
   despite the grammar comment at `json_binary.go:93-96` saying otherwise.
   `appendBinaryValElem` (`:988-994`) special-cases `JSONTypeCodeLiteral` and
   nothing else; Rust does the same at `binary_json.rs:1146` and `:1196`.
8. Key length above `math.MaxUint16` raises `ErrJSONObjectKeyTooLong` on both
   (`json_binary.go:1026` / `binary_json.rs:1150`).
9. String encoding: uvarint of the **byte** length, then the raw bytes
   (`appendBinaryString`, `json_binary.go:920` / `binary_json.rs:1096-1099`),
   and the same uvarint on decode.
10. Opaque encoding: one type-id byte, uvarint length, bytes.
11. Time is 8 bytes (the packed `CoreTime`); duration is 8 bytes plus a 4-byte
    fsp, 12 total (`json_binary.go:816-819` / `value_length`,
    `binary_json_ops` and `binary_json.rs:1334`).
12. `maxJSONDepth` 100 on both.

**Numbers**

13. Text parse order after the fix: int64, then uint64, then float64. Go's
    `strings.Contains(x.String(), "Ee.")` guard at `json_binary.go:901` searches
    for that literal three-character substring, so it never fires and the
    int-first order is what actually runs.
14. `1.0` is DOUBLE on both (`ParseInt("1.0")` fails; `serde_json` yields an
    `f64` whose `as_i64()` is `None`). `1e3` is DOUBLE on both. `-1` is INTEGER.
15. A value beyond `u64::MAX` (`123456789012345678901234567890`) becomes a
    DOUBLE on both.
16. `getInt64FractionLength` at `i64::MIN`: Go's `uint64(-i)` wraps to
    `0x8000000000000000`, which is what Rust's `unsigned_abs()` gives.

**Comparison and ordering**

17. The full cross-type total order. Go's negative precedence table
    (`json_constants.go:188-203`) and Rust's positive one
    (`binary_json.rs:780-798`) are the same sequence:
    BLOB > BIT > OPAQUE > DATETIME/TIMESTAMP > TIME > DATE > BOOLEAN > ARRAY >
    OBJECT > STRING > {INTEGER, UNSIGNED INTEGER, DOUBLE} > NULL. All three
    numeric kinds share one rank on both sides, so they compare by value.
18. `Type()` names, including `JSONTypeCodeTimestamp` reporting `"DATETIME"`
    (`json_binary_functions.go:70-71` / `binary_json.rs:391`) — which is what
    makes datetimes and timestamps mutually comparable and dates not.
19. The opaque sub-typing: BLOB for `{0x0f, 0xf9..0xfe}` (varchar, tiny/medium/
    long/blob, var-string, string) and BIT for `0x10`
    (`json_binary_functions.go:59-62` / `binary_json.rs:386-387`).
20. Integer comparison matrix: int/int and uint/uint exact; int-vs-uint returns
    `Less`/`Greater` immediately on a negative signed side, else widens to `u64`
    (`compareInt64Uint64`, `json_binary_functions.go:776` /
    `binary_json.rs:858-871`).
21. Integer-vs-double keeps the `1e-8` epsilon on both (after the fix, this is
    the only place it applies).
22. Boolean order: `false < true`. Null equals null.
23. Array comparison: elementwise, then element count. Object comparison: count
    first, then key bytes, then value — over keys in stored (byte-sorted) order
    (`json_binary_functions.go:835-865` / `binary_json.rs:733-764`).

**Paths and functions**

24. `**` recursion shape. Go re-applies the *whole* path expression to each
    child (`json_binary_functions.go:342-354`); Rust splits it into
    `extract_value` + `extract_descendants` (`binary_json_ops.rs:431-475`).
    Traced: the two expand to the same set, in the same order.
25. Extract result wrapping: unwrapped only when there is exactly one path, one
    match, *and* the path cannot match multiple values — the issue-30352 fix at
    `json_binary_functions.go:277` is present at `binary_json_ops.rs:59`. The
    dedup set is shared across all paths in the list on both sides.
26. `is_ecmascript_identifier`. Go iterates *bytes* (`rune(s[i])`,
    `json_path_expr.go:549-553`), so its Latin-1 letter set is exactly
    ASCII letters plus `0xAA`, `0xB5`, `0xBA`, `0xC0..0xD6`, `0xD8..0xF6`,
    `0xF8..0xFF`; its Mc / Pc / ZWNJ / ZWJ branches are unreachable from a byte
    cast, and `unicode.IsDigit` on a Latin-1 byte means ASCII digits only. Rust
    (`json_path.rs:432-451`, and the duplicate at `binary_json.rs:647-663`)
    encodes precisely that set. Both therefore reject `$.café` unquoted.
27. `HashValue` — the index-key projection, `json_binary.go:624-673` vs
    `append_hash_value`, `binary_json_ops.rs:798-836`. An int64 or uint64 whose
    significant fraction is 52 bits or fewer is rewritten as type code `0x0b`
    plus its `f64` bits; otherwise the raw type code and payload. Arrays and
    objects emit the type code plus the 4-byte element count, then recurse;
    object keys are emitted as the STRING *payload* (uvarint length + bytes,
    no type code). `significant_fraction_bits` is `64 - lz - tz - 1` with a
    zero special case, matching `getUint64FractionLength` including the
    `lz == 64 && tz == 64` branch.

Also checked and matching, without separate numbering: the float text form
(fixed notation inside `[1e-15, 1e15)`, scientific outside, `.0` appended to an
integral fixed value, no `+` in the exponent — `marshalFloat64To`,
`json_binary.go:333-384` vs `format_float64`, `binary_json.rs:519`);
`JSON_UNQUOTE` unquoting only a STRING value whose text is at least 2 bytes and
`"`-delimited (`UnquoteString`, `json_binary_functions.go:93` vs
`unquote_string`, `binary_json.rs:535`); the unknown-escape rule (backslash
dropped, character kept); merge-patch semantics (a `null` patch value deletes
the key, a missing target key merges against a non-object, and a non-object
patch replaces wholesale); `$[N]` autowrap on a non-array for index `0` and
`last`; `objectSearchKey`'s binary search against Rust's linear find (same
answer, since keys are unique and sorted); and the base64 `"base64:typeN:..."`
text form for opaque values.

`JSON_TABLE` is a standing hard skip in this project and was not examined.

---

## What is unverified

- **Everything above is code reading. No JSON document was pushed through
  either implementation.** No `cargo test`, no `gorun`, no `goeval`, no
  `tidb-server`.
- The three fixes compile clean (`cargo check`, `cargo clippy --all-targets`,
  both `EXIT=0` on `-p tidb-datatype`) and `cargo fmt --all --check` is clean,
  but **their tests were not run**. In particular
  `rust/crates/tidb-session/src/tests_json.rs:561` already asserts
  `JSON_TYPE('18446744073709551615') = 'UNSIGNED INTEGER'` — it passes today
  only because the live builtin uses the `tidb-expr` representation, not
  `BinaryJSON`. Fix 1 makes `BinaryJSON` agree with that assertion; nothing
  verified that no other test asserted the old float behavior (a repo-wide grep
  for `1.8446744073709552e19` and `9223372036854775808` under
  `crates/tidb-datatype` found nothing).
- Finding 1's ranking rests on reading `appendBinaryNumber` against a stale test
  comment that contradicts it. One `SELECT JSON_TYPE('18446744073709551615')`
  on a real server settles it.
- Findings 5, 6 and 7 involve `serde_json`'s exact behavior on out-of-range and
  malformed input, asserted from its documented behavior rather than observed.
  Related and *not* resolved: whether `serde_json` errors or yields infinity for
  `1e400`, where Go's `strconv.ParseFloat` errors and the whole parse fails.
- No fuzz or differential run against
  `rust/crates/tidb-datatype/fuzz/json_extract.rs`, which exists and is the
  natural way to close findings 3 and 8.
- The `tidb-expr` JSON implementation was read only far enough to establish that
  it exists and is separate. It was not audited, and the tidb-datatype findings
  here say nothing about whether it has the same ones.
