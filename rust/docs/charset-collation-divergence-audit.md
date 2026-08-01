# Charset and collation: `tidb-datatype` vs `pkg/util/collate` + `pkg/parser/charset`

Scope: `rust/crates/tidb-datatype/src/{charset.rs, collation.rs, charset_data/, collation_data/,
multibyte_encoding.rs, encoding_base.rs}` against `pkg/util/collate/*` and
`pkg/parser/charset/*`. This surface was explicitly listed as NOT audited by the `pkg/types`
sweep (`rust/docs/types-datatype-divergence-audit.md`), so its absence from that findings list
meant unexamined.

Ranking is by consequence: (1) a sort key that differs from Go's, because that is wrong index
bytes in shared storage; (2) a comparison that answers differently; (3) a missing collation that
silently falls back; (4) charset-conversion and diagnostic differences.

7 findings. 3 fixed in commit `19ea70f364`; 4 written up. Everything else on this surface was
checked and is listed as verified-equal below, including all seven generated weight images and
all four generated registry tables, which are byte-for-byte identical to Go's.

No test could be executed: `cargo test` cannot run on this machine. Every claim below is either a
source-to-source reading, or a mechanical re-derivation of Go's table from Go's source compared
against the shipped Rust image.

---

## Rank 1 — wrong sort key, i.e. wrong index bytes in shared storage

### F1. `ascii_bin` and `latin1_bin` did not trim trailing spaces — FIXED

* Go: `pkg/util/collate/collate.go:448-451` registers both names as `&binPaddingCollator{}`.
  `pkg/util/collate/bin.go:75-81` — `binPaddingCollator.Compare` and `.Key` both run
  `truncateTailingSpace`.
* Rust before the fix: `crates/tidb-datatype/src/collation.rs:558-560` (`compare`) and `:580-582`
  (`key`) put `AsciiBin` and `Latin1Bin` in the same arm as `Binary`, which is `binCollator` — no
  trim.

Distinguishing input, `latin1_bin`, string `"a "` (0x61 0x20):

| | Go | Rust (before) |
|---|---|---|
| `Key("a ")` | `61` | `61 20` |
| `Compare("a ", "a")` | `0` (equal) | `Greater` |

Both halves are wrong at once. The key half is the serious one: every `ascii`/`latin1`
`CHAR`/`VARCHAR` index entry a Rust node writes carries a trailing-space run that a Go node's
entry does not, so the two nodes order the same index differently and a Go node's range scan
skips rows a Rust node wrote. The compare half makes `WHERE c = 'a'` miss a stored `'a '` that
TiDB returns.

Note `KeyWithoutTrimRightSpace` was already correct on both sides (`bin.go:89-91` returns the
raw string), so only `key`/`compare` moved.

### F2. `gbk` / `gb18030` default collation: the registry and the DDL path disagree, and the live TiDB answer is neither of them consistently — NOT FIXED

* Go, package init: `pkg/parser/charset/charset.go:73-74` starts `gbk` at `CollationGBKBin` and
  `gb18030` at `CollationGB18030Bin`.
* Go, server bootstrap: `pkg/session/global_init.go:76` calls
  `collate.SetNewCollationEnabledForTest(newCollationEnabled)`, which runs `switchDefaultCollation`
  (`pkg/util/collate/charset.go:21-31`) and rewrites those two defaults to `gbk_chinese_ci` /
  `gb18030_chinese_ci`. **A live TiDB server therefore answers `gbk_chinese_ci`.**
* Rust: `collation.rs:31` — `NEW_COLLATION_ENABLED: AtomicBool = AtomicBool::new(true)`. But
  `charset.rs:433 set_new_collation_defaults` is only reachable from `set_new_collation_enabled`
  (`collation.rs:133`), and nothing calls that at startup. So a fresh Rust process reports
  new collations ENABLED while its registry still holds the DISABLED-mode defaults:
  `Registry::source` (`charset.rs:374`) seeds `("gbk", "gbk_bin", …)`, and the test at
  `charset.rs:650` pins `get_default_collation("gbk") == "gbk_bin"`.
* Meanwhile `Charset::Gbk.default_collation()` (`charset.rs:100`) is a `const fn` hard-coded to
  `GbkChineseCi` — the enabled-mode answer, with a comment citing a live capture.

Two Rust callers, two answers, for the same question:

| caller | function used | answer |
|---|---|---|
| `crates/tidb-exec/src/table_info_build.rs:473, :937` | `get_default_collation("gbk")` | `gbk_bin` |
| `crates/tidb-executor/src/ddl/column_types.rs:57` (`resolve_pair`) | `Charset::default_collation()` | `gbk_chinese_ci` |

Distinguishing input:

```sql
CREATE TABLE t (a VARCHAR(10)) DEFAULT CHARSET=gbk;
SELECT COLLATION(a) FROM t;
```

TiDB: `gbk_chinese_ci`. Rust through `table_info_build`: `gbk_bin`.

Consequence is rank 1 by effect even though the cause is startup ordering: `gbk_bin`'s key
transcodes UTF-8 to GBK bytes while `gbk_chinese_ci`'s key is the weight stream, so the column
gets an entirely different sort key and the index bytes are not comparable at all.

Not fixed here because the repair is cross-crate and has two defensible shapes — either call
`set_new_collation_defaults(true)` once at registry construction so the registry agrees with
`NEW_COLLATION_ENABLED`'s default (which flips the `charset.rs:650` assertion), or delete
`Charset::default_collation()`'s independent answer and route `resolve_pair` through the
registry. Either touches `tidb-exec` and `tidb-executor`, both outside this audit's edit scope.

### F3. `gb18030_bin` key: Go pads 19 PUA runes to a 4-byte source; Rust has no counterpart — UNVERIFIED

* Go: `pkg/util/collate/gb18030_bin.go:28-33` declares `fourBytesRune`, 19 runes in
  U+E78D..U+E864. `gb18030_bin.go:92-97`, inside `KeyWithoutTrimRightSpace`: for one of those
  runes it allocates `buf1 := make([]byte, 4)`, copies the rune's 3 UTF-8 bytes in, and encodes
  `buf1` — leaving a trailing 0x00 byte in the **source**.
* `customGB18030Encoder.Transform` (`pkg/parser/charset/encoding_gb18030.go:210-237`) loops over
  the whole source, so on the face of the code it encodes the rune AND the trailing NUL:
  `Key("")` would be `84 31 82 36 00`, five bytes, not four.
* Rust: `collation.rs:646 encoded_binary_key` runs the plain `Encoding::Gb18030` transform with no
  such padding, so it would emit `84 31 82 36`.

I could not settle this. It turns on whether `x/text`'s `transform.Bytes` destination growth makes
the extra source byte unreachable in practice, which needs one Go execution:

```go
collate.GetCollator("gb18030_bin").Key("")   // 4 bytes or 5?
```

If Go emits 5 bytes this is a rank-1 sort-key divergence confined to those 19 code points. The
19 runes and the reason Go singles them out are both in `gb18030_bin.go:26-33`.

---

## Rank 2 — wrong rows, right bytes

### F4. `gbk_bin` compiled `LIKE` by byte instead of by rune — FIXED

* Go: `pkg/util/collate/gbk_bin.go:99-106` — `gbkBinCollator.Pattern()` returns `&gbkBinPattern{}`,
  and `gbkBinPattern` embeds **`derivedBinPattern`**, which is rune-wise
  (`bin.go:109-122` → `stringutil.CompilePattern` / `DoMatch`, `string_util.go:148, 289`).
  The comment "use binPattern directly, they are totally same" is misleading: the embedded type
  is the rune one.
* Only `gb18030_bin` is byte-wise: `gb18030_bin.go:121-123`, `gb18030BinPattern` embeds
  `binPattern` (`bin.go:124-137` → `CompilePatternBinary` / `DoMatchBinary`).
* Rust before the fix: `collation.rs:524-526` grouped `GbkBin` with `Binary` and `Gb18030Bin` into
  `WildcardPattern::binary`.

Distinguishing input, `gbk_bin`:

| | Go | Rust (before) |
|---|---|---|
| `'中' LIKE '_'` | TRUE (one rune, one `_`) | FALSE (3 bytes, one `_`) |

`gb18030_bin` stays byte-wise on both sides and is unaffected.

---

## Rank 3 — missing or degraded implementation

### F5. `CanUseRawMemAsKey` did not accept `utf8mb4_0900_bin` — FIXED (latent)

* Go: `collate.go:414-422` returns true for `*binCollator` and `*derivedBinCollator`;
  `collate.go:456-457` registers `utf8mb4_0900_bin` as a `derivedBinCollator`, so Go answers true.
* Rust before the fix: `collation.rs:122` accepted only `DerivedBinary | New(Binary)`.

Sound to fix because `Collation::Utf8Mb40900Bin::key(v) == v`. `can_use_raw_mem_as_key` currently
has no callers in the Rust workspace, so this was latent — it would have become a needless copy,
not a wrong answer, the moment a caller appeared.

---

## Rank 4 — conversion and diagnostic differences

### F6. `SubstituteMissingCollationToDefault` returns the canonical name where Go returns the caller's spelling

* Go: `collate.go:222-233` — on a successful lookup it returns the **input string `co`**, untouched.
* Rust: `collation.rs:224-228` returns `row.name`, the registry's canonical lowercase name.

Distinguishing inputs: `"UTF8MB4_BIN"` → Go `"UTF8MB4_BIN"`, Rust `"utf8mb4_bin"`.
`"utf8mb3_bin"` → Go `"utf8mb3_bin"`, Rust `"utf8_bin"` (the `utf8Alias` rewrite leaks out).
The value is what a connection's `collation_connection` ends up holding, so it is visible in
`SHOW VARIABLES` and in `SELECT @@collation_connection`. Not fixed: Go's behavior is arguably the
bug, and the Rust answer is what several call sites downstream already assume.

### F7. The GBK / GB18030 codec substrate is a different library — UNVERIFIED

Go converts through `golang.org/x/text/encoding/simplifiedchinese`; Rust converts through
`encoding_rs`, which implements the WHATWG index. These are two independent tables, not one table
read twice, and the Rust side patches three known divergences by hand:

* `multibyte_encoding.rs:316-318` — reject `'€'` (U+20AC) for GBK, matching TiDB's
  `customGBKEncoder` (`encoding_gbk.go:165-170`). WHATWG GBK would encode it to `0x80`.
* `multibyte_encoding.rs:342-344` — a decode group starting with `0x80` is invalid, matching
  `customGBKDecoder` (`encoding_gbk.go:135-143`) and `customGB18030Decoder`
  (`encoding_gb18030.go:175-178`).
* `multibyte_encoding.rs:372-380` — **any** WHATWG GBK decode output containing a private-use
  character U+E000..U+F8FF is declared invalid.

The third is the risky one: it is a rule Go states nowhere, and TiDB's own GB18030 override table
deliberately maps U+E000..U+E864 to real GB18030 byte sequences
(`encoding_gb18030_data.go`, 2094 pairs). I did not enumerate all 65536 GBK code points against
`x/text` — that needs Go execution. Anyone picking this up should diff, for every 2-byte GBK
sequence, `charset.EncodingGBKImpl.Transform(nil, src, charset.OpDecodeReplace)` against Rust's
`Encoding::Gbk.transform(src, TransformOp::DECODE_REPLACE)`, and the same in the encode direction
over all of U+0000..U+FFFF.

The operation-bit vocabulary, the first-error rule, replacement, truncation and collect-from /
collect-to policy were read side by side and match: `encoding.go:113-134` and
`encoding_base.go:58-85` against `encoding_base.rs:31-70` and `TransformPolicy`.

---

## Verified equal

### Generated data — byte-for-byte identical

Each Go table was re-derived from Go source and packed exactly the way the shipped Rust image is
packed, then compared byte for byte.

| Rust image | Go source | size | result |
|---|---|---|---|
| `collation_data/general_ci_u16_le.bin` | `general_ci.go:305-335` `planeTable`, flattened per `convertRuneGeneralCI` | 131072 B | identical |
| `collation_data/gbk_chinese_ci_u16_le.bin` | `gbk_chinese_ci_data.go` `gbkChineseCISortKeyTable` (65536 u16) | 131072 B | identical |
| `collation_data/gb18030_chinese_ci_u32_le.bin` | `gb18030_weight.data` | 4456448 B | identical, SHA256 `64faeaa7…` on both |
| `collation_data/unicode_0400_u64_le.bin` | `ucadata.DUCET0400Table.MapTable4` (65536 u64) | 524288 B | identical |
| `collation_data/unicode_0400_long_u64_le.bin` | `DUCET0400Table.LongRuneMap` (22 rows) | 440 B | identical |
| `collation_data/unicode_0900_u64_le.bin` | `ucadata.DUCET0900Table.MapTable4` (183969 u64) | 1471752 B | identical |
| `collation_data/unicode_0900_long_u64_le.bin` | `DUCET0900Table.LongRuneMap` (27 rows) | 540 B | identical |

| Rust table | Go source | result |
|---|---|---|
| `charset_data/collations.rs` | `charset.go:349-623` `collations` | 273 rows, same order, every `(id, charset, name, is_default, sortlen, pad)` equal |
| `charset_data/known_charsets.rs` | `charset.go:305-347` `charsets` | 41 entries, every `(maxlen, default collation, description)` equal |
| `charset_data/gb18030_by_rune.rs`, `gb18030_by_bytes.rs` | `encoding_gb18030_data.go` `gb18030EncodingList` | 2094 pairs each direction, none missing/extra/differing, both sorted so `binary_search` is valid |
| `charset_data/gbk_cases.rs` | `encoding_gbk.go:86-110` `GBKCase` | 23 ranges, identical |
| `charset_data/gb18030_cases.rs` | `encoding_gb18030_data.go` `GB18030Case` | 58 ranges, identical |

The collation-ID registry is therefore exact: no wrong ID, no missing entry, no wrong
default-per-charset row, no wrong PAD attribute, for all 273 MySQL collations.

### Per-collation key / compare / pattern, after commit `19ea70f364`

| collation | Go collator | `Key` | `Compare` | `Pattern` | `MaxKeyLen` |
|---|---|---|---|---|---|
| `binary` | `binCollator` | equal (raw, NO PAD) | equal | equal (byte-wise) | equal |
| `ascii_bin` | `binPaddingCollator` | equal **after F1** | equal **after F1** | equal (rune-wise) | equal |
| `latin1_bin` | `binPaddingCollator` | equal **after F1** | equal **after F1** | equal (rune-wise) | equal |
| `utf8_bin` | `binPaddingCollator` | equal | equal | equal | equal |
| `utf8mb4_bin` | `binPaddingCollator` | equal | equal | equal | equal |
| `utf8mb4_0900_bin` | `derivedBinCollator` | equal (raw, NO PAD) | equal | equal | equal |
| `utf8_general_ci` | `generalCICollator` | equal | equal | equal | equal |
| `utf8mb4_general_ci` | `generalCICollator` | equal | equal | equal | equal |
| `utf8_unicode_ci` | `unicodeCICollator` (UCA 4.0) | equal | equal | equal (see note U1) | equal |
| `utf8mb4_unicode_ci` | `unicodeCICollator` (UCA 4.0) | equal | equal | equal (see note U1) | equal |
| `utf8mb4_0900_ai_ci` | `unicode0900AICICollator` | equal (NO PAD) | equal | equal | equal |
| `gbk_chinese_ci` | `gbkChineseCICollator` | equal | equal | equal | equal |
| `gb18030_chinese_ci` | `gb18030ChineseCICollator` | equal | equal | equal | equal |
| `gbk_bin` | `gbkBinCollator` | equal modulo F7 | equal modulo F7 | equal **after F4** | equal |
| `gb18030_bin` | `gb18030BinCollator` | **see F3** and F7 | equal modulo F7 | equal (byte-wise) | equal |
| `utf8mb4_zh_pinyin_tidb_as_cs` | `zhPinyinTiDBASCSCollator` | both panic | both panic | both panic | both panic |

Supporting readings for the "equal" claims:

* **Padding rule.** Exactly `binary`, `utf8mb4_0900_ai_ci`, `utf8mb4_0900_bin` are NO PAD on both
  sides. Every other implemented collation trims trailing 0x20 in `Key`/`Compare` and does not in
  `KeyWithoutTrimRightSpace`. Rust `collation.rs:309 is_pad_space_collation` matches
  `collate.go:363` name for name, and after F1 the `key`/`compare` arms agree with it.
* **Invalid UTF-8.** Go's CI collators return the partial key at the first invalid sequence
  (`general_ci.go:50-54`, `gbk_chinese_ci.go:50-54`, `gb18030_chinese_ci.go:65-69`,
  `unicode_0900_ai_ci_generated.go:126-129`) and `compareCommon` returns 0 (`collate.go:395-399`).
  Rust breaks out of the key loops and returns `Ordering::Equal` from the compare loops
  (`collation.rs:715-735, 746-749, 756-776, 976-1000`). Both distinguish an invalid byte sequence
  from a legitimately encoded U+FFFD by checking width == 1, Go via
  `utf8.DecodeRuneInString`, Rust via `decode_rune` returning `Err`.
* **Chinese-CI byte emission.** Go emits from the highest nonzero byte downward with an
  always-emitted low byte (`gbk_chinese_ci.go:58-61`, `gb18030_chinese_ci.go:73-82`). Rust
  `chinese_ci_key` (`collation.rs:778-797`) takes `to_be_bytes` and starts at the first nonzero
  byte, falling back to index 3 when the weight is 0. Identical for every u16/u32 value including
  0 (both emit one 0x00 byte), 0x0100 (both `01 00`) and 0x00010000 (both `01 00 00`).
* **UCA weight streaming.** Go's generated code walks u16 chunks low-16-first out of the
  `(first, second)` pair (`unicode_0900_ai_ci_generated.go:96-102` compare,
  `:134-141` key, big-endian per u16). Rust's `UcaCursor::append_packed` (`collation.rs:955-961`)
  pushes the same order and `weighted_key` writes `to_be_bytes`. Zero-weight (ignorable) runes are
  skipped identically: Go's refill loop keeps consuming while `an == 0`, Rust's `append_packed`
  pushes nothing for a 0 and the cursor loops.
* **Note U1 — `utf8mb4_unicode_ci` LIKE.** Go compares the RAW `MapTable4` entries and additionally
  requires codepoint equality when both are the long-rune marker `0xFFFD`
  (`unicode_0400_ci_impl.go:66-81`). Rust compares the RESOLVED `(first, second)` pair
  (`collation.rs:390-397`). I checked all 65536 codepoints against the shipped UCA 4.0 image: no
  non-long codepoint resolves to any long rune's expansion, and no two long runes share an
  expansion, so the two rules agree on every possible input. The 22 long runes are
  U+321D, U+321E, U+327C, U+3307, U+3315-U+3317, U+3319, U+331A, U+3320, U+332B, U+332E, U+3332,
  U+3334, U+3336, U+3347, U+334A, U+3356, U+337F, U+33AE, U+33AF, U+FDFB.
* **`utf8mb4_0900_ai_ci` at U+2CEE1.** `convertRuneUnicodeCI0900` guards with `>` and not `>=`
  against a 183969-entry array (`unicode_0900_ai_ci_impl.go:44`), so codepoint 183969 = U+2CEE1
  — an assigned CJK Ext. F character — indexes out of range and panics. Rust reproduces the panic
  at exactly that index (`collation.rs:850-858`). Bug-for-bug equal; both nodes die on the same
  input rather than disagreeing about it.
* **`gbk_bin` / `gb18030_bin` compare shape.** Go compares each character's encoded bytes in turn
  and falls back to remaining-UTF-8-length (`gbk_bin.go:37-64`, `gb18030_bin.go:46-73`); Rust
  encodes the whole string once and compares the byte vectors (`collation.rs:658-660`). These
  agree because no encoding in either charset is a proper prefix of another: 1-byte encodings are
  0x00-0x7F, 2-byte leads are 0x81-0xFE, and the 4-byte second byte (0x30-0x39) is disjoint from
  the 2-byte trail (0x40-0x7E, 0x80-0xFE). The `?` replacement byte (0x3F) is likewise not a
  prefix of any multi-byte sequence. Both sides also chunk the UTF-8 source by first byte only
  — Go via `runeLen` (`collate.go:293-302`) and `encodingUTF8.Peek` (`encoding_utf8.go:56-69`),
  which are the same function; Rust via `UTF8_ENCODING.peek` in
  `multibyte_encoding.rs:196-202` — so an invalid lead byte in 0x80..0xC1 consumes 2 bytes on both
  sides and produces one `?`.

### Registry behavior verified equal

* `get_collator` / `get_collator_with_mode`: legacy mode returns the derived binary collator for
  every name; new-collation mode falls back to `utf8mb4_bin` for an unknown name
  (`collate.go:146-161` vs `collation.rs:173-178`). The fallback is `utf8mb4_bin`, not raw byte
  comparison, on both sides — so there is no silent-fallback-to-binary wrong-answer generator here.
* `get_collator_by_id`: an id that is in the parser registry but has no implementation
  (e.g. 8 = `latin1_swedish_ci`) and an id that is in neither both fall back to `utf8mb4_bin`
  (`collate.go:181-194` vs `collation.rs:191-200`).
* `collation_id_to_name` / `collation_name_to_id` defaults: `"utf8mb4_bin"` / `46`, matching
  `mysql.DefaultCollationName` / `mysql.DefaultCollationID` (`pkg/parser/mysql/charset.go:539-549`).
* `RewriteNewCollationIDIfNeeded` / `RestoreCollationIDIfNeeded`, including the `i32::MIN` wrap
  (Go's `-id` on int32 and Rust's `wrapping_neg` both wrap to `i32::MIN`).
* `CompatibleCollate`'s three equivalence classes (`collate.go:102-111` vs `collation.rs:139-147`).
* `IsCICollation`, `ConvertAndGetBinCollation`, `IsBinCollation`, `IsPadSpaceCollation`,
  `IsDefaultCollationForUTF8MB4` — name-for-name identical membership.
* `GetSupportedCollations` under new collations: the same 15 names — Go's 16-entry `newCollatorMap`
  minus `utf8mb4_zh_pinyin_tidb_as_cs` — sorted by name (`collate.go:249-270` vs
  `collation.rs:231-260`).
* `GetCollationByName`'s `utf8mb3_*` aliasing and lowercasing (`charset.go:195-216` vs
  `charset.rs:470-477, 571-579`).
* `ValidCharsetAndCollation`, `GetDefaultCollationLegacy` (GBK and GB18030 deliberately absent),
  `GetCharsetInfoByID`'s special case for id 46.
* `NeedRestoredDataWithCollate`: `crates/tidb-datatype/src/field_type/mod.rs:915-935` now carries
  both `utf8mb4_0900_bin` in the `_bin` membership set AND Go's trailing explicit guard
  (`pkg/types/etc.go:147-155`). The encoding audit's rank-1 on this boolean is closed.

---

## Unverified

* Anything requiring execution. `cargo test`, `nextest`, `gorun` and `goeval` cannot run on this
  machine; no test was run and no runtime behavior was observed.
* **F3**, the `gb18030_bin` `fourBytesRune` padding — one Go run settles it.
* **F7**, the GBK / GB18030 codec substrate. The three hand-patched divergences were read; the
  remaining 65536-codepoint surface was not enumerated against `x/text`. The private-use-character
  heuristic at `multibyte_encoding.rs:372-380` is the specific line to distrust.
* `utf8mb4_zh_pinyin_tidb_as_cs` panics on both sides, but I did not check that the panic message
  or the exact call at which it fires match.
* The **connection** level of charset resolution (`SET NAMES`, `character_set_connection`,
  `character_set_client` and their precedence against a column's declared charset) lives in
  `pkg/session` / `pkg/sessionctx`, outside this crate. Only the column / table / server-default
  precedence was compared, and F2 is what came out of it.
* `Encoding::to_upper` / `to_lower`: the GBK and GB18030 `SpecialCase` tables were verified
  identical, but Go applies them through `strings.ToUpperSpecial`, which layers the Unicode
  default case mapping underneath, and Rust applies them through its own `map_case`. The tables
  agree; the layering underneath them was not compared.
