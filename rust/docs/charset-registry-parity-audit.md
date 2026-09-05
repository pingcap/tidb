# charset registry parity audit: Go `pkg/parser/charset` vs `tidb-datatype::charset`

Audit date: 2026-09-05. Method: element-level comparison of the
supported-charset descriptors and their default collations.

## Results

- **Supported set: 7 = 7.** Go's `CharacterSetInfos` carries utf8,
  utf8mb4, ascii, latin1, binary, gbk, gb18030 — exactly Rust's
  `Charset` enum.
- **Max lengths match** per charset (1/1/1/3/4/2/4), the same numbers
  Rust exposes through the metadata byte-width helpers.
- **Default collations match, including the conditional**: Go's
  gbk/gb18030 defaults flip between `*_bin` and `*_chinese_ci` with the
  new-collation state (`charset.go:239-243`), which Rust's
  `default_collation` mirrors through `new_collation_enabled()`
  (`charset.rs:105-117`).
- **The wider MySQL list is mirrored, not missing.** The
  script-generated `charset_data/known_charsets.rs` mirrors the full
  supported-descriptor table (descriptions, maxlens, default
  collations), and `charset_data/collations.rs` mirrors the 273-row
  collation descriptor superset. Regenerating with
  `rust/scripts/generate-parser-charset.py` against this Go master
  produces a byte-identical tree (2026-09-05), so the generated layer
  is current; the Go `CharsetIDs` legacy wire-id map is a separate
  table whose values disagree with the descriptor defaults for six
  charsets (Go carries both notions: `CharsetNameToID` wire ids vs
  `CharacterSetInfos` descriptors); the Rust generated table mirrors
  the descriptor side, and the wire path derives ids from the column's
  collation instead.

## Collation ID table (2026-09-05, second pass)

Mechanical diff of Go's `mysql.Collations` id-to-name map (223 entries)
against Rust's `charset_data/collations.rs` table (273 rows): **all 223
Go ids resolve to the same name, zero mismatches.** The 50 Rust-only
ids (utf8_tolower_ci 76, gb18030_unicode_520_ci 250, the 0900 family,
the 256+ dynamic range) come from Go's own collation descriptor list in
`charset.go:424+` — the superset `GetCollationByName` serves — so the
Rust table is that superset, not an invented addition. The id→name
fallback (46, utf8mb4_bin... GeneralCi) matches Go's
`DefaultCollationID`.
