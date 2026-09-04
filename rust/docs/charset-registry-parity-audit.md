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
- **Out of scope, deliberately**: Go's `CharsetIDs` legacy map (~260
  entries) carries wire IDs for the full MySQL charset list; this
  port's vocabulary is the 7 supported charsets, and the wider table is
  a fork-scope boundary rather than a divergence to fix.
