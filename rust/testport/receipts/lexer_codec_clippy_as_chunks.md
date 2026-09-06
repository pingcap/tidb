# lexer/codec clippy `as_chunks` gate cleanup (batch #37)

## Trigger

Same nightly lint refresh as batch #36: `clippy::chunks_exact_to_as_chunks`
warnings in `tidb-lexer` (1) and `tidb-codec` (2). The remaining `tidb-vardef`
"missing documentation" warning is in the sibling-owned sysvar stream and was
left untouched.

## Sites

| file | line | fix |
| --- | --- | --- |
| `tidb-lexer/src/lib.rs` | 1167 | hex-lit decode: `chunks_exact(2)` → `as_chunks::<2>().0.iter()`, `from_utf8(pair)` via array deref |
| `tidb-codec/src/bytes.rs` | 130 | group decode: `chunks_exact(GROUP_SIZE + 1)` → `as_chunks::<{ GROUP_SIZE + 1 }>().0` (const-generic) |
| `tidb-codec/src/column.rs` | 780/788 | offset table: `chunks_exact(OFFSET_BYTES)` → `as_chunks::<OFFSET_BYTES>().0`, inner `try_into().unwrap()` collapsed to `*bytes` |

All behavior-neutral: identical grouping, with the compiler now proving the
byte-width conversions total (the codec offset `unwrap` was always statically
safe — `OFFSET_BYTES` is 8 — and is now spelled `*bytes`).

## Verification

- `cargo fmt --check -p tidb-lexer -p tidb-codec` clean
- `cargo clippy -p tidb-lexer -p tidb-codec`: 0 warnings
- `nextest -p tidb-lexer -p tidb-codec`: 309/309
- `nextest -p tidb-parser` (lexer consumer): 833/833

No behavioral change, so no fail-before regression applies (clippy-batch
convention). Go-source fidelity of the codec group/offset decode paths is
unchanged — restructuring only.
