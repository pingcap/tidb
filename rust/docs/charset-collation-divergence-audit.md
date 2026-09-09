# Charset and collation parity record

Authority: TiDB Go commit `e2788410d8d696605e8cb002585877a063ccc909`.

The complete 35-artifact `pkg/util/collate` package maps to
`rust/crates/tidb-datatype/src/collation.rs`, its generated binary images and
generator, its collation tests, and `benches/collate.rs`. The package depends
on the separately owned `pkg/parser/charset` transcreation for registry and
GBK/GB18030 conversion behavior.

Current package findings: **0**.

The package audit covers every production collator, registry helper, wildcard
family, generated UCA implementation, source-data generator and input,
retained UCA fixture, package harness, benchmark, and Bazel target. The local
Go package is byte-identical to the authority commit.

The Rust generator mechanically rebuilds all seven collation images from the
Go authorities and verifies the retained UCA 4.0 fixture. Its `--check` gate
therefore covers the two Go generator programs, both templates, both allkeys
inputs, all generated Go tables, the GBK table, and the embedded GB18030 data.

The final parity corrections in this audit are:

- preserve the caller's spelling when
  `SubstituteMissingCollationToDefault` succeeds;
- borrow binary input storage from `ImmutableKey`, including trimmed borrowed
  slices for PAD SPACE binary collators;
- accept arbitrary Go-string bytes when compiling wildcard patterns;
- preserve the source pinyin stub's `implement me` panic contract;
- use Go map zero-value behavior for UCA 9.0 surrogate-marker lookups; and
- execute the actual immutable-key operation in the benchmark target.

The complete validation and artifact inventory are recorded in
`rust/testport/receipts/b018.md`.
