# Datatype source coverage

This inventory records dependency-closed `pkg/types` leaves owned by
`tidb-datatype`. A source row is `COVERED` only when its complete production
contract executes through the Rust API. Every dedicated original test, when
one exists, must also be complete; broad cross-source tests keep their own
conservative ledger status. Statement-context warning policy remains outside
this representation crate; typed conversion outcomes stop at the same
pre-context boundary.

| Go anchor | Status | Rust evidence | Boundary |
| --- | --- | --- | --- |
| `pkg/types/binary_literal.go:29-235` | `COVERED` | `src/binary_literal.rs`, `src/binary_literal_tests.rs` | Bit/hex syntax, quote trimming, raw bytes, hex/bit rendering, big-endian integer conversion, significant-leading-zero behavior, comparison, and fixed/minimal byte sizing are translated. `BinaryLiteralIntOutcome::Truncated` preserves Go's `MaxUint64` plus truncation disposition without a warning sink. `BinaryLiteralWidth` makes Go's panic-only invalid `byteSize` state unrepresentable by `from_uint`; the rejected-width assertion executes at typed construction. |
| `pkg/parser/types/eval_type.go:19-77` | `COVERED` | `src/eval_type.rs`, `src/eval_type_tests.rs` | All nine byte discriminants, exact display strings, `IsStringKind`, and `IsVectorKind` execute. `TryFrom<u8>` rejects invalid source bytes as `InvalidEvalType`, eliminating the value state whose Go `String` method panics. |
| `pkg/types/eval_type.go:20-41` | `COVERED` | `src/eval_type.rs`, `src/eval_type_tests.rs` | The alias package maps to the same `EvalType` definition and all nine public constants; Rust has no duplicate type or conversion seam. |
| `pkg/types/etc.go` type predicates | `PARTIAL` | `src/field_type.rs` | Exact source partitions for varchar/unspecified/prefixable/fractionable/time/float/integer/stored-integer/numeric/temporal/string and binary/non-binary field metadata execute. `NeedRestoredData` is complete for registered Rust collations; GBK/GB18030 and utf8mb4_0900 collations remain outside this dependency leaf. |
| `pkg/types/fsp.go:15-103` | `COVERED` | `src/fsp.rs`, `src/fsp_tests.rs` | FSP normalization/clamping, fractional parsing/rounding/overflow, and byte-indexed alignment are translated. The public parser consumes bytes because Go strings are arbitrary bytes; this preserves the source boundary without UTF-8 slicing states. |
| `pkg/types/enum.go:24-83` | `COVERED` | `src/enum_set.rs`, `src/enum_set_tests.rs` | Name-first parsing, exact Go base-0 unsigned fallback, binary/general-CI/UCA-4.0 comparison, one-based boundaries, copy/string/number behavior, the zero error sentinel, and `types`/1265 truncation identity execute. |
| `pkg/types/set.go:24-132` | `COVERED` | `src/enum_set.rs`, `src/enum_set_tests.rs` | Name-first parsing, exact Go base-0 unsigned fallback, collation-key deduplication, declaration-order canonicalization, bit decoding, remaining-bit error text, and copy/string/number behavior execute. The Go out-of-bounds panic above 64 value positions is a typed error. |
| `pkg/util/collate/general_ci.go` | `PARTIAL` | `src/collation.rs`, `src/collation_data/general_ci_u16_le.bin` | Byte-preserving compare/key/PAD SPACE/max-key behavior, invalid-UTF8 termination, and all 65,536 effective BMP weights execute; wildcard patterns remain open. |
| `pkg/util/collate/unicode_0400_ci_{generated,impl}.go` | `PARTIAL` | `src/collation.rs`, `src/collation_data/unicode_0400_*.bin` | Byte-preserving compare/key/PAD SPACE/max-key behavior, invalid-UTF8 termination, low-u16-first packing, supplementary fallback, and long-rune expansion execute; wildcard patterns remain open. |
| `pkg/util/collate/ucadata/unicode_ci_data_generated.go` | `COVERED` | `scripts/generate_collation_data.py`, `src/collation_data/unicode_0400_*.bin` | Every UCA 4.0 table and long-rune value is mechanically generated, compared with the retained original fixture, marker-closed, length-pinned, SHA-256-pinned, and regeneration-checked. |
| `pkg/types/binary_literal_test.go:24 TestBinaryLiteral` | `COVERED` | `src/binary_literal_tests.rs::binary_literal_executes_the_complete_original_source_table` | Every original nested table executes: 9 trim rows, 43 bit-parse rows, both empty-input assertions (including the source's duplicated `ParseBitStr` call), 14 hex-parse rows, 4 hex-string rows, 12 bit-string rows, 8 integer rows, 17 byte-size rows plus invalid-size assertion, 4 comparison rows, and both raw-string rows. Additional source-boundary tests cover empty unquoted payloads, repeated quote trimming, non-ASCII rejection without slicing panics, significant-width truncation, and invalid widths 0/9. |
| `pkg/types/fsp_test.go:24 TestCheckFsp` | `COVERED` | `src/fsp_tests.rs::check_fsp_executes_every_original_assertion` | Every original default, invalid-negative, upper-clamp, wide-integer, midpoint, and ordinary precision assertion executes. |
| `pkg/types/fsp_test.go:63 TestParseFrac` | `COVERED` | `src/fsp_tests.rs::parse_frac_executes_every_original_assertion` | Every original empty, invalid FSP, invalid number, scale-padding, rounding, leading-zero, and overflow assertion executes. |
| `pkg/types/fsp_test.go:121 TestAlignFrac` | `COVERED` | `src/fsp_tests.rs::align_frac_executes_every_original_assertion` | Every original positive/negative, short/already-wide byte-alignment assertion executes. |
| `pkg/types/time.go:633-678 ToPackedUint/FromPackedUint` | `PARTIAL` | `src/packed_time.rs`, `rust/crates/tidb-codec/tests/temporal_source.rs` | The eight-byte calendar bit layout, zero value, field extraction, and representable-width errors execute. SQL temporal validation, formatting, FSP/type metadata, timezone conversion, and Duration remain open. |
| `pkg/types/enum_test.go:24 TestEnum` | `COVERED` | `src/enum_set_tests.rs::enum_executes_every_original_test_enum_row_and_assertion` | All 10 defined rows and all 13 collation executions preserve the original assertions. |
| `pkg/types/set_test.go:24 TestSet` | `COVERED` | `src/enum_set_tests.rs::set_executes_every_original_test_set_row_and_assertion` | All 18 defined rows and all 25 collation executions preserve the original assertions. |
| `pkg/util/collate/collate_test.go:57 TestUTF8CollatorCompare` | `PARTIAL` | `src/collation_tests.rs::compare_executes_all_original_binary_bin_general_and_unicode_columns` | All 20 rows and 80 assertions for Binary, utf8mb4_bin, general-CI, and Unicode-CI execute. Four unrelated 0900/GBK columns remain open. |
| `pkg/util/collate/collate_test.go:86 TestUTF8CollatorKey` | `PARTIAL` | `src/collation_tests.rs::key_executes_all_original_binary_bin_general_and_unicode_columns` | All 8 rows and 64 Key/ImmutableKey assertions for the four relevant collations execute byte-exact. Four unrelated columns remain open. |
| `pkg/util/collate/collate_test.go:228 TestCampareInvalidUTF8Rune` | `PARTIAL` | `src/collation_tests.rs::invalid_utf8_executes_all_original_general_and_unicode_assertions` | All 14 general-CI/UCA-4.0 raw-byte assertions execute. Unrelated collators remain open. |
| `pkg/util/collate/ucadata/unicode_ci_data_test.go:23 TestUnicode0400IsTheSame` | `COVERED` | `scripts/generate_collation_data.py --check` | All 65,536 table entries and both values of all 22 long-rune entries are compared to the original fixture. |
| `pkg/util/collate/ucadata/unicode_ci_data_test.go:37 TestAllItemInLongRUneMapIsUnique` | `PARTIAL` | `src/collation.rs::tests::all_uca_0400_long_rune_weights_are_unique` | The complete UCA 4.0 half executes; the unrelated UCA 9.0 half remains open. |
| `pkg/types/field_type_test.go:368 TestAggregateEvalType` | `PARTIAL` | `src/eval_type_tests.rs::string_kind_classifies_every_source_discriminant` | The helper classification asserted by the original test executes exhaustively. The broad test remains partial because `FieldType.EvalType`, `AggregateEvalType`, merge rules, and flag propagation are outside these two source leaves and do not execute in Rust. |
| `pkg/expression/builtin_cast_test.go:1188 TestWrapWithCastAsTypesClasses` | `PARTIAL` | `src/eval_type_tests.rs::string_kind_classifies_every_source_discriminant` | The `IsStringKind` result assertion executes exhaustively. Cast construction, `FieldType.EvalType`, value evaluation, and unsigned propagation remain unported here, so none of the broad cast table is claimed. |

## CommonHandle integration audit

The literal representation is complete, but this leaf alone does not close
the production CommonHandle row. Go reaches the codec through
`types.Datum{KindBinaryLiteral}` and `codec.Encoder.encode`, which normalizes
the literal through `ToInt(StrictContext)` before unsigned key encoding.
Rust's shared `Datum` currently has no BinaryLiteral variant; adding one is an
exhaustive workspace migration across expression and executor coercion, not a
codec-local switch. A standalone `encode_binary_literal_key` helper or a
fixture-normalized `Datum::UInt` would bypass that production path, so neither
is introduced here. The transaction source test must remain `PARTIAL` until
the Datum steward lands that source-backed migration atomically.

## Source-owned sparse evidence rows

Each datatype source wave owns matching sparse fragments under
`difftests/corpus/coverage/evidence/{source,tests}/`; the current owners are
`datatype-binary-literal-source-wave.tsv`,
`datatype-enum-set-collation-source-wave.tsv`, `datatype-eval-type-source-wave.tsv`,
and `datatype-fsp-source-wave.tsv`.
The ledger rejects duplicate anchors across fragments, so the CommonHandle
`PARTIAL` row must have only one owner:

```text
pkg/types/binary_literal.go\tCOVERED\tdatatype-binary-literal-source-wave\trust/crates/tidb-datatype/src/binary_literal.rs\tComplete bit and hexadecimal literal parsing rendering raw bytes big-endian integer conversion comparison and typed byte-width contract execute; truncation remains a typed pre-statement-context outcome and the Go invalid-width panic state is eliminated
go_test\tpkg/types/binary_literal_test.go\t24\tTestBinaryLiteral\tCOVERED\tdatatype-binary-literal-source-wave\trust/crates/tidb-datatype/src/binary_literal_tests.rs\tEvery original trim bit parse hex parse render integer byte-size comparison and raw-string row executes including the duplicated empty bit parser assertion and typed replacement for the invalid byteSize panic
go_test\tpkg/kv/key_test.go\t240\tTestCommonHandlesFitIntHandleRange\tPARTIAL\tdatatype-binary-literal-source-wave\trust/difftests/transaction-tests/tests/handle.rs\tSix source rows execute through production Datum and codec encoding; the BinaryLiteral row is byte-exact only through the Go fixture and normalized UInt decoder until the shared Datum kind and codec dispatch land atomically
```
