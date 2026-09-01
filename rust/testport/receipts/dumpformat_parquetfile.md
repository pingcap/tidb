# `pkg/dumpformat/parquetfile` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

This package is newer than the hparser branch in several places, so source
validation was performed in a detached worktree at the comparison commit.

## Complete inventory

The package contains exactly 20 tracked artifacts and 9,586 lines (18 text
sources/tests/build files plus one generated Go source and two binary Parquet
fixtures). Every production function, test/helper, import, BUILD target,
generated table, and fixture input was read or inspected before editing. There
is no `doc.go`, platform-specific source variant, fuzz corpus, or generator
input beyond the checked-in Spark-generated table.

| artifact | lines | bytes | Git blob | SHA-256 |
| --- | ---: | ---: | --- | --- |
| `BUILD.bazel` | 84 | 2,780 | `a3cb7626e353a9a805d49a46a33c0e889489bd4c` | `6b1aeab60fc544b12b504788c25c71bddb527267c31a2853e072afd08fd56945` |
| `benchmark_decimal_test.go` | 101 | 2,621 | `08004bcc3ece673b8892ac4727590b375dc3a680` | `20027dac727ff9bb3cce0e17a09f3cef9e65c8a28bc607af3b80da98cfdcd258` |
| `column_buffer.go` | 85 | 2,882 | `c25708cbe5a64f4669a34ebdf7b5cafb03c476bc` | `b74b3d5ba2905ff312e0ac5faba2d092f0ce702d21dc2e53a62615104c1b102f` |
| `column_type.go` | 130 | 5,309 | `c251187e6bd6d863a4bb0abd4e328bbcfb783d40` | `22392f7612e479efd949d8d18f02db1ce1a80a12435521d15c6315fb3a599621` |
| `column_type_mapping_test.go` | 148 | 6,579 | `e3bb1b6f49c87c661f4a009e4ee16cacd5e3101a` | `cd6273547de03c99d7db10875132e52357b9055bb6175986d03479d7520123e1` |
| `column_value.go` | 268 | 9,247 | `f8a36ffb2a779636490128e376d6ddbd59125e37` | `bcbcbed089ea820131beafe834d83d188a43a1d3e67bfec09d0e968b07a3a7ff` |
| `column_value_conversion_test.go` | 318 | 10,673 | `58a94d1988e895c1e9e2886f1bf760a5b63fffae` | `977c18c5523bc1045c304c134afb6dbe5314e90afcc8d1d55bc9c63cfb3dbede` |
| `parser.go` | 971 | 26,157 | `0f6e0827ca89d7803a88aaa5f3e3df6d5956284c` | `92d48da1e000cefe698192c4a2ec556e23d2d796855071ef2eee0618171e8dc1` |
| `parser_test.go` | 2,153 | 68,363 | `8c547b02ca0c46db39140bdbaf5d36512035bd18` | `ab7cd42db5910d66aafb844591d18699f5e0cab15530caeaaa45e3b3aee8023b` |
| `reader_wrapper.go` | 310 | 8,624 | `98d3256573493b7bde9e35202770224d529a7c08` | `220cce8363420bc34323b98d37a1a2a90cad87867280ee7188869b83cfffa7c6` |
| `schema_builder.go` | 111 | 3,375 | `e54ddeb1223533d964b1addb3ee68cb6644db737` | `026db9aa2b67a01a862e2de78fbb0b4bbe8361d13c6e864fd79867b8289a017e` |
| `spark_rebase.go` | 314 | 12,181 | `232080ac6459cbaae8672cc13d4ee52c646fc499` | `087dcb21d4c7c0380f92b34677c0ef2e37d4a0e2aaca44181fc836cd9dd9128f` |
| `spark_rebase_micros_generated.go` | 3,108 | 393,170 | `2d1a4336162163eaa3199de102f810ce48daa2a8` | `4952de3695a4d783370197c9c5504127ec8dc02a6b6b46788329692d65a2334b` |
| `testfiles/aurora_snapshot.parquet` | 12* | 2,686 | `3e5c4897897fc0530e9e9368c6dddd7100bb879f` | `9c5d40bed5522616707446e94e69aa8b4b1e56d69ace4354623c67c8fc4516e0` |
| `testfiles/hive_dump.parquet` | 7* | 434 | `ae8a5001bc2b31b67f1a3edab824c8e55ec78cfa` | `21766c1a46ccc9c2f417ca1972f9169dabe96063637aac6cf885eebac532dc03` |
| `type_converter.go` | 492 | 15,163 | `0e97d91671d95e59b45fafe136e3b9b4a0ff1dcd` | `5fa50d52c7130a6b6da929f46f498e6e069a5be89293518645fa6964429d67b4` |
| `writer.go` | 346 | 10,015 | `d194b6be9819465194f71f900c6513767e5395bc` | `366e2a74b29681c0941b281537f109a0dff29f6442b9490d12b34c1e5fae2de4` |
| `writer_behavior_test.go` | 382 | 12,497 | `c68b8810d4d920cf4023aebb59f71583f8e2b6c0` | `9e15303542ffd612525a80240c377be86e65c8c99944f42ef0f30ca20b907ffa` |
| `writer_core_test.go` | 155 | 6,085 | `d145584b9b920a59483c10be4c2a70526f79734a` | `0cc542ad761c180cded076c9ce644cdb1313ca37c52a0b56ebfa9821266b7c32` |
| `writer_test_helpers_test.go` | 91 | 3,485 | `a979c0955ccbb223db703d5339cfbb8403ff8350` | `ed000f04d1805b79f45a75d139c7e2f4c016788af93feb6403c14090b424c5b1` |

`*` Binary Parquet files have no meaningful source line count; the displayed
values are newline counts returned by Git for inventory consistency.

The production files declare 126 functions/methods covering SQL-to-Parquet
type mapping, decimal/timestamp/time/INT96 conversion, Spark Julian/Gregorian
rebasing, schema validation, row-group and column readers, streaming and
in-memory object-store wrappers, memory tracking, and Parquet writing. The
test files contain 32 top-level tests/benchmarks plus 13 helper functions. The
BUILD target includes ten production sources, seven test sources, two fixture
files, and a 29-shard flaky short test target. The generated source is Apache
Spark 3.5.7 rebase data and is explicitly not hand-editable.

## Go-master behavior and validation

Go master adds or changes logical-type validation, repeated-field rejection,
whole-file/row-group/per-column preload strategies, proportional scanned-byte
reporting, context-aware reader cleanup, Spark timestamp/date rebasing, unsigned
and decimal conversion, page streaming, row-group memory accounting, writer
flush/error recovery, and the corresponding source regressions. The exact
detached Go-master failpoint-managed suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dumpformat/parquetfile -count=1
# PASS; ok github.com/pingcap/tidb/pkg/dumpformat/parquetfile 1.261s
# failpoints disabled during teardown (refcount returned to zero)
```

## Rust ownership and parity decision

Rust has parser recognition for `LOAD DATA ... FORMAT PARQUET` and a
`tidb-datatype` decimal-from-Parquet helper, but no dependency-closed
Arrow/Parquet reader, writer, object-store range reader, Spark rebase table,
or Lightning importer owner. Those isolated leaves cannot own this package's
schema, row-group, SQL temporal, memory, and fixture contracts. No Rust-only
behavior was found to remove, and no speculative Parquet crate or ignored
carrier was added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. Rust formatting,
repository lint, and `git diff --check` were run for the surrounding receipt
batches. No Go source, import section, test, Bazel target, or module dependency
changed in the hparser branch; `make bazel_prepare` is not required. Rust
Parquet behavior, full Bazel shards, generated-data regeneration, and a full
Rust workspace build remain unverified because no Rust owner exists. Runtime
correctness, compatibility, and performance risk are unchanged; this receipt
is an explicit cross-crate boundary, not a completed Rust transcreation claim.
