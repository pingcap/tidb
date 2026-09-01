# `pkg/dumpformat/parquetfile` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly 20 tracked artifacts: 16 production/generated
files, seven Go test/benchmark files, and two binary Parquet fixtures. The Go
text sources total 9,567 lines; the fixtures are 2,686 and 434 bytes. Every
tracked artifact was read or inspected in full in a detached worktree at the
pinned Go commit before editing. There is no `doc.go`, `OWNERS`,
platform-specific build variant, or generator input checked into this package.

| artifact | lines/bytes | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 84 lines | `a3cb7626e353a9a805d49a46a33c0e889489bd4c` | `6b1aeab60fc544b12b504788c25c71bddb527267c31a2853e072afd08fd56945` | public library, 29-shard flaky test target, fixture data glob |
| `column_buffer.go` | 85 lines | `c25708cbe5a64f4669a34ebdf7b5cafb03c476bc` | `b74b3d5ba2905ff312e0ac5faba2d092f0ce702d21dc2e53a62615104c1b102f` | typed row-group buffers, reset, allocation/physical-type validation |
| `column_type.go` | 130 lines | `c251187e6bd6d863a4bb0abd4e328bbcfb783d40` | `22392f7612e479efd949d8d18f02db1ce1a80a12435521d15c6315fb3a599621` | SQL-to-Parquet physical/logical mapping and decimal-width calculation |
| `column_value.go` | 268 lines | `f8a36ffb2a779636490128e376d6ddbd59125e37` | `bcbcbed089ea820131beafe834d83d188a43a1d3e67bfec09d0e968b07a3a7ff` | raw-value parsing, decimal scaling, timestamp/byte conversion, typed batches |
| `parser.go` | 971 lines | `0f6e0827ca89d7803a88aaa5f3e3df6d5956284c` | `92d48da1e000cefe698192c4a2ec556e23d2d796855071ef2eee0618171e8dc1` | Parquet schema validation, iterators, row-group parser, object-store readers, memory estimation |
| `reader_wrapper.go` | 310 lines | `98d3256573493b7bde9e35202770224d529a7c08` | `220cce8363420bc34323b98d37a1a2a90cad87867280ee7188869b83cfffa7c6` | seek/read adapter, row-group range calculation, preload and streaming strategies |
| `schema_builder.go` | 111 lines | `e54ddeb1223533d964b1addb3ee68cb6644db737` | `026db9aa2b67a01a862e2de78fbb0b4bbe8361d13c6e864fd79867b8289a017e` | column metadata validation and primitive Parquet schema construction |
| `spark_rebase.go` | 314 lines | `232080ac6459cbaae8672cc13d4ee52c646fc499` | `087dcb21d4c7c0380f92b34677c0ef2e37d4a0e2aaca44181fc836cd9dd9128f` | Spark legacy calendar/timezone detection and Julian/Gregorian rebasing |
| `spark_rebase_micros_generated.go` | 3,108 lines | `2d1a4336162163eaa3199de102f810ce48daa2a8` | `4952de3695a4d783370197c9c5504127ec8dc02a6b6b46788329692d65a2334b` | generated Spark 3.5.7 timezone switch/diff/record tables; DO NOT EDIT |
| `type_converter.go` | 492 lines | `0e97d91671d95e59b45fafe136e3b9b4a0ff1dcd` | `5fa50d52c7130a6b6da929f46f498e6e069a5be89293518645fa6964429d67b4` | Parquet physical/logical setters, decimal bytes, time/timestamp and INT96 conversion |
| `writer.go` | 346 lines | `d194b6be9819465194f71f900c6513767e5395bc` | `366e2a74b29681c0941b281537f109a0dff29f6442b9490d12b34c1e5fae2de4` | SQL-row Parquet writer, options, memory-limited row-group flushing and byte accounting |
| `benchmark_decimal_test.go` | 101 lines | `08004bcc3ece673b8892ac4727590b375dc3a680` | `20027dac727ff9bb3cce0e17a09f3cef9e65c8a28bc607af3b80da98cfdcd258` | decimal conversion benchmark: native, new string, and old string paths |
| `column_type_mapping_test.go` | 148 lines | `e3bb1b6f49c87c661f4a009e4ee16cacd5e3101a` | `cd6273547de03c99d7db10875132e52357b9055bb6175986d03479d7520123e1` | two mapping/precision tests with nested cases for SQL type families |
| `column_value_conversion_test.go` | 318 lines | `58a94d1988e895c1e9e2886f1bf760a5b63fffae` | `977c18c5523bc1045c304c134afb6dbe5314e90afcc8d1d55bc9c63cfb3dbede` | five decimal/raw-value/physical-buffer tests and error branches |
| `parser_test.go` | 2,153 lines | `8c547b02ca0c46db39140bdbaf5d36512035bd18` | `ab7cd42db5910d66aafb844591d18699f5e0cab15530caeaaa45e3b3aee8023b` | 16 parser tests, two benchmarks, cloud/preload, type, Spark, decimal, and allocator coverage |
| `writer_behavior_test.go` | 382 lines | `c68b8810d4d920cf4023aebb59f71583f8e2b6c0` | `9e15303542ffd612525a80240c377be86e65c8c99944f42ef0f30ca20b907ffa` | three writer tests, one benchmark, and nested flush/error/option cases |
| `writer_core_test.go` | 155 lines | `d145584b9b920a59483c10be4c2a70526f79734a` | `0cc542ad761c180cded076c9ce644cdb1313ca37c52a0b56ebfa9821266b7c32` | two end-to-end writer/schema tests |
| `writer_test_helpers_test.go` | 91 lines | `a979c0955ccbb223db703d5339cfbb8403ff8350` | `ed000f04d1805b79f45a75d139c7e2f4c016788af93feb6403c14090b424c5b1` | typed Parquet readback helpers used by writer tests |
| `testfiles/aurora_snapshot.parquet` | 2,686 bytes | `3e5c4897897fc0530e9e9368c6dddd7100bb879f` | `9c5d40bed5522616707446e94e69aa8b4b1e56d69ace4354623c67c8fc4516e0` | Aurora decimal/type parser fixture |
| `testfiles/hive_dump.parquet` | 434 bytes | `ae8a5001bc2b31b67f1a3edab824c8e55ec78cfa` | `21766c1a46ccc9c2f417ca1972f9169dabe96063637aac6cf885eebac532dc03` | Hive timestamp parser fixture |

The production functions were audited across all files: schema/type mapping,
typed buffers and value conversion; Parquet writer options, row parsing,
flush/close/size accounting; reader seek/read, preload/streaming and row-group
range logic; parser iterators, row lifecycle, positions, columns, metadata,
sampling and memory allocator; Spark version/timezone lookup, generated-table
slicing, Julian/Gregorian date and microsecond rebasing; and all typed datum
setters. The test inventory contains 29 `Test...` functions, three benchmarks,
and their nested cases: SQL/Parquet type mapping, decimal and physical-value
conversion, all row-group and cloud-reader modes, logical/null/repeated type
rejection, timestamp/time/INT96 semantics, Spark legacy metadata and timezone
tables, dictionary decimal decoding, Aurora/Hive fixtures, large-page memory
limits, writer options/errors/close recovery, and allocator behavior.

## Rust ownership and parity decision

The Rust workspace has no Arrow/Parquet dependency, `dumpformat` crate,
Parquet parser/writer call site, Spark rebase table, or object-store reader
owner. Existing SQL parser, row codec, and generic SQL escaper modules do not
own this package's Parquet schema, physical/logical conversion, cloud range
reads, generated Spark compatibility data, or fixture contracts. No Rust-only
behavior was found to remove, and no speculative Parquet implementation or
ignored test carrier was added. The generated table is retained as an
upstream-generated Go artifact and was not hand-edited.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
Go-master package suite passed with failpoints enabled and disabled by the
repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dumpformat/parquetfile -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dumpformat/parquetfile 1.114s
```

Repository format, lint, and diff hygiene were also run for this receipt
batch (`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all
-- --check`, `make lint`, and `git diff --check`). No Go source, import
section, test, Bazel target, or module dependency changed; `make bazel_prepare`
is not required. Rust tests and a full workspace build were not run because
this package has no Rust owner or changed Rust source. Parquet compatibility,
Spark historical-calendar correctness, cloud range-read behavior, generated
table provenance, and fixture interoperability remain unverified on the Rust
side; this receipt records the explicit ownership boundary rather than
claiming transcreated parity.
