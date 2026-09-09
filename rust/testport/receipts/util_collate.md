# `pkg/util/collate` — current Go-master package parity receipt

Go authority: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
unchanged from the earlier extraction authority
`e2788410d8d696605e8cb002585877a063ccc909`; this receipt refreshes the
inventory and Rust-owner evidence at the current Go pin.

## Complete Go inventory

All 35 files in the package boundary were read or structurally inspected
before ownership review. The inventory includes production, test, benchmark,
test harness, generated output, generator input/template, embedded data, and
all four Bazel targets. The boundary contains 141,124 logical lines and
11,543,224 bytes (the embedded GB18030 table is binary data represented as
74,424 `wc -l` records).

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 55 | `b593596c425d4ec1af085e876cbd9300192f2b9e` | `cdc245a8bba1d8336b6475d685b0c1a9565ef6926da88c66ffc5fd018521579a` | collate library/test targets, embedded data, and dependencies |
| `bin.go` | 137 | `f8bf6f123596697b7adb31dca5d452a9edaf5703` | `2a64d865d89a811e1b165ab9acaa187010b66700b1ba75e4b41ea6cf3e20f1c3` | binary/padded/derived collators and wildcard patterns |
| `charset.go` | 32 | `1015a026326e82eef1bff8ab90e8344c2fa11fab` | `13471972c0f0dfd55d9619f6230b00ca78f02dd13318a3fdf20171295e32d327` | charset conversion helpers and test registration |
| `collate.go` | 478 | `8158850bf4b515e3893ca1d1a4f6716f41d609b3` | `4375711c6df648b6c89539e022b8ca39861f280a968113fa87d5ce3f22408aaf` | registry, mode switch, protocol IDs, helper predicates, and common comparison |
| `collate_bench_test.go` | 243 | `0d6896fceab71a96d61c68e4d936808aba0ed360` | `35ae3c34eed9557a2bb2670b4dcc8ccecf1812501566d25f6c138e84c39b2724` | 45 compare/key/immutable-key benchmark functions |
| `collate_test.go` | 250 | `3987022b7df8e1350f26536085fb8ed2561e84d3` | `61c37b3eaaca00f4fe15faef033c6c48e18b210509c870046846a81f62639d4f` | six source tests for comparisons, keys, mode, IDs, lookup, invalid UTF-8 |
| `gb18030_bin.go` | 123 | `f8b5a06bdcc32c9a1975929c716db44bb93cd9c8` | `225f10e47d785020fa5b2d8132e2825ae9eee40d6e31f3f0011f84d02f56d7ad` | GB18030 binary encoder, four-byte PUA overrides, and keys |
| `gb18030_chinese_ci.go` | 120 | `1ddf285822f89eb63025e6beb2444ce13638ac5f` | `c0f1accbd56a6b39fcb7c40c95aa189c42aa31c6b38d2ee6bf2a9c87312f1be2` | GB18030 Chinese-CI comparison, keys, and patterns |
| `gb18030_weight.data` | 74,424 | `90a8971c213c5f858e3f0e81797466c5998c90fe` | `64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d` | 0x110000-entry embedded little-endian weight table |
| `gbk_bin.go` | 106 | `ecbe937c9ca0574a44fefd23fd932da896edf47f` | `0d64b45b82c7e2f641bc312d0b43ca494d82ba1bb27ba2bf4d2a1b274b57109a` | GBK binary encoder, keys, and rune wildcard pattern |
| `gbk_chinese_ci.go` | 104 | `623c8d16401e01d1947ff0927635448a8ce19acb` | `09d04b7c25da5649c906552741a377132d7682bc73026f0d96e808637ab57657` | GBK Chinese-CI comparison, keys, and patterns |
| `gbk_chinese_ci_data.go` | 278 | `01636cc3354cda84ab00182144ea6c45b310d34d` | `f4c81f9fbf27469f4dc2b7add68c315bbc399869f63efdf059142eddb2542dc1` | generated 65,536-entry GBK sort-key table |
| `general_ci.go` | 335 | `2afc1c12651d608c0c6667c7c3d0b02c08720f5e` | `5097347e7c43db70f732a75a1966a467dec511218e1ba883590af3780dc3317f` | generated General-CI planes and collator |
| `main_test.go` | 33 | `0d17190795229a3f365095c1d0cdda4778ec0532` | `0728ac75484bdaba7d97bd54b72bc3526a3413a3f65f6caca968ceacba2e69e4` | `TestMain` goleak/test setup |
| `pinyin_tidb_as_cs.go` | 54 | `a98f447700a35c1404de4608b0f79e1acd22d570` | `8d056fa9529c5b3c0c1dbdc370867b2c6100b363f699dc58e9957215d94736ba` | reserved source stub; every operation intentionally panics `implement me` |
| `unicode_0400_ci_generated.go` | 154 | `18d212f3500067c91fd9511e3aa260d5c3389c74` | `ba18743321c946eb7a790ed2adec69b8e594b23cd454ec27882c643acb5f6720` | generated UCA 4.0 collator plumbing |
| `unicode_0400_ci_impl.go` | 82 | `16c5656f9fab3bf5caf61f79eaf5ac6174a7ab95` | `ed121c19fc78a362cf4d65021e884ef3be0c264b264577fee8c7cdfd3cf42621` | UCA 4.0 weights and pattern implementation |
| `unicode_0900_ai_ci_generated.go` | 154 | `418d7b036aaa18a07473feee8b24d1b736bda3cf` | `d8c59356d57168d7ce63f6ccdab55c387c1ee53208b0fcc25d7c7a64a661924b` | generated UCA 9.0 collator plumbing |
| `unicode_0900_ai_ci_impl.go` | 73 | `46aab8822bf263c5dd26ac1064cc5e069f2fe8bb` | `b61756fab205b22bd37dd9c9536e072ff314b7ce22d89b119edbd5f9285f7075` | UCA 9.0 weights and patterns |
| `ucadata/BUILD.bazel` | 25 | `2f3fdb1a4331ce5bd8c277809f9b31fe9d2a745b` | `67b3e06d953b79600f85572659bc17cbdfb9511a97336842447cb2ed19686221` | generated UCA data library/test target |
| `ucadata/data.go` | 23 | `abe2d1610f510555d9a7be5537dee0e3edc78fe8` | `4f84db15f659d76cc7495e425c977418f360df66e2ba8ce1b41aaf9a32cf0ace` | UCA constants and two `go:generate` entrypoints |
| `ucadata/unicode_0900_ai_ci_data_generated.go` | 12,321 | `f8d012e0c55fab10e969dde97e32460881d4690e` | `bda4f630ef8a9cc74eeb0a8eda96ae3d3bdfa7bf880c936a9089a9556fee8bfc` | generated UCA 9.0 table and long-rune map |
| `ucadata/unicode_0900_ai_ci_data_test.go` | 34 | `ae7f266c1866cfcf1dd9e32f46f0eebaf71e1112` | `a19423e1a2a7b426fe034fa783934c06ce28957618f3592ba661f9d89073ea2f` | Hangul Jamo and nonzero-long-weight invariants |
| `ucadata/unicode_ci_data_generated.go` | 4,421 | `f009defdedf6e7aaff9f4b931977788d1675b6c5` | `0afd27e5c0456df24433f01925887609d7a2077323a44967e24945641c82db54` | generated UCA 4.0 table and long-rune map |
| `ucadata/unicode_ci_data_original_test.go` | 395 | `9b49ff0bf2bb8d49adefd10499bad2f066439ff6` | `dc2c396a197f2b68cd2c16821cb4fbdb7177fae47ae34af5ab67d0200218ffc4` | retained original UCA 4.0 fixture |
| `ucadata/unicode_ci_data_test.go` | 59 | `105f59d0967ad6a531b7cc656c4d96e35210a3ff` | `a6a89a7f7ecbc9d3e0b726c99ae4dbbba9198ff3fcbe4f7649719e2a0ba35cea` | UCA table equality and long-map uniqueness |
| `ucadata/generator/BUILD.bazel` | 22 | `619d15ffb9bde5aa976c9df381b3c2868e58427d` | `28d2ffbd9eb5b73c1ec69b94c666245dee23b29d1841e5ff31c3a98c6a1f185e` | generator library/binary target and embedded inputs |
| `ucadata/generator/allkeys-4.0.0.txt` | 15,169 | `3b75f829d8b2c3540d8a9d9bb149326247fdba15` | `866600028db037b68cb1007f5a36d3c8dd1b4884db80276b44a02cf52085885d` | Unicode 4.0 DUCET input |
| `ucadata/generator/allkeys-9.0.0.txt` | 30,699 | `9c92b5e2ac7c0a9bb6c4f8162cf76a510a3015f` | `0633f4520c99f249b0c53aa1442cd2521702041fb00a32df944fec13c9da3ed5` | Unicode 9.0 DUCET input |
| `ucadata/generator/data.go.tpl` | 31 | `3d8e0b3222c18f2f5c5f01fbfd45e42e61415e9b` | `0f811fd1b5525608df65a614b582a29b3e2e1f63e0486de0c7253c8d838a862f` | generated Go UCA table template |
| `ucadata/generator/magic.go` | 38 | `7e7f756e7f0a6b91915ce0d8a74bf7bbdcf35803` | `a9118cee3680138226756842f1c5f5632d2753ca4281033510bbc728660bb7eb` | parser hex table and long-rune marker |
| `ucadata/generator/main.go` | 421 | `d33547e8ddfc7b0fea404ee85e01b8fc4120c426` | `a487123185300687b319351c9713c0f1901e97336f8ed098ee9838b2b4908c4e` | DUCET parser, implicit weights, and generator entrypoint |
| `ucaimpl/BUILD.bazel` | 15 | `fc48a5930bef7d989e14efef713e0a5fb460a85c` | `70b167d145f3bf3a2e74c824401b2db4d7890e889789192eba2f5f31012808ac` | collator implementation generator target |
| `ucaimpl/main.go` | 62 | `83c4e9fabc2bc6d968c85e096a6d0fed44f0ba05` | `7c028deabab76b6312002ad40fe1778212486f6d281c986e5bb2af67ec15e5e5` | UCA implementation generator entrypoint |
| `ucaimpl/unicode_ci.go.tpl` | 154 | `101d68aa355e5edb089b263cd31f76fb2392af22` | `2c9022e947e4f6dd96f2cab18ac4f88a2f258d6fbb4bd08d8ed3b87249da870c` | inlined Go UCA compare/key template |

The root source surface has 14 production/generated files, six source tests
including `TestMain` (the benchmark file has 45 benchmark functions), one
embedded binary table, and no build tags, failpoints, fuzz tests, examples, or
platform-specific variants. The nested `ucadata` and `ucaimpl` directories
are generators/data rather than additional runtime packages with omitted
tests.

## Rust ownership and parity decision

The dependency-closed owner is `rust/crates/tidb-datatype`: `charset.rs`
provides the shared charset/collation registry and mode defaults, while
`collation.rs` owns all 16 implemented collations, wildcard matching,
comparison, sort keys, protocol ID conversion, and PAD SPACE helpers. The
source-derived `collation_tests.rs` covers all six Go test functions and adds
focused checks for borrowed immutable keys, invalid byte patterns, GB18030
PUA key padding, surrogate-marker zero values, and helper/registry behavior.

The seven generated Rust binary images under
`src/collation_data/` are lossless little-endian conversions of the Go
General-CI, UCA 4.0/9.0, GBK, and GB18030 authorities. The Rust generator
`scripts/generate_collation_data.py` parses those Go sources, verifies the
retained original UCA 4.0 fixture and long-map invariants, and checks exact
table dimensions. The Go DUCET and `ucaimpl` generators therefore have one
executable Rust generation gate, not a second hand-maintained authority.

Other Rust-side generated/platform artifacts inspected for this owner are the
parser charset tables in `src/charset_data/{collations,gb18030_by_bytes,
gb18030_by_rune,gb18030_cases,gbk_cases,known_charsets}.rs`; these are the
existing generated inputs for charset lookup and encoding and are not edited
by the collate owner. The collate benchmark is preserved in
`benches/collate.rs`; Cargo's `autotests = false` and aggregate `tests/all.rs`
include the source-derived integration tests.

The source `pinyin_tidb_as_cs.go` is intentionally a stub: its operations
panic with `implement me`, and Rust preserves that exact behavior with a
regression test. No Rust-only collation behavior or missing Go behavior was
found in this re-audit; no production, generated, test, or build artifact was
changed.

## Validation

Profile: Ready for this package boundary. This batch is evidence-only; the
Rust implementation and Go sources were unchanged.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/collate/... -count=1` — passed.
- `python3 rust/crates/tidb-datatype/scripts/generate_collation_data.py --check` — passed; all seven images match Go authorities.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --test all -- --test-threads=1` — passed (61 aggregate integration tests).

`make bazel_prepare` is not required: no Go source/import, Go test function,
Bazel file, or module dependency changed. No failpoint enable/disable is
needed because this package has no failpoint dependency.

## Risks and unverified scope

- Correctness: the source-derived Rust tests and generated-image checks cover
  the implemented collation families, invalid UTF-8 handling, wildcard
  semantics, and encoding-specific keys. The intentionally unimplemented
  pinyin collation remains a source-compatible panic.
- Compatibility: Go-only interface integrations and logger side effects are
  not part of this package; charset registry/encoding tables remain shared
  Rust dependencies.
- Performance: no runtime code changed; Rust keeps the generated fixed-width
  tables and typed cursor implementation rather than adding dynamic dispatch.
- Not verified locally: Go benchmark execution, Bazel test execution, every
  downstream SQL/ranger consumer, and Windows-specific behavior.
