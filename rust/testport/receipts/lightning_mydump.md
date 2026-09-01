# `pkg/lightning/mydump` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`). The comparison target is the Go package at that SHA; the
working branch is `hparser-integration`.

## Complete inventory

The package has exactly 37 tracked artifacts. The source and fixture inventory
contains 34,011 text lines plus one 43-byte compressed fixture. All Go
production, generated, test/support, fixture, and BUILD/platform inputs were
read and counted from the pinned tree before editing. There is no tracked
`parquet/` fixture even though the BUILD glob permits one.

| Go artifact | Lines/bytes | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 120 lines | `9be5c60070238c4d207f750c3547eebf9191043f` | library sources, `csv/*`/`examples/*`/`parquet/*` data, 50-shard tests |
| `bytes.go` | 39 | `976be0ac3d3576447936f13f3b9e4814c7e354b9` | byte-size helpers |
| `charset_convertor.go` | 136 | `80a224e2c066560ef8876ba311f14c01da06276c` | charset conversion |
| `charset_convertor_test.go` | 85 | `5bcea5d46565dbcb716166d7e78a54d24f73b2ab` | charset tests |
| `csv/gb18030_test_file.csv` | 3 | `35296367e131a2f185709a9566742f7fd2cef3f6` | GB18030 fixture |
| `csv/split_large_file.csv` | 5 | `7b6512d538f6ac7a28cfe11bd6a0397980df4347` | split fixture |
| `csv/split_large_file.csv.zst` | 43 bytes | `9609230bf04a5bb1a8584f6cf2d3a905d2820c4a` | compressed split fixture |
| `csv/utf8_test_file.csv` | 3 | `f3fde44b88f0ef6bebc7f0d5173cc9e87f115fb1` | UTF-8 fixture |
| `csv_parser.go` | 728 | `20d40539c7c386d98b23b01171fcc16f46a500a0` | CSV parser and unescape logic |
| `csv_parser_test.go` | 1,731 | `7949fe9e8cab4f96b318748108b24757edc96aa0` | CSV behavior and benchmarks |
| `examples/metadata` | 2 | `ce9e92da10045a9e1c9d363fdc7f3f7fde320ae8` | metadata fixture |
| `examples/mocker_test-schema-create.sql` | 1 | `240afe8eceb829e1c47573d4b358ad0544577e9c` | schema fixture |
| `examples/mocker_test.i-schema.sql` | 6 | `59a8ef3053ed89287a976f6b3c9c113ad81963a3` | schema fixture |
| `examples/mocker_test.i.sql` | 1 | `b7f1b005810a15492f49ae49cf6d41350fbbf09b` | data fixture |
| `examples/mocker_test.report_case_high_risk-schema.sql` | 8 | `f01882759381164b2fe69b2b47670c23bc9b7c0d` | schema fixture |
| `examples/mocker_test.report_case_high_risk.sql` | 1 | `27c7121e2e7ee7a92e251f7dd5d03d27c6d73d6b` | data fixture |
| `examples/mocker_test.tbl_autoid-schema.sql` | 8 | `ca8e1d72dd5c998c680847148b1e1d8d240b3cff` | schema fixture |
| `examples/mocker_test.tbl_autoid.sql` | 10,009 | `4d98b04b467d77a9d3c1ed2c97cfeeeafe7931a1` | data fixture |
| `examples/mocker_test.tbl_multi_index-schema.sql` | 9 | `bc7a3c103dee3971508ebedfdb62e8e84f076e03` | schema fixture |
| `examples/mocker_test.tbl_multi_index.sql` | 10,009 | `bed1e4b876d1bfe959bbb74ce86523c6ea633c6a` | data fixture |
| `loader.go` | 1,013 | `bcdac4ae25bf426ffdc962ed0c1414e99d40760b` | dump loader and routing |
| `loader_test.go` | 1,372 | `2dfb2870da09d4081a42d71d9dde5bd389c09ec4` | loader/router tests |
| `main_test.go` | 34 | `1dc76d3bc1255c382b2cc6c1835b99e2471442a5` | TestMain harness |
| `parser.go` | 736 | `fee5f372e4b0d171b2a377d07577658a59de2a85` | chunk parser and reader opening |
| `parser.rl` | 188 | `edd2643a7362202891e7d1cf647bdb3b5e8278c7` | Ragel generator input |
| `parser_generated.go` | 2,516 | `afb0602d822aa07f265760a2c61404724b07083f` | generated Ragel parser output (not hand-edited) |
| `parser_test.go` | 881 | `17ed4eef8d08156ec6c281c34e35a8a4220626a` | parser tests |
| `reader.go` | 217 | `ca7bfbecb1c57f9f09aca15fc4f064641c8326a7` | pooled/read-seek readers |
| `reader_test.go` | 279 | `5a1a073ff4dcee3911e23385fb298d1649d6b78b` | reader tests |
| `region.go` | 608 | `93e5245ad34741c224de7e1ede0f6da769c7c3f6` | file regions and splitting |
| `region_test.go` | 641 | `0702e1ef27a5f6a79a90c7d3ea19c5a2c887d261` | region tests |
| `router.go` | 443 | `d7b2bc7cad4d14f19ac610111dbb847c61ce0b24` | table/file routing |
| `router_test.go` | 328 | `0b0f4eb8fb6d1de67739003e09da0d449ee4841b` | routing tests |
| `schema_import.go` | 520 | `b0ba9b239a61f19c404131218a6bf6b90d3ec84c` | schema import |
| `schema_import_test.go` | 586 | `c51f5b283378fe7235e4ce46356722dd53d8ab3f` | schema import tests |
| `view_import.go` | 388 | `447d153511c6beded7b2bb807725e26a0481cafb` | view dependency/import ordering |
| `view_import_test.go` | 352 | `a2b989ba3c704cb016ca8957fa609c8ffe2b9766` | view import tests |

The eleven Go production files contain 162 function/method declarations,
including the generated parser entry point. The ten test/support files contain
116 `TestXxx`/`BenchmarkXxx` declarations (including `TestMain`), covering CSV,
loader, parser, reader, region, router, schema, view, and charset behavior.
`parser.rl` is generator input for `parser_generated.go`; the generated output
was inventory-only and was not edited. No platform-specific Go variant,
additional fuzz corpus, package doc, or unlisted build artifact exists.

## Go-master delta and implementation

Compared with the pre-change `hparser-integration` source, Go master removes
the regexp-based CSV/chunk unescape path and replaces it with a byte scanner,
uses the standard `io.ReadSeekCloser` contract, and adds `NewReaderOpener` so
Parquet can defer opening while non-Parquet inputs open eagerly. The branch now
matches those three production files byte-for-byte with Go master. The source
regression for a regexp metacharacter escape byte (`*`) and the
`BenchmarkCSVParserUnescape` matrix were added to the existing CSV suite.

Go master also migrates `view_import` to `ast.InPlaceVisitor` and `ast.Walk`.
This branch's parser AST does not provide those APIs (it only has the
replacement-returning `Visitor` and `Accept`), so the collector intentionally
retains the existing `Accept` contract. Porting the in-place visitor would
require the broader parser/AST generated visitor migration and is recorded as
a dependency boundary, not silently fabricated here.

## Rust ownership and parity result

Searches found no dependency-closed Rust owner or consumer for CSV parsing,
chunk parsing, dump loading, file regions, routing, schema/view import,
charset conversion, or the Ragel parser. No Rust-only behavior was found to
remove, and no isolated Rust facade or ignored source carrier was added. A
correct native port must move these parser, storage, SQL AST, metadata, and
Lightning consumers together.

## Validation

Profile: Ready for this implementation batch.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/mydump -run '^TestCustomEscapeChar$' -count=1
PASS; failpoint refcount 0

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/mydump -count=1
PASS; 1.853s; failpoint refcount 0

Detached origin/master (`5e8a1a229a7591ddac49a0cd3b795587c2595ab9`) exact-package
failpoint suite: PASS; 1.630s; failpoint refcount 0.
```

The required `make bazel_prepare` gate was attempted with the pinned Go
toolchain, but this environment has no `bazel` executable (`bazel: No such
file or directory`). Rust formatting, repository lint, and final diff checks
are run for the commit batch; the Bazel prerequisite remains explicitly
unverified locally.

## Risks and next boundary

- Correctness: the direct scanner preserves Go's escape mapping and handles
  custom escape bytes that are regexp metacharacters; the complete package
  suite passes on both branch and exact Go master.
- Compatibility: `view_import` remains on the branch's old AST visitor API;
  the in-place visitor migration must land with its parser dependency closure.
- Performance: regexp allocation and replacement are removed from hot CSV
  unescaping; the benchmark covers no-escape, backslash-dense, and custom
  escape workloads.

The package remains an explicit Rust ownership boundary until a
dependency-closed Lightning parser/import implementation exists.
