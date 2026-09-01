# `pkg/lightning/tikv` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly eight tracked artifacts and 1,209 text lines. Every
production, test, fixture, generated/platform candidate, and BUILD line was
read from the pinned Go tree before the ownership decision.

| Go artifact | Lines/bytes | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 76 lines | `0ed37379c49b4b87b1cf169a54d50f4c1edbced2` | library/test metadata and SST fixture data |
| `local_sst_writer.go` | 115 | `adeb3f96c8424059c75d392ab4e17d59d1631394` | Pebble TiKV write-CF SST encoder |
| `local_sst_writer_test.go` | 364 | `8d50ac77ad9884eb218d201c0d60dfa25d68410` | manual/integration and SST comparison tests |
| `prop_collector.go` | 232 | `fd4745622e527f16e55ae53850b86c6f097296a2` | MVCC and range property collectors |
| `sst-examples/0.sst` | 1,560 bytes | `0144af0a39744212494d7ccfcc330a4071c6e2c6` | expected TiKV SST fixture |
| `sst-examples/1.sst` | 23,526 bytes | `5e53390a5c06dcfe27909505eb2390772ce64b73` | large expected TiKV SST fixture |
| `tikv.go` | 267 | `187690ac9279d92e4d6e7fefac0ecc1dc23fae7c` | TiKV mode, compaction, metadata, and version helpers |
| `tikv_test.go` | 231 | `d8a28e0ec96c4c6af1ce83d484cf9a6b052efff0` | store, metrics, and version tests |

The three production Go files contain 31 function/method declarations; the
two test files contain seven `TestXxx` functions. `TestIntegrationTest`,
`TestPebbleWriteSST`, and `TestDebugReadSST` are manual or intentionally
skipped paths; no fuzz corpus, generated source, platform variant, package
doc, or additional fixture exists.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/lightning/tikv` is empty. The branch
already contains the complete Go-master package, including SST key/value
encoding, MVCC/range property metadata, TiKV import-mode RPCs, remote schema
fetch, and PD/TiKV version checks. No source fix or regression test is needed
for this package.

## Rust ownership and parity result

Rust has related TiKV client and BR restore code, but no dependency-closed
owner for this Go package's Pebble SST writer/property collectors and
Lightning-specific TiKV RPC/version helper group. The related raw-KV and
region-property code is not a substitute for this package's SST format and
Lightning callers. No Rust-only behavior was found to remove, and no
speculative facade was added.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/tikv -count=1
PASS; 0.789s; failpoint refcount 0

Detached origin/master (`5e8a1a229a7591ddac49a0cd3b795587c2595ab9`) exact-package
failpoint suite: PASS; 0.954s; failpoint refcount 0.
```

No Go, Rust, Bazel, module, generated, or test source changed, so
`make bazel_prepare` is not required for this receipt. Rust formatting,
repository lint, and `git diff --check` are run for the combined commit batch.

## Risks and next boundary

- Correctness: SST byte layout, property names, import-mode handling, and
  version gates remain covered by the existing source tests.
- Compatibility: a native Rust implementation would need Pebble/TiKV SST
  format compatibility and the import-sst RPC clients together; the current
  Rust client/BR modules do not close that dependency graph.
- Performance: no runtime code changed.

Keep this package as an explicit Rust ownership boundary until the concrete
SST and Lightning RPC dependencies can move together.
