# `pkg/objstore/compressedio` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains five tracked artifacts and 261 lines. Every production
source and the package BUILD target was read in full before this receipt was
written. It has no `doc.go`, test files, fixtures, generated/platform variants,
fuzz inputs, or additional build artifacts. The package is intentionally a
small dependency of the root `pkg/objstore` wrapper; the root package and all
other nested object-store packages have separate receipts.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 21 | `8be34b97b7564a2555a72d28a3b2019b4eb58351` | `f72ea11735aa1d964b565e38f11c777161b561f4cf767f49e536715220301dc3` | public compressed-I/O library target and codec dependencies |
| `buffer.go` | 74 | `05d33b1b77326efc098e58a05836382345904705` | `7c309562b5734f3ff34c91925cb34fd1d6f40a12755bc09200030fe99cfdc061` | compressed buffer adapter, capacity, reset, flush, and close |
| `def.go` | 75 | `ac7ba8690fef5196221c3b648056d8c7a4d8705f` | `da3ab89d167bfa2514a2a2b08fe9ce42968dac4818ed5497a65046a94217fc1f` | compression enum, suffixes, decompression config, and parser |
| `reader.go` | 42 | `7b3affa5adfa3c974fc2ab7250efe230afaf7279` | `c4225c3ed5646923c40d9336eee95d93bd7408dab3a99210d53f83944437c35c` | gzip, Snappy, and Zstandard reader construction |
| `writer.go` | 49 | `964348c6d22f386cd866b20241ecde7092b12fd8` | `13f362920e317e9835016e8cdcee835be760f947ea13e483fa7ba84b1d5ae092` | gzip, Snappy, and Zstandard writer construction |

The four Go sources contain 12 functions/methods: `Buffer`'s write/length,
capacity/reset/flush/close/compressed methods and `NewBuffer`; enum
`FileSuffix` and `ParseCompressType`; and `NewReader`/`NewWriter`. The reader
and writer constructors preserve the Go switch behavior: unsupported or
`NoCompression` readers return a nil reader without error, while unsupported
writers return nil; Zstandard alone consumes the configured decoder
concurrency. No package-local tests or benchmarks exist, so there are no
fixtures or test-support artifacts to inventory.

The package is unchanged between the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` and Go master. No source, test,
BUILD, generated, or platform delta required reconciliation.

## Rust ownership and explicit boundary

Rust's `tidb-util::compress` is a separate transcreation of Go
`pkg/util/compress` for pooled gzip streams used by statistics serialization;
it does not own `pkg/objstore/compressedio`'s Snappy/Zstandard codecs, suffix
parser, `Buffer` adapter, or object-store writer/reader contract. The remaining
Rust compression references are gRPC protocol negotiation or unrelated table
metadata. No dependency-closed object-store compression owner exists.

No Rust-only behavior was found to remove and no safe missing behavior can be
implemented without the root object-store reader/writer composition and its
separate nested backend packages. A disconnected codec facade would be
speculative, so this small package remains an explicit parity boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed; failpoints are not used by this package, and
the package has no tests to wrap. Therefore `make bazel_prepare`, Ready lint,
and Rust cargo gates are not required for this batch.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/compressedio
# passed: package compiles; [no test files]
```

Not verified here: root object-store tests, codec behavior under live storage,
Bazel, or full-workspace tests. No Rust validation was applicable because no
Rust source changed.
