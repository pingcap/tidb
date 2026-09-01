# `pkg/objstore/objectio` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 493 lines. Every production
source, test source, and BUILD target was read in full before this receipt was
written. There are no package fixtures, generated files, or platform variants.
The root `pkg/objstore` and sibling nested packages are separate package
claims; this receipt covers only the object I/O interfaces and buffered writer.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 32 | `2394e82c530f37aeaf456c7109af3ac9f71ac195` | `487a272bbca8dcf08a29b0ac1196477163425a0c595223dbeadf68ec0613da9f` | public object-I/O library and three-shard flaky test target |
| `interface.go` | 50 | `bdbed4217eeae005d79af2684c8ca24cb21cd129` | `b1d444aa3edfe5758e781b3f877061c3816f58d9a21837127d8b08f741ecbe00` | context-aware Reader/Writer contracts and `NewIOWriter` adapter |
| `writer.go` | 155 | `0e9f697a0435fb69934ddeb34700811123338f06` | `2077efc3f01c958e43a201c1af35934063c29b3cae828565ef1c0040cabe0cef` | plain/compressed intercept buffers and chunked buffered writer |
| `writer_test.go` | 256 | `8846eaac81f5a869a37abae4c266fe0475d426d4` | `045cdd6548d38a200aa14fb34a11519a440e5849115cb8e85a2b33879b9d57f1` | local writer, gzip/Snappy/Zstandard round trips, and sync/async Zstd tests |

The two production sources contain 14 functions/methods: the `Reader` and
`Writer` interface contracts, `NewIOWriter` and its context-binding adapter,
`EmptyFlusher`, plain/compressed buffer construction, all `BufferedWriter`
write/chunk/close/accessor methods, and the public writer constructors. The
single test source contains three top-level tests plus its local store/write
helpers. Tests cover short and multi-chunk local writes, all three supported
compression codecs and suffixes, decompression round trips through both the
raw file and `WithCompression` paths, and Zstandard's default asynchronous
versus explicit synchronous decoder concurrency using `testing/synctest`.

The package's current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is one additive interface helper:
`NewIOWriter` captures a context and adapts `Writer.Write(ctx, p)` to ordinary
`io.Writer`. No BUILD, test, generated, or platform delta accompanied it.

## Rust ownership and explicit boundary

Rust's `tidb-domain::replayer::ObjectWriter` is a narrow, caller-specific
boundary for plan-replayer dump files. It has `write`/`close` methods without
the object-store `Reader`, compression-buffer capacity/flush semantics, or
chunk upload lifecycle, and it is not a transcreation of this package. No Rust
crate owns the dependency-closed object-store buffered writer, codec adapter,
or `NewIOWriter` contract.

No Rust-only behavior was found to remove. Implementing an isolated Rust
buffered writer without the root storage backends and compression package
would be speculative and would not satisfy this package's consumers. The
package remains an explicit parity boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, and no package failpoints are used, so
`make bazel_prepare`, Ready lint, and Rust cargo gates are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/objectio -count=1
# exact Go origin/master source: passed in 0.708s
```

Not verified here: Bazel's three-shard target, live cloud/storage services,
Windows execution, or full-workspace tests. No Rust validation was applicable
because no Rust source changed.
