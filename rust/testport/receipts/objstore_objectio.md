# `pkg/objstore/objectio` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`42db2099af50704e424b792626f10a87f4247413` (2026-09-02).

## Complete inventory

The package contains four tracked artifacts and 522 lines. Every production
source, test source, and BUILD target was read in full before this receipt was
written. There are no package fixtures, generated files, or platform variants.
The root `pkg/objstore` and sibling nested packages are separate package
claims; this receipt covers only the object I/O interfaces and buffered writer.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 32 | `2394e82c530f37aeaf456c7109af3ac9f71ac195` | `487a272bbca8dcf08a29b0ac1196477163425a0c595223dbeadf68ec0613da9f` | public object-I/O library and three-shard flaky test target |
| `interface.go` | 50 | `bdbed4217eeae005d79af2684c8ca24cb21cd129` | `b1d444aa3edfe5758e781b3f877061c3816f58d9a21837127d8b08f741ecbe00` | context-aware Reader/Writer contracts and `NewIOWriter` adapter |
| `writer.go` | 155 | `0e9f697a0435fb69934ddeb34700811123338f06` | `2077efc3f01c958e43a201c1af35934063c29b3cae828565ef1c0040cabe0cef` | plain/compressed intercept buffers and chunked buffered writer |
| `writer_test.go` | 285 | `2c0b6c727a48808d9a7857db0759210f60f467e5` | `d6c691f36689d241a2a0f84c4f3e6c5a73416e2fb5d4eb23cdfa6218e43aac57` | local writer, context adapter forwarding, gzip/Snappy/Zstandard round trips, and sync/async Zstd tests |

The two production sources contain 15 concrete functions/methods: the `Reader`
and `Writer` interface contracts, `NewIOWriter` and its context-binding
adapter, `EmptyFlusher`, plain/compressed buffer construction, all `BufferedWriter`
write/chunk/close/accessor methods, and the public writer constructors. The
single test source contains four top-level tests plus its local store/write and
context-forwarding helpers. Tests cover short and multi-chunk local writes, all three supported
compression codecs and suffixes, decompression round trips through both the
raw file and `WithCompression` paths, and Zstandard's default asynchronous
versus explicit synchronous decoder concurrency using `testing/synctest`.

The package's current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is one additive interface helper:
`NewIOWriter` captures a context and adapts `Writer.Write(ctx, p)` to ordinary
`io.Writer`. The implementation and focused regression test are now present;
no BUILD, generated, or platform delta accompanied the upstream change.

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

Profile: **Ready** attempted for this Go behavior restoration. The package has
no failpoints, and the adapter has a focused regression plus a full package
run. `make bazel_prepare` is required because the existing test's import
section and top-level test changed; the local checkout has no `bazel` binary.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/objectio -run '^TestNewIOWriterBindsContext$' -count=1
# passed after the implementation; the same test failed before it with
# `undefined: objectio.NewIOWriter`.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/objectio -count=1
# passed

make bazel_prepare
# blocked: `make: bazel: No such file or directory`

make lint
# passed
```

Not verified here: Bazel's three-shard target, live cloud/storage services,
Windows execution, or full-workspace tests. Rust validation was not applicable
because no Rust source changed. The package-scoped revive check passed.
