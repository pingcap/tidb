# `pkg/util/compress` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909` (the source is
unchanged on the requested Go `master` baseline).

## Complete inventory

The package has exactly two artifacts, both read in full before implementation:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `gzip.go` | 36 | `4e4b71cf13536d9754e5eeef0ce1df0b3ddd693b` | `56c6a15d14aef5cb77da670d51fcf822f838cbbbe5a69101252848b053b15e03` | complete production behavior |
| `BUILD.bazel` | 11 | `f00125ae87b3747c023dff7bab41e7e30f953048` | `ec7337495722006c27608843cb1db3d63b96e9282654b72e9c9e8226e603f639` | build metadata inventoried; Cargo owner registered |

There is no `doc.go`, package-local test, test harness, benchmark, fixture,
generated input/output, platform/build-tag variant, example, fuzz target, or
other support artifact. The package has two direct Go consumers:
`pkg/statistics/handle/storage/json.go` and
`pkg/ingestor/ingestctrl/compress.go`.

## Go behavior

`GzipWriterPool` and `GzipReaderPool` are process-global `sync.Pool` values.
The writer pool's constructor returns a gzip writer bound to `io.Discard`; the
reader pool's constructor returns an uninitialized gzip reader. Consumers get
one stream, reset it to their own `io.Writer`/`io.Reader`, perform ordinary
gzip stream I/O, close the stream as appropriate, and put it back. The pools
are deliberately generic: they do not own a byte buffer, file, RPC message,
or statistics-specific format.

The production symbol map is complete: the `sync.Pool.New` closure behind
`GzipWriterPool` is its factory (`io::sink` initial target), `GzipWriter.Reset` and
`GzipWriter.Close` are `GzipWriter::{reset,close}`, the `sync.Pool.New` closure
behind `GzipReaderPool` is its factory (`io::empty` initial source), and
`GzipReader.Reset`/`Close` are `GzipReader::{reset,close}`. Ordinary `Write`,
`Flush`, and `Read` calls delegate to the native gzip streams.

## Rust ownership and integration decision

`rust/crates/tidb-util/src/compress.rs` is the sole native owner. Its public
`GzipWriterPool` and `GzipReaderPool` use the existing process-safe
`tidb_util::zeropool::Pool`, and their stream wrappers preserve Go's reset,
read/write, close, discard-bound, and invalid-header behavior. Rust erases a
caller stream type at the pool boundary with `Send`-bound trait objects; this
is the ownership-safe equivalent of Go's `any` pool values and does not add a
format or policy layer.

The existing statistics JSON block path in
`rust/crates/tidb-executor/src/load_stats.rs` now uses both pools. It keeps the
canonical Go sequence—marshal once, gzip once, split into blocks; concatenate,
decompress once, unmarshal once—and returns pooled objects on every success
and error path. A small private shared buffer is only the Rust ownership seam
needed to retrieve compressed bytes while returning the writer to the pool.
The executor's former direct `flate2` dependency was removed so this package
is the only compression owner for that consumer.

The pinned `pkg/ingestor/ingestctrl` consumer has no Rust owner in the current
workspace. It is recorded as an explicit integration boundary rather than
replaced with a fake gRPC compressor; when that package is transcreated, its
compressor can consume these same public pools without changing this owner.

## Focused regression coverage

Because the pinned Go package has no test artifact, the Rust owner adds only
four focused behavior regressions:

| Rust test | Contract proved |
| --- | --- |
| `pooled_streams_reset_and_round_trip` | discard-bound writer, reset, close, reader reset, and gzip round trip |
| `reader_reset_rejects_an_invalid_header` | invalid gzip header is rejected at the reset boundary and the reader is returned to the pool |
| `writer_pool_reuses_a_closed_stream_for_a_new_target` | a closed pooled writer can be rebound to a second target without stale bytes or state |
| `writer_reset_discards_unfinished_stream_state` | resetting before close does not append a stale trailer to the old target |

The existing `load_stats::tests::json_table_blocks_round_trip` remains the
consumer regression for canonical statistics block framing.

## Validation

Validation profile: **Ready**. The source package has no Go tests or generated
metadata to regenerate, and no Go source/import section changed, so
`make bazel_prepare` is not required.

Passed:

```text
cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/go go test ./pkg/util/compress
# no test files; package compilation passed

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +1.97 check --manifest-path rust/Cargo.toml --locked -p tidb-util -p tidb-executor

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +1.97 test --manifest-path rust/Cargo.toml --locked -p tidb-util \
  compress::tests:: -- --nocapture
# 4 passed

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +1.97 test --manifest-path rust/Cargo.toml --locked -p tidb-executor \
  load_stats::tests::json_table_blocks_round_trip -- --exact --nocapture
# 1 passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/go make lint
# exit 0

git diff --check
# exit 0
```

The initial environment-default Rust 1.95 invocation was blocked by the
workspace's declared Rust 1.97 minimum, and the first retries were blocked by
missing OpenSSL discovery; the command-local toolchain, bundled OpenSSL paths,
and cached Go toolchain above are the successful Ready-profile setup. Cargo
reported only pre-existing workspace warnings; none identifies these changes.

## Risks and unverified behavior

- Correctness: gzip bytes remain format-compatible and the existing statistics
  JSON round trip passes; flate2's implementation is not promised to emit the
  same DEFLATE byte layout as klauspost, which the Go package itself does not
  expose as a contract.
- Compatibility: the Rust stream wrappers require pooled reader/writer values
  to be `Send + 'static`, the necessary safe boundary for a process-global
  concurrent pool. The public names and lifecycle are retained; no
  Rust-specific compression format or high-level helper was added.
- Performance: pooled wrapper ownership is shared, but the current flate2
  backend recreates its erased stream when reset. An inactive target shim
  suppresses flate2's Drop-time trailer so an unfinished stream is discarded
  exactly as Go's `gzip.Writer.Reset` requires.
- Not verified locally: upstream has no Go package test target (the package
  compile-only `go test` check reports no test files), the absent Rust
  `pkg/ingestor/ingestctrl` consumer, and Windows/unsupported Rust targets.
  The repository-wide Ready `make lint` gate passed with the command-local Go
  toolchain above.
