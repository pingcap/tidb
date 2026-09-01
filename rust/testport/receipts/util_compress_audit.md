# `pkg/util/compress` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this refresh corrects the
artifact inventory and records current-master authority.

## Complete inventory

The package contains two tracked artifacts and 45 lines. Both were read in
full before this update. There is no package doc, package-local test, test
harness, benchmark, fixture, generated input/output, platform/build-tag
variant, example, fuzz target, or other support artifact.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 9 | `f00125ae87b3747c023dff7bab41e7e30f953048` | `ec7337495722006c27608843cb1db3d63b96e9282654b72e9c9e8226e603f639` | public gzip utility library target |
| `gzip.go` | 36 | `4e4b71cf13536d9754e5eeef0ce1df0b3ddd693b` | `56c6a15d14aef5cb77da670d51fcf822f838cbbbe5a69101252848b053b15e03` | process-global pooled gzip readers and writers |

`GzipWriterPool` and `GzipReaderPool` are process-global `sync.Pool` values.
The writer factory binds a gzip writer to `io.Discard`; the reader factory
returns an uninitialized gzip reader. Consumers reset streams to their own
reader/writer, perform ordinary gzip I/O, close as needed, and return them to
the pool. The pools own no statistics, file, RPC, or buffer policy. The two
direct Go consumers are statistics JSON storage and ingest-control compression.

## Rust ownership and parity

`rust/crates/tidb-util/src/compress.rs` is the sole native owner. Its pooled
writer/reader wrappers preserve Go's reset, close, discard-bound, invalid-header,
and stream lifecycle behavior. The statistics JSON block path uses both pools
for the canonical marshal → gzip → block split and concatenate → decompress →
unmarshal sequence, returning pooled objects on success and error paths. The
former direct executor `flate2` dependency was removed, so this path has one
compression owner. The Go ingest-control consumer has no Rust owner and remains
an explicit future integration boundary; no fake compressor was added.

The prior implementation removed Rust-only arbitrary stream helpers and kept
four focused regressions: pooled round trip, invalid-header rejection, closed
writer reuse, and unfinished-stream reset. The statistics block round-trip
test covers the integrated consumer.

## Validation and risk

Profile: **Ready** was completed for the implementation batch; this refresh is
documentation-only and does not alter source behavior. No Go source, imports,
Bazel metadata, or module files changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/compress -count=1
# passed: package compiled; no test files

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util compress::tests:: --lib --offline --locked -- --nocapture
# passed: 4 focused compression tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-executor load_stats::tests::json_table_blocks_round_trip --lib --offline --locked -- --exact --nocapture
# passed: 1 integrated statistics block test
```

The focused Rust commands emitted existing workspace warnings only. Not
verified here: the absent Rust ingest-control consumer, full workspace tests,
Bazel execution, and Windows/other unsupported targets. Existing unrelated
session worktree changes remain outside this receipt.
