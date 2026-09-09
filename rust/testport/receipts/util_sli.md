# `pkg/util/sli` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly two artifacts, both read in full (132 lines
total):

- `sli.go` (120 lines): transaction write-throughput accumulator, validity and
  small-transaction rules, metric selection, reset, rendering, and the
  `CheckTxnWriteThroughput` failpoint;
- `BUILD.bazel` (12 lines): the production library target.

There is no `doc.go`, package test, fixture, testdata, benchmark, fuzz target,
generated or platform-specific variant, nested package, or other build input.
The two files are byte-identical to the Go-master checkout.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sli.rs` is the package owner. Its production
behavior and existing cross-package integration already match the Go
accumulator: signed duration and native-width counters, wrapping arithmetic,
validity/small-transaction rules, metric metadata and buckets, reset and
rendering, and the failpoint hook. The ordinary session, executor, cluster,
real-TiKV, and text/prepared dispatch seams provide the same commit and scan
details as Go; estimated row sizes, affected-row proxies, cache-only paths,
synthetic fixtures, and alternate observation APIs are not used.

No Rust-only behavior or source delta was found in this package audit, so no
production edit or new package-local regression was warranted. The existing
source-derived integration regression remains the focused proof of the
cross-package behavior.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/sli -count=1` — passed (`[no test files]`).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/sli -count=1)` — passed (`[no test files]`).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server --features failpoints --lib txn_write_throughput_sli_matches_source --offline --locked --quiet` — passed (one source-derived test).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The Go package has no tests and therefore does not need the
failpoint test wrapper.

## Risks and boundaries

- Correctness: the source-derived integration regression covers accumulator
  state, statement invalidation, read/write key accounting, and failed-commit
  cleanup through the real session seams.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; existing metric initialization and
  reporting paths are unchanged.
