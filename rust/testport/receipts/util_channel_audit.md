# `pkg/util/channel` — Go-master parity audit receipt

Status: complete dependency-closed audit; no source behavior delta or
Rust-only execution policy was found.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package has
exactly two tracked artifacts and 30 Go lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 8 | public library target |
| `channel.go` | 22 | generic channel-drain helper |

There is no package `doc.go`, test file, generated source, platform-specific
variant, fixture, benchmark, or nested package. The sole generic production
function is `Clear`; every Go artifact was read in full before comparing the
Rust owner.

## Rust owner comparison

`rust/crates/tidb-util/src/channel.rs` is the dependency-closed owner and is
registered by `src/lib.rs`. Its `clear` helper ranges over a receiver until
all buffered values are discarded and every sender disconnects, matching Go's
`for range ch` behavior for bidirectional and receive-only channels. Rust's
receiver type is the native carrier for the Go channel type constraint; no
async channel, timeout, or nil-channel policy was added. The Go package has no
source tests or fixtures, so there is no missing regression carrier and no
Rust-only behavior to remove.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/channel -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib --offline --locked` (523 passed, 2 ignored)
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check`
- `git diff --check`
- Pinned `make lint` in the clean detached Go-master checkout.

The Go package reports no test files in both active and exact detached
Go-master checkouts, and the Rust utility owner check passes
(with existing warnings in the vendored TiKV client). No Go or Rust source
changed, so `make bazel_prepare`, failpoint toggling, and a new regression test
were not applicable. Broader channel consumers remain outside this leaf audit.

This receipt certifies the bounded `pkg/util/channel` inventory and parity
check; it is not a repository-wide transcreation claim.
