# `pkg/keyspace` — Go-master parity audit receipt

Status: complete dependency-closed audit for the bounded package at the
current Go-master authority. The Rust owner now exposes the same discardable
return-value contract as Go; no Rust-only execution path was added.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package has
exactly five tracked artifacts and 404 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 40 | library/test targets and shard metadata |
| `doc.go` | 38 | next-generation keyspace isolation contract |
| `keyspace.go` | 102 | names, etcd namespaces, codec bytes, logging, and PD context |
| `keyspace_test.go` | 132 | config, kernel-mode, policy, and benchmark coverage |
| `username_policy.go` | 92 | default and Starter username policies |

There is no generated Go source, platform-specific variant, fixture/testdata
directory, nested package, or additional build artifact. The production files
contain 17 function/method declarations plus the `CodecV1` value and keyspace
constants; the test file contains three tests and one benchmark. Every Go
production, test, documentation, and Bazel artifact was read in full before
comparing the Rust owner. The package is unchanged between the prior pin and
the current authority.

## Rust owner comparison

`rust/crates/tidb-util/src/keyspace.rs` is the dependency-closed owner and is
registered by `src/lib.rs`. It preserves the complete behavior of both Go
production files: API-v1 versus keyspace-ID etcd namespace paths (including
the slash variant), global-config name lookup, once-computed keyspace bytes
with Go's nil-on-classic result, empty-name detection, the `keyspaceName`
logger field, V1/V2 API-context construction, and the permissive/default and
Starter prefix username policies. The `KeyspaceCodec` trait and `ApiContext`
enum are local carriers for the absent client-go and PD types; they preserve
the source values and do not introduce a second runtime path.

The owner includes source-shaped regressions for both namespace forms, empty
and classic byte behavior, API context selection, default and prefix policy
validation/format/variants/original-name behavior, exact `[ddl:1468]` error
rendering, the empty-prefix bootstrap case, and the Go return-value contract.
The new `return_values_may_be_ignored_like_go` test failed before the fix with
11 `unused_must_use` errors. Removing all 11 explicit Rust `#[must_use]`
annotations makes the same calls compile and pass, matching Go's ability to
discard each result. No Go source delta exists at the comparison commit. The
Go benchmark has no production-observable contract and has no Rust benchmark
counterpart.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/keyspace -count=1`
- Same Go command from detached `/tmp/tidb-go-latest-c605`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib keyspace::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib keyspace::tests --offline --locked -- --test-threads=1`
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`
- `git diff --check`

The current and detached latest-master Go package suites pass. The focused
Rust regression and all seven keyspace owner tests pass. Ready formatting,
the pinned repository lint, and diff hygiene pass. No Go/Bazel artifact or
import section changed, so `make bazel_prepare` and failpoint toggling are not
applicable. Broader server, PD, and logger integration remains outside this
leaf audit.

This receipt certifies the bounded `pkg/keyspace` inventory and parity check;
it is not a repository-wide transcreation claim.
