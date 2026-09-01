# `pkg/util/errmsg` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

The package has exactly three Go-master artifacts and 288 lines, all read in
full: `BUILD.bazel`, `errmsg.go`, and `errmsg_test.go`. There is no package
`doc.go`, fixture, generated input/output, platform/build-tag variant,
README, ownership file, benchmark, fuzz target, example, or nested package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `5798f8d23a0a72bb8d05856a682a0d1d40510406` | `1a67ed84b2f51e9db4c1b44d185be10842acfacf85a6d8ee116365128cdc90ac` | library and short flaky test targets |
| `errmsg.go` | 52 | `ac66fc23e1b4ecac7c365c0c31a26f7e0f008a46` | `bcdcab0a20799dbb16a9b0ffb397befa8f5b41814a3f99328622d091fda5aa0d` | configured suffix extension |
| `errmsg_test.go` | 211 | `751f63a889b849e791151f99e7512b621215b959` | `0ddc83ef1c3b138d65641e918f6d845454f7fa29e1cdd18025ea96bef4d72506` | five source tests and matrices |

The production surface is `Extend`, which safely ignores a nil SQL error,
reads the prepared configuration snapshot, skips empty suffixes and absent
compiled regexps, applies only the first matching extension, trims trailing
periods, and appends the fixed `", suffix."` form. The five source tests cover
normal matching and punctuation, no configuration, invalid regex skipping,
longest-pattern preparation, and concurrent configuration publication. The
Bazel test target's `flaky` scheduling annotation has no Cargo semantic
analogue.

## Rust ownership and integration

`rust/crates/tidb-errmsg/src/lib.rs` owns the complete behavior over
`tidb_error::mysql::SqlError`. `Option<&mut SqlError>` is the native
representation of Go's nullable pointer. `tidb-config` owns the prepared
configuration snapshot: invalid regexps are dropped and patterns are ordered
before publication. The ordinary `tidb-server` connection writer invokes the
owner before encoding SQL-error packets and preserves raw packet bytes at the
wire boundary.

The Rust integration target contains exactly the five Go test identities and
their source cases. Its mutex only serializes global configuration mutation
between tests; it adds no production behavior or policy.

## Validation

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; the five source-derived tests remain the focused
regressions.

```text
git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..origin/master \
  -- pkg/util/errmsg
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/errmsg -count=1
# passed (current worktree and exact detached Go-master worktree; five tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-errmsg \
  --test errmsg_test --offline --locked -- --test-threads=1
# passed: five source-derived tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-errmsg -p tidb-server --offline --locked
# passed: owner and ordinary packet consumer (workspace warnings only)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-server \
  connection_writers::tests::error_packets_apply_configured_suffixes_and_preserve_raw_bytes \
  --offline --locked -- --exact --test-threads=1
# passed: ordinary ERR-packet consumer (one test; workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

The package has no live configuration reload or endpoint to verify locally;
the in-process prepared snapshot and packet consumer are covered. Full
workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: all source production branches and five source test identities
  are represented; config preparation and the packet call site are checked.
- Compatibility: nil errors, invalid patterns, first-match ordering,
  punctuation, and concurrent publication retain Go semantics.
- Performance: one immutable snapshot scan with early return and no additional
  allocation beyond the resulting message.
