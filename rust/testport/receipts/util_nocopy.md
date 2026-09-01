# `pkg/util/nocopy` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
unchanged from the earlier pinned audit; this receipt refreshes the authority
and records the complete artifact hashes.

## Complete inventory

The package contains two tracked artifacts and 32 lines. Both artifacts were
read in full before this update. There is no package doc, test, benchmark,
fixture, generated or platform variant, README, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `2302693640d29bb8ab7bd59bcb03cd0232aff1e7` | `84ded6ce1ef07c137634b415e5aec1a39ae7b481a889e177fd5c7b05064c4561` | public Go utility library target |
| `nocopy.go` | 24 | `7bfab1988fcb37b40061cdd8e731d81ea03bf934` | `e02781234846ccc78f8ad9e88486d1e26c99a0ab3d113cd06639cc339cbd992d` | zero-sized no-copy marker and no-op lock methods |

`NoCopy` is a zero-sized marker implementing `sync.Locker` with empty
`Lock`/`Unlock` methods. Go's vet analyzer recognizes that method pair and
prevents copying an embedding owner after use. The package has no tests or
support fixtures.

## Rust ownership and parity

`rust/crates/tidb-util/src/nocopy/mod.rs` owns the complete package. Its
zero-sized `NoCopy` marker has no `Copy` or `Clone` implementation and exposes
only the source-shaped no-op `lock`/`unlock` methods, providing native
ownership enforcement for Go's vet contract. Earlier Rust-only constructors,
`Default`/`Debug`, compile-fail tests, and semantic manifests were removed;
no Rust-only behavior remains in the current owner.

## Validation and risk

Profile: **WIP** for this documentation-only authority refresh. No Go source,
imports, Bazel metadata, or module files changed; `make bazel_prepare` and the
Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/nocopy -count=1
# passed: package compiled; no test files

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib nocopy --offline --locked -- --test-threads=1
# passed: zero tests ran, matching the Go package inventory
```

Not verified here: full workspace tests, Bazel execution, or compile-fail vet
integration. Existing unrelated session/planner worktree changes remain
outside this receipt.
