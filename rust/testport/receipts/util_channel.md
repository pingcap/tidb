# `pkg/util/channel` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
unchanged from the earlier pinned audit; this receipt refreshes the authority
and records the complete artifact hashes.

## Complete inventory

The package contains two tracked artifacts and 30 lines. Both the production
source and Bazel target were read line by line before this update. There are no
tests, generated sources, platform variants, benchmarks, fuzz targets, or
fixtures.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `436d1ea1fb22bc80b1ab4574dd294234867e117d` | `4881283deb525a7346afd6673f9a46f79713e1c731868d065ae183dad097c9e0` | public Go library target |
| `channel.go` | 22 | `4b14c14d137b6bc232fc148f6575a2f09015824c` | `8c6e2751e031a547da6fc9c491e8f2c9f3716174cc9d33720815295dc3edc8c1` | generic channel-drain helper |

`Clear` ranges over a send-or-receive channel until every sender disconnects.
It drains buffered and later values and intentionally blocks while a sender is
still open. The source package has no package-local tests or support fixtures;
callers exercise cleanup as part of executor shutdown paths.

## Rust ownership and parity

`tidb-util::channel::clear` is the dependency-closed Rust owner. It accepts a
borrowed native `std::sync::mpsc::Receiver`, drains it until disconnect, and
preserves the caller's receiver handle after return (matching Go's copyable
channel handle). The earlier Rust acceptance of arbitrary `IntoIterator`
values and ownership-consuming receiver calls was removed in the prior
package fix; no Rust-only behavior remains in the current owner.

## Validation and risk

Profile: **WIP** for this documentation-only authority refresh. No Go source,
imports, Bazel metadata, or module files changed; `make bazel_prepare` and the
Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/channel -count=1
# passed: package compiled; no test files

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --all-targets --offline --locked
# passed: tidb-util all targets (workspace warnings only)
```

Not verified here: full workspace tests, Bazel execution, or an external
extension/channel integration. Existing unrelated session/planner worktree
changes remain outside this receipt.
