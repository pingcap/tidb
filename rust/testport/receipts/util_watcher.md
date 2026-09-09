# `pkg/util/watcher` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

All four Go-master artifacts were read in full: `BUILD.bazel`, `event.go`,
`watcher.go`, and `watcher_test.go` (605 lines total). The package contains
two production files, thirteen production functions/methods, one
`TestWatcher` suite with one helper, and one Bazel library/test pair. It has
no package `doc.go`, README, fixture, benchmark, generated file, platform
variant, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `11e467fb84ac8667886eddceaa0080d439d25fdb` | `b864e930818d7a9fd130bb0a56e53d3862f6f08dd27632cf39b93096d87516e4` | library/test targets |
| `event.go` | 89 | `6762c11b93cc8286ccc28897f7731c74f828509d` | `b786d23dc804903185c0898af2178f0ffd4fdc6f369111ec898399ef96026d0d` | operation and event types |
| `watcher.go` | 331 | `0554a671ff00060a94a206e94f1b639ce694ab12` | `faa49cd3a28dd9b22287d428843cea5129ef647b4fdb64ef39e821af933edf43` | polling watcher implementation |
| `watcher_test.go` | 161 | `dd83334717133acf36ca5d0d0368bca3f9e55674` | `404df4df8f458c7ca73fa1cfdf9a4388ac73189fd34c1f8a986c42d66f46995e` | source event-sequence test |

## Rust ownership and audit result

`rust/crates/tidb-util/src/watcher.rs` is the sole owner. It retains Go's
unbuffered event/error delivery, non-recursive snapshots, operation priority,
same-file rename/move matching, original path for rename/move events, closed
and running state transitions, and add/remove serialization.

The audit restored Go's three distinct mutex phases for listing, event
delivery, and snapshot replacement. Rust had held one mutex across the whole
poll, preventing the interleavings Go permits. Directory child metadata now
uses no-follow lookup like Go `DirEntry.Info`; the watched root continues to
use following `Stat`. File sizes use Go's signed 64-bit domain, modification
time errors are no longer replaced with a fabricated epoch, and file identity
uses the platform volume/index representation on Windows as well as
device/inode on Unix.

The Rust-only injected-ticker execution path, `Default` and explicit `Drop`
policies, public single-op helper, public device/inode accessors, `must_use`
annotations, expanded module narrative, and four supplemental tests were
removed. The remaining test follows the source's real 10 ms ticker and exact
create/modify/chmod/rename/remove/create/move sequence.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; the existing source-derived `TestWatcher` remains
the focused behavioral regression.

```text
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- \
  pkg/util/watcher
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/watcher -count=1
# passed (current worktree and exact detached Go-master worktree; one source test)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib watcher::tests::test_watcher --offline --locked -- --exact --test-threads=1
# passed: one source-derived test

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed in a clean detached Go-master checkout
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. Only the
installed `aarch64-apple-darwin` target was compiled locally; the Windows
metadata branch was reviewed but not cross-compiled. Full workspace tests and
Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: reduced; lock interleavings, symlink metadata, signed size,
  modification-time failure, platform identity, and the source event sequence
  now follow Go.
- Compatibility: intentionally removes Rust-only constructors/policies,
  helper accessors, derives, and tests. There are no repository consumers of
  this package outside its owner test.
- Performance: the three shorter lock phases match Go and permit more
  add/remove concurrency between polling phases; filesystem polling and
  unbuffered delivery costs are otherwise unchanged.
