# `pkg/util/watcher` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All four pinned artifacts were read in full: `event.go`, `watcher.go`,
`watcher_test.go`, and `BUILD.bazel`. The package contains two production
files, one unit test, and one Bazel library/test pair. It has no package doc,
README, fixture, benchmark, generated file, platform variant, test harness, or
ownership file. The checkout is byte-identical to the pin.

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

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/watcher` — passed.
- `go test ./pkg/util/watcher -count=1` — passed (one source test).
- `cargo test -p tidb-util --lib --locked watcher::tests::test_watcher -- --exact` — passed (one source test).
- `cargo check -p tidb-util --lib --locked` — passed without warnings.
- `rustfmt --edition 2021 --check crates/tidb-util/src/watcher.rs` and
  `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Only the
installed `aarch64-apple-darwin` target was compiled locally; the Windows
metadata branch was reviewed but not cross-compiled.

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
