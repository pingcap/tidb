# `pkg/util/dbterror/exeerrors` — complete package parity receipt

Go source: `origin/master`
`db35d47066648fe73abce6318d53fc625df51490`.

Rust comparison branch: `origin/hparser-integration`
`5a005978dda57fbb3373a303660ea0a5f7990b38`.

## Complete Go inventory

The package has exactly two direct artifacts, both read in full:

- `errors.go` — 114 lines, one package comment and one declaration block with
  82 exported error prototypes;
- `BUILD.bazel` — 13 lines, one public `go_library` target over `errors.go`
  with dependencies on `pkg/errno`, `pkg/parser/mysql`, and
  `pkg/util/dbterror`.

There is no `doc.go`, `*_test.go`, benchmark, fixture/testdata, generated
source or generator input, platform/build-tag variant, README, or ownership
artifact in this Go package. The checkout copies of both artifacts are
byte-identical to the fetched Go source.

`errors.go` contains no function or method declarations. Its complete
production surface is the 82 prototypes. Seventy-eight use the standard errno
catalog directly; four use explicit `parser_mysql.Message(..., nil)` text.
The prototypes use the Executor class except for `ErrRoleNotGranted`
(Privilege), `ErrWrongStringLength`, `ErrUnsupportedFlashbackTmpTable`, and
`ErrUserNameNeedPrefix` (DDL), and `ErrTruncateWrongInsertValue` (Table).

## Rust ownership and comparison result

`rust/crates/tidb-util/src/dbterror/exeerrors.rs` is the package owner. It
contains 82 public `LazyLock<TerrorError>` prototypes and exports them through
`dbterror::exeerrors`. `rust/crates/tidb-util/src/dbterror/mod.rs` supplies the
shared Go-shaped error classes and standard/explicit-message constructors;
`rust/crates/tidb-util/src/lib.rs` publishes the module; and
`rust/crates/tidb-util/Cargo.toml` registers the owning crate.

`rust/crates/tidb-util/src/dbterror/exeerrors_go_fixture.txt` is the complete
82-row generated support artifact. Every row records the Go variable name,
numeric code, RFC identity, and exact message template separated by the ASCII
unit separator. The owner test `errors_match_go_fixture` compares the fixture
and Rust catalog in both directions, rejects a missing or extra prototype, and
checks every field. The ordinary `sqlkiller` owner consumes six of these
prototypes; the remaining public prototypes are the package API available to
later executor consumers.

Go commit `60996fd69eaae91e50c1460a110b4d005ae16eaf` added
`ErrSecondPasswordCannotBeEmpty`,
`ErrPasswordCannotBeRetainedOnPluginChange`, and
`ErrCurrentPasswordCannotBeRetained`. The Rust owner and generated fixture on
the requested hparser branch already contain all three with their exact
Executor classes, codes 3878/3894/3895, RFC identities, and messages. A
mechanical name comparison confirms 82 Go declarations, 82 Rust statics, and
82 fixture rows with no difference.

No missing Go behavior or Rust-only execution behavior was found. Therefore
this audit deliberately changes no production code and adds no synthetic
regression: the existing complete-catalog fixture test is the focused
source-derived guard, and a new test that only repeated three of its rows
would reduce neither risk nor uncertainty.

## Validation

Profile: Ready for this atomic package audit. This is not a repository-wide
parity or PR-readiness claim.

Commands run from the repository root:

- `git ls-tree -r --name-only origin/master -- pkg/util/dbterror/exeerrors`
  and full-file reads — confirmed the two-artifact inventory.
- `diff -u <(git show origin/master:pkg/util/dbterror/exeerrors/errors.go | sed -n 's/^[[:space:]]*\(Err[A-Za-z0-9_]*\)[[:space:]]*=.*/\1/p') <(cut -d $'\x1f' -f1 rust/crates/tidb-util/src/dbterror/exeerrors_go_fixture.txt)` — passed with no difference.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/dbterror/exeerrors -count=1` — passed; the source package has no test files.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib dbterror::exeerrors` — passed, 1 test.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked -p tidb-util --lib` — passed with one pre-existing vendored `private_bounds` warning.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go source, import block, test function, module metadata, or Bazel artifact
changed, so `make bazel_prepare` is not required.

## Risk and unverified scope

- Correctness: low. Complete bidirectional catalog coverage and exact
  code/RFC/message equality pass; the Go package itself compiles.
- Compatibility: no API or runtime behavior changed. The three newest Go
  prototypes were already public in the Rust owner.
- Performance: none; this package only defines lazily initialized error
  prototypes.
- Not verified locally: no non-host platform path exists in this package, and
  there are no Go tests or benchmarks to run. Higher-level executor behavior
  that generates these prototypes belongs to its own package audit and is not
  claimed here.
