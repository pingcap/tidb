# `pkg/util/disttask` — Go-master parity audit receipt

Original full-audit Go authority: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`. The corrective return-contract
follow-up below refreshes the authority to `c767f6fd8c01e9dcb459767611c0c1d4110d210d`.

## Complete inventory

The package contains exactly three artifacts, all read in full (133 lines
total):

- `idservice.go` (71 lines): executor-ID formatting, membership/index lookup,
  live infosync lookup, and test-only mock lookup;
- `idservice_test.go` (38 lines): the single `TestGenServerID` source test and
  all IPv4/IPv6/empty/out-of-range port vectors;
- `BUILD.bazel` (24 lines): production and flaky test targets.

There is no `doc.go`, package harness, fixture, testdata, benchmark, fuzz
target, example, generated/platform variant, nested package, or other build
input. All three files are byte-identical to Go master.

## Rust ownership and parity

`rust/crates/tidb-domain/src/disttask.rs` owns the complete package because
the Go implementation depends on domain infosync/server-info state. It
preserves `net.JoinHostPort` IPv4/IPv6 formatting, first-match index and `-1`
sentinel, membership, empty-on-discovery-error/empty-map/missing-ID behavior,
and the explicit test-only server-map lookup. The owner has one source-derived
test covering every Go vector; the earlier unused `tidb-util` projection and
non-Go discovery API are absent.

No Go or Rust production delta was found in this rolling audit, so no new
package-local regression was warranted. The existing source-derived test is
the focused regression carrier.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disttask -count=1` — passed (one test).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disttask -count=1)` — passed (one test).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-domain --lib disttask::tests --offline --locked -- --test-threads=1` — passed (one source-derived test).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed in the clean detached Go-master checkout; the active checkout may be temporarily instrumented by the concurrent failpoint test worker.
- `git diff --check` — passed for the documentation diff.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use, so the failpoint wrapper is not
applicable.

## Risks and boundaries

- Correctness: the source vector covers every formatting and index case;
  discovery errors and missing IDs are represented in the Rust API contract.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; lookup remains linear over the
  server list and constant-time in the infosync map.
- Not verified locally: a live distributed-task infosync deployment; the
  source test and Rust owner test cover the package logic.

## 2026-09-07 corrective return-contract follow-up

Current Go master `c767f6fd8c01e9dcb459767611c0c1d4110d210d`
was read package-completely before the Rust edit. The inventory remains exactly
three artifacts and 133 lines: `idservice.go` (71), `idservice_test.go` (38),
and `BUILD.bazel` (24). The production surface is exactly `GenerateExecID`,
`MatchServerInfo`, `FindServerInfo`, `GenerateSubtaskExecID`, and the test-only
`GenerateSubtaskExecID4Test`; the source test is the single
`TestGenServerID` with all five IPv4/IPv6/empty/out-of-range-port vectors.
There is still no `doc.go`, `TestMain`, fixture, testdata, benchmark/fuzz
target, example, generated/platform/build-tag variant, nested package, or
other build input. All three artifacts are byte-identical to current Go
master, and the package has no TiDB failpoint use.

The complete 17-artifact, 11,727-line pre-edit `tidb-domain` crate,
`disttask.rs`, its server-info/syncer dependencies, and all Rust references
were inventoried. The module remains the sole Rust owner. This follow-up
removes only the Rust-exclusive `#[must_use]` diagnostics from all five direct
Go function counterparts. `JoinHostPort` formatting, first-match membership,
the `-1` sentinel, and empty-string discovery/missing-ID behavior are
unchanged.

The focused `#[deny(unused_must_use)]` regression failed before the fix with
exactly five unused-return diagnostics and passes after it. Ready evidence:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --lib disttask::tests::disttask_returns_may_be_ignored_like_go -- --exact --nocapture` — failed before the fix with exactly five diagnostics, then passed 1/1.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-domain --lib -- --test-threads=1` — passed 161/161.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-domain --all-targets` — passed with pre-existing warnings.
- `rustfmt +nightly-2026-08-22 --edition 2021 --check rust/crates/tidb-domain/src/disttask.rs` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.
- `git diff --check` — passed.

This is Rust/testport-only work. No Go source, Go import, Go test, Bazel file,
or module dependency changed, so `make bazel_prepare` is not required.
