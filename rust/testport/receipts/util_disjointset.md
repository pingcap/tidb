# `pkg/util/disjointset` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly six artifacts, all read in full (302 lines
total):

- `int_set.go` (64 lines) and `set.go` (85 lines): dense and sparse generic
  disjoint-set implementations;
- `int_set_test.go` (43 lines) and `set_test.go` (49 lines): the two source
  test identities;
- `main_test.go` (33 lines): common setup and leak-checking `TestMain`;
- `BUILD.bazel` (28 lines): production and flaky test targets.

There is no `doc.go`, fixture, testdata, benchmark, fuzz target, example,
generated/platform variant, nested package, or other build input. All six
files are byte-identical to Go master.

## Rust ownership and parity

`rust/crates/tidb-util/src/disjointset/{int_set,set}.rs` is the complete owner,
re-exported by `mod.rs`. Dense and sparse parents use signed native-width
indices like Go `int`, preserve first/second root selection, insert missing
sparse values, compress paths, retain the current-value lookup, and panic at
the same negative/invalid boundaries. The existing Rust tests preserve both
Go source identities; the signed-boundary regression protects the prior
cross-language width fix. The `tidb-chunk` consumer uses the owner directly,
without a duplicate set implementation.

No Go or Rust production delta was found in this rolling audit, so no new
package-local regression was warranted. The two source-derived tests and the
existing signed-boundary regression remain the focused proof.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disjointset -count=1` — passed (two tests).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disjointset -count=1)` — passed (two tests).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib 'disjointset::' --offline --locked -- --test-threads=1` — passed (three tests).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed in the clean detached Go-master checkout; the active checkout may be temporarily instrumented by the concurrent failpoint test worker.
- `git diff --check` — passed for the documentation diff.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use, so the failpoint wrapper is not
applicable.

## Risks and boundaries

- Correctness: source tests cover dense/sparse union and membership behavior;
  the signed regression covers negative size/index rejection.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; path compression and allocation
  behavior remain unchanged.
- Not verified locally: broad `tidb-chunk` integration after this no-delta
  refresh; prior consumer checks remain outside this package batch.
