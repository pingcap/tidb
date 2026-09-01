# `pkg/util/domainutil` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly two artifacts, both read in full (207 lines
total):

- `repair_vars.go` (198 lines): the process-global, mutex-protected repair
  registry, table/database quarantine, lookup/removal operations, and session
  key strings;
- `BUILD.bazel` (9 lines): the public production library target.

There is no package test, `TestMain`, fixture, testdata, benchmark, fuzz
target, example, generated/platform variant, nested package, or other build
input. Both files are byte-identical to Go master.

## Rust ownership and parity

`rust/crates/tidb-domain/src/domainutil.rs` is the sole owner. It preserves
the process-global `RepairInfo` lock and state, Go lowercasing, list and
case-sensitive map matching, shallow DB copy with quarantined table pointers,
first-match lookup quirk, removal/repair-mode transition, and
`RepairedTable`/`RepairedDatabase` string values. Infoschema, planner, DDL, and
server startup retain their ordinary consumer boundaries; the removed
`tidb-exec` duplicate and Rust-only sorted-map/accessor policy are not present.

No Go or Rust production delta was found in this rolling audit. The Go package
has no source tests, so no new package-local regression was warranted; the
owner compiles cleanly and existing downstream session tests remain the
integration evidence.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/domainutil -count=1` — passed (`[no test files]`).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/domainutil -count=1)` — passed (`[no test files]`).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-domain --lib --offline --locked` — passed (owner compile; existing warning in `tidb-model`).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed in the clean detached Go-master checkout; the active checkout may be temporarily instrumented by the concurrent failpoint test worker.
- `git diff --check` — passed for the documentation diff.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use, so the failpoint wrapper is not
applicable.

## Risks and boundaries

- Correctness: owner compilation and ordinary consumer tests cover shared
  repair state, lowercasing, quarantine, and removal semantics.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; registry operations remain
  lock-protected and map/set-backed.
- Not verified locally: full `ADMIN REPAIR TABLE` distributed execution;
  server/session integration remains outside this leaf refresh.
