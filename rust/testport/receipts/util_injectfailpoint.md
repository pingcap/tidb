# `pkg/util/injectfailpoint` — Go-master package boundary receipt

Go source: `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
byte-for-byte unchanged from the previous audit, but this receipt now uses the
current Go-master authority rather than the older extraction pin.

## Complete inventory

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `000030623991d92d1c69328daf1f6c144727c949` | `730d9992c46bacafd0b6d76eba1ee9549db88b4833c988421f8629ba5a05febb` | public library target with errors and failpoint dependencies |
| `random_retry.go` | 78 | `ef107f873c6788ee783d592038939d42bf233110` | `2ffc805f3cb714b62ffaff5f8faa2e4e29db6c3d07b351284f825c778debe463` | DXF one-percent/one-per-thousand failpoint callbacks, read-error injection, caller-name capture, and probabilistic `RandomError` |

There is no `doc.go`, source test, fixture/testdata, generated/platform
variant, benchmark/fuzz target, or nested package. The package has 90 Go
lines and five exported helpers; all behavior is conditional test fault
injection used by DXF/import paths rather than normal SQL execution.

## Rust ownership and decision

Rust has no repository-wide failpoint registry and no DXF call path that
consumes these exact random-error helpers. Existing `cfg(feature="failpoints")`
hooks are crate-local compile-time seams and intentionally do not model
Go's named runtime failpoints, caller reflection, or random partial-read
errors. Adding a Rust random-fault helper would be test-only Rust behavior
without a production consumer. No Rust-only behavior was found and no
dependency-closed missing Go behavior can be implemented here; this package
remains explicitly unclaimed as Go failpoint infrastructure.

## Validation

Profile: Ready for this docs-only boundary refresh; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/injectfailpoint -count=1` — passed (`[no test files]`).
- Exact detached Go-master checkout at `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: the same package test passed (`[no test files]`).
- `git diff --stat 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..origin/master -- pkg/util/injectfailpoint` — empty; source is unchanged at the current Go-master authority.
- Rust failpoint search across all crates — confirmed only crate-local hooks and no owner for this helper package.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, pinned `make lint`, and `git diff --check` — passed for the repository audit batch.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Named
DXF failpoint injection and probabilistic fault distributions were not run;
they require the Go failpoint-enabled DXF integration harness.

## Risks and unverified scope

- Correctness: any future port must preserve the 0.01/0.001/0.2 probability
  boundaries, `n==0`/existing-error short-circuit, and partial-read result
  semantics.
- Compatibility: changing these helpers affects only failpoint-enabled DXF
  tests, but their names are externally configured by repository test scripts.
- Performance: no production path changed; random sampling remains test-only.
- Not verified locally: failpoint-enabled DXF and reader retry integrations.
