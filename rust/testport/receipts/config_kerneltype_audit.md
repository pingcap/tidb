# `pkg/config/kerneltype` — Go-master parity audit receipt

Status: complete dependency-closed audit; no source behavior delta or
Rust-only execution policy was found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly six tracked artifacts and 196 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 23 | library/test target and variant source list |
| `classic.go` | 28 | `!nextgen` compile-time implementation |
| `doc.go` | 40 | Classic/NextGen architecture contract |
| `nextgen.go` | 28 | `nextgen` compile-time implementation |
| `type.go` | 41 | names and PD kernel-type matching |
| `type_test.go` | 36 | source kernel/matching tests |

There is no generated Go source, fixture/testdata directory, benchmark, or
nested package. The two build-tagged production variants intentionally define
the same two functions, while `type.go` defines `Name` and `IsMatch`; the test
file contains two tests. Every Go production, platform variant, test,
documentation, and Bazel artifact was read in full before comparing the Rust
owner.

## Rust owner comparison

`rust/crates/tidb-config/src/kerneltype.rs` is the dependency-closed owner and
is registered by `src/lib.rs`. Its `cfg!(feature = "nextgen")` compile-time
selection is the native equivalent of Go's `nextgen` build tag. The owner
preserves Classic/NextGen predicates, canonical names, the empty-PD-type
compatibility rule for old PD versions, and exact matching behavior. Its two
inline tests mirror the source assertions for both kernel builds.

The Rust feature is binary-wide just like the Go tag; no runtime switch,
platform fallback, generated variant, or second kernel policy was introduced.
No current-master Go source delta exists and no Rust-only behavior was
justified for removal.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/config/kerneltype -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-config --lib kerneltype -- --test-threads=1` (from `rust/`)

The Go package and both filtered Rust owner tests pass. No Go/Bazel or Rust
production source changed, so `make bazel_prepare`, failpoint toggling, and
code-change lint were not applicable. The Rust command exercises the default
Classic feature; the feature-gated NextGen branch remains covered by the
source-equivalent `cfg!` implementation and is not separately built in this
leaf run. Broader server/PD integration remains outside this audit.

This receipt certifies the bounded `pkg/config/kerneltype` inventory and
parity check; it is not a repository-wide transcreation claim.
