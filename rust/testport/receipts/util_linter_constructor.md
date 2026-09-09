# `pkg/util/linter/constructor` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This package is a static-analysis marker, not runtime behavior.

## Complete inventory

Both Go-master artifacts were read in full. There are no package docs, tests,
fixtures, generated/platform variants, benchmarks, fuzz targets, or nested
packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `92335250d2f1a90fa06f217f664fa5c5f9bff066` | `1d665d73dd770ae3ca9e14fc8537468bf1929d99695d0b44134287b280bbc9f0` | public `go_library` target for the marker |
| `constructorflag.go` | 26 | `3be9b5c824eabba3338aefd5c8b1ec8f2215ebf0` | `1033ed0113637375b713cff335cbc60bcb304a244ae433149853b69ab73ecb09` | empty exported `Constructor` field marker consumed by the Go linter |

The sole production declaration is the zero-sized exported `Constructor`
struct. Embedding it with a `ctor:"New..."` tag tells the Go `constructor`
linter which constructor is allowed to initialize a type; it has no methods,
runtime state, serialization, or executable test contract.

## Rust ownership and integration decision

No Rust crate defines or consumes an equivalent marker, and the Rust workspace
does not run Go's AST linter. Adding a marker type or a second linter policy
would have no runtime consumer and would be Rust-only behavior. The package is
therefore explicitly unclaimed; no source edit is justified.

## Validation

Profile: **WIP**. This is a complete two-artifact inventory with no code
change, so `make bazel_prepare` and the Ready lint gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/linter/constructor -count=1
# ? github.com/pingcap/tidb/pkg/util/linter/constructor [no test files]
```

## Risks and unverified behavior

- Correctness: the marker and its public package path match Go master; no Rust
  implementation is claimed.
- Compatibility: the `ctor` struct tag is interpreted by an external Go
  linter and is not observable in a Rust binary.
- Performance: zero-sized marker only; no runtime code changed.
- Not verified locally: Bazel analysis and the repository's linter plugin
  invocation that consumes constructor tags.
