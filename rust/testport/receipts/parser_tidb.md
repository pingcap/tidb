# `pkg/parser/tidb` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly two tracked artifacts and 75 text lines. Every
production and BUILD line was read from the pinned tree before the ownership
decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 8 | `114b4b609589d1a3a226488c13e21f33cf96b69c` | public library metadata |
| `features.go` | 67 | `1adcbe82d80f61569009392fa309b29c67a49255` | TiDB feature identifiers and parser allowlist |

`features.go` contains one `CanParseFeature` function and twelve exported
feature constants. There are no tests, fixtures, generated inputs, platform
variants, fuzz/benchmark targets, or additional build artifacts.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/tidb` is empty. The current branch
matches Go master for the feature identifier spellings and the all-features
allowlist, including the intentionally excluded `resource_group` constant.
No source fix or new regression test is needed.

## Rust ownership and parity result

No Rust crate currently consumes or exposes this parser feature-ID registry.
The Rust parser and AST carry individual SQL features through their native
AST/planner paths, but no dependency-closed equivalent of Go's public
`CanParseFeature` variadic allowlist exists. Adding an unreferenced facade
would invent an API without a consumer; no Rust-only behavior was found to
remove. This two-file package remains an explicit ownership boundary.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./tidb -count=1 (current branch): PASS; no test files
Pinned Go master worktree: go test ./tidb -count=1: PASS; no test files
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt. There is no focused Rust test target because no
Rust owner or executable feature registry exists.

## Risks and next boundary

- Correctness: feature IDs are parser comment/API tokens; changing a spelling
  can silently reject or accept feature-gated SQL.
- Compatibility: a future Rust registry must preserve the exact allowlist and
  public empty TiDB feature ID while integrating with parser, planner, and
  compatibility checks.
- Performance: the Go helper is a small map lookup; no Rust facade or runtime
  path was added, so there is no current performance impact.
