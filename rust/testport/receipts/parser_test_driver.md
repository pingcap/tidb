# `pkg/parser/test_driver` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly six tracked artifacts and 1,274 text lines. Every
production, test, and BUILD line was read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 36 | `4104f002f7c3fd32299d86d723d3ba9a38e2cfc1` | library and in-place visitor test metadata |
| `accept_in_place_test.go` | 122 | `9f61ef120c7d29627571a6db0d115f04ea8e5e9f` | AST-source regression for skip-children guards |
| `test_driver.go` | 246 | `5a247742adc5536a0d6a4bd3a44c42ed425a3887` | parser value/parameter driver nodes |
| `test_driver_datum.go` | 512 | `db164e502c85b1cb51d3e9ba78c08e98010f6b14` | datum and literal conversion helpers |
| `test_driver_helper.go` | 72 | `129266cb40260de573f9124e6beacfded575bb3b` | type/charset helper functions |
| `test_driver_mydecimal.go` | 286 | `33bb06e0319c9e2013708284ba035103567808da` | decimal representation and conversion |

The production files contain 72 function declarations and the source-derived
test contributes one `TestAcceptInPlaceHonorsSkipChildren` entry point. There
are no generated inputs, platform variants, fixtures, fuzz corpora, or build
artifacts beyond the BUILD target and the embedded production source test.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/test_driver` is a focused
154-line addition and one-line deletion: Go adds two `AcceptInPlace` methods
to `ValueExpr` and `ParamMarkerExpr`, plus a source-inspection regression and
its Bazel test target. Those methods require `ast.InPlaceVisitor` and
`ast.Walk`, which are part of the larger AST migration recorded in
`receipts/parser_ast.md`; the current branch intentionally does not expose
that API yet. Copying this delta alone would not compile and would violate the
dependency-closed package boundary.

## Rust ownership and parity result

No Rust crate currently owns the parser test-driver value/decimal bridge or
the missing in-place visitor API. `tidb-ast` has a mutable visitor surface, but
not the Go-master `InPlaceVisitor`/`Walk` contract, and no Rust test-driver
consumer closes the dependency graph. No Rust-only behavior was found to
remove and no safe standalone implementation was identified. This package is
therefore recorded as an explicit dependency boundary pending the complete
parser AST/driver migration.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./test_driver -count=1 (current branch, no test files): PASS
Pinned Go master worktree: go test ./test_driver -count=1: PASS; 0.283s
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt. The Go-master test was run in a detached worktree
at the pinned commit so the AST-dependent regression was validated without
altering the current branch.

## Risks and next boundary

- Correctness: the two in-place methods must preserve `Enter`'s
  `skipChildren` result and always invoke `Leave`; implementing them before the
  AST API lands would create an incomplete traversal contract.
- Compatibility: parser consumers and generated AST nodes must migrate as one
  package-atomic change, including the Bazel test embedding metadata.
- Performance: in-place traversal is intended to avoid node replacement and
  allocations; a compatibility adapter could silently regress that property.
