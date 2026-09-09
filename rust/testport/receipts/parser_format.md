# `pkg/parser/format` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly three tracked artifacts and 661 text lines. All
production, test, and BUILD lines were read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 21 | `66842a8740fe78eb27d00dc291cf9ae75910097b` | library and test metadata |
| `format.go` | 531 | `74693f2066287454216c5bf8242fd5a83f93f707` | indent/flat formatter and restore context |
| `format_test.go` | 109 | `bc04033214cddcbec63a74d33ae1a1cf57893fb` | formatter, flags, escaping, and special-comment tests |

The production file contains 39 function/method declarations and the test
file contains three `TestXxx` functions. There are no generated inputs,
platform variants, fixtures, fuzz corpora, or additional build artifacts.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/format` is empty. The branch
already matches the Go-master formatter state: `%i`/`%u` indentation state,
flat formatting, output escaping, restore flags and precedence, keyword/name
quoting, special comments, CTE tracking, and parent-expression restore fields.
No behavior fix or regression test is needed for this package.

## Rust ownership and parity result

Rust's `tidb-ast` crate owns the corresponding restore context, flags, writer,
CTE state, and source-derived format tests. No Go-master delta or Rust-only
behavior was found in this package, and no source change or speculative adapter
was added.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/parser/format -count=1
PASS; 0.382s; failpoint refcount 0

Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go, Rust, Bazel, module, generated, or test source changed, so
`make bazel_prepare` is not required for this receipt.

## Risks and next boundary

- Correctness: restore flag precedence, quote/escape output, and formatter
  state transitions remain represented by both source tests and Rust tests.
- Compatibility: changes to restore flags or parent-expression state must be
  coordinated with every AST node and parser consumer.
- Performance: formatter writes are intentionally buffered and use direct
  `io.StringWriter` calls; no alternate path was introduced.

Keep this package aligned with the Rust AST owner; any future flag addition
must be audited as an AST-wide change rather than a local helper edit.
