# `pkg/parser/goyacc` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

Go master contains exactly three tracked artifacts and 1,443 lines. Every
generator production source and Bazel build input was read before the ownership
decision. The package has 46 production function declarations and no test,
benchmark, fuzz, fixture, `testdata`, generated-output, or platform-variant
artifact inside the package.

| Go-master artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 26 | `17a097044bb95808b5474abc9342bad50eb97e8d` | goyacc library/binary metadata |
| `format_yacc.go` | 576 | `18aa16b174a2524e053bfa0f42beb1ecefa1e78d` | grammar formatter and Go snippet rewriter |
| `main.go` | 841 | `72c3d82daa8cd3629be53ba23cebf63fcd675cfc` | modernc goyacc-compatible generator CLI |

The generator consumes grammar files supplied by callers and emits parser Go
source, reports, and optional examples; those outputs are outside this package
and are not hand-edited by this audit.

## Go-master comparison

The package is added on Go master and absent from the current hparser branch.
It is a build-time tool, not a SQL runtime dependency. The current branch's
Rust parser is handwritten and does not need a goyacc runtime or a Rust clone
of this generator. No safe isolated function port exists: `main.go` couples
modernc yacc processing, parse-table generation, Go formatting, error-example
output, and all command-line flags; `format_yacc.go` additionally couples the
same parser AST to TiDB's formatter.

## Rust ownership and parity result

There is no Rust goyacc owner, generated parser consumer, or dependency-closed
equivalent to this Go build tool. `tidb-parser` owns the handwritten SQL parser
and its lexer/AST contracts directly. The correct result is an explicit tooling
boundary: do not add a speculative Rust generator, and do not remove the
handwritten parser merely to mirror an obsolete Go code-generation pipeline.
Future grammar migration must be handled with the root parser grammar,
generated outputs, AST/visitor APIs, and test corpus as one atomic change.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
Inventory/read pass: 3 artifacts, 1,443 lines, 46 functions, 0 test entries
Exact Go-master `go test ./goyacc -count=1` setup reached the package but could not download modernc.org dependencies (proxy EOF); no package test exists.
cargo +nightly-2026-08-22 test -p tidb-parser --test all -- --test-threads=1: PASS; 90 passed, 1 ignored
cargo +nightly-2026-08-22 fmt --all -- --check: PASS
Pinned-Go make lint: PASS
git diff --check: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required. The Go compile remains unverified until the pinned modernc modules
are available locally or the proxy succeeds.

## Risks and next boundary

- Correctness: generator changes alter parser tables, diagnostics, and source
  formatting; grammar and generated outputs must be regenerated together.
- Compatibility: the modernc yacc CLI flags and emitted Go interfaces are
  build-tool contracts for legacy parser consumers, not runtime APIs.
- Performance: generated table shape affects parse speed and binary size;
  replacing it with handwritten Rust requires parser benchmarks.

