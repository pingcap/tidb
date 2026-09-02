# `pkg/parser` materialized-view DDL parity receipt

Go authority: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (the rolling
`origin/master` checkout). The syntax was introduced by Go commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`parser: port materialized view
DDL syntax`).

## Complete inventory before editing

The root `pkg/parser` package contains exactly 33 tracked artifacts and 64,956
Go/Bazel lines at the authority revision: 10 production Go files, 10 test Go
files, the generated parser and keyword outputs, the `parser.y` and
`hintparser.y` grammar inputs, module/build metadata, and repository support
files. The root artifacts are `.editorconfig`, `.gitignore`, `BUILD.bazel`,
`OWNERS`, `README.md`, `LICENSE`, `SECURITY.md`, `Makefile`, `bench_test.go`,
`consistent_test.go`, `digester.go`, `digester_test.go`, `generate.go`,
`go.mod`, `go.sum`, `hintparser.go`, `hintparser.y`, `hintparser_test.go`,
`hintparserimpl.go`, `keywords.go`, `keywords_test.go`, `lateral_test.go`,
`lexer.go`, `lexer_test.go`, `main_test.go`, `misc.go`,
`mview_stmt_options.go`, `parser.go`, `parser.y`, `parser_test.go`,
`reserved_words_test.go`, `test.sh`, and `yy_parser.go`. There are 346 function
declarations and 150 `Test`/`Benchmark`/`Fuzz` entry points. There is no root
fixture directory, platform variant, or binary artifact.

The nested Go `pkg/parser/ast` package was also read in full because the Go
delta adds its statement nodes and generated visitor coverage. It contains 36
tracked artifacts and 34,448 lines: 22 production/test files at the package
root, the `testdata/visitor_benchmark_master_test.go` fixture, generated
`visitor_inplace_generated.go`, and the six `visitor_codegen` source/build
artifacts (`BUILD.bazel`, `cmd/BUILD.bazel`, `cmd/main.go`, `generator.go`,
`generator_test.go`). It has 1,360 function declarations and 151 test/
benchmark/fuzz entry points. No platform variant is present.

## Go-to-Rust behavior map

The Rust owner is `tidb-ast` plus `tidb-parser`:

- `tidb-ast/src/ddl/materialized_view.rs` models every Go statement and
  clause, restores canonical names/options/string literals, and participates
  in the mutable visitor contract.
- `tidb-ast/src/stmt/ddl.rs`, `label.rs`, and `sem.rs` expose the six DDL
  variants to restore, visitor, statement labels, and semantic command text.
- `tidb-parser/src/ddl/materialized_view.rs` ports the Go grammar for create,
  alter, and drop materialized views/logs, including duplicate-option and
  purge-clause diagnostics. `statement.rs` selects the log forms before their
  view prefixes, matching Go's grammar precedence.
- `tidb-lexer/src/keywords.rs` is synchronized with the Go `tokenMap` at this
  revision (`ALERT`, `FAST`, `IMMEDIATE`, and `MATERIALIZED` were the missing
  generated entries; the table now has 811 entries).
- `tidb-parser/src/tests/materialized_view_source.rs` covers all 12 Go source
  round-trip/type forms, parenthesized queries, duplicate options, ordering,
  and incomplete purge clauses.

The Go change is parser/AST syntax only. The Go DDL executor does not provide
a dependency-closed materialized-view refresh/log worker in this source
delta, and Rust likewise leaves execution unsupported with its existing clear
unsupported-DDL response. No Rust-only materialized-view behavior was found
or removed, and no speculative executor was added.

## Validation

Profile: Ready for this parser-package batch. The package implementation and
regression tests are complete for the Go syntax delta; downstream compile
checks ensure the new `DdlStmt` variants do not break AST consumers.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-parser --lib materialized_view -- --nocapture
PASS: 5 focused tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor -p tidb-session
PASS

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-parser --lib -- --test-threads=1
PASS: 730 parser-library tests

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
PASS

git diff --check
PASS
```

`make bazel_prepare` is not required for this batch: no Go source, Go test,
Bazel file, module file, or Bazel target changed. The exact Go parser suite and
full Rust workspace test suite were not rerun locally; the focused parser
tests, downstream Rust checks, formatting, lint, and diff gates were run.

## Risks and follow-up boundary

- Correctness: expression parsing in `START WITH`/`NEXT`, option ordering, and
  canonical restore must remain aligned with Go's generated grammar.
- Compatibility: these AST variants are now visible to parser/privilege and
  digest consumers, while execution remains intentionally unsupported until a
  dependency-closed DDL owner exists.
- Performance: the additions are parser/AST allocations only and do not alter
  existing statement paths.
