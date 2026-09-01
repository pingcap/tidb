# `pkg/util/parser` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete package inventory

The package has exactly six tracked artifacts and 581 Go lines:

| Artifact | Lines | Inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 38 | one library and one flaky short-test target |
| `ast.go` | 158 | default-database visitor, simple INSERT qualification, restore helpers, and table-position scanner |
| `ast_test.go` | 64 | `TestSimpleCases` source regression |
| `main_test.go` | 33 | `TestMain` goleak/common test harness |
| `parser.go` | 118 | parser pool lifecycle, byte-pattern matchers, Unicode byte classes, and number conversion |
| `parser_test.go` | 170 | `TestSpace`, `TestDigit`, `TestNumber`, and `TestCharAndAnyChar` |

Every production, test, harness, and build artifact was read in full before
editing. There is no `doc.go`, README, fixture/testdata directory, generated
or platform-specific source, benchmark, fuzz target, example, or extra build
variant. The Go source has 18 production declarations and six test functions.

## Rust ownership and parity decision

The owner is `rust/crates/tidb-parser/src/util_parser.rs` (334 lines), with
the complete source-shaped carrier in
`rust/crates/tidb-parser/tests/util_parser_package_source.rs` (now 181 lines)
and the existing `tidb-parser` integration-test target. Rust maps
`GetDefaultDB`, `SimpleCases`, both restore helpers, all matcher/Unicode-byte
helpers, number conversion, and the parser error type. Parser pooling is
intentionally represented by cheap per-call parser construction because the
Rust parser has no reusable goyacc object; no cache-only replacement path is
introduced.

Go master commit `8c38aa4e6a` changes `GetDefaultDB` from the replacing
`StmtNode.Accept` visitor API to `ast.Walk` and updates the visitor signatures.
The traversal contract is unchanged. Rust already uses a non-replacing visitor
and preserves the same early-stop predicate, so no production adapter or
Rust-only behavior needed removal. A focused regression now covers multiple
qualified tables and a later implicit table.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/parser -count=1` — passed against Go master.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-parser --test all util_parser -- --test-threads=1` — seven tests passed, including the new regression.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check rust/crates/tidb-parser/tests/util_parser_package_source.rs` — passed; the workspace-wide check currently reports unrelated formatting-only changes in `tidb-util/src/topsql_stmtstats/aggregator.rs`.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed after this test/receipt batch.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

- Correctness is limited to the leaf visitor/matcher and restore contracts;
  higher-level binding consumers are covered by their own package boundaries.
- The Go API migration itself is source-compatible only within Go's visitor
  interfaces; Rust's public parser API is unchanged.
- No production performance behavior changed; the new test only exercises
  existing traversal short-circuiting.
