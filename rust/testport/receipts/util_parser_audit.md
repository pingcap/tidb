# `pkg/util/parser` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-02).

## Complete package inventory

The package has exactly six tracked artifacts and 581 Go lines:

| Artifact | Lines | Inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 38 | `88944f2d102ca861df79a40921c7bcb738110d53`; one library and one flaky short-test target |
| `ast.go` | 158 | `b586e6ceb0c8af31cf7f32142c15112f67476dc5`; default-database visitor, simple INSERT qualification, restore helpers, and table-position scanner |
| `ast_test.go` | 64 | `52b4524901df16f6455e073fd434ee790f139d9c`; `TestSimpleCases` source regression |
| `main_test.go` | 33 | `b11a48b6bc51479de087e58ca2ecd8bd08f3072b`; `TestMain` goleak/common test harness |
| `parser.go` | 118 | `e30e8d4eafbd9a11bf986ff0df4610c1511b8e1a`; parser pool lifecycle, byte-pattern matchers, Unicode byte classes, and number conversion |
| `parser_test.go` | 170 | `6e03ad51c20f40721569c7493fe021d381d575ca`; `TestSpace`, `TestDigit`, `TestNumber`, and `TestCharAndAnyChar` |

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

The pinned Go-master tree changes `GetDefaultDB` from the replacing
`StmtNode.Accept` visitor API to `ast.Walk` and updates the visitor signatures.
The traversal contract is unchanged. Rust already uses a non-replacing visitor
and preserves the same early-stop predicate, so no production adapter or
Rust-only behavior needed removal. A focused regression now covers multiple
qualified tables and a later implicit table.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/parser -count=1` — PASS on the current branch; 0.445s.
- Exact pinned Go-master detached worktree: `go test -count=1` from `pkg/util/parser` — PASS; 0.482s.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-parser --test all util_parser -- --test-threads=1` — seven tests passed, including the new regression.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — PASS.
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
