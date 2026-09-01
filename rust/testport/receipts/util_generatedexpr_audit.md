# `pkg/util/generatedexpr` — Go-master parity boundary receipt

Status: audited, but unclaimed as a package-complete transcreation. The
metadata model owner is still a broader `tidb-model` seed, so this receipt
records the complete leaf inventory and current behavior without claiming
that the surrounding `pkg/meta/model` package is closed.

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete Go inventory

The package has exactly four tracked artifacts and 181 Go lines:

| Artifact | Lines | Inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 35 | one library and one short test target |
| `generated_expr.go` | 84 | generated-expression parsing and column-name visitor |
| `gen_expr_test.go` | 29 | `TestParseExpression` |
| `main_test.go` | 33 | `TestMain` goleak/common test harness |

Every production, test, harness, and build artifact was read in full before
editing. There is no `doc.go`, README, fixture/testdata directory, generated
or platform-specific source, benchmark, fuzz target, example, or extra build
variant. The production file has four declarations (`Enter`, `Leave`,
`ParseExpression`, and `SimpleResolveName`), and the package has two test
functions including `TestMain`.

## Rust ownership and comparison

`rust/crates/tidb-model/src/generated_expr.rs` (201 lines) owns the parser and
name-resolution behavior, with five focused unit tests. `tidb-model` exports
this module but explicitly remains a broader Go-package seed; table metadata,
serde, DDL, and executor consumers are outside this leaf receipt.

Go master commit `8c38aa4e6a` migrates the visitor from the replacing
`StmtNode.Accept` API to `ast.Walk` and changes `Enter`/`Leave` signatures.
The visitor's predicate and short-circuit semantics are unchanged. Rust
already uses a non-replacing visitor, matches columns case-insensitively, and
returns the unchanged expression after validation. No Rust-only adapter or
production behavior needed removal, and no speculative API was added.

## Validation

Profile: WIP for a continuing seed-owner audit; a package-complete Ready claim
is intentionally not made.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/generatedexpr -count=1` — passed against Go master.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-model --lib generated_expr -- --test-threads=1` — five Rust owner tests passed.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check rust/crates/tidb-model/src/generated_expr.rs` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed for the repository Ready gate before this documentation-only receipt.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

- The surrounding `pkg/meta/model` package remains a seed with broader
  metadata and consumer integration gaps; this receipt does not overstate the
  generated-expression leaf as package-complete.
- Rust's parser error type and metadata table integration are validated by
  their own owners; cross-package executor/DDL flows remain outside this
  focused run.
- No production performance behavior changed in this audit.
