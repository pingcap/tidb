# `pkg/parser` root — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The root package contains exactly 33 tracked artifacts and 64,892 lines in the
pinned Go-master tree. Every production source, test, grammar input, generated
output, metadata/build input, and repository-facing support file was read before
the ownership decision. The root has 345 function declarations, including 150
`Test`, `Benchmark`, or `Fuzz` entry points. There are no root-level fixture,
`testdata`, platform-variant, or binary build artifacts.

| Go-master artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `.editorconfig` | 12 | `06f71386fed1616f980f6d14362010d89cd3d058` | editor metadata |
| `.gitignore` | 6 | `e4e219a816cdbfd4a0d1b0c2c13bc8fc2de2aa6f` | ignore metadata |
| `BUILD.bazel` | 69 | `4bd7b970c378a4a19c04349628a31e63032fc6e3` | Bazel metadata |
| `LICENSE` | 201 | `261eeb9e9f8b2b4b0d119366dda99c6fd7d35c64` | license |
| `Makefile` | 36 | `f7a203f03951b199ded7e25f9937a23829459fc6` | build/test targets |
| `OWNERS` | 13 | `b33b669a7acadf6a928d004d8c92d3901982ff34` | ownership metadata |
| `README.md` | 70 | `5bdb476dd4c21543301067f73f466cba0da7ad3a` | documentation |
| `SECURITY.md` | 31 | `4e315b4267150c63efca074edcbb0981e7fe2c68` | security metadata |
| `bench_test.go` | 66 | `8057b9fa393be9676321373f3b24f85f0a881814` | benchmarks |
| `consistent_test.go` | 99 | `c8f00f05f1eaebe5ef3821018b790e29cf939113` | consistency tests |
| `digester.go` | 688 | `f8ca68225cfe690ab6ed73f842fbcf0506ccd0b7` | SQL digester |
| `digester_test.go` | 264 | `4e655561fb7dab3518f17b67aa25b164f391a9ce` | digester tests |
| `generate.go` | 4 | `5f2d38640cb27a9182ed33f05a7669759dd67882` | goyacc directive |
| `go.mod` | 31 | `55ac7596d69989780f7529499f4eaac5fa35a3f8` | module metadata |
| `go.sum` | 95 | `1d09bf9277f4a7723f86b73b9988515e8682f539` | module checksums |
| `hintparser.go` | 1,699 | `156c8b7884a000071ef505f885023f4840d8d533` | generated hint parser |
| `hintparser.y` | 869 | `19b0f26adc35bd68f3f878bd8a0d474b79fcd8ad` | hint grammar |
| `hintparser_test.go` | 528 | `7b65abb3103592bed06fe6c6202b82880083fd66` | hint tests |
| `hintparserimpl.go` | 234 | `56a22ac083b11aed190307cb83ba34460ad9eea8` | hint implementation |
| `keywords.go` | 717 | `0ee0555199d127e209ba2aab1d99ad9c77ad1005` | generated keywords |
| `keywords_test.go` | 57 | `da84fe952b82b2225e52171587a1847be7f1f7b8` | keyword tests |
| `lateral_test.go` | 208 | `9d17a5732a480834cb753ca75401954faf607730` | lateral join tests |
| `lexer.go` | 1,160 | `ca1807b3bb530ad373077b0217ebae773460fbde` | SQL lexer |
| `lexer_test.go` | 722 | `05ecfeb77d452da4425a9f5b20ce3950f43b5007` | lexer tests |
| `main_test.go` | 34 | `6134c27b92c9cdbb7272ffbb882726066c7f216b` | test setup |
| `misc.go` | 1,195 | `5d26704f9dffed126bbcf2a4f31493cdee079291` | parser helpers |
| `mview_stmt_options.go` | 31 | `26b358724f0355785a46193dee48501b1b608e90` | materialized-view options |
| `parser.go` | 28,165 | `1262fb09b16d8eef557d3e087f96478fe945e4a7` | generated SQL parser |
| `parser.y` | 18,345 | `6613e7c811de04b404f395603208914ad8f098fd` | SQL grammar |
| `parser_test.go` | 8,604 | `d981841ec547bfad90167ae25883ebcd505403bf` | parser tests |
| `reserved_words_test.go` | 117 | `9d7f0b2f01c8b4c5cec1454b845e100a459057e6` | reserved-word tests |
| `test.sh` | 3 | `3f2b18cdf6e4926c39c5ad5b35dc9529bc29c991` | test helper |
| `yy_parser.go` | 519 | `af886c7038528bf560b065a7904fa0f087e15830` | parser runtime wrapper |

`parser.go` and `hintparser.go` are generated goyacc outputs and must only be
changed by regenerating from `parser.y` and `hintparser.y`. `keywords.go` is a
generated keyword table. The generation directives and module/build files are
inputs to that process, not optional support files.

## Go-master comparison

The current branch's root package is behind Go master by a large generated
parser consolidation: excluding already-audited nested parser subpackages,
`git diff HEAD..origin/master -- pkg/parser` spans 89 root artifacts with
51,833 insertions and 25,238 deletions. This rewrites grammar outputs, hint
parsing, lexer/parser helpers, tests, and generation metadata together; it is
not a safe single-file or single-function cherry-pick.

## Rust ownership and parity result

`rust/crates/tidb-parser` owns a hand-written parser, lexer, hint/digest
helpers, and source-derived tests (including root parser, hint, digester,
duration, auth, charset, and reserved-word cases). Its focused suite passes,
but the Rust AST/visitor and grammar surface do not yet provide a complete
dependency-closed equivalent of Go master's generated parser and public APIs.
The correct result for this audit is an explicit package boundary: no
speculative facade and no Rust-only behavior removal. A future parity change
must migrate grammar inputs, generated outputs, AST nodes, visitor contracts,
and all Go test entries as one package-level change.

## Validation

Profile: Ready for this parser-owner visitor coverage follow-up.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test . -count=1 (from pkg/parser): PASS; 0.525s
Exact Go-master detached worktree: go test . -count=1: PASS; 0.446s
cargo +nightly-2026-08-22 test -p tidb-parser --lib -- --test-threads=1: PASS; 730 passed
cargo +nightly-2026-08-22 test -p tidb-parser --test all -- --test-threads=1: PASS; 91 passed, 1 ignored
cargo +nightly-2026-08-22 fmt --all -- --check: PASS
Pinned-Go make lint: PASS
git diff --check: PASS
```

No Go source, generated output, or Bazel/module metadata changed, so
`make bazel_prepare` is not required for this Rust test-only receipt.
Generated artifacts remain unmodified.

## Risks and next boundary

- Correctness: grammar, lexer, hint, digest, and AST changes are coupled;
  changing one generated output without its grammar and tests can alter SQL
  acceptance or diagnostics.
- Compatibility: parser AST and visitor APIs are consumed by planner,
  executor, DDL, and external parser users; a partial Rust adapter would hide
  missing node/visitor behavior.
- Performance: generated parsers and keyword tables affect every SQL parse;
  any replacement must be benchmarked against the Go package.

## Rust follow-up: exact AST visitor script coverage

The complete Go root inventory remains the 33-artifact, 64,892-line package
recorded above. The Rust owner already carried the procedure source-row and
visitor tests, but the miscellaneous AST visitor tests had only isolated
statement samples. `tests::misc::test_ddl_visitor_cover_misc` and
`tests::misc::test_dml_vistor_cover` now execute the exact Go multi-statement
scripts through `parse_multi`, including the foreign-key DDL tail,
UNION-with-hints query, LOAD DATA, and IMPORT forms. A shared test helper
applies the balanced visitor to every returned statement, preserving the Go
enter/leave traversal contract.

This is test-only owner coverage; no parser production or generated artifact
changed. The corresponding `pkg/parser/ast` AST-crate carriers remain
documentary ignored tests because `tidb-ast` cannot depend on `tidb-parser`.

Validation: the two focused visitor tests, the complete `tidb-parser` library
and aggregate test targets, pinned Rust formatting, repository `make lint`,
and `git diff --check` all pass in the current Ready run.

## Rust follow-up: parser and optimizer-hint parentheses depth

Go master `e2b6ce7333` adds a 10,000-level `maxParenthesesDepth` guard to both
the SQL scanner and the dedicated optimizer-hint scanner. Before this
follow-up, Rust tokenized all parentheses and entered its recursive expression
parser; a 10,001-level `SELECT` nesting therefore aborted with a process stack
overflow instead of returning a parser error. The Rust `tidb-parser` owner now
computes the maximum nesting from the token stream before recursive parsing,
returns `parentheses nesting depth exceeds maximum 10000` from both single- and
multi-statement entrypoints, and reports the same diagnostic from standalone
`parse_hint`.

The focused regressions are
`parser_root_source::test_parentheses_depth_limit` and
`parser_hint_source::test_max_optimizer_hint_parentheses_depth`. The former
was run before the fix and reproduced the Rust stack-overflow abort; both tests
pass after the guard. No Go source, generated parser output, or Bazel metadata
was edited. The same batch also activates the Go `maxASTDepth` contract for
11,000-term binary, unary, and nested-CASE expressions: the source-derived
tests now return `AST nesting depth exceeds maximum 10064`. Rust checks unary
and CASE recursion before descent (because those parser productions recurse),
and runs the post-parse AST visitor on a larger stack only for large token
streams so the visitor itself cannot reintroduce a stack-overflow path.

Validation for this follow-up is recorded in the package batch: focused
parser/hint/AST regressions, the full owner check and test suite, pinned Rust
formatting baseline, `make lint`, and `git diff --check`.

## Rust follow-up: parenthesized temporal `INTERVAL` expressions

Go master commit `5bdb1b6bd179eef2d2d2778eefc2e87e6f0c6ad1` adds dedicated
`FunctionCallKeyword` productions for the ambiguous `INTERVAL` spelling. The
complete root-package inventory above remains the owning-package evidence
(33 artifacts and 64,892 lines at the pinned source); this follow-up audited
the changed `parser.y` grammar, generated-parser impact, and the twelve new
`TestBuiltin` rows in `parser_test.go` before touching Rust.

Before the fix, Rust dispatched every `INTERVAL (` prefix to the generic
function parser. That accepted `INTERVAL()`/`INTERVAL(1)` (Go rejects both)
and rejected valid parenthesized temporal forms such as
`INTERVAL (q - 1) QUARTER`, including their `DATE_ADD`/`DATE_SUB` and reverse
operand rewrites. The parser now scans the balanced parenthesized value,
tracks top-level commas, and selects the temporal `Expr::Interval` form only
when a recognized time unit follows; comma-bearing forms continue through the
scalar function path. The scalar path explicitly requires at least two
arguments, matching Go's two-argument and variadic productions.

The source-derived matrix in
`tidb-parser/tests/parser_run_test_builtin_source.rs::test_builtin` now
includes the exact Go rows for invalid zero/one-argument calls, two- and
variadic scalar calls, arithmetic/date-function placement, nested `ROW`
constructors, and the `MAKEDATE` composite expression. The focused regression
failed before the parser change on `INTERVAL()` and on the parenthesized
temporal row, and passes after it.

Ready validation for this batch:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-parser --test all test_builtin -- --nocapture: PASS
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check: PASS
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint: PASS
git diff --check: PASS
```

The full `tidb-parser --test all` Ready run is the next gate before the
package batch is committed; generated Go parser artifacts remain untouched.
