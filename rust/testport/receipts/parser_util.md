# `pkg/parser/util` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly four tracked artifacts and 152 text lines. Every
production, test, and BUILD line was read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 25 | `bc76160804fe08cd33f889bf995184e3afcffbc5` | library/test metadata |
| `escape.go` | 29 | `5e6041c75c3854bcbd053812a0df39f288d0481f` | MySQL backslash escape mapping |
| `escape_test.go` | 58 | `599c4d0385dc531af440ff6ba89b83d07f4780cf` | escape-byte regression table |
| `hash64.go` | 40 | `2692a1dda0700a37000ba90d41959e4ae62be198` | cascades hasher interface |

The package has one production function (`UnescapeChar`) and one test entry
point (`TestUnescapeChar`); `IHasher` is an interface with eleven methods and
no implementation. There are no generated inputs, platform variants,
fixtures, fuzz corpora, benchmarks, or additional build artifacts.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/util` is empty. The current branch
matches Go master for all single-byte escape outputs (including preserved
`\\%`/`\\_`) and the hasher method names and source-width contracts. No source
fix or new Go regression test is needed.

## Rust ownership and parity result

Ownership is intentionally split at dependency boundaries: `tidb-lexer` owns
`unescape_char` and uses it in the SQL lexer, while `tidb-hash` owns the
dependency-inversion `IHasher` trait used by planner/model hashing. Their
source-derived tests cover every byte, invalid UTF-8 Go-string input, all
method names, and the signed/unsigned width mappings. No Rust-only behavior
requiring removal was found.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./util -count=1 (from pkg/parser): PASS; 0.418s
Rust `cargo +nightly-2026-08-22 test -p tidb-lexer --test all parser_util -- --test-threads=1`: PASS; 2 tests
Rust `cargo +nightly-2026-08-22 test -p tidb-hash --test parser_util_package_source -- --test-threads=1`: PASS; 2 tests
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt.

## Risks and next boundary

- Correctness: escape handling feeds SQL string and pattern parsing; preserving
  versus removing the backslash is observable in `LIKE` and literal values.
- Compatibility: `IHasher` is a planner/model interface; changing source
  widths or byte-string semantics changes plan-cache and equality hashes.
- Performance: the lexer escape helper allocates a tiny byte vector and hash
  implementations remain caller-owned; no additional wrapper was introduced.

## Rust return-contract alignment (2026-09-07)

This follow-up reuses the complete four-artifact inventory above and does not
reopen Go code, following the user's Rust-only direction. Before editing, the
33-line `tidb-lexer/src/escape.rs`, the 68-line `tidb-hash` source owner, their
155 lines of dedicated owner tests, the 75-line planner consumer test, both
manifests, the shared 79-line aggregate-test build script, workspace/lock
entries, generated `all_tests.rs`, the duplicate inline source-table test, and
every lexer/parser/AST/planner caller were inspected. The owner paths were
byte-identical to the package receipt commit. There is no fixture, generated
source input, platform variant, feature, example, benchmark, fuzz target, or
other custom build surface for this package slice.

Go permits callers to discard `UnescapeChar`'s byte-slice result. Rust's sole
owner annotation added `#[must_use]` to `unescape_char`, producing a caller
error absent from the source package. A focused aggregate-test regression
calls it under `#[deny(unused_must_use)]`; the valid pre-fix run failed with
exactly one diagnostic and passes after removing only that annotation. The
complete `IHasher` half of the split owner is unchanged. No escape mapping,
allocation, scanner, parser, AST restoration, hashing, or planner behavior
changed.

Ready validation:

- Focused regression: `cargo +nightly-2026-08-22 test --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-lexer --test all
  parser_util_package_source::return_value_may_be_ignored_like_go -- --exact
  --test-threads=1` — passed, 1 test; its pre-fix run failed exactly once.
- Complete split-owner suites: `cargo +nightly-2026-08-22 nextest run
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-lexer -p
  tidb-hash --no-fail-fast` — passed, 99 tests with no skips.
- Owner all-target compilation: `cargo +nightly-2026-08-22 check
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-lexer -p
  tidb-hash --all-targets` — passed.
- Production consumer compilation: `cargo +nightly-2026-08-22 check
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-ast -p
  tidb-parser -p tidb-planner --lib --bins` — passed; only pre-existing
  dependency warnings were emitted.
- Scoped nightly `rustfmt --check`, repository `make lint`, and `git diff
  --check` — passed.
- The broader planner all-target test build remains independently broken in
  unchanged `core_logical_cte_topn_prune_source.rs`: its `RuleContext`
  initializer lacks the pre-existing `allow_agg_push_down` field. The owner
  and production consumer builds are clean, and this package changes neither
  planner source nor that test.

Only the Rust escape owner, its focused package regression, this receipt, and
the ExecPlans changed. No Go, Bazel, Cargo metadata, dependency, generated
input, or build-target input changed, so `make bazel_prepare` is not required.
Correctness and performance risk are minimal because the edit changes only a
compile-time caller diagnostic; compatibility improves for the direct
source-shaped return.
