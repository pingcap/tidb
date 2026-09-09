# `pkg/parser/opcode` — complete package parity receipt

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (`origin/master`).

## Complete inventory

The package contains exactly three tracked artifacts and 310 text lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 17 | library/test metadata |
| `opcode.go` | 246 | operator enum, names, literals, keyword bits, and four methods |
| `opcode_test.go` | 47 | one table/format smoke test (`TestT`) |

No `doc.go`, generated inputs, platform variants, fixtures, fuzz corpora,
benchmarks, or additional build artifacts exist. Every artifact was read in
full before editing.

## Restored Go behavior

Go master removes the obsolete fixed `len(ops) == 32` assertion from
`TestT`. The assertion made harmless additions to the operator table fail
without testing behavior; the test's per-op `Format`/`String` checks remain
the focused regression. The production operator table already matched Go
master, including the absence of the stale `Binary` opcode.

## Rust ownership and parity result

The Rust `tidb-ast` operator table is dependency-closed and already matches
the Go table. No Rust-only operator or behavior remains, and no Rust source
change was needed in this batch.

## Validation

Profile: Ready.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 tools/check/failpoint-go-test.sh ./pkg/parser/opcode -run '^TestT$' -count=1
PASS
make lint
git diff --check
make bazel_prepare (blocked: bazel executable is unavailable)
```

Because only an existing Go test changed, the package has no new production
API or runtime risk. Bazel regeneration remains the only unavailable gate.

## Rust follow-up: Go-discardable operator helpers

The complete three-artifact inventory and dependency-closed operator table
above remain unchanged. The Rust owner had four explicit `#[must_use]`
annotations on the public `Op::value`, `Op::name`, `Op::literal`, and
`Op::is_keyword` helpers. These correspond directly to Go's `Op` methods,
whose return values are freely discardable; the annotations therefore added
Rust-only diagnostics and are now removed. Operator values, names, literals,
keyword bits, and formatting are unchanged.

The source-derived regression
`parser_opcode_package_source::return_values_may_be_ignored_like_go` discards
all four helper results under `#[deny(unused_must_use)]`. On the pre-fix owner
it failed to compile with four unused-return errors; the fixed test passes.

Ready validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-ast --test all parser_opcode_package_source::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-ast --test all -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-ast --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-ast -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.
