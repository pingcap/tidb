# `pkg/planner/util/fixcontrol` — Go-master parity audit receipt

Go authority: `origin/master` at
`f5cf8f6337612c6ae51fb6e384e4bb3469dde680`.

## Complete Go inventory

The package contains exactly seven tracked artifacts and 712 lines. Every
production source, test, fixture, and build target was read in full before
editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 33 | library/test targets, fixture data, and shard configuration |
| `get.go` | 177 | fix IDs and string/bool/int/float getters |
| `set.go` | 86 | source-shaped text parser and duplicate warnings |
| `fixcontrol_test.go` | 97 | SQL-driven fixture test and empty-value regression |
| `main_test.go` | 52 | `TestMain`, testdata loading, and leak guards |
| `testdata/fix_control_suite_in.json` | 20 | SQL case input fixture |
| `testdata/fix_control_suite_out.json` | 247 | recorded values, errors, warnings, and variables |

The production declaration inventory was checked function by function:
`GetStr`, `GetStrWithDefault`, `GetBool`, `GetBoolWithDefault`, `GetInt`,
`GetIntWithDefault`, `GetFloat`, `GetFloatWithDefault`, and `ParseToMap`, plus
all 17 fix-control constants. The test inventory includes `TestFixControl`,
`TestParseToMapEmptyValue`, `TestMain`, fixture loading/output recording, and
all 13 SQL cases in the input/output pair. There is no package `doc.go`,
platform/build-tag variant, generated source, or nested package beyond the two
listed fixture files.

## Rust ownership and parity

The dependency-closed owner is
`rust/crates/tidb-planner/src/fix_control.rs`. Its issue-number catalog,
source-shaped parser, duplicate warning text, Go-compatible integer/float
parsing and diagnostics, typed getters, and planner/session consumers were
re-read. The exact Go fixture pair is consumed by
`tidb-session::tests_fix_control::go_fix_control_fixture_runs_through_the_session_writer`;
that test also exercises session warning and variable plumbing. Rust had
imposed `#[must_use]` on nine source-shaped getter results:
`as_map`, the string getters, boolean getters, integer getters, and float
getters. Go callers may discard every one of those results, so the annotations
were removed. Parsing grammar, duplicate replacement/warning behavior, numeric
conversion, Go error spelling, and session integration remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards all nine
source-shaped getter results. In a temporary clean worktree at the pre-fix
revision it failed with exactly nine `unused return value` diagnostics; after
the edit it passes 1/1. The source fixture parity test passes 1/1 as well.

## Validation (Ready profile)

- Current Go-master seven-artifact inventory, declaration/test/fixture mapping,
  and BUILD target review — passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib fix_control::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly nine diagnostics (captured in `/tmp/tidb-codex/fixcontrol-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib fix_control::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-session --lib tests_fix_control::go_fix_control_fixture_runs_through_the_session_writer -- --exact --nocapture` — passed (1/1; both Go fixtures consumed).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/fix_control.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The Go package uses no failpoints. This is a Rust-only batch: no Go source,
import section, Go test function, Bazel file, or module dependency changed, so
`make bazel_prepare` is not required. Full workspace tests, Bazel execution,
and cross-platform planner/session builds were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after getter regression, fixture parity
  test, and planner library check pass; parser, conversion, warning, and
  session behavior are unchanged.
- Compatibility: only Rust lint policy changed for source-shaped getters whose
  Go callers may discard results.
- Performance: no parser, map, numeric conversion, or session path changed.
- Not verified locally: full workspace tests, Bazel execution, and
  cross-platform planner/session builds.

## 2026-09-08 complete-package re-audit: hexadecimal exponent syntax

All seven artifacts above were read again before editing, including every
production declaration, both test functions and helpers, TestMain, all 13
fixture cases, and the build target. There are still no generated inputs,
platform variants, or additional package build artifacts. Master differs from
the integration checkout only in the Fix44855 explanatory comment; fixtures
and executable Go code are identical. The corresponding Rust owner was read
in full. Existing issue-ID consumers remain owned by their respective packages.

`parse_go_hex_float` previously let Rust integer overflow short-circuit exponent
syntax validation. For `0x1p18446744073709551616x` it returned infinity instead
of Go's positive zero plus `invalid syntax`. Negative exponents incorrectly
underflowed to zero without an error, and zero mantissas also hid the error.
The parser now validates all normalized exponent digits before classifying
integer overflow. A public-getter regression checks four signed, unsigned,
zero-mantissa and negative-exponent cases, exact errors and default fallback.
The existing valid overflow, underflow, underscore and rounding cases remain
covered by the package tests.

The regression failed before the fix (infinity bits `9218868437227405312`
versus expected zero). A standalone Go 1.26.2 `strconv.ParseFloat` oracle
returned zero plus the exact syntax error for all four inputs. No Go repository
file changed.

Current Ready validation (all exit 0):

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib fix_control::tests` — 7/7 passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-session --lib tests_fix_control::go_fix_control_fixture_runs_through_the_session_writer -- --exact --nocapture` — 1/1 passed, all 13 Go cases.
- `make lint` — passed using installed Go 1.26.2.
- `rustfmt +nightly-2026-08-22 --check rust/crates/tidb-planner/src/fix_control.rs` and `git diff --check` — passed.

Logs are `/tmp/fixcontrol-green-20260908.log`, `/tmp/fixcontrol-session-20260908.log`, and `/tmp/fixcontrol-ready-lint-20260908.log`. Existing dependency compiler warnings remain. Correctness risk is confined to exponent parsing; valid syntax and runtime interfaces are unchanged, and work remains linear in input length. Full workspace,
Bazel and cross-platform builds are outside this bounded validation scope.
