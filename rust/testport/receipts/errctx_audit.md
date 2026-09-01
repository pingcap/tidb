# `pkg/errctx` — Go-master parity audit receipt

Status: complete dependency-closed audit with no source behavior delta found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly three tracked artifacts and 389 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 29 | library/test targets and dependencies |
| `context.go` | 266 | levels, error groups, context copies, and handlers |
| `context_test.go` | 94 | source `TestContext` behavior suite |

There is no package `doc.go`, fixture/testdata directory, generated Go
source, platform variant, benchmark, or nested package. The production file
contains 13 function/method declarations (including `init`), and the test
file contains one test declaration. All three Go artifacts were read in full
before comparing the Rust owner.

## Rust owner comparison

`tidb-error::errctx` already covers the complete source surface: `Level`,
`LevelMap`, all seven `ErrGroup` values and their 18 error-code memberships,
copy-on-write context constructors, warning/note appenders, error-group
handling, strict context, and `ResolveErrLevel`. The Rust source test mirrors
the Go `TestContext` and pins every `errGroupMap` entry and the precedence
rule when both `ignore` and `warn` are set.

The `WarnAppender` trait and concrete `MultiError` are deliberate local
interface seams for Go's `pkg/util/context.WarnAppender` and
`pingcap/errors.ErrorGroup`; they are not Rust-only execution policy. No
production edit or duplicate regression carrier was justified, and no Rust-
only behavior was removed.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/errctx -count=1`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-error --test all errctx -- --test-threads=1` (run from `rust/`)

Both targeted suites pass. No code or generated artifact changed, so
`make bazel_prepare` and the code-change portion of the Ready profile were
not applicable. Broader session/executor integration and the Go dependency
packages remain outside this leaf audit.

This receipt certifies the bounded `pkg/errctx` inventory and parity check;
it is not a repository-wide transcreation claim.
