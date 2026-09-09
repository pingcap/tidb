# `pkg/parser/duration` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly three tracked artifacts and 156 text lines. Every
production, test, and BUILD line was read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `e25543983dbe7242b47d807075dc45b01d996f16` | library/test metadata |
| `duration.go` | 73 | `6eb13111b04240e6bf8d4d2e655d18fe8385a878` | fractional day/hour/minute parser |
| `duration_test.go` | 65 | `4a1555db69e41ca645f7408baed38e3e4adbb76b` | duration parsing cases |

The production file contains two function declarations and the test file has
one `TestParseDuration` function. There are no generated inputs, platform
variants, fixtures, fuzz corpora, or additional build artifacts.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/duration` is empty. The Go branch
already matches Go master for decimal concatenation, `d`/`h`/`m` units,
fractional values, and zero/invalid handling. No source fix or new Go
regression test is needed.

## Rust ownership and parity result

Rust's `tidb-parser::parse_config_duration` is the dependency-closed owner and
is used by the TTL and CALIBRATE SQL consumers. Its source-derived tests cover
the Go cases plus malformed numbers, Unicode-digit ParseFloat diagnostics,
non-ASCII unit byte reporting, overflow, and consumer error wrapping. Those
additional cases make error behavior explicit without changing the Go
contract; no Rust-only behavior requiring removal was found.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/parser/duration -count=1
PASS; failpoint refcount 0

Rust `cargo +nightly-2026-08-22 test -p tidb-parser --test all duration`: PASS; 5 tests
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt.

## Risks and next boundary

- Correctness: fractional conversion and duration overflow follow Go's
  `time.Duration` nanosecond semantics; malformed-number diagnostics are
  preserved through SQL consumers.
- Compatibility: changes to accepted units or error text affect TTL and
  CALIBRATE parsing and must update both consumer tests.
- Performance: the parser remains a small allocation-free component for
  ordinary inputs; no alternate path was introduced.
