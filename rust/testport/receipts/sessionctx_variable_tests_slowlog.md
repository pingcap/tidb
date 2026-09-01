# `pkg/sessionctx/variable/tests/slowlog` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

This nested test package is a separate claim from the 31-artifact
`pkg/sessionctx/variable` root inventory. It contains tests only; no Rust-only
production behavior was found to remove and no safe live-session accessor
implementation can be added without the cross-crate `SessionVars`, statement
context, execution-details, and slow-log writer owners.

## Complete inventory

Exactly three tracked artifacts (766 lines) were read before editing:

| artifact | lines | contents |
| --- | ---: | --- |
| `BUILD.bazel` | 25 | flaky short `go_test`, ten shards, executor/sessionctx/slowlogrule/stmtctx/variable, execdetails, mock, testify, TiKV util, testsetup, and goleak dependencies |
| `main_test.go` | 34 | `TestMain`, common test setup, and goleak allowlist |
| `slow_log_test.go` | 707 | ten tests covering field-accessor registration, six live rule-matching cases, typed execution-detail fields, single-field parsing, session/global rule parsing, and rule grouping |

There are no production files, fixtures, generated/platform variants, fuzz or
benchmark inputs, or additional nested build targets. The test harness creates
only process-local setup state.

## Owner comparison and parity decision

The parser half of the package is executable in Rust's
`tidb-exec::slow_log_parse`: it owns the 39-field parse-kind table, typed value
conversion, session/global rule grammar, canonical encoding, connection-ID
grouping, and CRC64 hashing. Rust source tests cover those parser contracts.
The logical AND/OR and session→specific-connection→global precedence are
executable in `tidb-exec::slow_log_match`, with aggregate source tests.
Typed threshold equality, same-type `>=`, zero handling, and signed-to-unsigned
guards are covered by `slow_log_threshold` and its source tests.

The remaining four accessor/match behaviors require live Go `SessionVars`,
`SlowQueryLogItems`, `StmtContext`, and `execdetails` mutation/writer wiring;
the Rust owners intentionally expose only the dependency-closed logical and
typed leaves. The ten Go test names and their status remain traceable in the
historical `rust/testport/receipts/b011.md`; no empty stub was removed in this
batch because those gaps still have no executable owner.

## Validation (Ready profile)

The exact Go-master nested suite passed with the repository failpoint wrapper,
including enable/disable teardown:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable/tests/slowlog -count=1
```

Rust aggregate slow-log owner tests passed (the crate intentionally aggregates
module-safe tests under one `all` target):

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec --test all slow_log -- --test-threads=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The Ready lint gate passed. No Go, import, Bazel, or module file changed, so
`make bazel_prepare` is not required for this documentation-only checkpoint.

## Risks and unverified surfaces

The parser and logical leaves are deterministic Rust owners, but full field
accessor matching remains unverified in Rust because its source dependencies
are not yet dependency-closed. The complete Go package's slow-log integration,
goleak lifecycle, and ten-shard Bazel execution were not reproduced locally;
the exact Go test command above is the authoritative focused evidence.
