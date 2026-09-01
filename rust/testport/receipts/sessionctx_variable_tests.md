# `pkg/sessionctx/variable/tests` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

This nested integration/test package is a separate claim from both the
`pkg/sessionctx/variable` root and its `tests/slowlog` child. It is test-only;
the Rust session-variable and executor owners are tracked below. No Rust-only
behavior was found to remove, and no safe package-local implementation can
replace the remaining live TestKit/Domain/SessionVars dependencies.

## Complete inventory

Exactly four tracked artifacts (1,904 lines) were read before editing:

| artifact | lines | contents |
| --- | ---: | --- |
| `BUILD.bazel` | 43 | flaky short `go_test`, 47 shards, and the complete config/parser/planner/sessionctx/testkit/types/util dependency closure |
| `main_test.go` | 35 | common setup and goleak/TestMain harness |
| `session_test.go` | 1,083 | 18 session/executor tests for system-variable writes, session counters, slow-log formatting, isolation, savepoints, plan cache, hooks, chunks, concurrent user variables, status, row IDs, and optimizer/storage settings |
| `variable_test.go` | 743 | 29 registry, error, registration, typed validation, native-value, scope, dependency-ordering, cache, and instance-variable tests |

There are no production files, fixtures, generated/platform variants, fuzz or
benchmark inputs, or additional nested build targets. TestKit creates only
temporary in-memory stores and process-local setup state.

## Owner comparison and parity decision

The registry/validation/native-value/scope/cache/dependency-order leaves are
executable in `tidb-session::sysvar`; source tests cover the current registry,
typed validation, native Datum conversion, cache skip policy, and dependency
ordering. Slow-log formatting is executable in `tidb-exec::slow_log_format`,
while parser, typed threshold, and logical rule leaves are covered by the
separate slow-log receipt.

The remaining session tests need the full cross-crate TestKit/Domain,
StatementContext, storage transaction, plan-cache, hook, execution-details,
and session-variable mutation owners. The historical `b010`/`b011`/`b012`
receipts retain source-to-stub traceability; this receipt records the current
owner split and does not claim the nested package is transcreated.

## Validation (Ready profile)

The exact Go-master focused scalar registry/validation subset passed with
failpoint enable/disable:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable/tests -run 'Test(SysVar|IntValidation|UintValidation|EnumValidation|DurationValidation|FloatValidation|BoolValidation|TimeValidation|GetNativeValType|ScopeToString|SkipSysvarCache|OrderByDependency)$' -count=1
```

The full nested package command was also attempted. It reached the tests but
failed in Go's existing `TestTiDBOptPartialOrderedIndexForTopNSessionAndGlobal`
with a `TestHookContext` assertion panic emitted from a background bootstrap
goroutine; no Rust source was changed in response to that unrelated failure.

Rust owner checks and repository lint passed:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec --test all slow_log -- --test-threads=1
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib 'sysvar::tests::' -- --test-threads=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go, import, Bazel, or module file changed, so `make bazel_prepare` is not
required for this documentation-only checkpoint.

## Risks and unverified surfaces

The focused Go scalar tests and Rust leaf owners do not prove the full
session/executor behavior. The 47-shard Bazel target, complete TestKit suite,
storage-backed transaction/savepoint paths, concurrent user-variable test,
and live slow-log field population remain unverified in Rust until their
dependency-closed owners land.
