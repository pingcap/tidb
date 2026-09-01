# `pkg/session/test/variable` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 593 lines. Every test
line, helper method, and BUILD declaration was read before comparing Rust.
There is no production source, `doc.go`, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 33 | `d847b42d2e10a7c3484ea68ff0c1e78b1e73ec11` | `0e2d25edd249497d9524b11868191d8e0834318968e36173173a9cb0426455d7` | flaky twelve-shard variable test target and dependency inventory |
| `main_test.go` | 62 | `e6425eb22a6121822bcbebc5e6ea3506bf00f6f4` | `284eef2f69256b1b19c8bedcc0c0260516893dac93b0dc076b1e987475eb189b` | TestMain, goleak, failpoint, and async-commit harness |
| `variable_test.go` | 498 | `710fe048abfd11d5f82210c9212f4290d9f28d3d` | `d1fbb7cac1a0cb2297061a77a97ebce32b27c190b3fa8e422d930b5cf300d68f` | twelve variable, coprocessor, execution, replica, and logging tests |

The package has sixteen functions: TestMain, twelve top-level tests, and
three `mockZapCore` methods. The tests cover mutually exclusive snapshot and
stale-read variables, coprocessor OOM/rate-limit actions, dynamic variable
scope errors, DML batch-size loading, rate-limit action state, execution-time
hints, classic replica-read and isolation-engine settings, last-query RU
information, zap-core capture, nonzero transaction start timestamps, and
binary/text general-log rendering.

## Rust ownership and explicit boundary

Rust has adjacent executable owners for the selected session-variable
contracts: `tidb-session` tests cover isolation-read engine propagation,
statement hints, autocommit and typed variable access, while `tidb-vardef`
and `tidb-session::sysvar` cover variable registration, scope validation,
replica-read defaults, and max-execution-time domains. The workspace also
contains storage/client request metadata for replica routing.

There is no dependency-closed Rust equivalent of this package's mock
coprocessor rate-limit/OOM failpoints, query RU-information accounting,
general-log zap-core interception, snapshot/staleness mutual exclusion, or
the full session-to-coprocessor dynamic-scope lifecycle. These tests cross
session, planner, coprocessor, logging, and memory authorities; adding a
local substitute would create a second source of truth. No Rust-only
behavior was found to remove, and no safe missing behavior can be implemented
in this test-only package. It is therefore recorded as an explicit
SEED/boundary; remaining parity belongs to coordinated session, vardef,
coprocessor, planner, memory, and logging owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/variable -count=1)
# passed: pkg/session/test/variable (10.868s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so a Rust owner test was not applicable. Not verified here:
Bazel execution, full Go repository tests, live TiKV/TiFlash replica routing,
coprocessor rate-limit/OOM failpoint timing, or a future dependency-closed
Rust logging and query-accounting implementation. Compatibility and
performance risk are unchanged because this batch modified documentation
only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
