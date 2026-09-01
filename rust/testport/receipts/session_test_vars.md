# `pkg/session/test/vars` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 638 lines in the
comparison source. Every test line and BUILD declaration was read before
comparing Rust. There is no production source, `doc.go`, fixture directory,
generated output, benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 31 | `021a7f5c2439fb4125fb7e8609f9bfc221a18f3b` | `d5d69d21230e73dcf05d7c316dc9799fa509f36431409fe3bcc62a38043cf33c` | flaky twelve-shard variable test target and dependency inventory |
| `main_test.go` | 62 | `e5904501fe47157d72b4933a0871be70c5aaaa32` | `4743a7b9d0e2ee25f6bc83fc4efba179e896329b30aaaea5adaaac7bf9bd6c72` | TestMain, goleak, failpoint, and async-commit harness |
| `vars_test.go` | 545 | `43571ea0254782e61dd712e8b99efb859983bbe5` | `034712a2828e9f565009ebcefff4ef16d6414111bd45a46738a3da98147d0f55` | twelve variable, upgrade, timezone, hint, and timestamp tests |

The comparison package has sixteen functions: TestMain, twelve top-level
tests, and three helper methods on `mockZapCore`. All twelve tests are
top-level functions. They cover KV variable
propagation and failpoint probing, dynamic sysvar removal, TiKV-backed GC
variables, upgrade/canonicalization paths, index-join compatibility,
instance-scope writes, TTL external-workload enablement, timezone conversion,
global-variable accessor and deployment-mode limits, prepared SQL hints,
timestamp validation, and checkpoint-lag limits.

## Rust ownership and explicit boundary

Rust has adjacent executable owners for selected variable behavior:
`tidb-session` covers session/global variable state, deployment-independent
scope handling, SQL hints, timezone, and timestamp validation; `tidb-vardef`
carries variable definitions, defaults, and focused validation tests. The
transaction/client crates expose request-variable propagation and typed
replica/read options.

The exact Go package still depends on the mock TiKV variable transport and
`probeSetVars` failpoint, persistent `mysql.tidb` upgrade/canonicalization,
TTL external-workload callback, deployment-mode global-variable policy,
prepared-statement hint activation, and checkpoint/Oracle integration. No
dependency-closed Rust package owns that combined lifecycle, and there is no
direct source carrier for these tests. No Rust-only behavior was found to
remove, and no safe missing behavior can be implemented in this test-only
package without duplicating variable registries, persistence, or
observability hooks. It is therefore recorded as an explicit SEED/boundary;
remaining parity belongs to coordinated session, vardef, domain, storage,
TTL, and observability owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed in this
batch, so `make bazel_prepare`, the Ready lint gate, and a new regression test
were not required. The comparison was made against exact Go master; the
working branch may carry separate, unstaged local changes to this package.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/vars -count=1)
# passed: pkg/session/test/vars (10.868s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so a Rust owner test was not applicable. Not verified here:
Bazel execution, full Go repository tests, live TiKV variable persistence,
TTL external-workload integration, or deployment-mode cluster policy.
Compatibility and performance risk are unchanged because this batch modified
documentation only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
