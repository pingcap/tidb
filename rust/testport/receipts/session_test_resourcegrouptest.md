# `pkg/session/test/resourcegrouptest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: two tracked artifacts and 76 lines. Every test line
and BUILD declaration was read before comparing Rust. There is no production
source, `doc.go`, TestMain, fixture directory, generated output, benchmark,
fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `bfc41da9ad232a05d05ed9acb759d5ec34dd47c1` | `67b9cfede85769bff5bd2375e5b9e80c65133f423a8fc122487bade71c774238` | flaky resource-group test target |
| `resource_group_test.go` | 63 | `9f9752823e88a5e2fcd33412a2ef37594065fa9c` | `84b594d82781534862ab79c8133c8e7c4dda39051eb79cee9084ef5e866796d9` | statement-hint and transaction resource-group propagation test |

The single test creates two resource groups and exercises autocommit plus an
explicit transaction. The `TxnResourceGroupChecker` failpoint pins `default`,
`rg1`, and `rg2` at statement, pessimistic-lock, prewrite, and commit stages.
It also covers unknown-group fallback, case-insensitive known groups, and the
SELECT rejection path.

## Rust ownership and explicit boundary

Rust's `tidb-session` owns the persistent and statement-active resource-group
names, hint precedence, lowercasing, and statement-context handoff. Rust's
transaction/coordinator/storage owners carry a resolved resource group into
reads, locks, prewrite, and commit, with focused session and commit-protocol
tests. The workspace does not yet provide the Go resource-group catalog/cost
controller plus the `TxnResourceGroupChecker` observation hook that makes this
exact test executable; its source carrier therefore remains ignored.

No Rust-only behavior was found to remove, and no safe missing behavior can be
implemented in this test-only package. A standalone fake group catalog or
failpoint would create a second authority and could mask differences in
unknown-name fallback, privilege warnings, pessimistic-lock tagging, or final
commit tagging. This package is therefore recorded as an explicit
SEED/boundary; remaining work belongs to a coordinated resource-manager,
session, and transaction integration.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/resourcegrouptest -count=1)
# passed: pkg/session/test/resourcegrouptest (2.214s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. Not
verified here: Bazel execution, full Go repository tests, live PD resource
control, or Rust's future resource-manager/catalog integration. Compatibility
and performance risk are unchanged because this batch modified documentation
only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
