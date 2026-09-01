# Audit of `pkg/util/breakpoint`

Status: complete atomic inventory; package not claimed implemented.

## Pinned inventory

Behavioral source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/breakpoint/breakpoint.go` | 1,216 | `60c0e1f82879995330ec051553b42c771375a5bf` |
| `pkg/util/breakpoint/BUILD.bazel` | 363 | `351dbcf3d3186ff7809777683088be53014b4a79` |

There is no `doc.go`, test, support file, fixture, benchmark, generated source,
or platform variant.

## Whole-package behavior

The package exports one typed session-context key whose string value is
`breakPointNotifyFunc`, and one `Inject` function. `Inject` evaluates the
named process failpoint; only when enabled does it load the session value,
type-assert `func(string)`, and synchronously invoke it with the failpoint name.
Missing values and values of other types are no-ops.

The two production consumers are both in pinned `pkg/executor/adapter.go`:

* `BreakPointBeforeExecutorFirstRun` immediately before an executor's first
  `Next` call;
* `BreakPointOnStmtRetryAfterLockError` after a lock-error retry rebuild.

Pinned `pkg/testkit/stepped.go` installs and clears the blocking callback.

## Rust comparison and decision

The workspace has the `fail` runtime in selected crates, but the ordinary Rust
session does not implement the shared heterogeneous `ValueStoreContext`, and
neither executor point invokes a breakpoint owner. No existing Rust production
module, test, or public symbol claims this package.

Adding a callback registry or injectable closure in isolation would move Go's
session/failpoint behavior to callers and create a second execution path. The
package therefore remains unclaimed until its session-value dependency and
both ordinary executor injection points can land together. There is no false
Rust carrier to delete in this audit.

## Validation

Read-only inventory/search commands:

    git ls-tree -r --long e2788410d8d696605e8cb002585877a063ccc909 pkg/util/breakpoint
    git grep -n 'breakpoint\.' e2788410d8d696605e8cb002585877a063ccc909 -- '*.go'
    rg -n 'breakPointNotifyFunc|BreakPointBeforeExecutorFirstRun|BreakPointOnStmtRetryAfterLockError' rust/crates

No package test exists upstream and no Rust code changed for this package. The
Bazel preparation gate is not required.
