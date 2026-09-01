# `pkg/util/breakpoint` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).
This package is a failpoint-backed test/debug hook with no production data
path of its own.

## Complete inventory

Both Go-master artifacts were read in full. There are no package docs, tests,
fixtures, generated outputs, platform variants, benchmarks, fuzz targets, or
nested packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `351dbcf3d3186ff7809777683088be53014b4a79` | `c209045142f70546a5d70876e25e3e6a3f39b980ce81138563d3470ed117c794` | public library target with sessionctx/stringutil/failpoint dependencies inventoried |
| `breakpoint.go` | 34 | `60c0e1f82879995330ec051553b42c771375a5bf` | `77622871b5790dd5674c929ced3ba588c072a34f7d71fc60759e8c5dc089a133` | exported notify-key constant and failpoint `Inject` hook inventoried |

The sole production function is `Inject`: it enables a named failpoint,
retrieves the session's `breakPointNotifyFunc` value, and invokes the typed
notification callback when present. The key is a `stringutil.StringerStr`
wrapper so it can be stored in a session context. No source test or fixture
asserts the callback behavior; callers own the failpoint declarations.

## Rust ownership and integration decision

No Rust crate has a failpoint runtime or a session-context breakpoint hook.
Adding a callback registry or test-only fault-injection API would not be an
ordinary SQL execution path and would create Rust-only behavior. The package
is explicitly unclaimed; no source change is justified.

## Validation

Profile: **Ready** for this docs-only authority refresh. This is a complete
two-artifact inventory and explicit boundary audit with no code change, so
`make bazel_prepare` is not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/breakpoint -count=1
# ? github.com/pingcap/tidb/pkg/util/breakpoint [no test files]
```

The same package probe passed in an exact detached checkout of Go master at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9`. Rust workspace formatting, the
pinned repository lint on a clean committed worktree, and `git diff --check`
also pass for this audit batch.

## Risks and unverified behavior

- Correctness: the failpoint name and typed session callback are Go test hooks;
  no Rust replacement is claimed.
- Compatibility: failpoint injection is build/tooling behavior, not a stable
  SQL protocol surface; a future port would need the complete failpoint and
  session-context infrastructure.
- Performance: no runtime code changed.
- Not verified locally: Bazel analysis, failpoint instrumentation of callers,
  and callback behavior under each caller's test harness.
