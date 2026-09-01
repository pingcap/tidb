# `pkg/dxf/framework/dxfutil` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly three tracked artifacts and 314 lines. Every
production, test, and Bazel file was read in full in a detached worktree at
the pinned Go commit before this receipt was written. There is no `doc.go`,
`OWNERS`, fixture/testdata directory, generated source or generator input,
platform-specific variant, benchmark, or fuzz target.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 33 | `5565e60aae67f35680a9bd57c523de209f016340` | `f9cf4d703fbf34f045e3e8f38dc88f38d9978ef2a1b8dda5596502e6b69a7e7f` | public utility library and flaky two-shard test target |
| `util.go` | 92 | `5241dd5d9ef0810752ec22f8365b70440190e0a8` | `5f0b2907510bc6ae56d0b032b5e72c268442a8342c679156505b0824b9062de6` | cross-keyspace runtime acquisition/release, runtime keyspace validation, and holder-ID formatting |
| `util_test.go` | 189 | `edac3b23d68262b35b88731ab296ee2fbe4351af` | `8d3bb77255efb95e3ce723cfcac8613bf02f9a1eee938458978eab3818a9f572` | current/different-keyspace acquisition, handle release, acquire/session errors, runtime/store/session keyspace mismatch tests |

Production behavior is the complete three-function public utility contract:
`AcquireTaskRuntime` obtains the current server runtime or a cross-keyspace
handle and returns a mandatory release closure; `CheckTaskRuntime` validates
both the runtime store keyspace and a task-manager session-pool keyspace;
`GenHolderID` emits the stable `DXF/<component>/<task-id>` holder key.
`releaseTaskRuntime` is the private type-asserted handle release helper and
`sessionProvider` is the minimal testable session abstraction. Tests cover all
four acquire paths and all three validation outcomes; there are no package
fixtures or generated artifacts.

## Rust ownership and parity decision

Rust search found no owner for cross-keyspace SQL-server runtime acquisition,
`KSRuntimeHandle` release, TiDB session-pool keyspace validation, or the
`DXF/<component>/<task-id>` holder namespace. The Rust `tidb-dxf` crate models
task/resource/step data but has no SQL server runtime or keyspace session-pool
equivalent, and its test mocks implement unrelated traits. No Rust-only
behavior or ignored dxfutil test was found to remove. A standalone holder-ID
formatter or runtime facade would not preserve the Go ownership and release
semantics, so no speculative port was added; this complete support package
remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/dxfutil -count=1
# ok github.com/pingcap/tidb/pkg/dxf/framework/dxfutil 0.561s
```

Ready repository gates for this receipt batch passed:
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The residual risk is integration-only: callers must invoke the returned
release closure for cross-keyspace handles, and future Rust runtime work must
provide equivalent session-pool/keyspace ownership before this boundary can be
implemented. The Go test suite exercises the mismatch guards and release
callback with mocks; no live multi-keyspace server is exercised here.
