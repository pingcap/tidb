# `pkg/dxf/importinto/taskkey` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 57 lines. Both files
were read in full in a detached worktree at the pinned Go commit before this
receipt was written. There is no `doc.go`, `OWNERS`, test, fixture, benchmark,
generated source or generator input, platform-specific variant, or other
checked-in artifact.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `e45a080a516ad7f4d5f29dde7d982a86cad49a21` | `5aed95ebbefe272309df16fc90905379a34da6aa4f186658d58ee3e20b3073e3` | public library target and its kernel-type, DXF-proto, and keyspace dependencies |
| `task_key.go` | 44 | `2438cfd44991e791ead7c8cfce36454a552321f9` | `48b44e9b42a9fdd18bcea0c717f9fa7e0cf695c3eab3a9d52f7f73f465ae7ce8` | classic and next-generation ImportInto task-key constructors |

The production inventory contains all three functions: `ForJob`,
`ForJobInKeyspace`, and the private `forJobInKeyspace`. Their contract is
mode-dependent: classic keys are `ImportInto/<job-id>`, while next-generation
keys are `<keyspace>/<ImportInto>/<job-id>`; an explicit keyspace is ignored in
classic mode and the configured keyspace is used by `ForJob` in next-generation
mode. There are no tests to port or fixtures to regenerate.

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns the `ImportInto` task-type and step labels, and
`tidb-metadef` carries task-key columns in system-table schemas. Neither is a
dependency-closed owner of this package's kernel-mode dispatch, configured
keyspace lookup, or task-key construction call path. No Rust module currently
constructs or consumes these ImportInto task keys.

No Rust-only task-key behavior was found to remove. Adding a formatter without
the mode/configuration owner and DXF scheduler/storage consumers would create a
second unobservable key policy, so no speculative API or ignored test was
added. The complete Go package remains an explicit integration boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package check passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/taskkey -count=1
# ? github.com/pingcap/tidb/pkg/dxf/importinto/taskkey [no test files]
```

Repository formatting, lint, and diff hygiene are run for this receipt batch
(`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
--check`, `make lint`, and `git diff --check`). No Go source, import section,
test, Bazel target, or module dependency changed, so `make bazel_prepare` is
not required. Rust tests and a full workspace build are not run because no
Rust source or owning target changed. Mode-specific key shape, configured
keyspace propagation, scheduler/storage consumers, and collision behavior
remain unverified on the Rust side; this receipt records the boundary rather
than claiming transcreated parity.
