# `pkg/dxf/framework/dxfmetric` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly three tracked artifacts and 296 lines. Every
production and Bazel file was read in full in a detached worktree at the
pinned Go commit before this receipt was written. There is no `doc.go`, test
file, fixture/testdata directory, generated source or generator input,
platform-specific variant, benchmark, fuzz target, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `68f621478c8af17f1e97c667714c1b8625a25d18` | `acf29a0f9603d066250829720eafa5c18ab588f6ea61d93aa610b817e8a30e4b` | public library target for the collector and metric definitions |
| `collector.go` | 177 | `6127d29c34aeb53cbdb98a1d4eb593061b112ea7` | `72a9e0cb6e5cbb4aa5be02aea3594ccdf739722fb58d9c1f05a66c9ea09afad2` | atomic task/subtask snapshots, Prometheus descriptors, status counts, and pending/running duration gauges |
| `metric.go` | 101 | `a433934d33523a92ce15791b13c5ae2cc4c1fdfe` | `06a5671ffacd729cf5bacbbfdfd4449ad25b621ce497152c686c0ea0153b4de9` | DXF metric vectors, event labels, initialization, and registration |

The production inventory contains the complete collector contract (`NewCollector`,
`UpdateInfo`, `Describe`, `Collect`, private task/subtask aggregation and
duration helpers) and the metric contract (`InitDistTaskMetrics`, `Register`),
including UUID labels for multiple test domains, atomic pointer snapshots,
task-type/state cardinality, task/exec/subtask labels, and time-since-create or
time-since-start gauges. There are no package-local tests; callers register the
collector and metric vectors from the DXF framework.

## Rust ownership and parity decision

Rust search found no dependency-closed owner for DXF task/subtask Prometheus
collectors, UUID-isolated test labels, atomic snapshot publication, status
cardinality, or pending/running duration gauges. The Rust `tidb-dxf` crate owns
task/resource/step data structures but has no equivalent Prometheus registry
integration or metric lifecycle. Existing Rust timer/worker counters measure a
different subsystem and cannot substitute for these labels or snapshots. No
Rust-only dxfmetric behavior or ignored test was found to remove, and no
standalone metric facade was added speculatively; this complete package remains
an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
Go-master package compile probe passed (there are no package-local tests):

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/dxfmetric -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric [no test files]
```

Ready repository gates for this receipt batch passed:
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

Residual risk is metric integration: collector callers must publish coherent
task/subtask snapshots and register the vectors exactly once, while duration
gauges intentionally depend on wall-clock time. No live Prometheus scrape or
multi-domain registry is exercised by the no-test package probe; the receipt
does not claim Rust metric parity.
