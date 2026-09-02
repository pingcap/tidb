# `pkg/metrics` — Go-master parity receipt

Comparison source: Go `origin/master` at commit
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete package inventory

Before editing, all 60 tracked artifacts under `pkg/metrics` were enumerated:
33 root artifacts (31 Go production/test files, `BUILD.bazel`, and `OWNERS`),
the one alertmanager rule fixture, three `common` artifacts, ten Grafana
dashboard/readme/generator artifacts, and thirteen next-generation Grafana
dashboard/readme/generator artifacts. The inventory totals 5,577 Go lines
across 33 Go files, 171 production/test declarations, and 139,824 lines across
all source, test, build, rule, dashboard, and generated JSON artifacts. There
is no `doc.go`, platform-specific Go source, or additional generated Go input;
the dashboard JSON files are generated outputs paired with the checked-in
Jsonnet sources and were updated only to the fetched Go-master state.

The root package owns metric declarations and registration, resource-group,
RU-v2/v3, session, server, executor, DDL, storage, and statement-summary
collectors. `pkg/metrics/common` is a separately nested helper package and
the alertmanager/Grafana trees are fixture/build outputs in this package
boundary. No Rust crate provides a dependency-closed owner for the complete
Go `pkg/metrics` registry, so no speculative Rust facade was added.

## Go-master delta and implementation

The fetched master delta is the package-scoped set of eleven source/fixture
artifacts: RUV3 scalar, SQL-type, and engine counters with fixed label values;
RUV3 initialization and registration; removal of obsolete global-memory
arbitrator task labels; correction of the resource-control dashboard instance
selector; and the corresponding next-generation dashboard panels. The
focused `TestRUV3MetricDefinitions` regression asserts label vocabularies,
counter registration, and gathered metric-family labels. The existing package
test remains the integration check for collector initialization and global
registry behavior.

## Validation (Ready profile)

- Pre-fix command (focused regression before restoring the symbols):
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/metrics -run '^TestRUV3MetricDefinitions$' -count=1 -vet=off`
  — failed as expected with missing RUV3 symbols and label constants.
- Post-fix focused command with the same environment — passed (`1.217s`).
- Post-fix full package command:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test -tags=intest ./pkg/metrics -count=1 -vet=off`
  — passed (`0.746s`). The untagged full run was also attempted and hit the
  existing `intest.InTest` expectation in
  `TestSetupChannelzCollectorSkippedInTest`; the tagged Ready command is the
  package's intended test mode.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare`
  — required for the BUILD shard change and blocked locally because no
  `bazel` executable is installed.

## Boundaries and risks

The Rust workspace contains metric helpers and leaf collectors but no
dependency-closed owner for the shared Go registry, dashboard generation, and
cross-subsystem registration. The Go batch therefore remains the executable
parity surface. Correctness risk is limited to stable RUV3 metric names,
labels, and registration order; the focused gather test and tagged full suite
cover these paths. Dashboard JSON was synchronized to checked-in Jsonnet
inputs; generator execution was not repeated because it clones an external
Grafonnet repository and the fetched outputs are byte-for-byte the Go-master
artifacts. Performance impact is three Prometheus counters initialized once
and one fewer memory-arbitrator label family.
