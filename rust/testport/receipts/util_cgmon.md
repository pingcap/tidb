# `pkg/util/cgmon` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This package is a
Linux-only process monitor with Go metrics and server lifecycle consumers; no
dependency-closed Rust owner exists.

## Complete inventory

All three artifacts were read in full before making the ownership decision:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 28 | `52162ff2072f92c8064474e157ee7b7b512b7bbd` | `d76f6aae837bd27dbad2a0c4e5b9230039513a1dbce528df07dbf3f8533d52b1` | library/test targets and gopsutil/metrics dependencies inventoried |
| `cgmon.go` | 160 | `aa2443effd7e6c4556ae166405130a067c23f69c` | `f2f214ddd524adeaa927020a8f3e7838f2c5621cba6985b148ab921cffc66ab1` | production monitor inventoried |
| `cgmon_test.go` | 41 | `0bb1119272ce86f00ea2fc967a0759b017b90d2f` | `2873ddcc75b37177955f51e8450996812d1365fa39431419e3fb040a8e37060b` | no-cgroup fallback regression inventoried and executed |

There is no `doc.go`, README, generated output, fixture, nested package,
benchmark, fuzz target, or additional platform source. `BUILD.bazel` has no
data glob; the implementation itself gates the monitor on `runtime.GOOS ==
"linux"`.

## Go behavior

`StartCgroupMonitor`/`StopCgroupMonitor` manage a process-global, non-thread-
safe lifecycle. The refresh goroutine runs immediately and every ten seconds,
recovers panics through `util.Recover`, and logs start/stop events. CPU refresh
starts at `runtime.NumCPU`, replaces it with the ceiling of a positive cgroup
quota/period ratio when that ratio is lower, and publishes changes to the
`metrics.MaxProcs` gauge. Memory refresh takes the smaller of gopsutil's
physical total and a positive cgroup memory limit, publishing changes to
`metrics.MemoryLimit`. Errors are returned for the test seam but do not stop
the loop; the default values remain in effect.

## Rust ownership and integration decision

`tidb-util::cgroup` already reads cgroup v1/v2 CPU and memory quotas, and
`tidb-util::memory::process` chooses an effective memory limit. Those are
supporting readers, not an owner for this package's ten-second scheduler,
process-global lifecycle, metrics publication, panic recovery, and server
startup/shutdown integration. Rust has no equivalent `MaxProcs`/`MemoryLimit`
metric wiring or `cgmon` consumer. Adding a detached timer would create
Rust-only scheduling and duplicate the existing cgroup authority, so no Rust
source change is justified.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix and no package-completion claim; `make bazel_prepare`, the Ready lint
gate, and a broad server integration run are not triggered.

Passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/cgmon -count=1
# ok
```

The existing Rust `tidb-util::cgroup` suite was already run in the adjacent
`pkg/util/cpu` audit (16 tests passed); it validates the supporting quota and
memory readers, not this monitor's lifecycle.

## Risks and unverified behavior

- Correctness: the Go fallback regression passes; no Rust monitor is claimed.
- Compatibility: Linux-only cgroup scheduling and metric updates remain
  unported; non-Linux builds intentionally no-op.
- Performance: no runtime code changed. A future owner must preserve the
  immediate refresh plus ten-second cadence and avoid duplicate cgroup reads.
- Not verified locally: Linux cgroup v1/v2 live deployment, server lifecycle
  integration, race/flaky Bazel execution, and a Rust metrics owner.
