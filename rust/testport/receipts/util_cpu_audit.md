# `pkg/util/cpu` — rolling package audit (unclaimed)

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200`. The package is byte-identical
between that baseline and this worktree. Its latest Go source change is
`59dfa4d3b214ded26f957249efbda21f95149bb5`.

## Complete inventory

All four package artifacts were read in full before making an integration
decision:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `d0284eea2b4b491846f9843c72cceabc6f117abd` | `4f86ddfe8f1dec7be46e9ffc7ba6811f7bbd27f6fd4a442c47718d14f9d34baf` | library/test targets, race/flaky metadata, and dependencies inventoried |
| `cpu.go` | 132 | `6d14cbfada58d24f6d7f82f46f95912cdc0442e3` | `d0181ae4815fc029497e6b299674eaec1bdf3c9e0bda9795efa341eae3da4a3c` | production owner inventoried; not transcreated |
| `cpu_test.go` | 102 | `4458263e73aeef21d507967ff9c4e549411c9f25` | `9ebf08c012aac98af5a348d98961ab34bbb3b176052758693d7e8dcc054f3b55` | both source tests inventoried and executed through the failpoint workflow |
| `main_test.go` | 34 | `e3447fda1e47fa0d31e438427aaafd24d6e9dc73` | `66a32afde29399c8ecaf8ac63a00930e331579983d14c087280e03d0a258928d` | common-test setup and goroutine-leak exclusions inventoried |

There is no `doc.go`, README, benchmark, fixture, generated input/output,
example, fuzz target, platform/build-tag variant, or nested package artifact.
The package contains two process-global atomic values, one observer type, six
production functions/methods, two source tests, and one `TestMain` harness.

## Go behavior

`GetCPUUsage` snapshots the process-wide exponential-moving-average value and
the permanent unsupported flag. `NewCPUObserver` captures a monotonic wall
baseline and constructs the package's `mathutil.ExponentialMovingAverage`
with decay `0.95` and warm-up window `10`. `Start` probes cgroup CPU support
before spawning a 100 ms ticker. Each tick reads cumulative process user and
system milliseconds, divides their elapsed nanoseconds by elapsed wall time
and cgroup CPU shares, publishes the EMA to the process atomic and
`EMACPUUsageGauge`, and continues after process-time read errors. `Stop` closes
the exit channel and joins the worker. `GetCPUCount` returns the current
`runtime.GOMAXPROCS(0)`, with only the source failpoint override.

`TestCPUValue` is the container-only live observation test. It creates ten
busy goroutines and checks ten samples are in `(0, 1)`. `TestFailpointCPUValue`
injects the cgroup probe error, proves the observer stays stopped with a zero
value and the unsupported bit set, and proves the resource-manager CPU
scheduler holds its pool. `TestMain` supplies the repository test setup and
the five source goroutine-leak exclusions.

## Rust ownership and integration decision

No Rust package currently owns this Go package. `tidb-util::cgroup` is a
complete supporting owner for CPU quotas and accounting, but it deliberately
does not implement the process-time sampler, EMA publication loop, global
unsupported state, process CPU gauge, or resource-manager scheduler behavior.
`tidb-util::ppcpuusage` is statement-level TiDB/TiKV accounting and is not an
equivalent owner.

The production boundary is broad and live: the source observer is constructed
by `pkg/resourcemanager`, read by its CPU scheduler, and started and stopped by
the server/domain lifecycle. `GetCPUCount` is also consumed by server startup,
DXF scheduling, Lightning, importer, profiling, and Top SQL paths. The Rust
workspace has neither the ordinary resource-manager CPU scheduler nor a
shared `EMACPUUsageGauge` consumer to wire. Adding only a detached timer or a
public CPU-count helper would be a partial package port and would create
Rust-only behavior, so no source file or test carrier is added. The package
remains explicitly unclaimed until the observer, metrics, scheduler, and
startup consumers can land as one dependency-closed package claim.

## Validation

Profile: **WIP**. This is an inventory/integration-boundary audit with no code
fix and no package-completion claim. No Go/import/Bazel file changed, so
`make bazel_prepare` and the Ready lint gate are not triggered.

Passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/util/cpu -count=1
# PASS; the wrapper enabled and then disabled failpoints

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib cgroup -- --test-threads=1
# 16 passed; only the supporting cgroup/process-memory owner is selected
```

A diagnostic direct `go test ./pkg/util/cpu -count=1` failed
`TestFailpointCPUValue` because direct Go compilation does not rewrite the
package's `failpoint.Inject` calls. Following the repository failpoint runner
made the same focused test and the complete package pass; this is command
evidence, not a source regression.

## Risks and unverified behavior

- Correctness: the Go package passes its canonical failpoint-enabled test
  command, and the existing Rust cgroup support passes. No Rust observer is
  claimed.
- Compatibility: no public API or consumer changed.
- Performance: no runtime code changed. A future implementation must preserve
  the 100 ms interval, `0.95`/`10` EMA, cgroup-share normalization, and the
  source's process-global publication behavior.
- Not verified locally: the macOS host skipped the container-only live CPU
  range assertions; Linux process accounting, the race-enabled Bazel target,
  server/resource-manager integration, and a Rust package owner remain
  unverified and unclaimed.
