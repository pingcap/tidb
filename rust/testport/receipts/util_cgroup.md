# Complete `pkg/util/cgroup` package receipt

Status: Ready on the host target. Linux cgroup-file execution and the live
container test remain unrun because this desktop test environment does not
expose `/proc/self/cgroup`; the pinned source fixtures exercise those paths.

## Pinned inventory

Comparison source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

The complete package tree at that pin contains exactly these nine artifacts:

    pkg/util/cgroup/BUILD.bazel
    pkg/util/cgroup/cgroup.go
    pkg/util/cgroup/cgroup_cpu.go
    pkg/util/cgroup/cgroup_cpu_linux.go
    pkg/util/cgroup/cgroup_cpu_test.go
    pkg/util/cgroup/cgroup_cpu_unsupport.go
    pkg/util/cgroup/cgroup_memory.go
    pkg/util/cgroup/cgroup_memory_unsupport.go
    pkg/util/cgroup/cgroup_mock_test.go

There is no `doc.go`, generated source, benchmark, fixture directory, or
additional `testdata`. The two platform files are the unsupported-target
variants selected by Go build constraints; `cgroup_mock_test.go` contains the
source memory/CPU fixture matrix and its file-writing helper.

## Go-to-Rust mapping

| Go artifact / contract | Rust evidence | Decision |
| --- | --- | --- |
| `cgroup.go`: `CPUQuotaStatus`, `CPUUsage`, `Version` | `tidb_util::cgroup::{CpuQuotaStatus,CpuUsage,Version}` | Direct behavior; Rust field/variant names follow Rust conventions while preserving values (`Unknown=0`, `V1=1`, `V2=2`). |
| `cgroup.go`: controller matching, `/proc/self/cgroup` discovery, mountinfo parsing, v1/v2 file parsing | `cgroup.rs::{controller_matches,detect_control_path,detect_mounts,detect_mount_version,read_*}` | Direct behavior, including order-independent controllers, colon-containing cgroup paths, namespace-relative v1 mounts, v2 `max`, and per-value hybrid fallback. |
| `cgroup_cpu.go`: CPU quota/usage and `CPUShares` | `cgroup_cpu_at`, `CpuUsage::cpu_shares`, and the source CPU matrix in `cgroup.rs` tests | Direct behavior. The hybrid memory-usage fallback intentionally keeps Go's pinned choice of joining the v2 path to the v1 mount. |
| `cgroup_cpu_linux.go`: `GetCgroupCPU`, `CPUQuotaToGOMAXPROCS`, `GetCPUPeriodAndQuota`, `InContainer` | `get_cgroup_cpu`, `cpu_quota_to_gomaxprocs`, `get_cpu_period_and_quota`, `in_container` | Direct Linux behavior. The signed quota result preserves Go's non-Linux `-1` sentinel. |
| `cgroup_cpu_unsupport.go` | target-selected non-Linux functions in `cgroup.rs` plus `tests/cgroup_source.rs` | Direct fallback: logical CPU count, `(-1,-1)`, `(-1,Undefined)`, and `false`. |
| `cgroup_memory.go`: public memory reads and v1/v2/hybrid selection | `get_memory_limit`, `get_cgroup_memory_limit`, `get_memory_usage`, `get_memory_inactive_file_usage`, `memory_value`, `memory_usage_at` | Direct behavior, including v1 `memory.stat` keys, v2 `memory.max/current`, `max -> math.MaxInt64`, and source fallback order. |
| `cgroup_memory_unsupport.go` and its private test helpers | target-selected public fallbacks plus test-enabled shared parser helpers | Public zero/`Unknown` behavior is direct; private helpers remain available to the source-shaped test carrier on unsupported targets. |
| `cgroup_cpu_test.go::TestGetCgroupCPU` | `source_live_cpu_test_keeps_the_container_contract` | Source test translated with ten yield workers and the container guard. It is a no-op on this host because `/proc` is unavailable. |
| `cgroup_mock_test.go::{TestCgroupsGetMemoryUsage,TestCgroupsGetMemoryInactiveFileUsage,TestCgroupsGetMemoryLimit,TestCgroupsGetCPU}` | Four source-named matrix tests in the owner module | All missing-file/controller/mount, v1, namespace/path, v2, malformed, max, and hybrid cases are represented by deterministic temporary fixtures. The large Go constants are replaced by equivalent builders; no behavior is omitted. |
| `BUILD.bazel` library/test target, Linux select, failpoint dependency | `tidb-util` Cargo target selection, `src/lib.rs` export, and `tests/cgroup_source.rs` | Build mapping. Go's `GetCgroupCPUErr` failpoint is test fault injection, not a production cgroup contract; no Rust-only failpoint surface was added. |
| `cgroup.go::SetGOMAXPROCS` | `cpu_quota_to_gomaxprocs` | The quota decision is direct. The Go process-global `runtime.GOMAXPROCS` mutation and logging/undo closure have no Rust scheduler equivalent, so they are an explicit integration boundary rather than an invented Rust API. |

## Rust-only behavior removed or relocated

The seed had `effective_memory_limit`, host RAM probes, process RSS probes,
`runtime_parallelism_recommendation`, and a `usize`-only `quota_parallelism`
API inside the cgroup owner. Host RAM/process RSS are memory authorities, not
cgroup package behavior; they now live in `tidb_util::memory::process` and the
three existing consumers call `tidb_util::memory::effective_memory_limit`.
The Rust-only recommendation wrappers were removed; the direct signed quota
conversion remains as `cpu_quota_to_gomaxprocs`.

The parser also now follows Go's raw controller-field count and starts the
mountinfo separator search at field 7, matching Go's optional-field scan even
when an earlier field happens to contain `-`.

## Integration boundary

Go's package is consumed by `pkg/util/memory`, `pkg/util/cpu`, and
`pkg/util/cgmon`. The Rust SQL tier has no cgroup CPU observer or monitor
owner yet, so those callers remain outside this package claim. The Rust memory
authority uses the cgroup limit through `tidb_util::memory::effective_memory_limit`,
and its process RSS fallback is owned by `memory::process`; this keeps the
cross-package integration explicit without claiming cgroup behavior for the
memory package.

## Validation

Profile: Ready for the continuing package loop. Commands run from the
repository root:

    git ls-tree -r -l e2788410d8d696605e8cb002585877a063ccc909 pkg/util/cgroup
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib cgroup
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib memory::process
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib memory
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --test cgroup_source
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked -p tidb-util -p tidb-session -p tidb-exec -p tidb-stats-handle-cache-internal-lfu --lib
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint

Observed on 2026-09-01:

* The pinned `git ls-tree` command listed exactly the nine artifacts above.
* `go test` passed for `pkg/util/cgroup` with failpoints enabled and disabled
  by `tools/check/failpoint-go-test.sh ... -count=1`.
* The Rust cgroup owner suite passed all 16 tests; the memory-process filter
  passed 3 tests; the complete `tidb-util --lib memory` filter passed 51 tests
  with 2 pre-existing ignored tests; and the public non-Linux carrier passed
  its 1 test.
* The affected-crate `cargo check` passed. Existing workspace warnings in
  `tikv-client`, planner, executor, session, and related crates were emitted;
  none were introduced by this batch.
* Nightly formatting, offline locked metadata, `git diff --check`, and
  `make lint` passed. The first no-argument failpoint-wrapper invocation hit
  the wrapper's empty-array `set -u` bug before any test ran; the successful
  `-count=1` invocation is the recorded validation command.

`make bazel_prepare` was not required: this batch adds no Go files, Go imports,
Go tests, Bazel files, or module dependencies.

## Risks and unverified targets

Linux cgroup file parsing is exercised through the complete source fixture
matrix but not against live `/proc` and `/sys/fs/cgroup` on this host. The
container-only ten-worker test therefore remains unrun. Windows and other
unsupported-target compilation/runtime are represented by target-selected
functions but not executed here. The Go failpoint hook is intentionally not
claimed because it injects a test error at a call site rather than changing
package behavior.
