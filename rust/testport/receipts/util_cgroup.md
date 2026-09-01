# Complete `pkg/util/cgroup` package receipt

Status: Ready on the host target. The latest fetched Go master is unchanged
for this package. Linux cgroup-file execution and the live container test
remain unrun because this desktop test environment does not expose the Linux
cgroup files; the pinned source fixtures exercise those paths.

## Pinned inventory

Comparison source: Go commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

The complete package tree at that pin contains exactly these nine artifacts
and 2,668 lines:

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

| Artifact | Lines | Go blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 82 | `8ecdc456c991ac399276a4ed8f3f1289f2129980` | `18570df7422561ae0a9c32aef144238669ae64e1e6714021edf8309d124a48c9` | library, platform select, and flaky short test target |
| `cgroup.go` | 467 | `c499405c75284b9555f7532f3fc564944b92c596` | `485586f587dcae83355e82271e197070d62b213f1d2022ee6553e840a0c50c7e` | shared types, discovery, mount parsing, and file readers |
| `cgroup_cpu.go` | 141 | `08170bc89f6a0134b91ade285b4819dad10ad81a` | `15a50a64cd6d631fd3b4c5e38b6d5e9518b70d66c8d1f96f0f79bfa8a321ec97` | CPU v1/v2 selection, quota, usage, and `CPUShares` |
| `cgroup_cpu_linux.go` | 100 | `665b81f24e2e9f32a53c8b14b522b8e6593a529f` | `319eb7d613ecd3a99bba6f4eef996821b347f268bf082bf4f4dde3e9e3e1e486` | Linux public CPU/container APIs and failpoint hook |
| `cgroup_cpu_test.go` | 105 | `d32da890cfa47d67d4f313d14e7517b51008f876` | `fe7a07005a9f73f2f3ba7617608093a2c4ecfc3f31586711f3f7f456c1e08172` | Linux live ten-worker CPU test and kernel guard |
| `cgroup_cpu_unsupport.go` | 55 | `092875c09657bec2927dc195283507f7bb1a2064` | `3bb4f0dd10d469ca0f9b199d8cbb66bd06cbde68caa95671d96a6df3a916d51d` | non-Linux CPU/container sentinels |
| `cgroup_memory.go` | 236 | `4c298a4834d0d454f6e273b72b2caee628e5afdf` | `b4152b078b5126fe5aa3c63d81a4ef73af628107f0a6713e8ac42665ec96d831` | Linux memory limit, usage, inactive-file, and v1/v2 readers |
| `cgroup_memory_unsupport.go` | 225 | `d7cf38c6b9fb460b5df79afbcd3979c836aa30dd` | `561ca5ce8ada1d341fb34e4f1dd6cb7e3af42f79b26ecbf62f3846aec0e77af6` | non-Linux memory sentinels and private test helpers |
| `cgroup_mock_test.go` | 1,257 | `7d01073e5135f8ca9255087398cf69b78e06a345` | `f4bf28f806277c3a28492ee0ccbedd94ede5c2433129ac6e5f5534d08b30baed` | complete temporary-file CPU/memory fixture matrix |

## Go-to-Rust mapping

| Go artifact / contract | Rust evidence | Decision |
| --- | --- | --- |
| `cgroup.go`: `CPUQuotaStatus`, `CPUUsage`, `Version` | `tidb_util::cgroup::{CpuQuotaStatus,CpuUsage,Version}` | Direct behavior; Rust field/variant names follow Rust conventions while preserving values (`Unknown=0`, `V1=1`, `V2=2`). |
| `cgroup.go`: controller matching, `/proc/self/cgroup` discovery, mountinfo parsing, v1/v2 file parsing | `cgroup.rs::{controller_matches,detect_control_path,detect_mounts,detect_mount_version,read_*}` | Direct behavior, including order-independent controllers, raw duplicate-field count, colon-containing cgroup paths, namespace-relative v1 mounts, v2 `max`, and per-value hybrid fallback. |
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

    git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/cgroup
    git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/cgroup
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib cgroup
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib memory::process
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib memory
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --test cgroup_source
    OPENSSL_DIR=<bundled-poppler-root> DYLD_LIBRARY_PATH=<bundled-poppler-root>/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked -p tidb-util -p tidb-session -p tidb-exec -p tidb-stats-handle-cache-internal-lfu --lib
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint

Observed on 2026-09-02:

* The latest `git ls-tree` command listed exactly the nine artifacts above;
  the authority comparison against Go master is unchanged.
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
