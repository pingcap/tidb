# `pkg/util/cpu` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete Go inventory

The package has exactly four Go-master artifacts and 308 lines, all read in
full: `BUILD.bazel`, `cpu.go`, `cpu_test.go`, and `main_test.go`. There is no
package `doc.go`, fixture, generated input/output, README, ownership file,
benchmark, fuzz target, example, nested package, or separate Go platform
file. The `gosigar` dependency supplies the source's Unix/Windows process-time
implementation inside one production file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `d0284eea2b4b491846f9843c72cceabc6f117abd` | `4f86ddfe8f1dec7be46e9ffc7ba6811f7bbd27f6fd4a442c47718d14f9d34baf` | library and two-shard race-enabled test target |
| `cpu.go` | 132 | `6d14cbfada58d24f6d7f82f46f95912cdc0442e3` | `d0181ae4815fc029497e6b299674eaec1bdf3c9e0bda9795efa341eae3da4a3c` | usage observer, process time, CPU count |
| `cpu_test.go` | 102 | `4458263e73aeef21d507967ff9c4e549411c9f25` | `9ebf08c012aac98af5a348d98961ab34bbb3b176052758693d7e8dcc054f3b55` | `TestCPUValue`, failpoint test |
| `main_test.go` | 34 | `e3447fda1e47fa0d31e438427aaafd24d6e9dc73` | `66a32afde29399c8ecaf8ac63a00930e331579983d14c087280e03d0a258928d` | common setup and goleak harness |

The production surface is `GetCPUUsage`, `NewCPUObserver`, `Observer.Start`,
`Observer.Stop`, the private `observe` and `getCPUTime`, and
`GetCPUCount`. `Start` performs cgroup preflight, samples cumulative process
user/system milliseconds every 100 ms, normalizes by elapsed wall time and
cgroup CPU shares, and publishes the 0.95-factor/10-sample exponential moving
average to `tidb_rm_ema_cpu_usage`. A preflight error sets the process-global
unsupported flag and starts no sampler. `GetCPUCount` returns the current
runtime parallelism and honors the `mockNumCpu` failpoint. `TestMain` only
installs common test state and goleak exclusions.

## Rust ownership and integration

`rust/crates/tidb-util/src/cpu.rs` owns the complete behavior and includes
explicit Unix, Windows, and unsupported-platform `get_cpu_time` variants. Its
threaded observer, process-global usage/unsupported state, cgroup preflight,
EMA arithmetic, gauge registration, failpoint hook, and CPU-count installation
are consumed by `tidb-resourcemanager` and `tidb-server` in the ordinary
startup/scheduling paths. The two Rust integration targets
`cpu_value.rs` and `cpu_failpoint_value.rs` retain the two Go test identities;
the latter is gated by the `failpoints` feature and mirrors the source
failpoint cleanup.

No Rust-only `must_use` diagnostics, cache-only path, or supplemental policy
was found. The Rust platform branches are implementation variants of Go's
cross-platform `gosigar` dependency, not additional behavior.

## Validation

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. The Go package uses failpoints, so both Go
runs used the repository failpoint wrapper, which enables and disables hooks
with cleanup.

```text
git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..origin/master \
  -- pkg/util/cpu
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/util/cpu \
  -run 'Test(CPUValue|FailpointCPUValue)$' -count=1
# passed (current worktree; failpoints enabled then disabled; two tests)

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/util/cpu \
  -run 'Test(CPUValue|FailpointCPUValue)$' -count=1
# passed (exact detached Go-master worktree; failpoints enabled then disabled)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-resourcemanager --test cpu_value --offline --locked -- --test-threads=1
# passed: one source test (non-container path is a clean skip where applicable)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-resourcemanager --test cpu_failpoint_value \
  --features failpoints,intest --offline --locked -- --test-threads=1
# passed: one failpoint source test

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-util -p tidb-resourcemanager -p tidb-server --offline --locked
# passed: owner, scheduler, and startup consumers (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

The local host is macOS arm64: the Linux cgroup/container sampling path and
Windows process-time implementation were reviewed from source but not
executed locally. Full workspace tests and Bazel execution remain outside
this leaf receipt.

## Risk

- Correctness: usage/unsupported state, EMA normalization, failpoint behavior,
  scheduler interaction, and CPU-count startup are covered by owner tests and
  consumer checks.
- Compatibility: cgroup preflight failure remains fail-closed; Unix/Windows
  process-time boundaries and runtime parallelism preserve source intent.
- Performance: one 100 ms observer thread and one bounded EMA update per tick;
  no additional synchronization beyond the existing observer state.
