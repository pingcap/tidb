# `pkg/util/cgroup` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `cgroup.go`
- `cgroup_cpu.go`
- `cgroup_cpu_linux.go`
- `cgroup_cpu_unsupport.go`
- `cgroup_memory.go`
- `cgroup_memory_unsupport.go`
- `cgroup_cpu_test.go`
- `cgroup_mock_test.go`

## Rust ownership and integration

- `tidb-util::cgroup` owns cgroup v1, v2, and hybrid discovery; CPU usage,
  quota, and period reads; quota-to-worker conversion; memory limit, usage,
  and inactive-file reads; platform fallbacks; and Linux container detection.
- Controller matching retains Go's raw comma-field count and set membership,
  including reordered and duplicated controllers. Mountinfo parsing starts at
  field seven, after the six fixed fields, so an earlier optional field named
  `-` is not mistaken for the filesystem separator.
- Hybrid fallback order and path construction match the pinned source,
  including its observable v1-mount path for the `memory.current` fallback.
- Go's `runtime.GOMAXPROCS` mutation has no Rust process-runtime operation to
  invoke: this server uses native OS threads rather than one mutable Go
  scheduler. The source quota conversion is retained as
  `cpu_quota_to_gomaxprocs`; no Rust-only scheduler recommendation or
  connection-concurrency workaround remains.
- Process RSS belongs to `tidb-util::memory`, not the Go cgroup package. Its
  existing memory-controller consumer now calls that private owner directly;
  the cgroup package no longer exposes process-memory behavior.
- The four portable source tests and Linux-only live CPU test are represented
  with the source test identities. Their helpers cover v1/v2/hybrid values,
  namespaces and controller ordering, malformed and missing control files,
  the pinned hybrid memory path, and container detection.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --quiet --offline -p tidb-util --lib cgroup::tests
cargo check --quiet --offline -p tidb-util -p tidb-exec -p tidb-session -p tidb-stats-handle-cache-internal-lfu
```

Results: formatting completed, all cgroup tests passed on macOS, and all
affected crates checked successfully. Cargo emitted an existing
`tikv-client-rs` private-bound warning.

Not run in this WIP gate: Linux live-container execution, Windows or other
unsupported-target execution, workspace-wide tests, or `make lint`. Those are
reserved for the Ready profile before overall task completion or PR-readiness
is claimed.
