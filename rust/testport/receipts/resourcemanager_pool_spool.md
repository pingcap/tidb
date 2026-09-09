# `pkg/resourcemanager/pool/spool` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `main_test.go`
- `option.go`
- `spool.go`
- `spool_test.go`

## Rust ownership and integration

- `tidb-resourcemanager::spool` owns the default blocking option and its
  override, construction/registration, exact `tidb_rm_pool_concurrency{type}`
  gauge (`How many concurrency in the pool`), capacity, running and waiting
  counts, stop state, task manager, and embedded base-pool behavior.
- Every admitted function receives a new native thread; threads are not reused.
  Panics are recovered and logged before the running count is decremented.
  Blocking submissions retry at the source five-millisecond interval, while
  nonblocking submissions return the canonical overload error.
- Grouped submission retains the requested concurrency in metadata while
  starting the smaller admitted count. Workers share the task and exit
  channels, increment/decrement task running state, drain buffered tasks after
  close, and stop on channel close or a downclock signal.
- Tuning ignores zero, records the current tune time, changes the exact metric,
  creates at most one grouped worker on each increase, and requests at most one
  grouped worker exit on each decrease. Release rejects new work, waits for all
  pending submitters and workers with wait-group-equivalent handshakes, then
  unregisters the pool from the process-global manager.
- All six source tests retain their identities and workloads: release during
  submission, scale up/down plus grouped execution, overload, two insufficient
  capacity cases, and task-manager tuning. Explicit release/wait handshakes
  provide the source `TestMain` leak-check boundary.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --quiet --offline -p tidb-resourcemanager --lib spool::tests
cargo test --quiet --offline -p tidb-resourcemanager --features failpoints,intest
cargo check --quiet --offline -p tidb-server
```

Results: formatting passed; all six spool tests passed; the full affected crate
passed eight library tests and both CPU integration tests; and the server
dependency checked successfully. Cargo emitted existing workspace warnings and
no warning from a changed file.

Not run in this WIP gate: race instrumentation equivalent to Go's Bazel
`race = "on"`, repeated flaky-test stress, workspace-wide tests, or the
Ready-profile `make lint` gate.
