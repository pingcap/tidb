# pkg/resourcemanager parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/resourcemanager` (rm.go, schedule.go, pool/
basepool+spool+workerpool, poolmanager/task_manager+iterator+scheduler,
scheduler/cpu_scheduler+scheduler, util/mock_gpool+shard_pool_map+util)
against `rust/crates/tidb-resourcemanager`.

## Result: no behavior-breaking divergences

Every surface matched, including: the singleton and its Start/Stop/
Register/Unregister/Reset lifecycle; the schedule chain (DistTask skip,
Hold-on-no-running, Downclock guard, `MinSchedulerInterval` 200 ms gate,
±1 tune with the overclock cap); spool's Tune ordering and blocking
admission with the 5 ms sleep and LIFO waiting counters; workerpool's
panic fallback text, first-error CAS context, Tune add/remove WaitGroup
behavior and Release ordering; the 8-shard pool manager with shard-ordered
iteration; CPU scheduler thresholds (<0.5 Overclock, >0.7 Downclock,
unsupported Hold); the prometheus metrics (`tidb_rm_pool_concurrency{type}`,
`tidb_rm_ema_cpu_usage`) reproduced on the default registry; and the full
`pkg/util/cpu` replacement surface in `tidb_util::cpu` (usage EMA observer,
GOMAXPROCS install) so the CPU scheduling dependency is covered, not
dropped.

## Polish documented at sites this batch

1. `workerpool.rs` `run_worker`: Go re-reads the task/result channel fields
   on every select; Rust clones them once at spawn. Re-wiring a running pool
   is invalid usage upstream (both are set before `Start`); now noted at the
   spawn site.
2. `poolmanager.rs` `iter`: Go seeds `compareTS` with the zero time (year 1)
   vs `UNIX_EPOCH` here; unreachable since `fn` runs only after a real
   `Meta` sets it. Noted.
3. `workerpool.rs` constructor: Go applies options before assigning
   `createWorker` (an option can observe nil); Rust stores it after. Noted.
4. `lib.rs` header now states the CPU scheduling surface is fully ported via
   `tidb_util::cpu`, so future audits do not re-investigate a gap that does
   not exist.

## Accepted narrowings (Rust-only, unobservable)

- `MaxOverclockCount` is a mutable atomic where Go has a compile-time
  constant (no mutator exists on either side).
- `start()` after `stop()` is a guarded no-op where Go re-spawns goroutines
  that exit immediately; double `stop()` asserts like Go's double-close
  panic. `exec` is public for the in-crate test where Go's is unexported.

## Validation

- `cargo test -p tidb-resourcemanager`, `cargo fmt -p tidb-resourcemanager`,
  `git diff --check`, `make lint`.
