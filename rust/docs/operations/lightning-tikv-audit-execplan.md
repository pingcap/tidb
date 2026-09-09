# `pkg/lightning/tikv` parity audit ExecPlan

## Objective

Inventory every Go-master Lightning TiKV source, test, SST fixture, and BUILD
input, compare the hparser branch with Go master, and record whether Rust has
a dependency-closed owner.

## Completed

- Read all eight pinned artifacts: three production files, two test files,
  BUILD metadata, and both binary SST fixtures.
- Counted 1,209 text lines, 31 production declarations, seven test functions,
  and no generated/platform variants or additional fixture trees.
- Verified the branch package is byte-identical to Go master; no behavior fix
  or regression test was needed.
- Confirmed related Rust TiKV/BR code does not own the Lightning SST writer,
  property collectors, and import/version helper group as one closure.

## Validation gate

- [x] Current-branch failpoint-aware package suite passes; refcount returns to
      zero.
- [x] Detached exact-Go-master package suite passes; refcount returns to zero.
- [x] Ready formatting, lint, and diff checks pass for the combined batch.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

A native port requires the Pebble-compatible SST writer/property format and
the concrete TiKV import-sst/PD clients plus Lightning consumers. Do not add a
partial utility facade until those dependencies are closed.
