# Retire detached PD experiments and redundant adapter checks


This living ExecPlan follows root PLANS.md. Baseline is TiDB 5826ee36e1bc;
native c594ea976154 remains unchanged. Fresh Go master ab37692e9ebef selects
PD afa43111d149. Work stays in the existing Cloud checkouts.

## Purpose and milestones


Remove stale alternative workflows and checks of constants that production never
uses. Generated clients own method paths; real socket tests observe dispatch.
Keep useful field-number checks, Go behavior, TLS, cancellation and close coverage.
The first milestone inventories callers and archives obsolete artifacts. The
second removes them as one batch and validates the remaining active owner.
The third publishes through normal gates and saves the reusable checkpoint.

## Progress


- [x] Inspect instructions, clean baselines, fresh master and all candidate callers.
- [x] Remove nine path constants/eight self-comparisons, unreferenced mock profiler,
  detached historical transport experiments and nine completed PD plans.
- [x] Preserve immutable archives, source inventory, original Go obligations and
  active socket/wire/TLS/cancellation coverage; update historical links.
- [x] Run retained PD tests, affected all-target check, lint and self-review.
- [ ] Actual precommit locked build, fresh locked pre-push build, remote verification
  and saved Cloud checkpoint; record results externally after execution.

## Surprises & Discoveries


The old runner explicitly rejects every native HEAD except 6163ecfc587b. Its
observations concern tonic 0.12.3; the workspace uses 0.14. The detached h2
accessor is intentionally rejected and its preface alternative was never integrated.
The loopback profiler describes an older per-command block_on worker design and
is not referenced by any maintained workflow. None of this accepts grpcutil.

## Decision Log


2026-10-08: archive the entire disconnected experiment workflow, retaining a short
historical index and its full source inventory. Preserve the original upstream
Go tests and all package obligations. Delete only path self-comparisons inside
two useful tests; their real wire and behavioral assertions remain.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh; use CARGO_BUILD_JOBS=1 and run Cargo from
/workspace/tidb/rust. Run cargo test --locked -p tidb-pd-client --lib --test all --
--test-threads=1, then cargo check --locked -p tidb-pd-client -p tidb-server
--all-targets. Do not compete with short-deadline socket tests by compiling in
parallel. Run make lint at repository root. No Go/Bazel inputs change. Verify
retired constants have no remaining callers and all archived document links use
the baseline commit. Record exact outcomes in pd-artifact-cleanup-validation.json.

The actual hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server.
Repeat that build immediately before the authorized normal push; verify remote SHA.
Never bypass hooks, reset concurrent work or force-push. Restore an artifact from
its immutable archive only after reassessing its current owner; never rerun the
external edit.py deletion script as startup. Preserve caches and generated files.

## Outcomes & Retrospective


Edits and validation are complete: 100 retained tests pass, 1 existing live-PD case is ignored; affected all-target checking, changed-file formatting and make lint pass. Publication gates remain and are recorded externally after execution. No finding or package closure and
no measured speedup claim. Removing the profiler eliminates one automatic example
target; detached experiment retirement removes misleading manual workflows only.

Revision 2026-10-08: completed grouped validation; all retired-file archives and pinned Go inventory hashes verified. Exact commands and outcomes are in parity/current-audit/pd-artifact-cleanup-validation.json.
