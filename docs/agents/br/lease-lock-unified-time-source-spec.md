# Lease Lock Unified Time Source Spec

## Status

Draft for the next narrowed implementation phase.

This spec narrows the near-term work from the full delayed renewal-write
protocol to one smaller feature: make lease-lock time decisions use an
explicit lease clock, and make BR correctness paths pass PD-backed time down
from the upper layer.

The broader delayed renewal-write design remains recorded in
`docs/agents/br/lease-lock-renewal-delayed-write-analysis.md`. This spec does
not try to finish that whole protocol.

## Context

The lease-based object-store lock currently records `LockedAt` and `ExpireAt`
in lock metadata and uses time when acquiring, renewing, and cleaning up stale
locks. All current lock time decisions are simple "get current time" call sites:
construct metadata, generate the instance path time prefix, check renewal
expiration, and decide stale cleanup eligibility.

The minimal abstraction needed for this phase is therefore only a small lease
clock interface:

```go
type LeaseClock interface {
    Now(ctx context.Context) (time.Time, error)
}
```

Use an interface instead of a function so callers pass a named lease-clock
object rather than an anonymous callback. Keep this exported interface stable
as a one-method interface; any future lease-clock capability should use a new
interface, optional extension interface, or separate parameter instead of
adding methods here. The important constraint is that `pkg/objstore` only calls
it to obtain time; it does not own PD connectivity and does not hide a fallback
local clock behind BR correctness paths.

For PD-backed BR paths, the clock returns a trusted PD-allocated physical
timestamp. Callers must not assume it is the local receipt-time instant of the
RPC. Any later proof that depends on remaining lease after a potentially slow
operation must either bound the clock call or account for elapsed time
conservatively.

BR paths need a shared lease clock so different hosts do not make lease
decisions from different local clocks. The intended shared lease clock is PD TSO
converted to physical time.

## Problem

The lock layer still obtains lease time from local wall-clock helpers. As long
as acquire, renewal, and cleanup use local time, two BR processes on different
hosts can make different lease decisions from clock skew.

The immediate problem is therefore not the entire delayed renewal-write race. It
is the smaller clock-consistency problem:

- acquire should not create or return a lease based on local time in BR
  correctness paths;
- renewal should refresh and validate leases using the same lease clock as
  acquire;
- cleanup should decide whether a lock is stale using the same lease clock as
  acquire and renewal;
- BR upper layers that already have a PD client, or can create one from PD/TLS
  config, should pass a PD-backed lease clock into the lock calls.

## Goals

- Keep `pkg/objstore` independent of PD packages.
- Use an explicit lease clock for lease timestamps and lease-expiration
  decisions in the object-store lock.
- Add a small BR-side helper based on `pd.Client.GetTS` and
  `oracle.GetTimeFromTS`.
- Pass the PD-backed lease clock from BR upper layers into BR lock paths that
  participate in lease-lock correctness.
- Fail closed when the PD-backed lease clock cannot return trusted time.
- Keep each implementation step independently testable.

## Non-Goals

- Do not implement cleanup tombstones in this phase.
- Do not implement the local renewal freshness watchdog in this phase.
- Do not claim the full delayed renewal-write race is closed by this phase.
- Do not add a PD-time cache. Direct PD time is the first implementation.
- Do not move PD dependencies into `pkg/objstore`.
- Do not change acquire conflict rules or teach acquire to ignore tombstoned
  lock files.
- Do not introduce storage strong-consistency gating in this phase. That belongs
  to the later tombstone/delayed-write proof work.
- Do not introduce a new lock manager/client object in this phase. The lease
  clock is a narrow dependency passed through existing lock call paths.
- Do not migrate the manual `br operator migrate-to` command in this phase.
  It remains on the existing storage-only lock behavior because the command is
  a manual operator tool and its current config does not include PD/TLS inputs.

## Design

### Object-Store Lease Clock Usage

`pkg/objstore` owns the lock protocol and only receives an `objstore.LeaseClock`
that returns the current lease time. Internal helpers should prefer passing the
already-read `time.Time` value when that is enough.

Acquire:

- call `clock.Now(ctx)` before constructing lock metadata;
- use that time for `LockedAt`;
- use `LockedAt.Add(LeaseTTL)` for `ExpireAt`;
- use the same timestamp as the time prefix input for the 32-hex generation;
- after the write and family verification succeed, call `clock.Now(ctx)`
  again and prove the lease is still valid before returning `RemoteLock`;
- if the post-acquire proof fails, best-effort delete the physical lock path and
  return an acquire error.

Renewal:

- read the current lock metadata from the holder's physical path;
- call `clock.Now(ctx)` before the refresh write;
- use that value for the old-lease expiration check;
- write refreshed metadata with `ExpireAt = nowBeforeWrite.Add(LeaseTTL)`;
- keep local retry timers as local monotonic scheduling controls, not as lease
  validity proofs.

Cleanup:

- use the lease clock when deciding whether a candidate's
  `ExpireAt.Add(LeaseTTL)` cleanup delay has passed;
- if the clock fails, report a candidate cleanup error and do not delete
  that candidate;
- keep existing stale cleanup behavior otherwise.

### PD Time Helper

BR should provide a small clock implementation outside `pkg/objstore`, for
example under `br/pkg/leaseclock` or another BR-local package:

```go
type PDClock struct {
    client tsoClient
}

func NewPDClock(client tsoClient) *PDClock

func (c *PDClock) Now(ctx context.Context) (time.Time, error)
```

`PDClock.Now` should call `client.GetTS(ctx)`, compose the returned physical
and logical parts with `oracle.ComposeTS`, then convert it with
`oracle.GetTimeFromTS`.

If `GetTS` fails, `Now` returns an error. It does not fall back to
`time.Now()`.

If a full `pd.Client` fake is too large for focused unit tests, the adapter may
depend internally on a small interface containing only `GetTS(ctx)`.

### BR Propagation

BR upper layers should construct the PD-backed lease clock once per operation
and pass it down to the lock call path:

```go
leaseClock := leaseclock.NewPDClock(pdClient)
```

The expected call paths are:

- `br/pkg/restore/log_client/client.go`, where `LogClient` already has
  `pdClient`;
- `br/pkg/restore/snap_client/pitr_collector.go`, where `PiTRCollDep` already
  has `PDCli` and `prepareMig` appends migrations through `MigrationExt`;
- `br/pkg/stream/stream_metas.go`, by letting `MigrationExt` carry a lease
  clock and forward it to lock acquisition, renewal, and cleanup helpers;
- `br/pkg/task/stream.go`, for truncate lock cleanup/acquire and migration
  extension use.

The manual `br operator migrate-to` path is intentionally excluded from this
phase. Its `MigrateToConfig` currently has storage settings but no PD/TLS
settings, and the command is used as an operator repair/debug tool. It should
continue to use the existing lock behavior until a separate operator-command
design decides whether PD connectivity is required there.

## Failure Semantics

When BR provides a PD-backed lease clock:

- acquire clock failure returns an acquire error and does not create a new
  lock;
- post-acquire clock failure returns an acquire error and does not expose a
  `RemoteLock`;
- renewal clock failure is a renewal attempt error and cannot be reported
  as a successful renewal;
- cleanup clock failure prevents reclaiming the affected candidate.

No BR correctness path with a PD-backed lease clock falls back to local
wall-clock time for lease decisions.

Legacy storage-only lock entry points may keep existing local-time behavior
until their callers are migrated. They are not the correctness path for this
phase.

## Implementation Slices

1. Add the minimal object-store lease clock interface and internal helper variants.
2. Convert acquire to use an explicit lease clock and add post-acquire proof.
3. Convert renewal lease timestamp and expiration checks to the explicit time
   clock.
4. Convert stale cleanup time decisions to the explicit lease clock.
5. Add the BR PD time helper with focused tests.
6. Let `MigrationExt` carry a lease clock and inject PD time from
   `LogClient` and PiTR collector paths.
7. Inject PD time into stream truncate paths.

Each slice should update tests and the implementation notes for only the scope
it completes.

## Acceptance Criteria

- `pkg/objstore` acquire, renewal, and stale cleanup lease decisions can use an
  explicit lease clock supplied by the caller.
- BR correctness paths inject a PD-backed lease clock from their upper layer.
- PD time retrieval errors are surfaced as errors and never silently replaced
  with local wall-clock time.
- Focused tests cover acquire, renewal, cleanup, the PD time helper, and BR
  propagation.
- `make bazel_prepare` is run for any slice that adds Go files or new top-level
  Go test functions.

## Relationship to Delayed Renewal-Write Fix

This phase is a prerequisite for the full delayed renewal-write fix, but it is
not sufficient by itself.

After this phase, BR lease-lock paths can make time decisions from PD instead
of local clocks. This phase removes cross-host clock skew only. It does not
prove that a delayed storage write did not land after cleanup, does not make
acquire or renewal linearizable with object storage, and does not replace
terminal renewal state, local freshness watchdogs, strong-consistency tombstone
checks, or cleanup tombstones. The remaining delayed-write protocol work should
stay in the broader delayed-write design and be implemented only after this
smaller lease-clock phase is reviewed.
