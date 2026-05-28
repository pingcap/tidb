# Lease Lock Renewal Delayed Write Analysis

## Context

This note records the current design discussion for a narrow renewal race in the
lease-based object-store lock.

The accepted instance-generation design fixes the fixed-path stale cleanup race
by making each acquire write a unique physical instance path, for example:

```text
v1/LOCK.WRIT.<generation>
v1/LOCK.READ.<generation>
v1/APPEND_LOCK.WRIT.<generation>
truncating.lock.<generation>
```

Renewal deliberately does not migrate generation. It refreshes `ExpireAt` in
place on the `RemoteLock.path` acquired by the holder.

The remaining question is what happens if a holder starts renewal while the old
lease is still valid, but the final `WriteFile` is delayed long enough that
cleanup can reclaim the old instance and another holder can acquire a different
generation before the delayed write lands.

## Current Race Shape

Example with a migration write lock:

```text
Alice owns v1/LOCK.WRIT.genA with ExpireAt = E.

Alice starts tryRenew before E:
  1. read genA
  2. verify TxnID
  3. verify now < E
  4. prepare refreshed metadata
  5. final WriteFile(genA, refreshed metadata) is delayed

After E + LeaseTTL:
  Bob observes genA as stale enough.
  Bob cleanup deletes genA.
  Bob acquires v1/LOCK.WRIT.genB.

Then Alice's delayed WriteFile(genA, refreshed metadata) lands.
```

At that point the family may contain two write instances, `genA` and `genB`.
The write to `genA` rewrites the full `LockMeta`, not only the `ExpireAt` field,
so reading `genA` after the write cannot prove whether the object was deleted
and recreated in the middle.

The existing `ExpireAt + LeaseTTL` cleanup delay greatly reduces the practical
window. The delayed final write must remain pending past the cleanup grace
period and then still successfully land. However, this delay is not ruled out by
the current `storeapi.Storage` contract, so it is not a strict proof.

The correctness argument in this note assumes the lock is used on a storage
backend that provides strong read-after-write/list/existence visibility for
lock objects and cleanup tombstones. In current terms, this means the lock
protocol proof applies to `storeapi.StrongConsistency` storage. Without that
visibility, a successful tombstone write might not be observed by a later
`FileExists` or list, so the protocol should fail closed rather than claim the
same proof.

## Agreed Constraints

- Do not extend the acquire `conditionalPut` protocol casually. The family
  acquire protocol is intentionally scoped to acquire-time intent handling.
- Do not introduce renewal intent as the current fix. It expands the protocol
  surface, creates new orphan-intent states, and conflicts with the earlier
  decision that renewal is not part of the family acquire protocol.
- Do not migrate generation during renewal. Generation is created at acquire
  time and remains stable for the lifetime of the `RemoteLock`.
- Prefer false failure over false success. If renewal cannot prove it is still
  inside its previously valid lease window, it should report lease lost rather
  than continue business work.

## Agreed Direction

### Unified Lease Time

Lease timestamps should use one shared time source instead of each process's
local clock for BR correctness paths. For BR callers, the intended source is PD
time: BR obtains a unified timestamp from PD and passes a clock provider into
the lease-lock layer. `pkg/objstore` avoids a direct PD dependency. The boundary
is a small clock provider interface that tests can fake and BR can back with PD
time.

This does not by itself solve the delayed write race, but it removes
cross-host clock skew from `ExpireAt`, renewal, and cleanup decisions.

The lease clock is allowed to fail. If PD time is unavailable, the lock layer
must not fall back to local wall-clock time for lease decisions:

- acquire cannot create a new lease lock because it cannot fill trusted
  `LockedAt` and `ExpireAt` values;
- renewal cannot report success, but treats the failed time read as a transient
  renewal error handled by the existing retry budget;
- cleanup cannot decide that an instance is stale enough, so it records a
  candidate cleanup error and does not reclaim that instance.

The error classification should be consistent across paths:

- acquire treats `LeaseClock.Now` failure as acquire failure and must not create
  a new lock;
- renewal treats `LeaseClock.Now` failure as a transient renewal error for that
  attempt; the attempt cannot report success, and retry exhaustion still reports
  lease lost;
- cleanup treats `LeaseClock.Now` failure as a candidate cleanup error and must
  not write a tombstone or delete the candidate;
- the local watchdog does not use PD time and continues to run on its local
  monotonic timer; PD errors cannot extend or reset the watchdog.

Local monotonic timers still drive scheduling: `renewInterval`,
`renewBaseBackoff`, and `renewMaxRetries` remain local retry controls. They do
not decide whether a lease is expired. If PD time remains unavailable until the
renewal retry budget is exhausted, the renewal loop reports lease lost because
it can no longer prove the holder is safe.

Acquire should use one PD-backed timestamp for the new generation's time prefix
and the new `LockMeta`:

```text
leaseNow = leaseClock.Now()
generation = fmt.Sprintf("%016x%016x", leaseNow.UnixNano(), random64)
LockedAt = leaseNow
ExpireAt = leaseNow + LeaseTTL
```

The generation remains exactly 32 hexadecimal characters: a fixed-width
16-hex-character PD-time prefix followed by a fixed-width 16-hex-character
random suffix. The timestamp component is still only for physical-path
uniqueness, observability, and rough ordering. It must not be used as a
lease-validity proof, a cleanup watermark, or a tombstone range.

The lock layer should separate the clock interface from the clock
implementation. `pkg/objstore` only needs a small interface, for example:

```go
type LeaseClock interface {
    Now(ctx context.Context) (time.Time, error)
}
```

`LeaseClock.Now` means "return trusted PD-backed lease time." Whether the clock
implementation obtains that time by direct PD RPC or by a proven-fresh cache is
outside the lock layer. However, the first implementation should stay simple and
use direct PD time rather than introducing a PD-time cache. A future cache
optimization would need its own freshness proof. If the clock implementation
cannot prove the returned time is trusted, it must return an error instead of
falling back to local `time.Now()`.

For compatibility, `pkg/objstore` may keep a local-clock default for tests or
non-BR callers that do not provide a `LeaseClock`. That default preserves legacy
behavior but does not provide the cross-process unified-time guarantee described
in this note. BR lease-lock call paths must explicitly inject the PD-backed
clock. The correctness claims for delayed renewal writes apply to that
PD-backed path, not to a local wall-clock fallback.

### Acquire Return-Time Proof

Acquire has the same "write may return late" shape as renewal, even though
business work has not started yet. The lifecycle acquire path must not expose an
active lock to business code if the acquire write and verification consumed the
whole lease window.

After acquire writes the lock metadata and finishes acquire-time verification,
but before returning a lock whose renewal ownership can start, it should obtain
PD time again and prove the acquired lease is still valid:

```text
leaseNow = leaseClock.Now()
LockedAt = leaseNow
ExpireAt = leaseNow + LeaseTTL
write/acquire/verify lock

nowAfterAcquire = leaseClock.Now()
if existingRenewExpiredPredicate(nowAfterAcquire, ExpireAt):
    acquire fails; do not expose an active lock

remainingLease = ExpireAt - nowAfterAcquire
firstRenewAt = LockedAt + renewInterval
firstDelay = max(0, firstRenewAt - nowAfterAcquire)
```

This proof is synchronous with acquire. Starting renewal later cannot be the
first place that discovers the acquired lock was already expired, because the
business caller could otherwise begin protected work with no remaining proven
lease window.

### Conservative Renewal Success Rule

The holder should treat the previous lease window as the only trusted window.
During renewal:

```text
oldExpireAt = metadata.ExpireAt
nowBeforeWrite = leaseClock.Now()
if existingRenewExpiredPredicate(nowBeforeWrite, oldExpireAt):
    lease lost

newExpireAt = nowBeforeWrite + LeaseTTL
write refreshed metadata

nowAfterWrite = leaseClock.Now()
if existingRenewExpiredPredicate(nowAfterWrite, oldExpireAt):
    lease lost

if own cleanup tombstone exists:
    lease lost

remainingLease = newExpireAt - nowAfterWrite
```

If the PD time observed after the write satisfies the existing renewal expired
predicate against `oldExpireAt`, the holder must treat the lease as lost even if
`WriteFile` returned success.

The renewal expiration predicate should keep the existing boundary condition.
The current behavior is strict `now.After(meta.ExpireAt)` when `ExpireAt` is
set; this design does not change it to `>=`.

The refreshed `ExpireAt` is based on `nowBeforeWrite + LeaseTTL`, not
`nowAfterWrite + LeaseTTL`. The post-write PD time is only a proof that the
write returned while the previous lease window was still valid; it should not
extend the new lease window after a slow storage write.

Because the new `ExpireAt` is anchored at `nowBeforeWrite`, a slow but still
valid renewal may return with much less than a full `LeaseTTL` remaining. After
a proven successful renewal, the next renewal schedule and the local watchdog
duration must be based on the remaining lease window observed at success time,
for example `newExpireAt.Sub(nowAfterWrite)`, not a fresh full `LeaseTTL`.

The next renewal delay should also subtract the time consumed by the successful
refresh operation. In other words, the normal next-renewal target is anchored at
the lease timestamp, not at the time the storage write returns:

```text
leaseAnchor = nowBeforeWrite
nextRenewAt = leaseAnchor + renewInterval
nextDelay = max(0, nextRenewAt - nowAfterWrite)
```

If the renewal write itself takes longer than the normal interval, for example
longer than `LeaseTTL / 3`, `nextDelay` becomes zero and the renewal loop should
start the next renewal attempt immediately.

This is stricter than checking `writeDuration < LeaseTTL`, because a renewal
could start very close to the old expiration time. The important condition is
whether the holder completed renewal while its previous lease was still valid.

This still does not eliminate every failure mode. If the delayed write lands
after the old lease expires and the process crashes before it can react, the
old generation can remain. But this rule prevents a live holder from accepting
such a renewal as successful.

Failures to read PD time or the holder's own tombstone state are transient
renewal errors. The current `tryRenew` attempt must not report success, but the
renewal loop may retry. If retry budget is exhausted, or a later attempt proves
the old lease window has closed, the holder reports lease lost.

Read failures for the holder's own lock object need one extra distinction. A
storage read error whose existence state is unknown remains transient, but a
confirmed missing object is a permanent lease-lost condition:

```text
ReadFile(lockPath) succeeds:
    continue TxnID, ExpireAt, refresh, tombstone checks

ReadFile(lockPath) fails:
    FileExists(lockPath) == false:
        lease lost immediately
    FileExists(lockPath) == true:
        transient renewal error
    FileExists(lockPath) also fails:
        transient renewal error
```

The lock file is the lease carrier. Once the holder can confirm its own
physical lock instance no longer exists, it must stop business work even if no
cleanup tombstone is visible. A tombstone explains why the file disappeared, but
it is not required to conclude lease lost from confirmed absence.

### Local Renewal Freshness Watchdog

The remote proof above does not stop business code while a renewal storage
operation is blocked. If a holder starts `WriteFile` for renewal and the call
does not return for a long time, the renewal goroutine cannot rely on the write
return path to notify business code promptly.

Therefore the holder also needs a local monotonic freshness watchdog. The
implementation can model it as a timer over the remaining proven lease window:

```text
after successful acquire:
    start watchdog timer for the remaining acquired lease window

after each proven successful renewal:
    reset watchdog timer for the remaining renewed lease window

if the watchdog timer fires before the next proven successful renewal:
    report lease lost
```

The initial acquired window follows the same rule as renewal. Acquire writes
`LockedAt = leaseNow` and `ExpireAt = leaseNow + LeaseTTL`. When renewal
ownership starts, it should obtain PD time again and compute:

```text
remainingLease = ExpireAt - nowAfterAcquire
firstRenewAt = LockedAt + renewInterval
firstDelay = max(0, firstRenewAt - nowAfterAcquire)
```

If acquire and verification consumed more than the normal renewal interval, the
first renewal attempt should start immediately instead of sleeping a full
`renewInterval` from the time `startRenewal` begins.

Do not introduce an extra minimum-remaining-time threshold in the first design.
If the lock has not expired under the existing predicate but only a tiny
remaining window is left, renewal ownership may still start, but the initial
renewal delay should be zero and the watchdog should use that tiny remaining
window. If renewal cannot refresh in time, the watchdog will report lease lost.

This watchdog does not prove remote lease expiration. It is a local safety stop:
the process has consumed its last proven remaining lease window without
completing another proven successful renewal, so it must stop business work
instead of waiting indefinitely for a blocked storage call.

The renewal loop's next sleep should also be computed from the remaining lease
window after a successful acquire or renewal. A fixed sleep such as
`renewInterval` after the write returns can be too late if the successful
operation consumed much of the lease window. The scheduler should target
`leaseAnchor + renewInterval`; if that time has already passed, it should renew
again immediately.

The local watchdog and the PD-backed remote proof are conjunctive requirements,
not fallback paths:

```text
holder may continue only if:
    local freshness watchdog timer has not fired
    and PD-backed renewal proof succeeds when renewal runs
    and own cleanup tombstone does not exist
    and own physical lock object is not confirmed missing
```

If either side reports lease lost, the `RemoteLock` is terminally lost in this
process. A later PD read, tombstone check, or delayed renewal write return must
not revive it. Only a complete new acquire can create a new active lock holder.

There is no success-over-failure priority between watchdog and renewal I/O.
The lock remains active only while all required proofs continue to hold. Any
failure source that reaches the shared state first makes the lock terminally
lost: watchdog timeout, confirmed missing physical instance, expired previous
lease window, own tombstone, or retry exhaustion. A later successful renewal
return cannot cancel or overwrite an already-observed failure.

Implementation should make this explicit with a shared renewal state, such as
`active`, `lost`, and `stopped`. Transitions from `active` to `lost` call
`onLeaseLost` exactly once. Normal `stopRenewal` or `Unlock` transitions to
`stopped` without calling `onLeaseLost`. Once the state is `lost` or `stopped`,
delayed renewal returns must not reset the watchdog or report renewal success.

Normal stop owns watchdog shutdown. When `stopRenewal` or `Unlock` begins, it
should attempt to transition `active -> stopped`; on success it stops the
watchdog, cancels any in-flight renewal attempt, and waits for the renewal loop
to exit before continuing normal unlock/delete work. If the watchdog or another
failure source has already transitioned `active -> lost`, normal stop must not
rewrite that state to `stopped`; it should only perform resource cleanup and any
best-effort unlock/delete behavior that remains appropriate.

If a lock is already `lost`, a later `Unlock` may still attempt the existing
best-effort physical delete as cleanup hygiene, but that delete result must not
change the terminal lost state. A successful delete does not restore
correctness, and a failed delete does not make the lost proof weaker. `Unlock`
should not write cleanup tombstones; tombstones remain owned by cleanup.

When the local watchdog fires, the implementation should also cancel the
in-flight renewal attempt context if one exists. This cancel is only resource
hygiene: it may unblock a stuck `ReadFile`, `WriteFile`, `FileExists`, or list
operation sooner, but correctness must not depend on the storage backend
honoring the context. The terminal lease-lost state is the proof boundary. If a
delayed storage call later returns success, it must observe the terminal state
and must not refresh the local freshness window or report renewal success.

The watchdog is owned by the renewal loop, not by business callers:

- a lock that has been acquired but has not started renewal does not start the
  watchdog;
- `startRenewal` starts the watchdog timer together with the renewal loop;
- a proven successful renewal resets the watchdog timer;
- normal `stopRenewal` or `Unlock` stops the watchdog without calling
  `onLeaseLost`;
- a watchdog timeout enters the same terminal lease-lost path as other
  permanent renewal failures and calls `onLeaseLost` once.

### Family Checks During Renewal

Current renewal only validates the holder's own physical instance. It does not
scan the family to detect that another conflicting lock instance already exists.

The agreed first version should keep that boundary: renewal only proves the
holder's own instance is still valid. It does not perform family-level
arbitration, does not inspect acquire intents, and does not delete other
instances. Family conflict detection remains part of acquire-time verification
and cleanup, not renewal.

This avoids expanding renewal into the acquire `conditionalPut` protocol. It
also keeps the delayed-write fix focused on the facts renewal actually needs:
the previous lease window, the local freshness watchdog, the holder's own
physical object, and the holder's own cleanup tombstone.

Family sanity checks may still be considered later as defense-in-depth, but
they are not part of the first correctness plan.

### Cleanup Tombstone

The agreed cleanup direction is to make cleanup deletion durable in two phases:

```text
1. confirm the lock instance is stale enough to reclaim
2. write the cleanup tombstone
3. delete the physical lock instance
```

The tombstone is a per-instance singleton empty file. Its existence, not its
content, commits the cleanup decision. Once the tombstone write succeeds, the
corresponding physical lock instance must never again be treated as a valid
lock, even if the object still exists or a delayed renewal write recreates it.

Use a dedicated namespace that cannot be matched by existing lock-family scans:

```text
v1/LOCK_CLEANUP_TOMBSTONE/LOCK/WRIT/<generation>
v1/LOCK_CLEANUP_TOMBSTONE/LOCK/READ/<generation>
v1/LOCK_CLEANUP_TOMBSTONE/APPEND_LOCK/WRIT/<generation>
v1/LOCK_CLEANUP_TOMBSTONE/TRUNCATE/WRIT/<generation>
```

The mapping from physical lock instance to tombstone path is:

```text
truncating.lock.<generation>
  -> v1/LOCK_CLEANUP_TOMBSTONE/TRUNCATE/WRIT/<generation>

v1/LOCK.WRIT.<generation>
  -> v1/LOCK_CLEANUP_TOMBSTONE/LOCK/WRIT/<generation>

v1/LOCK.READ.<generation>
  -> v1/LOCK_CLEANUP_TOMBSTONE/LOCK/READ/<generation>

v1/APPEND_LOCK.WRIT.<generation>
  -> v1/LOCK_CLEANUP_TOMBSTONE/APPEND_LOCK/WRIT/<generation>
```

Tombstones only apply to current generation-based lock instance paths. Legacy
or malformed names that cannot be parsed as a current lock generation do not get
tombstones and are not automatically deleted by this new cleanup protocol. They
remain fail-closed: if such a file blocks acquire, the protocol should not
silently ignore or reclaim it as part of the delayed-renewal fix.

Cleanup should report such a legacy or malformed lock instance as a candidate
cleanup error, but it should continue processing other parseable generation
instances in the same family. The bad instance remains visible for diagnosis
and may still block acquire, while normal stale instances can still be reclaimed
through the tombstone protocol.

Concurrent cleanup attempts may write the same singleton tombstone path. This is
safe because the tombstone is empty and idempotent: one successful write is
enough to tombstone the instance, and multiple successful writes do not change
the meaning.

Tombstone writes do not require create-if-not-exists semantics. A normal
`WriteFile` of the empty tombstone object is enough, even if another cleanup
attempt already wrote the same tombstone. Overwriting the same empty singleton
tombstone does not change the cleanup decision, and avoiding conditional create
keeps tombstone handling out of the acquire `conditionalPut` protocol.

The agreed first version does not garbage collect tombstones. This is
intentional: tombstones are part of the correctness protocol, not only an audit
log. Removing them would require a separate proof about the maximum possible
delayed object-store write window, which the current storage abstraction does
not provide.

Tombstone checks are intentionally narrow:

- acquire family verification does not read tombstones; if a tombstoned zombie
  lock file still exists, acquire may fail conservatively and rely on retry
  cleanup to remove it;
- cleanup checks tombstones, and if an instance is already tombstoned it can
  delete that instance without re-reading metadata or re-checking staleness;
- renewal checks only the singleton tombstone path derived from its own
  `RemoteLock.path` after the refresh write returns, and treats that tombstone
  as lease lost.

This intentionally chooses fail-closed acquire behavior. A tombstone does not
allow acquire to ignore a visible lock instance. If tombstone write succeeds but
the following object delete fails, the tombstoned zombie lock can continue to
block new acquire attempts until cleanup later deletes it. That hurts
availability but avoids teaching acquire another tombstone-read path and avoids
false success when the tombstone view is unavailable or ambiguous.

The renewal tombstone check should be combined with the conservative previous
lease-window rule: a renewal is accepted only if the refresh write returns
before the old `ExpireAt` and the lock's own singleton tombstone does not
exist. Renewal should use an exact existence check rather than a tombstone
prefix scan:

```text
FileExists(tombstonePathFor(RemoteLock.path)) == true:
    lease lost
FileExists(tombstonePathFor(RemoteLock.path)) == false:
    continue
FileExists(tombstonePathFor(RemoteLock.path)) fails:
    transient renewal error
```

Cleanup may batch-list tombstones because it processes a family candidate set.
Renewal only cares about one physical instance, so an exact check keeps the
failure surface narrow.

Cleanup uses the tombstone as a write-ahead record for delete:

- if tombstone write fails, cleanup must not delete the lock instance; the
  candidate is reported as a cleanup error and an outer retry may try again;
- if tombstone write succeeds, the cleanup decision is committed;
- if delete succeeds after tombstone, cleanup succeeds;
- if delete reports that the object is already missing after tombstone, cleanup
  also succeeds because the target state is already reached;
- if delete returns another error after tombstone, cleanup reports an error, and
  a later cleanup may delete the tombstoned instance directly.

If the storage layer does not expose a typed delete-not-found error, cleanup can
classify this case with a follow-up `FileExists` check on the physical lock
instance. Confirmed absence after a successful tombstone write is cleanup
success. Unknown existence remains a cleanup error.

Cleanup does not need to re-read or re-verify the tombstone after a successful
tombstone write. Once the tombstone write succeeds, the cleanup decision is the
durable fact. A later delete success or delete-not-found only confirms the
physical object is gone.

Tombstone writes do not need a complex per-candidate retry loop in the first
design. If writing the tombstone fails once, cleanup should not delete the
instance; it should report a candidate cleanup error and rely on the existing
outer acquire/cleanup retry path to try again. After a tombstone write succeeds,
delete failures are also retried by later cleanup attempts, which can delete the
already-tombstoned instance directly.

Cleanup should first build the cleanup-eligible candidate list for the logical
family, then batch-list tombstones for the corresponding family/kind prefixes
and build a tombstoned set. Only after both the lock-family view and tombstone
view are available should it process candidates:

```text
list family candidates
classify cleanup-eligible instances
batch-list tombstones for this logical family
build tombstoned set by family/kind/generation

for each cleanup-eligible instance:
    if instance is tombstoned:
        delete directly
        continue

    read meta
    now = leaseClock.Now()
    if existingStalePredicate(now, meta.ExpireAt, LeaseTTL):
        write singleton tombstone
        delete physical lock instance
```

The per-candidate metadata read is a double-check against the latest visible
`ExpireAt` before cleanup writes the tombstone. Cleanup should not tombstone or
delete a non-tombstoned instance based only on the list result. If reading
metadata, parsing metadata, or reading PD time fails, that candidate is reported
as a cleanup error and is not reclaimed.

This double-check does not eliminate the race between the metadata read and the
tombstone write. It only makes cleanup use the freshest metadata it can observe.
The durable tombstone and the holder-side tombstone check are what preserve the
cleanup decision if a delayed renewal write later recreates the object.

The stale predicate itself should keep the existing boundary condition. The
current behavior is strict `now.After(meta.ExpireAt.Add(LeaseTTL))`; this design
does not change it to `>=` or otherwise adjust the cleanup grace window.

If the batch tombstone list fails, this family cleanup attempt should not write
new tombstones or delete any candidate. Tombstones are part of the cleanup
protocol; without that view, cleanup cannot safely distinguish ordinary lock
instances from already-tombstoned zombies.

## Two-Phase Metadata Field

A proposed alternative was to add an internal field such as `renew_pending` to
the lock metadata:

```text
1. write refreshed metadata with renew_pending=true
2. verify the renewal is still within the previous lease window
3. write again with renew_pending=false
4. only pending=false is considered usable
```

This is not the preferred direction right now.

It makes the delayed-write state explicit, but it introduces a new intermediate
state and a new recovery problem. If the process crashes after writing
`renew_pending=true` and before clearing it, cleanup and acquire must understand
that state. If the pending write itself is delayed until after another holder
acquires a new generation, the old generation can still reappear, only now as a
pending lock. The gap is transformed into a recovery protocol; it is not
removed.

Without storage-level conditional write / object version support, this approach
does not provide a simple proof that the object was not deleted and recreated
between read and write.

## TSO-Based Conflict Arbitration

Another alternative was to attach a PD TSO to lock writes and use the larger TSO
as the winner when multiple conflicting lock instances are observed. This is
useful diagnostic information, and an immutable acquire-time TSO may still be a
good observability field, but it is not enough to replace cleanup tombstones.

The problem is that TSO arbitration only works while conflicting instances are
simultaneously visible. It cannot preserve the history that another holder
legally acquired a newer lock, performed work, and then unlocked before an old
delayed renewal write reappeared.

Example:

```text
A owns genA.
A reads genA and sees it has not expired.
A starts renewal WriteFile(genA), but the write is delayed for 10 minutes.

B later observes genA as expired enough to reclaim.
B deletes genA.
B acquires genB.
B performs lock-protected work.
B unlocks and deletes genB.

A's delayed WriteFile(genA) returns success.
Only genA is visible now.
A sees its own TxnID and refreshed metadata, then continues work.
```

At the end, there is no visible `genB` left to compare against `genA`. Even if
`genB` had a larger acquire TSO, that evidence disappeared when B unlocked.
The remaining `genA` object looks self-consistent unless the cleanup decision
against `genA` was recorded elsewhere.

Cleanup tombstones preserve exactly that missing historical fact:

```text
B must tombstone genA before deleting it.
If A's delayed WriteFile recreates genA later, A's renewal check sees the
tombstone and reports lease lost.
```

Therefore, TSO fields can help ordering and troubleshooting, but they do not
replace the tombstone protocol for delayed renewal writes.

## Current Residual Risk

The strongest complete proof would require storage support such as an ETag,
generation ID, or object version returned by `ReadFile` and checked by a
conditional `WriteFile`. The current `storeapi.Storage` interface does not
expose that.

Until such a primitive exists, the agreed first-version direction is:

- use a shared lease clock, backed by PD time in BR;
- separate the `LeaseClock` interface from its implementation, with the first
  BR implementation using direct PD time rather than a PD-time cache;
- allow a local-clock default only as legacy compatibility for non-BR callers,
  while requiring BR lock paths to inject the PD-backed clock;
- use the PD-backed lease timestamp for acquire metadata and the generation
  time prefix;
- scope the protocol proof to strong-consistency storage and fail closed when
  the needed tombstone or lock visibility cannot be trusted;
- require acquire to take PD time after acquire-time writes and verification,
  and to fail before exposing the lock if the acquired lease window is already
  expired;
- require each renewal to take PD time before and after the refresh write, and
  to complete before the previous `ExpireAt`;
- compute the next renewal delay and local watchdog duration from the remaining
  proven lease window after acquire or renewal, not from a fresh full
  `LeaseTTL`;
- cancel any in-flight renewal attempt when the local watchdog fires, while
  treating that cancel only as resource hygiene;
- keep watchdog ownership inside the renewal loop rather than exposing it to
  business callers;
- add singleton cleanup tombstones so cleanup deletion becomes durable before
  the object delete;
- keep renewal scoped to the holder's own physical instance, without
  family-level arbitration;
- keep acquire conservative by not ignoring tombstoned lock files in family
  verification;
- keep generation stable during renewal;
- treat late or tombstoned renewal as lease lost;
- document the remaining crash window explicitly.

## First Implementation Scope

The first implementation should include:

- add a `LeaseClock` abstraction in the lock layer, with BR injecting a direct
  PD-backed clock;
- keep a local-clock default only for compatibility paths that do not claim the
  BR delayed-renewal correctness guarantee;
- use PD-backed time for acquire metadata, generation time prefix, renewal
  checks, and cleanup stale checks;
- keep the lock proof scoped to strong-consistency storage semantics for lock
  and tombstone reads, writes, lists, and existence checks;
- keep existing renewal and cleanup expiration boundary predicates;
- make acquire prove the acquired lease is still valid after acquire-time writes
  and verification, before returning an active lock to business code;
- make renewal take PD time before and after the refresh write, anchor the new
  `ExpireAt` at the pre-write PD time, and accept success only if the previous
  lease window is still valid after the write;
- compute first/next renewal delays from the lease anchor plus `renewInterval`,
  subtracting time already consumed by acquire or renewal;
- reset the local watchdog using the remaining proven lease window after acquire
  or renewal;
- make the renewal state terminal: any observed failure transitions the lock to
  lost, calls `onLeaseLost` once, and cannot be reversed by a later successful
  storage return;
- let normal `stopRenewal` or `Unlock` stop the watchdog and cancel in-flight
  renewal attempts without calling `onLeaseLost`;
- distinguish confirmed missing own lock object from transient read errors in
  renewal;
- write singleton empty cleanup tombstones before deleting stale generation lock
  instances;
- make cleanup read the latest metadata and PD time before tombstoning a
  non-tombstoned candidate;
- let cleanup directly delete already-tombstoned generation instances;
- make renewal check exactly its own singleton tombstone path;
- keep acquire fail-closed: it does not read tombstones or ignore visible lock
  files;
- leave legacy or malformed lock names undeleted by the tombstone protocol, but
  report them as candidate cleanup errors while continuing other parseable
  candidates.

The first implementation should not include:

- renewal intent files or renewal participation in the acquire
  `conditionalPut` protocol;
- acquire protocol changes beyond using the PD-backed lease timestamp;
- family-level arbitration or intent scanning during renewal;
- tombstone garbage collection;
- PD-time cache optimization;
- TSO-based conflict arbitration;
- an extra minimum remaining lease threshold;
- `Unlock` writing cleanup tombstones;
- automatic cleanup of legacy or malformed lock names.

## Testing Guidance

This analysis intentionally does not contain the full executable test matrix.
The implementation plan should design that matrix in detail before code work
continues. The tests should focus on race-shaped correctness boundaries rather
than only happy-path renewal:

- delayed renewal write after cleanup and replacement acquire;
- acquire write/verification returning after the acquired lease window has
  expired;
- watchdog firing while renewal storage I/O is blocked;
- successful renewal that returns with little remaining lease time and therefore
  schedules the next renewal immediately;
- confirmed missing own lock object during renewal;
- tombstoned zombie instances blocking acquire but being directly deletable by
  cleanup;
- PD time failure classification in acquire, renewal, and cleanup;
- legacy or malformed lock names remaining undeleted while other parseable
  candidates can still be cleaned.

There is a preserved regression test for the unresolved delayed final-write
scenario:

```text
pkg/objstore/locking_test.go:
TestTryRenewSuccess/blocks_competing_acquire_while_final_renew_write_is_pending
```

At the time of this note, that test is expected to fail until the renewal
strategy is finalized.
