# tidb-owner parity audit

Go source: `pkg/owner` @ a85e0fd5df (`manager.go` 723 lines, `mock.go` 235,
`mock_owner_state.go` 85). Rust: `rust/crates/tidb-owner` (`lib.rs`, `mock.rs`,
`tests/manager_source.rs`).

The port is a seam adaptation: Go drives `clientv3` + the etcd `concurrency`
recipes (`Election`/`Session`/`Mutex`) directly; Rust declares an
[`OwnerStore`] trait (lease, create-with-lease, CAS-with-lease, prefix read,
watch) implemented by `tidb_pd_client::EtcdClient` and by the test `FakeStore`.
Every Go control-flow decision maps onto that seam.

## Fixed divergences (this batch)

1. **Watch base revision was `ModRevision`, Go uses the response header
   revision.** `getOwnerInfo` returns `resp.Header.Revision` (4th return;
   `ModRevision` is the discarded 5th), `GetOwnerKeyInfo` forwards it, and
   `campaignAndWatch` starts `watchOwner` at `currRev + 1`
   (`manager.go:461-487, 518-538, 614-619`). Rust `get_owner_key_info`
   returned `owner.mod_revision` and `campaign_loop` watched from
   `mod_revision + 1` -- events between the key's creation and the read could
   be missed by a store that replays from the watch revision. Fixed: the
   `OwnerStore` trait gained `get_prefix_metadata_with_revision` (backed by
   the existing `etcd.rs` `GetPrefixMetadata` command, which sorts
   `Create`/`Ascend` and already carries the header revision), `get_owner_info`
   returns the header revision, and both the campaign watch start and
   `get_owner_key_info` use it. Pre-fix regression
   `test_get_owner_key_info_returns_header_revision` failed (returned 1 = the
   key's mod revision instead of 2 = the store's global revision).

## Verified equal

- `Manager` interface: all 12 methods, with Go's exact error strings
  ("This node is not a owner, can't be resigned", "ownerInfoNotMatch",
  "put owner key failed, cmp is false", "election: no leader").
- `OpType` byte values, `String()` rendering ("sync upgrading state"/"none"),
  `IsSyncedUpgradingState`.
- `ForceToBeOwner`: session refresh, the 3-attempt loop each sleeping
  `WaitTimeOnForceOwner` first, single-txn delete-others-plus-put-own
  (`tryToBeOwnerOnce`), the 5s campaign wait, and the Go quirk of returning
  nil even when all attempts failed. Campaign key format `{key}/{lease:x}`.
- `CampaignOwner` TTL override (`withTTL ...int`), session refresh only when
  absent, `ManagerSessionTTL` 60 + `tidb_manager_ttl` env override.
- `campaignLoop`: session-done / lease-not-found both refresh the session
  with unlimited retries and exit the loop when that fails; campaign key
  (re)creation, first-create wait, the second ownership re-read and its
  `ownerInfoNotMatch`-equivalent retry, become/retire listener calls around
  the watch.
- `SetOwnerOpValue`: same-op no-op, owner check, ModRevision-guarded CAS with
  the session lease, false-CAS error text.
- `GetOwnerID`/`GetOwnerOpValue` (including the nil-store mock arm),
  `WatchOwnerForTest`, `DeleteOwnerKeyByID` (prefix scan, UUID match, single
  delete), `splitOwnerValues`/`joinOwnerValues` wire format
  (`ownerID "_" opByte`; >2 parts -> OpNone, as Go's `len(vals) != 2` arm).
- `AcquireDistributedLock`: lease + `{key}/{lease:x}` lock key (etcd Mutex's
  `pfx + lease` layout), first-create wait, keeper thread, release closure
  deleting the key then revoking.
- `ListenersWrapper` broadcast; `DDLOwnerChecker` blanket impl.
- Mock (`mock.go` + `mock_owner_state.go`): global state map keyed by
  (store ID, owner key) with Get/Set(set-if-empty)/Unset(unset-if-equal)/Is
  exactly, `"mock_store_id"` fallback, `mockOwnerOpValue` reset on
  construction and its nil-store read arm, campaign loop (set-owner tick +
  1s sleep, resign -> retire + 1s pause -> reclaim), `BreakCampaignLoop` =
  `Close` (Go's own documented contract violation for unistore),
  `ForceToBeOwner` no-op, `GetOwnerID` "no owner".

## Documented narrowings (intentional, site-commented or structural)

- Prometheus counters (`CampaignOwnerCounter`, `WatchOwnerCounter`,
  `PanicCounter` with the `LabelDDLOwner` label) have no metrics seam at this
  tier; logging (Go logs every state transition) is likewise absent.
- `splitOwnerValues` panics on a two-part value with an empty op part
  (`vals[1][0]`); etcd never stores that shape. Rust returns OpNone
  (site comment at `split_owner_values`).
- Rust-only `closed` flag rejects `campaign_owner` after `close()`; Go
  documents "after close, no methods can be called" but does not enforce it.
  Re-campaigning while a loop already runs is a Rust no-op where Go would
  start a second loop (leaking the first).
- Failpoints (`MockDelOwnerKey`, `MockNotSetOwnerOp`,
  `mockAcquireDistLockFailed`) are test injection only and are not ported;
  the campaign-loop's failpoint-triggered early return path is therefore
  absent with them.
- `AcquireDistributedLock` does not port Go's `RunWithRetry(10)` around
  `mu.Lock` (the seam's create+wait fails hard instead); retry-on-transient
  will be revisited with the bootstrap-lock consumer.
- Timing: lease-keeper at ttl/3, 20ms campaign poll, 30ms key-op retry, and
  poll-based watch stand in for Go's channel-driven watches and
  `NewSession` internals; `refreshSession`'s per-context 2s
  `KeyOpDefaultTimeout` is owned by the pd-client worker.
- Go's `sessionLease` survives `closeSession` (never reset), so
  `SetOwnerOpValue` after session close retries the stale lease and surfaces
  etcd's lease error; Rust reports "owner session is not initialized".
  Both error; messages differ on that unreachable-in-practice path.

## Validation

- `cargo test -p tidb-owner` -- 13 passed / 0 failed (12 pre-existing + the
  new header-revision regression; pre-fix baseline verified red).
- `cargo check -p tidb-server -p tidb-ddl-notifier -p tidb-workloadrepo`
  (trait consumers) -- clean.
- fmt, `git diff --check`, `make lint` per Ready profile.
