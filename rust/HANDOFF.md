# TiDB to Rust rewrite handoff

_Current operating handoff. Updated 2026-07-20. This records the active
frontier and verified receipts; generated ledger counters remain in
[`STATUS.md`](STATUS.md)._

## Standing goal and completion bar

Rewrite TiDB's SQL layer in Rust without dropping any behavior or original
test obligation. The target is a standalone Rust SQL process using the real
MySQL, PD, TiKV, kvproto, and client-go contracts. Go may act as an independent
oracle over network protocols; it is not an in-process backend.

The user has made these requirements explicit:

- Support both reads and writes through real PD/TiKV. In-memory `Database`,
  injected storage/transaction traits, synthetic rows, and mock transport are
  not acceptance paths.
- Support real MySQL prepared statements. Do not use text interpolation,
  `--db-ps-mode=disable`, or a driver fallback from `COM_STMT_*` to
  `COM_QUERY`.
- Support actual small-scale sysbench and then the ordinary prepared write and
  read/write workloads.
- Implement TiDB-compatible TLS on the production listener, including the
  cryptographic SSLRequest upgrade, certificate verification, secure-transport
  policy, reload, and remaining original TLS tests. A parsed SSLRequest or an
  asserted secure-transport enum is not TLS support.
- Complete the transaction/KV implementation beyond normal optimistic 2PC:
  region-aware BatchGet/Scan/write batching, explicit transaction lifecycle,
  read-your-writes and savepoints, retry/cleanup, lock TTL and heartbeats,
  pessimistic locks, async commit, 1PC, pipelined DML, and the full TiDB and
  pinned client-go test inventory.
- One process owns one PD worker, RegionCache, TiKV BatchCommands transport,
  lock resolver, retry policy, background supervisor, and shutdown order for
  reads and writes. Do not introduce a second transaction client or parallel
  runtime.

The design completion bar is recorded in
[`../docs/design/2026-07-11-tidb-rust-rewrite.md`](../docs/design/2026-07-11-tidb-rust-rewrite.md).

## Read in this order

1. [`STATUS.md`](STATUS.md) — generated queue and source/test ledger state.
2. [`workstreams/plans/2026-07-read-path-27.md`](workstreams/plans/2026-07-read-path-27.md)
   — prepared point-read proof and closure state.
3. [`workstreams/plans/2026-07-read-path-28.md`](workstreams/plans/2026-07-read-path-28.md)
   — first real prepared write vertical and normal optimistic 2PC.
4. [`PARALLEL.md`](PARALLEL.md), [`workstreams/slices/README.md`](workstreams/slices/README.md),
   root `AGENTS.md`, and `PLANS.md` — ownership and validation protocol.

## Verified current frontier

### Campaign 27: real prepared point reads

Implementation and real acceptance pass. The production server owns a
per-connection prepared registry, typed signed-BIGINT parameters with type
reuse, binary rows, silent close, and exact command telemetry.

Final live receipt after the lint-driven production refactor:

- Go TiDB fixture: v8.5.6-dirty, commit
  `ae18096e023780bb56bfce33698abec0d4640d0a`, failpoint/test API enabled.
- Rust server SHA-256:
  `4475d17f451ee5921b37edc0560cb3bc9132a4d7e49c22c45776f0041781195c`.
- Raw client: connection/session 4/4, two binary executes, type reuse, silent
  close, sixteen negative cases with no storage work.
- Actual sysbench 1.0.20 linked to Oracle
  `libmysqlclient.24.dylib`: one thread, exactly eight events, 30-second cap.
- Server wire counters on the sysbench connection:
  `COM_QUERY=0`, `COM_STMT_PREPARE=1/1`,
  `COM_STMT_EXECUTE=8/8`, `COM_STMT_CLOSE=1`.
- Real table/region 114/1010, topology `4 -> 1 -> 5 -> 1`, shutdown 118 ms
  inside a 10,000 ms grace, accepted/completed/failed/active `5/5/0/0`.
- Tag-owned processes, endpoints, data, auth, and runtime state were removed.

Behavioral loopback regressions also prove exact eight-execute accounting and
that a malformed execute increments command count without success. C27 still
has one active live claim and needs the immutable shared gate plus campaign
closure; unsupported cursor/reset/long-data/NULL/unsigned/type breadth remains
explicit in the ledgers.

### Campaign 28 Stage A: transaction RPC leaf

Covered and receipt-released. The sole BatchCommands transport performs typed
real `Get`, `Prewrite`, `Commit`, and `BatchRollback`. The live proof executed
`Prewrite -> Commit -> Get` and `Prewrite -> BatchRollback -> Get(not_found)`
using real PD timestamps, request IDs, routes, channel/stream identities, and
cleanup. Cancellation after publication retains attempt identity.

### Campaign 28 Stage B: normal optimistic 2PC

Implemented, real-live passed, conservatively promoted, and claim retained for
the shared immutable gate. The production transaction opener is capability
only: it derives from an already-running concrete `SharedReadOpener` and
cloned `PdClient`; the standalone second process authority was removed.

Final real receipt:

- cluster `7664574949704693070`
- start/commit TS `467808533790326785 / 467808533868969985`
- primary/secondary regions `26 / 8`
- rollback start TS `467808533868969987`
- older lock TS `467808533868969995`
- newer lock start/commit TS
  `467808534013149188 / 467808534013149189`

The proof covers multi-region batching, primary-containing batch commit,
stale-route regroup with exact old/new epoch and physical address, real older
lock wait/resolve/same-start retry, newer-lock `WriteConflict` without
resolution, rollback cleanup, PutExisting assertions, and independent
readback. Commit ambiguity follows client-go: only an explicitly undetermined
result or a published attempt with no decoded outcome is undetermined; decoded
region/key rejection permits cleanup. `CommitTsExpired` is pinned to the exact
attempted commit TS and one-hour delta. A real zero-duration-at-expiry bug was
fixed with a bounded 10 ms retry delay.

Focused validation includes 64 txnkv library tests, real-target compilation,
and txnkv Clippy with `-D warnings`. Remaining review caveat: secondary-commit
and rollback regroup-failure branches are correct by inspection but still need
direct regression tests before a Ready/PR claim.

### Campaign 28 remaining stages

- Stage C: exact TiDB clustered record-key/rowcodec lowering and prepared
  INSERT/UPDATE planning, with UPDATE reading at the transaction start TS.
- Stage D: the single steward-owned rename/migration from the read-named
  process authority to one KV-wide authority, then prepared DML TCP/OK/error
  framing. No aliases or parallel workers.
- Stage E: one-thread bounded prepared read/write sysbench against Rust,
  independent Go TiDB verification before/after Rust restart, and no text
  fallback. This proves the first write vertical, not full transaction parity.

## Required next campaigns

### Real MySQL TLS

The pure SSLRequest state machine exists, but live MySQL does not advertise or
complete TLS. `mysql_connection.rs` clones `TcpStream` into independent reader
and writer owners, so rustls cannot be truthfully bolted on. First refactor to
one bidirectional plaintext/rustls stream owner; retain a raw clone only for
shutdown cancellation.

Dependency order:

1. `rustls 0.23` + `rustls-pemfile 2`, validated CA/cert/key config, TLS 1.2
   default and TLS 1.3 option, fail-closed startup.
2. Real `CLIENT_SSL` advertisement and SSLRequest socket upgrade before
   credentials, exact packet sequence and pre-read preservation.
3. Client-cert policy and account `REQUIRE SSL/X509/ISSUER/SUBJECT/SAN/CIPHER`.
4. Live `require_secure_transport`, including secure-only dynamic enable.
5. Atomic `ALTER INSTANCE RELOAD TLS [NO ROLLBACK ON ERROR]` retaining last
   good config and established sessions.
6. AutoTLS, status/observability, remaining status/cluster TLS suites, and a
   stock MySQL `VERIFY_IDENTITY` real-PD/TiKV proof.

Primary Go tests include `TestTLSVerify`, `TestTLSBasic`,
`TestErrorNoRollback`, `TestReloadTLS`, `TestInvalidTLS`, `TestTLSAuto`,
`TestTLSVersion`, security config tests, and account TLS privilege cases.

### Complete transactions and batch KV

Use the following dependency order; C28 Stage B is only a reusable normal-2PC
foundation:

1. Real snapshot BatchGet and forward/reverse Scan with region/size batching,
   bounded concurrency, lock resolution, retry/regroup, and Go comparison.
2. One concrete mutable KV transaction per SQL session: mem-buffer, staging,
   tombstones, union reads/iteration, BEGIN/COMMIT/ROLLBACK, autocommit-off.
3. Production normal 2PC completion: write batching, retry budgets, exact
   outstanding cleanup, ambiguity/status recovery, secondary completion.
4. TTL/minCommitTS/TxnHeartBeat, CheckTxnStatus/CheckSecondaryLocks,
   ResolveLock/BatchResolveLocks, owned background lifecycle.
5. Typed TiDB transaction options, session retry/replay, and real savepoints.
6. Pessimistic transactions: lock/rollback, waits/timeouts/kills/deadlocks,
   RC/RR/serializable, shared/fair/aggressive locking.
7. Async commit eligibility, fallback, recovery, and secondaries.
8. 1PC success plus structural fallback to normal 2PC.
9. Pipelined DML flush generations, throttling, range cleanup, and crash
   recovery.
10. Parity/chaos closure plus prepared `oltp_write_only` and
    `oltp_read_write`, verified independently through Go after Rust stops.

The primary closure set is 224 TiDB behavioral top-level transaction tests
plus all nested cases, and six pinned client-go behavioral tests including
`TestBufferBatchGetter`, `TestMinCommitTsManager`, `TestLockKeys`,
`TestSharedLockCommitterIncompatibilities`, `TestLockResolverCache`, and
`TestTryAsyncResolve`.

## Immediate next actions

1. Add direct secondary-commit and rollback regroup-failure regressions, then
   freeze the current C27/C28 claims and run one shared integration gate.
2. Receipt-release/close C27 and release C28 Stage B without consuming or
   invalidating unrelated receipt entries; regenerate status.
3. Implement C28 Stage C, then the sole KV-authority Stage D migration and the
   real mixed read/write live proof.
4. Freeze valid source/test-complete TLS and full-transaction campaigns before
   claiming them. Campaign validation requires at least nine production source
   files and fifty original obligations; do not leave partial draft manifests
   in the tree.
5. Continue ledger triage until every original TiDB and pinned client-go test
   has an explicit honest disposition. Never convert broad source families to
   COVERED from one bounded vertical.

## Validation and repository rules

- Use 12 jobs for every build.
- WIP validation is appropriate while these campaigns remain open. Ready
  requires the repository profile, including `make -j12 lint` for code changes.
- Rust-only work does not require `make bazel_prepare`; follow the root gate if
  any Go/import/Bazel/module trigger appears.
- RealTiKV tests own their TiUP topology and must prove readiness, retain
  diagnostics on failure, and remove only tagged state.
- `campaign_close.py` now supports covered inactive historical members plus
  unrelated active claims: the gate receipt must match the exact active claim
  set, and only active members of the closing campaign are released.
- Keep unsupported behavior fail-closed before PD/TiKV publication.

## Durable local facts

- Checkout: `/Users/qiliu/projects/tidb`
- Integration branch: `hparser-integration`, tracking
  `ngaut/hparser-integration`
- Exact Go v8.5.6 fixture:
  `/Users/qiliu/projects/tidb-rust-worktrees/campaign22-v856-fixture/bin/tidb-server`
- Oracle-MySQL-linked sysbench:
  `/Users/qiliu/projects/tidb/rust/target/sysbench-mysql-client/bin/sysbench`
- Root `godump`, `gorun`, `goeval`, second-opinion outputs, and
  `.agents/skills/second-opinion/` are local helpers/artifacts and must not be
  staged with rewrite code.
- Claims are local coordination state and remain uncommitted. Preserve all
  unrelated user files and never use destructive Git cleanup.
