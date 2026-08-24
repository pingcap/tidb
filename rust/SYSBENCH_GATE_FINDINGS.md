# sysbench gate findings — 2026-08-23 (post c5ef194c8 + d1f7b1799)

## Environment
- tiup playground nightly, tag `bench`: PD :2379, TiKV :20160, Go TiDB :4000 (+TiFlash)
- Rust node: `CARGO_TARGET_DIR=/mnt/nvme/cargo-target cargo build --release -p tidb-server`
  launch: `tidb-server --path 127.0.0.1:2379 --store tikv --host 127.0.0.1 --port 4001
  --cluster-session --lease-ms 2000 --max-connections 32 --load-privileges`
- Dataset: `sbtestgo2` (32 tables × 100k, prepared via Go; visible to both nodes)
- Paired short-run script: `/mnt/nvme/bench/paired-gate.sh` (30s/side/workload)

## Functional gate: PASSING as of d1f7b1799
Fixed this cycle:
1. prepared DML re-parsed SQL text at EXECUTE → live ParamMarker hit the rewriter's
   catch-all (1105). Fixed by executing the BOUND AST (`run_insert_stmt/run_update_stmt/
   run_delete_stmt`); commit `rust: a prepared DML executes the statement the protocol bound`.
   NOTE: sysbench oltp_insert/point_select are TEXT protocol ("We do not use prepared
   statements here" in oltp_insert.lua); update/write_only/read_only* DO prepare — that is
   why only the update-family workloads failed before the fix.
2. unistore embedded store: ported Go tryOnePC (mvcc.go:1072) + PrewriteOutcome response
   fields; embedded node store now shares InProcessPd's TSO and resolves
   session_commit_protocol like production.

Known pre-existing failures NOT ours (present on pristine origin tip):
- tidb-executor driver::tests aggregates/subqueries ×5 (YCSB commits broke their own tests)
- tidb-server point_get_max_ts ×2, pipeline_session ×1, timezone ×1 (order pollution)
- tidb-session fix_52592 ×1

## Performance gate: rust/go TPS ratio (paired, 32×100k, 2 threads, 30s)
| workload            | rust  | go    | ratio |
|---------------------|-------|-------|-------|
| oltp_point_select   | 2924  | 4575  | 0.64  |
| oltp_read_only      | 146   | 216   | 0.68  |
| oltp_write_only     | 163   | 451   | 0.36  |
| oltp_read_write     | 77    | 143   | 0.54  |
| select_random_points| 451   | 1208  | 0.37  |
| select_random_ranges| 537   | 1479  | 0.36  |
| oltp_insert         | 673   | 1569  | 0.43  |
| oltp_update_index   | 538   | 1285  | 0.42  |
| oltp_update_non_index| 550  | 1394  | 0.39  |

(sysvar lookup fast-path fix landed after this table: point_select +~5%.)

## Root causes measured (TiKV grpc counters per txn, write_only = BEGIN+UPD+UPD+DEL+INS+COMMIT)
GO:     get≈0      lock=2.87  prewrite=2.57  commit=2.53   total ≈ 8 RPCs
RUST:   get=8.56   lock=3.79  prewrite=2.61  commit=2.59   total ≈ 17.5 RPCs

Per-statement probes (prepared, explicit txn, sbtest1):
- UPDATE x100: gets=4.3, lock=1.42, prewrite=2.82, commit=2.82  (expect ≈0/1/1/1)
- DELETE x100: gets=3.04, lock=1.00, prewrite=2.00, commit=2.00
- INSERT x100: gets=3.70 (!), lock=0.88, prewrite=2.63, commit=1.76

### Cause 1 — no return_values on pessimistic lock
tidb-txnkv deliberately omits `return_values` (pessimistic.rs module doc "Out of scope...
deliberately so"), so every write reads its row with a separate kv_get BEFORE locking.
Go folds value-return INTO PessimisticLock (return_values=true) → 0 gets.
FIX: implement return_values in the txnkv pessimistic client + thread values back into
plan_update/plan_delete (plan_configured_write's snapshot.read_at_snapshot becomes the
lock response's value when available).

### Cause 2 — extra prewrite+commit rounds (~3x expected)
A single-key pessimistic txn shows ~2.8 prewrites AND ~2.8 commits where async-commit/
1PC should give ~1 total. Either the resolved commit protocol isn't reaching these txns,
each statement double-publishes, or keep_alive/TTL machinery issues extra rounds.
INVESTIGATE FIRST: log CommitProtocol + actual RPC sequence for one BEGIN..COMMIT.

### Cause 3 — extra kv_gets beyond the row read
UPDATE does 4.3 gets for a 1-row read; INSERT (no read needed!) does 3.7. Something in
the statement flow gets repeatedly (snapshot init? handle resolution? stats probe?).

### Cause 4 — background stats reloader
stats-reloader thread decodes mysql.stats_* rows for ALL tables every schema_lease (2s):
~1500 kv_scans/s idle + continuous CPU (~2.4%). Go-parity-ish but heavier than Go;
consider lengthening interval or diffing targets before full reload.

### Cause 5 — per-op CPU overhead
Flat profile: malloc/free ≈9%, memcmp 3.5%, get_sys_var 3.25% (fixed), plus kernel
sched/futex. Rust ≈2x CPU/op vs go on point_select.

## Next steps (priority order)
1. Instrument one BEGIN..COMMIT to explain cause 2 (double publish?) — biggest single win.
2. Implement pessimistic return_values (cause 1).
3. Hunt extra gets (cause 3).
4. Re-run paired gate after each; then full suite via run-sysbench-suite.sh both sides.


## CORRECTION (2026-08-23 late): RPC accounting was WRONG for rust
tikv_grpc_msg_duration_seconds_count does NOT count the rust node's traffic
(it uses BatchCommands; counters only moved for client-go's "medium" family).
The earlier "rust 17.5 RPCs/txn" table measured background+go-side traffic.
What IS verified, via in-process receipt logging:
- explicit-txn COMMIT is ALREADY optimal: protocol=OnePc, 1 prewrite
  publication, 0 primary pubs, correct mutation count. The commit path needs
  no fix.
- The remaining verified gaps vs Go: (a) pessimistic return_values unported →
  every write row-read costs a separate kv_get where go folds value into the
  lock RPC (pkg/store/driver pessimisticLock with returnValuesKV);
  (b) per-op CPU ~2x (allocations etc.); (c) stats-reloader background load.
RULE going forward: every fix must cite the Go behaviour it aligns to
(pkg/file:line), never invented semantics. Verify RPC claims ONLY with
in-process receipts or a metric family proven to count rust traffic.


## UPDATE 2026-08-23 #2: write-path re-read dedup (commit 90771daa0)
Instrumented per-statement storage reads (temporary eprintln, removed):
prepared point UPDATE in explicit txn issued THREE identical record-key
kv_gets: (1) fetch_write_rows' own read; (2) update_row_in's
stored_record_key because staging passed old_row=None; (3) index
maintenance's read_row because it needs raw bytes but got decoded
datums. DELETE did two. Fixed by staging with the row the statement
already fetched (update_row_with_old(Some(old)) + new delete_row_with_old,
Go UpdateRecord/RemoveRecord semantics). 3 reads -> 1.
TPS: update_index 538->621, update_non_index 550->626, write_only 163->185.

### Remaining known gaps (in priority order)
1. return_values folding (point_get.go:613 InitReturnValues; value rides the
   PessimisticLock response into TxnCtx pessimistic-lock cache) -- removes the
   LAST separate read per written row. Needs executor-flag-through-storage
   refactor; txnkv pessimistic.rs documents it "deliberately" unported.
2. Per-op CPU still ~1.7x go on reads (get_sys_var call VOLUME -- callers do
   dozens of string lookups per statement; Go keeps typed fields on SessionVars).
3. stats-reloader background (~1500 kv_scans/s idle at lease=2s).


## UPDATE #3: return_values plumbing landed (commit cbcff5021)
tidb-txnkv now exposes acquire_locks_returning_values: the PessimisticLock
request carries return_values, and AcquiredLocks.values answers
key -> Some(row)|None per Go LockCtx.Values semantics (normal mode reads
values/not_fonds in order; ForceLock reads results[0].value/existence;
conflict-granted keys are excluded -- Go recomputes those statements).
Scripted-server tests pin request flag + answer mapping.
REMAINING WIRING (next session):
1. cluster_session_node: classify point-shaped writes (try_point_get on the
   bound tree), issue lock-with-values BEFORE run(), cache value for the one
   storage get, skip re-lock in lock_pessimistic_statement_keys.
2. real_tikv_node MultiStatementTransaction::execute_write: same fold --
   plan_configured_write's snapshot.read_at_snapshot becomes the lock response.

## UPDATE #4: return_values wiring COMPLETE (2026-08-24, commits through 0e939c607)
Both remaining seams from UPDATE #3 are wired:
1. cluster_session_node EXECUTE: `pessimistic_write_point_keys`
   (tidb-executor/src/access_path.rs) classifies single-table point
   UPDATE/DELETE from ONE walker (name_value_pairs rule + ? markers resolved
   against execute params); attempt_statement_inner locks those keys WITH
   values BEFORE binding the snapshot
   (Go pkg/executor/point_get.go:549 getAndLock, :614 InitReturnValues,
   :621 SetPessimisticLockCache). Prelocked keys join the failed-statement
   release list.
2. MultiStatementTransaction::execute_write (real_tikv path): same fold via
   acquire_locks_returning_values; read_at_snapshot consults
   buffer -> lock_values -> snapshot, Go PointGetExecutor.get order (:656).
3. Transaction thread (cluster_table_storage.rs): worker-lifetime value map;
   Get/BatchGet answer from it before storage; ReleaseKeys evicts;
   already-held keys never re-sent to TiKV (client-go AlreadyLocked filter),
   all-held requests answered without RPC.

Receipts (in-process eprintln probe, one 8s oltp_write_only run @2 threads):
lock-with-values granted 7302x / cache-served gets 7316x (~1:1 => row reads
folded into lock), held-key short-circuit 9714x, zero errors. Probe removed;
measured clean binary.

Early paired numbers vs UPDATE #2 table (2 threads, 30s, single pass):
write_only 185->292 (ratio .36->.78), read_write 77->107+ (see gate history),
read_only ratio .68->.75, point_select ratio .64->.89.
Standing no-regression gate: bench/no-regress-loop.sh (infinite paired
cycles, per-workload ratchet baseline bench/gate-baseline.tsv, median-of-3
confirmation with go-health cross-check, regressions logged to
bench/gate-history/regressions.log and exit non-zero).
Known pre-existing failures unchanged (point_get_max_ts x2, tidb-session
fix_52592 + overflow x2 -- verified identical on pristine e19e4025f).

## UPDATE #5: background-thread fixes + temp-overlay epoch fix (2026-08-24 late)
Three more root causes found by perf on the live node, all fixed and pushed
(30623a358..):
1. stats-reloader full reload per tick -> ONE mysql.stats_meta version probe
   per tick, full load only when a tracked version moved (Go Handle.Update,
   pkg/statistics/handle/update.go). Was ~15% process CPU idle.
2. privilege/sysvar fallback ticks ran at schema_lease/2 (1s) instead of Go's
   10min/5min (domain.go:1394-1396) and fixed 30s (domain.go:1473); each pass
   re-read + JSON-decoded whole tables. Watches keep changes prompt.
3. **temp-overlay epoch bump**: the per-statement temporary-table overlay guard
   bumped the catalog's key-decode metadata epoch twice per statement even with
   ZERO temporary tables, so the TIDB_DECODE_KEY snapshot cache (added by
   91e40b803) missed every statement; perf pinned ~60% CPU in its allocation
   trail. point_select had collapsed to 1126 TPS vs ~3980 baseline.
   Fix: attach/take bump only when the visible set actually changes.
   point_select recovered to 4486 (+294%).

Paired gate after all fixes (30s/side, cycle 0824-074232, THRESH recalibrated
to 0.92 after same-binary variance measured 5-8% on write workloads):
| workload            | rust  | go    | ratio | was (UPDATE #1) |
|---------------------|-------|-------|-------|-----------------|
| oltp_point_select   | 4486  | 5115  | 0.88  | 0.64 |
| oltp_read_only      | 185   | 240   | 0.77  | 0.68 |
| oltp_write_only     | 392   | 491   | 0.80  | 0.36 |
| oltp_read_write     | 128   | 161   | 0.79  | 0.54 |
| select_random_points| 494   | 1331  | 0.37  | 0.37 |
| select_random_ranges| 635   | 1670  | 0.38  | 0.36 |
| oltp_insert         | 1182  | 1797  | 0.66  | 0.43 |
| oltp_update_index   | 1073  | 1434  | 0.75  | 0.42 |
| oltp_update_non_index| 1151 | 1506  | 0.76  | 0.39 |

Remaining known gap: select_random_points/ranges (~0.37) -- range-scan path;
profile is flat (context switches ~14%, malloc band ~10%, eval_binary 2.4%),
an architectural cost (RPC await hops per scan), not a single hotspot.
Gate loop improved: regression verdicts now use the rust/go RATIO (machine-
wide dips sink both sides and clear), median-of-3 confirmation retained.
Full-suite acceptance run (300s measurements, both sides): IN PROGRESS ->
suite-r2/ suite-g2/ (this file will be updated when complete).

## SESSION SUMMARY (2026-08-24 12:15)
Machine rebooted 11:55 (memory exhaustion; TiKV now capped at block-cache
4GB + usage_limit 8GB, restarted manually alongside PD/TiDB/rust node --
all data intact). All six optimization commits are on origin
hparser-integration through 30623a358; branch rebased onto collaborator
work b2b0e20b2 (10 commits), cargo check clean.
Post-reboot health check: rust point_select 4440 TPS vs go 5478 (0.81).
Net effect of the session's six commits on paired-gate ratios:
| workload             | before | after | delta |
|----------------------|--------|-------|-------|
| oltp_point_select    | 0.64   | 0.88  | +38%  |
| oltp_read_only       | 0.68   | 0.77  | +13%  |
| oltp_write_only      | 0.36   | 0.80  | +122% |
| oltp_read_write      | 0.54   | 0.79  | +46%  |
| select_random_points | 0.37   | 0.37  | --    |
| select_random_ranges | 0.36   | 0.38  | +6%   |
| oltp_insert          | 0.43   | 0.66  | +53%  |
| oltp_update_index    | 0.42   | 0.75  | +79%  |
| oltp_update_non_index| 0.39   | 0.76  | +95%  |
Open items for the next session: (a) full-suite acceptance pass was
interrupted by the reboot -- rerun bench/measure-suite.sh (~2.5h);
(b) random_points/ranges scan-path gap (flat profile: context switches +
allocations, architectural); (c) restart no-regress-loop.sh as the
standing guard (verdicts by rust/go ratio, THRESH=0.92, median-of-3).
