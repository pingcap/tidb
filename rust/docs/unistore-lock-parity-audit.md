# unistore lockstore/lockwaiter parity audit (baseline a85e0fd5df)

Audit of the two Go packages this crate claims ported whole:
`pkg/store/mockstore/unistore/lockstore` (arena skiplist) vs
`src/lockstore.rs` (+arena.rs), and `.../util/lockwaiter` vs
`src/lockwaiter.rs`. The crate's other modules exceed the claim and are
out of scope.

## Result: no behavior-breaking divergences

- lockstore: constants/node layout (maxHeight 16, header 16, nexts
  8/level), findGreater/findLess/findSpliceForLevel/findLast incl. the
  forced-descend and head guards, Put/PutWithHint height heuristic and
  stale-hint fast path, replace/delete splice semantics and length
  accounting, randomHeight p=1/4 geometric cap 16, MaxEntrySize formula,
  Get buf-refill, arena alignment/overflow/free-window/grow — all match.
- lockwaiter: sentinels (LOCK_NO_WAIT -1, WaitTimeout -1,
  WakeUpThisWaiter 0, WakeupDelayTimeout 1), channel cap 32, 100 ms
  default delay, oldest-by-startTS grant, non-blocking wakes outside the
  manager lock, deadlock blocking send, match predicate, delayed-wait
  timer shortening with already-fired guard, CleanUp draining — match.

## Aligned this batch (low)

1. `lockwaiter.rs`: the delayed-wait deadline now uses Go's raw signed
   arithmetic — a negative configured `wake-up-delay-duration` keeps the
   original deadline (the port clamped to 0, returning immediately).
2. `lockstore.rs` replace/delete: assert non-null `hint.prev[i]` before
   `node_set_next`, panicking like Go's nil index instead of silently
   writing block 0 (unreachable with valid hints).

## Documented narrowings (already at sites)

- Height RNG is splitmix64 vs Go's seeded lagged-Fibonacci source (same
  distribution); `with_seed` is a Rust-only test aid.
- Atomics-vs-borrow discipline replaces Go's atomic link load/store;
  iterator lives in lockstore/iterator.go -> iterator.rs, outside this
  scope.
- The waiter timer is per-call (Go keeps a persistent timer field); no
  caller waits twice. Equal-startTS waiter order is stable here,
  unstable in Go.

## Validation

- `cargo build -p tidb-unistore` and `cargo test -p tidb-unistore --lib`
  note: the lib TEST target currently fails to compile from a pre-existing
  trait-bound break between the sibling distsql realignment and
  `client::InProcessClient` (verified identical with and without this
  batch's changes); the lib itself compiles clean.
- `cargo fmt`, `git diff --check`, `make lint`.

## Follow-up (same session)

The pre-existing lib-test compile break is resolved: `InProcessClient` now
implements `SynchronousBatchRequestDispatcher` (in-process coprocessor
dispatch; address/forwarding irrelevant, cancellation short-circuits via
`CallerCancelled`), completing the glue the sibling distsql realignment
needed from this crate. `cargo test -p tidb-unistore --lib`: 114 tests
pass — this suite was entirely unrunnable before the impl.

## mvcc_store pessimistic-path status (2026-09-07 audit)

The module header previously declared the pessimistic path "a later course"
with `Prewrite` refusing `for_update_ts > 0`. That contract had drifted: the
tree implements the pessimistic suite (`pessimistic_lock`,
`prewrite_pessimistic`, `pessimistic_rollback`, `txn_heart_beat`,
`check_txn_status`, `check_secondary_locks`, `resolve_lock`) with transcreated
Go tests. Audit findings against `mvcc.go` @ a85e0fd5df:

- Verified equal point-by-point in `pessimistic_lock` vs
  `pessimisticLockInner` (`mvcc.go:226-390`): sort-unless-ReturnValues,
  the LockOnlyIfExists contract error (message now Go's literal text,
  pinned by
  `lock_only_if_exists_without_return_values_keeps_go_error_text`),
  lock-type-mismatch, the duplicate-command rule
  (`lock.ForUpdateTS >= req.ForUpdateTs`), the primary key's
  extra-txn-status arms (`ErrAlreadyRollback` / op-lock-committed = dup),
  the `Force` first-key value/commit-ts answer, and the
  `ReturnValues`/`CheckExistence` response filling.
- Header rewritten to state the real landed set; the named absent pieces are
  `Flush` (`mvcc.go:986`), async-commit/1PC (PD-refused, unchanged), and the
  pessimistic lock-WAITING machinery (`lockWaiterManager`,
  `normalizeWaitTime`, `handleCheckPessimisticErr`'s wake-up path,
  `WakeUpModeForceLock` per-key `Results` with `LockedWithConflictTs`) —
  a conflicted lock returns its error immediately (Go's no-wait outcome)
  without the parked-then-retry behavior.
- Still open for a full-budget session: line-level audit of
  `prewrite_pessimistic`/`pessimistic_rollback`/`check_txn_status`/
  `resolve_lock` bodies against `mvcc.go:435-935`, and the `cophandler`
  seed vs the closure_exec/analyze/mpp tail.

## mvcc.go:418-935 body-level audit (2026-09-07, same session)

Every pessimistic-suite body compared line-by-line against `mvcc.go` @
a85e0fd5df:

- `pessimistic_rollback` vs `PessimisticRollback` (435): sorted keys, the
  four-way lock predicate (present, `Op_PessimisticLock`, our start ts,
  `ForUpdateTS <= req.ForUpdateTs`), delete-only write. Equal; the
  `WakeUp`/`CleanUp` calls stay named narrowings.
- `txn_heart_beat` vs `TxnHeartBeat` (465): existence + start-ts gate,
  non-primary rejection, never-shrink TTL raise through the lock store,
  TTL answer. Equal.
- `check_txn_status` vs `CheckTxnStatus` (497): primary mismatch,
  async-commit guard, physical-millisecond TTL expiry with the
  resolving-pessimistic vs plain rollback split, `MinCommitTSPushed` under
  `maxSystemTS` and the `minCommitTS >= callerStartTS + 1` invariant with
  `max(callerStartTs+1, currentTs)`, then the no-lock ladder
  (committed / rollback / op-lock committed / `RollbackIfNotExist` with its
  two actions / `ErrTxnNotFound`). Equal, including the tombstone writes.
- `check_secondary_locks` vs `CheckSecondaryLocks` (590): the sorted walk,
  the commit short-circuit, the op-lock short-circuit, the on-the-spot
  tombstone for neither, and the pessimistic-secondary immediate
  `Rollback(key, true)`. Equal.
- `resolve_lock` vs `ResolveLock` (1645): the empty-key scan, start-ts
  filter, and the per-key write arms replicate `write.go`
  `Commit`/`Rollback` exactly (pessimistic lock deleted with nothing
  written; non-Lock ops written at the commit ts; the primary's Op_Lock
  landing in the extra-status key; rollback tombstone plus lock deletion).
- `prewrite_pessimistic` vs `prewritePessimistic` (850): the constraint map
  with its range check, `CheckNotExists` -> `ErrInvalidOp`, the
  DO_PESSIMISTIC_CHECK valid/`pessimistic lock not found`/duplicated/TT L
  raise arms, DO_CONSTRAINT_CHECK's `LazyUniquenessCheck` conflict, the
  non-pessimistic-key TTL-zero lock error and duplicate short-circuit, then
  the shared `prewriteMutations`. Equal. Go's unguarded `lock.ForUpdateTS`
  under a constraint for a lockless key would panic; the Rust seam treats
  that shape as passing (documented hardening, same class as
  `splitOwnerValues`).
- `build_pessimistic_lock` vs `buildPessimisticLock` (663): the
  latest-extra-meta raise, the `Force` skip, the `PessimisticRetry`
  conflict, `Assertion_NotExist` -> `ErrKeyAlreadyExists`, `doesNeedLock`,
  and the lock fields (in the Normal-only slice `lockedWithConflictTS` is
  always zero, so `ForUpdateTS` is the request's). Equal.

Fixed in this batch: three contract-refusal messages carried decorative
Rust text instead of Go's wire-visible strings -- `TxnHeartBeat`'s
"heartbeat on non-primary key" and "lock doesn't exists" (both
`errors.New` literals), and `prewritePessimistic`'s formatted range check
("...constraint set for index %v while %v mutations were given", now a
`KvError::Message(String)` arm rendered through the Abort field). Three
regressions pin the texts.

## cophandler front-door audit (2026-09-07, same session)

`cop_handler.go` @ a85e0fd5df compared against `cophandler.rs` (2,460 lines).
Structural note: Go's ordinary DAG path is now MPP-first —
`handleCopDAGRequest` routes through `buildAndRunMPPExecutor` and the
`mpp_exec` closure tree — while the Rust seed executes its own flat
scan/selection/limit/aggregation lowering. A like-for-like front-door audit
therefore belongs to the mpp course; this round covered the pieces that are
architecture-independent:

- Dispatch (`HandleCopRequestWithMPPCtx` 98-110 vs `handle_cop_request`):
  the four request types and Go's exact "unsupported request type %d"
  answer (already pinned by a test).
- `buildDAG` guards (393-442 vs `build_dag`): empty ranges
  ("request range is null"), the type check, proto decode, and the
  three-way timezone split are equal; `resolvedLocks`/`keyspaceID` ride the
  region-context narrowing.
- FIXED: `handleCopChecksumRequest` answers a marshalled
  `tipb.ChecksumResponse{1,1,1}` -- a fake SUCCESS on Go's side, a refusal
  here. The trimmed tipb build drops `ChecksumResponse`, so the six
  deterministic wire bytes are hand-encoded with the field numbers cited;
  pinned by `checksum_answers_gos_stub_response`.
- FIXED: `extractKVRanges`' malformed-range rejection
  ("invalid range, start should be smaller than end: %v %v", Go `%v`
  byte-slice rendering) was absent; a malformed request now answers Go's
  error before any scan, pinned by
  `a_malformed_range_answers_gos_validation_error`. The region clipping
  halves (`maxStartKey`/`minEndKey`/reverse-on-desc) stay under the
  whole-keyspace narrowing -- the Rust scans walk the request's ranges
  directly, reversing for desc.
- Verified equal at the composition layer: `validate_executor_list`
  enforces the parentIdx/leaf invariants `ExecutorListsToTree` panics on,
  and the scan/selection/limit/aggregation lowering answers the
  `closure_exec` shapes the seed claims.

Still open for a full-budget session: the mpp course (`mpp.go` 780 +
`mpp_exec.go` 1579 -- on Go's ordinary path now), `closure_exec.go`'s
remaining expression/TopN executors (1,218), `analyze.go` (704), and the
row-decoder warnings channel.

## TopN executor port (2026-09-07, same session)

The pushed-down bounded sort lands in the flat lowering:
`buildTopNProcessor`'s parse (order-by keys with per-key direction from the
ByItem field-type collation, heap size = the TopN limit), key evaluation at
row-survival time through the leaf `eval_datum` (computed keys refuse by
name, as group-by keys already do), and the Finish replay-in-key-order
contract -- the buffering lowering sorts once instead of heap-evicting, the
same N rows in the same order. `compare_sort_keys` ports
`types.Datum.Compare` for the producible kinds: NULL before every non-NULL
datum, value comparison across signedness, `Real`/`Float32` total order,
`Time`/`Duration` through their own comparators, strings under the key's
collation; exotic kinds are refused at evaluation so the comparator is
total. A separate Limit above the TopN caps the replay further, as Go's
limitProcessor does; Aggregation+TopN refuses by name like Aggregation+Limit
already did. Regressions: DESC keeps the two largest largest-first, ASC
keeps the two smallest smallest-first (pre-fix the shape refused as a later
course). Go's unstable `sort.Sort` tie order is a documented narrowing --
the stable Rust sort is deterministic where Go leaves ties unordered.

## Server command-surface audit + NeedCommitTs (2026-09-07, same session)

`server.go` @ a85e0fd5df compared against `kv_handler.rs`. The twelve landed
handlers (get/scan/batch_get/prewrite/commit/batch_rollback/
pessimistic_lock/pessimistic_rollback/txn_heart_beat/check_txn_status/
check_secondary_locks/resolve_lock) are faithful at the body level,
including the PessimisticLock ForceLock result padding and the
CheckTxnStatus lock-ttl extraction. The Go arms absent here --
`KvCleanup`, `KvScanLock`, `KvGC`, `KvDeleteRange`, the Raw* family,
`KvFlush`/`KvImport`/`KvBufferBatchGet`, `RawGetKeyTTL`'s empty stub -- have
NO dispatching caller in the Rust client surface (grep-verified across
`tidb-txnkv`'s rpc/unary/batch modules): absent, not broken.

FIXED: `NeedCommitTs` was ignored. The Rust client SENDS
`need_commit_ts: true` on its snapshot batch-gets (Go's client does the
same), but the mock answered no commit ts. `GetPair`/`BatchGet`
(`mvcc.go:1826-1866`) under `requestCtx.returnCommitTS` answer each pair's
commit ts AND disable the committed-lock shortcut -- Go nils
`committedLocks` "to make sure all KvPair has CommitTS", so a lock only the
shortcut knew about reports as locked instead of answering a value whose
commit ts cannot be known. `get_with_commit_ts`/
`batch_get_with_commit_ts` port both halves; `KvGet`/`KvBatchGet` fill the
wire `commit_ts` fields. Three regressions: commit ts answered only when
requested, the shortcut disabled under the flag (locked, Go's error), and
batch-get pairs carrying the ts.

## mvcc.go full-coverage closure (2026-09-07, same session)

The remaining unaudited range (`mvcc.go:1421-1817`) closes the file:

- Lock-check primitives verified exact: `checkLock` (resolved -> absent;
  visible+write-lock+not-primary-get gates; `maxSystemTS` is literally
  `math.MaxUint64` on both sides; committed-set -> LockPair; else
  `BuildLockErr`), `checkLockForRcCheckTS` (any unresolved write lock is an
  `RcCheckTs` conflict carrying the key, `ConflictCommitTS` unset/0), and
  `inTSSet` = `slices.Contains` == Rust slice `contains`.
  `value_from_lock` (Op_Put only) equal.
- Unreachable-from-client arms dispositioned (grep-verified no dispatching
  caller in the Rust client surface): `Cleanup`, `ScanLock`,
  `appendScannedLock`, `scanPessimisticLocks`, `PhysicalScanLock`,
  `ReadBufferFromLock` (BufferBatchGet), `UpdateSafePoint` (GC),
  `DeleteFileInRange`, `StartDeadlockDetection` (the client-side waiter
  machinery this port intentionally lacks). `MvccGetByKey`/`MvccGetRange`/
  `MvccGetByStartTs` debug face exists in the store with transcreated tests;
  no client dispatch, matching the tier.

With the optimistic suite, the pessimistic suite, the read paths, and the
lock primitives dispositioned, `mvcc.go` (2,182 lines) has no unaudited
function remaining.

## Coprocessor arithmetic family (2026-09-07, same session)

The trimmed tipb enum already carried the decimal arithmetic and integer
MOD signatures; the evaluator refused them. Landed in the flat lowering
with Go's `pkg/expression` semantics:

- `PlusDecimal`/`MinusDecimal`/`MultiplyDecimal` (`EvalPlusDecimal`
  family): exact decimal arithmetic through the Go-aligned `*_mysql`
  decimal ops, NULL in -> NULL out; composes as a comparison operand
  (`GetAccurateCmpType` makes decimal arithmetic a natural child of the
  decimal comparisons) and answers its own truth as a bare condition.
- `ModDecimal` / `ModInt{UnsignedUnsigned,UnsignedSigned,SignedUnsigned,
  SignedSigned}`: NULL on a zero divisor (MySQL), truncated remainder with
  the sign of the dividend otherwise. Go picks between the four integer
  signatures by the arguments' UNSIGNED flags -- the truncated VALUE is
  identical under all four, so the i128 evaluator serves them with one
  arm. Go's `MinInt % -1` panic is unreachable with operands riding i128
  (documented hardening, same class as `splitOwnerValues`).
- `ModReal` stays in the refusal arm: the evaluator has no Real
  comparison family for it to compose with yet.

Regressions: `integer_mod_follows_mysql` (-7 % 2 = -1; zero divisor NULL)
and `decimal_arithmetic_composes_into_conditions` (c + (-1) > 0 as a
selection condition).

## Coprocessor REAL family (2026-09-07, same session)

The trimmed tipb enum gains the six REAL comparisons and the four REAL
arithmetic signatures at their upstream contract ids (`expression.proto` @
the pinned tipb: LTReal 101 .. NEReal 151; PlusReal 200, MinusReal 204,
MultiplyReal 208, DivideReal 211), and the evaluator lands them:

- `LTReal`/`LEReal`/`GTReal`/`GEReal`/`EQReal`/`NEReal`: binary64
  comparison with total ordering (`f64::total_cmp`), NULL in -> NULL out.
- `PlusReal`/`MinusReal`/`MultiplyReal`: binary64 arithmetic, NULL in ->
  NULL out; `DivideReal`: NULL on a zero divisor (`EvalDivideReal`).
  Both compose recursively as operands (`x / 2 + 1`) and a bare
  arithmetic answers its own truth (`ToBool`: non-zero and not-NaN).
- `SimpleExpr` gains a `Real(f64)` leaf: `ExprType_Float64` constants
  decode through Go `convertFloat`'s `codec.DecodeFloat` -- eight
  big-endian IEEE-754 bits -- and `eval_datum` maps the leaf so aggregate
  argument leaves stay exhaustive.

Regressions: `real_comparisons_and_arithmetic_follow_mysql` (GTReal truth,
composed `x / 2 + 1`, zero-divisor NULL) and
`a_float64_literal_decodes_go_convert_float` (wire bits -> leaf).

## Coprocessor integer DIV family (2026-09-07, same session)

The trimmed tipb enum gains the integer DIV signatures at their upstream
contract ids (`IntDivideInt` 213, `IntDivideDecimal` 214,
`IntDivideIntUnsigned{Unsigned,Signed,SignedUnsigned}` 234/235/237,
`IntDivideIntSignedSigned` 236), and the evaluator lands them:

- `IntDivideInt` + the three unsigned-flag pairings: truncated division
  with the dividend's sign, NULL on a zero divisor. Go picks between the
  four by the arguments' UNSIGNED flags; the quotient VALUE is identical
  under all four for the values those flags admit, so one i128 arm
  serves. Go's `MinInt / -1` panic is unreachable at this width
  (documented hardening).
- `IntDivideDecimal`: `DecimalDiv` then `ToInt` -- the quotient truncated
  to the integer part, NULL on a zero divisor. A quotient wider than
  BIGINT errors in Go where this seam answers NULL (`div_rem` folds both)
  -- a narrowing, not a value change inside BIGINT. The wire operands are
  DECIMAL by the time the sig runs (Go casts at build), which the
  regression exercises.

Regression: `integer_division_follows_mysql` (-7 DIV 2 = -3; zero divisor
NULL; -9.5 DIV 2 = -4).

## Coprocessor integer arithmetic family + error channel (2026-09-07)

`eval_expr` now carries Go's expression-level error: `Result<Option<i128>,
String>`. This is what makes the integer arithmetic family portable --
`types.AddInt`/`SubInt`/`MulInt`/`AddUint`/`SubUint`/`MulUint` answer a
request-level 1690 overflow terror (`BIGINT [UNSIGNED] value is out of range
in 'ADD'|'SUBTRACT'|'MULTIPLY'`) rather than a NULL, and the selection loops
convert that error into the response's error channel instead of silently
filtering the row.

- Landed: `PlusInt` 203, `MinusInt` 207, `MultiplyInt` 210,
  `MultiplyIntUnsigned` 218, the `PlusIntUnsigned*` 219-222 pairings, the
  `MinusIntUnsigned*` 227-230 pairings, and the `MinusIntForced*` 231-233
  pairings, at their upstream contract ids.
- Result domain per signature: BIGINT for the plain signed sigs, BIGINT
  UNSIGNED for every flag-pairing and forced form. The mixed/forced
  unsigned pairings additionally reject a NEGATIVE operand immediately:
  Go converts it through uint64, which wraps into an immediate overflow
  regardless of the in-range difference a naive evaluation would answer.
- Regressions: `integer_arithmetic_overflow_answers_gos_1690_text` and
  `unsigned_minus_rejects_negative_operand_like_go`.

## Coprocessor IS NULL + LIKE families (2026-09-07, same session)

- `DecimalIsNull`/`DurationIsNull`/`RealIsNull`/`StringIsNull`/
  `TimeIsNull` (upstream 3111-3115, already in the trimmed enum) land
  beside `IntIsNull`: each inspects its own leaf's DATUM for `Null` --
  deliberately not the int truth channel, where a present non-NULL
  non-integer datum would masquerade as NULL. A leaf whose cast is
  refused propagates that error.
- `LikeSig` 4310 lands as `SimpleSig::Like(collation)` over
  (target, pattern, escape): the wildcard state machine is
  `tidb_datatype::like_matches` (now public -- it already served the JSON
  path), `%`/`_`/escape semantics per MySQL. Case handling follows the
  comparison's collation: `_ci` folds both sides before the match, `_bin`
  is exact. Narrowing, documented at the arm: Go folds through the
  collator's per-collation weight tables; this fold is
  character-lowercase, exact for the ASCII families that dominate pushed
  patterns.

Regressions: `like_follows_collation_case_sensitivity` (general_ci vs bin
on the same bytes, plus the escape quoting a literal `%`) and
`is_null_family_checks_its_own_leaf` (present REAL answers FALSE, absent
answers TRUE).

## Coprocessor JSON MEMBER OF (2026-09-07, same session)

`JsonMemberOfSig` 5029 lands, the one JSON signature that is
condition-shaped. `SimpleExpr` gains a `Json(BinaryJSON)` leaf:
`ExprType_MysqlJson` constants decode through `codec.DecodeOne` exactly as
Go `convertJSON` does. The target operand coerces through the
`CreateBinaryJSON` subset over scanned datums (int/uint/real/string/bytes/
json); a NULL datum answers NULL rather than a JSON `null` document. The
answer follows `builtinJSONMemberOfSig.evalInt`: whole-document EQUALITY
against a non-array doc, equality against any ARRAY element otherwise
(structural `JSON_CONTAINS` is a different function and is not substituted).

`JsonReplaceSig`/`JsonArrayAppendSig`/`JsonMergePatchSig` stay refused by
name: they are projection VALUE functions and this lowering has no
projection executor yet, so their arms would be unreachable decoration
(`merge_patch_binary_json` and friends already sit in the datatype crate
for the projection course).

Regression: `json_member_of_follows_go_equality_rules` -- array hit, miss
answering FALSE, non-array equality, NULL target answering NULL.

## Coprocessor numeric CAST family (2026-09-07, same session)

The six numeric cast signatures the trimmed enum already carried land with
Go `builtinCast*` semantics:

- `CastIntAsInt`: identity over the int channel.
- `CastRealAsInt`/`CastDecimalAsInt` (`AS SIGNED`): `ConvertFloatToInt`
  ROUNDS to nearest first (half away from zero -- `CAST(2.5)` is 3), then
  the range check answers the cast overflow error
  ("constant %v overflows bigint", Go `%v` float rendering reproduced by
  `go_float_display`); `CastDecimalAsInt` truncates toward zero and maps
  `Decimal.ToInt`'s overflow warning to the same error.
- `CastIntAsReal`/`CastRealAsReal`/`CastDecimalAsReal`: widening through
  `eval_real`'s composition arms, so a widened column feeds REAL
  comparisons and arithmetic as an operand; a bare cast answers its own
  `ToBool` truth.
- The INT-source recursion inside `eval_real` flattens the int channel's
  (impossible-there) error with `.ok()` -- documented at the site.

Regressions: `real_cast_rounds_then_checks_range` (2.5 -> 3, -2.5 -> -3,
9.3e18 overflow text) and `real_cast_composes_as_a_comparison_operand`
(widened column under `GTReal`).
