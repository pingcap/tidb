# Coprocessor request/response parity: Rust vs `pkg/distsql` + `pkg/store/copr` + `ToPB`

What we push to TiKV is executed *by TiKV*. A divergence here returns rows, not
an error, and this tier's `EXPLAIN` never prints `cop[tikv]`, so a wrong request
is invisible from the plan text. That is what this audit is for.

**Nothing in this document was executed.** This machine cannot run a freshly
built binary (`syspolicyd` wedge: every new executable hangs in `_dyld_start`),
so no `cargo test`, no `gorun`, no `goeval`, no TiKV round trip. Every claim
below is read from source on both sides, with file:line on each. The
"unverified" section at the end is explicit about what that leaves open.

Go tree: `/Users/qiliu/projects/tidb` on `hparser-integration`.
Rust tree: `rust/` in the same repo.
`tipb` pin: `github.com/pingcap/tipb v0.0.0-20260623093813-5f9928e91afe`
(`go.mod:108`).

## Counts

| Rank | Meaning | Count |
| --- | --- | --- |
| 1 | TiKV computes a different result (or errors where Go succeeds) | **1** |
| 2 | Correctness-relevant request flag dropped | **1** (one call-site, 12 fields) |
| 3 | Lock / region error / warning channel mishandled | **1** |
| 4 | Pushdown or encoding we decline that Go performs (performance only) | **3** |
| — | Areas checked and found equal | **11** |

---

## Status

Items **1.1** and **3.1** below were FIXED TOGETHER, for the reason stated in
3.1: either one alone makes the observable behavior worse. `DAGRequest.flags`
now comes from `StmtContext::push_down_flags()` (the statement's own class and
levels, feeding the `statement_pushdown` port), and the collector the response
channel fills is the statement's own sink, drained through
`StmtContext::take_warnings` into the session buffer `SHOW WARNINGS` reads and
the count `wire_warning_count` publishes.

One half remains DEFERRED: the read-only tier (`real_tikv_read.rs`) sends the
computed SELECT flags, but no driver of that tier reads its warning sink yet —
that tier has no `SHOW WARNINGS` surface at all. The cluster-storage path
(`cop_scan.rs`), which is the one that reaches a session, is complete.

The `ROUND(s) = 12` case over `s = '12abc'` remains a LIVE check: only a real
region can confirm it now warns rather than failing.

---

## Rank 1 — TiKV computes a different result

### 1.1 `DAGRequest.flags` is hardcoded to `0` on both live paths

* Go: `pkg/executor/internal/builder/builder_utils.go:72`
  — `dagReq.Flags = sc.PushDownFlags()`, unconditionally, for every DAG.
* Rust: `rust/crates/tidb-exec/src/real_tikv_read.rs:1384-1390` and
  `rust/crates/tidb-exec/src/cop_scan.rs:259-264` — both construct
  `DagRequestContext::new(tz_name, tz_offset, 0, EncodeType::Default)`. The
  third argument is `push_down_flags`, and it is the literal `0`.

The port itself is correct and complete:
`rust/crates/tidb-exec/src/statement_pushdown.rs:85-125` reproduces
`PushDownFlagsWithTypeFlagsAndErrLevels` and `PushDownFlags` bit for bit,
including the `FLAG_TRUNCATE_AS_WARNING | FLAG_OVERFLOW_AS_WARNING` pairing and
the `IGNORE_TRUNCATE`-wins precedence. It has **no production caller**. The
constant `0` is passed instead. The crate's own tests pass `32`
(`rust/crates/tidb-exec/tests/tikv_scan_dag_lowering_source.rs:67`), so the
tests do not see the value production sends.

**What Go sends for a plain `SELECT`.** `pkg/executor/select.go:1101` sets
`errLevels[ErrGroupDividedByZero] = LevelWarn` before the statement switch;
`pkg/executor/select.go:1159-1166` (the `*ast.SelectStmt` arm) sets
`WithTruncateAsWarning(true)` and `WithIgnoreZeroInDate(true)` and
`sc.InSelectStmt = true`. Through
`pkg/sessionctx/stmtctx/stmtctx.go:1252-1289` and the bit values in
`pkg/meta/model/flags.go:21-43` that is:

```
FlagTruncateAsWarning       1<<1 =   2
FlagInSelectStmt            1<<5 =  32
FlagOverflowAsWarning       1<<6 =  64
FlagIgnoreZeroInDate        1<<7 = 128
FlagDividedByZeroAsWarning  1<<8 = 256
                                  ----
                                   482
```

TiDB sends `flags = 482`. We send `flags = 0`.

**Distinguishing case.** `ROUND` over a string column is in our pushdown
catalog (`rust/crates/tidb-expr/src/pushdown_catalog.rs:413-434`, and the
`RoundReal` row is reached for a `VarString` argument — pinned by
`round_keeps_its_argument_domain_and_refuses_the_frac_overload` at
`pushdown_catalog.rs:1208-1213`), with an implicit `CastStringAsReal` wrapper
inserted by `coerced_to_pb` (`pushdown_catalog.rs:943-989`, cast table at
`:708-725`).

```sql
CREATE TABLE t (s VARCHAR(32));
INSERT INTO t VALUES ('12abc');
SELECT * FROM t WHERE ROUND(s) = 12;
```

Both sides push `RoundReal(CastStringAsReal(ColumnRef#0))`. The *only* channel
telling TiKV what to do with the truncation inside `CastStringAsReal` is
`DAGRequest.flags`.

* TiDB sends `482`. `FlagTruncateAsWarning` is set, so TiKV degrades the
  truncation to a warning, casts to `12.0`, and the row comes back.
* We send `0`. Neither `FlagIgnoreTruncate` nor `FlagTruncateAsWarning` is set,
  which is the strictest configuration TiKV has for that field, so the region
  raises the truncation instead of degrading it.

Same shape for overflow (`POW`, `pushdown_catalog.rs:508-522`) via
`FlagOverflowAsWarning`, for divide-by-zero (`MOD`,
`pushdown_catalog.rs:345-406`) via `FlagDividedByZeroAsWarning`, and for
zero-dates via `FlagIgnoreZeroInDate`. Every one of them is a query that
succeeds against a real TiDB and does not succeed here, decided entirely by a
`u64` we never populate.

**Fix size.** Not small. It needs the statement's `ConversionFlags` and
`LevelMap` threaded from the session into `RealTiKvReadSession` /
`cop_scan`'s scanner — neither struct carries them today (both carry only
`time_zone_name` / `time_zone_offset_secs`, e.g. `real_tikv_read.rs:1116`). The
computation is already written and tested; only the plumbing is missing. Not
attempted here, because a wrong flag word returns rows rather than failing and
I cannot send one request to check.

---

## Rank 2 — a correctness-relevant flag dropped

### 2.1 `SetFromSessionVars` has no production caller

* Go: `pkg/distsql/request_builder.go:339-379` — every DAG request goes through
  `SetFromSessionVars`, which sets isolation level, priority, `NotFillCache`,
  `TaskID`, `ReplicaRead`, the resource-group tagger, paging, request source,
  store batch size, resource-group name, store-busy threshold, client read
  timeout, `MaxExecutionTime`, and `MaxKeysRead`.
* Rust: `rust/crates/tidb-distsql/src/request_builder.rs:306-311`
  (`set_from_context`) is the faithful port — the weak-consistency-beats-RC-check
  precedence, the RC-check-forces-leader-read rule
  (`rust/crates/tidb-distsql/src/request.rs:132-145`), and the three-value
  priority map (`request.rs:163-169`) are all correct. **Grep finds no caller
  outside the crate's own tests.**
* Both live builders call bare `RequestBuilder::new()` and set four things:
  `real_tikv_read.rs:1395-1400` and `cop_scan.rs:404-409` set only `start_ts`,
  `keep_order`, key ranges, and the DAG payload.

So `KvRequestMetadata` stays at its defaults, and
`build_tikv_unary_request_inner`
(`rust/crates/tidb-distsql/src/cop_paging/tikv_rpc_contract.rs:142-189`)
encodes those defaults into `kvrpcpb.Context`.

**Distinguishing case.**

```sql
SET @@tidb_replica_read = 'follower';
SET @@tidb_read_consistency = 'weak';
SELECT LOW_PRIORITY SQL_NO_CACHE * FROM t WHERE id > 100;
```

| `kvrpcpb.Context` field | TiDB | here |
| --- | --- | --- |
| `isolation_level` | `RC` (1) | `SI` (0) |
| `replica_read` | `true`, type Follower | `false`, Leader |
| `priority` | `Low` (1) | `Normal` (0) |
| `not_fill_cache` | `true` | `false` |
| `resource_group_tag` | SQL+plan digest | empty |
| `resource_control_context.resource_group_name` | session's group | `""` |
| `task_id` | statement task id | `0` |
| `busy_threshold_ms` | session threshold | `0` |

The enum numbering itself is right — `IsolationLevel` and `Priority`
(`rust/crates/tidb-txnkv/src/kv_contract.rs:131-177`) use `0/1/2` in the same
order as `kv.IsoLevel` / `kv.Priority` and as `kvrpcpb`, so Go's
`isolationLevelToPB` / `priorityToPB` identity mapping and our direct
`.raw() as i32` cast agree. Only the *value* never arrives.

Ranked 2 and not 1 because every one of these divergences lands on the
conservative side of the read: `SI` at an explicit `start_ts` is stronger than
`RC`, leader-read is stronger than follower-read, and `Normal` priority is not
wrong, only unfair. The genuinely lost behaviours are `resource_group_name`
(resource control is not applied at all), `resource_group_tag` (Top SQL and
runaway-query detection are blind to every read this tier issues), and
`MaxExecutionTime` (no KV-side deadline).

---

## Rank 3 — a channel mishandled

### 3.1 Warnings TiKV reports never reach the session

* Go: `pkg/distsql/select_result.go:464-466` —
  `for _, warning := range r.selectResp.Warnings { r.ctx.AppendWarning(dbterror.ClassTiKV.Synthesize(...)) }`.
  `r.ctx` is the session's `DistSQLContext`, so those land in `SHOW WARNINGS`.
* Rust: `rust/crates/tidb-distsql/src/response_channel.rs:793-798` does exactly
  the right thing — `self.warnings.append_tikv_warning(code, msg)`, in the right
  place in the sequence (after the `SelectResponse.Error` check at `:781` and
  the intermediate-output count check at `:787`, which is Go's order at
  `select_result.go:448-465`).
* But every production construction of that collector is a fresh, unshared one:
  `real_tikv_read.rs:1359`, `real_tikv_read.rs:1413`, `cop_scan.rs:418` all pass
  `WarningCollector::new()`. `WarningCollector` is `Arc<Mutex<Vec<Warning>>>`
  (`warning.rs:69-72`) and is shared only by cloning; a fresh one is a private
  sink. `TableIndexReader::with_warnings`
  (`rust/crates/tidb-exec/src/storage_reader/table_index_reader.rs:90`) is the
  one seam that would let a session's collector in, and nothing calls it.

**Distinguishing case.** Any query where TiKV degrades an error to a warning —
i.e. exactly the queries item 1.1 is about, once flags are fixed. With
`flags = 482` and `SELECT * FROM t WHERE ROUND(s) = 12` over `s = '12abc'`,
TiDB answers `SHOW WARNINGS` with `Warning | 1292 | Truncated incorrect
DOUBLE value: '12abc'`. Here the warning is appended to a `WarningCollector`
that is dropped when the statement ends, and `SHOW WARNINGS` is empty. The row
count is the same; the diagnostic is silently gone.

Note the interaction: 1.1 and 3.1 currently mask each other. Fixing flags
without fixing the warning sink turns "query fails" into "query silently
returns a truncated value with no warning" — which is worse. **They must be
fixed together.**

Region errors and locks, by contrast, are handled properly. Response error
precedence in `decode_tikv_unary_response`
(`tikv_rpc_contract.rs:221-239`) is region → lock → other → batch → success,
matching Go's unary path. Region errors go through a real disposition machine
(`direct_unary_query_transport.rs:1376-1387`, `:1556-1686`) with
`RetrySelector` / `RetryRoute` / `RebuildRanges` / `ReturnRegionError` /
`Terminal`, including the stale-read `data_is_not_ready` and `store_not_match`
special cases. Locks reach an optimistic resolver with TTL wait and deadline
budget (`cop_paging/lock_recovery.rs:52-82`) that ends in `RetrySameTask`.

---

## Rank 4 — performance only

### 4.1 `encode_type` is hardcoded to `TypeDefault`

Go `pkg/executor/internal/builder/builder_utils.go:85` calls
`distsql.SetEncodeType`, which (`pkg/distsql/distsql.go`, `SetEncodeType` /
`canUseChunkRPC`) picks `TypeChunk` plus a `ChunkMemoryLayout` whenever
`EnableChunkRPC` (default on) and the alignment check passes. Both Rust
production sites pass `EncodeType::Default`
(`real_tikv_read.rs:1388`, `cop_scan.rs:262`). The lowering itself handles both
(`dag_request.rs:329-343`, including the endian probe), and the *decoder*
handles both correctly, so this costs bytes on the wire, not answers.

### 4.2 `collect_execution_summaries` is never set

Go sets it when `sc.RuntimeStatsColl != nil`
(`builder_utils.go:68-71`). `DagRequestContext::new`
(`dag_request.rs:66-82`) leaves it `false` and no caller overrides it, so
`EXPLAIN ANALYZE` gets no per-executor coprocessor summaries.

### 4.3 Pushdown breadth

Our catalog (`pushdown_catalog.rs:341-618`) is a deliberate subset of
`scalarExprSupportedByTiKV` (`pkg/expression/infer_pushdown.go:186+`): `mod`,
`round`, the trig/`pi`/`pow` family, `conv`, `char_length`, `upper`, `lower`,
`substr`/`substring`/`mid` — plus the comparison / `IS NULL` / `IN` / `OR` /
`NOT` set in `pb_predicate.rs`. Go's TiKV list is much longer (`like`,
`nulleq`, `is_truth*`, the bit ops, `plus`/`minus`/`mul`/`div`/`intdiv`/`abs`,
the date family, `concat`, `if`/`ifnull`/`case`, `json_*`, …). Every absence is
a refusal, not a wrong push, so this is only slower. It also refuses in the
*right direction* on the two families where a wrong push would be a wrong
answer: `ENUM`/`SET` leaves (which need `elems` on the wire) and `BIT`/`JSON`
(which Go gates behind `IsPushDownEnabled`) are refused at
`pushdown_catalog.rs:1061-1085` and `pb_predicate.rs:104-108` rather than
guessed.

The one direction that would be a wrong answer — pushing something TiKV
evaluates differently — I found **no instance of**. See below.

---

## Verified equal

### The signature-numbering verdict

**Clean.** All 52 `ScalarFuncSig` constants declared in
`rust/crates/tidb-proto/proto/select.proto:93-168` carry the same integer as
upstream `tipb`. Checked mechanically against the `ScalarFuncSig_value` map in
`go-tipb/expression.pb.go` of the pinned module (640 upstream entries; zero
mismatches, zero names absent upstream).

That is the numbering. The *selection* — which signature we name for a given
function and argument types — was checked function by function against Go's own
`getFunction` switches, and also matches:

| Family | Go | Rust |
| --- | --- | --- |
| six comparisons, `ETInt` | `generateCmpSigs` `ETInt` arm | `pb_predicate.rs:418-429` |
| six comparisons, `ETString` | `generateCmpSigs` `ETString` arm | `pb_predicate.rs:344-358` |
| `MOD` | `arithmeticModFunctionClass.getFunction`: Real > Decimal > Int, then a 4-way split on the two `UNSIGNED` flags | `pushdown_catalog.rs:345-406` (all four `ModInt*` orderings, `Unsigned`/`Signed` in Go's argument order) |
| `ROUND` | `Int`/`Dec`/`Real` by argument eval type; the `frac` overload excluded | `pushdown_catalog.rs:413-434`, 2-arg refused |
| `ATAN` | `Atan1Arg` / `Atan2Args` by arity | `pushdown_catalog.rs:455-477` |
| `UPPER`/`LOWER`/`CHAR_LENGTH` | `types.IsBinaryStr(arg)` picks the non-UTF8 spelling | `pushdown_catalog.rs:542-578` |
| `SUBSTR`/`SUBSTRING`/`MID` | 4-way on (arity, `IsBinaryStr`) | `pushdown_catalog.rs:587-598` |
| `CONV` | single signature, reads bytes either way | `pushdown_catalog.rs:604-612` |
| implicit casts | `newBaseBuiltinFuncWithTp` inserts `Cast<From>As<To>` | `pushdown_catalog.rs:708-725` |
| `IS NOT NULL`, `NOT IN` | rewriter spells them `UnaryNot(positive)` | `pb_predicate.rs:36-41`, no invented `IsNotNull` |

Two subtleties I specifically went looking for and found correct:

* A string comparison's collation lives on the **comparison node's own**
  `FieldType`, not the operands' — Go's `scalarFuncToPBExpr` does
  `tp := *expr.RetType; tp.SetCollate(str1)`. `pb_predicate.rs:227-254` writes
  the derived collation onto a `BIGINT(1)` return type. Getting this wrong is a
  silently wrong answer computed at the region; it is right.
* Collation crosses the wire as a **negative** id.
  `pkg/util/misc.go:342` (`ColumnInfo`) and `pkg/util/collate/collate.go:368`
  (`FieldType`) both route through `RewriteNewCollationIDIfNeeded`. Ported at
  `rust/crates/tidb-datatype/src/collation.rs:148-155` and used by
  `pushdown_catalog.rs:1046` and `pb_predicate.rs:247,303,325`.

### Also checked and equal

1. **Handle-range encoding, including the degenerate one-key range.**
   Go `pkg/distsql/request_builder.go:549-559` (`encodeHandleKey`) and
   `:533-548` (`tableRangesToKVRangesWithoutSplit`); Rust
   `rust/crates/tidb-distsql/src/signed_handle_range.rs:110-133`. Both:
   `EncodeInt` the bound, `PrefixNext` the low iff `LowExclude`, `PrefixNext`
   the high iff **not** `HighExclude`, then `EncodeRowKey(tid, ·)`. So
   `WHERE id = 5` (low=high=5, both inclusive) becomes
   `[t{tid}_r<enc 5>, t{tid}_r<enc 5>+1)` on both sides — a half-open interval
   over exactly one key, byte for byte.
2. **`coprocessor.Request` envelope field numbers.**
   `rust/crates/tidb-distsql/src/coprocessor_request.rs:34-144` carries
   `context`(1) `tp`(2) `data`(3) `ranges`(4) `is_cache_enabled`(5)
   `cache_if_match_version`(6) `start_ts`(7) `schema_ver`(8)
   `is_trace_enabled`(9) `paging_size`(10) `connection_id`(12)
   `connection_alias`(13) `max_keys_read`(16) `paging_size_bytes`(17), matching
   `pkg/store/copr/coprocessor.go`'s construction.
3. **`request_source` string synthesis.**
   `tikv_rpc_contract.rs:241-262` reproduces the `unknown` /
   `{origin}_{type}[_{explicit}]` shape, including suppressing the explicit
   suffix when it equals the source type.
4. **`request_origin` = TiDB.** `tikv_rpc_contract.rs:185` encodes the value
   client-go's process default fills in, rather than leaking the pre-fill zero.
   Correct and non-obvious.
5. **DAG executor chaining.** `dag_request.rs:316-328` builds
   `[scan] → Selection? → Limit?` in that order, which is
   `ConstructListBasedDistExec`'s order.
6. **`executor_id` presence.** Go takes the address of an empty string for
   `TableScan` (`physical_table_scan.go:822`), `Selection`
   (`physical_selection.go:183`) and `Limit` (`physical_limit.go:185`), but
   `PhysicalIndexScan.ToPB` (`physical_index_scan.go:576`) returns an
   `Executor` with **no** `ExecutorId` at all. Rust matches exactly:
   `Some(String::new())` at `dag_request.rs:401,474`, `None` at `:497`. That is
   a one-byte wire difference that was got right.
7. **Pushed `Limit` is `offset + count`.** Go `pkg/planner/core/task.go:630`
   etc. build the cop-side `PhysicalLimit` with `Count: p.Offset + p.Count`;
   `dag_request.rs:182-199` documents and implements the same.
8. **`IndexScan.unique`.** Go `checkCoverIndex` (single range, unique index,
   full-length point) vs `check_cover_index` at `dag_request.rs:508-512`.
9. **`ColumnRef` payload is the schema *offset*, `codec.EncodeInt`-encoded** —
   not the column id. Go `expr_to_pb.go` `columnToPBExpr`'s DAG-basic branch:
   `codec.EncodeInt(nil, int64(column.Index))`. Rust
   `pushdown_catalog.rs:903` / `pb_predicate.rs:456`, both via
   `tidb_codec::encode_int`.
10. **Literal `FieldType`s.** Go `types.DefaultTypeForValue`
    (`pkg/types/field_type.go`) adds `NotNullFlag` for any non-nil value, sets
    `TypeLonglong` with `flen = StrLenOfInt64Fast(x)`, `decimal = 0`, binary
    charset/collation for `int64`; and `TypeVarString`,
    `flen = len(x)` **bytes**, `decimal = UnspecifiedLength`, no binary flag,
    for `string`. Rust matches both:
    `pb_predicate.rs:458-467` and `pb_predicate.rs:314-331`.
11. **Response decode dispatch on `encode_type`.** Go
    `select_result.go` `Next` switches on `r.selectResp.GetEncodeType()` and
    returns `"unsupported encode type"` for anything but
    `TypeDefault`/`TypeChunk`. Rust `response_channel.rs:839-882` reads the
    encode type off the *response*, defaults an absent field to `TypeDefault`,
    errors on an out-of-range integer, and errors explicitly on `TypeCHBlock`.
    Same behaviour, including the refusal.

### Noted, inert, not fixed

`rust/crates/tidb-expr/src/pb_predicate.rs:54` hardcodes
`BINARY_COLLATION_PROTO_ID = -63` in `int_field_type`, where the same file's
string path correctly calls `collation_to_proto`. When new collations are
*disabled*, Go's `CollationToProto("binary")` returns `+63` and we would still
send `-63`. It only ever lands on integer and boolean nodes, whose collation
TiKV does not consult, and new collations are on by default. Left alone
deliberately: it is a wire-byte change I cannot test, and its payoff in the
default configuration is zero.

---

## What is unverified because nothing can execute here

* **No request was ever sent.** Every "TiKV would do X" claim is inferred from
  the flag/signature contract, not observed. In particular I did not confirm
  against a running TiKV that `flags = 0` makes the `ROUND(s)` case fail rather
  than merely warn — only that `0` selects the strictest branch of the contract
  and `482` does not.
* **No test was run.** Not `cargo test`, not `cargo nextest`, not a Go test
  binary, not `gorun`/`goeval`. Existing Rust tests that pin the current
  behaviour (there are several under `crates/tidb-distsql/tests/` and
  `crates/tidb-exec/tests/`) were read, not executed.
* **The signature-number check is the one mechanical result here**, and it is
  a pure text comparison between our `.proto` and the pinned module's generated
  `ScalarFuncSig_value` map — no compilation or execution involved, so the
  `syspolicyd` wedge does not affect it.
* Ranks 1–3 are each a *missing call*, which `cargo check` cannot see. They
  would be caught instantly by one live query against a real cluster and by no
  amount of local static checking.
