# TiDB to Rust rewrite handoff

_Current operating handoff. Updated 2026-07-20. This records the active
frontier and verified receipts; generated ledger counters remain in
[`STATUS.md`](STATUS.md)._

## Standing goal and completion bar

Rewrite TiDB's SQL layer in Rust without dropping any behavior or original
test obligation. The target is a standalone Rust SQL process using the real
MySQL, PD, TiKV, kvproto, and client-go contracts. Go may act as an independent
oracle over network protocols; it is not an in-process backend.

## Transcreation discipline (user directive, 2026-07-21)

TiDB's Go implementation is the source of truth. When transcreating a Go
function/module, port the WHOLE implementation (every branch/type case) AND its
Go tests — never a partial subset that looks complete. If a branch's dependency
is not ported yet, make that branch FAIL CLOSED with an explicit error and name
it in the risk register below; never a silent partial.

### Partial-transcreation risk register (open debt, surfaced not hidden)

These are places currently ported as bounded subsets of a larger Go function.
Each is fail-closed (rejects the unsupported case), but each is incomplete
against its Go source and must be finished:

- `column.DumpBinaryRow` (`pkg/server/internal/column/column.go`): numeric +
  string cases DONE (2026-07-21). All fixed-width integers
  (`Tiny/Short/Year/Int24/Long/Longlong` via `dump.Uint16/32/64`), `Float`/
  `Double` (IEEE-754 bit dump), and the string group are ported faithfully with
  per-width byte tests; `INT` now reads back as a true `LONG` (4-byte), gap
  closed. Still fail-closed in the stream: temporal
  `Date/Datetime/Timestamp` and `Duration` (the encoders
  `dump.BinaryDateTime`/`dump.BinaryTime` are now ported + tested but unwired —
  see below), `Enum/Set/JSON/TiDBVectorFloat32` (all need the string `EncodeData`
  charset re-encode, still unported). `Float`/`Double` are DONE end to end
  (2026-07-21): the encoder cells, the stream admission
  (`is_binary_float_result_type`), and the connection dispatch (`Datum::Real` ->
  `Float`/`Double` by column type) are wired and mutation-verified.
  `NewDecimal` is DONE at cell+stream+dispatch (2026-07-21): `BinaryResultCell::
  NewDecimal(Decimal)` length-encodes `MyDecimal.String()` with NO `EncodeData`
  (Go dumps a decimal as ASCII directly — the one string-ish case that skips the
  charset re-encode), `is_binary_decimal_result_type` admits `TYPE_NEW_DECIMAL`,
  and `Datum::Decimal` dispatches in `connection_resultset`; byte-tested against
  Go `MyDecimal.String()` vectors + mutation-verified. Like `Float`/`Double`, it
  is not yet reachable end to end because the configured catalog/read path does
  not produce a `DECIMAL` column yet (catalog type-breadth item below).
  CORRECTION: a prior handoff note here claimed `tidb_datatype::Datum` has no
  float variant — that was WRONG. `Datum::Real(f64)` exists (with `new_real`),
  and `decode_column_datums` already produces it for `Float`/`Double` columns.
  The false entry is removed; do not re-add it.
  - `dump.BinaryTime` DONE (2026-07-21): `encode_binary_time(nanoseconds)` in
    `tidb-protocol`, ported whole with the `dump_test.go` `TestDumpBinaryTime`
    vectors + the 8-byte no-micros branch, mutation-verified. Not wired yet (no
    `Duration` result column flows through), but complete + tested.
  - `dump.BinaryDateTime` DONE (2026-07-21): `encode_binary_datetime(PackedTime,
    BinaryDateTimeType)` in `tidb-protocol`, ported whole — every branch of Go's
    `t.Type()` switch (zero -> `[0]`; date-only -> length 4; HH:MM:SS -> length 7;
    microseconds -> length 11; DATE discards all time bits). No separate rich
    `Time` type was needed: `PackedTime::parts()` already exposes
    year/month/day/hour/minute/second/microsecond, and `PackedTime::is_zero()`
    matches Go `Time.IsZero()` (both are the all-zero packed value, which
    `ToPackedUint` produces iff every field is zero). Verified against byte
    vectors generated from Go `dump.BinaryDateTime` itself (throwaway fixturegen,
    not committed), and mutation-tested on the year byte-order and the
    DATE-ignores-time branch. Not wired yet (no temporal `BinaryResultCell` /
    `Datum` time value flows through the stream, which still fail-closes temporal
    result columns) — complete + tested at the encoder layer, exactly like
    `encode_binary_time`.
- Prepared param decoder (`decode_prepared_statement_execute`): signed
  `LONGLONG` only, vs Go `parseBinaryParams` (`pkg/server/conn_stmt_params.go`),
  which handles all binary param types incl. strings, NULL, unsigned, temporal.
  SCOPING (2026-07-21, read the Go source): `parseBinaryParams` is a *length
  splitter*, not a value interpreter. Per param it (a) honors `ComStmtSendLongData`
  bound params (treated as BLOB / the paramType), (b) applies the NULL bitmap,
  (c) derives a byte `length` from the type tag — fixed widths Tiny=1,
  Short/Year=2, Int24/Long/Float=4, Longlong/Double=8; temporal
  (Date/Datetime/Timestamp/Duration) read ONE leading length byte then that many
  bytes; NewDecimal/Blob* and the string group (Varchar/VarString/String/Enum/
  Set/Geometry/Bit) read a length-encoded int — (d) slices `length` raw bytes as
  `Val`, and (e) for the string group only, runs `enc.DecodeInput` (client→utf8;
  identity for a utf8 client). It emits `[]BinaryParam{Tp,IsUnsigned,IsNull,Val}`
  — the temporal/decimal/int *interpretation* into a `Datum` is a SEPARATE
  downstream step, so porting the splitter needs NO Rust `Time`/decimal-from-bytes
  parser. Clean decomposition: Unit A = port the splitter to `Vec<BinaryParam>`
  (whole, all type cases; string decode is utf8-identity for the configured
  node) with Go byte-vector tests, unwired; Unit B = port `BinaryParam -> Datum`
  interpretation per type and swap the execute path onto it (this is where the
  `PreparedValue` expansion / execute-binding ripple lands, and where temporal
  finally needs a Time parser). The current signed-LONGLONG fast path stays until
  Unit B. Temporal string parser (that Time parser) STARTED (2026-07-21):
  `parse_date_format` in `tidb-datatype` (`time_parse.rs`) is a faithful, fully
  byte-tested port of Go `types.ParseDateFormat` (the datetime tokenizer + its
  `TestParseDateFormat` vectors, mutation-verified) — the first slice. The core
  `parseDatetime` (field interpretation, ~277 lines), `GetTimezone`, and the
  numeric-form parsers remain; only `PackedTime` (packed repr) existed before, no
  string→`Time` parser at all.
  Unit A DONE (2026-07-21): `parse_binary_params` in `tidb-protocol`
  (`crates/tidb-protocol/src/binary_params.rs`) — the whole splitter to
  `Vec<BinaryParam{tp,is_unsigned,is_null,val}>`, with `parse_length_encoded_int`
  (ported from `util.ParseLengthEncodedInt`) and a bounds-checked
  `take_binary_param_value` (rejects the `1<<63` overflow without panicking).
  Every type arm, the NULL bitmap, `ComStmtSendLongData` bound params, and the
  unsigned flag are covered; string-group decode is utf8-identity (non-utf8
  charset transform deferred — this node speaks utf8mb4). Tested against the Go
  `conn_stmt_params_test.go` vectors: the error cases (`TestParseExecArgs` "For
  error test" + `TestParseExecArgsMalformedLengthEncodedParam`) ported verbatim,
  the success cases assert the raw split of the same Go inputs (their interpreted
  values belong to Unit B). Mutation-verified (width, overflow guard, lenenc NULL
  propagation).
  Unit B (partial, WIRED) DONE (2026-07-21): the LIVE decoder
  `decode_prepared_statement_execute` now admits the whole SIGNED INTEGER family
  — `TYPE_TINY`/`SHORT`/`YEAR`/`INT24`/`LONG`/`LONGLONG` — each sign-extended to
  one `i64` exactly as Go `ExecBinaryParam` widens `int8/16/32/64` (was
  `LONGLONG`-only). This matters for Stage E because MySQL connectors send an
  `INT` bind as `TYPE_LONG` (4 bytes), which the old decoder rejected.
  `PreparedParameterType` gained `SignedTiny/SignedShort/SignedLong`; the
  downstream binding is unchanged (`PreparedValue` still resolves to `i64`, so
  the `&[i64]` currency into the point-read/write templates is untouched).
  Mutation-verified (per-width read, sign-extension, admission).
  Unit B STRING PATH DONE + WIRED end to end (2026-07-21): a prepared INSERT can
  now bind `CHAR`/`VARCHAR` string parameters (sysbench `c`/`pad`) and persist
  them. `PreparedValue` gained `String(Vec<u8>)` and `PreparedParameterType`
  gained `String` (admitting `TYPE_VARCHAR`/`VAR_STRING`/`STRING`, decoded as a
  length-encoded utf8 string — `ExecBinaryParam`'s string arm). The `&[i64]`
  parameter currency became a planner-local `PreparedBindValue { Int, Bytes }`
  (in `tidb-planner`, keeping the planner codec-agnostic): `ConfiguredInsertRow`
  carries it, `tidb-exec` `split_row` maps each to a `tidb-codec`
  `ConfiguredValue` via `encode_configured_mixed_row`, checking int-vs-`CHAR`
  column type (`ConfiguredWriteError::ColumnTypeMismatch`), and
  `mysql_connection` converts decoded values (write -> `PreparedBindValue`,
  point-read -> `i64`, erroring on a string handle). The point-read path stays
  `&[i64]` (a handle is always integer). A sysbench-shaped `INSERT(id INT, k INT,
  c CHAR, pad CHAR)` bind test asserts byte-parity with the mixed-row codec, and
  the type-mismatch guard is mutation-verified. STILL fail closed: unsigned
  integers and the decimal/temporal param families. NOTE: the live decoder still
  duplicates the fixed-width split logic with `parse_binary_params` (both
  faithful to the same Go source); a later slice should unify the live path onto
  `parse_binary_params` once the type-reuse path (which carries
  `PreparedParameterType`, not raw type bytes) is reconciled.
- Configured catalog: admits only signed `BIGINT`/`INT`/`CHAR`(utf8mb4_bin)
  columns and one clustered signed-int handle — vs TiDB's full `TableInfo`
  (indexes, NULL, defaults, all types, partitions, generated columns). It is a
  fixed startup descriptor, not a real `InfoSchema`.
- Prepared DML shapes: the frozen INSERT/UPDATE forms only, vs full
  `pkg/executor/insert.go`/`update.go`.

Do not add to this list silently; when you port one of these, port it whole.

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
- `MyDecimal` binary storage codec (`pkg/types/mydecimal.go`): WHOLE codec DONE
  (2026-07-21) in `tidb-datatype` `decimal.rs` — `ToBin`/`WriteBin`, `FromBin`,
  and `DecimalBinSize`, plus every helper (`writeWord`, `readWord`,
  `countLeadingZeroes`, `fixWordCntError`, `digitsToWords`, `removeLeadingZeros`)
  on a `MyDecimalWords` word-buffer view built from / rendered back to the
  normalized Rust digit string (`from_decimal`/`to_decimal`, the latter
  extracting coefficient digits exactly as Go `ToString` reads the word view).
  The FULL Go tests are ported and green: `go_to_bin_byte_vectors` (byte-exact
  vs TiDB's own `MyDecimal.ToBin` for all 34 `TestToBinFromBin` inputs),
  `go_to_bin_from_bin_round_trip` (the complete `TestToBinFromBin`
  `FromString→ToBin→FromBin→ToString` round trip + its illegal-precision
  `errTests`), and `go_decimal_bin_size_vectors` (`TestDecimalBinSize`). Both
  directions mutation-verified (sign-mask flips fail the vectors), clean
  `clippy -D warnings`. Surfaced divergence (in-code): the Rust `Decimal`
  normalizes leading integer zeros vs Go's nine-word preservation — converges
  via `removeLeadingZeros`; differs only on a Go word-count-clamp quirk outside
  any real `DECIMAL` (precision ≤ 65 < 81). WIRED (2026-07-21): building this
  exposed a DUPLICATE — `tidb-codec`'s `decimal.rs` carried its own hand-rolled
  `MyDecimal` payload codec (`encode_digit_groups`/`decode_digit_groups`, ~100
  lines). Verified the two were byte- and error-identical over the
  `TestToBinFromBin` vectors, then collapsed `tidb-codec::encode_decimal_fixed`/
  `decode_decimal` onto `Decimal::to_bin`/`from_bin` and deleted the duplicate
  (`decimal.rs` 304→201 lines, all `tidb-codec` tests green). The verified
  primitive is now the SINGLE implementation and IS referenced by the live
  value/datum/key codec (`tidb-codec` `value.rs`/`datum.rs` decode `DECIMAL`
  through it); only the deployable single-relation node still exposes no
  `DECIMAL` column (catalog type-breadth item).

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

The Stage B review caveat is now closed. The coordinator names its client,
region-loader, and timestamp capabilities as type parameters — the same shape
the retained lock resolver already uses for `LockRecoveryClient` — so
`TransactionCommandClient` has one production implementation on the sole
`TonicCoprocessorClient`, `ProductionOptimisticTransaction` is the one
production instantiation, and `RealOptimisticTransactionOpener::begin` remains
the only production construction path. `optimistic_2pc_failure_branch_source`
then drives the real transport, real BatchCommands publication identity, and
real region recovery, scripting only TiKV responses and topology, to prove:

- a secondary Commit whose regroup fails after region recovery keeps a
  determinate `Committed` outcome carrying the unresolved secondary keys,
  region, address, and publication, and publishes no BatchRollback;
- a BatchRollback whose regroup fails reports `CleanupFailed` with the original
  prewrite cause, the outstanding keys, and the same physical identity, while
  the sibling batch is still cleaned.

Each assertion was verified by mutation: downgrading the committed outcome to
undetermined, swallowing the cleanup regroup failure, and dropping failure
identity each fail the new tests. Focused validation is the full workspace —
3789 tests, `cargo fmt --check`, and workspace Clippy with `-D warnings`.
Real-cluster acceptance remains `optimistic_2pc_realtikv_source`.

### Campaign 28 Stage C: prepared DML lowering and row codec

Implemented and focused-verified (2026-07-20). Three layers, each owning one
contract:

- `tidb-codec::configured_row_write` returns the record key and row value from
  one call, so Go's `tables.CanSkip` rule — a clustered int handle is skipped
  from the row value — is structural instead of a per-call-site convention.
- `tidb-planner::prepared_dml` admits only the frozen INSERT/UPDATE shapes and
  binds positional markers; every other DML feature is rejected before a
  prepared handle exists.
- `tidb-exec::real_tikv_dml` turns a bound command into mutations plus affected
  rows and composes the Stage B transaction (`snapshot_get` → plan →
  `commit`/`finish_without_writes`).

Byte-exactness is proved against real `tablecodec`/`rowcodec` output from
`difftests/transaction-tests/fixtures/generate_configured_rows.go`. 37 focused
tests pass. Five assertions were mutation-verified: the handle leaking into the
row value, an unchanged UPDATE publishing anyway, `wrapping_add` replacing
`checked_add`, an UPDATE dropping untouched stored columns, and a marker
numbered out of source order.

Two findings worth carrying forward:

- `pkg/executor/write.go:174-184` is the affected-rows contract: an unchanged
  row takes `AddTouchedRows(1)` and adds an affected row only under
  `ClientFoundRows`. The bounded path never negotiates that capability.
- Go's signed `+` overflow check at `builtin_arithmetic.go:263` is exactly
  `i64::checked_add`, so no hand-written boundary condition is needed.

### Campaign 28 Stage D: prepared writes over the wire

Implemented (2026-07-21). A prepared INSERT now travels the real MySQL
connection and returns an OK packet carrying affected rows.

- `ProductionReadProcessAuthority::transaction_opener()` derives write
  capability from the authority that is already running, so reads and writes
  share one PD worker, one RegionCache, one BatchCommands transport, and one
  shutdown order. It holds only cloneable handles and starts nothing.
- `tidb-exec::real_tikv_dml` gained `prepare_configured_write` and
  `commit_configured_write`, mirroring the read path's
  `prepare_configured_point_read`. This keeps the dependency direction
  `tidb-server -> tidb-exec -> tidb-txnkv`: the server names no transaction
  type and parses no SQL itself.
- `sql_node.rs` gained `PreparedWrite`, the `PreparedStatement` enum, and
  `WriteOutcome`; `mysql_connection.rs` admits a read first and only offers a
  rejected statement to the write planner, so an existing prepared SELECT keeps
  its exact error text.
- Affected rows reach the client only from `OptimisticCommitOutcome::Committed`.
  Rolled back, cleanup failed, and undetermined all surface as errors, because
  an OK packet asserts durable rows.

Two limits were found and removed rather than worked around: the execute
decoder hard-coded a single parameter (`parameter_count != 1`), and the
connection passed a literal `1` instead of the statement's own count. The
decoder body was already count-generic, so only the guard changed — it now
rejects just a zero-marker execute.

Still open for Stage D's original scope: the read-named process authority keeps
its name (the rename is mechanical but touches many files). The wire contract is
owned by `a_prepared_write_answers_with_an_ok_packet_carrying_affected_rows` in
`crates/tidb-server/tests/mysql_client_lifecycle_source.rs` with a stubbed
publication, which is deliberately not acceptance.

### Campaign 28 Stage E (minimum): one real INSERT persists

Passed against real PD/TiKV (2026-07-21). The first genuinely durable write of
the rewrite. `crates/tidb-exec/tests/prepared_write_persists_realtikv_source.rs`
(runner `scripts/run-campaign28-prepared-write-realtikv.sh`) drives the exact
Stage D composition — `ProductionReadProcessAuthority::connect` →
`transaction_opener()` → `commit_configured_write` — against a tag-owned
three-TiKV playground, then reads the row back from a **freshly reconnected
authority** (distinct authority_id, new PD client/RegionCache/transport). The
committed value therefore lives in TiKV, not in process memory. Receipt:
`final_balance=107 write_authority_id=1 restart_authority_id=2`.

This test caught and fixed a real regression the workspace suite could not: the
Stage D `transaction_opener` clone held a `PdClient` handle that
`shutdown()` never released, so the PD stage (which needs
`Arc::strong_count == 1`) refused to stop — for every server, read-only
included. The drain check only fires against real PD, so only a live cluster
surfaced it. Fixed by making the authority's `transaction_opener` an `Option`
dropped in `shutdown()`, symmetric with the read opener. See the memory note
`rust-rewrite-authority-shutdown-drain`.

What this proof is NOT: it exercises the exec-level composition, not the full
TCP→client path, and it reads back through a fresh Rust authority rather than an
independent Go TiDB. That gap is now closed by the core live write proof below.

### Campaign 28 Stage E (core live write proof): GREEN over TCP with an independent Go TiDB

Passed against real PD/TiKV (2026-07-21). `scripts/run-campaign28-stage-e-write-proof.sh`
starts a tag-owned tiup `--db 1` v8.5.6 playground (3 TiKV + one **Go TiDB**);
the Go TiDB creates `campaign28.accounts` and seeds 16 rows (sum=13600); the
**deployable** Rust node (`target/release/tidb-server` via `run_configured_node`)
is launched against the discovered `TABLE_ID`; the raw-socket prepared client
`scripts/mysql-prepared-client.py write` drives 16 prepared point reads + 16
prepared arithmetic `UPDATE`s over TCP through the Rust node's binary protocol;
a **separate Go TiDB connection** verifies `SUM(balance)` advanced by exactly the
16 committed UPDATEs (13600→13616), then again across a **Rust-node restart**
(→13632). Tag-owned cleanup drains with no orphans.

This exposed and fixed a real gap: `RealTiKvServerSession` (the deployable
single-relation session `run_configured_node` uses) never implemented
`prepare_write`/`execute_prepared_write` — only the multi-relation join session
did, and Stage E-min + the wire test used the exec layer / a mock, so the real
deployable node fell to the fail-closed `prepare_write` stub. Fixed in
`real_tikv_node.rs` by threading `transaction_opener` from the authority through
`RealTiKvSessionFactory` into the session and implementing both methods,
mirroring `real_tikv_multi_node.rs`. Env notes (memory
`c28-stage-e-live-harness-path`): tiup's own v8.5.6 Go TiDB is the oracle (no
failpoint build needed for a basic write proof); local mysql 9.5 needs
`--plugin-dir`; Homebrew sysbench links mariadb-connector-c which forces TLS, so
the matrix uses the raw-socket client (same `COM_STMT_PREPARE`/`EXECUTE` path).

The FULL prepared matrix + MULTI-REGION are now proven too (2026-07-21):
`mysql-prepared-client.py matrix` drives one-row INSERT, two-row INSERT, direct
`SET` update, arithmetic UPDATE, and a point read through the restarted
deployable node, each Go-TiDB-verified (id101→1500, id102→2005, id103→3000); and
with the table split at handle 102 (`SPLIT TABLE ... BY (102)`, 2 regions),
insert keys 101 (region A) and 103 (region B) both commit — proving the write
path's RegionCache routes to a non-first region. TWO acceptance-matrix ERROR
cases are also proven end to end, each with Go TiDB confirming the stored row is
unchanged: (1) DUPLICATE-KEY — re-`INSERT`ing existing id=101 is rejected (client
gets a numeric error, not OK; the `Insert`/NotExist assertion holds) and id=101
stays 1500; (2) VALUE OVERFLOW — a `balance + 1` UPDATE on id=104 stored at
`i64::MAX` is rejected by the write path's checked signed addition
(`ConfiguredWriteError::Overflow` = TiDB `ErrOverflow`) and id=104 stays
`i64::MAX` (no wrap). So all five ExecPlan matrix shapes, multi-region write
routing, and the duplicate-key + overflow rejections commit/reject through the
real node and are independently Go-TiDB-verified.

Still remaining for the LITERAL FULL ExecPlan Stage E (neither an unproven Rust
behavior): (1) sysbench SPECIFICALLY — NOW DONE for the bounded C28 workload
(2026-07-21). A libmysqlclient-linked sysbench exists at
`target/sysbench-mysql-client/bin/sysbench` (`otool -L` → `libmysqlclient.24`,
no mariadb-connector-c, so no forced-TLS segfault). Wired via `C28_SYSBENCH`,
`scripts/sysbench-prepared-read-write.lua` ran `--db-ps-mode=auto` (real MySQL
prepared statements, no text fallback) against the deployable Rust node:
**1000 events = 2000 prepared queries (1 point read + 1 arithmetic UPDATE each)
at ~2076 q/s, ignored errors: 0**, independently followed by Go-TiDB
verification. SCOPE HONESTY: this is the BOUNDED C28 prepared read+write mix on
the single-column-family BIGINT `accounts` table — NOT sysbench's full
`oltp_read_write` (`sbtest` with `CHAR` columns, a secondary index, range/
`ORDER BY`/`DISTINCT`/`SUM`, `DELETE`, 4-column `INSERT`), which stays gated on
the capability gaps in "Capability gap to real sysbench and TPC-C" below. (2)
Conflict/cancellation cases — now UNBLOCKED: the pinned v8.5.6 failpoint fixture
exists at `tidb-rust-worktrees/campaign22-v856-fixture/bin/tidb-server`
(`v8.5.6-dirty` ae18096e, `beforeCommitSecondaries`+`enableTestAPI` present), so
a client-go-grounded lock state can be produced deterministically for the Rust
read/write path's already-transcreated lock resolver (`lock/resolver.rs`,
`cop_paging/lock_recovery.rs`, coordinator prewrite) to resolve live — NOT a
timing race (an earlier invented timing harness was correctly reverted). (3)
Stage F evidence promotion (`campaign_close`) — repo ceremony.

### Campaign 28 remaining stages

- Stage E: one-thread bounded prepared read/write sysbench against Rust,
  independent Go TiDB verification before/after Rust restart, and no text
  fallback. This proves the first write vertical, not full transaction parity.

## Capability gap to real sysbench and TPC-C

Measured against the actual workload definitions, not an estimate.
`target/sysbench-mysql-client/share/sysbench/oltp_common.lua` creates:

    CREATE TABLE sbtest%d(
      id INTEGER NOT NULL AUTO_INCREMENT,
      k INTEGER DEFAULT '0' NOT NULL,
      c CHAR(120) DEFAULT '' NOT NULL,
      pad CHAR(60) DEFAULT '' NOT NULL,
      PRIMARY KEY (id)
    );
    CREATE INDEX k_%d ON sbtest%d(k);

and prepares exactly these ten statements:

    SELECT c FROM sbtest%u WHERE id=?
    SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ?
    SELECT SUM(k) FROM sbtest%u WHERE id BETWEEN ? AND ?
    SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c
    SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c
    UPDATE sbtest%u SET k=k+1 WHERE id=?
    UPDATE sbtest%u SET c=? WHERE id=?
    DELETE FROM sbtest%u WHERE id=?
    INSERT INTO sbtest%u (id, k, c, pad) VALUES (?, ?, ?, ?)
    BEGIN / COMMIT

What that requires, in dependency order:

1. Stage D write dispatch. DONE (2026-07-21) — see Stage D/E above.
2. Type breadth.
   - `k INTEGER` (int32): DONE (2026-07-21). `ConfiguredScalarType {BigInt, Int}`
     plus `ConfiguredColumn::stored_int_not_null`; scan metadata is
     `MYSQL_TYPE_LONG`/`column_len=11`; the exec write path range-validates
     INSERT values and UPDATE results against the column's domain (i32 for INT),
     matching Go's `ConvertIntToInt` overflow. INT and BIGINT store byte-identical
     bytes because rowcodec uses the value's compact width, not the column type.
     Five focused tests, three mutation-verified guards. The change is additive:
     all 82 existing `ConfiguredColumn` call sites are unchanged.
   - `c`/`pad` are `CHAR(N)`: the string-column plumbing is now DONE end to end
     through the deployable node (2026-07-21). All the sub-pieces below (row
     codec, catalog `Char` type, `PreparedValue::String`, string parameter
     decoding, binary string result rows) landed across CHAR slices 1–3c and
     `parseBinaryParams` Unit B, and the FINAL wiring — the deployable node's
     typed stored-column configuration — is now in place: `node_config.rs`
     `ConfiguredReadColumnKind` gained `StoredIntNotNull` + `StoredCharNotNull
     {max_length}`, the CLI descriptor parses `name:id:stored-int-not-null` /
     `name:id:stored-char-not-null:N` (range-checked 1..=255, descriptor
     round-trips through the ready-JSON), and `real_tikv_node::configured_table`
     maps them to `ConfiguredColumn::stored_int_not_null` /
     `stored_char_not_null`, which the exec read/write path already handles
     (`real_tikv_dml.rs` encodes a `Char` value as `Bytes`). So `run_configured
     _node` can now serve the sysbench `sbtest` shape (BIGINT PK + INT + two
     CHAR). Point `DELETE` DONE too (2026-07-21): the fail-closed
     `DmlStmt::Delete` arm now lowers `DELETE FROM t WHERE pk=?` to a
     `DeletePoint` template (tidb-planner `prepared_dml.rs`), the transaction
     layer gained `OptimisticMutationKind::Delete` = `Op_Del` + `AssertExist`
     (Go `TableCommon.removeRecord`), and `plan_delete` (tidb-exec) removes the
     record key of an existing row (missing row -> no write). Full tests at all
     three layers; the deployable node reaches it through generic delegation.
     PK range read DONE (2026-07-21): module A — `flatten_and_bind` desugars a
     non-negated `x BETWEEN low AND high` to `x >= low AND x <= high` (Go's
     expression rewrite; `NOT BETWEEN` -> OR, rejected); module B — the PREPARED
     read planner (`lower_prepared_point_read`) is generalized from one `Eq`
     marker to N clustered-PK comparisons (any op) with markers at contiguous
     positions `0..N`, the template holds `Vec<PreparedReadComparison>` +
     `parameter_count`, and `bind` substitutes each marker and delegates to the
     same `lower_validated` the ranger already folds into a closed range. So the
     prepared `SELECT c FROM t WHERE id BETWEEN ? AND ?` (sysbench read 2) lowers
     and binds to a range scan end to end; the point read stays the one-`Eq`
     special case (existing tests green). `ORDER BY c` (sysbench reads 4,5) DONE
     at code+unit (2026-07-21): the prepared template resolves `ORDER BY
     <projected col>` to `PreparedOrderColumn {output_offset, direction,
     scalar_type}` (`read_only_scan.rs`, getter `order_by()`; fail-closed on
     unprojected/positional/expression keys; the literal COM_QUERY
     `lower_validated` now rejects ORDER BY rather than returning silently-
     unsorted rows); exec `stable_order_prepared_rows` (`order.rs`) sorts the
     buffered output rows — signed int numeric, CHAR via
     `Collation::Utf8Mb4Bin.compare` (PAD SPACE, Go `binPaddingCollator`); the
     server wraps the scan in `SortingResultSetSource` only when order keys exist
     (via new `QueryResult::into_source`). The existing INT-only configured-order
     path (`stable_order_configured_rows`/`configured_topn`) could NOT be reused —
     it rejects non-`Datum::Int` keys — so a collation-aware sibling was built,
     delegating string order to the already-Go-grounded `collation.rs`. 17 new
     tests (planner 8 / exec 6 / server 3), clippy -D warnings clean. `DISTINCT c`
     (sysbench read 5) DONE at code+unit (2026-07-21): planner carries a
     `distinct` flag on the template (`is_distinct()`; text COM_QUERY path
     fail-closes it as SelectModifier); exec exposes the Go-grounded
     `DistinctChecker` (`aggregate_distinct.rs`, transcreation of
     `aggregation/util.go::distinctChecker`) as `pub`; server
     `DistinctResultSetSource` is a STREAMING HASH-SET dedup composed OUTSIDE the
     sort, so `DISTINCT ... ORDER BY` returns distinct rows already sorted (8 new
     tests). RISK (surfaced): DISTINCT identity is RAW BYTES (Go `EncodeValue` /
     the tree's `Datum::String` eq), NOT collation-normalized — so `"a"` and
     `"a "` are distinct here even though utf8mb4_bin sorts them as a PAD SPACE
     tie (hence a hash set, not adjacent-dedup, for soundness). This matches the
     existing Rust tree's DISTINCT/GROUP-BY string identity but MAY diverge from
     real TiDB new-collation PAD SPACE grouping; it's a pre-existing whole-tree
     question, invisible to sysbench (its `c` never collides only on trailing
     spaces). `SUM(k)` (sysbench read 3) DONE at code+unit (2026-07-22): the
     planner accepts one `SUM(<int/bigint col>)` field -> `PreparedAggregate` on
     the template (result type from Go `typeInfer4Sum`: `DECIMAL(arg.flen+21, 0)`,
     binary charset — INT->DECIMAL(32,0), BIGINT->DECIMAL(41,0)); fail-closed on
     every other aggregate shape and on the text path. Exec REUSES the Go-grounded
     `fold_values(Sum)` (Int->Decimal, empty group->NULL). Server
     `AggregateResultSetSource` folds the scan column into one row and carries its
     own DECIMAL `columns()` (the binary encoder dispatches per column type);
     result metadata overridden at prepare. This forced a real sub-port —
     `BinaryResultCell::Null` + Go `DumpBinaryRow`'s null bitmap (bit `i+2`), the
     first NULL the binary result path could represent (all prior columns were
     NOT NULL); byte-tested, and it benefits every nullable result. 11 new tests.
     PLAN DIVERGENCE (surfaced): TiDB pushes a coprocessor partial-sum + SQL
     finalize; this reads all `k` to the SQL node and sums there — identical
     value, no pushdown. All five sysbench READ statements now lower+execute at
     code+unit. Secondary index on `k` — exec-level maintenance DONE + consistent
     + byte-exact (2026-07-22): codec `encode_non_unique_index_key`/
     `non_unique_index_value` (byte-exact vs Go `GenIndexKey`/`GenIndexValuePortal`),
     txnkv `index_put`/`index_delete` kinds (Op_Put/Op_Del + `None` assertion, Go
     `tables.index` Set/Delete), planner `ConfiguredIndex` + additive
     `ConfiguredTable::with_indexes`, and `real_tikv_dml` plan_insert/delete/update
     add/remove/move the entry (committed atomically with the row via the existing
     batch 2PC), fail-closed on unique/non-int; 8 new tests. NODE-CONFIG
     declaration DONE (2026-07-22): `--read-table` takes an optional trailing
     `<index_count> <name:index_id:column_id>...` section (backward compatible),
     `ConfiguredReadIndex` on `ConfiguredReadTable`, and `configured_table` maps
     it to `ConfiguredTable::with_indexes` — so `run_configured_node` declares the
     sbtest `k` index end to end (5 more tests). Secondary index MODULE is
     complete at code+unit; only the live real-TiKV index proof remains (task #22).
     `plan_update` CHAR row rebuild DONE (task #31, 2026-07-22): it now decodes
     each unchanged column at its own type (`decode_stored_column_value`) and
     re-encodes via `encode_configured_mixed_row`, so `UPDATE sbtest SET k=k+1`
     works on the CHAR-bearing table with the k index moving old->new (4 tests
     incl. the full indexed-mixed scenario). `UPDATE c=?` STRING assignment DONE
     (task #32, 2026-07-22): `ConfiguredBigIntAssignment` -> `ConfiguredAssignment`
     with a `SetBytes(Vec<u8>)` variant, the lowering picks SetInt/SetBytes by
     column type (and rejects `col+?` on a non-int column), bind extracts the
     typed value, and `plan_update` sets the CHAR column's bytes (6 tests).
     MILESTONE: with #31 + #32, ALL sysbench DML statements (5 reads, both
     UPDATEs, DELETE, INSERT) work at code+unit through the deployable prepared
     path. The ONLY remaining sysbench-workload gap is `BEGIN/COMMIT` (task #29,
     statement 10). Fixed a real UPDATE-on-sbtest commit bug (task #33,
     2026-07-22): the UPDATE `planned_bytes` budget (`max_configured_row_value_len`)
     counted CHAR columns as 8 bytes, so a whole-row UPDATE under-provisioned and
     the coordinator rejected it as `TransactionTooLarge` at commit; now
     type-aware (`CHAR(N)` -> `N*4`). `CHAR(N)` length enforcement DONE (task #34,
     2026-07-22): `tidb_datatype::char_length::produce_char_value` (faithful port
     of the CHAR path of Go `types.ProduceStrWithSpecifiedTp` — rune count, all-
     trailing-whitespace overflow truncated, else `DataTooLong` in strict mode) is
     applied by both the INSERT and UPDATE write paths (8 tests). The ORDER BY /
     DISTINCT / SUM / index / UPDATE live real-TiKV proof is folded into task #22.
     The ONLY remaining sysbench-workload gap is `BEGIN/COMMIT` (task #29).

     String row codec: DONE (2026-07-21, CHAR slice 1). `tidb-codec` gained
     `ConfiguredValue {Int, Bytes}`, `encode_configured_mixed_row` /
     `encode_configured_row_value_typed`, and `decode_configured_row_bytes`
     (additive; the integer-only encoder now delegates through the typed
     helper). String values store as raw bytes and are proven byte-exact against
     real Go `rowcodec` output (`value_char_*` in `configured_rows.hex`, from
     `generate_configured_rows.go`) across empty, ASCII, multibyte UTF-8, and
     trailing-space cases; the byte-exactness assertion is mutation-verified.
     This is the foundation slice; the remaining CHAR work (catalog `Char` type
     + scan metadata, the non-`Copy` `PreparedValue` string ripple through
     session/exec bind signatures, string parameter decoding, and binary string
     result rows) is mechanical-but-broad and NOT started.

     Catalog Char type: DONE (2026-07-21, CHAR slice 2). `ConfiguredScalarType`
     gained `Char { max_length }` and `ConfiguredColumn::stored_char_not_null`;
     `value_range` became `integer_range -> Option` (a `Char` has no integer
     range, so the exec range check is a no-op there rather than a false
     rejection); `scan_column` now takes its collation from the type. A `CHAR`
     projects with `tp=254`, `collation=-46`, `column_len=max_length`, verified
     through the real lowering path and mutation-checked on the collation sign.

     Scan-metadata correctness finding (verified in Go, 2026-07-21): a `CHAR`
     column's coprocessor `ColumnInfo` is NOT simply `tp=254, collation=46`. Go
     sends the collation NEGATED when new collation is enabled (the default) —
     `pkg/util/collate/collate.go:118 RewriteNewCollationIDIfNeeded` returns
     `-id`, so `utf8mb4_bin`(46) is sent as `-46`. The existing integer columns
     use collation `63` positive and work only because integers do not collate;
     strings do, so the sign is load-bearing for any pushed-down string
     comparison. The `-46` value is now in `collation_id()` per the Go source,
     but it is NOT yet load-bearing (no wired path sends it) and MUST be
     confirmed against real TiKV when the string read path lands — a wrong sign
     misbehaves silently, it does not error. `TypeString`(CHAR)=254,
     `TypeVarString`=253, `utf8mb4_bin` is a PadSpace collation.

     Binary result cell encoder: DONE (2026-07-21, CHAR slice 3a). `tidb-protocol`
     gained `BinaryResultCell {SignedLongLong, String}` and
     `encode_binary_result_row`, producing a correct MySQL binary row — `0x00`
     header, `ceil((n+2)/8)`-byte null bitmap (two reserved low bits), 8-byte
     int cells and length-encoded string cells. Additive, zero ripple to the
     existing int-only stream, unit-verified against the wire format (including a
     7-column test that pins the reserved-bits offset) and mutation-checked.

     Binary result stream + connection wiring: DONE (2026-07-21, CHAR slice 3b).
     `BinarySignedLongLongResultSetStream` -> `BinaryResultSetStream`: `new`
     admits LONGLONG plus the `DumpBinaryRow` string types
     (`is_binary_string_result_type` = `TYPE_STRING`/`VAR_STRING`/`VARCHAR`),
     `row_packet` takes `&[BinaryResultCell]` and validates each cell against its
     column type. `connection_resultset.rs` maps each `Datum` to a cell
     dispatched by column type (`Datum::Int` for integer columns, `Datum::String`/
     `Datum::Bytes` for string columns), exactly as Go's `DumpBinaryRow` switches
     on `columns[i].Type`. Verified with protocol tests (mixed row bytes, cell/
     column mismatch, unsupported type) and the updated structure test.

     Remaining for a runnable `oltp_point_select` read vertical (needs only an INT
     param, so it avoids the write-side `PreparedValue` ripple):
     - CHAR slice 3c. De-risked (2026-07-21): the coprocessor string DECODE
       already exists and is Go-grounded — `tidb_codec::decode_column_datums`
       (`crates/tidb-codec/src/column.rs`) handles `Varchar/VarString/String/Blob`
       and produces `Datum::new_collation_string(bytes, collation)` (or
       `new_bytes` for a binary string). So 3c is NOT porting decode; it is
       threading a `CHAR` column's string `FieldType` through the read path so
       the response decodes as a string and the client sees a string column:
         * the `final_field_types` that `chunk_decode::decode_datums` consumes
           (built in `tidb-distsql` / the read runtime), and
         * `real_tikv_read::protocol_columns` (result `ColumnInfo`, must carry
           `tp=254`/charset), and
         * the DAG request column info for the scan (slice 2 set the scan
           metadata; confirm it flows into the request).
       All three derive from the `ConfiguredColumn`, which already knows it is
       `Char`. After threading, a real-cluster read-projection proof confirms the
       `-46` scan collation sign on the wire (still the one unverified bit).

       Result column metadata: DONE (2026-07-21, CHAR slice 3c-result).
       `real_tikv_read::protocol_columns` previously hardcoded
       `type_code=LongLong, charset=63, column_length=20` for EVERY column; it now
       derives from the column type via `ConfiguredScalarType::result_type_code`/
       `result_charset_id`/`result_column_length`, grounded in Go:
         * charset is the POSITIVE client id (`CharsetNameToID`: utf8mb4->46,
           binary->63) — deliberately NOT the negated `-46` coprocessor scan
           collation (`scan_column().collation`);
         * a string length is scaled by the charset max byte width
           (`ConvertColumnInfo`: `CHAR(120)` -> 480);
         * `ResolvedProjectionColumn` now exposes `scalar_type()`.
       Mutation-verified the ×4 length and the positive-sign charset.

       KNOWN FAITHFULNESS GAP (documented, not a bug for value transport): an INT
       column reports as LONGLONG on the result and is dumped as an 8-byte cell,
       because the binary encoder has no 4-byte INT cell. TiDB's `DumpBinaryRow`
       would use `TypeLong` + `dump.Uint32` (4 bytes). Value is exact but typed as
       BIGINT. Closing it needs a `BinaryResultCell::SignedLong(i32)` + the
       `TypeLong` result type + connection dispatch — a small coupled slice.

       Remaining 3c (still needs a cluster): thread the string `FieldType` into the
       `final_field_types` that `chunk_decode::decode_datums` consumes and confirm
       the scan metadata flows into the DAG request, then a real-cluster
       read-projection proof that a `CHAR` column round-trips and confirms the
       `-46` scan collation on the wire.
     The separate write vertical still needs the non-`Copy` `PreparedValue`
     string variant + string parameter decoding (`parseBinaryParams` in
     `pkg/server/conn_stmt_params.go` is the Go source for that).

     Storage-format finding (verified in Go, 2026-07-21): the row bytes for a
     string column depend on collation via
     `pkg/types/etc.go:147 NeedRestoredDataWithCollate`. It returns true — the
     row then carries extra restored-collation bytes — only when the column is a
     non-binary string AND `(!IsBinCollation(collate) || IsTypeVarchar)` AND the
     collation is not `utf8mb4_0900_bin`. `IsBinCollation`
     (`pkg/util/collate/collate.go:356`) includes `utf8mb4_bin`, which is TiDB's
     default utf8mb4 collation (`TestDefaultCollationForUTF8MB4`). So a
     `CHAR(N)` at the default `utf8mb4_bin` needs NO restored data — it stores
     raw string bytes. `VARCHAR` or a non-bin collation (e.g.
     `utf8mb4_general_ci`) DOES need the restore format. sysbench uses `CHAR`,
     so the first increment can target the raw-bytes (no-restored-data) path,
     but the live proof MUST confirm the playground's actual column collation
     (Go creates the table) before trusting byte-exactness, and VARCHAR/ci
     collations remain a later, larger sub-case.
3. Secondary index `k_1`: index key encode/decode plus maintenance on every
   INSERT, UPDATE, and DELETE. This is the first place a single SQL statement
   must produce more than one mutation per row.
4. `DELETE`.
5. Explicit `BEGIN`/`COMMIT` with autocommit off, so one transaction spans the
   whole event.
6. `SUM` pushdown, `DISTINCT`, and `ORDER BY` on a non-handle column.
7. A real catalog. sysbench runs its own `CREATE TABLE`, so the static
   configured catalog must be replaced by real `TableInfo` read from TiKV meta
   — or DDL must be implemented.
8. `AUTO_INCREMENT` is avoidable at first: sysbench's own INSERT supplies `id`
   explicitly, so the allocator is only needed if a workload omits it.

TPC-C adds, on top of all of the above: nine tables, composite clustered common
handles, `DECIMAL` and `DATETIME`, cross-table joins, `ORDER BY ... LIMIT` on
non-handle columns, and `SELECT ... FOR UPDATE`, which requires pessimistic
locks — an entire transaction family the design lists as its own dependency
step. Treat sysbench `oltp_read_write` as the first real target and TPC-C as
the gate after pessimistic locking exists.

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

1. Freeze the current C27/C28 claims and run one shared integration gate. The
   secondary-commit and rollback regroup-failure regressions that blocked this
   are landed and mutation-verified; no Stage B review caveat remains.
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
