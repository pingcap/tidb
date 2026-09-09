# pkg/ttl parity audit (baseline a85e0fd5df)

Full-file audit of the transcreated subpackages of Go `pkg/ttl` —
`cache/` (base, infoschema, table, task, ttlstatus), `sqlbuilder/`,
`session/` — against `rust/crates/tidb-ttl`. The `ttlworker/`, `client/`
and `metrics/` subpackages are not part of this crate (the worker runtime
needs the executor seam) and stay an open claim.

## Documented boundaries this batch (behavior)

1. `sql_builder.rs` string-key escape path: Go writes `sqlescape.EscapeString`
   output raw, so a non-binary string key column holding non-UTF-8 bytes
   (e.g. latin1) yields non-UTF-8 SQL text. This crate's SQL surface is
   `&str` end to end, and a lossy conversion would build a DELETE targeting
   wrong rows — so the build errors loudly. Commented at the site; a raw-byte
   port would require re-typing the TTL→executor seam.
2. `sql_builder.rs` `write_value_expr` non-numeric arms: unreachable through
   the same routing as Go (bit/blob/binary-string columns take the hex branch
   before any AST value expression); pinned with a comment.
3. `table.rs` `unsigned_edge` fallback arm: Go's `d.GetInt64()` panics on
   non-int kinds; the clone is the documented substitute for that panic path.

## Verified matching (one line each)

- `base.go` ↔ `base.rs`: complete (zero-time None, strict `>` interval).
- `infoschema.go` ↔ `infoschema.rs`: schema-ver short-circuit, TTL filter,
  partition fan-out, pointer-identity reuse, skip-on-error.
- `table.go` ↔ `table.rs`: constructors/error texts, ValidateKeyPrefix,
  FullName, the whole split family (charset/collate switch, unsigned
  MaxInt64 split, common-handle skip rules, raw key ranges), all four
  decoders, flag bytes, Key.Next, DecodeCmpUintToInt, TimeUnitType iota and
  String(), EvalExpireTime SQL text and DST-safe shift/truncate.
- `task.go` ↔ `task.rs`: byte-identical SELECT/INSERT task builders, all
  four builders' SQL/arg order, PeekWaitingTTLTask, TaskStatus values,
  TTLTaskState JSON tags incl. prev_owner, NULL branches.
- `ttlstatus.go` ↔ `ttlstatus.rs`: 17-column SELECT, WithID reader, six
  JobStatus values, every NULL-guarded RowToTableStatus field, Update error
  propagation.
- `sqlbuilder/sql.go` ↔ `sql_builder.rs`: state machine + ordinals, Build
  safety check, SELECT/DELETE text incl. PARTITION(...), common/IN/expire
  conditions, ORDER BY/LIMIT, ScanQueryGenerator semantics, empty-rows
  delete error, backslash escaping.
- `session.go` ↔ `session.rs`: interface shape, TxnMode incl. unknown-mode
  error, RunInTxn defer ordering and every failure path, ResetWithGlobal-
  TimeZone, KillStmt, DrainRecordSet(8).

## Accepted narrowings (documented at sites)

- Go panics on nil primary index / nil TimeColumn / out-of-range indices;
  Rust returns errors or guards (unreachable-for-valid-tables hardening).
- `MockExpireTimeKey` is honored without an `intest.InTest` gate: the key
  only exists when a test put it there (commented at the site).
- Rollback under a fresh 1s ctx, `kv.InternalTxnTTL` tagging, one-shot
  `sqlExec` caching, `AvoidReuse` nil-guard: boundary/narrowing notes.

## Validation

- `cargo test -p tidb-ttl`, `cargo fmt -p tidb-ttl`, `git diff --check`,
  `make lint`.
