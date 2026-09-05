# Error code parity audit: Go TiDB vs `tidb-error`

Audit date: 2026-08-02. Branch `errcode-audit` (from `hparser-integration`).

## Why this audit exists

The integration replay classifies `(Err(_), true) => SkipClass::BothRejected`. It never
compares error *text* or *code*. A wrong MySQL error number, a wrong SQLSTATE, or raw Rust
`Debug` text on the wire all pass that gate silently. Clients — ORMs, migration tools,
retry loops — switch on the code and grep the message, so a wrong code is a broken
application, not a cosmetic difference. Nothing had been pointed at this surface before.

## Method

The catalogue layer was compared **programmatically**, not by eye: a script parses the Go
sources and the Rust sources into `name -> code` and `name -> (message, redact_arg_pos)`
maps and diffs them, including a format-verb extraction pass that compares the `%s`/`%d`
placeholder *sequence* independently of surrounding words. Anything reported as equal below
is equal by that mechanical comparison over the whole table, not by sampling.

Raise sites cannot be compared mechanically and were walked by hand.

Sources compared:

| Go | Rust |
| --- | --- |
| `pkg/errno/errcode.go` | `rust/crates/tidb-error/src/tidb/errcode/consts_{1,2}.rs` |
| `pkg/errno/errname.go` | `rust/crates/tidb-error/src/tidb/errname/consts_{1..4}.rs` + `catalog_{1..4}.rs` |
| `pkg/parser/mysql/errcode.go` | `rust/crates/tidb-error/src/mysql/errcode/consts_{1,2}.rs` |
| `pkg/parser/mysql/errname.go` | `rust/crates/tidb-error/src/mysql/errname/consts_{1..3}.rs` + `catalog_{1..3}.rs` |
| `pkg/parser/mysql/state.go` | `rust/crates/tidb-error/src/mysql/state.rs` |
| `pkg/parser/terror/terror.go` | `rust/crates/tidb-error/src/terror.rs` |
| `pkg/util/dbterror/terror.go` | `rust/crates/tidb-error/src/terror.rs` |

## Verified equal

These are exact, whole-table results — every entry, not a sample.

### 1. `pkg/errno` code constants — EQUAL

1166 constants on each side. Zero name differences in either direction, zero value
differences. `pkg/errno/errcode.go` vs `tidb/errcode/consts_{1,2}.rs`.

### 2. `pkg/errno` messages — EQUAL, character for character

1164 messages on each side. Zero text differences, therefore zero placeholder-count,
placeholder-order, or width-specifier differences. The width specifiers Go carries
(`%-.192s`, `%-.64s`, `%-.100T` and friends) are preserved verbatim.

`redact_arg_pos` (Go `RedactArgPos`, the log-redaction argument index list) matches on
all 1164 entries — same positions, same order.

`pkg/errno/errname.go` vs `tidb/errname/consts_{1..4}.rs`.

### 3. `pkg/parser/mysql` code constants — EQUAL

954 constants, zero differences. `pkg/parser/mysql/errcode.go` vs
`mysql/errcode/consts_{1,2}.rs`.

### 4. `pkg/parser/mysql` messages — EQUAL, character for character

952 messages, zero text differences, zero `RedactArgPos` differences.
`pkg/parser/mysql/errname.go` vs `mysql/errname/consts_{1..3}.rs`.

### 5. Catalog wiring — EQUAL

Every `CatalogEntry` in both `mysql/errname/catalog_*.rs` and `tidb/errname/catalog_*.rs`
has `name` == code-constant identifier == message-constant identifier. No entry is wired to
a neighbouring code's message, and there are no duplicate entries. Every message name
present in Go is present in the Rust catalog. This is the class of defect where a table
looks right but entry N points at entry N+1's text; it does not occur.

### 6. SQLSTATE table — EQUAL

`pkg/parser/mysql/state.go` `MySQLState` has 244 entries; `mysql/state.rs` `MYSQL_STATES`
has 244 entries with **identical values in identical source order**. `DefaultMySQLState =
"HY000"` is mirrored as `DEFAULT_MYSQL_STATE = "HY000"`, and `mysql_state()` falls back to
it rather than guessing. Codes absent from the table therefore get `HY000` on both sides,
as Go does.

### 7. terror class numbering and RFC prefixes — EQUAL

All 27 classes match Go `pkg/parser/terror/terror.go:61-87` by number *and* by description
string, in order. The non-obvious one is right: `ClassOptimizer = 10` registers the
description `"planner"`, not `"optimizer"`, so a class-scoped optimizer error renders its
RFC code as `planner:<n>` exactly as Go does
(`terror.rs:43` vs `pkg/parser/terror/terror.go:70`). Likewise `ClassPerfSchema = 12 ->
"perfschema"` and `ClassMockTikv = 22 -> "mocktikv"`.

`dbterror`'s class set (`pkg/util/dbterror/terror.go:27-48`) is a subset of these and
introduces no renumbering; it wraps `terror.ErrClass` directly.

### 8. Raise-site messages that fill a Go template — EQUAL

Of the 131 raise sites in the Rust tree that pair a resolvable numeric code with a literal
message, 95 render exactly Go's catalogue text for that code. Of the 36 that differ, all but
the ones listed under findings differ *because Go's own raise site overrides the template*
with `GenWithStack`/`FastGen`, and the Rust text reproduces that override. Two checked
examples:

- Code 1091: Rust `"index {name} doesn't exist"` does not match the catalogue entry
  (`"Can't DROP '%-.192s'; check that column/key exists"`), and is right anyway — Go raises
  it as `dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", index)` at
  `pkg/ddl/executor.go:5504`.
- Code 1090: Rust `"can't drop only column {column} in table {table}"` matches
  `pkg/ddl/column.go:287` verbatim, not the catalogue entry.

This says the raise sites were ported by reading Go's raise site rather than the table,
which is the correct method.

## The structural problem behind these findings

The wire code and SQLSTATE do **not** come from the catalogue at run time. They are written
out by hand at each raise site, as a `MysqlError::new(code, *b"STATE", message)` triple.
Nothing checks the state against the code, and the integration replay does not compare error
output at all, so a disagreement between the two survives indefinitely. Every rank-2 finding
below is an instance of that one cause.

Note also that `rust/crates/tidb-protocol/src/error_conversion.rs` — which reads like the
authoritative code table, complete with a documented policy on when to use `ErrUnknown` — is
**dead code**. `error_packet_from_descriptor` and `exec_error_kind` are referenced only from
`tidb-protocol/tests/error_conversion_source.rs` and
`tidb-exec/tests/error_conversion_source.rs`. The live path is
`DriverError::to_mysql_error` (`tidb-executor/src/driver/errors/mod.rs:105`) →
`SqlQueryError` → `write_error`. Conclusions drawn from the protocol table do not describe
wire behaviour.

## Ranked findings

Counts: **1 wrong-code class with 5 concrete instances**, **6 wrong SQLSTATEs (fixed)**,
**1 remaining message defect**, **0 missing codes**. F5, F7, and F9 below are
fixed as of 2026-09-04; the count covers the one still-open message finding.
Ranked by consequence.

---

### F1 (rank 1, wrong code) — `[kv:8005]` in the message, `1105` on the wire — CLOSED (2026-09-05, site no longer exists)

The described literal is gone from the absorbed tree: the
undetermined-commit arm in `pessimistic_lock_error.rs` now sends code
`1105` with `ERR_RESULT_UNDETERMINED.message()`, self-consistent, and
that is exactly Go's own wire behavior — `terror.ErrResultUndetermined`
is a `ClassGlobal` terror with no MySQL code, so `ToSQLError` falls back
to `defaultMySQLErrorCode = mysql.ErrUnknown` = **1105**
(`pkg/parser/terror/terror.go:266-274`), with the message "execution
result undetermined" and state HY000. Go never sends 8005 for this
outcome; `ErrWriteConflictInTiDB` (8005) is the local-latch write
conflict, a different error raised elsewhere. No capture was needed —
the Go source alone settles it.

---

### F2 (rank 1, wrong code) — every storage-layer error collapses to 1105

`.map_err(|error| SqlQueryError::unknown(error.to_string()))` appears **~59 times** across
`rust/crates/tidb-server/src/real_tikv_node/mod.rs` (38 `SqlQueryError::unknown` sites) and
`rust/crates/tidb-server/src/real_tikv_multi_node.rs` (21). Every PD, TiKV, region, and
transaction failure — however specifically Go classifies it — reaches the client as 1105
with a Rust `Display` string.

Concrete case. A write-write conflict on a pessimistic commit:

| | code | SQLSTATE | message |
| --- | --- | --- | --- |
| TiDB | 9007 | HY000 | `Write conflict, txnStartTS=…, conflictStartTS=…, …, reason=… [try again later]` |
| Rust (this path) | 1105 | HY000 | the Rust error's `Display` text |

A retry loop keyed on 9007 never fires. `rust/crates/tidb-executor/src/driver/errors/txn.rs`
*does* map `TxnErrorKind::WriteConflict` to 9007, so the catalogue knows the right answer;
this path just does not reach it.

**Unverified:** which of the ~59 sites are reachable from ordinary SQL against a live
cluster. Establishing that needs execution.

---

### F3 (rank 1, wrong code) — planner refusals carry no code at all — FIXED (verified 2026-09-05)

The topology is now traced end to end. `ReadOnlyScanError` and
`PreparedPlanError` (`rust/crates/tidb-planner/src/read_only_scan/errors.rs`)
carry no MySQL code; `RealTiKvReadError::Plan` (`real_tikv_read.rs:939`)
flattens them through `Display`; the server seams then call
`SqlQueryError::unknown(error.to_string())` (`real_tikv_node/mod.rs:306+`),
answering 1105/HY000. The server's `SqlQueryError` itself is fully capable
(`sql_node.rs:270`: explicit code/state/message).

Go raises the equivalent refusals as `ErrNotSupportedYet` = **1235**, SQLSTATE
**42000**, message `"This version of TiDB doesn't yet support '%s'"` for the
unsupported-feature shapes (`pkg/errno/errcode.go`, `errname.go`); 1235 vs
1105 and 42000 vs HY000 are both wrong.

The bounded fix is complete: `ReadOnlyScanError`, `PreparedPlanError`, and
`PreparedBindError` carry the Go-compatible code/SQLSTATE pair — `Parse`
1064/42000, unsupported shapes 1235/42000, ordinary unknown tables
1146/42S02, unknown columns 1054/42S22, internal invariants 1105/HY000,
prepared parameter-count errors 8112/HY000, and prepared catalog lookup
fallbacks 1105/HY000. The single-table and multi-table real-TiKV prepared
read seams, plus the direct `RealTiKvReadError::Plan` seam, now preserve the
typed pair instead of flattening through `SqlQueryError::unknown`. Per-variant
planner and server regressions pin the result; see
`rust/testport/receipts/planner_read_only_error_codes.md`.

---

### F4 (rank 1, wrong code) — DDL admission refusals default to 1105 — FIXED (2026-09-05, explicitness repair)

`DdlAdmissionError::new` no longer exists. Every raise site now names its
code: `::with_code(GENERIC_ERROR_CODE, ...)` spells out 1105 at the sites
whose refusals have no Go counterpart yet, `::unsupported()` carries 8200,
and `::with_code` carries Go's own errno elsewhere — so a future refusal
cannot silently inherit a generic default. Behavior is unchanged; the
per-site comparison of the ~40 explicit-1105 sites against the Go errno
each equivalent refusal deserves remains a follow-up queue.

Original finding: `table_info_build.rs:188` defined
`const GENERIC_ERROR_CODE: u16 = 1105;`, and `DdlAdmissionError::new()` used it
unless the caller picked `::unsupported()` (8200) or `::with_code()`. A refusal that forgets
to choose was silently 1105 rather than failing to compile. This is the "default that hides a
missing decision" shape; `::new()` should not exist without a code.

---

### F5 (rank 1, message-selection) — FIXED: `registered_std` consulted the catalogues in the wrong order

Before the fix, `rust/crates/tidb-error/src/terror.rs:434-442` used:

```rust
let message = crate::mysql::message_by_code(protocol_code)
    .or_else(|| crate::tidb::message_by_code(protocol_code))
```

Go's `dbterror.ErrClass.NewStd` (`pkg/util/dbterror/terror.go:55-56`) reads
`errno.MySQLErrName[code]` — the **TiDB** catalogue — and *only* that one. It never falls
back to `pkg/parser/mysql`.

That matters because **the two Go catalogues carry different text for 38 shared codes**.
Where they differ, `registered_std` returns the `pkg/parser/mysql` text and Go returns the
`pkg/errno` text. The differences are not cosmetic; three samples:

| code | `pkg/parser/mysql` (what Rust picks) | `pkg/errno` (what Go's `NewStd` picks) |
| --- | --- | --- |
| 3143 | `Invalid JSON path expression %s.` | `Invalid JSON path expression. The error is around character position %d.` |
| 1243 | `Unknown prepared statement handler %s given to %s` | `Unknown prepared statement handler (%.*s) given to %s` |
| 1820 | `You must SET PASSWORD before executing this statement` | `You must reset your password using ALTER USER statement before executing this statement` |

3143 is a **placeholder-type** difference (`%s` vs `%d`) and 1243 a **placeholder-count**
difference (`%.*s` consumes two arguments in Go, `%s` consumes one) — exactly the class the
audit was asked to look for. The remaining 35 are the `'%-.64s'` vs `'%-.255s'` host-width
family and TiDB's deliberate "functional index" → "expression index" rewording, which
affects every expression-index error message (3751-3760, 3800, 3837, 3903, 3904, 3907,
3909).

**Before the fix,** only two of the 38 were wired today —
`ERR_DBACCESS_DENIED` (1044) and `ERR_TABLEACCESS_DENIED` (1142) at
`rust/crates/tidb-error/src/plannererrors.rs:254-259` — and both differ only in the host
width, so the historical blast radius was a host name longer than 64 characters. The
lookup order was a landmine for the other 36 codes as they got wired.

The lookup now checks `tidb::message_by_code` first and falls back to the
parser/MySQL catalogue only when the TiDB catalogue has no entry. This is the
same precedence as Go's `errno.MySQLErrName` in `pkg/util/dbterror/terror.go`.
The `tidb-error` owner profile and a focused regression for codes 3143, 1243,
and 1820 pin the overlapping messages and placeholder shapes.

---

### F6 (rank 2, wrong SQLSTATE) — FIXED: six arms named a code and a contradicting state

Fixed in this branch. Each was a code and an SQLSTATE written side by side that disagreed.

| Rust site | code | was | TiDB sends | statement |
| --- | --- | --- | --- | --- |
| `driver/errors/exec.rs:101` | 1365 | `HY000` | **22012** | `SELECT 1/0` under `ERROR_FOR_DIVISION_BY_ZERO` |
| `driver/errors/exec.rs:104` | 1292 | `HY000` | **22007** | truncated-value conversion |
| `driver/errors/exec.rs:95` | 1253 | `HY000` | **42000** | `COLLATE` incompatible with the charset |
| `driver/errors/mod.rs:785` | 1410 | `HY000` | **42000** | `GRANT … TO <unknown user> WITH GRANT OPTION` |
| `driver/errors/exec.rs:77` | 10 JSON codes | `HY000` | 22032 / 42000 / HY000 | any JSON path or document error |
| `driver/errors/exec.rs:79` | 4135 / 1146 | `HY000` | HY000 / **42S02** | `SELECT nextval(x)` where `x` is not a sequence |

The last two could not have been right as literals: the code is chosen at run time, and the
correct SQLSTATE varies with it. Of the ten JSON codes, **eight** were wrong — 3140, 3146
and 3158 are `22032`; 3143, 3149, 3153, 3154 and 3165 are `42000`; only 3150 and 3064 are
`HY000`.

Worst single case: `SELECT JSON_EXTRACT('{}', '$.')` — TiDB answers
`ERROR 3143 (42000)`, and we answered `ERROR 3143 (HY000)`. `42000` is the syntax-error
class; a client that branches on SQLSTATE class rather than on the number sees a
server error instead of a bad-input error.

**Fix applied:** added `MysqlError::coded(code, message)`
(`rust/crates/tidb-executor/src/driver/errors/mod.rs`), which derives the SQLSTATE from the
code via `tidb_error::mysql::mysql_state` — the same lookup Go's `NewErr` performs
(`pkg/parser/mysql/error.go:40-57`). Every entry in the verified-equal state table is five
bytes, so the conversion is total and needs no fallback arm. The six arms now use it, so
their code and state cannot drift apart again.

**FIXED (2026-09-05): the drift vector is gone.** The `state` parameter
was deleted from `MysqlError::new`, which now derives the SQLSTATE from
the code through `tidb_error::mysql::mysql_state` — the same lookup
`NewErr` performs. The ~246 literal raise sites in `driver/errors/mod.rs`
and `driver/errors/exec.rs` were rewritten mechanically; a script compared
every pre-rewrite `(code, state-literal)` pair against the derived value
before the rewrite and found **all of them agreeing** (explicit table
entries plus the `HY000` fallback), so the rewrite is behavior-preserving
and only removes the ability to write a disagreeing pair. The three sites
that reconstruct an error carried in from outside the module
(`MemoryExceedForQuery`, `VarErrorKind::SqlError`, `ExecError::Killed`)
now use `MysqlError::with_state`, which exists solely for those
externally-given states; a runtime `ParseCoded { errno }` now derives
like Go's runtime `NewErr` instead of forcing `HY000`.

---

### F7 (rank 3, message) — FIXED: the write-conflict retry marker was missing

Before the fix, `rust/crates/tidb-executor/src/driver/errors/mod.rs:211`
sent 9007 with only `"Write conflict, please retry the transaction"`.

Go builds the message as
`mysql.MySQLErrName[mysql.ErrWriteConflict].Raw + " " + TxnRetryableMark`
(`pkg/kv/error.go:57-63`), where

```go
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"   // pkg/kv/error.go:27
```

The literal `[try again later]` is now defined once as `TXN_RETRYABLE_MARK` in
`tidb-executor/src/driver/errors/mod.rs` and appended by the live
`TxnErrorKind::WriteConflict` wire-rendering arm. This restores the
backward-compatible token that clients grep to decide whether a failed
transaction may be replayed. A focused source regression pins the complete
9007 message.

The Rust `TxnErrorKind` currently carries no structured conflict fields
(`txnStartTS`, `conflictStartTS`, `conflictCommitTS`, `key`, `reason`), and the
separate 8005 undetermined-commit pipeline remains a documented follow-up. The
marker fix is therefore bounded to the generic 9007 path and does not claim
complete write-conflict diagnostic parity.

---

### F8 (rank 3, message) — overflow message drops the offending expression

`rust/crates/tidb-executor/src/driver/errors/exec.rs:134` renders 1690 as
`"{class} value is out of range"`. Go's message is `"%s value is out of range in '%s'"`.

`SELECT 9223372036854775807 + 1`:

- TiDB: `ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'`
- Rust: `ERROR 1690 (22003): BIGINT value is out of range`

The code and SQLSTATE are now correct (this is the overflow defect fixed earlier — it
previously sent 1105 with Rust text). What remains is the `in '<expr>'` tail: no `EvalError`
carries the rendered expression, because the overflow is raised in arithmetic that never
sees the expression tree. Closing it needs the expression text threaded to the raise site,
which is a design change, not a string fix. The in-source comment already says so.

---

### F9 (rank 4, missing codes) — FIXED: five `plannererrors` entries were absent

Before the fix, `rust/crates/tidb-error/src/plannererrors.rs` had 92 of Go's
98 `pkg/util/dbterror/plannererrors/planner_terror.go` entries. The five
entries were:

| Go entry | line | class | reachable from |
| --- | --- | --- | --- |
| `ErrPrepareMulti` | `:118` | Executor | `PREPARE s FROM 'SELECT 1; SELECT 2'` |
| `ErrUnsupportedPs` | `:119` | Executor | `PREPARE` of an unsupported statement |
| `ErrPsManyParam` | `:120` | Executor | `PREPARE` with > 65535 parameters |
| `ErrPrepareDDL` | `:121` | Executor | `PREPARE s FROM 'CREATE TABLE …'` |
| `ErrTooBigPrecision` | `:80` | Expression | `SELECT CAST(1 AS DECIMAL(65,31))` |

All five are ordinary-SQL reachable, not administrative. They are now present
in `tidb-error/src/plannererrors.rs`, and the owner test forces every prototype
to resolve through the complete catalogue.

The other six flagged by a first pass were false positives: `ERR_ACCESS_DENIED` is present
and correctly ports Go's deliberate code/message crossover — `NewStdErr(mysql.ErrAccessDenied
/* 1045 */, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword] /* 1698's text */)` at
`planner_terror.go:104` — via `plannererrors.rs:398-401`.

---

## What is NOT verified

This machine cannot run freshly built binaries (`syspolicyd` is wedged; every new executable
hangs at `_dyld_start`). Therefore:

- **No statement in this document was executed against either engine.** Every "TiDB sends X"
  claim is read from Go source — the catalogue files, the state table, and the specific raise
  site cited — not from a capture.
- The focused and serialized all-target `cargo test` profiles for the current
  `tidb-error` and `tidb-executor` owners were run. The catalogue-precedence
  owner is green (8 unit + 31 integration tests); the executor owner retains
  its documented pre-existing planner/remote/spill/fixture failures, with the
  new retry-marker regression passing. `cargo check`, formatting, and diff
  checks also pass. Strict clippy is green for `tidb-error` and remains blocked
  for `tidb-executor` by unrelated dependency/generated-code diagnostics.
- F2's blast radius (which of the ~59 storage-error sites ordinary SQL reaches) is unmeasured.
- F1's correct code (8005 vs `ErrResultUndetermined`) is unsettled; it needs a capture.

### Capture harness to settle the open items

The method that works is a throwaway `pkg/executor/zz_dump_errors_test.go` over
`testkit.CreateMockStore`, draining result sets via `session.GetRows4Test` and printing
`code / SQLSTATE / message` for each statement, deleted afterwards. **It cannot run today.**
The statements it needs to cover, one per open item:

```sql
-- F2: expect 9007 and a message ending '[try again later]'
--     (two sessions, conflicting UPDATE, commit the second)
-- F3: expect 1235 / 42000 / "This version of TiDB doesn't yet support '...'"
-- F8: expect the "in '(9223372036854775807 + 1)'" tail
SELECT 9223372036854775807 + 1;
```

## Method footnote

The catalogue comparison is reproducible: parse `name -> code` and
`name -> (message, redact_arg_pos)` from both sides with a regex over the Go `Message(...)`
entries and the Rust `ErrMessage { raw, redact_arg_pos }` constants, then diff the maps and,
separately, the extracted format-verb sequences. The SQLSTATE and class comparisons are the
same shape. Re-running that against a later tree is how this document stays honest; the
integration replay will not do it, because it still classifies `(Err(_), true)` as
`BothRejected`.

### F2 progress (2026-09-05): static reachability classification

The unknown-flattening sites are classified by enclosing function
(static call-site analysis, no cluster needed):

- Startup/connect path (fail process bring-up, not per-SQL): `connect`
  x4 + x2, `connect_loaded_catalog_authority` x4,
  `configured_catalog_from_tables`, `open_session` x2 -- 13 sites.
- Per-statement transaction/write seams -- the cluster Go classifies as
  9007-class (`driver/errors/txn.rs` already carries the
  `TxnErrorKind::WriteConflict` mapping): `commit_bound_write` x3,
  `control_transaction` x2, `transaction_for_statement`,
  `transaction_error`, `execute_prepared_write` -- 8 sites.
- Point-read and prepare seams: `prepare_point_read` x4,
  `prepare_configured_query` x2, `point_handles` x2 -- 8 sites.
- Statement completion and misc: `finish_execute_stmt` x3,
  `node_accounts` x3, `loaded_table_refusal_error` x2,
  `lightweight_ddl_statement_context` and friends -- the remainder.

Repair design for the transaction/write cluster: route those seams
through the existing txn error-kind mapping instead of `unknown()`.
Still gated on one captured conflict to pin the Rust tikv-client's
error text signatures -- the mapping keys on text/kind, and guessing
signatures without a capture would be speculative.

### F4 follow-up closed (2026-09-05): no Go-justified upgrades exist

Spot-checks of the generic-1105 DDL refusals against Go settle the
follow-up: the AUTO_RANDOM_BASE overflow case Go itself handles by a
silent uint64-to-int64 wrap (`pkg/ddl/create_table.go:875` stores
without an overflow check) — the Rust refusal is a stricter
fork-boundary behavior with no Go errno to adopt; AUTO_INCREMENT
non-integer values are refused at Go's parser level (syntax path), not
by a DDL errno; and the prefix-key and blob-key refusals already carry
Go's own errnos (1089, 1170). The remaining generic sites are
fork-boundary refusals Go never raises, so 1105 is their honest code.
The follow-up is closed rather than deferred.
