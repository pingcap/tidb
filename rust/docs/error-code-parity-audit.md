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
**3 message defects**, **5 missing codes**. Ranked by consequence.

---

### F1 (rank 1, wrong code) — `[kv:8005]` in the message, `1105` on the wire

`rust/crates/tidb-exec/src/pessimistic_lock_error.rs:120-127` builds an undetermined-commit
error whose **message text declares `[kv:8005]`** while the **code field is `1105`**. The
two disagree inside a single error value, so a client that parses the class prefix and a
client that reads the code number get different answers from the same packet.

Go: `pkg/kv/error.go` defines `ErrWriteConflictInTiDB` as `mysql.ErrWriteConflictInTiDB`
= **8005**. A commit whose outcome is undetermined is `terror.ErrResultUndetermined`, not
1105.

This one is unambiguous — the code that the message already names is the code that should be
in the field. I did not change it because `tidb-exec` is a second, largely test-facing
pipeline (see above) and the correct choice between 8005 and `ErrResultUndetermined` needs a
capture to settle.

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

### F3 (rank 1, wrong code) — planner refusals carry no code at all

`rust/crates/tidb-planner/src/read_only_scan/errors.rs` defines `ReadOnlyScanError`,
`UnsupportedReadOnlyFeature` (23 variants) and `UnsupportedReadOnlyPredicate` with **no
MySQL code field**. They surface through `SqlQueryError::unknown` as 1105.

Go raises `ErrNotSupportedYet` = **1235**, SQLSTATE **42000**, message
`"This version of TiDB doesn't yet support '%s'"` for the equivalent refusals
(`pkg/errno/errcode.go`, `errname.go`). 1235 vs 1105 and 42000 vs HY000 are both wrong, and
`ErrorKind::NotSupportedYet` already exists in the (dead) protocol table, so the intent was
recorded and never wired.

---

### F4 (rank 1, wrong code) — DDL admission refusals default to 1105

`rust/crates/tidb-exec/src/table_info_build.rs:188` defines
`const GENERIC_ERROR_CODE: u16 = 1105;`, and `DdlAdmissionError::new()` (`:119-125`) uses it
unless the caller picks `::unsupported()` (8200) or `::with_code()`. A refusal that forgets
to choose is silently 1105 rather than failing to compile. This is the "default that hides a
missing decision" shape; `::new()` should not exist without a code.

---

### F5 (rank 1, message-selection) — `registered_std` consults the catalogues in the wrong order

`rust/crates/tidb-error/src/terror.rs:434-442`:

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

**Currently reachable:** only two of the 38 are wired today —
`ERR_DBACCESS_DENIED` (1044) and `ERR_TABLEACCESS_DENIED` (1142) at
`rust/crates/tidb-error/src/plannererrors.rs:254-259` — and both differ only in the host
width, so today's blast radius is a host name longer than 64 characters. The lookup order is
still wrong, and it is a landmine for the other 36 codes as they get wired.

**Not fixed here:** swapping the order is a one-line change, but it silently re-renders every
existing `registered_std` message, and I cannot run a test to confirm nothing depends on the
current text. It needs an owner who can execute.

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

**Not fixed:** the other ~169 hand-written `(code, state)` pairs. All 169 are currently
*correct* — I checked each mechanically against Go's table — but they remain able to drift.
The real repair is to delete the `state` parameter from `MysqlError::new` so the pair cannot
be written down at all; that is a ~175-site change across crates other agents hold, so it
belongs to a dedicated unit.

---

### F7 (rank 3, message) — the write-conflict retry marker is missing

`rust/crates/tidb-executor/src/driver/errors/mod.rs:131` sends 9007 with
`"Write conflict, please retry the transaction"`.

Go builds the message as
`mysql.MySQLErrName[mysql.ErrWriteConflict].Raw + " " + TxnRetryableMark`
(`pkg/kv/error.go:57-63`), where

```go
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"   // pkg/kv/error.go:27
```

The literal `[try again later]` **does not appear anywhere in the Rust tree**. Go's own
comment states that this string is a compatibility contract; it is the token a client greps
to decide whether a failed transaction may be replayed. We also drop every structured field
(`txnStartTS`, `conflictStartTS`, `conflictCommitTS`, `key`, `reason`), which is what an
operator uses to identify the contending transaction.

The same omission applies to 8005 (`ErrWriteConflictInTiDB`), which Go also suffixes with the
mark.

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

### F9 (rank 4, missing codes) — five `plannererrors` entries are absent

`rust/crates/tidb-error/src/plannererrors.rs` has 92 of Go's 98
`pkg/util/dbterror/plannererrors/planner_terror.go` entries. Missing:

| Go entry | line | class | reachable from |
| --- | --- | --- | --- |
| `ErrPrepareMulti` | `:118` | Executor | `PREPARE s FROM 'SELECT 1; SELECT 2'` |
| `ErrUnsupportedPs` | `:119` | Executor | `PREPARE` of an unsupported statement |
| `ErrPsManyParam` | `:120` | Executor | `PREPARE` with > 65535 parameters |
| `ErrPrepareDDL` | `:121` | Executor | `PREPARE s FROM 'CREATE TABLE …'` |
| `ErrTooBigPrecision` | `:80` | Expression | `SELECT CAST(1 AS DECIMAL(65,31))` |

All five are ordinary-SQL reachable, not administrative. They are absent rather than wrong,
so they rank last — but each is a statement where we cannot currently produce TiDB's code.

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
- `cargo check`, `cargo clippy --all-targets` and `cargo fmt --all --check` were run and are
  clean (exit 0) for the changed crate. **`cargo test` was not run.** The one test I edited
  (`driver/errors/exec.rs`, the 1365 assertion, `HY000` → `22012`) is asserted-by-reading,
  not by running.
- F2's blast radius (which of the ~59 storage-error sites ordinary SQL reaches) is unmeasured.
- F1's correct code (8005 vs `ErrResultUndetermined`) is unsettled; it needs a capture.

### Capture harness to settle the open items

The method that works is a throwaway `pkg/executor/zz_dump_errors_test.go` over
`testkit.CreateMockStore`, draining result sets via `session.GetRows4Test` and printing
`code / SQLSTATE / message` for each statement, deleted afterwards. **It cannot run today.**
The statements it needs to cover, one per open item:

```sql
-- F5: does NewStd pick errno's text?  expect the '%d character position' wording
SELECT JSON_EXTRACT('{"a":1}', '$.');
-- F2: expect 9007 and a message ending '[try again later]'
--     (two sessions, conflicting UPDATE, commit the second)
-- F3: expect 1235 / 42000 / "This version of TiDB doesn't yet support '...'"
-- F8: expect the "in '(9223372036854775807 + 1)'" tail
SELECT 9223372036854775807 + 1;
-- F6 regression, now fixed: expect 3143 / 42000, and 1365 / 22012
SELECT 1/0;
```

## Method footnote

The catalogue comparison is reproducible: parse `name -> code` and
`name -> (message, redact_arg_pos)` from both sides with a regex over the Go `Message(...)`
entries and the Rust `ErrMessage { raw, redact_arg_pos }` constants, then diff the maps and,
separately, the extracted format-verb sequences. The SQLSTATE and class comparisons are the
same shape. Re-running that against a later tree is how this document stays honest; the
integration replay will not do it, because it still classifies `(Err(_), true)` as
`BothRejected`.
