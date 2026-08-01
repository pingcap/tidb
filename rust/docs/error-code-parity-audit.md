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

## Ranked findings

See the sections below. Findings are ranked by consequence: wrong code first, then wrong
SQLSTATE, then wrong/malformed message, then a code we never raise.

<!-- populated below -->
