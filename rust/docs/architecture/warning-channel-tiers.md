# The warning channel: three stacks, not one fragmented channel

Measured at `e48b552bd0`. This is a measurement record, not a refactor plan.
Sites named below were the state at that commit; the four commits that follow
it changed Stack A as recorded in "What was unified".

## Verdict: DO NOT COLLAPSE

The nine warning emission sites are **not nine doors onto one channel**. They
are three independent stacks plus one dead decision point. Only one stack
reaches a client. Unifying them would fuse a live accumulator with two
bounded-tier ports, which is the mistake `#122` avoided with the five
statement classifiers.

What the measurement *did* find is worse than fragmentation: two of the three
stacks emit into buffers that **nothing on the live path drains**, and the one
piece Go actually centralizes — the error-vs-warning decision — is the piece
with zero production callers.

## Stack A — LIVE. The only route to a client.

| Site | Role |
| --- | --- |
| `tidb-executor/src/stmt_context.rs:40,562,661,757` | per-statement **evaluation accumulator**, `Rc<RefCell<Vec<(u16, String)>>>` |
| `tidb-session/src/warnings.rs:142` `drain_eval_warnings` | **transfer**, executor accumulator -> session buffer |
| `tidb-session/src/warnings.rs:98` `warning_output` | **publisher**, renders `SHOW WARNINGS` / `SHOW ERRORS` / `SHOW COUNT(*) WARNINGS` |
| `tidb-session/src/variables.rs:380` `warn_truncated_var` | direct push into the session buffer |
| `tidb-session/src/variables.rs:314` `warn_removed_feature_var` | direct push into the session buffer |
| `tidb-session/src/lib.rs:475` (in `run_with_columns`) | files a FAILED statement's own error as an `Error` row |
| `tidb-session/src/noop.rs:86,129` | the two `tidb_enable_noop_functions` 1235 arms |
| `tidb-session/src/account.rs:1042` | 3929, an unregistered dynamic privilege |
| `tidb-session/src/dispatch.rs:550,559` | discarded `CHECK` (1105) and `LINEAR HASH` (8200) |

The brief listed nine sites across four crates. The count is right and the
distribution is wrong: **seven of the appending sites are in `tidb-session`
alone**, and `lib.rs:475` -- the one that decides a failed statement leaves its
own error behind -- was not on the list at all.

`drain_eval_warnings` is called from six dispatch arms
(`tidb-session/src/dispatch.rs:263,372,385,395,429,443`) and
`explain_arm.rs:119`.

The two `variables.rs` sites bypass the executor accumulator because the sysvar
assignment path never builds a `StmtContext`. That is not a fragmentation bug;
it is the correct shape for a statement that has no evaluation phase. Go does
the same thing — `SetSessionSystemVar` reaches `StmtCtx.AppendWarning` directly,
not through a type-conversion context.

## Stack B — BOUNDED. `tidb-exec`, source-faithful `WarnHandler` port.

| Site | Role |
| --- | --- |
| `tidb-exec/src/warning_publication.rs:53` `StaticWarningHandler` | source `StaticWarnHandler`: mutable sink with the `MaxUint16` cap |
| `tidb-exec/src/warning_publication.rs:175` `IgnoreWarnings` | source `ignoreWarn`: no-op sink |
| `tidb-exec/src/warning_publication.rs:286` `WarningPublication` | **read-only borrowed view**, no storage of its own |
| `tidb-exec/src/statement_status.rs:114,262` | per-statement **status owner**; warnings are one field beside affected-rows and counters |

`tidb-session/Cargo.toml` does **not** depend on `tidb-exec`. This stack is
reachable only from `tidb-exec`, its own tests, and the `ReadOnlyScanPlan`
bounded proof tier documented in `read-tier-boundary.md`.

## Stack C — BOUNDED, and unwired past its own crate.

| Site | Role |
| --- | --- |
| `tidb-distsql/src/warning.rs:70` `WarningCollector` | `Arc<Mutex<Vec<Warning>>>` sink shared across `Detach` |
| `tidb-distsql/src/context.rs:89,184` | `DistSQLContext.warning_handler`, the field that survives detach |
| `tidb-distsql/src/response_channel.rs:794,905` | **transport decoder**: TiKV `SelectResponse` error values -> `Warning` |

`tidb-executor` depends on `tidb-distsql`, but every import is a range helper
(`handle_range.rs:82`, `kv_table/table_scan.rs:216`, `remote_scan.rs:272`
comment only). The live executor never constructs a `DistSQLContext` and never
reads a `WarningCollector`.

**Consequence:** a warning TiKV reports in a coprocessor `SelectResponse` on the
live `cluster_session` read path has nowhere to go. `response_channel.rs`
decodes it correctly into a buffer that the live path does not own.

## The dead decision point — `tidb-datatype` `ConversionContext`

`ConversionContext::handle_truncate` (`truncate.rs:77`) is the Rust image of
Go's error-vs-warning decision: `TypeFlags.TruncateAsWarning` deciding whether a
truncation is returned as an error or diverted to the `WarnAppender`. It is the
single most load-bearing piece of Go's design — the reason one channel works in
Go is that the *decision*, not just the storage, is centralized.

Production callers of `handle_truncate`: **one**, `binary_literal.rs:149`, inside
`BinaryLiteral::to_int_with_context`.

Production callers of `to_int_with_context`: **zero**. Every caller is a test
(`tidb-datatype/tests/value_context_format_source.rs:61`,
`tests/conversion_context_source.rs:137,153,162`).

Production implementors of `ConversionWarningAppender`: `StaticWarningHandler`
and `IgnoreWarnings` in the bounded Stack B, plus the no-op
`IgnoreConversionWarnings`. No live-path type implements it.

So the decision point is wired only to a bounded sink, and reached only by
tests. This is the same shape as the seven earlier "present but unwired" cases,
including `Validated{truncated}` — the precursor that forced
`warn_truncated_var` into existence.

## What was unified, and what was deliberately left alone

**Unified:** the seven `tidb-session` sites, behind `Session::append_warning`
(`warnings.rs`). That is the door the three queued units (#150, #153, #154)
would otherwise have become the eighth, ninth and tenth caller of. The pure
move landed on its own; the rule it enabled — Go's `MaxUint16` retention
limit — landed in the commit after it with tests on both buffers.

**Left separate, deliberately:** Stack B and Stack C. Their pieces are a
mutable sink, a no-op sink, a borrowed read-only view, a per-statement status
owner, a detach-shared `Arc` sink, and a transport decoder. Those are six
roles, not six copies, and no dependency edge exists to merge them across.
Collapsing them is `#122`'s five-classifier mistake with a wider blast radius.

**Left separate, reluctantly:** `MAX_WARNING_COUNT` now exists twice —
`tidb-executor` for the live path, `tidb-exec` for the bounded one. The
session cannot see `tidb-exec`, so this is the honest spelling until the two
stacks are on one dependency edge.

## The axis that would pay next

Go's design works because the *decision* is centralized, not just the storage:
`TypeFlags.TruncateAsWarning` decides error-vs-warning once. The live path has
no such decision point — `ConversionContext::handle_truncate` is it, and it is
dead. Wiring it is **behaviour-bearing** and needs a capture, so it is its own
unit.

## Bugs the audit found

1. **Fixed** — `reports_warnings` decided warning-buffer inheritance by
   sniffing the raw SQL for `SHOW ... WARNINGS`/`ERRORS`. Both are UNRESERVED
   keywords, so `SHOW CREATE TABLE warnings` and `SHOW COLUMNS FROM warnings`
   took the inherit branch and the next `SHOW WARNINGS` reported a statement
   two back. Go decides on the parsed node (`ResetContextOfStmt`). The module
   doc justified the sniff by the cost of a second parse — stale since the
   parse-once refactor put the node one line below.
2. **Fixed** — `ShowSessionStates` was missing from the inheriting set that Go
   spells with exactly three entries. Unreachable today (`SHOW SESSION_STATES`
   is refused) but a silent trap for whoever admits it.
3. **Fixed** — no retention limit on either live buffer, where the
   source-faithful port has had Go's `MaxUint16` since it was written. A
   non-strict bulk write allocated one `String` per converted value, unbounded.
4. **Open, own unit** — the wire never reports a warning count.
   `ResultSetOptions.warnings` is the literal `0` at every construction site in
   `tidb-server/src/mysql_connection.rs` (`461`, `684`, `1414`, `1435`,
   `1582`), and `OkPacket::default()` supplies `0` for the rest. Go sends
   `cc.ctx.WarningCount()` in both `writeOK` (`pkg/server/conn.go:1692`) and
   `writeEOF` (`:1779`). `SHOW WARNINGS` works over the wire because it is a
   result set; the *count* channel that the `mysql` CLI prints and JDBC uses to
   decide whether to build a `SQLWarning` chain is dead.
5. **Open, own unit** — the live `WarningLevel` has two variants (`Warning`,
   `Error`). Go and both bounded stacks have three. Nothing on the live path
   can produce a `Note`, and `drain_eval_warnings` stamps `Warning` on
   everything the executor recorded, since the accumulator carries no level.

## Mutation probe

Neutering the single live route — `drain_eval_warnings`'s body, keeping
`take_warnings()` so the accumulator still drains — fails **19** of
`tidb-session`'s 730 tests, and every one is a warning observation. The first
failure is `tests_zero_date::empty_sql_mode_warns_and_stores_the_zero_date`,
panicking `left: []` against the expected `1292` entry *before* it reaches its
stored-value assertion. The remaining **710 pass**, including every
stored-value test that does not also assert warnings.

That is the asymmetry the route is supposed to have: cutting it stops warnings
reaching the client and leaves the stored values correct. Control, in the
other direction: the `reports_warnings` fix's regression test fails on the
`SHOW CREATE TABLE warnings` probe (`left: 1, right: 0`) with the old
string-sniff decision restored, and passes with the parsed-node one.
