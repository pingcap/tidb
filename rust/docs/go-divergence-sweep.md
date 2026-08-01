# The Go↔Rust divergence sweep: what the pattern is

Before 2026-08-02 every divergence in this project was found one of two ways: a
live cluster disagreed, or someone chased a single visible symptom. No one had
ever read the two implementations side by side.

This document is the index over the per-surface inventories, and — more usefully
— the **shape** the findings keep taking. The individual bugs are in the linked
documents. What follows is the part that predicts where the next one is.

## The inventories

| Surface | Document | Result |
| --- | --- | --- |
| Encoding (`tablecodec`, `codec`) | [codec-divergence-inventory.md](codec-divergence-inventory.md) | 27 findings, 10 rank‑1 |
| Types (`pkg/types`) | [types-datatype-divergence-audit.md](types-datatype-divergence-audit.md) | 31 findings, 16 rank‑1 |
| Wire protocol (`pkg/server`) | [wire-protocol-divergence.md](wire-protocol-divergence.md) | 12 findings, 2 rank‑1 |
| Sysvars / stmtctx | [architecture/sysvar-and-stmtctx-divergence.md](architecture/sysvar-and-stmtctx-divergence.md) | 14 findings; **948/948 names, 0 declarative divergences** |
| Error catalogue (`pkg/errno`) | [error-code-parity.md](error-code-parity.md) | 15 findings; **1166 codes + 1164 messages + 244 SQLSTATEs equal** |
| Coprocessor (`pkg/distsql`, `ToPB`) | [distsql-coprocessor-parity.md](distsql-coprocessor-parity.md) | 6 findings; **all 52 `ScalarFuncSig` numbers equal** |

Catalog model JSON, error catalogue, coprocessor pushdown, builtin expressions,
2PC, chunk+stats, charset/collation, JSON, planner rules, DDL and the parser
were dispatched in the same sweep.

## Six shapes, and they recur

### 1. An instrument that cannot fail for the bug it exists to catch

The strongest single finding of the sweep is not a bug. All fifteen row‑v2 tests
are **self‑round‑trips** — encode with our encoder, decode with our decoder.
Encode and decode are each other's inverse, so all fifteen pass *while producing
bytes TiDB never writes*. They hid a rank‑1 corruption.

The same shape, elsewhere:

- the integration replay compares **rejected‑vs‑accepted**, never error text, so
  a wrong code, a wrong message and Rust `Debug` on the wire all pass;
- it observes warnings on **28 of 4,906** statements, and the hole is in the
  *recording*, so no reader change can close it;
- `EXPLAIN` in this tier never prints `cop[tikv]`, so pushdown is structurally
  unobservable from plan text;
- the mock's snapshot is a clone of the committed store, so it **cannot express
  a read‑ts/write‑ts split** — which is why a silent lost update lived for weeks;
- decimal fixtures all enter through the *parse* path, where Go normalises too,
  so negative zero diverges only when a value arrives **as bytes**.

**The lesson is about fixture style, not fixture count.** A test that consults
only our own code proves internal consistency, which is exactly the property
that cannot detect divergence from Go. Where a surface talks to TiKV or to a
client, the fixture has to come from Go.

### 2. Present but unwired — the most common shape by a wide margin

A component exists, is **correct**, and nothing calls it. The coprocessor audit
found three in one surface and changed no code, because in every case the port
was already right:

- `statement_pushdown.rs` computes `DAGRequest.flags` bit‑for‑bit correctly and
  has **no production caller** — both live paths pass a literal `0`, where Go
  sends `482` for a plain `SELECT`. The crate's own tests pass `32`, so they
  never see production's value.
- `SetFromSessionVars` — a faithful port, uncalled. Isolation level, replica
  read, priority, resource‑group tag and `MaxExecutionTime` all ship at defaults.
- TiKV's warnings are appended correctly, in Go's exact order, into a collector
  **every production site constructs fresh**, so `SHOW WARNINGS` is empty.

Elsewhere: `has_instance_scope` (no caller — all 28 instance sysvars unsettable
in both directions), nine sysvars accepted and stored but never read,
`decode_index_kv` and its restored‑data helpers (no callers, no tests — the
index read path has never run), `ResultEncoder` (nothing constructs it, so
`@@character_set_results` has zero wire effect), `error_conversion.rs` (reads
like the authoritative code table, referenced only from tests),
`isolation_state.rs` and `UNSUPPORTED_ISOLATION_LEVEL`.

**Grep for callers before believing a port is live.** "Ported" and "reachable"
are different claims and this project keeps conflating them. Note also that
`cargo check` **structurally cannot see a missing call** — this class is
invisible to every gate we have, and one live query would catch any of them.

**Two of these mask each other**, which is the reason to fix them together:
flags `0` is TiKV's *strictest* branch, so a truncation that TiDB degrades to a
warning currently makes the region raise. Fix the flags alone and the query
stops failing — and starts silently truncating with no warning at all, because
the warning channel is the other unwired one. That is worse than the bug.

### 3. Two implementations of one thing, and the wrong one is live

Two decimals — a faithful `MyDecimal` port and a digit‑string reimplementation —
and `Datum::Decimal` holds the **reimplementation**; eight of ten decimal
findings are against it. Two `STR_TO_DATE`s, where the expression copy never
calls the other. Previously: two `CREATE TABLE` builders (unified), and two
string‑to‑int scanners where the second had no diagnostic channel at all.

**A reimplementation diverges wherever Go's behaviour is arbitrary rather than
principled** — and MySQL compatibility is mostly arbitrary. Any claim that
something is ported must name *which* copy the value path uses.

### 4. Dropped context

Go threads a `Context` carrying TypeFlags, timezone and a warning sink into every
conversion. Several Rust entry points dropped it and hardcoded what it carried.
`Datum::compare` has **no warning sink at all**. The tell that this is threading
and not logic: two Rust paths disagree about `CAST(TIME ... AS SIGNED)`, and the
one that is right is the one that still has the context.

### 5. Accept, then discard

We accept a shape and then ignore it: `NONCLUSTERED`, `FORCE INDEX`, an
inapplicable hint, `SET transaction_isolation='SERIALIZABLE'` (Go errors 8048),
`max_allowed_packet` (stored unrounded, hardcoded at the wire). Each tells the
client yes and then does something else. **Refusing is better than accepting and
ignoring**, because a refusal is visible.

### 6. A doc comment asserting the opposite of the code

`commit_staged_buffer` claimed its fresh timestamp was "exactly as Go's implicit
per-statement transaction does". Go takes **one** timestamp; the comment sat on
top of silent data loss. The meta‑key doc described an eight‑byte type flag as a
single byte. Both were confident and wrong in the crate whose correctness they
described.

## What the sweep also proved equal

Worth as much as the findings, because it says where not to look: 948 sysvar
names with zero set difference and zero declarative divergences; 223 collation
ids, 41 charset defaults and 244 SQLSTATE overrides machine‑diffed clean; an
841‑cell `fieldTypeMergeRules` table diffed to zero; packet framing, the >16 MiB
split, sequence‑id reset points and the binary null bitmap; `compare.go`,
`truncate.go` and `context.go::Flags` in full.

## The standing caveat

Every finding here was derived by **reading** both implementations. Nothing was
executed: `syspolicyd` is wedged on this machine and no freshly built binary can
run. Fixes landed during the sweep are compile‑and‑lint verified only, and must
be gated the moment that clears.
