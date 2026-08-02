# The plan that comes out of the sweep

Fifteen surfaces were read against Go on 2026-08-02. This is what to do about
it, in order, and why that order.

The findings are in [go-divergence-sweep.md](go-divergence-sweep.md) and the
per-surface inventories it links. This document is only the sequencing.

## Read this correction first

The ordering below was written ranked by **consequence**, and that is the wrong
axis on its own. Reviewed against the full picture, three things were missing,
and the third changes the order.

**1. It is a bug-fix plan, not a project plan.** The goal is a faithful, running
TiDB advanced along the deployment ladder. The ladder currently reaches **rung 7
green** — sysbench runs unmodified, the Rust-vs-Go checksum matches on the same
TiKV, rung 6 is eight of eight. Nothing below says what advances it to rung 8,
and a plan that spends a month on correctness without moving the ladder has
chosen for the reader without telling them.

**2. There is no sizing.** #191 (decide the decimal representation) may be weeks;
#196 (identifier case) may be an afternoon once the one-line check settles it.
Ranked by consequence alone, they sit in the same phase. A plan that cannot be
scheduled is a list.

**3. Consequence is not reachability, and that reorders things.** Every finding
below was ranked by *how bad it is if hit*. Nothing was ranked by *how often it
is hit*, and the difference is large:

- **#186 fires on ordinary traffic.** Every JDBC client with
  `useLocalTransactionState=true` reads a status bit we always set wrong, on
  every transaction. Silent application-level data loss, today, with no unusual
  input.
- **#188 fires on every decimal column** — an ordinary schema choice.
- **#202 fires on `ALTER TABLE ... ADD COLUMN ... FIRST`** — a common migration.
- **#189 needs a negative zero with non-zero scale arriving as bytes.** Nearly
  nobody writes one.
- **#196 needs a non-ASCII identifier** whose simple and full case mappings
  differ — Greek final sigma, Turkish dotted I.
- **JSON u64** needs a literal past `i64::MAX`.

So the honest order is **consequence × reachability**: #186, #188 and #202 are
the ones costing real users real data right now; #189, #196 and the JSON u64
case are real bugs that a fixture should pin and that can wait behind them.

**4. Nothing here has been triaged for false positives.** About 170 findings are
derived from reading two sources, and this project's premises have been
overturned repeatedly — including twice during this sweep, where a unit
falsified my account of *which code produced a warning* and of *whether an
access path was at risk*. Assume a non-zero false-positive rate. **Confirming a
finding is cheaper than fixing it**, and for several the confirmation is one
command.

## The one fact that sets the order

**About 26 fixes landed with nothing executed**, because the machine's disk was
full for most of the sweep and no freshly built binary could run. Roughly 170
further findings are written up and unverified. So the first question is not
"which bug is worst" — it is **"can we tell a fix from a regression yet?"**

For most of these surfaces the answer is still no, and that is itself a finding:
the row‑v2 suite is fifteen self‑round‑trips, the replay never reads error text,
`EXPLAIN` cannot print `cop[tikv]`, and the mock cannot express a
read‑ts/write‑ts split. **Building the oracles is not preparation for the work.
It is the first phase of the work.**

## Phase 0 — Verify what already landed

Gate the sweep's commits, push, then run the deployment ladder. Until this is
done every number in every inventory is a reading, not a measurement.

Specifically re-check the fixes that touched bytes, because they are the ones a
green compile says nothing about: common‑handle padding, the unsigned hash int
tag, `ascii_bin`/`latin1_bin` trailing‑space trimming, the JSON u64 type code,
and `NeedRestoredDataWithCollate`. Confirm nothing in `table_key_source.rs` or
`table_row_key_source.rs` depended on the old unpadded form.

## Phase 1 — Build the missing oracles

Nothing downstream is trustworthy without these, and each one is small next to
what it unblocks.

1. **Go‑produced byte fixtures** for row v2, index keys, the JSON binary format
   and collation sort keys, checked in with the Go program that produced them.
   This is the single highest‑leverage item in the plan: it is what makes the
   corruption tier *checkable*, and one `DECIMAL(10,4)` vector alone catches a
   rank‑1 that fifteen green tests miss.
2. **Run the DDL refusal capture harness.** It already exists at
   `rust/difftests/tools/ddl_refusal_capture/` with twenty case blocks, marked
   UNRUN. The replay can confirm we now refuse; only this can confirm we refuse
   with Go's code and message.
3. **A wire‑level capture** for the protocol findings — status flags, long data,
   charset results. No SQL‑level test can see any of them.

## Phase 2 — Storage corruption, in dependency order

These persist. A wrong query answer is wrong once; a wrong byte is wrong forever
and is read by every node, including nodes that were not running when we wrote
it.

**Decide the decimal representation first** (#191). `Datum::Decimal` holds a
digit‑string reimplementation while a faithful `MyDecimal` port sits beside it
unused, and eight of ten decimal findings are against the reimplementation. Both
#188 (declared precision not carried) and #189 (negative zero unrepresentable)
are downstream of it. Fixing them against the reimplementation is work that gets
thrown away if the value path moves.

Then, independently of each other:

- **#202 — name‑key the column references.** Generated‑column expressions,
  partition expressions and FK columns address columns by offset; three mutators
  shift offsets and none remaps them. Go is name‑keyed and cannot have this bug.
  Decide whether the compiled expression should hold names and resolve at
  evaluation time; remapping the three mutators leaves the next one free to
  forget.
- **#196 — identifier case mapping.** Run the one‑line comparison first
  (`strings.ToLower` versus `to_lowercase` on `ΟΔΟΣ`); the claim is derived from
  spec reading, not observation, and a rank‑1 deserves the ten seconds. If it
  holds, the scope is every `to_lowercase()` on an identifier, not just
  `CiString`. #203's `Ä`/`ä` finding is the same question from the DDL side.
- **#197 — preserve unknown enum values.** `index_type` and partition type
  collapse to 0; five AST enums hard‑fail. Go keeps the raw int and says why.
  Decide the policy once for all seven.

## Phase 3 — What clients see

- **#186 status flags.** Every OK packet says AUTOCOMMIT; Connector/J with
  `useLocalTransactionState=true` concludes no transaction is open and **skips
  COMMIT**. Application‑level data loss from one status bit. Fix the seam — read
  the live session status where each packet is written — not the three
  hardcoded constants.
- **#187 long data.** `COM_STMT_SEND_LONG_DATA` gets an ERR where Go sends
  nothing, desynchronising every later response. Needs the per‑statement buffer;
  answering with silence alone would drop the data instead.
- **Coprocessor flags and warnings, together.** `DAGRequest.flags` is `0` where
  Go sends `482`, and TiKV's warnings reach a collector nobody reads. **Fixing
  the flags alone turns "query fails" into "silently truncated with no
  warning"** — strictly worse. One change, both halves.

## Phase 4 — The structural causes

Each of these closes a cluster rather than a bug, and all three are the same
disease: **a decision made where the information isn't.**

- **#192** — entry points that dropped Go's `Context` and hardcoded the flags,
  timezone and warning sink it carried. `Datum::compare` has no sink at all.
- **#198** — Go picks a signature at build time from `FieldType`s; we re‑dispatch
  on the runtime `Datum` kind, which cannot carry an unsigned flag, a declared
  width or a collation's binary‑ness. The float path already does it Go's way and
  the decimal path does not, so the shape is already in the codebase.
- **The unwired components** (#184, #185, and the coprocessor's three). `cargo
  check` structurally cannot see a missing call; one live query catches any of
  them. Consider a test that asserts production paths actually reach the ported
  code, since that is the gap every one of these fell through.

## Phase 5 — Transactions, on a cluster

- **#194** — a pessimistic transaction's pinned primary is dropped on the way to
  prewrite because the plan type has no field for it. Two keys claim to be
  primary, the heartbeat refreshes the wrong one, the real primary is resolved as
  abandoned. Torn transaction.
- **#195** — a lost async‑commit/1PC prewrite response is rolled back where
  client‑go declares it undetermined. Under 1PC the prewrite *is* the commit, so
  this can roll back a committed transaction. Fix with #195's D4 half or the
  truthful `Undetermined` gets flattened to `NotCommitted` one layer up.
- **#174/#183** — Go's retry switches the statement to pessimistic mode; ours
  stays optimistic. Decide whether to port the mode switch or keep optimistic
  retries **with the contention cost measured**. Either is defensible; keeping
  it silently is not.

None of these is reachable from the mock. They need the contended harness from
the lost‑update work.

## Sizing and what runs in parallel

| Item | Rough size | Needs |
| --- | --- | --- |
| #186 status flags | small — one seam, three call sites | wire capture to confirm |
| #187 long data | medium — a buffer with a lifecycle | wire capture |
| Coprocessor flags + warnings | small — two missing calls | a live query |
| #188 declared decimal shape | medium, **blocked on #191** | Go byte fixture |
| #202 name-keyed column refs | large — representation change | fixtures + DDL cases |
| #191 decimal representation | **large, decide before building** | read why `decimal.rs` exists |
| #196 identifier case | small **if** the one-line check confirms it | one Go/Rust comparison |
| #197 unknown enum values | small — the house style already exists | catalog fixture |
| #203 type-change table | small — transcribe Go's pairs | DDL capture harness |
| #199 `Selectivity()` | medium | estRows comparison on unanalysed tables |
| #194 / #195 transactions | medium, **cluster-only** | contended harness |

Phases 2, 3 and 4 are largely independent and can run concurrently — they touch
different crates. Phase 1's fixtures gate Phase 2 only.

## What not to do

- **Do not fix the eight decimal findings before #191 is decided.**
- **Do not fix builtin findings B/C/D/E before #198 is decided.**
- **Do not add the three missing reserved keywords** — the one‑line fix is right
  for one gate and wrong for the other, because Go has two lists and we reuse
  one (#201).
- **Do not "correct" Go's arithmetic.** `row_count_index.go:374` overflows a
  `uint64` on purpose‑by‑accident and clamps to the whole table; computing it
  correctly in `f64` gave a point lookup where Go full‑scans. Faithfulness means
  reproducing the overflow, with the Go line cited.
