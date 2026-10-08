# Repair sequence for Go/Rust structural parity

Use the [current batch map](remaining-batches.md) for all ten owner batches and
the current unresolved findings, and the [living ExecPlan](../../full-structural-parity-execplan.md)
for implementation, validation and publication. The former W01–W12 assignment,
old source pins and chronological repair notes are preserved exactly in the
[historical sequence](https://github.com/pingcap/tidb/blob/9a319a5d6d78593e623a9db8e8aed1f750380507/rust/docs/parity/current-audit/repair-sequence.md).
They are evidence of earlier work, not a second current queue.

## Dependency order

Start with B01 native PD, service discovery, TSO and transport ownership. Migrate
all TiDB consumers before retiring competing routing or caches; MPP consumers
remain part of that migration even when supplied by B07.

B02 shared SQL/table execution and B03 schema, identity and timestamp protection
are coupled with B04 durable DDL. Protect active transactions, cursors and
internal sessions before enabling MVCC GC. Durable delete-range registration
and consumption must precede corresponding cleanup. Preserve the current
IMPORT and unsafe REORGANIZE refusals until complete owners exist.

B05 independent account, charset and configuration owners can proceed once
their own prerequisites are ready; remote administration still needs B03
identity. B06 typed execution supplies B07 MPP and B10 inference consumers.
B08 job/resource runtimes precede bulk/history consumers. B09 observation must
read real production events and state, not substitute fixture providers.

## Acceptance boundary

Read each selected package's doc.go when present, inventory every source,
original test/support/fixture, generated input and platform/build variant,
and include its complete dependency closure. A package spanning several batches
retains one acceptance decision. Shared caches retain their separate consumer
budgets and lifetimes; repaired cache findings must not be reopened merely by
following an old workstream description.

Use freshly fetched Go master's go.mod for external module pins, and the current
finding register for residual behavior. A passing helper, fewer files, or a
zero count in a known-defect list does not establish whole-package parity.
Group all related regressions and required checks at the completed batch boundary.
Performance claims require matching workloads, semantics and measured evidence.
