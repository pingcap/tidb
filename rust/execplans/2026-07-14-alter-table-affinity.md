# Port ALTER TABLE AFFINITY structurally

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

## Purpose / Big Picture

Port Go's narrow `ALTER TABLE ... AFFINITY [=] 'level'` parser and restore
contract as a typed Rust action. The parser ring obtains canonical restores
without pretending the seed executor owns the partition-placement metadata,
validation, or DDL job machinery required to execute it.

## Progress

- [x] (2026-07-14) Traced Go `parseTableOption`, `parseAlterTableOptions`,
  and `AlterTableSpec.Restore`.
- [x] (2026-07-14) Added a typed `SetAffinity` action, string-only parser
  boundary, canonical restore, and pre-mutation executor gate.
- [x] (2026-07-14) Added direct parser/executor regression tests and a
  checked-Go-oracle selector for the complete ALTER AFFINITY fixture slice.
- [x] (2026-07-14) Reviewed the full static outcome distribution and updated
  it from 48,608 matches / 2,111 parser failures to 48,629 / 2,090; every
  other outcome category remained unchanged.

## Surprises & Discoveries

- Go accepts arbitrary string payloads at parser time, including invalid
  affinity levels; semantic validity is a later DDL concern.
- Go restores the payload's spelling unchanged but always inserts ` = `.

## Decision Log

- Decision: model AFFINITY as its own `AlterTableAction`, not a raw or broad
  generic table option.
  Rationale: this preserves the string-only grammar boundary and avoids
  expanding the executor's generic table-option acceptance surface.
  Date/Author: 2026-07-14 / Codex.
- Decision: reject execution before mutation.
  Rationale: correct execution needs table/partition affinity metadata and
  validation tied to partition layout, absent from the seed catalog.
  Date/Author: 2026-07-14 / Codex.

## Outcomes & Retrospective

All 21 accepted one-statement `ALTER TABLE ... AFFINITY` fixture rows now
restore byte-for-byte from the checked Go oracle. The full parser replay moved
exactly those 21 rows from parse failures to matches; restore mismatches and
Go-rejection directions did not change. CREATE TABLE AFFINITY remains outside
this ALTER-only increment and deliberately visible in the static queue.

## Validation and Acceptance

- The source-derived selector must restore every accepted ALTER AFFINITY row
  byte-for-byte.
- Non-string AFFINITY values must remain parse errors.
- The executor must preserve transaction state when it returns unsupported.
- The static snapshot may change only by the reviewed selected rows.
