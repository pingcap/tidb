# Share diagnostic projection and remote digest selection


This living plan follows root PLANS.md. Work in /workspace/tidb on
hparser-integration from 2bce1b5ba2eb985412c1d6bef9f6299d71872d1f. Native
/workspace/client-rust remains unchanged. Go master is
518bfd82dcf3efadfbd63f9faad9787b4247fabb.

## Purpose and context


Go's transaction, deadlock and lock-wait readers retrieve SQL text only when
their resolved columns require it. Cluster scans carry selected column IDs and
digest predicates to the receiving reader. Rust currently computes full rows
before projection and sends restricted digest lookups without their predicate.
This can fail a simple ID query on an unrelated history-file or remote error.
Connect those boundaries together for I01/I03/O18/N05, preserving privileges,
warnings, current/history selection and shared peer transport.

## Progress


- [x] Inspect the current register, physical memory-table scans, local readers,
  peer sender/receiver and Go expression/util.go and executor/infoschema_reader.go.
- [x] Add grouped baseline regressions to the existing real peer/summary fixture.
- [x] Capture four behavioral failures before changing production owners: all three
  lazy readers touched unavailable history, and fallback omitted Selection.
- [x] Propagate pruned columns, retire unconditional diagnostic lookups, transmit
  digest predicates and compose receiving selection through existing expressions.
- [x] Validate 21 grouped reader/peer tests, real MySQL/gRPC in both summary modes,
  lint and locked server build; update both registers and durable receipts.
- [x] Save validated source and receipts for publication under the actual
  precommit and fresh pre-push locked-build gates. Record publication outcome
  and the reusable Cloud checkpoint in the external final-handoff.json.

## Milestones and concrete steps


First run cargo test --locked -p tidb-server --lib digest_projection_batch --
--test-threads=1 from rust/ after sourcing /workspace/.cloud-setup/env.sh and
exporting CARGO_BUILD_JOBS=1. Three fixtures deny history access while projecting
only IDs/digests; the remote fixture requires a real Selection in the outgoing
DAG. Then carry the physical MemTable column union through Session's existing
materialization and cluster reader. Keep unselected slots only as internal NULL
placeholders for the existing complete-schema overlay, never as returned values.
Move remote projection/selection before expensive diagnostic generation and
reuse the existing typed TiPB decoder/executor. Test non-prefix column IDs,
selection-only columns, predicate/CTE references, privileges and error paths.

Finish source edits before grouped validation. Run affected checks and make lint
once at the successful batch boundary. Actual hooks/pre-commit must pass cd rust
&& cargo build --locked -p tidb-server; rerun it immediately before every push.
Never bypass hooks, force-push or overwrite concurrent work. Keep logs and the
recovery checkpoint in /workspace/.cloud-setup/digest-projection-batch.

## Surprises & Discoveries


The existing SQL path already plans a schema-only memory table before generating
rows. Reuse its pruned physical columns instead of guessing needed fields from
the SELECT AST. CTE and multiple scans require a union of retained columns.

## Decision Log


Maintain existing owners; no new summary cache, transport or shadow SQL session.
Keep full expression builtin/privilege integration and general operator/package
acceptance outside this bounded reader migration. Do not close broad findings
merely because these connected failure paths pass.

## Validation, recovery and outcomes


Acceptance requires previously failing ID-only reads to succeed despite broken
history storage, selected SQL text to retain errors, and actual wire requests to
carry digest selection with original column identities. Source-backed assertions
and Rust cleanup/cancellation checks remain. Remove only this batch's edits if
interrupted; preserve build caches and retire only completed owned executables.
No complete package, multi-node interoperability or performance claim is made.
Final grouped validation passed all 21 peer/diagnostic/summary tests with no ignored
cases. The real MySQL/gRPC probe passed 28 assertions in v1/v2 summary modes;
lint and locked server build passed. See current-audit/digest-projection-validation.json
for hashes, commands and limits. Publication results belong in
/workspace/.cloud-setup/digest-projection-batch/final-handoff.json, which must
record the actual hook, fresh pre-push build and remote SHA before success.

Review discovered that catalog construction also supplied the outgoing client.
Move that borrowed handle into PeerService and attach it independently in both
production stores, so text-only deadlock scans retain global fallback without
constructing metadata. Non-hex IN literals use the shared collator, including
trailing-space semantics; the reader fast filter accepts only canonical hex.

## Outcomes & Retrospective


The connected reader/sender/receiver paths now honor column and digest requirements
without alternate caches, transports or expression evaluation. The four original
failures pass, while selected-field errors and privileges remain enforced.
I01/I03/O18/N05 remain partial for the explicit broader obligations in both
registers; counts stay 86 tracked, 32 repaired and 54 unresolved. No complete Go
package or distributed/performance acceptance is inferred from these checks.
