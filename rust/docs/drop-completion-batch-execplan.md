# Complete destructive DDL targets through shared owners


This living ExecPlan follows PLANS.md. Update progress, discoveries and outcomes.

## Purpose / Big Picture


DROP must preserve each completed target when a later target fails, keep non-table
objects intact, protect system tables and preflight foreign-key references across
the original list. Every completed target must publish its own schema difference,
so other sessions remove all dropped views. This connected maintenance advances
D01/E02/I04/O18 without accepting their complete upstream packages.

## Progress


- [x] Refresh Go master and verify clean integration/native checkouts.
- [x] Reproduce missing-target, wrong-object, duplicate-target and FK failures.
- [x] Migrate shared DROP policy and all live cluster/embedded consumers.
- [x] Validate the grouped batch and update registers.
- [ ] Commit through the actual hook, repeat the locked build and verify normal push.
- [ ] Save and verify the matching Cloud checkpoint.

## Context and Orientation


Work in /workspace/tidb on hparser-integration, initially 19d977345bbb6566b0fa76f50fac61736c4c3209.
Go master 7a3dacb52efe58d28db360ae8639d8838c376544 is exported under
/workspace/.cloud-setup/go-master. Native master 8b752f9638ad157931725b66ffdc57e0465432a9
is unchanged. Read pkg/ddl/executor.go::dropTableObject: FK checks precede the
whole list; object checks and jobs run in written order; missing names accumulate
until completion. Existing Rust cluster DDL uses direct metadata transactions,
not the complete durable job owner. This maintenance preserves that limit.

## Plan of Work / Milestones


First record real unistore/MySQL failures. Then share object admission and ordered
missing-name completion, move cluster DROP through one independently published
operation per target, and remove the incorrect multi-target transaction planner.
Migrate embedded table/view consumers to the same policy, retaining typed errors.
Apply shared FK preflight after implicit commit and before any target mutation.
Finally validate partial completion, cache/system guards, schema reload, original
observation and temporary/persistent ownership together.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh; export CARGO_BUILD_JOBS=1. Use Cargo from
/workspace/tidb/rust, select existing session/DDL test aggregates and group filters.
Run affected all-target checking, make lint at repository root, and cargo build
--locked -p tidb-server. The external wire.py in
/workspace/.cloud-setup/drop-completion-batch starts and joins its isolated server.
Failures must precede passing regressions. The actual precommit hook runs the
locked server build; repeat that build immediately before each normal push and
verify the remote SHA. Never force-push or bypass hooks.

## Surprises & Discoveries


The first 20 live assertions found 12 failures. A later optional ALTER TABLE CACHE
probe stopped that run because the cluster route is unsupported. Cache admission
will be validated in the embedded owner; the revised live batch retains supported
paths. DROP VIEW reports 1105 rather than 1347, DROP TABLE deletes a view, duplicate
names suppress required missing notes, and FK checks do not protect earlier names.

The complete baseline has 28 live checks: 16 fail, with no harness error and a
cleanly joined server. Grouped Rust validation initially passed 174 cases and
failed one stale view CHECK OPTION fixture. Fresh Go parser.y accepts explicit
LOCAL/CASCADED but rejects bare WITH CHECK OPTION. The retained Go TestView
transcreation independently fails before the parser repair (5 other matching
cases pass) and all 6 matching cases pass afterward. Correct the stale embedded
fixture rather than suppressing the source-owned regression.

The first final live run passed 38 of 39 checks and exposed a connected view
replacement defect: CREATE OR REPLACE VIEW omitted the old table ID from its
schema difference, leaving a ghost catalog entry after DROP. Go
pkg/ddl/schema_version.go::SetSchemaDiffForCreateView publishes both IDs. Carry
that old identity and extend the retained DDL test before rerunning live checks.

## Decision Log


- Decision: publish each cluster target through the existing DDL authority and
  share list completion policy with embedded execution. Rationale: putting all
  mutations in one transaction loses prior completion and earlier schema diffs.
- Decision: repair view CHECK OPTION admission in the same view-DDL batch and
  retain the original 60-vector Go parser test. Remove the unsupported bare-form
  success expectation, keeping its 1064 rejection and valid metadata checks.
- Decision: preserve durable-job, delete-range and GC limitations. Per-target
  direct publication is not durable DDL package acceptance.

## Idempotence and Recovery


Preserve all concurrent work. Reuse dependency caches; retire only completed owned
executables with hash/hard-link/process receipts when linking needs about 1 GiB.
Never replay one-use mutation scripts. Preserve a verified external recovery bundle.

## Outcomes & Retrospective


Implementation is complete. 182 selected Rust tests and 39 live checks pass, as do affected all-target checking, lint and the locked server build. The live process exits cleanly. Full Go suites, multi-node
TiKV, durable recovery and performance remain unverified.

Revision note: grouped DROP and view grammar repairs are validated together; parent statuses retain their complete-owner limits. Publication/configuration receipts follow externally to avoid self-referential commit pins.
