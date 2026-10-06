# Retire stale JSON test carriers

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove obsolete string-only JSON harnesses and duplicate test carriers while
preserving their useful SQL checks in existing owner tests. Work in
/workspace/tidb on hparser-integration from
b0a7039c1a1eab52695bc7714557b00b88938500. Fresh Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Go expression JSON signatures return
ETJson/BinaryJSON; Go testkit renders those values through Datum.ToString.
Rust now carries Datum::Json for these families, but eleven integration carriers
and several comments still describe an older string-only implementation.

## Progress


- [x] Verify restored Cloud heads and clean trees; refresh Go master.
- [x] Inspect eleven carriers, existing JSON owner tests and Go signatures/tests.
- [x] Reproduce eleven original carriers: three passed and eight stale-string failures.
- [x] Migrate 31 value vectors and exact wildcard diagnostics; retire eleven carriers.
- [x] Pass sixteen owner tests and one focused diagnostic rerun; lint, metadata and continuity checks pass.
- [ ] Commit through actual hook, fresh locked build, push and verify remote.
- [ ] Save verified recovery bundle and Cloud checkpoint when tools are available.

## Milestones and Plan of Work


Retire the eleven json_*_source integration modules listed in the current-audit
receipt, excluding json_search_source. Migrate unique constant expressions into
existing json_value_functions and json_mutation_functions in
rust/crates/tidb-session/src/tests_json.rs. Move JSON-column arrows/filtering
into json_column_type and the bitwise vector into the existing math/builtin
owner. Remove only duplicates with an explicit stronger retained assertion.
Preserve JSON text, SQL NULL, invalid-path errors and unsigned arithmetic.
For migrated JSON-returning constants assert the typed datum and JSON column
metadata as well as the value. Remove stale comments claiming these families
return strings or CAST loses JSON structure. Retain JSON_SEARCH's separately
known text/result-type boundary and its tests.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh and use CARGO_BUILD_JOBS=1. First run the
eleven original carriers together using a temporary filtered tests/all.rs,
restoring that file byte-for-byte afterward. After migration run from rust/:

    cargo test --locked -p tidb-session --lib -- tests_json:: tests_core::builtins::math_and_conditional_builtins

Verify every removed assertion has a retained owner or migrated vector; do not
remove a failing expectation merely because it fails. Run make lint and git
diff --check from root. No production, Go, Bazel, manifest or fixture change is
planned. Do not run full unrelated suites. Record failures distinctly.

Commit normally using executable hooks/pre-commit selected by core.hooksPath.
The hook must pass cd rust && cargo build --locked -p tidb-server. Rerun the
same locked build immediately before the authorized normal push to
pingcap/tidb hparser-integration; verify remote SHA. Never bypass hooks or force
push. Keep native client-rust unchanged.

## Surprises & Discoveries


Four stale JSON failures were recorded by the previous SQL-helper cleanup.
The wider JSON carrier cluster contains the same outdated representation
assumption. Existing owner tests already use shared row_text and include
exact storage sizes and typed columns. JSON_SEARCH still has a distinct known
text/result-type divergence, so it is not part of this retirement.

## Decision Log


Remove carriers after mapping all 43 assertions: 41 value assertions, one
wildcard-path rejection and one weak size bound. Retain duplicate semantic
checks in their existing owner; move unique cases. No public production change
or complete Go package acceptance is implied. Baseline-only temporary test
executables may be pruned after validation if inactive and invalidated by
restoring the normal test root; retain logs and record hashes/process checks.

## Outcomes & Retrospective


Eleven carriers (466 lines), eleven registrations and obsolete representation
narratives are retired. Net 331 Rust lines removed. All 43 original assertions
have retained owners; 17 migrated queries now check JSON datum/column types.
The grouped sixteen owner tests and focused diagnostic rerun pass. Publication
and checkpoint evidence remain pending. Counts remain 86 tracked,
30 repaired, 56 unresolved (27 open, 29 partial). This is harness maintenance;
no parity root repair or measured speedup is claimed.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show
b0a7039c1a1eab52695bc7714557b00b88938500:<path>, preserving concurrent work.
External inventory and logs live in
/workspace/.cloud-setup/json-carrier-retirement. The durable receipt is
rust/docs/parity/current-audit/json-carrier-retirement-validation.json.
No dependency changes. Saving a Cloud draft, Publish and fresh-task restoration
are distinct; report each only when verified.

Revision: replace completed shared-helper cleanup with the JSON carrier and
stale representation cleanup, preserving unique behavior in existing owners.
